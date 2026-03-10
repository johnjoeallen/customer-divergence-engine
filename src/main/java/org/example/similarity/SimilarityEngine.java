package org.example.similarity;

import org.example.db.DatabaseManager;
import org.example.model.CustomerProfile;
import org.example.model.SimilarityResult;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Compares customers against a reference customer based on their category spending profiles,
 * ranking them by how similar they are in some categories and how different they are in others.
 *
 * <p>The core use case is: <em>"Find me customers who spend like customer X in categories
 * A, B and C, but very differently in categories D and E."</em> This is useful for identifying
 * customers who share the same base spending behaviour but diverge in specific areas —
 * for example, two customers with identical grocery and utility habits but completely
 * different travel or dining patterns.</p>
 *
 * <p>Similarity between two customers is measured using <strong>Euclidean distance</strong>
 * across their category scores — the lower the distance, the more alike they are in those
 * categories. A <strong>cosine similarity</strong> score is also computed to capture whether
 * two customers spend in the same proportional shape, regardless of absolute amounts;
 * this is returned alongside results as supplementary information.</p>
 *
 * <p>When both similar and dissimilar categories are provided, each candidate is scored as:
 * <pre>  combined = −similarityDistance + dissimilarWeight × dissimilarDistance</pre>
 * Candidates are ranked by this score descending — rewarding those who are
 * <strong>close</strong> in the similar categories and <strong>far apart</strong>
 * in the dissimilar categories. The {@code dissimilarWeight} parameter controls how strongly
 * the dissimilar categories influence the final ranking relative to the similar ones.</p>
 */
public class SimilarityEngine {

    private final DatabaseManager db;

    public SimilarityEngine(DatabaseManager db) {
        this.db = db;
    }

    // ── Profile loading ──────────────────────────────────────────────────

    /**
     * Load the customer's profile for a given period label from aggregated scores.
     * If no aggregated scores exist, falls back to a single month from monthly scores.
     */
    public CustomerProfile loadProfile(String customerId, String period) throws SQLException {
        Map<String, Integer> scores = new LinkedHashMap<>();

        // Try aggregated first
        String sql = """
                SELECT category, score
                FROM aggregated_category_score
                WHERE customer_id = ? AND period_label = ?
                ORDER BY category
                """;
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, customerId);
            ps.setString(2, period);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    scores.put(rs.getString("category"), rs.getInt("score"));
                }
            }
        }

        if (scores.isEmpty()) {
            // Fall back to monthly
            String monthlySql = """
                    SELECT category, score
                    FROM monthly_category_score
                    WHERE customer_id = ? AND year_month = ?
                    ORDER BY category
                    """;
            try (Connection conn = db.getConnection();
                 PreparedStatement ps = conn.prepareStatement(monthlySql)) {
                ps.setString(1, customerId);
                ps.setString(2, period);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        scores.put(rs.getString("category"), rs.getInt("score"));
                    }
                }
            }
        }

        return new CustomerProfile(customerId, period, scores);
    }

    /**
     * Load profiles for ALL customers for a given period.
     */
    public List<CustomerProfile> loadAllProfiles(String period) throws SQLException {
        // Try aggregated first; if empty, try monthly
        List<CustomerProfile> profiles = loadAllFromTable(
                "aggregated_category_score", "period_label", period);
        if (profiles.isEmpty()) {
            profiles = loadAllFromTable(
                    "monthly_category_score", "year_month", period);
        }
        return profiles;
    }

    private List<CustomerProfile> loadAllFromTable(String table, String periodCol, String period)
            throws SQLException {
        String sql = "SELECT customer_id, category, score FROM " + table
                     + " WHERE " + periodCol + " = ? ORDER BY customer_id, category";
        Map<String, Map<String, Integer>> map = new LinkedHashMap<>();
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, period);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    map.computeIfAbsent(rs.getString("customer_id"), k -> new LinkedHashMap<>())
                            .put(rs.getString("category"), rs.getInt("score"));
                }
            }
        }
        List<CustomerProfile> results = new ArrayList<>();
        for (var entry : map.entrySet()) {
            results.add(new CustomerProfile(entry.getKey(), period, entry.getValue()));
        }
        return results;
    }

    // ── Similarity search ────────────────────────────────────────────────

    /**
     * Find customers most similar to {@code referenceCustomerId} across the given
     * {@code similarCategories}, for the specified period.
     *
     * @param referenceCustomerId  the customer to compare against
     * @param period               the time period (month or aggregation label)
     * @param similarCategories    categories where similarity is desired
     * @param limit                max number of results to return
     * @return sorted list of similarity results (most dissimilar first)
     */
    public List<SimilarityResult> findSimilar(
            String referenceCustomerId,
            String period,
            Collection<String> similarCategories,
            int limit) throws SQLException {

        CustomerProfile reference = loadProfile(referenceCustomerId, period);
        List<CustomerProfile> allProfiles = loadAllProfiles(period);

        List<SimilarityResult> results = new ArrayList<>();
        for (CustomerProfile other : allProfiles) {
            if (other.customerId().equals(referenceCustomerId)) continue;

            results.add(compare(reference, other, similarCategories));
        }

        // Sort by distance descending (most dissimilar first)
        results.sort(Comparator.comparingDouble(SimilarityResult::overallDistance).reversed());
        if (results.size() > limit) {
            results = results.subList(0, limit);
        }
        return results;
    }

    /**
     * Find customers that are similar in {@code similarCategories} but <em>dissimilar</em>
     * in {@code dissimilarCategories}.
     *
     * <p>The final ranking is:
     * {@code combined = −similarityDistance + dissimilarWeight × dissimilarDistance}.
     * Higher combined score = better match: small distance in similar categories
     * and large distance in dissimilar categories ranks first.</p>
     *
     * @param referenceCustomerId   customer to compare against
     * @param period                time period
     * @param similarCategories     categories where similarity is desired
     * @param dissimilarCategories  categories where difference is desired
     * @param dissimilarWeight      how much to reward dissimilarity (default 1.0)
     * @param limit                 max results
     */
    public List<SimilarityResult> findSimilarButDifferent(
            String referenceCustomerId,
            String period,
            Collection<String> similarCategories,
            Collection<String> dissimilarCategories,
            double dissimilarWeight,
            int limit) throws SQLException {

        CustomerProfile reference = loadProfile(referenceCustomerId, period);
        List<CustomerProfile> allProfiles = loadAllProfiles(period);

        record Candidate(SimilarityResult result, double combinedScore) {}
        List<Candidate> candidates = new ArrayList<>();

        for (CustomerProfile other : allProfiles) {
            if (other.customerId().equals(referenceCustomerId)) continue;

            SimilarityResult simResult = compare(reference, other, similarCategories);
            double simDistance = simResult.overallDistance();

            // Compute dissimilarity distance (we WANT this to be large)
            SimilarityResult diffResult = compare(reference, other, dissimilarCategories);
            double diffDistance = diffResult.overallDistance();

            // Combined: low sim distance + high diff distance → high score → better
            double combined = -simDistance + dissimilarWeight * diffDistance;

            // Merge category distances
            Map<String, Integer> allDists = new LinkedHashMap<>(simResult.categoryDistances());
            allDists.putAll(diffResult.categoryDistances());

            candidates.add(new Candidate(
                    new SimilarityResult(other.customerId(), simDistance,
                            simResult.overallSimilarity(), allDists),
                    combined));
        }

        candidates.sort(Comparator.comparingDouble(Candidate::combinedScore).reversed());
        return candidates.stream()
                .map(Candidate::result)
                .limit(limit)
                .toList();
    }

    // ── Core comparison ──────────────────────────────────────────────────

    /**
     * Compare two customer profiles across the specified categories.
     */
    public SimilarityResult compare(
            CustomerProfile a,
            CustomerProfile b,
            Collection<String> categories) {

        Map<String, Integer> distances = new LinkedHashMap<>();
        double sumSqDist = 0;
        double dotProduct = 0;
        double normA = 0;
        double normB = 0;

        for (String cat : categories) {
            int scoreA = a.scoreFor(cat);
            int scoreB = b.scoreFor(cat);
            int diff = Math.abs(scoreA - scoreB);
            distances.put(cat, diff);
            sumSqDist += diff * diff;
            dotProduct += scoreA * scoreB;
            normA += scoreA * scoreA;
            normB += scoreB * scoreB;
        }

        double euclidean = Math.sqrt(sumSqDist);
        double cosine = (normA == 0 || normB == 0)
                ? 0.0
                : dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));

        return new SimilarityResult(b.customerId(), euclidean, cosine, distances);
    }
}
