package org.example.scoring;

import org.example.db.DatabaseManager;
import org.example.model.CategoryScore;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Computes per-customer, per-category scores from raw transactions and materialises them
 * into the monthly_category_score and aggregated_category_score tables.
 *
 * <h3>Scoring algorithm</h3>
 * <ol>
 *   <li>For a given customer + time window, sum spend per category.</li>
 *   <li>Compute proportion = category_spend / total_spend.</li>
 *   <li>Find the maximum proportion among all categories for this customer.</li>
 *   <li>Score = round(proportion / max_proportion * 100).  Highest category → 100, zero-spend → 0.</li>
 * </ol>
 */
public class ScoringEngine {

    private final DatabaseManager db;

    public ScoringEngine(DatabaseManager db) {
        this.db = db;
    }

    // ── Monthly scoring ──────────────────────────────────────────────────

    /**
     * Compute and materialise monthly scores for every customer and every month found in
     * the transaction table.  Existing rows are replaced (MERGE).
     */
    public void computeMonthlyScores() throws SQLException {
        // Push all scoring computation into PostgreSQL — no data loaded into Java heap
        String sql = """
                INSERT INTO monthly_category_score
                    (customer_id, category, year_month, raw_spend, proportion_of_total, score)
                SELECT
                    customer_id,
                    category,
                    ym,
                    cat_spend,
                    cat_spend / total_spend                                  AS proportion_of_total,
                    ROUND(cat_spend / max_spend_in_group * 100)::INT         AS score
                FROM (
                    SELECT
                        customer_id,
                        category,
                        TO_CHAR(transaction_date, 'YYYY-MM')                 AS ym,
                        SUM(amount)                                          AS cat_spend,
                        SUM(SUM(amount)) OVER (PARTITION BY customer_id,
                            TO_CHAR(transaction_date, 'YYYY-MM'))            AS total_spend,
                        MAX(SUM(amount)) OVER (PARTITION BY customer_id,
                            TO_CHAR(transaction_date, 'YYYY-MM'))            AS max_spend_in_group
                    FROM transaction
                    GROUP BY customer_id, category, TO_CHAR(transaction_date, 'YYYY-MM')
                ) sub
                WHERE total_spend > 0
                ON CONFLICT (customer_id, category, year_month)
                DO UPDATE SET raw_spend           = EXCLUDED.raw_spend,
                              proportion_of_total = EXCLUDED.proportion_of_total,
                              score               = EXCLUDED.score
                """;
        try (Connection conn = db.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    // ── Read-back helpers ────────────────────────────────────────────────

    /**
     * Aggregate monthly scores into a wider time window and persist to
     * aggregated_category_score.
     *
     * @param periodLabel  label for this aggregation, e.g. "2025-Q1", "2025", "ALL"
     * @param fromMonth    inclusive start month, e.g. "2025-01"
     * @param toMonth      inclusive end month,   e.g. "2025-03"
     */
    public void computeAggregatedScores(String periodLabel, String fromMonth, String toMonth)
            throws SQLException {
        String sql = """
                INSERT INTO aggregated_category_score
                    (customer_id, category, period_label, raw_spend, proportion_of_total, score)
                SELECT
                    customer_id,
                    category,
                    ?                                                        AS period_label,
                    cat_spend,
                    cat_spend / total_spend                                  AS proportion_of_total,
                    ROUND(cat_spend / max_spend_in_group * 100)::INT         AS score
                FROM (
                    SELECT
                        customer_id,
                        category,
                        SUM(amount)                                          AS cat_spend,
                        SUM(SUM(amount)) OVER (PARTITION BY customer_id)    AS total_spend,
                        MAX(SUM(amount)) OVER (PARTITION BY customer_id)    AS max_spend_in_group
                    FROM transaction
                    WHERE TO_CHAR(transaction_date, 'YYYY-MM') >= ?
                      AND TO_CHAR(transaction_date, 'YYYY-MM') <= ?
                    GROUP BY customer_id, category
                ) sub
                WHERE total_spend > 0
                ON CONFLICT (customer_id, category, period_label)
                DO UPDATE SET raw_spend           = EXCLUDED.raw_spend,
                              proportion_of_total = EXCLUDED.proportion_of_total,
                              score               = EXCLUDED.score
                """;
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, periodLabel);
            ps.setString(2, fromMonth);
            ps.setString(3, toMonth);
            ps.execute();
        }
    }


    // ── Read-back helpers ────────────────────────────────────────────────

    /**
     * Retrieve monthly scores for a customer.
     */
    public List<CategoryScore> getMonthlyScores(String customerId) throws SQLException {
        String sql = """
                SELECT customer_id, category, year_month, raw_spend, proportion_of_total, score
                FROM monthly_category_score
                WHERE customer_id = ?
                ORDER BY year_month, category
                """;
        return queryScores(sql, customerId);
    }

    /**
     * Retrieve aggregated scores for a customer and a specific period label.
     */
    public List<CategoryScore> getAggregatedScores(String customerId, String periodLabel)
            throws SQLException {
        String sql = """
                SELECT customer_id, category, period_label, raw_spend, proportion_of_total, score
                FROM aggregated_category_score
                WHERE customer_id = ? AND period_label = ?
                ORDER BY category
                """;
        List<CategoryScore> results = new ArrayList<>();
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, customerId);
            ps.setString(2, periodLabel);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapScore(rs));
                }
            }
        }
        return results;
    }

    private List<CategoryScore> queryScores(String sql, String customerId) throws SQLException {
        List<CategoryScore> results = new ArrayList<>();
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    results.add(mapScore(rs));
                }
            }
        }
        return results;
    }

    private CategoryScore mapScore(ResultSet rs) throws SQLException {
        return new CategoryScore(
                rs.getString(1),
                rs.getString(2),
                rs.getString(3),
                rs.getBigDecimal(4),
                rs.getDouble(5),
                rs.getInt(6)
        );
    }
}
