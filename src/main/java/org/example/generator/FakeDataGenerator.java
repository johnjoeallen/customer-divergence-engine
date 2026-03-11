package org.example.generator;

import com.github.f4b6a3.uuid.UuidCreator;
import org.example.db.DatabaseManager;
import org.example.db.TransactionDao;
import org.example.model.Transaction;
import org.example.scoring.ScoringEngine;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

/**
 * Generates a large, realistic fake dataset of customer transactions.
 *
 * <h3>Design</h3>
 * <p>Each customer is assigned a <em>spending persona</em> — a probability distribution
 * across categories.  Transactions are generated per-month with randomised amounts
 * weighted by the persona.  This produces realistic clustering where some customers
 * naturally share similar spending patterns while others differ.</p>
 *
 * <h3>Personas</h3>
 * <ul>
 *   <li><b>Foodie</b> — heavy food &amp; dining</li>
 *   <li><b>Techie</b> — electronics &amp; subscriptions</li>
 *   <li><b>Traveller</b> — travel &amp; transport</li>
 *   <li><b>Fashionista</b> — clothing &amp; accessories</li>
 *   <li><b>Homebody</b> — utilities &amp; groceries</li>
 *   <li><b>Balanced</b> — even across all categories</li>
 * </ul>
 */
public class FakeDataGenerator {

    /** All spending categories used in generation. */
    public static final List<String> CATEGORIES = List.of(
            "Groceries", "Dining", "Electronics", "Clothing",
            "Travel", "Transport", "Healthcare", "Entertainment",
            "Utilities", "Subscriptions", "Home & Garden", "Education"
    );

    /**
     * Persona definitions: each is a map of category → relative weight.
     * Weights don't need to sum to 1 — they're normalised internally.
     */
    private static final Map<String, Map<String, Double>> PERSONAS = Map.of(
            "Foodie", Map.ofEntries(
                    Map.entry("Groceries", 0.30), Map.entry("Dining", 0.25), Map.entry("Entertainment", 0.10),
                    Map.entry("Utilities", 0.08), Map.entry("Transport", 0.07), Map.entry("Healthcare", 0.05),
                    Map.entry("Subscriptions", 0.05), Map.entry("Clothing", 0.04), Map.entry("Travel", 0.03),
                    Map.entry("Electronics", 0.02), Map.entry("Home & Garden", 0.005), Map.entry("Education", 0.005)),
            "Techie", Map.ofEntries(
                    Map.entry("Electronics", 0.30), Map.entry("Subscriptions", 0.20), Map.entry("Entertainment", 0.12),
                    Map.entry("Dining", 0.08), Map.entry("Groceries", 0.08), Map.entry("Utilities", 0.06),
                    Map.entry("Transport", 0.04), Map.entry("Clothing", 0.04), Map.entry("Healthcare", 0.03),
                    Map.entry("Travel", 0.03), Map.entry("Home & Garden", 0.01), Map.entry("Education", 0.01)),
            "Traveller", Map.ofEntries(
                    Map.entry("Travel", 0.35), Map.entry("Transport", 0.15), Map.entry("Dining", 0.12),
                    Map.entry("Entertainment", 0.08), Map.entry("Groceries", 0.07), Map.entry("Clothing", 0.06),
                    Map.entry("Utilities", 0.05), Map.entry("Healthcare", 0.04), Map.entry("Subscriptions", 0.03),
                    Map.entry("Electronics", 0.03), Map.entry("Home & Garden", 0.01), Map.entry("Education", 0.01)),
            "Fashionista", Map.ofEntries(
                    Map.entry("Clothing", 0.30), Map.entry("Dining", 0.12), Map.entry("Entertainment", 0.12),
                    Map.entry("Groceries", 0.10), Map.entry("Travel", 0.08), Map.entry("Subscriptions", 0.06),
                    Map.entry("Utilities", 0.06), Map.entry("Healthcare", 0.05), Map.entry("Transport", 0.04),
                    Map.entry("Electronics", 0.04), Map.entry("Home & Garden", 0.02), Map.entry("Education", 0.01)),
            "Homebody", Map.ofEntries(
                    Map.entry("Utilities", 0.20), Map.entry("Groceries", 0.20), Map.entry("Home & Garden", 0.15),
                    Map.entry("Subscriptions", 0.10), Map.entry("Healthcare", 0.08), Map.entry("Entertainment", 0.07),
                    Map.entry("Dining", 0.06), Map.entry("Clothing", 0.04), Map.entry("Electronics", 0.04),
                    Map.entry("Transport", 0.03), Map.entry("Travel", 0.02), Map.entry("Education", 0.01)),
            "Balanced", Map.ofEntries(
                    Map.entry("Groceries", 0.09), Map.entry("Dining", 0.09),
                    Map.entry("Electronics", 0.09), Map.entry("Clothing", 0.09),
                    Map.entry("Travel", 0.08), Map.entry("Transport", 0.08),
                    Map.entry("Healthcare", 0.08), Map.entry("Entertainment", 0.08),
                    Map.entry("Utilities", 0.08), Map.entry("Subscriptions", 0.08),
                    Map.entry("Home & Garden", 0.08), Map.entry("Education", 0.07))
    );

    private static final List<String> PERSONA_NAMES = List.copyOf(PERSONAS.keySet());

    private final DatabaseManager db;
    private final Random random;

    public FakeDataGenerator(DatabaseManager db) {
        this(db, new Random());
    }

    public FakeDataGenerator(DatabaseManager db, long seed) {
        this(db, new Random(seed));
    }

    private FakeDataGenerator(DatabaseManager db, Random random) {
        this.db = db;
        this.random = random;
    }

    /**
     * Generate a full dataset and compute all scores.
     *
     * @param customerCount  number of customers (e.g. 10_000)
     * @param startMonth     first month of data, e.g. LocalDate.of(2024,1,1)
     * @param months         number of months of data (e.g. 12)
     * @param txnsPerMonth   average transactions per customer per month (e.g. 15)
     * @return summary stats
     */
    public GenerationResult generate(int customerCount, LocalDate startMonth,
                                     int months, int txnsPerMonth) throws SQLException {
        long t0 = System.currentTimeMillis();

        TransactionDao dao = new TransactionDao(db);
        long totalTxns = 0;

        // Process in batches to avoid memory pressure
        int batchSize = 500;
        List<Transaction> batch = new ArrayList<>(batchSize);

        for (int c = 0; c < customerCount; c++) {
            String customerId = UuidCreator.getTimeOrderedEpoch().toString();
            String persona = PERSONA_NAMES.get(random.nextInt(PERSONA_NAMES.size()));
            Map<String, Double> weights = PERSONAS.get(persona);

            // Monthly budget: between 500 and 5000, with some noise per month
            double baseBudget = 500 + random.nextDouble() * 4500;

            for (int m = 0; m < months; m++) {
                LocalDate monthStart = startMonth.plusMonths(m);
                int daysInMonth = monthStart.lengthOfMonth();

                // Some monthly variance (+/- 30%)
                double monthBudget = baseBudget * (0.7 + random.nextDouble() * 0.6);
                int txnCount = Math.max(1, txnsPerMonth + random.nextInt(7) - 3);

                for (int t = 0; t < txnCount; t++) {
                    String category = pickWeightedCategory(weights);
                    double amount = generateAmount(monthBudget / txnCount, category);
                    int day = 1 + random.nextInt(daysInMonth);
                    LocalDate date = monthStart.withDayOfMonth(day);

                    batch.add(new Transaction(0, customerId, category,
                            BigDecimal.valueOf(amount).setScale(2, RoundingMode.HALF_UP), date));
                    totalTxns++;

                    if (batch.size() >= batchSize) {
                        dao.insertBatch(batch);
                        batch.clear();
                    }
                }
            }
        }

        // Flush remaining
        if (!batch.isEmpty()) {
            dao.insertBatch(batch);
        }

        // Sync categories
        dao.syncCategories();

        // Compute scores
        ScoringEngine scoring = new ScoringEngine(db);
        scoring.computeMonthlyScores();

        // Compute quarterly and yearly aggregations
        for (int m = 0; m < months; m += 3) {
            LocalDate qStart = startMonth.plusMonths(m);
            LocalDate qEnd = startMonth.plusMonths(Math.min(m + 2, months - 1));
            int quarter = (qStart.getMonthValue() - 1) / 3 + 1;
            String label = qStart.getYear() + "-Q" + quarter;
            String from = String.format("%d-%02d", qStart.getYear(), qStart.getMonthValue());
            String to = String.format("%d-%02d", qEnd.getYear(), qEnd.getMonthValue());
            scoring.computeAggregatedScores(label, from, to);
        }

        // Full period aggregation
        String fromAll = String.format("%d-%02d", startMonth.getYear(), startMonth.getMonthValue());
        LocalDate endMonth = startMonth.plusMonths(months - 1);
        String toAll = String.format("%d-%02d", endMonth.getYear(), endMonth.getMonthValue());
        scoring.computeAggregatedScores("ALL", fromAll, toAll);

        long elapsed = System.currentTimeMillis() - t0;

        return new GenerationResult(customerCount, totalTxns, months,
                CATEGORIES.size(), elapsed);
    }

    /**
     * Pick a category weighted by the persona distribution.
     */
    private String pickWeightedCategory(Map<String, Double> weights) {
        double total = weights.values().stream().mapToDouble(Double::doubleValue).sum();
        double r = random.nextDouble() * total;
        double cumulative = 0;
        for (var entry : weights.entrySet()) {
            cumulative += entry.getValue();
            if (r <= cumulative) return entry.getKey();
        }
        // Fallback (shouldn't happen)
        return weights.keySet().iterator().next();
    }

    /**
     * Generate a transaction amount with category-appropriate variance.
     */
    private double generateAmount(double baseAmount, String category) {
        // Different categories have different typical transaction sizes
        double multiplier = switch (category) {
            case "Travel" -> 2.0 + random.nextDouble() * 3.0;    // 2-5x base
            case "Electronics" -> 1.5 + random.nextDouble() * 2.5; // 1.5-4x
            case "Healthcare" -> 0.5 + random.nextDouble() * 3.0;  // 0.5-3.5x
            case "Education" -> 1.0 + random.nextDouble() * 4.0;   // 1-5x (tuition etc)
            case "Home & Garden" -> 0.8 + random.nextDouble() * 2.0;
            case "Dining" -> 0.3 + random.nextDouble() * 0.7;      // smaller transactions
            case "Groceries" -> 0.4 + random.nextDouble() * 0.8;
            case "Subscriptions" -> 0.1 + random.nextDouble() * 0.3; // small recurring
            default -> 0.5 + random.nextDouble() * 1.0;
        };
        double amount = baseAmount * multiplier;
        return Math.max(1.00, amount); // minimum $1
    }

    /**
     * Summary of a generation run.
     */
    public record GenerationResult(
            int customers,
            long transactions,
            int months,
            int categories,
            long elapsedMs
    ) {
    }
}
