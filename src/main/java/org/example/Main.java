package org.example;

import org.example.api.ApiServer;
import org.example.db.DatabaseManager;
import org.example.db.TransactionDao;
import org.example.generator.FakeDataGenerator;
import org.example.model.CategoryScore;
import org.example.model.SimilarityResult;
import org.example.scoring.ScoringEngine;
import org.example.similarity.SimilarityEngine;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;

/**
 * Entry point for the Customer Similarity Engine.
 *
 * <h3>Usage</h3>
 * <pre>
 *   java -jar app.jar serve                       # start REST API on port 8080
 *   java -jar app.jar serve 9090                   # start REST API on custom port
 *   java -jar app.jar generate [customers] [months] [txnsPerMonth]
 *   java -jar app.jar demo                         # run original small demo
 * </pre>
 *
 * <p>Default (no args) starts the API server.</p>
 */
public class Main {

    public static void main(String[] args) throws SQLException {
        String command = args.length > 0 ? args[0] : "serve";

        switch (command) {
            case "serve" -> {
                int port = args.length > 1 ? Integer.parseInt(args[1]) : 9096;
                startServer(port);
            }
            case "generate" -> {
                int customers = args.length > 1 ? Integer.parseInt(args[1]) : 1000;
                int months = args.length > 2 ? Integer.parseInt(args[2]) : 12;
                int txns = args.length > 3 ? Integer.parseInt(args[3]) : 15;
                runGenerate(customers, months, txns);
            }
            case "demo" -> runDemo();
            default -> {
                System.err.println("Unknown command: " + command);
                System.err.println("Usage: java -jar app.jar [serve|generate|demo]");
                System.exit(1);
            }
        }
    }

    private static void startServer(int port) throws SQLException {
        DatabaseManager db = DatabaseManager.fromEnv();
        db.initialiseSchema();
        new ApiServer(db, port);
        System.out.println("Customer Similarity Engine API started on port " + port);
        System.out.println("  Health:  http://localhost:" + port + "/api/health");
        System.out.println("  Docs:    See insomnia-collection.json for all endpoints");
    }

    private static void runGenerate(int customers, int months, int txns) throws SQLException {
        try (DatabaseManager db = DatabaseManager.fromEnv()) {
            db.initialiseSchema();
            System.out.printf("Generating %d customers × %d months × ~%d txns/month...%n",
                    customers, months, txns);
            FakeDataGenerator gen = new FakeDataGenerator(db, 42);
            FakeDataGenerator.GenerationResult result =
                    gen.generate(customers, LocalDate.of(2024, 1, 1), months, txns);
            System.out.printf("Done in %dms: %d customers, %d transactions, %d categories%n",
                    result.elapsedMs(), result.customers(),
                    result.transactions(), result.categories());
        }
    }

    private static void runDemo() throws SQLException {
        try (DatabaseManager db = DatabaseManager.fromEnv()) {
            db.initialiseSchema();

            TransactionDao txnDao = new TransactionDao(db);
            ScoringEngine scoring = new ScoringEngine(db);
            SimilarityEngine similarity = new SimilarityEngine(db);

            seedSampleData(txnDao);
            txnDao.syncCategories();

            System.out.println("=== Categories ===");
            txnDao.allCategories().forEach(c -> System.out.println("  " + c));

            scoring.computeMonthlyScores();

            System.out.println("\n=== Monthly scores for CUST-001 ===");
            for (CategoryScore cs : scoring.getMonthlyScores("CUST-001")) {
                System.out.printf("  [%s] %-15s  spend=%8s  proportion=%.2f  score=%3d%n",
                        cs.period(), cs.category(), cs.rawSpend(),
                        cs.proportionOfTotal(), cs.score());
            }

            scoring.computeAggregatedScores("2025-Q1", "2025-01", "2025-03");

            System.out.println("\n=== Q1-2025 aggregated scores for CUST-001 ===");
            for (CategoryScore cs : scoring.getAggregatedScores("CUST-001", "2025-Q1")) {
                System.out.printf("  %-15s  spend=%8s  proportion=%.2f  score=%3d%n",
                        cs.category(), cs.rawSpend(),
                        cs.proportionOfTotal(), cs.score());
            }

            System.out.println("\n=== Similar customers in [Food, Electronics] for Q1-2025 ===");
            List<SimilarityResult> results = similarity.findSimilar(
                    "CUST-001", "2025-Q1", List.of("Food", "Electronics"), 10);
            for (SimilarityResult r : results) {
                System.out.printf("  %s  distance=%.2f  similarity=%.4f  diffs=%s%n",
                        r.customerId(), r.overallDistance(), r.overallSimilarity(),
                        r.categoryDistances());
            }
        }
    }

    private static void seedSampleData(TransactionDao dao) throws SQLException {
        dao.insert("CUST-001", "Food",        new BigDecimal("500.00"), LocalDate.of(2025, 1, 5));
        dao.insert("CUST-001", "Electronics", new BigDecimal("300.00"), LocalDate.of(2025, 1, 12));
        dao.insert("CUST-001", "Travel",      new BigDecimal("50.00"),  LocalDate.of(2025, 1, 20));
        dao.insert("CUST-001", "Clothing",    new BigDecimal("80.00"),  LocalDate.of(2025, 1, 25));
        dao.insert("CUST-001", "Food",        new BigDecimal("450.00"), LocalDate.of(2025, 2, 3));
        dao.insert("CUST-001", "Electronics", new BigDecimal("350.00"), LocalDate.of(2025, 2, 14));
        dao.insert("CUST-001", "Travel",      new BigDecimal("30.00"),  LocalDate.of(2025, 2, 18));
        dao.insert("CUST-001", "Food",        new BigDecimal("600.00"), LocalDate.of(2025, 3, 1));
        dao.insert("CUST-001", "Electronics", new BigDecimal("200.00"), LocalDate.of(2025, 3, 10));
        dao.insert("CUST-001", "Healthcare",  new BigDecimal("100.00"), LocalDate.of(2025, 3, 15));

        dao.insert("CUST-002", "Food",        new BigDecimal("480.00"), LocalDate.of(2025, 1, 7));
        dao.insert("CUST-002", "Electronics", new BigDecimal("320.00"), LocalDate.of(2025, 1, 15));
        dao.insert("CUST-002", "Travel",      new BigDecimal("200.00"), LocalDate.of(2025, 1, 22));
        dao.insert("CUST-002", "Food",        new BigDecimal("520.00"), LocalDate.of(2025, 2, 5));
        dao.insert("CUST-002", "Electronics", new BigDecimal("280.00"), LocalDate.of(2025, 2, 10));
        dao.insert("CUST-002", "Clothing",    new BigDecimal("60.00"),  LocalDate.of(2025, 2, 20));
        dao.insert("CUST-002", "Food",        new BigDecimal("500.00"), LocalDate.of(2025, 3, 3));
        dao.insert("CUST-002", "Electronics", new BigDecimal("250.00"), LocalDate.of(2025, 3, 12));

        dao.insert("CUST-003", "Travel",      new BigDecimal("800.00"), LocalDate.of(2025, 1, 3));
        dao.insert("CUST-003", "Food",        new BigDecimal("100.00"), LocalDate.of(2025, 1, 10));
        dao.insert("CUST-003", "Travel",      new BigDecimal("900.00"), LocalDate.of(2025, 2, 5));
        dao.insert("CUST-003", "Electronics", new BigDecimal("50.00"),  LocalDate.of(2025, 2, 15));
        dao.insert("CUST-003", "Travel",      new BigDecimal("700.00"), LocalDate.of(2025, 3, 1));
        dao.insert("CUST-003", "Healthcare",  new BigDecimal("150.00"), LocalDate.of(2025, 3, 20));

        dao.insert("CUST-004", "Food",        new BigDecimal("200.00"), LocalDate.of(2025, 1, 2));
        dao.insert("CUST-004", "Electronics", new BigDecimal("200.00"), LocalDate.of(2025, 1, 8));
        dao.insert("CUST-004", "Travel",      new BigDecimal("200.00"), LocalDate.of(2025, 1, 15));
        dao.insert("CUST-004", "Clothing",    new BigDecimal("200.00"), LocalDate.of(2025, 1, 22));
        dao.insert("CUST-004", "Healthcare",  new BigDecimal("200.00"), LocalDate.of(2025, 1, 28));
        dao.insert("CUST-004", "Food",        new BigDecimal("180.00"), LocalDate.of(2025, 2, 5));
        dao.insert("CUST-004", "Electronics", new BigDecimal("220.00"), LocalDate.of(2025, 2, 12));
        dao.insert("CUST-004", "Travel",      new BigDecimal("190.00"), LocalDate.of(2025, 2, 18));
        dao.insert("CUST-004", "Clothing",    new BigDecimal("210.00"), LocalDate.of(2025, 2, 25));
        dao.insert("CUST-004", "Healthcare",  new BigDecimal("200.00"), LocalDate.of(2025, 3, 5));
    }
}
