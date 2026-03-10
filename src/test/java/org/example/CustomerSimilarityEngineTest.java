package org.example;

import org.example.db.DatabaseManager;
import org.example.db.TransactionDao;
import org.example.model.CategoryScore;
import org.example.model.CustomerProfile;
import org.example.model.SimilarityResult;
import org.example.scoring.ScoringEngine;
import org.example.similarity.SimilarityEngine;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Comprehensive tests for the customer similarity engine.
 * Uses Testcontainers to spin up a real PostgreSQL instance.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class CustomerSimilarityEngineTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("similarity_test")
            .withUsername("test")
            .withPassword("test");

    static DatabaseManager db;
    static TransactionDao txnDao;
    static ScoringEngine scoring;
    static SimilarityEngine similarity;

    @BeforeAll
    static void setUp() throws SQLException {
        db = DatabaseManager.create(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
        db.initialiseSchema();
        txnDao = new TransactionDao(db);
        scoring = new ScoringEngine(db);
        similarity = new SimilarityEngine(db);

        seedData();
        txnDao.syncCategories();
        scoring.computeMonthlyScores();
        scoring.computeAggregatedScores("2025-Q1", "2025-01", "2025-03");
    }

    @AfterAll
    static void tearDown() {
        if (db != null) {
            db.close();
        }
    }

    // ── Schema & data tests ──────────────────────────────────────────────

    @Test
    @Order(1)
    void categoriesAreRegistered() throws SQLException {
        List<String> categories = txnDao.allCategories();
        assertThat(categories).containsExactlyInAnyOrder(
                "Food", "Electronics", "Travel", "Clothing", "Healthcare");
    }

    @Test
    @Order(2)
    void allCustomersPresent() throws SQLException {
        List<String> ids = txnDao.allCustomerIds();
        assertThat(ids).containsExactlyInAnyOrder("CUST-001", "CUST-002", "CUST-003", "CUST-004");
    }

    // ── Monthly scoring tests ────────────────────────────────────────────

    @Test
    @Order(10)
    void monthlyScoresExistForAllCustomers() throws SQLException {
        for (String id : txnDao.allCustomerIds()) {
            List<CategoryScore> scores = scoring.getMonthlyScores(id);
            assertThat(scores).isNotEmpty();
        }
    }

    @Test
    @Order(11)
    void highestCategoryGetsScore100() throws SQLException {
        // CUST-001 in Jan: Food=500, Electronics=300, Travel=50, Clothing=80
        // Food is highest → score should be 100
        List<CategoryScore> jan = scoring.getMonthlyScores("CUST-001").stream()
                .filter(cs -> cs.period().equals("2025-01"))
                .toList();

        CategoryScore food = jan.stream().filter(cs -> cs.category().equals("Food")).findFirst().orElseThrow();
        assertThat(food.score()).isEqualTo(100);

        // All scores should be between 0 and 100
        for (CategoryScore cs : jan) {
            assertThat(cs.score()).isBetween(0, 100);
        }
    }

    @Test
    @Order(12)
    void scoresAreProportionalToSpend() throws SQLException {
        // CUST-001 Jan: Food=500 (100), Electronics=300 (should be ~60)
        List<CategoryScore> jan = scoring.getMonthlyScores("CUST-001").stream()
                .filter(cs -> cs.period().equals("2025-01"))
                .toList();

        int foodScore = jan.stream().filter(cs -> cs.category().equals("Food"))
                .mapToInt(CategoryScore::score).findFirst().orElseThrow();
        int elecScore = jan.stream().filter(cs -> cs.category().equals("Electronics"))
                .mapToInt(CategoryScore::score).findFirst().orElseThrow();

        assertThat(foodScore).isEqualTo(100);
        // Electronics proportion = 300/930 ≈ 0.3226, Food proportion = 500/930 ≈ 0.5376
        // elecScore = round(0.3226 / 0.5376 * 100) = round(60.0) = 60
        assertThat(elecScore).isBetween(55, 65);
    }

    @Test
    @Order(13)
    void balancedSpenderHasHighScoresAcrossBoard() throws SQLException {
        // CUST-004 Jan: 200 each in 5 categories → all proportions equal → all score 100
        List<CategoryScore> jan = scoring.getMonthlyScores("CUST-004").stream()
                .filter(cs -> cs.period().equals("2025-01"))
                .toList();

        for (CategoryScore cs : jan) {
            assertThat(cs.score()).isEqualTo(100);
        }
    }

    // ── Aggregated scoring tests ─────────────────────────────────────────

    @Test
    @Order(20)
    void aggregatedScoresExistForQ1() throws SQLException {
        for (String id : txnDao.allCustomerIds()) {
            List<CategoryScore> scores = scoring.getAggregatedScores(id, "2025-Q1");
            assertThat(scores).isNotEmpty();
        }
    }

    @Test
    @Order(21)
    void aggregatedHighestCategoryIs100() throws SQLException {
        // CUST-001 Q1: Food = 500+450+600 = 1550 (highest)
        List<CategoryScore> q1 = scoring.getAggregatedScores("CUST-001", "2025-Q1");
        CategoryScore food = q1.stream().filter(cs -> cs.category().equals("Food")).findFirst().orElseThrow();
        assertThat(food.score()).isEqualTo(100);
        assertThat(food.rawSpend()).isEqualByComparingTo(new BigDecimal("1550.00"));
    }

    // ── Profile loading tests ────────────────────────────────────────────

    @Test
    @Order(30)
    void profileLoadsCorrectly() throws SQLException {
        CustomerProfile profile = similarity.loadProfile("CUST-001", "2025-Q1");
        assertThat(profile.customerId()).isEqualTo("CUST-001");
        assertThat(profile.scores()).containsKey("Food");
        assertThat(profile.scoreFor("Food")).isEqualTo(100);
        // Non-existent category returns 0
        assertThat(profile.scoreFor("Gambling")).isEqualTo(0);
    }

    @Test
    @Order(31)
    void allProfilesLoadForPeriod() throws SQLException {
        List<CustomerProfile> profiles = similarity.loadAllProfiles("2025-Q1");
        assertThat(profiles).hasSize(4);
    }

    // ── Similarity search tests ──────────────────────────────────────────

    @Test
    @Order(40)
    void similarCustomersInFoodAndElectronics() throws SQLException {
        // CUST-002 should be most similar to CUST-001 in Food & Electronics
        List<SimilarityResult> results = similarity.findSimilar(
                "CUST-001", "2025-Q1", List.of("Food", "Electronics"), 10);

        assertThat(results).isNotEmpty();
        // CUST-002 should be first (closest)
        assertThat(results.get(0).customerId()).isEqualTo("CUST-002");
        // CUST-003 (travel-heavy) should be further away
        SimilarityResult cust3 = results.stream()
                .filter(r -> r.customerId().equals("CUST-003")).findFirst().orElseThrow();
        assertThat(cust3.overallDistance()).isGreaterThan(results.get(0).overallDistance());
    }

    @Test
    @Order(41)
    void referenceCustomerNotInResults() throws SQLException {
        List<SimilarityResult> results = similarity.findSimilar(
                "CUST-001", "2025-Q1", List.of("Food"), 10);
        assertThat(results).noneMatch(r -> r.customerId().equals("CUST-001"));
    }

    @Test
    @Order(42)
    void similarButDifferentInTravel() throws SQLException {
        // Find customers similar in Food & Electronics but different in Travel
        List<SimilarityResult> results = similarity.findSimilarButDifferent(
                "CUST-001", "2025-Q1",
                List.of("Food", "Electronics"),
                List.of("Travel"),
                1.0, 10);

        assertThat(results).isNotEmpty();

        // CUST-003 spends heavily on Travel (different from CUST-001) but is not as similar
        // in Food/Electronics. The ranking should balance both factors.
        // CUST-002 has higher travel than CUST-001 → should still rank well
    }

    @Test
    @Order(43)
    void limitIsRespected() throws SQLException {
        List<SimilarityResult> results = similarity.findSimilar(
                "CUST-001", "2025-Q1", List.of("Food"), 1);
        assertThat(results).hasSize(1);
    }

    // ── Direct comparison tests ──────────────────────────────────────────

    @Test
    @Order(50)
    void identicalProfilesHaveZeroDistance() {
        CustomerProfile a = new CustomerProfile("A", "P",
                Map.of("Food", 100, "Travel", 50));
        CustomerProfile b = new CustomerProfile("B", "P",
                Map.of("Food", 100, "Travel", 50));

        SimilarityResult result = similarity.compare(a, b, List.of("Food", "Travel"));
        assertThat(result.overallDistance()).isEqualTo(0.0);
        assertThat(result.overallSimilarity()).isCloseTo(1.0, within(0.001));
    }

    @Test
    @Order(51)
    void oppositeProfilesHaveHighDistance() {
        CustomerProfile a = new CustomerProfile("A", "P",
                Map.of("Food", 100, "Travel", 0));
        CustomerProfile b = new CustomerProfile("B", "P",
                Map.of("Food", 0, "Travel", 100));

        SimilarityResult result = similarity.compare(a, b, List.of("Food", "Travel"));
        assertThat(result.overallDistance()).isGreaterThan(100);
        assertThat(result.overallSimilarity()).isCloseTo(0.0, within(0.001));
    }

    @Test
    @Order(52)
    void partialCategoryComparisonIgnoresOthers() {
        CustomerProfile a = new CustomerProfile("A", "P",
                Map.of("Food", 80, "Travel", 20, "Clothing", 100));
        CustomerProfile b = new CustomerProfile("B", "P",
                Map.of("Food", 80, "Travel", 20, "Clothing", 0));

        // Compare only Food + Travel → should be identical
        SimilarityResult result = similarity.compare(a, b, List.of("Food", "Travel"));
        assertThat(result.overallDistance()).isEqualTo(0.0);
    }

    // ── Helper ───────────────────────────────────────────────────────────

    private static void seedData() throws SQLException {
        // CUST-001: heavy Food & Electronics
        txnDao.insert("CUST-001", "Food",        new BigDecimal("500.00"), LocalDate.of(2025, 1, 5));
        txnDao.insert("CUST-001", "Electronics", new BigDecimal("300.00"), LocalDate.of(2025, 1, 12));
        txnDao.insert("CUST-001", "Travel",      new BigDecimal("50.00"),  LocalDate.of(2025, 1, 20));
        txnDao.insert("CUST-001", "Clothing",    new BigDecimal("80.00"),  LocalDate.of(2025, 1, 25));
        txnDao.insert("CUST-001", "Food",        new BigDecimal("450.00"), LocalDate.of(2025, 2, 3));
        txnDao.insert("CUST-001", "Electronics", new BigDecimal("350.00"), LocalDate.of(2025, 2, 14));
        txnDao.insert("CUST-001", "Travel",      new BigDecimal("30.00"),  LocalDate.of(2025, 2, 18));
        txnDao.insert("CUST-001", "Food",        new BigDecimal("600.00"), LocalDate.of(2025, 3, 1));
        txnDao.insert("CUST-001", "Electronics", new BigDecimal("200.00"), LocalDate.of(2025, 3, 10));
        txnDao.insert("CUST-001", "Healthcare",  new BigDecimal("100.00"), LocalDate.of(2025, 3, 15));

        // CUST-002: similar to CUST-001
        txnDao.insert("CUST-002", "Food",        new BigDecimal("480.00"), LocalDate.of(2025, 1, 7));
        txnDao.insert("CUST-002", "Electronics", new BigDecimal("320.00"), LocalDate.of(2025, 1, 15));
        txnDao.insert("CUST-002", "Travel",      new BigDecimal("200.00"), LocalDate.of(2025, 1, 22));
        txnDao.insert("CUST-002", "Food",        new BigDecimal("520.00"), LocalDate.of(2025, 2, 5));
        txnDao.insert("CUST-002", "Electronics", new BigDecimal("280.00"), LocalDate.of(2025, 2, 10));
        txnDao.insert("CUST-002", "Clothing",    new BigDecimal("60.00"),  LocalDate.of(2025, 2, 20));
        txnDao.insert("CUST-002", "Food",        new BigDecimal("500.00"), LocalDate.of(2025, 3, 3));
        txnDao.insert("CUST-002", "Electronics", new BigDecimal("250.00"), LocalDate.of(2025, 3, 12));

        // CUST-003: Travel-heavy
        txnDao.insert("CUST-003", "Travel",      new BigDecimal("800.00"), LocalDate.of(2025, 1, 3));
        txnDao.insert("CUST-003", "Food",        new BigDecimal("100.00"), LocalDate.of(2025, 1, 10));
        txnDao.insert("CUST-003", "Travel",      new BigDecimal("900.00"), LocalDate.of(2025, 2, 5));
        txnDao.insert("CUST-003", "Electronics", new BigDecimal("50.00"),  LocalDate.of(2025, 2, 15));
        txnDao.insert("CUST-003", "Travel",      new BigDecimal("700.00"), LocalDate.of(2025, 3, 1));
        txnDao.insert("CUST-003", "Healthcare",  new BigDecimal("150.00"), LocalDate.of(2025, 3, 20));

        // CUST-004: balanced
        txnDao.insert("CUST-004", "Food",        new BigDecimal("200.00"), LocalDate.of(2025, 1, 2));
        txnDao.insert("CUST-004", "Electronics", new BigDecimal("200.00"), LocalDate.of(2025, 1, 8));
        txnDao.insert("CUST-004", "Travel",      new BigDecimal("200.00"), LocalDate.of(2025, 1, 15));
        txnDao.insert("CUST-004", "Clothing",    new BigDecimal("200.00"), LocalDate.of(2025, 1, 22));
        txnDao.insert("CUST-004", "Healthcare",  new BigDecimal("200.00"), LocalDate.of(2025, 1, 28));
        txnDao.insert("CUST-004", "Food",        new BigDecimal("180.00"), LocalDate.of(2025, 2, 5));
        txnDao.insert("CUST-004", "Electronics", new BigDecimal("220.00"), LocalDate.of(2025, 2, 12));
        txnDao.insert("CUST-004", "Travel",      new BigDecimal("190.00"), LocalDate.of(2025, 2, 18));
        txnDao.insert("CUST-004", "Clothing",    new BigDecimal("210.00"), LocalDate.of(2025, 2, 25));
        txnDao.insert("CUST-004", "Healthcare",  new BigDecimal("200.00"), LocalDate.of(2025, 3, 5));
    }
}
