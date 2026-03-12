package org.example.api;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.json.JavalinJackson;
import org.example.db.DatabaseManager;
import org.example.db.TransactionDao;
import org.example.generator.FakeDataGenerator;
import org.example.model.CategoryScore;
import org.example.model.CustomerProfile;
import org.example.model.SimilarityResult;
import org.example.scoring.ScoringEngine;
import org.example.similarity.SimilarityEngine;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
/**
 * REST API for the Customer Similarity Engine.
 *
 * <h3>Endpoints</h3>
 * <pre>
 * GET  /api/health                           - health check
 * GET  /api/categories                       - list all categories
 * GET  /api/customers                        - list customer names (paginated)
 * GET  /api/customers/random                 - random customer name
 * GET  /api/customers/{id}/scores/monthly    - monthly scores
 * GET  /api/customers/{id}/scores/{period}   - aggregated scores
 * GET  /api/customers/{id}/profile/{period}  - customer profile map
 * GET  /api/similarity/find                  - find similar customers
 * GET  /api/similarity/find-different        - similar-but-different
 * POST /api/scoring/compute-monthly          - recompute monthly scores
 * POST /api/scoring/compute-aggregated       - compute aggregated scores
 * POST /api/generate                         - generate fake dataset
 * </pre>
 */
public class ApiServer {
    private final DatabaseManager db;
    private final TransactionDao txnDao;
    private final ScoringEngine scoring;
    private final SimilarityEngine similarity;
    private final Javalin app;
    // Generation state
    private final AtomicBoolean generating = new AtomicBoolean(false);
    private final AtomicReference<String> generationStatus = new AtomicReference<>("idle");
    private volatile long generationStartedAt = 0;
    public ApiServer(DatabaseManager db, int port) {
        this.db = db;
        this.txnDao = new TransactionDao(db);
        this.scoring = new ScoringEngine(db);
        this.similarity = new SimilarityEngine(db);
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper, false));
            config.http.defaultContentType = "application/json";
        });
        registerRoutes();
        app.start(port);
    }
    public void stop() {
        app.stop();
    }
    private void registerRoutes() {
        // Serve index.html explicitly so no-cache headers are always applied
        app.get("/", this::serveIndex);
        app.get("/index.html", this::serveIndex);

        app.get("/api/health", ctx ->
                ctx.json(Map.of("status", "UP", "timestamp", System.currentTimeMillis())));
        app.get("/api/stats", this::getStats);
        app.get("/api/categories", this::getCategories);
        app.get("/api/customers", this::getCustomers);
        app.get("/api/customers/random", this::getRandomCustomer);
        app.get("/api/customers/{id}/scores/monthly", this::getMonthlyScores);
        app.get("/api/customers/{id}/scores/{period}", this::getAggregatedScores);
        app.get("/api/customers/{id}/profile/{period}", this::getProfile);
        app.get("/api/similarity/find", this::findSimilar);
        app.get("/api/similarity/find-different", this::findSimilarButDifferent);
        app.post("/api/scoring/compute-monthly", this::computeMonthly);
        app.post("/api/scoring/compute-aggregated", this::computeAggregated);
        app.post("/api/generate", this::generateData);
        app.exception(SQLException.class, (e, ctx) ->
                ctx.status(500).json(Map.of("error", "Database error", "message", e.getMessage())));
        app.exception(IllegalArgumentException.class, (e, ctx) ->
                ctx.status(400).json(Map.of("error", "Bad request", "message", e.getMessage())));
    }
    // -- Handlers ---------------------------------------------------------
    private void serveIndex(Context ctx) throws Exception {
        try (var is = getClass().getResourceAsStream("/public/index.html")) {
            if (is == null) { ctx.status(404).result("index.html not found"); return; }
            String html = new String(is.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
            ctx.header("Cache-Control", "no-cache, no-store, must-revalidate");
            ctx.header("Pragma", "no-cache");
            ctx.header("Expires", "0");
            ctx.contentType("text/html; charset=UTF-8");
            ctx.result(html);
        }
    }

    private void getCategories(Context ctx) throws SQLException {
        ctx.json(Map.of("categories", txnDao.allCategories()));
    }
    private void getCustomers(Context ctx) throws SQLException {
        int page = ctx.queryParamAsClass("page", Integer.class).getOrDefault(1);
        int size = ctx.queryParamAsClass("size", Integer.class).getOrDefault(50);
        long total = txnDao.countCustomers();
        List<String> pageItems = txnDao.customerDisplayNamesPage(page, size);
        ctx.json(Map.of(
                "customers", pageItems,
                "page", page,
                "size", size,
                "totalCustomers", total,
                "totalPages", size > 0 ? (int) Math.ceil((double) total / size) : 0
        ));
    }
    private void getRandomCustomer(Context ctx) throws SQLException {
        String customer = txnDao.randomCustomerDisplayName();
        if (customer == null) {
            ctx.json(Map.of("customer", ""));
            return;
        }
        ctx.json(Map.of("customer", customer));
    }
    private void getMonthlyScores(Context ctx) throws SQLException {
        String customerId = resolveCustomerId(ctx.pathParam("id"));
        List<CategoryScore> scores = scoring.getMonthlyScores(customerId);
        // Return as a flat array for frontend
        ctx.json(scores.stream().map(cs -> Map.of(
                "category", cs.category(),
                "score", cs.score(),
                "rawSpend", cs.rawSpend(),
                "proportionOfTotal", cs.proportionOfTotal()
        )).toList());
    }
    private void getAggregatedScores(Context ctx) throws SQLException {
        String customerId = resolveCustomerId(ctx.pathParam("id"));
        String period = ctx.pathParam("period");
        List<CategoryScore> scores = scoring.getAggregatedScores(customerId, period);
        // Return as a flat array for frontend
        ctx.json(scores.stream().map(cs -> Map.of(
                "category", cs.category(),
                "score", cs.score(),
                "rawSpend", cs.rawSpend(),
                "proportionOfTotal", cs.proportionOfTotal()
        )).toList());
    }
    private void getProfile(Context ctx) throws SQLException {
        String id = resolveCustomerId(ctx.pathParam("id"));
        String period = ctx.pathParam("period");
        CustomerProfile profile = similarity.loadProfile(id, period);
        ctx.json(Map.of(
                "customerName", txnDao.displayNameForCustomer(id),
                "period", profile.period(),
                "scores", profile.scores()
        ));
    }
    private void findSimilar(Context ctx) throws SQLException {
        String customerId = resolveCustomerId(requireParam(ctx, "customerId"));
        String period = requireParam(ctx, "period");
        List<String> categories = parseCategories(requireParam(ctx, "categories"));
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);
        List<SimilarityResult> results = similarity.findSimilar(
                customerId, period, categories, limit);
        Map<String, String> names = namesForResults(results);
        ctx.json(Map.of(
                "referenceCustomer", txnDao.displayNameForCustomer(customerId),
                "period", period,
                "categories", categories,
                "results", results.stream().map(r -> Map.of(
                        "customerName", names.getOrDefault(r.customerId(), r.customerId()),
                        "overallDistance", r.overallDistance(),
                        "overallSimilarity", r.overallSimilarity(),
                        "categoryDistances", r.categoryDistances()
                )).toList()
        ));
    }
    private void findSimilarButDifferent(Context ctx) throws SQLException {
        String customerId = resolveCustomerId(requireParam(ctx, "customerId"));
        String period = requireParam(ctx, "period");
        List<String> simCats = parseCategories(requireParam(ctx, "similarCategories"));
        List<String> diffCats = parseCategories(requireParam(ctx, "dissimilarCategories"));
        double weight = ctx.queryParamAsClass("dissimilarWeight", Double.class).getOrDefault(1.0);
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(10);
        List<SimilarityResult> results = similarity.findSimilarButDifferent(
                customerId, period, simCats, diffCats, weight, limit);
        Map<String, String> names = namesForResults(results);
        ctx.json(Map.of(
                "referenceCustomer", txnDao.displayNameForCustomer(customerId),
                "period", period,
                "similarCategories", simCats,
                "dissimilarCategories", diffCats,
                "dissimilarWeight", weight,
                "results", results.stream().map(r -> Map.of(
                        "customerName", names.getOrDefault(r.customerId(), r.customerId()),
                        "overallDistance", r.overallDistance(),
                        "overallSimilarity", r.overallSimilarity(),
                        "categoryDistances", r.categoryDistances()
                )).toList()
        ));
    }
    private void computeMonthly(Context ctx) throws SQLException {
        long t0 = System.currentTimeMillis();
        scoring.computeMonthlyScores();
        ctx.json(Map.of("status", "completed", "elapsedMs", System.currentTimeMillis() - t0));
    }
    private void computeAggregated(Context ctx) throws SQLException {
        var body = ctx.bodyAsClass(AggregateRequest.class);
        if (body.periodLabel == null || body.fromMonth == null || body.toMonth == null) {
            throw new IllegalArgumentException(
                    "Required fields: periodLabel, fromMonth, toMonth");
        }
        long t0 = System.currentTimeMillis();
        scoring.computeAggregatedScores(body.periodLabel, body.fromMonth, body.toMonth);
        ctx.json(Map.of(
                "status", "completed",
                "periodLabel", body.periodLabel,
                "elapsedMs", System.currentTimeMillis() - t0
        ));
    }
    // Cache stats for 15 seconds
    private volatile Map<String, Object> cachedStats = null;
    private volatile long statsCachedAt = 0;

    private void getStats(Context ctx) throws SQLException {
        long now = System.currentTimeMillis();
        if (cachedStats != null && (now - statsCachedAt) < 15_000) {
            Map<String, Object> fresh = new HashMap<>(cachedStats);
            fresh.put("generationStatus", generationStatus.get());
            if (generationStartedAt > 0) {
                fresh.put("elapsedSeconds", (now - generationStartedAt) / 1000);
            }
            ctx.json(fresh);
            return;
        }
        long totalTransactions = 0;
        long totalCustomers = 0;
        long distinctMonths = 0;
        try (var conn = db.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("SET LOCAL statement_timeout = '5s'");
            // Fast estimate for transaction count — no seq scan needed
            try (var rs = stmt.executeQuery("""
                    SELECT n_live_tup
                    FROM pg_stat_user_tables
                    WHERE relname = 'transaction'
                    """)) {
                if (rs.next()) totalTransactions = rs.getLong(1);
            }
            // monthly_category_score is far smaller than transaction — these are fast
            try (var rs = stmt.executeQuery("""
                    SELECT COUNT(DISTINCT customer_id), COUNT(DISTINCT year_month)
                    FROM monthly_category_score
                    """)) {
                if (rs.next()) {
                    totalCustomers = rs.getLong(1);
                    distinctMonths = rs.getLong(2);
                }
            }
        }
        double avgTxnsPM = (totalCustomers > 0 && distinctMonths > 0)
                ? (double) totalTransactions / (totalCustomers * distinctMonths) : 0.0;

        Map<String, Object> result = new HashMap<>();
        result.put("totalCustomers", totalCustomers);
        result.put("totalTransactions", totalTransactions);
        result.put("avgTxnsPerCustomerPerMonth", Math.round(avgTxnsPM * 10.0) / 10.0);
        result.put("generationStatus", generationStatus.get());
        if (generationStartedAt > 0) {
            result.put("elapsedSeconds", (System.currentTimeMillis() - generationStartedAt) / 1000);
        }
        cachedStats = result;
        statsCachedAt = now;
        ctx.json(result);
    }
    private void generateData(Context ctx) {
        if (generating.get()) {
            ctx.status(409).json(Map.of("error", "Generation already in progress"));
            return;
        }
        var body = ctx.bodyAsClass(GenerateRequest.class);
        int customers = body.customers > 0 ? body.customers : 1000;
        int months    = body.months > 0 ? body.months : 12;
        int txns      = body.txnsPerMonth > 0 ? body.txnsPerMonth : 15;
        LocalDate start = body.startMonth != null
                ? LocalDate.parse(body.startMonth + "-01")
                : LocalDate.of(2024, 1, 1);
        generating.set(true);
        generationStartedAt = System.currentTimeMillis();
        generationStatus.set("running");
        Thread t = new Thread(() -> {
            try {
                FakeDataGenerator gen = body.seed > 0
                        ? new FakeDataGenerator(db, body.seed)
                        : new FakeDataGenerator(db);
                gen.generate(customers, start, months, txns);
                // Refresh pg_stat estimates so the stats endpoint shows accurate counts
                try (var conn = db.getConnection();
                     var stmt = conn.createStatement()) {
                    stmt.execute("ANALYZE \"transaction\", monthly_category_score");
                }
                generationStatus.set("completed");
            } catch (Exception e) {
                generationStatus.set("error: " + e.getMessage());
            } finally {
                generating.set(false);
                cachedStats = null; // invalidate cache so next poll reflects new data
            }
        });
        t.setDaemon(true);
        t.start();
        ctx.status(202).json(Map.of(
                "status", "accepted",
                "message", "Generation started for " + customers + " customers"
        ));
    }
    // -- Helpers -----------------------------------------------------------
    private String requireParam(Context ctx, String name) {
        String val = ctx.queryParam(name);
        if (val == null || val.isBlank()) {
            throw new IllegalArgumentException(
                    "Missing required query parameter: " + name);
        }
        return val;
    }
    private List<String> parseCategories(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private String resolveCustomerId(String idOrName) throws SQLException {
        String resolved = txnDao.resolveCustomerId(idOrName);
        if (resolved == null) {
            throw new IllegalArgumentException("Unknown customer: " + idOrName);
        }
        return resolved;
    }

    private Map<String, String> namesForResults(List<SimilarityResult> results) throws SQLException {
        List<String> ids = results.stream()
                .map(SimilarityResult::customerId)
                .toList();
        return txnDao.displayNamesForCustomers(ids);
    }
    // -- Request DTOs -----------------------------------------------------
    public static class AggregateRequest {
        public String periodLabel;
        public String fromMonth;
        public String toMonth;
    }
    public static class GenerateRequest {
        public int customers;
        public int months;
        public int txnsPerMonth;
        public String startMonth;
        public long seed;
    }
}
