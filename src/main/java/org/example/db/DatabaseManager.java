package org.example.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Manages a PostgreSQL database with connection pooling via HikariCP.
 */
public class DatabaseManager implements AutoCloseable {

    private final HikariDataSource dataSource;

    /**
     * Creates a DatabaseManager from explicit JDBC parameters.
     */
    public static DatabaseManager create(String jdbcUrl, String username, String password) {
        HikariConfig cfg = new HikariConfig();
        // connectTimeout only — no socketTimeout, it kills long-running generation queries
        String url = jdbcUrl.contains("?")
                ? jdbcUrl + "&connectTimeout=5"
                : jdbcUrl + "?connectTimeout=5";
        cfg.setJdbcUrl(url);
        cfg.setUsername(username);
        cfg.setPassword(password);
        cfg.setMaximumPoolSize(10);
        cfg.setMinimumIdle(2);
        cfg.setConnectionTimeout(5000);
        return new DatabaseManager(new HikariDataSource(cfg));
    }

    /**
     * Creates a DatabaseManager from environment variables:
     * <ul>
     *   <li>{@code DB_HOST} (default: localhost)</li>
     *   <li>{@code DB_PORT} (default: 5432)</li>
     *   <li>{@code DB_NAME} (default: similarity)</li>
     *   <li>{@code DB_USER} (default: similarity)</li>
     *   <li>{@code DB_PASSWORD} (default: similarity)</li>
     * </ul>
     */
    public static DatabaseManager fromEnv() {
        String host = env("DB_HOST", "localhost");
        String port = env("DB_PORT", "5432");
        String name = env("DB_NAME", "similarity");
        String user = env("DB_USER", "similarity");
        String pass = env("DB_PASSWORD", "similarity");
        String url = "jdbc:postgresql://" + host + ":" + port + "/" + name;
        return create(url, user, pass);
    }

    private static String env(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }

    private DatabaseManager(HikariDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Initialises the full schema (tables + indexes).
     * Uses PostgreSQL-compatible DDL.
     */
    public void initialiseSchema() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {

            // ── Raw transactions ──────────────────────────────────────────
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS transaction (
                        id               BIGSERIAL      PRIMARY KEY,
                        customer_id      VARCHAR(64)    NOT NULL,
                        category         VARCHAR(128)   NOT NULL,
                        amount           DECIMAL(15,2)  NOT NULL,
                        transaction_date DATE           NOT NULL
                    )
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_txn_customer
                        ON transaction(customer_id)
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_txn_date
                        ON transaction(transaction_date)
                    """);

            // ── Monthly category scores (materialised) ────────────────────
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS monthly_category_score (
                        id                  BIGSERIAL      PRIMARY KEY,
                        customer_id         VARCHAR(64)    NOT NULL,
                        category            VARCHAR(128)   NOT NULL,
                        year_month          VARCHAR(7)     NOT NULL,
                        raw_spend           DECIMAL(15,2)  NOT NULL,
                        proportion_of_total DOUBLE PRECISION NOT NULL,
                        score               INT            NOT NULL,
                        UNIQUE(customer_id, category, year_month)
                    )
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mcs_customer
                        ON monthly_category_score(customer_id)
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_mcs_yearmonth
                        ON monthly_category_score(year_month)
                    """);

            // ── Aggregated scores over arbitrary time windows ─────────────
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS aggregated_category_score (
                        id                  BIGSERIAL      PRIMARY KEY,
                        customer_id         VARCHAR(64)    NOT NULL,
                        category            VARCHAR(128)   NOT NULL,
                        period_label        VARCHAR(32)    NOT NULL,
                        raw_spend           DECIMAL(15,2)  NOT NULL,
                        proportion_of_total DOUBLE PRECISION NOT NULL,
                        score               INT            NOT NULL,
                        UNIQUE(customer_id, category, period_label)
                    )
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_acs_customer
                        ON aggregated_category_score(customer_id)
                    """);

            stmt.execute("""
                    CREATE INDEX IF NOT EXISTS idx_acs_period
                        ON aggregated_category_score(period_label)
                    """);

            // ── Known categories (reference) ──────────────────────────────
            stmt.execute("""
                    CREATE TABLE IF NOT EXISTS category (
                        name VARCHAR(128) PRIMARY KEY
                    )
                    """);
        }
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
