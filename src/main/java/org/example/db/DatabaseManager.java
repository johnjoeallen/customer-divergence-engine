package org.example.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Manages a PostgreSQL database with connection pooling via HikariCP.
 * Schema migrations are handled by Flyway from classpath:db/migration.
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
     * Runs all pending Flyway migrations from classpath:db/migration.
     */
    public void initialiseSchema() {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .load()
                .migrate();
    }

    @Override
    public void close() {
        dataSource.close();
    }
}
