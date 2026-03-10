package org.example.db;

import org.example.model.Transaction;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

/**
 * DAO for raw customer transactions.
 */
public class TransactionDao {

    private final DatabaseManager db;

    public TransactionDao(DatabaseManager db) {
        this.db = db;
    }

    /**
     * Insert a single transaction and return it with its generated id.
     */
    public Transaction insert(String customerId, String category, BigDecimal amount, LocalDate date)
            throws SQLException {
        String sql = "INSERT INTO transaction (customer_id, category, amount, transaction_date) VALUES (?,?,?,?)";
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, customerId);
            ps.setString(2, category);
            ps.setBigDecimal(3, amount);
            ps.setDate(4, Date.valueOf(date));
            ps.executeUpdate();

            try (ResultSet keys = ps.getGeneratedKeys()) {
                keys.next();
                long id = keys.getLong(1);
                return new Transaction(id, customerId, category, amount, date);
            }
        }
    }

    /**
     * Batch insert transactions for efficiency.
     */
    public void insertBatch(List<Transaction> transactions) throws SQLException {
        String sql = "INSERT INTO transaction (customer_id, category, amount, transaction_date) VALUES (?,?,?,?)";
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (Transaction t : transactions) {
                ps.setString(1, t.customerId());
                ps.setString(2, t.category());
                ps.setBigDecimal(3, t.amount());
                ps.setDate(4, Date.valueOf(t.transactionDate()));
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        }
    }

    /**
     * Ensure all categories from transactions are registered in the category table.
     */
    public void syncCategories() throws SQLException {
        String sql = """
                INSERT INTO category(name)
                SELECT DISTINCT category FROM transaction
                ON CONFLICT (name) DO NOTHING
                """;
        try (Connection conn = db.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    /**
     * List all known categories.
     */
    public List<String> allCategories() throws SQLException {
        List<String> categories = new ArrayList<>();
        try (Connection conn = db.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM category ORDER BY name")) {
            while (rs.next()) {
                categories.add(rs.getString(1));
            }
        }
        return categories;
    }

    /**
     * List all distinct customer IDs.
     */
    public List<String> allCustomerIds() throws SQLException {
        List<String> ids = new ArrayList<>();
        try (Connection conn = db.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT DISTINCT customer_id FROM transaction ORDER BY customer_id")) {
            while (rs.next()) {
                ids.add(rs.getString(1));
            }
        }
        return ids;
    }
}
