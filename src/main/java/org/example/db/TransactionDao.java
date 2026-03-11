package org.example.db;

import org.example.model.Transaction;
import org.example.model.CustomerName;

import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * Batch upsert customer names.
     */
    public void insertCustomerNamesBatch(List<CustomerName> customers) throws SQLException {
        if (customers == null || customers.isEmpty()) return;
        String sql = """
                INSERT INTO customer(customer_id, first_name, middle_name, last_name, display_name)
                VALUES (?,?,?,?,?)
                ON CONFLICT (customer_id)
                DO UPDATE SET first_name = EXCLUDED.first_name,
                              middle_name = EXCLUDED.middle_name,
                              last_name = EXCLUDED.last_name,
                              display_name = EXCLUDED.display_name
                """;
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            conn.setAutoCommit(false);
            for (CustomerName c : customers) {
                ps.setString(1, c.customerId());
                ps.setString(2, c.firstName());
                ps.setString(3, c.middleName());
                ps.setString(4, c.lastName());
                ps.setString(5, c.displayName());
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
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

    /**
     * List all customer display names (falls back to ID where no name exists).
     */
    public List<String> allCustomerDisplayNames() throws SQLException {
        List<String> names = new ArrayList<>();
        String sql = """
                WITH ids AS (SELECT DISTINCT customer_id FROM transaction)
                SELECT COALESCE(c.display_name, ids.customer_id) AS display_name
                FROM ids
                LEFT JOIN customer c ON c.customer_id = ids.customer_id
                ORDER BY display_name
                """;
        try (Connection conn = db.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                names.add(rs.getString("display_name"));
            }
        }
        return names;
    }

    /**
     * Resolve an input value to a concrete customer ID.
     * Input may already be an ID or may be a display name.
     */
    public String resolveCustomerId(String idOrName) throws SQLException {
        String sql = """
                WITH ids AS (SELECT DISTINCT customer_id FROM transaction),
                matches AS (
                    SELECT customer_id, 1 AS priority FROM ids WHERE customer_id = ?
                    UNION ALL
                    SELECT ids.customer_id, 2 AS priority
                    FROM ids
                    JOIN customer c ON c.customer_id = ids.customer_id
                    WHERE c.display_name = ?
                )
                SELECT customer_id
                FROM matches
                ORDER BY priority
                LIMIT 1
                """;
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, idOrName);
            ps.setString(2, idOrName);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("customer_id");
            }
        }
        return null;
    }

    /**
     * Resolve a customer ID to its display name, falling back to the ID itself.
     */
    public String displayNameForCustomer(String customerId) throws SQLException {
        String sql = "SELECT display_name FROM customer WHERE customer_id = ?";
        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, customerId);
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) return rs.getString("display_name");
            }
        }
        return customerId;
    }

    /**
     * Resolve many customer IDs to display names. Missing rows fall back to ID.
     */
    public Map<String, String> displayNamesForCustomers(Collection<String> customerIds) throws SQLException {
        Map<String, String> result = new HashMap<>();
        if (customerIds == null || customerIds.isEmpty()) return result;

        List<String> ids = new ArrayList<>(customerIds);
        String placeholders = String.join(",", java.util.Collections.nCopies(ids.size(), "?"));
        String sql = "SELECT customer_id, display_name FROM customer WHERE customer_id IN (" + placeholders + ")";

        try (Connection conn = db.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ids.size(); i++) {
                ps.setString(i + 1, ids.get(i));
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    result.put(rs.getString("customer_id"), rs.getString("display_name"));
                }
            }
        }

        for (String id : ids) {
            result.putIfAbsent(id, id);
        }
        return result;
    }
}
