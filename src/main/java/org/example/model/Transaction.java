package org.example.model;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.YearMonth;

/**
 * A single customer transaction.
 */
public record Transaction(
        long id,
        String customerId,
        String category,
        BigDecimal amount,
        LocalDate transactionDate
) {
    public YearMonth yearMonth() {
        return YearMonth.from(transactionDate);
    }
}
