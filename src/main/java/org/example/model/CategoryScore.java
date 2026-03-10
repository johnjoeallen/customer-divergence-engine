package org.example.model;

import java.math.BigDecimal;

/**
 * A score (0–100) for a single category within a specific time period.
 *
 * <p>{@code rawSpend} is the actual amount spent in the category.
 * {@code proportionOfTotal} is rawSpend / totalSpend for the period.
 * {@code score} is the 0–100 normalised score where the highest-proportion category = 100
 * and zero-spend categories = 0. Intermediate categories are scaled linearly.</p>
 */
public record CategoryScore(
        String customerId,
        String category,
        String period,          // e.g. "2025-03" for monthly, "2025-Q1" for quarterly, "2025" for yearly
        BigDecimal rawSpend,
        double proportionOfTotal,
        int score                // 0-100
) {
}
