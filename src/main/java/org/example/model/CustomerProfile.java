package org.example.model;

import java.util.List;
import java.util.Map;

/**
 * Aggregated profile for a customer over a given time window.
 * The {@code scores} map is category → score (0–100).
 */
public record CustomerProfile(
        String customerId,
        String period,
        Map<String, Integer> scores   // category → 0-100 score
) {
    /**
     * Returns the score for a category, defaulting to 0 if absent.
     */
    public int scoreFor(String category) {
        return scores.getOrDefault(category, 0);
    }
}
