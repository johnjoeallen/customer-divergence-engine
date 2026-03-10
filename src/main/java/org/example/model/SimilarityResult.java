package org.example.model;

import java.util.Map;

/**
 * Result of a similarity comparison between two customers.
 *
 * @param customerId        the compared customer
 * @param overallDistance    Euclidean distance across the selected categories (lower = more similar)
 * @param overallSimilarity 0.0–1.0 cosine-style similarity (higher = more similar)
 * @param categoryDistances per-category absolute score difference
 */
public record SimilarityResult(
        String customerId,
        double overallDistance,
        double overallSimilarity,
        Map<String, Integer> categoryDistances
) {
}
