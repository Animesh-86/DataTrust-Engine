package io.datatrust.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.time.Instant;

/**
 * Snapshot of a computed trust score for a single data asset.
 * Immutable by design — once computed, a score is a historical fact.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record TrustScore(
    String tableId,
    String fullyQualifiedName,
    String displayName,
    double overallScore,
    SignalBreakdown breakdown,
    String grade,
    Instant computedAt
) {
    public TrustScore(String tableId, String fqn, String displayName,
                      double score, SignalBreakdown breakdown) {
        this(tableId, fqn, displayName, score, breakdown,
             gradeFromScore(score), Instant.now());
    }

    private static String gradeFromScore(double score) {
        if (score >= 90) return "A";
        if (score >= 75) return "B";
        if (score >= 60) return "C";
        if (score >= 40) return "D";
        return "F";
    }
}
