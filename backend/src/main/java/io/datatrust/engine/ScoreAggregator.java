package io.datatrust.engine;

import io.datatrust.model.SignalBreakdown;
import io.datatrust.model.TrustScore;

import java.util.Map;

/**
 * The math behind the trust score.
 *
 * Combines individual signal scores using configurable weights.
 * Weights must sum to 1.0 — this is validated at construction time.
 */
public class ScoreAggregator {
    private final double wQuality;
    private final double wGovernance;
    private final double wLineage;
    private final double wFreshness;

    public ScoreAggregator() {
        // defaults tuned for data platform environments
        this(0.35, 0.25, 0.25, 0.15);
    }

    public ScoreAggregator(double wQuality, double wGovernance,
                           double wLineage, double wFreshness) {
        double sum = wQuality + wGovernance + wLineage + wFreshness;
        if (Math.abs(sum - 1.0) > 0.001) {
            throw new IllegalArgumentException(
                    "Weights must sum to 1.0, got " + sum);
        }
        this.wQuality = wQuality;
        this.wGovernance = wGovernance;
        this.wLineage = wLineage;
        this.wFreshness = wFreshness;
    }

    public TrustScore compute(String tableId, String fqn, String displayName,
                              Map<String, Double> signals,
                              Map<String, String> details) {
        double qScore = signals.getOrDefault("quality", 0.0);
        double gScore = signals.getOrDefault("governance", 0.0);
        double lScore = signals.getOrDefault("lineage", 50.0);
        double fScore = signals.getOrDefault("freshness", 0.0);

        double overall = (qScore * wQuality)
                       + (gScore * wGovernance)
                       + (lScore * wLineage)
                       + (fScore * wFreshness);

        // Clamp to valid range
        overall = Math.round(overall * 10.0) / 10.0;
        overall = Math.min(100.0, Math.max(0.0, overall));

        var breakdown = new SignalBreakdown(
                round(qScore), round(gScore), round(lScore), round(fScore),
                details.get("quality"), details.get("governance"),
                details.get("lineage"), details.get("freshness")
        );

        return new TrustScore(tableId, fqn, displayName, overall, breakdown);
    }

    public Map<String, Double> getWeights() {
        return Map.of(
                "quality", wQuality,
                "governance", wGovernance,
                "lineage", wLineage,
                "freshness", wFreshness
        );
    }

    private double round(double v) {
        return Math.round(v * 10.0) / 10.0;
    }
}
