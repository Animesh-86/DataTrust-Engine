package io.datatrust.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Holds individual signal scores that feed into the overall trust calculation.
 * Each value is normalized to 0–100.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record SignalBreakdown(
    double quality,
    double governance,
    double lineage,
    double freshness,
    String qualityDetail,
    String governanceDetail,
    String lineageDetail,
    String freshnessDetail
) {
    public SignalBreakdown(double quality, double governance,
                           double lineage, double freshness) {
        this(quality, governance, lineage, freshness,
             null, null, null, null);
    }
}
