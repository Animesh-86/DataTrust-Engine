package io.datatrust.collectors;

import io.datatrust.model.AssetInfo;

/**
 * A signal collector produces a 0–100 sub-score for one dimension of trust.
 *
 * Each implementation focuses on a single concern (quality, governance, etc.)
 * and returns a normalized score that the aggregator can combine.
 */
public interface SignalCollector {

    /** Human-readable name for logging and UI display */
    String name();

    /**
     * Compute a score for the given asset.
     *
     * @return value between 0.0 and 100.0, or -1 if the signal
     *         is not applicable (e.g., no profiler data exists)
     */
    double collect(AssetInfo asset);

    /** Optional detail string explaining the score */
    default String detail(AssetInfo asset) {
        return null;
    }
}
