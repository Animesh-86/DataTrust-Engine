package io.datatrust.collectors;

import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scores tables based on how well they're governed — ownership,
 * documentation, classification, and column-level descriptions.
 *
 * This is the "metadata hygiene" signal. A well-governed table
 * is more trustworthy because someone is actively responsible for it.
 */
public class GovernanceCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(GovernanceCollector.class);

    @Override
    public String name() { return "governance"; }

    @Override
    public double collect(AssetInfo asset) {
        double score = 0;

        // 1. Ownership (30 points) — most important governance signal
        if (asset.hasOwner()) score += 30;

        // 2. Table description (25 points)
        if (asset.hasDescription()) score += 25;

        // 3. Tier classification (20 points)
        if (asset.tier() != null) score += 20;

        // 4. Column documentation coverage (25 points)
        if (asset.totalColumns() > 0) {
            double coverage = (double) asset.describedColumns() / asset.totalColumns();
            score += coverage * 25.0;
        }

        return Math.min(100.0, score);
    }

    @Override
    public String detail(AssetInfo asset) {
        var parts = new StringBuilder();
        parts.append(asset.hasOwner() ? "✓ Owner" : "✗ No owner");
        parts.append(asset.hasDescription() ? ", ✓ Described" : ", ✗ No description");
        parts.append(asset.tier() != null ? ", ✓ " + asset.tier() : ", ✗ No tier");
        if (asset.totalColumns() > 0) {
            int pct = (int) ((double) asset.describedColumns() / asset.totalColumns() * 100);
            parts.append(", ").append(pct).append("% columns documented");
        }
        return parts.toString();
    }
}
