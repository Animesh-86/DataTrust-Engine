package io.datatrust.collectors;

import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Scores tables based on how well they're governed — ownership,
 * documentation, classification, domain assignment, glossary terms,
 * and column-level descriptions.
 *
 * This is the "metadata hygiene" signal. A well-governed table
 * is more trustworthy because someone is actively responsible for it.
 *
 * Deep OM Integration: leverages owners, tags, tiers, domains,
 * glossary terms, and community votes from the OM API.
 */
public class GovernanceCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(GovernanceCollector.class);

    @Override
    public String name() { return "governance"; }

    @Override
    public double collect(AssetInfo asset) {
        double score = 0;

        // 1. Ownership (25 points) — most important governance signal
        if (asset.hasOwner()) score += 25;

        // 2. Table description (20 points)
        if (asset.hasDescription()) score += 20;

        // 3. Tier classification (15 points)
        if (asset.tier() != null) score += 15;

        // 4. Column documentation coverage (20 points)
        if (asset.totalColumns() > 0) {
            double coverage = (double) asset.describedColumns() / asset.totalColumns();
            score += coverage * 20.0;
        }

        // 5. Domain assignment (8 points) — OM Governance feature
        if (asset.hasDomain()) score += 8;

        // 6. Glossary terms applied (7 points) — OM Governance feature
        if (asset.hasGlossaryTerms()) score += 7;

        // 7. Community votes / engagement (5 points) — OM social feature
        if (asset.voteCount() > 0) score += Math.min(5, asset.voteCount());

        return Math.min(100.0, score);
    }

    @Override
    public String detail(AssetInfo asset) {
        var parts = new StringBuilder();
        parts.append(asset.hasOwner() ? "✓ Owner" : "✗ No owner");
        parts.append(asset.hasDescription() ? ", ✓ Described" : ", ✗ No description");
        parts.append(asset.tier() != null ? ", ✓ " + asset.tier() : ", ✗ No tier");
        parts.append(asset.hasDomain() ? ", ✓ Domain" : ", ✗ No domain");
        parts.append(asset.hasGlossaryTerms() ? ", ✓ Glossary" : ", ✗ No glossary");
        if (asset.totalColumns() > 0) {
            int pct = (int) ((double) asset.describedColumns() / asset.totalColumns() * 100);
            parts.append(", ").append(pct).append("% cols documented");
        }
        if (asset.voteCount() > 0) {
            parts.append(", ").append(asset.voteCount()).append(" votes");
        }
        return parts.toString();
    }
}
