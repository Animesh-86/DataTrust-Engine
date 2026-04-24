package io.datatrust.collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Scores tables based on the health of their upstream lineage.
 *
 * Key insight: a table is only as trustworthy as its sources.
 * If an upstream pipeline is broken or its source data is untested,
 * that risk propagates downstream.
 *
 * Scoring:
 *   - No lineage data -> neutral score (50) — we can't tell
 *   - Has upstream with good governance -> bonus
 *   - Orphan nodes (no upstream, no downstream) -> slight penalty
 */
public class LineageCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(LineageCollector.class);
    private final OpenMetadataClient client;

    // cache governance scores during a run to avoid redundant API calls
    private final Map<String, Double> governanceCache = new HashMap<>();

    public LineageCollector(OpenMetadataClient client) {
        this.client = client;
    }

    @Override
    public String name() { return "lineage"; }

    @Override
    public double collect(AssetInfo asset) {
        var lineageData = client.getLineage(asset.id());
        if (lineageData == null) return 50.0; // no lineage info = neutral

        var upstreamEdges = lineageData.path("upstreamEdges");
        var downstreamEdges = lineageData.path("downstreamEdges");
        var nodes = lineageData.path("nodes");

        boolean hasUpstream = upstreamEdges.isArray() && !upstreamEdges.isEmpty();
        boolean hasDownstream = downstreamEdges.isArray() && !downstreamEdges.isEmpty();

        if (!hasUpstream && !hasDownstream) {
            return 40.0; // orphan table — slight penalty
        }

        double score = 50.0; // baseline

        // Bonus for having documented lineage at all
        if (hasUpstream) score += 15.0;
        if (hasDownstream) score += 10.0;

        // Evaluate upstream source health
        if (hasUpstream && nodes.isArray()) {
            double upstreamHealth = evaluateUpstreamHealth(upstreamEdges, nodes);
            score += upstreamHealth * 25.0; // up to 25 extra points
        }

        return Math.min(100.0, Math.max(0.0, score));
    }

    /**
     * Check if upstream source nodes have owners and descriptions.
     * Returns 0.0 to 1.0 representing upstream health fraction.
     */
    private double evaluateUpstreamHealth(JsonNode edges, JsonNode nodes) {
        Set<String> upstreamIds = new HashSet<>();
        for (var edge : edges) {
            upstreamIds.add(edge.path("fromEntity").asText(""));
        }

        if (upstreamIds.isEmpty()) return 0.5;

        // Build a quick lookup of node metadata
        Map<String, JsonNode> nodeMap = new HashMap<>();
        for (var node : nodes) {
            nodeMap.put(node.path("id").asText(""), node);
        }

        int healthy = 0;
        int checked = 0;

        for (var id : upstreamIds) {
            var node = nodeMap.get(id);
            if (node == null) continue;
            checked++;

            boolean owned = node.has("owners") && node.get("owners").isArray()
                    && !node.get("owners").isEmpty();
            boolean described = node.has("description")
                    && !node.path("description").asText("").isBlank();

            if (owned || described) healthy++;
        }

        return checked > 0 ? (double) healthy / checked : 0.5;
    }

    @Override
    public String detail(AssetInfo asset) {
        var lineageData = client.getLineage(asset.id());
        if (lineageData == null) return "No lineage data";

        var up = lineageData.path("upstreamEdges");
        var down = lineageData.path("downstreamEdges");
        int upCount = up.isArray() ? up.size() : 0;
        int downCount = down.isArray() ? down.size() : 0;
        return upCount + " upstream, " + downCount + " downstream";
    }

    /** Clear cached scores between engine runs */
    public void resetCache() {
        governanceCache.clear();
    }
}
