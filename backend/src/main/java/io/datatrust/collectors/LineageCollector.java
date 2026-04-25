package io.datatrust.collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Scores tables based on lineage graph connectivity AND upstream trust propagation.
 *
 * Key insight: a table is only as trustworthy as its sources.
 * If upstream tables have low governance or broken tests, that risk
 * propagates downstream through the lineage graph.
 *
 * This is the deepest OM integration — it traverses the lineage graph
 * using the /api/v1/lineage endpoint with upstream depth, inspects
 * each upstream node's governance signals, and propagates trust penalties.
 *
 * Scoring dimensions:
 *   1. Connectivity (25 pts) — does the table have documented lineage?
 *   2. Upstream Health (35 pts) — are upstream tables well-governed?
 *   3. Pipeline Coverage (20 pts) — are upstream edges from pipelines?
 *   4. Depth Penalty (20 pts) — deeper lineage chains = more risk
 */
public class LineageCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(LineageCollector.class);
    private final OpenMetadataClient client;

    // Cache upstream health scores within a scoring cycle to avoid redundant API calls
    private final Map<String, Double> upstreamHealthCache = new HashMap<>();

    public LineageCollector(OpenMetadataClient client) {
        this.client = client;
    }

    @Override
    public String name() { return "lineage"; }

    @Override
    public double collect(AssetInfo asset) {
        var lineageData = client.getLineage(asset.id());
        if (lineageData == null) return 50.0; // no lineage API response = neutral

        var upstreamEdges = lineageData.path("upstreamEdges");
        var downstreamEdges = lineageData.path("downstreamEdges");
        var nodes = lineageData.path("nodes");

        boolean hasUpstream = upstreamEdges.isArray() && !upstreamEdges.isEmpty();
        boolean hasDownstream = downstreamEdges.isArray() && !downstreamEdges.isEmpty();

        if (!hasUpstream && !hasDownstream) {
            return 40.0; // orphan table — no lineage documented
        }

        double score = 0;

        // === 1. Connectivity Score (up to 25 points) ===
        score += computeConnectivityScore(hasUpstream, hasDownstream);

        // === 2. Upstream Health — Trust Propagation (up to 35 points) ===
        if (hasUpstream && nodes.isArray()) {
            score += computeUpstreamTrustScore(upstreamEdges, nodes) * 35.0;
        } else if (!hasUpstream) {
            // Source table (no upstream) — gets full marks for being a root
            score += 30.0;
        }

        // === 3. Pipeline Coverage (up to 20 points) ===
        score += computePipelineCoverage(upstreamEdges, downstreamEdges);

        // === 4. Depth Bonus/Penalty (up to 20 points) ===
        score += computeDepthScore(upstreamEdges, nodes);

        return Math.min(100.0, Math.max(0.0, score));
    }

    /**
     * Score based on whether the table has documented upstream/downstream connections.
     */
    private double computeConnectivityScore(boolean hasUpstream, boolean hasDownstream) {
        if (hasUpstream && hasDownstream) return 25.0; // fully connected
        if (hasUpstream) return 20.0; // has sources documented
        if (hasDownstream) return 18.0; // is consumed downstream
        return 5.0;
    }

    /**
     * Trust Propagation: evaluates the governance health of upstream tables.
     *
     * This is the key differentiator — instead of just checking "does lineage exist",
     * we actually inspect each upstream source's metadata quality (owner, description,
     * tags) to determine if the upstream data is trustworthy.
     *
     * Returns 0.0 to 1.0 representing upstream trust fraction.
     */
    private double computeUpstreamTrustScore(JsonNode edges, JsonNode nodes) {
        Set<String> upstreamIds = new HashSet<>();
        for (var edge : edges) {
            upstreamIds.add(edge.path("fromEntity").asText(""));
        }

        if (upstreamIds.isEmpty()) return 0.5;

        // Build node lookup
        Map<String, JsonNode> nodeMap = new HashMap<>();
        for (var node : nodes) {
            nodeMap.put(node.path("id").asText(""), node);
        }

        int total = 0;
        double totalHealth = 0;

        for (var id : upstreamIds) {
            // Check cache first
            if (upstreamHealthCache.containsKey(id)) {
                totalHealth += upstreamHealthCache.get(id);
                total++;
                continue;
            }

            var node = nodeMap.get(id);
            if (node == null) continue;
            total++;

            double health = 0;

            // Does upstream have an owner?
            boolean owned = node.has("owners") && node.get("owners").isArray()
                    && !node.get("owners").isEmpty();
            if (owned) health += 0.35;

            // Does upstream have a description?
            boolean described = node.has("description")
                    && !node.path("description").asText("").isBlank();
            if (described) health += 0.25;

            // Does upstream have tags/classification?
            boolean tagged = node.has("tags") && node.get("tags").isArray()
                    && !node.get("tags").isEmpty();
            if (tagged) health += 0.2;

            // Does upstream have a tier?
            if (tagged) {
                for (var tag : node.get("tags")) {
                    if (tag.path("tagFQN").asText("").startsWith("Tier.")) {
                        health += 0.1;
                        break;
                    }
                }
            }

            // Does upstream have profile data? (data is being monitored)
            boolean profiled = node.has("profile") && !node.get("profile").isNull();
            if (profiled) health += 0.1;

            health = Math.min(1.0, health);
            upstreamHealthCache.put(id, health);
            totalHealth += health;
        }

        return total > 0 ? totalHealth / total : 0.5;
    }

    /**
     * Checks if lineage edges come from pipelines (automated) vs manual.
     * Pipeline-documented lineage is more trustworthy.
     */
    private double computePipelineCoverage(JsonNode upEdges, JsonNode downEdges) {
        int totalEdges = 0;
        int pipelineEdges = 0;

        for (var edge : upEdges) {
            totalEdges++;
            if (hasPipelineInfo(edge)) pipelineEdges++;
        }
        for (var edge : downEdges) {
            totalEdges++;
            if (hasPipelineInfo(edge)) pipelineEdges++;
        }

        if (totalEdges == 0) return 10.0;

        double ratio = (double) pipelineEdges / totalEdges;
        return 10.0 + ratio * 10.0; // 10-20 points
    }

    private boolean hasPipelineInfo(JsonNode edge) {
        return edge.has("pipeline") && !edge.get("pipeline").isNull();
    }

    /**
     * Score based on lineage depth — very deep chains are riskier
     * because errors compound. Moderate depth is ideal.
     */
    private double computeDepthScore(JsonNode upEdges, JsonNode nodes) {
        int upCount = upEdges.isArray() ? upEdges.size() : 0;
        int nodeCount = nodes.isArray() ? nodes.size() : 0;

        if (nodeCount <= 1) return 15.0; // simple, direct lineage
        if (nodeCount <= 3) return 20.0; // moderate depth — ideal
        if (nodeCount <= 6) return 15.0; // getting complex
        return 10.0; // very deep chain — risk compounds
    }

    @Override
    public String detail(AssetInfo asset) {
        var lineageData = client.getLineage(asset.id());
        if (lineageData == null) return "No lineage data";

        var up = lineageData.path("upstreamEdges");
        var down = lineageData.path("downstreamEdges");
        var nodes = lineageData.path("nodes");
        int upCount = up.isArray() ? up.size() : 0;
        int downCount = down.isArray() ? down.size() : 0;
        int nodeCount = nodes.isArray() ? nodes.size() : 0;

        StringBuilder sb = new StringBuilder();
        sb.append(upCount).append(" upstream, ").append(downCount).append(" downstream");

        if (upCount > 0 && nodes.isArray()) {
            double trustPropagation = computeUpstreamTrustScore(up, nodes);
            int pct = (int) (trustPropagation * 100);
            sb.append(", ").append(pct).append("% upstream health");
        }

        sb.append(", ").append(nodeCount).append(" total nodes");
        return sb.toString();
    }

    /** Clear cached scores between engine runs */
    public void resetCache() {
        upstreamHealthCache.clear();
    }
}
