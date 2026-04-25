package io.datatrust.model;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Lightweight view of an OpenMetadata table entity.
 * Extracts all governance-relevant signals from the raw API payload
 * including deep-integration features: domain, glossary, votes.
 */
public record AssetInfo(
    String id,
    String fullyQualifiedName,
    String displayName,
    String serviceType,
    boolean hasOwner,
    boolean hasDescription,
    String tier,
    int totalColumns,
    int describedColumns,
    boolean hasDomain,
    boolean hasGlossaryTerms,
    int voteCount,
    JsonNode profileData,
    JsonNode rawJson
) {
    /**
     * Parse from raw OM API response node.
     * Defensive parsing — missing fields default to safe values.
     */
    public static AssetInfo fromJson(JsonNode node) {
        var id = node.path("id").asText("");
        var fqn = node.path("fullyQualifiedName").asText("");
        var name = node.has("displayName") && !node.get("displayName").isNull()
                ? node.get("displayName").asText()
                : node.path("name").asText(fqn);

        var serviceType = node.path("serviceType").asText("unknown");
        var hasOwner = node.has("owners") && node.get("owners").isArray()
                && !node.get("owners").isEmpty();
        var hasDesc = node.has("description")
                && !node.path("description").asText("").isBlank();

        // Tier is nested in tags
        String tier = null;
        boolean hasGlossary = false;
        if (node.has("tags") && node.get("tags").isArray()) {
            for (var tag : node.get("tags")) {
                var tagFqn = tag.path("tagFQN").asText("");
                if (tagFqn.startsWith("Tier.")) {
                    tier = tagFqn;
                }
                // Glossary terms have source "Glossary"
                var source = tag.path("source").asText("");
                if ("Glossary".equalsIgnoreCase(source)) {
                    hasGlossary = true;
                }
            }
        }

        // Column description coverage
        int totalCols = 0, describedCols = 0;
        if (node.has("columns") && node.get("columns").isArray()) {
            for (var col : node.get("columns")) {
                totalCols++;
                var colDesc = col.path("description").asText("");
                if (!colDesc.isBlank()) describedCols++;
            }
        }

        // Domain assignment (OM governance feature)
        boolean hasDomain = node.has("domain") && !node.get("domain").isNull()
                && node.get("domain").has("id");

        // Community votes (OM social feature)
        int votes = 0;
        if (node.has("votes")) {
            var votesNode = node.get("votes");
            votes = votesNode.path("upVotes").asInt(0);
        }

        var profile = node.has("profile") ? node.get("profile") : null;

        return new AssetInfo(id, fqn, name, serviceType,
                hasOwner, hasDesc, tier, totalCols, describedCols,
                hasDomain, hasGlossary, votes,
                profile, node);
    }
}
