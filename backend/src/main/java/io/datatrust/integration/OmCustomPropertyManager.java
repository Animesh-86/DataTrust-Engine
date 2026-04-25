package io.datatrust.integration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.TrustScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Manages custom property definitions on the "table" entity type
 * in OpenMetadata, and writes computed trust scores back to each
 * table's extension data — so scores show up inside the OM UI.
 */
public class OmCustomPropertyManager {
    private static final Logger log = LoggerFactory.getLogger(OmCustomPropertyManager.class);

    private final OpenMetadataClient client;
    private final ObjectMapper mapper = new ObjectMapper();

    // Custom property names we register on the table type
    private static final String PROP_TRUST_SCORE = "trustScore";
    private static final String PROP_TRUST_GRADE = "trustGrade";
    private static final String PROP_LAST_SCORED = "trustLastScored";

    private String tableTypeId;
    private boolean propertiesReady = false;
    private int lastWritebackCount = 0;
    private String lastWritebackTime = "never";

    public OmCustomPropertyManager(OpenMetadataClient client) {
        this.client = client;
    }

    /**
     * On startup, ensure our custom properties exist on the "table" type.
     * Uses PUT to be idempotent — safe to call on every boot.
     */
    public void ensureCustomProperties() {
        try {
            // Fetch the table type definition
            var tableType = client.getTypeByName("table");
            if (tableType == null) {
                log.warn("Could not find 'table' entity type — custom properties skipped");
                return;
            }
            tableTypeId = tableType.path("id").asText();
            log.info("Table entity type ID: {}", tableTypeId);

            // Register each property
            ensureProperty(tableTypeId, PROP_TRUST_SCORE,
                    "Trust Score computed by DataTrust Engine (0–100)",
                    "integer");

            ensureProperty(tableTypeId, PROP_TRUST_GRADE,
                    "Trust Grade: A (≥90), B (75–89), C (60–74), D (40–59), F (<40)",
                    "string");

            ensureProperty(tableTypeId, PROP_LAST_SCORED,
                    "Timestamp of last trust score computation",
                    "string");

            propertiesReady = true;
            log.info("Custom properties registered on table entity type");
        } catch (Exception e) {
            log.warn("Failed to register custom properties (non-fatal): {}", e.getMessage());
        }
    }

    private void ensureProperty(String typeId, String name, String description, String propertyType) {
        try {
            // Find the property type reference
            var propTypeRef = client.getPropertyType(propertyType);
            if (propTypeRef == null) {
                log.warn("Property type '{}' not found, skipping custom property '{}'", propertyType, name);
                return;
            }

            var payload = mapper.createObjectNode()
                    .put("name", name)
                    .put("description", description);

            var typeRef = payload.putObject("propertyType");
            typeRef.put("id", propTypeRef.path("id").asText());
            typeRef.put("type", "type");

            client.addCustomProperty(typeId, payload);
            log.info("Ensured custom property: {}", name);
        } catch (Exception e) {
            log.debug("Custom property '{}' may already exist: {}", name, e.getMessage());
        }
    }

    /**
     * After a scoring cycle, write each score back to the corresponding
     * table entity in OpenMetadata via JSON Patch on the extension field.
     */
    public void writeBackScores(List<TrustScore> scores) {
        if (!propertiesReady || tableTypeId == null) {
            log.debug("Custom properties not ready — skipping writeback");
            return;
        }

        int success = 0;
        int failed = 0;

        for (var score : scores) {
            try {
                var extensionData = mapper.createObjectNode();
                extensionData.put(PROP_TRUST_SCORE, (int) Math.round(score.overallScore()));
                extensionData.put(PROP_TRUST_GRADE, score.grade());
                extensionData.put(PROP_LAST_SCORED, score.computedAt().toString());

                client.patchTableExtension(score.tableId(), extensionData);
                success++;
            } catch (Exception e) {
                failed++;
                log.debug("Writeback failed for {}: {}",
                        score.fullyQualifiedName(), e.getMessage());
            }
        }

        lastWritebackCount = success;
        lastWritebackTime = java.time.Instant.now().toString();
        log.info("Writeback complete: {}/{} tables updated in OM",
                success, scores.size());
    }

    public boolean isReady() { return propertiesReady; }
    public int getLastWritebackCount() { return lastWritebackCount; }
    public String getLastWritebackTime() { return lastWritebackTime; }
}
