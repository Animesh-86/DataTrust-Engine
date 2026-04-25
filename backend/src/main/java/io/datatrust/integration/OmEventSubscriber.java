package io.datatrust.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datatrust.api.OpenMetadataClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers a webhook-based Event Subscription in OpenMetadata
 * so the engine receives real-time notifications when table metadata
 * changes — enabling event-driven rescoring instead of blind polling.
 */
public class OmEventSubscriber {
    private static final Logger log = LoggerFactory.getLogger(OmEventSubscriber.class);

    private static final String SUBSCRIPTION_NAME = "datatrust-engine-webhook";

    private final OpenMetadataClient client;
    private final String callbackUrl;
    private final ObjectMapper mapper = new ObjectMapper();

    private boolean subscribed = false;
    private String subscriptionId = null;

    public OmEventSubscriber(OpenMetadataClient client, String engineBaseUrl) {
        this.client = client;
        this.callbackUrl = engineBaseUrl + "/api/webhook/om";
    }

    /**
     * Register (or verify existing) event subscription for table events.
     * Idempotent — checks if subscription already exists.
     */
    public void subscribe() {
        try {
            // Check if subscription already exists
            var existing = client.getEventSubscription(SUBSCRIPTION_NAME);
            if (existing != null) {
                subscriptionId = existing.path("id").asText();
                subscribed = true;
                log.info("Event subscription '{}' already exists (id={})",
                        SUBSCRIPTION_NAME, subscriptionId);
                return;
            }

            // Create new subscription
            var payload = buildSubscriptionPayload();
            var result = client.createEventSubscription(payload);

            if (result != null) {
                subscriptionId = result.path("id").asText();
                subscribed = true;
                log.info("Registered webhook event subscription '{}' → {}",
                        SUBSCRIPTION_NAME, callbackUrl);
            }
        } catch (Exception e) {
            log.warn("Failed to register event subscription (non-fatal): {}", e.getMessage());
        }
    }

    private ObjectNode buildSubscriptionPayload() {
        var payload = mapper.createObjectNode();
        payload.put("name", SUBSCRIPTION_NAME);
        payload.put("alertType", "Notification");
        payload.put("enabled", true);
        payload.put("batchSize", 10);

        // Trigger config — real-time
        var trigger = payload.putObject("trigger");
        trigger.put("triggerType", "RealTime");

        // Resources at top level (OM 1.12.5 format)
        var resources = payload.putArray("resources");
        resources.add("table");

        // Destination — webhook to our callback
        var destinations = payload.putArray("destinations");
        var dest = destinations.addObject();
        dest.put("category", "External");
        dest.put("type", "Webhook");
        dest.put("enabled", true);
        dest.put("timeout", 10);

        var config = dest.putObject("config");
        config.put("endpoint", callbackUrl);

        return payload;
    }

    public boolean isSubscribed() { return subscribed; }
    public String getSubscriptionId() { return subscriptionId; }
    public String getCallbackUrl() { return callbackUrl; }
}
