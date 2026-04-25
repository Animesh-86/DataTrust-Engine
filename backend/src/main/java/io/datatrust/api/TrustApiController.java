package io.datatrust.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.datatrust.engine.TrustScoreEngine;
import io.datatrust.integration.OmCustomPropertyManager;
import io.datatrust.integration.OmEventSubscriber;
import io.javalin.Javalin;
import io.javalin.http.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST API exposing trust scores, engine controls, integration status,
 * and the OpenMetadata webhook callback endpoint.
 */
public class TrustApiController {
    private static final Logger log = LoggerFactory.getLogger(TrustApiController.class);

    private final TrustScoreEngine engine;
    private final String omServerUrl;
    private OmCustomPropertyManager propertyManager;
    private OmEventSubscriber eventSubscriber;

    public TrustApiController(TrustScoreEngine engine, String omServerUrl) {
        this.engine = engine;
        this.omServerUrl = omServerUrl;
    }

    public void setPropertyManager(OmCustomPropertyManager mgr) { this.propertyManager = mgr; }
    public void setEventSubscriber(OmEventSubscriber sub) { this.eventSubscriber = sub; }

    public void register(Javalin app) {
        // Score endpoints
        app.get("/api/scores", this::getAllScores);
        app.get("/api/scores/stats", this::getStats);
        app.get("/api/scores/{fqn}", this::getScoreByFqn);
        app.get("/api/scores/{fqn}/history", this::getHistory);

        // Engine control
        app.post("/api/engine/run", this::triggerRun);
        app.get("/api/engine/status", this::getStatus);

        // Health check
        app.get("/api/health", this::health);

        // Deep Integration endpoints
        app.post("/api/webhook/om", this::handleOmWebhook);
        app.get("/api/integration/status", this::integrationStatus);
        app.get("/api/config", this::getConfig);

        log.info("API routes registered (including webhook + integration endpoints)");
    }

    private void getAllScores(Context ctx) {
        ctx.json(engine.getLatestScores());
    }

    private void getStats(Context ctx) {
        ctx.json(engine.getStats());
    }

    private void getScoreByFqn(Context ctx) {
        var fqn = ctx.pathParam("fqn");
        fqn = fqn.replace("%2E", ".");
        var score = engine.getScoreForTable(fqn);
        if (score == null) {
            ctx.status(404).json(Map.of("error", "No score found for " + fqn));
        } else {
            ctx.json(score);
        }
    }

    private void getHistory(Context ctx) {
        var fqn = ctx.pathParam("fqn");
        fqn = fqn.replace("%2E", ".");
        int limit = ctx.queryParamAsClass("limit", Integer.class).getOrDefault(50);
        ctx.json(engine.getHistory(fqn, limit));
    }

    private void triggerRun(Context ctx) {
        engine.runOnce();
        ctx.json(Map.of("status", "triggered", "message", "Scoring cycle started"));
    }

    private void getStatus(Context ctx) {
        var status = engine.getEngineStatus();
        ctx.json(status);
    }

    private void health(Context ctx) {
        ctx.json(Map.of(
                "status", "up",
                "service", "datatrust-engine",
                "omServer", omServerUrl
        ));
    }

    /**
     * Receives webhook events from OpenMetadata when table metadata changes.
     * Parses the entity FQN from the event and triggers a targeted rescore.
     */
    private void handleOmWebhook(Context ctx) {
        try {
            var body = ctx.body();
            log.info("Received OM webhook event ({} bytes)", body.length());

            var mapper = new ObjectMapper();
            var event = mapper.readTree(body);

            // OM sends events as arrays or single objects
            if (event.isArray()) {
                for (var item : event) {
                    processWebhookEvent(item);
                }
            } else {
                processWebhookEvent(event);
            }

            ctx.status(200).json(Map.of("status", "processed"));
        } catch (Exception e) {
            log.warn("Webhook processing error: {}", e.getMessage());
            ctx.status(200).json(Map.of("status", "received"));
        }
    }

    private void processWebhookEvent(com.fasterxml.jackson.databind.JsonNode event) {
        var entityType = event.path("entityType").asText("");
        var eventType = event.path("eventType").asText("");
        var entityFqn = event.path("entityFullyQualifiedName").asText(
                event.path("entity").path("fullyQualifiedName").asText(""));

        log.info("OM Event: type={}, entity={}, fqn={}", eventType, entityType, entityFqn);

        if ("table".equalsIgnoreCase(entityType)) {
            log.info("Table metadata changed — triggering rescore for: {}", entityFqn);
            engine.runOnce();
        }
    }

    /**
     * Returns the current integration status — custom properties,
     * webhook subscription, writeback health.
     */
    private void integrationStatus(Context ctx) {
        var status = new LinkedHashMap<String, Object>();

        status.put("omServer", omServerUrl);

        // Custom property writeback status
        var propStatus = new LinkedHashMap<String, Object>();
        if (propertyManager != null) {
            propStatus.put("ready", propertyManager.isReady());
            propStatus.put("lastWritebackCount", propertyManager.getLastWritebackCount());
            propStatus.put("lastWritebackTime", propertyManager.getLastWritebackTime());
        } else {
            propStatus.put("ready", false);
        }
        status.put("customProperties", propStatus);

        // Webhook subscription status
        var webhookStatus = new LinkedHashMap<String, Object>();
        if (eventSubscriber != null) {
            webhookStatus.put("subscribed", eventSubscriber.isSubscribed());
            webhookStatus.put("subscriptionId", eventSubscriber.getSubscriptionId());
            webhookStatus.put("callbackUrl", eventSubscriber.getCallbackUrl());
        } else {
            webhookStatus.put("subscribed", false);
        }
        status.put("webhook", webhookStatus);

        ctx.json(status);
    }

    /**
     * Expose config to the dashboard (OM server URL for deep links etc.)
     */
    private void getConfig(Context ctx) {
        ctx.json(Map.of(
                "omServerUrl", omServerUrl,
                "service", "datatrust-engine"
        ));
    }
}
