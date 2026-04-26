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
        app.get("/api/scores/{fqn}/detail", this::getDetail);

        // Engine control
        app.post("/api/engine/run", this::triggerRun);
        app.post("/api/engine/simulate-incident", this::simulateIncident);
        app.get("/api/engine/status", this::getStatus);
        app.get("/api/engine/weights", this::getWeights);

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

    private void simulateIncident(Context ctx) {
        var incident = engine.simulateIncident();
        if (incident != null) {
            ctx.json(Map.of("status", "triggered", "message", "Incident simulated on " + incident.displayName()));
        } else {
            ctx.status(400).json(Map.of("error", "No tables available"));
        }
    }

    private void getStatus(Context ctx) {
        var status = engine.getEngineStatus();
        ctx.json(status);
    }

    private void getWeights(Context ctx) {
        ctx.json(engine.getEngineStatus().get("weights"));
    }

    /**
     * Rich detail endpoint for the asset detail panel.
     * Returns score breakdown, governance checklist, lineage summary,
     * quality details, and actionable recommendations.
     */
    private void getDetail(Context ctx) {
        var fqn = ctx.pathParam("fqn").replace("%2E", ".");
        var score = engine.getScoreForTable(fqn);
        if (score == null) {
            ctx.status(404).json(Map.of("error", "Not found"));
            return;
        }

        var detail = new LinkedHashMap<String, Object>();
        detail.put("fqn", fqn);
        detail.put("displayName", score.displayName());
        detail.put("overallScore", score.overallScore());
        detail.put("grade", score.grade());
        detail.put("breakdown", score.breakdown());
        detail.put("computedAt", score.computedAt().toString());
        detail.put("omLink", omServerUrl + "/table/" + fqn);

        // Parse governance detail into checklist items
        var govDetail = score.breakdown().governanceDetail();
        if (govDetail != null) {
            var checks = new java.util.ArrayList<Map<String, Object>>();
            for (var part : govDetail.split(", ")) {
                var passed = part.startsWith("✓");
                checks.add(Map.of(
                        "label", part.replaceAll("^[✓✗] ", ""),
                        "passed", passed
                ));
            }
            detail.put("governanceChecks", checks);
        }

        // Build recommendations
        var recommendations = new java.util.ArrayList<Map<String, Object>>();
        if (govDetail != null && govDetail.contains("✗ No owner"))
            recommendations.add(Map.of("action", "Assign an owner", "impact", "+25 pts", "priority", "high"));
        if (govDetail != null && govDetail.contains("✗ No description"))
            recommendations.add(Map.of("action", "Add a table description", "impact", "+20 pts", "priority", "high"));
        if (govDetail != null && govDetail.contains("✗ No tier"))
            recommendations.add(Map.of("action", "Add a tier classification", "impact", "+15 pts", "priority", "medium"));
        if (govDetail != null && govDetail.contains("✗ No domain"))
            recommendations.add(Map.of("action", "Assign to a domain", "impact", "+8 pts", "priority", "medium"));
        if (govDetail != null && govDetail.contains("✗ No glossary"))
            recommendations.add(Map.of("action", "Apply glossary terms", "impact", "+7 pts", "priority", "low"));

        var qualDetail = score.breakdown().qualityDetail();
        if (qualDetail != null && qualDetail.contains("No tests"))
            recommendations.add(Map.of("action", "Add data quality tests", "impact", "+15 pts", "priority", "high"));

        detail.put("recommendations", recommendations);

        // History summary
        var history = engine.getHistory(fqn, 10);
        if (!history.isEmpty()) {
            var latest = history.get(0);
            var oldest = history.get(history.size() - 1);
            double trend = latest.overallScore() - oldest.overallScore();
            detail.put("trend", Map.of(
                    "direction", trend > 0 ? "improving" : trend < 0 ? "declining" : "stable",
                    "delta", Math.round(trend * 10.0) / 10.0,
                    "dataPoints", history.size()
            ));
        }

        ctx.json(detail);
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
