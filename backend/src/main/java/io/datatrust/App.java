package io.datatrust;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.api.TrustApiController;
import io.datatrust.engine.HistoryStore;
import io.datatrust.engine.TrustScoreEngine;
import io.datatrust.integration.OmCustomPropertyManager;
import io.datatrust.integration.OmEventSubscriber;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataTrust Engine — entry point.
 *
 * Spins up the scoring engine + REST API + serves the dashboard.
 * Deep integration: registers custom properties in OM, subscribes
 * to webhook events, and writes trust scores back to table entities.
 */
public class App {
    private static final Logger log = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        log.info("""
            
            ╔══════════════════════════════════════════╗
            ║         DataTrust Score Engine            ║
            ║   Trust scores for your data assets       ║
            ╚══════════════════════════════════════════╝
        """);

        // Config from environment (with sensible defaults for local dev)
        var omUrl = env("OM_SERVER_URL", "http://localhost:8585");
        var omUser = env("OM_USER", "admin@open-metadata.org");
        var omPassword = env("OM_PASSWORD", "admin");
        var omToken = env("OM_BOT_TOKEN", null);
        var port = Integer.parseInt(env("PORT", "4000"));
        var dbPath = env("DB_PATH", "datatrust.db");
        var interval = Integer.parseInt(env("SCORING_INTERVAL_MINUTES", "5"));
        var dashboardConfigured = env("DASHBOARD_PATH", null);
        final var dashboardPath = dashboardConfigured != null ? dashboardConfigured
                : (new java.io.File("dashboard").isDirectory() ? "dashboard" : "../dashboard");
        var engineUrl = env("ENGINE_URL", "http://localhost:" + port);

        // Build the OpenMetadata client
        OpenMetadataClient omClient;
        if (omToken != null && !omToken.isBlank()) {
            omClient = new OpenMetadataClient(omUrl, omToken);
        } else {
            omClient = new OpenMetadataClient(omUrl, omUser, omPassword);
        }

        // Verify connection
        if (omClient.isHealthy()) {
            log.info("Connected to OpenMetadata {} at {}", omClient.getServerVersion(), omUrl);
        } else {
            log.warn("OpenMetadata at {} is not reachable — scores will retry on next cycle", omUrl);
        }

        // History store (SQLite)
        var store = new HistoryStore(dbPath);

        // Alert Manager for tasks and slack
        var slackWebhook = env("SLACK_WEBHOOK_URL", null);
        var criticalThreshold = Double.parseDouble(env("CRITICAL_SCORE_THRESHOLD", "50.0"));
        var alertManager = new io.datatrust.integration.AlertManager(omClient, slackWebhook, criticalThreshold);

        // Scoring engine
        var engine = new TrustScoreEngine(omClient, store, alertManager, interval);

        // ==== Deep Integration ====

        // 1. Custom Property Manager — register trust score properties on table type
        var propertyManager = new OmCustomPropertyManager(omClient);
        try {
            propertyManager.ensureCustomProperties();
            engine.setPropertyManager(propertyManager);
            log.info("Custom properties writeback enabled");
        } catch (Exception e) {
            log.warn("Custom property registration failed (scoring continues): {}", e.getMessage());
        }

        // 2. Event Subscriber — webhook for real-time rescoring
        var eventSubscriber = new OmEventSubscriber(omClient, engineUrl);
        try {
            eventSubscriber.subscribe();
            log.info("Webhook event subscription active → {}", eventSubscriber.getCallbackUrl());
        } catch (Exception e) {
            log.warn("Event subscription failed (polling continues): {}", e.getMessage());
        }

        // JSON mapper configured for Java time types
        var mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

        // HTTP server
        var app = Javalin.create(config -> {
            config.jsonMapper(new JavalinJackson(mapper));
            config.staticFiles.add(dashboardPath, io.javalin.http.staticfiles.Location.EXTERNAL);
            config.plugins.enableCors(cors -> cors.add(rule -> {
                rule.anyHost();
            }));
        });

        // Register API routes (with integration references)
        var api = new TrustApiController(engine, omUrl);
        api.setPropertyManager(propertyManager);
        api.setEventSubscriber(eventSubscriber);
        api.register(app);

        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down...");
            engine.shutdown();
            app.stop();
        }));

        // GO
        app.start(port);
        engine.start();

        log.info("Dashboard:    http://localhost:{}", port);
        log.info("API:          http://localhost:{}/api/scores", port);
        log.info("Integration:  http://localhost:{}/api/integration/status", port);
        log.info("Webhook:      http://localhost:{}/api/webhook/om", port);
    }

    private static String env(String key, String fallback) {
        var val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : fallback;
    }
}
