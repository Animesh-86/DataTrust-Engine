package io.datatrust;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.api.TrustApiController;
import io.datatrust.engine.HistoryStore;
import io.datatrust.engine.TrustScoreEngine;
import io.javalin.Javalin;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataTrust Engine — entry point.
 *
 * Spins up the scoring engine + REST API + serves the dashboard.
 * Configuration via environment variables for Docker-friendliness.
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
        var omUser = env("OM_USER", "admin");
        var omPassword = env("OM_PASSWORD", "admin");
        var omToken = env("OM_BOT_TOKEN", null);
        var port = Integer.parseInt(env("PORT", "4000"));
        var dbPath = env("DB_PATH", "datatrust.db");
        var interval = Integer.parseInt(env("SCORING_INTERVAL_MINUTES", "5"));
        var dashboardPath = env("DASHBOARD_PATH", "../dashboard");

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

        // Scoring engine
        var engine = new TrustScoreEngine(omClient, store, interval);

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

        // Register API routes
        var api = new TrustApiController(engine, omUrl);
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

        log.info("Dashboard: http://localhost:{}", port);
        log.info("API:       http://localhost:{}/api/scores", port);
    }

    private static String env(String key, String fallback) {
        var val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : fallback;
    }
}
