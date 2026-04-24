package io.datatrust.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.datatrust.engine.TrustScoreEngine;
import io.javalin.Javalin;
import io.javalin.http.Context;
import io.javalin.json.JavalinJackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * REST API exposing trust scores to the dashboard and any other consumer.
 */
public class TrustApiController {
    private static final Logger log = LoggerFactory.getLogger(TrustApiController.class);

    private final TrustScoreEngine engine;
    private final String omServerUrl;

    public TrustApiController(TrustScoreEngine engine, String omServerUrl) {
        this.engine = engine;
        this.omServerUrl = omServerUrl;
    }

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

        log.info("API routes registered");
    }

    private void getAllScores(Context ctx) {
        ctx.json(engine.getLatestScores());
    }

    private void getStats(Context ctx) {
        ctx.json(engine.getStats());
    }

    private void getScoreByFqn(Context ctx) {
        var fqn = ctx.pathParam("fqn");
        // Handle URL-encoded dots in FQN
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
}
