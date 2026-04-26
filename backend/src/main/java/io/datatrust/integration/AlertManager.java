package io.datatrust.integration;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.TrustScore;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Keeps an eye on the trust scores and yells at people when they drop.
 * 1. Posts a message to the OM feed so owners see it.
 * 2. Shoots a message to Slack so everyone panics.
 */
public class AlertManager {
    private static final Logger log = LoggerFactory.getLogger(AlertManager.class);
    private static final MediaType JSON = MediaType.get("application/json");

    private final OpenMetadataClient omClient;
    private final String slackWebhookUrl;
    private final double criticalThreshold;
    private final OkHttpClient http;
    private final ObjectMapper mapper;

    // Stop the bot from spamming Slack every 5 minutes
    private final Map<String, Long> lastAlertTimes = new HashMap<>();
    private static final long ALERT_COOLDOWN_MS = 1000 * 60 * 60 * 24; // 24 hours

    public AlertManager(OpenMetadataClient omClient, String slackWebhookUrl, double criticalThreshold) {
        this.omClient = omClient;
        this.slackWebhookUrl = slackWebhookUrl;
        this.criticalThreshold = criticalThreshold;
        this.mapper = new ObjectMapper();
        this.http = new OkHttpClient.Builder()
                .connectTimeout(5, TimeUnit.SECONDS)
                .build();
        
        if (slackWebhookUrl != null && !slackWebhookUrl.isBlank()) {
            log.info("AlertManager initialized with Slack Webhook. Threshold: {}", criticalThreshold);
        } else {
            log.info("AlertManager initialized without Slack Webhook. Will only post OM feeds. Threshold: {}", criticalThreshold);
        }
    }

    public void evaluateScore(TrustScore newScore, TrustScore oldScore) {
        // Only alert if the score is actually bad
        if (newScore.overallScore() >= criticalThreshold) {
            return;
        }

        // Check if we already alerted recently
        long now = System.currentTimeMillis();
        if (lastAlertTimes.containsKey(newScore.fullyQualifiedName())) {
            long lastAlert = lastAlertTimes.get(newScore.fullyQualifiedName());
            if (now - lastAlert < ALERT_COOLDOWN_MS) {
                return; // Cooldown active
            }
        }

        // Trigger Alert
        log.warn("CRITICAL: Trust score for {} dropped to {}. Triggering alerts.", 
                 newScore.fullyQualifiedName(), newScore.overallScore());
        
        lastAlertTimes.put(newScore.fullyQualifiedName(), now);

        String message = String.format("🚨 *Data Quality Alert:* The trust score for `%s` has dropped to *%.1f* (Grade: %s).\n" +
                "Please review the asset and address the governance and quality gaps.",
                newScore.displayName() != null ? newScore.displayName() : newScore.fullyQualifiedName(),
                newScore.overallScore(), newScore.grade());

        // 1. Send Slack Alert
        sendSlackAlert(message);

        // 2. Post to OpenMetadata Feed
        postToOmFeed(newScore.fullyQualifiedName(), message);
    }

    private void sendSlackAlert(String message) {
        if (slackWebhookUrl == null || slackWebhookUrl.isBlank()) {
            return;
        }

        try {
            var payload = mapper.createObjectNode();
            payload.put("text", message);

            var body = RequestBody.create(mapper.writeValueAsString(payload), JSON);
            var req = new Request.Builder().url(slackWebhookUrl).post(body).build();

            try (var resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful()) {
                    log.warn("Slack webhook failed: {}", resp.code());
                } else {
                    log.info("Slack alert sent successfully.");
                }
            }
        } catch (IOException e) {
            log.error("Failed to send Slack alert: {}", e.getMessage());
        }
    }

    private void postToOmFeed(String fqn, String message) {
        try {
            omClient.createFeedPost(fqn, message);
            log.info("Posted alert to OM feed for {}", fqn);
        } catch (IOException e) {
            log.error("Failed to post OM feed for {}: {}", fqn, e.getMessage());
        }
    }
}
