package io.datatrust.collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Scores tables based on data freshness — when was the profiler
 * last run, and how recent is the data?
 *
 * Stale data is risky data. If nobody is profiling a table,
 * it might have silently broken weeks ago.
 *
 * Decay curve:
 *   - Profiled within 24h -> 100
 *   - Profiled within 3 days -> 80
 *   - Profiled within 7 days -> 60
 *   - Profiled within 14 days -> 40
 *   - Profiled within 30 days -> 20
 *   - Older than 30 days or never -> 0
 */
public class FreshnessCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(FreshnessCollector.class);

    @Override
    public String name() { return "freshness"; }

    @Override
    public double collect(AssetInfo asset) {
        var profile = asset.profileData();
        if (profile == null || profile.isNull()) return 0.0;

        // Profile timestamp can be epoch millis or ISO string
        long profileTimestamp = extractTimestamp(profile);
        if (profileTimestamp <= 0) return 0.0;

        var profileTime = Instant.ofEpochMilli(profileTimestamp);
        long hoursAgo = ChronoUnit.HOURS.between(profileTime, Instant.now());

        if (hoursAgo < 0) hoursAgo = 0; // clock skew protection

        // Smooth decay: score drops as data gets older
        if (hoursAgo <= 24) return 100.0;
        if (hoursAgo <= 72) return 80.0;
        if (hoursAgo <= 168) return 60.0;    // 7 days
        if (hoursAgo <= 336) return 40.0;    // 14 days
        if (hoursAgo <= 720) return 20.0;    // 30 days
        return 0.0;
    }

    private long extractTimestamp(JsonNode profile) {
        // OM stores timestamps as epoch millis in the "timestamp" field
        if (profile.has("timestamp")) {
            var ts = profile.get("timestamp");
            if (ts.isNumber()) {
                long val = ts.asLong();
                // OM sometimes stores seconds, sometimes millis
                return val < 1_000_000_000_000L ? val * 1000 : val;
            }
        }
        // Fallback: check profileDate
        if (profile.has("profileDate")) {
            try {
                var dateStr = profile.get("profileDate").asText();
                return Instant.parse(dateStr).toEpochMilli();
            } catch (Exception ignored) {}
        }
        return -1;
    }

    @Override
    public String detail(AssetInfo asset) {
        var profile = asset.profileData();
        if (profile == null || profile.isNull()) return "Never profiled";

        long ts = extractTimestamp(profile);
        if (ts <= 0) return "No profile timestamp";

        var profileTime = Instant.ofEpochMilli(ts);
        long hoursAgo = ChronoUnit.HOURS.between(profileTime, Instant.now());

        if (hoursAgo < 1) return "Profiled just now";
        if (hoursAgo < 24) return "Profiled " + hoursAgo + "h ago";
        long daysAgo = hoursAgo / 24;
        return "Profiled " + daysAgo + " day" + (daysAgo != 1 ? "s" : "") + " ago";
    }
}
