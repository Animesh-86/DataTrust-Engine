package io.datatrust.engine;

import io.datatrust.api.OpenMetadataClient;
import io.datatrust.collectors.*;
import io.datatrust.integration.OmCustomPropertyManager;
import io.datatrust.model.AssetInfo;
import io.datatrust.model.TrustScore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The heart of DataTrust — orchestrates signal collection,
 * score computation, persistence, and OM writeback on a scheduled loop.
 */
public class TrustScoreEngine {
    private static final Logger log = LoggerFactory.getLogger(TrustScoreEngine.class);

    private final OpenMetadataClient client;
    private final HistoryStore store;
    private final ScoreAggregator aggregator;
    private final List<SignalCollector> collectors;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicReference<List<TrustScore>> latestScores = new AtomicReference<>(List.of());
    private final AtomicReference<String> lastRunStatus = new AtomicReference<>("never");

    private final int intervalMinutes;
    private OmCustomPropertyManager propertyManager;
    private io.datatrust.integration.AlertManager alertManager;

    public TrustScoreEngine(OpenMetadataClient client, HistoryStore store, io.datatrust.integration.AlertManager alertManager, int intervalMinutes) {
        this.client = client;
        this.store = store;
        this.alertManager = alertManager;
        this.intervalMinutes = intervalMinutes;
        this.aggregator = new ScoreAggregator();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "trust-engine");
            t.setDaemon(true);
            return t;
        });

        // Wire up collectors
        this.collectors = List.of(
                new DataQualityCollector(client),
                new GovernanceCollector(),
                new LineageCollector(client),
                new FreshnessCollector()
        );
    }

    /** Set the custom property manager for OM writeback */
    public void setPropertyManager(OmCustomPropertyManager mgr) {
        this.propertyManager = mgr;
    }

    /**
     * Start the scoring loop. First run happens immediately,
     * subsequent runs on the configured interval.
     */
    public void start() {
        log.info("Starting TrustScore engine (interval={}min)", intervalMinutes);
        scheduler.scheduleAtFixedRate(this::runCycle, 0, intervalMinutes, TimeUnit.MINUTES);
    }

    /** Run a single scoring cycle on-demand */
    public void runOnce() {
        CompletableFuture.runAsync(this::runCycle);
    }

    private void runCycle() {
        if (running.getAndSet(true)) {
            log.info("Scoring cycle already in progress, skipping");
            return;
        }

        long startTime = System.currentTimeMillis();
        try {
            log.info("=== Trust scoring cycle started ===");

            // 1) Fetch all tables from OpenMetadata
            var rawTables = client.listAllTables();
            if (rawTables.isEmpty()) {
                log.warn("No tables found in OpenMetadata — is sample data ingested?");
                lastRunStatus.set("no_data");
                return;
            }

            // 2) Parse into our model
            var assets = rawTables.stream()
                    .map(AssetInfo::fromJson)
                    .filter(a -> !a.id().isBlank())
                    .toList();

            log.info("Scoring {} tables...", assets.size());

            // 3) Compute scores
            var scores = new ArrayList<TrustScore>();
            for (var asset : assets) {
                try {
                    var score = computeScore(asset);
                    scores.add(score);
                    
                    // Trigger alerts if necessary
                    if (alertManager != null) {
                        var oldScore = store.getLatest(asset.fullyQualifiedName());
                        alertManager.evaluateScore(score, oldScore);
                    }
                } catch (Exception e) {
                    log.warn("Failed to score {}: {}", asset.fullyQualifiedName(), e.getMessage());
                }
            }

            // 4) Persist locally
            store.saveBatch(scores);
            latestScores.set(Collections.unmodifiableList(scores));

            // 5) Write scores back to OpenMetadata custom properties
            if (propertyManager != null && propertyManager.isReady()) {
                try {
                    propertyManager.writeBackScores(scores);
                } catch (Exception e) {
                    log.warn("Writeback to OM failed: {}", e.getMessage());
                }
            }

            long elapsed = System.currentTimeMillis() - startTime;
            log.info("=== Scored {} tables in {}ms ===", scores.size(), elapsed);
            lastRunStatus.set("ok");

            // Reset caches in collectors that need it
            collectors.stream()
                    .filter(c -> c instanceof LineageCollector)
                    .forEach(c -> ((LineageCollector) c).resetCache());

        } catch (Exception e) {
            log.error("Scoring cycle failed: {}", e.getMessage(), e);
            lastRunStatus.set("error: " + e.getMessage());
        } finally {
            running.set(false);
        }
    }

    private TrustScore computeScore(AssetInfo asset) {
        var signals = new HashMap<String, Double>();
        var details = new HashMap<String, String>();

        for (var collector : collectors) {
            try {
                double score = collector.collect(asset);
                signals.put(collector.name(), score);
                var detail = collector.detail(asset);
                if (detail != null) details.put(collector.name(), detail);
            } catch (Exception e) {
                log.debug("Collector {} failed for {}: {}",
                        collector.name(), asset.fullyQualifiedName(), e.getMessage());
                signals.put(collector.name(), 0.0);
                details.put(collector.name(), "Error: " + e.getMessage());
            }
        }

        return aggregator.compute(
                asset.id(), asset.fullyQualifiedName(),
                asset.displayName(), signals, details
        );
    }

    // ---- public getters for the API layer ----

    public List<TrustScore> getLatestScores() {
        var cached = latestScores.get();
        return cached.isEmpty() ? store.getLatestScores() : cached;
    }

    public TrustScore getScoreForTable(String fqn) {
        return getLatestScores().stream()
                .filter(s -> s.fullyQualifiedName().equals(fqn))
                .findFirst()
                .orElse(null);
    }

    public List<TrustScore> getHistory(String fqn, int limit) {
        return store.getHistory(fqn, limit);
    }

    public HistoryStore.Stats getStats() {
        return store.getStats();
    }

    public Map<String, Object> getEngineStatus() {
        return Map.of(
                "running", running.get(),
                "lastRun", lastRunStatus.get(),
                "interval", intervalMinutes + "min",
                "scoresCount", latestScores.get().size(),
                "weights", aggregator.getWeights()
        );
    }

    public void shutdown() {
        scheduler.shutdown();
        store.close();
    }
}
