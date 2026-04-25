package io.datatrust.collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Scores tables based on their data quality — combining test case results
 * AND profiler statistics from OpenMetadata.
 *
 * Two data sources, weighted together:
 *   1. Test Case Results (60% of quality signal)
 *      - Ratio of passing vs failing tests
 *   2. Profiler Stats (40% of quality signal)
 *      - Column null ratio — high nulls = low quality
 *      - Row count — empty tables are suspect
 *      - Column count vs profiled columns ratio
 *
 * This gives a much richer picture than test cases alone, especially
 * for tables that haven't had tests written yet but do have profiling.
 */
public class DataQualityCollector implements SignalCollector {
    private static final Logger log = LoggerFactory.getLogger(DataQualityCollector.class);
    private final OpenMetadataClient client;

    public DataQualityCollector(OpenMetadataClient client) {
        this.client = client;
    }

    @Override
    public String name() { return "quality"; }

    @Override
    public double collect(AssetInfo asset) {
        double testScore = computeTestScore(asset);
        double profilerScore = computeProfilerScore(asset);

        // If we have both signals, blend them
        boolean hasTests = client.getTestCasesForTable(asset.fullyQualifiedName()).size() > 0;
        boolean hasProfile = asset.profileData() != null && !asset.profileData().isNull();

        if (hasTests && hasProfile) {
            return testScore * 0.6 + profilerScore * 0.4;
        } else if (hasTests) {
            return testScore;
        } else if (hasProfile) {
            return profilerScore * 0.7; // profiler alone is weaker signal
        } else {
            return 0.0; // no tests, no profile = untested = 0
        }
    }

    /**
     * Score based on test case pass/fail ratio.
     */
    private double computeTestScore(AssetInfo asset) {
        List<JsonNode> testCases = client.getTestCasesForTable(asset.fullyQualifiedName());
        if (testCases.isEmpty()) return 0.0;

        int passed = 0, failed = 0, aborted = 0, total = 0;

        for (var tc : testCases) {
            total++;
            var result = tc.path("testCaseResult");
            if (result.isMissingNode() || result.isNull()) {
                aborted++;
                continue;
            }

            var status = result.path("testCaseStatus").asText("").toUpperCase();
            switch (status) {
                case "SUCCESS" -> passed++;
                case "FAILED" -> failed++;
                case "ABORTED" -> aborted++;
                default -> aborted++;
            }
        }

        if (total == 0) return 0.0;
        double score = ((passed * 1.0) + (aborted * 0.3)) / total * 100.0;
        return Math.min(100.0, Math.max(0.0, score));
    }

    /**
     * Score based on OM profiler statistics — null ratios, row counts,
     * and column profiling coverage.
     *
     * Deep OM integration: reads the table's profile field which contains
     * aggregated profiler results from the OM ingestion pipeline.
     */
    private double computeProfilerScore(AssetInfo asset) {
        var profile = asset.profileData();
        if (profile == null || profile.isNull()) return 0.0;

        double score = 40.0; // baseline for having a profile at all

        // Row count check — empty tables are suspect
        long rowCount = profile.path("rowCount").asLong(
                profile.path("profileSample").asLong(0));
        if (rowCount > 0) score += 15.0;
        if (rowCount > 100) score += 5.0;

        // Column-level profiling coverage
        var columnProfile = profile.path("columnProfile");
        if (columnProfile.isArray() && !columnProfile.isEmpty()) {
            int profiledCols = columnProfile.size();
            int totalCols = asset.totalColumns();
            if (totalCols > 0) {
                double coverage = (double) profiledCols / totalCols;
                score += coverage * 15.0; // up to 15 points
            }

            // Check average null ratio across columns
            double totalNullRatio = 0;
            int checkedCols = 0;
            for (var col : columnProfile) {
                double nullFraction = col.path("nullProportion").asDouble(
                        col.path("nullCount").asDouble(0) /
                                Math.max(1, rowCount));
                if (nullFraction >= 0 && nullFraction <= 1) {
                    totalNullRatio += nullFraction;
                    checkedCols++;
                }
            }
            if (checkedCols > 0) {
                double avgNullRatio = totalNullRatio / checkedCols;
                // Low null ratio = high quality
                double nullScore = (1.0 - avgNullRatio) * 25.0;
                score += Math.max(0, nullScore);
            }
        } else {
            // No column-level profile, but table profile exists
            score += 10.0;
        }

        return Math.min(100.0, Math.max(0.0, score));
    }

    @Override
    public String detail(AssetInfo asset) {
        var testCases = client.getTestCasesForTable(asset.fullyQualifiedName());
        var profile = asset.profileData();

        StringBuilder sb = new StringBuilder();

        // Test info
        if (testCases.isEmpty()) {
            sb.append("No tests configured");
        } else {
            long passed = testCases.stream()
                    .filter(tc -> "Success".equalsIgnoreCase(
                            tc.path("testCaseResult").path("testCaseStatus").asText("")))
                    .count();
            sb.append(passed).append("/").append(testCases.size()).append(" tests passing");
        }

        // Profiler info
        if (profile != null && !profile.isNull()) {
            long rows = profile.path("rowCount").asLong(0);
            sb.append(", ").append(rows > 0 ? rows + " rows profiled" : "profile exists");

            var colProfile = profile.path("columnProfile");
            if (colProfile.isArray() && !colProfile.isEmpty()) {
                sb.append(", ").append(colProfile.size()).append(" cols profiled");
            }
        } else {
            sb.append(", no profiler data");
        }

        return sb.toString();
    }
}
