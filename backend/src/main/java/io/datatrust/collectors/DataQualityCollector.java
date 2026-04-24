package io.datatrust.collectors;

import com.fasterxml.jackson.databind.JsonNode;
import io.datatrust.api.OpenMetadataClient;
import io.datatrust.model.AssetInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Scores tables based on their data quality test results.
 *
 * Logic:
 *   - No tests configured at all -> score = 0 (untested data is untrustworthy)
 *   - All tests passing -> score = 100
 *   - Mixed results -> proportional score weighted by recent results
 *   - Aborted/queued tests count as partial failures
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
        List<JsonNode> testCases = client.getTestCasesForTable(asset.fullyQualifiedName());
        if (testCases.isEmpty()) return 0.0; // no tests = no trust

        int passed = 0, failed = 0, aborted = 0, total = 0;

        for (var tc : testCases) {
            total++;
            var result = tc.path("testCaseResult");
            if (result.isMissingNode() || result.isNull()) {
                aborted++; // never ran
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

        // Passed tests contribute fully, aborted tests are half-credit
        double score = ((passed * 1.0) + (aborted * 0.3)) / total * 100.0;
        return Math.min(100.0, Math.max(0.0, score));
    }

    @Override
    public String detail(AssetInfo asset) {
        var testCases = client.getTestCasesForTable(asset.fullyQualifiedName());
        if (testCases.isEmpty()) return "No tests configured";

        long passed = testCases.stream()
                .filter(tc -> "Success".equalsIgnoreCase(
                        tc.path("testCaseResult").path("testCaseStatus").asText("")))
                .count();
        return passed + "/" + testCases.size() + " tests passing";
    }
}
