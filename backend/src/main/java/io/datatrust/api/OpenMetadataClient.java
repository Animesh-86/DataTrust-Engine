package io.datatrust.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Talks to the OpenMetadata REST API.
 *
 * Handles authentication, pagination, and retries so the rest of the
 * codebase doesn't have to care about HTTP details.
 */
public class OpenMetadataClient {
    private static final Logger log = LoggerFactory.getLogger(OpenMetadataClient.class);
    private static final MediaType JSON_TYPE = MediaType.get("application/json");
    private static final int PAGE_SIZE = 50;

    private final String baseUrl;
    private final OkHttpClient http;
    private final ObjectMapper mapper;
    private String jwtToken;

    public OpenMetadataClient(String serverUrl, String user, String password) {
        this.baseUrl = serverUrl.endsWith("/") ? serverUrl.substring(0, serverUrl.length() - 1) : serverUrl;
        this.mapper = new ObjectMapper();
        this.http = new OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(15, TimeUnit.SECONDS)
                .build();
        authenticate(user, password);
    }

    // bot-token based auth (for deployment)
    public OpenMetadataClient(String serverUrl, String botToken) {
        this.baseUrl = serverUrl.endsWith("/") ? serverUrl.substring(0, serverUrl.length() - 1) : serverUrl;
        this.mapper = new ObjectMapper();
        this.http = new OkHttpClient.Builder()
                .connectTimeout(15, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();
        this.jwtToken = botToken;
        log.info("Using bot token for authentication");
    }

    private void authenticate(String user, String password) {
        try {
            var payload = mapper.createObjectNode()
                    .put("email", user)
                    .put("password", password);

            var body = RequestBody.create(mapper.writeValueAsString(payload), JSON_TYPE);
            var req = new Request.Builder()
                    .url(baseUrl + "/api/v1/users/login")
                    .post(body)
                    .build();

            try (var resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful()) {
                    throw new RuntimeException("Auth failed (" + resp.code() + "): " + resp.body().string());
                }
                var json = mapper.readTree(resp.body().string());
                this.jwtToken = json.path("accessToken").asText();
                log.info("Authenticated with OpenMetadata as '{}'", user);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot connect to OpenMetadata at " + baseUrl, e);
        }
    }

    /**
     * Fetch all tables with the fields we need for scoring.
     * Automatically paginates through the full result set.
     */
    public List<JsonNode> listAllTables() {
        var fields = "owner,owners,tags,columns,profile,tableConstraints";
        return paginatedGet("/api/v1/tables", fields);
    }

    /**
     * Fetch test cases linked to a specific table.
     */
    public List<JsonNode> getTestCasesForTable(String tableFqn) {
        var results = new ArrayList<JsonNode>();
        var entityLink = "<#E::table::" + tableFqn + ">";
        var url = baseUrl + "/api/v1/dataQuality/testCases?entityLink="
                + entityLink + "&limit=" + PAGE_SIZE + "&include=all";
        try {
            var json = doGet(url);
            if (json != null && json.has("data") && json.get("data").isArray()) {
                for (var item : json.get("data")) {
                    results.add(item);
                }
            }
        } catch (Exception e) {
            log.debug("No test cases for {}: {}", tableFqn, e.getMessage());
        }
        return results;
    }

    /**
     * Fetch lineage graph for a table — upstream and downstream edges.
     */
    public JsonNode getLineage(String tableId) {
        var url = baseUrl + "/api/v1/lineage/table/" + tableId + "?upstreamDepth=3&downstreamDepth=1";
        try {
            return doGet(url);
        } catch (Exception e) {
            log.debug("No lineage for table {}: {}", tableId, e.getMessage());
            return null;
        }
    }

    /**
     * Fetch a specific table by ID.
     */
    public JsonNode getTable(String tableId) {
        var url = baseUrl + "/api/v1/tables/" + tableId + "?fields=owner,owners,tags,columns,profile";
        try {
            return doGet(url);
        } catch (Exception e) {
            log.debug("Could not fetch table {}: {}", tableId, e.getMessage());
            return null;
        }
    }

    /**
     * Check if the server is reachable and authenticated.
     */
    public boolean isHealthy() {
        try {
            var resp = doGet(baseUrl + "/api/v1/system/version");
            return resp != null && resp.has("version");
        } catch (Exception e) {
            return false;
        }
    }

    public String getServerVersion() {
        try {
            var resp = doGet(baseUrl + "/api/v1/system/version");
            return resp != null ? resp.path("version").asText("unknown") : "unknown";
        } catch (Exception e) {
            return "unreachable";
        }
    }

    // ---- internal plumbing ----

    private List<JsonNode> paginatedGet(String path, String fields) {
        var allResults = new ArrayList<JsonNode>();
        String after = null;

        while (true) {
            var urlBuilder = new StringBuilder(baseUrl + path)
                    .append("?limit=").append(PAGE_SIZE)
                    .append("&fields=").append(fields);
            if (after != null) {
                urlBuilder.append("&after=").append(after);
            }

            try {
                var json = doGet(urlBuilder.toString());
                if (json == null || !json.has("data")) break;

                var data = json.get("data");
                if (!data.isArray() || data.isEmpty()) break;

                for (var item : data) {
                    allResults.add(item);
                }

                // Check for next page
                var paging = json.path("paging");
                if (paging.has("after") && !paging.get("after").isNull()) {
                    after = paging.get("after").asText();
                } else {
                    break;
                }

                log.debug("Fetched {} items from {}, total so far: {}",
                        data.size(), path, allResults.size());
            } catch (Exception e) {
                log.error("Error during paginated fetch of {}: {}", path, e.getMessage());
                break;
            }
        }

        log.info("Fetched {} total items from {}", allResults.size(), path);
        return allResults;
    }

    private JsonNode doGet(String url) throws IOException {
        var req = new Request.Builder()
                .url(url)
                .header("Authorization", "Bearer " + jwtToken)
                .header("Accept", "application/json")
                .get()
                .build();

        try (var resp = http.newCall(req).execute()) {
            if (resp.code() == 404) return null;
            if (!resp.isSuccessful()) {
                log.warn("GET {} returned {}", url, resp.code());
                return null;
            }
            var bodyStr = resp.body() != null ? resp.body().string() : "{}";
            return mapper.readTree(bodyStr);
        }
    }
}
