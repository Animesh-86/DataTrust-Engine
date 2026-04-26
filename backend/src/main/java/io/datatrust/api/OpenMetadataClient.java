package io.datatrust.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import okhttp3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
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
                .addInterceptor(chain -> chain.proceed(chain.request().newBuilder()
                        .header("ngrok-skip-browser-warning", "true").build()))
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
                .addInterceptor(chain -> chain.proceed(chain.request().newBuilder()
                        .header("ngrok-skip-browser-warning", "true").build()))
                .build();
        this.jwtToken = botToken;
        log.info("Using bot token for authentication");
    }

    private void authenticate(String user, String password) {
        try {
            // OM 1.12+ requires base64-encoded password
            var encodedPwd = Base64.getEncoder().encodeToString(password.getBytes());
            var payload = mapper.createObjectNode()
                    .put("email", user)
                    .put("password", encodedPwd);

            var body = RequestBody.create(mapper.writeValueAsString(payload), JSON_TYPE);
            var req = new Request.Builder()
                    .url(baseUrl + "/api/v1/users/login")
                    .post(body)
                    .build();

            try (var resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful()) {
                    log.error("Auth failed ({}): {} - Running in disconnected mode", resp.code(), resp.body().string());
                    return;
                }
                var json = mapper.readTree(resp.body().string());
                this.jwtToken = json.path("accessToken").asText();
                log.info("Authenticated with OpenMetadata as '{}'", user);
            }
        } catch (Exception e) {
            log.error("Cannot connect to OpenMetadata at {} - Running in disconnected mode", baseUrl);
        }
    }

    /**
     * Fetch all tables with the fields we need for scoring.
     * Automatically paginates through the full result set.
     */
    public List<JsonNode> listAllTables() {
        var fields = "owners,tags,columns,profile";
        return paginatedGet("/api/v1/tables", fields);
    }

    /**
     * Fetch test cases linked to a specific table.
     */
    public List<JsonNode> getTestCasesForTable(String tableFqn) {
        var results = new ArrayList<JsonNode>();
        var entityLink = "<#E::table::" + tableFqn + ">";
        var encoded = URLEncoder.encode(entityLink, StandardCharsets.UTF_8);
        var url = baseUrl + "/api/v1/dataQuality/testCases?entityLink="
                + encoded + "&limit=" + PAGE_SIZE + "&include=all";
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
        var url = baseUrl + "/api/v1/tables/" + tableId + "?fields=owners,tags,columns,profile";
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

    // ==== Deep Integration APIs ====

    /**
     * Fetch entity type definition by name (e.g., "table").
     * Used to register custom properties on entity types.
     */
    public JsonNode getTypeByName(String typeName) {
        var url = baseUrl + "/api/v1/metadata/types/name/" + typeName + "?fields=customProperties";
        try {
            return doGet(url);
        } catch (Exception e) {
            log.warn("Failed to fetch type '{}': {}", typeName, e.getMessage());
            return null;
        }
    }

    /**
     * Fetch a property type reference by name (e.g., "integer", "string", "dateTime").
     */
    public JsonNode getPropertyType(String propertyTypeName) {
        var url = baseUrl + "/api/v1/metadata/types/name/" + propertyTypeName;
        try {
            return doGet(url);
        } catch (Exception e) {
            log.debug("Property type '{}' not found: {}", propertyTypeName, e.getMessage());
            return null;
        }
    }

    /**
     * Add or update a custom property on an entity type.
     * PUT /api/v1/metadata/types/{id} with CustomProperty body.
     */
    public void addCustomProperty(String typeId, JsonNode propertyPayload) throws IOException {
        var body = RequestBody.create(mapper.writeValueAsString(propertyPayload), JSON_TYPE);
        var req = new Request.Builder()
                .url(baseUrl + "/api/v1/metadata/types/" + typeId)
                .header("Authorization", "Bearer " + jwtToken)
                .put(body)
                .build();

        try (var resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.debug("addCustomProperty returned {}: {}", resp.code(),
                        resp.body() != null ? resp.body().string() : "");
            }
        }
    }

    /**
     * Patch a table's extension field with trust score data.
     * Uses JSON Patch to set custom property values.
     * Sends a single "add" on /extension with the full object.
     */
    public void patchTableExtension(String tableId, JsonNode extensionData) throws IOException {
        var patchOps = mapper.createArrayNode();

        // Single "add" op that replaces/creates the whole extension map
        var op = mapper.createObjectNode();
        op.put("op", "add");
        op.put("path", "/extension");
        op.set("value", extensionData);
        patchOps.add(op);

        var body = RequestBody.create(mapper.writeValueAsString(patchOps),
                MediaType.get("application/json-patch+json"));
        var req = new Request.Builder()
                .url(baseUrl + "/api/v1/tables/" + tableId)
                .header("Authorization", "Bearer " + jwtToken)
                .patch(body)
                .build();

        try (var resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful() && resp.code() != 400) {
                log.debug("patchTableExtension for {} returned {}", tableId, resp.code());
            }
        }
    }

    /**
     * Get an event subscription by name.
     */
    public JsonNode getEventSubscription(String name) {
        var url = baseUrl + "/api/v1/events/subscriptions/name/" + name;
        try {
            return doGet(url);
        } catch (Exception e) {
            log.debug("Event subscription '{}' not found", name);
            return null;
        }
    }

    /**
     * Create a new event subscription (webhook).
     */
    public JsonNode createEventSubscription(JsonNode payload) throws IOException {
        var body = RequestBody.create(mapper.writeValueAsString(payload), JSON_TYPE);
        var req = new Request.Builder()
                .url(baseUrl + "/api/v1/events/subscriptions")
                .header("Authorization", "Bearer " + jwtToken)
                .post(body)
                .build();

        try (var resp = http.newCall(req).execute()) {
            if (resp.isSuccessful() && resp.body() != null) {
                return mapper.readTree(resp.body().string());
            }
            log.warn("createEventSubscription returned {}", resp.code());
            return null;
        }
    }

    /**
     * Fetch Data Insights aggregate data.
     */
    public JsonNode getDataInsights(String dataReportIndex, long startTs, long endTs) {
        var url = baseUrl + "/api/v1/analytics/dataInsights/data"
                + "?dataReportIndex=" + dataReportIndex
                + "&startTs=" + startTs + "&endTs=" + endTs;
        try {
            return doGet(url);
        } catch (Exception e) {
            log.debug("Data insights not available: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Create a feed post (Conversation/Task) in OpenMetadata.
     */
    public void createFeedPost(String entityFqn, String message) throws IOException {
        var payload = mapper.createObjectNode();
        payload.put("about", "<#E::table::" + entityFqn + ">");
        payload.put("message", message);

        var body = RequestBody.create(mapper.writeValueAsString(payload), JSON_TYPE);
        var req = new Request.Builder()
                .url(baseUrl + "/api/v1/feed")
                .header("Authorization", "Bearer " + jwtToken)
                .post(body)
                .build();

        try (var resp = http.newCall(req).execute()) {
            if (!resp.isSuccessful()) {
                log.warn("createFeedPost returned {}: {}", resp.code(),
                        resp.body() != null ? resp.body().string() : "");
            }
        }
    }

    /** Get the base URL for generating deep links */
    public String getBaseUrl() { return baseUrl; }
}
