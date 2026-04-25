# OpenMetadata Hackathon Submission: DataTrust Engine

## Project Description

**DataTrust Engine** is a deterministic, context-aware reliability layer built explicitly to extend OpenMetadata. It moves beyond passive metadata cataloging by automatically computing a dynamic "Trust Score" (0-100) for every table in the platform, answering the critical question: *"Can I trust this data right now?"*

The engine deeply integrates with OpenMetadata's APIs, leveraging:
1. **Profiler API Integration**: Instead of just counting tests, it natively parses OpenMetadata's row-level statistics, column null-proportions, and profiling coverage.
2. **Lineage-Based Trust Propagation**: It doesn't just score a table in isolation. The engine uses OpenMetadata's Lineage API to traverse the upstream graph. If a source table loses an owner or starts failing tests, that risk automatically propagates down to all consumer tables.
3. **Webhook Subscriptions (Event-Driven)**: Instead of slow batch polling, the engine registers an `OmEventSubscriber` via OpenMetadata's Webhook API. When metadata changes (e.g., an owner is added, a description is updated), OpenMetadata pings the engine, triggering an instant, targeted rescore.
4. **Custom Property Writeback**: The computed `trustScore` and `trustGrade` are dynamically registered and written back natively to the OpenMetadata `table` entity via JSON Patch, making the score visible directly inside the OpenMetadata UI.
5. **Actionable Recommendations Engine**: The dashboard identifies governance gaps and computes "Fix First" recommendations (e.g., "Assign an owner: +25 pts"), turning metadata hygiene into a gamified, actionable workflow.

## Video Demo Link

[Insert YouTube/Loom Link Here]

### Demo Recording Script (3 Minutes)

*   **[0:00 - 0:30] Introduction & Problem:** "Welcome to the DataTrust Engine. Passive metadata isn't enough—data engineers need to know if data is reliable *right now*. Our engine sits on top of OpenMetadata to compute a real-time, 100-point trust score for every asset."
*   **[0:30 - 1:15] The Dashboard & "Fix First":** (Show the Bento UI). "Our dashboard gives a high-level 'Trust Pulse'. On the left, our 'Fix First' engine parses OpenMetadata to find the highest-impact actions. It knows that adding an owner to `dim_customers` adds exactly 25 trust points."
*   **[1:15 - 2:00] Deep Integration - Lineage & Profiler:** (Click a row to open the Asset Detail Slide-out). "This isn't a generic scoring tool. It deeply integrates with OM. Notice the lineage score—our engine traverses the OpenMetadata lineage graph. If an upstream source degrades, this table's trust score drops automatically. We also natively parse the OM Profiler API to detect null ratios and row-count anomalies without running separate queries."
*   **[2:00 - 2:45] Event-Driven Rescoring & Writeback:** (Show OpenMetadata UI, assign an owner). "Watch this. We assign an owner in OpenMetadata. Because we registered a webhook, OpenMetadata instantly pings our engine. The engine rescores the table, and using the Custom Properties API, writes the new 'Trust Score' back into the OpenMetadata UI via JSON Patch. Seamless integration."
*   **[2:45 - 3:00] Conclusion:** "DataTrust Engine turns OpenMetadata from a catalog into an active observability platform."

## Technical Implementation Details (For Judges)

*   **Language & Stack:** Java 17, Javalin (Backend), Vanilla JS/CSS (Frontend).
*   **Authentication:** Uses OpenMetadata Bot Tokens (`OpenMetadataClient.java`).
*   **API Usage:** 
    *   `GET /api/v1/tables` (Pagination and base metadata).
    *   `GET /api/v1/lineage/table/{id}` (Graph traversal for trust propagation).
    *   `GET /api/v1/dataQuality/testCases` (Test results extraction).
    *   `POST /api/v1/events/subscriptions` (Dynamic webhook registration).
    *   `PUT /api/v1/metadata/types/table` (Custom property schema registration).
    *   `PATCH /api/v1/tables/{id}` (Writeback of trust scores).
*   **Deployment:** Fully Dockerized. Includes `docker-compose.yml` configured to connect to standard OpenMetadata instances on port 8585.

## GitHub Repository
[Insert Repository Link Here]

## Deployment Link
[Insert Live Dashboard Link Here]
