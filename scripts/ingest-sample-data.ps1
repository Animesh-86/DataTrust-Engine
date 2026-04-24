$ErrorActionPreference = "Continue"
$omUrl = "http://localhost:8585"
$schemaFqn = "sample_data_warehouse.analytics_db.public"

$body = @{email='admin@open-metadata.org'; password=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('admin'))} | ConvertTo-Json
$resp = Invoke-RestMethod -Uri "$omUrl/api/v1/users/login" -Method Post -Body $body -ContentType 'application/json'
$h = @{Authorization="Bearer $($resp.accessToken)"; "Content-Type"="application/json"}

# Create tables with different metadata quality
$tablesJson = @'
[
  {"name":"fact_orders","displayName":"Orders Fact Table","description":"Central fact table for all customer orders. Updated hourly via ETL pipeline.","columns":[{"name":"order_id","dataType":"INT","description":"Unique order identifier"},{"name":"customer_id","dataType":"INT","description":"FK to dim_customers"},{"name":"order_date","dataType":"DATETIME","description":"Timestamp of order placement"},{"name":"total_amount","dataType":"DECIMAL","description":"Order total in USD"},{"name":"status","dataType":"VARCHAR","description":"Order status: pending, shipped, delivered, cancelled"}]},
  {"name":"dim_customers","displayName":"Customer Dimension","description":"Master customer table with demographic and contact information.","columns":[{"name":"customer_id","dataType":"INT","description":"Primary key"},{"name":"first_name","dataType":"VARCHAR","description":"Customer first name"},{"name":"last_name","dataType":"VARCHAR","description":"Customer last name"},{"name":"email","dataType":"VARCHAR","description":"Contact email"},{"name":"signup_date","dataType":"DATE","description":"Account creation date"},{"name":"segment","dataType":"VARCHAR","description":"Customer segment: premium, standard, basic"}]},
  {"name":"fact_page_views","displayName":"Page Views","description":"Web analytics event stream. High volume.","columns":[{"name":"event_id","dataType":"BIGINT","description":"Unique event ID"},{"name":"user_id","dataType":"INT","description":""},{"name":"page_url","dataType":"VARCHAR","description":"Page URL visited"},{"name":"referrer","dataType":"VARCHAR","description":""},{"name":"timestamp","dataType":"DATETIME","description":"Event timestamp"},{"name":"session_id","dataType":"VARCHAR","description":"Browser session ID"},{"name":"device_type","dataType":"VARCHAR","description":""}]},
  {"name":"dim_products","displayName":"Product Catalog","description":"","columns":[{"name":"product_id","dataType":"INT","description":""},{"name":"product_name","dataType":"VARCHAR","description":"Product display name"},{"name":"category","dataType":"VARCHAR","description":""},{"name":"price","dataType":"DECIMAL","description":"Retail price in USD"},{"name":"created_at","dataType":"DATETIME","description":""}]},
  {"name":"stg_raw_events","displayName":"Raw Event Staging","description":"","columns":[{"name":"event_payload","dataType":"VARCHAR","description":""},{"name":"received_at","dataType":"DATETIME","description":""},{"name":"source","dataType":"VARCHAR","description":""}]},
  {"name":"fact_revenue","displayName":"Revenue Metrics","description":"Aggregated revenue data by day and product category.","columns":[{"name":"date","dataType":"DATE","description":"Revenue date"},{"name":"category","dataType":"VARCHAR","description":"Product category"},{"name":"revenue","dataType":"DECIMAL","description":"Total revenue in USD"},{"name":"units_sold","dataType":"INT","description":"Number of units sold"},{"name":"refunds","dataType":"DECIMAL","description":"Total refunds"},{"name":"net_revenue","dataType":"DECIMAL","description":"Revenue minus refunds"}]},
  {"name":"dim_locations","displayName":"Location Master","description":"Geographic reference table for warehouses and delivery zones.","columns":[{"name":"location_id","dataType":"INT","description":"Primary key"},{"name":"city","dataType":"VARCHAR","description":"City name"},{"name":"state","dataType":"VARCHAR","description":"State/province"},{"name":"country","dataType":"VARCHAR","description":"ISO country code"},{"name":"zip_code","dataType":"VARCHAR","description":"Postal code"},{"name":"latitude","dataType":"DECIMAL","description":""},{"name":"longitude","dataType":"DECIMAL","description":""}]},
  {"name":"audit_log","displayName":"Audit Log","description":"","columns":[{"name":"log_id","dataType":"BIGINT","description":""},{"name":"action","dataType":"VARCHAR","description":""},{"name":"user_name","dataType":"VARCHAR","description":""},{"name":"ts","dataType":"DATETIME","description":""},{"name":"details","dataType":"VARCHAR","description":""}]}
]
'@

$tables = $tablesJson | ConvertFrom-Json
$created = @()

foreach ($tbl in $tables) {
    $payload = @{
        name = $tbl.name
        displayName = $tbl.displayName
        description = $tbl.description
        databaseSchema = $schemaFqn
        columns = $tbl.columns
        tableType = "Regular"
    } | ConvertTo-Json -Depth 5

    try {
        $result = Invoke-RestMethod -Uri "$omUrl/api/v1/tables" -Method Post -Body $payload -Headers $h
        $created += $result
        Write-Host "Created: $($result.fullyQualifiedName)" -ForegroundColor Green
    } catch {
        Write-Host "Skipped $($tbl.name): already exists or error" -ForegroundColor Yellow
    }
}

Write-Host "`nCreated $($created.Count) tables" -ForegroundColor Cyan

# Add owners to well-governed tables
$adminUser = Invoke-RestMethod -Uri "$omUrl/api/v1/users?limit=1" -Headers $h
$adminId = $adminUser.data[0].id

$hp = @{Authorization="Bearer $($resp.accessToken)"; "Content-Type"="application/json-patch+json"}

foreach ($tbl in $created[0..3]) {
    try {
        $patchBody = "[{`"op`":`"add`",`"path`":`"/owners`",`"value`":[{`"id`":`"$adminId`",`"type`":`"user`"}]}]"
        Invoke-RestMethod -Uri "$omUrl/api/v1/tables/$($tbl.id)" -Method Patch -Body $patchBody -Headers $hp | Out-Null
        Write-Host "Added owner: $($tbl.name)" -ForegroundColor Green
    } catch { Write-Host "Owner patch failed for $($tbl.name)" -ForegroundColor Yellow }
}

# Add tier to top tables
foreach ($tbl in $created[0..1]) {
    try {
        $tierPatch = "[{`"op`":`"add`",`"path`":`"/tags`",`"value`":[{`"tagFQN`":`"Tier.Tier1`",`"source`":`"Classification`"}]}]"
        Invoke-RestMethod -Uri "$omUrl/api/v1/tables/$($tbl.id)" -Method Patch -Body $tierPatch -Headers $hp | Out-Null
        Write-Host "Added Tier.Tier1: $($tbl.name)" -ForegroundColor Green
    } catch { Write-Host "Tier patch failed for $($tbl.name)" -ForegroundColor Yellow }
}

Write-Host "`n=== Sample data ingestion complete ===" -ForegroundColor Cyan
