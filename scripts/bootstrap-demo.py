import requests
import json
import time

OM_URL = "http://localhost:8585/api/v1"
AUTH = ('admin', 'admin')

# 1. Login to get JWT Token
print("Authenticating with OpenMetadata...")
login_payload = {"email": "admin@open-metadata.org", "password": "admin"}
try:
    resp = requests.post(f"{OM_URL}/users/login", json=login_payload)
    resp.raise_for_status()
    token = resp.json().get('accessToken')
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("✓ Authenticated")
except Exception as e:
    print(f"Failed to authenticate. Is OM running on localhost:8585? {e}")
    exit(1)

# 2. Create a Database Service (MySQL)
print("\nCreating Database Service 'demo_db'...")
service_payload = {
    "name": "demo_db",
    "serviceType": "Mysql",
    "connection": {"config": {"type": "Mysql", "username": "root", "hostPort": "localhost:3306"}}
}
requests.post(f"{OM_URL}/services/databaseServices", headers=headers, json=service_payload)

# 3. Create a Database
print("Creating Database 'ecommerce'...")
db_payload = {
    "name": "ecommerce",
    "service": "demo_db"
}
requests.post(f"{OM_URL}/databases", headers=headers, json=db_payload)

# 4. Create a Schema
print("Creating Schema 'public'...")
schema_payload = {
    "name": "public",
    "database": "demo_db.ecommerce"
}
requests.post(f"{OM_URL}/databaseSchemas", headers=headers, json=schema_payload)

# 5. Create Tables
tables = [
    {
        "name": "dim_customers",
        "description": "Dimension table for customer accounts.",
        "columns": [
            {"name": "customer_id", "dataType": "INT"},
            {"name": "email", "dataType": "VARCHAR"},
            {"name": "status", "dataType": "VARCHAR"}
        ]
    },
    {
        "name": "fact_orders",
        "description": "Fact table containing all placed orders.",
        "columns": [
            {"name": "order_id", "dataType": "INT"},
            {"name": "customer_id", "dataType": "INT"},
            {"name": "amount", "dataType": "FLOAT"}
        ]
    },
    {
        "name": "stg_stripe_payments",
        "description": "Raw staging table for Stripe payments. Low quality.",
        "columns": [
            {"name": "payment_id", "dataType": "VARCHAR"},
            {"name": "order_id", "dataType": "INT"},
            {"name": "status", "dataType": "VARCHAR"}
        ]
    }
]

print("\nCreating Tables...")
for t in tables:
    payload = {
        "name": t['name'],
        "description": t.get('description', ''),
        "columns": t['columns'],
        "databaseSchema": "demo_db.ecommerce.public"
    }
    resp = requests.post(f"{OM_URL}/tables", headers=headers, json=payload)
    if resp.status_code in [200, 201]:
        print(f"✓ Created table {t['name']}")
    else:
        print(f"Failed to create table {t['name']}: {resp.status_code} - {resp.text}")

print("\nAdding Lineage: stg_stripe_payments -> fact_orders")
lineage_payload = {
    "edge": {
        "fromEntity": {"id": requests.get(f"{OM_URL}/tables/name/demo_db.ecommerce.public.stg_stripe_payments", headers=headers).json().get('id'), "type": "table"},
        "toEntity": {"id": requests.get(f"{OM_URL}/tables/name/demo_db.ecommerce.public.fact_orders", headers=headers).json().get('id'), "type": "table"}
    }
}
requests.put(f"{OM_URL}/lineage", headers=headers, json=lineage_payload)

print("\n🎉 Bootstrap Complete! The DataTrust Engine will now start scoring these tables.")
