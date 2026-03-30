# Microsoft Fabric Setup Guide for Snowflake Integration

This guide covers the Microsoft Fabric configuration needed for bidirectional data access with Snowflake using Apache Iceberg tables.

## Architecture Overview

```
┌─────────────────────┐                    ┌─────────────────────┐
│     SNOWFLAKE       │                    │   MICROSOFT FABRIC  │
│                     │                    │                     │
│  Iceberg Tables     │ ──── OneLake ────▶ │  Lakehouse Tables   │
│  (write to OneLake) │                    │  (read from OneLake)│
│                     │                    │                     │
│  Catalog-Linked DB  │ ◀── OneLake ────── │  Lakehouse Tables   │
│  (read from OneLake)│                    │  (write to OneLake) │
└─────────────────────┘                    └─────────────────────┘
                          Apache Iceberg
                         (Open Table Format)
```

Both platforms read/write the same Parquet + Iceberg metadata files in OneLake. No data copying occurs.

---

## Prerequisites

| Requirement | Details |
|---|---|
| Microsoft Fabric capacity | F2 or higher (trial works for testing) |
| Azure subscription | With Microsoft Entra ID |
| Snowflake account | Enterprise edition or higher |
| Snowflake role | ACCOUNTADMIN (for integration setup) |

---

## Step 1: Create a Fabric Workspace

1. Go to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **New workspace**
3. Name: `snowflake-demo-workspace`
4. Select a Fabric capacity (F2+ or Trial)
5. Click **Create**

**Record the Workspace ID:**
- Open workspace Settings → About → copy the GUID from the URL
- Example: `https://app.fabric.microsoft.com/groups/<workspace_id>/...`

---

## Step 2: Create a Lakehouse

1. In the workspace, click **New** → **Lakehouse**
2. Name: `demo_lakehouse`
3. Click **Create**

**Record the Lakehouse ID:**
- Open lakehouse Settings → About → copy the GUID
- This is needed for the catalog namespace in Snowflake

---

## Step 3: Configure Entra ID Application

Snowflake needs an Entra ID (Azure AD) app registration to authenticate with OneLake.

### 3a. Create App Registration

1. Go to [Azure Portal](https://portal.azure.com) → **Microsoft Entra ID** → **App registrations**
2. Click **New registration**
3. Name: `snowflake-fabric-integration`
4. Supported account types: **Single tenant**
5. Click **Register**

### 3b. Create Client Secret

1. In the app registration, go to **Certificates & secrets**
2. Click **New client secret**
3. Description: `snowflake-access`
4. Expiry: 12 months
5. **Copy the secret value immediately** (shown only once)

### 3c. Record Values

| Value | Where to Find |
|---|---|
| Tenant ID | Entra ID → Overview |
| Client ID | App Registration → Overview → Application (client) ID |
| Client Secret | Created in step 3b |

### 3d. Grant OneLake Permissions

1. Go to your **Fabric workspace** → **Manage access**
2. Click **Add people or groups**
3. Search for `snowflake-fabric-integration` (the app registration name)
4. Assign **Contributor** role
5. Click **Add**

---

## Step 4: Grant Snowflake Service Principal Access

When Snowflake creates an external volume, it generates its own service principal.

1. In Snowflake, run:
   ```sql
   DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;
   ```
2. Copy the `AZURE_CONSENT_URL` value
3. Open it in a browser and **Accept** the consent prompt
4. Go to your Fabric workspace → **Manage access**
5. Add the Snowflake service principal (from `AZURE_MULTI_TENANT_APP_NAME`)
6. Assign **Contributor** role

---

## Step 5: Create Sample Data in Fabric

Create some tables in the Fabric Lakehouse for Snowflake to read.

### Using Fabric Notebook (PySpark):

```python
# In a Fabric Spark Notebook
from pyspark.sql.types import *
from pyspark.sql import functions as F
from datetime import datetime, timedelta
import random

# Sample: Regional sales targets from Fabric
schema = StructType([
    StructField("region", StringType(), False),
    StructField("quarter", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("target_revenue", DoubleType(), False),
    StructField("target_customers", IntegerType(), False),
    StructField("budget_allocated", DoubleType(), False),
])

data = []
regions = ["North America", "Europe", "Asia Pacific", "Latin America"]
quarters = ["Q1", "Q2", "Q3", "Q4"]

for year in [2024, 2025]:
    for region in regions:
        for quarter in quarters:
            data.append((
                region, quarter, year,
                random.uniform(500000, 2000000),
                random.randint(500, 5000),
                random.uniform(50000, 200000),
            ))

df = spark.createDataFrame(data, schema)

# Write as Delta table to Lakehouse (auto-converts to Iceberg for OneLake)
df.write.format("delta").mode("overwrite").saveAsTable("regional_sales_targets")

# Sample: Marketing campaign data from Fabric
campaign_schema = StructType([
    StructField("campaign_id", IntegerType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False),
    StructField("budget", DoubleType(), False),
    StructField("impressions", IntegerType(), False),
    StructField("clicks", IntegerType(), False),
    StructField("conversions", IntegerType(), False),
])

campaigns = [
    (1, "Spring Sale 2024", "Email", "2024-03-01", "2024-03-31", 50000.0, 1000000, 45000, 2200),
    (2, "Summer Launch", "Social Media", "2024-06-15", "2024-07-15", 75000.0, 2000000, 80000, 4500),
    (3, "Back to School", "Search", "2024-08-01", "2024-09-15", 60000.0, 1500000, 67000, 3100),
    (4, "Holiday Special", "Display", "2024-11-15", "2024-12-31", 100000.0, 3000000, 120000, 8000),
    (5, "New Year Kickoff", "Email", "2025-01-01", "2025-01-31", 45000.0, 800000, 36000, 1800),
]

campaign_df = spark.createDataFrame(campaigns, campaign_schema)
campaign_df.write.format("delta").mode("overwrite").saveAsTable("marketing_campaigns")

print("Fabric sample data created.")
```

---

## Step 6: Verify Bidirectional Access

### Snowflake → Fabric (verify in Fabric)

1. Open **demo_lakehouse** in Fabric
2. Go to **Tables** section
3. Look for Iceberg tables created by Snowflake:
   - `customer_360`
   - `sales_summary`
   - `product_performance`
   - `ml_predictions`
4. Query via **SQL Endpoint**:
   ```sql
   SELECT TOP 10 * FROM customer_360;
   SELECT region, SUM(net_revenue) AS total_revenue
   FROM sales_summary
   GROUP BY region
   ORDER BY total_revenue DESC;
   ```

### Fabric → Snowflake (verify in Snowflake)

```sql
-- In Snowflake, after catalog-linked database is set up:
USE DATABASE FABRIC_DATA;
SHOW SCHEMAS;
SHOW TABLES;

-- Query Fabric data
SELECT * FROM FABRIC_DATA.<schema>.regional_sales_targets LIMIT 10;

-- Join Fabric targets with Snowflake actuals
SELECT
    t.region,
    t.quarter,
    t.target_revenue,
    s.NET_REVENUE AS actual_revenue,
    ROUND((s.NET_REVENUE / t.target_revenue) * 100, 2) AS attainment_pct
FROM FABRIC_DATA.<schema>.regional_sales_targets t
JOIN MSFT_SNOWFLAKE_DEMO.ANALYTICS.DT_SALES_SUMMARY s
    ON t.region = s.REGION
WHERE t.year = 2024;
```

---

## Step 7: Power BI Integration

### DirectLake Mode with Iceberg Tables

1. Open **Power BI** in your Fabric workspace
2. Click **New** → **Semantic Model (default)**
3. Select the Lakehouse containing Snowflake Iceberg tables
4. Choose tables: `customer_360`, `sales_summary`, `product_performance`
5. Power BI reads directly from OneLake (no import needed)

### Sample Power BI Query (DAX):

```dax
// Total revenue by customer tier
SUMMARIZECOLUMNS(
    customer_360[CUSTOMER_TIER],
    "Total LTV", SUM(customer_360[LIFETIME_VALUE]),
    "Avg Order Value", AVERAGE(customer_360[AVG_ORDER_VALUE]),
    "Customer Count", COUNTROWS(customer_360)
)
```

---

## Troubleshooting

| Issue | Solution |
|---|---|
| Tables not appearing in Fabric | Check external volume connectivity: `SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_EXTERNAL_VOL');` |
| Catalog-linked DB shows no tables | Verify Entra app has Contributor on workspace; check `DESCRIBE CATALOG INTEGRATION` |
| "Access denied" reading Fabric data | Ensure Entra app client ID/secret are correct and scopes include `storage.azure.com` |
| Stale data in Snowflake | Run `ALTER DATABASE FABRIC_DATA REFRESH;` or wait for auto-refresh |
| Stale data in Fabric | Run `ALTER ICEBERG TABLE ... REFRESH;` in Snowflake |
| OneLake path not found | Verify workspace ID and lakehouse ID in external volume URL |

---

## Quick Reference — Values Used Across Scripts

| Parameter | Where to Get | Used In |
|---|---|---|
| `<workspace_id>` | Fabric workspace URL | External Volume, Catalog Integration |
| `<lakehouse_id>` | Fabric lakehouse URL | Catalog namespace |
| `<azure_tenant_id>` | Entra ID → Overview | External Volume, Catalog Integration |
| `<entra_app_client_id>` | App Registration → Overview | Catalog Integration |
| `<entra_app_client_secret>` | App Registration → Secrets | Catalog Integration |
| `AZURE_CONSENT_URL` | `DESC EXTERNAL VOLUME` output | Browser consent flow |
| `AZURE_MULTI_TENANT_APP_NAME` | `DESC EXTERNAL VOLUME` output | Fabric workspace access |
