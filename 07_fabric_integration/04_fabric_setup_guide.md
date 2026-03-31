# Microsoft Fabric Setup Guide for Snowflake Integration

This guide covers the Microsoft Fabric configuration needed for bidirectional data access with Snowflake using Apache Iceberg tables.

## Architecture Overview

```
┌─────────────────────┐                    ┌─────────────────────┐
│     SNOWFLAKE       │                    │   MICROSOFT FABRIC  │
│                     │                    │                     │
│  Iceberg Tables     │ ──── OneLake ────▶ │  Lakehouse Tables   │
│  (write to Files/)  │                    │  (via shortcut)     │
│                     │                    │                     │
│  Iceberg Tables     │ ◀── OneLake ────── │  Delta/Iceberg      │
│  (METADATA_FILE_PATH│                    │  Tables (Tables/)   │
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
| Azure region match | Fabric capacity **must be in the same Azure region** as your Snowflake account. Check: bottom-left of Snowflake UI shows the Azure region. |
| Azure subscription | With Microsoft Entra ID |
| Snowflake account | Enterprise edition or higher |
| Snowflake role | ACCOUNTADMIN (for integration setup) |

> **Region check:** In Snowflake, go to the bottom-left account menu — it shows the cloud provider and region (e.g., `Azure East US 2`). Your Fabric capacity must be in the same region. If they differ, create a new Fabric capacity in the matching region before proceeding.

---

## Step 1: Create a Fabric Workspace

1. Go to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **New workspace**
3. Name: `snowflake-demo-workspace`
4. Select a Fabric capacity (F2+ or Trial) — **confirm it matches your Snowflake Azure region**
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
- Open lakehouse → click the "..." → Settings
- Copy the GUID from the URL: `.../lakehouses/<lakehouse_id>/...`
- This is needed for both external volume URLs

---

## Step 3: Grant Snowflake Service Principal Access

When Snowflake creates an external volume, it registers a service principal in Entra ID. You must grant this principal access to your Fabric workspace.

1. In Snowflake (Snowsight), run:
   ```sql
   DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;
   ```
2. From the output, copy:
   - **`AZURE_CONSENT_URL`** — the application consent link
   - **`AZURE_MULTI_TENANT_APP_NAME`** — the Snowflake service principal name
3. Open the `AZURE_CONSENT_URL` in a browser tab → click **Accept**
   (You may be redirected to the Snowflake homepage — that is expected)
4. In Fabric Portal, open your workspace (`snowflake-demo-workspace`)
5. Click **Manage access** (top-right of the workspace)
6. Click **Add people or groups**
7. Paste the `AZURE_MULTI_TENANT_APP_NAME` value into the search box
8. Select the Snowflake service principal → assign **Contributor** role → click **Add**

> **What this does:** `Contributor` lets Snowflake read and write files in any lakehouse in this workspace. The consent URL registers the Snowflake multi-tenant app in your Entra ID tenant — this is a standard OAuth app consent flow, not an IAM assignment.

---

## Step 4: Create Sample Data in Fabric

Create Fabric-managed tables that Snowflake will read back via the read external volume.

### Using a Fabric Notebook (PySpark):

1. In the workspace, click **New** → **Notebook**
2. Attach the notebook to `demo_lakehouse`
3. Paste and run the following:

```python
# In a Fabric Spark Notebook
from pyspark.sql.types import *
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
df.write.format("delta").mode("overwrite").saveAsTable("regional_sales_targets")

# Sample: Marketing campaign data
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

OneLake automatically generates Iceberg metadata alongside the Delta files. Snowflake reads this metadata via `METADATA_FILE_PATH` in `03_catalog_integration_onelake.sql`.

---

## Step 5: Create OneLake Shortcuts (Required for Fabric to See Snowflake Tables)

After running `02_iceberg_tables_to_fabric.sql`, Snowflake has written Iceberg files to the `Files/snowflake-iceberg/` folder in OneLake. To surface them as proper Lakehouse Tables in Fabric, you must create **OneLake shortcuts**.

> **Why shortcuts?** Fabric does not automatically scan the Files area for Iceberg tables. A shortcut explicitly maps a folder in Files to a table in the Tables section.

For each Snowflake Iceberg table:

1. Open `demo_lakehouse` in Fabric
2. In the **Tables** panel (left side), click **...** → **New shortcut**
3. Select **OneLake** as the source
4. Navigate to: `Files` → `snowflake-iceberg` → select the table folder
5. Click **Next** → confirm the name → **Create shortcut**

Repeat for each table:

| Shortcut Name | Source Path |
|---|---|
| `customer_360` | `Files/snowflake-iceberg/customer_360/` |
| `sales_summary` | `Files/snowflake-iceberg/sales_summary/` |
| `product_performance` | `Files/snowflake-iceberg/product_performance/` |
| `ml_predictions` | `Files/snowflake-iceberg/ml_predictions/` |

After creating shortcuts, tables appear in the Lakehouse Tables section and are queryable via Fabric SQL Endpoint, Spark Notebooks, and Power BI DirectLake mode.

---

## Step 6: Verify Bidirectional Access

### Snowflake → Fabric (verify in Fabric)

1. Open **demo_lakehouse** → **Tables** section
2. The shortcut tables should appear: `customer_360`, `sales_summary`, `product_performance`, `ml_predictions`
3. Query via **SQL Endpoint**:
   ```sql
   SELECT TOP 10 * FROM customer_360;
   SELECT region, SUM(net_revenue) AS total_revenue
   FROM sales_summary
   GROUP BY region
   ORDER BY total_revenue DESC;
   ```

### Fabric → Snowflake (verify in Snowflake)

```sql
-- After running 03_catalog_integration_onelake.sql:
SELECT * FROM MSFT_SNOWFLAKE_DEMO.ICEBERG.FABRIC_REGIONAL_TARGETS LIMIT 10;

-- Join Fabric targets with Snowflake actuals
SELECT
    t.region,
    t.quarter,
    t.target_revenue,
    s.NET_REVENUE AS actual_revenue,
    ROUND((s.NET_REVENUE / t.target_revenue) * 100, 2) AS attainment_pct
FROM MSFT_SNOWFLAKE_DEMO.ICEBERG.FABRIC_REGIONAL_TARGETS t
JOIN MSFT_SNOWFLAKE_DEMO.ANALYTICS.DT_SALES_SUMMARY s
    ON t.region = s.REGION
WHERE t.year = 2024;
```

---

## Step 7: Power BI Integration

### DirectLake Mode with Iceberg Tables

1. Open **Power BI** in your Fabric workspace
2. Click **New** → **Semantic Model (default)**
3. Select the Lakehouse containing the shortcut tables
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
| Tables not appearing in Fabric Tables section | OneLake shortcuts not created — follow Step 5 above |
| `SYSTEM$VERIFY_EXTERNAL_VOLUME` fails | Consent URL not accepted, or Snowflake service principal not assigned Contributor in Fabric workspace → redo Step 3 |
| Iceberg table read fails in Snowflake | `METADATA_FILE_PATH` is outdated — run `ALTER ICEBERG TABLE ... REFRESH '<new_metadata_file>'` |
| "Storage location not found" | Workspace ID or Lakehouse ID in external volume URL is wrong — verify from the Fabric lakehouse URL |
| Stale data in Snowflake | Fabric wrote new data → find new metadata file in OneLake and run `ALTER ICEBERG TABLE ... REFRESH` |
| Stale data in Fabric | Run `ALTER ICEBERG TABLE ... REFRESH;` in Snowflake, then the shortcut reflects latest data |
| Region mismatch error | Fabric capacity and Snowflake account must be in the same Azure region |

---

## Quick Reference — Values Used Across Scripts

| Parameter | Where to Get | Used In |
|---|---|---|
| `<workspace_id>` | Fabric workspace URL | External volumes, `01_account_setup.sql` |
| `<lakehouse_id>` | Fabric lakehouse URL | External volumes, `01_account_setup.sql` |
| `<azure_tenant_id>` | Entra ID → Overview | External volumes |
| `AZURE_CONSENT_URL` | `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL` output | Browser consent flow (Step 3) |
| `AZURE_MULTI_TENANT_APP_NAME` | `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL` output | Fabric workspace Manage Access (Step 3) |
| `<metadata_file_path>` | Fabric lakehouse → Tables → browse metadata/ folder | `03_catalog_integration_onelake.sql` |
