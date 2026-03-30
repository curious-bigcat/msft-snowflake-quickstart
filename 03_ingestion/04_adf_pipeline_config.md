# Azure Data Factory Pipeline Configuration Guide

## Overview
This guide walks through setting up ADF pipelines to load inventory and supplier data into Snowflake using the Snowflake V2 connector.

---

## Step 1: Create ADF Instance

1. Azure Portal → Create Resource → Data Factory
2. Name: `adf-snowflake-demo`
3. Region: Same as your storage account (e.g., East US 2)
4. Version: V2
5. Click **Review + Create**

---

## Step 2: Create Linked Services

### 2a. Snowflake V2 Linked Service

1. ADF Studio → Manage → Linked services → New
2. Search "Snowflake" → Select **Snowflake V2**
3. Configure:

| Property | Value |
|----------|-------|
| Name | `LS_Snowflake_Demo` |
| Account identifier | `<org>-<account>` (e.g., `myorg-myaccount`) |
| Database | `MSFT_SNOWFLAKE_DEMO` |
| Warehouse | `DEMO_WH` |
| Role | `DEMO_ADMIN` |
| Authentication type | Basic |
| User | Your Snowflake username |
| Password | Azure Key Vault reference (recommended) |

4. Test connection → Create

**JSON Payload:**
```json
{
    "name": "LS_Snowflake_Demo",
    "properties": {
        "type": "SnowflakeV2",
        "typeProperties": {
            "accountIdentifier": "<org>-<account>",
            "database": "MSFT_SNOWFLAKE_DEMO",
            "warehouse": "DEMO_WH",
            "authenticationType": "Basic",
            "user": "<your_username>",
            "password": {
                "type": "AzureKeyVaultSecret",
                "store": {
                    "referenceName": "<your_keyvault_linked_service>",
                    "type": "LinkedServiceReference"
                },
                "secretName": "snowflake-password"
            }
        },
        "connectVia": {
            "referenceName": "AutoResolveIntegrationRuntime",
            "type": "IntegrationRuntimeReference"
        }
    }
}
```

### 2b. Azure Blob Storage Linked Service

1. ADF Studio → Manage → Linked services → New
2. Search "Blob" → Select **Azure Blob Storage**
3. Configure:

| Property | Value |
|----------|-------|
| Name | `LS_AzureBlobStorage` |
| Authentication method | SAS URI or Account Key |
| Storage account | `snowflakedemostore` |

4. Test connection → Create

---

## Step 3: Create Datasets

### 3a. Source Dataset — CSV in Blob Storage

Create two source datasets:

**DS_CSV_Inventory:**
- Linked service: `LS_AzureBlobStorage`
- File path: `adf-staging / inventory / *.csv`
- Format: DelimitedText, first row as header

**DS_CSV_Suppliers:**
- Linked service: `LS_AzureBlobStorage`
- File path: `adf-staging / suppliers / *.csv`
- Format: DelimitedText, first row as header

### 3b. Sink Dataset — Snowflake Tables

**DS_SF_Inventory:**
- Linked service: `LS_Snowflake_Demo`
- Schema: `STAGING`
- Table: `ADF_INVENTORY`

**DS_SF_Suppliers:**
- Linked service: `LS_Snowflake_Demo`
- Schema: `STAGING`
- Table: `ADF_SUPPLIER_DATA`

---

## Step 4: Create Pipelines

### Pipeline: PL_Ingest_Inventory

1. ADF Studio → Author → Pipelines → New pipeline
2. Name: `PL_Ingest_Inventory`
3. Add **Copy Data** activity:

| Setting | Value |
|---------|-------|
| Source dataset | `DS_CSV_Inventory` |
| Sink dataset | `DS_SF_Inventory` |
| Staging account | `LS_AzureBlobStorage` |
| Staging path | `adf-staging/temp` |
| Pre-copy script | `TRUNCATE TABLE STAGING.ADF_INVENTORY` (optional) |

4. Column mapping: Auto-map or manually map columns
5. Settings:
   - Enable staging: **Yes** (required for Snowflake sink)
   - Staging linked service: `LS_AzureBlobStorage`

### Pipeline: PL_Ingest_Suppliers

Same pattern as above, using `DS_CSV_Suppliers` → `DS_SF_Suppliers`.

### Pipeline: PL_Master_Ingestion (Orchestrator)

1. Create a new pipeline: `PL_Master_Ingestion`
2. Add two **Execute Pipeline** activities:
   - Execute `PL_Ingest_Inventory`
   - Execute `PL_Ingest_Suppliers` (in parallel)
3. Add a **Script** activity at the end (optional):
   - Linked service: `LS_Snowflake_Demo`
   - Script: `SELECT 'ADF ingestion complete' AS STATUS, CURRENT_TIMESTAMP() AS COMPLETED_AT;`

---

## Step 5: Upload Sample Data to Blob Storage

Upload the sample CSV files to your Azure Blob Storage:

### inventory_data.csv
```csv
INVENTORY_ID,PRODUCT_ID,WAREHOUSE_LOCATION,QUANTITY_ON_HAND,QUANTITY_RESERVED,REORDER_POINT,LAST_RESTOCK_DATE,SUPPLIER_ID
1,42,Warehouse-East,1500,120,200,2026-02-15,5
2,87,Warehouse-West,3200,450,300,2026-03-01,12
3,15,Warehouse-Central,800,50,100,2026-01-20,3
4,123,Distribution-Hub-1,5000,800,500,2026-03-10,8
5,56,Warehouse-South,250,30,50,2026-02-28,15
```

### supplier_data.csv
```csv
SUPPLIER_ID,SUPPLIER_NAME,CONTACT_EMAIL,COUNTRY,LEAD_TIME_DAYS,RELIABILITY_SCORE,CONTRACT_START_DATE,CONTRACT_END_DATE
1,Global Technologies,contact@globaltech.com,United States,7,0.95,2025-01-01,2027-01-01
2,Pacific Solutions,info@pacificsol.com,Japan,14,0.88,2025-06-01,2026-12-01
3,Continental Systems,sales@contisys.com,Germany,10,0.92,2024-09-01,2026-09-01
```

Upload to:
- `adf-staging/inventory/inventory_data.csv`
- `adf-staging/suppliers/supplier_data.csv`

---

## Step 6: Trigger and Monitor

1. Click **Debug** on `PL_Master_Ingestion` to test
2. Monitor tab shows run status
3. Set up a **Schedule trigger** for recurring loads:
   - Every 6 hours or daily, depending on requirements

---

## Step 7: Verify in Snowflake

```sql
USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;

SELECT COUNT(*) FROM MSFT_SNOWFLAKE_DEMO.STAGING.ADF_INVENTORY;
SELECT COUNT(*) FROM MSFT_SNOWFLAKE_DEMO.STAGING.ADF_SUPPLIER_DATA;

-- Check ADF pipeline run IDs
SELECT DISTINCT ADF_PIPELINE_RUN_ID, COUNT(*) AS ROWS_LOADED
FROM STAGING.ADF_INVENTORY
GROUP BY ADF_PIPELINE_RUN_ID;
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Connection failed | Check account identifier format: `<org>-<account>`, not full URL |
| COPY INTO error | Ensure staging is enabled; Snowflake V2 requires blob staging |
| Permission denied | Verify `DEMO_ADMIN` role has INSERT on target tables |
| Timeout | Increase ADF activity timeout; check warehouse auto-resume |
| Data type mismatch | Review column mapping; ensure CSV headers match table columns |
