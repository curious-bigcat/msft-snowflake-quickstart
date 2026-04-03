-- =============================================================================
-- Fabric + AI Foundry Integration — Snowflake <-> Fabric SQL Setup
-- =============================================================================
-- Part 1: External Volumes — validate ONELAKE_EXTERNAL_VOL and ONELAKE_READ_VOL
-- Part 2: Iceberg Write-back — export Gold/ML tables to OneLake as Iceberg
-- Part 3: Fabric Catalog Integration — read Fabric-managed tables in Snowflake
--
-- Data flow:
--   Part 2 (Snowflake -> Fabric): Snowflake writes Iceberg to OneLake Files/
--                                 Fabric creates shortcuts to surface as tables
--   Part 3 (Fabric -> Snowflake): Fabric Delta tables generate Iceberg metadata
--                                 Snowflake reads via OBJECT_STORE catalog
--
-- Prerequisites: Run 01_setup/01_account_setup.sql first.
-- =============================================================================

-- =============================================================================
-- PART 1: EXTERNAL VOLUMES
-- =============================================================================
-- Validates ONELAKE_EXTERNAL_VOL (write) and ONELAKE_READ_VOL (read-only).
-- Both volumes were created in 01_setup/01_account_setup.sql.

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- Inspect write volume — copy AZURE_CONSENT_URL and AZURE_MULTI_TENANT_APP_NAME
DESCRIBE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;

-- Consent flow (one-time setup):
--   1. Copy AZURE_CONSENT_URL from the output above
--   2. Open in a browser and accept the consent prompt
--   3. In Fabric Portal (app.fabric.microsoft.com):
--      workspace -> Manage access -> Add people or groups
--      Paste AZURE_MULTI_TENANT_APP_NAME -> assign Contributor role -> Add

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_EXTERNAL_VOL');

-- Inspect read-only volume (same service principal, no second consent needed)
DESCRIBE EXTERNAL VOLUME ONELAKE_READ_VOL;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_READ_VOL');

-- Recreate write volume if needed (update with your actual IDs):
-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL
--   STORAGE_LOCATIONS = ((
--     NAME = 'onelake_write_vol'
--     STORAGE_PROVIDER = 'AZURE'
--     STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Files/snowflake-iceberg/'
--     AZURE_TENANT_ID = '<your_azure_tenant_id>'
--   ));
-- GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;

-- Recreate read volume if needed:
-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_READ_VOL
--   STORAGE_LOCATIONS = ((
--     NAME = 'onelake_read_vol'
--     STORAGE_PROVIDER = 'AZURE'
--     STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Tables/'
--     AZURE_TENANT_ID = '<your_azure_tenant_id>'
--   ))
--   ALLOW_WRITES = FALSE;
-- GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL TO ROLE DEMO_ADMIN;

GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ML_ENGINEER;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL     TO ROLE DEMO_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL     TO ROLE DEMO_ML_ENGINEER;

-- Troubleshooting:
-- "Storage access denied"    -> Consent URL not accepted or Contributor not assigned
-- "Storage location not found" -> Workspace ID or Lakehouse ID incorrect
-- "AZURE_TENANT_ID mismatch" -> Azure Portal -> Entra ID -> Overview to find tenant ID

SELECT 'Part 1 complete -- external volumes verified.' AS STATUS;

-- =============================================================================
-- PART 2: ICEBERG WRITE-BACK TO FABRIC  (Snowflake -> OneLake)
-- =============================================================================
-- Exports Gold and ML tables to OneLake as Snowflake-managed Iceberg.
-- Fabric users can access these via SQL Endpoint, Spark, and Power BI DirectLake.
--
-- Prerequisites: Run phases 01-04 (setup through Gold processing).
-- After running: create OneLake shortcuts in Fabric for each table.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

CREATE SCHEMA IF NOT EXISTS MSFT_SNOWFLAKE_DEMO.ICEBERG
  COMMENT = 'Iceberg tables for Snowflake-Fabric bidirectional access';

USE SCHEMA ICEBERG;
GRANT USAGE ON SCHEMA ICEBERG TO ROLE DEMO_ADMIN;
GRANT USAGE ON SCHEMA ICEBERG TO ROLE DEMO_ANALYST;

-- Customer 360 (Gold -> Fabric)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.CUSTOMER_360_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'customer_360/'
  COMMENT = 'Customer 360 data in Iceberg format -- readable from Fabric'
AS
SELECT
    CUSTOMER_ID, FIRST_NAME, LAST_NAME, CUSTOMER_EMAIL,
    CUSTOMER_SEGMENT, CUSTOMER_CITY, CUSTOMER_STATE,
    TOTAL_ORDERS, LIFETIME_VALUE, AVG_ORDER_VALUE,
    FIRST_ORDER_DATE, LAST_ORDER_DATE, DAYS_SINCE_LAST_ORDER,
    PREFERRED_PAYMENT, PREFERRED_CHANNEL, PRIMARY_REGION,
    CANCELLED_ORDERS, ENGAGEMENT_STATUS, CUSTOMER_TIER,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM GOLD.DT_CUSTOMER_360;

-- Sales Summary (Gold -> Fabric)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.SALES_SUMMARY_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'sales_summary/'
  COMMENT = 'Sales summary data in Iceberg format -- readable from Fabric'
AS
SELECT
    ORDER_MONTH, ORDER_YEAR, REGION, CHANNEL,
    CUSTOMER_SEGMENT, ORDER_VALUE_TIER,
    ORDER_COUNT, UNIQUE_CUSTOMERS, GROSS_REVENUE,
    TOTAL_DISCOUNTS, NET_REVENUE, AVG_ORDER_VALUE,
    MEDIAN_ORDER_VALUE, MAX_ORDER_VALUE,
    CANCELLED_ORDERS, CANCELLATION_RATE_PCT,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM GOLD.DT_SALES_SUMMARY;

-- Product Performance (Gold -> Fabric)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.PRODUCT_PERFORMANCE_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'product_performance/'
  COMMENT = 'Product performance data in Iceberg format -- readable from Fabric'
AS
SELECT
    PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY, BRAND, LIST_PRICE,
    ORDERS_CONTAINING_PRODUCT, TOTAL_UNITS_SOLD, TOTAL_REVENUE,
    AVG_SELLING_PRICE, AVG_DISCOUNT_PCT,
    AVG_RATING, REVIEW_COUNT, POSITIVE_REVIEW_PCT,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM GOLD.DT_PRODUCT_PERFORMANCE;

-- ML Predictions (ML Output -> Fabric)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.ML_PREDICTIONS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'ml_predictions/'
  COMMENT = 'ML regression predictions in Iceberg format -- readable from Fabric'
AS
SELECT
    ORDER_MONTH, CATEGORY,
    MONTHLY_REVENUE AS ACTUAL_REVENUE, PREDICTED_REVENUE,
    RESIDUAL, PCT_ERROR, MODEL_NAME, MODEL_VERSION,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM ML.REGRESSION_PREDICTIONS;

ALTER ICEBERG TABLE ICEBERG.CUSTOMER_360_ICEBERG       REFRESH;
ALTER ICEBERG TABLE ICEBERG.SALES_SUMMARY_ICEBERG      REFRESH;
ALTER ICEBERG TABLE ICEBERG.PRODUCT_PERFORMANCE_ICEBERG REFRESH;
ALTER ICEBERG TABLE ICEBERG.ML_PREDICTIONS_ICEBERG     REFRESH;

SHOW ICEBERG TABLES IN SCHEMA ICEBERG;

SELECT 'CUSTOMER_360_ICEBERG'       AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ICEBERG.CUSTOMER_360_ICEBERG
UNION ALL SELECT 'SALES_SUMMARY_ICEBERG',       COUNT(*) FROM ICEBERG.SALES_SUMMARY_ICEBERG
UNION ALL SELECT 'PRODUCT_PERFORMANCE_ICEBERG', COUNT(*) FROM ICEBERG.PRODUCT_PERFORMANCE_ICEBERG
UNION ALL SELECT 'ML_PREDICTIONS_ICEBERG',      COUNT(*) FROM ICEBERG.ML_PREDICTIONS_ICEBERG;

GRANT SELECT ON ALL TABLES IN SCHEMA ICEBERG TO ROLE DEMO_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ICEBERG TO ROLE DEMO_ANALYST;

-- Surface in Fabric via OneLake shortcuts (manual step):
--   Fabric workspace -> demo_lakehouse -> Tables -> "..." -> New shortcut -> OneLake
--   customer_360        -> Files/snowflake-iceberg/customer_360/
--   sales_summary       -> Files/snowflake-iceberg/sales_summary/
--   product_performance -> Files/snowflake-iceberg/product_performance/
--   ml_predictions      -> Files/snowflake-iceberg/ml_predictions/
-- Once shortcuts exist, tables are queryable via Fabric SQL Endpoint, Spark,
-- and Power BI DirectLake (reads Parquet files directly, no data import).

SELECT 'Part 2 complete -- Iceberg tables written to OneLake. Create shortcuts in Fabric.' AS STATUS;

-- =============================================================================
-- PART 3: FABRIC CATALOG INTEGRATION  (Fabric -> Snowflake)
-- =============================================================================
-- Reads Fabric-managed Delta/Iceberg tables directly in Snowflake.
-- No data is copied -- both platforms read the same Parquet files in OneLake.
--
-- How it works:
--   Fabric Delta tables -> OneLake auto-generates Iceberg metadata (*.metadata.json)
--   Snowflake reads via CATALOG_SOURCE=OBJECT_STORE + METADATA_FILE_PATH
--
-- Before running: update METADATA_FILE_PATH with real filenames from Fabric Portal.
-- Find paths: Fabric Portal -> table -> Properties -> ABFS path -> browse metadata/
-- Path is relative to ONELAKE_READ_VOL STORAGE_BASE_URL (pointing to Tables/).
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

CREATE OR REPLACE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT   = ICEBERG
  ENABLED        = TRUE
  COMMENT = 'Catalog integration for reading Fabric OneLake tables via Iceberg metadata';

DESCRIBE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT;
GRANT USAGE ON INTEGRATION FABRIC_ONELAKE_CATALOG_INT TO ROLE DEMO_ADMIN;

USE SCHEMA ICEBERG;

-- Update METADATA_FILE_PATH values before executing:

CREATE OR REPLACE ICEBERG TABLE ICEBERG.FABRIC_REGIONAL_TARGETS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'regional_sales_targets/metadata/<latest>.metadata.json'
  COMMENT = 'Fabric-managed regional sales targets -- read via OneLake Iceberg';

CREATE OR REPLACE ICEBERG TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'marketing_campaigns/metadata/<latest>.metadata.json'
  COMMENT = 'Fabric-managed marketing campaign data -- read via OneLake Iceberg';

GRANT SELECT ON TABLE ICEBERG.FABRIC_REGIONAL_TARGETS    TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.FABRIC_REGIONAL_TARGETS    TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS TO ROLE DEMO_ANALYST;

SELECT * FROM ICEBERG.FABRIC_REGIONAL_TARGETS    LIMIT 10;
SELECT * FROM ICEBERG.FABRIC_MARKETING_CAMPAIGNS LIMIT 10;

-- Join Fabric targets with Snowflake actuals:
SELECT
    t.region,
    t.fiscal_quarter                              AS quarter,
    t.revenue_target,
    s.NET_REVENUE                                 AS actual_revenue,
    ROUND((s.NET_REVENUE / t.revenue_target) * 100, 2) AS attainment_pct
FROM ICEBERG.FABRIC_REGIONAL_TARGETS t
JOIN MSFT_SNOWFLAKE_DEMO.GOLD.DT_SALES_SUMMARY s
    ON t.region = s.REGION
WHERE t.fiscal_year = 2024;

-- Refresh after Fabric writes new data:
-- ALTER ICEBERG TABLE ICEBERG.FABRIC_REGIONAL_TARGETS    REFRESH 'regional_sales_targets/metadata/<new>.metadata.json';
-- ALTER ICEBERG TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS REFRESH 'marketing_campaigns/metadata/<new>.metadata.json';

-- Bidirectional summary:
--   Part 2 (Snowflake -> Fabric): EXTERNAL_VOLUME=ONELAKE_EXTERNAL_VOL, CATALOG='SNOWFLAKE'
--                                 Files land in OneLake Files/snowflake-iceberg/
--   Part 3 (Fabric -> Snowflake): EXTERNAL_VOLUME=ONELAKE_READ_VOL, CATALOG=FABRIC_ONELAKE_CATALOG_INT
--                                 Reads Fabric Delta tables via auto-generated Iceberg metadata

SELECT 'Part 3 complete -- Fabric catalog integration ready. Fabric data is queryable from Snowflake.' AS STATUS;
