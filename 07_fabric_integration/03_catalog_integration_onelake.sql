-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Read Fabric Tables from Snowflake via OneLake
-- =============================================================================
-- Creates a catalog integration and individual Iceberg table references to read
-- data managed by Microsoft Fabric from Snowflake.
--
-- This is the "Fabric → Snowflake" direction of bidirectional access:
--   Fabric stores Delta tables in OneLake → Fabric auto-generates Iceberg
--   metadata → Snowflake reads via OBJECT_STORE catalog + METADATA_FILE_PATH
--
-- How it works:
--   1. Fabric Lakehouse tables are Delta format, but OneLake automatically
--      generates Iceberg metadata (*.metadata.json) alongside them.
--   2. Snowflake uses CATALOG_SOURCE = OBJECT_STORE — no REST catalog server,
--      no OAuth, no Entra app credentials. Access is granted via the
--      ONELAKE_READ_VOL external volume (same service principal as the write
--      volume, Contributor on Fabric workspace).
--   3. Each Fabric table = one Iceberg table definition in Snowflake,
--      pointing to its latest *.metadata.json file.
--
-- Prerequisites:
--   - ONELAKE_READ_VOL external volume configured (01_account_setup.sql)
--   - Consent URL opened and accepted (DESC EXTERNAL VOLUME ONELAKE_READ_VOL)
--   - Snowflake service principal (AZURE_MULTI_TENANT_APP_NAME) has
--     Contributor role on your Fabric workspace (Manage access)
--   - Fabric Lakehouse has tables with Iceberg metadata generated
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. CREATE CATALOG INTEGRATION — OBJECT_STORE (no credentials required)
-- =============================================================================
-- OBJECT_STORE tells Snowflake to read Iceberg metadata directly from the
-- files in the external volume. No REST catalog server, no OAuth, no URI.
-- Access is controlled entirely by the external volume's storage permissions.

CREATE OR REPLACE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE
  COMMENT = 'Catalog integration for reading Fabric OneLake tables via Iceberg metadata files';

DESCRIBE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT;

GRANT USAGE ON INTEGRATION FABRIC_ONELAKE_CATALOG_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 2. FIND THE METADATA FILE PATH IN FABRIC
-- =============================================================================
-- Before creating each Iceberg table in Snowflake, locate the latest
-- *.metadata.json file for the Fabric table.
--
-- How to find the path:
--   1. Open your Fabric Lakehouse portal
--   2. Click the "..." next to a table → Properties → copy the ABFS path
--      e.g.: abfss://<workspace_id>@onelake.dfs.fabric.microsoft.com/<lakehouse_id>/Tables/regional_sales_targets
--   3. Browse to Tables/<table_name>/metadata/
--   4. The latest file is the highest-numbered *.metadata.json
--      e.g.: 00001-a1b2c3d4-....metadata.json
--   5. METADATA_FILE_PATH is relative to ONELAKE_READ_VOL's STORAGE_BASE_URL
--      (which points to Tables/), so the path is just:
--      '<table_name>/metadata/<filename>.metadata.json'
--
--   Alternatively, use OneLake Explorer on Windows to browse the Tables folder.

-- =============================================================================
-- 3. CREATE ICEBERG TABLES — One per Fabric Lakehouse Table
-- =============================================================================
-- Each Fabric table requires one Iceberg table definition referencing the
-- latest metadata file. Replace <latest> with the actual filename.

USE SCHEMA ICEBERG;

-- Regional Sales Targets (created in Fabric via PySpark)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.FABRIC_REGIONAL_TARGETS
  EXTERNAL_VOLUME = 'ONELAKE_READ_VOL'
  CATALOG = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'regional_sales_targets/metadata/<latest>.metadata.json'
  COMMENT = 'Fabric-managed regional sales targets — read via OneLake Iceberg';

-- Marketing Campaigns (created in Fabric via PySpark)
CREATE OR REPLACE ICEBERG TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS
  EXTERNAL_VOLUME = 'ONELAKE_READ_VOL'
  CATALOG = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'marketing_campaigns/metadata/<latest>.metadata.json'
  COMMENT = 'Fabric-managed marketing campaign data — read via OneLake Iceberg';

-- =============================================================================
-- 4. GRANT READ ACCESS
-- =============================================================================

GRANT SELECT ON TABLE ICEBERG.FABRIC_REGIONAL_TARGETS TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.FABRIC_REGIONAL_TARGETS TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS TO ROLE DEMO_ANALYST;

-- =============================================================================
-- 5. QUERY FABRIC DATA FROM SNOWFLAKE
-- =============================================================================

-- Sample query: Fabric-managed targets
SELECT * FROM ICEBERG.FABRIC_REGIONAL_TARGETS LIMIT 10;
SELECT * FROM ICEBERG.FABRIC_MARKETING_CAMPAIGNS LIMIT 10;

-- Join Fabric targets with Snowflake actuals
SELECT
    t.region,
    t.quarter,
    t.target_revenue,
    s.NET_REVENUE AS actual_revenue,
    ROUND((s.NET_REVENUE / t.target_revenue) * 100, 2) AS attainment_pct
FROM ICEBERG.FABRIC_REGIONAL_TARGETS t
JOIN MSFT_SNOWFLAKE_DEMO.ANALYTICS.DT_SALES_SUMMARY s
    ON t.region = s.REGION
WHERE t.year = 2024;

-- =============================================================================
-- 6. REFRESH WHEN FABRIC DATA CHANGES
-- =============================================================================
-- When Fabric writes new data, it creates a new metadata file. Point the
-- Iceberg table to the updated metadata file path:

-- ALTER ICEBERG TABLE ICEBERG.FABRIC_REGIONAL_TARGETS
--   REFRESH 'regional_sales_targets/metadata/<new_version>.metadata.json';

-- ALTER ICEBERG TABLE ICEBERG.FABRIC_MARKETING_CAMPAIGNS
--   REFRESH 'marketing_campaigns/metadata/<new_version>.metadata.json';

-- =============================================================================
-- 7. BIDIRECTIONAL SUMMARY
-- =============================================================================
-- DIRECTION 1: Snowflake → Fabric (02_iceberg_tables_to_fabric.sql)
--   Snowflake writes Iceberg tables → OneLake Files/snowflake-iceberg/ →
--   Fabric Lakehouse creates OneLake shortcuts → Tables appear in Fabric
--   External volume: ONELAKE_EXTERNAL_VOL
--
-- DIRECTION 2: Fabric → Snowflake (this script)
--   Fabric Delta tables → OneLake auto-generates Iceberg metadata →
--   Snowflake reads via OBJECT_STORE catalog + METADATA_FILE_PATH
--   External volume: ONELAKE_READ_VOL (ALLOW_WRITES = FALSE)
--   Catalog: FABRIC_ONELAKE_CATALOG_INT (OBJECT_STORE, no credentials)
--
-- No data is copied. Both platforms read the same files in OneLake.

SELECT 'Catalog integration and Iceberg table references created. Fabric data is queryable from Snowflake.' AS STATUS;
