-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: External Volumes for OneLake
-- =============================================================================
-- Configures and validates the two external volumes used for bidirectional
-- access between Snowflake and Microsoft Fabric OneLake:
--
--   ONELAKE_EXTERNAL_VOL  — WRITE: Snowflake → OneLake Files area
--   ONELAKE_READ_VOL      — READ:  Fabric OneLake Tables area → Snowflake
--
-- Both volumes were created in 01_setup/01_account_setup.sql.
-- This script validates them and walks through the consent flow.
--
-- Prerequisites:
--   - Azure tenant ID, Fabric workspace ID, and lakehouse ID
--   - Run 01_setup/01_account_setup.sql first
--   - Fabric capacity in the SAME Azure region as your Snowflake account
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. VERIFY WRITE EXTERNAL VOLUME (Snowflake → Fabric)
-- =============================================================================
-- The write volume points to the Files area of the Fabric Lakehouse.
-- Snowflake writes Iceberg Parquet files and metadata here.

DESCRIBE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;

-- The DESCRIBE output includes:
--   AZURE_CONSENT_URL          — Open this URL in a browser to grant consent
--   AZURE_MULTI_TENANT_APP_NAME — The Snowflake service principal name
--
-- Action required:
--   1. Copy the AZURE_CONSENT_URL value
--   2. Open it in a browser and accept the consent prompt
--   3. In Fabric Portal (app.fabric.microsoft.com), open your workspace
--   4. Click Manage access → Add people or groups
--   5. Paste the AZURE_MULTI_TENANT_APP_NAME value → assign Contributor role → Add

-- =============================================================================
-- 2. RECREATE WRITE VOLUME IF NEEDED
-- =============================================================================
-- If you need to update the write external volume with correct values:

-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL
--   STORAGE_LOCATIONS = (
--     (
--       NAME = 'onelake_write_vol'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<your_workspace_id>/<your_lakehouse_id>/Files/snowflake-iceberg/'
--       AZURE_TENANT_ID = '<your_azure_tenant_id>'
--     )
--   );
--
-- GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 3. VALIDATE WRITE VOLUME CONNECTIVITY
-- =============================================================================
-- This will fail if consent has not been granted yet — that is expected.
-- Once consent is granted and the Contributor role is assigned, it succeeds.

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_EXTERNAL_VOL');

-- =============================================================================
-- 4. VERIFY READ EXTERNAL VOLUME (Fabric → Snowflake)
-- =============================================================================
-- The read volume points to the Tables area of the Fabric Lakehouse.
-- Fabric stores Delta tables and auto-generated Iceberg metadata here.
-- ALLOW_WRITES = FALSE prevents accidental writes to Fabric-managed data.
--
-- The same Snowflake service principal (AZURE_MULTI_TENANT_APP_NAME) is
-- reused — no additional consent step needed once ONELAKE_EXTERNAL_VOL
-- consent is already accepted.

DESCRIBE EXTERNAL VOLUME ONELAKE_READ_VOL;

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_READ_VOL');

-- Recreate read volume if needed:
-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_READ_VOL
--   STORAGE_LOCATIONS = (
--     (
--       NAME = 'onelake_read_vol'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<your_workspace_id>/<your_lakehouse_id>/Tables/'
--       AZURE_TENANT_ID = '<your_azure_tenant_id>'
--     )
--   )
--   ALLOW_WRITES = FALSE;
--
-- GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 5. GRANT TO ROLES
-- =============================================================================

GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ML_ENGINEER;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL TO ROLE DEMO_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL TO ROLE DEMO_ML_ENGINEER;

-- =============================================================================
-- 6. TROUBLESHOOTING
-- =============================================================================
-- Error: "Storage access denied"
--   → Consent URL not accepted or service principal not granted Contributor
--   → Run: DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;
--   → Copy AZURE_CONSENT_URL and accept in browser
--   → In Fabric: workspace → Manage access → add AZURE_MULTI_TENANT_APP_NAME
--
-- Error: "Storage location not found"
--   → Workspace ID or Lakehouse ID is incorrect in the external volume URL
--   → Verify in Fabric Portal: open lakehouse → Files → Properties → copy URL
--   → URL format: https://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>
--
-- Error: "AZURE_TENANT_ID mismatch"
--   → The tenant ID in the external volume does not match your Entra ID
--   → Find correct tenant ID: Azure Portal → Microsoft Entra ID → Overview

SELECT 'External volumes verified. Complete the consent flow before proceeding.' AS STATUS;
