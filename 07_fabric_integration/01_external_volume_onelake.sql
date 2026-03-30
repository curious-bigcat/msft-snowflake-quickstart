-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: External Volume for OneLake
-- =============================================================================
-- Configures the external volume and validates connectivity to Microsoft
-- Fabric OneLake for bidirectional Iceberg table access.
--
-- The external volume was initially created in 01_setup/01_account_setup.sql.
-- This script validates it and sets up the consent flow.
--
-- Prerequisites:
--   - Azure tenant ID and Fabric workspace ID
--   - Fabric workspace with Contributor role for Snowflake's service principal
--   - Run 01_setup/01_account_setup.sql first
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. VERIFY EXTERNAL VOLUME
-- =============================================================================
-- The external volume ONELAKE_EXTERNAL_VOL was created in account setup.
-- Let's verify it and extract the consent URL.

DESCRIBE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;

-- The DESCRIBE output includes:
--   AZURE_CONSENT_URL  — Open this URL in a browser to grant consent
--   AZURE_MULTI_TENANT_APP_NAME — The Snowflake service principal name
--
-- Action required:
--   1. Copy the AZURE_CONSENT_URL value
--   2. Open it in a browser and accept the consent prompt
--   3. In Azure Portal, go to your Fabric workspace
--   4. Grant the Snowflake service principal "Contributor" role

-- =============================================================================
-- 2. RECREATE IF NEEDED (with your actual values)
-- =============================================================================
-- If you need to update the external volume with correct values:

-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL
--   STORAGE_LOCATIONS = (
--     (
--       NAME = 'onelake_vol'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<your_workspace_id>/.SnowflakeDatabase/SnowflakeVolume/'
--       AZURE_TENANT_ID = '<your_azure_tenant_id>'
--     )
--   );
--
-- GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 3. VALIDATE CONNECTIVITY
-- =============================================================================
-- After granting consent and Contributor role, test the connection:

-- This will fail if consent hasn't been granted yet — that's expected.
-- Once consent is granted, it should succeed.

SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME('ONELAKE_EXTERNAL_VOL');

-- =============================================================================
-- 4. ALTERNATIVE: EXTERNAL VOLUME WITH ALLOWLIST
-- =============================================================================
-- For tighter security, you can allowlist specific paths:

-- CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL_RESTRICTED
--   STORAGE_LOCATIONS = (
--     (
--       NAME = 'onelake_restricted'
--       STORAGE_PROVIDER = 'AZURE'
--       STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Tables/'
--       AZURE_TENANT_ID = '<your_azure_tenant_id>'
--     )
--   )
--   ALLOW_WRITES = TRUE;

-- =============================================================================
-- 5. GRANT TO ROLES
-- =============================================================================

GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;
GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ML_ENGINEER;

-- =============================================================================
-- 6. TROUBLESHOOTING
-- =============================================================================
-- Common issues:
--
-- Error: "Storage access denied"
--   → Consent URL not accepted or service principal not granted Contributor
--   → Run: DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;
--   → Copy AZURE_CONSENT_URL and accept in browser
--
-- Error: "Storage location not found"
--   → Workspace ID or Lakehouse ID is incorrect
--   → Verify in Fabric Portal: Workspace → Settings → Details
--
-- Error: "AZURE_TENANT_ID mismatch"
--   → The tenant ID in the external volume doesn't match your Azure AD
--   → Find correct tenant ID: Azure Portal → Microsoft Entra ID → Overview

SELECT 'External volume configured. Complete the consent flow before proceeding.' AS STATUS;
