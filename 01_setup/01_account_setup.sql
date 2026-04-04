-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Account Foundation Setup
-- =============================================================================
-- This script creates the foundational Snowflake objects needed for the
-- Microsoft + Snowflake integration hands-on lab. Run as ACCOUNTADMIN.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. CUSTOM ROLES
-- =============================================================================

CREATE ROLE IF NOT EXISTS DEMO_ADMIN
  COMMENT = 'Admin role for the MSFT-Snowflake demo lab';

CREATE ROLE IF NOT EXISTS DEMO_ANALYST
  COMMENT = 'Analyst role for querying demo data';

CREATE ROLE IF NOT EXISTS DEMO_ML_ENGINEER
  COMMENT = 'ML engineer role for model training and deployment';

CREATE ROLE IF NOT EXISTS DEMO_AGENT_USER
  COMMENT = 'Role for Cortex Agent and Intelligence access';

-- Role hierarchy
GRANT ROLE DEMO_ANALYST TO ROLE DEMO_ADMIN;
GRANT ROLE DEMO_ML_ENGINEER TO ROLE DEMO_ADMIN;
GRANT ROLE DEMO_AGENT_USER TO ROLE DEMO_ADMIN;
GRANT ROLE DEMO_ADMIN TO ROLE SYSADMIN;

-- Cortex database roles
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DEMO_ADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DEMO_ANALYST;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE DEMO_AGENT_USER;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE DEMO_ADMIN;
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE DEMO_AGENT_USER;

-- =============================================================================
-- 2. WAREHOUSES
-- =============================================================================

CREATE WAREHOUSE IF NOT EXISTS DEMO_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'General-purpose warehouse for demo queries';

CREATE WAREHOUSE IF NOT EXISTS DEMO_ML_WH
  WITH WAREHOUSE_SIZE = 'MEDIUM'
  WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
  MAX_CONCURRENCY_LEVEL = 1
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Snowpark-optimized warehouse for ML training';

CREATE WAREHOUSE IF NOT EXISTS DEMO_CORTEX_WH
  WITH WAREHOUSE_SIZE = 'SMALL'
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for Cortex AI services';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE DEMO_WH TO ROLE DEMO_ADMIN;
GRANT USAGE ON WAREHOUSE DEMO_WH TO ROLE DEMO_ANALYST;
GRANT USAGE ON WAREHOUSE DEMO_ML_WH TO ROLE DEMO_ADMIN;
GRANT USAGE ON WAREHOUSE DEMO_ML_WH TO ROLE DEMO_ML_ENGINEER;
GRANT USAGE ON WAREHOUSE DEMO_CORTEX_WH TO ROLE DEMO_ADMIN;
GRANT USAGE ON WAREHOUSE DEMO_CORTEX_WH TO ROLE DEMO_ANALYST;
GRANT USAGE ON WAREHOUSE DEMO_CORTEX_WH TO ROLE DEMO_AGENT_USER;

-- =============================================================================
-- 3. DATABASE AND SCHEMAS
-- =============================================================================

CREATE DATABASE IF NOT EXISTS MSFT_SNOWFLAKE_DEMO
  COMMENT = 'Database for Microsoft + Snowflake integration lab';

GRANT OWNERSHIP ON DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN
  COPY CURRENT GRANTS;

USE DATABASE MSFT_SNOWFLAKE_DEMO;

CREATE SCHEMA IF NOT EXISTS BRONZE
  COMMENT = 'Landing zone for raw source data';
CREATE SCHEMA IF NOT EXISTS SILVER
  COMMENT = 'Cleansed and enriched data';
CREATE SCHEMA IF NOT EXISTS GOLD
  COMMENT = 'Aggregated and consumption-ready data';
CREATE SCHEMA IF NOT EXISTS ML
  COMMENT = 'ML features, models, and predictions';
CREATE SCHEMA IF NOT EXISTS AGENTS
  COMMENT = 'Cortex Agents, Search services, and Semantic views';

-- Grant schema privileges to DEMO_ADMIN
GRANT ALL ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;

-- Grant create privileges
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE DYNAMIC TABLE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE STREAM ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE TASK ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE PIPE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE STAGE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE FUNCTION ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE PROCEDURE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT CREATE ICEBERG TABLE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DEMO_ADMIN;

-- Analyst read access
GRANT USAGE ON DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ANALYST;

-- ML Engineer access
GRANT USAGE ON DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ML_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ML_ENGINEER;
GRANT SELECT ON ALL TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ML_ENGINEER;
GRANT SELECT ON FUTURE TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_ML_ENGINEER;
GRANT CREATE TABLE ON SCHEMA ML TO ROLE DEMO_ML_ENGINEER;
GRANT CREATE STAGE ON SCHEMA ML TO ROLE DEMO_ML_ENGINEER;
GRANT CREATE FUNCTION ON SCHEMA ML TO ROLE DEMO_ML_ENGINEER;

-- Agent user access
GRANT USAGE ON DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_AGENT_USER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_AGENT_USER;
GRANT SELECT ON ALL TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_AGENT_USER;
GRANT SELECT ON FUTURE TABLES IN DATABASE MSFT_SNOWFLAKE_DEMO TO ROLE DEMO_AGENT_USER;

-- =============================================================================
-- 4. AZURE STORAGE INTEGRATION (for ADLS Gen2)
-- =============================================================================
-- Replace placeholders with your Azure values

CREATE OR REPLACE STORAGE INTEGRATION AZURE_STORAGE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://<your_storage_account>.blob.core.windows.net/<your_container>/'
  );

-- Run this to get the consent URL and service principal name:
-- DESC STORAGE INTEGRATION AZURE_STORAGE_INT;
-- Then grant 'Storage Blob Data Contributor' to the AZURE_MULTI_TENANT_APP_NAME
-- in your Azure Storage Account's IAM settings.

GRANT USAGE ON INTEGRATION AZURE_STORAGE_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 5. EXTERNAL STAGE + SNOWPIPE NOTIFICATION INTEGRATION
-- =============================================================================
-- External stage points to the ADLS Gen2 container used for data landing.
-- The notification integration triggers Snowpipe automatically when new
-- files arrive (via Azure Storage Queue events).
-- =============================================================================

USE SCHEMA MSFT_SNOWFLAKE_DEMO.BRONZE;

-- External stage — replace placeholders with your Azure values
CREATE OR REPLACE STAGE BRONZE.ADLS_DATA_STAGE
  URL = 'azure://<your_storage_account>.blob.core.windows.net/snowflake-data/'
  STORAGE_INTEGRATION = AZURE_STORAGE_INT
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL'))
  COMMENT = 'External stage for ADLS Gen2 data landing container';

GRANT USAGE ON STAGE BRONZE.ADLS_DATA_STAGE TO ROLE DEMO_ADMIN;

-- List files in the stage to verify connectivity:
-- LIST @BRONZE.ADLS_DATA_STAGE;

-- Notification integration — triggers Snowpipe when files land in ADLS
-- Requires an Azure Storage Queue connected to blob create events.
-- Steps:
--   1. Create a Storage Queue in your Azure Storage Account
--   2. Add an Event Grid subscription: source = storage account,
--      event type = "Blob Created", endpoint = the Storage Queue
--   3. Replace <your_azure_tenant_id>, <your_storage_account>, <your_queue>
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<your_storage_account>.queue.core.windows.net/<your_queue>'
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  COMMENT = 'Notification integration for Snowpipe auto-ingest from ADLS Gen2';

-- Run this to get the service principal that needs Queue permissions:
-- DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT;
-- Then assign 'Storage Queue Data Contributor' IAM role to AZURE_MULTI_TENANT_APP_NAME
-- on the Storage Queue in Azure Portal.

GRANT USAGE ON INTEGRATION AZURE_SNOWPIPE_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 6. EXTERNAL VOLUMES FOR ONELAKE / FABRIC
-- =============================================================================
-- Two external volumes are needed for bidirectional access:
--   ONELAKE_EXTERNAL_VOL  — write Iceberg tables from Snowflake → OneLake Files area
--   ONELAKE_READ_VOL      — read Fabric-managed tables from OneLake Tables area

-- WRITE volume: points to the Files area of the Fabric Lakehouse
-- Snowflake writes Iceberg metadata + Parquet files here.
-- URL pattern: azure://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Files/
CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL
  STORAGE_LOCATIONS = (
    (
      NAME = 'onelake_write_vol'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<your_workspace_id>/<your_lakehouse_id>/Files/snowflake-iceberg/'
      AZURE_TENANT_ID = '<your_azure_tenant_id>'
    )
  );

-- Run DESC to get the consent URL and Snowflake service principal name:
-- DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;
-- Open AZURE_CONSENT_URL in a browser → accept the prompt.
-- Then in Fabric: workspace → Manage access → add AZURE_MULTI_TENANT_APP_NAME → Contributor.

GRANT USAGE ON EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL TO ROLE DEMO_ADMIN;

-- READ volume: points to the Tables area of the Fabric Lakehouse
-- Snowflake reads Delta/Iceberg tables managed by Fabric.
-- ALLOW_WRITES = FALSE prevents accidental writes to Fabric-managed data.
-- URL pattern: azure://onelake.dfs.fabric.microsoft.com/<workspace_id>/<lakehouse_id>/Tables/
CREATE OR REPLACE EXTERNAL VOLUME ONELAKE_READ_VOL
  STORAGE_LOCATIONS = (
    (
      NAME = 'onelake_read_vol'
      STORAGE_PROVIDER = 'AZURE'
      STORAGE_BASE_URL = 'azure://onelake.dfs.fabric.microsoft.com/<your_workspace_id>/<your_lakehouse_id>/Tables/'
      AZURE_TENANT_ID = '<your_azure_tenant_id>'
    )
  )
  ALLOW_WRITES = FALSE;

-- DESC to get consent URL (same Snowflake app, same consent — only needed once):
-- DESC EXTERNAL VOLUME ONELAKE_READ_VOL;

GRANT USAGE ON EXTERNAL VOLUME ONELAKE_READ_VOL TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 7. CROSS-REGION INFERENCE
-- =============================================================================
-- Enable if your Snowflake region doesn't have full Cortex model support

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

-- =============================================================================
-- 8. FILE FORMATS
-- =============================================================================

USE SCHEMA BRONZE;

CREATE OR REPLACE FILE FORMAT CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Standard CSV format with header';

CREATE OR REPLACE FILE FORMAT JSON_FORMAT
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = TRUE
  STRIP_NULL_VALUES = FALSE
  COMMENT = 'Standard JSON format';

CREATE OR REPLACE FILE FORMAT PARQUET_FORMAT
  TYPE = 'PARQUET'
  COMMENT = 'Standard Parquet format';

-- =============================================================================
-- 9. VERIFICATION
-- =============================================================================

SHOW ROLES LIKE 'DEMO%';
SHOW WAREHOUSES LIKE 'DEMO%';
SHOW SCHEMAS IN DATABASE MSFT_SNOWFLAKE_DEMO;
SHOW STORAGE INTEGRATIONS;
SHOW NOTIFICATION INTEGRATIONS;
SHOW STAGES IN SCHEMA MSFT_SNOWFLAKE_DEMO.BRONZE;
SHOW FILE FORMATS IN SCHEMA MSFT_SNOWFLAKE_DEMO.BRONZE;

SELECT 'Setup complete. Review the output above to verify all objects were created.' AS STATUS;
