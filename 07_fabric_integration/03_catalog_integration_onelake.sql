-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Catalog Integration — Read Fabric from Snowflake
-- =============================================================================
-- Creates a catalog integration and catalog-linked database to read data
-- that lives in Microsoft Fabric from Snowflake.
--
-- This is the "Fabric → Snowflake" direction of bidirectional access:
--   Fabric stores data in OneLake → Snowflake reads it via catalog integration
--
-- Uses Iceberg REST Catalog (ICEBERG_REST) to discover and read tables
-- managed by Fabric's Lakehouse.
--
-- Prerequisites:
--   - External volume ONELAKE_EXTERNAL_VOL configured
--   - Fabric Lakehouse with tables created
--   - Fabric SQL Endpoint URL and credentials
-- =============================================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;

-- =============================================================================
-- 1. CREATE CATALOG INTEGRATION — OneLake / Fabric
-- =============================================================================
-- A catalog integration connects Snowflake to Fabric's Iceberg catalog,
-- allowing Snowflake to discover and read tables managed by Fabric.

CREATE OR REPLACE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_URI = 'https://onelake.dfs.fabric.microsoft.com'
  CATALOG_NAMESPACE = '<your_workspace_id>.<your_lakehouse_id>'
  REST_CONFIG = (
    CATALOG_API_TYPE = MICROSOFT_FABRIC
  )
  REST_AUTHENTICATION = (
    TYPE = EXTERNAL_OAUTH
    OAUTH_TOKEN_URI = 'https://login.microsoftonline.com/<your_azure_tenant_id>/oauth2/v2.0/token'
    OAUTH_CLIENT_ID = '<your_entra_app_client_id>'
    OAUTH_CLIENT_SECRET = '<your_entra_app_client_secret>'
    OAUTH_ALLOWED_SCOPES = ('https://storage.azure.com/.default')
  )
  ENABLED = TRUE
  COMMENT = 'Catalog integration for reading Fabric OneLake data via Iceberg REST';

-- Verify the catalog integration
DESCRIBE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT;

-- Grant to admin role
GRANT USAGE ON INTEGRATION FABRIC_ONELAKE_CATALOG_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 2. CREATE CATALOG-LINKED DATABASE
-- =============================================================================
-- A catalog-linked database auto-discovers tables from the Fabric catalog
-- and makes them queryable in Snowflake as read-only Iceberg tables.

CREATE OR REPLACE DATABASE FABRIC_DATA
  FROM CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT
  AUTO_REFRESH = TRUE
  COMMENT = 'Catalog-linked database — reads Fabric Lakehouse tables via OneLake';

-- Grant access
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_ADMIN;
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_ANALYST;

-- =============================================================================
-- 3. DISCOVER FABRIC TABLES
-- =============================================================================
-- After the catalog-linked database is created, Snowflake discovers tables
-- from the Fabric Lakehouse automatically.

SHOW SCHEMAS IN DATABASE FABRIC_DATA;

-- List all tables discovered from Fabric
SHOW TABLES IN DATABASE FABRIC_DATA;

-- =============================================================================
-- 4. QUERY FABRIC DATA FROM SNOWFLAKE
-- =============================================================================
-- Once tables are discovered, query them like any Snowflake table.
-- These queries execute against the Iceberg files in OneLake.

-- Example: Query a Fabric-managed table
-- SELECT * FROM FABRIC_DATA.<schema_name>.<table_name> LIMIT 10;

-- Example: Join Fabric data with Snowflake data
-- SELECT
--     f.customer_id,
--     f.fabric_metric,
--     s.LIFETIME_VALUE,
--     s.CUSTOMER_TIER
-- FROM FABRIC_DATA.<schema_name>.<fabric_table> f
-- JOIN MSFT_SNOWFLAKE_DEMO.ANALYTICS.DT_CUSTOMER_360 s
--     ON f.customer_id = s.CUSTOMER_ID;

-- =============================================================================
-- 5. ALTERNATIVE — Manual Iceberg Table from OneLake
-- =============================================================================
-- If catalog integration is not available, you can create individual
-- Iceberg tables pointing to specific Fabric data:

-- CREATE OR REPLACE ICEBERG TABLE MSFT_SNOWFLAKE_DEMO.ICEBERG.FABRIC_SALES_DATA
--   CATALOG = 'ICEBERG_REST'
--   CATALOG_TABLE_NAME = 'sales_data'
--   EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
--   CATALOG_NAMESPACE = '<workspace_id>.<lakehouse_id>'
--   COMMENT = 'Fabric sales data read via Iceberg REST catalog';

-- =============================================================================
-- 6. AUTO-REFRESH CONFIGURATION
-- =============================================================================
-- The catalog-linked database refreshes automatically, but you can also
-- trigger a manual refresh:

-- ALTER DATABASE FABRIC_DATA REFRESH;

-- Check refresh status:
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.CATALOG_REFRESH_HISTORY('FABRIC_DATA'))
-- ORDER BY REFRESH_START_TIME DESC
-- LIMIT 10;

-- =============================================================================
-- 7. BIDIRECTIONAL SUMMARY
-- =============================================================================
-- At this point, bidirectional data access is configured:
--
-- DIRECTION 1: Snowflake → Fabric (02_iceberg_tables_to_fabric.sql)
--   Snowflake Iceberg tables → OneLake → Fabric reads as Lakehouse tables
--   Tables: CUSTOMER_360_ICEBERG, SALES_SUMMARY_ICEBERG,
--           PRODUCT_PERFORMANCE_ICEBERG, ML_PREDICTIONS_ICEBERG
--
-- DIRECTION 2: Fabric → Snowflake (this script)
--   Fabric Lakehouse tables → OneLake → Snowflake reads via catalog integration
--   Database: FABRIC_DATA (auto-discovers Fabric tables)
--
-- Both directions use Apache Iceberg as the interoperability format.
-- No data copying — both platforms read the same files in OneLake.

-- =============================================================================
-- 8. GRANT ACCESS TO ALL ROLES
-- =============================================================================

-- Ensure all roles can read Fabric data
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_ADMIN;
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_ANALYST;
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_ML_ENGINEER;
GRANT USAGE ON DATABASE FABRIC_DATA TO ROLE DEMO_AGENT_USER;

-- Grant future schemas and tables
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FABRIC_DATA TO ROLE DEMO_ADMIN;
GRANT SELECT ON FUTURE TABLES IN DATABASE FABRIC_DATA TO ROLE DEMO_ADMIN;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE FABRIC_DATA TO ROLE DEMO_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE FABRIC_DATA TO ROLE DEMO_ANALYST;

SELECT 'Catalog integration and catalog-linked database created. Fabric data is queryable from Snowflake.' AS STATUS;
