-- =============================================================================
-- MEDALLION ARCHITECTURE: BRONZE Layer — ADLS CSV Ingestion
-- =============================================================================
-- Ingests CSV files from Azure Data Lake Storage Gen2 into Bronze tables
-- exclusively via Snowpipe auto-ingest (Azure Event Grid → Storage Queue).
--
-- Prerequisites:
--   - 01_setup/01_account_setup.sql run (roles, warehouse, database created)
--   - Upload CSV files to ADLS under: snowflake-data/csv/
--       regional_sales_targets/ → regional_sales_targets.csv
--       marketing_campaigns/    → marketing_campaigns.csv
--       store_locations/        → store_locations.csv
-- =============================================================================

-- =============================================================================
-- 0. AZURE INTEGRATIONS & STAGE  (ACCOUNTADMIN)
-- =============================================================================
-- Skip this section if already run from 01_setup/01_account_setup.sql.
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Storage integration — allows Snowflake to access ADLS Gen2
-- Replace placeholders with your Azure values
CREATE OR REPLACE STORAGE INTEGRATION AZURE_STORAGE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://<your_storage_account>.blob.core.windows.net/<your_container>/'
  );

-- Run DESC to get the consent URL and AZURE_MULTI_TENANT_APP_NAME:
DESC STORAGE INTEGRATION AZURE_STORAGE_INT;
-- Then in Azure Portal: Storage Account → IAM → Add role assignment
--   Role: Storage Blob Data Contributor
--   Member: AZURE_MULTI_TENANT_APP_NAME (from DESC output above)

GRANT USAGE ON INTEGRATION AZURE_STORAGE_INT TO ROLE DEMO_ADMIN;

-- Notification integration — triggers Snowpipe when files land in ADLS
-- Requires an Azure Storage Queue connected to blob-created events via Event Grid.
-- Steps:
--   1. Create a Storage Queue in your Azure Storage Account
--   2. Create an Event Grid subscription:
--        Source:     your Storage Account
--        Event type: Microsoft.Storage.BlobCreated
--        Endpoint:   Azure Storage Queue → select your queue
--   3. Replace placeholders below with your values
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<your_storage_account>.queue.core.windows.net/<your_queue>'
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  COMMENT = 'Notification integration for Snowpipe auto-ingest from ADLS Gen2';

-- Run DESC to get the service principal that needs Queue permissions:
DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT;
-- Then in Azure Portal: Storage Queue → IAM → Add role assignment
--   Role: Storage Queue Data Contributor
--   Member: AZURE_MULTI_TENANT_APP_NAME (from DESC output above)

GRANT USAGE ON INTEGRATION AZURE_SNOWPIPE_INT TO ROLE DEMO_ADMIN;

-- External stage — points to the ADLS container used for CSV file landing
USE SCHEMA MSFT_SNOWFLAKE_DEMO.BRONZE;

CREATE OR REPLACE STAGE BRONZE.ADLS_DATA_STAGE
  URL = 'azure://<your_storage_account>.blob.core.windows.net/snowflake-data/'
  STORAGE_INTEGRATION = AZURE_STORAGE_INT
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL'))
  COMMENT = 'External stage for ADLS Gen2 data landing container';

GRANT USAGE ON STAGE BRONZE.ADLS_DATA_STAGE TO ROLE DEMO_ADMIN;

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA BRONZE;

-- Verify stage connectivity
LIST @BRONZE.ADLS_DATA_STAGE/csv/;

-- =============================================================================
-- 1. TARGET BRONZE TABLES
-- =============================================================================

CREATE OR REPLACE TABLE BRONZE.REGIONAL_SALES_TARGETS (
    TARGET_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    REGION            VARCHAR(50)   NOT NULL,
    CHANNEL           VARCHAR(50)   NOT NULL,
    FISCAL_YEAR       NUMBER(4)     NOT NULL,
    FISCAL_QUARTER    NUMBER(1)     NOT NULL,
    REVENUE_TARGET    NUMBER(14,2)  NOT NULL,
    ORDER_TARGET      NUMBER        NOT NULL,
    CUSTOMER_TARGET   NUMBER        NOT NULL,
    LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE       VARCHAR(500)
)
COMMENT = 'Regional revenue targets — loaded from ADLS CSV via Snowpipe';

CREATE OR REPLACE TABLE BRONZE.MARKETING_CAMPAIGNS (
    CAMPAIGN_ID       VARCHAR(50)   PRIMARY KEY,
    CAMPAIGN_NAME     VARCHAR(200)  NOT NULL,
    CAMPAIGN_TYPE     VARCHAR(50)   COMMENT 'Email, Paid Search, Social, Display, Direct Mail',
    START_DATE        DATE,
    END_DATE          DATE,
    TARGET_SEGMENT    VARCHAR(50),
    BUDGET_USD        NUMBER(12,2),
    SPEND_USD         NUMBER(12,2),
    IMPRESSIONS       NUMBER,
    CLICKS            NUMBER,
    CONVERSIONS       NUMBER,
    LOADED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE       VARCHAR(500)
)
COMMENT = 'Marketing campaign metadata — loaded from ADLS CSV via Snowpipe';

CREATE OR REPLACE TABLE BRONZE.STORE_LOCATIONS (
    STORE_ID        VARCHAR(20)   PRIMARY KEY,
    STORE_NAME      VARCHAR(200)  NOT NULL,
    REGION          VARCHAR(50),
    CITY            VARCHAR(100),
    STATE           VARCHAR(50),
    COUNTRY         VARCHAR(50),
    STORE_TYPE      VARCHAR(50)   COMMENT 'Flagship, Standard, Outlet, Online',
    OPEN_DATE       DATE,
    IS_ACTIVE       BOOLEAN       DEFAULT TRUE,
    LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SOURCE_FILE     VARCHAR(500)
)
COMMENT = 'Retail store master data — loaded from ADLS CSV via Snowpipe';

-- =============================================================================
-- 2. SNOWPIPE — Auto-Ingest on New File Arrival
-- =============================================================================
-- Pipes are triggered automatically when new files land in ADLS.
-- Azure Event Grid fires a blob-created event → Storage Queue →
-- AZURE_SNOWPIPE_INT polls the queue → COPY INTO executes automatically.
-- =============================================================================

CREATE OR REPLACE PIPE BRONZE.CSV_REGIONAL_TARGETS_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_SNOWPIPE_INT
  COMMENT = 'Auto-ingest regional sales targets from ADLS CSV'
AS
COPY INTO BRONZE.REGIONAL_SALES_TARGETS (
    REGION, CHANNEL, FISCAL_YEAR, FISCAL_QUARTER,
    REVENUE_TARGET, ORDER_TARGET, CUSTOMER_TARGET, SOURCE_FILE
)
FROM (
    SELECT $1, $2, $3::NUMBER(4), $4::NUMBER(1),
           $5::NUMBER(14,2), $6::NUMBER, $7::NUMBER,
           METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/regional_sales_targets/
)
FILE_FORMAT = BRONZE.CSV_FORMAT;

CREATE OR REPLACE PIPE BRONZE.CSV_MARKETING_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_SNOWPIPE_INT
  COMMENT = 'Auto-ingest marketing campaign data from ADLS CSV'
AS
COPY INTO BRONZE.MARKETING_CAMPAIGNS (
    CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE, START_DATE, END_DATE,
    TARGET_SEGMENT, BUDGET_USD, SPEND_USD, IMPRESSIONS, CLICKS, CONVERSIONS, SOURCE_FILE
)
FROM (
    SELECT $1, $2, $3,
           TRY_TO_DATE($4, 'YYYY-MM-DD'), TRY_TO_DATE($5, 'YYYY-MM-DD'),
           $6, $7::NUMBER(12,2), $8::NUMBER(12,2),
           $9::NUMBER, $10::NUMBER, $11::NUMBER,
           METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/marketing_campaigns/
)
FILE_FORMAT = BRONZE.CSV_FORMAT;

CREATE OR REPLACE PIPE BRONZE.CSV_STORE_LOCATIONS_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = AZURE_SNOWPIPE_INT
  COMMENT = 'Auto-ingest store location data from ADLS CSV'
AS
COPY INTO BRONZE.STORE_LOCATIONS (
    STORE_ID, STORE_NAME, REGION, CITY, STATE, COUNTRY,
    STORE_TYPE, OPEN_DATE, IS_ACTIVE, SOURCE_FILE
)
FROM (
    SELECT $1, $2, $3, $4, $5, $6, $7,
           TRY_TO_DATE($8, 'YYYY-MM-DD'), $9::BOOLEAN,
           METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/store_locations/
)
FILE_FORMAT = BRONZE.CSV_FORMAT;

-- Get notification channel URL — register with Azure Event Grid
SHOW PIPES IN SCHEMA BRONZE;

SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_REGIONAL_TARGETS_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_MARKETING_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_STORE_LOCATIONS_PIPE');

-- =============================================================================
-- 3. MONITORING — Verify Snowpipe Ingestion
-- =============================================================================
-- Steps to test end-to-end:
--   1. Upload CSV files to ADLS (snowflake-data/csv/<table>/<file>.csv)
--   2. Wait ~30-60 seconds for Event Grid → Queue → Snowpipe to trigger
--   3. Run the queries below to confirm ingestion
-- =============================================================================

-- All pipe statuses (executionState should be 'RUNNING')
SELECT 'CSV_REGIONAL_TARGETS_PIPE' AS PIPE_NAME,
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_REGIONAL_TARGETS_PIPE')) AS STATUS
UNION ALL SELECT 'CSV_MARKETING_PIPE',
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_MARKETING_PIPE'))
UNION ALL SELECT 'CSV_STORE_LOCATIONS_PIPE',
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_STORE_LOCATIONS_PIPE'));

-- Ingestion history across all tables (last 1 hour)
SELECT 'REGIONAL_SALES_TARGETS' AS TABLE_NAME, FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BRONZE.REGIONAL_SALES_TARGETS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
UNION ALL
SELECT 'MARKETING_CAMPAIGNS', FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BRONZE.MARKETING_CAMPAIGNS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
UNION ALL
SELECT 'STORE_LOCATIONS', FILE_NAME, STATUS, ROW_COUNT, ERROR_COUNT, LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BRONZE.STORE_LOCATIONS',
    START_TIME => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
ORDER BY LAST_LOAD_TIME DESC;

-- Row counts
SELECT 'REGIONAL_SALES_TARGETS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.REGIONAL_SALES_TARGETS
UNION ALL SELECT 'MARKETING_CAMPAIGNS',  COUNT(*) FROM BRONZE.MARKETING_CAMPAIGNS
UNION ALL SELECT 'STORE_LOCATIONS',      COUNT(*) FROM BRONZE.STORE_LOCATIONS;
