-- =============================================================================
-- MEDALLION ARCHITECTURE: BRONZE Layer — ADLS CSV Ingestion
-- =============================================================================
-- Ingests CSV files from Azure Data Lake Storage Gen2 into Bronze tables.
-- Uses BRONZE.ADLS_DATA_STAGE (created in 01_setup/01_account_setup.sql) and
-- Snowpipe auto-ingest via Azure Event Grid notifications.
--
-- Prerequisites:
--   - 01_setup/01_account_setup.sql (ADLS_DATA_STAGE, AZURE_STORAGE_INT,
--     AZURE_SNOWPIPE_INT already created)
--   - Upload CSV files to ADLS under: snowflake-data/csv/
--       regional_sales_targets/ → regional_sales_targets.csv
--       marketing_campaigns/    → marketing_campaigns.csv
--   - Azure Event Grid subscription routing blob events to storage queue
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA BRONZE;

-- Verify stage connectivity (stage created in 01_setup/01_account_setup.sql)
LIST @BRONZE.ADLS_DATA_STAGE/csv/;

-- =============================================================================
-- 1. TARGET BRONZE TABLES
-- =============================================================================

-- Regional sales targets loaded from Fabric / planning tools as CSV exports
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
COMMENT = 'Regional revenue targets — loaded from ADLS CSV exports';

-- Marketing campaign definitions exported from Fabric / CRM
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
COMMENT = 'Marketing campaign metadata — loaded from ADLS CSV exports';

-- =============================================================================
-- 2. COPY INTO — Manual / Scheduled Bulk Load
-- =============================================================================

-- Load regional sales targets
COPY INTO BRONZE.REGIONAL_SALES_TARGETS (
    REGION, CHANNEL, FISCAL_YEAR, FISCAL_QUARTER,
    REVENUE_TARGET, ORDER_TARGET, CUSTOMER_TARGET, SOURCE_FILE
)
FROM (
    SELECT
        $1, $2, $3::NUMBER(4), $4::NUMBER(1),
        $5::NUMBER(14,2), $6::NUMBER, $7::NUMBER,
        METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/regional_sales_targets/
)
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Load marketing campaigns
COPY INTO BRONZE.MARKETING_CAMPAIGNS (
    CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE, START_DATE, END_DATE,
    TARGET_SEGMENT, BUDGET_USD, SPEND_USD, IMPRESSIONS, CLICKS, CONVERSIONS, SOURCE_FILE
)
FROM (
    SELECT
        $1, $2, $3,
        TRY_TO_DATE($4, 'YYYY-MM-DD'), TRY_TO_DATE($5, 'YYYY-MM-DD'),
        $6, $7::NUMBER(12,2), $8::NUMBER(12,2),
        $9::NUMBER, $10::NUMBER, $11::NUMBER,
        METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/marketing_campaigns/
)
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- =============================================================================
-- 3. SNOWPIPE — Auto-Ingest on New File Arrival
-- =============================================================================

-- Pipe for regional sales targets (triggered by Azure Event Grid → Storage Queue)
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

-- Pipe for marketing campaigns
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
           TRY_TO_DATE($4,'YYYY-MM-DD'), TRY_TO_DATE($5,'YYYY-MM-DD'),
           $6, $7::NUMBER(12,2), $8::NUMBER(12,2),
           $9::NUMBER, $10::NUMBER, $11::NUMBER,
           METADATA$FILENAME
    FROM @BRONZE.ADLS_DATA_STAGE/csv/marketing_campaigns/
)
FILE_FORMAT = BRONZE.CSV_FORMAT;

-- Get the notification channel URL to register with Azure Event Grid
SHOW PIPES IN SCHEMA BRONZE;
SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_REGIONAL_TARGETS_PIPE');
SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_MARKETING_PIPE');

-- =============================================================================
-- 4. MONITORING
-- =============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('BRONZE.CSV_REGIONAL_TARGETS_PIPE');
SELECT SYSTEM$PIPE_STATUS('BRONZE.CSV_MARKETING_PIPE');

-- Check ingestion history (last 24 hours)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'BRONZE.REGIONAL_SALES_TARGETS',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- Row counts
SELECT 'REGIONAL_SALES_TARGETS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.REGIONAL_SALES_TARGETS
UNION ALL
SELECT 'MARKETING_CAMPAIGNS', COUNT(*) FROM BRONZE.MARKETING_CAMPAIGNS;

-- =============================================================================
-- 5. ADDITIONAL CSV SOURCE — STORE LOCATIONS
-- =============================================================================

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
COMMENT = 'Retail store master data — loaded from ADLS CSV exports';

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
FILE_FORMAT = BRONZE.CSV_FORMAT
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

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

SELECT SYSTEM$PIPE_FORCE_RESUME('BRONZE.CSV_STORE_LOCATIONS_PIPE');

-- =============================================================================
-- 6. SNOWPIPE END-TO-END TEST
-- =============================================================================
-- Steps to verify auto-ingest is working:
--   1. Upload sample_data/store_locations.csv to ADLS:
--      snowflake-data/csv/store_locations/store_locations.csv
--   2. Azure Event Grid fires a blob-created event -> Storage Queue
--   3. AZURE_SNOWPIPE_INT polls the queue and triggers COPY INTO automatically
--   4. Wait ~30-60 seconds, then run the queries below to confirm ingestion
-- =============================================================================

-- All pipe statuses (executionState should be 'RUNNING', pendingFileCount >= 0)
SELECT 'CSV_REGIONAL_TARGETS_PIPE'  AS PIPE_NAME,
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_REGIONAL_TARGETS_PIPE'))  AS STATUS
UNION ALL SELECT 'CSV_MARKETING_PIPE',
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_MARKETING_PIPE'))
UNION ALL SELECT 'CSV_STORE_LOCATIONS_PIPE',
       PARSE_JSON(SYSTEM$PIPE_STATUS('BRONZE.CSV_STORE_LOCATIONS_PIPE'));

-- Recent ingestion history across all tables (last 1 hour)
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

-- Row counts after ingestion
SELECT 'REGIONAL_SALES_TARGETS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.REGIONAL_SALES_TARGETS
UNION ALL SELECT 'MARKETING_CAMPAIGNS',  COUNT(*) FROM BRONZE.MARKETING_CAMPAIGNS
UNION ALL SELECT 'STORE_LOCATIONS',      COUNT(*) FROM BRONZE.STORE_LOCATIONS;
