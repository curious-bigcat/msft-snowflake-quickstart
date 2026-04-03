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
-- 0. AZURE INTEGRATIONS & STAGE  (ACCOUNTADMIN required)
-- =============================================================================
-- Complete end-to-end setup for Snowpipe auto-ingest from ADLS Gen2.
-- Steps marked [SQL] are run in Snowflake.
-- Steps marked [AZURE PORTAL] are manual actions in the Azure Portal.
--
-- Reference: https://interworks.com/blog/2023/01/24/automated-ingestion-from-azure-storage-into-snowflake-via-snowpipe/
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 0A. STORAGE INTEGRATION
--     Allows Snowflake to authenticate and read files from ADLS Gen2.
-- =============================================================================

-- STEP 0A-1 [SQL]: Create the storage integration.
--   This registers a Snowflake-managed service principal in your Azure tenant
--   that will be used to read blobs from the storage container.
--   Replace all placeholders with your actual Azure values.
CREATE OR REPLACE STORAGE INTEGRATION AZURE_STORAGE_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://<your_storage_account>.blob.core.windows.net/<your_container>/'
  );

-- STEP 0A-2 [SQL]: Describe the integration.
--   Run this and record two values from the output:
--     AZURE_CONSENT_URL          → open in browser (Step 0A-3)
--     AZURE_MULTI_TENANT_APP_NAME → use for IAM assignment (Step 0A-4)
DESC STORAGE INTEGRATION AZURE_STORAGE_INT;

-- STEP 0A-3 [AZURE PORTAL]: Approve the Snowflake service principal in your tenant.
--   1. Copy the AZURE_CONSENT_URL from the DESC output above.
--   2. Open it in a browser while signed in as an Azure AD Global Admin
--      (or a user with permission to grant admin consent to enterprise apps).
--   3. Review the permissions and click Accept.
--   Result: A new enterprise application for Snowflake appears in
--           Azure AD → Enterprise Applications.

-- STEP 0A-4 [AZURE PORTAL]: Assign the blob-read IAM role to the Snowflake principal.
--   1. In the Azure Portal go to:
--        Storage Accounts → <your_storage_account> → Access Control (IAM)
--   2. Click "Add role assignment".
--   3. Role tab:  select "Storage Blob Data Contributor"
--   4. Members tab:
--        Assign access to: User, group, or service principal
--        Click "+ Select members"
--        Search for the AZURE_MULTI_TENANT_APP_NAME value from Step 0A-2
--        Select it and click Select.
--   5. Click "Review + assign" twice to confirm.

-- STEP 0A-5 [SQL]: Grant usage on the integration to the DEMO_ADMIN role.
GRANT USAGE ON INTEGRATION AZURE_STORAGE_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 0B. AZURE STORAGE QUEUE + EVENT GRID SUBSCRIPTION
--     The queue records each new blob; Event Grid pushes events to it.
--     All steps here are in the Azure Portal — no SQL required.
-- =============================================================================

-- STEP 0B-1 [AZURE PORTAL]: Create a Storage Queue.
--   1. In the Azure Portal open your Storage Account
--      (the same account that holds your blob container).
--   2. In the left menu go to: Data storage → Queues.
--   3. Click "+ Queue".
--   4. Enter a queue name (e.g. snowpipe-queue) and click OK.
--   The queue now appears in the Queues list.
--   Note the queue name — you will need it in Step 0B-2 and Section 0C.

-- STEP 0B-2 [AZURE PORTAL]: Copy the Storage Queue URL.
--   1. Click the queue you just created.
--   2. The full URL is shown at the top of the page, e.g.:
--        https://<storage_account>.queue.core.windows.net/<queue_name>
--   Record this URL exactly.
--   IMPORTANT: Keep the https:// prefix.
--              Do NOT change it to azure:// — Snowflake requires https:// here.

-- STEP 0B-3 [AZURE PORTAL]: Create an Event Grid subscription.
--   This subscription fires an event into the queue whenever a blob is created.
--   1. Still in your Storage Account, go to: Events (left menu).
--   2. Click "+ Event Subscription".
--   3. Fill in the Basic tab:
--        Name:              snowpipe-event  (any descriptive name)
--        Event Schema:      Event Grid Schema
--                           !! Do NOT select "Cloud Event Schema v1.0" !!
--                           Snowflake only supports Event Grid Schema.
--        Filter to Event Types:
--                           Tick "Blob Created" (Microsoft.Storage.BlobCreated)
--                           Untick all other event types.
--        Endpoint Type:     Storage Queues
--        Endpoint:          Click "Select an endpoint"
--                             Storage account: <your_storage_account>
--                             Queue: <queue from Step 0B-1>
--                           Click "Confirm selection".
--   4. Before clicking Create, switch to the "Filters" tab:
--        Enable subject filtering: turn ON
--        Subject Begins With:
--          /blobServices/default/containers/<your_container>/blobs/
--          (Replace <your_container> with your blob container name.)
--        Optionally add "Subject Ends With: .csv" to limit to CSV files only.
--   5. Click Create.
--   Verify the subscription appears in the Events pane of your storage account.
--   Test: Upload a file to the container — after ~30 s check the queue
--         message count; it should increment by 1.

-- =============================================================================
-- 0C. NOTIFICATION INTEGRATION
--     Snowflake polls the Storage Queue via this integration to trigger pipes.
-- =============================================================================

-- STEP 0C-1 [SQL]: Create the notification integration.
--   Replace <your_storage_account>, <your_queue_name>, and <your_azure_tenant_id>
--   with your actual values (queue URL from Step 0B-2).
CREATE OR REPLACE NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT
  ENABLED = TRUE
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
  AZURE_STORAGE_QUEUE_PRIMARY_URI = 'https://<your_storage_account>.queue.core.windows.net/<your_queue_name>'
  AZURE_TENANT_ID = '<your_azure_tenant_id>'
  COMMENT = 'Notification integration for Snowpipe auto-ingest from ADLS Gen2';

-- STEP 0C-2 [SQL]: Describe the notification integration.
--   Run this and record two values from the output:
--     AZURE_CONSENT_URL          → open in browser (Step 0C-3)
--     AZURE_MULTI_TENANT_APP_NAME → use for IAM assignment (Step 0C-4)
--   NOTE: This is a DIFFERENT service principal from the storage integration (0A).
--         Both consent flows and IAM assignments are required independently.
DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT;

-- STEP 0C-3 [AZURE PORTAL]: Approve the notification service principal.
--   1. Copy the AZURE_CONSENT_URL from the DESC output above.
--   2. Open it in a browser (as Azure AD Global Admin or equivalent).
--   3. Click Accept when prompted.

-- STEP 0C-4 [AZURE PORTAL]: Assign the queue IAM role to the Snowflake principal.
--   1. In the Azure Portal navigate to your Storage Queue:
--        Storage Accounts → <your_storage_account>
--        → Data storage → Queues → <your_queue_name>
--        → Access Control (IAM)
--      (You can also grant at the Storage Account level if preferred.)
--   2. Click "Add role assignment".
--   3. Role tab:  select "Storage Queue Data Contributor"
--   4. Members tab:
--        Assign access to: User, group, or service principal
--        Click "+ Select members"
--        Search for the AZURE_MULTI_TENANT_APP_NAME value from Step 0C-2
--        Select it and click Select.
--   5. Click "Review + assign" twice to confirm.
--   Allow 1–2 minutes for the role assignment to propagate before creating pipes.

-- STEP 0C-5 [SQL]: Grant usage on the integration to the DEMO_ADMIN role.
GRANT USAGE ON INTEGRATION AZURE_SNOWPIPE_INT TO ROLE DEMO_ADMIN;

-- =============================================================================
-- 0D. EXTERNAL STAGE
--     Maps a Snowflake stage to the ADLS container; used by Snowpipe COPY INTO.
-- =============================================================================

USE SCHEMA MSFT_SNOWFLAKE_DEMO.BRONZE;

-- STEP 0D-1 [SQL]: Create the CSV file format used by Snowpipe COPY INTO statements.
CREATE OR REPLACE FILE FORMAT MSFT_SNOWFLAKE_DEMO.BRONZE.CSV_FORMAT
  TYPE = 'CSV'
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = TRUE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  COMMENT = 'Standard CSV format for Snowpipe ingestion';

GRANT USAGE ON FILE FORMAT MSFT_SNOWFLAKE_DEMO.BRONZE.CSV_FORMAT TO ROLE DEMO_ADMIN;

-- STEP 0D-2 [SQL]: Create the external stage.
--   URL must use azure:// and point to the root of your container.
CREATE OR REPLACE STAGE BRONZE.ADLS_DATA_STAGE
  URL = 'azure://<your_storage_account>.blob.core.windows.net/snowflake-data/'
  STORAGE_INTEGRATION = AZURE_STORAGE_INT
  FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1
                 FIELD_OPTIONALLY_ENCLOSED_BY = '"' NULL_IF = ('', 'NULL'))
  COMMENT = 'External stage for ADLS Gen2 CSV landing zone';

GRANT USAGE ON STAGE BRONZE.ADLS_DATA_STAGE TO ROLE DEMO_ADMIN;

-- STEP 0D-3 [SQL]: Verify stage connectivity.
--   A successful LIST (even returning 0 files) confirms the storage integration
--   and IAM role assignment are working correctly.
USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA BRONZE;

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
FILE_FORMAT = MSFT_SNOWFLAKE_DEMO.BRONZE.CSV_FORMAT;

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
FILE_FORMAT = MSFT_SNOWFLAKE_DEMO.BRONZE.CSV_FORMAT;

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
FILE_FORMAT = MSFT_SNOWFLAKE_DEMO.BRONZE.CSV_FORMAT;

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
