-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Snowpipe Auto-Ingest from ADLS Gen2
-- =============================================================================
-- Sets up Snowpipe with AUTO_INGEST to continuously load clickstream data
-- from Azure Data Lake Storage Gen2 into Snowflake.
--
-- Flow: ADLS Gen2 → Event Grid → Storage Queue → Snowpipe → RAW.WEBSITE_CLICKSTREAM
--
-- Prerequisites:
--   1. Run 01_setup/01_account_setup.sql (storage + notification integrations)
--   2. Run 02_native_data/01_create_tables.sql (target table)
--   3. Configure Azure resources per 01_setup/02_azure_prerequisites.md
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_INGESTION_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA RAW;

-- =============================================================================
-- 1. EXTERNAL STAGE — Points to ADLS Gen2 container
-- =============================================================================
-- Uses the storage integration created in 01_account_setup.sql
-- The URL uses blob.core.windows.net for all Azure storage types including ADLS Gen2

CREATE OR REPLACE STAGE RAW.ADLS_CLICKSTREAM_STAGE
  STORAGE_INTEGRATION = AZURE_STORAGE_INT
  URL = 'azure://<your_storage_account>.blob.core.windows.net/<your_container>/raw-data/clickstream/'
  FILE_FORMAT = (FORMAT_NAME = RAW.JSON_FORMAT)
  COMMENT = 'External stage for clickstream data in ADLS Gen2';

-- Verify stage access (should list files if any exist)
-- LIST @RAW.ADLS_CLICKSTREAM_STAGE;

-- =============================================================================
-- 2. SNOWPIPE — Auto-ingest from ADLS via Event Grid notifications
-- =============================================================================

CREATE OR REPLACE PIPE RAW.CLICKSTREAM_PIPE
  AUTO_INGEST = TRUE
  INTEGRATION = 'AZURE_EVENT_NOTIFICATION_INT'
  COMMENT = 'Auto-ingest clickstream data from ADLS Gen2'
AS
  COPY INTO RAW.WEBSITE_CLICKSTREAM (
      EVENT_ID, SESSION_ID, CUSTOMER_ID, PAGE_URL, EVENT_TYPE,
      REFERRER, DEVICE_TYPE, BROWSER, IP_ADDRESS, EVENT_TIMESTAMP,
      EVENT_PROPERTIES
  )
  FROM (
      SELECT
          $1:event_id::VARCHAR,
          $1:session_id::VARCHAR,
          $1:customer_id::NUMBER,
          $1:page_url::VARCHAR,
          $1:event_type::VARCHAR,
          $1:referrer::VARCHAR,
          $1:device_type::VARCHAR,
          $1:browser::VARCHAR,
          $1:ip_address::VARCHAR,
          $1:event_timestamp::TIMESTAMP_NTZ,
          $1:event_properties::VARIANT
      FROM @RAW.ADLS_CLICKSTREAM_STAGE
  )
  FILE_FORMAT = (FORMAT_NAME = RAW.JSON_FORMAT)
  ON_ERROR = 'CONTINUE';

-- =============================================================================
-- 3. VERIFY PIPE SETUP
-- =============================================================================

-- Check pipe status
SELECT SYSTEM$PIPE_STATUS('RAW.CLICKSTREAM_PIPE');

-- Show pipe details (notification_channel will show the Event Grid queue)
SHOW PIPES LIKE 'CLICKSTREAM_PIPE' IN SCHEMA RAW;

-- =============================================================================
-- 4. SAMPLE JSON DATA FOR TESTING
-- =============================================================================
-- Upload files matching this JSON format to your ADLS container:
-- Path: <container>/raw-data/clickstream/
--
-- Sample JSON file content (save as clickstream_batch_001.json):
/*
[
  {
    "event_id": "evt-001-abc",
    "session_id": "sess-12345",
    "customer_id": 42,
    "page_url": "https://store.example.com/products/laptop",
    "event_type": "page_view",
    "referrer": "https://www.google.com",
    "device_type": "Desktop",
    "browser": "Chrome",
    "ip_address": "192.168.1.100",
    "event_timestamp": "2026-03-15T10:30:00",
    "event_properties": {"category": "Electronics", "search_term": "laptop"}
  },
  {
    "event_id": "evt-002-def",
    "session_id": "sess-12345",
    "customer_id": 42,
    "page_url": "https://store.example.com/products/laptop/add-to-cart",
    "event_type": "add_to_cart",
    "referrer": "https://store.example.com/products/laptop",
    "device_type": "Desktop",
    "browser": "Chrome",
    "ip_address": "192.168.1.100",
    "event_timestamp": "2026-03-15T10:31:15",
    "event_properties": {"product_id": 15, "quantity": 1, "price": 1299.99}
  }
]
*/

-- =============================================================================
-- 5. MANUAL REFRESH (for files that existed before pipe creation)
-- =============================================================================

-- If files were already in the stage before the pipe was created:
-- ALTER PIPE RAW.CLICKSTREAM_PIPE REFRESH;

-- =============================================================================
-- 6. MONITORING QUERIES
-- =============================================================================

-- Check recent pipe load history
SELECT *
FROM TABLE(INFORMATION_SCHEMA.PIPE_USAGE_HISTORY(
    DATE_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP()),
    PIPE_NAME => 'MSFT_SNOWFLAKE_DEMO.RAW.CLICKSTREAM_PIPE'
));

-- Check copy history for errors
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'MSFT_SNOWFLAKE_DEMO.RAW.WEBSITE_CLICKSTREAM',
    START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;

-- Verify loaded data
SELECT COUNT(*) AS TOTAL_EVENTS,
       MIN(EVENT_TIMESTAMP) AS EARLIEST_EVENT,
       MAX(EVENT_TIMESTAMP) AS LATEST_EVENT,
       COUNT(DISTINCT SESSION_ID) AS UNIQUE_SESSIONS
FROM RAW.WEBSITE_CLICKSTREAM;

SELECT 'Snowpipe from ADLS Gen2 configured.' AS STATUS;
