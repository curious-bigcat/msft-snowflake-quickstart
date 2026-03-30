-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Streams and Tasks Pipeline
-- =============================================================================
-- Demonstrates CDC (Change Data Capture) processing using Streams and Tasks.
-- Builds a Slowly Changing Dimension (SCD Type 2) for orders and a
-- task DAG for multi-step incremental processing.
--
-- Prerequisites: Run phases 01 and 02 first.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. STREAMS — Capture changes on raw tables
-- =============================================================================

-- Stream on ORDERS to capture inserts, updates, deletes
CREATE OR REPLACE STREAM RAW.ORDERS_STREAM
  ON TABLE RAW.ORDERS
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream on orders table';

-- Stream on SUPPORT_TICKETS for incremental ticket processing
CREATE OR REPLACE STREAM RAW.SUPPORT_TICKETS_STREAM
  ON TABLE RAW.SUPPORT_TICKETS
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream on support tickets';

-- =============================================================================
-- 2. SCD TYPE 2 TARGET TABLE
-- =============================================================================

CREATE OR REPLACE TABLE CURATED.ORDERS_SCD2 (
    SURROGATE_KEY     NUMBER AUTOINCREMENT,
    ORDER_ID          NUMBER,
    CUSTOMER_ID       NUMBER,
    ORDER_DATE        TIMESTAMP_NTZ,
    ORDER_STATUS      VARCHAR(20),
    TOTAL_AMOUNT      NUMBER(12,2),
    NET_AMOUNT        NUMBER(12,2),
    REGION            VARCHAR(50),
    CHANNEL           VARCHAR(50),
    SOURCE_SYSTEM     VARCHAR(50),
    -- SCD2 metadata
    EFFECTIVE_FROM    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_TO      TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    IS_CURRENT        BOOLEAN DEFAULT TRUE,
    DML_TYPE          VARCHAR(10) COMMENT 'INSERT, UPDATE, DELETE'
)
COMMENT = 'SCD Type 2 history table for orders';

-- =============================================================================
-- 3. TASK: Process orders stream into SCD2
-- =============================================================================

CREATE OR REPLACE TASK CURATED.TASK_ORDERS_SCD2
  WAREHOUSE = DEMO_WH
  SCHEDULE = '5 MINUTE'
  COMMENT = 'Processes order changes into SCD Type 2 table'
WHEN
  SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM')
AS
  MERGE INTO CURATED.ORDERS_SCD2 tgt
  USING (
      SELECT
          ORDER_ID,
          CUSTOMER_ID,
          ORDER_DATE,
          ORDER_STATUS,
          TOTAL_AMOUNT,
          TOTAL_AMOUNT - DISCOUNT_AMOUNT + SHIPPING_AMOUNT AS NET_AMOUNT,
          REGION,
          CHANNEL,
          SOURCE_SYSTEM,
          METADATA$ACTION AS DML_ACTION,
          METADATA$ISUPDATE AS IS_UPDATE
      FROM RAW.ORDERS_STREAM
  ) src
  ON tgt.ORDER_ID = src.ORDER_ID AND tgt.IS_CURRENT = TRUE

  -- Close out old record on update
  WHEN MATCHED AND src.DML_ACTION = 'INSERT' AND src.IS_UPDATE = TRUE THEN
    UPDATE SET
      tgt.EFFECTIVE_TO = CURRENT_TIMESTAMP(),
      tgt.IS_CURRENT = FALSE

  -- Handle deletes
  WHEN MATCHED AND src.DML_ACTION = 'DELETE' THEN
    UPDATE SET
      tgt.EFFECTIVE_TO = CURRENT_TIMESTAMP(),
      tgt.IS_CURRENT = FALSE,
      tgt.DML_TYPE = 'DELETE'

  -- Insert new or updated records
  WHEN NOT MATCHED AND src.DML_ACTION = 'INSERT' THEN
    INSERT (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT,
            NET_AMOUNT, REGION, CHANNEL, SOURCE_SYSTEM, DML_TYPE)
    VALUES (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.ORDER_STATUS,
            src.TOTAL_AMOUNT, src.NET_AMOUNT, src.REGION, src.CHANNEL,
            src.SOURCE_SYSTEM, IFF(src.IS_UPDATE, 'UPDATE', 'INSERT'));

-- =============================================================================
-- 4. AGGREGATION TABLE & TASK
-- =============================================================================

CREATE OR REPLACE TABLE ANALYTICS.DAILY_ORDER_METRICS (
    METRIC_DATE     DATE,
    REGION          VARCHAR(50),
    CHANNEL         VARCHAR(50),
    ORDER_COUNT     NUMBER,
    TOTAL_REVENUE   NUMBER(14,2),
    AVG_ORDER_VALUE NUMBER(12,2),
    NEW_CUSTOMERS   NUMBER,
    UPDATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Daily order metrics updated by task';

-- Child task: Aggregate daily metrics after SCD2 processing
CREATE OR REPLACE TASK ANALYTICS.TASK_DAILY_METRICS
  WAREHOUSE = DEMO_WH
  COMMENT = 'Aggregates daily order metrics from SCD2 table'
  AFTER CURATED.TASK_ORDERS_SCD2
AS
  MERGE INTO ANALYTICS.DAILY_ORDER_METRICS tgt
  USING (
      SELECT
          ORDER_DATE::DATE AS METRIC_DATE,
          REGION,
          CHANNEL,
          COUNT(*) AS ORDER_COUNT,
          SUM(NET_AMOUNT) AS TOTAL_REVENUE,
          AVG(NET_AMOUNT) AS AVG_ORDER_VALUE,
          COUNT(DISTINCT CUSTOMER_ID) AS NEW_CUSTOMERS
      FROM CURATED.ORDERS_SCD2
      WHERE IS_CURRENT = TRUE
      GROUP BY ORDER_DATE::DATE, REGION, CHANNEL
  ) src
  ON tgt.METRIC_DATE = src.METRIC_DATE
     AND tgt.REGION = src.REGION
     AND tgt.CHANNEL = src.CHANNEL
  WHEN MATCHED THEN
    UPDATE SET
      tgt.ORDER_COUNT = src.ORDER_COUNT,
      tgt.TOTAL_REVENUE = src.TOTAL_REVENUE,
      tgt.AVG_ORDER_VALUE = src.AVG_ORDER_VALUE,
      tgt.NEW_CUSTOMERS = src.NEW_CUSTOMERS,
      tgt.UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (METRIC_DATE, REGION, CHANNEL, ORDER_COUNT, TOTAL_REVENUE,
            AVG_ORDER_VALUE, NEW_CUSTOMERS)
    VALUES (src.METRIC_DATE, src.REGION, src.CHANNEL, src.ORDER_COUNT,
            src.TOTAL_REVENUE, src.AVG_ORDER_VALUE, src.NEW_CUSTOMERS);

-- =============================================================================
-- 5. SUPPORT TICKET PROCESSING TASK
-- =============================================================================

CREATE OR REPLACE TABLE ANALYTICS.SUPPORT_TICKET_METRICS (
    METRIC_DATE        DATE,
    CATEGORY           VARCHAR(50),
    PRIORITY           VARCHAR(20),
    TICKET_COUNT       NUMBER,
    AVG_RESOLUTION_HRS NUMBER(10,2),
    AVG_SATISFACTION   NUMBER(3,2),
    OPEN_TICKETS       NUMBER,
    UPDATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Support ticket metrics for ML and analytics';

CREATE OR REPLACE TASK ANALYTICS.TASK_TICKET_METRICS
  WAREHOUSE = DEMO_WH
  SCHEDULE = '10 MINUTE'
  COMMENT = 'Processes support ticket changes into metrics'
WHEN
  SYSTEM$STREAM_HAS_DATA('RAW.SUPPORT_TICKETS_STREAM')
AS
  INSERT INTO ANALYTICS.SUPPORT_TICKET_METRICS (
      METRIC_DATE, CATEGORY, PRIORITY, TICKET_COUNT,
      AVG_RESOLUTION_HRS, AVG_SATISFACTION, OPEN_TICKETS
  )
  SELECT
      CURRENT_DATE() AS METRIC_DATE,
      CATEGORY,
      PRIORITY,
      COUNT(*) AS TICKET_COUNT,
      ROUND(AVG(RESOLUTION_TIME_HOURS), 2) AS AVG_RESOLUTION_HRS,
      ROUND(AVG(SATISFACTION_SCORE), 2) AS AVG_SATISFACTION,
      SUM(CASE WHEN STATUS IN ('Open', 'In Progress') THEN 1 ELSE 0 END) AS OPEN_TICKETS
  FROM RAW.SUPPORT_TICKETS_STREAM
  WHERE METADATA$ACTION = 'INSERT'
  GROUP BY CATEGORY, PRIORITY;

-- =============================================================================
-- 6. RESUME ALL TASKS (tasks are created in suspended state)
-- =============================================================================

-- Resume in reverse dependency order (children first, then parents)
ALTER TASK ANALYTICS.TASK_DAILY_METRICS RESUME;
ALTER TASK CURATED.TASK_ORDERS_SCD2 RESUME;
ALTER TASK ANALYTICS.TASK_TICKET_METRICS RESUME;

-- =============================================================================
-- 7. TEST: Simulate new data to trigger streams
-- =============================================================================

-- Insert a batch of new orders to trigger the stream
INSERT INTO RAW.ORDERS (
    CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT, DISCOUNT_AMOUNT,
    SHIPPING_AMOUNT, PAYMENT_METHOD, REGION, CHANNEL, SOURCE_SYSTEM
)
SELECT
    UNIFORM(1, 10000, RANDOM()),
    CURRENT_TIMESTAMP(),
    'Pending',
    ROUND(UNIFORM(100, 5000, RANDOM())::NUMERIC(12,2), 2),
    ROUND(UNIFORM(0, 100, RANDOM())::NUMERIC(10,2), 2),
    ROUND(UNIFORM(5, 25, RANDOM())::NUMERIC(10,2), 2),
    'Credit Card',
    ARRAY_CONSTRUCT('North America','Europe','Asia Pacific')[UNIFORM(0,2,RANDOM())]::VARCHAR,
    'Online',
    'NATIVE'
FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- =============================================================================
-- 8. MONITORING
-- =============================================================================

-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('RAW.ORDERS_STREAM') AS ORDERS_STREAM_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('RAW.SUPPORT_TICKETS_STREAM') AS TICKETS_STREAM_HAS_DATA;

-- Check task execution history
SELECT NAME, SCHEMA_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
ORDER BY SCHEDULED_TIME DESC;

-- Show task DAG
SHOW TASKS IN DATABASE MSFT_SNOWFLAKE_DEMO;

-- Verify SCD2 data
SELECT ORDER_ID, ORDER_STATUS, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO, DML_TYPE
FROM CURATED.ORDERS_SCD2
ORDER BY ORDER_ID, EFFECTIVE_FROM
LIMIT 20;

SELECT 'Streams and Tasks pipeline created and resumed.' AS STATUS;
