-- =============================================================================
-- MEDALLION ARCHITECTURE: SILVER Layer
-- =============================================================================
-- Part 1: Dynamic Tables — declarative Bronze → Silver pipeline
-- Part 2: Streams and Tasks — CDC processing, SCD Type 2, and Task DAG
-- Part 3: Gold-schema Dynamic Tables — aggregations built on Silver data
--
-- Pipeline DAG:
--   BRONZE.ORDERS + BRONZE.CUSTOMERS
--     → SILVER.DT_ORDERS_CLEANED  (validation, quality flags)
--       → SILVER.DT_ORDERS_ENRICHED (customer join, value tiers)
--         → GOLD.DT_SALES_SUMMARY, GOLD.DT_CUSTOMER_360, GOLD.DT_PRODUCT_PERFORMANCE
--
--   BRONZE.ORDERS_STREAM → SILVER.TASK_ORDERS_SCD2 → SILVER.ORDERS_SCD2
--                        → SILVER.TASK_DAILY_METRICS → GOLD.DAILY_ORDER_METRICS
--   BRONZE.SUPPORT_TICKETS_STREAM → SILVER.TASK_TICKET_METRICS → GOLD.SUPPORT_TICKET_METRICS
--
-- Prerequisites: Run phases 01 and 02 (setup + bronze).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- PART 1: DYNAMIC TABLES
-- =============================================================================

-- =============================================================================
-- 1a. CLEANED ORDERS — Validate and standardise raw orders
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_ORDERS_CLEANED
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = DEMO_WH
  COMMENT = 'Bronze: Cleaned and validated orders'
AS
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    UPPER(TRIM(ORDER_STATUS))    AS ORDER_STATUS,
    TOTAL_AMOUNT,
    DISCOUNT_AMOUNT,
    SHIPPING_AMOUNT,
    TOTAL_AMOUNT - DISCOUNT_AMOUNT + SHIPPING_AMOUNT AS NET_AMOUNT,
    UPPER(TRIM(PAYMENT_METHOD))  AS PAYMENT_METHOD,
    SHIPPING_ADDRESS,
    UPPER(TRIM(REGION))          AS REGION,
    UPPER(TRIM(CHANNEL))         AS CHANNEL,
    SOURCE_SYSTEM,
    CREATED_AT,
    UPDATED_AT,
    IFF(TOTAL_AMOUNT > 0, TRUE, FALSE)                              AS IS_VALID_AMOUNT,
    IFF(CUSTOMER_ID IS NOT NULL AND CUSTOMER_ID > 0, TRUE, FALSE)   AS HAS_VALID_CUSTOMER
FROM BRONZE.ORDERS
WHERE ORDER_DATE IS NOT NULL
  AND TOTAL_AMOUNT IS NOT NULL;

-- =============================================================================
-- 1b. ENRICHED ORDERS — Join with customer for analytics
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_ORDERS_ENRICHED
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = DEMO_WH
  COMMENT = 'Silver: Orders enriched with customer and product info'
AS
SELECT
    o.ORDER_ID,
    o.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.EMAIL                                          AS CUSTOMER_EMAIL,
    c.CUSTOMER_SEGMENT,
    c.CITY                                           AS CUSTOMER_CITY,
    c.STATE                                          AS CUSTOMER_STATE,
    o.ORDER_DATE,
    DATE_TRUNC('month', o.ORDER_DATE)                AS ORDER_MONTH,
    DATE_TRUNC('quarter', o.ORDER_DATE)              AS ORDER_QUARTER,
    YEAR(o.ORDER_DATE)                               AS ORDER_YEAR,
    o.ORDER_STATUS,
    o.TOTAL_AMOUNT,
    o.DISCOUNT_AMOUNT,
    o.SHIPPING_AMOUNT,
    o.NET_AMOUNT,
    o.PAYMENT_METHOD,
    o.REGION,
    o.CHANNEL,
    o.SOURCE_SYSTEM,
    DATEDIFF('day', c.REGISTRATION_DATE, o.ORDER_DATE::DATE) AS CUSTOMER_TENURE_DAYS,
    CASE
        WHEN o.NET_AMOUNT >= 5000 THEN 'High Value'
        WHEN o.NET_AMOUNT >= 1000 THEN 'Medium Value'
        ELSE 'Standard'
    END AS ORDER_VALUE_TIER
FROM SILVER.DT_ORDERS_CLEANED o
LEFT JOIN BRONZE.CUSTOMERS c ON o.CUSTOMER_ID = c.CUSTOMER_ID
WHERE o.IS_VALID_AMOUNT = TRUE
  AND o.HAS_VALID_CUSTOMER = TRUE;

-- Verify dynamic tables
SHOW DYNAMIC TABLES IN SCHEMA MSFT_SNOWFLAKE_DEMO.SILVER;

SELECT NAME, SCHEMA_NAME, STATE, STATE_MESSAGE, REFRESH_ACTION, REFRESH_TRIGGER
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME IN ('DT_ORDERS_CLEANED','DT_ORDERS_ENRICHED')
ORDER BY DATA_TIMESTAMP DESC
LIMIT 20;

SELECT * FROM SILVER.DT_ORDERS_CLEANED  LIMIT 5;
SELECT * FROM SILVER.DT_ORDERS_ENRICHED LIMIT 5;

SELECT 'Silver dynamic tables created.' AS STATUS;

-- =============================================================================
-- PART 2: STREAMS AND TASK DAG
-- =============================================================================

-- =============================================================================
-- 2a. STREAMS — CDC on Bronze tables
-- =============================================================================

CREATE OR REPLACE STREAM BRONZE.ORDERS_STREAM
  ON TABLE BRONZE.ORDERS
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream on orders table';

CREATE OR REPLACE STREAM BRONZE.SUPPORT_TICKETS_STREAM
  ON TABLE BRONZE.SUPPORT_TICKETS
  APPEND_ONLY = FALSE
  COMMENT = 'CDC stream on support tickets';

-- =============================================================================
-- 2b. TARGET TABLES
-- =============================================================================

CREATE OR REPLACE TABLE SILVER.ORDERS_SCD2 (
    SURROGATE_KEY  NUMBER AUTOINCREMENT,
    ORDER_ID       NUMBER,
    CUSTOMER_ID    NUMBER,
    ORDER_DATE     TIMESTAMP_NTZ,
    ORDER_STATUS   VARCHAR(20),
    TOTAL_AMOUNT   NUMBER(12,2),
    NET_AMOUNT     NUMBER(12,2),
    REGION         VARCHAR(50),
    CHANNEL        VARCHAR(50),
    SOURCE_SYSTEM  VARCHAR(50),
    EFFECTIVE_FROM TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EFFECTIVE_TO   TIMESTAMP_NTZ DEFAULT '9999-12-31'::TIMESTAMP_NTZ,
    IS_CURRENT     BOOLEAN DEFAULT TRUE,
    DML_TYPE       VARCHAR(10) COMMENT 'INSERT, UPDATE, DELETE'
)
COMMENT = 'SCD Type 2 history table for orders';

CREATE OR REPLACE TABLE GOLD.DAILY_ORDER_METRICS (
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

CREATE OR REPLACE TABLE GOLD.SUPPORT_TICKET_METRICS (
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

-- =============================================================================
-- 2c. TASK DAG
-- All tasks in SILVER schema (Snowflake DAG constraint: tasks must share schema)
-- =============================================================================

-- Root task: Process orders stream → SCD2
CREATE OR REPLACE TASK SILVER.TASK_ORDERS_SCD2
  WAREHOUSE = DEMO_WH
  SCHEDULE = '5 MINUTE'
  COMMENT = 'Processes order changes into SCD Type 2 table'
WHEN
  SYSTEM$STREAM_HAS_DATA('BRONZE.ORDERS_STREAM')
AS
  MERGE INTO SILVER.ORDERS_SCD2 tgt
  USING (
      SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS,
             TOTAL_AMOUNT,
             TOTAL_AMOUNT - DISCOUNT_AMOUNT + SHIPPING_AMOUNT AS NET_AMOUNT,
             REGION, CHANNEL, SOURCE_SYSTEM,
             METADATA$ACTION AS DML_ACTION,
             METADATA$ISUPDATE AS IS_UPDATE
      FROM BRONZE.ORDERS_STREAM
  ) src
  ON tgt.ORDER_ID = src.ORDER_ID AND tgt.IS_CURRENT = TRUE
  WHEN MATCHED AND src.DML_ACTION = 'INSERT' AND src.IS_UPDATE = TRUE THEN
    UPDATE SET tgt.EFFECTIVE_TO = CURRENT_TIMESTAMP(), tgt.IS_CURRENT = FALSE
  WHEN MATCHED AND src.DML_ACTION = 'DELETE' THEN
    UPDATE SET tgt.EFFECTIVE_TO = CURRENT_TIMESTAMP(), tgt.IS_CURRENT = FALSE, tgt.DML_TYPE = 'DELETE'
  WHEN NOT MATCHED AND src.DML_ACTION = 'INSERT' THEN
    INSERT (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT,
            NET_AMOUNT, REGION, CHANNEL, SOURCE_SYSTEM, DML_TYPE)
    VALUES (src.ORDER_ID, src.CUSTOMER_ID, src.ORDER_DATE, src.ORDER_STATUS,
            src.TOTAL_AMOUNT, src.NET_AMOUNT, src.REGION, src.CHANNEL,
            src.SOURCE_SYSTEM, IFF(src.IS_UPDATE, 'UPDATE', 'INSERT'));

-- Child task: Aggregate daily metrics after SCD2 (same SILVER schema)
CREATE OR REPLACE TASK SILVER.TASK_DAILY_METRICS
  WAREHOUSE = DEMO_WH
  COMMENT = 'Aggregates daily order metrics from SCD2 table'
  AFTER SILVER.TASK_ORDERS_SCD2
AS
  MERGE INTO GOLD.DAILY_ORDER_METRICS tgt
  USING (
      SELECT ORDER_DATE::DATE AS METRIC_DATE, REGION, CHANNEL,
             COUNT(*) AS ORDER_COUNT, SUM(NET_AMOUNT) AS TOTAL_REVENUE,
             AVG(NET_AMOUNT) AS AVG_ORDER_VALUE,
             COUNT(DISTINCT CUSTOMER_ID) AS NEW_CUSTOMERS
      FROM SILVER.ORDERS_SCD2
      WHERE IS_CURRENT = TRUE
      GROUP BY ORDER_DATE::DATE, REGION, CHANNEL
  ) src
  ON tgt.METRIC_DATE = src.METRIC_DATE AND tgt.REGION = src.REGION AND tgt.CHANNEL = src.CHANNEL
  WHEN MATCHED THEN
    UPDATE SET tgt.ORDER_COUNT = src.ORDER_COUNT, tgt.TOTAL_REVENUE = src.TOTAL_REVENUE,
               tgt.AVG_ORDER_VALUE = src.AVG_ORDER_VALUE, tgt.NEW_CUSTOMERS = src.NEW_CUSTOMERS,
               tgt.UPDATED_AT = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN
    INSERT (METRIC_DATE, REGION, CHANNEL, ORDER_COUNT, TOTAL_REVENUE, AVG_ORDER_VALUE, NEW_CUSTOMERS)
    VALUES (src.METRIC_DATE, src.REGION, src.CHANNEL, src.ORDER_COUNT,
            src.TOTAL_REVENUE, src.AVG_ORDER_VALUE, src.NEW_CUSTOMERS);

-- Independent task: Support ticket metrics
CREATE OR REPLACE TASK SILVER.TASK_TICKET_METRICS
  WAREHOUSE = DEMO_WH
  SCHEDULE = '10 MINUTE'
  COMMENT = 'Processes support ticket changes into metrics'
WHEN
  SYSTEM$STREAM_HAS_DATA('BRONZE.SUPPORT_TICKETS_STREAM')
AS
  INSERT INTO GOLD.SUPPORT_TICKET_METRICS (
      METRIC_DATE, CATEGORY, PRIORITY, TICKET_COUNT,
      AVG_RESOLUTION_HRS, AVG_SATISFACTION, OPEN_TICKETS
  )
  SELECT
      CURRENT_DATE() AS METRIC_DATE,
      CATEGORY,
      PRIORITY,
      COUNT(*) AS TICKET_COUNT,
      ROUND(AVG(RESOLUTION_TIME_HOURS), 2) AS AVG_RESOLUTION_HRS,
      ROUND(AVG(SATISFACTION_SCORE), 2)    AS AVG_SATISFACTION,
      SUM(CASE WHEN STATUS IN ('Open', 'In Progress') THEN 1 ELSE 0 END) AS OPEN_TICKETS
  FROM BRONZE.SUPPORT_TICKETS_STREAM
  WHERE METADATA$ACTION = 'INSERT'
  GROUP BY CATEGORY, PRIORITY;

-- Resume in reverse dependency order (children first, then roots)
ALTER TASK SILVER.TASK_DAILY_METRICS  RESUME;
ALTER TASK SILVER.TASK_ORDERS_SCD2    RESUME;
ALTER TASK SILVER.TASK_TICKET_METRICS RESUME;

-- =============================================================================
-- 2d. TEST — Simulate data to trigger streams
-- =============================================================================
-- INSERT into BRONZE.ORDERS          → populates ORDERS_STREAM      → triggers TASK_ORDERS_SCD2
-- INSERT into BRONZE.SUPPORT_TICKETS → populates SUPPORT_TICKETS_STREAM → triggers TASK_TICKET_METRICS

-- Triggers ORDERS_STREAM → TASK_ORDERS_SCD2
INSERT INTO BRONZE.ORDERS (
    CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT, DISCOUNT_AMOUNT,
    SHIPPING_AMOUNT, PAYMENT_METHOD, REGION, CHANNEL, SOURCE_SYSTEM
)
SELECT
    UNIFORM(1, 10000, RANDOM()),
    CURRENT_TIMESTAMP(), 'Pending',
    ROUND(UNIFORM(100, 5000, RANDOM())::NUMERIC(12,2), 2),
    ROUND(UNIFORM(0, 100, RANDOM())::NUMERIC(10,2), 2),
    ROUND(UNIFORM(5, 25, RANDOM())::NUMERIC(10,2), 2),
    'Credit Card',
    ARRAY_CONSTRUCT('North America','Europe','Asia Pacific')[UNIFORM(0,2,RANDOM())]::VARCHAR,
    'Online', 'NATIVE'
FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- Triggers SUPPORT_TICKETS_STREAM → TASK_TICKET_METRICS
INSERT INTO BRONZE.SUPPORT_TICKETS (
    CUSTOMER_ID, TICKET_SUBJECT, CATEGORY, PRIORITY, STATUS,
    RESOLUTION_TIME_HOURS, SATISFACTION_SCORE
)
SELECT
    UNIFORM(1, 10000, RANDOM()),
    ARRAY_CONSTRUCT('Order issue','Billing question','Technical problem','Shipping delay','Account access')[UNIFORM(0,4,RANDOM())]::VARCHAR,
    ARRAY_CONSTRUCT('Billing','Technical','Shipping','Returns','Account')[UNIFORM(0,4,RANDOM())]::VARCHAR,
    ARRAY_CONSTRUCT('Low','Medium','High','Critical')[UNIFORM(0,3,RANDOM())]::VARCHAR,
    ARRAY_CONSTRUCT('Open','In Progress','Resolved','Closed')[UNIFORM(0,3,RANDOM())]::VARCHAR,
    ROUND(UNIFORM(1, 72, RANDOM())::NUMERIC(10,2), 2),
    ROUND(UNIFORM(1, 5, RANDOM())::NUMERIC(2,1), 1)
FROM TABLE(GENERATOR(ROWCOUNT => 50));

-- =============================================================================
-- 2e. MONITORING
-- =============================================================================

SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.ORDERS_STREAM')          AS ORDERS_STREAM_HAS_DATA;
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.SUPPORT_TICKETS_STREAM') AS TICKETS_STREAM_HAS_DATA;

SELECT NAME, SCHEMA_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME, ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 20
))
ORDER BY SCHEDULED_TIME DESC;

SHOW TASKS IN DATABASE MSFT_SNOWFLAKE_DEMO;

SELECT ORDER_ID, ORDER_STATUS, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO, DML_TYPE
FROM SILVER.ORDERS_SCD2
ORDER BY ORDER_ID, EFFECTIVE_FROM
LIMIT 20;

SELECT 'Silver processing pipeline created and running.' AS STATUS;

-- =============================================================================
-- PART 3: GOLD-SCHEMA DYNAMIC TABLES
-- =============================================================================
-- Auto-refreshing aggregations built from curated Silver and Bronze data.
-- These serve as the processed, analytics-ready layer consumed by Gold views,
-- ML models, and Cortex AI features.

-- =============================================================================
-- 3a. SALES SUMMARY
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_SALES_SUMMARY
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Sales aggregations by region, channel, and time period'
AS
SELECT
    ORDER_MONTH,
    ORDER_YEAR,
    REGION,
    CHANNEL,
    CUSTOMER_SEGMENT,
    ORDER_VALUE_TIER,
    COUNT(*) AS ORDER_COUNT,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    SUM(TOTAL_AMOUNT) AS GROSS_REVENUE,
    SUM(DISCOUNT_AMOUNT) AS TOTAL_DISCOUNTS,
    SUM(NET_AMOUNT) AS NET_REVENUE,
    AVG(NET_AMOUNT) AS AVG_ORDER_VALUE,
    MEDIAN(NET_AMOUNT) AS MEDIAN_ORDER_VALUE,
    MAX(NET_AMOUNT) AS MAX_ORDER_VALUE,
    SUM(CASE WHEN ORDER_STATUS = 'CANCELLED' THEN 1 ELSE 0 END) AS CANCELLED_ORDERS,
    ROUND(SUM(CASE WHEN ORDER_STATUS = 'CANCELLED' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2)
        AS CANCELLATION_RATE_PCT
FROM SILVER.DT_ORDERS_ENRICHED
GROUP BY ORDER_MONTH, ORDER_YEAR, REGION, CHANNEL, CUSTOMER_SEGMENT, ORDER_VALUE_TIER;

-- =============================================================================
-- 3b. CUSTOMER 360
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_CUSTOMER_360
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Customer 360 aggregation for analytics and ML'
AS
SELECT
    o.CUSTOMER_ID,
    o.FIRST_NAME,
    o.LAST_NAME,
    o.CUSTOMER_EMAIL,
    o.CUSTOMER_SEGMENT,
    o.CUSTOMER_CITY,
    o.CUSTOMER_STATE,
    COUNT(*) AS TOTAL_ORDERS,
    SUM(o.NET_AMOUNT) AS LIFETIME_VALUE,
    AVG(o.NET_AMOUNT) AS AVG_ORDER_VALUE,
    MIN(o.ORDER_DATE) AS FIRST_ORDER_DATE,
    MAX(o.ORDER_DATE) AS LAST_ORDER_DATE,
    DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_TIMESTAMP()) AS DAYS_SINCE_LAST_ORDER,
    COUNT(DISTINCT o.REGION) AS REGIONS_ORDERED_FROM,
    COUNT(DISTINCT o.CHANNEL) AS CHANNELS_USED,
    MODE(o.PAYMENT_METHOD) AS PREFERRED_PAYMENT,
    MODE(o.CHANNEL) AS PREFERRED_CHANNEL,
    MODE(o.REGION) AS PRIMARY_REGION,
    SUM(CASE WHEN o.ORDER_STATUS = 'CANCELLED' THEN 1 ELSE 0 END) AS CANCELLED_ORDERS,
    CASE
        WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_TIMESTAMP()) <= 30 THEN 'Active'
        WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_TIMESTAMP()) <= 90 THEN 'Warm'
        WHEN DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_TIMESTAMP()) <= 180 THEN 'Cooling'
        ELSE 'At Risk'
    END AS ENGAGEMENT_STATUS,
    CASE
        WHEN SUM(o.NET_AMOUNT) >= 50000 THEN 'Platinum'
        WHEN SUM(o.NET_AMOUNT) >= 20000 THEN 'Gold'
        WHEN SUM(o.NET_AMOUNT) >= 5000 THEN 'Silver'
        ELSE 'Bronze'
    END AS CUSTOMER_TIER
FROM SILVER.DT_ORDERS_ENRICHED o
GROUP BY o.CUSTOMER_ID, o.FIRST_NAME, o.LAST_NAME, o.CUSTOMER_EMAIL,
         o.CUSTOMER_SEGMENT, o.CUSTOMER_CITY, o.CUSTOMER_STATE;

-- =============================================================================
-- 3c. PRODUCT PERFORMANCE
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_PRODUCT_PERFORMANCE
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Product performance metrics'
AS
SELECT
    oi.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUB_CATEGORY,
    p.BRAND,
    p.UNIT_PRICE AS LIST_PRICE,
    COUNT(DISTINCT oi.ORDER_ID) AS ORDERS_CONTAINING_PRODUCT,
    SUM(oi.QUANTITY) AS TOTAL_UNITS_SOLD,
    SUM(oi.LINE_TOTAL) AS TOTAL_REVENUE,
    AVG(oi.UNIT_PRICE) AS AVG_SELLING_PRICE,
    AVG(oi.DISCOUNT_PCT) AS AVG_DISCOUNT_PCT,
    r.AVG_RATING,
    r.REVIEW_COUNT,
    r.POSITIVE_REVIEW_PCT
FROM BRONZE.ORDER_ITEMS oi
LEFT JOIN BRONZE.PRODUCTS p ON oi.PRODUCT_ID = p.PRODUCT_ID
LEFT JOIN (
    SELECT
        PRODUCT_ID,
        ROUND(AVG(RATING), 2) AS AVG_RATING,
        COUNT(*) AS REVIEW_COUNT,
        ROUND(SUM(CASE WHEN RATING >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS POSITIVE_REVIEW_PCT
    FROM BRONZE.PRODUCT_REVIEWS
    GROUP BY PRODUCT_ID
) r ON oi.PRODUCT_ID = r.PRODUCT_ID
GROUP BY oi.PRODUCT_ID, p.PRODUCT_NAME, p.CATEGORY, p.SUB_CATEGORY, p.BRAND,
         p.UNIT_PRICE, r.AVG_RATING, r.REVIEW_COUNT, r.POSITIVE_REVIEW_PCT;

-- =============================================================================
-- 3d. VERIFICATION
-- =============================================================================

SHOW DYNAMIC TABLES IN SCHEMA GOLD;

SELECT * FROM GOLD.DT_SALES_SUMMARY              LIMIT 5;
SELECT * FROM GOLD.DT_CUSTOMER_360  ORDER BY LIFETIME_VALUE DESC LIMIT 5;
SELECT * FROM GOLD.DT_PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC LIMIT 5;

SELECT 'Silver layer complete — dynamic tables, streams, tasks, and Gold DTs created.' AS STATUS;
