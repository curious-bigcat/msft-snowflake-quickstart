-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Table Definitions
-- =============================================================================
-- Creates all tables across RAW, STAGING schemas, plus internal stages.
-- Prerequisites: Run 01_setup/01_account_setup.sql first.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- RAW SCHEMA — Landing zone for all source data
-- =============================================================================

USE SCHEMA RAW;

-- -----------------------------------------------------------------------------
-- Customers — Master customer data
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    CUSTOMER_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    FIRST_NAME         VARCHAR(100)   NOT NULL,
    LAST_NAME          VARCHAR(100)   NOT NULL,
    EMAIL              VARCHAR(255),
    PHONE              VARCHAR(20),
    ADDRESS            VARCHAR(500),
    CITY               VARCHAR(100),
    STATE              VARCHAR(50),
    COUNTRY            VARCHAR(50)    DEFAULT 'United States',
    POSTAL_CODE        VARCHAR(20),
    CUSTOMER_SEGMENT   VARCHAR(50)    COMMENT 'Enterprise, SMB, or Consumer',
    REGISTRATION_DATE  DATE,
    IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
    CREATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Customer master data';

-- -----------------------------------------------------------------------------
-- Products — Product catalog
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.PRODUCTS (
    PRODUCT_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    PRODUCT_NAME  VARCHAR(200)   NOT NULL,
    CATEGORY      VARCHAR(100)   COMMENT 'Electronics, Software, Cloud Services, Hardware, Accessories',
    SUB_CATEGORY  VARCHAR(100),
    BRAND         VARCHAR(100),
    UNIT_PRICE    NUMBER(10,2),
    COST_PRICE    NUMBER(10,2),
    DESCRIPTION   VARCHAR(2000),
    SKU           VARCHAR(50)    UNIQUE,
    IS_ACTIVE     BOOLEAN        DEFAULT TRUE,
    CREATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Product catalog';

-- -----------------------------------------------------------------------------
-- Orders — Sales orders (native + ADF + streaming sources)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.ORDERS (
    ORDER_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       NUMBER         NOT NULL,
    ORDER_DATE        TIMESTAMP_NTZ  NOT NULL,
    ORDER_STATUS      VARCHAR(20)    COMMENT 'Pending, Processing, Shipped, Delivered, Cancelled',
    TOTAL_AMOUNT      NUMBER(12,2),
    DISCOUNT_AMOUNT   NUMBER(10,2)   DEFAULT 0,
    SHIPPING_AMOUNT   NUMBER(10,2)   DEFAULT 0,
    PAYMENT_METHOD    VARCHAR(50)    COMMENT 'Credit Card, Debit Card, Wire Transfer, PayPal',
    SHIPPING_ADDRESS  VARCHAR(500),
    REGION            VARCHAR(50)    COMMENT 'North America, Europe, Asia Pacific, Latin America',
    CHANNEL           VARCHAR(50)    COMMENT 'Online, In-Store, Partner, Marketplace',
    SOURCE_SYSTEM     VARCHAR(50)    DEFAULT 'NATIVE' COMMENT 'NATIVE, ADF, or STREAMING',
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Sales orders from multiple sources';

-- -----------------------------------------------------------------------------
-- Order Items — Line items for each order
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.ORDER_ITEMS (
    ORDER_ITEM_ID  NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID       NUMBER         NOT NULL,
    PRODUCT_ID     NUMBER         NOT NULL,
    QUANTITY       NUMBER         NOT NULL,
    UNIT_PRICE     NUMBER(10,2)   NOT NULL,
    LINE_TOTAL     NUMBER(12,2)   NOT NULL,
    DISCOUNT_PCT   NUMBER(5,2)    DEFAULT 0
)
COMMENT = 'Order line items';

-- -----------------------------------------------------------------------------
-- Product Reviews — Unstructured text for Cortex Search
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.PRODUCT_REVIEWS (
    REVIEW_ID      NUMBER AUTOINCREMENT PRIMARY KEY,
    PRODUCT_ID     NUMBER         NOT NULL,
    CUSTOMER_ID    NUMBER         NOT NULL,
    REVIEW_TEXT    VARCHAR(5000)  NOT NULL,
    RATING         NUMBER(2,1)   COMMENT '1.0 to 5.0',
    REVIEW_DATE    DATE,
    SENTIMENT      VARCHAR(20)   COMMENT 'Populated by Cortex AI',
    HELPFUL_VOTES  NUMBER        DEFAULT 0
)
COMMENT = 'Product reviews — used for Cortex Search and sentiment analysis';

-- -----------------------------------------------------------------------------
-- Support Tickets — ML classification target
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.SUPPORT_TICKETS (
    TICKET_ID             NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID           NUMBER         NOT NULL,
    PRODUCT_ID            NUMBER,
    TICKET_SUBJECT        VARCHAR(500)   NOT NULL,
    TICKET_DESCRIPTION    VARCHAR(5000),
    PRIORITY              VARCHAR(20)    COMMENT 'Critical, High, Medium, Low — ML target',
    STATUS                VARCHAR(20)    COMMENT 'Open, In Progress, Resolved, Closed',
    CATEGORY              VARCHAR(50)    COMMENT 'Technical, Billing, Shipping, Product Defect, General Inquiry',
    RESOLUTION_TIME_HOURS NUMBER(10,2),
    SATISFACTION_SCORE    NUMBER(2,1),
    CREATED_AT            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_AT           TIMESTAMP_NTZ
)
COMMENT = 'Support tickets — used for ML priority classification';

-- -----------------------------------------------------------------------------
-- Website Clickstream — Snowpipe auto-ingest from ADLS
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE RAW.WEBSITE_CLICKSTREAM (
    EVENT_ID          VARCHAR(100),
    SESSION_ID        VARCHAR(100),
    CUSTOMER_ID       NUMBER,
    PAGE_URL          VARCHAR(1000),
    EVENT_TYPE        VARCHAR(50)    COMMENT 'page_view, click, add_to_cart, purchase, search',
    REFERRER          VARCHAR(500),
    DEVICE_TYPE       VARCHAR(20)    COMMENT 'Desktop, Mobile, Tablet',
    BROWSER           VARCHAR(50),
    IP_ADDRESS        VARCHAR(50),
    EVENT_TIMESTAMP   TIMESTAMP_NTZ,
    EVENT_PROPERTIES  VARIANT,
    LOADED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Website clickstream — loaded via Snowpipe from ADLS Gen2';

-- -----------------------------------------------------------------------------
-- IoT Sensor Data — Snowpipe Streaming from Event Hubs
-- -----------------------------------------------------------------------------
-- Note: Snowpipe Streaming auto-creates this table, but we define it for reference.
-- The Kafka connector creates columns RECORD_METADATA and RECORD_CONTENT.
CREATE OR REPLACE TABLE RAW.IOT_SENSOR_DATA (
    RECORD_METADATA  VARIANT COMMENT 'Kafka metadata (offset, partition, topic, etc.)',
    RECORD_CONTENT   VARIANT COMMENT 'Raw JSON payload from IoT sensors'
)
COMMENT = 'IoT sensor data — loaded via Snowpipe Streaming from Azure Event Hubs';

-- Convenience view to extract structured fields from the raw JSON
CREATE OR REPLACE VIEW RAW.V_IOT_SENSOR_DATA AS
SELECT
    RECORD_CONTENT:device_id::VARCHAR          AS DEVICE_ID,
    RECORD_CONTENT:sensor_type::VARCHAR        AS SENSOR_TYPE,
    RECORD_CONTENT:temperature::FLOAT          AS TEMPERATURE,
    RECORD_CONTENT:humidity::FLOAT             AS HUMIDITY,
    RECORD_CONTENT:pressure::FLOAT             AS PRESSURE,
    RECORD_CONTENT:battery_level::FLOAT        AS BATTERY_LEVEL,
    RECORD_CONTENT:location::VARCHAR           AS LOCATION,
    RECORD_CONTENT:timestamp::TIMESTAMP_NTZ    AS EVENT_TIMESTAMP,
    RECORD_CONTENT:alert_status::VARCHAR       AS ALERT_STATUS
FROM RAW.IOT_SENSOR_DATA;

-- =============================================================================
-- STAGING SCHEMA — Landing zone for external ingestion
-- =============================================================================

USE SCHEMA STAGING;

-- -----------------------------------------------------------------------------
-- ADF Inventory — Landed by Azure Data Factory
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE STAGING.ADF_INVENTORY (
    INVENTORY_ID         NUMBER,
    PRODUCT_ID           NUMBER,
    WAREHOUSE_LOCATION   VARCHAR(100),
    QUANTITY_ON_HAND     NUMBER,
    QUANTITY_RESERVED    NUMBER,
    REORDER_POINT        NUMBER,
    LAST_RESTOCK_DATE    DATE,
    SUPPLIER_ID          NUMBER,
    INGESTED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    ADF_PIPELINE_RUN_ID  VARCHAR(100)
)
COMMENT = 'Inventory data loaded by Azure Data Factory';

-- -----------------------------------------------------------------------------
-- ADF Supplier Data — Landed by Azure Data Factory
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE STAGING.ADF_SUPPLIER_DATA (
    SUPPLIER_ID          NUMBER,
    SUPPLIER_NAME        VARCHAR(200),
    CONTACT_EMAIL        VARCHAR(255),
    COUNTRY              VARCHAR(50),
    LEAD_TIME_DAYS       NUMBER,
    RELIABILITY_SCORE    NUMBER(3,2),
    CONTRACT_START_DATE  DATE,
    CONTRACT_END_DATE    DATE,
    INGESTED_AT          TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Supplier data loaded by Azure Data Factory';

-- =============================================================================
-- INTERNAL STAGES — For ML models and semantic model YAML
-- =============================================================================

USE SCHEMA ML;
CREATE OR REPLACE STAGE ML.ML_MODELS
  COMMENT = 'Stage for storing trained ML model artifacts (joblib, pickle)';

USE SCHEMA AGENTS;
CREATE OR REPLACE STAGE AGENTS.SEMANTIC_MODELS
  COMMENT = 'Stage for semantic model YAML files used by Cortex Analyst';

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE, ROW_COUNT
FROM MSFT_SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'MSFT_SNOWFLAKE_DEMO'
  AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

SELECT STAGE_SCHEMA, STAGE_NAME
FROM MSFT_SNOWFLAKE_DEMO.INFORMATION_SCHEMA.STAGES
WHERE STAGE_CATALOG = 'MSFT_SNOWFLAKE_DEMO'
ORDER BY STAGE_SCHEMA, STAGE_NAME;

SELECT 'Table creation complete.' AS STATUS;
