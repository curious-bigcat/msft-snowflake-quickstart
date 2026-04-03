-- =============================================================================
-- MEDALLION ARCHITECTURE: BRONZE Layer — Fabric OneLake Integration
-- =============================================================================
-- Three-section file covering the full Fabric <-> Snowflake data exchange:
--
--   SECTION A -- DEMO/DEV seed: writes synthetic data to OneLake as Iceberg
--               (run this when no real Fabric pipeline exists yet)
--
--   SECTION B -- PRODUCTION catalog: points Snowflake at real Fabric-managed tables
--               (run this when Fabric Lakehouse tables exist in OneLake)
--
--   NOTE: Sections A and B both CREATE OR REPLACE BRONZE.FABRIC_* tables.
--         They are mutually exclusive -- run ONE of the two, not both.
--
--   SECTION C -- Write-back: exports all BRONZE raw tables to OneLake as Iceberg
--               (run after either A or B; independent of A vs B choice)
--
-- Prerequisites: Run 01_setup/01_account_setup.sql first (volumes + integrations).
-- =============================================================================

-- =============================================================================
-- SECTION A: SEED SYNTHETIC DATA TO ONELAKE  [DEMO / DEV]
-- =============================================================================
-- Writes synthetic data directly to Microsoft Fabric OneLake as Snowflake-managed
-- Iceberg tables. Data physically lives in OneLake (Files/snowflake-iceberg/fabric/).
-- BRONZE.FABRIC_* tables are backed by OneLake storage -- no fallback, no copies.
--
-- Run this section when no real Fabric pipeline exists yet.
-- Skip and run Section B when real Fabric Lakehouse tables exist.
--
-- Prerequisites:
--   - ONELAKE_EXTERNAL_VOL created and validated (01_setup/01_account_setup.sql)
--   - Service principal has Storage Blob Data Contributor on the Fabric workspace
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA BRONZE;

-- -----------------------------------------------------------------------------
-- A1. FABRIC_CLICKSTREAM_EVENTS -- ~100K web and mobile events
-- Simulates Fabric Real-Time Intelligence (Eventhouse -> KQL -> OneLake)
-- OneLake path: Files/snowflake-iceberg/fabric/clickstream_events/
-- -----------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS (
    EVENT_ID             STRING   NOT NULL,
    SESSION_ID           STRING   NOT NULL,
    USER_ID              BIGINT                  COMMENT 'NULL for anonymous sessions',
    PAGE_URL             STRING,
    EVENT_TYPE           STRING             COMMENT 'page_view, click, add_to_cart, purchase, search, wishlist',
    REFERRER_SOURCE      STRING            COMMENT 'organic, paid_search, email, social, direct, affiliate',
    DEVICE_TYPE          STRING             COMMENT 'mobile, desktop, tablet',
    BROWSER              STRING,
    COUNTRY              STRING,
    PRODUCT_ID           BIGINT                  COMMENT 'Populated for product-related events',
    SESSION_DURATION_SEC BIGINT                  COMMENT 'Set on session_end events only',
    EVENT_TIMESTAMP      TIMESTAMP_NTZ NOT NULL
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'fabric/clickstream_events/'
  COMMENT = 'Clickstream events seeded to Fabric OneLake -- Snowflake-managed Iceberg';

INSERT INTO BRONZE.FABRIC_CLICKSTREAM_EVENTS (
    EVENT_ID, SESSION_ID, USER_ID, PAGE_URL, EVENT_TYPE,
    REFERRER_SOURCE, DEVICE_TYPE, BROWSER, COUNTRY, PRODUCT_ID,
    SESSION_DURATION_SEC, EVENT_TIMESTAMP
)
SELECT
    UUID_STRING()                                                              AS EVENT_ID,
    'SES-' || LPAD(UNIFORM(1, 25000, RANDOM())::VARCHAR, 8, '0')             AS SESSION_ID,
    CASE WHEN UNIFORM(1,10,RANDOM()) > 2
         THEN UNIFORM(1, 10000, RANDOM()) ELSE NULL END                       AS USER_ID,
    ARRAY_CONSTRUCT(
        '/home','/products','/products/electronics','/products/clothing',
        '/products/home-garden','/cart','/checkout','/account',
        '/search','/deals','/about'
    )[UNIFORM(0, 10, RANDOM())]::VARCHAR                                      AS PAGE_URL,
    ARRAY_CONSTRUCT(
        'page_view','page_view','page_view','click','click',
        'add_to_cart','add_to_cart','purchase','search','wishlist'
    )[UNIFORM(0, 9, RANDOM())]::VARCHAR                                       AS EVENT_TYPE,
    ARRAY_CONSTRUCT(
        'organic','organic','organic','paid_search','paid_search',
        'email','social','social','direct','affiliate'
    )[UNIFORM(0, 9, RANDOM())]::VARCHAR                                       AS REFERRER_SOURCE,
    ARRAY_CONSTRUCT('mobile','mobile','desktop','desktop','tablet')
        [UNIFORM(0, 4, RANDOM())]::VARCHAR                                    AS DEVICE_TYPE,
    ARRAY_CONSTRUCT('Chrome','Chrome','Safari','Safari','Firefox','Edge','Samsung Internet')
        [UNIFORM(0, 6, RANDOM())]::VARCHAR                                    AS BROWSER,
    ARRAY_CONSTRUCT(
        'United States','United States','United States',
        'United Kingdom','Germany','Australia','Canada',
        'France','Singapore','Brazil','Japan','India'
    )[UNIFORM(0, 11, RANDOM())]::VARCHAR                                      AS COUNTRY,
    CASE WHEN UNIFORM(1,10,RANDOM()) > 4
         THEN UNIFORM(1, 5000, RANDOM()) ELSE NULL END                        AS PRODUCT_ID,
    CASE WHEN UNIFORM(1,10,RANDOM()) = 1
         THEN UNIFORM(30, 1200, RANDOM()) ELSE NULL END                       AS SESSION_DURATION_SEC,
    DATEADD('second', -UNIFORM(0, 90*24*3600, RANDOM()), CURRENT_TIMESTAMP()) AS EVENT_TIMESTAMP
FROM TABLE(GENERATOR(ROWCOUNT => 100000));

-- -----------------------------------------------------------------------------
-- A2. FABRIC_IOT_EVENTS -- ~50K warehouse and logistics sensor events
-- Simulates Fabric Real-Time Intelligence (IoT Hub -> Eventhouse -> OneLake)
-- OneLake path: Files/snowflake-iceberg/fabric/iot_events/
-- -----------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_IOT_EVENTS (
    EVENT_ID        STRING   NOT NULL,
    DEVICE_ID       STRING   NOT NULL,
    DEVICE_TYPE     STRING             COMMENT 'temperature, humidity, pressure, motion, vibration',
    SENSOR_VALUE    FLOAT         NOT NULL,
    UNIT            STRING,
    WAREHOUSE_ID    STRING,
    LOCATION_ZONE   STRING,
    ALERT_TRIGGERED BOOLEAN       DEFAULT FALSE,
    EVENT_TIMESTAMP TIMESTAMP_NTZ NOT NULL
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'fabric/iot_events/'
  COMMENT = 'Warehouse IoT sensor events seeded to Fabric OneLake -- Snowflake-managed Iceberg';

INSERT INTO BRONZE.FABRIC_IOT_EVENTS (
    EVENT_ID, DEVICE_ID, DEVICE_TYPE, SENSOR_VALUE,
    UNIT, WAREHOUSE_ID, LOCATION_ZONE, ALERT_TRIGGERED, EVENT_TIMESTAMP
)
WITH devices AS (
    SELECT
        'DEV-' || LPAD(SEQ4()::VARCHAR, 5, '0')                              AS DEVICE_ID,
        ARRAY_CONSTRUCT(
            'temperature','temperature','humidity','pressure','motion','vibration'
        )[UNIFORM(0, 5, RANDOM())]::VARCHAR                                   AS DEVICE_TYPE,
        ARRAY_CONSTRUCT(
            'WH-NORTH','WH-SOUTH','WH-EAST','WH-WEST','WH-CENTRAL'
        )[UNIFORM(0, 4, RANDOM())]::VARCHAR                                   AS WAREHOUSE_ID,
        ARRAY_CONSTRUCT(
            'Zone A - Receiving','Zone B - Storage','Zone C - Picking',
            'Zone D - Packing','Zone E - Dispatch','Cold Room','Loading Dock'
        )[UNIFORM(0, 6, RANDOM())]::VARCHAR                                   AS LOCATION_ZONE
    FROM TABLE(GENERATOR(ROWCOUNT => 50000))
)
SELECT
    UUID_STRING()                                                              AS EVENT_ID,
    DEVICE_ID,
    DEVICE_TYPE,
    ROUND(CASE DEVICE_TYPE
        WHEN 'temperature' THEN UNIFORM(15, 35, RANDOM()) + RANDOM()
        WHEN 'humidity'    THEN UNIFORM(30, 90, RANDOM()) + RANDOM()
        WHEN 'pressure'    THEN UNIFORM(980, 1030, RANDOM()) + RANDOM()
        WHEN 'motion'      THEN UNIFORM(0, 1, RANDOM())
        WHEN 'vibration'   THEN UNIFORM(0, 50, RANDOM()) + RANDOM()
        ELSE UNIFORM(0, 100, RANDOM()) + RANDOM()
    END, 2)                                                                    AS SENSOR_VALUE,
    CASE DEVICE_TYPE
        WHEN 'temperature' THEN 'celsius'
        WHEN 'humidity'    THEN 'percent'
        WHEN 'pressure'    THEN 'hPa'
        WHEN 'motion'      THEN 'boolean'
        WHEN 'vibration'   THEN 'mm/s'
        ELSE 'unit'
    END                                                                        AS UNIT,
    WAREHOUSE_ID,
    LOCATION_ZONE,
    CASE
        WHEN DEVICE_TYPE = 'temperature' AND SENSOR_VALUE > 32 THEN TRUE
        WHEN DEVICE_TYPE = 'temperature' AND SENSOR_VALUE < 16 THEN TRUE
        WHEN DEVICE_TYPE = 'humidity'    AND SENSOR_VALUE > 80 THEN TRUE
        WHEN DEVICE_TYPE = 'vibration'   AND SENSOR_VALUE > 40 THEN TRUE
        ELSE FALSE
    END                                                                        AS ALERT_TRIGGERED,
    DATEADD('second', -UNIFORM(0, 90*24*3600, RANDOM()), CURRENT_TIMESTAMP()) AS EVENT_TIMESTAMP
FROM devices;

-- -----------------------------------------------------------------------------
-- A3. FABRIC_REGIONAL_TARGETS -- 52 planning rows (2024-2025, 5 regions)
-- Simulates Fabric (Power BI + Excel Online + Dataflows -> Lakehouse)
-- OneLake path: Files/snowflake-iceberg/fabric/regional_targets/
-- -----------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_REGIONAL_TARGETS (
    REGION          STRING  NOT NULL,
    CHANNEL         STRING  NOT NULL,
    FISCAL_YEAR     NUMBER(4, 0)  NOT NULL,
    FISCAL_QUARTER  NUMBER(1, 0)  NOT NULL,
    REVENUE_TARGET  NUMBER(14,2) NOT NULL,
    ORDER_TARGET    BIGINT       NOT NULL,
    CUSTOMER_TARGET BIGINT       NOT NULL
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'fabric/regional_targets/'
  COMMENT = 'Regional sales targets from Fabric planning tools -- seeded to OneLake as Iceberg';

INSERT INTO BRONZE.FABRIC_REGIONAL_TARGETS
    (REGION, CHANNEL, FISCAL_YEAR, FISCAL_QUARTER, REVENUE_TARGET, ORDER_TARGET, CUSTOMER_TARGET)
VALUES
('North America','Online',2024,1, 8500000.00,34000,27200),
('North America','Online',2024,2, 9200000.00,36800,29440),
('North America','Online',2024,3,10800000.00,43200,34560),
('North America','Online',2024,4,12500000.00,50000,40000),
('EMEA',         'Online',2024,1, 5200000.00,20800,16640),
('EMEA',         'Online',2024,2, 5800000.00,23200,18560),
('EMEA',         'Online',2024,3, 6500000.00,26000,20800),
('EMEA',         'Online',2024,4, 7800000.00,31200,24960),
('APAC',         'Online',2024,1, 3800000.00,15200,12160),
('APAC',         'Online',2024,2, 4200000.00,16800,13440),
('APAC',         'Online',2024,3, 5100000.00,20400,16320),
('APAC',         'Online',2024,4, 6000000.00,24000,19200),
('LATAM',        'Online',2024,1, 1800000.00, 7200, 5760),
('LATAM',        'Online',2024,2, 2100000.00, 8400, 6720),
('LATAM',        'Online',2024,3, 2400000.00, 9600, 7680),
('LATAM',        'Online',2024,4, 2800000.00,11200, 8960),
('ANZ',          'Online',2024,1, 1200000.00, 4800, 3840),
('ANZ',          'Online',2024,2, 1400000.00, 5600, 4480),
('ANZ',          'Online',2024,3, 1600000.00, 6400, 5120),
('ANZ',          'Online',2024,4, 1900000.00, 7600, 6080),
('North America','Online', 2025,1, 9500000.00,38000,30400),
('North America','Online', 2025,2,10200000.00,40800,32640),
('North America','Online', 2025,3,12000000.00,48000,38400),
('North America','Online', 2025,4,14000000.00,56000,44800),
('North America','Retail', 2025,1, 3200000.00,12800,10240),
('North America','Retail', 2025,2, 3500000.00,14000,11200),
('North America','Retail', 2025,3, 4100000.00,16400,13120),
('North America','Retail', 2025,4, 4800000.00,19200,15360),
('EMEA',         'Online', 2025,1, 5800000.00,23200,18560),
('EMEA',         'Online', 2025,2, 6400000.00,25600,20480),
('EMEA',         'Online', 2025,3, 7200000.00,28800,23040),
('EMEA',         'Online', 2025,4, 8600000.00,34400,27520),
('EMEA',         'Direct', 2025,1, 1800000.00, 7200, 5760),
('EMEA',         'Direct', 2025,2, 2000000.00, 8000, 6400),
('EMEA',         'Direct', 2025,3, 2300000.00, 9200, 7360),
('EMEA',         'Direct', 2025,4, 2700000.00,10800, 8640),
('APAC',         'Online', 2025,1, 4200000.00,16800,13440),
('APAC',         'Online', 2025,2, 4700000.00,18800,15040),
('APAC',         'Online', 2025,3, 5600000.00,22400,17920),
('APAC',         'Online', 2025,4, 6600000.00,26400,21120),
('APAC',         'Partner',2025,1, 1500000.00, 6000, 4800),
('APAC',         'Partner',2025,2, 1700000.00, 6800, 5440),
('APAC',         'Partner',2025,3, 2000000.00, 8000, 6400),
('APAC',         'Partner',2025,4, 2400000.00, 9600, 7680),
('LATAM',        'Online', 2025,1, 2000000.00, 8000, 6400),
('LATAM',        'Online', 2025,2, 2300000.00, 9200, 7360),
('LATAM',        'Online', 2025,3, 2650000.00,10600, 8480),
('LATAM',        'Online', 2025,4, 3100000.00,12400, 9920),
('ANZ',          'Online', 2025,1, 1350000.00, 5400, 4320),
('ANZ',          'Online', 2025,2, 1550000.00, 6200, 4960),
('ANZ',          'Online', 2025,3, 1750000.00, 7000, 5600),
('ANZ',          'Online', 2025,4, 2100000.00, 8400, 6720);

-- -----------------------------------------------------------------------------
-- A4. FABRIC_MARKETING_CAMPAIGNS -- 25 campaigns (2024-2025)
-- Simulates Fabric (Dynamics 365 / CRM Dataflows -> Lakehouse)
-- OneLake path: Files/snowflake-iceberg/fabric/marketing_campaigns/
-- -----------------------------------------------------------------------------

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS (
    CAMPAIGN_ID    STRING  NOT NULL,
    CAMPAIGN_NAME  STRING NOT NULL,
    CAMPAIGN_TYPE  STRING           COMMENT 'Email, Paid Search, Social, Display, Direct Mail',
    START_DATE     DATE,
    END_DATE       DATE,
    TARGET_SEGMENT STRING,
    BUDGET_USD     NUMBER(12,2),
    SPEND_USD      NUMBER(12,2),
    IMPRESSIONS    BIGINT,
    CLICKS         BIGINT,
    CONVERSIONS    BIGINT
)
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'fabric/marketing_campaigns/'
  COMMENT = 'Marketing campaign data from Fabric CRM -- seeded to OneLake as Iceberg';

INSERT INTO BRONZE.FABRIC_MARKETING_CAMPAIGNS
    (CAMPAIGN_ID, CAMPAIGN_NAME, CAMPAIGN_TYPE, START_DATE, END_DATE,
     TARGET_SEGMENT, BUDGET_USD, SPEND_USD, IMPRESSIONS, CLICKS, CONVERSIONS)
VALUES
('CAMP-2024-001','Spring Sale Email Blast',          'Email',       '2024-03-01','2024-03-31','Standard',      25000.00, 23800.00,  450000,  18000, 1440),
('CAMP-2024-002','Q1 Paid Search Branded',           'Paid Search', '2024-01-01','2024-03-31','New Customers',  85000.00, 82000.00, 1200000,  48000, 2400),
('CAMP-2024-003','Social Media Brand Awareness Q1',  'Social',      '2024-01-15','2024-03-15','Standard',       35000.00, 33500.00, 2500000,  75000, 1500),
('CAMP-2024-004','Premium Customer Nurture',         'Email',       '2024-02-01','2024-04-30','Premium',        15000.00, 14200.00,  120000,   9600,  960),
('CAMP-2024-005','Summer Display Campaign',          'Display',     '2024-06-01','2024-08-31','Standard',       60000.00, 58000.00, 8000000, 160000, 3200),
('CAMP-2024-006','Enterprise Direct Mail Outreach',  'Direct Mail', '2024-04-01','2024-05-31','Enterprise',     45000.00, 43500.00,   50000,   5000,  750),
('CAMP-2024-007','Mid-Year Sale Paid Search',        'Paid Search', '2024-07-01','2024-07-31','New Customers',  95000.00, 91000.00, 1500000,  60000, 3000),
('CAMP-2024-008','Back to School Social',            'Social',      '2024-08-01','2024-09-15','Standard',       40000.00, 38500.00, 3200000,  96000, 1920),
('CAMP-2024-009','Re-engagement Email Series',       'Email',       '2024-05-01','2024-06-30','Re-engagement',  20000.00, 18900.00,  280000,  11200,  560),
('CAMP-2024-010','Q3 Retargeting Display',           'Display',     '2024-07-01','2024-09-30','Re-engagement',  55000.00, 53000.00, 6500000, 130000, 2600),
('CAMP-2024-011','Holiday Season Paid Search',       'Paid Search', '2024-10-01','2024-12-31','New Customers', 150000.00,148000.00, 2200000,  88000, 7040),
('CAMP-2024-012','Black Friday Email Campaign',      'Email',       '2024-11-25','2024-11-30','Standard',       30000.00, 29500.00,  600000,  36000, 4320),
('CAMP-2024-013','Holiday Social Media Blitz',       'Social',      '2024-11-01','2024-12-31','Standard',       75000.00, 73000.00, 5000000, 200000, 6000),
('CAMP-2024-014','Year-End Display Retargeting',     'Display',     '2024-12-01','2024-12-31','Re-engagement',  65000.00, 63000.00, 7500000, 150000, 4500),
('CAMP-2024-015','Premium Holiday Direct Mail',      'Direct Mail', '2024-11-15','2024-12-15','Premium',        55000.00, 53500.00,   75000,   7500, 1125),
('CAMP-2025-001','New Year Paid Search Launch',      'Paid Search', '2025-01-01','2025-01-31','New Customers',  70000.00, 65000.00,  950000,  38000, 1900),
('CAMP-2025-002','Valentines Day Email',             'Email',       '2025-02-01','2025-02-14','Standard',       22000.00, 21000.00,  380000,  15200, 1216),
('CAMP-2025-003','Q1 Brand Awareness Social',        'Social',      '2025-01-15','2025-03-31','Standard',       50000.00, 47500.00, 4000000, 120000, 2400),
('CAMP-2025-004','Spring Launch Display',            'Display',     '2025-03-01','2025-05-31','New Customers',  80000.00, 75000.00,10000000, 200000, 4000),
('CAMP-2025-005','SMB Outreach Direct Mail',         'Direct Mail', '2025-02-01','2025-03-31','SMB',            40000.00, 38000.00,   60000,   6000,  720),
('CAMP-2025-006','Budget Segment Email Nurture',     'Email',       '2025-01-01','2025-03-31','Budget',         18000.00, 17200.00,  320000,  12800,  640),
('CAMP-2025-007','Spring Paid Search Non-Brand',     'Paid Search', '2025-04-01','2025-06-30','New Customers', 110000.00,104500.00, 1600000,  64000, 3840),
('CAMP-2025-008','Mothers Day Social',               'Social',      '2025-04-15','2025-05-12','Standard',       45000.00, 43000.00, 3500000, 105000, 3150),
('CAMP-2025-009','Q2 Retargeting Display',           'Display',     '2025-04-01','2025-06-30','Re-engagement',  70000.00, 66500.00, 8500000, 170000, 5100),
('CAMP-2025-010','Summer Preview Email',             'Email',       '2025-05-15','2025-06-15','Standard',       28000.00, 27000.00,  500000,  20000, 1600);

GRANT SELECT ON TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS    TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_IOT_EVENTS            TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_REGIONAL_TARGETS      TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS   TO ROLE DEMO_ANALYST;

SHOW ICEBERG TABLES LIKE 'FABRIC_%' IN SCHEMA BRONZE;

SELECT 'FABRIC_CLICKSTREAM_EVENTS'  AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.FABRIC_CLICKSTREAM_EVENTS
UNION ALL SELECT 'FABRIC_IOT_EVENTS',          COUNT(*) FROM BRONZE.FABRIC_IOT_EVENTS
UNION ALL SELECT 'FABRIC_REGIONAL_TARGETS',    COUNT(*) FROM BRONZE.FABRIC_REGIONAL_TARGETS
UNION ALL SELECT 'FABRIC_MARKETING_CAMPAIGNS', COUNT(*) FROM BRONZE.FABRIC_MARKETING_CAMPAIGNS;

-- Metadata locations in OneLake (use these paths for Section B in production):
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_CLICKSTREAM_EVENTS')   AS CLICKSTREAM_META;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_IOT_EVENTS')           AS IOT_META;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_REGIONAL_TARGETS')     AS TARGETS_META;
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_MARKETING_CAMPAIGNS')  AS CAMPAIGNS_META;

-- Surface in Fabric via OneLake shortcuts (manual):
--   Fabric workspace -> demo_lakehouse -> Tables -> "..." -> New shortcut -> OneLake
--   clickstream_events   -> Files/snowflake-iceberg/fabric/clickstream_events/
--   iot_events           -> Files/snowflake-iceberg/fabric/iot_events/
--   regional_targets     -> Files/snowflake-iceberg/fabric/regional_targets/
--   marketing_campaigns  -> Files/snowflake-iceberg/fabric/marketing_campaigns/

-- Refresh after inserting new rows:
-- ALTER ICEBERG TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS   REFRESH;
-- ALTER ICEBERG TABLE BRONZE.FABRIC_IOT_EVENTS           REFRESH;
-- ALTER ICEBERG TABLE BRONZE.FABRIC_REGIONAL_TARGETS     REFRESH;
-- ALTER ICEBERG TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS  REFRESH;

SELECT 'Section A complete -- synthetic Fabric data written to OneLake as Iceberg.' AS STATUS;

-- =============================================================================
-- SECTION B: PRODUCTION CATALOG INTEGRATION  [PRODUCTION]
-- =============================================================================
-- Points Snowflake at real Fabric-managed Iceberg tables in OneLake.
-- No data is copied -- both platforms read the same Parquet files.
--
-- How it works:
--   1. Fabric Lakehouse tables are stored as Delta in OneLake.
--   2. OneLake auto-generates Iceberg metadata (*.metadata.json) alongside each table.
--   3. Snowflake uses CATALOG_SOURCE = OBJECT_STORE (no REST catalog, no OAuth).
--      Access via ONELAKE_READ_VOL (service principal with Contributor on workspace).
--
-- !! STOP: Complete Step 0 below before running the CREATE TABLE statements. !!
-- The METADATA_FILE_PATH must be the exact path to a real .metadata.json file.
-- Running with a placeholder will cause "Missing or invalid file" errors.
--
-- Skip this section if you ran Section A (they CREATE OR REPLACE the same tables).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- B0. GET ACTUAL METADATA FILE PATHS  (run first, do not skip)
-- -----------------------------------------------------------------------------
-- OPTION 1: If you ran Section A, extract paths from the seeded Iceberg tables.
-- The metadataLocation value is the full OneLake URL; extract the part after
-- the ONELAKE_READ_VOL base URL and paste it into the METADATA_FILE_PATH below.

SELECT
    'FABRIC_CLICKSTREAM_EVENTS'  AS TABLE_NAME,
    PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_CLICKSTREAM_EVENTS'))
        :metadataLocation::STRING AS METADATA_LOCATION
UNION ALL SELECT
    'FABRIC_IOT_EVENTS',
    PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_IOT_EVENTS'))
        :metadataLocation::STRING
UNION ALL SELECT
    'FABRIC_REGIONAL_TARGETS',
    PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_REGIONAL_TARGETS'))
        :metadataLocation::STRING
UNION ALL SELECT
    'FABRIC_MARKETING_CAMPAIGNS',
    PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('BRONZE.FABRIC_MARKETING_CAMPAIGNS'))
        :metadataLocation::STRING;

-- OPTION 2: Browse OneLake directly to find the latest metadata file.
-- In the Fabric portal: Lakehouse -> Files -> snowflake-iceberg -> fabric ->
--   <table_folder> -> metadata -> copy the filename ending in .metadata.json
-- The path to use is relative to the ONELAKE_READ_VOL base URL, e.g.:
--   fabric/clickstream_events/metadata/00001-<uuid>.metadata.json

-- OPTION 3: List files via the external volume stage.
-- LIST @BRONZE.ADLS_DATA_STAGE/snowflake-iceberg/fabric/clickstream_events/metadata/;
-- LIST @BRONZE.ADLS_DATA_STAGE/snowflake-iceberg/fabric/iot_events/metadata/;
-- LIST @BRONZE.ADLS_DATA_STAGE/snowflake-iceberg/fabric/regional_targets/metadata/;
-- LIST @BRONZE.ADLS_DATA_STAGE/snowflake-iceberg/fabric/marketing_campaigns/metadata/;

-- After retrieving the paths, replace the METADATA_FILE_PATH values below and
-- then run the remainder of this section.
-- -----------------------------------------------------------------------------

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

CREATE OR REPLACE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT   = ICEBERG
  ENABLED        = TRUE
  COMMENT = 'Reads Iceberg metadata from Fabric OneLake for zero-copy table access';

DESCRIBE CATALOG INTEGRATION FABRIC_ONELAKE_CATALOG_INT;
GRANT USAGE ON INTEGRATION FABRIC_ONELAKE_CATALOG_INT TO ROLE DEMO_ADMIN;

USE ROLE DEMO_ADMIN;
USE SCHEMA BRONZE;

-- !! Replace each METADATA_FILE_PATH value with the actual path from Step B0 above !!
-- Path is relative to the ONELAKE_READ_VOL base URL.
-- Example: 'fabric/clickstream_events/metadata/00001-abc123def456.metadata.json'

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'fabric/clickstream_events/metadata/<replace-with-actual-uuid>.metadata.json'
  COMMENT = 'Clickstream events from Fabric Real-Time Intelligence -- zero-copy via OneLake';

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_IOT_EVENTS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'fabric/iot_events/metadata/<replace-with-actual-uuid>.metadata.json'
  COMMENT = 'IoT sensor events from Fabric Real-Time Intelligence -- zero-copy via OneLake';

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_REGIONAL_TARGETS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'fabric/regional_targets/metadata/<replace-with-actual-uuid>.metadata.json'
  COMMENT = 'Regional sales targets from Fabric Lakehouse -- zero-copy via OneLake';

CREATE OR REPLACE ICEBERG TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS
  EXTERNAL_VOLUME    = 'ONELAKE_READ_VOL'
  CATALOG            = FABRIC_ONELAKE_CATALOG_INT
  METADATA_FILE_PATH = 'fabric/marketing_campaigns/metadata/<replace-with-actual-uuid>.metadata.json'
  COMMENT = 'Marketing campaigns from Fabric Lakehouse -- zero-copy via OneLake';

GRANT SELECT ON TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS    TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS    TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_IOT_EVENTS            TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE BRONZE.FABRIC_IOT_EVENTS            TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_REGIONAL_TARGETS      TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE BRONZE.FABRIC_REGIONAL_TARGETS      TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS   TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS   TO ROLE DEMO_ANALYST;

SHOW ICEBERG TABLES IN SCHEMA BRONZE;

SELECT * FROM BRONZE.FABRIC_CLICKSTREAM_EVENTS  LIMIT 5;
SELECT * FROM BRONZE.FABRIC_IOT_EVENTS           LIMIT 5;
SELECT * FROM BRONZE.FABRIC_REGIONAL_TARGETS     LIMIT 5;
SELECT * FROM BRONZE.FABRIC_MARKETING_CAMPAIGNS  LIMIT 5;

-- Refresh after Fabric writes new data:
-- ALTER ICEBERG TABLE BRONZE.FABRIC_CLICKSTREAM_EVENTS   REFRESH 'clickstream_events/metadata/<new>.metadata.json';
-- ALTER ICEBERG TABLE BRONZE.FABRIC_IOT_EVENTS           REFRESH 'iot_events/metadata/<new>.metadata.json';
-- ALTER ICEBERG TABLE BRONZE.FABRIC_REGIONAL_TARGETS     REFRESH 'regional_sales_targets/metadata/<new>.metadata.json';
-- ALTER ICEBERG TABLE BRONZE.FABRIC_MARKETING_CAMPAIGNS  REFRESH 'marketing_campaigns/metadata/<new>.metadata.json';

SELECT 'Section B complete -- Fabric OneLake catalog integration ready.' AS STATUS;

-- =============================================================================
-- SECTION C: WRITE BRONZE RAW TABLES BACK TO FABRIC  [BOTH PATHS]
-- =============================================================================
-- Exports all BRONZE raw tables to OneLake as Snowflake-managed Iceberg in ICEBERG schema.
-- Makes data available to Fabric (Spark, SQL Endpoint, Power BI DirectLake).
--
-- Run after either Section A or Section B.
-- Prerequisites: BRONZE tables populated (02_bronze/01_tables_and_data.sql).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA ICEBERG;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_CUSTOMERS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/customers/'
  COMMENT = 'Raw customer master data -- Bronze synthetic data exported to Fabric'
AS
SELECT CUSTOMER_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE,
       ADDRESS, CITY, STATE, COUNTRY, POSTAL_CODE,
       CUSTOMER_SEGMENT, REGISTRATION_DATE, IS_ACTIVE,
       CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.CUSTOMERS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_PRODUCTS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/products/'
  COMMENT = 'Raw product catalog -- Bronze synthetic data exported to Fabric'
AS
SELECT PRODUCT_ID, PRODUCT_NAME, CATEGORY, SUB_CATEGORY,
       BRAND, SKU, UNIT_PRICE, COST_PRICE, IS_ACTIVE,
       CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.PRODUCTS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_ORDERS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/orders/'
  COMMENT = 'Raw orders -- Bronze synthetic data exported to Fabric'
AS
SELECT ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_STATUS,
       TOTAL_AMOUNT, DISCOUNT_AMOUNT, SHIPPING_AMOUNT,
       PAYMENT_METHOD, REGION, CHANNEL, SOURCE_SYSTEM,
       CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.ORDERS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_ORDER_ITEMS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/order_items/'
  COMMENT = 'Raw order line items -- Bronze synthetic data exported to Fabric'
AS
SELECT ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, QUANTITY,
       UNIT_PRICE, DISCOUNT_PCT, LINE_TOTAL,
       CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.ORDER_ITEMS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_PRODUCT_REVIEWS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/product_reviews/'
  COMMENT = 'Raw product reviews -- Bronze synthetic data exported to Fabric'
AS
SELECT REVIEW_ID, PRODUCT_ID, CUSTOMER_ID, RATING,
       REVIEW_TEXT, REVIEW_DATE, HELPFUL_VOTES,
       CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.PRODUCT_REVIEWS;

CREATE OR REPLACE ICEBERG TABLE ICEBERG.RAW_SUPPORT_TICKETS_ICEBERG
  CATALOG         = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION   = 'raw/support_tickets/'
  COMMENT = 'Raw support tickets -- Bronze synthetic data exported to Fabric'
AS
SELECT TICKET_ID, CUSTOMER_ID, PRODUCT_ID, TICKET_SUBJECT, TICKET_DESCRIPTION,
       PRIORITY, STATUS, CATEGORY, RESOLUTION_TIME_HOURS, SATISFACTION_SCORE,
       CREATED_AT, RESOLVED_AT, CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM BRONZE.SUPPORT_TICKETS;

GRANT SELECT ON TABLE ICEBERG.RAW_CUSTOMERS_ICEBERG       TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_CUSTOMERS_ICEBERG       TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.RAW_PRODUCTS_ICEBERG        TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_PRODUCTS_ICEBERG        TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.RAW_ORDERS_ICEBERG          TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_ORDERS_ICEBERG          TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.RAW_ORDER_ITEMS_ICEBERG     TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_ORDER_ITEMS_ICEBERG     TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.RAW_PRODUCT_REVIEWS_ICEBERG TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_PRODUCT_REVIEWS_ICEBERG TO ROLE DEMO_ANALYST;
GRANT SELECT ON TABLE ICEBERG.RAW_SUPPORT_TICKETS_ICEBERG TO ROLE DEMO_ADMIN;
GRANT SELECT ON TABLE ICEBERG.RAW_SUPPORT_TICKETS_ICEBERG TO ROLE DEMO_ANALYST;

SHOW ICEBERG TABLES IN SCHEMA ICEBERG;

SELECT 'RAW_CUSTOMERS_ICEBERG'       AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ICEBERG.RAW_CUSTOMERS_ICEBERG
UNION ALL SELECT 'RAW_PRODUCTS_ICEBERG',       COUNT(*) FROM ICEBERG.RAW_PRODUCTS_ICEBERG
UNION ALL SELECT 'RAW_ORDERS_ICEBERG',         COUNT(*) FROM ICEBERG.RAW_ORDERS_ICEBERG
UNION ALL SELECT 'RAW_ORDER_ITEMS_ICEBERG',    COUNT(*) FROM ICEBERG.RAW_ORDER_ITEMS_ICEBERG
UNION ALL SELECT 'RAW_PRODUCT_REVIEWS_ICEBERG',COUNT(*) FROM ICEBERG.RAW_PRODUCT_REVIEWS_ICEBERG
UNION ALL SELECT 'RAW_SUPPORT_TICKETS_ICEBERG',COUNT(*) FROM ICEBERG.RAW_SUPPORT_TICKETS_ICEBERG;

-- Surface in Fabric via OneLake shortcuts:
--   raw_customers       -> Files/snowflake-iceberg/raw/customers/
--   raw_products        -> Files/snowflake-iceberg/raw/products/
--   raw_orders          -> Files/snowflake-iceberg/raw/orders/
--   raw_order_items     -> Files/snowflake-iceberg/raw/order_items/
--   raw_product_reviews -> Files/snowflake-iceberg/raw/product_reviews/
--   raw_support_tickets -> Files/snowflake-iceberg/raw/support_tickets/

-- Refresh after Bronze data changes:
-- ALTER ICEBERG TABLE ICEBERG.RAW_CUSTOMERS_ICEBERG       REFRESH;
-- ALTER ICEBERG TABLE ICEBERG.RAW_PRODUCTS_ICEBERG        REFRESH;
-- ALTER ICEBERG TABLE ICEBERG.RAW_ORDERS_ICEBERG          REFRESH;
-- ALTER ICEBERG TABLE ICEBERG.RAW_ORDER_ITEMS_ICEBERG     REFRESH;
-- ALTER ICEBERG TABLE ICEBERG.RAW_PRODUCT_REVIEWS_ICEBERG REFRESH;
-- ALTER ICEBERG TABLE ICEBERG.RAW_SUPPORT_TICKETS_ICEBERG REFRESH;

SELECT 'Section C complete -- Bronze raw data written to Fabric OneLake as Iceberg.' AS STATUS;
