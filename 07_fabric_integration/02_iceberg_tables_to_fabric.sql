-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Iceberg Tables — Snowflake to Fabric
-- =============================================================================
-- Creates Apache Iceberg tables in Snowflake that write data to OneLake,
-- making it readable from Microsoft Fabric (Lakehouse, Spark, SQL Endpoint).
--
-- This is the "Snowflake → Fabric" direction of bidirectional access:
--   Snowflake creates Iceberg tables → stored in OneLake → Fabric reads them
--
-- Iceberg tables use open Parquet + metadata format, so Fabric can read
-- them natively without any Snowflake dependency.
--
-- Prerequisites:
--   - External volume ONELAKE_EXTERNAL_VOL configured and consent granted
--   - Run phases 01-04 (setup through processing)
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. CREATE SCHEMA FOR ICEBERG TABLES
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS MSFT_SNOWFLAKE_DEMO.ICEBERG
  COMMENT = 'Iceberg tables for Snowflake-Fabric bidirectional access';

USE SCHEMA ICEBERG;

GRANT USAGE ON SCHEMA ICEBERG TO ROLE DEMO_ADMIN;
GRANT USAGE ON SCHEMA ICEBERG TO ROLE DEMO_ANALYST;

-- =============================================================================
-- 2. ICEBERG TABLE — Customer 360 (Gold Layer → Fabric)
-- =============================================================================
-- Export the Customer 360 gold layer to Fabric for Power BI dashboards
-- and cross-platform analytics.

CREATE OR REPLACE ICEBERG TABLE ICEBERG.CUSTOMER_360_ICEBERG
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION = 'customer_360/'
  COMMENT = 'Customer 360 data in Iceberg format — readable from Fabric'
AS
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    CUSTOMER_EMAIL,
    CUSTOMER_SEGMENT,
    CUSTOMER_CITY,
    CUSTOMER_STATE,
    TOTAL_ORDERS,
    LIFETIME_VALUE,
    AVG_ORDER_VALUE,
    FIRST_ORDER_DATE,
    LAST_ORDER_DATE,
    DAYS_SINCE_LAST_ORDER,
    PREFERRED_PAYMENT,
    PREFERRED_CHANNEL,
    PRIMARY_REGION,
    CANCELLED_ORDERS,
    ENGAGEMENT_STATUS,
    CUSTOMER_TIER,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM ANALYTICS.DT_CUSTOMER_360;

-- =============================================================================
-- 3. ICEBERG TABLE — Sales Summary (Gold Layer → Fabric)
-- =============================================================================
-- Export aggregated sales data for Fabric SQL Analytics Endpoint
-- and Power BI reporting.

CREATE OR REPLACE ICEBERG TABLE ICEBERG.SALES_SUMMARY_ICEBERG
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION = 'sales_summary/'
  COMMENT = 'Sales summary data in Iceberg format — readable from Fabric'
AS
SELECT
    ORDER_MONTH,
    ORDER_YEAR,
    REGION,
    CHANNEL,
    CUSTOMER_SEGMENT,
    ORDER_VALUE_TIER,
    ORDER_COUNT,
    UNIQUE_CUSTOMERS,
    GROSS_REVENUE,
    TOTAL_DISCOUNTS,
    NET_REVENUE,
    AVG_ORDER_VALUE,
    MEDIAN_ORDER_VALUE,
    MAX_ORDER_VALUE,
    CANCELLED_ORDERS,
    CANCELLATION_RATE_PCT,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM ANALYTICS.DT_SALES_SUMMARY;

-- =============================================================================
-- 4. ICEBERG TABLE — Product Performance (Gold Layer → Fabric)
-- =============================================================================
-- Export product metrics for Fabric data scientists and Power BI.

CREATE OR REPLACE ICEBERG TABLE ICEBERG.PRODUCT_PERFORMANCE_ICEBERG
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION = 'product_performance/'
  COMMENT = 'Product performance data in Iceberg format — readable from Fabric'
AS
SELECT
    PRODUCT_ID,
    PRODUCT_NAME,
    CATEGORY,
    SUB_CATEGORY,
    BRAND,
    LIST_PRICE,
    ORDERS_CONTAINING_PRODUCT,
    TOTAL_UNITS_SOLD,
    TOTAL_REVENUE,
    AVG_SELLING_PRICE,
    AVG_DISCOUNT_PCT,
    AVG_RATING,
    REVIEW_COUNT,
    POSITIVE_REVIEW_PCT,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM ANALYTICS.DT_PRODUCT_PERFORMANCE;

-- =============================================================================
-- 5. ICEBERG TABLE — ML Predictions (ML Output → Fabric)
-- =============================================================================
-- Export ML predictions so Fabric users can build Power BI reports
-- showing predicted vs actual values.

CREATE OR REPLACE ICEBERG TABLE ICEBERG.ML_PREDICTIONS_ICEBERG
  CATALOG = 'SNOWFLAKE'
  EXTERNAL_VOLUME = 'ONELAKE_EXTERNAL_VOL'
  BASE_LOCATION = 'ml_predictions/'
  COMMENT = 'ML regression predictions in Iceberg format — readable from Fabric'
AS
SELECT
    ORDER_MONTH,
    CATEGORY,
    MONTHLY_REVENUE AS ACTUAL_REVENUE,
    PREDICTED_REVENUE,
    RESIDUAL,
    PCT_ERROR,
    MODEL_NAME,
    MODEL_VERSION,
    CURRENT_TIMESTAMP() AS EXPORTED_AT
FROM ML.REGRESSION_PREDICTIONS;

-- =============================================================================
-- 6. REFRESH ICEBERG TABLES
-- =============================================================================
-- Iceberg tables with CATALOG = 'SNOWFLAKE' are managed by Snowflake.
-- To update the data, refresh by inserting/replacing data:

-- Example: Full refresh of customer 360
-- INSERT OVERWRITE INTO ICEBERG.CUSTOMER_360_ICEBERG
-- SELECT ... FROM ANALYTICS.DT_CUSTOMER_360;

-- Or use ALTER to refresh metadata:
ALTER ICEBERG TABLE ICEBERG.CUSTOMER_360_ICEBERG REFRESH;
ALTER ICEBERG TABLE ICEBERG.SALES_SUMMARY_ICEBERG REFRESH;
ALTER ICEBERG TABLE ICEBERG.PRODUCT_PERFORMANCE_ICEBERG REFRESH;
ALTER ICEBERG TABLE ICEBERG.ML_PREDICTIONS_ICEBERG REFRESH;

-- =============================================================================
-- 7. VERIFY ICEBERG TABLES
-- =============================================================================

SHOW ICEBERG TABLES IN SCHEMA ICEBERG;

-- Check row counts
SELECT 'CUSTOMER_360_ICEBERG' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM ICEBERG.CUSTOMER_360_ICEBERG
UNION ALL
SELECT 'SALES_SUMMARY_ICEBERG', COUNT(*) FROM ICEBERG.SALES_SUMMARY_ICEBERG
UNION ALL
SELECT 'PRODUCT_PERFORMANCE_ICEBERG', COUNT(*) FROM ICEBERG.PRODUCT_PERFORMANCE_ICEBERG
UNION ALL
SELECT 'ML_PREDICTIONS_ICEBERG', COUNT(*) FROM ICEBERG.ML_PREDICTIONS_ICEBERG;

-- Check Iceberg metadata
SELECT
    TABLE_NAME,
    TABLE_TYPE,
    IS_ICEBERG,
    BYTES,
    ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'ICEBERG'
ORDER BY TABLE_NAME;

-- =============================================================================
-- 8. GRANT ACCESS
-- =============================================================================

GRANT SELECT ON ALL TABLES IN SCHEMA ICEBERG TO ROLE DEMO_ADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA ICEBERG TO ROLE DEMO_ANALYST;

-- =============================================================================
-- 9. MAKE TABLES VISIBLE IN FABRIC (Required: OneLake Shortcut)
-- =============================================================================
-- The Iceberg tables are now written to OneLake's Files area. To surface them
-- as proper Lakehouse Tables in Fabric (Power BI, Spark, SQL Endpoint), you
-- MUST create a OneLake shortcut in the Lakehouse Tables section.
--
-- Without the shortcut, the data exists in Files but Fabric does NOT
-- automatically treat it as a queryable Lakehouse table.
--
-- Steps in Microsoft Fabric Portal (app.fabric.microsoft.com):
--   1. Open your Fabric workspace → open demo_lakehouse
--   2. In the Tables panel (left side), click "..." → New shortcut
--   3. Select OneLake as the source
--   4. Navigate to: Files → snowflake-iceberg → <table_folder>
--      e.g., Files/snowflake-iceberg/customer_360/
--   5. Click Next → confirm the name → Create shortcut
--   6. Repeat for each table:
--      - customer_360      → Files/snowflake-iceberg/customer_360/
--      - sales_summary     → Files/snowflake-iceberg/sales_summary/
--      - product_performance → Files/snowflake-iceberg/product_performance/
--      - ml_predictions    → Files/snowflake-iceberg/ml_predictions/
--
-- After creating shortcuts, tables appear in the Lakehouse Tables section
-- and are immediately queryable via:
--   - Fabric SQL Endpoint (T-SQL)
--   - Fabric Spark Notebooks (PySpark)
--   - Power BI DirectLake mode (no data import, reads directly from OneLake)

SELECT 'Iceberg tables created. Create OneLake shortcuts in Fabric to surface them as Lakehouse tables.' AS STATUS;
