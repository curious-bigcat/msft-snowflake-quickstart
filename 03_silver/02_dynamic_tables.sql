-- =============================================================================
-- MEDALLION ARCHITECTURE: SILVER Layer — Dynamic Tables
-- =============================================================================
-- Auto-refreshing aggregations built from curated Silver and Bronze data.
-- These serve as the processed, analytics-ready layer consumed by Gold views,
-- ML models, and Cortex AI features.
--
-- Prerequisites: 03_silver/01_silver_processing.sql (streams + tasks running)
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. SALES SUMMARY
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
-- 2. CUSTOMER 360
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
-- 3. PRODUCT PERFORMANCE
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
-- 4. VERIFICATION
-- =============================================================================

SHOW DYNAMIC TABLES IN SCHEMA GOLD;

SELECT * FROM GOLD.DT_SALES_SUMMARY              LIMIT 5;
SELECT * FROM GOLD.DT_CUSTOMER_360  ORDER BY LIFETIME_VALUE DESC LIMIT 5;
SELECT * FROM GOLD.DT_PRODUCT_PERFORMANCE ORDER BY TOTAL_REVENUE DESC LIMIT 5;

SELECT 'Silver dynamic tables created.' AS STATUS;
