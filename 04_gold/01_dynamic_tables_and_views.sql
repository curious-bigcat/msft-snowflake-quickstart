-- =============================================================================
-- MEDALLION ARCHITECTURE: GOLD Layer — Dynamic Tables and Materialized Views
-- =============================================================================
-- Consumption-ready Gold layer built from curated Silver data.
-- Dynamic Tables: auto-refreshing aggregations for analytics workloads.
-- Materialized Views: pre-computed, low-latency views for BI and Cortex AI.
--
-- Prerequisites: 03_silver/01_dynamic_tables.sql (SILVER DTs must exist)
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. GOLD DYNAMIC TABLES — Auto-refreshing aggregations
-- =============================================================================

-- Sales summary by region, channel, and time
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_SALES_SUMMARY
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Gold: Sales aggregations by region, channel, and time period'
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

-- Customer 360 view
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_CUSTOMER_360
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Gold: Customer 360 aggregation for analytics and ML'
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
    -- RFM-style segmentation
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

-- Product performance
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_PRODUCT_PERFORMANCE
  TARGET_LAG = '15 minutes'
  WAREHOUSE = DEMO_WH
  COMMENT = 'Gold: Product performance metrics'
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
    -- Review metrics
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
-- 2. GOLD MATERIALIZED VIEWS — Pre-aggregated for BI and low-latency consumption
-- =============================================================================

-- MV: Top customers by lifetime value — refreshes automatically when DT_CUSTOMER_360 changes
CREATE OR REPLACE MATERIALIZED VIEW GOLD.MV_TOP_CUSTOMERS
  COMMENT = 'Top 1000 customers ranked by lifetime value — optimized for BI dashboards'
AS
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_SEGMENT,
    CITY,
    STATE,
    COUNTRY,
    LIFETIME_VALUE,
    TOTAL_ORDERS,
    AVG_ORDER_VALUE,
    DAYS_SINCE_LAST_ORDER,
    ENGAGEMENT_STATUS,
    CUSTOMER_TIER,
    DENSE_RANK() OVER (ORDER BY LIFETIME_VALUE DESC) AS LTV_RANK
FROM GOLD.DT_CUSTOMER_360
WHERE LIFETIME_VALUE > 0
QUALIFY LTV_RANK <= 1000;

-- MV: Monthly KPI summary — for executive dashboards and Cortex Analyst
CREATE OR REPLACE MATERIALIZED VIEW GOLD.MV_MONTHLY_KPI
  COMMENT = 'Monthly KPIs by region and channel — pre-aggregated for fast BI access'
AS
SELECT
    MONTH,
    REGION,
    CHANNEL,
    TOTAL_ORDERS,
    TOTAL_REVENUE,
    UNIQUE_CUSTOMERS,
    AVG_ORDER_VALUE,
    TOTAL_DISCOUNT,
    ROUND(TOTAL_DISCOUNT / NULLIF(TOTAL_REVENUE + TOTAL_DISCOUNT, 0) * 100, 2) AS DISCOUNT_RATE_PCT,
    SUM(TOTAL_REVENUE) OVER (
        PARTITION BY REGION ORDER BY MONTH
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS CUMULATIVE_REVENUE
FROM GOLD.DT_SALES_SUMMARY;

-- MV: Product health — combines sales performance with review sentiment
CREATE OR REPLACE MATERIALIZED VIEW GOLD.MV_PRODUCT_HEALTH
  COMMENT = 'Product 360 view — revenue, units, sentiment, review scores for merchandising'
AS
SELECT
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUB_CATEGORY,
    p.BRAND,
    p.UNIT_PRICE,
    p.TOTAL_UNITS_SOLD,
    p.TOTAL_REVENUE,
    p.AVG_RATING,
    p.REVIEW_COUNT,
    p.POSITIVE_REVIEW_PCT,
    ps.AVG_SENTIMENT,
    ps.POSITIVE_REVIEWS,
    ps.NEGATIVE_REVIEWS,
    ps.TOTAL_HELPFUL_VOTES,
    CASE
        WHEN p.AVG_RATING >= 4.5 AND ps.AVG_SENTIMENT > 0.3 THEN 'Star'
        WHEN p.AVG_RATING >= 4.0 AND ps.AVG_SENTIMENT > 0.1 THEN 'Strong'
        WHEN p.AVG_RATING >= 3.0 THEN 'Average'
        WHEN p.AVG_RATING < 3.0 OR ps.AVG_SENTIMENT < -0.1 THEN 'At Risk'
        ELSE 'Unrated'
    END AS PRODUCT_HEALTH_SCORE
FROM GOLD.DT_PRODUCT_PERFORMANCE p
LEFT JOIN GOLD.PRODUCT_SENTIMENT_SUMMARY ps ON p.PRODUCT_ID = ps.PRODUCT_ID;

-- =============================================================================
-- 3. VERIFICATION
-- =============================================================================

-- Check dynamic table status
SHOW DYNAMIC TABLES IN SCHEMA GOLD;

-- Check materialized view status
SHOW MATERIALIZED VIEWS IN SCHEMA GOLD;

-- Sample gold layer output
SELECT * FROM GOLD.DT_SALES_SUMMARY   LIMIT 5;
SELECT * FROM GOLD.DT_CUSTOMER_360    ORDER BY LIFETIME_VALUE DESC LIMIT 5;
SELECT * FROM GOLD.MV_TOP_CUSTOMERS   LIMIT 5;
SELECT * FROM GOLD.MV_MONTHLY_KPI     ORDER BY MONTH DESC LIMIT 10;
SELECT * FROM GOLD.MV_PRODUCT_HEALTH  ORDER BY TOTAL_REVENUE DESC LIMIT 10;

SELECT 'Gold layer dynamic tables and materialized views created.' AS STATUS;
