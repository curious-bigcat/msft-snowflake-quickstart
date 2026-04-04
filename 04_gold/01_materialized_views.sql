-- =============================================================================
-- MEDALLION ARCHITECTURE: GOLD Layer — Materialized Views
-- =============================================================================
-- Pre-computed, low-latency consumption views for BI, dashboards, and Cortex AI.
-- Built on top of Silver dynamic tables — refreshes automatically when upstream
-- dynamic tables change.
--
-- Prerequisites: 03_silver/02_dynamic_tables.sql (Silver DTs must exist)
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. TOP CUSTOMERS BY LIFETIME VALUE
-- =============================================================================

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

-- =============================================================================
-- 2. MONTHLY KPI SUMMARY
-- =============================================================================

-- For executive dashboards and Cortex Analyst
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

-- =============================================================================
-- 3. PRODUCT HEALTH
-- =============================================================================

-- Combines sales performance with review sentiment
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
-- 4. VERIFICATION
-- =============================================================================

SHOW MATERIALIZED VIEWS IN SCHEMA GOLD;

SELECT * FROM GOLD.MV_TOP_CUSTOMERS  LIMIT 5;
SELECT * FROM GOLD.MV_MONTHLY_KPI    ORDER BY MONTH DESC LIMIT 10;
SELECT * FROM GOLD.MV_PRODUCT_HEALTH ORDER BY TOTAL_REVENUE DESC LIMIT 10;

SELECT 'Gold materialized views created.' AS STATUS;
