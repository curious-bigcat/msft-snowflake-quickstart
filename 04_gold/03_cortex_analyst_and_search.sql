-- =============================================================================
-- MEDALLION ARCHITECTURE: GOLD Layer — Cortex Analyst and Search Services
-- =============================================================================
-- Part 1: Semantic View — maps business concepts to physical tables for
--         natural-language-to-SQL via Cortex Analyst
-- Part 2: Cortex Search Services — hybrid keyword+vector search over
--         product reviews and support tickets
--
-- Prerequisites: Run phases 01-04 (setup through Silver processing).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- PART 1: SEMANTIC VIEW — Sales & Customer Analytics
-- =============================================================================
-- Enables natural language questions like:
--   "What was total revenue by region last quarter?"
--   "Who are our top 10 customers by lifetime value?"
--   "Which product category has the highest average order value?"

CREATE OR REPLACE SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV

  TABLES (
    customers AS MSFT_SNOWFLAKE_DEMO.BRONZE.CUSTOMERS
      PRIMARY KEY (CUSTOMER_ID)
      WITH SYNONYMS = ('customer', 'buyer', 'client')
      COMMENT = 'Customer master data with segment and geography info',

    products AS MSFT_SNOWFLAKE_DEMO.BRONZE.PRODUCTS
      PRIMARY KEY (PRODUCT_ID)
      WITH SYNONYMS = ('product', 'item', 'SKU')
      COMMENT = 'Product catalog with categories, pricing, and brands',

    orders AS MSFT_SNOWFLAKE_DEMO.BRONZE.ORDERS
      PRIMARY KEY (ORDER_ID)
      WITH SYNONYMS = ('order', 'sale', 'transaction', 'purchase')
      COMMENT = 'Sales orders from multiple channels and sources',

    order_items AS MSFT_SNOWFLAKE_DEMO.BRONZE.ORDER_ITEMS
      PRIMARY KEY (ORDER_ITEM_ID)
      WITH SYNONYMS = ('line item', 'order line', 'order detail')
      COMMENT = 'Individual line items within each order',

    reviews AS MSFT_SNOWFLAKE_DEMO.BRONZE.PRODUCT_REVIEWS
      PRIMARY KEY (REVIEW_ID)
      WITH SYNONYMS = ('review', 'feedback', 'rating')
      COMMENT = 'Product reviews with ratings and text'
  )

  RELATIONSHIPS (
    orders (CUSTOMER_ID) REFERENCES customers,
    order_items (ORDER_ID) REFERENCES orders,
    order_items (PRODUCT_ID) REFERENCES products,
    reviews (PRODUCT_ID) REFERENCES products,
    reviews (CUSTOMER_ID) REFERENCES customers
  )

  FACTS (
    orders.net_amount AS TOTAL_AMOUNT - DISCOUNT_AMOUNT + SHIPPING_AMOUNT
      COMMENT = 'Net order amount after discount plus shipping',
    order_items.line_total_amount AS LINE_TOTAL
      COMMENT = 'Line item total amount (quantity * unit price - discount)',
    products.margin AS UNIT_PRICE - COST_PRICE
      COMMENT = 'Per-unit profit margin',
    reviews.review_rating AS RATING
      COMMENT = 'Review star rating from 1.0 to 5.0'
  )

  DIMENSIONS (
    customers.customer_name AS CONCAT(FIRST_NAME, ' ', LAST_NAME)
      WITH SYNONYMS = ('name', 'client name')
      COMMENT = 'Full customer name',
    customers.customer_segment AS CUSTOMER_SEGMENT
      WITH SYNONYMS = ('segment', 'tier', 'type')
      COMMENT = 'Customer segment: Enterprise, SMB, or Consumer',
    customers.customer_city AS CITY
      COMMENT = 'Customer city',
    customers.customer_state AS STATE
      COMMENT = 'Customer state',
    customers.customer_country AS COUNTRY
      COMMENT = 'Customer country',
    customers.registration_date AS REGISTRATION_DATE
      COMMENT = 'Date when customer registered',
    products.product_name AS PRODUCT_NAME
      WITH SYNONYMS = ('item name')
      COMMENT = 'Product name',
    products.category AS CATEGORY
      WITH SYNONYMS = ('product category', 'type')
      COMMENT = 'Product category: Electronics, Software, Cloud Services, Hardware, Accessories',
    products.sub_category AS SUB_CATEGORY
      COMMENT = 'Product sub-category',
    products.brand AS BRAND
      COMMENT = 'Product brand name',
    orders.order_date AS ORDER_DATE
      WITH SYNONYMS = ('date', 'purchase date', 'sale date')
      COMMENT = 'Date and time the order was placed',
    orders.order_year AS YEAR(ORDER_DATE)
      COMMENT = 'Year the order was placed',
    orders.order_month AS DATE_TRUNC('month', ORDER_DATE)
      COMMENT = 'Month the order was placed',
    orders.order_quarter AS DATE_TRUNC('quarter', ORDER_DATE)
      COMMENT = 'Quarter the order was placed',
    orders.order_status AS ORDER_STATUS
      WITH SYNONYMS = ('status')
      COMMENT = 'Order status: Pending, Processing, Shipped, Delivered, Cancelled',
    orders.payment_method AS PAYMENT_METHOD
      WITH SYNONYMS = ('payment type')
      COMMENT = 'Payment method: Credit Card, Debit Card, Wire Transfer, PayPal',
    orders.region AS REGION
      WITH SYNONYMS = ('geography', 'area', 'market')
      COMMENT = 'Sales region: North America, Europe, Asia Pacific, Latin America',
    orders.channel AS CHANNEL
      WITH SYNONYMS = ('sales channel')
      COMMENT = 'Sales channel: Online, In-Store, Partner, Marketplace',
    orders.source_system AS SOURCE_SYSTEM
      COMMENT = 'Data source system'
  )

  METRICS (
    orders.total_revenue AS SUM(TOTAL_AMOUNT)
      WITH SYNONYMS = ('revenue', 'sales', 'gross revenue')
      COMMENT = 'Total gross revenue from orders',
    orders.net_revenue AS SUM(orders.net_amount)
      WITH SYNONYMS = ('net sales')
      COMMENT = 'Total net revenue after discounts plus shipping',
    orders.total_discount AS SUM(DISCOUNT_AMOUNT)
      COMMENT = 'Total discount amount given',
    orders.average_order_value AS AVG(TOTAL_AMOUNT)
      WITH SYNONYMS = ('AOV', 'avg order')
      COMMENT = 'Average order value',
    orders.order_count AS COUNT(ORDER_ID)
      WITH SYNONYMS = ('number of orders', 'total orders')
      COMMENT = 'Total number of orders',
    orders.unique_customers AS COUNT(DISTINCT CUSTOMER_ID)
      COMMENT = 'Number of distinct customers who placed orders',
    orders.cancelled_orders AS COUNT_IF(ORDER_STATUS = 'Cancelled')
      COMMENT = 'Number of cancelled orders',
    order_items.total_units_sold AS SUM(QUANTITY)
      WITH SYNONYMS = ('units sold', 'quantity sold')
      COMMENT = 'Total units sold across all orders',
    order_items.total_line_revenue AS SUM(LINE_TOTAL)
      COMMENT = 'Total revenue from line items',
    customers.customer_count AS COUNT(CUSTOMER_ID)
      WITH SYNONYMS = ('number of customers')
      COMMENT = 'Total number of customers',
    reviews.average_rating AS AVG(RATING)
      WITH SYNONYMS = ('avg rating', 'avg stars')
      COMMENT = 'Average product review rating',
    reviews.review_count AS COUNT(REVIEW_ID)
      WITH SYNONYMS = ('number of reviews')
      COMMENT = 'Total number of product reviews'
  )

  COMMENT = 'Semantic view for sales, customer, and product analytics -- powers Cortex Analyst'
  AI_SQL_GENERATION 'Use this semantic view for questions about sales, revenue, orders, customers, products, and reviews.'
;

SHOW SEMANTIC VIEWS IN SCHEMA AGENTS;
DESCRIBE SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;
SHOW SEMANTIC DIMENSIONS IN SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;
SHOW SEMANTIC METRICS IN SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;

-- Test: Total revenue by region
SELECT * FROM SEMANTIC_VIEW(
    AGENTS.SALES_ANALYTICS_SV
    METRICS orders.total_revenue, orders.order_count
    DIMENSIONS orders.region
)
ORDER BY total_revenue DESC;

-- Test: Monthly revenue trend
SELECT * FROM SEMANTIC_VIEW(
    AGENTS.SALES_ANALYTICS_SV
    METRICS orders.total_revenue, orders.average_order_value
    DIMENSIONS orders.order_month
)
ORDER BY order_month;

GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_ANALYST;
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_AGENT_USER;

SELECT 'Semantic view created.' AS STATUS;

-- =============================================================================
-- PART 2: CORTEX SEARCH SERVICES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 2a. PRODUCT REVIEWS SEARCH
-- Semantic search over review text — find by meaning, not just keywords.
-- Example: "reviews about battery life", "customers who mentioned overheating"
-- -----------------------------------------------------------------------------

CREATE OR REPLACE CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH
  ON REVIEW_TEXT
  ATTRIBUTES PRODUCT_NAME, CATEGORY, BRAND, RATING, SENTIMENT_LABEL
  WAREHOUSE = DEMO_CORTEX_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search over product reviews -- used by Cortex Agent'
AS (
    SELECT
        r.REVIEW_ID,
        r.REVIEW_TEXT,
        r.RATING,
        r.REVIEW_DATE,
        r.HELPFUL_VOTES,
        p.PRODUCT_NAME,
        p.CATEGORY,
        p.BRAND,
        COALESCE(pr.SENTIMENT_LABEL, 'unknown') AS SENTIMENT_LABEL,
        COALESCE(pr.SENTIMENT_SCORE, 0)         AS SENTIMENT_SCORE
    FROM BRONZE.PRODUCT_REVIEWS r
    LEFT JOIN BRONZE.PRODUCTS p       ON r.PRODUCT_ID = p.PRODUCT_ID
    LEFT JOIN GOLD.PROCESSED_REVIEWS pr ON r.REVIEW_ID = pr.REVIEW_ID
);

-- -----------------------------------------------------------------------------
-- 2b. SUPPORT TICKET SEARCH
-- Semantic search over ticket text — find similar past issues.
-- Example: "network connectivity timeout errors", "billing overcharge"
-- -----------------------------------------------------------------------------

CREATE OR REPLACE CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH
  ON TICKET_CONTENT
  ATTRIBUTES CATEGORY, PRIORITY, STATUS, CUSTOMER_SEGMENT, PRODUCT_NAME
  WAREHOUSE = DEMO_CORTEX_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search over support tickets -- used by Cortex Agent'
AS (
    SELECT
        t.TICKET_ID,
        t.TICKET_SUBJECT || '. ' || COALESCE(t.TICKET_DESCRIPTION, '') AS TICKET_CONTENT,
        t.TICKET_SUBJECT,
        t.CATEGORY,
        t.PRIORITY,
        t.STATUS,
        t.RESOLUTION_TIME_HOURS,
        t.SATISFACTION_SCORE,
        t.CREATED_AT,
        c.CUSTOMER_SEGMENT,
        c.STATE                          AS CUSTOMER_STATE,
        COALESCE(p.PRODUCT_NAME, 'N/A')  AS PRODUCT_NAME,
        COALESCE(p.CATEGORY, 'N/A')      AS PRODUCT_CATEGORY
    FROM BRONZE.SUPPORT_TICKETS t
    LEFT JOIN BRONZE.CUSTOMERS c ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN BRONZE.PRODUCTS p  ON t.PRODUCT_ID  = p.PRODUCT_ID
);

SHOW CORTEX SEARCH SERVICES IN SCHEMA AGENTS;
DESCRIBE CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH;
DESCRIBE CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH;

-- Test: Product review search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.PRODUCT_REVIEW_SEARCH',
        '{
            "query": "battery life and charging issues",
            "columns": ["REVIEW_TEXT", "PRODUCT_NAME", "RATING", "SENTIMENT_LABEL"],
            "limit": 5
        }'
    )
) AS RESULTS;

-- Test: Support ticket search
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.SUPPORT_TICKET_SEARCH',
        '{
            "query": "network connectivity timeout errors",
            "columns": ["TICKET_SUBJECT", "CATEGORY", "PRIORITY", "STATUS", "RESOLUTION_TIME_HOURS"],
            "limit": 5
        }'
    )
) AS RESULTS;

GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_AGENT_USER;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_AGENT_USER;

SELECT 'Cortex Analyst semantic view and Search services created.' AS STATUS;
