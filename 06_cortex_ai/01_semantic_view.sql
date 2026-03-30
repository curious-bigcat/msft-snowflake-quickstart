-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Semantic View for Cortex Analyst
-- =============================================================================
-- Creates a Semantic View that maps business concepts to physical tables,
-- enabling natural-language-to-SQL via Cortex Analyst.
--
-- The semantic view covers:
--   - Sales/revenue analysis (orders, items, products)
--   - Customer analytics (segments, lifetime value, geography)
--   - Product performance (reviews, ratings, revenue)
--
-- Prerequisites: Run phases 01-04 (setup through processing).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 1. SEMANTIC VIEW — Sales & Customer Analytics
-- =============================================================================
-- This semantic view connects orders, customers, products, and order items
-- to enable natural language questions like:
--   "What was total revenue by region last quarter?"
--   "Who are our top 10 customers by lifetime value?"
--   "Which product category has the highest average order value?"

CREATE OR REPLACE SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV

  TABLES (
    customers AS MSFT_SNOWFLAKE_DEMO.RAW.CUSTOMERS
      PRIMARY KEY (CUSTOMER_ID)
      WITH SYNONYMS = ('customer', 'buyer', 'client')
      COMMENT = 'Customer master data with segment and geography info',

    products AS MSFT_SNOWFLAKE_DEMO.RAW.PRODUCTS
      PRIMARY KEY (PRODUCT_ID)
      WITH SYNONYMS = ('product', 'item', 'SKU')
      COMMENT = 'Product catalog with categories, pricing, and brands',

    orders AS MSFT_SNOWFLAKE_DEMO.RAW.ORDERS
      PRIMARY KEY (ORDER_ID)
      WITH SYNONYMS = ('order', 'sale', 'transaction', 'purchase')
      COMMENT = 'Sales orders from multiple channels and sources',

    order_items AS MSFT_SNOWFLAKE_DEMO.RAW.ORDER_ITEMS
      PRIMARY KEY (ORDER_ITEM_ID)
      WITH SYNONYMS = ('line item', 'order line', 'order detail')
      COMMENT = 'Individual line items within each order',

    reviews AS MSFT_SNOWFLAKE_DEMO.RAW.PRODUCT_REVIEWS
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
    -- Order facts
    orders.net_amount AS TOTAL_AMOUNT - DISCOUNT_AMOUNT + SHIPPING_AMOUNT
      COMMENT = 'Net order amount after discount plus shipping',
    order_items.line_total_amount AS LINE_TOTAL
      COMMENT = 'Line item total amount (quantity * unit price - discount)',

    -- Product facts
    products.margin AS UNIT_PRICE - COST_PRICE
      COMMENT = 'Per-unit profit margin',

    -- Review facts
    reviews.review_rating AS RATING
      COMMENT = 'Review star rating from 1.0 to 5.0'
  )

  DIMENSIONS (
    -- Customer dimensions
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

    -- Product dimensions
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

    -- Order dimensions
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
    -- Revenue metrics
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

    -- Count metrics
    orders.order_count AS COUNT(ORDER_ID)
      WITH SYNONYMS = ('number of orders', 'total orders')
      COMMENT = 'Total number of orders',
    orders.unique_customers AS COUNT(DISTINCT CUSTOMER_ID)
      COMMENT = 'Number of distinct customers who placed orders',
    orders.cancelled_orders AS COUNT_IF(ORDER_STATUS = 'Cancelled')
      COMMENT = 'Number of cancelled orders',

    -- Product metrics
    order_items.total_units_sold AS SUM(QUANTITY)
      WITH SYNONYMS = ('units sold', 'quantity sold')
      COMMENT = 'Total units sold across all orders',
    order_items.total_line_revenue AS SUM(LINE_TOTAL)
      COMMENT = 'Total revenue from line items',

    -- Customer metrics
    customers.customer_count AS COUNT(CUSTOMER_ID)
      WITH SYNONYMS = ('number of customers')
      COMMENT = 'Total number of customers',

    -- Review metrics
    reviews.average_rating AS AVG(RATING)
      WITH SYNONYMS = ('avg rating', 'avg stars')
      COMMENT = 'Average product review rating',
    reviews.review_count AS COUNT(REVIEW_ID)
      WITH SYNONYMS = ('number of reviews')
      COMMENT = 'Total number of product reviews'
  )

  COMMENT = 'Semantic view for sales, customer, and product analytics — powers Cortex Analyst'
  AI_SQL_GENERATION 'Use this semantic view for questions about sales, revenue, orders, customers, products, and reviews.'
;

-- =============================================================================
-- 2. VERIFY SEMANTIC VIEW
-- =============================================================================

SHOW SEMANTIC VIEWS IN SCHEMA AGENTS;

DESCRIBE SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;

-- Show all dimensions
SHOW SEMANTIC DIMENSIONS IN SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;

-- Show all metrics
SHOW SEMANTIC METRICS IN SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV;

-- =============================================================================
-- 3. TEST — Query the Semantic View
-- =============================================================================

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

-- Test: Revenue by product category
SELECT * FROM SEMANTIC_VIEW(
    AGENTS.SALES_ANALYTICS_SV
    METRICS order_items.total_line_revenue, order_items.total_units_sold
    DIMENSIONS products.category
)
ORDER BY total_line_revenue DESC;

-- Test: Top customer segments by revenue
SELECT * FROM SEMANTIC_VIEW(
    AGENTS.SALES_ANALYTICS_SV
    METRICS orders.total_revenue, orders.unique_customers, orders.average_order_value
    DIMENSIONS customers.customer_segment
)
ORDER BY total_revenue DESC;

-- =============================================================================
-- 4. TEST — Cortex Analyst (natural language to SQL)
-- =============================================================================

-- Test Cortex Analyst with a natural language question
-- This uses the semantic view to generate SQL from plain English
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'claude-3-5-sonnet',
    'Given the semantic view AGENTS.SALES_ANALYTICS_SV with metrics total_revenue, order_count, average_order_value and dimensions region, channel, order_month — write a SQL query to find total revenue and order count by region and channel for 2024.'
) AS GENERATED_SQL;

-- =============================================================================
-- 5. GRANT ACCESS
-- =============================================================================

-- Grant SELECT on the semantic view to analyst and agent roles
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_ANALYST;
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_AGENT_USER;

SELECT 'Semantic view created and tested.' AS STATUS;
