-- =============================================================================
-- MEDALLION ARCHITECTURE: GOLD Layer
-- =============================================================================
-- Part 1: Materialized Views — pre-computed consumption views for BI and AI
-- Part 2: Cortex Analyst — Semantic View for natural-language-to-SQL
--         Cortex Search Services — hybrid keyword+vector search
-- Part 3: Cortex Agent — orchestrates Analyst + Search tools
--         MCP Server — exposes Cortex tools to external clients
--         Snowflake Intelligence — publishes agent to Snowsight chat UI
--
-- Prerequisites: Run all Silver layer files first.
-- =============================================================================

-- =============================================================================
-- PART 1: MATERIALIZED VIEWS
-- =============================================================================
-- Pre-computed, low-latency consumption views for BI, dashboards, and Cortex AI.
-- Built on top of Silver dynamic tables — refreshes automatically when upstream
-- dynamic tables change.

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1a. TOP CUSTOMERS BY LIFETIME VALUE
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
-- 1b. MONTHLY KPI SUMMARY
-- =============================================================================

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
-- 1c. PRODUCT HEALTH
-- =============================================================================

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

-- Verification
SHOW MATERIALIZED VIEWS IN SCHEMA GOLD;

SELECT * FROM GOLD.MV_TOP_CUSTOMERS  LIMIT 5;
SELECT * FROM GOLD.MV_MONTHLY_KPI    ORDER BY MONTH DESC LIMIT 10;
SELECT * FROM GOLD.MV_PRODUCT_HEALTH ORDER BY TOTAL_REVENUE DESC LIMIT 10;

SELECT 'Gold materialized views created.' AS STATUS;

-- =============================================================================
-- PART 2: CORTEX ANALYST AND SEARCH SERVICES
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 2a. SEMANTIC VIEW — Sales & Customer Analytics
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
-- 2b. PRODUCT REVIEWS SEARCH
-- Semantic search over review text — find by meaning, not just keywords.
-- Example: "reviews about battery life", "customers who mentioned overheating"
-- =============================================================================

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

-- =============================================================================
-- 2c. SUPPORT TICKET SEARCH
-- Semantic search over ticket text — find similar past issues.
-- Example: "network connectivity timeout errors", "billing overcharge"
-- =============================================================================

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

-- =============================================================================
-- PART 3: CORTEX AGENT, MCP SERVER, AND SNOWFLAKE INTELLIGENCE
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 3a. CORTEX AGENT
-- =============================================================================
-- Multi-tool agent that automatically decides which tool to use based on the
-- question, then synthesizes results into a natural language response.

CREATE OR REPLACE AGENT AGENTS.SALES_SUPPORT_AGENT
  COMMENT = 'Multi-tool agent for sales analytics, product reviews, and support ticket resolution'
  PROFILE = '{"display_name": "Sales & Support Assistant", "color": "blue"}'
  FROM SPECIFICATION
  $$
  models:
    orchestration: claude-4-sonnet

  orchestration:
    budget:
      seconds: 60
      tokens: 16000

  instructions:
    system: >
      You are an intelligent assistant for a technology company. You help
      business users analyze sales data, understand product feedback, and
      resolve support issues. Always provide clear, actionable answers
      with relevant numbers and context.

    orchestration: >
      Route questions as follows:
      - For revenue, sales, orders, customer metrics, or product performance
        questions, use the SalesAnalyst tool to query structured data.
      - For questions about product feedback, customer opinions, or review
        sentiment, use the ReviewSearch tool to find relevant reviews.
      - For support ticket lookup, troubleshooting, or finding similar past
        issues, use the TicketSearch tool to search ticket history.
      - If a question spans multiple areas, use multiple tools and combine
        the insights in your response.

    response: >
      Be concise but thorough. Include specific numbers, percentages, and
      comparisons where relevant. If data comes from a search, cite the
      source (e.g., ticket ID or review). Format tables when presenting
      multiple data points.

    sample_questions:
      - question: "What was our total revenue last quarter by region?"
        answer: "I'll query the sales data to get revenue breakdown by region for last quarter."
      - question: "What are customers saying about our Electronics products?"
        answer: "I'll search product reviews to find feedback about Electronics products."
      - question: "Find support tickets about billing issues"
        answer: "I'll search our ticket history for billing-related issues."
      - question: "Which product category has the most negative reviews?"
        answer: "I'll combine review search with sales data to identify categories with negative feedback."

  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "SalesAnalyst"
        description: >
          Queries structured sales, order, customer, and product data.
          Use for questions about revenue, order counts, average order
          values, customer segments, product performance, regional
          breakdowns, and time-based trends.

    - tool_spec:
        type: "cortex_search"
        name: "ReviewSearch"
        description: >
          Searches product reviews using semantic search. Use for finding
          customer feedback, sentiment about specific products or features,
          reviews mentioning particular topics like battery life or ease
          of use. Can filter by rating, category, brand, and sentiment.

    - tool_spec:
        type: "cortex_search"
        name: "TicketSearch"
        description: >
          Searches support tickets using semantic search. Use for finding
          similar past issues, tickets about specific problems, resolution
          patterns, or ticket status. Can filter by priority, category,
          status, and customer segment.

  tool_resources:
    SalesAnalyst:
      semantic_view: "MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_ANALYTICS_SV"
      execution_environment:
        type: "warehouse"
        warehouse: "DEMO_CORTEX_WH"

    ReviewSearch:
      name: "MSFT_SNOWFLAKE_DEMO.AGENTS.PRODUCT_REVIEW_SEARCH"
      max_results: "10"
      title_column: "PRODUCT_NAME"

    TicketSearch:
      name: "MSFT_SNOWFLAKE_DEMO.AGENTS.SUPPORT_TICKET_SEARCH"
      max_results: "10"
      title_column: "TICKET_SUBJECT"
  $$;

SHOW AGENTS IN SCHEMA AGENTS;
DESCRIBE AGENT AGENTS.SALES_SUPPORT_AGENT;

-- Test: Structured data (routes to Cortex Analyst)
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What is the total revenue by region? Show me a breakdown.'
) AS RESPONSE;

-- Test: Review search
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What are customers saying about battery life in their reviews?'
) AS RESPONSE;

-- Test: Multi-tool question
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'How many support tickets do we have for Electronics products, and what is the average rating for Electronics in reviews?'
) AS RESPONSE;

-- Parse agent response (extract text from JSON):
SELECT
    RESPONSE:content[0]:text::VARCHAR AS ANSWER,
    RESPONSE:citations               AS CITATIONS,
    RESPONSE:request_id::VARCHAR     AS REQUEST_ID
FROM (
    SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
        'What was our average order value last month?'
    ) AS RESPONSE
);

GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_ANALYST;
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_AGENT_USER;

SELECT 'Cortex Agent created.' AS STATUS;

-- =============================================================================
-- 3b. MCP SERVER
-- =============================================================================
-- Snowflake-managed MCP server that exposes all Cortex tools to external
-- MCP clients (Cursor, Claude Desktop, Microsoft AI Foundry, etc.).

CREATE OR REPLACE MCP SERVER AGENTS.DEMO_MCP_SERVER
  FROM SPECIFICATION $$
    tools:
      - name: "sales-analyst"
        type: "CORTEX_ANALYST_MESSAGE"
        identifier: "MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_ANALYTICS_SV"
        description: "Query structured sales, order, customer, and product data using natural language. Converts questions to SQL via semantic view."
        title: "Sales Analytics"

      - name: "product-review-search"
        type: "CORTEX_SEARCH_SERVICE_QUERY"
        identifier: "MSFT_SNOWFLAKE_DEMO.AGENTS.PRODUCT_REVIEW_SEARCH"
        description: "Semantic search over product reviews. Find customer feedback about products, features, quality, and sentiment."
        title: "Product Review Search"

      - name: "support-ticket-search"
        type: "CORTEX_SEARCH_SERVICE_QUERY"
        identifier: "MSFT_SNOWFLAKE_DEMO.AGENTS.SUPPORT_TICKET_SEARCH"
        description: "Semantic search over support tickets. Find similar issues, troubleshooting patterns, and resolution history."
        title: "Support Ticket Search"

      - name: "sales-support-agent"
        type: "CORTEX_AGENT_RUN"
        identifier: "MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT"
        description: "Orchestrated agent that combines structured data analysis, review search, and ticket search to answer complex business questions."
        title: "Sales & Support Agent"

      - name: "sql-executor"
        type: "SYSTEM_EXECUTE_SQL"
        description: "Execute SQL queries directly against the Snowflake database for ad-hoc analysis."
        title: "SQL Execution"
  $$;

SHOW MCP SERVERS IN SCHEMA AGENTS;
DESCRIBE MCP SERVER AGENTS.DEMO_MCP_SERVER;

GRANT USAGE ON MCP SERVER AGENTS.DEMO_MCP_SERVER TO ROLE DEMO_ANALYST;
GRANT USAGE ON MCP SERVER AGENTS.DEMO_MCP_SERVER TO ROLE DEMO_AGENT_USER;

-- MCP Server URL:
--   https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse
--
-- Cursor (.cursor/mcp.json) and Claude Desktop (claude_desktop_config.json):
-- {
--   "mcpServers": {
--     "snowflake-demo": {
--       "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
--       "headers": { "Authorization": "Bearer <YOUR_PAT_TOKEN>" }
--     }
--   }
-- }
--
-- Generate a PAT: Snowsight -> profile icon -> Preferences -> Authentication -> New Token
-- Same URL format works for Microsoft AI Foundry MCP client connections.

SELECT 'MCP Server created.' AS STATUS;

-- =============================================================================
-- 3c. SNOWFLAKE INTELLIGENCE
-- =============================================================================
-- Publishes the agent to Snowsight AI & ML -> Agents for no-code chat access.

SHOW AGENTS IN SCHEMA AGENTS;

-- Update agent profile and sample questions for Intelligence UI:
ALTER AGENT AGENTS.SALES_SUPPORT_AGENT MODIFY LIVE VERSION SET SPECIFICATION =
$$
models:
  orchestration: claude-4-sonnet

orchestration:
  budget:
    seconds: 60
    tokens: 16000

instructions:
  system: >
    You are an intelligent assistant for a technology company. You help
    business users analyze sales data, understand product feedback, and
    resolve support issues. Always provide clear, actionable answers
    with relevant numbers and context.

  orchestration: >
    Route questions as follows:
    - For revenue, sales, orders, customer metrics, or product performance
      questions, use the SalesAnalyst tool to query structured data.
    - For questions about product feedback, customer opinions, or review
      sentiment, use the ReviewSearch tool to find relevant reviews.
    - For support ticket lookup, troubleshooting, or finding similar past
      issues, use the TicketSearch tool to search ticket history.
    - If a question spans multiple areas, use multiple tools and combine
      the insights in your response.

  response: >
    Be concise but thorough. Include specific numbers, percentages, and
    comparisons where relevant. If data comes from a search, cite the
    source (e.g., ticket ID or review). Format tables when presenting
    multiple data points.

  sample_questions:
    - question: "What was our total revenue last quarter by region?"
      answer: "I'll query the sales data to break down revenue by region for last quarter."
    - question: "What are customers saying about our Electronics products?"
      answer: "I'll search product reviews to find feedback about Electronics."
    - question: "Find critical support tickets about billing issues"
      answer: "I'll search our ticket history for critical billing-related issues."
    - question: "Which product category has the highest average order value?"
      answer: "I'll analyze order data to compare average order values across categories."
    - question: "Show me the trend in monthly revenue for the past year"
      answer: "I'll query monthly revenue data and show the trend over time."
    - question: "Are there any recent complaints about product quality?"
      answer: "I'll search recent reviews for quality-related feedback and complaints."

tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "SalesAnalyst"
      description: >
        Queries structured sales, order, customer, and product data.

  - tool_spec:
      type: "cortex_search"
      name: "ReviewSearch"
      description: >
        Searches product reviews using semantic search.

  - tool_spec:
      type: "cortex_search"
      name: "TicketSearch"
      description: >
        Searches support tickets using semantic search.

  - tool_spec:
      type: "data_to_chart"
      name: "data_to_chart"
      description: >
        Generates visualizations and charts from data.

tool_resources:
  SalesAnalyst:
    semantic_view: "MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_ANALYTICS_SV"
    execution_environment:
      type: "warehouse"
      warehouse: "DEMO_CORTEX_WH"

  ReviewSearch:
    name: "MSFT_SNOWFLAKE_DEMO.AGENTS.PRODUCT_REVIEW_SEARCH"
    max_results: "10"
    title_column: "PRODUCT_NAME"

  TicketSearch:
    name: "MSFT_SNOWFLAKE_DEMO.AGENTS.SUPPORT_TICKET_SEARCH"
    max_results: "10"
    title_column: "TICKET_SUBJECT"
$$;

-- Grant access for Intelligence users
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT              TO ROLE DEMO_ANALYST;
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT              TO ROLE DEMO_AGENT_USER;
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV      TO ROLE DEMO_ANALYST;
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV      TO ROLE DEMO_AGENT_USER;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_AGENT_USER;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_AGENT_USER;

-- Test via Intelligence
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'Show me total revenue and order count by customer segment for the current year.'
) AS RESPONSE;

-- Full inventory check
SHOW SEMANTIC VIEWS         IN SCHEMA AGENTS;
SHOW CORTEX SEARCH SERVICES IN SCHEMA AGENTS;
SHOW AGENTS                 IN SCHEMA AGENTS;
SHOW MCP SERVERS            IN SCHEMA AGENTS;

-- Access via Snowsight:
--   AI & ML -> Agents -> Sales & Support Assistant

SELECT 'Gold layer complete — materialized views, Cortex Analyst, Search, Agent, MCP Server, and Intelligence setup done.' AS STATUS;
