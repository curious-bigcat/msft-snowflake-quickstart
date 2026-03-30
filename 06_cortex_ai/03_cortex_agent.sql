-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Cortex Agent
-- =============================================================================
-- Creates a Cortex Agent that orchestrates across multiple tools:
--   - Cortex Analyst (semantic view) for structured data queries
--   - Cortex Search (product reviews) for review insights
--   - Cortex Search (support tickets) for ticket lookup
--
-- The agent automatically decides which tool to use based on the question,
-- then synthesizes results into a natural language response.
--
-- Prerequisites: Run 01_semantic_view.sql and 02_cortex_search_service.sql.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 1. CREATE CORTEX AGENT
-- =============================================================================

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

-- =============================================================================
-- 2. VERIFY AGENT
-- =============================================================================

SHOW AGENTS IN SCHEMA AGENTS;

DESCRIBE AGENT AGENTS.SALES_SUPPORT_AGENT;

-- =============================================================================
-- 3. TEST — Structured Data Questions (Cortex Analyst)
-- =============================================================================

-- Test: Revenue by region
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What is the total revenue by region? Show me a breakdown.'
) AS RESPONSE;

-- Test: Top products
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What are the top 5 product categories by units sold?'
) AS RESPONSE;

-- =============================================================================
-- 4. TEST — Review Search Questions (Cortex Search)
-- =============================================================================

-- Test: Search reviews
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What are customers saying about battery life in their reviews?'
) AS RESPONSE;

-- Test: Negative feedback
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'Find negative reviews about Hardware products. What are the main complaints?'
) AS RESPONSE;

-- =============================================================================
-- 5. TEST — Support Ticket Questions (Cortex Search)
-- =============================================================================

-- Test: Find similar tickets
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'Find support tickets related to software crashes or application errors.'
) AS RESPONSE;

-- Test: Multi-tool question
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'How many support tickets do we have for Electronics products, and what is the average rating for Electronics in reviews?'
) AS RESPONSE;

-- =============================================================================
-- 6. PARSE AGENT RESPONSE (Python example for Notebooks)
-- =============================================================================

-- The DATA_AGENT_RUN function returns a JSON response.
-- Here's how to extract the text answer:
SELECT
    RESPONSE:content[0]:text::VARCHAR AS ANSWER,
    RESPONSE:citations AS CITATIONS,
    RESPONSE:request_id::VARCHAR AS REQUEST_ID
FROM (
    SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
        'What was our average order value last month?'
    ) AS RESPONSE
);

-- =============================================================================
-- 7. GRANT ACCESS
-- =============================================================================

GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_ANALYST;
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_AGENT_USER;

SELECT 'Cortex Agent created and tested.' AS STATUS;
