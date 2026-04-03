-- =============================================================================
-- MEDALLION ARCHITECTURE: GOLD Layer — Cortex Agent, MCP Server, and Intelligence
-- =============================================================================
-- Part 1: Cortex Agent — orchestrates across Analyst + Search tools
-- Part 2: MCP Server — exposes Cortex tools to external clients
--         (Cursor, Claude Desktop, Microsoft AI Foundry)
-- Part 3: Snowflake Intelligence — publishes agent to Snowsight chat UI
--
-- Prerequisites: Run 03_cortex_analyst_and_search.sql first.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- PART 1: CORTEX AGENT
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
-- PART 2: MCP SERVER
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
-- PART 3: SNOWFLAKE INTELLIGENCE
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

SELECT 'Cortex Agent, MCP Server, and Snowflake Intelligence setup complete.' AS STATUS;
