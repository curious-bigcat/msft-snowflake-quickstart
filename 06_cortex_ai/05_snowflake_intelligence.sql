-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Snowflake Intelligence
-- =============================================================================
-- Publishes the Cortex Agent to Snowflake Intelligence, making it accessible
-- as a conversational interface in Snowsight for business users.
--
-- Snowflake Intelligence provides:
--   - Chat-based UI for interacting with agents
--   - Role-based access control
--   - Conversation history and sharing
--   - No-code access to AI-powered analytics
--
-- Prerequisites: Run 01-04 in this folder (semantic view, search, agent, MCP).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 1. PUBLISH AGENT TO SNOWFLAKE INTELLIGENCE
-- =============================================================================
-- Snowflake Intelligence surfaces Cortex Agents in Snowsight's AI & ML section.
-- Once the agent is created (03_cortex_agent.sql), it automatically appears
-- in Snowflake Intelligence for users with the appropriate role.
--
-- The agent is already accessible at:
--   Snowsight → AI & ML → Agents → Sales & Support Assistant
--
-- Users with USAGE on the agent can start conversations directly.

-- Verify the agent is visible in Intelligence
SHOW AGENTS IN SCHEMA AGENTS;

-- =============================================================================
-- 2. CONFIGURE AGENT PROFILE FOR INTELLIGENCE UI
-- =============================================================================
-- The PROFILE attribute controls how the agent appears in Snowflake Intelligence.
-- We set this during CREATE AGENT, but can update it here:

ALTER AGENT AGENTS.SALES_SUPPORT_AGENT SET
  PROFILE = '{
    "display_name": "Sales & Support Assistant",
    "avatar": "business-icon.png",
    "color": "blue"
  }';

-- =============================================================================
-- 3. ADD SAMPLE QUESTIONS FOR INTELLIGENCE UI
-- =============================================================================
-- Sample questions appear as suggestions in the Intelligence chat interface.
-- They help users understand what the agent can do.

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

  - tool_spec:
      type: "data_to_chart"
      name: "data_to_chart"
      description: >
        Generates visualizations and charts from data. Use when users
        ask for visual representations of trends, comparisons, or
        distributions.

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
-- 4. ROLE-BASED ACCESS FOR INTELLIGENCE
-- =============================================================================
-- Different roles get different levels of access in Intelligence.

-- Analysts can use the agent to ask questions
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_ANALYST;

-- Agent users have full access including configuration
GRANT USAGE ON AGENT AGENTS.SALES_SUPPORT_AGENT TO ROLE DEMO_AGENT_USER;

-- Ensure underlying objects are accessible
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_ANALYST;
GRANT SELECT ON SEMANTIC VIEW AGENTS.SALES_ANALYTICS_SV TO ROLE DEMO_AGENT_USER;

GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_AGENT_USER;

GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_AGENT_USER;

-- =============================================================================
-- 5. TEST AGENT VIA DATA_AGENT_RUN
-- =============================================================================
-- These simulate what Intelligence users will experience:

-- Business question: Revenue analysis
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'Show me total revenue and order count by customer segment for the current year.'
) AS RESPONSE;

-- Product feedback question
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'What are the most common complaints in product reviews for the Software category?'
) AS RESPONSE;

-- Support question
SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
    'MSFT_SNOWFLAKE_DEMO.AGENTS.SALES_SUPPORT_AGENT',
    'Find the most recent critical support tickets and their resolution status.'
) AS RESPONSE;

-- =============================================================================
-- 6. INTELLIGENCE ACCESS INSTRUCTIONS
-- =============================================================================
-- To access the agent in Snowflake Intelligence:
--
-- 1. Log in to Snowsight (https://<org>-<account>.snowflakecomputing.com)
-- 2. Navigate to AI & ML → Agents
-- 3. You should see "Sales & Support Assistant" listed
-- 4. Click to open the chat interface
-- 5. Try one of the sample questions or ask your own
--
-- The agent will:
--   - Understand your question in natural language
--   - Route to the appropriate tool (Analyst, Search, or both)
--   - Return a synthesized answer with data
--   - Generate charts when visualization is requested

-- =============================================================================
-- 7. VERIFICATION
-- =============================================================================

-- Confirm all Cortex AI objects are created
SELECT 'Semantic Views' AS OBJECT_TYPE, COUNT(*) AS COUNT
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
UNION ALL
SELECT 'Cortex Search Services', COUNT(*)
FROM INFORMATION_SCHEMA.CORTEX_SEARCH_SERVICES
WHERE SCHEMA_NAME = 'AGENTS';

-- Full inventory check
SHOW SEMANTIC VIEWS IN SCHEMA AGENTS;
SHOW CORTEX SEARCH SERVICES IN SCHEMA AGENTS;
SHOW AGENTS IN SCHEMA AGENTS;
SHOW MCP SERVERS IN SCHEMA AGENTS;

SELECT 'Snowflake Intelligence setup complete. Agent is ready for use.' AS STATUS;
