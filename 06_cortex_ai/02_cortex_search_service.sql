-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Cortex Search Service
-- =============================================================================
-- Creates Cortex Search Services for semantic search over unstructured text:
--   1. Product Reviews search — find reviews by meaning, not just keywords
--   2. Support Tickets search — find similar tickets for troubleshooting
--
-- Cortex Search builds a hybrid (keyword + vector) search index and
-- keeps it fresh via automatic incremental refresh.
--
-- Prerequisites: Run phases 01-04 (setup through processing).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 1. CORTEX SEARCH SERVICE — Product Reviews
-- =============================================================================
-- Enables semantic search over product review text.
-- Users can ask things like "reviews about battery life" or
-- "customers who mentioned overheating" and get relevant results
-- even without exact keyword matches.

CREATE OR REPLACE CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH
  ON REVIEW_TEXT
  ATTRIBUTES PRODUCT_NAME, CATEGORY, BRAND, RATING, SENTIMENT_LABEL
  WAREHOUSE = DEMO_CORTEX_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search over product reviews — used by Cortex Agent'
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
        -- Include sentiment if processed reviews exist
        COALESCE(pr.SENTIMENT_LABEL, 'unknown') AS SENTIMENT_LABEL,
        COALESCE(pr.SENTIMENT_SCORE, 0) AS SENTIMENT_SCORE
    FROM RAW.PRODUCT_REVIEWS r
    LEFT JOIN RAW.PRODUCTS p ON r.PRODUCT_ID = p.PRODUCT_ID
    LEFT JOIN ANALYTICS.PROCESSED_REVIEWS pr ON r.REVIEW_ID = pr.REVIEW_ID
);

-- =============================================================================
-- 2. CORTEX SEARCH SERVICE — Support Tickets
-- =============================================================================
-- Enables semantic search over support ticket descriptions.
-- Agents can find similar past tickets to help resolve new issues,
-- search by symptom description, or find tickets by topic.

CREATE OR REPLACE CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH
  ON TICKET_CONTENT
  ATTRIBUTES CATEGORY, PRIORITY, STATUS, CUSTOMER_SEGMENT, PRODUCT_NAME
  WAREHOUSE = DEMO_CORTEX_WH
  TARGET_LAG = '1 hour'
  COMMENT = 'Semantic search over support tickets — used by Cortex Agent'
AS (
    SELECT
        t.TICKET_ID,
        -- Combine subject and description for richer search
        t.TICKET_SUBJECT || '. ' || COALESCE(t.TICKET_DESCRIPTION, '') AS TICKET_CONTENT,
        t.TICKET_SUBJECT,
        t.CATEGORY,
        t.PRIORITY,
        t.STATUS,
        t.RESOLUTION_TIME_HOURS,
        t.SATISFACTION_SCORE,
        t.CREATED_AT,
        c.CUSTOMER_SEGMENT,
        c.STATE AS CUSTOMER_STATE,
        COALESCE(p.PRODUCT_NAME, 'N/A') AS PRODUCT_NAME,
        COALESCE(p.CATEGORY, 'N/A') AS PRODUCT_CATEGORY
    FROM RAW.SUPPORT_TICKETS t
    LEFT JOIN RAW.CUSTOMERS c ON t.CUSTOMER_ID = c.CUSTOMER_ID
    LEFT JOIN RAW.PRODUCTS p ON t.PRODUCT_ID = p.PRODUCT_ID
);

-- =============================================================================
-- 3. VERIFY SEARCH SERVICES
-- =============================================================================

SHOW CORTEX SEARCH SERVICES IN SCHEMA AGENTS;

-- Check service status (may take a minute to build initial index)
DESCRIBE CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH;
DESCRIBE CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH;

-- =============================================================================
-- 4. TEST — Product Review Search
-- =============================================================================

-- Search for reviews about battery life
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

-- Search for positive reviews about software
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.PRODUCT_REVIEW_SEARCH',
        '{
            "query": "excellent software easy to use",
            "columns": ["REVIEW_TEXT", "PRODUCT_NAME", "CATEGORY", "RATING"],
            "filter": {"@gte": {"RATING": 4}},
            "limit": 5
        }'
    )
) AS RESULTS;

-- =============================================================================
-- 5. TEST — Support Ticket Search
-- =============================================================================

-- Search for tickets about connectivity problems
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

-- Search for critical tickets in billing category
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MSFT_SNOWFLAKE_DEMO.AGENTS.SUPPORT_TICKET_SEARCH',
        '{
            "query": "billing overcharge incorrect invoice",
            "columns": ["TICKET_SUBJECT", "CATEGORY", "PRIORITY", "CUSTOMER_SEGMENT"],
            "filter": {"@eq": {"PRIORITY": "Critical"}},
            "limit": 5
        }'
    )
) AS RESULTS;

-- =============================================================================
-- 6. GRANT ACCESS
-- =============================================================================

GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.PRODUCT_REVIEW_SEARCH TO ROLE DEMO_AGENT_USER;

GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_ANALYST;
GRANT USAGE ON CORTEX SEARCH SERVICE AGENTS.SUPPORT_TICKET_SEARCH TO ROLE DEMO_AGENT_USER;

SELECT 'Cortex Search services created and tested.' AS STATUS;
