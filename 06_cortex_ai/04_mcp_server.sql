-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Snowflake Managed MCP Server
-- =============================================================================
-- Creates a Snowflake-managed MCP (Model Context Protocol) server that
-- exposes Cortex tools to external MCP clients:
--   - Cortex Analyst (via semantic view) for structured data queries
--   - Cortex Search (reviews & tickets) for semantic search
--   - Cortex Agent for orchestrated multi-tool access
--   - SQL execution tool for ad-hoc queries
--
-- External MCP clients (Cursor, Claude Desktop, Microsoft Foundry, etc.)
-- connect to this server to discover and invoke these tools.
--
-- Prerequisites: Run 01-03 in this folder (semantic view, search, agent).
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_CORTEX_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;
USE SCHEMA AGENTS;

-- =============================================================================
-- 1. CREATE MCP SERVER — Full Tool Suite
-- =============================================================================
-- This MCP server bundles all Cortex AI capabilities into a single endpoint
-- that any MCP-compatible client can connect to.

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

-- =============================================================================
-- 2. VERIFY MCP SERVER
-- =============================================================================

SHOW MCP SERVERS IN SCHEMA AGENTS;

DESCRIBE MCP SERVER AGENTS.DEMO_MCP_SERVER;

-- =============================================================================
-- 3. GRANT ACCESS
-- =============================================================================

-- Grant USAGE on MCP server to roles that need external client access
GRANT USAGE ON MCP SERVER AGENTS.DEMO_MCP_SERVER TO ROLE DEMO_ANALYST;
GRANT USAGE ON MCP SERVER AGENTS.DEMO_MCP_SERVER TO ROLE DEMO_AGENT_USER;

-- =============================================================================
-- 4. CLIENT CONFIGURATION GUIDE
-- =============================================================================
-- To connect an MCP client to this server, you need:
--   1. Your Snowflake account URL (e.g., https://<org>-<account>.snowflakecomputing.com)
--   2. A Personal Access Token (PAT) for authentication
--   3. The MCP server endpoint path
--
-- MCP Server URL format:
--   https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse
--
-- Note: Use hyphens (-) not underscores (_) in the hostname.

-- =============================================================================
-- 4a. Cursor IDE Configuration
-- =============================================================================
-- Create or edit .cursor/mcp.json in your project root:
--
-- {
--   "mcpServers": {
--     "snowflake-demo": {
--       "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
--       "headers": {
--         "Authorization": "Bearer <YOUR_PAT_TOKEN>"
--       }
--     }
--   }
-- }

-- =============================================================================
-- 4b. Claude Desktop Configuration
-- =============================================================================
-- Add to ~/Library/Application Support/Claude/claude_desktop_config.json (macOS)
-- or %APPDATA%\Claude\claude_desktop_config.json (Windows):
--
-- {
--   "mcpServers": {
--     "snowflake-demo": {
--       "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
--       "headers": {
--         "Authorization": "Bearer <YOUR_PAT_TOKEN>"
--       }
--     }
--   }
-- }

-- =============================================================================
-- 4c. Generate a Personal Access Token (PAT)
-- =============================================================================
-- Run the following in Snowsight or SnowSQL to create a PAT:
--
--   ALTER USER <your_username> ADD DELEGATED AUTHORIZATION
--     OF ROLE DEMO_ANALYST
--     TO SECURITY INTEGRATION SNOWSERVICES_INGRESS_OAUTH;
--
-- Or generate via Snowsight:
--   1. Click your profile icon → Preferences → Authentication
--   2. Generate new Personal Access Token
--   3. Copy the token and use in MCP client config

-- =============================================================================
-- 5. TEST — Verify MCP Server Connectivity (curl)
-- =============================================================================
-- Test the MCP server endpoint with curl:
--
-- curl -X POST \
--   "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse" \
--   -H "Authorization: Bearer <YOUR_PAT_TOKEN>" \
--   -H "Content-Type: application/json" \
--   -d '{
--     "jsonrpc": "2.0",
--     "id": 1,
--     "method": "tools/list"
--   }'
--
-- Expected: JSON response listing all 5 configured tools.

-- =============================================================================
-- 6. MICROSOFT FOUNDRY MCP CLIENT CONFIGURATION
-- =============================================================================
-- When connecting from Microsoft Foundry agents (Phase 8), use:
--
-- MCP Server URL:
--   https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse
--
-- Authentication: Bearer token (PAT)
--
-- This enables Foundry agents to invoke Snowflake Cortex tools via MCP,
-- creating a cross-platform multi-agent orchestration pattern.

SELECT 'MCP Server created. Configure your MCP client using the instructions above.' AS STATUS;
