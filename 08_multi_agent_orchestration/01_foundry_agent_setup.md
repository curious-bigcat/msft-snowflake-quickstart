# Microsoft Azure AI Foundry — Agent Setup Guide

Step-by-step guide to create three AI agents in the Azure AI Foundry Portal that connect to Snowflake via MCP for cross-platform multi-agent orchestration.

## Prerequisites

- Azure AI Foundry project with a deployed model (gpt-4o or gpt-4o-mini)
- Snowflake MCP server created (run `06_cortex_ai/04_mcp_server.sql` first)
- Snowflake Personal Access Token (PAT) for MCP authentication

---

## Step 1: Open Azure AI Foundry

1. Navigate to [Azure AI Foundry](https://ai.azure.com)
2. Select your project (or create one: **+ New project**)
3. Verify a model is deployed: **Models + Endpoints** in the left nav
   - If not deployed, click **+ Deploy model** → select `gpt-4o-mini` → **Deploy**
   - Note the **deployment name** (e.g., `gpt-4o-mini`)

---

## Step 2: Connect Snowflake MCP Server

Before creating agents, add the Snowflake MCP server as a connected resource.

1. In your Foundry project, go to **Management** → **Connected resources**
2. Click **+ New connection**
3. Select **MCP Server** (or **Custom** if MCP is not listed)
4. Configure:

| Field | Value |
|---|---|
| **Name** | `snowflake-mcp` |
| **Endpoint URL** | `https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse` |
| **Authentication** | Bearer Token |
| **Token** | Your Snowflake PAT (see note below) |

> **Getting a Snowflake PAT:** In Snowsight, click your profile icon → **Preferences** → **Authentication** → **Generate new Personal Access Token**. Copy the token immediately.

5. Click **Save**
6. Verify: The connection should show as **Connected** with 5 tools discovered:
   - `sales-analyst` (Cortex Analyst)
   - `product-review-search` (Cortex Search)
   - `support-ticket-search` (Cortex Search)
   - `sales-support-agent` (Cortex Agent)
   - `sql-executor` (SQL Execution)

---

## Step 3: Create Agent 1 — Snowflake Data Analyst

This agent queries structured sales/business data from Snowflake via the MCP server.

1. Go to **Build** → **Agents** in the left nav
2. Click **+ New agent**
3. Fill in:

| Field | Value |
|---|---|
| **Name** | `Snowflake Data Analyst` |
| **Model** | `gpt-4o-mini` (your deployment name) |

4. In the **Instructions** box, paste:

```
You are a data analyst agent that queries sales and business data from Snowflake.
You have access to a Snowflake MCP server with tools for:

- Sales Analytics: Query revenue, orders, customer metrics, and product performance
  using natural language (converted to SQL via Cortex Analyst semantic view).
- SQL Execution: Run direct SQL queries against Snowflake for ad-hoc analysis.

When answering questions:
1. Use the sales-analyst tool for business questions about revenue, orders, etc.
2. Use the sql-executor tool for custom queries not covered by the semantic view.
3. Always present data in clear tables or summaries.
4. Include trends and comparisons when relevant.
5. Mention the data source (Snowflake) in your responses.
```

5. Under **Tools**, click **+ Add tool**:
   - Select the `snowflake-mcp` connection
   - Enable these tools:
     - `sales-analyst`
     - `sql-executor`

6. Click **Create**

### Test Agent 1

In the agent playground chat, try these questions:
- "What is the total revenue by region?"
- "How many orders were placed last quarter?"
- "What are the top 5 product categories by units sold?"

---

## Step 4: Create Agent 2 — Customer Insights Agent

This agent searches unstructured data (reviews, tickets) via Cortex Search.

1. Click **+ New agent**
2. Fill in:

| Field | Value |
|---|---|
| **Name** | `Customer Insights Agent` |
| **Model** | `gpt-4o-mini` |

3. In the **Instructions** box, paste:

```
You are a customer insights agent that analyzes product reviews and support
tickets from Snowflake. You have access to:

- Product Review Search: Semantic search over customer reviews to find feedback
  about products, features, quality, and sentiment.
- Support Ticket Search: Semantic search over support tickets to find similar
  issues, troubleshooting patterns, and resolution history.

When answering questions:
1. Search relevant reviews or tickets based on the user's question.
2. Summarize patterns and themes you find.
3. Highlight sentiment trends (positive/negative).
4. Reference specific reviews or tickets by ID when citing evidence.
5. Suggest actionable insights based on the feedback patterns.
```

4. Under **Tools**, click **+ Add tool**:
   - Select the `snowflake-mcp` connection
   - Enable these tools:
     - `product-review-search`
     - `support-ticket-search`

5. Click **Create**

### Test Agent 2

In the agent playground chat, try:
- "What are customers saying about battery life?"
- "Find support tickets about billing issues"
- "What are the most common complaints in negative reviews?"

---

## Step 5: Create Agent 3 — Fabric Data Agent

This agent queries data from Microsoft Fabric for cross-platform analysis.

1. Click **+ New agent**
2. Fill in:

| Field | Value |
|---|---|
| **Name** | `Fabric Data Agent` |
| **Model** | `gpt-4o-mini` |

3. In the **Instructions** box, paste:

```
You are a data agent that queries Microsoft Fabric data. You have access to:

- Regional sales targets and budgets stored in Fabric Lakehouse
- Marketing campaign performance data
- Any other data available in the Fabric workspace

When answering questions:
1. Query the relevant Fabric tables for the requested data.
2. Compare Fabric data (targets/budgets) with Snowflake actuals when asked.
3. Present data clearly with tables and summaries.
4. Highlight variances between targets and actuals.
5. Provide recommendations based on the data analysis.
```

4. Under **Tools**, click **+ Add tool**:
   - Enable **Code Interpreter** (for data analysis)
   - Optionally connect to Fabric data via file upload or Fabric connection

5. Click **Create**

### Test Agent 3

In the agent playground chat, try:
- "Show me the regional sales targets for Q1 2025"
- "What was the marketing budget allocation by channel?"

---

## Step 6: Create Orchestrator Agent (Optional)

For multi-agent routing, create a master orchestrator agent.

1. Click **+ New agent**
2. Fill in:

| Field | Value |
|---|---|
| **Name** | `Business Intelligence Orchestrator` |
| **Model** | `gpt-4o-mini` |

3. In the **Instructions** box, paste:

```
You are an orchestrator that routes business questions to the right specialist agent.

Route questions as follows:
- Revenue, sales, orders, metrics, KPIs → Use the Snowflake Data Analyst tools
- Reviews, feedback, sentiment, complaints, tickets → Use the Customer Insights tools
- Targets, budgets, marketing campaigns → Use the Fabric Data tools
- Complex questions needing multiple data sources → Use the sales-support-agent tool
  (Snowflake Cortex Agent that orchestrates across structured and unstructured data)

Always synthesize results into clear, actionable business insights.
```

4. Under **Tools**, click **+ Add tool**:
   - Select the `snowflake-mcp` connection
   - Enable **all 5 tools** (sales-analyst, product-review-search, support-ticket-search, sales-support-agent, sql-executor)
   - Also enable **Code Interpreter** for data processing

5. Click **Create**

### Test Orchestrator

Try questions that span multiple domains:
- "Give me a complete overview of our Electronics category: sales, reviews, and support issues"
- "Compare our sales targets with actual revenue by region"

---

## Agent Summary

After completing all steps, you should have 4 agents in your Foundry project:

| Agent | Purpose | MCP Tools Used |
|---|---|---|
| Snowflake Data Analyst | Structured data queries | `sales-analyst`, `sql-executor` |
| Customer Insights Agent | Review & ticket search | `product-review-search`, `support-ticket-search` |
| Fabric Data Agent | Fabric Lakehouse data | Code Interpreter |
| Business Intelligence Orchestrator | Routes to best agent | All 5 MCP tools + Code Interpreter |

All agents connect to Snowflake through the Snowflake Managed MCP Server created in Phase 5.

---

## Troubleshooting

| Issue | Solution |
|---|---|
| MCP connection fails | Verify PAT is valid; check account URL uses hyphens not underscores |
| "Tool not found" error | Verify MCP server has all 5 tools: `DESCRIBE MCP SERVER AGENTS.DEMO_MCP_SERVER;` |
| Agent can't query data | Ensure the PAT user has USAGE on the MCP server and underlying objects |
| Model not available | Deploy gpt-4o-mini in **Models + Endpoints** |
| Agent times out | Increase timeout in agent settings; verify Snowflake warehouse is running |
