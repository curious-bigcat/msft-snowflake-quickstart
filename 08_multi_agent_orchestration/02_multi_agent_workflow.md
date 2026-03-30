# Multi-Agent Workflow Testing Guide

Step-by-step guide to test multi-agent orchestration workflows using Snowflake Intelligence (Snowsight), Azure AI Foundry Playground, and MCP client integrations — all through UI, no CLI needed.

---

## Overview of Workflow Patterns

| Pattern | Description | Where to Test |
|---|---|---|
| **Sequential** | Agent A → Agent B → Synthesizer | Foundry Playground (manual chaining) |
| **Parallel** | Multiple agents answer same question, results compared | Side-by-side in Foundry + Snowsight |
| **Router** | Orchestrator routes to the best agent | Foundry Orchestrator Agent |
| **Cross-Platform** | Fabric targets vs Snowflake actuals | Foundry + Snowflake Intelligence |

---

## Workflow 1: Test Snowflake Cortex Agent (Snowflake Intelligence)

### Open Snowflake Intelligence

1. Log in to **Snowsight** (`https://<org>-<account>.snowflakecomputing.com`)
2. Navigate to **AI & ML** → **Agents** in the left nav
3. Find **Sales & Support Assistant** and click to open the chat

### Test Questions — Structured Data (Cortex Analyst)

Type each question in the chat and observe how the agent routes to the correct tool:

```
What is the total revenue by region?
```
> Expected: Agent uses Cortex Analyst → generates SQL → returns revenue table

```
What are the top 5 product categories by units sold?
```
> Expected: Agent queries order items + products → returns ranked list

```
Show me monthly revenue trend for the past year
```
> Expected: Agent generates time-series query → may include a chart

### Test Questions — Unstructured Search (Cortex Search)

```
What are customers saying about battery life in their reviews?
```
> Expected: Agent uses Product Review Search → returns relevant reviews

```
Find support tickets about software crashes
```
> Expected: Agent uses Support Ticket Search → returns matching tickets

### Test Questions — Multi-Tool (Agent routes to multiple tools)

```
How many support tickets do we have for Electronics products, and what is the average rating for Electronics in reviews?
```
> Expected: Agent uses both Analyst (ticket count) and Search (review ratings) → synthesizes

```
Give me a complete overview of our Software category: sales, customer feedback, and support issues.
```
> Expected: Agent chains Analyst + both Search services → comprehensive report

---

## Workflow 2: Test Foundry Agents (Azure AI Foundry Playground)

### Open Agent Playground

1. Go to [Azure AI Foundry](https://ai.azure.com) → your project
2. Navigate to **Build** → **Agents**
3. Click on an agent to open its **Playground** (chat interface)

### Test Data Analyst Agent

Open the **Snowflake Data Analyst** agent playground:

```
What is the total revenue by region? Summarize the top 3 regions.
```
> Observe: Agent calls `sales-analyst` MCP tool → Snowflake returns data → agent formats

```
How many unique customers placed orders in each channel?
```
> Observe: Agent calls `sales-analyst` → returns channel breakdown

```
Run this SQL: SELECT REGION, COUNT(*) as ORDER_COUNT FROM RAW.ORDERS GROUP BY REGION
```
> Observe: Agent uses `sql-executor` tool for direct SQL

### Test Customer Insights Agent

Open the **Customer Insights Agent** playground:

```
What are the main themes in negative product reviews?
```
> Observe: Agent calls `product-review-search` → summarizes patterns

```
Find support tickets related to billing disputes for Enterprise customers
```
> Observe: Agent calls `support-ticket-search` with filter → returns matches

### Test Fabric Data Agent

Open the **Fabric Data Agent** playground:

```
Show me the regional sales targets for 2024
```
> Observe: Agent uses Code Interpreter to analyze Fabric data

---

## Workflow 3: Sequential Pipeline (Manual Chaining)

Simulate a sequential pipeline by passing output from one agent as context to the next.

### Step 1: Query Data Analyst

Open **Snowflake Data Analyst** and ask:
```
What is the revenue and order count by product category? Include average order value.
```
**Copy the agent's response** (select all text in the response).

### Step 2: Feed to Customer Insights

Open **Customer Insights Agent** and paste:
```
Based on this data analysis, find related customer feedback for the top categories:

[PASTE DATA ANALYST RESPONSE HERE]

What do reviews say about the top-performing and worst-performing categories?
```

**Copy this agent's response** too.

### Step 3: Synthesize with Orchestrator

Open **Business Intelligence Orchestrator** (or create a new chat with any agent) and paste:
```
Create an executive summary combining these findings:

DATA ANALYSIS:
[PASTE DATA ANALYST RESPONSE]

CUSTOMER INSIGHTS:
[PASTE CUSTOMER INSIGHTS RESPONSE]

Highlight key metrics, customer sentiment trends, and actionable recommendations.
```

> Result: A synthesized executive report combining structured data with customer feedback.

---

## Workflow 4: Parallel Comparison (Side-by-Side)

Open two browser tabs to run agents in parallel and compare results.

### Tab 1: Snowflake Intelligence

Ask the Cortex Agent in Snowsight:
```
Give me a complete overview of our Electronics category: revenue, top products, and customer sentiment.
```

### Tab 2: Azure AI Foundry

Ask the **Business Intelligence Orchestrator** in Foundry:
```
Give me a complete overview of our Electronics category: revenue, top products, and customer sentiment.
```

### Compare

| Aspect | Snowflake Cortex Agent | Foundry Orchestrator |
|---|---|---|
| Data source | Direct Snowflake access | Via MCP to Snowflake |
| Response time | Faster (no network hop) | Slightly slower (MCP roundtrip) |
| Tool routing | Automatic (built-in orchestration) | Automatic (LLM-based routing) |
| Visualization | May include charts (data_to_chart) | Text-based (unless Code Interpreter) |

---

## Workflow 5: Cross-Platform Comparison (Fabric vs Snowflake)

### Step 1: Get Targets from Fabric

Open **Fabric Data Agent** in Foundry:
```
What are the regional sales targets and budgets for 2024? Show target_revenue by region.
```
**Copy the response.**

### Step 2: Get Actuals from Snowflake

Open **Snowflake Intelligence** (Cortex Agent):
```
What is the total net revenue by region for 2024?
```
**Copy the response.**

### Step 3: Compare

Open **Business Intelligence Orchestrator** in Foundry:
```
Compare these sales targets with actual results and calculate attainment percentage:

TARGETS (from Microsoft Fabric):
[PASTE FABRIC RESPONSE]

ACTUALS (from Snowflake):
[PASTE SNOWFLAKE RESPONSE]

Create a table showing: Region, Target Revenue, Actual Revenue, Attainment %, and status (Over/Under).
```

> Result: A cross-platform comparison table showing Fabric targets vs Snowflake actuals.

---

## Workflow 6: MCP Client Integration (Cursor / Claude Desktop / VS Code)

For a richer agent experience, connect an MCP client directly to the Snowflake MCP server.

### Option A: Cursor IDE

1. Open your project in Cursor
2. Create or edit `.cursor/mcp.json` at the project root:

```json
{
  "mcpServers": {
    "snowflake-demo": {
      "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
      "headers": {
        "Authorization": "Bearer <YOUR_SNOWFLAKE_PAT>"
      }
    }
  }
}
```

3. Go to **Cursor** → **Settings** → **Cursor Settings** → **Tools & MCP**
4. Verify **snowflake-demo** appears under Installed Servers
5. In Cursor chat, ask: "What is our total revenue by region?" — Cursor will invoke the MCP tools

### Option B: Claude Desktop

1. Edit the config file:
   - macOS: `~/Library/Application Support/Claude/claude_desktop_config.json`
   - Windows: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "snowflake-demo": {
      "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
      "headers": {
        "Authorization": "Bearer <YOUR_SNOWFLAKE_PAT>"
      }
    }
  }
}
```

2. Restart Claude Desktop
3. Verify the Snowflake tools appear in the tool list
4. Ask questions — Claude will use the MCP tools to query Snowflake

### Option C: VS Code (GitHub Copilot)

1. Open VS Code Settings (JSON)
2. Add MCP server configuration under `github.copilot.chat.mcpServers`
3. Use Copilot Chat to query Snowflake data via MCP

---

## Sample Test Matrix

Use this checklist to verify all workflows:

| # | Test | Platform | Tool Expected | Pass? |
|---|---|---|---|---|
| 1 | "Total revenue by region" | Snowflake Intelligence | Cortex Analyst | |
| 2 | "Reviews about battery life" | Snowflake Intelligence | Cortex Search | |
| 3 | "Revenue + reviews for Electronics" | Snowflake Intelligence | Both tools | |
| 4 | "Top 5 categories by revenue" | Foundry Data Analyst | sales-analyst (MCP) | |
| 5 | "Negative review themes" | Foundry Customer Insights | product-review-search (MCP) | |
| 6 | "Billing support tickets" | Foundry Customer Insights | support-ticket-search (MCP) | |
| 7 | "Sales targets for 2024" | Foundry Fabric Agent | Code Interpreter | |
| 8 | "Complete Electronics overview" | Foundry Orchestrator | Multiple MCP tools | |
| 9 | Sequential: Data → Insights → Report | Manual chaining | 3 agents in sequence | |
| 10 | Cross-platform: Fabric vs Snowflake | Foundry + Snowsight | Compare targets vs actuals | |
| 11 | MCP client (Cursor/Claude) | Local IDE | MCP tools | |

---

## Expected Outcomes

After completing all workflows, you have demonstrated:

1. **Snowflake Cortex Agent** answering questions via Snowflake Intelligence
2. **Foundry Agents** invoking Snowflake tools via MCP from Azure
3. **Sequential pipeline** where each agent builds on previous output
4. **Parallel comparison** of the same question across platforms
5. **Cross-platform analysis** combining Fabric targets with Snowflake actuals
6. **MCP client integration** enabling any compatible tool to access Snowflake AI
