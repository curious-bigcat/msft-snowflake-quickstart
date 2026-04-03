# Fabric + AI Foundry Integration Guide

Three-part guide for bidirectional Snowflake ↔ Microsoft Fabric data access and cross-platform multi-agent orchestration with Azure AI Foundry.

- **Part 1** — Fabric Setup: workspace, lakehouse, consent flow, OneLake shortcuts
- **Part 2** — AI Foundry Agent Setup: create Foundry agents connected to Snowflake via MCP
- **Part 3** — Multi-Agent Workflow Testing: end-to-end test patterns across platforms

---

## Part 1: Fabric Setup

### Architecture Overview

```
┌─────────────────────┐                    ┌─────────────────────┐
│     SNOWFLAKE       │                    │   MICROSOFT FABRIC  │
│                     │                    │                     │
│  Iceberg Tables     │ ──── OneLake ────▶ │  Lakehouse Tables   │
│  (write to Files/)  │                    │  (via shortcut)     │
│                     │                    │                     │
│  Iceberg Tables     │ ◀── OneLake ─────  │  Delta/Iceberg      │
│  (METADATA_FILE_PATH│                    │  Tables (Tables/)   │
└─────────────────────┘                    └─────────────────────┘
                          Apache Iceberg
                         (Open Table Format)
```

Both platforms read/write the same Parquet + Iceberg metadata files in OneLake. No data copying occurs.

### Prerequisites

| Requirement | Details |
|---|---|
| Microsoft Fabric capacity | F2 or higher (trial works for testing) |
| Azure region match | Fabric capacity **must be in the same Azure region** as your Snowflake account |
| Azure subscription | With Microsoft Entra ID |
| Snowflake account | Enterprise edition or higher |
| Snowflake role | ACCOUNTADMIN (for integration setup) |

> **Region check:** In Snowflake, go to the bottom-left account menu — it shows the cloud provider and region (e.g., `Azure East US 2`). Your Fabric capacity must match.

### Step 1: Create a Fabric Workspace

1. Go to [Microsoft Fabric](https://app.fabric.microsoft.com)
2. Click **Workspaces** → **New workspace**
3. Name: `snowflake-demo-workspace`
4. Select a Fabric capacity (F2+ or Trial) — confirm it matches your Snowflake Azure region
5. Click **Create**

**Record the Workspace ID:** workspace Settings → About → copy the GUID from the URL
(`https://app.fabric.microsoft.com/groups/<workspace_id>/...`)

### Step 2: Create a Lakehouse

1. In the workspace, click **New** → **Lakehouse**
2. Name: `demo_lakehouse`
3. Click **Create**

**Record the Lakehouse ID:** open lakehouse → "..." → Settings → copy the GUID from the URL

### Step 3: Grant Snowflake Service Principal Access

1. In Snowflake (Snowsight), run: `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;`
2. Copy `AZURE_CONSENT_URL` and `AZURE_MULTI_TENANT_APP_NAME` from the output
3. Open `AZURE_CONSENT_URL` in a browser → click **Accept**
4. In Fabric Portal, open `snowflake-demo-workspace`
5. Click **Manage access** → **Add people or groups**
6. Paste `AZURE_MULTI_TENANT_APP_NAME` → assign **Contributor** role → **Add**

> `Contributor` lets Snowflake read and write files in any lakehouse in this workspace. The consent URL registers the Snowflake multi-tenant app in your Entra ID tenant — this is a standard OAuth app consent flow.

### Step 4: Create Sample Data in Fabric (PySpark Notebook)

In Fabric: **New** → **Notebook** → attach to `demo_lakehouse`, then run:

```python
from pyspark.sql.types import *
import random

# Regional sales targets
schema = StructType([
    StructField("region", StringType(), False),
    StructField("quarter", StringType(), False),
    StructField("year", IntegerType(), False),
    StructField("target_revenue", DoubleType(), False),
    StructField("target_customers", IntegerType(), False),
    StructField("budget_allocated", DoubleType(), False),
])
data = []
for year in [2024, 2025]:
    for region in ["North America", "Europe", "Asia Pacific", "Latin America"]:
        for quarter in ["Q1", "Q2", "Q3", "Q4"]:
            data.append((region, quarter, year,
                random.uniform(500000, 2000000),
                random.randint(500, 5000),
                random.uniform(50000, 200000)))
df = spark.createDataFrame(data, schema)
df.write.format("delta").mode("overwrite").saveAsTable("regional_sales_targets")

# Marketing campaigns
campaign_schema = StructType([
    StructField("campaign_id", IntegerType(), False),
    StructField("campaign_name", StringType(), False),
    StructField("channel", StringType(), False),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False),
    StructField("budget", DoubleType(), False),
    StructField("impressions", IntegerType(), False),
    StructField("clicks", IntegerType(), False),
    StructField("conversions", IntegerType(), False),
])
campaigns = [
    (1, "Spring Sale 2024",  "Email",       "2024-03-01", "2024-03-31",  50000.0, 1000000,  45000, 2200),
    (2, "Summer Launch",     "Social Media","2024-06-15", "2024-07-15",  75000.0, 2000000,  80000, 4500),
    (3, "Back to School",    "Search",      "2024-08-01", "2024-09-15",  60000.0, 1500000,  67000, 3100),
    (4, "Holiday Special",   "Display",     "2024-11-15", "2024-12-31", 100000.0, 3000000, 120000, 8000),
    (5, "New Year Kickoff",  "Email",       "2025-01-01", "2025-01-31",  45000.0,  800000,  36000, 1800),
]
campaign_df = spark.createDataFrame(campaigns, campaign_schema)
campaign_df.write.format("delta").mode("overwrite").saveAsTable("marketing_campaigns")
print("Fabric sample data created.")
```

OneLake automatically generates Iceberg metadata alongside the Delta files. Snowflake reads this via `METADATA_FILE_PATH` in `01_fabric_snowflake_integration.sql` Part 3.

### Step 5: Create OneLake Shortcuts (Required for Fabric to See Snowflake Tables)

After running Part 2 of `01_fabric_snowflake_integration.sql`, create shortcuts to surface Snowflake Iceberg files as Fabric tables:

1. Open `demo_lakehouse` → **Tables** panel → **...** → **New shortcut**
2. Select **OneLake** as source
3. Navigate to `Files` → `snowflake-iceberg` → select the table folder
4. Click **Next** → confirm name → **Create shortcut**

| Shortcut Name | Source Path |
|---|---|
| `customer_360` | `Files/snowflake-iceberg/customer_360/` |
| `sales_summary` | `Files/snowflake-iceberg/sales_summary/` |
| `product_performance` | `Files/snowflake-iceberg/product_performance/` |
| `ml_predictions` | `Files/snowflake-iceberg/ml_predictions/` |

### Step 6: Verify Bidirectional Access

**Snowflake → Fabric** — in Fabric SQL Endpoint:
```sql
SELECT TOP 10 * FROM customer_360;
SELECT region, SUM(net_revenue) AS total_revenue FROM sales_summary GROUP BY region;
```

**Fabric → Snowflake** — in Snowflake:
```sql
SELECT * FROM MSFT_SNOWFLAKE_DEMO.ICEBERG.FABRIC_REGIONAL_TARGETS LIMIT 10;
SELECT t.region, t.fiscal_quarter, t.revenue_target,
       s.NET_REVENUE AS actual_revenue,
       ROUND((s.NET_REVENUE / t.revenue_target) * 100, 2) AS attainment_pct
FROM MSFT_SNOWFLAKE_DEMO.ICEBERG.FABRIC_REGIONAL_TARGETS t
JOIN MSFT_SNOWFLAKE_DEMO.GOLD.DT_SALES_SUMMARY s ON t.region = s.REGION
WHERE t.fiscal_year = 2024;
```

### Step 7: Power BI (DirectLake Mode)

1. Fabric workspace → **New** → **Semantic Model**
2. Select `demo_lakehouse` → choose shortcut tables
3. Power BI reads directly from OneLake Parquet files — no import

### Troubleshooting

| Issue | Solution |
|---|---|
| Tables not appearing in Fabric Tables | OneLake shortcuts not created — follow Step 5 |
| `SYSTEM$VERIFY_EXTERNAL_VOLUME` fails | Consent URL not accepted or Contributor not assigned — redo Step 3 |
| Iceberg read fails in Snowflake | `METADATA_FILE_PATH` outdated — run `ALTER ICEBERG TABLE ... REFRESH '<new_file>'` |
| "Storage location not found" | Workspace ID or Lakehouse ID in external volume URL is wrong |
| Region mismatch | Fabric capacity and Snowflake must be in the same Azure region |

### Quick Reference

| Parameter | Where to Get |
|---|---|
| `<workspace_id>` | Fabric workspace URL |
| `<lakehouse_id>` | Fabric lakehouse URL |
| `<azure_tenant_id>` | Azure Portal → Entra ID → Overview |
| `AZURE_CONSENT_URL` | `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL` output |
| `AZURE_MULTI_TENANT_APP_NAME` | `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL` output |
| `<metadata_file_path>` | Fabric lakehouse → Tables → browse metadata/ folder |

---

## Part 2: AI Foundry Agent Setup

Step-by-step guide to create agents in Azure AI Foundry that connect to Snowflake via MCP.

### Prerequisites

- Azure AI Foundry project with a deployed model (gpt-4o or gpt-4o-mini)
- Snowflake MCP server created (`04_gold/04_cortex_agent_and_intelligence.sql` Part 2)
- Snowflake Personal Access Token (PAT) for MCP authentication

### Step 1: Open Azure AI Foundry

1. Navigate to [Azure AI Foundry](https://ai.azure.com) → select your project
2. Verify a model is deployed: **Models + Endpoints** in the left nav
   - If not: **+ Deploy model** → select `gpt-4o-mini` → **Deploy**

### Step 2: Connect Snowflake MCP Server

1. **Management** → **Connected resources** → **+ New connection**
2. Select **MCP Server** (or **Custom**)
3. Configure:

| Field | Value |
|---|---|
| **Name** | `snowflake-mcp` |
| **Endpoint URL** | `https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse` |
| **Authentication** | Bearer Token |
| **Token** | Your Snowflake PAT |

> **Getting a PAT:** Snowsight → profile icon → **Preferences** → **Authentication** → **Generate new Personal Access Token**

4. Click **Save** — verify 5 tools are discovered: `sales-analyst`, `product-review-search`, `support-ticket-search`, `sales-support-agent`, `sql-executor`

### Step 3: Create Agent 1 — Snowflake Data Analyst

**Build** → **Agents** → **+ New agent**

| Field | Value |
|---|---|
| **Name** | `Snowflake Data Analyst` |
| **Model** | `gpt-4o-mini` |

**Instructions:**
```
You are a data analyst agent that queries sales and business data from Snowflake.
Use the sales-analyst tool for business questions about revenue, orders, customers,
and product performance. Use sql-executor for custom ad-hoc queries.
Always present data in clear tables or summaries with trends and comparisons.
```

**Tools:** enable `sales-analyst` and `sql-executor` from `snowflake-mcp`

**Test:** "What is the total revenue by region?" / "Top 5 product categories by units sold?"

### Step 4: Create Agent 2 — Customer Insights Agent

**+ New agent**

| Field | Value |
|---|---|
| **Name** | `Customer Insights Agent` |
| **Model** | `gpt-4o-mini` |

**Instructions:**
```
You are a customer insights agent that analyzes product reviews and support tickets
from Snowflake. Use product-review-search for review feedback and sentiment.
Use support-ticket-search for similar issues and resolution patterns.
Summarize themes, highlight sentiment trends, and reference specific IDs as evidence.
```

**Tools:** enable `product-review-search` and `support-ticket-search` from `snowflake-mcp`

**Test:** "What are customers saying about battery life?" / "Find billing support tickets"

### Step 5: Create Agent 3 — Fabric Data Agent

**+ New agent**

| Field | Value |
|---|---|
| **Name** | `Fabric Data Agent` |
| **Model** | `gpt-4o-mini` |

**Instructions:**
```
You are a data agent that queries Microsoft Fabric data including regional sales
targets and marketing campaign performance. Compare Fabric targets with Snowflake
actuals when asked. Highlight variances and provide recommendations.
```

**Tools:** enable **Code Interpreter**

**Test:** "Show me the regional sales targets for Q1 2025"

### Step 6: Create Orchestrator Agent (Optional)

**+ New agent** — Name: `Business Intelligence Orchestrator`

**Instructions:**
```
You are an orchestrator routing business questions to the right tools.
Revenue/sales/orders → use sales-analyst.
Reviews/feedback/tickets → use product-review-search or support-ticket-search.
Targets/budgets/campaigns → use Fabric data tools.
Complex multi-source questions → use sales-support-agent.
Synthesize results into clear, actionable business insights.
```

**Tools:** enable all 5 MCP tools + Code Interpreter

### Agent Summary

| Agent | Purpose | Tools |
|---|---|---|
| Snowflake Data Analyst | Structured data queries | `sales-analyst`, `sql-executor` |
| Customer Insights Agent | Review & ticket search | `product-review-search`, `support-ticket-search` |
| Fabric Data Agent | Fabric Lakehouse data | Code Interpreter |
| Business Intelligence Orchestrator | Routes to best agent | All 5 MCP tools + Code Interpreter |

### Troubleshooting

| Issue | Solution |
|---|---|
| MCP connection fails | Verify PAT is valid; account URL uses hyphens not underscores |
| "Tool not found" | `DESCRIBE MCP SERVER AGENTS.DEMO_MCP_SERVER;` to verify all 5 tools exist |
| Agent can't query data | PAT user needs USAGE on the MCP server and underlying objects |
| Agent times out | Verify Snowflake warehouse is running; check warehouse auto-resume is enabled |

---

## Part 3: Multi-Agent Workflow Testing

### Workflow Patterns

| Pattern | Description | Where to Test |
|---|---|---|
| **Sequential** | Agent A → Agent B → Synthesizer | Foundry Playground (manual chaining) |
| **Parallel** | Same question across platforms, results compared | Foundry + Snowsight side-by-side |
| **Router** | Orchestrator routes to best agent | Foundry Orchestrator Agent |
| **Cross-Platform** | Fabric targets vs Snowflake actuals | Foundry + Snowflake Intelligence |

### Workflow 1: Snowflake Intelligence (Cortex Agent)

Snowsight → **AI & ML** → **Agents** → **Sales & Support Assistant**

**Structured data (routes to Cortex Analyst):**
- "What is the total revenue by region?"
- "What are the top 5 product categories by units sold?"
- "Show me monthly revenue trend for the past year"

**Unstructured search (routes to Cortex Search):**
- "What are customers saying about battery life in their reviews?"
- "Find support tickets about software crashes"

**Multi-tool (agent uses both):**
- "How many support tickets do we have for Electronics products, and what is the average rating for Electronics in reviews?"
- "Give me a complete overview of our Software category: sales, customer feedback, and support issues."

### Workflow 2: Azure AI Foundry Agent Playground

**Snowflake Data Analyst:**
- "What is the total revenue by region? Summarize the top 3 regions."
- "How many unique customers placed orders in each channel?"

**Customer Insights Agent:**
- "What are the main themes in negative product reviews?"
- "Find support tickets related to billing disputes for Enterprise customers"

**Fabric Data Agent:**
- "Show me the regional sales targets for 2024"

### Workflow 3: Sequential Pipeline (Manual Chaining)

**Step 1:** Open Snowflake Data Analyst → ask:
"What is the revenue and order count by product category? Include average order value."
→ Copy the response.

**Step 2:** Open Customer Insights Agent → paste:
"Based on this data analysis, find related customer feedback for the top categories: [PASTE STEP 1 RESPONSE]. What do reviews say about the top and worst-performing categories?"
→ Copy the response.

**Step 3:** Open Business Intelligence Orchestrator → paste both responses:
"Create an executive summary combining these findings: DATA ANALYSIS: [Step 1] CUSTOMER INSIGHTS: [Step 2]. Highlight key metrics, sentiment trends, and actionable recommendations."

### Workflow 4: Parallel Comparison (Side-by-Side)

**Tab 1** (Snowflake Intelligence): "Give me a complete overview of our Electronics category: revenue, top products, and customer sentiment."

**Tab 2** (Foundry Orchestrator): Same question.

| Aspect | Snowflake Cortex Agent | Foundry Orchestrator |
|---|---|---|
| Data source | Direct Snowflake access | Via MCP to Snowflake |
| Response time | Faster (no network hop) | Slightly slower (MCP roundtrip) |
| Tool routing | Automatic (built-in) | LLM-based routing |
| Visualization | Charts via data_to_chart | Text-based (unless Code Interpreter) |

### Workflow 5: Cross-Platform Comparison (Fabric vs Snowflake)

**Step 1 — Targets from Fabric** (Foundry Fabric Data Agent):
"What are the regional sales targets and budgets for 2024? Show target_revenue by region."

**Step 2 — Actuals from Snowflake** (Snowflake Intelligence):
"What is the total net revenue by region for 2024?"

**Step 3 — Compare** (Foundry Orchestrator):
"Compare these sales targets with actual results and calculate attainment percentage: TARGETS: [Step 1] ACTUALS: [Step 2]. Create a table: Region, Target Revenue, Actual Revenue, Attainment %, Status (Over/Under)."

### Workflow 6: MCP Client Integration

**Cursor (`.cursor/mcp.json`):**
```json
{
  "mcpServers": {
    "snowflake-demo": {
      "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
      "headers": { "Authorization": "Bearer <YOUR_SNOWFLAKE_PAT>" }
    }
  }
}
```

**Claude Desktop** (`~/Library/Application Support/Claude/claude_desktop_config.json`):
```json
{
  "mcpServers": {
    "snowflake-demo": {
      "url": "https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/DEMO_MCP_SERVER/sse",
      "headers": { "Authorization": "Bearer <YOUR_SNOWFLAKE_PAT>" }
    }
  }
}
```

Restart the client → verify Snowflake tools appear → ask any business question.

### Test Matrix

| # | Test | Platform | Tool Expected |
|---|---|---|---|
| 1 | "Total revenue by region" | Snowflake Intelligence | Cortex Analyst |
| 2 | "Reviews about battery life" | Snowflake Intelligence | Cortex Search |
| 3 | "Revenue + reviews for Electronics" | Snowflake Intelligence | Both tools |
| 4 | "Top 5 categories by revenue" | Foundry Data Analyst | sales-analyst (MCP) |
| 5 | "Negative review themes" | Foundry Customer Insights | product-review-search (MCP) |
| 6 | "Billing support tickets" | Foundry Customer Insights | support-ticket-search (MCP) |
| 7 | "Sales targets for 2024" | Foundry Fabric Agent | Code Interpreter |
| 8 | "Complete Electronics overview" | Foundry Orchestrator | Multiple MCP tools |
| 9 | Sequential: Data → Insights → Report | Manual chaining | 3 agents in sequence |
| 10 | Cross-platform: Fabric vs Snowflake | Foundry + Snowsight | Compare targets vs actuals |
| 11 | MCP client (Cursor/Claude Desktop) | Local IDE | MCP tools |
