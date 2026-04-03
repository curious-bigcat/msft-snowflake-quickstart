# Microsoft Azure + Snowflake End-to-End Quickstart Lab

A comprehensive hands-on lab demonstrating enterprise data integration between **Microsoft Azure services** and **Snowflake**, built on a **Medallion Architecture** (Bronze → Silver → Gold) with Snowpark processing, ML models, Cortex AI agents, bidirectional Microsoft Fabric integration, and Azure AI Foundry multi-agent orchestration.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                   │
│  Native SQL (Snowflake) │ ADLS Gen2 CSV │ OneLake JSON │ Fabric Delta   │
└──────────┬──────────────────────┬────────────────┬──────────────────────┘
           │                      │                │
           ▼                      ▼                ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE  (schema: BRONZE)                                                │
│  Raw ingestion — minimal transformation, full history retained           │
│  • Native tables: CUSTOMERS, ORDERS, PRODUCTS, ORDER_ITEMS,             │
│    PRODUCT_REVIEWS, SUPPORT_TICKETS                                      │
│  • CSV from ADLS:   REGIONAL_SALES_TARGETS, MARKETING_CAMPAIGNS         │
│    (Snowpipe auto-ingest via Azure Event Grid)                           │
│  • JSON from OneLake: CLICKSTREAM_EVENTS, IOT_EVENTS                    │
│    (Snowpipe + LATERAL FLATTEN for semi-structured data)                 │
└──────────┬───────────────────────────────────────────────────────────────┘
           │  Streams (CDC) + Dynamic Tables
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER  (schema: SILVER)                                                │
│  Curation, cleansing, enrichment — no aggregation                        │
│  • DT_ORDERS_CLEANED   (Dynamic Table, DOWNSTREAM)                      │
│  • DT_ORDERS_ENRICHED  (Dynamic Table, DOWNSTREAM)                      │
│  • ORDERS_SCD2         (SCD Type 2 via Streams + Tasks)                 │
│  • Snowpark UDF: UDF_SENTIMENT_SCORE (permanent Python UDF)             │
│  • PROCESSED_REVIEWS, PRODUCT_SENTIMENT_SUMMARY,                        │
│    MONTHLY_REVENUE_BY_CATEGORY (Snowpark DataFrame API)                 │
└──────────┬───────────────────────────────────────────────────────────────┘
           │  Dynamic Tables + Materialized Views
           ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD  (schema: GOLD)                                                    │
│  Consumption-ready — analytics, ML, AI                                   │
│  Dynamic Tables:  DT_SALES_SUMMARY · DT_CUSTOMER_360 ·                  │
│                   DT_PRODUCT_PERFORMANCE  (15-min lag)                   │
│  Materialized Views: MV_TOP_CUSTOMERS · MV_MONTHLY_KPI ·                │
│                      MV_PRODUCT_HEALTH                                   │
│  ML Models:  TICKET_PRIORITY_CLASSIFIER · REVENUE_PREDICTOR             │
│  Cortex AI:  Semantic View · Search Services · Agent · MCP Server       │
└────────┬──────────────────────────────────────────────────────┬──────────┘
         │                                                       │
         ▼                                                       ▼
┌─────────────────────────┐          ┌──────────────────────────────────┐
│  SNOWFLAKE INTELLIGENCE  │          │  MICROSOFT FABRIC / ADLS         │
│  Cortex Agent chat UI    │          │  Iceberg write-back to OneLake   │
│  + Azure AI Foundry MCP  │          │  Catalog integration (read)      │
└────────────┬────────────┘          └──────────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  MULTI-AGENT ORCHESTRATION                                               │
│  Foundry Agents + Cortex Agent via MCP Server                           │
│  Sequential │ Parallel │ Router │ Cross-platform Fabric + Snowflake     │
└─────────────────────────────────────────────────────────────────────────┘
```

## Prerequisites

| Requirement | Minimum |
|---|---|
| Snowflake Account | Enterprise edition (Cortex AI features required) |
| Azure Subscription | With Entra ID, ADLS Gen2, Storage Queue |
| Microsoft Fabric | F2 capacity or Trial |
| Azure AI Foundry | Project with GPT-4o-mini deployed |
| Python | 3.9+ (only needed for local Snowpark; Workspace notebooks are self-contained) |

See [`01_setup/02_azure_prerequisites.md`](01_setup/02_azure_prerequisites.md) for detailed Azure resource provisioning.

## Lab Phases

### Phase 1: Foundation Setup
| File | Description |
|---|---|
| [`01_setup/01_account_setup.sql`](01_setup/01_account_setup.sql) | Roles, warehouses, schemas (BRONZE/SILVER/GOLD/ML/AGENTS/ICEBERG), storage integration, Snowpipe notification integration, OneLake external volumes, file formats |
| [`01_setup/02_azure_prerequisites.md`](01_setup/02_azure_prerequisites.md) | Azure resource checklist: ADLS Gen2, Fabric workspace, Entra app registration, AI Foundry project |

### Phase 2: Bronze Layer — Raw Ingestion
| File | Description |
|---|---|
| [`02_bronze/01_tables_and_data.sql`](02_bronze/01_tables_and_data.sql) | Part 1: Raw table DDL (CUSTOMERS, PRODUCTS, ORDERS, ORDER_ITEMS, PRODUCT_REVIEWS, SUPPORT_TICKETS). Part 2: ~200K rows of synthetic data using `TABLE(GENERATOR(...))` |
| [`02_bronze/02_adls_ingestion.sql`](02_bronze/02_adls_ingestion.sql) | External stage + COPY INTO + Snowpipe auto-ingest for CSV files from ADLS Gen2 (REGIONAL_SALES_TARGETS, MARKETING_CAMPAIGNS) |
| [`02_bronze/03_fabric_integration.sql`](02_bronze/03_fabric_integration.sql) | **Section A (Demo)** — writes synthetic clickstream (~100K), IoT events (~50K), targets, and campaigns to OneLake as Snowflake-managed Iceberg via `ONELAKE_EXTERNAL_VOL`. **Section B (Production)** — catalog integration (`OBJECT_STORE`) for real Fabric Lakehouse tables; update `METADATA_FILE_PATH` placeholders first. **Section C** — exports all 6 Bronze tables to `ICEBERG.RAW_*` in OneLake. Sections A and B are mutually exclusive. |

**Sample data files** (upload to ADLS before running `02_adls_ingestion.sql`):
| File | Rows | ADLS path |
|---|---|---|
| [`sample_data/regional_sales_targets.csv`](sample_data/regional_sales_targets.csv) | 52 | `snowflake-data/csv/regional_sales_targets/` |
| [`sample_data/marketing_campaigns.csv`](sample_data/marketing_campaigns.csv) | 25 | `snowflake-data/csv/marketing_campaigns/` |

> Note: CSV files include a header row. Ensure `BRONZE.CSV_FORMAT` has `SKIP_HEADER = 1`.

**Ingestion patterns:** Native SQL, COPY INTO / Snowpipe (ADLS CSV), Iceberg write-back to OneLake (Snowflake → Fabric via `ONELAKE_EXTERNAL_VOL`), catalog integration for real Fabric tables (production).

### Phase 3: Silver Layer — Curation and Cleansing
| File | Description |
|---|---|
| [`03_silver/01_silver_processing.sql`](03_silver/01_silver_processing.sql) | Part 1: `SILVER.DT_ORDERS_CLEANED` (validation, quality flags) → `SILVER.DT_ORDERS_ENRICHED` (customer join, value tiers). Part 2: CDC streams on BRONZE tables, SCD Type 2 via MERGE, Task DAG writing to GOLD |
| [`04_gold/02_snowpark_processing.ipynb`](04_gold/02_snowpark_processing.ipynb) | Snowpark: window functions, Python UDF registration, sentiment scoring, writes to GOLD |

**Key patterns:** Dynamic Tables with `DOWNSTREAM` lag, Streams for CDC, Task DAGs (all tasks in SILVER schema), Snowpark DataFrame API, permanent Python UDFs stored in `@ML.ML_MODELS`.

### Phase 4: Gold Layer — Consumption, ML, and AI
| File | Description |
|---|---|
| [`04_gold/01_dynamic_tables_and_views.sql`](04_gold/01_dynamic_tables_and_views.sql) | Gold Dynamic Tables (DT_SALES_SUMMARY, DT_CUSTOMER_360, DT_PRODUCT_PERFORMANCE at 15-min lag) + Materialized Views (MV_TOP_CUSTOMERS, MV_MONTHLY_KPI, MV_PRODUCT_HEALTH) |
| [`04_gold/02_ml_models/01_ticket_priority_classifier.ipynb`](04_gold/02_ml_models/01_ticket_priority_classifier.ipynb) | Feature engineering from GOLD tables → RandomForest classifier → Model Registry |
| [`04_gold/02_ml_models/02_revenue_predictor.ipynb`](04_gold/02_ml_models/02_revenue_predictor.ipynb) | Time-series feature engineering → XGBRegressor → Model Registry |
| [`04_gold/03_cortex_analyst_and_search.sql`](04_gold/03_cortex_analyst_and_search.sql) | Part 1: Semantic View mapping business concepts (revenue, customers, products) to BRONZE tables. Part 2: Cortex Search services — PRODUCT_REVIEW_SEARCH, SUPPORT_TICKET_SEARCH (hybrid keyword + vector, 1-hr lag) |
| [`04_gold/04_cortex_agent_and_intelligence.sql`](04_gold/04_cortex_agent_and_intelligence.sql) | Part 1: Cortex Agent with Analyst (SQL), Review Search, Ticket Search tools. Part 2: MCP Server exposing all Cortex tools to external clients (Cursor, Claude Desktop, AI Foundry). Part 3: Publish agent to Snowflake Intelligence chat UI |

**ML stack:** `snowflake.ml.modeling`, Snowflake Model Registry, Snowpark-optimized warehouse (`DEMO_ML_WH`).
**AI stack:** Cortex Analyst (text-to-SQL), Cortex Search (semantic), Cortex Agent (multi-tool), MCP Server (external access).

### Phase 5: Fabric Integration and AI Foundry
| File | Description |
|---|---|
| [`05_fabric_and_ai_foundry/01_fabric_snowflake_integration.sql`](05_fabric_and_ai_foundry/01_fabric_snowflake_integration.sql) | Part 1: Validate OneLake external volumes (write + read), consent flow. Part 2: Iceberg write-back of GOLD layer data to OneLake (DT_CUSTOMER_360, DT_SALES_SUMMARY, DT_PRODUCT_PERFORMANCE, ML_PREDICTIONS). Part 3: Catalog integration reading Fabric-managed Delta tables from Snowflake; cross-platform join: Fabric targets vs GOLD actuals |
| [`05_fabric_and_ai_foundry/02_ai_foundry_guide.md`](05_fabric_and_ai_foundry/02_ai_foundry_guide.md) | Three-part guide: Part 1 — Fabric workspace, Lakehouse, service principal consent, OneLake shortcuts, Power BI DirectLake. Part 2 — Create 4 Azure AI Foundry agents connected to Snowflake via MCP. Part 3 — Test 6 workflow patterns: sequential, parallel, cross-platform, MCP client integration |
| [`05_fabric_and_ai_foundry/03_orchestration_config.yaml`](05_fabric_and_ai_foundry/03_orchestration_config.yaml) | Central config: agents, tools, workflows, MCP client JSON for Cursor and Claude Desktop |

**Integration pattern:** Bidirectional via Apache Iceberg — Snowflake GOLD → OneLake (Fabric reads via shortcuts); Fabric Delta → OneLake (Snowflake reads via catalog integration).

## Execution Order

| Step | File | Role | How |
|---|---|---|---|
| 1 | `01_setup/01_account_setup.sql` | ACCOUNTADMIN | Snowsight SQL Worksheet |
| 2 | `01_setup/02_azure_prerequisites.md` | — | Azure Portal (manual) |
| 3 | `02_bronze/01_tables_and_data.sql` | DEMO_ADMIN | Snowsight SQL Worksheet |
| 4 | `02_bronze/02_adls_ingestion.sql` | DEMO_ADMIN | Upload `sample_data/*.csv` to ADLS first, then Snowsight |
| 5 | `02_bronze/03_fabric_integration.sql` (Section A or B, then C) | DEMO_ADMIN / ACCOUNTADMIN | Section A: synthetic data to OneLake. Section B: real Fabric tables. Section C: write-back. A and B are mutually exclusive. |
| 6 | `03_silver/01_silver_processing.sql` | DEMO_ADMIN | Snowsight SQL Worksheet |
| 7 | `04_gold/02_snowpark_processing.ipynb` | DEMO_ADMIN | Snowflake Workspace Notebook |
| 8 | `04_gold/01_dynamic_tables_and_views.sql` | DEMO_ADMIN | Snowsight SQL Worksheet |
| 9 | `04_gold/02_ml_models/01_ticket_priority_classifier.ipynb` | DEMO_ML_ENGINEER | Snowflake Workspace Notebook (DEMO_ML_WH) |
| 10 | `04_gold/02_ml_models/02_revenue_predictor.ipynb` | DEMO_ML_ENGINEER | Snowflake Workspace Notebook (DEMO_ML_WH) |
| 11 | `04_gold/03_cortex_analyst_and_search.sql` | DEMO_ADMIN | Snowsight SQL Worksheet |
| 12 | `04_gold/04_cortex_agent_and_intelligence.sql` | DEMO_ADMIN | Snowsight SQL Worksheet |
| 13 | `05_fabric_and_ai_foundry/01_fabric_snowflake_integration.sql` | ACCOUNTADMIN / DEMO_ADMIN | Snowsight SQL Worksheet |
| 14 | `05_fabric_and_ai_foundry/02_ai_foundry_guide.md` | — | Fabric Portal + Azure AI Foundry Portal + MCP clients (manual) |

## Snowflake Object Inventory

| Object Type | Names |
|---|---|
| **Database** | `MSFT_SNOWFLAKE_DEMO` |
| **Schemas** | `BRONZE` · `SILVER` · `GOLD` · `ML` · `AGENTS` · `ICEBERG` |
| **Roles** | `DEMO_ADMIN` · `DEMO_ANALYST` · `DEMO_ML_ENGINEER` · `DEMO_AGENT_USER` |
| **Warehouses** | `DEMO_WH` · `DEMO_ML_WH` (Snowpark-optimized) · `DEMO_CORTEX_WH` |
| **Bronze Tables** | `CUSTOMERS` · `PRODUCTS` · `ORDERS` · `ORDER_ITEMS` · `PRODUCT_REVIEWS` · `SUPPORT_TICKETS` (native) · `REGIONAL_SALES_TARGETS` · `MARKETING_CAMPAIGNS` (ADLS CSV) · `FABRIC_CLICKSTREAM_EVENTS` · `FABRIC_IOT_EVENTS` · `FABRIC_REGIONAL_TARGETS` · `FABRIC_MARKETING_CAMPAIGNS` (Fabric Iceberg — zero-copy) |
| **Silver Tables** | `ORDERS_SCD2` · `PROCESSED_REVIEWS` · `PRODUCT_SENTIMENT_SUMMARY` · `MONTHLY_REVENUE_BY_CATEGORY` · `DAILY_ORDER_METRICS` · `SUPPORT_TICKET_METRICS` |
| **Silver DTs** | `DT_ORDERS_CLEANED` · `DT_ORDERS_ENRICHED` |
| **Silver Tasks** | `TASK_ORDERS_SCD2` · `TASK_DAILY_METRICS` · `TASK_TICKET_METRICS` |
| **Gold DTs** | `DT_SALES_SUMMARY` · `DT_CUSTOMER_360` · `DT_PRODUCT_PERFORMANCE` |
| **Gold MVs** | `MV_TOP_CUSTOMERS` · `MV_MONTHLY_KPI` · `MV_PRODUCT_HEALTH` |
| **ML Models** | `TICKET_PRIORITY_CLASSIFIER v1` · `REVENUE_PREDICTOR v1` |
| **Cortex AI** | `SALES_ANALYTICS_SV` (Semantic View) · `PRODUCT_REVIEW_SEARCH` · `SUPPORT_TICKET_SEARCH` · `SALES_SUPPORT_AGENT` · `DEMO_MCP_SERVER` |
| **Iceberg Tables** | `RAW_CUSTOMERS_ICEBERG` · `RAW_PRODUCTS_ICEBERG` · `RAW_ORDERS_ICEBERG` · `RAW_ORDER_ITEMS_ICEBERG` · `RAW_PRODUCT_REVIEWS_ICEBERG` · `RAW_SUPPORT_TICKETS_ICEBERG` (Bronze→Fabric) · `CUSTOMER_360_ICEBERG` · `SALES_SUMMARY_ICEBERG` · `PRODUCT_PERFORMANCE_ICEBERG` · `ML_PREDICTIONS_ICEBERG` (Gold→Fabric) · `FABRIC_REGIONAL_TARGETS` · `FABRIC_MARKETING_CAMPAIGNS` (Fabric→Snowflake read) |
| **Snowpipes** | `CSV_REGIONAL_TARGETS_PIPE` · `CSV_MARKETING_PIPE` (ADLS CSV only) |

## Placeholder Values

Replace these throughout the scripts before running:

| Placeholder | Where | Description |
|---|---|---|
| `<storage_account>` | Bronze CSV/JSON stages | Azure Storage Account name |
| `<your_azure_tenant_id>` | `01_account_setup.sql` | Azure Entra ID tenant ID |
| `<your_workspace_id>` | `01_account_setup.sql` | Microsoft Fabric workspace GUID |
| `<your_lakehouse_id>` | `01_account_setup.sql` | Microsoft Fabric Lakehouse GUID |
| `<your_queue>` | `01_account_setup.sql` | Azure Storage Queue name (Snowpipe) |
| `<org>-<account>` | MCP server URL | Snowflake account identifier |
| `<your-resource>` | AI Foundry setup | Azure AI Foundry resource name |
| `<your-project>` | AI Foundry setup | Azure AI Foundry project name |
| `<latest>.metadata.json` | Catalog integration | Iceberg metadata file path in OneLake |

## Cleanup

```sql
USE ROLE ACCOUNTADMIN;

-- Drop database (removes all schemas, tables, dynamic tables, views, tasks, streams, etc.)
DROP DATABASE IF EXISTS MSFT_SNOWFLAKE_DEMO;

-- Drop warehouses
DROP WAREHOUSE IF EXISTS DEMO_WH;
DROP WAREHOUSE IF EXISTS DEMO_ML_WH;
DROP WAREHOUSE IF EXISTS DEMO_CORTEX_WH;

-- Drop integrations
DROP STORAGE INTEGRATION     IF EXISTS AZURE_STORAGE_INT;
DROP NOTIFICATION INTEGRATION IF EXISTS AZURE_SNOWPIPE_INT;
DROP CATALOG INTEGRATION     IF EXISTS FABRIC_ONELAKE_CATALOG_INT;
DROP EXTERNAL VOLUME         IF EXISTS ONELAKE_EXTERNAL_VOL;
DROP EXTERNAL VOLUME         IF EXISTS ONELAKE_READ_VOL;

-- Drop roles
DROP ROLE IF EXISTS DEMO_AGENT_USER;
DROP ROLE IF EXISTS DEMO_ML_ENGINEER;
DROP ROLE IF EXISTS DEMO_ANALYST;
DROP ROLE IF EXISTS DEMO_ADMIN;
```
