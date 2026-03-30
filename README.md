# Microsoft Azure + Snowflake End-to-End Quickstart Lab

A comprehensive hands-on lab demonstrating enterprise data integration between **Microsoft Azure services** and **Snowflake**, covering data processing, ML, AI agents, Fabric interoperability, and multi-agent orchestration.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                      DATA SOURCES                         │
│          Native Snowflake SQL  │  ADLS Gen2               │
└──────────────────┬─────────────┴──────────┬──────────────┘
                   │                        │
                   ▼                        ▼
┌──────────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE DATA PLATFORM                          │
│                                                                      │
│  ┌─────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   RAW    │→ │   STAGING    │→ │   CURATED    │→ │  ANALYTICS   │ │
│  │ (Bronze) │  │ (Dyn Tables) │  │   (Silver)   │  │   (Gold)     │ │
│  └─────────┘  └──────────────┘  └──────────────┘  └──────┬───────┘ │
│                                                           │         │
│  ┌──────────┐  ┌──────────────┐  ┌──────────────────────┐│         │
│  │    ML    │  │   AGENTS     │  │    ICEBERG            ││         │
│  │ Models & │  │ Semantic View│  │  (→ Fabric)           ││         │
│  │ Registry │  │ Search, Agent│  │                       ││         │
│  └──────────┘  │ MCP Server   │  └───────────────────────┘│         │
│                └──────┬───────┘                            │         │
└───────────────────────┼───────────────────────────────────┼─────────┘
                        │                                   │
                        ▼                                   ▼
┌───────────────────────────────┐  ┌────────────────────────────────┐
│   SNOWFLAKE INTELLIGENCE      │  │   MICROSOFT FABRIC              │
│   (Chat UI for Agents)        │  │   Lakehouse / Power BI          │
└───────────────────────────────┘  └────────────────────────────────┘
                        │                                   │
                        └──────────────┬────────────────────┘
                                       ▼
               ┌───────────────────────────────────────────┐
               │   MULTI-AGENT ORCHESTRATION                │
               │   Foundry Agents + Cortex Agents via MCP   │
               │   Sequential │ Parallel │ Router           │
               └───────────────────────────────────────────┘
```

## Prerequisites

| Requirement | Minimum |
|---|---|
| Snowflake Account | Enterprise edition (for Cortex AI features) |
| Azure Subscription | With Entra ID, ADLS Gen2 |
| Microsoft Fabric | F2 capacity or Trial |
| Azure AI Foundry | Project with GPT-4o model deployed |
| Python | 3.9+ with snowflake-ml-python, azure-ai-projects |

See [`01_setup/02_azure_prerequisites.md`](01_setup/02_azure_prerequisites.md) for detailed Azure resource setup.

## Lab Phases

### Phase 1: Foundation Setup
| File | Description |
|---|---|
| [`01_setup/01_account_setup.sql`](01_setup/01_account_setup.sql) | Roles, warehouses, database, schemas, integrations, file formats |
| [`01_setup/02_azure_prerequisites.md`](01_setup/02_azure_prerequisites.md) | Azure resource checklist and configuration guide |

**Creates:** 4 roles, 3 warehouses (including Snowpark-optimized), 6 schemas, storage integration, external volume for OneLake.

### Phase 2: Data Creation
| File | Description |
|---|---|
| [`02_native_data/01_create_tables.sql`](02_native_data/01_create_tables.sql) | Tables across RAW schema, internal stages |
| [`02_native_data/02_generate_synthetic_data.sql`](02_native_data/02_generate_synthetic_data.sql) | ~200K rows of synthetic data (customers, orders, reviews, tickets) |

**Data sources:** Native Snowflake SQL with synthetic data generation.

### Phase 3: Data Processing
| File | Description |
|---|---|
| [`04_processing/01_dynamic_tables.sql`](04_processing/01_dynamic_tables.sql) | Bronze→Silver→Gold pipeline (5 dynamic tables) |
| [`04_processing/02_streams_and_tasks.sql`](04_processing/02_streams_and_tasks.sql) | CDC with Streams, SCD Type 2, Task DAG |
| [`04_processing/03_snowpark_processing.py`](04_processing/03_snowpark_processing.py) | Window functions, Python UDFs, sentiment analysis |

**Key patterns:** Dynamic Tables with TARGET_LAG and DOWNSTREAM, Streams for CDC, Task DAGs, Snowpark DataFrame API.

### Phase 4: Machine Learning
| File | Description |
|---|---|
| [`05_ml_models/01_ticket_priority_classifier.ipynb`](05_ml_models/01_ticket_priority_classifier.ipynb) | End-to-end: features → RandomForest + LogisticRegression → registry |
| [`05_ml_models/02_revenue_predictor.ipynb`](05_ml_models/02_revenue_predictor.ipynb) | End-to-end: features → XGBRegressor + LinearRegression → registry |

**ML stack:** snowflake.ml.modeling (scikit-learn wrappers), Snowflake Model Registry, Snowpark-optimized warehouse.

### Phase 5: Cortex AI
| File | Description |
|---|---|
| [`06_cortex_ai/01_semantic_view.sql`](06_cortex_ai/01_semantic_view.sql) | Semantic View mapping business concepts to physical tables |
| [`06_cortex_ai/02_cortex_search_service.sql`](06_cortex_ai/02_cortex_search_service.sql) | Search services for reviews and tickets (hybrid keyword + vector) |
| [`06_cortex_ai/03_cortex_agent.sql`](06_cortex_ai/03_cortex_agent.sql) | Cortex Agent with Analyst + Search tools |
| [`06_cortex_ai/04_mcp_server.sql`](06_cortex_ai/04_mcp_server.sql) | MCP Server exposing all tools to external clients |
| [`06_cortex_ai/05_snowflake_intelligence.sql`](06_cortex_ai/05_snowflake_intelligence.sql) | Publish agent to Snowflake Intelligence chat UI |

**AI capabilities:** Natural language to SQL (Cortex Analyst), semantic search (Cortex Search), multi-tool orchestration (Cortex Agent), external tool access (MCP Server).

### Phase 6: Microsoft Fabric Integration
| File | Description |
|---|---|
| [`07_fabric_integration/01_external_volume_onelake.sql`](07_fabric_integration/01_external_volume_onelake.sql) | External volume validation and consent flow |
| [`07_fabric_integration/02_iceberg_tables_to_fabric.sql`](07_fabric_integration/02_iceberg_tables_to_fabric.sql) | Iceberg tables writing gold-layer data to OneLake |
| [`07_fabric_integration/03_catalog_integration_onelake.sql`](07_fabric_integration/03_catalog_integration_onelake.sql) | Catalog-linked database reading Fabric data from Snowflake |
| [`07_fabric_integration/04_fabric_setup_guide.md`](07_fabric_integration/04_fabric_setup_guide.md) | End-to-end Fabric workspace and Lakehouse setup guide |

**Integration pattern:** Bidirectional via Apache Iceberg — Snowflake writes to OneLake (Fabric reads), Fabric writes to OneLake (Snowflake reads via catalog integration).

### Phase 7: Multi-Agent Orchestration
| File | Description |
|---|---|
| [`08_multi_agent_orchestration/01_foundry_agent_setup.md`](08_multi_agent_orchestration/01_foundry_agent_setup.md) | Guide to create Foundry agents via Azure AI Foundry Portal UI |
| [`08_multi_agent_orchestration/02_multi_agent_workflow.md`](08_multi_agent_orchestration/02_multi_agent_workflow.md) | Test 4 workflow patterns via Snowsight + Foundry Portal + MCP clients |
| [`08_multi_agent_orchestration/03_orchestration_config.yaml`](08_multi_agent_orchestration/03_orchestration_config.yaml) | Central config for agents, tools, workflows, and MCP client setup |

**Orchestration patterns:** Sequential pipeline, parallel comparison, intelligent routing, cross-platform Fabric vs Snowflake analysis. All done via UI — no CLI or Python scripts required.

## Execution Order

Run the phases in order. Within each phase, run files in numeric order.

```
Phase 1 → Phase 2 → Phase 3 → Phase 4 → Phase 5 → Phase 6 → Phase 7
```

| Step | Script | How to Run |
|---|---|---|
| 1 | `01_setup/01_account_setup.sql` | Snowsight SQL Worksheet (as ACCOUNTADMIN) |
| 2 | `01_setup/02_azure_prerequisites.md` | Manual Azure Portal setup |
| 3 | `02_native_data/01_create_tables.sql` | Snowsight SQL Worksheet |
| 4 | `02_native_data/02_generate_synthetic_data.sql` | Snowsight SQL Worksheet |
| 5 | `04_processing/01_dynamic_tables.sql` | Snowsight SQL Worksheet |
| 6 | `04_processing/02_streams_and_tasks.sql` | Snowsight SQL Worksheet |
| 7 | `04_processing/03_snowpark_processing.py` | Snowflake Notebook or local Python |
| 8 | `05_ml_models/01_ticket_priority_classifier.ipynb` | Snowflake Workspace Notebook (DEMO_ML_WH) |
| 9 | `05_ml_models/02_revenue_predictor.ipynb` | Snowflake Workspace Notebook (DEMO_ML_WH) |
| 10 | `06_cortex_ai/01_semantic_view.sql` | Snowsight SQL Worksheet |
| 11 | `06_cortex_ai/02_cortex_search_service.sql` | Snowsight SQL Worksheet |
| 12 | `06_cortex_ai/03_cortex_agent.sql` | Snowsight SQL Worksheet |
| 13 | `06_cortex_ai/04_mcp_server.sql` | Snowsight SQL Worksheet |
| 14 | `06_cortex_ai/05_snowflake_intelligence.sql` | Snowsight SQL Worksheet |
| 15 | `07_fabric_integration/01_external_volume_onelake.sql` | Snowsight (ACCOUNTADMIN) |
| 16 | `07_fabric_integration/02_iceberg_tables_to_fabric.sql` | Snowsight SQL Worksheet |
| 17 | `07_fabric_integration/03_catalog_integration_onelake.sql` | Snowsight (ACCOUNTADMIN) |
| 18 | `07_fabric_integration/04_fabric_setup_guide.md` | Fabric Portal |
| 19 | `08_multi_agent_orchestration/01_foundry_agent_setup.md` | Azure AI Foundry Portal |
| 20 | `08_multi_agent_orchestration/02_multi_agent_workflow.md` | Snowsight + Foundry Portal + MCP clients |

## Key Snowflake Objects Created

| Object Type | Names |
|---|---|
| **Database** | `MSFT_SNOWFLAKE_DEMO` |
| **Schemas** | RAW, STAGING, CURATED, ANALYTICS, ML, AGENTS, ICEBERG |
| **Roles** | DEMO_ADMIN, DEMO_ANALYST, DEMO_ML_ENGINEER, DEMO_AGENT_USER |
| **Warehouses** | DEMO_WH, DEMO_ML_WH (Snowpark), DEMO_CORTEX_WH |
| **Dynamic Tables** | 5 (Bronze→Silver→Gold pipeline) |
| **ML Models** | TICKET_PRIORITY_CLASSIFIER, REVENUE_PREDICTOR |
| **Semantic View** | SALES_ANALYTICS_SV |
| **Search Services** | PRODUCT_REVIEW_SEARCH, SUPPORT_TICKET_SEARCH |
| **Cortex Agent** | SALES_SUPPORT_AGENT |
| **MCP Server** | DEMO_MCP_SERVER |
| **Iceberg Tables** | 4 (Customer 360, Sales, Products, ML Predictions) |

## Python Dependencies

Phase 4 (ML) notebooks are designed to run in **Snowflake Workspace** — all required packages (`snowflake-ml-python`, `xgboost`) are pre-installed. No local `pip install` needed.

For Phase 3 Snowpark processing, if running locally:
```bash
pip install snowflake-snowpark-python
```

Phase 7 (Multi-Agent Orchestration) is entirely UI-driven via Azure AI Foundry Portal and Snowsight — no Python packages needed.

## Placeholder Values

Replace these placeholders with your actual values throughout the scripts:

| Placeholder | Description |
|---|---|
| `<org>-<account>` | Snowflake account identifier |
| `<your_azure_tenant_id>` | Azure Entra ID tenant ID |
| `<your_workspace_id>` | Microsoft Fabric workspace GUID |
| `<your_lakehouse_id>` | Microsoft Fabric Lakehouse GUID |
| `<your_storage_account>` | Azure Storage Account name |
| `<your_entra_app_client_id>` | Entra ID app registration client ID |
| `<your_entra_app_client_secret>` | Entra ID app registration secret |
| `<your-resource>` | Azure AI Foundry resource name |
| `<your-project>` | Azure AI Foundry project name |

## Cleanup

To remove all lab resources from Snowflake:

```sql
USE ROLE ACCOUNTADMIN;

-- Drop database (removes all schemas, tables, views, dynamic tables, etc.)
DROP DATABASE IF EXISTS MSFT_SNOWFLAKE_DEMO;
DROP DATABASE IF EXISTS FABRIC_DATA;

-- Drop warehouses
DROP WAREHOUSE IF EXISTS DEMO_WH;
DROP WAREHOUSE IF EXISTS DEMO_ML_WH;
DROP WAREHOUSE IF EXISTS DEMO_CORTEX_WH;

-- Drop integrations
DROP STORAGE INTEGRATION IF EXISTS AZURE_STORAGE_INT;
DROP CATALOG INTEGRATION IF EXISTS FABRIC_ONELAKE_CATALOG_INT;
DROP EXTERNAL VOLUME IF EXISTS ONELAKE_EXTERNAL_VOL;

-- Drop roles
DROP ROLE IF EXISTS DEMO_AGENT_USER;
DROP ROLE IF EXISTS DEMO_ML_ENGINEER;
DROP ROLE IF EXISTS DEMO_ANALYST;
DROP ROLE IF EXISTS DEMO_ADMIN;
```
