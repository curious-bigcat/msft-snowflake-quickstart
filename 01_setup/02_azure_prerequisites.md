# Azure Prerequisites for MSFT-Snowflake Quickstart Lab

## Overview
This document lists all Azure resources required across all phases of the hands-on lab.
Collect the values in the **Quick Reference** table at the bottom before running any Snowflake scripts.

---

## 1. Azure Subscription & Resource Group

- [ ] Azure subscription with sufficient credits
- [ ] Resource group: `rg-snowflake-demo`
- [ ] Region: **East US 2** (recommended for Fabric + Snowflake proximity)

---

## 2. Azure Storage Account (ADLS Gen2 — for Snowpipe & ADF)

- [ ] Storage account name: `snowflakedemostore` (must be globally unique)
- [ ] Account kind: **StorageV2**
- [ ] Enable **hierarchical namespace** (ADLS Gen2)
- [ ] Performance: Standard
- [ ] Redundancy: LRS (sufficient for demo)

### Containers to Create
| Container | Purpose |
|-----------|---------|
| `raw-data` | Snowpipe auto-ingest (clickstream CSV/JSON files) |
| `adf-staging` | ADF pipeline staging area |
| `export-data` | Data export from Snowflake |

### Event Grid & Queue Setup (for Snowpipe)
- [ ] Enable **Event Grid** integration on the storage account
- [ ] Create Storage Queue: `snowpipe-notifications`
- [ ] Create Event Grid subscription:
  - Source: Storage account → Blob storage
  - Event types: `Blob Created`
  - Endpoint: Storage Queue → `snowpipe-notifications`
  - Filter: Subject begins with `/blobServices/default/containers/raw-data`

### IAM Grants for Snowflake Service Principal
After creating the storage integration in Snowflake, run:
```sql
DESC STORAGE INTEGRATION AZURE_STORAGE_INT;
```
Copy the `AZURE_MULTI_TENANT_APP_NAME` value, then in Azure:
- [ ] Storage Account → Access Control (IAM) → Add role assignment
  - Role: **Storage Blob Data Contributor**
  - Assign to: The Snowflake service principal
- [ ] Storage Account → Access Control (IAM) → Add role assignment
  - Role: **Storage Queue Data Contributor**
  - Assign to: The Snowflake service principal

---

## 3. Azure Event Hubs (for Snowpipe Streaming)

- [ ] Event Hubs namespace: `snowflake-demo-eh`
  - Pricing tier: **Standard** (required for Kafka protocol support)
  - Throughput units: 1
  - Enable Kafka: Yes (automatic with Standard tier)
- [ ] Event Hub (topic): `iot-sensor-stream`
  - Partition count: **4**
  - Message retention: 1 day
- [ ] Shared access policy: `snowflake-consumer`
  - Claims: **Listen** + **Send**
  - Note down the **Connection string—primary key**
- [ ] Bootstrap server: `<namespace>.servicebus.windows.net:9093`

---

## 4. Azure Data Factory

- [ ] ADF instance: `adf-snowflake-demo`
- [ ] Integration runtime: **Azure IR** (auto-resolve)

### Linked Services
| Name | Type | Config |
|------|------|--------|
| `LS_Snowflake` | Snowflake V2 | Account identifier, warehouse: `DEMO_WH`, database: `MSFT_SNOWFLAKE_DEMO`, basic auth |
| `LS_AzureBlobStorage` | Azure Blob Storage | Connection to `snowflakedemostore`, SAS auth |

### Pipelines
| Pipeline | Source | Sink | Notes |
|----------|--------|------|-------|
| `PL_Ingest_Inventory` | CSV in `adf-staging/inventory/` | `STAGING.ADF_INVENTORY` | Copy activity with column mapping |
| `PL_Ingest_Suppliers` | CSV in `adf-staging/suppliers/` | `STAGING.ADF_SUPPLIER_DATA` | Copy activity with column mapping |

### ADF Snowflake V2 Connector Notes
- Uses `COPY INTO` command internally for high performance
- Requires Azure Blob Storage as staging area
- Supports key pair authentication (recommended) or basic auth
- Account identifier format: `<org>-<account>` (no `.snowflakecomputing.com`)

---

## 5. Azure Virtual Machine (Kafka Connector for Snowpipe Streaming)

- [ ] VM name: `vm-kafka-connector`
- [ ] Size: **Standard D2s v3** (2 vCPUs, 8 GB RAM)
- [ ] Image: Red Hat Enterprise Linux 8+ or Ubuntu 22.04
- [ ] Network: Allow SSH (port 22), outbound HTTPS (port 443)
- [ ] Disk: 30 GB standard SSD

### Software to Install
```bash
# Java
sudo yum install -y java-1.8.0-openjdk-devel   # RHEL
# sudo apt install -y openjdk-11-jdk             # Ubuntu

# Kafka
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar xvfz kafka_2.12-2.8.1.tgz

# Snowflake Kafka Connector + Streaming SDK
wget https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/2.2.1/snowflake-kafka-connector-2.2.1.jar
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-ingest-sdk/2.1.0/snowflake-ingest-sdk-2.1.0.jar
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.5/snowflake-jdbc-3.14.5.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar
```

### RSA Key Pair for Snowflake Auth
```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
# Copy contents of rsa_key.pub (without header/footer) for Snowflake ALTER USER
```

---

## 6. Microsoft Fabric Workspace

- [ ] Fabric capacity: **F2** or higher
- [ ] Workspace: `snowflake-demo-workspace`

### Admin Portal Settings
- [ ] Admin Portal → Tenant Settings → Enable **Snowflake database** feature
- [ ] Admin Portal → Tenant Settings → Enable **OneLake table APIs**

### Cloud Connection to Snowflake
- [ ] Manage Connections & Gateways → New Connection → Snowflake
  - Server: `<org>-<account>.snowflakecomputing.com`
  - Authentication: Basic (use `FABRIC_SERVICE` user from Snowflake)
  - Database: `MSFT_SNOWFLAKE_DEMO`
- [ ] Grant Snowflake-managed service principal as **Workspace Contributor**
  - Get the service principal from: `DESC EXTERNAL VOLUME ONELAKE_EXTERNAL_VOL;`
  - Copy `AZURE_MULTI_TENANT_APP_NAME`
  - Fabric workspace → Manage access → Add member → paste service principal name

---

## 7. Microsoft Foundry (Azure AI Foundry)

- [ ] Azure AI Foundry hub and project in the same region
- [ ] Model deployments:
  - **GPT-4.1** (or GPT-5.2) — for agent orchestration
  - **text-embedding-ada-002** — for embeddings (if needed)

### Python SDK Setup
```bash
pip install azure-ai-projects azure-ai-agents azure-identity openai httpx
```

### MCP Connection to Snowflake
- URL format:
  ```
  https://<org>-<account>.snowflakecomputing.com/api/v2/databases/MSFT_SNOWFLAKE_DEMO/schemas/AGENTS/mcp-servers/<MCP_SERVER_NAME>
  ```
- Auth options:
  - **PAT** (Programmatic Access Token) — for development/testing
  - **OAuth** — for production / Claude Desktop integration

### Foundry Agent Service
- [ ] Enable Agent Service in your Foundry project
- [ ] Configure MCP tool connection to Snowflake MCP Server

---

## 8. Microsoft Entra ID (Azure AD)

- [ ] **Tenant ID** — needed for storage integration, notification integration, external volume
  - Find at: Entra ID → Overview → Tenant ID
- [ ] **App Registration** (if using OAuth for MCP):
  - Redirect URI: `https://login.microsoftonline.com/common/oauth2/nativeclient`
  - API permissions: User.Read
- [ ] **Service Principals** created automatically by Snowflake integrations

---

## 9. Network Configuration

For this demo, **public endpoints** are sufficient:
- [ ] Ensure Snowflake account can reach Azure services over the internet
- [ ] No private link configuration needed
- [ ] If corporate firewall: whitelist `*.blob.core.windows.net`, `*.queue.core.windows.net`, `*.servicebus.windows.net`, `*.dfs.fabric.microsoft.com`

---

## Quick Reference: Values to Collect

| Value | Where to Find | Used In |
|-------|--------------|---------|
| Azure Tenant ID | Entra ID → Overview | Storage integration, notification integration, external volume |
| Storage Account Name | Storage account → Overview | Stage URLs, external volume |
| Storage Queue URI | Storage account → Queues → Properties | Notification integration |
| Event Hub Connection String | Event Hub namespace → Shared access policies | Kafka connector config |
| Event Hub Bootstrap Server | `<namespace>.servicebus.windows.net:9093` | Kafka connector config |
| Snowflake Account Identifier | Snowflake → Admin → Accounts | ADF, Fabric, Foundry connections |
| Snowflake Service Principal | `DESC STORAGE INTEGRATION AZURE_STORAGE_INT` | Azure IAM grants |
| Fabric Workspace ID | Fabric → Workspace settings → About | External volume URL |
| Foundry Project Endpoint | Foundry portal → Project → Settings | Python SDK config |
| RSA Public Key | `cat rsa_key.pub` on VM | Snowflake ALTER USER |
