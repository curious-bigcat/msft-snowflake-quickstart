# Azure Prerequisites — Detailed Step-by-Step Setup Guide

This guide walks you through every Azure resource needed for the Microsoft + Snowflake hands-on lab. It is written for someone **new to Microsoft Azure** — every click is documented.

**Time to complete:** Plan for 45–60 minutes to set up everything.

---

## Table of Contents

1. [Sign In to Azure Portal](#1-sign-in-to-azure-portal)
2. [Create a Resource Group](#2-create-a-resource-group)
3. [Find Your Microsoft Entra ID Tenant ID](#3-find-your-microsoft-entra-id-tenant-id)
4. [Create a Storage Account (ADLS Gen2)](#4-create-a-storage-account-adls-gen2)
   - [4a. Create the Storage Account](#4a-create-the-storage-account)
   - [4b. Create a Container](#4b-create-a-container)
   - [4c. Grant Snowflake Access (Blob)](#4c-grant-snowflake-access-to-the-storage-account-after-snowflake-setup)
   - [4d. Create a Storage Queue](#4d-create-a-storage-queue-for-snowpipe-auto-ingest)
   - [4e. Set Up Event Grid Subscription](#4e-set-up-event-grid-subscription-blob-created--queue)
   - [4f. Grant Snowpipe Access to Queue](#4f-grant-snowpipe-integration-access-to-the-queue-after-snowflake-setup)
5. [Create a Microsoft Fabric Workspace and Lakehouse](#5-create-a-microsoft-fabric-workspace-and-lakehouse)
6. [Register an Entra ID Application (for Fabric Integration)](#6-register-an-entra-id-application-for-fabric-integration)
7. [Set Up Azure AI Foundry (Hub, Project, Model)](#7-set-up-azure-ai-foundry-hub-project-model)
8. [Network and Firewall Notes](#8-network-and-firewall-notes)
9. [Consolidated Values Reference Sheet](#9-consolidated-values-reference-sheet)
10. [Cleanup — Delete Everything When Done](#10-cleanup--delete-everything-when-done)

---

## Key Concepts for Azure Beginners

| Term | What It Means |
|---|---|
| **Subscription** | Your billing account. All Azure resources cost money and charges go to your subscription. Think of it like a credit card on file. |
| **Resource Group** | A folder that holds related Azure resources. Everything for this lab goes in one resource group so you can delete it all at once when done. |
| **Region / Location** | The physical data center where your resources run. Pick one region and use it everywhere for lowest latency. |
| **Microsoft Entra ID** | Microsoft's identity service (formerly called Azure Active Directory / Azure AD). It manages users, apps, and permissions. |
| **Tenant ID** | A unique ID for your organization's Entra ID directory. Many Snowflake integrations need this value. |

---

## 1. Sign In to Azure Portal

The Azure Portal is the web-based management console for all Azure services.

1. Open your browser and go to **https://portal.azure.com**
2. Sign in with your Microsoft account (your work/school email or personal Microsoft account)
3. If you don't have an Azure subscription yet:
   - Click **Start free** or visit **https://azure.microsoft.com/free**
   - You get **$200 in free credits** for 30 days — more than enough for this lab
   - You need a credit card (for identity verification) but you won't be charged during the free trial
4. After signing in, you'll see the **Azure Portal Home** page with a search bar at the top

> **Tip:** The search bar at the top of the Azure Portal is your best friend. Whenever this guide says "search for X", use that search bar.

---

## 2. Create a Resource Group

A resource group is a container that holds all related Azure resources. Create one for this entire lab.

**Used by:** All phases (every Azure resource goes here)

### Steps

1. In the **search bar** at the top of the portal, type `Resource groups` and click the result
2. Click **+ Create** (blue button at the top left)
3. Fill in the **Create a resource group** form:

| Field | What to Enter |
|---|---|
| **Subscription** | Select your subscription from the dropdown (if you only have one, it's already selected) |
| **Resource group** | `rg-snowflake-demo` |
| **Region** | `East US 2` (recommended — this region has good availability for all services we need, including Fabric and AI Foundry) |

4. Click **Review + create** (blue button at the bottom)
5. Review the summary, then click **Create**
6. Wait a few seconds — you'll see a "Resource group created" notification

> **Record this value:**
> - Resource group name: `rg-snowflake-demo`
> - Region: `East US 2`

---

## 3. Find Your Microsoft Entra ID Tenant ID

Your Tenant ID is needed by multiple Snowflake integrations (storage integration, external volume). Let's find it now.

**Used by:** `01_setup/01_account_setup.sql` (storage integration, external volume)

### Steps

1. In the **search bar**, type `Microsoft Entra ID` and click the result
   - (If you see "Azure Active Directory" instead, that's the same thing — Microsoft renamed it)
2. You land on the **Overview** page
3. Look for **Tenant ID** — it's displayed on the right side under "Basic Information"
   - It looks like: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (a GUID)
4. Click the **copy icon** next to the Tenant ID to copy it to your clipboard

> **Record this value:**
> - **Tenant ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
> - This replaces `<your_azure_tenant_id>` in all Snowflake scripts

---

## 4. Create a Storage Account (ADLS Gen2)

Azure Data Lake Storage Gen2 (ADLS Gen2) is a cloud storage service for big data analytics. Snowflake uses it via a storage integration for external stages and data access.

**Used by:**
- `01_setup/01_account_setup.sql` — Storage integration (`AZURE_STORAGE_INT`), external stage (`RAW.ADLS_DATA_STAGE`), Snowpipe notification integration (`AZURE_SNOWPIPE_INT`)

### 4a. Create the Storage Account

1. In the **search bar**, type `Storage accounts` and click the result
2. Click **+ Create** (top left)
3. Fill in the **Basics** tab:

| Field | What to Enter |
|---|---|
| **Subscription** | Your subscription |
| **Resource group** | `rg-snowflake-demo` (select from dropdown) |
| **Storage account name** | `snowflakedemostore` (must be globally unique — if taken, try `snowflakedemo<yourinitials>`, all lowercase, no hyphens, 3-24 chars) |
| **Region** | `East US 2` |
| **Performance** | `Standard` (leave default) |
| **Redundancy** | `Locally-redundant storage (LRS)` (cheapest option, fine for a demo) |

4. Click **Next: Advanced** (bottom of page)
5. On the **Advanced** tab, find the **Data Lake Storage Gen2** section:
   - Check the box **Enable hierarchical namespace** — **this is critical**, it's what makes this ADLS Gen2 instead of regular blob storage
6. Leave all other Advanced settings as default
7. Click **Next: Networking** — leave defaults (public endpoint, all networks)
8. Click **Next: Data protection** — leave defaults
9. Click **Review + create**
10. Review the summary — verify that **Hierarchical namespace** says **Enabled**
11. Click **Create**
12. Wait 30-60 seconds for deployment to complete
13. Click **Go to resource**

> **Record this value:**
> - **Storage account name:** `snowflakedemostore` (or whatever you chose)
> - This replaces `<your_storage_account>` in Snowflake scripts

### 4b. Create a Container

Containers are top-level folders within a storage account.

1. You should be on the storage account page. In the left menu, click **Containers** (under "Data storage")
2. Click **+ Container** (top bar)
3. Name: `snowflake-data`
4. Leave **Public access level** as `Private` (default)
5. Click **Create**

> **Record this value:**
> - **Container name:** `snowflake-data`
> - This replaces `<your_container>` in Snowflake scripts

### 4c. Grant Snowflake Access to the Storage Account (After Snowflake Setup)

This step happens **after** you run `01_setup/01_account_setup.sql` in Snowflake. Come back here once you have the Snowflake service principal name.

1. In Snowflake (Snowsight), run:
   ```sql
   DESC STORAGE INTEGRATION AZURE_STORAGE_INT;
   ```
2. Find the row `AZURE_CONSENT_URL` — open that URL in your browser and click **Accept**
3. Find the row `AZURE_MULTI_TENANT_APP_NAME` — copy the value (it looks like `Snowflake_SFCRole_xxxxxxx`)
4. Go back to your **storage account** in Azure Portal
5. In the left menu, click **Access Control (IAM)**
6. Click **+ Add** → **Add role assignment**
7. On the **Role** tab:
   - Search for `Storage Blob Data Contributor`
   - Select it and click **Next**
8. On the **Members** tab:
   - Click **+ Select members**
   - In the search box, paste the `AZURE_MULTI_TENANT_APP_NAME` value
   - Select the Snowflake service principal from the results
   - Click **Select**
9. Click **Review + assign** → **Review + assign** again

> **What this does:** Granting `Storage Blob Data Contributor` lets Snowflake read and write files in your ADLS Gen2 storage account.

### 4d. Create a Storage Queue (for Snowpipe Auto-Ingest)

Snowpipe monitors a **Storage Queue** to know when new files arrive in ADLS. Azure sends queue messages automatically via Event Grid. This enables Snowpipe's `AUTO_INGEST = TRUE` mode — no manual triggers needed.

1. You should still be on your storage account page. In the left menu, scroll down to **Queues** (under "Data storage") and click it
2. Click **+ Queue** (top bar)
3. Name: `snowpipe-events`
4. Click **OK**

> **Record this value:**
> - **Queue name:** `snowpipe-events`
> - This replaces `<your_queue>` in `01_account_setup.sql`

### 4e. Set Up Event Grid Subscription (Blob Created → Queue)

Event Grid watches your storage account and automatically sends a message to the queue every time a new file (blob) is created. This is what triggers Snowpipe.

1. Go back to your **storage account** page
2. In the left menu, click **Events** (you may need to scroll — it's under the "Monitoring" or top section)
3. Click **+ Event Subscription** (top bar)
4. Fill in the form:

| Field | What to Enter |
|---|---|
| **Name** | `snowpipe-blob-created` |
| **Event Schema** | `Event Grid Schema` (default) |
| **System Topic Name** | `snowflake-storage-topic` (auto-fills or type this) |
| **Filter to Event Types** | Uncheck everything, then check only **Blob Created** |
| **Endpoint Type** | `Storage Queue` |

5. Click **Select an endpoint** (next to Endpoint Type)
   - **Subscription:** your subscription
   - **Storage account:** your storage account (`snowflakedemostore`)
   - **Queue:** `snowpipe-events` (the queue you just created)
   - Click **Confirm Selection**
6. Click **Create**

> **What this does:** Every time a file is uploaded to your storage account, Azure automatically sends a notification message to the `snowpipe-events` queue. Snowpipe polls that queue and loads the new file.

### 4f. Grant Snowpipe Integration Access to the Queue (After Snowflake Setup)

This step happens **after** you run `01_setup/01_account_setup.sql` in Snowflake and create `AZURE_SNOWPIPE_INT`. Come back here once you have the service principal name.

1. In Snowflake (Snowsight), run:
   ```sql
   DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT;
   ```
2. Find the row `AZURE_MULTI_TENANT_APP_NAME` — copy the service principal name
   (It looks like `Snowflake_SFCRole_xxxxxxx`)
3. Go back to your **storage account** in Azure Portal
4. In the left menu, click **Access Control (IAM)**
5. Click **+ Add** → **Add role assignment**
6. On the **Role** tab:
   - Search for `Storage Queue Data Contributor`
   - Select it and click **Next**
7. On the **Members** tab:
   - Click **+ Select members**
   - Paste the `AZURE_MULTI_TENANT_APP_NAME` value
   - Select the Snowflake service principal
   - Click **Select**
8. Click **Review + assign** → **Review + assign** again

> **What this does:** `Storage Queue Data Contributor` lets Snowflake's notification integration read and delete messages from the queue. Without this, Snowpipe cannot receive the "new file" notifications.

---

## 5. Create a Microsoft Fabric Workspace and Lakehouse

Microsoft Fabric is an all-in-one analytics platform. We use its OneLake storage for bidirectional Iceberg table sharing with Snowflake.

**Used by:**
- `01_setup/01_account_setup.sql` — External volume (`ONELAKE_EXTERNAL_VOL`)
- `07_fabric_integration/01_external_volume_onelake.sql` — External volume validation
- `07_fabric_integration/02_iceberg_tables_to_fabric.sql` — Writing Iceberg tables to OneLake
- `07_fabric_integration/03_catalog_integration_onelake.sql` — Reading Fabric tables from Snowflake
- `07_fabric_integration/04_fabric_setup_guide.md` — Detailed Fabric integration guide

### 5a. Access Microsoft Fabric

1. Open your browser and go to **https://app.fabric.microsoft.com**
2. Sign in with the **same Microsoft account** you use for Azure
3. If you've never used Fabric before:
   - You may need a **Fabric trial**: Click your profile icon (top right) → **Start trial**
   - The trial gives you an **F64 capacity** for 60 days — more than enough
   - If your organization has a Fabric capacity (F2+), you can use that instead

### 5b. Create a Workspace

A workspace is a collaborative space that holds all your Fabric items (lakehouses, notebooks, reports).

1. In the left sidebar, click **Workspaces**
2. Click **+ New workspace**
3. Fill in:

| Field | What to Enter |
|---|---|
| **Name** | `snowflake-demo-workspace` |
| **Description** | (optional) `Workspace for Snowflake integration demo` |

4. Expand **Advanced** settings:
   - **License mode**: Select your Fabric capacity (Trial or a paid capacity like F2+)
5. Click **Apply**

#### Find the Workspace ID

1. You're now inside the workspace
2. Click the **gear icon** (Settings) for the workspace — or click **Workspace settings** in the top bar
3. Look at the URL in your browser: `https://app.fabric.microsoft.com/groups/<workspace_id>/...`
4. The GUID after `/groups/` is your **Workspace ID**
   - Alternatively, in Settings → **About**, you may see the ID directly

> **Record this value:**
> - **Workspace ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
> - This replaces `<your_workspace_id>` in Snowflake scripts

### 5c. Create a Lakehouse

A lakehouse is Fabric's data storage layer, backed by OneLake (which speaks the Delta/Iceberg format).

1. Inside your workspace, click **+ New item**
2. Select **Lakehouse**
3. Name: `demo_lakehouse`
4. Click **Create**
5. You'll see the lakehouse explorer with **Tables** and **Files** sections

#### Find the Lakehouse ID

1. Click the **gear icon** or open Settings for the lakehouse
2. The Lakehouse ID is in the URL: `https://app.fabric.microsoft.com/groups/<workspace_id>/lakehouses/<lakehouse_id>`
3. Copy the GUID after `/lakehouses/`

> **Record this value:**
> - **Lakehouse ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
> - This is needed for the catalog namespace in Snowflake

### 5d. Enable Required Admin Settings

If you're a Fabric administrator (or can ask one), enable these settings:

1. Click the **gear icon** (top right of Fabric) → **Admin portal**
2. Go to **Tenant settings**
3. Search for and enable:
   - **Snowflake database** — allows Snowflake to connect to OneLake
   - **OneLake table APIs** — allows external services to read/write tables
4. Click **Apply** for each setting

> **Note:** If you don't have admin access, ask your Fabric administrator to enable these. Without them, the Snowflake external volume cannot access OneLake.

### 5e. Create Sample Data in Fabric (Optional)

To test the Fabric → Snowflake direction, create sample tables in the lakehouse. Follow the PySpark notebook instructions in `07_fabric_integration/04_fabric_setup_guide.md` (Step 5).

---

## 6. Register an Entra ID Application (for Fabric Integration)

Snowflake's catalog integration with Fabric uses OAuth for authentication. This requires an Entra ID app registration.

**Used by:** `07_fabric_integration/03_catalog_integration_onelake.sql` — Catalog integration

### 6a. Create the App Registration

1. In the Azure Portal (**https://portal.azure.com**), search for `App registrations` and click the result
   - This is under Microsoft Entra ID → App registrations
2. Click **+ New registration**
3. Fill in:

| Field | What to Enter |
|---|---|
| **Name** | `snowflake-fabric-integration` |
| **Supported account types** | `Accounts in this organizational directory only (Single tenant)` |
| **Redirect URI** | Leave blank (not needed for this scenario) |

4. Click **Register**
5. You land on the app's **Overview** page

> **Record these values:**
> - **Application (client) ID:** `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` (shown on Overview page)
> - This replaces `<entra_app_client_id>` in Snowflake scripts

### 6b. Create a Client Secret

1. In the left menu of your app registration, click **Certificates & secrets**
2. Click **+ New client secret**
3. Fill in:

| Field | What to Enter |
|---|---|
| **Description** | `snowflake-access` |
| **Expires** | `12 months` (or choose based on your needs) |

4. Click **Add**
5. **Immediately copy the Value** (not the Secret ID) — it is shown only once and cannot be retrieved later

> **Record this value:**
> - **Client secret:** `your_secret_value_here`
> - This replaces `<entra_app_client_secret>` in Snowflake scripts

### 6c. Grant the App Access to Fabric Workspace

1. Go back to **Microsoft Fabric** (https://app.fabric.microsoft.com)
2. Open your `snowflake-demo-workspace`
3. Click **Manage access** (top right, or in workspace settings)
4. Click **+ Add people or groups**
5. Search for `snowflake-fabric-integration` (the app registration name)
6. Set role to **Contributor**
7. Click **Add**

> **What this does:** The app registration acts as an identity that Snowflake uses to authenticate with Fabric. Granting Contributor role lets it read and discover tables in the workspace.

---

## 7. Set Up Azure AI Foundry (Hub, Project, Model)

Azure AI Foundry is Microsoft's platform for building AI applications and agents. We use it to create AI agents that connect to Snowflake via MCP.

**Used by:** `08_multi_agent_orchestration/01_foundry_agent_setup.md` — Creating Foundry agents

### 7a. Create an AI Foundry Hub (via Azure Portal)

The Hub is the top-level resource that provides shared infrastructure (networking, storage, identity) for AI projects.

1. In the **Azure Portal search bar**, type `Azure AI Foundry` and click the result
2. Click **+ Create** → select **Hub**
3. Fill in the **Basics** tab:

| Field | What to Enter |
|---|---|
| **Subscription** | Your subscription |
| **Resource group** | `rg-snowflake-demo` |
| **Hub name** | `snowflake-demo-ai-hub` |
| **Region** | `East US 2` |

4. Click **Next: Resources**
   - **Create new AI Services resource** — Azure creates an AI Services resource automatically
   - Leave Storage, Key Vault, etc. as auto-generated defaults
5. Click **Review + create** → **Create**
6. Wait for deployment (3-5 minutes — this creates several sub-resources)
7. Click **Go to resource**

### 7b. Create a Project Under the Hub

1. On the Hub resource page, click **Launch Azure AI Foundry** (blue button)
   - This opens **https://ai.azure.com** in a new tab
2. You should see your hub. Click **+ New project** (or find it in the Hub overview)
3. Fill in:

| Field | What to Enter |
|---|---|
| **Project name** | `snowflake-demo-project` |
| **Hub** | Your hub (should be pre-selected) |

4. Click **Create project**
5. Wait for the project to be created — you'll be redirected to the project page

### 7c. Deploy a Model

You need a deployed LLM model for your agents to use.

1. In your project, click **Models + endpoints** in the left nav (or **Model catalog**)
2. Click **+ Deploy model** → **Deploy base model**
3. Search for `gpt-4o-mini` (good balance of capability and cost)
4. Click **Confirm** (or **Deploy**)
5. Configure:

| Field | What to Enter |
|---|---|
| **Deployment name** | `gpt-4o-mini` (keep default) |
| **Model version** | Latest available |
| **Tokens per minute rate limit** | `30K` (default is fine) |

6. Click **Deploy**
7. Wait for deployment to complete (1-2 minutes)

> **Record this value:**
> - **Model deployment name:** `gpt-4o-mini`
> - Used when creating agents in `08_multi_agent_orchestration/01_foundry_agent_setup.md`

### 7d. Next Steps

Once the model is deployed, follow `08_multi_agent_orchestration/01_foundry_agent_setup.md` to:
- Connect the Snowflake MCP Server as a resource
- Create 4 AI agents (Data Analyst, Customer Insights, Fabric Data, Orchestrator)
- Test the agents in the Foundry Playground

---

## 8. Network and Firewall Notes

For this demo, **public endpoints** are used for all Azure services. No VPN, private link, or virtual network peering is required.

If you are behind a **corporate firewall** or proxy, ensure outbound access to:

| Endpoint | Used By |
|---|---|
| `*.blob.core.windows.net` | Storage account (ADLS Gen2) |
| `*.dfs.core.windows.net` | ADLS Gen2 hierarchical namespace |
| `*.dfs.fabric.microsoft.com` | OneLake (Fabric integration) |
| `*.snowflakecomputing.com` | Snowflake account |
| `ai.azure.com` | Azure AI Foundry portal |
| `portal.azure.com` | Azure Portal |
| `app.fabric.microsoft.com` | Microsoft Fabric portal |

If your Snowflake account uses **network policies**, ensure the Azure service IPs are allowed. For this demo, this should not be an issue.

---

## 9. Consolidated Values Reference Sheet

Copy this table and fill in your values. Every `<placeholder>` in the lab scripts maps to one of these.

| # | Value | Your Value | Where to Find It | Used In (Script File) |
|---|---|---|---|---|
| 1 | **Azure Tenant ID** | `________________` | Section 3 (Entra ID → Overview) | `01_account_setup.sql` (storage integration, notification integration, external volume) |
| 2 | **Storage Account Name** | `________________` | Section 4a (storage account creation) | `01_account_setup.sql` (ADLS stage URL, notification integration queue URI) |
| 3 | **Storage Container Name** | `snowflake-data` | Section 4b (container creation) | `01_account_setup.sql` (ADLS stage URL) |
| 4 | **Snowflake Storage Service Principal** | `________________` | Run `DESC STORAGE INTEGRATION AZURE_STORAGE_INT;` → `AZURE_MULTI_TENANT_APP_NAME` | Section 4c (Storage Blob Data Contributor IAM grant) |
| 5 | **Storage Queue Name** | `snowpipe-events` | Section 4d (queue creation) | `01_account_setup.sql` (notification integration queue URI) |
| 6 | **Snowpipe Service Principal** | `________________` | Run `DESC NOTIFICATION INTEGRATION AZURE_SNOWPIPE_INT;` → `AZURE_MULTI_TENANT_APP_NAME` | Section 4f (Storage Queue Data Contributor IAM grant) |
| 7 | **Fabric Workspace ID** | `________________` | Section 5b (workspace URL) | `01_account_setup.sql` (external volume), `03_catalog_integration_onelake.sql` |
| 8 | **Fabric Lakehouse ID** | `________________` | Section 5c (lakehouse URL) | `03_catalog_integration_onelake.sql` (catalog namespace) |
| 9 | **Entra App Client ID** | `________________` | Section 6a (app registration Overview) | `03_catalog_integration_onelake.sql` (catalog integration) |
| 10 | **Entra App Client Secret** | `________________` | Section 6b (client secret creation) | `03_catalog_integration_onelake.sql` (catalog integration) |
| 11 | **Snowflake Account Identifier** | `________________` | Snowsight → Admin → Accounts → `<org>-<account>` | Fabric connection, MCP server URL |
| 12 | **AI Foundry Model Deployment** | `________________` | Section 7c (model deployment) | `01_foundry_agent_setup.md` (agent model selection) |
| 13 | **Snowflake PAT** | `________________` | Snowsight → Profile → Preferences → Authentication → Generate PAT | `01_foundry_agent_setup.md` (MCP connection token) |

> **Security note:** Values #10 and #13 are secrets. Do not commit them to source control or share them in plain text.

---

## 10. Cleanup — Delete Everything When Done

When you're finished with the lab, delete the resource group to remove **all** Azure resources at once.

### Delete via Azure Portal

1. In the **search bar**, type `Resource groups` and click the result
2. Click on `rg-snowflake-demo`
3. Click **Delete resource group** (top bar)
4. Type the resource group name `rg-snowflake-demo` to confirm
5. Click **Delete**
6. Wait 5-10 minutes for all resources to be removed

> **What this deletes:** The storage account, AI Foundry hub, and all sub-resources. This is why we put everything in one resource group.

### Delete the Fabric Workspace (Separate)

Fabric workspaces are not inside Azure resource groups, so they must be deleted separately.

1. Go to **https://app.fabric.microsoft.com**
2. Click **Workspaces** → hover over `snowflake-demo-workspace`
3. Click the **...** (more options) menu → **Workspace settings**
4. Scroll to the bottom → **Remove this workspace**
5. Confirm deletion

### Delete the Entra ID App Registration (Separate)

1. In Azure Portal, search for `App registrations`
2. Click `snowflake-fabric-integration`
3. Click **Delete** (top bar) → **Yes**

### Estimated Costs for This Lab

If you use the recommended configurations and run the lab for a few hours:

| Resource | Estimated Cost |
|---|---|
| Storage account (LRS, minimal data) | < $1 |
| AI Foundry (GPT-4o-mini tokens) | ~$0.50-2.00 depending on usage |
| Fabric (trial is free) | $0 during trial |
| **Total for a 1-day lab** | **~$1-3** |
