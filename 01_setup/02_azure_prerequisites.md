# Azure Prerequisites — Detailed Step-by-Step Setup Guide

This guide walks you through every Azure resource needed for the Microsoft + Snowflake hands-on lab. It is written for someone **new to Microsoft Azure** — every click is documented.

**Time to complete:** Plan for 60–90 minutes to set up everything.

---

## Table of Contents

1. [Sign In to Azure Portal](#1-sign-in-to-azure-portal)
2. [Create a Resource Group](#2-create-a-resource-group)
3. [Find Your Microsoft Entra ID Tenant ID](#3-find-your-microsoft-entra-id-tenant-id)
4. [Create a Storage Account (ADLS Gen2)](#4-create-a-storage-account-adls-gen2)
5. [Create Azure Data Factory](#5-create-azure-data-factory)
6. [Create Azure Event Hubs](#6-create-azure-event-hubs)
7. [Create an Azure Virtual Machine (Kafka Connector)](#7-create-an-azure-virtual-machine-kafka-connector)
8. [Create a Microsoft Fabric Workspace and Lakehouse](#8-create-a-microsoft-fabric-workspace-and-lakehouse)
9. [Register an Entra ID Application (for Fabric Integration)](#9-register-an-entra-id-application-for-fabric-integration)
10. [Set Up Azure AI Foundry (Hub, Project, Model)](#10-set-up-azure-ai-foundry-hub-project-model)
11. [Network and Firewall Notes](#11-network-and-firewall-notes)
12. [Consolidated Values Reference Sheet](#12-consolidated-values-reference-sheet)
13. [Cleanup — Delete Everything When Done](#13-cleanup--delete-everything-when-done)

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

Your Tenant ID is needed by multiple Snowflake integrations (storage integration, notification integration, external volume). Let's find it now.

**Used by:** `01_setup/01_account_setup.sql` (storage integration, notification integration, external volume)

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

Azure Data Lake Storage Gen2 (ADLS Gen2) is a cloud storage service for big data analytics. We use it as the landing zone for Snowpipe (auto-ingest) and as a staging area for Azure Data Factory.

**Used by:**
- `01_setup/01_account_setup.sql` — Storage integration (`AZURE_STORAGE_INT`) and notification integration
- `03_ingestion/02_snowpipe_adls.sql` — External stage and Snowpipe
- `03_ingestion/04_adf_pipeline_config.md` — ADF blob storage linked service

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

### 4b. Create Containers

Containers are top-level folders within a storage account. We need three.

1. You should be on the storage account page. In the left menu, click **Containers** (under "Data storage")
2. Click **+ Container** (top bar)
3. Create the following containers one at a time:

| Container Name | Purpose |
|---|---|
| `raw-data` | Snowpipe auto-ingest reads clickstream JSON files from here |
| `adf-staging` | ADF pipeline staging area and source CSV files |
| `export-data` | Data exported from Snowflake (optional) |

For each container:
- Type the name in the **Name** field
- Leave **Public access level** as `Private` (default)
- Click **Create**

### 4c. Upload Sample CSV Files for ADF

ADF needs source files to ingest. Let's upload them now.

1. Click on the **`adf-staging`** container
2. Click **+ Add Directory** (or just upload into folder paths — Azure will create directories automatically)
3. Click **Upload** (top bar)
4. For the inventory data:
   - Click **Advanced** to expand options
   - In **Upload to folder**, type: `inventory`
   - Create a file called `inventory_data.csv` on your local machine with this content:
     ```csv
     INVENTORY_ID,PRODUCT_ID,WAREHOUSE_LOCATION,QUANTITY_ON_HAND,QUANTITY_RESERVED,REORDER_POINT,LAST_RESTOCK_DATE,SUPPLIER_ID
     1,42,Warehouse-East,1500,120,200,2026-02-15,5
     2,87,Warehouse-West,3200,450,300,2026-03-01,12
     3,15,Warehouse-Central,800,50,100,2026-01-20,3
     4,123,Distribution-Hub-1,5000,800,500,2026-03-10,8
     5,56,Warehouse-South,250,30,50,2026-02-28,15
     ```
   - Browse to the file and click **Upload**
5. For the supplier data:
   - Click **Upload** again
   - In **Upload to folder**, type: `suppliers`
   - Create a file called `supplier_data.csv` on your local machine with this content:
     ```csv
     SUPPLIER_ID,SUPPLIER_NAME,CONTACT_EMAIL,COUNTRY,LEAD_TIME_DAYS,RELIABILITY_SCORE,CONTRACT_START_DATE,CONTRACT_END_DATE
     1,Global Technologies,contact@globaltech.com,United States,7,0.95,2025-01-01,2027-01-01
     2,Pacific Solutions,info@pacificsol.com,Japan,14,0.88,2025-06-01,2026-12-01
     3,Continental Systems,sales@contisys.com,Germany,10,0.92,2024-09-01,2026-09-01
     ```
   - Browse to the file and click **Upload**

### 4d. Enable Event Grid and Create a Storage Queue (for Snowpipe)

Snowpipe uses Azure Event Grid notifications to know when new files arrive. The notifications go to a Storage Queue that Snowflake polls.

#### Create the Storage Queue

1. Go back to your storage account page (click `snowflakedemostore` in the breadcrumb at the top)
2. In the left menu, click **Queues** (under "Data storage")
3. Click **+ Queue** (top bar)
4. Name: `snowpipe-notifications`
5. Click **OK**

> **Record this value:**
> - **Queue URI:** `https://snowflakedemostore.queue.core.windows.net/snowpipe-notifications`
> - This replaces `<your_queue_name>` in the notification integration

#### Create an Event Grid Subscription

This connects blob events (file uploads) to the storage queue.

1. Go back to your storage account page
2. In the left menu, click **Events** (under "Settings" — you may need to scroll down)
3. Click **+ Event Subscription** (top bar)
4. Fill in the form:

| Field | What to Enter |
|---|---|
| **Name** | `snowpipe-blob-events` |
| **Event Schema** | `Event Grid Schema` (default) |
| **System Topic Name** | `snowpipe-topic` (auto-generated if you leave it blank) |
| **Filter to Event Types** | Check **only** `Blob Created` (uncheck all others) |
| **Endpoint Type** | Select `Storage Queue` from the dropdown |
| **Endpoint** | Click **Select an endpoint** → select your subscription → storage account `snowflakedemostore` → queue `snowpipe-notifications` → click **Confirm Selection** |

5. Click the **Filters** tab
6. Check **Enable subject filtering**
7. Set:
   - **Subject Begins With:** `/blobServices/default/containers/raw-data`
   - **Subject Ends With:** (leave blank)
   - This ensures only files uploaded to the `raw-data` container trigger notifications
8. Click **Create**

> **What this does:** When you upload a JSON file to the `raw-data` container, Azure Event Grid sends a notification to the `snowpipe-notifications` queue. Snowflake's notification integration polls this queue and triggers Snowpipe to load the file.

### 4e. Grant Snowflake Access to the Storage Account (After Snowflake Setup)

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
10. Repeat steps 6-9 for a second role:
    - Role: `Storage Queue Data Contributor`
    - Same member (Snowflake service principal)
    - This allows Snowflake to read the notification queue

> **Why two roles?**
> - `Storage Blob Data Contributor` — lets Snowflake read files from ADLS
> - `Storage Queue Data Contributor` — lets Snowflake read Snowpipe notifications from the queue

---

## 5. Create Azure Data Factory

Azure Data Factory (ADF) is a cloud ETL/ELT service. We use it to copy CSV data from blob storage into Snowflake tables.

**Used by:** `03_ingestion/04_adf_pipeline_config.md` — ADF pipelines and linked services

### 5a. Create the ADF Instance

1. In the **search bar**, type `Data factories` and click the result
2. Click **+ Create** (top left)
3. Fill in the **Basics** tab:

| Field | What to Enter |
|---|---|
| **Subscription** | Your subscription |
| **Resource group** | `rg-snowflake-demo` |
| **Name** | `adf-snowflake-demo` (must be globally unique — add your initials if taken) |
| **Region** | `East US 2` |
| **Version** | `V2` (should be default) |

4. Click **Next: Git configuration**
   - Select **Configure Git later** (for this demo we don't need source control)
5. Click **Next: Networking** — leave defaults
6. Click **Review + create** → **Create**
7. Wait for deployment (1-2 minutes)
8. Click **Go to resource**

### 5b. Open ADF Studio

1. On the Data Factory resource page, click **Launch Studio** (big blue button)
   - This opens **ADF Studio** in a new tab — this is where you build pipelines
2. You'll see the ADF Studio home page with icons for Author, Monitor, Manage, etc.

### 5c. Create Linked Services

Linked services are connections to external systems. You need two: one to Snowflake, one to Blob Storage.

#### Snowflake V2 Linked Service

1. In ADF Studio, click the **Manage** icon (wrench) in the left nav
2. Click **Linked services** → **+ New**
3. In the search box, type `Snowflake` → select **Snowflake V2** → click **Continue**
4. Fill in:

| Field | What to Enter |
|---|---|
| **Name** | `LS_Snowflake_Demo` |
| **Account identifier** | Your Snowflake account identifier in `<org>-<account>` format (e.g., `myorg-myaccount`). Find this in Snowsight under Admin → Accounts |
| **Database** | `MSFT_SNOWFLAKE_DEMO` |
| **Warehouse** | `DEMO_WH` |
| **Role** | `DEMO_ADMIN` |
| **Authentication type** | `Basic` |
| **User** | Your Snowflake username |
| **Password** | Your Snowflake password |

5. Click **Test connection** (bottom left) — should show "Connection successful"
6. Click **Create**

> **Note:** For production, use Azure Key Vault for the password instead of typing it directly. For this demo, basic auth is fine.

#### Azure Blob Storage Linked Service

1. Still in Manage → Linked services, click **+ New**
2. Search `Azure Blob Storage` → select it → **Continue**
3. Fill in:

| Field | What to Enter |
|---|---|
| **Name** | `LS_AzureBlobStorage` |
| **Authentication method** | `Account key` |
| **Storage account name** | Select `snowflakedemostore` from the dropdown (or enter manually) |

4. Click **Test connection** → **Create**

### 5d. Build the Pipelines

Follow the detailed pipeline setup in `03_ingestion/04_adf_pipeline_config.md`. That guide covers:
- Creating source (CSV) and sink (Snowflake) datasets
- Building `PL_Ingest_Inventory` and `PL_Ingest_Suppliers` pipelines
- Creating the orchestrator pipeline `PL_Master_Ingestion`
- Running and monitoring the pipelines

---

## 6. Create Azure Event Hubs

Azure Event Hubs is a real-time data streaming service. We use it as a Kafka-compatible message broker for IoT sensor data that flows into Snowflake via Snowpipe Streaming.

**Used by:** `03_ingestion/03_snowpipe_streaming_eventhubs.sql` — Kafka connector configuration

### 6a. Create the Event Hubs Namespace

A namespace is a container for one or more event hubs (topics).

1. In the **search bar**, type `Event Hubs` and click the result
2. Click **+ Create** (top left)
3. Fill in the **Basics** tab:

| Field | What to Enter |
|---|---|
| **Subscription** | Your subscription |
| **Resource group** | `rg-snowflake-demo` |
| **Namespace name** | `snowflake-demo-eh` (must be globally unique — add your initials if taken) |
| **Location** | `East US 2` |
| **Pricing tier** | **Standard** (required for Kafka protocol support — Basic does NOT work) |
| **Throughput Units** | `1` (default, sufficient for this demo) |

4. Click **Review + create** → **Create**
5. Wait for deployment (1-2 minutes)
6. Click **Go to resource**

> **Why Standard tier?** The Snowflake Kafka Connector uses the Kafka protocol to consume messages from Event Hubs. Only Standard tier and above support the Kafka endpoint.

### 6b. Create an Event Hub (Topic)

1. On the Event Hubs Namespace page, click **+ Event Hub** (top bar)
2. Fill in:

| Field | What to Enter |
|---|---|
| **Name** | `iot-sensor-stream` |
| **Partition count** | `4` |
| **Message retention** | `1` day |

3. Click **Review + create** → **Create**

### 6c. Create a Shared Access Policy (for Kafka Authentication)

1. In the left menu of the namespace page, click **Shared access policies** (under "Settings")
2. Click **+ Add**
3. Fill in:

| Field | What to Enter |
|---|---|
| **Policy name** | `snowflake-consumer` |
| **Manage** | Unchecked |
| **Send** | Checked |
| **Listen** | Checked |

4. Click **Create**
5. Click on the new `snowflake-consumer` policy
6. Copy the **Connection string—primary key** value

> **Record these values:**
> - **Event Hub namespace:** `snowflake-demo-eh`
> - **Bootstrap server:** `snowflake-demo-eh.servicebus.windows.net:9093`
> - **Connection string:** `Endpoint=sb://snowflake-demo-eh.servicebus.windows.net/;SharedAccessKeyName=snowflake-consumer;SharedAccessKey=...`
> - These replace `<your_eventhub_namespace>` and `<your_eventhub_connection_string>` in Snowflake scripts

---

## 7. Create an Azure Virtual Machine (Kafka Connector)

The Snowflake Kafka Connector runs on a VM and bridges Event Hubs to Snowpipe Streaming. It reads from Event Hubs using the Kafka protocol and writes to Snowflake using the Snowpipe Streaming SDK.

**Used by:** `03_ingestion/03_snowpipe_streaming_eventhubs.sql` — Kafka connector setup and config files

### 7a. Create the VM

1. In the **search bar**, type `Virtual machines` and click the result
2. Click **+ Create** → **Azure virtual machine**
3. Fill in the **Basics** tab:

| Field | What to Enter |
|---|---|
| **Subscription** | Your subscription |
| **Resource group** | `rg-snowflake-demo` |
| **Virtual machine name** | `vm-kafka-connector` |
| **Region** | `East US 2` |
| **Availability options** | `No infrastructure redundancy required` |
| **Security type** | `Standard` |
| **Image** | `Ubuntu Server 22.04 LTS` (click "See all images" if not visible) |
| **VM architecture** | `x64` |
| **Size** | Click **See all sizes** → search `D2s_v3` → select **Standard_D2s_v3** (2 vCPUs, 8 GB RAM) |
| **Authentication type** | `SSH public key` (recommended) or `Password` |
| **Username** | `azureuser` |
| **SSH public key source** | `Generate new key pair` (if using SSH) |

4. Click **Next: Disks**
   - OS disk size: `30 GiB` (default is fine)
   - OS disk type: `Standard SSD` 
5. Click **Next: Networking**
   - Public IP: Make sure one is assigned (default: yes)
   - NIC network security group: `Basic`
   - Public inbound ports: `Allow selected ports`
   - Select inbound ports: `SSH (22)` — you need this to connect to the VM
6. Click **Review + create** → **Create**
7. If you chose "Generate new key pair": a dialog appears — click **Download private key and create resource**
   - Save the `.pem` file somewhere safe — you need it to SSH into the VM
8. Wait for deployment (2-3 minutes)
9. Click **Go to resource**

### 7b. Connect to the VM

1. On the VM overview page, find the **Public IP address** and copy it
2. Open a terminal on your local machine:
   ```bash
   chmod 400 ~/Downloads/vm-kafka-connector_key.pem   # Restrict key permissions
   ssh -i ~/Downloads/vm-kafka-connector_key.pem azureuser@<PUBLIC_IP_ADDRESS>
   ```
3. Accept the fingerprint prompt by typing `yes`

### 7c. Install Software on the VM

Run these commands on the VM after SSH-ing in:

```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Install Java (required by Kafka)
sudo apt install -y openjdk-11-jdk
java -version   # Verify: should show openjdk 11.x

# Install Python (for the IoT data producer script)
sudo apt install -y python3 python3-pip
pip3 install kafka-python

# Create working directory
mkdir -p ~/snowpipe-streaming/scripts
cd ~/snowpipe-streaming

# Download Kafka
wget https://archive.apache.org/dist/kafka/2.8.1/kafka_2.12-2.8.1.tgz
tar xvfz kafka_2.12-2.8.1.tgz

# Download Snowflake Kafka Connector and dependencies
cd kafka_2.12-2.8.1/libs
wget https://repo1.maven.org/maven2/com/snowflake/snowflake-kafka-connector/2.2.1/snowflake-kafka-connector-2.2.1.jar
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-ingest-sdk/2.1.0/snowflake-ingest-sdk-2.1.0.jar
wget https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.5/snowflake-jdbc-3.14.5.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/1.0.1/bc-fips-1.0.1.jar
wget https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/1.0.3/bcpkix-fips-1.0.3.jar
cd ~/snowpipe-streaming
```

### 7d. Generate RSA Key Pair (for Snowflake Authentication)

Snowpipe Streaming requires key-pair authentication (password auth is not supported).

```bash
# Generate the private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out ~/snowpipe-streaming/rsa_key.p8 -nocrypt

# Extract the public key
openssl rsa -in ~/snowpipe-streaming/rsa_key.p8 -pubout -out ~/snowpipe-streaming/rsa_key.pub

# View the public key (you'll paste this into Snowflake)
cat ~/snowpipe-streaming/rsa_key.pub
```

> **Record this value:**
> - **RSA public key content** — copy everything between `-----BEGIN PUBLIC KEY-----` and `-----END PUBLIC KEY-----` (without those header/footer lines)
> - This is used in `03_ingestion/03_snowpipe_streaming_eventhubs.sql` to set `RSA_PUBLIC_KEY` on the `STREAMING_USER`

The Kafka connector configuration files are provided in `03_ingestion/03_snowpipe_streaming_eventhubs.sql` — follow that file to create `connect-standalone.properties` and `snowflakeconnectorEH.properties` using the values you've recorded.

---

## 8. Create a Microsoft Fabric Workspace and Lakehouse

Microsoft Fabric is an all-in-one analytics platform. We use its OneLake storage for bidirectional Iceberg table sharing with Snowflake.

**Used by:**
- `01_setup/01_account_setup.sql` — External volume (`ONELAKE_EXTERNAL_VOL`)
- `07_fabric_integration/01_external_volume_onelake.sql` — External volume validation
- `07_fabric_integration/02_iceberg_tables_to_fabric.sql` — Writing Iceberg tables to OneLake
- `07_fabric_integration/03_catalog_integration_onelake.sql` — Reading Fabric tables from Snowflake
- `07_fabric_integration/04_fabric_setup_guide.md` — Detailed Fabric integration guide

### 8a. Access Microsoft Fabric

1. Open your browser and go to **https://app.fabric.microsoft.com**
2. Sign in with the **same Microsoft account** you use for Azure
3. If you've never used Fabric before:
   - You may need a **Fabric trial**: Click your profile icon (top right) → **Start trial**
   - The trial gives you an **F64 capacity** for 60 days — more than enough
   - If your organization has a Fabric capacity (F2+), you can use that instead

### 8b. Create a Workspace

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

### 8c. Create a Lakehouse

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

### 8d. Enable Required Admin Settings

If you're a Fabric administrator (or can ask one), enable these settings:

1. Click the **gear icon** (top right of Fabric) → **Admin portal**
2. Go to **Tenant settings**
3. Search for and enable:
   - **Snowflake database** — allows Snowflake to connect to OneLake
   - **OneLake table APIs** — allows external services to read/write tables
4. Click **Apply** for each setting

> **Note:** If you don't have admin access, ask your Fabric administrator to enable these. Without them, the Snowflake external volume cannot access OneLake.

### 8e. Create Sample Data in Fabric (Optional)

To test the Fabric → Snowflake direction, create sample tables in the lakehouse. Follow the PySpark notebook instructions in `07_fabric_integration/04_fabric_setup_guide.md` (Step 5).

---

## 9. Register an Entra ID Application (for Fabric Integration)

Snowflake's catalog integration with Fabric uses OAuth for authentication. This requires an Entra ID app registration.

**Used by:** `07_fabric_integration/03_catalog_integration_onelake.sql` — Catalog integration

### 9a. Create the App Registration

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

### 9b. Create a Client Secret

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

### 9c. Grant the App Access to Fabric Workspace

1. Go back to **Microsoft Fabric** (https://app.fabric.microsoft.com)
2. Open your `snowflake-demo-workspace`
3. Click **Manage access** (top right, or in workspace settings)
4. Click **+ Add people or groups**
5. Search for `snowflake-fabric-integration` (the app registration name)
6. Set role to **Contributor**
7. Click **Add**

> **What this does:** The app registration acts as an identity that Snowflake uses to authenticate with Fabric. Granting Contributor role lets it read and discover tables in the workspace.

---

## 10. Set Up Azure AI Foundry (Hub, Project, Model)

Azure AI Foundry is Microsoft's platform for building AI applications and agents. We use it to create AI agents that connect to Snowflake via MCP.

**Used by:** `08_multi_agent_orchestration/01_foundry_agent_setup.md` — Creating Foundry agents

### 10a. Create an AI Foundry Hub (via Azure Portal)

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

### 10b. Create a Project Under the Hub

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

### 10c. Deploy a Model

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

### 10d. Next Steps

Once the model is deployed, follow `08_multi_agent_orchestration/01_foundry_agent_setup.md` to:
- Connect the Snowflake MCP Server as a resource
- Create 4 AI agents (Data Analyst, Customer Insights, Fabric Data, Orchestrator)
- Test the agents in the Foundry Playground

---

## 11. Network and Firewall Notes

For this demo, **public endpoints** are used for all Azure services. No VPN, private link, or virtual network peering is required.

If you are behind a **corporate firewall** or proxy, ensure outbound access to:

| Endpoint | Used By |
|---|---|
| `*.blob.core.windows.net` | Storage account (ADLS Gen2) |
| `*.queue.core.windows.net` | Storage queue (Snowpipe notifications) |
| `*.dfs.core.windows.net` | ADLS Gen2 hierarchical namespace |
| `*.servicebus.windows.net` | Event Hubs (Kafka endpoint) |
| `*.dfs.fabric.microsoft.com` | OneLake (Fabric integration) |
| `*.snowflakecomputing.com` | Snowflake account |
| `ai.azure.com` | Azure AI Foundry portal |
| `portal.azure.com` | Azure Portal |
| `app.fabric.microsoft.com` | Microsoft Fabric portal |

If your Snowflake account uses **network policies**, ensure the Azure service IPs are allowed. For this demo, this should not be an issue.

---

## 12. Consolidated Values Reference Sheet

Copy this table and fill in your values. Every `<placeholder>` in the lab scripts maps to one of these.

| # | Value | Your Value | Where to Find It | Used In (Script File) |
|---|---|---|---|---|
| 1 | **Azure Tenant ID** | `________________` | Section 3 (Entra ID → Overview) | `01_account_setup.sql` (storage integration, notification integration, external volume) |
| 2 | **Storage Account Name** | `________________` | Section 4a (storage account creation) | `01_account_setup.sql`, `02_snowpipe_adls.sql`, `01_adf_landing_tables.sql` |
| 2b | **Storage Container Name** | `raw-data` | Section 4b (container creation) — this is the container for Snowpipe; use `adf-staging` for ADF | `01_account_setup.sql` (STORAGE_ALLOWED_LOCATIONS), `02_snowpipe_adls.sql` (stage URL) |
| 3 | **Storage Queue URI** | `________________` | Section 4d (queue creation) — format: `https://<storageaccount>.queue.core.windows.net/<queuename>` | `01_account_setup.sql` (notification integration) |
| 4 | **Snowflake Service Principal** | `________________` | Run `DESC STORAGE INTEGRATION AZURE_STORAGE_INT;` in Snowflake → `AZURE_MULTI_TENANT_APP_NAME` | Section 4e (IAM role grants) |
| 5 | **Event Hub Namespace** | `________________` | Section 6a (namespace creation) | `03_snowpipe_streaming_eventhubs.sql` |
| 6 | **Event Hub Connection String** | `________________` | Section 6c (SAS policy) | `03_snowpipe_streaming_eventhubs.sql` (Kafka config) |
| 7 | **Event Hub Bootstrap Server** | `________________` | Format: `<namespace>.servicebus.windows.net:9093` | `03_snowpipe_streaming_eventhubs.sql` (Kafka config) |
| 8 | **VM Public IP Address** | `________________` | Section 7a (VM overview page) | SSH connection to configure Kafka connector |
| 9 | **RSA Public Key** | `________________` | Section 7d (generated on VM) | `03_snowpipe_streaming_eventhubs.sql` (`ALTER USER ... SET RSA_PUBLIC_KEY`) |
| 10 | **Fabric Workspace ID** | `________________` | Section 8b (workspace URL) | `01_account_setup.sql` (external volume), `03_catalog_integration_onelake.sql` |
| 11 | **Fabric Lakehouse ID** | `________________` | Section 8c (lakehouse URL) | `03_catalog_integration_onelake.sql` (catalog namespace) |
| 12 | **Entra App Client ID** | `________________` | Section 9a (app registration Overview) | `03_catalog_integration_onelake.sql` (catalog integration) |
| 13 | **Entra App Client Secret** | `________________` | Section 9b (client secret creation) | `03_catalog_integration_onelake.sql` (catalog integration) |
| 14 | **Snowflake Account Identifier** | `________________` | Snowsight → Admin → Accounts → `<org>-<account>` | ADF linked service, Fabric connection, MCP server URL |
| 15 | **AI Foundry Model Deployment** | `________________` | Section 10c (model deployment) | `01_foundry_agent_setup.md` (agent model selection) |
| 16 | **Snowflake PAT** | `________________` | Snowsight → Profile → Preferences → Authentication → Generate PAT | `01_foundry_agent_setup.md` (MCP connection token) |

> **Security note:** Values #6, #13, and #16 are secrets. Do not commit them to source control or share them in plain text.

---

## 13. Cleanup — Delete Everything When Done

When you're finished with the lab, delete the resource group to remove **all** Azure resources at once.

### Delete via Azure Portal

1. In the **search bar**, type `Resource groups` and click the result
2. Click on `rg-snowflake-demo`
3. Click **Delete resource group** (top bar)
4. Type the resource group name `rg-snowflake-demo` to confirm
5. Click **Delete**
6. Wait 5-10 minutes for all resources to be removed

> **What this deletes:** The storage account, data factory, event hubs namespace, virtual machine, AI Foundry hub, and all sub-resources. This is why we put everything in one resource group.

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
| Event Hubs (Standard, 1 TU) | ~$0.70/day |
| Azure Data Factory (pay per activity) | < $1 |
| Virtual Machine (D2s_v3) | ~$2.30/day (deallocate when not in use) |
| AI Foundry (GPT-4o-mini tokens) | ~$0.50-2.00 depending on usage |
| Fabric (trial is free) | $0 during trial |
| **Total for a 1-day lab** | **~$5-10** |

> **Tip to save money:** Deallocate the VM when not using it (VM overview page → **Stop**). It won't incur compute charges while stopped.
