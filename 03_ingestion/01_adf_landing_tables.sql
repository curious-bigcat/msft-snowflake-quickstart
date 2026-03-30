-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: ADF Landing Tables & Sample Data
-- =============================================================================
-- Tables and sample data files for Azure Data Factory ingestion.
-- ADF will use Snowflake V2 connector (COPY INTO) to load data into these tables.
-- Prerequisites: Run 01_setup/ and 02_native_data/ scripts first.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- 1. EXTERNAL STAGE FOR ADF STAGING (Azure Blob Storage)
-- =============================================================================
-- ADF uses a staging area in Azure Blob Storage for the COPY INTO command.

CREATE OR REPLACE STAGE STAGING.ADF_STAGE
  STORAGE_INTEGRATION = AZURE_STORAGE_INT
  URL = 'azure://<your_storage_account>.blob.core.windows.net/adf-staging/'
  FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
  COMMENT = 'External stage for ADF pipeline staging';

-- =============================================================================
-- 2. GENERATE SAMPLE CSV DATA FOR ADF
-- =============================================================================
-- These queries generate sample data that you should export to CSV files
-- and upload to your Azure Blob Storage container (adf-staging/) for ADF to pick up.

-- Sample Inventory Data (save as inventory_data.csv in adf-staging/inventory/)
-- Copy this output and save as CSV:
SELECT
    SEQ4() AS INVENTORY_ID,
    UNIFORM(1, 200, RANDOM()) AS PRODUCT_ID,
    ARRAY_CONSTRUCT('Warehouse-East','Warehouse-West','Warehouse-Central',
                    'Warehouse-South','Distribution-Hub-1','Distribution-Hub-2')
        [UNIFORM(0,5,RANDOM())]::VARCHAR AS WAREHOUSE_LOCATION,
    UNIFORM(0, 5000, RANDOM()) AS QUANTITY_ON_HAND,
    UNIFORM(0, 500, RANDOM()) AS QUANTITY_RESERVED,
    UNIFORM(10, 200, RANDOM()) AS REORDER_POINT,
    DATEADD('day', -UNIFORM(1, 90, RANDOM()), CURRENT_DATE()) AS LAST_RESTOCK_DATE,
    UNIFORM(1, 50, RANDOM()) AS SUPPLIER_ID
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Sample Supplier Data (save as supplier_data.csv in adf-staging/suppliers/)
SELECT
    SEQ4() AS SUPPLIER_ID,
    CONCAT(
        ARRAY_CONSTRUCT('Global','Pacific','Atlantic','Continental','Premier',
                        'Apex','Pinnacle','Summit','Vertex','Core')
            [UNIFORM(0,9,RANDOM())]::VARCHAR,
        ' ',
        ARRAY_CONSTRUCT('Technologies','Solutions','Systems','Industries','Logistics',
                        'Manufacturing','Components','Electronics','Supply Co','Corp')
            [UNIFORM(0,9,RANDOM())]::VARCHAR
    ) AS SUPPLIER_NAME,
    LOWER(CONCAT(RANDSTR(6, RANDOM()), '@supplier.com')) AS CONTACT_EMAIL,
    ARRAY_CONSTRUCT('United States','Germany','China','Japan','South Korea',
                    'Taiwan','India','Mexico','Canada','United Kingdom')
        [UNIFORM(0,9,RANDOM())]::VARCHAR AS COUNTRY,
    UNIFORM(3, 45, RANDOM()) AS LEAD_TIME_DAYS,
    ROUND(UNIFORM(0.60, 0.99, RANDOM())::NUMERIC(3,2), 2) AS RELIABILITY_SCORE,
    DATEADD('month', -UNIFORM(1, 36, RANDOM()), CURRENT_DATE()) AS CONTRACT_START_DATE,
    DATEADD('month', UNIFORM(6, 24, RANDOM()), CURRENT_DATE()) AS CONTRACT_END_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 50));

-- =============================================================================
-- 3. COPY INTO COMMANDS (used by ADF internally, or run manually)
-- =============================================================================
-- These are the COPY INTO commands that ADF's Snowflake V2 connector runs.
-- You can also run these manually after uploading files to the stage.

/*
-- Load inventory data
COPY INTO STAGING.ADF_INVENTORY (
    INVENTORY_ID, PRODUCT_ID, WAREHOUSE_LOCATION, QUANTITY_ON_HAND,
    QUANTITY_RESERVED, REORDER_POINT, LAST_RESTOCK_DATE, SUPPLIER_ID
)
FROM @STAGING.ADF_STAGE/inventory/
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;

-- Load supplier data
COPY INTO STAGING.ADF_SUPPLIER_DATA (
    SUPPLIER_ID, SUPPLIER_NAME, CONTACT_EMAIL, COUNTRY,
    LEAD_TIME_DAYS, RELIABILITY_SCORE, CONTRACT_START_DATE, CONTRACT_END_DATE
)
FROM @STAGING.ADF_STAGE/suppliers/
FILE_FORMAT = (FORMAT_NAME = RAW.CSV_FORMAT)
ON_ERROR = 'CONTINUE'
PURGE = FALSE;
*/

-- =============================================================================
-- 4. DIRECT INSERT (alternative to ADF for quick demo setup)
-- =============================================================================
-- If you want to skip ADF setup and load data directly:

INSERT INTO STAGING.ADF_INVENTORY (
    INVENTORY_ID, PRODUCT_ID, WAREHOUSE_LOCATION, QUANTITY_ON_HAND,
    QUANTITY_RESERVED, REORDER_POINT, LAST_RESTOCK_DATE, SUPPLIER_ID
)
SELECT
    SEQ4() AS INVENTORY_ID,
    UNIFORM(1, 200, RANDOM()) AS PRODUCT_ID,
    ARRAY_CONSTRUCT('Warehouse-East','Warehouse-West','Warehouse-Central',
                    'Warehouse-South','Distribution-Hub-1','Distribution-Hub-2')
        [UNIFORM(0,5,RANDOM())]::VARCHAR AS WAREHOUSE_LOCATION,
    UNIFORM(0, 5000, RANDOM()) AS QUANTITY_ON_HAND,
    UNIFORM(0, 500, RANDOM()) AS QUANTITY_RESERVED,
    UNIFORM(10, 200, RANDOM()) AS REORDER_POINT,
    DATEADD('day', -UNIFORM(1, 90, RANDOM()), CURRENT_DATE()) AS LAST_RESTOCK_DATE,
    UNIFORM(1, 50, RANDOM()) AS SUPPLIER_ID
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

INSERT INTO STAGING.ADF_SUPPLIER_DATA (
    SUPPLIER_ID, SUPPLIER_NAME, CONTACT_EMAIL, COUNTRY,
    LEAD_TIME_DAYS, RELIABILITY_SCORE, CONTRACT_START_DATE, CONTRACT_END_DATE
)
SELECT
    SEQ4() AS SUPPLIER_ID,
    CONCAT(
        ARRAY_CONSTRUCT('Global','Pacific','Atlantic','Continental','Premier',
                        'Apex','Pinnacle','Summit','Vertex','Core')
            [UNIFORM(0,9,RANDOM())]::VARCHAR,
        ' ',
        ARRAY_CONSTRUCT('Technologies','Solutions','Systems','Industries','Logistics',
                        'Manufacturing','Components','Electronics','Supply Co','Corp')
            [UNIFORM(0,9,RANDOM())]::VARCHAR
    ) AS SUPPLIER_NAME,
    LOWER(CONCAT(RANDSTR(6, RANDOM()), '@supplier.com')) AS CONTACT_EMAIL,
    ARRAY_CONSTRUCT('United States','Germany','China','Japan','South Korea',
                    'Taiwan','India','Mexico','Canada','United Kingdom')
        [UNIFORM(0,9,RANDOM())]::VARCHAR AS COUNTRY,
    UNIFORM(3, 45, RANDOM()) AS LEAD_TIME_DAYS,
    ROUND(UNIFORM(0.60, 0.99, RANDOM())::NUMERIC(3,2), 2) AS RELIABILITY_SCORE,
    DATEADD('month', -UNIFORM(1, 36, RANDOM()), CURRENT_DATE()) AS CONTRACT_START_DATE,
    DATEADD('month', UNIFORM(6, 24, RANDOM()), CURRENT_DATE()) AS CONTRACT_END_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 50));

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'ADF_INVENTORY' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM STAGING.ADF_INVENTORY
UNION ALL SELECT 'ADF_SUPPLIER_DATA', COUNT(*) FROM STAGING.ADF_SUPPLIER_DATA;

SELECT 'ADF landing tables loaded.' AS STATUS;
