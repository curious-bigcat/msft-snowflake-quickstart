-- =============================================================================
-- MEDALLION ARCHITECTURE: BRONZE Layer — Table Definitions and Synthetic Data
-- =============================================================================
-- Part 1: DDL for all BRONZE tables and internal stages
-- Part 2: Synthetic data generation (~200K rows across 6 tables)
--
-- Prerequisites: Run 01_setup/01_account_setup.sql first.
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE WAREHOUSE DEMO_WH;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- =============================================================================
-- PART 1: TABLE DEFINITIONS
-- =============================================================================

USE SCHEMA BRONZE;

-- -----------------------------------------------------------------------------
-- Customers — Master customer data
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CUSTOMERS (
    CUSTOMER_ID        NUMBER AUTOINCREMENT PRIMARY KEY,
    FIRST_NAME         VARCHAR(100)   NOT NULL,
    LAST_NAME          VARCHAR(100)   NOT NULL,
    EMAIL              VARCHAR(255),
    PHONE              VARCHAR(20),
    ADDRESS            VARCHAR(500),
    CITY               VARCHAR(100),
    STATE              VARCHAR(50),
    COUNTRY            VARCHAR(50)    DEFAULT 'United States',
    POSTAL_CODE        VARCHAR(20),
    CUSTOMER_SEGMENT   VARCHAR(50)    COMMENT 'Enterprise, SMB, or Consumer',
    REGISTRATION_DATE  DATE,
    IS_ACTIVE          BOOLEAN        DEFAULT TRUE,
    CREATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT         TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Customer master data';

-- -----------------------------------------------------------------------------
-- Products — Product catalog
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCTS (
    PRODUCT_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    PRODUCT_NAME  VARCHAR(200)   NOT NULL,
    CATEGORY      VARCHAR(100)   COMMENT 'Electronics, Software, Cloud Services, Hardware, Accessories',
    SUB_CATEGORY  VARCHAR(100),
    BRAND         VARCHAR(100),
    UNIT_PRICE    NUMBER(10,2),
    COST_PRICE    NUMBER(10,2),
    DESCRIPTION   VARCHAR(2000),
    SKU           VARCHAR(50)    UNIQUE,
    IS_ACTIVE     BOOLEAN        DEFAULT TRUE,
    CREATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Product catalog';

-- -----------------------------------------------------------------------------
-- Orders — Sales orders
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDERS (
    ORDER_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID       NUMBER         NOT NULL,
    ORDER_DATE        TIMESTAMP_NTZ  NOT NULL,
    ORDER_STATUS      VARCHAR(20)    COMMENT 'Pending, Processing, Shipped, Delivered, Cancelled',
    TOTAL_AMOUNT      NUMBER(12,2),
    DISCOUNT_AMOUNT   NUMBER(10,2)   DEFAULT 0,
    SHIPPING_AMOUNT   NUMBER(10,2)   DEFAULT 0,
    PAYMENT_METHOD    VARCHAR(50)    COMMENT 'Credit Card, Debit Card, Wire Transfer, PayPal',
    SHIPPING_ADDRESS  VARCHAR(500),
    REGION            VARCHAR(50)    COMMENT 'North America, Europe, Asia Pacific, Latin America',
    CHANNEL           VARCHAR(50)    COMMENT 'Online, In-Store, Partner, Marketplace',
    SOURCE_SYSTEM     VARCHAR(50)    DEFAULT 'NATIVE' COMMENT 'Data source system',
    CREATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Sales orders from multiple sources';

-- -----------------------------------------------------------------------------
-- Order Items — Line items for each order
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.ORDER_ITEMS (
    ORDER_ITEM_ID  NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ID       NUMBER         NOT NULL,
    PRODUCT_ID     NUMBER         NOT NULL,
    QUANTITY       NUMBER         NOT NULL,
    UNIT_PRICE     NUMBER(10,2)   NOT NULL,
    LINE_TOTAL     NUMBER(12,2)   NOT NULL,
    DISCOUNT_PCT   NUMBER(5,2)    DEFAULT 0
)
COMMENT = 'Order line items';

-- -----------------------------------------------------------------------------
-- Product Reviews — Unstructured text for Cortex Search
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.PRODUCT_REVIEWS (
    REVIEW_ID      NUMBER AUTOINCREMENT PRIMARY KEY,
    PRODUCT_ID     NUMBER         NOT NULL,
    CUSTOMER_ID    NUMBER         NOT NULL,
    REVIEW_TEXT    VARCHAR(5000)  NOT NULL,
    RATING         NUMBER(2,1)   COMMENT '1.0 to 5.0',
    REVIEW_DATE    DATE,
    SENTIMENT      VARCHAR(20)   COMMENT 'Populated by Cortex AI',
    HELPFUL_VOTES  NUMBER        DEFAULT 0
)
COMMENT = 'Product reviews — used for Cortex Search and sentiment analysis';

-- -----------------------------------------------------------------------------
-- Support Tickets — ML classification target
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.SUPPORT_TICKETS (
    TICKET_ID             NUMBER AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_ID           NUMBER         NOT NULL,
    PRODUCT_ID            NUMBER,
    TICKET_SUBJECT        VARCHAR(500)   NOT NULL,
    TICKET_DESCRIPTION    VARCHAR(5000),
    PRIORITY              VARCHAR(20)    COMMENT 'Critical, High, Medium, Low — ML target',
    STATUS                VARCHAR(20)    COMMENT 'Open, In Progress, Resolved, Closed',
    CATEGORY              VARCHAR(50)    COMMENT 'Technical, Billing, Shipping, Product Defect, General Inquiry',
    RESOLUTION_TIME_HOURS NUMBER(10,2),
    SATISFACTION_SCORE    NUMBER(2,1),
    CREATED_AT            TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
    RESOLVED_AT           TIMESTAMP_NTZ
)
COMMENT = 'Support tickets — used for ML priority classification';

-- =============================================================================
-- INTERNAL STAGES — For ML models and semantic model YAML
-- =============================================================================

USE SCHEMA ML;
CREATE OR REPLACE STAGE ML.ML_MODELS
  COMMENT = 'Stage for storing trained ML model artifacts (joblib, pickle)';

USE SCHEMA AGENTS;
CREATE OR REPLACE STAGE AGENTS.SEMANTIC_MODELS
  COMMENT = 'Stage for semantic model YAML files used by Cortex Analyst';

-- Verify tables
SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE
FROM MSFT_SNOWFLAKE_DEMO.INFORMATION_SCHEMA.TABLES
WHERE TABLE_CATALOG = 'MSFT_SNOWFLAKE_DEMO'
  AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA')
ORDER BY TABLE_SCHEMA, TABLE_NAME;

SELECT 'Table creation complete.' AS STATUS;

-- =============================================================================
-- PART 2: SYNTHETIC DATA GENERATION (~200K rows)
-- =============================================================================

USE SCHEMA BRONZE;

-- =============================================================================
-- 1. CUSTOMERS — ~10,000 rows
-- =============================================================================

INSERT INTO BRONZE.CUSTOMERS (
    FIRST_NAME, LAST_NAME, EMAIL, PHONE, ADDRESS, CITY, STATE, COUNTRY,
    POSTAL_CODE, CUSTOMER_SEGMENT, REGISTRATION_DATE, IS_ACTIVE
)
SELECT
    ARRAY_CONSTRUCT(
        'James','Mary','John','Patricia','Robert','Jennifer','Michael','Linda',
        'David','Elizabeth','William','Barbara','Richard','Susan','Joseph','Jessica',
        'Thomas','Sarah','Christopher','Karen','Charles','Lisa','Daniel','Nancy',
        'Matthew','Betty','Anthony','Margaret','Mark','Sandra',
        'Raj','Priya','Wei','Mei','Carlos','Maria','Ahmed','Fatima','Yuki','Hiro'
    )[UNIFORM(0,39,RANDOM())]::VARCHAR AS FIRST_NAME,

    ARRAY_CONSTRUCT(
        'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
        'Rodriguez','Martinez','Anderson','Taylor','Thomas','Hernandez','Moore',
        'Martin','Jackson','Thompson','White','Lopez','Lee','Harris','Clark',
        'Lewis','Robinson','Walker','Perez','Hall','Young','Allen',
        'Patel','Kim','Singh','Chen','Kumar','Nakamura','Mueller','Santos','Ivanov','Ali'
    )[UNIFORM(0,39,RANDOM())]::VARCHAR AS LAST_NAME,

    LOWER(CONCAT(
        RANDSTR(5, RANDOM()), '.', RANDSTR(4, RANDOM()), '@',
        ARRAY_CONSTRUCT('outlook.com','gmail.com','company.com','enterprise.org','techcorp.io')
            [UNIFORM(0,4,RANDOM())]::VARCHAR
    )) AS EMAIL,

    CONCAT('+1-', UNIFORM(200,999,RANDOM())::VARCHAR, '-',
           UNIFORM(100,999,RANDOM())::VARCHAR, '-',
           UNIFORM(1000,9999,RANDOM())::VARCHAR) AS PHONE,

    CONCAT(UNIFORM(100,9999,RANDOM())::VARCHAR, ' ',
        ARRAY_CONSTRUCT('Main St','Oak Ave','Maple Dr','Cedar Ln','Pine Rd',
                        'Elm St','Washington Blvd','Park Ave','Broadway','Market St')
            [UNIFORM(0,9,RANDOM())]::VARCHAR) AS ADDRESS,

    ARRAY_CONSTRUCT('New York','Los Angeles','Chicago','Houston','Phoenix',
                    'Philadelphia','San Antonio','San Diego','Dallas','Seattle',
                    'Denver','Boston','Austin','Nashville','Portland',
                    'Miami','Atlanta','Minneapolis','Detroit','Tampa')
        [UNIFORM(0,19,RANDOM())]::VARCHAR AS CITY,

    ARRAY_CONSTRUCT('NY','CA','IL','TX','AZ','PA','TX','CA','TX','WA',
                    'CO','MA','TX','TN','OR','FL','GA','MN','MI','FL')
        [UNIFORM(0,19,RANDOM())]::VARCHAR AS STATE,

    'United States' AS COUNTRY,

    LPAD(UNIFORM(10000,99999,RANDOM())::VARCHAR, 5, '0') AS POSTAL_CODE,

    ARRAY_CONSTRUCT('Enterprise','SMB','Consumer','Enterprise','SMB',
                    'Consumer','Consumer','SMB','Enterprise','Consumer')
        [UNIFORM(0,9,RANDOM())]::VARCHAR AS CUSTOMER_SEGMENT,

    DATEADD('day', -UNIFORM(30,1825,RANDOM()), CURRENT_DATE()) AS REGISTRATION_DATE,

    IFF(UNIFORM(1,100,RANDOM()) > 10, TRUE, FALSE) AS IS_ACTIVE

FROM TABLE(GENERATOR(ROWCOUNT => 10000));

-- =============================================================================
-- 2. PRODUCTS — 200 rows across 5 categories
-- =============================================================================

INSERT INTO BRONZE.PRODUCTS (
    PRODUCT_NAME, CATEGORY, SUB_CATEGORY, BRAND, UNIT_PRICE, COST_PRICE,
    DESCRIPTION, SKU, IS_ACTIVE
)
WITH product_base AS (
    SELECT
        SEQ4() AS RN,
        ARRAY_CONSTRUCT('Pro','Elite','Essential','Premium','Advanced',
                        'Basic','Ultra','Enterprise','Standard','Performance')
            [UNIFORM(0,9,RANDOM())]::VARCHAR AS PREFIX,
        CASE UNIFORM(1,5,RANDOM())
            WHEN 1 THEN 'Electronics'
            WHEN 2 THEN 'Software'
            WHEN 3 THEN 'Cloud Services'
            WHEN 4 THEN 'Hardware'
            WHEN 5 THEN 'Accessories'
        END AS CATEGORY
    FROM TABLE(GENERATOR(ROWCOUNT => 200))
)
SELECT
    CONCAT(PREFIX, ' ',
        CASE CATEGORY
            WHEN 'Electronics' THEN
                ARRAY_CONSTRUCT('Laptop','Monitor','Keyboard','Mouse','Headset',
                                'Webcam','Docking Station','Tablet','Speaker','Microphone')
                    [UNIFORM(0,9,RANDOM())]::VARCHAR
            WHEN 'Software' THEN
                ARRAY_CONSTRUCT('Office Suite','Security Suite','Project Manager','CRM Platform',
                                'ERP System','Analytics Platform','DevOps Tool','Collaboration Hub',
                                'Data Warehouse','AI Toolkit')
                    [UNIFORM(0,9,RANDOM())]::VARCHAR
            WHEN 'Cloud Services' THEN
                ARRAY_CONSTRUCT('Compute Instance','Storage Plan','AI Service','365 License',
                                'Power BI Pro','Database Service','Network Plan','CDN Service',
                                'IoT Hub','Container Service')
                    [UNIFORM(0,9,RANDOM())]::VARCHAR
            WHEN 'Hardware' THEN
                ARRAY_CONSTRUCT('Server Rack','Router','Network Switch','Firewall',
                                'UPS System','SSD Drive','Memory Module','NIC Card',
                                'Cable Kit','Rack Mount')
                    [UNIFORM(0,9,RANDOM())]::VARCHAR
            WHEN 'Accessories' THEN
                ARRAY_CONSTRUCT('USB Cable','Laptop Case','Screen Protector','Charger',
                                'Adapter Kit','Stylus Pen','Mousepad','Monitor Arm',
                                'Cable Organizer','Cleaning Kit')
                    [UNIFORM(0,9,RANDOM())]::VARCHAR
        END
    ) AS PRODUCT_NAME,
    CATEGORY,
    CASE CATEGORY
        WHEN 'Electronics'    THEN ARRAY_CONSTRUCT('Computing','Peripherals','Audio','Video')[UNIFORM(0,3,RANDOM())]::VARCHAR
        WHEN 'Software'       THEN ARRAY_CONSTRUCT('Productivity','Security','Development','Analytics')[UNIFORM(0,3,RANDOM())]::VARCHAR
        WHEN 'Cloud Services' THEN ARRAY_CONSTRUCT('IaaS','PaaS','SaaS','AI/ML')[UNIFORM(0,3,RANDOM())]::VARCHAR
        WHEN 'Hardware'       THEN ARRAY_CONSTRUCT('Networking','Storage','Compute','Power')[UNIFORM(0,3,RANDOM())]::VARCHAR
        WHEN 'Accessories'    THEN ARRAY_CONSTRUCT('Cables','Protection','Ergonomics','Maintenance')[UNIFORM(0,3,RANDOM())]::VARCHAR
    END AS SUB_CATEGORY,
    ARRAY_CONSTRUCT('Microsoft','Dell','HP','Lenovo','Cisco','Logitech',
                    'Samsung','Snowflake','Adobe','ServiceNow')
        [UNIFORM(0,9,RANDOM())]::VARCHAR AS BRAND,
    ROUND(UNIFORM(19.99, 2999.99, RANDOM())::NUMERIC(10,2), 2) AS UNIT_PRICE,
    ROUND(UNIFORM(19.99, 2999.99, RANDOM())::NUMERIC(10,2) * UNIFORM(0.35, 0.70, RANDOM()), 2) AS COST_PRICE,
    CONCAT('High-quality ', LOWER(CATEGORY), ' product designed for enterprise and business use.') AS DESCRIPTION,
    CONCAT('SKU-', UPPER(LEFT(CATEGORY,3)), '-', LPAD(RN::VARCHAR, 5, '0')) AS SKU,
    IFF(UNIFORM(1,100,RANDOM()) > 5, TRUE, FALSE) AS IS_ACTIVE
FROM product_base;

-- =============================================================================
-- 3. ORDERS — ~50,000 rows over the last 2 years
-- =============================================================================

INSERT INTO BRONZE.ORDERS (
    CUSTOMER_ID, ORDER_DATE, ORDER_STATUS, TOTAL_AMOUNT, DISCOUNT_AMOUNT,
    SHIPPING_AMOUNT, PAYMENT_METHOD, SHIPPING_ADDRESS, REGION, CHANNEL, SOURCE_SYSTEM
)
SELECT
    UNIFORM(1, 10000, RANDOM()) AS CUSTOMER_ID,
    DATEADD('second', -UNIFORM(0, 63072000, RANDOM()), CURRENT_TIMESTAMP()) AS ORDER_DATE,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 5  THEN 'Pending'
        WHEN UNIFORM(1,100,RANDOM()) <= 15 THEN 'Processing'
        WHEN UNIFORM(1,100,RANDOM()) <= 30 THEN 'Shipped'
        WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 'Delivered'
        ELSE 'Cancelled'
    END AS ORDER_STATUS,
    ROUND(UNIFORM(50.00, 10000.00, RANDOM())::NUMERIC(12,2), 2) AS TOTAL_AMOUNT,
    ROUND(UNIFORM(0.00, 500.00, RANDOM())::NUMERIC(10,2), 2)    AS DISCOUNT_AMOUNT,
    ROUND(UNIFORM(0.00, 99.99, RANDOM())::NUMERIC(10,2), 2)     AS SHIPPING_AMOUNT,
    ARRAY_CONSTRUCT('Credit Card','Debit Card','Wire Transfer','PayPal','Credit Card','Credit Card')
        [UNIFORM(0,5,RANDOM())]::VARCHAR AS PAYMENT_METHOD,
    CONCAT(UNIFORM(1,9999,RANDOM())::VARCHAR, ' ',
        ARRAY_CONSTRUCT('Main St','Oak Ave','Maple Dr','Cedar Ln','Pine Rd')[UNIFORM(0,4,RANDOM())]::VARCHAR,
        ', ',
        ARRAY_CONSTRUCT('New York','Los Angeles','Chicago','Houston','Seattle','Miami','Denver','Boston')
            [UNIFORM(0,7,RANDOM())]::VARCHAR) AS SHIPPING_ADDRESS,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 50 THEN 'North America'
        WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 'Europe'
        WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 'Asia Pacific'
        ELSE 'Latin America'
    END AS REGION,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 40 THEN 'Online'
        WHEN UNIFORM(1,100,RANDOM()) <= 60 THEN 'In-Store'
        WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN 'Partner'
        ELSE 'Marketplace'
    END AS CHANNEL,
    'NATIVE' AS SOURCE_SYSTEM
FROM TABLE(GENERATOR(ROWCOUNT => 50000));

-- =============================================================================
-- 4. ORDER ITEMS — ~2-3 items per order (~120,000 rows)
-- =============================================================================

INSERT INTO BRONZE.ORDER_ITEMS (ORDER_ID, PRODUCT_ID, QUANTITY, UNIT_PRICE, LINE_TOTAL, DISCOUNT_PCT)
WITH order_ids AS (SELECT ORDER_ID FROM BRONZE.ORDERS),
expanded AS (
    SELECT
        o.ORDER_ID,
        ROW_NUMBER() OVER (PARTITION BY o.ORDER_ID ORDER BY RANDOM()) AS ITEM_NUM
    FROM order_ids o, TABLE(GENERATOR(ROWCOUNT => 3)) g
    WHERE UNIFORM(1,100,RANDOM()) <= 80
)
SELECT
    ORDER_ID,
    UNIFORM(1, 200, RANDOM())                                                    AS PRODUCT_ID,
    UNIFORM(1, 10, RANDOM())                                                     AS QUANTITY,
    ROUND(UNIFORM(19.99, 999.99, RANDOM())::NUMERIC(10,2), 2)                   AS UNIT_PRICE,
    ROUND(UNIFORM(1,10,RANDOM()) * UNIFORM(19.99, 999.99, RANDOM())::NUMERIC(12,2), 2) AS LINE_TOTAL,
    ROUND(UNIFORM(0, 25, RANDOM())::NUMERIC(5,2), 2)                            AS DISCOUNT_PCT
FROM expanded;

-- =============================================================================
-- 5. PRODUCT REVIEWS — ~15,000 reviews with varied sentiment text
-- =============================================================================

INSERT INTO BRONZE.PRODUCT_REVIEWS (
    PRODUCT_ID, CUSTOMER_ID, REVIEW_TEXT, RATING, REVIEW_DATE, HELPFUL_VOTES
)
SELECT
    UNIFORM(1, 200, RANDOM()) AS PRODUCT_ID,
    UNIFORM(1, 10000, RANDOM()) AS CUSTOMER_ID,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 30 THEN
            CONCAT(ARRAY_CONSTRUCT('Absolutely love this product!','Excellent quality and fast delivery.',
                'Best purchase I have made this year.','Outstanding product, exceeded all expectations.',
                'Highly recommend to everyone.','Perfect for our enterprise needs.',
                'Incredible value for the price.')[UNIFORM(0,6,RANDOM())]::VARCHAR, ' ',
                ARRAY_CONSTRUCT('The build quality is top-notch and it integrates seamlessly with our existing systems.',
                'Setup was straightforward and the performance has been consistently reliable.',
                'Customer support was excellent when we had questions during onboarding.',
                'We deployed this across our entire organization and the ROI has been fantastic.',
                'Works perfectly with Microsoft 365 and our Azure infrastructure.')[UNIFORM(0,4,RANDOM())]::VARCHAR)
        WHEN UNIFORM(1,100,RANDOM()) <= 65 THEN
            CONCAT(ARRAY_CONSTRUCT('Good product overall, meets our requirements.','Solid performance with a few minor issues.',
                'Very satisfied with this purchase.','Works well for our team, good value.',
                'Reliable product with room for improvement.')[UNIFORM(0,4,RANDOM())]::VARCHAR, ' ',
                ARRAY_CONSTRUCT('The documentation could be more detailed but the product itself is great.',
                'Minor UI issues but functionality is solid. Would buy again.',
                'Integration with our cloud stack took some effort but works well now.',
                'Performs well under load. The admin console could use some polish.')[UNIFORM(0,3,RANDOM())]::VARCHAR)
        WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN
            CONCAT(ARRAY_CONSTRUCT('Average product, nothing special.','Decent but there are better options.',
                'It works but does not stand out.','Meets basic requirements but lacks advanced features.',
                'OK for the price point.')[UNIFORM(0,4,RANDOM())]::VARCHAR, ' ',
                ARRAY_CONSTRUCT('The performance is adequate but not impressive compared to competitors.',
                'Some features are missing that we expected at this price point.',
                'Works for basic use cases but struggles with our enterprise workload.')[UNIFORM(0,2,RANDOM())]::VARCHAR)
        WHEN UNIFORM(1,100,RANDOM()) <= 95 THEN
            CONCAT(ARRAY_CONSTRUCT('Disappointing experience overall.','Did not meet our expectations.',
                'Quality is below what was advertised.','Would not purchase again.',
                'Needs significant improvement.')[UNIFORM(0,4,RANDOM())]::VARCHAR, ' ',
                ARRAY_CONSTRUCT('We encountered multiple issues during setup and the support team was slow to respond.',
                'The product crashed several times during our testing phase.',
                'Performance degrades significantly under heavy load.')[UNIFORM(0,2,RANDOM())]::VARCHAR)
        ELSE
            CONCAT(ARRAY_CONSTRUCT('Terrible product, do not buy.','Complete waste of money.',
                'The worst purchase our team has made.','Absolutely unacceptable quality.')[UNIFORM(0,3,RANDOM())]::VARCHAR, ' ',
                ARRAY_CONSTRUCT('The product stopped working after just one week. Support has been unresponsive.',
                'Data loss occurred during a routine operation. This is unacceptable for enterprise use.',
                'Completely incompatible with our systems despite claiming full integration support.')[UNIFORM(0,2,RANDOM())]::VARCHAR)
    END AS REVIEW_TEXT,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 30 THEN ROUND(UNIFORM(4.5, 5.0, RANDOM())::NUMERIC(2,1), 1)
        WHEN UNIFORM(1,100,RANDOM()) <= 65 THEN ROUND(UNIFORM(3.5, 4.4, RANDOM())::NUMERIC(2,1), 1)
        WHEN UNIFORM(1,100,RANDOM()) <= 85 THEN ROUND(UNIFORM(2.5, 3.4, RANDOM())::NUMERIC(2,1), 1)
        WHEN UNIFORM(1,100,RANDOM()) <= 95 THEN ROUND(UNIFORM(1.5, 2.4, RANDOM())::NUMERIC(2,1), 1)
        ELSE ROUND(UNIFORM(1.0, 1.4, RANDOM())::NUMERIC(2,1), 1)
    END AS RATING,
    DATEADD('day', -UNIFORM(1, 730, RANDOM()), CURRENT_DATE()) AS REVIEW_DATE,
    UNIFORM(0, 50, RANDOM()) AS HELPFUL_VOTES
FROM TABLE(GENERATOR(ROWCOUNT => 15000));

-- =============================================================================
-- 6. SUPPORT TICKETS — ~8,000 tickets
-- =============================================================================

INSERT INTO BRONZE.SUPPORT_TICKETS (
    CUSTOMER_ID, PRODUCT_ID, TICKET_SUBJECT, TICKET_DESCRIPTION, PRIORITY,
    STATUS, CATEGORY, RESOLUTION_TIME_HOURS, SATISFACTION_SCORE, CREATED_AT, RESOLVED_AT
)
SELECT
    UNIFORM(1, 10000, RANDOM()) AS CUSTOMER_ID,
    UNIFORM(1, 200, RANDOM())   AS PRODUCT_ID,
    CASE cat.CATEGORY
        WHEN 'Technical' THEN
            ARRAY_CONSTRUCT('Cannot connect to cloud service','Software crashes on startup',
                'Integration error with Azure AD','Performance degradation in production',
                'API returning 500 errors','Database connection timeout issues',
                'SSL certificate validation failure','Memory leak in application server')
                [UNIFORM(0,7,RANDOM())]::VARCHAR
        WHEN 'Billing' THEN
            ARRAY_CONSTRUCT('Incorrect charge on invoice','Need to update payment method',
                'Requesting license upgrade','Duplicate billing for subscription',
                'Refund request for cancelled service','Question about enterprise pricing')
                [UNIFORM(0,5,RANDOM())]::VARCHAR
        WHEN 'Shipping' THEN
            ARRAY_CONSTRUCT('Order not received after 2 weeks','Package arrived damaged',
                'Wrong item delivered','Need expedited shipping upgrade',
                'Tracking number not working','Return shipping label request')
                [UNIFORM(0,5,RANDOM())]::VARCHAR
        WHEN 'Product Defect' THEN
            ARRAY_CONSTRUCT('Hardware malfunction out of box','Screen flickering on new monitor',
                'Battery not holding charge','Keyboard keys sticking',
                'Device overheating under normal use','USB ports not functioning')
                [UNIFORM(0,5,RANDOM())]::VARCHAR
        ELSE
            ARRAY_CONSTRUCT('General product inquiry','Need help choosing the right plan',
                'Requesting product documentation','Feature request for next release',
                'Question about compatibility','Account access help needed')
                [UNIFORM(0,5,RANDOM())]::VARCHAR
    END AS TICKET_SUBJECT,
    CONCAT('Customer reported: ',
        ARRAY_CONSTRUCT('The system is not responding as expected after the latest update.',
            'We have been experiencing intermittent failures across our deployment.',
            'Multiple users in our organization are affected by this problem.',
            'This issue is blocking our critical business operations.',
            'We need urgent assistance to resolve this before our deadline.',
            'The problem started occurring after integrating with our Azure environment.',
            'We have tried the troubleshooting steps in the documentation without success.',
            'This is a recurring issue that we reported previously.')[UNIFORM(0,7,RANDOM())]::VARCHAR,
        ' Environment: ', ARRAY_CONSTRUCT('Production','Staging','Development','Testing')[UNIFORM(0,3,RANDOM())]::VARCHAR,
        '. Impact: ', ARRAY_CONSTRUCT('All users affected','Limited to specific team','Single user','Intermittent for all')
            [UNIFORM(0,3,RANDOM())]::VARCHAR, '.') AS TICKET_DESCRIPTION,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 5  THEN 'Critical'
        WHEN UNIFORM(1,100,RANDOM()) <= 20 THEN 'High'
        WHEN UNIFORM(1,100,RANDOM()) <= 70 THEN 'Medium'
        ELSE 'Low'
    END AS PRIORITY,
    CASE
        WHEN UNIFORM(1,100,RANDOM()) <= 15 THEN 'Open'
        WHEN UNIFORM(1,100,RANDOM()) <= 35 THEN 'In Progress'
        WHEN UNIFORM(1,100,RANDOM()) <= 75 THEN 'Resolved'
        ELSE 'Closed'
    END AS STATUS,
    cat.CATEGORY,
    ROUND(UNIFORM(2, 240, RANDOM())::NUMERIC(10,2), 2)  AS RESOLUTION_TIME_HOURS,
    ROUND(UNIFORM(1.0, 5.0, RANDOM())::NUMERIC(2,1), 1) AS SATISFACTION_SCORE,
    DATEADD('second', -UNIFORM(0, 31536000, RANDOM()), CURRENT_TIMESTAMP()) AS CREATED_AT,
    CASE WHEN UNIFORM(1,100,RANDOM()) <= 65
         THEN DATEADD('hour', UNIFORM(2, 240, RANDOM()),
                      DATEADD('second', -UNIFORM(0, 31536000, RANDOM()), CURRENT_TIMESTAMP()))
         ELSE NULL END AS RESOLVED_AT
FROM TABLE(GENERATOR(ROWCOUNT => 8000)) g,
     LATERAL (
         SELECT CASE
             WHEN UNIFORM(1,100,RANDOM()) <= 35 THEN 'Technical'
             WHEN UNIFORM(1,100,RANDOM()) <= 55 THEN 'Billing'
             WHEN UNIFORM(1,100,RANDOM()) <= 70 THEN 'Shipping'
             WHEN UNIFORM(1,100,RANDOM()) <= 90 THEN 'Product Defect'
             ELSE 'General Inquiry'
         END AS CATEGORY
     ) cat;

-- =============================================================================
-- VERIFICATION
-- =============================================================================

SELECT 'CUSTOMERS'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM BRONZE.CUSTOMERS
UNION ALL SELECT 'PRODUCTS',       COUNT(*) FROM BRONZE.PRODUCTS
UNION ALL SELECT 'ORDERS',         COUNT(*) FROM BRONZE.ORDERS
UNION ALL SELECT 'ORDER_ITEMS',    COUNT(*) FROM BRONZE.ORDER_ITEMS
UNION ALL SELECT 'PRODUCT_REVIEWS',COUNT(*) FROM BRONZE.PRODUCT_REVIEWS
UNION ALL SELECT 'SUPPORT_TICKETS',COUNT(*) FROM BRONZE.SUPPORT_TICKETS
ORDER BY TABLE_NAME;

SELECT 'Bronze tables created and synthetic data loaded.' AS STATUS;
