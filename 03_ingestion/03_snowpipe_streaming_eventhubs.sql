-- =============================================================================
-- MSFT-SNOWFLAKE QUICKSTART LAB: Snowpipe Streaming from Azure Event Hubs
-- =============================================================================
-- Sets up Snowpipe Streaming to ingest real-time IoT sensor data from
-- Azure Event Hubs using the Snowflake Kafka Connector.
--
-- Flow: IoT Devices → Event Hub (Kafka) → Kafka Connector → Snowpipe Streaming → RAW.IOT_SENSOR_DATA
--
-- Prerequisites:
--   1. Run 01_setup/01_account_setup.sql
--   2. Run 02_native_data/01_create_tables.sql
--   3. Azure Event Hub namespace (Standard tier) with topic "iot-sensor-stream"
--   4. Azure VM with Kafka + Snowflake Kafka Connector installed
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. CREATE STREAMING USER WITH KEY-PAIR AUTH
-- =============================================================================
-- Snowpipe Streaming requires key-pair authentication (not password-based).

SET STREAMING_USER = 'STREAMING_USER';
SET STREAMING_ROLE = 'STREAMING_RL';
SET STREAMING_WH = 'DEMO_INGESTION_WH';
SET STREAMING_DB = 'MSFT_SNOWFLAKE_DEMO';

CREATE USER IF NOT EXISTS IDENTIFIER($STREAMING_USER)
  COMMENT = 'Service user for Snowpipe Streaming from Event Hubs';

CREATE OR REPLACE ROLE IDENTIFIER($STREAMING_ROLE);

-- Grants
GRANT ROLE IDENTIFIER($STREAMING_ROLE) TO USER IDENTIFIER($STREAMING_USER);
GRANT ROLE IDENTIFIER($STREAMING_ROLE) TO ROLE DEMO_ADMIN;
GRANT USAGE ON WAREHOUSE IDENTIFIER($STREAMING_WH) TO ROLE IDENTIFIER($STREAMING_ROLE);
GRANT USAGE ON DATABASE IDENTIFIER($STREAMING_DB) TO ROLE IDENTIFIER($STREAMING_ROLE);
GRANT USAGE ON SCHEMA MSFT_SNOWFLAKE_DEMO.RAW TO ROLE IDENTIFIER($STREAMING_ROLE);
GRANT CREATE TABLE ON SCHEMA MSFT_SNOWFLAKE_DEMO.RAW TO ROLE IDENTIFIER($STREAMING_ROLE);
GRANT INSERT ON ALL TABLES IN SCHEMA MSFT_SNOWFLAKE_DEMO.RAW TO ROLE IDENTIFIER($STREAMING_ROLE);
GRANT INSERT ON FUTURE TABLES IN SCHEMA MSFT_SNOWFLAKE_DEMO.RAW TO ROLE IDENTIFIER($STREAMING_ROLE);

-- Set defaults
ALTER USER IDENTIFIER($STREAMING_USER) SET DEFAULT_ROLE = $STREAMING_ROLE;
ALTER USER IDENTIFIER($STREAMING_USER) SET DEFAULT_WAREHOUSE = $STREAMING_WH;

-- =============================================================================
-- 2. SET RSA PUBLIC KEY FOR STREAMING USER
-- =============================================================================
-- Generate an RSA key pair on the VM:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--
-- Then paste the public key content (without header/footer) below:

-- ALTER USER STREAMING_USER SET RSA_PUBLIC_KEY = '<paste_public_key_content_here>';

-- =============================================================================
-- 3. GET ACCOUNT IDENTIFIER
-- =============================================================================
-- You need this for the Kafka connector configuration.

WITH HOSTLIST AS (
    SELECT * FROM TABLE(FLATTEN(INPUT => PARSE_JSON(SYSTEM$ALLOWLIST())))
)
SELECT REPLACE(VALUE:host, '.snowflakecomputing.com', '') AS ACCOUNT_IDENTIFIER
FROM HOSTLIST
WHERE VALUE:type = 'SNOWFLAKE_DEPLOYMENT_REGIONLESS';

-- =============================================================================
-- 4. KAFKA CONNECTOR CONFIGURATION FILES
-- =============================================================================
-- Create these files on the Azure VM running the Kafka connector.

-- ---- connect-standalone.properties ----
-- Save to: /home/azureuser/snowpipe-streaming/scripts/connect-standalone.properties
/*
bootstrap.servers=<your_eventhub_namespace>.servicebus.windows.net:9093
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
key.converter.schemas.enable=true
value.converter.schemas.enable=true
offset.storage.file.filename=/tmp/connect.offsets
offset.flush.interval.ms=10000

# Azure Event Hub Kafka security
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<your_eventhub_connection_string>";
consumer.security.protocol=SASL_SSL
consumer.sasl.mechanism=PLAIN
consumer.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="<your_eventhub_connection_string>";
plugin.path=/home/azureuser/snowpipe-streaming/kafka_2.12-2.8.1/libs
*/

-- ---- snowflakeconnectorEH.properties ----
-- Save to: /home/azureuser/snowpipe-streaming/scripts/snowflakeconnectorEH.properties
/*
name=snowpipeStreaming
connector.class=com.snowflake.kafka.connector.SnowflakeSinkConnector
tasks.max=4
topics=iot-sensor-stream
snowflake.database.name=MSFT_SNOWFLAKE_DEMO
snowflake.schema.name=RAW
snowflake.topic2table.map=iot-sensor-stream:IOT_SENSOR_DATA
buffer.count.records=10000
buffer.flush.time=5
buffer.size.bytes=20000000
snowflake.url.name=<your_account_identifier>.snowflakecomputing.com
snowflake.user.name=STREAMING_USER
snowflake.private.key=<your_private_key_content>
snowflake.role.name=STREAMING_RL
snowflake.ingestion.method=snowpipe_streaming
snowflake.enable.schematization=false
value.converter.schemas.enable=false
jmx=true
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=com.snowflake.kafka.connector.records.SnowflakeJsonConverter
errors.tolerance=all
*/

-- =============================================================================
-- 5. START THE KAFKA CONNECTOR (run on the Azure VM)
-- =============================================================================
/*
cd /home/azureuser/snowpipe-streaming

# Start the connector in standalone mode
./kafka_2.12-2.8.1/bin/connect-standalone.sh \
    scripts/connect-standalone.properties \
    scripts/snowflakeconnectorEH.properties
*/

-- =============================================================================
-- 6. SAMPLE IOT DATA PRODUCER (run on the Azure VM)
-- =============================================================================
-- This Python script simulates IoT sensor data and sends it to Event Hub.
-- Save as: /home/azureuser/snowpipe-streaming/iot_producer.py
/*
#!/usr/bin/env python3
"""Simulates IoT sensor data and sends to Azure Event Hub via Kafka protocol."""
import json, random, time, uuid
from datetime import datetime
from kafka import KafkaProducer

BOOTSTRAP_SERVERS = '<your_eventhub_namespace>.servicebus.windows.net:9093'
TOPIC = 'iot-sensor-stream'
CONNECTION_STRING = '<your_eventhub_connection_string>'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

LOCATIONS = ['Building-A', 'Building-B', 'Building-C', 'Warehouse-1', 'Warehouse-2']
SENSOR_TYPES = ['temperature', 'humidity', 'pressure', 'air_quality']
DEVICE_IDS = [f'device-{i:03d}' for i in range(1, 51)]

while True:
    for device_id in random.sample(DEVICE_IDS, k=random.randint(5, 15)):
        event = {
            'device_id': device_id,
            'sensor_type': random.choice(SENSOR_TYPES),
            'temperature': round(random.uniform(15.0, 45.0), 2),
            'humidity': round(random.uniform(20.0, 90.0), 2),
            'pressure': round(random.uniform(980.0, 1040.0), 2),
            'battery_level': round(random.uniform(10.0, 100.0), 2),
            'location': random.choice(LOCATIONS),
            'timestamp': datetime.utcnow().isoformat(),
            'alert_status': random.choice(['normal', 'normal', 'normal', 'warning', 'critical'])
        }
        producer.send(TOPIC, value=event)
    producer.flush()
    time.sleep(5)
*/

-- =============================================================================
-- 7. MONITORING AND VERIFICATION
-- =============================================================================

USE ROLE DEMO_ADMIN;
USE DATABASE MSFT_SNOWFLAKE_DEMO;

-- Check if data is flowing
SELECT COUNT(*) AS TOTAL_RECORDS,
       MIN(RECORD_CONTENT:timestamp::TIMESTAMP_NTZ) AS EARLIEST,
       MAX(RECORD_CONTENT:timestamp::TIMESTAMP_NTZ) AS LATEST
FROM RAW.IOT_SENSOR_DATA;

-- Query the structured view
SELECT *
FROM RAW.V_IOT_SENSOR_DATA
ORDER BY EVENT_TIMESTAMP DESC
LIMIT 20;

-- Sensor stats
SELECT
    DEVICE_ID,
    SENSOR_TYPE,
    COUNT(*) AS READING_COUNT,
    ROUND(AVG(TEMPERATURE), 2) AS AVG_TEMP,
    ROUND(AVG(HUMIDITY), 2) AS AVG_HUMIDITY,
    MIN(EVENT_TIMESTAMP) AS FIRST_READING,
    MAX(EVENT_TIMESTAMP) AS LAST_READING
FROM RAW.V_IOT_SENSOR_DATA
GROUP BY DEVICE_ID, SENSOR_TYPE
ORDER BY DEVICE_ID, SENSOR_TYPE
LIMIT 50;

-- Alert distribution
SELECT
    ALERT_STATUS,
    COUNT(*) AS COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE
FROM RAW.V_IOT_SENSOR_DATA
GROUP BY ALERT_STATUS
ORDER BY COUNT DESC;

SELECT 'Snowpipe Streaming from Event Hubs configured.' AS STATUS;
