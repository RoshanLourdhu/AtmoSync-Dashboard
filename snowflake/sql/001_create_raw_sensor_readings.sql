-- =============================================================================
-- AtmoSync Dashboard - Snowflake RAW Layer DDL
-- Script: snowflake/sql/001_create_raw_sensor_readings.sql
-- Target: ATMOSYNC_DB.RAW.SENSOR_READINGS
-- Description: Idempotent DDL script establishing the database, schema, and
--              landing table for streaming IoT sensor events.
-- Data Contract: shared/schemas/sensor_reading.schema.json (Draft 2020-12)
-- =============================================================================

-- Step 1: Create Database
CREATE DATABASE IF NOT EXISTS ATMOSYNC_DB
    COMMENT = 'AtmoSync real-time container monitoring, spoilage prediction, and commodity arbitrage database';

-- Step 2: Create RAW Ingestion Schema
CREATE SCHEMA IF NOT EXISTS ATMOSYNC_DB.RAW
    COMMENT = 'Landing zone for raw validated event streams and external source data';

-- Step 3: Establish Context
USE DATABASE ATMOSYNC_DB;
USE SCHEMA RAW;

-- Step 4: Create Raw Sensor Readings Table
-- Uses Snowflake VARIANT for semi-structured JSON storage, preserving the raw
-- contract event intact while capturing Kafka streaming and ingestion metadata.
CREATE TABLE IF NOT EXISTS ATMOSYNC_DB.RAW.SENSOR_READINGS (
    -- Raw Event Payload
    -- Contains the 7 validated contract fields:
    -- container_id, shipment_id, commodity, temperature, humidity, vibration, timestamp
    EVENT_PAYLOAD       VARIANT         NOT NULL
        COMMENT 'Raw validated JSON event payload adhering to shared/schemas/sensor_reading.schema.json',

    -- Streaming & Ingestion Metadata
    KAFKA_TOPIC         VARCHAR(255)    NOT NULL    DEFAULT 'container-sensor-data'
        COMMENT 'Source Kafka topic from which the event was consumed',

    KAFKA_PARTITION     NUMBER(10, 0)
        COMMENT 'Kafka partition ID from which the message was read',

    KAFKA_OFFSET        NUMBER(38, 0)
        COMMENT 'Kafka message offset within the partition',

    INGESTED_AT         TIMESTAMP_TZ    NOT NULL    DEFAULT CURRENT_TIMESTAMP()
        COMMENT 'Timestamp with timezone recording when the record was inserted into Snowflake'
)
COMMENT = 'Landing table for validated IoT container environmental readings ingested via Kafka consumer';

