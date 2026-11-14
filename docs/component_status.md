# AtmoSync Component Ownership & Implementation Status

## 1. Overview

This document provides a live tracking matrix of all platform components, designating module owners, architectural locations, and current delivery status. It ensures transparent alignment across the team and clearly distinguishes completed foundational work from in-progress or planned tasks.

---

## 2. Component Progress Matrix

| Component | Owner | Directory | Current Status | Delivery Stage |
|---|---|---|---|---|
| **Sensor Simulator** | Sam | `sensor_simulator/` | Implemented | Standardized physics drift and anomaly generator in `sensor_simulator/simulator.py` conforming to schema v0.1 |
| **Kafka Producer** | Sam / Roshan integration | `streaming/producer/` | Implemented | Producer client in `streaming/producer/producer.py` with pre-validation, container key partitioning, and batch delivery |
| **Kafka Consumer** | Roshan | `streaming/consumer/` | Implemented | Batch consumer in `streaming/consumer/consumer.py` with DLQ quarantine, validation, and offset commit safety |
| **Data Validation** | Roshan | `shared/schemas/` and `tests/` | Implemented | Official JSON Schema draft 2020-12 and runtime validator in `shared/validation/sensor_validator.py` with 17 tests |
| **Snowflake Ingestion** | Roshan | `snowflake/` | Implemented | Table DDL in `snowflake/sql/`, connection manager, single and batch `executemany` ingestion engine |
| **Data Staging & Lineage** | Nisha / Roshan integration | `dbt_analytics/` | Implemented | `stg_sensor_readings.sql` unpacking VARIANT `EVENT_PAYLOAD` with deduplication and Kafka metadata |
| **Spoilage Risk Analytics** | Nisha / Roshan integration | `dbt_analytics/` | Implemented | `fct_spoilage_risk.sql` with commodity thermal limits, risk flags, and composite spoilage score formula |
| **Commodity Arbitrage Readiness** | Nisha / Roshan integration | `dbt_analytics/` | Implemented | `fct_arbitrage_opportunities.sql` identifying reroute candidates and prescriptive action recommendations |
| **Superset Dashboard Stack** | Navya / Roshan integration | `dashboard/` | Implemented | Docker Compose stack, `snowflake-sqlalchemy` driver, KPI specifications, and dataset/chart definitions |

---

## 3. Detailed Component Breakdown

### 3.1 Sensor Simulator (`sensor_simulator/`)
- **Owner:** Sam Roshan
- **Completed Work:** Prototype simulation script developed on branch `sam-roshan` generating temperature, humidity, and vibration records for three containers (`C001`, `C002`, `C003`).
- **Required Refactoring:**
  1. Relocate code from root (`atmosync_sensor_simulator.py`) into `sensor_simulator/simulator.py`.
  2. Fix abnormal event trigger condition (`event_num % 15 == 0` flaw).
  3. Align emitted payload with `shared/schemas/sensor_reading.schema.json` (add `shipment_id`, rename fields to standard names, remove disallowed extra properties).
  4. Fix monotonic event ID generation to prevent duplicate IDs across restarts.
  5. Replace static uniform thresholds ($>8^\circ\text{C}$) with commodity-realistic baselines.

### 3.2 Kafka Streaming Pipeline (`streaming/`)
- **Owners:** Sam Roshan (Producer client), Roshan (Producer integration, Consumer service, topic management)
- **Completed Work:** Topic name `container-sensor-data` and consumer group `atmosync-sensor-consumer` standardized in `.env.example` and data contract.
- **Planned Work:**
  1. Implement Kafka Producer in `streaming/producer/producer.py` with pre-send schema validation.
  2. Implement Kafka Consumer in `streaming/consumer/consumer.py` with deserialization and dead-letter queue routing.

### 3.3 Data Validation & Contracts (`shared/schemas/` & `tests/`)
- **Owner:** Roshan (with whole team review)
- **Completed Work:** Official JSON Schema draft 2020-12 authored at `shared/schemas/sensor_reading.schema.json` enforcing seven mandatory fields: `container_id`, `shipment_id`, `commodity`, `temperature`, `humidity`, `vibration`, `timestamp`.
- **Planned Work:** Runtime validation helper module and automated `pytest` test suite verifying payloads against edge cases.

### 3.4 Snowflake Ingestion & Storage (`snowflake/`)
- **Owner:** Roshan
- **Completed Work:** Database target `ATMOSYNC_DB`, schema `RAW`, and landing table `SENSOR_READINGS` designed on paper and documented in architecture guides.
- **Planned Work:**
  1. Author DDL scripts in `snowflake/sql/` for database, schemas, and table definitions.
  2. Implement batch loader or Snowpipe connector to stream accepted events into Snowflake.

### 3.5 Data Engineering & Analytics (`dbt_analytics/`)
- **Owner:** Nisha
- **Completed Work:** Exploratory data cleaning and outlier analysis conducted in `data_cleaning.ipynb` on branch `nisha`.
- **Planned Work:**
  1. Initialize production dbt project in `dbt_analytics/`.
  2. Build staging model `stg_sensor_readings` reading from `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
  3. Model container condition history in `int_container_conditions`.
  4. Formulate commodity-specific spoilage thresholds and build `fct_spoilage_risk`.
  5. Ingest external commodity price datasets and build `fct_arbitrage_opportunities`.

### 3.6 Apache Superset Dashboard (`dashboard/`)
- **Owner:** Navya
- **Completed Work:** Dashboard roles and target analytical marts defined in system architecture.
- **Planned Work:**
  1. Configure Snowflake warehouse connection in Apache Superset.
  2. Build real-time container monitoring views.
  3. Visualize spoilage risk indicators and transit alerts.
  4. Develop commodity price comparison and arbitrage opportunity charts.
  5. Export and track dashboard definitions and SQL queries in `dashboard/`.

