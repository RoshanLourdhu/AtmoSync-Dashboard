# AtmoSync Component Ownership & Implementation Status

## 1. Overview

This document provides a live tracking matrix of all platform components, designating module owners, architectural locations, code implementation status, automated testing coverage, and live service verification requirements.

---

## 2. Component Progress Matrix

| Component | Owner | Directory | Code Implementation | Automated Test Coverage | Live Service Verification |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Sensor Simulator** | Sam / Roshan integration | `sensor_simulator/` | Complete (`simulator.py`) | 12 tests (`tests/test_kafka_producer.py`) | Ready for live broker |
| **Kafka Producer** | Sam / Roshan integration | `streaming/producer/` | Complete (`producer.py`) | 12 tests (`tests/test_kafka_producer.py`) | Pending live Kafka broker |
| **Kafka Consumer** | Roshan | `streaming/consumer/` | Complete (`consumer.py`) | 11 tests (`tests/test_kafka_consumer.py`) | Pending live Kafka broker & Snowflake |
| **Data Validation & Contract** | Roshan | `shared/` & `tests/` | Complete (`sensor_validator.py`) | 17 tests (`tests/test_sensor_validator.py`) | Verified against JSON Schema Draft 2020-12 |
| **Snowflake Ingestion Engine** | Roshan | `snowflake/` | Complete (`ingestion.py`, `connection.py`) | 25 tests (`tests/test_snowflake_*.py`) | DDL ready; pending live Snowflake account |
| **dbt Analytics & Staging** | Nisha / Roshan integration | `dbt_analytics/` | Complete (`stg_sensor_readings.sql`, marts) | 7 tests (`tests/test_dbt_and_dashboard_integration.py`) | Pending `dbt-snowflake` run against live warehouse |
| **Superset Dashboard Stack** | Navya / Roshan integration | `dashboard/` | Complete (`docker-compose.yml`, driver specs) | 4 tests (`tests/test_dbt_and_dashboard_integration.py`) | Pending Docker Compose container launch |

**Total Automated Tests:** 76 passing offline unit and integration tests.

---

## 3. Detailed Component Breakdown

### 3.1 Sensor Simulator (`sensor_simulator/`)
- **Module:** `sensor_simulator/simulator.py`
- **Owner:** Sam Roshan (Physics & simulation logic), Roshan (Schema integration)
- **Code Completion:** Complete. Implements mean-reverting environmental temperature drift, humidity fluctuations, and transport anomaly injection (`TEMP_SPIKE`, `HIGH_VIBRATION`, `SENSOR_FAULT`). Output strictly conforms to `shared/schemas/sensor_reading.schema.json`.
- **Automated Testing:** 12 tests in `tests/test_kafka_producer.py` verifying schema validity, batch generation, and physical clamping bounds over extended cycles.
- **Live Service Verification:** Fully operational standalone module; emits events directly to live Kafka producer when invoked.

### 3.2 Kafka Producer Service (`streaming/producer/`)
- **Module:** `streaming/producer/producer.py`
- **Owner:** Sam Roshan / Roshan integration
- **Code Completion:** Complete. Uses `confluent-kafka` client with pre-send schema validation (`validate_before_send=True`), message partitioning by `container_id` key, delivery callbacks, and batch buffer flushing.
- **Automated Testing:** Tested via mocked Kafka producer instances verifying serialization, key partitioning, pre-validation rejection, and delivery handling.
- **Live Service Verification:** Requires live Kafka broker on `localhost:9092` with topic `container-sensor-data`.

### 3.3 Kafka Consumer Service (`streaming/consumer/`)
- **Module:** `streaming/consumer/consumer.py`
- **Owner:** Roshan
- **Code Completion:** Complete. Subscribes to `container-sensor-data`, safely deserializes JSON payloads, enforces runtime schema validation, isolates malformed records into `invalid_records` (dead-letter handling), aggregates valid events into batches, executes atomic batch insertion into Snowflake RAW, and commits Kafka offsets **only upon successful database commit**.
- **Automated Testing:** 11 tests in `tests/test_kafka_consumer.py` verifying offset safety on DB failure, invalid JSON handling, and batch orchestration.
- **Live Service Verification:** Requires live Kafka broker connection and live Snowflake warehouse connection.

### 3.4 Data Validation & Contracts (`shared/schemas/` & `shared/validation/`)
- **Modules:** `shared/schemas/sensor_reading.schema.json`, `shared/validation/sensor_validator.py`
- **Owner:** Roshan
- **Code Completion:** Complete. JSON Schema Draft 2020-12 enforcing mandatory 7-field contract (`container_id`, `shipment_id`, `commodity`, `temperature`, `humidity`, `vibration`, `timestamp`) with `additionalProperties: false`.
- **Automated Testing:** 17 tests in `tests/test_sensor_validator.py` testing type assertions, boundary limits, missing properties, and invalid types.
- **Live Service Verification:** Complete and self-contained; runs offline and in production pipelines.

### 3.5 Snowflake Ingestion & Storage (`snowflake/`)
- **Modules:** `snowflake/sql/001_create_raw_sensor_readings.sql`, `snowflake/connection.py`, `snowflake/ingestion.py`
- **Owner:** Roshan
- **Code Completion:** Complete. RAW table DDL with `VARIANT` payload and `TIMESTAMP_TZ` ingestion metadata. Environment-based connection manager. Transactional single-event and batch `executemany` insertion engines with automatic rollback on error.
- **Automated Testing:** 25 tests across `tests/test_snowflake_ingestion.py` and `tests/test_snowflake_batch_ingestion.py` verifying SQL parameter binding, JSON serialization, transaction rollback, and cursor cleanup.
- **Live Service Verification:** Pending execution of `001_create_raw_sensor_readings.sql` on live Snowflake account.

### 3.6 Data Engineering & dbt Analytics (`dbt_analytics/`)
- **Project:** `dbt_analytics/dbt_project.yml`
- **Owner:** Nisha (Analytical requirements), Roshan (dbt Core architecture)
- **Code Completion:** Complete.
  - `stg_sensor_readings.sql`: Unpacks JSON variant fields, extracts streaming metadata, and deduplicates records.
  - `fct_spoilage_risk.sql`: Calculates commodity-specific thermal limits, humidity and vibration flags, and composite Spoilage Score ($0.6 \cdot \text{Temp} + 0.3 \cdot \text{Hum} + 0.1 \cdot \text{Vib}$).
  - `fct_arbitrage_opportunities.sql`: Identifies reroute candidates and prescriptive logistics actions without fabricating unsupported spot prices.
  - `schema.yml`: Comprehensive data quality tests (`not_null`, `unique`, `accepted_values`).
- **Automated Testing:** 7 tests in `tests/test_dbt_and_dashboard_integration.py` verifying YAML configurations, source bindings, SQL contract compliance, and absence of fabricated prices.
- **Live Service Verification:** Requires running `dbt run` and `dbt test` against live Snowflake warehouse using `dbt-snowflake`.

### 3.7 Apache Superset Dashboard (`dashboard/`)
- **Stack:** `dashboard/docker-compose.yml`, `dashboard/config/superset_config.py`
- **Owner:** Navya (BI & Dashboard Lead), Roshan (Integration)
- **Code Completion:** Complete. Containerized environment (PostgreSQL 13, Redis 6, Superset latest). Snowflake SQLAlchemy driver included in `requirements-local.txt`. Dataset specifications and calculated metric formulas documented in `datasets_and_charts.yaml`.
- **Automated Testing:** 4 tests in `tests/test_dbt_and_dashboard_integration.py` verifying Compose service dependencies, healthchecks, driver requirements, and calculated metric formulas.
- **Live Service Verification:** Requires starting Docker containers (`docker compose up -d`), connecting to live Snowflake database, and assembling dashboard tiles in the UI.

---

## 4. Next Integration Steps

1. **Step 1 — Cloud Kafka Cluster Setup:**
   - Provision local Kafka cluster (or Confluent Cloud).
   - Create topic `container-sensor-data` with 3 partitions and retention policy.
2. **Step 2 — Snowflake Warehouse Deployment:**
   - Execute `snowflake/sql/001_create_raw_sensor_readings.sql` on target Snowflake account.
   - Configure service user credentials in local `.env`.
3. **Step 3 — End-to-End Streaming Run:**
   - Start Kafka Consumer service (`streaming/consumer/consumer.py`).
   - Run Sensor Producer (`streaming/producer/producer.py`) to stream batches to Kafka and verify automated insertion into `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
4. **Step 4 — Live dbt Compilation & Execution:**
   - Execute `dbt debug` to confirm Snowflake connectivity.
   - Execute `dbt run` to materialize views in `STAGING` and tables in `MARTS`.
   - Execute `dbt test` to validate all data quality constraints on live records.
5. **Step 5 — Superset Dashboard Launch & Publication:**
   - Start Docker stack (`docker compose up -d`).
   - Connect Superset to Snowflake analytical data marts.
   - Publish executive dashboard based on the KPI blueprint.
6. **Step 6 — Wholesale Market Spot Price Integration:**
   - Ingest live wholesale destination price feeds into Snowflake to calculate monetary arbitrage spreads in `fct_arbitrage_opportunities`.
