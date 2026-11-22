# AtmoSync Component Ownership & Implementation Status

## 1. Overview

This document provides a live tracking matrix of all platform components, designating module owners, architectural locations, code implementation status, automated testing coverage, and live service verification requirements.

---

## 2. Component Progress Matrix

| Component | Owner | Directory | Code Implementation | Automated Test Coverage | Live Service Verification |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Sensor Simulator** | Sam / Roshan integration | `sensor_simulator/` | Complete (`simulator.py`) | 44 shared tests (`tests/test_kafka_producer.py`) | Ready for live broker |
| **Kafka Producer** | Sam / Roshan integration | `streaming/producer/` | Complete (`producer.py`, `README.md`) | 44 tests (`tests/test_kafka_producer.py`) | Pending live Kafka broker |
| **Kafka Consumer** | Roshan | `streaming/consumer/` | Complete (`consumer.py`) | 20 tests (`tests/test_kafka_consumer.py`) | Pending live Kafka broker & Snowflake |
| **Data Validation & Contract** | Roshan | `shared/` & `tests/` | Complete (`sensor_validator.py`) | 17 tests (`tests/test_sensor_validator.py`) | Verified against JSON Schema Draft 2020-12 |
| **Data Quality & Filter Validator** | Navya / Roshan integration | `dashboard/data_quality/` | Complete (`validator.py`, `filter_validator.py`, `report_generator.py`) | 24 tests (`tests/test_data_quality_*.py`, `test_filter_*.py`, `dashboard/data_quality/test_report_generator.py`) | Verified across 7-field records & dashboard filters |
| **Snowflake Ingestion Engine** | Roshan | `snowflake/` | Complete (`ingestion.py`, `connection.py`) | 25 tests (`tests/test_snowflake_*.py`) | DDL ready; pending live Snowflake account |
| **dbt Analytics & Data Quality** | Nisha / Roshan integration | `dbt_analytics/` | Complete (`stg_sensor_readings.sql`, `fct_sensor_data_quality.sql`, marts) | 10 tests (`tests/test_dbt_and_dashboard_integration.py`) | Modern dbt 1.12 syntax; pending live warehouse run |
| **Superset Dashboard Stack** | Navya / Roshan integration | `dashboard/` | Complete (`docker-compose.yml`, driver specs, DQ metrics) | 7 tests (`tests/test_dbt_and_dashboard_integration.py`) | Pending Docker Compose container launch |
| **Pipeline E2E Integration** | Roshan | `tests/` & `scripts/` | Complete (`smoke_test_pipeline_e2e.py`, `test_pipeline_smoke.py`, `verify_pipeline_e2e.py`) | 14 tests (`tests/integration/test_pipeline_smoke.py`, `tests/test_pipeline_e2e_integration.py`) | Verified through automated dual-mode 10-stage smoke test harness (simulation & live) |

**Total Automated Tests:** 161 passing offline unit and integration tests (100% pass rate).

---

## 3. Detailed Component Breakdown

### 3.1 Sensor Simulator (`sensor_simulator/`)
- **Module:** `sensor_simulator/simulator.py`
- **Owner:** Sam Roshan (Physics & simulation logic), Roshan (Schema integration)
- **Code Completion:** Complete. Implements mean-reverting environmental temperature drift, humidity fluctuations, and transport anomaly injection (`TEMP_SPIKE`, `HIGH_VIBRATION`, `SENSOR_FAULT`). Output strictly conforms to `shared/schemas/sensor_reading.schema.json`.
- **Automated Testing:** 12 tests in `tests/test_kafka_producer.py` verifying schema validity, batch generation, and physical clamping bounds over extended cycles.
- **Live Service Verification:** Fully operational standalone module; emits events directly to live Kafka producer when invoked.

### 3.2 Kafka Producer Service (`streaming/producer/`)
- **Module:** `streaming/producer/producer.py`, `streaming/producer/README.md`
- **Owner:** Sam Roshan / Roshan integration
- **Code Completion:** Complete.
  - Integrates Sam's robust bootstrap broker parsing (`validate_bootstrap_servers`) and topic naming validator (`validate_topic`), enforcing fast-fail configuration checks on startup.
  - Uses `confluent-kafka` client with pre-send schema validation (`validate_before_send=True`) and stable `container_id` partition key routing.
  - Broker delivery callback wrapping (`_build_delivery_callback`) tracking asynchronous `produced`, `delivered`, `delivery_failed`, `produce_failed`, `in_flight`, and `pending` counts via `ProducerStats`.
  - Bounded retries (`max_retries`, `retry_backoff_seconds`) for transient local queue/buffer exhaustion (`BufferError`) without infinite loops or duplicate events.
  - Fault-isolated batch publishing (`produce_batch(..., continue_on_error=True)`) so individual event failures do not crash remaining valid telemetry.
  - Configurable flush timeout (`flush_timeout_seconds`) reporting undelivered pending records.
  - Graceful shutdown lifecycle (`request_shutdown`, `close`, `stream_simulation`) halting new event acceptance, flushing pending buffers, and logging final statistics.
- **Automated Testing:** 44 tests in `tests/test_kafka_producer.py` covering broker address validation, topic naming constraints, configuration rules, schema pre-validation, delivery confirmation callbacks, broker failure tracking, bounded retries, flush timeouts, mixed batch isolation, graceful shutdown, simulator physics, and end-to-end producer-consumer flow.
- **Live Service Verification:** Requires live Kafka broker on `localhost:9092` with topic `container-sensor-data`.

### 3.3 Kafka Consumer Service (`streaming/consumer/`)
- **Module:** `streaming/consumer/consumer.py`
- **Owner:** Roshan
- **Code Completion:** Complete.
  - Subscribes to `container-sensor-data`, safely deserializes JSON payloads, and enforces runtime schema validation.
  - Isolates malformed or non-conforming records into `invalid_records` (dead-letter tracking) with credential sanitization (`sanitize_raw_payload`).
  - Aggregates valid events into batches and executes atomic batch insertion into Snowflake RAW (`ATMOSYNC_DB.RAW.SENSOR_READINGS`).
  - Commits Kafka offsets **only upon successful database commit**; offset commit failures raise `KafkaConsumerError`.
  - Recovers from Snowflake connection and ingestion failures by rolling back transactions and clearing stale internal connections (`_reset_snowflake_conn`) for automatic reconnection.
  - Implements resilient poison-pill isolation: pure invalid batches are acknowledged to prevent consumer partition deadlock without executing redundant database queries.
  - Structured operational logging across lifecycle (startup, message counts, validation splits, ingestion timings, offset commits, and graceful shutdown).
  - Resilient consumer execution loop `run(max_batches, stop_event)`.
- **Automated Testing:** 20 tests in `tests/test_kafka_consumer.py` verifying offset safety on DB failure, connection failure recovery, invalid payload sanitization, poison pill handling, single/multi-batch processing, and structured logging.
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

### 3.6 Data Engineering, dbt Analytics & Quality Monitoring (`dbt_analytics/`)
- **Project:** `dbt_analytics/dbt_project.yml`
- **Owner:** Nisha (Analytical requirements), Roshan (dbt Core architecture & data quality)
- **Code Completion:** Complete.
  - `stg_sensor_readings.sql`: Unpacks JSON variant fields, extracts streaming metadata, and deduplicates records.
  - `fct_sensor_data_quality.sql`: Audits all arriving RAW records, detects physical impossibility, completeness, anomalies, and duplicates, classifying records into `PASS`, `WARNING`, and `FAIL` with diagnostic reason tracking.
  - `fct_spoilage_risk.sql`: Calculates commodity-specific thermal limits, humidity and vibration flags, and composite Spoilage Score ($0.6 \cdot \text{Temp} + 0.3 \cdot \text{Hum} + 0.1 \cdot \text{Vib}$).
  - `fct_arbitrage_opportunities.sql`: Identifies reroute candidates and prescriptive logistics actions without fabricating unsupported spot prices.
  - `tests/assert_stg_*.sql`: Four singular dbt SQL assertion tests validating humidity physical bounds [0, 100], non-negative vibration, temperature physical limits [-50, 60], and staging deduplication uniqueness.
  - `schema.yml`: Comprehensive data quality tests (`not_null`, `unique`, `accepted_values`).
- **Automated Testing:** 10 tests in `tests/test_dbt_and_dashboard_integration.py` verifying YAML configurations, source bindings, SQL contract compliance, data quality classifications, singular tests, and absence of fabricated prices.
- **Live Service Verification:** Requires running `dbt run` and `dbt test` against live Snowflake warehouse using `dbt-snowflake`.

### 3.7 Apache Superset Dashboard (`dashboard/`)
- **Stack:** `dashboard/docker-compose.yml`, `dashboard/config/superset_config.py`
- **Owner:** Navya (BI & Dashboard Lead), Roshan (Integration & Data Quality)
- **Code Completion:** Complete.
  - Resolves Navya's `docs/superset_readiness_audit.md` with explicit schema mappings, SQL metric formulas, and Snowflake driver packages.
  - Containerized environment (PostgreSQL 13, Redis 6, Superset latest).
  - Snowflake SQLAlchemy driver included in `requirements-local.txt`.
  - Dataset specifications for 4 tables (`STG_SENSOR_READINGS`, `FCT_SPOILAGE_RISK`, `FCT_ARBITRAGE_OPPORTUNITIES`, `FCT_SENSOR_DATA_QUALITY`).
  - 15 calculated KPI metrics (including 8 sensor data-quality KPIs: total readings, pass/warning/fail counts, pass/fail percentages, at-risk containers, and issue reason counts).
  - 15 chart slice specifications (including 6 data-quality visualizations: status distribution, trend over time, frequent issues, container breakdown, commodity breakdown, anomaly audit table).
  - Native dashboard filter definitions (`Date Range`, `Container ID`, `Commodity`, `Quality Status`).
- **Automated Testing:** 7 tests in `tests/test_dbt_and_dashboard_integration.py` verifying Compose service dependencies, healthchecks, driver requirements, calculated metric formulas, data quality datasets, chart definitions, and filter configurations.
- **Live Service Verification:** Requires starting Docker containers (`docker compose up -d`), connecting to live Snowflake database, and assembling dashboard tiles in the UI.

### 3.8 Tabular Data Quality & Dashboard Filter Suite (`dashboard/data_quality/`)
- **Modules:** `dashboard/data_quality/validator.py`, `dashboard/data_quality/report_generator.py`, `dashboard/data_quality/filter_validator.py`, `dashboard/data_quality/visualizations/quality_dashboard.py`
- **Owner:** Navya (Data Quality & BI visualization), Roshan (Contract alignment & robust scoring)
- **Code Completion:** Complete.
  - Pure Python validator enforcing 7-field schema contract with backward compatibility for Navya's 5-field baseline.
  - Detects missing columns, nulls, duplicate keys `(container_id, timestamp)`, unparseable timestamps, invalid numeric values, humidity excursion beyond [0, 100], and statistical IQR outliers (1.5 * IQR).
  - Calculates composite quality score (0-100%) and serializes to formatted JSON report (`data_quality_report.json`).
  - Integrated Navya's `filter_validator.py` verifying that dashboard filter targets and column mappings match actual upstream dataset columns.
  - Optional Plotly dashboard visualization generator (`quality_dashboard.py`) with gauge indicators and issue breakdown bars.
- **Automated Testing:** 15 tests across `tests/test_data_quality_validator.py` (8 tests) and `tests/test_filter_validator.py` (7 tests) testing clean data, missing columns, duplicates, timestamp coercion, numeric handling, IQR outliers, score penalization, JSON export, and dashboard filter configurations.
- **Live Service Verification:** Complete and self-contained; runs in batch ETL or interactive CLI.

### 3.9 End-to-End Pipeline Verification Suite (`scripts/` & `tests/`)
- **Modules:** `scripts/verify_pipeline_e2e.py`, `tests/test_pipeline_e2e_integration.py`
- **Owner:** Roshan
- **Code Completion:** Complete.
  - Deterministic multi-stage contract verification chaining Simulator → Producer → Consumer → Snowflake RAW → dbt Staging → dbt Marts → Superset filter semantics.
  - CLI verification runner (`scripts/verify_pipeline_e2e.py`) probing live service connectivity (Kafka socket, Snowflake credentials, Docker engine) and executing contract audits with clear status reporting.
- **Automated Testing:** 2 tests in `tests/test_pipeline_e2e_integration.py` asserting the entire multi-stage flow and exception exports.
- **Live Service Verification:** CLI probe ready; reports exact infrastructure readiness and blockers.

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
