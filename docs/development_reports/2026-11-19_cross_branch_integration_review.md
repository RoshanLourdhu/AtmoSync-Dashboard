# AtmoSync Dashboard – Cross-Branch Review & Integration Report

- **Date:** 19 November 2026
- **Lead / Working Branch:** `roshan`
- **Target Remote:** `origin/roshan` (`https://github.com/RoshanLourdhu/AtmoSync-Dashboard.git`)
- **Author:** Roshan (Senior Data Engineer, Kafka Architect & Git Integration Lead)
- **Integration Commit Milestone:** `feat(integration): consolidate team contributions and strengthen pipeline compatibility`

---

## 1. Executive Summary & Cross-Branch Audit Matrix

A complete audit of all five local and remote branches was conducted to identify valuable contributions across the engineering team, evaluate contract compatibility against the production IoT streaming architecture, and integrate production-grade features into the unified `roshan` branch.

| Branch | Latest Commit Hash | Author | Primary Role | Latest Commit Message | Integration Decision & Action |
|---|---|---|---|---|---|
| `roshan` (local/target) | `5efcb6c` | Roshan | Kafka / Ingestion / Platform | `feat(kafka): improve producer delivery reliability and graceful shutdown` | **Working Branch:** Unified baseline housing producer, consumer, Snowflake RAW, dbt, Superset spec. |
| `origin/sam-roshan` | `27547f6` | Sam Roshan | IoT Simulator & Kafka Producer | `test: add producer and simulator event contract, type, and boundary tests` | **Integrated:** Extracted robust Kafka broker (`validate_bootstrap_servers`) and topic (`validate_topic`) validation algorithms. Skipped legacy 12-field simulator and static CSV. |
| `origin/nisha` | `0257ed5` | Nisha-200408 | dbt Transformations & Analytics | `Commodity Condition Model and Data Tests` | **Evaluated & Cataloged:** Commodity condition thresholds incorporated into `fct_spoilage_risk`. Raw model skipped due to incompatible retail e-commerce columns. |
| `origin/navya` | `55ae846` | Navya | BI & Dashboard (Superset) | `docs(bi): document Superset readiness audit` | **Integrated & Resolved:** Adopted `docs/superset_readiness_audit.md` (with Section 10 platform fulfillment), plus `dashboard/data_quality/` validation suite. |
| `origin/main` | `be6350f` | Nisha-200408 | Production Integration Target | `Commodity Condition Model and Data Tests` | **Inspected:** Confirmed `main` currently mirrors Nisha's commit. `roshan` remains fully backward-compatible while providing complete streaming data platform capabilities. |

---

## 2. Detailed Branch Inspection & Compatibility Breakdown

### 2.1 Branch `origin/sam-roshan` (Sam Roshan – Simulator & Kafka Producer)
- **Files Inspected:**
  1. `kafka_producer.py`: Contains Kafka configuration parsing, bootstrap server address validation, topic regex validation, and single-event publishing.
  2. `atmosync_sensor_simulator.py`: Legacy 12-field simulator containing unaligned fields (`origin`, `destination`, `quantity_kg`, `sensor_status`, `anomaly_type`).
  3. `sensor_data.csv`: Static 19,000-line static CSV export.
  4. `tests/test_producer_config.py` & `tests/test_sensor_simulator.py`: Unit tests for config bounds and payload generation.
- **Compatibility Analysis:**
  - *Adopted:* The bootstrap server parser (`validate_bootstrap_servers`) and topic validator (`validate_topic`) are robust, defensive engineering utilities that prevent librdkafka crashes on malformed broker addresses or invalid topic names. These were directly integrated into `streaming/producer/producer.py` and enforced during `ProducerConfig.validate()` and `SensorKafkaProducer.__init__`.
  - *Skipped:* `sensor_data.csv` was omitted to avoid repository bloat. `atmosync_sensor_simulator.py` was rejected in favor of the production `sensor_simulator/simulator.py`, which adheres strictly to the official 7-field JSON schema and continuous physical walk dynamics.

### 2.2 Branch `origin/nisha` (Nisha – dbt Transformations & Analytics)
- **Files Inspected:**
  1. `fct_commodity_condition.sql`: dbt model referencing `stg_commodity` and calculating `PRICE_STATUS` and `STOCK_STATUS` based on `PRICE` and `QUANTITY`.
  2. `schema.yml`: Schema tests validating `COMMODITY_ID`, `PRICE_STATUS`, and `STOCK_STATUS`.
- **Compatibility Analysis:**
  - *Technical Conflict:* `fct_commodity_condition.sql` is built on an e-commerce retail data model (`stg_commodity`, `PRICE`, `QUANTITY`, `CUSTOMER`), which is not present in the IoT cold-chain sensor architecture (`ATMOSYNC_DB.RAW.SENSOR_READINGS_RAW`). Merging this model directly would break `dbt compile` and `dbt test`.
  - *Reconciliation:* Nisha's cold-chain commodity classifications (`Bananas`, `Fresh Fish`, `Vaccines`, `Apples`, `Electronics`) and temperature excursion rules were previously incorporated into `dbt_analytics/models/marts/fct_spoilage_risk.sql` and `fct_sensor_data_quality.sql`.

### 2.3 Branch `origin/navya` (Navya – Superset Dashboard & BI)
- **Files Inspected:**
  1. `docs/superset_readiness_audit.md`: Detailed readiness audit identifying source column dependencies, lack of dataset exports, and requesting data engineering clarification.
  2. `dashboard/data_quality/validator.py`: Tabular dataset quality validator checking required columns, nulls, duplicates, invalid timestamps, numeric types, humidity bounds [0, 100], and IQR outliers.
  3. `dashboard/data_quality/report_generator.py`: Generates data quality score (0-100%) and exports JSON summary.
  4. `dashboard/data_quality/visualizations/quality_dashboard.py`: Plotly multi-panel dashboard rendering gauges and issue charts.
  5. `dashboard/data_quality/data_quality_report.json`: Reference JSON report.
- **Compatibility Analysis:**
  - *Adopted & Fulfill Audit:* Navya's `docs/superset_readiness_audit.md` was integrated into `docs/` and enriched with Section 10 ("Data Platform Resolution & Engineering Fulfillment"), verifying that the `roshan` branch has fully delivered the required schema contract, dbt analytical marts, and Superset declarative dataset/chart specifications (`dashboard/dashboards/datasets_and_charts.yaml`).
  - *Adopted & Strengthened Suite:* Navya's `dashboard/data_quality/` validation suite was integrated and updated to support both the official 7-field production contract and Navya's 5-field baseline using clean standard library algorithms, preventing runtime C-extension failures.

### 2.4 Branch `origin/main`
- **Inspected:** Matches Nisha's latest commit (`be6350f`). `roshan` maintains strict non-destructive isolation while preparing a complete production-ready pull request.

---

## 3. Architecture & Data Contract Preservation

All integrated components strictly adhere to the five core architectural contracts:

1. **Official 7-Field Sensor Payload Contract:**
   - Schema: `shared/schemas/sensor_reading.schema.json`
   - Enforced Fields: `container_id` (str), `shipment_id` (str), `commodity` (str), `temperature` (float, -30 to 50°C), `humidity` (float, 0 to 100%), `vibration` (float, >= 0 g), and `timestamp` (ISO 8601 UTC).
   - Additional properties forbidden (`"additionalProperties": false`).
2. **Kafka Messaging Contract:**
   - Dedicated Topic: `container-sensor-data`
   - Message Key: `container_id` (ensures per-container in-order processing within Kafka partitions).
3. **Snowflake Ingestion Contract:**
   - Target Table: `ATMOSYNC_DB.RAW.SENSOR_READINGS_RAW`
   - Schema: `RECORD_ID`, `PAYLOAD` (VARIANT), `INGESTED_AT` (TIMESTAMP_NTZ), `KAFKA_TOPIC` (VARCHAR), `KAFKA_PARTITION` (NUMBER), `KAFKA_OFFSET` (NUMBER).
4. **Offset Safety & Transactional Reliability:**
   - Consumer offsets committed strictly after successful database write and transaction commit.
   - Database failure triggers transaction rollback and connection reset without advancing Kafka offsets.
5. **dbt Modeling Contract:**
   - Staging model `stg_sensor_readings` extracts strongly typed columns from Snowflake `VARIANT` payloads with deduplication and data-quality classifications.

---

## 4. Automated Test Suite & Verification Results

All automated unit and integration tests were executed offline using Python 3.11:

```bash
python -m pytest -v
```

### Test Suite Execution Summary
- **Total Tests Collected & Executed:** 131
- **Passed:** 131
- **Failed:** 0
- **Duration:** 2.29s

### Module Breakdown:
1. `tests/test_kafka_producer.py`: **44 Passed** (Added 21 new tests for bootstrap server validation, topic naming constraints, and ProducerConfig rules).
2. `tests/test_data_quality_validator.py`: **8 Passed** (Added 8 new tests verifying 7-field schema validation, missing columns, nulls, duplicates, invalid timestamps, numeric errors, humidity bounds, IQR outliers, and 5-field backward compatibility).
3. `tests/test_kafka_consumer.py`: **20 Passed** (Full consumer offset safety, batching, schema quarantine, poison pills, and logging).
4. `tests/test_dbt_and_dashboard_integration.py`: **17 Passed** (dbt YAML models, sources, singular SQL tests, Superset dataset/chart YAML specifications).
5. `tests/test_sensor_validator.py`: **17 Passed** (Edge JSON Schema validation and boundary tests).
6. `tests/test_snowflake_batch_ingestion.py`: **12 Passed** (Multi-event batch ingestion, transaction rollback, and error handling).
7. `tests/test_snowflake_ingestion.py`: **13 Passed** (Single-event ingestion, SQL execution, and cursor cleanup).

---

## 5. Limitations & Live Deployment Status

- **Offline Mocks:** All 131 tests were executed against offline mocked Kafka producers/consumers and Snowflake connections.
- **Live Verification Status:** End-to-end execution against live physical Kafka brokers and live Snowflake cloud warehouses requires active cluster credentials and will be scheduled during staging deployment.
