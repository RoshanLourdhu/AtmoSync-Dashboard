# AtmoSync – Data Quality & Validation Specification

## 1. Objective
Define validation checks to maintain the accuracy, consistency, and reliability of sensor data used in the AtmoSync analytics dashboard.

---

## 2. Data Quality Dimensions

| Dimension | Description |
|---|---|
| **Completeness** | Check whether required sensor values are missing from payloads |
| **Uniqueness** | Identify duplicate sensor records using container ID and timestamp |
| **Validity** | Detect invalid or out-of-range sensor values violating schema limits |
| **Consistency** | Verify that data strictly follows the RFC 3339 timestamp and numeric formats |
| **Timeliness** | Check whether sensor readings are received within expected transit intervals |
| **Accuracy** | Identify unusual readings that require operational investigation |

---

## 3. Validation Checks

### 3.1 Missing Value Validation
- Ensure `container_id` and `shipment_id` are non-null and non-empty.
- Validate `timestamp` presence.
- Ensure `temperature`, `humidity`, and `vibration` readings are present and numeric.
- Calculate and monitor the percentage of missing values.

### 3.2 Duplicate Record Validation
- Identify duplicate records using composite key `(container_id, recorded_at)`.
- Use window function deduplication (`ROW_NUMBER() OVER (...)`) in `stg_sensor_readings.sql`.
- Avoid silently dropping records without logging audit trails in raw storage.

### 3.3 Timestamp Validation
- Validate ISO 8601 / RFC 3339 UTC format (`YYYY-MM-DDTHH:MM:SSZ`).
- Prevent future timestamps beyond clock-skew tolerances.
- Verify chronological ordering per container.
- Flag unexpected telemetric gaps (>30 minutes).

### 3.4 Sensor Value Validation
- Enforce relative humidity within physical bounds (`0.0 <= humidity <= 100.0`).
- Flag extreme temperatures outside commodity refrigeration bounds.
- Detect excessive vibration spikes exceeding transit safety limits (>0.28 g).

### 3.5 Outlier Detection
- Apply statistical methods such as Interquartile Range (IQR).
- Flag unusual readings as operational alerts rather than automatic data errors, preserving genuine transport incidents.

---

## 4. Data Quality Metrics

| Metric | Calculation | Purpose |
|---|---|---|
| **Missing Value Rate** | `(Missing values / Total expected fields) * 100` | Ingestion pipeline health |
| **Duplicate Rate** | `(Duplicate records / Total raw records) * 100` | Streaming replay and retry frequency |
| **Valid Record Rate** | `(Valid records passing schema / Total records) * 100` | Contract compliance monitoring |
| **Data Freshness** | `CURRENT_TIMESTAMP() - MAX(recorded_at)` | Pipeline latency tracking |

---

## 5. Implementation Dependencies
- Validated sensor contract (`shared/schemas/sensor_reading.schema.json`)
- Upstream staging views (`dbt_analytics/models/staging/stg_sensor_readings.sql`)
- Apache Superset monitoring views (`dashboard/`)
