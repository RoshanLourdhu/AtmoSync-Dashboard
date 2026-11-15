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

### 3.1 Missing Value & Completeness Validation
- Ensure `container_id` and `shipment_id` are non-null and non-empty.
- Validate `timestamp` presence and RFC 3339 formatting.
- Ensure `temperature`, `humidity`, and `vibration` readings are present and numeric.
- Missing mandatory fields trigger immediate classification as `FAIL` (`is_missing_mandatory = TRUE`).
- Calculate and monitor the percentage of missing values.

### 3.2 Duplicate Record Validation
- Identify duplicate records using composite key `(container_id, recorded_at)` via `ROW_NUMBER() OVER (PARTITION BY container_id, recorded_at ORDER BY ingested_at ASC)`.
- Use window function deduplication in `stg_sensor_readings.sql` (`dedup_rank = 1`) to preserve analytics cleanliness.
- In `fct_sensor_data_quality.sql`, preserve duplicate occurrences with `is_duplicate_event = TRUE` and flag them as `WARNING` (`duplicate_occurrence_rank > 1`).
- Avoid silently dropping records without logging audit trails in raw storage.

### 3.3 Timestamp & Chronological Validation
- Validate ISO 8601 / RFC 3339 UTC format (`YYYY-MM-DDTHH:MM:SSZ` or with offset).
- Prevent future timestamps beyond 5-minute clock-skew tolerance (`recorded_at > DATEADD('minute', 5, CURRENT_TIMESTAMP())`), flagging them with `is_future_timestamp = TRUE` as `WARNING`.
- Verify chronological ordering per container.
- Flag unexpected telemetric gaps (>30 minutes).

### 3.4 Physical Bounds vs. Operational Anomaly Thresholds
- **Physical Bounds (`FAIL` - Corrupted / Incompatible Data):**
  - Relative humidity outside physical limits (`humidity < 0.0` or `humidity > 100.0`).
  - Temperature outside physical reality for cold-chain sensors (`temperature < -50.0` or `temperature > 60.0`).
  - Negative vibration acceleration (`vibration < 0.0`).
- **Operational Anomaly Thresholds (`WARNING` - Real-world Incidents / Sensor Drift):**
  - Extreme humidity approaching saturation (`humidity > 95.0%`).
  - Extreme transit shock/vibration (`vibration > 0.50 g`).
  - Refrigeration excursions outside operational safe range (`temperature < 0.0` or `temperature > 25.0`).
  - Unknown or unregistered commodity types.

### 3.5 Outlier Detection
- Apply statistical methods such as Interquartile Range (IQR).
- Flag unusual readings as operational alerts rather than automatic data errors, preserving genuine transport incidents.

---

## 4. Three-Tier Data Quality Classification Model (`fct_sensor_data_quality`)

Each sensor reading ingested into `ATMOSYNC_DB.RAW.SENSOR_READINGS` is evaluated and classified into one of three operational quality tiers in `fct_sensor_data_quality`:

```
                      +-----------------------------+
                      | Raw Ingested Sensor Event   |
                      +--------------+--------------+
                                     |
             +-----------------------+-----------------------+
             |                                               |
   Violates Physical Limits                Within Physical Bounds &
   OR Missing Required Fields              All Mandatory Fields Present
             |                                               |
             v                                               v
    +-----------------+                     +---------------------------------+
    |  Status: FAIL   |                     | Operational Anomaly, Drift, or  |
    | (Corrupt data)  |                     | Duplicate Arrival Detected?     |
    +-----------------+                     +----------------+----------------+
                                                             |
                                            +----------------+----------------+
                                            |                                 |
                                           YES                               NO
                                            |                                 |
                                            v                                 v
                                   +-----------------+               +-----------------+
                                   | Status: WARNING |               |  Status: PASS   |
                                   | (Investigate)   |               | (Clean record)  |
                                   +-----------------+               +-----------------+
```

| Quality Tier | Criteria | Ingestion Action | Downstream Impact |
|---|---|---|---|
| **`FAIL`** | Missing mandatory keys (`container_id`, `shipment_id`, etc.), temperature < -50°C or > 60°C, humidity < 0% or > 100%, vibration < 0 g | Retained in RAW & audited in `fct_sensor_data_quality` with diagnostic reasons | Excluded from downstream KPI calculations to prevent metric corruption |
| **`WARNING`** | Duplicate arrival (`rank > 1`), future timestamp (> 5m), temperature excursion (< 0°C or > 25°C), high humidity (> 95%), shock (> 0.5g), unknown commodity | Fully processed and audited | Highlighted on operator triage dashboards; included in risk analytics |
| **`PASS`** | Clean, physically valid, chronologically sound, unique payload | Fully processed | Standard input for spoilage risk and logistics arbitrage models |

### Issue Reason Tracking
The `quality_issue_reasons` column concatenates semicolon-delimited diagnostic tags for every triggered rule (e.g., `MISSING_MANDATORY_FIELDS;HUMIDITY_OUT_OF_BOUNDS;OPERATIONAL_SHOCK_HIGH_VIBRATION`), ensuring immediate root-cause transparency for data operations teams.

---

## 5. dbt Singular Quality Tests

In addition to schema-level generic tests (`not_null`, `unique`, `accepted_values`), AtmoSync enforces four singular SQL assertion tests located in `dbt_analytics/tests/`:

1. **`assert_stg_humidity_within_bounds.sql`**: Fails if any staging record has `humidity < 0.0` or `humidity > 100.0`.
2. **`assert_stg_vibration_non_negative.sql`**: Fails if any staging record has `vibration < 0.0`.
3. **`assert_stg_temperature_physically_plausible.sql`**: Fails if any staging record has `temperature < -50.0` or `temperature > 60.0`.
4. **`assert_stg_sensor_readings_unique_event.sql`**: Fails if deduplication logic in `stg_sensor_readings` produces duplicate records for composite key `(container_id, recorded_at)`.

---

## 6. Data Quality Metrics

| Metric | Calculation | Purpose |
|---|---|---|
| **Missing Value Rate** | `(Missing values / Total expected fields) * 100` | Ingestion pipeline health |
| **Duplicate Rate** | `(Duplicate records / Total raw records) * 100` | Streaming replay and retry frequency |
| **Valid Record Rate** | `(Valid records passing schema / Total records) * 100` | Contract compliance monitoring |
| **Data Freshness** | `CURRENT_TIMESTAMP() - MAX(recorded_at)` | Pipeline latency tracking |

---

## 7. dbt Verification & Quality Commands

To compile models and execute automated data quality verification tests:

```bash
# Compile and run the data quality mart model
dbt run --select fct_sensor_data_quality

# Run all singular data quality assertion tests
dbt test --select test_type:singular

# Run schema and singular tests for the data quality mart
dbt test --select fct_sensor_data_quality

# Run complete dbt test suite across all models
dbt test
```

---

## 8. Implementation Dependencies & Lineage
- Validated sensor contract (`shared/schemas/sensor_reading.schema.json`)
- Raw ingestion storage (`ATMOSYNC_DB.RAW.SENSOR_READINGS`)
- Upstream staging views (`dbt_analytics/models/staging/stg_sensor_readings.sql`)
- Data quality mart (`dbt_analytics/models/marts/fct_sensor_data_quality.sql`)
- Apache Superset monitoring views (`dashboard/`)
