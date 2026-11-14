# AtmoSync – dbt Analytics Project (`dbt_analytics/`)

This directory houses the production **dbt Core** project transforming raw IoT telemetry from the Snowflake RAW landing table into curated analytical staging views and business-ready data marts.

---

## 1. Pipeline Overview & Architectural Role

The AtmoSync analytics pipeline structures cold-chain telemetry into dimensional models supporting real-time spoilage tracking and logistics rerouting decisions:

```text
Snowflake RAW Layer
  └─ ATMOSYNC_DB.RAW.SENSOR_READINGS (Append-only landing table)
       │ - EVENT_PAYLOAD (VARIANT containing JSON sensor contract)
       │ - INGESTED_AT (TIMESTAMP_TZ)
       │ - KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
       │
       ▼
dbt Staging View (models/staging/stg_sensor_readings.sql)
       ├─ Unpacks 7 contract fields (container_id, shipment_id, commodity, temperature, humidity, vibration, recorded_at)
       ├─ Extracts Kafka streaming lineage (topic, partition, offset, ingested_at)
       └─ Deduplicates events via window function: QUALIFY ROW_NUMBER() OVER (PARTITION BY container_id, recorded_at)
            │
            ├──────────────────────────────────────────────────────┐
            ▼                                                      ▼
dbt Mart: Spoilage Risk                               dbt Mart: Arbitrage Readiness
(models/marts/fct_spoilage_risk.sql)                  (models/marts/fct_arbitrage_opportunities.sql)
  ├─ Evaluates commodity-specific thermal limits        ├─ Filters to latest state per container
  ├─ Evaluates humidity and vibration breach flags      ├─ Flags is_reroute_candidate = TRUE
  ├─ Calculates composite Spoilage Score (0.0 to 1.0)   ├─ Prescriptive logistics action recommendations
  └─ Classifies risk: NORMAL, WARNING, CRITICAL         └─ Architecture ready for market price feeds
```

---

## 2. Snowflake RAW Source & Semi-Structured Extraction

### Target Source Table
The upstream ingestion engine (`streaming/consumer/consumer.py` via `snowflake/ingestion.py`) loads validated IoT payloads into:
- **Database:** `ATMOSYNC_DB`
- **Schema:** `RAW`
- **Table:** `SENSOR_READINGS`

### `EVENT_PAYLOAD` Extraction Mechanics
The table stores telemetry as a semi-structured `VARIANT` column named `EVENT_PAYLOAD`. dbt extracts fields using Snowflake path navigation and explicit type casting:

```sql
SELECT
    EVENT_PAYLOAD:container_id::STRING          AS container_id,
    EVENT_PAYLOAD:shipment_id::STRING           AS shipment_id,
    EVENT_PAYLOAD:commodity::STRING             AS commodity,
    EVENT_PAYLOAD:temperature::FLOAT            AS temperature,
    EVENT_PAYLOAD:humidity::FLOAT               AS humidity,
    EVENT_PAYLOAD:vibration::FLOAT              AS vibration,
    EVENT_PAYLOAD:timestamp::TIMESTAMP_TZ       AS recorded_at,
    KAFKA_TOPIC                                 AS kafka_topic,
    KAFKA_PARTITION                             AS kafka_partition,
    KAFKA_OFFSET                                AS kafka_offset,
    INGESTED_AT                                 AS ingested_at
FROM {{ source('raw', 'SENSOR_READINGS') }}
```

---

## 3. Transformation Layers

### A. Staging Layer (`models/staging/`)
- **Materialization:** `view` (defined in `dbt_project.yml` under schema `STAGING`).
- **File:** `models/staging/stg_sensor_readings.sql`
- **Core Functionality:**
  1. Unpacks all seven mandatory fields guaranteed by `shared/schemas/sensor_reading.schema.json`.
  2. Preserves streaming lineage coordinates (`kafka_topic`, `kafka_partition`, `kafka_offset`, `ingested_at`).
  3. Ensures idempotent downstream processing by eliminating duplicates via `ROW_NUMBER() OVER (PARTITION BY container_id, recorded_at ORDER BY ingested_at DESC, kafka_offset DESC) = 1`.

### B. Marts Layer (`models/marts/`)
- **Materialization:** `table` (defined in `dbt_project.yml` under schema `MARTS`).
- **Models:**
  1. `fct_spoilage_risk.sql`:
     - **Thermal Limits:** Commodity-specific thresholds derived from agronomic baselines (Avocados: $\le 8.0^\circ\text{C}$, Mangoes: $\le 10.5^\circ\text{C}$, Bananas: $\le 15.0^\circ\text{C}$; default: $10.0^\circ\text{C}$).
     - **Humidity Breach:** Relative humidity $> 85.0\%$.
     - **Vibration Spike:** Mechanical acceleration $> 0.28\text{ g}$ (derived from IQR transport outlier analysis).
     - **Composite Spoilage Score:**
       $$\text{Spoilage Score} = 0.6 \cdot \text{Temp\_Risk} + 0.3 \cdot \text{Hum\_Risk} + 0.1 \cdot \text{Vib\_Risk}$$
     - **Risk Classification:**
       - `CRITICAL` ($\text{Score} \ge 0.70$): Cargo requires immediate intervention, expedited dispatch, or liquidation.
       - `WARNING` ($0.30 \le \text{Score} < 0.70$): Container requires route review and refrigeration unit verification.
       - `NORMAL` ($\text{Score} < 0.30$): Conditions within safe preservation parameters.
  2. `fct_arbitrage_opportunities.sql`:
     - Filters to the latest active observation per container (`ROW_NUMBER() OVER (PARTITION BY container_id ORDER BY recorded_at DESC) = 1`).
     - Recommends actionable logistics directives:
       - `IMMEDIATE_REROUTE_OR_LIQUIDATION` for `CRITICAL` risk cargo.
       - `INSPECT_AND_EXPEDITE` for `WARNING` risk cargo.
       - `MAINTAIN_SCHEDULE` for `NORMAL` cargo.
     - **Data Availability & Commercial Pricing Note:** To prevent analytical distortion, wholesale price spreads and spot market prices are **not fabricated**. Once external destination wholesale market feeds are integrated, price differential columns will be joined here.

---

## 4. Data Quality & Schema Testing

dbt tests are declared in `models/staging/schema.yml` and `models/marts/schema.yml`:

- **Not-Null Constraints:** Enforced across all primary business fields (`container_id`, `shipment_id`, `recorded_at`, `temperature`, `humidity`, `vibration`, `spoilage_score`, `risk_level`).
- **Accepted Values:**
  - `commodity`: `['Avocados', 'Mangoes', 'Bananas']`
  - `temp_risk_flag`, `hum_risk_flag`, `vib_risk_flag`: `[0, 1]`
  - `risk_level`: `['NORMAL', 'WARNING', 'CRITICAL']`
- **Uniqueness:** Enforced on `container_id` in `fct_arbitrage_opportunities` to guarantee one operational record per active container.

---

## 5. Snowflake Profile Configuration (`profiles.yml`)

The dbt project strictly avoids hardcoded credentials by reading all parameters from environment variables.

### Setting Up `profiles.yml`
Copy `profiles.yml.example` to your profiles directory (or local project root):

```powershell
Copy-Item profiles.yml.example profiles.yml
```

The profile configuration maps to standard environment variables:

```yaml
atmosync_profile:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: "{{ env_var('SNOWFLAKE_ROLE', 'ATMOSYNC_INGEST_ROLE') }}"
      database: "{{ env_var('SNOWFLAKE_DATABASE', 'ATMOSYNC_DB') }}"
      warehouse: "{{ env_var('SNOWFLAKE_WAREHOUSE', 'ATMOSYNC_WH') }}"
      schema: "{{ env_var('SNOWFLAKE_SCHEMA', 'ANALYTICS') }}"
      threads: 4
      client_session_keep_alive: False
```

---

## 6. Execution Commands & Workflow

```powershell
# 1. Verify environment configuration and warehouse connectivity
dbt debug --profiles-dir .

# 2. Install any required dbt packages (if declared in packages.yml)
dbt deps --profiles-dir .

# 3. Build Staging Views
dbt run --select staging --profiles-dir .

# 4. Materialize Analytical Data Marts
dbt run --select marts --profiles-dir .

# 5. Execute all schema tests and integrity assertions
dbt test --profiles-dir .

# 6. Generate and view interactive documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

---

## 7. Testing vs. Live Snowflake Execution Boundaries

| Environment | Scope & Verification | What Is Validated |
| :--- | :--- | :--- |
| **Offline Unit Tests (`pytest`)** | Validated via `tests/test_dbt_and_dashboard_integration.py` | Validates YAML parsing, SQL source references, Jinja macros, contract field extractions, and scoring formulas without requiring Snowflake credentials. |
| **Live Snowflake Execution** | Executed via `dbt run` and `dbt test` | Compiles SQL against active warehouse, creates views in `STAGING` and tables in `MARTS`, and executes live data assertions against actual rows. |
