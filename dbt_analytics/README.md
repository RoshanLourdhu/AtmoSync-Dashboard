# AtmoSync – dbt Analytics Project (`dbt_analytics/`)

This directory houses the production **dbt Core** project transforming raw IoT telemetry from Snowflake RAW into analytical staging and data mart models.

---

## 1. Architecture & Layering

```text
Snowflake RAW Layer
  └─ ATMOSYNC_DB.RAW.SENSOR_READINGS (Append-only, VARIANT EVENT_PAYLOAD)
       │
       ▼
dbt Staging View
  └─ models/staging/stg_sensor_readings.sql
       ├─ Unpacks 7 contract fields (container_id, shipment_id, commodity, temperature, humidity, vibration, recorded_at)
       ├─ Retains Kafka lineage (topic, partition, offset, ingested_at)
       └─ Deduplicates events via QUALIFY ROW_NUMBER()
            │
            ├──────────────────────────────────────────────┐
            ▼                                              ▼
dbt Mart: Spoilage Risk                      dbt Mart: Arbitrage Readiness
  └─ models/marts/fct_spoilage_risk.sql        └─ models/marts/fct_arbitrage_opportunities.sql
       ├─ Commodity thermal limits                  ├─ Rerouting candidate identification
       ├─ Temp/Hum/Vib risk flags                   ├─ Prescriptive action recommendations
       ├─ Composite Spoilage Score                  └─ Ready for external market price integration
       └─ Risk level (NORMAL, WARNING, CRITICAL)
```

---

## 2. Configuration & Profiles

The project uses environment variables to prevent hardcoded credentials.

### Setting Up `profiles.yml`
Copy `profiles.yml.example` to your dbt profiles directory (e.g., `~/.dbt/profiles.yml` or locally):

```bash
cp profiles.yml.example profiles.yml
```

The profile reads these variables from your active environment:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_ROLE` (defaults to `ATMOSYNC_INGEST_ROLE`)
- `SNOWFLAKE_DATABASE` (defaults to `ATMOSYNC_DB`)
- `SNOWFLAKE_WAREHOUSE` (defaults to `ATMOSYNC_WH`)
- `SNOWFLAKE_SCHEMA` (defaults to `ANALYTICS`)

---

## 3. Execution Commands

```bash
# Verify project dependencies and connection
dbt debug --profiles-dir .

# Run staging models (materialized as VIEW)
dbt run --select staging --profiles-dir .

# Run analytical marts (materialized as TABLE)
dbt run --select marts --profiles-dir .

# Run complete schema and integrity tests
dbt test --profiles-dir .
```
