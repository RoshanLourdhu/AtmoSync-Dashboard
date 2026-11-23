# Integration guide

## Current state

Interfaces in this guide have been fully implemented, integrated, and validated across the pipeline on branch `roshan`:
- **Sensor Simulator & Kafka Producer:** Continuous physics walk, anomaly injection, and strict JSON Schema validation. Integrated Sam's robust broker parsing (`validate_bootstrap_servers`), topic naming constraints (`validate_topic`), and delivery exceptions (`ProducerDeliveryError`).
- **Kafka Consumer & Ingestion:** Resilient batch ingestion into Snowflake RAW (`ATMOSYNC_DB.RAW.SENSOR_READINGS_RAW`), transactional rollback, dead-letter quarantine, and strict offset commit safety.
- **dbt Analytics & Quality Monitoring:** Models implemented for `stg_sensor_readings`, `fct_spoilage_risk`, `fct_arbitrage_readiness`, and `fct_sensor_data_quality`.
- **Superset Dashboard Stack & Data Quality:** Declarative dataset/chart specifications in `dashboard/dashboards/datasets_and_charts.yaml`, containerized docker compose stack, standalone data quality suite with Plotly visualization, dashboard filter validator (`filter_validator.py`), and full configuration validator (`dashboard_config_validator.py`).
- **End-to-End Pipeline Verification Tooling:** Multi-stage pipeline validation test suite (`verify_pipeline_e2e.py`) and repeatable dual-mode 10-stage traceable smoke test CLI (`scripts/smoke_test_pipeline_e2e.py`, `tests/integration/test_pipeline_smoke.py`) supporting deterministic offline simulation (`--mode simulation`) and strict live infrastructure integration (`--mode live`).
- **Automated Test Suite:** 173 passing automated tests across the codebase (100% pass rate).

Remote branches present on `origin` are `main`, `roshan`, `sam-roshan`, `nisha`, and `navya`. Consolidated integration work is maintained on `roshan`.

## Configuration

1. Copy `.env.example` to `.env`.
2. Fill Kafka and Snowflake placeholders locally when those services exist.
3. Never commit `.env`, passwords, keys, or account secrets.
4. Shared non-secret configuration, when it exists, goes in `shared/config/`. That directory is empty.

`.env.example` is the only environment file that should be tracked. `.gitignore` ignores `.env` and `.env.*`, then un-ignores `.env.example`.

## Handoffs

```text
Sam Roshan
  sensor_simulator
        |
        | JSON object matching sensor_reading.schema.json
        v
Roshan
  streaming/producer  -->  topic container-sensor-data
  streaming/consumer  -->  validates JSON
        |
        | accepted rows only
        v
Roshan
  ATMOSYNC_DB.RAW.SENSOR_READINGS
        |
        v
Nisha
  dbt_analytics
    stg_sensor_readings
    stg_commodity_prices
    int_container_conditions
    fct_spoilage_risk
    fct_arbitrage_opportunities
        |
        v
Navya
  dashboard (Apache Superset on the analytical models)
```

The Snowflake table and the five dbt models are planned names. Creating them is later work.

## Module boundaries

| Module | Owner | Produces | Consumes |
| --- | --- | --- | --- |
| `sensor_simulator/` | Sam Roshan | Sensor JSON events, including normal and abnormal cases | Shared schema |
| `streaming/producer/` | Roshan, fed by Sam's events | Kafka records on `container-sensor-data` | Valid sensor JSON |
| `streaming/consumer/` | Roshan | Load into the raw sensor table | `container-sensor-data` |
| `snowflake/` | Roshan | Database, schema, and raw table design | Validated events |
| `dbt_analytics/` | Nisha | Staging, intermediate, and fact models | `ATMOSYNC_DB.RAW.SENSOR_READINGS` and a future price source |
| `dashboard/` | Navya | Superset datasets, charts, and dashboards | Nisha's analytical models |
| `shared/schemas/` | Whole team, Roshan coordinates | Sensor contract | Nothing |
| `tests/` | Whoever changes a module | Automated checks, when added | Module under test |
| `scripts/` | Whoever needs a repeatable local command | Helper scripts, when added | Local environment |

## Integration rules

- Sam's simulator must not publish to Kafka with a payload that fails the schema. Roshan's consumer must validate again and skip invalid messages for the raw table.
- Roshan delivers rows to `ATMOSYNC_DB.RAW.SENSOR_READINGS`. Nisha's first sensor model, `stg_sensor_readings`, should read that table rather than Kafka.
- Nisha owns price integration, spoilage calculation, and arbitrage analysis. Those rules stay out of the raw event.
- Navya connects Superset to the analytical models. Rerouting recommendations come from those models once Nisha defines them.
- A contract change updates the schema and `docs/data_contract.md` together, then the producer, consumer, and `stg_sensor_readings` follow. See the change process in the data contract.

## Suggested development order

1. Keep the shared schema stable.
2. Sam emits fixture events that pass the schema.
3. Roshan adds topic configuration, the consumer, and the raw table.
4. Nisha builds dbt models from raw data and documents spoilage and price assumptions in the analytics project.
5. Navya builds Superset charts on the published models.

Kafka, Snowflake, and Superset installation is a later environment task. It is outside this foundation.

## GitHub

- Default integration branch for finished work is `main`.
- Keep personal work on the existing personal branch (`roshan`, `sam-roshan`, `nisha`, `navya`).
- Open a pull request for review. Roshan coordinates integration.
- Do not force-push `main`.
- Do not commit `.env` or local datasets.
