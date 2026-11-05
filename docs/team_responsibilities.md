# Team responsibilities

Official internship start: 5 November 2026.

The split below is the working agreement for the foundation. Items marked **planned** are not implemented.

## Roshan — project lead and backend engineer

Coordination, GitHub integration, and the path from Kafka to Snowflake.

Owned paths:

- `streaming/producer/` (Kafka publish path and topic configuration)
- `streaming/consumer/`
- `snowflake/schemas/`
- `snowflake/sql/`
- Shared contract coordination in `shared/schemas/`

### Backend roadmap

All of the following are planned components:

| Work item | Intended result | Status |
| --- | --- | --- |
| Kafka infrastructure | Local or shared broker settings documented through `.env.example` | Not implemented |
| Kafka topic configuration | Topic `container-sensor-data` | Not implemented |
| Kafka consumer | Read that topic from group `atmosync-sensor-consumer` | Not implemented |
| JSON parsing and validation | Accept only events that match `shared/schemas/sensor_reading.schema.json` | Schema written; parser not implemented |
| Snowflake database and schema | `ATMOSYNC_DB` and schema `RAW` | Designed only |
| Raw sensor table | `ATMOSYNC_DB.RAW.SENSOR_READINGS` | Designed only |
| Sensor data ingestion | Insert validated readings into the raw table | Not implemented |
| Integration with Sam's simulator | Producer accepts simulator output that passes the contract | Not implemented |
| Delivery to Nisha's dbt pipeline | Raw table is the source for `stg_sensor_readings` | Not implemented |

Planned dbt model names, owned by Nisha and consumed after ingestion:

- `stg_sensor_readings`
- `stg_commodity_prices`
- `int_container_conditions`
- `fct_spoilage_risk`
- `fct_arbitrage_opportunities`

Roshan does not define spoilage thresholds or arbitrage rules. Those stay with Nisha.

## Sam Roshan — sensor simulator

Owned path: `sensor_simulator/`.

- Build the Python sensor simulator.
- Generate temperature, humidity, and vibration readings.
- Cover normal and abnormal scenarios.
- Validate readings against the shared schema before they are passed to Roshan's producer.
- Agree the vibration unit, and the temperature unit, with Roshan and Nisha before abnormal scenarios depend on a physical scale.

Status: not started. No thresholds are stored in the shared contract.

## Nisha — data engineering and analytics

Owned path: `dbt_analytics/`.

- Configure dbt.
- Clean and transform sensor data, starting at `stg_sensor_readings` from `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
- Integrate commodity prices through `stg_commodity_prices`. The price source is not chosen yet.
- Calculate spoilage risk in `fct_spoilage_risk`. Document every threshold in the analytics project when it is chosen.
- Analyse arbitrage opportunities in `fct_arbitrage_opportunities`.
- Publish `int_container_conditions` for container-level history.

Status: model names are reserved. The dbt project is not created.

## Navya — dashboard developer

Owned path: `dashboard/`.

- Configure Apache Superset.
- Connect Superset to Snowflake.
- Build container monitoring dashboards.
- Visualize spoilage risk.
- Visualize commodity price analysis.
- Present rerouting recommendations from the analytical models.

Status: not started. Dashboards should use Nisha's models rather than the raw topic payload.

## Shared duties

- Keep `docs/data_contract.md` and `shared/schemas/sensor_reading.schema.json` aligned.
- Keep secrets out of Git. Use `.env` locally and `.env.example` for placeholders.
- Review contract changes before a producer and consumer diverge.
