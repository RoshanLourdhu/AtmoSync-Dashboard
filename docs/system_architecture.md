# System architecture

## Flow

```text
Python Sensor Simulator
        |
        v
  Kafka Producer
        |
        v
container-sensor-data
        |
        v
  Kafka Consumer
        |
        v
Snowflake Raw Tables
        |
        v
 dbt Transformations
        |
        v
Analytical Data Models
        |
        v
Apache Superset Dashboard
```

Each arrow is a planned handoff. None of these runtime components are deployed in the repository yet.

## Components

### Python sensor simulator

Owned by Sam Roshan under `sensor_simulator/`.

Produces temperature, humidity, and vibration readings for a container and shipment, covering normal and abnormal scenarios. Every emitted object must validate against `shared/schemas/sensor_reading.schema.json` before it is handed to the producer.

### Kafka producer

Lives under `streaming/producer/`.

Publishes one validated sensor reading as a JSON message to the Kafka topic `container-sensor-data`. Roshan owns Kafka topic configuration and the producer integration. Sam owns the simulator that supplies the payload.

### Topic `container-sensor-data`

The single raw sensor topic for this foundation. Message value is a JSON object with the seven contract fields and no additional fields. Key selection is not decided yet.

### Kafka consumer

Owned by Roshan under `streaming/consumer/`.

Reads `container-sensor-data`, parses JSON, validates the payload against the shared schema, and writes accepted events to Snowflake. Invalid events must not be written to the raw sensor table. The rejection sink (log, dead-letter topic, or both) is not decided yet.

### Snowflake raw tables

Planned destination:

`ATMOSYNC_DB.RAW.SENSOR_READINGS`

Roshan owns database and schema setup, the raw table design, and sensor ingestion under `snowflake/schemas/` and `snowflake/sql/`. The database, schema, and table are design names only. No DDL has been applied.

The raw table should preserve the seven contract fields. Column types and any ingestion metadata (load time, Kafka offset, partition) are still to be designed when the consumer is implemented.

### dbt transformations

Owned by Nisha under `dbt_analytics/`.

dbt reads from the raw sensor table and from a commodity price source that is not defined yet. Planned models, not implemented:

| Model | Planned role |
| --- | --- |
| `stg_sensor_readings` | Clean, typed sensor readings |
| `stg_commodity_prices` | Clean commodity price inputs |
| `int_container_conditions` | Container-level condition history |
| `fct_spoilage_risk` | Spoilage risk facts |
| `fct_arbitrage_opportunities` | Arbitrage opportunity facts |

Spoilage formulas and price-source mappings are Nisha's analytics work. They are not part of the raw sensor contract.

### Analytical data models

The dbt marts above are the intended source for dashboards. Superset should query these models rather than the raw Kafka payload.

### Apache Superset

Owned by Navya under `dashboard/`.

Planned views: container monitoring, spoilage risk, commodity price analysis, and rerouting recommendations. Warehouse connectivity will use Snowflake. No Superset instance is configured in this repository.

## Ownership boundary

```text
Sam Roshan          sensor_simulator/
Roshan              streaming/  snowflake/  shared contract coordination
Nisha               dbt_analytics/
Navya               dashboard/
Whole team          shared/schemas/sensor_reading.schema.json
```

`shared/config/` is reserved for non-secret configuration shared across modules. It is empty. Secrets stay in an untracked `.env`.

## Runtime status

Kafka, Snowflake, dbt, and Superset are stack choices. This foundation does not install or connect to them.
