# AtmoSync Dashboard

Real-time container monitoring, spoilage risk prediction, and commodity arbitrage intelligence for perishable goods in transit.

This repository is the shared foundation for a four-person internship team. The official internship start date is **5 November 2026**. The streaming pipeline, Snowflake objects, dbt models, and Superset dashboards are planned and are not implemented yet.

Repository: [RoshanLourdhu/AtmoSync-Dashboard](https://github.com/RoshanLourdhu/AtmoSync-Dashboard)

## Objectives

- Generate simulated temperature, humidity, and vibration readings.
- Stream sensor events through Apache Kafka.
- Ingest raw events into Snowflake.
- Transform data with dbt.
- Calculate commodity-specific spoilage risk.
- Analyse commodity prices and potential arbitrage opportunities.
- Visualize results in Apache Superset.

## Technology stack

Python, Apache Kafka, Snowflake, dbt, Apache Superset, SQL, JSON, and GitHub.

## Architecture

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

The planned raw landing table is `ATMOSYNC_DB.RAW.SENSOR_READINGS`. Planned dbt models are `stg_sensor_readings`, `stg_commodity_prices`, `int_container_conditions`, `fct_spoilage_risk`, and `fct_arbitrage_opportunities`. Those names are design targets, not deployed objects.

## Repository layout

```text
AtmoSync-Dashboard/
├── README.md
├── .gitignore
├── .env.example
├── docs/
├── shared/
│   ├── schemas/
│   └── config/
├── sensor_simulator/
├── streaming/
│   ├── producer/
│   └── consumer/
├── snowflake/
│   ├── schemas/
│   └── sql/
├── dbt_analytics/
├── dashboard/
├── tests/
└── scripts/
```

Module directories are reserved for implementation. They do not contain pipeline code yet. `shared/config/` is for future non-secret shared configuration. Credentials belong only in a local `.env` file.

## Team

| Person | Role | Primary modules |
| --- | --- | --- |
| Roshan | Project lead and backend engineer | Kafka, consumer, Snowflake ingestion, GitHub integration |
| Sam Roshan | Sensor simulator | `sensor_simulator/` |
| Nisha | Data engineering and analytics | `dbt_analytics/` |
| Navya | Dashboard developer | `dashboard/` |

Details are in [docs/team_responsibilities.md](docs/team_responsibilities.md).

## Documentation

- [Project overview](docs/project_overview.md)
- [System architecture](docs/system_architecture.md)
- [Sensor data contract](docs/data_contract.md)
- [Integration guide](docs/integration_guide.md)
- [Team responsibilities](docs/team_responsibilities.md)
- [Sensor reading JSON Schema](shared/schemas/sensor_reading.schema.json)

## Local configuration

```powershell
Copy-Item .env.example .env
```

Replace the placeholders in `.env`. Do not commit `.env` or any real Kafka or Snowflake credentials. Kafka, Snowflake, dbt, and Superset are not installed by this foundation.

## Development status

| Area | Status |
| --- | --- |
| Repository foundation and documentation | In progress on branch `roshan` |
| Shared sensor event contract | Defined |
| Sensor simulator | Not started |
| Kafka producer and consumer | Not started |
| Snowflake database, schema, and raw table | Designed only (`ATMOSYNC_DB.RAW.SENSOR_READINGS`) |
| dbt models | Named only; not implemented |
| Superset dashboards | Not started |
| Spoilage thresholds | Not defined; do not invent them in the raw contract |
| Vibration unit | Open; requires team agreement |
| Temperature unit | Open; the contract stores a number only |
