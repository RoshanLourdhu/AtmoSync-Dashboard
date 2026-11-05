# Project overview

## Title

AtmoSync Dashboard: Real-Time Container Monitoring, Spoilage Risk Prediction and Commodity Arbitrage Intelligence Platform.

## Purpose

AtmoSync monitors environmental conditions inside containers that transport perishable commodities. The platform is intended to show when a shipment is drifting into unsafe conditions, estimate spoilage risk for the commodity being carried, and surface commodity price context that may indicate an arbitrage opportunity.

## Internship

- Official start date: 5 November 2026
- Team size: 4
- Repository: https://github.com/RoshanLourdhu/AtmoSync-Dashboard
- Active foundation branch: `roshan`

## Objectives

1. Generate simulated temperature, humidity, and vibration readings, including normal and abnormal scenarios.
2. Stream each reading as a JSON event through Apache Kafka on the topic `container-sensor-data`.
3. Ingest validated raw events into Snowflake.
4. Clean and model the data with dbt.
5. Calculate commodity-specific spoilage risk from the modeled sensor history and agreed business rules.
6. Analyse commodity prices and potential arbitrage opportunities.
7. Visualize container conditions, spoilage risk, prices, and rerouting recommendations in Apache Superset.

## Technology stack

| Layer | Technology |
| --- | --- |
| Simulation and services | Python |
| Event streaming | Apache Kafka |
| Raw and analytical storage | Snowflake |
| Transformations | dbt, SQL |
| Event contract | JSON, JSON Schema |
| Visualization | Apache Superset |
| Source control | GitHub |

## Scope of this foundation

This change establishes the repository layout, shared sensor contract, security ignore rules, and team integration notes.

It does not install Kafka, Snowflake, or Superset, and it does not implement the simulator, producer, consumer, loaders, dbt project, or dashboards.

## Business rules that are intentionally unset

- Commodity-specific spoilage thresholds are not defined. Later analytics work must record them explicitly. They must not be inferred from the sample event.
- The vibration unit is not agreed.
- The temperature unit is not agreed. The contract requires a JSON number and does not encode a unit.

## Development status

The project is in foundation setup ahead of the 5 November 2026 start. Implementation status by module is listed in the root `README.md` and in `docs/team_responsibilities.md`.
