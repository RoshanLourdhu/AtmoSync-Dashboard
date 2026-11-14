# AtmoSync – Dashboard Blueprint

## 1. Objective
Provide a centralized dashboard to monitor container micro-climate conditions and support risk-based logistics decisions.

## 2. Target Users
- Logistics Managers
- Supply Chain Analysts
- Business Analysts

## 3. Dashboard Overview
Planned KPI cards:
- Average Temperature
- Average Humidity
- Vibration Index
- At-Risk Container Count

## 4. Sensor Monitoring Charts
| Chart | Visualization | Purpose | Source Mart / Model |
|---|---|---|---|
| Temperature Trend | Line Chart | Monitor temperature over time per container | `stg_sensor_readings` |
| Humidity Trend | Line Chart | Monitor humidity variations | `stg_sensor_readings` |
| Vibration Monitoring | Area Chart | Visualize vibration patterns across transit | `stg_sensor_readings` |
| Spoilage Risk Distribution | Bar / Histogram | Distribution of NORMAL, WARNING, CRITICAL containers | `fct_spoilage_risk` |
| Container Risk Summary | Table | Identify containers requiring immediate attention | `fct_spoilage_risk` |
| Rerouting Readiness | Table / Bar | Containers flagged as reroute candidates with action recommendations | `fct_arbitrage_opportunities` |

## 5. Dashboard Filters
- Date Range (`recorded_at`)
- Commodity (`Avocados`, `Mangoes`, `Bananas`)
- Container ID
- Risk Level (`NORMAL`, `WARNING`, `CRITICAL`)

## 6. Required Dataset Fields
- `container_id`
- `shipment_id`
- `commodity`
- `temperature`
- `humidity`
- `vibration`
- `recorded_at`
- `spoilage_score`
- `risk_level`

## 7. Dependencies
- Apache Superset
- PostgreSQL metadata database
- Validated Snowflake analytical models in `dbt_analytics/` (`stg_sensor_readings`, `fct_spoilage_risk`, `fct_arbitrage_opportunities`)
