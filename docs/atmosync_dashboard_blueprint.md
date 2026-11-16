# AtmoSync – Dashboard Blueprint

## 1. Objective
Provide a centralized dashboard to monitor container micro-climate conditions and support risk-based logistics decisions.

## 2. Target Users
- Logistics Managers
- Supply Chain Analysts
- Business Analysts
- Data Operations & Platform Quality Engineers

## 3. Dashboard Overview
Planned KPI cards:
- **Operations & Cold-Chain:**
  - Average Temperature (°C)
  - Average Humidity (%)
  - Vibration Index (RMS)
  - At-Risk Container Count
- **Sensor Data Quality & Reliability:**
  - Total Sensor Readings (`COUNT(*)`)
  - PASS Reading Count (`quality_status = 'PASS'`)
  - WARNING Reading Count (`quality_status = 'WARNING'`)
  - FAIL Reading Count (`quality_status = 'FAIL'`)
  - Data Quality Pass Rate (%)
  - Data Quality Failure Rate (%)
  - Containers with Warnings or Failures
  - Records with Quality Issues

## 4. Visualization Charts

### 4.1 Cold-Chain Operations & Risk Charts
| Chart | Visualization | Purpose | Source Mart / Model |
|---|---|---|---|
| Temperature Trend | Line Chart | Monitor temperature over time per container | `stg_sensor_readings` |
| Humidity Trend | Line Chart | Monitor humidity variations | `stg_sensor_readings` |
| Vibration Monitoring | Area Chart | Visualize vibration patterns across transit | `stg_sensor_readings` |
| Spoilage Risk Distribution | Pie / Donut | Distribution of NORMAL, WARNING, CRITICAL containers | `fct_spoilage_risk` |
| Container Risk Summary | Table | Identify containers requiring immediate attention | `fct_spoilage_risk` |
| Rerouting Readiness | Table | Containers flagged as reroute candidates with action recommendations | `fct_arbitrage_opportunities` |

### 4.2 Sensor Data Quality Monitoring Charts
| Chart | Visualization | Purpose | Source Mart / Model |
|---|---|---|---|
| Data Quality Status Distribution | Pie / Donut | Proportion of readings classified into PASS, WARNING, or FAIL | `fct_sensor_data_quality` |
| Data Quality Trend over Time | Timeseries Line | Telemetry volume classified by quality status over transit time | `fct_sensor_data_quality` |
| Most Frequent Quality Issue Reasons | Bar Chart | Ranking of triggered anomaly codes and diagnostic tags | `fct_sensor_data_quality` |
| Container-wise Quality Breakdown | Grouped Bar | Distribution of quality statuses across active container fleet | `fct_sensor_data_quality` |
| Commodity-wise Quality Breakdown | Bar Chart | Data health and physical plausibility compared across commodities | `fct_sensor_data_quality` |
| Sensor Anomaly Audit Log | Table | Triage log with container ID, timestamps, sensor readings, and issue reasons | `fct_sensor_data_quality` |

## 5. Dashboard Filters
- Date / Time Range (`recorded_at`)
- Commodity (`Avocados`, `Mangoes`, `Bananas`)
- Container ID (`container_id`)
- Risk Level (`NORMAL`, `WARNING`, `CRITICAL`)
- Quality Status (`PASS`, `WARNING`, `FAIL`)

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
- `quality_status`
- `quality_issue_reasons`
- `is_missing_required_fields`
- `is_temperature_physically_impossible`
- `is_temperature_operational_anomaly`
- `is_humidity_out_of_bounds`
- `is_humidity_near_saturation`
- `is_vibration_negative`
- `is_vibration_shock_alert`
- `is_timestamp_missing`
- `is_timestamp_future`
- `is_duplicate_event`
- `is_unknown_commodity`

## 7. Dependencies
- Apache Superset
- PostgreSQL metadata database
- Validated Snowflake analytical models in `dbt_analytics/` (`stg_sensor_readings`, `fct_spoilage_risk`, `fct_arbitrage_opportunities`, `fct_sensor_data_quality`)
