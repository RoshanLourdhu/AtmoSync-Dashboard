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
| Chart | Visualization | Purpose |
|---|---|---|
| Temperature Trend | Line Chart | Monitor temperature over time |
| Humidity Trend | Line Chart | Monitor humidity variations |
| Vibration Monitoring | Area Chart | Visualize vibration patterns |
| Container Risk | Table | Identify containers requiring attention |
| Rerouting Opportunities | Bar Chart | Compare potential rerouting benefits |

## 5. Dashboard Filters
- Date Range
- Container ID

## 6. Required Dataset Fields
- Container ID
- Timestamp
- Temperature
- Humidity
- Vibration

Exact source column names require validation against the actual dataset.

## 7. Dependencies
- Apache Superset
- PostgreSQL metadata database
- Validated sensor dataset
- Confirmed operational risk thresholds

## 8. Implementation Status
This document describes the planned dashboard design. Actual charts and KPI calculations are not yet implemented.

Sensor thresholds, financial assumptions and source schema remain pending validation.
