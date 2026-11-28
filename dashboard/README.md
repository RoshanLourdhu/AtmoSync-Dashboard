# AtmoSync Superset Dashboard

This directory contains all assets needed to deploy the Apache Superset environment and data quality monitoring stack for the AtmoSync project.

## Overview
- **Docker Compose (`docker-compose.yml`):** Deploys PostgreSQL 13, Redis 6, and Apache Superset containers with volume persistence and healthchecks.
- **Custom Configuration (`config/superset_config.py`):** Externalizes secrets and SQLAlchemy database URI bindings.
- **Snowflake Drivers (`requirements-local.txt`):** Includes `psycopg2-binary`, `snowflake-sqlalchemy`, and `snowflake-connector-python` to enable direct connectivity to Snowflake analytical data marts.
- **Declarative Specs (`dashboards/datasets_and_charts.yaml`):** Semantic layer targeting Snowflake `ATMOSYNC_DB` (`ANALYTICS_STAGING.STG_SENSOR_READINGS`, `ANALYTICS_MARTS.FCT_SPOILAGE_RISK`, `ANALYTICS_MARTS.FCT_ARBITRAGE_OPPORTUNITIES`, `ANALYTICS_MARTS.FCT_SENSOR_DATA_QUALITY`), 20 calculated KPI metrics, 12 chart slices, and 4 dashboard filters (`Date Range`, `Container ID`, `Commodity`, `Quality Status`). Note: Snowflake reflects unquoted column identifiers in UPPERCASE (`CONTAINER_ID`, `RECORDED_AT`, etc.) when queried via Superset's `/api/v1/chart/data` API.
- **Data Quality Suite (`data_quality/`):** Python validation engine evaluating 7-field telemetry contracts, physical boundaries, IQR outliers, quality scores, JSON export, Plotly visualizations (`quality_dashboard.py`), YAML config validation (`dashboard_config_validator.py`), and filter validation (`filter_validator.py`).

## Current Limitations
- **No Historical Route Map:** The canonical 7-field sensor schema (`container_id`, `shipment_id`, `commodity`, `temperature`, `humidity`, `vibration`, `timestamp`) has no GPS or origin-destination fields; route coordinates are intentionally not fabricated.
- **No Wholesale Market Price Spreads:** `FCT_ARBITRAGE_OPPORTUNITIES` exposes telemetry, spoilage risk, and rerouting recommendations without fabricated market prices.

## Reproducible Superset Dashboard Export & Import
Once a dashboard is configured and verified in Superset (`http://localhost:8088`), export and restore it reproducibly via Superset's CLI (keeping database passwords out of version control):
```bash
# Export dashboard bundle from the running Superset container
docker exec dashboard-superset-1 superset export-dashboards -f /tmp/atmosync_dashboard_export.zip
docker cp dashboard-superset-1:/tmp/atmosync_dashboard_export.zip dashboard/dashboards/atmosync_dashboard_export.zip

# Restore into a clean Superset instance (after superset db upgrade & superset init)
docker cp dashboard/dashboards/atmosync_dashboard_export.zip dashboard-superset-1:/tmp/atmosync_dashboard_export.zip
docker exec dashboard-superset-1 superset import-dashboards -p /tmp/atmosync_dashboard_export.zip -u admin
```

## Quick Start
```bash
# From the repository root
cd dashboard
cp .env.example .env
# Edit .env with your secrets and Snowflake connection parameters
docker compose up -d   # or `docker-compose up -d` if using legacy binary
```

Superset will be reachable at <http://localhost:8088>. Default admin credentials for local development:
- **Username:** admin
- **Password:** admin (or configured via `SUPERSET_ADMIN_PASSWORD` in `.env`)

> **Note:** Before deploying to staging or production, generate a long random `SUPERSET_SECRET_KEY` and set strong unique passwords in `.env`.

## Data Quality & Filter Validation Tests
To run the automated data quality validator and dashboard filter tests:
```bash
python -m unittest discover -s dashboard/data_quality -p "test_*.py" -v
# or with pytest:
pytest dashboard/data_quality/ -v
```

## Shutdown
```bash
docker compose down   # stops containers (add -v to delete persistent volumes & DB data)
```
