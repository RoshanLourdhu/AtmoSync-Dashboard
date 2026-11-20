# AtmoSync Superset Dashboard

This directory contains all assets needed to deploy the Apache Superset environment and data quality monitoring stack for the AtmoSync project.

## Overview
- **Docker Compose (`docker-compose.yml`):** Deploys PostgreSQL 13, Redis 6, and Apache Superset containers with volume persistence and healthchecks.
- **Custom Configuration (`config/superset_config.py`):** Externalizes secrets and SQLAlchemy database URI bindings.
- **Snowflake Drivers (`requirements-local.txt`):** Includes `psycopg2-binary`, `snowflake-sqlalchemy`, and `snowflake-connector-python` to enable direct connectivity to Snowflake analytical data marts.
- **Declarative Specs (`dashboards/datasets_and_charts.yaml`):** Semantic layer declaring 4 datasets (`STG_SENSOR_READINGS`, `FCT_SPOILAGE_RISK`, `FCT_ARBITRAGE_OPPORTUNITIES`, `FCT_SENSOR_DATA_QUALITY`), 15 calculated KPI metrics, 15 chart slices, and 4 dashboard filters (`Date Range`, `Container ID`, `Commodity`, `Quality Status`).
- **Data Quality Suite (`data_quality/`):** Python validation engine evaluating 7-field telemetry contracts, physical boundaries, IQR outliers, quality scores, JSON export, Plotly visualizations, and automated filter configuration validation.

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
