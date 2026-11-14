# Dashboard Integration Guide

This document describes how to integrate the Apache Superset dashboard environment into the AtmoSync project.

## Prerequisites
- Docker Engine (v20.10+) and Docker Compose (v2) installed on your machine.
- Environment variables configured in `dashboard/.env` (copied from `dashboard/.env.example`).

## Directory Structure
```
dashboard/
├── README.md               # Overview and quick-start guide
├── docker-compose.yml      # Compose file for PostgreSQL, Redis, Superset
├── requirements-local.txt  # Python packages including snowflake-sqlalchemy
├── .env.example            # Environment template for local deployment
├── config/
│   └── superset_config.py  # Custom Superset config (secret keys, features)
└── dashboards/
    └── datasets_and_charts.yaml # Spec for datasets and KPI cards
```

## Quick Start
1. **Navigate to the `dashboard` folder:**
   ```bash
   cd dashboard
   ```
2. **Create local `.env`:**
   ```bash
   cp .env.example .env
   ```
   Set strong passwords for `POSTGRES_PASSWORD`, `REDIS_PASSWORD`, and `SUPERSET_SECRET_KEY`.
3. **Launch the stack:**
   ```bash
   docker compose up -d
   ```
4. **Access Superset:**
   Open a browser at `http://localhost:8088`. Log in with `admin` / `admin` (or your configured admin user).
5. **Connect to Snowflake:**
   In Superset UI (*Settings → Database Connections → + Database*), select **Snowflake** and provide the SQLAlchemy URI:
   ```text
   snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}
   ```
6. **Connect Datasets:**
   Add physical datasets for:
   - `ATMOSYNC_DB.STAGING.STG_SENSOR_READINGS`
   - `ATMOSYNC_DB.MARTS.FCT_SPOILAGE_RISK`
   - `ATMOSYNC_DB.MARTS.FCT_ARBITRAGE_OPPORTUNITIES`
