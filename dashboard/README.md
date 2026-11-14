# AtmoSync – Apache Superset Dashboard (`dashboard/`)

This directory contains the production containerized deployment configuration for **Apache Superset**, delivering real-time micro-climate monitoring and commodity spoilage analytics for the AtmoSync Dashboard platform.

---

## 1. Architecture & Services

The Superset dashboard stack is containerized via Docker Compose:

- **`postgres`:** Relational metadata store for Superset dashboards, slices, user accounts, and permissions.
- **`redis`:** In-memory cache and task queue broker for query caching and asynchronous charting.
- **`superset`:** Web UI application server running `apache/superset:latest` with custom configurations and Snowflake drivers (`snowflake-sqlalchemy`).

```text
+--------------------------------------------------------------+
| Snowflake Analytical Marts (`ATMOSYNC_DB.MARTS`)            |
|  - STG_SENSOR_READINGS                                       |
|  - FCT_SPOILAGE_RISK                                         |
|  - FCT_ARBITRAGE_OPPORTUNITIES                               |
+--------------------------------------------------------------+
                               │
                               │ Snowflake SQLAlchemy connection
                               ▼
+--------------------------------------------------------------+
| Apache Superset Service (`http://localhost:8088`)            |
|  - Overview KPI Tiles: Avg Temp, Avg Hum, Vib RMS, At-Risk   |
|  - Real-time Thermal & Humidity Line Trends                  |
|  - Spoilage Risk Categorization (NORMAL, WARNING, CRITICAL)  |
|  - Rerouting Candidate Table & Action Recommendations        |
+--------------------------------------------------------------+
```

---

## 2. Quick Start

### Step 1: Configure Environment Variables
Copy `.env.example` to `.env` (which is gitignored) and configure passwords:

```bash
cd dashboard
cp .env.example .env
```

### Step 2: Start Containers
```bash
docker compose up -d
```

### Step 3: Access Superset
Navigate to `http://localhost:8088` in your browser.
Log in with configured admin credentials (default: `admin` / `admin`).

### Step 4: Configure Snowflake Connection
In Superset UI (*Settings → Database Connections → + Database*):
1. Select **Snowflake**.
2. Supply the SQLAlchemy URI format:
   ```text
   snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}
   ```
3. Test connection and save.

### Step 5: Connect Datasets
Import or create physical datasets corresponding to dbt models:
- `STG_SENSOR_READINGS` (Staging time-series)
- `FCT_SPOILAGE_RISK` (Container condition risk scores)
- `FCT_ARBITRAGE_OPPORTUNITIES` (Rerouting candidates)

Metric definitions and chart specifications are documented in [dashboard/dashboards/datasets_and_charts.yaml](file:///D:/Infotach%20Internship/Project-1-AtmoSync/dashboard/dashboards/datasets_and_charts.yaml).

---

## 3. Shutdown
```bash
# Stop containers while preserving data volumes
docker compose down

# Stop and remove volumes (clean reset)
docker compose down -v
```
