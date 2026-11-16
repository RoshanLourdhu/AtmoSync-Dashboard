# AtmoSync – Apache Superset Dashboard Stack (`dashboard/`)

This directory contains the production containerized deployment configuration for **Apache Superset**, delivering real-time cold-chain micro-climate monitoring and commodity spoilage analytics for the AtmoSync platform.

---

## 1. Containerized Service Architecture

The visualization stack is orchestrated via Docker Compose (`dashboard/docker-compose.yml`), comprising three dedicated services:

| Service | Image | Role | Configuration Details |
| :--- | :--- | :--- | :--- |
| **`postgres`** | `postgres:13` | Application Metadata Store | Stores dashboard layouts, slice configurations, user credentials, RBAC permissions, and saved queries. Includes healthcheck (`pg_isready`). |
| **`redis`** | `redis:6` | Caching & Async Broker | In-memory key-value cache accelerating dashboard rendering and query caching across web workers. |
| **`superset`** | `apache/superset:latest` | Web UI & Visualization Engine | Web application server running on port `8088`. Dynamically installs `snowflake-sqlalchemy` driver via `requirements-local.txt` and mounts custom configuration. |

### Volume Management
- `postgres_data`: Persists Superset metadata across container restarts.
- `redis_data`: Persists cached analytical queries.
- `superset_home`: Stores local dashboard asset exports and configuration state.

---

## 2. Snowflake Connection Configuration

Superset connects to Snowflake analytical data marts via the official SQLAlchemy connector (`snowflake-sqlalchemy>=1.5.0` and `snowflake-connector-python>=3.5.0`).

### Connection Steps:
1. Access the Superset Web UI at `http://localhost:8088`.
2. Navigate to **Settings** $\to$ **Database Connections** $\to$ **+ Database**.
3. Select **Snowflake** from the database list.
4. Provide the SQLAlchemy URI format:
   ```text
   snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}&role={role}
   ```
   *Example:*
   ```text
   snowflake://atmosync_svc:YourStrongPassword@xy12345.us-east-1/ATMOSYNC_DB/MARTS?warehouse=ATMOSYNC_WH&role=ATMOSYNC_INGEST_ROLE
   ```
5. Click **Test Connection** to confirm connectivity, then click **Connect**.

> [!IMPORTANT]
> **Credential Security:** Never hardcode passwords or commit `.env` files with live Snowflake credentials. Use dedicated service accounts (`atmosync_svc`) adhering to the principle of least privilege.

---

## 3. Registering dbt Analytical Datasets in Superset

Once the Snowflake connection is active, register the physical tables created by `dbt_analytics/`:

1. Navigate to **Data** $\to$ **Datasets** $\to$ **+ Dataset**.
2. Select database `AtmoSync Snowflake`, schema `STAGING` or `MARTS`, and add:
   - **`STG_SENSOR_READINGS`:** Raw validated time-series telemetry.
   - **`FCT_SPOILAGE_RISK`:** Container-level thermal stress and spoilage risk scores.
   - **`FCT_ARBITRAGE_OPPORTUNITIES`:** Rerouting candidate grid and action recommendations.
   - **`FCT_SENSOR_DATA_QUALITY`:** Comprehensive data-quality profiling classifying readings into PASS, WARNING, and FAIL with granular anomaly flags and issue reasons.

### Configured Calculated Metrics
In the dataset configuration tab, add the pre-defined calculated metrics documented in [dashboard/dashboards/datasets_and_charts.yaml](file:///D:/Infotach%20Internship/Project-1-AtmoSync/dashboard/dashboards/datasets_and_charts.yaml):

#### Telemetry & Spoilage Metrics
| Metric Name | Verbose Name | SQL Expression | Purpose |
| :--- | :--- | :--- | :--- |
| `avg_temperature` | Average Temperature (°C) | `AVG(temperature)` | Mean thermal condition across monitored containers. |
| `max_temperature` | Peak Temperature (°C) | `MAX(temperature)` | Detects extreme thermal spikes in transit. |
| `avg_humidity` | Average Humidity (%) | `AVG(humidity)` | Monitors ambient moisture levels. |
| `vibration_index` | Vibration Index (RMS) | `SQRT(AVG(vibration * vibration))` | Quantifies cumulative mechanical shock and rough transit. |
| `at_risk_container_count` | Total At-Risk Containers | `COUNT(DISTINCT CASE WHEN risk_level IN ('WARNING', 'CRITICAL') THEN container_id END)` | Operational alert count of containers requiring intervention. |
| `reroute_candidates_count` | Containers Needing Rerouting | `COUNT(DISTINCT CASE WHEN is_reroute_candidate = TRUE THEN container_id END)` | Actionable count of containers recommended for rerouting. |

#### Data Quality Monitoring Metrics (`FCT_SENSOR_DATA_QUALITY`)
| Metric Name | Verbose Name | SQL Expression | Purpose |
| :--- | :--- | :--- | :--- |
| `total_sensor_readings` | Total Sensor Readings | `COUNT(*)` | Total volume of audited IoT sensor events arriving in Snowflake RAW. |
| `pass_reading_count` | PASS Readings Count | `COUNT(CASE WHEN quality_status = 'PASS' THEN 1 END)` | Volume of clean, structurally valid sensor readings. |
| `warning_reading_count` | WARNING Readings Count | `COUNT(CASE WHEN quality_status = 'WARNING' THEN 1 END)` | Volume of readings with operational anomalies, duplicate arrivals, or clock drift. |
| `fail_reading_count` | FAIL Readings Count | `COUNT(CASE WHEN quality_status = 'FAIL' THEN 1 END)` | Volume of unprocessable records violating physical bounds or missing required keys. |
| `quality_pass_rate` | Data Quality Pass Rate (%) | `(COUNT(CASE WHEN quality_status = 'PASS' THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0)` | Overall platform compliance and data contract hygiene percentage. |
| `quality_fail_rate` | Data Quality Failure Rate (%) | `(COUNT(CASE WHEN quality_status = 'FAIL' THEN 1 END) * 100.0) / NULLIF(COUNT(*), 0)` | Critical corruption percentage requiring engineering investigation. |
| `warning_or_failed_containers_count` | Containers with Warnings or Failures | `COUNT(DISTINCT CASE WHEN quality_status IN ('WARNING', 'FAIL') THEN container_id END)` | Distinct count of containers exhibiting sensor anomalies or duplicate payloads. |
| `quality_issue_records_count` | Records with Quality Issues | `COUNT(CASE WHEN quality_issue_reasons IS NOT NULL AND TRIM(quality_issue_reasons) != '' THEN 1 END)` | Aggregate count of sensor events carrying diagnostic issue reason tags. |

---

## 4. Dashboard KPIs & Visualization Blueprint

The dashboard layout conforms to [docs/atmosync_dashboard_blueprint.md](file:///D:/Infotach%20Internship/Project-1-AtmoSync/docs/atmosync_dashboard_blueprint.md):

### 4.1 Cold-Chain Operations & Spoilage Intelligence View
1. **Top KPI Tiles:**
   - Big Number cards displaying *Average Temperature*, *Average Humidity*, *Vibration Index*, and *At-Risk Container Count*.
2. **Thermal & Moisture Time-Series:**
   - ECharts line charts plotting temperature and humidity over transit time per container.
3. **Mechanical Vibration Area Chart:**
   - Stacked area chart showing vibration acceleration patterns grouped by commodity.
4. **Spoilage Risk Distribution:**
   - Donut chart displaying proportion of containers in `NORMAL`, `WARNING`, and `CRITICAL` condition.
5. **Operational Intervention Grid:**
   - Detailed interactive table from `FCT_ARBITRAGE_OPPORTUNITIES` listing containers flagged as `is_reroute_candidate = TRUE` alongside prescriptive action recommendations (`IMMEDIATE_REROUTE_OR_LIQUIDATION`, `INSPECT_AND_EXPEDITE`).

### 4.2 Sensor Data Quality Monitoring View
1. **Data Quality Status Distribution:**
   - Pie / Donut chart visualizing proportion of events classified as `PASS`, `WARNING`, or `FAIL`.
2. **Data Quality Trend over Time:**
   - ECharts timeseries line chart plotting event volume by `quality_status` against `recorded_at`.
3. **Most Frequent Quality Issue Reasons:**
   - Horizontal bar chart highlighting top triggered issue reasons (e.g., `MISSING_REQUIRED_FIELDS`, `TEMPERATURE_PHYSICALLY_IMPOSSIBLE`, `HUMIDITY_OUT_OF_BOUNDS`, `DUPLICATE_EVENT`).
4. **Container-wise Quality Breakdown:**
   - Grouped bar chart tracking data health across specific monitored containers.
5. **Commodity-wise Quality Breakdown:**
   - Segmented bar chart comparing data quality across perishable commodity classes (`Avocados`, `Mangoes`, `Bananas`).
6. **Sensor Anomaly Audit Log:**
   - High-density triage table providing container ID, shipment ID, commodity, timestamp, temperature, humidity, vibration, `quality_status`, and `quality_issue_reasons`.

---

## 5. Steps to Create, Assemble, and Publish the Dashboard

Follow this step-by-step workflow when deploying the live dashboard:

1. **Step 1 — Environment Initialization:**
   ```powershell
   cd dashboard
   Copy-Item .env.example .env
   # Edit .env with secure passwords
   docker compose up -d
   ```
2. **Step 2 — Database Connection:**
   Connect Superset to `ATMOSYNC_DB` using the instructions in Section 2.
3. **Step 3 — Dataset Registration:**
   Register the four dbt models (`STG_SENSOR_READINGS`, `FCT_SPOILAGE_RISK`, `FCT_ARBITRAGE_OPPORTUNITIES`, `FCT_SENSOR_DATA_QUALITY`) as physical datasets and add custom metrics.
4. **Step 4 — Slice Authoring:**
   Create charts using the chart specifications in `datasets_and_charts.yaml`.
5. **Step 5 — Dashboard Layout Assembly:**
   Navigate to **Dashboards** $\to$ **+ Dashboard**, create dashboards for **"AtmoSync Cold-Chain Operations & Spoilage Intelligence"** and **"AtmoSync Sensor Data Quality & Anomaly Monitoring"**, and drag the authored charts into a grid layout.
6. **Step 6 — Native Filters Configuration:**
   Add interactive dashboard filters for:
   - Date / Time Range (`recorded_at`)
   - Container ID (`container_id`)
   - Commodity (`Avocados`, `Mangoes`, `Bananas`)
   - Risk Level (`NORMAL`, `WARNING`, `CRITICAL`)
   - Quality Status (`PASS`, `WARNING`, `FAIL`)
7. **Step 7 — Publication & Access Control:**
   Toggle status from **Draft** to **Published** and assign view access to logistics analysts and data platform engineers.

---

## 6. Testing vs. Live Deployment Boundaries

| Stage | Verification Status | Operational Reality |
| :--- | :--- | :--- |
| **Offline Testing (`pytest`)** | Verified in `tests/test_dbt_and_dashboard_integration.py` | Verifies `docker-compose.yml` service dependencies, Python syntax of `superset_config.py`, package declarations in `requirements-local.txt`, all 4 dataset definitions, 15 calculated KPI metrics, 15 chart slices, and native filter targets in `datasets_and_charts.yaml`. |
| **Live Interactive Dashboard** | Pending Live Environment | Requires running Docker daemon, initializing the Superset container stack, and establishing an active connection to live Snowflake data marts. |
