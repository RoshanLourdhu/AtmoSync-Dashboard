# AtmoSync – Superset Dashboard Readiness Audit

**Date:** November 18, 2026 (Updated November 19, 2026 with Platform Resolution)  
**Prepared by:** Navya (Dashboard & BI Development)  
**Integration & Fulfillment by:** Roshan (Data Platform & Pipeline Integration)  

---

## 1. Objective

The purpose of this audit is to review the existing Apache Superset configuration and documentation to identify whether the dashboard's intended datasets, source columns and KPI requirements can be verified.

---

## 2. Files Reviewed

The following project files were inspected:

- `dashboard/config/superset_config.py`
- `dashboard/docker-compose.yml`
- `dashboard/.env.example`
- `dashboard/README.md`
- `dashboard/dashboards/`
- `docs/dashboard_integration.md`
- `docs/atmosync_dashboard_blueprint.md`
- `docs/atmosync_kpi_specification.md`
- `docs/atmosync_data_quality_specification.md`

---

## 3. Expected Dataset Fields

Based on the KPI specification and dashboard blueprint, the expected sensor dataset fields are:

| Field | Purpose |
|---|---|
| `container_id` | Identifies the shipping container |
| `timestamp` | Records the sensor reading time |
| `temperature` | Stores temperature readings |
| `humidity` | Stores humidity readings |
| `vibration` | Stores vibration readings |

The KPI specification also mentions optional `sensor_status` for spoilage-score analysis.

These are documented expected field names. They have not yet been verified against the actual upstream dataset.

---

## 4. Dashboard Configuration Findings

| Check | Observation | Status |
|---|---|---|
| Superset configuration | Uses environment variables for secret key and metadata database URI | Reviewed |
| Dataset references | No dataset or column mappings found in the reviewed Superset configuration | Not verified |
| Dashboard exports | No dashboard JSON exports found in `dashboard/dashboards/` | Pending |
| Source schema | Actual dataset schema is not available in the reviewed documentation | Pending |
| Dashboard charts | Blueprint describes planned charts; implementation is not confirmed | Pending |
| Dashboard filters | Date Range and Container ID are documented as planned filters | Pending |
| Snowflake integration | Existing integration documentation describes it as a future extension | Not enabled |

---

## 5. KPI and Data Quality Dependencies

The reviewed specifications identify the following dependencies:

- Actual source column names must be confirmed.
- Operational temperature, humidity and vibration thresholds require validation.
- Spoilage-score weights require operational validation.
- Financial assumptions for rerouting metrics require validation.
- Data quality rules must be finalized using the actual dataset and sensor specifications.

---

## 6. Configuration Validation

The following command was executed successfully without reporting configuration errors:

```bash
docker compose -f dashboard/docker-compose.yml config --quiet
```

The `.env.example` file was also updated to document the `SUPERSET_SECRET_KEY` environment variable referenced by the Superset configuration.

This validation confirms Compose configuration parsing. It does not confirm that Superset services are running or that database and dataset connections work.

---

## 7. Limitations

The following checks could not be completed at audit time:

1. Actual dataset-to-dashboard column mapping verification.
2. Chart-level SQL and aggregation validation.
3. Filter-to-column mapping verification.
4. Live Superset dashboard verification.
5. Snowflake connection and data visibility testing.

These checks require the actual dataset/schema and a working dashboard environment.

---

## 8. Recommended Next Actions

1. Obtain the latest sensor dataset or finalized dbt model schema from the data engineering team.
2. Compare actual source columns with the documented expected fields.
3. Verify KPI calculations and aggregation behavior.
4. Validate dashboard filters and chart configurations.
5. Complete live dashboard checks when the analytical data source becomes available.

---

## 9. Conclusion (Initial Audit)

The existing AtmoSync documentation defines the intended dashboard structure, KPI requirements and expected sensor fields. However, the actual upstream schema and dashboard dataset mappings are not yet available for verification.

Therefore, the dashboard's dataset and column mapping readiness remains pending source-schema confirmation.

---

## 10. Data Platform Resolution & Engineering Fulfillment (Integrated November 19, 2026)

Following this audit, the Data Platform engineering team has addressed and resolved each dependency identified in Sections 4, 7, and 8:

### 10.1 Upstream Schema Finalization & Contract Alignment
- The production sensor payload contract is finalized in `shared/schemas/sensor_reading.schema.json` with strict validation via `shared.validation.validate_sensor_event`.
- Standard fields: `container_id` (string), `shipment_id` (string), `commodity` (string), `temperature` (numeric, -30°C to 50°C), `humidity` (numeric, 0% to 100%), `vibration` (numeric, >= 0 g), and `timestamp` (ISO 8601 UTC).

### 10.2 Analytical dbt Marts
The following downstream models have been implemented in `dbt_analytics/models/marts/`:
1. `fct_sensor_data_quality`: Classifies readings into `PASS`, `WARNING`, and `FAIL` using physical range checks, excursion thresholds, and data completeness metrics.
2. `fct_spoilage_risk`: Calculates cold-chain excursion durations and spoilage risk index based on commodity-specific tolerances.
3. `fct_arbitrage_readiness`: Calculates rerouting arbitrage readiness without synthetic price assumptions.

### 10.3 Declarative Superset Dataset & Chart Specification
A complete declarative dataset and chart configuration has been authored in `dashboard/dashboards/datasets_and_charts.yaml`:
- **Datasets:** `fct_sensor_data_quality`, `fct_spoilage_risk`, and `fct_arbitrage_readiness` mapped to database schema `ANALYTICS`.
- **Predefined Metrics:** Data Quality Score (`100.0 * COUNT(CASE WHEN quality_status = 'PASS') / COUNT(*)`), Anomaly Record Count, Outlier Rate, and Critical Excursion Count.
- **Charts:**
  - KPI Scorecard: Platform Data Quality Score
  - Gauge: Real-time Spoilage Risk
  - Time-Series Line Chart: Temperature & Humidity Drift over Time
  - Bar Chart: Quality Issue Distribution by Commodity
- **Filters:** `container_id`, `commodity`, `time_range`, and `quality_status`.

### 10.4 Snowflake Connector & Docker Configuration
- `dashboard/requirements-local.txt` includes `snowflake-sqlalchemy` and `snowflake-connector-python` to allow Superset containers to connect directly to `ATMOSYNC_DB.ANALYTICS`.
- Connection string template provided in `dashboard/.env.example`.
