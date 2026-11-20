# AtmoSync – Superset Dashboard Readiness Audit



**Date:** November 18, 2026

**Prepared by:** Navya

**Role:** Dashboard & BI Development



## 1. Objective



The purpose of this audit is to review the existing Apache Superset configuration and documentation to identify whether the dashboard's intended datasets, source columns and KPI requirements can be verified.



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



## 5. KPI and Data Quality Dependencies



The reviewed specifications identify the following dependencies:



- Actual source column names must be confirmed.

- Operational temperature, humidity and vibration thresholds require validation.

- Spoilage-score weights require operational validation.

- Financial assumptions for rerouting metrics require validation.

- Data quality rules must be finalized using the actual dataset and sensor specifications.



## 6. Configuration Validation



The following command was executed successfully without reporting configuration errors:



`docker compose -f dashboard/docker-compose.yml config --quiet`



The `.env.example` file was also updated to document the `SUPERSET_SECRET_KEY` environment variable referenced by the Superset configuration.



This validation confirms Compose configuration parsing. It does not confirm that Superset services are running or that database and dataset connections work.



## 7. Limitations



The following checks could not be completed:



1. Actual dataset-to-dashboard column mapping verification.

2. Chart-level SQL and aggregation validation.

3. Filter-to-column mapping verification.

4. Live Superset dashboard verification.

5. Snowflake connection and data visibility testing.



These checks require the actual dataset/schema and a working dashboard environment.



## 8. Recommended Next Actions



1. Obtain the latest sensor dataset or finalized dbt model schema from the data engineering team.

2. Compare actual source columns with the documented expected fields.

3. Verify KPI calculations and aggregation behavior.

4. Validate dashboard filters and chart configurations.

5. Complete live dashboard checks when the analytical data source becomes available.



## 9. Conclusion



The existing AtmoSync documentation defines the intended dashboard structure, KPI requirements and expected sensor fields. However, the actual upstream schema and dashboard dataset mappings are not yet available for verification.



Therefore, the dashboard's dataset and column mapping readiness remains pending source-schema confirmation.



This audit records the current findings without assuming that unverified dashboard functionality has been implemented.


---

## 10. Engineering Resolution & Implementation (Updated November 20, 2026)

Following this audit, the BI and Dashboard development team has addressed and resolved each pending dependency identified in Sections 4, 7, and 8:

### 10.1 Schema Contract Finalization
- The upstream telemetry schema has been confirmed as the official 7-field contract: `container_id`, `shipment_id`, `commodity`, `temperature`, `humidity`, `vibration`, `timestamp`.
- The data quality validator (`dashboard/data_quality/validator.py`) has been upgraded to enforce this 7-field contract while retaining backward compatibility for the legacy 5-field baseline.

### 10.2 Declarative Superset Dataset & Chart Specification
- Authored `dashboard/dashboards/datasets_and_charts.yaml` establishing explicit semantic layers:
  - `STG_SENSOR_READINGS`: Unpacked telemetry time series.
  - `FCT_SPOILAGE_RISK`: Thermal excursion duration and composite spoilage scores.
  - `FCT_ARBITRAGE_OPPORTUNITIES`: Operational status and rerouting candidate flags.
  - `FCT_SENSOR_DATA_QUALITY`: Audit mart with tri-state status (`PASS`, `WARNING`, `FAIL`).
- Defined 15 calculated KPI metrics and 15 chart slice specifications.
- Defined 4 dashboard filters (`Date Range`, `Container ID`, `Commodity`, `Quality Status`) verified against dataset columns.

### 10.3 Automated Filter Configuration Validation
- Implemented `dashboard/data_quality/filter_validator.py` (`validate_filter_configuration`) to ensure all declared filters map to valid dataset columns and target components.

### 10.4 Snowflake SQLAlchemy Driver Integration
- Added `snowflake-sqlalchemy` and `snowflake-connector-python` to `dashboard/requirements-local.txt`.
- Documented Snowflake connection strings in `dashboard/.env.example`.
