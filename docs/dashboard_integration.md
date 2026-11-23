# Dashboard Integration Guide

This document describes how to integrate the Apache Superset dashboard environment into the AtmoSync project.

## 1. Prerequisites

- Docker Engine (v20.10+) and Docker Compose (v2) installed.
- Python installed for running dashboard configuration validation.
- Recommended: Configure environment variables for sensitive values (see `.env.example`).

## 2. Directory Structure

```text
/dashboard/
├── README.md
├── docker-compose.yml
├── config/
│   └── Custom Superset configuration files
├── dashboards/
│   └── Exported dashboard definitions
├── datasets/
│   └── Exported dataset definitions
├── data_quality/
│   ├── dashboard_config_validator.py
│   └── test_dashboard_config_validator.py
└── screenshots/
    └── Dashboard screenshots
```

## 3. Quick Start

### Step 1: Navigate to the Dashboard Folder

```bash
cd dashboard
```

### Step 2: Configure Environment Variables

Create a `.env` file (optional) to override default configuration values.

Example:

```env
POSTGRES_USER=superset
POSTGRES_PASSWORD=your_secure_password
POSTGRES_DB=superset
REDIS_PASSWORD=your_secure_password
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=your_secure_password
```

**Important:** Do not commit the `.env` file to version control.

### Step 3: Launch the Docker Stack

```bash
docker compose up -d
```

Docker Compose starts the services defined in the dashboard configuration.

### Step 4: Access Apache Superset

Open a browser and navigate to:

```text
http://localhost:8088
```

Log in using the administrator credentials configured for the environment.

### Step 5: Import Dashboards and Datasets

Once Superset is running, dashboards and datasets can be imported through the Superset interface or CLI.

CLI examples:

```bash
docker exec -it <superset_container> superset import-dashboards -p /app/superset_home/dashboards
```

```bash
docker exec -it <superset_container> superset import-datasets -p /app/superset_home/datasets
```

Replace `<superset_container>` with the actual Superset container name.

### Step 6: Shut Down the Services

To stop the services:

```bash
docker compose down
```

**Note:** Avoid using `docker compose down -v` unless you intentionally want to delete persistent volumes and their stored data.

## 4. Dashboard Configuration Validation

The AtmoSync project includes an automated Python-based dashboard configuration validator.

It checks dashboard configuration files before integration to identify missing fields, duplicate definitions, and invalid references.

### 4.1 Run the Configuration Validator

From the project root directory:

```bash
python dashboard/data_quality/dashboard_config_validator.py
```

This command validates the default dashboard configuration.

### 4.2 Validate a Custom Configuration File

To validate a specific YAML configuration file:

```bash
python dashboard/data_quality/dashboard_config_validator.py --config dashboard/dashboards/datasets_and_charts.yaml
```

The `--config` argument allows users to provide a custom configuration file path.

### 4.3 Validation Checks

The validator performs the following checks:

- **Dataset Validation:** Checks dataset definitions and required fields.
- **Metric Validation:** Checks KPI metric definitions and expressions.
- **Chart Validation:** Checks chart configurations and dataset references.
- **Filter Validation:** Checks dashboard filter configurations and their targets.
- **Duplicate Detection:** Identifies duplicate dataset, metric, and chart definitions.
- **Reference Validation:** Identifies missing or unknown dataset and metric references.

### 4.4 Validation Output

The validator displays a summary containing:

- Total datasets.
- Total metrics.
- Total charts.
- Total filters.
- Validation errors.
- Validation warnings.

### 4.5 CLI Exit Codes

| Exit Code | Description |
|---|---|
| 0 | Configuration validation successful |
| 1 | Configuration validation errors detected |
| 2 | Configuration file missing or invalid, or YAML loading error |

These exit codes allow the validator to be integrated into automated scripts and CI/CD workflows.

## 5. Automated Testing

The dashboard configuration validator includes unit tests to verify its functionality.

Run the tests from the project root:

```bash
python -m unittest dashboard.data_quality.test_dashboard_config_validator -v
```

The tests cover:

- Valid configuration handling.
- Actual project YAML validation.
- Duplicate dataset detection.
- Duplicate metric detection.
- Duplicate chart detection.
- Missing metric expressions.
- Unknown chart datasets.
- Unknown chart metrics.
- Unknown filter targets.
- Missing configuration file handling through the CLI.

## 6. Future Extensions

- **Snowflake Connectivity:** Add the Snowflake SQLAlchemy connector to the Superset service and configure the appropriate connection settings. Do not enable it until the integration is ready.
- **TLS / HTTPS:** Configure a reverse proxy such as Nginx and enable TLS certificates.
- **CI/CD:** Integrate dashboard configuration validation into the deployment pipeline.
- **Dashboard Monitoring:** Extend monitoring capabilities for data quality and dashboard configuration.

## 7. Validation Checklist

- [ ] Docker Compose configuration syntax is valid.
- [ ] Required environment variables are configured.
- [ ] Persistent volumes are declared.
- [ ] No hard-coded secrets are committed.
- [ ] Dashboard configuration YAML files are valid.
- [ ] Dataset definitions are verified.
- [ ] KPI metric definitions are verified.
- [ ] Chart and filter references are valid.
- [ ] Dashboard configuration validator executes successfully.
- [ ] Automated validator tests pass.
- [ ] Documentation files are maintained and updated.

---

*This document is part of the AtmoSync repository and should be updated whenever dashboard integration or validation procedures change.*