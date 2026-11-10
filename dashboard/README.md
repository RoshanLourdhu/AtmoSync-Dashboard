# AtmoSync Superset Dashboard

This directory contains all assets needed to deploy the Apache Superset environment for the AtmoSync project.

## Overview
- Docker Compose configuration (`docker-compose.yml`) sets up PostgreSQL, Redis and Superset containers.
- `config/` can hold custom Superset configuration files (e.g., `superset_config.py`).
- `dashboards/` will store JSON export files for Superset dashboards.
- `datasets/` will store JSON export files for Superset datasets.
- `screenshots/` will hold screenshots of dashboards for documentation.

## Quick Start
```bash
# From the repository root
cd dashboard
docker compose up -d   # or `docker-compose up -d` if you have the legacy binary
```

Superset will be reachable at <http://localhost:8088>. The default admin credentials are:
- **Username:** admin (temporary, local‑development only)
- **Password:** admin (temporary, local‑development only)

> **Note:** Before deploying to any environment, configure your own strong Superset admin credentials via the `.env` file or environment variables.


## Shutdown
```bash
docker compose down   # stops containers (add -v to also delete persistent volumes & DB data)
```

## Next Steps
- Add custom configuration in `config/`.
- Import dashboards and datasets using Superset UI or CLI.
- Update `docs/dashboard_integration.md` with detailed integration steps.

