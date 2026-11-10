# Dashboard Integration Guide

This document describes how to integrate the Apache Superset dashboard environment into the AtmoSync project.

## Prerequisites
- Docker Engine (v20.10+) and Docker Compose (v2) installed on your machine.
- Recommended: set environment variables for sensitive values (see `.env.example`).

## Directory Structure
```
/dashboard/
├─ README.md               # Overview and quick‑start
├─ docker-compose.yml       # Compose file for PostgreSQL, Redis, Superset
├─ config/                 # Custom Superset config files (e.g., superset_config.py)
├─ dashboards/             # Exported JSON dashboard definitions
├─ datasets/               # Exported JSON dataset definitions
└─ screenshots/            # PNG/JPG screenshots for documentation
```

## Quick Start
1. **Navigate to the `dashboard` folder**
   ```bash
   cd dashboard
   ```
2. **Create a `.env` file** (optional) to override defaults:
   ```env
   POSTGRES_USER=superset
   POSTGRES_PASSWORD=verysecurepassword
   POSTGRES_DB=superset
   REDIS_PASSWORD=anothersecurepwd
   SUPERSET_ADMIN_USERNAME=admin
   SUPERSET_ADMIN_PASSWORD=admin
   ```
   Do **not** commit this file to version control.
3. **Launch the stack**
   ```bash
   docker compose up -d   # or `docker-compose up -d` for the legacy binary
   ```
   Docker will pull the official `apache/superset:latest` image, set up PostgreSQL and Redis, run database migrations, and start Superset on port `8088`.
4. **Access Superset**
   Open a browser and go to `http://localhost:8088`. Log in with the admin credentials defined above (default: `admin` / `admin`).
5. **Import dashboards and datasets**
   - Via UI: *Settings → Import Dashboard* or *Import Dataset*.
   - Via CLI (once Superset is running):
     ```bash
     docker exec -it <superset_container> superset import-dashboards -p /app/superset_home/dashboards
     docker exec -it <superset_container> superset import-datasets -p /app/superset_home/datasets
     ```
6. **Shutdown**
   ```bash
   docker compose down -v
   ```
   The `-v` flag removes the named volumes, deleting all persisted metadata.

## Future Extensions
- **Snowflake connectivity**: Add the Snowflake SQLAlchemy connector to the `superset` service, and set the `SQLALCHEMY_URL` environment variable. Do **not** enable it yet.
- **TLS / HTTPS**: Front the Superset UI with a reverse proxy (e.g., Nginx) and configure TLS certificates.
- **CI/CD**: Include the `dashboard` folder in deployment pipelines.

## Validation Checklist
- [ ] `docker-compose.yml` syntax is valid (`docker compose config` succeeds).
- [ ] All required environment variables are defined (defaults provided).
- [ ] Persistent volumes (`postgres_data`, `redis_data`, `superset_home`) are declared.
- [ ] No hard‑coded secrets are committed.
- [ ] Documentation files (`README.md`, `dashboard_integration.md`) are present.

---
*This file is part of the AtmoSync repository and should be kept up‑to‑date with any changes to the Superset deployment.*

