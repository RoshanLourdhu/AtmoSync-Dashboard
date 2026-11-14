# AtmoSync Dashboard

> **Real-time container monitoring, spoilage risk prediction, and commodity arbitrage intelligence for perishable goods in transit.**

Repository: [RoshanLourdhu/AtmoSync-Dashboard](https://github.com/RoshanLourdhu/AtmoSync-Dashboard)  
Active Foundation Branch: `roshan`  
Internship Timeline: Official Start Date — **5 November 2026**

---

## 1. Project Overview

AtmoSync is an enterprise-grade IoT streaming, analytical modeling, and business intelligence platform designed to monitor cold-chain cargo containers transporting perishable agricultural commodities (such as Avocados, Mangoes, and Bananas).

### Core Objectives:
1. **IoT Simulation:** Generate realistic telemetric readings (temperature, humidity, vibration) including transit fluctuations and abnormal condition spikes.
2. **Event Streaming:** Stream validated sensor events in real-time over Apache Kafka.
3. **Data Warehousing:** Ingest raw events into Snowflake (`ATMOSYNC_DB.RAW.SENSOR_READINGS`).
4. **Data Modeling & Transformation:** Transform and clean sensor history using dbt models.
5. **Predictive Spoilage Analytics:** Compute cumulative thermal stress and predict commodity-specific spoilage risks.
6. **Market Arbitrage Intelligence:** Analyze destination market prices against transit health to identify rerouting opportunities.
7. **Executive Dashboards:** Visualize real-time container states, transit alerts, and financial intelligence in Apache Superset.

---

## 2. System Architecture

```text
+----------------------------+
|  Python Sensor Simulator   |  (Sam Roshan: sensor_simulator/)
+----------------------------+
              |
              | Raw JSON Events (Validating against sensor_reading.schema.json)
              v
+----------------------------+
|       Kafka Producer       |  (Sam / Roshan: streaming/producer/)
+----------------------------+
              |
              | Topic: container-sensor-data
              v
+----------------------------+
|       Kafka Consumer       |  (Roshan: streaming/consumer/)
+----------------------------+
              |
              | Pre-ingestion validation & dead-letter routing
              v
+----------------------------+
|    Snowflake Raw Table     |  (Roshan: ATMOSYNC_DB.RAW.SENSOR_READINGS)
+----------------------------+
              |
              v
+----------------------------+
|     dbt Transformations    |  (Nisha: dbt_analytics/)
| - stg_sensor_readings      |
| - int_container_conditions |
| - fct_spoilage_risk        |
| - fct_arbitrage_opps       |
+----------------------------+
              |
              v
+----------------------------+
|  Apache Superset Dashboard |  (Navya: dashboard/)
+----------------------------+
```

---

## 3. Team Members & Responsibilities

| Member | Role | Primary Modules | Focus Areas |
|---|---|---|---|
| **Roshan** | Project Lead & Backend Engineer | `streaming/`, `snowflake/`, `shared/` | Architecture coordination, Kafka consumer, schema validation, Snowflake raw table design & ingestion |
| **Sam Roshan** | Sensor Simulation & Producer | `sensor_simulator/`, `streaming/producer/` | IoT sensor telemetry generation, abnormal condition modeling, Kafka producer client integration |
| **Nisha** | Data Engineering & Analytics | `dbt_analytics/` | Data preprocessing, dbt transformation models, spoilage risk formulas, market price integration |
| **Navya** | Visualization & Dashboard | `dashboard/` | Apache Superset configuration, real-time container monitoring views, spoilage alerts, arbitrage charts |

Detailed roles and handoff boundaries are documented in [docs/team_responsibilities.md](docs/team_responsibilities.md).

---

## 4. Branch Strategy & Team Workflow

To prevent code collisions and ensure smooth review integration, all work is partitioned into dedicated feature branches:

* `roshan` — Project lead foundation, Kafka consumer, and Snowflake backend.
* `sam-roshan` — Sensor simulator and Kafka producer development.
* `nisha` — Data cleaning pipelines and dbt analytics models.
* `navya` — Apache Superset dashboards and visualization assets.
* `main` — Protected production integration branch (updated only via Pull Requests).

### Key Rules:
1. Work strictly on your assigned branch. Never commit directly to another member's branch or to `main`.
2. All changes must be tested locally before committing (`pytest tests/`).
3. Commit messages must follow Conventional Commits (e.g., `feat:`, `fix:`, `docs:`).
4. Cross-branch integration into `main` requires an integration Pull Request coordinated by Roshan.

See [docs/branch_workflow.md](docs/branch_workflow.md) and [docs/team_commit_policy.md](docs/team_commit_policy.md) for complete details.

---

## 5. Repository Layout

```text
AtmoSync-Dashboard/
├── README.md                 # Primary project overview and quickstart
├── .gitignore                # Protects credentials and untracked datasets
├── .env.example              # Environment configuration template
├── requirements.txt          # Python dependencies
├── docs/                     # Comprehensive architecture and team documentation
│   ├── project_overview.md   # Project scope and business purpose
│   ├── system_architecture.md# Detailed technical data flow
│   ├── data_contract.md      # Sensor event schema specification
│   ├── integration_guide.md  # Inter-module handoffs and boundaries
│   ├── team_responsibilities.md # Ownership matrix
│   ├── team_commit_policy.md # Internship review commit requirements
│   ├── team_onboarding.md    # Developer onboarding guide (Windows)
│   ├── branch_workflow.md    # Branching rules and PR process
│   └── component_status.md   # Live status and ownership table
├── shared/
│   ├── schemas/              # Formal JSON Schemas (sensor_reading.schema.json)
│   └── config/               # Shared non-secret configuration
├── sensor_simulator/         # [Sam] Mock IoT sensor simulation engine
├── streaming/
│   ├── producer/             # [Sam / Roshan] Kafka event publisher
│   └── consumer/             # [Roshan] Kafka subscriber and validation worker
├── snowflake/
│   ├── schemas/              # [Roshan] Snowflake database and schema DDL
│   └── sql/                  # [Roshan] Table DDL and ingestion SQL
├── dbt_analytics/            # [Nisha] dbt analytical project
├── dashboard/                # [Navya] Apache Superset datasets and dashboard exports
├── tests/                    # Automated unit, schema, and integration tests
└── scripts/                  # Utility and setup automation scripts
```

---

## 6. Local Setup & Quickstart

### Prerequisites
- Windows 10/11 with PowerShell
- Git 2.40+ installed
- Python 3.11+ installed

### Step-by-Step Environment Setup
```powershell
# 1. Clone the repository
git clone https://github.com/RoshanLourdhu/AtmoSync-Dashboard.git
cd AtmoSync-Dashboard

# 2. Switch to your assigned feature branch (example: sam-roshan)
git checkout sam-roshan

# 3. Create and activate a virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 4. Install dependencies
pip install -r requirements.txt

# 5. Create local environment configuration
Copy-Item .env.example .env
```

*Note: Update `.env` with your local connection credentials. Never commit `.env` to Git.*  
Complete step-by-step instructions are available in [docs/team_onboarding.md](docs/team_onboarding.md).

---

## 7. Current Implementation Status

| Component | Owner | Directory | Status |
|---|---|---|---|
| **Sensor Simulator** | Sam | `sensor_simulator/` | Implemented (continuous physics drift, anomaly injection, schema standardization) |
| **Kafka Producer** | Sam / Roshan integration | `streaming/producer/` | Implemented (confluent-kafka, pre-validation, container_id key partitioning, batch delivery) |
| **Kafka Consumer** | Roshan | `streaming/consumer/` | Implemented (contract validation, DLQ quarantine, batching, offset management) |
| **Data Validation** | Roshan | `shared/schemas/` & `tests/` | Implemented (JSON Schema draft 2020-12 runtime validator & automated test suite) |
| **Snowflake Ingestion** | Roshan | `snowflake/` | Implemented (RAW landing DDL, connection manager, single & batch ingestion engine) |
| **Data Transformations & Marts** | Nisha / Roshan integration | `dbt_analytics/` | Implemented (stg_sensor_readings, fct_spoilage_risk, fct_arbitrage_opportunities, schema tests) |
| **Superset Dashboard Stack** | Navya / Roshan integration | `dashboard/` | Implemented (Docker Compose, snowflake-sqlalchemy, KPI blueprint, dataset specs) |

For full details and next implementation steps, see [docs/component_status.md](docs/component_status.md).

---

## 8. Documentation Index

- [Project Overview](docs/project_overview.md) — Scope, technology stack, and objectives.
- [System Architecture](docs/system_architecture.md) — Pipeline components and data flow.
- [Sensor Data Contract](docs/data_contract.md) — Schema v0.1 definitions, field types, and rules.
- [Integration Guide](docs/integration_guide.md) — Module handoff interfaces and protocols.
- [KPI Specification](docs/atmosync_kpi_specification.md) — Formulas and thresholds for thermal, humidity, vibration, and spoilage scores.
- [Dashboard Blueprint](docs/atmosync_dashboard_blueprint.md) — Superset visualization layout, cards, and filtering rules.
- [Dashboard Integration Guide](docs/dashboard_integration.md) — Containerized Apache Superset deployment and Snowflake connection.
- [Team Responsibilities](docs/team_responsibilities.md) — Detailed ownership and roadmaps.
- [Team Git Commit Policy](docs/team_commit_policy.md) — Mid/Final review commit day requirements.
- [Team Onboarding Guide](docs/team_onboarding.md) — Local Windows environment setup and component execution.
- [Branching & Workflow Guide](docs/branch_workflow.md) — Feature branches, testing, and PR guidelines.
- [Component Status Tracker](docs/component_status.md) — Component progress and task breakdowns.
- [Sensor Reading JSON Schema](shared/schemas/sensor_reading.schema.json) — Official Draft 2020-12 event specification.
