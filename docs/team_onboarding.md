# Team Onboarding Guide

Welcome to the **AtmoSync Dashboard** project! This guide walks you through setting up your local Windows development environment, understanding the codebase architecture, working on your assigned component, and safely committing your code.

---

## 1. Prerequisites & Git Installation

### Installing Git on Windows
If Git is not installed on your machine, open PowerShell as Administrator and run:
```powershell
winget install --id Git.Git -e --source winget
```
Alternatively, download and run the official installer from [git-scm.com](https://git-scm.com/download/win).

After installation, verify Git by running:
```powershell
git --version
```

Configure your Git identity (use your GitHub username and email):
```powershell
git config --global user.name "Your Name"
git config --global user.email "your.email@example.com"
```

### Installing Python 3.11+
Verify Python is available:
```powershell
python --version
```
If Python is not installed, install Python 3.11 or later via `winget install Python.Python.3.11` or from [python.org](https://www.python.org/).

---

## 2. Cloning the Repository

Open Windows PowerShell, navigate to your preferred workspace directory (for example, `D:\Infotach Internship`), and clone the repository:

```powershell
cd "D:\Infotach Internship"
git clone https://github.com/RoshanLourdhu/AtmoSync-Dashboard.git Project-1-AtmoSync
cd Project-1-AtmoSync
```

---

## 3. Opening the Project in Cursor or Antigravity

Open the project folder directly in your preferred editor:

* **In Cursor:**
  ```powershell
  cursor .
  ```
  *(Or open Cursor $\rightarrow$ File $\rightarrow$ Open Folder $\rightarrow$ Select `Project-1-AtmoSync`)*

* **In Google Antigravity / VS Code:**
  ```powershell
  code .
  ```
  *(Or open the editor $\rightarrow$ File $\rightarrow$ Open Folder $\rightarrow$ Select `Project-1-AtmoSync`)*

---

## 4. Branch Management: Check and Switch Branches

The repository has dedicated branches for each team member:
- `roshan` — Project Lead, Kafka Backend & Snowflake Integration
- `sam-roshan` — Sensor Simulator & Kafka Producer
- `nisha` — Data Preprocessing & dbt Analytics
- `navya` — Apache Superset Dashboard

### Check your current branch:
```powershell
git branch
```
The active branch is marked with an asterisk (`*`).

### View all local and remote branches:
```powershell
git branch -a
```

### Switch to your assigned branch:
To work on your branch, switch to it:

* **For Sam:**
  ```powershell
  git checkout sam-roshan
  ```
* **For Nisha:**
  ```powershell
  git checkout nisha
  ```
* **For Navya:**
  ```powershell
  git checkout navya
  ```
* **For Roshan:**
  ```powershell
  git checkout roshan
  ```

If switching for the first time from remote:
```powershell
git checkout -b <branch-name> origin/<branch-name>
```

> [!WARNING]
> Always verify `git branch` before making any edits. Never make commits directly on `main` or on a teammate's branch.

---

## 5. Setting Up Python Virtual Environment & Dependencies

From the project root (`Project-1-AtmoSync`), set up a virtual environment:

```powershell
# Create virtual environment
python -m venv .venv

# Activate virtual environment in PowerShell
.\.venv\Scripts\Activate.ps1
```
*(If PowerShell blocks script execution, run: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`)*

Install the project dependencies:
```powershell
pip install --upgrade pip
pip install -r requirements.txt
```

Set up your local configuration by copying `.env.example`:
```powershell
Copy-Item .env.example .env
```
Update `.env` with your local credentials. Remember: `.env` is gitignored and must never be committed to Git.

---

## 6. Understanding the Folder Structure

Place your code only in your designated component directory:

```text
AtmoSync-Dashboard/
├── README.md                 # Project entry point and architecture summary
├── .gitignore                # Git ignore rules (protects credentials and raw CSVs)
├── .env.example              # Template environment configuration
├── requirements.txt          # Python dependencies
├── docs/                     # System documentation, contracts, and onboarding guides
│   ├── project_overview.md   # Project scope and business context
│   ├── system_architecture.md# End-to-end data pipeline design
│   ├── data_contract.md      # Sensor event schema rules and definitions
│   ├── integration_guide.md  # Module interfaces and handoff specifications
│   ├── team_responsibilities.md # Member ownership breakdown
│   ├── team_commit_policy.md # Review commit rules and qualifying day criteria
│   ├── team_onboarding.md    # This onboarding guide
│   ├── branch_workflow.md    # Branching, testing, and PR workflow
│   └── component_status.md   # Live status and ownership table
├── shared/
│   ├── schemas/              # Formal JSON Schemas (sensor_reading.schema.json)
│   └── config/               # Shared non-secret configuration files
├── sensor_simulator/         # [Sam] Sensor generation scripts and abnormal event logic
├── streaming/
│   ├── producer/             # [Sam / Roshan] Kafka producer publishing sensor events
│   └── consumer/             # [Roshan] Kafka consumer ingesting and validating records
├── snowflake/
│   ├── schemas/              # [Roshan] Database and schema definitions
│   └── sql/                  # [Roshan] DDL table creation and ingestion queries
├── dbt_analytics/            # [Nisha] dbt project (staging, intermediate, fact models)
├── dashboard/                # [Navya] Apache Superset datasets, charts, and dashboards
├── tests/                    # Automated unit, schema, and integration tests
└── scripts/                  # Automation and local execution helper scripts
```

---

## 7. How to Run Your Assigned Component

### Sam — Sensor Simulator & Producer (`sensor_simulator/` & `streaming/producer/`)
* **Goal:** Generate mock IoT readings for cargo containers (Avocados, Mangoes, Bananas) and stream valid events.
* **Schema Contract:** Every generated event must conform strictly to `shared/schemas/sensor_reading.schema.json`.
* **Execution:**
  ```powershell
  python sensor_simulator/simulator.py
  ```
  Ensure events contain `container_id`, `shipment_id`, `commodity`, `temperature`, `humidity`, `vibration`, and `timestamp` (ISO 8601 with `Z`).

### Roshan — Kafka Consumer & Snowflake (`streaming/consumer/` & `snowflake/`)
* **Goal:** Ingest messages from topic `container-sensor-data`, validate against the schema, and land clean records into `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
* **Execution:**
  ```powershell
  python streaming/consumer/consumer.py
  ```

### Nisha — Data Engineering & dbt Analytics (`dbt_analytics/`)
* **Goal:** Transform raw sensor readings into analytical marts (`stg_sensor_readings`, `int_container_conditions`, `fct_spoilage_risk`, `fct_arbitrage_opportunities`).
* **Execution:**
  ```powershell
  cd dbt_analytics
  dbt debug
  dbt run
  dbt test
  cd ..
  ```

### Navya — Apache Superset Dashboard (`dashboard/`)
* **Goal:** Connect Superset to Snowflake analytical models and build real-time container condition monitoring, spoilage alerts, and arbitrage insights.
* **Assets:** Exported Superset dashboard slices, dashboard JSON definitions, and SQL queries stored in `dashboard/`.

---

## 8. Committing and Pushing Changes

Follow this safe workflow whenever you make changes:

1. **Check status of modified files:**
   ```powershell
   git status
   ```
2. **Review your diff to make sure no unwanted files or secrets are staged:**
   ```powershell
   git diff
   ```
3. **Stage specific files (never use `git add .` blindly):**
   ```powershell
   git add path/to/your/file.py
   ```
4. **Commit with a descriptive message:**
   ```powershell
   git commit -m "feat(simulator): add container vibration threshold handling"
   ```
5. **Push to your remote branch:**
   ```powershell
   git push origin <your-branch-name>
   ```

> [!TIP]
> Before opening a pull request or pushing major updates, run automated checks:
> ```powershell
> pytest tests/
> ```

