# Branching and Git Workflow Guide

## 1. Team Branches Architecture

The AtmoSync project uses a feature-branch and ownership-based Git workflow. Development is partitioned across dedicated branches to prevent code collisions while maintaining accountability:

| Branch Name | Owner | Primary Responsibility | Target Directory |
|---|---|---|---|
| **`roshan`** | Roshan | Project Lead, Kafka Backend, Snowflake Storage & Coordination | `streaming/`, `snowflake/`, `shared/` |
| **`sam-roshan`** | Sam Roshan | Sensor Simulation Engine & Kafka Producer Client | `sensor_simulator/`, `streaming/producer/` |
| **`nisha`** | Nisha | Data Cleaning Pipeline, dbt Analytics Models & Marts | `dbt_analytics/` |
| **`navya`** | Navya | Apache Superset Dashboards, Charts & Visualizations | `dashboard/` |
| **`main`** | Whole Team | Production Integration Branch (Protected, stable code only) | Whole Repository |

---

## 2. Core Workflow Principles

To maintain high code quality and avoid integration bottlenecks, every team member must adhere to the following rules:

### 1. Isolated Branch Development
Each member works exclusively on their assigned branch.  
**Rule:** Never commit directly to another team member's branch or to the `main` branch. If you need functionality from another teammate's module, consult Roshan and coordinate an interface check.

### 2. Pre-Commit Verification
Never commit untested code.  
**Rule:** Before committing, test your scripts locally. Verify syntax, validate payloads against `shared/schemas/sensor_reading.schema.json`, and run unit tests:
```powershell
pytest tests/
```

### 3. Meaningful Commit Messages
Commits are permanent historical records. Use structured, descriptive commit messages adhering to Conventional Commits:
* `feat(component): add specific feature description`
* `fix(component): resolve specific bug description`
* `docs(topic): update documentation or guide`
* `refactor(component): clean up code structure without behavioral change`
* `test(component): add automated unit or integration tests`

*Avoid vague messages such as "update", "changes", "fix", or "Add files via upload".*

### 4. Push to Your Designated Remote Branch
Push changes only to your corresponding remote branch on GitHub:
```powershell
git push origin <your-branch-name>
```

### 5. Integration via Pull Requests (PRs)
The `main` branch represents verified, integrated platform code.  
**Rule:** Direct pushes to `main` are strictly forbidden. When a component milestone is ready, open a Pull Request (PR) from your personal branch targeting `main`.

### 6. Architectural Review & Coordination
Roshan coordinates repository architecture and cross-component integration. Every PR targeting `main` must undergo an integration review to verify:
* Schema compliance with `shared/schemas/sensor_reading.schema.json`.
* Elimination of duplicate records, dead code, and trailing whitespaces.
* Absence of committed secrets, `.env` files, or raw bulky CSV datasets.
* Alignment of module handoffs and file placement.

---

## 3. Step-by-Step Feature Workflow

### Step 1: Syncing with Main
Before starting new work, pull the latest verified changes from `main` into your local feature branch:
```powershell
# Ensure you are on your branch
git checkout <your-branch-name>

# Fetch latest updates from remote
git fetch origin

# Rebase or merge verified updates from main if instructed
git merge origin/main
```

### Step 2: Developing in Your Directory
Confine all new files and modifications to your designated directory. Avoid placing loose scripts or output datasets in the root folder.

### Step 3: Verifying Working Tree Cleanliness
Before staging, verify that only the intended files were modified:
```powershell
git status
```
Ensure untracked files like `.env`, intermediate CSVs, or virtual environment directories (`.venv/`) are not staged.

### Step 4: Staging and Committing
Stage individual files explicitly:
```powershell
git add sensor_simulator/simulator.py
git commit -m "feat(simulator): implement realistic sensor drift with mean reversion"
```

### Step 5: Pushing and Opening a PR
Push your commit to origin:
```powershell
git push origin <your-branch-name>
```
On GitHub, open a Pull Request against `main`. Provide a clear description of:
1. What was implemented.
2. What tests were run.
3. How the output adheres to the team data contract.

---

## 4. Caution on Merging Entire Branches

> [!CAUTION]
> **Do not merge entire unreviewed branches.**  
> Wholesale branch merges without prior file-by-file inspection risk:
> * Overwriting critical foundation files (such as `README.md` or configuration templates).
> * Introducing root-level temporary files or duplicate datasets into the Git history.
> * Breaking existing schema validation checks across dependent services.
> 
> Cross-branch integration must always proceed selectively or via peer-reviewed Pull Requests.

