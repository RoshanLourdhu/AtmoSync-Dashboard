"""
Automated Unit Tests for GitHub Actions CI Workflow Configuration.

Validates that .github/workflows/pipeline_ci.yml:
1. Exists and contains valid YAML syntax.
2. Configures mandatory branch triggers (push to main & roshan, PR to main, workflow_dispatch).
3. Targets ubuntu-latest with Python 3.11 and appropriate job timeouts.
4. References existing scripts, configuration files, and requirements in the repository.
"""
from __future__ import annotations

from pathlib import Path
import yaml
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CI_WORKFLOW_PATH = PROJECT_ROOT / ".github" / "workflows" / "pipeline_ci.yml"


@pytest.fixture(scope="module")
def workflow_config() -> dict:
    """Loads and parses the pipeline CI workflow YAML."""
    assert CI_WORKFLOW_PATH.is_file(), f"Workflow file not found at {CI_WORKFLOW_PATH}"
    with CI_WORKFLOW_PATH.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert isinstance(data, dict), "Workflow YAML must parse into a top-level mapping"
    return data


def test_ci_workflow_triggers(workflow_config: dict) -> None:
    """Verifies that CI triggers on push (main, roshan), PR (main), and workflow_dispatch."""
    triggers = workflow_config.get("on")
    assert isinstance(triggers, dict), "Workflow must define an 'on' trigger mapping"

    assert "push" in triggers, "Workflow must trigger on push events"
    push_branches = triggers["push"].get("branches", [])
    assert "main" in push_branches, "Push triggers must monitor 'main'"
    assert "roshan" in push_branches, "Push triggers must monitor 'roshan'"

    assert "pull_request" in triggers, "Workflow must trigger on pull_request events"
    pr_branches = triggers["pull_request"].get("branches", [])
    assert "main" in pr_branches, "Pull request triggers must target 'main'"

    assert "workflow_dispatch" in triggers, "Workflow must support manual workflow_dispatch execution"


def test_ci_workflow_job_configuration(workflow_config: dict) -> None:
    """Verifies that the CI job runs on ubuntu-latest with reasonable timeout and Python 3.11."""
    jobs = workflow_config.get("jobs", {})
    assert "pipeline-ci" in jobs, "Workflow must define a 'pipeline-ci' job"

    job = jobs["pipeline-ci"]
    assert job.get("runs-on") == "ubuntu-latest", "CI job should execute on ubuntu-latest"
    assert job.get("timeout-minutes", 0) <= 20, "CI job timeout must be bounded (<= 20 minutes)"

    steps = job.get("steps", [])
    assert len(steps) >= 5, "CI job must contain multi-stage build and validation steps"

    # Verify Python setup step
    setup_python_steps = [
        s for s in steps if "setup-python" in s.get("uses", "")
    ]
    assert len(setup_python_steps) == 1, "CI job must include actions/setup-python step"
    python_version = setup_python_steps[0].get("with", {}).get("python-version")
    assert str(python_version) == "3.11", "CI job must pin Python 3.11 runtime"


def test_ci_workflow_permissions_concurrency_and_reporting(workflow_config: dict) -> None:
    """Verifies least-privilege permissions, concurrency cancellation, and JUnit/summary reporting."""
    permissions = workflow_config.get("permissions", {})
    assert permissions.get("contents") == "read", "Workflow must enforce read-only contents permission"

    concurrency = workflow_config.get("concurrency", {})
    assert concurrency.get("group"), "Workflow must define a concurrency group"
    assert concurrency.get("cancel-in-progress") is True, "Workflow should cancel superseded runs"

    steps = workflow_config["jobs"]["pipeline-ci"]["steps"]
    pytest_steps = [s for s in steps if "--junitxml=" in s.get("run", "")]
    assert len(pytest_steps) == 1, "Pytest step must emit a JUnit XML report for visibility"

    artifact_steps = [s for s in steps if "upload-artifact" in s.get("uses", "")]
    assert len(artifact_steps) == 1, "CI job must upload test artifacts"
    assert artifact_steps[0].get("if") == "always()", "Artifact upload must run even on test failure"

    summary_steps = [s for s in steps if "GITHUB_STEP_SUMMARY" in s.get("run", "")]
    assert len(summary_steps) == 1, "CI job must publish a step summary"
    assert summary_steps[0].get("if") == "always()", "Step summary must publish even on failure"


def test_ci_referenced_paths_exist_in_repository(workflow_config: dict) -> None:
    """Asserts that all files and directories referenced in workflow steps exist in the workspace."""
    assert (PROJECT_ROOT / "requirements.txt").is_file()
    assert (PROJECT_ROOT / "dashboard" / "data_quality" / "dashboard_config_validator.py").is_file()
    assert (PROJECT_ROOT / "scripts" / "smoke_test_pipeline_e2e.py").is_file()
    assert (PROJECT_ROOT / "dbt_analytics" / "profiles.yml.example").is_file()
    assert (PROJECT_ROOT / "dbt_analytics" / "dbt_project.yml").is_file()
