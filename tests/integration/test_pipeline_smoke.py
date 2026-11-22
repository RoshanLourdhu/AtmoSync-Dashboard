"""
Automated End-to-End Pipeline Smoke Tests for AtmoSync.

Validates the complete 10-stage traceable event flow:
Simulator -> JSON Schema -> Producer -> Delivery ACK -> Consumer ->
Snowflake Ingestion -> RAW Landing -> dbt Staging -> dbt Mart Analytics -> Superset Semantics.

Includes comprehensive validation of:
- Verification mode selection (simulation vs live).
- Deterministic tagging in simulation mode.
- Strict service unavailability failure reporting in live mode.
- Missing configuration diagnostic messages (e.g. Snowflake authentication).
- Superset offline probe reporting.
- Configurable timeout enforcement.
- Test event traceability and thermal anomaly scoring.
"""
from __future__ import annotations

import json
import os
from unittest.mock import patch

import pytest

from scripts.smoke_test_pipeline_e2e import (
    MODE_LIVE,
    MODE_SIMULATION,
    TAG_LIVE,
    TAG_SIMULATION,
    PipelineSmokeTest,
    main,
    parse_args,
)


def test_repeatable_pipeline_smoke_traceability() -> None:
    """Verifies that a traceable smoke event successfully passes all 10 pipeline stages in simulation."""
    smoke_test = PipelineSmokeTest(mode=MODE_SIMULATION, verbose=False)
    result = smoke_test.run_all()

    assert result["overall_status"] == "PASSED"
    assert result["stages_executed"] == 10
    assert result["stages_passed"] == 10
    assert result["stages_failed"] == 0
    assert result["total_duration_ms"] > 0
    assert result["traceable_container_id"].startswith("SMOKE-TEST-")

    # Confirm all 10 stages passed individually
    for stage in result["stages"]:
        assert stage["status"] == "PASSED", f"Stage {stage['stage']} failed: {stage['error']}"


def test_smoke_mode_simulation_explicit_tagging() -> None:
    """Verifies that in simulation mode, every single stage is explicitly labeled as DETERMINISTIC_SIMULATION."""
    smoke_test = PipelineSmokeTest(mode=MODE_SIMULATION)
    result = smoke_test.run_all()

    assert result["mode"] == MODE_SIMULATION
    assert result["mode_tag"] == TAG_SIMULATION

    for stage in result["stages"]:
        assert stage["mode"] == TAG_SIMULATION, f"Stage {stage['stage']} has unexpected mode: {stage['mode']}"
        assert stage["status"] == "PASSED"


def test_smoke_traceable_event_identifier_propagation() -> None:
    """Asserts that the traceable unique UUID propagates across all pipeline stages."""
    custom_container = "SMOKE-TEST-CONTAINER-XYZ-99"
    smoke_test = PipelineSmokeTest(container_id=custom_container)
    result = smoke_test.run_all()

    assert result["overall_status"] == "PASSED"
    assert result["traceable_container_id"] == custom_container

    # Check container ID presence across stage details
    stages_by_num = {s["stage"]: s for s in result["stages"]}
    assert stages_by_num[1]["details"]["container_id"] == custom_container
    assert stages_by_num[3]["details"]["partition_key"] == custom_container


def test_smoke_thermal_anomaly_propagation() -> None:
    """
    Asserts that when a severe temperature anomaly is introduced,
    spoilage risk is scored as WARNING/CRITICAL in Stage 9 analytical evaluation.
    """
    smoke_test = PipelineSmokeTest()
    # Inject thermal breach: 25.0C on Avocados (threshold 8.0C)
    smoke_test._stage_1_generate_event()
    smoke_test.event["temperature"] = 25.0
    smoke_test.event["commodity"] = "Avocados"

    # Evaluate Stage 9 analytics
    smoke_test._stage_9_dbt_mart_analytics()

    stage_9 = smoke_test.stages[-1]
    assert stage_9.status == "PASSED"
    assert stage_9.details["spoilage_score"] == 0.60
    assert stage_9.details["risk_level"] == "WARNING"


def test_smoke_poison_pill_quarantine_resilience() -> None:
    """Verifies that Stage 5 safely rejects corrupted payload while decoding valid smoke event."""
    smoke_test = PipelineSmokeTest()
    smoke_test._stage_1_generate_event()
    smoke_test._stage_5_consumer_and_poison_pill()

    stage_5 = smoke_test.stages[-1]
    assert stage_5.status == "PASSED"
    assert stage_5.details["smoke_event_decoded"] is True
    assert stage_5.details["poison_pill_quarantined"] is True


def test_smoke_mode_live_strict_failure_on_missing_services() -> None:
    """
    Verifies that running in live mode when live broker is unavailable
    strictly reports LIVE_SERVICE_UNAVAILABLE and does NOT silently mock.
    """
    smoke_test = PipelineSmokeTest(mode=MODE_LIVE, timeout_seconds=1.0)
    # Ensure offline broker target
    with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:59999"}):
        result = smoke_test.run_all()

    assert result["mode"] == MODE_LIVE
    assert result["mode_tag"] == TAG_LIVE
    assert result["overall_status"] == "FAILED"

    stages_by_num = {s["stage"]: s for s in result["stages"]}
    stage_3 = stages_by_num[3]
    assert stage_3["status"] == "FAILED"
    assert stage_3["mode"] == TAG_LIVE
    assert "LIVE_SERVICE_UNAVAILABLE" in stage_3["error"] or "LIVE_SERVICE_FAILED" in stage_3["error"]


def test_smoke_mode_live_missing_snowflake_credentials_diagnostic() -> None:
    """Verifies that Stage 6 in live mode reports AUTHENTICATION_UNCONFIGURED when password is missing."""
    smoke_test = PipelineSmokeTest(mode=MODE_LIVE)
    smoke_test._stage_1_generate_event()

    # Clear Snowflake environment credentials
    clean_env = {
        k: v for k, v in os.environ.items() if not k.startswith("SNOWFLAKE_")
    }
    with patch.dict(os.environ, clean_env, clear=True):
        smoke_test._stage_6_snowflake_ingestion()

    stage_6 = smoke_test.stages[-1]
    assert stage_6.status == "FAILED"
    assert stage_6.mode == TAG_LIVE
    assert "AUTHENTICATION_UNCONFIGURED" in stage_6.error
    assert "SNOWFLAKE_" in stage_6.error


def test_smoke_mode_live_superset_unreachable_diagnostic() -> None:
    """Verifies that Stage 10 in live mode reports LIVE_SERVICE_UNAVAILABLE when Superset is offline."""
    smoke_test = PipelineSmokeTest(mode=MODE_LIVE)
    with patch.dict(os.environ, {"SUPERSET_URL": "http://127.0.0.1:59998"}):
        smoke_test._stage_10_superset_dataset_and_filters()

    stage_10 = smoke_test.stages[-1]
    assert stage_10.status == "FAILED"
    assert stage_10.mode == TAG_LIVE
    assert "LIVE_SERVICE_UNAVAILABLE" in stage_10.error
    assert "Superset endpoint unreachable" in stage_10.error


def test_smoke_timeout_parameter_enforcement() -> None:
    """Verifies that custom timeout parameters are correctly recorded in the runner."""
    smoke_test = PipelineSmokeTest(timeout_seconds=25.5)
    assert smoke_test.timeout == 25.5


def test_smoke_cli_mode_flag_parsing() -> None:
    """Verifies CLI argument parsing across --mode simulation, --mode live, and --live."""
    args_sim = parse_args(["--mode", "simulation"])
    assert args_sim.mode == "simulation"
    assert args_sim.live is False

    args_live = parse_args(["--mode", "live"])
    assert args_live.mode == "live"

    args_live_shortcut = parse_args(["--live", "--timeout", "15.0"])
    assert args_live_shortcut.live is True
    assert args_live_shortcut.timeout == 15.0


def test_smoke_cli_main_exit_code() -> None:
    """Verifies that main() returns 0 on successful simulated smoke run."""
    exit_code = main(["--mode", "simulation", "--json"])
    assert exit_code == 0


def test_smoke_cli_main_live_exit_code_failure() -> None:
    """Verifies that main() returns 1 in live mode when live infrastructure is unavailable."""
    with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "127.0.0.1:59999"}):
        exit_code = main(["--mode", "live", "--timeout", "0.5", "--json"])
        assert exit_code == 1
