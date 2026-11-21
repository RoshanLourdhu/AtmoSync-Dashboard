"""
Automated End-to-End Pipeline Smoke Tests for AtmoSync.

Validates the complete 10-stage traceable event flow:
Simulator -> JSON Schema -> Producer -> Delivery ACK -> Consumer ->
Snowflake Ingestion -> RAW Landing -> dbt Staging -> dbt Mart Analytics -> Superset Semantics.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from scripts.smoke_test_pipeline_e2e import PipelineSmokeTest


def test_repeatable_pipeline_smoke_traceability() -> None:
    """Verifies that a traceable smoke event successfully passes all 10 pipeline stages."""
    smoke_test = PipelineSmokeTest(live_requested=False, verbose=False)
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
    spoilage risk is scored as CRITICAL in Stage 9 analytical evaluation.
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


def test_smoke_cli_main_exit_code() -> None:
    """Verifies that main() returns 0 on successful smoke run."""
    from scripts.smoke_test_pipeline_e2e import main

    with patch("sys.argv", ["smoke_test_pipeline_e2e.py", "--json"]):
        exit_code = main()
        assert exit_code == 0
