"""
Automated Unit Tests for Mid-Review Acceptance Checks and Kafka Benchmark Logic.

Tests configuration validation, throughput/latency/failure calculations,
schema validation, dbt result classification, Snowflake/Superset blocked
reporting, and explicit PASS/FAIL/BLOCKED/NOT_RUN status differentiation.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from scripts.benchmark_kafka_pipeline import (
    STATUS_BLOCKED as KAFKA_BLOCKED,
    BenchmarkConfig,
    compute_benchmark_metrics,
    parse_first_broker_host_port,
    run_kafka_benchmark,
    validate_benchmark_config,
)
from scripts.mid_review_readiness import (
    BLOCKED,
    FAIL,
    NOT_RUN,
    PASS,
    check_schema_and_dashboard_config,
    check_snowflake_raw_table,
    check_superset_health,
    check_superset_raw_dataset,
    classify_dbt_command_result,
    has_real_snowflake_credentials,
    run_mid_review_readiness,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def test_benchmark_config_validation_accepts_valid_and_rejects_invalid() -> None:
    validate_benchmark_config(BenchmarkConfig(bootstrap_servers="localhost:9092", topic="container-sensor-data", message_count=100, batch_size=25))
    assert parse_first_broker_host_port("localhost:9092,broker2:9093") == ("localhost", 9092)

    with pytest.raises(ValueError):
        validate_benchmark_config(BenchmarkConfig(message_count=0))
    with pytest.raises(ValueError):
        validate_benchmark_config(BenchmarkConfig(batch_size=-5))
    with pytest.raises(ValueError):
        validate_benchmark_config(BenchmarkConfig(bootstrap_servers="  "))
    with pytest.raises(ValueError):
        parse_first_broker_host_port("localhost:notaport")


def test_compute_benchmark_metrics_throughput_and_missing_messages() -> None:
    metrics = compute_benchmark_metrics(
        requested_messages=100,
        messages_produced=100,
        messages_consumed=95,
        schema_valid_messages=93,
        schema_invalid_messages=2,
        production_duration_sec=2.0,
        consumption_duration_sec=4.0,
        latencies_ms=[10.0, 20.0, 30.0],
        consumer_lag=5,
    )
    assert metrics["failed_or_unprocessed_messages"] == 7  # 5 unconsumed + 2 schema-invalid
    assert metrics["production_throughput_msg_per_sec"] == 50.0
    assert metrics["consumption_throughput_msg_per_sec"] == 23.75
    assert metrics["avg_latency_ms"] == 20.0
    assert metrics["max_latency_ms"] == 30.0
    assert metrics["consumer_lag"] == 5


def test_run_kafka_benchmark_reports_live_kafka_unavailable_when_unreachable() -> None:
    with patch(
        "scripts.benchmark_kafka_pipeline.check_kafka_broker_reachable",
        return_value=(False, "Connection refused"),
    ):
        res = run_kafka_benchmark(BenchmarkConfig(bootstrap_servers="127.0.0.1:19092", message_count=50, batch_size=10))
    assert res.status == KAFKA_BLOCKED
    assert "LIVE_KAFKA_UNAVAILABLE" in (res.reason or "")
    assert res.messages_produced == 0
    assert res.messages_consumed == 0


def test_schema_and_dashboard_config_validation_passes_on_repo() -> None:
    res = check_schema_and_dashboard_config(PROJECT_ROOT)
    assert res.status == PASS
    assert res.details["validated_events"] >= 3


def test_dbt_result_classification_distinguishes_pass_and_fail() -> None:
    status_ok, msg_ok = classify_dbt_command_result(0, "Success", "", "parse")
    assert status_ok == PASS
    assert "succeeded" in msg_ok

    status_err, msg_err = classify_dbt_command_result(2, "", "Compilation Error in stg_sensor_readings", "compile")
    assert status_err == FAIL
    assert "failed" in msg_err


def test_snowflake_missing_or_dummy_credentials_reported_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ci_dummy_account")
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_WAREHOUSE", raising=False)

    has_creds, missing = has_real_snowflake_credentials()
    assert has_creds is False
    assert "SNOWFLAKE_ACCOUNT" in missing
    assert "SNOWFLAKE_PASSWORD" in missing

    res = check_snowflake_raw_table(PROJECT_ROOT)
    assert res.status == BLOCKED
    assert "missing" in res.summary.lower()


def test_superset_unavailable_reported_blocked() -> None:
    # Port 1 is reserved/unreachable, avoiding accidental local service hits
    health_res = check_superset_health("http://127.0.0.1:1")
    assert health_res.status == BLOCKED

    dataset_res = check_superset_raw_dataset(PROJECT_ROOT, superset_url="http://127.0.0.1:1")
    assert dataset_res.status == BLOCKED
    assert dataset_res.details.get("static_config_verified") is True


def test_mid_review_readiness_distinguishes_pass_fail_blocked_not_run(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)
    monkeypatch.delenv("SNOWFLAKE_WAREHOUSE", raising=False)

    offline_report = run_mid_review_readiness(PROJECT_ROOT, offline=True)
    statuses = {c["check_id"]: c["status"] for c in offline_report["checks"]}
    assert statuses["schema_validation"] == PASS
    assert statuses["dbt_parse"] == PASS
    assert statuses["dbt_compile"] == BLOCKED
    assert statuses["kafka_live_throughput"] == NOT_RUN
    assert statuses["kafka_lag"] == NOT_RUN
    assert statuses["snowflake_raw_table_query"] == NOT_RUN
    assert statuses["superset_health"] == NOT_RUN
    assert statuses["superset_raw_dataset_query"] == NOT_RUN
    assert offline_report["counts"][FAIL] == 0
