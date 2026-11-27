#!/usr/bin/env python3
"""
AtmoSync Mid-Project Review Readiness Validation Runner.

Executes offline and live acceptance checks across the AtmoSync pipeline and
reports explicit statuses:
  - PASS    : Check genuinely executed and succeeded.
  - FAIL    : Check executed and detected a defect or mismatch.
  - BLOCKED : Required external service, credential, or OS dependency unavailable.
  - NOT_RUN : Check was intentionally skipped (e.g. --offline / --skip-kafka).

Never fabricates live results or turns BLOCKED / NOT_RUN into PASS.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_quality.dashboard_config_validator import validate_dashboard_config
from scripts.benchmark_kafka_pipeline import (
    STATUS_BLOCKED as KAFKA_BLOCKED,
    STATUS_PASS as KAFKA_PASS,
    BenchmarkConfig,
    run_kafka_benchmark,
)
from sensor_simulator.simulator import SensorSimulator
from shared.validation import validate_sensor_event

PASS = "PASS"
FAIL = "FAIL"
BLOCKED = "BLOCKED"
NOT_RUN = "NOT_RUN"

REQUIRED_SENSOR_FIELDS = (
    "container_id",
    "shipment_id",
    "commodity",
    "temperature",
    "humidity",
    "vibration",
    "timestamp",
)
REQUIRED_SNOWFLAKE_ENV_VARS = (
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_WAREHOUSE",
)


@dataclass
class CheckResult:
    """Result for a single Mid-Review acceptance check."""

    check_id: str
    name: str
    status: str  # PASS, FAIL, BLOCKED, NOT_RUN
    summary: str
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_schema_and_dashboard_config(repo_root: Path = PROJECT_ROOT) -> CheckResult:
    """Validate 7-field simulator events against JSON Schema and Superset YAML config."""
    try:
        simulator = SensorSimulator()
        events = simulator.generate_batch()
        for ev in events:
            validate_sensor_event(ev)

        yaml_path = repo_root / "dashboard" / "dashboards" / "datasets_and_charts.yaml"
        if not yaml_path.is_file():
            return CheckResult(
                check_id="schema_validation",
                name="Schema & Dashboard Config Validation",
                status=FAIL,
                summary=f"Missing dashboard config: {yaml_path}",
            )
        cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8"))
        dash_res = validate_dashboard_config(cfg)
        if dash_res.get("status") != "Passed":
            return CheckResult(
                check_id="schema_validation",
                name="Schema & Dashboard Config Validation",
                status=FAIL,
                summary=f"Dashboard config validation failed: {dash_res.get('errors')}",
                details={"dashboard_validation": dash_res},
            )
        return CheckResult(
            check_id="schema_validation",
            name="Schema & Dashboard Config Validation",
            status=PASS,
            summary=f"Validated {len(events)} simulator events against 7-field JSON Schema and Superset YAML config.",
            details={
                "validated_events": len(events),
                "dashboard_summary": dash_res.get("summary", {}),
            },
        )
    except Exception as exc:
        return CheckResult(
            check_id="schema_validation",
            name="Schema & Dashboard Config Validation",
            status=FAIL,
            summary=f"Schema validation error: {exc}",
        )


def _check_dbt_staging_contracts(dbt_dir: Path) -> list[str]:
    """Verify dbt staging sources.yml and stg_sensor_readings.sql match RAW.SENSOR_READINGS."""
    issues: list[str] = []
    sources_path = dbt_dir / "models" / "staging" / "sources.yml"
    stg_sql_path = dbt_dir / "models" / "staging" / "stg_sensor_readings.sql"

    if not sources_path.is_file():
        issues.append(f"Missing {sources_path}")
    else:
        sources_data = yaml.safe_load(sources_path.read_text(encoding="utf-8")) or {}
        sources_list = sources_data.get("sources", [])
        raw_src = next((s for s in sources_list if s.get("name") == "raw"), None)
        if raw_src is None or raw_src.get("schema") != "RAW":
            issues.append("sources.yml missing 'raw' source with schema 'RAW'.")
        else:
            tables = [t.get("name") for t in raw_src.get("tables", [])]
            if "SENSOR_READINGS" not in tables:
                issues.append("sources.yml missing table 'SENSOR_READINGS'.")

    if not stg_sql_path.is_file():
        issues.append(f"Missing {stg_sql_path}")
    else:
        sql_text = stg_sql_path.read_text(encoding="utf-8")
        for field_name in REQUIRED_SENSOR_FIELDS:
            if f"EVENT_PAYLOAD:{field_name}" not in sql_text:
                issues.append(f"stg_sensor_readings.sql missing extraction for '{field_name}'.")
        if "source('raw', 'SENSOR_READINGS')" not in sql_text:
            issues.append("stg_sensor_readings.sql does not reference source('raw', 'SENSOR_READINGS').")

    return issues


def _find_dbt_executable() -> str | None:
    """Locate the dbt CLI executable in PATH or alongside the active Python interpreter."""
    dbt_bin = shutil.which("dbt")
    if dbt_bin:
        return dbt_bin
    candidate = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    if candidate.is_file():
        return str(candidate)
    scripts_candidate = Path(sys.executable).parent / "Scripts" / "dbt.exe"
    if scripts_candidate.is_file():
        return str(scripts_candidate)
    return None


def classify_dbt_command_result(returncode: int, stdout: str, stderr: str, mode: str) -> tuple[str, str]:
    """Classify dbt parse or compile subprocess outcome into PASS or FAIL."""
    combined = (stdout + "\n" + stderr).strip()
    if returncode == 0:
        return PASS, f"dbt {mode} succeeded cleanly."
    last_lines = " | ".join(combined.splitlines()[-5:]) if combined else f"exit code {returncode}"
    return FAIL, f"dbt {mode} failed (exit {returncode}): {last_lines}"


def check_dbt_parse(repo_root: Path = PROJECT_ROOT) -> CheckResult:
    """Verify dbt staging SQL contracts and run offline `dbt parse --no-partial-parse`."""
    dbt_dir = repo_root / "dbt_analytics"
    if not (dbt_dir / "dbt_project.yml").is_file():
        return CheckResult(
            check_id="dbt_parse",
            name="dbt Parse & Staging Contract",
            status=FAIL,
            summary=f"Missing dbt_project.yml in {dbt_dir}",
        )

    contract_issues = _check_dbt_staging_contracts(dbt_dir)
    if contract_issues:
        return CheckResult(
            check_id="dbt_parse",
            name="dbt Parse & Staging Contract",
            status=FAIL,
            summary="; ".join(contract_issues),
        )

    dbt_bin = _find_dbt_executable()
    if not dbt_bin:
        return CheckResult(
            check_id="dbt_parse",
            name="dbt Parse & Staging Contract",
            status=BLOCKED,
            summary="dbt CLI executable not found in PATH or active Python environment.",
        )

    example_profile = dbt_dir / "profiles.yml.example"
    with tempfile.TemporaryDirectory() as tmp_dir:
        profiles_dir = Path(tmp_dir)
        if example_profile.is_file():
            shutil.copy2(example_profile, profiles_dir / "profiles.yml")
        env = os.environ.copy()
        env.setdefault("SNOWFLAKE_ACCOUNT", "ci_dummy_account")
        env.setdefault("SNOWFLAKE_USER", "ci_dummy_user")
        env.setdefault("SNOWFLAKE_PASSWORD", "ci_dummy_password")
        env.setdefault("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
        env.setdefault("SNOWFLAKE_DATABASE", "ATMOSYNC_DB")
        env.setdefault("SNOWFLAKE_ROLE", "TRANSFORMER")

        proc = subprocess.run(
            [dbt_bin, "parse", "--project-dir", str(dbt_dir), "--profiles-dir", str(profiles_dir), "--no-partial-parse"],
            capture_output=True,
            text=True,
            env=env,
            check=False,
        )
        status, msg = classify_dbt_command_result(proc.returncode, proc.stdout, proc.stderr, "parse")
        return CheckResult(
            check_id="dbt_parse",
            name="dbt Parse & Staging Contract",
            status=status,
            summary=f"{msg} Verified all 7 sensor fields in stg_sensor_readings.sql and RAW.SENSOR_READINGS source.",
            details={"returncode": proc.returncode},
        )


def has_real_snowflake_credentials(env: dict[str, str] | None = None) -> tuple[bool, list[str]]:
    """Return (True, []) when non-placeholder Snowflake credentials are configured."""
    source_env = env if env is not None else os.environ
    missing: list[str] = []
    for var in REQUIRED_SNOWFLAKE_ENV_VARS:
        val = (source_env.get(var) or "").strip()
        if not val or val.startswith("ci_dummy_"):
            missing.append(var)
    return len(missing) == 0, missing


def check_dbt_compile(repo_root: Path = PROJECT_ROOT) -> CheckResult:
    """Run `dbt compile` only when real Snowflake credentials are configured."""
    has_creds, missing = has_real_snowflake_credentials()
    if not has_creds:
        return CheckResult(
            check_id="dbt_compile",
            name="dbt Compile (Snowflake Target)",
            status=BLOCKED,
            summary=f"Snowflake credentials missing ({', '.join(missing)}). Configure environment variables to run dbt compile.",
            details={"missing_env_vars": missing},
        )

    dbt_bin = _find_dbt_executable()
    if not dbt_bin:
        return CheckResult(
            check_id="dbt_compile",
            name="dbt Compile (Snowflake Target)",
            status=BLOCKED,
            summary="dbt CLI executable not found in PATH or active Python environment.",
        )

    dbt_dir = repo_root / "dbt_analytics"
    example_profile = dbt_dir / "profiles.yml.example"
    with tempfile.TemporaryDirectory() as tmp_dir:
        profiles_dir = Path(tmp_dir)
        if example_profile.is_file():
            shutil.copy2(example_profile, profiles_dir / "profiles.yml")
        proc = subprocess.run(
            [dbt_bin, "compile", "--project-dir", str(dbt_dir), "--profiles-dir", str(profiles_dir), "--no-partial-parse"],
            capture_output=True,
            text=True,
            check=False,
        )
        status, msg = classify_dbt_command_result(proc.returncode, proc.stdout, proc.stderr, "compile")
        return CheckResult(
            check_id="dbt_compile",
            name="dbt Compile (Snowflake Target)",
            status=status,
            summary=msg,
            details={"returncode": proc.returncode},
        )


def check_snowflake_raw_table(repo_root: Path = PROJECT_ROOT) -> CheckResult:
    """Verify Snowflake RAW table DDL/config and query ATMOSYNC_DB.RAW.SENSOR_READINGS when authorized."""
    ddl_path = repo_root / "snowflake" / "sql" / "001_create_raw_sensor_readings.sql"
    if not ddl_path.is_file() or "ATMOSYNC_DB.RAW.SENSOR_READINGS" not in ddl_path.read_text(encoding="utf-8"):
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=FAIL,
            summary="Snowflake RAW table DDL missing or does not define ATMOSYNC_DB.RAW.SENSOR_READINGS.",
        )

    has_creds, missing = has_real_snowflake_credentials()
    if not has_creds:
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=BLOCKED,
            summary=f"Snowflake credentials missing ({', '.join(missing)}); live query blocked.",
            details={"missing_env_vars": missing, "ddl_verified": True},
        )

    try:
        from snowflake.connection import SnowflakeConnectionError, get_snowflake_config, get_snowflake_connection
    except ImportError as exc:
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=BLOCKED,
            summary=f"Snowflake connector native extension blocked or unavailable: {exc}",
            details={"ddl_verified": True},
        )

    try:
        cfg = get_snowflake_config()
        db = cfg["database"]
        schema = cfg["schema"]
        conn = get_snowflake_connection(cfg)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT COUNT(*), MAX(INGESTED_AT) FROM {db}.{schema}.SENSOR_READINGS")
            row = cur.fetchone()
            cur.execute(f"SELECT EVENT_PAYLOAD FROM {db}.{schema}.SENSOR_READINGS LIMIT 1")
            sample = cur.fetchone()
        finally:
            conn.close()
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=PASS,
            summary=f"Queried {db}.{schema}.SENSOR_READINGS successfully (row_count={row[0] if row else 0}).",
            details={"row_count": row[0] if row else 0, "sample_present": sample is not None},
        )
    except SnowflakeConnectionError as exc:
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=BLOCKED,
            summary=f"Snowflake connection blocked: {exc}",
        )
    except Exception as exc:
        return CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=FAIL,
            summary=f"Snowflake raw table query failed: {exc}",
        )


def check_superset_health(superset_url: str | None = None) -> CheckResult:
    """Check whether the Apache Superset webserver /health endpoint is reachable."""
    base_url = (superset_url or os.getenv("SUPERSET_URL", "http://localhost:8088")).rstrip("/")
    health_url = f"{base_url}/health"
    try:
        with urllib.request.urlopen(health_url, timeout=3.0) as resp:
            code = getattr(resp, "status", resp.getcode())
            body = resp.read().decode("utf-8", errors="replace").strip()
            if code == 200:
                return CheckResult(
                    check_id="superset_health",
                    name="Superset Health Endpoint",
                    status=PASS,
                    summary=f"Superset reachable at {health_url} (HTTP 200).",
                    details={"url": health_url, "body": body[:200]},
                )
            return CheckResult(
                check_id="superset_health",
                name="Superset Health Endpoint",
                status=FAIL,
                summary=f"Superset health endpoint returned HTTP {code}.",
                details={"url": health_url},
            )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return CheckResult(
            check_id="superset_health",
            name="Superset Health Endpoint",
            status=BLOCKED,
            summary=f"Superset service unavailable at {health_url}: {exc}",
            details={"url": health_url},
        )


def check_superset_raw_dataset(
    repo_root: Path = PROJECT_ROOT,
    superset_url: str | None = None,
) -> CheckResult:
    """Verify Superset dataset config and query live Superset dataset API when available."""
    yaml_path = repo_root / "dashboard" / "dashboards" / "datasets_and_charts.yaml"
    if not yaml_path.is_file():
        return CheckResult(
            check_id="superset_raw_dataset_query",
            name="Superset Raw Dataset Query",
            status=FAIL,
            summary=f"Missing Superset dataset specification: {yaml_path}",
        )
    cfg = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    datasets = cfg.get("datasets", [])
    stg_ds = next((d for d in datasets if d.get("table_name") == "STG_SENSOR_READINGS"), None)
    if stg_ds is None or cfg.get("database_name") != "AtmoSync Snowflake":
        return CheckResult(
            check_id="superset_raw_dataset_query",
            name="Superset Raw Dataset Query",
            status=FAIL,
            summary="datasets_and_charts.yaml does not bind STG_SENSOR_READINGS to 'AtmoSync Snowflake'.",
        )

    base_url = (superset_url or os.getenv("SUPERSET_URL", "http://localhost:8088")).rstrip("/")
    username = os.getenv("SUPERSET_ADMIN_USERNAME", "admin")
    password = os.getenv("SUPERSET_ADMIN_PASSWORD", "admin")
    login_url = f"{base_url}/api/v1/security/login"
    payload = json.dumps({"username": username, "password": password, "provider": "db"}).encode("utf-8")
    req = urllib.request.Request(login_url, data=payload, headers={"Content-Type": "application/json"}, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
        access_token = token_data.get("access_token")
        if not access_token:
            return CheckResult(
                check_id="superset_raw_dataset_query",
                name="Superset Raw Dataset Query",
                status=BLOCKED,
                summary="Superset login did not return an access_token; cannot query live dataset.",
            )
        ds_req = urllib.request.Request(
            f"{base_url}/api/v1/dataset/",
            headers={"Authorization": f"Bearer {access_token}"},
        )
        with urllib.request.urlopen(ds_req, timeout=3.0) as ds_resp:
            ds_json = json.loads(ds_resp.read().decode("utf-8"))
        names = [item.get("table_name") for item in ds_json.get("result", [])]
        if "STG_SENSOR_READINGS" in names:
            return CheckResult(
                check_id="superset_raw_dataset_query",
                name="Superset Raw Dataset Query",
                status=PASS,
                summary="Superset API confirmed live dataset 'STG_SENSOR_READINGS' registered.",
                details={"datasets": names},
            )
        return CheckResult(
            check_id="superset_raw_dataset_query",
            name="Superset Raw Dataset Query",
            status=FAIL,
            summary=f"Superset reachable, but 'STG_SENSOR_READINGS' not found in live datasets: {names}",
        )
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        return CheckResult(
            check_id="superset_raw_dataset_query",
            name="Superset Raw Dataset Query",
            status=BLOCKED,
            summary=(
                f"Superset API unavailable at {base_url} ({exc}). "
                "Static config binds 'AtmoSync Snowflake' -> 'STG_SENSOR_READINGS', but live rendering is unverified."
            ),
            details={"static_config_verified": True, "url": base_url},
        )


def check_kafka_checks(
    skip_kafka: bool = False,
    message_count: int = 200,
    batch_size: int = 50,
) -> tuple[CheckResult, CheckResult]:
    """Run live Kafka throughput & lag benchmark or return explicit NOT_RUN / BLOCKED statuses."""
    if skip_kafka:
        return (
            CheckResult(
                check_id="kafka_live_throughput",
                name="Kafka Live Throughput",
                status=NOT_RUN,
                summary="Skipped (--skip-kafka / --offline).",
            ),
            CheckResult(
                check_id="kafka_lag",
                name="Kafka Consumer Lag",
                status=NOT_RUN,
                summary="Skipped (--skip-kafka / --offline).",
            ),
        )

    bench = run_kafka_benchmark(BenchmarkConfig(message_count=message_count, batch_size=batch_size))
    if bench.status == KAFKA_BLOCKED:
        return (
            CheckResult(
                check_id="kafka_live_throughput",
                name="Kafka Live Throughput",
                status=BLOCKED,
                summary=bench.reason or "LIVE_KAFKA_UNAVAILABLE",
                details=bench.to_dict(),
            ),
            CheckResult(
                check_id="kafka_lag",
                name="Kafka Consumer Lag",
                status=BLOCKED,
                summary=bench.reason or "LIVE_KAFKA_UNAVAILABLE",
            ),
        )

    tput_status = PASS if bench.status == KAFKA_PASS else FAIL
    tput_check = CheckResult(
        check_id="kafka_live_throughput",
        name="Kafka Live Throughput",
        status=tput_status,
        summary=(
            f"Produced {bench.messages_produced}/{bench.requested_messages} "
            f"({bench.production_throughput_msg_per_sec} msg/s), "
            f"consumed {bench.messages_consumed} ({bench.consumption_throughput_msg_per_sec} msg/s)."
        ),
        details=bench.to_dict(),
    )

    if bench.consumer_lag is None:
        lag_check = CheckResult(
            check_id="kafka_lag",
            name="Kafka Consumer Lag",
            status=FAIL,
            summary="Broker did not expose reliable partition lag offsets.",
        )
    elif bench.consumer_lag == 0:
        lag_check = CheckResult(
            check_id="kafka_lag",
            name="Kafka Consumer Lag",
            status=PASS,
            summary="Confirmed 0 consumer lag across assigned partitions.",
            details={"consumer_lag": 0},
        )
    else:
        lag_check = CheckResult(
            check_id="kafka_lag",
            name="Kafka Consumer Lag",
            status=FAIL,
            summary=f"Detected non-zero consumer lag: {bench.consumer_lag} messages.",
            details={"consumer_lag": bench.consumer_lag},
        )
    return tput_check, lag_check


def run_mid_review_readiness(
    repo_root: Path = PROJECT_ROOT,
    offline: bool = False,
    skip_kafka: bool = False,
    kafka_messages: int = 200,
    kafka_batch_size: int = 50,
) -> dict[str, Any]:
    """Execute all Mid-Review acceptance checks and return a structured readiness report."""
    schema_check = check_schema_and_dashboard_config(repo_root)
    kafka_tput, kafka_lag = check_kafka_checks(
        skip_kafka=offline or skip_kafka,
        message_count=kafka_messages,
        batch_size=kafka_batch_size,
    )
    dbt_parse_check = check_dbt_parse(repo_root)
    dbt_compile_check = check_dbt_compile(repo_root)

    if offline:
        sf_check = CheckResult(
            check_id="snowflake_raw_table_query",
            name="Snowflake Raw Table Query",
            status=NOT_RUN,
            summary="Skipped (--offline).",
        )
        sup_health = CheckResult(
            check_id="superset_health",
            name="Superset Health Endpoint",
            status=NOT_RUN,
            summary="Skipped (--offline).",
        )
        sup_dataset = CheckResult(
            check_id="superset_raw_dataset_query",
            name="Superset Raw Dataset Query",
            status=NOT_RUN,
            summary="Skipped (--offline).",
        )
    else:
        sf_check = check_snowflake_raw_table(repo_root)
        sup_health = check_superset_health()
        sup_dataset = check_superset_raw_dataset(repo_root)

    checks = [
        schema_check,
        kafka_tput,
        kafka_lag,
        dbt_parse_check,
        dbt_compile_check,
        sf_check,
        sup_health,
        sup_dataset,
    ]
    counts = {
        PASS: sum(1 for c in checks if c.status == PASS),
        FAIL: sum(1 for c in checks if c.status == FAIL),
        BLOCKED: sum(1 for c in checks if c.status == BLOCKED),
        NOT_RUN: sum(1 for c in checks if c.status == NOT_RUN),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": "offline" if offline else "full",
        "counts": counts,
        "checks": [c.to_dict() for c in checks],
    }


def format_readiness_report(report: dict[str, Any]) -> str:
    """Format the readiness report as a readable terminal summary."""
    counts = report["counts"]
    lines = [
        "=" * 80,
        "ATMOSYNC MID-PROJECT REVIEW READINESS REPORT",
        "=" * 80,
        f"Generated At : {report['generated_at']}",
        f"Mode         : {report['mode']}",
        f"Summary      : PASS={counts[PASS]} | FAIL={counts[FAIL]} | BLOCKED={counts[BLOCKED]} | NOT_RUN={counts[NOT_RUN]}",
        "-" * 80,
        f"{'Check ID':<28} {'Status':<10} Summary",
        "-" * 80,
    ]
    for chk in report["checks"]:
        lines.append(f"{chk['check_id']:<28} {chk['status']:<10} {chk['summary']}")
    lines.append("=" * 80)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run AtmoSync Mid-Review acceptance checks.")
    parser.add_argument("--offline", action="store_true", help="Run only offline checks; mark live checks NOT_RUN.")
    parser.add_argument("--skip-kafka", action="store_true", help="Skip live Kafka throughput/lag benchmark.")
    parser.add_argument("--kafka-messages", type=int, default=200)
    parser.add_argument("--kafka-batch-size", type=int, default=50)
    parser.add_argument("--json", action="store_true", help="Output JSON instead of terminal table.")
    args = parser.parse_args(argv)

    report = run_mid_review_readiness(
        repo_root=PROJECT_ROOT,
        offline=args.offline,
        skip_kafka=args.skip_kafka,
        kafka_messages=args.kafka_messages,
        kafka_batch_size=args.kafka_batch_size,
    )
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(format_readiness_report(report))

    return 1 if report["counts"][FAIL] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
