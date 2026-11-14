"""
Automated unit tests for dbt analytics models and Superset dashboard configuration.

Verifies:
1. Valid YAML syntax across dbt project, profile template, sources, and schemas.
2. Strict alignment of dbt staging models with the 7-field JSON Schema contract.
3. Verification of Snowflake RAW landing table references (ATMOSYNC_DB.RAW.SENSOR_READINGS).
4. Validation of spoilage score formula and threshold flags in dbt marts.
5. Verification that arbitrage mart does NOT fabricate unsupported monetary prices.
6. Validation of Superset Docker Compose services, dependencies, and snowflake-sqlalchemy connector.
7. Verification of Superset KPI metric expressions matching the KPI blueprint.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DBT_ROOT = PROJECT_ROOT / "dbt_analytics"
DASHBOARD_ROOT = PROJECT_ROOT / "dashboard"


# =========================================================================
# 1. dbt Project & Configuration Tests
# =========================================================================


def test_dbt_project_config_validity() -> None:
    project_file = DBT_ROOT / "dbt_project.yml"
    assert project_file.exists(), "dbt_project.yml does not exist"

    with open(project_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert config["name"] == "atmosync_analytics"
    assert config["profile"] == "atmosync_profile"
    assert "models" in config["model-paths"]
    assert "clean-targets" in config


def test_dbt_profiles_template_validity() -> None:
    profiles_template = DBT_ROOT / "profiles.yml.example"
    assert profiles_template.exists(), "profiles.yml.example does not exist"

    with open(profiles_template, "r", encoding="utf-8") as f:
        profiles = yaml.safe_load(f)

    assert "atmosync_profile" in profiles
    dev_target = profiles["atmosync_profile"]["outputs"]["dev"]
    assert dev_target["type"] == "snowflake"
    assert "SNOWFLAKE_ACCOUNT" in dev_target["account"]
    assert "SNOWFLAKE_USER" in dev_target["user"]
    assert "SNOWFLAKE_PASSWORD" in dev_target["password"]


def test_dbt_sources_definition() -> None:
    sources_file = DBT_ROOT / "models" / "staging" / "sources.yml"
    assert sources_file.exists(), "models/staging/sources.yml does not exist"

    with open(sources_file, "r", encoding="utf-8") as f:
        sources_data = yaml.safe_load(f)

    source = sources_data["sources"][0]
    assert source["name"] == "raw"
    assert source["schema"] == "RAW"
    tables = {t["name"]: t for t in source["tables"]}
    assert "SENSOR_READINGS" in tables

    columns = [c["name"] for c in tables["SENSOR_READINGS"]["columns"]]
    assert "EVENT_PAYLOAD" in columns
    assert "INGESTED_AT" in columns
    assert "KAFKA_TOPIC" in columns
    assert "KAFKA_PARTITION" in columns
    assert "KAFKA_OFFSET" in columns


# =========================================================================
# 2. dbt SQL Model & Contract Alignment Tests
# =========================================================================


def test_dbt_staging_sql_contract_alignment() -> None:
    staging_sql = DBT_ROOT / "models" / "staging" / "stg_sensor_readings.sql"
    assert staging_sql.exists(), "stg_sensor_readings.sql does not exist"

    content = staging_sql.read_text(encoding="utf-8")

    # Verify source reference
    assert "source('raw', 'SENSOR_READINGS')" in content

    # Verify unpacking of all 7 mandatory contract fields
    contract_fields = [
        "EVENT_PAYLOAD:container_id::STRING",
        "EVENT_PAYLOAD:shipment_id::STRING",
        "EVENT_PAYLOAD:commodity::STRING",
        "EVENT_PAYLOAD:temperature::FLOAT",
        "EVENT_PAYLOAD:humidity::FLOAT",
        "EVENT_PAYLOAD:vibration::FLOAT",
        "EVENT_PAYLOAD:timestamp::TIMESTAMP_TZ",
    ]
    for field in contract_fields:
        assert field in content, f"Missing contract extraction: {field}"

    # Verify Kafka lineage columns
    assert "KAFKA_TOPIC" in content
    assert "KAFKA_PARTITION" in content
    assert "KAFKA_OFFSET" in content
    assert "INGESTED_AT" in content

    # Verify deduplication
    assert "PARTITION BY container_id, recorded_at" in content


def test_dbt_spoilage_risk_mart_logic() -> None:
    mart_sql = DBT_ROOT / "models" / "marts" / "fct_spoilage_risk.sql"
    assert mart_sql.exists(), "fct_spoilage_risk.sql does not exist"

    content = mart_sql.read_text(encoding="utf-8")

    # Verify reference to staging
    assert "ref('stg_sensor_readings')" in content

    # Verify threshold flags
    assert "temp_risk_flag" in content
    assert "hum_risk_flag" in content
    assert "vib_risk_flag" in content

    # Verify agreed composite spoilage score formula
    assert "0.6 * temp_risk_flag" in content
    assert "0.3 * hum_risk_flag" in content
    assert "0.1 * vib_risk_flag" in content

    # Verify risk levels
    assert "CRITICAL" in content
    assert "WARNING" in content
    assert "NORMAL" in content


def test_dbt_arbitrage_mart_no_fabricated_prices() -> None:
    arb_sql = DBT_ROOT / "models" / "marts" / "fct_arbitrage_opportunities.sql"
    assert arb_sql.exists(), "fct_arbitrage_opportunities.sql does not exist"

    content = arb_sql.read_text(encoding="utf-8")

    # Verify reference to spoilage mart
    assert "ref('fct_spoilage_risk')" in content

    # Verify rerouting decision column
    assert "is_reroute_candidate" in content
    assert "logistics_action_recommendation" in content

    # Verify that fake prices are NOT fabricated
    assert "price" not in content.lower() or "price differential" in content.lower()
    assert "dollar" not in content.lower()
    assert "arbitrary" not in content.lower()


def test_dbt_schema_tests_definitions() -> None:
    staging_schema = DBT_ROOT / "models" / "staging" / "schema.yml"
    marts_schema = DBT_ROOT / "models" / "marts" / "schema.yml"

    with open(staging_schema, "r", encoding="utf-8") as f:
        stg_data = yaml.safe_load(f)

    with open(marts_schema, "r", encoding="utf-8") as f:
        marts_data = yaml.safe_load(f)

    # Check staging tests
    stg_model = stg_data["models"][0]
    stg_col_names = [c["name"] for c in stg_model["columns"]]
    assert "container_id" in stg_col_names
    assert "shipment_id" in stg_col_names
    assert "commodity" in stg_col_names

    # Check marts tests
    mart_models = {m["name"]: m for m in marts_data["models"]}
    assert "fct_spoilage_risk" in mart_models
    assert "fct_arbitrage_opportunities" in mart_models


# =========================================================================
# 3. Superset Dashboard Configuration Tests
# =========================================================================


def test_dashboard_docker_compose_validity() -> None:
    compose_file = DASHBOARD_ROOT / "docker-compose.yml"
    assert compose_file.exists(), "docker-compose.yml does not exist"

    with open(compose_file, "r", encoding="utf-8") as f:
        compose_data = yaml.safe_load(f)

    services = compose_data["services"]
    assert "postgres" in services
    assert "redis" in services
    assert "superset" in services

    # Verify healthcheck on postgres
    assert "healthcheck" in services["postgres"]
    # Verify depends_on
    assert "postgres" in services["superset"]["depends_on"]
    assert "redis" in services["superset"]["depends_on"]


def test_dashboard_requirements_include_snowflake() -> None:
    req_file = DASHBOARD_ROOT / "requirements-local.txt"
    assert req_file.exists(), "requirements-local.txt does not exist"

    content = req_file.read_text(encoding="utf-8")
    assert "snowflake-sqlalchemy" in content
    assert "psycopg2-binary" in content
    assert "snowflake-connector-python" in content


def test_dashboard_superset_config_validity() -> None:
    config_file = DASHBOARD_ROOT / "config" / "superset_config.py"
    assert config_file.exists(), "superset_config.py does not exist"

    content = config_file.read_text(encoding="utf-8")
    # Verify Python syntax parses with ast
    ast.parse(content)

    assert "SECRET_KEY" in content
    assert "SQLALCHEMY_DATABASE_URI" in content


def test_dashboard_specifications_validity() -> None:
    specs_file = DASHBOARD_ROOT / "dashboards" / "datasets_and_charts.yaml"
    assert specs_file.exists(), "datasets_and_charts.yaml does not exist"

    with open(specs_file, "r", encoding="utf-8") as f:
        specs_data = yaml.safe_load(f)

    datasets = {d["table_name"]: d for d in specs_data["datasets"]}
    assert "STG_SENSOR_READINGS" in datasets
    assert "FCT_SPOILAGE_RISK" in datasets
    assert "FCT_ARBITRAGE_OPPORTUNITIES" in datasets

    # Verify KPI expressions
    stg_metrics = {m["metric_name"]: m["expression"] for m in datasets["STG_SENSOR_READINGS"]["metrics"]}
    assert stg_metrics["avg_temperature"] == "AVG(temperature)"
    assert stg_metrics["avg_humidity"] == "AVG(humidity)"
    assert "SQRT(AVG(vibration * vibration))" in stg_metrics["vibration_index"]

    risk_metrics = {m["metric_name"]: m["expression"] for m in datasets["FCT_SPOILAGE_RISK"]["metrics"]}
    assert "COUNT(DISTINCT" in risk_metrics["at_risk_container_count"]
