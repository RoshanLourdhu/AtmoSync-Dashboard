"""
Unit tests for AtmoSync Dashboard Filter Configuration Validator.

Verifies:
1. Valid filter configurations against target datasets and chart IDs.
2. Rejection of non-dictionary filter items and missing keys.
3. Column schema validation against dataset columns.
4. Unknown chart ID detection in filter scopes.
5. Warning generation for filters with empty scopes.
6. Rejection of duplicate filter names.
7. Compatibility with both 'name'/'filter_name' and 'scope'/'targets' schemas.
8. Validation against the actual project datasets_and_charts.yaml filter definitions.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from dashboard.data_quality.filter_validator import validate_filter_configuration


@pytest.fixture
def dataset_columns() -> list[str]:
    """Sample dataset columns conforming to 7-field schema plus metadata."""
    return [
        "container_id",
        "shipment_id",
        "commodity",
        "timestamp",
        "recorded_at",
        "temperature",
        "humidity",
        "vibration",
        "quality_status",
    ]


@pytest.fixture
def chart_ids() -> list[str]:
    """Sample chart IDs."""
    return [
        "temperature_chart",
        "humidity_chart",
        "vibration_chart",
        "spoilage_gauge",
    ]


def test_valid_filter_configuration(
    dataset_columns: list[str], chart_ids: list[str]
) -> None:
    """Verifies valid filter configuration passes with 0 errors."""
    filters = [
        {
            "name": "Container ID",
            "column": "container_id",
            "scope": ["temperature_chart", "humidity_chart"],
        }
    ]
    result = validate_filter_configuration(filters, dataset_columns, chart_ids)
    assert result["status"] == "Passed"
    assert result["total_filters"] == 1
    assert result["errors"] == []
    assert "Container ID" in result["validated_filters"]


def test_invalid_filter_structure() -> None:
    """Verifies non-dictionary configurations and missing keys are detected."""
    filters: list[Any] = [
        "not-a-dict",
        {"column": "container_id"},  # missing name
    ]
    result = validate_filter_configuration(filters)
    assert result["status"] == "Failed"
    assert len(result["errors"]) == 2


def test_invalid_dataset_column(
    dataset_columns: list[str], chart_ids: list[str]
) -> None:
    """Verifies filters referencing non-existent columns are rejected."""
    filters = [
        {
            "name": "NonExistentFilter",
            "column": "non_existent_column",
            "scope": ["temperature_chart"],
        }
    ]
    result = validate_filter_configuration(filters, dataset_columns, chart_ids)
    assert result["status"] == "Failed"
    assert any("does not exist" in err for err in result["errors"])


def test_invalid_chart_scope(
    dataset_columns: list[str], chart_ids: list[str]
) -> None:
    """Verifies filters referencing unknown charts are flagged with errors."""
    filters = [
        {
            "name": "Container Filter",
            "column": "container_id",
            "scope": ["unknown_chart_xyz"],
        }
    ]
    result = validate_filter_configuration(filters, dataset_columns, chart_ids)
    assert result["status"] == "Failed"
    assert any("Unknown chart reference" in err for err in result["errors"])


def test_empty_filter_scope(
    dataset_columns: list[str], chart_ids: list[str]
) -> None:
    """Verifies filters with empty scope generate warnings but can pass."""
    filters = [
        {
            "name": "Container Filter",
            "column": "container_id",
            "scope": [],
        }
    ]
    result = validate_filter_configuration(filters, dataset_columns, chart_ids)
    assert result["status"] == "Passed"
    assert len(result["warnings"]) == 1


def test_duplicate_filter_names(
    dataset_columns: list[str], chart_ids: list[str]
) -> None:
    """Verifies duplicate filter names are rejected."""
    filters = [
        {
            "name": "Container Filter",
            "column": "container_id",
            "scope": ["temperature_chart"],
        },
        {
            "name": "Container Filter",
            "column": "commodity",
            "scope": ["humidity_chart"],
        },
    ]
    result = validate_filter_configuration(filters, dataset_columns, chart_ids)
    assert result["status"] == "Failed"
    assert any("Duplicate filter name" in err for err in result["errors"])


def test_datasets_and_charts_yaml_filters_validation() -> None:
    """Validates the actual project datasets_and_charts.yaml filter definitions."""
    yaml_path = (
        Path(__file__).resolve().parent.parent
        / "dashboard"
        / "dashboards"
        / "datasets_and_charts.yaml"
    )
    assert yaml_path.exists()

    with open(yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    raw_filters = config.get("filters", [])
    assert len(raw_filters) == 4

    # Extract all declared column names across all datasets in the YAML
    valid_cols: set[str] = set()
    for ds in config.get("datasets", []):
        for col in ds.get("columns", []):
            if isinstance(col, dict) and "name" in col:
                valid_cols.add(col["name"])
        # Standard columns present across marts
        valid_cols.update(["container_id", "commodity", "recorded_at", "quality_status"])

    # Extract all valid table names as targets
    valid_targets = [ds["table_name"] for ds in config.get("datasets", [])]

    result = validate_filter_configuration(
        filters=raw_filters,
        dataset_columns=list(valid_cols),
        chart_ids=valid_targets,
    )

    assert result["status"] == "Passed"
    assert result["total_filters"] == 4
    assert result["errors"] == []
