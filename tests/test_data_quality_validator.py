"""
Unit tests for AtmoSync Tabular Data Quality Validator and Report Generator.

Verifies:
1. Required column validation for the official 7-field schema.
2. Missing values, duplicates, and invalid timestamp detection.
3. Numeric type conversion and out-of-bound humidity detection (<0 or >100).
4. IQR-based outlier identification across temperature, humidity, and vibration.
5. Overall quality score computation and JSON report export.
6. Backward compatibility with 5-field column subsets.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from dashboard.data_quality.report_generator import export_report, generate_quality_report
from dashboard.data_quality.validator import (
    REQUIRED_COLUMNS,
    validate_sensor_data,
)


@pytest.fixture
def valid_sensor_records() -> list[dict[str, Any]]:
    """Fixture providing clean sensor records meeting the official 7-field contract."""
    return [
        {
            "container_id": f"CONT-00{i}",
            "shipment_id": f"SHIP-10{i}",
            "commodity": "Bananas",
            "timestamp": f"2026-11-19T10:0{i}:00Z",
            "temperature": 13.5 + (i * 0.1),
            "humidity": 85.0 + (i * 0.5),
            "vibration": 0.12 + (i * 0.01),
        }
        for i in range(10)
    ]


def test_validator_perfect_data_passes(valid_sensor_records: list[dict[str, Any]]) -> None:
    """Verifies clean conforming sensor dataset achieves Completed status with 0 issues."""
    report = validate_sensor_data(valid_sensor_records)
    assert report["status"] == "Completed"
    assert report["total_records"] == 10
    assert report["missing_columns"] == []
    assert report["duplicate_records"] == 0
    assert report["invalid_timestamps"] == 0
    assert report["invalid_humidity_records"] == 0
    assert sum(report["missing_values"].values()) == 0


def test_validator_missing_required_column_fails() -> None:
    """Verifies omitting required column returns Failed status."""
    records = [{"container_id": "CONT-001", "temperature": 14.0}]
    report = validate_sensor_data(records)
    assert report["status"] == "Failed"
    assert "shipment_id" in report["missing_columns"]
    assert "commodity" in report["missing_columns"]
    assert "timestamp" in report["missing_columns"]


def test_validator_missing_values_and_duplicates() -> None:
    """Verifies null values and duplicated (container_id, timestamp) pairs are identified."""
    records = [
        {
            "container_id": "CONT-001",
            "shipment_id": "SHIP-001",
            "commodity": "Vaccines",
            "timestamp": "2026-11-19T12:00:00Z",
            "temperature": 4.0,
            "humidity": 50.0,
            "vibration": 0.05,
        },
        {
            "container_id": "CONT-001",
            "shipment_id": "SHIP-001",
            "commodity": None,  # null commodity
            "timestamp": "2026-11-19T12:00:00Z",  # duplicate key
            "temperature": None,  # null temperature
            "humidity": 50.0,
            "vibration": 0.05,
        },
    ]
    report = validate_sensor_data(records)
    assert report["status"] == "Completed"
    assert report["duplicate_records"] == 1
    assert report["missing_values"]["commodity"] == 1
    assert report["missing_values"]["temperature"] == 1


def test_validator_invalid_timestamps_and_numerics() -> None:
    """Verifies unparseable timestamps and string numerics are identified."""
    records = [
        {
            "container_id": "CONT-001",
            "shipment_id": "SHIP-001",
            "commodity": "Fresh Fish",
            "timestamp": "not-a-timestamp",
            "temperature": "unparseable_temp",
            "humidity": 125.0,  # out of bound > 100
            "vibration": "zero",
        }
    ]
    report = validate_sensor_data(records)
    assert report["status"] == "Completed"
    assert report["invalid_timestamps"] == 1
    assert report["invalid_numeric_values"]["temperature"] == 1
    assert report["invalid_numeric_values"]["vibration"] == 1
    assert report["invalid_humidity_records"] == 1


def test_validator_outliers_iqr() -> None:
    """Verifies statistical outliers beyond 1.5 * IQR are counted."""
    temps = [4.0, 4.1, 4.0, 4.2, 3.9, 4.0, 4.1, 4.0, 50.0]  # 50.0 is outlier
    records = [
        {
            "container_id": f"CONT-00{i}",
            "shipment_id": "SHIP-001",
            "commodity": "Vaccines",
            "timestamp": f"2026-11-19T12:0{i}:00Z",
            "temperature": t,
            "humidity": 50.0,
            "vibration": 0.05,
        }
        for i, t in enumerate(temps)
    ]
    report = validate_sensor_data(records)
    assert report["outliers"]["temperature"] >= 1


def test_generate_quality_report_score_calculation(
    valid_sensor_records: list[dict[str, Any]],
) -> None:
    """Verifies quality score computes 100% for clean data and scales down for issues."""
    clean_summary = generate_quality_report(valid_sensor_records)
    assert clean_summary["validation_status"] == "Completed"
    assert clean_summary["quality_score"] == 100.0

    # Dataset with 1 duplicate and 1 invalid timestamp out of 4 records
    imperfect_records = [
        {
            "container_id": "CONT-001",
            "shipment_id": "SHIP-001",
            "commodity": "Apples",
            "timestamp": "2026-11-19T10:00:00Z",
            "temperature": 2.0,
            "humidity": 90.0,
            "vibration": 0.05,
        },
        {
            "container_id": "CONT-001",
            "shipment_id": "SHIP-001",
            "commodity": "Apples",
            "timestamp": "2026-11-19T10:00:00Z",  # duplicate
            "temperature": 2.0,
            "humidity": 90.0,
            "vibration": 0.05,
        },
        {
            "container_id": "CONT-002",
            "shipment_id": "SHIP-002",
            "commodity": "Apples",
            "timestamp": "corrupt-date",  # invalid timestamp
            "temperature": 2.0,
            "humidity": 90.0,
            "vibration": 0.05,
        },
        {
            "container_id": "CONT-003",
            "shipment_id": "SHIP-003",
            "commodity": "Apples",
            "timestamp": "2026-11-19T10:05:00Z",
            "temperature": 2.0,
            "humidity": 90.0,
            "vibration": 0.05,
        },
    ]
    imperfect_summary = generate_quality_report(imperfect_records)
    assert imperfect_summary["quality_score"] < 100.0
    assert imperfect_summary["duplicate_records"] == 1
    assert imperfect_summary["invalid_timestamps"] == 1


def test_export_report_and_file_serialization(tmp_path: Path) -> None:
    """Verifies JSON export produces a valid, readable report file."""
    sample_report = {
        "total_records": 10,
        "quality_score": 95.0,
        "missing_values": 0,
        "duplicate_records": 0,
    }
    out_file = tmp_path / "exported_report.json"
    exported_path = export_report(sample_report, out_file)
    assert Path(exported_path).exists()

    with open(exported_path, "r", encoding="utf-8") as f:
        loaded = json.load(f)
    assert loaded["quality_score"] == 95.0


def test_validator_navya_5_field_backward_compatibility() -> None:
    """Verifies validator supports Navya's 5-field baseline via required_columns parameter."""
    navya_5_fields = ["container_id", "timestamp", "temperature", "humidity", "vibration"]
    records = [
        {
            "container_id": "C101",
            "timestamp": "2026-11-15 10:00:00",
            "temperature": 25.5,
            "humidity": 65.0,
            "vibration": 2.1,
        }
    ]
    report = validate_sensor_data(records, required_columns=navya_5_fields)
    assert report["status"] == "Completed"
    assert report["total_records"] == 1
    assert report["missing_columns"] == []
