"""
Automated Unit Tests for AtmoSync Sensor Data Quality Validator & Report Generator.

Verifies:
1. 7-field official schema validation.
2. Missing columns, missing/null values, and duplicate key detection.
3. Invalid timestamps and non-numeric value parsing.
4. Physical boundary violations: humidity out of [0, 100], negative vibration, temperature limits.
5. IQR-based statistical outlier detection.
6. Quality score calculation and JSON report export.
7. Backward compatibility for legacy 5-field datasets.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

try:
    from .report_generator import export_report, generate_quality_report
    from .validator import (
        LEGACY_5_FIELD_COLUMNS,
        REQUIRED_COLUMNS,
        validate_sensor_data,
    )
except ImportError:
    from report_generator import export_report, generate_quality_report
    from validator import (
        LEGACY_5_FIELD_COLUMNS,
        REQUIRED_COLUMNS,
        validate_sensor_data,
    )


class TestSensorDataQualityValidator(unittest.TestCase):
    """Test suite for tabular sensor data quality validation."""

    def setUp(self) -> None:
        self.valid_records = [
            {
                "container_id": f"C00{i}",
                "shipment_id": f"SHIP-10{i}",
                "commodity": "Bananas",
                "timestamp": f"2026-11-20T10:0{i}:00Z",
                "temperature": 13.5 + (i * 0.1),
                "humidity": 85.0 + (i * 0.5),
                "vibration": 0.12 + (i * 0.01),
            }
            for i in range(10)
        ]

    def test_valid_7_field_data_passes(self) -> None:
        """Verifies clean conforming records pass with Completed status and 0 issues."""
        report = validate_sensor_data(self.valid_records)
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["total_records"], 10)
        self.assertEqual(report["missing_columns"], [])
        self.assertEqual(report["duplicate_records"], 0)
        self.assertEqual(report["invalid_timestamps"], 0)
        self.assertEqual(report["invalid_humidity_records"], 0)
        self.assertEqual(report["invalid_vibration_records"], 0)
        self.assertEqual(report["invalid_temperature_records"], 0)
        self.assertEqual(sum(report["missing_values"].values()), 0)

    def test_missing_required_column_fails(self) -> None:
        """Verifies omitting required column returns Failed status."""
        records = [{"container_id": "C001", "temperature": 14.0}]
        report = validate_sensor_data(records)
        self.assertEqual(report["status"], "Failed")
        self.assertIn("shipment_id", report["missing_columns"])
        self.assertIn("commodity", report["missing_columns"])
        self.assertIn("timestamp", report["missing_columns"])

    def test_missing_values_and_duplicates(self) -> None:
        """Verifies null values and duplicated (container_id, timestamp) keys are detected."""
        records = [
            {
                "container_id": "C001",
                "shipment_id": "SHIP-001",
                "commodity": "Vaccines",
                "timestamp": "2026-11-20T12:00:00Z",
                "temperature": 4.0,
                "humidity": 50.0,
                "vibration": 0.05,
            },
            {
                "container_id": "C001",
                "shipment_id": "SHIP-001",
                "commodity": None,  # Missing commodity
                "timestamp": "2026-11-20T12:00:00Z",  # Duplicate key
                "temperature": None,  # Missing temperature
                "humidity": 50.0,
                "vibration": 0.05,
            },
        ]
        report = validate_sensor_data(records)
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["duplicate_records"], 1)
        self.assertEqual(report["missing_values"]["commodity"], 1)
        self.assertEqual(report["missing_values"]["temperature"], 1)

    def test_invalid_timestamps_and_numerics(self) -> None:
        """Verifies non-parseable timestamps and string numerics are detected."""
        records = [
            {
                "container_id": "C001",
                "shipment_id": "SHIP-001",
                "commodity": "Fresh Fish",
                "timestamp": "invalid-date",
                "temperature": "corrupt_temp",
                "humidity": 80.0,
                "vibration": "not_numeric",
            }
        ]
        report = validate_sensor_data(records)
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["invalid_timestamps"], 1)
        self.assertEqual(report["invalid_numeric_values"]["temperature"], 1)
        self.assertEqual(report["invalid_numeric_values"]["vibration"], 1)

    def test_physical_boundaries_humidity_vibration_temperature(self) -> None:
        """Verifies physical limits: humidity [0, 100], vibration >= 0, temperature [-50, 60]."""
        records = [
            {
                "container_id": "C001",
                "shipment_id": "SHIP-001",
                "commodity": "Bananas",
                "timestamp": "2026-11-20T10:00:00Z",
                "temperature": 150.0,  # Extreme temperature > 60
                "humidity": 120.0,  # Humidity > 100
                "vibration": -0.5,  # Negative vibration < 0
            }
        ]
        report = validate_sensor_data(records)
        self.assertEqual(report["invalid_humidity_records"], 1)
        self.assertEqual(report["invalid_vibration_records"], 1)
        self.assertEqual(report["invalid_temperature_records"], 1)

    def test_iqr_outlier_detection(self) -> None:
        """Verifies statistical outliers beyond 1.5 * IQR are detected."""
        temps = [4.0, 4.1, 4.0, 4.2, 3.9, 4.0, 4.1, 4.0, 50.0]  # 50.0 is an outlier
        records = [
            {
                "container_id": f"C00{i}",
                "shipment_id": "SHIP-001",
                "commodity": "Vaccines",
                "timestamp": f"2026-11-20T12:0{i}:00Z",
                "temperature": t,
                "humidity": 50.0,
                "vibration": 0.05,
            }
            for i, t in enumerate(temps)
        ]
        report = validate_sensor_data(records)
        self.assertGreaterEqual(report["outliers"]["temperature"], 1)

    def test_legacy_5_field_backward_compatibility(self) -> None:
        """Verifies backward compatibility with Navya's legacy 5-field baseline."""
        legacy_records = [
            {
                "container_id": "C101",
                "timestamp": "2026-11-15 10:00:00",
                "temperature": 25.5,
                "humidity": 65.0,
                "vibration": 2.1,
            }
        ]
        report = validate_sensor_data(
            legacy_records,
            required_columns=LEGACY_5_FIELD_COLUMNS,
        )
        self.assertEqual(report["status"], "Completed")
        self.assertEqual(report["total_records"], 1)
        self.assertEqual(report["missing_columns"], [])

    def test_generate_quality_report_and_export(self) -> None:
        """Verifies score computation and JSON file export."""
        summary = generate_quality_report(self.valid_records)
        self.assertEqual(summary["validation_status"], "Completed")
        self.assertEqual(summary["quality_score"], 100.0)

        with tempfile.TemporaryDirectory() as tmpdir:
            out_path = Path(tmpdir) / "test_report.json"
            exported = export_report(summary, out_path)
            self.assertTrue(Path(exported).exists())

            with open(exported, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.assertEqual(data["quality_score"], 100.0)


if __name__ == "__main__":
    unittest.main()
