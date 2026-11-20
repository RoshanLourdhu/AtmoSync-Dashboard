"""
Automated Unit Tests for AtmoSync Dashboard Filter Configuration Validator.

Verifies:
1. Valid filter configurations against target datasets and chart IDs.
2. Rejection of non-dictionary filter items and missing keys.
3. Column schema validation against dataset columns.
4. Unknown chart ID detection in filter scopes.
5. Warning generation for filters with empty scopes.
6. Rejection of duplicate filter names.
7. Validation against the project's declarative datasets_and_charts.yaml filters.
"""
from __future__ import annotations

import unittest
from pathlib import Path
from typing import Any

try:
    from .filter_validator import validate_filter_configuration
except ImportError:
    from filter_validator import validate_filter_configuration


class TestFilterConfiguration(unittest.TestCase):
    """Test suite for dashboard filter configuration validation."""

    def setUp(self) -> None:
        self.dataset_columns = [
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

        self.chart_ids = [
            "temperature_chart",
            "humidity_chart",
            "vibration_chart",
            "spoilage_gauge",
            "STG_SENSOR_READINGS",
            "FCT_SPOILAGE_RISK",
            "FCT_ARBITRAGE_OPPORTUNITIES",
            "FCT_SENSOR_DATA_QUALITY",
        ]

    def test_valid_filter_configuration(self) -> None:
        """Verifies valid filter configuration passes with 0 errors."""
        filters = [
            {
                "name": "Container ID",
                "column": "container_id",
                "scope": ["temperature_chart", "humidity_chart"],
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids,
        )

        self.assertEqual(result["status"], "Passed")
        self.assertEqual(result["total_filters"], 1)
        self.assertEqual(result["errors"], [])
        self.assertIn("Container ID", result["validated_filters"])

    def test_invalid_filter_structure(self) -> None:
        """Verifies non-dictionary filter items and missing fields are rejected."""
        filters: list[Any] = [
            "not-a-dict",
            {"column": "container_id"},  # missing name
        ]

        result = validate_filter_configuration(filters)
        self.assertEqual(result["status"], "Failed")
        self.assertEqual(len(result["errors"]), 2)

    def test_invalid_dataset_column(self) -> None:
        """Verifies filter referencing unknown column is flagged."""
        filters = [
            {
                "name": "Date Filter",
                "column": "non_existent_reading_date",
                "scope": ["temperature_chart"],
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids,
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_invalid_chart_scope(self) -> None:
        """Verifies unknown chart ID reference in scope is flagged."""
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": ["unknown_chart_xyz"],
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids,
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_empty_filter_scope(self) -> None:
        """Verifies filter with empty scope generates a warning."""
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": [],
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids,
        )

        self.assertEqual(result["status"], "Passed")
        self.assertTrue(result["warnings"])

    def test_duplicate_filter_names(self) -> None:
        """Verifies duplicate filter names are detected and rejected."""
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": ["temperature_chart"],
            },
            {
                "name": "Container Filter",
                "column": "humidity",
                "scope": ["humidity_chart"],
            },
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids,
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_declared_yaml_filters_validation(self) -> None:
        """Validates the actual project datasets_and_charts.yaml filter specifications."""
        yaml_path = (
            Path(__file__).resolve().parent.parent
            / "dashboards"
            / "datasets_and_charts.yaml"
        )
        self.assertTrue(yaml_path.exists())

        try:
            import yaml
        except ImportError:
            self.skipTest("PyYAML not installed in test environment")

        with open(yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        raw_filters = config.get("filters", [])
        self.assertEqual(len(raw_filters), 4)

        result = validate_filter_configuration(
            filters=raw_filters,
            dataset_columns=self.dataset_columns,
            chart_ids=self.chart_ids,
        )

        self.assertEqual(result["status"], "Passed")
        self.assertEqual(result["total_filters"], 4)
        self.assertEqual(result["errors"], [])


if __name__ == "__main__":
    unittest.main()
