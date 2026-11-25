
import unittest
import yaml
import subprocess
import sys

from pathlib import Path

try:
    from .dashboard_config_validator import validate_dashboard_config
except ImportError:
    from dashboard_config_validator import validate_dashboard_config


class TestDashboardConfigValidator(unittest.TestCase):

    def setUp(self):
        self.valid_config = {
            "datasets": [
                {
                    "table_name": "SENSOR_DATA",
                    "metrics": [
                        {
                            "metric_name": "avg_temperature",
                            "expression": "AVG(temperature)"
                        }
                    ]
                }
            ],
            "charts": [
                {
                    "slice_name": "Temperature Chart",
                    "dataset": "SENSOR_DATA",
                    "metrics": ["avg_temperature"]
                }
            ],
            "filters": [
                {
                    "filter_name": "Container Filter",
                    "column": "container_id",
                    "targets": ["SENSOR_DATA"]
                }
            ]
        }

    def test_valid_configuration(self):
        result = validate_dashboard_config(self.valid_config)
        self.assertEqual(result["status"], "Passed")
        self.assertEqual(result["errors"], [])

    def test_empty_configuration(self):
        config = {}

        result = validate_dashboard_config(config)

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_missing_dashboard_sections(self):
        config = {
            "datasets": self.valid_config["datasets"]
        }

        result = validate_dashboard_config(config)

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_invalid_charts_section_type(self):
        config = self.valid_config.copy()
        config["charts"] = "invalid"

        result = validate_dashboard_config(config)

        self.assertEqual(result["status"], "Failed")
        self.assertIn(
            "Charts must be provided as a list.",
            result["errors"],
        )

    def test_duplicate_dataset(self):
        config = self.valid_config.copy()
        config["datasets"] = config["datasets"] * 2

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_unknown_chart_dataset(self):
        config = self.valid_config.copy()
        config["charts"][0]["dataset"] = "UNKNOWN"

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_unknown_chart_metric(self):
        config = self.valid_config.copy()
        config["charts"][0]["metrics"] = ["unknown_metric"]

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_duplicate_metric(self):
        config = self.valid_config.copy()
        config["datasets"][0]["metrics"].append(
            config["datasets"][0]["metrics"][0].copy()
        )

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_unknown_filter_target(self):
        config = self.valid_config.copy()
        config["filters"][0]["targets"] = ["UNKNOWN"]

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_duplicate_chart(self):
        config = self.valid_config.copy()
        config["charts"].append(config["charts"][0].copy())

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_missing_metric_expression(self):
        config = self.valid_config.copy()
        config["datasets"][0]["metrics"][0]["expression"] = ""

        result = validate_dashboard_config(config)
        self.assertEqual(result["status"], "Failed")

    def test_actual_project_yaml(self):
        yaml_path = (
            Path(__file__).resolve().parent.parent
            / "dashboards"
            / "datasets_and_charts.yaml"
        )

        with open(yaml_path, "r", encoding="utf-8") as file:
            config = yaml.safe_load(file)

        result = validate_dashboard_config(config)

        self.assertEqual(result["status"], "Passed")
        self.assertEqual(result["summary"]["validated_datasets"], 4)
        self.assertEqual(result["summary"]["validated_metrics"], 20)
        self.assertEqual(result["summary"]["validated_charts"], 12)
        self.assertEqual(result["summary"]["validated_filters"], 4)

    def test_cli_missing_config_file(self):
        validator_path = (
            Path(__file__).resolve().parent
            / "dashboard_config_validator.py"
        )

        result = subprocess.run(
            [
                sys.executable,
                str(validator_path),
                "--config",
                "missing_config.yaml",
            ],
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 2)
        self.assertIn(
            "Configuration file not found",
            result.stdout,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)