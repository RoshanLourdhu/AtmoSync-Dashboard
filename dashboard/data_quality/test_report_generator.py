import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from dashboard.data_quality.report_generator import export_report, generate_quality_report
from dashboard.data_quality.validator import validate_sensor_data


class TestReportGenerator(unittest.TestCase):
    def setUp(self):
        # Base valid data conforming to official 7-field schema contract
        self.base_data = {
            "container_id": ["C1", "C2", "C3"],
            "shipment_id": ["S1", "S2", "S3"],
            "commodity": ["Avocados", "Mangoes", "Bananas"],
            "timestamp": [
                "2026-01-01 10:00:00",
                "2026-01-01 10:05:00",
                "2026-01-01 10:10:00",
            ],
            "temperature": [5.0, 6.0, 7.0],
            "humidity": [30, 40, 50],
            "vibration": [0.5, 0.6, 0.7],
        }

    def test_generate_quality_report_valid_data(self):
        """All data valid – expect perfect score and zero issues."""
        df = pd.DataFrame(self.base_data)
        report = generate_quality_report(df)
        self.assertEqual(report["total_records"], 3)
        self.assertEqual(report["quality_score"], 100.0)
        self.assertEqual(report["missing_values"], 0)
        self.assertEqual(report["duplicate_records"], 0)
        self.assertEqual(report["invalid_timestamps"], 0)
        self.assertEqual(report["invalid_numeric_values"], 0)
        self.assertEqual(report["invalid_humidity_records"], 0)
        self.assertEqual(report["invalid_vibration_records"], 0)
        self.assertEqual(report["invalid_temperature_records"], 0)
        self.assertTrue(all(v == 0 for v in report["outliers"].values()))
        self.assertEqual(report["validation_status"], "Completed")

    def test_quality_score_calculation(self):
        """Introduce a known set of issues and verify the computed score."""
        data = self.base_data.copy()
        # Add a duplicate record (container_id + timestamp)
        data["container_id"].append("C1")
        data["shipment_id"].append("S1")
        data["commodity"].append("Avocados")
        data["timestamp"].append("2026-01-01 10:00:00")
        data["temperature"].append(99.0)
        data["humidity"].append(200)  # invalid humidity
        data["vibration"].append(5.0)
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        total = report["total_records"]
        # Calculate expected issues
        missing = report["missing_values"]
        dup = report["duplicate_records"]
        inv_ts = report["invalid_timestamps"]
        inv_hum = report["invalid_humidity_records"]
        inv_vib = report["invalid_vibration_records"]
        inv_temp = report["invalid_temperature_records"]
        inv_num = report["invalid_numeric_values"]
        issues = missing + dup + inv_ts + inv_hum + inv_vib + inv_temp + inv_num
        expected_score = max(0.0, round((1.0 - min(issues / (total * 5), 1.0)) * 100.0, 2))
        self.assertEqual(report["quality_score"], expected_score)

    def test_empty_dataframe(self):
        """An empty DataFrame should yield a score of 0 and no errors."""
        df = pd.DataFrame(columns=[
            "container_id", "shipment_id", "commodity", "timestamp", "temperature", "humidity", "vibration"
        ])
        report = generate_quality_report(df)
        self.assertEqual(report["total_records"], 0)
        self.assertEqual(report["quality_score"], 0.0)
        self.assertEqual(report["validation_status"], "Completed")

    def test_missing_required_columns(self):
        """When a required column is missing, the validator returns a failure dict."""
        data = {
            "container_id": ["C1"],
            "shipment_id": ["S1"],
            "commodity": ["Avocados"],
            "timestamp": ["2026-01-01 10:00:00"],
            "temperature": [5.0],
            # humidity column omitted intentionally
            "vibration": [0.5],
        }
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertIn("missing_columns", report)
        self.assertIn("humidity", report["missing_columns"])
        self.assertEqual(report["status"], "Failed")

    def test_duplicate_records(self):
        data = self.base_data.copy()
        data["container_id"].append("C1")
        data["shipment_id"].append("S1")
        data["commodity"].append("Avocados")
        data["timestamp"].append("2026-01-01 10:00:00")
        data["temperature"].append(5.0)
        data["humidity"].append(30)
        data["vibration"].append(0.5)
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertEqual(report["duplicate_records"], 1)
        self.assertTrue(0 < report["quality_score"] < 100)

    def test_invalid_timestamps(self):
        data = self.base_data.copy()
        data["timestamp"] = ["bad-date", "also-bad", "2026-01-01 10:10:00"]
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertEqual(report["invalid_timestamps"], 2)
        self.assertTrue(report["quality_score"] < 100)

    def test_invalid_humidity_values(self):
        data = self.base_data.copy()
        data["humidity"] = [-5, 150, 50]  # two out-of-range values
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertEqual(report["invalid_humidity_records"], 2)
        self.assertTrue(report["quality_score"] < 100)

    def test_non_numeric_values(self):
        data = self.base_data.copy()
        data["temperature"] = ["N/A", 6.0, 7.0]
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertGreater(report["invalid_numeric_values"], 0)
        self.assertTrue(report["quality_score"] < 100)

    def test_export_report_to_temp_file(self):
        df = pd.DataFrame(self.base_data)
        report = generate_quality_report(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "report.json")
            returned_path = export_report(report, output_path)
            self.assertTrue(os.path.isfile(returned_path))
            with open(returned_path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self.assertEqual(loaded, report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
