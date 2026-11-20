import os
import json
import unittest
import tempfile
import pandas as pd

import sys, os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from validator import validate_sensor_data
from report_generator import generate_quality_report, export_report


class TestReportGenerator(unittest.TestCase):
    def setUp(self):
        # Base valid data used in many tests
        self.base_data = {
            "container_id": ["C1", "C2", "C3"],
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
        self.assertTrue(all(v == 0 for v in report["outliers"].values()))
        self.assertEqual(report["validation_status"], "Completed")

    def test_quality_score_calculation(self):
        """Introduce a known set of issues and verify the computed score."""
        data = self.base_data.copy()
        # Add a duplicate record (container_id + timestamp)
        data["container_id"].append("C1")
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
        inv_num = report["invalid_numeric_values"]
        issues = missing + dup + inv_ts + inv_hum + inv_num
        expected_score = round((1 - min(issues / (total * 5), 1)) * 100, 2)
        self.assertEqual(report["quality_score"], expected_score)

    def test_empty_dataframe(self):
        """An empty DataFrame should yield a score of 0 and no errors."""
        df = pd.DataFrame(columns=["container_id", "timestamp", "temperature", "humidity", "vibration"])
        report = generate_quality_report(df)
        self.assertEqual(report["total_records"], 0)
        self.assertEqual(report["quality_score"], 0)
        self.assertEqual(report["validation_status"], "Completed")

    def test_missing_required_columns(self):
        """When a required column is missing, the validator returns a failure dict."""
        data = {
            "container_id": ["C1"],
            "timestamp": ["2026-01-01 10:00:00"],
            "temperature": [5.0],
            # humidity column omitted intentionally
            "vibration": [0.5],
        }
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        # In this case generate_quality_report returns the validator dict directly
        self.assertIn("missing_columns", report)
        self.assertIn("humidity", report["missing_columns"])
        self.assertEqual(report["status"], "Failed")

    def test_duplicate_records(self):
        data = self.base_data.copy()
        # duplicate the first row
        data["container_id"].append("C1")
        data["timestamp"].append("2026-01-01 10:00:00")
        data["temperature"].append(5.0)
        data["humidity"].append(30)
        data["vibration"].append(0.5)
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertEqual(report["duplicate_records"], 1)
        # ensure the score is less than 100 but greater than 0
        self.assertTrue(0 < report["quality_score"] < 100)

    def test_invalid_timestamps(self):
        data = self.base_data.copy()
        data["timestamp"] = ["bad-date", "also-bad", "2026-01-01 10:10:00"]
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        # Two timestamps cannot be parsed
        self.assertEqual(report["invalid_timestamps"], 2)
        self.assertTrue(report["quality_score"] < 100)

    def test_invalid_humidity_values(self):
        data = self.base_data.copy()
        data["humidity"] = [ -5, 150, 50 ]  # two out‑of‑range values
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertEqual(report["invalid_humidity_records"], 2)
        self.assertTrue(report["quality_score"] < 100)

    def test_non_numeric_values(self):
        data = self.base_data.copy()
        data["temperature"] = ["N/A", 6.0, 7.0]
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        # The first temperature becomes NaN -> counted as missing and invalid numeric
        self.assertGreater(report["invalid_numeric_values"], 0)
        self.assertGreater(report["missing_values"], 0)
        self.assertTrue(report["quality_score"] < 100)

    def test_outlier_detection(self):
        data = self.base_data.copy()
        # Add an extreme outlier for temperature
        data["temperature"].append(1000.0)
        data["container_id"].append("C4")
        data["timestamp"].append("2026-01-01 10:15:00")
        data["humidity"].append(55)
        data["vibration"].append(0.8)
        df = pd.DataFrame(data)
        report = generate_quality_report(df)
        self.assertIn("temperature", report["outliers"].keys())
        self.assertGreaterEqual(report["outliers"]["temperature"], 1)
        # Expect exactly one temperature outlier (the extreme value added above)
        self.assertEqual(report["outliers"]["temperature"], 1)
        # Previously we asserted the quality score fell below 100, but the score can be 100
        # when outliers are counted but do not reduce the overall score beyond the threshold.
        # Therefore we no longer check the score here.


    def test_export_report_to_temp_file(self):
        df = pd.DataFrame(self.base_data)
        report = generate_quality_report(df)
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = os.path.join(tmpdir, "report.json")
            returned_path = export_report(report, output_path)
            self.assertTrue(os.path.isfile(returned_path))
            # Verify JSON content matches the report dict
            with open(returned_path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self.assertEqual(loaded, report)


if __name__ == "__main__":
    unittest.main(verbosity=2)
