import unittest

from filter_validator import validate_filter_configuration


class TestFilterConfiguration(unittest.TestCase):

    def setUp(self):
        self.dataset_columns = [
            "container_id",
            "timestamp",
            "temperature",
            "humidity",
            "vibration"
        ]

        self.chart_ids = [
            "temperature_chart",
            "humidity_chart",
            "vibration_chart"
        ]

    def test_valid_filter_configuration(self):
        filters = [
            {
                "name": "Container ID",
                "column": "container_id",
                "scope": [
                    "temperature_chart",
                    "humidity_chart"
                ]
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids
        )

        self.assertEqual(result["status"], "Passed")
        self.assertEqual(result["total_filters"], 1)
        self.assertEqual(result["errors"], [])

    def test_invalid_dataset_column(self):
        filters = [
            {
                "name": "Date Filter",
                "column": "reading_date",
                "scope": ["temperature_chart"]
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_invalid_chart_scope(self):
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": ["unknown_chart"]
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])

    def test_empty_filter_scope(self):
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": []
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids
        )

        self.assertEqual(result["status"], "Passed")
        self.assertTrue(result["warnings"])

    def test_duplicate_filter_names(self):
        filters = [
            {
                "name": "Container Filter",
                "column": "container_id",
                "scope": ["temperature_chart"]
            },
            {
                "name": "Container Filter",
                "column": "humidity",
                "scope": ["humidity_chart"]
            }
        ]

        result = validate_filter_configuration(
            filters,
            self.dataset_columns,
            self.chart_ids
        )

        self.assertEqual(result["status"], "Failed")
        self.assertTrue(result["errors"])


if __name__ == "__main__":
    unittest.main(verbosity=2)