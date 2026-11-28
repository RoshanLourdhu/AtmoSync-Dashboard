import copy
import csv
import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from atmosync_sensor_simulator import (
    CSV_FIELDNAMES,
    active_scenarios,
    parse_args,
    run_simulator,
    state,
)


class TestCsvSchemaAndExport(unittest.TestCase):
    def setUp(self):
        self._orig_state = copy.deepcopy(state)
        self._orig_scenarios = copy.deepcopy(active_scenarios)

    def tearDown(self):
        state.clear()
        state.update(self._orig_state)
        active_scenarios.clear()
        active_scenarios.update(self._orig_scenarios)

    def test_committed_sensor_data_csv_matches_12_field_contract(self):
        with open("sensor_data.csv", newline="", encoding="utf-8") as handle:
            rows = list(csv.reader(handle))

        self.assertGreater(len(rows), 1)
        self.assertEqual(rows[0], CSV_FIELDNAMES)
        for row in rows[1:]:
            self.assertEqual(len(row), 12)
            self.assertIn(row[10], {"NORMAL", "ALERT", "FAULT"})
            self.assertIn(
                row[11],
                {"NONE", "COOLING_FAILURE", "DOOR_OPEN", "ROUGH_TRANSIT", "SENSOR_DROPOUT"},
            )

    def test_simulator_csv_out_writes_12_column_telemetry_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = os.path.join(tmpdir, "telemetry_out.csv")
            args = parse_args(
                [
                    "--dry-run",
                    "--max-events",
                    "3",
                    "--interval",
                    "0",
                    "--scenario",
                    "COOLING_FAILURE",
                    "--csv-out",
                    csv_path,
                ]
            )
            buf = io.StringIO()
            with redirect_stdout(buf):
                count = run_simulator(args)

            self.assertEqual(count, 3)
            with open(csv_path, newline="", encoding="utf-8") as handle:
                rows = list(csv.reader(handle))

            self.assertEqual(rows[0], CSV_FIELDNAMES)
            self.assertEqual(len(rows), 4)
            self.assertEqual(rows[1][11], "COOLING_FAILURE")


if __name__ == "__main__":
    unittest.main()