import copy
import json
import re
import unittest
from datetime import datetime

from atmosync_sensor_simulator import (
    CONTAINERS,
    active_scenarios,
    generate_event,
    state,
)
from kafka_producer import build_sample_event

REQUIRED_FIELDS = {
    "event_id",
    "timestamp_utc",
    "container_id",
    "commodity",
    "origin",
    "destination",
    "quantity_kg",
    "temperature_c",
    "humidity_pct",
    "vibration_g",
    "sensor_status",
    "anomaly_type",
}

ALLOWED_STATUSES = {"NORMAL", "WARNING", "ALERT", "FAULT"}
ALLOWED_ANOMALIES = {
    "NONE",
    "COOLING_FAILURE",
    "DOOR_OPEN",
    "ROUGH_TRANSIT",
    "SENSOR_DROPOUT",
}
EVENT_ID_PATTERN = re.compile(r"^EV\d{6}$")


class TestEventContract(unittest.TestCase):
    def setUp(self):
        self._orig_state = copy.deepcopy(state)
        self._orig_scenarios = copy.deepcopy(active_scenarios)

    def tearDown(self):
        state.clear()
        state.update(self._orig_state)
        active_scenarios.clear()
        active_scenarios.update(self._orig_scenarios)

    def _assert_valid_contract(self, event):
        self.assertEqual(set(event.keys()), REQUIRED_FIELDS)

        # String & identifier fields
        self.assertIsInstance(event["event_id"], str)
        self.assertRegex(event["event_id"], EVENT_ID_PATTERN)

        self.assertIsInstance(event["timestamp_utc"], str)
        parsed_ts = datetime.fromisoformat(event["timestamp_utc"])
        self.assertIsNotNone(parsed_ts.tzinfo)

        self.assertIn(event["container_id"], {"C001", "C002", "C003"})
        for str_field in ("commodity", "origin", "destination"):
            self.assertIsInstance(event[str_field], str)
            self.assertTrue(event[str_field].strip())

        # Numeric fields (exclude bool, which is a subclass of int in Python)
        self.assertIsInstance(event["quantity_kg"], int)
        self.assertNotIsInstance(event["quantity_kg"], bool)
        self.assertGreater(event["quantity_kg"], 0)

        for float_field in ("temperature_c", "humidity_pct", "vibration_g"):
            self.assertIsInstance(event[float_field], (int, float))
            self.assertNotIsInstance(event[float_field], bool)

        # Enum fields
        self.assertIn(event["sensor_status"], ALLOWED_STATUSES)
        self.assertIn(event["anomaly_type"], ALLOWED_ANOMALIES)

        # Must serialize cleanly to UTF-8 JSON
        encoded = json.dumps(event).encode("utf-8")
        decoded = json.loads(encoded.decode("utf-8"))
        self.assertEqual(decoded["event_id"], event["event_id"])

    def test_producer_sample_event_matches_contract(self):
        event = build_sample_event()
        self._assert_valid_contract(event)
        self.assertEqual(event["sensor_status"], "NORMAL")
        self.assertEqual(event["anomaly_type"], "NONE")

    def test_simulator_normal_events_all_containers_and_boundaries(self):
        for idx, container in enumerate(CONTAINERS, start=1):
            with self.subTest(container_id=container["container_id"]):
                event = generate_event(
                    container,
                    event_id_num=idx,
                    forced_scenario="NONE",
                    anomaly_rate=0.0,
                )
                self._assert_valid_contract(event)
                self.assertEqual(event["anomaly_type"], "NONE")
                self.assertGreaterEqual(event["temperature_c"], -5.0)
                self.assertLessEqual(event["temperature_c"], 35.0)
                self.assertGreaterEqual(event["humidity_pct"], 40.0)
                self.assertLessEqual(event["humidity_pct"], 100.0)
                self.assertGreaterEqual(event["vibration_g"], 0.01)
                self.assertLessEqual(event["vibration_g"], 2.0)

    def test_cooling_failure_breaches_max_temp_and_triggers_alert(self):
        container = CONTAINERS[0]  # C001 Avocados, temp_max=7.5
        events = [
            generate_event(
                container,
                event_id_num=100 + i,
                forced_scenario="COOLING_FAILURE",
                target_container="C001",
            )
            for i in range(6)
        ]
        for ev in events:
            self._assert_valid_contract(ev)
            self.assertEqual(ev["anomaly_type"], "COOLING_FAILURE")

        self.assertGreater(events[-1]["temperature_c"], container["temp_max"])
        self.assertEqual(events[-1]["sensor_status"], "ALERT")

    def test_rough_transit_breaches_vibration_max_and_triggers_alert(self):
        container = CONTAINERS[1]  # C002 Mangoes, vibration_max=0.35
        event = generate_event(
            container,
            event_id_num=200,
            forced_scenario="ROUGH_TRANSIT",
            target_container="C002",
        )
        self._assert_valid_contract(event)
        self.assertEqual(event["anomaly_type"], "ROUGH_TRANSIT")
        self.assertGreater(event["vibration_g"], container["vibration_max"])
        self.assertEqual(event["sensor_status"], "ALERT")

    def test_sensor_dropout_emits_fault_and_sentinel_values(self):
        container = CONTAINERS[2]  # C003 Bananas
        event = generate_event(
            container,
            event_id_num=300,
            forced_scenario="SENSOR_DROPOUT",
            target_container="C003",
        )
        self._assert_valid_contract(event)
        self.assertEqual(event["sensor_status"], "FAULT")
        self.assertEqual(event["anomaly_type"], "SENSOR_DROPOUT")
        self.assertEqual(event["temperature_c"], -99.9)
        self.assertEqual(event["humidity_pct"], -1.0)
        self.assertEqual(event["vibration_g"], -1.0)


if __name__ == "__main__":
    unittest.main()