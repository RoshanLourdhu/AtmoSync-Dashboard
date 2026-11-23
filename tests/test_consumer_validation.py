import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock
from kafka.errors import NoBrokersAvailable

from atmosync_sensor_simulator import CONTAINERS, active_scenarios, generate_event, state
from kafka_consumer import (
    ConsumerConfigError,
    TelemetryValidationError,
    consume_events,
    create_consumer,
    main,
    parse_args,
    run_consumer,
    validate_telemetry_event,
)


class TestConsumerValidation(unittest.TestCase):
    def setUp(self):
        self._orig_state = copy.deepcopy(state)
        self._orig_scenarios = copy.deepcopy(active_scenarios)

    def tearDown(self):
        state.clear()
        state.update(self._orig_state)
        active_scenarios.clear()
        active_scenarios.update(self._orig_scenarios)

    def test_validate_telemetry_event_accepts_valid_12_field_payload(self):
        event = generate_event(CONTAINERS[0], 1, forced_scenario="NONE")
        validated = validate_telemetry_event(event)
        self.assertEqual(validated["event_id"], "EV000001")
        self.assertEqual(validated["anomaly_type"], "NONE")

    def test_validate_telemetry_event_rejects_missing_fields_and_bad_status(self):
        event = generate_event(CONTAINERS[0], 1, forced_scenario="NONE")
        incomplete = dict(event)
        del incomplete["anomaly_type"]
        with self.assertRaises(TelemetryValidationError):
            validate_telemetry_event(incomplete)

        bad_status = dict(event)
        bad_status["sensor_status"] = "UNKNOWN"
        with self.assertRaises(TelemetryValidationError):
            validate_telemetry_event(bad_status)

    def test_create_consumer_validates_config_and_handles_no_brokers(self):
        mock_cls = MagicMock()
        consumer, cfg = create_consumer(
            {
                "bootstrap_servers": "127.0.0.1:9092",
                "topic": "atmosync_sensor_events",
                "group_id": "test-group",
            },
            consumer_cls=mock_cls,
        )
        self.assertIsNotNone(consumer)
        self.assertEqual(cfg["bootstrap_servers"], ["127.0.0.1:9092"])
        self.assertEqual(cfg["group_id"], "test-group")

        with self.assertRaises(ConsumerConfigError):
            create_consumer({"bootstrap_servers": "   "}, consumer_cls=mock_cls)

        failing_cls = MagicMock(side_effect=NoBrokersAvailable())
        with self.assertRaises(RuntimeError) as ctx:
            create_consumer({"bootstrap_servers": "localhost:9092"}, consumer_cls=failing_cls)
        self.assertIn("No Kafka brokers reachable", str(ctx.exception))

    def test_consume_events_tracks_valid_alerts_and_malformed_records(self):
        normal_event = generate_event(CONTAINERS[0], 1, forced_scenario="NONE")
        alert_event = generate_event(
            CONTAINERS[1], 2, forced_scenario="COOLING_FAILURE", target_container="C002"
        )
        records = [
            SimpleNamespace(value=json.dumps(normal_event).encode("utf-8")),
            SimpleNamespace(value=json.dumps(alert_event).encode("utf-8")),
            SimpleNamespace(value=b"{not-valid-json"),
        ]

        seen_alerts = []
        buf = io.StringIO()
        with redirect_stdout(buf):
            summary = consume_events(records, max_messages=3, on_alert=seen_alerts.append)

        self.assertEqual(summary, {"consumed": 2, "alerts": 1, "invalid": 1})
        self.assertEqual(len(seen_alerts), 1)
        self.assertEqual(seen_alerts[0]["anomaly_type"], "COOLING_FAILURE")

    def test_run_consumer_dry_run_and_main_summary(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            main(["--dry-run", "--max-messages", "4"])

        output = buf.getvalue()
        self.assertIn("[CONSUMER SUMMARY] consumed=4", output)


if __name__ == "__main__":
    unittest.main()