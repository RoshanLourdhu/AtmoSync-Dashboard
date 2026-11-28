import copy
import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from kafka.errors import KafkaError

from atmosync_sensor_simulator import (
    active_scenarios,
    main,
    parse_args,
    run_simulator,
    state,
)
from kafka_producer import ProducerConfigError, ProducerDeliveryError


class FakeFuture:
    def __init__(self, metadata=None, error=None):
        self.metadata = metadata
        self.error = error

    def add_callback(self, fn):
        if self.error is None:
            fn(self.metadata)
        return self

    def add_errback(self, fn):
        if self.error is not None:
            fn(self.error)
        return self

    def get(self, timeout=None):
        if self.error is not None:
            raise self.error
        return self.metadata


class TestSimulatorPublisherIntegration(unittest.TestCase):
    def setUp(self):
        self._orig_state = copy.deepcopy(state)
        self._orig_scenarios = copy.deepcopy(active_scenarios)

    def tearDown(self):
        state.clear()
        state.update(self._orig_state)
        active_scenarios.clear()
        active_scenarios.update(self._orig_scenarios)

    def test_dry_run_generates_exact_max_events_without_kafka(self):
        args = parse_args(
            ["--dry-run", "--max-events", "4", "--interval", "0", "--scenario", "NONE"]
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            count = run_simulator(args)

        self.assertEqual(count, 4)
        lines = [line for line in buf.getvalue().strip().splitlines() if "[DRY RUN]" in line]
        self.assertEqual(len(lines), 4)
        self.assertIn('"event_id": "EV000001"', lines[0])
        self.assertIn('"event_id": "EV000004"', lines[3])

    @patch("atmosync_sensor_simulator.create_producer")
    def test_simulator_publishes_via_producer_pipeline_and_closes(self, mock_create):
        fake_producer = MagicMock()
        offsets = iter([101, 102, 103])
        fake_producer.send.side_effect = lambda topic, key, value: FakeFuture(
            metadata=SimpleNamespace(
                topic=topic,
                partition=0,
                offset=next(offsets),
            )
        )
        mock_create.return_value = (
            fake_producer,
            {"bootstrap_servers": ["localhost:9092"], "topic": "atmosync_sensor_events"},
        )

        args = parse_args(
            ["--max-events", "3", "--interval", "0", "--scenario", "NONE"]
        )
        buf = io.StringIO()
        with redirect_stdout(buf):
            count = run_simulator(args)

        self.assertEqual(count, 3)
        self.assertEqual(fake_producer.send.call_count, 3)

        sent_keys = [call.kwargs["key"] for call in fake_producer.send.call_args_list]
        self.assertEqual(sent_keys, ["C001", "C002", "C003"])

        output = buf.getvalue()
        self.assertIn("[DELIVERY ACK] event_id=EV000001", output)
        self.assertIn("[DELIVERY ACK] event_id=EV000003", output)

        fake_producer.flush.assert_called_once_with(timeout=10)
        fake_producer.close.assert_called_once_with(timeout=10)

    @patch("atmosync_sensor_simulator.create_producer")
    def test_simulator_closes_producer_when_delivery_fails(self, mock_create):
        fake_producer = MagicMock()
        fake_producer.send.return_value = FakeFuture(error=KafkaError("Broker failure"))
        mock_create.return_value = (
            fake_producer,
            {"bootstrap_servers": ["localhost:9092"], "topic": "atmosync_sensor_events"},
        )

        args = parse_args(["--max-events", "2", "--interval", "0"])
        with self.assertRaises(ProducerDeliveryError):
            run_simulator(args)

        fake_producer.flush.assert_called_once_with(timeout=10)
        fake_producer.close.assert_called_once_with(timeout=10)

    def test_main_exits_cleanly_on_invalid_bootstrap_config(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(SystemExit) as ctx:
                main(["--bootstrap-servers", "invalid-host", "--max-events", "1"])

        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("[SIMULATOR CONFIG ERROR]", buf.getvalue())


if __name__ == "__main__":
    unittest.main()