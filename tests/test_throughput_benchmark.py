import copy
import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from kafka.errors import KafkaError

from atmosync_sensor_simulator import active_scenarios, state
from kafka_throughput_test import main, run_throughput_test


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


class TestThroughputBenchmark(unittest.TestCase):
    def setUp(self):
        self._orig_state = copy.deepcopy(state)
        self._orig_scenarios = copy.deepcopy(active_scenarios)

    def tearDown(self):
        state.clear()
        state.update(self._orig_state)
        active_scenarios.clear()
        active_scenarios.update(self._orig_scenarios)

    def test_dry_run_throughput_benchmark_reports_metrics(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            summary = run_throughput_test(num_events=15, dry_run=True)

        self.assertEqual(summary["mode"], "DRY-RUN")
        self.assertEqual(summary["requested_events"], 15)
        self.assertEqual(summary["acknowledged_events"], 15)
        self.assertEqual(summary["failed_events"], 0)
        self.assertGreater(summary["events_per_sec"], 0)
        self.assertIn("[THROUGHPUT DRY-RUN] sent=15/15 failed=0", buf.getvalue())

    @patch("kafka_throughput_test.create_producer")
    def test_live_throughput_benchmark_tracks_acks_and_closes_producer(self, mock_create):
        fake_producer = MagicMock()
        fake_producer.send.side_effect = lambda topic, key, value: FakeFuture(
            metadata=SimpleNamespace(topic=topic, partition=0, offset=1)
        )
        mock_create.return_value = (
            fake_producer,
            {"bootstrap_servers": ["localhost:9092"], "topic": "atmosync_sensor_events"},
        )

        buf = io.StringIO()
        with redirect_stdout(buf):
            summary = run_throughput_test(num_events=6, dry_run=False)

        self.assertEqual(summary["mode"], "KAFKA-LIVE")
        self.assertEqual(summary["acknowledged_events"], 6)
        self.assertEqual(summary["failed_events"], 0)
        self.assertEqual(fake_producer.send.call_count, 6)
        fake_producer.flush.assert_called_once_with(timeout=10)
        fake_producer.close.assert_called_once_with(timeout=10)

    @patch("kafka_throughput_test.create_producer")
    def test_live_throughput_counts_partial_delivery_failures(self, mock_create):
        fake_producer = MagicMock()
        outcomes = [
            FakeFuture(metadata=SimpleNamespace(topic="atmosync_sensor_events", partition=0, offset=1)),
            FakeFuture(error=KafkaError("Simulated broker timeout")),
            FakeFuture(metadata=SimpleNamespace(topic="atmosync_sensor_events", partition=0, offset=2)),
        ]
        fake_producer.send.side_effect = outcomes
        mock_create.return_value = (
            fake_producer,
            {"bootstrap_servers": ["localhost:9092"], "topic": "atmosync_sensor_events"},
        )

        buf = io.StringIO()
        with redirect_stdout(buf):
            summary = run_throughput_test(num_events=3, dry_run=False)

        self.assertEqual(summary["acknowledged_events"], 2)
        self.assertEqual(summary["failed_events"], 1)
        fake_producer.close.assert_called_once_with(timeout=10)

    def test_main_exits_on_non_positive_num_events(self):
        buf = io.StringIO()
        with redirect_stdout(buf):
            with self.assertRaises(SystemExit) as ctx:
                main(["--num-events", "0", "--dry-run"])

        self.assertEqual(ctx.exception.code, 1)
        self.assertIn("[THROUGHPUT CONFIG ERROR]", buf.getvalue())


if __name__ == "__main__":
    unittest.main()