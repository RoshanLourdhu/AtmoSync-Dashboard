import os
import unittest
from unittest.mock import MagicMock, patch
from kafka.errors import NoBrokersAvailable

from kafka_producer import (
    ProducerConfigError,
    create_producer,
    validate_bootstrap_servers,
    validate_producer_config,
    validate_topic,
)


class TestProducerConfigValidation(unittest.TestCase):
    def test_valid_default_config(self):
        with patch.dict(os.environ, {}, clear=True):
            cfg = validate_producer_config()
        self.assertEqual(cfg["bootstrap_servers"], ["localhost:9092"])
        self.assertEqual(cfg["topic"], "atmosync_sensor_events")
        self.assertEqual(cfg["acks"], "all")
        self.assertEqual(cfg["retries"], 3)

    def test_valid_multiple_bootstrap_servers(self):
        servers = validate_bootstrap_servers("broker1:9092, broker2:9093")
        self.assertEqual(servers, ["broker1:9092", "broker2:9093"])

    def test_missing_or_empty_bootstrap_servers_raises(self):
        for bad in (None, "", "   ", []):
            with self.subTest(bad=bad):
                with self.assertRaises(ProducerConfigError) as ctx:
                    validate_bootstrap_servers(bad)
                self.assertIn("KAFKA_BOOTSTRAP_SERVERS", str(ctx.exception))

    def test_malformed_bootstrap_servers_raises(self):
        for bad in ("localhost", ":9092", "localhost:abc", "localhost:0", "localhost:70000"):
            with self.subTest(bad=bad):
                with self.assertRaises(ProducerConfigError):
                    validate_bootstrap_servers(bad)

    def test_invalid_topic_raises(self):
        for bad in (None, "", "   ", ".", "..", "bad topic!"):
            with self.subTest(bad=bad):
                with self.assertRaises(ProducerConfigError):
                    validate_topic(bad)

    def test_invalid_acks_raises(self):
        with self.assertRaises(ProducerConfigError) as ctx:
            validate_producer_config({"acks": "quorum"})
        self.assertIn("Invalid acks", str(ctx.exception))

    def test_invalid_numeric_settings_raise(self):
        invalid_cases = [
            {"retries": -1},
            {"retries": "abc"},
            {"linger_ms": -5},
            {"batch_size": 100},
            {"request_timeout_ms": 500},
            {"retries": True},
        ]
        for override in invalid_cases:
            with self.subTest(override=override):
                with self.assertRaises(ProducerConfigError):
                    validate_producer_config(override)

    def test_create_producer_passes_validated_settings(self):
        mock_cls = MagicMock()
        producer, cfg = create_producer(
            config={
                "bootstrap_servers": "127.0.0.1:9092",
                "topic": "atmosync_sensor_events",
                "acks": "1",
                "retries": 5,
            },
            producer_cls=mock_cls,
        )
        mock_cls.assert_called_once()
        kwargs = mock_cls.call_args.kwargs
        self.assertEqual(kwargs["bootstrap_servers"], ["127.0.0.1:9092"])
        self.assertEqual(kwargs["acks"], 1)
        self.assertEqual(kwargs["retries"], 5)
        self.assertEqual(cfg["topic"], "atmosync_sensor_events")

    def test_create_producer_no_brokers_clear_error(self):
        mock_cls = MagicMock(side_effect=NoBrokersAvailable())
        with self.assertRaises(RuntimeError) as ctx:
            create_producer(
                config={"bootstrap_servers": "localhost:9092"},
                producer_cls=mock_cls,
            )
        self.assertIn("No Kafka brokers reachable", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()