import io
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest.mock import MagicMock
from kafka.errors import KafkaError, KafkaTimeoutError

from kafka_producer import (
    ProducerDeliveryError,
    build_sample_event,
    close_producer,
    on_send_error,
    on_send_success,
    send_event,
)


class FakeFuture:
    """Deterministic future stub supporting callbacks, errbacks, and synchronous get()."""

    def __init__(self, metadata=None, error=None):
        self.metadata = metadata
        self.error = error
        self._callbacks = []
        self._errbacks = []

    def add_callback(self, fn):
        self._callbacks.append(fn)
        if self.error is None:
            fn(self.metadata)
        return self

    def add_errback(self, fn):
        self._errbacks.append(fn)
        if self.error is not None:
            fn(self.error)
        return self

    def get(self, timeout=None):
        if self.error is not None:
            raise self.error
        return self.metadata


class TestProducerDeliveryHandling(unittest.TestCase):
    def test_send_event_success_triggers_callback_and_returns_metadata(self):
        expected_meta = SimpleNamespace(
            topic="atmosync_sensor_events",
            partition=1,
            offset=42,
        )
        producer = MagicMock()
        producer.send.return_value = FakeFuture(metadata=expected_meta)

        success_calls = []
        error_calls = []
        event = build_sample_event()

        meta = send_event(
            producer,
            "atmosync_sensor_events",
            event,
            timeout=5,
            on_success=lambda m, event_id: success_calls.append((m, event_id)),
            on_error=lambda exc, event_id, topic: error_calls.append((exc, event_id, topic)),
        )

        producer.send.assert_called_once_with(
            "atmosync_sensor_events",
            key="C001",
            value=event,
        )
        self.assertEqual(meta.partition, 1)
        self.assertEqual(meta.offset, 42)
        self.assertEqual(len(success_calls), 1)
        self.assertEqual(success_calls[0][1], "EV000001")
        self.assertEqual(len(error_calls), 0)

    def test_send_event_timeout_invokes_errback_and_raises_delivery_error(self):
        producer = MagicMock()
        producer.send.return_value = FakeFuture(error=KafkaTimeoutError("Broker ack timed out"))

        errback_events = []
        event = build_sample_event()

        with self.assertRaises(ProducerDeliveryError) as ctx:
            send_event(
                producer,
                "atmosync_sensor_events",
                event,
                timeout=3,
                on_error=lambda exc, event_id, topic: errback_events.append((type(exc), event_id, topic)),
            )

        self.assertIn("timed out after 3s", str(ctx.exception))
        self.assertEqual(len(errback_events), 2)  # once from errback, once from timeout handler
        self.assertEqual(errback_events[0][0], KafkaTimeoutError)
        self.assertEqual(errback_events[0][1], "EV000001")

    def test_send_event_kafka_error_raises_delivery_error(self):
        producer = MagicMock()
        producer.send.return_value = FakeFuture(error=KafkaError("MessageSizeTooLargeError"))

        with self.assertRaises(ProducerDeliveryError) as ctx:
            send_event(producer, "atmosync_sensor_events", build_sample_event())

        self.assertIn("Kafka delivery failed", str(ctx.exception))
        self.assertIn("EV000001", str(ctx.exception))

    def test_send_event_rejects_non_dict_payload(self):
        producer = MagicMock()
        with self.assertRaises(ValueError):
            send_event(producer, "atmosync_sensor_events", "not-a-dict")
        producer.send.assert_not_called()

    def test_default_callbacks_log_structured_messages(self):
        buf = io.StringIO()
        meta = SimpleNamespace(topic="atmosync_sensor_events", partition=0, offset=99)
        with redirect_stdout(buf):
            on_send_success(meta, event_id="EV000099")
            on_send_error(KafkaError("Broker unavailable"), event_id="EV000099", topic="atmosync_sensor_events")

        output = buf.getvalue()
        self.assertIn("[DELIVERY ACK] event_id=EV000099", output)
        self.assertIn("partition=0 offset=99", output)
        self.assertIn("[DELIVERY ERROR] event_id=EV000099", output)
        self.assertIn("KafkaError", output)

    def test_close_producer_flushes_and_closes_even_on_flush_error(self):
        producer = MagicMock()
        producer.flush.side_effect = KafkaError("Flush failed")

        buf = io.StringIO()
        with redirect_stdout(buf):
            close_producer(producer, timeout=5)

        producer.flush.assert_called_once_with(timeout=5)
        producer.close.assert_called_once_with(timeout=5)
        self.assertIn("[PRODUCER FLUSH ERROR]", buf.getvalue())


if __name__ == "__main__":
    unittest.main()