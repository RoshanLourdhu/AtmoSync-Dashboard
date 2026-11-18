"""
Unit and integration tests for AtmoSync Kafka Producer and Simulator.

Verifies:
1. Producer configuration loading from environment (including flush timeout and retries).
2. Message serialization, container_id partitioning keys, and broker delivery callbacks.
3. Delivery statistics tracking (produced, delivered, failed, pending, in-flight).
4. Bounded retry on transient buffer errors and isolation of produce/validation failures.
5. Configurable flushing, timeout pending reporting, and graceful shutdown lifecycle.
6. Pre-send schema validation rejecting non-conforming payloads before reaching Kafka.
7. Sensor simulator continuous physics, anomaly injection, and strict schema compliance.
8. End-to-end Producer-to-Consumer integration:
   - Valid producer events flow through consumer into Snowflake batch insertion.
   - Offsets committed only after database success.
   - Database failures prevent offset commitment.
   - Legacy unaligned payloads are safely quarantined.
"""

from __future__ import annotations

import json
import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from sensor_simulator.simulator import DEFAULT_CONTAINERS, SensorSimulator
from shared.validation import SensorValidationError
from snowflake.ingestion import SnowflakeIngestionError
from streaming.consumer.consumer import SensorKafkaConsumer
from streaming.producer.producer import (
    KafkaProducerError,
    ProducerConfig,
    ProducerStats,
    SensorKafkaProducer,
)


class MockKafkaMessage:
    """Mock representing confluent_kafka.Message for consumer verification."""

    def __init__(
        self,
        value: bytes | str,
        topic: str = "container-sensor-data",
        partition: int = 0,
        offset: int = 100,
        error: Any = None,
    ) -> None:
        self._value = value.encode("utf-8") if isinstance(value, str) else value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._error = error

    def value(self) -> bytes:
        return self._value

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Any:
        return self._error


@pytest.fixture
def valid_sensor_event() -> dict[str, Any]:
    return {
        "container_id": "C001",
        "shipment_id": "SHIP-1001",
        "commodity": "Avocados",
        "temperature": 5.8,
        "humidity": 86.4,
        "vibration": 0.14,
        "timestamp": "2026-11-13T12:00:00Z",
    }


@pytest.fixture
def sam_legacy_unaligned_event() -> dict[str, Any]:
    return {
        "event_id": "EV000001",
        "timestamp_utc": "2026-11-03T14:13:38+00:00",
        "container_id": "C001",
        "commodity": "Avocados",
        "origin": "Chennai",
        "destination": "Bengaluru",
        "quantity_kg": 1000,
        "temperature_c": 5.54,
        "humidity_pct": 86.1,
        "vibration_g": 0.125,
        "sensor_status": "NORMAL",
    }


# =========================================================================
# 1. Producer Configuration Tests
# =========================================================================


def test_producer_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:9092")
    monkeypatch.setenv("KAFKA_TOPIC_SENSOR_DATA", "custom-sensor-topic")
    monkeypatch.setenv("KAFKA_PRODUCER_CLIENT_ID", "test-producer-client")
    monkeypatch.setenv("KAFKA_PRODUCER_ACKS", "1")
    monkeypatch.setenv("KAFKA_PRODUCER_VALIDATE", "true")

    config = ProducerConfig.from_env()

    assert config.bootstrap_servers == "kafka-broker:9092"
    assert config.topic == "custom-sensor-topic"
    assert config.client_id == "test-producer-client"
    assert config.acks == "1"
    assert config.validate_before_send is True


# =========================================================================
# 2. Producer Core Functionality Tests
# =========================================================================


def test_producer_event_serialization_and_key(valid_sensor_event: dict[str, Any]) -> None:
    mock_producer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(topic="container-sensor-data"),
        kafka_producer=mock_producer,
    )

    success = producer.produce_event(valid_sensor_event)

    assert success is True
    mock_producer.produce.assert_called_once()
    kwargs = mock_producer.produce.call_args[1]

    assert kwargs["topic"] == "container-sensor-data"
    assert kwargs["key"] == b"C001"
    assert json.loads(kwargs["value"].decode("utf-8")) == valid_sensor_event
    mock_producer.poll.assert_called_once_with(0)


def test_producer_prevalidation_rejection(sam_legacy_unaligned_event: dict[str, Any]) -> None:
    """Verifies that unaligned events are rejected before touching Kafka."""
    mock_producer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(validate_before_send=True),
        kafka_producer=mock_producer,
    )

    with pytest.raises(SensorValidationError):
        producer.produce_event(sam_legacy_unaligned_event)

    mock_producer.produce.assert_not_called()


def test_producer_custom_topic_and_key(valid_sensor_event: dict[str, Any]) -> None:
    mock_producer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    producer.produce_event(
        valid_sensor_event,
        topic="priority-alerts",
        key="custom-key-123",
    )

    kwargs = mock_producer.produce.call_args[1]
    assert kwargs["topic"] == "priority-alerts"
    assert kwargs["key"] == b"custom-key-123"


def test_producer_batch_production(valid_sensor_event: dict[str, Any]) -> None:
    mock_producer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    events = [dict(valid_sensor_event), dict(valid_sensor_event)]
    count = producer.produce_batch(events)

    assert count == 2
    assert mock_producer.produce.call_count == 2
    mock_producer.flush.assert_called_once()


def test_producer_flush_and_close() -> None:
    mock_producer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    producer.close()
    mock_producer.flush.assert_called_once()


# =========================================================================
# 3. Sensor Simulator Tests
# =========================================================================


def test_simulator_generates_schema_valid_events() -> None:
    simulator = SensorSimulator()
    for container in DEFAULT_CONTAINERS:
        event = simulator.generate_event(container)

        assert event["container_id"] == container["container_id"]
        assert event["shipment_id"] == container["shipment_id"]
        assert event["commodity"] == container["commodity"]
        assert 1.0 <= event["temperature"] <= 25.0
        assert 0.0 <= event["humidity"] <= 100.0
        assert event["vibration"] >= 0.0
        assert "T" in event["timestamp"] and event["timestamp"].endswith("Z")


def test_simulator_batch_generation() -> None:
    simulator = SensorSimulator()
    batch = simulator.generate_batch()

    assert len(batch) == 3
    container_ids = [e["container_id"] for e in batch]
    assert container_ids == ["C001", "C002", "C003"]


def test_simulator_physics_bounds_over_multiple_steps() -> None:
    simulator = SensorSimulator()
    c = DEFAULT_CONTAINERS[0]

    for _ in range(50):
        event = simulator.generate_event(c)
        assert 1.0 <= event["temperature"] <= 25.0
        assert 45.0 <= event["humidity"] <= 98.0
        assert 0.02 <= event["vibration"] <= 1.0


# =========================================================================
# 4. End-to-End Producer to Consumer Integration Tests
# =========================================================================


def test_end_to_end_producer_consumer_batch_flow() -> None:
    """
    Tests complete integration pipeline:
    1. Simulator creates schema-compliant event.
    2. Producer serializes and queues message.
    3. Consumer polls message, deserializes, and validates.
    4. Batch inserted into Snowflake RAW table.
    5. Kafka consumer commits offsets.
    """
    mock_kproducer = MagicMock()
    producer = SensorKafkaProducer(
        config=ProducerConfig(topic="container-sensor-data"),
        kafka_producer=mock_kproducer,
    )

    simulator = SensorSimulator()
    simulated_events = simulator.generate_batch()

    # Produce events
    producer.produce_batch(simulated_events)
    assert mock_kproducer.produce.call_count == 3

    # Collect produced payloads from mock
    produced_messages = []
    for idx, call in enumerate(mock_kproducer.produce.call_args_list):
        kwargs = call[1]
        msg = MockKafkaMessage(
            value=kwargs["value"],
            topic=kwargs["topic"],
            partition=0,
            offset=100 + idx,
        )
        produced_messages.append(msg)

    # Feed produced messages directly to Consumer
    mock_kconsumer = MagicMock()
    consumer = SensorKafkaConsumer(
        kafka_consumer=mock_kconsumer,
        snowflake_connection=MagicMock(),
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events", return_value=True
    ) as mock_snowflake_batch:
        result = consumer.process_batch(produced_messages)

    assert result.valid_count == 3
    assert result.invalid_count == 0
    assert result.committed is True
    assert result.inserted_into_snowflake is True

    # Verify Snowflake batch received all 3 valid events with Kafka metadata
    mock_snowflake_batch.assert_called_once()
    passed_tuples = mock_snowflake_batch.call_args[1]["events"]
    assert len(passed_tuples) == 3
    for i, t in enumerate(passed_tuples):
        assert t[0]["container_id"] == simulated_events[i]["container_id"]
        assert t[1:] == ("container-sensor-data", 0, 100 + i)

    # Verify Kafka commit called only after database insertion
    mock_kconsumer.commit.assert_called_once_with(asynchronous=False)


def test_end_to_end_database_failure_prevents_offset_commit() -> None:
    """Verifies that Snowflake insertion failure prevents Kafka offset commitment."""
    simulator = SensorSimulator()
    simulated_event = simulator.generate_event(DEFAULT_CONTAINERS[0])

    msg = MockKafkaMessage(
        value=json.dumps(simulated_event),
        topic="container-sensor-data",
        partition=0,
        offset=200,
    )

    mock_kconsumer = MagicMock()
    consumer = SensorKafkaConsumer(
        kafka_consumer=mock_kconsumer,
        snowflake_connection=MagicMock(),
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events",
        side_effect=SnowflakeIngestionError("Snowflake warehouse unavailable"),
    ):
        with pytest.raises(SnowflakeIngestionError):
            consumer.process_batch([msg])

    # Offset commit MUST NOT be called!
    mock_kconsumer.commit.assert_not_called()


def test_end_to_end_quarantines_legacy_unaligned_payload(
    sam_legacy_unaligned_event: dict[str, Any]
) -> None:
    """
    Verifies that when legacy unaligned events arrive alongside valid producer events:
    - Legacy unaligned events are quarantined in invalid_records.
    - Valid events are successfully batched into Snowflake.
    """
    simulator = SensorSimulator()
    valid_event = simulator.generate_event(DEFAULT_CONTAINERS[0])

    valid_msg = MockKafkaMessage(
        value=json.dumps(valid_event),
        topic="container-sensor-data",
        partition=0,
        offset=301,
    )
    legacy_msg = MockKafkaMessage(
        value=json.dumps(sam_legacy_unaligned_event),
        topic="container-sensor-data",
        partition=0,
        offset=302,
    )

    mock_kconsumer = MagicMock()
    consumer = SensorKafkaConsumer(
        kafka_consumer=mock_kconsumer,
        snowflake_connection=MagicMock(),
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events", return_value=True
    ) as mock_snowflake_batch:
        result = consumer.process_batch([valid_msg, legacy_msg])

    assert result.valid_count == 1
    assert result.invalid_count == 1
    assert result.committed is True
    assert result.inserted_into_snowflake is True

    # Check quarantine details
    assert len(result.invalid_records) == 1
    assert result.invalid_records[0]["offset"] == 302
    assert "Schema validation error" in result.invalid_records[0]["error"]

    # Only 1 valid event passed to Snowflake
    mock_snowflake_batch.assert_called_once()
    assert len(mock_snowflake_batch.call_args[1]["events"]) == 1


# =========================================================================
# 5. Producer Delivery Reliability, Error Handling & Graceful Shutdown Tests
# =========================================================================


def test_producer_config_reliability_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifies reliability configuration parameters load from environment."""
    monkeypatch.setenv("KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS", "15.5")
    monkeypatch.setenv("KAFKA_PRODUCER_MAX_RETRIES", "5")
    monkeypatch.setenv("KAFKA_PRODUCER_RETRY_BACKOFF_SECONDS", "0.5")

    config = ProducerConfig.from_env()

    assert config.flush_timeout_seconds == 15.5
    assert config.max_retries == 5
    assert config.retry_backoff_seconds == 0.5


def test_producer_successful_delivery_callback_updates_stats(
    valid_sensor_event: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verifies broker delivery confirmation is not marked delivered until callback fires."""
    mock_producer = MagicMock()
    captured_callbacks: list[Any] = []
    custom_called: list[tuple[Any, Any]] = []

    def capture_produce(**kwargs: Any) -> None:
        captured_callbacks.append(kwargs["on_delivery"])

    mock_producer.produce.side_effect = capture_produce

    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    with caplog.at_level("INFO"):
        producer.produce_event(
            valid_sensor_event,
            on_delivery=lambda err, msg: custom_called.append((err, msg)),
        )

        # Queued, NOT yet confirmed delivered by broker
        assert producer.stats.produced == 1
        assert producer.stats.delivered == 0
        assert producer.stats.in_flight == 1

        # Simulate broker acknowledging delivery
        broker_msg = MockKafkaMessage(json.dumps(valid_sensor_event), partition=2, offset=42)
        captured_callbacks[0](None, broker_msg)

    assert producer.stats.produced == 1
    assert producer.stats.delivered == 1
    assert producer.stats.failed == 0
    assert producer.stats.in_flight == 0
    assert len(custom_called) == 1
    assert custom_called[0] == (None, broker_msg)
    assert "Kafka message delivered" in caplog.text


def test_producer_failed_delivery_callback_tracks_and_logs_error(
    valid_sensor_event: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verifies broker delivery failure callback increments failed counter and logs error."""
    mock_producer = MagicMock()
    captured_callbacks: list[Any] = []

    def capture_produce(**kwargs: Any) -> None:
        captured_callbacks.append(kwargs["on_delivery"])

    mock_producer.produce.side_effect = capture_produce

    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    with caplog.at_level("ERROR"):
        producer.produce_event(valid_sensor_event)
        captured_callbacks[0]("Broker: Message timed out", None)

    assert producer.stats.produced == 1
    assert producer.stats.delivered == 0
    assert producer.stats.delivery_failed == 1
    assert producer.stats.failed == 1
    assert producer.stats.in_flight == 0
    assert len(producer.stats.delivery_errors) == 1
    assert "Broker: Message timed out" in producer.stats.delivery_errors[0]
    assert "Kafka broker delivery failed" in caplog.text


def test_producer_produce_exception_non_retryable_fails_cleanly(
    valid_sensor_event: dict[str, Any],
) -> None:
    """Verifies non-retryable Kafka produce exceptions fail immediately without infinite retries."""
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = RuntimeError("Broker serialization error")

    producer = SensorKafkaProducer(
        config=ProducerConfig(max_retries=3),
        kafka_producer=mock_producer,
    )

    with pytest.raises(KafkaProducerError, match="Failed to produce message"):
        producer.produce_event(valid_sensor_event)

    # Non-retryable error must NOT retry
    assert mock_producer.produce.call_count == 1
    assert producer.stats.retries == 0
    assert producer.stats.produce_failed == 1
    assert producer.stats.produced == 0


def test_producer_bounded_retry_on_buffer_error(
    valid_sensor_event: dict[str, Any],
) -> None:
    """Verifies bounded retry drains local queue on BufferError and succeeds without duplicates."""
    mock_producer = MagicMock()
    # Fail twice with BufferError, succeed on 3rd attempt
    mock_producer.produce.side_effect = [
        BufferError("Local: Queue full"),
        BufferError("Local: Queue full"),
        None,
    ]

    producer = SensorKafkaProducer(
        config=ProducerConfig(max_retries=3, retry_backoff_seconds=0.01),
        kafka_producer=mock_producer,
    )

    result = producer.produce_event(valid_sensor_event)

    assert result is True
    assert mock_producer.produce.call_count == 3
    assert producer.stats.retries == 2
    assert producer.stats.produced == 1
    assert producer.stats.produce_failed == 0


def test_producer_bounded_retry_exhaustion_raises(
    valid_sensor_event: dict[str, Any],
) -> None:
    """Verifies bounded retry stops cleanly after max_retries without infinite loop."""
    mock_producer = MagicMock()
    mock_producer.produce.side_effect = BufferError("Local: Queue permanently full")

    producer = SensorKafkaProducer(
        config=ProducerConfig(max_retries=2, retry_backoff_seconds=0.01),
        kafka_producer=mock_producer,
    )

    with pytest.raises(KafkaProducerError, match="Failed to produce message"):
        producer.produce_event(valid_sensor_event)

    # 1 initial attempt + 2 retries = 3 total attempts
    assert mock_producer.produce.call_count == 3
    assert producer.stats.retries == 2
    assert producer.stats.produce_failed == 1
    assert producer.stats.produced == 0


def test_producer_flush_timeout_reports_pending_messages(
    valid_sensor_event: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verifies flush timeout accurately reports undelivered pending messages."""
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 4  # 4 messages still undelivered

    producer = SensorKafkaProducer(
        config=ProducerConfig(flush_timeout_seconds=2.5),
        kafka_producer=mock_producer,
    )

    with caplog.at_level("WARNING"):
        remaining = producer.flush()

    assert remaining == 4
    assert producer.stats.pending == 4
    mock_producer.flush.assert_called_once_with(2.5)
    assert "flush timed out after 2.5s with 4 message(s) still undelivered/pending" in caplog.text


def test_producer_mixed_successful_and_failed_deliveries_in_batch(
    valid_sensor_event: dict[str, Any],
    sam_legacy_unaligned_event: dict[str, Any],
) -> None:
    """
    Verifies a batch containing mixed valid events, invalid events, broker successes,
    and broker failures isolates failures without crashing remaining events.
    """
    mock_producer = MagicMock()
    callbacks: list[Any] = []

    def side_effect_produce(**kwargs: Any) -> None:
        callbacks.append(kwargs["on_delivery"])

    mock_producer.produce.side_effect = side_effect_produce
    mock_producer.flush.return_value = 0

    producer = SensorKafkaProducer(
        config=ProducerConfig(validate_before_send=True),
        kafka_producer=mock_producer,
    )

    ev1 = dict(valid_sensor_event, container_id="C001")
    ev2 = dict(sam_legacy_unaligned_event)  # Invalid schema
    ev3 = dict(valid_sensor_event, container_id="C002")

    queued = producer.produce_batch([ev1, ev2, ev3], continue_on_error=True)

    # 2 valid queued, 1 rejected by schema without crashing batch
    assert queued == 2
    assert producer.stats.produced == 2
    assert producer.stats.validation_errors == 1
    assert len(callbacks) == 2

    # Simulate 1 successful broker delivery and 1 broker delivery failure
    callbacks[0](None, MockKafkaMessage(json.dumps(ev1), offset=10))
    callbacks[1]("Broker: Leader not available", None)

    stats = producer.get_stats()
    assert stats["produced"] == 2
    assert stats["delivered"] == 1
    assert stats["delivery_failed"] == 1
    assert stats["failed"] == 1
    assert stats["validation_errors"] == 1
    assert stats["pending"] == 0
    assert stats["in_flight"] == 0


def test_producer_graceful_shutdown_blocks_new_events_and_flushes(
    valid_sensor_event: dict[str, Any],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Verifies close() halts new event acceptance, flushes buffer, and logs final stats."""
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 0

    producer = SensorKafkaProducer(
        config=ProducerConfig(flush_timeout_seconds=5.0),
        kafka_producer=mock_producer,
    )

    with caplog.at_level("INFO"):
        producer.produce_event(valid_sensor_event)
        remaining = producer.close()

    assert remaining == 0
    assert producer.is_closed is True
    mock_producer.flush.assert_called_once_with(5.0)
    assert "Initiating SensorKafkaProducer graceful shutdown" in caplog.text
    assert "Kafka producer closed cleanly. Final stats:" in caplog.text

    # Subsequent event production must be rejected cleanly
    with pytest.raises(KafkaProducerError, match="closed or shutting down"):
        producer.produce_event(valid_sensor_event)


def test_producer_stream_simulation_graceful_stop_event() -> None:
    """Verifies stream_simulation stops generating events on stop_event, flushes, and closes."""
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 0
    simulator = SensorSimulator()

    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    stop_event = threading.Event()
    stop_event.set()  # Pre-set termination signal

    stats = producer.stream_simulation(
        simulator,
        max_cycles=5,
        stop_event=stop_event,
    )

    # Zero events generated because termination was already signaled
    assert stats.produced == 0
    assert producer.is_closed is True
    mock_producer.produce.assert_not_called()
    # Flush still executed during graceful close
    mock_producer.flush.assert_called_once()


def test_producer_stream_simulation_runs_exact_cycles_and_closes() -> None:
    """Verifies stream_simulation runs exact max_cycles and closes cleanly."""
    mock_producer = MagicMock()
    mock_producer.flush.return_value = 0
    simulator = SensorSimulator()  # 3 containers per batch

    producer = SensorKafkaProducer(
        config=ProducerConfig(),
        kafka_producer=mock_producer,
    )

    stats = producer.stream_simulation(
        simulator,
        max_cycles=2,
        interval_seconds=0.0,
    )

    # 2 cycles * 3 containers = 6 queued events
    assert stats.produced == 6
    assert producer.is_closed is True
    assert mock_producer.produce.call_count == 6
    # 2 batch flushes + 1 final shutdown flush = 3 flush calls
    assert mock_producer.flush.call_count == 3
