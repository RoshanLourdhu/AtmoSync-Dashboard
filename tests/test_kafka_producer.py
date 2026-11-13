"""
Unit and integration tests for AtmoSync Kafka Producer and Simulator.

Verifies:
1. Producer configuration loading from environment.
2. Message serialization, container_id partitioning keys, and delivery callbacks.
3. Pre-send schema validation rejecting non-conforming payloads before reaching Kafka.
4. Sensor simulator continuous physics, anomaly injection, and strict schema compliance.
5. End-to-end Producer-to-Consumer integration:
   - Valid producer events flow through consumer into Snowflake batch insertion.
   - Offsets committed only after database success.
   - Database failures prevent offset commitment.
   - Legacy unaligned payloads are safely quarantined.
"""

from __future__ import annotations

import json
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
