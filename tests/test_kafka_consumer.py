"""
Unit tests for the AtmoSync Kafka Consumer Module (streaming/consumer/consumer.py).

Verifies:
1. Environment variable configuration parsing.
2. Deserialization and runtime JSON Schema validation.
3. Safe isolation of malformed JSON and contract-violating payloads (e.g., Sam's unaligned events).
4. Batch ingestion via insert_sensor_events.
5. Strict offset management: commit offsets on DB success; prevent commit on DB failure.
6. Safe resource cleanup on shutdown.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from shared.validation import SensorValidationError
from snowflake.ingestion import SnowflakeIngestionError
from streaming.consumer.consumer import (
    BatchIngestionResult,
    ConsumerConfig,
    KafkaConsumerError,
    SensorKafkaConsumer,
)


class MockKafkaMessage:
    """Mock representing a confluent_kafka.Message."""

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
def valid_sensor_event_dict() -> dict[str, Any]:
    return {
        "container_id": "CONT-1001",
        "shipment_id": "SHIP-8801",
        "commodity": "Avocados",
        "temperature": 5.8,
        "humidity": 86.4,
        "vibration": 0.14,
        "timestamp": "2026-11-13T12:00:00Z",
    }


@pytest.fixture
def sam_unaligned_event_dict() -> dict[str, Any]:
    """Sample event from Sam's simulator demonstrating contract incompatibility."""
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


def test_consumer_config_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092,broker2:9092")
    monkeypatch.setenv("KAFKA_TOPIC_SENSOR_DATA", "test-sensor-topic")
    monkeypatch.setenv("KAFKA_CONSUMER_GROUP", "test-group")
    monkeypatch.setenv("KAFKA_BATCH_SIZE", "25")
    monkeypatch.setenv("KAFKA_POLL_TIMEOUT_SECONDS", "2.5")

    config = ConsumerConfig.from_env()

    assert config.bootstrap_servers == "broker1:9092,broker2:9092"
    assert config.topic == "test-sensor-topic"
    assert config.group_id == "test-group"
    assert config.batch_size == 25
    assert config.poll_timeout_seconds == 2.5
    assert config.enable_auto_commit is False


def test_deserialize_and_validate_valid_event(
    valid_sensor_event_dict: dict[str, Any]
) -> None:
    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=MagicMock(),
        snowflake_connection=MagicMock(),
    )
    payload_bytes = json.dumps(valid_sensor_event_dict).encode("utf-8")

    event, err = consumer.deserialize_and_validate(
        payload_bytes, "container-sensor-data", 0, 101
    )

    assert err is None
    assert event == valid_sensor_event_dict


def test_deserialize_and_validate_invalid_json() -> None:
    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=MagicMock(),
        snowflake_connection=MagicMock(),
    )
    bad_bytes = b'{"container_id": "C01", unclosed_json'

    event, err = consumer.deserialize_and_validate(
        bad_bytes, "container-sensor-data", 0, 102
    )

    assert event is None
    assert err is not None
    assert "JSON decode error" in err


def test_deserialize_and_validate_non_object_json() -> None:
    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=MagicMock(),
        snowflake_connection=MagicMock(),
    )
    array_bytes = b'[1, 2, 3]'

    event, err = consumer.deserialize_and_validate(
        array_bytes, "container-sensor-data", 0, 103
    )

    assert event is None
    assert err is not None
    assert "not a JSON object" in err


def test_deserialize_and_validate_sam_unaligned_payload_rejected(
    sam_unaligned_event_dict: dict[str, Any]
) -> None:
    """Verifies that Sam's simulator events are safely rejected by the contract."""
    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=MagicMock(),
        snowflake_connection=MagicMock(),
    )
    sam_bytes = json.dumps(sam_unaligned_event_dict).encode("utf-8")

    event, err = consumer.deserialize_and_validate(
        sam_bytes, "container-sensor-data", 0, 104
    )

    assert event is None
    assert err is not None
    assert "Schema validation error" in err


def test_poll_messages_respects_batch_size() -> None:
    mock_kconsumer = MagicMock()
    # Return 3 messages then None
    msg1 = MockKafkaMessage(json.dumps({"test": 1}), offset=1)
    msg2 = MockKafkaMessage(json.dumps({"test": 2}), offset=2)
    msg3 = MockKafkaMessage(json.dumps({"test": 3}), offset=3)
    mock_kconsumer.poll.side_effect = [msg1, msg2, msg3, None]

    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(batch_size=2),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=MagicMock(),
    )

    messages = consumer.poll_messages(max_records=2)

    assert len(messages) == 2
    assert messages[0] == msg1
    assert messages[1] == msg2


def test_process_batch_successful_ingestion_and_commit(
    valid_sensor_event_dict: dict[str, Any]
) -> None:
    mock_kconsumer = MagicMock()
    mock_sconnection = MagicMock()

    msg1 = MockKafkaMessage(json.dumps(valid_sensor_event_dict), offset=10)
    msg2 = MockKafkaMessage(json.dumps(valid_sensor_event_dict), offset=11)

    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=mock_sconnection,
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events", return_value=True
    ) as mock_batch_insert:
        result = consumer.process_batch([msg1, msg2])

    assert result.valid_count == 2
    assert result.invalid_count == 0
    assert result.committed is True
    assert result.inserted_into_snowflake is True

    # Snowflake batch insert called with both events
    mock_batch_insert.assert_called_once()
    passed_tuples = mock_batch_insert.call_args[1]["events"]
    assert len(passed_tuples) == 2
    assert passed_tuples[0][0] == valid_sensor_event_dict
    assert passed_tuples[0][1:] == ("container-sensor-data", 0, 10)

    # Kafka commit was executed synchronously
    mock_kconsumer.commit.assert_called_once_with(asynchronous=False)


def test_process_batch_database_failure_prevents_offset_commit(
    valid_sensor_event_dict: dict[str, Any]
) -> None:
    """Verifies that if Snowflake ingestion fails, Kafka offsets are NOT committed."""
    mock_kconsumer = MagicMock()
    mock_sconnection = MagicMock()

    msg = MockKafkaMessage(json.dumps(valid_sensor_event_dict), offset=50)

    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=mock_sconnection,
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events",
        side_effect=SnowflakeIngestionError("Database network timeout"),
    ):
        with pytest.raises(SnowflakeIngestionError):
            consumer.process_batch([msg])

    # Offset commit MUST NOT be called!
    mock_kconsumer.commit.assert_not_called()


def test_process_batch_mixed_valid_and_invalid_events(
    valid_sensor_event_dict: dict[str, Any],
    sam_unaligned_event_dict: dict[str, Any],
) -> None:
    """Verifies that invalid events are safely isolated while valid events are ingested."""
    mock_kconsumer = MagicMock()
    mock_sconnection = MagicMock()

    valid_msg = MockKafkaMessage(json.dumps(valid_sensor_event_dict), offset=100)
    sam_msg = MockKafkaMessage(json.dumps(sam_unaligned_event_dict), offset=101)
    corrupt_msg = MockKafkaMessage("{malformed json", offset=102)

    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=mock_sconnection,
    )

    with patch(
        "streaming.consumer.consumer.insert_sensor_events", return_value=True
    ) as mock_batch_insert:
        result = consumer.process_batch([valid_msg, sam_msg, corrupt_msg])

    assert result.valid_count == 1
    assert result.invalid_count == 2
    assert result.committed is True
    assert result.inserted_into_snowflake is True
    assert len(result.invalid_records) == 2

    # Only 1 valid event passed to Snowflake
    mock_batch_insert.assert_called_once()
    passed_events = mock_batch_insert.call_args[1]["events"]
    assert len(passed_events) == 1
    assert passed_events[0][0] == valid_sensor_event_dict

    # Offset committed so valid records progress and invalid are acknowledged
    mock_kconsumer.commit.assert_called_once_with(asynchronous=False)


def test_process_batch_empty_list() -> None:
    mock_kconsumer = MagicMock()
    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=MagicMock(),
    )

    result = consumer.process_batch([])

    assert result.valid_count == 0
    assert result.invalid_count == 0
    assert result.committed is False
    assert result.inserted_into_snowflake is False
    mock_kconsumer.commit.assert_not_called()


def test_consumer_close_cleanup() -> None:
    mock_kconsumer = MagicMock()
    mock_sconnection = MagicMock()

    consumer = SensorKafkaConsumer(
        config=ConsumerConfig(),
        kafka_consumer=mock_kconsumer,
        snowflake_connection=mock_sconnection,
    )
    consumer.close()

    mock_kconsumer.close.assert_called_once()
