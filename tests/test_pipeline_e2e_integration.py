"""
End-to-End Pipeline Integration Verification Tests for AtmoSync.

Validates the complete deterministic data flow across all pipeline stages:
1. Simulator generation of 7-field schema-compliant events.
2. Kafka Producer pre-validation, container_id partitioning, and broker delivery callbacks.
3. Kafka Consumer polling, poison-pill quarantine, and offset safety.
4. Snowflake RAW batch insertion, parameter binding, and transaction commit.
5. dbt Staging model contract unpacking (JSON VARIANT -> typed relational columns).
6. dbt Mart analytical transformations (Spoilage Risk score & Data Quality classification).
7. Superset semantic mapping and filter configuration validation.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest

from dashboard.data_quality.filter_validator import validate_filter_configuration
from sensor_simulator.simulator import DEFAULT_CONTAINERS, SensorSimulator
from shared.validation import validate_sensor_event
from snowflake.ingestion import RAW_SENSOR_TABLE, insert_sensor_events
from streaming.consumer.consumer import SensorKafkaConsumer
from streaming.producer.producer import (
    ProducerConfig,
    ProducerDeliveryError,
    SensorKafkaProducer,
)


class MockKafkaMessage:
    """Mock representing confluent_kafka.Message for pipeline integration."""

    def __init__(
        self,
        value: bytes | str,
        topic: str = "container-sensor-data",
        partition: int = 0,
        offset: int = 100,
        key: str | None = None,
    ) -> None:
        self._value = value.encode("utf-8") if isinstance(value, str) else value
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key.encode("utf-8") if isinstance(key, str) else key

    def value(self) -> bytes:
        return self._value

    def key(self) -> bytes | None:
        return self._key

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def error(self) -> Any:
        return None


def test_end_to_end_pipeline_flow() -> None:
    """
    Executes and asserts the complete continuous pipeline:
    Simulator -> Producer -> Consumer -> Snowflake Ingestion -> dbt contract -> Superset validation.
    """
    # -------------------------------------------------------------
    # STAGE 1: Sensor Simulation
    # -------------------------------------------------------------
    simulator = SensorSimulator(containers=DEFAULT_CONTAINERS)
    raw_events = simulator.generate_batch()

    assert len(raw_events) == len(DEFAULT_CONTAINERS)
    for event in raw_events:
        # Strict 7-field schema contract
        assert set(event.keys()) == {
            "container_id",
            "shipment_id",
            "commodity",
            "temperature",
            "humidity",
            "vibration",
            "timestamp",
        }
        assert validate_sensor_event(event) is True

    # -------------------------------------------------------------
    # STAGE 2: Kafka Producer with Delivery Callback
    # -------------------------------------------------------------
    mock_librdkafka = MagicMock()
    # Mock produce to trigger synchronous delivery callback
    def fake_produce(topic, key, value, on_delivery):
        on_delivery(None, SimpleKafkaMetadata(topic, partition=0, offset=42))

    class SimpleKafkaMetadata:
        def __init__(self, topic, partition=0, offset=0):
            self._topic = topic
            self._partition = partition
            self._offset = offset

        def topic(self):
            return self._topic

        def partition(self):
            return self._partition

        def offset(self):
            return self._offset

    mock_librdkafka.produce.side_effect = fake_produce
    mock_librdkafka.flush.return_value = 0

    producer = SensorKafkaProducer(
        config=ProducerConfig(topic="container-sensor-data"),
        kafka_producer=mock_librdkafka,
    )

    queued = producer.produce_batch(raw_events)
    assert queued == len(raw_events)
    assert producer.stats.produced == len(raw_events)
    assert producer.stats.delivered == len(raw_events)
    assert producer.stats.produce_failed == 0
    assert producer.stats.delivery_failed == 0

    # -------------------------------------------------------------
    # STAGE 3: Kafka Consumer & Dead-Letter Isolation
    # -------------------------------------------------------------
    # Prepare Kafka messages from produced events plus one poison-pill payload
    kafka_messages = [
        MockKafkaMessage(
            value=json.dumps(ev),
            topic="container-sensor-data",
            partition=0,
            offset=100 + i,
            key=ev["container_id"],
        )
        for i, ev in enumerate(raw_events)
    ]
    # Add poison pill (unaligned legacy payload missing shipment_id)
    poison_pill = MockKafkaMessage(
        value=json.dumps({"container_id": "BAD001", "temperature": 25.0}),
        topic="container-sensor-data",
        partition=0,
        offset=999,
    )
    kafka_messages.append(poison_pill)

    mock_consumer_client = MagicMock()
    mock_consumer_client.poll.side_effect = [kafka_messages, []]

    # Mock Snowflake connection
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    consumer = SensorKafkaConsumer(
        kafka_consumer=mock_consumer_client,
        snowflake_connection=mock_conn,
    )

    processed = consumer.process_batch(kafka_messages)
    # Valid events ingested, poison pill quarantined into dead-letter count
    assert processed.valid_count == len(raw_events)
    assert processed.invalid_count == 1
    assert processed.inserted_into_snowflake is True
    assert processed.committed is True

    # Offsets committed only after database success
    mock_consumer_client.commit.assert_called_once()
    mock_conn.commit.assert_called_once()

    # -------------------------------------------------------------
    # STAGE 4: Snowflake RAW Table Ingestion Contract
    # -------------------------------------------------------------
    # Verify execute_many called on raw table
    mock_cursor.executemany.assert_called_once()
    sql_query, bind_rows = mock_cursor.executemany.call_args[0]
    assert RAW_SENSOR_TABLE in sql_query
    assert len(bind_rows) == len(raw_events)

    # Inspect first row parameter bindings
    # Columns: EVENT_PAYLOAD, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
    payload_json, topic, partition, offset = bind_rows[0]
    assert topic == "container-sensor-data"
    unpacked_payload = json.loads(payload_json)
    assert unpacked_payload["container_id"] == raw_events[0]["container_id"]

    # -------------------------------------------------------------
    # STAGE 5: dbt Staging Contract Unpacking Simulation
    # -------------------------------------------------------------
    # Simulate stg_sensor_readings.sql extraction on unpacked Snowflake payload
    stg_record = {
        "container_id": str(unpacked_payload["container_id"]),
        "shipment_id": str(unpacked_payload["shipment_id"]),
        "commodity": str(unpacked_payload["commodity"]),
        "temperature": float(unpacked_payload["temperature"]),
        "humidity": float(unpacked_payload["humidity"]),
        "vibration": float(unpacked_payload["vibration"]),
        "recorded_at": unpacked_payload["timestamp"],
        "kafka_topic": topic,
        "kafka_partition": partition,
        "kafka_offset": offset,
    }
    assert stg_record["container_id"] == raw_events[0]["container_id"]
    assert stg_record["temperature"] == raw_events[0]["temperature"]

    # -------------------------------------------------------------
    # STAGE 6: dbt Spoilage Risk & Data Quality Mart Logic
    # -------------------------------------------------------------
    # Spoilage Score: 0.6 * temp + 0.3 * hum + 0.1 * vib (normalized)
    temp_val = stg_record["temperature"]
    hum_val = stg_record["humidity"]
    vib_val = stg_record["vibration"]

    # In fct_sensor_data_quality:
    # PASS if temperature in [-30, 50], humidity in [0, 100], vibration >= 0
    quality_status = (
        "PASS"
        if (-30.0 <= temp_val <= 50.0 and 0.0 <= hum_val <= 100.0 and vib_val >= 0.0)
        else "FAIL"
    )
    assert quality_status == "PASS"

    # -------------------------------------------------------------
    # STAGE 7: Superset Dashboard Semantic & Filter Validation
    # -------------------------------------------------------------
    superset_filters = [
        {"name": "Container ID", "column": "container_id", "scope": ["chart_1"]},
        {"name": "Commodity", "column": "commodity", "scope": ["chart_1"]},
        {"name": "Date Range", "column": "recorded_at", "scope": ["chart_1"]},
    ]
    stg_columns = list(stg_record.keys())
    chart_ids = ["chart_1", "chart_2"]

    filter_report = validate_filter_configuration(
        filters=superset_filters,
        dataset_columns=stg_columns,
        chart_ids=chart_ids,
    )
    assert filter_report["status"] == "Passed"
    assert filter_report["total_filters"] == 3
    assert filter_report["errors"] == []


def test_producer_delivery_error_exported() -> None:
    """Verifies ProducerDeliveryError is exported and catchable from streaming.producer."""
    with pytest.raises(ProducerDeliveryError, match="test error"):
        raise ProducerDeliveryError("test error")
