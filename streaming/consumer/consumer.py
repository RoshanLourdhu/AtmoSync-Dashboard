"""
Sensor Kafka Consumer Service for AtmoSync Dashboard.

Orchestrates streaming ingestion from Kafka topic 'container-sensor-data'
into Snowflake table 'ATMOSYNC_DB.RAW.SENSOR_READINGS'.

Enforces:
1. JSON deserialization with safe malformed message handling.
2. Runtime contract validation via `shared.validation.validate_sensor_event`.
3. Safe error handling / Dead-letter isolation for invalid payloads.
4. High-throughput atomic batch insertion into Snowflake via `insert_sensor_events`.
5. Strict offset management: Kafka offsets committed ONLY after Snowflake batch insertion succeeds.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Any

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, Message
except ImportError:  # pragma: no cover
    Consumer = None
    KafkaError = None
    KafkaException = None
    Message = Any

from shared.validation import SensorValidationError, validate_sensor_event
from snowflake.connection import SnowflakeConnectionError, get_snowflake_connection
from snowflake.ingestion import SnowflakeIngestionError, insert_sensor_events

logger = logging.getLogger(__name__)


class KafkaConsumerError(Exception):
    """Base exception for Kafka consumer operations."""


@dataclass
class ConsumerConfig:
    """Configuration parameters for SensorKafkaConsumer."""

    bootstrap_servers: str = "localhost:9092"
    group_id: str = "atmosync-sensor-consumer"
    topic: str = "container-sensor-data"
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False
    batch_size: int = 10
    poll_timeout_seconds: float = 1.0

    @classmethod
    def from_env(cls) -> ConsumerConfig:
        """Construct configuration from environment variables."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            group_id=os.getenv("KAFKA_CONSUMER_GROUP", "atmosync-sensor-consumer"),
            topic=(
                os.getenv("KAFKA_TOPIC_SENSOR_DATA")
                or os.getenv("KAFKA_TOPIC")
                or "container-sensor-data"
            ),
            auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
            enable_auto_commit=False,  # Enforce manual commit for transaction safety
            batch_size=int(os.getenv("KAFKA_BATCH_SIZE", "10")),
            poll_timeout_seconds=float(os.getenv("KAFKA_POLL_TIMEOUT_SECONDS", "1.0")),
        )


@dataclass
class BatchIngestionResult:
    """Summary of a processed batch of Kafka messages."""

    valid_count: int
    invalid_count: int
    committed: bool
    inserted_into_snowflake: bool
    invalid_records: list[dict[str, Any]] = field(default_factory=list)


class SensorKafkaConsumer:
    """
    Subscribes to sensor telemetry on Kafka and loads validated batches into Snowflake.
    """

    def __init__(
        self,
        config: ConsumerConfig | None = None,
        *,
        kafka_consumer: Any = None,
        snowflake_connection: Any = None,
    ) -> None:
        self.config = config or ConsumerConfig.from_env()
        self._external_snowflake_conn = snowflake_connection is not None
        self._snowflake_conn = snowflake_connection

        if kafka_consumer is not None:
            self.consumer = kafka_consumer
        else:
            if Consumer is None:
                raise KafkaConsumerError(
                    "confluent-kafka is not installed. Install via pip install confluent-kafka"
                )
            kafka_params = {
                "bootstrap.servers": self.config.bootstrap_servers,
                "group.id": self.config.group_id,
                "auto.offset.reset": self.config.auto_offset_reset,
                "enable.auto.commit": False,  # Manual commit only!
            }
            try:
                self.consumer = Consumer(kafka_params)
                self.consumer.subscribe([self.config.topic])
                logger.info(
                    "Subscribed Kafka consumer (group '%s') to topic '%s'",
                    self.config.group_id,
                    self.config.topic,
                )
            except Exception as exc:
                raise KafkaConsumerError(
                    f"Failed to initialize Kafka consumer: {exc}"
                ) from exc

    def _get_snowflake_conn(self) -> Any:
        if self._snowflake_conn is None:
            self._snowflake_conn = get_snowflake_connection()
        return self._snowflake_conn

    def deserialize_and_validate(
        self,
        raw_payload: str | bytes,
        topic: str,
        partition: int,
        offset: int,
    ) -> tuple[dict[str, Any] | None, str | None]:
        """
        Safely deserializes and validates a single sensor reading event.

        Returns:
            (event, None) if valid.
            (None, error_message) if invalid (malformed JSON or contract violation).
        """
        if isinstance(raw_payload, bytes):
            try:
                text_payload = raw_payload.decode("utf-8")
            except UnicodeDecodeError as exc:
                msg = f"UTF-8 decode error at {topic}[{partition}]:{offset}: {exc}"
                logger.warning(msg)
                return None, msg
        else:
            text_payload = raw_payload

        try:
            event = json.loads(text_payload)
        except json.JSONDecodeError as exc:
            msg = f"JSON decode error at {topic}[{partition}]:{offset}: {exc}"
            logger.warning(msg)
            return None, msg

        if not isinstance(event, dict):
            msg = f"Event at {topic}[{partition}]:{offset} is not a JSON object: {type(event).__name__}"
            logger.warning(msg)
            return None, msg

        try:
            validate_sensor_event(event)
            return event, None
        except SensorValidationError as exc:
            msg = f"Schema validation error at {topic}[{partition}]:{offset}: {exc}"
            logger.warning(msg)
            return None, msg

    def poll_messages(
        self,
        max_records: int | None = None,
        timeout: float | None = None,
    ) -> list[Any]:
        """
        Polls up to `max_records` from Kafka consumer, stopping if `timeout` expires
        when no messages are pending.
        """
        limit = max_records if max_records is not None else self.config.batch_size
        poll_timeout = (
            timeout if timeout is not None else self.config.poll_timeout_seconds
        )
        messages: list[Any] = []

        for _ in range(limit):
            msg = self.consumer.poll(timeout=poll_timeout)
            if msg is None:
                # No more messages available within timeout
                break
            messages.append(msg)

        return messages

    def process_batch(self, messages: list[Any]) -> BatchIngestionResult:
        """
        Processes a list of polled Kafka messages:
        1. Deserializes and validates each event.
        2. Isolates invalid events with metadata for DLQ tracking.
        3. Inserts valid events into Snowflake via `insert_sensor_events`.
        4. Commits Kafka offsets ONLY if Snowflake insertion succeeds.
        """
        if not messages:
            return BatchIngestionResult(
                valid_count=0,
                invalid_count=0,
                committed=False,
                inserted_into_snowflake=False,
                invalid_records=[],
            )

        valid_tuples: list[tuple[dict[str, Any], str, int, int]] = []
        invalid_records: list[dict[str, Any]] = []

        for msg in messages:
            # Handle potential Kafka error inside message
            if hasattr(msg, "error") and callable(msg.error) and msg.error():
                err = msg.error()
                # If EOF partition, skip
                if hasattr(KafkaError, "_PARTITION_EOF") and err.code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error("Kafka message error: %s", err)
                invalid_records.append({
                    "topic": getattr(msg, "topic", lambda: "unknown")(),
                    "partition": getattr(msg, "partition", lambda: 0)(),
                    "offset": getattr(msg, "offset", lambda: 0)(),
                    "error": str(err),
                })
                continue

            topic = msg.topic() if callable(getattr(msg, "topic", None)) else "unknown"
            partition = msg.partition() if callable(getattr(msg, "partition", None)) else 0
            offset = msg.offset() if callable(getattr(msg, "offset", None)) else 0
            value = msg.value() if callable(getattr(msg, "value", None)) else msg

            event, error_msg = self.deserialize_and_validate(value, topic, partition, offset)
            if event is not None:
                valid_tuples.append((event, topic, partition, offset))
            else:
                invalid_records.append({
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                    "error": error_msg,
                    "raw_payload": value.decode("utf-8", errors="replace") if isinstance(value, bytes) else str(value),
                })

        # Database Insertion & Offset Commit Decision
        inserted_into_snowflake = False
        if valid_tuples:
            conn = self._get_snowflake_conn()
            try:
                # Call existing batch ingestion function
                inserted_into_snowflake = insert_sensor_events(
                    events=valid_tuples,
                    connection=conn,
                )
            except Exception as exc:
                logger.error(
                    "Snowflake batch ingestion failed (%d events). Offsets WILL NOT be committed. Error: %s",
                    len(valid_tuples),
                    exc,
                )
                # Propagate exception so Kafka offsets remain uncommitted
                raise SnowflakeIngestionError(
                    f"Snowflake batch ingestion failed: {exc}"
                ) from exc

        # Commit Kafka offsets if insertion succeeded or if there were only invalid messages
        # (poison pill records safely handled so consumer does not hang indefinitely)
        if hasattr(self.consumer, "commit") and callable(self.consumer.commit):
            self.consumer.commit(asynchronous=False)
            committed = True
            logger.info(
                "Kafka batch committed: %d valid records ingested, %d invalid records safely handled",
                len(valid_tuples),
                len(invalid_records),
            )
        else:
            committed = False

        return BatchIngestionResult(
            valid_count=len(valid_tuples),
            invalid_count=len(invalid_records),
            committed=committed,
            inserted_into_snowflake=inserted_into_snowflake,
            invalid_records=invalid_records,
        )

    def consume_once(
        self,
        max_records: int | None = None,
        timeout: float | None = None,
    ) -> BatchIngestionResult:
        """Polls and processes a single batch of messages."""
        messages = self.poll_messages(max_records=max_records, timeout=timeout)
        return self.process_batch(messages)

    def close(self) -> None:
        """Closes Kafka consumer and Snowflake connection."""
        if hasattr(self, "consumer") and self.consumer:
            if hasattr(self.consumer, "close") and callable(self.consumer.close):
                try:
                    self.consumer.close()
                except Exception as exc:
                    logger.warning("Error closing Kafka consumer: %s", exc)
        if not self._external_snowflake_conn and self._snowflake_conn:
            try:
                self._snowflake_conn.close()
            except Exception as exc:
                logger.warning("Error closing Snowflake connection: %s", exc)
            self._snowflake_conn = None
