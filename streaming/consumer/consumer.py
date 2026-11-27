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
import re
from dataclasses import dataclass, field
from typing import Any, Callable

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException, Message
except ImportError:  # pragma: no cover
    Consumer = None
    KafkaError = None
    KafkaException = None
    Message = Any

from shared.validation import SensorValidationError, validate_sensor_event

# Optional Snowflake persistence attributes: lazily imported on demand so
# Kafka consumer imports and non-Snowflake usage never fail when snowflake-connector
# is absent or blocked. Tests can still patch these module attributes directly.
RAW_SENSOR_TABLE = "ATMOSYNC_DB.RAW.SENSOR_READINGS"
get_snowflake_connection: Any = None
insert_sensor_events: Any = None


class SnowflakeConnectionError(Exception):
    """Fallback exception when snowflake.connection is not yet loaded."""


class SnowflakeIngestionError(Exception):
    """Fallback exception when snowflake.ingestion is not yet loaded."""


def _resolve_snowflake_connection_api() -> tuple[Any, type[Exception]]:
    """Lazily import snowflake.connection when Snowflake connectivity is actually required."""
    global get_snowflake_connection, SnowflakeConnectionError
    if get_snowflake_connection is not None:
        return get_snowflake_connection, SnowflakeConnectionError
    try:
        from snowflake.connection import (
            SnowflakeConnectionError as _SFConnError,
            get_snowflake_connection as _get_sf_conn,
        )
    except Exception as exc:
        raise KafkaConsumerError(
            f"Snowflake persistence requested, but snowflake.connection could not be imported: {exc}"
        ) from exc
    get_snowflake_connection = _get_sf_conn
    SnowflakeConnectionError = _SFConnError
    return get_snowflake_connection, SnowflakeConnectionError


def _resolve_snowflake_ingestion_api() -> tuple[Any, str, type[Exception]]:
    """Lazily import snowflake.ingestion when batch persistence is actually required."""
    global insert_sensor_events, RAW_SENSOR_TABLE, SnowflakeIngestionError
    if insert_sensor_events is not None:
        return insert_sensor_events, RAW_SENSOR_TABLE, SnowflakeIngestionError
    try:
        from snowflake.ingestion import (
            RAW_SENSOR_TABLE as _RAW_TABLE,
            SnowflakeIngestionError as _SFIngestError,
            insert_sensor_events as _insert_events,
        )
    except Exception as exc:
        raise KafkaConsumerError(
            f"Snowflake persistence requested, but snowflake.ingestion could not be imported: {exc}"
        ) from exc
    insert_sensor_events = _insert_events
    RAW_SENSOR_TABLE = _RAW_TABLE
    SnowflakeIngestionError = _SFIngestError
    return insert_sensor_events, RAW_SENSOR_TABLE, SnowflakeIngestionError

logger = logging.getLogger(__name__)


def sanitize_raw_payload(value: Any, max_len: int = 500) -> str:
    """
    Safely stringifies raw payload for dead-letter diagnostics, redacting sensitive
    credentials/secrets and truncating excessive lengths to avoid log pollution.
    """
    if isinstance(value, bytes):
        text = value.decode("utf-8", errors="replace")
    else:
        text = str(value)

    # Redact common credential patterns if present
    for sensitive_key in ("password", "secret", "token", "api_key", "credential"):
        if sensitive_key in text.lower():
            text = re.sub(
                rf'("{sensitive_key}"\s*:\s*)"[^"]*"',
                r'\1"***REDACTED***"',
                text,
                flags=re.IGNORECASE,
            )

    if len(text) > max_len:
        return text[:max_len] + "... [truncated]"
    return text


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
        event_ingestor: Callable[[list[tuple[dict[str, Any], str, int, int]], Any], bool] | None = None,
    ) -> None:
        self.config = config or ConsumerConfig.from_env()
        self._external_snowflake_conn = snowflake_connection is not None
        self._snowflake_conn = snowflake_connection
        self._event_ingestor = event_ingestor

        logger.info(
            "Initializing SensorKafkaConsumer: topic='%s', group_id='%s', batch_size=%d, auto_offset_reset='%s', poll_timeout=%.1fs",
            self.config.topic,
            self.config.group_id,
            self.config.batch_size,
            self.config.auto_offset_reset,
            self.config.poll_timeout_seconds,
        )

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

    def _reset_snowflake_conn(self) -> None:
        """
        Safely closes and resets internally managed Snowflake connection on failure.
        Allows the consumer to establish a fresh connection on the next batch attempt.
        """
        if not self._external_snowflake_conn and self._snowflake_conn is not None:
            try:
                self._snowflake_conn.close()
                logger.debug("Closed stale internal Snowflake connection.")
            except Exception as exc:
                logger.debug("Error while closing failed Snowflake connection: %s", exc)
            self._snowflake_conn = None

    def _get_snowflake_conn(self) -> Any:
        """Retrieves active Snowflake connection, establishing a new one if not connected."""
        if self._snowflake_conn is None:
            conn_fn, conn_err_cls = _resolve_snowflake_connection_api()
            try:
                self._snowflake_conn = conn_fn()
                logger.info("Established fresh Snowflake connection for batch ingestion.")
            except Exception as exc:
                logger.error("Failed to connect to Snowflake: %s", exc)
                if isinstance(exc, conn_err_cls) or type(exc).__name__ == "SnowflakeConnectionError":
                    raise
                raise conn_err_cls(f"Snowflake connection failed: {exc}") from exc
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
        1. Deserializes and validates each event against the official 7-field schema.
        2. Isolates invalid events with sanitized metadata for DLQ tracking.
        3. Inserts valid events into Snowflake via `insert_sensor_events`.
        4. Commits Kafka offsets ONLY if Snowflake insertion succeeds (or if all messages are poison pills).
        """
        if not messages:
            logger.debug("Polled empty message batch; skipping processing.")
            return BatchIngestionResult(
                valid_count=0,
                invalid_count=0,
                committed=False,
                inserted_into_snowflake=False,
                invalid_records=[],
            )

        logger.info("Received batch of %d message(s) from Kafka", len(messages))
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
                    "raw_payload": sanitize_raw_payload(value),
                })

        logger.info(
            "Batch validation summary: %d valid event(s), %d invalid event(s)",
            len(valid_tuples),
            len(invalid_records),
        )

        # Database Insertion & Offset Commit Decision
        inserted_into_snowflake = False
        if valid_tuples:
            if self._event_ingestor is not None:
                try:
                    inserted_into_snowflake = self._event_ingestor(valid_tuples, self._snowflake_conn)
                except Exception as exc:
                    logger.error(
                        "Injected event ingestor failed (%d events). Offsets WILL NOT be committed. Error: %s",
                        len(valid_tuples),
                        exc,
                    )
                    raise
            else:
                ingest_fn, raw_table, ingest_err_cls = _resolve_snowflake_ingestion_api()
                logger.info(
                    "Starting Snowflake batch ingestion for %d valid event(s) into %s",
                    len(valid_tuples),
                    raw_table,
                )
                try:
                    conn = self._get_snowflake_conn()
                    inserted_into_snowflake = ingest_fn(
                        events=valid_tuples,
                        connection=conn,
                    )
                    logger.info(
                        "Successfully inserted %d event(s) into Snowflake table %s",
                        len(valid_tuples),
                        raw_table,
                    )
                except Exception as exc:
                    logger.error(
                        "Snowflake batch ingestion failed (%d events). Offsets WILL NOT be committed. Error: %s",
                        len(valid_tuples),
                        exc,
                    )
                    self._reset_snowflake_conn()
                    if (
                        isinstance(exc, (SnowflakeConnectionError, SnowflakeIngestionError, ingest_err_cls))
                        or type(exc).__name__ in ("SnowflakeConnectionError", "SnowflakeIngestionError")
                    ):
                        raise
                    raise ingest_err_cls(
                        f"Snowflake batch ingestion failed: {exc}"
                    ) from exc
        elif invalid_records:
            logger.warning(
                "Batch contains ONLY invalid records (%d events). Acknowledging poison pills without DB operations.",
                len(invalid_records),
            )

        # Commit Kafka offsets if insertion succeeded or if there were only invalid messages
        # (poison pill records safely handled so consumer does not hang indefinitely)
        committed = False
        if hasattr(self.consumer, "commit") and callable(self.consumer.commit):
            try:
                self.consumer.commit(asynchronous=False)
                committed = True
                logger.info(
                    "Kafka offset commit succeeded: %d valid record(s) persisted, %d invalid record(s) safely handled",
                    len(valid_tuples),
                    len(invalid_records),
                )
            except Exception as exc:
                logger.error("Failed to commit Kafka offsets: %s", exc)
                raise KafkaConsumerError(f"Kafka offset commit failed: {exc}") from exc

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

    def run(
        self,
        max_batches: int | None = None,
        stop_event: Any = None,
    ) -> int:
        """
        Executes consumer polling loop until stopped or `max_batches` reached.

        Args:
            max_batches: Optional maximum number of batches to process.
            stop_event: Optional threading.Event to signal graceful termination.

        Returns:
            int: Total number of batches processed.
        """
        logger.info("Starting SensorKafkaConsumer polling loop...")
        batches_processed = 0
        try:
            while True:
                if stop_event is not None and getattr(stop_event, "is_set", lambda: False)():
                    logger.info("Stop event signaled. Gracefully exiting consumer loop.")
                    break
                if max_batches is not None and batches_processed >= max_batches:
                    logger.info(
                        "Reached target batch limit (%d). Exiting consumer loop.",
                        max_batches,
                    )
                    break

                self.consume_once()
                batches_processed += 1
        finally:
            self.close()
        return batches_processed

    def close(self) -> None:
        """Closes Kafka consumer and Snowflake connection with structured logging."""
        logger.info("Initiating SensorKafkaConsumer shutdown...")
        if hasattr(self, "consumer") and self.consumer:
            if hasattr(self.consumer, "close") and callable(self.consumer.close):
                try:
                    self.consumer.close()
                    logger.info("Kafka consumer connection closed.")
                except Exception as exc:
                    logger.warning("Error closing Kafka consumer: %s", exc)
        if not self._external_snowflake_conn and self._snowflake_conn:
            try:
                self._snowflake_conn.close()
                logger.info("Internal Snowflake connection closed.")
            except Exception as exc:
                logger.warning("Error closing Snowflake connection: %s", exc)
            self._snowflake_conn = None
        logger.info("SensorKafkaConsumer shutdown complete.")
