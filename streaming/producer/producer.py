"""
Sensor Kafka Producer Service for AtmoSync Dashboard.

Orchestrates streaming publishing of IoT container telemetry to Kafka
topic 'container-sensor-data'.

Enforces:
1. Strict runtime schema validation via `shared.validation.validate_sensor_event`.
2. Safe JSON payload serialization.
3. Partitioning by stable container identifier (`container_id`) as the Kafka message key.
4. Broker delivery confirmation callbacks with structured delivery/failure metrics.
5. Bounded retry handling for transient buffer/queue errors without event duplication.
6. Configurable flushing with undelivered pending message reporting.
7. Graceful shutdown halting event generation, flushing pending records, and closing cleanly.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

try:
    from confluent_kafka import KafkaError, KafkaException, Producer
except ImportError:  # pragma: no cover
    Producer = None
    KafkaError = None
    KafkaException = None

from shared.validation import SensorValidationError, validate_sensor_event

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "container-sensor-data"
VALID_ACKS = {"0", "1", "all", 0, 1}
TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")


class KafkaProducerError(Exception):
    """Base exception for Kafka producer operations."""


class ProducerConfigError(KafkaProducerError, ValueError):
    """Raised when Kafka producer configuration is missing or invalid."""


def validate_bootstrap_servers(
    bootstrap_servers: str | list[str] | tuple[str, ...] | None,
) -> list[str]:
    """
    Validate bootstrap_servers string or sequence of 'host:port' entries.

    Args:
        bootstrap_servers: Comma-separated string or list/tuple of broker addresses.

    Returns:
        list[str]: Cleaned list of valid 'host:port' broker strings.

    Raises:
        ProducerConfigError: If bootstrap_servers is missing, empty, malformed, or port out of range.
    """
    if bootstrap_servers is None:
        raise ProducerConfigError(
            "KAFKA_BOOTSTRAP_SERVERS is missing. Expected 'host:port' (e.g., 'localhost:9092')."
        )

    if isinstance(bootstrap_servers, str):
        servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    elif isinstance(bootstrap_servers, (list, tuple)):
        servers = [str(s).strip() for s in bootstrap_servers if str(s).strip()]
    else:
        raise ProducerConfigError(
            f"Invalid bootstrap_servers type '{type(bootstrap_servers).__name__}'. Expected string or list."
        )

    if not servers:
        raise ProducerConfigError(
            "KAFKA_BOOTSTRAP_SERVERS cannot be empty. Expected 'host:port' (e.g., 'localhost:9092')."
        )

    for entry in servers:
        if ":" not in entry:
            raise ProducerConfigError(
                f"Invalid bootstrap server '{entry}': missing port. Expected 'host:port'."
            )
        host, port_str = entry.rsplit(":", 1)
        if not host:
            raise ProducerConfigError(
                f"Invalid bootstrap server '{entry}': host cannot be empty."
            )
        if not port_str.isdigit() or not (1 <= int(port_str) <= 65535):
            raise ProducerConfigError(
                f"Invalid port '{port_str}' in bootstrap server '{entry}'. Port must be between 1 and 65535."
            )

    return servers


def validate_topic(topic: str | None) -> str:
    """
    Validate Kafka topic name against standard Kafka naming constraints.

    Constraints:
        - Must be non-empty string.
        - Length between 1 and 249 characters.
        - Cannot be '.' or '..'.
        - Allowed characters: alphanumeric, '.', '_', '-'.

    Args:
        topic: Topic name to validate.

    Returns:
        str: Validated and stripped topic string.

    Raises:
        ProducerConfigError: If topic is invalid or violates naming constraints.
    """
    if not isinstance(topic, str) or not topic.strip():
        raise ProducerConfigError(
            "KAFKA_TOPIC is missing or empty. Expected a non-empty topic name (e.g., 'container-sensor-data')."
        )
    cleaned = topic.strip()
    if cleaned in (".", "..") or not TOPIC_PATTERN.match(cleaned):
        raise ProducerConfigError(
            f"Invalid KAFKA_TOPIC '{cleaned}'. Use 1-249 alphanumeric characters, '.', '_', or '-'."
        )
    return cleaned


@dataclass
class ProducerConfig:
    """Configuration parameters for SensorKafkaProducer."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "container-sensor-data"
    client_id: str = "atmosync-sensor-producer"
    acks: str = "all"
    validate_before_send: bool = True
    flush_timeout_seconds: float = 10.0
    max_retries: int = 3
    retry_backoff_seconds: float = 0.2

    def validate(self) -> None:
        """
        Validate and enforce constraints on producer configuration parameters.

        Raises:
            ProducerConfigError: If any configuration value violates constraints.
        """
        validate_bootstrap_servers(self.bootstrap_servers)
        validate_topic(self.topic)
        if self.acks not in VALID_ACKS:
            raise ProducerConfigError(
                f"Invalid acks value '{self.acks}'. Allowed values: 0, 1, 'all'."
            )
        if self.max_retries < 0 or self.max_retries > 20:
            raise ProducerConfigError(
                f"Invalid max_retries={self.max_retries}. Must be between 0 and 20."
            )
        if self.flush_timeout_seconds <= 0:
            raise ProducerConfigError(
                f"Invalid flush_timeout_seconds={self.flush_timeout_seconds}. Must be positive."
            )
        if self.retry_backoff_seconds < 0:
            raise ProducerConfigError(
                f"Invalid retry_backoff_seconds={self.retry_backoff_seconds}. Must be non-negative."
            )

    @classmethod
    def from_env(cls) -> ProducerConfig:
        """Construct configuration from environment variables."""
        return cls(
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            topic=(
                os.getenv("KAFKA_TOPIC_SENSOR_DATA")
                or os.getenv("KAFKA_TOPIC")
                or "container-sensor-data"
            ),
            client_id=os.getenv("KAFKA_PRODUCER_CLIENT_ID", "atmosync-sensor-producer"),
            acks=os.getenv("KAFKA_PRODUCER_ACKS", "all"),
            validate_before_send=os.getenv("KAFKA_PRODUCER_VALIDATE", "true").lower()
            in ("true", "1", "yes"),
            flush_timeout_seconds=float(
                os.getenv("KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS", "10.0")
            ),
            max_retries=int(os.getenv("KAFKA_PRODUCER_MAX_RETRIES", "3")),
            retry_backoff_seconds=float(
                os.getenv("KAFKA_PRODUCER_RETRY_BACKOFF_SECONDS", "0.2")
            ),
        )


@dataclass
class ProducerStats:
    """Operational delivery statistics tracked by SensorKafkaProducer."""

    produced: int = 0
    delivered: int = 0
    delivery_failed: int = 0
    produce_failed: int = 0
    validation_errors: int = 0
    pending: int = 0
    retries: int = 0
    delivery_errors: list[str] = field(default_factory=list)

    @property
    def failed(self) -> int:
        """Total failed messages across local produce failures and broker delivery failures."""
        return self.delivery_failed + self.produce_failed

    @property
    def in_flight(self) -> int:
        """Messages queued to local buffer not yet confirmed delivered or failed by broker."""
        return max(0, self.produced - self.delivered - self.delivery_failed)

    def to_dict(self) -> dict[str, int]:
        """Return a sanitized summary dictionary of numeric producer metrics."""
        return {
            "produced": self.produced,
            "delivered": self.delivered,
            "delivery_failed": self.delivery_failed,
            "produce_failed": self.produce_failed,
            "failed": self.failed,
            "validation_errors": self.validation_errors,
            "pending": self.pending,
            "in_flight": self.in_flight,
            "retries": self.retries,
        }


class SensorKafkaProducer:
    """
    Publishes validated IoT container sensor events to Kafka topics with
    broker delivery tracking, bounded retry, safe flushing, and graceful shutdown.
    """

    def __init__(
        self,
        config: ProducerConfig | None = None,
        *,
        kafka_producer: Any = None,
    ) -> None:
        self.config = config or ProducerConfig.from_env()
        self.config.validate()
        self.stats = ProducerStats()
        self._closed = False

        logger.info(
            "Initializing SensorKafkaProducer: client_id='%s', topic='%s', acks='%s', "
            "validate_before_send=%s, flush_timeout=%.1fs, max_retries=%d",
            self.config.client_id,
            self.config.topic,
            self.config.acks,
            self.config.validate_before_send,
            self.config.flush_timeout_seconds,
            self.config.max_retries,
        )

        if kafka_producer is not None:
            self.producer = kafka_producer
        else:
            if Producer is None:
                raise KafkaProducerError(
                    "confluent-kafka is not installed. Install via pip install confluent-kafka"
                )
            producer_params = {
                "bootstrap.servers": self.config.bootstrap_servers,
                "client.id": self.config.client_id,
                "acks": self.config.acks,
            }
            try:
                self.producer = Producer(producer_params)
                logger.info(
                    "Connected Kafka producer (client '%s', servers '%s')",
                    self.config.client_id,
                    self.config.bootstrap_servers,
                )
            except Exception as exc:
                raise KafkaProducerError(
                    f"Failed to initialize Kafka producer: {exc}"
                ) from exc

    @property
    def is_closed(self) -> bool:
        """Returns True if graceful shutdown has been initiated or completed."""
        return self._closed

    def get_stats(self) -> dict[str, int]:
        """Returns a sanitized dictionary snapshot of current delivery statistics."""
        return self.stats.to_dict()

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        """Determines whether a produce exception is transient and safe to retry."""
        if isinstance(exc, (BufferError, TimeoutError, ConnectionError)):
            return True
        if KafkaException is not None and isinstance(exc, KafkaException):
            kafka_err = exc.args[0] if exc.args else None
            if hasattr(kafka_err, "retriable") and callable(kafka_err.retriable):
                return bool(kafka_err.retriable())
        return False

    def _build_delivery_callback(
        self,
        user_callback: Callable[[Any, Any], None] | None = None,
        container_id: str = "unknown",
    ) -> Callable[[Any, Any], None]:
        """
        Constructs a broker delivery report callback that updates operational metrics,
        emits structured logs, and invokes an optional caller-provided callback.
        """
        def _callback(err: Any, msg: Any) -> None:
            if err is not None:
                self.stats.delivery_failed += 1
                error_detail = f"Delivery failed for container '{container_id}': {err}"
                self.stats.delivery_errors.append(error_detail)
                logger.error(
                    "Kafka broker delivery failed (container='%s', delivered=%d, failed=%d): %s",
                    container_id,
                    self.stats.delivered,
                    self.stats.failed,
                    err,
                )
            else:
                self.stats.delivered += 1
                topic = msg.topic() if callable(getattr(msg, "topic", None)) else "unknown"
                partition = msg.partition() if callable(getattr(msg, "partition", None)) else "unknown"
                offset = msg.offset() if callable(getattr(msg, "offset", None)) else "unknown"
                logger.info(
                    "Kafka message delivered: topic='%s', partition=%s, offset=%s, container='%s'",
                    topic,
                    partition,
                    offset,
                    container_id,
                )

            if user_callback is not None:
                try:
                    user_callback(err, msg)
                except Exception as cb_exc:
                    logger.warning("Custom delivery callback raised exception: %s", cb_exc)

        return _callback

    def _default_delivery_callback(self, err: Any, msg: Any) -> None:
        """Default delivery report callback invoked on broker acknowledgement."""
        self._build_delivery_callback()(err, msg)

    def produce_event(
        self,
        event: dict[str, Any],
        *,
        topic: str | None = None,
        key: str | bytes | None = None,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> bool:
        """
        Validates, serializes, and queues a single sensor event to Kafka.

        Note: Returning True confirms the event was queued to the local producer
        buffer. Actual broker confirmation is tracked asynchronously via the
        delivery callback in `self.stats.delivered` / `self.stats.delivery_failed`.

        Args:
            event: Sensor reading dictionary conforming to official schema.
            topic: Override target Kafka topic (defaults to config.topic).
            key: Message key for partition routing (defaults to event["container_id"]).
            on_delivery: Optional custom delivery callback.

        Returns:
            True if successfully queued to the producer buffer.

        Raises:
            SensorValidationError: If the payload violates the official schema.
            KafkaProducerError: If the producer is closed or the message cannot be queued.
        """
        if self._closed:
            raise KafkaProducerError(
                "Cannot produce event: Kafka producer is closed or shutting down."
            )

        if self.config.validate_before_send:
            try:
                validate_sensor_event(event)
            except SensorValidationError:
                self.stats.validation_errors += 1
                logger.warning(
                    "Rejected sensor event failing schema validation (validation_errors=%d).",
                    self.stats.validation_errors,
                )
                raise

        target_topic = topic or self.config.topic
        container_id = str(event.get("container_id", "unknown"))

        # Derive partition key from container_id for stable partition ordering
        if key is None:
            raw_key = str(event.get("container_id", ""))
            key_bytes = raw_key.encode("utf-8")
        elif isinstance(key, str):
            key_bytes = key.encode("utf-8")
        else:
            key_bytes = key

        # Serialize payload to JSON
        try:
            serialized_payload = json.dumps(event).encode("utf-8")
        except (TypeError, ValueError) as exc:
            self.stats.produce_failed += 1
            raise KafkaProducerError(f"Failed to serialize sensor event to JSON: {exc}") from exc

        callback = self._build_delivery_callback(
            user_callback=on_delivery,
            container_id=container_id,
        )

        max_attempts = max(1, self.config.max_retries + 1)
        last_exc: Exception | None = None
        attempts_made = 0

        for attempt in range(1, max_attempts + 1):
            attempts_made = attempt
            try:
                self.producer.produce(
                    topic=target_topic,
                    key=key_bytes,
                    value=serialized_payload,
                    on_delivery=callback,
                )
                self.stats.produced += 1
                # Service delivery callbacks without blocking
                self.producer.poll(0)
                return True
            except Exception as exc:
                last_exc = exc
                if attempt < max_attempts and self._is_retryable_error(exc):
                    self.stats.retries += 1
                    logger.warning(
                        "Transient Kafka produce error (attempt %d/%d, container='%s'): %s. "
                        "Polling buffer before retry.",
                        attempt,
                        max_attempts,
                        container_id,
                        exc,
                    )
                    if hasattr(self.producer, "poll") and callable(self.producer.poll):
                        self.producer.poll(self.config.retry_backoff_seconds)
                else:
                    break

        self.stats.produce_failed += 1
        logger.error(
            "Failed to queue message to Kafka after %d attempt(s) (container='%s'): %s",
            attempts_made,
            container_id,
            last_exc,
        )
        raise KafkaProducerError(f"Failed to produce message: {last_exc}") from last_exc

    def produce_batch(
        self,
        events: list[dict[str, Any]],
        *,
        topic: str | None = None,
        flush_timeout: float | None = None,
        continue_on_error: bool = True,
    ) -> int:
        """
        Validates, queues, and flushes a batch of sensor events.

        When `continue_on_error=True` (default), individual validation or produce
        failures are logged and isolated so remaining valid events in the batch
        are still published and flushed.

        Returns:
            Number of successfully queued messages.
        """
        if self._closed:
            raise KafkaProducerError(
                "Cannot produce batch: Kafka producer is closed or shutting down."
            )

        logger.info("Starting Kafka batch production for %d event(s)", len(events))
        queued_count = 0

        for idx, event in enumerate(events):
            if self._closed:
                logger.warning(
                    "Shutdown requested mid-batch at index %d; halting further event queuing.",
                    idx,
                )
                break
            try:
                if self.produce_event(event, topic=topic):
                    queued_count += 1
            except (SensorValidationError, KafkaProducerError) as exc:
                if not continue_on_error:
                    self.flush(timeout=flush_timeout)
                    raise
                logger.error(
                    "Isolated error for batch event index %d (continuing batch): %s",
                    idx,
                    exc,
                )

        remaining = self.flush(timeout=flush_timeout)
        logger.info(
            "Batch production complete: queued=%d, delivered=%d, failed=%d, pending=%d",
            queued_count,
            self.stats.delivered,
            self.stats.failed,
            remaining,
        )
        return queued_count

    def flush(self, timeout: float | None = None) -> int:
        """
        Flushes pending producer buffer to brokers within the configured timeout.

        Returns:
            Number of messages still pending delivery after timeout.
        """
        effective_timeout = (
            timeout if timeout is not None else self.config.flush_timeout_seconds
        )
        if hasattr(self.producer, "flush") and callable(self.producer.flush):
            raw_remaining = self.producer.flush(effective_timeout)
            remaining = raw_remaining if isinstance(raw_remaining, int) else 0
        else:
            remaining = 0

        self.stats.pending = remaining
        if remaining > 0:
            logger.warning(
                "Kafka producer flush timed out after %.1fs with %d message(s) still undelivered/pending "
                "(produced=%d, delivered=%d, failed=%d).",
                effective_timeout,
                remaining,
                self.stats.produced,
                self.stats.delivered,
                self.stats.failed,
            )
        else:
            logger.info(
                "Kafka producer flush completed cleanly: pending=0 (produced=%d, delivered=%d, failed=%d).",
                self.stats.produced,
                self.stats.delivered,
                self.stats.failed,
            )
        return remaining

    def request_shutdown(self) -> None:
        """Signals the producer to stop accepting new events."""
        if not self._closed:
            logger.info(
                "Shutdown requested for SensorKafkaProducer; halting new event production."
            )
            self._closed = True

    def close(self, timeout: float | None = None) -> int:
        """
        Gracefully shuts down producer resources: stops accepting new events,
        flushes pending messages, reports any undelivered records, and logs final stats.

        Returns:
            Number of messages still undelivered after final flush.
        """
        logger.info("Initiating SensorKafkaProducer graceful shutdown...")
        self._closed = True
        remaining = self.flush(timeout=timeout)
        if remaining > 0:
            logger.error(
                "Kafka producer shutdown completed with %d undelivered pending message(s).",
                remaining,
            )
        logger.info(
            "Kafka producer closed cleanly. Final stats: produced=%d, delivered=%d, "
            "failed=%d, validation_errors=%d, pending=%d, retries=%d",
            self.stats.produced,
            self.stats.delivered,
            self.stats.failed,
            self.stats.validation_errors,
            remaining,
            self.stats.retries,
        )
        return remaining

    def stream_simulation(
        self,
        simulator: Any,
        *,
        max_cycles: int | None = None,
        interval_seconds: float = 0.0,
        stop_event: Any = None,
        topic: str | None = None,
        flush_timeout: float | None = None,
    ) -> ProducerStats:
        """
        Continuously generates and publishes simulated sensor batches with graceful shutdown.

        Halts new event generation immediately upon `stop_event`, `request_shutdown()`,
        reaching `max_cycles`, or `KeyboardInterrupt`. Always flushes pending messages
        and closes producer resources in a `finally` block.
        """
        logger.info(
            "Starting sensor simulation stream (max_cycles=%s, interval=%.2fs, topic='%s')",
            max_cycles,
            interval_seconds,
            topic or self.config.topic,
        )
        cycles = 0
        try:
            while True:
                if self._closed or (
                    stop_event is not None
                    and getattr(stop_event, "is_set", lambda: False)()
                ):
                    logger.info("Termination signal received. Stopping event generation.")
                    break
                if max_cycles is not None and cycles >= max_cycles:
                    logger.info(
                        "Reached max_cycles (%d). Stopping simulation stream.",
                        max_cycles,
                    )
                    break

                try:
                    events = simulator.generate_batch()
                except Exception as sim_exc:
                    logger.error(
                        "Simulator batch generation error on cycle %d: %s",
                        cycles + 1,
                        sim_exc,
                    )
                    cycles += 1
                    continue

                self.produce_batch(
                    events,
                    topic=topic,
                    flush_timeout=flush_timeout,
                    continue_on_error=True,
                )
                cycles += 1

                if interval_seconds > 0 and not self._closed:
                    if stop_event is not None and hasattr(stop_event, "wait"):
                        if stop_event.wait(timeout=interval_seconds):
                            logger.info("Stop event triggered during sleep interval.")
                            break
                    else:
                        time.sleep(interval_seconds)
        except KeyboardInterrupt:
            logger.info("KeyboardInterrupt received. Initiating graceful producer shutdown.")
        finally:
            self.close(timeout=flush_timeout)

        return self.stats
