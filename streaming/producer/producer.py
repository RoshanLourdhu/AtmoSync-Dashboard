"""
Sensor Kafka Producer Service for AtmoSync Dashboard.

Orchestrates streaming publishing of IoT container telemetry to Kafka
topic 'container-sensor-data'.

Enforces:
1. Strict runtime schema validation via `shared.validation.validate_sensor_event`.
2. Safe JSON payload serialization.
3. Partitioning by stable container identifier (`container_id`) as the Kafka message key.
4. Robust delivery callback handling and error detection.
5. Deterministic flushing and resource cleanup.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Any, Callable

try:
    from confluent_kafka import KafkaError, KafkaException, Producer
except ImportError:  # pragma: no cover
    Producer = None
    KafkaError = None
    KafkaException = None

from shared.validation import SensorValidationError, validate_sensor_event

logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Base exception for Kafka producer operations."""


@dataclass
class ProducerConfig:
    """Configuration parameters for SensorKafkaProducer."""

    bootstrap_servers: str = "localhost:9092"
    topic: str = "container-sensor-data"
    client_id: str = "atmosync-sensor-producer"
    acks: str = "all"
    validate_before_send: bool = True

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
            validate_before_send=os.getenv("KAFKA_PRODUCER_VALIDATE", "true").lower() in ("true", "1", "yes"),
        )


class SensorKafkaProducer:
    """
    Publishes validated IoT container sensor events to Kafka topics.
    """

    def __init__(
        self,
        config: ProducerConfig | None = None,
        *,
        kafka_producer: Any = None,
    ) -> None:
        self.config = config or ProducerConfig.from_env()

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
                    "Initialized Kafka producer (client '%s', servers '%s')",
                    self.config.client_id,
                    self.config.bootstrap_servers,
                )
            except Exception as exc:
                raise KafkaProducerError(
                    f"Failed to initialize Kafka producer: {exc}"
                ) from exc

    def _default_delivery_callback(self, err: Any, msg: Any) -> None:
        """Default delivery report callback invoked on broker acknowledgement."""
        if err is not None:
            logger.error("Kafka delivery failed for record: %s", err)
        else:
            logger.debug(
                "Message delivered to topic '%s' [partition %s] at offset %s",
                getattr(msg, "topic", lambda: "unknown")(),
                getattr(msg, "partition", lambda: "unknown")(),
                getattr(msg, "offset", lambda: "unknown")(),
            )

    def produce_event(
        self,
        event: dict[str, Any],
        *,
        topic: str | None = None,
        key: str | bytes | None = None,
        on_delivery: Callable[[Any, Any], None] | None = None,
    ) -> bool:
        """
        Validates, serializes, and publishes a single sensor event to Kafka.

        Args:
            event: Sensor reading dictionary conforming to official schema.
            topic: Override target Kafka topic (defaults to config.topic).
            key: Message key for partition routing (defaults to event["container_id"]).
            on_delivery: Optional custom delivery callback.

        Returns:
            True if successfully queued to the producer buffer.

        Raises:
            SensorValidationError: If the payload violates the official schema.
            KafkaProducerError: If the message cannot be queued.
        """
        if self.config.validate_before_send:
            validate_sensor_event(event)

        target_topic = topic or self.config.topic

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
            raise KafkaProducerError(f"Failed to serialize sensor event to JSON: {exc}") from exc

        callback = on_delivery or self._default_delivery_callback

        try:
            self.producer.produce(
                topic=target_topic,
                key=key_bytes,
                value=serialized_payload,
                on_delivery=callback,
            )
            # Service delivery callbacks
            self.producer.poll(0)
            return True
        except Exception as exc:
            logger.error("Failed to queue message to Kafka: %s", exc)
            raise KafkaProducerError(f"Failed to produce message: {exc}") from exc

    def produce_batch(
        self,
        events: list[dict[str, Any]],
        *,
        topic: str | None = None,
        flush_timeout: float = 10.0,
    ) -> int:
        """
        Validates, queues, and flushes a batch of sensor events.

        Returns:
            Number of successfully queued messages.
        """
        queued_count = 0
        for event in events:
            if self.produce_event(event, topic=topic):
                queued_count += 1

        self.flush(timeout=flush_timeout)
        return queued_count

    def flush(self, timeout: float = 10.0) -> int:
        """
        Flushes pending producer buffer to brokers.

        Returns:
            Number of messages still pending delivery.
        """
        if hasattr(self.producer, "flush") and callable(self.producer.flush):
            return self.producer.flush(timeout)
        return 0

    def close(self, timeout: float = 10.0) -> None:
        """Flushes remaining messages and closes producer resources."""
        self.flush(timeout=timeout)
        logger.info("Kafka producer closed cleanly.")
