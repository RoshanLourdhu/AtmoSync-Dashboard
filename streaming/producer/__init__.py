"""
AtmoSync Kafka Producer Module.

Provides Kafka producer orchestration and schema validation for IoT sensor telemetry.
"""

from .producer import (
    KafkaProducerError,
    ProducerConfig,
    ProducerConfigError,
    ProducerDeliveryError,
    ProducerStats,
    SensorKafkaProducer,
    validate_bootstrap_servers,
    validate_topic,
)

__all__ = [
    "KafkaProducerError",
    "ProducerConfig",
    "ProducerConfigError",
    "ProducerDeliveryError",
    "ProducerStats",
    "SensorKafkaProducer",
    "validate_bootstrap_servers",
    "validate_topic",
]
