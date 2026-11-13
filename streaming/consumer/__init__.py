"""
AtmoSync Kafka Consumer Module.

Provides consumer orchestration connecting Kafka streaming topics
to Snowflake RAW batch ingestion with schema validation and safe error handling.
"""

from .consumer import (
    BatchIngestionResult,
    ConsumerConfig,
    KafkaConsumerError,
    SensorKafkaConsumer,
)

__all__ = [
    "BatchIngestionResult",
    "ConsumerConfig",
    "KafkaConsumerError",
    "SensorKafkaConsumer",
]
