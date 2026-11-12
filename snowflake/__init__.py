"""
AtmoSync Snowflake Data Warehousing & Ingestion Package.

Provides database connectivity, DDL management, and event ingestion utilities.
"""
import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)

from snowflake.connection import (
    SnowflakeConnectionError,
    get_snowflake_config,
    get_snowflake_connection,
)
from snowflake.ingestion import (
    DEFAULT_KAFKA_TOPIC,
    RAW_SENSOR_TABLE,
    SnowflakeIngestionError,
    insert_sensor_event,
    insert_sensor_events,
)

__all__ = [
    "DEFAULT_KAFKA_TOPIC",
    "RAW_SENSOR_TABLE",
    "SnowflakeConnectionError",
    "SnowflakeIngestionError",
    "get_snowflake_config",
    "get_snowflake_connection",
    "insert_sensor_event",
    "insert_sensor_events",
]
