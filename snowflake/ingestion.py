"""
Snowflake Ingestion Engine for IoT Sensor Readings.

Validates raw IoT events against the official JSON Schema and persists them
into the ATMOSYNC_DB.RAW.SENSOR_READINGS landing table in Snowflake.
"""
from __future__ import annotations

import json
from typing import Any

from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeConnectorError

from shared.validation.sensor_validator import SensorValidationError, validate_sensor_event
from snowflake.connection import SnowflakeConnectionError, get_snowflake_connection

RAW_SENSOR_TABLE = "ATMOSYNC_DB.RAW.SENSOR_READINGS"
DEFAULT_KAFKA_TOPIC = "container-sensor-data"


class SnowflakeIngestionError(Exception):
    """Raised when an error occurs during sensor event serialization or database insertion."""


def insert_sensor_event(
    event: dict[str, Any],
    topic: str = DEFAULT_KAFKA_TOPIC,
    partition: int | None = None,
    offset: int | None = None,
    connection: SnowflakeConnection | None = None,
    close_connection: bool | None = None,
) -> bool:
    """
    Validate a sensor event and insert it into the Snowflake RAW table.

    Target Table: ATMOSYNC_DB.RAW.SENSOR_READINGS
    Target Column: EVENT_PAYLOAD (VARIANT)

    Args:
        event: Dictionary representing the sensor event matching sensor_reading.schema.json.
        topic: Source Kafka topic name (default: 'container-sensor-data').
        partition: Source Kafka partition ID, if available.
        offset: Source Kafka message offset within the partition, if available.
        connection: Optional active Snowflake connection. If None, a new connection is
                    opened via get_snowflake_connection() and closed upon completion.
        close_connection: Whether to close the connection in the finally block.
                          Defaults to True if connection was internally created, False if injected.

    Returns:
        bool: True on successful insertion and transaction commit.

    Raises:
        SensorValidationError: If the event payload fails contract validation.
        SnowflakeConnectionError: If connection establishment fails.
        SnowflakeIngestionError: If SQL execution, parameter binding, or commit fails.
    """
    # 1. Enforce strict schema validation before attempting database operations
    validate_sensor_event(event)

    # 2. Serialize event dictionary safely into a JSON string
    try:
        payload_json = json.dumps(event, ensure_ascii=False)
    except (TypeError, ValueError) as err:
        raise SnowflakeIngestionError(f"Failed to serialize sensor event payload to JSON: {err}") from err

    # 3. Construct parameterized SQL statement targeting the RAW table
    sql = (
        f"INSERT INTO {RAW_SENSOR_TABLE} ("
        "EVENT_PAYLOAD, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET"
        ") SELECT PARSE_JSON(%s), %s, %s, %s"
    )
    params = (payload_json, topic, partition, offset)

    # 4. Determine connection lifecycle ownership
    should_close = close_connection if close_connection is not None else (connection is None)
    conn = connection if connection is not None else get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()
        return True
    except (SnowflakeConnectorError, Exception) as err:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise SnowflakeIngestionError(
            f"Failed to insert sensor reading into {RAW_SENSOR_TABLE}: {err}"
        ) from err
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception:
                pass
        if should_close and conn is not None:
            try:
                conn.close()
            except Exception:
                pass
