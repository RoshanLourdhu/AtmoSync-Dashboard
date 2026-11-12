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


def insert_sensor_events(
    events: list[dict[str, Any] | tuple[dict[str, Any], str, int | None, int | None]],
    topic: str = DEFAULT_KAFKA_TOPIC,
    partition: int | None = None,
    offset: int | None = None,
    connection: SnowflakeConnection | None = None,
    close_connection: bool | None = None,
) -> bool:
    """
    Validate a batch of sensor events and insert them into the Snowflake RAW table
    in a single atomic transaction.

    Target Table: ATMOSYNC_DB.RAW.SENSOR_READINGS
    Target Column: EVENT_PAYLOAD (VARIANT)

    Args:
        events: List of sensor event dictionaries (or tuples of (event_dict, topic, partition, offset)).
        topic: Default source Kafka topic name if not specified per-item (default: 'container-sensor-data').
        partition: Default Kafka partition ID if not specified per-item.
        offset: Default Kafka message offset if not specified per-item.
        connection: Optional active Snowflake connection. If None, a new connection is
                    opened via get_snowflake_connection() and closed upon completion.
        close_connection: Whether to close the connection in the finally block.
                          Defaults to True if connection was internally created, False if injected.

    Returns:
        bool: True on successful batch insertion and transaction commit.

    Raises:
        SensorValidationError: If any event payload in the batch fails contract validation.
        SnowflakeConnectionError: If connection establishment fails.
        SnowflakeIngestionError: If SQL execution, parameter binding, or commit fails.
    """
    if not isinstance(events, list):
        raise SnowflakeIngestionError(f"Expected events to be a list, got {type(events).__name__}.")

    # Empty batch is a valid no-op
    if not events:
        return True

    # 1. Enforce strict validation and serialization for ALL events before touching the database
    params_list: list[tuple[str, str, int | None, int | None]] = []
    for idx, item in enumerate(events):
        if isinstance(item, tuple) and len(item) == 4:
            ev, ev_topic, ev_partition, ev_offset = item
        elif isinstance(item, dict):
            ev, ev_topic, ev_partition, ev_offset = item, topic, partition, offset
        else:
            raise SnowflakeIngestionError(
                f"Event at index {idx} must be a dictionary or a (dict, topic, partition, offset) tuple."
            )

        # Validate against JSON schema - raises SensorValidationError if invalid
        validate_sensor_event(ev)

        try:
            payload_json = json.dumps(ev, ensure_ascii=False)
        except (TypeError, ValueError) as err:
            raise SnowflakeIngestionError(
                f"Failed to serialize sensor event at index {idx} to JSON: {err}"
            ) from err

        params_list.append((payload_json, ev_topic, ev_partition, ev_offset))

    # 2. Construct parameterized batch SQL statement
    sql = (
        f"INSERT INTO {RAW_SENSOR_TABLE} ("
        "EVENT_PAYLOAD, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET"
        ") SELECT PARSE_JSON(%s), %s, %s, %s"
    )

    # 3. Execute batch in a single atomic transaction
    should_close = close_connection if close_connection is not None else (connection is None)
    conn = connection if connection is not None else get_snowflake_connection()
    cursor = None

    try:
        cursor = conn.cursor()
        cursor.executemany(sql, params_list)
        conn.commit()
        return True
    except (SnowflakeConnectorError, Exception) as err:
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        raise SnowflakeIngestionError(
            f"Failed to batch insert {len(params_list)} sensor events into {RAW_SENSOR_TABLE}: {err}"
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
