"""
Automated Unit Tests for Snowflake Ingestion Module.

Tests connection management, event validation integration, SQL parameterization,
transaction control, cleanup handling, and Kafka metadata routing using mocks.
Does not require a live Snowflake instance.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from snowflake.connector.errors import ProgrammingError

from shared.validation.sensor_validator import SensorValidationError
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
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def valid_event() -> dict[str, Any]:
    """Load valid sensor event test fixture matching the raw event contract."""
    fixture_path = FIXTURES_DIR / "valid_sensor_event.json"
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def mock_snowflake_connection() -> MagicMock:
    """Provide a mock Snowflake connection and cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# Test 1: Successful sensor event insertion
# ---------------------------------------------------------------------------
def test_successful_sensor_event_insertion(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """1. Successful insertion commits transaction and returns True."""
    result = insert_sensor_event(valid_event, connection=mock_snowflake_connection)
    assert result is True

    cursor = mock_snowflake_connection.cursor.return_value
    cursor.execute.assert_called_once()
    sql_executed, params = cursor.execute.call_args[0]

    assert f"INSERT INTO {RAW_SENSOR_TABLE}" in sql_executed
    assert "PARSE_JSON(%s)" in sql_executed
    assert mock_snowflake_connection.commit.called
    assert not mock_snowflake_connection.rollback.called


# ---------------------------------------------------------------------------
# Test 2: Invalid sensor event rejection
# ---------------------------------------------------------------------------
def test_invalid_sensor_event_rejection(mock_snowflake_connection: MagicMock) -> None:
    """2. Invalid sensor event fails schema validation before database execution."""
    invalid_event = {
        "container_id": "CONT_001",
        # Missing shipment_id, commodity, temperature, humidity, vibration, timestamp
    }
    with pytest.raises(SensorValidationError):
        insert_sensor_event(invalid_event, connection=mock_snowflake_connection)

    # Database cursor must never be created or called
    mock_snowflake_connection.cursor.assert_not_called()
    assert not mock_snowflake_connection.commit.called


# ---------------------------------------------------------------------------
# Test 3: Snowflake connection failure
# ---------------------------------------------------------------------------
@patch("snowflake.ingestion.get_snowflake_connection")
def test_snowflake_connection_failure(
    mock_get_conn: MagicMock, valid_event: dict[str, Any]
) -> None:
    """3. Connection failure raises SnowflakeConnectionError cleanly."""
    mock_get_conn.side_effect = SnowflakeConnectionError("Connection timeout to Snowflake host")

    with pytest.raises(SnowflakeConnectionError) as exc_info:
        insert_sensor_event(valid_event, connection=None)

    assert "Connection timeout" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 4: SQL insertion failure
# ---------------------------------------------------------------------------
def test_sql_insertion_failure(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """4. SQL execution failure raises SnowflakeIngestionError."""
    cursor = mock_snowflake_connection.cursor.return_value
    cursor.execute.side_effect = ProgrammingError("Table does not exist")

    with pytest.raises(SnowflakeIngestionError) as exc_info:
        insert_sensor_event(valid_event, connection=mock_snowflake_connection)

    assert "Failed to insert sensor reading" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 5: Transaction rollback on failure
# ---------------------------------------------------------------------------
def test_transaction_rollback_on_failure(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """5. Failed SQL execution triggers rollback and leaves commit uncalled."""
    cursor = mock_snowflake_connection.cursor.return_value
    cursor.execute.side_effect = Exception("Disk quota exceeded in warehouse")

    with pytest.raises(SnowflakeIngestionError):
        insert_sensor_event(valid_event, connection=mock_snowflake_connection)

    mock_snowflake_connection.rollback.assert_called_once()
    assert not mock_snowflake_connection.commit.called


# ---------------------------------------------------------------------------
# Test 6: Cursor and connection cleanup
# ---------------------------------------------------------------------------
@patch("snowflake.ingestion.get_snowflake_connection")
def test_cursor_and_connection_cleanup_on_success(
    mock_get_conn: MagicMock, valid_event: dict[str, Any]
) -> None:
    """6a. Cursor and internally managed connection are closed after success."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    insert_sensor_event(valid_event, connection=None)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("snowflake.ingestion.get_snowflake_connection")
def test_cursor_and_connection_cleanup_on_error(
    mock_get_conn: MagicMock, valid_event: dict[str, Any]
) -> None:
    """6b. Cursor and internally managed connection are closed even after an error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = Exception("Unexpected warehouse error")
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    with pytest.raises(SnowflakeIngestionError):
        insert_sensor_event(valid_event, connection=None)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Test 7: Correct JSON payload serialization
# ---------------------------------------------------------------------------
def test_correct_json_payload_serialization(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """7. Event dictionary is safely serialized to JSON for VARIANT storage."""
    insert_sensor_event(valid_event, connection=mock_snowflake_connection)

    cursor = mock_snowflake_connection.cursor.return_value
    _, params = cursor.execute.call_args[0]
    payload_json = params[0]

    assert isinstance(payload_json, str)
    deserialized = json.loads(payload_json)
    assert deserialized == valid_event
    assert deserialized["container_id"] == "CONT_001"
    assert deserialized["temperature"] == 8.5


# ---------------------------------------------------------------------------
# Test 8: Kafka metadata handling
# ---------------------------------------------------------------------------
def test_kafka_metadata_handling_custom_values(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """8a. Custom Kafka topic, partition, and offset are correctly bound to query parameters."""
    insert_sensor_event(
        valid_event,
        topic="custom-container-events",
        partition=2,
        offset=98765,
        connection=mock_snowflake_connection,
    )

    cursor = mock_snowflake_connection.cursor.return_value
    _, params = cursor.execute.call_args[0]

    assert params[1] == "custom-container-events"
    assert params[2] == 2
    assert params[3] == 98765


def test_kafka_metadata_handling_default_values(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """8b. Default Kafka topic is used and partition/offset default to None."""
    insert_sensor_event(valid_event, connection=mock_snowflake_connection)

    cursor = mock_snowflake_connection.cursor.return_value
    _, params = cursor.execute.call_args[0]

    assert params[1] == DEFAULT_KAFKA_TOPIC
    assert params[2] is None
    assert params[3] is None


# ---------------------------------------------------------------------------
# Tests 9-11: Connection configuration and factory unit tests
# ---------------------------------------------------------------------------
def test_connection_config_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configuration loader parses required environment variables properly."""
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "test_acc")
    monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_pass")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "test_wh")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "ATMOSYNC_DB")
    monkeypatch.setenv("SNOWFLAKE_SCHEMA", "RAW")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "test_role")

    config = get_snowflake_config()
    assert config["account"] == "test_acc"
    assert config["user"] == "test_user"
    assert config["warehouse"] == "test_wh"
    assert config["database"] == "ATMOSYNC_DB"
    assert config["schema"] == "RAW"
    assert config["role"] == "test_role"


def test_connection_config_missing_variables_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """Missing required configuration variables raises SnowflakeConnectionError."""
    monkeypatch.delenv("SNOWFLAKE_ACCOUNT", raising=False)
    monkeypatch.delenv("SNOWFLAKE_USER", raising=False)
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

    with pytest.raises(SnowflakeConnectionError) as exc_info:
        get_snowflake_config()

    assert "Missing required Snowflake environment variable" in str(exc_info.value)


@patch("snowflake.connector.connect")
def test_connection_factory_creates_connection(mock_connect: MagicMock) -> None:
    """get_snowflake_connection delegates to snowflake.connector.connect."""
    test_config = {
        "account": "acc",
        "user": "usr",
        "password": "pwd",
        "warehouse": "wh",
        "database": "db",
        "schema": "sch",
    }
    get_snowflake_connection(config=test_config)
    mock_connect.assert_called_once_with(**test_config)

