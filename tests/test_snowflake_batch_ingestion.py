"""
Automated Unit Tests for Snowflake Batch Ingestion Engine.

Validates multi-event batch insertion, transaction atomicity (commit/rollback),
empty batch handling, pre-execution schema rejection, resource cleanup, and
JSON serialization using mocked Snowflake connections.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from snowflake.connector.errors import ProgrammingError

from shared.validation.sensor_validator import SensorValidationError
from snowflake.ingestion import (
    DEFAULT_KAFKA_TOPIC,
    RAW_SENSOR_TABLE,
    SnowflakeIngestionError,
    insert_sensor_events,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def valid_event() -> dict[str, Any]:
    """Load valid sensor reading fixture."""
    fixture_path = FIXTURES_DIR / "valid_sensor_event.json"
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)


@pytest.fixture
def valid_batch(valid_event: dict[str, Any]) -> list[dict[str, Any]]:
    """Create a batch of 3 distinct valid events."""
    event1 = dict(valid_event)
    event2 = {
        **valid_event,
        "container_id": "CONT_002",
        "shipment_id": "SHIP_1002",
        "commodity": "Bananas",
        "temperature": 13.5,
        "humidity": 82.0,
        "vibration": 0.18,
        "timestamp": "2026-11-12T10:00:00Z",
    }
    event3 = {
        **valid_event,
        "container_id": "CONT_003",
        "shipment_id": "SHIP_1003",
        "commodity": "Mangoes",
        "temperature": 8.0,
        "humidity": 78.5,
        "vibration": 0.12,
        "timestamp": "2026-11-12T10:01:00Z",
    }
    return [event1, event2, event3]


@pytest.fixture
def mock_snowflake_connection() -> MagicMock:
    """Mock Snowflake database connection and cursor."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value = cursor
    return conn


# ---------------------------------------------------------------------------
# Test 1: Successful insertion of multiple valid events
# ---------------------------------------------------------------------------
def test_successful_batch_insertion(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """Batch of valid events is inserted via executemany and committed."""
    result = insert_sensor_events(valid_batch, connection=mock_snowflake_connection)
    assert result is True

    cursor = mock_snowflake_connection.cursor.return_value
    cursor.executemany.assert_called_once()
    sql_executed, params_list = cursor.executemany.call_args[0]

    assert f"INSERT INTO {RAW_SENSOR_TABLE}" in sql_executed
    assert "PARSE_JSON(%s)" in sql_executed
    assert len(params_list) == len(valid_batch)
    assert mock_snowflake_connection.commit.called
    assert not mock_snowflake_connection.rollback.called


# ---------------------------------------------------------------------------
# Test 2: Empty batch handling
# ---------------------------------------------------------------------------
def test_empty_batch_handling(mock_snowflake_connection: MagicMock) -> None:
    """Empty batch succeeds as a no-op without creating cursor or running queries."""
    result = insert_sensor_events([], connection=mock_snowflake_connection)
    assert result is True

    mock_snowflake_connection.cursor.assert_not_called()
    assert not mock_snowflake_connection.commit.called
    assert not mock_snowflake_connection.rollback.called


# ---------------------------------------------------------------------------
# Test 3: Invalid event rejection
# ---------------------------------------------------------------------------
def test_invalid_event_rejection_in_batch(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """If any event in the batch is invalid, validation fails before DB operations."""
    valid_batch[1] = {"container_id": "CONT_002"}  # Missing required fields

    with pytest.raises(SensorValidationError):
        insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    # Database cursor must never be created or called
    mock_snowflake_connection.cursor.assert_not_called()
    assert not mock_snowflake_connection.commit.called
    assert not mock_snowflake_connection.rollback.called


def test_invalid_batch_type_raises(mock_snowflake_connection: MagicMock) -> None:
    """Non-list input raises SnowflakeIngestionError."""
    with pytest.raises(SnowflakeIngestionError):
        insert_sensor_events("not_a_list", connection=mock_snowflake_connection)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Test 4: Database insertion failure
# ---------------------------------------------------------------------------
def test_batch_database_insertion_failure(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """SQL execution failure raises SnowflakeIngestionError."""
    cursor = mock_snowflake_connection.cursor.return_value
    cursor.executemany.side_effect = ProgrammingError("Warehouse suspended")

    with pytest.raises(SnowflakeIngestionError) as exc_info:
        insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    assert "Failed to batch insert" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test 5: Transaction rollback
# ---------------------------------------------------------------------------
def test_batch_transaction_rollback_on_failure(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """Failure triggers rollback and leaves commit uncalled."""
    cursor = mock_snowflake_connection.cursor.return_value
    cursor.executemany.side_effect = Exception("Network connection lost")

    with pytest.raises(SnowflakeIngestionError):
        insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    mock_snowflake_connection.rollback.assert_called_once()
    assert not mock_snowflake_connection.commit.called


# ---------------------------------------------------------------------------
# Test 6: Successful transaction commit
# ---------------------------------------------------------------------------
def test_batch_transaction_commit_on_success(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """Successful execution calls commit exactly once and does not roll back."""
    insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    mock_snowflake_connection.commit.assert_called_once()
    assert not mock_snowflake_connection.rollback.called


# ---------------------------------------------------------------------------
# Test 7: Cursor and connection cleanup
# ---------------------------------------------------------------------------
@patch("snowflake.ingestion.get_snowflake_connection")
def test_batch_cleanup_on_success(
    mock_get_conn: MagicMock, valid_batch: list[dict[str, Any]]
) -> None:
    """Cursor and internally created connection are cleaned up on success."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    insert_sensor_events(valid_batch, connection=None)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


@patch("snowflake.ingestion.get_snowflake_connection")
def test_batch_cleanup_on_failure(
    mock_get_conn: MagicMock, valid_batch: list[dict[str, Any]]
) -> None:
    """Cursor and internally created connection are cleaned up on failure."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.executemany.side_effect = Exception("Warehouse offline")
    mock_conn.cursor.return_value = mock_cursor
    mock_get_conn.return_value = mock_conn

    with pytest.raises(SnowflakeIngestionError):
        insert_sensor_events(valid_batch, connection=None)

    mock_cursor.close.assert_called_once()
    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Test 8: Correct JSON payload serialization
# ---------------------------------------------------------------------------
def test_batch_json_serialization(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """All events in batch are serialized to valid JSON matching original dicts."""
    insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    cursor = mock_snowflake_connection.cursor.return_value
    _, params_list = cursor.executemany.call_args[0]

    for idx, (payload_str, _, _, _) in enumerate(params_list):
        parsed = json.loads(payload_str)
        assert parsed == valid_batch[idx]
        assert parsed["container_id"] == valid_batch[idx]["container_id"]


# ---------------------------------------------------------------------------
# Test 9: Kafka metadata handling
# ---------------------------------------------------------------------------
def test_batch_kafka_metadata_default(
    valid_batch: list[dict[str, Any]], mock_snowflake_connection: MagicMock
) -> None:
    """Default Kafka metadata parameters are correctly bound to each row."""
    insert_sensor_events(valid_batch, connection=mock_snowflake_connection)

    cursor = mock_snowflake_connection.cursor.return_value
    _, params_list = cursor.executemany.call_args[0]

    for _, topic, partition, offset in params_list:
        assert topic == DEFAULT_KAFKA_TOPIC
        assert partition is None
        assert offset is None


def test_batch_kafka_metadata_custom_and_tuples(
    valid_event: dict[str, Any], mock_snowflake_connection: MagicMock
) -> None:
    """Batch supports per-item tuple metadata (event, topic, partition, offset)."""
    item1 = (valid_event, "topic-a", 0, 100)
    item2 = (valid_event, "topic-b", 1, 200)

    insert_sensor_events([item1, item2], connection=mock_snowflake_connection)

    cursor = mock_snowflake_connection.cursor.return_value
    _, params_list = cursor.executemany.call_args[0]

    assert params_list[0][1:] == ("topic-a", 0, 100)
    assert params_list[1][1:] == ("topic-b", 1, 200)
