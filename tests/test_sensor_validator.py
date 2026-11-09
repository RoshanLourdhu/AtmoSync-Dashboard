"""
Automated Unit Tests for AtmoSync Sensor Event Validator.

Covers schema conformance, edge cases, negative validations, and error field extraction.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from shared.validation.sensor_validator import (
    SensorValidationError,
    load_sensor_schema,
    validate_sensor_event,
)

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture
def valid_event() -> dict[str, Any]:
    """Load the baseline valid sensor event fixture."""
    fixture_path = FIXTURES_DIR / "valid_sensor_event.json"
    with fixture_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def test_valid_sensor_event_passes(valid_event: dict[str, Any]) -> None:
    """1. Valid sensor event should pass validation and return True."""
    assert validate_sensor_event(valid_event) is True


def test_missing_container_id_fails(valid_event: dict[str, Any]) -> None:
    """2. Missing container_id should fail and report 'container_id'."""
    del valid_event["container_id"]
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "container_id"
    assert exc_info.value.validator_name == "required"


def test_missing_shipment_id_fails(valid_event: dict[str, Any]) -> None:
    """3. Missing shipment_id should fail and report 'shipment_id'."""
    del valid_event["shipment_id"]
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "shipment_id"
    assert exc_info.value.validator_name == "required"


def test_missing_timestamp_fails(valid_event: dict[str, Any]) -> None:
    """4. Missing timestamp should fail and report 'timestamp'."""
    del valid_event["timestamp"]
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "timestamp"
    assert exc_info.value.validator_name == "required"


def test_temperature_incorrect_type_fails(valid_event: dict[str, Any]) -> None:
    """5. Temperature with incorrect data type (string instead of number) should fail."""
    valid_event["temperature"] = "invalid_string_temp"
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "temperature"
    assert exc_info.value.validator_name == "type"


@pytest.mark.parametrize("invalid_humidity", [100.1, 150.0, -0.1, -25.0])
def test_humidity_outside_permitted_range_fails(
    valid_event: dict[str, Any], invalid_humidity: float
) -> None:
    """6. Humidity outside [0, 100] scale should fail."""
    valid_event["humidity"] = invalid_humidity
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "humidity"
    assert exc_info.value.validator_name in ("maximum", "minimum")


def test_additional_unexpected_property_fails(valid_event: dict[str, Any]) -> None:
    """7. Additional unexpected property should fail because additionalProperties is false."""
    valid_event["unauthorized_field"] = "unexpected_data"
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(valid_event)
    assert exc_info.value.field == "unauthorized_field"
    assert exc_info.value.validator_name == "additionalProperties"


def test_empty_dictionary_fails() -> None:
    """8. An empty dictionary should fail validation due to missing required properties."""
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event({})
    assert exc_info.value.validator_name == "required"
    assert exc_info.value.field is not None


@pytest.mark.parametrize("invalid_input", [None, [], "not_a_dict", 12345])
def test_non_dictionary_input_fails(invalid_input: Any) -> None:
    """Validator must reject non-dictionary inputs gracefully."""
    with pytest.raises(SensorValidationError) as exc_info:
        validate_sensor_event(invalid_input)
    assert "must be a dictionary" in str(exc_info.value)


def test_missing_schema_file_raises_filenotfound() -> None:
    """Non-existent schema path must raise FileNotFoundError."""
    non_existent = Path("non/existent/path/schema.json")
    with pytest.raises(FileNotFoundError):
        validate_sensor_event({"some": "data"}, schema_path=non_existent)


def test_load_sensor_schema_success() -> None:
    """load_sensor_schema should parse the schema and return a valid dictionary."""
    schema = load_sensor_schema()
    assert isinstance(schema, dict)
    assert schema.get("title") == "AtmoSync sensor reading"
    assert "required" in schema

