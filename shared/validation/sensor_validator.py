"""
Sensor Reading Event Validation Module.

Validates raw IoT sensor event payloads against the official JSON Schema
(shared/schemas/sensor_reading.schema.json) for the AtmoSync Dashboard project.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

# Default schema path relative to this module's location:
# shared/validation/sensor_validator.py -> shared/schemas/sensor_reading.schema.json
DEFAULT_SCHEMA_PATH = (
    Path(__file__).resolve().parent.parent / "schemas" / "sensor_reading.schema.json"
)

# In-memory cache for loaded schemas and compiled validators
_SCHEMA_CACHE: dict[str, dict[str, Any]] = {}
_VALIDATOR_CACHE: dict[str, Draft202012Validator] = {}


class SensorValidationError(ValueError):
    """
    Raised when a sensor reading event fails contract schema validation.

    Attributes:
        message (str): Explanatory error message.
        field (str | None): The specific payload field that caused the failure, if identified.
        validator_name (str | None): The JSON Schema validator keyword that failed (e.g. 'required', 'type').
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        validator_name: str | None = None,
    ) -> None:
        self.message = message
        self.field = field
        self.validator_name = validator_name
        detail = f" (field: '{field}')" if field else ""
        super().__init__(f"Sensor event validation error{detail}: {message}")


def load_sensor_schema(schema_path: Path | str | None = None) -> dict[str, Any]:
    """
    Load and parse the sensor reading JSON Schema from disk.

    Args:
        schema_path: Path to the schema file. If None, uses the default schema location.

    Returns:
        dict: The loaded JSON Schema as a Python dictionary.

    Raises:
        FileNotFoundError: If the specified schema file does not exist.
        ValueError: If the schema file is not valid JSON or fails JSON Schema draft 2020-12 checks.
    """
    target_path = Path(schema_path).resolve() if schema_path else DEFAULT_SCHEMA_PATH.resolve()
    cache_key = str(target_path)

    if cache_key in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[cache_key]

    if not target_path.is_file():
        raise FileNotFoundError(f"Sensor reading schema file not found at: {target_path}")

    try:
        content = target_path.read_text(encoding="utf-8")
        schema = json.loads(content)
    except json.JSONDecodeError as err:
        raise ValueError(f"Malformed JSON in schema file '{target_path}': {err}") from err

    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as err:
        raise ValueError(f"Invalid JSON Schema draft 2020-12 in '{target_path}': {err.message}") from err

    _SCHEMA_CACHE[cache_key] = schema
    return schema


def get_sensor_validator(schema_path: Path | str | None = None) -> Draft202012Validator:
    """
    Retrieve a cached Draft202012Validator instance for the given schema path.

    Args:
        schema_path: Path to the schema file. If None, uses the default schema location.

    Returns:
        Draft202012Validator: A compiled schema validator.
    """
    target_path = Path(schema_path).resolve() if schema_path else DEFAULT_SCHEMA_PATH.resolve()
    cache_key = str(target_path)

    if cache_key not in _VALIDATOR_CACHE:
        schema = load_sensor_schema(target_path)
        _VALIDATOR_CACHE[cache_key] = Draft202012Validator(schema)

    return _VALIDATOR_CACHE[cache_key]


def _extract_field_from_error(error: ValidationError, event: dict[str, Any]) -> str | None:
    """
    Identify the specific payload property name associated with a validation error.

    Args:
        error: The ValidationError raised by jsonschema.
        event: The event payload dictionary that was validated.

    Returns:
        str | None: The offending field name, or None if not determinable.
    """
    if error.path:
        return ".".join(str(p) for p in error.path)

    if error.validator == "required":
        # Match "'<field>' is a required property"
        match = re.search(r"'([^']+)' is a required property", error.message)
        if match:
            return match.group(1)
        if hasattr(error, "validator_value") and isinstance(error.validator_value, (list, tuple)):
            missing = [k for k in error.validator_value if k not in event]
            if missing:
                return missing[0]

    if error.validator == "additionalProperties":
        # Match "Additional properties are not allowed ('<field>' was unexpected)"
        match = re.search(r"\('([^']+)' was unexpected\)", error.message)
        if match:
            return match.group(1)

    return None


def validate_sensor_event(event: Any, schema_path: Path | str | None = None) -> bool:
    """
    Validate a single sensor reading event dictionary against the official JSON Schema.

    Args:
        event: The sensor event dictionary to validate.
        schema_path: Optional custom path to a JSON Schema file.

    Returns:
        bool: True if the event passes all schema validation rules.

    Raises:
        SensorValidationError: If the event is not a dict or fails any schema constraint.
        FileNotFoundError: If the schema file cannot be found.
    """
    if not isinstance(event, dict):
        raise SensorValidationError(
            f"Sensor event must be a dictionary, got {type(event).__name__}.",
            field=None,
            validator_name="type",
        )

    validator = get_sensor_validator(schema_path)
    errors = list(validator.iter_errors(event))

    if not errors:
        return True

    # Take the first validation error encountered
    first_error = errors[0]
    field = _extract_field_from_error(first_error, event)

    raise SensorValidationError(
        message=first_error.message,
        field=field,
        validator_name=first_error.validator,
    )

