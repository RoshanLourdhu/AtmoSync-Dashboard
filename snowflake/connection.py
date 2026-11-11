"""
Snowflake Database Connection Management.

Provides secure, reusable session management with Snowflake using environment
variables. Ensures credentials remain strictly in environment configurations.
"""
from __future__ import annotations

import os
from typing import Any

from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import Error as SnowflakeConnectorError

# Load variables from .env if present
load_dotenv()


class SnowflakeConnectionError(Exception):
    """Raised when Snowflake connection parameters are missing or connection fails."""


def get_snowflake_config(overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """
    Retrieve Snowflake connection parameters from environment variables.

    Reads from:
        - SNOWFLAKE_ACCOUNT: Account identifier.
        - SNOWFLAKE_USER: Username.
        - SNOWFLAKE_PASSWORD: Password.
        - SNOWFLAKE_WAREHOUSE: Compute warehouse.
        - SNOWFLAKE_DATABASE: Target database (defaults to 'ATMOSYNC_DB').
        - SNOWFLAKE_SCHEMA: Target schema (defaults to 'RAW').
        - SNOWFLAKE_ROLE: Optional role name.

    Args:
        overrides: Optional dictionary of parameter overrides.

    Returns:
        dict: Parameters dictionary for snowflake.connector.connect.

    Raises:
        SnowflakeConnectionError: If any required parameter is missing.
    """
    config: dict[str, Any] = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "ATMOSYNC_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    }

    role = os.getenv("SNOWFLAKE_ROLE")
    if role:
        config["role"] = role

    if overrides:
        config.update({k: v for k, v in overrides.items() if v is not None})

    required_keys = ["account", "user", "password", "warehouse", "database", "schema"]
    missing = [k for k in required_keys if not config.get(k)]

    if missing:
        missing_vars = [f"SNOWFLAKE_{k.upper()}" for k in missing]
        raise SnowflakeConnectionError(
            f"Missing required Snowflake environment variable(s): {', '.join(missing_vars)}. "
            "Configure them in your local environment or .env file."
        )

    return config


def get_snowflake_connection(
    config: dict[str, Any] | None = None,
    **kwargs: Any,
) -> SnowflakeConnection:
    """
    Create and return an active connection to Snowflake.

    Args:
        config: Explicit connection dictionary. If None, reads from environment variables.
        **kwargs: Additional keyword arguments passed to snowflake.connector.connect.

    Returns:
        SnowflakeConnection: Active database connection session.

    Raises:
        SnowflakeConnectionError: If configuration is invalid or the connection fails.
    """
    params = config.copy() if config else get_snowflake_config()
    params.update(kwargs)

    try:
        connection = snowflake.connector.connect(**params)
        return connection
    except SnowflakeConnectorError as err:
        raise SnowflakeConnectionError(f"Snowflake connection failed: {err}") from err
    except Exception as err:
        raise SnowflakeConnectionError(f"Unexpected error establishing Snowflake connection: {err}") from err

