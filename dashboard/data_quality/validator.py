"""
AtmoSync Tabular and Event Sensor Data Quality Validator.

Evaluates sensor telemetry records against schema requirements, null checks,
duplicate keys, timestamp parseability, numeric bounds, physical range limits,
and statistical IQR outliers.

Supports both standard Python list-of-dicts records and optional Pandas DataFrames.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

# Official 7-field cold-chain IoT contract
REQUIRED_COLUMNS: list[str] = [
    "container_id",
    "shipment_id",
    "commodity",
    "timestamp",
    "temperature",
    "humidity",
    "vibration",
]

# Legacy 5-field baseline for backward compatibility
LEGACY_5_FIELD_COLUMNS: list[str] = [
    "container_id",
    "timestamp",
    "temperature",
    "humidity",
    "vibration",
]

CORE_NUMERIC_COLUMNS: list[str] = [
    "temperature",
    "humidity",
    "vibration",
]


def _compute_iqr_outliers(values: list[float]) -> int:
    """Compute count of statistical outliers using 1.5 * IQR on sorted floats."""
    if len(values) < 4:
        return 0
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    q1_idx = int(0.25 * n)
    q3_idx = int(0.75 * n)
    q1 = sorted_vals[q1_idx]
    q3 = sorted_vals[q3_idx]
    iqr = q3 - q1
    lower_limit = q1 - 1.5 * iqr
    upper_limit = q3 + 1.5 * iqr
    return sum(1 for v in sorted_vals if v < lower_limit or v > upper_limit)


def validate_sensor_data(
    data: Any,
    required_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """
    Validate tabular or list-of-dict sensor data and generate a data quality report.

    Args:
        data: Input records as a list of dicts, or a Pandas DataFrame.
        required_columns: Optional sequence of expected columns. Defaults to REQUIRED_COLUMNS.

    Returns:
        dict[str, Any]: Quality validation report including missing columns, nulls,
                        duplicates, invalid timestamps/numerics, physical bounds, and IQR outliers.
    """
    cols = list(required_columns) if required_columns is not None else REQUIRED_COLUMNS
    report: dict[str, Any] = {}

    # Handle DataFrame conversion if a DataFrame is passed
    if hasattr(data, "columns") and hasattr(data, "to_dict"):
        df_cols = list(data.columns)
        missing_columns = [col for col in cols if col not in df_cols]
        report["missing_columns"] = missing_columns
        total_records = len(data)
        report["total_records"] = total_records
        if missing_columns:
            report["status"] = "Failed"
            return report
        records: list[dict[str, Any]] = data.to_dict(orient="records")
    elif isinstance(data, list):
        total_records = len(data)
        report["total_records"] = total_records
        if not data:
            report["missing_columns"] = []
            report["status"] = "Completed"
            report["missing_values"] = {col: 0 for col in cols}
            report["duplicate_records"] = 0
            report["invalid_timestamps"] = 0
            report["invalid_numeric_values"] = {col: 0 for col in CORE_NUMERIC_COLUMNS}
            report["invalid_humidity_records"] = 0
            report["invalid_vibration_records"] = 0
            report["invalid_temperature_records"] = 0
            report["outliers"] = {col: 0 for col in CORE_NUMERIC_COLUMNS}
            return report

        # Check missing columns against all unique keys across records
        present_keys = set().union(*(r.keys() for r in data if isinstance(r, dict)))
        missing_columns = [col for col in cols if col not in present_keys]
        report["missing_columns"] = missing_columns
        if missing_columns:
            report["status"] = "Failed"
            return report
        records = data
    else:
        report["missing_columns"] = cols
        report["total_records"] = 0
        report["status"] = "Failed"
        return report

    # 1. Missing values per required column
    missing_values: dict[str, int] = {col: 0 for col in cols}
    for r in records:
        for col in cols:
            val = r.get(col)
            if val is None or (isinstance(val, str) and not val.strip()):
                missing_values[col] += 1
    report["missing_values"] = missing_values

    # 2. Duplicate records (based on container_id and timestamp)
    seen_keys: set[tuple[Any, Any]] = set()
    duplicate_count = 0
    for r in records:
        key = (r.get("container_id"), r.get("timestamp"))
        if key in seen_keys:
            duplicate_count += 1
        else:
            seen_keys.add(key)
    report["duplicate_records"] = duplicate_count

    # 3. Timestamp validation
    invalid_timestamps = 0
    for r in records:
        ts = r.get("timestamp")
        if ts is None:
            invalid_timestamps += 1
            continue
        ts_str = str(ts).strip().replace("Z", "+00:00")
        try:
            datetime.fromisoformat(ts_str)
        except Exception:
            invalid_timestamps += 1
    report["invalid_timestamps"] = invalid_timestamps

    # 4. Numeric validation and range checks
    invalid_numerics: dict[str, int] = {
        col: 0 for col in CORE_NUMERIC_COLUMNS if col in cols
    }
    extracted_numerics: dict[str, list[float]] = {
        col: [] for col in CORE_NUMERIC_COLUMNS if col in cols
    }
    invalid_humidity_count = 0
    invalid_vibration_count = 0
    invalid_temperature_count = 0

    for r in records:
        for col in invalid_numerics:
            val = r.get(col)
            if val is None or isinstance(val, bool):
                invalid_numerics[col] += 1
                continue
            try:
                num = float(val)
                extracted_numerics[col].append(num)

                # Physical range boundaries
                if col == "humidity" and (num < 0.0 or num > 100.0):
                    invalid_humidity_count += 1
                elif col == "vibration" and num < 0.0:
                    invalid_vibration_count += 1
                elif col == "temperature" and (num < -50.0 or num > 60.0):
                    invalid_temperature_count += 1

            except (ValueError, TypeError):
                invalid_numerics[col] += 1

    report["invalid_numeric_values"] = invalid_numerics
    report["invalid_humidity_records"] = invalid_humidity_count
    report["invalid_vibration_records"] = invalid_vibration_count
    report["invalid_temperature_records"] = invalid_temperature_count

    # 5. Outliers via IQR
    outliers: dict[str, int] = {}
    for col, values in extracted_numerics.items():
        outliers[col] = _compute_iqr_outliers(values)
    report["outliers"] = outliers

    report["status"] = "Completed"
    return report
