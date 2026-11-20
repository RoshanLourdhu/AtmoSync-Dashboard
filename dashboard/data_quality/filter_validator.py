"""
AtmoSync Dashboard Filter Configuration Validator.

Validates dashboard filter configurations, ensuring target columns exist in the
supplied dataset schema and chart/dataset targets match declared IDs.
"""
from __future__ import annotations

from typing import Any, Sequence


def validate_filter_configuration(
    filters: Sequence[dict[str, Any]],
    dataset_columns: Sequence[str] | None = None,
    chart_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """
    Validate dashboard filter configuration.

    Expected filter structure:
    {
        "name": "Container ID",
        "column": "container_id",
        "scope": ["temperature_chart", "humidity_chart"]  # or "targets"
    }

    Args:
        filters: Sequence of filter configuration dictionaries.
        dataset_columns: Optional sequence of valid dataset column names.
        chart_ids: Optional sequence of valid chart or target component IDs.

    Returns:
        dict[str, Any]: Report containing status ('Passed' or 'Failed'),
                        total_filters, validated_filters, errors, and warnings.
    """
    errors: list[str] = []
    warnings: list[str] = []
    validated_filters: list[str] = []

    available_columns = set(dataset_columns) if dataset_columns is not None else None
    available_charts = set(chart_ids) if chart_ids is not None else None
    filter_names: set[str] = set()

    for index, filter_config in enumerate(filters):
        prefix = f"Filter {index + 1}"

        if not isinstance(filter_config, dict):
            errors.append(f"{prefix}: Configuration must be a dictionary.")
            continue

        name = filter_config.get("name") or filter_config.get("filter_name")
        column = filter_config.get("column")
        scope = filter_config.get("scope")
        if scope is None and "targets" in filter_config:
            scope = filter_config.get("targets")

        if not name or not isinstance(name, str) or not name.strip():
            errors.append(f"{prefix}: Filter name must be a non-empty string.")
            continue

        cleaned_name = name.strip()
        if cleaned_name in filter_names:
            errors.append(f"{prefix}: Duplicate filter name '{cleaned_name}'.")
        else:
            filter_names.add(cleaned_name)

        if not column or not isinstance(column, str) or not column.strip():
            errors.append(f"{cleaned_name}: Column must be a non-empty string.")
        elif available_columns is not None and column not in available_columns:
            errors.append(
                f"{cleaned_name}: Column '{column}' does not exist "
                "in the supplied dataset schema."
            )

        if scope is not None and not isinstance(scope, list):
            errors.append(f"{cleaned_name}: Scope must be a list of chart IDs.")
            continue

        if scope is not None and not scope:
            warnings.append(f"{cleaned_name}: Filter has no chart scope.")

        if scope is not None and available_charts is not None:
            for chart_id in scope:
                if chart_id not in available_charts:
                    errors.append(
                        f"{cleaned_name}: Unknown chart reference '{chart_id}'."
                    )

        if not errors:
            validated_filters.append(cleaned_name)

    return {
        "status": "Failed" if errors else "Passed",
        "total_filters": len(filters),
        "validated_filters": validated_filters,
        "errors": errors,
        "warnings": warnings,
    }
