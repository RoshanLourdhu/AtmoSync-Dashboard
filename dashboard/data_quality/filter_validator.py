"""
AtmoSync Dashboard Filter Configuration Validator.

Validates filter columns and chart scopes against
a supplied dataset schema and available chart IDs.
"""


def validate_filter_configuration(
    filters,
    dataset_columns,
    chart_ids
):
    """
    Validate dashboard filter configuration.

    Expected filter structure:
    {
        "name": "Container ID",
        "column": "container_id",
        "scope": ["temperature_chart", "humidity_chart"]
    }

    Returns a validation report.
    """

    errors = []
    warnings = []
    validated_filters = []

    available_columns = set(dataset_columns)
    available_charts = set(chart_ids)
    filter_names = set()

    for index, filter_config in enumerate(filters):

        prefix = f"Filter {index + 1}"

        if not isinstance(filter_config, dict):
            errors.append(
                f"{prefix}: Configuration must be a dictionary."
            )
            continue

        required_keys = {"name", "column", "scope"}

        missing_keys = required_keys - set(filter_config.keys())

        if missing_keys:
            errors.append(
                f"{prefix}: Missing keys: {sorted(missing_keys)}"
            )
            continue

        name = filter_config["name"]
        column = filter_config["column"]
        scope = filter_config["scope"]

        if not isinstance(name, str) or not name.strip():
            errors.append(
                f"{prefix}: Filter name must be a non-empty string."
            )
            continue

        if name in filter_names:
            errors.append(
                f"{prefix}: Duplicate filter name '{name}'."
            )
        else:
            filter_names.add(name)

        if not isinstance(column, str) or not column.strip():
            errors.append(
                f"{name}: Column must be a non-empty string."
            )
        elif column not in available_columns:
            errors.append(
                f"{name}: Column '{column}' does not exist "
                "in the supplied dataset schema."
            )

        if not isinstance(scope, list):
            errors.append(
                f"{name}: Scope must be a list of chart IDs."
            )
            continue

        if not scope:
            warnings.append(
                f"{name}: Filter has no chart scope."
            )

        for chart_id in scope:
            if chart_id not in available_charts:
                errors.append(
                    f"{name}: Unknown chart reference '{chart_id}'."
                )

        validated_filters.append(name)

    return {
        "status": "Failed" if errors else "Passed",
        "total_filters": len(filters),
        "validated_filters": validated_filters,
        "errors": errors,
        "warnings": warnings
    }