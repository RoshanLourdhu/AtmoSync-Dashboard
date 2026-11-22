"""
AtmoSync Dashboard Configuration Validator.

Validates dataset definitions, KPI metrics, chart references,
and dashboard filter targets before Superset deployment.
"""

from __future__ import annotations

from typing import Any


def validate_dashboard_config(config: dict[str, Any]) -> dict[str, Any]:
    """Validate the complete AtmoSync dashboard configuration."""

    errors: list[str] = []
    warnings: list[str] = []

    datasets = config.get("datasets", [])
    charts = config.get("charts", [])
    filters = config.get("filters", [])

    if not isinstance(datasets, list):
        datasets = []
        errors.append("Datasets must be provided as a list.")

    if not isinstance(charts, list):
        charts = []
        errors.append("Charts must be provided as a list.")

    if not isinstance(filters, list):
        filters = []
        errors.append("Filters must be provided as a list.")

    dataset_map: dict[str, dict[str, Any]] = {}
    metric_names: set[tuple[str, str]] = set()
    chart_names: set[str] = set()
    filter_names: set[str] = set()

    validated_datasets = 0
    validated_metrics = 0
    validated_charts = 0
    validated_filters = 0

    # 1. Validate datasets and KPI metrics
    for index, dataset in enumerate(datasets):
        if not isinstance(dataset, dict):
            errors.append(f"Dataset {index + 1}: Invalid dataset structure.")
            continue

        table_name = dataset.get("table_name")

        if not isinstance(table_name, str) or not table_name.strip():
            errors.append(f"Dataset {index + 1}: Missing table_name.")
            continue

        table_name = table_name.strip()

        if table_name in dataset_map:
            errors.append(f"Duplicate dataset: {table_name}.")
            continue

        dataset_map[table_name] = dataset
        validated_datasets += 1

        metrics = dataset.get("metrics", [])

        if not isinstance(metrics, list):
            errors.append(f"{table_name}: Metrics must be a list.")
            continue

        for metric in metrics:
            if not isinstance(metric, dict):
                errors.append(f"{table_name}: Invalid metric definition.")
                continue

            metric_name = metric.get("metric_name")
            expression = metric.get("expression")

            if not isinstance(metric_name, str) or not metric_name.strip():
                errors.append(f"{table_name}: Metric name is missing.")
                continue

            if not isinstance(expression, str) or not expression.strip():
                errors.append(
                    f"{table_name}: Metric '{metric_name}' has no expression."
                )
                continue

            key = (table_name, metric_name)

            if key in metric_names:
                errors.append(
                    f"{table_name}: Duplicate metric '{metric_name}'."
                )
            else:
                metric_names.add(key)
                validated_metrics += 1

    # 2. Validate chart definitions
    for index, chart in enumerate(charts):
        if not isinstance(chart, dict):
            errors.append(f"Chart {index + 1}: Invalid chart structure.")
            continue

        chart_name = chart.get("slice_name")
        dataset_name = chart.get("dataset")

        if not isinstance(chart_name, str) or not chart_name.strip():
            errors.append(f"Chart {index + 1}: Missing slice_name.")
            continue

        if chart_name in chart_names:
            errors.append(f"Duplicate chart name: {chart_name}.")
        else:
            chart_names.add(chart_name)

        if dataset_name not in dataset_map:
            errors.append(
                f"Chart '{chart_name}': Unknown dataset '{dataset_name}'."
            )
            continue

        chart_metrics = chart.get("metrics", [])

        if not isinstance(chart_metrics, list):
            errors.append(
                f"Chart '{chart_name}': Metrics must be a list."
            )
            continue

        available_metrics = {
            metric.get("metric_name")
            for metric in dataset_map[dataset_name].get("metrics", [])
            if isinstance(metric, dict)
        }

        chart_valid = True

        for metric in chart_metrics:
            # Allow inline aggregate expressions such as COUNT(*).
            if metric in available_metrics:
                continue

            if isinstance(metric, str) and metric.strip().upper() == "COUNT(*)":
                continue

            errors.append(
                f"Chart '{chart_name}': Metric '{metric}' "
                f"is not defined in dataset '{dataset_name}'."
            )
            chart_valid = False

        if chart_valid:
            validated_charts += 1

    # 3. Validate dashboard filters
    for index, filter_config in enumerate(filters):
        if not isinstance(filter_config, dict):
            errors.append(f"Filter {index + 1}: Invalid filter structure.")
            continue

        name = filter_config.get("filter_name")
        column = filter_config.get("column")
        targets = filter_config.get("targets", [])

        if not isinstance(name, str) or not name.strip():
            errors.append(f"Filter {index + 1}: Missing filter_name.")
            continue

        if name in filter_names:
            errors.append(f"Duplicate filter name: '{name}'.")
        else:
            filter_names.add(name)

        if not isinstance(column, str) or not column.strip():
            errors.append(f"Filter '{name}': Missing column.")

        if not isinstance(targets, list):
            errors.append(f"Filter '{name}': Targets must be a list.")
            continue

        if not targets:
            warnings.append(f"Filter '{name}': No target datasets configured.")

        filter_valid = True

        for target in targets:
            if target not in dataset_map:
                errors.append(
                    f"Filter '{name}': Unknown target dataset '{target}'."
                )
                filter_valid = False

        if filter_valid:
            validated_filters += 1

    return {
        "status": "Failed" if errors else "Passed",
        "summary": {
            "total_datasets": len(datasets),
            "validated_datasets": validated_datasets,
            "total_metrics": sum(
                len(d.get("metrics", []))
                for d in datasets
                if isinstance(d, dict)
                and isinstance(d.get("metrics", []), list)
            ),
            "validated_metrics": validated_metrics,
            "total_charts": len(charts),
            "validated_charts": validated_charts,
            "total_filters": len(filters),
            "validated_filters": validated_filters,
        },
        "errors": errors,
        "warnings": warnings,
    }