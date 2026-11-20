"""
AtmoSync Data Quality Plotly Dashboard Visualization.

Renders quality score indicator gauges, record count metrics, validation issue
bars, and IQR outlier distribution charts using Plotly.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    PLOTLY_AVAILABLE = True
except ImportError:  # pragma: no cover
    go = None
    make_subplots = None
    PLOTLY_AVAILABLE = False


BASE_DIR = Path(__file__).resolve().parent.parent
REPORT_PATH = BASE_DIR / "data_quality_report.json"


def load_report(path: Path | str = REPORT_PATH) -> dict[str, Any]:
    """Load JSON data quality report from disk."""
    report_file = Path(path)
    with report_file.open("r", encoding="utf-8") as file:
        return json.load(file)


def create_dashboard(report: dict[str, Any]) -> Any:
    """
    Construct Plotly figure with KPI gauges, issue breakdown, and outlier bars.

    Args:
        report: Quality summary dictionary.

    Returns:
        plotly.graph_objects.Figure: Configured multi-panel figure.

    Raises:
        RuntimeError: If plotly is not installed in the environment.
    """
    if not PLOTLY_AVAILABLE:
        raise RuntimeError(
            "Plotly is not installed. Install via: pip install plotly"
        )

    # KPI values
    quality_score = report.get("quality_score", 0.0)
    total_records = report.get("total_records", 0)
    missing = report.get("missing_values", 0)
    duplicates = report.get("duplicate_records", 0)
    invalid_timestamps = report.get("invalid_timestamps", 0)
    invalid_numeric = report.get("invalid_numeric_values", 0)
    invalid_humidity = report.get("invalid_humidity_records", 0)
    invalid_vibration = report.get("invalid_vibration_records", 0)
    invalid_temperature = report.get("invalid_temperature_records", 0)

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}],
            [{"type": "bar"}, {"type": "bar"}],
        ],
        subplot_titles=(
            "Data Quality Score",
            "Total Sensor Records",
            "Validation Issues",
            "Sensor Outliers",
        ),
    )

    # KPI 1: Quality Score Gauge
    fig.add_trace(
        go.Indicator(
            mode="number+gauge",
            value=quality_score,
            number={"suffix": "%"},
            gauge={"axis": {"range": [0, 100]}},
        ),
        row=1,
        col=1,
    )

    # KPI 2: Total Records
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_records,
        ),
        row=1,
        col=2,
    )

    # Validation Issues
    issue_names = [
        "Missing Values",
        "Duplicates",
        "Invalid Timestamps",
        "Invalid Numeric",
        "Invalid Humidity",
        "Invalid Vibration",
        "Invalid Temperature",
    ]
    issue_values = [
        missing,
        duplicates,
        invalid_timestamps,
        invalid_numeric,
        invalid_humidity,
        invalid_vibration,
        invalid_temperature,
    ]

    fig.add_trace(
        go.Bar(
            x=issue_names,
            y=issue_values,
            name="Issues",
        ),
        row=2,
        col=1,
    )

    # Sensor Outliers
    outliers = report.get("outliers", {})
    fig.add_trace(
        go.Bar(
            x=list(outliers.keys()),
            y=list(outliers.values()),
            name="Outliers",
        ),
        row=2,
        col=2,
    )

    fig.update_layout(
        title="AtmoSync – Data Quality Dashboard",
        height=750,
        showlegend=False,
        template="plotly_white",
    )

    return fig


if __name__ == "__main__":
    report_data = load_report()
    dashboard = create_dashboard(report_data)
    out_path = BASE_DIR / "visualizations" / "quality_dashboard.html"
    dashboard.write_html(str(out_path))
    print("Dashboard generated successfully!")
    print("Output:", out_path)
