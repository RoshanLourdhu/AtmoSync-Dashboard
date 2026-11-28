"""
AtmoSync Data Quality Plotly Dashboard Visualization.

Renders quality score indicator gauges, record count metrics, validation issue
bars, and IQR outlier distribution charts using Plotly with explicit handling
for missing files, malformed JSON, empty datasets, invalid values, and missing
Plotly installations.
"""
from __future__ import annotations

import json
import math
import sys
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
    """Load and validate a JSON data quality report from disk."""
    report_file = Path(path)
    if not report_file.is_file():
        raise FileNotFoundError(
            f"Data quality report not found at '{report_file}'. "
            "Run report_generator.py first to generate a report."
        )

    raw_text = report_file.read_text(encoding="utf-8").strip()
    if not raw_text:
        raise ValueError(f"Data quality report '{report_file}' is empty.")

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed JSON in data quality report '{report_file}': {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(
            f"Data quality report '{report_file}' must contain a JSON object, got {type(data).__name__}."
        )
    return data


def _safe_number(report: dict[str, Any], key: str, default: float = 0.0) -> float:
    """Extract a finite numeric value from report[key], raising ValueError on invalid input."""
    val = report.get(key, default)
    if val is None:
        return default
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise ValueError(f"Report field '{key}' must be numeric, got {type(val).__name__}: {val!r}")
    numeric = float(val)
    if math.isnan(numeric) or math.isinf(numeric):
        raise ValueError(f"Report field '{key}' must be finite, got {val!r}")
    return numeric


def create_dashboard(report: dict[str, Any]) -> Any:
    """
    Construct Plotly figure with KPI gauges, issue breakdown, and outlier bars.

    Args:
        report: Quality summary dictionary.

    Returns:
        plotly.graph_objects.Figure: Configured multi-panel figure.

    Raises:
        RuntimeError: If plotly is not installed in the environment.
        ValueError: If report is not a dict or contains invalid numeric/outlier fields.
    """
    if not PLOTLY_AVAILABLE or go is None or make_subplots is None:
        raise RuntimeError(
            "Plotly is not installed. Install via: pip install plotly"
        )

    if not isinstance(report, dict):
        raise ValueError(f"Quality report must be a dictionary, got {type(report).__name__}.")

    quality_score = _safe_number(report, "quality_score", 0.0)
    total_records = _safe_number(report, "total_records", 0.0)
    missing = _safe_number(report, "missing_values", 0.0)
    duplicates = _safe_number(report, "duplicate_records", 0.0)
    invalid_timestamps = _safe_number(report, "invalid_timestamps", 0.0)
    invalid_numeric = _safe_number(report, "invalid_numeric_values", 0.0)
    invalid_humidity = _safe_number(report, "invalid_humidity_records", 0.0)
    invalid_vibration = _safe_number(report, "invalid_vibration_records", 0.0)
    invalid_temperature = _safe_number(report, "invalid_temperature_records", 0.0)

    outliers_raw = report.get("outliers", {})
    if outliers_raw is None:
        outliers_raw = {}
    if not isinstance(outliers_raw, dict):
        raise ValueError(f"Report field 'outliers' must be a mapping, got {type(outliers_raw).__name__}.")
    outliers = {str(k): _safe_number(outliers_raw, k, 0.0) for k in outliers_raw}

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

    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_records,
        ),
        row=1,
        col=2,
    )

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

    fig.add_trace(
        go.Bar(
            x=list(outliers.keys()),
            y=list(outliers.values()),
            name="Outliers",
        ),
        row=2,
        col=2,
    )

    layout_kwargs: dict[str, Any] = {
        "title": "AtmoSync – Data Quality Dashboard",
        "height": 750,
        "showlegend": False,
        "template": "plotly_white",
    }
    if total_records == 0:
        layout_kwargs["annotations"] = [
            {
                "text": "Empty dataset: no sensor records present in report.",
                "xref": "paper",
                "yref": "paper",
                "x": 0.5,
                "y": 0.98,
                "showarrow": False,
            }
        ]

    fig.update_layout(**layout_kwargs)
    return fig


def main(report_path: Path | str = REPORT_PATH) -> int:
    """CLI entrypoint to load report and render HTML dashboard safely."""
    try:
        report_data = load_report(report_path)
        dashboard = create_dashboard(report_data)
        out_path = BASE_DIR / "visualizations" / "quality_dashboard.html"
        dashboard.write_html(str(out_path))
        print("Dashboard generated successfully!")
        print("Output:", out_path)
        return 0
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        print(f"Error generating data quality dashboard: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
