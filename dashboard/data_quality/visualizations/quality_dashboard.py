import json
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots


BASE_DIR = Path(__file__).resolve().parent.parent

REPORT_PATH = BASE_DIR / "data_quality_report.json"


def load_report():
    with REPORT_PATH.open("r", encoding="utf-8") as file:
        return json.load(file)


def create_dashboard(report):

    # KPI values
    quality_score = report["quality_score"]
    total_records = report["total_records"]
    missing = report["missing_values"]
    duplicates = report["duplicate_records"]
    invalid_timestamps = report["invalid_timestamps"]
    invalid_numeric = report["invalid_numeric_values"]
    invalid_humidity = report["invalid_humidity_records"]

    fig = make_subplots(
        rows=2,
        cols=2,
        specs=[
            [{"type": "indicator"}, {"type": "indicator"}],
            [{"type": "bar"}, {"type": "bar"}]
        ],
        subplot_titles=(
            "Data Quality Score",
            "Total Sensor Records",
            "Validation Issues",
            "Sensor Outliers"
        )
    )

    # KPI 1: Quality Score
    fig.add_trace(
        go.Indicator(
            mode="number+gauge",
            value=quality_score,
            number={"suffix": "%"},
            gauge={"axis": {"range": [0, 100]}}
        ),
        row=1,
        col=1
    )

    # KPI 2: Total Records
    fig.add_trace(
        go.Indicator(
            mode="number",
            value=total_records
        ),
        row=1,
        col=2
    )

    # Validation Issues
    issue_names = [
        "Missing Values",
        "Duplicates",
        "Invalid Timestamps",
        "Invalid Numeric",
        "Invalid Humidity"
    ]

    issue_values = [
        missing,
        duplicates,
        invalid_timestamps,
        invalid_numeric,
        invalid_humidity
    ]

    fig.add_trace(
        go.Bar(
            x=issue_names,
            y=issue_values,
            name="Issues"
        ),
        row=2,
        col=1
    )

    # Sensor Outliers
    outliers = report["outliers"]

    fig.add_trace(
        go.Bar(
            x=list(outliers.keys()),
            y=list(outliers.values()),
            name="Outliers"
        ),
        row=2,
        col=2
    )

    fig.update_layout(
        title="AtmoSync – Data Quality Dashboard",
        height=750,
        showlegend=False,
        template="plotly_white"
    )

    return fig


if __name__ == "__main__":

    report = load_report()

    dashboard = create_dashboard(report)

    output_path = BASE_DIR / "visualizations" / "quality_dashboard.html"

    dashboard.write_html(str(output_path))

    print("Dashboard generated successfully!")
    print("Output:", output_path)