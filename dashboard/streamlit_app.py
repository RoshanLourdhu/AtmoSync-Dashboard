"""Streamlit application for AtmoSync – Data Quality Monitor.

Integrates existing configuration validation and data‑quality report generation.
The app provides four main sections via a sidebar:

1. Overview – high‑level counts and a validation trigger.
2. Configuration Validation – runs the dashboard config validator and displays
   results, errors and warnings.
3. Data Quality – generates (or loads) a data‑quality report and visualises the
   key metrics and outlier statistics.
4. Reports – shows the raw JSON report and offers a download button.
5. About – project description.

All heavy lifting (validation, report generation) re‑uses the existing
functions in ``dashboard_config_validator.py`` and ``report_generator.py``.
"""

import yaml
import pathlib
from typing import Any, Dict, List

import streamlit as st
import pandas as pd
import plotly.express as px

# ---------------------------------------------------------------------------
# Helper paths – keep them relative to this file for portability.
# ---------------------------------------------------------------------------
ROOT_DIR = pathlib.Path(__file__).parent
CONFIG_PATH = ROOT_DIR / "dashboards" / "datasets_and_charts.yaml"
REPORT_JSON_PATH = ROOT_DIR / "data_quality" / "data_quality_report.json"

# ---------------------------------------------------------------------------
# Lazy imports of the project's business logic (kept outside the top‑level
# to avoid import‑time side effects when the module is imported by the test
# suite).
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def load_config_validator():
    from dashboard.data_quality.dashboard_config_validator import (
        validate_dashboard_config,
    )

    return validate_dashboard_config

@st.cache_resource(show_spinner=False)
def load_report_generator():
    from dashboard.data_quality.report_generator import (
        generate_quality_report_new as generate_quality_report,
        export_report,
    )

    return generate_quality_report, export_report

# ---------------------------------------------------------------------------
# UI helpers
# ---------------------------------------------------------------------------
def sidebar_nav() -> str:
    """Render the sidebar navigation and return the selected page name."""
    st.sidebar.title("AtmoSync – Data Quality Monitor")
    page = st.sidebar.radio(
        "Navigation",
        (
            "Overview",
            "Configuration Validation",
            "Data Quality",
            "Reports",
            "About",
        ),
    )
    return page

# ---------------------------------------------------------------------------
# Page implementations
# ---------------------------------------------------------------------------
def page_overview():
    st.header("Overview")
    # Load configuration safely
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as exc:
        st.error(f"Failed to read configuration file: {exc}")
        return

    # Basic counts (datasets, metrics, charts, filters)
    total_datasets = len(config.get("datasets", []))
    total_metrics = sum(len(ds.get("metrics", [])) for ds in config.get("datasets", []))
    total_charts = len(config.get("charts", []))
    total_filters = len(config.get("filters", [])) if isinstance(config.get("filters"), list) else 0

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Datasets", total_datasets)
    col2.metric("Metrics", total_metrics)
    col3.metric("Charts", total_charts)
    col4.metric("Filters", total_filters)

    # Validation status (lazy – on button press)
    if st.button("Run Configuration Validation"):
        validate_fn = load_config_validator()
        result = validate_fn(config)
        status = "PASS" if not result.get("errors") else "FAIL"
        st.success(f"Validation result: **{status}**")
        if result.get("errors"):
            st.error("**Errors**")
            for e in result["errors"]:
                st.write(f"- {e}")
        if result.get("warnings"):
            st.warning("**Warnings**")
            for w in result["warnings"]:
                st.write(f"- {w}")

def page_configuration_validation():
    st.header("Configuration Validation")
    # Load YAML
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except Exception as exc:
        st.error(f"Unable to read configuration file: {exc}")
        return

    validate_fn = load_config_validator()
    result = validate_fn(config)
    status = "PASS" if not result.get("errors") else "FAIL"
    st.subheader(f"Validation Status: {status}")

    summary = result.get("summary", {})
    if summary:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Datasets", f"{summary.get('validated_datasets',0)}/{summary.get('total_datasets',0)}")
        col2.metric("Metrics", f"{summary.get('validated_metrics',0)}/{summary.get('total_metrics',0)}")
        col3.metric("Charts", f"{summary.get('validated_charts',0)}/{summary.get('total_charts',0)}")
        col4.metric("Filters", f"{summary.get('validated_filters',0)}/{summary.get('total_filters',0)}")

    if result.get("errors"):
        st.error("**Errors**")
        for e in result["errors"]:
            st.write(f"- {e}")
    if result.get("warnings"):
        st.warning("**Warnings**")
        for w in result["warnings"]:
            st.write(f"- {w}")

def _load_or_generate_quality_report() -> Dict[str, Any]:
    """Return a quality report dict.

    Preference order:
    1. Load a previously exported JSON report if it exists.
    2. Generate a fresh report using the sample data embedded in
       ``report_generator`` (the same data used by its ``__main__`` block).
    """
    if REPORT_JSON_PATH.is_file():
        try:
            return json.loads(REPORT_JSON_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass  # fall back to generation
    # Fallback – use the sample data from the module's __main__ section.
    generate_report, _ = load_report_generator()
    sample_data = [
        {
            "container_id": "C101",
            "shipment_id": "SHIP-001",
            "commodity": "Bananas",
            "timestamp": "2026-11-20T10:00:00Z",
            "temperature": 14.5,
            "humidity": 85.0,
            "vibration": 0.15,
        },
        {
            "container_id": "C102",
            "shipment_id": "SHIP-002",
            "commodity": "Vaccines",
            "timestamp": "2026-11-20T10:05:00Z",
            "temperature": 4.0,
            "humidity": 50.0,
            "vibration": 0.05,
        },
        {
            "container_id": "C101",
            "shipment_id": "SHIP-001",
            "commodity": "Bananas",
            "timestamp": "2026-11-20T10:00:00Z",
            "temperature": 14.5,
            "humidity": 85.0,
            "vibration": 0.15,
        },
        {
            "container_id": "C103",
            "shipment_id": "SHIP-003",
            "commodity": "Fresh Fish",
            "timestamp": "invalid-date",
            "temperature": 100.0,
            "humidity": 120.0,
            "vibration": -1.0,
        },
    ]
    return generate_report(sample_data)

def page_data_quality():
    st.header("Data Quality")
    report = _load_or_generate_quality_report()
    if not report:
        st.error("Unable to obtain a data quality report.")
        return

    # Show key KPI cards
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Records", report.get("total_records", 0))
    col2.metric("Quality Score", f"{report.get('quality_score', 0)}%")
    col3.metric("Missing Values", report.get("missing_values", 0))

    col4, col5, col6 = st.columns(3)
    col4.metric("Duplicates", report.get("duplicate_records", 0))
    col5.metric("Invalid Timestamps", report.get("invalid_timestamps", 0))
    col6.metric("Invalid Humidity", report.get("invalid_humidity_records", 0))

    # Outliers bar chart (if present)
    outliers = report.get("outliers", {})
    if outliers:
        st.subheader("Outlier Summary")
        df_out = (
            pd.DataFrame(list(outliers.items()), columns=["Metric", "Count"]).sort_values("Count", ascending=False)
        )
        fig = px.bar(df_out, x="Metric", y="Count", color="Metric", title="Outlier Counts per Metric")
        fig.update_layout(showlegend=False, template="plotly_dark")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No outlier information available in the report.")

    st.subheader("Full Report (JSON)")
    st.code(json.dumps(report, indent=4), language="json")

def page_reports():
    st.header("Reports")
    report = _load_or_generate_quality_report()
    if not report:
        st.warning("No report data available.")
        return
    st.subheader("Data Quality Report JSON")
    st.code(json.dumps(report, indent=4), language="json")
    # Provide a download button – Streamlit expects bytes.
    json_bytes = json.dumps(report, indent=4).encode("utf-8")
    st.download_button(
        label="Download JSON Report",
        data=json_bytes,
        file_name="data_quality_report.json",
        mime="application/json",
    )

def page_about():
    st.header("About AtmoSync – Data Quality Monitor")
    st.markdown(
        """
        **Project Objective**
        : AtmoSync monitors micro‑climate variations inside refrigerated agricultural shipping containers.
        It uses temperature, humidity and vibration sensor data to ensure product quality and to support
        business‑intelligence decisions.

        **Problem Statement**
        : Sensor streams often contain missing values, duplicates, out‑of‑range readings and timestamp
        anomalies. A systematic data‑quality assessment is required before any downstream analytics.

        **Technology Stack**
        - Python (3.13)
        - Pandas for data manipulation
        - Plotly for interactive visualisations
        - Streamlit for the lightweight web UI
        - YAML configuration for Superset dashboard definitions
        - Existing validation modules (`dashboard_config_validator`, `report_generator`)

        **Your Role**
        : *Dashboard and BI Development* – building the user‑facing analytics interface, integrating
        validation logic, and presenting clear, actionable insights.
        """
    )

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
def main():
    st.set_page_config(page_title="AtmoSync – Data Quality Monitor", layout="wide")
    page = sidebar_nav()
    if page == "Overview":
        page_overview()
    elif page == "Configuration Validation":
        page_configuration_validation()
    elif page == "Data Quality":
        page_data_quality()
    elif page == "Reports":
        page_reports()
    elif page == "About":
        page_about()

if __name__ == "__main__":
    main()
