import json
import pandas as pd
from pathlib import Path

from validator import validate_sensor_data


def generate_quality_report(df):
    """
    Generate a structured data quality summary.
    """

    validation = validate_sensor_data(df.copy())

    if validation["status"] == "Failed":
        return validation

    total_records = validation["total_records"]

    missing_count = sum(
        validation["missing_values"].values()
    )

    duplicate_count = validation["duplicate_records"]
    invalid_timestamps = validation["invalid_timestamps"]
    invalid_humidity = validation["invalid_humidity_records"]

    invalid_numeric = sum(
        validation["invalid_numeric_values"].values()
    )

    # Calculate data quality score
    if total_records == 0:
        quality_score = 0
    else:
        issues = (
            missing_count
            + duplicate_count
            + invalid_timestamps
            + invalid_humidity
            + invalid_numeric
        )

        quality_score = max(
            0,
            round((1 - min(issues / (total_records * 5), 1)) * 100, 2)
        )

    report = {
        "total_records": total_records,
        "quality_score": quality_score,
        "missing_values": missing_count,
        "duplicate_records": duplicate_count,
        "invalid_timestamps": invalid_timestamps,
        "invalid_numeric_values": invalid_numeric,
        "invalid_humidity_records": invalid_humidity,
        "outliers": validation["outliers"],
        "validation_status": validation["status"]
    }

    return report


def export_report(report, output_path="data_quality_report.json"):
    """
    Export quality report as JSON.
    """

    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as file:
        json.dump(report, file, indent=4)

    return str(output)


if __name__ == "__main__":

    sample_data = {
        "container_id": ["C101", "C102", "C101", "C103"],
        "timestamp": [
            "2026-11-16 10:00:00",
            "2026-11-16 10:05:00",
            "2026-11-16 10:00:00",
            "invalid-date"
        ],
        "temperature": [25.5, 28.0, 25.5, 100.0],
        "humidity": [65, 70, 65, 120],
        "vibration": [2.1, 3.5, 2.1, 4.0]
    }

    df = pd.DataFrame(sample_data)

    report = generate_quality_report(df)

    report_path = export_report(report)

    print("\n===== ATMOSYNC DATA QUALITY SUMMARY =====")

    for key, value in report.items():
        print(f"{key}: {value}")

report_path = export_report(
    report,
    "dashboard/data_quality/data_quality_report.json"
)