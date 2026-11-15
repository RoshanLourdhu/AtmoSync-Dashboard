import pandas as pd


REQUIRED_COLUMNS = [
    "container_id",
    "timestamp",
    "temperature",
    "humidity",
    "vibration"
]


def validate_sensor_data(df):
    """
    Validate sensor data and generate a data quality report.
    """

    report = {}

    # 1. Required column validation
    missing_columns = [
        column for column in REQUIRED_COLUMNS
        if column not in df.columns
    ]

    report["missing_columns"] = missing_columns

    if missing_columns:
        report["status"] = "Failed"
        return report

    total_records = len(df)

    # 2. Missing value validation
    report["missing_values"] = (
        df[REQUIRED_COLUMNS].isnull().sum().to_dict()
    )

    # 3. Duplicate record validation
    report["duplicate_records"] = int(
        df.duplicated(
            subset=["container_id", "timestamp"]
        ).sum()
    )

    # 4. Timestamp validation
    timestamps = pd.to_datetime(
        df["timestamp"],
        errors="coerce"
    )

    report["invalid_timestamps"] = int(
        timestamps.isnull().sum()
    )

    # 5. Sensor value validation
    numeric_columns = [
        "temperature",
        "humidity",
        "vibration"
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce"
        )

    report["invalid_numeric_values"] = (
        df[numeric_columns].isnull().sum().to_dict()
    )

    # 6. Humidity validation
    report["invalid_humidity_records"] = int(
        ((df["humidity"] < 0) | (df["humidity"] > 100)).sum()
    )

    # 7. Outlier identification using IQR
    outliers = {}

    for column in numeric_columns:
        values = df[column].dropna()

        if values.empty:
            outliers[column] = 0
            continue

        q1 = values.quantile(0.25)
        q3 = values.quantile(0.75)
        iqr = q3 - q1

        lower_limit = q1 - 1.5 * iqr
        upper_limit = q3 + 1.5 * iqr

        outliers[column] = int(
            ((values < lower_limit) |
             (values > upper_limit)).sum()
        )

    report["outliers"] = outliers

    # 8. Overall record count
    report["total_records"] = total_records

    report["status"] = "Completed"

    return report