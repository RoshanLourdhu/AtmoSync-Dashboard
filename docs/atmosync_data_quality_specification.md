# AtmoSync – Data Quality & Validation Specification

## 1. Objective
Define validation checks to maintain the accuracy, consistency and reliability of sensor data used in the AtmoSync analytics dashboard.

## 2. Data Quality Dimensions

| Dimension | Description |
|---|---|
| Completeness | Check whether required sensor values are missing |
| Uniqueness | Identify duplicate sensor records |
| Validity | Detect invalid or unexpected sensor values |
| Consistency | Verify that data follows a consistent format |
| Timeliness | Check whether sensor readings are received on time |
| Accuracy | Identify unusual readings that require investigation |

## 3. Validation Checks

### 3.1 Missing Value Validation
- Check missing container IDs.
- Check missing timestamps.
- Identify missing temperature, humidity and vibration readings.
- Calculate the percentage of missing values.

### 3.2 Duplicate Record Validation
- Identify duplicate records using container ID and timestamp.
- Report duplicate record counts.
- Avoid silently deleting records before investigation.

### 3.3 Timestamp Validation
- Validate timestamp format.
- Identify future timestamps.
- Check chronological ordering of sensor readings.
- Identify unexpected gaps between readings.

### 3.4 Sensor Value Validation
- Identify negative humidity or physically implausible readings.
- Flag unusual temperature and vibration values.
- Compare readings against approved operational limits.

Exact acceptable ranges must be confirmed using the actual sensor specifications and project dataset.

### 3.5 Outlier Detection
- Use statistical methods such as Interquartile Range (IQR).
- Flag unusual readings for further investigation.
- Do not automatically classify every outlier as an error.

## 4. Data Quality Metrics

| Metric | Calculation |
|---|---|
| Missing Value Rate | Missing values / Total expected values × 100 |
| Duplicate Rate | Duplicate records / Total records × 100 |
| Valid Record Rate | Valid records / Total records × 100 |
| Data Freshness | Current time - Latest reading timestamp |

## 5. Dashboard Visualization Suggestions

- Data Quality Score – KPI Card
- Missing Values – Bar Chart
- Sensor Data Completeness – Table
- Data Freshness – KPI Card
- Invalid Records – Table
- Outlier Distribution – Histogram

## 6. Implementation Dependencies
- Validated sensor dataset
- Confirmed source column names
- Apache Superset
- PostgreSQL or approved analytical data source
- Defined sensor operating limits

## 7. Current Status
This document defines the proposed data quality and validation approach.

Actual validation rules, acceptable sensor ranges and dashboard metrics must be finalized after inspecting the source dataset.

