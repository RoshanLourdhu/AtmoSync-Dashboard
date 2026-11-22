Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


# AtmoSync – Commodity Analytics using dbt and Snowflake

## Project Overview

AtmoSync is a data analytics project built using Snowflake and dbt.
The project analyzes commodity data to identify spoilage risks,
price differences between locations, and data quality issues.

## Technologies Used

- Snowflake
- dbt Core
- SQL
- Python
- Git and GitHub
- VS Code

## Project Objectives

1. Data preprocessing
2. Data staging using dbt
3. Commodity spoilage analytics
4. Commodity arbitrage analytics
5. Data quality analytics

## Project Architecture

Raw Data (Snowflake)
        |
        v
Staging Models (dbt)
        |
        v
Analytics Models
        |
        +---- Spoilage Analytics
        |
        +---- Arbitrage Analytics
        |
        +---- Data Quality Analytics
        |
        v
Snowflake Analytical Views

## Data Source

Database: SNOWFLAKE_LEARNING_DB

Schema: PUBLIC

Main source table:
- RAW_COMMODITY
- RAW_SENSOR

## dbt Models

### 1. Staging Models

- stg_commodity
- stg_customer
- stg_sensor

Purpose:
Clean and prepare raw data for analytics.

### 2. Spoilage Analytics

Model: fct_commodity_spoilage

Purpose:
- Calculate commodity shelf life.
- Calculate days stored.
- Estimate spoilage percentage.
- Calculate spoiled and good quantities.

### 3. Arbitrage Analytics

Model: fct_commodity_arbitrage

Purpose:
- Compare commodity prices across locations.
- Identify lowest buying price.
- Identify highest selling price.
- Calculate price difference.
- Calculate arbitrage percentage.
- Estimate gross profit.

### 4. Data Quality Analytics

Model: fct_commodity_data_quality

Purpose:
- Identify missing commodity information.
- Detect invalid prices.
- Detect invalid quantities.
- Assign data quality status.
- Identify valid records.

## Sample Analytics Results

### Arbitrage Analysis

| Commodity | Buy Location | Sell Location |
|---|---|---|
| Bananas | Trichy | Chennai |
| Mangoes | Chennai | Madurai |
| Rice | Salem | Coimbatore |
| Tomatoes | Salem | Coimbatore |
| Wheat | Madurai | Chennai |

Total sample estimated gross profit: 4800

Note: These are sample/demo values. Transport, taxes and transaction costs are not included.

### Data Quality Results

- Total records: 10
- Valid records: 10
- Invalid records: 0

## How to Run the Project

Run these commands from the project root directory.

### Check dbt connection

```bash
dbt debug
```

### Run all models

```bash
dbt run
```

### Run all tests

```bash
dbt test
```

### Parse dbt project

```bash
dbt parse --no-partial-parse
```

## Testing Results

- Total data tests: 18
- Passed: 18
- Warnings: 0
- Errors: 0

## Project Status

Completed:
- Data staging
- Spoilage analytics
- Arbitrage analytics
- Data quality analytics
- dbt data testing
- Git version control

## Author

Nisha