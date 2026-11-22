# AtmoSync – dbt Data Transformation & Analytics

This repository contains the dbt transformation layer for **AtmoSync: Micro-Climate Arbitrage Analytics**, built on top of the **Snowflake Data Cloud**.

The dbt project ingests raw IoT sensor telemetry, customer benchmarks, and commodity market data, providing clean staging models, data quality assertions, and analytical data marts.

---

## 1. Project Directory Structure

```text
AtmoSync-Nisha-Workspace/
├── .gitignore                      # Excludes profiles.yml, .env, target/, and logs
├── README.md                       # Comprehensive project documentation
├── dbt_project.yml                 # Main dbt project configuration (profile: my_dbt_project)
├── profiles.yml.example            # Template for ~/.dbt/profiles.yml (credential-free)
├── data_cleaning.ipynb             # Exploratory analysis and CSV preprocessing notebook
├── models/
│   ├── example/                    # Starter dbt models
│   │   ├── my_first_dbt_model.sql
│   │   ├── my_second_dbt_model.sql
│   │   └── schema.yml              # Starter model tests
│   ├── staging/                    # Staging views (materialized as view)
│   │   ├── sources.yml             # Snowflake source declarations
│   │   ├── schema.yml              # Column documentation and schema tests
│   │   ├── stg_commodity.sql       # Commodity inventory and pricing staging
│   │   ├── stg_customer.sql        # Customer dimension staging (from TPCH_SF1)
│   │   └── stg_sensor.sql          # IoT telemetry staging with deduplication
│   └── marts/                      # Analytical marts (materialized as table)
│       ├── fct_commodity_condition.sql # Pricing and stock status classifications
│       └── schema.yml              # Fact mart data quality tests
└── tests/                          # Singular SQL data quality assertions
    ├── assert_commodity_id_unique.sql
    ├── assert_commodity_price_positive.sql
    └── assert_commodity_quantity_positive.sql
```

---

## 2. Snowflake Profile Configuration (`profiles.yml`)

The project expects a profile named **`my_dbt_project`** inside your home dbt directory:
- **WSL / Linux:** `/home/nisha/.dbt/profiles.yml`
- **Windows:** `%USERPROFILE%\.dbt\profiles.yml`

### Setup in WSL (Copy-Paste Command)

Run the following command inside your WSL terminal to generate a safe template:

```bash
mkdir -p ~/.dbt

cat << 'EOF' > ~/.dbt/profiles.yml
my_dbt_project:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "YOUR_ACCOUNT_IDENTIFIER"     # e.g., xy12345.us-east-1
      user: "YOUR_SNOWFLAKE_USER"
      password: "YOUR_SNOWFLAKE_PASSWORD"
      role: "ACCOUNTADMIN"                   # Or SYSADMIN / ATMOSYNC_INGEST_ROLE
      database: "SNOWFLAKE_LEARNING_DB"      # Or ATMOSYNC_DB
      warehouse: "COMPUTE_WH"                # Or ATMOSYNC_WH
      schema: "PUBLIC"                       # Or ANALYTICS
      threads: 4
      client_session_keep_alive: False
EOF
```

> [!CAUTION]
> **Credential Security Notice:**
> If any Snowflake password was previously shared in terminal logs or screenshots, treat it as compromised and request an immediate credential rotation from the account administrator before proceeding. Never commit `profiles.yml` or `.env` to Git.

---

## 3. Environment Variable Support

As best practice, you can supply credentials via environment variables:

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"
export SNOWFLAKE_USER="nisha"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
export SNOWFLAKE_DATABASE="SNOWFLAKE_LEARNING_DB"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_SCHEMA="PUBLIC"
```

---

## 4. Models and Business Logic

### Staging Layer (`models/staging/`)
- **`stg_sensor`**: Ingests raw cold chain IoT telemetry from `RAW_SENSOR`, enforcing uppercase column naming, explicit Snowflake data types (`FLOAT`, `TIMESTAMP_TZ`), and deduplicating latest readings per `SENSOR_ID` using `QUALIFY ROW_NUMBER() = 1`.
- **`stg_customer`**: Extracts customer benchmark records from `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER`.
- **`stg_commodity`**: Ingests perishable commodity inventory and market prices from `RAW_COMMODITY` with structured schema and type casting.

### Marts Layer (`models/marts/`)
- **`fct_commodity_condition`**: Computes core business logic:
  - **Price Status**:
    - `>= 100` $\rightarrow$ `High Price`
    - `>= 50` $\rightarrow$ `Medium Price`
    - `< 50` $\rightarrow$ `Low Price`
  - **Stock Status**:
    - `<= 10` $\rightarrow$ `Low Stock`
    - `<= 50` $\rightarrow$ `Medium Stock`
    - `> 50` $\rightarrow$ `Available Stock`

---

## 5. Data Quality and Testing

The project implements both generic schema tests and singular business rule tests:

1. **Generic Schema Tests:**
   - `unique` and `not_null` on `COMMODITY_ID`, `SENSOR_ID`, and `CUSTOMER_ID`.
   - `accepted_values` on `CATEGORY`: `['Fruits', 'Grains', 'Vegetables']`.
   - `accepted_values` on `PRICE_STATUS`: `['High Price', 'Medium Price', 'Low Price']`.
   - `accepted_values` on `STOCK_STATUS`: `['Low Stock', 'Medium Stock', 'Available Stock']`.
2. **Singular SQL Tests (`tests/`):**
   - **`assert_commodity_price_positive.sql`**: Ensures no commodity has a price $\le 0$ or `NULL`.
   - **`assert_commodity_quantity_positive.sql`**: Ensures no inventory item has a quantity $\le 0$ or `NULL`.
   - **`assert_commodity_id_unique.sql`**: Fails if any duplicate `COMMODITY_ID` exists in staging.

---

## 6. Execution Instructions (WSL)

Run these commands inside your project directory in WSL:

```bash
# 1. Verify profile and Snowflake connectivity
dbt debug

# 2. Validate Jinja syntax and model DAG without database execution
dbt parse

# 3. Compile SQL models and assert queries
dbt compile

# 4. Execute transformations against Snowflake
dbt run

# 5. Execute all generic and singular data quality tests
dbt test
```

---

## 7. Known Production Integration Roadmap

- **Production Source Alignment**: Roshan's streaming ingestion pipeline loads IoT events into `ATMOSYNC_DB.RAW.SENSOR_READINGS` with JSON variant payloads (`EVENT_PAYLOAD`). When integrating into production, update `sources.yml` to point to `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
