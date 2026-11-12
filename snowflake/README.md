# AtmoSync Snowflake Data Warehousing & Ingestion

This directory contains the Snowflake data warehouse architecture, Data Definition Language (DDL) scripts, database connection management, and Python ingestion engine for the **AtmoSync Dashboard** platform.

---

## 1. Architectural Role of the RAW Layer

The `RAW` schema serves as the immutable landing zone for all event streams ingested into Snowflake. In alignment with modern ELT (Extract, Load, Transform) data engineering practices:

1. **Schema Preservation via VARIANT:** Incoming validated sensor events are stored in their native JSON representation inside a Snowflake `VARIANT` column (`EVENT_PAYLOAD`). This preserves 100% of the raw data fidelity without premature loss of precision or rigid column type casting.
2. **Decoupled Transformations:** Business logic, temperature unit normalization, data cleaning, and spoilage indicators are intentionally excluded from the RAW layer. All transformation logic is owned by downstream dbt models in `dbt_analytics/`.
3. **Auditable Streaming Metadata:** In addition to the raw payload, streaming coordinates (`KAFKA_TOPIC`, `KAFKA_PARTITION`, `KAFKA_OFFSET`) and an ingestion timestamp (`INGESTED_AT TIMESTAMP_TZ`) are recorded for observability, idempotency, and timezone-aware lineage tracking.

```text
+----------------------------+
| Kafka Consumer Service     |
| (streaming/consumer/)      |
| Validates event via schema |
+----------------------------+
              |
              | insert_sensor_event(event, topic, partition, offset)
              v
+-------------------------------------------------------+
| Snowflake RAW Layer: ATMOSYNC_DB.RAW.SENSOR_READINGS   |
| - EVENT_PAYLOAD (VARIANT)                             |
| - KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET          |
| - INGESTED_AT (TIMESTAMP_TZ)                          |
+-------------------------------------------------------+
              |
              | dbt Source: {{ source('raw', 'sensor_readings') }}
              v
+-------------------------------------------------------+
| dbt Analytics Layer (dbt_analytics/)                  |
| - stg_sensor_readings (Extraction & Casting)          |
| - int_container_conditions (Condition history)        |
| - fct_spoilage_risk (Predictive risk modeling)        |
| - fct_arbitrage_opportunities (Market pricing)        |
+-------------------------------------------------------+
```

---

## 2. Target Database, Schema & Table Structure

* **Database:** `ATMOSYNC_DB`
* **Schema:** `RAW`
* **Table:** `ATMOSYNC_DB.RAW.SENSOR_READINGS`
* **DDL Script:** `snowflake/sql/001_create_raw_sensor_readings.sql`

### Table Schema Definition

| Column Name | Snowflake Data Type | Nullable | Default | Description |
|---|---|---|---|---|
| `EVENT_PAYLOAD` | `VARIANT` | **No** | *None* | Raw validated JSON payload conforming to `shared/schemas/sensor_reading.schema.json`. |
| `KAFKA_TOPIC` | `VARCHAR(255)` | **No** | `'container-sensor-data'` | Name of the source Kafka topic. |
| `KAFKA_PARTITION` | `NUMBER(10, 0)` | Yes | *None* | Kafka topic partition ID from which the record was read. |
| `KAFKA_OFFSET` | `NUMBER(38, 0)` | Yes | *None* | Kafka message offset within the partition. |
| `INGESTED_AT` | `TIMESTAMP_TZ` | **No** | `CURRENT_TIMESTAMP()` | Timestamp with timezone recording when the row was inserted into Snowflake, preserving explicit timezone offset information. |

### Payload Structure (`EVENT_PAYLOAD`)
Every record in `EVENT_PAYLOAD` conforms strictly to the v0.1 data contract:
```json
{
  "container_id": "CONT_001",
  "shipment_id": "SHIP_1001",
  "commodity": "Apples",
  "temperature": 8.5,
  "humidity": 75.2,
  "vibration": 0.35,
  "timestamp": "2026-11-05T10:30:00Z"
}
```

---

## 3. Python Connector & Environment Configuration

### Installation
The Snowflake connector is managed via the project root dependencies:
```powershell
pip install -r requirements.txt
```
*(Package: `snowflake-connector-python>=3.5.0`)*

### Environment Variables
Configure the following parameters in your local `.env` file (copied from `.env.example`). **Never commit `.env` or hardcode credentials:**

```ini
# Snowflake Connection Parameters
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=atmosync_svc
SNOWFLAKE_PASSWORD=StrongPasswordHere!
SNOWFLAKE_WAREHOUSE=ATMOSYNC_WH
SNOWFLAKE_DATABASE=ATMOSYNC_DB
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=ATMOSYNC_INGEST_ROLE
```

---

## 4. Python Ingestion Engine (`snowflake/`)

The Python ingestion engine consists of two modules:
1. `snowflake/connection.py`: Secure connection manager reading from environment variables.
2. `snowflake/ingestion.py`: High-throughput, transaction-safe event validator and inserter.

### Key Capabilities:
- **Contract Enforcement:** Validates events against `shared/schemas/sensor_reading.schema.json` prior to initiating database traffic.
- **Batch & Single-Event Ingestion:** Provides `insert_sensor_event` for individual records and `insert_sensor_events` for high-performance atomic batch ingestion using `executemany`.
- **Parameterized SQL:** Utilizes parameter placeholders (`PARSE_JSON(%s)`) preventing SQL injection.
- **Atomic Transactions:** Commits only when the complete batch succeeds; automatically executes `conn.rollback()` on any failure (preventing partial batch inserts).
- **Resource Management:** Closes cursors and connections deterministically in `finally:` blocks.
- **Dependency Injection:** Allows passing an external or mock connection for testing and connection reuse.

### Example Usage: Single-Event Ingestion
```python
from snowflake.ingestion import insert_sensor_event

# Validated IoT reading conforming to sensor_reading.schema.json
sensor_event = {
    "container_id": "C001",
    "shipment_id": "SHIP_1001",
    "commodity": "Avocados",
    "temperature": 5.8,
    "humidity": 86.4,
    "vibration": 0.14,
    "timestamp": "2026-11-11T12:00:00Z",
}

# Ingest event with Kafka streaming coordinates
success = insert_sensor_event(
    event=sensor_event,
    topic="container-sensor-data",
    partition=0,
    offset=12405,
)
print(f"Event ingested successfully: {success}")
```

### Example Usage: Atomic Batch Ingestion
```python
from snowflake.ingestion import insert_sensor_events

# Batch of validated IoT sensor events
batch_events = [
    {
        "container_id": "C001",
        "shipment_id": "SHIP_1001",
        "commodity": "Avocados",
        "temperature": 5.8,
        "humidity": 86.4,
        "vibration": 0.14,
        "timestamp": "2026-11-11T12:00:00Z",
    },
    {
        "container_id": "C002",
        "shipment_id": "SHIP_1002",
        "commodity": "Bananas",
        "temperature": 13.2,
        "humidity": 90.1,
        "vibration": 0.08,
        "timestamp": "2026-11-11T12:00:00Z",
    },
]

# Atomically insert all events in a single transaction
# If any event fails validation or insertion, the entire batch is rolled back
success = insert_sensor_events(batch_events)
print(f"Batch inserted: {success}")
```

---

## 5. Kafka Consumer Integration Architecture

In the upcoming streaming milestone, the Kafka Consumer (`streaming/consumer/consumer.py`) calls `insert_sensor_event` during message polling:

```python
from confluent_kafka import Consumer
from shared.validation import SensorValidationError
from snowflake.ingestion import insert_sensor_event, SnowflakeIngestionError

# Planned consumer integration loop
for message in consumer:
    try:
        raw_event = json.loads(message.value().decode("utf-8"))

        # Ingest directly into Snowflake RAW table
        insert_sensor_event(
            event=raw_event,
            topic=message.topic(),
            partition=message.partition(),
            offset=message.offset(),
        )
    except SensorValidationError as val_err:
        # Route to Dead Letter Queue (DLQ)
        logger.error(f"Malformed sensor event rejected: {val_err}")
    except SnowflakeIngestionError as db_err:
        # Log and handle database retry
        logger.error(f"Database insertion failed: {db_err}")
```

---

## 6. How to Execute the DDL Script

The DDL script `snowflake/sql/001_create_raw_sensor_readings.sql` is fully idempotent (`CREATE ... IF NOT EXISTS`).

### Option A: Via Snowflake Snowsight (Web Interface)
1. Log into your Snowflake account via Snowsight.
2. Open a new SQL Worksheet.
3. Select an active role with database creation privileges (e.g., `SYSADMIN` or your project role).
4. Paste the contents of [snowflake/sql/001_create_raw_sensor_readings.sql](file:///D:/Infotach%20Internship/Project-1-AtmoSync/snowflake/sql/001_create_raw_sensor_readings.sql) and click **Run All**.

### Option B: Via SnowSQL CLI
```powershell
snowsql -c my_connection -f snowflake/sql/001_create_raw_sensor_readings.sql
```

---

## 7. Downstream Consumption by dbt (`dbt_analytics/`)

In the next phase of the project, Nisha's dbt project will consume `ATMOSYNC_DB.RAW.SENSOR_READINGS`.

### Staging Model: `models/staging/stg_sensor_readings.sql`
dbt unpacks the `VARIANT` column into clean, strongly typed relational columns:
```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'sensor_readings') }}
),

staged AS (
    SELECT
        -- Unpack JSON contract fields
        EVENT_PAYLOAD:container_id::STRING        AS container_id,
        EVENT_PAYLOAD:shipment_id::STRING         AS shipment_id,
        EVENT_PAYLOAD:commodity::STRING           AS commodity,
        EVENT_PAYLOAD:temperature::FLOAT          AS temperature,
        EVENT_PAYLOAD:humidity::FLOAT             AS humidity,
        EVENT_PAYLOAD:vibration::FLOAT            AS vibration,
        EVENT_PAYLOAD:timestamp::TIMESTAMP_TZ     AS recorded_at,

        -- Ingestion and stream lineage
        KAFKA_TOPIC                               AS kafka_topic,
        KAFKA_PARTITION                           AS kafka_partition,
        KAFKA_OFFSET                              AS kafka_offset,
        INGESTED_AT                               AS ingested_at
    FROM source
)

SELECT * FROM staged
```

---

## 8. Testing Strategy: Mocked vs. Live Integration

> [!IMPORTANT]
> **Mocked Unit Tests vs. Live Warehouse Verification:**
> The automated test suites (`tests/test_snowflake_ingestion.py` and `tests/test_snowflake_batch_ingestion.py`) use mocked Snowflake sessions (`unittest.mock.MagicMock`). They rigorously verify single-event insertion, batch insertion, SQL parameter binding, pre-validation rejection, atomic transaction commits, transaction rollbacks, resource cleanup, and error propagation **without requiring actual Snowflake credentials or network connectivity**.
> Live database execution occurs when environment variables point to an active Snowflake account and `001_create_raw_sensor_readings.sql` has been executed.

### Running the Test Suite:
```powershell
# Run single & batch Snowflake ingestion unit tests
python -m pytest -v tests/test_snowflake_ingestion.py tests/test_snowflake_batch_ingestion.py

# Run all project test suites (42 automated tests)
python -m pytest -v
```

---

## 9. Directory Layout

```text
snowflake/
├── __init__.py                            # Package exports
├── connection.py                          # Snowflake connection manager
├── ingestion.py                           # Sensor event validation & RAW insertion engine
├── README.md                              # This architecture and operations guide
├── schemas/                               # Reserved for detailed schema definitions / YAML
└── sql/
    └── 001_create_raw_sensor_readings.sql # Idempotent DDL script for database, schema, and raw table
```
