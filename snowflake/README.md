# AtmoSync Snowflake Data Warehousing

This directory contains the Snowflake data warehouse architecture, Data Definition Language (DDL) scripts, and schema definitions for the **AtmoSync Dashboard** platform.

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
              | SQL INSERT (Batch / Streaming)
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

## 3. How to Execute the DDL Script

The DDL script `snowflake/sql/001_create_raw_sensor_readings.sql` is fully idempotent (`CREATE ... IF NOT EXISTS`). It can be safely executed multiple times.

### Option A: Via Snowflake Snowsight (Web Interface)
1. Log into your Snowflake account via Snowsight.
2. Open a new SQL Worksheet.
3. Select an active role with database creation privileges (e.g., `SYSADMIN` or your project role).
4. Paste the contents of [snowflake/sql/001_create_raw_sensor_readings.sql](file:///D:/Infotach%20Internship/Project-1-AtmoSync/snowflake/sql/001_create_raw_sensor_readings.sql) and click **Run All**.

### Option B: Via SnowSQL CLI
Ensure your connection parameters are configured in `~/.snowsql/config` or use environment variables from `.env`:
```powershell
snowsql -c my_connection -f snowflake/sql/001_create_raw_sensor_readings.sql
```

### Option C: Via Python Script
When runtime infrastructure is provisioned, the script can be applied programmatically using `snowflake-connector-python`:
```python
import snowflake.connector

conn = snowflake.connector.connect(...)
with open("snowflake/sql/001_create_raw_sensor_readings.sql", "r", encoding="utf-8") as f:
    statements = f.read().split(";")
    for stmt in statements:
        if stmt.strip():
            conn.cursor().execute(stmt)
```

---

## 4. Ingestion Mechanism (Future Kafka Consumer Integration)

> [!NOTE]
> **Status:** The DDL script is implemented. The Kafka consumer service (`streaming/consumer/`) is planned and will be implemented in subsequent backend milestones.

Once the streaming consumer is implemented, it will operate as follows:
1. **Poll & Deserialization:** The consumer subscribes to topic `container-sensor-data`.
2. **Schema Validation:** Each JSON record is validated using `shared.validation.validate_sensor_event()`. Malformed records are diverted to a dead-letter queue and excluded from Snowflake.
3. **Batch Insertion:** Validated records are micro-batched and written to Snowflake using parameterized SQL:
   ```sql
   INSERT INTO ATMOSYNC_DB.RAW.SENSOR_READINGS (
       EVENT_PAYLOAD,
       KAFKA_TOPIC,
       KAFKA_PARTITION,
       KAFKA_OFFSET,
       INGESTED_AT
   )
   SELECT
       PARSE_JSON(?),
       ?,
       ?,
       ?,
       CURRENT_TIMESTAMP();
   ```

---

## 5. Downstream Consumption by dbt (`dbt_analytics/`)

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

Downstream models (`int_container_conditions`, `fct_spoilage_risk`, `fct_arbitrage_opportunities`) build directly on this staging model to calculate condition drifts, spoilage degradation, and market rerouting recommendations.

---

## 6. Directory Layout

```text
snowflake/
├── README.md                              # This architecture and operations guide
├── schemas/                               # Reserved for detailed schema definitions / YAML
└── sql/
    └── 001_create_raw_sensor_readings.sql # Idempotent DDL script for database, schema, and raw table
```

