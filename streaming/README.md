# AtmoSync – Kafka Streaming Architecture & Consumer Service

This directory contains the Apache Kafka streaming integration for the **AtmoSync Dashboard** real-time IoT monitoring platform.

---

## 1. Architectural Role

The **Sensor Kafka Consumer Service** acts as the high-throughput bridge between edge sensor telemetry on Kafka and the Snowflake analytical data warehouse:

```text
+-----------------------------+
|   IoT Container Sensors     |
|   (Simulators & Producers)  |
+-----------------------------+
               |
               | (JSON payload on topic 'container-sensor-data')
               v
+--------------------------------------------------------------+
|               Kafka Consumer Service (`streaming/consumer`)  |
|  1. Safe JSON deserialization                                |
|  2. Schema enforcement (`shared.validation`)                 |
|  3. Safe isolation of malformed / non-conforming events      |
|  4. Batch accumulation (default: 10 records)                 |
|  5. Transactional Snowflake loading (`snowflake.ingestion`)  |
|  6. Synchronous offset commit ONLY upon insertion success    |
+--------------------------------------------------------------+
               |
               | insert_sensor_events(events=[(payload, topic, part, offset)])
               v
+--------------------------------------------------------------+
| Snowflake RAW Layer: ATMOSYNC_DB.RAW.SENSOR_READINGS         |
|  - EVENT_PAYLOAD (VARIANT)                                   |
|  - INGESTED_AT (TIMESTAMP_TZ)                                |
|  - KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET                |
+--------------------------------------------------------------+
```

---

## 2. Technical Capabilities

- **Strict Schema Enforcement:** Validates every incoming event against `shared/schemas/sensor_reading.schema.json` via `validate_sensor_event()`.
- **Safe Malformed Handling:** Invalid payloads (such as malformed JSON or unaligned contracts) are quarantined with topic, partition, offset, and error reason. Valid records continue through the ingestion pipeline.
- **Atomic Batch Ingestion:** Valid events are grouped into batches and ingested into `ATMOSYNC_DB.RAW.SENSOR_READINGS` using `snowflake.ingestion.insert_sensor_events`.
- **Lossless Offset Management:** Kafka offsets are committed synchronously (`commit(asynchronous=False)`) **only after** Snowflake acknowledges successful transaction commit. If database ingestion fails, offsets are **not committed**, guaranteeing zero message loss and supporting replay.
- **Flexible Dependency Injection:** Fully supports injected mock consumers and Snowflake connections for fast, offline unit testing.

---

## 3. Configuration & Environment Variables

The consumer service is configured via `.env` (or environment variables):

| Variable | Default | Purpose |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker bootstrap list |
| `KAFKA_TOPIC_SENSOR_DATA` | `container-sensor-data` | Telemetry topic for container sensor readings |
| `KAFKA_CONSUMER_GROUP` | `atmosync-sensor-consumer` | Consumer group identifier |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset strategy when no prior commit exists |
| `KAFKA_BATCH_SIZE` | `10` | Maximum messages accumulated per database batch |
| `KAFKA_POLL_TIMEOUT_SECONDS` | `1.0` | Timeout per message poll in seconds |

---

## 4. Usage Example

```python
from streaming.consumer import SensorKafkaConsumer, ConsumerConfig

# Initialize consumer from environment configuration
consumer = SensorKafkaConsumer()

try:
    # Poll and ingest a single batch of messages
    batch_result = consumer.consume_once(max_records=10)
    print(
        f"Batch ingested: {batch_result.valid_count} valid records, "
        f"{batch_result.invalid_count} rejected, "
        f"Committed: {batch_result.committed}"
    )
finally:
    consumer.close()
```

---

## 5. Automated Testing

All streaming consumer tests run offline using mocked Kafka message interfaces (`tests/test_kafka_consumer.py`):

```powershell
# Run Kafka consumer tests
python -m pytest -v tests/test_kafka_consumer.py

# Run complete test suite (53 tests)
python -m pytest -v
```
