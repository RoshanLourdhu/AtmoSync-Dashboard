# AtmoSync – Kafka Streaming Pipeline: Producer & Consumer Integration

This directory contains the production Apache Kafka streaming integration for the **AtmoSync Dashboard** real-time IoT monitoring platform.

---

## 1. End-to-End Pipeline Architecture

The streaming pipeline connects refrigerated IoT container telemetry to Snowflake RAW batch storage:

```text
+-------------------------------------------------------------------+
|               Sensor Simulator (`sensor_simulator/`)              |
|  - Continuous physics drift (mean-reverting temperature/humidity) |
|  - Transport anomalies (TEMP_SPIKE, HIGH_VIBRATION, SENSOR_FAULT) |
|  - Schema standardization (conforms to sensor_reading.schema.json)|
+-------------------------------------------------------------------+
                                  |
                                  v
+-------------------------------------------------------------------+
|               Kafka Producer Service (`streaming/producer/`)       |
|  - Pre-send schema validation (`shared.validation`)               |
|  - JSON serialization & container_id key partitioning            |
|  - Asynchronous delivery callbacks & batch flushing               |
+-------------------------------------------------------------------+
                                  |
                                  | Publishes to topic 'container-sensor-data'
                                  v
+-------------------------------------------------------------------+
|               Apache Kafka Broker (`localhost:9092`)              |
|  - Topic: container-sensor-data                                   |
|  - Partitioned by: container_id                                   |
+-------------------------------------------------------------------+
                                  |
                                  | Consumed by consumer group 'atmosync-sensor-consumer'
                                  v
+-------------------------------------------------------------------+
|               Kafka Consumer Service (`streaming/consumer/`)       |
|  - Safe JSON deserialization & malformed payload quarantine       |
|  - Schema contract enforcement (`validate_sensor_event`)          |
|  - High-throughput batch aggregation (default: 10 records)        |
|  - Transactional RAW loading (`snowflake.ingestion`)              |
|  - Synchronous offset commit ONLY after Snowflake insert succeeds  |
+-------------------------------------------------------------------+
                                  |
                                  | insert_sensor_events(events=[(event, topic, part, offset)])
                                  v
+-------------------------------------------------------------------+
| Snowflake RAW Layer: ATMOSYNC_DB.RAW.SENSOR_READINGS              |
|  - EVENT_PAYLOAD (VARIANT)                                        |
|  - INGESTED_AT (TIMESTAMP_TZ)                                     |
|  - KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET                     |
+-------------------------------------------------------------------+
```

---

## 2. Standardized Sensor Event Contract

The official JSON schema is defined at [shared/schemas/sensor_reading.schema.json](file:///D:/Infotach%20Internship/Project-1-AtmoSync/shared/schemas/sensor_reading.schema.json).

### Field Mapping from Simulator Prototype to Official Schema:
| Sam's Prototype Field | Official Schema Field | Type | Description |
| :--- | :--- | :--- | :--- |
| `container_id` | `container_id` | `string` | Monitored refrigerated container identifier (e.g. `C001`) |
| *(Missing)* | `shipment_id` | `string` | Unique shipment identifier (e.g. `SHIP-1001`) |
| `commodity` | `commodity` | `string` | Monitored perishable commodity (`Avocados`, `Mangoes`, `Bananas`) |
| `temperature_c` | `temperature` | `number` | Continuous container temperature in Celsius |
| `humidity_pct` | `humidity` | `number` | Relative humidity (0 to 100) |
| `vibration_g` | `vibration` | `number` | Vibration acceleration magnitude ($\ge 0$) |
| `timestamp_utc` | `timestamp` | `string` | UTC ISO 8601 / RFC 3339 formatted timestamp (e.g. `2026-11-13T12:00:00Z`) |
| `event_id`, `origin`, `destination`, `quantity_kg`, `sensor_status` | *(Omitted)* | - | Omitted because official schema specifies `"additionalProperties": false` |

---

## 3. Configuration & Environment Variables

Both producer and consumer read from `.env` (or environment variables) without hardcoding secrets:

| Variable | Default | Component | Purpose |
| :--- | :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Producer & Consumer | Kafka broker bootstrap list |
| `KAFKA_TOPIC_SENSOR_DATA` | `container-sensor-data` | Producer & Consumer | Telemetry topic for sensor events |
| `KAFKA_PRODUCER_CLIENT_ID` | `atmosync-sensor-producer` | Producer | Client identifier for producer |
| `KAFKA_PRODUCER_ACKS` | `all` | Producer | Broker acknowledgement reliability level |
| `KAFKA_CONSUMER_GROUP` | `atmosync-sensor-consumer` | Consumer | Consumer group identifier |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Consumer | Offset strategy for fresh consumer groups |
| `KAFKA_BATCH_SIZE` | `10` | Consumer | Maximum batch size before flushing to Snowflake |
| `KAFKA_POLL_TIMEOUT_SECONDS` | `1.0` | Consumer | Message poll timeout |

---

## 4. Usage Examples

### A. Producing Simulated Sensor Telemetry
```python
from sensor_simulator import SensorSimulator
from streaming.producer import SensorKafkaProducer

# Initialize simulator and producer
simulator = SensorSimulator()
producer = SensorKafkaProducer()

try:
    # Generate schema-compliant events and publish batch to Kafka
    events = simulator.generate_batch()
    queued_count = producer.produce_batch(events)
    print(f"Queued and flushed {queued_count} events to topic 'container-sensor-data'")
finally:
    producer.close()
```

### B. Consuming Messages and Ingesting into Snowflake
```python
from streaming.consumer import SensorKafkaConsumer

# Initialize consumer from environment variables
consumer = SensorKafkaConsumer()

try:
    # Poll and ingest a batch into Snowflake
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

All tests execute fully offline with mocked Kafka brokers and mocked Snowflake sessions:

```powershell
# Run Kafka producer and simulator unit tests
python -m pytest -v tests/test_kafka_producer.py

# Run Kafka consumer unit tests
python -m pytest -v tests/test_kafka_consumer.py

# Run complete test suite (65 tests)
python -m pytest -v
```
