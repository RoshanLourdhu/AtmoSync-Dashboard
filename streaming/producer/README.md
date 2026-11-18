# AtmoSync – Kafka Sensor Producer (`streaming/producer/`)

This module implements the production Kafka producer (`SensorKafkaProducer`) responsible for publishing schema-validated container telemetry from the IoT sensor simulator into Kafka topic `container-sensor-data`.

---

## 1. Core Reliability Guarantees

1. **Pre-Send Schema Validation:** Enforces `shared/schemas/sensor_reading.schema.json` before serializing or touching Kafka (`validate_before_send=True`). Non-conforming events are rejected and counted in `validation_errors`.
2. **Partition Ordering:** Derives stable UTF-8 partition keys from `container_id` so all telemetry for a given container arrives in strict chronological order within a single partition.
3. **Broker Delivery Confirmation & Metrics (`ProducerStats`):**
   - `produced`: Events queued into the local `confluent-kafka` producer buffer.
   - `delivered`: Events asynchronously confirmed by broker delivery callbacks (`err is None`).
   - `delivery_failed`: Events rejected or timed out by the broker (`err is not None`).
   - `produce_failed`: Events that failed local queuing after exhausting bounded retries.
   - `pending`: Undelivered messages remaining after `flush(timeout)`.
   - `in_flight`: Queued events awaiting broker acknowledgement (`produced - delivered - delivery_failed`).
4. **Bounded Retry for Transient Errors:** Automatically retries transient local buffer saturation (`BufferError`) or retriable Kafka client exceptions up to `max_retries` times, polling the local buffer between attempts to drain in-flight messages without generating duplicates.
5. **Fault Isolation in Batches:** `produce_batch(..., continue_on_error=True)` isolates individual schema or produce errors so one bad event does not abort remaining valid events in the batch, and always flushes queued events.
6. **Graceful Shutdown (`close` & `stream_simulation`):**
   - Sets `is_closed = True` to immediately reject new events (`KafkaProducerError`).
   - Flushes all pending messages within `flush_timeout_seconds`.
   - Reports any undelivered pending count and logs final delivery statistics.

---

## 2. Environment Configuration (`ProducerConfig`)

| Environment Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated Kafka broker addresses |
| `KAFKA_TOPIC_SENSOR_DATA` | `container-sensor-data` | Target Kafka topic name |
| `KAFKA_PRODUCER_CLIENT_ID` | `atmosync-sensor-producer` | Logical producer client identifier |
| `KAFKA_PRODUCER_ACKS` | `all` | Broker acknowledgement mode (`all`, `1`, `0`) |
| `KAFKA_PRODUCER_VALIDATE` | `true` | Enable pre-send JSON Schema validation |
| `KAFKA_PRODUCER_FLUSH_TIMEOUT_SECONDS` | `10.0` | Maximum seconds to wait during `flush()` / `close()` |
| `KAFKA_PRODUCER_MAX_RETRIES` | `3` | Maximum bounded retry attempts for transient buffer errors |
| `KAFKA_PRODUCER_RETRY_BACKOFF_SECONDS` | `0.2` | Buffer poll wait duration between retries |

---

## 3. Usage Example

```python
import threading
from sensor_simulator.simulator import SensorSimulator
from streaming.producer import ProducerConfig, SensorKafkaProducer

config = ProducerConfig.from_env()
producer = SensorKafkaProducer(config=config)
simulator = SensorSimulator()

stop_event = threading.Event()
stats = producer.stream_simulation(
    simulator,
    max_cycles=10,
    interval_seconds=1.0,
    stop_event=stop_event,
)
print(stats.to_dict())
```

---

## 4. Automated Testing & Live Boundaries

Run the producer unit and integration test suite offline (mocked Kafka broker):

```bash
python -m pytest -v tests/test_kafka_producer.py
```

- **Offline Verification:** Delivery callbacks, failure tracking, bounded retries, flush timeouts, batch fault isolation, and graceful shutdown are verified via mocked tests.
- **Live Kafka Verification:** End-to-end broker acknowledgements require an active Kafka broker reachable at `KAFKA_BOOTSTRAP_SERVERS` with topic `container-sensor-data`.
