# AtmoSync Cold-Chain IoT Telemetry Pipeline

Real-time cold-chain container telemetry ingestion, anomaly simulation, schema validation, and throughput benchmarking built with Python and Apache Kafka.

## Architecture & Repository Layout

- **`kafka_producer.py`**: Validated `KafkaProducer` factory with `acks`, `retries`, synchronous/asynchronous delivery callbacks, timeout handling, and custom exceptions (`ProducerConfigError`, `ProducerDeliveryError`).
- **`atmosync_sensor_simulator.py`**: Cold-chain IoT telemetry generator for containers `C001` (Avocados), `C002` (Mangoes), and `C003` (Bananas) supporting stateful anomaly scenarios (`COOLING_FAILURE`, `DOOR_OPEN`, `ROUGH_TRANSIT`, `SENSOR_DROPOUT`), `--dry-run`, and `--csv-out` persistence.
- **`kafka_consumer.py`**: `KafkaConsumer` subscriber and 12-field schema validator that detects `ALERT`/`FAULT` anomalies and tracks invalid payloads.
- **`kafka_throughput_test.py`**: Configurable producer throughput benchmark (`events/sec` and partial delivery failure tracking).
- **`sensor_data.csv`**: Historical 12-column cold-chain telemetry dataset aligned to the live event contract.
- **`tests/`**: Automated `unittest` unit and integration suite (35 tests across 7 modules).

## 12-Field Telemetry Event Contract

Every JSON message (keyed by `container_id`) and CSV row adheres to the following 12 fields:

| Field | Type | Description / Allowed Values |
| :--- | :--- | :--- |
| `event_id` | `string` | Sequential event identifier (e.g., `EV000001`) |
| `timestamp_utc` | `string` | ISO-8601 UTC timestamp |
| `container_id` | `string` | Container partition key (`C001`, `C002`, `C003`) |
| `commodity` | `string` | Cargo type (`Avocados`, `Mangoes`, `Bananas`) |
| `origin` | `string` | Dispatch city (`Chennai`, `Coimbatore`) |
| `destination` | `string` | Arrival city (`Bengaluru`, `Hyderabad`) |
| `quantity_kg` | `int` | Shipment weight in kilograms |
| `temperature_c` | `float` | Internal container temperature (°C) |
| `humidity_pct` | `float` | Relative humidity (%) |
| `vibration_g` | `float` | Transit shock/vibration (`g`) |
| `sensor_status` | `string` | `NORMAL`, `WARNING`, `ALERT`, or `FAULT` |
| `anomaly_type` | `string` | `NONE`, `COOLING_FAILURE`, `DOOR_OPEN`, `ROUGH_TRANSIT`, or `SENSOR_DROPOUT` |

## Quickstart & CLI Usage

### 1. Run Offline Dry-Run Checks (No Broker Required)
```powershell
python atmosync_sensor_simulator.py --dry-run --max-events 5 --interval 0
python kafka_consumer.py --dry-run --max-messages 5
python kafka_throughput_test.py --dry-run --num-events 100

### 2. SQLite Persistence, dbt Staging Export & Live Dashboard (Ports 8000 & 3000)
`powershell
python .\kafka_consumer.py --dry-run --max-messages 20 --db-out .\atmosync_telemetry.db --dbt-csv-out .\dbt_staging_commodities.csv
python .\telemetry_sink.py --db .\atmosync_telemetry.db --serve --port 8000
node .\dashboard\server.js
`

### 3. Live Local Apache Kafka Streaming (Optional)
`powershell
python .\kafka_consumer.py --bootstrap-servers 127.0.0.1:9092 --topic atmosync_sensor_events --db-out .\atmosync_telemetry.db --dbt-csv-out .\dbt_staging_commodities.csv
`
