"""
AtmoSync real-time mock IoT sensor simulator
Generates one sensor event per container every INTERVAL_SECONDS.
Writes events to console and appends them to sensor_data.csv.
Stop with Ctrl+C.
"""
import csv
import random
import time
import json
from kafka import KafkaProducer
from datetime import datetime, timezone
from pathlib import Path

OUTPUT_FILE = Path("sensor_data.csv")
INTERVAL_SECONDS = 2

KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
KAFKA_TOPIC = "atmosync-sensor-data"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
CONTAINERS = [
    {"container_id": "C001", "commodity": "Avocados", "origin": "Chennai", "destination": "Bengaluru", "quantity_kg": 1000},
    {"container_id": "C002", "commodity": "Mangoes", "origin": "Chennai", "destination": "Hyderabad", "quantity_kg": 850},
    {"container_id": "C003", "commodity": "Bananas", "origin": "Coimbatore", "destination": "Bengaluru", "quantity_kg": 1200},
]
FIELDS = [
    "event_id", "timestamp_utc", "container_id", "commodity", "origin",
    "destination", "quantity_kg", "temperature_c", "humidity_pct",
    "vibration_g", "sensor_status"
]

# Starting conditions; C001 occasionally experiences a temperature spike
state = {
    "C001": {"temperature_c": 5.5, "humidity_pct": 85.0, "vibration_g": 0.15},
    "C002": {"temperature_c": 8.0, "humidity_pct": 78.0, "vibration_g": 0.12},
    "C003": {"temperature_c": 13.0, "humidity_pct": 82.0, "vibration_g": 0.18},
}

def clamp(value, low, high):
    return max(low, min(high, value))

def main():
    file_exists = OUTPUT_FILE.exists()
    with OUTPUT_FILE.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        if not file_exists or OUTPUT_FILE.stat().st_size == 0:
            writer.writeheader()

        event_num = 1
        print(f"AtmoSync simulator running; writing to {OUTPUT_FILE.resolve()}")
        print("Press Ctrl+C to stop.")
        try:
            while True:
                now = datetime.now(timezone.utc).isoformat(timespec="seconds")
                for container in CONTAINERS:
                    cid = container["container_id"]
                    s = state[cid]

                    # Small natural sensor variation
                    s["temperature_c"] += random.uniform(-0.25, 0.25)
                    s["humidity_pct"] += random.uniform(-1.2, 1.2)
                    s["vibration_g"] += random.uniform(-0.04, 0.04)

                    # Simulated abnormal event for C001 about every 30 seconds
                    if cid == "C001" and event_num % 15 == 0:
                        s["temperature_c"] += random.uniform(1.5, 3.0)
                        s["humidity_pct"] += random.uniform(2.0, 5.0)

                    s["temperature_c"] = round(clamp(s["temperature_c"], -2, 30), 2)
                    s["humidity_pct"] = round(clamp(s["humidity_pct"], 35, 100), 2)
                    s["vibration_g"] = round(clamp(s["vibration_g"], 0.01, 2.0), 3)

                    status = "ALERT" if s["temperature_c"] > 8 or s["humidity_pct"] > 92 or s["vibration_g"] > 0.8 else "NORMAL"
                    row = {
                        "event_id": f"EV{event_num:06d}",
                        "timestamp_utc": now,
                        **container,
                        "temperature_c": s["temperature_c"],
                        "humidity_pct": s["humidity_pct"],
                        "vibration_g": s["vibration_g"],
                        "sensor_status": status,
                    }
                    writer.writerow(row)
                    f.flush()

                    producer.send(KAFKA_TOPIC, row)
                    producer.flush()

                    print("Published to Kafka:", row)
                    event_num += 1
                time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            print(f"\nStopped. Data saved in {OUTPUT_FILE.resolve()}")

if __name__ == "__main__":
    main()
