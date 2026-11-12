import time
import json
import random
from datetime import datetime, timezone
from kafka import KafkaProducer

KAFKA_TOPIC = "atmosync_sensor_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

CONTAINERS = [
    {"container_id": "C001", "commodity": "Avocados", "origin": "Chennai", "destination": "Bengaluru", "quantity_kg": 1000, "temp_target": 5.5, "temp_max": 8.0},
    {"container_id": "C002", "commodity": "Mangoes", "origin": "Chennai", "destination": "Hyderabad", "quantity_kg": 850, "temp_target": 8.0, "temp_max": 10.5},
    {"container_id": "C003", "commodity": "Bananas", "origin": "Coimbatore", "destination": "Bengaluru", "quantity_kg": 1200, "temp_target": 13.0, "temp_max": 15.0},
]

state = {
    "C001": {"temperature_c": 5.5, "humidity_pct": 85.0, "vibration_g": 0.15},
    "C002": {"temperature_c": 8.0, "humidity_pct": 78.0, "vibration_g": 0.12},
    "C003": {"temperature_c": 13.0, "humidity_pct": 82.0, "vibration_g": 0.18},
}

def clamp(value, low, high):
    return max(low, min(high, value))

def generate_event(container, event_id_num):
    cid = container["container_id"]
    st = state[cid]

    # Mean-reverting environmental drift + sensor noise
    st["temperature_c"] += (container["temp_target"] - st["temperature_c"]) * 0.15 + random.uniform(-0.25, 0.3)
    st["humidity_pct"] += random.uniform(-0.6, 0.6)
    st["vibration_g"] += (0.15 - st["vibration_g"]) * 0.25 + random.uniform(-0.02, 0.03)

    # Inject realistic transport anomalies
    anomaly = random.choices(
        ["NONE", "TEMP_SPIKE", "HIGH_VIBRATION", "SENSOR_FAULT"],
        weights=[82, 9, 6, 3]
    )[0]

    if anomaly == "TEMP_SPIKE":
        st["temperature_c"] += random.uniform(2.5, 4.5)
    elif anomaly == "HIGH_VIBRATION":
        st["vibration_g"] = random.uniform(0.32, 0.55)

    st["temperature_c"] = round(clamp(st["temperature_c"], 1.0, 25.0), 2)
    st["humidity_pct"] = round(clamp(st["humidity_pct"], 45.0, 98.0), 2)
    st["vibration_g"] = round(clamp(st["vibration_g"], 0.02, 1.0), 3)

    if anomaly == "SENSOR_FAULT":
        status = "FAULT"
    elif st["temperature_c"] > container["temp_max"] or st["vibration_g"] > 0.28:
        status = "ALERT"
    else:
        status = "NORMAL"

    return {
        "event_id": f"EV{event_id_num:06d}",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "container_id": cid,
        "commodity": container["commodity"],
        "origin": container["origin"],
        "destination": container["destination"],
        "quantity_kg": container["quantity_kg"],
        "temperature_c": st["temperature_c"],
        "humidity_pct": st["humidity_pct"],
        "vibration_g": st["vibration_g"],
        "sensor_status": status,
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    print(f"Streaming improved telemetry to '{KAFKA_TOPIC}'...")
    event_counter = 1
    try:
        while True:
            for container in CONTAINERS:
                event = generate_event(container, event_counter)
                producer.send(KAFKA_TOPIC, value=event)
                print(f"[{event['sensor_status']}] {event['event_id']} | {event['container_id']} | Temp: {event['temperature_c']}C | Vib: {event['vibration_g']}g")
                event_counter += 1
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nSimulator stopped.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()