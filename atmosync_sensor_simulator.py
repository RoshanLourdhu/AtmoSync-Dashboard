import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_TOPIC = "atmosync_sensor_events"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

CONTAINERS = [
    {
        "container_id": "C001",
        "commodity": "Avocados",
        "origin": "Chennai",
        "destination": "Bengaluru",
        "quantity_kg": 4200,
        "temp_target": 5.5,
        "temp_max": 7.5,
        "humidity_target": 85.0,
        "vibration_max": 0.35,
    },
    {
        "container_id": "C002",
        "commodity": "Mangoes",
        "origin": "Chennai",
        "destination": "Hyderabad",
        "quantity_kg": 5000,
        "temp_target": 8.0,
        "temp_max": 10.5,
        "humidity_target": 78.0,
        "vibration_max": 0.35,
    },
    {
        "container_id": "C003",
        "commodity": "Bananas",
        "origin": "Coimbatore",
        "destination": "Bengaluru",
        "quantity_kg": 4800,
        "temp_target": 13.0,
        "temp_max": 15.0,
        "humidity_target": 82.0,
        "vibration_max": 0.35,
    },
]

state = {
    "C001": {"temperature_c": 5.5, "humidity_pct": 85.0, "vibration_g": 0.15},
    "C002": {"temperature_c": 8.0, "humidity_pct": 78.0, "vibration_g": 0.12},
    "C003": {"temperature_c": 13.0, "humidity_pct": 82.0, "vibration_g": 0.18},
}

SCENARIO_DURATIONS = {
    "COOLING_FAILURE": 6,
    "DOOR_OPEN": 4,
    "ROUGH_TRANSIT": 5,
    "SENSOR_DROPOUT": 3,
}

active_scenarios = {
    "C001": {"type": "NONE", "remaining": 0},
    "C002": {"type": "NONE", "remaining": 0},
    "C003": {"type": "NONE", "remaining": 0},
}


def clamp(value, low, high):
    return max(low, min(high, value))


def _resolve_anomaly(cid, forced_scenario=None, target_container="ALL", anomaly_rate=0.18):
    current = active_scenarios[cid]

    if forced_scenario and forced_scenario != "NONE":
        if target_container in ("ALL", cid):
            if current["type"] != forced_scenario or current["remaining"] <= 0:
                active_scenarios[cid] = {
                    "type": forced_scenario,
                    "remaining": SCENARIO_DURATIONS[forced_scenario],
                }
            active_scenarios[cid]["remaining"] -= 1
            return forced_scenario
        return "NONE"

    if current["remaining"] > 0:
        current["remaining"] -= 1
        active_type = current["type"]
        if current["remaining"] == 0:
            active_scenarios[cid] = {"type": "NONE", "remaining": 0}
        return active_type

    if target_container in ("ALL", cid) and random.random() < anomaly_rate:
        chosen = random.choices(
            ["COOLING_FAILURE", "DOOR_OPEN", "ROUGH_TRANSIT", "SENSOR_DROPOUT"],
            weights=[35, 25, 25, 15],
        )[0]
        active_scenarios[cid] = {
            "type": chosen,
            "remaining": SCENARIO_DURATIONS[chosen] - 1,
        }
        return chosen

    return "NONE"


def generate_event(
    container,
    event_id_num,
    forced_scenario=None,
    target_container="ALL",
    anomaly_rate=0.18,
):
    cid = container["container_id"]
    st = state[cid]
    anomaly = _resolve_anomaly(cid, forced_scenario, target_container, anomaly_rate)

    if anomaly == "COOLING_FAILURE":
        st["temperature_c"] = clamp(
            st["temperature_c"] + random.uniform(0.8, 1.4), -5.0, 35.0
        )
        st["humidity_pct"] = clamp(
            st["humidity_pct"] + random.uniform(-0.4, 0.5), 40.0, 100.0
        )
        st["vibration_g"] = clamp(
            st["vibration_g"] + (0.15 - st["vibration_g"]) * 0.25 + random.uniform(-0.02, 0.02),
            0.01,
            2.0,
        )
    elif anomaly == "DOOR_OPEN":
        st["temperature_c"] = clamp(
            st["temperature_c"] + random.uniform(0.7, 1.2), -5.0, 35.0
        )
        st["humidity_pct"] = clamp(
            st["humidity_pct"] + random.uniform(2.0, 3.5), 40.0, 100.0
        )
        st["vibration_g"] = clamp(
            st["vibration_g"] + random.uniform(-0.01, 0.03), 0.01, 2.0
        )
    elif anomaly == "ROUGH_TRANSIT":
        st["temperature_c"] = clamp(
            st["temperature_c"]
            + (container["temp_target"] - st["temperature_c"]) * 0.15
            + random.uniform(-0.2, 0.25),
            -5.0,
            35.0,
        )
        st["humidity_pct"] = clamp(
            st["humidity_pct"]
            + (container["humidity_target"] - st["humidity_pct"]) * 0.1
            + random.uniform(-0.6, 0.6),
            40.0,
            100.0,
        )
        st["vibration_g"] = round(random.uniform(0.40, 0.65), 3)
    else:
        st["temperature_c"] = clamp(
            st["temperature_c"]
            + (container["temp_target"] - st["temperature_c"]) * 0.2
            + random.uniform(-0.25, 0.25),
            -5.0,
            35.0,
        )
        st["humidity_pct"] = clamp(
            st["humidity_pct"]
            + (container["humidity_target"] - st["humidity_pct"]) * 0.15
            + random.uniform(-0.6, 0.6),
            40.0,
            100.0,
        )
        st["vibration_g"] = clamp(
            st["vibration_g"]
            + (0.15 - st["vibration_g"]) * 0.3
            + random.uniform(-0.02, 0.03),
            0.01,
            2.0,
        )

    temp_out = -99.9 if anomaly == "SENSOR_DROPOUT" else round(st["temperature_c"], 2)
    hum_out = -1.0 if anomaly == "SENSOR_DROPOUT" else round(st["humidity_pct"], 2)
    vib_out = -1.0 if anomaly == "SENSOR_DROPOUT" else round(st["vibration_g"], 3)

    if anomaly == "SENSOR_DROPOUT":
        status = "FAULT"
    elif (
        temp_out > container["temp_max"]
        or vib_out > container["vibration_max"]
    ):
        status = "ALERT"
    elif temp_out > (container["temp_max"] - 0.8) or anomaly != "NONE":
        status = "WARNING"
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
        "temperature_c": temp_out,
        "humidity_pct": hum_out,
        "vibration_g": vib_out,
        "sensor_status": status,
        "anomaly_type": anomaly,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="AtmoSync IoT Cold-Chain Sensor Simulator"
    )
    parser.add_argument(
        "--scenario",
        choices=["random", "cooling_failure", "door_open", "rough_transit", "sensor_dropout"],
        default="random",
        help="Force a specific abnormal scenario or use random injection (default: random)",
    )
    parser.add_argument(
        "--container",
        choices=["ALL", "C001", "C002", "C003"],
        default="ALL",
        help="Target container for scenario injection (default: ALL)",
    )
    parser.add_argument(
        "--anomaly-rate",
        type=float,
        default=0.18,
        help="Probability of triggering a new scenario per tick in random mode (default: 0.18)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Seconds between events (default: 2.0)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after N events; 0 runs continuously (default: 0)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    forced = None if args.scenario == "random" else args.scenario.upper()
    producer = None
    event_counter = 1

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
        )
        print(
            f"Streaming telemetry to '{KAFKA_TOPIC}' "
            f"(scenario={args.scenario}, target={args.container})..."
        )

        while args.max_events == 0 or event_counter <= args.max_events:
            container = CONTAINERS[(event_counter - 1) % len(CONTAINERS)]
            event = generate_event(
                container,
                event_counter,
                forced_scenario=forced,
                target_container=args.container,
                anomaly_rate=args.anomaly_rate,
            )
            try:
                producer.send(
                    KAFKA_TOPIC,
                    key=event["container_id"],
                    value=event,
                )
                print(
                    f"[{event['sensor_status']:<7}] {event['event_id']} | "
                    f"{event['container_id']} | Temp: {event['temperature_c']}C | "
                    f"Hum: {event['humidity_pct']}% | Vib: {event['vibration_g']}g | "
                    f"Scenario: {event['anomaly_type']}"
                )
            except KafkaError as exc:
                print(f"Kafka send failed for {event['event_id']}: {exc}")

            event_counter += 1
            if args.max_events == 0 or event_counter <= args.max_events:
                time.sleep(args.interval)

    except KeyboardInterrupt:
        print("\nSimulator stopped.")
    except KafkaError as exc:
        print(f"Kafka connection failed: {exc}")
    finally:
        if producer is not None:
            try:
                producer.flush(timeout=10)
            except KafkaError as exc:
                print(f"Kafka flush failed: {exc}")
            finally:
                try:
                    producer.close(timeout=10)
                except KafkaError as exc:
                    print(f"Kafka close failed: {exc}")


if __name__ == "__main__":
    main()