import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka.errors import KafkaError

from kafka_producer import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_TOPIC,
    ProducerConfigError,
    ProducerDeliveryError,
    close_producer,
    create_producer,
    send_event,
)

KAFKA_TOPIC = DEFAULT_TOPIC
KAFKA_BOOTSTRAP_SERVERS = DEFAULT_BOOTSTRAP_SERVERS
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="AtmoSync Cold-Chain IoT Sensor Telemetry Simulator"
    )
    parser.add_argument(
        "--scenario",
        choices=["AUTO", "NONE", "COOLING_FAILURE", "DOOR_OPEN", "ROUGH_TRANSIT", "SENSOR_DROPOUT"],
        default="AUTO",
        help="Force a specific anomaly scenario or use AUTO for random anomalies (default: AUTO)",
    )
    parser.add_argument(
        "--container",
        choices=["ALL", "C001", "C002", "C003"],
        default="ALL",
        help="Target container for forced scenarios (default: ALL)",
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
        default=1.0,
        help="Seconds to sleep between container telemetry batches (default: 1.0)",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=0,
        help="Stop after publishing N events; 0 runs continuously (default: 0)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Generate and print events locally without connecting to Kafka",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=KAFKA_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {KAFKA_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--topic",
        default=KAFKA_TOPIC,
        help=f"Kafka target topic (default: {KAFKA_TOPIC})",
    )
    return parser.parse_args(argv)


def run_simulator(args, producer=None):
    """Execute the telemetry simulation loop using validated kafka_producer helpers."""
    owns_producer = False
    topic = args.topic

    if not args.dry_run and producer is None:
        producer, cfg = create_producer(
            {
                "bootstrap_servers": args.bootstrap_servers,
                "topic": args.topic,
            }
        )
        topic = cfg["topic"]
        owns_producer = True

    forced = None if args.scenario == "AUTO" else args.scenario
    event_counter = 0

    try:
        while True:
            for container in CONTAINERS:
                if args.max_events > 0 and event_counter >= args.max_events:
                    return event_counter

                event_counter += 1
                event = generate_event(
                    container,
                    event_counter,
                    forced_scenario=forced,
                    target_container=args.container,
                    anomaly_rate=args.anomaly_rate,
                )

                if args.dry_run:
                    print(f"[DRY RUN] {json.dumps(event)}")
                else:
                    send_event(producer, topic, event, timeout=10)

            if args.max_events > 0 and event_counter >= args.max_events:
                return event_counter

            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\n[SIMULATOR STOPPED] Published {event_counter} events.")
        return event_counter
    finally:
        if owns_producer and producer is not None:
            close_producer(producer, timeout=10)


def main(argv=None):
    args = parse_args(argv)
    try:
        run_simulator(args)
    except ProducerConfigError as exc:
        print(f"[SIMULATOR CONFIG ERROR] {exc}")
        raise SystemExit(1) from exc
    except (RuntimeError, ProducerDeliveryError, KafkaError) as exc:
        print(f"[SIMULATOR RUNTIME ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()