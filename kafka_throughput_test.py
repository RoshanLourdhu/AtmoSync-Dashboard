import argparse
import time
from datetime import datetime, timezone
from kafka.errors import KafkaError

from atmosync_sensor_simulator import CONTAINERS, generate_event
from kafka_producer import (
    DEFAULT_BOOTSTRAP_SERVERS,
    DEFAULT_TOPIC,
    ProducerConfigError,
    ProducerDeliveryError,
    close_producer,
    create_producer,
    send_event,
)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="AtmoSync Kafka Producer Throughput & Reliability Benchmark"
    )
    parser.add_argument(
        "--num-events",
        type=int,
        default=100,
        help="Total telemetry events to generate and publish (default: 100)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Measure event generation and serialization throughput without connecting to Kafka",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka target topic (default: {DEFAULT_TOPIC})",
    )
    return parser.parse_args(argv)


def run_throughput_test(
    num_events=100,
    dry_run=False,
    bootstrap_servers=DEFAULT_BOOTSTRAP_SERVERS,
    topic=DEFAULT_TOPIC,
    producer=None,
):
    """Benchmark validated event generation and Kafka delivery throughput."""
    if num_events <= 0:
        raise ValueError("num_events must be a positive integer.")

    owns_producer = False
    if not dry_run and producer is None:
        producer, cfg = create_producer(
            {
                "bootstrap_servers": bootstrap_servers,
                "topic": topic,
            }
        )
        topic = cfg["topic"]
        owns_producer = True

    acknowledged = 0
    failed = 0
    start_time = time.perf_counter()

    try:
        for idx in range(1, num_events + 1):
            container = CONTAINERS[(idx - 1) % len(CONTAINERS)]
            event = generate_event(
                container,
                idx,
                forced_scenario=None,
                target_container="ALL",
                anomaly_rate=0.15,
            )
            if dry_run:
                acknowledged += 1
            else:
                try:
                    send_event(
                        producer,
                        topic,
                        event,
                        timeout=10,
                        on_success=lambda meta, event_id: None,
                        on_error=lambda exc, event_id, topic: None,
                    )
                    acknowledged += 1
                except ProducerDeliveryError:
                    failed += 1
    finally:
        if owns_producer and producer is not None:
            close_producer(producer, timeout=10)

    elapsed_sec = max(time.perf_counter() - start_time, 1e-6)
    events_per_sec = acknowledged / elapsed_sec
    mode_label = "DRY-RUN" if dry_run else "KAFKA-LIVE"

    summary = {
        "mode": mode_label,
        "requested_events": num_events,
        "acknowledged_events": acknowledged,
        "failed_events": failed,
        "elapsed_sec": round(elapsed_sec, 4),
        "events_per_sec": round(events_per_sec, 2),
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    print(
        f"[THROUGHPUT {mode_label}] sent={acknowledged}/{num_events} "
        f"failed={failed} elapsed={summary['elapsed_sec']}s "
        f"rate={summary['events_per_sec']} events/s"
    )
    return summary


def main(argv=None):
    args = parse_args(argv)
    try:
        run_throughput_test(
            num_events=args.num_events,
            dry_run=args.dry_run,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
        )
    except (ValueError, ProducerConfigError) as exc:
        print(f"[THROUGHPUT CONFIG ERROR] {exc}")
        raise SystemExit(1) from exc
    except (RuntimeError, KafkaError) as exc:
        print(f"[THROUGHPUT RUNTIME ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()