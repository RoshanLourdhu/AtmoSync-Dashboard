import argparse
import json
from telemetry_sink import DBT_STAGING_FIELDS, TelemetrySQLiteSink, to_dbt_commodity_row
from types import SimpleNamespace
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable

from atmosync_sensor_simulator import CONTAINERS, CSV_FIELDNAMES, generate_event
from kafka_producer import DEFAULT_BOOTSTRAP_SERVERS, DEFAULT_TOPIC

DEFAULT_GROUP_ID = "atmosync-consumer-group"
REQUIRED_FIELDS = tuple(CSV_FIELDNAMES)
VALID_STATUSES = {"NORMAL", "WARNING", "ALERT", "FAULT"}
VALID_ANOMALIES = {
    "NONE",
    "COOLING_FAILURE",
    "DOOR_OPEN",
    "ROUGH_TRANSIT",
    "SENSOR_DROPOUT",
}


class ConsumerConfigError(ValueError):
    """Raised when Kafka consumer configuration is invalid."""


class TelemetryValidationError(ValueError):
    """Raised when a consumed telemetry record violates the 12-field contract."""


def _normalize_bootstrap_servers(servers):
    if isinstance(servers, str):
        items = [s.strip() for s in servers.split(",") if s.strip()]
    elif isinstance(servers, (list, tuple)):
        items = [str(s).strip() for s in servers if str(s).strip()]
    else:
        items = []

    if not items:
        raise ConsumerConfigError("bootstrap_servers must contain at least one host:port.")
    return items


def validate_telemetry_event(event):
    """Validate that a parsed telemetry payload conforms to the 12-field schema."""
    if not isinstance(event, dict):
        raise TelemetryValidationError("Telemetry payload must be a JSON object.")

    missing = [field for field in REQUIRED_FIELDS if field not in event]
    if missing:
        raise TelemetryValidationError(f"Missing required telemetry fields: {missing}")

    status = event.get("sensor_status")
    if status not in VALID_STATUSES:
        raise TelemetryValidationError(f"Invalid sensor_status: {status!r}")

    anomaly = event.get("anomaly_type")
    if anomaly not in VALID_ANOMALIES:
        raise TelemetryValidationError(f"Invalid anomaly_type: {anomaly!r}")

    return event


def create_consumer(config=None, consumer_cls=KafkaConsumer):
    """Create and configure a validated KafkaConsumer instance."""
    cfg = dict(config or {})
    bootstrap_servers = _normalize_bootstrap_servers(
        cfg.get("bootstrap_servers", DEFAULT_BOOTSTRAP_SERVERS)
    )
    topic = str(cfg.get("topic", DEFAULT_TOPIC)).strip()
    if not topic:
        raise ConsumerConfigError("topic must be a non-empty string.")

    group_id = str(cfg.get("group_id", DEFAULT_GROUP_ID)).strip()
    if not group_id:
        raise ConsumerConfigError("group_id must be a non-empty string.")

    auto_offset_reset = str(cfg.get("auto_offset_reset", "earliest")).strip().lower()
    if auto_offset_reset not in {"earliest", "latest"}:
        raise ConsumerConfigError("auto_offset_reset must be 'earliest' or 'latest'.")

    try:
        consumer_timeout_ms = int(cfg.get("consumer_timeout_ms", 5000))
    except (TypeError, ValueError) as exc:
        raise ConsumerConfigError("consumer_timeout_ms must be a positive integer.") from exc
    if consumer_timeout_ms <= 0:
        raise ConsumerConfigError("consumer_timeout_ms must be a positive integer.")

    resolved = {
        "bootstrap_servers": bootstrap_servers,
        "topic": topic,
        "group_id": group_id,
        "auto_offset_reset": auto_offset_reset,
        "consumer_timeout_ms": consumer_timeout_ms,
    }

    try:
        consumer = consumer_cls(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            consumer_timeout_ms=consumer_timeout_ms,
            enable_auto_commit=True,
        )
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            f"No Kafka brokers reachable at {bootstrap_servers}: {exc}"
        ) from exc

    return consumer, resolved


def _decode_record_value(raw_value):
    if isinstance(raw_value, dict):
        return raw_value
    if isinstance(raw_value, bytes):
        raw_value = raw_value.decode("utf-8")
    if isinstance(raw_value, str):
        try:
            return json.loads(raw_value)
        except json.JSONDecodeError as exc:
            raise TelemetryValidationError(f"Malformed JSON payload: {exc}") from exc
    raise TelemetryValidationError(f"Unsupported record payload type: {type(raw_value).__name__}")


def consume_events(consumer, max_messages=0, on_alert=None, on_event=None):
    """Read, validate, and summarize telemetry messages from a Kafka consumer iterable."""
    consumed = 0
    alerts = 0
    invalid = 0

    for record in consumer:
        raw_value = getattr(record, "value", record)
        try:
            payload = _decode_record_value(raw_value)
            event = validate_telemetry_event(payload)
        except TelemetryValidationError as exc:
            invalid += 1
            print(f"[CONSUMER INVALID] {exc}")
            if max_messages > 0 and (consumed + invalid) >= max_messages:
                break
            continue

        consumed += 1
        if callable(on_event):
            on_event(event)
        if event["sensor_status"] in {"WARNING", "ALERT", "FAULT"} or event["anomaly_type"] != "NONE":
            alerts += 1
            print(
                f"[CONSUMER ALERT] id={event['event_id']} container={event['container_id']} "
                f"status={event['sensor_status']} anomaly={event['anomaly_type']} "
                f"temp={event['temperature_c']}C"
            )
            if callable(on_alert):
                on_alert(event)
        else:
            print(
                f"[CONSUMER OK] id={event['event_id']} container={event['container_id']} "
                f"temp={event['temperature_c']}C humidity={event['humidity_pct']}%"
            )

        if max_messages > 0 and (consumed + invalid) >= max_messages:
            break

    return {"consumed": consumed, "alerts": alerts, "invalid": invalid}


def close_consumer(consumer):
    """Safely close a KafkaConsumer instance."""
    if consumer is not None and hasattr(consumer, "close"):
        consumer.close()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="AtmoSync Cold-Chain Kafka Telemetry Consumer & Schema Validator"
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=DEFAULT_BOOTSTRAP_SERVERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--topic",
        default=DEFAULT_TOPIC,
        help=f"Kafka topic to subscribe to (default: {DEFAULT_TOPIC})",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_GROUP_ID,
        help=f"Kafka consumer group ID (default: {DEFAULT_GROUP_ID})",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after processing N messages; 0 reads until timeout (default: 0)",
    )
    parser.add_argument(
        "--timeout-ms",
        type=int,
        default=5000,
        help="Consumer poll timeout in milliseconds (default: 5000)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate a sample batch of generated telemetry records offline without Kafka",
    )
    parser.add_argument(
        "--db-out",
        default=None,
        help="Optional SQLite database path for persisting validated telemetry events",
    )
    parser.add_argument(
        "--dbt-csv-out",
        default=None,
        help="Optional CSV path for exporting dbt-aligned commodity staging rows",
    )
    return parser.parse_args(argv)


def run_consumer(args, consumer=None):
    db_out = getattr(args, "db_out", None)
    dbt_csv_out = getattr(args, "dbt_csv_out", None)
    sink = TelemetrySQLiteSink(db_path=db_out) if db_out else None
    on_event = sink.write_event if sink is not None else None
    staged_events = [] if (sink is None and dbt_csv_out) else None
    if staged_events is not None:
        on_event = staged_events.append

    if args.dry_run:
        sample_count = args.max_messages if args.max_messages > 0 else 3
        records = []
        for idx in range(1, sample_count + 1):
            container = CONTAINERS[(idx - 1) % len(CONTAINERS)]
            event = generate_event(container, idx, anomaly_rate=0.35)
            records.append(SimpleNamespace(value=json.dumps(event).encode("utf-8")))
        summary = consume_events(records, max_messages=sample_count, on_event=on_event)
    else:
        owns_consumer = False
        if consumer is None:
            consumer, _ = create_consumer(
                {
                    "bootstrap_servers": args.bootstrap_servers,
                    "topic": args.topic,
                    "group_id": args.group_id,
                    "consumer_timeout_ms": args.timeout_ms,
                }
            )
            owns_consumer = True

        try:
            summary = consume_events(
                consumer, max_messages=args.max_messages, on_event=on_event
            )
        finally:
            if owns_consumer:
                close_consumer(consumer)

    if sink is not None:
        summary["persisted"] = sink.stats()["events_count"]
        if dbt_csv_out:
            sink.export_dbt_staging_csv(dbt_csv_out)
    elif dbt_csv_out and staged_events is not None:
        import csv
        from telemetry_sink import DBT_STAGING_FIELDS, to_dbt_commodity_row

        with open(dbt_csv_out, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DBT_STAGING_FIELDS))
            writer.writeheader()
            writer.writerows(to_dbt_commodity_row(ev) for ev in staged_events)

    return summary


def main(argv=None):
    args = parse_args(argv)
    try:
        summary = run_consumer(args)
        persisted_part = (
            f" persisted={summary['persisted']}" if "persisted" in summary else ""
        )
        print(
            f"[CONSUMER SUMMARY] consumed={summary['consumed']} "
            f"alerts={summary['alerts']} invalid={summary['invalid']}{persisted_part}"
        )
    except ConsumerConfigError as exc:
        print(f"[CONSUMER CONFIG ERROR] {exc}")
        raise SystemExit(1) from exc
    except (RuntimeError, KafkaError) as exc:
        print(f"[CONSUMER RUNTIME ERROR] {exc}")
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()