import py313_compat
import json
import os
import re
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError, NoBrokersAvailable

DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_TOPIC = "atmosync_sensor_events"
VALID_ACKS = {"0", "1", "all", 0, 1}
TOPIC_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,249}$")


class ProducerConfigError(ValueError):
    """Raised when Kafka producer configuration is missing or invalid."""


class ProducerDeliveryError(RuntimeError):
    """Raised when an event fails to be delivered to Kafka."""


def validate_bootstrap_servers(bootstrap_servers):
    """Validate bootstrap_servers string or sequence of 'host:port' entries."""
    if bootstrap_servers is None:
        raise ProducerConfigError(
            "KAFKA_BOOTSTRAP_SERVERS is missing. Expected 'host:port' (e.g., 'localhost:9092')."
        )

    if isinstance(bootstrap_servers, str):
        servers = [s.strip() for s in bootstrap_servers.split(",") if s.strip()]
    elif isinstance(bootstrap_servers, (list, tuple)):
        servers = [str(s).strip() for s in bootstrap_servers if str(s).strip()]
    else:
        raise ProducerConfigError(
            f"Invalid bootstrap_servers type '{type(bootstrap_servers).__name__}'. Expected string or list."
        )

    if not servers:
        raise ProducerConfigError(
            "KAFKA_BOOTSTRAP_SERVERS cannot be empty. Expected 'host:port' (e.g., 'localhost:9092')."
        )

    for entry in servers:
        if ":" not in entry:
            raise ProducerConfigError(
                f"Invalid bootstrap server '{entry}': missing port. Expected 'host:port'."
            )
        host, port_str = entry.rsplit(":", 1)
        if not host:
            raise ProducerConfigError(
                f"Invalid bootstrap server '{entry}': host cannot be empty."
            )
        if not port_str.isdigit() or not (1 <= int(port_str) <= 65535):
            raise ProducerConfigError(
                f"Invalid port '{port_str}' in bootstrap server '{entry}'. Port must be between 1 and 65535."
            )

    return servers


def validate_topic(topic):
    """Validate Kafka topic name against standard Kafka naming constraints."""
    if not isinstance(topic, str) or not topic.strip():
        raise ProducerConfigError(
            "KAFKA_TOPIC is missing or empty. Expected a non-empty topic name (e.g., 'atmosync_sensor_events')."
        )
    cleaned = topic.strip()
    if cleaned in (".", "..") or not TOPIC_PATTERN.match(cleaned):
        raise ProducerConfigError(
            f"Invalid KAFKA_TOPIC '{cleaned}'. Use 1-249 alphanumeric characters, '.', '_', or '-'."
        )
    return cleaned


def validate_producer_config(config=None):
    """Validate and normalize producer configuration dictionary or environment defaults."""
    raw = {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        "topic": os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC),
        "acks": os.getenv("KAFKA_ACKS", "all"),
        "retries": os.getenv("KAFKA_RETRIES", 3),
        "linger_ms": os.getenv("KAFKA_LINGER_MS", 5),
        "batch_size": os.getenv("KAFKA_BATCH_SIZE", 32768),
        "request_timeout_ms": os.getenv("KAFKA_REQUEST_TIMEOUT_MS", 10000),
    }
    if config:
        raw.update(config)

    servers = validate_bootstrap_servers(raw.get("bootstrap_servers"))
    topic = validate_topic(raw.get("topic"))

    acks = raw.get("acks")
    if isinstance(acks, str) and acks.isdigit():
        acks = int(acks)
    if acks not in VALID_ACKS:
        raise ProducerConfigError(
            f"Invalid acks value '{acks}'. Allowed values: 0, 1, 'all'."
        )

    int_rules = {
        "retries": (0, 20),
        "linger_ms": (0, 60000),
        "batch_size": (1024, 10485760),
        "request_timeout_ms": (1000, 120000),
    }
    validated_ints = {}
    for field, (low, high) in int_rules.items():
        val = raw.get(field)
        if val is None or isinstance(val, bool):
            raise ProducerConfigError(
                f"Invalid {field}='{val}'. Must be an integer between {low} and {high}."
            )
        try:
            parsed = int(val)
        except (TypeError, ValueError) as exc:
            raise ProducerConfigError(
                f"Invalid {field}='{val}'. Must be an integer between {low} and {high}."
            ) from exc

        if not (low <= parsed <= high):
            raise ProducerConfigError(
                f"Out-of-range {field}={parsed}. Must be between {low} and {high}."
            )
        validated_ints[field] = parsed

    return {
        "bootstrap_servers": servers,
        "topic": topic,
        "acks": acks,
        **validated_ints,
    }


def create_producer(config=None, producer_cls=KafkaProducer):
    """Validate configuration and construct a configured KafkaProducer instance."""
    cfg = validate_producer_config(config)
    try:
        producer = producer_cls(
            bootstrap_servers=cfg["bootstrap_servers"],
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=cfg["acks"],
            retries=cfg["retries"],
            linger_ms=cfg["linger_ms"],
            batch_size=cfg["batch_size"],
            request_timeout_ms=cfg["request_timeout_ms"],
        )
        print(
            f"[PRODUCER READY] Connected to {','.join(cfg['bootstrap_servers'])} "
            f"| topic='{cfg['topic']}' | acks={cfg['acks']} | retries={cfg['retries']}"
        )
        return producer, cfg
    except NoBrokersAvailable as exc:
        raise RuntimeError(
            f"[PRODUCER STARTUP ERROR] No Kafka brokers reachable at "
            f"{','.join(cfg['bootstrap_servers'])}. Verify Zookeeper and Kafka are running."
        ) from exc
    except KafkaError as exc:
        raise RuntimeError(
            f"[PRODUCER STARTUP ERROR] Kafka initialization failed for "
            f"{','.join(cfg['bootstrap_servers'])}: {exc}"
        ) from exc


def on_send_success(record_metadata, event_id="UNKNOWN"):
    """Callback invoked when Kafka acknowledges record delivery."""
    print(
        f"[DELIVERY ACK] event_id={event_id} topic='{record_metadata.topic}' "
        f"partition={record_metadata.partition} offset={record_metadata.offset}"
    )


def on_send_error(exc, event_id="UNKNOWN", topic="UNKNOWN"):
    """Callback invoked when asynchronous record delivery fails."""
    print(
        f"[DELIVERY ERROR] event_id={event_id} topic='{topic}' "
        f"error_type={type(exc).__name__} detail={exc}"
    )


def send_event(producer, topic, event, timeout=10, on_success=on_send_success, on_error=on_send_error):
    """Publish a telemetry event with delivery callbacks and structured failure reporting."""
    if not isinstance(event, dict):
        raise ValueError("Event payload must be a dictionary.")

    event_id = event.get("event_id", "UNKNOWN")
    key = event.get("container_id")

    try:
        future = producer.send(topic, key=key, value=event)
        future.add_callback(lambda meta: on_success(meta, event_id=event_id))
        future.add_errback(lambda exc: on_error(exc, event_id=event_id, topic=topic))
        metadata = future.get(timeout=timeout)
        return metadata
    except KafkaTimeoutError as exc:
        on_error(exc, event_id=event_id, topic=topic)
        raise ProducerDeliveryError(
            f"Delivery timed out after {timeout}s for event_id={event_id} on topic='{topic}'."
        ) from exc
    except KafkaError as exc:
        on_error(exc, event_id=event_id, topic=topic)
        raise ProducerDeliveryError(
            f"Kafka delivery failed for event_id={event_id} on topic='{topic}': {exc}"
        ) from exc


def close_producer(producer, timeout=10):
    """Flush pending records and safely close the producer instance."""
    if producer is None:
        return
    try:
        producer.flush(timeout=timeout)
    except KafkaError as exc:
        print(f"[PRODUCER FLUSH ERROR] {exc}")
    finally:
        try:
            producer.close(timeout=timeout)
        except KafkaError as exc:
            print(f"[PRODUCER CLOSE ERROR] {exc}")


def build_sample_event():
    return {
        "event_id": "EV000001",
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "container_id": "C001",
        "commodity": "Avocados",
        "origin": "Chennai",
        "destination": "Bengaluru",
        "quantity_kg": 4200,
        "temperature_c": 5.5,
        "humidity_pct": 85.0,
        "vibration_g": 0.15,
        "sensor_status": "NORMAL",
        "anomaly_type": "NONE",
    }


def main():
    producer = None
    try:
        producer, cfg = create_producer()
        event = build_sample_event()
        send_event(producer, cfg["topic"], event, timeout=10)
    except ProducerConfigError as exc:
        print(f"[CONFIG ERROR] {exc}")
        raise SystemExit(1) from exc
    except (RuntimeError, ProducerDeliveryError, KafkaError) as exc:
        print(f"{exc}")
        raise SystemExit(1) from exc
    finally:
        close_producer(producer, timeout=10)


if __name__ == "__main__":
    main()