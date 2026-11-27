#!/usr/bin/env python3
"""
AtmoSync Kafka Throughput & Consumer Lag Benchmark.

Measures live Kafka producer/consumer throughput, processing duration,
end-to-end latency, schema conformance, and partition consumer lag on topic
`container-sensor-data` using the canonical SensorSimulator,
SensorKafkaProducer, and SensorKafkaConsumer components.

Never fabricates benchmark metrics or substitutes mocks when Kafka is offline:
if the broker is unreachable, reports `LIVE_KAFKA_UNAVAILABLE` with the exact
socket/connection error.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from sensor_simulator.simulator import SensorSimulator
from shared.validation import SensorValidationError, validate_sensor_event
from streaming.consumer.consumer import ConsumerConfig, SensorKafkaConsumer
from streaming.producer.producer import ProducerConfig, SensorKafkaProducer

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_BLOCKED = "LIVE_KAFKA_UNAVAILABLE"


@dataclass(frozen=True)
class BenchmarkConfig:
    """Validated configuration for a live Kafka throughput and lag benchmark."""

    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.getenv("KAFKA_TOPIC", "container-sensor-data")
    message_count: int = 500
    batch_size: int = 100
    consume_timeout_sec: float = 15.0
    socket_timeout_sec: float = 2.0


@dataclass
class BenchmarkResult:
    """Structured result of a Kafka throughput and consumer lag benchmark."""

    status: str  # PASS, FAIL, or LIVE_KAFKA_UNAVAILABLE
    bootstrap_servers: str
    topic: str
    requested_messages: int
    batch_size: int
    messages_produced: int = 0
    messages_consumed: int = 0
    failed_or_unprocessed_messages: int = 0
    schema_valid_messages: int = 0
    schema_invalid_messages: int = 0
    production_duration_sec: float = 0.0
    consumption_duration_sec: float = 0.0
    total_duration_sec: float = 0.0
    production_throughput_msg_per_sec: float = 0.0
    consumption_throughput_msg_per_sec: float = 0.0
    avg_latency_ms: float | None = None
    max_latency_ms: float | None = None
    consumer_lag: int | None = None
    reason: str | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_benchmark_config(config: BenchmarkConfig) -> None:
    """Validate benchmark parameters before execution."""
    if not config.bootstrap_servers or not config.bootstrap_servers.strip():
        raise ValueError("bootstrap_servers must be a non-empty host:port string.")
    if not config.topic or not config.topic.strip():
        raise ValueError("topic must be a non-empty Kafka topic name.")
    if config.message_count <= 0:
        raise ValueError("message_count must be > 0.")
    if config.batch_size <= 0:
        raise ValueError("batch_size must be > 0.")
    if config.consume_timeout_sec <= 0:
        raise ValueError("consume_timeout_sec must be > 0.")


def parse_first_broker_host_port(bootstrap_servers: str) -> tuple[str, int]:
    """Extract (host, port) from the first entry in a bootstrap.servers string."""
    first = bootstrap_servers.split(",")[0].strip()
    if "://" in first:
        first = first.split("://", 1)[1]
    if ":" not in first:
        return first, 9092
    host, port_str = first.rsplit(":", 1)
    try:
        port = int(port_str)
    except ValueError as exc:
        raise ValueError(f"Invalid Kafka broker port in '{bootstrap_servers}': {port_str}") from exc
    return host.strip(), port


def check_kafka_broker_reachable(bootstrap_servers: str, timeout_sec: float = 2.0) -> tuple[bool, str]:
    """Attempt a raw TCP connection to the first configured Kafka broker."""
    host, port = parse_first_broker_host_port(bootstrap_servers)
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            return True, f"TCP connection to {host}:{port} succeeded."
    except (socket.timeout, ConnectionRefusedError, OSError) as exc:
        return False, f"Cannot reach Kafka broker at {host}:{port}: {exc}"


def compute_benchmark_metrics(
    requested_messages: int,
    messages_produced: int,
    messages_consumed: int,
    schema_valid_messages: int,
    schema_invalid_messages: int,
    production_duration_sec: float,
    consumption_duration_sec: float,
    latencies_ms: list[float],
    consumer_lag: int | None,
) -> dict[str, Any]:
    """Compute throughput, failure counts, and latency summary metrics."""
    failed_or_unprocessed = max(0, requested_messages - messages_consumed) + schema_invalid_messages
    prod_tput = (messages_produced / production_duration_sec) if production_duration_sec > 0 else 0.0
    cons_tput = (messages_consumed / consumption_duration_sec) if consumption_duration_sec > 0 else 0.0
    avg_lat = (sum(latencies_ms) / len(latencies_ms)) if latencies_ms else None
    max_lat = max(latencies_ms) if latencies_ms else None
    return {
        "failed_or_unprocessed_messages": failed_or_unprocessed,
        "production_throughput_msg_per_sec": round(prod_tput, 2),
        "consumption_throughput_msg_per_sec": round(cons_tput, 2),
        "avg_latency_ms": round(avg_lat, 3) if avg_lat is not None else None,
        "max_latency_ms": round(max_lat, 3) if max_lat is not None else None,
        "consumer_lag": consumer_lag,
    }


def _compute_partition_lag(consumer_obj: Any, topic: str) -> int | None:
    """Calculate total remaining lag across assigned partitions using broker watermarks."""
    raw_consumer = getattr(consumer_obj, "consumer", None) or getattr(consumer_obj, "_consumer", None)
    if raw_consumer is None:
        return None
    try:
        assignment = raw_consumer.assignment()
        if not assignment:
            return None
        positions = raw_consumer.position(assignment)
        total_lag = 0
        for tp in positions:
            if tp.topic != topic:
                continue
            _, high_offset = raw_consumer.get_watermark_offsets(tp, timeout=2.0, cached=False)
            current_offset = max(0, tp.offset)
            if high_offset > current_offset:
                total_lag += high_offset - current_offset
        return total_lag
    except Exception:
        return None


def run_kafka_benchmark(config: BenchmarkConfig) -> BenchmarkResult:
    """Execute a live Kafka produce + consume benchmark and return measured metrics."""
    validate_benchmark_config(config)

    reachable, reach_msg = check_kafka_broker_reachable(
        config.bootstrap_servers, timeout_sec=config.socket_timeout_sec
    )
    if not reachable:
        return BenchmarkResult(
            status=STATUS_BLOCKED,
            bootstrap_servers=config.bootstrap_servers,
            topic=config.topic,
            requested_messages=config.message_count,
            batch_size=config.batch_size,
            reason=f"{STATUS_BLOCKED}: {reach_msg}",
        )

    simulator = SensorSimulator()
    run_tag = uuid.uuid4().hex[:8]
    events: list[dict[str, Any]] = []
    for idx in range(config.message_count):
        container = simulator.containers[idx % len(simulator.containers)]
        ev = simulator.generate_event(container)
        ev["shipment_id"] = f"SHP-BENCH-{run_tag}-{idx:05d}"
        validate_sensor_event(ev)
        events.append(ev)

    producer_cfg = ProducerConfig(
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
    )
    consumer_cfg = ConsumerConfig(
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
        group_id=f"atmosync-benchmark-{run_tag}",
        auto_offset_reset="earliest",
        batch_size=config.batch_size,
    )

    produced_count = 0
    consumed_count = 0
    schema_valid = 0
    schema_invalid = 0
    latencies_ms: list[float] = []
    prod_start = time.perf_counter()
    emit_times: dict[str, float] = {}

    producer = SensorKafkaProducer(config=producer_cfg)
    try:
        for start_idx in range(0, len(events), config.batch_size):
            batch = events[start_idx : start_idx + config.batch_size]
            for ev in batch:
                emit_times[ev["shipment_id"]] = time.perf_counter()
            producer.produce_batch(batch)
        producer.flush(timeout=10.0)
        stats = producer.get_stats()
        produced_count = stats.get("delivered", 0)
    finally:
        producer.close(timeout=5.0)
    prod_duration = max(1e-6, time.perf_counter() - prod_start)

    cons_start = time.perf_counter()
    deadline = cons_start + config.consume_timeout_sec
    consumer = SensorKafkaConsumer(config=consumer_cfg)
    lag: int | None = None
    try:
        while consumed_count < config.message_count and time.perf_counter() < deadline:
            raw_msgs = consumer.poll_messages(max_records=config.batch_size, timeout=0.5)
            if not raw_msgs:
                continue
            recv_ts = time.perf_counter()
            for msg in raw_msgs:
                try:
                    raw_val = msg.value()
                    payload = json.loads(raw_val.decode("utf-8") if isinstance(raw_val, bytes) else raw_val)
                    if not str(payload.get("shipment_id", "")).startswith(f"SHP-BENCH-{run_tag}-"):
                        continue
                    validate_sensor_event(payload)
                    schema_valid += 1
                    consumed_count += 1
                    sent_ts = emit_times.get(payload["shipment_id"])
                    if sent_ts is not None:
                        latencies_ms.append((recv_ts - sent_ts) * 1000.0)
                except (SensorValidationError, ValueError, TypeError):
                    schema_invalid += 1
                    consumed_count += 1
        lag = _compute_partition_lag(consumer, config.topic)
    finally:
        consumer.close()
    cons_duration = max(1e-6, time.perf_counter() - cons_start)
    total_duration = prod_duration + cons_duration

    metrics = compute_benchmark_metrics(
        requested_messages=config.message_count,
        messages_produced=produced_count,
        messages_consumed=consumed_count,
        schema_valid_messages=schema_valid,
        schema_invalid_messages=schema_invalid,
        production_duration_sec=prod_duration,
        consumption_duration_sec=cons_duration,
        latencies_ms=latencies_ms,
        consumer_lag=lag,
    )

    passed = (
        produced_count == config.message_count
        and consumed_count == config.message_count
        and schema_invalid == 0
        and (lag is None or lag == 0)
    )
    status = STATUS_PASS if passed else STATUS_FAIL
    reason = None if passed else (
        f"Produced {produced_count}/{config.message_count}, consumed {consumed_count}/{config.message_count}, "
        f"schema_invalid={schema_invalid}, lag={lag}."
    )

    return BenchmarkResult(
        status=status,
        bootstrap_servers=config.bootstrap_servers,
        topic=config.topic,
        requested_messages=config.message_count,
        batch_size=config.batch_size,
        messages_produced=produced_count,
        messages_consumed=consumed_count,
        failed_or_unprocessed_messages=metrics["failed_or_unprocessed_messages"],
        schema_valid_messages=schema_valid,
        schema_invalid_messages=schema_invalid,
        production_duration_sec=round(prod_duration, 4),
        consumption_duration_sec=round(cons_duration, 4),
        total_duration_sec=round(total_duration, 4),
        production_throughput_msg_per_sec=metrics["production_throughput_msg_per_sec"],
        consumption_throughput_msg_per_sec=metrics["consumption_throughput_msg_per_sec"],
        avg_latency_ms=metrics["avg_latency_ms"],
        max_latency_ms=metrics["max_latency_ms"],
        consumer_lag=lag,
        reason=reason,
    )


def format_benchmark_report(result: BenchmarkResult) -> str:
    """Format a human-readable Kafka benchmark report."""
    lines = [
        "=" * 72,
        "ATMOSYNC KAFKA THROUGHPUT & LAG BENCHMARK",
        "=" * 72,
        f"Status                : {result.status}",
        f"Bootstrap Servers     : {result.bootstrap_servers}",
        f"Topic                 : {result.topic}",
        f"Requested Messages    : {result.requested_messages} (batch_size={result.batch_size})",
    ]
    if result.status == STATUS_BLOCKED:
        lines.append(f"Blocker               : {result.reason}")
    else:
        lines.extend([
            f"Messages Produced     : {result.messages_produced}",
            f"Messages Consumed     : {result.messages_consumed}",
            f"Failed / Unprocessed  : {result.failed_or_unprocessed_messages}",
            f"Schema Valid/Invalid  : {result.schema_valid_messages} / {result.schema_invalid_messages}",
            f"Production Throughput : {result.production_throughput_msg_per_sec} msg/s ({result.production_duration_sec}s)",
            f"Consumption Throughput: {result.consumption_throughput_msg_per_sec} msg/s ({result.consumption_duration_sec}s)",
            f"Total Duration        : {result.total_duration_sec}s",
            f"Avg / Max Latency     : {result.avg_latency_ms} ms / {result.max_latency_ms} ms",
            f"Consumer Lag          : {result.consumer_lag if result.consumer_lag is not None else 'Unavailable'}",
        ])
        if result.reason:
            lines.append(f"Failure Reason        : {result.reason}")
    lines.append("=" * 72)
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Benchmark live Kafka throughput and consumer lag.")
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "container-sensor-data"))
    parser.add_argument("--messages", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    cfg = BenchmarkConfig(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        message_count=args.messages,
        batch_size=args.batch_size,
        consume_timeout_sec=args.timeout,
    )
    res = run_kafka_benchmark(cfg)
    if args.json:
        print(json.dumps(res.to_dict(), indent=2))
    else:
        print(format_benchmark_report(res))
    return 0 if res.status == STATUS_PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
