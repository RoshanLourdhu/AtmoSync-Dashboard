import json
import time
from collections import Counter
from kafka import KafkaProducer
from atmosync_sensor_simulator import (
    CONTAINERS,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    generate_event,
)

TOTAL_EVENTS = 3000


def run_throughput_test():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        linger_ms=5,
        batch_size=32768,
    )

    print(f"Starting throughput test: sending {TOTAL_EVENTS} events to '{KAFKA_TOPIC}'...")
    futures = []
    start_time = time.perf_counter()

    for i in range(1, TOTAL_EVENTS + 1):
        container = CONTAINERS[(i - 1) % len(CONTAINERS)]
        event = generate_event(container, i)
        key_bytes = event["container_id"].encode("utf-8")
        future = producer.send(KAFKA_TOPIC, key=key_bytes, value=event)
        futures.append(future)

    partition_counts = Counter()
    for future in futures:
        metadata = future.get(timeout=15)
        partition_counts[metadata.partition] += 1

    producer.flush()
    elapsed = time.perf_counter() - start_time
    producer.close()

    throughput = TOTAL_EVENTS / elapsed
    avg_latency_ms = (elapsed / TOTAL_EVENTS) * 1000

    print("\n--- Throughput Test Results ---")
    print(f"Total Events Sent : {TOTAL_EVENTS}")
    print(f"Total Time        : {elapsed:.2f} s")
    print(f"Throughput        : {throughput:.2f} events/sec")
    print(f"Avg Per-Event Time: {avg_latency_ms:.3f} ms")
    print("Partition Distribution:")
    for partition in sorted(partition_counts):
        print(f"  Partition {partition}: {partition_counts[partition]} events")


if __name__ == "__main__":
    run_throughput_test()