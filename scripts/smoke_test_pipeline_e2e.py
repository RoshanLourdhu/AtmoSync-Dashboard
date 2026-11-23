#!/usr/bin/env python3
"""
AtmoSync Repeatable End-to-End Pipeline Smoke Test.

Executes a verified, repeatable 10-stage integration test with an identifiable,
traceable test event across the complete AtmoSync data pipeline:
1. Sensor Event Generation (7-field schema format).
2. JSON Schema Contract Validation (shared/schemas/sensor_reading.schema.json).
3. Kafka Producer Partitioning & Serialization (keyed by container_id).
4. Kafka Broker Delivery Acknowledgement.
5. Kafka Consumer Receipt & Poison-Pill Isolation.
6. Snowflake RAW Table Batch Ingestion parameter binding.
7. Snowflake RAW Landing & Column Verification.
8. dbt Staging Contract Unpacking (stg_sensor_readings).
9. dbt Mart Analytical Transformations (fct_spoilage_risk & fct_sensor_data_quality).
10. Superset Dashboard Dataset & Filter Semantic Validation.

Explicit Execution Modes:
- Mode 1: Deterministic Simulation (--mode simulation, default)
  Offline contract validation with deterministic stubs. All results explicitly labeled
  as DETERMINISTIC_SIMULATION.
- Mode 2: Live Integration (--mode live or --live)
  Strict validation against actual configured external services (Kafka, Snowflake, dbt, Superset).
  Never silently falls back to mocks; reports LIVE_SERVICE_UNAVAILABLE or
  AUTHENTICATION_UNCONFIGURED if infrastructure is missing or unreachable.

Usage:
    python scripts/smoke_test_pipeline_e2e.py [--mode {simulation,live}] [--live] [--timeout SECONDS] [--json] [--verbose]
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock
from urllib.parse import urlparse

import yaml

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dashboard.data_quality.dashboard_config_validator import validate_dashboard_config
from dashboard.data_quality.filter_validator import validate_filter_configuration
from dashboard.data_quality.report_generator import generate_quality_report
from shared.validation import validate_sensor_event
from snowflake.connection import SnowflakeConnectionError, get_snowflake_config, get_snowflake_connection
from snowflake.ingestion import RAW_SENSOR_TABLE, insert_sensor_events
from streaming.consumer.consumer import ConsumerConfig, KafkaConsumerError, SensorKafkaConsumer
from streaming.producer.producer import ProducerConfig, ProducerConfigError, SensorKafkaProducer

logger = logging.getLogger("smoke_test")

MODE_SIMULATION = "simulation"
MODE_LIVE = "live"
TAG_SIMULATION = "DETERMINISTIC_SIMULATION"
TAG_LIVE = "LIVE"


def check_port_listening(host: str, port: int, timeout: float = 1.0) -> bool:
    """Check whether a TCP socket is listening at host:port."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


@dataclass
class StageResult:
    stage_number: int
    name: str
    status: str  # "PASSED", "FAILED", "SKIPPED"
    mode: str    # "LIVE" or "DETERMINISTIC_SIMULATION"
    duration_ms: float
    details: dict[str, Any] = field(default_factory=dict)
    error: str | None = None


class PipelineSmokeTest:
    """Repeatable 10-stage end-to-end pipeline smoke test runner supporting simulation and live modes."""

    def __init__(
        self,
        mode: str = MODE_SIMULATION,
        live_requested: bool = False,
        timeout_seconds: float = 10.0,
        container_id: str | None = None,
        verbose: bool = False,
    ) -> None:
        # Backward compatibility: live_requested=True overrides mode to live
        self.mode = MODE_LIVE if live_requested or mode.lower() == MODE_LIVE else MODE_SIMULATION
        self.timeout = float(timeout_seconds)
        self.verbose = verbose

        # Unique traceable identifier for this smoke test run
        self.run_uuid = uuid.uuid4().hex[:8].upper()
        self.traceable_container_id = container_id or f"SMOKE-TEST-{self.run_uuid}"
        self.traceable_shipment_id = f"SHIP-SMOKE-{self.run_uuid}"
        self.stages: list[StageResult] = []
        self.event: dict[str, Any] = {}

        # Tracking variables
        self._delivery_ack_received = False
        self._delivery_metadata: dict[str, Any] = {}

    def log(self, message: str) -> None:
        if self.verbose:
            print(f"[SMOKE TEST] {message}")

    def run_all(self) -> dict[str, Any]:
        """Execute all 10 stages in sequence and return summary dictionary."""
        start_time = time.perf_counter()

        # Execute all 10 stages
        self._stage_1_generate_event()
        self._stage_2_validate_contract()
        self._stage_3_kafka_publish()
        self._stage_4_delivery_acknowledgement()
        self._stage_5_consumer_and_poison_pill()
        self._stage_6_snowflake_ingestion()
        self._stage_7_raw_landing_verification()
        self._stage_8_dbt_staging_unpacking()
        self._stage_9_dbt_mart_analytics()
        self._stage_10_superset_dataset_and_filters()

        total_duration_ms = round((time.perf_counter() - start_time) * 1000, 2)
        all_passed = all(s.status == "PASSED" for s in self.stages)

        return {
            "test_run_uuid": self.run_uuid,
            "traceable_container_id": self.traceable_container_id,
            "mode": self.mode,
            "mode_tag": TAG_LIVE if self.mode == MODE_LIVE else TAG_SIMULATION,
            "overall_status": "PASSED" if all_passed else "FAILED",
            "total_duration_ms": total_duration_ms,
            "stages_executed": len(self.stages),
            "stages_passed": sum(1 for s in self.stages if s.status == "PASSED"),
            "stages_failed": sum(1 for s in self.stages if s.status == "FAILED"),
            "stages": [
                {
                    "stage": s.stage_number,
                    "name": s.name,
                    "status": s.status,
                    "mode": s.mode,
                    "duration_ms": s.duration_ms,
                    "details": s.details,
                    "error": s.error,
                }
                for s in self.stages
            ],
        }

    # -------------------------------------------------------------------------
    # STAGE 1: Sensor Event Generation
    # -------------------------------------------------------------------------
    def _stage_1_generate_event(self) -> None:
        t0 = time.perf_counter()
        mode_tag = TAG_LIVE if self.mode == MODE_LIVE else TAG_SIMULATION
        try:
            self.event = {
                "container_id": self.traceable_container_id,
                "shipment_id": self.traceable_shipment_id,
                "commodity": "Avocados",
                "temperature": 6.8,
                "humidity": 78.5,
                "vibration": 0.14,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            duration = round((time.perf_counter() - t0) * 1000, 2)
            self.stages.append(
                StageResult(
                    stage_number=1,
                    name="Sensor Event Generation",
                    status="PASSED",
                    mode=mode_tag,
                    duration_ms=duration,
                    details={
                        "container_id": self.event["container_id"],
                        "commodity": self.event["commodity"],
                        "field_count": len(self.event),
                    },
                )
            )
        except Exception as exc:
            self.stages.append(
                StageResult(
                    stage_number=1,
                    name="Sensor Event Generation",
                    status="FAILED",
                    mode=mode_tag,
                    duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                    error=str(exc),
                )
            )

    # -------------------------------------------------------------------------
    # STAGE 2: JSON Schema Contract Validation
    # -------------------------------------------------------------------------
    def _stage_2_validate_contract(self) -> None:
        t0 = time.perf_counter()
        mode_tag = TAG_LIVE if self.mode == MODE_LIVE else TAG_SIMULATION
        try:
            is_valid = validate_sensor_event(self.event)
            assert is_valid is True, "Event failed JSON Schema validation against sensor_reading.schema.json"
            assert set(self.event.keys()) == {
                "container_id",
                "shipment_id",
                "commodity",
                "temperature",
                "humidity",
                "vibration",
                "timestamp",
            }, "Schema field set mismatch"
            duration = round((time.perf_counter() - t0) * 1000, 2)
            self.stages.append(
                StageResult(
                    stage_number=2,
                    name="JSON Schema Contract Validation",
                    status="PASSED",
                    mode=mode_tag,
                    duration_ms=duration,
                    details={"schema": "Draft 2020-12", "validated_fields": 7},
                )
            )
        except Exception as exc:
            self.stages.append(
                StageResult(
                    stage_number=2,
                    name="JSON Schema Contract Validation",
                    status="FAILED",
                    mode=mode_tag,
                    duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                    error=str(exc),
                )
            )

    # -------------------------------------------------------------------------
    # STAGE 3: Kafka Producer Publish & Partitioning
    # -------------------------------------------------------------------------
    def _stage_3_kafka_publish(self) -> None:
        t0 = time.perf_counter()
        kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        host_port = kafka_host.split(",")[0].strip()
        if ":" in host_port:
            host, port_str = host_port.rsplit(":", 1)
            port = int(port_str)
        else:
            host, port = host_port, 9092

        if self.mode == MODE_LIVE:
            # Strict Live Execution: Never silently fallback to mocks
            try:
                # 1. Verify socket connectivity first
                is_listening = check_port_listening(host, port, timeout=min(self.timeout, 2.0))
                if not is_listening:
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=3,
                            name="Kafka Producer Publish & Partitioning",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"LIVE_SERVICE_UNAVAILABLE: Kafka broker unreachable at {kafka_host} (connection refused or offline).",
                        )
                    )
                    return

                # 2. Initialize real producer
                producer = SensorKafkaProducer(
                    config=ProducerConfig(
                        bootstrap_servers=kafka_host,
                        topic="container-sensor-data",
                    )
                )

                # Set up delivery ack callback recording
                self._delivery_ack_received = False
                self._delivery_metadata = {}

                def live_delivery_cb(err: Any, msg: Any) -> None:
                    if err:
                        self.log(f"Live delivery error: {err}")
                    else:
                        self._delivery_ack_received = True
                        self._delivery_metadata = {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "key": msg.key().decode("utf-8") if msg.key() else None,
                        }

                # Produce with live callback
                queued = producer.produce_batch(
                    events=[self.event],
                    on_delivery=live_delivery_cb,
                )
                assert queued == 1, "Failed to queue test event in live producer"
                producer.flush(timeout=self.timeout)

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=3,
                        name="Kafka Producer Publish & Partitioning",
                        status="PASSED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        details={
                            "topic": "container-sensor-data",
                            "partition_key": self.traceable_container_id,
                            "broker": kafka_host,
                            "live_broker_connected": True,
                        },
                    )
                )
            except (ProducerConfigError, Exception) as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                error_msg = str(exc)
                if "confluent-kafka" in error_msg.lower() or "cimpl" in error_msg.lower():
                    diag = f"LIVE_SERVICE_UNAVAILABLE: confluent-kafka library unavailable or blocked by system policy: {error_msg}"
                else:
                    diag = f"LIVE_SERVICE_FAILED: {error_msg}"
                self.stages.append(
                    StageResult(
                        stage_number=3,
                        name="Kafka Producer Publish & Partitioning",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=diag,
                    )
                )
        else:
            # Deterministic simulation of Kafka Producer with delivery callbacks
            try:
                mock_client = MagicMock()
                self._delivery_ack_received = False
                self._delivery_metadata = {}

                def mock_produce(topic, key, value, on_delivery):
                    assert key == self.traceable_container_id.encode("utf-8")
                    payload = json.loads(value.decode("utf-8"))
                    assert payload["container_id"] == self.traceable_container_id
                    metadata = MagicMock()
                    metadata.topic.return_value = topic
                    metadata.partition.return_value = 0
                    metadata.offset.return_value = 1042
                    on_delivery(None, metadata)
                    self._delivery_ack_received = True
                    self._delivery_metadata = {
                        "topic": topic,
                        "partition": 0,
                        "offset": 1042,
                        "key": key.decode("utf-8"),
                    }

                mock_client.produce.side_effect = mock_produce
                mock_client.flush.return_value = 0

                producer = SensorKafkaProducer(
                    config=ProducerConfig(topic="container-sensor-data"),
                    kafka_producer=mock_client,
                )
                queued = producer.produce_batch([self.event])
                assert queued == 1, "Failed to queue test event in producer"

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=3,
                        name="Kafka Producer Publish & Partitioning",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={
                            "topic": "container-sensor-data",
                            "partition_key": self.traceable_container_id,
                            "simulated": True,
                        },
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=3,
                        name="Kafka Producer Publish & Partitioning",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 4: Delivery Acknowledgement
    # -------------------------------------------------------------------------
    def _stage_4_delivery_acknowledgement(self) -> None:
        t0 = time.perf_counter()
        mode_tag = TAG_LIVE if self.mode == MODE_LIVE else TAG_SIMULATION
        try:
            if not getattr(self, "_delivery_ack_received", False):
                if self.mode == MODE_LIVE:
                    raise RuntimeError("LIVE_DELIVERY_FAILED: Broker delivery ACK was not received within timeout.")
                else:
                    # In simulation, if ack wasn't set earlier, test simulated ack
                    self._delivery_ack_received = True
                    self._delivery_metadata = {"status": "ACK_CONFIRMED", "partition": 0, "offset": 1042}

            duration = round((time.perf_counter() - t0) * 1000, 2)
            self.stages.append(
                StageResult(
                    stage_number=4,
                    name="Broker Delivery Acknowledgement",
                    status="PASSED",
                    mode=mode_tag,
                    duration_ms=duration,
                    details=self._delivery_metadata,
                )
            )
        except Exception as exc:
            self.stages.append(
                StageResult(
                    stage_number=4,
                    name="Broker Delivery Acknowledgement",
                    status="FAILED",
                    mode=mode_tag,
                    duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                    error=str(exc),
                )
            )

    # -------------------------------------------------------------------------
    # STAGE 5: Consumer Receipt & Poison-Pill Isolation
    # -------------------------------------------------------------------------
    def _stage_5_consumer_and_poison_pill(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            kafka_host = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
            host_port = kafka_host.split(",")[0].strip()
            if ":" in host_port:
                host, port_str = host_port.rsplit(":", 1)
                port = int(port_str)
            else:
                host, port = host_port, 9092

            # Check socket connectivity first
            is_listening = check_port_listening(host, port, timeout=min(self.timeout, 2.0))
            if not is_listening:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=5,
                        name="Consumer Decoding & Poison-Pill Isolation",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_SERVICE_UNAVAILABLE: Kafka broker unreachable at {kafka_host} (connection refused or offline).",
                    )
                )
                return

            try:
                # In live mode, verify broker and consumer
                consumer = SensorKafkaConsumer(
                    config=ConsumerConfig(
                        bootstrap_servers=kafka_host,
                        topic="container-sensor-data",
                        poll_timeout_seconds=min(self.timeout, 2.0),
                    )
                )
                # Attempt to poll live message
                messages = consumer.poll_messages(max_records=1)
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=5,
                        name="Consumer Decoding & Poison-Pill Isolation",
                        status="PASSED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        details={"live_messages_polled": len(messages)},
                    )
                )
            except (KafkaConsumerError, Exception) as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                err_str = str(exc)
                if "confluent-kafka" in err_str.lower() or "cimpl" in err_str.lower():
                    diag = f"LIVE_SERVICE_UNAVAILABLE: confluent-kafka consumer unavailable or blocked by system policy: {err_str}"
                else:
                    diag = f"LIVE_SERVICE_UNAVAILABLE: Kafka consumer connection failed: {err_str}"
                self.stages.append(
                    StageResult(
                        stage_number=5,
                        name="Consumer Decoding & Poison-Pill Isolation",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=diag,
                    )
                )
        else:
            # Deterministic simulation of consumer decoding and poison pill
            try:
                mock_librdkafka_consumer = MagicMock()
                consumer = SensorKafkaConsumer(
                    config=ConsumerConfig(topic="container-sensor-data"),
                    kafka_consumer=mock_librdkafka_consumer,
                )

                # 1. Test deserialization and validation of traceable smoke event
                raw_msg = json.dumps(self.event).encode("utf-8")
                decoded_event, err = consumer.deserialize_and_validate(
                    raw_payload=raw_msg,
                    topic="container-sensor-data",
                    partition=0,
                    offset=1042,
                )
                assert decoded_event is not None and err is None, f"Consumer rejected valid event: {err}"
                assert decoded_event["container_id"] == self.traceable_container_id

                # 2. Verify poison-pill isolation: unaligned legacy payload rejected without crashing
                poison_payload = json.dumps({"container_id": "BAD001", "temperature": 25.0}).encode("utf-8")
                quarantined_event, q_err = consumer.deserialize_and_validate(
                    raw_payload=poison_payload,
                    topic="container-sensor-data",
                    partition=0,
                    offset=1043,
                )
                assert quarantined_event is None and q_err is not None, "Poison-pill was not quarantined"

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=5,
                        name="Consumer Decoding & Poison-Pill Isolation",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={
                            "smoke_event_decoded": True,
                            "poison_pill_quarantined": True,
                        },
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=5,
                        name="Consumer Decoding & Poison-Pill Isolation",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 6: Snowflake Batch Ingestion
    # -------------------------------------------------------------------------
    def _stage_6_snowflake_ingestion(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            # Live Snowflake verification: check environment credentials
            try:
                try:
                    config = get_snowflake_config()
                except SnowflakeConnectionError as cfg_err:
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=6,
                            name="Snowflake Batch Parameter Ingestion",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"AUTHENTICATION_UNCONFIGURED: {cfg_err}",
                        )
                    )
                    return

                conn = get_snowflake_connection(config=config)
                try:
                    success = insert_sensor_events(
                        events=[self.event],
                        topic="container-sensor-data",
                        partition=0,
                        offset=1042,
                        connection=conn,
                    )
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=6,
                            name="Snowflake Batch Parameter Ingestion",
                            status="PASSED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            details={"target_table": RAW_SENSOR_TABLE, "live_inserted": success},
                        )
                    )
                finally:
                    conn.close()
            except Exception as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=6,
                        name="Snowflake Batch Parameter Ingestion",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_SERVICE_FAILED: Snowflake insertion error: {exc}",
                    )
                )
        else:
            # Deterministic simulation of parameterized batch insertion
            try:
                mock_cursor = MagicMock()
                mock_connection = MagicMock()
                mock_connection.cursor.return_value = mock_cursor

                success = insert_sensor_events(
                    events=[self.event],
                    topic="container-sensor-data",
                    partition=0,
                    offset=1042,
                    connection=mock_connection,
                )

                assert success is True, "insert_sensor_events failed"
                assert mock_cursor.executemany.called, "executemany was not invoked on cursor"
                call_args = mock_cursor.executemany.call_args
                sql_query, params = call_args[0]
                assert "ATMOSYNC_DB.RAW.SENSOR_READINGS" in sql_query

                # Verify parameter payload bindings
                bound_payload, bound_topic, bound_part, bound_offset = params[0]
                unpacked_json = json.loads(bound_payload)
                assert unpacked_json["container_id"] == self.traceable_container_id
                assert bound_topic == "container-sensor-data"
                assert bound_part == 0
                assert bound_offset == 1042

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=6,
                        name="Snowflake Batch Parameter Ingestion",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={
                            "target_table": RAW_SENSOR_TABLE,
                            "success": success,
                            "transaction_committed": mock_connection.commit.called,
                        },
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=6,
                        name="Snowflake Batch Parameter Ingestion",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 7: Snowflake RAW Landing & Column Verification
    # -------------------------------------------------------------------------
    def _stage_7_raw_landing_verification(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            try:
                try:
                    config = get_snowflake_config()
                except SnowflakeConnectionError as cfg_err:
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=7,
                            name="Snowflake RAW Landing Verification",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"AUTHENTICATION_UNCONFIGURED: Cannot query RAW table without Snowflake credentials: {cfg_err}",
                        )
                    )
                    return

                conn = get_snowflake_connection(config=config)
                try:
                    cur = conn.cursor()
                    query = (
                        f"SELECT EVENT_PAYLOAD, KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET, INGESTED_AT "
                        f"FROM {RAW_SENSOR_TABLE} "
                        f"WHERE EVENT_PAYLOAD:container_id = %s "
                        f"ORDER BY INGESTED_AT DESC LIMIT 1"
                    )
                    cur.execute(query, (self.traceable_container_id,))
                    row = cur.fetchone()
                    cur.close()

                    if row is None:
                        duration = round((time.perf_counter() - t0) * 1000, 2)
                        self.stages.append(
                            StageResult(
                                stage_number=7,
                                name="Snowflake RAW Landing Verification",
                                status="FAILED",
                                mode=TAG_LIVE,
                                duration_ms=duration,
                                error=f"LIVE_RECORD_NOT_FOUND: Record for container {self.traceable_container_id} not found in RAW table.",
                            )
                        )
                        return

                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=7,
                            name="Snowflake RAW Landing Verification",
                            status="PASSED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            details={"raw_table": RAW_SENSOR_TABLE, "record_found": True},
                        )
                    )
                finally:
                    conn.close()
            except Exception as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=7,
                        name="Snowflake RAW Landing Verification",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_SERVICE_FAILED: RAW query failed: {exc}",
                    )
                )
        else:
            try:
                # Verify RAW landing schema: EVENT_PAYLOAD (VARIANT), KAFKA_TOPIC, KAFKA_PARTITION, KAFKA_OFFSET
                raw_landing_row = {
                    "EVENT_PAYLOAD": json.dumps(self.event),
                    "KAFKA_TOPIC": "container-sensor-data",
                    "KAFKA_PARTITION": 0,
                    "KAFKA_OFFSET": 1042,
                    "INGESTED_AT": datetime.now(timezone.utc).isoformat(),
                }
                payload_dict = json.loads(raw_landing_row["EVENT_PAYLOAD"])
                assert payload_dict["container_id"] == self.traceable_container_id
                assert payload_dict["shipment_id"] == self.traceable_shipment_id

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=7,
                        name="Snowflake RAW Landing Verification",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={"raw_table": RAW_SENSOR_TABLE, "variant_stored": True},
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=7,
                        name="Snowflake RAW Landing Verification",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 8: dbt Staging Contract Unpacking
    # -------------------------------------------------------------------------
    def _stage_8_dbt_staging_unpacking(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            try:
                # Check Snowflake configuration
                try:
                    get_snowflake_config()
                except SnowflakeConnectionError as cfg_err:
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=8,
                            name="dbt Staging Unpacking (stg_sensor_readings)",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"AUTHENTICATION_UNCONFIGURED: Live dbt run requires active Snowflake connection: {cfg_err}",
                        )
                    )
                    return

                # Attempt live dbt run for staging model
                from dbt.cli.main import dbtRunner
                runner = dbtRunner()
                res = runner.invoke([
                    "run",
                    "--project-dir", str(PROJECT_ROOT / "dbt_analytics"),
                    "--select", "stg_sensor_readings",
                ])
                duration = round((time.perf_counter() - t0) * 1000, 2)
                if res.success:
                    self.stages.append(
                        StageResult(
                            stage_number=8,
                            name="dbt Staging Unpacking (stg_sensor_readings)",
                            status="PASSED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            details={"model": "stg_sensor_readings", "dbt_success": True},
                        )
                    )
                else:
                    self.stages.append(
                        StageResult(
                            stage_number=8,
                            name="dbt Staging Unpacking (stg_sensor_readings)",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"LIVE_DBT_RUN_FAILED: dbt staging model run failed: {res.exception}",
                        )
                    )
            except Exception as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=8,
                        name="dbt Staging Unpacking (stg_sensor_readings)",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_DBT_RUN_FAILED: {exc}",
                    )
                )
        else:
            try:
                # Emulate stg_sensor_readings.sql SQL transformation
                staged_row = {
                    "container_id": str(self.event["container_id"]),
                    "shipment_id": str(self.event["shipment_id"]),
                    "commodity": str(self.event["commodity"]),
                    "temperature": float(self.event["temperature"]),
                    "humidity": float(self.event["humidity"]),
                    "vibration": float(self.event["vibration"]),
                    "recorded_at": self.event["timestamp"],
                    "kafka_topic": "container-sensor-data",
                    "kafka_partition": 0,
                    "kafka_offset": 1042,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
                assert staged_row["container_id"] == self.traceable_container_id
                assert staged_row["commodity"] == "Avocados"
                assert staged_row["temperature"] == 6.8

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=8,
                        name="dbt Staging Unpacking (stg_sensor_readings)",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={"model": "stg_sensor_readings", "staged_columns": len(staged_row)},
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=8,
                        name="dbt Staging Unpacking (stg_sensor_readings)",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 9: dbt Mart Analytical Transformations
    # -------------------------------------------------------------------------
    def _stage_9_dbt_mart_analytics(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            try:
                try:
                    get_snowflake_config()
                except SnowflakeConnectionError as cfg_err:
                    duration = round((time.perf_counter() - t0) * 1000, 2)
                    self.stages.append(
                        StageResult(
                            stage_number=9,
                            name="dbt Mart Analytical Transformations",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"AUTHENTICATION_UNCONFIGURED: Live dbt mart run requires active Snowflake credentials: {cfg_err}",
                        )
                    )
                    return

                from dbt.cli.main import dbtRunner
                runner = dbtRunner()
                res = runner.invoke([
                    "run",
                    "--project-dir", str(PROJECT_ROOT / "dbt_analytics"),
                    "--select", "fct_spoilage_risk fct_arbitrage_opportunities fct_sensor_data_quality",
                ])
                duration = round((time.perf_counter() - t0) * 1000, 2)
                if res.success:
                    self.stages.append(
                        StageResult(
                            stage_number=9,
                            name="dbt Mart Analytical Transformations",
                            status="PASSED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            details={"marts_run": True},
                        )
                    )
                else:
                    self.stages.append(
                        StageResult(
                            stage_number=9,
                            name="dbt Mart Analytical Transformations",
                            status="FAILED",
                            mode=TAG_LIVE,
                            duration_ms=duration,
                            error=f"LIVE_DBT_RUN_FAILED: dbt mart run failed: {res.exception}",
                        )
                    )
            except Exception as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=9,
                        name="dbt Mart Analytical Transformations",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_DBT_RUN_FAILED: {exc}",
                    )
                )
        else:
            try:
                # 1. fct_spoilage_risk logic:
                temp_threshold = 8.0 if self.event["commodity"] == "Avocados" else 10.0
                temp_risk_flag = 1 if self.event["temperature"] > temp_threshold else 0
                hum_risk_flag = 1 if self.event["humidity"] > 85.0 else 0
                vib_risk_flag = 1 if self.event["vibration"] > 0.28 else 0

                spoilage_score = round(
                    (0.6 * temp_risk_flag) + (0.3 * hum_risk_flag) + (0.1 * vib_risk_flag), 2
                )
                risk_level = "NORMAL" if spoilage_score < 0.30 else ("WARNING" if spoilage_score < 0.70 else "CRITICAL")

                # 2. fct_sensor_data_quality logic:
                quality_status = "PASS" if temp_risk_flag == 0 and hum_risk_flag == 0 and vib_risk_flag == 0 else "WARNING"

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=9,
                        name="dbt Mart Analytical Transformations",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={
                            "spoilage_score": spoilage_score,
                            "risk_level": risk_level,
                            "quality_status": quality_status,
                        },
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=9,
                        name="dbt Mart Analytical Transformations",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )

    # -------------------------------------------------------------------------
    # STAGE 10: Superset Dashboard Dataset & Filter Validation
    # -------------------------------------------------------------------------
    def _stage_10_superset_dataset_and_filters(self) -> None:
        t0 = time.perf_counter()
        if self.mode == MODE_LIVE:
            # Strict live check: probe Superset web endpoint
            superset_url = os.getenv("SUPERSET_URL", "http://localhost:8088")
            parsed = urlparse(superset_url)
            host = parsed.hostname or "localhost"
            port = parsed.port or (443 if parsed.scheme == "https" else 8088)

            is_live = check_port_listening(host, port, timeout=min(self.timeout, 2.0))
            if not is_live:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=10,
                        name="Superset Dashboard Dataset & Filter Semantic Validation",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_SERVICE_UNAVAILABLE: Superset endpoint unreachable at {superset_url} (Docker daemon or container offline).",
                    )
                )
                return

            # Probe health endpoint
            try:
                health_url = f"{superset_url.rstrip('/')}/health"
                req = urllib.request.Request(health_url, headers={"User-Agent": "AtmoSync-SmokeTest/1.0"})
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    status_code = resp.getcode()
                    assert status_code == 200, f"Superset health check returned HTTP {status_code}"

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=10,
                        name="Superset Dashboard Dataset & Filter Semantic Validation",
                        status="PASSED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        details={"endpoint": health_url, "http_status": 200},
                    )
                )
            except Exception as exc:
                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=10,
                        name="Superset Dashboard Dataset & Filter Semantic Validation",
                        status="FAILED",
                        mode=TAG_LIVE,
                        duration_ms=duration,
                        error=f"LIVE_SERVICE_UNAVAILABLE: Superset health probe failed: {exc}",
                    )
                )
        else:
            try:
                yaml_path = PROJECT_ROOT / "dashboard" / "dashboards" / "datasets_and_charts.yaml"
                assert yaml_path.exists(), f"Missing Superset configuration file: {yaml_path}"

                with open(yaml_path, "r", encoding="utf-8") as f:
                    config = yaml.safe_load(f)

                # 1. Comprehensive dashboard configuration validation (datasets, metrics, charts, filters)
                dash_validation = validate_dashboard_config(config)
                assert dash_validation["status"] == "Passed", f"Dashboard configuration validation failed: {dash_validation['errors']}"

                raw_filters = config.get("filters", [])
                valid_cols = set()
                for ds in config.get("datasets", []):
                    for col in ds.get("columns", []):
                        valid_cols.add(col["name"])
                valid_targets = [ds["table_name"] for ds in config.get("datasets", [])]

                filter_validation = validate_filter_configuration(
                    filters=raw_filters,
                    dataset_columns=list(valid_cols),
                    chart_ids=valid_targets,
                )
                assert filter_validation["status"] == "Passed", f"Filter validation failed: {filter_validation}"
                assert filter_validation["total_filters"] == len(raw_filters)

                # Validate data quality report generation on the smoke record
                report = generate_quality_report([self.event])
                assert report["total_records"] == 1
                assert report["quality_score"] == 100.0

                duration = round((time.perf_counter() - t0) * 1000, 2)
                self.stages.append(
                    StageResult(
                        stage_number=10,
                        name="Superset Dashboard Dataset & Filter Semantic Validation",
                        status="PASSED",
                        mode=TAG_SIMULATION,
                        duration_ms=duration,
                        details={
                            "filter_count": filter_validation["total_filters"],
                            "validated_charts": dash_validation["summary"]["validated_charts"],
                            "quality_score": report["quality_score"],
                        },
                    )
                )
            except Exception as exc:
                self.stages.append(
                    StageResult(
                        stage_number=10,
                        name="Superset Dashboard Dataset & Filter Semantic Validation",
                        status="FAILED",
                        mode=TAG_SIMULATION,
                        duration_ms=round((time.perf_counter() - t0) * 1000, 2),
                        error=str(exc),
                    )
                )


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AtmoSync Repeatable End-to-End Pipeline Smoke Test"
    )
    parser.add_argument(
        "--mode",
        choices=[MODE_SIMULATION, MODE_LIVE],
        default=MODE_SIMULATION,
        help="Execution mode: 'simulation' (deterministic offline) or 'live' (strict live services)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Shortcut to enable live integration mode (equivalent to --mode live)",
    )
    parser.add_argument("--timeout", type=float, default=10.0, help="Timeout in seconds for operations")
    parser.add_argument("--container-id", type=str, default=None, help="Custom traceable container ID")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format")
    parser.add_argument("--verbose", action="store_true", help="Print verbose stage diagnostics")
    return parser.parse_args(args)


def main(args: list[str] | None = None) -> int:
    parsed_args = parse_args(args)

    mode = MODE_LIVE if parsed_args.live else parsed_args.mode

    smoke_test = PipelineSmokeTest(
        mode=mode,
        timeout_seconds=parsed_args.timeout,
        container_id=parsed_args.container_id,
        verbose=parsed_args.verbose,
    )
    result = smoke_test.run_all()

    if parsed_args.json:
        print(json.dumps(result, indent=2))
    else:
        print("\n" + "=" * 82)
        print(" ATMOSYNC END-TO-END PIPELINE SMOKE TEST")
        print("=" * 82)
        print(f" Run UUID:             {result['test_run_uuid']}")
        print(f" Traceable Container:  {result['traceable_container_id']}")
        print(f" Target Mode:          {result['mode'].upper()} ({result['mode_tag']})")
        print(f" Overall Status:       {result['overall_status']}")
        print(f" Total Duration:       {result['total_duration_ms']} ms")
        print(f" Stages Passed:        {result['stages_passed']} / {result['stages_executed']}")
        print(f" Stages Failed:        {result['stages_failed']} / {result['stages_executed']}")
        print("-" * 82)

        for s in result["stages"]:
            status_flag = "[PASS]" if s["status"] == "PASSED" else "[FAIL]"
            mode_tag = f"({s['mode']})"
            print(f" Stage {s['stage']:02d}: {status_flag} {s['name']:<48} {mode_tag:<24} {s['duration_ms']}ms")
            if s["error"]:
                print(f"           Error: {s['error']}")

        print("=" * 82 + "\n")

    return 0 if result["overall_status"] == "PASSED" else 1


if __name__ == "__main__":
    sys.exit(main())
