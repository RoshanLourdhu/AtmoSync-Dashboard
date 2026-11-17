# AtmoSync – Kafka Consumer Reliability & Failure Recovery Report

- **Date:** 17 November 2026
- **Module:** `streaming/consumer/consumer.py`
- **Working Branch:** `roshan`
- **Remote Target:** `origin/roshan` (`https://github.com/RoshanLourdhu/AtmoSync-Dashboard.git`)
- **Lead:** Roshan (Kafka Consumer, Schema Validation & Snowflake Ingestion Lead)

---

## 1. Executive Summary & Problem Analysis

In production IoT streaming pipelines, network interruptions, database warehouse pauses, broker rebalances, and malformed telemetry can destabilize the consumer service. A technical audit of `streaming/consumer/consumer.py` revealed several critical operational reliability gaps:

1. **Stale Database Connection Retention:** When a transient database error occurred during batch ingestion, `self._snowflake_conn` was retained in memory without being closed or reset. Any subsequent retry attempts would attempt to reuse the broken session.
2. **Uncaught Snowflake Connection Errors:** `_get_snowflake_conn()` was invoked outside the batch exception handler, causing connection failures to escape without structured batch context logging or connection reset.
3. **Unprotected Kafka Offset Commits:** When `self.consumer.commit(asynchronous=False)` failed (e.g., due to a coordinator timeout or group rebalance), the error was unhandled and did not raise a domain exception.
4. **Poison-Pill Deadlock:** While mixed batches handled invalid events, batches composed *exclusively* of invalid messages did not log explicit poison-pill acknowledgement, leaving operators blind to whether the consumer was stuck or advancing.
5. **Credential Exposure Risk:** Dead-letter diagnostics (`raw_payload`) recorded raw message payloads without redacting sensitive tokens or credentials that might accidentally arrive in malformed events.
6. **Lack of Continuous Polling Lifecycle:** The consumer lacked a formal polling loop (`run()`) with graceful signal/event termination and structured lifecycle logging.

---

## 2. Implemented Reliability Enhancements

### A. Snowflake Failure Recovery & Automatic Reconnection
- **Unified Batch Exception Handling:** Database connection acquisition and batch ingestion now execute inside a unified `try ... except (SnowflakeConnectionError, SnowflakeIngestionError, Exception)` block.
- **Connection Reset Helper (`_reset_snowflake_conn()`):** If a database operation fails, the consumer automatically closes the failed socket and resets `self._snowflake_conn = None`. This guarantees that the next retry batch establishes a clean, fresh connection rather than reusing an aborted session.
- **Guaranteed Rollback & Offset Protection:** Transaction rollback in `insert_sensor_events` is preserved, and Kafka offsets **are never committed** when database persistence fails, ensuring at-least-once streaming delivery.

### B. Poison-Pill Isolation & Resilient Batch Handling
- **Poison-Pill Batch Acknowledgment:** When a batch contains exclusively invalid records (`valid_count == 0` and `invalid_records > 0`), the consumer safely acknowledges and commits the offset after quarantining the records. This prevents the partition from hanging in an infinite crash-loop.
- **Single and Multi-Event Batches:** Fully verified for single-message batches, multi-message batches, and empty polling timeouts (`messages == []`).

### C. Dead-Letter Sanitization (`sanitize_raw_payload`)
- Implemented `sanitize_raw_payload(value)` which inspects incoming raw payloads, redacting sensitive patterns (`password`, `token`, `secret`, `api_key`, `credential`) with `***REDACTED***` and truncating oversized payloads (> 500 characters) to prevent log bloat.

### D. Kafka Offset Commit Error Handling
- Offset commits (`self.consumer.commit(asynchronous=False)`) are now wrapped in error handling that logs errors and raises `KafkaConsumerError`, allowing upstream orchestrators to detect partition rebalance or broker disconnection issues.

### E. Structured Operational Observability
Added structured logging across every stage of the consumer lifecycle:
- Startup configuration summary (topic, group ID, batch size, auto offset reset, poll timeout) avoiding credential exposure.
- Batch receipt logging (`Received batch of X message(s) from Kafka`).
- Validation summary logging (`Batch validation summary: X valid, Y invalid`).
- Database ingestion start and completion metrics (`Starting Snowflake batch ingestion...`, `Successfully inserted X event(s)...`).
- Database ingestion failure alerts with event counts.
- Offset commit confirmations and failure alerts.
- Graceful shutdown lifecycle logging for both Kafka consumer and Snowflake connections.

### F. Continuous Execution Polling Loop (`run`)
- Implemented `SensorKafkaConsumer.run(max_batches, stop_event)` enabling controlled continuous consumption with clean shutdown in production containers or daemon tasks.

---

## 3. Automated Test Verification

The test suite in `tests/test_kafka_consumer.py` was expanded from 11 tests to **20 tests** (9 new reliability and failure-recovery test cases):

| Test Case | Description | Result |
| :--- | :--- | :--- |
| `test_consumer_config_from_env` | Environment configuration loading | **PASSED** |
| `test_deserialize_and_validate_valid_event` | Single event deserialization and schema validation | **PASSED** |
| `test_deserialize_and_validate_invalid_json` | Malformed JSON syntax rejection | **PASSED** |
| `test_deserialize_and_validate_non_object_json` | Non-dictionary JSON rejection | **PASSED** |
| `test_deserialize_and_validate_sam_unaligned_payload_rejected` | Unaligned contract rejection | **PASSED** |
| `test_poll_messages_respects_batch_size` | Polling limits and timeout respect | **PASSED** |
| `test_process_batch_successful_ingestion_and_commit` | End-to-end batch ingestion and offset commit | **PASSED** |
| `test_process_batch_database_failure_prevents_offset_commit` | Offset uncommitted on DB insertion error | **PASSED** |
| `test_process_batch_mixed_valid_and_invalid_events` | Isolation of invalid events during valid batch ingestion | **PASSED** |
| `test_process_batch_empty_list` | Safe no-op on empty message batch | **PASSED** |
| `test_consumer_close_cleanup` | Proper socket closure on shutdown | **PASSED** |
| `test_process_batch_snowflake_connection_failure` | Connection failure prevents commit and resets state | **PASSED** (New) |
| `test_process_batch_connection_reset_on_ingestion_failure` | Internal connection closed and reset on insertion error | **PASSED** (New) |
| `test_process_batch_single_event` | Single-event batch persistence and commit | **PASSED** (New) |
| `test_process_batch_pure_invalid_poison_pill_batch` | Poison-pill batch committed without database calls | **PASSED** (New) |
| `test_process_batch_kafka_offset_commit_failure` | Broker commit timeout raises `KafkaConsumerError` | **PASSED** (New) |
| `test_sanitize_raw_payload_masks_credentials` | Redaction of secrets and payload truncation | **PASSED** (New) |
| `test_consumer_run_loop_with_max_batches` | Run loop processes target batch count and closes | **PASSED** (New) |
| `test_consumer_run_loop_with_stop_event` | Run loop exits cleanly on signal event | **PASSED** (New) |
| `test_consumer_structured_logging` | Emitted logs verified across full lifecycle | **PASSED** (New) |

**Overall Project Test Suite:** **91 / 91 passed (100%)** in 2.30s.

---

## 4. How to Run the Tests

To execute the Kafka consumer reliability test suite:

```bash
# Run only the Kafka consumer reliability tests
python -m pytest -v tests/test_kafka_consumer.py

# Run both Kafka producer and consumer streaming integration tests
python -m pytest -v tests/test_kafka_producer.py tests/test_kafka_consumer.py

# Run the complete repository test suite
python -m pytest -v
```

---

## 5. Live Service Verification Boundaries

- **Mocked Verification:** All tests above use mocked Kafka message instances and mocked Snowflake connections to verify offset safety, transaction rollback, and error isolation without external dependencies.
- **Live Verification Requirement:** Real-time end-to-end validation requires an active Kafka broker on `localhost:9092` with topic `container-sensor-data` and a provisioned Snowflake account with table `ATMOSYNC_DB.RAW.SENSOR_READINGS`.
