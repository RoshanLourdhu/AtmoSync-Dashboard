"""Unit and integration tests for telemetry_sink.py and kafka_consumer sink wiring."""

import csv
import io
import json
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import urlopen

import kafka_consumer
from telemetry_sink import (
    ALLOWED_PRICE_STATUSES,
    ALLOWED_STOCK_STATUSES,
    DBT_STAGING_FIELDS,
    TelemetrySQLiteSink,
    classify_price_status,
    classify_stock_status,
    create_api_server,
    to_dbt_commodity_row,
    validate_dbt_commodity_rows,
)


def _sample_event(
    event_id: str = "EV000101",
    timestamp_utc: str = "2026-11-27T09:00:00Z",
    container_id: str = "C001",
    commodity: str = "Fresh Apples",
    origin: str = "Chennai-Hub",
    destination: str = "Bengaluru-DC",
    quantity_kg: float = 40.0,
    temperature_c: float = 4.2,
    humidity_pct: float = 84.5,
    vibration_g: float = 0.12,
    sensor_status: str = "NORMAL",
    anomaly_type: str = "NONE",
) -> dict:
    return {
        "event_id": event_id,
        "timestamp_utc": timestamp_utc,
        "container_id": container_id,
        "commodity": commodity,
        "origin": origin,
        "destination": destination,
        "quantity_kg": quantity_kg,
        "temperature_c": temperature_c,
        "humidity_pct": humidity_pct,
        "vibration_g": vibration_g,
        "sensor_status": sensor_status,
        "anomaly_type": anomaly_type,
    }


class TestTelemetrySink(unittest.TestCase):
    def test_idempotent_persistence_and_alert_routing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            sink = TelemetrySQLiteSink(Path(tmpdir) / "telemetry.db")
            normal_ev = _sample_event(event_id="EV000001", sensor_status="NORMAL", anomaly_type="NONE")
            alert_ev = _sample_event(
                event_id="EV000002",
                container_id="C002",
                commodity="mRNA Vaccines",
                quantity_kg=8.0,
                temperature_c=11.4,
                sensor_status="ALERT",
                anomaly_type="DOOR_OPEN",
            )

            self.assertTrue(sink.write_event(normal_ev))
            self.assertFalse(sink.write_event(normal_ev))
            self.assertTrue(sink.write_event(alert_ev))

            self.assertEqual(sink.stats(), {"events_count": 2, "alerts_count": 1})
            alerts = sink.fetch_recent_alerts(limit=10)
            self.assertEqual(len(alerts), 1)
            self.assertEqual(alerts[0]["event_id"], "EV000002")
            self.assertEqual(alerts[0]["anomaly_type"], "DOOR_OPEN")

    def test_dbt_price_and_stock_classification_matches_fct_model(self) -> None:
        self.assertEqual(classify_stock_status(10.0), "Low Stock")
        self.assertEqual(classify_stock_status(10.1), "Medium Stock")
        self.assertEqual(classify_stock_status(50.0), "Medium Stock")
        self.assertEqual(classify_stock_status(50.1), "Available Stock")

        self.assertEqual(classify_price_status(49.99), "Low Price")
        self.assertEqual(classify_price_status(50.0), "Medium Price")
        self.assertEqual(classify_price_status(99.99), "Medium Price")
        self.assertEqual(classify_price_status(100.0), "High Price")

        rows = [
            to_dbt_commodity_row(_sample_event(commodity="Fresh Apples", quantity_kg=8.0)),
            to_dbt_commodity_row(_sample_event(commodity="Frozen Seafood", quantity_kg=35.0)),
            to_dbt_commodity_row(_sample_event(commodity="mRNA Vaccines", quantity_kg=120.0)),
        ]
        self.assertEqual(validate_dbt_commodity_rows(rows), [])
        self.assertEqual(rows[0]["PRICE_STATUS"], "Low Price")
        self.assertEqual(rows[0]["STOCK_STATUS"], "Low Stock")
        self.assertEqual(rows[1]["PRICE_STATUS"], "Medium Price")
        self.assertEqual(rows[1]["STOCK_STATUS"], "Medium Stock")
        self.assertEqual(rows[2]["PRICE_STATUS"], "High Price")
        self.assertEqual(rows[2]["STOCK_STATUS"], "Available Stock")

    def test_export_dbt_staging_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            sink = TelemetrySQLiteSink(base / "telemetry.db")
            sink.write_batch(
                [
                    _sample_event(event_id="EV000010", commodity="Dairy Milk", quantity_kg=9.0),
                    _sample_event(event_id="EV000011", commodity="mRNA Vaccines", quantity_kg=90.0),
                ]
            )
            csv_path = base / "dbt_staging.csv"
            count = sink.export_dbt_staging_csv(csv_path)
            self.assertEqual(count, 2)

            with csv_path.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 2)
            self.assertEqual(tuple(rows[0].keys()), DBT_STAGING_FIELDS)
            self.assertEqual(validate_dbt_commodity_rows(rows), [])
            self.assertEqual(rows[0]["STOCK_STATUS"], "Low Stock")
            self.assertEqual(rows[0]["PRICE_STATUS"], "Low Price")
            self.assertEqual(rows[1]["STOCK_STATUS"], "Available Stock")
            self.assertEqual(rows[1]["PRICE_STATUS"], "High Price")

    def test_rest_api_bridge_endpoints(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "api_test.db"
            sink = TelemetrySQLiteSink(db_path)
            sink.write_batch(
                [
                    _sample_event(
                        event_id="EV000021",
                        container_id="C001",
                        commodity="Frozen Seafood",
                        quantity_kg=30.0,
                    ),
                    _sample_event(
                        event_id="EV000022",
                        container_id="C002",
                        commodity="mRNA Vaccines",
                        quantity_kg=8.0,
                        temperature_c=-99.9,
                        sensor_status="FAULT",
                        anomaly_type="SENSOR_DROPOUT",
                    ),
                ]
            )

            server = create_api_server(db_path=db_path, host="127.0.0.1", port=0)
            port = server.server_address[1]
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                base_url = f"http://127.0.0.1:{port}"
                with urlopen(f"{base_url}/health") as resp:
                    health = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(health["events_count"], 2)
                self.assertEqual(health["alerts_count"], 1)

                with urlopen(f"{base_url}/api/telemetry?container_id=C001") as resp:
                    telemetry = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(telemetry["count"], 1)
                self.assertEqual(telemetry["events"][0]["container_id"], "C001")

                with urlopen(f"{base_url}/api/alerts") as resp:
                    alerts = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(alerts["count"], 1)
                self.assertEqual(alerts["alerts"][0]["sensor_status"], "FAULT")

                with urlopen(f"{base_url}/api/commodities") as resp:
                    commodities = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(commodities["count"], 2)
                self.assertEqual(validate_dbt_commodity_rows(commodities["commodities"]), [])
                self.assertEqual(commodities["commodities"][0]["PRICE_STATUS"], "Medium Price")
                self.assertEqual(commodities["commodities"][0]["STOCK_STATUS"], "Medium Stock")
                self.assertEqual(commodities["commodities"][1]["PRICE_STATUS"], "High Price")
                self.assertEqual(commodities["commodities"][1]["STOCK_STATUS"], "Low Stock")

                with self.assertRaises(HTTPError) as ctx:
                    urlopen(f"{base_url}/api/unknown")
                self.assertEqual(ctx.exception.code, 404)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)

    def test_consumer_dry_run_persists_to_sqlite_and_exports_dbt_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            db_path = base / "dry_run.db"
            csv_path = base / "dry_run_dbt.csv"
            buf = io.StringIO()
            with patch("sys.stdout", buf):
                kafka_consumer.main(
                    [
                        "--dry-run",
                        "--max-messages",
                        "5",
                        "--db-out",
                        str(db_path),
                        "--dbt-csv-out",
                        str(csv_path),
                    ]
                )
            output = buf.getvalue()
            self.assertIn("consumed=5", output)
            self.assertIn("persisted=5", output)

            sink = TelemetrySQLiteSink(db_path)
            self.assertEqual(sink.stats()["events_count"], 5)
            with csv_path.open("r", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 5)
            self.assertEqual(validate_dbt_commodity_rows(rows), [])
            for row in rows:
                self.assertIn(row["STOCK_STATUS"], ALLOWED_STOCK_STATUSES)
                self.assertIn(row["PRICE_STATUS"], ALLOWED_PRICE_STATUSES)

    def test_run_consumer_with_mock_records_persists_idempotently(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "mock_consumer.db"
            ev1 = kafka_consumer.generate_event(
                kafka_consumer.CONTAINERS[0], 1, anomaly_rate=0.0
            )
            ev2 = kafka_consumer.generate_event(
                kafka_consumer.CONTAINERS[1], 2, anomaly_rate=1.0
            )
            records = [
                SimpleNamespace(value=json.dumps(ev1).encode("utf-8")),
                SimpleNamespace(value=json.dumps(ev1).encode("utf-8")),
                SimpleNamespace(value=json.dumps(ev2).encode("utf-8")),
            ]
            args = kafka_consumer.parse_args(["--max-messages", "3", "--db-out", str(db_path)])
            buf = io.StringIO()
            with patch("sys.stdout", buf):
                summary = kafka_consumer.run_consumer(args, consumer=records)

            self.assertEqual(summary["consumed"], 3)
            self.assertEqual(summary["alerts"], 1)
            self.assertEqual(summary["persisted"], 2)


if __name__ == "__main__":
    unittest.main()
