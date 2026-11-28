"""End-to-end integration tests for the Node.js/React dashboard proxy and dbt CSV schema."""

import csv
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from urllib.request import urlopen

from telemetry_sink import (
    TelemetrySQLiteSink,
    create_api_server,
    validate_dbt_commodity_rows,
)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class TestDashboardBridge(unittest.TestCase):
    def test_exported_dbt_staging_csv_passes_schema_validation(self) -> None:
        csv_path = Path("dbt_staging_commodities.csv")
        if not csv_path.exists():
            self.skipTest("dbt_staging_commodities.csv not generated in workspace")
        with csv_path.open("r", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
        self.assertGreater(len(rows), 0)
        self.assertEqual(validate_dbt_commodity_rows(rows), [])

    @unittest.skipUnless(shutil.which("node"), "Node.js runtime not installed")
    def test_node_dashboard_serves_react_ui_and_proxies_api(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "dash_test.db"
            sink = TelemetrySQLiteSink(db_path)
            sink.write_event(
                {
                    "event_id": "EV009001",
                    "timestamp_utc": "2026-11-27T12:00:00Z",
                    "container_id": "C001",
                    "commodity": "mRNA Vaccines",
                    "origin": "Chennai-Hub",
                    "destination": "Bengaluru-DC",
                    "quantity_kg": 8.0,
                    "temperature_c": 10.5,
                    "humidity_pct": 80.0,
                    "vibration_g": 0.45,
                    "sensor_status": "ALERT",
                    "anomaly_type": "DOOR_OPEN",
                }
            )

            api_server = create_api_server(db_path=db_path, host="127.0.0.1", port=0)
            api_port = api_server.server_address[1]
            api_thread = threading.Thread(target=api_server.serve_forever, daemon=True)
            api_thread.start()

            dash_port = _free_port()
            env = os.environ.copy()
            env["PORT"] = str(dash_port)
            env["HOST"] = "127.0.0.1"
            env["API_BASE_URL"] = f"http://127.0.0.1:{api_port}"

            proc = subprocess.Popen(
                ["node", "dashboard/server.js"],
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            try:
                base = f"http://127.0.0.1:{dash_port}"
                for _ in range(30):
                    try:
                        with urlopen(f"{base}/health", timeout=1) as resp:
                            if resp.status == 200:
                                break
                    except Exception:
                        time.sleep(0.1)

                with urlopen(f"{base}/") as resp:
                    html = resp.read().decode("utf-8")
                self.assertIn("AtmoSync Cold-Chain &amp; Commodity Dashboard", html)

                with urlopen(f"{base}/api/commodities") as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(payload["count"], 1)
                self.assertEqual(payload["commodities"][0]["PRICE_STATUS"], "High Price")
                self.assertEqual(payload["commodities"][0]["STOCK_STATUS"], "Low Stock")
            finally:
                proc.terminate()
                proc.wait(timeout=5)
                api_server.shutdown()
                api_server.server_close()
                api_thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
