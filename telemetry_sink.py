"""Downstream SQLite sink, dbt staging exporter, and REST JSON bridge for AtmoSync."""

from __future__ import annotations

import argparse
import contextlib
import csv
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import sqlite3
from typing import Any, Iterable
from urllib.parse import parse_qs, urlparse

ALLOWED_STOCK_STATUSES = ("Low Stock", "Medium Stock", "Available Stock")

DBT_STAGING_FIELDS = (
    "COMMODITY_ID",
    "COMMODITY",
    "QUANTITY",
    "STOCK_STATUS",
    "CONTAINER_ID",
    "ORIGIN",
    "DESTINATION",
    "TEMPERATURE_C",
    "HUMIDITY_PCT",
    "VIBRATION_G",
    "SENSOR_STATUS",
    "ANOMALY_TYPE",
    "EVENT_ID",
    "TIMESTAMP_UTC",
)


def classify_stock_status(quantity_kg: float) -> str:
    """Map container quantity_kg into dbt schema.yml accepted STOCK_STATUS tiers."""
    qty = float(quantity_kg)
    if qty < 250.0:
        return "Low Stock"
    if qty < 750.0:
        return "Medium Stock"
    return "Available Stock"


def to_dbt_commodity_row(event: dict[str, Any]) -> dict[str, Any]:
    """Transform a 12-field telemetry payload into a Snowflake/dbt staging record."""
    quantity = round(float(event["quantity_kg"]), 2)
    commodity = str(event["commodity"])
    return {
        "COMMODITY_ID": commodity,
        "COMMODITY": commodity,
        "QUANTITY": quantity,
        "STOCK_STATUS": classify_stock_status(quantity),
        "CONTAINER_ID": str(event["container_id"]),
        "ORIGIN": str(event["origin"]),
        "DESTINATION": str(event["destination"]),
        "TEMPERATURE_C": round(float(event["temperature_c"]), 2),
        "HUMIDITY_PCT": round(float(event["humidity_pct"]), 2),
        "VIBRATION_G": round(float(event["vibration_g"]), 4),
        "SENSOR_STATUS": str(event["sensor_status"]),
        "ANOMALY_TYPE": str(event["anomaly_type"]),
        "EVENT_ID": str(event["event_id"]),
        "TIMESTAMP_UTC": str(event["timestamp_utc"]),
    }


class TelemetrySQLiteSink:
    """Persistent SQLite storage and query interface for validated telemetry events."""

    def __init__(self, db_path: str | Path = "atmosync_telemetry.db") -> None:
        self.db_path = Path(db_path)
        self.init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def init_schema(self) -> None:
        with contextlib.closing(self._connect()) as conn, conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS telemetry_events (
                    event_id TEXT PRIMARY KEY,
                    timestamp_utc TEXT NOT NULL,
                    container_id TEXT NOT NULL,
                    commodity TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    quantity_kg REAL NOT NULL,
                    temperature_c REAL NOT NULL,
                    humidity_pct REAL NOT NULL,
                    vibration_g REAL NOT NULL,
                    sensor_status TEXT NOT NULL,
                    anomaly_type TEXT NOT NULL,
                    ingested_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS anomaly_alerts (
                    event_id TEXT PRIMARY KEY,
                    timestamp_utc TEXT NOT NULL,
                    container_id TEXT NOT NULL,
                    commodity TEXT NOT NULL,
                    origin TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    quantity_kg REAL NOT NULL,
                    temperature_c REAL NOT NULL,
                    humidity_pct REAL NOT NULL,
                    vibration_g REAL NOT NULL,
                    sensor_status TEXT NOT NULL,
                    anomaly_type TEXT NOT NULL,
                    ingested_at_utc TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_events_container ON telemetry_events(container_id, timestamp_utc)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_alerts_status ON anomaly_alerts(sensor_status, timestamp_utc)"
            )

    def write_event(self, event: dict[str, Any]) -> bool:
        """Persist one validated event; returns False if event_id already exists."""
        return self.write_batch([event]) == 1

    def write_batch(self, events: Iterable[dict[str, Any]]) -> int:
        """Persist a batch of validated events idempotently."""
        inserted = 0
        ingested_at = datetime.now(timezone.utc).isoformat()
        with contextlib.closing(self._connect()) as conn, conn:
            for event in events:
                sensor_status = str(event["sensor_status"])
                row_values = (
                    str(event["event_id"]),
                    str(event["timestamp_utc"]),
                    str(event["container_id"]),
                    str(event["commodity"]),
                    str(event["origin"]),
                    str(event["destination"]),
                    float(event["quantity_kg"]),
                    float(event["temperature_c"]),
                    float(event["humidity_pct"]),
                    float(event["vibration_g"]),
                    sensor_status,
                    str(event["anomaly_type"]),
                    ingested_at,
                )
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO telemetry_events (
                        event_id, timestamp_utc, container_id, commodity, origin,
                        destination, quantity_kg, temperature_c, humidity_pct,
                        vibration_g, sensor_status, anomaly_type, ingested_at_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row_values,
                )
                if cur.rowcount == 1:
                    inserted += 1
                    if sensor_status.upper() != "NORMAL":
                        conn.execute(
                            """
                            INSERT OR IGNORE INTO anomaly_alerts (
                                event_id, timestamp_utc, container_id, commodity, origin,
                                destination, quantity_kg, temperature_c, humidity_pct,
                                vibration_g, sensor_status, anomaly_type, ingested_at_utc
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            row_values,
                        )
        return inserted

    def fetch_recent_events(
        self, limit: int = 50, container_id: str | None = None
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 500))
        with contextlib.closing(self._connect()) as conn:
            if container_id:
                rows = conn.execute(
                    """
                    SELECT * FROM telemetry_events
                    WHERE container_id = ?
                    ORDER BY timestamp_utc DESC, rowid DESC
                    LIMIT ?
                    """,
                    (container_id, safe_limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT * FROM telemetry_events
                    ORDER BY timestamp_utc DESC, rowid DESC
                    LIMIT ?
                    """,
                    (safe_limit,),
                ).fetchall()
        return [dict(row) for row in rows]

    def fetch_recent_alerts(self, limit: int = 25) -> list[dict[str, Any]]:
        safe_limit = max(1, min(int(limit), 500))
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT * FROM anomaly_alerts
                ORDER BY timestamp_utc DESC, rowid DESC
                LIMIT ?
                """,
                (safe_limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def fetch_commodity_conditions(self) -> list[dict[str, Any]]:
        """Return latest dbt-aligned condition rows per commodity for dashboard APIs."""
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                """
                SELECT t.*
                FROM telemetry_events t
                INNER JOIN (
                    SELECT commodity, MAX(rowid) AS max_rowid
                    FROM telemetry_events
                    GROUP BY commodity
                ) latest ON t.rowid = latest.max_rowid
                ORDER BY t.commodity ASC
                """
            ).fetchall()
        return [to_dbt_commodity_row(dict(row)) for row in rows]

    def export_dbt_staging_csv(self, csv_path: str | Path) -> int:
        """Export persisted events into Snowflake/dbt staging CSV format."""
        out_path = Path(csv_path)
        with contextlib.closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM telemetry_events ORDER BY rowid ASC"
            ).fetchall()
        mapped = [to_dbt_commodity_row(dict(row)) for row in rows]
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(DBT_STAGING_FIELDS))
            writer.writeheader()
            writer.writerows(mapped)
        return len(mapped)

    def stats(self) -> dict[str, int]:
        with contextlib.closing(self._connect()) as conn:
            events_count = conn.execute(
                "SELECT COUNT(*) FROM telemetry_events"
            ).fetchone()[0]
            alerts_count = conn.execute(
                "SELECT COUNT(*) FROM anomaly_alerts"
            ).fetchone()[0]
        return {"events_count": int(events_count), "alerts_count": int(alerts_count)}


def create_api_server(
    db_path: str | Path = "atmosync_telemetry.db",
    host: str = "127.0.0.1",
    port: int = 8000,
) -> ThreadingHTTPServer:
    """Create a CORS-enabled JSON HTTP server backed by TelemetrySQLiteSink."""
    sink = TelemetrySQLiteSink(db_path=db_path)

    class TelemetryApiHandler(BaseHTTPRequestHandler):
        def _send_json(self, status_code: int, payload: Any) -> None:
            body = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
            self.send_header("Access-Control-Allow-Headers", "Content-Type")
            self.end_headers()
            self.wfile.write(body)

        def do_OPTIONS(self) -> None:  # noqa: N802
            self._send_json(200, {"status": "ok"})

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            params = parse_qs(parsed.query)
            if parsed.path == "/health":
                self._send_json(200, {"status": "ok", **sink.stats()})
            elif parsed.path == "/api/telemetry":
                limit = int(params.get("limit", ["50"])[0])
                container_id = params.get("container_id", [None])[0]
                items = sink.fetch_recent_events(limit=limit, container_id=container_id)
                self._send_json(200, {"count": len(items), "events": items})
            elif parsed.path == "/api/alerts":
                limit = int(params.get("limit", ["25"])[0])
                items = sink.fetch_recent_alerts(limit=limit)
                self._send_json(200, {"count": len(items), "alerts": items})
            elif parsed.path == "/api/commodities":
                items = sink.fetch_commodity_conditions()
                self._send_json(200, {"count": len(items), "commodities": items})
            else:
                self._send_json(404, {"error": "Not Found", "path": parsed.path})

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

    return ThreadingHTTPServer((host, port), TelemetryApiHandler)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="AtmoSync SQLite sink, dbt staging exporter, and dashboard JSON bridge."
    )
    parser.add_argument("--db", default="atmosync_telemetry.db", help="Path to SQLite database.")
    parser.add_argument("--export-dbt-csv", default=None, help="Export dbt staging CSV to path.")
    parser.add_argument("--serve", action="store_true", help="Start REST JSON API server.")
    parser.add_argument("--host", default="127.0.0.1", help="API bind host.")
    parser.add_argument("--port", type=int, default=8000, help="API bind port.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    sink = TelemetrySQLiteSink(db_path=args.db)
    if args.export_dbt_csv:
        exported = sink.export_dbt_staging_csv(args.export_dbt_csv)
        print(f"[SINK DBT-CSV] exported={exported} path={args.export_dbt_csv}")
    if args.serve:
        server = create_api_server(db_path=args.db, host=args.host, port=args.port)
        actual_host, actual_port = server.server_address
        print(f"[SINK API] Listening on http://{actual_host}:{actual_port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.shutdown()
            server.server_close()
    elif not args.export_dbt_csv:
        print(f"[SINK STATS] db={args.db} {sink.stats()}")


if __name__ == "__main__":
    main()
