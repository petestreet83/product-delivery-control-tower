from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.request
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path

from app.connectors import OpenMeteoConnector, RateLimitError, SchemaValidationError
from app.contract import TemporalUpdateContract
from app.orchestrator import UpdateOrchestrator
from app.server import APIServer
from app.storage import TemporalStore


class AlwaysFailConnector:
    source_name = "failing-source"
    source_version = "v1"

    def fetch_metrics(self):
        raise RateLimitError("rate limited by upstream API")


class FakeTriggerConnector:
    source_name = "fake-source"
    source_version = "v1"

    def fetch_metrics(self):
        return []


class TemporalPipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "test.db")

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_connector_normalizes_payload(self):
        connector = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {
                "current": {
                    "time": "2026-01-01T12:00:00",
                    "temperature_2m": 21.5,
                    "relative_humidity_2m": 48,
                }
            },
        )

        metrics = connector.fetch_metrics()

        self.assertEqual(2, len(metrics))
        self.assertEqual("temperature_2m", metrics[0].metric)
        self.assertEqual("relative_humidity_2m", metrics[1].metric)
        self.assertEqual("open-meteo", metrics[0].source)

    def test_connector_detects_schema_drift(self):
        connector = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {"current": {"time": "2026-01-01T12:00:00"}},
        )

        with self.assertRaises(SchemaValidationError):
            connector.fetch_metrics()

    def test_store_insert_is_idempotent(self):
        connector = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {
                "current": {
                    "time": "2026-01-01T12:00:00",
                    "temperature_2m": 21.5,
                    "relative_humidity_2m": 48,
                }
            },
        )
        store = TemporalStore(self.db_path)
        metrics = connector.fetch_metrics()

        inserted_first = store.insert_points(metrics)
        inserted_second = store.insert_points(metrics)

        self.assertEqual(2, inserted_first)
        self.assertEqual(0, inserted_second)

    def test_orchestrator_retries_and_dead_letters_on_rate_limit(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[AlwaysFailConnector()],
            contract=TemporalUpdateContract(max_retries=1, retry_backoff_seconds=0),
        )

        result = orchestrator.run_connector_once("failing-source")

        self.assertEqual("failed", result["status"])
        with sqlite3.connect(self.db_path) as conn:
            dead_letter_count = conn.execute("SELECT COUNT(*) FROM dead_letter_events").fetchone()[0]
            run_status = conn.execute("SELECT status FROM connector_runs ORDER BY id DESC LIMIT 1").fetchone()[0]
        self.assertEqual(1, dead_letter_count)
        self.assertEqual("failed", run_status)

    def test_server_latest_endpoint(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[FakeTriggerConnector()],
            contract=TemporalUpdateContract(cadence_seconds=300),
        )

        connector = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {
                "current": {
                    "time": "2026-01-01T12:00:00",
                    "temperature_2m": 21.5,
                    "relative_humidity_2m": 48,
                }
            },
        )
        store.insert_points(connector.fetch_metrics())

        api = APIServer(
            host="127.0.0.1",
            port=0,
            store=store,
            orchestrator=orchestrator,
            contract=TemporalUpdateContract(freshness_target_seconds=1000),
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.create_handler())
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/latest?source=open-meteo&metric=temperature_2m",
                timeout=5,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual("temperature_2m", payload["metric"])
            self.assertEqual("open-meteo", payload["source"])
        finally:
            httpd.shutdown()
            httpd.server_close()


if __name__ == "__main__":
    unittest.main()
