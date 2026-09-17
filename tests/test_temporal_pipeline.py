from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

from app.connectors import NormalizedMetric, OpenMeteoConnector, RateLimitError, SchemaValidationError
from app.contract import RetentionPolicy, TemporalUpdateContract
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

    def test_connector_normalizes_z_and_offset_timestamps(self):
        connector_z = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {
                "current": {
                    "time": "2026-01-01T12:00:00Z",
                    "temperature_2m": 10.0,
                    "relative_humidity_2m": 50,
                }
            },
        )
        z_metrics = connector_z.fetch_metrics()
        self.assertEqual("2026-01-01T12:00:00+00:00", z_metrics[0].event_timestamp)

        connector_offset = OpenMeteoConnector(
            latitude=1.0,
            longitude=2.0,
            url_fetcher=lambda _url, _timeout: {
                "current": {
                    "time": "2026-01-01T07:00:00-05:00",
                    "temperature_2m": 10.0,
                    "relative_humidity_2m": 50,
                }
            },
        )
        offset_metrics = connector_offset.fetch_metrics()
        self.assertEqual("2026-01-01T12:00:00+00:00", offset_metrics[0].event_timestamp)

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

    def test_store_rejects_invalid_retention_policy(self):
        with self.assertRaises(ValueError):
            TemporalStore(self.db_path, retention_policy=RetentionPolicy(rollup_after_days=30, raw_retention_days=7))

    def test_rollup_and_retention_only_delete_rolled_data(self):
        policy = RetentionPolicy(rollup_after_days=1, raw_retention_days=2)
        store = TemporalStore(self.db_path, retention_policy=policy)
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(days=3)).isoformat()
        recent_ts = (now - timedelta(hours=12)).isoformat()
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=20.0,
                    unit="celsius",
                    event_timestamp=old_ts,
                    ingest_timestamp=old_ts,
                    source_version="v1",
                ),
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=22.0,
                    unit="celsius",
                    event_timestamp=recent_ts,
                    ingest_timestamp=recent_ts,
                    source_version="v1",
                ),
            ]
        )
        store.apply_rollup_and_retention(now=now)

        with sqlite3.connect(self.db_path) as conn:
            raw_count = conn.execute("SELECT COUNT(*) FROM metric_points").fetchone()[0]
            rollup_count = conn.execute("SELECT COUNT(*) FROM metric_rollups_hourly").fetchone()[0]
        self.assertEqual(1, raw_count)
        self.assertEqual(1, rollup_count)

    def test_history_and_delta_normalize_query_offsets(self):
        store = TemporalStore(self.db_path)
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=10.0,
                    unit="celsius",
                    event_timestamp="2026-01-01T17:00:00+00:00",
                    ingest_timestamp="2026-01-01T17:00:01+00:00",
                    source_version="v1",
                ),
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=14.0,
                    unit="celsius",
                    event_timestamp="2026-01-01T18:00:00+00:00",
                    ingest_timestamp="2026-01-01T18:00:01+00:00",
                    source_version="v1",
                ),
            ]
        )

        history = store.query_history(
            source="open-meteo",
            metric="temperature_2m",
            start="2026-01-01T12:00:00-05:00",
            end="2026-01-01T13:30:00-05:00",
        )
        delta = store.query_delta(
            source="open-meteo",
            metric="temperature_2m",
            since="2026-01-01T12:00:00-05:00",
        )
        self.assertEqual(2, len(history))
        self.assertEqual(4.0, delta["delta"])

    def test_freshness_falls_back_to_rollups(self):
        policy = RetentionPolicy(rollup_after_days=1, raw_retention_days=2)
        store = TemporalStore(self.db_path, retention_policy=policy)
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(days=3)).isoformat()
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=20.0,
                    unit="celsius",
                    event_timestamp=old_ts,
                    ingest_timestamp=old_ts,
                    source_version="v1",
                )
            ]
        )
        store.apply_rollup_and_retention(now=now)

        freshness = store.query_freshness("open-meteo", freshness_target_seconds=999999, now=now)
        self.assertIsNotNone(freshness["latest_event_timestamp"])

    def test_latest_falls_back_to_rollups(self):
        policy = RetentionPolicy(rollup_after_days=1, raw_retention_days=2)
        store = TemporalStore(self.db_path, retention_policy=policy)
        now = datetime.now(timezone.utc)
        old_ts = (now - timedelta(days=3)).isoformat()
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=20.0,
                    unit="celsius",
                    event_timestamp=old_ts,
                    ingest_timestamp=old_ts,
                    source_version="v1",
                )
            ]
        )
        store.apply_rollup_and_retention(now=now)
        latest = store.query_latest("open-meteo", "temperature_2m")
        self.assertEqual("rollup", latest["source_version"])

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

    def test_server_contract_and_ready_and_health(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[FakeTriggerConnector()],
            contract=TemporalUpdateContract(cadence_seconds=42, freshness_target_seconds=99),
        )
        api = APIServer(
            host="127.0.0.1",
            port=0,
            store=store,
            orchestrator=orchestrator,
            contract=TemporalUpdateContract(cadence_seconds=42, freshness_target_seconds=99),
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.create_handler())
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/contract", timeout=5) as response:
                contract_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(42, contract_payload["cadence_seconds"])

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=5) as response:
                ready_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual("ok", ready_payload["status"])

            with urllib.request.urlopen(f"http://127.0.0.1:{port}/connectors/health", timeout=5) as response:
                health_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual([], health_payload)
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_server_history_delta_and_freshness_endpoints(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[FakeTriggerConnector()],
            contract=TemporalUpdateContract(cadence_seconds=300),
        )
        past_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        future_ts = (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=20.0,
                    unit="celsius",
                    event_timestamp=past_ts,
                    ingest_timestamp=past_ts,
                    source_version="v1",
                ),
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=24.0,
                    unit="celsius",
                    event_timestamp=future_ts,
                    ingest_timestamp=future_ts,
                    source_version="v1",
                ),
            ]
        )

        api = APIServer(
            host="127.0.0.1",
            port=0,
            store=store,
            orchestrator=orchestrator,
            contract=TemporalUpdateContract(freshness_target_seconds=10**9),
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.create_handler())
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            start = urllib.parse.quote((datetime.now(timezone.utc) - timedelta(days=1)).isoformat())
            end = urllib.parse.quote((datetime.now(timezone.utc) + timedelta(days=1)).isoformat())
            since = urllib.parse.quote((datetime.now(timezone.utc) - timedelta(days=1)).isoformat())
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/history?source=open-meteo&metric=temperature_2m&start={start}&end={end}",
                timeout=5,
            ) as response:
                history_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(2, len(history_payload))

            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/delta?source=open-meteo&metric=temperature_2m&since={since}",
                timeout=5,
            ) as response:
                delta_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(4.0, delta_payload["delta"])

            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/freshness?source=open-meteo",
                timeout=5,
            ) as response:
                freshness_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual(0, freshness_payload["staleness_seconds"])
            self.assertTrue(freshness_payload["within_target"])
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_server_post_trigger_and_validation(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[FakeTriggerConnector()],
            contract=TemporalUpdateContract(cadence_seconds=300),
        )

        api = APIServer(
            host="127.0.0.1",
            port=0,
            store=store,
            orchestrator=orchestrator,
            contract=TemporalUpdateContract(),
            trigger_token="secret-token",
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.create_handler())
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            req = urllib.request.Request(
                f"http://127.0.0.1:{port}/connectors/fake-source/trigger",
                method="POST",
                headers={"X-Trigger-Token": "secret-token"},
            )
            with urllib.request.urlopen(req, timeout=5) as response:
                trigger_payload = json.loads(response.read().decode("utf-8"))
            self.assertEqual("success", trigger_payload["status"])

            bad_token_req = urllib.request.Request(
                f"http://127.0.0.1:{port}/connectors/fake-source/trigger",
                method="POST",
                headers={"X-Trigger-Token": "wrong-token"},
            )
            with self.assertRaises(HTTPError) as bad_token_error:
                urllib.request.urlopen(bad_token_req, timeout=5)
            self.assertEqual(401, bad_token_error.exception.code)

            bad_req = urllib.request.Request(
                f"http://127.0.0.1:{port}/metrics/latest?source=open-meteo",
                method="GET",
            )
            with self.assertRaisesRegex(Exception, "HTTP Error 400"):
                urllib.request.urlopen(bad_req, timeout=5)

            malformed_trigger_req = urllib.request.Request(
                f"http://127.0.0.1:{port}/connectors/fake-source/extra/trigger",
                method="POST",
            )
            with self.assertRaisesRegex(Exception, "HTTP Error 404"):
                urllib.request.urlopen(malformed_trigger_req, timeout=5)
        finally:
            httpd.shutdown()
            httpd.server_close()

    def test_server_freshness_for_stale_data(self):
        store = TemporalStore(self.db_path)
        orchestrator = UpdateOrchestrator(
            store=store,
            connectors=[FakeTriggerConnector()],
            contract=TemporalUpdateContract(cadence_seconds=300),
        )
        stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        store.insert_points(
            [
                NormalizedMetric(
                    source="open-meteo",
                    metric="temperature_2m",
                    value=20.0,
                    unit="celsius",
                    event_timestamp=stale_ts,
                    ingest_timestamp=stale_ts,
                    source_version="v1",
                )
            ]
        )

        api = APIServer(
            host="127.0.0.1",
            port=0,
            store=store,
            orchestrator=orchestrator,
            contract=TemporalUpdateContract(freshness_target_seconds=30),
        )
        httpd = ThreadingHTTPServer(("127.0.0.1", 0), api.create_handler())
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/metrics/freshness?source=open-meteo",
                timeout=5,
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertGreater(payload["staleness_seconds"], 0)
            self.assertFalse(payload["within_target"])
        finally:
            httpd.shutdown()
            httpd.server_close()

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
