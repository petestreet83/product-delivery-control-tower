from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .contract import TemporalUpdateContract
from .orchestrator import UpdateOrchestrator
from .storage import TemporalStore


class APIServer:
    def __init__(self, host: str, port: int, store: TemporalStore, orchestrator: UpdateOrchestrator, contract: TemporalUpdateContract):
        self.host = host
        self.port = port
        self.store = store
        self.orchestrator = orchestrator
        self.contract = contract

    def create_handler(self):
        store = self.store
        orchestrator = self.orchestrator
        contract = self.contract

        class Handler(BaseHTTPRequestHandler):
            def _send_json(self, status: int, body: dict | list):
                payload = json.dumps(body).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def _require(self, query: dict, name: str) -> str:
                value = query.get(name, [None])[0]
                if not value:
                    raise ValueError(f"missing query parameter: {name}")
                return value

            def do_GET(self):
                parsed = urlparse(self.path)
                query = parse_qs(parsed.query)

                if parsed.path == "/contract":
                    self._send_json(HTTPStatus.OK, asdict(contract))
                    return

                if parsed.path == "/metrics/latest":
                    try:
                        source = self._require(query, "source")
                        metric = self._require(query, "metric")
                    except ValueError as exc:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    latest = store.query_latest(source, metric)
                    if latest is None:
                        self._send_json(HTTPStatus.NOT_FOUND, {"error": "no metric found"})
                    else:
                        self._send_json(HTTPStatus.OK, latest)
                    return

                if parsed.path == "/metrics/history":
                    try:
                        source = self._require(query, "source")
                        metric = self._require(query, "metric")
                        start = self._require(query, "start")
                        end = self._require(query, "end")
                        datetime.fromisoformat(start)
                        datetime.fromisoformat(end)
                    except ValueError as exc:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    self._send_json(HTTPStatus.OK, store.query_history(source, metric, start, end))
                    return

                if parsed.path == "/metrics/delta":
                    try:
                        source = self._require(query, "source")
                        metric = self._require(query, "metric")
                        since = self._require(query, "since")
                        datetime.fromisoformat(since)
                    except ValueError as exc:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    self._send_json(HTTPStatus.OK, store.query_delta(source, metric, since))
                    return

                if parsed.path == "/metrics/freshness":
                    try:
                        source = self._require(query, "source")
                    except ValueError as exc:
                        self._send_json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
                        return
                    self._send_json(HTTPStatus.OK, store.query_freshness(source, contract.freshness_target_seconds))
                    return

                if parsed.path == "/connectors/health":
                    source = query.get("source", [None])[0]
                    self._send_json(HTTPStatus.OK, store.query_connector_health(source))
                    return

                if parsed.path == "/readyz":
                    self._send_json(HTTPStatus.OK, {"status": "ok"})
                    return

                self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})

            def do_POST(self):
                parsed = urlparse(self.path)
                if not parsed.path.startswith("/connectors/") or not parsed.path.endswith("/trigger"):
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "not found"})
                    return
                source_name = parsed.path.split("/")[2]
                if source_name not in orchestrator.connectors:
                    self._send_json(HTTPStatus.NOT_FOUND, {"error": "unknown connector"})
                    return
                result = orchestrator.run_connector_once(source_name, trigger_type="webhook")
                status = HTTPStatus.OK if result["status"] == "success" else HTTPStatus.BAD_GATEWAY
                self._send_json(status, result)

            def log_message(self, format: str, *args):
                return

        return Handler

    def serve_forever(self):
        server = ThreadingHTTPServer((self.host, self.port), self.create_handler())
        server.serve_forever()
