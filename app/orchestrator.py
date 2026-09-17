from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from .connectors import ConnectorError, PublicAPIConnector, RateLimitError
from .contract import TemporalUpdateContract
from .storage import TemporalStore


class UpdateOrchestrator:
    def __init__(self, store: TemporalStore, connectors: list[PublicAPIConnector], contract: TemporalUpdateContract):
        self.store = store
        self.contract = contract
        self.connectors = {c.source_name: c for c in connectors}
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def run_connector_once(self, source_name: str, trigger_type: str = "manual") -> dict:
        connector = self.connectors[source_name]
        started = datetime.now(timezone.utc)
        run_id = self.store.record_run_started(source_name, trigger_type, started.isoformat())

        retries = 0
        while True:
            try:
                metrics = connector.fetch_metrics()
                points_written = self.store.insert_points(metrics)
                self.store.apply_rollup_and_retention()
                finished = datetime.now(timezone.utc)
                latency_ms = int((finished - started).total_seconds() * 1000)
                self.store.record_run_finished(run_id, "success", finished.isoformat(), latency_ms, points_written)
                return {"source": source_name, "status": "success", "points_written": points_written}
            except (RateLimitError, ConnectorError) as exc:
                retries += 1
                if retries > self.contract.max_retries:
                    finished = datetime.now(timezone.utc)
                    latency_ms = int((finished - started).total_seconds() * 1000)
                    self.store.record_run_finished(run_id, "failed", finished.isoformat(), latency_ms, 0, str(exc))
                    self.store.record_dead_letter(source_name, trigger_type, finished.isoformat(), str(exc))
                    return {"source": source_name, "status": "failed", "error": str(exc)}
                sleep_seconds = self.contract.retry_backoff_seconds * (2 ** (retries - 1))
                time.sleep(sleep_seconds)

    def run_all_once(self, trigger_type: str = "schedule") -> list[dict]:
        return [self.run_connector_once(name, trigger_type=trigger_type) for name in self.connectors]

    def start_schedule(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()

        def _worker() -> None:
            while not self._stop_event.is_set():
                self.run_all_once(trigger_type="schedule")
                self._stop_event.wait(self.contract.cadence_seconds)

        self._thread = threading.Thread(target=_worker, daemon=True)
        self._thread.start()

    def stop_schedule(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)
