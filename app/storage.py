from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from .connectors import NormalizedMetric
from .contract import RetentionPolicy


class TemporalStore:
    def __init__(self, db_path: str, retention_policy: RetentionPolicy | None = None):
        self.db_path = db_path
        self.retention_policy = retention_policy or RetentionPolicy()
        if self.retention_policy.raw_retention_days < self.retention_policy.rollup_after_days:
            raise ValueError("raw_retention_days must be greater than or equal to rollup_after_days")
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS metric_points (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value REAL NOT NULL,
                    unit TEXT NOT NULL,
                    event_timestamp TEXT NOT NULL,
                    ingest_timestamp TEXT NOT NULL,
                    source_version TEXT NOT NULL,
                    UNIQUE(source, metric, event_timestamp)
                );
                CREATE TABLE IF NOT EXISTS metric_rollups_hourly (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    bucket_start TEXT NOT NULL,
                    avg_value REAL NOT NULL,
                    min_value REAL NOT NULL,
                    max_value REAL NOT NULL,
                    sample_count INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(source, metric, bucket_start)
                );
                CREATE TABLE IF NOT EXISTS connector_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    trigger_type TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    latency_ms INTEGER,
                    points_written INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS dead_letter_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    trigger_type TEXT NOT NULL,
                    failed_at TEXT NOT NULL,
                    error_message TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_metric_points_source_metric_time
                    ON metric_points (source, metric, event_timestamp);
                CREATE INDEX IF NOT EXISTS idx_metric_points_source_time
                    ON metric_points (source, event_timestamp);
                CREATE INDEX IF NOT EXISTS idx_connector_runs_source_status_finished
                    ON connector_runs (source, status, finished_at);
                """
            )

    def record_run_started(self, source: str, trigger_type: str, started_at: str) -> int:
        with self._conn() as conn:
            cur = conn.execute(
                "INSERT INTO connector_runs (source, trigger_type, started_at, status) VALUES (?, ?, ?, 'running')",
                (source, trigger_type, started_at),
            )
            return int(cur.lastrowid)

    def record_run_finished(self, run_id: int, status: str, finished_at: str, latency_ms: int, points_written: int, error_message: str | None = None) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE connector_runs
                SET status = ?, finished_at = ?, latency_ms = ?, points_written = ?, error_message = ?
                WHERE id = ?
                """,
                (status, finished_at, latency_ms, points_written, error_message, run_id),
            )

    def record_dead_letter(self, source: str, trigger_type: str, failed_at: str, error_message: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO dead_letter_events (source, trigger_type, failed_at, error_message) VALUES (?, ?, ?, ?)",
                (source, trigger_type, failed_at, error_message),
            )

    def insert_points(self, metrics: list[NormalizedMetric]) -> int:
        rows = [
            (
                m.source,
                m.metric,
                m.value,
                m.unit,
                self._normalize_iso_utc(m.event_timestamp),
                self._normalize_iso_utc(m.ingest_timestamp),
                m.source_version,
            )
            for m in metrics
        ]
        with self._conn() as conn:
            cur = conn.executemany(
                """
                INSERT OR IGNORE INTO metric_points
                (source, metric, value, unit, event_timestamp, ingest_timestamp, source_version)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return cur.rowcount if cur.rowcount is not None else 0

    def apply_rollup_and_retention(self, now: datetime | None = None) -> None:
        now = now or datetime.now(timezone.utc)
        rollup_cutoff = (now - timedelta(days=self.retention_policy.rollup_after_days)).isoformat()
        delete_cutoff = (now - timedelta(days=self.retention_policy.raw_retention_days)).isoformat()

        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO metric_rollups_hourly
                (source, metric, bucket_start, avg_value, min_value, max_value, sample_count, created_at)
                SELECT
                    source,
                    metric,
                    substr(event_timestamp, 1, 13) || ':00:00+00:00',
                    AVG(value),
                    MIN(value),
                    MAX(value),
                    COUNT(*),
                    ?
                FROM metric_points
                WHERE event_timestamp < ?
                GROUP BY source, metric, substr(event_timestamp, 1, 13)
                ON CONFLICT(source, metric, bucket_start) DO UPDATE SET
                    avg_value = excluded.avg_value,
                    min_value = excluded.min_value,
                    max_value = excluded.max_value,
                    sample_count = excluded.sample_count,
                    created_at = excluded.created_at
                """,
                (now.isoformat(), rollup_cutoff),
            )
            conn.execute(
                """
                DELETE FROM metric_points
                WHERE event_timestamp < ?
                  AND event_timestamp < ?
                  AND EXISTS (
                      SELECT 1
                      FROM metric_rollups_hourly r
                      WHERE r.source = metric_points.source
                        AND r.metric = metric_points.metric
                        AND r.bucket_start = substr(metric_points.event_timestamp, 1, 13) || ':00:00+00:00'
                  )
                """,
                (delete_cutoff, rollup_cutoff),
            )

    def query_latest(self, source: str, metric: str) -> dict | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT source, metric, value, unit, event_timestamp, ingest_timestamp, source_version
                FROM metric_points
                WHERE source = ? AND metric = ?
                ORDER BY event_timestamp DESC
                LIMIT 1
                """,
                (source, metric),
            ).fetchone()
            if row:
                return dict(row)
            rollup_row = conn.execute(
                """
                SELECT source, metric, avg_value, bucket_start
                FROM metric_rollups_hourly
                WHERE source = ? AND metric = ?
                ORDER BY bucket_start DESC
                LIMIT 1
                """,
                (source, metric),
            ).fetchone()
            if not rollup_row:
                return None
            return {
                "source": rollup_row["source"],
                "metric": rollup_row["metric"],
                "value": rollup_row["avg_value"],
                "unit": None,
                "event_timestamp": rollup_row["bucket_start"],
                "ingest_timestamp": None,
                "source_version": "rollup",
            }

    def query_history(self, source: str, metric: str, start: str, end: str) -> list[dict]:
        normalized_start = self._normalize_iso_utc(start)
        normalized_end = self._normalize_iso_utc(end)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT source, metric, value, unit, event_timestamp, ingest_timestamp, source_version
                FROM metric_points
                WHERE source = ? AND metric = ? AND event_timestamp BETWEEN ? AND ?
                ORDER BY event_timestamp ASC
                """,
                (source, metric, normalized_start, normalized_end),
            ).fetchall()
            if rows:
                return [dict(r) for r in rows]
            rollup_rows = conn.execute(
                """
                SELECT source, metric, avg_value, bucket_start
                FROM metric_rollups_hourly
                WHERE source = ? AND metric = ? AND bucket_start BETWEEN ? AND ?
                ORDER BY bucket_start ASC
                """,
                (source, metric, normalized_start, normalized_end),
            ).fetchall()
            return [
                {
                    "source": r["source"],
                    "metric": r["metric"],
                    "value": r["avg_value"],
                    "unit": None,
                    "event_timestamp": r["bucket_start"],
                    "ingest_timestamp": None,
                    "source_version": "rollup",
                }
                for r in rollup_rows
            ]

    def query_delta(self, source: str, metric: str, since: str) -> dict:
        normalized_since = self._normalize_iso_utc(since)
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT
                    (SELECT value
                     FROM metric_points
                     WHERE source = ? AND metric = ? AND event_timestamp >= ?
                     ORDER BY event_timestamp ASC
                     LIMIT 1) AS first_value,
                    (SELECT value
                     FROM metric_points
                     WHERE source = ? AND metric = ? AND event_timestamp >= ?
                     ORDER BY event_timestamp DESC
                     LIMIT 1) AS last_value,
                    COUNT(*) AS samples
                FROM metric_points
                WHERE source = ? AND metric = ? AND event_timestamp >= ?
                """,
                (
                    source,
                    metric,
                    normalized_since,
                    source,
                    metric,
                    normalized_since,
                    source,
                    metric,
                    normalized_since,
                ),
            ).fetchone()
            samples = int(row["samples"]) if row else 0
            if samples < 2 or row["first_value"] is None or row["last_value"] is None:
                rollup_rows = conn.execute(
                    """
                    SELECT avg_value
                    FROM metric_rollups_hourly
                    WHERE source = ? AND metric = ? AND bucket_start >= ?
                    ORDER BY bucket_start ASC
                    """,
                    (source, metric, normalized_since),
                ).fetchall()
                rollup_values = [r["avg_value"] for r in rollup_rows]
                if len(rollup_values) < 2:
                    return {"delta": None, "samples": max(samples, len(rollup_values))}
                return {"delta": rollup_values[-1] - rollup_values[0], "samples": len(rollup_values)}
            return {"delta": row["last_value"] - row["first_value"], "samples": samples}

    def query_freshness(self, source: str, freshness_target_seconds: int, now: datetime | None = None) -> dict:
        now = now or datetime.now(timezone.utc)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT MAX(event_timestamp) AS latest_ts FROM metric_points WHERE source = ?",
                (source,),
            ).fetchone()
            latest_ts = row["latest_ts"] if row else None
            if latest_ts is None:
                rollup_row = conn.execute(
                    "SELECT MAX(bucket_start) AS latest_bucket FROM metric_rollups_hourly WHERE source = ?",
                    (source,),
                ).fetchone()
                latest_ts = rollup_row["latest_bucket"] if rollup_row else None
            if latest_ts is None:
                return {"source": source, "staleness_seconds": None, "within_target": False}
            latest = datetime.fromisoformat(latest_ts)
            staleness = max(0, int((now - latest).total_seconds()))
            return {
                "source": source,
                "latest_event_timestamp": latest_ts,
                "staleness_seconds": staleness,
                "within_target": staleness <= freshness_target_seconds,
            }

    def query_connector_health(self, source: str | None = None) -> list[dict]:
        query = """
            SELECT source,
                   COUNT(*) AS total_runs,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success_runs,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs,
                   MAX(finished_at) AS last_finished_at,
                   MAX(CASE WHEN status = 'success' THEN finished_at END) AS last_success_at,
                   AVG(latency_ms) AS avg_latency_ms
            FROM connector_runs
        """
        params: tuple = ()
        if source:
            query += " WHERE source = ?"
            params = (source,)
        query += " GROUP BY source"

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
            health = []
            for row in rows:
                total = row["total_runs"] or 0
                failed = row["failed_runs"] or 0
                health.append(
                    {
                        "source": row["source"],
                        "total_runs": total,
                        "success_runs": row["success_runs"] or 0,
                        "failed_runs": failed,
                        "failure_rate": (failed / total) if total else 0,
                        "last_finished_at": row["last_finished_at"],
                        "last_success_at": row["last_success_at"],
                        "avg_latency_ms": float(row["avg_latency_ms"]) if row["avg_latency_ms"] is not None else None,
                    }
                )
            return health

    def is_ready(self) -> bool:
        try:
            with self._conn() as conn:
                conn.execute("SELECT 1").fetchone()
            return True
        except sqlite3.Error:
            return False

    @staticmethod
    def _normalize_iso_utc(value: str) -> str:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).isoformat()
