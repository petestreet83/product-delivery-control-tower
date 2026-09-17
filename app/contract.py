from dataclasses import dataclass


@dataclass(frozen=True)
class TemporalUpdateContract:
    cadence_seconds: int = 60
    freshness_target_seconds: int = 180
    max_backfill_points: int = 1000
    max_retries: int = 3
    retry_backoff_seconds: float = 1.0


@dataclass(frozen=True)
class RetentionPolicy:
    rollup_after_days: int = 7
    raw_retention_days: int = 30
