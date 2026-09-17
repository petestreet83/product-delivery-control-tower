from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable


class ConnectorError(Exception):
    pass


class RateLimitError(ConnectorError):
    pass


class SchemaValidationError(ConnectorError):
    pass


@dataclass(frozen=True)
class NormalizedMetric:
    source: str
    metric: str
    value: float
    unit: str
    event_timestamp: str
    ingest_timestamp: str
    source_version: str


class PublicAPIConnector:
    source_name: str
    source_version: str

    def fetch_metrics(self) -> list[NormalizedMetric]:
        raise NotImplementedError


class OpenMeteoConnector(PublicAPIConnector):
    source_name = "open-meteo"
    source_version = "v1"

    def __init__(self, latitude: float, longitude: float, timeout_seconds: int = 10, url_fetcher: Callable[[str, int], dict] | None = None):
        self.latitude = latitude
        self.longitude = longitude
        self.timeout_seconds = timeout_seconds
        self.url_fetcher = url_fetcher or self._default_fetcher

    def fetch_metrics(self) -> list[NormalizedMetric]:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={self.latitude}&longitude={self.longitude}"
            "&current=temperature_2m,relative_humidity_2m"
        )
        payload = self.url_fetcher(url, self.timeout_seconds)
        current = payload.get("current")
        if not isinstance(current, dict):
            raise SchemaValidationError("missing current payload")

        event_time = current.get("time")
        temp = current.get("temperature_2m")
        humidity = current.get("relative_humidity_2m")
        if event_time is None or temp is None or humidity is None:
            raise SchemaValidationError("missing expected fields in payload")

        ingest_time = datetime.now(timezone.utc).isoformat()
        return [
            NormalizedMetric(
                source=self.source_name,
                metric="temperature_2m",
                value=float(temp),
                unit="celsius",
                event_timestamp=self._to_iso(event_time),
                ingest_timestamp=ingest_time,
                source_version=self.source_version,
            ),
            NormalizedMetric(
                source=self.source_name,
                metric="relative_humidity_2m",
                value=float(humidity),
                unit="percent",
                event_timestamp=self._to_iso(event_time),
                ingest_timestamp=ingest_time,
                source_version=self.source_version,
            ),
        ]

    @staticmethod
    def _default_fetcher(url: str, timeout_seconds: int) -> dict:
        request = urllib.request.Request(url, headers={"User-Agent": "product-delivery-control-tower/1.0"})
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                status = response.getcode()
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            if exc.code == 429:
                raise RateLimitError("rate limited by upstream API") from exc
            raise ConnectorError(f"upstream HTTP error: {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ConnectorError("failed to reach upstream API") from exc

        if status == 429:
            raise RateLimitError("rate limited by upstream API")
        if status >= 400:
            raise ConnectorError(f"upstream HTTP error: {status}")

        data = json.loads(body)
        if not isinstance(data, dict):
            raise SchemaValidationError("invalid payload shape")
        return data

    @staticmethod
    def _to_iso(raw_event_time: str) -> str:
        normalized = raw_event_time.replace("Z", "+00:00")
        if "T" not in normalized:
            normalized = normalized + "T00:00:00+00:00"
        if "+" not in normalized and not normalized.endswith("Z"):
            normalized = normalized + "+00:00"
        return datetime.fromisoformat(normalized).astimezone(timezone.utc).isoformat()
