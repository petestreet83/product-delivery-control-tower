# product-delivery-control-tower

Temporal public-data connector service for a delivery control tower. It ingests public API data on a schedule, stores time-series snapshots, and exposes query/health endpoints for downstream applications.

## Implemented temporal update contract

- Cadence: configurable, default `60s`
- Freshness target: configurable, default `180s`
- Retry policy: exponential backoff with configurable retry count
- Backfill/storage guardrails: idempotent writes with temporal retention and hourly rollups

## Architecture

- `app/contract.py`: temporal update and retention contracts
- `app/connectors.py`: public API connector layer and normalization (`OpenMeteoConnector`)
- `app/storage.py`: SQLite time-series storage, connector run logs, dead-letter events, rollups/retention
- `app/orchestrator.py`: scheduled/manual trigger orchestration with retries and dead-letter handling
- `app/server.py`: HTTP API for latest, history, delta, freshness, connector health, and manual trigger
- `app/main.py`: runnable entrypoint

## Run

```bash
python -m app.main --db-path /tmp/control_tower.db --cadence-seconds 60 --trigger-token your-token
```

Optional connector location override:

```bash
python -m app.main --latitude 37.7749 --longitude -122.4194
```

## API contract

### `GET /contract`
Returns active temporal contract values.

### `GET /metrics/latest?source=<source>&metric=<metric>`
Returns latest point for a metric; if raw points were compacted, it falls back to the latest hourly rollup bucket.

### `GET /metrics/history?source=<source>&metric=<metric>&start=<iso>&end=<iso>`
Returns time-window history.

### `GET /metrics/delta?source=<source>&metric=<metric>&since=<iso>`
Returns delta since a timestamp.

### `GET /metrics/freshness?source=<source>`
Returns staleness and freshness status.

### `GET /connectors/health[?source=<source>]`
Returns connector success/failure rate and latency indicators.

### `POST /connectors/<source>/trigger`
Manual/webhook trigger for a connector run. Requires header `X-Trigger-Token: <token>` and only works when `--trigger-token` is configured.

### `GET /readyz`
Simple readiness endpoint.

## Testing

```bash
python -m unittest discover -s tests -v
```

Tests cover:

- Normalization from a public API payload
- Schema drift detection
- Retry + dead-letter behavior for rate limits
- Idempotent temporal inserts
- API query endpoint behavior


## Runtime behavior

`python -m app.main` starts both the scheduler thread and HTTP server in one process.
Stop the service with `Ctrl+C` (or a `SIGTERM` from your process manager) to end the server loop and shut down the scheduler thread.
