# Operations / Observability

## 1. Goals

When an issue occurs, operators must be able to answer the following questions:

- Is the poller currently running?
- When was the last successful execution?
- How far behind is it?
- Is the failure caused by the source, handler, or commit?
- Are duplicates increasing?

## 2. Structured Logging

Emit structured logs at every major step.

Required fields:
- poller_name
- invocation_id
- batch_id
- source
- fetched_count
- batch_size
- committed
- checkpoint_before
- checkpoint_after
- lease_owner
- duration_ms
- fetch_duration_ms
- handler_duration_ms
- commit_duration_ms
- lag_seconds
- error_type
- result

Optional / reserved fields:
- schedule_time — reserved for future use and not populated in the current implementation

Stable event names:
- `tick_start`
- `fetch_empty`
- `batch_complete`
- `tick_complete`
- `lease_acquire_failed`
- `checkpoint_load_failed`
- `source_descriptor_failed`
- `fetch_failed`
- `normalize_failed`
- `handler_failed`
- `commit_failed`
- `lease_release_failed`

## 3. Metrics

| Type | Metric | Status | Notes |
|------|--------|--------|-------|
| Counter | `azfdb_batches_total` | implemented | `result` label is `success` or `failure` |
| Counter | `azfdb_events_total` | implemented | Emits processed event count for successful batches |
| Counter | `azfdb_failures_total` | implemented | `error_type` label only |
| Counter | `azfdb_duplicates_total` | deferred | Requires duplicate detection strategy |
| Counter | `azfdb_quarantined_total` | deferred | Requires quarantine sink support |
| Histogram | `azfdb_fetch_duration_ms` | implemented | Fetch duration per successful batch |
| Histogram | `azfdb_handler_duration_ms` | implemented | Handler duration per successful batch |
| Histogram | `azfdb_commit_duration_ms` | implemented | Checkpoint commit duration per successful batch |
| Histogram | `azfdb_batch_size` | implemented | Successful batch size |
| Gauge | `azfdb_lag_seconds` | implemented | Timestamp cursor lag when cursor is ISO datetime |
| Gauge | `azfdb_last_success_timestamp` | implemented | Successful tick timestamp in UTC epoch seconds |
| Gauge | `azfdb_lease_staleness_seconds` | deferred | Requires lease age tracking |

Metrics labels stay low-cardinality:
- `poller_name`
- `source`
- `result`
- `error_type`

### 3.1. Custom MetricsCollector Example

```python
from collections.abc import Mapping

from azure_functions_db import MetricsCollector, PollTrigger


class PrintMetricsCollector:
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("increment", name, value, labels)

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("observe", name, value, labels)

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("gauge", name, value, labels)


trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=checkpoint_store,
    metrics=PrintMetricsCollector(),
)
```

### 3.2. Metric Query Examples

Examples below are deployment-agnostic and assume your metrics backend supports basic aggregation.

- Successful batches by poller: `sum(rate(azfdb_batches_total{result="success"}[5m])) by (poller_name)`
- Failure rate by error type: `sum(rate(azfdb_failures_total[5m])) by (poller_name, error_type)`
- Average batch size: `avg(azfdb_batch_size) by (poller_name, source)`
- Last successful tick age: `now() - azfdb_last_success_timestamp{poller_name="orders"}`
- Current lag: `azfdb_lag_seconds{poller_name="orders"}`

## 4. Tracing

Tracing is deferred to `azure-functions-db-otel` so the base package keeps zero extra dependencies.

When tracing support lands, the expected shape remains:
- poll tick
  - lease acquire
  - fetch
  - normalize
  - handler
  - checkpoint commit

## 5. Lag Definition

Lag definition varies by strategy.

### Cursor timestamp-based
```text
lag_seconds = now_utc - last_successful_cursor_timestamp
```

Current implementation details:
- Lag is emitted only after a successful checkpoint commit.
- The runner reads `checkpoint["cursor"]` from the just-committed checkpoint.
- If the cursor is an ISO 8601 datetime string, it is parsed directly.
- If the cursor is a tuple, only the first part is inspected for an ISO 8601 datetime string.
- Naive datetimes and non-datetime cursors are ignored.

### Integer sequence-based
Use the difference between DB max(version) and the checkpoint.

## 6. Alerts

Minimum alerts:
- No success for 15+ minutes
- 5 consecutive failures
- Sudden lag spike
- Stale lease
- Increasing quarantine count

## 7. Operational Commands

Supported via CLI or management scripts:

- checkpoint inspect
- checkpoint reset
- checkpoint clone
- force lease break (use with caution)
- dry-run fetch
- backfill preview

## 8. Quarantine

If a batch repeatedly fails, the payload can be sent to a quarantine sink.

MVP:
- optional blob sink

Format:
```json
{
  "poller_name": "orders",
  "batch_id": "batch_...",
  "error": "...",
  "events": [...]
}
```

## 9. Operations Dashboard

Recommended cards:
- poller health table
- last success
- lag
- events/min
- failure rate
- top error types

## 10. Runbook Examples

### When a batch keeps failing
1. Inspect checkpoint
2. Dry-run source query
3. Identify the offending row
4. Decide whether to roll back handler code
5. If necessary, send to quarantine — do not manually advance checkpoint

### When lag spikes suddenly
1. Consider increasing batch size
2. Consider increasing max_batches_per_tick
3. Adjust schedule
4. Check for handler downstream bottlenecks
