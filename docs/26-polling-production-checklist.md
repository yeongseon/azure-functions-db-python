# Production Checklist — Polling Trigger

This page is the **pre-deployment checklist** for running `db.trigger` /
`PollTrigger` in production. It is the operator-runnable companion to
[Polling Runtime & Failure Scenarios](24-polling-runtime-semantics.md) and
[EngineProvider & Pooling Guidance](25-engine-provider-pooling.md). Walk
through every item below before promoting a polling trigger to production.

> If you skipped the runtime semantics doc, read at least [§1 Delivery
> Guarantee](24-polling-runtime-semantics.md#1-delivery-guarantee) and
> [§4 Duplicate Window Reference](24-polling-runtime-semantics.md#4-duplicate-window-reference)
> first. None of the items here make sense in isolation.

---

## 1. Handler correctness

- [ ] **The handler is idempotent.** A redelivery of any `RowChange` produces
      the same final state at the sink. Verify with the
      [duplicate window reference](24-polling-runtime-semantics.md#4-duplicate-window-reference).
- [ ] **A dedup key is documented.** The recommended default is
      `(poller_name, event.pk, event.cursor)`. If the sink does not
      natively support upsert, a `processed_events` table with a unique
      constraint on the dedup key is in place.
- [ ] **No partial in-batch state survives a handler exception.** If the
      handler raises mid-batch, any side effects already performed are
      either transactional (rolled back), idempotent on replay, or
      explicitly routed to a dead-letter sink.
- [ ] **Async handlers offload blocking work correctly.** If the handler is
      `async def`, it does not block the event loop on long sync work
      (the package already wraps DB calls in `asyncio.to_thread`; user code
      must do the same for its own blocking calls).

## 2. Source design

- [ ] **The cursor column is monotonically non-decreasing on every
      mutation you care about.** `created_at` alone is **not** sufficient
      if rows are mutated in place — use `updated_at` maintained by a
      `BEFORE INSERT OR UPDATE` trigger, a `version` column, or an outbox
      pattern. See
      [Semantics §1.2](03-semantics.md#12-source-preconditions).
- [ ] **The cursor column is indexed.** A composite index on
      `(cursor_column, pk_columns...)` is present so the source query
      `WHERE (cursor, pk) > (last_cursor, last_pk) ORDER BY cursor, pk
      LIMIT batch_size` runs as an index scan, not a sort over the whole
      table.
- [ ] **The PK columns are stable and totally orderable.** Tuples of
      stable surrogate keys (BIGINT, UUID v7) are fine; mutable natural
      keys are not.
- [ ] **Hard deletes are accounted for.** If the source allows hard
      deletes, you have either a soft-delete column, a tombstone table,
      or accept that hard deletes are not detected by the polling
      trigger. See
      [Semantics §4](03-semantics.md#4-delete-semantics).
- [ ] **Backfill uses a separate `name` and a separate state blob.** Do
      not point a backfill poller at the live poller's checkpoint. See
      [Semantics §11](03-semantics.md#11-backfill-mode).

## 3. Lease and timer sizing

- [ ] `lease_ttl_seconds > p99(fetch_ms + handler_ms + commit_ms) + 30s`.
- [ ] `timer_interval >= lease_ttl_seconds / 2`.
- [ ] `batch_size` chosen so that one batch's worst-case handler duration
      stays well below `lease_ttl_seconds`. The default `100` is a
      starting point; lower it before you raise `lease_ttl_seconds`.
- [ ] `max_batches_per_tick` matches your throughput needs. Increasing it
      raises tick duration linearly — recompute the lease budget if you
      change it.
- [ ] **You have measured `p99` handler duration in a load test or in
      production with a low-traffic poller**, not just guessed it. The
      runtime emits `azfdb_handler_duration_ms` as a metric (see §6).

See the formula and reasoning in
[Polling Runtime §7](24-polling-runtime-semantics.md#7-tuning-lease_ttl_seconds-and-timer-interval).

## 4. Engine and pool configuration

- [ ] A **module-level** `EngineProvider` is shared across the source and
      every binding that targets the same database.
- [ ] `engine_kwargs` is **identical** across bindings that should share
      a pool — otherwise the cache key splits and you build extra
      engines (see
      [EngineProvider §3.2](25-engine-provider-pooling.md#32-cache-key)).
- [ ] `pool_pre_ping=True` is set for every managed-database binding.
- [ ] `pool_recycle` is set **below** the database's server-side idle
      timeout (defaults: PG Flexible 5 min → 240s; MySQL 8h →
      `1800`; Azure SQL ~30 min → `1500`).
- [ ] `(pool_size + max_overflow) × max_function_app_instances ×
      workers_per_instance` stays well below the database's
      `max_connections` ceiling.
- [ ] `pool_timeout` is set explicitly (default 30s); a queue-bound
      function should fail fast rather than hang on a saturated pool.

## 5. Checkpoint blob and identity

- [ ] **Dedicated container** (default `db-state`) — not shared with
      `azure-webjobs-hosts` or other system containers.
- [ ] **Container is pre-created in production** with versioning /
      soft-delete enabled per your storage account's data-protection
      policy. (Azurite auto-creates; production does not.)
- [ ] **Function App identity has scoped RBAC**:
      `Storage Blob Data Contributor` on the `db-state` container only.
      Avoid account-wide roles.
- [ ] **One state blob per production poller.** No instance points at
      another poller's blob. Confirm with `state/{app_name}/{poller_name}.json`.
- [ ] **`source_fingerprint` is unchanged from last deploy** — if you
      changed `table`, `cursor_column`, `pk_columns`, or filters, the
      fingerprint mismatch will reject ticks until you reset
      deliberately. See
      [Checkpoint / Lease Spec §9](06-checkpoint-lease-spec.md#9-source-fingerprint).
- [ ] **Storage retry policy** on `ContainerClient` matches the timer
      schedule (the default Azure SDK retry is fine for ≥1-minute timer
      intervals; tighten for sub-minute schedules).

## 6. Observability

A `MetricsCollector` is wired to your metrics backend, and the following
signals have alerts. All metrics are emitted with the `azfdb_` prefix
(see [`src/azure_functions_db/observability.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/src/azure_functions_db/observability.py)
for the canonical names) and are labeled with `poller_name`.

- [ ] **`azfdb_failures_total`** — non-zero rate over a 5–10 min window pages
      on-call.
- [ ] **`azfdb_lag_seconds`** — gauge exceeding `2 × timer_interval` for more
      than 2 ticks indicates the trigger is falling behind.
- [ ] **`azfdb_last_success_timestamp`** — `now - last_success > 3 ×
      timer_interval` indicates the trigger is stuck (no successful tick).
- [ ] **`azfdb_batches_total{result="failure"}`** — repeating failures on the
      same `checkpoint_after` indicate a poison batch (see §7).
- [ ] **Structured logs** (`event=tick_complete`, `event=handler_failed`,
      `event=commit_failed`, `event=lease_acquire_failed`) flow into your
      log store with `poller_name` and `invocation_id` searchable.
- [ ] A dashboard shows `azfdb_handler_duration_ms`, `azfdb_commit_duration_ms`,
      and `azfdb_batch_size` percentiles per `poller_name` so you can
      detect drift before it breaches `lease_ttl_seconds`.

For the metric inventory see
[`src/azure_functions_db/observability.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/src/azure_functions_db/observability.py)
and the README **Observability** section.

## 7. Runbook items

The on-call runbook covers each of the following recovery paths.

### 7.1 Poison batch (same batch fails repeatedly)

1. Identify the failing batch: search for
   `event=handler_failed` with the same `checkpoint_after.cursor` repeated
   across ticks.
2. Decide the resolution:
   - **Fix forward** — patch the handler or the source row, redeploy.
     The next tick re-delivers the batch and succeeds.
   - **Skip forward (data loss)** — update the state blob to advance
     `checkpoint.cursor` past the poison row. Document this as a data
     incident.
3. There is no automatic quarantine sink in MVP. See
   [Polling Runtime §5.9](24-polling-runtime-semantics.md#59-poison-batch-permanent-handler-failure).

### 7.2 Lost lease / fencing rejection

1. Symptom: `LostLeaseError` in logs, `azfdb_failures_total{error_type="LostLeaseError"}`
   spiking.
2. Most common cause: handler duration exceeded `lease_ttl_seconds`.
   Check `azfdb_handler_duration_ms` p99 against `lease_ttl_seconds`.
3. Resolution: raise `lease_ttl_seconds`, lower `batch_size`, or split
   long-running side effects into a queue + worker pattern.

### 7.3 Storage outage

1. Symptom: `event=lease_acquire_failed` for every tick, no checkpoint
   movement.
2. The trigger self-heals once storage recovers. The last committed
   checkpoint is intact.
3. Confirm the storage account is reachable and the Function App
   identity still has the scoped RBAC role.

### 7.4 Source fingerprint mismatch after migration

1. Symptom: `FingerprintMismatchError` on every tick after a schema
   migration that changed `table`, `cursor_column`, `pk_columns`, or
   filters.
2. Decide whether to **resume from the existing checkpoint** (only safe
   if the cursor semantics did not change) or **reset and replay** (use a
   new `name` for the poller, point at a new state blob, decide whether
   to backfill).
3. There is no implicit reset in MVP. See
   [Checkpoint / Lease Spec §10](06-checkpoint-lease-spec.md#10-reset-policy).

### 7.5 Manual checkpoint advance

1. Last resort. Treat as a documented data incident.
2. Acquire the lease (or wait for it to expire).
3. Read the state blob, edit `checkpoint.cursor` and
   `checkpoint.last_successful_batch_id`, write back with the matching
   ETag.
4. Capture the before/after blob in the incident ticket.

## 8. Pre-deploy smoke

The following smoke runs against the production environment before
traffic is enabled:

- [ ] Deploy with the timer **disabled** for the smoke. The recommended
      mechanism on the v2 model is the per-function disable app setting
      (`AzureWebJobs.<FUNCTION_NAME>.Disabled=true`); a separate slot or
      a dedicated smoke environment also works. Avoid commenting out the
      `@app.schedule` decorator — that's a code change, not an
      operational toggle. Verify the Function App boots and the
      `EngineProvider` resolves the URL from app settings.
- [ ] Manually invoke the function once with a fixed timer payload.
      Verify a single successful tick: `event=tick_complete`,
      `result=success`, `total_processed=0` (no rows yet) or the
      expected backfill count.
- [ ] Verify the state blob exists and contains the expected
      `source_fingerprint` and an initial `checkpoint`.
- [ ] Re-enable the timer (`AzureWebJobs.<FUNCTION_NAME>.Disabled=false`
      or remove the setting).

---

## See Also

- [Polling Runtime & Failure Scenarios](24-polling-runtime-semantics.md) — operational reference.
- [EngineProvider & Pooling Guidance](25-engine-provider-pooling.md) — pool sizing detail.
- [Semantics](03-semantics.md) — formal contract.
- [Checkpoint / Lease Spec](06-checkpoint-lease-spec.md) — state blob format and CAS algorithm.
