# Polling Trigger Runtime Semantics & Failure Scenarios

This page is the **single operational reference** for running `db.trigger` /
`PollTrigger` in production. It collects the runtime contract, duplicate
windows, lease and checkpoint timing, scale-out behavior, and recovery
procedures in one place.

If you only read one document before deploying the polling trigger, read this
one.

> **The polling trigger is a pseudo trigger.** It is **not** a native Azure
> Functions trigger and **not** a database trigger. It runs as a plain timer
> trigger that polls the source on every tick. Delivery is **at-least-once**.
> Handlers MUST be idempotent.
>
> For the formal contract see [Semantics](03-semantics.md). For the on-disk
> state format see [Checkpoint / Lease Spec](06-checkpoint-lease-spec.md).

---

## 1. Delivery Guarantee

`PollTrigger` and `SqlAlchemySource` deliver each row change to your handler
**at least once**. This is the only delivery guarantee the framework promises.

Concretely:

- A row that remains visible to the source query and satisfies the source
  preconditions will be delivered to the handler **at least once** before the
  checkpoint advances past it. Until the checkpoint commit for the batch
  containing that row succeeds, the same row may be delivered **more than
  once** (process crash, lease loss, retry after a failed commit, etc.).
- After the checkpoint commit succeeds, the framework will not intentionally
  re-fetch rows at or below that checkpoint on subsequent ticks.
- The framework does **not** provide exactly-once delivery, cross-instance
  global deduplication, or cross-database transactional acknowledgment.

This guarantee holds only when the source preconditions in
[Semantics §1.2](03-semantics.md#12-source-preconditions) are met:
monotonic non-decreasing cursor, stable PK with total ordering, deterministic
query.

Handlers must therefore be idempotent. See
[Idempotent Handler Pattern](#9-idempotent-handler-pattern) below.

---

## 2. Tick Lifecycle

Every timer firing executes a single `tick`. The runner performs the following
steps in order. Each step has well-defined failure behavior — see
[§5 Failure Scenarios](#5-failure-scenarios).

```text
1. acquire_lease (CAS write to state blob)
2. load_checkpoint (read state blob)
3. for batch_idx in range(max_batches_per_tick):
   3a. source.fetch(cursor, batch_size)
   3b. if no rows -> break
   3c. normalize raw records into RowChange events
   3d. invoke handler(events[, context])
   3e. commit_checkpoint (CAS write to state blob)
4. release_lease (best effort)
```

Key invariants:

- The handler is invoked **only after fetch + normalize succeed**. A fetch
  failure never reaches the handler.
- The checkpoint is **only advanced after the handler returns successfully**.
  Handler return ≠ acknowledgment; checkpoint commit success = acknowledgment.
- All state mutations are CAS writes against the same state blob. A stale
  owner cannot silently overwrite a newer owner's checkpoint.

---

## 3. Checkpoint Commit Timing

The checkpoint advances **after handler success and only via CAS commit**.

| Phase | Checkpoint state | Outcome on crash |
|---|---|---|
| Before fetch | Last committed checkpoint | No effect |
| After fetch, before handler | Last committed checkpoint | Same batch re-fetched on next tick |
| Handler running | Last committed checkpoint | Whole batch re-fetched on next tick |
| Handler returned, before commit | Last committed checkpoint | Whole batch re-delivered on next tick |
| Commit succeeded | New checkpoint | Subsequent ticks resume from new checkpoint |
| Commit ambiguous (timeout) | Unknown | Resolved by reload on next tick; worst case duplicate, never loss |

**Implication for replay:** any side effects performed by the handler before
the commit succeeds may be replayed. Plan side effects accordingly (idempotent
writes, upserts, dedup keys).

---

## 4. Duplicate Window Reference

The polling trigger can produce duplicates (or full re-deliveries of a batch)
in the following windows. This is the authoritative list — every "why does my
handler see the same row twice" question maps to one of these.

| Window | Cause | Re-delivery scope | Detectable from handler? |
|---|---|---|---|
| **W1. Handler success, commit failure** | Checkpoint CAS failed (network, transient blob error, lease lost) | Entire batch | No — `event` is identical |
| **W2. Crash after fetch, before handler returns** | Process killed mid-batch (instance recycle, OOM, deploy) | Entire batch | No |
| **W3. Crash with partial side effects** | Handler did N of M writes, then crashed | Entire batch (M writes redone) | No |
| **W4. Lease lost during processing** | Heartbeat / commit CAS rejected because another instance acquired the lease | Entire batch (next owner re-fetches) | Indirectly — `LostLeaseError` logged |
| **W5. Commit response timeout (ambiguous)** | Network timeout on the commit blob write; commit may or may not have persisted | Entire batch (only if commit actually failed) | No |
| **W6. Redeploy / restart** | New instance starts from last committed checkpoint | Whole or partial in-flight batch | No |
| **W7. Cursor precision collapse** | Multiple updates within one cursor tick collapse into one | Latest state only — earlier intermediate states lost | Yes (only one event arrives) |

Windows that **cannot** produce duplicates within this framework:

- A successful commit followed by a clean shutdown. The next tick will only
  see rows strictly newer than the committed cursor + tiebreaker PK.
- Two ticks racing on the same instance — `lease_ttl_seconds` and the single
  state blob CAS prevent this.

For the matching state-machine view see
[Semantics §12 Failure Matrix](03-semantics.md#12-failure-matrix) and
[§13 Duplicate and Reprocessing Windows](03-semantics.md#13-duplicate-and-reprocessing-windows).

---

## 5. Failure Scenarios

This section maps each failure to the runtime behavior, the metric / log
emitted, and the operator action (if any).

### 5.1 Lease cannot be acquired

**Cause:** another instance currently holds the lease and it has not expired.

**Behavior:** `tick()` returns `0` immediately. `LeaseAcquireError` is raised
internally, caught, and logged at DEBUG. **No handler invocation. No
checkpoint change.** The next timer firing retries normally.

**Operator action:** none required. Multiple instances polling the same source
is the expected scale-out shape — exactly one tick runs at a time.

### 5.2 Checkpoint blob load fails

**Cause:** transient blob storage error, missing container, RBAC issue,
network partition.

**Behavior:** `FetchError` raised. Handler is **not** invoked. Lease is
released. Next tick retries.

**Operator action:** verify storage account connectivity and the RBAC role on
the `db-state` container.

### 5.3 Source fetch fails

**Cause:** database unreachable, query error, driver-level exception.

**Behavior:** `FetchError` raised. Handler is **not** invoked. Lease is
released. Next tick retries (the cursor has not advanced).

**Operator action:** investigate the source database. Polling will resume
automatically once fetches succeed.

### 5.4 Record normalization fails

**Cause:** missing cursor or PK column in returned rows; non-serializable
cursor type.

**Behavior:** `SerializationError` raised. Handler is **not** invoked. The
cursor does not advance — the same fetch will fail again next tick. **This
is a poison configuration**, not a transient failure.

**Operator action:** fix the source query / table / `SqlAlchemySource`
configuration. The trigger cannot make progress until this is resolved.

### 5.5 Handler raises

**Cause:** business logic error, downstream sink unavailable, etc.

**Behavior:** `HandlerError` raised. Checkpoint **does not advance**. Lease is
released. The same batch will be re-fetched and re-delivered on the next tick.

**Operator action:** if the failure is transient (network, sink restart) the
trigger self-heals. If the same batch fails repeatedly you have a **poison
batch** (see [§5.9](#59-poison-batch-permanent-handler-failure)).

### 5.6 Checkpoint commit fails (lease lost)

**Cause:** another instance acquired the lease while the handler was running
and bumped the fencing token.

**Behavior:** `LostLeaseError` is raised on commit. Handler results are
discarded; the next owner re-fetches the same batch. This is window **W4**.

**Operator action:** none — this is the lease protocol working correctly.
Reduce duplication risk by ensuring `lease_ttl_seconds` exceeds the worst-case
handler duration; see [§7](#7-tuning-lease_ttl_seconds-and-timer-interval).

### 5.7 Checkpoint commit fails (other)

**Cause:** transient blob error, ETag mismatch, network timeout.

**Behavior:** `CommitError` raised. Handler **already ran**. The same batch
will be re-delivered on the next tick. This is window **W1** (pure failure)
or **W5** (timeout — commit may or may not have actually persisted).

**Operator action:** none required. Handler must be idempotent so the replay
is safe.

### 5.8 Heartbeat / lease loss mid-handler

The current implementation does not perform an in-flight heartbeat from inside
a long-running handler. If `handler_duration > lease_ttl_seconds`:

1. Another instance may acquire the lease via the expiry + grace window in
   `BlobCheckpointStore`.
2. The original handler's commit will fail with `LostLeaseError` (window W4).
3. The new owner will re-fetch and re-deliver the same batch.

**Operator action:** size `lease_ttl_seconds` so `lease_ttl_seconds > p99
handler duration + commit time + safety margin`. See
[§7](#7-tuning-lease_ttl_seconds-and-timer-interval).

### 5.9 Poison batch / permanent handler failure

**Cause:** a batch deterministically fails the handler (bad row, schema
mismatch, downstream rejection that will never recover).

**Behavior:** the same batch is reprocessed indefinitely. The trigger does
**not** auto-quarantine in MVP.

**Operator action (MVP):**

1. Identify the poison batch from `event=handler_failed` logs (look for the
   same `checkpoint_after` repeated across ticks).
2. Either fix the source data, fix the handler, or **manually advance the
   checkpoint** in the state blob to skip past the poison row. Treat manual
   advance as data loss — log it.
3. Track quarantine sink work in the project roadmap.

### 5.10 Storage / `BlobCheckpointStore` unavailability

**Cause:** Azure Blob Storage outage, expired credentials, deleted container.

**Behavior:** lease acquisition fails, ticks no-op (logged at DEBUG/ERROR
depending on the failure), no handler invocation, no checkpoint change.
Polling resumes automatically when storage becomes available.

**Operator action:** restore storage access. There is no special recovery
required — the last committed checkpoint is intact and polling resumes from
it.

---

## 6. Partial-Batch Failure Behavior

`PollTrigger` treats a batch as an atomic unit:

- If **any** row in the batch causes the handler to raise, the **entire
  batch** is considered failed and the checkpoint does not advance.
- The handler will be re-invoked on the next tick with the **same** events
  (same `pk`, `cursor`, `op`, `before`, `after`).
- There is no row-by-row commit, no quarantine of the failing row, and no
  automatic batch splitting in MVP.

If you need finer granularity, structure your handler to:

1. Catch per-row exceptions inside the handler.
2. Route failures to your own dead-letter sink (queue, table, log).
3. Let the handler return successfully so the checkpoint advances past the
   batch.

This pushes the partial-failure decision to the handler, where you can apply
business rules.

---

## 7. Tuning `lease_ttl_seconds` and timer interval

The two timing knobs you control are:

- The **timer schedule** on the wrapping `@app.schedule(...)` decorator.
- `lease_ttl_seconds` on `PollTrigger` / `db.trigger(...)` (default `120`).

Recommended sizing:

```text
lease_ttl_seconds  >  p99(fetch + handler + commit) + safety_margin (~30s)
timer_interval     >= lease_ttl_seconds / 2
```

Reasoning:

- `lease_ttl_seconds` must outlast the worst-case tick. If the handler runs
  longer than the TTL, another instance can steal the lease and you fall into
  window W4.
- The timer interval should be at least half the TTL so a single instance
  comfortably renews ownership across ticks. Faster timers under contention
  just produce more `LeaseAcquireError` no-ops.
- Use the lease grace window in `BlobCheckpointStore` (`min(ttl * 0.5, 5s)`)
  as your buffer for clock skew.

If you **cannot** bound your handler duration, split the work: write events to
a queue inside the handler and process them asynchronously downstream.

---

## 8. Timer Overlap and Scale-Out

### 8.1 Timer overlap on a single instance

Azure Functions timers can overlap if `use_monitor=False` and a tick takes
longer than the schedule interval. With the polling trigger this is **safe**:

- The first tick holds the lease.
- The second (overlapping) tick calls `acquire_lease`, gets `LeaseAcquireError`,
  and immediately returns `0`. No handler runs. No checkpoint change.

We still recommend `use_monitor=True` so the Functions host serializes ticks
where possible.

### 8.2 Multiple instances polling the same source

This is the supported scale-out shape and the **purpose** of the lease.

- All instances point at the **same** state blob (same poller name, same
  checkpoint store). The `source_fingerprint` field guarantees they agree on
  the source definition.
- On every tick, each instance attempts `acquire_lease`. Exactly one wins.
  The losers no-op.
- If the winner's handler runs longer than `lease_ttl_seconds`, the lease
  becomes stealable after the grace window. A new instance takes over and
  the original owner's commit is rejected via fencing token mismatch (W4).

**Do not** point multiple instances at different state blobs for the same
source. That deliberately runs the source twice and produces duplicates by
construction.

### 8.3 Multiple pollers on the same source

If you want **independent** consumers of the same source (e.g. a backfill
poller alongside the live one), give them **distinct `name`** values. Each
gets its own state blob and its own checkpoint. They do not coordinate, and
each delivers every row independently.

---

## 9. Idempotent Handler Pattern

Because every duplicate window in [§4](#4-duplicate-window-reference) replays
the **same** `RowChange` events, you can dedupe with a stable key derived from
the event itself. The recommended dedup key is:

```text
dedup_key = (event.pk, event.cursor)
```

`event.pk` is the source's primary key dict. `event.cursor` is the value of
the source's cursor column at the time the event was emitted. Together they
uniquely identify a single source-side state, even if the row is updated
again later.

Three idiomatic patterns:

### 9.1 Upsert at the sink

```python
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.output(
    "out",
    url="%DEST_DB_URL%",
    table="processed_orders",
    action="upsert",
    conflict_columns=["order_id", "cursor"],
)
def orders_poll(timer, events: list[RowChange], out: DbOut) -> None:
    out.set([
        {
            "order_id": e.pk["id"],
            "cursor": str(e.cursor),
            "after": e.after,
        }
        for e in events
    ])
```

Replays collide on `(order_id, cursor)` and become no-ops.

### 9.2 Processed-events table

Maintain a `processed_events(poller_name, pk_hash, cursor)` table with a
unique constraint. In a transaction:

1. Insert the dedup row. If it conflicts, skip (already processed).
2. Apply the side effect.

This works for sinks that are not natively upsert-friendly (HTTP APIs,
external systems).

### 9.3 Downstream idempotency key

Pass `f"{poller_name}:{pk_hash}:{cursor}"` to a downstream system that
supports idempotency keys (Stripe, many message brokers, custom HTTP APIs).

---

## 10. Operational Checklist

A short summary is below. The **full pre-deployment checklist** — including
runbook items, observability alert thresholds, and pool-configuration
checks — lives in
[Production Checklist — Polling Trigger](26-polling-production-checklist.md).

Before promoting a `db.trigger` to production:

- [ ] Confirm `lease_ttl_seconds > p99 handler duration + commit time + 30s`.
- [ ] Confirm timer interval ≥ `lease_ttl_seconds / 2`.
- [ ] Confirm the handler is idempotent under [§4](#4-duplicate-window-reference).
- [ ] Confirm `BlobCheckpointStore` has its own dedicated container and the
      Function App identity has only the RBAC needed for that container.
- [ ] Confirm the source query has a stable `ORDER BY cursor ASC, pk ASC` and
      the cursor column is monotonically non-decreasing.
- [ ] Confirm metrics are wired (`MetricsCollector`) and `failures_total`,
      `lag_seconds`, `last_success_timestamp` have alerts.
- [ ] Confirm the runbook covers manual checkpoint advance for poison
      batches.

---

## 11. See Also

- [Production Checklist — Polling Trigger](26-polling-production-checklist.md) —
  full pre-deployment checklist with runbook items.
- [Semantics](03-semantics.md) — formal delivery contract, ordering, cursor,
  failure matrix.
- [Checkpoint / Lease Spec](06-checkpoint-lease-spec.md) — on-disk state
  format, CAS algorithm, fencing tokens.
- [Architecture](02-architecture.md) — overall component layout.
- [ADR-004 At-Least-Once Default](19-ADR-004-at-least-once-default.md) — why
  this is the chosen guarantee level.
