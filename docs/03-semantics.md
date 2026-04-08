# Semantics

This document defines the **most important contracts** of the project.

## 1. Core Guarantees

### 1.1 Delivery Contract (Precise Definition)
The default guarantee level is **at-least-once**.

Precise definition:
> Any change visible to the adapter query after checkpoint `C` at the source
> may be delivered to the handler zero or more times until the checkpoint commit for that batch succeeds.
> Once the checkpoint commit succeeds, the framework will not intentionally re-read rows at or below that checkpoint.
> This guarantee is only valid when the source preconditions below are satisfied.

### 1.2 Source Preconditions
Conditions the source must satisfy for the framework to operate correctly:
- The cursor column must be **monotonically non-decreasing**
- The PK/tiebreaker must be **stable and support total ordering**
- The source query must be **deterministic**
- If cursor precision is too low, intermediate updates may be collapsed into one

### 1.3 Acknowledgment Boundary
- **Handler success ≠ acknowledgment**. Even if the handler succeeds, if the checkpoint commit fails, the batch remains unacknowledged.
- **Checkpoint commit success = acknowledgment**. Only at this point is the batch finalized.
- If the commit response is ambiguous (timeout), the next tick resolves this by reloading state.

### 1.4 Not Exactly-Once
The following are not guaranteed:

- Global exactly-once
- Complete cross-instance deduplication
- Cross-DB transactional exactly-once

### 1.5 Ordering
Default ordering is only attempted within the following scope:

- Within the same poller
- Within the same source query
- `ORDER BY cursor ASC, pk ASC`

Global ordering is not guaranteed.

## 2. Checkpoint Semantics

A checkpoint represents the "last event **successfully processed** up to this point."

The default format is:

```text
(cursor_value, tiebreaker_pk, batch_id)
```

### Important
The checkpoint advances **after handler success**, not immediately after fetch.

## 3. Duplicate Tolerance Policy

Duplicates may occur in the following situations:

- Process exits between handler success and checkpoint commit
- Another instance reprocesses after lease expiry
- Timer overlap
- Re-reads due to DB isolation characteristics
- Redeployment/restart

Therefore, user handlers must be idempotent by default.

Recommended patterns:
- event_id-based deduplication
- Target-side upsert
- Processed table tracking
- Downstream idempotency key usage

## 4. Delete Semantics

In the cursor polling strategy, hard deletes are not detected by default.

Supported approaches:
- Soft delete column (`deleted_at`, `is_deleted`)
- Outbox row recording
- Native CDC strategy
- Tombstone table

If documentation or code claims delete support, it must be **specified per strategy**.

## 5. Cursor Semantics

### Supported Cursor Types
- Monotonic integer/bigint
- timestamp/datetime
- Logical version column
- Composite cursor

### Recommended Cursors
- `updated_at` + stable PK
- `version` + stable PK

### Not Recommended
- Strings with unstable sort order
- Naive datetime with unclear timezone
- Single `created_at` value that is never updated

## 6. Timestamp Tie Handling

Multiple rows sharing the same timestamp may be split across batch boundaries.
Therefore, the checkpoint uses **timestamp + PK tiebreaker** rather than a single timestamp.

Sort rules:
1. cursor ASC
2. PK ASC (tuple ASC for composite PKs)

## 7. Batch Semantics

A batch is the set of events delivered to the handler in a single invocation.

Default policy:
- Fixed upper limit on batch size
- Checkpoint advances when the entire batch succeeds
- If any row in the batch fails, the entire batch is reprocessed

### Future Options
- Row-by-row commit
- Partial batch acknowledgment
- Quarantine split

Not included in MVP.

## 8. Retry Semantics

Functions runtime retry and library-internal retry must not be confused.

Principles:
- DB fetch / retriable infrastructure errors: library-internal bounded retry is allowed
- Handler business errors: surfaced as function failure
- Checkpoint commit error: if commit fails, the batch remains unconfirmed

## 9. Lease Semantics

A lease is the "write authority for the current poller execution."

Rules:
- Checkpoint commits are forbidden without a valid lease
- Commits with a lower fencing token are rejected
- A heartbeat is required before lease expiry

## 10. Visibility Delay

A pseudo trigger is not a real-time push trigger.
The delay is approximately determined by:

```text
visibility_delay ~= schedule_interval + query_time + handler_time + commit_time
```

Users must understand this value when configuring schedules.

## 11. Backfill Mode

Backfill has different semantics from normal operation mode.

- Large-scale historical replay
- Throughput takes priority over high-frequency trigger-like responsiveness
- Should be separable from the normal operational checkpoint

Recommended:
- Use a distinct poller name
- Use a distinct checkpoint namespace

## 12. Failure Matrix

### Case A: Handler Failure
- Checkpoint does not advance
- Can be reprocessed on the next tick

### Case B: Handler Success, Commit Failure
- Batch can be reprocessed
- Duplicates may occur
- Handler success is not acknowledgment

### Case C: Commit Attempt After Lease Loss
- CAS etag mismatch causes automatic commit rejection
- Duplicates possible (another instance reprocesses the same batch)
- Loss prevention takes priority

### Case D: DB Row Updated Again After Being Updated
- Only the latest state may be visible
- Before/after diff depends on the strategy

### Case E: Crash After Fetch but Before Handler Starts
- No checkpoint change
- Same batch reprocessed on next tick
- Duplicates possible, no loss

### Case F: Crash After Partial Side Effects During Handler
- No checkpoint change
- Entire batch reprocessed
- Handler must be idempotent

### Case G: Commit Response Timeout (Ambiguous State)
- Commit success is uncertain
- Resolved by reloading state on the next tick
- Worst case: duplicate, no loss

### Case H: Lease Lost Due to Heartbeat Failure
- Lease already expired even if handler is still running
- CAS fails on commit attempt
- Handler result is discarded; next owner reprocesses

### Case I: Permanently Failing Batch (Poison Message)
- The same batch may be retried indefinitely
- Must be routed to a quarantine sink or requires manual checkpoint advance
- MVP requires operator intervention

## 13. Duplicate and Reprocessing Windows

The following situations produce duplicate or reprocessed events. Handlers MUST be idempotent.

| Window | What Happens | Why | Recommended Handler Behavior |
|--------|-------------|-----|------------------------------|
| Handler success, commit failure (Case B) | Batch reprocessed on next tick | Checkpoint not persisted despite handler completing | Use event_id-based deduplication or upsert |
| Process crash before checkpoint commit (Case E) | Entire batch reprocessed | Crash after fetch/normalize, handler never ran or partially ran | Idempotent side effects or transactional writes |
| Process crash after partial handler (Case F) | Entire batch reprocessed | Handler partially executed but checkpoint never committed | Idempotent side effects or transactional writes |
| Lease lost during processing (Case C/H) | Another instance reprocesses same batch | CAS rejects stale commit; new owner starts fresh | Dedupe via processed-event table |
| Commit timeout / ambiguous (Case G) | Possible duplicate if commit actually succeeded | Response lost; next tick reloads and may re-fetch | Upsert or dedupe at destination |
| Redeployment / restart | Partial batch re-delivered | New instance starts from last committed checkpoint | Same as Case E/F |

> **Key principle**: azure-functions-db provides **at-least-once** delivery. Handlers must be idempotent. Use event_id, upsert, or a processed-events table to handle duplicates safely.

## 14. Required User-Facing Disclosure

Must be maintained in README / docs / docstrings:

> azure-functions-db is not a native database trigger.
> It is a pseudo trigger framework built on Azure Functions timers.
> The default guarantee is close to at-least-once, and handlers must be idempotent.
