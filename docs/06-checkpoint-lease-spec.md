# Checkpoint / Lease Spec

## 1. Purpose

In a pseudo trigger, the most critical state consists of checkpoint and lease.

- checkpoint: how far processing has successfully progressed
- lease: who currently holds the processing authority

**Core design decision**: checkpoint and lease are stored in a **single state blob**,
and all state changes are performed via **ETag-based CAS (conditional write)**.
This eliminates TOCTOU race conditions between separate blobs entirely.

## 2. Storage Selection

### MVP
- Azure Blob Storage

Rationale:
- Easy to use alongside Azure Functions
- Already available in most operational environments
- Good cost/simplicity balance
- Supports ETag-based conditional writes (If-Match)

### Future Candidates
- Azure Table Storage
- Cosmos DB
- SQL table

## 3. Blob Layout

Container: `db-state`

Blob path example:
```text
state/{app_name}/{poller_name}.json
```

Instead of the legacy `checkpoints/` + `leases/` split, a **single state blob** is used.

## 4. State Document Format (Unified)

```json
{
  "version": 1,
  "poller_name": "orders",
  "source_fingerprint": "sha256:...",

  "checkpoint": {
    "cursor": {
      "kind": "timestamp+pk",
      "value": "2026-04-07T01:23:45.123456Z",
      "tiebreaker": {
        "id": 12093
      }
    },
    "last_successful_batch_id": "batch_20260407_012346_0001",
    "updated_at": "2026-04-07T01:23:46.020000Z",
    "metadata": {
      "row_count": 100
    }
  },

  "lease": {
    "owner_id": "funcapp/instance-abc123",
    "fencing_token": 42,
    "acquired_at": "2026-04-07T01:23:00Z",
    "heartbeat_at": "2026-04-07T01:23:20Z",
    "expires_at": "2026-04-07T01:25:00Z"
  }
}
```

## 5. CAS-Based State Transitions

All state changes follow this pattern:

```text
1. Read state blob → obtain (content, etag)
2. Apply changes (acquire lease, advance checkpoint, etc.)
3. Conditional write (If-Match: etag)
4. Success → complete
5. Failure (412 Precondition Failed) → retry or abort
```

This pattern applies equally to lease acquisition, heartbeat, and checkpoint commit.

## 6. Lease Acquisition Algorithm

```text
1. Read state blob → (state, etag)
2. If state doesn't exist → create new state (fencing_token=1)
3. If lease is expired → increment fencing_token and set owner
4. Conditional write (If-Match: etag)
5. Success → lease acquisition confirmed
6. Failure → another instance acquired first, skip
```

### Rules
- Stealing a lease before expiry from another instance is forbidden
- Use a safety margin to account for local clock skew
- All lease changes are performed atomically via CAS

## 7. Heartbeat

A heartbeat is required while a tick remains active across batches and retries.

Default rules:
- `PollRunner` renews before each batch, before and after retry backoff, and
  immediately before checkpoint commit
- The synchronous runner does not renew concurrently with a handler or sleep;
  the TTL must exceed the longest single handler attempt or retry delay
- Heartbeat = CAS write to state blob (updating heartbeat_at and expires_at)
- Any renewal failure aborts the tick; CAS failure means the lease was lost and
  the current batch must not be committed

## 8. Commit Algorithm

```text
1. Read state blob → (state, etag)
2. Verify owner_id / fencing_token match
3. Update checkpoint (cursor, batch_id, updated_at)
4. Conditional write (If-Match: etag)
5. Success → checkpoint advance complete
6. Failure → CommitError (batch is unconfirmed; can be reprocessed)
```

### Key Principles
- Checkpoint and lease validation occur **in the same CAS write**
- A stale owner's commit is automatically rejected by etag mismatch
- No separate lease blob check required → no TOCTOU race

## 9. Source Fingerprint

The state records a hash of the source definition.

Includes:
- DB URL (excluding password)
- table/query
- cursor column
- PK columns
- filters

If the source fingerprint changes, the default policy is:
- Reject execution, or
- Require an explicit reset/backfill

## 10. Reset Policy

Supported commands:
- `reset_to_beginning`
- `reset_to_checkpoint(file)`
- `reset_to_cursor(value, pk)`
- `clone_checkpoint(new_poller_name)`

Resets in production are dangerous; guardrails are required in the CLI.

## 11. Failure Scenarios

### CAS Write Succeeds, Then Function Exits
- Already committed
- Next tick starts from the new checkpoint

### Handler Succeeds, CAS Write Fails
- Same batch can be reprocessed on the next tick
- Duplicates may occur

### CAS Write Response Timeout (Ambiguous State)
- Commit success is uncertain
- Resolved by reloading state on the next tick
- Worst case: duplicate, no loss

### Lease Expires and Another Instance Takes Over
- Stale owner's commit attempt is automatically rejected by etag mismatch
- Duplicates possible, loss prevented

### Heartbeat CAS Failure
- Treated as lease loss
- Handler stops; no commit attempt is made

## 12. Operational Guidelines

- One state blob per production poller
- Use a separate poller_name for backfill (separate state blob)
- Do not reuse state blobs after source definition changes
- Minimize storage RBAC or connection string permissions
