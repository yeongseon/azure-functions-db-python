# Source fingerprint migration after a host change

## What changes identity

`SqlAlchemySource._compute_fingerprint()` hashes a sorted JSON configuration:
the resolved SQLAlchemy URL **with its password removed**, schema, table/query,
cursor column, ordered primary-key columns, filter, parameters, strategy,
operation mode, and cursor type names. URL host, port, username, database,
driver and query options remain identity inputs. Pool options are not inputs.
Changing just the URL password does not change the fingerprint; changing the
username can. Secrets in URL query options or query parameters are **not**
removed by this password-only transformation: never print the URL/configuration
or diagnostic state into shared logs. The fingerprint is a hash, not proof that
two databases contain the same data.

The checkpoint store rejects a stored, non-null fingerprint different from the
configured fingerprint, including during lease acquisition. This prevents a
new host/dataset from silently inheriting an old cursor. Do not delete/null the
fingerprint, hardcode the old fingerprint into the new store, or remove host
from the hash to bypass this protection.

## Verified failover: retain progress

This is an explicit maintenance procedure, not automatic failover. No CLI or
runtime override is added. Use an authorized Blob client or storage tool with
conditional writes; retain credentials outside commands/logs.

1. Stop **all** poller instances on both hosts, including deployment slots and
   local workers. Drain handlers and confirm leases are voluntarily released.
   If an owner crashed, establish that it cannot restart, wait beyond the stored
   expiry plus grace (`min(TTL * 0.5, 5)` seconds), and account for clock skew.
   Never migrate state while a handler or owner can still run.
2. Independently verify the destination is the same logical dataset: database
   identity/replication lineage, tables and filters, cursor types/ordering,
   stable keys, and replication completeness through the saved cursor. Check
   both old and new databases for missing writes and late commits. A matching
   table name, row count, host alias or fingerprint is insufficient evidence.
   If identity or completeness is uncertain, **do not reuse the cursor**.
3. In the configured container, download `state/<poller_name>.json`, recording
   the download's ETag and preserving the exact bytes in a restricted, durable
   backup. Record the old/new fingerprint hashes and the independent identity
   verification privately. Do not expose row/checkpoint contents or credentials.
4. Instantiate the new source configuration without fetching data and obtain
   `source.source_descriptor.fingerprint`. In a copy of the JSON, change **only**
   `source_fingerprint` to this new value. Preserve version, poller name,
   checkpoint, lease and fencing token. Do not reset fencing history.
5. Upload that JSON to the same blob with `overwrite=True`, the downloaded
   `etag`, and `match_condition=MatchConditions.IfNotModified`. On HTTP 412,
   abort and investigate the unexpected writer; never retry unconditionally.
6. Start one new-host poller, verify resume from the saved complete cursor and
   successful handler/checkpoint commits, then restore the intended scale.
   Retain idempotency: acknowledged external effects can still be replayed.

`tests/test_fingerprint_migration.py` exercises the exact JSON/ETag procedure
with the checkpoint-store SDK fake and verifies resume and rollback rejection.
This validates state mechanics, **not** actual database replication or Azure
failover. Operators must supply that identity evidence themselves.

## Reset instead of inheriting an unrelated cursor

If the destination is a different dataset, or lineage/completeness cannot be
proven, use a **new poller name** (or a separate checkpoint container) and its
new fingerprint. A missing state starts with an empty checkpoint and reads
eligible rows from the beginning, subject to filters/NULL exclusion. Expect
replay, capacity cost and duplicate downstream effects; use idempotent sinks.
Keep the old state and deployment stopped for audit/rollback. Do not delete
the old state merely to make the mismatch disappear or advance a cursor past
unprocessed rows. A separately approved backfill is needed if full replay is
not acceptable.

## Rollback

Before any new-host processing, stop every instance again and conditionally
restore the exact backup bytes using the **current** ETag, then restore the old
source configuration. This restores its old fingerprint/checkpoint and rejects
the new one. A 412 is an abort, not permission to overwrite a newer state.

After new-host processing, do not blindly restore an older checkpoint: external
effects and source divergence may have occurred. Reconcile writes/replication
and choose a verified resume point or an idempotent replay under an explicit
operator recovery decision. Keep both sides stopped until that decision is
complete. Never run old/new configurations concurrently against migrated state.
