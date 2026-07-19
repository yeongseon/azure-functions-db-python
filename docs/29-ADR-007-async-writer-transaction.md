# ADR-007: Async Writer Transaction via Dedicated Worker Thread

## Status
**Accepted** (2026-07-19)

## Context

`azure-functions-db` runs every database operation on a synchronous SQLAlchemy
engine (see [ADR-002](17-ADR-002-sqlalchemy-centric-adapter.md)). When an
`async def` handler receives a writer via `@db.inject_writer`, the package hands
it an `_AsyncDbWriterProxy` that offloads each blocking call to a worker thread
with `asyncio.to_thread`, so the event loop is not blocked.

Historically that proxy deliberately did **not** expose
`DbWriter.transaction()`. A SQLAlchemy `Connection` / `Transaction` is not safe
to use from more than one thread, and `asyncio.to_thread` draws an arbitrary
thread from the default executor pool for each call. A naive per-call async
transaction would therefore spread a single transaction's statements across
different OS threads and silently break atomicity. The documented workaround was
to wrap the whole unit in one `asyncio.to_thread` call driving a synchronous
`DbWriter` end-to-end (tracked by #116 / #128).

We want first-class multi-statement atomicity from async handlers without either
(a) breaking thread-affinity or (b) pulling in native asyncio drivers.

## Decision

Expose `transaction()` on the async writer proxy as an **`async` context
manager** that pins the entire transaction to a **single dedicated worker
thread** for the lifetime of the `async with` block.

- On enter, create a `concurrent.futures.ThreadPoolExecutor(max_workers=1)` and
  run the synchronous `DbWriter.transaction().__enter__()` on it.
- Yield a transactional proxy whose `insert` / `insert_many` / `upsert` /
  `upsert_many` / `update` / `delete` each route through that same single-worker
  executor, guaranteeing the connection is only ever touched by one thread.
- On exit, run the context manager's `__exit__` on the same executor (commit on
  success, rollback on error), then shut the executor down. The cleanup is
  shielded from cancellation so commit/rollback still runs if the awaiting task
  is cancelled; if the shield itself is cancelled, the cleanup is forced to
  complete synchronously on the still-alive worker thread.
- The executor is scoped to the `async with` block — two concurrent
  transactions get independent executors, threads, and connections.

Concurrent writes issued inside the block (for example via `asyncio.gather`) are
**serialized** onto the pinned thread. This is the correct guarantee: a
SQLAlchemy connection cannot be used concurrently even on a single thread.

## Alternatives considered

- **Native `AsyncEngine` (asyncpg / aiomysql / aiosqlite).** Rejected for this
  scope. It would add async driver dependencies and a second engine code path,
  diverging behavior across dialects — directly against
  [ADR-002](17-ADR-002-sqlalchemy-centric-adapter.md). Revisit only if native
  async throughput or streaming becomes a requirement.
- **Reject and keep documenting the `asyncio.to_thread` workaround.** Rejected:
  the workaround is correct but awkward and easy to get wrong (users must
  remember to keep every statement inside one offloaded call).
- **Actively reject concurrent writes inside a transaction.** Rejected:
  serialization via the single-worker executor already yields correct,
  predictable behavior; rejecting adds surface area for no benefit.

## Consequences

- Async handlers can now write atomically with `async with writer.transaction()`.
- A commit failure surfaces as `WriteError`; a rollback failure is logged while
  the original exception is preserved.
- Each transaction spins up (and tears down) one short-lived OS thread. For the
  expected transaction cadence in Functions handlers this overhead is
  negligible; it is the price of guaranteed thread-affinity without async
  drivers.
- The synchronous `DbWriter` remains the single source of truth for transaction
  semantics; the async path is a thin thread-pinning wrapper over it.

## References

- #128 — feat(async): native async writer transaction context manager
- #116 — the limitation doc that prompted this follow-up
- [ADR-002 — SQLAlchemy-centric Adapter](17-ADR-002-sqlalchemy-centric-adapter.md)
- `src/azure_functions_db/decorator.py` — `_AsyncDbWriterProxy`, `_AsyncTxWriterProxy`
