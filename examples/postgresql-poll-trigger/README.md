# PostgreSQL polling-trigger example

End-to-end runnable example for `azure-functions-db`'s poll-based pseudo
trigger against PostgreSQL, with checkpoints stored in
[Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
(the local Azure Storage emulator).

The example polls an `orders` table on a one-minute timer, treats each row
change as an event, and writes an idempotent projection into a
`processed_orders` table on every tick.

> Delivery is **at-least-once**. Handlers in this example are intentionally
> idempotent — see the inline comments in `function_app.py` and the
> [Polling Runtime & Failure Scenarios](../../docs/24-polling-runtime-semantics.md)
> page for the full duplicate-window reference.

---

## What you get

| File | Purpose |
|---|---|
| `docker-compose.yml` | PostgreSQL 16 + Azurite for the checkpoint store |
| `schema.sql` | `orders` source table (with a monotonic `updated_at` cursor and trigger), plus `processed_orders` projection |
| `function_app.py` | A timer-driven `@db.trigger` polling `orders`, writing into `processed_orders` via `@db.output` |
| `host.json` | Functions host config |
| `local.settings.json.example` | All required environment variables |
| `requirements.txt` | Function App dependencies |

---

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
  (`func` CLI)
- `psql` (or any PostgreSQL client) for seeding rows

---

## End-to-end run

### 1. Start PostgreSQL and Azurite

```bash
cd examples/postgresql-poll-trigger
docker compose up -d
```

This brings up:

- `postgres` on `localhost:5432` (user `app`, password `app`, db `app`)
- `azurite` on `localhost:10000` (Blob), `10001` (Queue), `10002` (Table)

Wait until both containers report healthy:

```bash
docker compose ps
```

### 2. Initialise the schema

```bash
psql "postgresql://app:app@localhost:5432/app" -f schema.sql
```

You should see `CREATE TABLE`, `CREATE FUNCTION`, `CREATE TRIGGER` etc.

### 3. Configure local settings

```bash
cp local.settings.json.example local.settings.json
```

The defaults already point at the docker-compose services and Azurite — no
edits are needed for the happy-path local run.

### 4. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 5. Run the Function App

```bash
func start
```

You should see `orders_poll` registered as a Timer trigger firing every
minute.

### 6. Insert / update rows in `orders`

In another terminal:

```bash
psql "postgresql://app:app@localhost:5432/app" <<'SQL'
INSERT INTO orders (id, customer_name, amount, status)
VALUES (1, 'Alice', 99.99, 'pending'),
       (2, 'Bob',   49.50, 'pending');

-- Wait for the next tick, then update one row to see another event:
UPDATE orders SET status = 'shipped', amount = 109.99 WHERE id = 1;
SQL
```

### 7. Observe events

In the `func start` log you should see structured log entries like:

```text
Poller 'orders' batch <id>: processed 2 events
```

Verify the projection table:

```bash
psql "postgresql://app:app@localhost:5432/app" -c \
  "SELECT order_id, customer_name, amount, processed_at FROM processed_orders ORDER BY order_id;"
```

Verify the checkpoint blob:

```bash
docker exec -it $(docker compose ps -q azurite) sh -c \
  "ls -la /data/__blobstorage__ 2>/dev/null || true"
```

(or use Azure Storage Explorer pointed at `UseDevelopmentStorage=true`).

### 8. Tear down

```bash
docker compose down -v   # -v also removes the postgres + azurite volumes
```

---

## Cursor column choice

The example uses `updated_at TIMESTAMPTZ NOT NULL` as the cursor column,
maintained by a row-level `BEFORE INSERT OR UPDATE` trigger
(`set_updated_at()` in `schema.sql`). This satisfies the framework's source
preconditions:

- **Monotonically non-decreasing** — every insert and update bumps
  `updated_at` to `now()`.
- **Stable PK / total ordering** — `(updated_at, id)` is unique enough for
  ordered batching (the framework appends `id` as the tiebreaker via
  `pk_columns=["id"]`).
- **Deterministic** — the source query is a plain
  `SELECT ... ORDER BY updated_at, id` with a `(updated_at, id)` cursor
  filter; no application-level non-determinism.

> If your real schema uses `created_at` only and rows are mutated in place,
> you will silently miss updates. Always pick a column that is updated on
> **every** mutation you care about, or use a soft-delete / outbox pattern.
> See [Semantics §4 — Delete Semantics](../../docs/03-semantics.md#4-delete-semantics).

## Idempotent handler pattern

`function_app.py` writes to `processed_orders` with
`action="upsert"` and `conflict_columns=["order_id"]`. Because the
polling trigger is at-least-once, the same `RowChange` may be redelivered
during commit failures, lease transitions, or process crashes
(see [§4 Duplicate Window Reference](../../docs/24-polling-runtime-semantics.md#4-duplicate-window-reference)).
The upsert collides on `order_id` and the replay becomes a no-op write of
identical data.

If your sink does not natively support upsert, swap the `@db.output` for an
`inject_writer`-based handler that maintains a `processed_events` table
keyed by `(event.pk, event.cursor)`.

## Checkpoint container configuration

`function_app.py` builds a `BlobCheckpointStore` against the container
`db-state` in the storage account named by `AzureWebJobsStorage`. The
container is created on first use by Azurite. In production, create it
explicitly with the minimal RBAC needed (Storage Blob Data Contributor on
that container only) — see
[Checkpoint / Lease Spec §12](../../docs/06-checkpoint-lease-spec.md#12-operational-guidelines).

## Tuning notes

The example uses the package defaults — `batch_size=100`,
`max_batches_per_tick=1`, `lease_ttl_seconds=120`, timer schedule
`0 */1 * * * *` (every minute). For production sizing rules and the
`lease_ttl_seconds` vs handler-duration relationship see
[Polling Runtime §7](../../docs/24-polling-runtime-semantics.md#7-tuning-lease_ttl_seconds-and-timer-interval).

For PostgreSQL pool settings (`pool_pre_ping`, `pool_recycle`,
`max_overflow`) see
[EngineProvider & Pooling §5.1](../../docs/25-engine-provider-pooling.md#51-postgresql-psycopg).
