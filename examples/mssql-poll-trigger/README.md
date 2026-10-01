# SQL Server polling-trigger example

End-to-end runnable example for `azure-functions-db`'s poll-based pseudo
trigger against **SQL Server**, with checkpoints stored in
[Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
(the local Azure Storage emulator).

The example polls an `orders` table on a one-minute timer, treats each row
change as an event, and writes an idempotent projection into a
`processed_orders` table on every tick.

> **SQL Server has no supported upsert path** in this package (upsert is
> available for PostgreSQL, SQLite, and MySQL only). This example therefore
> uses `@db.inject_writer` + a plain `insert`, swallowing the primary-key
> violation that a replay produces — the documented
> [*"when upsert is not available"*](../postgresql-poll-trigger/README.md#when-upsert-is-not-available)
> fallback. See `function_app.py`.

> For production **AAD / managed-identity** authentication instead of the SQL
> username/password used here, see the companion example
> [`examples/managed-identity-mssql/`](../managed-identity-mssql/).

---

## What you get

| File | Purpose |
|---|---|
| `docker-compose.yml` | SQL Server 2022 + Azurite for the checkpoint store |
| `schema.sql` | `dbo.orders` source table (with a monotonic `updated_at` cursor and trigger), plus `dbo.processed_orders` projection |
| `function_app.py` | A timer-driven `@db.trigger` polling `orders`, writing into `processed_orders` via `@db.inject_writer` |
| `host.json` | Functions host config |
| `local.settings.json.example` | All required environment variables |
| `requirements.txt` | Function App dependencies |

---

## Prerequisites

- Docker + Docker Compose
- Python 3.10+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
  (`func` CLI)
- **Microsoft ODBC Driver 18 for SQL Server** on the host running the Function
  App (the `pyodbc` wheel does not bundle it):
  - Linux: <https://learn.microsoft.com/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server>
  - Windows: install the "ODBC Driver 18 for SQL Server" MSI.

### ARM / Apple Silicon note

`mcr.microsoft.com/mssql/server:2022-latest` is an **x86_64** image. On ARM
hosts either enable Docker Desktop's Rosetta emulation
(Settings → General → *"Use Rosetta for x86/amd64 emulation"*; the compose file
already pins `platform: linux/amd64`), or substitute an ARM-friendly,
mostly-T-SQL-compatible image such as
`mcr.microsoft.com/azure-sql-edge:latest`.

### Licensing

The container runs the free **Developer** edition (`MSSQL_PID=Developer`), which
is licensed for development and testing only — not production.

---

## Connection string format

`SOURCE_DB_URL` / `DEST_DB_URL` use the SQLAlchemy `mssql+pyodbc` dialect with
the ODBC driver selected via a query parameter:

```
mssql+pyodbc://sa:Str0ng_Passw0rd1@localhost:1433/app?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=yes
```

- `driver=ODBC+Driver+18+for+SQL+Server` — must match the installed driver name.
- `Encrypt=yes` — Driver 18 encrypts by default.
- `TrustServerCertificate=yes` — accepts the container's self-signed cert (local
  dev only; drop it against a real server with a trusted certificate).
- URL-encode any special characters in the password (`@`, `:`, `/`, `?`).

---

## End-to-end run

### 1. Start SQL Server and Azurite

```bash
cd examples/mssql-poll-trigger
docker compose up -d
```

Wait until both containers report healthy:

```bash
docker compose ps
```

### 2. Create the database and schema

The container starts with only the system databases, so create `app` first:

```bash
docker exec -i afdb-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "Str0ng_Passw0rd1" -C \
  -Q "IF DB_ID('app') IS NULL CREATE DATABASE app;"

docker exec -i afdb-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "Str0ng_Passw0rd1" -C -d app < schema.sql
```

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

Make sure ODBC Driver 18 is installed (see Prerequisites).

### 5. Run the Function App

```bash
func start
```

`orders_poll` registers as a Timer trigger firing every minute.

### 6. Insert / update rows in `orders`

In another terminal:

```bash
docker exec -i afdb-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "Str0ng_Passw0rd1" -C -d app -Q "
    INSERT INTO dbo.orders (customer_name, amount, status)
    VALUES (N'Alice', 99.99, N'pending'), (N'Bob', 49.50, N'pending');
"
# Wait for the next tick, then update one row to produce another event.
docker exec -i afdb-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "Str0ng_Passw0rd1" -C -d app -Q "
    UPDATE dbo.orders SET status = N'shipped', amount = 109.99
     WHERE customer_name = N'Alice';
"
```

### 7. Observe events

Verify the projection table:

```bash
docker exec -i afdb-mssql /opt/mssql-tools18/bin/sqlcmd \
  -S localhost -U sa -P "Str0ng_Passw0rd1" -C -d app -Q "
    SELECT order_id, source_cursor, customer_name, amount, status
      FROM dbo.processed_orders ORDER BY order_id, source_cursor;"
```

You should see one row per delivered event — the initial insert produces one
row, and the subsequent `UPDATE` produces another row with the same `order_id`
but a later `source_cursor`.

### 8. Tear down

```bash
docker compose down -v   # -v also removes the SQL Server + Azurite volumes
```

---

## Cursor column choice

The example uses `updated_at DATETIME2(3) NOT NULL` as the cursor column,
maintained by an `AFTER INSERT, UPDATE` trigger (`orders_set_updated_at` in
`schema.sql`). SQL Server runs with `RECURSIVE_TRIGGERS` **OFF** by default, so
the trigger's own `UPDATE` of the same table does not re-fire it. This satisfies
the framework's source preconditions:

- **Monotonically non-decreasing** — every insert and update sets `updated_at`
  to `SYSUTCDATETIME()`.
- **Stable PK / total ordering** — `(updated_at, id)` is ordered via
  `pk_columns=["id"]` as the tiebreaker.

> If your real schema mutates rows without touching a cursor column you will
> silently miss updates. Always pick a column bumped on **every** mutation you
> care about. See [Semantics §4 — Delete Semantics](../../docs/03-semantics.md#4-delete-semantics).

## Idempotent handler pattern (no upsert)

Because SQL Server upsert is unsupported here, `function_app.py` uses
`@db.inject_writer` and `writer.insert(...)` per event into a
`processed_orders` table whose primary key is `(order_id, source_cursor)`. The
polling trigger is at-least-once, so the same `RowChange` may be redelivered
(see [§4 Duplicate Window Reference](../../docs/24-polling-runtime-semantics.md#4-duplicate-window-reference)).
A replay re-inserts the same composite key, raising a `WriteError` whose cause
is a SQLAlchemy `IntegrityError`; the handler swallows exactly that case as an
idempotent no-op and re-raises any other failure.

### When latest-state projection is what you want

If you only need the current state per `order_id` (last write wins), key the
destination table on `order_id` alone and use `writer.update(...)` /
`writer.insert(...)` accordingly — but be aware an out-of-order replay of an
older event can overwrite a newer projection.

## Checkpoint container configuration

`function_app.py` builds a `BlobCheckpointStore` against the container
`db-state` in the storage account named by `AzureWebJobsStorage`. Azurite
creates the container on first use. In production, pre-create it with the
minimal RBAC needed (Storage Blob Data Contributor on that container only) — see
[Checkpoint / Lease Spec §12](../../docs/06-checkpoint-lease-spec.md#12-operational-guidelines).

## Tuning notes

The example uses the package defaults — `batch_size=100`,
`max_batches_per_tick=1`, `lease_ttl_seconds=120`, timer schedule
`0 */1 * * * *` (every minute). For production sizing rules see
[Polling Runtime §7](../../docs/24-polling-runtime-semantics.md#7-tuning-lease_ttl_seconds-and-timer-interval).
