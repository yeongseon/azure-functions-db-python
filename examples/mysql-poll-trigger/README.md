# MySQL polling-trigger example

End-to-end runnable example for `azure-functions-db`'s poll-based pseudo
trigger against MySQL 8.0, with checkpoints stored in
[Azurite](https://learn.microsoft.com/azure/storage/common/storage-use-azurite)
(the local Azure Storage emulator).

The example polls an `orders` table on a one-minute timer, treats each row
change as an event, and writes an idempotent projection into a
`processed_orders` table on every tick.

> Delivery is **at-least-once**. Handlers in this example are intentionally
> idempotent — see the inline comments in `function_app.py` and the
> [Polling Runtime & Failure Scenarios](../../docs/24-polling-runtime-semantics.md)
> page for the full duplicate-window reference.

> Companion to the [`postgresql-poll-trigger`](../postgresql-poll-trigger/)
> example. The two share the same structure, so if you already know that
> example the only differences here are MySQL-specific — see
> [MySQL-specific gotchas](#mysql-specific-gotchas) below.

---

## What you get

| File | Purpose |
|---|---|
| `docker-compose.yml` | MySQL 8.0 + Azurite for the checkpoint store |
| `schema.sql` | `orders` source table (with a monotonic `updated_at` cursor maintained by `ON UPDATE CURRENT_TIMESTAMP(6)`), plus `processed_orders` projection |
| `function_app.py` | A timer-driven `@db.trigger` polling `orders`, writing into `processed_orders` via `@db.output` |
| `host.json` | Functions host config |
| `local.settings.json.example` | All required environment variables |
| `requirements.txt` | Function App dependencies |
| `smoke.sh` | Self-contained end-to-end smoke test (no `func` CLI required) |

---

## Prerequisites

- Docker + Docker Compose v2
- Python 3.10+
- [Azure Functions Core Tools v4](https://learn.microsoft.com/azure/azure-functions/functions-run-local)
  (`func` CLI)
- `mysql` client for seeding rows (`apt-get install mysql-client` or the
  `mysql-client` package for your distro)

---

## End-to-end run

> **Verify your environment first**: run [`./smoke.sh`](smoke.sh) to bring
> up MySQL + Azurite, apply the schema, drive a single poll tick from
> Python, and assert the projection table received the expected rows. The
> script is self-contained (no `func` CLI required) and exits non-zero on
> any failure. Use it as a smoke test in CI or as a sanity check before
> running the full Functions host below.

### 1. Start MySQL and Azurite

```bash
cd examples/mysql-poll-trigger
docker compose up -d
```

This brings up:

- `mysql` on `localhost:3306` (user `app`, password `app`, database `app`, root password `root`)
- `azurite` on `localhost:10000` (Blob), `10001` (Queue), `10002` (Table)

Wait until both containers report healthy (MySQL takes ~15–30 s on first
boot while it initialises the data directory and creates the `app`
user):

```bash
docker compose ps
```

### 2. Initialise the schema

```bash
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app < schema.sql
```

You should see no output on success (`mysql` is quiet on success by
default). Verify:

```bash
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app -e "SHOW TABLES;"
```

should list `orders` and `processed_orders`.

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
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app <<'SQL'
-- Let AUTO_INCREMENT allocate the ids; inserting explicit values does
-- not advance the counter and would collide with later auto-id inserts.
INSERT INTO orders (customer_name, amount, status)
VALUES ('Alice', 99.99, 'pending'),
       ('Bob',   49.50, 'pending');

-- Wait for the next tick, then update one row to see another event.
-- Identifying by customer_name keeps this snippet runnable without
-- knowing the assigned ids.
UPDATE orders SET status = 'shipped', amount = 109.99
 WHERE customer_name = 'Alice';
SQL
```

### 7. Observe events

In the `func start` log you should see structured log entries like:

```text
Poller 'orders' batch <id>: processed 2 events
```

Verify the projection table:

```bash
MYSQL_PWD=app mysql -h 127.0.0.1 -P 3306 -u app app -e \
  "SELECT order_id, source_cursor, customer_name, amount, status \
     FROM processed_orders ORDER BY order_id, source_cursor;"
```

You should see one row per delivered event — the initial insert produces
one row, and the subsequent `UPDATE` produces another row with the same
`order_id` but a later `source_cursor`. This is the strictly-idempotent
projection pattern: replays of the same `RowChange` collide on
`(order_id, source_cursor)` and are no-op upserts.

Verify the checkpoint blob:

```bash
docker exec -it $(docker compose ps -q azurite) sh -c \
  "ls -la /data/__blobstorage__ 2>/dev/null || true"
```

(or use Azure Storage Explorer pointed at `UseDevelopmentStorage=true`).

### 8. Tear down

```bash
docker compose down -v   # -v also removes the mysql + azurite volumes
```

---

## Cursor column choice

The example uses `updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)`
as the cursor column. This satisfies the framework's source
preconditions:

- **Monotonically non-decreasing** — the server bumps `updated_at` to
  `CURRENT_TIMESTAMP(6)` on every INSERT and every UPDATE via the
  `ON UPDATE` column attribute. **No application-level trigger is
  required** — this is one of the small ergonomic wins MySQL has over
  the PostgreSQL example.
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

`function_app.py` writes to `processed_orders` with `action="upsert"`
and `conflict_columns=["order_id", "source_cursor"]`. On MySQL the
framework rewrites this to
`INSERT INTO processed_orders (...) VALUES (...) ON DUPLICATE KEY UPDATE col = VALUES(col)`
under the hood (see
[`_build_mysql_upsert`](https://github.com/yeongseon/azure-functions-db-python/blob/main/src/azure_functions_db/binding/writer.py)).
Because the polling trigger is at-least-once, the same `RowChange` may be
redelivered during commit failures, lease transitions, or process crashes
(see [§4 Duplicate Window Reference](../../docs/24-polling-runtime-semantics.md#4-duplicate-window-reference)).
The composite key `(event.pk, event.cursor)` ensures the replay collides
on the exact same row with byte-identical column values, so the second
write is a true no-op.

### When latest-state projection is what you want

If you only need the *current* state per `order_id` (last write wins),
key on `order_id` alone — e.g. drop `source_cursor` from the primary key
and from `conflict_columns`. That's a simpler, smaller table, but be
aware that:

- An out-of-order replay of an older event can overwrite a newer
  projection.
- "Replay = no-op" only holds when the replay carries the same column
  values as the previous delivery; for true event-level dedup, prefer
  the composite-key pattern above.

### When upsert is not available

If your sink does not natively support upsert, swap the `@db.output`
for an `inject_writer`-based handler that maintains a
`processed_events` table keyed by `(event.pk, event.cursor)` with a
unique constraint, and swallow the unique-violation on replay. On MySQL
you can equivalently write `INSERT IGNORE INTO processed_events ...`,
which silently swallows duplicate-key errors on the primary key —
smoke.sh uses exactly this pattern.

## Checkpoint container configuration

`function_app.py` builds a `BlobCheckpointStore` against the container
`db-state` in the storage account named by `AzureWebJobsStorage`. The
container is created on first use by Azurite. In production, create it
explicitly with the minimal RBAC needed (Storage Blob Data Contributor on
that container only) — see
[Checkpoint / Lease Spec §12](../../docs/06-checkpoint-lease-spec.md#12-operational-guidelines).

## MySQL-specific gotchas

MySQL has several dialect and driver quirks that matter for polling. The
example is set up to avoid all of them; this section explains why so you
know what to preserve in your own deployment.

### 1. Timezone handling — always run the server on UTC

MySQL's `DATETIME` type is **timezone-naive**: it stores exactly what you
write and returns exactly what was stored, with no conversion. `TIMESTAMP`
is stored as UTC but converted on read using the *session* time zone,
which is a footgun for a monotonic cursor — two workers with different
session TZs would see different values and the cursor comparison would
become non-deterministic.

The example avoids this by:

- Using `DATETIME(6)` (not `TIMESTAMP`) for `updated_at`, `source_cursor`,
  `processed_at`.
- Starting the server with `--default-time-zone=+00:00` in
  `docker-compose.yml` so `CURRENT_TIMESTAMP(6)` always returns UTC.
- Setting the container env `TZ=UTC` for completeness.

In production, either mirror those flags in your MySQL configuration
(`my.cnf`: `default-time-zone = +00:00`) or make sure your application
always writes UTC values explicitly.

### 2. `ON UPDATE CURRENT_TIMESTAMP` replaces the PostgreSQL trigger

MySQL's `DATETIME DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)`
does what the PostgreSQL `BEFORE INSERT OR UPDATE` trigger does in the
sibling example. This is a one-liner in the column definition — no
stored function, no trigger. Prefer it.

### 3. Upsert syntax

The framework's `action="upsert"` compiles to
`INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)` on MySQL — see
`_build_mysql_upsert` in the writer implementation. The conflict target
must be a UNIQUE index or PRIMARY KEY on the columns you list in
`conflict_columns`; the example uses the composite PRIMARY KEY
`(order_id, source_cursor)`.

> **Note:** MySQL 8.0.20 deprecates the `VALUES(col)` form in favour of
> row-aliased syntax (`INSERT ... AS new ON DUPLICATE KEY UPDATE col = new.col`).
> The SQLAlchemy MySQL dialect this package uses generates the
> `VALUES(col)` form, which still works and is not going away — but if
> you inspect the raw SQL, that's what you'll see.

### 4. Character set / collation

Use `utf8mb4` / `utf8mb4_0900_ai_ci` (the MySQL 8 default), not the
legacy `utf8` alias which is 3-byte-only and cannot round-trip 4-byte
UTF-8 characters (most emoji, some CJK). All tables in `schema.sql`
declare this explicitly.

### 5. Driver choice — PyMySQL vs mysqlclient

The `[mysql]` extra pulls in [PyMySQL](https://pymysql.readthedocs.io/)
(`pymysql>=1.1`), a pure-Python driver. It is the default because it
installs without a C build step and works out of the box on every Python
version supported by the package (3.10 – 3.14).

If you need native throughput (2×–5× on tight workloads), install
[mysqlclient](https://github.com/PyMySQL/mysqlclient) separately and
switch the URL prefix from `mysql+pymysql://` to `mysql+mysqldb://`.
mysqlclient is a compiled C driver and needs a MySQL client library
(`libmysqlclient-dev` on Debian/Ubuntu, `mysql-devel` on RHEL) and a C
toolchain to install.

PyMySQL 1.1+ supports MySQL 8's default `caching_sha2_password`
authentication plugin, so the example does not force
`mysql_native_password` on the server.

### 6. `TRUNCATE` cannot list multiple tables

Unlike PostgreSQL, MySQL's `TRUNCATE` accepts exactly one table per
statement. Both the schema reset in `smoke.sh` and any manual cleanup
must run two separate statements
(`TRUNCATE TABLE orders; TRUNCATE TABLE processed_orders;`). `TRUNCATE`
also resets `AUTO_INCREMENT` to `1` automatically — no `RESTART
IDENTITY` clause needed.

## Tuning notes

The example uses the package defaults — `batch_size=100`,
`max_batches_per_tick=1`, `lease_ttl_seconds=120`, timer schedule
`0 */1 * * * *` (every minute). For production sizing rules and the
`lease_ttl_seconds` vs handler-duration relationship see
[Polling Runtime §7](../../docs/24-polling-runtime-semantics.md#7-tuning-lease_ttl_seconds-and-timer-interval).

For MySQL pool settings (`pool_pre_ping`, `pool_recycle`, `max_overflow`,
`connect_timeout`), see
[EngineProvider & Pooling §5](../../docs/25-engine-provider-pooling.md).
Two knobs matter more on MySQL than PostgreSQL:

- **`pool_recycle`** — set below MySQL's `wait_timeout` (default 28 800 s
  / 8 h). Azure Database for MySQL Flexible Server defaults to lower
  values; check yours and set `pool_recycle` to about half of it.
- **`connect_args={"connect_timeout": ...}`** — PyMySQL's default
  `connect_timeout` is 10 s. Raise it for cross-region deployments.
