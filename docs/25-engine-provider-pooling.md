# EngineProvider Lifecycle & SQLAlchemy Pooling Guidance

This page documents how `azure-functions-db` creates, caches, and reuses
SQLAlchemy engines, and gives recommended pool settings for Azure Functions
workloads.

If you have ever asked **"is my SQLAlchemy engine being created on every
invocation?"** or **"why am I getting stale connection errors after the
Function App has been idle?"** — start here.

---

## 1. Why this matters in Azure Functions

Azure Functions reuses Python worker processes across **warm** invocations.
SQLAlchemy engines and their connection pools therefore survive multiple
handler executions:

- The first invocation pays the cost of building the engine and opening
  pooled connections.
- Subsequent warm invocations on the same worker reuse the existing engine
  and pool — **no reconnect, no DNS lookup, no TLS handshake**.
- A cold start (new worker, scale-out, idle eviction, redeploy) builds a
  fresh engine.

This is the behaviour `azure-functions-db` relies on. To make sharing
explicit and safe, the package exposes [`EngineProvider`](#3-engineprovider).

---

## 2. Engine lifetime across warm invocations

### Without `EngineProvider`

If you do **not** pass an `engine_provider` to bindings or
`SqlAlchemySource`, each binding creates an independent SQLAlchemy engine
the first time it runs:

```python
@db.input("rows", url="%DB_URL%", query="SELECT * FROM users")
def list_users(rows): ...

@db.output("out", url="%DB_URL%", table="orders")
def write_order(out): ...
```

In the snippet above, the input binding and the output binding **each build
their own engine** the first time they execute. Both engines are then cached
inside their respective bindings for the lifetime of the worker process —
i.e. across all warm invocations — but they are **not** shared with each
other.

This is fine for small apps. For functions that fan out to many bindings on
the same database, you want a single shared engine; that is what
`EngineProvider` is for.

### With `EngineProvider`

```python
from azure_functions_db import EngineProvider

engine_provider = EngineProvider()

@db.input("rows", url="%DB_URL%", query="SELECT * FROM users",
          engine_provider=engine_provider)
def list_users(rows): ...

@db.output("out", url="%DB_URL%", table="orders",
           engine_provider=engine_provider)
def write_order(out): ...
```

Both bindings now resolve to the **same** engine instance. The pool is
shared, idle connections count once, and `engine_kwargs` is applied
consistently across bindings.

### When does the engine die?

| Event | Engine fate |
|---|---|
| Warm invocation | Reused — same instance, same pool |
| Process exit (scale-in, idle eviction, redeploy) | Engine and pool dropped with the process |
| `engine_provider.dispose_all()` called | All cached engines disposed; next access rebuilds |
| Source `engine_kwargs` change at runtime | New cache key → new engine; old engine remains until process exit (see [§3.2](#32-cache-key)) |

`EngineProvider` does **not** register an `atexit` hook. If you need
deterministic disposal during graceful shutdown (e.g. tests, custom
lifecycle), call `engine_provider.dispose_all()` explicitly.

---

## 3. `EngineProvider`

### 3.1 Engine caching

`EngineProvider` is an in-process, thread-safe cache of SQLAlchemy engines.

- Calling `get_engine(config)` returns the cached engine for that config, or
  creates one on the first call.
- Engine construction is serialized under a single `threading.Lock`, so
  concurrent first-call requests will not double-build the engine.
- The cache lives for the lifetime of the `EngineProvider` instance — by
  convention, module-level so it spans the worker process lifetime.

### 3.2 Cache key

Two `DbConfig` values resolve to the **same** cached engine if and only if
all of the following match exactly:

| Field | Notes |
|---|---|
| `connection_url` | After `%ENV_VAR%` resolution. Two configs that resolve to the same URL share an engine. |
| `pool_size` | Default `5`. |
| `pool_recycle` | Default `3600` (seconds). |
| `echo` | Default `False`. |
| `connect_args` | Compared by JSON-normalized contents. |
| `engine_kwargs` | Compared by JSON-normalized contents. |

Any difference in the above produces a **separate** engine. In particular,
adding a single `engine_kwargs` key (e.g. `pool_pre_ping=True`) on one
binding while leaving it off on another **builds two engines for the same
URL**. Keep `engine_kwargs` consistent across bindings that should share a
pool.

> **Tip:** if a binding accidentally creates its own engine when you expected
> sharing, dump the resolved configs side-by-side and look for a mismatched
> `engine_kwargs` key. The cache key is exactly the JSON shown by
> `EngineProvider._cache_key(config)`.

### 3.3 Disposal

`EngineProvider.dispose_all()` clears the cache and calls `engine.dispose()`
on every cached engine. Use it in tests and in custom shutdown paths. You do
**not** need to call it on every invocation — Azure Functions tears the
process down for you.

---

## 4. Recommended pool settings for serverless

The defaults (`pool_size=5`, `pool_recycle=3600`, `pool_pre_ping` unset) are
safe for development. For production on Azure Functions with managed
databases, prefer the following baseline:

```python
from azure_functions_db import DbConfig, EngineProvider

config = DbConfig(
    connection_url="%DB_URL%",
    pool_size=5,                # one engine per worker; total = pool_size * worker_count
    pool_recycle=1800,          # 30 min — below most managed-DB idle timeouts
    connect_args={
        "connect_timeout": 10,  # driver-level TCP / login timeout (seconds)
    },
    engine_kwargs={
        "pool_pre_ping": True,  # detect stale connections before use
        "max_overflow": 10,     # short bursts above pool_size during fan-out
        "pool_timeout": 30,     # wait at most 30s for a free connection
    },
)
```

> `DbConfig` exposes `connect_args` as a dedicated field. Prefer it over
> nesting `connect_args` inside `engine_kwargs`: a `connect_args` key inside
> `engine_kwargs` will silently override the dedicated field
> (see [`core/engine.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/src/azure_functions_db/core/engine.py)
> — `engine_kwargs` is applied after `connect_args`).

### 4.1 `pool_pre_ping=True`

**Recommended for every managed database.** Azure Database for PostgreSQL,
Azure Database for MySQL, Azure SQL, and most cloud-managed databases close
idle connections after a server-side timeout (often 4–30 minutes). Without
`pool_pre_ping`, the next checkout from a recycled-but-idle connection will
raise a connection error on the first query.

`pool_pre_ping=True` issues a cheap `SELECT 1` (or driver equivalent) when
checking out a connection and transparently reconnects if it has been
closed.

The cost is a single round-trip per checkout. On Azure Functions, where
idle gaps are common, this is almost always worth paying.

### 4.2 `pool_recycle`

`pool_recycle` is the **client-side** maximum age (seconds) for a pooled
connection. SQLAlchemy will discard and re-open any connection older than
this on next checkout, regardless of whether the server still considers it
alive.

Set it **below** your database's server-side idle timeout. Common values:

> The values below are **starting points**, not Azure platform guarantees.
> Server-side idle timeouts are configurable on every managed database and
> may differ from the defaults shown here. Always confirm the configured
> timeout on your specific database/server before relying on these numbers.

| Database | Typical server idle timeout | Recommended `pool_recycle` |
|---|---|---|
| Azure Database for PostgreSQL — Flexible Server | configurable, default 5 min | `1800` (30 min if you raised it) or `240` (4 min default) |
| Azure Database for MySQL — Flexible Server | configurable, default 8 hr (`wait_timeout`) | `1800` |
| Azure SQL Database / SQL Server | ~30 min idle disconnect | `1500` |
| PgBouncer (transaction pooling) | controlled by `server_idle_timeout` | match or shorten |

`pool_recycle` complements `pool_pre_ping` — recycle drops stale connections
proactively, pre-ping catches the rest. Use both.

### 4.3 `pool_size` and `max_overflow`

In Azure Functions, **the effective concurrent connection count is roughly
`pool_size × number_of_workers`**. The Functions Python worker pool size and
host instance count are controlled by host settings, not by your code.

- `pool_size` (default `5`) — connections kept open after use. Cheap to
  size moderately.
- `max_overflow` (SQLAlchemy default `10`) — extra connections opened above
  `pool_size` under burst load. These are closed when returned.
- `pool_timeout` (SQLAlchemy default `30`) — how long a request waits for a
  free connection before raising `TimeoutError`.

Sizing rule of thumb:

```text
max_db_connections_consumed_by_app
  ≈ (pool_size + max_overflow) × max_function_app_instances × workers_per_instance
```

Stay well below your database's `max_connections` ceiling. For Azure
Database for PostgreSQL Flexible Server, that ceiling scales with the SKU
(e.g. ~50 on Burstable B1ms, ~1700 on General Purpose D16s_v3). Confirm
your SKU's limit and divide.

For most polling triggers a single worker only needs 1–2 active connections
per binding. Defaults are fine. Increase `max_overflow` only if you see
`QueuePool limit of size N overflow N reached` warnings under burst.

### 4.4 SQLite and local-test behaviour

SQLite defaults differ from managed-DB defaults:

- **In-memory** SQLite (`sqlite:///:memory:`) gives each new connection a
  fresh empty database. SQLAlchemy uses a `StaticPool` so all checkouts
  share a single connection — meaning `pool_size` and `max_overflow` are
  effectively ignored.
- **File-based** SQLite (`sqlite:///path.db`) defaults to `NullPool`
  (every checkout opens a new connection). `pool_size`, `max_overflow`,
  and `pool_recycle` are no-ops there.
- SQLite drivers are not thread-safe by default. SQLAlchemy enforces
  `connect_args={"check_same_thread": False}` if you need multi-thread
  access. The bindings do **not** set this for you — pass it via
  `connect_args` if you genuinely need it.
- `pool_pre_ping=True` is harmless on SQLite and can stay enabled.

For tests, a typical config is:

```python
DbConfig(
    connection_url="sqlite:///:memory:",
    connect_args={"check_same_thread": False},
)
```

---

## 5. Per-dialect `engine_kwargs` snippets

### 5.1 PostgreSQL (psycopg)

```python
DbConfig(
    connection_url="postgresql+psycopg://user:pass@host:5432/db",
    pool_size=5,
    pool_recycle=1800,
    connect_args={
        "connect_timeout": 10,                        # TCP connect timeout (s)
        "options": "-c statement_timeout=30000",      # 30s server-side query timeout
    },
    engine_kwargs={
        "pool_pre_ping": True,
        "max_overflow": 10,
        "pool_timeout": 30,
    },
)
```

### 5.2 MySQL (PyMySQL)

```python
DbConfig(
    connection_url="mysql+pymysql://user:pass@host:3306/db",
    pool_size=5,
    pool_recycle=1800,
    connect_args={
        "connect_timeout": 10,
        "read_timeout": 30,
        "write_timeout": 30,
    },
    engine_kwargs={
        "pool_pre_ping": True,
        "max_overflow": 10,
        "pool_timeout": 30,
    },
)
```

### 5.3 SQL Server / Azure SQL (pyodbc)

```python
DbConfig(
    connection_url=(
        "mssql+pyodbc://user:pass@host:1433/db"
        "?driver=ODBC+Driver+18+for+SQL+Server"
        "&Encrypt=yes&TrustServerCertificate=no"
    ),
    pool_size=5,
    pool_recycle=1500,
    connect_args={
        "timeout": 10,                       # login timeout (s)
    },
    engine_kwargs={
        "pool_pre_ping": True,
        "max_overflow": 10,
        "pool_timeout": 30,
        "fast_executemany": True,            # bulk insert acceleration
    },
)
```

> SQL Server with `fast_executemany=True` requires homogeneous parameter
> types per `executemany` call. The `DbOut` writer batches by the same
> dict shape, so this is safe for our bindings.

---

## 6. Operational checklist

Before promoting a Function App to production:

- [ ] Use a **module-level** `EngineProvider` and pass it to every binding /
      `SqlAlchemySource` that targets the same database.
- [ ] Keep `engine_kwargs` **identical** across bindings that should share a
      pool (otherwise the cache key splits and you build extra engines).
- [ ] Set `pool_pre_ping=True` for managed databases.
- [ ] Set `pool_recycle` below your database's server-side idle timeout.
- [ ] Confirm `(pool_size + max_overflow) × instance_count × workers` stays
      well below the database's `max_connections`.
- [ ] On SQLite, do not rely on `pool_size`; pass
      `connect_args={"check_same_thread": False}` if you access from
      multiple threads.
- [ ] If you orchestrate disposal manually (tests, custom shutdown), call
      `engine_provider.dispose_all()`.

---

## 7. See Also

- [Architecture](02-architecture.md) — where `EngineProvider` sits in the
  component diagram.
- [Polling Runtime & Failure Scenarios](24-polling-runtime-semantics.md) —
  pool tuning interacts with `lease_ttl_seconds` and handler duration.
- [SQLAlchemy 2.0 — Connection Pooling](https://docs.sqlalchemy.org/en/20/core/pooling.html)
- [SQLAlchemy 2.0 — Engine Configuration](https://docs.sqlalchemy.org/en/20/core/engines.html)
