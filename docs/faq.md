# FAQ

## General

### What databases are supported?

PostgreSQL, MySQL, and SQL Server via SQLAlchemy dialect drivers. Install the
corresponding extra: `azure-functions-db[postgres]`, `azure-functions-db[mysql]`,
or `azure-functions-db[mssql]`.

### Is this an official Microsoft package?

No. This is an independent community project. It is not affiliated with,
endorsed by, or maintained by Microsoft.

### What Azure Functions programming model is required?

Python v2 only — the decorator-based `func.FunctionApp()` model. The legacy
`function.json`-based v1 model is not supported.

### What Python versions are supported?

Python 3.11, 3.12, 3.13, and 3.14. The project metadata declares `>=3.11,<3.15`.

## Trigger

### Is this a real database trigger?

No. It is a pseudo trigger — poll-based change detection built on top of a
timer trigger. The package polls the database at a configurable interval and
delivers changed rows to your handler.

### What delivery guarantee does the trigger provide?

At-least-once. Handlers must be idempotent. Duplicates may occur during
crashes, lease transitions, or commit failures.

### Can I use CDC instead of polling?

Not currently. The package uses cursor-based polling via a monotonically
increasing column (e.g. `updated_at`). Native CDC support is not planned.

### What happens if my handler fails?

The checkpoint does not advance. The same batch will be redelivered on the
next polling cycle.

### Are hard deletes detected?

No. Cursor-based polling only detects inserts and updates. Use soft deletes
(a `deleted_at` column) if you need to track deletions.

## Bindings

### When should I use `input` vs `inject_reader`?

Use `input` for simple reads — single row lookup by primary key or a single SQL
query. Use `inject_reader` when you need multiple queries, conditional logic,
or full `DbReader` control within the handler.

### When should I use `output` vs `inject_writer`?

Use `output` for simple writes — insert or upsert via `DbOut.set()`. Use
`inject_writer` when you need transactions, update/delete operations,
conditional writes, or per-row logic.

### Can I use input and output on the same handler?

Yes. Decorator composition is supported. Place them in the correct order
(Azure Functions decorators first, then database decorators closest to the
function definition).

### Can I use output and inject_writer on the same handler?

No. They are mutually exclusive. Choose one pattern per handler.

## Configuration

### How does environment variable substitution work?

Wrap variable names in percent signs: `url="%DB_URL%"`. The value is resolved
from the environment at runtime. Partial substitution is also supported:
`url="postgresql+psycopg://%DB_USER%:%DB_PASS%@%DB_HOST%/mydb"`.

### Can I pass custom SQLAlchemy engine options?

Yes. Use the `engine_kwargs` parameter on any decorator to pass additional
keyword arguments to `sqlalchemy.create_engine()`.

### Can I share a connection pool across decorators?

Yes. Create an `EngineProvider` instance and pass it to multiple decorators or
to `SqlAlchemySource`. Engines are created lazily and reused by URL.
