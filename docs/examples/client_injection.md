# Client Injection

For complex operations that go beyond simple reads or writes, use
`inject_reader` and `inject_writer` to get client instances with full
imperative control.

## When to Use

Use client injection instead of `input`/`output` when you need:

- Multiple queries in one handler
- Transactions or conditional logic
- Update or delete operations
- Fine-grained error handling per operation

## inject_reader

Inject a `DbReader` instance for read operations:

```python
from azure_functions_db import DbBindings, DbReader

db = DbBindings()


@db.inject_reader("reader", url="%DB_URL%", table="users")
def complex_read(reader: DbReader) -> None:
    # Single row lookup
    user = reader.get(pk={"id": 42})

    # SQL query with parameters
    orders = reader.query(
        "SELECT * FROM orders WHERE user_id = :uid AND status = :status",
        params={"uid": 42, "status": "pending"},
    )

    for order in orders:
        print(order["id"], order["total"])
```

### DbReader Methods

| Method | Description |
| --- | --- |
| `get(pk={...})` | Fetch a single row by primary key. Returns `dict \| None`. |
| `query(sql, params={...})` | Execute a SQL query. Returns `list[dict]`. |

### Composite primary keys

When a table's primary key spans two columns, `get()` needs **both** columns, and
the pair identifies exactly one row. The recipe below uses a temporary SQLite
table; nothing needs a network or an external database.

```sql
CREATE TABLE order_items (
    order_id INTEGER,
    item_id  INTEGER,
    qty      INTEGER,
    PRIMARY KEY (order_id, item_id)
);
-- rows: (1, 1, 2), (1, 2, 3), (2, 1, 1)
```

```python
from azure_functions_db import DbBindings, DbReader

db = DbBindings()


@db.inject_reader("reader", url="%ORDER_ITEMS_URL%", table="order_items")
def read_line(order_id: int, item_id: int, reader: DbReader) -> dict[str, object] | None:
    # Both columns together identify one row.
    return reader.get(pk={"order_id": order_id, "item_id": item_id})


print(read_line(1, 2))  # {'order_id': 1, 'item_id': 2, 'qty': 3}
print(read_line(9, 9))  # None: no row has this key pair
```

- An exact match returns that row as a `dict`; a key pair that matches nothing
  returns `None` (not an error).
- The order of keys in `pk` does not matter, but every primary-key column must be
  present: `reader.get(pk={"order_id": 1})` raises `ConfigurationError`
  ("Incomplete primary key") instead of returning one of the two matching rows.
- A column that is not part of the primary key is rejected the same way. To look
  rows up by a non-key column, use `query()` with bound parameters.

The snippet above is executed against a temporary SQLite database by
`tests/test_docs_composite_pk_example.py`
(`python -m pytest tests/test_docs_composite_pk_example.py --no-cov -q`).

!!! warning "Raw SQL is not restricted"
    `query()` runs any SQL the connection is allowed to execute, including writes. Bind
    untrusted values with `:name` placeholders and `params`, choose identifiers from an
    allow-list, and connect with a read-only role. See [Raw SQL](../security.md#raw-sql).

## inject_writer

Inject a `DbWriter` instance for write operations:

```python
from azure_functions_db import DbBindings, DbWriter

db = DbBindings()


@db.inject_writer("writer", url="%DB_URL%", table="orders")
def complex_write(writer: DbWriter) -> None:
    # Insert
    writer.insert(data={"id": 1, "status": "pending", "total": 99.99})

    # Update
    writer.update(data={"status": "shipped"}, pk={"id": 1})

    # Upsert
    writer.upsert(
        data={"id": 2, "status": "pending", "total": 49.99},
        conflict_columns=["id"],
    )

    # Delete
    writer.delete(pk={"id": 1})
```

### DbWriter Methods

| Method | Description |
| --- | --- |
| `insert(data={...})` | Insert a single row. |
| `update(data={...}, pk={...})` | Update a row by primary key. |
| `upsert(data={...}, conflict_columns=[...])` | Insert or update on conflict. |
| `delete(pk={...})` | Delete a row by primary key. |

## Combined Reader + Writer

Use both on the same handler for read-then-write workflows:

```python
from azure_functions_db import DbBindings, DbReader, DbWriter

db = DbBindings()


@db.inject_reader("reader", url="%DB_URL%", table="users")
@db.inject_writer("writer", url="%DB_URL%", table="audit_log")
def audit_user_access(reader: DbReader, writer: DbWriter) -> None:
    user = reader.get(pk={"id": 42})
    if user:
        writer.insert(data={
            "user_id": 42,
            "user_name": user["name"],
            "action": "profile_accessed",
        })
```

## With HTTP Trigger

Complete example with Azure Functions HTTP trigger:

```python
import azure.functions as func

from azure_functions_db import DbBindings, DbReader

app = func.FunctionApp()
db = DbBindings()


@app.function_name(name="search_users")
@app.route(route="users/search", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
@db.inject_reader("reader", url="%DB_URL%", table="users")
def search_users(req: func.HttpRequest, reader: DbReader) -> func.HttpResponse:
    name = req.params.get("name", "")
    results = reader.query(
        "SELECT id, name, email FROM users WHERE name ILIKE :pattern",
        params={"pattern": f"%{name}%"},
    )
    return func.HttpResponse(str(results), status_code=200)
```

## Configuration Reference

| Parameter | Type | Description |
| --- | --- | --- |
| `arg_name` | `str` | First positional argument. Name of the handler parameter to inject into. |
| `url` | `str` | Database connection URL. Supports `%ENV_VAR%` substitution. |
| `table` | `str` | Default table for operations. |
| `engine_provider` | `EngineProvider` | Optional shared engine provider for connection pooling. |

## Mutual Exclusivity

- `inject_writer` and `output` cannot be used on the same handler.
- `inject_reader` and `input` can coexist but typically serve different use cases.
