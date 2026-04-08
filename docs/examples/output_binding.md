# Output Binding

The `output` decorator injects a `DbOut` instance into your handler. Call `.set()`
to write data to the database.

## Basic Insert

```python
from azure_functions_db import DbBindings, DbOut

db = DbBindings()


@db.output("out", url="%DB_URL%", table="orders")
def create_order(out: DbOut) -> None:
    out.set({"id": 1, "status": "pending", "total": 99.99})
```

The write happens when `out.set()` is called. The handler's return value is
independent of the database write.

## Batch Insert

Pass a list of dicts to insert multiple rows:

```python
@db.output("out", url="%DB_URL%", table="orders")
def create_orders(out: DbOut) -> None:
    out.set([
        {"id": 1, "status": "pending", "total": 99.99},
        {"id": 2, "status": "pending", "total": 49.99},
    ])
```

## Upsert

Set `action="upsert"` and specify `conflict_columns` to handle duplicates:

```python
@db.output("out", url="%DB_URL%", table="orders",
           action="upsert", conflict_columns=["id"])
def upsert_orders(out: DbOut) -> None:
    out.set([
        {"id": 1, "status": "shipped", "total": 99.99},
        {"id": 2, "status": "pending", "total": 49.99},
    ])
```

Supported upsert dialects: PostgreSQL, SQLite, MySQL.

## With HTTP Trigger

Complete example with an HTTP POST handler:

```python
import azure.functions as func

from azure_functions_db import DbBindings, DbOut

app = func.FunctionApp()
db = DbBindings()


@app.function_name(name="create_order")
@app.route(route="orders", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS)
@db.output("out", url="%DB_URL%", table="orders")
def create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    body = req.get_json()
    out.set({"id": body["id"], "status": "pending", "total": body["total"]})
    return func.HttpResponse("Created", status_code=201)
```

The return value (`HttpResponse`) is sent to the caller. The database write
happens independently via `out.set()`.

## Configuration Reference

| Parameter | Type | Description |
| --- | --- | --- |
| `arg_name` | `str` | First positional argument. Name of the handler parameter to inject `DbOut` into. |
| `url` | `str` | Database connection URL. Supports `%ENV_VAR%` substitution. |
| `table` | `str` | Target table name. |
| `action` | `str` | Write action: `"insert"` (default) or `"upsert"`. |
| `conflict_columns` | `list[str]` | Columns for upsert conflict detection. Required when `action="upsert"`. |
| `engine_provider` | `EngineProvider` | Optional shared engine provider for connection pooling. |
| `engine_kwargs` | `dict` | Additional keyword arguments passed to `create_engine()`. |
