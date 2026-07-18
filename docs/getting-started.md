# Getting Started

This quickstart walks you from zero to a working database-integrated Azure Functions
app in a few minutes.

By the end, you will have:

- An input binding reading rows from a database
- An output binding writing rows to a database
- Client injection for complex operations

!!! tip "Who this is for"
    This page is for teams using the Azure Functions Python v2 programming model
    (`func.FunctionApp()` and decorators).

## Prerequisites

Before starting, make sure you have:

1. Python 3.11 or newer.
2. An Azure Functions Python v2 app structure.
3. A running database (PostgreSQL, MySQL, or SQL Server).
4. Dependencies installed:
    - `azure-functions`
    - `azure-functions-db[postgres]` (or your database extra)

See [Installation](installation.md) for version details.

## Step 1: Install the package

```bash
pip install azure-functions-db[postgres]
```

## Step 2: Add an input binding

Create or update `function_app.py` with this example:

```python
import azure.functions as func

from azure_functions_db import DbBindings

app = func.FunctionApp()
db = DbBindings()


@app.function_name(name="get_user")
@app.route(route="users/{user_id}", methods=["GET"], auth_level=func.AuthLevel.ANONYMOUS)
@db.input("user", url="%DB_URL%", table="users",
          pk=lambda req: {"id": int(req.route_params["user_id"])})
def get_user(req: func.HttpRequest, user: dict | None) -> func.HttpResponse:
    if user is None:
        return func.HttpResponse("Not found", status_code=404)
    return func.HttpResponse(str(user), status_code=200)
```

### Why this works

- `@db.input("user", ...)` injects the query result into the `user` parameter.
- `pk=lambda req: {...}` resolves the primary key dynamically from the request.
- The handler receives a typed `dict | None` — no database client needed.

!!! note "Decorator order"
    Place `@db.input(...)` closest to the function definition,
    below `@app.route(...)`.

## Step 3: Add an output binding

Add another handler that writes data:

```python
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

### Why this works

- `@db.output("out", ...)` injects a `DbOut` instance into the `out` parameter.
- Call `out.set(data)` to write — the handler's return value is independent.
- Pass a `dict` for single row or `list[dict]` for batch insert.

## Step 4: Run your app locally

Start your Azure Functions host:

```bash
func start
```

Make sure your environment has the `DB_URL` variable set:

```bash
export DB_URL="postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
```

Or configure it in `local.settings.json`:

```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "DB_URL": "postgresql+psycopg://postgres:postgres@localhost:5432/mydb"
  }
}
```

## Step 5: Test with curl

Read a user:

```bash
curl -i http://localhost:7071/api/users/1
```

Expected response:

```http
HTTP/1.1 200 OK
Content-Type: text/plain

{'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
```

Create an order:

```bash
curl -i -X POST http://localhost:7071/api/orders \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "total": 99.99}'
```

Expected response:

```http
HTTP/1.1 201 Created

Created
```

## Optional: Client injection for complex operations

For multiple queries or transactions, use `inject_reader` / `inject_writer`:

```python
from azure_functions_db import DbBindings, DbReader, DbWriter

db = DbBindings()


@db.inject_reader("reader", url="%DB_URL%", table="users")
@db.inject_writer("writer", url="%DB_URL%", table="audit_log")
def complex_operation(reader: DbReader, writer: DbWriter) -> None:
    user = reader.get(pk={"id": 42})
    if user:
        writer.insert(data={"user_id": 42, "action": "accessed"})
```

## Optional: Trigger for change detection

Detect database changes with the poll-based trigger. See the
[Trigger example](examples/trigger.md) for a complete walkthrough.

## Troubleshooting checkpoints

If something does not work as expected:

1. Confirm `DB_URL` environment variable is set and the database is reachable.
2. Confirm the database driver extra is installed (`pip show psycopg`).
3. Confirm `@db.input(...)` or `@db.output(...)` is closest to the function definition.
4. Confirm the table and column names match your database schema.
5. Confirm you are using the Python v2 programming model.

For deeper fixes, go to [Troubleshooting](troubleshooting.md).

## Next steps

- Explore [Input Binding](examples/input_binding.md) examples.
- Explore [Output Binding](examples/output_binding.md) examples.
- Learn about [Trigger](examples/trigger.md) for change detection.
- Read [Client Injection](examples/client_injection.md) for advanced patterns.
- Check [API Reference](api.md) for complete signatures.
