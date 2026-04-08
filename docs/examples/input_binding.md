# Input Binding

The `input` decorator injects query results directly into your handler — no
database client needed.

## Row Lookup Mode

Fetch a single row by primary key:

```python
from azure_functions_db import DbBindings

db = DbBindings()


@db.input("user", url="%DB_URL%", table="users", pk={"id": 42})
def load_user(user: dict | None) -> None:
    if user:
        print(user["name"])
```

### Dynamic Primary Key

Resolve the primary key from handler arguments at runtime:

```python
@db.input("user", url="%DB_URL%", table="users",
          pk=lambda req: {"id": int(req.route_params["user_id"])})
def get_user(req, user: dict | None) -> None:
    print(user)
```

The `pk` callable receives the same arguments as your handler.

## Query Mode

Fetch multiple rows with a SQL query:

```python
@db.input("users", url="%DB_URL%", query="SELECT * FROM users WHERE active = :active",
          params={"active": True})
def list_active_users(users: list[dict]) -> None:
    for user in users:
        print(user["email"])
```

## With HTTP Trigger

Complete example combining Azure Functions HTTP trigger with input binding:

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

## Configuration Reference

| Parameter | Type | Description |
| --- | --- | --- |
| `arg_name` | `str` | First positional argument. Name of the handler parameter to inject into. |
| `url` | `str` | Database connection URL. Supports `%ENV_VAR%` substitution. |
| `table` | `str` | Table name for row lookup mode. |
| `pk` | `dict` or callable | Primary key for row lookup. Static dict or callable receiving handler args. |
| `query` | `str` | SQL query for query mode. Mutually exclusive with `table`/`pk`. |
| `params` | `dict` | Query parameters for named placeholders (`:name`). |
| `engine_provider` | `EngineProvider` | Optional shared engine provider for connection pooling. |
| `engine_kwargs` | `dict` | Additional keyword arguments passed to `create_engine()`. |
