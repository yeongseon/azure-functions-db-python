# Azure Functions DB

[![PyPI](https://img.shields.io/pypi/v/azure-functions-db.svg)](https://pypi.org/project/azure-functions-db/)
[![Python Version](https://img.shields.io/badge/python-3.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://pypi.org/project/azure-functions-db/)
[![CI](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml)
[![Release](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml)
[![codecov](https://codecov.io/gh/yeongseon/azure-functions-db/branch/main/graph/badge.svg)](https://codecov.io/gh/yeongseon/azure-functions-db)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)
[![Docs](https://img.shields.io/badge/docs-gh--pages-blue)](https://yeongseon.github.io/azure-functions-db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Unified database integration for **Azure Functions Python v2** — change detection (trigger via polling), input binding (DbReader), and output binding (DbWriter) in a single package using SQLAlchemy.

---

Part of the **Azure Functions Python DX Toolkit**
→ Bring FastAPI-like developer experience to Azure Functions

## Why this exists

Azure Functions Python v2 has no built-in database integration story:

- **No DB trigger** — unlike Cosmos DB, there is no native trigger for relational databases
- **No input/output bindings** — no declarative way to read or write DB rows from a function
- **Driver confusion** — each database requires different drivers, connection strings, and setup
- **No change detection** — polling, CDC, or outbox patterns must be built from scratch every time

## What it does

- **Pseudo DB trigger** — poll-based change detection with checkpoint, lease, and at-least-once delivery
- **DbReader** — input binding for reading rows from any supported database
- **DbWriter** — output binding for writing rows with automatic batching
- **Multi-DB support** — PostgreSQL, MySQL, and SQL Server via SQLAlchemy dialects
- **Single `pip install`** — one package with optional extras for each database driver

## Installation

```bash
# Core package (pick your database)
pip install azure-functions-db[postgres]
pip install azure-functions-db[mysql]
pip install azure-functions-db[mssql]

# Multiple databases
pip install azure-functions-db[postgres,mysql]

# All drivers
pip install azure-functions-db[all]
```

Your Function App dependencies should include:

```text
azure-functions
azure-functions-db[postgres]
```

## Quick Start

### Trigger (change detection)

```python
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure_functions_db import BlobCheckpointStore, PollTrigger, RowChange, SqlAlchemySource

app = func.FunctionApp()

source = SqlAlchemySource(
    url="%ORDERS_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
)

trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=BlobCheckpointStore(
        container_client=ContainerClient.from_connection_string(
            conn_str="%AzureWebJobsStorage%",
            container_name="db-state",
        ),
        source_fingerprint=source.source_descriptor.fingerprint,
    ),
    batch_size=100,
)


def handle_orders(events: list[RowChange]) -> None:
    for event in events:
        print(f"Order {event.pk}: {event.op}")


@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
def orders_poll(timer: func.TimerRequest) -> None:
    trigger.run(timer=timer, handler=handle_orders)
```

### Input Binding (DbReader)

```python
from azure_functions_db import DbReader

reader = DbReader(
    connection_string="postgresql+psycopg://user:pass@host/db",
    table="products",
)


@app.route(route="products/{id}", methods=["GET"])
def get_product(req: func.HttpRequest) -> func.HttpResponse:
    product = reader.read(id=req.route_params["id"])
    return func.HttpResponse(json.dumps(product), mimetype="application/json")
```

### Output Binding (DbWriter)

```python
from azure_functions_db import DbWriter

writer = DbWriter(
    connection_string="postgresql+psycopg://user:pass@host/db",
    table="audit_logs",
)


@app.route(route="orders", methods=["POST"])
def create_order(req: func.HttpRequest) -> func.HttpResponse:
    order = req.get_json()
    writer.write({"action": "order_created", "payload": order})
    return func.HttpResponse(status_code=201)
```

## Supported Databases

| Database | Extra | Driver |
|----------|-------|--------|
| PostgreSQL | `azure-functions-db[postgres]` | [psycopg](https://www.psycopg.org/) |
| MySQL | `azure-functions-db[mysql]` | [PyMySQL](https://pymysql.readthedocs.io/) |
| SQL Server | `azure-functions-db[mssql]` | [pyodbc](https://github.com/mkleehammer/pyodbc) |

## Scope

- Azure Functions Python **v2 programming model**
- Timer-triggered functions for change detection
- HTTP/Queue/Event-triggered functions for read/write bindings
- SQLAlchemy 2.0+ for database abstraction
- Checkpoint storage via Azure Blob Storage

This package does **not** implement a native Azure Functions trigger extension. It uses a poll-based approach on top of the existing timer trigger.

## Key Design Decisions

- **Pseudo trigger** — timer-based polling instead of native C# extension ([ADR-001](docs/16-ADR-001-pseudo-trigger-over-native.md))
- **SQLAlchemy-centric** — single ORM layer for all databases ([ADR-002](docs/17-ADR-002-sqlalchemy-centric-adapter.md))
- **Blob checkpoint** — Azure Blob Storage for checkpoint persistence ([ADR-003](docs/18-ADR-003-blob-checkpoint-mvp.md))
- **At-least-once** — default delivery guarantee with idempotency support ([ADR-004](docs/19-ADR-004-at-least-once-default.md))
- **Unified package** — trigger + binding in one package ([ADR-005](docs/23-ADR-005-unified-package-design.md))

## Documentation

- Full docs: [yeongseon.github.io/azure-functions-db](https://yeongseon.github.io/azure-functions-db/)
- Examples: `examples/`
- [Architecture](docs/02-architecture.md)
- [Semantics](docs/03-semantics.md)
- [Python API Spec](docs/04-python-api-spec.md)
- [Adapter SDK](docs/05-adapter-sdk.md)

## Ecosystem

Part of the **Azure Functions Python DX Toolkit**:

| Package | Role |
|---------|------|
| [azure-functions-openapi](https://github.com/yeongseon/azure-functions-openapi) | OpenAPI spec and Swagger UI |
| [azure-functions-validation](https://github.com/yeongseon/azure-functions-validation) | Request and response validation |
| **azure-functions-db** | Database trigger and bindings |
| [azure-functions-logging](https://github.com/yeongseon/azure-functions-logging) | Structured logging and observability |
| [azure-functions-doctor](https://github.com/yeongseon/azure-functions-doctor) | Pre-deploy diagnostic CLI |
| [azure-functions-scaffold](https://github.com/yeongseon/azure-functions-scaffold) | Project scaffolding |
| [azure-functions-python-cookbook](https://github.com/yeongseon/azure-functions-python-cookbook) | Recipes and examples |

## Disclaimer

This project is an independent community project and is not affiliated with,
endorsed by, or maintained by Microsoft.

Azure and Azure Functions are trademarks of Microsoft Corporation.

## License

MIT
