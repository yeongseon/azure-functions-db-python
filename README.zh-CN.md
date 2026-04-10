# Azure Functions DB

[![PyPI](https://img.shields.io/pypi/v/azure-functions-db.svg)](https://pypi.org/project/azure-functions-db/)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://pypi.org/project/azure-functions-db/)
[![CI](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml)
[![Release](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml)
[![codecov](https://codecov.io/gh/yeongseon/azure-functions-db/branch/main/graph/badge.svg)](https://codecov.io/gh/yeongseon/azure-functions-db)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)
[![Docs](https://img.shields.io/badge/docs-gh--pages-blue)](https://yeongseon.github.io/azure-functions-db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Read this in: [English](README.md) | [한국어](README.ko.md) | [日本語](README.ja.md)

面向 **Azure Functions Python v2** 的数据库集成库，提供基于轮询的变更检测触发器，以及基于 SQLAlchemy 的输入/输出绑定。

---

**Azure Functions Python DX Toolkit** 的一部分
→ 为 Azure Functions 带来类似 FastAPI 的开发体验

## 为什么需要它

Azure Functions Python v2 缺少内置的数据库集成方案：

- **没有 DB 触发器** — 不同于 Cosmos DB，关系型数据库没有原生触发器
- **没有输入/输出绑定** — 无法以声明式方式在函数中读写数据库行
- **驱动选择复杂** — 每种数据库都需要不同驱动、连接字符串和配置
- **缺少变更检测** — polling、CDC、outbox 模式每次都要从零搭建

## 它能做什么

- **伪 DB 触发器** — 基于轮询的变更检测，带 checkpoint、lease 和 at-least-once 语义
- **多数据库支持** — 通过 SQLAlchemy dialect 支持 PostgreSQL、MySQL、SQL Server
- **一次 `pip install`** — 单一包，通过 extras 选择数据库驱动
- **数据注入** — `input` 直接注入查询结果；`output` 自动写入返回数据
- **客户端注入** — 需要命令式控制时可使用 `inject_reader`/`inject_writer`

## Shared Core

`azure-functions-db` 提供了面向后续绑定的共享基础设施。使用 `DbConfig` 统一连接配置；当多个组件需要复用惰性创建的 SQLAlchemy 引擎时，使用 `EngineProvider`。

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

你的 Function App 依赖应包含：

```text
azure-functions
azure-functions-db[postgres]
```

## Quick Start

### 该用哪个装饰器？

| Need | Decorator | Mode |
|------|-----------|------|
| Read data into handler | `input` | Declarative (data injection) |
| Write data to DB | `output` | Declarative (data injection) |
| Complex reads (multiple queries) | `inject_reader` | Imperative (client injection) |
| Complex writes (transactions) | `inject_writer` | Imperative (client injection) |
| React to DB changes | `trigger` | Event-driven (pseudo-trigger) |

### Input Binding (data injection)

`input` 会将实际查询结果直接注入处理函数，无需手动创建客户端。

**Row lookup mode** — 通过主键读取单行：

```python
from azure_functions_db import DbBindings

db = DbBindings()

# Static primary key
@db.input("user", url="%DB_URL%", table="users", pk={"id": 42})
def load_user(user: dict | None) -> None:
    if user:
        print(user["name"])

# Dynamic primary key — resolved from handler kwargs
@db.input("user", url="%DB_URL%", table="users",
             pk=lambda req: {"id": req.params["id"]})
def get_user(req, user: dict | None) -> None:
    print(user)
```

**Query mode** — 使用 SQL 获取多行：

```python
# Multiple rows by SQL query
@db.input("users", url="%DB_URL%",
             query="SELECT * FROM users WHERE active = :active",
             params={"active": True})
def list_active_users(users: list[dict]) -> None:
    for user in users:
        print(user["email"])
```

### Output Binding (data injection)

`output` 会向处理函数注入 `DbOut` 实例；调用 `.set()` 以显式写入。

```python
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

# Insert — call .set() with a dict for single row, list[dict] for batch
@db.output("out", url="%DB_URL%", table="orders")
def create_order(out: DbOut) -> str:
    out.set({"id": 1, "status": "pending", "total": 99.99})
    return "Created"

# Upsert — set action and conflict_columns
@db.output("out", url="%DB_URL%", table="orders",
              action="upsert", conflict_columns=["id"])
def upsert_orders(out: DbOut) -> str:
    out.set([
        {"id": 1, "status": "shipped", "total": 99.99},
        {"id": 2, "status": "pending", "total": 49.99},
    ])
    return "Upserted"
```

处理函数的返回值与数据库写入相互独立，可用于 HTTP 响应或其他用途。

```python
import azure.functions as func
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

@db.output("out", url="%DB_URL%", table="orders")
def create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    out.set({"id": 1, "status": "pending"})
    return func.HttpResponse("Created", status_code=201)
```

支持 upsert 的方言：PostgreSQL、SQLite、MySQL。

### Client Injection (imperative escape hatches)

对于复杂操作（多查询、事务、update/delete），可用 `inject_reader`/`inject_writer` 注入客户端实例。

```python
from azure_functions_db import DbBindings, DbReader, DbWriter

db = DbBindings()

@db.inject_reader("reader", url="%DB_URL%", table="users")
def complex_read(reader: DbReader) -> None:
    user = reader.get(pk={"id": 42})
    orders = reader.query("SELECT * FROM orders WHERE user_id = :uid", params={"uid": 42})

@db.inject_writer("writer", url="%DB_URL%", table="orders")
def complex_write(writer: DbWriter) -> None:
    writer.insert(data={"id": 1, "status": "pending"})
    writer.update(data={"status": "shipped"}, pk={"id": 1})
    writer.delete(pk={"id": 1})
```

### Trigger (change detection)

```python
import azure.functions as func
from azure.storage.blob import ContainerClient
from azure_functions_db import BlobCheckpointStore, DbBindings, RowChange, SqlAlchemySource

app = func.FunctionApp()
db = DbBindings()

source = SqlAlchemySource(
    url="%ORDERS_DB_URL%",
    table="orders",
    schema="public",
    cursor_column="updated_at",
    pk_columns=["id"],
)

checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str="%AzureWebJobsStorage%",
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
def orders_poll(timer: func.TimerRequest, events: list[RowChange]) -> None:
    for event in events:
        print(f"Order {event.pk}: {event.op}")
```

> 这是一个 **伪触发器** —— 需要配合真实的 Azure Functions 触发器（例如 timer）触发执行。

> 完整 API 参考见 [Python API Spec](docs/04-python-api-spec.md)。

### Combined: Trigger + Binding

处理数据库变更并将结果写入另一张表。使用 `EngineProvider` 共享连接池。

```python
import azure.functions as func
from azure.storage.blob import ContainerClient

from azure_functions_db import (
    BlobCheckpointStore,
    DbBindings,
    DbOut,
    EngineProvider,
    RowChange,
    SqlAlchemySource,
)

app = func.FunctionApp()
db = DbBindings()

engine_provider = EngineProvider()

source = SqlAlchemySource(
    url="%SOURCE_DB_URL%",
    table="orders",
    cursor_column="updated_at",
    pk_columns=["id"],
    engine_provider=engine_provider,
)

checkpoint_store = BlobCheckpointStore(
    container_client=ContainerClient.from_connection_string(
        conn_str="%AzureWebJobsStorage%",
        container_name="db-state",
    ),
    source_fingerprint=source.source_descriptor.fingerprint,
)

@app.function_name(name="orders_poll")
@app.schedule(schedule="0 */1 * * * *", arg_name="timer", use_monitor=True)
@db.trigger(arg_name="events", source=source, checkpoint_store=checkpoint_store)
@db.output(
    "out",
    url="%DEST_DB_URL%",
    table="processed_orders",
    action="upsert",
    conflict_columns=["order_id"],
    engine_provider=engine_provider,
)
def orders_poll(timer: func.TimerRequest, events: list[RowChange], out: DbOut) -> None:
    out.set([
        {
            "order_id": event.pk["id"],
            "customer": event.after["name"],
            "processed_at": str(event.cursor),
        }
        for event in events
        if event.after is not None
    ])
```

完整可运行示例见 [`examples/trigger_with_binding/`](examples/trigger_with_binding/)。

## Supported Databases

| Database | Extra | Driver |
|----------|-------|--------|
| PostgreSQL | `azure-functions-db[postgres]` | [psycopg](https://www.psycopg.org/) |
| MySQL | `azure-functions-db[mysql]` | [PyMySQL](https://pymysql.readthedocs.io/) |
| SQL Server | `azure-functions-db[mssql]` | [pyodbc](https://github.com/mkleehammer/pyodbc) |

## Scope

- Azure Functions Python **v2 programming model**
- Timer-triggered functions for poll-based change detection
- SQLAlchemy 2.0+ for database abstraction
- Checkpoint storage via Azure Blob Storage
- Read/write bindings via HTTP/Queue/Event triggers

该包**不会**实现原生 Azure Functions 触发器扩展。它是在现有 timer trigger 之上实现的轮询方案。

## Observability

`azure-functions-db` 提供结构化日志辅助能力以及轻量级 `MetricsCollector` 协议，可接入你自己的指标后端而无需引入重依赖。

```python
from collections.abc import Mapping

from azure_functions_db import MetricsCollector, PollTrigger


class PrintMetricsCollector:
    def increment(
        self, name: str, value: float = 1, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("increment", name, value, labels)

    def observe(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("observe", name, value, labels)

    def set_gauge(
        self, name: str, value: float, *, labels: Mapping[str, str] | None = None
    ) -> None:
        print("gauge", name, value, labels)


trigger = PollTrigger(
    name="orders",
    source=source,
    checkpoint_store=checkpoint_store,
    metrics=PrintMetricsCollector(),
)
```

## Key Design Decisions

- **Pseudo trigger** — 用基于 timer 的 polling 替代原生 C# 扩展（[ADR-001](docs/16-ADR-001-pseudo-trigger-over-native.md)）
- **SQLAlchemy-centric** — 用单一 ORM 层支持所有数据库（[ADR-002](docs/17-ADR-002-sqlalchemy-centric-adapter.md)）
- **Blob checkpoint** — 使用 Azure Blob Storage 持久化 checkpoint（[ADR-003](docs/18-ADR-003-blob-checkpoint-mvp.md)）
- **At-least-once** — 默认交付语义，支持幂等处理（[ADR-004](docs/19-ADR-004-at-least-once-default.md)）
- **Unified package** — 将 trigger + binding 放在同一包中（[ADR-005](docs/23-ADR-005-unified-package-design.md)）

## Duplicate Handling

该包提供 **at-least-once** 交付。进程崩溃、lease 切换或提交失败时可能出现重复。处理函数必须具备幂等性。详见 [Semantics — Duplicate Windows](docs/03-semantics.md#13-duplicate-and-reprocessing-windows)。

## Documentation

- Full docs: [yeongseon.github.io/azure-functions-db](https://yeongseon.github.io/azure-functions-db/)
- Examples: `examples/`
- [Architecture](docs/02-architecture.md)
- [Semantics](docs/03-semantics.md)
- [Python API Spec](docs/04-python-api-spec.md)
- [Adapter SDK](docs/05-adapter-sdk.md)

## Ecosystem

这是 **Azure Functions Python DX Toolkit** 的一部分：

| Package | Role |
|---------|------|
| [azure-functions-openapi](https://github.com/yeongseon/azure-functions-openapi) | OpenAPI spec generation and Swagger UI |
| [azure-functions-validation](https://github.com/yeongseon/azure-functions-validation) | Request/response validation and serialization |
| **azure-functions-db** | Database bindings for SQL, PostgreSQL, MySQL, SQLite, and Cosmos DB |
| [azure-functions-langgraph](https://github.com/yeongseon/azure-functions-langgraph) | LangGraph deployment adapter for Azure Functions |
| [azure-functions-scaffold](https://github.com/yeongseon/azure-functions-scaffold) | Project scaffolding CLI |
| [azure-functions-logging](https://github.com/yeongseon/azure-functions-logging) | Structured logging and observability |
| [azure-functions-doctor](https://github.com/yeongseon/azure-functions-doctor) | Pre-deploy diagnostic CLI |
| [azure-functions-durable-graph](https://github.com/yeongseon/azure-functions-durable-graph) | Manifest-first graph runtime with Durable Functions *(experimental)* |
| [azure-functions-python-cookbook](https://github.com/yeongseon/azure-functions-python-cookbook) | Recipes and examples |

## Disclaimer

本项目是独立的社区项目，与 Microsoft 没有关联，也未获得 Microsoft 的认可或维护。

Azure 和 Azure Functions 是 Microsoft Corporation 的商标。

## License

MIT
