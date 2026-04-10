# Azure Functions DB

[![PyPI](https://img.shields.io/pypi/v/azure-functions-db.svg)](https://pypi.org/project/azure-functions-db/)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://pypi.org/project/azure-functions-db/)
[![CI](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml)
[![Release](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml)
[![codecov](https://codecov.io/gh/yeongseon/azure-functions-db/branch/main/graph/badge.svg)](https://codecov.io/gh/yeongseon/azure-functions-db)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)
[![Docs](https://img.shields.io/badge/docs-gh--pages-blue)](https://yeongseon.github.io/azure-functions-db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Read this in: [English](README.md) | [한국어](README.ko.md) | [简体中文](README.zh-CN.md)

**Azure Functions Python v2** 向けのデータベース統合ライブラリです。SQLAlchemy を使ったポーリング型変更検知トリガーと入出力バインディングを提供します。

---

**Azure Functions Python DX Toolkit** の一部
→ Azure Functions に FastAPI ライクな開発体験を提供

## これが必要な理由

Azure Functions Python v2 には、データベース統合の標準的な仕組みがありません。

- **DB トリガーがない** — Cosmos DB とは異なり、RDB 向けのネイティブトリガーがない
- **入出力バインディングがない** — 関数で DB 行を宣言的に読み書きする方法がない
- **ドライバー選定が複雑** — DB ごとにドライバー、接続文字列、セットアップが異なる
- **変更検知がない** — polling / CDC / outbox を毎回ゼロから実装する必要がある

## 提供機能

- **疑似 DB トリガー** — チェックポイント、リース、at-least-once 配信を備えたポーリング型変更検知
- **マルチ DB 対応** — SQLAlchemy dialect により PostgreSQL / MySQL / SQL Server をサポート
- **単一 `pip install`** — DB ごとの optional extras を持つ 1 パッケージ
- **データ注入** — `input` はクエリ結果を直接注入し、`output` は戻り値の書き込みを自動化
- **クライアント注入** — 必要に応じて `inject_reader`/`inject_writer` で命令的制御が可能

## Shared Core

`azure-functions-db` は今後のバインディングで共有する基盤を公開しています。正規化された接続設定には `DbConfig` を、複数コンポーネントで遅延生成された SQLAlchemy エンジンを共有する場合は `EngineProvider` を利用してください。

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

Function App の依存関係には次を含めてください。

```text
azure-functions
azure-functions-db[postgres]
```

## Quick Start

### どのデコレーターを使うべきか？

| Need | Decorator | Mode |
|------|-----------|------|
| Read data into handler | `input` | Declarative (data injection) |
| Write data to DB | `output` | Declarative (data injection) |
| Complex reads (multiple queries) | `inject_reader` | Imperative (client injection) |
| Complex writes (transactions) | `inject_writer` | Imperative (client injection) |
| React to DB changes | `trigger` | Event-driven (pseudo-trigger) |

### Input Binding (data injection)

`input` は実際のクエリ結果をそのままハンドラーに注入します。クライアントは不要です。

**Row lookup mode** — 主キーで 1 行取得:

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

**Query mode** — SQL で複数行取得:

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

`output` は `DbOut` インスタンスをハンドラーに注入します。明示的に書き込むには `.set()` を呼びます。

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

ハンドラーの戻り値は書き込み処理とは独立しています。HTTP レスポンスなどに自由に使えます。

```python
import azure.functions as func
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

@db.output("out", url="%DB_URL%", table="orders")
def create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    out.set({"id": 1, "status": "pending"})
    return func.HttpResponse("Created", status_code=201)
```

対応 upsert dialect: PostgreSQL, SQLite, MySQL。

### Client Injection (imperative escape hatches)

複雑な処理（複数クエリ、トランザクション、update/delete）では、`inject_reader`/`inject_writer` でクライアントを注入して使います。

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

> これは **疑似トリガー** です。実際に動かすには Azure Functions の実トリガー（例: timer）が必要です。

> API 全体は [Python API Spec](docs/04-python-api-spec.md) を参照してください。

### Combined: Trigger + Binding

DB 変更を処理し、結果を別テーブルへ書き込みます。接続プール共有のため `EngineProvider` を利用します。

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

完全に実行可能なサンプルは [`examples/trigger_with_binding/`](examples/trigger_with_binding/) を参照してください。

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

このパッケージはネイティブ Azure Functions トリガー拡張を実装しません。既存の timer trigger 上で動くポーリング方式を採用しています。

## Observability

`azure-functions-db` は構造化ログヘルパーと軽量な `MetricsCollector` プロトコルを提供し、重い依存追加なしで任意のメトリクス基盤と接続できます。

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

- **Pseudo trigger** — native C# extension ではなく timer ベース polling ([ADR-001](docs/16-ADR-001-pseudo-trigger-over-native.md))
- **SQLAlchemy-centric** — 全 DB 向け単一 ORM レイヤー ([ADR-002](docs/17-ADR-002-sqlalchemy-centric-adapter.md))
- **Blob checkpoint** — チェックポイント永続化に Azure Blob Storage ([ADR-003](docs/18-ADR-003-blob-checkpoint-mvp.md))
- **At-least-once** — 冪等性を前提にした既定配信保証 ([ADR-004](docs/19-ADR-004-at-least-once-default.md))
- **Unified package** — trigger + binding を 1 パッケージで提供 ([ADR-005](docs/23-ADR-005-unified-package-design.md))

## Duplicate Handling

このパッケージは **at-least-once** 配信を提供します。プロセスクラッシュ、リース遷移、コミット失敗時に重複が発生する可能性があります。ハンドラーは冪等に実装してください。詳細は [Semantics — Duplicate Windows](docs/03-semantics.md#13-duplicate-and-reprocessing-windows) を参照してください。

## Documentation

- Full docs: [yeongseon.github.io/azure-functions-db](https://yeongseon.github.io/azure-functions-db/)
- Examples: `examples/`
- [Architecture](docs/02-architecture.md)
- [Semantics](docs/03-semantics.md)
- [Python API Spec](docs/04-python-api-spec.md)
- [Adapter SDK](docs/05-adapter-sdk.md)

## Ecosystem

**Azure Functions Python DX Toolkit** の一部:

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

このプロジェクトは独立したコミュニティプロジェクトであり、Microsoft とは提携・承認・保守関係にありません。

Azure および Azure Functions は Microsoft Corporation の商標です。

## License

MIT
