# Azure Functions DB

[![PyPI](https://img.shields.io/pypi/v/azure-functions-db.svg)](https://pypi.org/project/azure-functions-db/)
[![Python Version](https://img.shields.io/badge/python-3.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue)](https://pypi.org/project/azure-functions-db/)
[![CI](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/ci-test.yml)
[![Release](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/yeongseon/azure-functions-db/actions/workflows/publish-pypi.yml)
[![codecov](https://codecov.io/gh/yeongseon/azure-functions-db/branch/main/graph/badge.svg)](https://codecov.io/gh/yeongseon/azure-functions-db)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://pre-commit.com/)
[![Docs](https://img.shields.io/badge/docs-gh--pages-blue)](https://yeongseon.github.io/azure-functions-db/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Read this in: [English](README.md) | [日本語](README.ja.md) | [简体中文](README.zh-CN.md)

**Azure Functions Python v2**를 위한 데이터베이스 통합 라이브러리로, SQLAlchemy 기반의 폴링 변경 감지 트리거와 입력/출력 바인딩을 제공합니다.

---

**Azure Functions Python DX Toolkit**의 일부
→ Azure Functions에 FastAPI와 유사한 개발자 경험 제공

## 이 프로젝트가 필요한 이유

Azure Functions Python v2에는 데이터베이스 통합에 대한 기본 스토리가 없습니다.

- **DB 트리거 부재** — Cosmos DB와 달리 관계형 데이터베이스용 네이티브 트리거가 없음
- **입력/출력 바인딩 부재** — 함수에서 DB 행을 선언적으로 읽거나 쓰는 방식이 없음
- **드라이버 혼란** — 데이터베이스마다 드라이버, 연결 문자열, 설정이 모두 다름
- **변경 감지 부재** — polling, CDC, outbox 패턴을 매번 처음부터 구현해야 함

## 제공 기능

- **의사 DB 트리거** — 체크포인트, 리스, at-least-once 전달을 포함한 폴링 기반 변경 감지
- **멀티 DB 지원** — SQLAlchemy dialect를 통한 PostgreSQL, MySQL, SQL Server 지원
- **단일 `pip install`** — 데이터베이스별 선택 extras를 갖춘 단일 패키지
- **데이터 주입** — `input`은 쿼리 결과를 직접 주입하고 `output`은 반환값 쓰기를 자동화
- **클라이언트 주입** — 필요 시 `inject_reader`/`inject_writer`로 명령형 제어 가능

## Shared Core

`azure-functions-db`는 향후 바인딩에서 재사용할 공통 인프라를 제공합니다. 정규화된 연결 설정에는 `DbConfig`를, 여러 컴포넌트에서 지연 생성 SQLAlchemy 엔진을 공유할 때는 `EngineProvider`를 사용하세요.

## 설치

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

Function App 의존성에는 다음이 포함되어야 합니다.

```text
azure-functions
azure-functions-db[postgres]
```

## 빠른 시작

### 어떤 데코레이터를 써야 하나요?

| Need | Decorator | Mode |
|------|-----------|------|
| Read data into handler | `input` | Declarative (data injection) |
| Write data to DB | `output` | Declarative (data injection) |
| Complex reads (multiple queries) | `inject_reader` | Imperative (client injection) |
| Complex writes (transactions) | `inject_writer` | Imperative (client injection) |
| React to DB changes | `trigger` | Event-driven (pseudo-trigger) |

### Input Binding (data injection)

`input`은 실제 쿼리 결과를 핸들러에 직접 주입합니다. 별도 클라이언트가 필요하지 않습니다.

**Row lookup mode** — 기본 키로 단일 행 조회:

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

**Query mode** — SQL로 다중 행 조회:

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

`output`은 `DbOut` 인스턴스를 핸들러에 주입합니다. 명시적으로 쓰려면 `.set()`을 호출하세요.

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

핸들러의 반환값은 DB 쓰기와 독립적입니다. HTTP 응답 등 원하는 용도로 사용하세요.

```python
import azure.functions as func
from azure_functions_db import DbBindings, DbOut

db = DbBindings()

@db.output("out", url="%DB_URL%", table="orders")
def create_order(req: func.HttpRequest, out: DbOut) -> func.HttpResponse:
    out.set({"id": 1, "status": "pending"})
    return func.HttpResponse("Created", status_code=201)
```

지원되는 upsert dialect: PostgreSQL, SQLite, MySQL.

### Client Injection (imperative escape hatches)

복잡한 작업(다중 쿼리, 트랜잭션, update/delete)에는 `inject_reader`/`inject_writer`로 클라이언트 인스턴스를 주입받아 사용하세요.

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

> 이것은 **의사 트리거**입니다. 실행을 위해서는 실제 Azure Functions 트리거(예: timer)가 필요합니다.

> 전체 API는 [Python API Spec](docs/04-python-api-spec.md)을 참고하세요.

### Combined: Trigger + Binding

데이터베이스 변경을 처리하고 다른 테이블에 결과를 기록합니다. 공유 연결 풀링을 위해 `EngineProvider`를 사용합니다.

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

전체 실행 예제는 [`examples/trigger_with_binding/`](examples/trigger_with_binding/)를 참고하세요.

## 지원 데이터베이스

| Database | Extra | Driver |
|----------|-------|--------|
| PostgreSQL | `azure-functions-db[postgres]` | [psycopg](https://www.psycopg.org/) |
| MySQL | `azure-functions-db[mysql]` | [PyMySQL](https://pymysql.readthedocs.io/) |
| SQL Server | `azure-functions-db[mssql]` | [pyodbc](https://github.com/mkleehammer/pyodbc) |

## 범위

- Azure Functions Python **v2 programming model**
- Timer-triggered functions for poll-based change detection
- SQLAlchemy 2.0+ for database abstraction
- Checkpoint storage via Azure Blob Storage
- Read/write bindings via HTTP/Queue/Event triggers

이 패키지는 네이티브 Azure Functions 트리거 확장을 구현하지 않습니다. 기존 timer trigger 위에서 동작하는 폴링 기반 접근을 사용합니다.

## 관측 가능성

`azure-functions-db`는 구조화 로그 헬퍼와 가벼운 `MetricsCollector` 프로토콜을 제공하여, 하드 의존성 추가 없이 원하는 메트릭 백엔드를 연결할 수 있습니다.

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

## 주요 설계 결정

- **Pseudo trigger** — native C# extension 대신 timer 기반 polling ([ADR-001](docs/16-ADR-001-pseudo-trigger-over-native.md))
- **SQLAlchemy-centric** — 모든 데이터베이스를 위한 단일 ORM 계층 ([ADR-002](docs/17-ADR-002-sqlalchemy-centric-adapter.md))
- **Blob checkpoint** — 체크포인트 영속화를 위한 Azure Blob Storage ([ADR-003](docs/18-ADR-003-blob-checkpoint-mvp.md))
- **At-least-once** — 멱등성 지원을 전제로 한 기본 전달 보장 ([ADR-004](docs/19-ADR-004-at-least-once-default.md))
- **Unified package** — trigger + binding을 하나의 패키지로 제공 ([ADR-005](docs/23-ADR-005-unified-package-design.md))

## 중복 처리

이 패키지는 **at-least-once** 전달을 제공합니다. 프로세스 크래시, 리스 전환, 커밋 실패 시 중복이 발생할 수 있습니다. 핸들러는 멱등하게 작성되어야 합니다. 자세한 내용은 [Semantics — Duplicate Windows](docs/03-semantics.md#13-duplicate-and-reprocessing-windows)를 참고하세요.

## 문서

- 전체 문서: [yeongseon.github.io/azure-functions-db](https://yeongseon.github.io/azure-functions-db/)
- 예제: `examples/`
- [Architecture](docs/02-architecture.md)
- [Semantics](docs/03-semantics.md)
- [Python API Spec](docs/04-python-api-spec.md)
- [Adapter SDK](docs/05-adapter-sdk.md)

## 에코시스템

**Azure Functions Python DX Toolkit**의 일부:

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

## 면책 조항

이 프로젝트는 독립적인 커뮤니티 프로젝트이며 Microsoft와 제휴, 보증 또는 유지보수 관계가 없습니다.

Azure 및 Azure Functions는 Microsoft Corporation의 상표입니다.

## License

MIT
