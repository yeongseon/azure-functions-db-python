# PRD - azure-functions-db

## Overview

`azure-functions-db` provides poll-based database change detection and SQLAlchemy-powered
input/output bindings for the Azure Functions Python v2 programming model.

It is intended for decorator-based `func.FunctionApp()` applications that want database integration
without leaving the Azure Functions execution model.

## Problem Statement

Azure Functions Python applications that depend on relational databases often lack a consistent way to:

- react to row-level changes without native DB triggers
- inject query results into handlers with minimal boilerplate
- write records declaratively from function return flow
- standardize multi-database access and driver setup across projects

This leads to duplicated polling code, inconsistent change-processing semantics, and fragmented
developer experience across teams.

## Goals

- Provide a poll-trigger abstraction with checkpoint and lease management.
- Provide decorator-first input/output bindings and client injection APIs.
- Support PostgreSQL, MySQL, and SQL Server through SQLAlchemy dialects and extras.
- Stay aligned with Azure Functions Python v2 and companion DX Toolkit packages.

## Non-Goals

- Building a native Azure Functions trigger extension in C#
- Replacing Azure Functions scheduling and hosting concepts
- Providing full ORM modeling or migration tooling
- Owning API docs generation or request/response validation concerns

## Primary Users

- Maintainers of Azure Functions Python workloads backed by SQL databases
- Teams migrating queue/cron polling scripts into Functions-hosted handlers
- Users combining database bindings with `azure-functions-openapi` and `azure-functions-validation`

## Core Use Cases

- Poll a source table on a timer and process `RowChange` batches
- Inject a single row or query result directly into a handler via `@db.input`
- Persist single-row or batch writes via `@db.output` and `DbOut.set(...)`
- Execute imperative reads/writes and transactions via `@db.inject_reader` and `@db.inject_writer`

## Success Criteria

- Representative examples run with predictable at-least-once trigger semantics.
- Bindings cover common read/write workflows across supported SQL dialects.
- Docs and runnable samples remain aligned through CI and smoke-tested examples.

## Example-First Design

### Philosophy

Small ecosystem libraries gain adoption when developers can copy a working example and get immediate
results. `azure-functions-db` treats runnable examples as first-class deliverables so trigger,
binding, and engine-sharing patterns are visible and reproducible.

### Quick Start

The shortest path from zero to an injected DB result:

```python
from azure_functions_db import DbBindings

db = DbBindings()


@db.input(
    "users",
    url="%DB_URL%",
    query="SELECT * FROM users WHERE active = :active",
    params={"active": True},
)
def list_active_users(users: list[dict]) -> None:
    for user in users:
        print(user["email"])
```

For change processing, compose a timer trigger with `@db.trigger(...)` and a checkpoint store.

### Examples Inventory

| Role | Path | Pattern |
|---|---|---|
| Representative | `examples/poll_orders` | Timer-triggered poll with `@db.trigger` and `RowChange` handling |
| Representative | `examples/trigger_with_binding` | Combined trigger + output binding with shared `EngineProvider` |
| Integration | `examples/e2e_app` | End-to-end Function App sample for local/host validation |
| Utility | `examples/usage_postgres.py` | PostgreSQL-focused usage snippet for quick adaptation |

All examples should remain runnable and reflect the documented trigger/binding contracts.
