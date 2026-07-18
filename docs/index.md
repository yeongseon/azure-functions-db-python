# Azure Functions DB

Unified DB integration (trigger + input/output binding) for Azure Functions Python v2.

## Features

- **DB Change Detection (Trigger)**: Poll-based pseudo trigger that detects new/changed rows via cursor tracking
- **Input Binding**: Read rows declaratively with `@db.input()` — data injected into your handler
- **Output Binding**: Write rows declaratively with `@db.output()` and `DbOut.set()`
- **Client Injection**: Full imperative control with `@db.inject_reader()` / `@db.inject_writer()`
- **Multi-DB Support**: PostgreSQL, MySQL, SQL Server via SQLAlchemy
- **Azure Functions v2 Native**: Integrates with the Python v2 programming model

## Quick Start

```bash
pip install azure-functions-db[postgres]
```

```python
from azure_functions_db import DbBindings, DbOut, DbReader, DbWriter
from azure_functions_db import SqlAlchemySource, BlobCheckpointStore, EngineProvider
```

## Documentation

- [Installation](installation.md) — install and verify the package
- [Getting Started](getting-started.md) — quickstart walkthrough
- [Examples](examples/input_binding.md) — complete code examples
- [API Reference](api.md) — auto-generated API docs
- [Troubleshooting](troubleshooting.md) — common issues and solutions
- [FAQ](faq.md) — frequently asked questions

## Docs ownership & canonical sources

This project maintains two complementary documentation sets:

- **Numbered specs & ADRs** (`00-*` … `28-*`, including the ADRs) are the **canonical source for design and API contract**. `04-python-api-spec.md` is the authoritative API reference; the auto-generated [API Reference](api.md) is derived from source and cross-links back to it.
- **Standard user-facing pages** (this index, [Installation](installation.md), [Getting Started](getting-started.md), [Troubleshooting](troubleshooting.md), [FAQ](faq.md)) are **derivations** tuned for task-oriented reading.

**When changing behavior, update `04-python-api-spec.md` first, then propagate to the user-facing pages** so the two systems do not drift.
