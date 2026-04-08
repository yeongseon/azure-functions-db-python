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
