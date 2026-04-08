# Azure Functions DB

Unified DB integration (trigger + input/output binding) for Azure Functions Python v2.

## Features

- **DB Change Detection (Trigger)**: Poll-based pseudo trigger that detects new/changed rows via cursor tracking
- **Input Binding (DbReader)**: Read rows from any SQLAlchemy-supported database
- **Output Binding (DbWriter)**: Write, upsert, update, and delete rows
- **SQLAlchemy-powered**: Works with PostgreSQL, MySQL, SQLite, and MSSQL
- **Azure Functions v2 native**: Integrates with the Python v2 programming model

## Quick Start

```bash
pip install azure-functions-db[postgres]
```

```python
from azure_functions_db import DbBindings, PollTrigger, DbReader, DbWriter
from azure_functions_db import DbConfig, SqlAlchemySource
```

For detailed usage, see the [Python API Spec](04-python-api-spec.md).

## Documentation

- [Project Overview](00-project-overview.md)
- [Architecture](02-architecture.md)
- [Binding Semantics](22-binding-semantics.md)
- [API Reference](api.md)
