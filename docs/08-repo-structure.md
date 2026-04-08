# Repository Structure

## Recommended Layout

```text
azure-functions-db/
  src/
    azure_functions_db/
      __init__.py
      api.py

      core/
        __init__.py
        config.py        # DbConfig
        engine.py        # EngineProvider
        types.py         # Row, RowDict, shared types
        errors.py        # base error hierarchy
        serializers.py   # type normalization

      trigger/
        __init__.py
        runner.py        # PollRunner (was PollTrigger)
        context.py       # PollContext
        events.py        # RowChange, EventNormalizer
        retry.py
        decorators.py    # DbBindings (trigger, input, output)

        state/
          __init__.py
          store.py
          blob_store.py
          serializers.py

        adapters/
          __init__.py
          base.py
          sqlalchemy_base.py
          postgres.py
          mysql.py
          mssql.py

        strategies/
          __init__.py
          cursor.py
          outbox.py
          cdc_base.py

      binding/
        __init__.py
        reader.py        # DbReader (input)
        writer.py        # DbWriter (output)
        session.py       # per-invocation session management

      integrations/
        __init__.py
        azure_functions.py

      cli/
        __init__.py
        main.py

  tests/
    unit/
    integration/
    e2e/
    fixtures/

  examples/
  docs/
  schemas/
```

## Module Responsibilities

### api.py / __init__.py
Public re-export surface. Exposes `PollTrigger`, `SqlAlchemySource`, `BlobCheckpointStore`, `DbReader`, `DbWriter`, `DbBindings`.

### core/*
Common layer providing shared configuration, engine/pool, types, errors, and serializers.

### trigger/*
Core pseudo trigger orchestration body. Includes polling, adapters, state, retry, and decorators.

### trigger/adapters/*
Per-DB change fetch implementations.

### trigger/state/*
Checkpoint/lease persistence.

### binding/*
Imperative input/output binding layer. Performs DB read/write within a function invocation.

### integrations/azure_functions.py
Thin glue layer for integration with the Azure Functions runtime.

### cli/main.py
- validate config
- inspect checkpoint
- reset checkpoint
- backfill helpers

## Import Rules

- `binding` does not import from `trigger`.
- `trigger` does not import from `binding`.
- Both `trigger` and `binding` import shared elements only from `core`.
- `trigger/state` has no knowledge of `trigger/adapters`.
- `integrations` references only the `api` layer.
- `examples` uses only the public API.

## Package Separation Strategy

Core package:
- `azure-functions-db`

Extension package candidates:
- `azure-functions-db-mongo`
- `azure-functions-db-postgres-cdc`
- `azure-functions-db-otel`
- `azure-functions-db-cli`

Import name:
- `azure_functions_db`

Package strategy:
- Bundle trigger and binding in a single distribution to reduce installation and operational complexity.
- Per-DB advanced adapters or observability extensions can be separated into optional packages.

## Versioning Policy

- `0.x`: Rapid iteration
- `1.0`: After stabilization of PollTrigger / BlobCheckpointStore / SqlAlchemySource / DbReader / DbWriter
- Semantic versioning applied
