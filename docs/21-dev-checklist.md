# Development Checklist

## Phase 0: Design Finalization
- [x] Finalize package name
- [x] Finalize public API names
- [x] Finalize semantics documentation
- [x] Finalize 3 target DBs for MVP

## Phase 1: Core Implementation
- [x] RowChange
- [x] PollContext
- [x] Error hierarchy
- [x] Retry helper
- [x] PollRunner

## Phase 2: State Storage
- [x] BlobCheckpointStore
- [x] Lease/fencing implementation
- [x] Checkpoint serializer
- [x] Source fingerprint

## Phase 3: Adapters
- [x] SQLAlchemy base adapter
- [x] Postgres adapter
- [x] MySQL adapter
- [x] SQL Server adapter
- [x] Contract tests

## Phase 4: Functions Integration
- [x] Imperative runner
- [x] Decorator sugar
- [x] Sample function_app.py
- [x] Local runtime smoke test

## Phase 5: Observability
- [x] Structured logs
- [x] Metrics hooks
- [x] Lag calculation
- [x] Dashboard examples

## Phase 6: Release
- [x] README quickstart
- [x] PyPI metadata
- [x] Versioning
- [x] Changelog
- [x] Release checklist

## Phase 7: Hardening
- [x] Chaos tests
- [x] Crash recovery tests
- [x] Duplicate window documentation
- [x] Benchmark

## Phase 8: Shared Core
- [x] DbConfig
- [x] EngineProvider (lazy singleton, pool management)
- [x] Shared types (Row, RowDict)
- [x] Shared error hierarchy refactor
- [x] Shared serializers extraction

## Phase 9: Binding - Input
- [x] DbReader
- [x] get() (single row by PK)
- [x] query() (raw SQL)
- [x] Connection lifecycle
- [x] Unit tests
- [x] Integration tests (SQLite)

## Phase 10: Binding - Output
- [x] DbWriter
- [x] insert / upsert / update / delete
- [x] insert_many / upsert_many (batch)
- [x] Transaction management
- [x] Unit tests
- [x] Integration tests (SQLite)

## Phase 11: Binding Integration
- [x] Trigger + output binding combined example
- [x] Binding decorator sugar (DbBindings: trigger, input, output)
- [x] Docs update
- [x] Sample function_app.py with binding
