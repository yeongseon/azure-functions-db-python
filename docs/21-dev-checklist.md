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
- [ ] README quickstart
- [ ] PyPI metadata
- [ ] Versioning
- [ ] Changelog
- [ ] Release checklist

## Phase 7: Hardening
- [ ] Chaos tests
- [ ] Crash recovery tests
- [ ] Duplicate window documentation
- [ ] Benchmark

## Phase 8: Shared Core
- [ ] DbConfig
- [ ] EngineProvider (lazy singleton, pool management)
- [ ] Shared types (Row, RowDict)
- [ ] Shared error hierarchy refactor
- [ ] Shared serializers extraction

## Phase 9: Binding - Input
- [ ] DbReader
- [ ] get() (single row by PK)
- [ ] query() (raw SQL)
- [ ] Connection lifecycle
- [ ] Unit tests
- [ ] Integration tests (Postgres/MySQL/SQL Server)

## Phase 10: Binding - Output
- [ ] DbWriter
- [ ] insert / upsert / update / delete
- [ ] insert_many / upsert_many (batch)
- [ ] Transaction management
- [ ] Unit tests
- [ ] Integration tests (Postgres/MySQL/SQL Server)

## Phase 11: Binding Integration
- [ ] Trigger + output binding combined example
- [ ] Binding decorator sugar (optional)
- [ ] Docs update
- [ ] Sample function_app.py with binding
