# PRD

## 1. Product Name
azure-functions-db-python

## 2. One-Line Description
A framework that provides unified DB change detection (trigger), reading (input binding), and writing (output binding) for Azure Functions Python apps.

## 3. Background

In practice, the following two requirements frequently arise together:

- Leverage Azure Functions' serverless execution model.
- But the data source is not Azure-native services — it's various SQL databases.

This gap causes each team to implement the following on their own:

- Scheduled execution
- Incremental fetch
- Watermark storage
- Retry / quarantine
- Idempotency
- Observability
- Connection/session management for querying DB rows
- Boilerplate for writing function output results to another database

This project standardizes those repeated implementations.

## 4. Goals

### Must Achieve
- Must be easy to attach to Azure Functions Python v2.
- Must work with at least `updated_at` or monotonic cursor-based polling on Postgres/MySQL/SQL Server.
- The lease/checkpoint/dedup contract must be documented.
- Must provide safe-on-retry patterns as defaults.
- Handler developers must be able to focus on business logic.

### Nice to Have
- Pydantic model mapping
- OpenTelemetry hook
- Backfill mode
- Poison batch quarantine
- Event Hub / Service Bus relay mode

## 5. Non-Goals
- This is not a product that creates or manages DB-internal triggers.
- This is not a .NET extension that extends the Azure Functions host.
- This is not a SQLAlchemy ORM replacement.
- This does not replace an entire CDC platform.

## 6. Users

### Primary Users
- Python-based Azure Functions developers
- Backend engineers building data sync/post-processing/notification functions
- Teams wanting DB event-driven workflows

### Secondary Users
- Platform teams
- Internal standard library maintenance teams
- Teams operating multi-DB environments

## 7. Representative Use Cases

### UC-1 Post-Processing After Orders Table Changes
Detect inserts/updates on the orders table and sync to ERP, Slack, and CRM.

### UC-2 Multi-DB Periodic Sync
Poll multiple customer databases at a set interval and process only the changes.

### UC-3 Low-Cost Pseudo CDC
Automate simple change processing for services where a full CDC pipeline is overkill.

### UC-4 Outbox Consumption
Safely consume a service DB's outbox table and forward to Service Bus.

### UC-5 DB Row Lookup from HTTP Trigger
An HTTP trigger function uses `DbReader` to look up single or multiple rows based on request parameters.

### UC-6 Writing Function Results to Another DB Table
Use `DbWriter` to insert/upsert function execution results to another table, storing post-processing state.

### UC-7 Syncing Detected Changes to Another DB
Process change events detected by a trigger in the handler, then apply them to another DB via output binding.

## 8. Product Requirements

### API Requirements
- Support decorator-based declarations
- Support imperative runner
- Allow SQLAlchemy URL usage
- Support source/table/query-based definitions
- Allow configuration of cursor column, PK, batch size, schedule, and retry policy
- Pluggable checkpoint store
- Support handler pre/post hooks

### Binding API Requirements
- `DbReader` and `DbWriter` must be provided as public APIs
- `DbReader.get(pk=...)` must support single-row lookups
- `DbReader.query(sql, params=...)` must support raw SQL queries
- `DbWriter.insert/upsert/update/delete` must be provided
- `DbWriter.insert_many/upsert_many` must support batch writes
- Must be usable in the same way from both within trigger handlers and regular function code
- Import name must consistently be `azure_functions_db`

### Operational Requirements
- Two instances must not process the same batch simultaneously without a lease
- Checkpoint must not advance before batch success
- Failed batches must be retriable
- Structured logging is mandatory
- Metrics/export hooks must be provided

### Binding Operational Requirements
- Connection/engine management must be reused via shared core
- Binding failures must surface as invocation failures (fail closed)
- Not found must be expressed as `None` return, not an exception
- Default write unit must be a per-write-call transaction
- Predictable behavior must be provided without ambient transactions
- A minimum behavioral contract must be maintained across Postgres/MySQL/SQL Server

### Documentation Requirements
- Do not overstate the scope of guarantees
- Explicitly document delete detection limitations
- Document tie-handling for identical timestamps
- Document differences between backfill and normal mode

## 9. Success Metrics

### Initial Qualitative Metrics
- Users can bring up a local PoC within 30 minutes
- Handler code can be implemented in under 20 lines
- Per-DB custom polling code can be deleted

### Initial Quantitative Metrics
- MVP 3-DB integration test passes (green)
- Zero missed rows in a 10k-row catch-up benchmark
- In forced-restart/duplicate-execution chaos tests: duplicates are allowed, but zero loss
- README quickstart completable within 20 minutes

## 10. Release Criteria

### v0.1 GA-ish Criteria
- Postgres/MySQL/SQL Server polling succeeds
- Blob checkpoint store stabilized
- at-least-once contract documented
- local/CI/integration examples provided
- Scope limited to trigger functionality

### v0.2
- DbReader / DbWriter added
- input/output binding documented
- Pydantic mapping
- Quarantine
- Backfill mode
- Enhanced observability

### v0.3
- Mongo adapter
- Outbox helper
- Relay mode (Event Hub/Service Bus)

### v0.5
- CDC-capable adapters
- Richer partitioning
- Dynamic multi-source scheduler
