# Test Strategy

## 1. Goals

- Verify no event loss
- Allow duplicates but verify they stay within expected bounds
- Guarantee per-DB query correctness
- Maintain semantics through restart / crash / lease races

## 2. Test Levels

### 2.1 Unit
Targets:
- cursor comparator
- checkpoint serializer
- event_id generation
- SQL builder
- retry classifier

### 2.2 Contract
Verify that all adapters satisfy the same input/output contract.

Required items:
- ordering
- empty batch semantics
- checkpoint generation
- timezone normalization
- duplicate-safe cursor

### 2.3 Integration
Validate against real DB containers.
- PostgreSQL
- MySQL/MariaDB
- SQL Server

### 2.4 E2E
Combination of Azure Functions local runtime + Azurite + DB container.

## 3. Key Scenarios

### S1 First Run
No initial checkpoint → first batch processed successfully

### S2 100 Rows with Identical Timestamp
Multiple rows with the same `updated_at` → processed sequentially with no omissions

### S3 Handler Failure
Verify checkpoint does not advance

### S4 Commit Failure
Duplicates possible, but verify no loss

### S5 Process Crash
Crash immediately after handler success → reprocessing allowed

### S6 Lease Race
Two runners start simultaneously → only one commits

### S7 Schema Drift
Additional columns added → base dict payload maintained

### S8 Empty Tick
No changes → no-op succeeds

### S9 Backfill
Stably process a large number of rows across multiple ticks

## 4. Non-Functional Tests

### Performance
- 1k / 10k / 100k row catch-up
- Latency/throughput by batch size

### Durability
- 24-hour soak test
- Random restart
- Storage transient fault injection

### Compatibility
- Python 3.11 / 3.12
- SQLAlchemy minor versions
- Functions Core Tools 4.x

## 5. CI Configuration

Recommended matrix:
- OS: ubuntu-latest
- Python: 3.11, 3.12
- DB: postgres, mysql, mssql

Stages:
1. lint
2. unit
3. integration
4. e2e nightly

## 6. Success Criteria

MVP baseline:
- loss: 0
- Only known duplicate windows exist
- Unsupported deletes behave as documented
- Checkpoint recovery is deterministic

## 7. Failure Matrix Coverage Map

| Case | Description | Covered By |
|------|------------|------------|
| A | Handler failure | test_trigger_runner.py (test_handler_failure_*) |
| B | Handler success, commit failure | test_trigger_runner.py (test_commit_failure_raises) |
| C | Commit after lease loss | test_hardening.py (test_stale_runner_*) + test_state_blob.py |
| D | Row updated again | Inherent to cursor polling (no specific test needed) |
| E | Crash after fetch before handler | test_hardening.py (test_crash_after_fetch_*) |
| F | Crash after partial handler | test_hardening.py (test_crash_after_partial_*) |
| G | Commit response timeout | test_hardening.py (test_ambiguous_commit_*) |
| H | Lease lost before commit | test_hardening.py (test_stale_runner_*) + test_state_blob.py (store-level lease expiry) |
| I | Permanently failing batch | test_hardening.py (test_permanent_handler_failure_*) |
