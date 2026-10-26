# Database operation verification matrix

“Verified” below means the listed test actually executed on the named backend;
an executable test alone is not proof of execution. “Implemented” means runtime
code exists but the combination has not been executed in the cited evidence.
SQLAlchemy dialect availability alone is not certification.

Verification date: 2026-10-16. Local evidence used Python 3.11.16,
SQLAlchemy 2.0.50, package source `0fb2224`, and SQLite. PostgreSQL, MySQL, and
SQL Server require their selected CI service lanes; they were not available in
this local environment.

| Operation / contract | SQLite | PostgreSQL | MySQL | SQL Server |
| --- | --- | --- | --- | --- |
| Read / reflected table | Verified | Implemented; live lane | Implemented; live lane | Implemented; live lane |
| Integer cursor + PK resume | Verified | Implemented; live lane | Implemented; live lane | Implemented; live lane |
| DateTime cursor + equal-time PK tie-break | Verified | Unverified locally | Unverified locally | Unverified locally |
| Insert / batch insert | Verified | Implemented; live lane | Implemented; live lane | Implemented; live lane |
| Update / delete / transaction | Verified | Implemented; live lane | Implemented; live lane | Implemented; live lane |
| Upsert / batch upsert | Verified | Implemented; live lane | Implemented; live lane | Explicitly unsupported |

## Persisted typed cursor evidence

[`test_persisted_datetime_cursor_resumes_fresh_source`](https://github.com/yeongseon/azure-functions-db-python/blob/main/tests/integration/db/test_source_live.py)
uses the existing backend fixtures/markers. It executes five ticks over four
rows, writes each complete checkpoint as JSON to disk, reloads it into a fresh
store, source and runner, and verifies exact composite primary keys and every
resume position across equal timestamps. Both reflected-table mode and raw
query mode (`cursor_types=[datetime, int, int]`) are covered. Raw query results
are not promised to be typed rows; `cursor_types` governs checkpoint binding.

Local Python 3.11 execution: `1010 passed, 87 skipped, 11 deselected, 23 warnings
in 42.00s`. SQLite's two new cases passed; remote backends were skipped because
their settings were absent. Required CI runs the same marked cases on actual
PostgreSQL, MySQL and SQL Server; see the issue-linked PR's final CI evidence.

This is also the representative **composite cursor** persistence round trip.
Decimal and UUID have focused codec tests in
[`test_adapter_sqlalchemy.py`](https://github.com/yeongseon/azure-functions-db-python/blob/main/tests/test_adapter_sqlalchemy.py); no live
persisted Decimal/UUID cross-backend claim is made. The supported upsert
dialects remain PostgreSQL, SQLite and MySQL; SQL Server upsert is unsupported.

Selected-backend CI must fail closed when its configured backend collects no
tests or skips every selected case. Ordinary local runs intentionally skip
servers whose `TEST_*_URL` variable is absent.
