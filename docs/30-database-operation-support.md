# Database operation verification matrix

Verified combinations below mean the repository has an executable test for
that backend and operation. “Implemented” means runtime code exists but this
review did not execute that combination against a live server. SQLAlchemy
dialect availability alone is not certification.

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

The DateTime contract test follows the complete typed path: a real database
column is fetched, serialized to a JSON-safe checkpoint, rebound by a fresh
fetch, and checked across equal timestamps and batch boundaries. Decimal and
UUID checkpoint parts have focused codec tests; no cross-backend live claim is
made for them.

Selected-backend CI must fail closed when its configured backend collects no
tests or skips every selected case. Ordinary local runs intentionally skip
servers whose `TEST_*_URL` variable is absent.
