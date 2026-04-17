# Project Overview

## Project Name

`azure-functions-db-python`

## Problem Statement

Azure Functions provides a great trigger/binding experience for Azure-native services, but does not offer a consistent **trigger + binding development experience** across general-purpose databases. Teams using multiple RDBMSes repeatedly face the following problems:

- Polling code is written from scratch for each database.
- Connection/session code is duplicated in every function just to read DB rows.
- Input/output patterns are manually assembled each time to write function results to other tables.
- Checkpoint / dedup / batching / retry policies differ across teams.
- Function code becomes polluted with operational boilerplate rather than business logic.
- Local development and production behavior diverge significantly.
- The absence of a "looks like a trigger" abstraction reduces usability even within teams.

## Solution

`azure-functions-db-python` is a **unified DB integration framework** for use in Azure Functions Python apps.

- Execution model: Azure Functions `timer trigger`
- Change detection: polling, outbox, CDC adapter
- Data reading: imperative input binding (`DbReader`)
- Data writing: imperative output binding (`DbWriter`)
- DB connectivity: SQLAlchemy-centered + non-SQL adapter extensibility
- State storage: Blob checkpoint store (MVP), expandable to Table/Cosmos later
- Guarantee level: fundamentally a **pseudo trigger with near at-least-once semantics**
- Target language: Python (MVP)

## Project Philosophy

### 1. Honest Naming
This project is **not a native trigger**.
The terms `pseudo trigger`, `poll trigger`, and `db event trigger` are maintained throughout docs and code.

### 2. Define the Guarantee Contract First
More important than the number of supported databases is the **contract around failure, retry, deduplication, and ordering**.

### 3. SQLAlchemy as a Reach Multiplier
The goal is not an ORM — it's an **event-driven data processing DX**.

### 4. Production-Ready Defaults
Configurations that work comfortably locally and don't break in production take priority.

### 5. Start Small, Expand Strategies
The MVP supports only `cursor polling`;
DB-native capabilities (change feed / binlog / WAL / outbox) are added incrementally.

### 6. All DB Operations in One Package
Change detection (trigger), reading (input binding), and writing (output binding) are provided as **a single package** — not separate products — so Azure Functions developers can handle all DB operations in one consistent way.

## Vision

Make Azure Functions developers think:

> "If I need to work with a database, I just install azure-functions-db-python."

## MVP Scope

- PostgreSQL
- MySQL / MariaDB
- SQL Server
- Python Azure Functions v2 programming model
- Blob checkpoint store
- Cursor/updated_at polling
- Row dict payload
- DbReader
- DbWriter
- Single-table polling
- Single-function ownership per poller

## Out of Scope (Initial)

- True custom trigger extensions
- Exactly-once guarantees
- Cross-table transactional change merge
- Fully automatic DB delete event detection
- Automatic schema migration management
- Strong global ordering
- Multi-tenant SaaS control plane
