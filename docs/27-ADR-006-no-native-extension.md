# ADR-006: Python Wrapper over Native Azure Functions Extension

## Status
**Accepted** (2026-07-04)

## Context

`azure-functions-db-python` ships as a pure Python package that wraps existing
native Azure Functions triggers (primarily the timer trigger) with Python
decorators. It does **not** ship a native Azure Functions extension registered
with the Functions host.

That choice is currently explained inline in the README under
[`Why this exists`](https://github.com/yeongseon/azure-functions-db-python#why-this-exists), but it has never been
captured as a long-form ADR. When the question comes up in reviews or issues
— _"why not build a real SQLAlchemy trigger extension?"_ — the rationale needs
to live somewhere durable so future contributors do not accidentally re-litigate
it.

Two adjacent ADRs already touch this ground from different angles:

- [ADR-001 — Pseudo Trigger over Native Trigger](16-ADR-001-pseudo-trigger-over-native.md)
  explains the **data-flow** decision (poll on top of timer vs push from a real trigger).
- [ADR-002 — SQLAlchemy-centric Adapter](17-ADR-002-sqlalchemy-centric-adapter.md)
  explains the **abstraction** decision (SQLAlchemy dialects vs per-database driver code).

This ADR captures the **packaging and language boundary** decision that both of
those ADRs implicitly assume: the whole thing is a Python package on top of the
existing Functions host, not a .NET extension registered with the extension
bundle.

## Decision

Ship `azure-functions-db-python` as a **pure Python package** that installs via
`pip` and wraps the existing native timer trigger with Python decorators.

- Do **not** author, build, or publish a .NET Azure Functions extension.
- Do **not** register bindings with the Functions host via the WebJobs SDK.
- Do **not** depend on `func extensions install` or the extension bundle.
- Do **not** advertise runtime-native binding metadata to the Functions runtime.

The public surface (`DbBindings`, `@db.input`, `@db.output`, `@db.trigger`,
`SqlAlchemySource`, `BlobCheckpointStore`) is entirely Python. Every decorator
either resolves data before the handler runs, injects a client into the
handler, or wraps a real timer trigger and polls a source for changes.

## Trade-off: Runtime-Native Binding Registration vs Python Decorator Wrapper

| Aspect                              | Runtime-native extension                                       | This project (Python decorator wrapper)                        |
| ----------------------------------- | -------------------------------------------------------------- | -------------------------------------------------------------- |
| Where the trigger runs              | Functions host, before the Python worker is invoked            | Inside the Python worker, driven by a real timer trigger       |
| Binding metadata visible to host    | Yes (via `function.json` / decorator metadata)                 | No — the host only sees the underlying timer trigger           |
| Scaling                             | Functions scale controller inspects the trigger source         | Timer schedule + host scale controller — tied to timer cadence |
| Language of the extension           | C# / .NET (extension bundle)                                   | Python                                                         |
| Distribution                        | NuGet package via `func extensions install` / extension bundle | Standard `pip install azure-functions-db`                      |
| Delivery guarantee                  | Whatever the extension implements (often exactly-once)         | At-least-once, per [ADR-004](19-ADR-004-at-least-once-default.md) |
| Change detection surface            | Whatever the native source supports (e.g. SQL Change Tracking) | Cursor-column polling — works on any SQLAlchemy dialect        |
| Multi-database reach                | Bounded to what the extension author writes native code for    | Any database with a SQLAlchemy dialect                         |

The wrapper model gives up native binding metadata and scale-controller
integration in exchange for Python-first authorship, broad database reach, and
local-testable behavior. For this project's target user — a Python team on
Azure Functions that needs SQLAlchemy-powered DB integration — the trade lands
in favor of the wrapper.

## Rationale

### C# host extension surface area is hard to maintain from a Python-first project

A native Functions binding is a .NET component that implements the [WebJobs SDK
extension interfaces](https://learn.microsoft.com/azure/azure-functions/functions-bindings-register)
(`IExtensionConfigProvider`, `ITriggerBindingProvider`, `IBinding`, and
`IValueProvider` / `IAsyncCollector` for input and output bindings) and is
loaded by the Functions host through the extension bundle or an explicit
`func extensions install`. Building that component correctly requires:

- A .NET SDK build environment on every contributor's machine.
- NuGet packaging and signing infrastructure separate from the Python
  publishing pipeline.
- Ongoing tracking of the WebJobs SDK surface as the Functions runtime evolves.
- Cross-language debugging when a binding misbehaves — the extension runs in
  the .NET host process, the handler runs in the Python worker process.
- End-to-end tests that stand up the real Functions host, not just the Python
  runtime.

The project is authored and maintained by a Python-first team. Adopting a .NET
extension surface would multiply the maintenance cost of every trigger and
binding change and would gate every contribution on skills the contributor
base does not consistently have. The wrapper model keeps the entire codebase,
build, test, and release pipeline in Python.

### Functions scale-controller integration is not worth the extension cost for polling workloads

A runtime-native trigger extension can advertise a scale-controller hook that
lets the Functions host scale worker instances based on backlog observed at
the trigger source itself (queue depth, SQL change count, etc.). That
integration is valuable when the trigger source is push-based or exposes a
cheap backlog probe.

For DB polling workloads on general SQLAlchemy sources this integration is
either unavailable or expensive:

- **No standard backlog probe.** SQLAlchemy has no dialect-agnostic
  `unprocessed_change_count(...)` API. A scale controller that queried
  `COUNT(*) WHERE cursor > checkpoint` on every scale check would put
  additional load on the source database on top of the polling loop itself.
- **Per-dialect implementation.** Any useful scale metric would have to be
  implemented per dialect, which loses the SQLAlchemy-centric win from
  [ADR-002](17-ADR-002-sqlalchemy-centric-adapter.md).
- **Timer trigger already scales.** Because `@db.trigger` runs on top of a
  real `@app.schedule` timer trigger, the Functions host still scales the
  worker instances that run the polling loop — just via the timer's own
  scale-controller path rather than a bespoke DB scale controller. Throughput
  is controlled by the timer schedule (`schedule="0 */1 * * * *"`) and the
  configured batch size in `PollTrigger`.

The right answer for this project is to be explicit about the polling model
and its scaling boundary rather than to build a partial, per-dialect
scale-controller integration that would be hard to maintain and easy to get
wrong.

### The wrapper composes cleanly with the existing programming model

Because `@db.trigger` sits on top of a real timer trigger, users combine it
with existing Azure Functions primitives (`@app.schedule`, `@app.function_name`,
`use_monitor=True`, retry policies, host-level scaling) without any new
runtime concept. The same is true for `@db.input`, `@db.output`,
`@db.inject_reader`, and `@db.inject_writer` — they are decorators around a
plain Python handler, so they compose with any other native binding (HTTP,
Queue, Event Hub) the same handler declares.

### Distribution is a normal `pip install`

Users install `azure-functions-db-python` the same way they install any other
Python dependency:

```bash
pip install azure-functions-db[postgres]
```

No `func extensions install`. No extension bundle version pinning. The
`requirements.txt` entry is the only surface the user touches.

## Alternatives Considered

### Alternative A: Author a native .NET extension for a single database (e.g. PostgreSQL)

- **Pros:** Native binding metadata, scale-controller integration, delivery
  semantics enforced by the host.
- **Cons:** Requires .NET expertise on an otherwise Python-first project;
  ships only one dialect; every new dialect requires a new extension.
- **Rejected:** loses the SQLAlchemy-centric multi-dialect win from ADR-002
  and multiplies the maintenance surface across languages.

### Alternative B: Author a native .NET extension that wraps SQLAlchemy from the host process

- **Pros:** Would in principle offer native binding metadata for any
  SQLAlchemy dialect.
- **Cons:** SQLAlchemy is a Python library — running it from a .NET host
  process is not a supported configuration. Any bridge would end up
  reinventing dialect abstractions in C# or shelling out to Python, both
  of which are strictly worse than the current wrapper.
- **Rejected:** not technically viable without recreating SQLAlchemy in .NET.

### Alternative C: Ship a Python wrapper that pretends to be a native binding via `function.json` shims

- **Pros:** Would make bindings appear in Functions portal metadata.
- **Cons:** Would mislead the Functions runtime about the actual trigger
  source; would still not give scale-controller integration; would break the
  moment the Functions programming model changes shape.
- **Rejected:** dishonest metadata is worse than no metadata.

### Alternative D: Adopted — Python wrapper over the existing timer trigger

- **Pros:** Python-first, wide DB reach, single `pip install`, testable
  locally without the Functions host, delivery semantics fully controlled
  in library code, composes with any existing native binding.
- **Cons:** No native binding metadata, no per-DB scale-controller
  integration, delivery is at-least-once (not exactly-once), throughput is
  bounded by the timer schedule.
- **Accepted.**

## Consequences

### Positive

- **Python-only contribution surface.** New contributors need a Python
  toolchain and nothing else.
- **Broad database reach.** Any SQLAlchemy dialect works — PostgreSQL, MySQL,
  SQL Server out of the box; Oracle, DuckDB, CockroachDB, and others via BYOD.
- **Standard packaging.** `pip install`, standard Python versioning, standard
  extras — no extension bundle coupling.
- **Local-first testing.** The wrapper can be exercised end-to-end without
  the Functions host, which is the foundation of the project's `pytest`-based
  test suite.
- **Composability.** `@db.trigger` and the input/output/client decorators
  stack cleanly with any other native binding a handler declares.

### Negative

- **No native binding metadata.** The Functions host and Azure Portal do not
  see `@db.trigger` or `@db.input` as first-class bindings. Portal binding
  visualizations, `function.json` inspectors, and any tooling that reads
  binding metadata from the host will only see the underlying timer trigger.
- **No per-DB scale controller.** Worker scaling follows the timer trigger's
  own scale-controller path; the Functions host has no direct visibility into
  polling backlog on the DB side. Users that need push-based scaling should
  prefer the [official Azure SQL bindings](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql)
  for Azure SQL / SQL Server workloads.
- **At-least-once delivery.** Duplicates are possible across process crashes,
  lease transitions, and checkpoint commit failures. Handlers must be
  idempotent. See [ADR-004](19-ADR-004-at-least-once-default.md) and
  [Semantics — Duplicate Windows](03-semantics.md#13-duplicate-and-reprocessing-windows)
  for the formal contract.
- **Timer-driven throughput ceiling.** Throughput is bounded by
  `schedule` cadence multiplied by `PollTrigger` batch size. Sub-second
  latency between DB change and handler invocation is not a goal of this
  project.

### Follow-up

- Even if the Functions host later gains a first-class extension model for
  cross-language triggers, the public Python API (`DbBindings`, `@db.trigger`,
  `SqlAlchemySource`, `BlobCheckpointStore`) will remain stable. A future
  native-backed implementation would be a swap-in behind the same decorators,
  not a rewrite of user code.
- If a specific database gains a compelling native trigger surface (for
  example Postgres logical replication as a general-purpose Python client
  library), it may be added as an alternate `SourceAdapter` implementation
  rather than as a native Functions extension.

## References

- README — [`Why this exists`](https://github.com/yeongseon/azure-functions-db-python#why-this-exists) and
  [`Compared with official Azure SQL bindings`](https://github.com/yeongseon/azure-functions-db-python#compared-with-official-azure-sql-bindings)
- [ADR-001 — Pseudo Trigger over Native Trigger](16-ADR-001-pseudo-trigger-over-native.md)
- [ADR-002 — SQLAlchemy-centric Adapter](17-ADR-002-sqlalchemy-centric-adapter.md)
- [ADR-004 — At-least-once as Default Guarantee](19-ADR-004-at-least-once-default.md)
- [ADR-005 — Unified Package Design](23-ADR-005-unified-package-design.md)
- Microsoft Learn — [Register Azure Functions binding extensions](https://learn.microsoft.com/azure/azure-functions/functions-bindings-register)
- Microsoft Learn — [Azure Functions extension bundles](https://learn.microsoft.com/azure/azure-functions/functions-bindings-register#extension-bundles)
- Microsoft Learn — [Azure Functions SQL trigger (official extension)](https://learn.microsoft.com/azure/azure-functions/functions-bindings-azure-sql-trigger)
- Azure Functions Host — [WebJobs SDK bindings source](https://github.com/Azure/azure-webjobs-sdk)
