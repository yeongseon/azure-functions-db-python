# ADR-005: Unified Package Design (trigger + binding → azure-functions-db)

## Status
**Accepted** (2026-04-08)

## Context

In the initial design, a package named `azure-functions-dbtrigger` was planned to provide only
DB change detection (trigger) functionality.
During development, the following requirements emerged:

1. Providing DB reads (input binding) and writes (output binding) together improves user experience
2. Splitting trigger and binding into separate packages forces users to install multiple packages
3. A single package (`azure-functions-db`) simplifies installation and configuration

## Decision

Rename `azure-functions-dbtrigger` to `azure-functions-db` and include both
trigger (change detection) and binding (read/write) in a single package.

### Internal Structure
- `core/`: shared infrastructure (EngineProvider, DbConfig, types, errors, serializers)
- `trigger/`: existing polling trigger design, preserved as-is
- `binding/`: DbReader (input), DbWriter (output)
- Dependency direction: trigger→core, binding→core; cross-imports between trigger↔binding are prohibited

### API Strategy
- Imperative API (DbReader, DbWriter) is primary
- Decorator API (`DbBindings` with `trigger`, `input`, `output`) provides Azure Functions-native DX
- Packages are unified; programming models are not

## Alternatives Considered

### Alternative A: Keep trigger-only (azure-functions-dbtrigger)
- Pros: focused scope, clear naming
- Cons: users who need binding must implement it themselves or install a separate package
- Rejected: poor user DX

### Alternative B: Separate packages (azure-functions-dbtrigger + azure-functions-dbbinding)
- Pros: clear separation of concerns
- Cons: requires installing two packages; engine/config duplication
- Rejected: "having to download multiple packages is inconvenient"

### Alternative C: Immediate unified API design (decorator-centric)
- Pros: clean, single API surface
- Cons: risk of decorator conflicting with Azure Functions runtime; locks in API before validation
- Rejected: safer to add decorators after the imperative API stabilizes

## Rationale

1. **Technical synergy**: natural sharing of engine/config/pool
2. **User convenience**: a single `pip install azure-functions-db` covers all DB operations
3. **Scope management**: internal core/trigger/binding separation isolates complexity
4. **Incremental delivery**: trigger implemented first; binding added in Phase 8+
5. **Design review**: adopted recommendation of "one package, but not one programming model"

## Market Research

- Microsoft Azure Functions SQL Extension: SQL Server only, integrated trigger+binding extension
- Python-native package with equivalent scope: does not currently exist
- Our differentiators: multi-DB + Python-first + SQLAlchemy + single package

## Consequences

- Package name: `azure-functions-db`
- Import: `azure_functions_db`
- Existing trigger design is preserved as-is
- Binding semantics defined in a separate document (22-binding-semantics.md)
- Development phasing: trigger (Phase 1–7) → shared core (Phase 8) → binding (Phase 9–11)
