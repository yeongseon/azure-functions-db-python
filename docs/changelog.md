# Changelog

This page documents the version history and migration paths for the `azure-functions-db` package.

## Versioning Scheme

This project follows Semantic Versioning (semver.org). Given a version number MAJOR.MINOR.PATCH, increment the:

- MAJOR version when you make incompatible API changes
- MINOR version when you add functionality in a backward compatible manner
- PATCH version when you make backward compatible bug fixes

The changelog is generated from Conventional Commits using git-cliff. Breaking changes are explicitly listed under the "Breaking Changes" section for each release.

## Full Version History

### Unreleased (post-0.1.0)

The following changes have been merged to `main` but are not yet released. Breaking changes are acceptable — this is a pre-1.0 package with no external users.

#### Breaking Changes

- Renamed `DbFunctionApp` to `DbBindings` (#33)
- Dropped `db_` prefix from all decorators: `trigger`, `input`, `output`, `inject_reader`, `inject_writer` (#41, #45)
- Replaced `OutputResult` with `DbOut` class using `.set()` pattern (#50)
- Narrowed public exports to 27 stable symbols (#34)

#### Features

- Added `input` decorator for data injection with row lookup and query modes (#35)
- Added `output` decorator for declarative writes with `DbOut` (#35, #50)
- Added `inject_reader`/`inject_writer` for imperative client injection (#35)
- Added decorator composition validation with mutual exclusivity rules (#43)
- Added partial env var substitution for connection URLs (#39)
- Added `engine_kwargs` passthrough on all decorators (#40)
- Added `EngineProvider` for shared connection pooling

#### Documentation

- Added async support matrix documentation (#37)
- Added input mode (row lookup / query) documentation (#36)
- Added lifecycle and thread-safety documentation (#44)
- Updated README with ecosystem branding (#42)
- Aligned documentation structure with sibling repos

### v0.1.0 (2025-04-08)

#### Features

- Add MetricsCollector, structured logging, and lag calculation (#21)
- Add PollTrigger, `db.poll()` decorator, and normalizers (#20)
- Add SqlAlchemySource with cursor-based polling (#19)
- Add BlobCheckpointStore with ETag-based CAS leasing (#18)
- Core types, errors, trigger events, context, retry, and runner (#17)
- Initial project scaffold — unified DB integration framework for Azure Functions Python v2

#### Documentation

- Translate docs to English and align README with series style (#16)
- Release process documentation and CI/CD workflow fixes (#22)
