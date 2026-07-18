# Changelog

All notable changes to this project will be documented in this file.
## [0.4.4] - 2026-07-18

### Bug Fixes

- *(trigger)* Apply RetryPolicy in tick() (closes #171) (#172) 
- *(ci)* Replace fragile apt Core Tools install with pinned npm (#170) 
- *(writer)* Preserve original exception when rollback fails; raise WriteError for unsupported upsert dialect 
- Emit UserWarning for no-params raw SQL queries and unimplemented RetryPolicy 
- *(trigger)* Emit debug log when cursor datetime is tz-naive and lag metric is skipped 
- *(engine)* Register atexit cleanup and harden cache key type discrimination 

### Documentation

- Enable mermaid rendering and add canonical poll-trigger diagram (#194) 
- Canonicalize pip install token and clarify async-handler semantics (#193) 
- Add discoverability metadata (pepy badge + llms.txt) (#200) 
- Add 'For AI Coding Assistants' section pointing to llms.txt (#187) 
- Add 'Read this in:' translation links to main README (#185) 
- *(examples)* Add MySQL polling-trigger docker-compose example (#175) 
- *(comparison)* Add azure-functions-db vs Official Azure SQL Bindings page (#174) 
- *(adr)* Add ADR-006 — Python wrapper over native Azure Functions extension (#173) 

### Miscellaneous Tasks

- *(deps)* Bump github/codeql-action/analyze from 4.36.2 to 4.37.1 (#176) 
- *(deps)* Bump github/codeql-action/init from 4.36.2 to 4.37.1 (#178) 
- *(deps)* Bump mypy from 2.1.0 to 2.3.0 (#183) 
- *(deps)* Bump softprops/action-gh-release from 3.0.1 to 3.0.2 (#179) 
- *(deps)* Bump actions/setup-node from 6.4.0 to 7.0.0 (#180) 
- *(deps)* Bump actions/stale from 10.3.0 to 10.4.0 (#181) 
- *(deps)* Bump ruff from 0.15.20 to 0.15.22 (#182) 
- *(test)* Align e2e marker filter and correct Codecov version comment (#195) 
- *(deps)* Bump ruff from 0.15.16 to 0.15.20 (#169) 
- *(deps)* Bump softprops/action-gh-release from 3.0.0 to 3.0.1 (#168) 
- *(deps)* Bump actions/setup-python from 6.2.0 to 6.3.0 (#166) 
- *(deps)* Bump actions/checkout from 6.0.2 to 7.0.0 (#165) 
- *(deps)* Bump github/codeql-action from 4.36.1 to 4.36.2 (#147) 
- *(deps)* Bump codecov/codecov-action from 6.0.1 to 7.0.0 (#148) 
- *(deps)* Bump ruff from 0.15.15 to 0.15.16 (#149) 
- *(ci)* Standardize Action pinning to immutable SHAs (#146) 
- *(deps)* Bump ruff from 0.15.12 to 0.15.15 (#145) 
- *(deps)* Bump github/codeql-action from 4.35.4 to 4.36.1 (#144) 
- *(deps)* Bump actions/stale from 10.2.0 to 10.3.0 (#142) 
- *(deps)* Bump codecov/codecov-action from 6.0.0 to 6.0.1 (#137) 

### Other

- Bump version to 0.4.4 

### Refactor

- *(state)* De-duplicate release_lease via _verify_lease (#198) 
## [0.4.3] - 2026-05-14

### Documentation

- Update changelog 
- Fix ecosystem table names, badges, and Part of intro line 
- Mark cookbook as dogfood, fix ecosystem table description 

### Miscellaneous Tasks

- *(deps)* Bump mypy from 1.20.2 to 2.1.0 
- *(deps)* Bump github/codeql-action from 4.35.2 to 4.35.4 
- *(release)* Fix changelog template and decouple version test from literals 

### Other

- Bump version to 0.4.3 

### Styling

- *(tests)* Sort imports in test_decorator.py 

### Testing

- Raise coverage to 95%+ and enforce via AGENTS.md and pyproject.toml 
## [0.4.2] - 2026-04-30

### Bug Fixes

- Use hatch build in publish workflow to fix empty wheel on PyPI 
## [0.4.1] - 2026-04-30

### Bug Fixes

- Update version assertion to 0.4.1 
- Bump version to 0.4.1 to fix empty PyPI wheel 
## [0.4.0] - 2026-04-29

### Bug Fixes

- *(out)* Align DbOut.set type hints with runtime contract (Sequence + runtime list-only) (#125) 
- *(writer)* Roll back active transaction on DbWriter.close() (#126) 
- *(engine)* Reject connect_args inside engine_kwargs (#124) 
- *(trigger)* Treat LeaseConflictError as no-op in PollRunner.tick() (#122) 

### Documentation

- Update changelog 
- *(async)* Document async writer transaction limitation (#129) 
- *(examples)* Add runnable PostgreSQL + Azurite poll-trigger example (#100) (#110) 
- *(trigger)* Add production checklist for polling triggers (#111) 
- *(core)* Add EngineProvider lifecycle & pooling guidance (#99) (#112) 
- *(trigger)* Add polling runtime semantics & failure scenarios page (#98) (#108) 

### Features

- *(async)* Add scalar/one/one_or_none to _AsyncDbReaderProxy (#123) 

### Miscellaneous Tasks

- *(deps)* Bump ruff from 0.15.10 to 0.15.12 (#90) 
- *(deps)* Bump mypy from 1.20.1 to 1.20.2 (#89) 
- Allow 'test/' branch prefix in branch-naming-validation (#131) 

### Other

- Bump version to 0.4.0 

### Testing

- *(observability)* Pin metrics docs/code drift contract (#127) 
- *(examples)* Add PostgreSQL poll-trigger smoke script (#130) 
## [0.3.0] - 2026-04-26

### Documentation

- Update changelog 
- *(agents)* Add Issue Conventions section to AGENTS.md 

### Features

- DbWriter.transaction() and DbReader.scalar/one/one_or_none (#95) (#97) 

### Other

- Bump version to 0.3.0 

### Testing

- Bump expected __version__ to 0.3.0 ahead of release 
## [0.2.2] - 2026-04-26

### Bug Fixes

- Align distribution name with PyPI publish name (azure-functions-db) (#92) 
- Declare wheel packages explicitly for hatchling (#91) 
- Remove stale site/ artifacts and fix broken issue template URL 

### Documentation

- Update changelog 
- Reposition README against official Azure SQL bindings (#93) (#96) 
- Replace text-only binding flow with accurate Mermaid diagrams 

### Miscellaneous Tasks

- *(deps)* Bump actions/upload-artifact from 4 to 7 
- *(deps)* Bump github/codeql-action from 4.35.1 to 4.35.2 
- *(deps)* Bump mypy from 1.20.0 to 1.20.1 
- *(deps)* Bump actions/setup-python from 5 to 6 
- *(deps)* Bump actions/github-script from 8.0.0 to 9.0.0 
- *(deps)* Bump softprops/action-gh-release from 2.6.1 to 3.0.0 
- *(deps)* Bump actions/checkout from 4 to 6 
- Update repo references for azure-functions-{feature}-python naming convention 

### Other

- Bump version to 0.2.2 

### Testing

- Bump expected __version__ to 0.2.2 ahead of release 
## [0.2.1] - 2026-04-10

### Bug Fixes

- Resolve mypy errors in e2e and integration tests (#73) 

### Documentation

- Update changelog 
- Add BYOD example apps (Oracle DB) (#76) 
- Clarify that any SQLAlchemy-compatible database works (#75) 
- Standardize ecosystem table in README 

### Miscellaneous Tasks

- Add stale.yml and maintenance.yml workflows (#78) 
- *(deps)* Bump codecov/codecov-action from 5.5.3 to 6.0.0 (#1) 
- *(deps)* Bump softprops/action-gh-release from 2.2.2 to 2.6.1 (#53) 
- Add DESIGN.md, PRD.md, i18n READMEs and bump tool versions (#71) 
- Skip e2e-azure gracefully when OIDC secrets are missing 
- Add lightweight smoke test step to publish workflow 

### Other

- Bump version to 0.2.1 

### Refactor

- Rename metadata attr to _azure_functions_metadata (#80) 
## [0.2.0] - 2026-04-09

### Bug Fixes

- Install mssql-tools18 for sqlcmd in CI MSSQL job 
- Resolve CI failures for pytest 9.x fixture marks and GPG batch mode 
- Run Azurite as step with --skipApiVersionCheck instead of service container 

### Features

- Add three-layer test architecture for all supported databases 
- Align with sibling repo conventions 

### Other

- Bump version to 0.2.0 

### Performance

- Add thread-safe metadata cache to avoid per-invocation table reflection 
## [0.1.0] - 2026-04-08

### Bug Fixes

- *(docs)* Replace relative parent links with GitHub URLs for strict mkdocs (#29) 
- Add kw_only=True to RetryPolicy and create mkdocs.yml for docs workflow (#28) 

### Documentation

- Update changelog 
- Align documentation structure with sibling repos 
- Update branding, reorder README, add lifecycle/thread-safety docs (#48) 

### Features

- Partial env var substitution, engine_kwargs passthrough (#49) 
- Add OutputResult, async docs, decorator composition validation (#47) 
- Pydantic model mapping, async proxy, cursor index warning (#32) 
- *(decorator)* Redesign db_input/db_output to data injection bindings (#31) 
- *(decorator)* Redesign API to DbFunctionApp with db_trigger, db_input, db_output (#30) 
- *(binding)* Trigger+binding integration tests and example (Phase 11) 
- *(binding)* Implement DbWriter output binding (Phase 10) (#26) 
- *(binding)* Implement DbReader input binding (Phase 9) (#25) 
- *(core)* Phase 8 — extract shared core (DbConfig, EngineProvider, serializers, types) (#24) 
- *(hardening)* Phase 7 — crash recovery tests, benchmark, and duplicate docs (#23) 
- *(observability)* Add MetricsCollector, structured logging, and lag calculation (#21) 
- *(trigger)* Add PollTrigger, db.poll() decorator, and normalizers (#20) 
- *(adapter)* Add SqlAlchemySource with cursor-based polling (#19) 
- *(state)* Add BlobCheckpointStore with ETag-based CAS leasing (#18) 
- Phase 1 - Core types, errors, trigger events, context, retry, and runner (#17) 
- Initial project scaffold — unified DB integration framework for Azure Functions Python v2 

### Miscellaneous Tasks

- *(release)* Phase 6 release prep (#22) 
- Translate docs to English and align README with series style (#16) 

### Refactor

- Rename DbFunctionApp to DbBindings, drop db_ method prefix, narrow exports (#46) 
<!-- generated by git-cliff -->
