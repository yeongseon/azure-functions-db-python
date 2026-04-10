# Agent Playbook

## Source Of Truth
- `AGENT.md` for repository-wide engineering and agent rules.
- `README.md` for installation, quick start, and CLI examples.
- `CONTRIBUTING.md` for branch, commit, and release workflow.
- `pyproject.toml` and `Makefile` for supported commands.

## Repository Map
- `src/azure_functions_db/` package code.
- `tests/` database binding and trigger coverage.
- `examples/` example database apps.
- `docs/` documentation site content.

## Change Workflow
1. Confirm whether the change affects database binding API, trigger logic, connection handling, or docs only.
2. Update examples when they are used as public contract material.
3. Keep tests and docs in lockstep for public behavior changes.
4. Do not broaden supported Python versions or dependency ranges casually.

## Validation
- `make test`
- `make lint`
- `make typecheck`
- `make security`
- `make build`
