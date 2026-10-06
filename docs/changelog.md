# Changelog

This page documents the versioning scheme and migration paths for the
`azure-functions-db-python` package.

## Where the release history lives

[Release Please](https://github.com/googleapis/release-please) generates the
root [`CHANGELOG.md`](https://github.com/yeongseon/azure-functions-db-python/blob/main/CHANGELOG.md)
from Conventional Commits, and that file is the authoritative, complete history
from `v0.1.0` onward. Per-release notes are also published on the
[GitHub Releases](https://github.com/yeongseon/azure-functions-db-python/releases)
page. See [Release process](release_process.md) for how a release is cut.

This page is hand-maintained and holds only the versioning scheme and migration
guidance. It deliberately does not mirror the generated history.

## Versioning Scheme

This project follows Semantic Versioning (semver.org). Given a version number MAJOR.MINOR.PATCH, increment the:

- MAJOR version when you make incompatible API changes
- MINOR version when you add functionality in a backward compatible manner
- PATCH version when you make backward compatible bug fixes

Breaking changes are listed under the "Breaking Changes" heading of the release
that carries them in `CHANGELOG.md`.

## Migration Guides

### Migrating from v0.1.0 to v0.2.0

The v0.2.0 release renamed the public surface. Update call sites as follows:

- `DbFunctionApp` is now `DbBindings` (#33).
- Decorators lost their `db_` prefix: use `trigger`, `input`, `output`,
  `inject_reader`, and `inject_writer` (#41, #45).
- `OutputResult` is replaced by the `DbOut` class, which writes via `.set()`
  instead of returning a result object (#50).
- The public export list was narrowed; import only documented symbols from the
  package root (#34).

### Configuring engine options

Binding decorators accept `engine_provider` only. Driver-level engine options
belong on a `DbConfig` (`engine_kwargs`, `connect_args`), which an
`EngineProvider` forwards to `sqlalchemy.create_engine()`. See
[Engine provider and pooling](25-engine-provider-pooling.md).

### Python version support

The package requires Python `>=3.11,<3.15`.
