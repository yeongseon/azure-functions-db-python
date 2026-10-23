# Contributing Guide

We welcome contributions to the `azure-functions-db-python` project.

## Branch Strategy

Use GitHub Flow and branch from `main`.

Recommended branch prefixes:

- `feat/` for new features
- `fix/` for bug fixes
- `docs/` for documentation-only changes
- `chore/` for tooling and maintenance
- `ci/` for workflow updates

## Pull Request Review and Merge Policy

Apply identical PR review criteria to human-only and AI-assisted work
(OpenCode, Codex, Claude Code, ChatGPT, Copilot, and others).
Implementation tool choice never adds a mandatory AI review route.

- Open an issue-linked, focused PR with relevant tests, docs and a
  truthful account of checks performed or omitted.
- Trust required **GitHub CI** checks at the final head for official
  automated validation. Running `make check-all` locally is recommended,
  not a separate merge condition; do not assume a specific runner model.
- Review correctness, tests, security, maintainability and compatibility.
  AI review outputs are advisory inputs, not approvals or CI substitutes.
- Resolve actionable findings; the responsible maintainer decides on
  merge after checking required CI and existing runtime, security,
  release and integration safeguards. Permission/publishing changes
  receive explicit independent scrutiny.

## Development Workflow

1. Create a branch from `main`.
   ```bash
   git checkout main
   git pull origin main
   git checkout -b feat/your-feature-name
   ```
2. Write code and tests.
3. Run the local quality gate when practical; required GitHub CI is authoritative.
   ```bash
   make check-all
   ```
4. Push and create a pull request.
   ```bash
   git push origin feat/your-feature-name
   ```

## Local development quickstart

The package supports Python 3.11 through 3.14. Install Python 3.11 for the
Makefile workflow because the default Hatch environment runs on Python 3.11.
Offline quality checks and unit tests need no services. Full local verification
requires Docker or Podman with a reachable daemon, Azurite, Functions Core Tools,
the database driver extras and ODBC Driver 18 for SQL Server.

Install Hatch once in an isolated tool environment. From a fresh clone, create
the project-managed Hatch environment with:

```bash
make install
```

Then run the standard verification commands:

```bash
make test
make check
make verify
```

Routine commands never upgrade pip or reinstall Hatch. `make install-ci` installs
into CI's selected interpreter, never Hatch's Python 3.11 default. `make doctor`
checks prerequisites without installing anything; missing required tools exit nonzero.

## Project Commands

| Command | Contract |
| --- | --- |
| `make install` | Idempotent Hatch dev/docs/database-driver setup and local hooks |
| `make install-ci` | Install extras into the explicitly selected `PYTHON` |
| `make doctor` | Read-only prerequisites; nonzero for missing required components |
| `make format` / `make format-check` | Mutating Ruff format / non-mutating check |
| `make check` | Ruff lint/format, strict mypy once, Bandit and all workflow policy guards |
| `make test` | Offline SQLite/mock/unit suite, coverage >=95%, no live auto-skips |
| `make integration` | Run-owned SQLite, PostgreSQL 16, MySQL 8, SQL Server 2022 and private Azurite |
| `make integration-local` | Strict existing-URL tests; explicit dedicated-test-database confirmation |
| `make verify` | Always check + test + every backend + real host + installed wheel + build + strict docs |
| `make e2e-azure` | Existing deployed candidate only; explicit URL and expected version, no provisioning |
| `make report` | Render saved evidence only; no tests or services |
| `make clean` | Generated artifacts only; preserve environments and external resources |

`make check-all` is an exact backward-compatible alias of `make verify`.
`make clean-all` retains historical deep cleanup, including bytecode and `.venv`.
Run `make clean` from the real checkout directory. Cleanup refuses a repository
root that is itself a symlink before any deletion; symlinked ancestors are supported.
`lint`, `style`, `typecheck`, `security`, `cov`, `lint-workflows`, `build`, `docs`,
`host-smoke`, and `wheel-smoke` are focused/partial commands, not full verification.
`make check` has no Docker, browser, host or emulator prerequisite.

### Integration ownership and strict gates

`DB_BACKEND=sqlite make integration` selects one backend and records **PARTIAL**
evidence. Supported values: `sqlite`, `postgres`, `mysql`, `mssql`, `azurite`.
`make verify` explicitly clears that focus and requires every backend; an absent
container daemon or driver is INCOMPLETE/nonzero, never silently omitted.
Owned runs use random service names, loopback ports, temporary SQLite/Azurite
storage and bounded connection readiness. Cleanup removes only the exact containers
and process groups created by this run, including interruption and timeout paths.

For externally managed services, provide `TEST_POSTGRES_URL`, `TEST_MYSQL_URL`,
`TEST_MSSQL_URL`, `TEST_SQLITE_URL`, or `TEST_AZURITE_CONN_STR` for the selected lane:

```bash
DB_BACKEND=postgres DB_TEST_DATABASE_CONFIRMED=1 make integration-local
```

Use a **dedicated disposable test database**, never production or shared tables.
Existing fixtures create/seed/drop test tables (including `users`, `orders`,
`order_items`, and `typed_cursor_events`). Neither the runner nor cleanup ever
drops/deletes a user database or stops/removes user-managed containers. Azurite tests
delete only uniquely named test containers they created. Confirmation protects
against accidentally pointing these table-mutating tests at a user database.

Casual ad-hoc pytest discovery retains automatic missing-URL skips. Required lanes
instead preflight the actual connection, select only the intended `db_key`/marker,
and classify JUnit with executed >0 and **zero unexpected skips**. No broader
selector fallback exists. Azurite checkpoint-store tests are required in every CI
backend cell; there are no optional DB/Azurite cells in full local verification.

### Evidence and certification boundaries

```text
test-results/
  test/{junit.xml,coverage.xml,summary.json,environment.json,logs/}
  integration/{summary.json,environment.json,lanes.json,junit/,logs/}
  integration/<backend>/{junit.xml,summary.json,environment.json,logs/}
  host-smoke/{summary.json,environment.json,junit.xml,logs/}
  wheel-smoke/{summary.json,environment.json,logs/}
  verify/{summary.json,environment.json,lanes.json,logs/}
  azure-e2e/{junit.xml,summary.json,environment.json,logs/}
```

Each summary contains `schema_version`, command/lane, required status,
selected/passed/failed/skipped counts, scope, exit code and supporting artifact
paths. Backend summaries additionally record service version, driver, readiness and
cleanup. Environment metadata records actual Python/SDK/driver versions and source
commit, never an environment dump. Connection URLs, credentials, AccountKey, SAS,
`sig` and tokens are redacted in decoded XML text/attributes and sensitive JSON fields,
logs, console and Job Summary. HTML reports are retained as escaped static views of
sanitized report content (including pytest-html's embedded JSON); interactive scripts
and styling are discarded. Text evidence includes `.xml`, `.html`, `.json`, `.log`,
`.txt`, `.md` and `.out`; binary assets and symlinks are not rewritten.
Free-text redaction inspects plain, percent/form, HTML-entity and backslash-escaped
JSON/Python views through three decoding levels, mapping only secret value spans
back to the original representation. Azure-style values assigned to `code`, `key`,
`x-functions-key`, `master`, `default`, `function`, `host`, `secret` or `token` are
masked when they contain 40–64 URL-safe base64 characters with optional padding.
This is deliberately broader than one generated-key format: Functions supports
[custom keys and signed/checksummed generated keys](https://learn.microsoft.com/azure/azure-functions/function-keys-how-to#access-key-requirements).
Unlabelled hashes, UUIDs, paths and long identifiers are not treated as secrets;
hashes explicitly assigned to these credential labels may be masked conservatively.
Short status/error/percentage `code` values remain unchanged outside `?code=`/`&code=`
queries. Known environment secrets and explicit password/header/query credentials
remain masked irrespective of the Azure-style shape. Unknown unlabelled secrets,
custom short ambiguous keys and encodings deeper than three levels are not claimed
covered; avoid logging secrets even when evidence redaction is enabled.
ODBC/ADO `PWD` is masked case-insensitively in JSON and textual credentials,
including braced passwords containing semicolons and encoded `odbc_connect` DSNs.
Bare textual `pwd` values that are short numbers/percentages or start with a Unix
or Windows path are preserved as shell/status evidence; structured `PWD` fields
are always sensitive. Already-masked passwords (`***`, `****`, `<redacted>`,
`[REDACTED]`, `xxxxx`) are left byte-identical, including SQLAlchemy URL userinfo.
CI uploads available `test-results/**` with `if: always()` even on failure.

- **PASS**: successful executed checks; only `verify` carries FULL_LOCAL scope.
- **FAIL**: failing tests/processes, collection errors, readiness failure or timeout.
- **INCOMPLETE**: missing tools/URLs/results, zero tests, all skipped or any unexpected
  required skip; always nonzero.
- **NOT_APPLICABLE**: only explicitly unsupported capabilities, never absent services.

Database integration, real Functions-host execution, wheel-install SQLite smoke and
real Azure/PostgreSQL certification are separate evidence lanes. Local success is
not Azure certification. `make e2e-azure` requires `E2E_BASE_URL` and
`EXPECTED_VERSION`; it verifies the deployed identity before live tests. It never
provisions Azure. Release certification retains its existing exact-candidate wheel,
source/ref/version record, OIDC and explicit infrastructure cleanup workflow.

Before opening a PR, run `ruff format --check <changed-python-files>` (or
`ruff format <changed-python-files>` to fix them). `make format-check` checks
`src`, `tests`, `tools`, `examples` and `benchmarks` without changing files. CI checks those trees and also
reports formatting failures on changed Python files in a dedicated step.

## GitHub Actions Pinning

All external `uses:` references in `.github/workflows/` MUST pin to a
full 40-character commit SHA with a trailing comment documenting the
released version or channel. Example:

```yaml
- uses: actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd # v6
```

This applies to first-party (`actions/*`, `github/*`, `azure/*`) and
third-party Actions alike.

**Rationale.** Mutable tags (including immutable-looking version tags
like `@v6.0.1`) can be retroactively moved by an attacker who gains
write access to the upstream repository. The
[`tj-actions/changed-files` compromise (CVE-2025-30066, March 2025)](https://www.cve.org/CVERecord?id=CVE-2025-30066)
demonstrated this exact failure mode: ~23,000 repositories had their CI
secrets exfiltrated, and only workflows that pinned to a commit SHA were
safe. GitHub's own guidance now identifies SHA pinning as
[the only way to use an Action as an immutable release](https://docs.github.com/en/actions/security-for-github-actions/security-guides/security-hardening-for-github-actions#using-third-party-actions),
and OpenSSF Scorecard's
[`Pinned-Dependencies` check](https://github.com/ossf/scorecard/blob/main/docs/checks.md#pinned-dependencies)
flags anything weaker as Medium-risk.

Dependabot updates SHA-pinned references on the configured schedule and
keeps the trailing version comment in sync, so the human-readable
context never drifts from the pinned commit.

**Approved exceptions.** The following mutable refs are the only ones
permitted in this repository and are flagged with an inline comment at
the call site:

- `pypa/gh-action-pypi-publish@release/v1` — PyPA-maintained stable
  release channel; this pinning style is explicitly recommended upstream.
- Local composite actions (`uses: ./...`) — versioned with the repo.

When adding a new external Action, resolve the SHA with
`git ls-remote <repo-url> 'refs/tags/<tag>^{}'`. The trailing `^{}`
**dereferences** the ref to its target commit; without it, an
*annotated* tag returns the tag-object SHA instead of the commit SHA,
and the tag-object SHA is **not** a valid `uses:` target.

```bash
# Annotated tag — the two forms return *different* SHAs:
git ls-remote https://github.com/Azure/login 'refs/tags/v3.0.0' 'refs/tags/v3.0.0^{}'
# 93381592...   refs/tags/v3.0.0       <- tag object, do NOT pin to this
# 532459ea...   refs/tags/v3.0.0^{}    <- commit, pin to this one

# Lightweight tag — only the non-deref form returns a row, and it is
# already the commit SHA. Always include the `^{}` form anyway so the
# same command works for both tag types:
git ls-remote https://github.com/actions/setup-python 'refs/tags/v6' 'refs/tags/v6^{}'
```

Single-quote the `refs/...^{}` argument so the `{}` and `^` are not
interpreted by your shell (notably zsh with `EXTENDED_GLOB`).

## Example Coverage Policy

Examples are part of the supported API experience and should stay verified.

- Keep one representative example for the minimal DB trigger polling workflow.
- Keep one complex example for multi-table and SQLAlchemy-heavy behavior.
- Add or update smoke tests whenever an example changes.
- Prefer lightweight smoke coverage over infrastructure-heavy end-to-end tests.

## Issue Scope

Follow the [repository-scope rules in AGENTS.md](AGENTS.md#repository-scope)
when opening or editing an issue, including the checks before and after saving.
Explain this repository's problem, evidence, user impact, and completion criteria,
and state why any external project or source is relevant.

## Title Convention

This section is the single source of truth for issue, pull request, and commit titles in this repository. `AGENTS.md`, the pull request template, the issue forms, and the documentation site point here instead of restating the rules.

Issues, pull requests, and the final squash commit on the default branch all use:

```text
type: description
type(scope): description
```

- Write titles in English.
- `type` and `scope` are lowercase. Put exactly one space after the colon.
- `scope` is optional. Add it only when it narrows the area; do not add one just for uniformity.
- `description` states what changes and why, concisely. Start with a lowercase word, but keep the spelling of API names, acronyms, error codes, and proper nouns.
- No trailing period.
- No priority, size, status, or owner markers, and no bracket prefixes such as `[Bug]`, `[Feature]`, or `[WIP]`. Use labels and Draft pull requests instead.
- An issue title may describe the problem; the pull request title describes the change that resolves it. Keep them aligned when they cover the same work, without forcing identical wording when the scope differs.
- Do not put issue numbers in pull request titles. Link issues in the pull request body with `Closes #123` or `Refs #123`.

| type | use for |
|---|---|
| `feat` | a user-facing feature or capability |
| `fix` | a bug fix in product behavior |
| `docs` | documentation only |
| `test` | adding or changing tests |
| `perf` | a performance improvement |
| `refactor` | internal restructuring that keeps external behavior |
| `ci` | CI and automation workflows |
| `build` | build, packaging, and dependency changes |
| `chore` | maintenance that fits none of the above |
| `style` | formatting only, no behavior change |
| `revert` | reverting an earlier change |

A breaking change adds `!` before the colon: `type!: description` or `type(scope)!: description`. Whether a change is breaking, and how it is released, is decided by the Release Process in [AGENTS.md](AGENTS.md), not by the title alone. Release Please reads these titles from the squash commits on `main`: `fix` and `feat` drive the version bump, and the other types only shape the changelog.

Examples:

```text
fix: preserve the first scalar column
feat(reader): add an optional row limit
docs(readme): clarify the local setup steps
refactor!: rename the public configuration keys
```

Not accepted: `[Bug] crash on save` (bracket prefix, no type), `Fix: crash on save` (uppercase type), `feature: add export` (type not in the table), `fix: crash on save.` (trailing period), `fix: preserve the first scalar column (#503)` (the pull request number belongs in the body).

Pull requests are squash-merged and the pull request title becomes the final commit title. GitHub appends the pull request number by itself, so `fix: preserve the first scalar column` lands on `main` as `fix: preserve the first scalar column (#503)`. Do not write that number into the title yourself — the check rejects a title ending in `(#123)`, because it would be doubled. Link the issue from the pull request body instead, for example `Closes #503`. The **PR title** check validates the format whenever a pull request is opened, edited, reopened, or updated. Individual commits on a branch are not checked.

Issue titles are not enforced. Anyone can open an issue without knowing this convention; maintainers adjust the title during triage.

## Releases

Merging to `main` does not publish a release.

Release Please keeps an open **Release PR** showing what the next version would
be; merging that PR is what cuts the release, and it is what creates the `v*`
tag that starts the publish workflow. Publishing happens only after the release
verification requirements have been satisfied. Contributors never need to bump a
version or tag anything in an ordinary pull request.


See `AGENTS.md` for the maintainer-only release procedure.

## Code of Conduct

Be respectful and inclusive. See our [Code of Conduct](CODE_OF_CONDUCT.md) for details.
