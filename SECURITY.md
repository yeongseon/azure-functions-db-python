# Security Policy

## Reporting a Vulnerability

We take security seriously. If you discover a vulnerability, please report it
responsibly and avoid public disclosure until a fix is available.

### Preferred: GitHub Security Advisory

1. Go to the [Security Advisories page](https://github.com/yeongseon/azure-functions-db-python/security/advisories/new)
2. Click "Report a vulnerability"
3. Submit the details in the private advisory

### Alternative: Email

If you prefer email, contact: yeongseon.choe@gmail.com

### What to Include

* A clear description of the issue
* Steps to reproduce
* Impact assessment
* Suggested mitigation or fix, if available

### Response Timeline

* Initial response: within 48 hours
* Status update: within 7 days

## Supported Versions

| Version | Supported |
| ------- | --------- |
| Latest release | :white_check_mark: |
| Older releases | :x: |

## Security checks and evidence

| Where | Blocking scope | Evidence |
| --- | --- | --- |
| PR/main `security.yml` | Existing Bandit scan plus complete executable Python inventory (`src/`, `examples/`, `tools/`, `benchmarks/`); pinned Semgrep `p/python` action | Redacted JSON and job summary, uploaded with `always()` |
| PR/main tests | Offline SQL/credential canaries, Bicep/firewall and shell/workflow invariants | Test outcomes; no paid service required |
| Release `runtime-dependency-audit` | Exact wheel downloaded from `build`, isolated base and each `postgres`, `mysql`, `mssql`, `all` runtime closure on Python 3.11–3.14 | Per-matrix redacted JSON and summary, including failure uploads |
| Scheduled/manual `dependency-audit.yml` | Reporting-only latest runtime (`all`) and development closures | Same helper and redacted evidence; findings remain visible, not release authorization |
| Existing CodeQL/SBOM workflows | Unchanged code analysis and SPDX supply-chain inventory | Existing workflow evidence |

There is no `scripts/` directory today. Add it to the inventory if executable
scripts are introduced. Benchmarks are included conservatively. Bandit does not
parse Bicep or shell: `tests/test_security_infrastructure.py` and
`tests/test_security_workflows.py` cover those configuration boundaries.
Zero analyzed files, inventory drift, malformed/missing outputs and tool errors
cannot pass. Existing inline `# nosec` entries are not removed: scans use
`--ignore-nosec` and compare every marker against an exact file/line allow-list.
Unlisted findings at any severity block, including LOW.

`PASS` (exit 0), `FAIL` (exit 1: policy finding/expired exception), `TOOL_ERROR`
(exit 2: unusable scanner/policy evidence), and `INCOMPLETE` (exit 3: skipped or
missing scope) are distinct. All non-PASS results block a required policy gate.
Semgrep evidence without structured output is INCOMPLETE, explicitly "action outcome only",
not a fabricated clean scan. Only this evidence exit 3 is tolerated by its Makefile
entry point; the pinned Semgrep action remains the gate and failure stays failure.

The release policy blocks **all known runtime advisories**, including UNKNOWN
severity: pip-audit does not consistently supply severity, so a severity-only
threshold would fail open. The audited wheel environments contain only the
candidate and resolved runtime dependencies, not pip-audit/dev/docs tooling.
`all` is audited independently as combined resolution can differ from individual
driver closures. BYOD dependencies installed outside these extras remain the
consumer's audit responsibility. Wheel METADATA name/version must match the installed
candidate exactly before excluding only that candidate. Each isolated closure is frozen
to exact third-party name/version requirements and audited with no dependency resolution
or pip execution. Third-party skips, empty inventory, missing scan entries and identity
mismatches remain blocking. Required DB/Azurite lanes must treat missing-environment skips as
non-success; the strict lane runner owns this rule, not a duplicate runner here.

Use `make security` for local full security validation (also included in `make check-all`).
`make security-inventory`, `make security-bandit`, and `make security-typecheck` expose
the expanded scanner and strict tool checks. Set `SECURITY_PYTHON` to an existing tool
interpreter; defaults to the existing Hatch interpreter without synchronizing it. `make security-semgrep` records
`SCAN_OUTCOME`. `make security-audit SECURITY_ARGS="--site-packages PATH --candidate-wheel WHEEL"`
enforces release policy; `make security-audit-report` uses the same fail-closed policy in
scheduled reporting-only jobs. `make security-host` is the opt-in host test entry point.

`publish` requires the audit matrix in addition to all existing verification and
real-Azure certification gates. PyPI OIDC and exact-artifact reuse are unchanged.
For transient tool/advisory-service errors, retry the unchanged existing tag:
`gh workflow run publish-pypi.yml --ref vX.Y.Z`. Code/policy fixes require a new
release candidate; never move a tag or bypass a failed gate.

### Exceptions

Review `security/dependency-audit-policy.json` or `security/bandit-policy.json`
through the normal issue/PR workflow. Each exception needs an exact advisory ID
and normalized package (dependency) or rule/file/line (static), a specific
reason, owner, added date and expiry **at most 90 days** later. Missing fields,
duplicate rules, future added dates and invalid windows fail closed. Exceptions
expire; stale entries are counted for removal. `fix-not-available` stops applying
as soon as a fix is reported; `not-affected` needs documented reachability
evidence. No blanket IDs, permanent ignores, or suppression of tool failures.

### Redaction boundary

Security helpers retain IDs, relative paths, line numbers, severity/confidence,
counts, tool versions and exception IDs only. Never export source snippets,
scanner prose, payloads, connection strings, credentials or auth headers. Raw
scanner stdout/stderr is captured in memory and discarded, not uploaded. Invalid
metadata produces a fixed error message. Synthetic credential tests guard this
boundary. This is a schema-based evidence guarantee, not a general-purpose
secret detector for arbitrary strings in trusted source filenames or rule IDs.
Do not upload raw diagnostic files alongside the allow-listed reports.

## Raw SQL and production permissions

`DbReader.query()` and `scalar()` accept arbitrary SQL and do **not** validate it
as read-only. Bind untrusted values with `:name` placeholders and `params`;
parameters cannot bind identifiers. Choose table/column names from an allow-list
and use SQLAlchemy quoting/expression objects. Use a dedicated least-privilege
read-only DB role for readers and a separate narrowly scoped writer role.
See [client injection guidance](docs/examples/client_injection.md#raw-sql-trust-boundary).

Library error messages do not echo driver diagnostics, but chained driver
exceptions and third-party SQLAlchemy echo/DEBUG logs can contain sensitive data.
Do not expose full tracebacks to clients or artifacts; keep `echo=False`, redact
application diagnostics, require TLS, and keep secrets out of committed settings.
Local demos (including anonymous HTTP routes and local database credentials)
are not secure production defaults: authenticate/authorize handlers, limit
network access and DB privileges, and manage credentials/rotation separately.

## E2E network and credential lifecycle

The Bicep broad Azure-services PostgreSQL rule defaults to **off**. Certification
explicitly opts in for its uniquely named, ephemeral run/attempt resource group;
`0.0.0.0` allows more Azure-service connection attempts than just the test app.
TLS and credentials are still required; this is an exposure-minimization concern,
not a confirmed vulnerability. A new random password is generated each run and
masked before output; the full URL is masked before configuring the app.
A separate `always()` cleanup job requests RG deletion even after failure;
deletion is asynchronous and orphaned resources require operator monitoring.

Storage account-key connection strings remain a **test-only fallback**. No
settings/key dumps are uploaded. Keep E2E reports free of credentials and driver
tracebacks: GitHub masking does not sanitize artifact contents. Safer profiles
use VNet integration plus PostgreSQL private access/private endpoints and Entra
ID identities. Identity-based host/deployment storage, private networking and
explicit app/SCM minimum TLS need real-Azure validation before adoption; see
[infrastructure security profile](infra/README.md). These are not claimed verified
by offline template compilation or text invariants.
