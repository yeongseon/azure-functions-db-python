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

## Release gate: known advisories in the runtime dependencies

The `runtime-dependency-audit` job in `publish-pypi.yml` blocks `publish` on `v*` tags (and on a manual dispatch against a tag); pull requests do not run it. On each of Python 3.11-3.14 it installs the downloaded candidate wheel (the exact artifact `publish` uploads) with the `all` extra, the union of the `postgres`, `mysql` and `mssql` extras, into a clean virtual environment, runs `pip freeze --exclude azure-functions-db`, and audits that list with `pip-audit==2.10.1 --no-deps --disable-pip --strict`. A known advisory, an empty list or a package `pip-audit` cannot audit fails the job. `pip-audit` reports no severity, so every advisory blocks. The audit queries PyPI: if it is unreachable the release stays blocked until a retry passes (`gh workflow run publish-pypi.yml --ref vX.Y.Z`). Each cell uploads its JSON report (`runtime-audit-*`, 30 days). The weekly `dependency-audit.yml` stays reporting-only.

An advisory that cannot be fixed yet may be waived for at most 90 days in `security/runtime-advisory-exceptions.txt`, one line each:

```text
ADVISORY-ID owner added expires reason...
```

`added` and `expires` are ISO dates; `expires` is the last day the waiver applies. A waiver that is expired, not yet added, malformed or longer than 90 days fails the gate (`tools/runtime_audit_args.py`). The gate reads the file from the tagged commit, so a waiver must be on `main` before the Release PR is merged. Pull-request CI does not run the gate, so check an edit with `python tools/runtime_audit_args.py security/runtime-advisory-exceptions.txt`.

## Static analysis gate: Bandit

`make security` runs Bandit once over `src`, `examples`, `tools` and `benchmarks` with every Bandit test enabled and no skip list. Not scanned: `tests/` and the `.venv` that the example READMEs create inside `examples/<name>/` (`-x 'examples/*/.venv/*'`, given to Bandit and to the guard alike). Everything else is, including a package named `env` or `venv` and a `.venv` anywhere but under `examples/<name>/`. The recipe also gives Bandit an explicit empty `--ini`, so a `.bandit` file anywhere in the scope is never read and can neither skip tests nor exclude paths. The required `bandit` check (`security.yml`), the `quality` job of `ci-test.yml` and the weekly `maintenance.yml` run it. Bandit exits 0 for an empty directory, a missing path and a file it cannot parse, so the recipe writes Bandit's JSON report to `test-results/bandit.json` (deleted first, so a report left by an earlier run never counts) and `tools/check_bandit_report.py` fails the target when no code was analysed, a file was skipped, a Python file in scope was left unscanned, any finding remains or a nosec comment names no Bandit test ID. `security.yml` uploads the report as `bandit-report-<run attempt>`, also when the check fails.

Suppress a reviewed finding only inline, on the line Bandit reports, as `# nosec Bxxx # <reason>`. A blanket `# nosec` (no ID, words instead of an ID, or an ID Bandit does not know) fails the gate, because Bandit would suppress every test on the line. Separate several test IDs with spaces, not commas (Bandit 1.9.4 honours only the last ID of a comma list), and put the reason behind the second `#` (Bandit warns about every other word after the IDs). There is no skip list, allow-list or baseline file. The pre-commit hook covers the same four directories, on the changed files only and without the guard, so `make security` remains the gate.
