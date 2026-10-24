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
