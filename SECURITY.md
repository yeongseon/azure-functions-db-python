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

## Dependency Advisories

Publishing is blocked if `make audit-wheel` finds a known advisory in the third-party runtime
dependencies of the candidate wheel (Python 3.11-3.14, `all` extra). The candidate itself is
excluded. To excuse an advisory, add `<ID> <owner> <YYYY-MM-DD expiry> <reason>` to
`security/pip-audit-ignore.txt`; a malformed or expired entry fails the audit. The weekly
`dependency-audit.yml` runs the same target as non-blocking reporting.
