# Release History and Provenance

This page records how each published version of **azure-functions-db** maps to a Git tag, a
GitHub Release and a PyPI distribution, and lists the historical cases where those records do not
line up. The data was reconciled against Git, PyPI and GitHub on 2026-11-10. For how new versions
are released, see [Release Process](release_process.md).

---

## What each record proves

| Record | Proves | Does not prove |
|---|---|---|
| PyPI distribution | The package users install exists, with its files and upload time | That the wheel contains the package (4 of the 24 do not), or which commit built it (only PyPI attestations say so; all 24 versions have one) |
| Git tag | A commit was labelled with a version | That the tagged source is what shipped, or that anything was published |
| GitHub Release | A release page with notes exists | That a package was published |

A GitHub Release is never evidence of publication. Check the
[PyPI release history](https://pypi.org/project/azure-functions-db/#history), and open the wheel
before calling a version usable.

---

## Reconciliation summary

| Evidence set | Count |
|---|---:|
| Git tags | 25 |
| PyPI versions (`0.1.0` to `0.9.0`, without `0.5.2`) | 24 |
| GitHub Releases | 25 |

| Category | Versions |
|---|---|
| Published, with a matching tag and GitHub Release; wheel and source distribution match the tag | 20 versions: every published version except the four below |
| Published, but the wheel contains no package modules | `0.2.2`, `0.3.0`, `0.4.0`, `0.4.1` |
| Unpublished release candidate: tag and GitHub Release, never on PyPI | `0.5.2` |

For every published version the source distribution is byte-identical to the tagged commit, and
the PyPI attestation records a build from that tag at that commit (see
[How this was verified](#how-this-was-verified)).

---

## v0.5.2: unpublished, superseded by v0.5.3

The failure and its resolution are recorded in issue
[#257](https://github.com/yeongseon/azure-functions-db-python/issues/257).

| Time (UTC) | Event |
|---|---|
| 2026-08-14 06:10:37 | tag `v0.5.2` (commit `7dbc0d2`, which declares `0.5.2`) created |
| 06:10:43 | three workflows start: [`Publish to PyPI` run 31775404558](https://github.com/yeongseon/azure-functions-db-python/actions/runs/31775404558), `e2e-azure` run 31775404529 and `Create GitHub Release` run 31775404542 |
| 06:10:55 | Release published by the `Create GitHub Release` workflow |
| 06:11:07 | the job *Verify Azure release certification* fails |
| 06:11:49 | the publish run ends as failed; `publish` was skipped |
| 2026-09-11 10:26:37 | tag `v0.5.3` (commit `484d61e`, which includes the fix) created |
| 10:28:24 | `0.5.3` uploaded to PyPI |

**Facts.** In the run, *Build & verify version*, *Library tests* and the package runtime gate
succeeded. The job *Verify Azure release certification*, which requires a certification record for
the exact release commit, failed. It first selected a successful `e2e-azure` run for the commit
(run 31775404529) and then failed at *Download certification record*:
`Artifact not found for name: azure-cert`. That `e2e-azure` run had concluded successfully only
because its *Deploy & Test* job was skipped: the repository had no Azure OIDC secrets
(`Azure OIDC secrets not configured — skipping e2e tests`). The three other runs on this commit
(31774766234, 32004418184 and 32700352032) also show *Deploy & Test* skipped.

The `e2e-azure` workflow at this tag also had no step that writes an `azure-cert` artifact; that
step came with #294 on 2026-09-11. The certification gate (#240) had been added the day before the
tag, so with the workflows as they stood at the tag it could not pass whatever the Azure
configuration was. Issue #257 adds that the commit predated fixes for the real-Azure deploy path
(#295, #296), so certifying this tag was not possible and `0.5.3` was cut from `main` instead. The
cause was in the release workflows and the missing credentials, not in the package.

`v0.5.2` is an ancestor of `v0.5.3`, with 23 commits in between, so every change in `0.5.2` shipped
in `0.5.3`. No `0.5.2` distribution was ever published: PyPI has no such version (its per-version
JSON API returns 404), TestPyPI has no project of this name, the `publish` job (the only job that
uploads to PyPI) was skipped, and the Release has no assets. README and docs do not claim `0.5.2`
is installable. `CHANGELOG.md` has a `0.5.2` section; it is not edited.

**Decision**

- The tag and the Release are kept; nothing is moved, deleted or re-uploaded. The Release body now
  opens with a notice that `0.5.2` was not published and points to `0.5.3`; the notice is a
  protected statement in `.github/release-notes.json`.
- **Retry criteria.** Re-running a tag is only for a transient or environmental failure. A
  `workflow_dispatch` runs the workflow file at the tag, so it cannot pick up a correction made on
  `main`. At this tag the missing `azure-cert` step could not be added by a re-run, so fixing
  forward, which is what shipped `0.5.3`, was the only path. Never move a tag or upload an altered
  artifact under a consumed version.

---

## Empty wheels: v0.2.2, v0.3.0, v0.4.0 and v0.4.1

Tracked in issue [#532](https://github.com/yeongseon/azure-functions-db-python/issues/532). The
source distributions of these four versions are complete and identical to their tagged commits, but
their wheels on PyPI contain only the `*.dist-info` files:

| Version | PyPI upload (UTC) | Wheel on PyPI | Source distribution |
|---|---|---|---|
| `0.2.1` | 2026-04-10 | 31 entries, 26 package modules | complete |
| `0.2.2` | 2026-04-26 | 4 entries, 0 package modules | complete |
| `0.3.0` | 2026-04-26 | 4 entries, 0 package modules | complete |
| `0.4.0` | 2026-04-29 | 4 entries, 0 package modules | complete |
| `0.4.1` | 2026-04-30 | 4 entries, 0 package modules | complete |
| `0.4.2` | 2026-04-30 | 31 entries, 26 package modules | complete |

Installing one of the four with `pip install azure-functions-db==0.4.0` succeeds and installs no
module, so `import azure_functions_db` fails. This was checked with `pip` into a throwaway
directory for all four, with `0.4.2` as the control (26 package files, importable). All four tags
are on `main` and are ancestors of `v0.4.2`.

**Cause, reproduced from each tag.** From `0.2.2` on, `pyproject.toml` combined a global
`[tool.hatch.build] sources = ["src"]` with `[tool.hatch.build.targets.wheel] packages =
["src/azure_functions_db"]` (the second line came with #91 on 2026-04-25). Built from the tagged
source, `python -m build`, which builds the wheel from the unpacked source distribution, produced a
wheel with 0 package modules for `v0.2.2`, `v0.3.0`, `v0.4.0`, `v0.4.1` and `v0.4.2`, while
`python -m build --wheel`, which builds straight from the source tree, produced 26. Deleting the
`sources` line made the default build produce 26 for `v0.2.2`, `v0.3.0`, `v0.4.0` and `v0.4.1`.
`v0.2.1`, which had the `sources` line but no `packages` line, builds correctly. The publish
workflow ran `python -m build` through `v0.4.1`.

**How it was resolved.** `0.4.1` (commit `d1b5abc`, "bump version to 0.4.1 to fix empty PyPI
wheel", plus `f5ee040` for its test assertion) changed only `__version__`, so it published the
same empty wheel under a new number.
`0.4.2` (commit `939a925`) switched the publish workflow to `hatch build --clean`, which builds
from the source tree, and its wheel is complete. #239 removed the `sources` line in `0.5.3` and
added a publish step that refuses a wheel without modules and imports the built wheel in a clean
environment.

**Source builds.** The configuration did not change until `0.5.3`, so building the source
distribution of any version from `0.2.2` to `0.5.1` (all nine are on PyPI) with
`pip install --no-binary azure-functions-db==<version>` also installs no module. `0.1.0`, `0.2.0`,
`0.2.1`, `0.5.3`, `0.6.0` and `0.9.0` build correctly. The PyPI wheels of `0.4.2` to `0.5.1` are
complete, so a normal `pip install` is not affected for those versions.

**Impact.** Anyone who pinned exactly one of the four versions, or was resolved to one while it was
the newest, got a package that cannot be imported. README and docs pin none of them, and the
current version, `0.9.0`, is not affected.

**Decision**

- No file on PyPI is replaced or re-uploaded. Whether to yank the four versions is a maintainer
  decision, tracked in #532; none of them is yanked at the time of writing.
- The tags and Releases are kept. Each affected Release body opens with a notice that its wheel
  contains no package modules, and each notice is a protected statement in
  `.github/release-notes.json`.

---

## GitHub Releases

All 25 tags have a GitHub Release, so none was added. The bodies were rewritten into the standard
layout described in [Release Process](release_process.md), after saving each previous body. The
Releases of `v0.2.0`, `v0.4.1`, `v0.4.2` and `v0.5.1` contained only a compare link; the summaries
of `v0.2.0`, `v0.4.2` and `v0.5.1` are reconstructed from the commits between the tags and say so,
and the `v0.4.1` body, which is also an empty-wheel notice, names its two commits itself.

Older Release bodies link to the former repository name `azure-functions-db`; GitHub redirects
those URLs, including compare links, to the current repository. No external contributor is
credited: every merged pull request of this repository was opened by the owner (216) or by
Dependabot (59). The *New Contributors* lines in the original `v0.1.0` and `v0.2.1` bodies named
the owner and Dependabot, who are not credited, so they are not carried over.

---

## How this was verified

1. **Tag, commit and version.** `git show vX.Y.Z:src/azure_functions_db/__init__.py` gives the
   declared `__version__`, and `git merge-base --is-ancestor vX.Y.Z origin/main` tells whether the
   tag is on the history of `main`.
2. **PyPI.** The [JSON API](https://pypi.org/pypi/azure-functions-db/json) lists each version's
   files, upload time and yanked state.
3. **PyPI files against the tagged commit.** The snippet below compares the package files in the
   source distribution and in the wheel with the package directory at the tag, using Git blob IDs.
   For 20 of the 24 published versions both are identical to the tag; for the four versions above
   the wheel is not. Run it from a clone that has the tags.
4. **PyPI attestations.**
   `https://pypi.org/integrity/azure-functions-db/<version>/<sdist file>/provenance` returns an
   attestation whose certificate records the source commit and the workflow run. For all 24
   published versions the attested commit equals the tagged commit.
5. **Workflow runs.** The Actions API keeps run metadata, and job logs for about 90 days. The logs
   of the `v0.5.2` runs were read, and saved, while they were still retained.

```python
import hashlib
import io
import json
import subprocess
import tarfile
import urllib.request
import zipfile

version, tag = "0.4.2", "v0.4.2"
meta = json.load(urllib.request.urlopen(f"https://pypi.org/pypi/azure-functions-db/{version}/json"))


def blob_id(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def download(kind: str) -> bytes:
    url = next(u["url"] for u in meta["urls"] if u["packagetype"] == kind)
    return urllib.request.urlopen(url).read()


shipped: dict[str, dict[str, str]] = {"sdist": {}, "wheel": {}}
with tarfile.open(fileobj=io.BytesIO(download("sdist"))) as tar:
    for member in tar.getmembers():
        _, found, name = member.name.partition("/azure_functions_db/")
        if member.isfile() and found and "__pycache__" not in name:
            shipped["sdist"][name] = blob_id(tar.extractfile(member).read())
with zipfile.ZipFile(io.BytesIO(download("bdist_wheel"))) as wheel:
    for entry in wheel.namelist():
        name = entry.removeprefix("azure_functions_db/")
        if name != entry and not entry.endswith("/") and "__pycache__" not in name:
            shipped["wheel"][name] = blob_id(wheel.read(entry))

listing = subprocess.run(
    ["git", "ls-tree", "-r", f"{tag}:src/azure_functions_db"],
    capture_output=True,
    text=True,
    check=True,
).stdout
tagged = {line.split("\t")[1]: line.split()[2] for line in listing.splitlines()}
for kind, files in shipped.items():
    differing = sorted(k for k in files.keys() | tagged.keys() if files.get(k) != tagged.get(k))
    print(kind, "identical" if not differing else f"{len(differing)} of {len(tagged)} files differ")
```

---

## Rules for the future

- Never move or delete a tag, and never re-upload or replace a published file to repair history.
  Record the case here instead.
- Do not create a tag for a commit that did not produce the published package.
- When a GitHub Release is added for an existing tag, say so in its notes. Mark a release that was
  never published, or whose wheel is unusable, as such.
- Use PyPI, not a GitHub Release, to decide whether a version was published, and open the wheel
  before calling it usable.
