"""The committed historical exception, exercised against the audit with fixtures (#527).

The generic behaviour is covered by ``test_release_audit.py``. These cases use this repository's
own history: a healthy release, the ``v0.5.2`` candidate whose release certification failed after
its GitHub Release was created, a tag that declares another version, and missing GitHub metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from tools import release_audit as ra

ROOT = Path(__file__).resolve().parents[1]
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
OLD = NOW - timedelta(days=30)
CFG = ra.Config.load(ROOT / ra.CONFIG_PATH)


def tag(version: str, declared: str | None = None, on_mainline: bool = True) -> ra.TagFact:
    return ra.TagFact(f"v{version}", "a" * 40, OLD, declared or version, on_mainline)


def release(version: str) -> ra.ReleaseFact:
    return ra.ReleaseFact(f"v{version}", False, OLD)


def pypi(version: str) -> ra.PyPIFact:
    return ra.PyPIFact(version, False, OLD)


def facts(
    tags: list[ra.TagFact],
    releases: list[ra.ReleaseFact],
    published: list[ra.PyPIFact],
    changelog: set[str],
) -> ra.Facts:
    return ra.Facts(
        tags={t.name.removeprefix("v"): t for t in tags},
        releases={r.tag.removeprefix("v"): r for r in releases},
        pypi={p.version: p for p in published},
        changelog_versions=frozenset(changelog),
    )


def only(*versions: str) -> ra.Config:
    """The committed exceptions for just these versions, so a one-version fixture is not stale."""
    return ra.Config(
        repo=CFG.repo,
        package=CFG.package,
        mainline=CFG.mainline,
        approved={v: CFG.approved[v] for v in versions},
    )


def test_the_committed_exception_is_exactly_the_documented_case() -> None:
    assert set(CFG.approved) == {"0.5.2"}
    assert CFG.approved["0.5.2"].codes == {ra.RELEASE_WITHOUT_PYPI}


def test_a_healthy_release_is_clean() -> None:
    clean = facts([tag("0.9.0")], [release("0.9.0")], [pypi("0.9.0")], {"0.9.0"})
    report = ra.audit(only(), clean, NOW)
    assert report.ok
    assert report.findings == ()


def test_a_candidate_that_failed_after_its_github_release_is_a_documented_exception() -> None:
    candidate = facts([tag("0.5.2")], [release("0.5.2")], [], {"0.5.2"})
    unapproved = ra.audit(only(), candidate, NOW)
    assert not unapproved.ok
    assert unapproved.findings[0].unapproved == (ra.RELEASE_WITHOUT_PYPI,)
    report = ra.audit(only("0.5.2"), candidate, NOW)
    assert report.ok
    assert report.findings[0].approved == (ra.RELEASE_WITHOUT_PYPI,)


def test_a_new_unpublished_release_is_still_reported_next_to_the_exception() -> None:
    both = facts(
        [tag("0.5.2"), tag("0.10.0")],
        [release("0.5.2"), release("0.10.0")],
        [],
        {"0.5.2", "0.10.0"},
    )
    report = ra.audit(only("0.5.2"), both, NOW)
    assert not report.ok
    assert [f.version for f in report.findings if f.failing] == ["0.10.0"]


def test_a_tag_that_declares_another_version_is_reported_and_never_pending() -> None:
    wrong = facts(
        [ra.TagFact("v0.9.1", "a" * 40, NOW - timedelta(hours=1), "0.9.0", True)],
        [ra.ReleaseFact("v0.9.1", False, NOW - timedelta(hours=1))],
        [pypi("0.9.1")],
        {"0.9.1"},
    )
    report = ra.audit(only(), wrong, NOW)
    assert not report.ok
    assert report.findings[0].unapproved == (ra.TAG_VERSION_MISMATCH,)
    assert report.findings[0].pending == ()


def test_missing_github_metadata_is_reported() -> None:
    no_release = facts([tag("0.8.3")], [], [pypi("0.8.3")], {"0.8.3"})
    report = ra.audit(only(), no_release, NOW)
    assert not report.ok
    assert report.findings[0].unapproved == (ra.PYPI_WITHOUT_RELEASE,)

    no_tag = facts([], [release("0.8.3")], [pypi("0.8.3")], {"0.8.3"})
    report = ra.audit(only(), no_tag, NOW)
    assert not report.ok
    # A published version with a Release but no tag is reported from both sides.
    assert report.findings[0].unapproved == (ra.PYPI_WITHOUT_TAG, ra.RELEASE_WITHOUT_TAG)
