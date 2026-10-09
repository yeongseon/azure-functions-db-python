from __future__ import annotations

from pathlib import Path
import subprocess

SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "ci_changed_paths.sh"


def run_git(repository: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repository,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def commit_file(repository: Path, name: str, contents: str) -> str:
    (repository / name).write_text(contents, encoding="utf-8")
    run_git(repository, "add", name)
    run_git(
        repository,
        "-c",
        "user.name=CI Test",
        "-c",
        "user.email=ci@example.invalid",
        "commit",
        "-m",
        f"write {name}",
    )
    return run_git(repository, "rev-parse", "HEAD")


def test_push_range_rejects_non_ancestor_before_sha(tmp_path: Path) -> None:
    # Given: a force-push shape where the previous tip is on a discarded branch.
    repository = tmp_path / "repository"
    repository.mkdir()
    run_git(repository, "init", "--initial-branch=main")
    base = commit_file(repository, "base.txt", "base\n")
    before = commit_file(repository, "discarded.txt", "discarded\n")
    run_git(repository, "reset", "--hard", base)
    sha = commit_file(repository, "replacement.txt", "replacement\n")
    assert SCRIPT.is_file()

    # When: the workflow wrapper calculates the push range.
    result = subprocess.run(
        ["bash", str(SCRIPT)],
        cwd=repository,
        env={"EVENT_NAME": "push", "BEFORE_SHA": before, "SHA": sha},
        capture_output=True,
        text=True,
        check=False,
    )

    # Then: it rejects the unsafe range so the workflow selects the full matrix.
    assert result.returncode != 0
    assert result.stdout == ""
