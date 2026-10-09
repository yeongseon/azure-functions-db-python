from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess

SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "ci_changed_paths.sh"
CLASSIFY_EVENT_SCRIPT = Path(__file__).resolve().parents[1] / "tools" / "ci_classify_event.sh"


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


def test_pull_request_git_diff_failure_emits_full_matrix(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text("#!/usr/bin/env bash\nexit 1\n", encoding="utf-8")
    fake_git.chmod(0o755)

    result = subprocess.run(
        ["bash", str(CLASSIFY_EVENT_SCRIPT), str(output)],
        env={
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "EVENT_NAME": "pull_request",
            "BASE_SHA": "base",
            "HEAD_SHA": "head",
            "SAME_REPOSITORY": "true",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert output.read_text(encoding="utf-8") == (
        "docs_only=false\ndocs_changed=true\nfull_required=true\n"
    )


def test_push_git_diff_failure_emits_full_matrix(tmp_path: Path) -> None:
    output = tmp_path / "github-output"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text(
        "#!/usr/bin/env bash\n[[ $1 != diff ]] || exit 1\nexit 0\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o755)

    result = subprocess.run(
        ["bash", str(CLASSIFY_EVENT_SCRIPT), str(output)],
        env={
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "EVENT_NAME": "push",
            "BEFORE_SHA": "before",
            "SHA": "head",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert output.read_text(encoding="utf-8") == (
        "docs_only=false\ndocs_changed=true\nfull_required=true\n"
    )


def test_fork_pull_request_uses_trusted_classifier(tmp_path: Path) -> None:
    trusted_tools = tmp_path / "trusted" / "tools"
    trusted_tools.mkdir(parents=True)
    for script in ("ci_classify_event.sh", "ci_changed_paths.sh", "ci_classify_changes.sh"):
        shutil.copy2(SCRIPT.parent / script, trusted_tools / script)
    untrusted_tools = tmp_path / "fork" / "tools"
    untrusted_tools.mkdir(parents=True)
    (untrusted_tools / "ci_classify_changes.sh").write_text(
        "#!/usr/bin/env bash\n"
        "printf 'docs_only=true\\ndocs_changed=false\\nfull_required=false\\n'\n",
        encoding="utf-8",
    )
    output = tmp_path / "github-output"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_git = fake_bin / "git"
    fake_git.write_text(
        "#!/usr/bin/env bash\n"
        "if [[ $1 == diff ]]; then printf 'src/azure_functions_db/core/engine.py\\n'; fi\n",
        encoding="utf-8",
    )
    fake_git.chmod(0o755)

    result = subprocess.run(
        ["bash", str(trusted_tools / "ci_classify_event.sh"), str(output)],
        cwd=untrusted_tools.parent,
        env={
            "PATH": f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
            "EVENT_NAME": "pull_request",
            "BASE_SHA": "base",
            "HEAD_SHA": "head",
            "SAME_REPOSITORY": "false",
            "PR_NUMBER": "123",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert output.read_text(encoding="utf-8") == (
        "docs_only=false\ndocs_changed=true\nfull_required=true\n"
    )
