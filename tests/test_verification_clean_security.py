from pathlib import Path

import pytest

from tools.verification_environment import clean


@pytest.mark.parametrize("tree", ["tools", "src", "tests", "scripts"])
def test_clean_never_follows_symlinked_tree_root(tmp_path: Path, tree: str) -> None:
    # Given the exact external cache sentinel behind a linked source tree root.
    root, external = tmp_path / "repo", tmp_path / "external"
    root.mkdir()
    cache = external / "__pycache__"
    cache.mkdir(parents=True)
    sentinel = cache / "unrelated.pyc"
    sentinel.write_bytes(b"external-sentinel")
    (root / tree).symlink_to(external, target_is_directory=True)
    # When cleaning the checkout, then the external cache is untouched.
    clean(root)
    assert sentinel.read_bytes() == b"external-sentinel"
    assert (root / tree).is_symlink()


@pytest.mark.parametrize("kind", ["nested", "cache", "dangling"])
def test_clean_preserves_symlink_targets_at_every_depth(tmp_path: Path, kind: str) -> None:
    root, external = tmp_path / "repo", tmp_path / "external"
    source = root / "src"
    source.mkdir(parents=True)
    cache = external / "__pycache__"
    cache.mkdir(parents=True)
    sentinel = cache / "unrelated.pyc"
    sentinel.write_bytes(b"external-sentinel")
    link = source / ("__pycache__" if kind == "cache" else "nested")
    link.symlink_to(
        tmp_path / "absent" if kind == "dangling" else external, target_is_directory=True
    )
    owned = source / "owned/__pycache__"
    owned.mkdir(parents=True)
    (owned / "local.pyc").write_bytes(b"local")
    clean(root)
    assert sentinel.read_bytes() == b"external-sentinel"
    assert not owned.exists()


def test_clean_removes_top_level_link_without_touching_target(tmp_path: Path) -> None:
    root, external = tmp_path / "repo", tmp_path / "external"
    root.mkdir()
    external.mkdir()
    sentinel = external / "unrelated.pyc"
    sentinel.write_bytes(b"external-sentinel")
    (root / "test-results").symlink_to(external, target_is_directory=True)
    clean(root)
    assert sentinel.read_bytes() == b"external-sentinel"
    assert not (root / "test-results").is_symlink()
