from pathlib import Path

from pre_commit.clientlib import load_config


def test_pre_commit_config_is_valid() -> None:
    # Given the repository's committed pre-commit configuration
    config_path = Path(__file__).resolve().parent.parent / ".pre-commit-config.yaml"

    # When pre-commit parses the configuration
    load_config(str(config_path))

    # Then parsing completes without an InvalidConfigError.
