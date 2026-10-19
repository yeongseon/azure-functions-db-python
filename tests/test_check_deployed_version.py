from __future__ import annotations

import json
from unittest.mock import patch
from urllib.error import URLError

import pytest

from tools.check_deployed_version import (
    VersionMismatchError,
    assert_version_matches,
    fetch_deployed_version,
    main,
)


class TestAssertVersionMatches:
    def test_matching_version_does_not_raise(self) -> None:
        assert_version_matches("0.8.3", "0.8.3")

    def test_mismatched_version_raises(self) -> None:
        """Regression (#442): a deployed app resolving a different published
        version than the candidate being certified must fail closed."""
        with pytest.raises(VersionMismatchError, match="0.8.2.*0.8.3"):
            assert_version_matches("0.8.2", "0.8.3")

    def test_missing_version_raises(self) -> None:
        """Regression (#442): an app that never responded with a version
        (endpoint absent, network error, malformed JSON) must be treated as
        a certification failure, not silently skipped."""
        with pytest.raises(VersionMismatchError, match="did not report a version"):
            assert_version_matches(None, "0.8.3")


class TestFetchDeployedVersion:
    def test_returns_version_from_valid_response(self) -> None:
        class FakeResponse:
            status = 200

            def read(self) -> bytes:
                return json.dumps({"version": "0.8.3"}).encode()

            def __enter__(self) -> FakeResponse:
                return self

            def __exit__(self, *exc_info: object) -> None:
                return None

        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            assert fetch_deployed_version("https://example.azurewebsites.net") == "0.8.3"

    def test_returns_none_on_network_error(self) -> None:
        with patch("urllib.request.urlopen", side_effect=URLError("unreachable")):
            assert fetch_deployed_version("https://example.azurewebsites.net") is None

    def test_returns_none_on_non_200_status(self) -> None:
        class FakeResponse:
            status = 404

            def read(self) -> bytes:
                return b""

            def __enter__(self) -> FakeResponse:
                return self

            def __exit__(self, *exc_info: object) -> None:
                return None

        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            assert fetch_deployed_version("https://example.azurewebsites.net") is None

    def test_returns_none_on_malformed_json(self) -> None:
        class FakeResponse:
            status = 200

            def read(self) -> bytes:
                return b"not json"

            def __enter__(self) -> FakeResponse:
                return self

            def __exit__(self, *exc_info: object) -> None:
                return None

        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            assert fetch_deployed_version("https://example.azurewebsites.net") is None

    def test_returns_none_when_version_field_missing(self) -> None:
        class FakeResponse:
            status = 200

            def read(self) -> bytes:
                return json.dumps({"status": "ok"}).encode()

            def __enter__(self) -> FakeResponse:
                return self

            def __exit__(self, *exc_info: object) -> None:
                return None

        with patch("urllib.request.urlopen", return_value=FakeResponse()):
            assert fetch_deployed_version("https://example.azurewebsites.net") is None


class TestMain:
    def test_main_returns_zero_on_match(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("E2E_BASE_URL", "https://example.azurewebsites.net")
        monkeypatch.setenv("EXPECTED_VERSION", "0.8.3")
        with patch("tools.check_deployed_version.fetch_deployed_version", return_value="0.8.3"):
            assert main() == 0

    def test_main_returns_one_on_mismatch(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("E2E_BASE_URL", "https://example.azurewebsites.net")
        monkeypatch.setenv("EXPECTED_VERSION", "0.8.3")
        with patch("tools.check_deployed_version.fetch_deployed_version", return_value="0.8.2"):
            assert main() == 1

    def test_main_returns_one_when_version_missing(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("E2E_BASE_URL", "https://example.azurewebsites.net")
        monkeypatch.setenv("EXPECTED_VERSION", "0.8.3")
        with patch("tools.check_deployed_version.fetch_deployed_version", return_value=None):
            assert main() == 1
