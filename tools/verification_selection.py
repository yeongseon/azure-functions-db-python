from __future__ import annotations

import os

import pytest


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    backend = os.environ.get("VERIFICATION_DB_BACKEND")
    if not backend:
        return
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        callspec = getattr(item, "callspec", None)
        key = callspec.params.get("db_key") if callspec else None
        matches = key == backend or item.get_closest_marker(backend) is not None
        (selected if matches else deselected).append(item)
    items[:] = selected
    config.hook.pytest_deselected(items=deselected)
