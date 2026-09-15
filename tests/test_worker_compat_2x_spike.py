"""Opt-in spike: verify db decorators survive the installed azure-functions worker.

This module is **off by default**. It is:

* marked ``compat2x`` and excluded from the default ``addopts``, and
* additionally gated on the ``AZFUNC_2X_SPIKE`` environment variable,

so it never runs in normal CI or in a bare ``pytest`` invocation. Its purpose is
to be run **manually** (and via the advisory ``worker-compat-2x`` CI lane)
against a candidate ``azure-functions`` 2.x build to verify the worker-indexing
behavior the ``<2.0.0`` cap protects (see the cap comment in ``pyproject.toml``)
before that cap is lifted.

The load-bearing guarantee: a handler wrapped by a db decorator must still index
to a NON-EMPTY function list, and the db-injected parameter (``order`` / ``user``
/ ``reader`` / ``writer``) must NOT leak into the generated binding set — the
worker must see only the real Azure bindings (``req`` + ``$return``).

Run it like::

    export AZFUNC_2X_SPIKE=1
    pytest -m compat2x -o addopts='' tests/test_worker_compat_2x_spike.py
"""

from __future__ import annotations

from importlib.metadata import version
import json
import os

import azure.functions as func
import pytest

from azure_functions_db.decorator import DbBindings

pytestmark = [
    pytest.mark.compat2x,
    pytest.mark.skipif(
        os.environ.get("AZFUNC_2X_SPIKE") != "1",
        reason="opt-in spike; set AZFUNC_2X_SPIKE=1 to run against a candidate build",
    ),
]


def _registered_functions(app: func.FunctionApp) -> list[object]:
    getter = getattr(app, "get_functions", None)
    if callable(getter):
        return list(getter())
    registry = getattr(app, "_function_builders", None)
    return list(registry) if registry is not None else []


def _binding_names(builder: object) -> list[str]:
    raw = builder.get_function_json()  # type: ignore[attr-defined]
    return [b["name"] for b in json.loads(raw)["bindings"]]


def test_spike_reports_installed_version() -> None:
    installed = version("azure-functions")
    assert installed
    print(f"\n[compat2x] azure-functions=={installed}")


def test_output_hides_injected_param_and_indexes() -> None:
    db = DbBindings()

    @db.output("order", url="sqlite://", table="orders")
    def create_order(req: func.HttpRequest, order: object) -> func.HttpResponse:
        return func.HttpResponse("ok")

    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    app.route(route="orders", methods=["POST"])(create_order)

    functions = _registered_functions(app)
    assert len(functions) >= 1, "worker indexed zero functions — db wrapper was skipped"
    names = _binding_names(functions[0])
    assert "order" not in names, "injected 'order' param leaked into worker bindings"
    assert names == ["req", "$return"]


def test_input_hides_injected_param_and_indexes() -> None:
    db = DbBindings()

    @db.input("user", url="sqlite://", table="users", pk={"id": 1})
    def read_user(req: func.HttpRequest, user: object) -> func.HttpResponse:
        return func.HttpResponse("ok")

    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    app.route(route="users", methods=["GET"])(read_user)

    functions = _registered_functions(app)
    assert len(functions) >= 1
    names = _binding_names(functions[0])
    assert "user" not in names
    assert names == ["req", "$return"]


def test_inject_reader_hides_injected_param_and_indexes() -> None:
    db = DbBindings()

    @db.inject_reader("reader", url="sqlite://", table="users")
    def handler(req: func.HttpRequest, reader: object) -> func.HttpResponse:
        return func.HttpResponse("ok")

    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    app.route(route="r", methods=["GET"])(handler)

    functions = _registered_functions(app)
    assert len(functions) >= 1
    names = _binding_names(functions[0])
    assert "reader" not in names
    assert names == ["req", "$return"]


def test_inject_writer_hides_injected_param_and_indexes() -> None:
    db = DbBindings()

    @db.inject_writer("writer", url="sqlite://", table="orders")
    def handler(req: func.HttpRequest, writer: object) -> func.HttpResponse:
        return func.HttpResponse("ok")

    app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
    app.route(route="w", methods=["POST"])(handler)

    functions = _registered_functions(app)
    assert len(functions) >= 1
    names = _binding_names(functions[0])
    assert "writer" not in names
    assert names == ["req", "$return"]
