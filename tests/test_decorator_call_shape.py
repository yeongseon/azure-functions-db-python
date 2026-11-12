"""Regression (#538): keyword arguments stay keywords when forwarded to the wrapped callable.

0.9.0 (#446) re-emitted every call using the wrapped callable's *advertised*
parameter kinds. A wrapper that advertises positional-or-keyword parameters but
only accepts ``req`` positionally (``azure-functions-validation``'s
``wrapper(req, **kw)``) then failed with ``TypeError``. The #446 behaviour --
positional calls against the exposed signature reaching the right parameter --
must keep working.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import inspect
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine, text

from azure_functions_db import DbBindings
from azure_functions_db.decorator.validation import _bind_host_args, _finalize_injected_call


def _make_validate_http_like(*names: str) -> Callable[..., Any]:
    """Mimic ``@validate_http``: accepts only ``req`` positionally, the rest as
    keywords, yet advertises every parameter as positional-or-keyword."""

    def inner(req: Any, **kwargs: Any) -> dict[str, Any]:
        return {"req": req, "kwargs": dict(kwargs)}

    inner.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter(n, inspect.Parameter.POSITIONAL_OR_KEYWORD) for n in ("req", *names)]
    )
    return inner


_IDS = ["input", "output", "inject_reader", "inject_writer"]


@pytest.fixture()
def url(tmp_path: Path) -> str:
    db_url = f"sqlite:///{tmp_path / 'shape.db'}"
    engine = create_engine(db_url)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
            conn.execute(text("INSERT INTO users (id, name) VALUES (1, 'Alice')"))
    finally:
        engine.dispose()
    return db_url


def _decorators(url: str) -> list[Any]:
    db = DbBindings()
    return [
        db.input("out", url=url, table="users", pk={"id": 1}),
        db.output("out", url=url, table="users"),
        db.inject_reader("out", url=url, table="users"),
        db.inject_writer("out", url=url, table="users"),
    ]


@pytest.mark.parametrize(
    "index", range(4), ids=["input", "output", "inject_reader", "inject_writer"]
)
def test_keyword_call_over_positional_limited_wrapper(url: str, index: int) -> None:
    handler = _decorators(url)[index](_make_validate_http_like("out"))

    result = handler(req="r")

    assert result["req"] == "r"
    assert set(result["kwargs"]) == {"out"}


@pytest.mark.parametrize(
    "index", range(4), ids=["input", "output", "inject_reader", "inject_writer"]
)
def test_positional_call_over_positional_limited_wrapper(url: str, index: int) -> None:
    handler = _decorators(url)[index](_make_validate_http_like("out"))

    result = handler("r")

    assert result["req"] == "r"
    assert set(result["kwargs"]) == {"out"}


def test_async_keyword_call_over_positional_limited_wrapper(url: str) -> None:
    async def inner(req: Any, **kwargs: Any) -> dict[str, Any]:
        return {"req": req, "kwargs": dict(kwargs)}

    inner.__signature__ = inspect.Signature(  # type: ignore[attr-defined]
        [inspect.Parameter(n, inspect.Parameter.POSITIONAL_OR_KEYWORD) for n in ("req", "out")]
    )
    handler = DbBindings().output("out", url=url, table="users")(inner)

    result = asyncio.run(handler(req="r"))

    assert result["req"] == "r"
    assert set(result["kwargs"]) == {"out"}


def test_extra_host_parameters_stay_keywords_when_called_by_keyword(url: str) -> None:
    """The worker supplies ``req`` and ``context`` by keyword."""
    handler = DbBindings().output("out", url=url, table="users")(
        _make_validate_http_like("context", "out")
    )

    result = handler(req="r", context="c")

    assert result["req"] == "r"
    assert set(result["kwargs"]) == {"context", "out"}
    assert result["kwargs"]["context"] == "c"


# --- #446 behaviour is preserved --------------------------------------------


def test_positional_call_with_injected_param_first_still_binds_by_name() -> None:
    def fn(out: Any, label: str, flag: bool = False) -> tuple[Any, str, bool]:
        return out, label, flag

    host_sig = inspect.Signature(
        [p for n, p in inspect.signature(fn).parameters.items() if n != "out"]
    )
    bound = _bind_host_args(host_sig, ("x",), {})

    args, kwargs = _finalize_injected_call(fn, bound, "out", "INJ")

    assert fn(*args, **kwargs) == ("INJ", "x", False)
    # injected parameter precedes a caller-positional one, so it must be positional
    assert args[:2] == ("INJ", "x")


def test_mixed_call_keeps_caller_style() -> None:
    def fn(a: Any, out: Any, b: Any, c: Any) -> None:
        return None

    host_sig = inspect.Signature(
        [p for n, p in inspect.signature(fn).parameters.items() if n != "out"]
    )
    bound = _bind_host_args(host_sig, ("A",), {"b": "B", "c": "C"})

    args, kwargs = _finalize_injected_call(fn, bound, "out", "INJ")

    assert args == ("A",)
    assert kwargs == {"out": "INJ", "b": "B", "c": "C"}


def test_positional_only_and_keyword_only_kinds_are_respected() -> None:
    def fn(a: Any, /, out: Any, *, k: Any) -> tuple[Any, Any, Any]:
        return a, out, k

    host_sig = inspect.Signature(
        [p for n, p in inspect.signature(fn).parameters.items() if n != "out"]
    )
    bound = _bind_host_args(host_sig, ("A",), {"k": "K"})

    args, kwargs = _finalize_injected_call(fn, bound, "out", "INJ")

    assert args == ("A",)
    assert kwargs == {"out": "INJ", "k": "K"}
    assert fn(*args, **kwargs) == ("A", "INJ", "K")


def test_var_positional_and_var_keyword_are_forwarded() -> None:
    def fn(out: Any, a: Any, *rest: Any, **extra: Any) -> tuple[Any, ...]:
        return out, a, rest, extra

    host_sig = inspect.Signature(
        [p for n, p in inspect.signature(fn).parameters.items() if n != "out"]
    )
    bound = _bind_host_args(host_sig, ("A", "r1", "r2"), {"z": 1})

    args, kwargs = _finalize_injected_call(fn, bound, "out", "INJ")

    assert fn(*args, **kwargs) == ("INJ", "A", ("r1", "r2"), {"z": 1})


def test_finalize_without_recorded_call_shape_falls_back_to_signature_kinds() -> None:
    """A plain ``BoundArguments`` (not produced by ``_bind_host_args``) keeps the
    previous signature-kind based behaviour."""

    def fn(out: Any, a: Any) -> None:
        return None

    host_sig = inspect.Signature([inspect.signature(fn).parameters["a"]])
    plain = host_sig.bind("A")

    args, kwargs = _finalize_injected_call(fn, plain, "out", "INJ")

    assert args == ("INJ", "A")
    assert kwargs == {}


def test_real_validate_http_stacked_below_db_output(url: str) -> None:
    """Integration with the released validation package, when installed."""
    validate_http = pytest.importorskip("azure_functions_validation").validate_http
    import azure.functions as func
    from pydantic import BaseModel

    class Body(BaseModel):
        name: str

    def create_inner(req: func.HttpRequest, body: Body, out: Any) -> func.HttpResponse:
        return func.HttpResponse("ok")

    # db.output ABOVE validate_http, as in the #538 report
    create = DbBindings().output("out", url=url, table="users")(
        validate_http(body=Body)(create_inner)
    )

    req = func.HttpRequest(
        "POST", "/x", body=b'{"name":"a"}', headers={"content-type": "application/json"}
    )

    assert create(req=req).status_code == 200
