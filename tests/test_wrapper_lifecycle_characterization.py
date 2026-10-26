import asyncio
import inspect

import pytest

from azure_functions_db.decorator.wrapper import _wrap_handler


@pytest.mark.parametrize("async_handler", [False, True])
@pytest.mark.parametrize("managed", [False, True])
@pytest.mark.parametrize("failure", ["none", "bind", "handler", "convert"])
def test_wrapper_preserves_acquisition_binding_and_cleanup_order(
    async_handler: bool, managed: bool, failure: str
) -> None:
    # Given a resource wrapper whose observable lifecycle records each stage.
    stages: list[str] = []

    def acquire() -> str:
        stages.append("acquire")
        return "resource"

    def convert(resource: str) -> str:
        stages.append("convert")
        if failure == "convert":
            raise RuntimeError("convert")
        return resource

    def release(resource: str) -> None:
        assert resource == "resource"
        stages.append("release")

    def handler(injected: str, request: str) -> str:
        stages.append("handler")
        if failure == "handler":
            raise RuntimeError("handler")
        return injected + request

    async def async_fn(injected: str, request: str) -> str:
        return handler(injected, request)

    wrapped = _wrap_handler(
        async_fn if async_handler else handler,
        kind="inject_reader",
        arg_name="injected",
        metadata={"version": 1, "bindings": [], "injections": []},
        acquire=acquire,
        to_async_arg=convert,
        to_sync_arg=convert,
        release=release if managed else None,
    )
    assert list(inspect.signature(wrapped).parameters) == ["request"]

    def invoke() -> str:
        args = () if failure == "bind" else ("request",)
        if async_handler:
            result = asyncio.run(wrapped(*args))
        else:
            result = wrapped(*args)
        assert isinstance(result, str)
        return result

    # When binding, conversion or the handler succeeds/fails, cleanup order stays stable.
    if failure == "none":
        assert invoke() == "resourcerequest"
    else:
        with pytest.raises(TypeError if failure == "bind" else RuntimeError):
            invoke()
    # Then managed binding failures release resources; unmanaged ones acquire nothing.
    expected = [] if failure == "bind" and not managed else ["acquire"]
    if failure != "bind":
        expected.append("convert")
        if failure != "convert":
            expected.append("handler")
    if managed:
        expected.append("release")
    assert stages == expected
