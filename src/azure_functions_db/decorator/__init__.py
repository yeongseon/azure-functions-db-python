"""Azure Functions-style decorator API for database integration.

This package was split out of the original single ``decorator.py`` module
(see issue #265).  The public and private import surface is preserved
verbatim: every name that used to be importable from
``azure_functions_db.decorator`` is re-exported here, so existing consumers
and the test-suite continue to work unchanged.
"""

from __future__ import annotations

from .async_proxies import _AsyncDbOutProxy as _AsyncDbOutProxy
from .async_proxies import _AsyncDbReaderProxy as _AsyncDbReaderProxy
from .async_proxies import _AsyncDbWriterProxy as _AsyncDbWriterProxy
from .async_proxies import _AsyncProxyBase as _AsyncProxyBase
from .async_proxies import _AsyncTxWriterProxy as _AsyncTxWriterProxy
from .bindings import DbBindings as DbBindings
from .composition import _check_composition as _check_composition
from .composition import _get_db_decorators as _get_db_decorators
from .composition import _mark_decorator as _mark_decorator
from .composition import _merge_toolkit_metadata as _merge_toolkit_metadata
from .constants import _DB_DECORATOR_ATTR as _DB_DECORATOR_ATTR
from .constants import _RESERVED_ARGS as _RESERVED_ARGS
from .constants import _TOOLKIT_META_ATTR as _TOOLKIT_META_ATTR
from .metadata import get_db_metadata as get_db_metadata
from .out import DbOut as DbOut
from .validation import _apply_input_model as _apply_input_model
from .validation import _build_host_signature as _build_host_signature
from .validation import _normalize_output_row as _normalize_output_row
from .validation import _resolve_callable as _resolve_callable
from .validation import _validate_arg_name as _validate_arg_name
from .validation import _validate_model_type as _validate_model_type
from .validation import _validate_resolver as _validate_resolver
from .wrapper import _finalize_wrapper as _finalize_wrapper
from .wrapper import _wrap_handler as _wrap_handler

__all__ = [
    "DbBindings",
    "DbOut",
    "get_db_metadata",
]
