"""The :class:`DbOut` output-binding parameter object."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel

from ..binding.writer import DbWriter
from ..core.engine import EngineProvider
from ..core.errors import ConfigurationError
from .validation import _normalize_output_row


class DbOut:
    """Output binding parameter injected by the ``output`` decorator.

    Mirrors the native Azure Functions ``func.Out[T]`` pattern.
    The handler calls ``.set()`` to write data to the database explicitly,
    leaving the handler's return value free for other purposes (e.g.
    ``HttpResponse``).

    Example::

        @db.output("order", url="%DB_URL%", table="orders")
        def create_order(req, order: DbOut) -> func.HttpResponse:
            order.set({"id": 1, "status": "pending"})
            return func.HttpResponse("Created", status_code=201)

    Accepted types for ``.set()``:
        - ``dict`` — single-row write
        - ``list[dict]`` — batch write
        - ``BaseModel`` / ``list[BaseModel]`` — auto-dumped to dict
        - ``None`` — no-op (skip write)
    """

    def __init__(
        self,
        *,
        url: str,
        table: str,
        schema: str | None,
        action: Literal["insert", "upsert"],
        conflict_columns: list[str] | None,
        engine_provider: EngineProvider | None,
    ) -> None:
        self._url = url
        self._table = table
        self._schema = schema
        self._action = action
        self._conflict_columns = conflict_columns
        self._engine_provider = engine_provider

    def set(
        self,
        data: (
            dict[str, object] | Sequence[dict[str, object]] | BaseModel | Sequence[BaseModel] | None
        ),
    ) -> None:
        """Write *data* to the configured table.

        Parameters
        ----------
        data:
            ``dict`` for single row, ``list[dict]`` for batch,
            ``BaseModel`` / ``list[BaseModel]`` for Pydantic models,
            or ``None`` to skip. Tuples and other non-``list`` sequences
            are rejected with :class:`ConfigurationError`.
        """
        if data is None:
            return

        writer = DbWriter(
            url=self._url,
            table=self._table,
            schema=self._schema,
            engine_provider=self._engine_provider,
        )
        try:
            if isinstance(data, (dict, BaseModel)):
                row = _normalize_output_row(data)
                if self._action == "upsert":
                    if self._conflict_columns is None:
                        msg = "output: unreachable – upsert without conflict_columns"
                        raise ConfigurationError(msg)
                    writer.upsert(data=row, conflict_columns=self._conflict_columns)
                else:
                    writer.insert(data=row)
            elif isinstance(data, list):
                bad = next(
                    (i for i, row in enumerate(data) if not isinstance(row, (dict, BaseModel))),
                    None,
                )
                if bad is not None:
                    bad_type = type(data[bad]).__name__
                    msg = (
                        f"output: DbOut.set() received list with non-dict element "
                        f"at index {bad} ({bad_type}); expected list[dict | BaseModel]"
                    )
                    raise ConfigurationError(msg)
                rows = [_normalize_output_row(row) for row in data]
                if self._action == "upsert":
                    if self._conflict_columns is None:
                        msg = "output: unreachable – upsert without conflict_columns"
                        raise ConfigurationError(msg)
                    writer.upsert_many(rows=rows, conflict_columns=self._conflict_columns)
                else:
                    writer.insert_many(rows=rows)
            else:
                msg = (
                    f"output: DbOut.set() received {type(data).__name__}, "
                    f"expected dict, list[dict], BaseModel, list[BaseModel], or None"
                )
                raise ConfigurationError(msg)
        finally:
            writer.close()


__all__ = ["DbOut"]
