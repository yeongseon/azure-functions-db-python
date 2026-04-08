from __future__ import annotations


class DbError(Exception):
    pass


class DbConnectionError(DbError):
    pass


class QueryError(DbError):
    pass


class WriteError(DbError):
    pass


class NotFoundError(DbError):
    pass
