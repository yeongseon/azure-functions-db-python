from __future__ import annotations


class PollerError(Exception):
    pass


class LeaseAcquireError(PollerError):
    pass


class SourceConfigurationError(PollerError):
    pass


class FetchError(PollerError):
    pass


class HandlerError(PollerError):
    pass


class CommitError(PollerError):
    pass


class LostLeaseError(PollerError):
    pass


class SerializationError(PollerError):
    pass
