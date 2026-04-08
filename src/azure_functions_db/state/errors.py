from __future__ import annotations


class StateStoreError(Exception):
    pass


class LeaseConflictError(StateStoreError):
    pass


class FingerprintMismatchError(StateStoreError):
    pass
