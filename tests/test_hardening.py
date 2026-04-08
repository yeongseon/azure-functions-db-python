from __future__ import annotations

from collections.abc import Sequence

import pytest

from azure_functions_db.core.types import CursorValue, SourceDescriptor
from azure_functions_db.state.errors import LeaseConflictError
from azure_functions_db.trigger.errors import CommitError, HandlerError, LostLeaseError
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.runner import PollRunner, RawRecord


class SimulatedCrash(BaseException):
    """Sentinel to simulate an uncaught exception escaping PollRunner.

    Inherits from BaseException (not Exception) because PollRunner
    catches Exception internally. This bypasses those handlers to verify
    checkpoint safety. Note: unlike a real process crash, PollRunner's
    finally clause still runs (releasing the lease). Retry assertions
    model post-recovery reacquisition, not immediate same-tick retry.
    """


class FakeStateStore:
    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[str, object]] = {}
        self.leases: dict[str, str] = {}
        self.lease_counter: int = 0
        self.acquire_error: Exception | None = None
        self.commit_error: Exception | None = None
        self.commit_error_after_store: Exception | None = None
        self.load_error: Exception | None = None
        self.commit_calls: int = 0

    def acquire_lease(self, poller_name: str, ttl_seconds: int) -> str:
        del ttl_seconds
        if self.acquire_error:
            raise self.acquire_error
        if poller_name in self.leases:
            raise LeaseConflictError(f"lease already held for '{poller_name}'")
        self.lease_counter += 1
        lease_id = f"lease-{self.lease_counter}"
        self.leases[poller_name] = lease_id
        return lease_id

    def renew_lease(self, poller_name: str, lease_id: str, ttl_seconds: int) -> None:
        del poller_name, lease_id, ttl_seconds

    def release_lease(self, poller_name: str, lease_id: str) -> None:
        if self.leases.get(poller_name) == lease_id:
            _ = self.leases.pop(poller_name, None)

    def load_checkpoint(self, poller_name: str) -> dict[str, object]:
        if self.load_error:
            raise self.load_error
        return dict(self.checkpoints.get(poller_name, {}))

    def commit_checkpoint(
        self, poller_name: str, checkpoint: dict[str, object], lease_id: str
    ) -> None:
        self.commit_calls += 1
        current_lease = self.leases.get(poller_name)
        if current_lease != lease_id:
            raise LostLeaseError("lease stolen")
        if self.commit_error:
            raise self.commit_error
        self.checkpoints[poller_name] = dict(checkpoint)
        if self.commit_error_after_store:
            raise self.commit_error_after_store

    def steal_lease(self, poller_name: str) -> str:
        self.lease_counter += 1
        lease_id = f"lease-{self.lease_counter}"
        self.leases[poller_name] = lease_id
        return lease_id


class FakeSourceAdapter:
    def __init__(self, records: list[RawRecord]) -> None:
        self._records: list[RawRecord] = list(records)
        self._descriptor: SourceDescriptor = SourceDescriptor(
            name="test_table", kind="sqlalchemy", fingerprint="fp_test"
        )
        self.fetch_error: Exception | None = None
        self.descriptor_error: Exception | None = None
        self.fetch_history: list[CursorValue | None] = []

    @property
    def source_descriptor(self) -> SourceDescriptor:
        if self.descriptor_error:
            raise self.descriptor_error
        return self._descriptor

    def fetch(
        self, cursor: CursorValue | None, batch_size: int
    ) -> Sequence[RawRecord]:
        if self.fetch_error:
            raise self.fetch_error
        self.fetch_history.append(cursor)
        cursor_value = cursor[0] if isinstance(cursor, tuple) else cursor
        result: list[RawRecord] = []
        for record in self._records:
            record_cursor = record.get("updated_at")
            if (
                cursor_value is not None
                and isinstance(record_cursor, int)
                and isinstance(cursor_value, int)
                and record_cursor <= cursor_value
            ):
                continue
            result.append(record)
            if len(result) >= batch_size:
                break
        return result


def _default_normalizer(record: RawRecord, source: SourceDescriptor) -> RowChange:
    raw_cursor = record.get("updated_at")
    cursor = (
        raw_cursor
        if isinstance(raw_cursor, (str, int, float, bool)) or raw_cursor is None
        else None
    )
    return RowChange(
        event_id=f"evt-{record.get('id', 0)}",
        op="upsert",
        source=source,
        cursor=cursor,
        pk={"id": record.get("id", 0)},
        before=None,
        after=dict(record),
    )


def test_crash_after_fetch_before_handler_does_not_advance_checkpoint() -> None:
    records: list[RawRecord] = [{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 200}]
    store = FakeStateStore()
    source = FakeSourceAdapter(records)
    handler_called = False

    def should_not_be_called(events: list[RowChange]) -> None:
        nonlocal handler_called
        handler_called = True

    crash_count = 0

    def crashing_normalizer(record: RawRecord, src: SourceDescriptor) -> RowChange:
        nonlocal crash_count
        crash_count += 1
        raise SimulatedCrash("crash during normalization after fetch")

    runner = PollRunner(
        name="test_poller",
        source=source,
        state_store=store,
        normalizer=crashing_normalizer,
        handler=should_not_be_called,
        batch_size=10,
    )

    with pytest.raises(SimulatedCrash, match="crash during normalization after fetch"):
        runner.tick()

    assert not handler_called
    assert "test_poller" not in store.checkpoints
    assert store.commit_calls == 0
    assert crash_count == 1

    reprocessed: list[list[str]] = []
    retry_runner = PollRunner(
        name="test_poller",
        source=source,
        state_store=store,
        normalizer=_default_normalizer,
        handler=lambda events: reprocessed.append([event.event_id for event in events]),
        batch_size=10,
    )

    assert retry_runner.tick() == 2
    assert reprocessed == [["evt-1", "evt-2"]]
    assert source.fetch_history == [None, None]


def test_crash_after_partial_handler_entire_batch_reprocessed() -> None:
    records: list[RawRecord] = [
        {"id": 1, "updated_at": 100},
        {"id": 2, "updated_at": 200},
        {"id": 3, "updated_at": 300},
    ]
    store = FakeStateStore()
    source = FakeSourceAdapter(records)
    partial_side_effects: list[str] = []

    def crash_after_partial_work(events: list[RowChange]) -> None:
        partial_side_effects.append(events[0].event_id)
        partial_side_effects.append(events[1].event_id)
        raise SimulatedCrash("crash after partial side effects")

    runner = PollRunner(
        name="test_poller",
        source=source,
        state_store=store,
        normalizer=_default_normalizer,
        handler=crash_after_partial_work,
        batch_size=10,
    )

    with pytest.raises(SimulatedCrash, match="crash after partial side effects"):
        runner.tick()

    assert partial_side_effects == ["evt-1", "evt-2"]
    assert "test_poller" not in store.checkpoints

    reprocessed: list[list[str]] = []
    retry_runner = PollRunner(
        name="test_poller",
        source=source,
        state_store=store,
        normalizer=_default_normalizer,
        handler=lambda events: reprocessed.append([event.event_id for event in events]),
        batch_size=10,
    )

    assert retry_runner.tick() == 3
    assert reprocessed == [["evt-1", "evt-2", "evt-3"]]
    assert source.fetch_history == [None, None]


def test_ambiguous_commit_resolved_by_next_tick_reload() -> None:
    records: list[RawRecord] = [{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 200}]

    store_saved = FakeStateStore()
    source_saved = FakeSourceAdapter(records)
    store_saved.commit_error_after_store = RuntimeError("commit response lost")
    saved_runner = PollRunner(
        name="saved_poller",
        source=source_saved,
        state_store=store_saved,
        normalizer=_default_normalizer,
        handler=lambda events: None,
        batch_size=10,
    )

    with pytest.raises(CommitError, match="Checkpoint commit failed"):
        saved_runner.tick()

    assert store_saved.checkpoints["saved_poller"]["cursor"] == 200

    no_reprocess: list[list[str]] = []
    saved_retry_runner = PollRunner(
        name="saved_poller",
        source=source_saved,
        state_store=store_saved,
        normalizer=_default_normalizer,
        handler=lambda events: no_reprocess.append([event.event_id for event in events]),
        batch_size=10,
    )

    assert saved_retry_runner.tick() == 0
    assert no_reprocess == []
    assert source_saved.fetch_history == [None, 200]

    store_failed = FakeStateStore()
    source_failed = FakeSourceAdapter(records)
    store_failed.commit_error = RuntimeError("commit failed")
    failed_runner = PollRunner(
        name="failed_poller",
        source=source_failed,
        state_store=store_failed,
        normalizer=_default_normalizer,
        handler=lambda events: None,
        batch_size=10,
    )

    with pytest.raises(CommitError, match="Checkpoint commit failed"):
        failed_runner.tick()

    assert "failed_poller" not in store_failed.checkpoints

    # Clear the commit error so the retry runner can succeed
    store_failed.commit_error = None

    reprocessed: list[list[str]] = []
    retry_runner = PollRunner(
        name="failed_poller",
        source=source_failed,
        state_store=store_failed,
        normalizer=_default_normalizer,
        handler=lambda events: reprocessed.append([event.event_id for event in events]),
        batch_size=10,
    )

    assert retry_runner.tick() == 2
    assert reprocessed == [["evt-1", "evt-2"]]
    assert source_failed.fetch_history == [None, None]


def test_stale_runner_commit_rejected_winner_advances() -> None:
    records: list[RawRecord] = [{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 200}]
    store = FakeStateStore()
    source_a = FakeSourceAdapter(records)
    source_b = FakeSourceAdapter(records)
    runner_b_handled: list[list[str]] = []

    stolen_lease_id: str = ""

    def stale_handler(events: list[RowChange]) -> None:
        nonlocal stolen_lease_id
        assert [event.event_id for event in events] == ["evt-1", "evt-2"]
        stolen_lease_id = store.steal_lease("shared_poller")

    runner_a = PollRunner(
        name="shared_poller",
        source=source_a,
        state_store=store,
        normalizer=_default_normalizer,
        handler=stale_handler,
        batch_size=10,
    )

    with pytest.raises(LostLeaseError, match="lease stolen"):
        runner_a.tick()

    assert "shared_poller" not in store.checkpoints

    store.release_lease("shared_poller", stolen_lease_id)

    runner_b = PollRunner(
        name="shared_poller",
        source=source_b,
        state_store=store,
        normalizer=_default_normalizer,
        handler=lambda events: runner_b_handled.append(
            [event.event_id for event in events]
        ),
        batch_size=10,
    )

    assert runner_b.tick() == 2
    assert runner_b_handled == [["evt-1", "evt-2"]]
    assert store.checkpoints["shared_poller"]["cursor"] == 200


def test_permanent_handler_failure_does_not_advance_checkpoint_across_ticks() -> None:
    records: list[RawRecord] = [{"id": 1, "updated_at": 100}, {"id": 2, "updated_at": 200}]
    store = FakeStateStore()
    source = FakeSourceAdapter(records)
    handled_batches: list[list[str]] = []

    def always_fails(events: list[RowChange]) -> None:
        handled_batches.append([event.event_id for event in events])
        raise ValueError("permanent failure")

    runner = PollRunner(
        name="test_poller",
        source=source,
        state_store=store,
        normalizer=_default_normalizer,
        handler=always_fails,
        batch_size=10,
    )

    for _ in range(3):
        with pytest.raises(HandlerError, match="Handler failed"):
            runner.tick()

    assert "test_poller" not in store.checkpoints
    assert handled_batches == [
        ["evt-1", "evt-2"],
        ["evt-1", "evt-2"],
        ["evt-1", "evt-2"],
    ]
    assert source.fetch_history == [None, None, None]
