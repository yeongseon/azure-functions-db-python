from azure_functions_db.core.types import RawRecord, SourceDescriptor
from azure_functions_db.trigger import SourceAdapter
from azure_functions_db.trigger.events import RowChange
from azure_functions_db.trigger.runner import PollRunner
from examples.custom_source import NumberSource
from tests.test_trigger_runner import FakeStateStore


def test_documented_source_resumes_after_committed_batches() -> None:
    source: SourceAdapter = NumberSource()
    store = FakeStateStore()
    delivered: list[int] = []

    def normalize(record: RawRecord, descriptor: SourceDescriptor) -> RowChange:
        identifier = record["id"]
        assert isinstance(identifier, int)
        return RowChange(
            event_id=str(identifier),
            op="upsert",
            source=descriptor,
            cursor=identifier,
            pk={"id": identifier},
            before=None,
            after=record,
        )

    def handle(events: list[RowChange]) -> None:
        for event in events:
            identifier = event.pk["id"]
            assert isinstance(identifier, int)
            delivered.append(identifier)

    runner = PollRunner(
        name="numbers",
        source=source,
        state_store=store,
        normalizer=normalize,
        handler=handle,
        batch_size=2,
        max_batches_per_tick=2,
    )
    runner.tick()
    resumed = PollRunner(
        name="numbers",
        source=NumberSource(),
        state_store=store,
        normalizer=normalize,
        handler=handle,
        batch_size=2,
    )
    resumed.tick()

    assert delivered == [1, 2, 3, 4, 5]
    assert store.load_checkpoint("numbers")["cursor"] == 5
