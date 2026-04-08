# Roadmap

## v0.1
- Python core package
- PollTrigger
- SqlAlchemySource
- BlobCheckpointStore
- Postgres/MySQL/SQL Server integration
- cursor polling
- docs + examples + CI

## v0.2
- DbReader / DbWriter MVP (input/output binding)
- Pydantic model mapping
- quarantine sink
- backfill mode
- better metrics
- CLI reset/inspect

## v0.3
- ~~binding decorator sugar~~ (done: `DbBindings` with `trigger`, `input`, `output`)
- Mongo adapter
- outbox helper
- Service Bus/Event Hub relay mode
- multiple batches per tick

## v0.4
- Table/Cosmos checkpoint store
- per-source tuning profiles
- stronger lease backend

## v0.5
- CDC-capable adapters
- richer partitioning
- cloud event envelope
- dashboard templates

## v1.0 Criteria
- Public API stabilized
- Binding API stabilized
- Semantics documentation complete
- Production adoption signal from 3+ DBs
- Upgrade/migration story documented
