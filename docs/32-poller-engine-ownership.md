# Poller engine configuration and ownership

`SqlAlchemySource(db_config=...)` applies `DbConfig` pool settings, `echo`,
driver `connect_args`, and `engine_kwargs` whether or not an `EngineProvider`
is supplied. Environment placeholders in the connection URL are resolved once
before engine creation. Do not nest `connect_args` inside `engine_kwargs`;
use the dedicated field.

Without a provider, the source creates a private engine lazily on first fetch.
`source.dispose()` releases that engine's pool and clears reflected metadata;
a later fetch creates a new private engine. Failed table reflection also
disposes a private engine before propagating the configuration error.

With an explicit provider, the source borrows the cached engine. Disposing the
source or failing reflection does not dispose that shared engine. Its owner
must call `provider.dispose_all()` when all consumers are finished.

The SQLite regression tests in `tests/test_source_engine_config.py` execute
real queries with and without a provider, verify `NullPool`, pre-ping, recycle,
echo and the driver's busy timeout, and check engine disposal ownership.
