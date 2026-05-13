# gmr-event-schemas

JSON Schemas for events written to the `events.entity_events`
log in `gmr_app`. Versioned per event type; published as a
Python package (`gmr_event_schemas`) imported by every producer
and every consumer.

See [fontem-ontology/MIGRATION.md](https://contribute.void42.internal/fontem/fontem-ontology/src/branch/main/MIGRATION.md) for the broader event-log architecture.

## Layout

```
v1/                                # schema-version 1
  control/
    BeginGraphReplace.json
    EndGraphReplace.json
  entities/
    UpsertCompany.json
    DeleteCompany.json
    UpsertSanctionedEntity.json
    UpsertFiling.json
    …
  consolidation/
    AssertSameAs.json
    MergeRequested.json
    EntityFlagged.json
gmr_event_schemas/                 # the Python package
  __init__.py
  loader.py                        # find/parse schemas at import time
  validate.py                      # `validate(event_type, version, payload)`
  builders.py                      # typed payload constructors
  events.py                        # EventEnvelope dataclass
tests/
  test_examples.py                 # every Schema has a known-good example
  examples/v1/<EventType>.json     # one example per event type
```

## Versioning rule

Additive-only changes within a major version (new optional
fields, new enum values). Anything that breaks an existing
producer or consumer requires a new directory `v2/` and a
documented migration path. Producers and consumers stamp
`schema_version` on every event; sinks fail loudly on unknown
versions.

## Adding a new event type

1. Drop a JSON Schema at `v<N>/<category>/<EventType>.json`.
2. Drop a known-good example at `tests/examples/v<N>/<EventType>.json`.
3. CI runs `pytest` which validates every example against its schema.
4. Add a typed builder helper in `gmr_event_schemas/builders.py`
   if you want type-checked emission from producer code.

## Consumed by

- `gmr-events` — the Python lib that producers and consumers
  use to emit/consume events.
- `edgar-gmr-etl`, `gmr-consolidator` — producers.
- `gmr-virtuoso-sink`, `gmr-neo4j-sink` — consumers.
