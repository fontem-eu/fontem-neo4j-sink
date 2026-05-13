"""fontem_event_schemas — JSON Schemas + helpers for the gmr_app event log.

Every event written to ``events.entity_events`` (in gmr_app) has a
typed schema that lives under ``v<N>/<category>/<EventType>.json``.
This package exposes the schemas at import time and gives producers
+ consumers two affordances:

  * ``validate(event_type, version, payload)`` — strict validation;
    raises ``EventValidationError`` on mismatch. Sinks call this on
    every event before applying it; producers call it before emit.
  * ``builders.upsert_sanctioned_entity(...)`` etc. — typed
    constructors that build a payload conforming to the schema. Saves
    every producer from re-deriving the field set.

The full event envelope (the row written to entity_events) is the
``EventEnvelope`` dataclass in ``events`` — see ``fontem_events`` for
the runtime that actually inserts rows.
"""
from .events import EventEnvelope
from .loader import (
    SCHEMA_ROOT,
    load_schema,
    available_event_types,
)
from .validate import (
    EventValidationError,
    validate,
)

__all__ = [
    "EventEnvelope",
    "EventValidationError",
    "SCHEMA_ROOT",
    "available_event_types",
    "load_schema",
    "validate",
]
