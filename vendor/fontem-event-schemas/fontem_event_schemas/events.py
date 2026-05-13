"""Envelope dataclass — the row that lands in events.entity_events.

The actual insert is in ``fontem_events`` (separate package) so this
file stays dependency-light. Producers use this to build envelopes;
consumers receive them.
"""
from __future__ import annotations

import datetime as dt
import uuid
from dataclasses import dataclass, field
from typing import Any


@dataclass
class EventEnvelope:
    """One row of events.entity_events."""

    event_type: str           # 'UpsertCompany', 'AssertSameAs', etc.
    iri: str                  # Canonical subject IRI (single-subject events)
    domain: str               # 'company', 'sanctions', 'consolidation', …
    op: str                   # 'upsert' | 'delete' | 'control'
    payload: dict[str, Any]
    producer: str             # 'load_eu_sanctions', 'consolidator', …
    schema_version: int = 1
    batch_id: uuid.UUID | None = None
    ts: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.UTC))
    seq: int | None = None    # filled in after insert by Postgres

    def with_seq(self, seq: int) -> "EventEnvelope":
        """Return a copy with seq populated. Sinks use this to ack."""
        return EventEnvelope(**{**self.__dict__, "seq": seq})
