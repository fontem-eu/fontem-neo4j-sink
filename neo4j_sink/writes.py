"""The one write shape every renderer produces.

Its own module because both cypher.py and identity.py build them, and
having identity import from cypher (which imports identity for the
RENDERERS table) would be a cycle.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class CypherWrite:  # pylint: disable=too-many-instance-attributes
    # A write is a value bag by design; the guard/always pair is
    # what lets the sink express the Contract entity's high-water
    # semantics without a second write type.
    """One MERGE/SET statement worth of parameters."""
    label: str                  # 'Company', 'SanctionedEntity', 'Filing', …
    primary_key: dict           # {'gmr_id': '…'} or {'entity_id': '…'}
    set_props: dict             # the rest of the entity body
    extra_relationships: "list[tuple[str, str, dict]] | None" = None  # rel_type, target_iri, props
    extra_labels: list[str] | None = None  # secondary labels, e.g. ['Lobbyist']
    # Properties to REMOVE from the node. Needed because SET n += props
    # never deletes: a quarantined contract value that was rendered
    # before the quarantine event must be explicitly cleared.
    clear_props: list[str] | None = None
    # High-water-mark guard: when set, `set_props` may only be applied
    # when row.props.<guard_prop> >= the node's current <guard_prop>
    # (string comparison; ISO dates order correctly). The sink renders
    # a FOREACH-CASE conditional SET instead of a blind `SET n +=`, so
    # replay from seq 0 and out-of-order delivery converge on the same
    # state (the latest notice wins). Guarded writes are never
    # dict-collapsed in a batch — each row applies sequentially with
    # the guard, which IS the sequential per-event semantics.
    guard_prop: str | None = None
    # Props applied unconditionally even on a guarded write (e.g. the
    # award_value stamped by an award notice must survive a later
    # modification having already raised the high-water mark).
    always_props: dict | None = None
