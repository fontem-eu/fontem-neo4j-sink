"""Neo4j sink — projects events.entity_events into Cypher.

Two write paths mirror the Virtuoso sink:

  1. Inside a Begin/EndGraphReplace bracket: accumulate writes,
     prefix with a `DETACH DELETE` for the matching label, then
     UNWIND-MERGE the new batch. Single Neo4j transaction.

  2. Outside a bracket: per-event MERGE in its own transaction.

The IRI-to-Neo4j-key mapping is parsed from the canonical
fontem-id IRIs (e.g. http://data.fontem.eu/id/Company/<gmr_id>).
"""
from __future__ import annotations

import logging
import re
import os
from collections import defaultdict

from fontem_event_schemas import EventEnvelope
from fontem_events import EventConsumer
from neo4j import GraphDatabase

from .cypher import RENDERERS, CypherWrite, label_for_graph

logger = logging.getLogger(__name__)


class Neo4jSink(EventConsumer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._driver = GraphDatabase.driver(
            os.environ["NEO4J_URI"],
            auth=(
                os.environ.get("NEO4J_USER", "neo4j"),
                os.environ["NEO4J_PASSWORD"],
            ),
        )
        # Bracket state is sink-instance scope, NOT per-handle().
        # A single Begin/EndGraphReplace bracket can span many
        # handle() calls because batch_size caps each fetch.
        self._bracket_writes: dict[str, list[CypherWrite]] = defaultdict(list)
        self._bracket_label: dict[str, str] = {}

    def handle(self, batch: list[EventEnvelope]) -> None:
        bracket_writes = self._bracket_writes
        bracket_label = self._bracket_label

        for ev in batch:
            if ev.event_type == "BeginGraphReplace":
                graph = ev.payload["graph_iri"]
                label = ev.payload.get("label") or label_for_graph(graph)
                if label is None:
                    raise RuntimeError(
                        f"BeginGraphReplace for {graph} but no label "
                        f"resolvable; cannot bulk-delete safely"
                    )
                bracket_writes[graph] = []
                bracket_label[graph] = label
                logger.info("bracket-begin %s (label %s)", graph, label)
                continue

            if ev.event_type == "EndGraphReplace":
                graph = ev.payload["graph_iri"]
                writes = bracket_writes.pop(graph, None)
                label = bracket_label.pop(graph, None)
                if writes is None:
                    logger.warning(
                        "EndGraphReplace for %s without matching "
                        "Begin; treating as no-op", graph,
                    )
                    continue
                self._flush_bracket(label, writes)
                continue

            renderer = RENDERERS.get(ev.event_type)
            if renderer is None:
                continue
            write = renderer(ev.payload)
            if write is None:
                continue

            # Find an open bracket whose label matches this event.
            target = self._find_open_bracket(
                bracket_writes, bracket_label, write.label,
            )
            if target is not None:
                bracket_writes[target].append(write)
            else:
                self._apply_one(write)

    # ── implementation ────────────────────────────────────

    @staticmethod
    def _find_open_bracket(
        writes: dict[str, list[CypherWrite]],
        labels: dict[str, str],
        target_label: str,
    ) -> str | None:
        candidates = [g for g, lbl in labels.items() if lbl == target_label]
        if len(candidates) == 1:
            return candidates[0]
        if len(writes) == 1:
            return next(iter(writes))
        return None

    @staticmethod
    def _name_clean_fragment(label: str, has_name: bool) -> str:
        """For Company/Authority writes that include `name`, also
        materialise `name_clean = apoc.text.clean(name)`. This is
        the property the consolidator's resolver + dedup rules now
        look up via a range index — without this fragment they'd
        keep doing the function-on-property full scan that pinned
        each consolidate() to ~10s.

        Returns a Cypher SET fragment to append, or "" if N/A.
        """
        if label not in ("Company", "Authority") or not has_name:
            return ""
        return ", n.name_clean = apoc.text.clean(row.name)"

    def _flush_bracket(  # pylint: disable=too-many-locals

        self, label: str, writes: list[CypherWrite],
    ) -> None:
        if label == "_SameAs":
            self._flush_same_as_bracket(writes)
            return
        # PUT-replace semantics: drop everything with this label,
        # then MERGE the new set in.
        with self._driver.session() as session:
            session.run(
                f"""
                CALL {{ MATCH (n:{label}) DETACH DELETE n }}
                IN TRANSACTIONS OF 1000 ROWS
                """
            )
        # Group by primary-key shape (some labels mix); we expect
        # one shape per label so this is just a defensive bucket.
        if not writes:
            return
        first_keyset = tuple(sorted(writes[0].primary_key.keys()))
        rows = []
        for w in writes:
            row = {**w.primary_key, **w.set_props}
            rows.append(row)

        key_match = ", ".join(
            f"{k}: row.{k}" for k in first_keyset
        )
        set_clause = ", ".join(
            f"n.{k} = row.{k}"
            for k in writes[0].set_props.keys()
        ) or "n.gmr_id = n.gmr_id"  # no-op if no extra fields
        set_clause += self._name_clean_fragment(
            label, "name" in writes[0].set_props,
        )

        common_labels = set(writes[0].extra_labels or [])
        for _w in writes[1:]:
            common_labels &= set(_w.extra_labels or [])
        cypher = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{ {key_match} }}) "
            f"SET {set_clause}"
            + self._label_set_clause(sorted(common_labels))
        )
        with self._driver.session() as session:
            session.run(cypher, rows=rows)

        # Apply extra_relationships separately. For Filing this
        # is the REPORTED edge from Company → Filing.
        for w in writes:
            for rel_type, target_iri, props in (w.extra_relationships or []):
                self._apply_relationship(label, w.primary_key,
                                         rel_type, target_iri, props)

    def _flush_same_as_bracket(self, writes: list[CypherWrite]) -> None:
        # SameAs has no PUT-replace semantics; bracketed delivery
        # is just a batching hint. We don't pre-delete.
        for w in writes:
            self._apply_same_as(w)

    def _apply_one(self, w: CypherWrite) -> None:
        """Per-event MERGE for events delivered outside a
        bracket (consolidator outputs, etc.)."""
        if w.label == "_SameAs":
            self._apply_same_as(w)
            return
        if w.label == "_Relationship":
            self._apply_typed_relationship(w)
            return

        keyset = tuple(sorted(w.primary_key.keys()))
        key_match = ", ".join(f"{k}: ${k}" for k in keyset)
        set_clause = ", ".join(
            f"n.{k} = ${k}_val" for k in w.set_props.keys()
        )
        # Append the name_clean materialisation when applicable.
        # The fragment uses $name_val (already in params from the
        # set_props loop above) so no extra parameter wiring needed.
        clean_frag = self._name_clean_fragment(
            w.label, "name" in w.set_props,
        )
        if clean_frag:
            # Per-event MERGE uses $param_val style, not row.X — fix
            # the fragment for that case.
            clean_frag = clean_frag.replace("row.name", "$name_val")
            set_clause = (set_clause + clean_frag) if set_clause else clean_frag.lstrip(", ")
        params = {**w.primary_key,
                  **{f"{k}_val": v for k, v in w.set_props.items()}}
        cypher = (
            f"MERGE (n:{w.label} {{ {key_match} }}) "
            + (f"SET {set_clause}" if set_clause else "")
            + self._label_set_clause(w.extra_labels)
        )
        with self._driver.session() as session:
            session.run(cypher, params)
        for rel_type, target_iri, props in (w.extra_relationships or []):
            self._apply_relationship(w.label, w.primary_key,
                                     rel_type, target_iri, props)

    def _apply_same_as(self, w: CypherWrite) -> None:
        # IRIs are http://data.fontem.eu/id/<Label>/<key>. Parse
        # them into label + key for the MATCH.
        a_label, a_key = self._iri_to_label_key(w.primary_key["a_iri"])
        b_label, b_key = self._iri_to_label_key(w.primary_key["b_iri"])
        if a_label != b_label:
            logger.warning(
                "AssertSameAs across labels (%s vs %s); skipping",
                a_label, b_label,
            )
            return
        with self._driver.session() as session:
            session.run(
                f"MATCH (a:{a_label}), (b:{b_label}) "
                f"WHERE a.{self._key_field(a_label)} = $ak "
                f"  AND b.{self._key_field(b_label)} = $bk "
                f"MERGE (a)-[r:SAME_AS]->(b) "
                f"ON CREATE SET r += $props, r.reviewed = false",
                ak=a_key, bk=b_key, props=w.set_props,
            )

    def _apply_typed_relationship(self, w: CypherWrite) -> None:
        """UpsertRelationship → resolve both IRIs to (label, key)
        and MERGE a relationship of the given predicate name. The
        predicate is sanitised (alphanumeric + underscore) and
        UPPER_SNAKE_CASE'd to match Neo4j relationship-type
        conventions."""
        a_label, a_key = self._iri_to_label_key(w.primary_key["src_iri"])
        b_label, b_key = self._iri_to_label_key(w.primary_key["dst_iri"])
        rel_type = self._predicate_to_rel_type(w.primary_key["predicate"])
        a_field = self._key_field(a_label)
        b_field = self._key_field(b_label)
        with self._driver.session() as session:
            session.run(
                f"MATCH (a:{a_label}), (b:{b_label}) "
                f"WHERE a.{a_field} = $ak AND b.{b_field} = $bk "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                f"SET r += $props",
                ak=a_key, bk=b_key, props=w.set_props,
            )

    @staticmethod
    def _label_set_clause(extra_labels) -> str:
        """Cypher fragment that adds sanitised secondary labels to `n`.
        Labels can't be parameterised, so they're interpolated — hence
        the strict alphanumeric/underscore filter."""
        if not extra_labels:
            return ""
        safe = [re.sub(r"\W", "", lbl) for lbl in extra_labels]
        return "".join(f" SET n:`{lbl}`" for lbl in safe if lbl)

    @staticmethod
    def _predicate_to_rel_type(predicate: str) -> str:
        """Map a fontem ontology predicate to a Neo4j rel-type name.
        ``parentOf`` → ``PARENT_OF``; ``http://.../represents`` → ``REPRESENTS``."""
        # Strip any IRI prefix.
        local = predicate.rsplit("#", 1)[-1].rsplit("/", 1)[-1]
        # camelCase → UPPER_SNAKE_CASE.
        out = []
        for ch in local:
            if ch.isupper() and out and out[-1].islower():
                out.append("_")
            out.append(ch)
        cleaned = "".join(c if c.isalnum() else "_" for c in "".join(out))
        return cleaned.upper().strip("_") or "RELATED_TO"

    def _apply_relationship(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, src_label: str, src_key: dict,
        rel_type: str, target_iri: str, props: dict,
    ) -> None:
        # All five params carry distinct, non-mergeable meaning:
        # src_label + src_key locate the source node, rel_type names
        # the edge, target_iri identifies the destination, props are
        # edge attributes. A dataclass wrapper just renames the
        # indirection at every call site.
        tgt_label, tgt_key = self._iri_to_label_key(target_iri)
        direction = props.pop("_direction", "from_source")
        src_match = ", ".join(f"{k}: ${k}" for k in src_key.keys())
        tgt_field = self._key_field(tgt_label)
        if direction == "from_target":
            cypher = (
                f"MATCH (s:{src_label} {{ {src_match} }}) "
                f"MATCH (t:{tgt_label} {{ {tgt_field}: $tgt_key }}) "
                f"MERGE (t)-[r:{rel_type}]->(s) "
                f"SET r += $props"
            )
        else:
            cypher = (
                f"MATCH (s:{src_label} {{ {src_match} }}) "
                f"MATCH (t:{tgt_label} {{ {tgt_field}: $tgt_key }}) "
                f"MERGE (s)-[r:{rel_type}]->(t) "
                f"SET r += $props"
            )
        with self._driver.session() as session:
            session.run(cypher, **src_key, tgt_key=tgt_key, props=props)

    @staticmethod
    def _iri_to_label_key(iri: str) -> tuple[str, str]:
        # http://data.fontem.eu/id/<Label>/<key>
        suffix = iri.rsplit("/id/", 1)[-1]
        label, _, key = suffix.partition("/")
        return label, key

    _KEY_FIELD_BY_LABEL = {
        "SanctionedEntity": "entity_id",
        "Sanction": "entity_id",
        "Filing": "filing_iri",
        "Listing": "ticker",
        "Authority": "authority_id",
        "Contract": "ted_notice_id",
        "Cpv": "code",
        "Nuts": "code",
        "Mic": "code",
        "FirdsInstrument": "code",
        "ExchangeRate": "date",  # composite key, but date is the discriminator
    }

    @classmethod
    def _key_field(cls, label: str) -> str:
        return cls._KEY_FIELD_BY_LABEL.get(label, "gmr_id")
