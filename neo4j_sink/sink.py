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


def _merge_clears(cur, w) -> None:
    """Sequential-apply equivalence for clears: a later SET revives a
    field an earlier event cleared, and a later clear removes a field
    an earlier event set."""
    if cur.clear_props:
        cur.clear_props = [f for f in cur.clear_props
                           if f not in w.set_props]
    if w.clear_props:
        cur.clear_props = sorted(
            set(cur.clear_props or []) | set(w.clear_props))
        for f in w.clear_props:
            cur.set_props.pop(f, None)


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
        # Non-bracketed writes are accumulated and applied in one
        # UNWIND-batched pass per handle() (was a session.run() per
        # event — the per-relationship MATCH+MERGE that pinned the
        # drain to ~33 ev/s while Neo4j sat ~90% idle).
        pending: list[CypherWrite] = []

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
                # Apply pending non-bracketed writes that arrived
                # before this End first — the old per-event path
                # applied them immediately, i.e. before the bracket
                # flush, so a bracket edge targeting one still resolves.
                if pending:
                    self._apply_batch(pending)
                    pending = []
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
            if ev.op == "delete":
                # Entity deletes are a Virtuoso-side concern (subject
                # drop); rendering a delete's {"iri"} payload here
                # would KeyError on the schema fields. Skip, never
                # poison the drain.
                logger.debug("ignoring delete op for %s", ev.iri)
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
                pending.append(write)

        if pending:
            self._apply_batch(pending)

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

    # ── InvestmentFund relabel semantics ──────────────────────────
    #
    # Funds share the Company gmr_id namespace (same UUID5 derivation)
    # and usually FIRST enter the graph as :Company (GLEIF/EDGAR load
    # everything with an LEI). UpsertInvestmentFund therefore means
    # "this entity IS a fund": relabel any existing :Company node in
    # place — keeping the node, its properties and every edge — and
    # only then MERGE, so the identity never splits into two nodes.
    # The reverse never happens implicitly: a later UpsertCompany for
    # a relabeled fund refreshes the node's properties but does NOT
    # relabel it back — OpenFIGI instrument evidence outranks the
    # kind-agnostic GLEIF/EDGAR company refresh.
    _IFUND_MERGE_CYPHER = (
        "UNWIND $rows AS row "
        "OPTIONAL MATCH (c:Company {gmr_id: row.gmr_id}) "
        "FOREACH (x IN CASE WHEN c IS NULL THEN [] ELSE [c] END | "
        "SET x:InvestmentFund REMOVE x:Company) "
        "WITH row "
        "MERGE (n:InvestmentFund {gmr_id: row.gmr_id}) "
        "SET n += row.props"
    )
    _COMPANY_REFRESH_FUND_CYPHER = (
        "UNWIND $rows AS row "
        "MATCH (f:InvestmentFund {gmr_id: row.gmr_id}) "
        "SET f += row.props"
    )
    _COMPANY_MERGE_NON_FUND_CYPHER = (
        "UNWIND $rows AS row "
        "WITH row WHERE NOT EXISTS { "
        "MATCH (:InvestmentFund {gmr_id: row.gmr_id}) } "
        "MERGE (n:Company {gmr_id: row.gmr_id}) "
        "SET n += row.props"
    )

    @staticmethod
    def _match_label(label: str) -> str:
        """Label expression for MATCHing an entity by graph identity.
        Corporate gmr_ids can live under :Company or :InvestmentFund
        (relabeled funds keep their gmr_id), so Company-IRI targets
        must match either label or fund edges silently vanish."""
        return "Company|InvestmentFund" if label == "Company" else label

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

    def _apply_batch(self, writes: list[CypherWrite]) -> None:
        """Apply a run of non-bracketed writes in UNWIND-grouped passes
        instead of one session.run() per event. Order within the batch
        is preserved: nodes are MERGEd first (so same-batch edges
        resolve), then their extra-relationships, then typed
        relationships, then SameAs."""
        same_as = [w for w in writes if w.label == "_SameAs"]
        typed_rels = [w for w in writes if w.label == "_Relationship"]
        nodes = [w for w in writes
                 if w.label not in ("_SameAs", "_Relationship")]
        rel_items = self._flush_nodes(nodes)
        self._flush_extra_relationships(rel_items)
        self._flush_typed_relationships(typed_rels)
        for w in same_as:
            self._apply_same_as(w)

    @staticmethod
    def _collapse_node_writes(nodes: list[CypherWrite]) -> list[CypherWrite]:
        """Merge same-(label, primary_key) writes in arrival (seq)
        order: later set_props win per field, labels union, edges
        concatenate — so the node is MERGEd once with the same result
        a sequential per-event apply would give."""
        collapsed: dict = {}
        order: list = []
        for w in nodes:
            key = (w.label, tuple(sorted(w.primary_key.items())))
            cur = collapsed.get(key)
            if cur is None:
                collapsed[key] = CypherWrite(
                    label=w.label,
                    primary_key=dict(w.primary_key),
                    set_props=dict(w.set_props),
                    extra_relationships=list(w.extra_relationships or []),
                    extra_labels=list(w.extra_labels or []),
                    clear_props=list(w.clear_props or []) or None,
                )
                order.append(key)
                continue
            cur.set_props.update(w.set_props)
            _merge_clears(cur, w)
            if w.extra_labels:
                cur.extra_labels = sorted(
                    set(cur.extra_labels) | set(w.extra_labels))
            if w.extra_relationships:
                cur.extra_relationships.extend(w.extra_relationships)
        return [collapsed[k] for k in order]

    def _flush_nodes(self, nodes: list[CypherWrite]) -> list:
        """UNWIND-MERGE node writes grouped by (label, key-shape,
        extra-labels), collapsing same-key writes first. Returns the
        flattened extra-relationship items to flush next."""
        merged = self._collapse_node_writes(nodes)
        groups: dict = defaultdict(list)
        for w in merged:
            gkey = (w.label,
                    tuple(sorted(w.primary_key.keys())),
                    tuple(w.extra_labels or []),
                    tuple(w.clear_props or []))
            groups[gkey].append(w)
        for (label, keyset, extra_labels, clear_props), ws in groups.items():
            self._unwind_merge_nodes(label, keyset, list(extra_labels), ws,
                                     clear_props=list(clear_props))
        rel_items: list = []
        for w in merged:
            for rel_type, target_iri, props in (w.extra_relationships or []):
                rel_items.append(
                    (w.label, w.primary_key, rel_type, target_iri, props))
        return rel_items

    def _unwind_merge_nodes(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, label, keyset, extra_labels, writes, clear_props=None,
    ) -> None:
        key_match = ", ".join(f"{k}: row.{k}" for k in keyset)
        rows = []
        for w in writes:
            row = {k: w.primary_key[k] for k in keyset}
            row["props"] = w.set_props
            rows.append(row)
        if label == "InvestmentFund":
            with self._driver.session() as session:
                session.run(self._IFUND_MERGE_CYPHER, rows=rows)
            return
        if label == "Company":
            clean = (
                " SET n.name_clean = CASE WHEN row.props.name IS NOT NULL "
                "THEN apoc.text.clean(row.props.name) ELSE n.name_clean END"
            )
            with self._driver.session() as session:
                session.run(self._COMPANY_REFRESH_FUND_CYPHER, rows=rows)
                session.run(
                    self._COMPANY_MERGE_NON_FUND_CYPHER + clean
                    + self._label_set_clause(extra_labels),
                    rows=rows,
                )
            return
        clear_clause = ""
        if clear_props:
            # SET += never deletes; quarantined values rendered before
            # the quarantine event must be removed explicitly.
            clear_clause = " REMOVE " + ", ".join(
                f"n.{f}" for f in clear_props)
        cypher = (
            f"UNWIND $rows AS row "
            f"MERGE (n:{label} {{ {key_match} }}) "
            f"SET n += row.props"
            f"{clear_clause}"
        )
        if label in ("Company", "Authority"):
            # Materialise name_clean for the resolver range index, but
            # only when this row carries a name — rows in one group can
            # now vary since SET uses += row.props.
            cypher += (
                " SET n.name_clean = CASE WHEN row.props.name IS NOT NULL "
                "THEN apoc.text.clean(row.props.name) ELSE n.name_clean END"
            )
        cypher += self._label_set_clause(extra_labels)
        with self._driver.session() as session:
            session.run(cypher, rows=rows)

    def _flush_extra_relationships(  # pylint: disable=too-many-locals
        self, rel_items: list,
    ) -> None:
        """Batch the Company/Authority/Contract extra-relationship edges
        by (src_label, src-key-shape, tgt_label, rel_type, direction)
        into one UNWIND MATCH+MERGE per group."""
        groups: dict = defaultdict(list)
        for src_label, src_key, rel_type, target_iri, props in rel_items:
            tgt_label, tgt_key = self._iri_to_label_key(target_iri)
            if (tgt_label == src_label
                    and src_key.get(self._key_field(src_label)) == tgt_key):
                continue
            edge_props = dict(props)
            direction = edge_props.pop("_direction", "from_source")
            gkey = (src_label, tuple(sorted(src_key.keys())),
                    tgt_label, rel_type, direction)
            groups[gkey].append((src_key, tgt_key, edge_props))
        for gkey, items in groups.items():
            src_label, src_keyset, tgt_label, rel_type, direction = gkey
            src_match = ", ".join(f"{k}: row.{k}" for k in src_keyset)
            tgt_field = self._key_field(tgt_label)
            if direction == "from_target":
                arrow = f"(t)-[r:{rel_type}]->(s)"
            else:
                arrow = f"(s)-[r:{rel_type}]->(t)"
            cypher = (
                f"UNWIND $rows AS row "
                f"MATCH (s:{src_label} {{ {src_match} }}) "
                f"MATCH (t:{self._match_label(tgt_label)} "
                f"{{ {tgt_field}: row.tgt_key }}) "
                f"MERGE {arrow} "
                f"SET r += row.props"
            )
            rows = [{**src_key, "tgt_key": tgt_key, "props": p}
                    for src_key, tgt_key, p in items]
            with self._driver.session() as session:
                session.run(cypher, rows=rows)

    def _flush_typed_relationships(  # pylint: disable=too-many-locals
        self, writes: list[CypherWrite],
    ) -> None:
        """Batch UpsertRelationship (_Relationship) events by
        (src_label, dst_label, rel_type) into one UNWIND MATCH+MERGE
        per group."""
        groups: dict = defaultdict(list)
        for w in writes:
            a_label, a_key = self._iri_to_label_key(w.primary_key["src_iri"])
            b_label, b_key = self._iri_to_label_key(w.primary_key["dst_iri"])
            if (a_label, a_key) == (b_label, b_key):
                continue
            rel_type = self._predicate_to_rel_type(w.primary_key["predicate"])
            groups[(a_label, b_label, rel_type)].append(
                (a_key, b_key, w.set_props))
        for (a_label, b_label, rel_type), items in groups.items():
            a_field = self._key_field(a_label)
            b_field = self._key_field(b_label)
            cypher = (
                f"UNWIND $rows AS row "
                f"MATCH (a:{self._match_label(a_label)} {{ {a_field}: row.ak }}) "
                f"MATCH (b:{self._match_label(b_label)} {{ {b_field}: row.bk }}) "
                f"MERGE (a)-[r:{rel_type}]->(b) "
                f"SET r += row.props"
            )
            rows = [{"ak": ak, "bk": bk, "props": props}
                    for ak, bk, props in items]
            with self._driver.session() as session:
                session.run(cypher, rows=rows)

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
            + ((" REMOVE " + ", ".join(f"n.{f}" for f in w.clear_props))
               if w.clear_props else "")
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
                f"MATCH (a:{self._match_label(a_label)}), "
                f"(b:{self._match_label(b_label)}) "
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
        # Never create a self-relationship (e.g. GLEIF self-consolidation,
        # where a company is reported as SUBSIDIARY_OF itself).
        if (a_label, a_key) == (b_label, b_key):
            return
        rel_type = self._predicate_to_rel_type(w.primary_key["predicate"])
        a_field = self._key_field(a_label)
        b_field = self._key_field(b_label)
        with self._driver.session() as session:
            session.run(
                f"MATCH (a:{self._match_label(a_label)}), "
                f"(b:{self._match_label(b_label)}) "
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
        # Guard against a self-edge (src node == target node).
        if tgt_label == src_label and src_key.get(self._key_field(src_label)) == tgt_key:
            return
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
        "Programme": "code",
        "Fund": "code",
        "Mic": "code",
        "FirdsInstrument": "code",
        "ExchangeRate": "date",  # composite key, but date is the discriminator
    }

    @classmethod
    def _key_field(cls, label: str) -> str:
        return cls._KEY_FIELD_BY_LABEL.get(label, "gmr_id")
