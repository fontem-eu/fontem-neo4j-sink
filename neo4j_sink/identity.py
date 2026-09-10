"""Rendering the two identity events into Cypher.

Split out of cypher.py because identity is its own concern and because
these two renderers are the pair that has to stay consistent: whatever
AssertSameAs writes, RetractSameAs has to be able to take away.

The edge they maintain, :SAME_AS, is what the read path traverses to
resolve an identity class -- the graph-store equivalent of Virtuoso's
(owl:sameAs|^owl:sameAs)* property path. That is reachability over one
relationship type, not inference, which is why a store with no reasoner
can serve it.
"""

from __future__ import annotations

from neo4j_sink.writes import CypherWrite


def render_assert_same_as(p: dict) -> CypherWrite:
    """An APPROVED equivalence: a :SAME_AS edge between the two IRIs'
    Neo4j nodes. The sink resolves IRI -> (label, key) by parsing the
    IRI; deferred to the sink layer because it is coupled to the
    Virtuoso IRI scheme.

    Not a proposal. An unreviewed match is a :SAME_AS_CANDIDATE written
    by the consolidator directly and never reaches the event stream, so
    the existence of a :SAME_AS edge means the equivalence was asserted
    -- exactly what owl:sameAs means on the Virtuoso side.

    Why this is back
    ----------------
    c3e342c removed it because "nothing in Neo4j followed it": the edge
    was a second copy of a fact only Virtuoso acted on. That reasoning
    held right up until the read path started needing identity AND
    traversal in the same query.

    Federating the two stores per request was the alternative, and it is
    what the contract endpoints tried. It cost more than it bought:
    Virtuoso can express only ONE winner per contract (fontem:awardedTo
    is single-valued -- 122,863 triples over 122,863 subjects), against
    Neo4j's 190,123 AWARDED_TO edges, so 41% of company-contract pairs
    were invisible to a company page served from the triple store.

    So identity comes back to the graph that holds the edges, and the
    read path resolves the identity class with apoc.path.subgraphNodes
    over this type. The property-path closure it mirrors is
    (owl:sameAs|^owl:sameAs)* -- reachability, not inference, which is
    why a plain graph store can serve it.

    The type is what makes this safe to traverse. :SAME_AS_CANDIDATE
    mixes 39,193 approved with 304,702 pending; a traversal filtered on
    a relationship PROPERTY is one forgotten predicate away from merging
    eight times too much. Only asserted equivalences ever get this type.
    """
    return CypherWrite(
        label="_SameAs",  # virtual; the sink handles this specially
        primary_key={
            "a_iri": p["a_iri"],
            "b_iri": p["b_iri"],
        },
        set_props={
            "confidence": p["confidence"],
            "method": p["method"],
            "tier": p.get("tier"),
            "matched_via_alias": p.get("matched_via_alias", False),
            "rule": p.get("rule"),
        },
    )


def render_retract_same_as(p: dict) -> CypherWrite:
    """Withdraws an equivalence that was asserted and turned out wrong.

    Virtuoso drops the owl:sameAs. Neo4j's part is the durable block:
    :NOT_SAME_AS, so the consolidator's rules will not re-propose the
    pair — they are deterministic and would otherwise reach the same
    wrong conclusion on the next sweep — plus clearing the settled
    candidate so the queue does not re-offer it.
    """
    return CypherWrite(
        label="_NotSameAs",  # virtual; the sink handles this specially
        primary_key={
            "a_iri": p["a_iri"],
            "b_iri": p["b_iri"],
        },
        set_props={
            "reason": p["reason"],
            "reviewer": p.get("reviewer"),
            "retracted_method": p.get("retracted_method"),
        },
    )
