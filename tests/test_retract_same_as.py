"""AssertSameAs / RetractSameAs, and what a :SAME_AS edge means here.

The sink once stamped `reviewed = false` on every :SAME_AS it wrote,
because AssertSameAs meant "the consolidator matched these" and the
review queue read the edge directly. That made a guess and a conclusion
the same shape: anything traversing :SAME_AS could not tell them apart.
That stamp is gone for good — proposals are :SAME_AS_CANDIDATE, written
by the consolidator and never emitted, so the existence of a :SAME_AS
edge means the equivalence was asserted.

The edge itself is back. c3e342c removed it because nothing in Neo4j
followed it; the contract endpoints and the graph explorer now do,
resolving an identity class by traversing the type — the same closure
Virtuoso expresses as (owl:sameAs|^owl:sameAs)*. Which makes the
retraction path load-bearing: an edge left behind after a reviewer
separates two entities would keep them merged on every view.
"""

from unittest.mock import MagicMock

from neo4j_sink.cypher import RENDERERS, render_retract_same_as

A = "http://data.fontem.eu/id/Company/aaaa"
B = "http://data.fontem.eu/id/Company/bbbb"


def _sink_with_session():
    """A VirtuosoSink-shaped double that records the Cypher it runs."""
    from neo4j_sink.sink import Neo4jSink  # pylint: disable=import-outside-toplevel

    sink = Neo4jSink.__new__(Neo4jSink)
    session = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=session)
    ctx.__exit__ = MagicMock(return_value=None)
    driver = MagicMock()
    driver.session = MagicMock(return_value=ctx)
    sink._driver = driver  # pylint: disable=protected-access
    return sink, session


def test_retract_renderer_is_registered():
    assert RENDERERS["RetractSameAs"] is render_retract_same_as


def test_retract_renderer_carries_the_correction_provenance():
    w = render_retract_same_as({
        "a_iri": A, "b_iri": B,
        "reason": "different registration numbers",
        "reviewer": "review@fontem.eu",
        "retracted_method": "exact_name_country_match",
    })
    assert w.label == "_NotSameAs"
    assert w.primary_key == {"a_iri": A, "b_iri": B}
    assert w.set_props["reason"] == "different registration numbers"
    assert w.set_props["retracted_method"] == "exact_name_country_match"


def test_assert_same_as_writes_a_same_as_edge():
    """The assertion reaches Neo4j again, as a :SAME_AS edge, MERGEd
    undirected-agnostically between the two resolved nodes."""
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": B, "confidence": 0.97, "method": "lei_match",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "MERGE (a)-[r:SAME_AS]->(b)" in cypher
    assert "SAME_AS_CANDIDATE" not in cypher


def test_assert_same_as_never_stamps_reviewed():
    """The regression that justified deleting the edge in the first
    place. A traversal that merges entities along this type must not be
    fed rows that say a machine guess was reviewed by a person."""
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": B, "confidence": 0.5, "method": "fuzzy",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    assert "reviewed" not in session.run.call_args[0][0]
    assert "reviewed" not in (session.run.call_args[1].get("props") or {})


def test_assert_same_as_does_not_create_missing_endpoints():
    """MATCH-only. :SAME_AS is derived from entities the consolidator
    read out of this graph, so if an endpoint is gone the assertion is
    void — stubbing one in would invent a node to merge into."""
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": B, "confidence": 0.9, "method": "lei_match",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert cypher.startswith("MATCH")
    assert "MERGE (a:" not in cypher and "MERGE (b:" not in cypher


def test_assert_same_as_skips_a_self_reference():
    """A self-loop carries no information and would make an
    identity-class traversal revisit its own start node."""
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": A, "confidence": 0.9, "method": "lei_match",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    session.run.assert_not_called()


def test_assert_same_as_skips_across_labels():
    """A Company and an Authority are not the same entity, and merging
    across labels would corrupt every traversal that follows the type."""
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": "http://data.fontem.eu/id/Authority/bbbb",
        "confidence": 0.9, "method": "lei_match",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    session.run.assert_not_called()


def test_retraction_clears_the_candidate_and_blocks_the_pair():
    """Both are required. Clearing the settled candidate stops the queue
    re-offering it; :NOT_SAME_AS stops the rules — which are
    deterministic — re-proposing the identical pair on the next sweep,
    which would silently undo the correction.

    The assertion is deleted too, now that it exists here as well. A
    :SAME_AS left behind would keep the identity-class traversal merging
    two entities a reviewer has just separated.
    """
    sink, session = _sink_with_session()
    w = render_retract_same_as({
        "a_iri": A, "b_iri": B, "reason": "wrong", "reviewer": "x@fontem.eu",
    })
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "DELETE c" in cypher
    assert "NOT_SAME_AS" in cypher
    # The assertion goes as well, undirected for the same reason the
    # candidate match is.
    assert "(a)-[s:SAME_AS]-(b) DELETE s" in cypher
    assert "(a)-[s:SAME_AS]->(b)" not in cypher


def test_retraction_is_direction_agnostic():
    """Which side the consolidator treated as source is arbitrary, so a
    directed match would miss half the candidates."""
    sink, session = _sink_with_session()
    w = render_retract_same_as({"a_iri": A, "b_iri": B, "reason": "wrong"})
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "(a)-[c:SAME_AS_CANDIDATE]-(b)" in cypher
    assert "(a)-[c:SAME_AS_CANDIDATE]->(b)" not in cypher


def test_cross_label_retraction_is_skipped():
    sink, session = _sink_with_session()
    w = render_retract_same_as({
        "a_iri": A,
        "b_iri": "http://data.fontem.eu/id/Authority/bbbb",
        "reason": "wrong",
    })
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    session.run.assert_not_called()


def test_self_retraction_is_skipped():
    sink, session = _sink_with_session()
    w = render_retract_same_as({"a_iri": A, "b_iri": A, "reason": "wrong"})
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    session.run.assert_not_called()
