"""RetractSameAs, and what a SAME_AS edge means in this graph.

The sink used to stamp `reviewed = false` on every SAME_AS it wrote,
because AssertSameAs meant "the consolidator matched these" and the
review queue read the edge directly. That made a guess and a conclusion
the same shape: anything traversing SAME_AS could not tell them apart.

Neo4j now writes no equivalences at all. Identity lives in Virtuoso,
where owl:sameAs is closed transitively and symmetrically by the store;
a SAME_AS edge here was a second copy of that fact which nothing
followed. Neo4j keeps the graph and the review workflow —
:SAME_AS_CANDIDATE and :NOT_SAME_AS — and a query needing traversal AND
identity federates across the two stores.
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


def test_assert_same_as_writes_nothing_to_neo4j():
    """Identity lives in Virtuoso. This sink used to write a SAME_AS
    edge stamped `reviewed = false`, which both duplicated a fact
    Virtuoso already held and made every guess look like a conclusion to
    anything traversing the type."""
    assert RENDERERS["AssertSameAs"] is None


def test_retraction_clears_the_candidate_and_blocks_the_pair():
    """Both are required. Clearing the settled candidate stops the queue
    re-offering it; :NOT_SAME_AS stops the rules — which are
    deterministic — re-proposing the identical pair on the next sweep,
    which would silently undo the correction.

    There is no :SAME_AS to delete: the assertion only ever existed as
    an owl:sameAs triple, dropped by the virtuoso sink handling the very
    same event.
    """
    sink, session = _sink_with_session()
    w = render_retract_same_as({
        "a_iri": A, "b_iri": B, "reason": "wrong", "reviewer": "x@fontem.eu",
    })
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "DELETE c" in cypher
    assert "NOT_SAME_AS" in cypher
    assert "[s:SAME_AS]" not in cypher


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
