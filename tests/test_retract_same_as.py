"""RetractSameAs, and what a SAME_AS edge means in this graph.

The sink used to stamp `reviewed = false` on every SAME_AS it wrote,
because AssertSameAs meant "the consolidator matched these" and the
review queue read the edge directly. That made a guess and a conclusion
the same shape: anything traversing SAME_AS — the queue, the graph API,
the GDS cluster-collapse rule that MERGES what it finds — could not tell
them apart.

Proposals are now :SAME_AS_CANDIDATE, written by the consolidator and
never emitted. A SAME_AS edge here means the equivalence was approved,
and RetractSameAs is how one is withdrawn.
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


def test_assert_no_longer_stamps_reviewed():
    """The conflation, in one line of Cypher.

    `reviewed = false` on an asserted edge is what made the review queue
    read assertions as proposals.
    """
    sink, session = _sink_with_session()
    w = RENDERERS["AssertSameAs"]({
        "a_iri": A, "b_iri": B, "confidence": 0.99, "method": "exact_lei_match",
    })
    sink._apply_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "SAME_AS" in cypher
    assert "reviewed" not in cypher


def test_retraction_deletes_assertion_and_candidate_and_blocks():
    """All three are required.

    Deleting the SAME_AS alone leaves the candidate to be re-offered;
    deleting both without :NOT_SAME_AS lets the rules — which are
    deterministic — re-propose the identical pair on the next sweep, so
    the correction silently undoes itself.
    """
    sink, session = _sink_with_session()
    w = render_retract_same_as({
        "a_iri": A, "b_iri": B, "reason": "wrong", "reviewer": "x@fontem.eu",
    })
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "DELETE s" in cypher
    assert "DELETE c" in cypher
    assert "NOT_SAME_AS" in cypher


def test_retraction_is_direction_agnostic():
    """Which way the assertion was written depends on which side the
    consolidator treated as source, so a directed match would miss half
    of them and leave the equivalence standing."""
    sink, session = _sink_with_session()
    w = render_retract_same_as({"a_iri": A, "b_iri": B, "reason": "wrong"})
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    cypher = session.run.call_args[0][0]
    assert "(a)-[s:SAME_AS]-(b)" in cypher
    assert "(a)-[s:SAME_AS]->(b)" not in cypher


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
