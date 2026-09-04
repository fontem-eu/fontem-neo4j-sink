"""Source-stated relationships must never be silently dropped: a missing
endpoint gets a {_stub: true} placeholder MERGEd in the same write, and
the entity's own upsert later clears the flag (#269). SAME_AS is the
deliberate exception — a derived proposal about entities the consolidator
read from this graph, void if an endpoint vanished."""
# pylint: disable=protected-access
from unittest import mock

from neo4j_sink.sink import Neo4jSink
from tests.test_bracket_loss_repro import _contract, _make_sink_with_mock_driver


def test_extra_relationship_stubs_missing_target():
    """AWARDED_TO's Company target: stub-created when absent, matched via
    the Company|InvestmentFund disjunction when present."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract("d-2026", seq=1)])
    q = next(c[0] for c in calls if "[r:AWARDED_TO]" in c[0])
    assert "OPTIONAL MATCH (t0:Company|InvestmentFund" in q
    assert "MERGE (ts:Company" in q
    assert "SET ts._stub = true" in q
    # the edge still MATCHes the disjunction (never a bare :Company)
    assert "MATCH (t:Company|InvestmentFund" in q


def test_node_merge_cyphers_clear_stub():
    """Every node-upsert path clears the placeholder flag, so a stub
    becomes a real entity the moment its own event arrives."""
    for cy in (Neo4jSink._IFUND_MERGE_CYPHER,
               Neo4jSink._COMPANY_REFRESH_FUND_CYPHER,
               Neo4jSink._COMPANY_MERGE_NON_FUND_CYPHER,
               Neo4jSink._COMPANY_REVERT_CYPHER):
        assert "REMOVE" in cy and "._stub" in cy, cy[:60]


def test_typed_relationship_stubs_both_endpoints():
    """UpsertRelationship: neither endpoint is the event's own node, so
    both sides get the stub treatment."""
    sink, calls = _make_sink_with_mock_driver()
    w = mock.MagicMock()
    w.label = "_Relationship"
    w.primary_key = {
        "src_iri": "http://data.fontem.eu/id/Company/g-parent",
        "dst_iri": "http://data.fontem.eu/id/Company/g-child",
        "predicate": "parentOf",
    }
    w.set_props = {"source": "gleif-rr"}
    sink._flush_typed_relationships([w])  # pylint: disable=protected-access
    q = next(c[0] for c in calls if "PARENT_OF" in c[0])
    assert q.count("SET as._stub = true") == 1
    assert q.count("SET bs._stub = true") == 1


def test_not_same_as_stays_match_only():
    """Corrections are never stubbed — they are statements about
    entities already in the graph, not source facts that must survive
    ingest-order timing.

    (The old SAME_AS equivalents are gone with the edge type: identity
    lives in Virtuoso and this sink writes no equivalences.)
    """
    sink, calls = _make_sink_with_mock_driver()
    w = mock.MagicMock()
    w.label = "_NotSameAs"
    w.primary_key = {
        "a_iri": "http://data.fontem.eu/id/Company/g-a",
        "b_iri": "http://data.fontem.eu/id/Company/g-b",
    }
    w.set_props = {"reason": "different registration numbers"}
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    q = next(c[0] for c in calls if "NOT_SAME_AS" in c[0])
    assert "_stub" not in q


def test_not_same_as_self_reference_is_skipped():
    sink, calls = _make_sink_with_mock_driver()
    w = mock.MagicMock()
    w.label = "_NotSameAs"
    w.primary_key = {
        "a_iri": "http://data.fontem.eu/id/Company/same",
        "b_iri": "http://data.fontem.eu/id/Company/same",
    }
    w.set_props = {"reason": "x"}
    sink._apply_not_same_as(w)  # pylint: disable=protected-access
    assert not [c for c in calls if "NOT_SAME_AS" in c[0]]
