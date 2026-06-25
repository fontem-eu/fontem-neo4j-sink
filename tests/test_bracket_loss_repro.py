"""Regression test for the OOM-mid-bracket data-loss bug.

Original incident: 2026-06-07 against the EDGAR financial graph.
130k UpsertFiling events accumulated in self._bracket_writes pushed
the 1Gi sink pod past its memory limit; OOMKill mid-bracket dropped
the in-memory accumulator, the next container saw the End with no
matching Begin, logged a WARNING no-op, and consumer offsets
advanced past everything in between — every filing event silently
lost.

After fontem-api refactor/drop-financials-graph-brackets, neither
load_us_financials nor load_eu_listings emits Begin/End for the
financials graphs — the sink's per-event MERGE path (`_apply_one`)
handles each UpsertFiling independently, so the OOM scenario can't
recur for that data.

The bracket-handling code path remains in the sink, used by
load_eu_sanctions for its ~1.6k entity snapshot. These tests
continue to pin the sink-level invariants:

* test_current_behaviour_loses_bracket_when_sink_restarts — any
  bracket lost mid-flight is currently dropped silently.
* test_desired_behaviour_replays_bracket_on_restart — xfail.
  Acceptance test for an eventual all-or-nothing bracket model.
* test_bracket_writes_grow_unboundedly_until_end — pins the
  unbounded-memory growth pattern so a future loader can't
  reintroduce the original 1Gi OOM cliff unnoticed.
"""
# pylint: disable=protected-access
from __future__ import annotations

import datetime as dt
import logging
from typing import Any
from unittest import mock

import pytest
from fontem_event_schemas import EventEnvelope


_GRAPH = "http://data.fontem.eu/graph/financials/edgar"


def _ev(event_type: str, payload: dict[str, Any], seq: int) -> EventEnvelope:
    return EventEnvelope(
        event_type=event_type,
        iri=payload.get("gmr_id", "n/a"),
        domain="financials",
        op="control" if "Graph" in event_type else "upsert",
        payload=payload,
        producer="load_us_financials",
        ts=dt.datetime(2026, 6, 7, 10, 40, 11, tzinfo=dt.UTC),
        seq=seq,
    )


def _begin(seq: int = 1) -> EventEnvelope:
    return _ev("BeginGraphReplace", {"graph_iri": _GRAPH}, seq)


def _end(seq: int = 999) -> EventEnvelope:
    return _ev("EndGraphReplace", {"graph_iri": _GRAPH}, seq)


def _filing(gmr_id: str, year: int, seq: int) -> EventEnvelope:
    return _ev(
        "UpsertFiling",
        {
            "gmr_id": gmr_id, "year": year, "source": "edgar",
            "filing_date": f"{year}-01-01",
            "revenue": 100, "gross_profit": 50,
            "operating_income": 30, "net_income": 20, "eps": 1.0,
            "total_assets": 1000, "total_liabilities": 600,
            "equity": 400, "cash_and_equivalents": 50,
            "cash": 50, "capex": 10, "operating_cashflow": 80,
            "free_cashflow": 70, "current_assets": 200,
            "current_liabilities": 100, "shares_outstanding": 100,
            "long_term_debt": 200, "interest_expense": 5,
            "income_tax_expense": 8,
            "depreciation_amortization": 15, "inventory": 20,
        },
        seq,
    )


def _make_sink_with_mock_driver():
    """Build a Neo4jSink whose driver session.run() records every call
    instead of touching Neo4j. Returns (sink, run_calls list)."""
    run_calls: list[tuple[str, dict]] = []

    class _FakeSession:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def run(self, query, *args, **kwargs):
            params = dict(kwargs)
            if args and isinstance(args[0], dict):
                params = {**args[0], **params}
            run_calls.append((query, params))
            return mock.MagicMock()

    class _FakeDriver:
        def session(self):
            return _FakeSession()

        def close(self):
            pass

    env = {
        "NEO4J_URI": "bolt://stub",
        "NEO4J_USER": "neo4j",
        "NEO4J_PASSWORD": "stub",
        "EVENT_CONSUMER_NAME": "neo4j_sink_test",
        "EVENTS_DATABASE_URL": "postgres://stub/stub",
    }
    # Local imports so the env-var + GraphDatabase patches are active
    # at the moment Neo4jSink resolves its dependencies.
    with mock.patch.dict("os.environ", env), \
         mock.patch("neo4j_sink.sink.GraphDatabase.driver",
                    return_value=_FakeDriver()):
        from neo4j_sink.sink import Neo4jSink  # pylint: disable=import-outside-toplevel
        from fontem_events.consumer import ConsumerConfig  # pylint: disable=import-outside-toplevel
        cfg = ConsumerConfig(
            name="neo4j_sink_test",
            dsn="postgres://stub/stub",
            metrics_port=None,
        )
        sink = Neo4jSink(cfg)
    return sink, run_calls


def test_current_behaviour_loses_bracket_when_sink_restarts():
    """Pins today's broken behaviour: a fresh sink instance that
    receives only the End logs a WARNING no-op and discards the
    accumulated writes. The consumer base will have already committed
    the offset past those writes (per-batch commit in
    EventConsumer.run_once), so they cannot be replayed without
    manual intervention.

    Remove or invert this test when the all-or-nothing fix lands.
    """
    sink1, calls1 = _make_sink_with_mock_driver()

    # Batch 1: Begin + 50 UpsertFiling events. Bracket open in sink1.
    begin = _begin(seq=1)
    filings = [_filing(f"co{i}", 2024, seq=10 + i) for i in range(50)]
    sink1.handle([begin, *filings])

    # All accumulated, nothing flushed to Neo4j yet — Begin/End bracket
    # means "delete-then-replace" but the bracket is still open.
    assert sink1._bracket_writes[_GRAPH], (
        "bracket writes should have accumulated in memory"
    )
    assert not calls1, (
        "no Neo4j writes should have happened before End — the "
        "current pattern accumulates the whole batch in memory"
    )

    # Simulate sink restart: sink1 dies (OOMKill in prod), the
    # in-memory bracket state is gone. sink2 is a fresh instance that
    # picks up at the next event in the queue, which happens to be
    # the End for this bracket.
    sink2, calls2 = _make_sink_with_mock_driver()
    end = _end(seq=120)

    caplog_records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = caplog_records.append  # type: ignore[method-assign]
    logger = logging.getLogger("neo4j_sink.sink")
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        sink2.handle([end])
    finally:
        logger.removeHandler(handler)

    # Today's behaviour: warns and swallows.
    warnings = [r for r in caplog_records if r.levelno == logging.WARNING]
    assert any("without matching Begin" in r.getMessage() for r in warnings), (
        "current sink logs a WARNING and treats orphan End as a no-op"
    )
    # And no writes happened — the 50 filings were lost.
    assert not calls2, (
        "no writes hit Neo4j — the bracket was thrown away on restart"
    )


@pytest.mark.xfail(
    reason=(
        "Bracket atomicity not yet implemented. Desired model: the "
        "consumer offset must NOT advance past the Begin until the End "
        "is successfully flushed. A restart between Begin and End "
        "should replay the whole bracket. Track in "
        "DATA_QUALITY_BACKLOG.md item 2."
    ),
    strict=True,
)
def test_desired_behaviour_replays_bracket_on_restart():
    """When the fix is in place, a sink instance that sees only the End
    must NOT silently advance — either it raises so the consumer keeps
    its offset at Begin and replays, or it materialises the bracket
    from a durable source.

    Today this test fails: the orphan-End is a WARNING no-op."""
    sink1, _ = _make_sink_with_mock_driver()
    begin = _begin(seq=1)
    filings = [_filing(f"co{i}", 2024, seq=10 + i) for i in range(50)]
    sink1.handle([begin, *filings])

    sink2, calls2 = _make_sink_with_mock_driver()
    end = _end(seq=120)
    with pytest.raises(Exception):  # noqa: BLE001 — any raise is fine
        sink2.handle([end])
    # The consumer offset must stay at the Begin; the bracket's writes
    # must not have been silently dropped.
    assert not calls2, (
        "no partial writes either — all-or-nothing means nothing yet"
    )


def test_bracket_writes_grow_unboundedly_until_end():
    """The current bracket implementation accumulates every write in
    memory until End. This is the proximate cause of the OOM in prod:
    130k UpsertFiling events on a 1Gi sink pod with no chunked flush.

    This test pins the unbounded-accumulation behaviour so the OOM
    risk is visible in CI as soon as a future change tries to keep
    the pattern."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_begin(seq=1)])

    for batch in range(10):
        sink.handle([
            _filing(f"co{batch}_{i}", 2024, seq=10 + batch * 100 + i)
            for i in range(100)
        ])
        # No flushes happen until End — every batch just grows memory.
        assert not calls, (
            "current sink defers all writes until End; "
            "this is what blew memory in prod"
        )
        assert len(sink._bracket_writes[_GRAPH]) == (batch + 1) * 100

    # 1000 writes accumulated, still no flush.
    assert len(sink._bracket_writes[_GRAPH]) == 1000


def test_typed_relationship_skips_self_loop():
    from neo4j_sink.cypher import CypherWrite  # pylint: disable=import-outside-toplevel
    sink, calls = _make_sink_with_mock_driver()
    iri = "http://data.fontem.eu/id/Company/abc"
    w = CypherWrite(
        label="_Relationship",
        primary_key={"src_iri": iri, "dst_iri": iri, "predicate": "subsidiaryOf"},
        set_props={},
    )
    sink._apply_typed_relationship(w)  # pylint: disable=protected-access
    assert not calls  # a self-relationship issues no MERGE


def test_typed_relationship_creates_normal_edge():
    from neo4j_sink.cypher import CypherWrite  # pylint: disable=import-outside-toplevel
    sink, calls = _make_sink_with_mock_driver()
    w = CypherWrite(
        label="_Relationship",
        primary_key={
            "src_iri": "http://data.fontem.eu/id/Company/aaa",
            "dst_iri": "http://data.fontem.eu/id/Company/bbb",
            "predicate": "subsidiaryOf",
        },
        set_props={},
    )
    sink._apply_typed_relationship(w)  # pylint: disable=protected-access
    assert any("MERGE" in q for q, _ in calls)


def test_apply_relationship_skips_self_edge():
    sink, calls = _make_sink_with_mock_driver()
    # src Company 'x' and a target IRI resolving to the same Company 'x'.
    sink._apply_relationship(  # pylint: disable=protected-access
        "Company", {"gmr_id": "x"}, "FILED_BY",
        "http://data.fontem.eu/id/Company/x", {},
    )
    assert not calls  # self-edge issues no MERGE


def test_apply_relationship_creates_normal_edge():
    sink, calls = _make_sink_with_mock_driver()
    sink._apply_relationship(  # pylint: disable=protected-access
        "Company", {"gmr_id": "x"}, "FILED_BY",
        "http://data.fontem.eu/id/Company/y", {},
    )
    assert any("MERGE" in q for q, _ in calls)


def test_apply_one_attaches_extra_labels():
    # Covers the per-system secondary-label path (_label_set_clause) so a
    # lobbying disclosure lands as :Disclosure:Lobbyist.
    from neo4j_sink.cypher import CypherWrite  # pylint: disable=import-outside-toplevel
    sink, calls = _make_sink_with_mock_driver()
    w = CypherWrite(
        label="Disclosure",
        primary_key={"disclosure_id": "d1", "system": "eu-lobbying"},
        set_props={"title": "T"},
        extra_labels=["Lobbyist"],
    )
    sink._apply_one(w)  # pylint: disable=protected-access
    assert any("Lobbyist" in q for q, _ in calls)


def test_apply_one_no_extra_labels_is_plain_merge():
    from neo4j_sink.cypher import CypherWrite  # pylint: disable=import-outside-toplevel
    sink, calls = _make_sink_with_mock_driver()
    w = CypherWrite(
        label="Company", primary_key={"gmr_id": "g1"},
        set_props={"name": "Acme"},
    )
    sink._apply_one(w)  # pylint: disable=protected-access
    assert calls and all("Lobbyist" not in q for q, _ in calls)


def test_flush_bracket_applies_common_labels():
    # Covers the bracket PUT-replace path: DETACH DELETE the label then
    # UNWIND-MERGE the batch, attaching the labels common to the batch.
    from neo4j_sink.cypher import CypherWrite  # pylint: disable=import-outside-toplevel
    sink, calls = _make_sink_with_mock_driver()
    writes = [
        CypherWrite(label="Disclosure",
                    primary_key={"disclosure_id": "a", "system": "eu-lobbying"},
                    set_props={"title": "A"}, extra_labels=["Lobbyist"]),
        CypherWrite(label="Disclosure",
                    primary_key={"disclosure_id": "b", "system": "eu-lobbying"},
                    set_props={"title": "B"}, extra_labels=["Lobbyist"]),
    ]
    sink._flush_bracket("Disclosure", writes)  # pylint: disable=protected-access
    queries = [q for q, _ in calls]
    assert any("DETACH DELETE" in q for q in queries)
    assert any("Lobbyist" in q for q in queries)


# ── batched non-bracket apply (drain-throughput fix) ──────────


def _contract(nid: str, seq: int, authority_id="auth1",
              company_gmr_id="co1", **extra) -> EventEnvelope:
    payload = {
        "ted_notice_id": nid,
        "title": "T", "publication_date": "2026-01-15",
        "value_eur": 1000, "value_currency": "EUR",
        "procedure_type": "open", "tenders_received": 3,
        "authority_id": authority_id, "company_gmr_id": company_gmr_id,
    }
    payload.update(extra)
    return _ev("UpsertContract", payload, seq)


def test_non_bracket_contracts_are_unwind_batched():
    """5 contracts (node + AWARDED + AWARDED_TO each) must collapse to
    3 UNWIND session.run calls, not 15 per-event ones — this is the
    drain-throughput fix."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract(f"n{i}-2026", seq=10 + i) for i in range(5)])
    queries = [q for q, _ in calls]
    assert queries, "nothing rendered"
    assert all("UNWIND $rows AS row" in q for q in queries), queries
    assert len(calls) == 3, queries
    node_calls = [c for c in calls if "MERGE (n:Contract" in c[0]]
    assert len(node_calls) == 1
    assert len(node_calls[0][1]["rows"]) == 5
    rel_calls = [c for c in calls if "-[r:" in c[0]]
    assert len(rel_calls) == 2
    assert all(len(c[1]["rows"]) == 5 for c in rel_calls)


def test_batched_node_merge_uses_props_map_and_skips_absent_edges():
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract("solo-2026", seq=1,
                           company_gmr_id=None, authority_id=None)])
    node = [c for c in calls if "MERGE (n:Contract" in c[0]][0]
    assert "SET n += row.props" in node[0]
    row = node[1]["rows"][0]
    assert row["ted_notice_id"] == "solo-2026"
    assert row["props"]["procedure_type"] == "open"
    assert row["props"]["tenders_received"] == 3
    assert not [c for c in calls if "-[r:" in c[0]]


def test_same_key_writes_collapse_last_wins():
    """Two upserts of one company in a batch collapse to a single node
    row; later set_props win per field, earlier fields persist — same
    result as a sequential per-event apply."""
    sink, calls = _make_sink_with_mock_driver()
    e1 = _ev("UpsertCompany",
             {"gmr_id": "c1", "name": "Old", "country": "FR"}, 1)
    e2 = _ev("UpsertCompany",
             {"gmr_id": "c1", "name": "New", "lei": "X"}, 2)
    sink.handle([e1, e2])
    node_calls = [c for c in calls if "MERGE (n:Company" in c[0]]
    assert len(node_calls) == 1
    rows = node_calls[0][1]["rows"]
    assert len(rows) == 1
    props = rows[0]["props"]
    assert props["name"] == "New"
    assert props["country"] == "FR"
    assert props["lei"] == "X"


def test_directional_edges_keep_orientation_when_batched():
    """AWARDED is from_target (Authority->Contract); AWARDED_TO is
    from_source (Contract->Company). Batching must preserve both."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract("d-2026", seq=1)])
    rel_calls = {c[0].split("MERGE ", 1)[1].split(" ", 1)[0]: c
                 for c in calls if "-[r:" in c[0]}
    awarded = [q for q in rel_calls if "[r:AWARDED]" in q]
    awarded_to = [q for q in rel_calls if "[r:AWARDED_TO]" in q]
    assert awarded and "(t)-[r:AWARDED]->(s)" in awarded[0]
    assert awarded_to and "(s)-[r:AWARDED_TO]->(t)" in awarded_to[0]
