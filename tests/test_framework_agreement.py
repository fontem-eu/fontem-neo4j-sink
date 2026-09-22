"""A framework agreement as a node of its own (data-backlog Part 5, C6).

    (:Authority)-[:ESTABLISHED]->(:FrameworkAgreement)<-[:CALL_OFF_OF]-(:Contract)
                                        ^          ^
               (:Company)-[:PARTY_TO {lot, rank}]--+          |
               (:Notice)-[:ESTABLISHES]-----------------------+

Keyed by the establishing procedure (framework_id), so several notices
about one framework converge on one node. The ceiling is capacity, never
spend. The CALL_OFF_OF side is the call-off's own UpsertContract (see
test_contract_notice_model); this file covers the establishing event,
its edges, the IRI mapping and the schema migration."""
# pylint: disable=protected-access
import datetime as dt
import importlib.util
import pathlib

from fontem_event_schemas import EventEnvelope

from neo4j_sink.cleaning import render_upsert_framework_agreement
from neo4j_sink.cypher import RENDERERS
from neo4j_sink.sink import Neo4jSink
from tests.test_bracket_loss_repro import _ev, _make_sink_with_mock_driver

_FA = "http://data.fontem.eu/id/FrameworkAgreement/proc:FA-1"


def _payload(**extra):
    payload = {
        "framework_id": "proc:FA-1",
        "buyer_authority_id": "auth-1",
        "establishing_notice_id": "uuid-fa-notice",
        "country": "PRT",
        "ceiling_eur": 5_000_000.0,
        "ceiling_currency": "EUR",
        "ceiling_original": 5_000_000.0,
        "reestimated_value_eur": 3_200_000.0,
        "duration_start": "2026-01-01",
        "duration_end": "2029-12-31",
        "duration_months": 48,
        "cpv": "33600000",
        "lot_count": 4,
        "supplier_count": 2,
        "title": "Medicines framework",
        "suppliers": [
            {"company_gmr_id": "co-a", "lot": "LOT-0001", "rank": 1},
            {"company_gmr_id": "co-b", "lot": "LOT-0001", "rank": 2},
        ],
    }
    payload.update(extra)
    return payload


def _fa_event(payload, seq=1):
    return _ev("UpsertFrameworkAgreement", payload, seq)


# ── renderer ──────────────────────────────────────────────────────


def test_keyed_by_framework_id_with_the_scalar_props():
    w = render_upsert_framework_agreement(_payload())
    assert w.label == "FrameworkAgreement"
    assert w.primary_key == {"framework_id": "proc:FA-1"}
    assert w.set_props == {
        "buyer_authority_id": "auth-1",
        "establishing_notice_id": "uuid-fa-notice",
        "country": "PRT",
        "ceiling_eur": 5_000_000.0,
        "ceiling_currency": "EUR",
        "ceiling_original": 5_000_000.0,
        "reestimated_value_eur": 3_200_000.0,
        "duration_start": "2026-01-01",
        "duration_end": "2029-12-31",
        "duration_months": 48,
        "cpv": "33600000",
        "lot_count": 4,
        "supplier_count": 2,
        "title": "Medicines framework",
    }
    # suppliers are edges, never a list-of-maps prop
    assert "suppliers" not in w.set_props
    assert w.guard_prop is None and w.clear_props is None


def test_established_edge_runs_from_the_authority():
    w = render_upsert_framework_agreement(_payload())
    assert ("ESTABLISHED",
            "http://data.fontem.eu/id/Authority/auth-1",
            {"_direction": "from_target"}) in w.extra_relationships


def test_party_to_edge_per_supplier_from_the_company_with_lot_and_rank():
    w = render_upsert_framework_agreement(_payload())
    party_to = [r for r in w.extra_relationships if r[0] == "PARTY_TO"]
    assert party_to == [
        ("PARTY_TO", "http://data.fontem.eu/id/Company/co-a",
         {"_direction": "from_target", "lot": "LOT-0001", "rank": 1}),
        ("PARTY_TO", "http://data.fontem.eu/id/Company/co-b",
         {"_direction": "from_target", "lot": "LOT-0001", "rank": 2}),
    ]


def test_supplier_without_lot_rank_or_company_id():
    """lot/rank ride only when present; an item with no company id has
    nothing to resolve and yields no edge."""
    w = render_upsert_framework_agreement(_payload(suppliers=[
        {"company_gmr_id": "co-a"},
        {"lot": "LOT-0002", "rank": 1},
    ]))
    party_to = [r for r in w.extra_relationships if r[0] == "PARTY_TO"]
    assert party_to == [("PARTY_TO", "http://data.fontem.eu/id/Company/co-a",
                         {"_direction": "from_target"})]


def test_establishes_edge_runs_from_the_notice_when_present():
    w = render_upsert_framework_agreement(_payload())
    assert ("ESTABLISHES",
            "http://data.fontem.eu/id/Notice/uuid-fa-notice",
            {"_direction": "from_target"}) in w.extra_relationships
    w = render_upsert_framework_agreement(_payload(establishing_notice_id=None))
    assert not [r for r in w.extra_relationships if r[0] == "ESTABLISHES"]
    assert "establishing_notice_id" not in w.set_props


def test_absent_scalars_are_omitted_never_cleared():
    """SET += never deletes: an emit that omits a field leaves what an
    earlier emit wrote, like every other display field."""
    w = render_upsert_framework_agreement({"framework_id": "proc:FA-1"})
    assert not w.set_props
    assert w.clear_props is None
    assert w.extra_relationships is None


def test_registered_as_a_renderer():
    assert RENDERERS["UpsertFrameworkAgreement"] is render_upsert_framework_agreement


# ── IRI mapping ───────────────────────────────────────────────────


def test_iri_resolves_to_the_framework_id_key():
    assert Neo4jSink._iri_to_label_key(_FA) == ("FrameworkAgreement", "proc:FA-1")
    assert Neo4jSink._key_field("FrameworkAgreement") == "framework_id"
    assert Neo4jSink._node_label("FrameworkAgreement") == "FrameworkAgreement"
    assert Neo4jSink._match_label("FrameworkAgreement") == "FrameworkAgreement"


# ── sink ──────────────────────────────────────────────────────────


def test_sink_merges_the_node_then_its_three_edges():
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_fa_event(_payload())])
    node = next(c for c in calls if "MERGE (n:FrameworkAgreement" in c[0])
    assert node[0] == (
        "UNWIND $rows AS row "
        "MERGE (n:FrameworkAgreement { framework_id: row.framework_id }) "
        "SET n += row.props REMOVE n._stub"
    )
    assert node[1]["rows"][0]["framework_id"] == "proc:FA-1"
    assert node[1]["rows"][0]["props"]["ceiling_eur"] == 5_000_000.0

    established = next(c for c in calls if "[r:ESTABLISHED]" in c[0])
    assert ("MATCH (s:FrameworkAgreement { framework_id: row.framework_id })"
            in established[0])
    assert "MATCH (t:Authority { authority_id: row.tgt_key })" in established[0]
    assert "MERGE (t)-[r:ESTABLISHED]->(s)" in established[0]

    party_to = next(c for c in calls if "[r:PARTY_TO]" in c[0])
    assert "MATCH (t:Company|InvestmentFund { gmr_id: row.tgt_key })" in party_to[0]
    assert "MERGE (t)-[r:PARTY_TO]->(s)" in party_to[0]
    assert "SET r += row.props" in party_to[0]
    assert [r["props"] for r in party_to[1]["rows"]] == [
        {"lot": "LOT-0001", "rank": 1}, {"lot": "LOT-0001", "rank": 2}]

    establishes = next(c for c in calls if "[r:ESTABLISHES]" in c[0])
    assert "MATCH (t:Notice { ted_notice_id: row.tgt_key })" in establishes[0]
    assert "MERGE (t)-[r:ESTABLISHES]->(s)" in establishes[0]

    # node first, edges after (they MATCH the node this batch MERGEd)
    assert calls.index(node) < min(calls.index(established),
                                   calls.index(party_to),
                                   calls.index(establishes))


def test_sink_stubs_every_missing_endpoint():
    """Authority, companies and the establishing notice may all arrive
    after the framework: each gets a {_stub: true} placeholder so the
    edge is never dropped, and its own upsert clears the flag."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_fa_event(_payload())])
    for rel, label, field in (("ESTABLISHED", "Authority", "authority_id"),
                              ("PARTY_TO", "Company", "gmr_id"),
                              ("ESTABLISHES", "Notice", "ted_notice_id")):
        q = next(c[0] for c in calls if f"[r:{rel}]" in c[0])
        assert (f"MERGE (ts:{label} {{ {field}: row.tgt_key }}) "
                f"SET ts._stub = true" in q), rel


def test_establishing_event_fills_a_stub_left_by_an_earlier_call_off():
    """Arrival order in practice: a call-off's CALL_OFF_OF may reach the
    graph first and mint the stub; the framework's own MERGE lands on
    the same key and REMOVEs the flag."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_ev("UpsertContract", {
        "ted_notice_id": "uuid-call-off", "contract_key": "proc:CO-1",
        "notice_kind": "award", "publication_date": "2026-04-01",
        "framework_id": "proc:FA-1",
    }, 1)])
    stub = next(c[0] for c in calls if "[r:CALL_OFF_OF]" in c[0])
    assert "MERGE (ts:FrameworkAgreement { framework_id: row.tgt_key })" in stub
    sink.handle([_fa_event(_payload(), seq=2)])
    node = next(c for c in calls if "MERGE (n:FrameworkAgreement" in c[0])
    assert node[0].endswith("REMOVE n._stub")
    assert node[1]["rows"][0]["framework_id"] == "proc:FA-1"


def test_replay_of_the_same_batch_is_byte_identical():
    sink1, calls1 = _make_sink_with_mock_driver()
    sink2, calls2 = _make_sink_with_mock_driver()
    batch = [_fa_event(_payload())]
    sink1.handle(batch)
    sink2.handle(batch)
    sink2.handle(batch)          # the replay
    assert calls2[:len(calls1)] == calls1
    assert calls2[len(calls1):] == calls1


def test_delete_op_is_skipped_like_every_other_type():
    """Entity deletes are a Virtuoso-side concern; a delete's {iri}
    payload has none of the schema fields. Skipped, never poison."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([EventEnvelope(
        event_type="UpsertFrameworkAgreement", iri=_FA, domain="contract",
        op="delete", payload={"iri": _FA}, producer="load_ted_contracts",
        ts=dt.datetime(2026, 9, 22, tzinfo=dt.UTC), seq=1,
    )])
    assert not calls


# ── schema ────────────────────────────────────────────────────────


def _load_migration():
    path = (pathlib.Path(__file__).resolve().parents[1] / "migrations"
            / "framework_agreement_constraints_2026_09.py")
    spec = importlib.util.spec_from_file_location("fa_migration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_declares_a_unique_constraint_on_framework_id():
    """framework_id is what every edge resolves the node by and what a
    stub and the real event must meet on; the uniqueness constraint is
    also the index. IF NOT EXISTS makes the script re-runnable."""
    module = _load_migration()
    runs: list[str] = []

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def run(self, stmt, *_args, **_kwargs):
            runs.append(stmt)

    class _Driver:
        def session(self):
            return _Session()

    module.migrate(_Driver())
    assert runs == [
        "CREATE CONSTRAINT framework_agreement_framework_id_unique IF NOT EXISTS "
        "FOR (f:FrameworkAgreement) REQUIRE f.framework_id IS UNIQUE",
    ]
