"""repair_chains against a real Neo4j: a contract the old sink fused
with a stranger's is taken apart and comes back as a fresh ingest
would have built it. The event log is an in-memory stand-in; the graph
and the sink's write path are real.

Needs Docker, like test_contract_chain_replay (whose fixtures and
notices it reuses)."""
# pylint: disable=redefined-outer-name,unused-import,protected-access
from __future__ import annotations

import pytest

from neo4j_sink import repair_chains
from neo4j_sink.repair_chains import IRI, Repairer
from tests.test_repair_chains_plan import FakeLog
from tests.test_contract_chain_replay import (  # noqa: F401  (fixtures)
    _assert_two_separate_contracts, _award, _bulgarian_award,
    _bulgarian_mod_with_the_typo, _events, _german_award, _german_mod, _mod,
    _state, neo4j, pytestmark, sink,
)


def _fused_graph(sink, driver, log, *payloads):
    """The state prod was in: every notice ingested, then the two
    entities folded together and the wrong MODIFIES edge in place, as
    the pre-plausibility sink left them."""
    for p in payloads:
        log.add(p)
    sink.handle(_events(*payloads))
    with driver.session() as s:
        s.run("MATCH (m:Notice {ted_notice_id: 'BG-M'}), (t:Notice {ted_notice_id: 'DE-M'}) "
              "MERGE (m)-[:MODIFIES]->(t)")
        s.run("MATCH (e:Contract {contract_key: 'BGPROC'}), "
              "(o:Contract {contract_key: '375716-2020'}) "
              "CALL apoc.refactor.mergeNodes([e, o], {properties: 'discard', "
              "mergeRels: true}) YIELD node RETURN node")
    fused = _state(driver)
    assert list(fused["entities"]) == ["BGPROC"]          # one entity…
    assert {e[2] for e in fused["edges"]} == {"db-netz", "unwe", "glass", "alfalift"}


def test_a_fused_contract_is_rebuilt_as_two(sink, neo4j):
    _, driver = neo4j
    log = FakeLog()
    _fused_graph(sink, driver, log, _german_award(), _german_mod(),
                 _bulgarian_award(), _bulgarian_mod_with_the_typo())
    repairer = Repairer(sink, log)

    plan = repairer.plan("BGPROC")
    assert [(r["m"], r["t"]) for r in plan.refused] == [("BG-M", "DE-M")]
    assert sorted(sorted(c) for c in plan.contracts) == [
        ["BG-A", "BG-M"], ["DE-A", "DE-M"]]
    assert _state(driver)["entities"].keys() == {"BGPROC"}   # planning wrote nothing

    after = repairer.rebuild(plan)
    _assert_two_separate_contracts(_state(driver))
    assert {row["key"]: row["ours"] for row in after} == {"BGPROC": 2, "375716-2020": 2}


def test_rebuilding_twice_changes_nothing_more(sink, neo4j):
    _, driver = neo4j
    log = FakeLog()
    _fused_graph(sink, driver, log, _german_award(), _german_mod(),
                 _bulgarian_award(), _bulgarian_mod_with_the_typo())
    repairer = Repairer(sink, log)
    repairer.rebuild(repairer.plan("BGPROC"))
    once = _state(driver)
    again = repairer.plan("BGPROC")
    assert not again.changes_anything
    repairer.rebuild(again)
    assert _state(driver) == once


def test_the_newest_whole_notice_is_replayed_not_a_value_patch(sink, neo4j):
    """The tail of a notice's events can be a four-key rollup patch
    from the retired collapse job; replaying that would erase the
    notice."""
    _, driver = neo4j
    log = FakeLog()
    _fused_graph(sink, driver, log, _german_award(), _german_mod(),
                 _bulgarian_award(), _bulgarian_mod_with_the_typo())
    log.add({"ted_notice_id": "DE-M", "contract_key": "375716-2020",
             "is_current": False, "current_value": None})
    repairer = Repairer(sink, log)
    repairer.rebuild(repairer.plan("BGPROC"))
    _assert_two_separate_contracts(_state(driver))


def test_a_legacy_modification_keyed_by_a_wrong_back_link_gets_its_own_key(sink, neo4j):
    """A legacy modification's contract_key IS its back-link, so a
    wrong number lands it on the stranger's entity with no MODIFIES
    edge involved. The Czech road notice that 'modifies' an Estonian
    medical-supplies award."""
    _, driver = neo4j
    estonian = _award(ted_notice_id="EE-A", ted_publication_number="111-2021",
                      contract_key="111-2021", procedure_id=None, notice_version=None,
                      country="EST", title="Ühekordse kasutusega meditsiinitarvikud",
                      authority_id="ee-hospital", company_gmr_id="ee-med",
                      publication_date="2021-03-01")
    czech = _mod("CZ-M", ted_publication_number="222-2022", procedure_id=None,
                 contract_key="111-2021", modifies_publication_number="111-2021",
                 country="CZE", title="I/42 Brno, VMO Bauerova",
                 authority_id="cz-roads", company_gmr_id="cz-builder",
                 publication_date="2022-06-01", value_eur=777.0)
    log = FakeLog()
    for p in (estonian, czech):
        log.add(p)
    sink.handle(_events(estonian, czech))
    assert list(_state(driver)["entities"]) == ["111-2021"]   # fused by key alone

    repairer = Repairer(sink, log)
    plan = repairer.plan("111-2021")
    assert plan.rekeyed == {"CZ-M": "222-2022"}
    repairer.rebuild(plan)

    state = _state(driver)
    assert sorted(state["entities"]) == ["111-2021", "222-2022"]
    assert state["notices"]["CZ-M"]["entities"] == ["222-2022"]
    assert state["modifies"] == []
    assert ("AWARDED", "111-2021", "cz-roads") not in state["edges"]
    assert ("AWARDED", "222-2022", "cz-roads") in state["edges"]
    assert state["entities"]["222-2022"]["award_ingested"] is False


def test_an_entity_with_a_notice_missing_from_the_log_is_left_alone(sink, neo4j):
    _, driver = neo4j
    log = FakeLog()
    _fused_graph(sink, driver, log, _german_award(), _german_mod(),
                 _bulgarian_award(), _bulgarian_mod_with_the_typo())
    del log.rows[IRI.format("DE-A")]
    repairer = Repairer(sink, log)
    plan = repairer.plan("BGPROC")
    assert plan.missing == ["DE-A"]
    before = _state(driver)
    with pytest.raises(SystemExit):
        repairer.rebuild(plan)
    assert _state(driver) == before


def test_a_healthy_contract_has_nothing_to_repair(sink):
    log = FakeLog()
    award = _award()
    mod = _mod("M1", ted_publication_number="200-2026", modifies_notice_id="A",
               publication_date="2026-03-01", value_eur=1500.0)
    for p in (award, mod):
        log.add(p)
    sink.handle(_events(award, mod))
    plan = Repairer(sink, log).plan("P1")
    assert not plan.changes_anything
    assert "nothing to rebuild" in plan.describe()


def test_differently_keyed_notices_that_are_linked_are_one_contract(sink, neo4j):
    """An eForms modification adopted onto a legacy award: two stamped
    keys on one entity, and nothing wrong with it."""
    _, driver = neo4j
    legacy = _award(ted_notice_id="549184-2020", ted_publication_number="549184-2020",
                    contract_key="549184-2020", procedure_id=None,
                    notice_version=None, publication_date="2020-11-13")
    eforms_mod = _mod("E1", contract_key="AFC0", procedure_id="AFC0",
                      ted_publication_number="540529-2026",
                      modifies_publication_number="549184-2020",
                      publication_date="2026-08-20", value_eur=235.0)
    log = FakeLog()
    for p in (legacy, eforms_mod):
        log.add(p)
    sink.handle(_events(legacy, eforms_mod))
    assert list(_state(driver)["entities"]) == ["549184-2020"]
    plan = Repairer(sink, log).plan("549184-2020")
    assert len(plan.groups) == 2 and len(plan.contracts) == 1
    assert not plan.changes_anything


# ── stray links, stale keys, order, versions ──────────────────────


def _stray(driver, m, t):
    """A MODIFIES edge the retired link_ted_modifications wrote: from a
    modification to an award its back-link does not name."""
    with driver.session() as s:
        s.run("MATCH (m:Notice {ted_notice_id: $m}), (t:Notice {ted_notice_id: $t}) "
              "MERGE (m)-[:MODIFIES]->(t)", m=m, t=t)


def test_unlink_deletes_a_stray_edge_and_keeps_the_procedure_whole(sink, neo4j):
    """Two lots of one procedure and a modification of the first: the
    retired linker also pointed the modification at the second lot.
    One contract either way (one procedure, one key); only the edge goes."""
    _, driver = neo4j
    log = FakeLog()
    lot2 = _award(ted_notice_id="A2", ted_publication_number="150-2026",
                  publication_date="2026-06-01", value_eur=400.0)
    m1 = _mod("M1", ted_publication_number="200-2026", modifies_notice_id="A",
              publication_date="2026-03-01", value_eur=1500.0)
    for p in (_award(), m1, lot2):
        log.add(p)
    sink.handle(_events(_award(), m1, lot2))
    _stray(driver, "M1", "A2")
    repairer = Repairer(sink, log)
    assert repairer.count_stray() == 1

    assert repairer.unlink_stray() == ["P1"]
    state = _state(driver)
    assert state["modifies"] == [("M1", "A")]
    assert list(state["entities"]) == ["P1"]
    assert not repairer.plan("P1").changes_anything


def test_unlink_rebuilds_what_the_stray_edge_alone_held_together(sink, neo4j, capsys,
                                                                 monkeypatch):
    """Two procedures the old adopt folded into one entity over a stray
    edge: without it they plan as two contracts, and `unlink` rebuilds
    them apart."""
    _, driver = neo4j
    log = FakeLog()
    other = _award(ted_notice_id="B", ted_publication_number="300-2026",
                   contract_key="P2", procedure_id="P2", publication_date="2026-02-01",
                   title="Road works", value_eur=50.0)
    other_mod = _mod("MB", ted_publication_number="400-2026", contract_key="P2",
                     procedure_id="P2", modifies_notice_id="B",
                     publication_date="2026-04-01", value_eur=70.0)
    for p in (_award(), other, other_mod):
        log.add(p)
    sink.handle(_events(_award(), other, other_mod))
    _stray(driver, "MB", "A")
    with driver.session() as s:
        s.run("MATCH (e:Contract {contract_key: 'P1'}), (o:Contract {contract_key: 'P2'}) "
              "CALL apoc.refactor.mergeNodes([e, o], {properties: 'discard', "
              "mergeRels: true}) YIELD node RETURN node")
    assert list(_state(driver)["entities"]) == ["P1"]

    monkeypatch.setattr(repair_chains, "_connect", lambda: Repairer(sink, log))
    assert repair_chains.main(["unlink", "--stray", "--apply"]) == 0
    state = _state(driver)
    assert sorted(state["entities"]) == ["P1", "P2"]
    assert state["modifies"] == [("MB", "B")]
    assert {n: v["entities"] for n, v in state["notices"].items()} == {
        "A": ["P1"], "B": ["P2"], "MB": ["P2"]}
    assert "1 rebuilt" in capsys.readouterr().out


def test_an_entity_keyed_by_nothing_is_rebuilt_and_its_notices_go_home(sink, neo4j):
    """grain.notice_belongs_to_one_contract: the old loader's UUID-keyed
    entity still holds notices that also hang off their procedure's."""
    _, driver = neo4j
    log = FakeLog()
    lot2 = _award(ted_notice_id="A2", ted_publication_number="150-2026",
                  publication_date="2026-06-01", value_eur=400.0)
    for p in (_award(), lot2):
        log.add(p)
    sink.handle(_events(_award(), lot2))
    with driver.session() as s:
        s.run("CREATE (u:Contract {contract_key: 'uuid-A', notice_count: 2}) "
              "WITH u MATCH (n:Notice) WHERE n.ted_notice_id IN ['A', 'A2'] "
              "MERGE (n)-[:NOTICE_OF]->(u)")
    repairer = Repairer(sink, log)
    assert sorted(repairer.dual_homed()) == ["P1", "uuid-A"]
    assert not repairer.plan("P1").changes_anything
    plan = repairer.plan("uuid-A")
    assert plan.stale_key and plan.changes_anything

    repairer.rebuild(plan)
    state = _state(driver)
    assert list(state["entities"]) == ["P1"]
    assert {n: v["entities"] for n, v in state["notices"].items()} == {
        "A": ["P1"], "A2": ["P1"]}
    assert state["entities"]["P1"]["notice_count"] == 2
    assert repairer.dual_homed() == []


def test_rollup_stale_order_fixes_an_entity_rolled_up_by_dates(sink, neo4j):
    """A same-day modification the old roll-up ranked behind its award
    (a UUID coin flip). The sink's own roll-up, re-run, puts it first."""
    _, driver = neo4j
    log = FakeLog()
    award = _award(ted_notice_id="Z", publication_date="2026-03-01")
    mod = _mod("M", ted_publication_number="200-2026", modifies_notice_id="Z",
               publication_date="2026-03-01", value_eur=1500.0)
    sink.handle(_events(award, mod))
    with driver.session() as s:     # what the date-and-id order left behind
        s.run("MATCH (z:Notice {ted_notice_id: 'Z'}), (m:Notice {ted_notice_id: 'M'}), "
              "(e:Contract {contract_key: 'P1'}) SET z.is_current = true, "
              "m.is_current = false, e.ted_notice_id = 'Z', e.current_value = 1000.0")
    repairer = Repairer(sink, log)
    assert [r["key"] for r in repairer.stale_order()] == ["P1"]
    repairer.roll_up([r["nid"] for r in repairer.stale_order()])
    state = _state(driver)
    assert state["entities"]["P1"]["latest"] == "M"
    assert state["entities"]["P1"]["current_value"] == 1500.0


def test_versions_replays_the_newest_version_over_an_older_one(sink, neo4j):
    """The pre-guard state: the rescan's v01 re-emit left v01 on the node."""
    _, driver = neo4j
    log = FakeLog()
    v1 = _award()
    v2 = _award(notice_version="02", publication_date="2026-02-01", value_eur=1200.0)
    for p in (v1, v2, v1):
        log.add(p)
    sink.handle(_events(v1))
    repairer = Repairer(sink, log)
    behind = repairer.versions_behind()
    assert [(nid, p["notice_version"]) for nid, _seq, p in behind] == [("A", "02")]

    repairer.replay(behind)
    with driver.session() as s:
        row = s.run("MATCH (n:Notice {ted_notice_id: 'A'})-[:NOTICE_OF]->(e:Contract) "
                    "RETURN n.notice_version AS v, n.value_eur AS value, "
                    "e.current_value AS current").single()
    assert (row["v"], row["value"], row["current"]) == ("02", 1200.0, 1200.0)
    assert not repairer.versions_behind()
