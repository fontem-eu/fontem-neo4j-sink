"""repair_chains against a real Neo4j: a contract the old sink fused
with a stranger's is taken apart and comes back as a fresh ingest
would have built it. The event log is an in-memory stand-in; the graph
and the sink's write path are real.

Needs Docker, like test_contract_chain_replay (whose fixtures and
notices it reuses)."""
# pylint: disable=redefined-outer-name,unused-import,protected-access
from __future__ import annotations

import pytest

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
    assert "nothing to repair" in plan.describe()


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
