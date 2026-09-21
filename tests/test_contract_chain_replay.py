"""The native contract chain against a real Neo4j: one entity per
contract whichever order the notices arrive in, and replay converges.

Needs Docker (testcontainers); the CI python runner has none, so the
module skips there. Locally:

    TESTCONTAINERS_RYUK_DISABLED=true python3 -m pytest tests/test_contract_chain_replay.py

The scenarios are the four failure classes measured on prod on
2026-09-12 (gitops/docs/roadmap/contract-modifications-single-path.md):
award then modification, modification before its award, a modification
of a pre-eForms award keyed by publication number, and the split that
the old archive path produced (award keyed by its notice UUID, later
re-stamped with its procedure id).
"""
# pylint: disable=redefined-outer-name,protected-access
from __future__ import annotations

import os
import shutil
import subprocess
import time
from unittest import mock

import pytest

from tests.test_bracket_loss_repro import _ev

testcontainers_neo4j = pytest.importorskip("testcontainers.neo4j")


def _docker_available() -> bool:
    if not shutil.which("docker"):
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True,
                              check=False, timeout=20).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return False


pytestmark = pytest.mark.skipif(not _docker_available(),
                                reason="needs a reachable Docker daemon")

# Pinned by digest: `5-community` floats, so the same test could run a
# different Neo4j from one week to the next (Docker Hub re-pushes the
# tag). This is the image fontem-shared runs.
# renovate: datasource=docker depName=neo4j
_IMAGE = os.environ.get("NEO4J_TEST_IMAGE", "neo4j:5.26.30-community@sha256:22ec5cd05a8cbb372fc4bed5e384c30bc75fd92504c72be4462039761b105f61")

_SCHEMA = (
    "CREATE CONSTRAINT contract_contract_key_unique IF NOT EXISTS "
    "FOR (c:Contract) REQUIRE c.contract_key IS UNIQUE",
    "CREATE CONSTRAINT notice_ted_notice_id_unique IF NOT EXISTS "
    "FOR (n:Notice) REQUIRE n.ted_notice_id IS UNIQUE",
    "CREATE INDEX notice_ted_publication_number IF NOT EXISTS "
    "FOR (n:Notice) ON (n.ted_publication_number)",
    "CREATE INDEX notice_modifies_publication_number IF NOT EXISTS "
    "FOR (n:Notice) ON (n.modifies_publication_number)",
    "CREATE INDEX notice_modifies_notice_id IF NOT EXISTS "
    "FOR (n:Notice) ON (n.modifies_notice_id)",
)


@pytest.fixture(scope="module")
def neo4j():
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
    container = testcontainers_neo4j.Neo4jContainer(_IMAGE, password="testpass")
    container.with_env("NEO4J_PLUGINS", '["apoc"]')
    with container:
        driver = container.get_driver()
        deadline = time.time() + 120
        while True:  # APOC is loaded a moment after bolt comes up
            try:
                with driver.session() as s:
                    s.run("RETURN apoc.version()").single()
                break
            except Exception:  # pylint: disable=broad-except
                if time.time() > deadline:
                    raise
                time.sleep(2)
        with driver.session() as s:
            for stmt in _SCHEMA:
                s.run(stmt)
        yield container.get_connection_url(), driver
        driver.close()


@pytest.fixture
def sink(neo4j):
    uri, driver = neo4j
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    env = {
        "NEO4J_URI": uri, "NEO4J_USER": "neo4j", "NEO4J_PASSWORD": "testpass",
        "EVENT_CONSUMER_NAME": "neo4j_sink_test",
        "EVENTS_DATABASE_URL": "postgres://stub/stub",
    }
    with mock.patch.dict("os.environ", env):
        from neo4j_sink.sink import Neo4jSink  # pylint: disable=import-outside-toplevel
        from fontem_events.consumer import ConsumerConfig  # pylint: disable=import-outside-toplevel
        instance = Neo4jSink(ConsumerConfig(
            name="neo4j_sink_test", dsn="postgres://stub/stub",
            metrics_port=None,
        ))
    yield instance
    instance._driver.close()


# ── notices ───────────────────────────────────────────────────────


def _award(**extra):
    p = {
        "ted_notice_id": "A", "ted_publication_number": "100-2026",
        "contract_key": "P1", "procedure_id": "P1",
        "notice_kind": "award", "notice_type": "can-standard",
        "notice_version": "01",
        "publication_date": "2026-01-10", "value_eur": 1000.0,
        "title": "Bridge works", "authority_id": "auth-1",
        "company_gmr_id": "co-1", "tenders_received": 3,
        "procedure_type": "open",
    }
    p.update(extra)
    return p


def _mod(nid, **extra):
    p = {
        "ted_notice_id": nid, "contract_key": "P1", "procedure_id": "P1",
        "notice_kind": "modification", "notice_type": "can-modif",
        "notice_version": "01", "authority_id": "auth-1",
        "company_gmr_id": "co-1",
    }
    p.update(extra)
    return p


M1 = _mod("M1", ted_publication_number="200-2026", modifies_notice_id="A",
          publication_date="2026-03-01", value_eur=1500.0)
M2 = _mod("M2", ted_publication_number="300-2026",
          modifies_publication_number="200-2026",
          publication_date="2026-05-01", value_eur=1800.0)


def _events(*payloads, start=1):
    return [_ev("UpsertContract", p, seq) for seq, p in enumerate(payloads, start)]


def _state(driver) -> dict:
    """Everything the chain invariants are about, in one comparable dict."""
    with driver.session() as s:
        entities = {
            r["key"]: dict(r) for r in s.run(
                "MATCH (e:Contract) RETURN e.contract_key AS key, "
                "e.notice_count AS notice_count, e.award_ingested AS award_ingested, "
                "e.notice_kind AS notice_kind, e.current_value AS current_value, "
                "e.value_eur AS value_eur, e.award_value AS award_value, "
                "e.is_current AS is_current, e.ted_notice_id AS latest, "
                "e.canonical_publication_date AS canonical_publication_date "
                "ORDER BY key")
        }
        notices = {
            r["nid"]: dict(r) for r in s.run(
                "MATCH (n:Notice) OPTIONAL MATCH (n)-[:NOTICE_OF]->(e:Contract) "
                "RETURN n.ted_notice_id AS nid, n.is_current AS is_current, "
                "n.contract_key AS contract_key, "
                "collect(e.contract_key) AS entities ORDER BY nid")
        }
        modifies = sorted(
            (r["a"], r["b"]) for r in s.run(
                "MATCH (a:Notice)-[:MODIFIES]->(b:Notice) "
                "RETURN a.ted_notice_id AS a, b.ted_notice_id AS b")
        )
        edges = sorted(
            (r["t"], r["k"], r["o"]) for r in s.run(
                "MATCH (e:Contract)-[r]-(o) WHERE NOT o:Notice "
                "RETURN type(r) AS t, e.contract_key AS k, "
                "coalesce(o.authority_id, o.gmr_id) AS o")
        )
    return {"entities": entities, "notices": notices,
            "modifies": modifies, "edges": edges}


def _assert_one_chain(state, key, latest, current_value, award_ingested=True):
    assert list(state["entities"]) == [key], state["entities"]
    e = state["entities"][key]
    assert e["award_ingested"] is award_ingested
    assert e["notice_kind"] == ("award" if award_ingested else "modification")
    assert e["is_current"] is True
    assert e["latest"] == latest
    assert e["current_value"] == current_value
    assert e["value_eur"] == current_value
    assert e["notice_count"] == len(state["notices"])
    for nid, n in state["notices"].items():
        assert n["entities"] == [key], (nid, n)
        assert n["contract_key"] == key, (nid, n)
        assert n["is_current"] is (nid == latest), (nid, n)


# ── scenarios ─────────────────────────────────────────────────────


def test_award_then_two_modifications(sink, neo4j):
    _, driver = neo4j
    sink.handle(_events(_award(), M1, M2))
    state = _state(driver)
    _assert_one_chain(state, "P1", latest="M2", current_value=1800.0)
    assert state["modifies"] == [("M1", "A"), ("M2", "M1")]
    assert state["entities"]["P1"]["award_value"] == 1000.0
    assert state["entities"]["P1"]["canonical_publication_date"] == "2026-05-01"
    # the award's edges are on the entity exactly once
    assert state["edges"] == [("AWARDED", "P1", "auth-1"),
                              ("AWARDED_TO", "P1", "co-1")]


def test_modifications_before_their_award_converge_to_the_same_state(sink, neo4j):
    """A late award: the reverse back-link resolution links M1 to it,
    and the end state equals the forward-order one."""
    _, driver = neo4j
    sink.handle(_events(M2, M1))
    before = _state(driver)
    _assert_one_chain(before, "P1", latest="M2", current_value=1800.0,
                      award_ingested=False)
    assert before["modifies"] == [("M2", "M1")]
    sink.handle(_events(_award(), start=3))
    after = _state(driver)
    _assert_one_chain(after, "P1", latest="M2", current_value=1800.0)
    assert after["modifies"] == [("M1", "A"), ("M2", "M1")]
    assert after["entities"]["P1"]["award_value"] == 1000.0


def test_modification_of_a_legacy_award_adopts_its_entity(sink, neo4j):
    """The 540529-2026 case: an eForms modification (keyed by its
    procedure id) of a 2020 award keyed by publication number. The
    modification's own entity is folded into the award's; the graph
    ends with one contract, keyed as the root award is."""
    _, driver = neo4j
    legacy = _award(ted_notice_id="549184-2020", ted_publication_number="549184-2020",
                    contract_key="549184-2020", procedure_id=None,
                    notice_version=None, publication_date="2020-11-13",
                    value_eur=100.0)
    legacy.pop("procedure_id")
    legacy.pop("notice_version")
    eforms_mod = _mod("E1", contract_key="AFC0", procedure_id="AFC0",
                      ted_publication_number="540529-2026",
                      modifies_publication_number="549184-2020",
                      publication_date="2026-08-20", value_eur=235.0)
    sink.handle(_events(legacy, eforms_mod))
    state = _state(driver)
    _assert_one_chain(state, "549184-2020", latest="E1", current_value=235.0)
    assert state["modifies"] == [("E1", "549184-2020")]
    # ...and it holds in the other order too (award backfilled later)
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    sink.handle(_events(eforms_mod, legacy))
    _assert_one_chain(_state(driver), "549184-2020", latest="E1",
                      current_value=235.0)


def test_split_award_is_repaired_by_restamping(sink, neo4j):
    """The old archive path keyed an award by its notice UUID; the
    modification (search path) by procedure id: two contracts. The
    modification first adopts the award's entity as it stands; when the
    award is re-ingested with its procedure id (P6), the chain moves to
    the newly stamped key and the UUID entity disappears with no edge
    lost."""
    _, driver = neo4j
    unstamped = _award(contract_key="A")
    unstamped.pop("procedure_id")
    sink.handle(_events(unstamped, M1))
    mid = _state(driver)
    _assert_one_chain(mid, "A", latest="M1", current_value=1500.0)
    sink.handle(_events(_award(), start=3))
    after = _state(driver)
    _assert_one_chain(after, "P1", latest="M1", current_value=1500.0)
    assert after["edges"] == [("AWARDED", "P1", "auth-1"),
                              ("AWARDED_TO", "P1", "co-1")]


def test_modification_only_contract_is_flagged(sink, neo4j):
    """Award never ingested (pre-eForms, placeholder back-link...): the
    entity exists, counts once, and says it has no award."""
    _, driver = neo4j
    sink.handle(_events(M1))
    state = _state(driver)
    _assert_one_chain(state, "P1", latest="M1", current_value=1500.0,
                      award_ingested=False)
    assert state["modifies"] == []


def test_replay_converges(sink, neo4j):
    _, driver = neo4j
    events = _events(_award(), M1, M2)
    sink.handle(events)
    first = _state(driver)
    sink.handle(events)
    sink.handle(_events(M2, _award(), M1))
    assert _state(driver) == first


def test_wrong_back_link_never_merges_two_contracts(sink, neo4j):
    """The hazard found in prod on 2026-09-13: a modification keyed by
    its own procedure whose back-link names ANOTHER contract's award.
    Both contracts keep their entities (the modification links to the
    award it named, and the split meter reports it); the other
    contract's award, modifications and edges stay where they were."""
    _, driver = neo4j
    award_a = _award()  # P1
    award_b = _award(ted_notice_id="B", ted_publication_number="900-2026",
                     contract_key="P2", procedure_id="P2",
                     publication_date="2026-02-01", value_eur=5000.0,
                     company_gmr_id="co-2")
    mod_b = _mod("MB", contract_key="P2", procedure_id="P2",
                 modifies_notice_id="B", publication_date="2026-04-01",
                 value_eur=5500.0, company_gmr_id="co-2")
    stray = _mod("MX", contract_key="P2", procedure_id="P2",
                 modifies_notice_id="A",  # names contract A's award by mistake
                 publication_date="2026-05-01", value_eur=6000.0,
                 company_gmr_id="co-2")
    sink.handle(_events(award_a, award_b, mod_b, stray))
    state = _state(driver)
    assert sorted(state["entities"]) == ["P1", "P2"]
    assert state["modifies"] == [("MB", "B"), ("MX", "A")]
    assert state["notices"]["B"]["entities"] == ["P2"]
    assert state["notices"]["MB"]["entities"] == ["P2"]
    assert state["notices"]["MX"]["entities"] == ["P2"]
    assert state["entities"]["P1"]["notice_count"] == 1
    assert state["entities"]["P2"]["notice_count"] == 3
    assert state["entities"]["P2"]["current_value"] == 6000.0
    # the same events, all in one batch or replayed, converge the same way
    sink.handle(_events(stray, award_a, mod_b, award_b))
    assert _state(driver) == state
