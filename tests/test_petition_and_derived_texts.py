"""Petitions in every language, and summaries and translations of longer
texts, against a real Neo4j: what a reader of the graph finds after the
sink has seen the events.

Needs Docker (testcontainers); the CI python runner has none, so the
module skips there. Locally, with the signed mirror of the prod image:

    NEO4J_TEST_IMAGE=contribute.void42.internal/fontem/neo4j:5.26.31-r2 \\
    TESTCONTAINERS_RYUK_DISABLED=true python3 -m pytest tests/test_petition_and_derived_texts.py
"""
# pytest fixtures are named as the tests use them.
# pylint: disable=redefined-outer-name
from __future__ import annotations

import os
from unittest import mock

import pytest

from tests.test_bracket_loss_repro import _ev
from tests.test_contract_chain_replay import _docker_available

testcontainers_neo4j = pytest.importorskip("testcontainers.neo4j")

pytestmark = pytest.mark.skipif(not _docker_available(),
                                reason="needs a reachable Docker daemon")

_IMAGE = os.environ.get(
    "NEO4J_TEST_IMAGE",
    "neo4j:5.26.30-community"
    "@sha256:22ec5cd05a8cbb372fc4bed5e384c30bc75fd92504c72be4462039761b105f61")

EU = ("bg", "cs", "da", "de", "el", "en", "es", "et", "fi", "fr", "ga", "hr", "hu",
      "it", "lt", "lv", "mt", "nl", "pl", "pt", "ro", "sk", "sl", "sv")


@pytest.fixture(scope="module")
def neo4j():
    os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
    with testcontainers_neo4j.Neo4jContainer(_IMAGE, password="testpass") as container:
        driver = container.get_driver()
        yield container.get_connection_url(), driver
        driver.close()


@pytest.fixture
def sink(neo4j):
    uri, driver = neo4j
    with driver.session() as s:
        s.run("MATCH (n) DETACH DELETE n")
    env = {"NEO4J_URI": uri, "NEO4J_USER": "neo4j", "NEO4J_PASSWORD": "testpass",
           "EVENT_CONSUMER_NAME": "neo4j_sink_test", "EVENTS_DATABASE_URL": "postgres://stub/stub"}
    with mock.patch.dict("os.environ", env):
        from neo4j_sink.sink import Neo4jSink  # pylint: disable=import-outside-toplevel
        from fontem_events.consumer import ConsumerConfig  # pylint: disable=import-outside-toplevel
        instance = Neo4jSink(ConsumerConfig(name="neo4j_sink_test", dsn="postgres://stub/stub",
                                            metrics_port=None))
    yield instance
    instance._driver.close()  # pylint: disable=protected-access


def _node(driver, cypher: str, **params) -> dict | None:
    with driver.session() as s:
        record = s.run(cypher, **params).single()
    return dict(record["n"]) if record else None


def _petition(driver):
    return _node(driver, "MATCH (n:Petition {system: 'eu-eci', petition_id: 'ECI(2024)000007'}) "
                         "RETURN n")


def _lobbyist(driver):
    return _node(driver, "MATCH (n:Disclosure:Lobbyist {system: 'eu-lobbying', "
                         "disclosure_id: '9218245390-27'}) RETURN n")


OBJECTIVES = ("Require publishers that sell or license videogames to consumers in the "
              "European Union to leave said videogames in a working (playable) state. ") * 8


def _upsert_petition(**over):
    p = {"system": "eu-eci", "petition_id": "ECI(2024)000007",
         "title": "Stop Destroying Videogames", "title_lang": "en", "status": "ANSWERED",
         "objectives": OBJECTIVES, "total_supporters": 0, "partially_registered": False,
         "supporter_countries": ["DE", "FR"], "supporter_counts": [229472, 95817],
         "versions": {code: {"title": f"Videogames ({code})", "objectives": f"{code}: {OBJECTIVES}",
                             "annex_text": f"Annex {code}"} for code in EU}}
    p.update(over)
    return _ev("UpsertPetition", p, 1)


def test_a_petition_reads_in_every_language_the_register_publishes(sink, neo4j):
    _uri, driver = neo4j
    sink.handle([_upsert_petition()])
    n = _petition(driver)
    assert n["objectives"] == OBJECTIVES and len(n["objectives"]) > 500
    assert n["title_lang"] == "en" and n["title_fr"] == "Videogames (fr)"
    assert all(n[f"objectives_{code}"].startswith(f"{code}: ") for code in EU)
    assert n["annex_text_de"] == "Annex de"
    assert n["supporter_counts"] == [229472, 95817]
    assert n["total_supporters"] == 0 and n["partially_registered"] is False


def test_a_language_the_register_withdraws_is_gone_and_an_older_producer_leaves_them(sink, neo4j):
    _uri, driver = neo4j
    sink.handle([_upsert_petition()])
    fewer = {code: {"title": f"Videogames ({code})", "objectives": "x"} for code in ("en", "fr")}
    sink.handle([_upsert_petition(versions=fewer)])
    n = _petition(driver)
    assert n["title_fr"] == "Videogames (fr)" and "title_de" not in n and "annex_text_en" not in n
    legacy = _upsert_petition()
    del legacy.payload["versions"]                       # a producer from before versions
    sink.handle([legacy])
    assert _petition(driver)["title_fr"] == "Videogames (fr)"


def _summary(objectives=OBJECTIVES, **summaries):
    return _ev("SummarizePetitionObjectives", {
        "system": "eu-eci", "petition_id": "ECI(2024)000007", "objectives": objectives,
        "source_lang": "en", "summaries": summaries or {
            "en": "Asks the EU to make publishers keep sold games playable.",
            "fr": "Demande à l'UE d'obliger les éditeurs à garder jouables les jeux vendus."},
        "method": "linguistics:nebius", "summarized_at": "2026-10-09T12:00:00Z"}, 2)


def test_a_summary_lands_on_the_objectives_it_summarises_and_nowhere_else(sink, neo4j):
    _uri, driver = neo4j
    sink.handle([_summary()])                            # no petition yet: nothing created
    assert _petition(driver) is None
    sink.handle([_upsert_petition(), _summary()])        # same batch: the petition first
    n = _petition(driver)
    assert n["objectives_summary_fr"].startswith("Demande")
    assert n["objectives_summary_lang"] == "en" and n["objectives_summarized_from"] == OBJECTIVES
    sink.handle([_summary(objectives="The objectives as they read last year.",
                          en="A summary of text the petition no longer has.")])
    assert _petition(driver)["objectives_summary_en"].startswith("Asks the EU")
    sink.handle([_summary(en="Asks the EU, said again.")])      # a newer set replaces the old
    n = _petition(driver)
    assert n["objectives_summary_en"] == "Asks the EU, said again."
    assert "objectives_summary_fr" not in n


GOALS = "Die Interessen der deutschen Brauwirtschaft gegenüber der EU vertreten. " * 9


def _registrant(goals=GOALS):
    return _ev("UpsertDisclosure", {
        "system": "eu-lobbying", "disclosure_id": "9218245390-27",
        "disclosure_type": "lobbyist-registration", "title": "Deutscher Brauer-Bund e.V.",
        "details": {"goals": goals, "country_iso": "DE"}}, 1)


def _goals_event(kind, text=GOALS, field="goals", **texts):
    body = {"system": "eu-lobbying", "disclosure_id": "9218245390-27", "field": field,
            "text": text, "source_lang": "de", "source_lang_origin": "detected",
            "detected_by": "nebius:deepseek-ai/DeepSeek-V4-Flash-0731",
            "method": "linguistics:nebius"}
    key = "translations" if kind == "TranslateDisclosureText" else "summaries"
    body[key] = texts
    return _ev(kind, body, 2)


def test_a_registrants_goals_read_in_another_language_with_where_they_came_from(sink, neo4j):
    _uri, driver = neo4j
    sink.handle([_registrant(),
                 _goals_event("TranslateDisclosureText", en="Representing German brewers.",
                              fr="Représenter les brasseurs allemands."),
                 _goals_event("SummarizeDisclosureText", de="Vertritt deutsche Brauer.",
                              en="Represents German brewers.")])
    n = _lobbyist(driver)
    assert n["detail_goals"] == GOALS
    assert n["detail_goals_en"] == "Representing German brewers."
    assert n["detail_goals_lang"] == "de" and n["detail_goals_lang_origin"] == "detected"
    assert n["detail_goals_lang_detected_by"] == "nebius:deepseek-ai/DeepSeek-V4-Flash-0731"
    assert n["detail_goals_translated_from"] == GOALS
    assert n["detail_goals_summary_de"] == "Vertritt deutsche Brauer."
    assert n["detail_goals_summary_en"] == "Represents German brewers."


def test_goals_rewritten_since_keep_their_old_translation_out(sink, neo4j):
    _uri, driver = neo4j
    rewritten = "Wir vertreten heute auch alkoholfreie Getränke."
    sink.handle([_registrant(rewritten),
                 _goals_event("TranslateDisclosureText", en="Representing German brewers.")])
    assert "detail_goals_en" not in _lobbyist(driver)
    sink.handle([_goals_event("TranslateDisclosureText", text=rewritten,
                              en="Today we also represent soft drinks.")])
    assert _lobbyist(driver)["detail_goals_en"] == "Today we also represent soft drinks."


def test_a_field_that_cannot_name_a_property_is_skipped(sink, neo4j):
    _uri, driver = neo4j
    sink.handle([_registrant(),
                 _goals_event("TranslateDisclosureText", field="goals} SET n.x = 1 //",
                              en="Injected?")])
    n = _lobbyist(driver)
    assert "x" not in n and not any(k.endswith("_en") for k in n)
