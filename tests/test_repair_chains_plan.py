"""repair_chains' decisions, without a database: which event is a whole
notice, what a plan concludes, what the command line does with it. The
graph side is covered against a real Neo4j in test_repair_chains."""
# pylint: disable=protected-access
from __future__ import annotations

import json
from unittest import mock

import pytest

from neo4j_sink import repair_chains
from neo4j_sink.repair_chains import (
    IRI, Plan, Repairer, facts, is_keyed_by_back_link, own_key, with_identity,
)


class FakeLog:
    """events.entity_events, newest first per IRI — all repair reads."""

    def __init__(self):
        self.rows: dict[str, list[tuple[int, dict]]] = {}
        self._seq = 0
        self._hit: list = []

    def add(self, payload: dict) -> None:
        self._seq += 1
        self.rows.setdefault(IRI.format(payload["ted_notice_id"]), []).insert(
            0, (self._seq, payload))

    def cursor(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, _sql, params):
        self._hit = self.rows.get(params[0], [])

    def fetchall(self):
        return list(self._hit)


class FakeGraph:
    """A driver whose session answers the two reads plan() makes."""

    def __init__(self, notices, resolved):
        self._notices, self._resolved = notices, resolved
        self.queries: list[str] = []

    def session(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def run(self, query, **_params):
        self.queries.append(query)
        rows = self._notices if query == repair_chains._NOTICES_CYPHER else self._resolved
        return [mock.Mock(data=lambda r=r: r, __getitem__=lambda _s, k, r=r: r[k])
                for r in rows]


def _repairer(log, notices, resolved):
    sink = mock.Mock()
    sink._driver = FakeGraph([{"nid": n} for n in notices], resolved)
    return Repairer(sink, log), sink


DE_AWARD = {"ted_notice_id": "DE-A", "ted_publication_number": "375716-2020",
            "contract_key": "375716-2020", "country": "DEU", "notice_type": "can-standard",
            "title": "VP 71 Bauleistungen ESTW Angersdorf", "authority_id": "db-netz",
            "company_gmr_id": "glass", "publication_date": "2020-08-10"}
DE_MOD = {"ted_notice_id": "DE-M", "ted_publication_number": "37303-2022",
          "contract_key": "375716-2020", "country": "DEU", "notice_type": "can-modif",
          "modifies_publication_number": "375716-2020", "authority_id": "db-netz",
          "title": "VP 71 Bauleistungen ESTW Angersdorf Nachtrag",
          "parties": [{"role": "winner", "name": "Glass Ingenieurbau GmbH",
                       "company_gmr_id": "glass"},
                      {"role": "tenderer", "name": "Someone Else", "company_gmr_id": "x"}],
          "publication_date": "2022-01-20"}
BG_MOD = {"ted_notice_id": "BG-M", "ted_publication_number": "81781-2024",
          "contract_key": "BGPROC", "procedure_id": "BGPROC", "country": "BGR",
          "notice_type": "can-modif", "modifies_publication_number": "37303-2022",
          "title": "Абонаментно сервизно обслужване на асансьори",
          "authority_id": "unwe", "company_gmr_id": "alfalift",
          "publication_date": "2024-02-08"}


def test_facts_reads_winners_only_and_keeps_unknown_empty():
    f = facts(DE_MOD)
    assert f["buyer_ids"] == ["db-netz"] and f["winner_ids"] == ["glass"]
    assert f["winner_names"] == ["Glass Ingenieurbau GmbH"]
    bare = facts({"ted_notice_id": "X"})
    assert not bare["buyer_ids"] and not bare["winner_ids"] and bare["title"] is None


def test_own_key_prefers_procedure_then_publication_number_then_notice_id():
    assert own_key(BG_MOD) == "BGPROC"
    assert own_key(DE_MOD) == "37303-2022"
    assert own_key({"ted_notice_id": "only-this"}) == "only-this"


def test_only_a_legacy_modification_is_keyed_by_its_back_link():
    assert is_keyed_by_back_link(DE_MOD)
    assert not is_keyed_by_back_link(BG_MOD)        # eForms: its procedure id
    assert not is_keyed_by_back_link(DE_AWARD)      # no back-link at all


def test_the_newest_whole_notice_wins_over_a_later_value_patch():
    log = FakeLog()
    log.add(DE_MOD)
    log.add({"ted_notice_id": "DE-M", "contract_key": "375716-2020",
             "is_current": False, "current_value": None})
    repairer, _ = _repairer(log, [], [])
    seq, payload = repairer.whole_notice("DE-M")
    assert seq == 1 and payload["title"] == DE_MOD["title"]
    assert repairer.whole_notice("never-seen") is None


def test_a_pre_native_notice_is_keyed_the_way_the_ingest_path_keys_it_today():
    """Mid-2026 events carry no contract_key (350010-2021 in prod).
    Mirrors fontem-api load_ted_contracts.derive_contract_key."""
    legacy_mod = {k: v for k, v in DE_MOD.items() if k != "contract_key"}
    assert with_identity(legacy_mod)["contract_key"] == "375716-2020"   # its back-link
    legacy_award = {k: v for k, v in DE_AWARD.items() if k != "contract_key"}
    assert with_identity(legacy_award)["contract_key"] == "375716-2020"  # its own number
    eforms = {k: v for k, v in BG_MOD.items() if k != "contract_key"}
    assert with_identity(eforms)["contract_key"] == "BGPROC"            # procedure id
    assert with_identity({"ted_notice_id": "bare"})["contract_key"] == "bare"
    assert with_identity(DE_MOD) is DE_MOD                               # already stamped
    log = FakeLog()
    log.add(legacy_mod)
    repairer, _ = _repairer(log, [], [])
    assert repairer.whole_notice("DE-M")[1]["contract_key"] == "375716-2020"
    assert "contract_key" not in log.rows[IRI.format("DE-M")][0][1]      # log untouched


def test_a_payload_stored_as_text_is_parsed():
    log = FakeLog()
    log.rows[IRI.format("DE-A")] = [(7, json.dumps(DE_AWARD))]
    repairer, _ = _repairer(log, [], [])
    assert repairer.whole_notice("DE-A") == (7, DE_AWARD)


def test_plan_separates_the_stranger_and_explains_itself():
    log = FakeLog()
    for p in (DE_AWARD, DE_MOD, BG_MOD):
        log.add(p)
    repairer, sink = _repairer(log, ["DE-A", "DE-M", "BG-M"], [
        {"m": "DE-M", "target": "DE-A", "linked": True},
        {"m": "BG-M", "target": "DE-M", "linked": True},
    ])
    plan = repairer.plan("BGPROC")
    assert [(r["m"], r["t"]) for r in plan.refused] == [("BG-M", "DE-M")]
    assert plan.accepted == [("DE-M", "DE-A")]
    assert sorted(sorted(c) for c in plan.contracts) == [["BG-M"], ["DE-A", "DE-M"]]
    assert plan.changes_anything
    text = plan.describe()
    assert "refuse BG-M -> DE-M" in text and "BGR is not DEU" in text
    assert "contract of 2 notices [DEU]" in text
    sink.handle.assert_not_called()                  # planning never writes


def test_a_refused_link_the_graph_no_longer_acts_on_needs_no_repair():
    log = FakeLog()
    log.add(BG_MOD)
    log.add(DE_MOD)
    repairer, _ = _repairer(log, ["BG-M"], [
        {"m": "BG-M", "target": "DE-M", "linked": False}])
    plan = repairer.plan("BGPROC")
    assert len(plan.refused) == 1 and not plan.changes_anything
    assert "(already unlinked)" in plan.describe()
    assert "nothing to repair" in plan.describe()


def test_a_legacy_modification_with_a_refused_back_link_is_rekeyed():
    estonian = {"ted_notice_id": "EE-A", "ted_publication_number": "111-2021",
                "contract_key": "111-2021", "country": "EST", "authority_id": "ee",
                "notice_type": "can-standard", "title": "Meditsiinitarvikud",
                "company_gmr_id": "ee-med"}
    czech = {"ted_notice_id": "CZ-M", "ted_publication_number": "222-2022",
             "contract_key": "111-2021", "modifies_publication_number": "111-2021",
             "country": "CZE", "authority_id": "cz", "company_gmr_id": "cz-b",
             "notice_type": "can-modif", "title": "I/42 Brno, VMO Bauerova"}
    log = FakeLog()
    log.add(estonian)
    log.add(czech)
    repairer, _ = _repairer(log, ["EE-A", "CZ-M"], [
        {"m": "CZ-M", "target": "EE-A", "linked": False}])
    plan = repairer.plan("111-2021")
    assert plan.rekeyed == {"CZ-M": "222-2022"}
    assert plan.payloads["CZ-M"]["contract_key"] == "222-2022"
    assert log.rows[IRI.format("CZ-M")][0][1]["contract_key"] == "111-2021"  # log untouched
    assert plan.changes_anything and "re-key CZ-M" in plan.describe()


def test_a_notice_with_no_whole_event_blocks_the_rebuild():
    log = FakeLog()
    log.add(DE_MOD)
    repairer, sink = _repairer(log, ["DE-A", "DE-M"], [])
    plan = repairer.plan("375716-2020")
    assert plan.missing == ["DE-A"] and "NOT REBUILDABLE" in plan.describe()
    with pytest.raises(SystemExit, match="cannot be replayed"):
        repairer.rebuild(plan)
    sink.handle.assert_not_called()


def test_an_absurdly_large_entity_is_refused():
    log = FakeLog()
    repairer, _ = _repairer(log, [f"n{i}" for i in range(repair_chains._MAX_NOTICES + 1)], [])
    with pytest.raises(SystemExit, match="look at it by hand"):
        repairer.plan("huge")


def test_empty_plan_has_no_contracts():
    assert Plan("k").contracts == [] and not Plan("k").changes_anything


# ── command line ──────────────────────────────────────────────────


def _cli(monkeypatch, plans, suspects=()):
    repairer = mock.Mock()
    repairer.suspects.return_value = [{"key": k} for k in suspects]
    repairer.plan.side_effect = lambda key: plans[key]
    repairer.rebuild.return_value = [{"key": "K", "ours": 2, "notices": 2,
                                      "award": True, "value": 1.0, "buyers": ["B"]}]
    monkeypatch.setattr(repair_chains, "_connect", lambda: repairer)
    return repairer


def _broken(key="K"):
    plan = Plan(key, payloads={"a": {"contract_key": "1", "ted_notice_id": "a"},
                               "b": {"contract_key": "2", "ted_notice_id": "b"}})
    assert plan.changes_anything
    return plan


def test_rebuild_without_apply_changes_nothing(monkeypatch, capsys):
    repairer = _cli(monkeypatch, {"K": _broken()})
    assert repair_chains.main(["rebuild", "--entity", "K"]) == 0
    repairer.rebuild.assert_not_called()
    assert "1 of 1 entities need repair" in capsys.readouterr().out


def test_rebuild_apply_rebuilds_only_what_is_broken(monkeypatch, capsys):
    healthy = Plan("H", payloads={"a": {"contract_key": "1", "ted_notice_id": "a"}})
    repairer = _cli(monkeypatch, {"K": _broken(), "H": healthy}, suspects=["K", "H"])
    assert repair_chains.main(["rebuild", "--suspects", "--apply"]) == 0
    assert [c.args[0].key for c in repairer.rebuild.call_args_list] == ["K"]
    assert "now K: 2 of these notices" in capsys.readouterr().out


def test_scan_lists_only_entities_that_need_repair(monkeypatch, capsys):
    healthy = Plan("H", payloads={"a": {"contract_key": "1", "ted_notice_id": "a"}})
    _cli(monkeypatch, {"K": _broken(), "H": healthy}, suspects=["K", "H"])
    assert repair_chains.main(["scan"]) == 0
    out = capsys.readouterr().out
    assert "entity K" in out and "entity H" not in out
    assert "1 of 2 entities need repair" in out


def test_plan_needs_a_target(monkeypatch):
    _cli(monkeypatch, {})
    with pytest.raises(SystemExit):
        repair_chains.main(["plan"])
