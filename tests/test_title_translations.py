"""Title translations land only on the title they translate, and never
create the entity they name."""
from __future__ import annotations

from neo4j_sink import translations
from neo4j_sink.cypher import RENDERERS
from neo4j_sink.sink import Neo4jSink


def _contract_event(**over):
    p = {"contract_key": "344226-2021", "ted_notice_id": "344226-2021",
         "title": "Roboty budowlane", "source_lang": "pl", "source_lang_origin": "stated",
         "translations": {"en": "Construction works", "de": "Bauarbeiten"},
         "method": "nebius:google/gemma-3-27b-it", "translated_at": "2026-09-29T08:00:00Z"}
    p.update(over)
    return p


def test_the_translation_set_is_replaced_whole():
    """Every EU language is in the write: the absent ones as None, which
    SET += removes, so a retitle cannot keep the old title's translations."""
    w = RENDERERS["TranslateContractTitle"](_contract_event())
    props = w.set_props["props"]
    assert {k for k in props if k.startswith("title_") and len(k) == 8} == {
        f"title_{c}" for c in translations.EU_LANGS}
    assert props["title_en"] == "Construction works" and props["title_pl"] is None
    assert props["title_translated_from"] == "Roboty budowlane"
    assert "title_lang" not in props


def test_a_detected_language_is_recorded_as_detected_never_as_stated():
    w = RENDERERS["TranslateDisclosureTitle"]({
        "system": "eu-cohesion", "disclosure_id": "Q7", "title": "Fondų fondas",
        "source_lang": "lt", "source_lang_origin": "detected",
        "detected_by": "nebius:google/gemma-3-27b-it",
        "translations": {"en": "Fund of Funds"}, "translated_at": "2026-09-29T08:00:00Z"})
    props = w.set_props["props"]
    assert props["title_lang_detected"] == "lt"
    assert props["title_lang_detected_by"] == "nebius:google/gemma-3-27b-it"
    assert "title_lang" not in props
    stated = RENDERERS["TranslateContractTitle"](_contract_event()).set_props["props"]
    assert stated["title_lang_detected"] is None     # a stated title clears an old guess


def test_an_empty_translation_writes_nothing():
    assert RENDERERS["TranslateContractTitle"](_contract_event(translations={})) is None


class _Session:
    def __init__(self, applied):
        self.runs = []
        self._applied = applied

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def run(self, cypher, **params):
        self.runs.append((cypher, params))
        applied = self._applied

        class _R:
            @staticmethod
            def single():
                return {"applied": applied(params["rows"])}
        return _R()


class _Driver:
    def __init__(self, applied=len):
        self.session_ = _Session(applied)

    def session(self):
        return self.session_


def test_it_matches_on_the_title_and_never_merges():
    driver = _Driver()
    writes = [RENDERERS["TranslateContractTitle"](_contract_event()),
              RENDERERS["TranslateDisclosureTitle"]({
                  "system": "eu-cohesion", "disclosure_id": "Q7", "title": "T",
                  "source_lang_origin": "unknown", "translations": {"en": "T"}})]
    assert translations.apply_title_translations(driver, writes) == 2
    cyphers = [c for c, _ in driver.session_.runs]
    contract_match = ("MATCH (n:Contract { contract_key: row.contract_key }) "
                      "WHERE n.title = row.title")
    assert any(contract_match in c for c in cyphers)
    assert any("MATCH (n:Disclosure { disclosure_id: row.disclosure_id, system: row.system })"
               in c for c in cyphers)
    assert not any("MERGE" in c for c in cyphers)


def test_a_translation_of_a_title_the_entity_no_longer_shows_lands_nowhere():
    driver = _Driver(applied=lambda rows: 0)
    w = RENDERERS["TranslateContractTitle"](_contract_event())
    assert translations.apply_title_translations(driver, [w]) == 0


def test_translations_apply_after_the_entities_of_the_same_batch(monkeypatch):
    """Otherwise a contract loaded and translated in one batch would be
    matched before its title exists."""
    order = []
    sink = Neo4jSink.__new__(Neo4jSink)
    sink._driver = object()  # pylint: disable=protected-access
    monkeypatch.setattr(sink, "_flush_nodes",
                        lambda nodes: order.append(("nodes", len(nodes))) or [])
    monkeypatch.setattr(sink, "_flush_extra_relationships", lambda _items: None)
    monkeypatch.setattr(sink, "_apply_contract_chains", lambda _c: order.append(("chains", 0)))
    monkeypatch.setattr(sink, "_flush_typed_relationships", lambda _r: None)
    monkeypatch.setattr(translations, "apply_title_translations",
                        lambda _d, ws: order.append(("titles", len(ws))))
    contract = RENDERERS["UpsertCompany"]({"gmr_id": "g-1", "name": "Acme"})
    title = RENDERERS["TranslateContractTitle"](_contract_event())
    sink._apply_batch([title, contract])  # pylint: disable=protected-access
    assert order == [("nodes", 1), ("chains", 0), ("titles", 1)]
