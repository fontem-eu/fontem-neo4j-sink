"""assess_link: the rule that decides whether a modification's
back-link may fold two contracts into one. The cases are real pairs
from prod (2026-09-21), reduced to the fields the rule reads."""
from neo4j_sink.plausibility import DOUBTFUL, OK, REJECT, assess_link

BG_MOD = {
    "procedure_id": "7efc32da-c813-4b5a-9e16-c26571bedcd9",
    "buyer_ids": ["351190ef"], "winner_ids": ["e37b7812"],
    "winner_names": ["АЛФАЛИФТ ООД"], "country": "BGR",
    "title": "Абонаментно сервизно обслужване и ремонт на асансьорни уредби",
}
DB_NETZ_MOD = {
    "buyer_ids": ["db-netz"], "winner_ids": ["glass-ing"],
    "winner_names": ["Glass Ingenieurbau GmbH"], "country": "DEU",
    "title": "VP 71, Bauleistungen - ESTW Angersdorf, Halle-Rosengarten",
}


def test_the_typo_that_fused_a_bulgarian_and_a_german_contract_is_rejected():
    v = assess_link(BG_MOD, DB_NETZ_MOD)
    assert v.status == REJECT and not v.linkable
    assert "BGR is not DEU" in v.reason


def test_same_buyer_is_enough_even_when_the_contractor_changed():
    award = dict(DB_NETZ_MOD, winner_ids=["other"], winner_names=["Kemna Bau"],
                 title="Something else entirely about tracks")
    assert assess_link(DB_NETZ_MOD, award).signals == ("buyer",)


def test_one_authority_under_two_ids_is_carried_by_the_contractor():
    mod = dict(DB_NETZ_MOD, buyer_ids=["db-netz-duplicate"], title="Nachtrag 7")
    v = assess_link(mod, DB_NETZ_MOD)
    assert v.status == OK and v.signals == ("winner",)


def test_a_renamed_contractor_is_matched_by_name():
    mod = {"buyer_ids": ["a2"], "winner_ids": ["c-9"], "country": "DEU",
           "winner_names": ["Wolfgang Scharnagel GmbH"], "title": "Nachtrag"}
    award = {"buyer_ids": ["a1"], "winner_ids": ["c-1"], "country": "DEU",
             "winner_names": ["Wolfgang Scharnagl GmbH"],
             "title": "Hochwasserschutz Olbernhau"}
    assert assess_link(mod, award).signals == ("winner_name",)


def test_a_title_with_a_reference_number_in_front_still_matches():
    mod = {"buyer_ids": ["a2"], "winner_ids": ["c-9"], "country": "DEU",
           "title": "21O50323   TUD Sanierung Beyer-Bau"}
    award = {"buyer_ids": ["a1"], "winner_ids": ["c-1"], "country": "DEU",
             "title": "TUD Sanierung Beyer-Bau"}
    assert assess_link(mod, award).signals == ("title",)


def test_one_shared_word_is_not_a_matching_title():
    mod = {"buyer_ids": ["a2"], "country": "DEU", "title": "Works"}
    award = {"buyer_ids": ["a1"], "country": "DEU",
             "title": "Works on the northern bypass, lot 4"}
    assert assess_link(mod, award).status == DOUBTFUL


def test_nothing_in_common_in_one_country_links_but_is_marked():
    mod = {"buyer_ids": ["a2"], "winner_ids": ["c2"], "country": "DEU",
           "winner_names": ["Seele GmbH"],
           "title": "Stuttgart, Bahnhofshalle, Fassaden Glasdächer"}
    award = {"buyer_ids": ["a1"], "winner_ids": ["c1"], "country": "DEU",
             "winner_names": ["BIEGE S21, PFA 1.1"],
             "title": "S21, PFA 1.1 Rohbau Los 3"}
    v = assess_link(mod, award)
    assert v.status == DOUBTFUL and v.linkable


def test_the_same_procedure_outranks_everything():
    mod = dict(BG_MOD, country="BGR")
    award = dict(DB_NETZ_MOD, procedure_id=BG_MOD["procedure_id"])
    assert assess_link(mod, award).signals == ("procedure",)


def test_unknown_is_never_treated_as_different():
    """A stub target (only an id) or a notice written before parties
    were stamped: there is nothing to contradict, so the link stands."""
    assert assess_link(BG_MOD, {}).status == OK
    assert assess_link(BG_MOD, {"country": "DEU"}).status == OK
    assert assess_link({}, DB_NETZ_MOD).status == OK


def test_different_countries_alone_do_not_reject():
    """A cross-border buyer: the country differs but the buyer id is
    the same authority."""
    award = dict(DB_NETZ_MOD, country="AUT", title="x y z", winner_ids=["q"],
                 winner_names=["Q"])
    assert assess_link(DB_NETZ_MOD, award).status == OK
