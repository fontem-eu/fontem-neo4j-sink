# pylint: disable=protected-access
# The name_clean materialisation tests below pin down the behavior of
# Neo4jSink._name_clean_fragment, which is the kind of "do not regress"
# private-helper coverage that lives next to the symbol. The
# underscore prefix on the helper marks it sink-internal vs API;
# pylint's blanket no-touch policy is the wrong default here.
from unittest.mock import MagicMock
from neo4j_sink.cypher import (
    CypherWrite,
    render_upsert_investment_fund,
    RENDERERS, label_for_graph,
    render_assert_same_as, render_upsert_authority,
    render_upsert_company, render_upsert_contract,
    render_upsert_disclosure, render_upsert_exchange_rate,
    render_upsert_filing, render_upsert_listing,
    render_upsert_petition, render_upsert_relationship,
    render_upsert_sanctioned_entity,
    render_upsert_taxonomy_code,
    render_translate_authority_name,
)
from neo4j_sink.sink import Neo4jSink


def test_company_round_trip():
    w = render_upsert_company({
        "gmr_id": "abc", "name": "Foo", "country": "DE",
        "lei": None,  # nulls dropped
    })
    assert w.label == "Company"
    assert w.primary_key == {"gmr_id": "abc"}
    assert "lei" not in w.set_props


def test_sanction_keyed_by_entity_id():
    w = render_upsert_sanctioned_entity({
        "entity_id": "x", "eu_reference": "EU.1",
    })
    assert w.label == "SanctionedEntity"
    assert w.primary_key == {"entity_id": "x"}
    # absent subject_type (pre-2026-07-14 events) sets no property
    assert "subject_type" not in w.set_props

    person = render_upsert_sanctioned_entity({
        "entity_id": "p", "eu_reference": "EU.2", "subject_type": "person",
    })
    assert person.set_props["subject_type"] == "person"


def test_filing_extra_relationship_to_company():
    w = render_upsert_filing({
        "gmr_id": "abc", "year": 2024, "source": "edgar",
    })
    assert w.label == "FinancialYear"
    assert ("REPORTED",
            "http://data.fontem.eu/id/Company/abc",
            {"year": 2024, "_direction": "from_target"}) in (
        w.extra_relationships or []
    )


def test_same_as_carries_iris_in_key():
    w = render_assert_same_as({
        "a_iri": "http://x", "b_iri": "http://y",
        "confidence": 0.9, "method": "exact_lei",
    })
    assert w.label == "_SameAs"
    assert w.primary_key == {"a_iri": "http://x", "b_iri": "http://y"}
    assert w.set_props["confidence"] == 0.9


def test_label_for_graph():
    assert label_for_graph(
        "http://data.fontem.eu/graph/sanctions"
    ) == "SanctionedEntity"
    assert label_for_graph(
        "http://data.fontem.eu/graph/financials/edgar"
    ) == "FinancialYear"
    assert label_for_graph("http://elsewhere") is None


def test_listing_keyed_by_ticker_with_listed_as_edge():
    w = render_upsert_listing({
        "ticker": "AAPL",
        "company_gmr_id": "00040372-dad6-5d34-882c-8b8624b4e734",
        "exchange": "US", "currency": "USD", "active": True,
    })
    assert w.label == "Listing"
    assert w.primary_key == {"ticker": "AAPL"}
    assert w.set_props["exchange"] == "US"
    # Company → Listing edge gets materialised as `from_target`
    # (target_iri is the Company; the edge runs Company-[:LISTED_AS]->Listing).
    assert ("LISTED_AS",
            "http://data.fontem.eu/id/Company/00040372-dad6-5d34-882c-8b8624b4e734",
            {"_direction": "from_target"}) in (w.extra_relationships or [])


def test_authority_basic():
    w = render_upsert_authority({
        "authority_id": "auth-1", "name": "X", "country": "FR",
        "authority_type": "regulator",
    })
    assert w.label == "Authority"
    assert w.primary_key == {"authority_id": "auth-1"}
    assert w.set_props["authority_type"] == "regulator"


def test_translate_authority_name_basic():
    w = render_translate_authority_name({
        "authority_id": "auth-1",
        "name": "Urząd Miasta",
        "source_lang": "pl",
        "translations": {"de": "Stadtamt", "fr": "Mairie"},
        "method": "mistral-medium",
        "translated_at": "2026-07-25T12:00:00Z",
    })
    assert w is not None
    assert w.label == "Authority"
    assert w.primary_key == {"authority_id": "auth-1"}
    assert w.set_props["name_de"] == "Stadtamt"
    assert w.set_props["name_fr"] == "Mairie"
    assert w.set_props["name_lang"] == "pl"
    assert w.set_props["multilingual_updated_at"] == "2026-07-25T12:00:00Z"
    # Additive only: the canonical `name` must not be written here.
    assert "name" not in w.set_props


def test_translate_authority_name_empty_translations_is_none():
    assert render_translate_authority_name({
        "authority_id": "auth-1", "translations": {},
    }) is None
    assert render_translate_authority_name({
        "authority_id": "auth-1",
    }) is None


def test_contract_links_authority_and_company():
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS123-456789",
        "title": "Some contract",
        "authority_id": "auth-1",
        "company_gmr_id": "abc",
        "value_eur": 1000000.0,
    })
    assert w.label == "Contract"
    assert w.primary_key == {"ted_notice_id": "2025-OJS123-456789"}
    rels = w.extra_relationships or []
    # Authority -[:AWARDED]-> Contract  (from_target: target IRI is the Authority)
    assert ("AWARDED",
            "http://data.fontem.eu/id/Authority/auth-1",
            {"_direction": "from_target"}) in rels
    # Contract -[:AWARDED_TO]-> Company (from_source: source is the Contract)
    assert ("AWARDED_TO",
            "http://data.fontem.eu/id/Company/abc",
            {"_direction": "from_source"}) in rels


def test_contract_match_provenance_rides_the_awarded_to_edge():
    """match_tier/confidence/layer land on the AWARDED_TO edge (so exact
    vs name-based attributions are queryable per-edge), never on the
    Contract node."""
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS123-456789",
        "company_gmr_id": "abc",
        "match_tier": "name_country",
        "match_confidence": 0.95,
        "match_layer": 2,
    })
    awarded_to = [r for r in (w.extra_relationships or [])
                  if r[0] == "AWARDED_TO"]
    assert len(awarded_to) == 1
    _, target, props = awarded_to[0]
    assert target == "http://data.fontem.eu/id/Company/abc"
    assert props == {
        "_direction": "from_source",
        "match_tier": "name_country",
        "match_confidence": 0.95,
        "match_layer": 2,
    }
    for k in ("match_tier", "match_confidence", "match_layer"):
        assert k not in w.set_props


def test_contract_omits_relationships_when_keys_missing():
    """A bare contract event with no authority/company should not
    fabricate target IRIs — the sink would try to MATCH a missing
    node and fail."""
    w = render_upsert_contract({"ted_notice_id": "2025-OJS999-000000"})
    assert w.extra_relationships in (None, [])


def test_contract_writes_country_from_payload():
    """country is the alpha-3 of the contracting authority, cascaded
    onto the Contract at write time so jurisdictional panels (eg
    /data-quality/contracts/by-country) don't need to traverse to
    Authority. Earlier shape didn't include country on Contract — all
    56k contracts in staging reported country=NULL until this fix."""
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS123-456789",
        "authority_id": "auth-1",
        "country": "DEU",
    })
    assert w.set_props["country"] == "DEU"


def test_contract_omits_country_when_unset():
    """Missing country must not write an empty/None property — leaves
    the Contract.country untouched so a follow-up update with a real
    country can fill it."""
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS123-456789",
        "title": "no country here",
    })
    assert "country" not in w.set_props


def test_contract_writes_ted_publication_number_from_payload():
    """ted_publication_number is the human-readable TED identifier
    (e.g. "295342-2026") captured by the ETL via TED's v3 search API.
    Persisting it on the Contract node lets the API short-circuit the
    runtime UUID→pub-num lookup when building the canonical TED
    detail URL — without it every link click re-issues a TED search
    request and pays the LRU-miss cost on cold pods."""
    w = render_upsert_contract({
        "ted_notice_id": "912f1717-1ace-413d-aa61-cd21cd6b95e7",
        "ted_publication_number": "295342-2026",
    })
    assert w.set_props["ted_publication_number"] == "295342-2026"


def test_contract_omits_ted_publication_number_when_unset():
    """When the ETL couldn't resolve the publication-number (notice
    queued but not yet published, or TED search returned no match),
    don't write an empty property — leaves the Contract row clean
    so a subsequent ETL pass that does resolve it can fill in."""
    w = render_upsert_contract({
        "ted_notice_id": "912f1717-1ace-413d-aa61-cd21cd6b95e7",
        "title": "queued-only notice",
    })
    assert "ted_publication_number" not in w.set_props


def test_contract_persists_value_quality_signals():
    """The ETL's confidence scorer writes value_eur (the chosen,
    TotalAmount-preferred value) plus the estimate / payable cross-checks
    and the confidence + flag onto the node. value_low_confidence and
    value_payable_discrepancy are stored even when False — the False is
    meaningful (kept and counted)."""
    w = render_upsert_contract({
        "ted_notice_id": "n1",
        "value_eur": 7_274_615.93,
        "estimated_value_eur": 7_317_073.17,
        "value_payable_eur": 7_274_615_930.0,
        "value_confidence": 0.71,
        "value_confidence_consistency": 0.71,
        "value_confidence_plausibility": 1.0,
        "value_quality_flag": "ok",
        "value_low_confidence": False,
        "value_payable_discrepancy": True,
    })
    assert w.set_props["value_eur"] == 7_274_615.93
    assert w.set_props["estimated_value_eur"] == 7_317_073.17
    assert w.set_props["value_payable_eur"] == 7_274_615_930.0
    assert w.set_props["value_confidence"] == 0.71
    assert w.set_props["value_quality_flag"] == "ok"
    assert w.set_props["value_low_confidence"] is False
    assert w.set_props["value_payable_discrepancy"] is True


def test_contract_flags_low_confidence_value():
    """A flagged value is persisted with value_low_confidence True so DQ /
    coverage queries can exclude it from default aggregates."""
    w = render_upsert_contract({
        "ted_notice_id": "n2",
        "value_eur": 1.07e12,
        "value_quality_flag": "implausible_magnitude",
        "value_low_confidence": True,
    })
    assert w.set_props["value_low_confidence"] is True
    assert w.set_props["value_quality_flag"] == "implausible_magnitude"


def test_taxonomy_code_keyed_by_system_and_code():
    w = render_upsert_taxonomy_code({
        "system": "cpv", "code": "45000000",
        "label": "Construction work", "label_lang": "en",
    })
    assert w.label == "TaxonomyCode"
    assert w.primary_key == {"system": "cpv", "code": "45000000"}
    assert w.set_props["label"] == "Construction work"
    # No parent → no extra relationships.
    assert not (w.extra_relationships or [])


def test_taxonomy_code_parent_emits_child_of_edge():
    w = render_upsert_taxonomy_code({
        "system": "nuts", "code": "FR101", "parent_code": "FR1",
    })
    rels = w.extra_relationships or []
    assert len(rels) == 1
    rel_type, target_iri, props = rels[0]
    assert rel_type == "CHILD_OF"
    assert target_iri.endswith("/Nuts/FR1")
    assert props["_direction"] == "from_source"


def test_relationship_carries_iris_in_key():
    w = render_upsert_relationship({
        "src_iri": "http://data.fontem.eu/id/Company/A",
        "dst_iri": "http://data.fontem.eu/id/Company/B",
        "predicate": "parentOf",
        "valid_from": "2020-01-01",
    })
    assert w.label == "_Relationship"
    assert w.primary_key["predicate"] == "parentOf"
    assert w.set_props["valid_from"] == "2020-01-01"


def test_disclosure_filed_by_edge_to_company():
    w = render_upsert_disclosure({
        "system": "cdp",
        "disclosure_id": "CDP-12345",
        "company_gmr_id": "00040372-dad6-5d34-882c-8b8624b4e734",
        "year": 2024,
        "details": {"score": "A", "scope1": 1000.0},
    })
    assert w.label == "Disclosure"
    assert w.primary_key == {
        "system": "cdp", "disclosure_id": "CDP-12345",
    }
    assert w.set_props["detail_score"] == "A"
    rels = w.extra_relationships or []
    assert any(r[0] == "FILED_BY" for r in rels)


def test_lobbying_disclosure_gets_lobbyist_label_and_keeps_interests():
    """eu-lobbying disclosures must carry the :Lobbyist secondary label
    and preserve the `interests` list (a Neo4j array property), so the
    dashboard can query :Lobbyist and `'climate' IN d.detail_interests`."""
    w = render_upsert_disclosure({
        "system": "eu-lobbying",
        "disclosure_id": "TR-987",
        "details": {
            "name": "Acme Lobby",
            "interests": ["climate", "energy", "trade"],
            "category": "Companies",
        },
    })
    assert w.label == "Disclosure"
    assert w.extra_labels == ["Lobbyist"]
    assert w.set_props["detail_interests"] == ["climate", "energy", "trade"]
    assert w.set_props["detail_name"] == "Acme Lobby"


def test_cohesion_disclosure_label_and_programme_link():
    """eu-cohesion disclosures get the :CohesionProject label + an
    UNDER_PROGRAMME edge to the :Programme node (from programme_code in
    details, which is itself not stored as a detail property)."""
    w = render_upsert_disclosure({
        "system": "eu-cohesion",
        "disclosure_id": "Q123",
        "company_gmr_id": "ben-1",
        "details": {
            "programme": "Competitiveness PL", "fund": "ERDF",
            "programme_code": "prog-abc", "eu_contribution": 59877.7,
        },
    })
    assert w.extra_labels == ["CohesionProject"]
    assert "detail_programme_code" not in w.set_props
    assert w.set_props["detail_programme"] == "Competitiveness PL"
    rels = {(r[0], r[1]) for r in (w.extra_relationships or [])}
    assert ("FILED_BY", "http://data.fontem.eu/id/Company/ben-1") in rels
    assert ("UNDER_PROGRAMME",
            "http://data.fontem.eu/id/Programme/prog-abc") in rels


def test_non_lobbying_disclosure_has_no_extra_label():
    w = render_upsert_disclosure({
        "system": "cdp", "disclosure_id": "CDP-1", "details": {"score": "A"},
    })
    assert w.extra_labels is None


def test_nested_dict_details_still_dropped_but_scalar_list_kept():
    w = render_upsert_disclosure({
        "system": "eu-lobbying", "disclosure_id": "TR-1",
        "details": {
            "interests": ["a", "b"],          # scalar list -> kept
            "nested": {"x": 1},               # dict -> dropped
            "mixed": [{"y": 2}],              # non-scalar list -> dropped
        },
    })
    assert w.set_props["detail_interests"] == ["a", "b"]
    assert "detail_nested" not in w.set_props
    assert "detail_mixed" not in w.set_props


def test_disclosure_omits_filed_by_when_no_company():
    """EU lobbying register entries: registrant IS the Lobbyist,
    no parent Company. The renderer must skip the FILED_BY edge so
    the sink doesn't try to MATCH a non-existent Company node."""
    w = render_upsert_disclosure({
        "system": "eu-lobbying",
        "disclosure_id": "EU-TR-12345",
        "year": 2024,
        "details": {"members_fte": 4},
    })
    assert w.label == "Disclosure"
    assert w.extra_relationships in (None, [])


def test_exchange_rate_composite_key():
    w = render_upsert_exchange_rate({
        "base": "EUR", "target": "USD",
        "date": "2025-09-15", "rate": 1.0473, "source": "ecb",
    })
    assert w.label == "ExchangeRate"
    assert w.primary_key == {
        "base": "EUR", "target": "USD", "date": "2025-09-15",
    }
    assert w.set_props["rate"] == 1.0473
    assert w.set_props["source"] == "ecb"


def test_renderer_registry_covers_all_event_types():
    expected = {
        "BeginGraphReplace", "EndGraphReplace",
        "UpsertCompany", "UpsertInvestmentFund", "UpsertListing",
        "UpsertPetition", "UpsertSanctionedEntity", "UpsertFiling",
        "UpsertAuthority", "TranslateAuthorityName", "UpsertContract",
        "UpsertTaxonomyCode", "UpsertRelationship",
        "UpsertDisclosure", "UpsertExchangeRate",
        "AssertSameAs",
    }
    assert set(RENDERERS) == expected


# ── name_clean materialisation (sink-side) ──────────────────────

def test_name_clean_fragment_company_with_name():
    """Company writes with `name` set must materialise name_clean
    so the consolidator's resolver can use the indexed column."""
    frag = Neo4jSink._name_clean_fragment("Company", has_name=True)
    assert "n.name_clean" in frag
    assert "apoc.text.clean(row.name)" in frag


def test_name_clean_fragment_authority_with_name():
    frag = Neo4jSink._name_clean_fragment("Authority", has_name=True)
    assert "n.name_clean" in frag


def test_name_clean_fragment_skipped_for_other_labels():
    """Listing/Contract/SanctionedEntity don't get name_clean — the
    resolver only matches Company + Authority by it."""
    for label in ("Listing", "Contract", "SanctionedEntity", "FinancialYear"):
        assert Neo4jSink._name_clean_fragment(label, has_name=True) == ""


def test_name_clean_fragment_skipped_when_name_missing():
    """A consolidator partial-update event that doesn't include `name`
    must not blank out an existing name_clean by computing
    apoc.text.clean(NULL)."""
    assert Neo4jSink._name_clean_fragment("Company", has_name=False) == ""


def test_filing_rejects_implausible_year():
    # Botched XBRL period-end → no FinancialYear write (mirrors loader guard);
    # keeps a queue reprocess from replaying old junk-year events.
    assert render_upsert_filing({"gmr_id": "a", "year": 2039, "source": "edgar"}) is None
    assert render_upsert_filing({"gmr_id": "a", "year": 1980, "source": "edgar"}) is None
    assert render_upsert_filing({"gmr_id": "a", "year": 2024, "source": "edgar"}) is not None

def test_contract_persists_integrity_fields():
    """Tender-integrity fields land on the Contract node. is_framework=False
    is meaningful (kept); tenders_received=1 is the single-bidder flag."""
    w = render_upsert_contract({
        "ted_notice_id": "n2",
        "procedure_type": "open",
        "tenders_received": 1,
        "award_criterion_type": "price",
        "submission_deadline": "2026-02-15",
        "is_framework": False,
        "eu_funded": True,
        "funding_programme": "RRF",
    })
    assert w.set_props["procedure_type"] == "open"
    assert w.set_props["tenders_received"] == 1
    assert w.set_props["award_criterion_type"] == "price"
    assert w.set_props["submission_deadline"] == "2026-02-15"
    assert w.set_props["is_framework"] is False
    assert w.set_props["eu_funded"] is True
    assert w.set_props["funding_programme"] == "RRF"


def test_contract_materialises_integrity_red_flags():
    """The shared keystone flags are materialised onto the Contract node:
    a single-bidder, no-call, price-only award trips all flags + count."""
    w = render_upsert_contract({
        "ted_notice_id": "n3",
        "tenders_received": 1,
        "procedure_type": "neg-wo-call",
        "award_criterion_type": "price",
    })
    assert w.set_props["is_single_bidder"] is True
    assert w.set_props["is_non_open"] is True
    assert w.set_props["is_no_call"] is True
    assert w.set_props["is_price_only"] is True
    assert w.set_props["integrity_red_flags"] == 4


def test_contract_no_red_flags_when_inputs_absent():
    """No integrity inputs → no flags materialised (unknown != flagged)."""
    w = render_upsert_contract({"ted_notice_id": "n4", "title": "x"})
    assert "is_single_bidder" not in w.set_props
    assert "integrity_red_flags" not in w.set_props


def test_taxonomy_code_carries_per_system_label():
    """TaxonomyCode gets a :TaxonomyCode label plus a per-system label so
    relationships/queries can MATCH by system (e.g. :Programme, :Fund, :Cpv).
    Without it a FINANCED_BY/UNDER_PROGRAMME edge to a :Programme never
    resolves."""
    w = render_upsert_taxonomy_code({
        "system": "programme", "code": "abc", "label": "Competitiveness PL",
    })
    assert w.label == "TaxonomyCode"
    assert w.extra_labels == ["Programme"]
    wf = render_upsert_taxonomy_code({"system": "fund", "code": "erdf",
                                      "label": "ERDF"})
    assert wf.extra_labels == ["Fund"]
    # hyphenated systems camel-case correctly
    wl = render_upsert_taxonomy_code({"system": "eu-cohesion", "code": "x"})
    assert wl.extra_labels == ["EuCohesion"]


def test_contract_stamps_procedure_and_modification_fields():
    """The incremental loader stamps procedure_id + notice_type, and
    modifies_publication_number on can-modif contracts. The sink must
    render them so the MODIFIES linking pass can join on procedure_id."""
    w = render_upsert_contract({
        "ted_notice_id": "uuid-mod",
        "procedure_id": "proc-7bcd",
        "notice_type": "can-modif",
        "modifies_publication_number": "708565-2022",
    })
    assert w.set_props["procedure_id"] == "proc-7bcd"
    assert w.set_props["notice_type"] == "can-modif"
    assert w.set_props["modifies_publication_number"] == "708565-2022"


def test_contract_renders_modification_before_values():
    """A modification's before-values pass through to set_props — the
    before->after delta consumers query to flag suspicious value changes."""
    w = render_upsert_contract({
        "ted_notice_id": "24082-2024",
        "notice_type": "can-modif",
        "value_eur": 2184.6,
        "value_original": 2184.6,
        "value_before_eur": 1092.3,
        "value_before_original": 1092.3,
    })
    assert w.set_props["value_before_eur"] == 1092.3
    assert w.set_props["value_before_original"] == 1092.3


def test_contract_omits_before_values_when_unset():
    w = render_upsert_contract({"ted_notice_id": "x", "value_eur": 5.0})
    assert "value_before_eur" not in w.set_props
    assert "value_before_original" not in w.set_props


# ── InvestmentFund entity (funds are not companies) ───────────────


def test_investment_fund_renderer_keys_by_gmr_id():
    w = render_upsert_investment_fund({
        "gmr_id": "0b6cbfa6-6a30-5efc-9b4f-3e56d0f3f5a2",
        "name": "EXAMPLE UCITS FUND",
        "lei": "2138008K5B3Z4E8DHN12",
        "fund_type": "Open-End Fund",
    })
    assert w.label == "InvestmentFund"
    assert w.primary_key == {"gmr_id": "0b6cbfa6-6a30-5efc-9b4f-3e56d0f3f5a2"}
    assert w.set_props["fund_type"] == "Open-End Fund"
    assert "country" not in w.set_props     # unset stays absent


def test_listing_carries_security_type():
    w = render_upsert_listing({
        "ticker": "EGL", "company_gmr_id": "g1",
        "exchange": "PL", "security_type": "Common Stock",
    })
    assert w.set_props["security_type"] == "Common Stock"


def test_match_label_disjunction_for_company_only():
    """Company-IRI targets must match relabeled funds too, or fund
    edges (LISTED_AS, SAME_AS, typed rels) silently vanish."""
    assert Neo4jSink._match_label("Company") == "Company|InvestmentFund"
    assert Neo4jSink._match_label("Listing") == "Listing"
    assert Neo4jSink._match_label("InvestmentFund") == "InvestmentFund"


def test_investment_fund_merge_relabels_in_place():
    """The fund MERGE must relabel an existing :Company node (keeping
    its edges) BEFORE merging, and never leave the node dual-labeled."""
    c = Neo4jSink._IFUND_MERGE_CYPHER
    assert "OPTIONAL MATCH (c:Company {gmr_id: row.gmr_id})" in c
    assert "SET x:InvestmentFund REMOVE x:Company" in c
    assert "MERGE (n:InvestmentFund {gmr_id: row.gmr_id})" in c
    # relabel must come before the merge
    assert c.index("REMOVE x:Company") < c.index("MERGE (n:InvestmentFund")


def test_company_merge_never_resplits_a_fund():
    """A later UpsertCompany for a relabeled fund must refresh props on
    the fund node and must NOT create a parallel :Company node."""
    refresh = Neo4jSink._COMPANY_REFRESH_FUND_CYPHER
    merge = Neo4jSink._COMPANY_MERGE_NON_FUND_CYPHER
    assert "MATCH (f:InvestmentFund {gmr_id: row.gmr_id})" in refresh
    assert "WHERE NOT EXISTS" in merge
    assert "InvestmentFund {gmr_id: row.gmr_id}" in merge
    assert "MERGE (n:Company {gmr_id: row.gmr_id})" in merge


# ── Value quarantine (contract monetary fields withheld) ──────────


def test_contract_quarantine_clears_monetary_props():
    w = render_upsert_contract({
        "ted_notice_id": "n-1",
        "title": "Bus depot",
        "value_eur": 1.8e14,             # must NOT survive
        "estimated_value_eur": 2.0e14,
        "value_quarantined": True,
        "value_quarantine_reason": "implausible_magnitude",
    })
    assert "value_eur" not in w.set_props
    assert "estimated_value_eur" not in w.set_props
    assert w.set_props["value_quarantined"] is True
    assert w.set_props["value_quarantine_reason"] == "implausible_magnitude"
    assert "value_eur" in w.clear_props
    assert "estimated_value_eur" in w.clear_props
    assert w.set_props["title"] == "Bus depot"    # non-monetary untouched


def test_contract_zero_value_quarantine_keeps_estimate():
    w = render_upsert_contract({
        "ted_notice_id": "n-2",
        "value_eur": 0.0,
        "estimated_value_eur": 5.0e6,    # a published 0 doesn't taint this
        "value_quarantined": True,
        "value_quarantine_reason": "zero_value",
    })
    assert "value_eur" not in w.set_props
    assert w.set_props["estimated_value_eur"] == 5.0e6
    assert w.clear_props == ["value_eur", "value_original", "value_currency"]


def test_contract_without_quarantine_has_no_clears():
    w = render_upsert_contract({"ted_notice_id": "n-3", "value_eur": 5.0})
    assert w.clear_props is None
    assert w.set_props["value_eur"] == 5.0


def test_healthy_reemit_clears_stale_quarantine_marker():
    """A re-scored healthy value (benign flag, not quarantined) must
    strip the value_quarantined / reason a prior quarantine or backfill
    event left on the node — SET += never removes them, so the contract
    would read as quarantined-with-a-value forever
    (values.quarantined_carries_no_value)."""
    w = render_upsert_contract({
        "ted_notice_id": "n-heal",
        "value_eur": 367977.2,
        "value_currency": "SEK",
        "value_quality_flag": "ok",
    })
    assert w.set_props["value_eur"] == 367977.2       # value kept
    assert w.clear_props == ["value_quarantined", "value_quarantine_reason"]


def test_no_awarded_value_clears_stale_award_value():
    """no_awarded_value withholds the awarded value; a prior emit's
    (often sign-flipped, negative) value must be removed, not left stale
    (values.contract_value_nonneg)."""
    w = render_upsert_contract({
        "ted_notice_id": "n-nav",
        "value_quality_flag": "no_awarded_value",
    })
    for f in ("value_eur", "value_original", "value_currency",
              "value_quarantined", "value_quarantine_reason"):
        assert f in w.clear_props


def test_hard_flag_without_quarantine_does_not_wipe_marker():
    """A hard-flagged value arriving un-quarantined (e.g. a scale
    re-score that dropped the marker) keeps its quarantine decidable by
    the loader/backfill — the sink must NOT clear the marker, or a
    hard-flagged value would render un-quarantined
    (values.hard_flags_are_quarantined)."""
    w = render_upsert_contract({
        "ted_notice_id": "n-hard",
        "value_eur": 5.0,
        "value_quality_flag": "concession_negative",
    })
    assert w.clear_props is None


def test_nonpositive_tenders_received_is_cleared():
    """tenders_received is a bidder COUNT (>= 1); a 0/negative is broken
    parsing and must be dropped, not stored
    (values.contract_bidder_count_positive)."""
    w = render_upsert_contract({
        "ted_notice_id": "n-bid",
        "value_eur": 5.0,
        "value_quality_flag": "ok",
        "tenders_received": 0,
    })
    assert "tenders_received" not in w.set_props
    assert "tenders_received" in w.clear_props


def test_positive_tenders_received_is_kept():
    w = render_upsert_contract({
        "ted_notice_id": "n-bid2",
        "value_eur": 5.0,
        "value_quality_flag": "ok",
        "tenders_received": 4,
    })
    assert w.set_props["tenders_received"] == 4
    assert "tenders_received" not in (w.clear_props or [])


def test_collapse_respects_set_then_clear_order():
    a = CypherWrite("Contract", {"ted_notice_id": "n"}, {"value_eur": 5.0})
    b = CypherWrite("Contract", {"ted_notice_id": "n"}, {},
                    clear_props=["value_eur"])
    merged = Neo4jSink._collapse_node_writes([a, b])
    assert merged[0].clear_props == ["value_eur"]
    assert "value_eur" not in merged[0].set_props
    # ...and the reverse: a later corrective SET revives the field
    merged2 = Neo4jSink._collapse_node_writes([b, a])
    assert not merged2[0].clear_props
    assert merged2[0].set_props["value_eur"] == 5.0


# ── GLEIF identity block + entity_kind-driven relabel ─────────────


def test_render_company_carries_gleif_identity_block():
    w = render_upsert_company({
        "gmr_id": "g1", "name": "CARLSBERG A/S",
        "entity_kind": "GENERAL", "registered_as": "61056416",
        "registered_at": "RA000170", "jurisdiction": "DK",
        "aliases": ["Carlsberg Group"], "hq_city": "København V",
    })
    for k in ("entity_kind", "registered_as", "registered_at",
              "jurisdiction", "hq_city"):
        assert w.set_props[k] is not None, k
    assert w.set_props["aliases"] == ["Carlsberg Group"]


def test_merge_company_partitions_by_entity_kind():
    """The Company merge routes rows by GLEIF entity_kind: FUND relabels
    to fund, non-FUND reverts to company, absent leaves the label."""
    sink = Neo4jSink.__new__(Neo4jSink)
    sess = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=sess)
    ctx.__exit__ = MagicMock(return_value=False)
    sink._driver = MagicMock()
    sink._driver.session = MagicMock(return_value=ctx)

    rows = [
        {"gmr_id": "f1", "props": {"name": "A Fund", "entity_kind": "FUND"}},
        {"gmr_id": "c1", "props": {"name": "Ostrum AM", "entity_kind": "GENERAL"}},
        {"gmr_id": "e1", "props": {"name": "Edgar Co"}},  # no kind stated
    ]
    sink._merge_company_rows(rows, extra_labels=[])
    cyphers = [c.args[0] for c in sess.run.call_args_list]
    fund_c = [c for c in cyphers if "SET x:InvestmentFund REMOVE x:Company" in c]
    revert_c = [c for c in cyphers if "SET x:Company REMOVE x:InvestmentFund" in c]
    assert len(fund_c) == 1 and len(revert_c) == 1
    # the FUND row went to the fund cypher, the GENERAL row to revert
    fund_rows = next(c.kwargs["rows"] for c in sess.run.call_args_list
                     if "REMOVE x:Company" in c.args[0])
    assert [r["gmr_id"] for r in fund_rows] == ["f1"]
    revert_rows = next(c.kwargs["rows"] for c in sess.run.call_args_list
                       if "REMOVE x:InvestmentFund" in c.args[0])
    assert [r["gmr_id"] for r in revert_rows] == ["c1"]


def test_merge_company_unknown_kind_does_not_relabel():
    """A row with no entity_kind (EDGAR/TED) must never move an existing
    label — it uses the refresh-both path."""
    sink = Neo4jSink.__new__(Neo4jSink)
    sess = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=sess)
    ctx.__exit__ = MagicMock(return_value=False)
    sink._driver = MagicMock()
    sink._driver.session = MagicMock(return_value=ctx)
    sink._merge_company_rows([{"gmr_id": "e1", "props": {"name": "X"}}], [])
    cyphers = " ".join(c.args[0] for c in sess.run.call_args_list)
    assert "REMOVE x:Company" not in cyphers
    assert "REMOVE x:InvestmentFund" not in cyphers


def test_petition_round_trip():
    w = render_upsert_petition({
        "system": "eu-eci", "petition_id": "ECI(2024)000007",
        "title": "Stop Destroying Videogames", "status": "ANSWERED",
        "total_supporters": 1294188,
        "organizer_names": ["Daniel ONDRUSKA"],
        "organizer_roles": ["REPRESENTATIVE"],
        "registration_decision_celex": "32024D1824",
        "answer_refs": ["C(2026)4110"],
    })
    assert w.label == "Petition"
    assert w.primary_key == {"system": "eu-eci", "petition_id": "ECI(2024)000007"}
    assert w.set_props["total_supporters"] == 1294188
    assert "collection_deadline" not in w.set_props


def test_contract_rollup_partial_sets_only_current_value_on_entity():
    """A collapse_modifications rollup-only UpsertContract restates only
    current_value, on the canonical :Contract entity keyed by
    contract_key. It must NOT recompute integrity red flags (which would
    reset them on the live node), NOT set is_current (a per-notice flag),
    and NOT smuggle contract_key onto a ted_notice_id-grain node."""
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS111-000001",
        "current_value": 42.0,
        "is_current": False,
        "contract_key": "proc:P-1",
    })
    assert w.primary_key == {"contract_key": "proc:P-1"}
    assert w.set_props == {"current_value": 42.0}
    # no red-flag / value / title / is_current fields smuggled in
    leaked = [k for k in w.set_props if k.startswith("is_")]
    assert not leaked, f"leaked into rollup: {leaked}"
    assert "value_eur" not in w.set_props
    assert "title" not in w.set_props
    assert "contract_key" not in w.set_props  # rides in primary_key


def test_contract_full_emit_splits_into_notice_and_contract():
    """A full contract emit that carries contract_key takes the native
    Contract/Notice split: one :Notice write (per-notice provenance)
    plus one :Contract entity write keyed by contract_key, whose
    value_eur is aliased to the collapsed current_value (the figure
    every legacy `ct.value_eur` aggregate must see exactly once)."""
    writes = render_upsert_contract({
        "ted_notice_id": "2025-OJS111-000002",
        "title": "Bridge works",
        "value_eur": 100.0,
        "is_current": True,
        "current_value": 90.0,
        "contract_key": "proc:P-2",
    })
    assert isinstance(writes, list) and len(writes) == 2
    notice, contract = writes
    assert notice.label == "Notice"
    assert notice.primary_key == {"ted_notice_id": "2025-OJS111-000002"}
    assert notice.set_props["value_eur"] == 100.0
    assert contract.label == "Contract"
    assert contract.primary_key == {"contract_key": "proc:P-2"}
    assert contract.set_props["current_value"] == 90.0
    assert contract.set_props["value_eur"] == 90.0
