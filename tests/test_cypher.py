from neo4j_sink.cypher import (
    RENDERERS, label_for_graph,
    render_assert_same_as, render_upsert_authority,
    render_upsert_company, render_upsert_contract,
    render_upsert_disclosure, render_upsert_exchange_rate,
    render_upsert_filing, render_upsert_listing,
    render_upsert_relationship, render_upsert_sanctioned_entity,
    render_upsert_taxonomy_code,
)


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


def test_contract_omits_relationships_when_keys_missing():
    """A bare contract event with no authority/company should not
    fabricate target IRIs — the sink would try to MATCH a missing
    node and fail."""
    w = render_upsert_contract({"ted_notice_id": "2025-OJS999-000000"})
    assert w.extra_relationships in (None, [])


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
        "UpsertCompany", "UpsertListing",
        "UpsertSanctionedEntity", "UpsertFiling",
        "UpsertAuthority", "UpsertContract",
        "UpsertTaxonomyCode", "UpsertRelationship",
        "UpsertDisclosure", "UpsertExchangeRate",
        "AssertSameAs",
    }
    assert set(RENDERERS) == expected
