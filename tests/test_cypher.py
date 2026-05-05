from neo4j_sink.cypher import (
    RENDERERS, label_for_graph,
    render_assert_same_as, render_upsert_company,
    render_upsert_filing, render_upsert_sanctioned_entity,
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


def test_renderer_registry_covers_all_event_types():
    expected = {
        "BeginGraphReplace", "EndGraphReplace",
        "UpsertCompany", "UpsertSanctionedEntity", "UpsertFiling",
        "AssertSameAs",
    }
    assert set(RENDERERS) == expected
