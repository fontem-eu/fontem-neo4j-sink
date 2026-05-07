"""Event payload → Cypher MERGE statements.

The Neo4j sink mirrors the Virtuoso sink's renderer registry,
but emits Cypher fragments instead of triples. Each renderer
returns ``(cypher, params)`` — the sink batches multiple events
in one transaction with a list of params.

For the bracket case we batch the same way: accumulate
``(cypher, params)`` between Begin/End and execute as one
``UNWIND $rows AS row`` per node label, plus a `DETACH DELETE`
prelude that wipes anything currently carrying the label.

Per-entity (no bracket) updates fire one MERGE statement each.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass
class CypherWrite:
    """One MERGE/SET statement worth of parameters."""
    label: str                  # 'Company', 'SanctionedEntity', 'Filing', …
    primary_key: dict           # {'gmr_id': '…'} or {'entity_id': '…'}
    set_props: dict             # the rest of the entity body
    extra_relationships: list[tuple[str, str, dict]] = None  # rel_type, target_iri, props


def render_upsert_company(p: dict) -> CypherWrite:
    return CypherWrite(
        label="Company",
        primary_key={"gmr_id": p["gmr_id"]},
        set_props={
            k: p[k] for k in (
                "name", "country", "lei", "vat", "cik",
                "active", "legal_form", "postal_code",
            ) if p.get(k) is not None
        },
    )


def render_upsert_sanctioned_entity(p: dict) -> CypherWrite:
    set_props = {
        "eu_reference": p["eu_reference"],
        "name": p.get("name"),
        "aliases": list(p.get("aliases") or ()),
        "nationality": p.get("nationality"),
        "designation_date": p.get("designation_date"),
        "sanction_regime": p.get("sanction_regime"),
        "legal_basis": p.get("legal_basis"),
        "listing_reason": p.get("listing_reason"),
    }
    return CypherWrite(
        label="SanctionedEntity",
        primary_key={"entity_id": p["entity_id"]},
        set_props={k: v for k, v in set_props.items() if v is not None},
    )


def render_upsert_filing(p: dict) -> CypherWrite:
    """Filing is keyed by (gmr_id, year) per the Neo4j-era schema.
    Source is on the node, not in the key — Filing-EDGAR and
    Filing-ESEF for the same company-year overwrite each other
    here (matches today's behaviour; the de-collision is in
    Virtuoso where the IRI carries source).
    """
    set_props = {
        "source": p["source"],
        **{k: p[k] for k in (
            "filing_date", "revenue", "gross_profit",
            "operating_income", "net_income", "eps",
            "total_assets", "total_liabilities", "equity",
            "cash_and_equivalents", "cash", "capex",
            "operating_cashflow", "free_cashflow",
            "current_assets", "current_liabilities",
            "shares_outstanding", "long_term_debt",
            "interest_expense", "income_tax_expense",
            "depreciation_amortization", "inventory",
        ) if p.get(k) is not None},
    }
    return CypherWrite(
        label="FinancialYear",
        primary_key={"gmr_id": p["gmr_id"], "year": int(p["year"])},
        set_props=set_props,
        extra_relationships=[
            (
                "REPORTED",
                f"http://data.fontem.eu/id/Company/{p['gmr_id']}",
                {"year": int(p["year"]), "_direction": "from_target"},
            ),
        ],
    )


def render_upsert_listing(p: dict) -> CypherWrite:
    """Listing keyed by ticker (mirrors the Neo4j-era schema). The
    Company → Listing LISTED_AS edge is materialised by the sink
    via extra_relationships; downstream price + financial fetchers
    join via the Listing node, not through a property fan-out on
    Company."""
    set_props = {
        k: p[k] for k in (
            "exchange", "currency", "active", "isin", "mic",
        ) if p.get(k) is not None
    }
    return CypherWrite(
        label="Listing",
        primary_key={"ticker": p["ticker"]},
        set_props=set_props,
        extra_relationships=[
            (
                "LISTED_AS",
                f"http://data.fontem.eu/id/Company/{p['company_gmr_id']}",
                {"_direction": "from_target"},
            ),
        ],
    )


def render_upsert_authority(p: dict) -> CypherWrite:
    set_props = {
        k: p[k] for k in (
            "name", "country", "authority_type", "national_id",
            "url", "postal_code", "city", "nuts",
        ) if p.get(k) is not None
    }
    return CypherWrite(
        label="Authority",
        primary_key={"authority_id": p["authority_id"]},
        set_props=set_props,
    )


def render_upsert_contract(p: dict) -> CypherWrite:
    """Contract keyed by ted_notice_id. Two extra_relationships:
    Authority-[:AWARDED]->Contract (from_target), and
    Contract-[:AWARDED_TO]->Company (from_source). Either side
    may be missing; the relationship is only emitted when its key
    is populated."""
    set_props = {
        k: p[k] for k in (
            "title", "publication_date", "value_eur",
            "value_currency", "value_original",
            "cpv", "nuts", "language",
        ) if p.get(k) is not None
    }
    extras: list[tuple[str, str, dict]] = []
    if aid := p.get("authority_id"):
        extras.append((
            "AWARDED",
            f"http://data.fontem.eu/id/Authority/{aid}",
            {"_direction": "from_target"},
        ))
    if cid := p.get("company_gmr_id"):
        extras.append((
            "AWARDED_TO",
            f"http://data.fontem.eu/id/Company/{cid}",
            {"_direction": "from_source"},
        ))
    return CypherWrite(
        label="Contract",
        primary_key={"ted_notice_id": p["ted_notice_id"]},
        set_props=set_props,
        extra_relationships=extras or None,
    )


def render_assert_same_as(p: dict) -> CypherWrite:
    """Emitted as a SAME_AS edge between the two IRIs' Neo4j
    nodes. The sink resolves IRI → (label, key) by parsing the
    IRI; deferred until the sink layer because it's coupled to
    the Virtuoso IRI scheme.
    """
    return CypherWrite(
        label="_SameAs",  # virtual; the sink handles this specially
        primary_key={
            "a_iri": p["a_iri"],
            "b_iri": p["b_iri"],
        },
        set_props={
            "confidence": p["confidence"],
            "method": p["method"],
            "tier": p.get("tier"),
            "matched_via_alias": p.get("matched_via_alias", False),
            "rule": p.get("rule"),
        },
    )


RENDERERS: dict[str, Callable[[dict], CypherWrite] | None] = {
    "BeginGraphReplace": None,
    "EndGraphReplace": None,
    "UpsertCompany": render_upsert_company,
    "UpsertListing": render_upsert_listing,
    "UpsertSanctionedEntity": render_upsert_sanctioned_entity,
    "UpsertFiling": render_upsert_filing,
    "UpsertAuthority": render_upsert_authority,
    "UpsertContract": render_upsert_contract,
    "AssertSameAs": render_assert_same_as,
}


# Map graph IRI suffix → Neo4j label for the bulk-delete that
# starts a Begin/End bracket. The convention mirrors the Virtuoso
# named-graph layout.
GRAPH_TO_LABEL: dict[str, str] = {
    "company":   "Company",
    "listing":   "Listing",
    "contract":  "Contract",
    "authority": "Authority",
    "cpv":       "CPV",
    "nuts":      "NUTSRegion",
    "lobbyist":  "Lobbyist",
    "cohesion":  "CohesionProject",
    "sanctions": "SanctionedEntity",
    "financials/edgar": "FinancialYear",
    "financials/esef":  "FinancialYear",
}


def label_for_graph(graph_iri: str) -> str | None:
    """Recognise the Neo4j label that a graph_iri's bracket
    should `DETACH DELETE` before flushing the new batch."""
    for suffix, label in GRAPH_TO_LABEL.items():
        if graph_iri.endswith(f"/graph/{suffix}"):
            return label
    return None
