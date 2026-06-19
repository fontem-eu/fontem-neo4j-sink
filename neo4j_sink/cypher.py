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

import datetime

from dataclasses import dataclass
from typing import Callable


@dataclass
class CypherWrite:
    """One MERGE/SET statement worth of parameters."""
    label: str                  # 'Company', 'SanctionedEntity', 'Filing', …
    primary_key: dict           # {'gmr_id': '…'} or {'entity_id': '…'}
    set_props: dict             # the rest of the entity body
    extra_relationships: list[tuple[str, str, dict]] = None  # rel_type, target_iri, props
    extra_labels: list[str] | None = None  # secondary labels, e.g. ['Lobbyist']


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


def render_upsert_filing(p: dict) -> "CypherWrite | None":
    """Filing is keyed by (gmr_id, year) per the Neo4j-era schema.
    Source is on the node, not in the key — Filing-EDGAR and
    Filing-ESEF for the same company-year overwrite each other
    here (matches today's behaviour; the de-collision is in
    Virtuoso where the IRI carries source).
    """
    year = int(p["year"])
    # Refuse an implausible fiscal year (botched XBRL period-end like 2039).
    # Mirrors the loader guard so even a queue reprocess of old events can't
    # replay junk years into the graph.
    if not 1990 <= year <= datetime.date.today().year + 1:
        return None
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
            "cpv", "nuts", "language", "country",
            # TED publication-number (e.g. "295342-2026") captured by
            # the ETL via TED's v3 search API. Lets readers build the
            # canonical detail URL without a runtime UUID→pub-num
            # lookup. Null when the notice was queued but not yet
            # published at ETL time — readers fall back to the
            # /api/contracts/<id>/ted-link redirector.
            "ted_publication_number",
            # Value-quality signals from the ETL's confidence scorer.
            # value_low_confidence is the gate DQ / coverage queries use
            # to exclude a contract from default value aggregates while
            # keeping the node; the rest explain and support review.
            "estimated_value_eur", "value_payable_eur",
            "value_confidence", "value_confidence_consistency",
            "value_confidence_plausibility", "value_quality_flag",
            "value_low_confidence", "value_payable_discrepancy",
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


def render_upsert_taxonomy_code(p: dict) -> CypherWrite:
    """TaxonomyCode lands as a generic :TaxonomyCode label keyed by
    (system, code). The sink also adds a per-system label via
    SET n:<System> for label-specific queries (e.g. :CPV {code: ...})."""
    label = "TaxonomyCode"
    set_props = {
        k: p[k] for k in (
            "label", "label_lang", "level", "description",
        ) if p.get(k) is not None
    }
    extras: list[tuple[str, str, dict]] = []
    if parent := p.get("parent_code"):
        # CHILD_OF edge to the parent code in the same system.
        # The sink resolves the parent via system+parent_code.
        sys_camel = p["system"].replace("-", "_").title().replace("_", "")
        parent_iri = f"http://data.fontem.eu/id/{sys_camel}/{parent}"
        extras.append((
            "CHILD_OF", parent_iri, {"_direction": "from_source"},
        ))
    return CypherWrite(
        label=label,
        primary_key={"system": p["system"], "code": p["code"]},
        set_props=set_props,
        extra_relationships=extras or None,
    )


def render_upsert_relationship(p: dict) -> CypherWrite:
    """A typed edge between two existing entity nodes. The sink
    resolves both IRIs to (label, key) and MERGEs the relationship.

    We model this as a virtual ``_Relationship`` write so the sink
    layer (which knows how to parse fontem-id IRIs) can do the
    MATCH/MERGE in one place instead of leaking that into every
    renderer."""
    set_props = {
        k: p[k] for k in ("valid_from", "valid_to") if p.get(k) is not None
    }
    if extra := p.get("properties"):
        set_props.update(extra)
    return CypherWrite(
        label="_Relationship",
        primary_key={
            "src_iri": p["src_iri"],
            "dst_iri": p["dst_iri"],
            "predicate": p["predicate"],
        },
        set_props=set_props,
    )


# Per-system secondary labels promised in render_upsert_disclosure's
# docstring (so dashboards can query :Lobbyist instead of the generic
# :Disclosure). Applied by the sink's MERGE after the node is keyed.
_SYSTEM_LABELS: dict[str, list[str]] = {
    "eu-lobbying": ["Lobbyist"],
}


def render_upsert_disclosure(p: dict) -> CypherWrite:
    """Disclosure as a generic :Disclosure label keyed by
    (system, disclosure_id). Per-system label is added by the sink
    (e.g. :Lobbyist for system='eu-lobbying').

    The `details` bag is flattened: any scalar value lands as a
    detail_<key> property. Lists and nested dicts are dropped — if
    a producer needs them they need their own schema event.

    company_gmr_id is optional: when absent (EU lobbying register
    where the registrant is the Lobbyist itself), the FILED_BY edge
    is skipped and the registrant identity rides in details.
    """
    set_props = {
        k: p[k] for k in (
            "company_gmr_id", "disclosure_type", "filed_date",
            "year", "title", "url",
        ) if p.get(k) is not None
    }
    for k, v in (p.get("details") or {}).items():
        if v is None or isinstance(v, dict):
            continue
        if isinstance(v, list):
            # Keep list-of-scalar fields (e.g. lobbying `interests`) as a
            # Neo4j array property so `WHERE 'climate' IN d.detail_interests`
            # works; only nested / non-scalar lists are dropped.
            if v and all(isinstance(x, (str, int, float, bool)) for x in v):
                set_props[f"detail_{k}"] = v
            continue
        set_props[f"detail_{k}"] = v
    extras: list[tuple[str, str, dict]] = []
    if cid := p.get("company_gmr_id"):
        extras.append((
            "FILED_BY",
            f"http://data.fontem.eu/id/Company/{cid}",
            {"_direction": "from_source"},
        ))
    return CypherWrite(
        label="Disclosure",
        primary_key={
            "system": p["system"],
            "disclosure_id": p["disclosure_id"],
        },
        set_props=set_props,
        extra_relationships=extras or None,
        extra_labels=_SYSTEM_LABELS.get(p["system"]),
    )


def render_upsert_exchange_rate(p: dict) -> CypherWrite:
    """ExchangeRate keyed by (base, target, date). Three-part
    composite primary key — the sink layer handles MERGE on it."""
    set_props = {"rate": float(p["rate"])}
    if src := p.get("source"):
        set_props["source"] = src
    return CypherWrite(
        label="ExchangeRate",
        primary_key={
            "base": p["base"],
            "target": p["target"],
            "date": p["date"],
        },
        set_props=set_props,
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
    "UpsertTaxonomyCode": render_upsert_taxonomy_code,
    "UpsertRelationship": render_upsert_relationship,
    "UpsertDisclosure": render_upsert_disclosure,
    "UpsertExchangeRate": render_upsert_exchange_rate,
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
