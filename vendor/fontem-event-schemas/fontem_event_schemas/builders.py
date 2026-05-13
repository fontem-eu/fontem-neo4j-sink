"""Typed builders for common event payloads.

Producers should prefer these over hand-rolling dicts: they keep
the field set in lockstep with the schema and let mypy / IDE flag
typos at edit time. Each returns a dict conforming to the
corresponding JSON Schema.
"""
from __future__ import annotations

from typing import Any


def upsert_sanctioned_entity(
    *,
    entity_id: str,
    eu_reference: str,
    name: str | None = None,
    aliases: list[str] | None = None,
    nationality: str | None = None,
    designation_date: str | None = None,
    sanction_regime: str | None = None,
    legal_basis: str | None = None,
    listing_reason: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertSanctionedEntity payload (v1)."""
    out: dict[str, Any] = {
        "entity_id":    entity_id,
        "eu_reference": eu_reference,
    }
    for k, v in (
        ("name", name), ("aliases", aliases),
        ("nationality", nationality),
        ("designation_date", designation_date),
        ("sanction_regime", sanction_regime),
        ("legal_basis", legal_basis),
        ("listing_reason", listing_reason),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_filing(
    *,
    gmr_id: str,
    year: int,
    source: str,
    filing_date: str | None = None,
    **financials: float | None,
) -> dict[str, Any]:
    """Build an UpsertFiling payload (v1).

    ``financials`` accepts any of the optional numeric Filing
    fields (revenue, net_income, …); None/missing values are
    dropped so the JSON stays compact and schema-clean.
    """
    out: dict[str, Any] = {
        "gmr_id": gmr_id, "year": year, "source": source,
    }
    if filing_date:
        out["filing_date"] = filing_date
    for k, v in financials.items():
        if v is not None:
            out[k] = float(v)
    return out


def upsert_company(
    *,
    gmr_id: str,
    name: str | None = None,
    country: str | None = None,
    lei: str | None = None,
    vat: str | None = None,
    cik: str | None = None,
    active: bool | None = None,
    legal_form: str | None = None,
    postal_code: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertCompany payload (v1)."""
    out: dict[str, Any] = {"gmr_id": gmr_id}
    for k, v in (
        ("name", name), ("country", country), ("lei", lei),
        ("vat", vat), ("cik", cik), ("active", active),
        ("legal_form", legal_form), ("postal_code", postal_code),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_listing(
    *,
    ticker: str,
    company_gmr_id: str,
    exchange: str | None = None,
    currency: str | None = None,
    active: bool | None = None,
    isin: str | None = None,
    mic: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertListing payload (v1)."""
    out: dict[str, Any] = {
        "ticker": ticker, "company_gmr_id": company_gmr_id,
    }
    for k, v in (
        ("exchange", exchange), ("currency", currency),
        ("active", active), ("isin", isin), ("mic", mic),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_authority(
    *,
    authority_id: str,
    name: str | None = None,
    country: str | None = None,
    authority_type: str | None = None,
    national_id: str | None = None,
    url: str | None = None,
    postal_code: str | None = None,
    city: str | None = None,
    nuts: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertAuthority payload (v1)."""
    out: dict[str, Any] = {"authority_id": authority_id}
    for k, v in (
        ("name", name), ("country", country),
        ("authority_type", authority_type),
        ("national_id", national_id), ("url", url),
        ("postal_code", postal_code), ("city", city), ("nuts", nuts),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_contract(
    *,
    ted_notice_id: str,
    title: str | None = None,
    authority_id: str | None = None,
    company_gmr_id: str | None = None,
    publication_date: str | None = None,
    value_eur: float | None = None,
    value_currency: str | None = None,
    value_original: float | None = None,
    cpv: str | None = None,
    nuts: str | None = None,
    language: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertContract payload (v1)."""
    out: dict[str, Any] = {"ted_notice_id": ted_notice_id}
    for k, v in (
        ("title", title), ("authority_id", authority_id),
        ("company_gmr_id", company_gmr_id),
        ("publication_date", publication_date),
        ("value_eur", value_eur), ("value_currency", value_currency),
        ("value_original", value_original),
        ("cpv", cpv), ("nuts", nuts), ("language", language),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_taxonomy_code(
    *,
    system: str,
    code: str,
    label: str | None = None,
    label_lang: str | None = None,
    parent_code: str | None = None,
    level: int | None = None,
    description: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertTaxonomyCode payload (v1)."""
    out: dict[str, Any] = {"system": system, "code": code}
    for k, v in (
        ("label", label), ("label_lang", label_lang),
        ("parent_code", parent_code),
        ("level", level), ("description", description),
    ):
        if v is not None and v != "":
            out[k] = v
    return out


def upsert_relationship(
    *,
    src_iri: str,
    dst_iri: str,
    predicate: str,
    properties: dict[str, Any] | None = None,
    valid_from: str | None = None,
    valid_to: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertRelationship payload (v1)."""
    out: dict[str, Any] = {
        "src_iri": src_iri, "dst_iri": dst_iri, "predicate": predicate,
    }
    if properties:
        out["properties"] = properties
    if valid_from:
        out["valid_from"] = valid_from
    if valid_to:
        out["valid_to"] = valid_to
    return out


def upsert_disclosure(
    *,
    system: str,
    disclosure_id: str,
    company_gmr_id: str | None = None,
    disclosure_type: str | None = None,
    filed_date: str | None = None,
    year: int | None = None,
    title: str | None = None,
    url: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build an UpsertDisclosure payload (v1).

    ``company_gmr_id`` is optional — some regimes (EU lobbying)
    file under non-Company registrants, in which case the
    registrant identity rides in ``details``.
    """
    out: dict[str, Any] = {
        "system": system,
        "disclosure_id": disclosure_id,
    }
    if company_gmr_id:
        out["company_gmr_id"] = company_gmr_id
    for k, v in (
        ("disclosure_type", disclosure_type),
        ("filed_date", filed_date), ("year", year),
        ("title", title), ("url", url),
    ):
        if v is not None and v != "":
            out[k] = v
    if details:
        out["details"] = details
    return out


def upsert_exchange_rate(
    *,
    base: str,
    target: str,
    date: str,
    rate: float,
    source: str | None = None,
) -> dict[str, Any]:
    """Build an UpsertExchangeRate payload (v1)."""
    out: dict[str, Any] = {
        "base": base, "target": target,
        "date": date, "rate": float(rate),
    }
    if source:
        out["source"] = source
    return out


def assert_same_as(
    *,
    a_iri: str,
    b_iri: str,
    confidence: float,
    method: str,
    tier: str | None = None,
    matched_via_alias: bool = False,
    rule: str | None = None,
) -> dict[str, Any]:
    """Build an AssertSameAs payload (v1)."""
    out: dict[str, Any] = {
        "a_iri": a_iri, "b_iri": b_iri,
        "confidence": float(confidence), "method": method,
        "matched_via_alias": bool(matched_via_alias),
    }
    if tier is not None:
        out["tier"] = tier
    if rule is not None:
        out["rule"] = rule
    return out


def begin_graph_replace(
    *, graph_iri: str, label: str, domain: str | None = None,
) -> dict[str, Any]:
    """Build a BeginGraphReplace control payload (v1)."""
    out: dict[str, Any] = {"graph_iri": graph_iri, "label": label}
    if domain:
        out["domain"] = domain
    return out


def end_graph_replace(
    *, graph_iri: str, domain: str | None = None,
) -> dict[str, Any]:
    """Build an EndGraphReplace control payload (v1)."""
    out: dict[str, Any] = {"graph_iri": graph_iri}
    if domain:
        out["domain"] = domain
    return out
