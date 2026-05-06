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
