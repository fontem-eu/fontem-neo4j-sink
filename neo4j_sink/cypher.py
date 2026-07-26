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

from fontem_event_schemas.integrity import contract_red_flags


@dataclass
class CypherWrite:  # pylint: disable=too-many-instance-attributes
    # A write is a value bag by design; the guard/always pair is
    # what lets the sink express the Contract entity's high-water
    # semantics without a second write type.
    """One MERGE/SET statement worth of parameters."""
    label: str                  # 'Company', 'SanctionedEntity', 'Filing', …
    primary_key: dict           # {'gmr_id': '…'} or {'entity_id': '…'}
    set_props: dict             # the rest of the entity body
    extra_relationships: "list[tuple[str, str, dict]] | None" = None  # rel_type, target_iri, props
    extra_labels: list[str] | None = None  # secondary labels, e.g. ['Lobbyist']
    # Properties to REMOVE from the node. Needed because SET n += props
    # never deletes: a quarantined contract value that was rendered
    # before the quarantine event must be explicitly cleared.
    clear_props: list[str] | None = None
    # High-water-mark guard: when set, `set_props` may only be applied
    # when row.props.<guard_prop> >= the node's current <guard_prop>
    # (string comparison; ISO dates order correctly). The sink renders
    # a FOREACH-CASE conditional SET instead of a blind `SET n +=`, so
    # replay from seq 0 and out-of-order delivery converge on the same
    # state (the latest notice wins). Guarded writes are never
    # dict-collapsed in a batch — each row applies sequentially with
    # the guard, which IS the sequential per-event semantics.
    guard_prop: str | None = None
    # Props applied unconditionally even on a guarded write (e.g. the
    # award_value stamped by an award notice must survive a later
    # modification having already raised the high-water mark).
    always_props: dict | None = None


def render_upsert_company(p: dict) -> CypherWrite:
    return CypherWrite(
        label="Company",
        primary_key={"gmr_id": p["gmr_id"]},
        set_props={
            k: p[k] for k in (
                "name", "country", "lei", "vat", "cik",
                "active", "legal_form", "postal_code",
                # GLEIF identity block (verbatim; entity_kind drives the
                # :Company/:InvestmentFund label in the sink merge).
                "entity_kind", "registered_as", "registered_at",
                "jurisdiction", "registration_status",
                "entity_creation_date", "address", "city", "region",
                "hq_address", "hq_city", "hq_region", "hq_country",
                "hq_postal_code", "aliases",
            ) if p.get(k) is not None
        },
    )


def render_upsert_investment_fund(p: dict) -> CypherWrite:
    """Pooled investment vehicle. Shares the Company gmr_id namespace
    (same UUID5 derivation) — the sink's merge layer relabels an
    existing :Company node in place rather than creating a sibling,
    so the entity keeps its identity and every edge."""
    return CypherWrite(
        label="InvestmentFund",
        primary_key={"gmr_id": p["gmr_id"]},
        set_props={
            k: p[k] for k in (
                "name", "country", "lei", "active",
                "legal_form", "fund_type",
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
        # person|entity; absent on pre-2026-07-14 events (all entities then)
        "subject_type": p.get("subject_type"),
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
            "security_type",
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


def render_translate_authority_name(p: dict) -> "CypherWrite | None":
    """Multilingual name enrichment for an existing :Authority node.

    The translations land as ``name_<lang>`` props (e.g. name_de,
    name_fr) alongside ``name_lang`` (the source language) and
    ``multilingual_updated_at``. The sink applies these via
    ``SET n += props`` — additive, so it neither clobbers the
    authority's other props nor is clobbered by a later
    UpsertAuthority (which only sets `name`, never `name_<lang>`).
    Mirrors the old direct-write ETL's
    ``SET a += row.props, a.name_lang=…, a.multilingual_updated_at=…``.
    """
    translations = p.get("translations") or {}
    if not translations:
        return None
    set_props = {f"name_{lang}": val for lang, val in translations.items() if val}
    if p.get("source_lang"):
        set_props["name_lang"] = p["source_lang"]
    if p.get("translated_at"):
        set_props["multilingual_updated_at"] = p["translated_at"]
    return CypherWrite(
        label="Authority",
        primary_key={"authority_id": p["authority_id"]},
        set_props=set_props,
    )


# Monetary props withheld when a contract value is quarantined. The
# reason decides the blast radius: a published 0 (zero_value) poisons
# only the awarded-value fields — the estimate may be real — while the
# hard-review reasons make every monetary signal suspect.
_QUARANTINE_CLEARS: dict[str, tuple[str, ...]] = {
    "zero_value": ("value_eur", "value_original", "value_currency"),
}
_QUARANTINE_CLEARS_DEFAULT: tuple[str, ...] = (
    "value_eur", "value_original", "value_currency",
    "estimated_value_eur", "value_payable_eur",
    "value_before_eur", "value_before_original",
)


# Keys a collapse_modifications rollup-only UpsertContract carries. When an
# incoming payload is a subset of these, it is a partial value-rollup update
# (not a full contract emit), so integrity red flags must NOT be recomputed.
_ROLLUP_ONLY_KEYS = {"ted_notice_id", "current_value", "is_current", "contract_key"}


def render_upsert_contract(p: dict) -> "CypherWrite | list[CypherWrite]":
    """Contract events render in one of three shapes:

    1. **Legacy (no contract_key)** — pre-Contract/Notice-model events.
       Notice-grain :Contract keyed by ted_notice_id, byte-identical to
       the historical renderer. The event log is append-only and
       replayable from seq 0, so this shape is frozen forever.
    2. **Rollup partial** (payload keys are a subset of
       _ROLLUP_ONLY_KEYS) — a collapse_modifications value-rollup.
       Although it now carries contract_key, its ted_notice_id is a
       real notice identity and every such event in the log was emitted
       against a notice-grain graph. It keeps the legacy render (and
       therefore never creates a :Notice node nor a Contract entity):
       on replay it lands on the legacy :Contract notice node exactly
       as it always did, and the pass is inert against a converted
       graph (no :Contract{notice_type:'can-modif'} rows remain).
    3. **Native Contract/Notice model** (contract_key present + real
       notice fields) — one :Notice node (per-notice provenance), one
       :Contract entity (display fields, high-water-guarded), a
       NOTICE_OF edge, and AWARDED / AWARDED_TO / BID_ON edges attached
       to the Contract entity. This is the shape project_contracts used
       to build in batch; the sink now writes it natively.

    A contract *modification* (can-modif) is per-notice provenance on an
    existing contract and must NEVER carry the :Contract label — a
    :Contract{notice_type:'can-modif'} makes value aggregates
    double-count the restatement (grain.no_contract_is_a_modification).
    The native path (3) already renders a modification as a :Notice plus
    a high-water-guarded update of the award-keyed :Contract entity (the
    entity itself never gets notice_type). The legacy notice-grain path
    (1/2), however, blindly labelled every event :Contract and copied
    notice_type verbatim, so any keyless can-modif emit — legacy events,
    but also value/scale-correction re-emits of an old payload
    (correct_scale_errors) and modification rollups — regressed a
    modification back onto the :Contract label. Those render as a :Notice
    instead.
    """
    if p.get("contract_key") is None or set(p).issubset(_ROLLUP_ONLY_KEYS):
        if _notice_kind(p) == "modification":
            return _render_notice_grain_modification(p)
        return _render_contract_notice_grain(p)
    return [_render_notice(p), _render_contract_entity(p)]


def _notice_kind(p: dict) -> str:
    """award|modification. Producer-stamped when present; otherwise
    derived exactly like project_contracts' relabel phase (eForms
    can-modif => modification)."""
    kind = p.get("notice_kind")
    if kind is not None:
        return kind
    return "modification" if p.get("notice_type") == "can-modif" else "award"


# Per-notice fields (the legacy Contract prop tuple minus the collapse
# rollup fields, which are per-contract in the new model but stay
# renderable on the Notice for rollup replays).
_NOTICE_FIELDS: tuple[str, ...] = (
    "title", "publication_date", "value_eur",
    "value_currency", "value_original",
    "value_before_eur", "value_before_original",
    "cpv", "nuts", "language", "country",
    "ted_publication_number",
    "estimated_value_eur", "value_payable_eur",
    "value_confidence", "value_confidence_consistency",
    "value_confidence_plausibility", "value_quality_flag",
    "value_low_confidence", "value_payable_discrepancy",
    "value_quarantined", "value_quarantine_reason",
    "procedure_type", "tenders_received",
    "award_criterion_type", "submission_deadline",
    "is_framework", "eu_funded", "funding_programme",
    "procedure_id", "notice_type", "modifies_publication_number",
)

# Fields NOT denormalised onto the Contract entity: notice identity
# and modification linkage stay per-notice (project_contracts nulls
# them on the entity for the same reason).
_NOTICE_ONLY_FIELDS = frozenset({
    "notice_type", "modifies_publication_number",
})

# Edge props carried per parties[] item onto AWARDED_TO / BID_ON.
_PARTY_EDGE_PROPS: tuple[str, ...] = (
    "rank", "is_consortium_member", "tendering_party_id",
    "match_tier", "match_confidence", "match_layer",
)


def _quarantine_clears(p: dict) -> "list[str] | None":
    if not p.get("value_quarantined"):
        return None
    reason = p.get("value_quarantine_reason") or ""
    return list(_QUARANTINE_CLEARS.get(reason, _QUARANTINE_CLEARS_DEFAULT))


def _render_notice(p: dict) -> CypherWrite:
    """The per-notice provenance node. Carries everything the notice
    published (incl. the recomputed integrity red flags — their inputs
    are per-notice) plus notice_kind + contract_key, and the NOTICE_OF
    edge to its Contract entity. The Contract-entity target IRI uses
    the ContractEntity segment: the plain Contract segment must keep
    resolving by ted_notice_id forever (link_ted_modifications MODIFIES
    events in the log reference notices that way)."""
    set_props = {k: p[k] for k in _NOTICE_FIELDS if p.get(k) is not None}
    set_props["notice_kind"] = _notice_kind(p)
    set_props["contract_key"] = p["contract_key"]
    set_props.update(contract_red_flags(p))
    clear = _quarantine_clears(p)
    if clear:
        for k in clear:
            set_props.pop(k, None)
    return CypherWrite(
        label="Notice",
        primary_key={"ted_notice_id": p["ted_notice_id"]},
        set_props=set_props,
        extra_relationships=[(
            "NOTICE_OF",
            f"http://data.fontem.eu/id/ContractEntity/{p['contract_key']}",
            {"_direction": "from_source"},
        )],
        clear_props=clear,
    )


def _contract_entity_edges(p: dict) -> "list[tuple[str, str, dict]]":
    """AWARDED / AWARDED_TO from the legacy top-level keys, plus the
    parties[] fan-out: winners get AWARDED_TO (Contract->Company) with
    the party props on the edge; named tenderers (losing bidders named
    on the notice) get BID_ON (Company->Contract). The primary winner
    usually appears both as company_gmr_id and as a parties[] winner —
    same rel type + endpoints, so the MERGEs coalesce onto one edge."""
    extras: list[tuple[str, str, dict]] = []
    if aid := p.get("authority_id"):
        extras.append((
            "AWARDED",
            f"http://data.fontem.eu/id/Authority/{aid}",
            {"_direction": "from_target"},
        ))
    if cid := p.get("company_gmr_id"):
        edge_props = {"_direction": "from_source"}
        for mk in ("match_tier", "match_confidence", "match_layer"):
            if p.get(mk) is not None:
                edge_props[mk] = p[mk]
        extras.append((
            "AWARDED_TO",
            f"http://data.fontem.eu/id/Company/{cid}",
            edge_props,
        ))
    extras.extend(_party_edge(party) for party in p.get("parties") or [])
    return extras


def _party_edge(party: dict) -> "tuple[str, str, dict]":
    """One parties[] item -> its edge triple. Winners point outward
    (Contract->Company AWARDED_TO); named tenderers point inward
    (Company->Contract BID_ON)."""
    is_winner = party.get("role") == "winner"
    props = {"_direction": "from_source" if is_winner else "from_target"}
    for k in _PARTY_EDGE_PROPS:
        if party.get(k) is not None:
            props[k] = party[k]
    return (
        "AWARDED_TO" if is_winner else "BID_ON",
        f"http://data.fontem.eu/id/Company/{party['company_gmr_id']}",
        props,
    )


def _render_contract_entity(p: dict) -> CypherWrite:
    """The one-per-real-contract display entity, keyed by contract_key.

    Display fields are guarded by the canonical_publication_date
    high-water mark: only the latest notice (by publication_date) may
    restate current_value / value_eur and the denormalised display
    fields, so a late-delivered older notice can never regress the
    entity, and replay from seq 0 converges to the same state.
    notice_count is NOT set here — it increments only when a NOTICE_OF
    edge is created (sink-side ON CREATE), never on replay.

    Quarantine clears are folded into the guarded props as explicit
    nulls (`SET n += map` removes null-valued keys), so a quarantine
    only strips the entity's monetary fields when the quarantined
    notice actually is the canonical one."""
    guarded = {
        k: p[k] for k in _NOTICE_FIELDS
        if k not in _NOTICE_ONLY_FIELDS and p.get(k) is not None
    }
    # Canonical-notice identity, denormalised for the read surfaces
    # (TED links etc.) exactly like project_contracts' finalize.
    guarded["ted_notice_id"] = p["ted_notice_id"]
    guarded.update(contract_red_flags(p))
    if pub := p.get("publication_date"):
        guarded["canonical_publication_date"] = pub
    clear = _quarantine_clears(p)
    value = None if clear else p.get("value_eur")
    if clear:
        for k in clear:
            guarded.pop(k, None)
        # Guarded nulls: SET += removes them, but only when this
        # notice wins the high-water comparison.
        guarded.update(dict.fromkeys(clear))
        if "value_eur" in clear:
            guarded["current_value"] = None
    elif value is not None or p.get("current_value") is not None:
        current = p.get("current_value", value)
        if current is None:
            current = value
        guarded["current_value"] = current
        guarded["value_eur"] = current
    always: dict = {"is_current": True}
    if _notice_kind(p) == "award" and value is not None:
        always["award_value"] = value
    return CypherWrite(
        label="Contract",
        primary_key={"contract_key": p["contract_key"]},
        set_props=guarded,
        extra_relationships=_contract_entity_edges(p) or None,
        guard_prop="canonical_publication_date",
        always_props=always,
    )


def _render_notice_grain_modification(p: dict) -> CypherWrite:
    """A can-modif event that arrived without a native contract_key
    (pre-model legacy events, collapse modification rollups, and
    value/scale-correction re-emits of an old payload).

    It is a modification *notice*, so it renders as a :Notice — never a
    :Contract — keyed by ted_notice_id. A keyed modification carrying real
    notice fields takes the native path instead (which builds :Notice +
    NOTICE_OF + the guarded entity update); by the time an event falls
    through to here it has no usable contract_key, so the modification
    lands as an unlinked :Notice. That is still correct per-notice
    provenance and, crucially, is not a :Contract. The aggregatable
    AWARDED / AWARDED_TO edges are deliberately NOT attached (they belong
    on the entity, not the notice — mirrors project_contracts' phase-4
    strip)."""
    set_props = {k: p[k] for k in _NOTICE_FIELDS if p.get(k) is not None}
    set_props["notice_kind"] = "modification"
    set_props.update(contract_red_flags(p))
    clear = _quarantine_clears(p)
    if clear:
        for k in clear:
            set_props.pop(k, None)
    return CypherWrite(
        label="Notice",
        primary_key={"ted_notice_id": p["ted_notice_id"]},
        set_props=set_props,
        clear_props=clear,
    )


def _render_contract_notice_grain(p: dict) -> CypherWrite:
    """Contract keyed by ted_notice_id. Two extra_relationships:
    Authority-[:AWARDED]->Contract (from_target), and
    Contract-[:AWARDED_TO]->Company (from_source). Either side
    may be missing; the relationship is only emitted when its key
    is populated."""
    set_props = {
        k: p[k] for k in (
            "title", "publication_date", "value_eur",
            "value_currency", "value_original",
            "value_before_eur", "value_before_original",
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
            "value_quarantined", "value_quarantine_reason",
            # Tender-integrity fields (eForms) — inputs to the SMSB
            # single-bidder / non-open indicators + the DIGIWHIST CRI
            # red flags.
            "procedure_type", "tenders_received",
            "award_criterion_type", "submission_deadline",
            "is_framework", "eu_funded", "funding_programme",
            # eForms procedure id + notice type, and (on modifications)
            # the original award's publication-number. procedure_id is the
            # join key the MODIFIES linking pass uses; notice_type marks
            # can-modif contracts.
            "procedure_id", "notice_type", "modifies_publication_number",
            # Modification-collapse rollup (collapse_modifications ETL pass):
            # current_value = latest restated value for the contract,
            # is_current marks the single canonical node per contract that
            # value aggregations sum over, contract_key is the identity it
            # grouped on. Emitted as partial (rollup-only) UpsertContract
            # updates; SET n += props leaves the rest of the node intact.
            "current_value", "is_current", "contract_key",
        ) if p.get(k) is not None
    }
    # Materialise the shared integrity red flags (single-bidder, non-open,
    # no-call, price-only + CRI-lite count) so they are hot-queryable in
    # the graph rather than recomputed per query. One source of truth
    # (fontem_event_schemas.integrity), shared with the Virtuoso sink + API.
    # A rollup-only partial (collapse_modifications) carries just the
    # canonical/current_value fields; recomputing red flags off it would
    # reset every integrity flag to its default. Only (re)derive flags from
    # a real contract emit that carries the integrity inputs.
    if not set(p).issubset(_ROLLUP_ONLY_KEYS):
        set_props.update(contract_red_flags(p))
    extras: list[tuple[str, str, dict]] = []
    if aid := p.get("authority_id"):
        extras.append((
            "AWARDED",
            f"http://data.fontem.eu/id/Authority/{aid}",
            {"_direction": "from_target"},
        ))
    if cid := p.get("company_gmr_id"):
        # Match provenance rides on the edge, not the Contract node: it
        # describes how THIS award was attributed to THIS company, so
        # exact (lei/vat/cik) and name-based (name_country/fuzzy) edges
        # are distinguishable in queries + the UI. Absent on legacy
        # events, so only set what is present.
        edge_props = {"_direction": "from_source"}
        for mk in ("match_tier", "match_confidence", "match_layer"):
            if p.get(mk) is not None:
                edge_props[mk] = p[mk]
        extras.append((
            "AWARDED_TO",
            f"http://data.fontem.eu/id/Company/{cid}",
            edge_props,
        ))
    clear: list[str] | None = None
    if p.get("value_quarantined"):
        reason = p.get("value_quarantine_reason") or ""
        clear = list(_QUARANTINE_CLEARS.get(reason,
                                            _QUARANTINE_CLEARS_DEFAULT))
        # A quarantine event must not smuggle the very values it
        # withholds back in through set_props.
        for k in clear:
            set_props.pop(k, None)
    return CypherWrite(
        label="Contract",
        primary_key={"ted_notice_id": p["ted_notice_id"]},
        set_props=set_props,
        extra_relationships=extras or None,
        clear_props=clear,
    )


def render_upsert_taxonomy_code(p: dict) -> CypherWrite:
    """TaxonomyCode lands as a generic :TaxonomyCode label keyed by
    (system, code). The sink also adds a per-system label via
    SET n:<System> for label-specific queries (e.g. :CPV {code: ...})."""
    label = "TaxonomyCode"
    # Per-system secondary label (e.g. :Cpv, :Nuts, :Programme, :Fund) so
    # relationships and queries can MATCH a node by its system's label, which
    # _KEY_FIELD_BY_LABEL keys by `code`. Without this a relationship targeting
    # http://data.fontem.eu/id/Programme/<code> resolves to a :Programme that
    # never exists.
    sys_camel = p["system"].replace("-", "_").title().replace("_", "")
    set_props = {
        k: p[k] for k in (
            "label", "label_lang", "level", "description",
        ) if p.get(k) is not None
    }
    extras: list[tuple[str, str, dict]] = []
    if parent := p.get("parent_code"):
        # CHILD_OF edge to the parent code in the same system.
        # The sink resolves the parent via system+parent_code.
        parent_iri = f"http://data.fontem.eu/id/{sys_camel}/{parent}"
        extras.append((
            "CHILD_OF", parent_iri, {"_direction": "from_source"},
        ))
    return CypherWrite(
        label=label,
        primary_key={"system": p["system"], "code": p["code"]},
        set_props=set_props,
        extra_relationships=extras or None,
        extra_labels=[sys_camel],
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
    "eu-cohesion": ["CohesionProject"],
}


def _flatten_disclosure_details(details: dict | None) -> dict:
    """Flatten the details bag into detail_<key> props. Nested dicts and
    non-scalar lists are dropped; lists of scalars are kept as Neo4j array
    properties. `programme_code` is a :Programme node reference (linked by
    the caller), not a stored detail."""
    props: dict = {}
    for k, v in (details or {}).items():
        if k == "programme_code" or v is None or isinstance(v, dict):
            continue
        if isinstance(v, list):
            if v and all(isinstance(x, (str, int, float, bool)) for x in v):
                props[f"detail_{k}"] = v
            continue
        props[f"detail_{k}"] = v
    return props


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
    set_props.update(_flatten_disclosure_details(p.get("details")))
    extras: list[tuple[str, str, dict]] = []
    if cid := p.get("company_gmr_id"):
        extras.append((
            "FILED_BY",
            f"http://data.fontem.eu/id/Company/{cid}",
            {"_direction": "from_source"},
        ))
    # Cohesion projects link to their funding :Programme (itself
    # FINANCED_BY a :Fund). The loader passes the stable programme code
    # in details; the Programme/Fund taxonomy nodes are emitted ahead of
    # the disclosure so the MATCH resolves.
    if prog_code := (p.get("details") or {}).get("programme_code"):
        extras.append((
            "UNDER_PROGRAMME",
            f"http://data.fontem.eu/id/Programme/{prog_code}",
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


def render_upsert_petition(p: dict) -> CypherWrite:
    """Public petition keyed by (system, petition_id) — e.g. the EU
    Citizens' Initiative register. Organizer names arrive as parallel
    arrays (names/roles/countries); emails never reach the platform."""
    set_props = {
        k: p[k] for k in (
            "title", "status", "objectives", "registration_date",
            "collection_start_date", "collection_deadline", "closed_date",
            "submitted_date", "answered_date", "total_supporters",
            "support_link", "organizer_names", "organizer_roles",
            "organizer_countries", "funding_total_eur",
            "funding_sponsor_count", "registration_decision_celex",
            "answer_refs", "latest_update",
        ) if p.get(k) is not None
    }
    return CypherWrite(
        label="Petition",
        primary_key={"system": p["system"], "petition_id": p["petition_id"]},
        set_props=set_props,
    )


RENDERERS: dict[str, Callable[[dict], CypherWrite] | None] = {
    "BeginGraphReplace": None,
    "EndGraphReplace": None,
    "UpsertCompany": render_upsert_company,
    "UpsertInvestmentFund": render_upsert_investment_fund,
    "UpsertListing": render_upsert_listing,
    "UpsertPetition": render_upsert_petition,
    "UpsertSanctionedEntity": render_upsert_sanctioned_entity,
    "UpsertFiling": render_upsert_filing,
    "UpsertAuthority": render_upsert_authority,
    "TranslateAuthorityName": render_translate_authority_name,
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
