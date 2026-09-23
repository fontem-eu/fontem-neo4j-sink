"""What the ingest cleaning stage adds to the graph beyond scalars.

gitops data-backlog Part 5 lands the cleaner (C2/C4) and the framework
agreement node (C6) in one reprocess campaign (C8). Most of the scalar
fields they add ride cypher.py's per-notice whitelist; this module
holds what cannot be read in isolation, split out of cypher.py the way
identity.py was — the pair that has to stay consistent lives together:
the withheld-supplier lists (C2), and the :FrameworkAgreement node, the
CALL_OFF_OF edge that must resolve it by the same key, and the
framework scalars whose meaning that key decides (C6).
"""
from __future__ import annotations

from neo4j_sink.writes import CypherWrite

FRAMEWORK_AGREEMENT_IRI = "http://data.fontem.eu/id/FrameworkAgreement/"


def withheld_suppliers(p: dict) -> dict:
    """suppliers_withheld[] -> parallel list props.

    The cleaning stage refused to mint a company for these (the name
    field held a sentence, a placeholder, other non-name text): they are
    NOT in parties and no company exists for them, so the raw text is
    the only trace of who the notice named. Neo4j stores no list of
    maps, so the items flatten into parallel homogeneous lists in
    payload order — suppliers_withheld_names[i] was withheld for
    suppliers_withheld_reasons[i] — plus the count. An item without
    name_raw has nothing to keep and is dropped from both lists; a
    missing reason becomes '' so the lists stay aligned.

    Returns {} when the payload carries no suppliers_withheld at all:
    an emit that says nothing about withheld suppliers must not clear
    what an earlier one wrote (SET += props leaves absent keys alone,
    and on the entity the high-water guard decides). An explicit empty
    list is a statement and is written as such (count 0)."""
    items = p.get("suppliers_withheld")
    if items is None:
        return {}
    kept = [x for x in items if x.get("name_raw")]
    return {
        "suppliers_withheld_names": [x["name_raw"] for x in kept],
        "suppliers_withheld_reasons": [x.get("reason") or "" for x in kept],
        "suppliers_withheld_count": len(kept),
    }


# Framework fields of a plain UpsertContract, spliced into cypher.py's
# per-notice whitelist so they land on the :Notice and, because none of
# them is notice-only, on the :Contract entity the UI reads.
#
# framework_id is eForms OPT-100
# (efac:NoticeResult/efac:SettledContract/cac:NoticeDocumentReference/
# cbc:ID). The framework-establishing award notice and every call-off
# under it carry the IDENTICAL value — notices 761784-2024 and
# 3406-2025 both carry '536632-2024' — which is what makes it a
# GROUPING key and not a pointer: ~80% of the time it names a call for
# competition, which this platform does not ingest (prod holds 0
# contract notices), so it resolves to a :Contract we hold only ~13.6%
# of the time. Readers group on it (via the contract_framework_id
# index) and may say no more than "part of a framework agreement":
# nothing in the data separates an establishing award from a call-off,
# so the cluster has no order, and a contract carrying no framework_id
# is not a contract with no framework — pre-2024 notices have no such
# field at all.
#
# framework_id_source records which element the value was read off
# (OPT-100, or the BT-125 fallback, which the parser normalises: TED's
# own index matches '536632-2024' and returns nothing for the
# zero-padded '00536632-2024' the same notice carries at BT-125). It is
# deliberately not notice-only — the entity's framework_id is written
# under the canonical_publication_date high-water guard, so its
# provenance has to be written by the same notice under the same guard,
# or the entity ends up showing one notice's id with another's source.
#
# The framework terms are the agreement's CEILING and its companions:
# capacity, never money paid, so nothing may sum them into spend.
CONTRACT_FRAMEWORK_FIELDS: tuple[str, ...] = (
    "framework_max_value_eur", "framework_reestimated_value_eur",
    "framework_duration_months", "framework_max_operators",
    "framework_id", "framework_id_source",
)


def call_off_edge(p: dict) -> "tuple[str, str, dict] | None":
    """The CALL_OFF_OF edge of a call-off contract (Contract ->
    FrameworkAgreement, from the entity like AWARDED_TO), or None when
    the payload names no framework. The sink stubs the
    :FrameworkAgreement when the establishing event has not arrived
    yet; that event's own MERGE on the same key fills the stub in.

    UNDER REVIEW, deliberately left as written for now. framework_id is
    eForms OPT-100, which the framework-establishing award notice and
    every call-off under it carry identically (761784-2024 and
    3406-2025 both carry '536632-2024'), and no field in the data
    separates the two — is_framework is on both. So a contract this
    edge calls a call-off may be the establishing award, and ~80% of
    the time the value names a call for competition we never ingest, so
    the node on the far end is a stub with nothing in it. Renaming the
    edge to something the data supports touches every reader, so it is
    a coordinated change, not a quiet one here; it has to land before a
    producer starts filling framework_id (0 :Contract carry it in prod
    today). Meanwhile the read path — "other awards under this
    framework" — runs off the indexed :Contract.framework_id property,
    never off this edge, so the rename does not block it."""
    fid = p.get("framework_id")
    if not fid:
        return None
    return (
        "CALL_OFF_OF",
        f"{FRAMEWORK_AGREEMENT_IRI}{fid}",
        {"_direction": "from_source"},
    )


# Scalar props of a :FrameworkAgreement. The ceiling is capacity, never
# spend: framework-establishing contracts never enter spend sums,
# call-offs do, and the reader-facing figure is "EUR X of a EUR Y
# ceiling consumed across N call-offs".
_FRAMEWORK_AGREEMENT_FIELDS: tuple[str, ...] = (
    "buyer_authority_id", "establishing_notice_id", "country",
    "ceiling_eur", "ceiling_currency", "ceiling_original",
    "reestimated_value_eur", "duration_start", "duration_end",
    "duration_months", "cpv", "lot_count", "supplier_count", "title",
)

# Edge props carried per suppliers[] item onto PARTY_TO.
_FRAMEWORK_SUPPLIER_EDGE_PROPS: tuple[str, ...] = ("lot", "rank")


def render_upsert_framework_agreement(p: dict) -> CypherWrite:
    """A framework agreement as a node of its own, keyed by the
    establishing PROCEDURE (framework_id = the establishing notice's
    contract_key), so every notice about one framework converges on one
    node — which per-notice :Contract cannot do.

      (:Authority)-[:ESTABLISHED]->(:FrameworkAgreement)<-[:CALL_OFF_OF]-(:Contract)
                                          ^          ^
                 (:Company)-[:PARTY_TO {lot, rank}]--+          |
                 (:Notice)-[:ESTABLISHES]-----------------------+

    Every operator admitted to the framework is a PARTY_TO; winning a
    call-off is a separate fact (:Contract AWARDED_TO), and CALL_OFF_OF
    is written by the call-off's own UpsertContract (call_off_edge).
    Scalars are set only when present — SET += never deletes, so an
    emit that omits a field leaves what an earlier emit wrote (the
    never-clear-on-absence rule of every other display field); on what
    is present the last writer wins. All three edge targets may arrive
    after this event: the sink stubs them and their own upserts fill
    the stubs in."""
    set_props = {
        k: p[k] for k in _FRAMEWORK_AGREEMENT_FIELDS if p.get(k) is not None
    }
    extras: list[tuple[str, str, dict]] = []
    if aid := p.get("buyer_authority_id"):
        extras.append((
            "ESTABLISHED",
            f"http://data.fontem.eu/id/Authority/{aid}",
            {"_direction": "from_target"},
        ))
    if nid := p.get("establishing_notice_id"):
        extras.append((
            "ESTABLISHES",
            f"http://data.fontem.eu/id/Notice/{nid}",
            {"_direction": "from_target"},
        ))
    for supplier in p.get("suppliers") or []:
        cid = supplier.get("company_gmr_id")
        if not cid:
            continue
        props: dict = {"_direction": "from_target"}
        for k in _FRAMEWORK_SUPPLIER_EDGE_PROPS:
            if supplier.get(k) is not None:
                props[k] = supplier[k]
        extras.append((
            "PARTY_TO",
            f"http://data.fontem.eu/id/Company/{cid}",
            props,
        ))
    return CypherWrite(
        label="FrameworkAgreement",
        primary_key={"framework_id": p["framework_id"]},
        set_props=set_props,
        extra_relationships=extras or None,
    )
