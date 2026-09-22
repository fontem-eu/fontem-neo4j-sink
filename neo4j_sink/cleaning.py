"""What the ingest cleaning stage adds to the graph beyond scalars.

gitops data-backlog Part 5 lands the cleaner (C2/C4) and the framework
agreement node (C6) in one reprocess campaign (C8). The scalar fields
they add ride cypher.py's per-notice whitelist; this module holds the
shapes that are not plain scalars, split out of cypher.py the way
identity.py was — the pair that has to stay consistent lives together:
the withheld-supplier lists (C2), and the :FrameworkAgreement node plus
the CALL_OFF_OF edge that must resolve it by the same key (C6).
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


def call_off_edge(p: dict) -> "tuple[str, str, dict] | None":
    """The CALL_OFF_OF edge of a call-off contract (Contract ->
    FrameworkAgreement, from the entity like AWARDED_TO), or None when
    the payload names no framework. The sink stubs the
    :FrameworkAgreement when the establishing event has not arrived
    yet; that event's own MERGE on the same key fills the stub in."""
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
