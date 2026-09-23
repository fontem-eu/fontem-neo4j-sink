"""The contract chain, maintained by the sink at write time.

A contract is one :Contract entity with every notice of its chain
(the award and each modification) attached by NOTICE_OF. This module
is what makes that true on the FIRST write of each notice, so no
linking or collapse pass is needed afterwards, and a replay of the log
from seq 0 converges on the same graph whatever order the notices came
in. Three idempotent steps per batch, applied by the sink right after
the NOTICE_OF edges (see Neo4jSink._apply_batch).
"""
from __future__ import annotations

import logging

from .plausibility import DOUBTFUL, OK, REJECT, assess_link
from .writes import CypherWrite

logger = logging.getLogger(__name__)

# What a verdict leaves on the modifying notice (back_link_status).
_STATUS_ON_NOTICE = {OK: None, DOUBTFUL: "doubtful", REJECT: "rejected"}

# 1. Link. The notice's back-link resolves to the previous notice by
#    notice id or by publication number (two indexed seeks, not an
#    OR-disjunction). Then the reverse direction: notices already in
#    the graph whose back-link names THIS notice (a late award, or a
#    late middle notice). A `{prop: null}` pattern never matches, so
#    absent back-links cost nothing.
#
#    Resolving is not linking. The number in a back-link is typed by
#    the buyer and is sometimes wrong, and a MODIFIES edge is what lets
#    the adopt step fold two entities into one — so every candidate is
#    judged first (plausibility.assess_link) on what the two notices
#    themselves say: procedure, buyer, contractor, title, country. The
#    query only gathers those facts; the verdict is Python's, so the
#    rule has one home and unit tests that need no database.
#
#    A notice written before parties were stamped on notices
#    (cypher.notice_parties) has no buyer_id of its own; the parties of
#    the entity it sits on stand in. On a wrongly fused entity that set
#    is polluted, which can only make a link look MORE plausible —
#    repair_chains judges such entities from the event log instead.
def _facts(var: str) -> str:
    return (
        f"{var} {{ nid: {var}.ted_notice_id, .procedure_id, "
        f".legacy_procedure_id, .title, .country, "
        f"winner_names: coalesce({var}.winner_names, []), "
        f"buyer_ids: CASE WHEN {var}.buyer_id IS NOT NULL THEN [{var}.buyer_id] "
        f"ELSE [({var})-[:NOTICE_OF]->(:Contract)<-[:AWARDED]-(a:Authority) "
        f"| a.authority_id] END, "
        f"winner_ids: coalesce({var}.winner_ids, "
        f"[({var})-[:NOTICE_OF]->(:Contract)-[:AWARDED_TO]->(c:Company) "
        f"| c.gmr_id]) }}"
    )


CHAIN_CANDIDATES_CYPHER = (
    "UNWIND $rows AS row "
    "MATCH (n:Notice { ted_notice_id: row.nid }) "
    "OPTIONAL MATCH (p1:Notice { ted_notice_id: row.prev_nid }) "
    "OPTIONAL MATCH (p2:Notice { ted_publication_number: row.prev_pub }) "
    "WITH n, coalesce(p1, p2) AS p "
    "OPTIONAL MATCH (s1:Notice { modifies_notice_id: n.ted_notice_id }) "
    "WITH n, p, collect(s1) AS by_id "
    "OPTIONAL MATCH (s2:Notice { modifies_publication_number: "
    "n.ted_publication_number }) "
    "WITH n, p, by_id, collect(s2) AS by_pub "
    "WITH n, p, by_id + by_pub AS succ "
    f"RETURN {_facts('n')} AS notice, "
    f"CASE WHEN p IS NOT NULL AND p <> n THEN {_facts('p')} END AS target, "
    f"[s IN succ WHERE s <> n | {_facts('s')}] AS successors"
)
# `status` is null for an ordinary link, so the two properties exist
# only on the few notices that need a second look.
CHAIN_LINK_CYPHER = (
    "UNWIND $links AS l "
    "MATCH (m:Notice { ted_notice_id: l.m }) "
    "MATCH (t:Notice { ted_notice_id: l.t }) "
    "MERGE (m)-[:MODIFIES]->(t) "
    "SET m.back_link_status = l.status, m.back_link_reason = l.reason"
)
# A rejected link also removes an edge an earlier version of the sink
# may have written. That stops the chain from growing through it; an
# entity it already fused is taken apart by repair_chains.
CHAIN_REJECT_CYPHER = (
    "UNWIND $links AS l "
    "MATCH (m:Notice { ted_notice_id: l.m }) "
    "SET m.back_link_status = l.status, m.back_link_reason = l.reason "
    "WITH m, l "
    "MATCH (m)-[r:MODIFIES]->(:Notice { ted_notice_id: l.t }) "
    "DELETE r"
)
# 2. Adopt. The chain is everything reachable over MODIFIES. Its root
#    is the earliest award, or the earliest notice when no award has
#    been ingested. Every other :Contract entity the chain's notices
#    point at is folded INTO the root's entity — but only an entity that
#    has no award of its own outside this chain. A modification whose
#    back-link names another contract's award (a buyer's typo, a wrong
#    reference) must not drag that whole contract into this one: both
#    stay, and the split meter reports it. An entity whose awards are
#    all in the chain (the same award re-stamped under a new key, or a
#    modification-only entity) is folded in with apoc.refactor.mergeNodes,
#    which keeps the root's properties and moves the edges — NOTICE_OF,
#    AWARDED, AWARDED_TO, BID_ON — so nothing is lost. The root's entity
#    is the one keyed by the root's own stamped contract_key: a notice
#    re-stamped with a new key briefly has two NOTICE_OF edges, and the
#    newly stamped identity must win.
#
#    One notice per statement, not an UNWIND over the batch: a merge
#    deletes a node, and a later row of the same statement that had
#    already resolved that node (its chain shares an entity through
#    NOTICE_OF but not through MODIFIES) fails with "Node not found".
CHAIN_ADOPT_CYPHER = (
    "MATCH (n:Notice { ted_notice_id: $nid }) "
    "MATCH (n)-[:MODIFIES*0..30]-(x:Notice) "
    "WITH collect(DISTINCT x) AS chain "
    "WITH chain, [x IN chain WHERE x.notice_kind = 'award'] AS awards "
    "WITH chain, CASE WHEN size(awards) > 0 THEN awards ELSE chain END AS cands "
    "UNWIND cands AS c "
    "WITH chain, c ORDER BY coalesce(c.publication_date, ''), c.ted_notice_id "
    "WITH chain, head(collect(c)) AS root "
    "MATCH (root)-[:NOTICE_OF]->(e:Contract { contract_key: root.contract_key }) "
    "UNWIND chain AS x "
    "MATCH (x)-[:NOTICE_OF]->(o:Contract) WHERE o <> e "
    "AND NOT EXISTS { (o)<-[:NOTICE_OF]-(a:Notice { notice_kind: 'award' }) "
    "WHERE NOT a IN chain } "
    "WITH DISTINCT e, o "
    "CALL apoc.refactor.mergeNodes([e, o], "
    "{properties: 'discard', mergeRels: true}) YIELD node "
    "RETURN count(node) AS merged"
)
# 3. Roll up. On each entity the chain touched: is_current on exactly
#    the latest notice (by publication date), award_ingested /
#    notice_kind from whether an award is in the chain, notice_count
#    from the chain, current_value = the latest restated value
#    (the newest notice whose value was not withheld), and every
#    notice's contract_key restated as the entity's, so notice and
#    entity never disagree on identity.
CHAIN_ROLLUP_CYPHER = (
    "UNWIND $rows AS row "
    "MATCH (:Notice { ted_notice_id: row.nid })-[:NOTICE_OF]->(e:Contract) "
    "WITH DISTINCT e "
    "MATCH (e)<-[:NOTICE_OF]-(x:Notice) "
    "WITH e, x ORDER BY coalesce(x.publication_date, '') DESC, x.ted_notice_id DESC "
    "WITH e, collect(x) AS ordered "
    "WITH e, ordered, ordered[0] AS latest, "
    "[x IN ordered WHERE x.value_eur IS NOT NULL] AS valued, "
    "any(x IN ordered WHERE x.notice_kind = 'award') AS has_award "
    # A quarantined canonical notice means the platform does not stand
    # behind this contract's value. `valued` skips that notice (it
    # carries no value_eur, by design), so without this guard the
    # rollup reaches PAST it to an older notice and writes that figure
    # back onto the entity — silently undoing the quarantine clear the
    # entity write just made. That is the whole of
    # values.quarantined_carries_no_value: 860 prod violations on
    # 2026-09-23, every one of them a contract whose chain holds an
    # older notice that still had a number.
    "WITH e, ordered, latest, valued, has_award, "
    "coalesce(latest.value_quarantined, false) AS quarantined "
    "SET e.award_ingested = has_award, "
    "e.notice_kind = CASE WHEN has_award THEN 'award' ELSE 'modification' END, "
    "e.notice_count = size(ordered), "
    "e.is_current = true, "
    "e.current_value = CASE WHEN quarantined THEN null "
    "  WHEN size(valued) > 0 THEN valued[0].value_eur END, "
    "e.value_eur = CASE WHEN quarantined THEN null "
    "  WHEN size(valued) > 0 THEN valued[0].value_eur END, "
    "e.value_original = CASE WHEN quarantined THEN null "
    "  ELSE e.value_original END, "
    "e.value_currency = CASE WHEN quarantined THEN null "
    "  ELSE e.value_currency END, "
    # The marker follows the canonical notice for the same reason the
    # value does. The mirror of the bug above: an entity kept a
    # quarantine marker an OLDER notice left behind while a newer,
    # healthy notice supplied the value — quarantined and valued at
    # once (214 of the 862 prod violations on 2026-09-23). Deriving
    # both from `latest` keeps the two halves of the value story from
    # coming from different notices.
    "e.value_quarantined = CASE WHEN quarantined THEN true ELSE null END, "
    "e.value_quarantine_reason = CASE WHEN quarantined "
    "  THEN latest.value_quarantine_reason ELSE null END, "
    "e.ted_notice_id = latest.ted_notice_id, "
    "e.canonical_publication_date = latest.publication_date "
    "FOREACH (x IN ordered | SET x.is_current = (x = latest), "
    "x.contract_key = e.contract_key)"
)

def judge_candidates(records) -> tuple[list[dict], list[dict]]:
    """(links to write, links to refuse) from CHAIN_CANDIDATES_CYPHER
    rows. Each is {m, t, status, reason} — m modifies t."""
    links: dict[tuple[str, str], dict] = {}
    for rec in records:
        notice = rec["notice"]
        pairs = [(s, notice) for s in rec["successors"]]
        if rec["target"] is not None:
            pairs.append((notice, rec["target"]))
        for m, t in pairs:
            verdict = assess_link(m, t)
            status = _STATUS_ON_NOTICE[verdict.status]
            links[(m["nid"], t["nid"])] = {
                "m": m["nid"], "t": t["nid"], "linkable": verdict.linkable,
                "status": status,
                "reason": verdict.reason if status else None,
            }
    accepted = [l for l in links.values() if l["linkable"]]
    refused = [l for l in links.values() if not l["linkable"]]
    return accepted, refused


def link_notices(session, rows: list[dict]) -> None:
    """Step 1 for a batch: gather, judge, write."""
    records = [r.data() for r in session.run(CHAIN_CANDIDATES_CYPHER, rows=rows)]
    accepted, refused = judge_candidates(records)
    if accepted:
        session.run(CHAIN_LINK_CYPHER, links=accepted)
    if refused:
        for l in refused:
            logger.warning("back-link refused: %s -> %s (%s)",
                           l["m"], l["t"], l["reason"])
        session.run(CHAIN_REJECT_CYPHER, links=refused)


def apply_contract_chains(driver, writes: list[CypherWrite]) -> None:
    """Link → adopt → roll up. Link (gather, judge, write) and roll-up
    are per batch; adopt runs per notice (see CHAIN_ADOPT_CYPHER). Each step is
    idempotent, so a redelivered batch converges."""
    if not writes:
        return
    rows = [{
        "nid": w.primary_key["ted_notice_id"],
        "prev_nid": w.set_props.get("modifies_notice_id"),
        "prev_pub": w.set_props.get("modifies_publication_number"),
    } for w in writes]
    with driver.session() as session:
        link_notices(session, rows)
        for row in rows:
            session.run(CHAIN_ADOPT_CYPHER, nid=row["nid"])
        session.run(CHAIN_ROLLUP_CYPHER, rows=rows)
