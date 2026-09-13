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

from .writes import CypherWrite

# 1. Link. The notice's back-link resolves to the previous notice by
#    notice id or by publication number (two indexed seeks, not an
#    OR-disjunction). Then the reverse direction: notices already in
#    the graph whose back-link names THIS notice (a late award, or a
#    late middle notice) are linked too. A `{prop: null}` pattern
#    never matches, so absent back-links cost nothing.
CHAIN_LINK_CYPHER = (
    "UNWIND $rows AS row "
    "MATCH (n:Notice { ted_notice_id: row.nid }) "
    "OPTIONAL MATCH (p1:Notice { ted_notice_id: row.prev_nid }) "
    "OPTIONAL MATCH (p2:Notice { ted_publication_number: row.prev_pub }) "
    "WITH n, coalesce(p1, p2) AS p "
    "FOREACH (_ IN CASE WHEN p IS NOT NULL AND p <> n THEN [1] ELSE [] END | "
    "MERGE (n)-[:MODIFIES]->(p)) "
    "WITH n "
    "OPTIONAL MATCH (s1:Notice { modifies_notice_id: n.ted_notice_id }) "
    "WITH n, collect(s1) AS by_id "
    "OPTIONAL MATCH (s2:Notice { modifies_publication_number: "
    "n.ted_publication_number }) "
    "WITH n, by_id, collect(s2) AS by_pub "
    "WITH n, by_id + by_pub AS succ "
    "FOREACH (s IN [x IN succ WHERE x <> n] | MERGE (s)-[:MODIFIES]->(n))"
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
    "SET e.award_ingested = has_award, "
    "e.notice_kind = CASE WHEN has_award THEN 'award' ELSE 'modification' END, "
    "e.notice_count = size(ordered), "
    "e.is_current = true, "
    "e.current_value = CASE WHEN size(valued) > 0 THEN valued[0].value_eur END, "
    "e.value_eur = CASE WHEN size(valued) > 0 THEN valued[0].value_eur END, "
    "e.ted_notice_id = latest.ted_notice_id, "
    "e.canonical_publication_date = latest.publication_date "
    "FOREACH (x IN ordered | SET x.is_current = (x = latest), "
    "x.contract_key = e.contract_key)"
)

def apply_contract_chains(driver, writes: list[CypherWrite]) -> None:
    """Link → adopt → roll up. Link and roll-up are one UNWIND per
    batch; adopt runs per notice (see CHAIN_ADOPT_CYPHER). Each step is
    idempotent, so a redelivered batch converges."""
    if not writes:
        return
    rows = [{
        "nid": w.primary_key["ted_notice_id"],
        "prev_nid": w.set_props.get("modifies_notice_id"),
        "prev_pub": w.set_props.get("modifies_publication_number"),
    } for w in writes]
    with driver.session() as session:
        session.run(CHAIN_LINK_CYPHER, rows=rows)
        for row in rows:
            session.run(CHAIN_ADOPT_CYPHER, nid=row["nid"])
        session.run(CHAIN_ROLLUP_CYPHER, rows=rows)
