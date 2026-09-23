"""Neo4j index for :Contract(framework_id) — the framework grouping key.

framework_id is eForms OPT-100
(efac:NoticeResult/efac:SettledContract/cac:NoticeDocumentReference/cbc:ID).
The framework-establishing award notice and every call-off under it
carry the IDENTICAL value — notices 761784-2024 and 3406-2025 both
carry '536632-2024' — so the reader-facing query is "every other award
that carries this framework_id", a straight equality lookup on a
:Contract property.

A RANGE index, not a uniqueness constraint: the value is shared by
design (that is the whole point of a grouping key), and of 176 real
frameworks 55 (31.2%) are carried by two or more award notices.
:FrameworkAgreement keeps its own uniqueness constraint
(framework_agreement_constraints_2026_09.py) because there the value IS
the identity of one node.

Why it is not optional. Measured on prod (fontem-prod neo4j, 3,606,471
:Contract) on 2026-09-23, without the index:

    PROFILE MATCH (c:Contract) WHERE c.framework_id = '536632-2024'
      AND c.contract_key <> '2f3cab57-c3ee-4c35-9077-a056374882f2'
    OPTIONAL MATCH (a:Authority)-[:AWARDED]->(c)
    RETURN c.contract_key, c.title, c.value_eur, a.name
    ORDER BY c.publication_date DESC LIMIT 50

    -> NodeByLabelScan, DbHits 7,212,943, 8.3 s

against the API's 8 s cap — i.e. the sibling list times out before it
renders. With the index the same lookup is a NodeIndexSeek.

Run it BEFORE the first producer emits UpsertContract.framework_id, so
the scan never reaches a read path (0 :Contract carry the property in
prod as of 2026-09-23). Idempotent (CREATE ... IF NOT EXISTS), so it is
safe before or after the sink image that writes the property is live,
and safe to run again:

    NEO4J_URI=bolt://... NEO4J_PASSWORD=... \\
        python migrations/contract_framework_id_index_2026_09.py
"""
from __future__ import annotations

import logging
import os

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

STATEMENTS: tuple[str, ...] = (
    "CREATE INDEX contract_framework_id IF NOT EXISTS "
    "FOR (c:Contract) ON (c.framework_id)",
)


def migrate(driver) -> None:
    with driver.session() as session:
        for stmt in STATEMENTS:
            logger.info("applying: %s", stmt)
            session.run(stmt)
    logger.info("contract framework_id index migration complete")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    driver = GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ.get("NEO4J_USER", "neo4j"),
              os.environ["NEO4J_PASSWORD"]),
    )
    try:
        migrate(driver)
    finally:
        driver.close()


if __name__ == "__main__":
    main()
