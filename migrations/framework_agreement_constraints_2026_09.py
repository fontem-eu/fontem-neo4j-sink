"""Neo4j schema for the :FrameworkAgreement node (data-backlog Part 5, C6).

framework_id is the node's identity (the establishing procedure's
contract_key) and the field every CALL_OFF_OF / ESTABLISHED / PARTY_TO /
ESTABLISHES edge resolves the node by, so it gets a uniqueness
constraint — which in Neo4j is also the index those MERGEs and MATCHes
seek on. A stub minted by an early call-off and the establishing
event's own MERGE therefore meet on one node.

Idempotent (CREATE ... IF NOT EXISTS): safe to run before or after the
sink image that writes the label is live, and to run again. Run it
BEFORE any producer emits UpsertFrameworkAgreement, so the first writes
are indexed:

    NEO4J_URI=bolt://... NEO4J_PASSWORD=... \\
        python migrations/framework_agreement_constraints_2026_09.py
"""
from __future__ import annotations

import logging
import os

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

STATEMENTS: tuple[str, ...] = (
    "CREATE CONSTRAINT framework_agreement_framework_id_unique IF NOT EXISTS "
    "FOR (f:FrameworkAgreement) REQUIRE f.framework_id IS UNIQUE",
)


def migrate(driver) -> None:
    with driver.session() as session:
        for stmt in STATEMENTS:
            logger.info("applying: %s", stmt)
            session.run(stmt)
    logger.info("framework agreement constraint migration complete")


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
