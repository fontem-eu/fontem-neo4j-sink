"""One-time Neo4j schema migration for the native Contract/Notice model.

The live graph has a unique constraint on :Contract(ted_notice_id)
(created ad hoc by fontem-api's backfill_ted_publication_numbers). Under
the Contract/Notice model that key moves to :Notice, and the :Contract
entity is keyed by contract_key. Mixed-period writes (new sink against
old constraints) throw, so this MUST run before the new sink image
starts writing.

Runbook (in this order):

1. Scale the neo4j sink deployment to 0 (events queue safely).
2. Run fontem-api ``python -m src.etl.project_contracts`` to convert the
   existing notice-grain graph (idempotent; skip if already converted).
3. Run this script.
4. Deploy the new sink image and scale back up.

The script is idempotent (DROP/CREATE ... IF [NOT] EXISTS) and aborts
before creating the :Contract(contract_key) uniqueness constraint if the
graph still holds duplicate contract_key values — i.e. step 2 has not
run and the constraint would fail against notice-grain data.
"""
from __future__ import annotations

import logging
import os
import sys

from neo4j import GraphDatabase

logger = logging.getLogger(__name__)

_DROP_LEGACY = (
    "DROP CONSTRAINT contract_ted_notice_id_unique IF EXISTS"
)
# Legacy replays + MODIFIES linking still look contracts up by
# ted_notice_id; keep that fast with a plain (non-unique) range index
# now that the uniqueness moved to :Notice.
_LEGACY_LOOKUP_INDEX = (
    "CREATE INDEX contract_ted_notice_id IF NOT EXISTS "
    "FOR (c:Contract) ON (c.ted_notice_id)"
)
_NOTICE_UNIQUE = (
    "CREATE CONSTRAINT notice_ted_notice_id_unique IF NOT EXISTS "
    "FOR (n:Notice) REQUIRE n.ted_notice_id IS UNIQUE"
)
_CONTRACT_UNIQUE = (
    "CREATE CONSTRAINT contract_contract_key_unique IF NOT EXISTS "
    "FOR (c:Contract) REQUIRE c.contract_key IS UNIQUE"
)
_DUPLICATE_CHECK = """
MATCH (c:Contract)
WHERE c.contract_key IS NOT NULL
WITH c.contract_key AS ck, count(*) AS n
WHERE n > 1
RETURN count(ck) AS duplicated_keys
"""


def migrate(driver) -> None:
    with driver.session() as session:
        dupes = session.run(_DUPLICATE_CHECK).single()["duplicated_keys"]
        if dupes:
            logger.error(
                "%d contract_key values are shared by multiple :Contract "
                "nodes — the graph is still notice-grain. Run fontem-api "
                "src/etl/project_contracts.py first, then re-run this "
                "migration.", dupes,
            )
            sys.exit(1)
        for stmt in (_DROP_LEGACY, _LEGACY_LOOKUP_INDEX,
                     _NOTICE_UNIQUE, _CONTRACT_UNIQUE):
            logger.info("applying: %s", stmt.strip())
            session.run(stmt)
    logger.info("contract/notice constraint migration complete")


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
