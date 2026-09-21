"""Take a wrongly assembled contract apart and rebuild it from the log.

    python -m neo4j_sink.repair_chains scan
    python -m neo4j_sink.repair_chains plan    --entity 260030-2022
    python -m neo4j_sink.repair_chains rebuild --entity 260030-2022 --apply
    python -m neo4j_sink.repair_chains rebuild --suspects --apply

Run it where the sink runs (it needs the sink's environment: NEO4J_*,
EVENTS_DATABASE_URL):

    kubectl -n fontem-prod exec deploy/neo4j-sink -- \\
        python -m neo4j_sink.repair_chains scan

WHY. One :Contract entity is supposed to be one contract. The chain
step used to trust every back-link, so a buyer's typo could fold an
unrelated contract into it (plausibility.py has the story), and the
fold is destructive: apoc.refactor.mergeNodes discards the folded
entity's properties and pools its AWARDED / AWARDED_TO edges, so the
graph afterwards cannot say which notice brought which buyer. The
event log can. Every notice's full UpsertContract payload is there,
keyed by IRI.

HOW. Nothing is patched by hand. The entity is deleted and its
notices are fed back through the sink's own write path — the same
code that handles live events — now with the plausibility check in
the link step. Entities, NOTICE_OF, AWARDED, AWARDED_TO, BID_ON,
MODIFIES, adoption and roll-up all come out the way a fresh ingest
would produce them, which is the only definition of "right" this
graph has. Only four relationship types touch a :Contract and the
write path produces all four, so nothing else is lost.

One correction the write path cannot make by itself: a LEGACY
modification has no identity but its back-link (its contract_key IS
the number it says it modifies), so a wrong number puts it on the
wrong entity by key, link or no link. When the rule rejects such a
back-link, the notice is replayed under its own publication number.

`rebuild` without --apply prints the plan and changes nothing.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import os
import sys
from collections import defaultdict
from dataclasses import dataclass, field

import psycopg
from fontem_event_schemas import EventEnvelope
from fontem_events.consumer import ConsumerConfig

from .chain import CHAIN_ROLLUP_CYPHER
from .cypher import _ROLLUP_ONLY_KEYS, _notice_kind, notice_parties
from .plausibility import REJECT, assess_link

logger = logging.getLogger("repair_chains")

IRI = "http://data.fontem.eu/id/Contract/{}"
_REPLAY_BATCH = 200
_MAX_NOTICES = 5000

# The newest events first; the first that is a whole notice wins. A
# notice's tail can hold value-rollup patches (collapse_modifications,
# retired 2026-09) that carry four keys and no notice.
_EVENTS_SQL = (
    "SELECT seq, payload FROM events.entity_events "
    "WHERE iri = %s AND event_type = 'UpsertContract' "
    "ORDER BY seq DESC LIMIT 40"
)
# Entities whose buyers sit in more than one country. Cross-border
# joint procurement exists, so this nominates; the log decides.
# Starts from the degree of each entity (read off the relationship
# store, no expansion) rather than aggregating all ~700k AWARDED edges:
# the obvious "MATCH the edges, collect the countries" form dies on
# prod's 1 GiB per-transaction memory limit.
_SUSPECTS_CYPHER = (
    "MATCH (e:Contract) WHERE COUNT { (e)<-[:AWARDED]-() } > 1 "
    "MATCH (a:Authority)-[:AWARDED]->(e) "
    "WITH e, collect(DISTINCT a.country) AS countries "
    "WHERE size([c IN countries WHERE c IS NOT NULL]) > 1 "
    "RETURN e.contract_key AS key, countries"
)
_NOTICES_CYPHER = (
    "MATCH (n:Notice)-[:NOTICE_OF]->(:Contract { contract_key: $key }) "
    "RETURN n.ted_notice_id AS nid"
)
_RESOLVE_CYPHER = (
    "UNWIND $refs AS ref "
    "OPTIONAL MATCH (p1:Notice { ted_notice_id: ref.nid }) "
    "OPTIONAL MATCH (p2:Notice { ted_publication_number: ref.pub }) "
    "WITH ref, coalesce(p1, p2) AS p WHERE p IS NOT NULL "
    "RETURN ref.m AS m, p.ted_notice_id AS target, "
    "EXISTS { (:Notice { ted_notice_id: ref.m })-[:MODIFIES]->(p) } AS linked"
)
_TAKE_APART_CYPHER = (
    "MATCH (n:Notice) WHERE n.ted_notice_id IN $nids "
    "OPTIONAL MATCH (n)-[:NOTICE_OF]->(o:Contract) "
    "WITH collect(DISTINCT n) AS ns, collect(DISTINCT o.contract_key) AS keys "
    "UNWIND ns AS n "
    "OPTIONAL MATCH (n)-[m:MODIFIES]-() DELETE m "
    "WITH n, keys OPTIONAL MATCH (n)-[r:NOTICE_OF]->() DELETE r "
    "WITH DISTINCT keys "
    "MATCH (e:Contract { contract_key: $key }) DETACH DELETE e "
    "RETURN keys"
)
# An entity the rebuild left without a single notice has no provenance.
_DROP_HUSKS_CYPHER = (
    "MATCH (e:Contract) WHERE e.contract_key IN $keys "
    "AND NOT EXISTS { (e)<-[:NOTICE_OF]-(:Notice) } "
    "WITH e, e.contract_key AS key DETACH DELETE e RETURN collect(key) AS dropped"
)
_ANY_NOTICE_CYPHER = (
    "MATCH (e:Contract) WHERE e.contract_key IN $keys "
    "MATCH (e)<-[:NOTICE_OF]-(n:Notice) "
    "WITH e, head(collect(n.ted_notice_id)) AS nid RETURN nid"
)
_AFTER_CYPHER = (
    "MATCH (n:Notice) WHERE n.ted_notice_id IN $nids "
    "MATCH (n)-[:NOTICE_OF]->(e:Contract) "
    "OPTIONAL MATCH (a:Authority)-[:AWARDED]->(e) "
    "WITH e, count(DISTINCT n) AS ours, collect(DISTINCT a.name)[..3] AS buyers "
    "RETURN e.contract_key AS key, ours, e.notice_count AS notices, "
    "e.award_ingested AS award, e.current_value AS value, buyers "
    "ORDER BY ours DESC"
)


def facts(payload: dict) -> dict:
    """What plausibility.assess_link reads, from an event payload."""
    parties = notice_parties(payload)
    return {
        "nid": payload["ted_notice_id"],
        "procedure_id": payload.get("procedure_id"),
        "legacy_procedure_id": payload.get("legacy_procedure_id"),
        "title": payload.get("title"),
        "country": payload.get("country"),
        "buyer_ids": [parties["buyer_id"]] if "buyer_id" in parties else [],
        "winner_ids": parties.get("winner_ids", []),
        "winner_names": parties.get("winner_names", []),
    }


def own_key(payload: dict) -> str:
    """The identity a notice has without its back-link."""
    return (payload.get("procedure_id") or payload.get("ted_publication_number")
            or payload["ted_notice_id"])


def with_identity(payload: dict) -> dict:
    """A whole notice, stamped with a contract_key if it has none.

    Notices ingested before the Contract/Notice model (mid-2026) went
    into the log without one; the retired collapse job stamped the
    graph afterwards, and the 2026-09 re-ingest skipped them because
    the graph already looked stamped. Replayed as they are they would
    take the sink's frozen pre-native path and come back as something
    else. The key is derived exactly as the ingest path derives it
    today — fontem-api load_ted_contracts.derive_contract_key, which
    this mirrors and must keep mirroring."""
    if payload.get("contract_key"):
        return payload
    back_link = payload.get("modifies_publication_number")
    if payload.get("procedure_id"):
        key = payload["procedure_id"]
    elif _notice_kind(payload) == "modification" and back_link:
        key = back_link
    else:
        key = own_key(payload)
    return {**payload, "contract_key": key}


def is_keyed_by_back_link(payload: dict) -> bool:
    """A legacy modification: load_ted_contracts.derive_contract_key
    gave it the number it says it modifies."""
    return (not payload.get("procedure_id")
            and payload.get("modifies_publication_number") is not None
            and payload.get("contract_key") == payload["modifies_publication_number"])


@dataclass
class Plan:
    """What rebuilding one entity would do. Pure data: `plan` prints
    it, `rebuild --apply` executes it."""
    key: str
    payloads: dict[str, dict] = field(default_factory=dict)
    seqs: dict[str, int] = field(default_factory=dict)
    missing: list[str] = field(default_factory=list)
    refused: list[dict] = field(default_factory=list)
    accepted: list[tuple[str, str]] = field(default_factory=list)
    rekeyed: dict[str, str] = field(default_factory=dict)

    @property
    def groups(self) -> dict[str, list[str]]:
        out: dict[str, list[str]] = defaultdict(list)
        for nid, p in self.payloads.items():
            out[p["contract_key"]].append(nid)
        return dict(out)

    @property
    def contracts(self) -> list[list[str]]:
        """The notices partitioned into the contracts they really are:
        joined by a shared identity or by a back-link the rule accepts.
        Differently keyed notices on one entity are normal (an eForms
        modification adopted onto a legacy award); notices that NOTHING
        joins are two contracts."""
        parent = {nid: nid for nid in self.payloads}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for nids in self.groups.values():
            for nid in nids[1:]:
                parent[find(nid)] = find(nids[0])
        for m, t in self.accepted:
            if t in parent:
                parent[find(m)] = find(t)
        out: dict[str, list[str]] = defaultdict(list)
        for nid in self.payloads:
            out[find(nid)].append(nid)
        return sorted(out.values(), key=len, reverse=True)

    @property
    def changes_anything(self) -> bool:
        """A refused back-link stays declared on its notice for ever;
        it needs repair only while the graph still acts on it — the
        edge exists, or the notices it joined share this entity."""
        return (len(self.contracts) > 1 or bool(self.rekeyed)
                or any(r["linked"] for r in self.refused))

    def describe(self) -> str:
        lines = [f"entity {self.key}: {len(self.payloads)} notices"]
        if self.missing:
            lines.append(f"  NOT REBUILDABLE — no whole notice in the log for: "
                         f"{', '.join(self.missing[:8])}"
                         f"{' …' if len(self.missing) > 8 else ''}")
        for r in self.refused:
            lines.append(f"  refuse {r['m']} -> {r['t']}"
                         f"{'' if r['linked'] else ' (already unlinked)'}: {r['reason']}")
        for nid, key in self.rekeyed.items():
            lines.append(f"  re-key {nid}: back-link key -> {key}")
        for nids in self.contracts:
            sample = self.payloads[nids[0]]
            keys = sorted({self.payloads[n]["contract_key"] for n in nids})
            lines.append(f"  contract of {len(nids)} notices [{sample.get('country')}] "
                         f"{(sample.get('title') or '')[:50]!r} keys={keys[:3]}")
        if not self.changes_anything and not self.missing:
            lines.append("  nothing to repair: one contract, and no refused "
                         "back-link is still linked")
        return "\n".join(lines)


class Repairer:
    """Reads the event log and the graph; writes only through the sink."""

    def __init__(self, sink, pg):
        self._sink = sink
        self._driver = sink._driver  # pylint: disable=protected-access
        self._pg = pg

    # ── reading ───────────────────────────────────────────

    def whole_notice(self, nid: str) -> "tuple[int, dict] | None":
        with self._pg.cursor() as cur:
            cur.execute(_EVENTS_SQL, (IRI.format(nid),))
            for seq, payload in cur.fetchall():
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if not set(payload).issubset(_ROLLUP_ONLY_KEYS):
                    return seq, with_identity(payload)
        return None

    def suspects(self) -> list[dict]:
        with self._driver.session() as s:
            return [r.data() for r in s.run(_SUSPECTS_CYPHER)]

    def plan(self, key: str) -> Plan:
        plan = Plan(key)
        with self._driver.session() as s:
            nids = [r["nid"] for r in s.run(_NOTICES_CYPHER, key=key)]
            if len(nids) > _MAX_NOTICES:
                raise SystemExit(f"{key} has {len(nids)} notices; that is not a "
                                 f"contract, look at it by hand first")
            for nid in nids:
                found = self.whole_notice(nid)
                if found is None:
                    plan.missing.append(nid)
                else:
                    plan.seqs[nid], plan.payloads[nid] = found
            refs = [{"m": nid, "nid": p.get("modifies_notice_id"),
                     "pub": p.get("modifies_publication_number")}
                    for nid, p in plan.payloads.items()
                    if p.get("modifies_notice_id") or p.get("modifies_publication_number")]
            resolved = [r.data() for r in s.run(_RESOLVE_CYPHER, refs=refs)]
        for nid, target, linked in ((r["m"], r["target"], r["linked"]) for r in resolved):
            if target == nid:
                continue
            t_payload = plan.payloads.get(target) or (self.whole_notice(target) or (0, None))[1]
            if t_payload is None:
                continue
            verdict = assess_link(facts(plan.payloads[nid]), facts(t_payload))
            if verdict.status != REJECT:
                plan.accepted.append((nid, target))
                continue
            plan.refused.append({"m": nid, "t": target, "reason": verdict.reason,
                                 "linked": linked})
            if is_keyed_by_back_link(plan.payloads[nid]):
                rekeyed = dict(plan.payloads[nid])
                rekeyed["contract_key"] = own_key(rekeyed)
                plan.payloads[nid] = rekeyed
                plan.rekeyed[nid] = rekeyed["contract_key"]
        return plan

    # ── writing ───────────────────────────────────────────

    def rebuild(self, plan: Plan) -> list[dict]:
        if plan.missing:
            raise SystemExit(f"refusing to rebuild {plan.key}: "
                             f"{len(plan.missing)} notices cannot be replayed")
        nids = list(plan.payloads)
        with self._driver.session() as s:
            touched = s.run(_TAKE_APART_CYPHER, nids=nids, key=plan.key).single()
            touched_keys = set(touched["keys"] if touched else []) | {plan.key}
        ordered = sorted(nids, key=lambda n: (
            plan.payloads[n].get("publication_date") or "", n))
        now = dt.datetime.now(dt.UTC)
        for i in range(0, len(ordered), _REPLAY_BATCH):
            self._sink.handle([
                EventEnvelope(
                    event_type="UpsertContract", iri=IRI.format(nid),
                    domain="contract", op="upsert", payload=plan.payloads[nid],
                    producer="repair_chains", ts=now, seq=plan.seqs[nid],
                ) for nid in ordered[i:i + _REPLAY_BATCH]
            ])
        with self._driver.session() as s:
            dropped = s.run(_DROP_HUSKS_CYPHER, keys=sorted(touched_keys)).single()["dropped"]
            if dropped:
                logger.info("dropped entities left without a notice: %s", dropped)
            # Entities that only LOST notices were not rolled up by the replay.
            rows = [{"nid": r["nid"]} for r in
                    s.run(_ANY_NOTICE_CYPHER, keys=sorted(touched_keys))]
            if rows:
                s.run(CHAIN_ROLLUP_CYPHER, rows=rows)
            return [r.data() for r in s.run(_AFTER_CYPHER, nids=nids)]


def _connect():
    from .sink import Neo4jSink  # pylint: disable=import-outside-toplevel
    dsn = os.environ["EVENTS_DATABASE_URL"]
    sink = Neo4jSink(ConsumerConfig(name="repair_chains", dsn=dsn, metrics_port=None))
    return Repairer(sink, psycopg.connect(dsn, autocommit=True))


def main(argv: "list[str] | None" = None) -> int:
    parser = argparse.ArgumentParser(prog="repair_chains", description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("scan", help="entities whose buyers span countries, judged from the log")
    for name in ("plan", "rebuild"):
        p = sub.add_parser(name)
        p.add_argument("--entity", action="append", default=[], metavar="CONTRACT_KEY")
        p.add_argument("--suspects", action="store_true",
                       help="every entity `scan` says needs repair")
        if name == "rebuild":
            p.add_argument("--apply", action="store_true",
                           help="without it, print the plan and change nothing")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    repairer = _connect()

    keys = list(getattr(args, "entity", []))
    if args.cmd == "scan" or getattr(args, "suspects", False):
        keys += [s["key"] for s in repairer.suspects()]
    if not keys:
        parser.error("give --entity KEY or --suspects")
    broken = 0
    for key in dict.fromkeys(keys):
        plan = repairer.plan(key)
        if args.cmd == "scan" and not plan.changes_anything:
            continue
        broken += 1
        print(plan.describe())
        if args.cmd == "rebuild" and args.apply and plan.changes_anything:
            for row in repairer.rebuild(plan):
                print(f"  now {row['key']}: {row['ours']} of these notices "
                      f"({row['notices']} total), award={row['award']}, "
                      f"value={row['value']}, buyers={row['buyers']}")
    print(f"{broken} of {len(dict.fromkeys(keys))} entities "
          f"{'need repair' if args.cmd != 'rebuild' or not args.apply else 'processed'}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
