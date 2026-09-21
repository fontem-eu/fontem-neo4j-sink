> ### 🪞 This GitHub repository is a mirror
>
> Development happens on Fontem's own infrastructure; this mirror is
> updated automatically. **Issues and pull requests opened here are not
> monitored.**
>
> If you would like to contribute — code, data sources, review, or
> anything else — please get in touch at **team@fontem.eu** and we will
> set you up.

# fontem-neo4j-sink

Event consumer that projects events.entity_events into the Neo4j property graph. Vendored copies of fontem-events + fontem-event-schemas; runs in-cluster as a Deployment with the neo4j-sink consumer offset row in Postgres.

## Deploy

CI auto-deploys to the testing env on every merge to main. Promotion to staging / prod is **manual** — bump the version in `gitops/<env>/<service>.yaml` to land it in a given environment.

## Convention

See [/config/repos/CLAUDE.md](https://contribute.void42.internal/fontem/gitops) for workspace-wide rules (feature branches + CI gate, no direct push to main, full gate before declaring done, conventional commits).

## Repairing a wrongly assembled contract

One `:Contract` entity is one contract: an award and its modifications. A
modification says which notice it modifies with a number the buyer types, and
the number is sometimes wrong. The link step judges every back-link before it
may join two notices (`neo4j_sink/plausibility.py` — procedure, buyer,
contractor, title, country; the docstring has the measurements behind the
rule). A refused link leaves `back_link_status = 'rejected'` and a reason on
the notice; a link with nothing in common inside one country is kept but
marked `'doubtful'`.

Entities fused before that check existed, or by a legacy modification whose
only identity *is* its wrong back-link, are rebuilt from the event log:

```
kubectl -n fontem-prod exec deploy/neo4j-sink -- python -m neo4j_sink.repair_chains scan
kubectl -n fontem-prod exec deploy/neo4j-sink -- python -m neo4j_sink.repair_chains plan    --entity <contract_key>
kubectl -n fontem-prod exec deploy/neo4j-sink -- python -m neo4j_sink.repair_chains rebuild --entity <contract_key> --apply
kubectl -n fontem-prod exec deploy/neo4j-sink -- python -m neo4j_sink.repair_chains rebuild --suspects --apply
```

`scan` and `plan` read only; `rebuild` without `--apply` prints the plan and
changes nothing. A rebuild deletes the entity and replays each of its notices
(the newest whole `UpsertContract` in the log) through the sink's own write
path, so the result is what a fresh ingest would have produced — nothing is
patched by hand, and running it twice changes nothing more. It refuses an
entity if any of its notices has no whole event in the log.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
