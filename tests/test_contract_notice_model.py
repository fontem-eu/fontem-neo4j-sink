"""Native Contract/Notice model (the shape project_contracts built in batch).

A new-model UpsertContract (contract_key + real notice fields) renders as
one :Notice node, one :Contract entity keyed by contract_key, a NOTICE_OF
edge, and AWARDED / AWARDED_TO / BID_ON edges attached to the entity.
Old-shape events (no contract_key) and collapse_modifications rollup
partials keep the legacy notice-grain render byte-for-byte — the event log
is append-only and replayable from seq 0."""
# pylint: disable=protected-access
from neo4j_sink.cypher import render_upsert_contract
from neo4j_sink.sink import Neo4jSink
from tests.test_bracket_loss_repro import _ev, _make_sink_with_mock_driver


def _new_model_payload(**extra):
    payload = {
        "ted_notice_id": "uuid-award-1",
        "contract_key": "proc:P-77",
        "notice_kind": "award",
        "title": "Bridge works",
        "publication_date": "2026-03-01",
        "value_eur": 1000.0,
        "cpv": "45000000",
        "country": "PRT",
        "tenders_received": 3,
        "procedure_type": "open",
        "authority_id": "auth-1",
        "company_gmr_id": "co-1",
        "match_tier": "lei",
        "match_confidence": 1.0,
        "match_layer": 2,
    }
    payload.update(extra)
    return payload


def _contract_event(payload, seq=1):
    return _ev("UpsertContract", payload, seq)


# ── renderer split ────────────────────────────────────────────────


def test_new_shape_renders_notice_and_contract_entity():
    notice, contract = render_upsert_contract(_new_model_payload())
    assert notice.label == "Notice"
    assert notice.primary_key == {"ted_notice_id": "uuid-award-1"}
    assert notice.set_props["notice_kind"] == "award"
    assert notice.set_props["contract_key"] == "proc:P-77"
    assert notice.set_props["value_eur"] == 1000.0
    # per-notice provenance keeps the raw fields + red flags
    assert notice.set_props["tenders_received"] == 3

    assert contract.label == "Contract"
    assert contract.primary_key == {"contract_key": "proc:P-77"}
    assert contract.guard_prop == "canonical_publication_date"
    assert contract.set_props["canonical_publication_date"] == "2026-03-01"
    assert contract.set_props["current_value"] == 1000.0
    assert contract.set_props["value_eur"] == 1000.0
    assert contract.set_props["ted_notice_id"] == "uuid-award-1"
    assert contract.always_props == {"is_current": True,
                                     "award_value": 1000.0}


def test_notice_of_edge_targets_contract_entity_iri():
    notice, _ = render_upsert_contract(_new_model_payload())
    rels = notice.extra_relationships or []
    assert rels == [(
        "NOTICE_OF",
        "http://data.fontem.eu/id/ContractEntity/proc:P-77",
        {"_direction": "from_source"},
    )]


def test_awarded_edges_attach_to_contract_entity_not_notice():
    notice, contract = render_upsert_contract(_new_model_payload())
    notice_types = {r[0] for r in (notice.extra_relationships or [])}
    assert notice_types == {"NOTICE_OF"}
    rels = {r[0]: r for r in (contract.extra_relationships or [])}
    assert rels["AWARDED"][1].endswith("/Authority/auth-1")
    assert rels["AWARDED"][2] == {"_direction": "from_target"}
    assert rels["AWARDED_TO"][1].endswith("/Company/co-1")
    assert rels["AWARDED_TO"][2]["match_tier"] == "lei"


def test_modification_notice_kind_derived_from_notice_type():
    """Events without producer-stamped notice_kind derive it the way
    project_contracts' relabel phase did (can-modif => modification)."""
    notice, contract = render_upsert_contract(_new_model_payload(
        notice_kind=None, notice_type="can-modif", value_eur=1200.0,
    ))
    assert notice.set_props["notice_kind"] == "modification"
    # a modification must NOT stamp award_value, and notice_type stays
    # per-notice (the entity's is nulled by the model)
    assert contract.always_props == {"is_current": True}
    assert "notice_type" not in contract.set_props
    assert notice.set_props["notice_type"] == "can-modif"


def test_red_flags_land_on_contract_entity_too():
    """contract_red_flags is recomputed per notice; the flags must reach
    the display entity, not just the Notice."""
    notice, contract = render_upsert_contract(_new_model_payload(
        tenders_received=1, procedure_type="neg-wo-call",
        award_criterion_type="price",
    ))
    for w in (notice, contract):
        assert w.set_props["is_single_bidder"] is True
        assert w.set_props["integrity_red_flags"] >= 3


# ── parties[] fan-out ─────────────────────────────────────────────


def test_parties_winner_gets_awarded_to_with_props():
    _, contract = render_upsert_contract(_new_model_payload(parties=[{
        "company_gmr_id": "co-1", "name": "Acme", "role": "winner",
        "rank": 1, "is_consortium_member": False,
        "match_tier": "lei", "match_confidence": 1.0, "match_layer": 2,
    }]))
    awarded_to = [r for r in contract.extra_relationships
                  if r[0] == "AWARDED_TO"]
    # legacy company_gmr_id edge + parties winner edge: same rel type,
    # same endpoints — the sink MERGE coalesces them onto one edge.
    assert len(awarded_to) == 2
    targets = {r[1] for r in awarded_to}
    assert targets == {"http://data.fontem.eu/id/Company/co-1"}
    party_edge = awarded_to[1][2]
    assert party_edge["_direction"] == "from_source"
    assert party_edge["rank"] == 1
    assert party_edge["is_consortium_member"] is False
    assert party_edge["match_tier"] == "lei"


def test_parties_named_tenderer_gets_bid_on_company_to_contract():
    """BID_ON is a named losing bidder — Company->Contract."""
    _, contract = render_upsert_contract(_new_model_payload(parties=[{
        "company_gmr_id": "co-9", "name": "Loser Ltd",
        "role": "named_tenderer", "match_tier": "name_country",
        "match_confidence": 0.95, "match_layer": 2,
    }]))
    bid_on = [r for r in contract.extra_relationships if r[0] == "BID_ON"]
    assert len(bid_on) == 1
    _, target, props = bid_on[0]
    assert target == "http://data.fontem.eu/id/Company/co-9"
    assert props["_direction"] == "from_target"   # Company -> Contract
    assert props["match_tier"] == "name_country"


def test_consortium_members_carry_shared_party_props_no_summing():
    """Consortium members share one undivided value: the sink carries
    is_consortium_member / tendering_party_id verbatim on each edge and
    performs no value arithmetic."""
    _, contract = render_upsert_contract(_new_model_payload(
        company_gmr_id=None,
        parties=[
            {"company_gmr_id": "co-a", "name": "A", "role": "winner",
             "is_consortium_member": True, "tendering_party_id": "TPA-1"},
            {"company_gmr_id": "co-b", "name": "B", "role": "winner",
             "is_consortium_member": True, "tendering_party_id": "TPA-1"},
        ]))
    awarded_to = [r for r in contract.extra_relationships
                  if r[0] == "AWARDED_TO"]
    assert len(awarded_to) == 2
    for _, _, props in awarded_to:
        assert props["is_consortium_member"] is True
        assert props["tendering_party_id"] == "TPA-1"
    # the shared value stays on the entity, once
    assert contract.set_props["value_eur"] == 1000.0


# ── backward compat: old shapes render exactly as before ──────────


def test_old_shape_event_renders_legacy_cypher_byte_identical():
    """No contract_key => the exact node MERGE + edge Cypher the sink
    has always produced for notice-grain :Contract events."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event({
        "ted_notice_id": "2019-OJS100-1", "title": "Old",
        "publication_date": "2019-06-01", "value_eur": 5.0,
        "authority_id": "auth-1", "company_gmr_id": "co-1",
    })])
    node = [c for c in calls if "MERGE (n:" in c[0]]
    assert len(node) == 1
    assert node[0][0] == (
        "UNWIND $rows AS row "
        "MERGE (n:Contract { ted_notice_id: row.ted_notice_id }) "
        "SET n += row.props REMOVE n._stub"
    )
    assert node[0][1]["rows"][0]["props"]["value_eur"] == 5.0
    awarded_to = next(c for c in calls if "[r:AWARDED_TO]" in c[0])
    assert awarded_to[0] == (
        "UNWIND $rows AS row "
        "MATCH (s:Contract { ted_notice_id: row.ted_notice_id }) "
        "OPTIONAL MATCH (t0:Company|InvestmentFund "
        "{ gmr_id: row.tgt_key }) "
        "FOREACH (_ IN CASE WHEN t0 IS NULL THEN [1] ELSE [] END | "
        "MERGE (ts:Company { gmr_id: row.tgt_key }) "
        "SET ts._stub = true) "
        "WITH s, row "
        "MATCH (t:Company|InvestmentFund { gmr_id: row.tgt_key }) "
        "MERGE (s)-[r:AWARDED_TO]->(t) "
        "SET r += row.props"
    )


def test_rollup_partial_does_not_create_a_notice():
    """collapse_modifications rollup partials now carry contract_key but
    were all emitted against notice-grain graphs: they keep the legacy
    :Contract{ted_notice_id} render and never mint a :Notice node or a
    Contract entity."""
    w = render_upsert_contract({
        "ted_notice_id": "uuid-n1",
        "contract_key": "proc:P-1",
        "current_value": 42.0,
        "is_current": False,
    })
    assert not isinstance(w, list)
    assert w.label == "Contract"
    assert w.primary_key == {"ted_notice_id": "uuid-n1"}
    assert w.set_props["contract_key"] == "proc:P-1"
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event({
        "ted_notice_id": "uuid-n1", "contract_key": "proc:P-1",
        "current_value": 42.0, "is_current": False,
    })])
    assert not any(":Notice" in q for q, _ in calls), calls


def test_keyless_modification_renders_notice_not_contract():
    """A can-modif event that arrives without a native contract_key
    (pre-model legacy events, and value/scale-correction re-emits of an
    old payload) must render as a :Notice, never :Contract — otherwise it
    regresses onto the :Contract label and value aggregates double-count
    the restatement (grain.no_contract_is_a_modification)."""
    w = render_upsert_contract({
        "ted_notice_id": "uuid-mod-legacy",
        "title": "Modification of the bridge works",
        "publication_date": "2024-02-01",
        "value_eur": 2200.0,
        "notice_type": "can-modif",
        "authority_id": "auth-1",
        "company_gmr_id": "co-1",
    })
    assert not isinstance(w, list)
    assert w.label == "Notice"
    assert w.primary_key == {"ted_notice_id": "uuid-mod-legacy"}
    assert w.set_props["notice_kind"] == "modification"
    # aggregatable award edges must NOT ride on a notice
    assert w.extra_relationships is None
    # a bare legacy modification has no contract_key -> no NOTICE_OF, but
    # crucially it is not a :Contract
    assert "contract_key" not in w.set_props


def test_keyed_modification_native_path_never_labels_contract_modif():
    """A modification carrying a real contract_key + notice fields takes
    the native path: a :Notice keeps notice_type='can-modif', the entity
    is updated via the high-water guard, and the :Contract entity never
    carries notice_type — so it can never be a :Contract{can-modif}."""
    notice, contract = render_upsert_contract(_new_model_payload(
        ted_notice_id="uuid-mod-2", notice_kind="modification",
        notice_type="can-modif", publication_date="2026-06-01",
        value_eur=3300.0,
    ))
    assert notice.label == "Notice"
    assert notice.set_props["notice_kind"] == "modification"
    assert contract.label == "Contract"
    assert "notice_type" not in contract.set_props


def test_keyless_modification_never_hits_contract_label_at_sink():
    """End-to-end through the sink: a keyless can-modif produces a
    :Notice MERGE and no :Contract MERGE at all."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event({
        "ted_notice_id": "uuid-mod-3", "title": "Mod",
        "publication_date": "2024-04-01", "value_eur": 10.0,
        "notice_type": "can-modif", "authority_id": "auth-1",
        "company_gmr_id": "co-1",
    })])
    assert any("MERGE (n:Notice" in q for q, _ in calls), calls
    assert not any("MERGE (n:Contract" in q for q, _ in calls), calls


def test_keyless_award_still_renders_contract_notice_grain():
    """The fix is scoped to modifications: a keyless *award* keeps the
    frozen legacy notice-grain :Contract render."""
    w = render_upsert_contract({
        "ted_notice_id": "2019-OJS100-1", "title": "Old award",
        "publication_date": "2019-06-01", "value_eur": 5.0,
        "authority_id": "auth-1", "company_gmr_id": "co-1",
    })
    assert w.label == "Contract"
    assert w.primary_key == {"ted_notice_id": "2019-OJS100-1"}


def test_legacy_modifies_iri_resolution_unchanged():
    """link_ted_modifications events reference notices as
    id/Contract/<ted_notice_id>; that segment must keep resolving to
    :Contract{ted_notice_id} forever. The entity uses the dedicated
    ContractEntity segment (label :Contract, key contract_key)."""
    assert Neo4jSink._key_field("Contract") == "ted_notice_id"
    assert Neo4jSink._key_field("ContractEntity") == "contract_key"
    assert Neo4jSink._key_field("Notice") == "ted_notice_id"
    assert Neo4jSink._match_label("ContractEntity") == "Contract"


# ── sink write shapes for the new model ───────────────────────────


def test_new_model_contract_write_uses_high_water_guard():
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event(_new_model_payload())])
    contract = next(c for c in calls
                    if "MERGE (n:Contract { contract_key: row.contract_key })"
                    in c[0])
    assert contract[0] == (
        "UNWIND $rows AS row "
        "MERGE (n:Contract { contract_key: row.contract_key }) "
        "SET n += row.always "
        "FOREACH (_ IN CASE WHEN "
        "coalesce(row.props.canonical_publication_date, '') >= "
        "coalesce(n.canonical_publication_date, '') THEN [1] ELSE [] END | "
        "SET n += row.props) "
        "REMOVE n._stub"
    )
    row = contract[1]["rows"][0]
    assert row["contract_key"] == "proc:P-77"
    assert row["props"]["canonical_publication_date"] == "2026-03-01"
    assert row["always"] == {"is_current": True, "award_value": 1000.0}


def test_notice_of_edge_increments_notice_count_on_create_only():
    """notice_count is an ON CREATE side-effect of the NOTICE_OF MERGE:
    a replayed batch (offset commit is not transactional with the
    write) re-MERGEs the existing edge and must not inflate it."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event(_new_model_payload())])
    notice_of = next(c for c in calls if "[r:NOTICE_OF]" in c[0])
    assert ("MERGE (s)-[r:NOTICE_OF]->(t) "
            "ON CREATE SET t.notice_count = "
            "coalesce(t.notice_count, 0) + 1 " in notice_of[0])
    # the stub for a missing entity is minted under the real :Contract
    # label with the contract_key field
    assert ("MERGE (ts:Contract { contract_key: row.tgt_key }) "
            "SET ts._stub = true" in notice_of[0])
    # notice_count never rides in node props (it would replay-inflate)
    contract = next(c for c in calls
                    if "MERGE (n:Contract { contract_key" in c[0])
    for row in contract[1]["rows"]:
        assert "notice_count" not in row["props"]
        assert "notice_count" not in row["always"]


def test_same_batch_notices_stay_separate_guarded_rows_in_seq_order():
    """Two notices of one contract in one batch must NOT dict-collapse:
    each stays its own UNWIND row (in seq order) so the in-Cypher guard
    decides which one's display fields stick — an award followed by a
    later-published modification converges on the modification, and a
    late-delivered older notice cannot regress current_value."""
    sink, calls = _make_sink_with_mock_driver()
    award = _new_model_payload()
    mod = _new_model_payload(
        ted_notice_id="uuid-mod-1", notice_kind="modification",
        publication_date="2026-05-01", value_eur=1500.0,
        notice_type="can-modif",
    )
    # deliver out of pub-date order: the guard, not arrival order,
    # must decide (the earlier award may not regress the restated value)
    sink.handle([_contract_event(mod, seq=1), _contract_event(award, seq=2)])
    contract = next(c for c in calls
                    if "MERGE (n:Contract { contract_key" in c[0])
    rows = contract[1]["rows"]
    assert len(rows) == 2
    assert rows[0]["props"]["current_value"] == 1500.0
    assert rows[0]["props"]["canonical_publication_date"] == "2026-05-01"
    assert rows[1]["props"]["canonical_publication_date"] == "2026-03-01"
    # both Notice nodes still merge independently
    notice = next(c for c in calls if "MERGE (n:Notice" in c[0])
    assert len(notice[1]["rows"]) == 2


def test_replay_of_identical_batch_produces_identical_writes():
    """Delivering the same batch twice yields exactly the same Cypher +
    rows: MERGE-idempotent nodes/edges, guard re-passes with equal
    dates (>=) restating identical props, notice_count guarded by the
    edge ON CREATE. Converged state, no drift."""
    sink1, calls1 = _make_sink_with_mock_driver()
    sink2, calls2 = _make_sink_with_mock_driver()
    batch = [_contract_event(_new_model_payload())]
    sink1.handle(batch)
    sink2.handle(batch)
    sink2.handle(batch)          # the replay
    assert calls2[:len(calls1)] == calls1
    assert calls2[len(calls1):] == calls1


def test_quarantined_new_model_notice_clears_entity_values_guarded():
    """A quarantined canonical notice strips the entity's monetary
    fields via guarded nulls (SET += removes null-valued keys) — a
    stale quarantined notice behind the high-water mark cannot."""
    notice, contract = render_upsert_contract(_new_model_payload(
        value_eur=1.8e14, estimated_value_eur=2.0e14,
        value_quarantined=True,
        value_quarantine_reason="implausible_magnitude",
    ))
    assert "value_eur" in (notice.clear_props or [])
    assert contract.clear_props is None          # folded into guarded nulls
    assert contract.set_props["value_eur"] is None
    assert contract.set_props["current_value"] is None
    assert contract.set_props["estimated_value_eur"] is None
    assert "award_value" not in contract.always_props


def test_healthy_reemit_clears_stale_marker_on_entity_and_notice():
    """A re-scored healthy canonical notice strips the stale
    value_quarantined / reason on BOTH the :Notice (clear_props REMOVE)
    and the :Contract entity (guarded null) while keeping the value —
    the fix for values.quarantined_carries_no_value on a contract whose
    latest emit is healthy but was quarantined by an earlier backfill."""
    notice, contract = render_upsert_contract(_new_model_payload(
        value_eur=367977.2, value_quality_flag="ok",
    ))
    assert notice.set_props["value_eur"] == 367977.2
    assert "value_quarantined" in (notice.clear_props or [])
    assert "value_quarantine_reason" in (notice.clear_props or [])
    # entity clears via guarded nulls (SET += removes null-valued keys)
    assert contract.clear_props is None
    assert contract.set_props["value_quarantined"] is None
    assert contract.set_props["value_quarantine_reason"] is None
    assert contract.set_props["value_eur"] == 367977.2   # value survives
    assert contract.set_props["current_value"] == 367977.2


def test_no_awarded_value_clears_negative_on_entity_guarded():
    """no_awarded_value on the canonical notice must clear a prior
    (sign-flipped, negative) award value off the entity via guarded
    nulls — values.contract_value_nonneg."""
    notice, contract = render_upsert_contract(_new_model_payload(
        value_eur=None, value_original=None,
        value_quality_flag="no_awarded_value",
    ))
    assert "value_eur" in (notice.clear_props or [])
    assert contract.clear_props is None
    assert contract.set_props["value_eur"] is None
    assert contract.set_props["current_value"] is None


def test_nonpositive_tenders_cleared_on_entity_and_notice():
    """A 0 bidder count is dropped from both writes and REMOVEd from the
    node — values.contract_bidder_count_positive."""
    notice, contract = render_upsert_contract(_new_model_payload(
        tenders_received=0, value_quality_flag="ok",
    ))
    assert "tenders_received" not in notice.set_props
    assert "tenders_received" in (notice.clear_props or [])
    # entity clears via a guarded null (present as None; SET += removes it)
    assert contract.set_props["tenders_received"] is None


# ── stub lifecycle ────────────────────────────────────────────────


def test_unwind_merge_clears_stub_on_real_arrival():
    """A Contract minted as a {_stub: true} placeholder by a NOTICE_OF
    edge must lose the flag when its real write arrives — on both the
    guarded (Contract entity) and generic (legacy/Notice) UNWIND paths,
    which historically never cleared it."""
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event(_new_model_payload())])
    node_calls = [q for q, _ in calls if "MERGE (n:" in q]
    assert node_calls and all("REMOVE n._stub" in q for q in node_calls)


def test_explicit_null_current_value_falls_back_to_value_eur():
    """Producers may serialise current_value: null on a full emit; the
    entity's collapsed figure then falls back to the notice value."""
    _, contract = render_upsert_contract(_new_model_payload(
        current_value=None,
    ))
    assert contract.set_props["current_value"] == 1000.0
    assert contract.set_props["value_eur"] == 1000.0
