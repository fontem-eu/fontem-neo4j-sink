"""framework_id on the plain :Contract — the property the UI reads.

framework_id is eForms OPT-100
(efac:NoticeResult/efac:SettledContract/cac:NoticeDocumentReference/cbc:ID),
which the framework-establishing award notice and every call-off under
it carry identically (notices 761784-2024 and 3406-2025 both carry
'536632-2024'). It is a GROUPING key, not a pointer to a contract we
hold: ~80% of the time it names the call for competition, which this
platform does not ingest.

So the reader-facing query is "every other :Contract carrying this
framework_id" — a property lookup, not an edge traversal. This module
pins the two things that query needs: the property (and its
provenance, framework_id_source) actually reaching the :Contract
entity, and the index that keeps the lookup off a 3.6M-node label
scan. The :FrameworkAgreement node and its edges are
test_framework_agreement.py.
"""
# pylint: disable=protected-access
import importlib.util
import pathlib

from neo4j_sink.cleaning import CONTRACT_FRAMEWORK_FIELDS
from neo4j_sink.cypher import (_NOTICE_FIELDS, _NOTICE_ONLY_FIELDS,
                               render_upsert_contract)
from tests.test_bracket_loss_repro import _make_sink_with_mock_driver
from tests.test_contract_notice_model import (_contract_event,
                                              _new_model_payload)

_FW_ID = "536632-2024"


# ── the property ──────────────────────────────────────────────────


def test_framework_id_and_its_source_land_on_both_labels():
    """The notice keeps what it published; the entity is what the UI
    reads, so both carry the key and where it was read from."""
    notice, contract, _ = render_upsert_contract(_new_model_payload(
        framework_id=_FW_ID, framework_id_source="OPT-100"))
    for w in (notice, contract):
        assert w.set_props["framework_id"] == _FW_ID
        assert w.set_props["framework_id_source"] == "OPT-100"


def test_every_framework_field_reaches_the_entity():
    """The group is declared in cleaning.py and spliced into the
    per-notice whitelist; an edit there must not quietly drop a field
    out of the graph, and none of them may become notice-only — the
    entity is what the UI reads."""
    assert set(CONTRACT_FRAMEWORK_FIELDS) <= set(_NOTICE_FIELDS)
    assert not set(CONTRACT_FRAMEWORK_FIELDS) & _NOTICE_ONLY_FIELDS


def test_framework_id_source_is_not_notice_only():
    """It has to be denormalised onto the entity with the id it
    explains: the entity's framework_id is written under the
    canonical_publication_date high-water guard, so a source left on
    the notice alone would let the entity show one notice's id next to
    another notice's source."""
    assert "framework_id_source" in _NOTICE_FIELDS
    assert "framework_id_source" not in _NOTICE_ONLY_FIELDS
    _, contract, _ = render_upsert_contract(_new_model_payload(
        framework_id=_FW_ID, framework_id_source="BT-125"))
    assert contract.set_props["framework_id_source"] == "BT-125"
    assert contract.guard_prop == "canonical_publication_date"


def test_framework_terms_ride_with_the_key_on_the_entity():
    """The ceiling and its companions are capacity, never spend — they
    are displayed, so they belong on the entity next to the key that
    groups it."""
    terms = {
        "framework_max_value_eur": 5_000_000.0,
        "framework_reestimated_value_eur": 3_200_000.0,
        "framework_duration_months": 48,
        "framework_max_operators": 3,
    }
    _, contract, _ = render_upsert_contract(_new_model_payload(
        framework_id=_FW_ID, framework_id_source="OPT-100", **terms))
    for k, v in terms.items():
        assert contract.set_props[k] == v


def test_absent_source_is_omitted_never_cleared():
    """Pre-2024 notices carry no framework reference at all, and a
    contract with no reference is not a contract with no framework.
    SET += leaves absent keys alone; nothing writes an empty marker."""
    _, contract, _ = render_upsert_contract(_new_model_payload(
        framework_id=_FW_ID))
    assert "framework_id_source" not in contract.set_props
    assert "framework_id_source" not in (contract.clear_props or [])


def test_legacy_notice_grain_render_still_ignores_the_source():
    """The keyless shape is frozen for byte-identical replay from seq 0
    — the same freeze test_legacy_notice_grain_render_ignores_cleaning_fields
    pins for the other cleaning fields."""
    w = render_upsert_contract({
        "ted_notice_id": "2025-OJS111-000002", "title": "T",
        "framework_id": _FW_ID, "framework_id_source": "OPT-100",
    })
    assert w.label == "Contract"
    assert set(w.set_props) == {"title"}


def test_sink_writes_the_key_and_its_source_onto_the_contract_entity():
    sink, calls = _make_sink_with_mock_driver()
    sink.handle([_contract_event(_new_model_payload(
        framework_id=_FW_ID, framework_id_source="OPT-100"))])
    entity = next(c for c in calls
                  if "MERGE (n:Contract { contract_key: row.contract_key })"
                  in c[0])
    props = entity[1]["rows"][0]["props"]
    assert props["framework_id"] == _FW_ID
    assert props["framework_id_source"] == "OPT-100"


# ── schema ────────────────────────────────────────────────────────


def _load_migration():
    path = (pathlib.Path(__file__).resolve().parents[1] / "migrations"
            / "contract_framework_id_index_2026_09.py")
    spec = importlib.util.spec_from_file_location("fw_id_index", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_migration_declares_a_plain_range_index_on_framework_id():
    """Measured on prod 2026-09-23 (3,606,471 :Contract): the sibling
    read is a NodeByLabelScan at 7,212,943 db hits and 8.3 s without
    it, past the API's 8 s cap. A RANGE index, never a uniqueness
    constraint — the value is shared by every award of one framework
    (55 of 176 sampled frameworks have two or more). IF NOT EXISTS
    makes the script re-runnable."""
    module = _load_migration()
    runs: list[str] = []

    class _Session:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def run(self, stmt, *_args, **_kwargs):
            runs.append(stmt)

    class _Driver:
        def session(self):
            return _Session()

    module.migrate(_Driver())
    assert runs == [
        "CREATE INDEX contract_framework_id IF NOT EXISTS "
        "FOR (c:Contract) ON (c.framework_id)",
    ]
