"""Every shipped JSON Schema must have a known-good example, and
every known-good example must validate against its schema. Adding
a new event type without a paired example is a CI failure."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from gmr_event_schemas import (
    EventValidationError,
    available_event_types,
    validate,
)
from gmr_event_schemas.loader import _EVENT_TYPE_CATEGORY

_EXAMPLES = Path(__file__).parent / "examples" / "v1"


@pytest.mark.parametrize("event_type", available_event_types())
def test_every_event_has_an_example(event_type: str) -> None:
    cat = _EVENT_TYPE_CATEGORY[event_type]
    path = _EXAMPLES / cat / f"{event_type}.json"
    assert path.is_file(), f"missing example: {path.relative_to(Path.cwd())}"


@pytest.mark.parametrize("event_type", available_event_types())
def test_example_validates(event_type: str) -> None:
    cat = _EVENT_TYPE_CATEGORY[event_type]
    payload = json.loads((_EXAMPLES / cat / f"{event_type}.json").read_text())
    validate(event_type, 1, payload)  # raises on failure


def test_invalid_payload_is_rejected() -> None:
    """One spot-check that the validator fires when something is wrong."""
    bad = {"entity_id": "x"}  # missing eu_reference
    with pytest.raises(EventValidationError) as excinfo:
        validate("UpsertSanctionedEntity", 1, bad)
    assert any("eu_reference" in e for e in excinfo.value.errors)
