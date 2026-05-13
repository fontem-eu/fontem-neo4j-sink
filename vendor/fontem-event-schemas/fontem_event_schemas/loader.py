"""Locate + parse JSON Schemas at package import time.

The schemas ship inside the package (``importlib.resources``) so a
deployed sink that has only ``pip install gmr-event-schemas`` can
still validate without checking out the source repo.
"""
from __future__ import annotations

import json
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Iterator

# When installed via pip, schema files live under a ``schemas/``
# directory inside the package. When run from a source checkout
# they live one level up at the repo root. We support both.
SCHEMA_ROOT = "schemas"


def _candidate_roots() -> Iterator[Path]:
    pkg_root = Path(__file__).resolve().parent
    yield pkg_root / SCHEMA_ROOT  # installed shape
    yield pkg_root.parent          # source-checkout shape (v1/ at repo root)


@lru_cache(maxsize=None)
def _resolve(version: int, category: str, event_type: str) -> Path:
    rel = Path(f"v{version}") / category / f"{event_type}.json"
    for root in _candidate_roots():
        cand = root / rel
        if cand.is_file():
            return cand
    raise FileNotFoundError(
        f"schema not found: v{version}/{category}/{event_type}.json"
    )


# Map event_type -> (category, json_filename). Update when adding
# new types so producers can locate them by event_type alone.
_EVENT_TYPE_CATEGORY: dict[str, str] = {
    # control
    "BeginGraphReplace": "control",
    "EndGraphReplace":   "control",
    # entities
    "UpsertCompany":          "entities",
    "UpsertListing":          "entities",
    "UpsertSanctionedEntity": "entities",
    "UpsertFiling":           "entities",
    "UpsertAuthority":        "entities",
    "UpsertContract":         "entities",
    "UpsertTaxonomyCode":     "entities",
    "UpsertRelationship":     "entities",
    "UpsertDisclosure":       "entities",
    "UpsertExchangeRate":     "entities",
    # consolidation
    "AssertSameAs": "consolidation",
}


def load_schema(event_type: str, version: int = 1) -> dict:
    """Return the parsed JSON Schema for an event type.

    Cached per (event_type, version); safe to call hot.
    """
    category = _EVENT_TYPE_CATEGORY.get(event_type)
    if category is None:
        raise FileNotFoundError(
            f"unknown event_type: {event_type!r} "
            "(register it in fontem_event_schemas.loader._EVENT_TYPE_CATEGORY)"
        )
    return _read_cached(version, category, event_type)


@lru_cache(maxsize=None)
def _read_cached(version: int, category: str, event_type: str) -> dict:
    with _resolve(version, category, event_type).open() as fh:
        return json.load(fh)


def available_event_types() -> list[str]:
    """Names of every registered event type. Stable order."""
    return sorted(_EVENT_TYPE_CATEGORY)
