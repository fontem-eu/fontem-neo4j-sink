"""Strict JSON Schema validation for events.

Sinks call ``validate(event_type, version, payload)`` on every row
before applying it. Producers call it before emit. Both raise the
same exception so error reporting + DLQ handling has one shape.
"""
from __future__ import annotations

import jsonschema

from .loader import load_schema


class EventValidationError(ValueError):
    """Raised when a payload fails schema validation."""

    def __init__(self, event_type: str, version: int, errors: list[str]) -> None:
        super().__init__(
            f"{event_type} v{version} validation failed:\n  "
            + "\n  ".join(errors)
        )
        self.event_type = event_type
        self.version = version
        self.errors = errors


def validate(event_type: str, version: int, payload: dict) -> None:
    """Validate ``payload`` against the schema for ``event_type``/``version``.

    Raises ``EventValidationError`` on any conformance issue; the
    error carries the full list of problems so the DLQ row can
    record exactly what went wrong.
    """
    schema = load_schema(event_type, version)
    validator = jsonschema.Draft202012Validator(schema)
    errs = sorted(validator.iter_errors(payload), key=lambda e: e.path)
    if not errs:
        return
    raise EventValidationError(
        event_type, version,
        [f"{'/'.join(map(str, e.absolute_path))}: {e.message}" for e in errs],
    )
