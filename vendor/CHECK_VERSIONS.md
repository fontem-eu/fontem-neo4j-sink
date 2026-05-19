# Vendored wheel pinning

This sink pins `fontem-events` and `fontem-event-schemas` to the exact
versions whose wheels live in this directory. The pin lives in two
places:

- `pyproject.toml` — `fontem-events==X.Y.Z`
- `requirements.txt` — `fontem-events==X.Y.Z`

The Docker build installs from `vendor/*.whl`, so a mismatch between
the wheel filename and the pin causes a hard failure at image-build
time (pip refuses to satisfy the pin from a wheel with a different
version). That's deliberate: it means the version a sink runs against
is whatever wheel sits next to it, not whatever happens to be on PyPI
or a sibling clone at build time.

## Bumping fontem-events

1. In the `fontem-events` repo, tag the release and build the wheel:
   ```
   python3 -m build --wheel --outdir dist/
   ```
2. Copy `dist/fontem_events-<new>-py3-none-any.whl` into this directory.
3. Delete the old `fontem_events-<old>-py3-none-any.whl`.
4. Bump the pin in `pyproject.toml` and `requirements.txt`.
5. Run `pytest` against the new wheel locally before opening a PR.
6. Coordinate the deploy with `fontem-virtuoso-sink`, `fontem-consolidator`,
   and `fontem-api` — they all read/write the same `events.entity_events`
   table and must agree on the schema.
