"""A Neo4j outage must never be mistaken for a poison event.

EventConsumer skips an event that fails ``max_attempts`` times in a
row. Right for bad data, catastrophic for an outage: the event log is
the only copy and nothing re-emits a skipped event, so a Neo4j restart
would punch a permanent hole in the graph. That is not hypothetical —
virtuoso_sink lost 4,006 events to "Connection refused" during the
2026-09-06 shared replay, before this hook existed.
"""
# pylint: disable=protected-access,import-outside-toplevel
from unittest.mock import MagicMock, patch

import pytest
from neo4j import exceptions as neo4j_exceptions


@pytest.fixture(name="sink")
def _sink(monkeypatch):
    monkeypatch.setenv("NEO4J_URI", "bolt://neo4j.test:7687")
    monkeypatch.setenv("NEO4J_PASSWORD", "secret")
    from neo4j_sink.sink import Neo4jSink

    with patch("neo4j_sink.sink.EventConsumer.__init__", lambda self, *a, **k: None), \
         patch("neo4j_sink.sink.GraphDatabase.driver", return_value=MagicMock()):
        return Neo4jSink()


@pytest.mark.parametrize("exc", [
    neo4j_exceptions.ServiceUnavailable("cannot reach any server"),
    neo4j_exceptions.SessionExpired("server no longer accepts writes"),
    neo4j_exceptions.TransientError("deadlock detected"),
])
def test_server_failures_are_retryable(sink, exc):
    """The same event succeeds once Neo4j is healthy, so the offset
    must hold rather than advance past it."""
    assert sink.is_retryable(exc) is True


def test_cypher_errors_stay_poison(sink):
    """A ClientError is Neo4j rejecting THIS write — bad Cypher or a
    constraint violation. Retrying it forever recreates the 66-hour
    jam the poison-skip was built to end."""
    assert sink.is_retryable(
        neo4j_exceptions.ClientError("invalid syntax")) is False
    assert sink.is_retryable(KeyError("gmr_id")) is False
    assert sink.is_retryable(ValueError("bad payload")) is False
