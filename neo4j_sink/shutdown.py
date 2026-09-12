"""Stop cleanly when Kubernetes asks.

The container's ENTRYPOINT is exec-form ``python -m neo4j_sink``, so the
interpreter is PID 1 — and Linux does not apply default signal actions to
PID 1. A signal with no handler *registered* is ignored outright, not
fatal. Without the registration below, SIGTERM does nothing at all and
the process only dies when the grace period expires and the kernel sends
SIGKILL.

That is not cosmetic. kured drains a node with
``--drain-grace-period=1800`` and abandons the whole drain after
``--drain-timeout=35m`` with ``--force-reboot=false``. A container that
ignores SIGTERM burns 30 of those 35 minutes on its own; a drain that
times out leaves the node cordoned, and prod's Postgres sits on a
node-local PV. This cost fontem-web-ssr a 30-minute stall on
2026-09-12 before the same fix landed there.

Raising SystemExit is what stops the consumer, because the polling loop
lives in the vendored ``fontem_events`` wheel and takes no stop flag. It
unwinds cleanly rather than being swallowed: ``run_forever`` guards its
body with ``except Exception``, and SystemExit derives from
BaseException, so it passes straight through.

Where the signal lands decides how much work repeats, and both outcomes
are safe:

* Between batches — the overwhelmingly common case, since the sink idles
  in ``time.sleep(poll_interval)`` most of the time. The offset for the
  last batch is already committed, so nothing repeats.
* Mid-batch — the offset is committed only *after* a batch succeeds, so
  that batch is simply re-read on the next start. This is the same
  at-least-once replay the loop already performs whenever an iteration
  raises, and the Cypher writes are MERGE-based and idempotent.
"""
import logging
import signal

logger = logging.getLogger(__name__)

STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def install_stop_handlers(register=signal.signal) -> None:
    """Register the stop handlers. ``register`` is injectable for tests."""

    def _stop(signum, _frame):
        logger.info("neo4j_sink: signal %s received, stopping", signum)
        raise SystemExit(0)

    for sig in STOP_SIGNALS:
        register(sig, _stop)
