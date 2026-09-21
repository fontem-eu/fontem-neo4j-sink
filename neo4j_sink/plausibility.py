"""Is a modification's back-link believable?

A modification notice names the notice it modifies (BT-1501, or the OJ
reference on a legacy F20). The number is typed by the buyer, and it is
sometimes wrong: 81781-2024, a Bulgarian lift-maintenance contract,
declares that it modifies 037303-2022 — a DB Netz rail-construction
notice. The chain step used to trust every back-link, so that one typo
folded 568 German notices into a Bulgarian university's contract.

A modification is the SAME contract restated, so something has to carry
over: the procedure, the buyer, the contractor, or at least what the
contract is about. Measured on 20,000 MODIFIES edges in prod
(2026-09-21, against the event log):

    same buyer id                       83.9 %
    else same procedure id               5.8 %
    else shares a contractor id          5.6 %
    else contractor names match          2.7 %
    else titles match                    1.3 %
    nothing carries over, same country   0.5 %
    nothing carries over, other country  0.25 %

The identifiers are noisy (one authority under two ids, a consortium
renamed), which is why no single field can be the test — but 99.2 % of
links show at least one. Of the rest, the same-country ones are mostly
real contracts our matching failed to recognise (Stuttgart 21 lots
under a consortium's name), and the cross-country ones are, every one
inspected, a wrong number. So:

    REJECT    nothing carries over AND the two notices are in
              different countries. No MODIFIES edge; the notice keeps
              its own entity and says why.
    DOUBTFUL  nothing carries over, same (or unknown) country. Linked
              as before, but marked, so the data-quality meter can
              count them and a person can look.
    OK        anything carries over, or there is too little on either
              side to judge (a stub, a notice from before the parties
              were stamped).

Precision first: a rejected real link splits one contract in two and
under-reports its value, which is the very failure the chain exists to
prevent.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from difflib import SequenceMatcher

OK = "ok"
DOUBTFUL = "doubtful"
REJECT = "reject"

_NAME_MATCH = 0.8
_TITLE_MATCH = 0.6
_WORD = re.compile(r"[^\W_]+", re.UNICODE)


@dataclass(frozen=True)
class Verdict:
    """The outcome for one back-link and what it rests on."""
    status: str
    signals: tuple[str, ...] = ()
    reason: str | None = None

    @property
    def linkable(self) -> bool:
        return self.status != REJECT


def _words(text: str | None) -> list[str]:
    return _WORD.findall((text or "").lower())


def _similar(a: str | None, b: str | None) -> float:
    """0..1. The better of an edit ratio and word containment: a
    modification's title is often the award's with a reference number
    in front ("21O50323 TUD Sanierung Beyer-Bau"), which an edit ratio
    alone scores low."""
    wa, wb = _words(a), _words(b)
    if not wa or not wb:
        return 0.0
    ratio = SequenceMatcher(None, " ".join(wa), " ".join(wb)).ratio()
    small, large = sorted((set(wa), set(wb)), key=len)
    # Containment only counts when there is something to contain: two
    # titles sharing the single word "works" are not the same contract.
    contained = len(small & large) / len(small) if len(small) >= 3 else 0.0
    return max(ratio, contained)


def _any_name_matches(names_a, names_b) -> bool:
    return any(_similar(a, b) >= _NAME_MATCH
               for a in names_a or () for b in names_b or ())


def assess_link(notice: dict, target: dict) -> Verdict:
    """Judge ``notice`` -[:MODIFIES]-> ``target``.

    Both sides are plain dicts with any of: ``procedure_id``,
    ``legacy_procedure_id``, ``buyer_ids``, ``winner_ids``,
    ``winner_names``, ``title``, ``country``. Missing or empty means
    unknown, never "different"."""
    signals = []
    for key in ("procedure_id", "legacy_procedure_id"):
        if notice.get(key) and notice.get(key) == target.get(key):
            signals.append("procedure")
            break
    if set(notice.get("buyer_ids") or ()) & set(target.get("buyer_ids") or ()):
        signals.append("buyer")
    if set(notice.get("winner_ids") or ()) & set(target.get("winner_ids") or ()):
        signals.append("winner")
    elif _any_name_matches(notice.get("winner_names"), target.get("winner_names")):
        signals.append("winner_name")
    if _similar(notice.get("title"), target.get("title")) >= _TITLE_MATCH:
        signals.append("title")
    if signals:
        return Verdict(OK, tuple(signals))

    judgeable = any(
        notice.get(k) and target.get(k)
        for k in ("buyer_ids", "winner_ids", "winner_names", "title")
    )
    if not judgeable:
        return Verdict(OK, reason="too little on one side to judge")
    ca, cb = notice.get("country"), target.get("country")
    if ca and cb and ca != cb:
        return Verdict(
            REJECT,
            reason=f"no buyer, contractor, procedure or title in common, "
                   f"and {ca} is not {cb}",
        )
    return Verdict(DOUBTFUL,
                   reason="no buyer, contractor, procedure or title in common")
