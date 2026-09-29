"""Title translations: TranslateContractTitle and TranslateDisclosureTitle.

Published by the translation service (fontem-translator), which took the
job over from the consolidator, whose rules used to write title_<lang>
straight into the graph: absent from the event log, lost on a rebuild.

A translation belongs to a title, not to an entity. It is applied only
while the entity still shows exactly the title that was translated:
MATCH on the key, WHERE n.title = the event's title. A contract shows its
latest notice's title, so a translation of an older notice, or one that
arrives after the contract was retitled, changes nothing. It never
creates the entity (MATCH, not MERGE).

The translation set is replaced whole: every EU language absent from the
event is removed, so a retitle from French to English cannot leave the
old title's English translation behind. A detected source language is
recorded as title_lang_detected (with the model that detected it), and
never as title_lang, which only a source states.
"""
from __future__ import annotations

import logging

from neo4j_sink.writes import CypherWrite

logger = logging.getLogger(__name__)

LABEL = "_TitleTranslation"

#: The 24 EU official languages a title can be translated into.
EU_LANGS: tuple[str, ...] = (
    "bg", "cs", "da", "de", "el", "en", "es", "et", "fi", "fr", "ga", "hr",
    "hu", "it", "lt", "lv", "mt", "nl", "pl", "pt", "ro", "sk", "sl", "sv",
)

#: The node a translation targets, by the shape of its key.
_TARGETS: dict[tuple[str, ...], str] = {
    ("contract_key",): "Contract",
    ("disclosure_id", "system"): "Disclosure",
}


def _props(p: dict) -> dict:
    translations = p.get("translations") or {}
    props: dict = {f"title_{code}": translations.get(code) or None for code in EU_LANGS}
    detected = p.get("source_lang_origin") == "detected"
    props.update({
        "title_translated_from": p["title"],
        "multilingual_updated_at": p.get("translated_at"),
        "title_lang_detected": p.get("source_lang") if detected else None,
        "title_lang_detected_by": p.get("detected_by") if detected else None,
        "title_lang_detected_at": p.get("translated_at") if detected else None,
    })
    return props


def _write(key: dict, p: dict) -> CypherWrite | None:
    if not p.get("translations"):
        return None
    return CypherWrite(label=LABEL, primary_key=key,
                       set_props={"title": p["title"], "props": _props(p)})


def render_translate_contract_title(p: dict) -> CypherWrite | None:
    """Translations of a :Contract's title, keyed by contract_key."""
    return _write({"contract_key": p["contract_key"]}, p)


def render_translate_disclosure_title(p: dict) -> CypherWrite | None:
    """Translations of a :Disclosure's title, keyed by (system, disclosure_id)."""
    return _write({"system": p["system"], "disclosure_id": p["disclosure_id"]}, p)


def apply_title_translations(driver, writes: list[CypherWrite]) -> int:
    """Apply a batch of translations in event order; returns how many
    landed. The rest named a title the entity no longer shows."""
    by_target: dict[tuple[str, ...], list[dict]] = {}
    for w in writes:
        keyset = tuple(sorted(w.primary_key))
        row = {**w.primary_key, "title": w.set_props["title"], "props": w.set_props["props"]}
        by_target.setdefault(keyset, []).append(row)
    applied = 0
    with driver.session() as session:
        for keyset, rows in by_target.items():
            label = _TARGETS[keyset]
            key_match = ", ".join(f"{k}: row.{k}" for k in keyset)
            record = session.run(
                f"UNWIND $rows AS row "
                f"MATCH (n:{label} {{ {key_match} }}) WHERE n.title = row.title "
                "SET n += row.props "
                "RETURN count(n) AS applied",
                rows=rows,
            ).single()
            landed = record["applied"] if record else 0
            applied += landed
            if landed < len(rows):
                logger.info("title translations: %d of %d %s rows no longer match "
                            "the entity's title", len(rows) - landed, len(rows), label)
    return applied
