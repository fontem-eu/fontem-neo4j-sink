"""Texts derived from another text: summaries and translations of long fields.

SummarizePetitionObjectives, TranslateDisclosureText and SummarizeDisclosureText,
published by the translation service. Like a title translation (see
translations.py), a derived text belongs to the text it was made from, not
to the entity: it lands only while the entity still holds exactly that
text — MATCH on the key, WHERE the source property equals the event's text
— and it never creates the entity.

Each set is replaced whole: every EU language absent from the event is
written as None, which SET += removes, so a shorter set cannot leave an
older text's translations behind. The source text is recorded beside the
set (``<prop>_translated_from`` / ``<prop>_summarized_from``), so a reader
can tell a set made from a text the entity has since replaced.

Properties, for a source property ``<p>`` (``objectives`` on a petition,
``detail_<field>`` on a disclosure):

    <p>_<lang>            translations (TranslateDisclosureText)
    <p>_summary_<lang>    summaries, the source language's own included
    <p>_lang              the source text's language, with
    <p>_lang_origin       stated | detected | unknown, and
    <p>_lang_detected_by  the model that detected it
"""
from __future__ import annotations

import logging
import re

from neo4j_sink.translations import EU_LANGS
from neo4j_sink.writes import CypherWrite

logger = logging.getLogger(__name__)

LABEL = "_TextDerivation"

#: A disclosure field becomes part of property names: the schema allows
#: only plain identifiers, and the sink checks again before building Cypher.
_FIELD = re.compile(r"^[a-z][a-z_]{1,40}$")


def _per_language(prefix: str, texts: dict | None) -> dict:
    texts = texts or {}
    return {f"{prefix}_{code}": texts.get(code) or None for code in EU_LANGS}


def _source_language(prop: str, p: dict) -> dict:
    origin = p.get("source_lang_origin")
    return {f"{prop}_lang": p.get("source_lang") or None,
            f"{prop}_lang_origin": origin,
            f"{prop}_lang_detected_by": p.get("detected_by") if origin == "detected" else None}


def _write(label: str, key: dict, prop: str, text: str, props: dict) -> CypherWrite:
    return CypherWrite(label=LABEL, primary_key=key,
                       set_props={"target": label, "source_prop": prop,
                                  "text": text, "props": props})


def render_summarize_petition_objectives(p: dict) -> CypherWrite | None:
    """A :Petition's objectives summarised, keyed by (system, petition_id)."""
    if not p.get("summaries"):
        return None
    props = _per_language("objectives_summary", p["summaries"])
    props.update({"objectives_summary_lang": p.get("source_lang") or None,
                  "objectives_summarized_from": p["objectives"],
                  "objectives_summarized_at": p.get("summarized_at")})
    return _write("Petition", {"system": p["system"], "petition_id": p["petition_id"]},
                  "objectives", p["objectives"], props)


def _disclosure_field(p: dict) -> str | None:
    field = p.get("field") or ""
    if not _FIELD.match(field):
        logger.warning("disclosure text event with an unusable field %r: skipped", field)
        return None
    return f"detail_{field}"


def render_translate_disclosure_text(p: dict) -> CypherWrite | None:
    """Translations of a :Disclosure's details[field]."""
    prop = _disclosure_field(p)
    if prop is None or not p.get("translations"):
        return None
    props = _per_language(prop, p["translations"])
    props.update(_source_language(prop, p))
    props.update({f"{prop}_translated_from": p["text"],
                  f"{prop}_translated_at": p.get("translated_at")})
    return _write("Disclosure", {"system": p["system"], "disclosure_id": p["disclosure_id"]},
                  prop, p["text"], props)


def render_summarize_disclosure_text(p: dict) -> CypherWrite | None:
    """A :Disclosure's details[field] summarised."""
    prop = _disclosure_field(p)
    if prop is None or not p.get("summaries"):
        return None
    props = _per_language(f"{prop}_summary", p["summaries"])
    props.update(_source_language(prop, p))
    props.update({f"{prop}_summarized_from": p["text"],
                  f"{prop}_summarized_at": p.get("summarized_at")})
    return _write("Disclosure", {"system": p["system"], "disclosure_id": p["disclosure_id"]},
                  prop, p["text"], props)


def apply_text_derivations(driver, writes: list[CypherWrite]) -> int:
    """Apply a batch in event order; returns how many landed. The rest named
    a text the entity no longer holds, or an entity that does not exist."""
    groups: dict[tuple, list[dict]] = {}
    for w in writes:
        s = w.set_props
        key = (s["target"], tuple(sorted(w.primary_key)), s["source_prop"])
        groups.setdefault(key, []).append({**w.primary_key, "text": s["text"],
                                           "props": s["props"]})
    applied = 0
    with driver.session() as session:
        for (label, keyset, prop), rows in groups.items():
            key_match = ", ".join(f"{k}: row.{k}" for k in keyset)
            record = session.run(
                f"UNWIND $rows AS row "
                f"MATCH (n:{label} {{ {key_match} }}) WHERE n.{prop} = row.text "
                "SET n += row.props "
                "RETURN count(n) AS applied",
                rows=rows,
            ).single()
            landed = record["applied"] if record else 0
            applied += landed
            if landed < len(rows):
                logger.info("text derivations: %d of %d %s.%s rows no longer match the "
                            "entity's text", len(rows) - landed, len(rows), label, prop)
    return applied
