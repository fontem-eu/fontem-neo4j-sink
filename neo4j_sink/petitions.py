"""Petitions: UpsertPetition onto a :Petition keyed by (system, petition_id).

The EU Citizens' Initiative register publishes every initiative in up to
24 official languages; each version's texts land on the node as
<field>_<lang>, beside the English top-level ones and title_lang, the
language the initiative was registered in. Summaries of the objectives
arrive separately (see text_derivations).
"""
from __future__ import annotations

from neo4j_sink.translations import EU_LANGS
from neo4j_sink.writes import CypherWrite

#: UpsertPetition keys stored as they come: scalars and arrays of scalars.
_PETITION_PROPS = (
    "title", "title_lang", "status", "objectives", "annex_text", "treaties", "website",
    "categories", "register_id", "register_url",
    "registration_date", "collection_start_date", "collection_deadline", "ongoing_date",
    "closed_date", "verification_date", "submitted_date", "answered_date",
    "withdrawn_date", "rejected_date", "insufficient_support_date",
    "insufficient_support_after_verification_date", "early_closure_date",
    "partially_registered",
    "total_supporters", "online_supporters", "supporters_updated_at",
    "supporter_countries", "supporter_counts",
    "verified_supporters", "verified_countries", "verified_counts", "verified_after_submission",
    "support_link", "organizer_names", "organizer_roles", "organizer_countries",
    "representative_country",
    "funding_total_eur", "funding_sponsor_count", "funding_updated_at", "funding_document_name",
    "sponsor_names", "sponsor_amounts_eur", "sponsor_dates", "sponsor_private",
    "sponsor_anonymized", "sponsor_other_support",
    "registration_decision_celex", "registration_decision_url",
    "registration_decision_corrigendum",
    "annex_document_name", "annex_document_id", "draft_legal_act_name", "draft_legal_act_id",
    "answer_refs", "answer_communication_url", "answer_annex_url",
    "answer_press_release_url", "answer_follow_up_url",
    "latest_update",
)

#: The texts of each official language version kept on the node as
#: <field>_<lang>; the rest of a version stays in the event log.
_PETITION_VERSION_TEXTS = ("title", "objectives", "annex_text")


def render_upsert_petition(p: dict) -> CypherWrite:
    """Public petition keyed by (system, petition_id) — e.g. the EU
    Citizens' Initiative register. Organizer names, per-country counts and
    sponsors arrive as parallel arrays; emails never reach the platform.

    Every official language version lands as title_<lang>, objectives_<lang>
    and annex_text_<lang>. When the event carries versions, the set is
    replaced whole: a language the register no longer publishes is removed."""
    set_props = {k: p[k] for k in _PETITION_PROPS if p.get(k) is not None}
    if (versions := p.get("versions")) is not None:
        for field in _PETITION_VERSION_TEXTS:
            for code in EU_LANGS:
                set_props[f"{field}_{code}"] = (versions.get(code) or {}).get(field) or None
    return CypherWrite(
        label="Petition",
        primary_key={"system": p["system"], "petition_id": p["petition_id"]},
        set_props=set_props,
    )
