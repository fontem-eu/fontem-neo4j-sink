// One-off data repair for the three VALUES BLOCK failures on :Contract.
//
// REVIEW BEFORE RUNNING. Read-only counts are shown next to each block so
// you can confirm the blast radius first (numbers drift as the graph is
// live; run the matching count query and eyeball it before the mutation).
//
// These statements only clear STALE props left by the sink's partial
// updates (SET n += props never DELETEs). They mirror exactly what the
// fixed sink (fix/dq-contract-value-clearing) now converges each contract
// to on its next emit — so running this brings the already-rendered rows
// forward without waiting for a re-publish. Idempotent: re-running matches
// nothing.
//
// Run each statement on its own (cypher-shell executes one ; at a time).

// ─────────────────────────────────────────────────────────────────────
// #1  values.quarantined_carries_no_value  (~915)
//     A contract is value_quarantined=true but still carries a value.
//     Split by the node's CURRENT value_quality_flag:

// 1a — benign flag (ok / value_disagreement / …): the latest re-score is
//      healthy; the quarantine marker is stale (an earlier quarantine or
//      backfill event). Clear the marker, KEEP the value.  (~904)
//   count: MATCH (ct:Contract) WHERE ct.value_quarantined=true
//     AND (ct.value_eur IS NOT NULL OR ct.value_original IS NOT NULL)
//     AND NOT coalesce(ct.value_quality_flag,'') IN
//       ['zero_value','concession_negative','unverified_single_signal',
//        'implausible_magnitude','no_awarded_value']
//     RETURN count(*);
MATCH (ct:Contract)
WHERE ct.value_quarantined = true
  AND (ct.value_eur IS NOT NULL OR ct.value_original IS NOT NULL)
  AND NOT coalesce(ct.value_quality_flag, '') IN
      ['zero_value', 'concession_negative', 'unverified_single_signal',
       'implausible_magnitude', 'no_awarded_value']
REMOVE ct.value_quarantined, ct.value_quarantine_reason;

// 1b — no_awarded_value: there is no awarded value; the value present is a
//      prior (often sign-flipped) figure. Clear the value AND the marker.
//      (~9)
MATCH (ct:Contract)
WHERE ct.value_quarantined = true
  AND (ct.value_eur IS NOT NULL OR ct.value_original IS NOT NULL)
  AND ct.value_quality_flag = 'no_awarded_value'
REMOVE ct.value_eur, ct.value_original, ct.value_currency,
       ct.value_quarantined, ct.value_quarantine_reason;

// 1c — genuinely hard-flagged AND still quarantined: the value leaked past
//      an active quarantine. Withhold the monetary props, KEEP the
//      quarantine marker (so values.hard_flags_are_quarantined stays green).
//      (~2)
MATCH (ct:Contract)
WHERE ct.value_quarantined = true
  AND (ct.value_eur IS NOT NULL OR ct.value_original IS NOT NULL)
  AND coalesce(ct.value_quality_flag, '') IN
      ['zero_value', 'concession_negative', 'unverified_single_signal',
       'implausible_magnitude']
REMOVE ct.value_eur, ct.value_original, ct.value_currency,
       ct.estimated_value_eur, ct.value_payable_eur,
       ct.value_before_eur, ct.value_before_original;

// ─────────────────────────────────────────────────────────────────────
// #2  values.contract_bidder_count_positive  (~729)
//     tenders_received is a bidder COUNT (>= 1); a 0/negative is corrupt
//     parsing. Remove it (NULL passes the assertion).
//   count: MATCH (c:Contract) WHERE c.tenders_received IS NOT NULL
//     AND c.tenders_received < 1 RETURN count(*);
MATCH (c:Contract)
WHERE c.tenders_received IS NOT NULL AND c.tenders_received < 1
REMOVE c.tenders_received;

// ─────────────────────────────────────────────────────────────────────
// #3  values.contract_value_nonneg  (~1)
//     A negative value_eur that is NOT a flagged concession is an ingest
//     artefact (here: no_awarded_value with a stale sign-flipped value).
//     Remove the monetary props. concession_negative is excluded — a real
//     concession legitimately keeps its negative total.
//   count: MATCH (c:Contract) WHERE c.value_eur < 0
//     AND coalesce(c.value_quality_flag,'') <> 'concession_negative'
//     RETURN count(*);
MATCH (c:Contract)
WHERE c.value_eur < 0
  AND coalesce(c.value_quality_flag, '') <> 'concession_negative'
REMOVE c.value_eur, c.value_original, c.value_currency;
