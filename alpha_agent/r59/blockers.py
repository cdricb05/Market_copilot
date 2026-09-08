"""alpha_agent.r59.blockers - THE canonical research-blocker taxonomy (R61).

WHY THIS EXISTS
---------------
A blocked research job recorded a FREE-TEXT ``blocked_reason``. Seventeen jobs
sat blocked across five asset classes with sentences like ``generator produced
no non-degenerate candidate for RATES_FUTURES/SYMBOLIC`` and ``engine returned
NO_MEMBERS``, and no surface could answer the only question an operator has:
*is this waiting for the market, waiting for data we do not own, or finished?*
Each of those has a completely different next action, and a sentence cannot be
grouped, counted or waited on.

This module is the ONE place that turns what an owner actually recorded into a
canonical reason code. It is asset-agnostic by construction: nothing here reads
a ticker, a symbol or an equity concept, and the taxonomy applies unchanged to
equities, index / rates / commodity / FX futures, volatility and cross-asset
work. The asset class travels as DATA, never as a branch.

WHAT IT IS NOT
--------------
It is not a second blocker vocabulary layered over an existing one: before R61
the estate had no blocker vocabulary at all, only prose. It decides nothing
about scheduling, promotes nothing, and writes to no store. Classification is
PURE: reason text plus the job's own payload in, one canonical code out.

An unrecognised reason classifies as :data:`UNCLASSIFIED_BLOCKER` and keeps its
original sentence. That is deliberate — silently folding an unknown blocker into
a known bucket is how a real external outage comes to be reported as an
exhausted search space.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.r59.blockers"

# --------------------------------------------------------------------------- #
# THE canonical reasons. Each names a DIFFERENT next action.
# --------------------------------------------------------------------------- #
#: The market itself has to move: the scope's next observation does not exist
#: yet because its session has not happened.
WAITING_FOR_MARKET_SESSION = "WAITING_FOR_MARKET_SESSION"
#: A prospective challenger is accruing; only elapsed sessions can advance it.
WAITING_FOR_FORWARD_EVIDENCE = "WAITING_FOR_FORWARD_EVIDENCE"
#: A provider we ALREADY have has not delivered (or does not deliver) the field.
WAITING_FOR_PROVIDER_DATA = "WAITING_FOR_PROVIDER_DATA"
#: Enough rows exist to run, but not enough INDEPENDENT ones to conclude.
WAITING_FOR_SAMPLE = "WAITING_FOR_SAMPLE"
#: The data exists and is identified, but the estate is not entitled to it. No
#: amount of waiting or computing resolves this; a purchase gate decides it.
WAITING_FOR_EXTERNAL_ENTITLEMENT = "WAITING_FOR_EXTERNAL_ENTITLEMENT"
#: The search space for this family is generatively exhausted at the current
#: budget: further draws return what the estate has already tested.
FAMILY_EXHAUSTED = "FAMILY_EXHAUSTED"
#: A compute or capacity ceiling refused the work, not the evidence.
COMPUTE_GATE = "COMPUTE_GATE"
#: A prerequisite artifact, universe or upstream job is missing, so this job
#: cannot be attempted at all (an empty membership set is the common case).
DEPENDENCY_BLOCKED = "DEPENDENCY_BLOCKED"
#: The premise was withdrawn: the input this work rested on is not what its
#: name claims, so the work is not merely unfinished, it is void.
INVALIDATED = "INVALIDATED"
#: A later artifact answers the same question; this one is no longer the ask.
SUPERSEDED = "SUPERSEDED"
#: Recorded, unrecognised, and NEVER folded into a neighbour.
UNCLASSIFIED_BLOCKER = "UNCLASSIFIED_BLOCKER"

BLOCKER_REASONS = (
    WAITING_FOR_MARKET_SESSION, WAITING_FOR_FORWARD_EVIDENCE,
    WAITING_FOR_PROVIDER_DATA, WAITING_FOR_SAMPLE,
    WAITING_FOR_EXTERNAL_ENTITLEMENT, FAMILY_EXHAUSTED, COMPUTE_GATE,
    DEPENDENCY_BLOCKED, INVALIDATED, SUPERSEDED, UNCLASSIFIED_BLOCKER,
)

#: What can end the wait. ``TIME`` blockers clear on their own once a session
#: elapses; ``INFORMATION`` blockers need something to arrive; ``TERMINAL``
#: blockers never clear without a human decision or new code. A runtime may
#: legitimately SLEEP on a TIME blocker; sleeping on a TERMINAL one and calling
#: it research is the failure mode this taxonomy exists to make visible.
CLEARS_ON_TIME = "TIME"
CLEARS_ON_INFORMATION = "INFORMATION"
CLEARS_TERMINAL = "TERMINAL"
CLEARANCE_VOCAB = (CLEARS_ON_TIME, CLEARS_ON_INFORMATION, CLEARS_TERMINAL)

CLEARANCE: dict[str, str] = {
    WAITING_FOR_MARKET_SESSION: CLEARS_ON_TIME,
    WAITING_FOR_FORWARD_EVIDENCE: CLEARS_ON_TIME,
    WAITING_FOR_SAMPLE: CLEARS_ON_TIME,
    WAITING_FOR_PROVIDER_DATA: CLEARS_ON_INFORMATION,
    WAITING_FOR_EXTERNAL_ENTITLEMENT: CLEARS_TERMINAL,
    FAMILY_EXHAUSTED: CLEARS_ON_INFORMATION,
    COMPUTE_GATE: CLEARS_ON_TIME,
    DEPENDENCY_BLOCKED: CLEARS_ON_INFORMATION,
    INVALIDATED: CLEARS_TERMINAL,
    SUPERSEDED: CLEARS_TERMINAL,
    UNCLASSIFIED_BLOCKER: CLEARS_ON_INFORMATION,
}

#: The operator sentence for each code. One spelling, so every surface says the
#: same thing about the same state.
DESCRIPTION: dict[str, str] = {
    WAITING_FOR_MARKET_SESSION:
        "the next observation for this scope does not exist yet; its market "
        "session has not completed",
    WAITING_FOR_FORWARD_EVIDENCE:
        "a prospective challenger is accruing forward observations; only "
        "elapsed sessions advance it and none may be synthesised",
    WAITING_FOR_PROVIDER_DATA:
        "an owned provider has not delivered the field this work needs; no "
        "purchase is implied and none is permitted here",
    WAITING_FOR_SAMPLE:
        "the effective INDEPENDENT sample is below the floor this family "
        "needs to conclude anything",
    WAITING_FOR_EXTERNAL_ENTITLEMENT:
        "the information is identified but the estate is not entitled to it; "
        "only the purchase gate can change this state",
    FAMILY_EXHAUSTED:
        "the generative space for this family returns candidates the estate "
        "has already tested; a new input, not more compute, reopens it",
    COMPUTE_GATE:
        "a compute or capacity ceiling refused the work; the evidence was "
        "never the constraint",
    DEPENDENCY_BLOCKED:
        "a prerequisite universe, panel or upstream artifact is absent, so "
        "the work could not be attempted",
    INVALIDATED:
        "the input this work rested on is not what its name claims; the work "
        "is void rather than unfinished",
    SUPERSEDED:
        "a later artifact already answers this question",
    UNCLASSIFIED_BLOCKER:
        "the owner recorded a blocker this taxonomy does not recognise; it is "
        "reported verbatim rather than folded into a neighbouring reason",
}

# --------------------------------------------------------------------------- #
# Classification. Ordered, most specific first; every rule is a SUBSTRING of
# what an owner actually writes, so a reason is never inferred from a job's
# category alone.
# --------------------------------------------------------------------------- #
#: ``(fragment, reason)``. Matched case-insensitively against the recorded text.
_RULES: tuple[tuple[str, str], ...] = (
    ("no non-degenerate candidate", FAMILY_EXHAUSTED),
    ("no novel candidate", FAMILY_EXHAUSTED),
    ("space is exhausted", FAMILY_EXHAUSTED),
    ("already tested", FAMILY_EXHAUSTED),
    ("no_members", DEPENDENCY_BLOCKED),
    ("no members", DEPENDENCY_BLOCKED),
    ("no equity feature declared", DEPENDENCY_BLOCKED),
    ("no futures feature declared", DEPENDENCY_BLOCKED),
    ("no feature declared", DEPENDENCY_BLOCKED),
    ("panel unavailable", DEPENDENCY_BLOCKED),
    ("no panel", DEPENDENCY_BLOCKED),
    ("universe is empty", DEPENDENCY_BLOCKED),
    ("not entitled", WAITING_FOR_EXTERNAL_ENTITLEMENT),
    ("entitlement", WAITING_FOR_EXTERNAL_ENTITLEMENT),
    ("no purchase is permitted", WAITING_FOR_EXTERNAL_ENTITLEMENT),
    ("subscription", WAITING_FOR_EXTERNAL_ENTITLEMENT),
    ("awaiting sample", WAITING_FOR_SAMPLE),
    ("effective sample", WAITING_FOR_SAMPLE),
    ("independent observations", WAITING_FOR_SAMPLE),
    ("sample size", WAITING_FOR_SAMPLE),
    ("forward evidence", WAITING_FOR_FORWARD_EVIDENCE),
    ("forward observation", WAITING_FOR_FORWARD_EVIDENCE),
    ("market session", WAITING_FOR_MARKET_SESSION),
    ("session has not", WAITING_FOR_MARKET_SESSION),
    ("not a trading day", WAITING_FOR_MARKET_SESSION),
    ("provider", WAITING_FOR_PROVIDER_DATA),
    ("data_incomplete", WAITING_FOR_PROVIDER_DATA),
    ("coverage", WAITING_FOR_PROVIDER_DATA),
    ("budget", COMPUTE_GATE),
    ("capacity", COMPUTE_GATE),
    ("timeout", COMPUTE_GATE),
    ("invalidated", INVALIDATED),
    ("withdrawn", INVALIDATED),
    ("superseded", SUPERSEDED),
)


def classify(reason: Any, *, payload: Optional[dict] = None) -> dict:
    """Classify ONE recorded blocker into the canonical taxonomy.

    ``payload`` is the blocked job's own mandate body. It is read ONLY for the
    dimensions the operator needs beside the reason (asset class, family,
    mandate id) — never to guess the reason itself, because a category has no
    opinion about why an attempt failed.
    """
    text = "" if reason is None else str(reason)
    low = text.lower()
    code = UNCLASSIFIED_BLOCKER
    matched = None
    for fragment, candidate in _RULES:
        if fragment in low:
            code, matched = candidate, fragment
            break
    p = payload or {}
    return {
        "reason_code": code,
        "reason_vocabulary": list(BLOCKER_REASONS),
        "clears_on": CLEARANCE[code],
        "clearance_vocabulary": list(CLEARANCE_VOCAB),
        "description": DESCRIPTION[code],
        "recorded_reason": text or None,
        "matched_on": matched,
        "classified_by": CALCULATION_OWNER,
        # Dimensions, carried verbatim. Asset-agnostic: an asset class is a
        # label on the row, never a branch in the logic above.
        "asset_class": p.get("asset_class"),
        "family": p.get("family"),
        "mandate_id": p.get("mandate_id"),
        "mandate_kind": p.get("kind"),
        "expected_information_value": p.get("expected_information_value"),
    }


def classify_job(job_row: Any) -> dict:
    """Classify a queue row (a mapping or a ``ResearchJob``-shaped object)."""
    def _get(name: str):
        if isinstance(job_row, dict):
            return job_row.get(name)
        return getattr(job_row, name, None)

    payload = _get("payload")
    if isinstance(payload, str):
        import json
        try:
            payload = json.loads(payload)
        except ValueError:
            payload = None
    if not isinstance(payload, dict):
        payload = _get("payload_json")
        if isinstance(payload, str):
            import json
            try:
                payload = json.loads(payload)
            except ValueError:
                payload = None
    out = classify(_get("blocked_reason"), payload=payload if isinstance(
        payload, dict) else None)
    out.update({
        "job_id": _get("job_id"),
        "category": _get("category"),
        "lane": _get("lane"),
        "state": _get("state"),
        "attempts": _get("attempts"),
        "blocked_at": _get("updated_at"),
    })
    return out


def summarise(classified: list) -> dict:
    """Group classified blockers by reason and by clearance, for one read."""
    by_reason: dict[str, int] = {}
    by_clearance: dict[str, int] = {}
    by_asset_class: dict[str, int] = {}
    for row in classified or []:
        code = row.get("reason_code") or UNCLASSIFIED_BLOCKER
        by_reason[code] = by_reason.get(code, 0) + 1
        clears = row.get("clears_on") or CLEARANCE.get(code)
        by_clearance[clears] = by_clearance.get(clears, 0) + 1
        ac = row.get("asset_class") or "UNDECLARED"
        by_asset_class[ac] = by_asset_class.get(ac, 0) + 1
    return {
        "owner": CALCULATION_OWNER,
        "blocked_total": len(classified or []),
        "by_reason": by_reason,
        "by_clearance": by_clearance,
        "by_asset_class": by_asset_class,
        "every_blocker_is_classified": all(
            r.get("reason_code") in BLOCKER_REASONS for r in (classified or [])),
        "unclassified_count": by_reason.get(UNCLASSIFIED_BLOCKER, 0),
        # A blocker that only TIME can clear is a legitimate reason to wait; one
        # that time can never clear is not, and a runtime that sleeps on it is
        # idling rather than researching.
        "time_will_clear": by_clearance.get(CLEARS_ON_TIME, 0),
        "needs_new_information": by_clearance.get(CLEARS_ON_INFORMATION, 0),
        "terminal_without_a_decision": by_clearance.get(CLEARS_TERMINAL, 0),
    }


__all__ = [
    "CALCULATION_OWNER", "BLOCKER_REASONS", "CLEARANCE", "CLEARANCE_VOCAB",
    "CLEARS_ON_TIME", "CLEARS_ON_INFORMATION", "CLEARS_TERMINAL",
    "DESCRIPTION", "classify", "classify_job", "summarise",
    "WAITING_FOR_MARKET_SESSION", "WAITING_FOR_FORWARD_EVIDENCE",
    "WAITING_FOR_PROVIDER_DATA", "WAITING_FOR_SAMPLE",
    "WAITING_FOR_EXTERNAL_ENTITLEMENT", "FAMILY_EXHAUSTED", "COMPUTE_GATE",
    "DEPENDENCY_BLOCKED", "INVALIDATED", "SUPERSEDED", "UNCLASSIFIED_BLOCKER",
]
