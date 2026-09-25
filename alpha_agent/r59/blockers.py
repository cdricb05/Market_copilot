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


def classify(reason: Any, *, payload: Optional[dict] = None,
             ruling: Optional[dict] = None) -> dict:
    """Classify ONE recorded blocker into the canonical taxonomy.

    ``payload`` is the blocked job's own mandate body. It is read ONLY for the
    dimensions the operator needs beside the reason (asset class, family,
    mandate id) — never to guess the reason itself, because a category has no
    opinion about why an attempt failed.

    ``ruling`` (R72) is the research director's DURABLE verdict on this job's
    economic family, read from :meth:`alpha_agent.r59.memory.ResearchMemory
    .director_ruling`. When one exists it OVERRIDES the text classification,
    because the recorded sentence is one engine's symptom from one attempt
    while the ruling is a governance fact about the family. Three cross-asset
    jobs recorded ``engine returned NO_MEMBERS`` and classified as
    ``DEPENDENCY_BLOCKED``, which clears on INFORMATION — while the director
    had ruled that the only information that could reopen them is not owned
    and must not be re-proposed. That is a TERMINAL state wearing an
    informational one, and this module's own docstring calls sleeping on it
    "the failure mode this taxonomy exists to make visible".

    The override never invents a code: a ruling carries one of
    :data:`BLOCKER_REASONS` and the memory owner refuses to record anything
    else. Both answers are returned — ``reason_code`` is the authoritative one
    and ``recorded_reason_code`` is what the text alone said — so a reader can
    always see that a ruling moved it, and on what authority.
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
    recorded_code = code
    r = ruling or {}
    ruled = str(r.get("blocker_reason") or "")
    authority = None
    if ruled and ruled in BLOCKER_REASONS:
        code = ruled
        authority = {
            "ruled_by": r.get("decided_by"),
            "verdict": r.get("verdict"),
            "campaign_id": r.get("campaign_id"),
            "decision_date": r.get("decision_date"),
            "rationale": r.get("rationale"),
            "reopen_condition": r.get("reopen_condition"),
            "source_artifact": r.get("source_artifact"),
            "overrode_recorded_code": (recorded_code
                                       if recorded_code != code else None),
            # R72.1 - HOW FAR the ruling that moved this code reaches, and
            # which key answered. A reader who cannot see that a family-wide
            # ruling (rather than one about this mechanism) reclassified the
            # job cannot audit whether it should have.
            "ruling_scope": r.get("ruling_scope"),
            "information_family": r.get("information_family"),
            "model_family": r.get("model_family"),
            "matched_on": r.get("matched_on"),
            "matched_exactly": r.get("matched_exactly"),
        }
    return {
        "reason_code": code,
        # What the recorded sentence alone said. Kept beside the authoritative
        # answer rather than replaced by it: a reader who cannot see that a
        # ruling moved the code cannot audit the ruling.
        "recorded_reason_code": recorded_code,
        "director_ruling": authority,
        "is_authoritative": bool(authority),
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


def ruling_for(payload: Optional[dict], *, mem: Any = None) -> Optional[dict]:
    """The director's durable ruling for a job's MECHANISM, if one exists.

    R72.1 - the job's ``information_family`` and ``model_family`` are passed
    through when it carries them, so a mechanism-scoped ruling reaches the one
    mechanism it was written about. A job that names no mechanism - which is
    every job the live R59 queue currently holds - can only match a FAMILY-WIDE
    ruling. That asymmetry is deliberate: an unnamed mechanism is not evidence
    that the job is inside a narrower ruling, and treating it as such would
    terminate cells no director ever ruled on.

    Never raises and never opens a store the caller did not hand it unless it
    can do so read-only: a blocker classification must not fail, and must not
    contend with the live worker's writes, merely because it asked a question.
    """
    p = payload or {}
    ac, fam = p.get("asset_class"), p.get("family")
    if not ac or not fam:
        return None
    try:
        if mem is None:
            from . import memory as _M
            mem = _M.open_memory_readonly()
        return mem.director_ruling(
            asset_class=str(ac), economic_family=str(fam),
            information_family=p.get("information_family"),
            model_family=p.get("model_family"))
    except Exception:                                       # noqa: BLE001
        return None


def classify_job(job_row: Any, *, mem: Any = None,
                 consult_rulings: bool = True) -> dict:
    """Classify a queue row (a mapping or a ``ResearchJob``-shaped object).

    R72 - the director's durable ruling on the job's family is consulted by
    default, so every caller that already reads a blocked job gets the
    authoritative answer without being changed. ``consult_rulings=False``
    returns the text-only classification, which is what the taxonomy's own
    unit tests assert about the rules themselves.
    """
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
    payload = payload if isinstance(payload, dict) else None
    out = classify(_get("blocked_reason"), payload=payload,
                   ruling=(ruling_for(payload, mem=mem)
                           if consult_rulings else None))
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
        # R72 - how many of the above are the DIRECTOR's answer rather than an
        # engine sentence, and how many the ruling moved. A reconciliation that
        # cannot be counted is indistinguishable from one that never ran.
        "authoritative_from_a_director_ruling": sum(
            1 for r in (classified or []) if r.get("is_authoritative")),
        "reclassified_by_a_ruling": sum(
            1 for r in (classified or [])
            if (r.get("director_ruling") or {}).get("overrode_recorded_code")),
    }


def reconcile(classified: list) -> dict:
    """WHAT THE QUEUE BELIEVES vs WHAT THE DIRECTOR RULED, side by side.

    Pure, and deliberately so: it reports the corrected picture and mutates no
    job. A queue row is the worker's to write, the worker holds the lease, and
    a second writer reaching into a live queue to "fix" rows is how an estate
    acquires a second owner. The operator (or the worker's own next pass) acts
    on this; nothing here acts on its own.

    ``stale`` is the set a reader most needs: jobs whose recorded blocker still
    says INFORMATION while the director has ruled the family TERMINAL. Each one
    is a job the runtime would otherwise keep sleeping on, forever.
    """
    rows = list(classified or [])
    stale, ruled, unruled = [], [], []
    for r in rows:
        dr = r.get("director_ruling") or {}
        if not r.get("is_authoritative"):
            unruled.append(r)
            continue
        ruled.append(r)
        moved = dr.get("overrode_recorded_code")
        if not moved:
            continue
        was = CLEARANCE.get(moved)
        now = CLEARANCE.get(r.get("reason_code"))
        if was != now:
            stale.append({
                "job_id": r.get("job_id"), "lane": r.get("lane"),
                "asset_class": r.get("asset_class"), "family": r.get("family"),
                "recorded_reason_code": moved, "recorded_clears_on": was,
                "authoritative_reason_code": r.get("reason_code"),
                "authoritative_clears_on": now,
                "ruled_by": dr.get("ruled_by"),
                "verdict": dr.get("verdict"),
                "campaign_id": dr.get("campaign_id"),
                "reopen_condition": dr.get("reopen_condition"),
                "rationale": dr.get("rationale"),
            })
    return {
        "owner": CALCULATION_OWNER,
        "blocked_total": len(rows),
        "with_a_director_ruling": len(ruled),
        "without_a_director_ruling": len(unruled),
        "reclassified": stale,
        "n_reclassified": len(stale),
        "n_now_terminal": sum(1 for s in stale
                              if s["authoritative_clears_on"] == CLEARS_TERMINAL),
        "mutates_no_job": True,
        "why_it_mutates_nothing": (
            "the queue row belongs to the worker that holds the lease; a "
            "second writer repairing rows underneath it would be a second "
            "owner of the queue"),
        "headline": (
            "%d of %d blocked jobs carry a director ruling; %d are recorded "
            "under a clearance the ruling contradicts, and %d of those are in "
            "fact TERMINAL - work the runtime would otherwise wait on for ever."
            % (len(ruled), len(rows), len(stale),
               sum(1 for s in stale
                   if s["authoritative_clears_on"] == CLEARS_TERMINAL))),
    }


__all__ = [
    "CALCULATION_OWNER", "BLOCKER_REASONS", "CLEARANCE", "CLEARANCE_VOCAB",
    "CLEARS_ON_TIME", "CLEARS_ON_INFORMATION", "CLEARS_TERMINAL",
    "DESCRIPTION", "classify", "classify_job", "summarise", "reconcile",
    "ruling_for",
    "WAITING_FOR_MARKET_SESSION", "WAITING_FOR_FORWARD_EVIDENCE",
    "WAITING_FOR_PROVIDER_DATA", "WAITING_FOR_SAMPLE",
    "WAITING_FOR_EXTERNAL_ENTITLEMENT", "FAMILY_EXHAUSTED", "COMPUTE_GATE",
    "DEPENDENCY_BLOCKED", "INVALIDATED", "SUPERSEDED", "UNCLASSIFIED_BLOCKER",
]
