r"""alpha_agent.r67.forward_producer - can a registered challenger EVER be funded?

THE QUESTION NOBODY HAD ASKED
-----------------------------
Eight challengers are registered with the canonical forward-evidence owner.
Five have emitted a decision. Zero have matured an observation. Every release
since R46 has reported those three numbers and every release has read them as
"early days".

They are not early days for all eight. The capital gate
(``api.capital_eligibility_gate``) requires **60 raw matured observations**
before a sleeve may hold capital. An observation is produced by a DECISION that
matured. Decisions are produced by a CADENCE PRODUCER. So the gate is reachable
only if something re-scores the frozen specification at every cadence boundary,
forever, and the arithmetic of WHEN is fully determined by the cadence:

    sessions_to_floor  =  (60 - matured_already) * cadence_sessions  +  horizon

That is a fact about each registration that can be computed today, and it had
never been computed. This module computes it, and in doing so separates two
populations that the three headline numbers had merged:

    ACCRUING          a producer exists and runs; the floor has a DATE.
    NO_CADENCE_PRODUCER   no code path re-scores this specification. It emitted
                      once, at registration, by hand. At its next cadence
                      boundary it will correctly report
                      AWAITING_NEW_GOVERNED_FREEZE and it will report that
                      forever. Its floor date does not exist.

MEASURED, NOT ARGUED
--------------------
The second population is not a suspicion. ``alpha_agent.r58.challengers``
exposes ``freeze(price, session)`` - a real, working cadence re-scorer with a
declared ``CADENCE = 21``. A repository-wide search for a caller finds exactly
two hits: the module's own docstring, and the string
``"alpha_agent.r58.challengers (challengers/<challenger_id>.json)"`` inside
``api.canonical_forward_accrual.FROZEN_DECISION_OWNERS``, which is a MAP ENTRY
naming the owner, not a call. The four R58 challenger files on disk have not
been written since 2026-09-04. The producer exists and nothing invokes it.

WHY THIS IS NOT A FORFEITURE, AND MUST NOT BE RECORDED AS ONE
-------------------------------------------------------------
R66 settled the neighbouring contract deliberately: a cadence boundary at which
the owner never froze anything is ``AWAITING_NEW_GOVERNED_FREEZE``, a fact about
governance, not a loss. That contract is correct and this module does not touch
it. A missing producer is not a missed opportunity - it is an absent capability,
and the honest record of an absent capability is this projection, not a
forfeiture row. ``forfeitures/`` has still never been created.

WHAT THIS MODULE DOES NOT DO
----------------------------
It does not emit a prediction, register a challenger, wire a producer, or write
anything to the accrual store. Attaching a cadence producer to an identity that
was registered when no such producer existed changes what that identity means,
and that is a governed decision for the human owner. This module PREPARES that
decision and refuses to take it.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "alpha_agent.r67.forward_producer"

#: Sessions per calendar year, for turning a session count into a date estimate.
#: The US equity calendar's long-run average; futures venues differ slightly and
#: the estimate is reported as an ESTIMATE for that reason.
SESSIONS_PER_YEAR = 252.0

#: Producer states.
P_ACCRUING = "CADENCE_PRODUCER_LIVE"
P_NONE = "NO_CADENCE_PRODUCER"
P_UNKNOWN = "PRODUCER_NOT_DETERMINED"
PRODUCER_STATES = (P_ACCRUING, P_NONE, P_UNKNOWN)

#: R68 - THE DECLARATION MOVED, AND THAT IS THE POINT.
#:
#: R67 held this table itself. It was the right place to keep a DIAGNOSIS and
#: the wrong place to keep a CONTRACT: a release module is written once and
#: frozen, while "which stage produces which challenger" is a fact every future
#: release must maintain or the next orphan goes unnoticed for exactly as long
#: as these four did. It now lives with the health owner, which the architecture
#: audit and the live invariant both read, and this module reads it from there
#: so the estate has ONE list rather than two that can disagree.
#:
#: R68 also WIRED the four R58 challengers, so the reason code
#: ``R58_FREEZE_NEVER_WIRED_TO_THE_RUNTIME`` no longer describes anything. It is
#: deliberately not deleted: it is retained below as a historical reason code so
#: this module's own record of what it found remains readable, while the LIVE
#: answer comes from the health owner's declaration.
from paper_trader.api.forward_producer_health import (      # noqa: E402
    KNOWN_UNPRODUCED, RUNTIME_PRODUCER_STAGES, UNPRODUCED_DETAIL as _HEALTH_DETAIL,
    UNPRODUCED_IS_A_DEFECT as _HEALTH_DEFECT)

#: The reason codes this release MEASURED, retained verbatim. R58's is history
#: as of R68 and is kept so the finding stays legible, not so it stays true.
R58_UNWIRED = "R58_FREEZE_NEVER_WIRED_TO_THE_RUNTIME"
R58_REPAIRED_BY = "alpha_agent.r68.r58_cadence_runtime"

UNPRODUCED_IS_A_DEFECT = dict(_HEALTH_DEFECT)
UNPRODUCED_IS_A_DEFECT.setdefault(R58_UNWIRED, True)

UNPRODUCED_DETAIL = dict(_HEALTH_DETAIL)
UNPRODUCED_DETAIL[R58_UNWIRED] = (
    "AS MEASURED BY R67, AND REPAIRED BY R68. "
    "alpha_agent.r58.challengers.freeze(price, session) existed and declared "
    "CADENCE = 21, but no module in alpha_agent/, scripts/ or api/ called it. "
    "The four challenger records on disk were last written 2026-09-04 and each "
    "emitted exactly one prediction, on 2026-09-10, by the hand that registered "
    "it. R68 wired %s as the r58_cadence_prospective_decision stage of the same "
    "runtime, so all four now have an executable prediction path and this code "
    "describes a repaired condition rather than a live one." % R58_REPAIRED_BY)


def _projection_file_exists() -> bool:
    """Is there an accrual projection artifact on disk at all?

    The distinction this supports is the whole point of the read guard: a
    MISSING artifact legitimately means no accrual run has happened, while an
    artifact that is present and reads as nothing means the read failed.
    """
    try:
        from pathlib import Path

        from paper_trader.api import canonical_forward_accrual as CFA
        return (Path(CFA.store_dir()) / CFA.PROJECTION_ARTIFACT).exists()
    except Exception:                                        # noqa: BLE001
        return False


def _gate_floor(horizon_sessions: Any) -> dict:
    """The capital gate's frozen floors for one horizon.

    Delegates to ``api.capital_eligibility_gate.gate_thresholds`` so the floor
    can never drift from the one the gate actually applies. This module owns no
    threshold of its own.
    """
    from paper_trader.api import capital_eligibility_gate as G
    return G.gate_thresholds(horizon_sessions)


def producer_for(challenger_id: str) -> dict:
    """Which runtime stage, if any, re-scores this challenger on cadence."""
    for stage, ids in RUNTIME_PRODUCER_STAGES.items():
        if challenger_id in ids:
            return {"producer_state": P_ACCRUING, "producer_stage": stage,
                    "producer_owner": "alpha_agent.r52.runtime", "reason": None,
                    "is_a_defect": False, "detail": None}
    code = KNOWN_UNPRODUCED.get(challenger_id)
    if code:
        return {"producer_state": P_NONE, "producer_stage": None,
                "producer_owner": None, "reason": code,
                "is_a_defect": UNPRODUCED_IS_A_DEFECT.get(code, True),
                "detail": UNPRODUCED_DETAIL.get(code)}
    return {"producer_state": P_UNKNOWN, "producer_stage": None,
            "producer_owner": None, "is_a_defect": None,
            "reason": "CHALLENGER_NOT_IN_THE_DECLARED_PRODUCER_MAP",
            "detail": ("this challenger is registered but appears in neither "
                       "RUNTIME_PRODUCER_STAGES nor KNOWN_UNPRODUCED; the map "
                       "must be extended before its floor can be stated")}


def time_to_capital_floor(accrual: dict) -> dict:
    """When - if ever - can this registration satisfy the capital gate?

    The binding requirement is almost always ``min_raw_matured`` (60 at every
    declared horizon), because an observation needs a DECISION and decisions
    arrive one per cadence. ``min_calendar_days`` and
    ``min_effective_independent`` are reported alongside so the caller can see
    which one actually binds rather than taking this function's word for it.
    """
    cadence = accrual.get("cadence_sessions")
    horizon = accrual.get("horizon_sessions")
    matured = accrual.get("matured_observations") or 0
    th = _gate_floor(horizon)

    need_raw = int(th["min_raw_matured"]) - int(matured)
    out = {
        "challenger_id": accrual.get("challenger_id"),
        "asset_class": accrual.get("asset_class"),
        "cadence_sessions": cadence,
        "horizon_sessions": horizon,
        "matured_observations": matured,
        "predictions_emitted": accrual.get("predictions_emitted"),
        "min_raw_matured_required": th["min_raw_matured"],
        "min_effective_independent_required": th["min_effective_independent"],
        "min_calendar_days_required": th["min_calendar_days"],
        "observations_still_required": max(0, need_raw),
        "gate_threshold_owner": "api.capital_eligibility_gate.gate_thresholds",
    }
    out.update(producer_for(accrual.get("challenger_id") or ""))

    if out["producer_state"] != P_ACCRUING:
        return {**out,
                "sessions_to_floor": None,
                "years_to_floor_estimate": None,
                "floor_reachable": False,
                "floor_verdict": "UNREACHABLE_NO_CADENCE_PRODUCER",
                "floor_explanation": (
                    "no code path re-scores this specification, so it can "
                    "produce no further decisions and therefore no further "
                    "observations. %d of %d required observations exist. This "
                    "is an ABSENT CAPABILITY, not a forfeiture."
                    % (matured, th["min_raw_matured"]))}

    if not cadence or not horizon:
        return {**out, "sessions_to_floor": None,
                "years_to_floor_estimate": None, "floor_reachable": None,
                "floor_verdict": "CADENCE_OR_HORIZON_NOT_DECLARED",
                "floor_explanation": (
                    "the registration declares cadence_sessions=%s and "
                    "horizon_sessions=%s; a floor date cannot be computed "
                    "without both" % (cadence, horizon))}

    sessions = need_raw * int(cadence) + int(horizon)
    years = sessions / SESSIONS_PER_YEAR
    return {**out,
            "sessions_to_floor": sessions,
            "years_to_floor_estimate": round(years, 2),
            "floor_reachable": True,
            "floor_verdict": "REACHABLE_ON_CADENCE",
            "floor_explanation": (
                "%d further observations at one per %d sessions, plus the %d-"
                "session horizon of the last one = %d sessions (~%.1f years at "
                "%d sessions/year). This assumes the producer emits at EVERY "
                "cadence boundary with no data block."
                % (need_raw, cadence, horizon, sessions, years,
                   int(SESSIONS_PER_YEAR)))}


def reconcile(*, accrual_by_identity: Optional[dict] = None) -> dict:
    """The whole forward book, asked whether it can ever fund anything.

    Pure read. Loads the cheap persisted accrual projection rather than
    recomputing it, because recomputation is expensive and this projection is
    about the SHAPE of the book, not about advancing it.
    """
    from paper_trader.api import canonical_forward_accrual as CFA

    # R67 - ABSENCE OF OBSERVATION IS NOT OBSERVATION OF ABSENCE.
    #
    # The persisted projection is rewritten by the LIVE research worker. A
    # reader can therefore land mid-write and get an empty map back, and the
    # naive reading of that is "nothing is registered" - a false statement about
    # the forward book, produced by a file-system race rather than by any fact.
    # This was observed: a test run during a worker cycle reconciled 0
    # registrations while 8 were on disk.
    #
    # So the roll-up the artifact keeps for ITSELF is used to cross-check the
    # map we actually parsed. A disagreement is reported as a READ PROBLEM and
    # never as a book with no challengers in it.
    proj, read_problem, expected = accrual_by_identity, None, None
    if proj is None:
        proj = CFA.load_accrual_projection() or {}
        try:
            art = CFA.load_accrual_projection_artifact() or {}
            body = art.get("body", art)
            expected = body.get("n_registered")
        except Exception:                                    # noqa: BLE001
            expected = None
        if expected is not None and int(expected) != len(proj):
            read_problem = (
                "the accrual projection artifact reports n_registered=%s but "
                "only %d identities could be read. The live research worker "
                "rewrites this artifact, so this is almost certainly a "
                "concurrent read and NOT an empty forward book. Nothing below "
                "should be treated as a fact about registrations."
                % (expected, len(proj)))
        elif not proj and _projection_file_exists():
            # The cross-check above is not enough on its own: a read that lands
            # mid-write can fail BOTH the map and the roll-up, leaving
            # expected=None and len=0, which agree - and would be reported as a
            # confidently empty book. Observed in exactly that form. An artifact
            # that EXISTS but yields nothing is a failed read, not a fact.
            read_problem = (
                "the accrual projection artifact exists on disk but yielded no "
                "identities and no n_registered roll-up. An artifact that "
                "exists and reads as nothing is a FAILED READ, not an empty "
                "forward book.")

    rows = []
    for ident, acc in sorted(proj.items(),
                             key=lambda kv: str(kv[1].get("challenger_id"))):
        r = time_to_capital_floor(acc)
        r["identity_hash"] = ident
        r["registration_session"] = acc.get("registration_session")
        r["current_accrual_state"] = acc.get("current_accrual_state")
        r["last_emission_session"] = acc.get("last_emission_session")
        rows.append(r)

    live = [r for r in rows if r["producer_state"] == P_ACCRUING]
    dead = [r for r in rows if r["producer_state"] == P_NONE]
    unknown = [r for r in rows if r["producer_state"] == P_UNKNOWN]
    orphaned = [r for r in dead if r.get("is_a_defect")]
    by_design = [r for r in dead if r.get("is_a_defect") is False]
    reachable = [r for r in live if r.get("floor_reachable")]
    soonest = min((r["sessions_to_floor"] for r in reachable
                   if r.get("sessions_to_floor") is not None), default=None)

    return {
        "schema_version": "r67_forward_producer_reconciliation.v1",
        "calculation_owner": CALCULATION_OWNER,
        "registry_owner": "api.forward_challenger_registry",
        "accrual_owner": "api.canonical_forward_accrual",
        "gate_owner": "api.capital_eligibility_gate",
        "producer_state_vocabulary": list(PRODUCER_STATES),
        "projection_read_problem": read_problem,
        "projection_is_trustworthy": read_problem is None,
        "projection_n_registered_expected": expected,
        "n_registered": len(rows),
        "n_with_live_producer": len(live),
        "n_without_producer": len(dead),
        "n_orphaned_defect": len(orphaned),
        "n_without_producer_by_design": len(by_design),
        "orphaned_challenger_ids": sorted(r["challenger_id"] for r in orphaned),
        "n_producer_undetermined": len(unknown),
        "matured_observations_total": sum(r["matured_observations"] for r in rows),
        "soonest_sessions_to_any_capital_floor": soonest,
        "soonest_years_to_any_capital_floor_estimate": (
            round(soonest / SESSIONS_PER_YEAR, 2) if soonest is not None else None),
        "rows": rows,
        "headline": _headline(rows, live, orphaned, by_design, soonest,
                              read_problem),
        "forfeiture_semantics": (
            "A registration with no cadence producer is NOT a forfeiture. R66 "
            "settled that a cadence boundary at which the owner never froze "
            "anything is AWAITING_NEW_GOVERNED_FREEZE - a fact about "
            "governance, not a loss. That contract is untouched here."),
        "read_only": True,
        "writes_nothing": True,
        "emits_no_prediction": True,
    }


def _headline(rows, live, orphaned, by_design, soonest,
              read_problem=None) -> str:
    if read_problem:
        return ("THE FORWARD BOOK COULD NOT BE READ RELIABLY: %s" % read_problem)
    if not rows:
        return "no challenger is registered with the canonical forward owner"
    parts = [
        "%d registered; %d have a live cadence producer, %d are ORPHANED "
        "(a producer was intended and never wired), %d have no producer BY "
        "DESIGN."
        % (len(rows), len(live), len(orphaned), len(by_design))]
    if soonest is not None:
        parts.append(
            "The SOONEST any registration can satisfy the 60-observation "
            "capital floor is %d sessions (~%.1f years) from its current "
            "state, and that is the best case with no data block."
            % (soonest, soonest / SESSIONS_PER_YEAR))
    if orphaned:
        parts.append(
            "The %d orphaned can never reach it at any date: %s."
            % (len(orphaned),
               ", ".join(sorted(r["challenger_id"] for r in orphaned))))
    return " ".join(parts)
