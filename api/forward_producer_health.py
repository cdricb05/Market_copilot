r"""api/forward_producer_health.py - THE owner of the question R67 could only ask
once: can each registered forward challenger actually take its next decision,
and is the thing that takes it alive?

WHY THIS EXISTS
---------------
For five releases the estate reported three numbers about its forward book -
registered, emitted, matured - and read a low matured count as "early days".
R67 measured that four of the eight registrations had no code path able to
produce a second decision at all, so their matured count was not low, it was
final. The defect had been invisible for two reasons, and this module removes
both:

1. A REGISTRATION WAS DESCRIBED AS ACCRUING BECAUSE IT WAS REGISTERED.
   "Registered" and "accruing" were the same word. They are now different
   states, and :data:`ACCRUING` is unreachable without a live producer.

2. NOTHING ASKED WHETHER A PRODUCER RAN.
   A producer that stopped, failed or was never wired looked identical to one
   patiently waiting for its next boundary. The runtime journals every stage it
   runs; :func:`heartbeat` reads that journal and reports, per producer, when it
   last ran and what it said.

WHAT IT OWNS, AND WHAT IT DELEGATES
-----------------------------------
It owns exactly one thing: the LIFECYCLE state of a registration with respect to
its producer. It owns no threshold, no accrual counter and no clock.

  * WHETHER an observation is due, and what happened to it, stays
    :mod:`api.canonical_forward_accrual`'s - this module reads its projection.
  * HOW LONG until a registration can satisfy the capital floor stays
    :mod:`alpha_agent.r67.forward_producer`'s, which reads its declaration from
    here so the estate has ONE list of which stage produces what.
  * WHEN anything runs stays :mod:`alpha_agent.r52.runtime`'s.

THE INVARIANT THIS MODULE EXISTS TO ENFORCE
-------------------------------------------
:func:`producer_coverage` answers, over the LIVE registry, whether any active
registration lacks an executable prediction path. A registration may legally
have no producer, but only with a DECLARED reason that says so - and a reason
this file has never heard of is a failure, not a default. ``audit_architecture``
and ``tests/test_release68_forward_evidence_repair.py`` both fail on a
registration that is active, unproduced and undeclared, so the next release
cannot reintroduce a silent orphan and wait months for a 60-observation gate to
reveal it.

READ ONLY. This module emits no prediction, freezes no decision, writes no
registration, promotes no model, allocates no capital and creates no order.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

SCHEMA_VERSION = "forward_producer_health.v1"
CALCULATION_OWNER = "api.forward_producer_health"
ACCRUAL_OWNER = "api.canonical_forward_accrual"
REGISTRY_OWNER = "api.forward_challenger_registry"
RUNTIME_OWNER = "alpha_agent.r52.runtime"

# --------------------------------------------------------------------------- #
# 1. THE REGISTRATION LIFECYCLE VOCABULARY
# --------------------------------------------------------------------------- #
#: A registration that exists and has no code path able to decide for it. The
#: state R58's four challengers were in for thirteen days while being counted
#: among "8 registered, 5 emitted, 0 matured".
L_NOT_ARMED = "REGISTERED_NOT_ARMED"
#: A producer exists, its next boundary is known, and that boundary has not yet
#: come within reach. Nothing is wrong and nothing is late.
L_ARMED = "ARMED_FOR_NEXT_DECISION"
#: A producer exists, has emitted, and has a reachable next boundary. The ONLY
#: state that may be reported as accruing, and it is unreachable without a
#: producer by construction.
L_ACCRUING = "ACCRUING"
#: The boundary is reachable and the producer is waiting on a publication it
#: does not control. A wait, not a fault, and never a loss.
L_AWAITING_DATA = "AWAITING_PUBLISHED_DATA"
#: A boundary's emission window shut with no decision. Permanent, never
#: backfilled, and reported so it is counted rather than absorbed.
L_MISSED_GAP = "MISSED_DECISION_GAP"
#: The producer ran and failed. Distinct from having no producer and from
#: waiting for data, because only this one needs somebody to look at it.
L_PRODUCER_FAILED = "PRODUCER_FAILED"
#: A later registration carries this one's mandate. Not a defect.
L_SUPERSEDED = "SUPERSEDED"
#: Deliberately ended. Not a defect.
L_RETIRED = "RETIRED"
LIFECYCLE_STATES = (L_NOT_ARMED, L_ARMED, L_ACCRUING, L_AWAITING_DATA,
                    L_MISSED_GAP, L_PRODUCER_FAILED, L_SUPERSEDED, L_RETIRED)

#: The states in which a registration can still reach the capital floor.
LIVE_STATES = (L_ARMED, L_ACCRUING, L_AWAITING_DATA, L_MISSED_GAP)
#: The states that are a DEFECT - something the estate meant to have and has not.
DEFECT_STATES = (L_NOT_ARMED, L_PRODUCER_FAILED)

# --------------------------------------------------------------------------- #
# 2. THE PRODUCER DECLARATION - one list, read by everything that asks
# --------------------------------------------------------------------------- #
#: ``runtime stage -> the challenger ids that stage re-scores on cadence``.
#:
#: This is a DECLARATION WITH TEETH. ``check_release68_forward_producer_health``
#: asserts every stage named here still exists in
#: ``alpha_agent.r52.runtime.research_runtime_cycle``, so a stage that is
#: renamed or deleted fails the build instead of silently turning a live
#: producer into a phantom one - which is exactly how the R58 four became
#: orphans without anybody noticing.
RUNTIME_PRODUCER_STAGES = {
    "next_open_prospective_decision": (
        "REVERSED_SPY_PUT_CALL_SKEW_H5_NEXT_OPEN_V1",),
    "fx_carry_cadence_prospective_decision": (
        "ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7",),
    "futures_trend_prospective_decision": (
        "ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1",),
    # R68 - the repair. alpha_agent.r68.r58_cadence_runtime.advance re-scores
    # all four R58 specifications at their own declared 21-session cadence.
    "r58_cadence_prospective_decision": (
        "R58_SHORT_VOLUME_PRESSURE_V1",
        "R58_DISCLOSURE_INTENSITY_V1",
        "R58_FUND_MOMENTUM_VETO_V1",
        "R58_FCF_PURE_V1",
    ),
}

#: ``stage -> the module that owns it``. Reported so an operator reading a
#: producer's state can go straight to the code that produces it.
STAGE_OWNERS = {
    "next_open_prospective_decision":
        "alpha_agent.alpha_recovery.next_open_runtime",
    "fx_carry_cadence_prospective_decision":
        "alpha_agent.alpha_recovery.fx_carry_cadence_runtime",
    "futures_trend_prospective_decision":
        "alpha_agent.alpha_recovery.futures_trend_runtime",
    "r58_cadence_prospective_decision":
        "alpha_agent.r68.r58_cadence_runtime",
}

#: A registration with NO stage, and the DECLARED reason it has none. An
#: undeclared absence is a failure; see :func:`producer_coverage`.
KNOWN_UNPRODUCED = {
    "REVERSED_SPY_PUT_CALL_SKEW_H5": "SUPERSEDED_BY_THE_NEXT_OPEN_SIBLING",
}

#: Which declared reasons are a DEFECT. Counting these separately is the whole
#: point: a deliberately superseded record was never expected to accrue and
#: reporting it beside a genuine orphan hides the orphan.
UNPRODUCED_IS_A_DEFECT = {
    "SUPERSEDED_BY_THE_NEXT_OPEN_SIBLING": False,
}

UNPRODUCED_DETAIL = {
    "SUPERSEDED_BY_THE_NEXT_OPEN_SIBLING": (
        "R62.3.3 froze a zero-subscription sibling that enters at the NEXT OPEN "
        "and keeps 98.7% of the edge. The producer runs the sibling. This "
        "registration is retained as an immutable record and is not expected to "
        "accrue; it is NOT a defect."),
}

#: The reason code an UNDECLARED unproduced registration is reported under. It
#: exists so the failure has a name in the artifact as well as in the exception.
UNDECLARED = "PRODUCER_NOT_DECLARED_FOR_AN_ACTIVE_REGISTRATION"

#: Producer states, kept byte-identical to R67's so nothing that reads them
#: has to learn a second vocabulary.
P_LIVE = "CADENCE_PRODUCER_LIVE"
P_NONE = "NO_CADENCE_PRODUCER"
P_UNKNOWN = "PRODUCER_NOT_DETERMINED"
PRODUCER_STATES = (P_LIVE, P_NONE, P_UNKNOWN)

#: Runtime stage states that mean the producer RAN AND FAILED, as against ran
#: and correctly did nothing. Read from the runtime's own vocabulary.
_FAILED_STAGE_STATES = ("FAILED_RETRYABLE", "FAILED_INTEGRITY", "FAILED")
_DATA_STAGE_STATES = ("DATA_BLOCKED",)
_MISSED_STAGE_STATES = ("FORFEITED",)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def producer_for(challenger_id: str) -> dict:
    """Which runtime stage, if any, re-scores this challenger on cadence."""
    cid = str(challenger_id or "")
    for stage, ids in RUNTIME_PRODUCER_STAGES.items():
        if cid in ids:
            return {"producer_state": P_LIVE, "producer_stage": stage,
                    "producer_owner": STAGE_OWNERS.get(stage, RUNTIME_OWNER),
                    "reason": None, "is_a_defect": False, "detail": None}
    code = KNOWN_UNPRODUCED.get(cid)
    if code:
        return {"producer_state": P_NONE, "producer_stage": None,
                "producer_owner": None, "reason": code,
                "is_a_defect": UNPRODUCED_IS_A_DEFECT.get(code, True),
                "detail": UNPRODUCED_DETAIL.get(code)}
    return {"producer_state": P_UNKNOWN, "producer_stage": None,
            "producer_owner": None, "is_a_defect": True, "reason": UNDECLARED,
            "detail": ("this challenger is registered but appears in neither "
                       "RUNTIME_PRODUCER_STAGES nor KNOWN_UNPRODUCED. A "
                       "registration with no declared production path is an "
                       "orphan; declare its producer, or declare why it has "
                       "none, before it is described as active.")}


# --------------------------------------------------------------------------- #
# 3. THE HEARTBEAT - did the producer actually run?
# --------------------------------------------------------------------------- #
def heartbeat(*, runs: Optional[dict] = None) -> dict:
    """When each declared producer stage last ran, and what it said.

    Read from the runtime's own run journal, which already records every stage
    of every cycle. Nothing is instrumented here and no new store is created:
    the fact was always being written and was never being read.
    """
    if runs is None:
        try:
            from paper_trader.alpha_agent.r52 import runtime as RT
            runs = RT.load_runs() or {}
        except Exception as exc:                            # noqa: BLE001
            return {"readable": False, "detail": str(exc)[:200], "stages": {}}
    rows = list((runs or {}).get("runs") or [])
    latest = (runs or {}).get("latest_run")
    if latest and latest not in rows:
        rows = rows + [latest]
    rows.sort(key=lambda r: str((r or {}).get("started_utc") or ""))

    out = {}
    for stage in RUNTIME_PRODUCER_STAGES:
        seen = None
        for run in rows:
            for st in (run or {}).get("stages") or []:
                if str((st or {}).get("stage") or "") != stage:
                    continue
                seen = {"last_run_id": run.get("run_id"),
                        "last_run_started_utc": run.get("started_utc"),
                        "last_run_finished_utc": run.get("finished_utc"),
                        "last_stage_state": st.get("state"),
                        "last_advance_state": st.get("advance_state"),
                        "last_detail": st.get("detail"),
                        "blocked_on": st.get("blocked_on"),
                        "frozen": st.get("frozen"),
                        "duration_ms": st.get("duration_ms")}
        out[stage] = seen or {
            "last_run_id": None, "last_run_started_utc": None,
            "last_stage_state": None,
            "detail": ("this stage is DECLARED as a producer but appears in no "
                       "retained run. Either it has never run, or it was "
                       "renamed without this declaration being updated.")}
        out[stage]["owner"] = STAGE_OWNERS.get(stage, RUNTIME_OWNER)
        out[stage]["produces"] = list(RUNTIME_PRODUCER_STAGES[stage])
        out[stage]["ran"] = bool(out[stage].get("last_run_id"))
    return {"readable": True, "n_runs_read": len(rows),
            "runs_journal_owner": RUNTIME_OWNER, "stages": out}


# --------------------------------------------------------------------------- #
# 4. THE LIFECYCLE STATE OF ONE REGISTRATION
# --------------------------------------------------------------------------- #
def _lifecycle_terminal(accrual: dict) -> Optional[str]:
    life = str((accrual or {}).get("lifecycle_state") or "").upper()
    if life in ("RETIRED", "CONCLUDED", "WITHDRAWN", "INVALIDATED"):
        return L_RETIRED
    if life == "SUPERSEDED":
        return L_SUPERSEDED
    return None


def lifecycle_state(accrual: dict, *, beat: Optional[dict] = None) -> dict:
    """The producer lifecycle state of ONE registration.

    ``accrual`` is one row of ``api.canonical_forward_accrual``'s projection.
    ``beat`` is this registration's producer stage heartbeat, when one exists.

    The order of the tests is the point. A terminal lifecycle answers first,
    because a retired registration has no producer question. A missing producer
    answers next, because nothing downstream of it can be true. Only then do the
    states that describe a WORKING producer get a chance, and ACCRUING is last -
    so it can never be reached by a registration that merely exists.
    """
    acc = accrual or {}
    cid = str(acc.get("challenger_id") or "")
    prod = producer_for(cid)
    out = {
        "challenger_id": cid,
        "identity_hash": acc.get("identity_hash"),
        "asset_class": acc.get("asset_class"),
        "registration_session": acc.get("registration_session"),
        # Carried through so a caller asking about the CAPITAL FLOOR does not
        # have to re-open the accrual projection to learn the cadence. Omitting
        # them made the R68 capital-path report compute
        # CADENCE_OR_HORIZON_NOT_DECLARED for every registration, which reads as
        # "nothing can be funded" and was a defect in the reader, not a fact.
        "cadence_sessions": acc.get("cadence_sessions"),
        "horizon_sessions": acc.get("horizon_sessions"),
        "predictions_emitted": acc.get("predictions_emitted") or 0,
        "matured_observations": acc.get("matured_observations") or 0,
        "pending_observations": acc.get("pending_observations") or 0,
        "forfeitures_recorded": acc.get("forfeitures_recorded") or 0,
        "last_emission_session": acc.get("last_emission_session"),
        "next_decision_session": acc.get("next_eligible_observation_session"),
        "current_accrual_state": acc.get("current_accrual_state")
                                 or acc.get("state"),
        "accrual_blocker": acc.get("latest_blocker"),
        "lifecycle_vocabulary": list(LIFECYCLE_STATES),
        **prod,
    }
    if beat:
        out["producer_heartbeat"] = {
            k: beat.get(k) for k in
            ("ran", "last_run_id", "last_run_started_utc", "last_stage_state",
             "last_advance_state", "blocked_on", "last_detail")}

    terminal = _lifecycle_terminal(acc)
    if terminal:
        return {**out, "lifecycle": terminal, "is_a_defect": False,
                "why": "the registration's own lifecycle is terminal; no "
                       "producer is expected"}
    if prod["producer_state"] != P_LIVE:
        code = prod.get("reason")
        if code and UNPRODUCED_IS_A_DEFECT.get(code) is False:
            return {**out, "lifecycle": L_SUPERSEDED, "is_a_defect": False,
                    "why": prod.get("detail")}
        return {**out, "lifecycle": L_NOT_ARMED, "is_a_defect": True,
                "why": ("no declared code path re-scores this specification, so "
                        "it can produce no further decision and therefore no "
                        "further observation, at any future date")}

    stage_state = str((beat or {}).get("last_stage_state") or "")
    if stage_state in _FAILED_STAGE_STATES:
        return {**out, "lifecycle": L_PRODUCER_FAILED, "is_a_defect": True,
                "why": ("the producer stage %s last reported %s: %s"
                        % (prod.get("producer_stage"), stage_state,
                           str((beat or {}).get("last_detail"))[:200]))}
    if stage_state in _MISSED_STAGE_STATES or (out["forfeitures_recorded"] or 0):
        return {**out, "lifecycle": L_MISSED_GAP, "is_a_defect": False,
                "why": ("a decision window shut with no decision. The "
                        "opportunity is permanently gone and is never "
                        "backfilled; the producer itself is alive.")}
    if (stage_state in _DATA_STAGE_STATES
            or str(out["accrual_blocker"] or "").startswith("PRICE_PANEL")
            or str(out["current_accrual_state"] or "") == "DATA_BLOCKED"):
        return {**out, "lifecycle": L_AWAITING_DATA, "is_a_defect": False,
                "why": ("the producer is alive and waiting on a publication it "
                        "does not control: %s"
                        % ((beat or {}).get("blocked_on")
                           or out["accrual_blocker"] or "unstated"))}
    if (out["predictions_emitted"] or 0) > 0:
        return {**out, "lifecycle": L_ACCRUING, "is_a_defect": False,
                "why": ("a live producer, %d prediction(s) emitted and a "
                        "reachable next boundary"
                        % (out["predictions_emitted"] or 0))}
    return {**out, "lifecycle": L_ARMED, "is_a_defect": False,
            "why": "a live producer with no emission yet; its first boundary "
                   "has not come within reach"}


# --------------------------------------------------------------------------- #
# 5. THE INVARIANT
# --------------------------------------------------------------------------- #
def producer_coverage(*, accrual_by_identity: Optional[dict] = None,
                      runs: Optional[dict] = None) -> dict:
    """Does EVERY active registration have an executable prediction path?

    The live counterpart to the static audit check. A registration may have no
    producer, but only with a reason declared in :data:`KNOWN_UNPRODUCED` - an
    absence this file has never heard of is reported as a FAILURE, because that
    is exactly the shape the R58 four had and it read as healthy for thirteen
    days.
    """
    from paper_trader.api import canonical_forward_accrual as CFA

    proj = accrual_by_identity
    read_problem = None
    if proj is None:
        proj = CFA.load_accrual_projection() or {}
        try:
            art = CFA.load_accrual_projection_artifact() or {}
            expected = (art.get("body", art) or {}).get("n_registered")
        except Exception:                                   # noqa: BLE001
            expected = None
        # R67 - ABSENCE OF OBSERVATION IS NOT OBSERVATION OF ABSENCE. The live
        # worker rewrites this artifact, so a reader can land mid-write. A
        # disagreement is a READ PROBLEM and never a book with nothing in it.
        if expected is not None and int(expected) != len(proj):
            read_problem = (
                "the accrual projection reports n_registered=%s but %d "
                "identities could be read; this is a concurrent read, not an "
                "empty forward book" % (expected, len(proj)))

    beats = heartbeat(runs=runs)
    rows = []
    for _ident, acc in sorted(proj.items(),
                              key=lambda kv: str(kv[1].get("challenger_id"))):
        prod = producer_for(str(acc.get("challenger_id") or ""))
        beat = (beats.get("stages") or {}).get(prod.get("producer_stage") or "")
        rows.append(lifecycle_state(acc, beat=beat))

    orphans = [r for r in rows if r["lifecycle"] == L_NOT_ARMED]
    failed = [r for r in rows if r["lifecycle"] == L_PRODUCER_FAILED]
    undeclared = [r for r in rows if r.get("reason") == UNDECLARED]
    by_state = {}
    for r in rows:
        by_state[r["lifecycle"]] = by_state.get(r["lifecycle"], 0) + 1
    ok = not orphans and not failed and read_problem is None

    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "accrual_owner": ACCRUAL_OWNER,
        "registry_owner": REGISTRY_OWNER,
        "runtime_owner": RUNTIME_OWNER,
        "generated_at": _now_iso(),
        "lifecycle_vocabulary": list(LIFECYCLE_STATES),
        "producer_state_vocabulary": list(PRODUCER_STATES),
        "n_registered": len(rows),
        "by_lifecycle_state": by_state,
        "n_orphaned": len(orphans),
        "n_producer_failed": len(failed),
        "n_undeclared": len(undeclared),
        "orphaned_challenger_ids": sorted(r["challenger_id"] for r in orphans),
        "producer_failed_challenger_ids": sorted(r["challenger_id"]
                                                 for r in failed),
        "undeclared_challenger_ids": sorted(r["challenger_id"]
                                            for r in undeclared),
        "projection_read_problem": read_problem,
        "every_active_registration_has_a_producer": ok,
        "heartbeat": beats,
        "registrations": rows,
        "read_only": True,
        "writes_nothing": True,
        "emits_no_prediction": True,
        "headline": _headline(rows, orphans, failed, read_problem),
    }


def _headline(rows, orphans, failed, read_problem) -> str:
    if read_problem:
        return "THE FORWARD BOOK COULD NOT BE READ RELIABLY: %s" % read_problem
    if not rows:
        return "no challenger is registered with the canonical forward owner"
    parts = ["%d registered." % len(rows)]
    if orphans:
        parts.append(
            "%d have NO executable prediction path and can never reach the "
            "capital floor at any date: %s."
            % (len(orphans), ", ".join(sorted(r["challenger_id"]
                                              for r in orphans))))
    if failed:
        parts.append("%d have a producer that RAN AND FAILED: %s."
                     % (len(failed), ", ".join(sorted(r["challenger_id"]
                                                      for r in failed))))
    if not orphans and not failed:
        parts.append("Every one has a declared, executable prediction path.")
    return " ".join(parts)


__all__ = [
    "SCHEMA_VERSION", "CALCULATION_OWNER", "LIFECYCLE_STATES", "LIVE_STATES",
    "DEFECT_STATES", "L_NOT_ARMED", "L_ARMED", "L_ACCRUING", "L_AWAITING_DATA",
    "L_MISSED_GAP", "L_PRODUCER_FAILED", "L_SUPERSEDED", "L_RETIRED",
    "RUNTIME_PRODUCER_STAGES", "STAGE_OWNERS", "KNOWN_UNPRODUCED",
    "UNPRODUCED_IS_A_DEFECT", "UNPRODUCED_DETAIL", "UNDECLARED",
    "PRODUCER_STATES", "P_LIVE", "P_NONE", "P_UNKNOWN",
    "producer_for", "heartbeat", "lifecycle_state", "producer_coverage",
]
