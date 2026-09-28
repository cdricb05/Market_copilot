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

# --------------------------------------------------------------------------- #
# 2b. THE BOUNDARY RECONCILIATION VOCABULARY (R72)
# --------------------------------------------------------------------------- #
#: The producer and the accrual owner each answer "which sessions were this
#: registration's decision boundaries". Until now nobody compared the two
#: answers, and the comparison is the only thing that can tell a PERMANENT LOSS
#: from a challenger healthily waiting for its next turn: the accrual reports
#: both as ``NOT_DUE / AWAITING_NEW_GOVERNED_FREEZE``, deliberately and
#: correctly, because a boundary at which the owner froze nothing is a fact
#: about governance and not a forfeiture (R62.2 (d), re-affirmed by R66, and
#: enforced by ``check_release62_2_automatic_forward_accrual``).
#:
#: That contract is not touched here. This module owns the LIFECYCLE question,
#: so the loss is named HERE, beside the accrual's answer rather than instead of
#: it - the completion of the comparison R68 set up when it began reporting
#: ``next_boundary_from_producer`` "so the two can be compared rather than
#: confused".
B_AGREED = "PRODUCER_AND_ACCRUAL_AGREE"
#: The producer's own grid contained a boundary, its session is strictly past,
#: no decision was ever frozen for it, and the accrual holds no forfeiture for
#: it. The evidence does not exist and never will, and no counter said so.
B_UNRECORDED_MISS = "PERMANENT_MISS_NOT_RECORDED_AS_A_FORFEITURE"
#: The producer declares no grid at all (it failed to read one, or none is
#: declared). Reported rather than defaulted: an unknown is not an agreement.
B_NO_GRID = "PRODUCER_DECLARES_NO_BOUNDARY_GRID"
#: No producer is EXPECTED for this registration, and the absence is declared
#: and non-defect (``KNOWN_UNPRODUCED`` with ``UNPRODUCED_IS_A_DEFECT`` False).
#: Distinct from :data:`B_NO_GRID` for the reason ``UNPRODUCED_IS_A_DEFECT``
#: already gives one layer up - "a deliberately superseded record was never
#: expected to accrue and reporting it beside a genuine orphan hides the
#: orphan". A record with no producer has no boundary to lose, so calling its
#: losses uncountable is a false alarm that would sit in the ledger for ever
#: and drown the one silence this ledger exists to surface.
B_NOT_EXPECTED = "NO_PRODUCER_EXPECTED_SO_NO_BOUNDARY_GRID"
BOUNDARY_STATES = (B_AGREED, B_UNRECORDED_MISS, B_NO_GRID, B_NOT_EXPECTED)

# --------------------------------------------------------------------------- #
# 2c. THE PRE-BOUNDARY READINESS VOCABULARY (R74.2)
# --------------------------------------------------------------------------- #
#: R72 gave the estate a PERMANENT LOSS LEDGER, and it works: it counted 11
#: boundaries that passed with no decision. But every state in it is a POST
#: MORTEM. ``PERMANENT_MISS_NOT_RECORDED_AS_A_FORFEITURE`` is only ever true
#: after the opportunity is already gone, so the ledger's honesty was bought at
#: the price of being unable to prevent a single entry in it.
#:
#: The nine consecutive SPY misses of 2026-09-15..2026-09-25 are the proof. On
#: every one of those days the producer ran, reported ``DATA_BLOCKED`` with the
#: exact blocker ("the historical vendor has not published session S-1 yet"),
#: and then reported ``FORFEITED`` an hour later. The second miss was fully
#: predictable from the first, the third from the second, and nothing anywhere
#: turned that into a warning while the window was still open.
#:
#: This section is the missing half: the SAME facts, read BEFORE the deadline
#: rather than after it. It owns no clock, no threshold on the strategy and no
#: decision - it reads the producer's own journalled boundary grid and its own
#: journalled input state, and says whether the next boundary is reachable.

#: The producer is alive, its next boundary is ahead, and the inputs that
#: boundary needs are in hand. Nothing to do.
R_READY = "READY_FOR_NEXT_BOUNDARY"
#: The next boundary is ahead and its inputs are NOT in hand, but the boundary is
#: not yet within the warning lead. Informational: most of these resolve.
R_INPUT_PENDING = "INPUTS_PENDING_BOUNDARY_NOT_IMMINENT"
#: THE WARNING THIS SECTION EXISTS FOR. The boundary is within the warning lead
#: and the inputs it needs are still missing. Actionable while the window is open.
R_INPUT_LATE = "BOUNDARY_IMMINENT_AND_INPUTS_MISSING"
#: The boundary is within the warning lead and the producer has not run inside
#: its staleness tolerance. A dead producer and a late vendor are different
#: problems with different owners, so they are never merged.
R_PRODUCER_STALE = "BOUNDARY_IMMINENT_AND_PRODUCER_STALE"
#: A boundary is ahead and no code path can decide it. The R68 orphan, seen
#: prospectively instead of after the fact.
R_NO_PRODUCER = "BOUNDARY_AHEAD_AND_NO_PRODUCER"
#: The producer is alive and its inputs do arrive, yet it has missed
#: :data:`CHRONIC_CONSECUTIVE_MISSES` or more consecutive boundaries with no
#: emission since. That is not a risk to warn about once - it is a STRUCTURAL
#: statement that the declared window cannot be met by the declared source, and
#: it is the single most valuable thing this module can say. It is reported, not
#: repaired: changing the window or the source is a governed decision.
R_CHRONIC = "CHRONIC_MISS_WINDOW_UNREACHABLE_BY_DECLARED_SOURCE"
#: The producer declares no next boundary, so there is no deadline to be ready
#: for. An absence of a question, not an answer.
R_NO_BOUNDARY = "NO_NEXT_BOUNDARY_DECLARED"
#: No producer is expected (declared, non-defect). Cannot be late for a boundary
#: it was never going to take.
R_NOT_EXPECTED = "NO_PRODUCER_EXPECTED"
#: R80. The accrual owner has REFUSED this registration's declared entry mark: its
#: declared valuation path cannot price the instant the strategy says it enters.
#: Every OTHER readiness check passes for such a registration - its inputs are in
#: hand, its producer is live and fresh, a producer exists, and two misses are not
#: yet chronic - so it was counted READY_FOR_NEXT_BOUNDARY and the module's own
#: headline said "every one has a declared, executable prediction path" about a
#: registration whose prediction path the accrual owner had already declared
#: inexecutable. It had emitted nothing across two boundaries and was two days
#: from a third, and this monitor - the one instrument whose entire purpose is to
#: speak BEFORE a boundary is lost - reported OK.
#:
#: It is a DEFECT and it is actionable, because it is reachable only by a governed
#: decision on the entry contract or the valuation path, and that decision needs an
#: operator who knows the boundary is about to be lost. Like R_CHRONIC it is
#: REPORTED, never repaired here: this module changes no entry contract, no poll
#: cadence and no valuation path.
R_ENTRY_MARK_INFEASIBLE = "BOUNDARY_AHEAD_AND_ENTRY_MARK_UNPRICEABLE"
READINESS_STATES = (R_READY, R_INPUT_PENDING, R_INPUT_LATE, R_PRODUCER_STALE,
                    R_NO_PRODUCER, R_CHRONIC, R_NO_BOUNDARY, R_NOT_EXPECTED,
                    R_ENTRY_MARK_INFEASIBLE)


#: The accrual owner's own refusal code and blocked state, imported rather than
#: retyped so a rename cannot silently stop matching (the same discipline
#: ``api.autonomous_operating_status`` applies to the identical string).
def _unpriceable_entry_mark() -> tuple:
    try:
        from . import canonical_forward_accrual as _ACC
        return (_ACC.INTEGRITY_ENTRY_MARK_UNPRICEABLE,
                _ACC.ACC_INTEGRITY_BLOCKED)
    except Exception:                                       # noqa: BLE001
        return ("DECLARED_ENTRY_MARK_IS_NOT_SERVED_BY_THE_DECLARED_"
                "VALUATION_PATH", "INTEGRITY_BLOCKED")


UNPRICEABLE_ENTRY_MARK_BLOCKER, ACCRUAL_INTEGRITY_BLOCKED = \
    _unpriceable_entry_mark()

#: The readiness states that must reach an operator BEFORE the boundary passes.
ACTIONABLE_READINESS_STATES = (R_INPUT_LATE, R_PRODUCER_STALE, R_NO_PRODUCER,
                               R_CHRONIC, R_ENTRY_MARK_INFEASIBLE)

SEV_OK = "OK"
SEV_WARN = "WARN"
SEV_DEFECT = "DEFECT"
READINESS_SEVERITY = {
    R_READY: SEV_OK,
    R_INPUT_PENDING: SEV_OK,
    R_NO_BOUNDARY: SEV_OK,
    R_NOT_EXPECTED: SEV_OK,
    R_INPUT_LATE: SEV_WARN,
    R_CHRONIC: SEV_WARN,
    R_PRODUCER_STALE: SEV_DEFECT,
    R_NO_PRODUCER: SEV_DEFECT,
    R_ENTRY_MARK_INFEASIBLE: SEV_DEFECT,
}

#: Whether the inputs the next decision needs are in hand. Read from the
#: producer's OWN journalled publication state and blocker; never probed here,
#: because a second probe would be a second source of truth about a vendor.
I_PRESENT = "INPUTS_PRESENT"
I_MISSING = "INPUTS_MISSING"
#: The producer declares no input gate at all. Distinct from "present": a
#: producer that never says whether its data arrived has not told us it did.
I_NOT_DECLARED = "INPUT_STATE_NOT_DECLARED_BY_PRODUCER"
INPUT_STATES = (I_PRESENT, I_MISSING, I_NOT_DECLARED)

#: WHICH leg of the input chain is short. The vendor publishing and the local
#: panel containing the session are different facts with different owners, and
#: they came apart in the live estate for four consecutive days. Naming the leg
#: is what makes the warning actionable rather than merely true.
L_LEG_VENDOR = "VENDOR_HAS_NOT_SERVED_THE_SESSION"
L_LEG_LOCAL = "LOCAL_PANEL_HAS_NOT_REACHED_THE_PUBLISHED_SESSION"
INPUT_LEGS = (L_LEG_VENDOR, L_LEG_LOCAL)

#: How close a boundary must be before a missing input becomes a WARNING rather
#: than an observation. Three calendar days spans a weekend, which is exactly the
#: interval over which the SPY source's latency is survivable, so a boundary
#: inside it is one whose inputs should already exist.
WARN_LEAD_CALENDAR_DAYS = 3

#: How long a declared producer may be silent before a boundary inside the
#: warning lead is at risk from the producer rather than from the data. The
#: retained journal shows an hourly cycle, so six hours is a silence no healthy
#: runtime produces.
PRODUCER_STALE_AFTER_HOURS = 6

#: Consecutive missed boundaries, with no emission since the newest of them, that
#: turn a run of bad luck into a structural verdict. Three is the smallest number
#: that cannot be a one-off plus a retry.
CHRONIC_CONSECUTIVE_MISSES = 3

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
                        "duration_ms": st.get("duration_ms"),
                        # WHAT IS DUE NEXT, and what was permanently lost. This
                        # extraction is an ALLOW-LIST too, and leaving these out
                        # reproduced the defect one layer up: the journal
                        # carried next_boundaries and the health read dropped it
                        # again, so the operator-facing answer was still None
                        # while the fact sat on disk.
                        "next_boundaries": st.get("next_boundaries"),
                        "missed_boundaries": st.get("missed_boundaries"),
                        "forward_panel_last_session":
                            st.get("forward_panel_last_session"),
                        # R74.2 - WHETHER THE NEXT BOUNDARY CAN BE MET, and it
                        # is the SAME allow-list defect a third time. The
                        # producers have journalled ``publication``,
                        # ``entry_session``, ``information_session`` and
                        # ``entry_state`` since R62.3.3; this extraction dropped
                        # every one of them, so the only component that asks
                        # "are the inputs for the next decision in hand?" could
                        # not see the answer sitting on disk. Nine SPY
                        # boundaries were lost while the fact that would have
                        # predicted each one was being written and discarded.
                        "publication": st.get("publication"),
                        "entry_session": st.get("entry_session"),
                        "information_session": st.get("information_session"),
                        "entry_state": st.get("entry_state"),
                        "blocked_owner": st.get("blocked_owner"),
                        "declared_grid_owner": st.get("declared_grid_owner"),
                        "window_opens_at": st.get("window_opens_at"),
                        # R76 - the append_state above says what collection
                        # DID; this says whether collection was even reached.
                        # Nine SPY boundaries were lost with append_state
                        # absent, which reads identically to "the producer
                        # tried and the vendor had nothing".
                        "acquisition_precedes_decision_state":
                            st.get("acquisition_precedes_decision_state"),
                        "append_state": st.get("append_state")}
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


def boundary_reconciliation(accrual: dict,
                            beat: Optional[dict] = None,
                            *, producer: Optional[dict] = None) -> dict:
    """Do the producer and the accrual owner agree about what was LOST?

    The producer is the authority on its own grid - the accrual says so in as
    many words ("a decision is the originating owner's act and this module may
    not take one on its behalf"), and for a registration that observes on its
    instruments' own realised bar calendar the accrual cannot even ask an
    exchange-calendar boundary function. So the producer's declared
    ``missed_boundaries`` is the count of permanently lost opportunities, and
    the accrual's ``forfeitures`` is the count it has recorded as such.

    The difference is the number that was invisible. It is reported, never
    repaired: a forfeiture the accrual declines to record under its own contract
    is not this module's to write, and recording one here would be a second
    evidence store. Pure; reads nothing from disk.

    ``producer`` is this registration's :func:`producer_for` verdict, when the
    caller has one. It is consulted for a single distinction: a registration
    whose absence of a producer is DECLARED and non-defect has no grid because
    it is not supposed to have one, and saying its losses "cannot be counted"
    would be a permanent false alarm rather than a silence worth hearing.
    """
    acc = accrual or {}
    b = beat or {}
    prod = producer or {}
    declared = [str(s) for s in (b.get("missed_boundaries") or [])]
    recorded = int(acc.get("forfeitures") or acc.get("forfeitures_recorded") or 0)
    has_grid = b.get("missed_boundaries") is not None
    out = {
        "producer_declared_missed_boundaries": sorted(set(declared)),
        "n_producer_declared_missed": len(set(declared)),
        "n_accrual_recorded_forfeitures": recorded,
        "producer_next_boundaries": list(b.get("next_boundaries") or []),
        "accrual_next_eligible_observation_session":
            acc.get("next_eligible_observation_session"),
        "boundary_state": B_AGREED,
        "reconciliation_owner": CALCULATION_OWNER,
        "the_accrual_contract_is_not_changed_here": True,
    }
    # A DECLARED, non-defect absence of a producer answers first: there is no
    # grid because none was ever expected, which is a different fact from a
    # declared producer that has gone quiet, and the two must not share a state.
    if (prod.get("producer_state") == P_NONE
            and prod.get("reason")
            and not UNPRODUCED_IS_A_DEFECT.get(prod["reason"], True)):
        out["boundary_state"] = B_NOT_EXPECTED
        out["n_unrecorded_permanent_misses"] = 0
        out["declared_unproduced_reason"] = prod["reason"]
        out["why"] = ("no producer is expected for this registration (%s), so "
                      "it has no boundary to lose. Counted as nothing lost "
                      "rather than as an uncountable silence, which is the "
                      "distinction UNPRODUCED_IS_A_DEFECT already draws."
                      % prod["reason"])
        return out
    if not has_grid:
        out["boundary_state"] = B_NO_GRID
        out["why"] = ("this producer publishes no boundary grid on the runtime "
                      "journal, so whether it has lost a boundary cannot be "
                      "answered; an unknown is not an agreement")
        out["n_unrecorded_permanent_misses"] = None
        return out
    unrecorded = max(0, len(set(declared)) - recorded)
    out["n_unrecorded_permanent_misses"] = unrecorded
    if unrecorded:
        out["boundary_state"] = B_UNRECORDED_MISS
        out["why"] = (
            "%d boundary(ies) on this producer's OWN grid passed with no frozen "
            "decision and no recorded forfeiture: %s. The evidence does not "
            "exist and never will. The accrual reports these as NOT_DUE / "
            "AWAITING_NEW_GOVERNED_FREEZE, which is correct under its contract "
            "and is why the loss needs naming here."
            % (unrecorded, ", ".join(sorted(set(declared))[:12])))
    else:
        out["why"] = ("every boundary this producer declares is either frozen, "
                      "still ahead, or already recorded as a forfeiture")
    return out


def _as_date(value: Any):
    """A YYYY-MM-DD string or date to a date, or None. No calendar arithmetic."""
    from datetime import date as _date
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, _date):
        return value
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except Exception:                                       # noqa: BLE001
        return None


def input_readiness(beat: Optional[dict]) -> dict:
    """Are the inputs the NEXT decision needs in hand, per the producer itself?

    Read from the producer's own journalled ``publication`` state and blocker.
    Nothing is probed: a second probe of a vendor would be a second source of
    truth about whether that vendor published, and the producer is already the
    authority on its own source.

    A producer that declares no input gate is reported :data:`I_NOT_DECLARED`,
    never :data:`I_PRESENT`. Silence is not confirmation.

    TWO LEGS, AND THE SECOND ONE IS THE BINDING ONE
    -----------------------------------------------
    ``publication`` answers whether the VENDOR has served the session. It does
    NOT answer whether the LOCAL panel the producer actually scores contains it,
    and those came apart in the live estate for four days: on 2026-09-25 at
    18:10Z the journal carried ``publication=PUBLISHED`` for information session
    2026-09-24 while ``forward_panel_last_session`` was still 2026-09-21. A
    reader that stopped at the vendor leg would have called those inputs
    present, and the producer would still have had nothing to score.

    So the legs are separated and the LOCAL one decides, because the local panel
    is what the signal is computed from. :data:`L_LEG_VENDOR` and
    :data:`L_LEG_LOCAL` name which one is short, so the warning reaches the
    right owner instead of blaming a vendor that already delivered.
    """
    b = beat or {}
    blocked = b.get("blocked_on")
    stage_state = str(b.get("last_stage_state") or "").upper()
    pub = str(b.get("publication") or "").upper() or None
    panel = str(b.get("forward_panel_last_session") or "")[:10] or None
    info = str(b.get("information_session") or "")[:10] or None
    out = {
        "input_state": I_NOT_DECLARED,
        "input_state_vocabulary": list(INPUT_STATES),
        "short_leg": None,
        "publication_state_from_producer": b.get("publication"),
        "information_session": info,
        "entry_session_from_producer": b.get("entry_session"),
        "entry_state_from_producer": b.get("entry_state"),
        "blocked_on": blocked,
        "blocked_owner": b.get("blocked_owner"),
        "forward_panel_last_session": panel,
        "local_panel_reaches_the_information_session": (
            None if not (panel and info) else panel >= info),
    }
    if blocked or stage_state in _DATA_STAGE_STATES:
        return {**out, "input_state": I_MISSING, "short_leg": L_LEG_VENDOR,
                "why": ("the producer reports it is waiting on a publication it "
                        "does not control: %s"
                        % (str(blocked or "unstated")[:300]))}
    # THE LOCAL LEG. A vendor that has published and a local panel that has not
    # caught up is the case a single publication flag cannot express, and it is
    # the case that actually occurred. It is reported as MISSING, against the
    # LOCAL owner, because the producer scores the panel and not the vendor.
    if panel and info and panel < info:
        return {**out, "input_state": I_MISSING, "short_leg": L_LEG_LOCAL,
                "why": ("the VENDOR leg is satisfied (publication=%s) but the "
                        "LOCAL panel the producer scores reaches only %s, which "
                        "does not reach the information session %s. The missing "
                        "owner is the local acquisition/append path, not the "
                        "vendor." % (pub, panel, info))}
    if pub == "PUBLISHED":
        return {**out, "input_state": I_PRESENT,
                "why": ("the producer reports its source session as PUBLISHED"
                        + ("" if not (panel and info) else
                           " and its local panel reaches %s >= %s"
                           % (panel, info)))}
    if pub:
        return {**out, "input_state": I_MISSING, "short_leg": L_LEG_VENDOR,
                "why": ("the producer reports publication_state=%s for its "
                        "source session" % pub)}
    return {**out, "why": ("this producer journals no publication state, so "
                           "whether its inputs are in hand cannot be asserted "
                           "from the journal; reported as undeclared rather "
                           "than assumed present")}


def chronic_miss(accrual: dict, beat: Optional[dict]) -> dict:
    """Is this producer's declared window structurally unreachable?

    A single missed boundary is bad luck. A RUN of them, with no emission since
    the newest, is a statement about the contract: the window the strategy
    declares cannot be met by the source the producer reads. The estate could
    not say this before, so it said ``MISSED_DECISION_GAP`` nine times about SPY
    and each one read like an isolated accident.

    The rule is deliberately conservative about recovery: an emission dated at or
    after the newest declared miss proves the producer can still meet a boundary,
    so the run is over and nothing chronic is claimed. FX carry missed
    2026-09-15 and emitted 2026-09-22; that is a recovery, not a chronic fault,
    and must not be reported as one.

    Pure. Reads nothing from disk and changes no contract.
    """
    acc = accrual or {}
    missed = sorted({str(s)[:10] for s in ((beat or {}).get("missed_boundaries")
                                           or [])})
    last_em = str(acc.get("last_emission_session") or "")[:10] or None
    out = {
        "n_consecutive_missed_boundaries": len(missed),
        "first_missed_boundary": missed[0] if missed else None,
        "newest_missed_boundary": missed[-1] if missed else None,
        "last_emission_session": last_em,
        "threshold": CHRONIC_CONSECUTIVE_MISSES,
        "is_chronic": False,
        "recovered_after_the_newest_miss": False,
    }
    if not missed:
        return {**out, "why": "this producer declares no missed boundary"}
    if last_em and last_em >= missed[-1]:
        return {**out, "recovered_after_the_newest_miss": True,
                "why": ("an emission dated %s is at or after the newest missed "
                        "boundary %s, so this producer has proved it can still "
                        "meet a boundary; the run is over"
                        % (last_em, missed[-1]))}
    if len(missed) < CHRONIC_CONSECUTIVE_MISSES:
        return {**out, "why": ("%d missed boundary(ies) is below the %d needed "
                               "to call the window structurally unreachable"
                               % (len(missed), CHRONIC_CONSECUTIVE_MISSES))}
    return {**out, "is_chronic": True,
            "why": ("%d consecutive declared boundaries (%s..%s) passed with no "
                    "decision and nothing has been emitted since. The producer "
                    "is alive and its source does arrive, so what is failing is "
                    "not the code but the FIT between the declared emission "
                    "window and the source's publication latency. Changing "
                    "either is a governed decision and is not taken here."
                    % (len(missed), missed[0], missed[-1]))}


def preboundary_readiness(accrual: dict, beat: Optional[dict] = None, *,
                          producer: Optional[dict] = None,
                          now: Optional[datetime] = None) -> dict:
    """Can this registration MEET its next boundary - asked before it passes.

    The prospective counterpart to :func:`boundary_reconciliation`. That function
    counts what was lost; this one names what is ABOUT to be lost while there is
    still a window in which to act.

    It reads three facts the estate already had and never combined: the
    producer's own next boundary, the producer's own input state, and when the
    producer last ran. It owns no clock - the boundary comes from the producer's
    declared grid - and it computes no session arithmetic for a class whose
    calendar this estate does not own, reporting the distance in CALENDAR DAYS
    (a unit that needs no calendar) and adding an eligible-session distance only
    for the classes the authoritative exchange calendar covers.

    Pure apart from the exchange-calendar supplier, which is consulted through
    the ONE registry owner. Writes nothing, emits nothing, changes no contract.
    """
    acc = accrual or {}
    b = beat or {}
    prod = producer or producer_for(str(acc.get("challenger_id") or ""))
    now = now or datetime.now(timezone.utc)
    today = now.date()

    inputs = input_readiness(b)
    chronic = chronic_miss(acc, b)
    # R80 - the accrual owner's refusal of the declared entry mark. Read from the
    # accrual row under BOTH spellings the projection uses, because this estate has
    # now lost four separate facts to one of them travelling under a name its
    # reader did not use.
    acc_state = acc.get("current_accrual_state") or acc.get("state")
    acc_blocker = acc.get("latest_blocker") or acc.get("accrual_blocker")
    entry_mark_unpriceable = bool(
        str(acc_blocker or "") == UNPRICEABLE_ENTRY_MARK_BLOCKER
        or (str(acc_state or "") == ACCRUAL_INTEGRITY_BLOCKED
            and str(acc_blocker or "") == UNPRICEABLE_ENTRY_MARK_BLOCKER))
    boundaries = sorted({str(s)[:10] for s in (b.get("next_boundaries") or [])})
    next_boundary = boundaries[0] if boundaries else None
    nb_date = _as_date(next_boundary)
    days = (nb_date - today).days if nb_date else None

    out = {
        "readiness_vocabulary": list(READINESS_STATES),
        "actionable_readiness_states": list(ACTIONABLE_READINESS_STATES),
        "asked_at": now.isoformat(),
        "asked_before_the_boundary": bool(days is not None and days >= 0),
        "next_boundary": next_boundary,
        "next_boundary_source": ("PRODUCER_DECLARED_GRID" if next_boundary
                                 else None),
        "declared_grid_owner": b.get("declared_grid_owner"),
        "all_declared_next_boundaries": boundaries,
        "calendar_days_until_boundary": days,
        "warn_lead_calendar_days": WARN_LEAD_CALENDAR_DAYS,
        "boundary_is_imminent": bool(days is not None
                                     and 0 <= days <= WARN_LEAD_CALENDAR_DAYS),
        "window_opens_at": b.get("window_opens_at"),
        "producer_last_run_utc": b.get("last_run_started_utc"),
        "producer_stale_after_hours": PRODUCER_STALE_AFTER_HOURS,
        "input_readiness": inputs,
        "chronic_miss": chronic,
        # R80 - the inputs to the entry-mark verdict, so a reader handed the verdict
        # can check it rather than take it.
        "accrual_state": acc_state,
        "accrual_blocker": acc_blocker,
        "entry_mark_unpriceable": entry_mark_unpriceable,
        "entry_mark_refusal_owner": (ACCRUAL_OWNER if entry_mark_unpriceable
                                     else None),
        "monitoring_owner": CALCULATION_OWNER,
        "emits_no_prediction": True,
        "changes_no_decision_contract": True,
    }

    # Eligible-session distance, but ONLY for a class whose sessions this estate
    # decides authoritatively. For anything else the honest answer is that the
    # instrument's own bar calendar owns it, and a weekday count would be a
    # fabrication of exactly the kind the registry refuses.
    out["eligible_sessions_until_boundary"] = None
    out["session_distance_owner"] = None
    try:
        from paper_trader.api import forward_challenger_registry as REG
        cal = REG.observation_calendar_for(acc.get("asset_class"))
        out["observation_calendar_owner"] = cal.get("calendar_owner")
        if cal.get("is_exchange_session_calendar") and nb_date:
            out["session_distance_owner"] = REG.COMPOSITION_OWNER
            cur, n = today.isoformat(), 0
            while n < 400:
                nxt = REG.next_exchange_session_after(cur)
                if not nxt or nxt > next_boundary:
                    break
                n += 1
                cur = nxt
                if nxt == next_boundary:
                    out["eligible_sessions_until_boundary"] = n
                    break
    except Exception as exc:                                # noqa: BLE001
        out["observation_calendar_owner"] = None
        out["session_distance_note"] = str(exc)[:160]

    # Producer silence, measured against the tolerance rather than guessed.
    last_run = b.get("last_run_started_utc")
    hours_silent = None
    lr = None
    if last_run:
        try:
            lr = datetime.fromisoformat(str(last_run).replace("Z", "+00:00"))
            if lr.tzinfo is None:
                lr = lr.replace(tzinfo=timezone.utc)
            hours_silent = (now - lr).total_seconds() / 3600.0
        except Exception:                                   # noqa: BLE001
            hours_silent = None
    out["producer_hours_since_last_run"] = (round(hours_silent, 2)
                                            if hours_silent is not None
                                            else None)
    stale = bool(hours_silent is not None
                 and hours_silent > PRODUCER_STALE_AFTER_HOURS)
    out["producer_is_stale"] = stale

    # ----------------------------------------------------------------------- #
    # The verdict. Ordered so the most structural answer wins: a window that
    # cannot be met is a worse fact than one input being late inside it, and
    # saying "inputs missing" every day about a contract that can never be
    # satisfied is how nine losses looked like nine accidents.
    # ----------------------------------------------------------------------- #
    def _v(state, why):
        return {**out, "readiness": state,
                "severity": READINESS_SEVERITY.get(state, SEV_WARN),
                "is_actionable_now": state in ACTIONABLE_READINESS_STATES,
                "why": why}

    if (prod.get("producer_state") == P_NONE and prod.get("reason")
            and not UNPRODUCED_IS_A_DEFECT.get(prod["reason"], True)):
        return _v(R_NOT_EXPECTED,
                  "no producer is expected for this registration (%s), so it "
                  "has no boundary to be ready for" % prod["reason"])
    # R80. The most structural answer of all, and it outranks the chronic verdict
    # because it EXPLAINS it: a registration whose declared entry mark its declared
    # valuation path cannot price will miss every boundary it is ever handed, and
    # no amount of input freshness or producer liveness changes that. Asked of the
    # ACCRUAL owner, which is the only owner entitled to refuse an entry mark.
    if entry_mark_unpriceable:
        return _v(R_ENTRY_MARK_INFEASIBLE,
                  "a boundary is declared at %s (%s calendar day(s) away) and the "
                  "accrual owner has refused this registration's declared entry "
                  "mark: %s (accrual state %s). The producer is live and its "
                  "inputs are in hand, so nothing about this boundary is late - "
                  "the entry instant itself cannot be priced by the declared "
                  "valuation path, and it will be missed like the %d before it "
                  "until a governed decision changes the entry contract or the "
                  "valuation path. Reported here, never repaired here."
                  % (next_boundary, days, acc_blocker or "unstated",
                     acc_state or "unstated",
                     len(b.get("missed_boundaries") or [])))
    if chronic.get("is_chronic"):
        # The structural verdict outranks the next boundary's own state, because
        # a contract that cannot be met most days is the fact worth acting on
        # even on a day it happens to be satisfiable. But the operator must not
        # be told "unreachable" about a boundary whose inputs are in hand right
        # now, so the next boundary's own readiness travels WITH the verdict.
        reachable = inputs["input_state"] == I_PRESENT and not stale
        out["next_boundary_inputs_in_hand"] = reachable
        return _v(R_CHRONIC,
                  "%s The NEXT boundary at %s is %s: %s"
                  % (chronic.get("why"), next_boundary,
                     ("REACHABLE (its inputs are already in hand, so this one "
                      "should be met)" if reachable else
                      "NOT currently reachable"),
                     inputs.get("why")))
    if prod.get("producer_state") != P_LIVE:
        if next_boundary:
            return _v(R_NO_PRODUCER,
                      "a boundary is declared at %s and no code path can decide "
                      "it: %s" % (next_boundary, prod.get("reason")))
        return _v(R_NO_PRODUCER,
                  "this registration has no producer and no declared boundary: "
                  "%s" % prod.get("reason"))
    if not next_boundary:
        return _v(R_NO_BOUNDARY,
                  "this producer declares no next boundary, so there is no "
                  "deadline to be ready for")
    if stale and out["boundary_is_imminent"]:
        return _v(R_PRODUCER_STALE,
                  "the boundary at %s is %d calendar day(s) away and the "
                  "producer stage has not run for %.1f hours (tolerance %dh); "
                  "the owner to look at is %s, not the data source"
                  % (next_boundary, days, hours_silent or 0.0,
                     PRODUCER_STALE_AFTER_HOURS, prod.get("producer_owner")))
    if inputs["input_state"] == I_MISSING:
        if out["boundary_is_imminent"]:
            return _v(R_INPUT_LATE,
                      "the boundary at %s is %d calendar day(s) away and the "
                      "inputs it needs are not in hand: %s"
                      % (next_boundary, days, inputs.get("why")))
        return _v(R_INPUT_PENDING,
                  "the boundary at %s is %s calendar day(s) away, beyond the %d "
                  "day warning lead, and its inputs are not yet in hand: %s"
                  % (next_boundary, days, WARN_LEAD_CALENDAR_DAYS,
                     inputs.get("why")))
    return _v(R_READY,
              "a live producer that ran %s, a declared next boundary at %s (%s "
              "calendar day(s) away) and inputs reported %s"
              % (last_run, next_boundary, days, inputs["input_state"]))


def lifecycle_state(accrual: dict, *, beat: Optional[dict] = None,
                    now: Optional[datetime] = None) -> dict:
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
             "last_advance_state", "blocked_on", "last_detail",
             "next_boundaries", "missed_boundaries",
             # R74.2 - and here is the allow-list a FOURTH time. These are the
             # facts pre-boundary readiness is computed from; a reader given the
             # verdict and not the inputs cannot check it.
             "publication", "entry_session", "information_session",
             "entry_state", "blocked_owner", "declared_grid_owner",
             "window_opens_at", "forward_panel_last_session",
             # R76 - collection ordering, and what collection reported.
             "acquisition_precedes_decision_state", "append_state")}
        # R68 - NEXT DUE DECISION, from the producer rather than the accrual.
        #
        # The accrual projection sets next_eligible_observation_session only for
        # a cell that is DUE or whose session has not been reached. A boundary
        # that is known, armed and AWAITING_NEW_GOVERNED_FREEZE is neither, so
        # the field reads None - and "the next decision is unknown" is a much
        # worse answer than the one the producer already has. The producer's own
        # boundary travels on its heartbeat and is reported here beside it,
        # never instead of it, so the two can be compared rather than confused.
        nb = beat.get("next_boundaries") or []
        if nb:
            out["next_boundary_from_producer"] = sorted(nb)[0]
            if not out.get("next_decision_session"):
                out["next_decision_session"] = sorted(nb)[0]
                out["next_decision_session_source"] = "PRODUCER_HEARTBEAT"

    # R72 - the producer's grid against the accrual's forfeiture count. Always
    # present, so a reader never has to infer from an absence whether the
    # question was asked.
    out["boundary_reconciliation"] = boundary_reconciliation(acc, beat,
                                                             producer=prod)

    # R74.2 - the PROSPECTIVE half, always present for the same reason the
    # retrospective half is: a reader must never have to infer from an absence
    # whether the question was asked. boundary_reconciliation says what was
    # lost; this says what is about to be.
    # R80 - ``now`` is threaded so the AGGREGATE ledger can be asked at a frozen
    # instant, exactly as the per-registration verdict already could. Without it
    # ``producer_coverage`` always read the wall clock while its fixtures pinned
    # absolute dates, so tests/test_release74_2_preboundary_monitoring.py's live-
    # shaped fixture drifted one day further from its own journal every day and
    # eventually reported the FX carry producer 27.6 hours stale against a boundary
    # that had become imminent. The fixture was right and the seam had no clock.
    out["preboundary_readiness"] = preboundary_readiness(acc, beat,
                                                         producer=prod, now=now)

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
    # R72 - a PRODUCER-DECLARED permanent miss counts here too, and it has the
    # same precedence a recorded forfeiture already had (a lost boundary
    # outranks an emission, which is the existing doctrine, not a new one).
    # Without this, a registration whose first and only boundary had already
    # passed unfrozen was reported ARMED_FOR_NEXT_DECISION, "its first boundary
    # has not come within reach" - a sentence that was false about
    # ALPHA_RECOVERY_FUTURES_TS_TREND_H21_V1 from 2026-09-21 onward.
    n_unrecorded = (out["boundary_reconciliation"]
                    .get("n_unrecorded_permanent_misses") or 0)
    if (stage_state in _MISSED_STAGE_STATES or (out["forfeitures_recorded"] or 0)
            or n_unrecorded):
        return {**out, "lifecycle": L_MISSED_GAP, "is_a_defect": False,
                "why": ("a decision window shut with no decision. The "
                        "opportunity is permanently gone and is never "
                        "backfilled; the producer itself is alive. %s"
                        % out["boundary_reconciliation"].get("why", ""))}
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
                      runs: Optional[dict] = None,
                      now: Optional[datetime] = None) -> dict:
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
        rows.append(lifecycle_state(acc, beat=beat, now=now))

    orphans = [r for r in rows if r["lifecycle"] == L_NOT_ARMED]
    failed = [r for r in rows if r["lifecycle"] == L_PRODUCER_FAILED]
    undeclared = [r for r in rows if r.get("reason") == UNDECLARED]
    # R72 - the forward estate's PERMANENT LOSSES, totalled. Reported separately
    # from the lifecycle histogram on purpose: a single lifecycle word per
    # registration cannot say how many boundaries each one has lost, and the
    # count is the whole measure of how much forward evidence this estate has
    # already forfeited without recording it.
    recon = [r.get("boundary_reconciliation") or {} for r in rows]
    no_grid = [r["challenger_id"] for r in rows
               if (r.get("boundary_reconciliation") or {}).get("boundary_state")
               == B_NO_GRID]
    n_declared = sum(int(x.get("n_producer_declared_missed") or 0)
                     for x in recon)
    n_recorded = sum(int(x.get("n_accrual_recorded_forfeitures") or 0)
                     for x in recon)
    n_unrecorded = sum(int(x.get("n_unrecorded_permanent_misses") or 0)
                       for x in recon)
    by_state = {}
    for r in rows:
        by_state[r["lifecycle"]] = by_state.get(r["lifecycle"], 0) + 1
    ok = not orphans and not failed and read_problem is None

    # R74.2 - THE PRE-BOUNDARY WARNING LEDGER. Counted separately from the
    # permanent-loss ledger beside it, because the two answer opposite
    # questions and merging them would let a warning about a boundary that can
    # still be met be read as a loss that has already happened.
    ready = [r.get("preboundary_readiness") or {} for r in rows]
    by_readiness = {}
    for x in ready:
        k = x.get("readiness")
        if k:
            by_readiness[k] = by_readiness.get(k, 0) + 1
    warnings = [
        {"challenger_id": r["challenger_id"],
         "readiness": (r.get("preboundary_readiness") or {}).get("readiness"),
         "severity": (r.get("preboundary_readiness") or {}).get("severity"),
         "next_boundary": (r.get("preboundary_readiness") or {}
                           ).get("next_boundary"),
         "calendar_days_until_boundary":
             (r.get("preboundary_readiness") or {}
              ).get("calendar_days_until_boundary"),
         "producer_owner": r.get("producer_owner"),
         "input_state": ((r.get("preboundary_readiness") or {}
                          ).get("input_readiness") or {}).get("input_state"),
         # WHICH leg is short, so the warning reaches the owner that can act on
         # it rather than blaming a vendor that already delivered.
         "short_leg": ((r.get("preboundary_readiness") or {}
                        ).get("input_readiness") or {}).get("short_leg"),
         "blocked_on": ((r.get("preboundary_readiness") or {}
                         ).get("input_readiness") or {}).get("blocked_on"),
         "why": (r.get("preboundary_readiness") or {}).get("why")}
        for r in rows
        if (r.get("preboundary_readiness") or {}).get("is_actionable_now")]
    warnings.sort(key=lambda w: (str(w.get("next_boundary") or "9999-12-31"),
                                 str(w.get("challenger_id"))))
    n_defect_warnings = sum(1 for w in warnings if w["severity"] == SEV_DEFECT)

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
        # R72 - THE PERMANENT LOSS LEDGER, read from the producers themselves.
        "boundary_state_vocabulary": list(BOUNDARY_STATES),
        "n_permanent_misses_declared_by_producers": n_declared,
        "n_permanent_misses_recorded_as_forfeitures": n_recorded,
        "n_permanent_misses_unrecorded": n_unrecorded,
        "every_producer_declares_a_boundary_grid": not no_grid,
        "producers_declaring_no_boundary_grid": sorted(no_grid),
        "permanent_misses_by_challenger": {
            r["challenger_id"]:
                (r.get("boundary_reconciliation") or {})
                .get("producer_declared_missed_boundaries") or []
            for r in rows
            if (r.get("boundary_reconciliation") or {})
            .get("producer_declared_missed_boundaries")},
        # R74.2 - WHAT IS ABOUT TO BE LOST, asked before the deadline.
        "readiness_vocabulary": list(READINESS_STATES),
        "readiness_severity": dict(READINESS_SEVERITY),
        "by_readiness_state": by_readiness,
        "warn_lead_calendar_days": WARN_LEAD_CALENDAR_DAYS,
        "producer_stale_after_hours": PRODUCER_STALE_AFTER_HOURS,
        "chronic_consecutive_misses_threshold": CHRONIC_CONSECUTIVE_MISSES,
        "n_preboundary_warnings": len(warnings),
        "n_preboundary_defects": n_defect_warnings,
        "preboundary_warnings": warnings,
        "every_next_boundary_is_reachable": not warnings,
        "heartbeat": beats,
        "registrations": rows,
        "read_only": True,
        "writes_nothing": True,
        "emits_no_prediction": True,
        "headline": _headline(rows, orphans, failed, read_problem,
                              n_unrecorded=n_unrecorded, no_grid=no_grid,
                              warnings=warnings),
    }


def _headline(rows, orphans, failed, read_problem, *, n_unrecorded: int = 0,
              no_grid=(), warnings=()) -> str:
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
    # R80 - an entry mark the accrual owner has REFUSED is not an executable
    # prediction path, whatever the producer registry says. This clause used to be
    # printed on the strength of orphans and producer failures alone, so the
    # headline asserted that every registration could predict while one of them
    # carried INTEGRITY_BLOCKED and had emitted nothing across two boundaries. The
    # unpriceable registrations are named in the warning clause below; the blanket
    # reassurance is simply not printed when one of them exists.
    unpriceable = sorted(r["challenger_id"] for r in rows
                         if (r.get("preboundary_readiness") or {}).get(
                             "readiness") == R_ENTRY_MARK_INFEASIBLE)
    if unpriceable:
        parts.append(
            "%d have a declared entry mark the accrual owner REFUSES as "
            "unpriceable by the declared valuation path, so they have no "
            "executable prediction path until a governed decision changes one of "
            "the two: %s." % (len(unpriceable), ", ".join(unpriceable)))
    if not orphans and not failed and not unpriceable:
        parts.append("Every one has a declared, executable prediction path.")
    if n_unrecorded:
        parts.append(
            "%d decision boundary(ies) on the producers' OWN grids passed with "
            "no frozen decision and no recorded forfeiture: that forward "
            "evidence does not exist and never will." % n_unrecorded)
    if no_grid:
        parts.append("%d producer(s) declare no boundary grid, so their losses "
                     "cannot be counted: %s."
                     % (len(no_grid), ", ".join(sorted(no_grid))))
    # R74.2 - the warning goes in the headline, because a warning nobody reads
    # before the deadline is indistinguishable from no warning at all.
    if warnings:
        parts.append(
            "%d registration(s) CANNOT MEET their next declared boundary unless "
            "something changes first: %s."
            % (len(warnings),
               "; ".join("%s (%s, boundary %s)"
                         % (w.get("challenger_id"), w.get("readiness"),
                            w.get("next_boundary")) for w in warnings)))
    else:
        parts.append("Every next boundary is reachable with the inputs and "
                     "producers in hand.")
    return " ".join(parts)


__all__ = [
    "BOUNDARY_STATES", "B_AGREED", "B_UNRECORDED_MISS", "B_NO_GRID",
    "B_NOT_EXPECTED", "boundary_reconciliation",
    "SCHEMA_VERSION", "CALCULATION_OWNER", "LIFECYCLE_STATES", "LIVE_STATES",
    "DEFECT_STATES", "L_NOT_ARMED", "L_ARMED", "L_ACCRUING", "L_AWAITING_DATA",
    "L_MISSED_GAP", "L_PRODUCER_FAILED", "L_SUPERSEDED", "L_RETIRED",
    "RUNTIME_PRODUCER_STAGES", "STAGE_OWNERS", "KNOWN_UNPRODUCED",
    "UNPRODUCED_IS_A_DEFECT", "UNPRODUCED_DETAIL", "UNDECLARED",
    "PRODUCER_STATES", "P_LIVE", "P_NONE", "P_UNKNOWN",
    "producer_for", "heartbeat", "lifecycle_state", "producer_coverage",
    # R74.2 - pre-boundary readiness: the prospective half of the ledger.
    "READINESS_STATES", "ACTIONABLE_READINESS_STATES", "READINESS_SEVERITY",
    "R_READY", "R_INPUT_PENDING", "R_INPUT_LATE", "R_PRODUCER_STALE",
    "R_NO_PRODUCER", "R_CHRONIC", "R_NO_BOUNDARY", "R_NOT_EXPECTED",
    "R_ENTRY_MARK_INFEASIBLE", "UNPRICEABLE_ENTRY_MARK_BLOCKER",
    "ACCRUAL_INTEGRITY_BLOCKED",
    "SEV_OK", "SEV_WARN", "SEV_DEFECT",
    "INPUT_STATES", "I_PRESENT", "I_MISSING", "I_NOT_DECLARED",
    "INPUT_LEGS", "L_LEG_VENDOR", "L_LEG_LOCAL",
    "WARN_LEAD_CALENDAR_DAYS", "PRODUCER_STALE_AFTER_HOURS",
    "CHRONIC_CONSECUTIVE_MISSES",
    "input_readiness", "chronic_miss", "preboundary_readiness",
]
