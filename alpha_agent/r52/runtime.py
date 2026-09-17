"""alpha_agent.r52.runtime - ONE orchestration owner for prospective research.

``research_runtime_cycle()`` is the single path a scheduled invocation takes.
It coordinates canonical owners and calculates nothing itself:

1.  hold the R52 runtime lock (one runtime instance at a time; a second
    trigger firing mid-run is refused, reported, and harmless);
2.  derive the timing contract (:mod:`alpha_agent.r52.timing_contract`) and
    the emission policy for THIS instant;
3.  verify the shared evidence chains BEFORE writing anything - a broken
    chain fails the run CLOSED (no emission, no scoring, a loud health row);
4.  run the ONE tournament step (:func:`alpha_agent.r46.advance.advance`) -
    lanes, scoring, boards, money layer, continuation, emission - with the
    batch emission gated by the derived policy, under the campaign lock the
    advance itself now holds;
5.  sweep forfeitures (:mod:`alpha_agent.r52.forfeiture`);
6.  rebuild operational evidence velocity (:mod:`alpha_agent.r52.velocity_ops`);
7.  refresh the R51 promotion frontier (:mod:`alpha_agent.r52.frontier_refresh`);
8.  write the ONE runtime health read model and append the run journal row.

Every stage resolves to exactly one structured state; one lane's failure
never invalidates an independent lane (the advance already isolates its
stages, and the runtime isolates its own).

RESEARCH ONLY. This function cannot run the portfolio cycle, the daily
close, a rebalance, an approval or a promotion; it holds no HTTP client and
imports no operational write path.
"""
from __future__ import annotations

import datetime as _dt

from . import (ACCOUNTABILITY_START_DATE, RELEASE, artifact_body, read_json,
               runtime_dir, write_json)
from . import forfeiture as FF
from . import frontier_refresh as FR
from . import timing_contract as TC
from . import velocity_ops as VO
from ..r46 import CAMPAIGN_ID
from ..r46 import adopted_forward as AF
from ..r46 import clock as CK
from ..r46 import ledger as LG
from ..r46 import runlock as RL

CALCULATION_OWNER = "alpha_agent.r52.runtime"

RUNTIME_LOCK_NAME = "r52_runtime.lock"
RUN_JOURNAL = "runtime_runs.json"
HEALTH_ARTIFACT = "runtime_health.json"

#: Frozen stage-state vocabulary (section 14 of the release).
SUCCESS = "SUCCESS"
NOT_DUE = "NOT_DUE"
PIT_BLOCKED = "PIT_BLOCKED"
DATA_BLOCKED = "DATA_BLOCKED"
FORFEITED = "FORFEITED"
FAILED_RETRYABLE = "FAILED_RETRYABLE"
FAILED_INTEGRITY = "FAILED_INTEGRITY"
STAGE_STATES = (SUCCESS, NOT_DUE, PIT_BLOCKED, DATA_BLOCKED, FORFEITED,
                FAILED_RETRYABLE, FAILED_INTEGRITY)

#: Runtime-level states.
RUN_COMPLETED = "RUN_COMPLETED"
RUN_COMPLETED_WITH_FAILURES = "RUN_COMPLETED_WITH_FAILURES"
RUN_FAILED_INTEGRITY = "RUN_FAILED_INTEGRITY"
RUN_REFUSED_CONCURRENT = "RUN_REFUSED_CONCURRENT"
RUN_STATES = (RUN_COMPLETED, RUN_COMPLETED_WITH_FAILURES,
              RUN_FAILED_INTEGRITY, RUN_REFUSED_CONCURRENT)

_KEEP_RUNS = 400


def _lock_file():
    return runtime_dir() / RUNTIME_LOCK_NAME


def _stage(name: str, state: str, **extra) -> dict:
    return {"stage": name, "state": state, **extra}


def _chains_ok() -> dict:
    """Verify every shared evidence chain this runtime can touch."""
    reports = {}
    ok = True
    for name, fn in (("r46_forward", lambda: LG.verify(CAMPAIGN_ID)),
                     ("adopted_continuation", AF.verify),
                     ("r52_forfeiture", FF.verify)):
        try:
            rep = fn()
            reports[name] = {"all_intact": bool(rep.get("all_intact")),
                             "ledgers": rep.get("ledgers")}
            ok = ok and bool(rep.get("all_intact"))
        except Exception as exc:          # noqa: BLE001 - a broken verifier
            reports[name] = {"all_intact": False,
                             "error": "%s: %s" % (type(exc).__name__,
                                                  str(exc)[:160])}
            ok = False
    return {"all_intact": ok, "chains": reports}


def _lifecycle_by_challenger() -> dict:
    """``challenger_id -> the ONE lifecycle owner's CURRENT verdict``.

    A challenger can be withdrawn or invalidated AFTER it was registered, and a
    registration carries only the verdict that was true at adoption. This asks
    the canonical owner again, from persisted history, so a closed lifecycle
    stops the accrual at the next invocation rather than at the next release.

    Degrades to ``{}``, which means "no current verdict was established" - the
    accrual owner then reports ``lifecycle_rechecked: false`` rather than
    silently assuming the challenger is still active.
    """
    try:
        from paper_trader.alpha_agent import r59
        from paper_trader.alpha_agent.r59 import memory as M
        from paper_trader.api import prospective_adoption as PA
        mem = M.open_memory_readonly()
        out = {}
        for row in mem.list_hypotheses(outcome=r59.HO_FORWARD_FROZEN,
                                       limit=5000):
            cid = PA.freeze_challenger_id(row)
            if cid:
                out[str(cid)] = PA.classify_lifecycle(row)
        return out
    except Exception:                     # noqa: BLE001 - never block accrual
        return {}


def research_runtime_cycle(now: _dt.datetime = None, *,
                           campaign_id: str = CAMPAIGN_ID,
                           trigger: str = "MANUAL",
                           emit_override: str = None) -> dict:
    """One scheduled research invocation. Idempotent; never backdates.

    ``emit_override``: ``None`` (policy decides), ``"NEVER"`` (sweep-only
    invocation), never a force - there is no override that emits when the
    canonical owners refuse.
    """
    started = now or CK.now_utc()
    run_id = "r52run_" + started.strftime("%Y%m%dT%H%M%SZ")
    stages: list = []

    # --- 1. one runtime instance at a time --------------------------------- #
    holder = "r52_runtime:%s" % run_id
    try:
        lock = RL.acquire_path(_lock_file(), holder, wait_s=0,
                               stale_after_s=2 * 3600)
    except RL.AdvanceLockBusy as exc:
        body = _run_body(run_id, RUN_REFUSED_CONCURRENT, started, trigger,
                         [_stage("runtime_lock", FAILED_RETRYABLE,
                                 detail=str(exc)[:220])],
                         concurrent_holder=RL.state_path(_lock_file()))
        _journal(body)
        return body
    stages.append(_stage("runtime_lock", SUCCESS,
                         reclaimed=lock.get("reclaimed_stale")))

    advance_result = None
    forf = None
    canon = None
    s25 = None
    vel = None
    frontier = None
    integrity = None
    contract = None
    policy = None
    try:
        # --- 2. the timing contract, derived fresh ------------------------- #
        try:
            contract = TC.build(started)
            policy = contract.get("emission_policy_now") or {}
            stages.append(_stage("timing_contract", SUCCESS,
                                 emission_mode=policy.get("mode")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("timing_contract", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))
            policy = {"emit": False, "mode": "POLICY_UNAVAILABLE"}

        # --- 3. shared integrity, BEFORE any write ------------------------- #
        integrity = _chains_ok()
        if not integrity["all_intact"]:
            stages.append(_stage("chain_integrity", FAILED_INTEGRITY,
                                 chains=integrity["chains"]))
            body = _run_body(run_id, RUN_FAILED_INTEGRITY, started, trigger,
                             stages, integrity=integrity,
                             fail_closed=True,
                             nothing_was_written=True)
            _journal(body)
            _write_health(body, contract, None, None, None, integrity)
            return body
        stages.append(_stage("chain_integrity", SUCCESS))

        # --- 4. the ONE tournament step ------------------------------------ #
        emit_batch = bool(policy.get("emit")) and emit_override != "NEVER"
        try:
            from ..r46 import advance as AD
            advance_result = AD.advance(campaign_id, now=started,
                                        emit_batch=emit_batch,
                                        lock_holder=holder)
            st = str(advance_result.get("state"))
            if st == AD.STATE_ADVANCED:
                a_state = SUCCESS
            elif st == AD.STATE_NOTHING_DUE:
                a_state = NOT_DUE
            else:
                a_state = FAILED_RETRYABLE
            if advance_result.get("concurrent_run_refused"):
                a_state = FAILED_RETRYABLE
            stages.append(_stage(
                "tournament_advance", a_state,
                advance_state=st,
                emit_batch_requested=emit_batch,
                emission_mode=policy.get("mode"),
                outcomes_scored=advance_result.get(
                    "tournament_outcomes_scored"),
                predictions_emitted=advance_result.get(
                    "tournament_predictions_emitted"),
                duplicates_skipped=(advance_result.get("emission") or {})
                .get("n_duplicates_skipped"),
                n_stage_failures=advance_result.get("n_stage_failures")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("tournament_advance", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 5. forfeitures become first-class state ----------------------- #
        try:
            forf = FF.sweep(started, scheduler_state=trigger)
            n_new = int(forf.get("n_appended") or 0)
            stages.append(_stage("forfeiture_sweep",
                                 FORFEITED if n_new else SUCCESS,
                                 n_new_forfeitures=n_new,
                                 n_total=forf.get("n_total_forfeitures")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("forfeiture_sweep", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 5a2. the originating owner freezes its per-session decision --- #
        # R62.3.3. The accrual owner in 5b advances a MEASUREMENT and says so
        # plainly: "a decision is the originating owner's act and this module
        # may not take one on its behalf". For a release that freezes one
        # decision PER SESSION rather than one at adoption, that leaves a real
        # gap - nothing would ever call the research owner, and every entry
        # session would be MISSED while the registration looked merely young.
        # So the research owner is asked FIRST, inside this same lock and this
        # same cadence, and its answer is a stage like any other. No second
        # scheduler, and no decision taken by anyone but the owner of the rule.
        #
        # The call is idempotent end to end: the publication poll is
        # append-only, the surface append refuses a session it already holds,
        # and the freeze is first-write-wins. Firing it repeatedly inside the
        # nine-and-a-half-hour window is how the window is covered by a
        # schedule instead of by a person.
        try:
            from ..alpha_recovery import next_open_runtime as NOR
            adv = NOR.advance_daily(now=started.isoformat())
            a_st = str(adv.get("state"))
            if a_st == NOR.ADV_FROZEN:
                n_state = SUCCESS
            elif a_st == NOR.ADV_MISSED:
                n_state = FORFEITED
            elif a_st == NOR.ADV_AWAITING_SOURCE:
                n_state = DATA_BLOCKED
            elif a_st == NOR.ADV_BLOCKED:
                n_state = FAILED_RETRYABLE
            else:
                n_state = NOT_DUE
            stages.append(_stage(
                "next_open_prospective_decision", n_state,
                challenger_id=adv.get("challenger_id"),
                advance_state=a_st,
                information_session=adv.get("information_session"),
                entry_session=adv.get("entry_session"),
                entry_state=adv.get("entry_state"),
                publication=(adv.get("publication") or {}).get("outcome"),
                append_state=(adv.get("append") or {}).get("state"),
                paid_dollars=adv.get("paid_dollars"),
                frozen=bool((adv.get("freeze") or {}).get("frozen")),
                detail=adv.get("detail")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("next_open_prospective_decision",
                                 FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 5a3. the FX carry cadence owner freezes its boundary decision - #
        # ALPHA_RECOVERY_FX_CARRY_CADENCE_H1_F9B1ACA7 was registered for
        # TRUE_FORWARD evidence and accrued nothing, because the accrual stage
        # below may not take a decision on the research owner's behalf and no
        # one asked that owner. It is asked here, inside this lock and on this
        # cadence, AFTER the next-open owner and BEFORE the accrual: no second
        # scheduler. The call is idempotent (first-write-wins freeze), never
        # backfills, spends nothing and refreshes owned vendor settlements only
        # on the live clock.
        try:
            from ..alpha_recovery import fx_carry_cadence_runtime as FXR
            fx = FXR.advance(now=started.isoformat())
            fx_st = str(fx.get("state"))
            if fx_st in FXR.PROGRESS_STATES:
                fx_state = SUCCESS
            elif fx_st in FXR.MISSED_STATES:
                fx_state = FORFEITED
            elif fx_st in FXR.DATA_WAIT_STATES:
                fx_state = DATA_BLOCKED
            elif fx_st in FXR.FAILURE_STATES:
                fx_state = FAILED_RETRYABLE
            else:
                fx_state = NOT_DUE
            stages.append(_stage(
                "fx_carry_cadence_prospective_decision", fx_state,
                challenger_id=fx.get("challenger_id"),
                advance_state=fx_st,
                entry_session=fx.get("entry_session"),
                newest_published_session=fx.get("newest_published_session"),
                marks_refreshed=bool((fx.get("marks_refresh") or {}).get("ran")),
                paid_dollars=fx.get("paid_dollars"),
                frozen=fx_st in FXR.PROGRESS_STATES,
                detail=fx.get("detail")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("fx_carry_cadence_prospective_decision",
                                 FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 5a4. the FROZEN Stage-26 book gets its host back -------------- #
        # s25_operating_profitability was frozen on 2026-08-16 and accrued
        # ZERO marks, because the only production host of its mark producer -
        # the AlphaAgent-Collect task - was disabled THIRTEEN DAYS BEFORE the
        # book existed, and this runtime never inherited the responsibility.
        # The producer was never broken; its host was retired underneath it.
        #
        # So the responsibility is re-parented here, on this cadence and under
        # this lock, exactly like the two adapter stages above: the retired
        # task stays disabled, no second scheduler appears, and the mark is
        # still written by alpha_agent.tournament.advance_shadow_books into the
        # SAME shadow book under the SAME frozen h63 clock. The stage marks AT
        # MOST ONE session per invocation, never catches up over sessions it
        # missed, and refuses the 21 permanently forfeited ones outright.
        #
        # It fails closed by default: with no governed re-arm authorisation on
        # the store it reports AWAITING_ACTIVATION and writes nothing.
        try:
            from .. import stage26_forward_runtime as S26F
            s25 = S26F.advance(now=started.isoformat())
            s25_st = str(s25.get("state"))
            if s25_st in S26F.PROGRESS_STATES:
                s25_state = SUCCESS
            elif s25_st in S26F.BLOCKED_STATES:
                s25_state = DATA_BLOCKED
            elif s25_st in S26F.INTEGRITY_STATES:
                s25_state = FAILED_INTEGRITY
            elif s25_st == S26F.STATE_FAILED:
                s25_state = FAILED_RETRYABLE
            else:
                s25_state = NOT_DUE
            stages.append(_stage(
                "stage26_prospective_mark", s25_state,
                challenger_id=s25.get("challenger_id"),
                advance_state=s25_st,
                original_inception=s25.get("original_inception"),
                prospective_epoch_floor=s25.get(
                    "prospective_epoch_floor_session"),
                effective_prospective_epoch_floor=s25.get(
                    "effective_prospective_epoch_floor_session"),
                evidence_session=s25.get("evidence_session"),
                marks_before=s25.get("marks_before"),
                valid_marks=s25.get("valid_marks_before"),
                quarantined_marks=s25.get("quarantined_marks"),
                marks_written=s25.get("marks_written_this_run"),
                sessions_skipped=s25.get("n_sessions_skipped"),
                h63_maturity_rule=s25.get("h63_maturity_rule"),
                backfill=s25.get("backfill"),
                detail=s25.get("detail")))
        except Exception as exc:          # noqa: BLE001
            s25 = None
            stages.append(_stage("stage26_prospective_mark", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 5b. the canonical prospective registrations advance ----------- #
        # R62.2. A registration made by the canonical registrar names its
        # accrual owner and its maturation owner, and before this stage nothing
        # called either: the four R58 challengers adopted on 2026-09-09 were
        # clocks nothing wound. This runtime already owns the research cadence,
        # so the accrual becomes one more stage here rather than a second
        # scheduler. It emits only what is legally due, records what was
        # genuinely missed, and matures what has completed its horizon.
        try:
            from paper_trader.api import canonical_forward_accrual as CFA
            canon = CFA.advance_canonical_forward_accrual(
                now=started, lifecycle_by_challenger=_lifecycle_by_challenger())
            if canon.get("n_blocked"):
                c_state = DATA_BLOCKED
            elif canon.get("n_forfeitures_recorded_this_run"):
                c_state = FORFEITED
            elif canon.get("n_emitted_this_run"):
                c_state = SUCCESS
            elif canon.get("n_registered"):
                c_state = NOT_DUE
            else:
                c_state = NOT_DUE
            stages.append(_stage(
                "canonical_forward_accrual", c_state,
                registered=canon.get("n_registered"),
                due_now=canon.get("n_due_now"),
                emitted=canon.get("n_emitted_this_run"),
                duplicates_skipped=canon.get("n_duplicates_skipped"),
                forfeited=canon.get("n_forfeitures_recorded_this_run"),
                blocked=canon.get("n_blocked"),
                armed_for_a_future_session=canon.get(
                    "n_armed_for_a_future_session"),
                matured_total=canon.get("matured_observations_total"),
                effective_independent_observations=canon.get(
                    "effective_independent_observations_total")))
        except Exception as exc:          # noqa: BLE001
            canon = None
            stages.append(_stage("canonical_forward_accrual", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 6. operational velocity --------------------------------------- #
        try:
            vel = VO.build(started, campaign_id=campaign_id)
            stages.append(_stage("velocity_operational", SUCCESS))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("velocity_operational", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        # --- 7. the promotion frontier stays current ----------------------- #
        try:
            frontier = FR.refresh(started, campaign_id=campaign_id)
            stages.append(_stage(
                "promotion_frontier", SUCCESS,
                promotion_ready_count=frontier.get("promotion_ready_count"),
                transitions=frontier.get("packet_state_transitions")))
        except Exception as exc:          # noqa: BLE001
            stages.append(_stage("promotion_frontier", FAILED_RETRYABLE,
                                 error=type(exc).__name__,
                                 detail=str(exc)[:220]))

        failed = [s for s in stages
                  if s["state"] in (FAILED_RETRYABLE, FAILED_INTEGRITY)]
        state = (RUN_COMPLETED_WITH_FAILURES if failed else RUN_COMPLETED)
        body = _run_body(run_id, state, started, trigger, stages,
                         integrity=integrity,
                         advance=_advance_digest(advance_result),
                         forfeitures={
                             "n_new": (forf or {}).get("n_appended"),
                             "n_total": (forf or {}).get(
                                 "n_total_forfeitures")},
                         emission_policy=policy,
                         canonical_forward_accrual=_canonical_digest(canon),
                         stage26_prospective_mark=_stage26_digest(s25),
                         promotion_ready_count=(frontier or {}).get(
                             "promotion_ready_count"))
        _journal(body)
        _write_health(body, contract, advance_result, forf, frontier,
                      integrity, velocity=vel, canonical=canon, stage26=s25)
        return body
    finally:
        RL.release_path(_lock_file(), holder)


# --------------------------------------------------------------------------- #
def _advance_digest(a) -> dict:
    if not a:
        return {"state": "NOT_RUN"}
    return {k: a.get(k) for k in (
        "state", "tournament_outcomes_scored",
        "tournament_predictions_emitted",
        "tournament_forward_evidence_count", "pending_predictions",
        "tournament_challengers_active", "n_stage_failures",
        "ledger_chain_intact", "pnl_as_of")}


def _canonical_digest(c) -> dict:
    """What the canonical accrual stage did, in the run journal's own terms."""
    if not c:
        return {"state": "NOT_RUN"}
    return {k: c.get(k) for k in (
        "n_registered", "n_due_now", "n_emitted_this_run",
        "n_duplicates_skipped", "n_forfeitures_recorded_this_run", "n_blocked",
        "n_armed_for_a_future_session", "predictions_emitted_total",
        "matured_observations_total",
        "effective_independent_observations_total", "forfeitures_total")}


def _stage26_digest(s) -> dict:
    """What the re-armed Stage-26 forward stream looks like, in every run.

    This block is deliberately UNCONDITIONAL. The reason S25's clock could stop
    for 31 days unnoticed is that no read model this runtime writes had to say
    anything about it: an owner nothing reads reports nothing, so silence looked
    like health. A stalled or unauthorised stream now has to say so by name in
    the artifact the running runtime produces every cycle.
    """
    if not s:
        return {"state": "NOT_RUN",
                "why": "the stage did not run in this cycle"}
    return {k: s.get(k) for k in (
        "state", "challenger_id", "strategy_name", "shadow_book_id",
        "original_inception", "prospective_epoch_floor_session",
        # The EFFECTIVE floor is reported beside the stored one, because the
        # immutable activation record carries a floor that is no longer the one
        # in force and a read model that published only the stored value would
        # keep restating the defect it was corrected for.
        "effective_prospective_epoch_floor_session", "epoch_floor_was_corrected",
        "session_boundary_owner", "calendar_owner",
        "evidence_session", "threshold_session", "panel_latest_session",
        # RAW beside GOVERNED, never one in place of the other.
        "marks_before", "marks_after", "raw_marks", "valid_marks_before",
        "valid_marks_after", "quarantined_marks", "quarantined_mark_dates",
        "quarantine_class", "quarantine_reason",
        "marks_written_this_run",
        "n_sessions_skipped", "identity_verified", "h63_maturity_rule",
        "governance_decision", "forfeited_sessions", "backfill", "reason",
        "detail")}


def _run_body(run_id: str, state: str, started: _dt.datetime, trigger: str,
              stages: list, **extra) -> dict:
    return artifact_body(
        "r52_runtime_run/1", CALCULATION_OWNER,
        run_id=run_id,
        state=state,
        state_vocabulary=list(RUN_STATES),
        stage_state_vocabulary=list(STAGE_STATES),
        trigger=trigger,
        started_utc=CK.iso(started),
        started_utc_precise=CK.iso_precise(started),
        finished_utc=CK.iso(CK.now_utc()),
        stages=stages,
        calls_portfolio_cycle=False,
        runs_daily_close=False,
        promotes_models=False,
        backfills=False,
        **extra)


def _journal(body: dict) -> None:
    p = runtime_dir() / RUN_JOURNAL
    prior = read_json(p, default=None) or {}
    runs = list(prior.get("runs") or [])
    runs.append({k: body.get(k) for k in (
        "run_id", "state", "trigger", "started_utc", "finished_utc",
        "promotion_ready_count")}
        | {"stages": [{"stage": s.get("stage"), "state": s.get("state")}
                      for s in (body.get("stages") or ())]})
    kept = runs[-_KEEP_RUNS:]
    write_json(p, artifact_body(
        "r52_runtime_runs/1", CALCULATION_OWNER,
        n_runs_total=int(prior.get("n_runs_total") or 0) + 1,
        n_runs_retained=len(kept),
        latest_run=kept[-1] if kept else None,
        runs=kept))


def _next_invocation(now: _dt.datetime) -> dict:
    et = CK.to_eastern(now)
    times = sorted(t["local_time"] for t in TC.INVOCATION_PLAN)
    for t in times:
        hh, mm = t.split(":")
        cand = et.replace(hour=int(hh), minute=int(mm), second=0,
                          microsecond=0)
        if cand > et:
            return {"local_time": t, "date": str(et.date())}
    first = times[0]
    return {"local_time": first,
            "date": str(et.date() + _dt.timedelta(days=1))}


def _write_health(run_body: dict, contract, advance_result, forf, frontier,
                  integrity, velocity=None, canonical=None,
                  stage26=None) -> None:
    now = CK.now_utc()
    prior = read_json(runtime_dir() / HEALTH_ARTIFACT, default=None) or {}
    a = advance_result or {}
    emission = (a.get("emission") or {})
    vel_d = (a.get("evidence_velocity") or {})
    shadow = (a.get("shadow_pnl") or {})
    lanes = (a.get("lanes") or {})
    lane_counts = {"due": 0, "advanced": 0, "not_due": 0, "blocked": 0,
                   "failed": 0}
    for row in lanes.values():
        st = str(row.get("state"))
        if st == "CALLED_AND_EMITTED":
            lane_counts["advanced"] += 1
            lane_counts["due"] += 1
        elif st == "CALLED_QUIET_NOT_DUE":
            lane_counts["not_due"] += 1
        elif st in ("CALLED_DATA_BLOCKED", "CALLED_PIT_BLOCKED",
                    "CALLED_SAMPLE_BLOCKED"):
            lane_counts["blocked"] += 1
            lane_counts["due"] += 1
        elif st == "RETIRED":
            pass
        else:
            lane_counts["failed"] += 1
    ok_states = (RUN_COMPLETED,)
    last_successful = (run_body.get("finished_utc")
                       if run_body.get("state") in ok_states
                       else prior.get("last_successful"))
    body = artifact_body(
        "r52_runtime_health/1", CALCULATION_OWNER,
        current_time_utc=CK.iso(now),
        latest_eligible_session=str(TC.owned_last_session() or ""),
        runtime_state=run_body.get("state"),
        last_run_id=run_body.get("run_id"),
        last_trigger=run_body.get("trigger"),
        last_started=run_body.get("started_utc"),
        last_completed=run_body.get("finished_utc"),
        last_successful=last_successful,
        next_expected_invocation=_next_invocation(now),
        # ---- predictions ---------------------------------------------- #
        predictions_due=emission.get("n_offered"),
        next_entry_session=(contract or {}).get("emission_policy_now", {})
        .get("entry_session_date"),
        predictions_emitted=a.get("tournament_predictions_emitted"),
        duplicates_skipped=emission.get("n_duplicates_skipped"),
        predictions_forfeited=(forf or {}).get("n_total_forfeitures"),
        forfeited_cells_total=(forf or {}).get("n_cells_lost_total"),
        # ---- outcomes -------------------------------------------------- #
        outcomes_due=a.get("pending_predictions"),
        outcomes_scored=a.get("tournament_outcomes_scored"),
        # ---- lanes ----------------------------------------------------- #
        lanes_due=lane_counts["due"],
        lanes_advanced=lane_counts["advanced"],
        lanes_not_due=lane_counts["not_due"],
        lanes_blocked=lane_counts["blocked"],
        lanes_failed=lane_counts["failed"],
        # ---- integrity / frontier -------------------------------------- #
        forward_chain_integrity=(integrity or {}).get("all_intact"),
        promotion_frontier_state=("CURRENT" if frontier else
                                  prior.get("promotion_frontier_state")),
        promotion_ready_count=(frontier or {}).get("promotion_ready_count",
                                                   prior.get(
                                                     "promotion_ready_count")),
        active_challenger_cells=a.get("tournament_challengers_active"),
        effective_independent_observations=vel_d.get(
            "effective_independent_observations"),
        projected_effective_per_week=vel_d.get(
            "projected_effective_per_week"),
        research_shadow_nav=shadow.get("shadow_nav"),
        residual_alpha_vs_cash=shadow.get("residual_alpha_pnl_vs_cash"),
        # ---- canonical prospective registrations (R62.2) ---------------- #
        # Kept as its OWN block rather than folded into the R46 counters: an
        # R46 contract-cohort prediction and a canonical registration's
        # prospective emission are different evidence identities, and summing
        # them is exactly the mistake R62.2 exists to prevent.
        canonical_forward=_canonical_digest(canonical),
        canonical_forward_registered=(canonical or {}).get("n_registered"),
        canonical_forward_armed=(canonical or {}).get(
            "n_armed_for_a_future_session"),
        canonical_forward_effective_independent_observations=(
            (canonical or {}).get("effective_independent_observations_total")),
        # ---- the re-armed Stage-26 forward stream ----------------------- #
        # Its OWN block, and always present. This stream's marks live in the
        # legacy shadow book under the frozen h63 mark-count clock, so folding
        # them into either counter above would sum two different evidence
        # identities. It is reported even when the stage did not run, because
        # an owner nobody has to mention is an owner that can stop unnoticed -
        # which is precisely how this book lost 21 collectable sessions.
        stage26_prospective_mark=_stage26_digest(stage26),
        stage26_stream_state=(stage26 or {}).get("state", "NOT_RUN"),
        # RAW row count, named as raw. It is NOT the evidence count.
        stage26_marks=(stage26 or {}).get("marks_after",
                                          (stage26 or {}).get("marks_before")),
        stage26_raw_marks=(stage26 or {}).get("raw_marks"),
        # The GOVERNED count: marks strictly after the EFFECTIVE epoch. A health
        # reader that sums forward evidence must sum this one.
        stage26_valid_marks=(stage26 or {}).get(
            "valid_marks_after", (stage26 or {}).get("valid_marks_before")),
        stage26_quarantined_marks=(stage26 or {}).get("quarantined_marks"),
        stage26_effective_epoch_floor=(stage26 or {}).get(
            "effective_prospective_epoch_floor_session"),
        stage26_epoch_floor_was_corrected=(stage26 or {}).get(
            "epoch_floor_was_corrected"),
        stage26_forward_backfill_forbidden=True,
        accountability_start_date=ACCOUNTABILITY_START_DATE,
        runtime_lock=RL.state_path(_lock_file()),
        advance_lock=RL.state(),
        release=RELEASE,
    )
    write_json(runtime_dir() / HEALTH_ARTIFACT, body)


def load_health() -> dict:
    return read_json(runtime_dir() / HEALTH_ARTIFACT, default={}) or {}


def load_runs() -> dict:
    return read_json(runtime_dir() / RUN_JOURNAL, default={}) or {}


__all__ = ["CALCULATION_OWNER", "RUNTIME_LOCK_NAME", "RUN_JOURNAL",
           "HEALTH_ARTIFACT", "STAGE_STATES", "RUN_STATES", "SUCCESS",
           "NOT_DUE", "PIT_BLOCKED", "DATA_BLOCKED", "FORFEITED",
           "FAILED_RETRYABLE", "FAILED_INTEGRITY", "RUN_COMPLETED",
           "RUN_COMPLETED_WITH_FAILURES", "RUN_FAILED_INTEGRITY",
           "RUN_REFUSED_CONCURRENT", "research_runtime_cycle",
           "load_health", "load_runs"]
