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
                         promotion_ready_count=(frontier or {}).get(
                             "promotion_ready_count"))
        _journal(body)
        _write_health(body, contract, advance_result, forf, frontier,
                      integrity, velocity=vel)
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
                  integrity, velocity=None) -> None:
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
