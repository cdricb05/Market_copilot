"""alpha_agent.r46.advance - ONE tournament step, driven by the daily cycle.

Release 46 built the prospective tournament and emitted its first TRUE_FORWARD
batch. It could only ever be advanced by running the whole campaign by hand, so
the eleven predictions on the record would have matured with nobody scoring
them - which is precisely the failure Release 46 was created to end (five
releases froze seven shadow registries and never called their capture owner
again).

This module is the step the canonical Daily Research Cycle calls. It is NOT a
second tournament: the ledger, the registry, the judge, the emitter and the
leaderboard all stay exactly where Release 46 put them, and every write still
goes through them.

THE ORDER IS THE CONTRACT
-------------------------
1. **Resolve** the registry as it was frozen. Never re-freeze it: re-running
   ``registry.register`` would re-probe feasibility and rewrite the artifact,
   and the cohort's identity is evidence.
2. **Score** everything whose declared horizon has genuinely matured on the
   instrument's OWN realised bar calendar - BEFORE anything new is emitted, so
   a run can never be accused of having seen its own new outcome.
3. **Rebuild the board** on the evidence that now exists.
4. **Emit** the next eligible batch, idempotently, from data available at the
   emission instant.
5. **Rebuild the board** so the emitted counts are current.

Step 3 and step 5 both run because the board is a pure function of the ledgers
and the two answer different questions: step 3 is "what does the evidence say",
step 5 is "what is now outstanding". Doing only the second would report the new
batch on a board built before the outcomes it must not have seen.

WHAT IT NEVER DOES
------------------
Writes only into the Release-46 research root. Touches no operational store, no
portfolio, no target, no proposal, no order, no scheduler and no prior release's
artifacts. Promotes nothing: ``FORWARD_CONFIRMED`` remains research evidence that
a person must act on deliberately. Rewrites no prediction row - maturity and
scoring APPEND to the outcome ledger, keyed by ``prediction_id``, and the
original forecast stays byte-identical under its chain hash.

FAILURE IS ISOLATED
-------------------
Every stage is wrapped. A challenger that cannot emit records its reason and the
others still emit; a judge that cannot resolve a leg leaves that prediction
PENDING and scores the rest; a stage that raises degrades this step to a reported
state and never propagates into the daily cycle. A quiet tournament and a broken
one are different things, and this module is required to be able to tell them
apart out loud.
"""
from __future__ import annotations

import datetime as _dt

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import cftc as CF
from . import clock as CK
from . import contract as C
from . import credit as CR
from . import emit as EM
from . import events as EVN
from . import judge as JD
from . import leaderboard as LB
from . import ledger as LG
from . import macro as MC
from . import planner as PL
from . import registry as RG
from . import shadow as SH
from . import velocity as VL

CALCULATION_OWNER = "alpha_agent.r46.advance"

#: Release 46.4 - the orthogonal information lanes the step refreshes BEFORE
#: anything is scored or emitted. Each is bounded, free, and fail-soft: a lane
#: that cannot reach its source records that and the step continues.
LANE_STAGES = ("lane_cftc", "lane_credit", "lane_macro", "lane_events")

#: Stages that are read models or lanes OVER the tournament. A failure there
#: is reported loudly but can never make a live tournament read UNAVAILABLE.
NON_CORE_STAGES = ("evidence_velocity", "throughput_plan",
                   "shadow_pnl") + LANE_STAGES

#: The append-only record of every tournament advance. Research root only.
CYCLE_ARTIFACT = "R46_TOURNAMENT_CYCLES.json"

#: Frozen state vocabulary for ONE advance.
STATE_ADVANCED = "TOURNAMENT_ADVANCED"
STATE_NOTHING_DUE = "TOURNAMENT_LIVE_NOTHING_DUE"
STATE_NOT_REGISTERED = "TOURNAMENT_NOT_REGISTERED"
STATE_UNAVAILABLE = "TOURNAMENT_UNAVAILABLE"
STATES = (STATE_ADVANCED, STATE_NOTHING_DUE, STATE_NOT_REGISTERED,
          STATE_UNAVAILABLE)

#: How many recent advances the artifact keeps in full. Older ones are counted,
#: never deleted from the ledgers they describe.
_KEEP_CYCLES = 400


def _safe(fn, failures: list, label: str):
    try:
        return fn()
    except Exception as exc:                    # noqa: BLE001 - isolation is the point
        failures.append({"stage": label, "error": type(exc).__name__,
                         "detail": str(exc)[:200]})
        return None


# --------------------------------------------------------------------------- #
def advance(campaign_id: str = CAMPAIGN_ID, *,
            now: _dt.datetime = None,
            eligible_market_date=None,
            emit_batch: bool = True,
            registry: dict = None) -> dict:
    """Run ONE tournament step. Idempotent, fail-soft, research-root only."""
    started = now or CK.now_utc()
    failures: list = []

    reg = registry if registry is not None else (
        _safe(lambda: RG.load(campaign_id), failures, "load_registry") or {})
    challengers = list(reg.get("challengers") or ())
    if not challengers:
        return _body(campaign_id, STATE_NOT_REGISTERED, started, failures,
                     reason="no Release-46 challenger registry exists at this "
                            "research root; the tournament has not been created")

    # --- 1b. Release 46.4: REFRESH the orthogonal lanes (raw capture only). - #
    # Positioning, credit, macro prints and the event calendar are captured
    # with their acquisition instants BEFORE anything is scored or emitted,
    # so every row emitted later can point at a capture that preceded it.
    as_of = _pnl_as_of(eligible_market_date, started)
    acquire = _lanes_may_acquire()
    lanes = {
        "cftc": _safe(lambda: CF.run(acquire_now=acquire,
                                     campaign_id=campaign_id, as_of=as_of),
                      failures, "lane_cftc"),
        "credit": _safe(lambda: CR.run(acquire_now=acquire,
                                       campaign_id=campaign_id, as_of=as_of),
                        failures, "lane_credit"),
        "macro": _safe(lambda: MC.run(acquire_now=acquire,
                                      campaign_id=campaign_id, as_of=as_of),
                       failures, "lane_macro"),
        "events": _safe(lambda: EVN.run(acquire_now=acquire,
                                        campaign_id=campaign_id, as_of=as_of),
                        failures, "lane_events"),
    }

    # --- 2. SCORE first. Nothing new may exist when maturity is judged. ------ #
    judged = _safe(lambda: JD.score_pending(campaign_id, started),
                   failures, "score_matured") or {}
    n_scored = int(judged.get("n_newly_scored") or 0)

    # --- 3. The board on the evidence that now exists. ---------------------- #
    board_after_scoring = _safe(lambda: LB.build(campaign_id, reg),
                                failures, "leaderboard_after_scoring") or {}

    # --- 3b. Release 46.4: the MONEY layer, on the evidence that now exists. #
    # Open / mark / close research trades, roll the shadow NAVs, and DECIDE
    # the next allocation from outcomes matured on or before the session -
    # all BEFORE the next batch is emitted, so no weight can see it.
    shadow = _safe(lambda: SH.advance_pnl(as_of, reg, board_after_scoring,
                                          campaign_id, now=started),
                   failures, "shadow_pnl") or {}

    # --- 4. EMIT, idempotently, from data available at the emission instant. - #
    emission = None
    if emit_batch:
        emission = _safe(lambda: EM.emit(campaign_id, reg, started),
                         failures, "emit_batch")
    n_emitted = int((emission or {}).get("n_appended") or 0)

    # --- 5. The board with the new batch outstanding. ----------------------- #
    board = _safe(lambda: LB.build(campaign_id, reg),
                  failures, "leaderboard") or board_after_scoring or {}
    schedule = _safe(lambda: EM.maturity_schedule(campaign_id),
                     failures, "maturity_schedule") or {}
    chain = _safe(lambda: LG.verify(campaign_id), failures, "verify_chain") or {}

    # --- Release 46.3: the velocity and planning read models, rebuilt on the
    #     evidence that now exists. Pure functions of the ledgers and the
    #     registry; a failure here degrades to a stage failure and never
    #     stops scoring, emission or the cycle.
    vel = _safe(lambda: VL.build(campaign_id, reg),
                failures, "evidence_velocity") or {}
    _safe(lambda: PL.build(campaign_id, reg, vel),
          failures, "throughput_plan")

    preds = _safe(lambda: LG.predictions(campaign_id), failures,
                  "read_predictions") or []
    outs = _safe(lambda: LG.outcomes(campaign_id), failures,
                 "read_outcomes") or []
    scored_ids = {str(o.get("prediction_id")) for o in outs}
    n_pending = len([p for p in preds
                     if str(p.get("prediction_id")) not in scored_ids])

    # Release 46.3: only CORE stages decide availability. The velocity and
    # planning artifacts are read models OVER the tournament; a failure there
    # is reported loudly but cannot make a live tournament read UNAVAILABLE.
    # Release 46.4 adds the lanes and the P&L layer to that non-core set.
    core_failures = [f for f in failures
                     if f.get("stage") not in NON_CORE_STAGES]
    state = (STATE_UNAVAILABLE if core_failures and not preds
             else STATE_ADVANCED if (n_scored or n_emitted)
             else STATE_NOTHING_DUE)

    body = _body(
        campaign_id, state, started, failures,
        eligible_market_date=(str(eligible_market_date)
                              if eligible_market_date else None),

        # --- the six manifest facts the daily cycle reports ----------------- #
        # MATURED is cumulative (how much forward evidence has actually landed);
        # SCORED and EMITTED are what THIS advance did. Reporting only the deltas
        # would make a healthy quiet day indistinguishable from an empty
        # tournament, and reporting only the totals would hide the day's work.
        tournament_predictions_matured=len(outs),
        tournament_outcomes_scored=n_scored,
        tournament_predictions_emitted=n_emitted,
        tournament_challengers_active=_n_active(reg),
        tournament_forward_evidence_count=len(preds),
        tournament_next_maturity=schedule.get("next_material_evidence_time"),

        # --- the fuller picture -------------------------------------------- #
        total_forward_predictions=len(preds),
        total_matured_predictions=len(outs),
        pending_predictions=n_pending,
        challengers_registered=len(challengers),
        challengers_blocked=_n_blocked(reg),
        emission=_emission_digest(emission),
        judge=_judge_digest(judged),
        leaderboard=_board_digest(board),
        evidence_velocity=_velocity_digest(vel),
        maturity_schedule=schedule.get("schedule") or [],
        earliest_maturity=schedule.get("earliest_maturity"),
        ledger_chain_intact=bool(chain.get("all_intact")),

        # --- Release 46.4: the money facts and the lanes -------------------- #
        pnl_as_of=str(as_of),
        shadow_pnl=_shadow_digest(shadow),
        lanes=_lanes_digest(lanes),

        # --- what this step is not ------------------------------------------ #
        promoted_models=0,
        orders_created=0,
        portfolio_mutations=0,
        operational_writes=0,
        money_spent_usd=0.0,
        writes_only_into=str(campaign_dir(campaign_id)),
        rewrites_existing_predictions=False,
        forward_confirmed_is_research_evidence_only=True,
        promotion_requires_manual_governance=True,
    )
    _record_cycle(campaign_id, body)
    return body


# --------------------------------------------------------------------------- #
def _body(campaign_id: str, state: str, started: _dt.datetime, failures: list,
          **extra) -> dict:
    return artifact_body(
        "r46_tournament_advance/1", CALCULATION_OWNER,
        campaign_id=campaign_id,
        state=state,
        state_vocabulary=list(STATES),
        started_utc=CK.iso(started),
        started_utc_precise=CK.iso_precise(started),
        finished_utc=CK.iso(CK.now_utc()),
        available=state in (STATE_ADVANCED, STATE_NOTHING_DUE),
        stage_failures=failures,
        n_stage_failures=len(failures),
        one_blocked_challenger_never_blocks_the_tournament=True,
        idempotent=True,
        **extra)


def _n_active(reg: dict) -> int:
    return sum(1 for c in (reg.get("challengers") or ())
               if c.get("state") != C.DATA_BLOCKED)


def _n_blocked(reg: dict) -> int:
    return sum(1 for c in (reg.get("challengers") or ())
               if c.get("state") == C.DATA_BLOCKED)


def _emission_digest(emission) -> dict:
    if not emission:
        return {"state": "NOT_RUN", "n_appended": 0, "skipped": []}
    return {
        "state": emission.get("state"),
        "batch_id": emission.get("batch_id"),
        "emitted_at_utc": emission.get("emitted_at_utc"),
        "entry_session_date": emission.get("entry_session_date"),
        "n_candidates": emission.get("n_candidates"),
        "n_offered": emission.get("n_offered"),
        "n_appended": emission.get("n_appended"),
        "n_duplicates_skipped": emission.get("n_duplicates_skipped"),
        "challengers": emission.get("challengers") or [],
        "horizons": emission.get("horizons") or [],
        "earliest_expected_maturity": emission.get("earliest_expected_maturity"),
        # Every challenger that did NOT emit, and why. Named per challenger so a
        # quiet lane can never be mistaken for a broken one.
        "skipped": emission.get("skipped_challengers") or [],
        "non_emission_reasons": sorted({
            str(s.get("reason")) for s in
            (emission.get("skipped_challengers") or [])}),
        "idempotent": True,
    }


def _judge_digest(judged: dict) -> dict:
    return {
        "n_predictions": judged.get("n_predictions"),
        "n_already_scored": judged.get("n_already_scored"),
        "n_newly_scored": judged.get("n_newly_scored"),
        "n_still_pending": judged.get("n_still_pending"),
        "n_invalid": judged.get("n_invalid"),
        "pending_detail": (judged.get("pending_detail") or [])[:20],
        "never_revises_a_forecast": True,
        "scores_only_genuinely_matured_horizons": True,
    }


def _board_digest(board: dict) -> dict:
    rows = list(board.get("rows") or ())
    leader = next((r for r in rows if r.get("origin") == "R46_SEED"), None)
    return {
        "n_rows": board.get("n_rows"),
        "n_competing": board.get("n_competing"),
        "n_forward_pending": board.get("n_forward_pending"),
        "n_early": board.get("n_early"),
        "n_candidate": board.get("n_candidate"),
        "n_confirmed": board.get("n_confirmed"),
        "n_rejected": board.get("n_rejected"),
        "n_data_blocked": board.get("n_data_blocked"),
        "best_net_alpha_bps": board.get("best_net_alpha_bps"),
        "top_forward_challenger": (leader or {}).get("challenger_id"),
        "top_forward_state": (leader or {}).get("state"),
        "top_forward_net_alpha_bps": (leader or {}).get("net_alpha_bps"),
        "top_forward_effective_independent": (leader or {}).get(
            "effective_independent"),
        "ranking_rule": board.get("ranking_rule"),
        "no_row_may_read_proven": True,
    }


def _lanes_may_acquire() -> bool:
    """Lanes reach the network only OUTSIDE the hermetic pytest process.

    The suite declares itself hermetic with ``PAPER_TRADER_ACCEPTANCE_MODE``;
    inside it a lane reads whatever captures its (redirected) root holds and
    acquires nothing, so no test can write a capture into production or
    depend on a provider being up.
    """
    import os
    return os.environ.get("PAPER_TRADER_ACCEPTANCE_MODE") != "1"


def _pnl_as_of(eligible_market_date, started: _dt.datetime) -> _dt.date:
    """The session the money layer marks.

    The Daily Research Cycle names the completed session it is closing; a
    manual call marks the LAST PRINTED session of the NAV clock, never a
    session whose bars have not arrived - a NAV row is immutable once rolled
    and must not be built on stale marks.
    """
    if eligible_market_date:
        try:
            return _dt.date.fromisoformat(str(eligible_market_date)[:10])
        except ValueError:
            pass
    from . import marketdata as MD
    from . import trades as TR
    last = MD.last_session(TR.NAV_CALENDAR_INSTRUMENT)
    return last or CK.eastern_date(started)


def _shadow_digest(shadow: dict) -> dict:
    if not shadow:
        return {"state": "NOT_RUN"}
    keys = ("as_of", "shadow_nav", "shadow_return", "today_net_pnl",
            "cumulative_net_forward_pnl", "residual_alpha_pnl_vs_cash",
            "realised_pnl", "unrealised_pnl", "cost_drag", "max_drawdown",
            "inception", "trades_opened", "trades_marked", "trades_closed",
            "open_research_trades", "closed_research_trades",
            "signal_emitted", "funded_trades", "unfunded_open_trades",
            "canonical_policy", "n_allocated", "top_shadow_allocations",
            "canonical_cash_weight", "effective_independent_pnl_streams",
            "nominal_streams", "correlation_source",
            "economic_state_counts", "opportunity_counts",
            "best_net_pnl_strategy", "worst_net_pnl_strategy",
            "best_residual_alpha_strategy",
            "best_capital_efficiency_strategy", "ledgers_intact",
            "n_stage_failures", "stage_failures")
    return {k: shadow.get(k) for k in keys}


def _lanes_digest(lanes: dict) -> dict:
    out = {}
    for name, body in (lanes or {}).items():
        b = body or {}
        out[name] = {"state": b.get("state", "NOT_RUN"),
                     "as_of": b.get("as_of"),
                     "n_captures": (b.get("n_captures")
                                    if b.get("n_captures") is not None
                                    else (b.get("acquisition") or {}).get(
                                        "n_captures")),
                     "information_family": b.get("information_family"),
                     "challengers_frozen": b.get("challengers_frozen")}
    return out


def _velocity_digest(vel: dict) -> dict:
    if not vel:
        return {"state": "NOT_BUILT"}
    return {
        "raw_predictions_emitted": vel.get("raw_predictions_emitted"),
        "raw_matured_rows": vel.get("raw_matured_rows"),
        "effective_independent_observations":
            vel.get("effective_independent_observations"),
        "dependence_penalty": vel.get("dependence_penalty"),
        "projected_effective_per_week":
            vel.get("projected_effective_per_week"),
        "realised_effective_per_week":
            vel.get("realised_effective_per_week"),
        "n_dependence_clusters": vel.get("n_dependence_clusters"),
        "binding_bottleneck": ((vel.get("current_evidence_bottleneck") or {})
                               .get("binding") or {}).get("code"),
        "information_set_state": vel.get("information_set_state"),
    }


def _record_cycle(campaign_id: str, body: dict) -> None:
    """Append this advance to the campaign's own cycle record."""
    p = campaign_dir(campaign_id) / CYCLE_ARTIFACT
    prior = read_json(p, default=None) or {}
    cycles = list(prior.get("cycles") or [])
    cycles.append({k: body.get(k) for k in (
        "state", "started_utc", "started_utc_precise", "finished_utc",
        "eligible_market_date", "tournament_outcomes_scored",
        "tournament_predictions_emitted", "tournament_forward_evidence_count",
        "pending_predictions", "tournament_next_maturity",
        "tournament_challengers_active", "n_stage_failures")})
    n_total = int(prior.get("n_cycles_total") or 0) + 1
    kept = cycles[-_KEEP_CYCLES:]
    write_json(p, artifact_body(
        "r46_tournament_cycles/1", CALCULATION_OWNER,
        n_cycles_total=n_total,
        n_cycles_retained=len(kept),
        first_cycle=kept[0] if kept else None,
        latest_cycle=kept[-1] if kept else None,
        cycles=kept,
        note="one row per advance of the ONE Release-46 tournament; the "
             "prediction and outcome ledgers remain the authoritative record"))


__all__ = ["CALCULATION_OWNER", "CYCLE_ARTIFACT", "STATES", "STATE_ADVANCED",
           "STATE_NOTHING_DUE", "STATE_NOT_REGISTERED", "STATE_UNAVAILABLE",
           "advance"]
