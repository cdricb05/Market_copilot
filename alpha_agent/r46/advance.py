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
from . import clock as CK
from . import contract as C
from . import emit as EM
from . import judge as JD
from . import leaderboard as LB
from . import ledger as LG
from . import registry as RG

CALCULATION_OWNER = "alpha_agent.r46.advance"

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

    # --- 2. SCORE first. Nothing new may exist when maturity is judged. ------ #
    judged = _safe(lambda: JD.score_pending(campaign_id, started),
                   failures, "score_matured") or {}
    n_scored = int(judged.get("n_newly_scored") or 0)

    # --- 3. The board on the evidence that now exists. ---------------------- #
    board_after_scoring = _safe(lambda: LB.build(campaign_id, reg),
                                failures, "leaderboard_after_scoring") or {}

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

    preds = _safe(lambda: LG.predictions(campaign_id), failures,
                  "read_predictions") or []
    outs = _safe(lambda: LG.outcomes(campaign_id), failures,
                 "read_outcomes") or []
    scored_ids = {str(o.get("prediction_id")) for o in outs}
    n_pending = len([p for p in preds
                     if str(p.get("prediction_id")) not in scored_ids])

    state = (STATE_UNAVAILABLE if failures and not preds
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
        maturity_schedule=schedule.get("schedule") or [],
        earliest_maturity=schedule.get("earliest_maturity"),
        ledger_chain_intact=bool(chain.get("all_intact")),

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
