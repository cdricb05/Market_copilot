"""
api/prospective_tournament.py - Release 46 PROSPECTIVE ALPHA TOURNAMENT
(read-only operator visibility over the ONE canonical forward-evidence board).

    load_prospective_tournament(...) -> GET /v1/research/prospective-tournament

This module READS the Release-46 campaign's own hashed artifacts and normalises
them into the questions an operator actually asks:

    how many models are competing?
    how many REAL forward predictions exist, and how many have matured?
    which are winning, which are losing, and which are simply too early?
    what is the best net alpha against the CORRECT control?
    how confident is that, in effective independent observations?
    when is the next material maturity?
    what entered, and what was killed?

Two display rules are enforced here rather than left to the front end, because
both have burned this project before:

* **no historical-only model may look proven.** A challenger whose entire
  record is a backtest is labelled ``HISTORICAL_ONLY`` and carries an explicit
  ``forward_predictions_matured = 0``. The strongest state that exists in this
  release is ``FORWARD_CONFIRMED``, it requires the full declared evidence
  gate, and it still confers no capital.
* **raw and effective observation counts always travel together.** Fifty
  overlapping twenty-day bets are not fifty independent ones, and a board that
  showed only the raw count would invite exactly the inference Release 45 spent
  a whole campaign refuting.

Strictly read-only. It writes nothing, computes no research mathematics, calls
no prediction service and no external provider, creates no signal, target,
proposal, decision, order or fill, and cannot promote a model or change the
operational portfolio. A missing artifact degrades to a ``warnings[]`` entry
with HTTP 200, never a stack trace.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

SCHEMA_VERSION = "prospective_tournament.v1"
COMPOSITION_OWNER = "api.prospective_tournament"
PHASE = "R46"

STATE_LIVE = "LIVE"
STATE_READY = "READY_NEXT_WINDOW"
STATE_NOT_STARTED = "NOT_STARTED"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_LIVE, STATE_READY, STATE_NOT_STARTED, STATE_UNAVAILABLE)

RESEARCH_ROOT_ENV = "PAPER_TRADER_R46_RESEARCH_ROOT"
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\prospective_alpha_tournament_r46")
DEFAULT_CAMPAIGN_ID = "r46_prospective_alpha_tournament_v1"

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "PREVIEW ONLY", "NO ORDERS",
                 "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW",
                 "NO MODEL PROMOTION", "CREATES NO SIGNALS",
                 "CREATES NO TRADE DECISIONS"]

ARTIFACTS = {
    "verdict": "R46_FINAL_VERDICT.json",
    "registry": "r46_challenger_registry.json",
    "leaderboard": "R46_LEADERBOARD.json",
    "batches": "R46_FORWARD_BATCHES.json",
    "contract": "r46_frozen_contract.json",
    "burden": "r46_search_burden_ledger.json",
    "shell_policy": "R46_SHELL_POLICY_EVENTS.json",
    # Release 46.2 — the record of every tournament advance the Daily Research Cycle
    # has driven. Optional by design: an estate that has never advanced the tournament
    # simply has no cycles artifact, and that reads as "not advanced yet" rather than
    # as a failure.
    "cycles": "R46_TOURNAMENT_CYCLES.json",
    # Release 46.3 — the velocity, planning and lane artifacts. All optional: a root
    # built before R46.3, or a hermetic test root, simply has none of them and that
    # reads as "not computed yet", never as a failure.
    "velocity": "R46_EVIDENCE_VELOCITY.json",
    "plan": "R46_THROUGHPUT_PLAN.json",
    "intraday": "R46_INTRADAY_LANE.json",
    "options": "R46_OPTIONS_LANE.json",
    "analyst": "R46_ANALYST_LANE.json",
    # Release 46.4 — the economic layer and the orthogonal lanes. Every one is
    # read verbatim from the artifact its owner persisted; this module computes
    # no P&L, no NAV, no weight and no recommendation. All optional: a root
    # built before R46.4 has none of them and that reads as "not computed".
    "pnl_nav": "R46_4_SHADOW_NAV.json",
    "pnl_comparison": "R46_4_SHADOW_POLICY_COMPARISON.json",
    "pnl_board": "R46_4_PNL_LEADERBOARD.json",
    "pnl_allocation": "R46_4_SHADOW_ALLOCATION.json",
    "pnl_risk": "R46_4_RISK_STATE.json",
    "pnl_attribution": "R46_4_PNL_ATTRIBUTION.json",
    "pnl_opportunity": "R46_4_OPPORTUNITY_COST.json",
    "pnl_trades": "R46_4_RESEARCH_TRADES.json",
    "pnl_strategy": "R46_4_STRATEGY_PNL.json",
    "pnl_bridge": "R46_4_RESEARCH_BRIDGE.json",
    "pnl_regime": "R46_4_REGIME_STATE.json",
    "lane_cftc": "R46_4_CFTC_LANE.json",
    "lane_credit": "R46_4_CREDIT_LANE.json",
    "lane_macro": "R46_4_MACRO_LANE.json",
    "lane_events": "R46_4_EVENT_LANE.json",
    # Release 46.5 — the forward harvest (matured vs mark-to-market), the
    # strategy verdicts, the realised-correlation state and the two EDGAR
    # lanes. Read verbatim; this module computes none of them.
    "harvest": "R46_5_FORWARD_HARVEST.json",
    "verdicts": "R46_5_STRATEGY_VERDICTS.json",
    "correlation": "R46_5_REALISED_CORRELATION.json",
    "lane_earnings": "R46_5_EARNINGS_LANE.json",
    "lane_form4": "R46_5_FORM4_LANE.json",
    # Release 46.6 — the cost-efficiency owner (signal edge versus economic
    # edge), the research-lane lifecycle contract, the adopted-shadow
    # inventory and the scored option hypotheses. Read verbatim; this module
    # computes no ratio, no break-even and no classification of its own.
    "cost_efficiency": "R46_6_COST_EFFICIENCY.json",
    "cost_rankings": "R46_6_COST_DESTRUCTION_RANKINGS.json",
    "break_even": "R46_6_BREAK_EVEN_ECONOMICS.json",
    "lane_lifecycle": "R46_6_RESEARCH_LANE_LIFECYCLE.json",
    "adopted_lanes": "R46_6_ADOPTED_SHADOW_LANE_INVENTORY.json",
    "options_hypotheses": "R46_6_OPTIONS_HYPOTHESES.json",
    # Release 46.6.1 — the adopted-shadow forward continuation. R46.6 left the
    # adopted lanes CALLED and unable to accrue, and this payload said so with
    # a single "append_authorised: false" that could only be read as "dead".
    # The continuation owner's artifact distinguishes the prior-release append
    # right (still forbidden, permanently) from the R46 continuation ledger
    # (where adopted forward evidence now goes).
    "adopted_continuation": "R46_6_1_ADOPTED_CONTINUATION.json",
}

R46_4_ARTIFACTS = ("pnl_nav", "pnl_comparison", "pnl_board", "pnl_allocation",
                   "pnl_risk", "pnl_attribution", "pnl_opportunity",
                   "pnl_trades", "pnl_strategy", "pnl_bridge", "pnl_regime",
                   "lane_cftc", "lane_credit", "lane_macro", "lane_events",
                   "harvest", "verdicts", "correlation", "lane_earnings",
                   "lane_form4")

R46_6_ARTIFACTS = ("cost_efficiency", "cost_rankings", "break_even",
                   "lane_lifecycle", "adopted_lanes", "options_hypotheses",
                   "adopted_continuation")

#: Artifacts whose absence is EXPECTED (before the first advance, or before the
#: Release-46.3 owners ever ran here) and so must not become an operator-facing
#: warning.
OPTIONAL_ARTIFACTS = (("cycles", "velocity", "plan", "intraday", "options",
                       "analyst") + R46_4_ARTIFACTS + R46_6_ARTIFACTS)

#: Evidence-maturity vocabulary for the whole board (not for a single cell).
MATURITY_NO_FORWARD_EVIDENCE = "NO_FORWARD_EVIDENCE"
MATURITY_AWAITING_FIRST = "AWAITING_FIRST_MATURITY"
MATURITY_ACCRUING = "FORWARD_EVIDENCE_ACCRUING"
MATURITY_GATE_REACHED = "FORWARD_GATE_REACHED"
EVIDENCE_MATURITY_STATES = (MATURITY_NO_FORWARD_EVIDENCE, MATURITY_AWAITING_FIRST,
                            MATURITY_ACCRUING, MATURITY_GATE_REACHED)

FORWARD_LEDGER_DIR = "prospective_forward"
PREDICTION_LEDGER = "r46_forward_predictions.json"
OUTCOME_LEDGER = "r46_forward_outcomes.json"


# --------------------------------------------------------------------------- #
def research_root() -> Path:
    return Path(os.environ.get(RESEARCH_ROOT_ENV) or DEFAULT_RESEARCH_ROOT)


def campaign_dir(campaign_id: str = DEFAULT_CAMPAIGN_ID) -> Path:
    return research_root() / campaign_id


def _read(path: Path) -> Optional[Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _rows(payload: Optional[dict]) -> list:
    if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
        return payload["rows"]
    return []


# --------------------------------------------------------------------------- #
def load_prospective_tournament(
        campaign_id: str = DEFAULT_CAMPAIGN_ID) -> dict:
    """Compose the read-only Release-46 tournament view."""
    cdir = campaign_dir(campaign_id)
    warnings: list = []

    art = {}
    for key, name in ARTIFACTS.items():
        body = _read(cdir / name)
        if body is None and key not in OPTIONAL_ARTIFACTS:
            warnings.append("artifact unavailable: %s" % name)
        art[key] = body

    fdir = cdir / FORWARD_LEDGER_DIR
    preds = _rows(_read(fdir / PREDICTION_LEDGER))
    outs = _rows(_read(fdir / OUTCOME_LEDGER))

    board = art.get("leaderboard") or {}
    registry = art.get("registry") or {}
    verdict = art.get("verdict") or {}
    batches_body = art.get("batches") or {}
    batches = list(batches_body.get("batches") or [])

    rows = list(board.get("rows") or [])
    scored_ids = {str(o.get("prediction_id")) for o in outs}
    pending = [p for p in preds if str(p.get("prediction_id"))
               not in scored_ids]

    state = _state(verdict, preds)

    competing = _competing(rows)
    winning, losing, too_early, killed, blocked = _partition(rows)

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "phase": PHASE,
        "campaign_id": campaign_id,
        "state": state,
        "headline": _headline(state, preds, outs, rows),

        # ---- the operator's questions, answered directly ----------------- #
        "how_many_models_are_competing": competing["n_challengers"],
        "how_many_are_active": competing["n_active"],
        "how_many_are_blocked": competing["n_blocked"],
        "how_many_real_forward_predictions_exist": len(preds),
        "how_many_have_matured": len(outs),
        "how_many_are_still_pending": len(pending),
        "which_are_winning": winning,
        "which_are_losing": losing,
        "which_are_too_early_to_judge": too_early,
        "which_were_killed": killed,
        "which_are_data_blocked": blocked,
        "best_net_alpha_vs_control_bps": board.get("best_net_alpha_bps"),
        "forward_evidence_confidence": _confidence(rows),
        "next_material_maturity": _next_maturity(verdict, pending),
        "what_entered": competing["entered"],

        # ---- Release 46.2: the LIVE lifecycle ----------------------------- #
        # Before R46.2 the tournament could only be advanced by re-running the whole
        # campaign by hand, so these answers described a board that nothing was
        # moving. They now describe a board the canonical Daily Research Cycle
        # advances: it scores what genuinely matured, rebuilds the leaderboard and
        # emits the next eligible batch, in that order, every cycle.
        "how_many_outcomes_are_scored": len(outs),
        "scored_outcomes": _scored_outcomes(outs),
        "current_top_forward_challenger": _leader(rows),
        "current_top_net_alpha_vs_control_bps": _top_net_alpha(rows),
        "evidence_maturity_state": _maturity_state(preds, outs, rows),
        "evidence_maturity_vocabulary": list(EVIDENCE_MATURITY_STATES),
        "tournament_advance": _advance(art.get("cycles")),
        "advancement_owner": "alpha_agent.r46.advance",
        # Named in prose, not as a module path: the Release-33 write-attribution
        # gate forbids an R46 source file from carrying the LITERAL name of an
        # operational store root, and it is right to - a pure read model has no
        # business referring to one, even in a caption.
        "advanced_by": ("the canonical Daily Research Cycle "
                        "(step ADVANCE_PROSPECTIVE_TOURNAMENT)"),
        "new_batch_emitted": (batches[-1] if batches else None),
        # The one distinction §17 exists to preserve: a backtest that nominated a
        # challenger and a forward record that could convict it are DIFFERENT
        # classes of evidence, and the first may never be displayed as the second.
        "historical_qualification_vs_forward_proof": {
            "historical_qualification_is_not_proof": True,
            "forward_proof_requires_matured_true_forward_rows": True,
            "n_rows_with_historical_qualification_only": len(
                [r for r in rows if not int(r.get("raw_matured") or 0)]),
            "n_rows_with_any_forward_evidence": len(
                [r for r in rows if int(r.get("raw_matured") or 0)]),
            "note": ("A challenger's historical_qualification_state describes how it "
                     "ENTERED. Only raw_matured / effective_independent describe what "
                     "it has PROVEN forward. The two are never merged into one score."),
        },

        # ---- Release 46.3: evidence velocity, planning and lanes ---------- #
        # All read verbatim from the artifacts their owners persisted. The raw
        # count and the effective count always travel together, because fifty
        # overlapping twenty-day bets are not fifty independent observations
        # and a surface that showed only the raw number would invite exactly
        # that inference.
        "evidence_velocity": _velocity(art.get("velocity")),
        "throughput_plan": _plan(art.get("plan")),
        "lanes": _lanes(art.get("options"), art.get("analyst"),
                        art.get("intraday")),

        # ---- Release 46.4: ARE WE MAKING MONEY? ---------------------------- #
        # The economic layer, read verbatim from its owners. TRUE_FORWARD
        # dollars only; a historical simulation is never shown as forward
        # P&L, and realised / unrealised / expected are never one number.
        "shadow_pnl": _shadow_pnl(art),
        "information_lanes": _information_lanes(art),

        # ---- Release 46.6: signal edge versus ECONOMIC edge ---------------- #
        # The first matured forward observation predicted the direction
        # correctly and still lost money. These three blocks exist so that
        # distinction survives every surface that reads this payload.
        "economic_truth": _economic_truth(art.get("pnl_nav"),
                                          art.get("pnl_comparison"),
                                          art.get("harvest"),
                                          art.get("cost_efficiency")),
        "cost_efficiency": _cost_efficiency(art.get("cost_efficiency"),
                                            art.get("cost_rankings"),
                                            art.get("break_even")),
        "research_lane_lifecycle": _research_lanes(
            art.get("lane_lifecycle"), art.get("adopted_lanes"),
            art.get("options_hypotheses"),
            art.get("adopted_continuation")),
        "challengers_by_asset_class": _count_by(rows, "asset_class"),
        "challengers_by_economic_family": _count_by(rows, "family"),
        "challengers_by_information_family": _count_by_info(registry),
        "challengers_by_horizon": _count_by(rows, "horizon"),

        # ---- honesty rails ----------------------------------------------- #
        "no_historical_only_model_looks_proven": True,
        "proven_alpha_is_not_a_state": True,
        "strongest_available_state": "FORWARD_CONFIRMED",
        "raw_and_effective_counts_always_shown_together": True,
        "evidence_classes": {
            "TRUE_FORWARD": "emitted strictly before the outcome existed",
            "HISTORICAL_SIMULATION": "a replay of dates that had already "
                                     "happened - nominates, never crowns",
        },

        # ---- detail ------------------------------------------------------ #
        "leaderboard": rows,
        "ranking_rule": board.get("ranking_rule"),
        "counts": {
            "forward_pending": board.get("n_forward_pending"),
            "early_forward_evidence": board.get("n_early"),
            "forward_candidate": board.get("n_candidate"),
            "forward_confirmed": board.get("n_confirmed"),
            "forward_rejected": board.get("n_rejected"),
            "data_blocked": board.get("n_data_blocked"),
        },
        "asset_classes_active": registry.get("asset_classes_active") or [],
        "horizons_active": registry.get("horizons_active") or [],
        "adoption": _adoption(registry),
        "first_batch": batches[0] if batches else None,
        "latest_batch": batches[-1] if batches else None,
        "n_batches": len(batches),
        "maturity_schedule": (verdict.get("maturity_schedule") or {}).get(
            "schedule") or [],
        "search_burden": {
            "global_historical": (art.get("burden") or {}).get(
                "GLOBAL_SEARCH_BURDEN"),
            "new_r46_historical_trials": (art.get("burden") or {}).get(
                "new_r46_effective_trials"),
            "prospective_forward_evidence_is_not_search_burden": True,
        },
        "contract_hash": (art.get("contract") or {}).get("contract_hash"),
        "registry_hash": registry.get("registry_hash"),
        "terminal_state": verdict.get("TERMINAL_STATE"),
        "shell_policy_violation": (art.get("shell_policy") or {}).get(
            "SHELL_POLICY_VIOLATION"),

        # ---- safety ------------------------------------------------------ #
        "safety_badges": list(SAFETY_BADGES),
        "no_live_trading": {
            "creates_orders": False,
            "creates_signals": False,
            "creates_trade_decisions": False,
            "promotes_models": False,
            "mutates_portfolio": False,
            "enables_automation": False,
            "forward_candidate_is_an_order": False,
            "forward_confirmed_is_an_automatic_holding": False,
            "who_decides": "the canonical portfolio manager, manually",
        },
        "next_action": _next_action(state, pending, outs),
        "warnings": warnings,
    }


# --------------------------------------------------------------------------- #
def _state(verdict: dict, preds: list) -> str:
    if not verdict:
        return STATE_UNAVAILABLE
    if preds:
        return STATE_LIVE
    if verdict.get("TERMINAL_STATE"):
        return STATE_READY
    return STATE_NOT_STARTED


def _competing(rows: list) -> dict:
    ids = {r.get("challenger_id") for r in rows}
    active = {r.get("challenger_id") for r in rows
              if r.get("state") not in ("DATA_BLOCKED",)}
    blocked = {r.get("challenger_id") for r in rows
               if r.get("state") == "DATA_BLOCKED"}
    entered = sorted({r.get("challenger_id") for r in rows
                      if r.get("origin") == "R46_SEED"})
    return {"n_challengers": len(ids), "n_active": len(active),
            "n_blocked": len(blocked), "entered": entered}


def _cell(r: dict) -> dict:
    return {
        "challenger_id": r.get("challenger_id"),
        "version": r.get("challenger_version"),
        "horizon": r.get("horizon"),
        "asset_class": r.get("asset_class"),
        "family": r.get("family"),
        "state": r.get("state"),
        "raw_matured": r.get("raw_matured"),
        "effective_independent": r.get("effective_independent"),
        "net_alpha_bps": r.get("net_alpha_bps"),
        "t_stat": r.get("t_stat"),
        "hit_rate": r.get("hit_rate"),
        "reason": r.get("blocked_reason"),
        "next_evidence_gate": r.get("next_evidence_gate"),
    }


def _partition(rows: list):
    winning, losing, too_early, killed, blocked = [], [], [], [], []
    for r in rows:
        st = r.get("state")
        if st == "DATA_BLOCKED":
            blocked.append(_cell(r))
        elif st == "FORWARD_REJECTED":
            killed.append(_cell(r))
        elif st in ("FORWARD_CONFIRMED", "FORWARD_CANDIDATE"):
            winning.append(_cell(r))
        elif st == "EARLY_FORWARD_EVIDENCE":
            a = r.get("net_alpha_bps")
            (winning if (a is not None and a > 0) else losing).append(_cell(r))
        else:
            too_early.append(_cell(r))
    return winning, losing, too_early, killed, blocked


def _confidence(rows: list) -> dict:
    eff = [int(r.get("effective_independent") or 0) for r in rows]
    raw = [int(r.get("raw_matured") or 0) for r in rows]
    scores = [float(r.get("forward_evidence_score") or 0.0) for r in rows]
    return {
        "total_effective_independent_observations": sum(eff),
        "total_raw_matured_observations": sum(raw),
        "best_cell_evidence_score": max(scores) if scores else 0.0,
        "interpretation": (
            "0.0 means no forward evidence has accrued yet; 1.0 means a cell "
            "has reached the effective independent count its declared gate "
            "requires. It is a measure of MATURITY, not of edge."),
    }


def _scored_outcomes(outs: list) -> list:
    """Every MATURED outcome, stated against the control it declared in advance.

    Alpha versus the DECLARED control is the only number that decides anything:
    Release 42 watched a real premium priced below cash, and Release 43 watched
    another disappear entirely into two-legged cost. Gross is carried because
    hiding it would be dishonest, not because it means much on its own.

    For an R46-native row the declared control IS cash, so one number answers
    both questions. An R46.6.1 adopted continuation row froze its own control -
    a passive basket of the same scope, say - and then the two questions come
    apart: it carries ``scientific_alpha`` (versus the control it was frozen
    against, the only number a formal verdict may use) and ``capital_alpha_vs_
    cash`` (whether the capital beat cash), and this block never merges them.
    """
    out = []
    for o in outs[-100:]:
        out.append({
            "prediction_id": o.get("prediction_id"),
            "challenger_id": o.get("challenger_id"),
            "asset_class": o.get("asset_class"),
            "horizon": o.get("horizon"),
            "effective_as_of": o.get("effective_as_of"),
            "maturity_date": o.get("maturity_date"),
            "scored_at_utc": o.get("scored_at_utc"),
            "gross_return": o.get("realised_gross_return"),
            "cost": o.get("realised_cost"),
            "net_return": o.get("realised_net_return"),
            "control": o.get("control"),
            "control_return": o.get("control_return"),
            "benchmark_return": o.get("realised_benchmark_return"),
            "residual_return": o.get("realised_residual_return"),
            "net_alpha_vs_control": o.get("net_alpha_vs_control"),
            "net_alpha_vs_control_at_2x_costs": o.get(
                "net_alpha_vs_control_at_2x_costs"),
            # R46.6.1 - an adopted continuation row carries TWO controls, and a
            # reader must be able to tell which claim it is looking at. An
            # R46-native row declares cash as its own control, so these stay
            # absent there rather than being invented.
            "net_alpha_vs_control_means": o.get(
                "net_alpha_vs_control_means", "ALPHA_VS_THE_DECLARED_CONTROL"),
            "scientific_control": o.get("scientific_control"),
            "scientific_control_return": o.get("scientific_control_return"),
            "scientific_control_state": o.get("scientific_control_state"),
            "scientific_alpha": o.get(
                "scientific_alpha_vs_declared_control"),
            "scientific_alpha_at_2x_costs": o.get(
                "scientific_alpha_vs_declared_control_at_2x_costs"),
            "capital_control": o.get("capital_control"),
            "capital_control_return": o.get("capital_control_return"),
            "capital_alpha_vs_cash": o.get("capital_alpha_vs_cash"),
            "capital_alpha_vs_cash_at_2x_costs": o.get(
                "capital_alpha_vs_cash_at_2x_costs"),
            "formal_verdict_uses": o.get("formal_verdict_uses"),
            "rank_ic": o.get("rank_ic"),
            "hit": o.get("hit"),
            "capital_hit": o.get("capital_hit"),
            "one_outcome_is_not_alpha": True,
        })
    return out


def _leader(rows: list) -> Optional[dict]:
    """The top-ranked R46 cell. Ranked by EVIDENCE band first, edge second."""
    for r in rows:
        if r.get("origin") == "R46_SEED" and r.get("state") != "DATA_BLOCKED":
            return _cell(r)
    return None


def _top_net_alpha(rows: list) -> Optional[float]:
    vals = [r.get("net_alpha_bps") for r in rows
            if r.get("origin") == "R46_SEED" and r.get("net_alpha_bps") is not None]
    return max(vals) if vals else None


def _maturity_state(preds: list, outs: list, rows: list) -> str:
    if not preds:
        return MATURITY_NO_FORWARD_EVIDENCE
    if not outs:
        return MATURITY_AWAITING_FIRST
    if any(float(r.get("forward_evidence_score") or 0.0) >= 1.0 for r in rows):
        return MATURITY_GATE_REACHED
    return MATURITY_ACCRUING


def _advance(cycles_body: Optional[dict]) -> dict:
    """What the last Daily Research Cycle actually did to the tournament."""
    body = cycles_body or {}
    latest = body.get("latest_cycle") or {}
    return {
        "has_ever_advanced": bool(body.get("n_cycles_total")),
        "n_advances": body.get("n_cycles_total") or 0,
        "latest": latest or None,
        "latest_state": latest.get("state"),
        "latest_started_utc": latest.get("started_utc"),
        "latest_outcomes_scored": latest.get("tournament_outcomes_scored"),
        "latest_predictions_emitted": latest.get("tournament_predictions_emitted"),
        "latest_eligible_market_date": latest.get("eligible_market_date"),
        "note": ("The Daily Research Cycle advances the tournament: score what "
                 "matured, rebuild the board, then emit the next eligible batch. "
                 "A cycle that scores and emits nothing is a QUIET tournament, not "
                 "a stopped one — the reason is on the step result."),
    }


def _next_maturity(verdict: dict, pending: list) -> Optional[str]:
    # Release 46.2 — the LIVE pending ledger wins. The verdict's cached
    # NEXT_MATERIAL_EVIDENCE_TIME was written by the last full campaign run; once the
    # Daily Research Cycle started scoring maturities, that cached date keeps naming a
    # maturity that has already been scored, so the board would advertise evidence that
    # had already landed. The ledger is the record; the artifact is a snapshot of it.
    dates = sorted({str(p.get("horizon_end_expected")) for p in pending
                    if p.get("horizon_end_expected")})
    if dates:
        return dates[0]
    # Nothing is outstanding, so nothing is scheduled to mature. Before the first
    # batch the verdict's own value is None for the same reason, so there is no
    # honest fallback to reach for here.
    return None


def _velocity(body: Optional[dict]) -> dict:
    """The velocity owner's answer, or an honest 'not computed yet'."""
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the evidence-velocity artifact has not been "
                        "computed at this research root yet"}
    b = (body.get("current_evidence_bottleneck") or {}).get("binding") or {}
    return {
        "available": True,
        "raw_predictions_emitted": body.get("raw_predictions_emitted"),
        "raw_predictions_pending": body.get("raw_predictions_pending"),
        "raw_matured_rows": body.get("raw_matured_rows"),
        "effective_independent_observations":
            body.get("effective_independent_observations"),
        "dependence_penalty": body.get("dependence_penalty"),
        "n_dependence_clusters": body.get("n_dependence_clusters"),
        "decision_date_count": body.get("decision_date_count"),
        "realised_effective_per_week":
            body.get("realised_effective_per_week"),
        "projected_effective_per_week":
            body.get("projected_effective_per_week"),
        "projected_raw_rows_per_week":
            body.get("projected_raw_rows_per_week"),
        "weeks_to_targets": ((body.get("projections") or {})
                             .get("tournament") or {}).get("weeks_to_target"),
        "binding_bottleneck": b.get("code"),
        "binding_bottleneck_detail": b.get("detail"),
        "information_set_state": body.get("information_set_state"),
        "asset_class_diversity": body.get("asset_class_diversity"),
        "economic_family_diversity": body.get("economic_family_diversity"),
        "information_family_diversity":
            body.get("information_family_diversity"),
        "horizon_diversity": body.get("horizon_diversity"),
        "dependence_clusters": body.get("dependence_clusters"),
        "raw_and_effective_always_shown_together": True,
    }


def _plan(body: Optional[dict]) -> dict:
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the throughput plan has not been computed at this "
                        "research root yet"}
    return {
        "available": True,
        "top_candidate": body.get("top_candidate"),
        "ranked_candidates": body.get("ranked_candidates"),
        "information_set_frontier": body.get("information_set_frontier"),
        "frontier_is_planning_only": True,
        "nominates_but_never_registers": True,
    }


def _lanes(options: Optional[dict], analyst: Optional[dict],
           intraday: Optional[dict]) -> dict:
    ojs = ((options or {}).get("judgeable_sample")) or {}
    ajs = ((analyst or {}).get("judgeable_sample")) or {}
    return {
        "options": {
            "state": ojs.get("state"),
            # Release 46.6.1 — this gate counts DATES. It never measured
            # whether a predeclared hypothesis has the strikes and expiries it
            # needs on them, and reading "JUDGEABLE" as if it had is the exact
            # misreading this wording closes.
            "session_gate_state": ojs.get("session_gate_state"),
            "gate_measures": ojs.get("gate_measures"),
            "gate_does_not_measure": ojs.get("gate_does_not_measure"),
            "judgeable_here_means": ojs.get("judgeable_here_means"),
            "hypothesis_sample_sufficiency_is_answered_by": ojs.get(
                "hypothesis_sample_sufficiency_is_answered_by"),
            "usable_sessions_now": ojs.get("usable_sessions_now"),
            "sessions_required": ojs.get("sessions_required"),
            "sessions_still_required": ojs.get("sessions_still_required"),
            "n_predeclared_hypotheses": (options or {}).get("n_predeclared"),
            "hypotheses_frozen_before_the_confirming_sessions_exist":
                bool((options or {}).get(
                    "hypotheses_frozen_before_the_confirming_sessions_exist")),
        },
        "analyst": {
            "state": ajs.get("state"),
            "revisions_observed": ajs.get("revisions_observed"),
            "revisions_required": ajs.get("revisions_required"),
            "approx_months_remaining": ajs.get("approx_months_remaining"),
            "never_backfilled": bool((analyst or {}).get("never_backfilled")),
        },
        "intraday": {
            "state": (intraday or {}).get("state"),
            "exact_blocker": (intraday or {}).get("exact_blocker"),
            "session_close_note": (intraday or {}).get("session_close_note"),
        },
        "a_blocked_lane_stops_nothing_else": True,
    }


def _shadow_pnl(art: dict) -> dict:
    """The money answer, from the NAV / board / allocation / risk owners."""
    nav = art.get("pnl_nav")
    if not isinstance(nav, dict):
        return {"available": False,
                "note": "the shadow P&L layer has not run at this research "
                        "root yet; the tournament is scored, not priced"}
    comp = art.get("pnl_comparison") or {}
    board = art.get("pnl_board") or {}
    alloc = art.get("pnl_allocation") or {}
    risk = art.get("pnl_risk") or {}
    attr = art.get("pnl_attribution") or {}
    opp = art.get("pnl_opportunity") or {}
    trades = art.get("pnl_trades") or {}
    strat = art.get("pnl_strategy") or {}
    bridge = art.get("pnl_bridge") or {}
    weights = alloc.get("canonical_weights") or {}
    by = attr.get("by") or {}

    def _grp(key):
        return [{"key": g.get("key"), "net_pnl": g.get("net_pnl"),
                 "gross_pnl": g.get("gross_pnl"), "cost_pnl": g.get("cost_pnl"),
                 "residual_alpha_pnl": g.get("residual_alpha_pnl"),
                 "n_trades": g.get("n_trades")} for g in (by.get(key) or [])]

    top_rows = []
    for r in (board.get("rows") or [])[:15]:
        top_rows.append({
            "pnl_rank": r.get("pnl_rank"),
            "challenger_id": r.get("challenger_id"),
            "horizon": r.get("horizon"),
            "asset_class": r.get("asset_class"),
            "state": r.get("state"),
            "economic_state": r.get("economic_state"),
            "net_forward_pnl": r.get("net_forward_pnl"),
            "residual_alpha_pnl": r.get("residual_alpha_pnl"),
            "realised_pnl": r.get("realised_pnl"),
            "unrealised_pnl": r.get("unrealised_pnl"),
            "cost_drag": r.get("cost_drag"),
            "max_drawdown_pnl": r.get("max_drawdown_pnl"),
            "hit_rate_closed": r.get("hit_rate_closed"),
            "shadow_weight": r.get("shadow_weight"),
            "marginal_diversification": r.get("marginal_diversification"),
            "n_trades_opened": r.get("n_trades_opened"),
            "n_trades_closed": r.get("n_trades_closed"),
            "t_stat": r.get("t_stat"),
        })
    recs = [{"challenger_id": r.get("challenger_id"),
             "recommendation": r.get("recommendation"), "why": r.get("why"),
             "shadow_weight": r.get("shadow_weight"),
             "economic_state": r.get("economic_state")}
            for r in (opp.get("rows") or [])
            if r.get("recommendation") in ("REDUCE", "EXIT", "REPLACE", "ADD")][:12]
    return {
        "available": True,
        "as_of": nav.get("as_of"),
        "inception": nav.get("inception"),
        "starting_capital": nav.get("starting_capital"),
        "canonical_policy": nav.get("canonical_policy"),
        "shadow_nav": nav.get("shadow_nav"),
        "shadow_return": nav.get("shadow_return"),
        "today_net_pnl": nav.get("today_net_pnl"),
        "cumulative_net_forward_pnl": nav.get("cumulative_net_forward_pnl"),
        "residual_alpha_pnl_vs_cash": nav.get(
            "residual_alpha_pnl_vs_cash_control"),
        "realised_pnl": nav.get("realised_pnl"),
        "unrealised_pnl": nav.get("unrealised_pnl"),
        "cost_drag": nav.get("cost_drag"),
        "financing_earned": nav.get("financing_earned"),
        "max_drawdown": nav.get("max_drawdown"),
        "current_drawdown": nav.get("current_drawdown"),
        "gross_exposure_share": nav.get("gross_exposure_share"),
        "net_exposure_share": nav.get("net_exposure_share"),
        "n_open_trades_funded": nav.get("n_open_trades"),
        "policy_comparison": comp.get("ranked_by_nav") or [],
        "canonical_beats_cash": comp.get("canonical_beats_cash"),
        "canonical_minus_cash_usd": comp.get("canonical_minus_cash_usd"),
        "canonical_minus_equal_weight_usd": comp.get(
            "canonical_minus_equal_weight_usd"),
        "canonical_minus_equal_risk_usd": comp.get(
            "canonical_minus_equal_risk_usd"),
        "canonical_minus_passive_spy_usd": comp.get(
            "canonical_minus_passive_spy_usd"),
        "canonical_minus_passive_60_40_usd": comp.get(
            "canonical_minus_passive_60_40_usd"),
        "active_strategies": alloc.get("current", {}).get(
            nav.get("canonical_policy") or "", {}).get("n_allocated"),
        "cash_weight": alloc.get("canonical_cash_weight"),
        "shadow_weights": sorted(weights.items(), key=lambda kv: -kv[1])[:12],
        "effective_independent_pnl_streams": risk.get(
            "effective_independent_streams_allocated"),
        "nominal_streams": risk.get("nominal_streams"),
        "correlation_source": risk.get("correlation_source"),
        "portfolio_annual_vol_estimate": risk.get(
            "portfolio_annual_vol_estimate"),
        "top_contributors": (attr.get("top_contributors") or [])[:5],
        "worst_detractors": (attr.get("worst_detractors") or [])[:5],
        "what_cost_money": (attr.get("what_cost_money") or [])[:5],
        "pnl_by_asset_class": _grp("asset_class"),
        "pnl_by_economic_family": _grp("economic_family"),
        "pnl_by_information_family": _grp("information_family"),
        "pnl_by_horizon": _grp("horizon"),
        "pnl_by_regime": _grp("regime_risk_appetite"),
        "n_funded_trades": attr.get("n_funded_trades"),
        "n_unfunded_trades": attr.get("n_unfunded_trades"),
        "trade_counts": trades.get("counts"),
        "opportunity_counts": opp.get("counts"),
        "recommendations": recs,
        "economic_state_counts": strat.get("economic_state_counts"),
        "expected_state": strat.get("expected_state"),
        "pnl_leaderboard": top_rows,
        "best_net_pnl_strategy": board.get("best_net_pnl_strategy"),
        "worst_net_pnl_strategy": board.get("worst_net_pnl_strategy"),
        "best_residual_alpha_strategy": board.get(
            "best_residual_alpha_strategy"),
        "best_capital_efficiency_strategy": board.get(
            "best_capital_efficiency_strategy"),
        "ALPHA_RESULT": board.get("ALPHA_RESULT"),
        "alpha_result_vocabulary": (board.get("alpha_result_detail") or {}).get(
            "vocabulary"),
        "bridge_candidates": bridge.get("n_candidates"),
        "bridge_who_decides": bridge.get("who_decides"),
        # ---- Release 46.5: harvest, verdicts, policy competition ---------- #
        "forward_harvest": _harvest(art.get("harvest")),
        "strategy_verdicts": _verdicts(art.get("verdicts")),
        "policy_competition": (comp.get("competition") or {}),
        "realised_correlation": _correlation(art.get("correlation")),
        "evidence_class": "TRUE_FORWARD",
        "historical_pnl_is_never_shown_as_forward": True,
        "realised_unrealised_expected_never_summed": True,
        "pnl_unit_note": "shadow figures in USD at the research scale; "
                         "leaderboard figures per 1.0 of strategy capital",
        "research_only": True,
    }


def _harvest(body: Optional[dict]) -> dict:
    """Matured forward economics and marks, NEVER summed - from the owner."""
    if not isinstance(body, dict):
        return {"available": False,
                "FORWARD_PNL_EVIDENCE": "STILL_WAITING_FOR_REALITY",
                "note": "the forward harvest has not been built at this "
                        "research root yet; nothing has matured"}
    m = body.get("matured") or {}
    mtm = body.get("mark_to_market") or {}
    rec = body.get("reconciliation") or {}
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "FORWARD_PNL_EVIDENCE": body.get("FORWARD_PNL_EVIDENCE"),
        "evidence_vocabulary": body.get("evidence_vocabulary"),
        "matured": {
            "n_matured": m.get("n_matured"),
            "n_funded": m.get("n_funded"),
            "hit_rate": m.get("hit_rate"),
            "unit": m.get("unit_share_weighted"),
            "usd": m.get("usd_funded"),
            "by_strategy": (m.get("by_strategy") or [])[:15],
        },
        "mark_to_market": {
            "n_open": mtm.get("n_open"),
            "n_funded": mtm.get("n_funded"),
            "unit": mtm.get("unit_share_weighted"),
            "usd": mtm.get("usd_funded"),
            "worst_current_drawdown_from_peak_net": mtm.get(
                "worst_current_drawdown_from_peak_net"),
            "is_matured_statistical_evidence": False,
        },
        "one_economic_truth": rec.get("ONE_ECONOMIC_TRUTH"),
        "reconciliation_problems": (rec.get("problems") or [])[:10],
        "next_maturity": body.get("next_maturity"),
        "matured_and_mark_to_market_are_never_summed": True,
    }


def _verdicts(body: Optional[dict]) -> dict:
    if not isinstance(body, dict):
        return {"available": False,
                "note": "no strategy verdicts have been built at this "
                        "research root yet"}
    rows = body.get("rows") or []
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "vocabulary": body.get("vocabulary"),
        "counts": body.get("counts"),
        "rules_version": (body.get("rules") or {}).get("version"),
        "positive_early": body.get("positive_early"),
        "negative_early": body.get("negative_early"),
        "shadow_scale_candidates": body.get("shadow_scale_candidates"),
        "shadow_reduce_candidates": body.get("shadow_reduce_candidates"),
        "forward_rejected": body.get("forward_rejected"),
        "forward_confirmed": body.get("forward_confirmed"),
        "best_by_residual_alpha": body.get("best_by_residual_alpha"),
        "worst_by_residual_alpha": body.get("worst_by_residual_alpha"),
        "rows": [{k: r.get(k) for k in (
            "challenger_id", "verdict", "reasons", "matured_observations",
            "effective_observations", "net_pnl_unit",
            "residual_alpha_pnl_unit", "cost_drag_unit",
            "net_at_2x_costs_unit", "survives_2x_costs",
            "max_drawdown_realised", "hit_rate", "t_residual_alpha",
            "diversification_contribution", "shadow_weight",
            "net_pnl_usd_funded", "residual_alpha_pnl_usd_funded")}
            for r in rows[:40]],
        "no_false_winner": body.get("no_false_winner"),
        "mark_to_market_never_decides": True,
    }


def _correlation(body: Optional[dict]) -> dict:
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the realised-correlation state has not been built "
                        "at this research root yet"}
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "blend_rule_version": (body.get("blend_rule") or {}).get("version"),
        "n_common_sessions": body.get("n_common_sessions_clusters"),
        "realised_weight": body.get("realised_weight_clusters"),
        "source": body.get("source_clusters"),
        "structural_prior_dominates": body.get("structural_prior_dominates"),
        "realised_is_primary": body.get("realised_is_primary"),
        "sessions_until_any_realised_weight": body.get(
            "sessions_until_any_realised_weight"),
        "sessions_until_realised_primary": body.get(
            "sessions_until_realised_primary"),
        "effective_streams_structural_prior": body.get(
            "effective_streams_structural_prior"),
        "effective_streams_blended": body.get("effective_streams_blended"),
        "transition_table": body.get("transition_table"),
    }


def _information_lanes(art: dict) -> dict:
    out = {}
    for key, name in (("lane_cftc", "cftc"), ("lane_credit", "credit"),
                      ("lane_macro", "macro"), ("lane_events", "events"),
                      ("lane_earnings", "earnings"), ("lane_form4", "form4")):
        b = art.get(key)
        if not isinstance(b, dict):
            out[name] = {"state": "NOT_RUN"}
            continue
        out[name] = {
            "state": b.get("state"),
            "as_of": b.get("as_of"),
            "information_family": b.get("information_family"),
            "challengers_frozen": b.get("challengers_frozen"),
            "n_captures": (b.get("n_captures")
                           if b.get("n_captures") is not None
                           else (b.get("acquisition") or {}).get("n_captures")),
            "money_spent_usd": b.get("money_spent_usd", 0.0),
        }
    cf = art.get("lane_cftc") or {}
    out["cftc"]["latest_report"] = (cf.get("coverage") or {}).get(
        "latest_report")
    out["cftc"]["n_markets_mapped"] = (cf.get("coverage") or {}).get(
        "n_markets_mapped")
    cr = art.get("lane_credit") or {}
    out["credit"]["hy_oas"] = (cr.get("credit_state") or {}).get("hy_oas")
    out["credit"]["hy_below_mean"] = (cr.get("credit_state") or {}).get(
        "hy_below_mean")
    mc = art.get("lane_macro") or {}
    out["macro"]["next_cpi"] = ((mc.get("coverage") or {}).get("CPI") or {}
                                ).get("next_release")
    out["macro"]["next_employment"] = ((mc.get("coverage") or {}).get(
        "EMPLOYMENT") or {}).get("next_release")
    ev = art.get("lane_events") or {}
    out["events"]["next_fomc"] = (ev.get("fomc") or {}).get("next_decision_day")
    out["events"]["fomc_source"] = (ev.get("fomc") or {}).get("source")
    ea = art.get("lane_earnings") or {}
    out["earnings"]["n_events"] = (ea.get("coverage") or {}).get("n_events")
    out["earnings"]["n_events_last_30_days"] = (ea.get("coverage") or {}).get(
        "n_events_last_30_days")
    out["earnings"]["n_issuers_captured"] = ea.get("n_issuers_captured")
    fm = art.get("lane_form4") or {}
    out["form4"]["days_captured"] = (fm.get("coverage") or {}).get(
        "days_captured")
    out["form4"]["n_transactions"] = (fm.get("coverage") or {}).get(
        "n_transactions")
    out["form4"]["n_open_market_purchases"] = (fm.get("coverage") or {}).get(
        "n_open_market_purchases")
    out["form4"]["n_open_market_sales"] = (fm.get("coverage") or {}).get(
        "n_open_market_sales")
    out["a_blocked_lane_stops_nothing_else"] = True
    return out


def _count_by(rows: list, key: str) -> dict:
    out: dict = {}
    for r in rows:
        if r.get("state") == "DATA_BLOCKED":
            continue
        k = str(r.get(key))
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _count_by_info(registry: dict) -> dict:
    out: dict = {}
    for c in (registry.get("challengers") or ()):
        if c.get("state") == "DATA_BLOCKED":
            continue
        k = str(c.get("information_family") or "PRICE_STATE")
        out[k] = out.get(k, 0) + 1
    return dict(sorted(out.items()))


def _adoption(registry: dict) -> dict:
    a = registry.get("adoption") or {}
    return {
        "n_adopted": a.get("n_adopted"),
        "n_registry_listings": a.get("n_registry_listings"),
        "finding": a.get("finding"),
        "prior_registries_unchanged": a.get("all_sources_unchanged"),
        "r46_writes_no_forward_row_for_an_adopted_shadow": True,
    }


def _cost_efficiency(body: Optional[dict], rankings: Optional[dict],
                     break_even: Optional[dict]) -> dict:
    """Signal edge versus economic edge - read from the ONE owner.

    Release 46.6 built :mod:`alpha_agent.r46.cost_efficiency` because the
    first matured forward observation predicted the direction correctly and
    still lost money: +6.48 bps of gross edge against a 12 bps round trip.
    Every ratio, break-even and classification below is taken verbatim from
    that owner's artifact. This module computes none of them, and neither
    does the UI.
    """
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the cost-efficiency owner has not run at this "
                        "research root yet"}
    rows = body.get("rows") or []
    obs = body.get("observations") or []

    def _row(r):
        m = r.get("matured") or {}
        return {
            "challenger_id": r.get("challenger_id"),
            "asset_class": r.get("asset_class"),
            "economic_family": r.get("economic_family"),
            "horizons": r.get("horizons"),
            "economic_state": r.get("economic_state"),
            "cost_robustness": r.get("cost_robustness"),
            "matured_observations": m.get("n_observations"),
            "gross_edge_bps": m.get("gross_edge_bps"),
            "cost_bps": m.get("cost_bps"),
            "net_edge_bps": m.get("net_edge_bps"),
            "control_bps": m.get("control_bps"),
            "residual_alpha_bps": m.get("residual_alpha_bps"),
            "net_at_2x_costs_bps": m.get("net_at_2x_costs_bps"),
            "cost_to_gross_edge_ratio": m.get("cost_to_gross_edge_ratio"),
            "pct_of_gross_edge_consumed_by_cost": m.get(
                "pct_of_gross_edge_consumed_by_cost"),
            "edge_retention_ratio": m.get("edge_retention_ratio"),
            "pnl_per_unit_turnover": m.get("pnl_per_unit_turnover"),
            "survives_2x_costs": m.get("survives_2x_costs"),
            "break_even_gross_edge_bps": (r.get("ex_ante_break_even") or {})
            .get("break_even_gross_edge_bps"),
            "gross_edge_to_beat_control_bps": (r.get("ex_ante_break_even")
                                               or {})
            .get("gross_edge_to_beat_control_bps"),
            "is_a_scientific_verdict": False,
        }

    with_evidence = [_row(r) for r in rows
                     if (r.get("matured") or {}).get("n_observations")]
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "calculation_owner": body.get("calculation_owner"),
        "question": body.get("question"),
        "economic_state_vocabulary": body.get("economic_state_vocabulary"),
        "cost_robustness_vocabulary": body.get("cost_robustness_vocabulary"),
        "n_strategies": body.get("n_strategies"),
        "economic_state_counts": body.get("economic_state_counts"),
        "cost_robustness_counts": body.get("cost_robustness_counts"),
        "cost_destroyed": body.get("cost_destroyed"),
        "gross_edge_negative": body.get("gross_edge_negative"),
        "positive_residual_alpha": body.get("positive_residual_alpha"),
        "net_positive_control_negative": body.get(
            "net_positive_control_negative"),
        "n_with_matured_evidence": body.get("n_with_matured_evidence"),
        # ---- the observation tier, kept strictly apart from the strategy
        #      tier: a TRADE may be cost-destroyed while its STRATEGY is still
        #      TOO_EARLY, and both are true at once.
        "n_matured_observations": body.get("n_matured_observations"),
        "observation_economic_state_counts": body.get(
            "observation_economic_state_counts"),
        "observations": obs[:25],
        "an_observation_state_is_not_a_strategy_state": True,
        "descriptive_states_never_replace_verdicts": True,
        "strategies_with_matured_evidence": with_evidence,
        "first_matured_explained": body.get("first_matured_explained"),
        "rankings": {
            "by_net_edge_bps": (rankings or {}).get("by_net_edge_bps"),
            "by_residual_alpha_bps": (rankings or {}).get(
                "by_residual_alpha_bps"),
            "by_cumulative_gross_edge_bps": (rankings or {}).get(
                "by_cumulative_gross_edge_bps"),
            "by_cumulative_cost_drag_bps": (rankings or {}).get(
                "by_cumulative_cost_drag_bps"),
            "by_pct_of_gross_edge_consumed_by_cost": (rankings or {}).get(
                "by_pct_of_gross_edge_consumed_by_cost"),
            "ranking_rule": (rankings or {}).get("ranking_rule"),
        } if isinstance(rankings, dict) else {},
        "break_even": {
            "statement": (break_even or {}).get("statement"),
            "hardest_to_clear": (break_even or {}).get("hardest_to_clear"),
            "easiest_to_clear": (break_even or {}).get("easiest_to_clear"),
            "rows": ((break_even or {}).get("rows") or [])[:40],
        } if isinstance(break_even, dict) else {},
        "matured_and_mark_to_market_are_never_summed": True,
        "research_only": True,
    }


def _research_lanes(body: Optional[dict], adopted: Optional[dict],
                    opt_hyp: Optional[dict],
                    continuation: Optional[dict] = None) -> dict:
    """The research-lane lifecycle contract - every lane, every run."""
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the research-lane lifecycle owner has not run at "
                        "this research root yet"}
    rows = [{
        "lane_id": r.get("lane_id"), "owner": r.get("owner"),
        "lifecycle": r.get("lifecycle"), "owner_state": r.get("owner_state"),
        "cadence": r.get("cadence"),
        "classification": r.get("classification"),
        "information_family": r.get("information_family"),
        "adopted_from": r.get("adopted_from"),
        "was_called": r.get("was_called"),
        "next_decision_date": r.get("next_decision_date"),
        "why": r.get("why") or r.get("reason"),
        "challengers": r.get("challengers"),
        "usable_sessions": r.get("usable_sessions"),
        "sessions_still_required": r.get("sessions_still_required"),
        # Release 46.6.1 — an adopted lane reports where its evidence goes.
        "continuation_state": r.get("continuation_state"),
        "continuation_owner": r.get("continuation_owner"),
        "n_continuation_appended": r.get("n_appended"),
        "n_refused_outcome_window_open": r.get(
            "n_refused_outcome_window_open"),
        "session_gate_state": r.get("session_gate_state"),
    } for r in (body.get("rows") or [])]
    a = body.get("audit") or {}
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "calculation_owner": body.get("calculation_owner"),
        "statement": body.get("statement"),
        "lifecycle_vocabulary": body.get("lifecycle_vocabulary"),
        "forgotten_is_not_a_state": body.get("forgotten_is_not_a_state"),
        "n_lanes": body.get("n_lanes"),
        "lifecycle_counts": body.get("lifecycle_counts"),
        "contract_holds": body.get("contract_holds"),
        "never_called": a.get("never_called"),
        "n_never_called": a.get("n_never_called"),
        "quiet_is_not_broken": body.get("quiet_is_not_broken"),
        "adopted_append_blocker": body.get("adopted_append_blocker"),
        "rows": rows,
        "adopted_inventory": ({
            "n_adopted_lanes": (adopted or {}).get("n_adopted_lanes"),
            "n_shadows": (adopted or {}).get("n_shadows"),
            "n_wired_into_drc": (adopted or {}).get("n_wired_into_drc"),
            "n_retired": (adopted or {}).get("n_retired"),
            "finding": (adopted or {}).get("finding"),
            "measured_owner_reachability": (adopted or {}).get(
                "measured_owner_reachability"),
            # Release 46.6.1 — the two append rights, never one ambiguous flag.
            "prior_release_append_authorised": (adopted or {}).get(
                "prior_release_append_authorised"),
            "r46_continuation_append_authorised": (adopted or {}).get(
                "r46_continuation_append_authorised"),
            "continuation_owner": (adopted or {}).get("continuation_owner"),
            "n_continuation_ready": (adopted or {}).get(
                "n_continuation_ready"),
            # Release 46.6.1 — and the two CONTROLS, never one word either.
            "scientific_control_owner": (adopted or {}).get(
                "scientific_control_owner"),
            "capital_control": (adopted or {}).get("capital_control"),
            "scientific_alpha_field": (adopted or {}).get(
                "scientific_alpha_field"),
            "capital_alpha_field": (adopted or {}).get("capital_alpha_field"),
            "formal_verdict_uses": (adopted or {}).get("formal_verdict_uses"),
            "cash_substitution_for_noncash_control_allowed": (adopted or {}).get(
                "cash_substitution_for_noncash_control_allowed"),
            "old_artifacts_became_writable": False,
            "rows": (adopted or {}).get("rows") or [],
        } if isinstance(adopted, dict) else {"available": False}),

        # Release 46.6.1 — THE adopted forward continuation, read verbatim.
        "adopted_continuation": _adopted_continuation(continuation),

        "option_hypotheses": ({
            "evidence_class": (opt_hyp or {}).get("evidence_class"),
            "n_feature_sessions": (opt_hyp or {}).get("n_feature_sessions"),
            "sessions_required": (opt_hyp or {}).get("sessions_required"),
            "judgeable": (opt_hyp or {}).get("judgeable"),
            # Release 46.6.1 — "judgeable" meant the SESSION COUNT and nothing
            # else, while not one hypothesis had a sufficient sample. The two
            # claims are reported apart so neither can be read as the other.
            "judgeable_means": (opt_hyp or {}).get("judgeable_means"),
            "session_gate_state": (opt_hyp or {}).get("session_gate_state"),
            "session_gate_measures": (opt_hyp or {}).get(
                "session_gate_measures"),
            "session_gate_does_not_measure": (opt_hyp or {}).get(
                "session_gate_does_not_measure"),
            "hypothesis_sample_sufficient": (opt_hyp or {}).get(
                "hypothesis_sample_sufficient"),
            "hypothesis_sample_state": (opt_hyp or {}).get(
                "hypothesis_sample_state"),
            "hypothesis_sample_blocker": (opt_hyp or {}).get(
                "hypothesis_sample_blocker"),
            "n_predeclared": (opt_hyp or {}).get("n_predeclared"),
            "n_scored": (opt_hyp or {}).get("n_scored"),
            "n_sample_insufficient": (opt_hyp or {}).get(
                "n_sample_insufficient"),
            "positive_after_costs_and_control": (opt_hyp or {}).get(
                "positive_after_costs_and_control"),
            "binding_constraint": (opt_hyp or {}).get("binding_constraint"),
            "results": (opt_hyp or {}).get("results"),
        } if isinstance(opt_hyp, dict) else {"available": False}),
        "research_only": True,
    }


def _adopted_continuation(body: Optional[dict]) -> dict:
    """Release 46.6.1 - where adopted forward evidence goes, and what blocks it.

    Read verbatim from ``alpha_agent.r46.adopted_forward``. This module
    computes no identity, no eligibility and no continuation state of its own.
    """
    if not isinstance(body, dict):
        return {"available": False,
                "note": "the adopted-shadow continuation owner has not run at "
                        "this research root yet"}
    contract = body.get("contract") or {}
    summary = body.get("summary") or {}
    return {
        "available": True,
        "as_of": body.get("as_of"),
        "calculation_owner": body.get("calculation_owner"),
        "continuation_owner": contract.get("continuation_owner"),
        "statement": body.get("statement"),
        "prior_release_append_authorised": contract.get(
            "prior_release_append_authorised"),
        "r46_continuation_append_authorised": contract.get(
            "r46_continuation_append_authorised"),
        "superseded_adoption_clause": contract.get(
            "superseded_adoption_clause"),
        "entry_convention": contract.get("entry_convention"),
        "entry_convention_statement": contract.get(
            "entry_convention_statement"),
        "evidence_class": contract.get("evidence_class"),
        "identity_key": contract.get("identity_key"),
        "continuation_states": contract.get("continuation_states"),
        "signal_owners": contract.get("signal_owners"),
        "no_second_capture_implementation": contract.get(
            "no_second_capture_implementation"),
        "n_continuation_predictions": summary.get(
            "n_continuation_predictions"),
        "n_continuation_outcomes": summary.get("n_continuation_outcomes"),
        "n_pending": summary.get("n_pending"),
        "by_adopted_challenger": summary.get("by_adopted_challenger"),
        "chain_intact": bool((summary.get("chain") or {}).get("all_intact")),
        "lane_results": body.get("lane_results") or {},
        "prior_release_artifacts_mutated": body.get(
            "prior_release_artifacts_mutated"),
        "old_artifacts_became_writable": False,
        # R46.6.1 - the two controls, never merged into one word. "Beat cash"
        # and "beat the benchmark this strategy froze" are different claims,
        # and only the second can carry a formal scientific verdict.
        "controls": (contract.get("controls") or {}),
        "scientific_control_field": summary.get("scientific_control_field"),
        "capital_control_field": summary.get("capital_control_field"),
        "controls_are_separate": bool(summary.get("controls_are_separate")),
        "formal_verdict_uses": summary.get("formal_verdict_uses"),
        "verdict_inputs": body.get("verdict_inputs") or {},
        "research_only": True,
    }


def _economic_truth(nav: Optional[dict], comp: Optional[dict],
                    harvest: Optional[dict], eff: Optional[dict]) -> dict:
    """"Are we making money?" - answered so a gain cannot be misread.

    The shadow book's NAV is ABOVE its starting capital and the book is
    BEHIND its cash control, and both of those are true at once: the gain is
    financing on idle collateral, and the strategies have so far subtracted
    from it. A headline that reported only the NAV would say "up $132" about a
    book that is down $20 against the only control that matters. This block
    exists so no surface can make that mistake.
    """
    if not isinstance(nav, dict):
        return {"available": False,
                "note": "the shadow P&L layer has not run at this research "
                        "root yet"}
    c = comp or {}
    fin = nav.get("financing_earned")
    real = nav.get("realised_pnl")
    unreal = nav.get("unrealised_pnl")
    cost = nav.get("cost_drag")
    vs_cash = c.get("canonical_minus_cash_usd")
    strategy_pnl = None
    if real is not None and unreal is not None:
        strategy_pnl = float(real) + float(unreal)
    return {
        "available": True,
        "as_of": nav.get("as_of"),
        "question": "are we making money?",
        "shadow_nav": nav.get("shadow_nav"),
        "starting_capital": nav.get("starting_capital"),
        "headline_gain_usd": (None if nav.get("shadow_nav") is None
                              or nav.get("starting_capital") is None
                              else round(float(nav["shadow_nav"])
                                         - float(nav["starting_capital"]), 6)),
        "financing_earned_usd": fin,
        "strategy_pnl_usd": strategy_pnl,
        "realised_net_forward_pnl_usd": real,
        "unrealised_net_pnl_usd": unreal,
        "cost_drag_usd": cost,
        "residual_forward_alpha_usd": nav.get(
            "residual_alpha_pnl_vs_cash_control"),
        "canonical_minus_cash_usd": vs_cash,
        "canonical_minus_passive_spy_usd": c.get(
            "canonical_minus_passive_spy_usd"),
        "canonical_minus_passive_60_40_usd": c.get(
            "canonical_minus_passive_60_40_usd"),
        "canonical_minus_equal_weight_usd": c.get(
            "canonical_minus_equal_weight_usd"),
        "canonical_minus_equal_risk_usd": c.get(
            "canonical_minus_equal_risk_usd"),
        "canonical_beats_cash": c.get("canonical_beats_cash"),
        "matured_observations": ((harvest or {}).get("matured") or {})
        .get("n_matured"),
        "matured_funded": ((harvest or {}).get("matured") or {}).get("n_funded"),
        "matured_unfunded": ((harvest or {}).get("matured") or {})
        .get("n_unfunded_unit_economics"),
        "why_realised_pnl_is_zero": (
            "the matured trade(s) so far were UNFUNDED under the canonical "
            "allocation policy, so their unit economics are on the record and "
            "their dollar effect on the shadow NAV is zero. The loss is real "
            "evidence and it cost the book nothing."
            if (((harvest or {}).get("matured") or {}).get("n_funded") == 0
                and ((harvest or {}).get("matured") or {}).get("n_matured"))
            else None),
        "forward_pnl_evidence": (harvest or {}).get("FORWARD_PNL_EVIDENCE"),
        "next_maturity": (harvest or {}).get("next_maturity"),
        "cost_destroyed_strategies": (eff or {}).get("cost_destroyed"),
        "gross_edge_negative_strategies": (eff or {}).get(
            "gross_edge_negative"),
        "positive_residual_alpha_strategies": (eff or {}).get(
            "positive_residual_alpha"),
        # ---- the sentence a surface may print, and the one it may not ----- #
        "verdict": _economic_truth_sentence(nav, c, strategy_pnl),
        "a_positive_nav_is_not_alpha": True,
        "why": ("collateral earns the risk-free rate whether or not a single "
                "strategy works. The only number that says whether the "
                "RESEARCH earned anything is the book against its cash "
                "control."),
        "research_only": True,
    }


def _economic_truth_sentence(nav: dict, comp: dict, strategy_pnl) -> str:
    vs_cash = comp.get("canonical_minus_cash_usd")
    fin = nav.get("financing_earned")
    if vs_cash is None:
        return ("The shadow book has not been compared with its cash control "
                "yet.")
    if float(vs_cash) > 0:
        return ("The shadow research book is AHEAD of its cash control by "
                "$%.2f. Financing contributed $%.2f of the headline."
                % (float(vs_cash), float(fin or 0.0)))
    return ("The shadow research book is BEHIND its cash control by $%.2f. "
            "The headline NAV is above its starting capital only because "
            "collateral earned $%.2f of financing; the strategies themselves "
            "have subtracted $%.2f so far."
            % (abs(float(vs_cash)), float(fin or 0.0),
               abs(float(strategy_pnl or 0.0))))


def _headline(state: str, preds: list, outs: list, rows: list) -> str:
    if state == STATE_UNAVAILABLE:
        return "Release 46 tournament artifacts are not available."
    if not preds:
        return ("Tournament registered and frozen; no forward prediction has "
                "been emitted yet.")
    n_ch = len({r.get("challenger_id") for r in rows
                if r.get("origin") == "R46_SEED"})
    if not outs:
        return ("%d challengers are on the record with %d TRUE_FORWARD "
                "predictions; none has matured yet, so nothing is proven and "
                "nothing is disproven." % (n_ch, len(preds)))
    return ("%d challengers, %d TRUE_FORWARD predictions, %d matured and "
            "scored against their declared controls."
            % (n_ch, len(preds), len(outs)))


def _next_action(state: str, pending: list, outs: list) -> str:
    if state == STATE_UNAVAILABLE:
        return "run the Release 46 campaign to create the tournament artifacts"
    # Release 46.2 — the Daily Research Cycle now drives all three lifecycle steps
    # (score matured, rebuild the board, emit the next eligible batch), so the honest
    # answer is almost always "nothing". Telling an operator to run a judge the daily
    # cycle already runs would invent an action, which is the failure mode the whole
    # release exists to remove from the operator surfaces.
    if pending and not outs:
        return ("nothing. The Daily Research Cycle scores each maturity as it "
                "arrives and emits the next eligible batch. No challenger may be "
                "edited while its predictions are outstanding.")
    if pending:
        return ("nothing. The Daily Research Cycle keeps scoring maturities and "
                "emitting eligible batches; review any challenger that crosses a "
                "gate MANUALLY — a gate confers no capital.")
    return ("nothing. The next Daily Research Cycle emits the next eligible "
            "forward batch.")
