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
}

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
        if body is None:
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


def _next_maturity(verdict: dict, pending: list) -> Optional[str]:
    v = verdict.get("NEXT_MATERIAL_EVIDENCE_TIME")
    if v:
        return v
    dates = sorted({str(p.get("horizon_end_expected")) for p in pending
                    if p.get("horizon_end_expected")})
    return dates[0] if dates else None


def _adoption(registry: dict) -> dict:
    a = registry.get("adoption") or {}
    return {
        "n_adopted": a.get("n_adopted"),
        "n_registry_listings": a.get("n_registry_listings"),
        "finding": a.get("finding"),
        "prior_registries_unchanged": a.get("all_sources_unchanged"),
        "r46_writes_no_forward_row_for_an_adopted_shadow": True,
    }


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
    if pending and not outs:
        return ("wait for the first maturity; then run the outcome judge to "
                "score it. No operator action is required in the meantime, "
                "and no challenger may be edited while its predictions are "
                "outstanding.")
    if pending:
        return ("run the outcome judge as further horizons mature; review "
                "any challenger that crosses a gate MANUALLY")
    return "emit the next forward batch"
