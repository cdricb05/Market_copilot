"""alpha_agent.r57.competition - the research-only capital competition and the
machine-readable experiment registry.

The competition admits ONLY competitors that passed their appropriate
historical gates. R57 produced zero HISTORICAL_ALPHA_CANDIDATEs, so the
eligible field is exactly what it was before the campaign: the incumbent
operational strategy, SPY and cash - and saying so IS the result. No capital
moves; nothing is promoted.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import now_iso, read_artifact, write_artifact

COMPETITION_ARTIFACT = "capital_competition.json"
REGISTRY_ARTIFACT = "r57_experiment_registry.json"
REGISTRY_WORKTREE = Path(__file__).resolve().parents[2] / "research" / "r57" / "R57_EXPERIMENT_REGISTRY.json"


def build_competition() -> dict:
    eq = read_artifact("campaign_verdicts.json") or {}
    cal = read_artifact("calibration_results.json") or {}
    candidates = [f for f, v in (eq.get("equity_verdicts") or {}).items()
                  if v.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE"]
    candidates += [f for f, v in (eq.get("futures_verdicts") or {}).items()
                   if v.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE"]
    body = {
        "track": "CAPITAL_COMPETITION",
        "starting_assumption": "ALL INVESTABLE CAPITAL IS CASH",
        "eligibility_rule": ("only research candidates that passed their "
                             "pre-registered historical gates may compete"),
        "r57_qualified_candidates": candidates,
        "eligible_competitors": ["incumbent_operational_equity_strategy",
                                 "CASH", "SPY"] + candidates,
        "zero_base_research_portfolio": {
            "answer": ("UNCHANGED BY R57: no new competitor qualified, so the "
                       "zero-base research answer remains the R56 surface's "
                       "(api.cash_deployment_frontier / api.zero_base_target), "
                       "whose forecast lane is itself research-only and whose "
                       "governed lane still has no calibrated expected return"),
            "r57_contribution": ("the calibration track TESTED whether the "
                                 "strongest validation-positive signal could be "
                                 "calibrated to expected return and it FAILED "
                                 "out of sample; NOT_CALIBRATED is measured, "
                                 "not merely unattempted"),
        },
        "transition_aware_research_portfolio": "unchanged for the same reason",
        "cash_deployment_ladder": ("unchanged; R56's measured answer stands: "
                                   "deploying the ~4.5% idle cash clears only "
                                   "the research-forecast hurdle (~$3/horizon) "
                                   "and no governed economic hurdle exists"),
        "cross_asset_marginal_frontier": {
            "equity": "no qualified new sleeve",
            "futures": ("no qualified sleeve: best lockbox net Sharpe 0.44 "
                        "(F3 cross-market momentum) fails the pre-registered "
                        "materiality-with-robustness gates"),
            "cash": "remains a first-class destination and the default",
        },
        "next_1000_usd": {
            "research_answer": ("HOLD AS CASH pending a qualified competitor; "
                                "the only evidenced alternative remains the "
                                "incumbent book itself, whose realised excess "
                                "vs SPY is deeply negative and whose buy side "
                                "is the measured weak point"),
            "governed_answer": "MANUAL_REVIEW_REQUIRED_NO_ECONOMIC_PROOF",
        },
        "next_10000_usd": "same answer at 10x scale; no capacity constraint binds at this size",
        "cash_becomes_preferable": ("immediately, for any deployment that "
                                    "cannot name a positive after-cost "
                                    "expected edge; R57 found none it could "
                                    "defend"),
        "expected_return_state": (cal.get("expected_return_state")
                                  or "NOT_CALIBRATED"),
        "no_capital_moves": True,
    }
    write_artifact(COMPETITION_ARTIFACT, body)
    return body


def build_registry() -> dict:
    eq = read_artifact("campaign_verdicts.json") or {}
    eq_sel = read_artifact("equity_validation_selection.json") or {}
    fut_sel = read_artifact("futures_validation_selection.json") or {}
    cal = read_artifact("calibration_results.json") or {}
    con = read_artifact("construction_results.json") or {}
    to = read_artifact("turnover_results.json") or {}
    t1 = read_artifact("track1_incumbent_diagnosis.json") or {}

    rows = []

    def add(hid, family, info, asset, horizon, status, reason, result,
            quality, next_action, reopen):
        rows.append({
            "hypothesis_id": hid, "economic_family": family,
            "information_family": info, "asset_class": asset,
            "horizon": horizon, "data_readiness": "DATA_READY",
            "novelty_vs_prior_work": "pre-registered R57 family",
            "expected_information_value": None,
            "status": status, "reason": reason, "result": result,
            "evidence_quality": quality, "next_action": next_action,
            "reopen_condition": reopen,
        })

    for fam_id, v in (eq.get("equity_verdicts") or {}).items():
        sel = (eq_sel.get("families") or {}).get(fam_id, {})
        add("r57_" + fam_id.lower(), fam_id, "PRICE_STATE", "US_EQUITY",
            "21d" if fam_id != "E2_SHORT_REVERSAL" else "5d",
            v["verdict"],
            "failed gates: " + ",".join(v.get("failed_gates") or []) or "passed",
            {"selected_variant": sel.get("selected_variant"),
             "validation_ann_net_excess": sel.get("validation_ann_net_excess"),
             "lockbox_ann_net_excess": v.get("lockbox_ann_net_excess"),
             "lockbox_t": v.get("lockbox_t"),
             "lockbox_periods": v.get("lockbox_periods")},
            "HISTORICAL_FINAL_OOS on survivorship-safe PIT panel, 2023-01..2026-07",
            "no re-run without new information; forward families already live in R46",
            "a regime-stability overlay with its own pre-registered protocol, or new orthogonal information")

    for fam_id, v in (eq.get("futures_verdicts") or {}).items():
        sel = (fut_sel.get("families") or {}).get(fam_id, {})
        add("r57_" + fam_id.lower(), fam_id, "PRICE_STATE",
            "MULTI_ASSET_FUTURES", "5d-cadence daily-marked",
            v["verdict"],
            "failed gates: " + ",".join(v.get("failed_gates") or []),
            {"selected_variant": sel.get("selected_variant"),
             "validation_net_sharpe": sel.get("validation_net_sharpe"),
             "lockbox_net_sharpe": v.get("lockbox_net_sharpe"),
             "lockbox_ann_net_return": v.get("lockbox_ann_net_return")},
            "HISTORICAL_FINAL_OOS on 103 Norgate continuous markets, both roll methodologies",
            "F3 cross-market momentum is the strongest rejected futures family; a v2 protocol could pre-register it alone",
            "a pre-registered v2 futures protocol (fresh lockbox impossible - would need forward evidence)")

    rows.append({
        "hypothesis_id": "r57_calibration_e6",
        "economic_family": "SCORE_TO_RETURN_CALIBRATION",
        "information_family": "DERIVED", "asset_class": "US_EQUITY",
        "horizon": "21d", "data_readiness": "DATA_READY",
        "novelty_vs_prior_work": "first OOS-judged calibration attempt",
        "expected_information_value": None,
        "status": "REJECTED_OOS",
        "reason": "lockbox MAE worse than zero forecast; decile ordering inverted (tau -0.47)",
        "result": {k: cal.get(k) for k in ("selected_method", "shrinkage",
                                            "validation_mae", "lockbox_mae",
                                            "zero_forecast_mae",
                                            "lockbox_kendall_tau")},
        "evidence_quality": "HISTORICAL_FINAL_OOS",
        "next_action": "expected_return_state remains NOT_CALIBRATED and that is the published answer",
        "reopen_condition": "a signal whose decile ordering is stable across validation and lockbox",
    })
    rows.append({
        "hypothesis_id": "r57_track1_buy_sell_asymmetry",
        "economic_family": "INCUMBENT_DIAGNOSIS",
        "information_family": "TRUE_FORWARD_LEDGER", "asset_class": "US_EQUITY",
        "horizon": "1/5/20d", "data_readiness": "DATA_READY",
        "novelty_vs_prior_work": "first component-level buy/sell split on forward data",
        "expected_information_value": None,
        "status": "DIAGNOSTIC_COMPLETE",
        "reason": "diagnostic only; tiny n by construction",
        "result": {"summary": ("fundamental leg positive BOTH sides at every "
                               "horizon; momentum leg INVERTED (bottom decile "
                               "outperformed, IC negative); blend buy side "
                               "dragged negative at h20"),
                   "detail_artifact": "track1_incumbent_diagnosis.json"},
        "evidence_quality": "FORWARD n=8..25 sessions - directional only",
        "next_action": ("the operational question is the momentum leg's blend "
                        "weight / regime handling - a GOVERNED model review "
                        "question, not an R57 research claim"),
        "reopen_condition": "n/a - accrues automatically with the forward ledger",
    })
    rows.append({
        "hypothesis_id": "r57_turnover_bands",
        "economic_family": "TURNOVER_CONTROL",
        "information_family": "DERIVED", "asset_class": "US_EQUITY",
        "horizon": "21d", "data_readiness": "DATA_READY",
        "novelty_vs_prior_work": "hysteresis bands judged on after-cost OOS, not on turnover",
        "expected_information_value": None,
        "status": "DIAGNOSTIC_COMPLETE",
        "reason": "no HISTORICAL_ALPHA_CANDIDATE existed to attach it to",
        "result": {"summary": ("rank-band hysteresis (K_out=75..150) cut "
                               "turnover 35-61% with lockbox net excess flat "
                               "(+0.093 vs +0.093); after-cost performance "
                               "held, not improved"),
                   "detail_artifact": "turnover_results.json"},
        "evidence_quality": "HISTORICAL_FINAL_OOS diagnostic on a rejected family",
        "next_action": ("carries to the operational stack as a construction "
                        "candidate through GOVERNED review (R56 already showed "
                        "0/17 live swaps clear payback)"),
        "reopen_condition": "n/a",
    })

    body = {"registry": rows, "n_hypotheses": len(rows),
            "protocol": "research/r57/R57_RESEARCH_PROTOCOL.json",
            "generated_at": now_iso()}
    write_artifact(REGISTRY_ARTIFACT, body)
    REGISTRY_WORKTREE.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_WORKTREE.write_text(json.dumps(body, indent=1, default=str),
                                 encoding="utf-8")
    return body
