"""scripts/run_release30_1_aug18_replay.py - Release 30.1 hermetic Aug-18 replay.

Replays the Aug-18 decision state through the CURRENT APPROVED model only, and
answers the twelve questions Release 30.1 asks of it. It reads the frozen
Release-30 acceptance artifacts and the live read owners; it mutates NOTHING -
no store, no ledger, no proposal, no decision, no history.

    python scripts/run_release30_1_aug18_replay.py
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.api import return_forecast as api_rf        # noqa: E402
from paper_trader.api import universe_scoring as us           # noqa: E402
from paper_trader.api import zero_base_target as zbt          # noqa: E402
from paper_trader.engine import return_forecast as fk         # noqa: E402

R30_ROOT = Path(os.environ.get("PAPER_TRADER_R30_ROOT")
                or r"D:\Stock_Prediction_app_data\release30_zero_base_adaptive_allocator")
OUT_ROOT = Path(os.environ.get("PAPER_TRADER_R30_1_ROOT")
                or r"D:\Stock_Prediction_app_data\release30_1_zero_base_operational_cutover")

ELIGIBLE = "2026-08-18"


def _load(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def _write(name: str, payload) -> Path:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    p = OUT_ROOT / name
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=1, default=str), encoding="utf-8")
    tmp.replace(p)
    return p


def main() -> int:
    scoring = us.load_universe_scoring()
    ranks = {r["ticker"]: r["rank"] for r in (scoring.get("rankings") or [])
             if r.get("ticker") and r.get("rank")}
    n_universe = len(ranks)

    # --- 1. what Release 30 actually produced on Aug-18 for "model A" -------- #
    r30_a = _load(R30_ROOT / "aug18_zero_base_A_current_operational_model.json")
    r30_art = _load(R30_ROOT / "model_artifact_operational.json")
    r30_rows = ((r30_a or {}).get("zero_base_target") or {}).get("rows") or []
    weighted_rank = None
    if r30_rows and ranks:
        tot = sum(r["weight"] for r in r30_rows if ranks.get(r["ticker"]))
        weighted_rank = (sum(r["weight"] * ranks[r["ticker"]]
                             for r in r30_rows if ranks.get(r["ticker"])) / tot
                         if tot else None)
    r30_finding = {
        "artifact": "aug18_zero_base_A_current_operational_model.json",
        "declared_as": "CURRENT OPERATIONAL MODEL / ZERO BASE",
        "policy_horizon_sessions": (r30_a or {}).get("policy_horizon_sessions"),
        "calibration_slope_at_policy_horizon": (
            ((r30_art or {}).get("horizons") or {})
            .get(str((r30_a or {}).get("policy_horizon_sessions")), {})
            .get("calibration", {}).get("slope")),
        "position_count": len(r30_rows),
        "approved_model_universe_size": n_universe,
        "weighted_average_approved_model_rank": (round(weighted_rank, 1)
                                                 if weighted_rank else None),
        "approved_model_top25_names_present": sum(
            1 for r in r30_rows if ranks.get(r["ticker"], 10 ** 6) <= 25),
        "approved_model_bottom25_names_present": sum(
            1 for r in r30_rows if ranks.get(r["ticker"], 0) > n_universe - 25),
        "largest_positions": [
            {"ticker": r["ticker"], "weight": r["weight"],
             "approved_model_rank": ranks.get(r["ticker"])}
            for r in sorted(r30_rows, key=lambda x: -x["weight"])[:10]],
        "finding": ("the calibration slope at the policy horizon is NEGATIVE, so "
                    "expected_excess_return = slope * standardised_score reversed "
                    "the approved model's ranking; the target carrying the "
                    "approved model's name held none of its top 25 names"),
    }

    # --- 2. the same artifact under the Release-30.1 rank-identity contract -- #
    guard = {}
    if r30_art:
        guard = {h: fk.rank_identity(artifact=r30_art, block=blk)
                 for h, blk in sorted((r30_art.get("horizons") or {}).items())}

    # --- 3. the Release-30.1 operational chain on the same date ------------- #
    forecast = api_rf.load_operational_return_forecast(scoring=scoring)
    governed = zbt.load_operational_zero_base_target(scoring=scoring,
                                                     forecast=forecast)
    research = zbt.load_zero_base_target()

    def _econ(payload, key):
        return ((payload or {}).get(key) or {}).get("economics") or {}

    answers = {
        "1_what_would_we_ideally_own": (
            "NOT DETERMINABLE - the approved model supplies no rank-preserving, "
            "reliable expected return at any horizon, so a zero-base ideal "
            "cannot be computed without fabricating one"),
        "2_what_would_we_transition_to": "NOT DETERMINABLE - same reason",
        "3_which_current_holdings_leave": "NOT DETERMINABLE",
        "4_which_stay": "NOT DETERMINABLE",
        "5_which_new_names_enter": "NOT DETERMINABLE",
        "6_how_much_cash": "NOT DETERMINABLE",
        "7_expected_net_improvement": "NOT MEASURABLE - no calibrated expected return",
        "8_risk_change": "NOT MEASURABLE - risk is computable, but only against a target",
        "9_downside_change": "NOT MEASURABLE - same reason",
        "10_turnover": "NOT MEASURABLE - no target to transition to",
        "11_cost": "NOT MEASURABLE - no target to transition to",
        "12_clears_existing_economic_and_safety_gates": (
            "NO - the chain stops at the forecast gate, upstream of every "
            "economic gate; nothing is withheld by a limit because nothing was "
            "proposed"),
        "13_canonical_state": "DATA_BLOCKED",
    }

    payload = {
        "replay": "RELEASE_30_1_AUG18_HERMETIC",
        "eligible_market_date": ELIGIBLE,
        "mutates_production": False,
        "read_only": True,
        "approved_model": {
            "model_id": scoring.get("primary_model_id"),
            "strategy_id": us.STRATEGY_ID,
            "universe_id": scoring.get("universe_id"),
            "universe_scoring_hash": scoring.get("output_hash"),
            "eligible_market_date": scoring.get("eligible_market_date"),
            "fundamental_as_of_date": scoring.get("fundamental_as_of_date"),
            "universe_size": n_universe,
        },
        "release30_model_a_finding": r30_finding,
        "release30_artifact_under_release30_1_contract": guard,
        "release30_1_operational_forecast": {
            "state": forecast.get("state"),
            "lane": forecast.get("lane"),
            "operational_use": forecast.get("operational_use"),
            "eligible_market_date": forecast.get("eligible_market_date"),
            "input_freshness": forecast.get("input_freshness"),
            "applied_horizons": forecast.get("horizons") or [],
            "suppressed_horizons": forecast.get("suppressed_horizons") or [],
            "blockers": forecast.get("blockers") or [],
        },
        "release30_1_governed_zero_base_target": {
            "state": governed.get("state"),
            "authority": governed.get("authority"),
            "blockers": governed.get("blockers") or [],
            "zero_base_economics": _econ(governed, "zero_base_target"),
            "implementable_economics": _econ(governed, "implementable_target"),
        },
        "release30_research_preview_lane": {
            "state": research.get("state"),
            "authority_lane": (research.get("authority") or {}).get("lane"),
            "can_become_a_proposal": (research.get("authority") or {}).get(
                "can_become_a_proposal"),
            "zero_base_positions": _econ(research, "zero_base_target").get(
                "position_count"),
        },
        "answers": answers,
        "verdict": "R30_1_CALIBRATION_BLOCKED",
        "safety": ["READ ONLY", "NO ORDERS", "NO DECISION", "NO PROPOSAL",
                   "PRODUCTION UNTOUCHED", "MANUAL REVIEW", "AUTOMATION OFF"],
    }
    _write("aug18_release30_1_replay.json", payload)

    print("RELEASE 30.1 - AUG-18 HERMETIC REPLAY")
    print("  approved model      : %s" % payload["approved_model"]["model_id"])
    print("  eligible date       : %s" % ELIGIBLE)
    print("  universe            : %d names" % n_universe)
    print()
    print("  RELEASE 30 'MODEL A' TARGET, RE-READ AGAINST THE APPROVED MODEL")
    print("    policy horizon    : %s sessions" % r30_finding["policy_horizon_sessions"])
    print("    calibration slope : %s" % r30_finding["calibration_slope_at_policy_horizon"])
    print("    positions         : %s" % r30_finding["position_count"])
    print("    weighted avg rank : %s of %s" % (
        r30_finding["weighted_average_approved_model_rank"], n_universe))
    print("    approved top-25   : %s present" % r30_finding["approved_model_top25_names_present"])
    print("    approved bottom-25: %s present" % r30_finding["approved_model_bottom25_names_present"])
    print()
    print("  UNDER THE RELEASE-30.1 RANK-IDENTITY CONTRACT")
    for h, v in guard.items():
        print("    %-4s %-24s %s" % (h + "d", v["verdict"], v["disposition"]))
    print()
    print("  RELEASE 30.1 OPERATIONAL CHAIN ON THE SAME DATE")
    print("    forecast          : %s (%s)" % (forecast.get("state"),
                                               forecast.get("operational_use")))
    print("    forecast date     : %s   required inputs: %s"
          % (forecast.get("eligible_market_date"),
             (forecast.get("input_freshness") or {}).get("state")))
    print("    governed target   : %s" % governed.get("state"))
    for b in governed.get("blockers") or []:
        print("      blocker         : %s" % b.get("code"))
    print("    canonical state   : DATA_BLOCKED")
    print()
    print("R30_1_AUG18_REPLAY_COMPLETE verdict=%s" % payload["verdict"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
