"""alpha_agent.r45.closeout - the verdict, in the words the contract requires.

Every key the contract declares is present, whether or not the answer is
flattering. The terminal state is chosen by a rule written down before the
first result, not by how the numbers happened to land.
"""
from __future__ import annotations

import datetime as _dt
import json

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.closeout"


def _sleeve_result(rep: dict, sleeve: str) -> str:
    rows = (rep or {}).get("by_sleeve", {}).get(sleeve, [])
    if not rows:
        return "NOT_TESTED"
    judged = [r for r in rows
              if (r.get("n_events") or 0) >= C.MIN_EVENTS_TO_JUDGE_REPLICATION]
    if not judged:
        return "DATA_INSUFFICIENT"
    if any(r.get("replication_state") == "REPLICATES" for r in judged):
        return "REPLICATES"
    best = max(judged, key=lambda r: (r.get("net_t_cluster") or -9))
    return (f"DOES_NOT_REPLICATE (best {best['symbol']} "
            f"{best['net_bps_per_event']:+.2f} bps, t "
            f"{best.get('net_t_cluster') or 0:+.2f}, n {best['n_events']})")


def terminal_state(rep: dict, rv: dict, front: dict) -> str:
    if (front or {}).get("n_qualified"):
        return "R45_QUALIFIED_EVENT_ALPHA_FOUND"
    native = (rep or {}).get("L3_NATIVE_FUTURES", {}).get("replication_state")
    if native == "REPLICATES":
        return "R45_NATIVE_FUTURES_EVENT_ALPHA_CANDIDATE_FOUND"
    if (rv or {}).get("n_holdout_survivors"):
        return "R45_EVENT_RELATIVE_VALUE_ALPHA_CANDIDATE_FOUND"
    if (front or {}).get("n_research_candidates"):
        return "R45_STRONG_EVENT_CANDIDATE_FORWARD_PENDING"
    gold_bc = ((rep or {}).get("L1_GOLD_HOLDOUT", {})
               .get("replication_state"))
    if gold_bc == "REPLICATES":
        return "R45_GOLD_SPECIFIC_EFFECT_NOT_GENERAL_MACRO_ALPHA"
    judged_any = any(
        (r.get("n_events") or 0) >= C.MIN_EVENTS_TO_JUDGE_REPLICATION
        for r in (rep or {}).get("ranked", []))
    if not judged_any:
        return "R45_NATIVE_FUTURES_DATA_WALL_BINDING"
    return "R45_R44_MACRO_EFFECT_REFUTED_IN_NATIVE_MARKETS"


def build(result: dict) -> dict:
    lanes = result.get("lanes", {})
    rep = lanes.get("L1_L4_REPLICATION", {})
    causal = lanes.get("L5_L6_CAUSAL", {})
    disc = lanes.get("L7_DISCOVERY", {})
    rv = lanes.get("L8_RV", {})
    sur = lanes.get("L9_SURPRISE", {})
    mlr = lanes.get("L10_L11_STATE_AND_ML", {})
    kill = lanes.get("L12_KILL", {})
    opts = lanes.get("L13_OPTIONS", {})
    anl = lanes.get("L14_ANALYST", {})
    front = result.get("frontier", {})
    shadows = result.get("shadows", {})
    purchase = result.get("purchase", {})
    burden = result.get("burden", {})
    shell = result.get("shell_policy", {})

    best = (front or {}).get("best") or {}
    gold = (rep or {}).get("L1_GOLD_HOLDOUT", {})
    decay = gold.get("out_of_sample_decay", {})
    cs = (kill or {}).get("cost_stress", {})
    ls = (kill or {}).get("latency_stress", {})

    body = {
        "schema": "r45_final_verdict/1",
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "generated_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "contract_hash": result.get("contract_hash"),

        "QUALIFIED_ALPHA_RESULT":
            "NO_QUALIFIED_ALPHA" if not front.get("n_qualified")
            else "QUALIFIED_ALPHA_FOUND",
        "FROZEN_R44_RULE_NATIVE_REPLICATION_RESULT":
            rep.get("FROZEN_R44_RULE_NATIVE_REPLICATION_RESULT"),

        "BEST_CANDIDATE": best.get("CANDIDATE_ID"),
        "BEST_INSTRUMENT": best.get("INSTRUMENTS"),
        "BEST_EVENT_FAMILY": best.get("EVENT_FAMILY"),
        "BEST_HORIZON": f"+{C.FROZEN_RULE['entry_delay_min']}m entry / "
                        f"+{C.FROZEN_RULE['hold_min']}m hold",
        "BEST_ECONOMIC_EXPRESSION": best.get("ECONOMIC_EXPRESSION"),
        "BEST_MODEL": best.get("MODEL"),
        "BEST_NET_BPS_PER_EVENT": best.get("NET_BPS_PER_EVENT"),
        "BEST_NET_T": best.get("NET_T"),
        "BEST_HIT_RATE": best.get("HIT_RATE"),
        "BEST_CAPACITY": best.get("CAPACITY"),
        "BEST_QUALIFICATION_STATE": best.get("QUALIFICATION_STATE"),

        "RATES_REPLICATION_RESULT": _sleeve_result(rep, "RATES"),
        "EQUITY_INDEX_REPLICATION_RESULT": _sleeve_result(rep, "EQUITY"),
        "GOLD_REPLICATION_RESULT": _sleeve_result(rep, "GOLD"),
        "FX_REPLICATION_RESULT": _sleeve_result(rep, "FX"),
        "RELATIVE_VALUE_RESULT": rv.get("RELATIVE_VALUE_RESULT"),

        "EVENT_CAUSALITY_RESULT": causal.get("EVENT_CAUSALITY_RESULT"),
        "PLACEBO_RESULT": (causal.get("verdicts_by_zone", {}) or {}),
        "TIMING_PERTURBATION_RESULT": {
            z: {"peak_offset_min": v.get("peak_offset_min"),
                "peak_at_declared_minute":
                    v.get("peak_is_at_the_declared_minute")}
            for z, v in (causal.get("timing_sweeps_by_zone", {}) or {}).items()
        },
        "COST_STRESS_RESULT": {
            "measured_on": kill.get("symbol"),
            "zone": kill.get("zone"),
            "survives_x2": cs.get("survives_x2"),
            "survives_x3": cs.get("survives_x3"),
            "highest_surviving_multiplier":
                cs.get("highest_surviving_multiplier")},
        "LATENCY_RESULT": {
            "measured_on": kill.get("symbol"),
            "zone": kill.get("zone"),
            "survives_plus_1min": ls.get("survives_plus_1min"),
            "latest_useful_entry_min": ls.get("latest_useful_entry_min")},
        "KILL_BATTERY_ON_THE_GOLD_HOLDOUT": {
            k: (lanes.get("L12_KILL_GOLD_HOLDOUT", {}) or {}).get(k)
            for k in ("symbol", "zone", "KILL_BATTERY_RESULT",
                      "first_failure", "n_checks_passed", "n_checks",
                      "checks")},
        "SEARCH_ADJUSTED_RESULT": rv.get("benjamini_hochberg_on_holdout"),
        "FORWARD_SHADOWS_ADDED": shadows.get("FORWARD_SHADOWS_ADDED", 0),

        "GLOBAL_SEARCH_BURDEN": burden.get("GLOBAL_SEARCH_BURDEN"),
        "GLOBAL_SEARCH_BURDEN_CONSERVATIVE":
            burden.get("GLOBAL_SEARCH_BURDEN_CONSERVATIVE"),
        "NEW_R45_EFFECTIVE_TRIALS": burden.get("new_r45_effective_trials"),

        "OPTIONS_PROGRESS": {
            "sessions_now": (opts.get("surface", {}) or {}).get(
                "sessions_total"),
            "sessions_added_by_r45": (opts.get("surface", {}) or {}).get(
                "sessions_added_by_r45"),
            "still_required": (opts.get("judgeable_sample", {}) or {}).get(
                "sessions_still_required"),
            "state": (opts.get("judgeable_sample", {}) or {}).get("state"),
            "blocker": opts.get("blocker")},
        "ANALYST_REVISION_PROGRESS": {
            "span_days": (anl.get("ledger", {}) or {}).get("span_days"),
            "snapshots": (anl.get("ledger", {}) or {}).get("n_snapshot_dates"),
            "days_added_since_r44": (anl.get("growth_since_r44", {}) or {})
            .get("days_added"),
            "observed_revisions": (anl.get("revision_frequency", {}) or {})
            .get("n_observed_revisions"),
            "still_required": (anl.get("judgeable_sample", {}) or {}).get(
                "still_required"),
            "blocker": anl.get("blocker")},

        "NATIVE_FUTURES_DATA_RESULT": {
            "acquired_at_zero_cost": sorted(C.NATIVE_FUTURES_INSTRUMENTS),
            "resolution": "1-minute layered over 5-minute",
            "window": "approximately the last 71 days",
            "events_obtained": purchase.get(
                "native_futures_events_obtained_at_zero_cost"),
            "events_needed_to_judge": C.MIN_EVENTS_TO_JUDGE_REPLICATION,
            "state": rep.get("L3_NATIVE_FUTURES", {}).get(
                "replication_state"),
            "deep_history_blocker": "ACCOUNT_REQUIRED"},
        "TOP_DATA_PURCHASE_RECOMMENDATION":
            purchase.get("TOP_DATA_PURCHASE_RECOMMENDATION"),
        "TOP_RECOMMENDATION_STATE": purchase.get("TOP_RECOMMENDATION_STATE"),
        "EXACT_PRICE_IF_ANY": purchase.get("EXACT_PRICE_IF_ANY"),
        "ACCOUNT_REQUIRED": purchase.get("ACCOUNT_REQUIRED"),
        "PAYMENT_REQUIRED": purchase.get("PAYMENT_REQUIRED"),
        "EXTERNAL_BLOCKERS": [r.get("provider") + ": " + r.get("state", "")
                              for r in (purchase.get("blocked_routes") or [])],

        "MONEY_SPENT": 0.0, "NEW_ACCOUNTS": 0, "LICENCES_ACCEPTED": 0,
        "OPERATIONAL_WRITES": 0, "PORTFOLIO_MUTATIONS": 0, "ORDERS": 0,
        "MODEL_PROMOTIONS": 0, "SCHEDULER_CHANGES": 0,
        "SHELL_POLICY_VIOLATION": shell.get("SHELL_POLICY_VIOLATION"),

        "ML_ADDED_ECONOMIC_VALUE": mlr.get("ML_ADDED_ECONOMIC_VALUE"),
        "PRICE_DISCOVERY_ORDERING": disc.get(
            "speed_ranking_fastest_first"),
        "NEAR_REAL_TIME_REQUIREMENT": disc.get("latency_budget"),
        "SURPRISE_RESULT": {
            "state": sur.get("state"),
            "n_matched": sur.get("n_events_matched"),
            "corr_abs_surprise_vs_abs_shock":
                sur.get("abs_surprise_vs_abs_shock_correlation"),
            "reaction_scales_with_surprise":
                sur.get("reaction_scales_with_surprise")},

        "OUT_OF_SAMPLE_DECAY_OF_THE_R44_EFFECT": decay,
        "TERMINAL": terminal_state(rep, rv, front),
    }
    return body


def write(result: dict) -> dict:
    body = build(result)
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    (C.ARTIFACT_DIR / "R45_FINAL_VERDICT.json").write_text(
        json.dumps(body, indent=2, default=str), encoding="utf-8")
    return body
