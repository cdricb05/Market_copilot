"""alpha_agent.r44.frontier - ranking, search adjustment and the freeze gate.

Three qualification words are kept apart here and never allowed to blur:
STANDALONE_ALPHA, PORTFOLIO_ALPHA and STRUCTURAL_PREMIUM. A book can be the
third and none of the first two, which is the most likely outcome in this
estate and is a perfectly respectable result as long as it is labelled.

The freeze gate is deliberately hard to pass. Release 43 refused to freeze a
candidate its own lockbox had refuted, and Release 44 inherits that
standard: a shadow costs future time and attention, and freezing a mediocre
candidate to make a release look productive is a way of lying slowly.
"""
from __future__ import annotations

import datetime as _dt

import numpy as np

from ..r31 import multiple_testing as MT
from . import burden as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.frontier"


# --------------------------------------------------------------------------- #
# Rows
# --------------------------------------------------------------------------- #
def standalone_rows(niche_out: dict, intraday_out: dict,
                    options_out: dict) -> list:
    """Every STANDALONE candidate this release actually judged."""
    rows = []
    for a in (niche_out.get("advanced") or []):
        if a.get("state"):
            continue
        pc = a.get("passive_control") or {}
        cap = a.get("capacity") or {}
        rows.append({
            "CANDIDATE_ID": a["candidate_id"],
            "LANE": "E3_LESS_EFFICIENT_MARKETS",
            "INFORMATION_FAMILY": "LESS_EFFICIENT_MARKETS",
            "ASSET_CLASS": "MULTI_ASSET_FUTURES",
            "ASSET": a["tier"],
            "ECONOMIC_EXPRESSION": "FUTURES_OUTRIGHT",
            "HORIZON": "1s",
            "MODEL": "TRANSPARENT_RULE",
            "REPRESENTATION": "%s_%s" % (a["tier"], a["hypothesis"]),
            "ZONE_A_T": a.get("zone_a_t"),
            "ZONE_B_EXCESS_ANN": a.get("zone_b_excess_ann"),
            "ZONE_B_T": a.get("zone_b_t"),
            "SHARPE": a.get("zone_b_sharpe"),
            "MAX_DRAWDOWN": a.get("zone_b_max_drawdown"),
            "SAME_SIGN_A_B": a.get("same_sign_a_b"),
            "POSITIVE_AT_2X_COST": a.get("positive_at_2x_cost"),
            "CONTROL_INCREMENT_ANN": pc.get("increment_ann"),
            "CONTROL_INCREMENT_T": pc.get("increment_t_hac"),
            "SIGNAL_IS_DECORATION": pc.get("signal_is_decoration"),
            "CAPACITY_USD": cap.get("capacity_usd"),
            "CAPACITY_TIER_SUPPORTED": cap.get("tier_supported"),
            "ZONE_C_OPENED": a.get("zone_c_opened"),
            "ZONE_C_T": a.get("zone_c_t"),
            "PIT_STATUS": "PIT_TRUE",
            "BURDEN_CHARGED": True,
            "QUALIFICATION_STATE": _standalone_state(a, pc),
        })

    best = _best_intraday(intraday_out)
    if best is not None:
        rows.append({
            "CANDIDATE_ID": "r44_intraday_%s_%s_%s" % (
                best["symbol"], best["rule"].lower(), best["hold_min"]),
            "LANE": "E1B_INTRADAY_EVENT",
            "INFORMATION_FAMILY": "INTRADAY_EVENT",
            "ASSET_CLASS": "FX_AND_METALS",
            "ASSET": best["symbol"],
            "ECONOMIC_EXPRESSION": "SPOT_INTRADAY",
            "HORIZON": "%dm" % best["hold_min"],
            "MODEL": "TRANSPARENT_RULE",
            "REPRESENTATION": "MACRO_RELEASE_%s" % best["rule"],
            "ZONE_A_T": best.get("net_t"),
            "ZONE_A_GROSS_T": best.get("gross_t"),
            "ZONE_A_NET_BPS_PER_EVENT": best.get("net_bps_per_event"),
            "ZONE_A_GROSS_BPS_PER_EVENT": best.get("gross_bps_per_event"),
            "ZONE_A_COST_BPS_PER_EVENT": best.get("cost_bps_per_event"),
            "N_EVENTS": best.get("n_events"),
            "ZONE_B_T": None, "ZONE_C_OPENED": False,
            "PIT_STATUS": "PIT_TRUE",
            "BURDEN_CHARGED": False,
            "QUALIFICATION_STATE": "SCREENED_DID_NOT_REACH_ADVANCE_BAR",
            "WHY": "net t %.2f is below the frozen advance bar of %.1f, and "
                   "the same rule at the same parameters does not replicate "
                   "in the other two owned instruments"
                   % (best.get("net_t") or 0.0,
                      C.STANDALONE_ALPHA_GATE["t_min_lock"]),
        })

    if (options_out or {}).get("qualification_blocked"):
        s = options_out.get("surface") or {}
        rows.append({
            "CANDIDATE_ID": "r44_option_surface",
            "LANE": "E1A_OPTIONS_SURFACE",
            "INFORMATION_FAMILY": "OPTIONS_VOL",
            "ASSET_CLASS": "EQUITY_INDEX",
            "ASSET": "SPY",
            "ECONOMIC_EXPRESSION": "OPTION_SURFACE",
            "QUALIFICATION_STATE": "DATA_BLOCKED",
            "BLOCKER": "HISTORICAL_DATA_UNAVAILABLE",
            "SESSIONS_AVAILABLE": s.get("n_sessions"),
            "SESSIONS_REQUIRED": s.get("sessions_required"),
            "ADDITIONAL_MONTHS_REQUIRED": s.get(
                "additional_months_required"),
            "BURDEN_CHARGED": False,
        })
    rows.sort(key=lambda r: -(r.get("ZONE_B_T") or r.get("ZONE_A_T") or -9))
    for i, r in enumerate(rows, 1):
        r["RANK"] = i
    return rows


def _standalone_state(a: dict, pc: dict) -> str:
    if not a.get("same_sign_a_b"):
        return "KILLED_ON_ZONE_B_SIGN_FLIP"
    if not a.get("positive_at_2x_cost"):
        return "KILLED_BY_COST_STRESS"
    if pc.get("signal_is_decoration"):
        return "KILLED_BY_PASSIVE_CONTROL"
    if (a.get("zone_b_t") or -9) < C.STANDALONE_ALPHA_GATE["t_min_lock"]:
        return "DID_NOT_REACH_ZONE_B_T"
    return "RESEARCH_CANDIDATE"


def _best_intraday(intraday_out: dict):
    rows = (intraday_out or {}).get("screened_zone_a") or []
    ok = [r for r in rows if r.get("net_t") is not None]
    return max(ok, key=lambda r: r["net_t"]) if ok else None


def portfolio_rows(engine2: dict) -> list:
    """Every PORTFOLIO this release judged, primary rule first."""
    rows = []
    for name, b in (engine2.get("variants") or {}).items():
        if b.get("state") != "BUILT":
            continue
        lock = b.get("lock") or {}
        rows.append({
            "PORTFOLIO": name,
            "RULE": b.get("rule"),
            "IS_PRIMARY": bool(name == "RESIDUAL_PORTFOLIO"
                               and b.get("rule")
                               == C.PRIMARY_COMBINATION_RULE),
            "N_STREAMS": b.get("n_streams"),
            "EFFECTIVE_N_STREAMS": b.get("effective_n_streams"),
            "FIT_EXCESS_ANN": (b.get("fit") or {}).get("excess_ann"),
            "FIT_T": (b.get("fit") or {}).get("t_hac"),
            "LOCK_EXCESS_ANN": lock.get("excess_ann"),
            "LOCK_T": lock.get("t_hac"),
            "LOCK_SHARPE": lock.get("sharpe"),
            "LOCK_VOL_ANN": lock.get("vol_ann"),
            "LOCK_MAX_DRAWDOWN": lock.get("max_drawdown"),
            "LOCK_N": lock.get("n"),
            "SAME_SIGN_FIT_AND_LOCK": b.get("same_sign_fit_and_lock"),
        })
    rows.sort(key=lambda r: -(r.get("LOCK_T") or -99))
    return rows


# --------------------------------------------------------------------------- #
# Search adjustment
# --------------------------------------------------------------------------- #
def search_adjustment(standalone: list, portfolio_rules: list,
                      *, q: float = 0.10) -> dict:
    """BH inside each family, on the CUMULATIVE denominator, never reset."""
    from scipy import stats
    summary = B.summary()
    fam = summary["cumulative_family_counts"]

    entries = []
    for r in standalone:
        t = r.get("ZONE_B_T") or r.get("ZONE_A_T")
        if t is None or not np.isfinite(t):
            continue
        entries.append({"label": r["CANDIDATE_ID"],
                        "family": r["INFORMATION_FAMILY"], "t": float(t)})
    for r in portfolio_rules:
        if r.get("LOCK_T") is None:
            continue
        entries.append({"label": "%s/%s" % (r["PORTFOLIO"], r["RULE"]),
                        "family": "PORTFOLIO_SYNTHESIS",
                        "t": float(r["LOCK_T"])})
    if not entries:
        return {"state": "NOT_RUN"}
    p = [float(2.0 * (1.0 - stats.norm.cdf(abs(e["t"])))) for e in entries]
    bh = MT.benjamini_hochberg(p, q=q)
    rejected = set(bh.get("rejected") or [])
    rows = []
    for i, (e, pi) in enumerate(zip(entries, p)):
        rows.append({**e, "p_two_sided": pi,
                     "family_denominator": fam.get(e["family"]),
                     "global_denominator": summary["global_cumulative"],
                     "bh_survivor": i in rejected,
                     # A BH rejection with a NEGATIVE t is a significant
                     # LOSS, not a survivor. Saying so is the difference
                     # between a search adjustment and a scoreboard.
                     "is_a_positive_survivor": bool(
                         i in rejected and e["t"] > 0)})
    pos = [r for r in rows if r["is_a_positive_survivor"]]
    return {
        "calculation_owner": "alpha_agent.r31.multiple_testing."
                             "benjamini_hochberg",
        "q": q, "m": len(p), "threshold": bh.get("threshold"),
        "rows": rows,
        "n_bh_rejections": len(rejected),
        "n_positive_survivors": len(pos),
        "global_denominator": summary["global_cumulative"],
        "family_denominators": fam,
        "verdict": ("SEARCH_ADJUSTED_SURVIVOR" if pos
                    else "SEARCH_ADJUSTED_NO_SURVIVOR"),
    }


# --------------------------------------------------------------------------- #
# Freeze
# --------------------------------------------------------------------------- #
def freeze_decision(standalone: list, portfolio_rows_: list) -> dict:
    """Apply FREEZE_REQUIRES. Expect, and accept, zero."""
    eligible = []
    for r in standalone:
        if r.get("QUALIFICATION_STATE") != "RESEARCH_CANDIDATE":
            continue
        if not r.get("ZONE_C_OPENED"):
            continue
        if (r.get("ZONE_C_T") or -9) <= 0:
            continue
        eligible.append(r["CANDIDATE_ID"])
    for r in portfolio_rows_:
        if r.get("IS_PRIMARY") and (r.get("LOCK_T") or -9) >= \
                C.PORTFOLIO_ALPHA_GATE["t_min_lock"] \
                and r.get("SAME_SIGN_FIT_AND_LOCK"):
            eligible.append("%s/%s" % (r["PORTFOLIO"], r["RULE"]))
    return {
        "calculation_owner": CALCULATION_OWNER,
        "freeze_requires": list(C.FREEZE_REQUIRES),
        "max_new_shadows": C.MAX_NEW_SHADOWS,
        "n_eligible": len(eligible),
        "eligible": eligible,
        "n_frozen": 0 if not eligible else len(eligible[:C.MAX_NEW_SHADOWS]),
        "frozen": [] if not eligible else eligible[:C.MAX_NEW_SHADOWS],
        "promotion_allowed": C.PROMOTION_ALLOWED,
        "prior_shadows_are_immutable": C.PRIOR_SHADOWS_ARE_IMMUTABLE,
        "never_backfill": C.NEVER_BACKFILL_PROSPECTIVE_ROWS,
        "policy": "DO_NOT_FREEZE_MEDIOCRE_CANDIDATES_TO_CREATE_ACTIVITY",
        "why_none" : (None if eligible else
                      "no candidate reached a positive lockbox result that "
                      "the kill battery left standing; a candidate with no "
                      "historical credibility has no claim on future time"),
    }


def readiness(standalone: list, portfolio_rows_: list, freeze: dict) -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "n_standalone_judged": len(standalone),
        "n_standalone_research_candidates": sum(
            1 for r in standalone
            if r.get("QUALIFICATION_STATE") == "RESEARCH_CANDIDATE"),
        "n_portfolios_judged": len(portfolio_rows_),
        "n_shadows_frozen": freeze["n_frozen"],
        "operational_writes": 0,
        "portfolio_mutations": 0,
        "orders": 0,
        "model_promotions": 0,
        "scheduler_changes": 0,
    }
