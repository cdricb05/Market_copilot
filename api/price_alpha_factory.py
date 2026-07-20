"""
api/price_alpha_factory.py — Phase 21 Autonomous Price-Alpha Expansion (engine).

Activates the five trailing-price alpha families that the Phase 20 Alpha Factory could only list as
data-gated (MOMENTUM / TREND / VOLATILITY / RELATIVE_STRENGTH / MEAN_REVERSION) by wiring in the
owned point-in-time trailing-price panel (``api.price_panel``). It generates a disciplined,
interpretable grid of price-derived candidate signals, evaluates each at multiple forward horizons
with the SAME stdlib validation battery used by the Phase 17-A revalidation and the Phase 20
factory (so results are directly comparable to the fundamental champion), applies an explicit
overfit-defense rejection battery, computes cross-signal correlations, tests a small set of
transparent champion + price-family combinations for subperiod-stable diversification, ranks the
survivors, and can persist a complete Phase 21 artifact package to a dedicated LOCAL store.

Alignment (why the comparison is honest): price features live on EXACTLY the fundamental
representative rebalance grid — for each (calendar-month, ticker) representative row of the frozen
10-L panel that also has price history, the price factor is computed as of that row's rebalance
date and its forward returns run strictly after it. So a price factor and ``composite_sn`` share
the same month+ticker cross-sections, and their rank correlation is a real diversification measure.

Safety (identical posture to Phase 20): owned-data-only, pure stdlib, deterministic, no network / no
prediction service / no database. It writes NOTHING except, on an explicit confirmed build, the
dedicated local Phase 21 store. It never creates signals / trade decisions / orders / fills, never
replaces or mutates the champion, and never approves live trading. The current champion
(``composite_sn``) is unchanged regardless of any result; a winning combination is only ever labelled
RESEARCH or CHALLENGER_ELIGIBLE_FOR_FUTURE_PAPER_TEST.

Public API:
    build_price_alpha_factory(...) -> dict         # pure compute (no writes)
    run_price_alpha_factory(commit, confirm, ...)  # preview (no write) / confirmed build (writes store)
    load_price_alpha_factory(...)                  # read-only aggregate for the dashboard GET
    load_price_alpha_registry/leaderboard/correlation/combinations(...)
"""
from __future__ import annotations

import csv
import json
import math
import os
import tempfile
from pathlib import Path
from typing import Any, Optional, Union

from paper_trader.api import alpha_registry as reg
from paper_trader.api.alpha_registry import (
    FAM_MOMENTUM, FAM_TREND, FAM_VOLATILITY, FAM_RELATIVE_STRENGTH, FAM_MEAN_REVERSION,
    STATUS_ACTIVE, STATUS_CHALLENGER, STATUS_CHAMPION, STATUS_REJECTED, STATUS_RESEARCH,
    REJECT_LOW_COVERAGE, REJECT_NEGATIVE_IC, REJECT_STATISTICALLY_WEAK, REJECT_COST_KILLED,
    REJECT_UNSTABLE, REJECT_REDUNDANT, REJECT_INSUFFICIENT_PERIODS, REJECT_EXCESSIVE_TURNOVER,
    REJECT_SEVERE_DRAWDOWN, REJECT_PARAM_UNSTABLE, REJECT_CONCENTRATED,
)
from paper_trader.api import price_panel as pp
from paper_trader.api import alpha_factory as af
from paper_trader.api.alpha_factory import (
    evaluate_battery, load_panel as load_fundamental_panel,
    _to_float, _round, _mean, _std, _spearman, _pair_rank_corr, _iso_now, _read_json,
    _challenger_from_report, _regime_notes,
    C_REB, C_TICKER, C_COMPOSITE_SN, C_FWD, C_HAS_FWD,
    CHAMPION_SIGNAL, CHALLENGER_SIGNAL,
    PANEL_ENV, REVAL_REPORT_ENV, DEFAULT_PANEL, DEFAULT_REVAL_REPORT,
    MIN_NAMES_PER_MONTH,
)

PHASE = "21"
UNIVERSE_NAME = "S&P 500 multifactor price overlay (owned trailing-price panel & frozen 10-L universe)"

# --- forward horizons + per-family primary horizon -----------------------------------------
HORIZONS = list(pp.HORIZONS)  # [5, 10, 21, 63]
# A-priori (convention-based, NOT data-snooped) primary horizon per family: slow anomalies
# (intermediate momentum, relative strength, low-volatility) are judged at the quarterly (63d)
# horizon; trend-following at the monthly (21d) horizon; short-term reversal at the weekly (5d)
# horizon. The status decision uses only the primary horizon; every horizon is still reported.
PRIMARY_HORIZON = {
    FAM_MOMENTUM: 63, FAM_TREND: 21, FAM_VOLATILITY: 63,
    FAM_RELATIVE_STRENGTH: 63, FAM_MEAN_REVERSION: 5,
}
REBALANCE_CADENCE = "monthly"
TXN_COST_NOTE = "25bps / 50bps round-trip per monthly rebalance (net25 / net50); identical across horizons"

# --- automatic gate thresholds (transparent + testable) ------------------------------------
MIN_COVERAGE_PCT = 60.0
MIN_IC_MONTHS = 24               # need enough independent scored rebalance months
MIN_IC_T = 1.5
MIN_POSITIVE_IC_MONTH = 0.45     # price factors are noisier than the fundamental champion
MAX_CORR_REDUNDANT = 0.98
SEVERE_DD_RATIO = 1.0            # |maxdd| > this * cumulative_spread (with cumulative>0) -> fragile
TURNOVER_CAP = 0.98             # extreme churn ...
THIN_NET_FLOOR = 0.0015         # ... paired with a net25 below this -> excessive turnover
ACTIVE_MIN_IC_T = 2.0
ACTIVE_MIN_COVERAGE = 75.0
ACTIVE_MAX_CORR_VS_CHAMPION = 0.90

# --- combination weights (coarse, fixed; NO continuous optimisation) -----------------------
COMBO_WEIGHTS = [(0.7, 0.3), (0.5, 0.5), (0.3, 0.7)]

BUILD_CONFIRM_TOKEN = "RUN_PRICE_ALPHA_FACTORY_BUILD"

# --- status enums for the run/preview payload ----------------------------------------------
STATUS_BUILD_PREVIEW = "PRICE_ALPHA_FACTORY_BUILD_PREVIEW"
STATUS_BUILD_COMPLETE = "PRICE_ALPHA_FACTORY_BUILD_COMPLETE"
STATUS_CONFIRM_REQUIRED = "PRICE_ALPHA_FACTORY_CONFIRM_REQUIRED"
STATUS_PANEL_UNAVAILABLE = "PRICE_ALPHA_FACTORY_PANEL_UNAVAILABLE"
STATUS_READY = "PRICE_ALPHA_FACTORY_READY"

ELIG_CHALLENGER = "CHALLENGER_ELIGIBLE_FOR_FUTURE_PAPER_TEST"
ELIG_RESEARCH = "RESEARCH_ONLY"

# --- env seams -----------------------------------------------------------------------------
STORE_ENV = "PAPER_TRADER_PRICE_ALPHA_FACTORY_DIR"
DEFAULT_STORE = Path(r"D:\Stock_Prediction_app_data\phase21_price_alpha_factory")

# --- artifact file names (Phase 21 package) ------------------------------------------------
_MANIFEST_FILE = "trailing_price_panel_manifest.json"
_REGISTRY_FILE = "price_alpha_registry.json"
_LEADERBOARD_JSON = "price_alpha_leaderboard.json"
_LEADERBOARD_CSV = "price_alpha_leaderboard.csv"
_CORRELATION_JSON = "price_alpha_correlation.json"
_CORRELATION_CSV = "price_alpha_correlation.csv"
_HORIZON_CSV = "price_alpha_horizon_summary.csv"
_REJECTION_CSV = "price_alpha_rejection_report.csv"
_FAMILY_JSON = "price_alpha_family_summary.json"
_COMBINATION_JSON = "price_alpha_combination_report.json"
_DIAGNOSTICS_JSON = "price_alpha_diagnostics.json"
_FINAL_REPORT_JSON = "phase21_final_report.json"
_RUN_STATE_FILE = "price_alpha_factory_run_state.json"

_ALL_ARTIFACTS = [
    _MANIFEST_FILE, _REGISTRY_FILE, _LEADERBOARD_JSON, _LEADERBOARD_CSV, _CORRELATION_JSON,
    _CORRELATION_CSV, _HORIZON_CSV, _REJECTION_CSV, _FAMILY_JSON, _COMBINATION_JSON,
    _DIAGNOSTICS_JSON, _FINAL_REPORT_JSON, _RUN_STATE_FILE,
]


# --------------------------------------------------------------------------- #
# Candidate grid (disciplined + interpretable; NOT a hyper-parameter sweep)
# --------------------------------------------------------------------------- #
# Each spec: name, family, feature (key from the observation feature bundle), sign (+1 higher=long,
# -1 to invert), ladder (parameter-neighbour group + lookback rank; None = not on a ladder),
# feature_definition (human string).
def _candidate_specs() -> list[dict]:
    S = lambda name, fam, feat, sign, ladder, ldk, defn: {
        "name": name, "family": fam, "feature": feat, "sign": sign,
        "ladder": ladder, "ladder_k": ldk, "feature_definition": defn}
    return [
        # MOMENTUM
        S("mom_21", FAM_MOMENTUM, "ret_21", 1, "mom", 21, "Trailing 21-day total return."),
        S("mom_63", FAM_MOMENTUM, "ret_63", 1, "mom", 63, "Trailing 63-day total return."),
        S("mom_126", FAM_MOMENTUM, "ret_126", 1, "mom", 126, "Trailing 126-day total return."),
        S("mom_252", FAM_MOMENTUM, "ret_252", 1, "mom", 252, "Trailing 252-day total return."),
        S("mom_12_1", FAM_MOMENTUM, "mom_12_1", 1, None, None,
          "12-1 momentum: cumulative return t-252..t-21 (skips the most recent month)."),
        S("mom_accel", FAM_MOMENTUM, "mom_accel", 1, None, None,
          "Momentum acceleration: recent 63-day return minus the prior 63-day return."),
        S("mom_blend", FAM_MOMENTUM, "mom_blend", 1, None, None,
          "Multi-horizon momentum blend: average of 63/126/252-day returns."),
        # TREND
        S("trend_px_ma20", FAM_TREND, "px_vs_ma20", 1, "trend", 20, "Price vs 20-day moving average."),
        S("trend_px_ma63", FAM_TREND, "px_vs_ma63", 1, "trend", 63, "Price vs 63-day moving average."),
        S("trend_px_ma126", FAM_TREND, "px_vs_ma126", 1, "trend", 126, "Price vs 126-day moving average."),
        S("trend_ma20_ma63", FAM_TREND, "ma20_vs_ma63", 1, None, None, "20-day vs 63-day MA spread."),
        S("trend_persist", FAM_TREND, "trend_persist_63", 1, None, None,
          "Trend persistence: fraction of up-days over 63 days (0.5-centered)."),
        S("trend_quality", FAM_TREND, "trend_quality_126", 1, None, None,
          "Volatility-adjusted trend: 126-day return / 126-day realized volatility."),
        # VOLATILITY
        S("lowvol_63", FAM_VOLATILITY, "rvol_63", -1, "lowvol", 63, "Low realized volatility (63-day, inverted)."),
        S("lowvol_126", FAM_VOLATILITY, "rvol_126", -1, "lowvol", 126, "Low realized volatility (126-day, inverted)."),
        S("low_downside_126", FAM_VOLATILITY, "dvol_126", -1, None, None,
          "Low downside deviation (126-day semi-deviation, inverted)."),
        S("voladj_mom", FAM_VOLATILITY, "voladj_mom_63", 1, None, None,
          "Volatility-adjusted momentum: 63-day return / 63-day realized volatility."),
        S("ddadj_trend", FAM_VOLATILITY, "ddadj_trend_126", 1, None, None,
          "Drawdown-adjusted trend: 126-day return / |252-day max drawdown|."),
        # RELATIVE STRENGTH
        S("rs_63", FAM_RELATIVE_STRENGTH, "rs_63", 1, "rs", 63, "63-day return in excess of SPY."),
        S("rs_126", FAM_RELATIVE_STRENGTH, "rs_126", 1, "rs", 126, "126-day return in excess of SPY."),
        S("rs_252", FAM_RELATIVE_STRENGTH, "rs_252", 1, "rs", 252, "252-day return in excess of SPY."),
        S("rs_blend", FAM_RELATIVE_STRENGTH, "rs_blend", 1, None, None,
          "Multi-horizon relative strength: average excess-vs-SPY over 63/126/252 days."),
        S("rs_vs_universe_126", FAM_RELATIVE_STRENGTH, "rs_vs_universe_126", 1, None, None,
          "126-day return in excess of the equal-weight universe mean (cross-sectional)."),
        # MEAN REVERSION
        S("rev_5", FAM_MEAN_REVERSION, "ret_5", -1, "rev", 5, "Short-horizon reversal: negative trailing 5-day return."),
        S("rev_10", FAM_MEAN_REVERSION, "ret_10", -1, "rev", 10, "Short-horizon reversal: negative trailing 10-day return."),
        S("rev_21", FAM_MEAN_REVERSION, "ret_21", -1, "rev", 21, "Short-horizon reversal: negative trailing 21-day return."),
        S("rev_dist_ma10", FAM_MEAN_REVERSION, "px_vs_ma10", -1, None, None,
          "Distance below the 10-day MA (negative price-vs-MA10)."),
        S("resid_rev_21", FAM_MEAN_REVERSION, "resid_rev_21", 1, None, None,
          "Market-residual 21-day reversal (negative beta-adjusted excess return)."),
        S("gap_rev", FAM_MEAN_REVERSION, "ret_1", -1, None, None,
          "One-day gap reversal: negative most-recent daily return."),
    ]


# --------------------------------------------------------------------------- #
# Observation grid: price features anchored on the fundamental rebalance grid
# --------------------------------------------------------------------------- #
def _build_observations(fpanel: dict, price: dict) -> tuple[list[dict], dict]:
    """DENSE monthly observation grid: one observation per (calendar-month, ticker) at each month-end
    for every ticker in the price ∩ fundamental universe — so price factors are scored on their full
    natural cross-section (~all overlapping names each month), not just the sparse subset that happens
    to carry a fresh fundamental rebalance that month.

    Each observation carries the full price-feature bundle computed as of the last trading day of the
    month (PIT: only bars with date <= month-end), the forward returns at every horizon (strictly
    future), and — attached only when a fundamental representative row exists for that (month, ticker)
    — the contemporaneous ``composite_sn`` and the fundamental 63-day forward return (used for the
    champion-alignment correlation, the combinations and the no-look-ahead cross-check).
    """
    rows = fpanel["rows"]
    rep_index = fpanel["rep_index"]
    series = price["series"]
    fund_months = sorted(rep_index.keys())
    fund_names: set = set()
    for _m, tk_idx in rep_index.items():
        fund_names.update(tk_idx.keys())
    obs: list[dict] = []
    matched_names: set = set()
    for tk in sorted(fund_names & set(series.keys())):
        s = series[tk]
        dates = s["dates"]
        matched_names.add(tk)
        for month in fund_months:
            j = pp.asof_index(dates, month + "-31")
            if j < 1 or dates[j][:7] != month:
                continue  # ticker did not trade in this calendar month (no stale prior-month bar)
            feats = pp.compute_features(s, j)
            fwd = pp.forward_returns(s, j)
            ridx = rep_index.get(month, {}).get(tk)
            comp = None
            fund_fwd = None
            if ridx is not None:
                r = rows[ridx]
                comp = _to_float(r.get(C_COMPOSITE_SN))
                if str(r.get(C_HAS_FWD)).strip().lower() in ("true", "1", "yes"):
                    fund_fwd = _to_float(r.get(C_FWD))
            obs.append({"month": month, "ticker": tk, "asof": dates[j], "j": j, "feat": feats,
                        "fwd": fwd, "composite_sn": comp, "fund_fwd63": fund_fwd})
    # cross-sectional derived feature: 126-day return in excess of the equal-weight universe mean
    by_month: dict[str, list[dict]] = {}
    for o in obs:
        by_month.setdefault(o["month"], []).append(o)
    for m, bucket in by_month.items():
        vals = [o["feat"].get("ret_126") for o in bucket if o["feat"].get("ret_126") is not None]
        mu = (sum(vals) / len(vals)) if vals else None
        for o in bucket:
            r126 = o["feat"].get("ret_126")
            o["feat"]["rs_vs_universe_126"] = (r126 - mu) if (r126 is not None and mu is not None) else None
    meta = {
        "n_fundamental_names": len(fund_names),
        "n_price_matched_names": len(matched_names),
        "n_observations": len(obs),
        "n_months": len(by_month),
        "universe_overlap_pct": _round(100.0 * len(matched_names) / len(fund_names), 2) if fund_names else 0.0,
        "grid": "dense monthly (month-end) over the price-and-fundamental overlap universe",
    }
    return obs, meta


# --------------------------------------------------------------------------- #
# Per-candidate evaluation across horizons
# --------------------------------------------------------------------------- #
def _signal_maps(obs: list[dict], feature: str, sign: int) -> dict[str, dict[str, float]]:
    """{month: {ticker: sign*feature}} over observations where the feature is present."""
    out: dict[str, dict[str, float]] = {}
    for o in obs:
        v = o["feat"].get(feature)
        if v is not None:
            out.setdefault(o["month"], {})[o["ticker"]] = sign * v
    return out


def _monthly_for_horizon(obs: list[dict], feature: str, sign: int, h: int) -> tuple[dict, dict]:
    """Build the battery input {month:[{ticker,score,fwd}]} at horizon h and the coverage stats.

    Coverage denominator = observations with a valid forward-h return (the price-panel scoreable set
    at that horizon); covered = those that also carry the feature."""
    monthly: dict[str, list[dict]] = {}
    scoreable = 0
    covered = 0
    for o in obs:
        fwd = o["fwd"].get(h)
        if fwd is None:
            continue
        scoreable += 1
        v = o["feat"].get(feature)
        if v is None:
            continue
        covered += 1
        monthly.setdefault(o["month"], []).append({"ticker": o["ticker"], "score": sign * v, "fwd": fwd})
    cov_pct = (100.0 * covered / scoreable) if scoreable else 0.0
    return monthly, {"scoreable_name_months": scoreable, "covered_name_months": covered,
                     "coverage_pct": _round(cov_pct, 2), "missing_name_months": scoreable - covered}


def _evaluate_candidate(obs: list[dict], spec: dict) -> dict:
    feature, sign = spec["feature"], spec["sign"]
    per_h: dict[int, dict] = {}
    for h in HORIZONS:
        monthly, cov = _monthly_for_horizon(obs, feature, sign, h)
        metrics = evaluate_battery(monthly)
        metrics.update(cov)
        per_h[h] = metrics
    primary_h = PRIMARY_HORIZON[spec["family"]]
    return {"spec": spec, "per_horizon": per_h, "primary_horizon": primary_h,
            "primary": per_h[primary_h], "maps": _signal_maps(obs, feature, sign)}


# --------------------------------------------------------------------------- #
# Overfit-defense gates
# --------------------------------------------------------------------------- #
def _subperiod_spread(metrics: dict) -> tuple[Optional[float], Optional[float]]:
    sp = metrics.get("subperiod") or {}
    return ((sp.get("pre2020") or {}).get("mean_spread"), (sp.get("post2020") or {}).get("mean_spread"))


def _subperiod_ic(metrics: dict) -> tuple[Optional[float], Optional[float]]:
    sp = metrics.get("subperiod") or {}
    return ((sp.get("pre2020") or {}).get("mean_ic"), (sp.get("post2020") or {}).get("mean_ic"))


def _hard_gate_reason(metrics: dict, param_unstable: bool) -> Optional[str]:
    """First failing automatic gate at the candidate's primary horizon, or None if it survives."""
    cov = metrics.get("coverage_pct")
    if cov is None or cov < MIN_COVERAGE_PCT:
        return REJECT_LOW_COVERAGE
    if (metrics.get("n_ic_months") or 0) < MIN_IC_MONTHS:
        return REJECT_INSUFFICIENT_PERIODS
    ic = metrics.get("mean_ic")
    if ic is None or ic < 0:
        return REJECT_NEGATIVE_IC
    ic_t = metrics.get("ic_t_stat")
    if ic_t is None or ic_t < MIN_IC_T:
        return REJECT_STATISTICALLY_WEAK
    net25 = metrics.get("net25_spread")
    if net25 is None or net25 <= 0:
        return REJECT_COST_KILLED
    pre_ic, post_ic = _subperiod_ic(metrics)
    pos = metrics.get("positive_ic_month_rate")
    sign_rev = (pre_ic is not None and post_ic is not None and (pre_ic >= 0) != (post_ic >= 0))
    if sign_rev or (pos is not None and pos < MIN_POSITIVE_IC_MONTH):
        return REJECT_UNSTABLE
    pre_sp, post_sp = _subperiod_spread(metrics)
    if pre_sp is not None and post_sp is not None and min(pre_sp, post_sp) <= 0:
        return REJECT_CONCENTRATED
    if param_unstable:
        return REJECT_PARAM_UNSTABLE
    cum = metrics.get("cumulative_spread")
    mdd = metrics.get("max_drawdown")
    if cum is not None and cum > 0 and mdd is not None and abs(mdd) > SEVERE_DD_RATIO * cum:
        return REJECT_SEVERE_DRAWDOWN
    turn = metrics.get("mean_turnover")
    if turn is not None and turn > TURNOVER_CAP and (net25 is None or net25 < THIN_NET_FLOOR):
        return REJECT_EXCESSIVE_TURNOVER
    return None


def _param_neighbor_unstable(evals: list[dict]) -> set:
    """Flag a ladder member whose primary-horizon IC sign is opposite to BOTH lookback neighbours."""
    flagged: set = set()
    ladders: dict[str, list[dict]] = {}
    for ev in evals:
        lad = ev["spec"].get("ladder")
        if lad:
            ladders.setdefault(lad, []).append(ev)
    for lad, members in ladders.items():
        members = sorted(members, key=lambda e: e["spec"]["ladder_k"])
        if len(members) < 3:
            continue
        ics = [(_to_float(m["primary"].get("mean_ic"))) for m in members]
        for i in range(1, len(members) - 1):
            cur, lo, hi = ics[i], ics[i - 1], ics[i + 1]
            if cur is None or lo is None or hi is None:
                continue
            if (cur >= 0) != (lo >= 0) and (cur >= 0) != (hi >= 0):
                flagged.add(members[i]["spec"]["name"])
    return flagged


# --------------------------------------------------------------------------- #
# Combinations (champion + one price family; coarse fixed weights)
# --------------------------------------------------------------------------- #
def _zscore(vals: dict[str, float]) -> dict[str, float]:
    xs = list(vals.values())
    m = _mean(xs) if xs else None
    s = _std(xs, 1) if len(xs) > 1 else None
    if m is None or not s:
        return {k: 0.0 for k in vals}
    return {k: (v - m) / s for k, v in vals.items()}


def _champion_intersection_monthly(obs: list[dict], feature: str, sign: int) -> dict[str, list[dict]]:
    """Champion baseline on the intersection universe (names with composite_sn, the price feature and
    a fundamental 63-day forward). Score = composite_sn; forward = fundamental forward_63d_return."""
    monthly: dict[str, list[dict]] = {}
    for o in obs:
        if o["composite_sn"] is None or o["fund_fwd63"] is None or o["feat"].get(feature) is None:
            continue
        monthly.setdefault(o["month"], []).append(
            {"ticker": o["ticker"], "score": o["composite_sn"], "fwd": o["fund_fwd63"]})
    return monthly


def _combined_monthly(obs: list[dict], feature: str, sign: int, wf: float, wp: float) -> dict[str, list[dict]]:
    """z(composite_sn)*wf + z(sign*feature)*wp on the intersection universe; fwd = fundamental 63d."""
    monthly: dict[str, list[dict]] = {}
    for month in {o["month"] for o in obs}:
        bucket = [o for o in obs if o["month"] == month and o["composite_sn"] is not None
                  and o["fund_fwd63"] is not None and o["feat"].get(feature) is not None]
        if len(bucket) < MIN_NAMES_PER_MONTH:
            continue
        zf = _zscore({o["ticker"]: o["composite_sn"] for o in bucket})
        zp = _zscore({o["ticker"]: sign * o["feat"][feature] for o in bucket})
        for o in bucket:
            monthly.setdefault(month, []).append(
                {"ticker": o["ticker"], "score": wf * zf[o["ticker"]] + wp * zp[o["ticker"]],
                 "fwd": o["fund_fwd63"]})
    return monthly


def _combinations(obs: list[dict], best_by_family: dict[str, dict]) -> dict:
    """Test champion + each surviving price family's best leg (coarse weights) plus a multi-family
    equal-weight composite and a correlation-aware composite, all on the 63-day fundamental grid."""
    results: list[dict] = []
    any_legs = [b for b in best_by_family.values() if b]
    # pick a representative leg's champion baseline (intersection is per-feature; use full-champion
    # baseline on the union intersection = names with composite_sn + fund fwd, independent of feature)
    base_monthly: dict[str, list[dict]] = {}
    for o in obs:
        if o["composite_sn"] is not None and o["fund_fwd63"] is not None:
            base_monthly.setdefault(o["month"], []).append(
                {"ticker": o["ticker"], "score": o["composite_sn"], "fwd": o["fund_fwd63"]})
    champ_base = evaluate_battery(base_monthly)

    def _delta(combo: dict) -> dict:
        pre_c, post_c = _subperiod_spread(combo)
        pre_b, post_b = _subperiod_spread(champ_base)
        net_lift = (_to_float(combo.get("net25_spread")) or 0.0) - (_to_float(champ_base.get("net25_spread")) or 0.0)
        subperiod_stable = (pre_c is not None and post_c is not None and pre_b is not None
                            and post_b is not None and pre_c >= pre_b and post_c >= post_b)
        if net_lift > 0 and subperiod_stable:
            elig = ELIG_CHALLENGER
        else:
            elig = ELIG_RESEARCH
        return {"net25_lift_vs_champion": _round(net_lift, 6), "subperiod_stable": subperiod_stable,
                "eligibility": elig, "status": STATUS_RESEARCH}

    for fam, best in best_by_family.items():
        if not best:
            continue
        feature, sign, leg = best["spec"]["feature"], best["spec"]["sign"], best["spec"]["name"]
        for wf, wp in COMBO_WEIGHTS:
            combo = evaluate_battery(_combined_monthly(obs, feature, sign, wf, wp))
            row = {"name": "champ+%s_%d_%d" % (leg, int(wf * 10), int(wp * 10)),
                   "kind": "champion+%s" % fam.lower(), "price_leg": leg, "family": fam,
                   "weights": {"composite_sn": wf, leg: wp},
                   "net25_spread": _round(combo.get("net25_spread"), 6),
                   "mean_ic": _round(combo.get("mean_ic"), 6), "ic_t_stat": _round(combo.get("ic_t_stat"), 4),
                   "max_drawdown": _round(combo.get("max_drawdown"), 6),
                   "mean_turnover": _round(combo.get("mean_turnover"), 4)}
            row.update(_delta(combo))
            results.append(row)

    # multi-family equal-weight composite of all surviving best legs + champion
    if any_legs:
        combo = evaluate_battery(_multi_family_monthly(obs, any_legs))
        row = {"name": "champ+multifamily_ew", "kind": "champion+multi_family_ew",
               "price_leg": [b["spec"]["name"] for b in any_legs], "family": "MULTI",
               "weights": "equal-weight z of composite_sn + each surviving family best leg",
               "net25_spread": _round(combo.get("net25_spread"), 6),
               "mean_ic": _round(combo.get("mean_ic"), 6), "ic_t_stat": _round(combo.get("ic_t_stat"), 4),
               "max_drawdown": _round(combo.get("max_drawdown"), 6),
               "mean_turnover": _round(combo.get("mean_turnover"), 4)}
        row.update(_delta(combo))
        results.append(row)

    champ_summary = {"net25_spread": _round(champ_base.get("net25_spread"), 6),
                     "mean_ic": _round(champ_base.get("mean_ic"), 6),
                     "ic_t_stat": _round(champ_base.get("ic_t_stat"), 4),
                     "max_drawdown": _round(champ_base.get("max_drawdown"), 6),
                     "mean_turnover": _round(champ_base.get("mean_turnover"), 4),
                     "note": "champion composite_sn evaluated on the price-overlap intersection (63d)"}
    n_elig = sum(1 for r in results if r["eligibility"] == ELIG_CHALLENGER)
    return {"champion_baseline_on_intersection": champ_summary, "combinations": results,
            "n_combinations": len(results), "n_challenger_eligible": n_elig,
            "note": ("Coarse fixed weights only; no continuous weight optimisation. The champion is "
                     "never replaced - a winner is at most CHALLENGER_ELIGIBLE_FOR_FUTURE_PAPER_TEST.")}


def _multi_family_monthly(obs: list[dict], legs: list[dict]) -> dict[str, list[dict]]:
    feats = [(b["spec"]["feature"], b["spec"]["sign"]) for b in legs]
    monthly: dict[str, list[dict]] = {}
    for month in {o["month"] for o in obs}:
        bucket = [o for o in obs if o["month"] == month and o["composite_sn"] is not None
                  and o["fund_fwd63"] is not None and all(o["feat"].get(f) is not None for f, _s in feats)]
        if len(bucket) < MIN_NAMES_PER_MONTH:
            continue
        zf = _zscore({o["ticker"]: o["composite_sn"] for o in bucket})
        zlegs = [_zscore({o["ticker"]: s * o["feat"][f] for o in bucket}) for f, s in feats]
        w = 1.0 / (1 + len(zlegs))
        for o in bucket:
            tk = o["ticker"]
            score = w * zf[tk] + sum(w * zl[tk] for zl in zlegs)
            monthly.setdefault(month, []).append({"ticker": tk, "score": score, "fwd": o["fund_fwd63"]})
    return monthly


# --------------------------------------------------------------------------- #
# No-look-ahead cross-check (price 63d forward vs fundamental forward_63d_return)
# --------------------------------------------------------------------------- #
def _forward_cross_check(obs: list[dict]) -> dict:
    xs, ys = [], []
    for o in obs:
        pf, ff = o["fwd"].get(63), o["fund_fwd63"]
        if pf is not None and ff is not None:
            xs.append(pf)
            ys.append(ff)
    if len(xs) < 100:
        return {"available": False, "note": "insufficient overlapping forward returns"}
    return {"available": True, "n": len(xs),
            "spearman_price63_vs_fundamental63": _round(_spearman(xs, ys), 4),
            "note": ("Directional agreement between the price-derived 63-day forward (month-end anchor) "
                     "and the frozen 10-L forward_63d_return (rebalance-date anchor) over common "
                     "observations. A clearly positive rank correlation confirms the forwards move "
                     "together and are not look-ahead-inverted; it is below 1.0 by construction "
                     "(different vendors and a within-month anchor offset). The strict no-look-ahead "
                     "guarantee itself is structural - forwards read only bars strictly after the "
                     "observation (unit-tested) - not inferred from this correlation.")}


# --------------------------------------------------------------------------- #
# Correlation matrix (champion + survivors, primary-horizon feature maps)
# --------------------------------------------------------------------------- #
def _champion_maps(obs: list[dict]) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for o in obs:
        if o["composite_sn"] is not None:
            out.setdefault(o["month"], {})[o["ticker"]] = o["composite_sn"]
    return out


def _correlation_matrix(champ_maps, survivors: list[dict], eval_by_name: dict) -> dict:
    names = [CHAMPION_SIGNAL] + [s["spec"]["name"] for s in survivors]
    maps = {CHAMPION_SIGNAL: champ_maps}
    for s in survivors:
        maps[s["spec"]["name"]] = s["maps"]
    n = len(names)
    matrix = [[None] * n for _ in range(n)]
    off, top = [], None
    for a in range(n):
        for b in range(n):
            if a == b:
                matrix[a][b] = 1.0
            elif b < a:
                matrix[a][b] = matrix[b][a]
            else:
                c = _round(_pair_rank_corr(maps[names[a]], maps[names[b]]), 4)
                matrix[a][b] = c
                if c is not None:
                    off.append(c)
                    if top is None or abs(c) > abs(top[2]):
                        top = (names[a], names[b], c)
    vs_champ = {names[i]: matrix[0][i] for i in range(1, n)}
    # correlation clusters: greedy grouping of survivors at |corr| >= 0.8
    clusters = _corr_clusters([s["spec"]["name"] for s in survivors], maps)
    summary = {"n_signals": n,
               "max_abs_off_diagonal": _round(max((abs(x) for x in off), default=None), 4) if off else None,
               "mean_abs_off_diagonal": _round(_mean([abs(x) for x in off]), 4) if off else None,
               "most_correlated_pair": ({"a": top[0], "b": top[1], "corr": top[2]} if top else None),
               "mean_abs_corr_vs_champion": _round(_mean([abs(v) for v in vs_champ.values() if v is not None]), 4)
               if vs_champ else None,
               "min_abs_corr_vs_champion": _round(min((abs(v) for v in vs_champ.values() if v is not None),
                                                      default=None), 4) if vs_champ else None,
               "n_clusters": len(clusters)}
    return {"signals": names, "matrix": matrix, "vs_champion": vs_champ, "clusters": clusters,
            "summary": summary, "method": "mean monthly cross-sectional Spearman rank correlation"}


def _corr_clusters(names: list[str], maps: dict, thresh: float = 0.8) -> list[list[str]]:
    clusters: list[list[str]] = []
    for nm in names:
        placed = False
        for cl in clusters:
            c = _pair_rank_corr(maps[nm], maps[cl[0]])
            if c is not None and abs(c) >= thresh:
                cl.append(nm)
                placed = True
                break
        if not placed:
            clusters.append([nm])
    return clusters


# --------------------------------------------------------------------------- #
# Registry record construction
# --------------------------------------------------------------------------- #
def _price_record(ev: dict, status: str, signal_date: Optional[str], meta: dict,
                  reject_reason: Optional[str], corr_champ: Optional[float],
                  max_corr_accepted: Optional[float]) -> dict:
    spec = ev["spec"]
    primary = ev["primary"]
    primary_h = ev["primary_horizon"]
    per_h = {str(h): _horizon_slim(ev["per_horizon"][h]) for h in HORIZONS}
    pre_ic, post_ic = _subperiod_ic(primary)
    rec = reg.make_alpha_record(
        name=spec["name"], family=spec["family"], status=status,
        horizon=primary_h, universe=UNIVERSE_NAME, signal_date=signal_date,
        metrics=primary, corr_vs_champion=corr_champ, regime_notes=_regime_notes(primary),
        reject_reason=reject_reason, description=spec["feature_definition"],
        spec={"feature": spec["feature"], "sign": spec["sign"], "ladder": spec.get("ladder")},
        role="GENERATED PRICE CANDIDATE",
        extra={
            "feature_definition": spec["feature_definition"],
            "rebalance_cadence": REBALANCE_CADENCE,
            "transaction_cost_assumption": TXN_COST_NOTE,
            "primary_horizon_trading_days": primary_h,
            "horizons": per_h,
            "sample_start": meta.get("sample_start"), "sample_end": signal_date,
            "n_independent_periods": primary.get("n_ic_months"),
            "pre2020_mean_ic": _round(pre_ic, 6), "post2020_mean_ic": _round(post_ic, 6),
            "data_source": pp.SOURCE_NAME,
            "pit_caveats": ("survivorship (current members only); retro-adjusted close; ~"
                            + str(meta.get("universe_overlap_pct")) + "% fundamental-universe overlap"),
            "max_corr_vs_accepted": _round(max_corr_accepted, 4),
        })
    return rec


def _horizon_slim(m: dict) -> dict:
    return {"mean_ic": m.get("mean_ic"), "ic_t_stat": m.get("ic_t_stat"),
            "mean_gross_spread": m.get("mean_gross_spread"), "net25_spread": m.get("net25_spread"),
            "net50_spread": m.get("net50_spread"), "mean_turnover": m.get("mean_turnover"),
            "max_drawdown": m.get("max_drawdown"), "sharpe": m.get("sharpe"),
            "coverage_pct": m.get("coverage_pct"), "n_ic_months": m.get("n_ic_months"),
            "positive_ic_month_rate": m.get("positive_ic_month_rate")}


def _lb_row(rec: dict, full: bool = False) -> dict:
    row = {"name": rec["name"], "family": rec["family"], "status": rec["status"],
           "status_class": rec.get("status_class"), "horizon": rec.get("horizon"),
           "ic": rec.get("ic"), "ic_t": rec.get("ic_t"), "spread": rec.get("spread"),
           "net25": rec.get("net25"), "net50": rec.get("net50"), "turnover": rec.get("turnover"),
           "drawdown": rec.get("drawdown"), "sharpe": rec.get("sharpe"),
           "coverage_pct": rec.get("coverage_pct"), "corr_vs_champion": rec.get("corr_vs_champion")}
    if full:
        row["regime_notes"] = rec.get("regime_notes")
        row["description"] = rec.get("description")
        row["feature_definition"] = rec.get("feature_definition")
    return row


def _reject_row(rec: dict) -> dict:
    return {"name": rec["name"], "family": rec["family"], "horizon": rec.get("horizon"),
            "reject_reason": rec.get("reject_reason"), "reject_reason_text": rec.get("reject_reason_text"),
            "ic": rec.get("ic"), "ic_t": rec.get("ic_t"), "net25": rec.get("net25"),
            "coverage_pct": rec.get("coverage_pct"), "turnover": rec.get("turnover"),
            "drawdown": rec.get("drawdown"), "corr_vs_champion": rec.get("corr_vs_champion"),
            "max_corr_vs_accepted": rec.get("max_corr_vs_accepted")}


def _family_summary(cand_recs: list[dict], best_by_family: dict) -> list[dict]:
    fc = reg.family_counts(cand_recs)
    by_fam: dict[str, list[dict]] = {}
    for r in cand_recs:
        by_fam.setdefault(r["family"], []).append(r)
    out = []
    for fam in reg.PRICE_GATED_FAMILIES:
        c = fc.get(fam, {})
        best = best_by_family.get(fam)
        # family leader = top candidate by primary net25 REGARDLESS of survival (for display)
        members = by_fam.get(fam, [])
        leader = max(members, key=lambda r: (r.get("net25") if isinstance(r.get("net25"), (int, float))
                                             else -9.9), default=None)
        out.append({
            "family": fam, "description": reg.FAMILY_DESCRIPTIONS.get(fam),
            "data_ready": True, "status": "DATA_READY",
            "primary_horizon": PRIMARY_HORIZON[fam],
            "n_candidates": c.get("total", 0), "n_active": c.get(STATUS_ACTIVE, 0),
            "n_research": c.get(STATUS_RESEARCH, 0), "n_rejected": c.get(STATUS_REJECTED, 0),
            "best_survivor": (best["spec"]["name"] if best else None),
            "best_survivor_net25": (_round(best["primary"].get("net25_spread"), 6) if best else None),
            "best_survivor_ic_t": (_round(best["primary"].get("ic_t_stat"), 4) if best else None),
            "leader": (leader["name"] if leader else None),
            "leader_status": (leader["status"] if leader else None),
            "leader_net25": (leader.get("net25") if leader else None),
            "leader_ic_t": (leader.get("ic_t") if leader else None),
        })
    return out


# --------------------------------------------------------------------------- #
# The build (pure compute, no writes)
# --------------------------------------------------------------------------- #
_CACHE: dict = {}


def clear_cache() -> None:
    _CACHE.clear()


def _resolve_store(store_dir=None) -> Path:
    if store_dir is not None:
        return Path(store_dir)
    env = os.environ.get(STORE_ENV)
    return Path(env) if env else DEFAULT_STORE


def _cache_key(fpath: Path, ppath: Path, rpath: Path) -> Optional[tuple]:
    def mt(p):
        try:
            return p.stat().st_mtime
        except OSError:
            return None
    fm, pm = mt(fpath), mt(ppath)
    if fm is None or pm is None:
        return None
    return (str(fpath), fm, str(ppath), pm, str(rpath), mt(rpath) or 0.0)


def build_price_alpha_factory(*, panel_path=None, price_path=None, reval_report_path=None,
                              use_cache: bool = True) -> dict:
    """Pure, deterministic Phase 21 price-alpha build. Never writes, never raises."""
    fpath = Path(panel_path) if panel_path else (Path(os.environ[PANEL_ENV]) if os.environ.get(PANEL_ENV) else DEFAULT_PANEL)
    ppath = pp._resolve(price_path)
    rpath = Path(reval_report_path) if reval_report_path else (Path(os.environ[REVAL_REPORT_ENV]) if os.environ.get(REVAL_REPORT_ENV) else DEFAULT_REVAL_REPORT)
    key = _cache_key(fpath, ppath, rpath) if use_cache else None
    if key is not None and key in _CACHE:
        return _CACHE[key]

    fpanel = load_fundamental_panel(fpath)
    price = pp.load_price_panel(ppath)
    if fpanel is None or price is None:
        which = "fundamental 10-L panel" if fpanel is None else "owned trailing-price panel"
        result = {"status": STATUS_PANEL_UNAVAILABLE, "missing": which,
                  "fundamental_panel_path": str(fpath), "price_panel_path": str(ppath),
                  "warnings": ["%s not found or empty" % which]}
        return result

    signal_date = fpanel.get("signal_date")
    obs, meta = _build_observations(fpanel, price)
    meta["sample_start"] = price["manifest"].get("date_start")
    warnings: list[str] = []
    if meta["n_observations"] == 0:
        warnings.append("No overlapping (month, ticker) observations between the price and 10-L panels.")

    # --- evaluate every candidate across horizons -----------------------------------------
    specs = _candidate_specs()
    evals = [_evaluate_candidate(obs, s) for s in specs]
    param_unstable = _param_neighbor_unstable(evals)

    # --- champion reference (composite_sn) on the price-overlap grid at 63d ----------------
    champ_maps = _champion_maps(obs)
    champ_base_monthly: dict[str, list[dict]] = {}
    for o in obs:
        if o["composite_sn"] is not None and o["fund_fwd63"] is not None:
            champ_base_monthly.setdefault(o["month"], []).append(
                {"ticker": o["ticker"], "score": o["composite_sn"], "fwd": o["fund_fwd63"]})
    champ_metrics = evaluate_battery(champ_base_monthly)

    # --- correlation vs champion + hard gates ----------------------------------------------
    for ev in evals:
        ev["corr_vs_champion"] = _pair_rank_corr(ev["maps"], champ_maps)
        ev["reject_reason"] = _hard_gate_reason(ev["primary"], ev["spec"]["name"] in param_unstable)
        ev["status"] = None if ev["reject_reason"] is None else STATUS_REJECTED

    # --- redundancy gate (greedy, ranked; champion is the reference) -----------------------
    provisional = [ev for ev in evals if ev["status"] is None]
    provisional.sort(key=lambda e: reg.leaderboard_sort_key({
        "net25": e["primary"].get("net25_spread"), "ic_t": e["primary"].get("ic_t_stat"),
        "ic": e["primary"].get("mean_ic")}), reverse=True)
    accepted_ref = [(CHAMPION_SIGNAL, champ_maps)]
    for ev in provisional:
        max_corr = abs(ev["corr_vs_champion"]) if ev["corr_vs_champion"] is not None else 0.0
        for _n, amaps in accepted_ref:
            c = _pair_rank_corr(ev["maps"], amaps)
            if c is not None:
                max_corr = max(max_corr, abs(c))
        if max_corr > MAX_CORR_REDUNDANT:
            ev["status"] = STATUS_REJECTED
            ev["reject_reason"] = REJECT_REDUNDANT
            ev["max_corr_vs_accepted"] = _round(max_corr, 4)
        else:
            ic_t = ev["primary"].get("ic_t_stat") or 0.0
            cov = ev["primary"].get("coverage_pct") or 0.0
            corr_c = ev["corr_vs_champion"]
            distinct = (corr_c is None) or (abs(corr_c) < ACTIVE_MAX_CORR_VS_CHAMPION)
            ev["status"] = (STATUS_ACTIVE if (ic_t >= ACTIVE_MIN_IC_T and cov >= ACTIVE_MIN_COVERAGE
                                              and distinct) else STATUS_RESEARCH)
            ev["max_corr_vs_accepted"] = _round(max_corr, 4)
            accepted_ref.append((ev["spec"]["name"], ev["maps"]))

    # --- build registry records ------------------------------------------------------------
    cand_recs: list[dict] = []
    ev_by_name: dict[str, dict] = {}
    for ev in evals:
        rec = _price_record(ev, ev["status"], signal_date, meta, ev.get("reject_reason"),
                            ev.get("corr_vs_champion"), ev.get("max_corr_vs_accepted"))
        cand_recs.append(rec)
        ev_by_name[ev["spec"]["name"]] = ev

    survivors = [ev for ev in evals if ev["status"] in (STATUS_ACTIVE, STATUS_RESEARCH)]
    survivors.sort(key=lambda e: reg.leaderboard_sort_key({
        "net25": e["primary"].get("net25_spread"), "ic_t": e["primary"].get("ic_t_stat"),
        "ic": e["primary"].get("mean_ic")}), reverse=True)
    survivor_recs = {r["name"]: r for r in cand_recs}

    # best surviving candidate per family (by primary net25)
    best_by_family: dict[str, Optional[dict]] = {f: None for f in reg.PRICE_GATED_FAMILIES}
    for ev in survivors:
        fam = ev["spec"]["family"]
        cur = best_by_family.get(fam)
        if cur is None or (_to_float(ev["primary"].get("net25_spread")) or -9) > (_to_float(cur["primary"].get("net25_spread")) or -9):
            best_by_family[fam] = ev

    # --- champion + challenger registry rows (for context; never replaced) -----------------
    champion_rec = reg.make_alpha_record(
        name=CHAMPION_SIGNAL, family=reg.FAM_SECTOR_NEUTRAL, status=STATUS_CHAMPION,
        horizon=63, universe=UNIVERSE_NAME, signal_date=signal_date, metrics=champ_metrics,
        corr_vs_champion=1.0, regime_notes=_regime_notes(champ_metrics),
        description="Current paper champion (composite_sn), shown on the price-overlap grid for reference.",
        role="CURRENT PAPER CHAMPION",
        extra={"note": "unchanged by Phase 21; evaluated here on the price-overlap intersection only"})
    challenger_info, chall_corr = _challenger_from_report(_read_json(rpath), signal_date)
    challenger_rec = reg.make_alpha_record(
        name=CHALLENGER_SIGNAL, family=reg.FAM_SECTOR_NEUTRAL, status=STATUS_CHALLENGER,
        horizon=63, universe=UNIVERSE_NAME, signal_date=signal_date, metrics=challenger_info["metrics"],
        corr_vs_champion=chall_corr, regime_notes=_regime_notes(challenger_info["metrics"]),
        description="Sector-repaired paper challenger (composite_sn_repaired), enrolled from Phase 17-A.",
        role="SECTOR-REPAIRED PAPER CHALLENGER",
        extra={"enrollment_note": challenger_info["note"], "report_available": challenger_info["available"]})

    all_records = [champion_rec, challenger_rec] + cand_recs

    # --- leaderboard (champion reference + ranked survivors, each at its primary horizon) ---
    leaderboard = [{"rank": "champion", "is_champion": True, **_lb_row(champion_rec)}]
    for i, ev in enumerate(survivors, start=1):
        leaderboard.append({"rank": i, "is_champion": False, **_lb_row(survivor_recs[ev["spec"]["name"]])})

    # --- correlation, combinations, cross-check, taxonomy ----------------------------------
    correlation = _correlation_matrix(champ_maps, survivors, ev_by_name)
    combinations = _combinations(obs, best_by_family)
    cross_check = _forward_cross_check(obs)
    families = _family_summary(cand_recs, best_by_family)

    # --- horizon summary (every candidate x every horizon) ---------------------------------
    horizon_rows = []
    for ev in evals:
        for h in HORIZONS:
            m = ev["per_horizon"][h]
            horizon_rows.append({"name": ev["spec"]["name"], "family": ev["spec"]["family"],
                                 "horizon": h, "is_primary": h == ev["primary_horizon"],
                                 "mean_ic": m.get("mean_ic"), "ic_t": m.get("ic_t_stat"),
                                 "net25": m.get("net25_spread"), "turnover": m.get("mean_turnover"),
                                 "coverage_pct": m.get("coverage_pct"), "n_ic_months": m.get("n_ic_months")})

    # --- diagnostics -----------------------------------------------------------------------
    reject_counts: dict[str, int] = {}
    for r in cand_recs:
        if r["status"] == STATUS_REJECTED and r["reject_reason"]:
            reject_counts[r["reject_reason"]] = reject_counts.get(r["reject_reason"], 0) + 1
    # family best-horizon = the horizon with the highest mean IC t-stat across the family's
    # candidates (a labelled horizon-sensitivity diagnostic; NOT used to grant survivor status).
    family_best_horizon: dict[str, dict] = {}
    for fam in reg.PRICE_GATED_FAMILIES:
        fam_evals = [e for e in evals if e["spec"]["family"] == fam]
        best_h, best_v = None, None
        for h in HORIZONS:
            ts = [_to_float(e["per_horizon"][h].get("ic_t_stat")) for e in fam_evals]
            ts = [t for t in ts if t is not None]
            if not ts:
                continue
            avg = sum(ts) / len(ts)
            if best_v is None or avg > best_v:
                best_v, best_h = avg, h
        family_best_horizon[fam] = {"horizon": best_h, "mean_ic_t_at_horizon": _round(best_v, 4),
                                    "primary_horizon": PRIMARY_HORIZON[fam]}
    headline_findings = [
        ("Price factors are ~orthogonal to the fundamental champion (mean |corr vs composite_sn| %s) "
         "- genuine diversification vs the Phase 20 fundamental factor set." %
         correlation["summary"].get("mean_abs_corr_vs_champion")),
        ("Survivorship caveat: the owned price universe contains only names that survived to the "
         "download date. A survivor-only universe structurally inflates short-term reversal and "
         "deflates momentum, so momentum weakness / reversal strength here must be read with caution."),
        ("Low-volatility is inverted in this 2016-2026 large-cap window (high-vol names led); the "
         "low-vol candidates carry negative IC and are auto-rejected rather than sign-flipped."),
    ]
    diagnostics = {
        "headline_findings": headline_findings,
        "family_best_horizon": family_best_horizon,
        "generated_candidates": len(cand_recs),
        "survivors": len(survivors),
        "active": sum(1 for r in cand_recs if r["status"] == STATUS_ACTIVE),
        "research": sum(1 for r in cand_recs if r["status"] == STATUS_RESEARCH),
        "rejected": sum(1 for r in cand_recs if r["status"] == STATUS_REJECTED),
        "reject_reason_counts": reject_counts,
        "param_neighbor_unstable": sorted(param_unstable),
        "gate_thresholds": {"min_coverage_pct": MIN_COVERAGE_PCT, "min_ic_months": MIN_IC_MONTHS,
                            "min_ic_t": MIN_IC_T, "min_positive_ic_month": MIN_POSITIVE_IC_MONTH,
                            "max_corr_redundant": MAX_CORR_REDUNDANT, "severe_dd_ratio": SEVERE_DD_RATIO,
                            "turnover_cap": TURNOVER_CAP, "thin_net_floor": THIN_NET_FLOOR,
                            "active_min_ic_t": ACTIVE_MIN_IC_T, "active_min_coverage": ACTIVE_MIN_COVERAGE,
                            "active_max_corr_vs_champion": ACTIVE_MAX_CORR_VS_CHAMPION},
        "no_lookahead_cross_check": cross_check,
        "champion_on_intersection": {"mean_ic": _round(champ_metrics.get("mean_ic"), 6),
                                     "ic_t_stat": _round(champ_metrics.get("ic_t_stat"), 4),
                                     "net25_spread": _round(champ_metrics.get("net25_spread"), 6),
                                     "n_ic_months": champ_metrics.get("n_ic_months")},
    }

    result = {
        "phase": PHASE, "status": STATUS_READY, "signal_date": signal_date,
        "as_of_date": fpanel.get("as_of_date"), "horizons": HORIZONS, "primary_horizon": PRIMARY_HORIZON,
        "price_panel": {"manifest": price["manifest"], "overlap": meta,
                        "readiness": "READY" if meta["n_observations"] > 0 else "NO_OVERLAP"},
        "universe": {"name": UNIVERSE_NAME, "n_price_names": meta["n_price_matched_names"],
                     "n_fundamental_names": meta["n_fundamental_names"], "n_months": meta["n_months"],
                     "n_observations": meta["n_observations"]},
        "champion": champion_rec, "challenger": challenger_rec,
        "registry": {"schema": list(reg.REGISTRY_METADATA_FIELDS), "counts": reg.registry_counts(all_records),
                     "alphas": all_records},
        "leaderboard": leaderboard,
        "survivors": [_lb_row(survivor_recs[ev["spec"]["name"]], full=True) for ev in survivors],
        "rejected": [_reject_row(r) for r in cand_recs if r["status"] == STATUS_REJECTED],
        "best_by_family": {f: (best_by_family[f]["spec"]["name"] if best_by_family[f] else None)
                           for f in reg.PRICE_GATED_FAMILIES},
        "correlation": correlation, "combinations": combinations, "families": families,
        "horizon_summary": horizon_rows, "diagnostics": diagnostics,
        "candidate_reports": {r["name"]: r for r in cand_recs},
        "provenance": {"fundamental_panel_path": str(fpath), "price_panel_path": str(ppath),
                       "reval_report_path": str(rpath),
                       "sources": ["owned phase7i trailing-price panel", "frozen 10-L scored panel",
                                   "committed Phase 17-A report"],
                       "champion_signal": CHAMPION_SIGNAL, "challenger_signal": CHALLENGER_SIGNAL},
        "warnings": warnings,
    }
    if key is not None:
        _CACHE[key] = result
    return result


# --------------------------------------------------------------------------- #
# Read-only aggregate for the dashboard GET + slices
# --------------------------------------------------------------------------- #
def _persisted_state(sdir: Path) -> dict:
    state = _read_json(sdir / _RUN_STATE_FILE)
    files = [fn for fn in _ALL_ARTIFACTS if (sdir / fn).exists()]
    return {"has_artifacts": bool(state), "last_build_at": (state or {}).get("built_at"),
            "store_dir": str(sdir), "files": files,
            "last_build_signal_date": (state or {}).get("signal_date"),
            "last_build_counts": (state or {}).get("counts")}


def load_price_alpha_factory(*, panel_path=None, price_path=None, reval_report_path=None,
                             store_dir=None) -> dict:
    """Read-only aggregate dashboard payload. Computes the build in-memory (owned data), overlays the
    persisted store state, attaches the safety block. Never writes, never raises."""
    build = build_price_alpha_factory(panel_path=panel_path, price_path=price_path,
                                      reval_report_path=reval_report_path)
    sdir = _resolve_store(store_dir)
    if build.get("status") == STATUS_PANEL_UNAVAILABLE:
        payload = {"phase": PHASE, "status": STATUS_PANEL_UNAVAILABLE, "missing": build.get("missing"),
                   "fundamental_panel_path": build.get("fundamental_panel_path"),
                   "price_panel_path": build.get("price_panel_path"),
                   "warnings": build.get("warnings", []), "persisted": _persisted_state(sdir),
                   "next_recommended_action": ("Restore the missing owned panel, then re-read the Price "
                                               "Alpha Lab. No price candidates can be generated while a "
                                               "required owned panel is absent.")}
        payload.update(reg.safety_block())
        payload["loaded_at"] = _iso_now()
        return payload
    payload = dict(build)
    payload["persisted"] = _persisted_state(sdir)
    payload["next_recommended_action"] = (
        "Review the price-family leaderboard, correlation clusters and combination report, then "
        "optionally run a confirmed build to persist the Phase 21 artifact package. Paper research "
        "only - no candidate is promoted to live trading and the champion is never replaced.")
    payload.update(reg.safety_block())
    payload["loaded_at"] = _iso_now()
    return payload


def load_price_alpha_registry(**kw) -> dict:
    p = load_price_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "registry": p.get("registry"), "families": p.get("families"),
            "warnings": p.get("warnings", []), "persisted": p.get("persisted"),
            **reg.safety_block(), "loaded_at": _iso_now()}


def load_price_alpha_leaderboard(**kw) -> dict:
    p = load_price_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "leaderboard": p.get("leaderboard"), "survivors": p.get("survivors"),
            "best_by_family": p.get("best_by_family"), "horizon_summary": p.get("horizon_summary"),
            "champion": p.get("champion"), "warnings": p.get("warnings", []),
            **reg.safety_block(), "loaded_at": _iso_now()}


def load_price_alpha_correlation(**kw) -> dict:
    p = load_price_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "correlation": p.get("correlation"), "warnings": p.get("warnings", []),
            **reg.safety_block(), "loaded_at": _iso_now()}


def load_price_alpha_combinations(**kw) -> dict:
    p = load_price_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "combinations": p.get("combinations"), "best_by_family": p.get("best_by_family"),
            "warnings": p.get("warnings", []), **reg.safety_block(), "loaded_at": _iso_now()}


# --------------------------------------------------------------------------- #
# Artifact writing + the manual preview/commit build
# --------------------------------------------------------------------------- #
def _atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(obj, fh, indent=2, sort_keys=False)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _atomic_write_csv(path: Path, header: list[str], rows: list[list]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            w = csv.writer(fh)
            w.writerow(header)
            w.writerows(rows)
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)


def _write_artifacts(build: dict, sdir: Path, built_at: str) -> list[str]:
    sdir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []

    _atomic_write_json(sdir / _MANIFEST_FILE, {"phase": PHASE, "built_at": built_at,
                                               "price_panel": build["price_panel"],
                                               "universe": build["universe"],
                                               "horizons": build["horizons"],
                                               "primary_horizon": build["primary_horizon"],
                                               "provenance": build["provenance"]})
    written.append(_MANIFEST_FILE)

    _atomic_write_json(sdir / _REGISTRY_FILE, {"phase": PHASE, "built_at": built_at,
                                               "signal_date": build.get("signal_date"),
                                               "schema": build["registry"]["schema"],
                                               "counts": build["registry"]["counts"],
                                               "alphas": build["registry"]["alphas"]})
    written.append(_REGISTRY_FILE)

    _atomic_write_json(sdir / _LEADERBOARD_JSON, {"phase": PHASE, "built_at": built_at,
                                                  "leaderboard": build["leaderboard"]})
    written.append(_LEADERBOARD_JSON)
    lb_rows = [[r.get("rank"), r.get("name"), r.get("family"), r.get("status"), r.get("horizon"),
                r.get("ic"), r.get("ic_t"), r.get("net25"), r.get("turnover"), r.get("drawdown"),
                r.get("sharpe"), r.get("coverage_pct"), r.get("corr_vs_champion")]
               for r in build["leaderboard"]]
    _atomic_write_csv(sdir / _LEADERBOARD_CSV,
                      ["rank", "name", "family", "status", "horizon", "ic", "ic_t", "net25",
                       "turnover", "drawdown", "sharpe", "coverage_pct", "corr_vs_champion"], lb_rows)
    written.append(_LEADERBOARD_CSV)

    corr = build["correlation"]
    _atomic_write_json(sdir / _CORRELATION_JSON, {"phase": PHASE, "built_at": built_at, **corr})
    written.append(_CORRELATION_JSON)
    sig = corr["signals"]
    corr_rows = [[sig[i]] + [corr["matrix"][i][j] for j in range(len(sig))] for i in range(len(sig))]
    _atomic_write_csv(sdir / _CORRELATION_CSV, ["signal"] + sig, corr_rows)
    written.append(_CORRELATION_CSV)

    hz = build["horizon_summary"]
    _atomic_write_csv(sdir / _HORIZON_CSV,
                      ["name", "family", "horizon", "is_primary", "mean_ic", "ic_t", "net25",
                       "turnover", "coverage_pct", "n_ic_months"],
                      [[r["name"], r["family"], r["horizon"], r["is_primary"], r["mean_ic"], r["ic_t"],
                        r["net25"], r["turnover"], r["coverage_pct"], r["n_ic_months"]] for r in hz])
    written.append(_HORIZON_CSV)

    _atomic_write_csv(sdir / _REJECTION_CSV,
                      ["name", "family", "horizon", "reject_reason", "ic", "ic_t", "net25",
                       "coverage_pct", "turnover", "drawdown", "corr_vs_champion", "max_corr_vs_accepted"],
                      [[r["name"], r["family"], r["horizon"], r["reject_reason"], r["ic"], r["ic_t"],
                        r["net25"], r["coverage_pct"], r["turnover"], r["drawdown"],
                        r["corr_vs_champion"], r["max_corr_vs_accepted"]] for r in build["rejected"]])
    written.append(_REJECTION_CSV)

    _atomic_write_json(sdir / _FAMILY_JSON, {"phase": PHASE, "built_at": built_at,
                                             "families": build["families"],
                                             "best_by_family": build["best_by_family"]})
    written.append(_FAMILY_JSON)

    _atomic_write_json(sdir / _COMBINATION_JSON, {"phase": PHASE, "built_at": built_at,
                                                  **build["combinations"]})
    written.append(_COMBINATION_JSON)

    _atomic_write_json(sdir / _DIAGNOSTICS_JSON, {"phase": PHASE, "built_at": built_at,
                                                  **build["diagnostics"]})
    written.append(_DIAGNOSTICS_JSON)

    _atomic_write_json(sdir / _FINAL_REPORT_JSON, _final_report(build, built_at))
    written.append(_FINAL_REPORT_JSON)

    _atomic_write_json(sdir / _RUN_STATE_FILE, {"phase": PHASE, "built_at": built_at,
                                                "signal_date": build.get("signal_date"),
                                                "counts": build["registry"]["counts"],
                                                "universe": build.get("universe"),
                                                "provenance": build.get("provenance"),
                                                "artifacts": written + [_RUN_STATE_FILE]})
    written.append(_RUN_STATE_FILE)
    return written


def _final_report(build: dict, built_at: str) -> dict:
    d = build["diagnostics"]
    return {"phase": PHASE, "built_at": built_at, "signal_date": build.get("signal_date"),
            "price_source": pp.SOURCE_NAME,
            "panel_date_range": [build["price_panel"]["manifest"].get("date_start"),
                                 build["price_panel"]["manifest"].get("date_end")],
            "universe": build["universe"], "counts": build["registry"]["counts"],
            "families": build["families"], "best_by_family": build["best_by_family"],
            "correlation_summary": build["correlation"]["summary"],
            "combination_summary": {"n_combinations": build["combinations"]["n_combinations"],
                                    "n_challenger_eligible": build["combinations"]["n_challenger_eligible"]},
            "reject_reason_counts": d["reject_reason_counts"],
            "no_lookahead_cross_check": d["no_lookahead_cross_check"],
            "champion_unchanged": True, "champion_replaced": False, "promotes_to_live": False,
            **reg.safety_block()}


def run_price_alpha_factory(*, commit: bool = False, confirm: Optional[str] = None,
                            panel_path=None, price_path=None, reval_report_path=None,
                            store_dir=None, built_at: Optional[str] = None) -> dict:
    """Manual Phase 21 build. commit=False previews (no writes); commit=True requires the confirmation
    token and persists the Phase 21 artifact package to the dedicated LOCAL store only."""
    build = build_price_alpha_factory(panel_path=panel_path, price_path=price_path,
                                      reval_report_path=reval_report_path)
    sdir = _resolve_store(store_dir)

    if build.get("status") == STATUS_PANEL_UNAVAILABLE:
        out = {"phase": PHASE, "status": STATUS_PANEL_UNAVAILABLE, "wrote_store": False,
               "performed_write": False, "missing": build.get("missing"),
               "warnings": build.get("warnings", [])}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    counts = build["registry"]["counts"]
    if not commit:
        out = {"phase": PHASE, "status": STATUS_BUILD_PREVIEW, "wrote_store": False,
               "performed_write": False, "signal_date": build.get("signal_date"),
               "registry_counts": counts, "diagnostics": build["diagnostics"],
               "families": build["families"], "combinations": build["combinations"],
               "price_panel": build["price_panel"], "store_dir": str(sdir),
               "would_write_files": list(_ALL_ARTIFACTS), "confirm_required_token": BUILD_CONFIRM_TOKEN,
               "warnings": build.get("warnings", [])}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    if confirm != BUILD_CONFIRM_TOKEN:
        out = {"phase": PHASE, "status": STATUS_CONFIRM_REQUIRED, "wrote_store": False,
               "performed_write": False,
               "message": "A committing price-alpha build requires confirm='%s'." % BUILD_CONFIRM_TOKEN}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    built_at = built_at or _iso_now()
    written = _write_artifacts(build, sdir, built_at)
    clear_cache()
    out = {"phase": PHASE, "status": STATUS_BUILD_COMPLETE, "wrote_store": True,
           "performed_write": True, "built_at": built_at, "signal_date": build.get("signal_date"),
           "registry_counts": counts, "diagnostics": build["diagnostics"],
           "families": build["families"], "combinations": build["combinations"],
           "store_dir": str(sdir), "files_written": written, "warnings": build.get("warnings", [])}
    out.update(reg.safety_block())
    out["loaded_at"] = _iso_now()
    return out


__all__ = [
    "PHASE", "BUILD_CONFIRM_TOKEN", "HORIZONS", "PRIMARY_HORIZON", "STORE_ENV",
    "STATUS_READY", "STATUS_PANEL_UNAVAILABLE", "STATUS_BUILD_PREVIEW", "STATUS_BUILD_COMPLETE",
    "STATUS_CONFIRM_REQUIRED", "ELIG_CHALLENGER", "ELIG_RESEARCH",
    "build_price_alpha_factory", "run_price_alpha_factory", "load_price_alpha_factory",
    "load_price_alpha_registry", "load_price_alpha_leaderboard", "load_price_alpha_correlation",
    "load_price_alpha_combinations", "clear_cache",
    "_MANIFEST_FILE", "_REGISTRY_FILE", "_LEADERBOARD_JSON", "_CORRELATION_JSON", "_HORIZON_CSV",
    "_REJECTION_CSV", "_FAMILY_JSON", "_COMBINATION_JSON", "_DIAGNOSTICS_JSON", "_FINAL_REPORT_JSON",
    "_RUN_STATE_FILE", "_ALL_ARTIFACTS",
]
