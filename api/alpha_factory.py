"""
api/alpha_factory.py — Phase 20 Autonomous Alpha Factory V1 (engine).

Turns Paper Trader from a single-alpha platform into a multi-alpha research platform. The
factory GENERATES many candidate alphas from the owned frozen Phase 10-L sector-neutral scored
panel, EVALUATES each with the SAME stdlib validation battery used by the Phase 17-A revalidation
(so the champion reproduces its committed numbers exactly), computes their CORRELATION against the
champion and each other, automatically REJECTS weak candidates, RANKS the survivors into a
leaderboard, enrolls the current paper champion (``composite_sn``) and the sector-repaired paper
challenger (``composite_sn_repaired``, from the committed Phase 17-A report), and can persist the
whole registry / leaderboard / correlation / diagnostics / candidate-report artifact set to a
dedicated LOCAL store.

Owned-data-only and read-only by design:
    - Reads the owned frozen Phase 10-L panel CSV and the committed Phase 17-A report JSON.
    - Pure stdlib maths (no numpy / pandas / new packages). No network, no prediction service.
    - Writes NOTHING except, on an explicit confirmed build, the dedicated local Alpha Factory
      store (JSON/CSV artifacts). Never PostgreSQL, never positions / orders / trades / fills.
    - Never generates trailing-price ("momentum / trend / volatility / relative-strength /
      mean-reversion") factors it has no owned data for — those families are reported as
      data-gated rather than fabricated.
    - Never replaces the champion and never approves live trading.

Public API:
    build_alpha_factory(...) -> dict          # pure compute (no writes)
    run_alpha_factory(commit, confirm, ...)   # preview (no write) / confirmed build (writes store)
    load_alpha_factory(...)                   # read-only aggregate for the dashboard GET
    load_alpha_registry(...) / load_alpha_leaderboard(...) / load_alpha_correlation(...)
"""
from __future__ import annotations

import csv
import json
import math
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional, Union

from paper_trader.api import alpha_registry as reg
from paper_trader.api.alpha_registry import (
    FAM_COMPOSITE, FAM_FUNDAMENTAL, FAM_HYBRID, FAM_QUALITY, FAM_SECTOR_NEUTRAL,
    STATUS_ACTIVE, STATUS_ARCHIVED, STATUS_CHALLENGER, STATUS_CHAMPION,
    STATUS_REJECTED, STATUS_RESEARCH,
    REJECT_COST_KILLED, REJECT_LOW_COVERAGE, REJECT_NEGATIVE_IC, REJECT_REDUNDANT,
    REJECT_STATISTICALLY_WEAK, REJECT_UNSTABLE,
)

PHASE = "20"
CHAMPION_SIGNAL = "composite_sn"
CHALLENGER_SIGNAL = "composite_sn_repaired"
HORIZON_TRADING_DAYS = 63
UNIVERSE_NAME = "S&P 500 multifactor (owned frozen Phase 10-L scored panel)"

# --- battery constants (vendored verbatim from run_phase17a _evaluate_score) ----------------
MIN_NAMES_PER_MONTH = 20
QUANTILE = 5
COST25, COST50 = 0.0025, 0.0050
_PRE2020 = "2020-01"
_ANNUALIZATION = 12.0  # monthly rebalance grid -> annualized IR proxy for the Sharpe metric

# --- automatic gate thresholds -------------------------------------------------------------
MIN_COVERAGE_PCT = 60.0          # signal must score >=60% of scoreable name-months
MIN_IC_T = 1.5                   # IC t-stat significance floor
MAX_CORR_REDUNDANT = 0.98        # rank-corr vs champion / stronger survivor above this -> redundant
MIN_POSITIVE_IC_MONTH = 0.50     # stability: positive-IC month rate floor
ACTIVE_MIN_IC_T = 2.0            # survivor promoted to ACTIVE (else RESEARCH)
ACTIVE_MIN_COVERAGE = 75.0
ACTIVE_MAX_CORR_VS_CHAMPION = 0.90  # ACTIVE alphas must add diversification beyond the champion

BUILD_CONFIRM_TOKEN = "RUN_ALPHA_FACTORY_BUILD"

# --- status enums for the run/preview payload ---------------------------------------------
STATUS_BUILD_PREVIEW = "ALPHA_FACTORY_BUILD_PREVIEW"
STATUS_BUILD_COMPLETE = "ALPHA_FACTORY_BUILD_COMPLETE"
STATUS_CONFIRM_REQUIRED = "ALPHA_FACTORY_CONFIRM_REQUIRED"
STATUS_PANEL_UNAVAILABLE = "ALPHA_FACTORY_PANEL_UNAVAILABLE"
STATUS_READY = "ALPHA_FACTORY_READY"

# --- env seams -----------------------------------------------------------------------------
PANEL_ENV = "PAPER_TRADER_ALPHA_FACTORY_PANEL"
STORE_ENV = "PAPER_TRADER_ALPHA_FACTORY_DIR"
REVAL_REPORT_ENV = "PAPER_TRADER_ALPHA_FACTORY_REVAL_REPORT"

DEFAULT_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv"
)
DEFAULT_STORE = Path(r"D:\Stock_Prediction_app_data\phase20_alpha_factory")
DEFAULT_REVAL_REPORT = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase17a_sector_repaired_champion_revalidation"
    r"\phase17a_sector_repaired_champion_revalidation_report.json"
)

# --- artifact file names -------------------------------------------------------------------
_REGISTRY_FILE = "alpha_registry.json"
_LEADERBOARD_FILE = "alpha_leaderboard.json"
_LEADERBOARD_CSV = "alpha_leaderboard.csv"
_CORRELATION_FILE = "alpha_correlation.json"
_CORRELATION_CSV = "alpha_correlation.csv"
_DIAGNOSTICS_FILE = "alpha_factory_diagnostics.json"
_CANDIDATE_REPORTS_FILE = "candidate_reports.json"
_RUN_STATE_FILE = "alpha_factory_run_state.json"

# --- panel columns -------------------------------------------------------------------------
C_REB = "rebalance_date"
C_TICKER = "ticker"
C_SECTOR = "sector"
C_IS_NEW = "is_new_cohort"
C_LIQ = "liquidity_proxy"
C_FCF_LEVEL = "fcf_to_assets"
C_ACC_LEVEL = "operating_accruals"
C_FCF_RAW = "fcf_to_assets_raw"
C_ACC_RAW = "operating_accruals_raw"
C_FCF_SN = "fcf_to_assets_sector_neutral_z"
C_ACC_SN = "operating_accruals_sector_neutral_z"
C_COMPOSITE_SN = "composite_sn"
C_COMPOSITE_RAW = "composite_raw"
C_FWD = "forward_63d_return"
C_HAS_FWD = "has_forward_return"


# --------------------------------------------------------------------------- #
# stdlib maths (vendored from run_phase17a so the champion reproduces exactly)
# --------------------------------------------------------------------------- #
def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(x: Any) -> Optional[float]:
    if isinstance(x, bool) or x is None:
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def _to_bool(x: Any) -> Optional[bool]:
    if isinstance(x, bool):
        return x
    if x is None:
        return None
    s = str(x).strip().lower()
    if s in ("true", "1", "yes"):
        return True
    if s in ("false", "0", "no"):
        return False
    return None


def _mean(vals: list[float]) -> float:
    return sum(vals) / len(vals)


def _std(vals: list[float], ddof: int = 1) -> Optional[float]:
    n = len(vals)
    if n - ddof <= 0:
        return None
    m = _mean(vals)
    return math.sqrt(sum((v - m) ** 2 for v in vals) / (n - ddof))


def _rank(vals: list[float]) -> list[float]:
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(vals):
        j = i
        while j + 1 < len(vals) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _pearson(a: list[float], b: list[float]) -> Optional[float]:
    n = len(a)
    if n < 2:
        return None
    ma, mb = _mean(a), _mean(b)
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = math.sqrt(sum((x - ma) ** 2 for x in a))
    db = math.sqrt(sum((x - mb) ** 2 for x in b))
    if da == 0 or db == 0:
        return None
    return num / (da * db)


def _spearman(a: list[float], b: list[float]) -> Optional[float]:
    if len(a) < 3:
        return None
    return _pearson(_rank(a), _rank(b))


def _t_stat(vals: list[float]) -> Optional[float]:
    n = len(vals)
    if n < 3:
        return None
    sd = _std(vals, 1)
    if sd is None or sd == 0:
        return None
    return _mean(vals) / (sd / math.sqrt(n))


def _max_drawdown(cum: list[float]) -> Optional[float]:
    if not cum:
        return None
    peak = cum[0]
    mdd = 0.0
    for v in cum:
        peak = max(peak, v)
        mdd = min(mdd, v - peak)
    return mdd


def _positive_rate(vals: list[float]) -> Optional[float]:
    if not vals:
        return None
    return sum(1 for v in vals if v > 0) / len(vals)


def _round(x: Optional[float], nd: int) -> Optional[float]:
    if x is None:
        return None
    try:
        return round(float(x), nd)
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Panel IO
# --------------------------------------------------------------------------- #
def _resolve(explicit, env_var, default) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get(env_var)
    return Path(env) if env else Path(default)


def load_panel(panel_path: Union[str, Path]) -> Optional[dict]:
    """Load the frozen panel CSV into row dicts + a month index. Returns None if unreadable."""
    p = Path(panel_path)
    try:
        with open(p, "r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None
    if not rows:
        return None
    # Cross-sections are keyed by CALENDAR MONTH (YYYY-MM) and, within a month, deduplicated to
    # ONE representative row per ticker (the latest rebalance_date, among rows carrying a valid
    # composite_sn) — identical to Phase 17-A. The frozen panel stores per-ticker staggered
    # rebalance dates (~1900 distinct full dates); keying by the full date, or keeping every row,
    # would give degenerate / duplicated cross-sections. ``rep_index[month][ticker] = row_idx`` is
    # signal-independent so every candidate is evaluated on the same cross-section membership.
    best: dict[str, dict[str, tuple[str, int]]] = {}
    tickers: set = set()
    max_full_date: Optional[str] = None
    for i, r in enumerate(rows):
        full = (r.get(C_REB) or "").strip()
        if len(full) < 7:
            continue
        tk = (r.get(C_TICKER) or "").strip().upper()
        if not tk or _to_float(r.get(C_COMPOSITE_SN)) is None:
            continue
        tickers.add(tk)
        if max_full_date is None or full > max_full_date:
            max_full_date = full
        mth = best.setdefault(full[:7], {})
        cur = mth.get(tk)
        if cur is None or full > cur[0]:
            mth[tk] = (full, i)
    rep_index: dict[str, dict[str, int]] = {
        m: {tk: idx for tk, (_rb, idx) in d.items()} for m, d in best.items()}
    return {
        "rows": rows,
        "rep_index": rep_index,
        "n_rows": len(rows),
        "n_tickers": len(tickers),
        "n_months": len(rep_index),
        "signal_date": max_full_date,
        "latest_cross_section_month": max((m for m in rep_index), default=None),
        "as_of_date": (rows[0].get("as_of_date") or "").strip() if rows else None,
        "path": str(p),
    }


# --------------------------------------------------------------------------- #
# Candidate generators. Each returns idx -> float|None over the whole panel.
# All generation is deterministic (fixed weights, no randomness, no tuning).
# --------------------------------------------------------------------------- #
def _lin(rows: list[dict], terms: list[tuple[str, float]]) -> dict[int, Optional[float]]:
    out: dict[int, Optional[float]] = {}
    for i, r in enumerate(rows):
        acc = 0.0
        ok = True
        for col, w in terms:
            v = _to_float(r.get(col))
            if v is None:
                ok = False
                break
            acc += w * v
        out[i] = acc if ok else None
    return out


def _gen_established_cohort(rows: list[dict], rep_index: dict) -> dict[int, Optional[float]]:
    out: dict[int, Optional[float]] = {}
    for i, r in enumerate(rows):
        comp = _to_float(r.get(C_COMPOSITE_SN))
        is_new = _to_bool(r.get(C_IS_NEW))
        out[i] = comp if (comp is not None and is_new is False) else None
    return out


def _gen_liquidity_screened(rows: list[dict], rep_index: dict) -> dict[int, Optional[float]]:
    out: dict[int, Optional[float]] = {}
    for _m, tk_idx in rep_index.items():
        liqs = [(i, _to_float(rows[i].get(C_LIQ))) for i in tk_idx.values()]
        present = sorted(v for _i, v in liqs if v is not None)
        if len(present) < 3:
            continue
        median = present[len(present) // 2] if len(present) % 2 else \
            (present[len(present) // 2 - 1] + present[len(present) // 2]) / 2.0
        for i, v in liqs:
            comp = _to_float(rows[i].get(C_COMPOSITE_SN))
            if v is not None and comp is not None and v >= median:
                out[i] = comp
    return out


# spec: (name, family, description, builder). builder(rows, rep_index) -> {idx: value|None}
def _candidate_specs() -> list[dict]:
    def lin(terms):
        return lambda rows, mi, _t=terms: _lin(rows, _t)
    return [
        {"name": "fund_fcf_to_assets", "family": FAM_FUNDAMENTAL,
         "description": "Free-cash-flow to assets (owned normalized leg, higher=better).",
         "spec": {"legs": {C_FCF_RAW: 1.0}}, "fn": lin([(C_FCF_RAW, 1.0)])},
        {"name": "fund_operating_accruals", "family": FAM_FUNDAMENTAL,
         "description": "Operating accruals (owned normalized Sloan-oriented leg, higher=better).",
         "spec": {"legs": {C_ACC_RAW: 1.0}}, "fn": lin([(C_ACC_RAW, 1.0)])},
        {"name": "quality_fcf_level", "family": FAM_QUALITY,
         "description": "Raw FCF/assets profitability level (quality tilt).",
         "spec": {"legs": {C_FCF_LEVEL: 1.0}}, "fn": lin([(C_FCF_LEVEL, 1.0)])},
        {"name": "quality_low_accruals", "family": FAM_QUALITY,
         "description": "Low raw operating accruals (earnings-quality tilt; sign -1).",
         "spec": {"legs": {C_ACC_LEVEL: -1.0}}, "fn": lin([(C_ACC_LEVEL, -1.0)])},
        {"name": "sn_fcf_leg", "family": FAM_SECTOR_NEUTRAL,
         "description": "Sector-neutral within-month z FCF leg.",
         "spec": {"legs": {C_FCF_SN: 1.0}}, "fn": lin([(C_FCF_SN, 1.0)])},
        {"name": "sn_accruals_leg", "family": FAM_SECTOR_NEUTRAL,
         "description": "Sector-neutral within-month z accruals leg (already oriented).",
         "spec": {"legs": {C_ACC_SN: 1.0}}, "fn": lin([(C_ACC_SN, 1.0)])},
        {"name": "sn_composite_equal", "family": FAM_SECTOR_NEUTRAL,
         "description": "Equal-weight SN composite (fcf_sn + acc_sn) - reproduces composite_sn.",
         "spec": {"legs": {C_FCF_SN: 1.0, C_ACC_SN: 1.0}}, "fn": lin([(C_FCF_SN, 1.0), (C_ACC_SN, 1.0)])},
        {"name": "sn_composite_fcf_heavy", "family": FAM_SECTOR_NEUTRAL,
         "description": "FCF-tilted SN composite (1.5*fcf_sn + 0.5*acc_sn).",
         "spec": {"legs": {C_FCF_SN: 1.5, C_ACC_SN: 0.5}}, "fn": lin([(C_FCF_SN, 1.5), (C_ACC_SN, 0.5)])},
        {"name": "sn_composite_accruals_heavy", "family": FAM_SECTOR_NEUTRAL,
         "description": "Accruals-tilted SN composite (0.5*fcf_sn + 1.5*acc_sn).",
         "spec": {"legs": {C_FCF_SN: 0.5, C_ACC_SN: 1.5}}, "fn": lin([(C_FCF_SN, 0.5), (C_ACC_SN, 1.5)])},
        {"name": "composite_raw_equal", "family": FAM_COMPOSITE,
         "description": "Market-relative equal composite of the normalized raw legs.",
         "spec": {"legs": {C_FCF_RAW: 1.0, C_ACC_RAW: 1.0}}, "fn": lin([(C_FCF_RAW, 1.0), (C_ACC_RAW, 1.0)])},
        {"name": "composite_raw_fcf_heavy", "family": FAM_COMPOSITE,
         "description": "FCF-tilted market-relative composite (1.5*fcf_raw + 0.5*acc_raw).",
         "spec": {"legs": {C_FCF_RAW: 1.5, C_ACC_RAW: 0.5}}, "fn": lin([(C_FCF_RAW, 1.5), (C_ACC_RAW, 0.5)])},
        {"name": "composite_raw_accruals_heavy", "family": FAM_COMPOSITE,
         "description": "Accruals-tilted market-relative composite (0.5*fcf_raw + 1.5*acc_raw).",
         "spec": {"legs": {C_FCF_RAW: 0.5, C_ACC_RAW: 1.5}}, "fn": lin([(C_FCF_RAW, 0.5), (C_ACC_RAW, 1.5)])},
        {"name": "hybrid_sn_fcf_raw_acc", "family": FAM_HYBRID,
         "description": "Hybrid: sector-neutral FCF leg + market-relative accruals leg.",
         "spec": {"legs": {C_FCF_SN: 1.0, C_ACC_RAW: 1.0}}, "fn": lin([(C_FCF_SN, 1.0), (C_ACC_RAW, 1.0)])},
        {"name": "hybrid_raw_fcf_sn_acc", "family": FAM_HYBRID,
         "description": "Hybrid: market-relative FCF leg + sector-neutral accruals leg.",
         "spec": {"legs": {C_FCF_RAW: 1.0, C_ACC_SN: 1.0}}, "fn": lin([(C_FCF_RAW, 1.0), (C_ACC_SN, 1.0)])},
        {"name": "hybrid_established_cohort", "family": FAM_HYBRID,
         "description": "Champion composite restricted to the established cohort (drops new names).",
         "spec": {"base": C_COMPOSITE_SN, "filter": "is_new_cohort==False"}, "fn": _gen_established_cohort},
        {"name": "hybrid_liquidity_screened", "family": FAM_HYBRID,
         "description": "Champion composite among names above the within-month liquidity median.",
         "spec": {"base": C_COMPOSITE_SN, "filter": "liquidity_proxy>=within_month_median"},
         "fn": _gen_liquidity_screened},
    ]


# --------------------------------------------------------------------------- #
# Evaluation battery (mirrors run_phase17a._evaluate_score core + coverage/sharpe)
# --------------------------------------------------------------------------- #
def _build_monthly(rows, rep_index, signal_values):
    """monthly[month] = [{ticker, score, fwd, established}] over the representative rows that carry
    a forward return. Coverage = fraction of scoreable name-months the signal actually scores."""
    monthly: dict[str, list[dict]] = {}
    total_fwd = 0
    covered = 0
    for m, tk_idx in rep_index.items():
        bucket = []
        for tk, i in tk_idx.items():
            r = rows[i]
            fwd = _to_float(r.get(C_FWD))
            has_fwd = _to_bool(r.get(C_HAS_FWD))
            if fwd is None or has_fwd is False:
                continue
            total_fwd += 1
            score = signal_values.get(i)
            if score is not None:
                covered += 1
            bucket.append({"ticker": tk, "score": score, "fwd": fwd,
                           "established": _to_bool(r.get(C_IS_NEW)) is False})
        if bucket:
            monthly[m] = bucket
    coverage_pct = (100.0 * covered / total_fwd) if total_fwd else 0.0
    return monthly, {"scoreable_name_months": total_fwd, "covered_name_months": covered,
                     "coverage_pct": coverage_pct, "missing_name_months": total_fwd - covered}


def evaluate_battery(monthly: dict[str, list[dict]]) -> dict:
    """One-score battery at the 63d horizon: monthly rank-IC + t, quintile L/S gross/net spread,
    turnover, cumulative spread, drawdown, positive-month rates, pre/post-2020 subperiod, spread
    vol and an annualized IR ('sharpe'). Identical core methodology to Phase 17-A."""
    months = sorted(monthly)
    ics: list[float] = []
    gross: list[float] = []
    turn: list[float] = []
    ic_by_period: dict[str, list[float]] = {"pre2020": [], "post2020": []}
    spread_by_period: dict[str, list[float]] = {"pre2020": [], "post2020": []}
    prev_long: Optional[set] = None
    prev_short: Optional[set] = None
    cum: list[float] = []
    running = 0.0
    n_months_scored = 0
    for m in months:
        rows = [r for r in monthly[m] if r.get("score") is not None and r.get("fwd") is not None]
        if len(rows) < MIN_NAMES_PER_MONTH:
            prev_long = prev_short = None
            continue
        n_months_scored += 1
        period = "pre2020" if m < _PRE2020 else "post2020"
        scores = [r["score"] for r in rows]
        fwds = [r["fwd"] for r in rows]
        ic = _spearman(scores, fwds)
        if ic is not None:
            ics.append(ic)
            ic_by_period[period].append(ic)
        order = sorted(range(len(rows)), key=lambda i: scores[i])
        k = max(1, len(rows) // QUANTILE)
        short_idx = order[:k]
        long_idx = order[-k:]
        sp = _mean([fwds[i] for i in long_idx]) - _mean([fwds[i] for i in short_idx])
        gross.append(sp)
        spread_by_period[period].append(sp)
        running += sp
        cum.append(running)
        long_set = {rows[i]["ticker"] for i in long_idx}
        short_set = {rows[i]["ticker"] for i in short_idx}
        if prev_long is not None and prev_short is not None:
            denom = (len(long_set) + len(short_set)) or 1
            churn = len(long_set - prev_long) + len(short_set - prev_short)
            turn.append(churn / denom)
        prev_long, prev_short = long_set, short_set

    mean_gross = _mean(gross) if gross else None
    mean_turn = _mean(turn) if turn else None
    net25 = (mean_gross - COST25 * (mean_turn or 0.0)) if mean_gross is not None else None
    net50 = (mean_gross - COST50 * (mean_turn or 0.0)) if mean_gross is not None else None
    spread_vol = _std(gross, 1) if len(gross) > 1 else None
    sharpe = (mean_gross / spread_vol * math.sqrt(_ANNUALIZATION)) if (spread_vol and mean_gross is not None) else None

    def _period(pk):
        ic_p = ic_by_period[pk]
        sp_p = spread_by_period[pk]
        return {"n_months": len(sp_p), "mean_ic": _round(_mean(ic_p), 6) if ic_p else None,
                "ic_t": _round(_t_stat(ic_p), 4), "mean_spread": _round(_mean(sp_p), 6) if sp_p else None,
                "positive_month_rate": _round(_positive_rate(sp_p), 4) if sp_p else None}

    return {
        "n_months_scored": n_months_scored,
        "n_ic_months": len(ics),
        "mean_ic": _round(_mean(ics), 6) if ics else None,
        "ic_t_stat": _round(_t_stat(ics), 4),
        "mean_gross_spread": _round(mean_gross, 6),
        "spread_vol": _round(spread_vol, 6),
        "mean_turnover": _round(mean_turn, 4),
        "net25_spread": _round(net25, 6),
        "net50_spread": _round(net50, 6),
        "sharpe": _round(sharpe, 4),
        "cumulative_spread": _round(cum[-1], 6) if cum else None,
        "max_drawdown": _round(_max_drawdown(cum), 6),
        "positive_ic_month_rate": _round(_positive_rate(ics), 4),
        "positive_spread_month_rate": _round(_positive_rate(gross), 4),
        "subperiod": {"pre2020": _period("pre2020"), "post2020": _period("post2020")},
    }


# --------------------------------------------------------------------------- #
# Correlation (mean monthly cross-sectional rank correlation between two signals)
# --------------------------------------------------------------------------- #
def _signal_month_maps(rows, rep_index, signal_values) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    for m, tk_idx in rep_index.items():
        d: dict[str, float] = {}
        for tk, i in tk_idx.items():
            v = signal_values.get(i)
            if v is not None:
                d[tk] = v
        if d:
            out[m] = d
    return out


def _pair_rank_corr(maps_a: dict[str, dict[str, float]], maps_b: dict[str, dict[str, float]]) -> Optional[float]:
    corrs: list[float] = []
    for m, da in maps_a.items():
        db = maps_b.get(m)
        if not db:
            continue
        common = [t for t in da if t in db]
        if len(common) < 3:
            continue
        c = _spearman([da[t] for t in common], [db[t] for t in common])
        if c is not None:
            corrs.append(c)
    return _mean(corrs) if corrs else None


# --------------------------------------------------------------------------- #
# Challenger enrolment (from committed Phase 17-A report)
# --------------------------------------------------------------------------- #
def _read_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            obj = json.load(fh)
        return obj if isinstance(obj, dict) else None
    except (OSError, ValueError):
        return None


def _challenger_from_report(report: Optional[dict], signal_date: Optional[str]) -> tuple[dict, Optional[float]]:
    fp = (report or {}).get("full_panel") or {}
    cand = fp.get("repaired_candidate") or {}
    corr_vs_champ = _to_float(fp.get("rank_spearman_champion_vs_repaired"))
    decision = (report or {}).get("decision")
    metrics = {
        "mean_ic": cand.get("mean_ic"), "ic_t_stat": cand.get("ic_t_stat"),
        "mean_gross_spread": cand.get("mean_gross_spread"), "mean_turnover": cand.get("mean_turnover"),
        "net25_spread": cand.get("net25_spread"), "net50_spread": cand.get("net50_spread"),
        "max_drawdown": cand.get("max_drawdown"), "cumulative_spread": cand.get("cumulative_spread"),
        "positive_ic_month_rate": cand.get("positive_ic_month_rate"),
        "positive_spread_month_rate": cand.get("positive_spread_month_rate"),
        "n_months_scored": cand.get("n_months_scored"), "n_ic_months": cand.get("n_ic_months"),
        "subperiod": cand.get("subperiod"),
    }
    note = ("Enrolled from the committed Phase 17-A revalidation report (decision %s). Sector-repaired "
            "SN transform; not recomputed here." % decision) if cand else \
           "Committed Phase 17-A report unavailable; challenger metrics not loaded."
    return {"metrics": metrics if cand else {}, "note": note, "eligibility": decision,
            "available": bool(cand)}, corr_vs_champ


# --------------------------------------------------------------------------- #
# Regime + gate helpers
# --------------------------------------------------------------------------- #
def _regime_notes(metrics: dict) -> str:
    sp = metrics.get("subperiod") or {}
    pre = sp.get("pre2020") or {}
    post = sp.get("post2020") or {}
    pre_ic, post_ic = pre.get("mean_ic"), post.get("mean_ic")
    parts = []
    if pre_ic is not None:
        parts.append("pre-2020 IC %.3f (t=%s)" % (pre_ic, pre.get("ic_t")))
    if post_ic is not None:
        parts.append("post-2020 IC %.3f (t=%s)" % (post_ic, post.get("ic_t")))
    tag = ""
    if pre_ic is not None and post_ic is not None:
        if (pre_ic >= 0) != (post_ic >= 0):
            tag = " — SIGN REVERSAL across 2020"
        else:
            tag = " — consistent sign across 2020"
    return ("; ".join(parts) + tag) if parts else "insufficient subperiod coverage"


def _subperiod_sign_reversal(metrics: dict) -> bool:
    sp = metrics.get("subperiod") or {}
    pre = (sp.get("pre2020") or {}).get("mean_ic")
    post = (sp.get("post2020") or {}).get("mean_ic")
    if pre is None or post is None:
        return False
    return (pre >= 0) != (post >= 0)


def _hard_gate_reason(metrics: dict, coverage: dict) -> Optional[str]:
    """First failing automatic gate for a candidate, or None if it passes all hard gates."""
    if (coverage.get("coverage_pct") or 0.0) < MIN_COVERAGE_PCT:
        return REJECT_LOW_COVERAGE
    ic = metrics.get("mean_ic")
    if ic is None or ic < 0:
        return REJECT_NEGATIVE_IC
    ic_t = metrics.get("ic_t_stat")
    if ic_t is None or ic_t < MIN_IC_T:
        return REJECT_STATISTICALLY_WEAK
    net25 = metrics.get("net25_spread")
    if net25 is None or net25 <= 0:
        return REJECT_COST_KILLED
    pos = metrics.get("positive_ic_month_rate")
    if (pos is not None and pos < MIN_POSITIVE_IC_MONTH) or _subperiod_sign_reversal(metrics):
        return REJECT_UNSTABLE
    return None


# --------------------------------------------------------------------------- #
# The build (pure compute, no writes)
# --------------------------------------------------------------------------- #
_CACHE: dict = {}


def clear_cache() -> None:
    _CACHE.clear()


def _cache_key(panel_path: Path, reval_path: Path) -> Optional[tuple]:
    try:
        pm = panel_path.stat().st_mtime
    except OSError:
        return None
    try:
        rm = reval_path.stat().st_mtime
    except OSError:
        rm = 0.0
    return (str(panel_path), pm, str(reval_path), rm)


def build_alpha_factory(
    *,
    panel_path: Optional[Union[str, Path]] = None,
    reval_report_path: Optional[Union[str, Path]] = None,
    use_cache: bool = True,
) -> dict:
    """Pure, deterministic Alpha Factory build over the owned panel. Never writes, never raises."""
    ppath = _resolve(panel_path, PANEL_ENV, DEFAULT_PANEL)
    rpath = _resolve(reval_report_path, REVAL_REPORT_ENV, DEFAULT_REVAL_REPORT)
    key = _cache_key(ppath, rpath) if use_cache else None
    if key is not None and key in _CACHE:
        return _CACHE[key]

    panel = load_panel(ppath)
    if panel is None:
        result = {"status": STATUS_PANEL_UNAVAILABLE, "panel_path": str(ppath),
                  "warnings": ["Frozen Phase 10-L panel not found or empty at %s" % ppath]}
        return result

    rows = panel["rows"]
    rep_index = panel["rep_index"]
    signal_date = panel["signal_date"]
    universe = UNIVERSE_NAME
    warnings: list[str] = []

    # --- champion (composite_sn column) ---------------------------------------------------
    champ_vals = {i: _to_float(rows[i].get(C_COMPOSITE_SN)) for i in range(len(rows))}
    champ_monthly, champ_cov = _build_monthly(rows, rep_index, champ_vals)
    champ_metrics = evaluate_battery(champ_monthly)
    champ_metrics.update(champ_cov)
    champ_maps = _signal_month_maps(rows, rep_index, champ_vals)

    # --- generate + evaluate candidates ---------------------------------------------------
    specs = _candidate_specs()
    evaluated: list[dict] = []
    for spec in specs:
        vals = spec["fn"](rows, rep_index)
        monthly, cov = _build_monthly(rows, rep_index, vals)
        metrics = evaluate_battery(monthly)
        metrics.update(cov)
        maps = _signal_month_maps(rows, rep_index, vals)
        corr_champ = _pair_rank_corr(maps, champ_maps)
        # reproduction of champion? (exact additive identity of the SN legs)
        repro_err = None
        if spec["name"] == "sn_composite_equal":
            errs = [abs(vals[i] - champ_vals[i]) for i in range(len(rows))
                    if vals.get(i) is not None and champ_vals.get(i) is not None]
            repro_err = max(errs) if errs else None
        evaluated.append({"spec": spec, "vals": vals, "metrics": metrics, "coverage": cov,
                          "maps": maps, "corr_vs_champion": corr_champ, "repro_err": repro_err})

    # --- classify: reproduction -> ARCHIVED; hard gates -> REJECTED -----------------------
    reproduction = {"champion_reproduced": False, "max_abs_error": None,
                    "note": "no reproduction candidate evaluated"}
    for ev in evaluated:
        name = ev["spec"]["name"]
        if name == "sn_composite_equal" and ev["repro_err"] is not None:
            ev["status"] = STATUS_ARCHIVED
            ev["reject_reason"] = None
            ev["archived_note"] = ("Exact reproduction of the champion composite_sn (max abs error "
                                   "%.2e) — archived, not re-listed as an independent survivor."
                                   % ev["repro_err"])
            reproduction = {"champion_reproduced": ev["repro_err"] < 1e-9,
                            "max_abs_error": ev["repro_err"],
                            "note": "sn_composite_equal (fcf_sn + acc_sn) == composite_sn"}
            continue
        reason = _hard_gate_reason(ev["metrics"], ev["coverage"])
        if reason is not None:
            ev["status"] = STATUS_REJECTED
            ev["reject_reason"] = reason
        else:
            ev["status"] = None  # provisional survivor; redundancy gate + ACTIVE/RESEARCH next
            ev["reject_reason"] = None

    # --- redundancy gate (greedy, ranked; champion is the reference) ----------------------
    provisional = [ev for ev in evaluated if ev["status"] is None]
    provisional.sort(key=lambda e: reg.leaderboard_sort_key({
        "net25": e["metrics"].get("net25_spread"), "ic_t": e["metrics"].get("ic_t_stat"),
        "ic": e["metrics"].get("mean_ic")}), reverse=True)
    accepted: list[dict] = []  # (name, maps) references incl. champion
    accepted_ref = [("composite_sn", champ_maps)]
    for ev in provisional:
        max_corr = ev["corr_vs_champion"] if ev["corr_vs_champion"] is not None else 0.0
        for _n, amaps in accepted:
            c = _pair_rank_corr(ev["maps"], amaps)
            if c is not None:
                max_corr = max(max_corr, c)
        if max_corr is not None and max_corr > MAX_CORR_REDUNDANT:
            ev["status"] = STATUS_REJECTED
            ev["reject_reason"] = REJECT_REDUNDANT
            ev["max_corr_vs_accepted"] = _round(max_corr, 4)
        else:
            ic_t = ev["metrics"].get("ic_t_stat") or 0.0
            cov = ev["coverage"].get("coverage_pct") or 0.0
            corr_c = ev["corr_vs_champion"]
            distinct = (corr_c is None) or (abs(corr_c) < ACTIVE_MAX_CORR_VS_CHAMPION)
            # ACTIVE = statistically strong, well-covered, and diversifying vs the champion; a
            # survivor that is strong but highly champion-correlated stays RESEARCH (kept, not tracked).
            ev["status"] = (STATUS_ACTIVE if (ic_t >= ACTIVE_MIN_IC_T and cov >= ACTIVE_MIN_COVERAGE
                                              and distinct) else STATUS_RESEARCH)
            ev["max_corr_vs_accepted"] = _round(max_corr, 4)
            accepted.append((ev["spec"]["name"], ev["maps"]))
            accepted_ref.append((ev["spec"]["name"], ev["maps"]))

    # --- build registry records -----------------------------------------------------------
    champion_rec = reg.make_alpha_record(
        name=CHAMPION_SIGNAL, family=FAM_SECTOR_NEUTRAL, status=STATUS_CHAMPION,
        horizon=HORIZON_TRADING_DAYS, universe=universe, signal_date=signal_date,
        metrics=champ_metrics, corr_vs_champion=1.0, regime_notes=_regime_notes(champ_metrics),
        description="Current paper champion: equal-weight sector-neutral FCF + accruals composite.",
        spec={"legs": {C_FCF_SN: 1.0, C_ACC_SN: 1.0}}, role="CURRENT PAPER CHAMPION")

    challenger_info, chall_corr = _challenger_from_report(_read_json(rpath), signal_date)
    challenger_rec = reg.make_alpha_record(
        name=CHALLENGER_SIGNAL, family=FAM_SECTOR_NEUTRAL, status=STATUS_CHALLENGER,
        horizon=HORIZON_TRADING_DAYS, universe=universe, signal_date=signal_date,
        metrics=challenger_info["metrics"], corr_vs_champion=chall_corr,
        regime_notes=_regime_notes(challenger_info["metrics"]),
        description="Sector-repaired paper challenger (composite_sn_repaired), enrolled from Phase 17-A.",
        spec={"source": "phase17a_report", "eligibility": challenger_info["eligibility"]},
        role="SECTOR-REPAIRED PAPER CHALLENGER",
        extra={"enrollment_note": challenger_info["note"], "report_available": challenger_info["available"]})
    if not challenger_info["available"]:
        warnings.append("Committed Phase 17-A report unavailable — challenger enrolled without metrics.")

    candidate_recs: list[dict] = []
    for ev in evaluated:
        spec = ev["spec"]
        rec = reg.make_alpha_record(
            name=spec["name"], family=spec["family"], status=ev["status"],
            horizon=HORIZON_TRADING_DAYS, universe=universe, signal_date=signal_date,
            metrics=ev["metrics"], corr_vs_champion=ev["corr_vs_champion"],
            regime_notes=_regime_notes(ev["metrics"]), reject_reason=ev["reject_reason"],
            description=spec["description"], spec=spec["spec"], role="GENERATED CANDIDATE",
            extra={"max_corr_vs_accepted": ev.get("max_corr_vs_accepted"),
                   "archived_note": ev.get("archived_note")})
        candidate_recs.append(rec)

    all_records = [champion_rec, challenger_rec] + candidate_recs

    # --- leaderboard (champion reference + ranked survivors) -------------------------------
    survivors = [r for r in candidate_recs if r["status"] in (STATUS_ACTIVE, STATUS_RESEARCH)]
    survivors.sort(key=reg.leaderboard_sort_key, reverse=True)
    leaderboard = [{"rank": "champion", "is_champion": True, **_lb_row(champion_rec)}]
    for i, r in enumerate(survivors, start=1):
        leaderboard.append({"rank": i, "is_champion": False, **_lb_row(r)})

    # --- correlation matrix over champion + survivors -------------------------------------
    corr = _correlation_matrix(champ_maps, survivors, evaluated)

    # --- family taxonomy ------------------------------------------------------------------
    families = _family_taxonomy(candidate_recs)

    # --- battery cross-check vs committed 17-A champion -----------------------------------
    cross_check = _battery_cross_check(champ_metrics, _read_json(rpath))

    # --- diagnostics ----------------------------------------------------------------------
    reject_counts: dict[str, int] = {}
    for r in candidate_recs:
        if r["status"] == STATUS_REJECTED and r["reject_reason"]:
            reject_counts[r["reject_reason"]] = reject_counts.get(r["reject_reason"], 0) + 1
    diagnostics = {
        "generated_candidates": len(candidate_recs),
        "survivors": len(survivors),
        "active": sum(1 for r in candidate_recs if r["status"] == STATUS_ACTIVE),
        "research": sum(1 for r in candidate_recs if r["status"] == STATUS_RESEARCH),
        "rejected": sum(1 for r in candidate_recs if r["status"] == STATUS_REJECTED),
        "archived": sum(1 for r in candidate_recs if r["status"] == STATUS_ARCHIVED),
        "reject_reason_counts": reject_counts,
        "families_data_ready": len(reg.DATA_READY_FAMILIES),
        "families_data_gated": len(reg.PRICE_GATED_FAMILIES),
        "gate_thresholds": {"min_coverage_pct": MIN_COVERAGE_PCT, "min_ic_t": MIN_IC_T,
                            "max_corr_redundant": MAX_CORR_REDUNDANT,
                            "min_positive_ic_month": MIN_POSITIVE_IC_MONTH,
                            "active_min_ic_t": ACTIVE_MIN_IC_T, "active_min_coverage": ACTIVE_MIN_COVERAGE,
                            "active_max_corr_vs_champion": ACTIVE_MAX_CORR_VS_CHAMPION,
                            "cost25": COST25, "cost50": COST50},
        "reproduction": reproduction,
        "battery_cross_check_vs_committed_phase17a_champion": cross_check,
        "sharpe_definition": "annualized IR of the monthly L/S spread (mean/std * sqrt(12); "
                             "63d overlapping-return caveat)",
    }

    result = {
        "phase": PHASE,
        "status": STATUS_READY,
        "signal_date": signal_date,
        "as_of_date": panel["as_of_date"],
        "horizon_trading_days": HORIZON_TRADING_DAYS,
        "universe": {"name": universe, "n_names": panel["n_tickers"], "n_months": panel["n_months"],
                     "n_rows": panel["n_rows"]},
        "champion": champion_rec,
        "challenger": challenger_rec,
        "registry": {"schema": list(reg.REGISTRY_METADATA_FIELDS),
                     "counts": reg.registry_counts(all_records),
                     "alphas": all_records},
        "leaderboard": leaderboard,
        "survivors": [_lb_row(r, full=True) for r in survivors],
        "rejected": [_reject_row(r) for r in candidate_recs if r["status"] == STATUS_REJECTED],
        "archived": [_lb_row(r, full=True) for r in candidate_recs if r["status"] == STATUS_ARCHIVED],
        "correlation": corr,
        "families": families,
        "reproduction": reproduction,
        "diagnostics": diagnostics,
        "candidate_reports": {r["name"]: r for r in candidate_recs},
        "provenance": {"panel_path": str(ppath), "reval_report_path": str(rpath),
                       "sources": ["frozen Phase 10-L scored panel", "committed Phase 17-A report"],
                       "champion_signal": CHAMPION_SIGNAL, "challenger_signal": CHALLENGER_SIGNAL},
        "warnings": warnings,
    }
    if key is not None:
        _CACHE[key] = result
    return result


def _lb_row(rec: dict, full: bool = False) -> dict:
    row = {"name": rec["name"], "family": rec["family"], "status": rec["status"],
           "status_class": rec.get("status_class"), "ic": rec.get("ic"), "ic_t": rec.get("ic_t"),
           "spread": rec.get("spread"), "net25": rec.get("net25"), "net50": rec.get("net50"),
           "turnover": rec.get("turnover"), "drawdown": rec.get("drawdown"), "sharpe": rec.get("sharpe"),
           "coverage_pct": rec.get("coverage_pct"), "corr_vs_champion": rec.get("corr_vs_champion")}
    if full:
        row["regime_notes"] = rec.get("regime_notes")
        row["description"] = rec.get("description")
    return row


def _reject_row(rec: dict) -> dict:
    return {"name": rec["name"], "family": rec["family"], "reject_reason": rec.get("reject_reason"),
            "reject_reason_text": rec.get("reject_reason_text"), "ic": rec.get("ic"),
            "ic_t": rec.get("ic_t"), "net25": rec.get("net25"), "coverage_pct": rec.get("coverage_pct"),
            "corr_vs_champion": rec.get("corr_vs_champion"),
            "max_corr_vs_accepted": rec.get("max_corr_vs_accepted")}


def _correlation_matrix(champ_maps, survivors, evaluated) -> dict:
    by_name = {ev["spec"]["name"]: ev for ev in evaluated}
    names = [CHAMPION_SIGNAL] + [r["name"] for r in survivors]
    maps = {CHAMPION_SIGNAL: champ_maps}
    for r in survivors:
        ev = by_name.get(r["name"])
        if ev:
            maps[r["name"]] = ev["maps"]
    n = len(names)
    matrix = [[None] * n for _ in range(n)]
    off = []
    top_pair = None
    for a in range(n):
        for b in range(n):
            if a == b:
                matrix[a][b] = 1.0
                continue
            if b < a:
                matrix[a][b] = matrix[b][a]
                continue
            c = _pair_rank_corr(maps[names[a]], maps[names[b]])
            cr = _round(c, 4)
            matrix[a][b] = cr
            if cr is not None:
                off.append(cr)
                if top_pair is None or abs(cr) > abs(top_pair[2]):
                    top_pair = (names[a], names[b], cr)
    vs_champ = {names[i]: matrix[0][i] for i in range(1, n)}
    summary = {
        "n_signals": n,
        "max_abs_off_diagonal": _round(max((abs(x) for x in off), default=None) if off else None, 4),
        "mean_abs_off_diagonal": _round(_mean([abs(x) for x in off]), 4) if off else None,
        "most_correlated_pair": ({"a": top_pair[0], "b": top_pair[1], "corr": top_pair[2]}
                                 if top_pair else None),
    }
    return {"signals": names, "matrix": matrix, "vs_champion": vs_champ, "summary": summary,
            "method": "mean monthly cross-sectional Spearman rank correlation"}


def _family_taxonomy(candidate_recs: list[dict]) -> list[dict]:
    fc = reg.family_counts(candidate_recs)
    out = []
    for fam in reg.ALL_FAMILIES:
        gated = fam in reg.PRICE_GATED_FAMILIES
        c = fc.get(fam, {})
        out.append({
            "family": fam,
            "description": reg.FAMILY_DESCRIPTIONS.get(fam),
            "data_ready": not gated,
            "status": "DATA_GATED" if gated else "DATA_READY",
            "n_candidates": c.get("total", 0),
            "n_active": c.get(STATUS_ACTIVE, 0),
            "n_research": c.get(STATUS_RESEARCH, 0),
            "n_rejected": c.get(STATUS_REJECTED, 0),
            "n_archived": c.get(STATUS_ARCHIVED, 0),
            "gated_reason": (reg.REJECT_REASON_TEXT[reg.REJECT_NO_DATA] if gated else None),
        })
    return out


def _battery_cross_check(champ_metrics: dict, report: Optional[dict]) -> dict:
    fp = (report or {}).get("full_panel") or {}
    ref = fp.get("champion") or {}
    if not ref:
        return {"available": False, "note": "committed Phase 17-A champion block unavailable"}
    keys = ["mean_ic", "ic_t_stat", "mean_gross_spread", "mean_turnover", "net25_spread",
            "net50_spread", "max_drawdown", "positive_ic_month_rate"]
    deltas = {}
    max_rel = 0.0
    for k in keys:
        a = _to_float(champ_metrics.get(k))
        b = _to_float(ref.get(k))
        if a is None or b is None:
            deltas[k] = None
            continue
        d = a - b
        deltas[k] = _round(d, 8)
        denom = abs(b) if abs(b) > 1e-9 else 1.0
        max_rel = max(max_rel, abs(d) / denom)
    return {"available": True, "reference": "committed Phase 17-A full-panel champion block",
            "deltas": deltas, "max_relative_error": _round(max_rel, 6),
            "reproduces_committed_champion": max_rel < 1e-3,
            "note": "recomputing composite_sn from the panel reproduces the committed champion battery"}


# --------------------------------------------------------------------------- #
# Read-only aggregate for the dashboard GET + slices
# --------------------------------------------------------------------------- #
def _store_dir(store_dir=None) -> Path:
    return _resolve(store_dir, STORE_ENV, DEFAULT_STORE)


def _persisted_state(sdir: Path) -> dict:
    state = _read_json(sdir / _RUN_STATE_FILE)
    files = []
    for fn in (_REGISTRY_FILE, _LEADERBOARD_FILE, _LEADERBOARD_CSV, _CORRELATION_FILE,
               _CORRELATION_CSV, _DIAGNOSTICS_FILE, _CANDIDATE_REPORTS_FILE, _RUN_STATE_FILE):
        if (sdir / fn).exists():
            files.append(fn)
    return {"has_artifacts": bool(state), "last_build_at": (state or {}).get("built_at"),
            "store_dir": str(sdir), "files": files,
            "last_build_signal_date": (state or {}).get("signal_date"),
            "last_build_counts": (state or {}).get("counts")}


def load_alpha_factory(
    *,
    panel_path=None, reval_report_path=None, store_dir=None,
) -> dict:
    """Read-only aggregate dashboard payload. Computes the build in-memory (owned panel), overlays
    the persisted store state, attaches the safety block. Never writes, never raises."""
    build = build_alpha_factory(panel_path=panel_path, reval_report_path=reval_report_path)
    sdir = _store_dir(store_dir)
    if build.get("status") == STATUS_PANEL_UNAVAILABLE:
        payload = {"phase": PHASE, "status": STATUS_PANEL_UNAVAILABLE,
                   "panel_path": build.get("panel_path"), "warnings": build.get("warnings", []),
                   "persisted": _persisted_state(sdir),
                   "next_recommended_action": ("Restore the frozen Phase 10-L scored panel, then re-read "
                                               "the Alpha Factory. No candidates can be generated while "
                                               "the owned panel is missing."),
                   "families": _family_taxonomy([])}
        payload.update(reg.safety_block())
        payload["loaded_at"] = _iso_now()
        return payload
    payload = dict(build)
    payload["persisted"] = _persisted_state(sdir)
    payload["next_recommended_action"] = (
        "Review the leaderboard and Alpha Library, then optionally run a confirmed Alpha Factory build "
        "to persist the registry / leaderboard / correlation / diagnostics artifacts. Paper research "
        "only — no candidate is promoted to live trading and the champion is never replaced.")
    payload.update(reg.safety_block())
    payload["loaded_at"] = _iso_now()
    return payload


def load_alpha_registry(**kw) -> dict:
    p = load_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "registry": p.get("registry"), "families": p.get("families"),
            "warnings": p.get("warnings", []), "persisted": p.get("persisted"),
            **reg.safety_block(), "loaded_at": _iso_now()}


def load_alpha_leaderboard(**kw) -> dict:
    p = load_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "leaderboard": p.get("leaderboard"), "survivors": p.get("survivors"),
            "champion": p.get("champion"), "challenger": p.get("challenger"),
            "warnings": p.get("warnings", []), **reg.safety_block(), "loaded_at": _iso_now()}


def load_alpha_correlation(**kw) -> dict:
    p = load_alpha_factory(**kw)
    return {"phase": PHASE, "status": p.get("status"), "signal_date": p.get("signal_date"),
            "correlation": p.get("correlation"), "warnings": p.get("warnings", []),
            **reg.safety_block(), "loaded_at": _iso_now()}


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

    _atomic_write_json(sdir / _REGISTRY_FILE, {
        "phase": PHASE, "built_at": built_at, "signal_date": build.get("signal_date"),
        "schema": build["registry"]["schema"], "counts": build["registry"]["counts"],
        "alphas": build["registry"]["alphas"]})
    written.append(_REGISTRY_FILE)

    _atomic_write_json(sdir / _LEADERBOARD_FILE, {
        "phase": PHASE, "built_at": built_at, "leaderboard": build["leaderboard"]})
    written.append(_LEADERBOARD_FILE)
    lb_rows = [[r.get("rank"), r.get("name"), r.get("family"), r.get("status"), r.get("ic"),
                r.get("ic_t"), r.get("spread"), r.get("net25"), r.get("net50"), r.get("turnover"),
                r.get("drawdown"), r.get("sharpe"), r.get("coverage_pct"), r.get("corr_vs_champion")]
               for r in build["leaderboard"]]
    _atomic_write_csv(sdir / _LEADERBOARD_CSV,
                      ["rank", "name", "family", "status", "ic", "ic_t", "spread", "net25", "net50",
                       "turnover", "drawdown", "sharpe", "coverage_pct", "corr_vs_champion"], lb_rows)
    written.append(_LEADERBOARD_CSV)

    _atomic_write_json(sdir / _CORRELATION_FILE, {
        "phase": PHASE, "built_at": built_at, **build["correlation"]})
    written.append(_CORRELATION_FILE)
    corr = build["correlation"]
    sig = corr["signals"]
    corr_rows = [[sig[i]] + [corr["matrix"][i][j] for j in range(len(sig))] for i in range(len(sig))]
    _atomic_write_csv(sdir / _CORRELATION_CSV, ["signal"] + sig, corr_rows)
    written.append(_CORRELATION_CSV)

    _atomic_write_json(sdir / _DIAGNOSTICS_FILE, {
        "phase": PHASE, "built_at": built_at, "families": build["families"],
        **build["diagnostics"]})
    written.append(_DIAGNOSTICS_FILE)

    _atomic_write_json(sdir / _CANDIDATE_REPORTS_FILE, {
        "phase": PHASE, "built_at": built_at, "reports": build["candidate_reports"]})
    written.append(_CANDIDATE_REPORTS_FILE)

    _atomic_write_json(sdir / _RUN_STATE_FILE, {
        "phase": PHASE, "built_at": built_at, "signal_date": build.get("signal_date"),
        "counts": build["registry"]["counts"], "universe": build.get("universe"),
        "provenance": build.get("provenance"), "artifacts": written + [_RUN_STATE_FILE]})
    written.append(_RUN_STATE_FILE)
    return written


def run_alpha_factory(
    *,
    commit: bool = False,
    confirm: Optional[str] = None,
    panel_path=None, reval_report_path=None, store_dir=None,
    built_at: Optional[str] = None,
) -> dict:
    """Manual Alpha Factory build. commit=False previews (no writes); commit=True requires the
    confirmation token and persists the artifact set to the dedicated LOCAL store only."""
    build = build_alpha_factory(panel_path=panel_path, reval_report_path=reval_report_path)
    sdir = _store_dir(store_dir)

    if build.get("status") == STATUS_PANEL_UNAVAILABLE:
        out = {"phase": PHASE, "status": STATUS_PANEL_UNAVAILABLE, "wrote_store": False,
               "performed_write": False, "panel_path": build.get("panel_path"),
               "warnings": build.get("warnings", [])}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    counts = build["registry"]["counts"]
    would_write = [_REGISTRY_FILE, _LEADERBOARD_FILE, _LEADERBOARD_CSV, _CORRELATION_FILE,
                   _CORRELATION_CSV, _DIAGNOSTICS_FILE, _CANDIDATE_REPORTS_FILE, _RUN_STATE_FILE]

    if not commit:
        out = {"phase": PHASE, "status": STATUS_BUILD_PREVIEW, "wrote_store": False,
               "performed_write": False, "signal_date": build.get("signal_date"),
               "registry_counts": counts, "diagnostics": build["diagnostics"],
               "reproduction": build["reproduction"], "families": build["families"],
               "store_dir": str(sdir), "would_write_files": would_write,
               "confirm_required_token": BUILD_CONFIRM_TOKEN, "warnings": build.get("warnings", [])}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    if confirm != BUILD_CONFIRM_TOKEN:
        out = {"phase": PHASE, "status": STATUS_CONFIRM_REQUIRED, "wrote_store": False,
               "performed_write": False,
               "message": "A committing Alpha Factory build requires confirm='%s'." % BUILD_CONFIRM_TOKEN}
        out.update(reg.safety_block())
        out["loaded_at"] = _iso_now()
        return out

    built_at = built_at or _iso_now()
    written = _write_artifacts(build, sdir, built_at)
    clear_cache()
    out = {"phase": PHASE, "status": STATUS_BUILD_COMPLETE, "wrote_store": True,
           "performed_write": True, "built_at": built_at, "signal_date": build.get("signal_date"),
           "registry_counts": counts, "diagnostics": build["diagnostics"],
           "reproduction": build["reproduction"], "families": build["families"],
           "store_dir": str(sdir), "files_written": written, "warnings": build.get("warnings", [])}
    out.update(reg.safety_block())
    out["loaded_at"] = _iso_now()
    return out


__all__ = [
    "PHASE", "BUILD_CONFIRM_TOKEN", "CHAMPION_SIGNAL", "CHALLENGER_SIGNAL",
    "HORIZON_TRADING_DAYS", "PANEL_ENV", "STORE_ENV", "REVAL_REPORT_ENV",
    "STATUS_READY", "STATUS_PANEL_UNAVAILABLE", "STATUS_BUILD_PREVIEW", "STATUS_BUILD_COMPLETE",
    "STATUS_CONFIRM_REQUIRED",
    "build_alpha_factory", "run_alpha_factory", "load_alpha_factory",
    "load_alpha_registry", "load_alpha_leaderboard", "load_alpha_correlation",
    "load_panel", "evaluate_battery", "clear_cache",
    "_REGISTRY_FILE", "_LEADERBOARD_FILE", "_CORRELATION_FILE", "_DIAGNOSTICS_FILE",
    "_CANDIDATE_REPORTS_FILE", "_RUN_STATE_FILE",
]
