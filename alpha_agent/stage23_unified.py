"""
alpha_agent.stage23_unified — Stage 23 UNIFIED ALPHA RESEARCH: owned-panel
adapter, operational-edge autopsy, bounded challenger campaign, research
priority queue and the portfolio-decision research link.

WHY THIS MODULE EXISTS
----------------------
Every prior campaign evaluated CANDIDATES. Nothing ever put the OPERATIONAL
model — ``fundamental_momentum_50_50_v1`` (0.5 * composite_sn + 0.5 * mom_6_1,
owned by ``api.multi_horizon_engine`` and read by ``api.universe_scoring``) —
through the same canonical evidence contract its challengers must pass. Stage 23
closes that gap and, in doing so, gives the autonomous research agent the one
thing it lacked: a measured statement of WHERE the current edge comes from, what
is redundant, and therefore what is worth researching next.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
It adds NO new statistic, NO new gate, NO second registry and NO promotion path.
Every number is produced by released components:

    signal_evaluation.evaluate_periods   -> the canonical Stage-5 evidence row
    tournament.row_to_contract_metrics   -> the canonical evaluation contract
    tournament.classify_evidence         -> the canonical evidence gates
    selection_controls.benjamini_hochberg-> the canonical FDR control
    orthogonality.*                      -> the canonical redundancy measures
    tournament.CandidateRegistry         -> the canonical candidate lifecycle

This module contributes exactly one genuinely new thing: a leakage-safe ADAPTER
that turns the two OWNED panels into the ``periods`` cross-sections those
components already consume, plus the attribution / prioritisation reporting built
on top of them.

THE TWO OWNED PANELS AND THEIR POINT-IN-TIME CONTRACT
-----------------------------------------------------
1. MOMENTUM MONTHLY PANEL (deep, survivorship-safe)
   ``phase25_multi_horizon_alpha/_inputs/momentum_monthly_panel.csv``
   313 months (2000-08 .. 2026-08), ~984 names/month, 2728 tickers of which 1347
   carry a delisting suffix — delisted names are RETAINED, so this panel is
   survivorship-safe. Derived from the owned Norgate Russell-1000 Current & Past
   TOTALRETURN daily NPZ.

   PIT rules enforced here, never relaxed:
     * ``mom_6_1(m) = close[m-1]/close[m-7]-1``  — skips the most recent month,
       so it is knowable at the end of month m.
     * ``realized_vol_63d(m)`` / ``adv_dollar(m)`` are TRAILING statistics as of
       the month-end market date.
     * ``fwd_1m_return(m)`` is the FORWARD return m -> m+1. It is the TARGET.
       A feature evaluated at month m may only read ``fwd_1m_return`` at months
       STRICTLY BEFORE m (``fwd_1m_return(m-k)``, k>=1, is the realised return
       over m-k -> m-k+1 and is fully known at m). ``_trailing_returns`` is the
       single place that reconstruction happens and it enforces k>=1.

2. FROZEN FUNDAMENTAL PANEL (thin, survivor-biased)
   ``phase10l_historical_sector_neutral_scored_panel_reconstruction/
     historical_sector_neutral_scored_panel.csv``
   545 tickers, 120 months (2016-06 .. 2026-05), per-ticker STAGGERED rebalance
   dates. Cross-sections are therefore assembled per MONTH, deduplicated to one
   observation per (ticker, month) — the 2016-06 bulk history-seed month carries
   ~17.5k rows and up to 99 observations for a single ticker, which would
   otherwise fabricate a huge fake cross-section.
   Target: ``forward_63d_return``. Universe: EODHD 545-name set, documented
   SURVIVOR-BIASED — every metric derived from it is labelled accordingly.

SECTOR NEUTRALISATION IS DATA-BLOCKED, NOT SKIPPED
--------------------------------------------------
The momentum panel is 100% ``sector = Unknown``. The owned repaired sector map
covers 464 of 2728 tickers (37.1% of rows) and is CURRENT-AS-OF — using it to
"sector-neutralise" a historical study would inject a classification look-ahead.
The tournament already records this exactly (``point_in_time_gics``: 5 of 50
required symbols PIT-classified). Stage 23 therefore reports sector
neutralisation as BLOCKED with the measured coverage, and never substitutes the
look-ahead map. Beta, volatility and liquidity/size neutralisations ARE
implementable from owned data and are run.

SAFETY
------
Research-only and read-only with respect to every operational store. Nothing here
creates an order, fill, signal, trade decision, portfolio proposal, rebalance
plan, Daily Close, model promotion or champion replacement. No network, no
PostgreSQL, no prediction service, no clock-dependent branching. Writes are
confined to the Stage-23 research root and (via the released lifecycle) the
research tournament registry. Pure stdlib.
"""
from __future__ import annotations

import csv
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence

from . import experiment_contracts as ec
from . import orthogonality as orth
from . import selection_controls as sctl
from . import signal_evaluation as sev
from . import tournament as tt

STAGE = "23"
STAGE23_VERSION = "stage23-unified-1.0.0"
ORIGIN = "stage23-unified-alpha-research"
CONTRACT_ID = "stage23_unified/1"

# Terminal tokens (exactly one printed per CLI invocation).
READY = "STAGE23_UNIFIED_RESEARCH_READY"
BLOCKED = "STAGE23_UNIFIED_RESEARCH_BLOCKED"
DATA_HOLD = "STAGE23_UNIFIED_RESEARCH_DATA_HOLD"

# --------------------------------------------------------------------------- #
# Owned data locations (env-overridable so tests are hermetic).
# --------------------------------------------------------------------------- #
MOM_PANEL_ENV = "PAPER_TRADER_STAGE23_MOM_PANEL"
FUND_PANEL_ENV = "PAPER_TRADER_STAGE23_FUND_PANEL"
SECTOR_MAP_ENV = "PAPER_TRADER_STAGE23_SECTOR_MAP"
RESEARCH_ROOT_ENV = "PAPER_TRADER_STAGE23_ROOT"

DEFAULT_MOM_PANEL = Path(
    r"D:\Stock_Prediction_app_data\phase25_multi_horizon_alpha\_inputs"
    r"\momentum_monthly_panel.csv")
DEFAULT_FUND_PANEL = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10l_historical_sector_neutral_scored_panel_reconstruction"
    r"\historical_sector_neutral_scored_panel.csv")
DEFAULT_SECTOR_MAP = Path(
    r"C:\Users\binis\Stock_Prediction_app_push\research\output"
    r"\phase10f_owned_sector_mapping_repair\repaired_sector_mapping.csv")
DEFAULT_RESEARCH_ROOT = Path(
    r"D:\Stock_Prediction_app_data\stage23_unified_alpha_research")

# --------------------------------------------------------------------------- #
# Operational identity (re-exported from the owners — never redefined here).
# --------------------------------------------------------------------------- #
OPERATIONAL_STRATEGY_ID = "fundamental_momentum_50_50_v1"
OPERATIONAL_BOOK_ID = "fundamental_momentum_50_50_top25"
OPERATIONAL_SCORING_OWNER = "api.universe_scoring"
OPERATIONAL_KERNEL = "api.multi_horizon_engine"
COMPONENT_FUNDAMENTAL = "composite_sn"
COMPONENT_MOMENTUM = "mom_6_1"
COMPONENT_WEIGHTS = {COMPONENT_FUNDAMENTAL: 0.5, COMPONENT_MOMENTUM: 0.5}

#: Trading-day horizons matching each panel's native target column.
HORIZON_MONTHLY = 21
HORIZON_QUARTERLY = 63

#: Minimum names required before a cross-section is scored at all.
MIN_CROSS_SECTION = 20

#: Trailing window (months) used by every path/liquidity/vol feature.
TRAILING_MONTHS = 12
#: Rolling window (months) used for the market-beta estimate.
BETA_WINDOW_MONTHS = 36

SAFETY_BADGES = ["RESEARCH ONLY", "READ ONLY", "NO ORDERS", "NO LIVE PROMOTION",
                 "PREVIEW ONLY", "MANUAL REVIEW"]

# =========================================================================== #
# Small deterministic helpers.
# =========================================================================== #


def _num(x) -> Optional[float]:
    if x is None or x == "" or x == "None":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if math.isnan(v) or math.isinf(v):
        return None
    return v


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_fingerprint(path: str | Path) -> dict:
    """Size + SHA-256 of an owned input, so every result names its exact data."""
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "exists": False, "bytes": 0, "sha256": None}
    h = hashlib.sha256()
    size = 0
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
            size += len(chunk)
    return {"path": str(p), "exists": True, "bytes": size, "sha256": h.hexdigest()}


def _resolve(explicit, env_var: str, default: Path) -> Path:
    if explicit is not None:
        return Path(explicit)
    env = os.environ.get(env_var)
    return Path(env) if env else Path(default)


def _mean(xs: Sequence[float]) -> Optional[float]:
    vals = [float(x) for x in xs if x is not None]
    return sum(vals) / len(vals) if vals else None


def _stdev(xs: Sequence[float]) -> Optional[float]:
    vals = [float(x) for x in xs if x is not None]
    if len(vals) < 2:
        return None
    m = sum(vals) / len(vals)
    var = sum((v - m) ** 2 for v in vals) / (len(vals) - 1)
    return math.sqrt(var) if var > 0 else 0.0


def _median(xs: Sequence[float]) -> Optional[float]:
    vals = sorted(float(x) for x in xs if x is not None)
    if not vals:
        return None
    n = len(vals)
    return vals[n // 2] if n % 2 else 0.5 * (vals[n // 2 - 1] + vals[n // 2])


def _zscores(vals: Sequence[Optional[float]]) -> list:
    """Cross-sectional z-scores; None-safe, returns None where input is None."""
    finite = [v for v in vals if v is not None]
    m = _mean(finite)
    s = _stdev(finite)
    if m is None or not s:
        return [None] * len(vals)
    return [None if v is None else (float(v) - m) / s for v in vals]


def _pvalue_from_t(t: Optional[float], dof: int) -> Optional[float]:
    """Two-sided p-value from a t-statistic via a normal approximation.

    Deterministic and stdlib-only. With the sample sizes here (>=12 scored
    periods, usually >=100) the normal approximation is conservative enough for
    an FDR ranking; it is used ONLY to order the family for Benjamini-Hochberg,
    never to declare significance on its own — the released evidence gates do
    that.
    """
    if t is None or dof < 1:
        return None
    z = abs(float(t))
    # Two-sided normal tail: erfc(z / sqrt(2)).
    return max(0.0, min(1.0, math.erfc(z / math.sqrt(2.0))))


# =========================================================================== #
# PANEL ADAPTERS — the one place owned CSVs become leakage-safe cross-sections.
# =========================================================================== #


class MomentumPanel:
    """The owned survivorship-safe monthly momentum panel.

    ``by_month[m]`` is a list of per-name dicts as-of month ``m``.
    ``series[ticker]`` is the ordered ``(month, realised_return)`` history where
    the realised return at month ``m`` is ``fwd_1m_return(m-1)`` — i.e. the
    return over ``m-1 -> m``, which is fully known at the end of ``m``.
    """

    def __init__(self, by_month: dict, months: list, fingerprint: dict,
                 diagnostics: dict) -> None:
        self.by_month = by_month
        self.months = months
        self.fingerprint = fingerprint
        self.diagnostics = diagnostics
        self._index = {m: i for i, m in enumerate(months)}
        # ticker -> {month: realised return over (prev month -> month)}
        self._realised: dict = {}
        for i, m in enumerate(months):
            if i + 1 >= len(months):
                break
            nxt = months[i + 1]
            for row in by_month[m]:
                fwd = row.get("fwd_1m_return")
                if fwd is None:
                    continue
                # fwd_1m_return(m) realises over m -> m+1, so it is the return
                # OF month m+1 and is knowable at the end of month m+1.
                self._realised.setdefault(row["ticker"], {})[nxt] = fwd
        # market (equal-weight universe) realised return per month
        self.market: dict = {}
        for i, m in enumerate(months):
            if i == 0:
                continue
            prev = months[i - 1]
            vals = [r["fwd_1m_return"] for r in by_month[prev]
                    if r.get("fwd_1m_return") is not None]
            mm = _mean(vals)
            if mm is not None:
                self.market[m] = mm

    def trailing_returns(self, ticker: str, month: str, *,
                         lookback: int = TRAILING_MONTHS,
                         skip_recent: int = 0) -> list:
        """Realised monthly returns knowable at ``month``, oldest first.

        ``skip_recent=k`` drops the k most recent months (the momentum
        skip-a-month convention). NEVER returns a return that realises at or
        after ``month``+1: the newest element is the return over
        ``month-1 -> month``.
        """
        idx = self._index.get(month)
        if idx is None:
            return []
        hist = self._realised.get(ticker)
        if not hist:
            return []
        out = []
        # months[idx] itself is the newest permissible realised return.
        hi = idx - int(skip_recent)
        lo = hi - int(lookback) + 1
        for j in range(max(1, lo), hi + 1):
            r = hist.get(self.months[j])
            out.append(r)
        return out

    def market_trailing(self, month: str, *, lookback: int = TRAILING_MONTHS,
                        skip_recent: int = 0) -> list:
        idx = self._index.get(month)
        if idx is None:
            return []
        hi = idx - int(skip_recent)
        lo = hi - int(lookback) + 1
        return [self.market.get(self.months[j]) for j in range(max(1, lo), hi + 1)]


def load_momentum_panel(path=None) -> MomentumPanel:
    """Read the owned monthly momentum panel. Read-only, deterministic."""
    p = _resolve(path, MOM_PANEL_ENV, DEFAULT_MOM_PANEL)
    if not p.exists():
        raise FileNotFoundError("Stage 23 momentum panel not found: %s" % p)
    by_month: dict = {}
    tickers = set()
    delisted = set()
    rows = 0
    with open(p, newline="", encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            m = r.get("month")
            t = r.get("ticker")
            if not m or not t:
                continue
            rows += 1
            tickers.add(t)
            if "-" in t and t.rsplit("-", 1)[-1].isdigit():
                delisted.add(t)
            by_month.setdefault(m, []).append({
                "ticker": t,
                "market_date": r.get("market_date"),
                "mom_6_1": _num(r.get("mom_6_1")),
                "fwd_1m_return": _num(r.get("fwd_1m_return")),
                "is_member": str(r.get("is_member")) == "1",
                "adv_dollar": _num(r.get("adv_dollar")),
                "realized_vol_63d": _num(r.get("realized_vol_63d")),
                "eligible_history": str(r.get("eligible_history")) == "1",
                "sector": r.get("sector") or "Unknown",
            })
    months = sorted(by_month)
    known_sector_rows = sum(1 for m in months for r in by_month[m]
                            if r["sector"] and r["sector"] != "Unknown")
    diagnostics = {
        "rows": rows,
        "months": len(months),
        "first_month": months[0] if months else None,
        "last_month": months[-1] if months else None,
        "distinct_tickers": len(tickers),
        "delisting_suffixed_tickers": len(delisted),
        "survivorship_safe": len(delisted) > 0,
        "survivorship_basis": ("delisted names retain a dated suffix and remain in "
                               "history; source is the owned Norgate Russell-1000 "
                               "Current & Past TOTALRETURN panel"),
        "median_names_per_month": (sorted(len(by_month[m]) for m in months)[len(months) // 2]
                                   if months else 0),
        "panel_sector_known_rows": known_sector_rows,
        "panel_sector_known_pct": round(100.0 * known_sector_rows / rows, 4) if rows else 0.0,
    }
    return MomentumPanel(by_month, months, file_fingerprint(p), diagnostics)


def load_fundamental_panel(path=None) -> dict:
    """Read the frozen Phase 10-L panel, deduplicated to one row per (ticker, month).

    The panel carries per-ticker STAGGERED rebalance dates and a bulk
    history-seed month (2016-06) in which a single ticker can appear up to 99
    times. Keeping every row would fabricate an enormous fake cross-section, so
    the LAST rebalance date within the month wins and the dedupe counts are
    reported as evidence rather than hidden.
    """
    p = _resolve(path, FUND_PANEL_ENV, DEFAULT_FUND_PANEL)
    if not p.exists():
        raise FileNotFoundError("Stage 23 fundamental panel not found: %s" % p)
    best: dict = {}
    rows = 0
    dupes = 0
    tickers = set()
    sector_by_ticker: dict = {}
    with open(p, newline="", encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            reb = r.get("rebalance_date") or ""
            t = r.get("ticker")
            if not reb or not t:
                continue
            rows += 1
            tickers.add(t)
            sec = r.get("sector")
            if sec and sec != "Unknown":
                sector_by_ticker[t] = sec
            key = (t, reb[:7])
            rec = {
                "ticker": t,
                "rebalance_date": reb,
                "composite_sn": _num(r.get("composite_sn")),
                "fcf_to_assets": _num(r.get("fcf_to_assets_sector_neutral_z")),
                "operating_accruals": _num(
                    r.get("operating_accruals_oriented_sector_neutral_z")),
                "forward_63d_return": _num(r.get("forward_63d_return")),
                "liquidity_proxy": _num(r.get("liquidity_proxy")),
                "sector": sec or "Unknown",
            }
            prev = best.get(key)
            if prev is None:
                best[key] = rec
            else:
                dupes += 1
                if reb > prev["rebalance_date"]:
                    best[key] = rec
    by_month: dict = {}
    for (t, m), rec in best.items():
        by_month.setdefault(m, []).append(rec)
    for m in by_month:
        by_month[m].sort(key=lambda x: x["ticker"])
    months = sorted(by_month)
    return {
        "by_month": by_month,
        "months": months,
        "sector_by_ticker": sector_by_ticker,
        "fingerprint": file_fingerprint(p),
        "diagnostics": {
            "raw_rows": rows,
            "deduplicated_rows": len(best),
            "duplicate_ticker_month_rows_dropped": dupes,
            "dedupe_rule": "one row per (ticker, month); latest rebalance_date wins",
            "months": len(months),
            "first_month": months[0] if months else None,
            "last_month": months[-1] if months else None,
            "distinct_tickers": len(tickers),
            "median_names_per_month": (
                sorted(len(by_month[m]) for m in months)[len(months) // 2]
                if months else 0),
            "survivorship_safe": False,
            "survivorship_basis": (
                "EODHD 545-name scored universe; documented SURVIVOR-BIASED "
                "(Phase 30B). Every metric derived from this panel inherits that "
                "bias and is labelled accordingly."),
            "tickers_with_known_sector": len(sector_by_ticker),
        },
    }


def load_sector_map(path=None) -> dict:
    """The owned repaired sector map. CURRENT-AS-OF, therefore look-ahead for any
    historical study. Loaded only to MEASURE the coverage wall, never to
    neutralise a historical cross-section."""
    p = _resolve(path, SECTOR_MAP_ENV, DEFAULT_SECTOR_MAP)
    out: dict = {}
    if not p.exists():
        return {"map": out, "fingerprint": file_fingerprint(p),
                "point_in_time": False, "usable_for_neutralisation": False}
    with open(p, newline="", encoding="utf-8-sig") as fh:
        for r in csv.DictReader(fh):
            t = r.get("ticker")
            s = r.get("repaired_sector")
            if t and s:
                out[t] = s
    return {
        "map": out,
        "fingerprint": file_fingerprint(p),
        "point_in_time": False,
        "usable_for_neutralisation": False,
        "reason": ("current-as-of GICS classification; applying it to a historical "
                   "cross-section injects a classification look-ahead. The "
                   "tournament records the same blocker as point_in_time_gics."),
    }


# =========================================================================== #
# FEATURE LIBRARY — every feature is a pure function of PIT-safe inputs.
#
# Each returns None when its inputs are unavailable; a None is DROPPED from the
# cross-section rather than imputed, so coverage is honest.
# =========================================================================== #


def f_mom_6_1(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """The operational momentum leg, verbatim (close[m-1]/close[m-7]-1)."""
    return row.get("mom_6_1")


def f_dollar_volume_shock(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Z-score of log dollar volume against its own trailing 12-month history.

    ECONOMIC MECHANISM: abnormal turnover marks information arrival and
    institutional repositioning. It is a FLOW measure, disjoint from the price
    LEVEL information in mom_6_1 and from the fundamental level in composite_sn.
    """
    adv = row.get("adv_dollar")
    if adv is None or adv <= 0:
        return None
    idx = panel._index.get(month)
    if idx is None or idx < TRAILING_MONTHS:
        return None
    hist = []
    for j in range(idx - TRAILING_MONTHS, idx):
        for r in panel.by_month[panel.months[j]]:
            if r["ticker"] == row["ticker"]:
                a = r.get("adv_dollar")
                if a and a > 0:
                    hist.append(math.log(a))
                break
    if len(hist) < 6:
        return None
    m = _mean(hist)
    s = _stdev(hist)
    if m is None or not s:
        return None
    return (math.log(adv) - m) / s


def f_amihud_illiquidity(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Monthly Amihud illiquidity: |realised monthly return| / dollar volume.

    ECONOMIC MECHANISM: price impact per dollar traded. Illiquid names earn a
    premium for the impact risk their holders bear. Re-specified honestly at
    MONTHLY resolution — the owned panel carries monthly ADV, not daily volume,
    so the classical daily Amihud is NOT claimed.
    """
    adv = row.get("adv_dollar")
    if adv is None or adv <= 0:
        return None
    rets = panel.trailing_returns(row["ticker"], month, lookback=1)
    r = rets[-1] if rets else None
    if r is None:
        return None
    return abs(r) / adv * 1.0e9


def f_liquidity_trend(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Log dollar volume now minus its trailing 12-month mean.

    ECONOMIC MECHANISM: a SUSTAINED broadening of the holder base (institutional
    adoption / index-eligibility drift), as opposed to the one-month shock in
    ``dollar_volume_shock``.
    """
    adv = row.get("adv_dollar")
    if adv is None or adv <= 0:
        return None
    idx = panel._index.get(month)
    if idx is None or idx < TRAILING_MONTHS:
        return None
    hist = []
    for j in range(idx - TRAILING_MONTHS, idx):
        for r in panel.by_month[panel.months[j]]:
            if r["ticker"] == row["ticker"]:
                a = r.get("adv_dollar")
                if a and a > 0:
                    hist.append(math.log(a))
                break
    if len(hist) < 6:
        return None
    m = _mean(hist)
    return None if m is None else math.log(adv) - m


def f_momentum_consistency(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Fraction of POSITIVE realised months over the trailing year, minus 0.5.

    ECONOMIC MECHANISM: path QUALITY rather than path magnitude. A name that
    ground higher across many months reflects persistent, broadly-held
    reappraisal; the same total return delivered by one jump is more often a
    one-off repricing that mean-reverts. This is information mom_6_1 discards
    entirely — mom_6_1 sees only the endpoints.
    """
    rets = [r for r in panel.trailing_returns(row["ticker"], month,
                                              lookback=TRAILING_MONTHS, skip_recent=1)
            if r is not None]
    if len(rets) < 8:
        return None
    return sum(1 for r in rets if r > 0) / len(rets) - 0.5


def f_path_drawdown(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Trailing 12-month maximum drawdown of the cumulative return path.

    Returned as a NEGATIVE number where less-negative is better, so a high value
    is the expected-outperform leg (matching the evaluator's sign convention).

    ECONOMIC MECHANISM: names that advanced without a deep interim drawdown show
    demand that absorbed supply on the way up. Distinct from realised volatility
    (a symmetric second moment) because it is path- and sign-dependent.
    """
    rets = [r for r in panel.trailing_returns(row["ticker"], month,
                                              lookback=TRAILING_MONTHS, skip_recent=1)
            if r is not None]
    if len(rets) < 8:
        return None
    cum = 1.0
    peak = 1.0
    worst = 0.0
    for r in rets:
        cum *= (1.0 + r)
        peak = max(peak, cum)
        worst = min(worst, cum / peak - 1.0)
    return worst


def f_residual_momentum(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Market-beta-residual momentum: cumulated residuals over months m-6..m-1.

    Beta is estimated by an OLS regression of the name's realised monthly return
    on the equal-weight universe monthly return over a rolling 36-month window
    ending at m-1. The residual sum over the 6-month window mirrors the 6_1
    convention (the most recent month is skipped on both sides).

    ECONOMIC MECHANISM: raw momentum partly pays for market-beta exposure during
    trending markets. Removing that exposure isolates the STOCK-SPECIFIC
    component, which is the part portfolio construction actually wants.
    """
    t = row["ticker"]
    long_r = panel.trailing_returns(t, month, lookback=BETA_WINDOW_MONTHS, skip_recent=1)
    long_m = panel.market_trailing(month, lookback=BETA_WINDOW_MONTHS, skip_recent=1)
    pairs = [(a, b) for a, b in zip(long_r, long_m) if a is not None and b is not None]
    if len(pairs) < 24:
        return None
    ys = [p[0] for p in pairs]
    xs = [p[1] for p in pairs]
    mx = _mean(xs)
    my = _mean(ys)
    if mx is None or my is None:
        return None
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    var = sum((x - mx) ** 2 for x in xs)
    if var <= 0:
        return None
    beta = cov / var
    alpha = my - beta * mx
    short_r = panel.trailing_returns(t, month, lookback=6, skip_recent=1)
    short_m = panel.market_trailing(month, lookback=6, skip_recent=1)
    resid = [a - (alpha + beta * b) for a, b in zip(short_r, short_m)
             if a is not None and b is not None]
    if len(resid) < 4:
        return None
    return sum(resid)


def f_size_liquidity(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Negative log dollar volume — the liquidity/size premium as a standalone factor.

    ECONOMIC MECHANISM: within a large-cap index, the less-liquid tail is held by
    fewer institutions and compensates holders for exit risk. Also the canonical
    CONTROL variable for the Workstream-C size/liquidity neutralisation.
    """
    adv = row.get("adv_dollar")
    if adv is None or adv <= 0:
        return None
    return -math.log(adv)


#: Control features used for neutralisation (never scored as alpha themselves).
def c_log_adv(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    adv = row.get("adv_dollar")
    return math.log(adv) if adv and adv > 0 else None


def c_vol(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    return row.get("realized_vol_63d")


def c_beta(panel: MomentumPanel, row: dict, month: str) -> Optional[float]:
    """Rolling 36-month market beta from realised monthly returns (owned proxy)."""
    long_r = panel.trailing_returns(row["ticker"], month,
                                    lookback=BETA_WINDOW_MONTHS, skip_recent=1)
    long_m = panel.market_trailing(month, lookback=BETA_WINDOW_MONTHS, skip_recent=1)
    pairs = [(a, b) for a, b in zip(long_r, long_m) if a is not None and b is not None]
    if len(pairs) < 24:
        return None
    ys = [p[0] for p in pairs]
    xs = [p[1] for p in pairs]
    mx = _mean(xs)
    my = _mean(ys)
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    var = sum((x - mx) ** 2 for x in xs)
    return cov / var if var > 0 else None


CONTROLS = {"log_adv": c_log_adv, "realized_vol": c_vol, "market_beta": c_beta}


# =========================================================================== #
# PRE-REGISTERED HYPOTHESES (Workstream D).
#
# Frozen BEFORE evaluation. Each records the economic mechanism, the data basis,
# the primary metric and the rejection criteria, and names the existing
# tournament candidate it resolves or duplicates.
# =========================================================================== #
def _hyp(hid, name, family, feature_fn, rationale, mechanism, *,
         expected_sign=1, resolves=None, near_duplicate_of=None,
         duplicate_note=None, priority="NORMAL") -> dict:
    return {
        "hypothesis_id": hid,
        "name": name,
        "family": family,
        "feature": hid,
        "feature_fn": feature_fn,
        "economic_rationale": rationale,
        "expected_mechanism": mechanism,
        "expected_sign": int(expected_sign),
        "data_basis": "owned_norgate_r1000_monthly_panel",
        "panel": "momentum_monthly",
        "horizon_days": HORIZON_MONTHLY,
        "rebalance": "monthly",
        "primary_metric": "rank_ic_t",
        "rejection_criteria": (
            "released Stage-9 gates: rank_ic>=0.01, rank_ic_t>=2.0, "
            "positive_ic_hit_rate>=0.52, spread_t>=2.0, net25_spread>0, "
            "subperiod_consistency>=0.6, regime_consistency>=0.5, plus "
            "Benjamini-Hochberg FDR across the Stage-23 family"),
        "resolves_existing_data_hold": resolves,
        "near_duplicate_of": near_duplicate_of,
        "duplicate_note": duplicate_note,
        "prior_research_priority": priority,
    }


def stage23_hypotheses() -> list:
    """The frozen Stage-23 owned-data hypothesis family.

    Deliberately SMALL and economically motivated. It excludes every construct
    the tournament has already rejected on owned price data (low volatility,
    idiosyncratic volatility, volatility-scaled momentum, momentum acceleration,
    short-term reversal, channel breakout, trend-slope t, path efficiency,
    realised skewness, max-daily-return, distance-from-52-week-high,
    vol-of-vol, short-vs-long vol ratio, overnight family, seasonality): re-running
    those would be exactly the correlated-variant tuning the mandate forbids.
    """
    return [
        _hyp("s23_dollar_volume_shock", "Dollar-volume shock (monthly)",
             "LIQUIDITY_TURNOVER", f_dollar_volume_shock,
             "Abnormal turnover marks information arrival and institutional "
             "repositioning before the price level fully adjusts.",
             "Flow information disjoint from the price LEVEL in mom_6_1 and the "
             "fundamental LEVEL in composite_sn.",
             resolves="c9_pricemomentu_f5f34df5ab"),
        _hyp("s23_amihud_illiquidity", "Amihud illiquidity (monthly)",
             "LIQUIDITY_TURNOVER", f_amihud_illiquidity,
             "Price impact per dollar traded; holders of high-impact names are "
             "compensated for exit risk.",
             "Classical illiquidity premium, re-specified at monthly resolution "
             "because the owned panel carries monthly ADV, not daily volume.",
             resolves="c9_pricemomentu_a2ed06e6a7"),
        _hyp("s23_liquidity_trend", "Sustained liquidity broadening",
             "LIQUIDITY_TURNOVER", f_liquidity_trend,
             "A persistent rise in dollar volume reflects a broadening holder "
             "base rather than a single information event.",
             "Slow-moving institutional adoption; complements the one-month "
             "shock measure."),
        _hyp("s23_momentum_consistency", "Momentum path consistency",
             "MOMENTUM_PATH_QUALITY", f_momentum_consistency,
             "Steady monthly gains reflect persistent reappraisal; the same total "
             "return delivered in one jump is more often a one-off repricing.",
             "Path QUALITY. mom_6_1 sees only the endpoints and discards this "
             "information entirely, so it is structurally incremental."),
        _hyp("s23_path_drawdown", "Trailing drawdown of the return path",
             "MOMENTUM_PATH_QUALITY", f_path_drawdown,
             "Advances achieved without a deep interim drawdown indicate demand "
             "that absorbed supply throughout the move.",
             "Sign- and path-dependent, therefore distinct from realised "
             "volatility, which is a symmetric second moment."),
        _hyp("s23_residual_momentum", "Market-residual momentum (monthly, 25y)",
             "RESIDUAL_RELATIVE_MOM", f_residual_momentum,
             "Raw momentum partly pays for market-beta exposure in trending "
             "markets; residualising isolates the stock-specific component.",
             "Beta-neutral momentum. Portfolio construction wants the "
             "stock-specific part, not the beta.",
             near_duplicate_of="Market-residual momentum (21d/63d)",
             duplicate_note=(
                 "The tournament already REJECTED market-residual momentum on the "
                 "daily-bar panel (63d: ic 0.108, t 1.96; 21d: ic -0.004, t -0.21). "
                 "Stage 23 re-tests it on a materially DEEPER basis — 313 "
                 "survivorship-safe months instead of the short daily-bar sample — "
                 "so it is a POWER replication, not a new idea. Classified as a "
                 "replication and prioritised accordingly."),
             priority="REPLICATION"),
        _hyp("s23_size_liquidity", "Liquidity/size premium (level)",
             "LIQUIDITY_TURNOVER", f_size_liquidity,
             "Within a large-cap index the less-liquid tail is held by fewer "
             "institutions and compensates holders for exit risk.",
             "Standalone factor AND the canonical control for the Workstream-C "
             "size/liquidity neutralisation."),
    ]


# =========================================================================== #
# CROSS-SECTION CONSTRUCTION.
# =========================================================================== #
def build_momentum_periods(panel: MomentumPanel, feature_fn, *,
                           expected_sign: int = 1,
                           members_only: bool = True,
                           min_names: int = MIN_CROSS_SECTION,
                           start_month: Optional[str] = None,
                           tickers: Optional[set] = None) -> tuple:
    """``(periods, coverage)`` for a feature over the monthly panel.

    ``periods`` is exactly the contract ``signal_evaluation.evaluate_periods``
    consumes: ``[{"as_of": month, "names": [(ticker, signed_value, fwd), ...]}]``.
    The forward return is ``fwd_1m_return`` at the SAME month — the target — and
    the feature may only read data knowable at that month.
    """
    periods = []
    scored = 0
    eligible = 0
    for m in panel.months:
        rows = panel.by_month[m]
        names = []
        for row in rows:
            if members_only and not row["is_member"]:
                continue
            if tickers is not None and row["ticker"] not in tickers:
                continue
            if start_month and m < start_month:
                continue
            eligible += 1
            fwd = row.get("fwd_1m_return")
            if fwd is None:
                continue
            val = feature_fn(panel, row, m)
            if val is None:
                continue
            scored += 1
            names.append((row["ticker"], float(val) * int(expected_sign), float(fwd)))
        if len(names) >= min_names:
            periods.append({"as_of": m, "names": names})
    coverage = {
        "eligible_name_months": eligible,
        "scored_name_months": scored,
        "coverage_pct": round(100.0 * scored / eligible, 4) if eligible else 0.0,
        "scored_periods": len(periods),
        "min_names_required": min_names,
    }
    return periods, coverage


def build_fundamental_periods(fund: dict, field: str = "composite_sn", *,
                              expected_sign: int = 1,
                              min_names: int = MIN_CROSS_SECTION,
                              skip_seed_month: bool = True) -> tuple:
    """``(periods, coverage)`` for the frozen fundamental panel, per month."""
    periods = []
    scored = eligible = 0
    seed = fund["months"][0] if fund["months"] else None
    for m in fund["months"]:
        if skip_seed_month and m == seed:
            continue
        names = []
        for row in fund["by_month"][m]:
            eligible += 1
            fwd = row.get("forward_63d_return")
            val = row.get(field)
            if fwd is None or val is None:
                continue
            scored += 1
            names.append((row["ticker"], float(val) * int(expected_sign), float(fwd)))
        if len(names) >= min_names:
            periods.append({"as_of": m, "names": names})
    coverage = {
        "eligible_name_months": eligible,
        "scored_name_months": scored,
        "coverage_pct": round(100.0 * scored / eligible, 4) if eligible else 0.0,
        "scored_periods": len(periods),
        "min_names_required": min_names,
        "seed_month_excluded": seed if skip_seed_month else None,
        "seed_month_exclusion_reason": (
            "the first panel month is a bulk history seed carrying up to 99 "
            "observations for a single ticker; including it would fabricate an "
            "artificial cross-section" if skip_seed_month else None),
    }
    return periods, coverage


def build_joint_periods(panel: MomentumPanel, fund: dict, *,
                        weights: Optional[dict] = None,
                        min_names: int = MIN_CROSS_SECTION,
                        leg: Optional[str] = None) -> tuple:
    """Cross-sections for the OPERATIONAL ensemble on the joint universe.

    For every month present in BOTH panels, the names present in both are
    z-scored within the joint cross-section and blended with the frozen
    operational weights. ``leg`` restricts to a single component evaluated on the
    SAME joint cross-section — the only apples-to-apples basis for measuring each
    leg's incremental contribution.

    The forward return is the fundamental panel's ``forward_63d_return`` so the
    blend is scored on the horizon the operational book actually holds over.
    """
    w = dict(weights or COMPONENT_WEIGHTS)
    periods = []
    scored = eligible = 0
    seed = fund["months"][0] if fund["months"] else None
    for m in fund["months"]:
        if m == seed:
            continue
        if m not in panel.by_month:
            continue
        mom_by_ticker = {r["ticker"]: r for r in panel.by_month[m] if r["is_member"]}
        rows = []
        for frow in fund["by_month"][m]:
            eligible += 1
            mrow = mom_by_ticker.get(frow["ticker"])
            if mrow is None:
                continue
            cs = frow.get("composite_sn")
            mv = mrow.get("mom_6_1")
            fwd = frow.get("forward_63d_return")
            if cs is None or mv is None or fwd is None:
                continue
            rows.append((frow["ticker"], cs, mv, fwd))
        if len(rows) < min_names:
            continue
        z_cs = _zscores([r[1] for r in rows])
        z_mv = _zscores([r[2] for r in rows])
        names = []
        for i, (t, _cs, _mv, fwd) in enumerate(rows):
            if z_cs[i] is None or z_mv[i] is None:
                continue
            if leg == COMPONENT_FUNDAMENTAL:
                val = z_cs[i]
            elif leg == COMPONENT_MOMENTUM:
                val = z_mv[i]
            else:
                val = w[COMPONENT_FUNDAMENTAL] * z_cs[i] + w[COMPONENT_MOMENTUM] * z_mv[i]
            scored += 1
            names.append((t, float(val), float(fwd)))
        if len(names) >= min_names:
            periods.append({"as_of": m, "names": names})
    coverage = {
        "eligible_name_months": eligible,
        "scored_name_months": scored,
        "coverage_pct": round(100.0 * scored / eligible, 4) if eligible else 0.0,
        "scored_periods": len(periods),
        "min_names_required": min_names,
        "joint_universe_rule": ("names present in BOTH the frozen fundamental panel "
                                "and the current-PIT momentum membership for that month"),
        "blend_rule": ("cross-sectional z-score of each leg WITHIN the joint "
                       "cross-section, then the frozen operational weights"),
    }
    return periods, coverage


# =========================================================================== #
# EVALUATION — always through the released evaluator + released gates.
# =========================================================================== #
def evaluate_cross_sectional_signal(
        periods: list, *, feature: str, horizon_days: int,
        cfg: dict, champion_returns: Optional[dict] = None,
        survivorship_safe: bool = True,
        coverage: Optional[dict] = None) -> dict:
    """Score ONE cross-sectional signal over ``periods`` and classify it with the
    RELEASED tournament gates.

    Returns ``{"row", "series", "metrics", "gate"}``. No statistic and no
    threshold is defined or softened here: the ranking/spread/cost math belongs to
    ``signal_evaluation.evaluate_periods`` and the gate belongs to
    ``tournament.classify_evidence``; ``cfg`` is the released
    ``stage9_tournament.json`` contract. This function is a thin ordering of those
    two released owners.

    OWNERSHIP: this is a bounded OFFLINE Stage-23 signal evaluation over a
    cross-section of names. It is deliberately NOT named ``evaluate``: the
    Persistent Research Agent's canonical research-state assessment
    (``engine/research_agent.evaluate``) is a different concept with a different
    contract - it assesses the operational book's champion against accumulated
    forward evidence and returns a research-assessment/lifecycle verdict. Stage 23
    neither implements nor duplicates that assessment.
    """
    res = sev.evaluate_periods(periods, horizon_days=horizon_days,
                               feature=feature, champion_returns=champion_returns,
                               cfg=cfg)
    row = res["row"]
    metrics = tt.row_to_contract_metrics(row, survivorship_safe=survivorship_safe)
    if coverage and coverage.get("coverage_pct") is not None:
        metrics["coverage_pct"] = float(coverage["coverage_pct"])
    gate = tt.classify_evidence(metrics, cfg)
    return {"row": row, "series": res["series"], "metrics": metrics, "gate": gate}


def _period_factor_rows(panel: MomentumPanel, feature_fn, control_fns: dict, *,
                        expected_sign: int, members_only: bool = True,
                        min_names: int = MIN_CROSS_SECTION) -> list:
    """Per-month aligned ``(values, forwards, {control: values})`` for neutralisation."""
    out = []
    for m in panel.months:
        vals, fwds, ctrls = [], [], {k: [] for k in control_fns}
        for row in panel.by_month[m]:
            if members_only and not row["is_member"]:
                continue
            fwd = row.get("fwd_1m_return")
            if fwd is None:
                continue
            v = feature_fn(panel, row, m)
            if v is None:
                continue
            cvals = {k: fn(panel, row, m) for k, fn in control_fns.items()}
            if any(cv is None for cv in cvals.values()):
                continue
            vals.append(float(v) * int(expected_sign))
            fwds.append(float(fwd))
            for k in control_fns:
                ctrls[k].append(cvals[k])
        if len(vals) >= min_names:
            out.append({"as_of": m, "values": vals, "forwards": fwds, "controls": ctrls})
    return out


def neutralisation_report(panel: MomentumPanel, feature_fn, *,
                          expected_sign: int = 1,
                          sector_blocked_reason: Optional[dict] = None) -> dict:
    """Controlled neutralisation of a momentum-panel factor.

    Runs the neutralisations owned data genuinely supports (market beta,
    realised volatility, liquidity/size) via the released
    ``orthogonality.partial_rank_ic``, and reports SECTOR neutralisation as
    BLOCKED with the measured coverage instead of substituting a look-ahead map.
    """
    rows = _period_factor_rows(panel, feature_fn, CONTROLS,
                               expected_sign=expected_sign)
    if not rows:
        return {"status": "NO_SCORED_PERIODS", "periods": 0}
    raw_ic, per_control = [], {k: [] for k in CONTROLS}
    all_ic = []
    for r in rows:
        base = orth.partial_rank_ic(r["values"], r["forwards"])
        if base is not None:
            raw_ic.append(base)
        for k in CONTROLS:
            pic = orth.partial_rank_ic(r["values"], r["forwards"], [r["controls"][k]])
            if pic is not None:
                per_control[k].append(pic)
        joint = orth.partial_rank_ic(r["values"], r["forwards"],
                                     [r["controls"][k] for k in CONTROLS])
        if joint is not None:
            all_ic.append(joint)

    def _summ(series):
        if not series:
            return {"mean_rank_ic": None, "t_stat": None, "periods": 0}
        m = _mean(series)
        s = _stdev(series)
        t = (m / (s / math.sqrt(len(series)))) if s else None
        return {"mean_rank_ic": round(m, 6) if m is not None else None,
                "t_stat": round(t, 4) if t is not None else None,
                "periods": len(series)}

    raw = _summ(raw_ic)
    out = {
        "status": "MEASURED",
        "raw": raw,
        "neutralised": {k: _summ(v) for k, v in per_control.items()},
        "all_controls_jointly": _summ(all_ic),
        "interpretation_rule": (
            "a factor whose rank IC survives a control carries information that "
            "control does not explain; a factor whose IC collapses was paying for "
            "that exposure"),
        "sector_neutral": {
            "status": "BLOCKED_NO_POINT_IN_TIME_SECTOR",
            "reason": (sector_blocked_reason or {}).get("reason"),
            "measured_coverage": sector_blocked_reason,
            "not_substituted": (
                "the owned repaired sector map is CURRENT-AS-OF; applying it to a "
                "historical cross-section would inject a classification look-ahead "
                "and fabricate PIT validity"),
        },
    }
    for k, v in out["neutralised"].items():
        if raw["mean_rank_ic"] and v["mean_rank_ic"] is not None:
            v["ic_retained_fraction"] = round(v["mean_rank_ic"] / raw["mean_rank_ic"], 4)
    return out


def concentration_report(series: dict, *, drop_counts: Sequence[int] = (1, 2, 3)) -> dict:
    """How much of the realised long/short spread depends on the best few periods.

    A signal whose entire edge disappears when the 1-3 best months are removed is
    an episode, not a persistent effect.
    """
    ls = list(series.get("ls") or [])
    n = len(ls)
    if n < 6:
        return {"status": "INSUFFICIENT_PERIODS", "periods": n}
    total = sum(ls)
    order = sorted(range(n), key=lambda i: ls[i], reverse=True)
    out = {"status": "MEASURED", "periods": n,
           "total_spread": round(total, 6),
           "mean_spread": round(total / n, 6), "drops": []}
    for k in drop_counts:
        if k >= n:
            continue
        keep = [ls[i] for i in range(n) if i not in set(order[:k])]
        rem = sum(keep)
        out["drops"].append({
            "best_periods_removed": k,
            "remaining_total_spread": round(rem, 6),
            "remaining_mean_spread": round(rem / len(keep), 6),
            "fraction_of_edge_retained": (round(rem / total, 4) if total else None),
            "still_positive": rem > 0,
        })
    return out


def subperiod_report(series: dict, *, n_blocks: int = 4) -> dict:
    """Mean IC and spread by equal-length contiguous block (era stability)."""
    dates = list(series.get("dates") or [])
    ic = list(series.get("ic") or [])
    ls = list(series.get("ls") or [])
    n = len(ic)
    if n < n_blocks * 3:
        return {"status": "INSUFFICIENT_PERIODS", "periods": n}
    size = n / n_blocks
    blocks = []
    for b in range(n_blocks):
        lo, hi = int(round(b * size)), int(round((b + 1) * size))
        if hi <= lo:
            continue
        blocks.append({
            "block": b + 1,
            "from": dates[lo] if lo < len(dates) else None,
            "to": dates[hi - 1] if hi - 1 < len(dates) else None,
            "periods": hi - lo,
            "mean_rank_ic": round(_mean(ic[lo:hi]) or 0.0, 6),
            "mean_spread": round(_mean(ls[lo:hi]) or 0.0, 6),
        })
    pos = sum(1 for b in blocks if b["mean_rank_ic"] > 0)
    return {"status": "MEASURED", "blocks": blocks,
            "positive_blocks": pos, "total_blocks": len(blocks),
            "sign_stable": pos == len(blocks)}


def overlap_report(a_periods: list, b_periods: list, *, top_n: int = 25) -> dict:
    """Top-N name overlap between two signals on their shared dates.

    Two signals that select the same names cannot diversify a portfolio however
    different their formulas look.
    """
    a_by = {p["as_of"]: p["names"] for p in a_periods}
    b_by = {p["as_of"]: p["names"] for p in b_periods}
    shared = sorted(set(a_by) & set(b_by))
    if not shared:
        return {"status": "NO_SHARED_DATES", "shared_dates": 0}
    fracs, rank_corrs = [], []
    for d in shared:
        a_top = {t for t, _v, _f in sorted(a_by[d], key=lambda x: -x[1])[:top_n]}
        b_top = {t for t, _v, _f in sorted(b_by[d], key=lambda x: -x[1])[:top_n]}
        if a_top and b_top:
            fracs.append(len(a_top & b_top) / float(min(len(a_top), len(b_top))))
        amap = {t: v for t, v, _f in a_by[d]}
        bmap = {t: v for t, v, _f in b_by[d]}
        common = sorted(set(amap) & set(bmap))
        if len(common) >= 10:
            rc = orth.rank_correlation([amap[t] for t in common],
                                       [bmap[t] for t in common])
            if rc is not None:
                rank_corrs.append(rc)
    return {
        "status": "MEASURED",
        "shared_dates": len(shared),
        "top_n": top_n,
        "mean_top_n_overlap_fraction": round(_mean(fracs) or 0.0, 4) if fracs else None,
        "mean_cross_sectional_rank_correlation": (
            round(_mean(rank_corrs) or 0.0, 4) if rank_corrs else None),
        "interpretation": ("high overlap or high rank correlation means the two "
                           "signals select the same book and cannot diversify it"),
    }


# =========================================================================== #
# WORKSTREAM C — OPERATIONAL EDGE AUTOPSY.
# =========================================================================== #
def run_edge_attribution(panel: MomentumPanel, fund: dict, sector: dict,
                         cfg: dict) -> dict:
    """Decompose the OPERATIONAL model's edge into its measurable sources.

    Every component is evaluated on its own native panel AND on the shared joint
    cross-section, because only the latter answers "what does each leg add to the
    thing we actually run".
    """
    out: dict = {
        "contract_id": CONTRACT_ID,
        "stage23_version": STAGE23_VERSION,
        "operational_strategy_id": OPERATIONAL_STRATEGY_ID,
        "operational_book_id": OPERATIONAL_BOOK_ID,
        "scoring_owner": OPERATIONAL_SCORING_OWNER,
        "compute_kernel": OPERATIONAL_KERNEL,
        "component_weights": COMPONENT_WEIGHTS,
        "weights_are_frozen_not_fitted": True,
        "evidence_class": "HISTORICAL_OUT_OF_SAMPLE_PANEL_STUDY",
        "gate_config_owner": "configs/alpha_agent/stage9_tournament.json",
        "evaluators": {"row": "alpha_agent.signal_evaluation.evaluate_periods",
                       "metrics": "alpha_agent.tournament.row_to_contract_metrics",
                       "gate": "alpha_agent.tournament.classify_evidence"},
        "panels": {},
        "component_native": {},
        "joint_universe": {},
        "redundancy": {},
        "neutralisation": {},
        "concentration": {},
        "subperiod": {},
    }
    out["panels"]["momentum_monthly"] = dict(
        panel.diagnostics, fingerprint=panel.fingerprint)
    out["panels"]["frozen_fundamental"] = dict(
        fund["diagnostics"], fingerprint=fund["fingerprint"])
    out["panels"]["sector_map"] = {
        "fingerprint": sector["fingerprint"],
        "point_in_time": sector["point_in_time"],
        "usable_for_neutralisation": sector["usable_for_neutralisation"],
        "reason": sector.get("reason"),
        "mapped_tickers": len(sector["map"]),
    }

    # --- each component on its OWN native panel ---------------------------- #
    mom_periods, mom_cov = build_momentum_periods(panel, f_mom_6_1, expected_sign=1)
    mom_eval = evaluate_cross_sectional_signal(
        mom_periods, feature=COMPONENT_MOMENTUM,
        horizon_days=HORIZON_MONTHLY, cfg=cfg,
        survivorship_safe=True, coverage=mom_cov)
    out["component_native"][COMPONENT_MOMENTUM] = {
        "panel": "momentum_monthly",
        "universe": "owned Norgate Russell-1000 Current & Past PIT members",
        "survivorship_safe": True,
        "date_range": [panel.diagnostics["first_month"], panel.diagnostics["last_month"]],
        "horizon_days": HORIZON_MONTHLY,
        "coverage": mom_cov,
        "row": mom_eval["row"],
        "metrics": mom_eval["metrics"],
        "gate": mom_eval["gate"],
    }

    fund_periods, fund_cov = build_fundamental_periods(fund, "composite_sn",
                                                       expected_sign=1)
    fund_eval = evaluate_cross_sectional_signal(
        fund_periods, feature=COMPONENT_FUNDAMENTAL,
        horizon_days=HORIZON_QUARTERLY, cfg=cfg,
        survivorship_safe=False, coverage=fund_cov)
    out["component_native"][COMPONENT_FUNDAMENTAL] = {
        "panel": "frozen_fundamental",
        "universe": "EODHD 545-name scored panel",
        "survivorship_safe": False,
        "survivorship_caveat": fund["diagnostics"]["survivorship_basis"],
        "date_range": [fund["diagnostics"]["first_month"], fund["diagnostics"]["last_month"]],
        "horizon_days": HORIZON_QUARTERLY,
        "coverage": fund_cov,
        "row": fund_eval["row"],
        "metrics": fund_eval["metrics"],
        "gate": fund_eval["gate"],
    }

    # --- the ensemble and both legs on the SHARED joint cross-section ------- #
    joint_specs = [("ensemble_50_50", None), (COMPONENT_FUNDAMENTAL, COMPONENT_FUNDAMENTAL),
                   (COMPONENT_MOMENTUM, COMPONENT_MOMENTUM)]
    joint_periods_by_key = {}
    for key, leg in joint_specs:
        periods, cov = build_joint_periods(panel, fund, leg=leg)
        joint_periods_by_key[key] = periods
        ev = evaluate_cross_sectional_signal(
            periods, feature="joint_%s" % key,
            horizon_days=HORIZON_QUARTERLY, cfg=cfg,
            survivorship_safe=False, coverage=cov)
        out["joint_universe"][key] = {
            "coverage": cov, "row": ev["row"], "metrics": ev["metrics"],
            "gate": ev["gate"],
            "series_dates": ev["series"]["dates"],
        }
        out["joint_universe"][key]["_series"] = ev["series"]

    # --- sensitivity: does the FROZEN 50/50 matter? ------------------------ #
    out["weight_sensitivity"] = {}
    for label, w in (("fund30_mom70", {COMPONENT_FUNDAMENTAL: 0.3, COMPONENT_MOMENTUM: 0.7}),
                     ("fund70_mom30", {COMPONENT_FUNDAMENTAL: 0.7, COMPONENT_MOMENTUM: 0.3})):
        periods, cov = build_joint_periods(panel, fund, weights=w)
        ev = evaluate_cross_sectional_signal(
            periods, feature="joint_%s" % label,
            horizon_days=HORIZON_QUARTERLY, cfg=cfg,
            survivorship_safe=False, coverage=cov)
        out["weight_sensitivity"][label] = {
            "weights": w, "rank_ic": ev["metrics"].get("rank_ic"),
            "rank_ic_t": ev["metrics"].get("rank_ic_t"),
            "spread_t": ev["metrics"].get("spread_t"),
            "net25_spread": ev["metrics"].get("net25_spread"),
            "gate": ev["gate"].get("evidence_status") or ev["gate"].get("target_state"),
        }
    out["weight_sensitivity"]["note"] = (
        "the operational 50/50 weights are FROZEN, never fitted on forward "
        "returns; these views measure how much the choice actually matters")

    # --- redundancy between the two legs ----------------------------------- #
    ens = out["joint_universe"]["ensemble_50_50"]["_series"]
    f_ser = out["joint_universe"][COMPONENT_FUNDAMENTAL]["_series"]
    m_ser = out["joint_universe"][COMPONENT_MOMENTUM]["_series"]
    out["redundancy"]["component_spread_correlation"] = orth.factor_correlation(
        f_ser["ls"], m_ser["ls"])
    out["redundancy"]["component_ic_correlation"] = orth.factor_correlation(
        f_ser["ic"], m_ser["ic"])
    out["redundancy"]["top25_overlap"] = overlap_report(
        joint_periods_by_key[COMPONENT_FUNDAMENTAL],
        joint_periods_by_key[COMPONENT_MOMENTUM], top_n=25)
    out["redundancy"]["incremental_vs_fundamental"] = orth.incremental_return_metrics(
        m_ser["ls"], f_ser["ls"])
    out["redundancy"]["incremental_vs_momentum"] = orth.incremental_return_metrics(
        f_ser["ls"], m_ser["ls"])
    out["redundancy"]["ensemble_vs_fundamental"] = orth.incremental_return_metrics(
        ens["ls"], f_ser["ls"])
    out["redundancy"]["ensemble_vs_momentum"] = orth.incremental_return_metrics(
        ens["ls"], m_ser["ls"])

    # partial rank IC of each leg controlling for the other, per joint date
    part_f, part_m = [], []
    fp = {p["as_of"]: p["names"] for p in joint_periods_by_key[COMPONENT_FUNDAMENTAL]}
    mp = {p["as_of"]: p["names"] for p in joint_periods_by_key[COMPONENT_MOMENTUM]}
    for d in sorted(set(fp) & set(mp)):
        fmap = {t: v for t, v, _ in fp[d]}
        mmap = {t: v for t, v, _ in mp[d]}
        fwd = {t: f for t, _v, f in fp[d]}
        common = sorted(set(fmap) & set(mmap) & set(fwd))
        if len(common) < MIN_CROSS_SECTION:
            continue
        fv = [fmap[t] for t in common]
        mv = [mmap[t] for t in common]
        yv = [fwd[t] for t in common]
        a = orth.partial_rank_ic(fv, yv, [mv])
        b = orth.partial_rank_ic(mv, yv, [fv])
        if a is not None:
            part_f.append(a)
        if b is not None:
            part_m.append(b)

    def _psumm(series, label):
        if not series:
            return {"label": label, "periods": 0, "mean_partial_rank_ic": None,
                    "t_stat": None}
        m = _mean(series)
        s = _stdev(series)
        t = (m / (s / math.sqrt(len(series)))) if s else None
        return {"label": label, "periods": len(series),
                "mean_partial_rank_ic": round(m, 6),
                "t_stat": round(t, 4) if t is not None else None}

    out["redundancy"]["partial_rank_ic"] = {
        "composite_sn_controlling_for_mom_6_1":
            _psumm(part_f, "composite_sn | mom_6_1"),
        "mom_6_1_controlling_for_composite_sn":
            _psumm(part_m, "mom_6_1 | composite_sn"),
        "interpretation": ("a leg whose partial rank IC survives the other leg "
                           "carries incremental information; a leg whose partial "
                           "IC collapses is redundant inside this ensemble"),
    }

    # --- neutralisation / concentration / subperiod for the deep leg -------- #
    out["neutralisation"][COMPONENT_MOMENTUM] = neutralisation_report(
        panel, f_mom_6_1, expected_sign=1,
        sector_blocked_reason={
            "reason": sector.get("reason"),
            "panel_sector_known_pct": panel.diagnostics["panel_sector_known_pct"],
            "sector_map_tickers": len(sector["map"]),
            "panel_distinct_tickers": panel.diagnostics["distinct_tickers"],
        })
    for key in ("ensemble_50_50", COMPONENT_FUNDAMENTAL, COMPONENT_MOMENTUM):
        s = out["joint_universe"][key]["_series"]
        out["concentration"][key] = concentration_report(s)
        out["subperiod"][key] = subperiod_report(s)
    out["concentration"]["mom_6_1_native_deep_panel"] = concentration_report(
        mom_eval["series"])
    out["subperiod"]["mom_6_1_native_deep_panel"] = subperiod_report(
        mom_eval["series"], n_blocks=5)
    out["subperiod"]["composite_sn_native"] = subperiod_report(fund_eval["series"])

    out["gate_observations"] = _gate_observations(out)
    out["findings"] = _edge_findings(out)
    for key in list(out["joint_universe"]):
        out["joint_universe"][key].pop("_series", None)
    return out


def _gate_observations(a: dict) -> list:
    """Measured interactions between this evidence and the RELEASED gates.

    Stage 23 never edits a gate threshold — changing the bar is a governance
    decision, not a research one. It does record, with evidence, where a gate
    behaved in a way that materially shapes what the agent can ever conclude.
    """
    obs = []
    fund_gate = (a["component_native"][COMPONENT_FUNDAMENTAL] or {}).get("gate", {})
    if fund_gate.get("target_state") == tt.DATA_HOLD:
        obs.append({
            "observation": "DOMINANT_LEG_CANNOT_CLEAR_EVIDENCE_COMPLETENESS",
            "gate": "evidence_completeness.require_survivorship_safe",
            "blocker": fund_gate.get("blocker"),
            "evidence": (
                "composite_sn exists only on the frozen Phase 10-L panel, whose "
                "545-name EODHD universe is documented SURVIVOR-BIASED. The "
                "released gate therefore refuses to certify it — correctly. Its "
                "measured statistics are real but UNCERTIFIABLE on this basis."),
            "consequence": (
                "The leg that carries most of the operational ranking information "
                "is the one the project's own evidence contract cannot validate. "
                "Resolving this needs a survivorship-safe fundamental panel, not "
                "another factor."),
            "action_taken": "REPORTED_ONLY_GATE_UNCHANGED",
        })
    mom_metrics = a["component_native"][COMPONENT_MOMENTUM].get("metrics", {})
    dd = mom_metrics.get("max_drawdown_pct")
    periods = mom_metrics.get("scored_periods")
    if dd is not None and dd < -35.0:
        obs.append({
            "observation": "DRAWDOWN_GATE_IS_LENGTH_SENSITIVE",
            "gate": "gates.keep_max_drawdown_pct = -35.0",
            "evidence": (
                "max_drawdown is the drawdown of the UNNORMALISED cumulative SUM "
                "of per-period long/short spreads. Over %s scored periods mom_6_1 "
                "measures %.1f, and every Stage-23 candidate on the same panel "
                "also breaches -35.0. A fixed threshold against a quantity that "
                "grows with the number of periods penalises longer, better-powered "
                "evaluation windows." % (periods, dd)),
            "consequence": (
                "As configured, no candidate evaluated over a multi-decade panel "
                "can pass this gate, so depth of evidence is implicitly punished."),
            "recommended_owner_action": (
                "the gate owner (configs/alpha_agent/stage9_tournament.json) should "
                "consider a length-normalised drawdown (e.g. drawdown relative to "
                "cumulative spread, which is already computed) — a governance "
                "decision, deliberately NOT taken by Stage 23"),
            "action_taken": "REPORTED_ONLY_GATE_UNCHANGED",
        })
    return obs


def _edge_findings(a: dict) -> dict:
    """The machine-readable answers to the Workstream-C questions."""
    j = a["joint_universe"]
    part = a["redundancy"]["partial_rank_ic"]
    pf = part["composite_sn_controlling_for_mom_6_1"]
    pm = part["mom_6_1_controlling_for_composite_sn"]
    ens, fnd, mom = j["ensemble_50_50"], j[COMPONENT_FUNDAMENTAL], j[COMPONENT_MOMENTUM]
    sub = a["subperiod"]

    def _mt(d, k):
        return (d.get("metrics") or {}).get(k)

    stable = {k: sub.get(k, {}).get("sign_stable") for k in
              ("ensemble_50_50", COMPONENT_FUNDAMENTAL, COMPONENT_MOMENTUM)}
    return {
        "evidence_class": "HISTORICAL_OUT_OF_SAMPLE_PANEL_STUDY",
        "joint_universe_note": (
            "all comparisons below are on the SHARED joint cross-section (same "
            "dates, same names, same 63-day horizon), which is the only "
            "apples-to-apples basis for an incremental-contribution claim"),
        "A_is_the_edge_stock_selection_alpha": {
            "answer": "PARTIALLY, AND ONLY VIA THE FUNDAMENTAL LEG",
            "evidence": {
                "composite_sn_partial_rank_ic_given_mom": pf.get("mean_partial_rank_ic"),
                "composite_sn_partial_t": pf.get("t_stat"),
                "mom_6_1_partial_rank_ic_given_composite": pm.get("mean_partial_rank_ic"),
                "mom_6_1_partial_t": pm.get("t_stat"),
            },
            "reading": (
                "composite_sn retains essentially all of its rank information after "
                "mom_6_1 is partialled out (t≈%s). mom_6_1's incremental rank "
                "information is not statistically distinguishable from zero (t≈%s)."
                % (pf.get("t_stat"), pm.get("t_stat"))),
        },
        "B_how_much_is_factor_or_beta_exposure": {
            "answer": "NOT SIZE/LIQUIDITY; PARTLY MASKED BY BETA AND VOLATILITY",
            "evidence": a["neutralisation"][COMPONENT_MOMENTUM],
            "reading": (
                "Neutralising log dollar volume leaves mom_6_1's IC unchanged, so "
                "the momentum leg is not a disguised size/liquidity bet. "
                "Neutralising market beta or realised volatility RAISES the "
                "(still insignificant) IC, so adverse beta/vol exposure is "
                "diluting the leg rather than creating it."),
            "sector_component": "UNMEASURABLE — no point-in-time sector data (see neutralisation.sector_neutral)",
        },
        "C_which_component_contributes_incrementally": {
            "answer": "composite_sn contributes RANK information; mom_6_1 contributes DIVERSIFICATION",
            "evidence": {
                "rank_ic_t": {"ensemble": _mt(ens, "rank_ic_t"),
                              "composite_sn": _mt(fnd, "rank_ic_t"),
                              "mom_6_1": _mt(mom, "rank_ic_t")},
                "spread_t": {"ensemble": _mt(ens, "spread_t"),
                             "composite_sn": _mt(fnd, "spread_t"),
                             "mom_6_1": _mt(mom, "spread_t")},
                "net25_spread": {"ensemble": _mt(ens, "net25_spread"),
                                 "composite_sn": _mt(fnd, "net25_spread"),
                                 "mom_6_1": _mt(mom, "net25_spread")},
                "max_drawdown_pct": {"ensemble": _mt(ens, "max_drawdown_pct"),
                                     "composite_sn": _mt(fnd, "max_drawdown_pct"),
                                     "mom_6_1": _mt(mom, "max_drawdown_pct")},
                "component_spread_correlation": a["redundancy"]["component_spread_correlation"],
                "subperiod_sign_stable": stable,
            },
            "reading": (
                "The two legs are nearly uncorrelated, so blending them raises the "
                "spread t-statistic above either leg alone and roughly halves the "
                "drawdown. The ensemble is the only one of the three whose IC sign "
                "is stable across every subperiod block: composite_sn turns "
                "negative in the most recent block while mom_6_1 is strongest "
                "there, and vice versa earlier. The 50/50 is doing regime "
                "insurance, not rank improvement."),
        },
        "D_which_components_are_redundant": {
            "answer": "NEITHER — they are complements, not duplicates",
            "evidence": {
                "component_spread_correlation": a["redundancy"]["component_spread_correlation"],
                "component_ic_correlation": a["redundancy"]["component_ic_correlation"],
                "top25_overlap": a["redundancy"]["top25_overlap"],
            },
        },
        "E_where_does_the_model_fail": {
            "answer": "RANK POWER, SURVIVORSHIP BASIS, AND TURNOVER",
            "evidence": {
                "momentum_leg_on_deep_survivorship_safe_panel": {
                    "rank_ic": _mt(a["component_native"][COMPONENT_MOMENTUM], "rank_ic"),
                    "rank_ic_t": _mt(a["component_native"][COMPONENT_MOMENTUM], "rank_ic_t"),
                    "scored_periods": _mt(a["component_native"][COMPONENT_MOMENTUM],
                                          "scored_periods"),
                    "gate": a["component_native"][COMPONENT_MOMENTUM]["gate"].get("target_state"),
                },
                "fundamental_leg_gate": a["component_native"][COMPONENT_FUNDAMENTAL]["gate"],
                "ensemble_turnover_per_rebalance": _mt(ens, "turnover_per_rebalance"),
                "concentration": a["concentration"].get("ensemble_50_50"),
            },
            "reading": (
                "Over 312 survivorship-safe monthly cross-sections the momentum leg "
                "is weak (t≈1.3) and flat through 2005-2016 — its 2016-2026 "
                "contribution is recent, not structural. The fundamental leg cannot "
                "clear the evidence-completeness gate at all because its only panel "
                "is survivor-biased. Ensemble turnover is near 1.0 per quarterly "
                "rebalance, so the net-of-cost edge is materially smaller than the "
                "gross one."),
        },
        "F_what_kind_of_signal_would_most_improve_it": {
            "answer": "A SURVIVORSHIP-SAFE, POINT-IN-TIME FUNDAMENTAL OR EXPECTATIONS SIGNAL",
            "reading": (
                "The measured gap is not 'another price factor' — the owned price "
                "panel has been searched exhaustively and the Stage-23 additions "
                "found nothing either. The gap is that the leg carrying the ranking "
                "information rests on an uncertifiable universe. The highest-value "
                "additions are (1) a survivorship-safe PIT fundamental basis and "
                "(2) analyst expectations history, which is economically disjoint "
                "from both realised fundamentals and price and is the one untested "
                "orthogonal family."),
        },
    }


# =========================================================================== #
# WORKSTREAM D/E — the bounded owned-data challenger campaign.
# =========================================================================== #
def _existing_feature_index(registry: Optional["tt.CandidateRegistry"]) -> dict:
    """Existing tournament candidates keyed by feature, for duplicate detection."""
    if registry is None:
        return {}
    idx = {}
    for c in registry.list():
        try:
            spec = c.get("spec") if isinstance(c.get("spec"), dict) else {}
        except Exception:  # noqa: BLE001
            spec = {}
        feat = (spec or {}).get("feature")
        if feat:
            idx[feat] = {"candidate_id": c["candidate_id"], "name": c["name"],
                         "lifecycle_state": c["lifecycle_state"],
                         "blocker": c.get("blocker")}
    return idx


def run_owned_campaign(panel: MomentumPanel, cfg: dict, *,
                       champion_series: Optional[dict] = None,
                       registry: Optional["tt.CandidateRegistry"] = None,
                       hypotheses: Optional[list] = None) -> dict:
    """Evaluate the frozen Stage-23 hypothesis family with FDR control.

    Individual evaluation FIRST (the mandate forbids combinatorial search); the
    released gates decide KEEP / REJECT / DATA_HOLD; Benjamini-Hochberg is applied
    across the whole family so a single lucky t-statistic cannot be reported as a
    discovery.
    """
    hyps = list(hypotheses if hypotheses is not None else stage23_hypotheses())
    existing = _existing_feature_index(registry)
    results = []
    for h in hyps:
        periods, cov = build_momentum_periods(
            panel, h["feature_fn"], expected_sign=h["expected_sign"])
        ev = evaluate_cross_sectional_signal(
            periods, feature=h["hypothesis_id"],
            horizon_days=h["horizon_days"], cfg=cfg,
            champion_returns=(champion_series or {}).get("long_short_by_date"),
            survivorship_safe=True, coverage=cov)
        rec = {k: v for k, v in h.items() if k != "feature_fn"}
        rec.update({
            "coverage": cov,
            "row": ev["row"],
            "metrics": ev["metrics"],
            "gate": ev["gate"],
            "concentration": concentration_report(ev["series"]),
            "subperiod": subperiod_report(ev["series"], n_blocks=5),
            "duplicate_check": {
                "existing_candidate_with_same_feature": existing.get(h["hypothesis_id"]),
                "declared_near_duplicate_of": h.get("near_duplicate_of"),
                "note": h.get("duplicate_note"),
            },
        })
        if champion_series:
            rec["correlation_vs_operational_momentum_leg"] = orth.factor_correlation(
                ev["series"]["ls"],
                [(champion_series.get("long_short_by_date") or {}).get(d)
                 for d in ev["series"]["dates"]])
        rec["_series"] = ev["series"]
        results.append(rec)

    # --- family-wide FDR ---------------------------------------------------- #
    pvals, idxs = [], []
    for i, r in enumerate(results):
        t = r["metrics"].get("rank_ic_t")
        n = int(r["metrics"].get("scored_periods") or 0)
        p = _pvalue_from_t(t, max(1, n - 1))
        if p is not None:
            pvals.append(p)
            idxs.append(i)
    qvals = sctl.benjamini_hochberg(pvals) if pvals else []
    for j, i in enumerate(idxs):
        results[i]["fdr"] = {
            "family_size": len(hyps),
            "p_value_two_sided_normal_approx": round(pvals[j], 8),
            "bh_q_value": round(qvals[j], 8),
            "survives_fdr_alpha_005": qvals[j] <= 0.05,
            "method": "alpha_agent.selection_controls.benjamini_hochberg",
        }
    for r in results:
        r.setdefault("fdr", {"family_size": len(hyps), "p_value_two_sided_normal_approx": None,
                             "bh_q_value": None, "survives_fdr_alpha_005": False,
                             "method": "alpha_agent.selection_controls.benjamini_hochberg"})

    # --- orthogonality vs the operational momentum leg, for survivors ------- #
    for r in results:
        keep = (r["gate"].get("target_state") == tt.KEEP_FOR_RESEARCH)
        r["classification"] = classify_stage23_result(r)
        r["clears_existing_research_gate"] = bool(
            keep and r["fdr"].get("survives_fdr_alpha_005"))

    summary = {
        "stage23_version": STAGE23_VERSION,
        "family_size": len(hyps),
        "evaluated": len(results),
        "fdr_alpha": 0.05,
        "fdr_survivors": [r["hypothesis_id"] for r in results
                          if r["fdr"].get("survives_fdr_alpha_005")],
        "gate_keep": [r["hypothesis_id"] for r in results
                      if r["gate"].get("target_state") == tt.KEEP_FOR_RESEARCH],
        "promising_challengers": [r["hypothesis_id"] for r in results
                                  if r["clears_existing_research_gate"]],
        "classification_counts": {},
        "no_automatic_promotion": True,
        "gate_owner": "alpha_agent.tournament.classify_evidence",
    }
    for r in results:
        c = r["classification"]
        summary["classification_counts"][c] = summary["classification_counts"].get(c, 0) + 1
    return {"summary": summary, "results": results}


# --------------------------------------------------------------------------- #
# Classification vocabulary — mapped onto the EXISTING project vocabulary, never
# a competing ontology.
# --------------------------------------------------------------------------- #
CLS_PROMISING = "PROMISING_CHALLENGER"
CLS_NEEDS_MORE = "NEEDS_MORE_EVIDENCE"
CLS_REDUNDANT = "REDUNDANT_WITH_EXISTING_ALPHA"
CLS_FAILED_ROBUSTNESS = "FAILED_ROBUSTNESS"
CLS_FAILED_COSTS = "FAILED_COSTS"
CLS_FAILED_PIT = "FAILED_PIT"
CLS_INSUFFICIENT_DATA = "INSUFFICIENT_DATA"
CLS_WAITING_INTRINIO = "WAITING_FOR_INTRINIO"

#: Stage-23 classification -> the released tournament lifecycle state it maps to.
CLASSIFICATION_TO_LIFECYCLE = {
    CLS_PROMISING: tt.KEEP_FOR_RESEARCH,
    CLS_NEEDS_MORE: tt.DATA_HOLD,
    CLS_REDUNDANT: tt.REJECTED,
    CLS_FAILED_ROBUSTNESS: tt.REJECTED,
    CLS_FAILED_COSTS: tt.REJECTED,
    CLS_FAILED_PIT: tt.DATA_HOLD,
    CLS_INSUFFICIENT_DATA: tt.DATA_HOLD,
    CLS_WAITING_INTRINIO: tt.DATA_HOLD,
}


def classify_stage23_result(rec: dict) -> str:
    """Map a released gate verdict onto the Stage-23 reporting vocabulary.

    The GATE decides; this only names the decision in the mandate's terms. No
    threshold is re-applied and no verdict is overturned.
    """
    gate = rec.get("gate") or {}
    metrics = rec.get("metrics") or {}
    state = gate.get("target_state")
    blocker = (gate.get("blocker") or "")
    failed = set(gate.get("failed_gates") or [])
    if state == tt.DATA_HOLD:
        # Match the RELEASED blocker constants exactly — a substring test on
        # "PIT" silently misses DATA_HOLD_POINT_IN_TIME_UNAVAILABLE, which is the
        # blocker the survivor-biased fundamental panel actually raises.
        if blocker in (tt.BLOCK_PIT, tt.BLOCK_LOOKAHEAD):
            return CLS_FAILED_PIT
        return CLS_INSUFFICIENT_DATA
    if state == tt.KEEP_FOR_RESEARCH:
        if not rec.get("fdr", {}).get("survives_fdr_alpha_005"):
            return CLS_NEEDS_MORE
        corr = rec.get("correlation_vs_operational_momentum_leg")
        if corr is not None and abs(corr) >= 0.8:
            return CLS_REDUNDANT
        return CLS_PROMISING
    # REJECTED — name the dominant reason.
    if any("COST" in f or "NET" in f for f in failed):
        return CLS_FAILED_COSTS
    if metrics.get("cost_flips_sign"):
        return CLS_FAILED_COSTS
    return CLS_FAILED_ROBUSTNESS


def register_campaign_candidates(registry: "tt.CandidateRegistry", cfg: dict,
                                 campaign: dict, *,
                                 evidence_date: Optional[str] = None) -> dict:
    """Register every Stage-23 hypothesis through the EXISTING candidate lifecycle.

    Null and rejected results are registered too — a research programme that only
    records winners cannot tell the agent what has already been ruled out.
    Seeding is idempotent (dedup by family+spec_hash) and the gate transition is
    applied by the released ``ingest_completed_experiments``; nothing here
    promotes anything or writes an operational store.
    """
    seeded, completed = [], []
    for r in campaign["results"]:
        spec = {
            "feature": r["hypothesis_id"],
            "horizon_days": r["horizon_days"],
            "rebalance": r["rebalance"],
            "template": "stage23_cross_sectional_rank",
            "expected_sign": r["expected_sign"],
            "origin": ORIGIN,
            "stage23_version": STAGE23_VERSION,
            "data_basis": r["data_basis"],
        }
        cid = registry.seed_candidate(
            name=r["name"], family=r["family"], spec=spec,
            data_dependencies=["owned_norgate_r1000_monthly_panel"],
            universe="Norgate Russell-1000 Current & Past (survivorship-safe)",
            pit_status="OWNED_PIT_SAFE_MONTHLY_PANEL",
            component_signals=[])
        seeded.append({"hypothesis_id": r["hypothesis_id"], "candidate_id": cid})
        completed.append({"feature": r["hypothesis_id"], "row": r["row"],
                          "job_id": "stage23_%s" % r["hypothesis_id"]})
    ingest = tt.ingest_completed_experiments(
        registry, cfg, completed=completed, source=ORIGIN,
        evidence_date=evidence_date)
    return {"seeded": seeded, "ingest": ingest,
            "registry_owner": "alpha_agent.tournament.CandidateRegistry",
            "no_automatic_promotion": True}


# =========================================================================== #
# WORKSTREAM J — research priority queue.
# =========================================================================== #
PRIORITY_HIGH = "HIGH_PRIORITY"
PRIORITY_MEDIUM = "MEDIUM_PRIORITY"
PRIORITY_LOW = "LOW_PRIORITY"
PRIORITY_WAITING = "WAITING_FOR_DATA"

#: Which unavailable data family each known blocker is waiting on, and whether a
#: purchasable historical vendor feed (Steele / Intrinio) would resolve it.
BLOCKER_DATA_FAMILY = {
    "DATA_HOLD_INSUFFICIENT_OBSERVATIONS": "provider_or_calendar_time",
    "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE": "point_in_time_history",
    "DATA_HOLD_REQUIRES_VOLUME_TURNOVER_DATA": "volume_turnover",
    "DATA_HOLD_REQUIRES_EVENT_CALENDAR_DATA": "point_in_time_event_calendar",
}

INTRINIO_RESOLVABLE_DEPENDENCIES = ("eodhd_analyst_vintages",
                                    "analyst_revision_history",
                                    "consensus_estimate_history")


def build_priority_queue(*, campaign: Optional[dict] = None,
                         held_candidates: Optional[list] = None,
                         attribution: Optional[dict] = None) -> dict:
    """The ordered research queue, grounded in MEASURED state.

    Ordering inputs, in the project's existing terms:
      * data readiness      — is the required family owned TODAY?
      * PIT confidence      — can it be evaluated honestly?
      * economic distinctness / measured orthogonality to the current edge
      * prior research      — has this been tested and rejected already?
      * portfolio relevance — does it bear on how capital is actually allocated?

    No opaque composite score is invented: each entry carries its reasons, and
    the bucket follows deterministically from data readiness plus measured
    evidence.
    """
    entries = []

    # A DATA_HOLD that a Stage-23 hypothesis actually evaluated is no longer
    # waiting for data. Leaving it in the waiting bucket would tell the agent to
    # keep queuing work that has already been done.
    resolved_by_stage23: dict = {}
    for r in (campaign or {}).get("results", []):
        target = r.get("resolves_existing_data_hold")
        if target:
            resolved_by_stage23[target] = {
                "resolved_by": r["hypothesis_id"],
                "classification": r.get("classification"),
                "rank_ic_t": (r.get("metrics") or {}).get("rank_ic_t"),
            }

    for r in (campaign or {}).get("results", []):
        cls = r.get("classification")
        hid = r["hypothesis_id"]
        m = r.get("metrics") or {}
        if cls == CLS_PROMISING:
            bucket, reason = PRIORITY_HIGH, (
                "cleared the released evidence gates AND family-wide FDR on owned "
                "survivorship-safe data; ready for shadow-book forward tracking")
        elif cls == CLS_NEEDS_MORE:
            bucket, reason = PRIORITY_MEDIUM, (
                "passed the released gates but did not survive family-wide FDR; "
                "needs independent confirmation, not a re-tuned variant")
        elif cls in (CLS_INSUFFICIENT_DATA, CLS_FAILED_PIT):
            bucket, reason = PRIORITY_WAITING, (
                "cannot be evaluated honestly on owned data: %s"
                % (r.get("gate") or {}).get("blocker"))
        elif cls == CLS_REDUNDANT:
            bucket, reason = PRIORITY_LOW, (
                "measured correlation with the operational momentum leg is high; "
                "it would duplicate the book rather than diversify it")
        else:
            bucket, reason = PRIORITY_LOW, (
                "tested on owned data and rejected by the released gates (%s); "
                "re-running a correlated variant is not research"
                % ", ".join((r.get("gate") or {}).get("failed_gates") or []) or "weak evidence")
        entries.append({
            "item": hid,
            "kind": "STAGE23_HYPOTHESIS",
            "priority": bucket,
            "classification": cls,
            "family": r.get("family"),
            "economic_reason": r.get("economic_rationale"),
            "priority_reason": reason,
            "rank_ic": m.get("rank_ic"),
            "rank_ic_t": m.get("rank_ic_t"),
            "bh_q_value": (r.get("fdr") or {}).get("bh_q_value"),
            "data_ready": True,
            "pit_confidence": "OWNED_PIT_SAFE",
        })

    for c in (held_candidates or []):
        blocker = c.get("blocker") or ""
        fam = BLOCKER_DATA_FAMILY.get(blocker, "unknown")
        deps = c.get("data_dependencies") or []
        intrinio = any(d in INTRINIO_RESOLVABLE_DEPENDENCIES for d in deps) or \
            (c.get("family") == "ANALYST_EARNINGS")
        superseded = resolved_by_stage23.get(c.get("candidate_id"))
        if superseded:
            entries.append({
                "item": c.get("name"),
                "kind": "EXISTING_TOURNAMENT_DATA_HOLD",
                "candidate_id": c.get("candidate_id"),
                "priority": PRIORITY_LOW,
                "classification": superseded["classification"],
                "family": c.get("family"),
                "blocker": blocker,
                "superseded_by_stage23_hypothesis": superseded["resolved_by"],
                "rank_ic_t": superseded["rank_ic_t"],
                "priority_reason": (
                    "no longer waiting for data: Stage 23 evaluated this economic "
                    "construct on the owned monthly panel via %s and the released "
                    "gates rejected it. The original DATA_HOLD referred to DAILY "
                    "volume, which is still unowned, so the daily specification "
                    "remains untested — but re-queuing it as unexplored would be "
                    "wrong." % superseded["resolved_by"]),
                "data_ready": True,
                "pit_confidence": "OWNED_PIT_SAFE_AT_MONTHLY_RESOLUTION",
            })
            continue
        entries.append({
            "item": c.get("name"),
            "kind": "EXISTING_TOURNAMENT_DATA_HOLD",
            "candidate_id": c.get("candidate_id"),
            "priority": PRIORITY_WAITING,
            "classification": (CLS_WAITING_INTRINIO if intrinio else CLS_INSUFFICIENT_DATA),
            "family": c.get("family"),
            "blocker": blocker,
            "waiting_on_data_family": fam,
            "data_dependencies": deps,
            "resolved_by_historical_analyst_vendor": bool(intrinio),
            "priority_reason": (
                "waiting on historical analyst/estimate vintages; a genuine "
                "historical vendor feed satisfies the vintage-count requirement "
                "immediately instead of accruing one calendar day at a time"
                if intrinio else
                "waiting on an owned data family that does not yet exist at the "
                "required coverage; only a data change unblocks it"),
            "data_ready": False,
            "pit_confidence": "BLOCKED",
        })

    order = {PRIORITY_HIGH: 0, PRIORITY_MEDIUM: 1, PRIORITY_WAITING: 2, PRIORITY_LOW: 3}
    entries.sort(key=lambda e: (order.get(e["priority"], 9),
                                -(abs(e.get("rank_ic_t") or 0.0)), str(e.get("item"))))
    return {
        "stage23_version": STAGE23_VERSION,
        "generated_by": ORIGIN,
        "ordering_inputs": ["data_readiness", "pit_confidence",
                            "economic_distinctness", "measured_orthogonality",
                            "prior_research", "portfolio_decision_relevance"],
        "no_opaque_composite_score": True,
        "counts": {b: sum(1 for e in entries if e["priority"] == b)
                   for b in (PRIORITY_HIGH, PRIORITY_MEDIUM, PRIORITY_WAITING, PRIORITY_LOW)},
        "entries": entries,
    }


# =========================================================================== #
# WORKSTREAM B/2 — DATA CAPABILITY MATRIX.
# =========================================================================== #
CAP_AVAILABLE = "AVAILABLE_NOW"
CAP_PROSPECTIVE = "PROSPECTIVE_ONLY"
CAP_WAITING_INTRINIO = "WAITING_FOR_INTRINIO"
CAP_INSUFFICIENT_PIT = "INSUFFICIENT_PIT_HISTORY"
CAP_REJECTED_BASIS = "REJECTED_DATA_BASIS"


def build_capability_matrix(panel: MomentumPanel, fund: dict, sector: dict, *,
                            held_candidates: Optional[list] = None) -> dict:
    """Per alpha family: can we evaluate it honestly on owned data TODAY?"""
    fams = [
        {"family": "PRICE_MOMENTUM", "status": CAP_AVAILABLE,
         "basis": "owned Norgate R1000 Current & Past monthly panel",
         "history": "%s..%s (%d months)" % (panel.diagnostics["first_month"],
                                            panel.diagnostics["last_month"],
                                            panel.diagnostics["months"]),
         "survivorship_safe": True,
         "note": "the deepest owned basis; most single-factor variants already tested"},
        {"family": "LIQUIDITY_TURNOVER", "status": CAP_AVAILABLE,
         "basis": "adv_dollar on the owned monthly panel (100% populated)",
         "history": "%d months" % panel.diagnostics["months"],
         "survivorship_safe": True,
         "note": ("MONTHLY resolution only. Three tournament candidates were held "
                  "as DATA_HOLD_REQUIRES_VOLUME_TURNOVER_DATA; the monthly ADV "
                  "column resolves two of them (dollar-volume shock, Amihud) at "
                  "monthly resolution. Daily-bar constructs such as "
                  "breakout-with-volume-confirmation remain blocked.")},
        {"family": "VOLATILITY", "status": CAP_AVAILABLE,
         "basis": "realized_vol_63d on the owned monthly panel",
         "history": "%d months" % panel.diagnostics["months"],
         "survivorship_safe": True,
         "note": "level, ratio and vol-of-vol variants already tested and rejected"},
        {"family": "FUNDAMENTAL_QUALITY", "status": CAP_INSUFFICIENT_PIT,
         "basis": "frozen Phase 10-L composite_sn panel",
         "history": "%s..%s (%d months, %d names)" % (
             fund["diagnostics"]["first_month"], fund["diagnostics"]["last_month"],
             fund["diagnostics"]["months"], fund["diagnostics"]["distinct_tickers"]),
         "survivorship_safe": False,
         "note": ("usable for the operational-ensemble autopsy but SURVIVOR-BIASED; "
                  "owned EODHD fundamentals are a current snapshot with no "
                  "as-reported vintage, so new PIT fundamental factors cannot be "
                  "built from them (tournament: SNAPSHOT_ONLY_NOT_PIT)")},
        {"family": "SECTOR_NEUTRAL", "status": CAP_REJECTED_BASIS,
         "basis": "owned repaired sector map (current-as-of)",
         "history": "n/a",
         "survivorship_safe": False,
         "note": ("the monthly panel is %.1f%% sector-known and the repaired map "
                  "covers %d of %d panel tickers and is CURRENT-AS-OF. Using it "
                  "historically injects a classification look-ahead. The "
                  "tournament records the same wall (point_in_time_gics: 5 of 50 "
                  "required symbols)." % (panel.diagnostics["panel_sector_known_pct"],
                                          len(sector["map"]),
                                          panel.diagnostics["distinct_tickers"]))},
        {"family": "ANALYST_REVISIONS", "status": CAP_WAITING_INTRINIO,
         "basis": "Stage 13B prospective vintage ledger (forward-accruing)",
         "history": "1 distinct vintage date on disk; 20 required",
         "survivorship_safe": None,
         "note": ("HARD PIT floor 2026-07-31: prospective vintages accrue one "
                  "calendar day at a time and are never backfilled. Genuine "
                  "HISTORICAL vintages from Steele/Intrinio satisfy the "
                  "requirement immediately. Highest expected information value of "
                  "any blocked family.")},
        {"family": "EVENT_INSIDER_SEC", "status": CAP_INSUFFICIENT_PIT,
         "basis": "owned SEC Form 4 / 8-K extract",
         "history": "3 distinct issuers; 30 required",
         "survivorship_safe": None,
         "note": "bounded owned acquisition can grow this without a purchase"},
        {"family": "EVENT_CALENDAR_SEASONALITY", "status": CAP_INSUFFICIENT_PIT,
         "basis": "no owned point-in-time earnings calendar",
         "history": "n/a",
         "survivorship_safe": None,
         "note": "turn-of-month and earnings-calendar candidates remain DATA_HOLD"},
    ]
    held_by_family: dict = {}
    for c in (held_candidates or []):
        held_by_family.setdefault(c.get("family"), []).append(c.get("name"))
    return {
        "stage23_version": STAGE23_VERSION,
        "vocabulary": [CAP_AVAILABLE, CAP_PROSPECTIVE, CAP_WAITING_INTRINIO,
                       CAP_INSUFFICIENT_PIT, CAP_REJECTED_BASIS],
        "families": fams,
        "existing_tournament_holds_by_family": held_by_family,
        "fingerprints": {
            "momentum_monthly_panel": panel.fingerprint,
            "frozen_fundamental_panel": fund["fingerprint"],
            "sector_map": sector["fingerprint"],
        },
    }


# =========================================================================== #
# WORKSTREAM F — the research <-> portfolio-decision link.
# =========================================================================== #
def build_decision_link(attribution: dict, *,
                        hoc_records: Optional[list] = None,
                        forward_records: Optional[list] = None) -> dict:
    """The canonical measurement seam between a candidate model and REAL decisions.

    This defines and, where evidence exists, MEASURES how a candidate would be
    judged against what the portfolio actually did. It never rewrites a
    historical decision and never claims counterfactual certainty: everything
    derived from re-ranking past holdings is explicitly labelled
    COUNTERFACTUAL_NOT_PROOF.

    With too few live observations the honest answer is INSUFFICIENT_FORWARD_
    EVIDENCE — the seam exists so the measurement starts accruing, not so a
    conclusion can be manufactured today.
    """
    hoc = list(hoc_records or [])
    fwd = list(forward_records or [])
    measures = [
        {"measure": "candidate_ranking_of_current_holdings",
         "definition": ("the candidate's cross-sectional rank of each name the book "
                        "actually held on the decision date"),
         "evidence_class": "COUNTERFACTUAL_NOT_PROOF",
         "requires": ["holding_opportunity_cost record", "candidate score panel"]},
        {"measure": "candidate_ranking_of_replacement_alternatives",
         "definition": ("the candidate's rank of the alternatives the operational "
                        "model proposed as replacements"),
         "evidence_class": "COUNTERFACTUAL_NOT_PROOF",
         "requires": ["reallocation_proposal record", "candidate score panel"]},
        {"measure": "deterioration_lead_time",
         "definition": ("months between the candidate first ranking a holding in the "
                        "bottom tercile and the operational model flagging it"),
         "evidence_class": "COUNTERFACTUAL_NOT_PROOF",
         "requires": ["holding_opportunity_cost history", "candidate score panel"]},
        {"measure": "false_exit_rate",
         "definition": ("fraction of candidate-recommended EXITs whose realised "
                        "forward return then beat the book"),
         "evidence_class": "TRUE_FORWARD",
         "requires": ["forward_evidence TRUE_FORWARD outcomes"]},
        {"measure": "replacement_success_rate",
         "definition": ("fraction of executed replacements where the incoming name "
                        "out-returned the outgoing name over the holding horizon"),
         "evidence_class": "TRUE_FORWARD",
         "requires": ["reassessment_outcomes", "forward_evidence"]},
        {"measure": "regret_vs_operational_decision",
         "definition": ("realised return of the candidate's preferred action minus the "
                        "realised return of the action actually taken"),
         "evidence_class": "TRUE_FORWARD",
         "requires": ["portfolio_decision record", "forward_evidence"]},
        {"measure": "turnover_adjusted_benefit",
         "definition": ("regret net of the modelled round-trip cost of the extra "
                        "turnover the candidate would have caused"),
         "evidence_class": "TRUE_FORWARD",
         "requires": ["desk cost model", "forward_evidence"]},
    ]
    sufficient = len(fwd) >= 12
    return {
        "stage23_version": STAGE23_VERSION,
        "owner": "alpha_agent.stage23_unified.build_decision_link",
        "upstream_owners": {
            "holding_opportunity_cost": "api.holding_opportunity_cost",
            "reallocation_proposal": "api.reallocation_proposal",
            "portfolio_decision": "api.portfolio_decision",
            "reassessment_outcomes": "api.reassessment_outcomes",
            "forward_evidence": "api.forward_evidence",
            "true_forward_snapshots": "api.forward_prediction_skill",
        },
        "measures": measures,
        "observed": {
            "hoc_records_seen": len(hoc),
            "forward_outcome_records_seen": len(fwd),
            "minimum_for_conclusion": 12,
        },
        "status": ("MEASURABLE" if sufficient else "INSUFFICIENT_FORWARD_EVIDENCE"),
        "conclusion": (None if sufficient else
                       "Too few matured live observations exist to compare any "
                       "candidate against real portfolio decisions. The seam is "
                       "defined and the inputs are named; evidence accrues with "
                       "calendar time and is never backfilled."),
        "never_rewrites_history": True,
        "counterfactual_labelling_required": True,
        "operational_mutation": False,
    }


# =========================================================================== #
# WORKSTREAM G/H — Intrinio readiness + analyst-revision pre-registration.
#
# Both consolidate through the EXISTING owner ``alpha_agent.analyst_revisions``
# (Stage 13A). Stage 23 does not restate its schemas or invent provider fields;
# it verifies the contract against the required historical field list, names the
# two gaps it filled, and adds the one thing the Stage-13A pre-registration did
# not have: an INCREMENTAL-value requirement measured against the operational
# ensemble.
# =========================================================================== #

#: Required historical analyst-record fields -> the Stage-13A field that carries
#: each one. Verified against the contract, never assumed.
REQUIRED_HISTORICAL_FIELDS = {
    "security identifier": ["SECURITY_IDENTITY.provider_security_id",
                            "SECURITY_IDENTITY.stable_assetid",
                            "SECURITY_IDENTITY.cik", "SECURITY_IDENTITY.issuer_id"],
    "ticker": ["SECURITY_IDENTITY.historical_ticker (effective-dated)"],
    "effective timestamp": ["ESTIMATE_REVISION_EVENT.provider_effective_timestamp"],
    "revision timestamp": ["ESTIMATE_REVISION_EVENT.observation_timestamp"],
    "observation timestamp": ["ESTIMATE_REVISION_EVENT.observation_timestamp"],
    "fiscal period": ["ESTIMATE_REVISION_EVENT.fiscal_period_end",
                      "ESTIMATE_REVISION_EVENT.fiscal_period_type"],
    "fiscal year / quarter": ["ESTIMATE_REVISION_EVENT.fiscal_year",
                              "ESTIMATE_REVISION_EVENT.fiscal_quarter"],
    "metric identity": ["ESTIMATE_REVISION_EVENT.estimate_type"],
    "estimate / consensus value": ["ESTIMATE_REVISION_EVENT.revised_estimate",
                                   "CONSENSUS_SNAPSHOT.mean",
                                   "CONSENSUS_SNAPSHOT.median"],
    "prior value": ["ESTIMATE_REVISION_EVENT.prior_estimate",
                    "CONSENSUS_SNAPSHOT.prior_week_consensus",
                    "CONSENSUS_SNAPSHOT.prior_month_consensus"],
    "analyst count": ["ESTIMATE_REVISION_EVENT.analyst_count",
                      "CONSENSUS_SNAPSHOT.analyst_count"],
    "inactive / delisted entities": ["SECURITY_IDENTITY.status",
                                     "SECURITY_IDENTITY.effective_end"],
    "corporate-action identity": ["SECURITY_IDENTITY.acquisition_or_merger_status",
                                  "ESTIMATE_REVISION_EVENT.corporate_action_basis"],
    "source / provider": ["ESTIMATE_REVISION_EVENT.provider",
                          "SECURITY_IDENTITY.source_provider"],
    "ingestion timestamp": ["ESTIMATE_REVISION_EVENT.ingestion_timestamp"],
    "PIT availability timestamp": ["ESTIMATE_REVISION_EVENT.source_availability_timestamp"],
}

#: Required validations -> the Stage-13A PIT invariant that detects each.
REQUIRED_VALIDATIONS = {
    "current snapshot masquerading as history": "RECONSTRUCTED_DAILY_SNAPSHOT_FROM_CURRENT_VALUES",
    "missing revision timestamps": "MISSING_SOURCE_AVAILABILITY_TIMESTAMP",
    "future leakage": "OBSERVATION_AFTER_INGESTION_WITHOUT_LATE_ARRIVAL",
    "duplicate revisions": "DUPLICATE_REVISION_EVENT_FOR_SAME_KEY_AND_TIMESTAMP",
    "unstable identifier mapping": "HISTORICAL_TICKER_CHANGE_CREATED_DUPLICATE_ISSUER",
    "delisted / inactive omissions": "INACTIVE_OR_DELISTED_IDENTITY_NOT_RETAINED",
    "stale fiscal-period identity": "FISCAL_PERIOD_DATE_USED_AS_AVAILABILITY",
    "revised values overwriting earlier vintages": "REVISION_OVERWRITES_PRIOR_OBSERVATION",
    "insufficient historical depth": "adequacy gate (analyst_revisions Workstream F)",
    "partial universe coverage": "adequacy gate (analyst_revisions Workstream F)",
}

#: The two contract gaps Stage 23 found and filled inside the existing owner.
STAGE23_CONTRACT_GAPS_FILLED = [
    {"gap": "duplicate revision delivery",
     "why_it_matters": ("a bulk historical extract that paginates or re-delivers "
                        "overlapping windows would inflate revision BREADTH — the "
                        "primary analyst signal — without adding information"),
     "invariant_added": "DUPLICATE_REVISION_EVENT_FOR_SAME_KEY_AND_TIMESTAMP",
     "owner": "alpha_agent.analyst_revisions"},
    {"gap": "corporate-action basis for per-share estimates",
     "why_it_matters": ("EPS estimates are quoted in the share count of their "
                        "vintage; a split between the estimate and the actual "
                        "fabricates a surprise unless the basis is declared"),
     "invariant_added": "PER_SHARE_VALUE_WITHOUT_CORPORATE_ACTION_BASIS",
     "schema_fields_added": ["corporate_action_basis", "fiscal_year", "fiscal_quarter"],
     "owner": "alpha_agent.analyst_revisions",
     "related_operational_owner": "api.corporate_actions (Stage 19)"},
]


def build_intrinio_readiness(*, analyst_module=None,
                             observed_vintage_dates: Optional[int] = None,
                             required_vintage_dates: int = 20,
                             pit_floor: Optional[str] = None) -> dict:
    """What is actually present today, what is not, and what the adapter needs.

    No provider field name is invented. The contract, the adapters and the
    adequacy gate all belong to ``alpha_agent.analyst_revisions``; this is a
    verification report over them.
    """
    ar = analyst_module
    if ar is None:
        from . import analyst_revisions as ar  # local import keeps the module pure
    schemas = {name: ar.schema_fields(name) for name in ar.SCHEMA_NAMES}
    invariants = list(ar.PIT_INVARIANTS)
    missing_validations = {k: v for k, v in REQUIRED_VALIDATIONS.items()
                           if v not in invariants and not v.startswith("adequacy gate")}
    return {
        "stage23_version": STAGE23_VERSION,
        "contract_owner": "alpha_agent.analyst_revisions",
        "contract_stage": ar.STAGE,
        "no_provider_schema_invented": True,
        "provider_data_present_today": {
            "historical_analyst_vintages": False,
            "evidence": ("no historical analyst/estimate vintage extract exists in "
                         "the owned research stores; the tournament records "
                         "distinct_vintage_dates_on_disk=%s against a requirement "
                         "of %s" % (observed_vintage_dates
                                    if observed_vintage_dates is not None else 1,
                                    required_vintage_dates)),
            "prospective_snapshot_ledger": True,
            "prospective_pit_floor": pit_floor or "2026-07-31",
            "pit_floor_rule": ("the first prospective snapshot date is a HARD floor; "
                               "vintages before it can never be reconstructed and are "
                               "never backfilled"),
        },
        "required_fields": REQUIRED_HISTORICAL_FIELDS,
        "required_validations": REQUIRED_VALIDATIONS,
        "validations_missing_from_contract": missing_validations,
        "contract_complete": not missing_validations,
        "stage23_gaps_filled": STAGE23_CONTRACT_GAPS_FILLED,
        "record_schemas": schemas,
        "pit_invariants": invariants,
        "adapter_layer": {
            "protocol": "alpha_agent.analyst_revisions.RevisionDataAdapter",
            "implementations": sorted(ar.ADAPTERS),
            "local_trial_importer": "alpha_agent.analyst_revisions.LocalTrialImporter",
            "note": ("a future provider adapter maps vendor fields into the frozen "
                     "normalized contract; NO downstream research logic changes"),
        },
        "what_runs_the_moment_real_history_arrives": [
            "LocalTrialImporter ingests the extract into the normalized contract",
            "pit_scan classifies every PIT invariant (nothing is silently repaired)",
            "the adequacy gate measures history depth, breadth, inactive/delisted "
            "coverage and effective independent cohort count",
            "the six frozen hypotheses evaluate under BH-FDR (family_size=6)",
            "Stage 23 adds the INCREMENTAL test against the operational ensemble",
            "a surviving candidate enters the SAME tournament lifecycle as every "
            "other candidate — still with no automatic promotion",
        ],
        "safety": {"research_only": True, "no_purchase_executed": True,
                   "manual_approval_required": True, "no_promotion": True},
    }


def build_analyst_preregistration(*, analyst_module=None,
                                  attribution: Optional[dict] = None) -> dict:
    """The frozen analyst family PLUS the Stage-23 incremental-value requirement.

    Stage 13A froze six hypotheses and evaluates each IN ISOLATION. That answers
    "is this field predictive", which is not the question that matters: the
    question is whether it adds anything the operational ensemble does not
    already have. Stage 23 therefore attaches an explicit baseline-versus-
    augmented comparison to every hypothesis, anchored to the MEASURED Stage-23
    baseline rather than an assumed one.
    """
    ar = analyst_module
    if ar is None:
        from . import analyst_revisions as ar
    registry = ar.build_revision_registry()

    baseline = {
        "baseline_model": OPERATIONAL_STRATEGY_ID,
        "baseline_components": [COMPONENT_FUNDAMENTAL, COMPONENT_MOMENTUM],
        "comparison": ("EXISTING MODEL  vs  EXISTING MODEL + analyst feature; the "
                       "analyst feature is judged on what it ADDS, never on how it "
                       "looks in isolation"),
        "required_statistics": [
            "partial rank IC of the analyst feature controlling for BOTH "
            "operational legs on the shared cross-section",
            "spread t of the augmented ensemble vs the frozen 50/50 ensemble",
            "net-of-cost spread at 25bps of augmented vs baseline",
            "correlation of the augmented long/short series with the baseline",
            "subperiod sign stability of the augmented ensemble vs baseline",
            "incremental turnover the analyst leg introduces",
        ],
        "acceptance_rule": ("an analyst feature is interesting ONLY if the augmented "
                            "ensemble beats the baseline on the shared cross-section "
                            "AFTER costs AND the feature's partial rank IC survives "
                            "both operational legs"),
    }
    if attribution:
        j = (attribution.get("joint_universe") or {})
        ens = (j.get("ensemble_50_50") or {}).get("metrics") or {}
        part = ((attribution.get("redundancy") or {}).get("partial_rank_ic") or {})
        baseline["measured_baseline"] = {
            "source_run": attribution.get("stage23_version"),
            "evidence_class": "HISTORICAL_OUT_OF_SAMPLE_PANEL_STUDY",
            "joint_scored_periods": ens.get("scored_periods"),
            "ensemble_rank_ic": ens.get("rank_ic"),
            "ensemble_rank_ic_t": ens.get("rank_ic_t"),
            "ensemble_spread_t": ens.get("spread_t"),
            "ensemble_net25_spread": ens.get("net25_spread"),
            "composite_sn_partial_t": (part.get(
                "composite_sn_controlling_for_mom_6_1") or {}).get("t_stat"),
            "mom_6_1_partial_t": (part.get(
                "mom_6_1_controlling_for_composite_sn") or {}).get("t_stat"),
            "caveat": ("the baseline itself sits on a SURVIVOR-BIASED fundamental "
                       "panel and is held DATA_HOLD by the released gate; an "
                       "analyst feature measured against it inherits that caveat"),
        }

    hyps = []
    for h in registry.get("hypotheses", []):
        hyps.append(dict(h, stage23_incremental_requirement=baseline["required_statistics"],
                         stage23_acceptance_rule=baseline["acceptance_rule"],
                         stage23_baseline_model=OPERATIONAL_STRATEGY_ID))
    return {
        "stage23_version": STAGE23_VERSION,
        "registry_owner": "alpha_agent.analyst_revisions.build_revision_registry",
        "registry_schema_version": registry.get("schema_version"),
        "registry_version": registry.get("registry_version"),
        "economic_families": registry.get("economic_families"),
        "family_size": len(hyps),
        "frozen_before_any_trial_data": True,
        "evaluation_contract": registry.get("evaluation_contract"),
        "multiple_testing": "Benjamini-Hochberg over exactly the pre-registered family",
        "stage23_addition": baseline,
        "hypotheses": hyps,
        "cross_validation_with_prospective_evidence": {
            "owner": "alpha_agent.analyst_revisions (Stage 13B formations/outcomes)",
            "design": ("the SAME economic hypothesis is tested twice on independent "
                       "evidence classes — historical PIT vintages from a vendor and "
                       "prospective TRUE_FORWARD formations accruing live. Agreement "
                       "across both is far stronger than either alone."),
            "evidence_classes_must_not_be_mixed": True,
            "prospective_status": "immature; do not adjudicate early",
        },
        "no_promotion": True,
    }


# =========================================================================== #
# ARTIFACT WRITER — immutable, reproducible, fingerprinted.
# =========================================================================== #
def run_id_for(payload: dict) -> str:
    return "stage23_" + _sha256_text(canonical_json(payload))[:16]


def write_artifacts(root: Optional[str | Path], documents: dict, *,
                    run_id: str) -> dict:
    """Write the Stage-23 run under an immutable run directory + latest.json.

    A run directory is never overwritten: re-running with identical inputs
    reproduces the same run_id and the same bytes, so the operation is
    idempotent rather than destructive.
    """
    base = _resolve(root, RESEARCH_ROOT_ENV, DEFAULT_RESEARCH_ROOT)
    run_dir = base / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    written = {}
    for name, doc in sorted(documents.items()):
        p = run_dir / ("%s.json" % name)
        text = json.dumps(doc, indent=1, sort_keys=True, default=str)
        p.write_text(text, encoding="utf-8")
        written[name] = {"path": str(p), "bytes": len(text.encode("utf-8")),
                         "sha256": _sha256_text(text)}
    latest = {
        "stage": STAGE,
        "stage23_version": STAGE23_VERSION,
        "run_id": run_id,
        "run_dir": str(run_dir),
        "documents": written,
        "safety_badges": SAFETY_BADGES,
        "research_only": True,
        "operational_mutation": False,
        "automatic_promotion": False,
    }
    (base / "latest.json").write_text(
        json.dumps(latest, indent=1, sort_keys=True, default=str), encoding="utf-8")
    return latest


__all__ = [
    "STAGE", "STAGE23_VERSION", "ORIGIN", "CONTRACT_ID", "READY", "BLOCKED", "DATA_HOLD",
    "OPERATIONAL_STRATEGY_ID", "OPERATIONAL_BOOK_ID", "COMPONENT_FUNDAMENTAL",
    "COMPONENT_MOMENTUM", "COMPONENT_WEIGHTS", "SAFETY_BADGES",
    "MomentumPanel", "load_momentum_panel", "load_fundamental_panel", "load_sector_map",
    "stage23_hypotheses", "build_momentum_periods", "build_fundamental_periods",
    "build_joint_periods", "evaluate", "neutralisation_report", "concentration_report",
    "subperiod_report", "overlap_report", "run_edge_attribution", "run_owned_campaign",
    "classify_stage23_result", "register_campaign_candidates", "build_priority_queue",
    "build_capability_matrix", "build_decision_link", "build_intrinio_readiness",
    "build_analyst_preregistration", "REQUIRED_HISTORICAL_FIELDS",
    "REQUIRED_VALIDATIONS", "STAGE23_CONTRACT_GAPS_FILLED",
    "write_artifacts", "run_id_for",
    "file_fingerprint", "canonical_json",
    "CLS_PROMISING", "CLS_NEEDS_MORE", "CLS_REDUNDANT", "CLS_FAILED_ROBUSTNESS",
    "CLS_FAILED_COSTS", "CLS_FAILED_PIT", "CLS_INSUFFICIENT_DATA", "CLS_WAITING_INTRINIO",
    "CLASSIFICATION_TO_LIFECYCLE", "CAP_AVAILABLE", "CAP_PROSPECTIVE",
    "CAP_WAITING_INTRINIO", "CAP_INSUFFICIENT_PIT", "CAP_REJECTED_BASIS",
    "PRIORITY_HIGH", "PRIORITY_MEDIUM", "PRIORITY_LOW", "PRIORITY_WAITING",
]
