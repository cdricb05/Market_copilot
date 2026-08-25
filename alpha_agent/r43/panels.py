"""alpha_agent.r43.panels - READ-ONLY loaders for everything the estate owns.

Release 43 acquires no market data it does not already own except through
:mod:`alpha_agent.r43.acquisition`. Everything here opens bytes that earlier
releases legitimately acquired, under their own PIT rules, and never writes
into another release's root:

* 105 native dated futures markets (Norgate, entitled) via the R41 curve
  store - roll-aware tenor returns, calendar slopes, open interest, volume,
  days-to-expiry, contract identity;
* the R38 futures market registry - asset class, economic group, exchange,
  point value and the exchange's CURRENT margin (used only as a sanity
  check on the contract's declared conservative margin fractions, never as
  a point-in-time margin);
* the FRED daily panel R41 acquired - Treasury CMT curve, ICE BofA credit
  OAS by rating and maturity, EUR HY/IG OAS, EM OAS, breakevens, real
  yields, the broad dollar index, VIX/OVX/GVZ and the overnight rates;
* the CBOE volatility index histories (VIX term structure, VVIX, SKEW);
* official government yield curves (ECB AAA, JGB, BoC, RBA).

Every loader is cached in-process and returns a plain pandas object with a
naive daily DatetimeIndex, because mixing tz-aware and naive indices across
thirteen lanes is how alignment bugs become "alpha".
"""
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from . import R38_RESEARCH_ROOT, R41_RESEARCH_ROOT
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r43.panels"

CURVE_STORE = R41_RESEARCH_ROOT / "_data_curves"
FRED_PANEL = R41_RESEARCH_ROOT / "_data_fred" / "fred_daily_panel.csv"
CBOE_DIR = R41_RESEARCH_ROOT / "_data_cboe"
GOV_DIR = R41_RESEARCH_ROOT / "_data_curves_gov"
MARKET_REGISTRY = R38_RESEARCH_ROOT / "futures_market_registry.json"

#: R38 economic groups that are not literal keys of COST_BPS_PER_SIDE.
_GROUP_ALIAS = {
    "INTL_INDEX_FUTURES_EMERGING": "INTL_INDEX_FUTURES",
    "US_INDEX_FUTURES_MICRO": "US_INDEX_FUTURES",
    "CRYPTO_FUTURES_MICRO": "CRYPTO_FUTURES",
}


def cost_group(economic_group: str) -> str:
    g = _GROUP_ALIAS.get(economic_group, economic_group)
    return g if g in C.COST_BPS_PER_SIDE else "COMMODITY_INDEX"


# --------------------------------------------------------------------------- #
# Futures
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def market_registry() -> dict:
    body = json.loads(MARKET_REGISTRY.read_text(encoding="utf-8"))
    out = {}
    for m, v in (body.get("markets") or {}).items():
        meta = v.get("metadata") or {}
        out[m] = {
            "market": m,
            "name": v.get("market_name"),
            "asset_class": v.get("asset_class"),
            "economic_group": v.get("economic_group"),
            "cost_group": cost_group(v.get("economic_group") or ""),
            "exchange": meta.get("exchange"),
            "point_value": meta.get("point_value"),
            "current_margin_usd": meta.get("margin"),
            "first_quoted_date": v.get("first_quoted_date"),
            "activity_state": v.get("activity_state"),
        }
    return out


@lru_cache(maxsize=1)
def available_markets() -> tuple:
    if not CURVE_STORE.exists():
        return tuple()
    return tuple(sorted(p.name[:-len("_daily.csv")]
                        for p in CURVE_STORE.glob("*_daily.csv")))


@lru_cache(maxsize=256)
def futures_daily(market: str):
    """Roll-aware daily research series for ONE market (R41 curve store).

    Columns: ret1/ret2/ret3 (daily return of holding tenor k under the frozen
    R38 roll rule - a roll books no P&L), c1..c3, slope_ann (annualised
    front/second calendar slope), slope23_ann, oi1..oi3, v1/v2, dte1.
    """
    p = CURVE_STORE / ("%s_daily.csv" % market)
    if not p.exists():
        return None
    df = pd.read_csv(p, index_col=0, parse_dates=True)
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    return df[~df.index.duplicated(keep="last")].sort_index()


@lru_cache(maxsize=64)
def futures_panel(market: str):
    """The full tenor panel (c1..c8, oi, volume, dte, contract identity)."""
    p = CURVE_STORE / ("%s_panel.csv" % market)
    if not p.exists():
        return None
    df = pd.read_csv(p, index_col=0, parse_dates=True)
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    return df[~df.index.duplicated(keep="last")].sort_index()


def markets_by(asset_class: str = None, economic_group: str = None) -> list:
    reg = market_registry()
    have = set(available_markets())
    out = []
    for m, v in reg.items():
        if m not in have:
            continue
        if asset_class and v["asset_class"] != asset_class:
            continue
        if economic_group and v["economic_group"] != economic_group:
            continue
        out.append(m)
    return sorted(out)


def field_frame(markets, column: str, *, min_obs: int = 250) -> pd.DataFrame:
    """One wide frame: dates x markets for ``column`` of the daily series."""
    cols = {}
    for m in markets:
        d = futures_daily(m)
        if d is None or column not in d.columns:
            continue
        s = pd.to_numeric(d[column], errors="coerce")
        if s.notna().sum() >= min_obs:
            cols[m] = s
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(cols).sort_index()


def margin_sanity() -> dict:
    """Compare the contract's DECLARED conservative margin fractions with the
    exchange's CURRENT posted margin / current notional.

    The exchange table has no vintage, so it can never be a point-in-time
    input. It is used here for one purpose only: to demonstrate that the
    declared fractions are CONSERVATIVE (higher), so no candidate is
    flattered by an optimistic denominator.
    """
    reg = market_registry()
    rows = []
    for m in available_markets():
        v = reg.get(m)
        d = futures_daily(m)
        if not v or d is None or "c1" not in d.columns:
            continue
        px = pd.to_numeric(d["c1"], errors="coerce").dropna()
        pv, mg = v.get("point_value"), v.get("current_margin_usd")
        if px.empty or not pv or not mg:
            continue
        notional = float(px.iloc[-1]) * float(pv)
        if notional <= 0:
            continue
        observed = float(mg) / notional
        declared = C.FUTURES_MARGIN_FRACTION.get(
            v["cost_group"], max(C.FUTURES_MARGIN_FRACTION.values()))
        rows.append({"market": m, "economic_group": v["economic_group"],
                     "cost_group": v["cost_group"],
                     "observed_current_margin_fraction": observed,
                     "declared_margin_fraction": declared,
                     "declared_is_conservative": declared >= observed})
    n = len(rows)
    n_ok = sum(1 for r in rows if r["declared_is_conservative"])
    return {"n_markets_compared": n, "n_declared_conservative": n_ok,
            "fraction_conservative": (n_ok / n) if n else None,
            "source": "R38 futures_market_registry metadata.margin",
            "pit_state": "CURRENT_ONLY_NOT_POINT_IN_TIME",
            "used_as": "sanity check on the declared fractions only",
            "rows": sorted(rows, key=lambda r: -r[
                "observed_current_margin_fraction"])}


# --------------------------------------------------------------------------- #
# Macro / credit / volatility
# --------------------------------------------------------------------------- #
@lru_cache(maxsize=1)
def fred_panel() -> pd.DataFrame:
    df = pd.read_csv(FRED_PANEL, index_col=0, parse_dates=True)
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    return df.sort_index().apply(pd.to_numeric, errors="coerce")


CREDIT_OAS = ("OAS_IG", "OAS_HY", "OAS_AAA", "OAS_AA", "OAS_A", "OAS_BBB",
              "OAS_BB", "OAS_B", "OAS_CCC", "OAS_EM", "OAS_EUR_HY",
              "OAS_EUR_IG")
CREDIT_TERM = ("OAS_IG_1_3", "OAS_IG_3_5", "OAS_IG_5_7", "OAS_IG_7_10",
               "OAS_IG_10_15", "OAS_IG_15P")
CMT = ("CMT_3M", "CMT_1Y", "CMT_2Y", "CMT_5Y", "CMT_7Y", "CMT_10Y",
       "CMT_20Y", "CMT_30Y")


@lru_cache(maxsize=1)
def cboe_panel() -> pd.DataFrame:
    """VIX complex closes (VIX, VIX9D, VIX3M, VIX6M, VIX1D, VVIX, SKEW)."""
    frames = {}
    for p in sorted(CBOE_DIR.glob("*_History.csv")):
        name = p.name.split("_History")[0]
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        dcol = next((c for c in df.columns
                     if str(c).strip().upper() in ("DATE", "TRADE_DATE")),
                    df.columns[0])
        ccol = next((c for c in df.columns
                     if str(c).strip().upper() in ("CLOSE", name.upper(),
                                                   "VIX CLOSE", "SKEW")),
                    df.columns[-1])
        s = pd.Series(pd.to_numeric(df[ccol], errors="coerce").to_numpy(),
                      index=pd.to_datetime(df[dcol], errors="coerce"))
        s = s[s.index.notna()]
        s.index = pd.DatetimeIndex(s.index).tz_localize(None).normalize()
        frames[name] = s[~s.index.duplicated(keep="last")].sort_index()
    return pd.DataFrame(frames).sort_index() if frames else pd.DataFrame()


@lru_cache(maxsize=1)
def gov_curves() -> dict:
    """Official published government yield curves, as acquired by R41."""
    out = {}
    for key, fname in (("ECB_AAA", "ecb_aaa_spot_curve.csv"),
                       ("JGB", "mof_jgb_yields.csv"),
                       ("BOC", "boc_benchmark_yields.csv"),
                       ("RBA", "rba_f2_raw.csv")):
        p = GOV_DIR / fname
        if not p.exists():
            continue
        try:
            df = pd.read_csv(p, index_col=0, parse_dates=True)
            df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
            out[key] = df.sort_index()
        except Exception:
            continue
    return out


# --------------------------------------------------------------------------- #
# Alignment helpers
# --------------------------------------------------------------------------- #
def align(*frames, how: str = "inner") -> list:
    """Align frames/series on their common daily index (no interpolation)."""
    idx = None
    for f in frames:
        i = pd.DatetimeIndex(f.index)
        idx = i if idx is None else (idx.intersection(i) if how == "inner"
                                     else idx.union(i))
    idx = idx.sort_values()
    return [f.reindex(idx) for f in frames]


def zscore(x: pd.Series, win: int, *, min_periods: int = None) -> pd.Series:
    mp = min_periods or max(20, win // 3)
    mu = x.rolling(win, min_periods=mp).mean()
    sd = x.rolling(win, min_periods=mp).std()
    return ((x - mu) / sd.replace(0.0, np.nan)).replace(
        [np.inf, -np.inf], np.nan)


def inventory() -> dict:
    """The measured owned-data inventory this release actually reads."""
    reg = market_registry()
    have = available_markets()
    by_class = {}
    for m in have:
        k = (reg.get(m) or {}).get("asset_class") or "UNKNOWN"
        by_class[k] = by_class.get(k, 0) + 1
    fp = fred_panel()
    cb = cboe_panel()
    gv = gov_curves()
    spans = {}
    for m in have[:]:
        d = futures_daily(m)
        if d is not None and len(d):
            spans[m] = (str(d.index[0].date()), str(d.index[-1].date()))
    firsts = sorted(v[0] for v in spans.values())
    return {
        "calculation_owner": CALCULATION_OWNER,
        "futures_markets": len(have),
        "futures_by_asset_class": by_class,
        "futures_earliest": firsts[0] if firsts else None,
        "futures_latest": max(v[1] for v in spans.values()) if spans else None,
        "fred_series": len(fp.columns),
        "fred_span": (str(fp.index[0].date()), str(fp.index[-1].date())),
        "credit_oas_series": [c for c in CREDIT_OAS if c in fp.columns],
        "credit_term_series": [c for c in CREDIT_TERM if c in fp.columns],
        "cmt_series": [c for c in CMT if c in fp.columns],
        "cboe_series": list(cb.columns),
        "cboe_span": ((str(cb.index[0].date()), str(cb.index[-1].date()))
                      if len(cb) else None),
        "gov_curves": {k: [str(v.index[0].date()), str(v.index[-1].date())]
                       for k, v in gv.items()},
        "all_read_only": True,
        "written_to_prior_release_roots": False,
    }
