"""alpha_agent.r38.experiments - Phases 6-14: the frozen native campaign.

Executes exactly the configurations frozen in
``contract.FROZEN_PRIMARY_CONFIGURATIONS`` - no optimizer, no grid, no
result-driven expansion. Every executed configuration enters the
multiple-testing denominator; a configuration that cannot execute is recorded
with its reason and stays in the registry.

Reuse, never reinvention: the economic judge, traded-notional cost model and
volatility-matched control are :mod:`alpha_agent.r34.economics`; the
Benjamini-Hochberg machinery is :mod:`alpha_agent.r31.multiple_testing`; the
minimum detectable effect is
:func:`alpha_agent.r36.experiments.minimum_detectable_excess`; the CFTC COT
parser and its publication lag are :mod:`alpha_agent.r35.information`.

Alignment rule (leak-free by construction): the return labelled at decision
date d is realised strictly AFTER d - over (d, d+cadence] - and every signal
at d is computed from data dated <= d.
"""
from __future__ import annotations

import datetime as _dt
import math
from pathlib import Path
from typing import Callable, Optional

from .. import r38
from ..r31 import multiple_testing as _mt
from ..r34 import economics as _econ
from ..r35 import contract as _r35_contract
from ..r35 import information as _r35_info
from ..r36 import experiments as _r36_experiments
from . import contract as C
from . import enumeration as EN
from . import research_layer as RL

CALCULATION_OWNER = "alpha_agent.r38.experiments"
REGISTRY_SCHEMA = "r38_native_futures_experiment_registry/1"
ECONOMICS_SCHEMA = "r38_native_futures_economics/1"
MT_SCHEMA = "r38_multiple_testing_results/1"

#: Declared futures-root -> CFTC contract-market code mapping (legacy codes),
#: written from the CFTC's own code tables before any outcome was seen.
COT_CODE_MAPPING = {
    "GC": ("088691",), "SI": ("084691",), "HG": ("085692",),
    "PL": ("076651",), "PA": ("075651",),
    "CL": ("067651",), "HO": ("022651",), "RB": ("111659",),
    "NG": ("023651",),
    "ZC": ("002602",), "ZW": ("001602",), "KE": ("001612",),
    "MWE": ("001626",), "ZS": ("005602",), "ZM": ("026603",),
    "ZL": ("007601",), "ZO": ("004603",), "ZR": ("039601",),
    "KC": ("083731",), "SB": ("080732",), "CC": ("073732",),
    "CT": ("033661",), "OJ": ("040701",),
    "LE": ("057642",), "GF": ("061641",), "HE": ("054642",),
    "DC": ("052641",), "LBR": ("058643",),
}
R35_COT_DIR = Path(r"D:\Stock_Prediction_app_data\orthogonal_information_r35"
                   r"\acquired\cftc_commitments_of_traders")

MIN_DAILY_OBS_FOR_SIGNAL = 252


def _np():
    import numpy as np
    return np


def _pd():
    import pandas as pd
    return pd


# --------------------------------------------------------------------------- #
# Universe rules - the frozen text of each configuration, in code
# --------------------------------------------------------------------------- #
def _markets_by(registry: dict, *, asset_class=None, groups=None,
                exclude_groups=(), min_history_years=0.0) -> list:
    out = []
    for market, row in registry["markets"].items():
        if market in C.DUPLICATE_UNDERLYING_EXCLUSIONS:
            continue
        if asset_class is not None and row["asset_class"] != asset_class:
            continue
        if groups is not None and row["economic_group"] not in groups:
            continue
        if row["economic_group"] in exclude_groups:
            continue
        fq = row.get("first_quoted_date")
        if min_history_years and fq:
            first = _dt.date.fromisoformat(str(fq)[:10])
            years = (_dt.date.today() - first).days / 365.25
            if years < min_history_years:
                continue
        out.append(market)
    return sorted(out)


def config_universe(name: str, registry: dict) -> dict:
    """The frozen universe rule for one configuration, applied to delivered
    metadata. Returns {"markets": [...], "floor": int}."""
    commodity_xs = _markets_by(
        registry, asset_class="COMMODITY",
        exclude_groups=C.COMMODITY_INDEX_GROUPS_EXCLUDED_FROM_XS,
        min_history_years=3.0)
    if name == "CMDTY_XS_MOMENTUM_12_1":
        return {"markets": commodity_xs, "floor": 6}
    if name == "CMDTY_XS_CARRY":
        return {"markets": commodity_xs, "floor": 6}
    if name == "CMDTY_TS_TREND_12M":
        return {"markets": commodity_xs, "floor": 1}
    if name == "CMDTY_SEASONALITY":
        markets = _markets_by(
            registry, asset_class="COMMODITY",
            groups=("GRAINS_AND_OILSEEDS", "SOFTS", "LIVESTOCK", "ENERGY"),
            min_history_years=6.0)
        return {"markets": markets, "floor": 6}
    if name == "CMDTY_COT_HEDGING_PRESSURE":
        markets = [m for m in commodity_xs if m in COT_CODE_MAPPING]
        return {"markets": markets, "floor": 6}
    if name == "CMDTY_CALENDAR_SPREAD_MR":
        markets = _markets_by(
            registry, asset_class="COMMODITY",
            groups=("ENERGY", "GRAINS_AND_OILSEEDS", "SOFTS"),
            min_history_years=3.0)
        return {"markets": markets, "floor": 4}
    if name in ("RATES_TS_TREND", "RATES_XS_CURVE_CARRY"):
        markets = _markets_by(registry, asset_class="RATES",
                              groups=("TREASURY_FUTURES",))
        return {"markets": markets, "floor": 3}
    if name in ("VX_TERM_STRUCTURE_CARRY", "VX_CALENDAR_SLOPE_MR"):
        return {"markets": ["VX"], "floor": 1}
    if name in ("INTL_IDX_XS_MOMENTUM", "INTL_IDX_TS_TREND"):
        markets = _markets_by(
            registry, asset_class="INTERNATIONAL_EQUITY",
            groups=("INTL_INDEX_FUTURES", "INTL_INDEX_FUTURES_EMERGING"),
            min_history_years=3.0)
        floor = 4 if name == "INTL_IDX_XS_MOMENTUM" else 1
        return {"markets": markets, "floor": floor}
    if name == "FX_FUT_CARRY_IMPLEMENTATION":
        markets = [m for m in _markets_by(registry, asset_class="FX")
                   if m.startswith("6")]  # currency pairs vs USD, not DX
        return {"markets": markets, "floor": 5}
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Panel assembly
# --------------------------------------------------------------------------- #
def load_panel(markets: list, campaign_id: str = C.CAMPAIGN_ID) -> dict:
    panel = {}
    for market in sorted(set(markets)):
        series = RL.load_series(market, campaign_id)
        if series is not None and len(series):
            panel[market] = series
    return panel


def decision_calendar(panel: dict, cadence: int):
    pd = _pd()
    union = None
    for df in panel.values():
        union = df.index if union is None else union.union(df.index)
    if union is None or not len(union):
        return pd.DatetimeIndex([])
    return union[::cadence]


def forward_return_matrix(panel: dict, decisions, *, column: str = "ret"):
    pd = _pd()
    out = {}
    for market, df in panel.items():
        if column == "ret":
            daily = df
        else:
            daily = df[[column]].rename(columns={column: "ret"})
        out[market] = RL.period_returns(daily, decisions)
    return pd.DataFrame(out)


# --------------------------------------------------------------------------- #
# Signals (every value at decision date d uses data dated <= d)
# --------------------------------------------------------------------------- #
def _compound(ret_window):
    np = _np()
    window = ret_window.dropna()
    if not len(window):
        return float("nan")
    return float(np.prod(1.0 + window.to_numpy()) - 1.0)


def signal_matrix(name: str, panel: dict, decisions):
    np, pd = _np(), _pd()
    out = {}
    for market, df in panel.items():
        ret = df["ret"]
        vals = {}
        if name in ("CMDTY_XS_MOMENTUM_12_1", "INTL_IDX_XS_MOMENTUM"):
            for d in decisions:
                hist = ret.loc[ret.index <= d]
                if len(hist) < MIN_DAILY_OBS_FOR_SIGNAL:
                    vals[d] = np.nan
                    continue
                window = hist.iloc[-C.MOMENTUM_LOOKBACK_SESSIONS:
                                   -C.MOMENTUM_SKIP_SESSIONS]
                vals[d] = _compound(window)
        elif name in ("CMDTY_TS_TREND_12M", "RATES_TS_TREND",
                      "INTL_IDX_TS_TREND"):
            for d in decisions:
                hist = ret.loc[ret.index <= d]
                if len(hist) < MIN_DAILY_OBS_FOR_SIGNAL:
                    vals[d] = np.nan
                    continue
                vals[d] = _compound(hist.iloc[-C.TREND_LOOKBACK_SESSIONS:])
        elif name in ("CMDTY_XS_CARRY", "RATES_XS_CURVE_CARRY",
                      "FX_FUT_CARRY_IMPLEMENTATION",
                      "VX_TERM_STRUCTURE_CARRY"):
            slope = df["slope_ann"].dropna()
            for d in decisions:
                hist = slope.loc[slope.index <= d]
                vals[d] = float(hist.iloc[-1]) if len(hist) else np.nan
        elif name in ("CMDTY_CALENDAR_SPREAD_MR", "VX_CALENDAR_SLOPE_MR"):
            slope = df["slope_ann"]
            mean = slope.rolling(C.SPREAD_Z_WINDOW_SESSIONS,
                                 min_periods=126).mean()
            std = slope.rolling(C.SPREAD_Z_WINDOW_SESSIONS,
                                min_periods=126).std()
            z = (slope - mean) / std
            z = z.dropna()
            for d in decisions:
                hist = z.loc[z.index <= d]
                vals[d] = float(hist.iloc[-1]) if len(hist) else np.nan
        elif name == "CMDTY_SEASONALITY":
            monthly = (1.0 + ret.fillna(0.0)).groupby(
                [ret.index.year, ret.index.month]).prod() - 1.0
            for k, d in enumerate(decisions):
                target = decisions[k + 1] if k + 1 < len(decisions) else None
                if target is None:
                    vals[d] = np.nan
                    continue
                month = target.month
                prior = monthly.loc[
                    [i for i in monthly.index
                     if i[1] == month and i[0] < d.year]]
                if len(prior) < C.SEASONALITY_MIN_PRIOR_YEARS:
                    vals[d] = np.nan
                    continue
                vals[d] = float(prior.mean())
        else:
            raise KeyError(name)
        out[market] = pd.Series(vals, dtype=float)
    return pd.DataFrame(out)


#: Parsed COT frame cache: the 41 deacot archives parse identically for every
#: caller in one process, and leave-one-market-out would otherwise re-parse
#: them once per dropped market.
_COT_LOAD_CACHE: dict = {}


def _load_cot_cached(files: dict, codes: tuple) -> dict:
    key = (tuple(sorted(str(p) for p in files.values())), tuple(sorted(codes)))
    if key not in _COT_LOAD_CACHE:
        _COT_LOAD_CACHE[key] = _r35_info.load_cot(files, codes=list(codes))
    return _COT_LOAD_CACHE[key]


def cot_signal_matrix(panel: dict, decisions) -> dict:
    """Hedging-pressure signal: MINUS the z-score of commercial net/OI.

    Parsing and publication-lag semantics are Release 35's owners; only the
    commercial aggregation (R35 exposes the speculator side) is computed here.
    """
    np, pd = _np(), _pd()
    files = {p.stem.replace("deacot", ""): p
             for p in sorted(R35_COT_DIR.glob("deacot*.zip"))}
    if not files:
        return {"ok": False, "reason": "R35_COT_ARCHIVE_ABSENT",
                "matrix": None}
    # Every code in the declared mapping loads once; per-universe filtering
    # happens on the parsed frame, so the cache key is stable across
    # leave-one-market-out reruns.
    codes = tuple(c for codes_ in COT_CODE_MAPPING.values() for c in codes_)
    loaded = _load_cot_cached(files, codes)
    if not loaded.get("ok"):
        return {"ok": False, "reason": loaded.get("reason"), "matrix": None}
    frame = loaded["frame"]
    lag = _r35_contract.COT_PUBLICATION_LAG_DAYS
    out = {}
    for market in panel:
        subset = frame[frame["code"].isin(COT_CODE_MAPPING.get(market, ()))]
        if subset.empty:
            continue
        grouped = subset.groupby("as_of").agg(
            open_interest=("open_interest", "sum"),
            comm_long=("comm_long", "sum"),
            comm_short=("comm_short", "sum"))
        grouped = grouped[grouped["open_interest"] > 0]
        net = ((grouped["comm_long"] - grouped["comm_short"])
               / grouped["open_interest"])
        net.index = net.index + pd.Timedelta(days=int(lag))
        mean = net.rolling(C.COT_Z_WINDOW_WEEKS, min_periods=52).mean()
        std = net.rolling(C.COT_Z_WINDOW_WEEKS, min_periods=52).std()
        z = ((net - mean) / std).dropna()
        vals = {}
        for d in decisions:
            hist = z.loc[z.index <= d]
            vals[d] = -float(hist.iloc[-1]) if len(hist) else np.nan
        out[market] = pd.Series(vals, dtype=float)
    matrix = pd.DataFrame(out)
    return {"ok": True, "matrix": matrix,
            "cot_rows": loaded["rows"], "years_read": loaded["years_read"],
            "mapped_markets": sorted(out),
            "publication_lag_days": int(lag)}


# --------------------------------------------------------------------------- #
# Weights
# --------------------------------------------------------------------------- #
def xs_thirds_weights(signals, forward):
    """Long top third / short bottom third, 0.5 gross per side."""
    np, pd = _np(), _pd()
    W = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
    for d in signals.index:
        row = signals.loc[d]
        live = row[row.notna() & forward.loc[d].notna()] \
            if d in forward.index else row[row.notna()]
        n = len(live)
        if n < 3:
            continue
        k = max(n // 3, 1)
        ranked = live.sort_values()
        losers = ranked.index[:k]
        winners = ranked.index[-k:]
        W.loc[d, winners] = 0.5 / k
        W.loc[d, losers] = -0.5 / k
    return W


def ts_vol_scaled_weights(signals, panel, decisions):
    """Sign of signal, inverse-vol sized, normalised to unit gross."""
    np, pd = _np(), _pd()
    vol = {}
    for market, df in panel.items():
        daily = df["ret"]
        v = {}
        for d in decisions:
            hist = daily.loc[daily.index <= d].dropna()
            if len(hist) < C.VOLATILITY_WINDOW_SESSIONS:
                v[d] = np.nan
                continue
            v[d] = float(hist.iloc[-C.VOLATILITY_WINDOW_SESSIONS:].std())
        vol[market] = pd.Series(v, dtype=float)
    V = pd.DataFrame(vol)
    W = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
    for d in signals.index:
        row = signals.loc[d]
        vols = V.loc[d] if d in V.index else None
        if vols is None:
            continue
        live = row[row.notna() & vols.notna() & (vols > 0)]
        if not len(live):
            continue
        raw = {m: (1.0 if live[m] >= 0 else -1.0) / vols[m] for m in live.index}
        gross = sum(abs(x) for x in raw.values())
        if gross <= 0:
            continue
        for m, x in raw.items():
            W.loc[d, m] = x / gross
    return W


def single_market_vol_target_weights(direction, panel, market, decisions):
    """|w| = min(1, target_vol / realised_vol), sign from ``direction``."""
    np, pd = _np(), _pd()
    daily = panel[market]["ret"]
    W = pd.DataFrame(0.0, index=direction.index, columns=[market])
    ann = math.sqrt(_econ.SESSIONS_PER_YEAR)
    for d in direction.index:
        sign = direction.loc[d]
        if not _np().isfinite(sign) or sign == 0:
            continue
        hist = daily.loc[daily.index <= d].dropna()
        if len(hist) < C.VOLATILITY_WINDOW_SESSIONS:
            continue
        vol = float(hist.iloc[-C.VOLATILITY_WINDOW_SESSIONS:].std()) * ann
        if not math.isfinite(vol) or vol <= 0:
            continue
        W.loc[d, market] = float(np.sign(sign)) * min(
            1.0, C.SINGLE_MARKET_TARGET_VOL / vol)
    return W


def config_weights(name: str, signals, forward, panel, decisions):
    np, pd = _np(), _pd()
    if name in ("CMDTY_XS_MOMENTUM_12_1", "CMDTY_XS_CARRY",
                "CMDTY_SEASONALITY", "CMDTY_COT_HEDGING_PRESSURE",
                "INTL_IDX_XS_MOMENTUM", "FX_FUT_CARRY_IMPLEMENTATION"):
        return xs_thirds_weights(signals, forward)
    if name == "RATES_XS_CURVE_CARRY":
        W = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
        for d in signals.index:
            live = signals.loc[d].dropna()
            n = len(live)
            if n < 3:
                continue
            k = max(n // 2, 1)
            ranked = live.sort_values()
            W.loc[d, ranked.index[-k:]] = 0.5 / k
            W.loc[d, ranked.index[:n - k]] = -0.5 / (n - k)
        return W
    if name in ("CMDTY_TS_TREND_12M", "RATES_TS_TREND", "INTL_IDX_TS_TREND"):
        return ts_vol_scaled_weights(signals, panel, decisions)
    if name == "VX_TERM_STRUCTURE_CARRY":
        # slope_ann = log(F1/F2): negative in contango -> short the front.
        direction = signals["VX"].map(
            lambda s: 0.0 if not _np().isfinite(s) or s == 0
            else (-1.0 if s < 0 else 1.0))
        return single_market_vol_target_weights(direction, panel, "VX",
                                                decisions)
    if name in ("CMDTY_CALENDAR_SPREAD_MR", "VX_CALENDAR_SLOPE_MR"):
        # signal z is on slope_ann = log(F1/F2); the traded spread return is
        # ret2 - ret (long F2, short F1). High z (front rich) -> long spread;
        # low z -> short spread.
        W = pd.DataFrame(0.0, index=signals.index, columns=signals.columns)
        for d in signals.index:
            live = signals.loc[d].dropna()
            active = live[live.abs() >= C.SPREAD_ENTRY_Z]
            if not len(active):
                continue
            per = 1.0 / len(signals.columns)
            for m, z in active.items():
                W.loc[d, m] = per if z > 0 else -per
        return W
    raise KeyError(name)


# --------------------------------------------------------------------------- #
# Configuration execution
# --------------------------------------------------------------------------- #
def _meta_for(markets, registry) -> dict:
    meta = {}
    for m in markets:
        group = registry["markets"][m]["cost_group"]
        meta[m] = {"cost_bps_per_side": C.COST_BPS_PER_SIDE.get(group, 10.0)}
    return meta


def _rank_ic(signals, forward):
    """Mean cross-sectional Spearman rank IC of signal vs forward return."""
    np = _np()
    ics = []
    for d in signals.index:
        if d not in forward.index:
            continue
        s = signals.loc[d]
        f = forward.loc[d]
        both = s.notna() & f.notna()
        if both.sum() < 5:
            continue
        sr = s[both].rank()
        fr = f[both].rank()
        ic = np.corrcoef(sr, fr)[0, 1]
        if np.isfinite(ic):
            ics.append(float(ic))
    if len(ics) < 8:
        return {"n": len(ics), "mean": None, "t_stat": None}
    arr = np.asarray(ics)
    t = float(arr.mean() / (arr.std(ddof=1) / math.sqrt(arr.size)))
    return {"n": int(arr.size), "mean": float(arr.mean()), "t_stat": t}


def _hit_rate(weights, forward):
    np = _np()
    hits, total = 0, 0
    for d in weights.index:
        if d not in forward.index:
            continue
        for m in weights.columns:
            w = weights.loc[d, m]
            f = forward.loc[d, m] if m in forward.columns else float("nan")
            if w != 0 and np.isfinite(f):
                total += 1
                if (w > 0) == (f > 0):
                    hits += 1
    return {"n": int(total),
            "hit_rate": (hits / total) if total else None}


def run_configuration(cfg: dict, registry: dict, panel_all: dict,
                      *, cot_cache: Optional[dict] = None,
                      stress_costs: bool = False,
                      matrices_out: Optional[dict] = None) -> dict:
    np, pd = _np(), _pd()
    name = cfg["name"]
    cadence = int(cfg["cadence_sessions"])
    uni = config_universe(name, registry)
    markets = [m for m in uni["markets"] if m in panel_all]
    row = {"name": name, "lane": cfg["lane"], "family": cfg["family"],
           "cadence_sessions": cadence, "control": cfg["control"],
           "universe_declared": uni["markets"], "universe_floor": uni["floor"],
           "universe_with_data": markets}
    if len(markets) < uni["floor"]:
        row.update({"executed": False,
                    "failure_class": C.FAIL_DATA_BLOCKED,
                    "reason": "universe floor %d not met (%d markets with "
                              "usable series)" % (uni["floor"], len(markets))})
        return row

    panel = {m: panel_all[m] for m in markets}
    decisions = decision_calendar(panel, cadence)
    spread = name in ("CMDTY_CALENDAR_SPREAD_MR", "VX_CALENDAR_SLOPE_MR")
    return_column = "spread" if spread else "ret"
    if spread:
        for m in panel:
            df = panel[m]
            df = df.assign(spread=df["ret2"] - df["ret"])
            panel[m] = df
    forward = forward_return_matrix(panel, decisions, column=return_column)
    forward = forward.dropna(how="all")

    if name == "CMDTY_COT_HEDGING_PRESSURE":
        cot = cot_cache if cot_cache is not None else cot_signal_matrix(
            panel, forward.index)
        if not cot.get("ok"):
            row.update({"executed": False,
                        "failure_class": C.FAIL_DATA_BLOCKED,
                        "reason": "COT archive unusable: %s"
                                  % cot.get("reason")})
            return row
        signals = cot["matrix"].reindex(index=forward.index,
                                        columns=list(panel))
        row["cot"] = {k: cot[k] for k in
                      ("cot_rows", "years_read", "mapped_markets",
                       "publication_lag_days")}
    else:
        signals = signal_matrix(name, panel, forward.index)

    weights = config_weights(name, signals, forward, panel, forward.index)
    active = (weights.abs().sum(axis=1) > 0)
    if int(active.sum()) < C.MIN_DECISION_PERIODS:
        row.update({"executed": False,
                    "failure_class": C.FAIL_UNDERPOWERED,
                    "reason": "%d active decision periods < %d required"
                              % (int(active.sum()), C.MIN_DECISION_PERIODS)})
        return row
    first_active = active[active].index[0]
    weights = weights.loc[weights.index >= first_active]
    forward = forward.loc[forward.index >= first_active]

    meta = _meta_for(markets, registry)
    cash = pd.Series(0.0, index=weights.index)
    cost_multiplier = 2.0 if spread else 1.0
    if stress_costs:
        cost_multiplier *= C.COST_STRESS_MULTIPLIER
    if matrices_out is not None:
        matrices_out.update({
            "signals": signals, "forward": forward, "panel": panel,
            "markets": markets, "meta": meta, "cadence": cadence,
            "spread": spread, "cost_multiplier": cost_multiplier,
            "control_name": cfg["control"]})
    path = _econ.evaluate_book(weights, forward, cash, meta=meta,
                               horizon=cadence,
                               cost_multiplier=cost_multiplier)
    if path.get("state") != "OK":
        row.update({"executed": False,
                    "failure_class": C.FAIL_DATA_BLOCKED,
                    "reason": "judge state %s" % path.get("state")})
        return row

    # ---- control ----
    control_name = cfg["control"]
    if control_name == C.CONTROL_PASSIVE_ROLL_BASKET:
        # The passive basket is scored on the SAME forward-return matrix as
        # the book - same dates, same roll, same costs - holding every live
        # market equally. (Spread configs use the cash control, so ``forward``
        # here is always the front-roll return matrix.)
        passive_w = pd.DataFrame(0.0, index=weights.index, columns=markets)
        for d in weights.index:
            live = [m for m in markets
                    if m in forward.columns and np.isfinite(forward.loc[d, m])]
            if live:
                for m in live:
                    passive_w.loc[d, m] = 1.0 / len(live)
        basket_path = _econ.evaluate_book(
            passive_w, forward, cash, meta=meta, horizon=cadence)
        matched = _econ.volatility_matched_control(
            path["net"], basket_path["net"], cash.to_numpy())
        if matched.get("state") != "OK":
            row.update({"executed": True, "control_state": matched.get("state"),
                        "failure_class": C.FAIL_CONTROL_FAILURE,
                        "reason": "control could not be constructed"})
            return row
        control_series = matched["series"]
        row["control_detail"] = {
            "state": "OK", "weight": matched["weight"],
            "book_volatility": matched["book_volatility"],
            "benchmark_volatility": matched["benchmark_volatility"]}
    else:  # RISK_MATCHED_CASH: zero-exposure cash overlay in excess space
        control_series = np.zeros(len(weights.index))
        row["control_detail"] = {"state": "OK", "weight": 0.0,
                                 "kind": "RISK_MATCHED_CASH"}

    sig = _econ.excess_significance(path["net"], control_series,
                                    horizon=cadence)
    desc = _econ.describe(path, horizon=cadence, control=control_series)
    ann_excess = sig.get("annualised_excess")
    t_stat = sig.get("t_stat")

    # subperiod halves and thirds on the excess series
    diff = sig.get("diff")
    halves = None
    thirds_same_sign = None
    if diff is not None and len(diff) >= 16:
        half = len(diff) // 2
        halves = {"first_half_mean": float(np.mean(diff[:half])),
                  "second_half_mean": float(np.mean(diff[half:])),
                  "same_sign": bool(np.sign(np.mean(diff[:half]))
                                    == np.sign(np.mean(diff[half:])))}
        third = len(diff) // 3
        if third >= 8:
            signs = {float(np.sign(np.mean(diff[:third]))),
                     float(np.sign(np.mean(diff[third:2 * third]))),
                     float(np.sign(np.mean(diff[2 * third:])))}
            thirds_same_sign = len(signs) == 1

    utility_improvement = None
    try:
        utility_improvement = float(
            _econ.utility(path["net"], horizon=cadence)
            - _econ.utility(np.asarray(control_series, dtype=float),
                            horizon=cadence))
    except Exception:  # noqa: BLE001 - utility is diagnostic, never fatal
        utility_improvement = None

    xs_config = name in ("CMDTY_XS_MOMENTUM_12_1", "CMDTY_XS_CARRY",
                         "CMDTY_SEASONALITY", "CMDTY_COT_HEDGING_PRESSURE",
                         "INTL_IDX_XS_MOMENTUM",
                         "FX_FUT_CARRY_IMPLEMENTATION",
                         "RATES_XS_CURVE_CARRY")
    predictive = (_rank_ic(signals, forward) if xs_config
                  else _hit_rate(weights, forward))

    mde = _r36_experiments.minimum_detectable_excess(
        ann_excess if ann_excess is not None else float("nan"),
        t_stat if t_stat is not None else float("nan"))

    p_value = (_mt.two_sided_p(t_stat)
               if t_stat is not None and math.isfinite(t_stat) else None)

    failure_class = None
    if t_stat is None:
        failure_class = C.FAIL_UNDERPOWERED
    elif ann_excess is not None and ann_excess < 0 and t_stat <= -C.MIN_EXCESS_T_STAT:
        failure_class = C.FAIL_NEGATIVE
    elif abs(t_stat) < C.MIN_EXCESS_T_STAT:
        failure_class = C.FAIL_INDISTINGUISHABLE

    row.update({
        "executed": True,
        "decision_periods": int(len(weights.index)),
        "first_decision": str(weights.index[0].date()),
        "last_decision": str(weights.index[-1].date()),
        "economics": {
            "gross_return_annualised": desc.get("gross_return_annualised"),
            "net_return_annualised": desc.get("net_return_annualised"),
            "volatility_annualised": desc.get("volatility_annualised"),
            "sharpe": desc.get("sharpe"),
            "max_drawdown": desc.get("max_drawdown"),
            "turnover_periods_mean": float(np.mean(path["traded_notional"])),
            "cost_annualised": float(np.mean(path["costs"])
                                     * _econ.periods_per_year(cadence)),
            "after_cost_excess_vs_control_annualised": ann_excess,
            "excess_t_stat": t_stat,
            "excess_p_value": p_value,
            "minimum_detectable_excess": mde,
        },
        "cost_model_state": C.COST_MODEL_STATE,
        "cost_multiplier": cost_multiplier,
        "subperiod_halves": halves,
        "subperiod_thirds_same_sign": thirds_same_sign,
        "utility_improvement": utility_improvement,
        "predictive_diagnostic": predictive,
        "failure_class": failure_class,
    })
    return row


def _reevaluate_excluding(matrices: dict, exclude: str) -> dict:
    """The configuration's book with one market's COLUMN removed.

    Leave-one-market-out removes the market from the tradable set; the
    decision calendar and every other market's signal are unchanged, and the
    cross-sectional ranks re-form over the remaining columns. This is a
    column drop on the already-computed matrices, never a data rebuild.
    """
    import numpy as np

    pd = _pd()
    signals = matrices["signals"].drop(columns=[exclude], errors="ignore")
    forward = matrices["forward"].drop(columns=[exclude], errors="ignore")
    panel = {m: v for m, v in matrices["panel"].items() if m != exclude}
    name = matrices["name"]
    weights = config_weights(name, signals, forward, panel, forward.index)
    active = (weights.abs().sum(axis=1) > 0)
    if int(active.sum()) < C.MIN_DECISION_PERIODS:
        return {"state": "NOT_EXECUTED"}
    weights = weights.loc[weights.index >= active[active].index[0]]
    fwd = forward.loc[forward.index >= weights.index[0]]
    cash = pd.Series(0.0, index=weights.index)
    path = _econ.evaluate_book(
        weights, fwd, cash, meta=matrices["meta"],
        horizon=matrices["cadence"],
        cost_multiplier=matrices["cost_multiplier"])
    if path.get("state") != "OK":
        return {"state": "NOT_EXECUTED"}
    if matrices["control_name"] == C.CONTROL_PASSIVE_ROLL_BASKET:
        markets = [m for m in matrices["markets"] if m != exclude]
        passive_w = pd.DataFrame(0.0, index=weights.index, columns=markets)
        for d in weights.index:
            live = [m for m in markets
                    if m in fwd.columns and np.isfinite(fwd.loc[d, m])]
            for m in live:
                passive_w.loc[d, m] = 1.0 / len(live)
        basket = _econ.evaluate_book(passive_w, fwd, cash,
                                     meta=matrices["meta"],
                                     horizon=matrices["cadence"])
        matched = _econ.volatility_matched_control(
            path["net"], basket["net"], cash.to_numpy())
        if matched.get("state") != "OK":
            return {"state": "CONTROL_FAILURE"}
        control = matched["series"]
    else:
        control = np.zeros(len(weights.index))
    sig = _econ.excess_significance(path["net"], control,
                                    horizon=matrices["cadence"])
    return {"state": "OK",
            "excess": sig.get("annualised_excess"),
            "t_stat": sig.get("t_stat")}


def leave_one_market_out(cfg: dict, registry: dict, panel_all: dict,
                         base_row: dict, *,
                         matrices: Optional[dict] = None) -> dict:
    """Excess-sign sensitivity to removing each market, for candidates."""
    results = {}
    base_t = ((base_row.get("economics") or {}).get("excess_t_stat"))
    if matrices is None:
        matrices = {}
        run_configuration(cfg, registry, panel_all, matrices_out=matrices)
    matrices = dict(matrices)
    matrices["name"] = cfg["name"]
    uni = config_universe(cfg["name"], registry)
    for market in base_row.get("universe_with_data", []):
        remaining = [m for m in matrices["markets"] if m != market]
        if len(remaining) < uni["floor"]:
            results[market] = {"state": "FLOOR_BROKEN"}
            continue
        results[market] = _reevaluate_excluding(matrices, market)
    excesses = [v.get("excess") for v in results.values()
                if v.get("excess") is not None]
    base = ((base_row.get("economics") or {})
            .get("after_cost_excess_vs_control_annualised"))
    sign_flips = sum(1 for e in excesses
                     if base is not None and e is not None
                     and (e > 0) != (base > 0))
    return {"per_market": results, "sign_flips": int(sign_flips),
            "base_t_stat": base_t}


# --------------------------------------------------------------------------- #
# Campaign-level execution
# --------------------------------------------------------------------------- #
def run_all(*, campaign_id: str = C.CAMPAIGN_ID,
            progress: Optional[Callable[[str], None]] = None) -> dict:
    registry = EN.load_market_registry(campaign_id)
    if registry is None:
        raise RuntimeError("Phase-2 market registry not frozen yet")
    body = registry  # artifact body carries payload at top level
    needed = sorted({m for cfg in C.FROZEN_PRIMARY_CONFIGURATIONS
                     for m in config_universe(cfg["name"], body)["markets"]})
    panel_all = load_panel(needed, campaign_id)

    rows = []
    contexts = {}
    for cfg in C.FROZEN_PRIMARY_CONFIGURATIONS:
        matrices: dict = {}
        row = run_configuration(cfg, body, panel_all, matrices_out=matrices)
        contexts[cfg["name"]] = matrices
        rows.append(row)
        if progress is not None:
            econ = row.get("economics") or {}
            progress("%s executed=%s excess=%s t=%s" % (
                cfg["name"], row.get("executed"),
                econ.get("after_cost_excess_vs_control_annualised"),
                econ.get("excess_t_stat")))

    # Leave-one-market-out for every executed configuration whose |t| clears
    # the candidate bar, reusing the primary pass's matrices (a column drop,
    # never a data rebuild).
    for cfg, row in zip(C.FROZEN_PRIMARY_CONFIGURATIONS, rows):
        econ = row.get("economics") or {}
        t = econ.get("excess_t_stat")
        if (row.get("executed") and t is not None
                and abs(t) >= C.MIN_EXCESS_T_STAT
                and len(row.get("universe_with_data", [])) > 1):
            if progress is not None:
                progress("leave-one-out: %s" % cfg["name"])
            row["leave_one_market_out"] = leave_one_market_out(
                cfg, body, panel_all, row,
                matrices=contexts.get(cfg["name"]) or None)

    # ---- multiple testing over EVERY executed configuration ----
    executed = [r for r in rows if r.get("executed")]
    p_values, labels, signs = [], [], []
    for r in executed:
        econ = r.get("economics") or {}
        p = econ.get("excess_p_value")
        if p is not None:
            p_values.append(p)
            labels.append(r["name"])
            signs.append(1 if (econ.get("excess_t_stat") or 0) > 0 else -1)
    bh = _mt.benjamini_hochberg(p_values, q=C.FDR_Q)
    rejected = [{"name": labels[i], "p_value": p_values[i],
                 "direction": "POSITIVE" if signs[i] > 0 else "NEGATIVE"}
                for i in bh.get("rejected", [])]
    positive_survivors = [r for r in rejected if r["direction"] == "POSITIVE"]

    return {
        "rows": rows,
        "executed_count": len(executed),
        "not_executed": [{"name": r["name"], "reason": r.get("reason")}
                         for r in rows if not r.get("executed")],
        "denominator": len(p_values),
        "bh": {"q": C.FDR_Q, "threshold": bh.get("threshold"),
               "n_rejected": bh.get("n_rejected"), "rejected": rejected},
        "positive_survivors": positive_survivors,
    }
