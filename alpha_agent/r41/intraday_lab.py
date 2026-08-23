"""alpha_agent.r41.intraday_lab - Tracks 6/9: intraday market structure and
the Fibonacci placebo trial, on GENUINE minute history (Dukascopy tick/candle
mid-price bars with OBSERVED bid/ask spreads; FX spot is native, index /
commodity / bond CFDs are LEVEL 2 proxies and labelled so).

THE FIBONACCI QUESTION (contract.FIB_QUESTION): conditional on the
volatility regime, does price reaction near a NAMED retracement level
(23.6 / 38.2 / 50 / 61.8 / 78.6 / 127.2 / 161.8) contain information beyond
generic pullback geometry? The placebo arm uses the SAME machinery at the
contract's placebo levels (30 / 45 / 55 / 70 / 85 / 115 / 145). No
hindsight extrema: a swing extreme exists only after price has closed >= 1
ATR beyond it; the event is stamped at the CONFIRMATION bar. No visual
confirmation. The claim is judged on the NAMED-minus-PLACEBO difference,
clustered by day; the placebo book itself measures the generic pullback
edge.

Also declared: intraday time-series momentum (30m/1h/4h) and hour-of-day
seasonality books per instrument - the generic structure baselines the
Fibonacci claim must exceed.

Costs: observed per-bar half-spread + contract slippage add-on per side.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import evidence as EV
from . import burden as BURDEN
from . import sample_acquisition as SA

CALCULATION_OWNER = "alpha_agent.r41.intraday_lab"
FAMILY = "MICROSTRUCTURE"

BAR_MIN = 5                       # research grid: 5-minute bars
ATR_WIN = 48                      # 4 hours of 5m bars
SWING_WIN = 48                    # trailing extreme window
CONFIRM_ATR = 1.0
MAX_CONFIRM_BARS = 96
BAND_ATR = 0.10                   # half-width of a level band
HOLDS = (6, 12, 48)               # 30m / 1h / 4h
SLIP_ADDON = {"FX_SPOT": 0.2e-4, "METAL_SPOT": 0.5e-4, "INDEX_CFD": 0.5e-4,
              "ENERGY_CFD": 0.5e-4, "BOND_CFD": 0.5e-4, "CRYPTO_SPOT": 1e-4}


def load_bars(sym: str) -> pd.DataFrame:
    m1 = SA.load_dukascopy(sym)
    if m1 is None or not len(m1):
        return pd.DataFrame()
    g = m1.resample("%dmin" % BAR_MIN).agg(
        {"open": "first", "high": "max", "low": "min", "close": "last",
         "spread": "mean"})
    g = g.dropna(subset=["close"])
    return g


def atr(bars: pd.DataFrame, win: int = ATR_WIN) -> pd.Series:
    tr = pd.concat([(bars["high"] - bars["low"]),
                    (bars["high"] - bars["close"].shift(1)).abs(),
                    (bars["low"] - bars["close"].shift(1)).abs()],
                   axis=1).max(axis=1)
    return tr.rolling(win, min_periods=win // 2).mean()


def find_events(bars: pd.DataFrame) -> pd.DataFrame:
    """Confirmed swing (low -> high) pairs, causal. Returns one row per
    confirmed swing with the swing geometry frozen at confirmation."""
    h, l, c = (bars["high"].to_numpy(), bars["low"].to_numpy(),
               bars["close"].to_numpy())
    a = atr(bars).to_numpy()
    n = len(bars)
    events = []
    roll_max = pd.Series(h).rolling(SWING_WIN, min_periods=SWING_WIN).max() \
        .to_numpy()
    roll_min_idx = pd.Series(l).rolling(4 * SWING_WIN,
                                        min_periods=SWING_WIN).min().to_numpy()
    i = SWING_WIN
    last_event_bar = -10**9
    while i < n - 1:
        if not np.isfinite(a[i]) or a[i] <= 0:
            i += 1
            continue
        # candidate swing high: bar i-? holds the trailing max
        if h[i] >= roll_max[i] and i > last_event_bar + 12:
            hi_px = h[i]
            # find confirmation: close retreats >= CONFIRM_ATR * ATR
            j = i + 1
            conf = None
            while j < min(i + MAX_CONFIRM_BARS, n):
                if h[j] > hi_px:            # higher high -> restart there
                    break
                if hi_px - c[j] >= CONFIRM_ATR * a[i]:
                    conf = j
                    break
                j += 1
            if conf is not None:
                lo_px = roll_min_idx[i]
                if np.isfinite(lo_px) and hi_px - lo_px > 2 * a[i]:
                    events.append({"swing_high_bar": i, "confirm_bar": conf,
                                   "high": hi_px, "low": float(lo_px),
                                   "atr": float(a[i])})
                    last_event_bar = conf
                i = conf + 1 if conf is not None else i + 1
                continue
        i += 1
    return pd.DataFrame(events)


def level_touches(bars: pd.DataFrame, events: pd.DataFrame,
                  levels: tuple) -> pd.DataFrame:
    """First touch of each retracement band after confirmation; forward
    mid returns (bounce direction = +long) and per-side observed cost."""
    c = bars["close"].to_numpy()
    lo_ = bars["low"].to_numpy()
    sp = bars["spread"].to_numpy()
    n = len(bars)
    idx = bars.index
    rows = []
    for ev in events.itertuples():
        rng = ev.high - ev.low
        for r in levels:
            lv = ev.high - r * rng
            band = BAND_ATR * ev.atr
            j = ev.confirm_bar + 1
            hit = None
            while j < min(ev.confirm_bar + 288, n):     # within 24h
                if lo_[j] <= lv + band and c[j] >= lv - 3 * band:
                    hit = j
                    break
                if c[j] < lv - 3 * band:               # blew through
                    break
                j += 1
            if hit is None:
                continue
            row = {"event_bar": ev.confirm_bar, "touch_bar": hit,
                   "level": r, "ts": idx[hit], "atr": ev.atr,
                   "depth": (ev.high - c[hit]) / rng,
                   "spread": sp[hit] if np.isfinite(sp[hit]) else 0.0}
            for k in HOLDS:
                if hit + 1 + k < n:
                    entry = c[hit + 1]
                    row["fwd_%d" % k] = c[hit + 1 + k] / entry - 1.0
            rows.append(row)
    return pd.DataFrame(rows)


def fib_trial(sym: str, *, progress=None) -> dict:
    bars = load_bars(sym)
    if len(bars) < 50_000:
        return {"symbol": sym, "state": "INSUFFICIENT_BARS",
                "bars": int(len(bars)),
                "min_required": C.MIN_INTRADAY_BARS_FOR_RESEARCH}
    cls = SA.DUKA_CLASS.get(sym, "FX_SPOT")
    slip = SLIP_ADDON.get(cls, 0.5e-4)
    events = find_events(bars)
    named = level_touches(bars, events, C.FIB_NAMED_LEVELS)
    placebo = level_touches(bars, events, C.FIB_PLACEBO_LEVELS)
    if not len(named) or not len(placebo):
        return {"symbol": sym, "state": "NO_TOUCHES",
                "events": int(len(events))}
    rv = bars["close"].pct_change().rolling(288).std()
    rv_med = rv.median()
    out = {"symbol": sym, "state": "OK", "asset_class": cls,
           "bars": int(len(bars)), "events": int(len(events)),
           "named_touches": int(len(named)),
           "placebo_touches": int(len(placebo)),
           "first": str(bars.index[0]), "last": str(bars.index[-1]),
           "arms": {}}
    for k in HOLDS:
        col = "fwd_%d" % k
        arms = {}
        for arm_name, df in (("NAMED", named), ("PLACEBO", placebo)):
            d = df.dropna(subset=[col]).copy()
            # bounce trade: long at touch (uptrend pullback), pay spread
            cost = (d["spread"] / 2 + slip) * 2      # entry+exit, per unit
            net = d[col] - cost
            day = pd.DatetimeIndex(d["ts"]).date
            # cluster by day: average within day, then HAC over days
            byday = pd.DataFrame({"net": net.values, "day": day}) \
                .groupby("day")["net"].mean()
            r = EV.hac_t(byday.to_numpy(), lags=4)
            arms[arm_name] = {"n_touches": int(len(d)),
                              "n_days": int(len(byday)),
                              "mean_net_per_touch": float(net.mean()),
                              "gross_mean": float(d[col].mean()),
                              "cost_mean": float(cost.mean()),
                              "t_by_day": r["t"]}
            # volatility-regime split
            hi = d[pd.Series(rv.reindex(pd.DatetimeIndex(d["ts"])).values,
                             index=d.index) > rv_med]
            if len(hi) > 50:
                arms[arm_name]["highvol_mean_net"] = float(
                    (hi[col] - (hi["spread"] / 2 + slip) * 2).mean())
        # the Fibonacci CLAIM: named minus placebo (per-touch, day-clustered)
        dn = named.dropna(subset=[col])
        dp = placebo.dropna(subset=[col])
        nd = pd.DataFrame({"r": dn[col].values,
                           "day": pd.DatetimeIndex(dn["ts"]).date}) \
            .groupby("day")["r"].mean()
        pdx = pd.DataFrame({"r": dp[col].values,
                            "day": pd.DatetimeIndex(dp["ts"]).date}) \
            .groupby("day")["r"].mean()
        both = pd.concat([nd.rename("n"), pdx.rename("p")], axis=1).dropna()
        diff = (both["n"] - both["p"]).to_numpy()
        r = EV.hac_t(diff, lags=4)
        arms["NAMED_MINUS_PLACEBO"] = {"n_days_joint": int(len(both)),
                                       "mean_diff": r["mean"], "t": r["t"]}
        out["arms"]["hold_%d_bars" % k] = arms
        if progress:
            progress("%s hold=%d named t=%s placebo t=%s diff t=%s" % (
                sym, k, arms["NAMED"]["t_by_day"],
                arms["PLACEBO"]["t_by_day"],
                arms["NAMED_MINUS_PLACEBO"]["t"]))
    return out


# --------------------------------------------------------------------------- #
# Generic intraday structure books (baselines)
# --------------------------------------------------------------------------- #
def intraday_momentum(sym: str, *, progress=None) -> list:
    bars = load_bars(sym)
    if len(bars) < 50_000:
        return [{"symbol": sym, "state": "INSUFFICIENT_BARS"}]
    cls = SA.DUKA_CLASS.get(sym, "FX_SPOT")
    slip = SLIP_ADDON.get(cls, 0.5e-4)
    ret = bars["close"].pct_change()
    rv = ret.rolling(288).std()
    cost_side = (bars["spread"] / bars["close"] / 2).fillna(0) + slip
    dates = bars.index
    zones = EV.zone_split(dates, embargo=288)
    ppy = 252.0 * (288 / (24 / 24)) * (60 / BAR_MIN) / 60 * 24  # bars/yr
    ppy = 252.0 * 24 * 60 / BAR_MIN
    out = []
    for look, hold in ((12, 6), (48, 12), (96, 48)):
        mom = ret.rolling(look).sum().shift(1)
        sig = np.sign(mom / rv.replace(0, np.nan)).fillna(0.0)
        pos = sig.rolling(hold, min_periods=1).mean()
        gross = pos.shift(1) * ret
        cost = (pos - pos.shift(1)).abs() * cost_side
        spec = {"information_family": "INTRADAY_STRUCTURE",
                "asset_family": cls, "horizon": "%dx5m" % hold,
                "economic_expression": "INTRADAY_TS",
                "representation": "MOM_%d_%s" % (look, sym),
                "model": "TRANSPARENT_RULE",
                "hyperparameter_budget": 1,
                "parent_hypotheses": ["generic structure baseline"],
                "validation_touches": 1}
        g_a = gross[dates.isin(zones["A"])].to_numpy()
        c_a = cost[dates.isin(zones["A"])].to_numpy()
        card_a = EV.scorecard(g_a, c_a, np.zeros(len(g_a)),
                              periods_per_year=ppy, overlap=hold)
        t_a = card_a.get("excess_t_hac")
        sgn = 1.0
        if t_a is not None and t_a < 0:
            sgn = -1.0
            g_a = -g_a
            card_a = EV.scorecard(g_a, c_a, np.zeros(len(g_a)),
                                  periods_per_year=ppy, overlap=hold)
            t_a = card_a.get("excess_t_hac")
        row = {"symbol": sym, "look": look, "hold": hold, "sign": sgn,
               "zone_a_t": t_a,
               "zone_a_gross_ann": card_a.get("gross_ann"),
               "zone_a_cost_ann": card_a.get("cost_ann")}
        if t_a is not None and t_a >= 1.5:
            g_b = (sgn * gross)[dates.isin(zones["B"])].to_numpy()
            c_b = cost[dates.isin(zones["B"])].to_numpy()
            card_b = EV.scorecard(g_b, c_b, np.zeros(len(g_b)),
                                  periods_per_year=ppy, overlap=hold)
            cid = BURDEN.record_zone_b(spec, family=FAMILY)
            gate = EV.research_candidate_gate(card_b)
            row.update({"candidate_id": cid,
                        "zone_b": EV.summarise(card_b),
                        "gate": gate["passes"]})
        out.append(row)
        if progress:
            progress("%s mom look=%d hold=%d A t=%s B t=%s" % (
                sym, look, hold,
                None if t_a is None else round(t_a, 2),
                (row.get("zone_b") or {}).get("excess_t_hac")))
    return out


def run(symbols=("EURUSD", "XAUUSD", "USA500IDXUSD"), *,
        progress=None) -> dict:
    out = {"fib": {}, "momentum": {}, "bar_grid_minutes": BAR_MIN,
           "pivot_rule": C.PIVOT_CONFIRMATION_RULE,
           "named_levels": C.FIB_NAMED_LEVELS,
           "placebo_levels": C.FIB_PLACEBO_LEVELS}
    for sym in symbols:
        out["fib"][sym] = fib_trial(sym, progress=progress)
        out["momentum"][sym] = intraday_momentum(sym, progress=progress)
    return out
