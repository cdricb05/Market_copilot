"""alpha_agent.r63.features - ontology dimensions as point-in-time observables.

One builder per (scope kind, dimension). Every feature at decision index t
uses only information whose availability is <= session t (market prices on
their own session; publications after their declared lag; filings after their
filing date plus one session; revised statistics as they were known at t).
The as-of rule lives in :mod:`alpha_agent.r63.pit` and nowhere else.

A dimension yields a SMALL block of features (1-3), chosen before any result
was seen, so that a dimension is one economic state and not a formula zoo.
Blocks are returned as ``(n_inst x n_dates x k)`` arrays on the substrate's
session grid; the engine slices them at decision indices.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import (AC_COMMODITY, AC_EQUITY_INDEX, AC_FX, AC_RATES, AC_VOLATILITY,
               COT_PUBLICATION_LAG_DAYS, EIA_WEEKLY_PUBLICATION_LAG_DAYS,
               MARKET_BROADCAST_LAG_SESSIONS, SEC_FILING_BROADCAST_LAG_SESSIONS)
from . import panels as P
from . import pit

CALCULATION_OWNER = "alpha_agent.r63.features"
PPY = 252.0


# --------------------------------------------------------------------------- #
# Rolling helpers (past-only windows, NaN-aware)
# --------------------------------------------------------------------------- #
def _roll(a: np.ndarray, w: int, fn: str) -> np.ndarray:
    df = pd.DataFrame(a.T)
    r = df.rolling(int(w), min_periods=max(2, int(w * 0.6)))
    if fn == "mean":
        out = r.mean()
    elif fn == "std":
        out = r.std()
    elif fn == "sum":
        out = r.sum()
    elif fn == "max":
        out = r.max()
    elif fn == "skew":
        out = r.skew()
    else:
        raise ValueError(fn)
    return out.to_numpy().T


def _cum(ret: np.ndarray) -> np.ndarray:
    """Cumulative log index from simple returns, NaN where no bar."""
    lg = np.log1p(np.clip(np.where(np.isfinite(ret), ret, 0.0), -0.999999, None))
    return np.cumsum(lg, axis=1)


def _ret_over(ret: np.ndarray, w: int, skip: int = 0) -> np.ndarray:
    """Compounded return over the w sessions ending ``skip`` sessions ago."""
    c = _cum(ret)
    n = c.shape[1]
    out = np.full_like(c, np.nan)
    a, b = w + skip, skip
    if n > a:
        out[:, a:] = c[:, a - b:n - b] - c[:, :n - a] if b else c[:, a:] - c[:, :n - a]
    return np.expm1(out)


def _z(a: np.ndarray, w: int) -> np.ndarray:
    m = _roll(a, w, "mean")
    s = _roll(a, w, "std")
    return (a - m) / np.where(s > 0, s, np.nan)


def _lag(a: np.ndarray, k: int = MARKET_BROADCAST_LAG_SESSIONS) -> np.ndarray:
    if k <= 0:
        return a
    out = np.full_like(a, np.nan)
    out[:, k:] = a[:, :-k]
    return out


def _broadcast(series_on_grid: np.ndarray, n_inst: int) -> np.ndarray:
    return np.repeat(series_on_grid[None, :], n_inst, axis=0)


def _stack(*blocks) -> np.ndarray:
    return np.stack(blocks, axis=-1)


def _daily_on_grid(series: pd.Series, dates: np.ndarray, *, lag_sessions=1,
                   publication_lag_days=0) -> np.ndarray:
    return pit.as_of(series, dates, lag_sessions=lag_sessions,
                     publication_lag_days=publication_lag_days)


# --------------------------------------------------------------------------- #
# Futures: instrument-level blocks
# --------------------------------------------------------------------------- #
def futures_price_blocks(F: dict) -> dict:
    """The baseline price block for the futures substrate (all lagged 0: a
    close is used at its own close and the position is entered NEXT close)."""
    r = F["ret1"]
    c1, v1, oi1 = F["c1"], F["v1"], F["oi1"]
    ret_1 = r
    ret_5 = _ret_over(r, 5)
    ret_21 = _ret_over(r, 21)
    ret_63 = _ret_over(r, 63)
    mom_126_21 = _ret_over(r, 126, 21)
    trend_252_21 = _ret_over(r, 252, 21)
    cum = _cum(r)
    ma50 = _roll(cum, 50, "mean")
    ma200 = _roll(cum, 200, "mean")
    ma_cross = ma50 - ma200
    vol_21 = _roll(r, 21, "std") * np.sqrt(PPY)
    vol_63 = _roll(r, 63, "std") * np.sqrt(PPY)
    vol_252 = _roll(r, 252, "std") * np.sqrt(PPY)
    vol_ratio = vol_21 / np.where(vol_252 > 0, vol_252, np.nan)
    hi_252 = _roll(cum, 252, "max")
    drawdown = np.expm1(cum - hi_252)
    skew_63 = _roll(r, 63, "skew")
    max_abs_21 = _roll(np.abs(r), 21, "max") / np.where(vol_63 > 0, vol_63 / np.sqrt(PPY), np.nan)
    dvol = np.log(np.where((v1 > 0) & (c1 > 0), v1 * np.abs(c1), np.nan))
    log_adv = _roll(dvol, 63, "mean")
    amihud = np.log(_roll(np.abs(r) / np.where(np.isfinite(dvol), np.exp(dvol), np.nan),
                          63, "mean") + 1e-12)
    vol_z = _z(np.log(np.where(v1 > 0, v1, np.nan)), 252)
    loi = np.log(np.where(oi1 > 0, oi1, np.nan))
    oi_chg_21 = loi - _lag(loi, 21)
    oi_z = _z(loi, 252)
    return {
        "PRICE_RETURN_STATE": _stack(ret_21, ret_63),
        "TREND": _stack(trend_252_21, ma_cross),
        "MOMENTUM": _stack(mom_126_21),
        "REVERSAL": _stack(ret_1, ret_5),
        "REALISED_VOLATILITY": _stack(vol_21, vol_ratio),
        "TAIL_CRASH_STATE": _stack(drawdown, skew_63, max_abs_21),
        "LIQUIDITY": _stack(log_adv, amihud),
        "VOLUME_PARTICIPATION": _stack(vol_z, oi_chg_21, oi_z),
        "_vol_63": vol_63,
    }


def futures_carry_block(F: dict) -> np.ndarray:
    s = F["slope_ann"]
    return _stack(s, s - _lag(s, 21))


def futures_seasonality_block(F: dict) -> np.ndarray:
    """Month-of-year harmonics and the instrument's own same-calendar-month
    mean return over the prior years, computed as-of (past only)."""
    dates = pd.to_datetime(F["dates"])
    m = dates.month.to_numpy()
    n_i = F["ret1"].shape[0]
    ang = 2 * np.pi * (m - 1) / 12.0
    sin_m = _broadcast(np.sin(ang), n_i)
    cos_m = _broadcast(np.cos(ang), n_i)
    # as-of same-month history: cumulative sum of prior years' monthly returns
    r = F["ret1"]
    df = pd.DataFrame(r.T, index=dates)
    monthly = df.resample("ME").apply(lambda x: np.expm1(np.log1p(x.fillna(0)).sum()))
    monthly[df.resample("ME").count() == 0] = np.nan
    mon = monthly.index.month.to_numpy()
    hist = np.full(monthly.shape, np.nan)
    vals = monthly.to_numpy()
    for mm in range(1, 13):
        sel = np.where(mon == mm)[0]
        cs = np.nancumsum(vals[sel], axis=0)
        cn = np.cumsum(np.isfinite(vals[sel]), axis=0)
        # exclude the current month: shift by one same-month observation
        prev_cs = np.vstack([np.zeros((1, vals.shape[1])), cs[:-1]])
        prev_cn = np.vstack([np.zeros((1, vals.shape[1])), cn[:-1]])
        hist[sel] = np.where(prev_cn >= 3, prev_cs / np.where(prev_cn > 0, prev_cn, np.nan), np.nan)
    # map month-level history onto the daily grid: the value for month M is
    # known at the START of month M (it uses only completed prior months)
    hist_df = pd.DataFrame(hist, index=monthly.index)
    on_grid = hist_df.reindex(dates, method="bfill").to_numpy().T
    return _stack(sin_m, cos_m, on_grid)


def futures_positioning_block(F: dict) -> np.ndarray:
    cot = P.load_cot()
    dates = F["dates"]
    n_i, n_d = F["ret1"].shape
    spec_z = np.full((n_i, n_d), np.nan)
    spec_chg = np.full((n_i, n_d), np.nan)
    comm_z = np.full((n_i, n_d), np.nan)
    if len(cot) == 0:
        return _stack(spec_z, spec_chg, comm_z)
    for i, m in enumerate(F["markets"]):
        sub = cot[cot["market"] == m]
        if sub.empty:
            continue
        s = pd.Series(sub["spec_net"].values, index=pd.to_datetime(sub["as_of"]))
        c = pd.Series(sub["comm_net"].values, index=pd.to_datetime(sub["as_of"]))
        # weekly z over 156 weeks and 13-week change, computed on the weekly
        # series BEFORE the as-of projection (so the window is in weeks)
        sz = (s - s.rolling(156, min_periods=52).mean()) / s.rolling(156, min_periods=52).std()
        sc = s - s.shift(13)
        cz = (c - c.rolling(156, min_periods=52).mean()) / c.rolling(156, min_periods=52).std()
        kw = {"lag_sessions": MARKET_BROADCAST_LAG_SESSIONS,
              "publication_lag_days": COT_PUBLICATION_LAG_DAYS}
        spec_z[i] = pit.as_of(sz, dates, **kw)
        spec_chg[i] = pit.as_of(sc, dates, **kw)
        comm_z[i] = pit.as_of(cz, dates, **kw)
    return _stack(spec_z, spec_chg, comm_z)


def futures_inventory_block(F: dict) -> np.ndarray:
    eia = P.load_eia_weekly()
    dates = F["dates"]
    n_i, n_d = F["ret1"].shape
    z_seas = np.full((n_i, n_d), np.nan)
    chg_4w = np.full((n_i, n_d), np.nan)
    if len(eia) == 0:
        return _stack(z_seas, chg_4w)
    prepared = {}
    for key in eia.columns:
        s = eia[key].dropna()
        if s.empty:
            continue
        wk = s.index.isocalendar().week.to_numpy()
        vals = s.to_numpy()
        seas = np.full(len(s), np.nan)
        for j in range(len(s)):
            # same ISO week in the previous five years, strictly before j
            lo = max(0, j - 5 * 53)
            sel = [k for k in range(lo, j) if abs(int(wk[k]) - int(wk[j])) <= 1]
            if len(sel) >= 3:
                mu, sd = np.mean(vals[sel]), np.std(vals[sel])
                seas[j] = (vals[j] - mu) / sd if sd > 0 else np.nan
        zs = pd.Series(seas, index=s.index)
        c4 = np.log(s) - np.log(s.shift(4))
        kw = {"lag_sessions": MARKET_BROADCAST_LAG_SESSIONS,
              "publication_lag_days": EIA_WEEKLY_PUBLICATION_LAG_DAYS}
        prepared[key] = (pit.as_of(zs, dates, **kw), pit.as_of(c4, dates, **kw))
    for i, m in enumerate(F["markets"]):
        key = P.EIA_MARKET_MAP.get(m)
        if key in prepared:
            z_seas[i], chg_4w[i] = prepared[key]
    return _stack(z_seas, chg_4w)


# --------------------------------------------------------------------------- #
# Market-level daily conditioners (broadcast to every instrument)
# --------------------------------------------------------------------------- #
def _fred(col: str, dates: np.ndarray) -> np.ndarray:
    fr = P.load_fred_daily()
    if col not in fr.columns:
        return np.full(len(dates), np.nan)
    return _daily_on_grid(fr[col].dropna(), dates)


def _cboe(col: str, dates: np.ndarray) -> np.ndarray:
    cb = P.load_cboe()
    if col not in cb.columns:
        return np.full(len(dates), np.nan)
    return _daily_on_grid(cb[col].dropna(), dates)


def _z1(a: np.ndarray, w: int) -> np.ndarray:
    return _z(a[None, :], w)[0]


def _lag1(a: np.ndarray, k: int) -> np.ndarray:
    return _lag(a[None, :], k)[0]


def _latest_known(series_id: str, dates: np.ndarray) -> np.ndarray:
    """Latest KNOWN value of a revised statistic at each session: among all
    vintage rows with available_at < session, take the row with the latest
    observation date, and for that observation the latest vintage."""
    df = P.alfred_latest_known(series_id)
    if df.empty:
        return np.full(len(dates), np.nan)
    df = df.sort_values(["available_at", "observation_date"])
    cal = pd.to_datetime(dates)
    out = np.full(len(cal), np.nan)
    av = df["available_at"].values
    obs = df["observation_date"].values
    val = df["value"].values.astype(float)
    # iterate sessions; maintain best (obs_date, value) among rows available
    j = 0
    best_obs, best_val = None, np.nan
    for t, d in enumerate(cal.values):
        while j < len(df) and av[j] < d:
            if best_obs is None or obs[j] >= best_obs:
                best_obs, best_val = obs[j], val[j]
            j += 1
        out[t] = best_val
    return out


def _first_release(series_id: str, dates: np.ndarray) -> pd.Series:
    return P.alfred_first_release(series_id)


def market_conditioner_blocks(dates: np.ndarray, *, equity_ret: np.ndarray | None,
                              scope_returns: np.ndarray | None) -> dict:
    """All market-level dimension blocks on the session grid (1 x n_dates x k)."""
    n = len(dates)
    vix = _cboe("VIX", dates)
    vix9 = _cboe("VIX9D", dates)
    vix3m = _cboe("VIX3M", dates)
    vvix = _cboe("VVIX", dates)
    skew = _cboe("SKEW", dates)
    oas_hy = _fred("OAS_HY", dates)
    oas_ig = _fred("OAS_IG", dates)
    # the ICE BofA OAS series are licence-capped to three years in the owned
    # panel; the Moody's Baa - 10y Treasury spread (R35 archive, 1986-) is the
    # free long-history credit observable and carries the credit block
    baa = _daily_on_grid(P.load_fred_r35("BAA10Y"), dates)
    cmt3m = _fred("CMT_3M", dates)
    cmt2 = _fred("CMT_2Y", dates)
    cmt5 = _fred("CMT_5Y", dates)
    cmt10 = _fred("CMT_10Y", dates)
    cmt30 = _fred("CMT_30Y", dates)
    be10 = _fred("BE_10Y", dates)
    be5 = _fred("BE_5Y", dates)
    effr = _fred("EFFR", dates)
    sofr = _fred("SOFR", dates)
    # policy path (ZQ / SR3)
    pp = P.load_policy_path()
    zq_path = _daily_on_grid(pp["ZQ_path_6m"].dropna(), dates) if "ZQ_path_6m" in pp else np.full(n, np.nan)
    sr_path = _daily_on_grid(pp["SR3_path_6m"].dropna(), dates) if "SR3_path_6m" in pp else np.full(n, np.nan)
    policy = np.where(np.isfinite(zq_path), zq_path, sr_path)
    # ALFRED vintages
    nfci = _latest_known("NFCI", dates)
    unrate_fr = pit.strictly_after(_first_release("UNRATE", dates), dates)
    cpi_fr = _first_release("CPIAUCSL", dates)
    icsa_fr = _first_release("ICSA", dates)
    cpi_level = pit.strictly_after(cpi_fr, dates)
    icsa_level = pit.strictly_after(icsa_fr, dates)
    # first-release changes: compare first releases spaced k observations apart
    def _fr_change(s: pd.Series, k: int, log: bool = False) -> np.ndarray:
        if s.empty:
            return np.full(n, np.nan)
        v = np.log(s) if log else s
        d = v - v.shift(k)
        return pit.strictly_after(d.dropna(), dates)
    unrate_chg3 = _fr_change(_first_release("UNRATE", dates), 3)
    icsa_chg4 = _fr_change(_first_release("ICSA", dates), 4, log=True)
    cpi_yoy = _fr_change(_first_release("CPIAUCSL", dates), 12, log=True)
    # naive-forecast surprises on first releases
    def _surprise(s: pd.Series, k_avg: int) -> np.ndarray:
        if s.empty:
            return np.full(n, np.nan)
        d = s.diff()
        exp = d.rolling(k_avg, min_periods=1).mean().shift(1)
        sd = d.rolling(24, min_periods=6).std().shift(1)
        sur = (d - exp) / sd
        return pit.strictly_after(sur.dropna(), dates)
    unrate_sur = _surprise(_first_release("UNRATE", dates), 3)
    icsa_sur = _surprise(_first_release("ICSA", dates), 4)
    cpi_sur = _surprise(np.log(_first_release("CPIAUCSL", dates)) if not _first_release("CPIAUCSL", dates).empty else pd.Series(dtype=float), 3)
    # risk appetite composite (z of VIX, z of HY OAS, minus z of 63d equity ret)
    eq63 = np.full(n, np.nan)
    if equity_ret is not None:
        eq63 = _ret_over(equity_ret[None, :], 63)[0]
    risk_off = np.nanmean(np.vstack([_z1(vix, 252), _z1(oas_hy, 252), -_z1(eq63, 252)]), axis=0)
    risk_off = np.where(np.isfinite(vix) | np.isfinite(oas_hy) | np.isfinite(eq63), risk_off, np.nan)
    # cross-sectional dispersion of 21d returns within the scope
    disp = np.full(n, np.nan)
    if scope_returns is not None and scope_returns.shape[0] >= 3:
        r21 = _ret_over(scope_returns, 21)
        disp = np.nanstd(r21, axis=0)
        disp = np.where(np.sum(np.isfinite(r21), axis=0) >= 3, disp, np.nan)
    blocks = {
        "VOLATILITY_EXPECTATIONS_IV": _stack(_z1(vix, 252), vix3m / vix - 1.0,
                                             vix - _lag1(vix, 21)),
        "CREDIT_CONDITIONS": _stack(_z1(baa, 252), baa - _lag1(baa, 21),
                                    baa - _lag1(baa, 63)),
        "TERM_STRUCTURE": _stack(cmt10 - cmt2, cmt30 - cmt5,
                                 (cmt10 - cmt2) - _lag1(cmt10 - cmt2, 21)),
        "CURVE_SHAPE": _stack(2 * cmt5 - cmt2 - cmt10,
                              (2 * cmt5 - cmt2 - cmt10) - _lag1(2 * cmt5 - cmt2 - cmt10, 21)),
        "RATES_EXPECTATIONS": _stack(cmt2 - cmt3m, cmt2 - _lag1(cmt2, 21)),
        "POLICY_EXPECTATIONS": _stack(policy, policy - _lag1(policy, 21)),
        "INFLATION_EXPECTATIONS": _stack(_z1(be10, 252), be10 - _lag1(be10, 21), be5 - be10),
        # SOFR begins 2018, which starved the block; the fed-funds minus bill
        # spread (2000-) is the funding-stress proxy that has history
        "FUNDING_LIQUIDITY_CONDITIONS": _stack(nfci, nfci - _lag1(nfci, 21), effr - cmt3m),
        "_sofr_minus_effr": sofr - effr, "_oas_hy": oas_hy, "_oas_ig": oas_ig,
        "MACRO_LEVELS": _stack(unrate_fr, cpi_yoy),
        "MACRO_CHANGE": _stack(unrate_chg3, icsa_chg4, cpi_yoy - _lag1(cpi_yoy, 63)),
        "MACRO_SURPRISES": _stack(unrate_sur, icsa_sur, cpi_sur),
        "RISK_APPETITE": _stack(risk_off, risk_off - _lag1(risk_off, 21)),
        "DISPERSION": _stack(_z1(disp, 252)),
        "_vol_term_extra": _stack(vix9 / vix - 1.0, _z1(vvix, 252), _z1(skew, 252)),
        "_vix": vix, "_icsa_level": icsa_level, "_cpi_level": cpi_level,
    }
    return blocks


def cross_asset_block(F: dict) -> np.ndarray:
    """Lagged 21-session returns of four transmission channels: US equity
    (ES), the dollar (DX), US 10-year (ZN), broad commodities (GD)."""
    n_i, n_d = F["ret1"].shape
    chans = []
    for m in ("ES", "DX", "ZN", "GD"):
        if m in F["markets"]:
            i = F["markets"].index(m)
            chans.append(_ret_over(F["ret1"][i][None, :], 21)[0])
        else:
            chans.append(np.full(n_d, np.nan))
    return _stack(*[_broadcast(c, n_i) for c in chans])


def instrument_iv_block(F: dict, cond: dict) -> np.ndarray:
    """Instrument-specific implied volatility where a free index exists (OVX
    for energy, GVZ for gold), VIX otherwise; plus the variance risk premium
    against the instrument's own realised 21-session variance."""
    dates = F["dates"]
    n_i, n_d = F["ret1"].shape
    ovx = _fred("OVX", dates)
    gvz = _fred("GVZ", dates)
    vix = cond["_vix"]
    iv = np.full((n_i, n_d), np.nan)
    for i, m in enumerate(F["markets"]):
        if m in ("CL", "WBS", "BRN", "HO", "RB", "GAS"):
            iv[i] = np.where(np.isfinite(ovx), ovx, vix)
        elif m in ("GC", "SI", "PL", "PA"):
            iv[i] = np.where(np.isfinite(gvz), gvz, vix)
        else:
            iv[i] = vix
    rv21 = _roll(F["ret1"], 21, "std") * np.sqrt(PPY) * 100.0
    vrp = iv - rv21
    return _stack(_z(iv, 252), vrp)


def build_futures_features(F: dict, scope_rows: list) -> dict:
    """Every dimension block for the futures substrate restricted to
    ``scope_rows`` (row indices into F). Returns {dimension: (n x n_dates x k)}
    plus the auxiliary '_vol_63' series."""
    sub = {k: (v[scope_rows] if isinstance(v, np.ndarray) and v.ndim == 2 and v.shape[0] == len(F["markets"]) else v)
           for k, v in F.items()}
    sub["markets"] = [F["markets"][i] for i in scope_rows]
    price = futures_price_blocks(sub)
    es = F["ret1"][F["markets"].index("ES")] if "ES" in F["markets"] else None
    cond = market_conditioner_blocks(F["dates"], equity_ret=es, scope_returns=sub["ret1"])
    n_i = len(scope_rows)
    out = dict(price)
    out["CARRY"] = futures_carry_block(sub)
    out["CALENDAR_SEASONALITY"] = futures_seasonality_block(sub)
    out["POSITIONING_COMMITMENTS"] = futures_positioning_block(sub)
    out["INVENTORY"] = futures_inventory_block(sub)
    out["CROSS_ASSET_TRANSMISSION"] = cross_asset_block(F)[scope_rows]
    iv = instrument_iv_block(sub, cond)
    vte = cond["_vol_term_extra"]
    out["VOLATILITY_EXPECTATIONS_IV"] = _stack(
        iv[..., 0], iv[..., 1], _broadcast(cond["VOLATILITY_EXPECTATIONS_IV"][..., 1], n_i))
    for dim in ("CREDIT_CONDITIONS", "TERM_STRUCTURE", "CURVE_SHAPE",
                "RATES_EXPECTATIONS", "POLICY_EXPECTATIONS", "INFLATION_EXPECTATIONS",
                "FUNDING_LIQUIDITY_CONDITIONS", "MACRO_LEVELS", "MACRO_CHANGE",
                "MACRO_SURPRISES", "RISK_APPETITE", "DISPERSION"):
        blk = cond[dim]
        out[dim] = np.stack([_broadcast(blk[..., k], n_i) for k in range(blk.shape[-1])], axis=-1)
    out["_market_level"] = {"CREDIT_CONDITIONS", "TERM_STRUCTURE", "CURVE_SHAPE",
                            "RATES_EXPECTATIONS", "POLICY_EXPECTATIONS",
                            "INFLATION_EXPECTATIONS", "FUNDING_LIQUIDITY_CONDITIONS",
                            "MACRO_LEVELS", "MACRO_CHANGE", "MACRO_SURPRISES",
                            "RISK_APPETITE", "DISPERSION", "CROSS_ASSET_TRANSMISSION"}
    out["_regime_vix"] = cond["_vix"]
    return out


# --------------------------------------------------------------------------- #
# Equities: stock-level blocks on the R57 grid
# --------------------------------------------------------------------------- #
def equity_price_blocks(E: dict) -> dict:
    pr = E["price"]
    tr, un, vol = pr["tr"], pr["un"], pr["vol"]
    r = tr[:, 1:] / tr[:, :-1] - 1.0
    r = np.concatenate([np.full((tr.shape[0], 1), np.nan), r], axis=1)
    ret_5 = _ret_over(r, 5)
    ret_21 = _ret_over(r, 21)
    ret_63 = _ret_over(r, 63)
    mom = _ret_over(r, 126, 21)
    trend = _ret_over(r, 252, 21)
    vol_21 = _roll(r, 21, "std") * np.sqrt(PPY)
    vol_63 = _roll(r, 63, "std") * np.sqrt(PPY)
    vol_252 = _roll(r, 252, "std") * np.sqrt(PPY)
    cum = _cum(r)
    drawdown = np.expm1(cum - _roll(cum, 252, "max"))
    skew_63 = _roll(r, 63, "skew")
    dvol = np.log(np.where((vol > 0) & (un > 0), vol * un, np.nan))
    log_adv = _roll(dvol, 63, "mean")
    amihud = np.log(_roll(np.abs(r) / np.where(np.isfinite(dvol), np.exp(dvol), np.nan), 63, "mean") + 1e-12)
    vol_z = _z(np.log(np.where(vol > 0, vol, np.nan)), 252)
    return {
        "PRICE_RETURN_STATE": _stack(ret_21, ret_63),
        "TREND": _stack(trend),
        "MOMENTUM": _stack(mom),
        "REVERSAL": _stack(ret_5),
        "REALISED_VOLATILITY": _stack(vol_21, vol_21 / np.where(vol_252 > 0, vol_252, np.nan)),
        "TAIL_CRASH_STATE": _stack(drawdown, skew_63),
        "LIQUIDITY": _stack(log_adv, amihud),
        "VOLUME_PARTICIPATION": _stack(vol_z),
        "_ret": r, "_vol_63": vol_63,
    }


def _panel_f_on_grid(E: dict, name: str) -> np.ndarray:
    """PANEL-F feature (cadence-21 cube) carried forward onto the daily grid.
    A value at decision slot j is known from slot j's date (facts filed <= it)
    and stays known until the next slot, which only ADDS newer facts."""
    pf = E["panel_f"]
    cube = pf["cube"][:, :, pf["f_ix"][name]]
    dec = pf["dec"]
    n_sym, n_d = E["price"]["tr"].shape
    out = np.full((n_sym, n_d), np.nan)
    for j in range(len(dec)):
        a = int(dec[j])
        b = int(dec[j + 1]) if j + 1 < len(dec) else n_d
        out[:, a:b] = cube[:, j][:, None]
    return out


def equity_fundamental_blocks(E: dict) -> dict:
    fcf = _panel_f_on_grid(E, "fcf_to_assets")
    acc = _panel_f_on_grid(E, "accruals_to_assets")
    opi = _panel_f_on_grid(E, "opinc_to_assets")
    opi_p = _panel_f_on_grid(E, "opinc_to_assets_prior")
    ag = _panel_f_on_grid(E, "asset_growth")
    sg = _panel_f_on_grid(E, "sales_growth")
    wc = _panel_f_on_grid(E, "wc_to_revenue")
    wc_p = _panel_f_on_grid(E, "wc_to_revenue_prior")
    age = _panel_f_on_grid(E, "obs_age_days")
    filed_ix = _panel_f_on_grid(E, "filed_ix")
    n_d = fcf.shape[1]
    since_filed = np.arange(n_d)[None, :] - filed_ix
    since_filed = np.where(np.isfinite(filed_ix) & (filed_ix >= 0), since_filed, np.nan)
    return {
        "FUNDAMENTAL_LEVELS": _stack(-acc, opi),
        "FREE_CASH_FLOW": _stack(fcf),
        "FUNDAMENTAL_CHANGE": _stack(opi - opi_p, ag, sg, -(wc - wc_p)),
        "CORPORATE_DISCLOSURES": _stack(age, since_filed),
    }


def equity_insider_block(E: dict) -> np.ndarray:
    ins = P.load_insider()
    dates = E["dates"]
    n_sym, n_d = E["price"]["tr"].shape
    net_63 = np.full((n_sym, n_d), np.nan)
    buys_126 = np.full((n_sym, n_d), np.nan)
    sells_126 = np.full((n_sym, n_d), np.nan)
    if len(ins) == 0:
        return _stack(net_63, buys_126, sells_126)
    cal = pd.to_datetime(dates)
    by_cik = {c: g for c, g in ins.groupby("cik")}
    for si, sym in enumerate(E["symbols"]):
        cik = E["sym2cik"].get(sym)
        g = by_cik.get(cik) if cik else None
        if g is None:
            continue
        buy = pd.Series((g["direction"] == "BUY").astype(float).values, index=g["filed"]).groupby(level=0).sum()
        sell = pd.Series((g["direction"] == "SELL").astype(float).values, index=g["filed"]).groupby(level=0).sum()
        full = pd.date_range(cal[0] - pd.Timedelta(days=200), cal[-1], freq="D")
        b = buy.reindex(full, fill_value=0.0)
        s = sell.reindex(full, fill_value=0.0)
        b63, s63 = b.rolling(63).sum(), s.rolling(63).sum()
        b126, s126 = b.rolling(126).sum(), s.rolling(126).sum()
        net = (b63 - s63) / (b63 + s63 + 1.0)
        lag = SEC_FILING_BROADCAST_LAG_SESSIONS
        net_63[si] = pit.as_of(net, dates, lag_sessions=lag)
        buys_126[si] = pit.as_of(np.log1p(b126), dates, lag_sessions=lag)
        sells_126[si] = pit.as_of(np.log1p(s126), dates, lag_sessions=lag)
    return _stack(net_63, buys_126, sells_126)


def equity_disclosure_blocks(E: dict) -> dict:
    """DISCLOSURE_INTENSITY_LANGUAGE and EVENT_INFORMATION from the acquired
    SEC submissions histories. Empty (all NaN) when nothing was acquired."""
    sub = P.load_submissions()
    dates = E["dates"]
    n_sym, n_d = E["price"]["tr"].shape
    k8_z = np.full((n_sym, n_d), np.nan)
    nt_flag = np.full((n_sym, n_d), np.nan)
    amend = np.full((n_sym, n_d), np.nan)
    days_to_exp = np.full((n_sym, n_d), np.nan)
    if len(sub) == 0:
        return {"DISCLOSURE_INTENSITY_LANGUAGE": _stack(k8_z, nt_flag, amend),
                "EVENT_INFORMATION": _stack(days_to_exp)}
    cal = pd.to_datetime(dates)
    full = pd.date_range(cal[0] - pd.Timedelta(days=400), cal[-1], freq="D")
    sub = sub.copy()
    sub["form"] = sub["form"].astype(str)
    by_cik = {c: g for c, g in sub.groupby("cik")}
    lag = SEC_FILING_BROADCAST_LAG_SESSIONS
    for si, sym in enumerate(E["symbols"]):
        cik = E["sym2cik"].get(sym)
        g = by_cik.get(cik) if cik else None
        if g is None:
            continue
        fd = pd.to_datetime(g["filing_date"])
        is8k = g["form"].str.startswith("8-K").values
        isnt = g["form"].str.startswith("NT ").values
        isam = g["form"].isin(["10-K/A", "10-Q/A"]).values
        isper = g["form"].isin(["10-K", "10-Q"]).values
        k8 = pd.Series(is8k.astype(float), index=fd).groupby(level=0).sum().reindex(full, fill_value=0.0)
        nt = pd.Series(isnt.astype(float), index=fd).groupby(level=0).sum().reindex(full, fill_value=0.0)
        am = pd.Series(isam.astype(float), index=fd).groupby(level=0).sum().reindex(full, fill_value=0.0)
        k8_63 = k8.rolling(63).sum()
        k8_252 = k8.rolling(365).sum()
        rate_z = (k8_63 - k8_252 * 63.0 / 365.0) / np.sqrt(k8_252 * 63.0 / 365.0 + 1.0)
        k8_z[si] = pit.as_of(rate_z, dates, lag_sessions=lag)
        nt_flag[si] = pit.as_of((nt.rolling(365).sum() > 0).astype(float), dates, lag_sessions=lag)
        amend[si] = pit.as_of(am.rolling(365).sum(), dates, lag_sessions=lag)
        # expected next periodic filing: last periodic filing + median gap of
        # prior gaps (as-of), expressed as days until that expected date
        per = np.sort(fd[isper].unique())
        if len(per) >= 3:
            gaps = np.diff(per).astype("timedelta64[D]").astype(float)
            med = pd.Series(gaps, index=per[1:]).expanding(min_periods=2).median()
            exp_next = pd.Series(per[1:] + pd.to_timedelta(med.values, unit="D"), index=per[1:])
            exp_on_grid = pd.Series(exp_next.values.astype("datetime64[ns]"), index=exp_next.index)
            grid = exp_on_grid.reindex(full, method="ffill")
            dte = (grid - full).dt.days.astype(float)
            days_to_exp[si] = pit.as_of(dte.where(dte > -30), dates, lag_sessions=lag)
    return {"DISCLOSURE_INTENSITY_LANGUAGE": _stack(k8_z, nt_flag, amend),
            "EVENT_INFORMATION": _stack(days_to_exp)}


def build_equity_features(E: dict) -> dict:
    out = equity_price_blocks(E)
    out.update(equity_fundamental_blocks(E))
    out["INSIDER_BEHAVIOUR"] = equity_insider_block(E)
    out.update(equity_disclosure_blocks(E))
    out["_market_level"] = set()
    return out
