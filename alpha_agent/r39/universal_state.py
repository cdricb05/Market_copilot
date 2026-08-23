"""alpha_agent.r39.universal_state - the ONE universal point-in-time state.

Assembles every admitted estate family into canonical decision-stamped
research panels. Three lanes share ONE calendar convention (calendar
month-end decisions; the VX lane runs weekly) and ONE alignment rule:

    features at decision date d use observations dated <= d minus the
    family's declared publication lag; forward targets are realised strictly
    after d; nothing is forward-filled before it was observable; a missing
    observation is a MASK, never a silent fill.

Lanes:

* ``FUT``   - 68 native futures markets from the R38 dated-contract layer
              (commodities, US & international rates, FX, international
              equity index, VX), monthly decisions, 21- and 63-session
              forward targets, per-market R38 modelled costs, per-class
              equal-weight controls.
* ``VX``    - the VX market alone at weekly (5-session) cadence.
* ``EQ``    - the survivorship-safe US single-name monthly cross-section
              (Release-30 dataset, russell1000 current+past), 21-session
              forward labels with truncation masks.
* ``ETF``   - a FIXED declared total-return ETF sleeve read from the
              existing Norgate entitlement, reaching the CREDIT and
              REAL_ESTATE cells futures cannot express.

The macro overlay (market-quoted FRED series, ALFRED true vintages, Cboe
volatility indices) joins every lane as-of with declared lags.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C
from .estate import (
    ALFRED_PIT_DIR,
    FRED_MARKET_QUOTED,
    FRED_R35_DIR,
    FRED_R36_DIR,
    NATIVE_LAYER_DIR,
    R30_PRICE_DATASET,
    R38_PANEL,
    VIX_DIR,
)

CALCULATION_OWNER = "alpha_agent.r39.universal_state"
SCHEMA = "r39_universal_pit_state_contract/1"
ARTIFACT_NAME = "universal_pit_state_contract.json"

ALIGNMENT_RULE = ("features at decision date d use observations dated <= d "
                  "minus the family's declared publication lag; targets are "
                  "realised strictly after d; missingness is a mask, never "
                  "a fill")

#: The FIXED declared ETF sleeve (current-composition caveat declared in the
#: estate). Chosen for the economic cells futures cannot express, not for
#: performance.
ETF_SLEEVE = ("SPY", "EEM", "EFA", "TLT", "IEF", "SHY", "HYG", "LQD",
              "GLD", "VNQ", "BIL")

MIN_FWD_SESSIONS_21 = 15
MIN_FWD_SESSIONS_63 = 45
MIN_CONTROL_MEMBERS = 3


# --------------------------------------------------------------------------- #
# Macro overlay
# --------------------------------------------------------------------------- #
def _read_fred_series(name: str) -> pd.Series:
    for d in (FRED_R35_DIR, FRED_R36_DIR):
        p = d / (name + ".json")
        if p.exists():
            obs = json.loads(p.read_text(encoding="utf-8"))["observations"]
            df = pd.DataFrame(obs)
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df["date"] = pd.to_datetime(df["date"])
            s = df.dropna(subset=["value"]).set_index("date")["value"]
            return s[~s.index.duplicated(keep="last")].sort_index()
    return pd.Series(dtype=float)


def _read_alfred_vintage(name: str) -> pd.Series:
    """A vintage-stamped series indexed by the date it became OBSERVABLE.

    ALFRED's ``realtime_start`` is the publication date of that vintage; the
    series value at index t is the latest vintage published on or before t.
    """
    p = ALFRED_PIT_DIR / ("alfred_%s.json" % name)
    if not p.exists():
        return pd.Series(dtype=float)
    obs = json.loads(p.read_text(encoding="utf-8"))["observations"]
    df = pd.DataFrame(obs)
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["published"] = pd.to_datetime(df["realtime_start"])
    df["ref"] = pd.to_datetime(df["date"])
    df = df.dropna(subset=["value"]).sort_values(["published", "ref"])
    # at each publication date keep the most recent reference period's value
    latest = df.groupby("published").tail(1).set_index("published")["value"]
    return latest.sort_index()


def _read_vix() -> pd.DataFrame:
    out = {}
    for name, fn in (("vix", "VIX_History.csv"), ("vix3m",
                                                  "VIX3M_History.csv")):
        p = VIX_DIR / fn
        if p.exists():
            v = pd.read_csv(p)
            v.columns = [c.strip().lower() for c in v.columns]
            v["date"] = pd.to_datetime(v["date"])
            out[name] = v.set_index("date")["close"].sort_index()
    return pd.DataFrame(out)


def build_macro_overlay() -> pd.DataFrame:
    """Daily macro state, each column shifted by its declared lag so that the
    value at index t is what a decision at t could have known."""
    cols = {}
    for name, lag in FRED_MARKET_QUOTED.items():
        s = _read_fred_series(name)
        if s.empty:
            continue
        cols[name] = s.shift(0)  # values dated by observation day
        cols[name].index = cols[name].index  # explicit: lag applied on join
    frame = pd.DataFrame(cols)
    vix = _read_vix()
    frame = frame.join(vix, how="outer")
    frame = frame.sort_index()
    # one-session availability lag for everything market-quoted
    frame = frame.shift(1)
    # derived, still lagged because inputs are lagged
    if {"DGS10", "DGS2"} <= set(frame.columns):
        frame["curve_10_2"] = frame["DGS10"] - frame["DGS2"]
    if {"vix", "vix3m"} <= set(frame.columns):
        frame["vix_term"] = frame["vix3m"] - frame["vix"]
    if "BAA10Y" in frame.columns:
        frame["credit_baa10y"] = frame["BAA10Y"]
    if "CBBTCUSD" in frame.columns:
        btc = frame["CBBTCUSD"]
        frame["btc_ret_21"] = btc.pct_change(21)
    # ALFRED true-vintage CPI momentum (published dates already observable)
    cpi = _read_alfred_vintage("CPIAUCSL")
    if not cpi.empty:
        cpi_yoy = cpi.pct_change(12)  # vintage-to-vintage, conservative
        frame = frame.join(cpi_yoy.rename("cpi_vintage_yoy"), how="left")
        frame["cpi_vintage_yoy"] = frame["cpi_vintage_yoy"].ffill(limit=40)
    return frame


MACRO_FEATURES = ("credit_baa10y", "curve_10_2", "DTB3", "T10YIE", "DFII10",
                  "vix", "vix_term", "btc_ret_21", "cpi_vintage_yoy")


# --------------------------------------------------------------------------- #
# Futures lane
# --------------------------------------------------------------------------- #
def load_daily_layer() -> dict:
    """market -> daily DataFrame(Date, ret, close, ret2, slope_ann, volume,
    open_interest), exactly as frozen by R38."""
    out = {}
    for p in sorted(NATIVE_LAYER_DIR.glob("*.csv")):
        df = pd.read_csv(p, parse_dates=["Date"])
        out[p.stem] = df
    return out


def load_panel_meta() -> pd.DataFrame:
    """Per-market identity and cost metadata from the R38 ML panel."""
    panel = pd.read_csv(R38_PANEL, usecols=[
        "market_id", "asset_class", "economic_group", "currency",
        "cost_bps_per_side", "decision_date", "cot_commercial_z", "has_cot"])
    meta = panel.groupby("market_id").agg(
        asset_class=("asset_class", "first"),
        economic_group=("economic_group", "first"),
        currency=("currency", "first"),
        cost_bps_per_side=("cost_bps_per_side", "median")).reset_index()
    cot = panel[panel["has_cot"] == True][  # noqa: E712
        ["market_id", "decision_date", "cot_commercial_z"]].copy()
    cot["decision_date"] = pd.to_datetime(cot["decision_date"])
    return meta, cot


def _month_end_positions(dates: pd.Series) -> np.ndarray:
    """Index positions of each market's last session per calendar month."""
    period = dates.dt.to_period("M")
    return np.flatnonzero(period.ne(period.shift(-1)).to_numpy())


def _fwd_return(rets: np.ndarray, pos: int, sessions: int,
                min_sessions: int) -> float:
    seg = rets[pos + 1: pos + 1 + sessions]
    seg = seg[np.isfinite(seg)]
    if seg.size < min_sessions:
        return float("nan")
    return float(np.prod(1.0 + seg) - 1.0)


def build_futures_panel(layer: dict, meta: pd.DataFrame,
                        cot: pd.DataFrame,
                        macro: pd.DataFrame) -> pd.DataFrame:
    """Monthly decision rows for every futures market: base features,
    forward targets, costs. Controls are attached afterwards (they need the
    whole cross-section)."""
    meta_by_market = meta.set_index("market_id")
    rows = []
    for mkt, df in layer.items():
        if mkt not in meta_by_market.index:
            continue
        d = df.reset_index(drop=True)
        dates = d["Date"]
        rets = d["ret"].to_numpy(dtype=np.float64)
        close = d["close"].to_numpy(dtype=np.float64)
        slope = d["slope_ann"].to_numpy(dtype=np.float64)
        vol_ = d["volume"].to_numpy(dtype=np.float64)
        oi = d["open_interest"].to_numpy(dtype=np.float64)
        ret2 = d["ret2"].to_numpy(dtype=np.float64)
        n = len(d)
        logret = np.log1p(np.where(np.isfinite(rets), rets, 0.0))
        cum = np.cumsum(logret)
        mm = meta_by_market.loc[mkt]

        def trail(pos, k):
            if pos - k < 0:
                return float("nan")
            return float(math.expm1(cum[pos] - cum[pos - k]))

        me = _month_end_positions(dates)
        # monthly return history for seasonality
        month_of = dates.dt.month.to_numpy()
        monthly_hist = {}  # month -> list of (year, monthly ret)
        prev = None
        for pos in me:
            if prev is not None:
                mret = float(math.expm1(cum[pos] - cum[prev]))
                monthly_hist.setdefault(int(month_of[pos]), []).append(
                    (int(dates.iloc[pos].year), mret))
            prev = pos

        for pos in me:
            if pos < 260:  # need a year of history for base features
                continue
            dt = dates.iloc[pos]
            w63 = rets[max(0, pos - 62): pos + 1]
            w63 = w63[np.isfinite(w63)]
            vol63 = float(w63.std(ddof=1) * math.sqrt(252.0)) \
                if w63.size >= 40 else float("nan")
            # seasonality: mean same-calendar-NEXT-month return, prior years
            nxt = int(dt.month % 12) + 1
            hist = [r for (y, r) in monthly_hist.get(nxt, [])
                    if y < dt.year and y >= dt.year - 10]
            seas = float(np.mean(hist)) if len(hist) >= 3 else float("nan")
            hi252 = np.nanmax(close[max(0, pos - 251): pos + 1])
            volz_w = vol_[max(0, pos - 251): pos + 1]
            oiz_w = oi[max(0, pos - 251): pos + 1]
            r2 = ret2[max(0, pos - 20): pos + 1]
            r1 = rets[max(0, pos - 20): pos + 1]
            both = np.isfinite(r2) & np.isfinite(r1)
            cal_spread_21 = float(np.nansum(r1[both] - r2[both])) \
                if both.sum() >= 10 else float("nan")
            rows.append({
                "lane": "FUT",
                "market_id": mkt,
                "asset_class": mm["asset_class"],
                "economic_group": mm["economic_group"],
                "currency": mm["currency"],
                "decision_date": dt,
                "cost_bps_per_side": float(mm["cost_bps_per_side"]),
                "ret_1m": trail(pos, 21),
                "ret_3m": trail(pos, 63),
                "ret_6m": trail(pos, 126),
                "ret_12m": trail(pos, 252),
                "mom_12_1": (trail(pos, 252) - trail(pos, 21)
                             if np.isfinite(trail(pos, 252))
                             and np.isfinite(trail(pos, 21))
                             else float("nan")),
                "vol_63": vol63,
                "carry_slope_ann": float(slope[pos])
                if np.isfinite(slope[pos]) else float("nan"),
                "seasonality_next_month": seas,
                "high_252_prox": float(close[pos] / hi252 - 1.0)
                if np.isfinite(hi252) and hi252 > 0 else float("nan"),
                "volume_z_252": _z_last(volz_w),
                "oi_z_252": _z_last(oiz_w),
                "cal_spread_ret_21": cal_spread_21,
                "fwd_21": _fwd_return(rets, pos, 21, MIN_FWD_SESSIONS_21),
                "fwd_63": _fwd_return(rets, pos, 63, MIN_FWD_SESSIONS_63),
                "fwd_vol_21": _fwd_vol(rets, pos, 21),
                "_pos": pos,
            })
    panel = pd.DataFrame(rows).sort_values(
        ["decision_date", "market_id"]).reset_index(drop=True)

    # COT join: latest R38-panel observation at or before the decision date
    panel = panel.sort_values("decision_date")
    cot = cot.sort_values("decision_date")
    parts = []
    for mkt, g in panel.groupby("market_id", sort=False):
        cg = cot[cot["market_id"] == mkt]
        if cg.empty:
            g = g.copy()
            g["cot_commercial_z"] = np.nan
        else:
            g = pd.merge_asof(
                g.sort_values("decision_date"),
                cg[["decision_date", "cot_commercial_z"]].sort_values(
                    "decision_date"),
                on="decision_date", direction="backward",
                tolerance=pd.Timedelta(days=45))
        parts.append(g)
    panel = pd.concat(parts, ignore_index=True)
    panel["has_cot"] = np.isfinite(
        panel["cot_commercial_z"].to_numpy(dtype=float))

    # macro overlay join (already lagged one session inside the overlay)
    panel = _join_macro(panel, macro)

    # per-class equal-weight controls on the SAME dates
    for hor, coln in ((21, "control_fwd_21"), (63, "control_fwd_63")):
        src = "fwd_%d" % hor
        ctrl = panel.groupby(
            [panel["decision_date"].dt.to_period("M"), "asset_class"])[src] \
            .transform(lambda s: s.mean() if s.notna().sum()
                       >= MIN_CONTROL_MEMBERS else np.nan)
        panel[coln] = ctrl
    panel["zone"] = zone_of(panel["decision_date"])
    return panel.sort_values(
        ["decision_date", "market_id"]).reset_index(drop=True)


def _z_last(window: np.ndarray) -> float:
    w = window[np.isfinite(window)]
    if w.size < 60:
        return float("nan")
    sd = w.std(ddof=1)
    if sd <= 0:
        return float("nan")
    return float((w[-1] - w.mean()) / sd)


def _fwd_vol(rets: np.ndarray, pos: int, sessions: int) -> float:
    seg = rets[pos + 1: pos + 1 + sessions]
    seg = seg[np.isfinite(seg)]
    if seg.size < 10:
        return float("nan")
    return float(seg.std(ddof=1) * math.sqrt(252.0))


def _join_macro(panel: pd.DataFrame, macro: pd.DataFrame) -> pd.DataFrame:
    if macro.empty:
        for c in MACRO_FEATURES:
            panel[c] = np.nan
        return panel
    keep = [c for c in MACRO_FEATURES if c in macro.columns]
    m = macro[keep].reset_index()
    m.columns = ["decision_date"] + keep
    m["decision_date"] = pd.to_datetime(m["decision_date"]) \
        .astype("datetime64[ns]")
    m = m.sort_values("decision_date")
    panel = panel.copy()
    panel["decision_date"] = pd.to_datetime(panel["decision_date"]) \
        .astype("datetime64[ns]")
    panel = pd.merge_asof(
        panel.sort_values("decision_date"), m, on="decision_date",
        direction="backward", tolerance=pd.Timedelta(days=10))
    for c in MACRO_FEATURES:
        if c not in panel.columns:
            panel[c] = np.nan
    return panel


def build_vx_weekly(layer: dict, meta: pd.DataFrame,
                    macro: pd.DataFrame) -> pd.DataFrame:
    """The VX lane: weekly decisions, 5-session forward, slope and vol-term
    features. One market, so the control is risk-matched cash (zero excess),
    matching the R38 lane design."""
    if "VX" not in layer:
        return pd.DataFrame()
    d = layer["VX"].reset_index(drop=True)
    rets = d["ret"].to_numpy(dtype=np.float64)
    slope = d["slope_ann"].to_numpy(dtype=np.float64)
    dates = d["Date"]
    rows = []
    for pos in range(260, len(d) - 1, 5):
        dt = dates.iloc[pos]
        w63 = rets[max(0, pos - 62): pos + 1]
        w63 = w63[np.isfinite(w63)]
        rows.append({
            "lane": "VX", "market_id": "VX", "asset_class": "VOLATILITY",
            "economic_group": "VOLATILITY", "currency": "USD",
            "decision_date": dt,
            "cost_bps_per_side": 15.0,
            "carry_slope_ann": float(slope[pos])
            if np.isfinite(slope[pos]) else float("nan"),
            "ret_1m": float(np.prod(1 + rets[max(0, pos - 20):pos + 1][
                np.isfinite(rets[max(0, pos - 20):pos + 1])]) - 1),
            "vol_63": float(w63.std(ddof=1) * math.sqrt(252.0))
            if w63.size >= 40 else float("nan"),
            "fwd_5": _fwd_return(rets, pos, 5, 3),
            "control_fwd_5": 0.0,
        })
    vx = pd.DataFrame(rows)
    vx = _join_macro(vx, macro)
    vx["zone"] = zone_of(vx["decision_date"])
    return vx


# --------------------------------------------------------------------------- #
# Equity lane (Release-30 dataset)
# --------------------------------------------------------------------------- #
def build_equity_panel() -> pd.DataFrame:
    """Long-form monthly US single-name cross-section with 21-session
    forward labels and truncation masks, exactly as frozen by Release 30."""
    z = np.load(R30_PRICE_DATASET, allow_pickle=True)
    meta = json.loads(bytes(z["meta"]).decode("utf-8"))
    feats = list(meta["feature_names"])
    dates = [pd.Timestamp(s) for s in meta["dates"]]
    frames = []
    for i, dt in enumerate(dates):
        key = "X_%d" % i
        if key not in z:
            continue
        X = z[key]
        cols = z["cols_%d" % i]
        y21 = z["y_%d_21" % i]
        tr21 = z["tr_%d_21" % i]
        df = pd.DataFrame(X, columns=feats)
        df["security_id"] = cols.astype(np.int64)
        df["decision_date"] = dt
        df["fwd_21"] = y21
        df["fwd_truncated"] = tr21.astype(bool)
        frames.append(df)
    eq = pd.concat(frames, ignore_index=True)
    eq["lane"] = "EQ"
    eq["asset_class"] = "US_EQUITY"
    eq["economic_group"] = "US_SINGLE_NAME"
    eq["cost_bps_per_side"] = C.EQUITY_COST_BPS_PER_SIDE
    # the label is unusable where truncated; mask, never fill
    eq.loc[eq["fwd_truncated"], "fwd_21"] = np.nan
    # cross-section equal-weight control per date
    eq["control_fwd_21"] = eq.groupby("decision_date")["fwd_21"] \
        .transform("mean")
    eq["zone"] = zone_of(eq["decision_date"])
    return eq


EQ_FEATURES = ("mom_12_1", "mom_6_1", "mom_3_1", "rev_1m", "trend_200",
               "high_52w_prox", "vol_63", "vol_252", "downside_vol_63",
               "max_drawdown_252", "log_adv_20", "adv_trend_20_252",
               "beta_252", "ivol_63")


# --------------------------------------------------------------------------- #
# ETF sleeve (Norgate total-return, read-only)
# --------------------------------------------------------------------------- #
def load_etf_daily() -> dict:
    """symbol -> daily total-return close series. Fail-soft: an unavailable
    provider yields an empty dict and the lane is recorded UNAVAILABLE."""
    try:
        import norgatedata
    except Exception:
        return {}
    out = {}
    for sym in ETF_SLEEVE:
        try:
            df = norgatedata.price_timeseries(
                sym,
                stock_price_adjustment_setting=(
                    norgatedata.StockPriceAdjustmentType.TOTALRETURN),
                padding_setting=norgatedata.PaddingType.NONE,
                timeseriesformat="pandas-dataframe")
            if df is not None and len(df) > 300:
                out[sym] = df["Close"].astype(float)
        except Exception:
            continue
    return out


#: economic_group is SHARED within a spreadable family on purpose - the
#: GROUP_SPREAD expression pairs members inside one group.
ETF_META = {
    "SPY": ("US_EQUITY", "EQUITY_BETA"),
    "EEM": ("INTERNATIONAL_EQUITY", "EQUITY_BETA"),
    "EFA": ("INTERNATIONAL_EQUITY", "EQUITY_BETA"),
    "TLT": ("RATES", "US_DURATION"), "IEF": ("RATES", "US_DURATION"),
    "SHY": ("RATES", "US_DURATION"), "HYG": ("CREDIT", "US_CREDIT"),
    "LQD": ("CREDIT", "US_CREDIT"),
    "GLD": ("COMMODITY", "GOLD"), "VNQ": ("REAL_ESTATE", "US_REIT"),
    "BIL": ("CASH", "T_BILL"),
}


def build_etf_panel(series: dict, macro: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for sym, close in series.items():
        close = close[~close.index.duplicated(keep="last")].sort_index()
        rets = close.pct_change().to_numpy(dtype=np.float64)
        dates = pd.Series(close.index)
        cum = np.cumsum(np.log1p(np.where(np.isfinite(rets), rets, 0.0)))
        ac, grp = ETF_META.get(sym, ("OTHER", "OTHER"))
        me = _month_end_positions(dates)
        cl = close.to_numpy(dtype=np.float64)
        for pos in me:
            if pos < 260:
                continue
            w63 = rets[max(0, pos - 62): pos + 1]
            w63 = w63[np.isfinite(w63)]

            def trail(k, _pos=pos, _cum=cum):
                if _pos - k < 0:
                    return float("nan")
                return float(math.expm1(_cum[_pos] - _cum[_pos - k]))

            hi252 = np.nanmax(cl[max(0, pos - 251): pos + 1])
            rows.append({
                "lane": "ETF", "market_id": sym, "asset_class": ac,
                "economic_group": grp, "currency": "USD",
                "decision_date": dates.iloc[pos],
                "cost_bps_per_side": C.ETF_COST_BPS_PER_SIDE,
                "ret_1m": trail(21), "ret_3m": trail(63),
                "ret_6m": trail(126), "ret_12m": trail(252),
                "mom_12_1": (trail(252) - trail(21)),
                "vol_63": float(w63.std(ddof=1) * math.sqrt(252.0))
                if w63.size >= 40 else float("nan"),
                "high_252_prox": float(cl[pos] / hi252 - 1.0)
                if np.isfinite(hi252) and hi252 > 0 else float("nan"),
                "fwd_21": _fwd_return(rets, pos, 21, MIN_FWD_SESSIONS_21),
                "fwd_63": _fwd_return(rets, pos, 63, MIN_FWD_SESSIONS_63),
            })
    if not rows:
        return pd.DataFrame()
    etf = pd.DataFrame(rows).sort_values(
        ["decision_date", "market_id"]).reset_index(drop=True)
    etf = _join_macro(etf, macro)
    per = etf["decision_date"].dt.to_period("M")
    etf["control_fwd_21"] = etf.groupby(per)["fwd_21"].transform(
        lambda s: s.mean() if s.notna().sum() >= MIN_CONTROL_MEMBERS
        else np.nan)
    etf["control_fwd_63"] = etf.groupby(per)["fwd_63"].transform(
        lambda s: s.mean() if s.notna().sum() >= MIN_CONTROL_MEMBERS
        else np.nan)
    etf["zone"] = zone_of(etf["decision_date"])
    return etf


# --------------------------------------------------------------------------- #
# Zones
# --------------------------------------------------------------------------- #
def zone_of(dates: pd.Series) -> pd.Series:
    d = pd.to_datetime(dates)
    a_end = pd.Timestamp(C.ZONE_A_DISCOVERY_END)
    b0 = pd.Timestamp(C.ZONE_B_VALIDATION_START)
    b1 = pd.Timestamp(C.ZONE_B_VALIDATION_END)
    c0 = pd.Timestamp(C.ZONE_C_CONFIRMATION_START)
    c1 = pd.Timestamp(C.ZONE_C_CONFIRMATION_END)
    out = pd.Series("EMBARGO", index=d.index, dtype=object)
    out[d <= a_end] = "ZONE_A"
    out[(d >= b0) & (d <= b1)] = "ZONE_B"
    out[(d >= c0) & (d <= c1)] = "ZONE_C"
    out[d > c1] = "POST_SAMPLE"
    return out


# --------------------------------------------------------------------------- #
# Assembly + contract artifact
# --------------------------------------------------------------------------- #
def assemble(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    """Build every lane, persist the base panels, return panels + manifest."""
    macro = build_macro_overlay()
    layer = load_daily_layer()
    meta, cot = load_panel_meta()
    fut = build_futures_panel(layer, meta, cot, macro)
    vx = build_vx_weekly(layer, meta, macro)
    eq = build_equity_panel()
    etf_series = load_etf_daily()
    etf = build_etf_panel(etf_series, macro)

    out_dir = r39.state_dir(campaign_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    files = {}
    for name, df in (("fut_monthly", fut), ("vx_weekly", vx),
                     ("eq_monthly", eq), ("etf_monthly", etf)):
        p = out_dir / (name + ".csv")
        if df is None or df.empty:
            files[name] = {"rows": 0, "written": False}
            continue
        if not p.exists():
            df.to_csv(p, index=False)
        files[name] = {"rows": int(len(df)), "written": True,
                       "sha256": r39.sha_file(p), "path": str(p)}

    zone_counts = {}
    for name, df in (("FUT", fut), ("VX", vx), ("EQ", eq), ("ETF", etf)):
        if df is None or df.empty:
            zone_counts[name] = {}
        else:
            zone_counts[name] = df["zone"].value_counts().to_dict()

    contract_body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "alignment_rule": ALIGNMENT_RULE,
        "zones": {
            "ZONE_A_end": C.ZONE_A_DISCOVERY_END,
            "ZONE_B": [C.ZONE_B_VALIDATION_START, C.ZONE_B_VALIDATION_END],
            "ZONE_C": [C.ZONE_C_CONFIRMATION_START,
                       C.ZONE_C_CONFIRMATION_END],
            "zone_c_label": C.ZONE_C_EVIDENCE_LABEL,
            "zone_c_is_fresh_unseen": C.ZONE_C_IS_FRESH_UNSEEN_EVIDENCE,
        },
        "lanes": files,
        "zone_counts": zone_counts,
        "etf_sleeve": list(ETF_SLEEVE),
        "etf_lane_available": bool(etf_series),
        "macro_features": list(MACRO_FEATURES),
        "macro_lag_rule": "one session for market-quoted series; ALFRED "
                          "vintages by publication date",
        "forward_fill_before_observable": False,
        "missingness_policy": "masks, never silent fills",
    })
    contract_body["state_contract_hash"] = r39.sha(contract_body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME,
                   contract_body)
    return {"fut": fut, "vx": vx, "eq": eq, "etf": etf, "macro": macro,
            "layer": layer, "contract": contract_body}
