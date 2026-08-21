"""alpha_agent.r33.features - the ONE Release 33 feature registry.

Bounded and economically interpretable, by design. This module deliberately does
NOT search a library of thousands of technical transforms: Release 31 already
established that the binding constraint on this estate is INFORMATION rather
than METHOD, and a wide transform search buys data-mining risk instead of
knowledge. Every family below is declared, has an economic reading, and is
counted in the multiple-testing denominator through the configurations that use
it.

Two things are enforced rather than intended:

* **No look-ahead.** Every feature at decision index ``i`` is a backward-looking
  function of data through the close of session ``i``. The position is not
  entered until ``i+1``.
* **Global state is lagged.** The daily yield and spread series deliver one
  session later than the price series, so every global state variable is lagged
  one session before it is broadcast. Uniform, and it costs nothing.

Carry is declared per asset class rather than invented. A bond's carry is
observable from the owned yield curve. FX carry needs foreign short rates and
commodity carry needs a futures term structure; the owned estate has NEITHER, so
those features are ABSENT and are recorded as absent rather than approximated by
something that would look like carry and measure something else.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as _contract
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r33.features"
REGISTRY_SCHEMA = "r33_feature_registry/1"
ARTIFACT_NAME = "feature_registry.json"

SESSIONS_PER_YEAR = 252.0

#: Global state series are lagged this many sessions before broadcast.
GLOBAL_STATE_LAG_SESSIONS = 1

# --------------------------------------------------------------------------- #
# Declared families
# --------------------------------------------------------------------------- #
FAM_TREND = "MULTI_HORIZON_TREND"
FAM_TREND_NORM = "NORMALISED_TREND"
FAM_TSMOM = "TIME_SERIES_MOMENTUM"
FAM_BREAKOUT = "BREAKOUT_DISTANCE_FROM_RANGE"
FAM_VOL = "REALISED_VOLATILITY"
FAM_VOL_CHANGE = "VOLATILITY_CHANGE"
FAM_DOWNSIDE = "DOWNSIDE_VOLATILITY"
FAM_TAIL = "SKEW_TAIL_DIAGNOSTICS"
FAM_XS_MOM = "CROSS_SECTIONAL_MOMENTUM"
FAM_GROUP_RS = "RELATIVE_STRENGTH_WITHIN_GROUP"
FAM_CORR = "CORRELATION_STATE"
FAM_GLOBAL = "CROSS_ASSET_RISK_STATE"
FAM_CARRY = "TERM_STRUCTURE_CARRY"
FAM_DRAWDOWN = "DRAWDOWN_RECOVERY_STATE"
FAM_REVERSAL = "SHORT_TERM_REVERSAL"

FAMILIES = (FAM_TREND, FAM_TREND_NORM, FAM_TSMOM, FAM_BREAKOUT, FAM_VOL,
            FAM_VOL_CHANGE, FAM_DOWNSIDE, FAM_TAIL, FAM_XS_MOM, FAM_GROUP_RS,
            FAM_CORR, FAM_GLOBAL, FAM_CARRY, FAM_DRAWDOWN, FAM_REVERSAL)

#: feature name -> (family, economic reading)
FEATURES = {
    "trend_21": (FAM_TREND, "one-month price trend"),
    "trend_63": (FAM_TREND, "one-quarter price trend"),
    "trend_126": (FAM_TREND, "half-year price trend"),
    "trend_252": (FAM_TREND, "one-year price trend"),
    "trend_norm_63": (FAM_TREND_NORM, "quarterly trend per unit of risk"),
    "trend_norm_252": (FAM_TREND_NORM, "annual trend per unit of risk"),
    "tsmom_12_1": (FAM_TSMOM, "twelve-month momentum skipping the last month"),
    "vol_21": (FAM_VOL, "one-month realised volatility"),
    "vol_63": (FAM_VOL, "one-quarter realised volatility"),
    "vol_ratio_21_63": (FAM_VOL_CHANGE, "volatility expansion or contraction"),
    "downside_vol_63": (FAM_DOWNSIDE, "semi-deviation of negative returns"),
    "skew_63": (FAM_TAIL, "return asymmetry"),
    "kurt_63": (FAM_TAIL, "tail heaviness"),
    "range_pos_252": (FAM_BREAKOUT, "position within the one-year range"),
    "drawdown_252": (FAM_DRAWDOWN, "distance below the one-year high"),
    "xs_rank_252": (FAM_XS_MOM, "cross-sectional rank of annual trend"),
    "group_rel_63": (FAM_GROUP_RS, "quarterly trend relative to economic group"),
    "corr_state_63": (FAM_CORR, "average correlation with the panel"),
    "reversal_5": (FAM_REVERSAL, "one-week reversal"),
    "bond_carry_slope": (FAM_CARRY, "10-year minus 3-month yield, bonds only"),
    "g_vix_level": (FAM_GLOBAL, "log equity implied volatility"),
    "g_vix_change_21": (FAM_GLOBAL, "one-month change in implied volatility"),
    "g_yield_slope": (FAM_GLOBAL, "10-year minus 13-week Treasury yield"),
    "g_credit_spread": (FAM_GLOBAL, "Baa minus Aaa corporate yield"),
    "g_usd_trend_63": (FAM_GLOBAL, "quarterly trend in the dollar index"),
    "g_commodity_trend_63": (FAM_GLOBAL, "quarterly trend in broad commodities"),
    "g_breadth": (FAM_GLOBAL, "share of S&P 500 above its 200-day average"),
    "g_move_level": (FAM_GLOBAL, "log Treasury implied volatility"),
}

FEATURE_NAMES = tuple(sorted(FEATURES))

#: Global state symbols. Absent series degrade to a recorded absence.
GLOBAL_SYMBOLS = {
    "g_vix_level": "$VIX",
    "g_yield_slope": ("%TNX", "%IRX"),
    "g_credit_spread": ("%COBAA", "%COAAA"),
    "g_usd_trend_63": "$USDX",
    "g_commodity_trend_63": "$BCOM",
    "g_breadth": "#SPX%MA200",
    "g_move_level": "$MOVE",
}

#: Carry availability, declared per asset class. False means the owned estate
#: cannot support the feature, NOT that it was judged unimportant.
CARRY_AVAILABILITY = {
    _universe.AC_GOVT: True,
    _universe.AC_CREDIT: True,
    _universe.AC_FX: False,
    _universe.AC_COMMODITY: False,
    _universe.AC_PRECIOUS: False,
    _universe.AC_EQUITY: False,
}
#: Features that are ABSENT BY CONSTRUCTION for some asset classes. These are
#: filled with a neutral zero rather than a cross-sectional median, so "no carry
#: is observable here" stays distinguishable from "carry happened to be median".
STRUCTURALLY_ABSENT_FILL_ZERO = ("bond_carry_slope",)

CARRY_ABSENCE_REASON = {
    _universe.AC_FX: ("FX carry is the interest differential; the owned estate "
                      "has US short rates but no foreign short-rate history"),
    _universe.AC_COMMODITY: ("commodity carry is the futures term structure; "
                             "the owned Continuous Futures entitlement is one "
                             "market, so no curve exists"),
    _universe.AC_PRECIOUS: ("same as commodities: no owned futures curve"),
    _universe.AC_EQUITY: ("equity index carry is the dividend yield less "
                          "financing; the owned indices are price indices"),
}


# --------------------------------------------------------------------------- #
# Per-market features
# --------------------------------------------------------------------------- #
def _rolling_vol(logret: pd.DataFrame, window: int) -> pd.DataFrame:
    return logret.rolling(window, min_periods=max(5, window // 2)).std(ddof=1) \
        * np.sqrt(SESSIONS_PER_YEAR)


def build_market_features(panel: dict) -> dict:
    """Feature frames keyed by feature name, each ``dates x markets``."""
    px = panel["prices"]
    lr = panel["log_returns"]
    logpx = np.log(px)
    out: dict = {}

    for w in (21, 63, 126, 252):
        out[f"trend_{w}"] = logpx.diff(w)

    vol21 = _rolling_vol(lr, 21)
    vol63 = _rolling_vol(lr, 63)
    vol252 = _rolling_vol(lr, 252)
    out["vol_21"], out["vol_63"] = vol21, vol63
    out["vol_ratio_21_63"] = vol21 / vol63.replace(0.0, np.nan)

    # Trend per unit of risk: the scale a trend is measured on is the risk it
    # was taken at, which is why a raw trend comparison across a bond index and
    # a commodity index means very little.
    out["trend_norm_63"] = out["trend_63"] / (
        vol63.replace(0.0, np.nan) * np.sqrt(63.0 / SESSIONS_PER_YEAR))
    out["trend_norm_252"] = out["trend_252"] / (
        vol252.replace(0.0, np.nan) * np.sqrt(1.0))
    out["tsmom_12_1"] = logpx.shift(21).diff(231)

    neg = lr.where(lr < 0.0, 0.0)
    out["downside_vol_63"] = neg.rolling(63, min_periods=30).std(ddof=1) \
        * np.sqrt(SESSIONS_PER_YEAR)
    out["skew_63"] = lr.rolling(63, min_periods=30).skew()
    out["kurt_63"] = lr.rolling(63, min_periods=30).kurt()

    hi = px.rolling(252, min_periods=126).max()
    lo = px.rolling(252, min_periods=126).min()
    out["range_pos_252"] = (px - lo) / (hi - lo).replace(0.0, np.nan)
    out["drawdown_252"] = px / hi.replace(0.0, np.nan) - 1.0
    out["reversal_5"] = -logpx.diff(5)

    out["xs_rank_252"] = out["trend_252"].rank(axis=1, pct=True) - 0.5

    groups: dict = {}
    for sym, m in panel["meta"].items():
        groups.setdefault(m["economic_group"], []).append(sym)
    rel = pd.DataFrame(np.nan, index=px.index, columns=px.columns)
    t63 = out["trend_63"]
    for _g, syms in groups.items():
        if len(syms) < 2:
            continue
        block = t63[syms]
        rel[syms] = block.sub(block.mean(axis=1), axis=0)
    out["group_rel_63"] = rel

    # Correlation state: how much a market is currently moving with everything
    # else. A high-correlation regime is one where diversification is absent.
    demeaned = lr.sub(lr.mean(axis=1), axis=0)
    panel_mean = lr.mean(axis=1)
    cov = demeaned.rolling(63, min_periods=30).cov(panel_mean)
    sd_m = lr.rolling(63, min_periods=30).std(ddof=1)
    sd_p = panel_mean.rolling(63, min_periods=30).std(ddof=1)
    out["corr_state_63"] = cov.div(sd_m.mul(sd_p, axis=0)).clip(-1.0, 1.0)

    return out


def build_global_state(panel: dict) -> pd.DataFrame:
    """Global state variables, lagged and broadcast to every market."""
    calendar = panel["calendar"]
    cols: dict = {}

    def aligned(symbol):
        s = _universe.load_close(symbol)
        if s is None:
            return None
        s = s.reindex(s.index.union(calendar)).sort_index().ffill(limit=5)
        return s.reindex(calendar)

    vix = aligned("$VIX")
    if vix is not None:
        cols["g_vix_level"] = np.log(vix.clip(lower=1e-6))
        cols["g_vix_change_21"] = np.log(vix.clip(lower=1e-6)).diff(21)

    tnx, irx = aligned("%TNX"), aligned("%IRX")
    if tnx is not None and irx is not None:
        cols["g_yield_slope"] = tnx - irx

    baa, aaa = aligned("%COBAA"), aligned("%COAAA")
    if baa is not None and aaa is not None:
        cols["g_credit_spread"] = baa - aaa

    usdx = aligned("$USDX")
    if usdx is not None:
        cols["g_usd_trend_63"] = np.log(usdx).diff(63)

    bcom = aligned("$BCOM")
    if bcom is not None:
        cols["g_commodity_trend_63"] = np.log(bcom).diff(63)

    breadth = aligned("#SPX%MA200")
    if breadth is not None:
        cols["g_breadth"] = breadth / 100.0

    move = aligned("$MOVE")
    if move is not None:
        cols["g_move_level"] = np.log(move.clip(lower=1e-6))

    frame = pd.DataFrame(cols, index=calendar)
    return frame.shift(GLOBAL_STATE_LAG_SESSIONS)


def build_carry(panel: dict) -> pd.DataFrame:
    """Bond carry from the owned yield curve; NaN where carry is unavailable."""
    calendar = panel["calendar"]
    out = pd.DataFrame(np.nan, index=calendar, columns=panel["prices"].columns)

    def aligned(symbol):
        s = _universe.load_close(symbol)
        if s is None:
            return None
        s = s.reindex(s.index.union(calendar)).sort_index().ffill(limit=5)
        return s.reindex(calendar)

    tnx, irx = aligned("%TNX"), aligned("%IRX")
    if tnx is None or irx is None:
        return out
    slope = (tnx - irx).shift(GLOBAL_STATE_LAG_SESSIONS)
    for sym, m in panel["meta"].items():
        if CARRY_AVAILABILITY.get(m["asset_class"]):
            out[sym] = slope
    return out


# --------------------------------------------------------------------------- #
# Design matrix at the decision dates
# --------------------------------------------------------------------------- #
def design_matrix(panel: dict, *, horizon: int, feature_names=None) -> dict:
    """Stack features into ``(rows, features)`` at every decision date.

    Returns row-aligned arrays: ``X``, plus the ``symbol``/``date_index``/
    ``asset_class``/``economic_group`` labels each row belongs to. Rows with no
    finite feature at all are dropped; remaining gaps are filled with the
    CROSS-SECTIONAL MEDIAN of that feature on that date, which is a
    point-in-time statistic and never a full-sample one.
    """
    from . import panel as _panel

    names = list(feature_names or FEATURE_NAMES)
    market_feats = build_market_features(panel)
    global_state = build_global_state(panel)
    carry = build_carry(panel)

    idx = _panel.forecast_dates(panel["calendar"], horizon=horizon)
    symbols = list(panel["prices"].columns)
    sym_pos = {s: k for k, s in enumerate(symbols)}

    blocks = {}
    for name in names:
        if name in market_feats:
            blocks[name] = market_feats[name].to_numpy()
        elif name == "bond_carry_slope":
            blocks[name] = carry.to_numpy()
        elif name in global_state.columns:
            col = global_state[name].to_numpy()
            blocks[name] = np.repeat(col[:, None], len(symbols), axis=1)
        else:
            blocks[name] = np.full((len(panel["calendar"]), len(symbols)),
                                   np.nan)

    rows, row_sym, row_date, row_i = [], [], [], []
    for i in idx:
        mat = np.column_stack([blocks[n][i] for n in names])
        keep = np.isfinite(mat).any(axis=1)
        if not keep.any():
            continue
        # Point-in-time cross-sectional median fill, EXCEPT where a feature is
        # structurally absent for an asset class. Filling `bond_carry_slope`
        # with the cross-sectional median would hand the bond carry to every
        # currency and equity index in the panel - a feature declared
        # unavailable would then be silently present, and measuring something
        # other than what it claims.
        any_finite = np.isfinite(mat).any(axis=0)
        med = np.zeros(mat.shape[1])
        if any_finite.any():
            with np.errstate(invalid="ignore"):
                med[any_finite] = np.nanmedian(mat[:, any_finite], axis=0)
        med = np.where(np.isfinite(med), med, 0.0)
        filled = np.where(np.isfinite(mat), mat, med[None, :])
        for j, n in enumerate(names):
            if n in STRUCTURALLY_ABSENT_FILL_ZERO:
                col = mat[:, j]
                filled[:, j] = np.where(np.isfinite(col), col, 0.0)
        for k in np.flatnonzero(keep):
            rows.append(filled[k])
            row_sym.append(symbols[k])
            row_date.append(panel["calendar"][i])
            row_i.append(i)

    X = np.asarray(rows, dtype=np.float64) if rows else np.zeros((0, len(names)))
    meta = panel["meta"]
    return {
        "X": X,
        "feature_names": names,
        "symbol": np.asarray(row_sym),
        "date": pd.DatetimeIndex(row_date),
        "decision_index": np.asarray(row_i, dtype=np.int64),
        "asset_class": np.asarray([meta[s]["asset_class"] for s in row_sym]),
        "economic_group": np.asarray([meta[s]["economic_group"] for s in row_sym]),
        "symbol_position": np.asarray([sym_pos[s] for s in row_sym],
                                      dtype=np.int64),
    }


def registry_artifact(*, campaign_id: str, created_at: str,
                      available_globals: list) -> dict:
    from .. import r33
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "families": list(FAMILIES),
        "features": {n: {"family": f, "reading": r}
                     for n, (f, r) in sorted(FEATURES.items())},
        "feature_count": len(FEATURES),
        "global_state_lag_sessions": GLOBAL_STATE_LAG_SESSIONS,
        "global_state_available": sorted(available_globals),
        "global_state_declared": sorted(GLOBAL_SYMBOLS),
        "carry_availability": {k: bool(v)
                               for k, v in sorted(CARRY_AVAILABILITY.items())},
        "carry_absence_reason": dict(sorted(CARRY_ABSENCE_REASON.items())),
        "no_look_ahead": True,
        "search_policy": {
            "wide_transform_search": False,
            "reason": ("Release 31 established the binding constraint on this "
                       "estate is INFORMATION not METHOD; a wide transform "
                       "search buys data-mining risk, not knowledge"),
        },
        "missing_value_policy": "POINT_IN_TIME_CROSS_SECTIONAL_MEDIAN",
        "primary_metric_declared_before_validation": dict(
            _contract.PRIMARY_METRIC),
    }
    body = r33.artifact_body(REGISTRY_SCHEMA, payload)
    body["feature_registry_hash"] = r33.sha(payload)
    return body
