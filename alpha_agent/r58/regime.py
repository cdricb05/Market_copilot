"""alpha_agent.r58.regime - cross-asset conditioning variables, PIT by construction.

R57 already asked whether futures TRENDS are alpha and answered no. R58 asks the
different question the protocol registers: does cross-asset information CONDITION
an equity signal? The regime is therefore never traded; it only decides whether
the momentum leg gets a vote at a given decision date.

Market prices carry their own timestamps, so nothing here needs a vintage rule.
Every term at decision session t uses closes at or before t, and the definitions
are fixed in the protocol so no post-hoc regime label is possible.

    trend  the equal-risk cross-market futures composite's trailing 126-session
           dollar P&L is positive
    vol    the E-mini S&P 500's trailing 21-session realised dollar-P&L
           volatility is below its own trailing 252-session median

    futures_riskon = trend AND vol      (the pre-registered primary)
    trend_only     = trend
    vol_only       = vol
"""
from __future__ import annotations

import numpy as np

from ..r57 import futures as FUT

EQUITY_INDEX = "&ES"
VOL_SHORT = 21
VOL_LONG = 252
TREND_WINDOW = 126
RISK_WINDOW = 63
LEVERAGE_CAP = 10.0          # the R57 uniform bound; degenerate low-vol markets
                             # otherwise dominate an inverse-vol composite


def _daily_pnl(close: np.ndarray, point_values: np.ndarray) -> np.ndarray:
    d = np.diff(close, axis=1)
    pnl = np.full(close.shape, np.nan)
    pnl[:, 1:] = d * point_values[:, None]
    return pnl


def build(dec: np.ndarray) -> dict:
    """Boolean regime arrays indexed by decision slot."""
    fp = FUT.load_futures_panel()
    pv = fp["point_values"]
    pnl = _daily_pnl(fp["close_a"], pv)
    markets = fp["markets"]
    es = markets.index(EQUITY_INDEX) if EQUITY_INDEX in markets else None

    n = len(dec)
    trend = np.zeros(n, dtype=bool)
    vol = np.zeros(n, dtype=bool)
    detail = []
    for j, t in enumerate(dec):
        t = int(t)
        # --- cross-market composite, inverse-vol equal risk, data <= t
        lo_r = max(0, t - RISK_WINDOW + 1)
        win_r = pnl[:, lo_r:t + 1]
        with np.errstate(invalid="ignore"):
            sd = np.nanstd(np.where(np.isfinite(win_r), win_r, np.nan), axis=1)
        cnt = np.isfinite(win_r).sum(axis=1)
        live = (cnt >= RISK_WINDOW * 0.8) & np.isfinite(sd) & (sd > 0)
        comp = np.nan
        if live.sum() >= 20:
            scale = np.zeros_like(sd)
            scale[live] = 1.0 / sd[live]
            med = np.median(scale[live])
            scale = np.clip(scale, 0.0, LEVERAGE_CAP * med)
            lo_t = max(0, t - TREND_WINDOW + 1)
            win_t = pnl[:, lo_t:t + 1]
            contrib = np.where(np.isfinite(win_t), win_t, 0.0) * scale[:, None]
            comp = float(contrib[live].sum(axis=1).mean())
            trend[j] = comp > 0
        # --- equity-index volatility state, data <= t
        vs = vl = np.nan
        if es is not None:
            s = pnl[es, max(0, t - VOL_SHORT + 1):t + 1]
            s = s[np.isfinite(s)]
            l = pnl[es, max(0, t - VOL_LONG + 1):t + 1]
            l = l[np.isfinite(l)]
            if len(s) >= VOL_SHORT * 0.8 and len(l) >= VOL_LONG * 0.8:
                vs = float(s.std())
                # rolling 21-session vols inside the long window, for its median
                roll = np.array([l[k:k + VOL_SHORT].std()
                                 for k in range(0, len(l) - VOL_SHORT + 1, 5)])
                vl = float(np.median(roll)) if len(roll) else np.nan
                if np.isfinite(vs) and np.isfinite(vl):
                    vol[j] = vs < vl
        detail.append({"slot": j, "session": int(t), "composite_126d_pnl": comp,
                       "es_vol_21": vs, "es_vol_median_252": vl,
                       "trend": bool(trend[j]), "vol_calm": bool(vol[j])})
    return {
        "futures_riskon": trend & vol,
        "trend_only": trend,
        "vol_only": vol,
        "detail": detail,
        "definition": {
            "trend": "equal-risk inverse-vol cross-market futures composite, "
                     "trailing %d-session dollar P&L > 0, leverage bound %.0fx "
                     "the median inverse-vol scale" % (TREND_WINDOW, LEVERAGE_CAP),
            "vol": "%s trailing %d-session realised dollar-P&L volatility below "
                   "the median of its own rolling %d-session volatilities over "
                   "the trailing %d sessions" % (EQUITY_INDEX, VOL_SHORT,
                                                 VOL_SHORT, VOL_LONG),
            "pit": "every term uses closes at or before the decision session",
        },
        "n_markets": len(markets),
        "equity_index_available": es is not None,
    }
