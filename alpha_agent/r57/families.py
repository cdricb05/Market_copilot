"""alpha_agent.r57.families - the pre-registered equity signal families.

Every function maps ``(panel, t) -> scores`` (higher = better; np.nan =
unscorable) using data THROUGH session t only. The variant grid is exactly the
one the protocol registered; nothing may be added after registration.

A signal here is a FAMILY MEMBER, not a hypothesis of its own: the family is
the unit of multiple testing, validation selects one member per family, and
only that member ever sees the lockbox.
"""
from __future__ import annotations

import numpy as np


def _trailing_return(panel, t, lookback, skip):
    tr = panel["tr"]
    a = tr[:, t - skip]
    b = tr[:, t - skip - lookback]
    with np.errstate(invalid="ignore", divide="ignore"):
        r = a / b - 1.0
    return np.where(np.isfinite(r), r, np.nan)


def _daily_returns(panel, t, lookback):
    tr = panel["tr"][:, t - lookback:t + 1]
    with np.errstate(invalid="ignore", divide="ignore"):
        r = tr[:, 1:] / tr[:, :-1] - 1.0
    return r


def _spy_returns(panel, t, lookback):
    s = panel["spy_tr"][t - lookback:t + 1]
    return s[1:] / s[:-1] - 1.0


def _beta_resid(panel, t, lookback):
    """Per-name beta vs SPY and residual daily returns over the window."""
    r = _daily_returns(panel, t, lookback)
    m = _spy_returns(panel, t, lookback)
    mm = m - m.mean()
    var_m = float((mm * mm).mean()) or 1e-12
    r0 = np.where(np.isfinite(r), r, 0.0)
    cnt = np.isfinite(r).sum(axis=1)
    mean_r = r0.sum(axis=1) / np.maximum(cnt, 1)
    cov = ((r0 - mean_r[:, None]) * mm[None, :]).sum(axis=1) / np.maximum(cnt, 1)
    beta = cov / var_m
    resid = r - beta[:, None] * m[None, :]
    ok = cnt >= lookback * 0.8
    return np.where(ok, beta, np.nan), resid, ok


# --------------------------- E1 XS momentum -------------------------------- #
def mom(lookback, skip=21):
    def f(panel, t):
        return _trailing_return(panel, t, lookback, skip)
    f.__name__ = "mom_%d_%d" % (lookback, skip)
    return f


# --------------------------- E2 short reversal ----------------------------- #
def reversal(lookback):
    def f(panel, t):
        return -_trailing_return(panel, t, lookback, 0)
    f.__name__ = "rev_%d" % lookback
    return f


# --------------------------- E3 residual momentum -------------------------- #
def residual_mom(lookback, skip=21):
    def f(panel, t):
        beta, resid, ok = _beta_resid(panel, t, lookback + skip)
        seg = resid[:, :lookback]                       # excludes last `skip`
        cum = np.nansum(seg, axis=1)
        sd = np.nanstd(seg, axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            z = cum / np.where(sd > 0, sd * np.sqrt(lookback), np.nan)
        return np.where(ok, z, np.nan)
    f.__name__ = "resid_%d_%d" % (lookback, skip)
    return f


# --------------------------- E4 sector-relative momentum ------------------- #
def sector_rel_mom(lookback, skip=21):
    def f(panel, t):
        raw = _trailing_return(panel, t, lookback, skip)
        sec = panel["sectors"]
        out = np.full_like(raw, np.nan)
        for s in np.unique(sec):
            m = (sec == s) & np.isfinite(raw)
            if m.sum() >= 3:
                out[m] = raw[m] - np.nanmean(raw[m])
        return out
    f.__name__ = "srel_%d_%d" % (lookback, skip)
    return f


# --------------------------- E5 low risk ----------------------------------- #
def low_vol(lookback):
    def f(panel, t):
        r = _daily_returns(panel, t, lookback)
        sd = np.nanstd(r, axis=1)
        cnt = np.isfinite(r).sum(axis=1)
        return np.where((cnt >= lookback * 0.8) & (sd > 0), -sd, np.nan)
    f.__name__ = "lowvol_%d" % lookback
    return f


def low_beta(lookback):
    def f(panel, t):
        beta, _resid, ok = _beta_resid(panel, t, lookback)
        return np.where(ok, -beta, np.nan)
    f.__name__ = "lowbeta_%d" % lookback
    return f


# --------------------------- E6 idiosyncratic vol -------------------------- #
def low_idio_vol(lookback):
    def f(panel, t):
        _beta, resid, ok = _beta_resid(panel, t, lookback)
        sd = np.nanstd(resid, axis=1)
        return np.where(ok & (sd > 0), -sd, np.nan)
    f.__name__ = "idiovol_%d" % lookback
    return f


# --------------------------- E7 proximity to high -------------------------- #
def high_proximity(lookback):
    def f(panel, t):
        tr = panel["tr"][:, t - lookback:t + 1]
        hi = np.nanmax(tr, axis=1)
        cur = panel["tr"][:, t]
        with np.errstate(invalid="ignore", divide="ignore"):
            x = cur / hi
        return np.where(np.isfinite(x), x, np.nan)
    f.__name__ = "hi52_%d" % lookback
    return f


# --------------------------- E8 liquidity ---------------------------------- #
def amihud(lookback):
    def f(panel, t):
        r = _daily_returns(panel, t, lookback)
        dvol = (panel["un"] * panel["vol"])[:, t - lookback + 1:t + 1]
        with np.errstate(invalid="ignore", divide="ignore"):
            illiq = np.abs(r) / dvol
        m = np.nanmean(np.where(np.isfinite(illiq), illiq, np.nan), axis=1)
        return np.where(np.isfinite(m) & (m > 0), np.log(m), np.nan)
    f.__name__ = "amihud_%d" % lookback
    return f


def dvol_trend(lookback):
    def f(panel, t):
        dvol = (panel["un"] * panel["vol"])
        recent = np.nanmean(dvol[:, t - 20:t + 1], axis=1)
        base = np.nanmean(dvol[:, t - lookback:t - 20], axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            x = np.log(recent / base)
        return np.where(np.isfinite(x), x, np.nan)
    f.__name__ = "dvol_trend_%d" % lookback
    return f


# --------------------------- the registered grid --------------------------- #
EQUITY_FAMILIES = {
    "E1_XS_MOMENTUM": {
        "cadence": 21, "horizon": 21,
        "variants": {"mom_252_21": mom(252), "mom_189_21": mom(189),
                     "mom_126_21": mom(126)},
        "neighbour_order": ["mom_126_21", "mom_189_21", "mom_252_21"],
    },
    "E2_SHORT_REVERSAL": {
        "cadence": 5, "horizon": 5,
        "variants": {"rev_5": reversal(5), "rev_10": reversal(10)},
        "neighbour_order": ["rev_5", "rev_10"],
    },
    "E3_RESIDUAL_MOMENTUM": {
        "cadence": 21, "horizon": 21,
        "variants": {"resid_252_21": residual_mom(252),
                     "resid_126_21": residual_mom(126)},
        "neighbour_order": ["resid_126_21", "resid_252_21"],
    },
    "E4_SECTOR_RELATIVE_MOMENTUM": {
        "cadence": 21, "horizon": 21,
        "variants": {"srel_252_21": sector_rel_mom(252),
                     "srel_126_21": sector_rel_mom(126)},
        "neighbour_order": ["srel_126_21", "srel_252_21"],
    },
    "E5_LOW_RISK": {
        "cadence": 21, "horizon": 21,
        "variants": {"lowvol_252": low_vol(252), "lowbeta_252": low_beta(252)},
        "neighbour_order": ["lowvol_252", "lowbeta_252"],
    },
    "E6_IDIO_VOL": {
        "cadence": 21, "horizon": 21,
        "variants": {"idiovol_63": low_idio_vol(63),
                     "idiovol_126": low_idio_vol(126)},
        "neighbour_order": ["idiovol_63", "idiovol_126"],
    },
    "E7_HIGH_PROXIMITY": {
        "cadence": 21, "horizon": 21,
        "variants": {"hi52_252": high_proximity(252),
                     "hi52_126": high_proximity(126)},
        "neighbour_order": ["hi52_126", "hi52_252"],
    },
    "E8_LIQUIDITY": {
        "cadence": 21, "horizon": 21,
        "variants": {"amihud_63": amihud(63), "dvol_trend_63": dvol_trend(63)},
        "neighbour_order": ["amihud_63", "dvol_trend_63"],
    },
}
