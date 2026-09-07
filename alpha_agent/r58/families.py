"""alpha_agent.r58.families - the 13 pre-registered families plus one diagnostic.

Three groups, all defined in research/r58/R58_RESEARCH_PROTOCOL.json before any
experiment ran:

  A  FUNDAMENTAL RESCUE      is the champion's fundamental leg real on honest
                             point-in-time, survivorship-safe data?
  B  BLEND AND GATING        can momentum be used conditionally - as a veto or a
                             regime-switched weight - instead of a constant 50%
                             vote that R57 measured as inverted on live data?
  C  INFORMATION CHANGE      the class R57 never tested: the DIRECTION in which
                             a company's reported information is moving, and the
                             market's reaction to the filing itself.

Every score function has the same signature ``(pf, j, elig) -> np.ndarray`` and
returns NaN for a name it cannot score. NaN means "not in my universe"; it never
means zero.
"""
from __future__ import annotations

import numpy as np

from . import HORIZON
from .engine import xs_rank01, xs_z

AVG_WINDOW = 12          # decision slots (~1 year at 21-session cadence)
AVG_MIN_OBS = 8
STAB_EPS = 1e-6
FILING_MAX_AGE = 90      # sessions
TRANSFORM_GRID = ("level", "avg3", "stab")


# --------------------------------------------------------------------------- #
# Feature access and the uniform transform grid
# --------------------------------------------------------------------------- #
def feat(pf, name, j):
    return pf["cube"][:, j, pf["f_ix"][name]]


def _history(pf, name, j, window=AVG_WINDOW):
    lo = max(0, j - window + 1)
    return pf["cube"][:, lo:j + 1, pf["f_ix"][name]]


def transform(pf, name, j, elig, kind="level"):
    """level / avg3 / stab, applied to one PANEL-F feature. Pre-registered."""
    if kind == "level":
        return xs_z(feat(pf, name, j), elig)
    h = _history(pf, name, j)
    cnt = np.isfinite(h).sum(axis=1)
    if kind == "avg3":
        with np.errstate(invalid="ignore"):
            m = np.nanmean(np.where(np.isfinite(h), h, np.nan), axis=1)
        m = np.where(cnt >= AVG_MIN_OBS, m, np.nan)
        return xs_z(m, elig)
    if kind == "stab":
        with np.errstate(invalid="ignore"):
            sd = np.nanstd(np.where(np.isfinite(h), h, np.nan), axis=1)
        sd = np.where(cnt >= AVG_MIN_OBS, sd, np.nan)
        x = feat(pf, name, j)
        with np.errstate(invalid="ignore", divide="ignore"):
            v = x / (np.abs(sd) + STAB_EPS)
        return xs_z(np.where(np.isfinite(v), v, np.nan), elig)
    raise ValueError("unknown transform %r" % kind)


# --------------------------------------------------------------------------- #
# Price-side helper (the momentum leg's shape, for the blend families only)
# --------------------------------------------------------------------------- #
def momentum(pf, j, lookback=126, skip=21):
    """126-session total return skipping the last 21 - the champion leg's shape."""
    tr = pf["price"]["tr"]
    t = int(pf["dec"][j])
    a, b = t - skip, t - skip - lookback
    if b < 0:
        return np.full(tr.shape[0], np.nan)
    with np.errstate(invalid="ignore", divide="ignore"):
        r = tr[:, a] / tr[:, b] - 1.0
    return np.where(np.isfinite(r), r, np.nan)


# --------------------------------------------------------------------------- #
# GROUP A - fundamental rescue
# --------------------------------------------------------------------------- #
def a1_composite(w1=1.0, w2=1.0):
    """Cash generation is good; earnings not backed by cash are bad (Sloan)."""
    def fn(pf, j, elig):
        zf = xs_z(feat(pf, "fcf_to_assets", j), elig)
        za = xs_z(-feat(pf, "accruals_to_assets", j), elig)
        ok = np.isfinite(zf) & np.isfinite(za)
        out = np.full(zf.shape, np.nan)
        out[ok] = (w1 * zf[ok] + w2 * za[ok]) / (w1 + w2)
        return out
    return fn


def a2_fcf(kind="level"):
    def fn(pf, j, elig):
        return transform(pf, "fcf_to_assets", j, elig, kind)
    return fn


def a3_accruals(kind="level"):
    def fn(pf, j, elig):
        return -transform(pf, "accruals_to_assets", j, elig, kind)
    return fn


def a4_fresh(max_age=400):
    """A1 restricted to names whose fundamental observation is recent.

    Tests whether the live leg's STALENESS rather than its content is the
    problem: the operational panel's own scores stop at 2026-05-22.
    """
    base = a1_composite()
    def fn(pf, j, elig):
        s = base(pf, j, elig)
        age = feat(pf, "obs_age_days", j)
        return np.where(np.isfinite(age) & (age <= max_age), s, np.nan)
    return fn


# --------------------------------------------------------------------------- #
# GROUP B - blend and momentum gating
# --------------------------------------------------------------------------- #
def b0_blend_diagnostic():
    """The incumbent champion's shape. DIAGNOSTIC ONLY - never an alpha claim."""
    return b2_blend(0.5)


def b2_blend(w_fund=0.75):
    base = a1_composite()
    def fn(pf, j, elig):
        zf = xs_z(base(pf, j, elig), elig)
        zm = xs_z(momentum(pf, j), elig)
        ok = np.isfinite(zf) & np.isfinite(zm)
        out = np.full(zf.shape, np.nan)
        out[ok] = w_fund * zf[ok] + (1.0 - w_fund) * zm[ok]
        return out
    return fn


def b3_mom_gated(veto_q=0.20):
    """Momentum as a VETO, not a vote: fundamentals choose, price distress removes."""
    base = a1_composite()
    def fn(pf, j, elig):
        s = base(pf, j, elig)
        mr = xs_rank01(momentum(pf, j), elig)
        vetoed = np.isfinite(mr) & (mr < veto_q)
        return np.where(vetoed, np.nan, s)
    return fn


def b4_regime(mode="futures_riskon", regime=None):
    """Momentum earns its 50% only in the pre-registered RISK-ON regime."""
    base = a1_composite()
    def fn(pf, j, elig):
        on = bool(regime[mode][j]) if regime is not None else False
        zf = xs_z(base(pf, j, elig), elig)
        if not on:
            return zf
        zm = xs_z(momentum(pf, j), elig)
        ok = np.isfinite(zf) & np.isfinite(zm)
        out = np.full(zf.shape, np.nan)
        out[ok] = 0.5 * zf[ok] + 0.5 * zm[ok]
        return out
    return fn


def b5_hold_band():
    """A1's score; the hold band lives in the simulator, not in the score."""
    return a1_composite()


# --------------------------------------------------------------------------- #
# GROUP C - information change
# --------------------------------------------------------------------------- #
def _delta(pf, j, now, prior):
    a, b = feat(pf, now, j), feat(pf, prior, j)
    ok = np.isfinite(a) & np.isfinite(b)
    return np.where(ok, a - b, np.nan)


def _delta_transform(pf, j, elig, now, prior, kind, sign=1.0):
    if kind == "level":
        return xs_z(sign * _delta(pf, j, now, prior), elig)
    lo = max(0, j - AVG_WINDOW + 1)
    hist = np.stack([_delta(pf, k, now, prior) for k in range(lo, j + 1)], axis=1)
    cnt = np.isfinite(hist).sum(axis=1)
    with np.errstate(invalid="ignore"):
        if kind == "avg3":
            m = np.nanmean(np.where(np.isfinite(hist), hist, np.nan), axis=1)
            return xs_z(sign * np.where(cnt >= AVG_MIN_OBS, m, np.nan), elig)
        if kind == "stab":
            sd = np.nanstd(np.where(np.isfinite(hist), hist, np.nan), axis=1)
            sd = np.where(cnt >= AVG_MIN_OBS, sd, np.nan)
            v = _delta(pf, j, now, prior) / (np.abs(sd) + STAB_EPS)
            return xs_z(sign * np.where(np.isfinite(v), v, np.nan), elig)
    raise ValueError("unknown transform %r" % kind)


def c1_profit_accel(kind="level"):
    """The market underreacts to the DIRECTION of profitability, not its level."""
    def fn(pf, j, elig):
        return _delta_transform(pf, j, elig, "opinc_to_assets",
                                "opinc_to_assets_prior", kind, sign=+1.0)
    return fn


def c2_accrual_change(kind="level"):
    """Worsening accruals are deterioration even when the level looks ordinary."""
    def fn(pf, j, elig):
        return _delta_transform(pf, j, elig, "accruals_to_assets",
                                "accruals_to_assets_prior", kind, sign=-1.0)
    return fn


def c3_wc_build(kind="level"):
    """Inventory and receivables outgrowing sales: unsold product, pulled revenue."""
    def fn(pf, j, elig):
        return _delta_transform(pf, j, elig, "wc_to_revenue",
                                "wc_to_revenue_prior", kind, sign=-1.0)
    return fn


def c4_rnd(kind="level"):
    """R&D is expensed but is intangible investment. Stage 24's only FDR survivor."""
    def fn(pf, j, elig):
        return transform(pf, "rnd_to_assets", j, elig, kind)
    return fn


def c5_filing_drift(react=5, decay=21):
    """Post-filing drift - a pure information-TIMESTAMP signal.

    It uses no accounting number at all: only the SEC filed date and the price
    reaction to it. The reaction window must be COMPLETE at the decision date,
    so a filing younger than ``react`` sessions leaves the name unscored rather
    than borrowing a return from the future.
    """
    def fn(pf, j, elig):
        tr = pf["price"]["tr"]
        t = int(pf["dec"][j])
        fix = feat(pf, "filed_ix", j)
        out = np.full(tr.shape[0], np.nan)
        ok = elig & np.isfinite(fix) & (fix >= 0)
        if not ok.any():
            return out
        age = t - fix
        usable = ok & (age >= react) & (age <= FILING_MAX_AGE)
        if not usable.any():
            return out
        idx = np.where(usable)[0]
        f0 = fix[idx].astype(int)
        f1 = np.minimum(f0 + react, t)
        p0 = tr[idx, f0]
        p1 = tr[idx, f1]
        with np.errstate(invalid="ignore", divide="ignore"):
            r = p1 / p0 - 1.0
        # reference: EW mean of the eligible universe over the SAME window
        elig_ix = np.where(elig)[0]
        ref = {}
        for w0 in np.unique(f0):
            w1 = min(int(w0) + react, t)
            with np.errstate(invalid="ignore", divide="ignore"):
                rr = tr[elig_ix, w1] / tr[elig_ix, int(w0)] - 1.0
            rr = rr[np.isfinite(rr)]
            ref[int(w0)] = float(rr.mean()) if len(rr) else np.nan
        base = np.array([ref[int(x)] for x in f0])
        ab = r - base
        w = np.exp(-(age[idx]) / float(decay))
        v = np.where(np.isfinite(ab), ab * w, np.nan)
        out[idx] = v
        return xs_z(out, elig)
    return fn


# --------------------------------------------------------------------------- #
# Registry: family -> (grid, builder). Frozen by the protocol.
# --------------------------------------------------------------------------- #
def registry():
    return {
        "A1": {"group": "fundamental_rescue", "primary": "1.0:1.0",
               "grid": {"1.0:1.0": a1_composite(1.0, 1.0),
                        "2.0:1.0": a1_composite(2.0, 1.0),
                        "1.0:2.0": a1_composite(1.0, 2.0)},
               "label": "fundamental composite (FCF/assets + reversed accruals)"},
        "A2": {"group": "fundamental_rescue", "primary": "level",
               "grid": {k: a2_fcf(k) for k in TRANSFORM_GRID},
               "label": "free cash flow to assets alone"},
        "A3": {"group": "fundamental_rescue", "primary": "level",
               "grid": {k: a3_accruals(k) for k in TRANSFORM_GRID},
               "label": "reversed operating accruals alone"},
        "A4": {"group": "fundamental_rescue", "primary": "400",
               "grid": {"400": a4_fresh(400), "270": a4_fresh(270),
                        "550": a4_fresh(550)},
               "label": "fundamental composite, freshness-restricted"},
        "B0": {"group": "diagnostic_reference", "primary": "0.50",
               "grid": {"0.50": b0_blend_diagnostic()},
               "label": "INCUMBENT CHAMPION SHAPE (50/50 fundamental+momentum) "
                        "- diagnostic reference, never an alpha claim"},
        "B2": {"group": "blend_and_gating", "primary": "0.75",
               "grid": {"0.75": b2_blend(0.75), "0.85": b2_blend(0.85),
                        "0.65": b2_blend(0.65)},
               "label": "fundamental-heavy blend with momentum"},
        "B3": {"group": "blend_and_gating", "primary": "quintile",
               "grid": {"quintile": b3_mom_gated(0.20),
                        "decile": b3_mom_gated(0.10),
                        "tercile": b3_mom_gated(0.3333)},
               "label": "momentum as a veto on the fundamental score"},
        "B4": {"group": "blend_and_gating", "primary": "futures_riskon",
               "grid": {"futures_riskon": None, "vol_only": None,
                        "trend_only": None},
               "label": "regime-conditional momentum weight",
               "needs_regime": True},
        "B5": {"group": "blend_and_gating", "primary": "100",
               "grid": {"100": b5_hold_band(), "75": b5_hold_band(),
                        "150": b5_hold_band()},
               "hold_band": {"100": 100, "75": 75, "150": 150},
               "label": "fundamental composite with a turnover hold band"},
        "C1": {"group": "information_change", "primary": "level",
               "grid": {k: c1_profit_accel(k) for k in TRANSFORM_GRID},
               "label": "profitability acceleration (year on year)"},
        "C2": {"group": "information_change", "primary": "level",
               "grid": {k: c2_accrual_change(k) for k in TRANSFORM_GRID},
               "label": "accrual deterioration change"},
        "C3": {"group": "information_change", "primary": "level",
               "grid": {k: c3_wc_build(k) for k in TRANSFORM_GRID},
               "label": "working-capital build versus sales"},
        "C4": {"group": "information_change", "primary": "level",
               "grid": {k: c4_rnd(k) for k in TRANSFORM_GRID},
               "label": "R&D intensity"},
        "C5": {"group": "information_change", "primary": "react5_decay21",
               "grid": {"react5_decay21": c5_filing_drift(5, 21),
                        "react3_decay21": c5_filing_drift(3, 21),
                        "react5_decay42": c5_filing_drift(5, 42)},
               "label": "post-filing drift (information timestamp only)"},
    }
