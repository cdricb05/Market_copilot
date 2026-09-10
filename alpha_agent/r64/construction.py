"""alpha_agent.r64.construction - the ONE bounded, realistic, risk-controlled
research book.

R63 priced information through an UNLEVERED gross-2 cross-sectional book with
inverse-volatility weights capped per name, and through a per-instrument
vol-targeted time-series book. On the cross-asset scope the arithmetic net
increment was positive while the compounded path collapsed (-100% drawdown);
on a nine-name scope the cap left the book concentrated. Those books measured
their own construction, not the information.

This module applies, to BOTH arms of a cell identically, the controls a
deployable multi-asset book would actually carry (protocol section
"risk_controlled_construction"):

    * volatility target (10% annualised) from the unlevered book's OWN trailing
      realised volatility, computed strictly from past evaluation periods;
    * a gross leverage cap;
    * an instrument contribution cap on ex-ante risk;
    * equal asset-class risk budgets (cross-asset scope);
    * a no-trade band so a held name is not churned for a small weight change;
    * per-market cost on one-way turnover.

Because both arms are built by the same code with the same parameters, the
paired increment measures the information, not the construction. Nothing
here is tuned on a result: every parameter is fixed in the protocol.
"""
from __future__ import annotations

import math

import numpy as np

from alpha_agent.r63 import sensitivity as S

from . import (DEGENERATE_DD, LEVERAGE_AT_CAP_SHARE_MAX, MAX_GROSS_LEVERAGE,
               MAX_INSTRUMENT_RISK_SHARE, MAX_NAME_WEIGHT, NO_TRADE_BAND, TARGET_VOL,
               VOL_FLOOR, VOL_LOOKBACK_PERIODS, VOL_MIN_PERIODS)

CALCULATION_OWNER = "alpha_agent.r64.construction"
PPY = 252.0
BOOK_XS = "XS_LONG_SHORT_RISK_CONTROLLED"
BOOK_TS = "TS_VOL_TARGET_RISK_CONTROLLED"
LEV_WARMUP = "WARMUP"
LEV_TARGETED = "TARGETED"
LEV_AT_CAP = "AT_CAP"


def default_policy() -> dict:
    return {
        "construction_policy_version": "r64_risk_controlled_book.v1",
        "target_vol": TARGET_VOL,
        "max_gross_leverage": MAX_GROSS_LEVERAGE,
        "max_instrument_risk_share": MAX_INSTRUMENT_RISK_SHARE,
        "max_name_weight": MAX_NAME_WEIGHT,
        "no_trade_band": NO_TRADE_BAND,
        "vol_lookback_periods": VOL_LOOKBACK_PERIODS,
        "vol_min_periods": VOL_MIN_PERIODS,
        "vol_floor": VOL_FLOOR,
        "degenerate_dd": DEGENERATE_DD,
        "leverage_at_cap_share_max": LEVERAGE_AT_CAP_SHARE_MAX,
        "equal_asset_class_risk_budget": True,
        "applied_to_both_arms": True,
    }


# --------------------------------------------------------------------------- #
# Unlevered target weights for one period
# --------------------------------------------------------------------------- #
def xs_unlevered(pp: np.ndarray, vv: np.ndarray, inst: np.ndarray, *,
                 max_name_weight: float = MAX_NAME_WEIGHT) -> dict | None:
    """R63's cross-sectional selection and per-side inverse-vol weights: long
    the top quintile (tercile below ten names), short the bottom; each side
    sums to one; per-name cap max(1/k, ``max_name_weight``)."""
    n = len(pp)
    if n < 6:
        return None
    k = max(1, int(round(n * (0.2 if n >= 10 else 1.0 / 3.0))))
    o = np.argsort(pp)
    lo, hi = o[:k], o[-k:]
    iv = 1.0 / np.where(np.isfinite(vv) & (vv > 0), vv, np.nan)
    med = np.nanmedian(iv)
    iv = np.where(np.isfinite(iv), iv, med if np.isfinite(med) else 1.0)
    cap = max(1.0 / k, max_name_weight)
    wl = S._capped(iv[hi] / iv[hi].sum(), cap)
    ws = S._capped(iv[lo] / iv[lo].sum(), cap)
    w: dict = {}
    for j, x in zip(inst[hi], wl):
        w[int(j)] = float(x)
    for j, x in zip(inst[lo], ws):
        w[int(j)] = w.get(int(j), 0.0) - float(x)
    return w


def ts_unlevered(pp: np.ndarray, vv: np.ndarray, inst: np.ndarray,
                 pred_scale: float) -> dict | None:
    """R63's time-series position: tanh(score / training std), per-instrument
    10% vol target capped at 3x, equal-weighted across the scope."""
    n = len(pp)
    if n == 0:
        return None
    s = np.tanh(pp / (pred_scale if pred_scale and pred_scale > 0 else 1.0))
    lev = 0.10 / np.where(np.isfinite(vv) & (vv > 0.02), vv, np.nan)
    lev = np.where(np.isfinite(lev), np.minimum(lev, 3.0), 0.0)
    return {int(j): float(x) for j, x in zip(inst, s * lev / n)}


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #
def _side_gross(w: dict, sign: int) -> float:
    return float(sum(abs(x) for x in w.values() if sign * x > 0))


def apply_class_risk_budget(w: dict, vol_by: dict, cls_by: dict) -> dict:
    """Equalise ex-ante risk (|w| x vol) across the asset classes present on
    each side of the book, preserving each side's gross. A side with one
    class is unchanged."""
    out = dict(w)
    for sign in (1, -1):
        names = [i for i, x in out.items() if sign * x > 0]
        classes = sorted({cls_by.get(i) for i in names})
        if len(classes) < 2:
            continue
        gross_before = sum(abs(out[i]) for i in names)
        risk_c = {c: sum(abs(out[i]) * (vol_by.get(i) or 0.0)
                         for i in names if cls_by.get(i) == c) for c in classes}
        total = sum(risk_c.values())
        if total <= 0:
            continue
        target = total / len(classes)
        for i in names:
            rc = risk_c.get(cls_by.get(i)) or 0.0
            if rc > 0:
                out[i] *= target / rc
        gross_after = sum(abs(out[i]) for i in names)
        if gross_after > 0:
            for i in names:
                out[i] *= gross_before / gross_after
    return out


def apply_instrument_cap(w: dict, vol_by: dict, cap: float, iters: int = 30) -> dict:
    """No instrument may carry more than ``cap`` of the book's ex-ante risk
    (|w_i| vol_i / sum |w_j| vol_j). Excess risk is redistributed pro rata to
    the uncapped names; signs are kept; the book's total ex-ante risk is
    preserved. The cap BINDS: no gross rescale follows it (the first
    implementation rescaled each side back to its gross afterwards, which
    could re-breach the cap; repaired before the campaign and disclosed in the
    protocol). Scale is the volatility target's job, applied afterwards."""
    names = list(w)
    if len(names) < 2:
        return dict(w)
    vol = {i: (vol_by.get(i) or 0.0) for i in names}
    risk = {i: abs(w[i]) * vol[i] for i in names}
    total = sum(risk.values())
    if total <= 0 or cap >= 1.0:
        return dict(w)
    limit = cap * total
    over = [i for i in names if risk[i] > limit + 1e-15]
    excess = sum(risk[i] - limit for i in over)
    for i in over:
        risk[i] = limit
    # water-fill the released risk into the names with headroom, pro rata to
    # their risk, until it is fully placed (or no headroom remains: then the
    # cap is infeasible for this cross-section and the book simply holds less
    # ex-ante risk, which the volatility target restores)
    for _ in range(iters):
        if excess <= 1e-15:
            break
        under = [i for i in names if risk[i] < limit - 1e-15]
        su = sum(risk[i] for i in under)
        if not under or su <= 0:
            break
        given = 0.0
        for i in under:
            g = min(limit - risk[i], excess * risk[i] / su)
            risk[i] += g
            given += g
        excess -= given
        if given <= 1e-15:
            break
    out = {}
    for i in names:
        out[i] = math.copysign(risk[i] / vol[i], w[i]) if vol[i] > 0 else w[i]
    return out


def leverage_from_history(history: list, gross_unlevered: float, horizon: int,
                          policy: dict) -> tuple:
    """(leverage, state). The vol estimate uses ONLY past unlevered book
    returns (strictly before the decision). Warm-up leverage is 1.0."""
    if len(history) < int(policy["vol_min_periods"]):
        return 1.0, LEV_WARMUP
    r = np.asarray(history[-int(policy["vol_lookback_periods"]):], dtype=float)
    ppy = PPY / float(horizon)
    sd = float(np.std(r, ddof=1)) * math.sqrt(ppy) if len(r) > 1 else float("nan")
    if not np.isfinite(sd):
        return 1.0, LEV_WARMUP
    sd = max(sd, float(policy["vol_floor"]))
    lev = float(policy["target_vol"]) / sd
    cap = float(policy["max_gross_leverage"]) / max(gross_unlevered, 1e-12)
    if lev > cap:
        return cap, LEV_AT_CAP
    return lev, LEV_TARGETED


def apply_no_trade_band(w_new: dict, w_prev: dict, band: float) -> dict:
    """A name held before AND after stays at its previous weight when the
    change is smaller than ``band`` of the larger weight; entries and exits
    always trade."""
    out = dict(w_new)
    for i, x in w_new.items():
        p = w_prev.get(i)
        if p is None or p == 0.0 or x == 0.0:
            continue
        if abs(x - p) < band * max(abs(x), abs(p)):
            out[i] = p
    return out


# --------------------------------------------------------------------------- #
# The book
# --------------------------------------------------------------------------- #
def build_book(pred: np.ndarray, y: np.ndarray, gid: np.ndarray, iid: np.ndarray,
               vol: np.ndarray, cost: np.ndarray, *, mode: str, horizon: int,
               pred_scale: float = 1.0, class_of: np.ndarray | None = None,
               policy: dict | None = None) -> dict:
    """Per-period gross / net return, one-way turnover, cost, gross leverage,
    max |weight|, max class share and the leverage state of the risk-controlled
    book built from ``pred`` on OOS rows.

    Rows are (instrument ``iid``, evaluation period ``gid``) with the forward
    return ``y`` realised over the horizon, the trailing 63-session volatility
    ``vol`` known at the decision and the instrument cost per side ``cost``.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    order = np.lexsort((iid, gid))
    p, yy, g, ii, v, c = (pred[order], y[order], gid[order], iid[order], vol[order],
                          cost[order])
    cls_all = (np.asarray(class_of)[order] if class_of is not None else None)
    periods = np.unique(g)
    starts = np.searchsorted(g, periods, side="left")
    ends = np.searchsorted(g, periods, side="right")
    gross, net, turn, costs, maxw, glev, maxcls, lev_state = [], [], [], [], [], [], [], []
    unlev_hist: list = []
    prev: dict = {}
    for a, b in zip(starts, ends):
        pp, yv, inst, vv, cc = p[a:b], yy[a:b], ii[a:b], v[a:b], c[a:b]
        cl = cls_all[a:b] if cls_all is not None else None
        ok = np.isfinite(pp) & np.isfinite(yv)
        pp, yv, inst, vv, cc = pp[ok], yv[ok], inst[ok], vv[ok], cc[ok]
        cl = cl[ok] if cl is not None else None
        w_u = (xs_unlevered(pp, vv, inst, max_name_weight=pol["max_name_weight"])
               if mode == "XS" else ts_unlevered(pp, vv, inst, pred_scale))
        if not w_u:
            continue
        vol_by = {int(j): float(x) for j, x in zip(inst, vv)}
        cost_by = {int(j): float(x) for j, x in zip(inst, cc)}
        ret_by = {int(j): float(x) for j, x in zip(inst, yv)}
        if cl is not None and pol.get("equal_asset_class_risk_budget", True):
            cls_by = {int(j): str(x) for j, x in zip(inst, cl)}
            w_u = apply_class_risk_budget(w_u, vol_by, cls_by)
        else:
            cls_by = None
        w_u = apply_instrument_cap(w_u, vol_by, float(pol["max_instrument_risk_share"]))
        gross_u = sum(abs(x) for x in w_u.values())
        # the unlevered book's own return this period (for the NEXT decision's
        # volatility estimate; never for this one)
        r_u = sum(w_u[x] * ret_by.get(x, 0.0) for x in w_u)
        lev, state = leverage_from_history(unlev_hist, gross_u, horizon, pol)
        unlev_hist.append(r_u)
        w_t = {x: lev * val for x, val in w_u.items()}
        w = apply_no_trade_band(w_t, prev, float(pol["no_trade_band"]))
        med_cost = float(np.median(cc)) if len(cc) else 0.0
        traded = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) for x in set(w) | set(prev))
        cost_t = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) * cost_by.get(x, med_cost)
                     for x in set(w) | set(prev))
        gr = sum(w[x] * ret_by.get(x, 0.0) for x in w)
        g_all = sum(abs(x) for x in w.values())
        gross.append(gr)
        net.append(gr - cost_t)
        turn.append(traded / 2.0)
        costs.append(cost_t)
        maxw.append(max(abs(x) for x in w.values()) if w else 0.0)
        glev.append(g_all)
        if cls_by:
            by_c: dict = {}
            for x, val in w.items():
                by_c[cls_by.get(x)] = by_c.get(cls_by.get(x), 0.0) + abs(val)
            maxcls.append(max(by_c.values()) / g_all if g_all > 0 else 0.0)
        else:
            maxcls.append(1.0)
        lev_state.append(state)
        prev = w
    return {"gross": np.array(gross), "net": np.array(net), "turnover": np.array(turn),
            "cost": np.array(costs), "max_weight": np.array(maxw),
            "gross_leverage": np.array(glev), "max_class_share": np.array(maxcls),
            "leverage_state": np.array(lev_state, dtype=object),
            "unlevered_gross": np.array(unlev_hist),
            "book": BOOK_XS if mode == "XS" else BOOK_TS, "policy": pol}


def summarise(book: dict, horizon: int, lag: int) -> dict:
    """The R63 economic summary of a book, plus the R64 control statistics."""
    base = S._econ_summary(book, horizon, lag)
    n = len(book["net"])
    if n == 0:
        return base
    ppy = PPY / float(horizon)
    at_cap = float(np.mean(book["leverage_state"] == LEV_AT_CAP))
    warm = float(np.mean(book["leverage_state"] == LEV_WARMUP))
    sd = float(np.std(book["net"], ddof=1)) * math.sqrt(ppy) if n > 2 else None
    base.update({
        "ann_vol": sd,
        "mean_gross_leverage": float(book["gross_leverage"].mean()),
        "max_gross_leverage": float(book["gross_leverage"].max()),
        "share_periods_leverage_at_cap": at_cap,
        "share_periods_warmup": warm,
        "max_class_share": float(book["max_class_share"].max()),
        "book": book["book"],
    })
    base["degenerate_under_controls"] = degenerate(base, book["policy"])
    return base


def degenerate(summary: dict, policy: dict | None = None) -> bool:
    pol = policy or default_policy()
    dd = summary.get("max_dd")
    at_cap = summary.get("share_periods_leverage_at_cap") or 0.0
    return bool((dd is not None and dd < float(pol["degenerate_dd"]))
                or at_cap > float(pol["leverage_at_cap_share_max"]))


def paired_increment(book_b: dict, book_bd: dict, horizon: int, lag: int) -> dict:
    """Augmented minus baseline per-period NET return under the identical
    construction: Newey-West t, annualised increment, Sharpe increment and the
    increment when the incremental trading is charged twice."""
    n = min(len(book_b["net"]), len(book_bd["net"]))
    if n == 0:
        return {"periods": 0}
    ppy = PPY / float(horizon)
    diff = book_bd["net"][:n] - book_b["net"][:n]
    st = S.nw_tstat(diff, lag)
    extra_cost = book_bd["cost"][:n] - book_b["cost"][:n]
    sb, sbd = summarise(book_b, horizon, lag), summarise(book_bd, horizon, lag)
    sh_inc = ((sbd.get("sharpe") or 0.0) - (sb.get("sharpe") or 0.0)
              if sb.get("sharpe") is not None and sbd.get("sharpe") is not None else None)
    return {"periods": int(n), "ann_net_increment": float(diff.mean() * ppy),
            "ann_net_increment_at_2x_cost": float((diff - extra_cost).mean() * ppy),
            "sharpe_increment": sh_inc, "t_increment": st["t"],
            "p_increment_one_sided": st["p_one_sided"],
            "baseline": sb, "augmented": sbd,
            "book": book_bd["book"], "owner": CALCULATION_OWNER,
            "construction_policy_version": book_bd["policy"]["construction_policy_version"]}
