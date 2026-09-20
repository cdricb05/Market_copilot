r"""campaign_r57_wave2.executors - the TEN Wave-2 hypotheses, as code.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION.

One function per pre-registered cell, each returning a PLAN the canonical
runner executes:

    {"book", "panel", "make_fn" | "score_fn", "signal_sign", "book_kwargs"}

``make_fn`` (not ``score_fn``) is used wherever the construction carries state
between decisions - the three partial-rebalance cells - so that the validation
run never starts from the discovery run's book and a placebo never starts from
the candidate's.

WHAT THE SECOND CALLABLE MEANS depends on the book, and the difference is
deliberate:

    FUTURES_DATED_CONTRACT   weights_fn(t, live) -> float[n_markets]
                             a weights rule, because not every construction
                             here is a rank book (W2-08 is a leg split)
    EQUITY_TOPN              score_fn(panel, t) -> float[n_names], NaN = unscorable

TIMING. Every window below is closed at t-1 for price and slope and at t-2 for
volume and open interest, per ``panels`` - and the book enters at the
settlement of t, so nothing scored here is known after the decision instant.

The nine construction choices that were NOT forced by the mechanism (level vs
own-history z, 5d vs 10d, 21d vs 63d, commodity vs all-68, rank vs risk-parity
legs, and the three rebalance speeds) were settled on FEATURE-SIDE turnover
alone, with no return scored, before pre-registration. They are charged
through ``within_family_tests = 2`` on every cell rather than hidden.
"""
from __future__ import annotations

import numpy as np

from alpha_agent import r59
from alpha_agent.agents_v2 import books as B
from alpha_agent.r59 import native

from . import panels as PN

MIN_MARKETS = 12
EPS = 1e-12


# --------------------------------------------------------------------------- #
# Shared construction
# --------------------------------------------------------------------------- #
def _demean_by(feature: np.ndarray, groups, scored: np.ndarray,
               min_members: int = 2) -> np.ndarray:
    """Subtract each group's own mean. A group with too few scored members is
    DROPPED, not compared against itself."""
    out = np.full(feature.shape, np.nan)
    g = np.asarray(groups)
    for key in set(g[scored].tolist()):
        sel = scored & (g == key)
        if int(sel.sum()) >= int(min_members):
            out[sel] = feature[sel] - float(np.mean(feature[sel]))
    return out


def _rank_target(score: np.ndarray, scored: np.ndarray) -> np.ndarray:
    """``native._xs_weights`` - rank-centred, dollar-neutral, unit gross."""
    return B.rank_weights(score, scored, min_markets=MIN_MARKETS)


def _partial_rebalance(lam: float):
    """A frozen, stateful partial rebalance toward the target.

    ``W_t = W_{t-1} + lam * (target - W_{t-1})``, masked to live markets and
    renormalised to UNIT GROSS - no leverage, no gross shrinkage. It exists
    because every construction in this campaign whose natural turnover exceeds
    the frozen 0.40 one-way ceiling would otherwise be unregistrable, and
    ``lam`` was set from FEATURE-SIDE turnover alone to sit near 60% of that
    ceiling, before any return was scored.

    The masking mirrors what the book does to a weights vector after it is
    returned, so this function's memory of the book and the book's own memory
    can never diverge.
    """
    state = {"w": None}

    def apply(target: np.ndarray, live: np.ndarray) -> np.ndarray:
        target = np.where(np.isfinite(target) & live, target, 0.0)
        prev = state["w"]
        if prev is None:
            w = target
        else:
            w = prev + float(lam) * (target - prev)
        w = np.where(np.isfinite(w) & live, w, 0.0)
        g = float(np.abs(w).sum())
        if g > EPS:
            w = w / g
        else:
            w = np.zeros_like(w)
        state["w"] = w
        return w

    return apply


def _mean_window(arr: np.ndarray, lo: int, hi: int, min_finite: int
                 ) -> np.ndarray:
    """Mean of ``arr`` over grid slots [lo, hi), NaN below ``min_finite``."""
    lo, hi = max(0, int(lo)), max(0, int(hi))
    if hi <= lo:
        return np.full(arr.shape[0], np.nan)
    w = arr[:, lo:hi]
    fin = np.isfinite(w)
    n = fin.sum(axis=1)
    s = np.where(fin, w, 0.0).sum(axis=1)
    return np.where(n >= int(min_finite), s / np.maximum(n, 1), np.nan)


def _std_window(arr: np.ndarray, lo: int, hi: int, min_finite: int
                ) -> np.ndarray:
    lo, hi = max(0, int(lo)), max(0, int(hi))
    if hi <= lo:
        return np.full(arr.shape[0], np.nan)
    w = arr[:, lo:hi]
    out = np.full(arr.shape[0], np.nan)
    for i in range(arr.shape[0]):
        x = w[i][np.isfinite(w[i])]
        if len(x) >= int(min_finite):
            out[i] = float(np.std(x, ddof=1)) if len(x) > 1 else np.nan
    return out


def _log_sum_return(ret: np.ndarray, lo: int, hi: int,
                    min_finite: int) -> np.ndarray:
    """Compounded log return over [lo, hi). A missing session earns zero, the
    owner's ``_fwd`` convention; too few sessions is NaN, not zero."""
    lo, hi = max(0, int(lo)), max(0, int(hi))
    if hi <= lo:
        return np.full(ret.shape[0], np.nan)
    w = ret[:, lo:hi]
    fin = np.isfinite(w)
    n = fin.sum(axis=1)
    s = np.log1p(np.where(fin, w, 0.0)).sum(axis=1)
    return np.where(n >= int(min_finite), s, np.nan)


def _futures_plan(make_fn, *, horizon=21, cadence=21, label="",
                  closed_rule="DROP"):
    return {"book": B.BOOK_FUTURES, "panel": PN.futures_layer(),
            "make_fn": make_fn, "signal_sign": 1,
            "book_kwargs": {"horizon": horizon, "cadence": cadence,
                            "label": label, "charge_rolls": True,
                            "closed_rule": closed_rule}}


# --------------------------------------------------------------------------- #
# W2-01  FUT_CURVE_HEDGING_CONCENTRATION_COMMODITY   COMMODITY / POSITIONING
# --------------------------------------------------------------------------- #
def w2_01(row=None):
    """Where on the curve the open interest sits, against its own 12-month
    norm. Hedging pressure concentrates in the nearby contract; a market whose
    interest has drifted FORWARD has thin nearby hedging demand to absorb."""
    layer = PN.futures_layer()
    oi, agg = layer["open_interest"], layer["oi_aggregate"]
    smooth = _partial_rebalance(1.0 / 3.0)

    def weights(t, live):
        scored = PN.universe_commodity_oi(layer, t, live)
        with np.errstate(divide="ignore", invalid="ignore"):
            fs = np.where(np.isfinite(agg) & (agg > 0), oi / agg, np.nan)
        x = _mean_window(fs, t - 22, t - PN.OI_LAG + 1, 10)
        mu = _mean_window(fs, t - 253, t - PN.OI_LAG + 1, 200)
        sd = _std_window(fs, t - 253, t - PN.OI_LAG + 1, 200)
        z = np.where(np.isfinite(sd) & (sd > 0), (x - mu) / sd, np.nan)
        scored = scored & np.isfinite(z)
        f = _demean_by(z, layer["economic_group"], scored)
        scored = scored & np.isfinite(f)
        return smooth(_rank_target(np.where(scored, +1.0 * f, np.nan),
                                   scored), live)

    return _futures_plan(lambda: weights,
                         label="W2-01_CURVE_HEDGING_CONCENTRATION")


# --------------------------------------------------------------------------- #
# W2-02  FUT_RISK_TRANSFER_CHURN_COMMODITY           COMMODITY / LIQUIDITY
# --------------------------------------------------------------------------- #
def _churn_weights(layer, universe_fn, denominator, group_key, sign):
    """Volume per unit of open interest: how fast the risk being held turns
    over. Slow turnover is risk TRANSFERRED and paid for; fast turnover is
    speculation among participants who transfer none."""
    vol = layer["volume"]

    def weights(t, live):
        scored = universe_fn(layer, t, live)
        with np.errstate(divide="ignore", invalid="ignore"):
            churn = np.where(np.isfinite(denominator) & (denominator > 0),
                             vol / denominator, np.nan)
        c = _mean_window(churn, t - 22, t - PN.OI_LAG + 1, 15)
        f = np.where(np.isfinite(c) & (c > 0), np.log(np.maximum(c, EPS)),
                     np.nan)
        scored = scored & np.isfinite(f)
        f = _demean_by(f, layer[group_key], scored)
        scored = scored & np.isfinite(f)
        w = _rank_target(np.where(scored, sign * f, np.nan), scored)
        return np.where(np.isfinite(w) & live, w, 0.0)

    return weights


def w2_02(row=None):
    layer = PN.futures_layer()
    return _futures_plan(
        lambda: _churn_weights(layer, PN.universe_commodity_oi,
                               layer["oi_aggregate"], "economic_group", -1.0),
        label="W2-02_RISK_TRANSFER_CHURN_COMMODITY")


# --------------------------------------------------------------------------- #
# W2-03  FUT_BASIS_UNCERTAINTY_PREMIUM_COMMODITY     COMMODITY / VOLATILITY
# --------------------------------------------------------------------------- #
def w2_03(row=None):
    """The SECOND moment of the curve. An unstable convenience yield means
    inventory is scarce and the nearby holder bears real stock-out risk."""
    layer = PN.futures_layer()
    slope = layer["slope"]

    def weights(t, live):
        scored = PN.universe_commodity_slope(layer, t, live)
        ds = np.full(slope.shape, np.nan)
        ds[:, 1:] = slope[:, 1:] - slope[:, :-1]
        u = _std_window(ds, t - 65, t - PN.PRICE_LAG, 40)
        f = np.where(np.isfinite(u) & (u > 0), np.log(np.maximum(u, EPS)),
                     np.nan)
        scored = scored & np.isfinite(f)
        f = _demean_by(f, layer["economic_group"], scored)
        scored = scored & np.isfinite(f)
        w = _rank_target(np.where(scored, +1.0 * f, np.nan), scored)
        return np.where(np.isfinite(w) & live, w, 0.0)

    return _futures_plan(lambda: weights,
                         label="W2-03_BASIS_UNCERTAINTY_PREMIUM")


# --------------------------------------------------------------------------- #
# W2-04  XA_RISK_TRANSFER_CHURN_ALL_MARKETS          CROSS_ASSET / LIQUIDITY
# --------------------------------------------------------------------------- #
def w2_04(row=None):
    """W2-02's GENERALITY TEST, declared as one. Whole-curve open interest
    does not exist outside commodities (measured: 0% coverage), so this cell
    uses the FRONT contract's own open interest and demeans by r59 asset class
    rather than economic group, which keeps the single-market groups in."""
    layer = PN.futures_layer()
    return _futures_plan(
        lambda: _churn_weights(layer, PN.universe_all_traded,
                               layer["open_interest"], "asset_class", -1.0),
        label="W2-04_RISK_TRANSFER_CHURN_ALL_MARKETS")


# --------------------------------------------------------------------------- #
# W2-05  XA_FLOW_DRIVEN_WEEKLY_REVERSAL              CROSS_ASSET / H5
# --------------------------------------------------------------------------- #
def w2_05(row=None):
    """A move made on abnormally HIGH volume is a liquidity provider being
    paid to absorb an imbalance, and it reverts; a move on LOW volume is
    information, and it does not. One signed interaction states both halves."""
    layer = PN.futures_layer()
    ret, vol = layer["ret"], layer["volume"]
    smooth = _partial_rebalance(0.35)

    def weights(t, live):
        scored = PN.universe_all_traded(layer, t, live)
        r5 = _log_sum_return(ret, t - 6, t - PN.PRICE_LAG, 3)
        sigma = _std_window(ret, t - 27, t - PN.PRICE_LAG, 15)
        short = _mean_window(vol, t - 6, t - PN.OI_LAG + 1, 3)
        long = _mean_window(vol, t - 66, t - PN.OI_LAG + 1, 30)
        with np.errstate(divide="ignore", invalid="ignore"):
            v = np.where(np.isfinite(short) & np.isfinite(long) & (long > 0)
                         & (short > 0), np.log(short / long), np.nan)
        v = np.clip(v, -1.0, 1.0)
        f = np.where(np.isfinite(sigma) & (sigma > 0), r5 / sigma, np.nan) * v
        scored = scored & np.isfinite(f)
        return smooth(_rank_target(np.where(scored, -1.0 * f, np.nan),
                                   scored), live)

    return _futures_plan(lambda: weights, horizon=5, cadence=5,
                         label="W2-05_FLOW_DRIVEN_WEEKLY_REVERSAL")


# --------------------------------------------------------------------------- #
# W2-06  XA_DOLLAR_FUNDING_SLOW_DIFFUSION            CROSS_ASSET / LEAD_LAG
# --------------------------------------------------------------------------- #
def w2_06(row=None):
    """A stronger dollar tightens global funding; the real-economy response
    takes weeks, so the cross-section of dollar exposures keeps pricing a
    shock that has already happened. The 8 FX markets DEFINE the factor and
    are never scored, so no market can predict itself."""
    layer = PN.futures_layer()
    ret = layer["ret"]
    fx = PN.fx_factor_rows(layer)
    smooth = _partial_rebalance(0.35)
    # CME FX futures are quoted USD per foreign unit, so a long-dollar basket
    # is the NEGATED cross-market mean.
    with np.errstate(invalid="ignore"):
        dollar = -np.nanmean(np.where(np.isfinite(ret[fx]), ret[fx], np.nan),
                             axis=0)

    def weights(t, live):
        scored = PN.universe_nonfx(layer, t, live)
        lo, hi = max(0, t - 253), max(0, t - PN.PRICE_LAG + 1)
        D = dollar[lo:hi]
        okD = np.isfinite(D)
        beta = np.full(ret.shape[0], np.nan)
        if int(okD.sum()) >= 150:
            for i in np.where(scored)[0]:
                y = ret[i, lo:hi]
                ok = okD & np.isfinite(y)
                if int(ok.sum()) < 150:
                    continue
                x, yy = D[ok], y[ok]
                vx = float(np.var(x))
                if vx > EPS:
                    beta[i] = float(np.cov(x, yy, ddof=1)[0, 1] / vx)
        u_lo, u_hi = max(0, t - 22), max(0, t - PN.PRICE_LAG + 1)
        seg = dollar[u_lo:u_hi]
        u = float(np.log1p(seg[np.isfinite(seg)]).sum())
        f = beta * u
        scored = scored & np.isfinite(f)
        return smooth(_rank_target(np.where(scored, -1.0 * f, np.nan),
                                   scored), live)

    return _futures_plan(lambda: weights,
                         label="W2-06_DOLLAR_FUNDING_SLOW_DIFFUSION")


# --------------------------------------------------------------------------- #
# W2-07  XA_COMPLEX_LEAVE_ONE_OUT_MOMENTUM           CROSS_ASSET / MOMENTUM
# --------------------------------------------------------------------------- #
def w2_07(row=None):
    """The PEERS' trend with the market's OWN return removed. A complex-level
    shock is priced at different speeds by markets whose participants
    specialise, so a market's peers still carry information its own path has
    already discounted. Not demeaned: demeaning would erase a group score."""
    layer = PN.futures_layer()
    ret = layer["ret"]
    groups = np.asarray(layer["economic_group"])

    def weights(t, live):
        scored = PN.universe_all_traded(layer, t, live)
        r = _log_sum_return(ret, t - 253, t - 22, 120)
        scored = scored & np.isfinite(r)
        f = np.full(ret.shape[0], np.nan)
        for key in set(groups[scored].tolist()):
            sel = scored & (groups == key)
            k = int(sel.sum())
            if k >= 3:
                tot = float(r[sel].sum())
                f[sel] = (tot - r[sel]) / (k - 1)
        scored = scored & np.isfinite(f)
        w = _rank_target(np.where(scored, +1.0 * f, np.nan), scored)
        return np.where(np.isfinite(w) & live, w, 0.0)

    return _futures_plan(lambda: weights,
                         label="W2-07_COMPLEX_LEAVE_ONE_OUT_MOMENTUM")


# --------------------------------------------------------------------------- #
# W2-08  FUT_BETTING_AGAINST_VOLATILITY_INTL_INDEX   EQUITY_INDEX / VOLATILITY
# --------------------------------------------------------------------------- #
def w2_08(row=None):
    """Frazzini-Pedersen leg construction, not a rank book. The risk-parity
    leg scaling harvests the leverage-aversion premium without leaving the
    book structurally short global equity beta, which a plain dollar-neutral
    rank book on a volatility score would be."""
    layer = PN.futures_layer()
    ret = layer["ret"]

    def weights(t, live):
        scored = PN.universe_intl_index(layer, t, live)
        sigma = _std_window(ret, t - 253, t - PN.PRICE_LAG + 1, 200)
        scored = scored & np.isfinite(sigma) & (sigma > 0)
        w = np.zeros(ret.shape[0])
        if int(scored.sum()) < 10:
            return w
        s = sigma[scored]
        med = float(np.median(s))
        rows = np.where(scored)[0]
        low = rows[sigma[rows] <= med]
        high = rows[sigma[rows] > med]
        if not len(low) or not len(high):
            return w
        for leg, side in ((low, +0.5), (high, -0.5)):
            inv = 1.0 / sigma[leg]
            w[leg] = side * inv / float(inv.sum())
        return np.where(np.isfinite(w) & live, w, 0.0)

    return _futures_plan(lambda: weights,
                         label="W2-08_BETTING_AGAINST_VOLATILITY_INTL_INDEX")


# --------------------------------------------------------------------------- #
# W2-09 / W2-10  the PANEL-F operating-profitability pair    US_EQUITY
# --------------------------------------------------------------------------- #
def _fundamental_score(sign: float, statistic: str):
    """Score the S&P cross-section from the FOUR-YEAR path of operating
    profitability. Every sample is what was FILED AND AVAILABLE at its own
    decision date, so the series carries no restatement and no look-ahead."""
    panel = PN.equity_panel()

    def score(p, t):
        n = p["tr"].shape[0]
        out = np.full(n, np.nan)
        x = PN.fundamental_samples(panel, t)
        if x is None:
            return out
        elig = PN.sp500_eligible(p, t)
        for i in np.where(elig)[0]:
            v = x[i]
            ok = np.isfinite(v)
            if int(ok.sum()) < PN.FUND_MIN_FINITE:
                continue
            k = np.arange(len(v), dtype=np.float64)[ok]
            y = v[ok]
            if statistic == "TREND":
                vk = float(np.var(k))
                if vk > EPS:
                    out[i] = float(np.cov(k, y, ddof=1)[0, 1] / vk)
            else:                                   # STABILITY
                d = np.diff(y)
                if len(d) >= PN.FUND_MIN_FINITE:
                    sd = float(np.std(d, ddof=1))
                    if sd > 0:
                        out[i] = sd
        return sign * out

    return score


def _equity_plan(score_fn, label):
    panel = PN.equity_panel()
    return {"book": B.BOOK_EQUITY_TOPN, "panel": panel,
            "score_fn": score_fn, "signal_sign": 1,
            "book_kwargs": {"elig": PN.sp500_eligible, "top_n": 100,
                            "cadence": 21, "horizon": 21, "label": label,
                            "first_date": PN.fundamental_first_date(panel),
                            "cost_rate": r59.EQ_COST_RATE_PER_SIDE}}


def w2_09(row=None):
    """The FIRST moment: the four-year DIRECTION of operating profitability.
    Investors anchor on the latest quarter and underreact to the trend."""
    return _equity_plan(_fundamental_score(+1.0, "TREND"),
                        "W2-09_OPERATING_PROFITABILITY_MULTIYEAR_TREND")


def w2_10(row=None):
    """The SECOND moment of the SAME owned series: stability. Investors
    overpay for uncertain cash-flow streams and underpay for predictable ones,
    so the compensation shows up as a premium to stability. Sign is -1: LONG
    the most stable."""
    return _equity_plan(_fundamental_score(-1.0, "STABILITY"),
                        "W2-10_OPERATING_PROFITABILITY_STABILITY")


EXECUTORS = {
    "w2_01": w2_01, "w2_02": w2_02, "w2_03": w2_03, "w2_04": w2_04,
    "w2_05": w2_05, "w2_06": w2_06, "w2_07": w2_07, "w2_08": w2_08,
    "w2_09": w2_09, "w2_10": w2_10,
}
