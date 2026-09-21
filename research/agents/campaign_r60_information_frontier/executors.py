r"""campaign_r60_information_frontier.executors - the EIGHT cells, as code.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION.

One function per pre-registered cell, each returning a PLAN the canonical
runner executes:

    {"book", "panel", "make_fn" | "score_fn", "signal_sign", "book_kwargs"}

WHAT THE SECOND CALLABLE MEANS depends on the book:

    FUTURES_DATED_CONTRACT   weights_fn(t, live) -> float[n_markets]
    EQUITY_TOPN              score_fn(panel, t)  -> float[n_names], NaN = unscorable
    EQUITY_DAILY_TRANCHE     score_fn(panel, t)  -> float[n_names]

THE SIGN IS BAKED INTO THE BOOK, NEVER FLIPPED AT EVALUATION. A cell whose
pre-registered ``expected_sign`` is -1 holds the BOTTOM of its feature, and it
does so because the executor negates the score HERE, before any return is
computed. ``signal_sign`` in the plan reports what was frozen; it does not
change what was traded.

TIMING, obeyed everywhere below and stated once per feature:

    ret / ret2 / price / turnover    windows close at t-1   (PRICE_LAG)
    open interest                    windows close at t-2   (OI_LAG, the
                                                             newest row is
                                                             provisional)
    fundamentals                     the value whose FILING DATE precedes the
                                     decision date (latest-filed-as-of-t)
    dividends                        dated by declarationDate, NEVER ex-date

OWN SESSIONS, NOT GRID SLOTS. A market that did not trade is NaN, so every
futures window below counts a market's OWN sessions. Counting grid slots is
the documented R56 trap: a complete US series fails a "63 of 63" test whenever
three US holidays fall in the window.
"""
from __future__ import annotations

import numpy as np

from alpha_agent import r59
from alpha_agent.agents_v2 import books as B

from . import panels as PN

EPS = 1e-12
MIN_MARKETS = 12
MIN_INTL_MARKETS = 8
TOP_N = 100

#: The frozen one-way ceiling. Every cell in this campaign states the same
#: rule: measured feature-side turnover above it HALTS the cell, and the cell
#: is NEVER re-smoothed, re-banded or re-tranched into compliance, because a
#: retune is a new experiment and a new burden charge.
TURNOVER_CEILING = r59.GATE_MAX_TURNOVER

_TURNOVER_CACHE: dict = {}


class TurnoverCeilingExceeded(RuntimeError):
    """The pre-registered turnover ceiling refused the cell, before any return
    was scored. This is a RESULT, not a failure to be worked around."""


def measure_turnover(name: str, plan: dict) -> float:
    """Median one-way feature-side turnover over EVERY decision.

    No return is scored: turnover is a property of the weights path alone, so
    this costs no statistical budget and reveals no layer. That is precisely
    why the ceiling can be enforced BEFORE the experiment is measured.
    """
    if name in _TURNOVER_CACHE:
        return _TURNOVER_CACHE[name]
    from alpha_agent.r57 import engine as K
    from alpha_agent.r59 import native

    fn = plan["score_fn"]
    panel = plan["panel"]
    dates = np.asarray(panel["dates"])
    turns = []
    if plan["book"] == B.BOOK_FUTURES:
        idx = native.decision_indices(dates, r59.CADENCE, r59.HORIZON)
        idx = idx[idx >= int(np.searchsorted(dates, r59.DISCOVERY_START))]
        live = np.isfinite(panel["ret"])
        prev = None
        for t in idx:
            w = np.asarray(fn(int(t), live[:, int(t)]), dtype=np.float64)
            w = np.where(np.isfinite(w), w, 0.0)
            if prev is not None:
                turns.append(float(np.abs(w - prev).sum()) / 2.0)
            prev = w
    else:
        hold = 5 if plan["book"] == B.BOOK_EQUITY_TRANCHE else r59.CADENCE
        idx = K.decision_indices(dates, hold, r59.DISCOVERY_START,
                                 hold if hold == 5 else r59.HORIZON)
        top_n = (plan.get("book_kwargs") or {}).get("top_n", TOP_N)
        prev = None
        for t in idx:
            s = np.asarray(fn(panel, int(t)), dtype=np.float64)
            fin = np.where(np.isfinite(s))[0]
            if len(fin) < top_n:
                continue
            cur = set(fin[np.argsort(-s[fin])[:top_n]].tolist())
            if prev is not None:
                turns.append(len(cur - prev) / float(top_n))
            prev = cur
    med = float(np.median(turns)) if turns else float("nan")
    _TURNOVER_CACHE[name] = med
    return med


def gated(name: str, plan: dict) -> dict:
    """Apply the frozen turnover ceiling to a built plan."""
    med = measure_turnover(name, plan)
    plan["measured_oneway_turnover"] = med
    if np.isfinite(med) and med > TURNOVER_CEILING:
        raise TurnoverCeilingExceeded(
            "TURNOVER_CEILING_EXCEEDED: %s measured median one-way turnover "
            "%.4f against the pre-registered ceiling %.2f. The cell HALTS; it "
            "is not re-smoothed, re-banded or re-tranched into compliance."
            % (name, med, TURNOVER_CEILING))
    return plan


# --------------------------------------------------------------------------- #
# Own-session windows
# --------------------------------------------------------------------------- #
def _own_cumsums(values: np.ndarray, mask: np.ndarray):
    """Cumulative value and cumulative COUNT over masked (own) sessions.

    Returned arrays have one extra leading zero column, so a half-open window
    [a, b) is ``cum[:, b] - cum[:, a]`` with no special-casing at the edges.
    """
    v = np.where(mask, np.nan_to_num(values, nan=0.0), 0.0)
    n_m, n_d = v.shape
    cv = np.zeros((n_m, n_d + 1), dtype=np.float64)
    cn = np.zeros((n_m, n_d + 1), dtype=np.int64)
    np.cumsum(v, axis=1, out=cv[:, 1:])
    np.cumsum(mask.astype(np.int64), axis=1, out=cn[:, 1:])
    return cv, cn


def _window_start(cn: np.ndarray, end_excl: int, n_own: int) -> np.ndarray:
    """First index of the window holding the LAST ``n_own`` own sessions
    before ``end_excl``, per market. -1 where the market has too few."""
    have = cn[:, end_excl]
    target = have - int(n_own)
    out = np.full(cn.shape[0], -1, dtype=np.int64)
    for i in range(cn.shape[0]):
        if target[i] < 0:
            continue
        out[i] = int(np.searchsorted(cn[i, :end_excl + 1], target[i],
                                     side="left"))
    return out


def _own_sum(cv, cn, end_excl: int, n_own: int):
    """Sum over the last ``n_own`` own sessions before ``end_excl``."""
    start = _window_start(cn, end_excl, n_own)
    ok = start >= 0
    out = np.full(cv.shape[0], np.nan)
    idx = np.where(ok)[0]
    if len(idx):
        out[idx] = cv[idx, end_excl] - cv[idx, start[idx]]
    return out, ok


def _own_mean(cv, cn, end_excl: int, n_own: int, min_obs: int):
    """Mean over the last ``n_own`` own sessions, NaN below ``min_obs``."""
    start = _window_start(cn, end_excl, n_own)
    out = np.full(cv.shape[0], np.nan)
    for i in range(cv.shape[0]):
        s = start[i]
        if s < 0:
            continue
        n = cn[i, end_excl] - cn[i, s]
        if n >= int(min_obs) and n > 0:
            out[i] = (cv[i, end_excl] - cv[i, s]) / float(n)
    return out


def _demean(feature: np.ndarray, groups, scored: np.ndarray,
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


# --------------------------------------------------------------------------- #
# Futures universes
# --------------------------------------------------------------------------- #
def _base_live(layer, t, live, *, min_own=227, window=252):
    m = np.asarray(live, bool) & np.asarray(layer["certified"], bool)
    own = layer["own"]
    lo, hi = max(0, t - window), max(0, t)
    n = own[:, lo:hi].sum(axis=1)
    return m & (n >= int(min_own))


# --------------------------------------------------------------------------- #
# r60_01 / r60_06  BASIS MOMENTUM - the rescued deferred leg
# --------------------------------------------------------------------------- #
def _basis_momentum_plan(rows_fn, group_key: str, *, min_markets=MIN_MARKETS):
    """front-minus-deferred cumulative return over 63 OWN PAIRED sessions.

    A session enters BOTH cumulations or NEITHER: the paired mask is
    ``isfinite(ret) & isfinite(ret2)``. Summing each leg over its own finite
    set would difference two different session sets, which is not a basis at
    all - it is a calendar artefact that would look like signal.
    """
    layer = PN.futures_layer()
    ret, ret2 = layer["ret"], layer["ret2"]
    paired = np.isfinite(ret) & np.isfinite(ret2)
    cv1, cn = _own_cumsums(ret, paired)
    cv2, _ = _own_cumsums(ret2, paired)
    rows = rows_fn(layer)
    groups = layer[group_key]

    def weights_fn(t, live):
        end = max(0, int(t) - PN.PRICE_LAG) + 1        # closes at t-1
        s1, ok1 = _own_sum(cv1, cn, end, 63)
        s2, _ = _own_sum(cv2, cn, end, 63)
        feat = np.where(ok1, s1 - s2, np.nan)
        elig = _base_live(layer, int(t), live) & rows & np.isfinite(feat)
        if int(elig.sum()) < min_markets:
            return np.zeros(ret.shape[0])
        dm = _demean(feat, groups, elig)
        scored = elig & np.isfinite(dm)
        if int(scored.sum()) < min_markets:
            return np.zeros(ret.shape[0])
        return B.rank_weights(dm, scored, min_markets=min_markets)

    return {"book": B.BOOK_FUTURES, "panel": layer, "score_fn": weights_fn,
            "signal_sign": 1}


def r60_01(row):
    """FUT_BASIS_MOMENTUM_COMMODITY. Commodities, demeaned within R38
    economic group. expected_sign +1: hold the top of the feature."""
    return gated("r60_01", _basis_momentum_plan(PN.commodity_rows,
                                                "economic_group"))


def _xa_rows(layer):
    """The DISJOINT set: rates, FX and equity-index futures. Shares no market
    with r60_01, which is what makes it a replication rather than a re-test."""
    keep = (r59.AC_RATES, r59.AC_FX, r59.AC_EQUITY_INDEX)
    return np.array([a in keep for a in layer["asset_class"]])


def r60_06(row):
    """XA_BASIS_MOMENTUM_REPLICATION. IDENTICAL formula to r60_01 on the
    disjoint instrument set, demeaned within r59 asset class because these
    classes have too few members per economic group."""
    return gated("r60_06", _basis_momentum_plan(_xa_rows, "asset_class"))


# --------------------------------------------------------------------------- #
# r60_05 / r60_07  OPEN-INTEREST GROWTH - zero is MISSING
# --------------------------------------------------------------------------- #
def _oi_growth_plan(rows_fn, *, demean_key, min_markets):
    """log(mean OI over 21 own sessions / mean OI over 63), both closing t-2.

    ``panels.futures_layer`` has already turned every ``open_interest == 0``
    into NaN, so a zero can neither enter a mean nor become a denominator.
    Windows require 15 and 40 positive observations respectively.

    expected_sign is -1: crowding predicts LOWER returns, so the book SHORTS
    the fastest open-interest expansion. The negation happens here.
    """
    layer = PN.futures_layer()
    oi = layer["open_interest"]
    have = np.isfinite(oi) & (oi > 0)
    cv, cn = _own_cumsums(oi, have)
    rows = rows_fn(layer)
    groups = layer[demean_key] if demean_key else None

    def weights_fn(t, live):
        end = max(0, int(t) - PN.OI_LAG) + 1            # closes at t-2
        m21 = _own_mean(cv, cn, end, 21, 15)
        m63 = _own_mean(cv, cn, end, 63, 40)
        with np.errstate(invalid="ignore", divide="ignore"):
            feat = np.log(np.where((m21 > 0) & (m63 > 0), m21 / m63, np.nan))
        elig = _base_live(layer, int(t), live) & rows & np.isfinite(feat)
        if int(elig.sum()) < min_markets:
            return np.zeros(oi.shape[0])
        dm = _demean(feat, groups, elig) if groups is not None else feat
        scored = elig & np.isfinite(dm)
        if int(scored.sum()) < min_markets:
            return np.zeros(oi.shape[0])
        # expected_sign = -1: short the crowded, long the contracting.
        return B.rank_weights(-dm, scored, min_markets=min_markets)

    return {"book": B.BOOK_FUTURES, "panel": layer, "score_fn": weights_fn,
            "signal_sign": -1}


def r60_05(row):
    """FUT_OPEN_INTEREST_GROWTH_COMMODITY, demeaned within economic group."""
    return gated("r60_05",
                 _oi_growth_plan(PN.commodity_rows,
                                 demean_key="economic_group",
                                 min_markets=MIN_MARKETS))


def r60_07(row):
    """FUT_INTL_INDEX_OPEN_INTEREST_GROWTH. No demeaning - the universe is a
    single economic group. Flat unless at least 8 markets score."""
    return gated("r60_07",
                 _oi_growth_plan(PN.intl_index_rows, demean_key=None,
                                 min_markets=MIN_INTL_MARKETS))


# --------------------------------------------------------------------------- #
# Equity helpers
# --------------------------------------------------------------------------- #
def _fund_at(fund, panel, t, concept):
    j = fund["slot"].get(int(t))
    if j is None:
        return None
    return fund["cube"][:, j, fund["f_ix"][concept]]


def _eq_elig(panel, t):
    return PN.extension_eligible(panel, int(t))


# --------------------------------------------------------------------------- #
# r60_02  NET SHARE ISSUANCE
# --------------------------------------------------------------------------- #
def r60_02(row):
    """log(S_t / S_{t-252}) on filed share counts, latest-filed-as-of-t.

    S_{t-252} is the count that WAS KNOWN a year ago, not today's restated
    opinion about a year ago - that distinction is the whole point of keeping
    every filing's own ``filed`` date.

    NON-POSITIVE COUNTS ARE DROPPED ON BOTH ENDS. A sampled issuer carries
    -25,878,050 shares, later corrected to +25,878,050; a ratio spanning that
    sign flip would park the name at a cross-sectional extreme every decision.

    expected_sign -1: the long-only book holds the LOWEST issuance (the
    repurchasers), so the score is negated here and never at evaluation.
    """
    panel = PN.equity_panel()
    fund = PN.fundamental_panel()
    prim = "CommonStockSharesOutstanding"
    fall = "EntityCommonStockSharesOutstanding"

    def score_fn(_panel, t):
        j = fund["slot"].get(int(t))
        n = len(panel["symbols"])
        if j is None:
            return np.full(n, np.nan)
        s_now = fund["cube"][:, j, fund["f_ix"][prim]]
        alt = fund["cube"][:, j, fund["f_ix"][fall]]
        s_now = np.where(np.isfinite(s_now), s_now, alt)
        s_old = fund["shares_252"][:, j]
        good = (np.isfinite(s_now) & np.isfinite(s_old)
                & (s_now > 0) & (s_old > 0))
        with np.errstate(invalid="ignore", divide="ignore"):
            feat = np.where(good, np.log(np.where(good, s_now, 1.0)
                                         / np.where(good, s_old, 1.0)),
                            np.nan)
        feat = np.where(_eq_elig(panel, int(t)), feat, np.nan)
        return -feat                      # expected_sign = -1

    return gated("r60_02", {
        "book": B.BOOK_EQUITY_TOPN, "panel": panel, "score_fn": score_fn,
        "signal_sign": -1,
        "book_kwargs": {"elig": _eq_elig, "top_n": TOP_N,
                        "cost_rate": B.EQ_EXT_COST_RATE,
                        "first_date": r59.DISCOVERY_START}})


# --------------------------------------------------------------------------- #
# r60_03  GROSS PROFITABILITY
# --------------------------------------------------------------------------- #
def r60_03(row):
    """GrossProfit / Assets, with Revenues - CostOfRevenue as the declared
    fallback FROM THE SAME FILING. Non-positive Assets is UNSCORED. No
    winsorisation, no imputation - both were ruled out at pre-registration."""
    panel = PN.equity_panel()
    fund = PN.fundamental_panel()

    def score_fn(_panel, t):
        j = fund["slot"].get(int(t))
        n = len(panel["symbols"])
        if j is None:
            return np.full(n, np.nan)
        gp = fund["cube"][:, j, fund["f_ix"]["GrossProfit"]]
        rev = fund["cube"][:, j, fund["f_ix"]["Revenues"]]
        cor = fund["cube"][:, j, fund["f_ix"]["CostOfRevenue"]]
        assets = fund["cube"][:, j, fund["f_ix"]["Assets"]]
        alt = np.where(np.isfinite(rev) & np.isfinite(cor), rev - cor, np.nan)
        gp = np.where(np.isfinite(gp), gp, alt)
        ok = np.isfinite(gp) & np.isfinite(assets) & (assets > 0)
        with np.errstate(invalid="ignore", divide="ignore"):
            feat = np.where(ok, gp / np.where(ok, assets, 1.0), np.nan)
        return np.where(_eq_elig(panel, int(t)), feat, np.nan)

    return gated("r60_03", {
        "book": B.BOOK_EQUITY_TOPN, "panel": panel, "score_fn": score_fn,
        "signal_sign": 1,
        "book_kwargs": {"elig": _eq_elig, "top_n": TOP_N,
                        "cost_rate": B.EQ_EXT_COST_RATE,
                        "first_date": r59.DISCOVERY_START}})


# --------------------------------------------------------------------------- #
# r60_04  DIVIDEND DECLARATION DRIFT
# --------------------------------------------------------------------------- #
class CoveragePrecheckFailed(RuntimeError):
    """The pre-registered BLOCKING coverage gate refused the cell."""


def r60_04(row):
    """Declared dividend change, dated by declarationDate and NEVER by ex-date.

    THE BLOCKING PRE-CHECK RUNS FIRST, before any return is scored, exactly as
    pre-registered: non-null declarationDate per calendar year 2011-2023 must
    clear 0.80. Threshold and years were frozen before measurement. If it
    fails, the cell raises and the runner RECORDS the failure - there is no
    ex-date fallback and no post-hoc truncation.
    """
    panel = PN.equity_panel()
    store = PN.dividend_store()
    check = PN.dividend_precheck(store)
    if not check["passed"]:
        raise CoveragePrecheckFailed(
            "UNMEASURABLE_COVERAGE: declarationDate non-null below %.2f in %s"
            % (check["floor"], sorted(check["failed_years"])))

    dates = np.asarray(panel["dates"])
    sym_ix = {str(s): i for i, s in enumerate(panel["symbols"])}
    n = len(panel["symbols"])

    # (declarationDate, amount) per symbol, declaration-sorted. A record with
    # a null declarationDate is DROPPED and never dated by its ex-date.
    decl: dict = {}
    for sym, rows in store.items():
        i = sym_ix.get(sym)
        if i is None:
            continue
        got = []
        for r in rows or ():
            d, v = r.get("declarationDate"), r.get("unadjustedValue")
            if v is None:
                v = r.get("value")
            if d and v is not None:
                got.append((str(d), float(v)))
        if got:
            got.sort()
            decl[i] = got

    def score_fn(_panel, t):
        out = np.full(n, np.nan)
        hi = str(dates[max(0, int(t) - PN.PRICE_LAG)])
        lo = str(dates[max(0, int(t) - PN.PRICE_LAG - 63)])
        prior_lo = str(dates[max(0, int(t) - PN.PRICE_LAG - 756)])
        for i, rows in decl.items():
            latest = None
            for d, v in rows:
                if lo < d <= hi:
                    latest = (d, v)
            if latest is None:
                continue
            d, v = latest
            prev = None
            for d0, v0 in rows:
                if prior_lo < d0 < d:
                    prev = v0
            if prev is None:
                out[i] = 0.25 if v > 0 else -0.25        # INITIATION
            elif v <= 0:
                out[i] = -0.25                           # OMISSION
            elif prev > 0:
                out[i] = float(np.log(v / prev))
        return np.where(_eq_elig(panel, int(t)), out, np.nan)

    return gated("r60_04", {
        "book": B.BOOK_EQUITY_TOPN, "panel": panel, "score_fn": score_fn,
        "signal_sign": 1, "coverage_precheck": check,
        "book_kwargs": {"elig": _eq_elig, "top_n": TOP_N,
                        "cost_rate": B.EQ_EXT_COST_RATE,
                        "first_date": r59.DISCOVERY_START}})


# --------------------------------------------------------------------------- #
# r60_08  H5 IMMEDIACY REVERSAL
# --------------------------------------------------------------------------- #
def r60_08(row):
    """Turnover-weighted 5-session return, as ONE parameter-free product.

    ( cumulative adjusted log return over the 5 sessions ending t-1 ) x
    ( log ratio of 5-session to 63-session mean dollar turnover, clipped to
      [-1, 1] ).

    The two legs are never scored separately and no threshold is fitted, so
    the hypothesis is falsifiable in a single test.

    expected_sign -1: the long-only book holds the BOTTOM 100 - the largest
    declines made on abnormally elevated turnover. Negated here.
    """
    panel = PN.equity_panel()
    tr = panel["tr"]
    dv = panel["dv"]
    with np.errstate(invalid="ignore", divide="ignore"):
        logtr = np.log(np.where(np.isfinite(tr) & (tr > 0), tr, np.nan))

    def score_fn(_panel, t):
        t1 = max(0, int(t) - PN.PRICE_LAG)
        a, b = max(0, t1 - 5), t1
        n = tr.shape[0]
        if b <= a:
            return np.full(n, np.nan)
        r5 = logtr[:, b] - logtr[:, a]
        w5 = dv[:, a:b]
        w63 = dv[:, max(0, t1 - 63):b]
        with np.errstate(invalid="ignore", divide="ignore"):
            m5 = np.nanmean(np.where(np.isfinite(w5), w5, np.nan), axis=1)
            m63 = np.nanmean(np.where(np.isfinite(w63), w63, np.nan), axis=1)
            ratio = np.log(np.where((m5 > 0) & (m63 > 0), m5 / m63, np.nan))
        feat = r5 * np.clip(ratio, -1.0, 1.0)
        feat = np.where(_eq_elig(panel, int(t)), feat, np.nan)
        return -feat                      # expected_sign = -1

    return gated("r60_08", {
        "book": B.BOOK_EQUITY_TRANCHE, "panel": panel,
        "score_fn": score_fn, "signal_sign": -1,
        "book_kwargs": {"elig": _eq_elig, "top_n": TOP_N, "hold": 5,
                        "cost_rate": B.EQ_EXT_COST_RATE,
                        "first_date": r59.DISCOVERY_START}})


EXECUTORS = {
    "r60_01": r60_01, "r60_02": r60_02, "r60_03": r60_03, "r60_04": r60_04,
    "r60_05": r60_05, "r60_06": r60_06, "r60_07": r60_07, "r60_08": r60_08,
}
