r"""R66 - can a $99,127 paper book actually HOLD an institutional hedged trade?

RESEARCH ONLY. READ ONLY. This module computes no return, scores no signal and
writes nothing outside the R66 campaign artifacts. It answers ONE question the
estate has never asked: given the real contract specifications and the real
operational book, is a multi-leg futures structure EXPRESSIBLE at this size at
all - before anyone argues about whether it is profitable.

WHY THIS IS A SEPARATE QUESTION FROM ALPHA
------------------------------------------
Every previous release measured returns per unit of NOTIONAL or against a
volatility-matched book, and reported capital as a FRACTION. A fraction hides
the only thing that binds a small account: a futures contract is an indivisible
unit. You cannot hold 0.37 of a ZN. So a structure whose hedge ratio is 0.53 is
not expressible at one contract per leg at ANY level of conviction - it must be
scaled until both legs are near-integer, and that scaling multiplies the
notional the book must carry.

THE THREE CONSTRAINTS, MEASURED SEPARATELY
------------------------------------------
    MARGIN        can the free cash post it? (initial margin, per leg)
    NOTIONAL      how large is the resulting market exposure against NAV?
    GRANULARITY   how badly does integer rounding break the intended hedge?

They fail in different places and the estate has always collapsed them into one
number. Margin is usually affordable here; notional and granularity are not.

WHAT THE MARGIN NUMBERS ARE, AND ARE NOT
----------------------------------------
``futures_market_registry.json`` carries Norgate's CURRENT margin per contract
for 105 markets. R43 recorded its state as CURRENT_ONLY_NOT_POINT_IN_TIME and
used it as a sanity check. The same honesty applies here and harder:

  * These are CURRENT margins applied to a CURRENT feasibility question, which
    is the one case where that is legitimate - we are asking whether the book
    could hold the trade TODAY, not what margin was in 1998.
  * No exchange SPAN spread credit is owned. A real calendar or curve spread
    receives a large margin offset, so the committed capital computed here is
    an UPPER BOUND on margin and is deliberately conservative.
  * Variation margin is NOT modelled. A futures position marks daily against
    cash, so the relevant cash buffer is not the initial margin but the initial
    margin plus a drawdown reserve. That is reported separately.
"""
from __future__ import annotations

import csv
import json
import os
from pathlib import Path

import numpy as np

CALCULATION_OWNER = "research.agents.campaign_r66_hedged_rv.feasibility"

R38_ROOT = Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
                r"\r38_native_futures_information_frontier_v4")
REGISTRY = R38_ROOT / "futures_market_registry.json"
LAYER_DIR = R38_ROOT / "native_contract_layer"

#: The estate's own cash-buffer convention for a margin book: hold the initial
#: margin PLUS this multiple of it against variation margin before the position
#: is called affordable. Declared here, before any structure is evaluated.
VARIATION_MARGIN_RESERVE_MULTIPLE = 1.0

#: SPAN SPREAD CREDIT - A NAMED ASSUMPTION, NOT DATA.
#:
#: A real exchange margins a recognised spread far below the sum of its legs,
#: because the legs offset. The estate owns NO SPAN parameter file and no
#: exchange spread-credit table, so the true offset is UNKNOWN. R43 recorded the
#: same limitation and used the registry margins as a sanity check only.
#:
#: Rather than pick a number and let it harden into a fact, every committed
#: capital figure is reported at THREE declared credits. 0.00 is the honest
#: upper bound and is the DEFAULT used for every verdict; the other two show how
#: much the verdict depends on an assumption the estate cannot verify.
#: Provenance: published CME inter- and intra-commodity spread credits commonly
#: fall in the 50-75% range for recognised spreads. That is context for the
#: sensitivity band, and it is NOT owned data.
SPAN_CREDIT_SENSITIVITY = (0.00, 0.50, 0.75)
SPAN_CREDIT_DEFAULT = 0.00
SPAN_CREDIT_STATE = "UNKNOWN_NOT_OWNED_ASSUMPTION_ONLY"

#: Verdict vocabulary for a structure against one book.
V_IMPLEMENTABLE = "IMPLEMENTABLE"
V_SINGLE_ONLY = "SINGLE_POSITION_ONLY"
V_NOT_AT_THIS_NAV = "NOT_IMPLEMENTABLE_AT_THIS_NAV"

#: A structure whose gross notional exceeds this multiple of NAV is refused
#: outright: the book would carry more market exposure in one hedged trade than
#: it holds in its entire equity portfolio. Declared before any evaluation.
MAX_GROSS_NOTIONAL_OVER_NAV = 1.0


def _registry() -> dict:
    d = json.loads(REGISTRY.read_text(encoding="utf-8"))
    return d.get("markets", d)


def _last_close_curve_store(symbol: str):
    """Last front-contract settle from the R41 curve store (``c1``)."""
    try:
        import pandas as pd

        from paper_trader.alpha_agent.r43 import panels as P
        d = P.futures_daily(symbol)
        if d is None or "c1" not in d.columns:
            return None, None
        c = pd.to_numeric(d["c1"], errors="coerce").dropna()
        if not len(c):
            return None, None
        return float(c.iloc[-1]), str(c.index[-1])[:10]
    except Exception:                                       # noqa: BLE001
        return None, None


def _vol_curve_store(symbol: str, n: int = 504):
    """Trailing realised daily vol from the R41 curve store (``ret1``)."""
    try:
        import pandas as pd

        from paper_trader.alpha_agent.r43 import panels as P
        d = P.futures_daily(symbol)
        if d is None or "ret1" not in d.columns:
            return None
        r = pd.to_numeric(d["ret1"], errors="coerce").dropna()
        if len(r) < 60:
            return None
        a = np.asarray(r.iloc[-n:], dtype=float)
        return float(np.std(a, ddof=1))
    except Exception:                                       # noqa: BLE001
        return None


def _last_close(symbol: str):
    """The last settle, with its date.

    Prefers the certified R38 dated-contract layer. Falls back to the R41 curve
    store for the markets that never entered the certified layer - SR3, ZQ, the
    international government bonds and the international short rates are all in
    the 105-market registry and the curve store but NOT in the certified 68. A
    feasibility question about those markets is still answerable; it just has to
    say which substrate answered it.
    """
    p = LAYER_DIR / ("%s.csv" % symbol)
    if not p.exists():
        return _last_close_curve_store(symbol)
    last = None
    with p.open() as f:
        for row in csv.DictReader(f):
            if row.get("close"):
                last = row
    if last is None:
        return None, None
    return float(last["close"]), str(last["Date"])[:10]


def market_facts(symbols: list) -> dict:
    """Per-market notional, margin and cost. No returns are read."""
    reg = _registry()
    out = {}
    for s in symbols:
        m = reg.get(s) or {}
        meta = m.get("metadata", m)
        pv, mg = meta.get("point_value"), meta.get("margin")
        px, dt = _last_close(s)
        if not pv or not mg or px is None:
            out[s] = {"state": "SPEC_OR_PRICE_MISSING",
                      "point_value": pv, "margin": mg, "last_close": px}
            continue
        out[s] = {
            "state": "OK",
            "asset_class": meta.get("asset_class"),
            "economic_group": meta.get("economic_group"),
            "last_close": px, "as_of": dt,
            "point_value": float(pv),
            "notional_per_contract_usd": px * float(pv),
            "initial_margin_usd": float(mg),
            "margin_fraction_of_notional": float(mg) / (px * float(pv)),
            "cost_bps_per_side": meta.get("cost_bps_per_side"),
        }
    return out


def _vol(symbol: str, n: int = 504):
    """Trailing realised daily vol of the FRONT contract.

    Used ONLY to size a hedge ratio. A volatility is a risk characteristic, not
    a performance statistic: nothing here reads the spread's P&L, so computing
    it costs the campaign no look at any result.
    """
    p = LAYER_DIR / ("%s.csv" % symbol)
    if not p.exists():
        return _vol_curve_store(symbol, n)
    r = []
    with p.open() as f:
        for row in csv.DictReader(f):
            v = row.get("ret")
            if v:
                try:
                    r.append(float(v))
                except ValueError:
                    pass
    if len(r) < 60:
        return _vol_curve_store(symbol, n)
    a = np.array(r[-n:], dtype=float)
    a = a[np.isfinite(a)]
    return float(np.std(a, ddof=1)) if len(a) >= 60 else None


def hedge_ratio(long_sym: str, short_sym: str):
    """Notional ratio that equalises the two legs' DAILY RISK.

    ``h`` = (vol_long * notional_long) / (vol_short * notional_short), i.e. how
    many SHORT-leg contracts neutralise one LONG-leg contract in dollar
    volatility terms. For two points on one issuer's curve this is very close to
    the DV01 ratio, and it needs no duration data the estate does not own.
    """
    f = market_facts([long_sym, short_sym])
    if f[long_sym]["state"] != "OK" or f[short_sym]["state"] != "OK":
        return None
    vl, vs = _vol(long_sym), _vol(short_sym)
    if not vl or not vs:
        return None
    dv_long = vl * f[long_sym]["notional_per_contract_usd"]
    dv_short = vs * f[short_sym]["notional_per_contract_usd"]
    return dv_long / dv_short if dv_short else None


#: A hedge that is within this fraction of the intended risk ratio is treated as
#: faithful. Declared before any structure was evaluated. 10 % of the hedge leg
#: is a residual an institutional desk would tolerate and a small book must.
HEDGE_ERROR_TOLERANCE = 0.10


def _best_integer_expression(h: float, max_units: int = 12,
                             tolerance: float = HEDGE_ERROR_TOLERANCE):
    """The SMALLEST integer (n_long, n_short) that hedges ``h`` acceptably.

    This is the granularity constraint made explicit, and the objective matters.
    A large desk would pick the most ACCURATE ratio; a $99k book must pick the
    SMALLEST one that is accurate enough, because every extra contract is
    notional it cannot carry. So the rule is: minimise total contracts subject
    to hedge error <= ``tolerance``, and only if nothing qualifies fall back to
    the least-inaccurate expression and say so.

    Worked: h = 0.531 (ZN against ZB). 2:1 gives 0.500, a 5.8 % error, and costs
    3 contracts. 9:5 gives 0.556, a 4.6 % error, and costs 14. Both hedge; only
    one is a trade a small book could contemplate.
    """
    within, any_expr = [], []
    for nl in range(1, max_units + 1):
        ns = int(round(nl * h))
        if ns < 1:
            continue
        err = abs((ns / nl) - h) / h
        any_expr.append((err, nl + ns, nl, ns))
        if err <= tolerance:
            within.append((nl + ns, err, nl, ns))
    if within:
        total, err, nl, ns = min(within)
        return {"n_long": nl, "n_short": ns, "hedge_error_fraction": err,
                "total_contracts": total, "within_tolerance": True}
    if not any_expr:
        return None
    err, total, nl, ns = min(any_expr)
    return {"n_long": nl, "n_short": ns, "hedge_error_fraction": err,
            "total_contracts": total, "within_tolerance": False}


def structure_feasibility(long_sym: str, short_sym: str, *, nav: float,
                          free_cash: float, label: str = "") -> dict:
    """Is ONE minimum faithful expression of this two-leg structure holdable?"""
    f = market_facts([long_sym, short_sym])
    out = {"label": label or "%s_vs_%s" % (long_sym, short_sym),
           "long": long_sym, "short": short_sym,
           "calculation_owner": CALCULATION_OWNER,
           "nav_usd": nav, "free_cash_usd": free_cash}
    if f[long_sym]["state"] != "OK" or f[short_sym]["state"] != "OK":
        return {**out, "state": "SPEC_OR_PRICE_MISSING"}
    h = hedge_ratio(long_sym, short_sym)
    if h is None:
        return {**out, "state": "HEDGE_RATIO_NOT_COMPUTABLE"}
    ie = _best_integer_expression(h)
    if ie is None:
        return {**out, "state": "NOT_EXPRESSIBLE_IN_INTEGER_CONTRACTS",
                "risk_hedge_ratio": h}

    nl, ns = ie["n_long"], ie["n_short"]
    notl = nl * f[long_sym]["notional_per_contract_usd"]
    nots = ns * f[short_sym]["notional_per_contract_usd"]
    margin = (nl * f[long_sym]["initial_margin_usd"]
              + ns * f[short_sym]["initial_margin_usd"])
    reserve = margin * (1.0 + VARIATION_MARGIN_RESERVE_MULTIPLE)

    gross = notl + nots
    blockers = []
    if margin > free_cash:
        blockers.append("INITIAL_MARGIN_EXCEEDS_FREE_CASH")
    elif reserve > free_cash:
        blockers.append("NO_VARIATION_MARGIN_RESERVE")
    if gross > nav * MAX_GROSS_NOTIONAL_OVER_NAV:
        blockers.append("GROSS_NOTIONAL_EXCEEDS_NAV")
    if not ie.get("within_tolerance"):
        blockers.append("NOT_HEDGEABLE_WITHIN_TOLERANCE_AT_ANY_SMALL_SIZE")

    # The verdict the director commissioned. GROSS NOTIONAL is the structural
    # test and no cash policy can change it, so it decides the worst verdict;
    # margin is a policy test and only separates IMPLEMENTABLE from
    # SINGLE_POSITION_ONLY.
    if ("GROSS_NOTIONAL_EXCEEDS_NAV" in blockers
            or "NOT_HEDGEABLE_WITHIN_TOLERANCE_AT_ANY_SMALL_SIZE" in blockers):
        verdict = V_NOT_AT_THIS_NAV
    elif margin > free_cash or reserve > free_cash:
        verdict = V_SINGLE_ONLY
    else:
        verdict = V_IMPLEMENTABLE

    span = {}
    for credit in SPAN_CREDIT_SENSITIVITY:
        m = margin * (1.0 - credit)
        span["credit_%d_pct" % int(round(credit * 100))] = {
            "committed_capital_usd": m,
            "fits_free_cash": bool(m <= free_cash),
            "fits_free_cash_with_variation_reserve": bool(
                m * (1.0 + VARIATION_MARGIN_RESERVE_MULTIPLE) <= free_cash)}

    return {**out,
            "state": "HOLDABLE" if not blockers else "NOT_HOLDABLE",
            "verdict": verdict,
            "blockers": blockers,
            "span_credit_state": SPAN_CREDIT_STATE,
            "span_credit_sensitivity": span,
            "risk_hedge_ratio": h,
            "minimum_faithful_expression": ie,
            "gross_notional_usd": notl + nots,
            "gross_notional_over_nav": (notl + nots) / nav,
            "net_notional_usd": notl - nots,
            "net_notional_over_nav": (notl - nots) / nav,
            "committed_capital_initial_margin_usd": margin,
            "committed_capital_over_free_cash": margin / free_cash,
            "cash_needed_with_variation_reserve_usd": reserve,
            "margin_is_an_upper_bound": (
                "no exchange SPAN spread credit is owned; a real spread would "
                "receive an offset, so this overstates committed capital"),
            "as_of": f[long_sym]["as_of"]}
