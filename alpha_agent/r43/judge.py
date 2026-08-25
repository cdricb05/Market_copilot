"""alpha_agent.r43.judge - THE universal implementable economic judge.

Release 41 scored every book per unit of TRADED NOTIONAL against a ZERO
control. Release 42 scored one book per unit of COMMITTED CAPITAL against
the RISK-FREE RATE. Both are special cases of one equation, and which case
applies is a property of the COLLATERAL, declared in the frozen contract
before any result exists.

    pnl_on_notional(t) = gross(t) - cost(t) - borrow(t)
    pnl_on_capital(t)  = pnl_on_notional(t) / K
    benchmark(t)       = on(t) * rf(t) * (1 - rho)
    excess(t)          = pnl_on_capital(t) - benchmark(t)

where ``K`` is committed capital per unit of the notional the gross stream
is quoted on, and ``rho`` in [0, 1] is the fraction of the risk-free rate
that capital EARNS while immobilised.

    rho = 0.0, K = 1.35  ->  reproduces alpha_agent.r42.capital exactly
                             (crypto: coin and stablecoin pay nothing)
    rho = 1.0, K = 1.00  ->  reproduces the R41 convention exactly
                             (per-notional return, zero control)
    rho = 1.0, K = margin ->  the honest treatment of an exchange-traded
                             futures book: margin is posted in T-bills and
                             IS remunerated, so the correction is a rescale
                             onto committed capital, NOT a subtraction of
                             the risk-free rate

Both equivalences are proven numerically by the Release-43 regression
against the canonical owners; this module is never allowed to drift into a
third convention.

Nothing here decides ``rho`` or ``K`` from a result. Both come from
:mod:`alpha_agent.r43.contract`.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import R41_RESEARCH_ROOT
from . import contract as C
from ..r41 import evidence as EV

CALCULATION_OWNER = "alpha_agent.r43.judge"

FRED_PANEL = R41_RESEARCH_ROOT / "_data_fred" / "fred_daily_panel.csv"
#: Adopted UNCHANGED from the R42 owner (alpha_agent.r42.contract) so the
#: convention equivalence below is EXACT and this release does not invent a
#: third risk-free convention. CMT_3M carries the pre-2018 history the
#: overnight series do not.
RISK_FREE_SERIES_PREFERENCE = ("SOFR", "EFFR", "CMT_3M")
TRADING_DAYS = 252.0
CALENDAR_DAYS = 365.0


# --------------------------------------------------------------------------- #
# Risk-free rate (the same FRED panel R41 acquired; read-only)
# --------------------------------------------------------------------------- #
_RF_CACHE = None


def _rf_series() -> pd.Series:
    global _RF_CACHE
    if _RF_CACHE is None:
        df = pd.read_csv(FRED_PANEL, index_col=0, parse_dates=True)
        df.index = pd.to_datetime(df.index).tz_localize(None)
        s = None
        for c in RISK_FREE_SERIES_PREFERENCE:
            if c in df.columns:
                v = pd.to_numeric(df[c], errors="coerce")
                s = v if s is None else s.fillna(v)
        if s is None:
            raise RuntimeError("no risk-free series in %s" % FRED_PANEL)
        _RF_CACHE = (s.astype(float) / 100.0).sort_index()
    return _RF_CACHE


def risk_free_daily(index, *, day_count: float = TRADING_DAYS) -> pd.Series:
    """Per-period risk-free rate on ``index``, forward-filled from the last
    published rate (which is exactly what an overnight balance earns)."""
    idx = pd.DatetimeIndex(index)
    naive = idx.tz_localize(None) if idx.tz is not None else idx
    s = _rf_series()
    rf = s.reindex(s.index.union(naive)).ffill().reindex(naive)
    out = pd.Series((rf / day_count).to_numpy(), index=idx, name="rf")
    return out.fillna(0.0)


def risk_free_summary(index, *, day_count: float = TRADING_DAYS) -> dict:
    rf = risk_free_daily(index, day_count=day_count)
    return {"series_preference": list(RISK_FREE_SERIES_PREFERENCE),
            "source": "FRED daily panel acquired by R41 (read-only)",
            "day_count": day_count,
            "mean_annualised": float(rf.mean() * day_count),
            "min_annualised": float(rf.min() * day_count),
            "max_annualised": float(rf.max() * day_count),
            "n_periods": int(rf.notna().sum())}


# --------------------------------------------------------------------------- #
# Committed capital
# --------------------------------------------------------------------------- #
def futures_committed_capital(leg_groups, leg_weights=None) -> dict:
    """Conservative committed capital for a futures book, per unit of ONE
    leg's notional (the unit R41 quoted gross on).

    ``leg_groups`` is the ordered list of contract.COST_BPS_PER_SIDE cost
    groups the book trades; ``leg_weights`` their notional weights (absolute
    hedge ratios). Capital is MARGIN_STRESS_BUFFER_MULTIPLIER times the sum
    of initial margins, floored at MARGIN_FLOOR_FRACTION_OF_GROSS of gross
    notional so no book can claim an implausibly small denominator.
    """
    groups = list(leg_groups)
    w = [1.0] * len(groups) if leg_weights is None else \
        [abs(float(x)) for x in leg_weights]
    if len(w) != len(groups):
        raise ValueError("leg_weights must match leg_groups")
    im = 0.0
    missing = []
    for g, wi in zip(groups, w):
        frac = C.FUTURES_MARGIN_FRACTION.get(g)
        if frac is None:
            missing.append(g)
            frac = max(C.FUTURES_MARGIN_FRACTION.values())
        im += float(frac) * wi
    gross = sum(w)
    K = max(C.MARGIN_STRESS_BUFFER_MULTIPLIER * im,
            C.MARGIN_FLOOR_FRACTION_OF_GROSS * gross)
    return {"legs": groups, "leg_weights": w,
            "gross_notional_per_leg_unit": gross,
            "initial_margin": im,
            "stress_buffer_multiplier": C.MARGIN_STRESS_BUFFER_MULTIPLIER,
            "committed_capital": K,
            "effective_leverage_on_capital": (gross / K) if K else None,
            "unknown_cost_groups": missing,
            "floor_binding": bool(
                C.MARGIN_FLOOR_FRACTION_OF_GROSS * gross
                > C.MARGIN_STRESS_BUFFER_MULTIPLIER * im)}


#: A margin book quoted on the exchange's minimum margin is quoted at 15-30x
#: leverage, which produces annualised volatilities above 100% and drawdowns
#: of -100%. Those are not returns anyone can earn; they are the arithmetic
#: of dividing by a number chosen by a clearing house to cover two days of
#: risk, not by an investor to survive a decade of it.
#:
#: POST-FREEZE ADDITION, DISCLOSED: this denominator did not exist in the
#: frozen contract. It is REPORTED ALONGSIDE the frozen primary and never
#: replaces it, it is computed from the FITTING ZONE ONLY so it cannot leak
#: judged-zone volatility, and it is strictly HARSHER than every frozen
#: denominator for every book in this release - it can only reduce a claimed
#: return, never inflate one.
RISK_SIZED_TARGET_VOL_ANN = 0.10
RISK_SIZED_CAPITAL_IS_POST_FREEZE = True


def risk_sized_capital(pnl_on_notional: pd.Series, fit_dates, *,
                       target_vol: float = RISK_SIZED_TARGET_VOL_ANN,
                       floor: float = 0.0,
                       day_count: float = TRADING_DAYS) -> dict:
    """Capital that sizes the book to ``target_vol`` annualised.

    The volatility is measured on ``fit_dates`` (Zone A) only. A book whose
    own risk demands more capital than the clearing house does is quoted on
    the larger of the two, which is what an investor would actually have to
    commit.
    """
    s = pnl_on_notional.reindex(pd.DatetimeIndex(fit_dates)).dropna()
    if len(s) < 250:
        return {"state": "NOT_RUN", "reason": "fewer than 250 fitting days",
                "committed_capital": None}
    vol_ann = float(np.nanstd(s.to_numpy(dtype=float), ddof=1)
                    * np.sqrt(day_count))
    K = vol_ann / float(target_vol) if target_vol > 0 else None
    return {"target_vol_ann": target_vol,
            "book_vol_ann_on_notional": vol_ann,
            "risk_sized_capital": K,
            "margin_capital": floor,
            "committed_capital": max(K or 0.0, float(floor)),
            "binding": ("RISK" if (K or 0.0) >= float(floor) else "MARGIN"),
            "measured_on": "FITTING_ZONE_ONLY",
            "post_freeze_disclosed": True}


def capital_table(leg_groups, leg_weights=None) -> dict:
    """Every reported denominator for the same futures book."""
    base = futures_committed_capital(leg_groups, leg_weights)
    im, gross = base["initial_margin"], base["gross_notional_per_leg_unit"]
    return {
        "TRADED_NOTIONAL": 1.0,
        "COMMITTED_MARGIN": max(im, C.MARGIN_FLOOR_FRACTION_OF_GROSS * gross),
        "COMMITTED_MARGIN_X2": base["committed_capital"],
        "GROSS_EXPOSURE": gross,
        "_detail": base,
    }


# --------------------------------------------------------------------------- #
# Costs
# --------------------------------------------------------------------------- #
def cost_stream(turnover: pd.Series, leg_groups, leg_weights=None, *,
                multiplier: float = 1.0) -> pd.Series:
    """Per-period execution cost per unit of ONE leg's notional.

    ``turnover`` is |change in position| in leg-notional units; each unit
    traded crosses every leg's half-spread once.
    """
    groups = list(leg_groups)
    w = [1.0] * len(groups) if leg_weights is None else \
        [abs(float(x)) for x in leg_weights]
    bps = 0.0
    for g, wi in zip(groups, w):
        v = C.COST_BPS_PER_SIDE.get(g)
        if not isinstance(v, (int, float)):
            v = max(x for x in C.COST_BPS_PER_SIDE.values()
                    if isinstance(x, (int, float)))
        bps += float(v) * wi
    return (turnover.abs().fillna(0.0) * (bps / 1e4) * float(multiplier)) \
        .rename("cost")


# --------------------------------------------------------------------------- #
# THE equation
# --------------------------------------------------------------------------- #
def implementable_book(gross_per_notional: pd.Series,
                       position: pd.Series, *,
                       committed_capital: float,
                       collateral_class: str,
                       cost: pd.Series = None,
                       borrow: pd.Series = None,
                       day_count: float = TRADING_DAYS,
                       rho_override: float = None) -> pd.DataFrame:
    """Assemble the complete equation for ONE position stream.

    ``gross_per_notional`` is the per-period P&L of holding ONE unit of the
    book's leg notional, ALREADY aligned to the period it is earned in.
    ``position`` is the size HELD in that period (callers lag their own
    signals; the judge never guesses a convention).
    """
    if collateral_class not in C.COLLATERAL_CLASSES:
        raise ValueError("undeclared collateral class %r" % collateral_class)
    rho = float(C.COLLATERAL_CLASSES[collateral_class]["collateral_earns_rf"]
                if rho_override is None else rho_override)
    K = float(committed_capital)
    if K <= 0:
        raise ValueError("committed capital must be positive")

    idx = gross_per_notional.index
    held = position.reindex(idx).astype(float)
    on = (held != 0) & held.notna()
    gross = (held * gross_per_notional.reindex(idx)).fillna(0.0)
    cost = (pd.Series(0.0, index=idx) if cost is None
            else cost.reindex(idx).fillna(0.0))
    borrow = (pd.Series(0.0, index=idx) if borrow is None
              else borrow.reindex(idx).fillna(0.0))

    rf = risk_free_daily(idx, day_count=day_count).fillna(0.0)
    pnl_on_notional = gross - cost - borrow
    pnl_on_capital = pnl_on_notional / K
    # The capital forgoes only the fraction of the risk-free rate its
    # collateral does not earn. rho is contractual, never fitted.
    benchmark = on.astype(float) * rf * (1.0 - rho)
    financing_forgone = on.astype(float) * rf * (1.0 - rho) * K
    excess = pnl_on_capital - benchmark

    return pd.DataFrame({
        "held": held, "on": on.astype(float),
        "gross": gross, "cost": cost, "borrow": borrow,
        "pnl_on_notional": pnl_on_notional,
        "pnl_on_capital": pnl_on_capital,
        "rf": rf, "rho": rho, "committed_capital": K,
        "financing_forgone": financing_forgone,
        "benchmark": benchmark, "excess": excess,
    }, index=idx)


def score(book: pd.DataFrame, dates=None, *, overlap: int = 1,
          day_count: float = TRADING_DAYS) -> dict:
    """Score a book's EXCESS stream with the canonical R41 evidence owner."""
    d = book if dates is None else book.reindex(pd.DatetimeIndex(dates))
    d = d.dropna(subset=["pnl_on_capital"])
    if len(d) < 24:
        return {"n": int(len(d)), "excess_ann": None, "excess_t_hac": None,
                "insufficient": True}
    K = float(d["committed_capital"].iloc[0])
    # Gross and cost are handed over SEPARATELY, both expressed on capital,
    # so the scorecard's cost-stress multipliers act on the real cost term.
    card = EV.scorecard(((d["gross"] - d["borrow"]) / K).to_numpy(),
                        (d["cost"] / K).to_numpy(),
                        d["benchmark"].to_numpy(),
                        periods_per_year=day_count, overlap=overlap)
    card.pop("diff_stream", None)
    card["n"] = int(len(d))
    card["gross_ann_on_notional"] = float(np.nanmean(d["gross"]) * day_count)
    card["cost_ann_on_notional"] = float(np.nanmean(d["cost"]) * day_count)
    card["return_on_capital_ann"] = float(
        np.nanmean(d["pnl_on_capital"]) * day_count)
    card["cash_hurdle_ann"] = float(np.nanmean(d["benchmark"]) * day_count)
    card["committed_capital"] = float(d["committed_capital"].iloc[0])
    card["rho"] = float(d["rho"].iloc[0])
    card["days_on"] = int(d["on"].sum())
    return card


def dual_quotation(gross_per_notional: pd.Series, position: pd.Series, *,
                   leg_groups, leg_weights=None,
                   collateral_class: str,
                   cost: pd.Series = None, dates=None,
                   overlap: int = 1,
                   day_count: float = TRADING_DAYS) -> dict:
    """Quote the SAME book on every declared denominator.

    This is what the contract's DUAL_QUOTATION_REQUIRED means in code: a
    result can never be presented on the denominator that flatters it,
    because every denominator is always reported.
    """
    table = capital_table(leg_groups, leg_weights)
    out = {}
    for name in C.CAPITAL_MODELS_REPORTED:
        K = float(table[name])
        bk = implementable_book(gross_per_notional, position,
                                committed_capital=K,
                                collateral_class=collateral_class,
                                cost=cost, day_count=day_count)
        card = score(bk, dates, overlap=overlap, day_count=day_count)
        out[name] = {
            "committed_capital": K,
            "return_on_capital_ann": card.get("return_on_capital_ann"),
            "cash_hurdle_ann": card.get("cash_hurdle_ann"),
            "excess_ann": card.get("excess_ann"),
            "excess_t_hac": card.get("excess_t_hac"),
            "sharpe": card.get("sharpe"),
            "vol_ann": card.get("vol_ann"),
            "max_drawdown": card.get("max_drawdown"),
            "is_primary": name == C.PRIMARY_CAPITAL_MODEL,
        }
    out["_capital_detail"] = table["_detail"]
    out["_collateral_class"] = collateral_class
    out["_rho"] = C.COLLATERAL_CLASSES[collateral_class]["collateral_earns_rf"]
    return out


# --------------------------------------------------------------------------- #
# Convention equivalences (asserted by the regression, reported in artifacts)
# --------------------------------------------------------------------------- #
def convention(name: str) -> dict:
    """The (K, rho) pair that reproduces a named prior-release convention."""
    conv = {
        "R41_PER_NOTIONAL_ZERO_CONTROL": {"K": 1.0, "rho": 1.0,
                                          "owner": "alpha_agent.r41.evidence"},
        "R42_COMMITTED_CAPITAL_CASH_CONTROL": {
            "K": C.COLLATERAL_CLASSES["UNREMUNERATED_FULLY_FUNDED"][
                "committed_capital"],
            "rho": 0.0, "owner": "alpha_agent.r42.capital"},
    }
    if name not in conv:
        raise ValueError("unknown convention %r" % name)
    return conv[name]
