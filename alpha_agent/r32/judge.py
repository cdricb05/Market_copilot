"""alpha_agent.r32.judge - the ONE common economic judge for every sleeve.

The whole point of Release 32 is that six very different strategies become
comparable. That only works if ONE judge measures all of them, on the same
units, with the same costs, over a stated common window, against the same
alternatives - including cash.

What the judge measures:

* **After-cost return.** Cost is charged on TRADED NOTIONAL (``sum |dw|``,
  sells and buys), never on one-way turnover. Release 31 shipped that bug and
  every net return it produced understated cost by roughly half.
* **Cash as a real asset.** Unallocated weight earns the observed 13-week bill
  yield, lagged one session. A sleeve that sits in cash is making a decision and
  is scored on it, not given a free zero.
* **Standalone economics AND marginal portfolio value.** A sleeve that earns
  less than the benchmark can still be worth capital if it earns it when the
  benchmark does not; a sleeve that beats the benchmark by being the benchmark
  is worth nothing. Both numbers are reported, always.
* **Three windows.** Maximum legitimate history, the matched period, and the
  common overlap shared by every sleeve. A sleeve that wins only on its own
  favourable stretch is not a winner, and reporting one window hides that.

Statistical primitives are imported from :mod:`alpha_agent.r31.judge` and
:mod:`alpha_agent.r31.multiple_testing`. Release 32 adds sleeves, not a second
statistics library, and a second Sharpe implementation is exactly how two parts
of a system come to disagree about the same number.
"""
from __future__ import annotations

from typing import Optional

import numpy as np

from .. import r32
from ..r31.judge import (  # ONE statistics owner, reused
    annualise,
    cvar,
    max_drawdown,
    newey_west_t,
    sharpe,
    sortino,
    volatility,
)
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.judge"
JUDGE_SCHEMA = "r32_common_economic_judge/1"
ARTIFACT_NAME = "common_economic_judge.json"

#: Periods per year at the campaign's decision cadence (21 sessions ~ monthly).
PERIODS_PER_YEAR = 252.0 / float(_contract.STEP_SESSIONS)


# --------------------------------------------------------------------------- #
# Economics
# --------------------------------------------------------------------------- #
def traded_notional(previous: Optional[dict], target: dict) -> float:
    """``sum |dw|`` across the union of both books - sells AND buys.

    The first period is the initial purchase, so the notional is the book's own
    gross weight: you pay to get in.
    """
    if previous is None:
        return float(sum(abs(float(v)) for v in target.values()))
    keys = set(previous) | set(target)
    return float(sum(abs(float(target.get(k, 0.0)) - float(previous.get(k, 0.0)))
                     for k in keys))


def transition_cost(previous: Optional[dict], target: dict,
                    *, rate_per_side_bps: float) -> float:
    return traded_notional(previous, target) * (float(rate_per_side_bps) / 1e4)


def book_return(weights: dict, instrument_returns: dict, *,
                cash_weight: float, cash_return: float) -> float:
    """Gross period return of one research book, cash included."""
    total = float(cash_weight) * float(cash_return)
    for sym, w in weights.items():
        r = instrument_returns.get(sym)
        if r is None or not np.isfinite(r):
            # An instrument with no observed return contributes nothing and its
            # weight is treated as cash for that period rather than silently
            # dropped, which would inflate the return of what remains.
            total += float(w) * float(cash_return)
            continue
        total += float(w) * float(r)
    return float(total)


def evaluate_path(opportunities: list, *, instrument_returns_by_date: dict,
                  cash_return_by_date: dict, rate_per_side_bps: float) -> dict:
    """Turn a sleeve's opportunity sequence into an after-cost return path.

    The book built here is a MEASUREMENT DEVICE. It is not a portfolio target,
    it is never written anywhere operational, and no capital stands behind it.
    """
    gross, net, costs, dates, cashw, notional = [], [], [], [], [], []
    previous = None
    for opp in opportunities:
        d = opp.decision_date
        ir = instrument_returns_by_date.get(d)
        cr = cash_return_by_date.get(d)
        if ir is None or cr is None:
            continue
        w = dict(opp.recommended_exposure)
        g = book_return(w, ir, cash_weight=opp.cash_weight, cash_return=cr)
        tn = traded_notional(previous, w)
        c = tn * (float(rate_per_side_bps) / 1e4)
        gross.append(g)
        costs.append(c)
        net.append(g - c)
        cashw.append(opp.cash_weight)
        notional.append(tn)
        dates.append(d)
        previous = w
    return {"dates": dates,
            "gross": np.asarray(gross, dtype=float),
            "net": np.asarray(net, dtype=float),
            "cost": np.asarray(costs, dtype=float),
            "cash_weight": np.asarray(cashw, dtype=float),
            "traded_notional": np.asarray(notional, dtype=float),
            "research_book_is_not_a_portfolio_target": True}


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #
def _finite(a) -> np.ndarray:
    a = np.asarray(a, dtype=float)
    return a[np.isfinite(a)]


def metrics(period_returns, *, periods_per_year: float = PERIODS_PER_YEAR) -> dict:
    """The common metric block every sleeve reports."""
    r = _finite(period_returns)
    if r.size == 0:
        return {"n": 0, "annual_return": None, "annual_volatility": None,
                "sharpe": None, "sortino": None, "max_drawdown": None,
                "cvar_5": None, "hit_rate": None}
    scale = periods_per_year / (252.0 / float(_contract.STEP_SESSIONS))
    ann = annualise(r)
    return {"n": int(r.size),
            "annual_return": _none_if_nan(ann),
            "annual_volatility": _none_if_nan(volatility(r)),
            "sharpe": _none_if_nan(sharpe(r)),
            "sortino": _none_if_nan(sortino(r)),
            "max_drawdown": _none_if_nan(max_drawdown(r)),
            "cvar_5": _none_if_nan(cvar(r, 0.05)),
            "hit_rate": float(np.mean(r > 0.0)),
            "periods_per_year_scale": float(scale)}


def _none_if_nan(x):
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return None if not np.isfinite(v) else v


def excess_significance(sleeve_net, alternative_net, *, lags: int = 3) -> dict:
    """Newey-West t on the paired difference vs one alternative."""
    a = np.asarray(sleeve_net, dtype=float)
    b = np.asarray(alternative_net, dtype=float)
    n = min(a.size, b.size)
    if n == 0:
        return {"n": 0, "mean_excess": None, "t_stat": None}
    d = a[:n] - b[:n]
    d = d[np.isfinite(d)]
    if d.size == 0:
        return {"n": 0, "mean_excess": None, "t_stat": None}
    return {"n": int(d.size), "mean_excess": float(np.mean(d)),
            "annual_excess": _none_if_nan(annualise(d)),
            "t_stat": _none_if_nan(newey_west_t(d, lags))}


def volatility_matched_control(sleeve_net, benchmark_net, cash_net) -> dict:
    """The passive mix of benchmark and cash with the SAME risk as the sleeve.

    This is the control that decides whether a sleeve has skill, and Release 32
    exists partly because the obvious alternatives do not.

    Measuring a sleeve against CASH rewards it for holding equities: over any
    long window, anything with equity exposure beats bills, so a "significant"
    excess over cash says nothing about the sleeve. Measuring against the
    benchmark punishes it for holding less than 100 % equity, which is exactly
    what a defensive sleeve is supposed to do, so a genuinely skilful risk
    manager looks like a failure.

    The volatility-matched control removes both errors. It holds

        w = sleeve_volatility / benchmark_volatility

    of the benchmark and the rest in cash, so it carries the sleeve's risk with
    none of its timing. Beating it means the sleeve earned more than static
    de-risking would have earned at the same risk - which is skill, and is the
    only thing worth capital.

    ``w`` is computed from the realised volatilities of the SCORED window. That
    is in-sample, and deliberately so: it is used to build the control, not to
    forecast, and a control fitted to match the sleeve is harder to beat rather
    than easier.
    """
    a = np.asarray(sleeve_net, dtype=float)
    b = np.asarray(benchmark_net, dtype=float)
    c = np.asarray(cash_net, dtype=float)
    n = min(a.size, b.size, c.size)
    if n < 2:
        return {"ok": False, "reason": "TOO_FEW_OBSERVATIONS"}
    a, b, c = a[:n], b[:n], c[:n]
    ok = np.isfinite(a) & np.isfinite(b) & np.isfinite(c)
    a, b, c = a[ok], b[ok], c[ok]
    if a.size < 2:
        return {"ok": False, "reason": "TOO_FEW_FINITE_OBSERVATIONS"}
    vs, vb = float(np.std(a, ddof=1)), float(np.std(b, ddof=1))
    if not np.isfinite(vb) or vb <= 0.0:
        return {"ok": False, "reason": "BENCHMARK_HAS_NO_VARIATION"}
    # Leverage is not available to this project, so the control cannot hold more
    # than 100 % of the benchmark. A sleeve riskier than the benchmark is
    # therefore compared with the benchmark itself, which is the honest
    # conservative choice rather than an imaginary levered alternative.
    w = min(1.0, vs / vb)
    matched = w * b + (1.0 - w) * c
    return {"ok": True, "equity_weight": float(w),
            "sleeve_volatility": vs, "benchmark_volatility": vb,
            "control_annual_return": _none_if_nan(annualise(matched)),
            "control_sharpe": _none_if_nan(sharpe(matched)),
            "leverage_available": False,
            "series": matched}


def marginal_portfolio_value(sleeve_net, benchmark_net, *,
                             weights=(0.05, 0.10, 0.20)) -> dict:
    """Does adding a slice of this sleeve improve a benchmark-only portfolio?

    This is the question that decides whether an opportunity deserves capital.
    A sleeve can lose to the benchmark standalone and still raise the Sharpe of
    a portfolio that holds the benchmark, because it earns at different times.
    The converse is the trap the frontier exists to catch: a sleeve that beats
    the benchmark by BEING the benchmark adds a higher return and no value.
    """
    a = np.asarray(sleeve_net, dtype=float)
    b = np.asarray(benchmark_net, dtype=float)
    n = min(a.size, b.size)
    if n < 2:
        return {"base_sharpe": None, "by_weight": {}, "best_weight": None,
                "improves": False}
    a, b = a[:n], b[:n]
    ok = np.isfinite(a) & np.isfinite(b)
    a, b = a[ok], b[ok]
    if a.size < 2:
        return {"base_sharpe": None, "by_weight": {}, "best_weight": None,
                "improves": False}
    base = sharpe(b)
    rows, best_w, best_s = {}, None, base
    for w in weights:
        blend = (1.0 - w) * b + w * a
        s = sharpe(blend)
        rows[str(w)] = {"sharpe": _none_if_nan(s),
                        "delta_sharpe": _none_if_nan(s - base),
                        "annual_return": _none_if_nan(annualise(blend)),
                        "max_drawdown": _none_if_nan(max_drawdown(blend))}
        if np.isfinite(s) and s > best_s:
            best_s, best_w = s, w
    corr = float(np.corrcoef(a, b)[0, 1]) if a.size > 2 else float("nan")
    return {"base_sharpe": _none_if_nan(base), "by_weight": rows,
            "best_weight": best_w, "best_sharpe": _none_if_nan(best_s),
            "correlation_with_benchmark": _none_if_nan(corr),
            "improves": best_w is not None}


# --------------------------------------------------------------------------- #
# Scoring one candidate
# --------------------------------------------------------------------------- #
def score(opportunities: list, *, sleeve: str, instrument_returns_by_date: dict,
          cash_return_by_date: dict, benchmark_return_by_date: dict) -> dict:
    """Score one sleeve configuration end to end."""
    rate = _contract.COST_RATE_PER_SIDE_BPS[sleeve]
    path = evaluate_path(opportunities,
                         instrument_returns_by_date=instrument_returns_by_date,
                         cash_return_by_date=cash_return_by_date,
                         rate_per_side_bps=rate)
    dates = path["dates"]
    if not dates:
        return {"scored": False, "reason": "NO_SCORED_DECISIONS", "n": 0}
    cash = np.asarray([cash_return_by_date[d] for d in dates], dtype=float)
    bench = np.asarray([benchmark_return_by_date.get(d, np.nan)
                        for d in dates], dtype=float)
    out = {
        "scored": True,
        "sleeve": sleeve,
        "n": len(dates),
        "first_decision": dates[0],
        "last_decision": dates[-1],
        "cost_rate_per_side_bps": rate,
        "cost_base": _contract.COST_BASE,
        "gross": metrics(path["gross"]),
        "net": metrics(path["net"]),
        "cash": metrics(cash),
        "benchmark": metrics(bench),
        "mean_cash_weight": float(np.mean(path["cash_weight"])),
        "mean_traded_notional": float(np.mean(path["traded_notional"])),
        "annual_cost_drag": _none_if_nan(annualise(path["cost"])),
        "vs_cash": excess_significance(path["net"], cash),
        "vs_benchmark": excess_significance(path["net"], bench),
        "marginal_portfolio_value": marginal_portfolio_value(path["net"], bench),
        "research_book_is_not_a_portfolio_target": True,
    }
    control = volatility_matched_control(path["net"], bench, cash)
    matched_series = control.pop("series", None)
    out["volatility_matched_control"] = control
    out["vs_volatility_matched_control"] = (
        excess_significance(path["net"], matched_series)
        if matched_series is not None
        else {"n": 0, "mean_excess": None, "t_stat": None})
    out["beats_cash"] = bool(
        (out["vs_cash"].get("mean_excess") or 0.0) > 0.0)
    out["beats_benchmark"] = bool(
        (out["vs_benchmark"].get("mean_excess") or 0.0) > 0.0)
    out["beats_volatility_matched_control"] = bool(
        (out["vs_volatility_matched_control"].get("mean_excess") or 0.0) > 0.0)
    out["_net_path"] = path["net"]
    out["_dates"] = dates
    return out


# --------------------------------------------------------------------------- #
# Behaviour hash and contract
# --------------------------------------------------------------------------- #
def behaviour_declaration() -> dict:
    """Everything that changes what a score MEANS."""
    return {
        "cost_base": _contract.COST_BASE,
        "cost_rate_per_side_bps": dict(_contract.COST_RATE_PER_SIDE_BPS),
        "cash_yield_symbol": _contract.CASH_YIELD_SYMBOL,
        "cash_is_scored": True,
        "cash_lagged_one_session": True,
        "benchmark": _contract.BENCHMARK_TOTAL_RETURN,
        "hold_sessions": _contract.HOLD_SESSIONS,
        "step_sessions": _contract.STEP_SESSIONS,
        "periods_per_year": PERIODS_PER_YEAR,
        "marginal_value_weights": [0.05, 0.10, 0.20],
        "missing_instrument_return_treated_as": "CASH",
        "significance": "NEWEY_WEST_T_ON_PAIRED_DIFFERENCE",
        "newey_west_lags": 3,
        "primary_control": "VOLATILITY_MATCHED_BENCHMARK_CASH_MIX",
        "primary_control_rationale":
            "excess over cash rewards merely holding equities; excess over the "
            "benchmark punishes holding less than 100 % equity. The "
            "volatility-matched mix carries the sleeve's risk with none of its "
            "timing, so beating it is skill.",
        "control_leverage_available": False,
    }


def behaviour_hash() -> str:
    """Bind judge BEHAVIOUR, not a schema name.

    Release 31 bound a constant schema string here and a corrected cost model
    silently reused candidates measured under the old one. A behaviour hash
    means a changed cost, cadence, benchmark or cash treatment produces
    different specification hashes, so the two populations cannot mix in one
    leaderboard, lockbox or multiple-testing denominator.
    """
    return r32.sha(behaviour_declaration())


def build_contract(*, campaign_id: str = _contract.CAMPAIGN_ID) -> dict:
    payload = {"calculation_owner": CALCULATION_OWNER,
               "campaign_id": campaign_id,
               "behaviour": behaviour_declaration(),
               "behaviour_hash": behaviour_hash(),
               "windows_reported": ["MAXIMUM_LEGITIMATE_HISTORY",
                                    "MATCHED_PERIOD", "COMMON_OVERLAP"],
               "qualification_requires_beating_cash": _contract.MUST_BEAT_CASH,
               "qualification_requires_benchmark_or_marginal_value":
                   _contract.MUST_BEAT_BENCHMARK_OR_ADD_MARGINAL_VALUE}
    body = r32.artifact_body(JUDGE_SCHEMA, payload)
    body["judge_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID):
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r32.write_json(path_for(body["campaign_id"]), body)
