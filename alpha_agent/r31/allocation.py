"""alpha_agent.r31.allocation - the ONE Release 31 portfolio-construction seam.

Campaign v2 turned a candidate's predictions into a portfolio by taking the top N
names at roughly equal weight, with N in {15, 25, 40} and cash pinned at zero. That
construction answers "which names does this model like?", which is a fine
DIAGNOSTIC and is not the business question. The business question is the one
Release 30 built an allocator to answer:

    "If every investable dollar were cash right now, given everything legitimately
     known at this timestamp, what portfolio should we own?"

A judge that forces the system to own 25 names cannot tell a model that found
nothing worth owning from a model that found twenty-five good names, because both
are made to hold twenty-five. Cash has to be a real choice or the whole comparison
is rigged in favour of whichever model is least bad at a task nobody asked for.

Both tracks land here
---------------------
    TRACK A   information -> expected return -> THIS SEAM -> stocks + cash
    TRACK B   information -> proposed weights -> THIS SEAM -> stocks + cash

Track A delegates the optimisation itself to ``engine.zero_base_allocator``. Track
B may propose weights directly and never needs a return forecast, but its proposal
is made FEASIBLE here, against the same canonical caps, the same liquidity floor
and the same cost semantics Track A faces. Two constraint systems would mean the
architectures were compared on two different sets of rules, and the more permissive
one would win on the strength of its permissions.

This module owns NO mathematics of its own. Covariance comes from
``engine.holding_opportunity_cost.build_covariance``; the optimisation comes from
``engine.zero_base_allocator.optimise``; caps come from ``name_caps``; the cost
rate comes from the canonical policy. What it owns is the ASSEMBLY of
point-in-time inputs for those owners, and the guarantee that both tracks are
assembled identically.
"""
from __future__ import annotations

from typing import Optional

import numpy as np

from ...engine import holding_opportunity_cost as _hoc
from ...engine import zero_base_allocator as _zb

CALCULATION_OWNER = "alpha_agent.r31.allocation"

#: The two architectures under comparison.
TRACK_A = "FORECAST_THEN_ALLOCATE"
TRACK_B = "DIRECT_PORTFOLIO_DECISION"
TRACKS = (TRACK_A, TRACK_B)

#: Historical sector classification is NOT reconstructable point-in-time from the
#: owned estate, and the canonical PIT sector owner declares the current entity
#: SIC snapshot inadmissible for historical construction. Applying today's sectors
#: backwards would violate the rule it claims to enforce, so the sector cap is
#: DISABLED historically and the limitation is reported on every result rather
#: than being quietly absorbed into a number.
SECTOR_STATE = "UNMEASURABLE_PIT"


def _sector_map(tickers) -> dict:
    """Sectors that DISABLE the sector cap rather than fake it.

    The obvious encoding of "sector unknown" - give every name the same sentinel -
    is catastrophically wrong here, and silently so. The canonical constraint set
    caps any one sector at 25% of the book, so putting all 500 names in a single
    sector caps the ENTIRE PORTFOLIO at 25% invested. Every candidate would then
    appear to hold 75% cash, the zero-base allocator would look broken, and the
    campaign would report a cash preference that is an artifact of a placeholder
    string.

    Assigning each name its own singleton sector makes the sector constraint
    non-binding by construction - a one-name sector can never exceed the 25%
    sector cap before it hits the 10% name cap - which is the honest encoding of
    a constraint the point-in-time evidence cannot support. The name cap and the
    liquidity cap continue to bind normally.
    """
    return {str(tk): "PIT_SECTOR_UNKNOWN::%s" % (tk,) for tk in tickers}


def policy() -> dict:
    return _zb.default_policy()


# --------------------------------------------------------------------------- #
# Point-in-time covariance, from the canonical owner
# --------------------------------------------------------------------------- #
def build_covariance(*, tickers, returns_by_ticker: dict, dates,
                     pol: Optional[dict] = None) -> dict:
    """Covariance over PAST returns only, via the canonical builder.

    ``returns_by_ticker`` must already be truncated at the decision date by the
    caller. This module does not slice history itself, because a covariance that
    silently reaches one day past the decision is the single easiest way to leak
    the future into a backtest and the hardest to see afterwards.
    """
    pol = pol or policy()
    aligned = {"dates": list(dates), "series": dict(returns_by_ticker)}
    return _hoc.build_covariance(tickers=list(tickers),
                                 aligned_returns=aligned, policy=pol)


def horizon_scale(cov: dict, *, sessions: int) -> dict:
    """Scale a DAILY covariance to the holding horizon.

    The allocator states that ``cov_h`` arrives already horizon-scaled, so it
    holds no second opinion about the horizon. Scaling linearly in time is the
    i.i.d. convention the canonical daily builder's output is expressed in.
    """
    k = float(sessions)
    return {i: {j: float(v) * k for j, v in row.items()} for i, row in cov.items()}


# --------------------------------------------------------------------------- #
# Track A: expected return -> canonical zero-base allocation
# --------------------------------------------------------------------------- #
def zero_base_target(*, tickers, mu: dict, sigma: dict, cov_h: dict,
                     cov_included, adv: dict, nav: float = 100000.0,
                     pol: Optional[dict] = None,
                     current_weight: Optional[dict] = None) -> dict:
    """The canonical zero-base target: stocks + CASH.

    ``current_weight=None`` is the true zero-base question - the search starts
    from all cash and no transition cost is charged, which is the formal statement
    that existing holdings carry no investment privilege. Supplying it produces
    the implementable target, where the cost of moving is priced.

    Cash is whatever the optimiser does not invest. It is free to be 100%: if no
    name clears its own risk and cost, holding nothing is the correct answer and
    the judge must be able to observe a candidate reaching it.
    """
    pol = pol or policy()
    candidates = [{"ticker": tk, "adv_dollar": float(adv.get(tk, 0.0) or 0.0)}
                  for tk in tickers]
    caps = _zb.name_caps(candidates=candidates, nav=float(nav), policy=pol)
    sector_of = _sector_map(tickers)

    res = _zb.optimise(candidates=candidates, mu=dict(mu),
                       sigma_forecast=dict(sigma), cov_h=dict(cov_h),
                       cov_included=list(cov_included), caps=caps,
                       sector_of=sector_of, policy=pol,
                       current_weight=current_weight)

    w = {tk: float(v) for tk, v in (res.get("weights") or {}).items()
         if float(v) > 0.0}
    invested = float(sum(w.values()))
    return {
        "track": TRACK_A,
        "weights": w,
        "invested_weight": invested,
        # Cash is a residual by construction, never a plug: the allocator's budget
        # constraint is an inequality, so anything it declines to invest IS cash.
        "cash_weight": max(0.0, 1.0 - invested),
        "names_held": len(w),
        "converged": bool(res.get("converged")),
        "convergence_reason": res.get("convergence_reason"),
        "iterations": res.get("iterations"),
        "allocator_owner": _zb.CALCULATION_OWNER,
        "sector_cap_state": SECTOR_STATE,
    }


# --------------------------------------------------------------------------- #
# Track B: proposed weights -> the SAME canonical feasibility
# --------------------------------------------------------------------------- #
def feasible_portfolio(*, tickers, proposed: dict, adv: dict,
                       nav: float = 100000.0,
                       pol: Optional[dict] = None) -> dict:
    """Make a directly-proposed portfolio feasible under the canonical constraints.

    Track B is allowed to skip the return forecast entirely - that is the point of
    the architecture - but it is not allowed to skip the constraints. Applied here,
    in this order:

      long only              a negative proposal is not a short book, it is an
                             infeasible one; the campaign models no short side
      name cap               canonical ``max_name_weight``
      liquidity cap          canonical ADV participation, a HARD bound
      minimum position       canonical floor; released weight becomes cash
      gross <= 100%          scaled down if over, never scaled UP

    The last rule matters: a proposal summing to 0.6 is LEFT at 0.6 and the
    remaining 0.4 is cash. Normalising it to 1.0 would silently overrule a
    deliberate decision to hold cash, which is the very freedom Track B is being
    granted.
    """
    pol = pol or policy()
    candidates = [{"ticker": tk, "adv_dollar": float(adv.get(tk, 0.0) or 0.0)}
                  for tk in tickers]
    caps = _zb.name_caps(candidates=candidates, nav=float(nav), policy=pol)
    floor = float(pol["min_position_weight"])

    w = {}
    dust_weight = 0.0
    dust_names = 0
    for tk in tickers:
        v = float(proposed.get(tk, 0.0) or 0.0)
        if not np.isfinite(v) or v <= 0.0:
            continue                                   # long only
        v = min(v, float(caps.get(tk, 0.0)))           # name + liquidity cap
        if v >= floor:                                 # minimum position
            w[tk] = v
        else:
            dust_weight += v
            dust_names += 1

    gross = float(sum(w.values()))
    scaled = False
    if gross > 1.0:
        w = {tk: v / gross for tk, v in w.items()}
        gross, scaled = 1.0, True

    return {
        "track": TRACK_B,
        "weights": w,
        "invested_weight": gross,
        "cash_weight": max(0.0, 1.0 - gross),
        "names_held": len(w),
        "scaled_to_gross_limit": scaled,
        # Weight the proposal asked for and the canonical floor turned into cash.
        # REPORTED, because otherwise a diffuse Track-B proposal reads as a
        # deliberate preference for cash when much of it is a proposal the book
        # could not hold. A softmax over ~500 names puts about 0.2% in each, which
        # is below the canonical 0.5% floor, so a learner that has not concentrated
        # is converted almost entirely to cash. That is the correct operational
        # answer - and it is a different fact from "the model wanted cash", so the
        # two are reported separately rather than summed into one number.
        "dust_weight_dropped_to_cash": round(dust_weight, 6),
        "dust_names_dropped": dust_names,
        "constraints_owner": _zb.CALCULATION_OWNER,
        "sector_cap_state": SECTOR_STATE,
    }


# --------------------------------------------------------------------------- #
# The ONE transition-cost calculation, shared by both tracks
# --------------------------------------------------------------------------- #
def traded_notional(current: Optional[dict], target: dict) -> float:
    """Total notional that changes hands, aligned BY SECURITY IDENTITY.

    ``sum |w_i,target - w_i,current|`` over the UNION of names, where ``i`` is a
    SYMBOL. Never an array position. Campaign v2's direct-portfolio learner
    compared consecutive weight vectors positionally whenever their lengths
    matched, which is only correct if the same row means the same company on both
    dates - and it does not. Membership changes, names delist, eligibility moves,
    and row order is an artifact of assembly. Positional comparison silently
    reports a portfolio that sold everything and bought everything else as having
    done nothing.

    Sells PLUS buys, because the canonical rate is quoted PER SIDE. With no prior
    book this is the initial purchase: a buy side and no sell side, so the notional
    is the invested weight itself rather than twice it.
    """
    tgt = {str(k): float(v) for k, v in (target or {}).items() if float(v) != 0.0}
    if current is None:
        return float(sum(abs(v) for v in tgt.values()))
    cur = {str(k): float(v) for k, v in current.items() if float(v) != 0.0}
    keys = set(cur) | set(tgt)
    return float(sum(abs(tgt.get(k, 0.0) - cur.get(k, 0.0)) for k in keys))


def transition_cost(current: Optional[dict], target: dict,
                    pol: Optional[dict] = None) -> float:
    """Canonical per-side cost on the symbol-aligned traded notional."""
    pol = pol or policy()
    return traded_notional(current, target) * float(pol["cost_rate_per_side"])


def realised_return(target: dict, returns_by_symbol: dict) -> Optional[float]:
    """Gross return of a portfolio over the holding window, aligned by SYMBOL.

    A name with no return over the window - delisted mid-hold, or absent from the
    panel - contributes nothing and its weight is reported as unrealised rather
    than being credited a silent zero. A zero is a claim that the position went
    nowhere; the honest statement is that the panel cannot say.
    """
    if not target:
        return 0.0
    tot = 0.0
    for sym, w in target.items():
        r = returns_by_symbol.get(str(sym))
        if r is not None and np.isfinite(r):
            tot += float(w) * float(r)
    return float(tot)


def unrealised_weight(target: dict, returns_by_symbol: dict) -> float:
    """Weight whose holding return the panel cannot represent."""
    if not target:
        return 0.0
    miss = 0.0
    for sym, w in target.items():
        r = returns_by_symbol.get(str(sym))
        if r is None or not np.isfinite(r):
            miss += float(w)
    return float(miss)
