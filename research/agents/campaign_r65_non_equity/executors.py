r"""R65 executors - the two pre-registered open-interest-growth cells.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION, NO ADOPTION.

What these cells are
--------------------
R60 registered this mechanism and then REJECTED both cells before a single
return was computed, because ``GATE_MAX_TURNOVER = 0.40`` - one one-way
turnover scalar - was applied to a futures book that does not pay an equity
book's costs. R61 measured what those halts actually cost: the commodity cell's
0.598 one-way turnover at 5 bp/side and 12 rebalances a year is a 0.72%/yr
drag, and the equity-index cell's 0.612 is 0.73%/yr. Both were killed by a
21.9%/yr equity constraint imported into a world that never pays it.

So the MECHANISM WAS NEVER MEASURED. These two cells measure it, under the
corrected annualised cost budget, at a new experiment id, charged to the
burden. The R60 halts stand as they fired and are not re-run.

The construction, frozen by the director on 2026-09-22
------------------------------------------------------
``log(mean OI over 21 own sessions / mean OI over 63 own sessions)``, both
windows closing at ``t-2``, then DEMEAN WITHIN ECONOMIC GROUP FIRST and RANK
THE DEMEANED RATIOS ACROSS THE WHOLE BOOK. ``expected_sign = -1`` is frozen:
crowding predicts LOWER returns, so the book SHORTS the fastest open-interest
expansion. A positive net excess is a FAILURE, never a re-read.

r65_01 (international equity index) carries ``group_demeaning = False``: its
universe is a single economic family and there is nothing to demean against.
r65_03 (commodity) demeans within ``economic_group``, and a group with fewer
than two scorable markets contributes nothing - which is why INDUSTRIAL_METALS
(HG alone in the published universe) is expected to sit out. That is the frozen
rule operating as designed, not an exclusion chosen after the fact.

THE PRE-MEASUREMENT GATES
-------------------------
Two things can stop a cell here, and both are computed WITHOUT SCORING A SINGLE
RETURN, so neither consumes a statistical layer:

    open-interest coverage   below 0.80 usable non-zero coverage over
                             decision-eligible market-sessions -> DATA_HOLD
    annualised cost budget   above 0.03/yr of modelled drag -> REJECTED

Both are settled through ``AgentPipeline.record_pre_measurement_halt``, which
cannot write NO_ALPHA_EVIDENCE: a cell that computed no return has said nothing
about alpha, and filing silence as evidence of absence is the exact
mislabelling R60's director refused.
"""
from __future__ import annotations

import numpy as np

from alpha_agent import r59
from alpha_agent.agents_v2 import books as B
from alpha_agent.r59 import native
from alpha_agent.r61 import cost_budget as CB

from . import panels as PN

# --------------------------------------------------------------------------- #
# Frozen constants
# --------------------------------------------------------------------------- #
#: Minimum scorable markets before the book goes flat. These are the values
#: R60 froze for THESE TWO PANELS before any return in either campaign was
#: computed (executors.MIN_MARKETS / MIN_INTL_MARKETS). They are carried
#: forward unchanged and on purpose: choosing a new minimum for R65 would be a
#: parameter selected in sight of a universe, and the universes only got
#: tighter. 14 international markets are published and 8 are required; 38
#: commodity markets are published and 12 are required.
MIN_MARKETS = 12
MIN_INTL_MARKETS = 8
MIN_MARKETS_PROVENANCE = (
    "R60 executors.py, frozen 2026-09-20 before any R60 or R65 return was "
    "computed; carried forward unchanged")

#: Window rules, as pre-registered.
FAST_WINDOW = 21
SLOW_WINDOW = 63
FAST_MIN_OBS = 15
SLOW_MIN_OBS = 40

#: The cell-level coverage floor the director froze on 2026-09-22.
OI_COVERAGE_FLOOR = 0.80

#: The corrected budget. NOT the retired 0.40 turnover scalar.
COST_BUDGET_CEILING = CB.COST_BUDGET_CEILING

_CACHE: dict = {}


class CoverageFloorFailed(RuntimeError):
    """Usable open-interest coverage was below the pre-registered floor. This
    is a RESULT (DATA_HOLD), not a failure to be worked around."""

    halt_reason = "DATA_COVERAGE_FLOOR"


class CostBudgetFailed(RuntimeError):
    """The frozen annualised cost budget refused the construction before any
    return was scored. A RESULT (REJECTED), and the cell is NEVER re-smoothed,
    re-banded or re-tranched into compliance - a retune is a new experiment and
    a new burden charge."""

    halt_reason = "COST_BUDGET_EXCEEDED"


# --------------------------------------------------------------------------- #
# Own-session windows - imported, not reimplemented
# --------------------------------------------------------------------------- #
from campaign_r60_information_frontier.executors import (  # noqa: E402
    _demean, _own_cumsums, _own_mean)

# ``_base_live`` is NOT imported: R60's version screens on
# ``layer['certified']`` plus a 227-of-252 own-session liveness rule. R65 keeps
# the liveness rule (a market that barely traded cannot carry a 63-session
# window) but takes its universe from the PUBLISHED artifact instead of an
# asset-class predicate, so the two are composed explicitly below.
BASE_LIVE_WINDOW = 252
BASE_LIVE_MIN_OWN = 227


def _live(layer: dict, t: int, live: np.ndarray) -> np.ndarray:
    m = np.asarray(live, bool) & np.asarray(layer["certified"], bool)
    own = layer["own"]
    lo, hi = max(0, int(t) - BASE_LIVE_WINDOW), max(0, int(t))
    return m & (own[:, lo:hi].sum(axis=1) >= BASE_LIVE_MIN_OWN)


# --------------------------------------------------------------------------- #
# The construction
# --------------------------------------------------------------------------- #
def oi_growth_plan(universe_id: str, *, demean_key, min_markets: int) -> dict:
    """log(mean OI over 21 own sessions / mean OI over 63), both closing t-2.

    ``panels.futures_layer`` has already turned every ``open_interest == 0``
    into NaN, so a zero can neither enter a mean nor become a denominator.
    The windows require 15 and 40 positive observations respectively; a market
    that cannot supply them is unscorable on that decision.
    """
    layer = PN.futures_layer()
    oi = layer["open_interest"]
    key = ("plan", universe_id)
    if key not in _CACHE:
        have = np.isfinite(oi) & (oi > 0)
        _CACHE[key] = _own_cumsums(oi, have)
    cv, cn = _CACHE[key]
    rows = PN.universe_rows(layer, universe_id)
    groups = layer[demean_key] if demean_key else None

    def weights_fn(t, live):
        end = max(0, int(t) - PN.OI_LAG) + 1             # closes at t-2
        m_fast = _own_mean(cv, cn, end, FAST_WINDOW, FAST_MIN_OBS)
        m_slow = _own_mean(cv, cn, end, SLOW_WINDOW, SLOW_MIN_OBS)
        with np.errstate(invalid="ignore", divide="ignore"):
            feat = np.log(np.where((m_fast > 0) & (m_slow > 0),
                                   m_fast / m_slow, np.nan))
        elig = _live(layer, int(t), live) & rows & np.isfinite(feat)
        if int(elig.sum()) < min_markets:
            return np.zeros(oi.shape[0])
        # (B)-(D): demean within group FIRST, then rank the demeaned ratios
        # across the whole book.
        dm = _demean(feat, groups, elig) if groups is not None else feat
        scored = elig & np.isfinite(dm)
        if int(scored.sum()) < min_markets:
            return np.zeros(oi.shape[0])
        # expected_sign = -1: SHORT the crowded, LONG the contracting.
        return B.rank_weights(-dm, scored, min_markets=min_markets)

    return {"book": B.BOOK_FUTURES, "panel": layer, "score_fn": weights_fn,
            "signal_sign": -1, "universe_id": universe_id,
            "min_markets": int(min_markets),
            "demean_key": demean_key,
            "rows": rows}


# --------------------------------------------------------------------------- #
# Pre-measurement gate 1: open-interest coverage
# --------------------------------------------------------------------------- #
def coverage_precheck(plan: dict) -> dict:
    """The cell-level 0.80 floor. The per-market universe screen does NOT
    discharge it: a book of individually-adequate markets can still be too
    sparse in aggregate."""
    cov = PN.oi_coverage(plan["panel"], plan["rows"])
    cov["floor"] = OI_COVERAGE_FLOOR
    cov["passed"] = bool(np.isfinite(cov["coverage"])
                         and cov["coverage"] >= OI_COVERAGE_FLOOR)
    if not cov["passed"]:
        raise CoverageFloorFailed(
            "OI_COVERAGE_FLOOR: %s measured usable non-zero open-interest "
            "coverage %.4f over %d decision-eligible market-sessions against "
            "the pre-registered floor %.2f."
            % (plan["universe_id"], cov["coverage"],
               cov["eligible_market_sessions"], OI_COVERAGE_FLOOR))
    return cov


# --------------------------------------------------------------------------- #
# Pre-measurement gate 2: the annualised cost budget
# --------------------------------------------------------------------------- #
def weights_path(plan: dict) -> dict:
    """Replay the book's WEIGHTS over every decision, scoring NO RETURN.

    Turnover, traded notional and roll exposure are properties of the weights
    path alone. Computing them costs no statistical budget and reveals no
    layer, which is precisely why the budget can be enforced BEFORE the
    experiment is measured.

    The roll arithmetic here mirrors ``run_futures_book``'s roll charge on the
    same inputs (roll flags, weights, per-market rates). The BOOK remains the
    sole authority on the cost actually charged in a measured result; this is a
    cost-only forecast used to decide whether the cell may be measured at all,
    and :func:`cost_parity` checks the two against each other afterwards so the
    duplication cannot drift in silence.
    """
    layer = plan["panel"]
    dates = np.asarray(layer["dates"])
    ret, roll = layer["ret"], layer["roll"]
    costs = np.asarray(layer["cost_per_side"], dtype=np.float64)
    n_m, n_d = ret.shape
    horizon, cadence = r59.HORIZON, r59.CADENCE

    idx = native.decision_indices(dates, cadence, horizon)
    idx = idx[idx >= int(np.searchsorted(dates, r59.DISCOVERY_START))]
    fn = plan["score_fn"]

    turns, roll_costs, rebal_costs = [], [], []
    traded = np.zeros(n_m, dtype=np.float64)
    n_positions = []
    prev = np.zeros(n_m)
    for t in idx:
        t = int(t)
        live = B.live_markets(layer, t, closed_rule="DROP")
        w = np.asarray(fn(t, live), dtype=np.float64)
        w = np.where(np.isfinite(w) & live, w, 0.0)
        d = np.abs(w - prev)
        traded += d
        turns.append(float(d.sum()) / 2.0)
        rebal_costs.append(float(d @ costs))
        n_positions.append(int((w != 0.0).sum()))

        lo, hi = min(t + 2, n_d), min(t + horizon + 1, n_d)
        interior = roll[:, lo:hi].astype(np.float64)
        at_entry = (roll[:, t + 1].astype(np.float64) if t + 1 < n_d
                    else np.zeros(n_m))
        same = (np.sign(w) == np.sign(prev)) & (w != 0.0)
        extra = np.where(same, 2.0 * np.minimum(np.abs(w), np.abs(prev)), 0.0)
        entry_cost = float((at_entry * extra) @ costs)
        per_day = (interior * (2.0 * np.abs(w) * costs)[:, None]).sum(axis=0)
        roll_costs.append(entry_cost + float(per_day.sum()))
        prev = w

    ppy = 252.0 / float(horizon)
    return {
        "n_decisions": int(len(idx)),
        "median_oneway_turnover": (float(np.median(turns)) if turns
                                   else float("nan")),
        "mean_oneway_turnover": (float(np.mean(turns)) if turns
                                 else float("nan")),
        "traded_notional_per_market": traded,
        "effective_cost_per_side": CB.effective_cost_per_side(traded, costs),
        "ann_roll_cost_drag": (float(np.mean(roll_costs)) * ppy
                               if roll_costs else float("nan")),
        "ann_rebalance_cost_drag_forecast": (float(np.mean(rebal_costs)) * ppy
                                             if rebal_costs else float("nan")),
        "median_positions": (float(np.median(n_positions)) if n_positions
                             else 0.0),
        "flat_decisions": int(sum(1 for p in n_positions if p == 0)),
        "periods_per_year": ppy,
        "returns_scored": 0,
    }


def cost_precheck(plan: dict, *, label: str) -> dict:
    """Apply the CORRECTED annualised cost budget before any return exists."""
    path = weights_path(plan)
    verdict = CB.evaluate_cost_budget(
        one_way_turnover=path["median_oneway_turnover"],
        cost_per_side=path["effective_cost_per_side"],
        rebalance_interval_sessions=r59.CADENCE,
        additional_ann_cost_drag=path["ann_roll_cost_drag"],
        label=label)
    out = {k: v for k, v in path.items()
           if k != "traded_notional_per_market"}
    out["verdict"] = verdict
    out["retired_turnover_ceiling"] = CB.RETIRED_TURNOVER_CEILING
    out["would_the_retired_scalar_have_killed_it"] = bool(
        np.isfinite(path["median_oneway_turnover"])
        and path["median_oneway_turnover"] > CB.RETIRED_TURNOVER_CEILING)
    state = verdict.get("state")
    if state != CB.BUDGET_PASS:
        raise CostBudgetFailed(
            "%s: %s. annualized_cost_drag %s against the frozen ceiling %.4f "
            "(one-way turnover %s, effective %s/side, roll %s/yr)."
            % (label, state, verdict.get("annualized_cost_drag"),
               COST_BUDGET_CEILING, path["median_oneway_turnover"],
               path["effective_cost_per_side"], path["ann_roll_cost_drag"]))
    return out


def cost_parity(precheck: dict, layers: dict, *, tol: float = 0.15) -> dict:
    """Did the BOOK charge what the precheck forecast?

    The precheck computes roll and rebalance drag on the weights path; the book
    computes them again while scoring returns. They read the same inputs and
    must agree. A material disagreement means the duplication drifted, and it
    is surfaced rather than absorbed.
    """
    out = {"tolerance_relative": tol, "checks": []}
    ok = True
    for metric, forecast_key in (("ann_roll_cost_drag", "ann_roll_cost_drag"),
                                 ("ann_rebalance_cost_drag",
                                  "ann_rebalance_cost_drag_forecast")):
        book_vals = [layers[s].get(metric) for s in r59.STAGES
                     if s in layers and layers[s].get(metric) is not None]
        if not book_vals:
            continue
        book = float(np.mean([float(v) for v in book_vals]))
        fore = float(precheck.get(forecast_key, float("nan")))
        denom = max(abs(book), abs(fore), 1e-9)
        rel = abs(book - fore) / denom
        passed = bool(np.isfinite(rel) and rel <= tol)
        ok = ok and passed
        out["checks"].append({"metric": metric, "book_mean_over_layers": book,
                              "precheck_forecast": fore,
                              "relative_difference": rel, "passed": passed})
    out["passed"] = ok
    return out


# --------------------------------------------------------------------------- #
# The two registered cells
# --------------------------------------------------------------------------- #
def _built(name: str, universe_id: str, *, demean_key,
           min_markets: int) -> dict:
    """Build a cell's plan and run both pre-measurement gates, ONCE.

    The campaign driver calls the executor to find out whether the cell may be
    measured at all, and the runner then calls it again to measure it. The
    weights-path replay behind the cost budget is the expensive part of that,
    so the built plan is cached by executor name. The cache holds a plan, never
    a result.
    """
    key = ("built", name)
    if key in _CACHE:
        return _CACHE[key]
    plan = oi_growth_plan(universe_id, demean_key=demean_key,
                          min_markets=min_markets)
    plan["coverage"] = coverage_precheck(plan)
    plan["cost_precheck"] = cost_precheck(plan, label=name)
    _CACHE[key] = plan
    return plan


def r65_01(row):
    """FUT_INTL_INDEX_OPEN_INTEREST_GROWTH_COST_HONEST.

    14 international equity-index futures, no group demeaning (the universe is
    one economic family), flat unless 8 markets score.
    """
    return _built("r65_01", "r65_intl_index_oi_covered", demean_key=None,
                  min_markets=MIN_INTL_MARKETS)


def r65_03(row):
    """FUT_COMMODITY_OPEN_INTEREST_GROWTH_COST_HONEST.

    38 commodity futures, demeaned within R38 economic group, flat unless 12
    markets score.
    """
    return _built("r65_03", "r65_commodity_oi_covered",
                  demean_key="economic_group", min_markets=MIN_MARKETS)


EXECUTORS = {"r65_01": r65_01, "r65_03": r65_03}
