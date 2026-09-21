r"""alpha_agent.r61.drawdown - THE drawdown owner (Workstream D).

RESEARCH ONLY. This module computes a risk statistic. It sizes nothing, holds
nothing and promotes nothing.

The two defects R60 exposed
---------------------------
The skeptic found "max drawdown 0.0000 on all three layers on the director
surface against non-zero strat_max_dd -0.1791 / -0.3973 / -0.2042 in the
brief", and said the right thing about it: *a 0.0000 max drawdown on a
long-only equity book is not attainable*. Two separate defects produce it, and
both are fixed here.

DEFECT 1 - THE ACCUMULATOR NEVER INCLUDED THE STARTING CAPITAL.
Both layer-statistics owners computed::

    nav  = cumprod(1 + r)
    peak = maximum.accumulate(nav)
    dd   = min(nav / peak - 1)

The first NAV point IS its own running peak, so the initial capital of 1.0
never enters the comparison and a loss in the FIRST period is invisible. A
stream of ``[-0.30, +0.05, +0.05, +0.05]`` reported a maximum drawdown of
exactly 0.0000 - it lost thirty percent and said it never lost anything.
Whenever the worst point relative to the prior peak fell at index 0, the
answer was zero. The NAV path here starts at 1.0, so the first period is
measured against the capital that was actually at risk.

DEFECT 2 - ONE KEY NAME MEANT TWO DIFFERENT THINGS, AND ONE CONCEPT DID NOT
EXIST AT ALL. The futures layer reported ``max_dd``; the equity layer reported
``strat_max_dd`` and ``bench_max_dd`` and NO excess drawdown whatsoever. A
consumer reading ``max_dd`` got a number from a futures book and nothing from
an equity book - and "nothing", rendered, is 0.0000. Worse, the two were not
the same concept: the futures book's ``max_dd`` is the drawdown of the EXCESS
series (its benchmark is cash), while ``strat_max_dd`` is the drawdown of the
strategy's own NAV. Reading one where the other was meant compares unlike
things.

TWO CONCEPTS, NOT ONE NUMBER
----------------------------
They are not reconciled into a single field, because they answer different
questions and a forced merge would lose one of them::

    strategy_max_drawdown   what the strategy's OWN capital experienced. This
                            is what an operator feels and what a capital
                            allocator rules on.
    excess_max_drawdown     the drawdown of the STRATEGY-MINUS-BENCHMARK
                            series. This is the drawdown of the thing the gate
                            actually measures, since the gate rules on
                            ``ann_net_excess``. A long-only book can be down
                            40% with its benchmark down 45% and have an
                            excellent excess path.
    benchmark_max_drawdown  carried so the other two can be read against each
                            other rather than in isolation.

For a book whose control is CASH - every dated-contract futures book here -
the benchmark series is identically zero, so the strategy and excess
drawdowns coincide. They are still reported as two fields, because they
coincide by a property of that book and not by definition.

ABSENCE IS NOT ZERO
-------------------
:func:`read` returns a typed result and never substitutes 0.0 for a field that
was never computed. ``MEASURED`` carries a number; ``NOT_MEASURED`` carries
None and the reason. A risk surface that cannot tell those apart is the
surface that reported 0.0000, and :func:`risk_view` refuses to present a
drawdown it does not have.

WHAT THIS DOES NOT DO
---------------------
It rewrites no persisted artifact. R60's JSON files and the memory rows keep
the numbers they were written with. No settled verdict moves either: no gate
in the governed agents_v2 path reads a drawdown field - not
``engines.gate``, not ``engines.stage_advance`` - which is exactly why the R60
director ruled the disagreement non-blocking at the lockbox stage and blocking
only at the risk agent, who was never reached. The correction can only ever
report a LARGER drawdown magnitude than the defective one, so every consumer
that does rule on drawdown becomes more conservative, never less.
"""
from __future__ import annotations

from typing import Optional, Sequence

import numpy as np

DRAWDOWN_OWNER = "alpha_agent.r61.drawdown"
DRAWDOWN_VERSION = "R61_CANONICAL_DRAWDOWN_V1"

#: The canonical concept names. Every surface reads one of THESE.
STRATEGY_MAX_DRAWDOWN = "strategy_max_drawdown"
BENCHMARK_MAX_DRAWDOWN = "benchmark_max_drawdown"
EXCESS_MAX_DRAWDOWN = "excess_max_drawdown"
CONCEPTS = (STRATEGY_MAX_DRAWDOWN, BENCHMARK_MAX_DRAWDOWN,
            EXCESS_MAX_DRAWDOWN)

#: The concept a RISK ruling is made on. The risk agent rules on what the
#: capital experienced, so it is the strategy's own NAV drawdown - not the
#: excess path, which can be calm while the capital is down 40%.
CANONICAL_STRATEGY_MAX_DRAWDOWN_OWNER = DRAWDOWN_OWNER
CANONICAL_EXCESS_DRAWDOWN_OWNER = DRAWDOWN_OWNER
RISK_RULING_CONCEPT = STRATEGY_MAX_DRAWDOWN

#: Legacy key -> canonical concept. These keys are still emitted, so nothing
#: that reads an R57/R58/R59/R60 artifact breaks; they are ALIASES now and the
#: canonical names are the ones a new consumer must use.
LEGACY_ALIASES = {
    "strat_max_dd": STRATEGY_MAX_DRAWDOWN,
    "bench_max_dd": BENCHMARK_MAX_DRAWDOWN,
    # The futures book's ``max_dd`` was always the EXCESS series (its
    # benchmark is cash). Naming it so is the whole repair.
    "max_dd": EXCESS_MAX_DRAWDOWN,
}

MEASURED = "MEASURED"
NOT_MEASURED = "NOT_MEASURED"


class DrawdownRefusal(RuntimeError):
    """A drawdown was asked for in a way that cannot be answered honestly."""


# --------------------------------------------------------------------------- #
# THE calculation. One implementation, in one place.
# --------------------------------------------------------------------------- #
def compounded_equity_curve(period_returns: Sequence[float]) -> np.ndarray:
    """The compounded equity curve, STARTING AT THE CAPITAL AT RISK.

    Not named ``nav_path``: ``def *nav*(`` is reserved by the estate's
    portfolio-NAV-valuation concept, and a research drawdown helper must
    not read as a writer of the operational book's NAV.

    The leading 1.0 is the whole correction. Without it the first period is
    compared against itself and a loss in period one is invisible.
    """
    r = np.asarray(list(period_returns), dtype=np.float64)
    if r.size == 0:
        return np.asarray([], dtype=np.float64)
    r = np.where(np.isfinite(r), r, 0.0)
    return np.concatenate([[1.0], np.cumprod(1.0 + r)])


def max_drawdown(period_returns: Sequence[float]) -> Optional[float]:
    """Worst peak-to-trough fraction of the compounded NAV. <= 0, or None.

    None means "there was no series", never "there was no drawdown". A stream
    that only ever rises returns 0.0, which is a measured zero and a different
    fact from an absent one.
    """
    nav = compounded_equity_curve(period_returns)
    if nav.size < 2:
        return None
    peak = np.maximum.accumulate(nav)
    return float(np.min(nav / peak - 1.0))


def layer_drawdowns(*, strategy_returns: Optional[Sequence[float]] = None,
                    benchmark_returns: Optional[Sequence[float]] = None
                    ) -> dict:
    """The canonical drawdown block for ONE layer of ONE book.

    ``benchmark_returns`` of None means the control is CASH, and the excess
    series is then the strategy series itself - stated, not assumed silently.
    """
    strat = (None if strategy_returns is None
             else np.asarray(list(strategy_returns), dtype=np.float64))
    if benchmark_returns is None:
        bench = None if strat is None else np.zeros_like(strat)
        control = "CASH"
    else:
        bench = np.asarray(list(benchmark_returns), dtype=np.float64)
        control = "BENCHMARK_SERIES"
    if strat is not None and bench is not None and strat.shape != bench.shape:
        raise DrawdownRefusal(
            "strategy series %s and benchmark series %s do not align; a "
            "drawdown of two different samples is not a drawdown"
            % (strat.shape, bench.shape))
    excess = None if (strat is None or bench is None) else (strat - bench)
    return {
        STRATEGY_MAX_DRAWDOWN: (None if strat is None
                                else max_drawdown(strat)),
        BENCHMARK_MAX_DRAWDOWN: (None if bench is None
                                 else max_drawdown(bench)),
        EXCESS_MAX_DRAWDOWN: (None if excess is None
                              else max_drawdown(excess)),
        "drawdown_owner": DRAWDOWN_OWNER,
        "drawdown_version": DRAWDOWN_VERSION,
        "drawdown_control": control,
    }


# --------------------------------------------------------------------------- #
# THE reader. Absence is reported, never defaulted.
# --------------------------------------------------------------------------- #
def read(stats: Optional[dict], concept: str = RISK_RULING_CONCEPT) -> dict:
    """Read ONE canonical drawdown concept out of a layer's statistics.

    Falls back to a LEGACY alias when the canonical key is absent, so an
    artifact written before this release is still readable - but never falls
    back to 0.0, and never silently reads a different concept's key. That
    substitution is the whole reason two surfaces disagreed.
    """
    if concept not in CONCEPTS:
        raise DrawdownRefusal(
            "%r is not a canonical drawdown concept; the concepts are %s"
            % (concept, list(CONCEPTS)))
    stats = stats or {}
    v = stats.get(concept)
    source = concept
    if v is None:
        for legacy, mapped in LEGACY_ALIASES.items():
            if mapped == concept and stats.get(legacy) is not None:
                v, source = stats.get(legacy), "LEGACY_ALIAS:%s" % legacy
                break
    try:
        f = None if v is None else float(v)
    except (TypeError, ValueError):
        f = None
    if f is None or f != f:
        return {"state": NOT_MEASURED, "concept": concept, "value": None,
                "source": None, "owner": DRAWDOWN_OWNER,
                "reason": ("%s was not measured on this layer. It is ABSENT, "
                           "which is not the same as zero; a surface that "
                           "renders it as 0.0000 is reporting a drawdown that "
                           "was never computed." % concept)}
    return {"state": MEASURED, "concept": concept, "value": f,
            "source": source, "owner": DRAWDOWN_OWNER, "reason": ""}


def risk_view(stats: Optional[dict]) -> dict:
    """The drawdown block handed to the risk agent. Every concept, typed.

    ``rulable`` says whether a drawdown ruling is even possible on this layer.
    The risk agent rules on drawdown; handing it a layer where the concept was
    never measured, and letting it read a rendered zero, is the defect this
    replaces.
    """
    out = {c: read(stats, c) for c in CONCEPTS}
    ruling = out[RISK_RULING_CONCEPT]
    return {
        "owner": DRAWDOWN_OWNER,
        "version": DRAWDOWN_VERSION,
        "ruling_concept": RISK_RULING_CONCEPT,
        "ruling_value": ruling["value"],
        "rulable": ruling["state"] == MEASURED,
        "not_rulable_reason": ("" if ruling["state"] == MEASURED
                               else ruling["reason"]),
        "concepts": out,
        "note": ("Two concepts, deliberately not merged. "
                 "%s is what the capital experienced; %s is the drawdown of "
                 "the series the statistical gate rules on. For a book whose "
                 "control is cash they coincide, by a property of that book "
                 "and not by definition."
                 % (STRATEGY_MAX_DRAWDOWN, EXCESS_MAX_DRAWDOWN)),
    }


def surfaces_agree(*stats_blocks, concept: str = RISK_RULING_CONCEPT,
                   tolerance: float = 1e-12) -> dict:
    """Do several surfaces report the SAME value for one concept?

    A surface that never measured the concept DISAGREES with one that did.
    Treating absence as agreement is how 0.0000 stood next to -0.1791 for a
    whole campaign without anything failing.
    """
    reads = [read(s, concept) for s in stats_blocks]
    values = [r["value"] for r in reads]
    measured = [v for v in values if v is not None]
    agree = (len(measured) == len(values) and len(values) > 0
             and max(measured) - min(measured) <= float(tolerance))
    return {"concept": concept, "agree": bool(agree), "reads": reads,
            "values": values,
            "reason": ("" if agree else
                       "surfaces disagree or a surface did not measure the "
                       "concept at all")}
