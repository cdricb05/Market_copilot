r"""alpha_agent.agents_v2.books - the LIVE book owner of the agent campaigns.

RESEARCH ONLY. PAPER ONLY. NO ORDERS, NO FILLS, NO PROMOTION. Everything here
simulates books on owned historical data and returns numbers.

Why this module exists, and what it deliberately does NOT do
------------------------------------------------------------
The book accounting the agent campaigns need already exists and is already
parity-tested against its owners: ``research/agents/campaign_r56_v2/
evaluator.py`` is FROZEN EVIDENCE of a settled campaign (its bytes are pinned
by ``.gitattributes`` and by ``tests/test_release57_research_engine_repair.py``).
This module therefore LOADS that evaluator and re-exports it. It does not
reimplement a single book. A second implementation of ``run_futures_book``
would be a second accounting, and two accountings of the same book is how an
estate ends up with two answers to one question.

What this module ADDS is the thing no owner had, and the thing R56 proved was
missing: **the layers are measured one at a time, in order.**

    stage_panel / stage_decision_idx   the exact prefix of the decision grid
                                       that resolves layer S and nothing after
    run_stage                          measure ONE layer
    placebo_cost_neutral               a permuted-signal book re-simulated at
                                       the CANDIDATE'S cost drag
    doubled_cost / subperiods          the deterministic adversarial attacks

THE PREFIX IS EXACT, NOT APPROXIMATE. Truncating the session axis to
``boundary + horizon + 1`` sessions leaves precisely the decisions of layers up
to S, each with its full forward window, and admits no decision of the next
layer - because ``decision_indices`` stops at ``len(dates) - horizon - 2``.
Running the prefix therefore reproduces layer S's statistics EXACTLY as a full
run would, including the turnover carried in from the previous layer and the
embargo purge at the boundary, while never computing a return the estate has
not yet earned the right to see.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Callable, Optional

import numpy as np

from .. import r59
from ..r57 import engine as K
from ..r59 import engines as E
from ..r59 import native

CALCULATION_OWNER = "alpha_agent.agents_v2.books"

#: The frozen evidence this module re-exports. Never edited; loaded by path so
#: there is exactly ONE implementation of every book in the estate.
FROZEN_EVALUATOR = (Path(__file__).resolve().parents[2] / "research" /
                    "agents" / "campaign_r56_v2" / "evaluator.py")


def _load_frozen():
    name = "alpha_agent_agents_v2_frozen_evaluator"
    mod = sys.modules.get(name)
    if mod is not None:
        return mod
    if not FROZEN_EVALUATOR.exists():
        raise FileNotFoundError(
            "the frozen campaign evaluator is missing: %s" % FROZEN_EVALUATOR)
    spec = importlib.util.spec_from_file_location(name, FROZEN_EVALUATOR)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


EV = _load_frozen()

# Re-exported, NOT reimplemented.
run_futures_book = EV.run_futures_book
run_equity_topn = EV.run_equity_topn
run_equity_daily_tranche_book = EV.run_equity_daily_tranche_book
rank_weights = EV.rank_weights
live_markets = EV.live_markets
equity_topn_daily = EV.equity_topn_daily
FUT_COST_MODEL = EV.FUT_COST_MODEL
EQ_COST_MODEL = EV.EQ_COST_MODEL
EQ_EXT_COST_RATE = EV.EQ_EXT_COST_RATE

#: Books this module knows how to stage.
BOOK_FUTURES = "FUTURES_DATED_CONTRACT"
BOOK_EQUITY_TOPN = "EQUITY_TOPN"
BOOK_EQUITY_TRANCHE = "EQUITY_DAILY_TRANCHE"
BOOKS = (BOOK_FUTURES, BOOK_EQUITY_TOPN, BOOK_EQUITY_TRANCHE)

#: Panel arrays sliced on the session axis when a stage prefix is taken.
#: A 2-D array is sliced on its second axis; a 1-D array is sliced only when
#: its length IS the session count, so a per-name vector (``sectors``,
#: ``symbols``, ``cost_per_side``) is left alone.
_PANEL_SESSION_KEYS = ("tr", "op_tr", "px", "op", "adv", "vol", "mcap", "un",
                       "dv", "mem", "sp500_mem", "spy_tr",
                       "ret", "ret2", "slope", "open_interest", "volume",
                       "roll", "eligible", "sector")


class BookRefusal(RuntimeError):
    """A book could not be measured as asked."""


# --------------------------------------------------------------------------- #
# The stage prefix
# --------------------------------------------------------------------------- #
def stage_boundary(dates: np.ndarray, stage: str) -> int:
    """Index of the FIRST date that belongs to the layer AFTER ``stage``."""
    if stage not in r59.STAGES:
        raise BookRefusal("unknown stage %r" % (stage,))
    if stage == "D":
        return int(np.searchsorted(dates, r59.VALIDATION_START))
    if stage == "V":
        return int(np.searchsorted(dates, r59.LOCKBOX_START))
    return int(len(dates))


def stage_sessions(dates: np.ndarray, stage: str, horizon: int) -> int:
    """How many sessions of the panel layer ``stage`` is entitled to see.

    ``boundary + horizon + 1``. One more session and ``decision_indices``
    (which stops at ``len - horizon - 2``) would admit the first decision of
    the NEXT layer; one fewer and the last decision of THIS layer would lose
    the tail of its forward window.
    """
    n = int(len(dates))
    if stage == "L":
        return n
    return min(n, stage_boundary(dates, stage) + int(horizon) + 1)


def truncate_panel(panel: dict, n_sessions: int) -> dict:
    """The same panel, seeing only its first ``n_sessions`` sessions."""
    n = int(n_sessions)
    out = dict(panel)
    out["dates"] = np.asarray(panel["dates"])[:n]
    for k in _PANEL_SESSION_KEYS:
        v = panel.get(k)
        if isinstance(v, np.ndarray) and v.ndim == 2:
            out[k] = v[:, :n]
        elif isinstance(v, np.ndarray) and v.ndim == 1 and len(v) == len(
                panel["dates"]):
            out[k] = v[:n]
    return out


def stage_decision_idx(dates: np.ndarray, stage: str, *, cadence: int,
                       horizon: int) -> np.ndarray:
    """Decision indices of every layer UP TO AND INCLUDING ``stage``.

    Used for the futures book, whose panel is a market-by-session block that is
    cheaper to keep whole and restrict by decision than to slice.
    """
    idx = native.decision_indices(dates, cadence, horizon)
    lab = K.layer_of(dates, idx, cadence, horizon)
    keep = set(r59.STAGES[:r59.STAGES.index(stage) + 1])
    return idx[np.array([bool(x) and x in keep for x in lab])]


# --------------------------------------------------------------------------- #
# Measuring ONE layer
# --------------------------------------------------------------------------- #
#: What the second callable MEANS, per book. The dated-contract book is given
#: a WEIGHTS function because its constructions are not all rank books (a
#: risk-parity leg split is a weighting rule, not a score); the two equity
#: books are given a SCORE function and do their own weighting.
CALLABLE_CONTRACT = {
    BOOK_FUTURES: "weights_fn(t, live) -> float[n_markets], masked and finite",
    BOOK_EQUITY_TOPN: "score_fn(panel, t) -> float[n_names], NaN = unscorable",
    BOOK_EQUITY_TRANCHE: "score_fn(panel, t) -> float[n_names]",
}


def run_stage(*, book: str, stage: str, panel: dict, score_fn: Callable,
              cadence: int = r59.CADENCE, horizon: int = r59.HORIZON,
              label: str = "", cost_mult: float = 1.0,
              elig=None, top_n: int = 100,
              cost_rate: Optional[float] = None,
              charge_rolls: bool = True,
              closed_rule: str = "DROP",
              min_markets: int = 12,
              first_date: str = r59.DISCOVERY_START,
              hold: Optional[int] = None) -> dict:
    """Measure exactly ONE layer of one book, on the prefix that resolves it.

    Returns ``{"stats": <layer stats>, "result": <raw book result>}``. The raw
    result is kept for the deterministic attacks (placebo, doubled cost,
    subperiod, duplicate identity); it contains the prefix ONLY, so nothing
    downstream of ``stage`` exists to be inspected.
    """
    if book not in BOOKS:
        raise BookRefusal("unknown book %r" % (book,))
    if stage not in r59.STAGES:
        raise BookRefusal("unknown stage %r" % (stage,))
    label = label or ("%s:%s" % (book, stage))

    if book == BOOK_FUTURES:
        dates = np.asarray(panel["dates"])
        idx = stage_decision_idx(dates, stage, cadence=cadence,
                                 horizon=horizon)
        if not len(idx):
            raise BookRefusal("no decisions in layer %s" % stage)
        res = run_futures_book(
            panel, score_fn, horizon=horizon, cadence=cadence, label=label,
            cost_mult=cost_mult, charge_rolls=charge_rolls,
            closed_rule=closed_rule, decision_idx=idx)
    elif book == BOOK_EQUITY_TOPN:
        n = stage_sessions(np.asarray(panel["dates"]), stage, horizon)
        res = run_equity_topn(
            truncate_panel(panel, n), score_fn, elig, label=label,
            cadence=cadence, horizon=horizon, top_n=top_n,
            cost_rate=(EQ_EXT_COST_RATE if cost_rate is None else cost_rate),
            cost_mult=cost_mult, first_date=first_date)
    else:
        n = stage_sessions(np.asarray(panel["dates"]), stage,
                           int(hold or 5) + 1)
        res = run_equity_daily_tranche_book(
            truncate_panel(panel, n), score_fn, elig, label=label,
            hold=int(hold or 5), top_n=top_n,
            cost_rate=(EQ_EXT_COST_RATE if cost_rate is None else cost_rate),
            cost_mult=cost_mult, first_date=first_date)
    # Every frozen book reports ``layers`` as {D/V/L: stats}, computed by the
    # canonical statistics owner. The layer is READ from the book, never
    # recomputed here: one book, one accounting.
    stats = (res.get("layers") or {}).get(stage)
    if not stats:
        raise BookRefusal("%s reported no layer %s" % (book, stage))
    return {"stats": stats, "result": res}


# --------------------------------------------------------------------------- #
# Placebo - COST-NEUTRAL by construction
# --------------------------------------------------------------------------- #
def permuted_score_fn(score_fn: Callable, *, seed: int,
                      axis_len: Optional[int] = None) -> Callable:
    """``score_fn`` with its CROSS-SECTION shuffled at every decision date.

    The panel, the eligibility mask, the calendar, the cost vector and the
    gross exposure are untouched; only the assignment of a score to an
    instrument is destroyed. That is the null this attack needs: "the ranking
    carries no information", not "the book does not exist".
    """
    rng = np.random.default_rng(int(seed))

    def _fn(*args, **kwargs):
        s = np.asarray(score_fn(*args, **kwargs), dtype=np.float64)
        out = s.copy()
        fin = np.where(np.isfinite(s))[0]
        if len(fin) > 1:
            out[fin] = s[rng.permutation(fin)]
        return out

    return _fn


def placebo_cost_neutral(*, candidate_stats: dict, run_placebo: Callable,
                         seeds=(101, 202, 303),
                         tolerance: float = E.PLACEBO_COST_TOLERANCE,
                         max_rescale_passes: int = 6) -> dict:
    """Run the placebo, then re-run it at the CANDIDATE'S cost drag.

    ``run_placebo(seed, cost_mult) -> layer stats``.

    A permuted signal churns, so its natural cost drag is larger than the
    candidate's and a NET comparison would credit the candidate for trading
    less rather than for predicting better. The placebo's cost multiplier is
    therefore solved so that its realised drag MATCHES the candidate's; only
    then is the margin applied (``alpha_agent.r59.engines.placebo_verdict``).

    The solve is a fixed point on a strictly proportional quantity (drag is
    linear in the cost rate at a fixed turnover path), so it converges in one
    pass; the loop exists to prove convergence rather than to hunt for it.
    """
    target = E.cost_drag(candidate_stats)
    rows = []
    for seed in seeds:
        raw = run_placebo(int(seed), 1.0)
        raw_drag = E.cost_drag(raw)
        mult, stats, drag = 1.0, raw, raw_drag
        for _ in range(int(max_rescale_passes)):
            if abs(drag - target) <= tolerance:
                break
            if abs(drag) < 1e-12:
                raise BookRefusal(
                    "placebo pays no cost, so it cannot be matched to the "
                    "candidate's drag of %.6g" % target)
            mult = mult * (target / drag)
            stats = run_placebo(int(seed), float(mult))
            drag = E.cost_drag(stats)
        if abs(drag - target) > tolerance:
            raise BookRefusal(
                "placebo cost could not be matched: target %.6g, reached "
                "%.6g after %d passes" % (target, drag, max_rescale_passes))
        rows.append({"seed": int(seed), "cost_mult": float(mult),
                     "raw_cost_drag": float(raw_drag),
                     "matched_cost_drag": float(drag), "stats": stats})
    # The attack is answered by the STRONGEST placebo, never the average one.
    metrics = [k for k in r59.GATE_MATERIALITY_FLOORS if k in candidate_stats]
    key = metrics[0] if metrics else None
    worst = max(rows, key=lambda r: (E._as_float(r["stats"].get(key))
                                     if key else 0.0) or -1e18)
    return {"placebo_runs": rows, "strongest_seed": worst["seed"],
            "strongest": worst["stats"],
            "candidate_cost_drag": float(target),
            "cost_basis": "COST_NEUTRAL"}


# --------------------------------------------------------------------------- #
# The other deterministic attacks
# --------------------------------------------------------------------------- #
def subperiod_split(stats_fn: Callable, layer_result: dict, book: str,
                    stage: str = "L") -> dict:
    """Is the layer's effect produced by one half of itself?

    Reported, never graded here: the skeptic owns the verdict. What this
    guarantees is that the number it grades was MEASURED and not asserted.
    """
    row = stats_fn(layer_result, stage) if callable(stats_fn) else {}
    halves = row.get("halves_ann_net_excess") or row.get("halves")
    return {"halves": halves,
            "both_positive": bool(halves and all(h > 0 for h in halves)),
            "measured": (min(halves) if halves else None)}


def rank_fingerprint(per_decision: list) -> str:
    """Delegated to the canonical owner - the duplicate-identity attack must
    use the SAME fingerprint the estate already settled 8,000 hypotheses on."""
    return E._rank_fingerprint(per_decision)
