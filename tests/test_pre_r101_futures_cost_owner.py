"""PRE-R101 - ONE research futures cost owner (D-PRE-R101-1).

The governed review of the R96 instrument-cost estimator (alpha_agent.r61.
instrument_costs) REJECTED it as a book cost: its R96 application is not
point-in-time (calendar-year medians that include days after the trade, today's
contract specification, a back-adjusted price as the denominator) and its volume
input is not the exchange volume. The research books keep
FUTURES_PER_MARKET_R38_PLUS_ROLL_V1. These tests keep that decision from being
reversed silently: by an import, by a renamed model, or by a book whose cost
stops being the per-market vector it is handed.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from alpha_agent.agents_v2 import books as B
from alpha_agent.r61 import instrument_costs as IC

ROOT = Path(__file__).resolve().parents[1]

# Every module that charges, budgets or re-derives a research futures cost.
_COST_CONSUMERS = (
    sorted((ROOT / "alpha_agent" / "agents_v2").glob("*.py"))
    + sorted((ROOT / "alpha_agent" / "r59").glob("*.py"))
    + [ROOT / "alpha_agent" / "r61" / "cost_budget.py",
       ROOT / "alpha_agent" / "r61" / "calibration_panels.py",
       ROOT / "research" / "agents" / "campaign_r56_v2" / "evaluator.py"])


def test_the_research_books_have_one_futures_cost_model():
    assert B.FUT_COST_MODEL == "FUTURES_PER_MARKET_R38_PLUS_ROLL_V1"


def test_the_r96_estimator_is_recorded_as_rejected_for_book_costs():
    assert IC.ADOPTION_STATUS == "REJECTED_AS_BOOK_COST"
    assert IC.ADOPTION_DECISION == "D-PRE-R101-1"
    decisions = (ROOT / "docs" / "ARCHITECTURE_DECISIONS.md").read_text(encoding="utf-8")
    assert "### D-PRE-R101-1" in decisions


def test_no_book_runner_or_cost_budget_reads_the_r96_estimator():
    offenders = [str(p.relative_to(ROOT)) for p in _COST_CONSUMERS
                 if p.exists() and "instrument_costs" in p.read_text(encoding="utf-8")]
    assert offenders == []


def _dates(n=140):
    d = np.arange("2011-06-01", "2012-06-01", dtype="datetime64[D]")
    return d[np.is_busday(d)][:n].astype(str)


def test_book_cost_is_exactly_the_per_market_vector_it_is_handed():
    # The re-costing diagnostic and every cost budget rely on this: rebalance
    # and roll cost are linear in layer["cost_per_side"], the gross is not
    # touched, and turnover does not depend on the rate.
    dates = _dates()
    n_d = len(dates)
    rng = np.random.default_rng(3)
    ret = rng.normal(0.0, 0.01, (3, n_d))
    roll = np.zeros((3, n_d), dtype=np.uint8)
    roll[0, 40] = roll[1, 75] = roll[2, 100] = 1
    ws = {30: np.array([0.5, -0.25, 0.25]), 51: np.array([0.25, 0.25, -0.5]),
          72: np.array([-0.5, 0.25, 0.25]), 93: np.array([0.5, -0.5, 0.0])}
    base = np.array([0.0002, 0.0005, 0.0010])

    def run(cost):
        layer = {"dates": dates, "ret": ret, "roll": roll, "cost_per_side": cost}
        return B.run_futures_book(layer, lambda t, live: ws[t], horizon=21,
                                  cadence=21, label="x", decision_idx=sorted(ws))

    a, b = run(base), run(base * np.array([3.0, 1.0, 0.5]))
    sa, sb = a["series"], b["series"]
    assert np.allclose(sa["gross"], sb["gross"])
    # hand-computed per-market attribution of the first rebalance (from flat)
    assert sa["rebalance_cost"][0] == pytest.approx(0.5 * 0.0002 + 0.25 * 0.0005 + 0.25 * 0.0010)
    assert sb["rebalance_cost"][0] == pytest.approx(0.5 * 0.0006 + 0.25 * 0.0005 + 0.25 * 0.0005)
    assert sa["roll_cost"].sum() > 0
    k = run(base * 4.0)["series"]
    assert np.allclose(np.asarray(k["rebalance_cost"]), 4.0 * np.asarray(sa["rebalance_cost"]))
    assert np.allclose(np.asarray(k["roll_cost"]), 4.0 * np.asarray(sa["roll_cost"]))
