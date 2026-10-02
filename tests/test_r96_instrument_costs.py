"""R96 B1 — deterministic tests for the per-instrument research cost estimators.

Hermetic: synthetic prices only, no owned store is opened.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

import numpy as np
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from paper_trader.alpha_agent.r61 import instrument_costs as IC  # noqa: E402


def _simulate(spread: float, days: int = 400, ticks_per_day: int = 390, seed: int = 7):
    """Efficient log price is a random walk; every trade prints at mid +/- spread/2."""
    rng = np.random.default_rng(seed)
    sigma_tick = 0.012 / math.sqrt(ticks_per_day)
    m = 100.0
    H, L, C = [], [], []
    for _ in range(days):
        logs = np.log(m) + np.cumsum(rng.normal(0.0, sigma_tick, ticks_per_day))
        mid = np.exp(logs)
        side = rng.choice([-1.0, 1.0], ticks_per_day)
        trade = mid * (1.0 + side * spread / 2.0)
        H.append(trade.max())
        L.append(trade.min())
        C.append(trade[-1])
        m = mid[-1]
    return np.array(H), np.array(L), np.array(C)


def test_abdi_ranaldo_recovers_a_known_spread():
    h, lo, c = _simulate(0.004)
    est = IC.abdi_ranaldo(h, lo, c)
    assert est is not None
    assert 0.002 < est < 0.006


def test_corwin_schultz_is_nonnegative_and_orders_spreads():
    s_small = np.nanmean(IC.corwin_schultz(*_simulate(0.001, seed=3)))
    s_large = np.nanmean(IC.corwin_schultz(*_simulate(0.010, seed=3)))
    assert s_small >= 0.0 and s_large >= 0.0
    assert s_large > s_small


def test_estimators_are_deterministic():
    h, lo, c = _simulate(0.003, seed=11)
    assert IC.abdi_ranaldo(h, lo, c) == IC.abdi_ranaldo(h, lo, c)
    a, b = IC.corwin_schultz(h, lo, c), IC.corwin_schultz(h, lo, c)
    assert np.array_equal(np.nan_to_num(a), np.nan_to_num(b))


def test_abdi_ranaldo_refuses_a_tiny_window():
    assert IC.abdi_ranaldo([1, 1, 1], [1, 1, 1], [1, 1, 1]) is None


def test_tick_floor_and_sqrt_impact():
    assert IC.tick_floor_half_spread(0.25, 5000.0) == pytest.approx(0.25e-4)
    assert IC.tick_floor_half_spread(0.0, 5000.0) is None
    imp = IC.sqrt_impact(0.01, 1e6, 1e8, 0.7)
    assert imp == pytest.approx(0.7 * 0.01 * 0.1)
    assert IC.sqrt_impact(0.01, 1e6, 0.0, 0.7) is None


def test_evidence_classes_are_enforced_and_never_promoted():
    with pytest.raises(ValueError):
        IC.component(0.001, "MEASURED", "x")
    comps = {"half_spread": IC.component(0.0004, IC.ESTIMATED, "abdi_ranaldo"),
             "commission": IC.component(0.0001, IC.ASSUMED, "schedule"),
             "tick": IC.component(0.00002, IC.OBSERVED, "spec")}
    tot = IC.one_way_cost(comps)
    assert tot["state"] == "COMPLETE"
    assert tot["total"] == pytest.approx(0.00052)
    assert tot["by_evidence"][IC.ESTIMATED] == pytest.approx(0.0004)
    assert abs(sum(tot["share_by_evidence"].values()) - 1.0) < 1e-12


def test_a_missing_component_is_unknown_not_zero():
    comps = {"half_spread": IC.component(None, IC.ESTIMATED, "abdi_ranaldo"),
             "commission": IC.component(0.0001, IC.ASSUMED, "schedule")}
    tot = IC.one_way_cost(comps)
    assert tot["total"] is None
    assert tot["state"] == "UNKNOWN_COMPONENT_MISSING"
    assert tot["missing"] == ["half_spread"]


def test_the_module_writes_nothing_and_owns_no_operational_policy():
    src = Path(IC.__file__).read_text(encoding="utf-8")
    for forbidden in ("open(", "write_text", "sqlite3", "EQ_COST_RATE_PER_SIDE =",
                      "FUT_COST_RATE_PER_SIDE ="):
        assert forbidden not in src
