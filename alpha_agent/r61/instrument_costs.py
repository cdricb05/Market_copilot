"""
alpha_agent/r61/instrument_costs.py — R96 B1: per-instrument RESEARCH cost estimates.

The research books charge FLAT per-side rates (equities 12.5 bp, the R38 futures
layer 2-15 bp per market). R95 named that the first P0 gap: every net-of-cost
claim depends on a cost number nobody measured per instrument. This module is
the pure estimator half of the fix; the campaign script applies it to owned
data and writes the comparison.

It does NOT change any operational cost policy, any book's charged rate or any
frozen gate. Adoption into a book is a separate governed decision.

That decision was taken on 2026-10-05 (D-PRE-R101-1, docs/ARCHITECTURE_DECISIONS.md):
REJECTED as a book cost. The R96 application of these estimators
(campaign_r96_*/r96_build_cost_model.py) prices a market-year from that whole
year's median price, volume and volatility (days after the trade), applies
today's contract specification to 2004-2026, divides tick and commission by a
back-adjusted continuation price, reads a volume series that is orders of
magnitude below exchange volume for some markets (PL ~20 contracts/day) and
never reads open interest. The research books keep the ONE canonical cost
owner, FUTURES_PER_MARKET_R38_PLUS_ROLL_V1 (alpha_agent.agents_v2.books). These
estimators stay as pure research functions; no book, runner or cost budget may
import them (tests/test_pre_r101_futures_cost_owner.py).

Every number is tagged with its EVIDENCE CLASS, never promoted:

  OBSERVED   read directly from an owned record (a contract's tick size and
             point value, a traded volume, a settlement price)
  ESTIMATED  inferred by a published estimator from owned prices (Corwin-
             Schultz, Abdi-Ranaldo, square-root impact on observed volume)
  ASSUMED    a declared constant with no owned evidence behind it
             (commission schedule, impact coefficient, order size)

A spread estimated from OHLC bars is ESTIMATED even when the bars are one minute
wide: OHLCV carries no quotes.
"""
from __future__ import annotations

import math
from typing import Optional, Sequence

import numpy as np

OWNER = "alpha_agent.r61.instrument_costs"
VERSION = "R96_INSTRUMENT_COSTS_V1"
ADOPTION_STATUS = "REJECTED_AS_BOOK_COST"
ADOPTION_DECISION = "D-PRE-R101-1"

OBSERVED = "OBSERVED"
ESTIMATED = "ESTIMATED"
ASSUMED = "ASSUMED"
EVIDENCE_CLASSES = (OBSERVED, ESTIMATED, ASSUMED)

#: Declared research assumptions (ASSUMED). Changing one is a recorded decision.
ASSUMPTIONS = {
    "impact_coefficient_Y": 0.7,          # square-root law coefficient (literature 0.5-1.0)
    "research_order_notional_usd": 1_000_000.0,
    "equity_commission_usd_per_share": 0.005,
    "futures_commission_usd_per_contract_side": 2.50,
    "liquid_futures_quoted_spread_ticks": 1.0,
}

_K = 3.0 - 2.0 * math.sqrt(2.0)


def corwin_schultz(high: Sequence[float], low: Sequence[float],
                   close: Optional[Sequence[float]] = None) -> np.ndarray:
    """Corwin & Schultz (2012) two-day high-low spread estimates (proportional
    full spread, one per adjacent day pair; negatives set to 0 as in the paper's
    common practice). With ``close`` the overnight adjustment is applied: day
    t+1's range is shifted so it contains day t's close."""
    h = np.asarray(high, dtype=float).copy()
    lo = np.asarray(low, dtype=float).copy()
    if close is not None:
        c = np.asarray(close, dtype=float)
        for t in range(1, len(h)):
            prev = c[t - 1]
            if np.isfinite(prev) and np.isfinite(lo[t]) and prev < lo[t]:
                d = lo[t] - prev
                h[t] -= d
                lo[t] -= d
            elif np.isfinite(prev) and np.isfinite(h[t]) and prev > h[t]:
                d = prev - h[t]
                h[t] += d
                lo[t] += d
    with np.errstate(divide="ignore", invalid="ignore"):
        r = np.log(h / lo) ** 2
        beta = r[:-1] + r[1:]
        hh = np.maximum(h[:-1], h[1:])
        ll = np.minimum(lo[:-1], lo[1:])
        gamma = np.log(hh / ll) ** 2
        alpha = (np.sqrt(2.0 * beta) - np.sqrt(beta)) / _K - np.sqrt(gamma / _K)
        s = 2.0 * (np.exp(alpha) - 1.0) / (1.0 + np.exp(alpha))
    s = np.where(np.isfinite(s), np.maximum(s, 0.0), np.nan)
    return s


def abdi_ranaldo(high: Sequence[float], low: Sequence[float],
                 close: Sequence[float]) -> Optional[float]:
    """Abdi & Ranaldo (2017) close-high-low estimator over a window: proportional
    full spread S = sqrt(max(4 E[(c_t - eta_t)(c_t - eta_{t+1})], 0))."""
    h = np.log(np.asarray(high, dtype=float))
    lo = np.log(np.asarray(low, dtype=float))
    c = np.log(np.asarray(close, dtype=float))
    eta = 0.5 * (h + lo)
    prod = (c[:-1] - eta[:-1]) * (c[:-1] - eta[1:])
    prod = prod[np.isfinite(prod)]
    if len(prod) < 5:
        return None
    s2 = 4.0 * float(np.mean(prod))
    return math.sqrt(max(s2, 0.0))


def tick_floor_half_spread(tick_size: float, price: float, ticks: float = 1.0) -> Optional[float]:
    """Half of a ``ticks``-tick quoted spread as a fraction of price."""
    if not tick_size or not price or price <= 0:
        return None
    return 0.5 * float(ticks) * float(tick_size) / float(price)


def sqrt_impact(daily_vol: float, order_notional: float, adv_notional: float,
                coefficient: float) -> Optional[float]:
    """Square-root market impact as a fraction of price: Y * sigma_d * sqrt(Q/ADV)."""
    if not adv_notional or adv_notional <= 0 or not np.isfinite(daily_vol):
        return None
    return float(coefficient) * float(daily_vol) * math.sqrt(max(order_notional, 0.0) / adv_notional)


def component(value: Optional[float], evidence: str, method: str, **inputs) -> dict:
    if evidence not in EVIDENCE_CLASSES:
        raise ValueError("unknown evidence class %r" % evidence)
    return {"value": None if value is None or not np.isfinite(value) else float(value),
            "evidence": evidence, "method": method, "inputs": inputs}


def one_way_cost(components: dict) -> dict:
    """Sum the per-side components and report the share of the total that each
    evidence class carries. A missing component makes the total UNKNOWN, never a
    silent zero."""
    missing = [k for k, c in components.items() if c.get("value") is None]
    if missing:
        return {"total": None, "state": "UNKNOWN_COMPONENT_MISSING", "missing": missing,
                "by_evidence": {}}
    total = sum(c["value"] for c in components.values())
    by = {e: sum(c["value"] for c in components.values() if c["evidence"] == e)
          for e in EVIDENCE_CLASSES}
    return {"total": float(total), "state": "COMPLETE", "missing": [],
            "by_evidence": {e: float(v) for e, v in by.items()},
            "share_by_evidence": {e: (float(v) / total if total > 0 else None) for e, v in by.items()}}


__all__ = ["OWNER", "VERSION", "ADOPTION_STATUS", "ADOPTION_DECISION", "OBSERVED", "ESTIMATED", "ASSUMED", "EVIDENCE_CLASSES",
           "ASSUMPTIONS", "corwin_schultz", "abdi_ranaldo", "tick_floor_half_spread",
           "sqrt_impact", "component", "one_way_cost"]
