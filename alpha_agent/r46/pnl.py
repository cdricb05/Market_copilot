"""alpha_agent.r46.pnl - THE alpha-to-P&L owner. One calculation per concept.

Release 46 answered "did the model predict correctly, after costs, against the
correct control?" and it answered it ONCE, in :mod:`alpha_agent.r46.judge`, on
the day a prediction matured. Release 46.4 asks the question money asks every
session: "what is this position worth right now, what did it cost to hold,
and how much of what it earned was the market rather than the model?"

This module is the single place a prospective prediction becomes an economic
trade result. Every other surface - the trade ledger, the strategy streams,
the shadow NAV, the allocator, the leaderboard, the API, the UI - consumes
what is computed here and computes none of it again.

Three rules keep it honest.

**The judge stays the owner of maturity.** When a prediction has matured, its
gross, cost, net, control and residual are TAKEN from the outcome row the
judge appended - never recomputed. This module adds the dollar layer, the
cost-stress layer and the attribution descriptors, and it RECONCILES its own
open-trade arithmetic against the judge's number at close: a mismatch is
reported as ``RECONCILIATION_MISMATCH``, not silently absorbed.

**Marks are point-in-time.** An open trade is marked at the last bar printed
ON OR BEFORE the mark session, per leg, on the instrument's own calendar. A
bar that had not printed is never used, and a leg whose entry has not printed
leaves the trade unmarkable rather than approximately marked.

**Cost is recognised in full at open.** Both sides of the round trip are
charged the moment a trade opens, on traded notional, at the SAME per-class
number the judge uses (the research cost classes below are a decomposition of
:data:`alpha_agent.r46.contract.COST_BPS_PER_SIDE` plus slippage, and a test
pins the sum). Unrealised P&L is therefore conservative by construction and
converges to the judge's realised net with no jump at maturity. The 2x and
STRESS scenarios exist to show how fragile an edge is; they never replace the
declared BASE number on any ledger.

RESEARCH ONLY. Unit economics here are per 1.0 of shadow capital; the dollar
layer is applied by :mod:`alpha_agent.r46.nav` under a frozen policy. Nothing
here is a position, an order, a holding or a target.
"""
from __future__ import annotations

import datetime as _dt
import math
from typing import Callable, Optional

from . import contract as C
from . import marketdata as MD

CALCULATION_OWNER = "alpha_agent.r46.pnl"

#: Evidence classes for economic results. A number computed from dates that
#: had already happened when the calculation ran is never TRUE_FORWARD.
EVIDENCE_TRUE_FORWARD = C.TRUE_FORWARD
EVIDENCE_HISTORICAL_SIMULATION = C.HISTORICAL_SIMULATION
EVIDENCE_IMPLEMENTATION_CALIBRATION = "IMPLEMENTATION_CALIBRATION"
EVIDENCE_RISK_PRIOR = "RISK_PRIOR"
ECONOMIC_EVIDENCE_CLASSES = (EVIDENCE_TRUE_FORWARD,
                             EVIDENCE_HISTORICAL_SIMULATION,
                             EVIDENCE_IMPLEMENTATION_CALIBRATION,
                             EVIDENCE_RISK_PRIOR)

#: Scenario vocabulary. BASE is the declared contract cost; the others are
#: fragility probes and never enter a ledger as the realised number.
SCENARIO_BASE = "BASE"
SCENARIO_2X = "2X"
SCENARIO_STRESS = "STRESS"
SCENARIOS = (SCENARIO_BASE, SCENARIO_2X, SCENARIO_STRESS)
STRESS_TRANSACTION_MULTIPLIER = 3.0

RECONCILIATION_TOLERANCE = 1e-9

# --------------------------------------------------------------------------- #
# Canonical research cost classes - a DECOMPOSITION of the frozen contract cost
# --------------------------------------------------------------------------- #
#: Per-side transaction components in bps of traded notional. For every class
#: the contract already prices, ``half_spread + commission + slippage +
#: impact`` equals ``COST_BPS_PER_SIDE[class] + SLIPPAGE_BPS_PER_SIDE`` exactly,
#: so the judge and this module charge the same BASE number; the decomposition
#: exists so an operator can see WHAT the number is made of and so the STRESS
#: scenario can stress the components that actually move under size. Holding
#: costs (borrow, financing, roll, option carry) are annualised bps on the
#: relevant notional and are charged ONLY in the STRESS scenario - the judge
#: never charged them and the BASE ledger number must remain the judge's.
COST_CLASSES = {
    "US_EQUITY": {"half_spread_bps": 3.0, "commission_bps": 1.0,
                  "slippage_bps": 1.0, "impact_bps": 1.0,
                  "borrow_bps_annual_short": 50.0, "financing_bps_annual": 0.0,
                  "roll_bps_annual": 0.0, "instrument": "US listed equity"},
    "US_ETF": {"half_spread_bps": 1.0, "commission_bps": 0.5,
               "slippage_bps": 1.0, "impact_bps": 0.5,
               "borrow_bps_annual_short": 30.0, "financing_bps_annual": 0.0,
               "roll_bps_annual": 0.0, "instrument": "US listed ETF"},
    "LISTED_REAL_ESTATE": {"half_spread_bps": 3.0, "commission_bps": 1.0,
                           "slippage_bps": 1.0, "impact_bps": 1.0,
                           "borrow_bps_annual_short": 100.0,
                           "financing_bps_annual": 0.0, "roll_bps_annual": 0.0,
                           "instrument": "US listed REIT / real-estate ETF"},
    "EQUITY_INDEX_FUTURES": {"half_spread_bps": 0.5, "commission_bps": 0.25,
                             "slippage_bps": 1.0, "impact_bps": 0.25,
                             "borrow_bps_annual_short": 0.0,
                             "financing_bps_annual": 0.0,
                             "roll_bps_annual": 8.0,
                             "instrument": "CME/Eurex equity index future"},
    "RATES_FUTURES": {"half_spread_bps": 0.25, "commission_bps": 0.25,
                      "slippage_bps": 1.0, "impact_bps": 0.25,
                      "borrow_bps_annual_short": 0.0,
                      "financing_bps_annual": 0.0, "roll_bps_annual": 6.0,
                      "instrument": "CBOT/Eurex rates future"},
    "COMMODITY_FUTURES": {"half_spread_bps": 1.5, "commission_bps": 0.5,
                          "slippage_bps": 1.0, "impact_bps": 0.5,
                          "borrow_bps_annual_short": 0.0,
                          "financing_bps_annual": 0.0, "roll_bps_annual": 21.0,
                          "instrument": "NYMEX/COMEX/CBOT/ICE commodity future"},
    "FX_FUTURES": {"half_spread_bps": 0.5, "commission_bps": 0.25,
                   "slippage_bps": 1.0, "impact_bps": 0.25,
                   "borrow_bps_annual_short": 0.0,
                   "financing_bps_annual": 0.0, "roll_bps_annual": 8.0,
                   "instrument": "CME currency future"},
    "FX_SPOT": {"half_spread_bps": 1.0, "commission_bps": 0.0,
                "slippage_bps": 1.0, "impact_bps": 0.5,
                "borrow_bps_annual_short": 0.0,
                "financing_bps_annual": 25.0, "roll_bps_annual": 0.0,
                "instrument": "G10 spot cross (tom-next financing)"},
    "VOLATILITY_FUTURES": {"half_spread_bps": 8.0, "commission_bps": 1.0,
                           "slippage_bps": 3.0, "impact_bps": 1.0,
                           "borrow_bps_annual_short": 0.0,
                           "financing_bps_annual": 0.0,
                           "roll_bps_annual": 156.0,
                           "instrument": "Cboe VIX future"},
    "CRYPTO": {"half_spread_bps": 3.0, "commission_bps": 1.0,
               "slippage_bps": 1.0, "impact_bps": 1.0,
               "borrow_bps_annual_short": 300.0,
               "financing_bps_annual": 500.0, "roll_bps_annual": 0.0,
               "instrument": "spot / perpetual crypto (where eligible)"},
    "OPTIONS": {"half_spread_bps": 15.0, "commission_bps": 2.0,
                "slippage_bps": 5.0, "impact_bps": 3.0,
                "borrow_bps_annual_short": 0.0,
                "financing_bps_annual": 0.0, "roll_bps_annual": 0.0,
                "instrument": "listed SPY option, per unit of underlying "
                              "notional; not yet priced by the contract"},
}

#: Classes whose BASE per-side number must equal the frozen contract exactly.
CONTRACT_PRICED_CLASSES = tuple(sorted(C.COST_BPS_PER_SIDE))

#: A spec-level class that is never a leg class; legs carry their own.
MIXED_CLASS_FALLBACK = "US_EQUITY"


def base_per_side_bps(cost_class: str) -> float:
    """BASE per-side cost in bps of traded notional - the judge's number."""
    k = COST_CLASSES.get(cost_class)
    if k is None:
        return float(C.COST_BPS_PER_SIDE.get(cost_class, 5.0)
                     + C.SLIPPAGE_BPS_PER_SIDE)
    return float(k["half_spread_bps"] + k["commission_bps"]
                 + k["slippage_bps"] + k["impact_bps"])


def contract_per_side_bps(cost_class: str) -> float:
    return float(C.COST_BPS_PER_SIDE.get(cost_class, 5.0)
                 + C.SLIPPAGE_BPS_PER_SIDE)


def decomposition_matches_contract() -> dict:
    """Prove, not assert, that BASE == the frozen contract for every class."""
    rows = {}
    for k in CONTRACT_PRICED_CLASSES:
        rows[k] = {"contract_per_side_bps": contract_per_side_bps(k),
                   "decomposed_per_side_bps": base_per_side_bps(k),
                   "matches": abs(contract_per_side_bps(k)
                                  - base_per_side_bps(k)) < 1e-9}
    return {"all_match": all(r["matches"] for r in rows.values()),
            "classes": rows}


def cost_stack(legs: list, spec_cost_class: str = None,
               horizon_sessions: int = 1, scenario: str = SCENARIO_BASE) -> dict:
    """Round-trip cost of a book, per 1.0 of capital, itemised.

    ``transaction`` is charged on BOTH sides on traded notional. ``holding``
    (borrow on short legs, financing, roll) is prorated by the horizon and
    charged only under STRESS. Returned in DECIMAL return units.
    """
    mult = {SCENARIO_BASE: 1.0, SCENARIO_2X: 2.0,
            SCENARIO_STRESS: STRESS_TRANSACTION_MULTIPLIER}.get(scenario, 1.0)
    years = float(max(1, int(horizon_sessions))) / 252.0
    comp = {"half_spread": 0.0, "commission": 0.0, "slippage": 0.0,
            "impact": 0.0, "borrow": 0.0, "financing": 0.0, "roll": 0.0}
    for l in legs or ():
        w = abs(float(l.get("weight") or 0.0))
        klass = l.get("cost_class") or spec_cost_class or MIXED_CLASS_FALLBACK
        k = COST_CLASSES.get(klass)
        if k is None:
            per_side = contract_per_side_bps(klass)
            comp["half_spread"] += w * per_side * 2.0
            continue
        comp["half_spread"] += w * k["half_spread_bps"] * 2.0
        comp["commission"] += w * k["commission_bps"] * 2.0
        comp["slippage"] += w * k["slippage_bps"] * 2.0
        comp["impact"] += w * k["impact_bps"] * 2.0
        if scenario == SCENARIO_STRESS:
            if float(l.get("weight") or 0.0) < 0:
                comp["borrow"] += w * k["borrow_bps_annual_short"] * years
            comp["financing"] += w * k["financing_bps_annual"] * years
            comp["roll"] += w * k["roll_bps_annual"] * years
    transaction_bps = (comp["half_spread"] + comp["commission"]
                       + comp["slippage"] + comp["impact"]) * mult
    holding_bps = comp["borrow"] + comp["financing"] + comp["roll"]
    return {
        "scenario": scenario,
        "transaction_multiplier": mult,
        "components_bps": {k: round(v * (mult if k in (
            "half_spread", "commission", "slippage", "impact") else 1.0), 6)
            for k, v in comp.items()},
        "transaction_bps_round_trip": round(transaction_bps, 6),
        "holding_bps": round(holding_bps, 6),
        "total_bps": round(transaction_bps + holding_bps, 6),
        "total_return_units": (transaction_bps + holding_bps) / 1e4,
        "charged_on": "TRADED_NOTIONAL_BOTH_SIDES",
    }


def cost_scenarios(legs: list, spec_cost_class: str = None,
                   horizon_sessions: int = 1) -> dict:
    return {s: cost_stack(legs, spec_cost_class, horizon_sessions, s)
            for s in SCENARIOS}


# --------------------------------------------------------------------------- #
# Point-in-time marks - the instrument's own calendar, never a future bar
# --------------------------------------------------------------------------- #
SeriesFn = Callable[[str], Optional[object]]


def _dates(series) -> list:
    return [ts.date() for ts in series.index]


def entry_mark(series, entry_date: _dt.date):
    """(date, price) of the first bar ON OR AFTER the entry date.

    Identical to the judge's entry resolution: a holiday moves the entry to
    the instrument's next realised bar and never earlier.
    """
    if series is None or not len(series):
        return None, None
    for ts, px in series.items():
        if ts.date() >= entry_date:
            return ts.date(), float(px)
    return None, None


def mark_on_or_before(series, as_of: _dt.date, not_before: _dt.date = None):
    """(date, price) of the LAST bar on or before ``as_of``.

    ``not_before`` refuses a mark older than the entry (a leg that has not
    printed since it entered has no legitimate mark yet).
    """
    if series is None or not len(series):
        return None, None
    best_d, best_px = None, None
    for ts, px in series.items():
        d = ts.date()
        if d > as_of:
            break
        best_d, best_px = d, float(px)
    if best_d is None or (not_before is not None and best_d < not_before):
        return None, None
    return best_d, best_px


def maturity_mark(series, entry_date: _dt.date, horizon: int):
    """(date, price) ``horizon`` realised sessions after the entry close."""
    if series is None or not len(series):
        return None, None
    dates = _dates(series)
    idx = next((i for i, d in enumerate(dates) if d >= entry_date), None)
    if idx is None:
        return None, None
    j = idx + int(horizon)
    if j >= len(dates):
        return None, None
    return dates[j], float(series.iloc[j])


def sessions_elapsed(series, entry_date: _dt.date, as_of: _dt.date) -> int:
    """Realised sessions strictly after the entry close, up to ``as_of``."""
    if series is None or not len(series):
        return 0
    dates = _dates(series)
    idx = next((i for i, d in enumerate(dates) if d >= entry_date), None)
    if idx is None:
        return 0
    return sum(1 for d in dates[idx + 1:] if d <= as_of)


# --------------------------------------------------------------------------- #
# Descriptors carried on every economic unit
# --------------------------------------------------------------------------- #
def _spec_descriptors(prediction: dict, registry_entry: dict = None) -> dict:
    e = registry_entry or {}
    from . import challengers as CH
    return {
        "asset_class": prediction.get("asset_class"),
        "economic_family": (e.get("family") or prediction.get("model_family")),
        "information_family": (e.get("information_family")
                               or CH.info_family_for(
                                   {"challenger_id":
                                        prediction.get("challenger_id")})),
        "dependence_cluster": (e.get("dependence_cluster")
                               or CH.cluster_for(
                                   {"challenger_id":
                                        prediction.get("challenger_id"),
                                    "family": prediction.get("model_family"),
                                    "asset_class":
                                        prediction.get("asset_class")})),
        "trade_structure": prediction.get("prediction_type"),
        "instrument": prediction.get("instrument"),
        "instruments": list(((prediction.get("instrument_identity") or {})
                             .get("legs")) or []),
        "cost_class": e.get("cost_class"),
        "control": prediction.get("control"),
        "benchmark": prediction.get("benchmark"),
    }


def _liquidity(prediction: dict) -> dict:
    """Liquidity and capacity DESCRIPTORS, not estimates. The estate owns no
    depth data; what can be stated is the structure of the book."""
    legs = ((prediction.get("position_expression") or {}).get("legs")) or []
    weights = [abs(float(l.get("weight") or 0.0)) for l in legs]
    return {
        "n_legs": len(legs),
        "max_leg_weight": max(weights) if weights else None,
        "gross_notional_per_unit_capital": float(sum(weights)),
        "net_notional_per_unit_capital": float(sum(
            float(l.get("weight") or 0.0) for l in legs)),
        "capacity_state": "NOT_MEASURED_NO_DEPTH_DATA",
        "liquidity_state": "DECLARED_LIQUID_UNIVERSE_ONLY",
    }


# --------------------------------------------------------------------------- #
# THE conversion: one prediction -> one economic result at one instant
# --------------------------------------------------------------------------- #
def economics(prediction: dict, as_of: _dt.date, outcome: dict = None,
              registry_entry: dict = None, series_fn: SeriesFn = None,
              risk_free_annual: float = None) -> dict:
    """Economic state of ONE prediction as of ``as_of`` (a session date).

    Returns unit economics (per 1.0 of capital). ``state`` is one of
    ``ENTRY_NOT_PRINTED`` / ``OPEN`` / ``CLOSED`` / ``INVALIDATED``.
    """
    sf = series_fn or MD.closes
    pid = prediction.get("prediction_id")
    horizon = int(prediction.get("horizon") or 0)
    legs = ((prediction.get("position_expression") or {}).get("legs")) or []
    desc = _spec_descriptors(prediction, registry_entry)
    liq = _liquidity(prediction)
    try:
        entry_date = _dt.date.fromisoformat(
            str(prediction.get("effective_as_of"))[:10])
    except ValueError:
        return {"prediction_id": pid, "state": "INVALIDATED",
                "reason": "INVALID_EFFECTIVE_AS_OF", **desc}
    if not legs or prediction.get("status") == C.STATUS_INVALIDATED:
        return {"prediction_id": pid, "state": "INVALIDATED",
                "reason": "NO_LEGS_OR_INVALIDATED", **desc}

    spec_class = (registry_entry or {}).get("cost_class")
    scen = cost_scenarios(legs, spec_class, horizon)
    cost_base = scen[SCENARIO_BASE]["total_return_units"]
    base = {
        "prediction_id": pid,
        "challenger_id": prediction.get("challenger_id"),
        "challenger_version": prediction.get("challenger_version"),
        "challenger_spec_hash": prediction.get("challenger_spec_hash"),
        "horizon": horizon,
        "signal_timestamp_utc": prediction.get("data_cutoff_utc"),
        "decision_timestamp_utc": prediction.get("emitted_at_utc"),
        "direction": prediction.get("direction"),
        "target_exposure_per_unit_capital": liq["gross_notional_per_unit_capital"],
        "sizing_rule": "gross notional 1.0 per unit of allocated capital; "
                       "overlapping cohorts share a cell's capital 1/horizon",
        "entry_session_expected": str(entry_date),
        "as_of": str(as_of),
        "cost_scenarios": scen,
        "cost_recognition": "FULL_ROUND_TRIP_AT_OPEN",
        "evidence_status": prediction.get("forward_evidence_type"),
        "pit_status": prediction.get("point_in_time_status"),
        "calculation_owner": CALCULATION_OWNER,
        **desc, **liq,
    }

    # ---- CLOSED: the judge's numbers are the numbers ---------------------- #
    if outcome is not None:
        gross = float(outcome.get("realised_gross_return") or 0.0)
        cost = float(outcome.get("realised_cost") or 0.0)
        net = float(outcome.get("realised_net_return") or 0.0)
        control_ret = outcome.get("control_return")
        bench_ret = outcome.get("realised_benchmark_return")
        residual = outcome.get("realised_residual_return")
        net_alpha = outcome.get("net_alpha_vs_control")
        recon = _reconcile(prediction, outcome, sf, horizon, spec_class)
        return dict(base, **{
            "state": "CLOSED",
            "entry_session": (outcome.get("entry_dates") or [None])[0],
            "exit_session": outcome.get("maturity_date"),
            "per_leg": outcome.get("per_leg"),
            "entry_marks": {l["instrument"]: l.get("entry_price")
                            for l in (outcome.get("per_leg") or [])},
            "exit_marks": {l["instrument"]: l.get("maturity_price")
                           for l in (outcome.get("per_leg") or [])},
            "sessions_held": horizon,
            "gross_return": gross,
            "cost_return": cost,
            "cost_breakdown": scen[SCENARIO_BASE]["components_bps"],
            "net_return": net,
            "net_return_at_2x": outcome.get("realised_net_return_at_2x_costs"),
            "net_return_at_stress": gross - scen[SCENARIO_STRESS][
                "total_return_units"],
            "control_return": control_ret,
            "benchmark_return": bench_ret,
            "residual_return": residual,
            "residual_alpha_vs_control": net_alpha,
            "residual_alpha_at_2x": outcome.get(
                "net_alpha_vs_control_at_2x_costs"),
            "turnover_per_unit_capital": outcome.get("turnover"),
            "realised": True,
            "source_of_truth": "alpha_agent.r46.judge outcome row",
            "reconciliation": recon,
        })

    # ---- OPEN or not yet entered: mark point-in-time ---------------------- #
    per_leg, missing = [], []
    for l in legs:
        sym = l["instrument"]
        s = sf(sym)
        e_d, e_px = entry_mark(s, entry_date)
        if e_px is None or e_px <= 0 or (e_d is not None and e_d > as_of):
            missing.append({"instrument": sym, "why": "ENTRY_NOT_PRINTED"})
            continue
        m_d, m_px = mark_on_or_before(s, as_of, not_before=e_d)
        if m_px is None or m_px <= 0:
            missing.append({"instrument": sym, "why": "NO_MARK_SINCE_ENTRY"})
            continue
        w = float(l["weight"])
        r = m_px / e_px - 1.0
        per_leg.append({"instrument": sym, "weight": w, "side": l.get("side"),
                        "cost_class": l.get("cost_class"),
                        "entry_date": str(e_d), "entry_price": e_px,
                        "mark_date": str(m_d), "mark_price": m_px,
                        "leg_return": r, "weighted_return": w * r})
    if missing and not per_leg:
        return dict(base, state="ENTRY_NOT_PRINTED", missing=missing[:10],
                    realised=False)
    if missing:
        return dict(base, state="ENTRY_NOT_PRINTED", missing=missing[:10],
                    n_legs_resolved=len(per_leg), realised=False,
                    reason="a leg has not printed its entry; a partially "
                           "entered book is not marked")

    entry_dates = sorted({l["entry_date"] for l in per_leg})
    gross = float(sum(l["weighted_return"] for l in per_leg))
    net = gross - cost_base
    entry_session = _dt.date.fromisoformat(entry_dates[0])
    # Sessions elapsed on the FIRST leg's own calendar; a book's clock is the
    # slowest leg's clock, the judge counts the same way at maturity.
    elapsed = min(sessions_elapsed(sf(l["instrument"]), entry_session, as_of)
                  for l in per_leg) if per_leg else 0
    rf = risk_free_annual
    if rf is None:
        rf = MD.risk_free_annual().get("annual")
    control = prediction.get("control")
    bench_sym = prediction.get("benchmark")
    bench_ret = None
    if bench_sym and bench_sym != "CASH":
        bs = sf(bench_sym)
        _, b_e = entry_mark(bs, entry_date)
        _, b_m = mark_on_or_before(bs, as_of, not_before=entry_session)
        if b_e and b_m and b_e > 0:
            bench_ret = b_m / b_e - 1.0
    if control == C.CONTROL_BENCHMARK:
        control_ret = bench_ret
    else:
        control_ret = (None if rf is None
                       else float(rf) * float(elapsed) / 252.0)
    net_alpha = None if control_ret is None else net - control_ret
    residual = None if bench_ret is None else gross - bench_ret
    return dict(base, **{
        "state": "OPEN",
        "entry_session": entry_dates[0],
        "entry_sessions": entry_dates,
        "mark_session": max(l["mark_date"] for l in per_leg),
        "sessions_held": int(elapsed),
        "sessions_remaining": max(0, horizon - int(elapsed)),
        "per_leg": per_leg,
        "entry_marks": {l["instrument"]: l["entry_price"] for l in per_leg},
        "current_marks": {l["instrument"]: l["mark_price"] for l in per_leg},
        "gross_return": gross,
        "cost_return": cost_base,
        "cost_breakdown": scen[SCENARIO_BASE]["components_bps"],
        "net_return": net,
        "net_return_at_2x": gross - 2.0 * cost_base,
        "net_return_at_stress": gross - scen[SCENARIO_STRESS][
            "total_return_units"],
        "control_return": control_ret,
        "benchmark_return": bench_ret,
        "residual_return": residual,
        "residual_alpha_vs_control": net_alpha,
        "turnover_per_unit_capital": liq["gross_notional_per_unit_capital"]
                                     * 2.0,
        "realised": False,
        "unrealised": True,
        "source_of_truth": "point-in-time mark on or before as_of",
        "risk_free_annual_used": rf,
    })


def _reconcile(prediction: dict, outcome: dict, sf: SeriesFn, horizon: int,
               spec_class: str) -> dict:
    """Recompute the judge's net from marks and compare. Never overrides."""
    try:
        legs = ((prediction.get("position_expression") or {}).get("legs")) or []
        entry_date = _dt.date.fromisoformat(
            str(prediction.get("effective_as_of"))[:10])
        gross = 0.0
        for l in legs:
            s = sf(l["instrument"])
            _, e_px = entry_mark(s, entry_date)
            _, m_px = maturity_mark(s, entry_date, horizon)
            if e_px is None or m_px is None or e_px <= 0:
                return {"state": "NOT_RECOMPUTABLE",
                        "reason": "a leg's bars are no longer readable"}
            gross += float(l["weight"]) * (m_px / e_px - 1.0)
        cost = cost_stack(legs, spec_class, horizon)["total_return_units"]
        net = gross - cost
        judge_net = float(outcome.get("realised_net_return") or 0.0)
        judge_cost = float(outcome.get("realised_cost") or 0.0)
        ok = (abs(net - judge_net) <= RECONCILIATION_TOLERANCE
              and abs(cost - judge_cost) <= RECONCILIATION_TOLERANCE)
        return {"state": "RECONCILED" if ok else "RECONCILIATION_MISMATCH",
                "pnl_owner_net": net, "judge_net": judge_net,
                "pnl_owner_cost": cost, "judge_cost": judge_cost,
                "abs_diff_net": abs(net - judge_net),
                "the_judge_number_is_used": True}
    except Exception as exc:                    # noqa: BLE001 - reported
        return {"state": "NOT_RECOMPUTABLE", "reason": type(exc).__name__}


# --------------------------------------------------------------------------- #
# Dollar layer helpers - pure arithmetic, used by nav/trades
# --------------------------------------------------------------------------- #
def dollars(unit_return: Optional[float], capital: float) -> Optional[float]:
    if unit_return is None or capital is None:
        return None
    v = float(unit_return) * float(capital)
    return v if math.isfinite(v) else None


def contract_summary() -> dict:
    return {
        "calculation_owner": CALCULATION_OWNER,
        "economic_evidence_classes": list(ECONOMIC_EVIDENCE_CLASSES),
        "scenarios": list(SCENARIOS),
        "stress_transaction_multiplier": STRESS_TRANSACTION_MULTIPLIER,
        "cost_recognition": "FULL_ROUND_TRIP_AT_OPEN",
        "cost_classes": {k: dict(v) for k, v in COST_CLASSES.items()},
        "base_per_side_bps": {k: base_per_side_bps(k) for k in COST_CLASSES},
        "decomposition_matches_contract": decomposition_matches_contract(),
        "closed_trades_take_the_judge_number": True,
        "open_trades_are_marked_point_in_time": True,
        "reconciliation_tolerance": RECONCILIATION_TOLERANCE,
        "historical_pnl_is_never_labelled_forward": True,
    }


__all__ = ["CALCULATION_OWNER", "COST_CLASSES", "SCENARIOS", "SCENARIO_BASE",
           "SCENARIO_2X", "SCENARIO_STRESS", "ECONOMIC_EVIDENCE_CLASSES",
           "base_per_side_bps", "decomposition_matches_contract",
           "cost_stack", "cost_scenarios", "entry_mark", "mark_on_or_before",
           "maturity_mark", "sessions_elapsed", "economics", "dollars",
           "contract_summary"]
