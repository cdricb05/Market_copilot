r"""Release 47 - PORTFOLIO DECISION FORWARD EVIDENCE (pure calculation kernel).

The question this kernel exists to answer
-----------------------------------------
The system can now identify a better feasible portfolio, price the switch and let an
operator approve a paper reallocation. Exactly one question can ever validate that
machinery:

    DID THE REALLOCATION ACTUALLY ADD VALUE?

Answering it honestly requires two paths, not one:

  A. THE EXECUTED PAPER PORTFOLIO - what we actually own after the reallocation.
  B. THE COUNTERFACTUAL HOLD PORTFOLIO - what we would have owned had we kept the
     previous holdings and done nothing.

The difference between them, after the transaction cost the switch actually paid, is
PORTFOLIO_DECISION_ALPHA. It is a different quantity from the Release-46 challenger
alpha (which measures a research signal) and from the Stage-21 reassessment outcome
evidence (which measures whether a RECOMMENDATION was any good). This kernel measures
whether an EXECUTED CAPITAL DECISION was any good.

Why the counterfactual must be frozen, not reconstructed
--------------------------------------------------------
A hold portfolio reconstructed later is worthless: by then we know which names went
up, and any judgement call made while rebuilding it - which shares, which prices,
which corporate-action treatment - is made with hindsight. So the hold basket, its
reference prices and its NAV are FROZEN AT DECISION TIME, before a single forward
price exists, exactly as the paired executed basket is. This kernel therefore refuses
to measure any record that was not frozen strictly before the evidence it is measured
against (:func:`measure_paths` raises no exception - it returns
``POINT_IN_TIME_VIOLATION`` and measures nothing).

Purity
------
No file, database, network, provider, prediction or clock access. Every price, every
date and every calendar arrives from the composition owner
(``api.portfolio_decision_outcome``), which reads them from the canonical owners.
Nothing here creates an order, a fill, a target, a holding, a cash movement or a NAV.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

SCHEMA_VERSION = "portfolio_decision_outcome.v1"
RECORD_SCHEMA_VERSION = "portfolio_decision_record.v1"
MEASUREMENT_POLICY_VERSION = "portfolio_decision_outcome_policy.v1"
CALCULATION_OWNER = "engine.portfolio_decision_outcome"
PHASE = "R47"

# --- The two frozen paths ---------------------------------------------------- #
PATH_EXECUTED = "EXECUTED_PAPER_PORTFOLIO"
PATH_HOLD = "COUNTERFACTUAL_HOLD_PORTFOLIO"
PATH_VOCAB = (PATH_EXECUTED, PATH_HOLD)

# --- Measurement state vocabulary -------------------------------------------- #
M_NOT_YET_MEASURABLE = "NOT_YET_MEASURABLE"      # no forward session has completed
M_MEASURED = "MEASURED"
M_INSUFFICIENT_COVERAGE = "INSUFFICIENT_PRICE_COVERAGE"
M_POINT_IN_TIME_VIOLATION = "POINT_IN_TIME_VIOLATION"
M_UNMEASURABLE = "UNMEASURABLE"
MEASUREMENT_STATE_VOCAB = (M_NOT_YET_MEASURABLE, M_MEASURED,
                           M_INSUFFICIENT_COVERAGE, M_POINT_IN_TIME_VIOLATION,
                           M_UNMEASURABLE)

#: Cash earns zero on both paths. This is the SAME declared paper assumption the
#: zero-base allocator makes (``engine.zero_base_allocator.CASH_RETURN``); it is
#: restated here because both paths must price cash identically or the comparison
#: silently rewards whichever path happens to hold more of it.
CASH_RETURN = 0.0
CASH_RETURN_POLICY = "ZERO_RETURN_PAPER_ASSUMPTION"

#: The verdict vocabulary for the decision itself. It is EVIDENCE, never policy: a
#: verdict here promotes no model, changes no threshold and approves nothing.
V_ADDED_VALUE = "DECISION_ADDED_VALUE"
V_DESTROYED_VALUE = "DECISION_DESTROYED_VALUE"
V_INDISTINGUISHABLE = "DECISION_INDISTINGUISHABLE_FROM_HOLDING"
V_PENDING = "PENDING_FORWARD_EVIDENCE"
VERDICT_VOCAB = (V_ADDED_VALUE, V_DESTROYED_VALUE, V_INDISTINGUISHABLE, V_PENDING)

_TRADING_DAYS_YEAR = 252.0


def default_policy() -> dict[str, Any]:
    """The single explicit, versioned measurement policy.

    Frozen before any decision was measured. Nothing here is fitted to a realised
    outcome - a threshold tuned on the answer it is supposed to judge is not a
    threshold.
    """
    return {
        "measurement_policy_version": MEASUREMENT_POLICY_VERSION,
        # A path is measured only when this fraction of its INVESTED weight can be
        # priced at the measurement date. Below it the value is withheld, never
        # extrapolated. Mirrors the Slice-7 covariance-coverage philosophy.
        "min_price_coverage": 0.80,
        # Minimum completed forward observations before a risk-adjusted statistic is
        # reported at all. Below it a ratio is noise wearing a decimal point.
        "min_observations_for_risk_adjustment": 20,
        # The band inside which the two paths are declared indistinguishable, in
        # return terms. 10 bps is well below one round trip of the canonical desk
        # cost (25 bps), so a "win" can never be smaller than the cost of trading.
        "indistinguishable_band_return": 0.001,
    }


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _r(x: Optional[float], nd: int) -> Optional[float]:
    return None if x is None else round(float(x), nd)


def _money(x: Optional[float]) -> Optional[float]:
    return None if x is None else round(float(x), 2)


_VOLATILE_KEYS = frozenset({"frozen_at", "generated_at", "measured_at",
                            "record_hash"})


def _strip_volatile(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _strip_volatile(v) for k, v in sorted(obj.items())
                if k not in _VOLATILE_KEYS}
    if isinstance(obj, (list, tuple)):
        return [_strip_volatile(v) for v in obj]
    return obj


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(
        json.dumps(_strip_volatile(obj), sort_keys=True, separators=(",", ":"),
                   default=str).encode("utf-8")).hexdigest()


def decision_id_for(*, proposal_hash: Optional[str],
                    order_plan_id: Optional[str],
                    eligible_market_date: Optional[str],
                    active_book_id: Optional[str]) -> str:
    """The immutable identity of ONE executed portfolio decision.

    It binds the exact proposal, the exact order plan, the book and the decision
    session, so replaying an approval or an execution can never create a second
    record for the same decision.
    """
    return "pdo_%s_%s_%s_%s" % (
        eligible_market_date or "nodate", active_book_id or "book",
        (proposal_hash or "nohash")[:12], (order_plan_id or "noplan")[-12:])


# --------------------------------------------------------------------------- #
# Freezing the decision record
# --------------------------------------------------------------------------- #
def freeze_decision_record(*, decision_id: str, frozen_at: str,
                           eligible_market_date: Optional[str],
                           active_book_id: Optional[str],
                           previous_portfolio: dict,
                           proposed_target: dict,
                           executed_target: dict,
                           reference_prices: dict,
                           nav_at_decision: Optional[float],
                           transaction_cost: Optional[float],
                           decision_reasons: Optional[dict] = None,
                           expected_improvement: Optional[dict] = None,
                           risk_at_decision: Optional[dict] = None,
                           constraints_at_decision: Optional[dict] = None,
                           model_state: Optional[dict] = None,
                           provenance: Optional[dict] = None,
                           policy: Optional[dict] = None) -> dict:
    """Freeze ONE immutable portfolio-decision record with BOTH forward paths.

    ``previous_portfolio`` / ``proposed_target`` / ``executed_target`` are weight
    maps. ``reference_prices`` is the point-in-time price of every ticker either path
    needs, read at the decision session by the canonical mark owner.

    The two paths are frozen HERE, together, before any forward price exists:

    * :data:`PATH_EXECUTED` holds the executed target and carries the transaction
      cost the switch actually paid;
    * :data:`PATH_HOLD` holds the PREVIOUS portfolio and pays nothing, because not
      trading costs nothing.

    A ticker either path needs but cannot be priced at the decision session is
    recorded as a named gap. It is never dropped silently and never priced later:
    a basket completed after the fact is a reconstruction, and this record exists
    precisely so that no reconstruction is ever necessary.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)

    prev = {tk: (_f(w) or 0.0) for tk, w in (previous_portfolio or {}).items()
            if (_f(w) or 0.0) > 0}
    prop = {tk: (_f(w) or 0.0) for tk, w in (proposed_target or {}).items()
            if (_f(w) or 0.0) > 0}
    execu = {tk: (_f(w) or 0.0) for tk, w in (executed_target or {}).items()
             if (_f(w) or 0.0) > 0}
    prices = {tk: _f(p) for tk, p in (reference_prices or {}).items()
              if _f(p) is not None and (_f(p) or 0.0) > 0}

    def _path(kind: str, basket: dict, cost: Optional[float]) -> dict:
        missing = sorted(tk for tk in basket if tk not in prices)
        invested = sum(basket.values())
        priced = sum(w for tk, w in basket.items() if tk in prices)
        coverage = (priced / invested) if invested > 0 else 1.0
        return {
            "path_kind": kind,
            "basket": {tk: _r(w, 8) for tk, w in sorted(basket.items())},
            "position_count": len(basket),
            "invested_weight": _r(invested, 8),
            "cash_weight": _r(max(0.0, 1.0 - invested), 8),
            "reference_prices": {tk: _r(prices[tk], 6)
                                 for tk in sorted(basket) if tk in prices},
            "unpriced_tickers": missing,
            "price_coverage_at_decision": _r(coverage, 6),
            "transaction_cost_charged": _money(cost),
            "cost_doc": ("The executed path pays the transaction cost the switch "
                         "actually incurred; the hold path pays nothing, because "
                         "not trading costs nothing."),
        }

    record = {
        "schema_version": RECORD_SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "decision_id": decision_id,
        "frozen_at": frozen_at,
        "eligible_market_date": eligible_market_date,
        "active_book_id": active_book_id,
        "nav_at_decision": _money(_f(nav_at_decision)),
        "previous_portfolio": {tk: _r(w, 8) for tk, w in sorted(prev.items())},
        "proposed_target": {tk: _r(w, 8) for tk, w in sorted(prop.items())},
        "executed_target": {tk: _r(w, 8) for tk, w in sorted(execu.items())},
        "executed_matches_proposed": bool(
            sorted(execu) == sorted(prop)
            and all(abs(execu.get(t, 0.0) - prop.get(t, 0.0)) < 1e-6
                    for t in set(execu) | set(prop))),
        "paths": {
            PATH_EXECUTED: _path(PATH_EXECUTED, execu, _f(transaction_cost) or 0.0),
            PATH_HOLD: _path(PATH_HOLD, prev, 0.0),
        },
        "path_vocabulary": list(PATH_VOCAB),
        "decision_reasons": dict(decision_reasons or {}),
        "expected_improvement": dict(expected_improvement or {}),
        "transaction_cost": _money(_f(transaction_cost)),
        "risk_at_decision": dict(risk_at_decision or {}),
        "constraints_at_decision": dict(constraints_at_decision or {}),
        "model_state": dict(model_state or {}),
        "provenance": dict(provenance or {}),
        "cash_return_assumed": CASH_RETURN,
        "cash_return_policy": CASH_RETURN_POLICY,
        "measurement_policy": pol,
        "counterfactual_doc": (
            "The hold basket, its reference prices and its NAV are frozen at "
            "decision time, before any forward price exists. It is NEVER "
            "reconstructed afterwards."),
        "immutable": True,
        "created_orders": False,
        "changed_holdings": False,
        "changed_cash": False,
        "changed_nav": False,
    }
    record["record_hash"] = stable_hash(record)
    return record


# --------------------------------------------------------------------------- #
# Forward measurement
# --------------------------------------------------------------------------- #
def _path_return(basket: dict, ref: dict, now_prices: dict,
                 coverage_floor: float) -> tuple:
    """One path's gross return at a measurement date, and its price coverage.

    Cash (``1 - sum(basket)``) earns :data:`CASH_RETURN`. A name with no price at the
    measurement date contributes NOTHING and is subtracted from coverage - it is
    never carried at its entry price, which would silently assert it did not move.
    """
    invested = sum(basket.values())
    priced_w = 0.0
    acc = 0.0
    for tk, w in basket.items():
        p0, p1 = _f(ref.get(tk)), _f(now_prices.get(tk))
        if p0 is None or p1 is None or p0 <= 0:
            continue
        priced_w += w
        acc += w * (p1 / p0 - 1.0)
    coverage = (priced_w / invested) if invested > 0 else 1.0
    if invested > 0 and coverage < coverage_floor:
        return None, coverage
    cash_w = max(0.0, 1.0 - invested)
    return acc + cash_w * CASH_RETURN, coverage


def measure_paths(*, record: dict, price_history: dict,
                  measurement_dates: Optional[list] = None,
                  policy: Optional[dict] = None) -> dict:
    """Measure both frozen paths forward and report the incremental evidence.

    ``price_history`` is ``{ticker: {date: close}}`` supplied by the canonical mark
    owner. ``measurement_dates`` is the ordered list of COMPLETED sessions strictly
    AFTER the decision session; when omitted it is derived from the price history,
    which never contains a date the mark owner has not settled.

    Every date used is strictly later than ``eligible_market_date``. A record whose
    frozen date is not strictly earlier than its evidence returns
    :data:`M_POINT_IN_TIME_VIOLATION` and measures nothing: that ordering is the one
    guarantee that makes the counterfactual worth anything.
    """
    # The policy the record was FROZEN with wins over the current default, so a
    # later policy change can never silently re-judge an old decision. An explicit
    # caller override is applied last and only when supplied.
    pol = dict(default_policy())
    pol.update((record or {}).get("measurement_policy") or {})
    if policy:
        pol.update(policy)

    rec = record or {}
    decided = rec.get("eligible_market_date")
    paths = rec.get("paths") or {}
    ex = paths.get(PATH_EXECUTED) or {}
    ho = paths.get(PATH_HOLD) or {}
    nav0 = _f(rec.get("nav_at_decision"))
    cost = _f(ex.get("transaction_cost_charged")) or 0.0

    base = {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "decision_id": rec.get("decision_id"),
        "record_hash": rec.get("record_hash"),
        "eligible_market_date": decided,
        "state_vocabulary": list(MEASUREMENT_STATE_VOCAB),
        "verdict_vocabulary": list(VERDICT_VOCAB),
        "path_vocabulary": list(PATH_VOCAB),
        "measurement_policy": pol,
        "hindsight_reconstruction": False,
        "counterfactual_frozen_prospectively": True,
    }

    if not decided or nav0 is None or nav0 <= 0:
        return {**base, "state": M_UNMEASURABLE, "verdict": V_PENDING,
                "observations": [], "observation_count": 0,
                "detail": ("The record carries no decision session or no NAV, so "
                           "neither path can be valued.")}

    if measurement_dates is None:
        dates = sorted({d for series in (price_history or {}).values()
                        for d in (series or {})})
    else:
        dates = sorted(set(measurement_dates))
    forward = [d for d in dates if str(d) > str(decided)]
    if not forward:
        return {**base, "state": M_NOT_YET_MEASURABLE, "verdict": V_PENDING,
                "observations": [], "observation_count": 0,
                "detail": ("No completed session exists after the decision session "
                           "yet. The paths are frozen and waiting.")}

    floor = float(pol["min_price_coverage"])
    observations: list[dict] = []
    for d in forward:
        now = {tk: (series or {}).get(d)
               for tk, series in (price_history or {}).items()}
        r_ex, cov_ex = _path_return(
            {k: float(v) for k, v in (ex.get("basket") or {}).items()},
            ex.get("reference_prices") or {}, now, floor)
        r_ho, cov_ho = _path_return(
            {k: float(v) for k, v in (ho.get("basket") or {}).items()},
            ho.get("reference_prices") or {}, now, floor)
        if r_ex is None or r_ho is None:
            observations.append({
                "date": d, "state": M_INSUFFICIENT_COVERAGE,
                "executed_price_coverage": _r(cov_ex, 4),
                "hold_price_coverage": _r(cov_ho, 4)})
            continue
        v_ex = (nav0 - cost) * (1.0 + r_ex)
        v_ho = nav0 * (1.0 + r_ho)
        observations.append({
            "date": d, "state": M_MEASURED,
            "executed_return": _r(r_ex, 8),
            "hold_return": _r(r_ho, 8),
            "executed_value": _money(v_ex),
            "hold_value": _money(v_ho),
            "executed_value_return": _r(v_ex / nav0 - 1.0, 8),
            "hold_value_return": _r(v_ho / nav0 - 1.0, 8),
            "incremental_pnl": _money(v_ex - v_ho),
            "incremental_return": _r((v_ex - v_ho) / nav0, 8),
            "executed_price_coverage": _r(cov_ex, 4),
            "hold_price_coverage": _r(cov_ho, 4),
        })

    measured = [o for o in observations if o["state"] == M_MEASURED]
    if not measured:
        return {**base, "state": M_INSUFFICIENT_COVERAGE, "verdict": V_PENDING,
                "observations": observations,
                "observation_count": len(observations),
                "detail": ("No forward session could price both frozen baskets to "
                           "the required coverage. The value is withheld rather "
                           "than extrapolated.")}

    last = measured[-1]
    ex_curve = [o["executed_value_return"] for o in measured]
    ho_curve = [o["hold_value_return"] for o in measured]
    dd_ex = _max_drawdown(ex_curve)
    dd_ho = _max_drawdown(ho_curve)
    incr = [o["incremental_return"] for o in measured]
    step = [incr[0]] + [incr[i] - incr[i - 1] for i in range(1, len(incr))]
    min_obs = int(pol["min_observations_for_risk_adjustment"])
    risk_adjusted = None
    incr_vol = None
    if len(step) >= min_obs:
        mean = sum(step) / len(step)
        var = sum((x - mean) ** 2 for x in step) / max(1, len(step) - 1)
        sd = math.sqrt(var)
        incr_vol = sd * math.sqrt(_TRADING_DAYS_YEAR)
        if sd > 0:
            risk_adjusted = mean / sd * math.sqrt(_TRADING_DAYS_YEAR)

    band = float(pol["indistinguishable_band_return"])
    delta = last["incremental_return"] or 0.0
    if abs(delta) <= band:
        verdict = V_INDISTINGUISHABLE
    elif delta > 0:
        verdict = V_ADDED_VALUE
    else:
        verdict = V_DESTROYED_VALUE

    return {
        **base,
        "state": M_MEASURED,
        "verdict": verdict,
        "as_of": last["date"],
        "observations": observations,
        "observation_count": len(observations),
        "measured_observation_count": len(measured),
        "nav_at_decision": _money(nav0),
        "transaction_cost_paid": _money(cost),
        "executed_value": last["executed_value"],
        "hold_value": last["hold_value"],
        "executed_return": last["executed_value_return"],
        "hold_return": last["hold_value_return"],
        "incremental_pnl": last["incremental_pnl"],
        "incremental_return": last["incremental_return"],
        "incremental_return_gross_of_cost": _r(
            (last["incremental_return"] or 0.0) + (cost / nav0), 8),
        "executed_max_drawdown": _r(dd_ex, 6),
        "hold_max_drawdown": _r(dd_ho, 6),
        "incremental_drawdown": _r(
            None if (dd_ex is None or dd_ho is None) else dd_ex - dd_ho, 6),
        "incremental_volatility_annualised": _r(incr_vol, 6),
        "risk_adjusted_improvement": _r(risk_adjusted, 6),
        "risk_adjustment_state": (
            "AVAILABLE" if risk_adjusted is not None
            else "INSUFFICIENT_OBSERVATIONS"),
        "holding_period_opportunity_cost": _money(
            max(0.0, -(last["incremental_pnl"] or 0.0))),
        "opportunity_cost_doc": (
            "What holding would have earned that the reallocation gave up. Zero "
            "when the executed path is ahead."),
        "improvement_basis": "EXECUTED_MINUS_FROZEN_COUNTERFACTUAL_HOLD_NET_OF_COST",
        "portfolio_decision_alpha": last["incremental_return"],
        "alpha_is_separate_from_research_alpha": True,
        "alpha_doc": ("PORTFOLIO_DECISION_ALPHA measures an EXECUTED capital "
                      "decision against the portfolio it replaced. It is a "
                      "different quantity from Release-46 challenger alpha and "
                      "from Stage-21 reassessment outcome evidence, and the three "
                      "are never summed."),
    }


def _max_drawdown(curve: list) -> Optional[float]:
    """The worst peak-to-trough fall of a cumulative-return curve, as a negative
    number. ``None`` when the curve carries no usable point."""
    vals = [1.0 + (_f(x) or 0.0) for x in curve if _f(x) is not None]
    if not vals:
        return None
    peak = vals[0]
    worst = 0.0
    for v in vals:
        peak = max(peak, v)
        if peak > 0:
            worst = min(worst, v / peak - 1.0)
    return worst


def point_in_time_check(*, record: dict, evidence_dates: list) -> dict:
    """Every piece of evidence must be strictly later than the frozen record.

    This is the guarantee that makes the counterfactual meaningful, so it is checked
    explicitly rather than assumed from how the caller happened to build its inputs.
    """
    decided = str((record or {}).get("eligible_market_date") or "")
    bad = sorted(str(d) for d in (evidence_dates or []) if str(d) <= decided)
    return {
        "decision_session": decided or None,
        "evidence_before_or_on_decision": bad,
        "ok": not bad and bool(decided),
        "state": (M_POINT_IN_TIME_VIOLATION if (bad or not decided)
                  else M_MEASURED),
        "rule": ("Forward evidence must come from sessions strictly AFTER the "
                 "decision session. A record with no decision session cannot be "
                 "measured at all."),
    }


def safety_block() -> dict:
    return {
        "read_only": True, "paper_only": True, "manual_review": True,
        "automation_off": True, "created_orders": False, "created_fills": False,
        "changed_holdings": False, "changed_cash": False, "changed_nav": False,
        "promoted_model": False, "recalibrated_model": False,
        "changed_policy": False, "broker_enabled": False,
        "live_orders_enabled": False, "hindsight_reconstruction": False,
        "safety_badges": ["PAPER ONLY", "READ ONLY", "EVIDENCE ONLY", "NO ORDERS",
                          "NO BROKER", "MANUAL REVIEW", "AUTOMATION OFF"],
    }


__all__ = [
    "SCHEMA_VERSION", "RECORD_SCHEMA_VERSION", "CALCULATION_OWNER", "PHASE",
    "PATH_EXECUTED", "PATH_HOLD", "PATH_VOCAB", "MEASUREMENT_STATE_VOCAB",
    "VERDICT_VOCAB", "M_MEASURED", "M_NOT_YET_MEASURABLE",
    "M_INSUFFICIENT_COVERAGE", "M_POINT_IN_TIME_VIOLATION", "M_UNMEASURABLE",
    "V_ADDED_VALUE", "V_DESTROYED_VALUE", "V_INDISTINGUISHABLE", "V_PENDING",
    "default_policy", "decision_id_for", "freeze_decision_record",
    "measure_paths", "point_in_time_check", "stable_hash", "safety_block",
]
