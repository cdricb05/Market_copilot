"""alpha_agent.alpha_recovery.forecast_contract - the ONE canonical forecast product.

Workstream 2. The estate emits rank SCORES (the incumbent's 0.91 is a rank,
not a return) and a forecast consumer cannot tell a calibrated expected return
from a score wearing its name. This module fixes the contract every future
daily / intraday decision process will read:

    A. BROAD MARKET / REGIME       instrument, as_of, horizon, probability_up,
                                   probability_down, expected_return,
                                   expected_excess_return, uncertainty,
                                   downside probability, tail probability,
                                   regime probabilities, model identity
    B. CROSS-SECTIONAL EQUITIES    ticker, horizon, expected (excess) return,
                                   uncertainty, probability_positive, rank,
                                   downside / tail estimate, information
                                   attribution, model identity
    C. MULTI-ASSET / SLEEVES       asset / sleeve, horizon, expected return,
                                   expected risk, downside, confidence, capital
                                   applicability, liquidity / cost assumptions,
                                   evidence maturity

Every economic field is a ``Field`` that is either VALUE (with the calibration
evidence that licenses it) or UNAVAILABLE (with the reason). A probability
must lie in [0, 1]; probability_up + probability_down must sum to one; a
score may NEVER populate expected_return - the builders refuse it unless a
calibration record with ``calibrated=True`` and an out-of-sample calibration
test is attached. Research only: nothing here is wired into capital
allocation; ``research_only`` is stamped on every product.
"""
from __future__ import annotations

import math
from typing import Any

CALCULATION_OWNER = "alpha_agent.alpha_recovery.forecast_contract"
SCHEMA = "paper_trader_forecast_product/1"

VALUE = "VALUE"
UNAVAILABLE = "UNAVAILABLE"

KIND_MARKET = "BROAD_MARKET_REGIME"
KIND_XS_EQUITY = "CROSS_SECTIONAL_EQUITY"
KIND_SLEEVE = "MULTI_ASSET_SLEEVE"
KINDS = (KIND_MARKET, KIND_XS_EQUITY, KIND_SLEEVE)

EVIDENCE_MATURITIES = ("HISTORICAL_OOS_ONLY", "TRUE_FORWARD_ACCRUING", "TRUE_FORWARD_MATURE",
                       "NONE")

#: A calibration record must carry these to license an economic field.
CALIBRATION_REQUIRED_KEYS = ("calibrated", "method", "oos_test", "n_oos_periods")


class ForecastContractError(ValueError):
    """The contract refused a value that would have misrepresented a forecast."""


def field(value: Any = None, *, state: str = VALUE, reason: str | None = None,
          calibration: dict | None = None, units: str | None = None) -> dict:
    """One economic field. UNAVAILABLE carries a reason and no value."""
    if state not in (VALUE, UNAVAILABLE):
        raise ForecastContractError("unknown field state %r" % state)
    if state == UNAVAILABLE:
        if not reason:
            raise ForecastContractError("an UNAVAILABLE field must state why")
        return {"state": UNAVAILABLE, "value": None, "reason": reason, "units": units}
    if value is None or (isinstance(value, float) and not math.isfinite(value)):
        raise ForecastContractError("a VALUE field needs a finite value; use UNAVAILABLE")
    return {"state": VALUE, "value": value, "reason": None, "units": units,
            "calibration": calibration}


def unavailable(reason: str, units: str | None = None) -> dict:
    return field(state=UNAVAILABLE, reason=reason, units=units)


def _is_calibrated(calibration: dict | None) -> bool:
    if not isinstance(calibration, dict):
        return False
    if any(k not in calibration for k in CALIBRATION_REQUIRED_KEYS):
        return False
    if calibration.get("calibrated") is not True:
        return False
    if not calibration.get("oos_test"):
        return False
    try:
        return int(calibration.get("n_oos_periods") or 0) > 0
    except (TypeError, ValueError):
        return False


def probability(p: float | None, *, calibration: dict | None = None,
                reason: str | None = None) -> dict:
    """A calibrated probability in [0, 1], or UNAVAILABLE."""
    if p is None:
        return unavailable(reason or "no calibrated probability model", units="probability")
    try:
        p = float(p)
    except (TypeError, ValueError):
        raise ForecastContractError("probability must be numeric") from None
    if not math.isfinite(p) or p < 0.0 or p > 1.0:
        raise ForecastContractError("probability %r is outside [0, 1]" % p)
    if not _is_calibrated(calibration):
        raise ForecastContractError(
            "a probability may only be reported with an out-of-sample calibration record "
            "(calibrated=True, method, oos_test, n_oos_periods); an uncalibrated model "
            "output must be reported as UNAVAILABLE")
    return field(p, calibration=calibration, units="probability")


def expected_return(x: float | None, *, calibration: dict | None = None,
                    reason: str | None = None, units: str = "fraction_over_horizon") -> dict:
    """A calibrated expected return, or UNAVAILABLE. A rank score is refused."""
    if x is None:
        return unavailable(reason or "no calibrated expected-return model", units=units)
    if isinstance(calibration, dict) and calibration.get("is_score"):
        raise ForecastContractError("a score may not populate expected_return")
    try:
        x = float(x)
    except (TypeError, ValueError):
        raise ForecastContractError("expected_return must be numeric") from None
    if not math.isfinite(x):
        raise ForecastContractError("expected_return must be finite")
    if not _is_calibrated(calibration):
        raise ForecastContractError(
            "expected_return needs an out-of-sample calibration record; a model score is not "
            "an expected return (contract rule 5)")
    return field(x, calibration=calibration, units=units)


def score(x: float | None, *, name: str, scale: str) -> dict:
    """A SCORE, labelled as one. It is carried for attribution and ranking and
    can never be mistaken for a return by a consumer of this contract."""
    if x is None:
        return unavailable("no score", units="score")
    return {"state": VALUE, "value": float(x), "reason": None, "units": "score",
            "is_score": True, "score_name": name, "scale": scale,
            "not_an_expected_return": True}


def _identity(model_id: str, spec_hash: str | None, evidence_maturity: str,
              information: list | None) -> dict:
    if evidence_maturity not in EVIDENCE_MATURITIES:
        raise ForecastContractError("unknown evidence maturity %r" % evidence_maturity)
    return {"model_id": model_id, "specification_hash": spec_hash,
            "evidence_maturity": evidence_maturity,
            "information_families": list(information or [])}


def _validate_probability_pair(p_up: dict, p_down: dict) -> None:
    if p_up["state"] == VALUE and p_down["state"] == VALUE:
        if abs(p_up["value"] + p_down["value"] - 1.0) > 1e-9:
            raise ForecastContractError("probability_up + probability_down must equal 1")


def market_forecast(*, instrument: str, as_of: str, horizon_sessions: int, model_id: str,
                    probability_up: dict, expected_return: dict, expected_excess_return: dict,
                    uncertainty: dict, downside_probability: dict, tail_probability: dict,
                    regime_probabilities: dict | None, evidence_maturity: str,
                    specification_hash: str | None = None, information: list | None = None,
                    probability_down: dict | None = None, notes: str | None = None) -> dict:
    if probability_down is None:
        probability_down = (field(1.0 - probability_up["value"], calibration=probability_up.get("calibration"),
                                  units="probability") if probability_up["state"] == VALUE
                            else unavailable(probability_up.get("reason") or "no probability", "probability"))
    _validate_probability_pair(probability_up, probability_down)
    if regime_probabilities:
        tot = sum(float(v) for v in regime_probabilities.values())
        if abs(tot - 1.0) > 1e-6 or any(float(v) < 0 for v in regime_probabilities.values()):
            raise ForecastContractError("regime probabilities must be a distribution")
    return {"schema": SCHEMA, "kind": KIND_MARKET, "research_only": True,
            "instrument": instrument, "as_of": as_of, "horizon_sessions": int(horizon_sessions),
            "probability_up": probability_up, "probability_down": probability_down,
            "expected_return": expected_return, "expected_excess_return": expected_excess_return,
            "uncertainty": uncertainty, "downside_probability": downside_probability,
            "tail_probability": tail_probability,
            "regime_probabilities": regime_probabilities or None,
            "identity": _identity(model_id, specification_hash, evidence_maturity, information),
            "notes": notes, "calculation_owner": CALCULATION_OWNER}


def equity_forecast(*, ticker: str, as_of: str, horizon_sessions: int, model_id: str,
                    rank: int | None, n_ranked: int | None, expected_excess_return: dict,
                    uncertainty: dict, probability_positive: dict, downside: dict,
                    information_attribution: dict | None, evidence_maturity: str,
                    score_field: dict | None = None, specification_hash: str | None = None,
                    information: list | None = None, notes: str | None = None) -> dict:
    if expected_excess_return.get("is_score"):
        raise ForecastContractError("a score may not be placed in expected_excess_return")
    return {"schema": SCHEMA, "kind": KIND_XS_EQUITY, "research_only": True,
            "ticker": ticker, "as_of": as_of, "horizon_sessions": int(horizon_sessions),
            "rank": rank, "n_ranked": n_ranked, "score": score_field,
            "expected_excess_return": expected_excess_return, "uncertainty": uncertainty,
            "probability_positive": probability_positive, "downside": downside,
            "information_attribution": information_attribution or None,
            "identity": _identity(model_id, specification_hash, evidence_maturity, information),
            "notes": notes, "calculation_owner": CALCULATION_OWNER}


def sleeve_forecast(*, sleeve: str, as_of: str, horizon_sessions: int, model_id: str,
                    expected_return: dict, expected_risk: dict, downside: dict, confidence: dict,
                    capital_applicability: dict, liquidity_cost_assumptions: dict,
                    evidence_maturity: str, specification_hash: str | None = None,
                    information: list | None = None, notes: str | None = None) -> dict:
    if expected_return.get("is_score"):
        raise ForecastContractError("a score may not be placed in expected_return")
    return {"schema": SCHEMA, "kind": KIND_SLEEVE, "research_only": True,
            "sleeve": sleeve, "as_of": as_of, "horizon_sessions": int(horizon_sessions),
            "expected_return": expected_return, "expected_risk": expected_risk,
            "downside": downside, "confidence": confidence,
            "capital_applicability": capital_applicability,
            "liquidity_cost_assumptions": liquidity_cost_assumptions,
            "identity": _identity(model_id, specification_hash, evidence_maturity, information),
            "notes": notes, "calculation_owner": CALCULATION_OWNER}


def validate(product: dict) -> dict:
    """Re-check a product: kinds, probability bounds, the pair identity, and
    that no score sits in an economic field. Returns {valid, failures}."""
    failures = []
    if product.get("schema") != SCHEMA or product.get("kind") not in KINDS:
        failures.append("schema or kind")
    if product.get("research_only") is not True:
        failures.append("research_only flag missing")
    econ_keys = ("expected_return", "expected_excess_return", "probability_up", "probability_down",
                 "probability_positive", "downside_probability", "tail_probability")
    for k in econ_keys:
        f = product.get(k)
        if not isinstance(f, dict):
            continue
        if f.get("is_score"):
            failures.append("%s carries a score" % k)
        if f.get("state") == VALUE:
            v = f.get("value")
            if v is None or not math.isfinite(float(v)):
                failures.append("%s VALUE without a finite value" % k)
            if k.startswith("probability") and not (0.0 <= float(v) <= 1.0):
                failures.append("%s outside [0, 1]" % k)
            if k in ("expected_return", "expected_excess_return", "probability_up",
                     "probability_down", "probability_positive") and not _is_calibrated(f.get("calibration")):
                failures.append("%s VALUE without a calibration record" % k)
        elif f.get("state") == UNAVAILABLE and not f.get("reason"):
            failures.append("%s UNAVAILABLE without a reason" % k)
    if product.get("kind") == KIND_MARKET:
        try:
            _validate_probability_pair(product["probability_up"], product["probability_down"])
        except (ForecastContractError, KeyError) as exc:
            failures.append(str(exc) or "probability pair")
    return {"valid": not failures, "failures": failures}


def calibration_record(*, calibrated: bool, method: str, oos_test: dict | None,
                       n_oos_periods: int, notes: str | None = None) -> dict:
    return {"calibrated": bool(calibrated), "method": method, "oos_test": oos_test,
            "n_oos_periods": int(n_oos_periods), "notes": notes}
