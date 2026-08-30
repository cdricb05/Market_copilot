r"""engine/cross_asset_risk.py - Release 50: the ONE cross-asset risk state (pure).

One risk representation for the whole capital pool - equities, futures, FX, cash -
built on the canonical covariance owner the equity book already uses
(``engine.holding_opportunity_cost.build_covariance``; never a second covariance
implementation) over EXPOSURE weights (notional / NAV), which is the only basis on
which a $50-point-value contract and a share can sit in the same matrix.

It owns: total volatility, covariance / correlation over the included instruments,
marginal risk and risk contribution, gross / net exposure, asset-class / sleeve /
currency exposure, factor-proxy and single-instrument concentration, liquidity /
capacity, and the drawdown it is HANDED by the canonical drawdown owner (it never
recomputes a drawdown). Every approximation is labelled; the numbers remain usable
by portfolio construction because a labelled approximation is honest and a blank
is not.

Pure stdlib: no I/O, no clock, no provider, no write.
"""
from __future__ import annotations

import hashlib
import json
import math
from typing import Any, Optional

from paper_trader.engine import holding_opportunity_cost as hoc_kernel
from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
CALCULATION_OWNER = "engine.cross_asset_risk"
SCHEMA_VERSION = "cross_asset_risk.v1"

STATE_AVAILABLE = "AVAILABLE"
STATE_PARTIAL = "PARTIAL_COVERAGE"
STATE_UNAVAILABLE = "UNAVAILABLE"
STATE_VOCAB = (STATE_AVAILABLE, STATE_PARTIAL, STATE_UNAVAILABLE)

_TRADING_DAYS_YEAR = 252.0

APPROXIMATIONS = {
    "covariance_basis": "SAMPLE_COVARIANCE_OF_DAILY_RETURNS_OVER_THE_POLICY_LOOKBACK",
    "weights_basis": "NOTIONAL_EXPOSURE_OVER_NAV (a future enters at its notional, not its margin)",
    "futures_returns_basis": "CONTINUOUS_SERIES_PRICE_RETURNS (roll-adjusted owned series)",
    "fx_translation": "UNHEDGED_USD_TRANSLATION (non-USD contract returns are in local terms; "
                      "currency exposure is reported separately, not folded into the covariance)",
    "cash": "RISKLESS_BY_DECLARATION (zero variance, zero covariance)",
    "factor_concentration": "ASSET_CLASS_PROXY (no estimated factor model; asset labels are "
                            "grouping proxies, never risk factors)",
    "precision": "CONSERVATIVE_LABELLED_APPROXIMATION - usable for construction, never "
                 "presented as a forecast",
}


def default_policy() -> dict[str, Any]:
    return {
        "covariance_lookback": 60,          # reused: Slice-6 lookback
        "min_covariance_obs": 40,           # reused: Slice-6 minimum
        "covariance_variance_floor": 1.0e-12,
        "min_risk_coverage": 0.80,          # reused: Slice-7 volatility coverage floor
        "liquidity_participation_rate": 0.10,
        "liquidity_days_liquid_max": 1.0,
        "liquidity_days_moderate_max": 5.0,
    }


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


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(",", ":"),
                                     default=str).encode("utf-8")).hexdigest()[:32]


# --------------------------------------------------------------------------- #
# Covariance + portfolio risk over exposure weights
# --------------------------------------------------------------------------- #
def portfolio_risk(*, weights: dict, aligned_returns: dict, policy: dict) -> dict:
    """Total risk, marginal risk and contributions over the covariance-included
    instruments. Reuses the ONE covariance builder; never forks it."""
    w = {k: (_f(v) or 0.0) for k, v in (weights or {}).items() if (_f(v) or 0.0) > 0}
    if not w:
        return {"state": STATE_UNAVAILABLE, "reason": "NO_EXPOSURE"}
    built = hoc_kernel.build_covariance(tickers=sorted(w), aligned_returns=aligned_returns or {},
                                        policy=policy)
    included = list(built.get("included_tickers") or [])
    cov = built.get("covariance") or {}
    if not included:
        return {"state": STATE_UNAVAILABLE, "reason": "COVARIANCE_UNAVAILABLE",
                "excluded": built.get("excluded_tickers") or [],
                "observations_used": built.get("observations_used", 0)}
    total_w = sum(w.values())
    covered_w = sum(w[t] for t in included)
    coverage = covered_w / total_w if total_w > 0 else 0.0
    sw = {i: sum((cov.get(i) or {}).get(j, 0.0) * w[j] for j in included) for i in included}
    var_d = sum(w[i] * sw[i] for i in included)
    if var_d <= float(policy.get("covariance_variance_floor", 1e-12)):
        return {"state": STATE_UNAVAILABLE, "reason": "VARIANCE_BELOW_FLOOR",
                "coverage": _r(coverage, 4), "included": included}
    sd_d = math.sqrt(var_d)
    vol_ann = sd_d * math.sqrt(_TRADING_DAYS_YEAR)
    contributions = {i: (w[i] * sw[i] / var_d) for i in included}
    marginal = {i: (sw[i] / sd_d) * math.sqrt(_TRADING_DAYS_YEAR) for i in included}
    stand_vol = {i: math.sqrt(max(0.0, (cov.get(i) or {}).get(i, 0.0))) * math.sqrt(_TRADING_DAYS_YEAR)
                 for i in included}
    weighted_avg_vol = sum(w[i] * stand_vol[i] for i in included) / covered_w if covered_w else None
    div_ratio = (weighted_avg_vol / (vol_ann * (covered_w / total_w))
                 if (weighted_avg_vol and vol_ann and total_w) else None)
    corr = {}
    for i in included:
        corr[i] = {}
        for j in included:
            si, sj = stand_vol[i], stand_vol[j]
            cij = (cov.get(i) or {}).get(j, 0.0) * _TRADING_DAYS_YEAR
            corr[i][j] = _r(cij / (si * sj), 4) if (si > 0 and sj > 0) else None
    state = STATE_AVAILABLE if coverage >= float(policy.get("min_risk_coverage", 0.8)) else STATE_PARTIAL
    return {
        "state": state,
        "total_volatility_annualised": _r(vol_ann, 6),
        "portfolio_variance_daily": var_d,
        "coverage": _r(coverage, 4),
        "included": included,
        "excluded": built.get("excluded_tickers") or [],
        "observations_used": built.get("observations_used", 0),
        "risk_contribution": {k: _r(v, 6) for k, v in sorted(contributions.items())},
        "marginal_risk_annualised": {k: _r(v, 6) for k, v in sorted(marginal.items())},
        "standalone_volatility_annualised": {k: _r(v, 6) for k, v in sorted(stand_vol.items())},
        "diversification_ratio": _r(div_ratio, 4),
        "correlation": corr,
        "_sigma_w_daily": sw,
        "_sd_daily": sd_d,
    }


def diversification_effect(*, risk: dict, candidate: str, aligned_returns: dict,
                           weights: dict, delta_weight: float, policy: dict) -> dict:
    """ADVISORY: the marginal change in portfolio volatility from adding
    ``delta_weight`` of ``candidate`` (first order, then re-measured exactly).
    Reported for the frontier; it drives no decision unless a calibrated risk
    price exists, because trading a validated score point against a variance unit
    without a calibrated price would be false precision."""
    base_vol = _f((risk or {}).get("total_volatility_annualised"))
    w = {k: (_f(v) or 0.0) for k, v in (weights or {}).items()}
    w2 = dict(w)
    w2[candidate] = w2.get(candidate, 0.0) + float(delta_weight)
    after = portfolio_risk(weights=w2, aligned_returns=aligned_returns, policy=policy)
    after_vol = _f(after.get("total_volatility_annualised"))
    return {
        "candidate": candidate, "delta_weight": _r(delta_weight, 6),
        "volatility_before": _r(base_vol, 6), "volatility_after": _r(after_vol, 6),
        "volatility_change": _r((after_vol - base_vol) if (after_vol is not None and base_vol is not None) else None, 6),
        "diversifies": (bool(after_vol < base_vol) if (after_vol is not None and base_vol is not None) else None),
        "advisory_only": True, "basis": "EXACT_REMEASUREMENT_ON_THE_CANONICAL_COVARIANCE",
        "state": after.get("state"),
    }


# --------------------------------------------------------------------------- #
# The risk state
# --------------------------------------------------------------------------- #
def build_risk_state(*, positions: list, aligned_returns: dict, nav: Optional[float],
                     cash: Optional[float] = None, drawdown: Optional[dict] = None,
                     liquidity: Optional[dict] = None, policy: Optional[dict] = None,
                     as_of: Optional[str] = None) -> dict:
    """The ONE cross-asset risk state of a list of position contracts.

    ``positions`` are ``engine.instrument_contract.value_position`` rows (or any rows
    carrying ``instrument_id`` / ``exposure_weight`` / ``asset_class`` / ``sleeve_id`` /
    ``currency``). ``drawdown`` is the canonical owner's block, passed through
    verbatim. ``liquidity`` is ``{instrument_id: {"days_to_liquidate": ..}}``.
    """
    pol = dict(default_policy())
    if policy:
        pol.update(policy)
    pos = [p for p in (positions or []) if isinstance(p, dict) and p.get("instrument_id")]
    weights = {p["instrument_id"]: (_f(p.get("exposure_weight")) or 0.0) for p in pos
               if (_f(p.get("exposure_weight")) or 0.0) > 0}
    exposures = ic.aggregate_exposures(pos, nav=nav, cash=cash)
    risk = portfolio_risk(weights=weights, aligned_returns=aligned_returns, policy=pol)

    # concentration: single instrument and asset-class proxy
    ws = sorted(weights.values(), reverse=True)
    gross = sum(ws) if ws else 0.0
    hhi = sum((w / gross) ** 2 for w in ws) if gross > 0 else None
    top = ws[0] if ws else None
    by_class = exposures.get("by_asset_class") or {}
    class_hhi = (sum((v / gross) ** 2 for v in by_class.values()) if gross > 0 else None)

    # liquidity / capacity
    liq_rows = []
    unknown_liq = 0
    for p in pos:
        tk = p["instrument_id"]
        lq = (liquidity or {}).get(tk) or {}
        days = _f(lq.get("days_to_liquidate"))
        if days is None:
            days = _f(p.get("days_to_liquidate"))
        state = hoc_kernel.liquidity_state(days, pol) if days is not None else "UNAVAILABLE"
        if state == "UNAVAILABLE":
            unknown_liq += 1
        liq_rows.append({"instrument_id": tk, "days_to_liquidate": _r(days, 3),
                         "liquidity_state": state})
    worst_days = max((r["days_to_liquidate"] for r in liq_rows if r["days_to_liquidate"] is not None),
                     default=None)

    contrib = risk.get("risk_contribution") or {}
    by_class_contrib: dict[str, float] = {}
    by_sleeve_contrib: dict[str, float] = {}
    for p in pos:
        c = contrib.get(p["instrument_id"])
        if c is None:
            continue
        by_class_contrib[p.get("asset_class") or "UNKNOWN"] = by_class_contrib.get(p.get("asset_class") or "UNKNOWN", 0.0) + c
        by_sleeve_contrib[p.get("sleeve_id") or "UNKNOWN"] = by_sleeve_contrib.get(p.get("sleeve_id") or "UNKNOWN", 0.0) + c

    max_contrib = max(contrib.values()) if contrib else None
    body = {
        "schema_version": SCHEMA_VERSION, "phase": PHASE, "calculation_owner": CALCULATION_OWNER,
        "as_of": as_of, "state": risk.get("state"), "state_vocabulary": list(STATE_VOCAB),
        "nav": _r(_f(nav), 2),
        "position_count": len(pos),
        "total_volatility_annualised": risk.get("total_volatility_annualised"),
        "covariance_coverage": risk.get("coverage"),
        "covariance_included": risk.get("included") or [],
        "covariance_excluded": risk.get("excluded") or [],
        "covariance_observations": risk.get("observations_used"),
        "risk_contribution": contrib,
        "marginal_risk_annualised": risk.get("marginal_risk_annualised") or {},
        "standalone_volatility_annualised": risk.get("standalone_volatility_annualised") or {},
        "correlation": risk.get("correlation") or {},
        "diversification_ratio": risk.get("diversification_ratio"),
        "exposure": {k: v for k, v in exposures.items()},
        "gross_exposure": exposures.get("gross_exposure"),
        "net_exposure": exposures.get("net_exposure"),
        "asset_class_exposure": by_class,
        "sleeve_exposure": exposures.get("by_sleeve") or {},
        "currency_exposure": exposures.get("by_currency") or {},
        "non_usd_exposure": exposures.get("non_usd_exposure"),
        "collateral_weight": exposures.get("collateral_weight"),
        "risk_contribution_by_asset_class": {k: _r(v, 6) for k, v in sorted(by_class_contrib.items())},
        "risk_contribution_by_sleeve": {k: _r(v, 6) for k, v in sorted(by_sleeve_contrib.items())},
        "concentration": {
            "herfindahl": _r(hhi, 6),
            "largest_instrument_weight": _r(top, 6),
            "largest_risk_contribution": _r(max_contrib, 6),
            "asset_class_herfindahl": _r(class_hhi, 6),
            "factor_concentration_basis": APPROXIMATIONS["factor_concentration"],
        },
        "liquidity": {"rows": liq_rows, "worst_days_to_liquidate": worst_days,
                      "unknown_count": unknown_liq,
                      "participation_rate": pol["liquidity_participation_rate"]},
        "drawdown": dict(drawdown or {"state": "NOT_SUPPLIED"}),
        "drawdown_owner": (drawdown or {}).get("owner") or "api.paper_trading_desk.current_drawdown",
        "approximations": dict(APPROXIMATIONS),
        "policy": pol,
        "risk_unavailable_reason": risk.get("reason"),
        "safety": ic.safety_block(),
    }
    body["risk_state_hash"] = stable_hash({k: v for k, v in body.items()
                                           if k not in ("safety", "correlation")})
    return body


__all__ = ["PHASE", "CALCULATION_OWNER", "SCHEMA_VERSION", "STATE_VOCAB", "APPROXIMATIONS",
           "default_policy", "portfolio_risk", "diversification_effect", "build_risk_state",
           "stable_hash"]
