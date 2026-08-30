r"""api/opportunity_frontier.py - Release 50: composition + read owner of the ONE
cross-asset opportunity frontier.

Sources (never computes):

* equity rankings           -> ``api.universe_scoring`` (the approved model's percentiles)
* sleeve eligibility        -> ``api.investability_registry`` (the ONE registry)
* non-equity descriptors    -> registry + ``api.market_reference_data`` (owned metadata / marks)
* current positions + NAV   -> ``api.portfolio_state`` (position contracts)
* risk inputs               -> ``api.cross_asset_risk`` (the ONE risk state)
* calibrated expected return-> ``api.return_forecast`` operational lane ONLY when calibrated

and runs ``engine.opportunity_frontier.build_frontier`` once. It also owns the
frontier REVIEW of non-equity holdings (the equity holdings are reviewed by the
Holding Opportunity-Cost owner): a non-equity position whose sleeve is no longer
capital-eligible is a mandatory exit; an eligible one is retained and re-sized by
the frontier's own score. Read-only; writes nothing; promotes nothing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import investability_registry as ir
from paper_trader.engine import instrument_contract as ic
from paper_trader.engine import opportunity_frontier as kernel

PHASE = "R50"
OWNER = "api.opportunity_frontier"
ROUTE = "/v1/operations/opportunity-frontier"

REVIEW_OWNER = "api.opportunity_frontier (non-equity holdings)"


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def frontier_reviews(frontier: dict, positions: list) -> list[dict]:
    """HOC-shaped review rows for NON-EQUITY holdings (the shape the proposal
    kernel already consumes). Eligibility-based: an ineligible sleeve's position
    is a mandatory EXIT; an eligible one is HOLD (its size is then set by the
    frontier score through the same allocation passes as every other name)."""
    rows = {r["instrument_id"]: r for r in (frontier or {}).get("rows") or []}
    out = []
    for p in positions or []:
        if p.get("instrument_type") in (None, ic.IT_CASH_EQUITY, ic.IT_CASH):
            continue
        tk = p.get("instrument_id")
        r = rows.get(tk) or {}
        eligible = bool(r.get("eligible"))
        out.append({
            "ticker": tk, "recommendation": "HOLD" if eligible else "EXIT",
            "current_rank": r.get("rank"), "current_score": r.get("opportunity_score"),
            "signal_strength": r.get("opportunity_score"),
            "strongest_replacement_ticker": None, "replacement_rank": None,
            "replacement_score": None, "gross_score_improvement": None,
            "net_improvement": None, "switching_cost_usd": None,
            "deterioration_state": "ELIGIBLE" if eligible else "SLEEVE_INELIGIBLE",
            "drawdown_60d": None, "volatility_60d": r.get("volatility_annualised"),
            "liquidity_state": r.get("liquidity_state"),
            "risk_contribution_pct": r.get("risk_contribution"),
            "review_owner": REVIEW_OWNER,
            "reason_codes": ([] if eligible else [r.get("eligibility_reason") or "SLEEVE_NOT_CAPITAL_ELIGIBLE"]),
        })
    return out


def load_opportunity_frontier(*, portfolio_state: Optional[dict] = None,
                              scoring: Optional[dict] = None,
                              registry: Optional[dict] = None,
                              risk_state: Optional[dict] = None,
                              approvals: Optional[dict] = None,
                              expected_returns: Optional[dict] = None,
                              policy: Optional[dict] = None,
                              probe: bool = True) -> dict:
    """The GET read model. Read-only, degrade-safe. ``approvals`` is the hermetic
    injection seam of the registry (never a production input)."""
    from paper_trader.api import capital_pool as cp
    if portfolio_state is None:
        from paper_trader.api import portfolio_state as _ps
        portfolio_state = _ps.load_portfolio_state()
    ps = portfolio_state or {}
    cap = ps.get("capital") or {}
    nav = _f(cap.get("nav"))
    as_of = (ps.get("dates") or {}).get("eligible_market_date")
    if scoring is None:
        try:
            from paper_trader.api import universe_scoring as us
            scoring = us.load_universe_scoring()
        except Exception:  # noqa: BLE001
            scoring = {"rankings": []}
    if registry is None:
        registry = ir.load_investability_registry(approvals=approvals, probe=probe,
                                                  as_of=as_of, nav=nav)
    pol = {"max_name_weight": 0.10, "min_adv_dollar": 1.0e7, "max_adv_participation": 1.0}
    try:
        from paper_trader.api import multi_horizon_engine as eng
        pol.update({"max_name_weight": float(eng.MAX_INDIVIDUAL_WEIGHT),
                    "min_adv_dollar": float(eng.MIN_ADV_DOLLAR)})
    except Exception:  # noqa: BLE001
        pass
    if policy:
        pol.update(policy)
    positions = cp.positions_from_state(ps)
    sm = ir.sleeve_map(registry)
    equity_ok = bool((sm.get(ic.DEFAULT_EQUITY_SLEEVE) or {}).get("capital_eligible"))
    try:
        instruments = ir.eligible_non_equity_instruments(
            registry, nav=nav, as_of=as_of, max_name_weight=float(pol["max_name_weight"]))
    except Exception:  # noqa: BLE001
        instruments = []
    fr = kernel.build_frontier(
        eligible_market_date=as_of, nav=nav,
        equity_rankings=list((scoring or {}).get("rankings") or []),
        equity_sleeve_eligible=equity_ok, non_equity_instruments=instruments,
        positions=positions, risk_state=risk_state, expected_returns=expected_returns,
        policy=pol)
    fr.update({
        "owner": OWNER, "route": ROUTE,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "registry_identity": {"capital_eligible_sleeve_ids": registry.get("capital_eligible_sleeve_ids"),
                              "non_equity_eligible_sleeve_ids": registry.get("non_equity_eligible_sleeve_ids"),
                              "approvals_injected": registry.get("approvals_injected")},
        "universe_scoring_hash": (scoring or {}).get("output_hash"),
        "portfolio_state_hash": ps.get("state_hash"),
        "economic_state_hash": ps.get("economic_state_hash"),
        "risk_state_hash": (risk_state or {}).get("risk_state_hash"),
        "non_equity_reviews": frontier_reviews(fr, positions),
        "candidate_rows_for_proposal": kernel.candidate_rows_for_proposal(fr),
    })
    return fr


__all__ = ["PHASE", "OWNER", "ROUTE", "REVIEW_OWNER", "frontier_reviews",
           "load_opportunity_frontier"]
