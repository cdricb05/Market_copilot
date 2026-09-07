r"""api/cash_deployment_frontier.py - Release 56: the composition and read owner
of the CASH DEPLOYMENT FRONTIER and the INCUMBENT OPPORTUNITY COST.

It owns no mathematics. Every number comes from an existing canonical owner:

* eligible universe / sector / liquidity / rank -> ``api.universe_scoring``
* NAV, cash, available capital, holdings       -> ``api.portfolio_state`` /
                                                  ``api.capital_pool``
* expected excess return / uncertainty         -> ``api.return_forecast``
* trailing returns                             -> ``api.price_panel``
* covariance at the policy horizon             -> ``engine.zero_base_allocator
                                                  .horizon_covariance``
* construction caps and risk prices            -> ``api.multi_horizon_engine`` /
                                                  the zero-base policy
* transaction cost rate                        -> ``api.paper_trading_desk``
* the objective, the targets and the transition arithmetic
                                               -> ``engine.zero_base_allocator``
* the ladder, the payback horizon and the hurdle
                                               -> ``engine.alpha_capital_frontier``

WHAT IT ADDS
------------
The allocator answers "which portfolio". This owner answers the question an
allocator never asks: *how many dollars of it, and does the NEXT dollar pay for
itself?* It walks a ladder of capital increments - $1,000, $2,500, $5,000, 5%,
10%, 25% and 100% of NAV - and for each one reports the destination, the
expected gain, the incremental risk and concentration, the transaction cost,
the liquidity participation, the turnover, whether the economic hurdle clears,
and - when it does not - why CASH won.

TWO LANES, NEVER BLURRED
------------------------
The utility ladder is computed from the Release-30 research forecast, which is
NOT the approved operational model and is NOT activated. It is EVIDENCE. The
GOVERNED lane is reported beside it and states plainly that no calibrated
expected return exists operationally, so a governed cash deployment today rests
on ordering evidence and a human, not on a proven economic hurdle.

Read-only. It is not a proposal (``engine.reallocation_proposal`` remains the
one proposal owner) and not a decision (``api.portfolio_decision`` remains the
one decision owner). It creates no signal, target, order plan, order or fill;
it changes no holding, cash or NAV; it promotes no model, activates no sleeve
and enables no automation.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.engine import alpha_capital_frontier as kernel
from paper_trader.engine import zero_base_allocator as zba

SCHEMA_VERSION = "cash_deployment_frontier.v1"
COMPOSITION_OWNER = "api.cash_deployment_frontier"
CALCULATION_OWNER = kernel.CALCULATION_OWNER
PHASE = "R56"
ROUTE = "/v1/operations/cash-deployment-frontier"

STATE_READY = "READY"
STATE_BLOCKED = "BLOCKED"
STATE_UNAVAILABLE = "UNAVAILABLE"
READ_STATE_VOCAB = (STATE_READY, STATE_BLOCKED, STATE_UNAVAILABLE)

SAFETY_BADGES = list(kernel.SAFETY_BADGES)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def weights_from_rows(rows: Optional[list]) -> dict:
    """The weight map a target's published rows already carry (pure projection).

    Reading the weights back off the allocator's own rows is deliberate: it
    guarantees the ladder prices exactly the portfolio the operator is shown,
    not a re-solved approximation of it.
    """
    out: dict = {}
    for r in rows or []:
        tk = (r or {}).get("ticker")
        w = _f((r or {}).get("weight"))
        if tk and w is not None and w > 0:
            out[str(tk)] = w
    return out


def candidate_meta(*, scoring: Optional[dict], current_weights: dict,
                   frontier: Optional[dict] = None) -> dict:
    """Per-instrument descriptors the ladder reports beside a destination.

    Sector, liquidity and rank belong to the scoring owner; sleeve and asset
    class to the frontier owner. Nothing is computed here.
    """
    meta: dict = {}
    for r in (scoring or {}).get("rankings") or []:
        tk = r.get("ticker")
        if not tk:
            continue
        meta[str(tk)] = {
            "sector": r.get("sector") or "Unknown",
            "adv_dollar": _f(r.get("adv_dollar")),
            "rank": r.get("rank"),
            "percentile": _f(r.get("percentile")),
            "destination_kind": (kernel.DEST_EXISTING_HOLDING
                                 if tk in current_weights
                                 else kernel.DEST_NEW_EQUITY),
        }
    for r in (frontier or {}).get("rows") or []:
        tk = r.get("instrument_id")
        if not tk:
            continue
        m = meta.setdefault(str(tk), {})
        m.setdefault("sector", r.get("sector"))
        m.setdefault("adv_dollar", _f(r.get("liquidity_adv_dollar")))
        m.setdefault("rank", r.get("rank"))
        m["sleeve_id"] = r.get("sleeve_id")
        m["asset_class"] = r.get("asset_class")
        if r.get("asset_class") and r.get("asset_class") not in ("US_EQUITY", "CASH"):
            m["destination_kind"] = kernel.DEST_OTHER_SLEEVE
        elif "destination_kind" not in m:
            m["destination_kind"] = (kernel.DEST_EXISTING_HOLDING
                                     if tk in current_weights
                                     else kernel.DEST_NEW_EQUITY)
    return meta


def _governed_destinations(hoc: Optional[dict], frontier: Optional[dict],
                           current_weights: dict, entry_rank: Optional[int]) -> list:
    """Eligible, not-currently-held destinations ordered by the APPROVED model's
    own ranking. Ordering evidence only - it proves no expected return."""
    out = []
    for a in (hoc or {}).get("addition_candidates") or []:
        tk = a.get("ticker")
        if not tk:
            continue
        out.append({"instrument_id": tk, "rank": a.get("rank"),
                    "score": a.get("score"), "sector": a.get("sector"),
                    "source": "api.holding_opportunity_cost.addition_candidates",
                    "reason_codes": a.get("reason_codes") or []})
    if out:
        return out
    lim = int(entry_rank or 25)
    for r in (frontier or {}).get("rows") or []:
        tk = r.get("instrument_id")
        rk = r.get("rank")
        if (not tk or tk in current_weights or not r.get("eligible")
                or rk is None or int(rk) > lim):
            continue
        out.append({"instrument_id": tk, "rank": rk,
                    "score": r.get("opportunity_score"), "sector": r.get("sector"),
                    "source": "api.opportunity_frontier.rows",
                    "reason_codes": ["ELIGIBLE_WITHIN_ENTRY_RANK_NOT_HELD"]})
    out.sort(key=lambda r: (r.get("rank") if r.get("rank") is not None else 10 ** 9))
    return out


def run_frontier(*, portfolio_state: Optional[dict] = None,
                 scoring: Optional[dict] = None,
                 forecast: Optional[dict] = None,
                 price_panel: Optional[dict] = None,
                 artifact: Optional[dict] = None,
                 opportunity_frontier: Optional[dict] = None,
                 holding_opportunity_cost: Optional[dict] = None,
                 capital_pool: Optional[dict] = None,
                 increments: Optional[list] = None,
                 policy_overrides: Optional[dict] = None) -> dict:
    """Compute the frontier. PURE with respect to state - no write, no mutation."""
    from paper_trader.api import portfolio_state as ps_owner
    from paper_trader.api import return_forecast as rfc
    from paper_trader.api import universe_scoring as us
    from paper_trader.api import zero_base_target as zbt

    ps = portfolio_state if portfolio_state is not None else ps_owner.load_portfolio_state()
    sc = scoring if scoring is not None else us.load_universe_scoring()
    art = artifact if artifact is not None else rfc.load_model_artifact()
    fc = forecast if forecast is not None else rfc.build(artifact=art)
    pol = zbt.resolve_policy(artifact=art, policy_overrides=policy_overrides)
    horizon = int(pol["policy_horizon_sessions"])

    # ONE input contract, ONE allocator run, ONE covariance. Building the
    # contract twice would load the price panel twice and, worse, would let the
    # ladder price a portfolio the allocator never produced.
    ic = zbt.build_input_contract(portfolio_state=ps, scoring=sc, forecast=fc,
                                  price_panel=price_panel, policy=pol)
    alloc = zba.build_allocation(input_contract=ic, policy=pol)
    if alloc.get("state") in (zba.STATE_BLOCKED, zba.STATE_NO_ACTIVE_BOOK):
        return _blocked(alloc, pol, ic, fc)

    cur_w = {k: v for k, v in (ic.get("current_weights") or {}).items()
             if _f(v) and _f(v) > 0}
    zb_w = weights_from_rows((alloc.get("zero_base_target") or {}).get("rows"))
    impl_w = weights_from_rows((alloc.get("implementable_target") or {}).get("rows"))

    caps = zba.name_caps(candidates=[c for c in ic.get("candidates") or []
                                     if c.get("ticker") in (ic.get("mu") or {})],
                         nav=_f(ic.get("nav")) or 0.0, policy=pol)
    cov = zba.horizon_covariance(tickers=sorted(set(caps) | set(cur_w)),
                                 aligned_returns=ic.get("aligned_returns") or {},
                                 policy=pol, horizon=horizon)
    mu = {k: (_f(v) or 0.0) for k, v in (ic.get("mu") or {}).items()}
    sig = {k: (_f(v) or 0.0) for k, v in (ic.get("sigma_forecast") or {}).items()}

    if capital_pool is None:
        from paper_trader.api import capital_pool as cp
        capital_pool = cp.load_capital_pool(portfolio_state=ps)
    if opportunity_frontier is None:
        try:
            from paper_trader.api import opportunity_frontier as of
            opportunity_frontier = of.load_opportunity_frontier(
                portfolio_state=ps, scoring=sc)
        except Exception:                                          # noqa: BLE001
            opportunity_frontier = {}
    if holding_opportunity_cost is None:
        try:
            from paper_trader.api import holding_opportunity_cost as hoc
            holding_opportunity_cost = hoc.load_holding_opportunity_cost()
        except Exception:                                          # noqa: BLE001
            holding_opportunity_cost = {}

    meta = candidate_meta(scoring=sc, current_weights=cur_w,
                          frontier=opportunity_frontier)
    nav = _f(ic.get("nav"))
    common = dict(
        current_weights=cur_w, implementable_weights=impl_w,
        zero_base_weights=zb_w, mu=mu, sigma_forecast=sig,
        cov_h=cov["covariance_horizon"], cov_included=cov["included_tickers"],
        policy=pol, horizon=horizon, nav=nav,
        cash=_f((capital_pool or {}).get("cash")),
        available_capital=_f((capital_pool or {}).get("available_capital")),
        candidate_meta=meta, increments=increments,
        min_order_notional=_min_order_notional(),
        expected_return_state=(opportunity_frontier or {}).get("expected_return_state"))
    ladder = kernel.build_deployment_ladder(mode=kernel.MODE_CASH_ONLY, **common)
    redeploy = kernel.build_deployment_ladder(mode=kernel.MODE_REDEPLOYMENT,
                                              **common)
    opp = kernel.incumbent_opportunity_cost(
        current_weights=cur_w, zero_base_weights=zb_w,
        implementable_weights=impl_w, mu=mu, sigma_forecast=sig,
        cov_h=cov["covariance_horizon"], cov_included=cov["included_tickers"],
        policy=pol, horizon=horizon, nav=nav)

    hoc_policy = (holding_opportunity_cost or {}).get("policy") or {}
    governed = kernel.governed_capital_hurdle(
        expected_return_state=(opportunity_frontier or {}).get("expected_return_state"),
        forecast_lane=fc.get("operational_use"),
        entry_rank=hoc_policy.get("entry_rank"),
        eligible_destinations=_governed_destinations(
            holding_opportunity_cost, opportunity_frontier, cur_w,
            hoc_policy.get("entry_rank")))

    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "route": ROUTE,
        "state": STATE_READY,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "policy_horizon_sessions": horizon,
        "capital": {
            "nav": (capital_pool or {}).get("nav"),
            "cash": (capital_pool or {}).get("cash"),
            "available_capital": (capital_pool or {}).get("available_capital"),
            "invested_capital": (capital_pool or {}).get("invested_capital"),
            "cash_weight": (capital_pool or {}).get("cash_weight"),
            "starting_nav": (capital_pool or {}).get("starting_nav"),
            "position_count": (capital_pool or {}).get("position_count"),
            "asset_class_exposure": (capital_pool or {}).get("asset_class_exposure"),
            "capital_pool_owner": (capital_pool or {}).get("owner"),
        },
        "research_lane": {
            "lane": kernel.LANE_RESEARCH_UTILITY,
            "forecast_state": fc.get("state"),
            "forecast_operational_use": fc.get("operational_use"),
            "activation_state": (fc.get("activation") or {}).get("state"),
            "can_become_a_proposal": False,
            "doc": ("the ladder's expected returns come from the Release-30 "
                    "research forecast. It is not the approved operational "
                    "model and it is not activated, so every economic result "
                    "here is EVIDENCE and never an instruction."),
        },
        "governed_lane": governed,
        "deployment_ladder": ladder,
        "redeployment_ladder": redeploy,
        "two_questions": {
            "cash_deployment": ("should the cash the book is holding be put to "
                                "work? Buys only; never spends more than the "
                                "cash on hand; every existing position stays."),
            "redeployment": ("should the BOOK be rotated? Walks the allocator's "
                             "own path, funded by cash AND sales."),
            "why_separate": ("a rotation reported as a cash decision tells an "
                             "operator that new money is at work when what "
                             "actually happened is that old money moved"),
        },
        "incumbent_opportunity_cost": opp,
        "targets": {
            "zero_base_position_count": len(zb_w),
            "implementable_position_count": len(impl_w),
            "current_position_count": len(cur_w),
            "zero_base_economics": (alloc.get("zero_base_target") or {}).get("economics"),
            "implementable_economics": (alloc.get("implementable_target") or {}).get("economics"),
            "current_economics": (alloc.get("current_portfolio") or {}).get("economics"),
            "comparison": alloc.get("comparison"),
            "allocation_hash": alloc.get("allocation_hash"),
            "allocation_state": alloc.get("state"),
        },
        "policy": {k: pol.get(k) for k in (
            "policy_horizon_sessions", "max_name_weight", "sector_cap_fraction",
            "min_adv_dollar", "cost_rate_per_side", "cost_bps_per_side",
            "risk_aversion_gamma", "uncertainty_aversion_phi",
            "downside_aversion_delta", "risk_price_source",
            "min_position_weight", "max_gross_exposure")},
        "provenance": {
            "portfolio_state_hash": ic.get("portfolio_state_hash"),
            "universe_scoring_hash": ic.get("universe_scoring_hash"),
            "forecast_model_spec_hash": ic.get("forecast_model_spec_hash"),
            "feature_snapshot_hash": ic.get("feature_snapshot_hash"),
            "economic_state_hash": (ps or {}).get("economic_state_hash"),
            "opportunity_frontier_hash": (opportunity_frontier or {}).get("frontier_hash"),
            "hoc_assessment_hash": (holding_opportunity_cost or {}).get("assessment_hash"),
            "covariance_observations": cov.get("observations_used"),
            "covariance_included": len(cov.get("included_tickers") or []),
            "sources": ic.get("sources"),
        },
        "safety": _safety(),
    }


def _min_order_notional() -> float:
    try:
        from paper_trader.config import settings          # type: ignore
        v = getattr(settings, "min_order_notional", None)
        if v is not None:
            return float(v)
    except Exception:                                              # noqa: BLE001
        pass
    return float(kernel.DEFAULT_MIN_ORDER_NOTIONAL)


def _safety() -> dict:
    return {
        "badges": list(SAFETY_BADGES),
        "read_only": True, "paper_only": True, "manual_review_only": True,
        "creates_signals": False, "creates_trade_decisions": False,
        "creates_orders": False, "creates_fills": False,
        "creates_proposal": False, "creates_portfolio_target": False,
        "mutates_holdings": False, "mutates_cash": False,
        "promotes_model": False, "activates_sleeve": False,
        "enables_automation": False, "writes_operational_store": False,
        "broker_enabled": False,
        "proposal_owner": "engine.reallocation_proposal",
        "decision_owner": "api.portfolio_decision",
    }


def _blocked(alloc: dict, pol: dict, ic: dict, fc: dict) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE, "route": ROUTE,
        "state": STATE_BLOCKED,
        "state_vocabulary": list(READ_STATE_VOCAB),
        "generated_at": _now_iso(),
        "eligible_market_date": ic.get("eligible_market_date"),
        "active_book_id": ic.get("active_book_id"),
        "policy_horizon_sessions": int(pol.get("policy_horizon_sessions") or 0),
        "blockers": alloc.get("blockers") or [{"code": "ALLOCATION_BLOCKED"}],
        "research_lane": {"lane": kernel.LANE_RESEARCH_UTILITY,
                          "forecast_state": (fc or {}).get("state"),
                          "can_become_a_proposal": False},
        "deployment_ladder": {"state": kernel.STATE_BLOCKED, "rungs": []},
        "incumbent_opportunity_cost": {},
        "safety": _safety(),
    }


def load_cash_deployment_frontier(**kwargs) -> dict:
    """The GET read surface. Degrades to an explicit UNAVAILABLE payload rather
    than raising, so a missing research input can never take down the operator
    surface."""
    try:
        return run_frontier(**kwargs)
    except Exception as exc:                                       # noqa: BLE001
        return {
            "schema_version": SCHEMA_VERSION,
            "composition_owner": COMPOSITION_OWNER,
            "calculation_owner": CALCULATION_OWNER,
            "phase": PHASE, "route": ROUTE,
            "state": STATE_UNAVAILABLE,
            "state_vocabulary": list(READ_STATE_VOCAB),
            "generated_at": _now_iso(),
            "blockers": [{"code": "CASH_DEPLOYMENT_FRONTIER_UNAVAILABLE",
                          "detail": type(exc).__name__}],
            "deployment_ladder": {"state": kernel.STATE_BLOCKED, "rungs": []},
            "incumbent_opportunity_cost": {},
            "safety": _safety(),
        }


def summary(payload: Optional[dict] = None, **kwargs) -> dict:
    """Compact block for the Alpha & Capital surface and Today."""
    p = payload if payload is not None else load_cash_deployment_frontier(**kwargs)
    lad = p.get("deployment_ladder") or {}
    rdp = p.get("redeployment_ladder") or {}
    opp = p.get("incumbent_opportunity_cost") or {}
    zb_leg = (opp.get("against") or {}).get("zero_base") or {}
    impl_leg = (opp.get("against") or {}).get("implementable") or {}
    return {
        "state": p.get("state"),
        "eligible_market_date": p.get("eligible_market_date"),
        "nav": (p.get("capital") or {}).get("nav"),
        "cash": (p.get("capital") or {}).get("cash"),
        "cash_weight": (p.get("capital") or {}).get("cash_weight"),
        "n_rungs": lad.get("n_rungs"),
        "n_rungs_clearing_hurdle": lad.get("n_rungs_clearing_hurdle"),
        "all_rungs_retain_cash": lad.get("all_rungs_retain_cash"),
        "best_clearing_rung": lad.get("best_clearing_rung"),
        "deployable_capacity_usd": lad.get("deployable_capacity_usd"),
        "first_dollar_pays": (lad.get("marginal_dollar") or {}).get("first_dollar_pays"),
        "cost_share_of_marginal_gain":
            (lad.get("marginal_dollar") or {}).get("cost_share_of_marginal_gain"),
        "redeployment_rungs_clearing_hurdle": rdp.get("n_rungs_clearing_hurdle"),
        "redeployment_best_rung": rdp.get("best_clearing_rung"),
        "redeployment_best_net_gain_usd": rdp.get("best_clearing_net_gain_usd"),
        "zero_base_gap_per_horizon": zb_leg.get("utility_gap_per_horizon"),
        "zero_base_gap_dollars_annualised": zb_leg.get("utility_gap_dollars_annualised"),
        "zero_base_switch_cost_usd": zb_leg.get("switch_cost_dollars"),
        "zero_base_payback_verdict": (zb_leg.get("payback") or {}).get("verdict"),
        "zero_base_payback_sessions": (zb_leg.get("payback") or {}).get("payback_sessions"),
        "implementable_payback_verdict": (impl_leg.get("payback") or {}).get("verdict"),
        "implementable_payback_sessions":
            (impl_leg.get("payback") or {}).get("payback_sessions"),
        "governed_hurdle_state": (p.get("governed_lane") or {}).get("hurdle_state"),
        "governed_economic_proof": (p.get("governed_lane") or {}).get("economic_proof"),
    }


__all__ = ["SCHEMA_VERSION", "COMPOSITION_OWNER", "CALCULATION_OWNER", "PHASE",
           "ROUTE", "STATE_READY", "STATE_BLOCKED", "STATE_UNAVAILABLE",
           "READ_STATE_VOCAB", "SAFETY_BADGES", "weights_from_rows",
           "candidate_meta", "run_frontier", "load_cash_deployment_frontier",
           "summary"]
