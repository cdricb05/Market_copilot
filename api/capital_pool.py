r"""api/capital_pool.py - Release 50: the ONE multi-asset capital pool (read model).

The entire operational portfolio is ONE pool of capital. Cash, the equity
holdings, and every eligible futures / FX / crypto position live in the same
Alpha Paper Book #1, valued by the same NAV replay (``api.paper_trading_desk.book_nav``,
extended with the position contract). There is no second operational portfolio
per asset class; sub-ledgers exist only as attribution (the asset-class / sleeve /
currency breakdown below).

This module COMPOSES the ONE NAV owner's facts into the capital-pool contract:

    starting NAV, cash, available (free) capital, invested capital,
    gross exposure, net exposure, capital usage, collateral,
    asset-class exposure, sleeve exposure, currency exposure,
    and every position in the canonical contract.

It computes no valuation of its own - every dollar is the desk's - and it writes
nothing. It exists so that "how much capital is deployed where" has exactly one
answer on every surface.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
OWNER = "api.capital_pool"
SCHEMA_VERSION = "capital_pool.v1"
ROUTE = "/v1/operations/capital-pool"
NAV_OWNER = "api.paper_trading_desk.book_nav"


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _r2(x):
    return None if x is None else round(float(x), 2)


def positions_from_state(portfolio_state: dict) -> list[dict]:
    """The position contracts of the canonical portfolio state (pure projection).
    Each row is already valued by the desk; this only re-labels the keys the
    multi-asset owners read (``instrument_id`` / ``exposure_weight`` ...)."""
    out = []
    for p in (portfolio_state or {}).get("positions") or []:
        if not isinstance(p, dict) or not p.get("ticker"):
            continue
        it = p.get("instrument_type") or ic.IT_CASH_EQUITY
        mv = _f(p.get("market_value"))
        notional = _f(p.get("notional_usd")) if p.get("notional_usd") is not None else mv
        out.append({
            "instrument_id": p["ticker"], "label": p.get("label") or p["ticker"],
            "sleeve_id": p.get("sleeve_id") or ic.DEFAULT_EQUITY_SLEEVE,
            "asset_class": p.get("asset_class") or ic.AC_US_EQUITY,
            "asset_class_label": ic.ASSET_CLASS_LABELS.get(p.get("asset_class") or ic.AC_US_EQUITY),
            "instrument_type": it, "sector": p.get("sector"),
            "quantity": p.get("quantity"), "unit_type": p.get("unit_type") or ic.UNIT_TYPE.get(it),
            "mark": _f(p.get("price")), "currency": p.get("currency") or ic.REPORTING_CURRENCY,
            "multiplier": _f(p.get("multiplier")) or 1.0, "fx_to_usd": _f(p.get("fx_to_usd")) or 1.0,
            "market_value_usd": mv, "notional_usd": notional,
            "capital_usage_usd": (_f(p.get("capital_usage_usd")) if p.get("capital_usage_usd") is not None else mv),
            "collateral_usd": _f(p.get("collateral_usd")) or 0.0,
            "unrealized_pnl_usd": _f(p.get("unrealized_pnl")),
            "exposure_weight": (_f(p.get("exposure_weight")) if p.get("exposure_weight") is not None
                                else _f(p.get("portfolio_weight"))),
            "cost_basis_usd": _f(p.get("cost_basis")),
            "risk_contribution": None, "liquidity_state": None,
            "execution_convention": p.get("execution_convention") or "NEXT_CLOSE",
        })
    return out


def build_capital_pool(*, book_id: Optional[str], valuation_date: Optional[str],
                       nav: Optional[float], cash: Optional[float],
                       starting_capital: Optional[float], positions: list,
                       valuation_contract: Optional[dict] = None) -> dict:
    """Compose the ONE capital-pool contract from the desk's own valuation facts.
    Pure: no I/O; aggregation over owner values only."""
    vc = valuation_contract or {}
    exposures = ic.aggregate_exposures(positions, nav=nav, cash=cash)
    navv, c = _f(nav), _f(cash)
    invested = sum((_f(p.get("market_value_usd")) or 0.0) for p in positions)
    collateral = _f(vc.get("collateral"))
    if collateral is None:
        collateral = _f(exposures.get("collateral_usd")) or 0.0
    free_cash = (c - collateral) if c is not None else None
    usage = _f(exposures.get("capital_usage_usd")) or 0.0
    by_class = exposures.get("by_asset_class") or {}
    cash_w = (c / navv) if (c is not None and navv) else None
    allocation = dict(by_class)
    if cash_w is not None and cash_w > 1e-9:
        allocation[ic.AC_CASH] = round(cash_w, 6)
    allocation = {k: v for k, v in sorted(allocation.items(), key=lambda kv: (-(kv[1] or 0.0), kv[0]))}
    return {
        "schema_version": SCHEMA_VERSION, "phase": PHASE, "owner": OWNER,
        "nav_owner": NAV_OWNER, "reporting_currency": ic.REPORTING_CURRENCY,
        "book_id": book_id, "valuation_date": valuation_date,
        "one_capital_pool": True,
        "starting_nav": _r2(starting_capital), "nav": _r2(navv), "cash": _r2(c),
        "collateral": _r2(collateral), "available_capital": _r2(free_cash),
        "invested_capital": _r2(invested),
        "capital_usage": _r2(usage),
        "gross_exposure_usd": _r2(sum((_f(p.get("notional_usd")) or 0.0) for p in positions)),
        "gross_exposure": exposures.get("gross_exposure"),
        "net_exposure": exposures.get("net_exposure"),
        "cash_weight": exposures.get("cash_weight"),
        "collateral_weight": exposures.get("collateral_weight"),
        "asset_class_exposure": by_class,
        "sleeve_exposure": exposures.get("by_sleeve") or {},
        "currency_exposure": exposures.get("by_currency") or {},
        "non_usd_exposure": exposures.get("non_usd_exposure"),
        "allocation": allocation,
        "allocation_labels": {k: ic.ASSET_CLASS_LABELS.get(k, k) for k in allocation},
        "asset_classes_present": sorted(by_class),
        "position_count": len(positions),
        "non_equity_position_count": sum(1 for p in positions
                                         if p.get("instrument_type") != ic.IT_CASH_EQUITY),
        "unpriced_positions": exposures.get("unpriced_positions"),
        "futures_notional": _r2(_f(vc.get("futures_notional"))),
        "futures_unrealized_pnl": _r2(_f(vc.get("futures_unrealized_pnl"))),
        "positions": positions,
        "position_contract_version": vc.get("position_contract_version") or ic.SCHEMA_VERSION,
        "semantics": {
            "nav": "cash + sum(market_value_usd) where a future's market value is its unrealised variation",
            "collateral": ic.COLLATERAL_SEMANTICS[ic.IT_FUTURE],
            "available_capital": "cash - collateral (encumbered margin is not spendable)",
            "exposure_weight": "notional_usd / nav (a future enters at its notional)",
            "long_only": True, "sub_ledgers": "attribution only; never separate portfolios",
            "cash_return_policy": ic.CASH_RETURN_POLICY,
        },
        "safety": ic.safety_block(),
    }


def load_capital_pool(*, portfolio_state: Optional[dict] = None) -> dict:
    """The GET read model. Read-only; composes the canonical portfolio state
    (which composes the ONE NAV owner) and nothing else."""
    if portfolio_state is None:
        from paper_trader.api import portfolio_state as _ps
        portfolio_state = _ps.load_portfolio_state()
    cp = (portfolio_state or {}).get("capital_pool")
    if isinstance(cp, dict) and cp.get("owner") == OWNER:
        out = dict(cp)
    else:
        ps = portfolio_state or {}
        cap = ps.get("capital") or {}
        out = build_capital_pool(
            book_id=(ps.get("active_book") or {}).get("book_id"),
            valuation_date=(ps.get("dates") or {}).get("valuation_date"),
            nav=cap.get("nav"), cash=cap.get("cash"),
            starting_capital=cap.get("initial_capital"),
            positions=positions_from_state(ps),
            valuation_contract=ps.get("valuation_contract"))
    out["generated_at"] = datetime.now(timezone.utc).isoformat()
    out["portfolio_state_hash"] = (portfolio_state or {}).get("state_hash")
    out["economic_state_hash"] = (portfolio_state or {}).get("economic_state_hash")
    out["route"] = ROUTE
    return out


__all__ = ["PHASE", "OWNER", "SCHEMA_VERSION", "ROUTE", "NAV_OWNER",
           "positions_from_state", "build_capital_pool", "load_capital_pool"]
