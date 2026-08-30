r"""api/cross_asset_risk.py - Release 50: composition + read owner of the ONE
cross-asset risk state.

It sources - never computes - the inputs of ``engine.cross_asset_risk``:

* positions            -> the canonical portfolio state (position contracts)
* equity returns       -> ``api.price_panel.aligned_returns`` (owned point-in-time)
* non-equity returns   -> ``api.market_reference_data.daily_bars`` (owned settlements),
                          aligned on the SAME calendar
* liquidity            -> the scoring owner's ``adv_dollar`` for equities, owned
                          contract volume for futures
* drawdown             -> ``api.paper_trading_desk.current_drawdown`` (the ONE owner)

and runs the pure kernel once. Read-only; writes nothing.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import market_reference_data as mrd
from paper_trader.engine import cross_asset_risk as kernel
from paper_trader.engine import instrument_contract as ic

PHASE = "R50"
OWNER = "api.cross_asset_risk"
ROUTE = "/v1/operations/cross-asset-risk"
_RETURN_LOOKBACK = 120


def _f(x: Any) -> Optional[float]:
    try:
        return None if x is None else float(x)
    except (TypeError, ValueError):
        return None


def non_equity_returns(*, symbols: list, dates: list) -> dict:
    """Daily simple returns of owned non-equity symbols mapped onto ``dates``
    (``None`` where the instrument has no bar that day). Read-only."""
    out: dict[str, list] = {}
    if not symbols or not dates:
        return out
    start = str(dates[0])[:10]
    for s in symbols:
        try:
            bars = mrd.daily_bars(s, start=start)
        except Exception:  # noqa: BLE001
            bars = ()
        closes = {b[0]: b[1] for b in bars}
        ordered = sorted(closes)
        prev: Optional[str] = None
        rets: dict[str, float] = {}
        for d in ordered:
            if prev is not None and closes[prev] and closes[prev] > 0 and closes[d] is not None:
                rets[d] = closes[d] / closes[prev] - 1.0
            prev = d
        out[s] = [rets.get(str(d)[:10]) for d in dates]
    return out


def build_aligned_returns(*, positions: list, price_panel: Optional[dict],
                          as_of: Optional[str], extra_symbols: Optional[list] = None,
                          lookback: int = _RETURN_LOOKBACK) -> dict:
    """ONE aligned return panel over every instrument the risk state needs."""
    from paper_trader.api import price_panel as pp
    eq = sorted({p["instrument_id"] for p in positions
                 if p.get("instrument_type") in (None, ic.IT_CASH_EQUITY)}
                | {s for s in (extra_symbols or []) if not mrd.is_owned_non_equity_symbol(s)})
    ne = sorted({p["instrument_id"] for p in positions
                 if p.get("instrument_type") in (ic.IT_FUTURE, ic.IT_FX_SPOT)}
                | {s for s in (extra_symbols or []) if mrd.is_owned_non_equity_symbol(s)})
    aligned = {"dates": [], "series": {}}
    if eq and as_of:
        panel = price_panel if price_panel is not None else pp.load_operational_price_panel()
        try:
            aligned = pp.aligned_returns(price_panel=panel, tickers=eq, as_of=as_of,
                                         lookback=lookback)
        except Exception:  # noqa: BLE001
            aligned = {"dates": [], "series": {}}
    dates = list(aligned.get("dates") or [])
    if not dates and ne and as_of:
        # No equity calendar: the non-equity instruments' own union calendar.
        cal: set = set()
        for s in ne:
            for b in mrd.daily_bars(s):
                if b[0] <= str(as_of)[:10]:
                    cal.add(b[0])
        dates = sorted(cal)[-int(lookback):]
        aligned = {"dates": dates, "series": {}}
    if ne and dates:
        aligned["series"].update(non_equity_returns(symbols=ne, dates=dates))
    aligned["owners"] = {"equities": "api.price_panel.aligned_returns",
                         "non_equity": "api.market_reference_data.daily_bars"}
    return aligned


def _liquidity(positions: list, scoring: Optional[dict], policy: dict) -> dict:
    from paper_trader.engine import holding_opportunity_cost as hoc_kernel
    adv = {r.get("ticker"): _f(r.get("adv_dollar")) for r in ((scoring or {}).get("rankings") or [])}
    out = {}
    rate = float(policy.get("liquidity_participation_rate", 0.10))
    for p in positions:
        tk = p["instrument_id"]
        mv = _f(p.get("notional_usd"))
        if p.get("instrument_type") in (None, ic.IT_CASH_EQUITY):
            dv = adv.get(tk)
        else:
            try:
                units = mrd.average_daily_volume(tk)
                un = (_f(p.get("mark")) or 0.0) * float(p.get("multiplier") or 1.0) * float(p.get("fx_to_usd") or 1.0)
                dv = (units * un) if (units is not None and un) else None
            except Exception:  # noqa: BLE001
                dv = None
        out[tk] = {"days_to_liquidate": hoc_kernel.days_to_liquidate(mv, dv, rate),
                   "median_dollar_volume": dv}
    return out


def load_cross_asset_risk(*, portfolio_state: Optional[dict] = None,
                          price_panel: Optional[dict] = None,
                          scoring: Optional[dict] = None,
                          performance: Optional[dict] = None,
                          aligned_returns: Optional[dict] = None,
                          policy: Optional[dict] = None) -> dict:
    """The GET read model. Read-only, degrade-safe."""
    from paper_trader.api import capital_pool as cp
    from paper_trader.api import paper_trading_desk as desk
    if portfolio_state is None:
        from paper_trader.api import portfolio_state as _ps
        portfolio_state = _ps.load_portfolio_state()
    ps = portfolio_state or {}
    positions = cp.positions_from_state(ps)
    cap = ps.get("capital") or {}
    as_of = (ps.get("dates") or {}).get("eligible_market_date") or (ps.get("dates") or {}).get("valuation_date")
    pol = dict(kernel.default_policy())
    if policy:
        pol.update(policy)
    if aligned_returns is None:
        try:
            aligned_returns = build_aligned_returns(positions=positions, price_panel=price_panel,
                                                    as_of=as_of, lookback=_RETURN_LOOKBACK)
        except Exception:  # noqa: BLE001
            aligned_returns = {"dates": [], "series": {}}
    try:
        dd = desk.current_drawdown(performance=performance)
    except Exception:  # noqa: BLE001
        dd = {"state": "UNAVAILABLE", "owner": "api.paper_trading_desk.current_drawdown"}
    liq = _liquidity(positions, scoring, pol)
    state = kernel.build_risk_state(positions=positions, aligned_returns=aligned_returns,
                                    nav=cap.get("nav"), cash=cap.get("cash"), drawdown=dd,
                                    liquidity=liq, policy=pol, as_of=as_of)
    state.update({"owner": OWNER, "route": ROUTE,
                  "generated_at": datetime.now(timezone.utc).isoformat(),
                  "portfolio_state_hash": ps.get("state_hash"),
                  "economic_state_hash": ps.get("economic_state_hash"),
                  "return_panel_owners": (aligned_returns or {}).get("owners"),
                  "return_panel_dates": len((aligned_returns or {}).get("dates") or [])})
    return state


__all__ = ["PHASE", "OWNER", "ROUTE", "non_equity_returns", "build_aligned_returns",
           "load_cross_asset_risk"]
