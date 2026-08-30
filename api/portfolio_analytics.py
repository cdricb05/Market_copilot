"""Phase 29J.3 — Canonical read-only Portfolio Analytics aggregator.

ONE chart-ready payload for the interactive Portfolio charts and the dedicated
drill-down detail view (``/ui/analytics.html``). It COMPOSES the already-canonical
read-only sources — it introduces no new portfolio, P&L, benchmark, or return math
of its own beyond assembling series that the owners already computed plus two
standard deterministic concentration descriptors (HHI, top-5 weight):

  * performance  — ``paper_trading_desk.load_performance`` forward rows
                   (NAV, cumulative return, SPY benchmark, per-date drawdown).
  * pnl          — ``forward_evidence.load_attribution_history`` per-close rows
                   (daily P&L, cumulative P&L, daily return).
  * allocation   — ``operational_book.load_operational_book`` portfolio_summary +
                   holdings_detail (cash/invested, sector exposure, top holdings).
  * contributors — ``forward_evidence.load_holding_contributions`` contribution-aware
                   winners/losers for the latest processed close; falls back to the
                   operational book's unrealized best/worst when no processed close.
  * drift        — operational_book holdings_detail current vs target weight drift.

Contract: strictly READ-ONLY. It creates/reads no order, signal, or model; writes
nothing; mutates no ledger, portfolio, target, or model; promotes nothing. Every
section degrades to an explicit ``available: False`` empty state on any missing
input; the endpoint is always HTTP 200. Paper only; no broker; no automation.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Callable, Optional

from paper_trader.api import forward_evidence as _fe
from paper_trader.api import operational_book as _opbook
from paper_trader.api import paper_trading_desk as _desk

PHASE = "29J.3"
CANONICAL_OWNER = "api.portfolio_analytics"
CONTRACT_ID = "portfolio_analytics.v1"

STATUS_OK = "PORTFOLIO_ANALYTICS_OK"
STATUS_EMPTY = "PORTFOLIO_ANALYTICS_EMPTY"

# Chart keys shared with the UI (inline cards + detail-view ?chart= selector).
CHART_KEYS = ("performance", "pnl", "drawdown", "allocation", "contributors", "drift")


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _f(v) -> Optional[float]:
    """Coerce to a finite float or None (never raises)."""
    try:
        if v is None:
            return None
        f = float(v)
        return f if f == f and f not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def _d10(v) -> Optional[str]:
    return str(v)[:10] if v else None


def _safety() -> dict:
    """Read-only safety contract carried by the analytics payload."""
    return {
        "paper_only": True,
        "read_only": True,
        "orders_enabled": False,
        "broker_enabled": False,
        "automation_enabled": False,
        "live_orders_enabled": False,
        "performed_write": False,
        "manual_review": True,
        "safety_badges": ["READ ONLY", "PAPER ONLY", "NO ORDERS", "NO BROKER",
                          "AUTOMATION OFF", "MANUAL REVIEW"],
    }


# --------------------------------------------------------------------------- #
# Section builders (each pure; each degrades to available: False)
# --------------------------------------------------------------------------- #
def _build_performance(perf: Optional[dict]) -> dict:
    # Release 50 - the chart is a CURRENT-state surface, so it reads the desk's
    # corporate-action-corrected ``current_rows`` (the ONE drawdown owner's basis),
    # never the immutable raw rows; with an empty registry the two are identical.
    rows = list((perf or {}).get("current_rows") or (perf or {}).get("rows") or [])
    rows = [r for r in rows if isinstance(r, dict) and r.get("date")]
    rows.sort(key=lambda r: str(r.get("date")))
    points = []
    for r in rows:
        cum = _f(r.get("cumulative_return_pct"))
        spy = _f(r.get("benchmark_cumulative_return_pct"))
        points.append({
            "date": _d10(r.get("date")),
            "nav": _f(r.get("nav")),
            "cum_return_pct": cum,
            "spy_cum_return_pct": spy,
            "excess_pp": (round(cum - spy, 4) if (cum is not None and spy is not None) else None),
            "drawdown_pct": _f(r.get("drawdown_pct")),
        })
    last = points[-1] if points else None
    return {
        "available": bool(points),
        "baseline_date": (points[0]["date"] if points else None),
        "through_date": (last["date"] if last else None),
        "n_sessions": len(points),
        "points": points,
        "last": last,
        "basis": "CURRENT_ECONOMIC_STATE_FORWARD_PERFORMANCE_LEDGER",
        "drawdown_owner": "api.paper_trading_desk.current_drawdown",
        "max_drawdown_pct": _f(((perf or {}).get("current_summary")
                                or (perf or {}).get("summary") or {}).get("max_drawdown_pct")),
    }


def _build_pnl(attr: Optional[dict]) -> dict:
    rows = list((attr or {}).get("rows") or [])
    rows = [r for r in rows if isinstance(r, dict) and r.get("market_date")]
    # attribution-history returns most-recent-first; chart wants ascending.
    rows.sort(key=lambda r: str(r.get("market_date")))
    points = [{
        "date": _d10(r.get("market_date")),
        "daily_pnl": _f(r.get("daily_pnl")),
        "cumulative_pnl": _f(r.get("cumulative_pnl")),
        "daily_return_pct": _f(r.get("daily_return_pct")),
    } for r in rows]
    has_any = any(p["daily_pnl"] is not None or p["cumulative_pnl"] is not None for p in points)
    last = points[-1] if points else None
    return {
        "available": bool(points and has_any),
        "through_date": (last["date"] if last else None),
        "n_sessions": len(points),
        "points": points,
        "last": last,
    }


def _build_allocation(ob: Optional[dict], cs: Optional[dict]) -> dict:
    ps = (ob or {}).get("portfolio_summary") or {}
    hd = (ob or {}).get("holdings_detail") or []
    nav = _f((cs or {}).get("nav")) or _f((ob or {}).get("nav"))
    cash = _f(ps.get("cash"))
    if cash is None:
        cash = _f((cs or {}).get("cash")) if cs else _f((ob or {}).get("cash"))
    sectors = [{"sector": s.get("sector"), "weight": _f(s.get("weight"))}
               for s in (ps.get("sector_exposure") or []) if _f(s.get("weight")) is not None]
    sectors.sort(key=lambda s: -(s["weight"] or 0.0))
    # Top holdings by current weight from the authoritative holdings_detail.
    ranked = [h for h in hd if _f(h.get("current_weight")) is not None]
    ranked.sort(key=lambda h: -(_f(h.get("current_weight")) or 0.0))
    top_holdings = [{
        "ticker": h.get("ticker"), "sector": h.get("sector"),
        "weight": _f(h.get("current_weight")),
        "market_value": _f(h.get("market_value")),
    } for h in ranked[:10]]
    # Deterministic concentration descriptors over the invested weights.
    weights = [_f(h.get("current_weight")) or 0.0 for h in ranked]
    hhi = round(sum(w * w for w in weights), 6) if weights else None
    top5 = round(sum(weights[:5]), 6) if weights else None
    available = bool(sectors or top_holdings)
    return {
        "available": available,
        "nav": nav,
        "cash": cash,
        "cash_weight": _f(ps.get("cash_weight")),
        "invested_value": _f(ps.get("invested_value")),
        "invested_weight": _f(ps.get("invested_weight")),
        "sectors": sectors,
        "top_holdings": top_holdings,
        "hhi": hhi,
        "concentration_top5": top5,
        "n_holdings": len(ranked),
    }


def _build_contributors(contrib: Optional[dict], ob: Optional[dict]) -> dict:
    """Contribution-aware winners/losers from the latest processed close; falls back
    to the operational book's unrealized best/worst when no processed close exists."""
    holds = [h for h in ((contrib or {}).get("holdings") or [])
             if isinstance(h, dict) and _f(h.get("pnl_contribution")) is not None]
    if (contrib or {}).get("available") and holds:
        holds.sort(key=lambda h: (_f(h.get("pnl_contribution")) or 0.0))

        def _row(h):
            return {"ticker": h.get("ticker"), "sector": h.get("sector"),
                    "contribution_usd": _f(h.get("pnl_contribution")),
                    "contribution_pp": _f(h.get("contribution_pp")),
                    "return_pct": _f(h.get("daily_return_pct"))}
        top = [_row(h) for h in reversed(holds[-8:]) if (_f(h.get("pnl_contribution")) or 0.0) >= 0][:8]
        bottom = [_row(h) for h in holds[:8] if (_f(h.get("pnl_contribution")) or 0.0) < 0][:8]
        return {
            "available": bool(top or bottom),
            "source": "holding_contributions",
            "metric": "daily_pnl_contribution",
            "as_of": _d10((contrib or {}).get("market_date")),
            "prior_date": _d10((contrib or {}).get("prior_market_date")),
            "window": "daily",
            "top": top, "bottom": bottom,
        }
    # Fallback: unrealized best/worst performers (return-based, not contribution).
    ps = (ob or {}).get("portfolio_summary") or {}
    best = ps.get("best_performers") or []
    worst = ps.get("worst_performers") or []

    def _urow(r):
        return {"ticker": r.get("ticker"), "sector": r.get("sector"),
                "contribution_usd": _f(r.get("unrealized_pnl")),
                "contribution_pp": None,
                "return_pct": (_f(r.get("unrealized_pnl_pct")) * 100.0
                               if _f(r.get("unrealized_pnl_pct")) is not None else None)}
    top = [_urow(r) for r in best if _f(r.get("unrealized_pnl_pct")) is not None
           and (_f(r.get("unrealized_pnl_pct")) or 0.0) > 0][:8]
    bottom = [_urow(r) for r in worst if _f(r.get("unrealized_pnl_pct")) is not None][:8]
    return {
        "available": bool(top or bottom),
        "source": "unrealized_fallback",
        "metric": "unrealized_return",
        "as_of": None, "prior_date": None, "window": "since_cost_basis",
        "top": top, "bottom": bottom,
    }


def _build_drift(ob: Optional[dict]) -> dict:
    hd = (ob or {}).get("holdings_detail") or []
    holdings = [{
        "ticker": h.get("ticker"), "sector": h.get("sector"),
        "current_weight": _f(h.get("current_weight")),
        "target_weight": _f(h.get("target_weight")),
        "weight_drift": _f(h.get("weight_drift")),
    } for h in hd if _f(h.get("weight_drift")) is not None]
    holdings.sort(key=lambda h: -abs(h["weight_drift"] or 0.0))
    drifts = [abs(h["weight_drift"]) for h in holdings if h["weight_drift"] is not None]
    max_abs = round(max(drifts), 6) if drifts else None
    off = sum(1 for h in holdings if abs(h["weight_drift"] or 0.0) >= 0.01)
    return {
        "available": bool(holdings),
        "holdings": holdings,
        "max_abs_drift": max_abs,
        "names_off_target": off,
    }


# --------------------------------------------------------------------------- #
# Composition
# --------------------------------------------------------------------------- #
def build_portfolio_analytics(
    *,
    desk_dir=None,
    ledger_dir=None,
    today: Optional[str] = None,
    attribution_limit: int = 180,
    performance_loader: Optional[Callable[[], dict]] = None,
    attribution_loader: Optional[Callable[[], dict]] = None,
    operational_book_loader: Optional[Callable[[], dict]] = None,
    contributions_loader: Optional[Callable[[], dict]] = None,
) -> dict:
    """Compose the read-only chart-ready analytics payload. Loaders may be injected
    (deterministic tests); each is called defensively and degrades to an empty
    section on any error — the whole call never raises."""
    perf_load = performance_loader or (lambda: _desk.load_performance(desk_dir=desk_dir))
    attr_load = attribution_loader or (
        lambda: _fe.load_attribution_history(limit=attribution_limit, desk_dir=desk_dir, today=today))
    ob_load = operational_book_loader or (
        lambda: _opbook.load_operational_book(desk_dir=desk_dir, ledger_dir=ledger_dir, today=today))
    contrib_load = contributions_loader or (
        lambda: _fe.load_holding_contributions(desk_dir=desk_dir, today=today))

    def _safe(fn):
        try:
            out = fn()
            return out if isinstance(out, dict) else {}
        except Exception:  # noqa: BLE001 — any source failure degrades gracefully
            return {}

    perf = _safe(perf_load)
    attr = _safe(attr_load)
    ob_payload = _safe(ob_load)
    contrib = _safe(contrib_load)

    ob = ob_payload.get("operational_book") or {}
    cs = ob_payload.get("canonical_state") or ob.get("canonical_state") or {}

    performance = _build_performance(perf)
    pnl = _build_pnl(attr)
    allocation = _build_allocation(ob, cs)
    contributors = _build_contributors(contrib, ob)
    drift = _build_drift(ob)

    valuation_date = _d10(cs.get("valuation_date") or ob.get("desk_mark_date"))
    target_date = _d10(cs.get("target_date") or ob.get("target_market_date"))
    refresh_date = _d10(((ob.get("current_target") or {}).get("alpha_market_date")))

    any_available = any(sec["available"] for sec in
                        (performance, pnl, allocation, contributors, drift))

    return {
        "status": STATUS_OK if any_available else STATUS_EMPTY,
        "phase": PHASE,
        "canonical_owner": CANONICAL_OWNER,
        "contract_id": CONTRACT_ID,
        "generated_at": _now_iso(),
        "book_id": ob.get("book_id"),
        "book_label": ob.get("book_label"),
        "chart_keys": list(CHART_KEYS),
        "freshness": {
            "valuation_date": valuation_date,
            "target_date": target_date,
            "refresh_date": refresh_date,
            "performance_through": performance["through_date"],
            "pnl_through": pnl["through_date"],
            "contributors_as_of": contributors.get("as_of"),
            "n_sessions": performance["n_sessions"],
            "sources": [
                "paper_trading_desk.load_performance",
                "forward_evidence.load_attribution_history",
                "operational_book.load_operational_book",
                "forward_evidence.load_holding_contributions",
            ],
        },
        "performance": performance,
        "pnl": pnl,
        "allocation": allocation,
        "contributors": contributors,
        "drift": drift,
        **_safety(),
    }


__all__ = ["build_portfolio_analytics", "PHASE", "CANONICAL_OWNER", "CONTRACT_ID",
           "STATUS_OK", "STATUS_EMPTY", "CHART_KEYS"]
