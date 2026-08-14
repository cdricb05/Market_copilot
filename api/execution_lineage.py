"""Stage 21 (Workstream 0A) — execution-lineage COMPOSITION owner.

Reads the immutable Paper Desk ledgers, hands them to the ONE pure calculation owner
(``engine.execution_lineage``) and returns the read-only lineage contract. It performs
no arithmetic of its own, values no portfolio (the canonical valuation owner does that)
and NEVER writes.

Why this exists: after the 2026-08-13 settlement the canonical rebalance read model
collapsed to ``REBALANCE_NO_PROPOSAL`` because it re-derived execution identity from the
CURRENT reallocation proposal. The completed 29-order rebalance was still fully recorded
in the ledger — it had simply become unreachable through the read model. Lineage is now
recovered from that ledger directly, so a completed rebalance stays discoverable for as
long as the evidence exists.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from paper_trader.api import paper_trading_desk as desk
from paper_trader.engine import execution_lineage as kernel

PHASE = "STAGE21"
OWNER = "api.execution_lineage"
COMPOSITION_OWNER = OWNER
CALCULATION_OWNER = kernel.CALCULATION_OWNER
SCHEMA_VERSION = kernel.SCHEMA_VERSION

# The kernel mirrors the desk's status/side vocabulary so it can stay dependency-free.
# If the desk owner ever renames one, this assertion fails loudly at import instead of
# silently mis-counting a lineage cohort.
assert kernel.ST_FILLED == desk.ST_FILLED
assert kernel.ST_CANCELLED == desk.ST_CANCELLED
assert kernel.ST_SUBMITTED == desk.ST_SUBMITTED
assert kernel.ST_APPROVED == desk.ST_APPROVED
assert kernel.SIDE_BUY == desk.SIDE_BUY
assert kernel.SIDE_SELL == desk.SIDE_SELL


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safety() -> dict:
    return {
        "read_only": True, "performed_write": False, "provider_called": False,
        "prediction_called": False, "created_orders": False, "created_fills": False,
        "changed_holdings": False, "changed_cash": False, "changed_nav": False,
        "broker_enabled": False, "live_orders_enabled": False, "automation_enabled": False,
        "promoted_model": False, "recalibrated_model": False,
        "safety_badges": ["READ ONLY", "PAPER ONLY", "NO ORDERS", "NO BROKER",
                          "AUTOMATION OFF", "MANUAL REVIEW"],
    }


def _default_portfolio_state_loader() -> dict:
    from paper_trader.api import portfolio_state as ps
    return ps.load_portfolio_state()


def _resulting_portfolio(portfolio_state: Optional[dict] = None,
                         portfolio_state_loader=None) -> dict:
    """The post-settlement holdings / cash / NAV.

    Delegated to ``api.portfolio_state`` — the ONE canonical composition of the current
    economic position (itself corporate-action projected through the canonical registry
    owner). This module introduces NO second NAV, cash or holdings owner and performs no
    valuation arithmetic; it reports what the canonical owner already computed.
    """
    try:
        ps = portfolio_state if portfolio_state is not None else (
            portfolio_state_loader or _default_portfolio_state_loader)()
        cap = (ps or {}).get("capital") or {}
        positions = (ps or {}).get("positions") or []
        return {"owner": "api.portfolio_state",
                "holdings_count": len([p for p in positions
                                       if isinstance(p, dict) and p.get("quantity")]),
                "cash": cap.get("cash"), "nav": cap.get("nav"),
                "economic_state_hash": (ps or {}).get("economic_state_hash")}
    except Exception:  # noqa: BLE001 - a pure read must never crash the caller
        return {}


def load_execution_lineage(*, desk_dir=None, orders=None, fills=None,
                           portfolio_state: Optional[dict] = None,
                           portfolio_state_loader=None,
                           resulting_portfolio: Optional[dict] = None) -> dict:
    """READ-ONLY post-execution rebalance lineage for the operational book."""
    generated_at = _now_iso()
    try:
        sdir = desk._desk_dir(desk_dir)
        o = orders if orders is not None else desk._orders_state(sdir)
        f = fills if fills is not None else desk._fills(sdir)
        rp = (resulting_portfolio if resulting_portfolio is not None
              else _resulting_portfolio(portfolio_state, portfolio_state_loader))
        view = kernel.build_execution_lineage(o, fills=f, resulting_portfolio=rp)
    except Exception as exc:  # noqa: BLE001
        return {"phase": PHASE, "owner": OWNER, "status": "UNAVAILABLE",
                "generated_at": generated_at, "latest_completed_rebalance": None,
                "order_plans": [], "order_plan_count": 0, "superseded_plans": [],
                "superseded_plan_ids": [], "historical_implementation_fill_count": 0,
                "message": "Execution lineage unavailable: %s" % str(exc)[:160],
                **_safety()}
    latest = view.get("latest_completed_rebalance")
    return {
        "phase": PHASE, "owner": OWNER, "composition_owner": COMPOSITION_OWNER,
        "calculation_owner": CALCULATION_OWNER, "status": "OK",
        "generated_at": generated_at,
        **view,
        "message": (
            "Completed controlled rebalance %s: %d of %d order(s) filled on the %s "
            "lifecycle, settled %s. Recovered from the immutable desk ledger, never "
            "recomputed from the current target."
            % (latest.get("order_plan_id_short"), latest.get("filled_count"),
               latest.get("order_count"), latest.get("execution_model"),
               latest.get("settlement_market_date"))
            if latest else
            "No controlled rebalance has executed for this book yet."),
        **_safety(),
    }


__all__ = ["PHASE", "OWNER", "COMPOSITION_OWNER", "CALCULATION_OWNER", "SCHEMA_VERSION",
           "load_execution_lineage"]
