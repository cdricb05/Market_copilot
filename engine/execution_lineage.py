"""Stage 21 (Workstream 0A) — POST-EXECUTION REBALANCE LINEAGE (pure calculation).

THE DEFECT THIS OWNER EXISTS TO FIX
-----------------------------------
The Stage-19 controlled rebalance completed for real on 2026-08-13: order plan
``rbop_2026-08-12_alpha_paper_book_1_1a198f560cca`` filled 29 of 29 orders on the
NEXT_CLOSE lifecycle, superseding the defective 22-order plan
``rbop_2026-08-12_alpha_paper_book_1_5bf9c6c20f8a`` (all CANCELLED, never executed).

After settlement the canonical rebalance read model reported::

    rebalance_state = REBALANCE_NO_PROPOSAL
    bound           = null
    execution_summary.filled_count = 0

...because it derived the CURRENT rebalance from the CURRENT reallocation proposal.
Once the eligible session advanced past 2026-08-12 that proposal was no longer the
current one, ``bound.proposal_hash`` resolved to ``None``, the desk-order cohort came
back empty and a completed, fully reconciled 29-order rebalance became undiscoverable.
A second, latent defect selected the "current" plan with ``sorted(plan_ids)[-1]`` — the
plan-id suffix is a HASH, so that ordering is arbitrary, and it ranks the defective
``5bf9...`` plan ABOVE the executed ``1a198...`` plan purely on hexadecimal ordering.

Both defects share one cause: execution identity was RE-DERIVED from current research
state instead of READ from the immutable ledger that already records it.

THE CONTRACT
------------
This module is the ONE calculation owner for post-execution rebalance lineage. It is
PURE: no network, no provider, no prediction, no file I/O, no clock, no writes. It is
handed the desk's already-folded order state and fill rows and it folds them into
per-plan lineage records.

Lineage is recovered from IMMUTABLE evidence only — the lineage block each paper order
carries, the order statuses and the fill ledger. Nothing here is ever recomputed from a
current target, a current proposal or a current ranking, so a completed rebalance stays
discoverable for as long as the ledger exists.

Plans are ordered CHRONOLOGICALLY by their recorded ``created_at`` (never by id, never
by hash). The executed plan is the one whose orders actually filled; a plan whose orders
were all cancelled is SUPERSEDED_CANCELLED and can never be presented as current.
"""
from __future__ import annotations

from typing import Any, Optional

CALCULATION_OWNER = "engine.execution_lineage"
SCHEMA_VERSION = "execution_lineage.v1"
PHASE = "STAGE21"

# --------------------------------------------------------------------------- #
# Lineage states. ONE vocabulary; every consumer reports these verbatim.
# --------------------------------------------------------------------------- #
#: Every order of the plan filled — the terminal, reconciled state.
STATE_EXECUTED = "PAPER_EXECUTED_RECONCILED"
#: Some orders filled, others remain live.
STATE_PARTIAL = "PAPER_EXECUTION_PARTIALLY_FILLED"
#: Orders are live (approved / submitted) and nothing has filled yet.
STATE_PENDING = "PAPER_EXECUTION_PENDING"
#: Every order was cancelled/expired and NOTHING filled. A defective or replaced plan.
#: This state can never be surfaced as the current or executed rebalance.
STATE_SUPERSEDED_CANCELLED = "SUPERSEDED_CANCELLED"
#: A plan with no orders at all (defensive; never expected).
STATE_EMPTY = "NO_ORDERS"

LINEAGE_STATE_VOCAB = (STATE_EXECUTED, STATE_PARTIAL, STATE_PENDING,
                       STATE_SUPERSEDED_CANCELLED, STATE_EMPTY)

#: Desk order statuses. Mirrored (not imported) to keep this kernel dependency-free;
#: `api.execution_lineage` asserts they still match the desk owner's constants.
ST_PROPOSED = "PROPOSED"
ST_APPROVED = "APPROVED"
ST_SUBMITTED = "SUBMITTED"
ST_FILLED = "FILLED"
ST_CANCELLED = "CANCELLED"
ST_EXPIRED = "EXPIRED"

_TERMINAL_DEAD = (ST_CANCELLED, ST_EXPIRED)
_LIVE_UNFILLED = (ST_PROPOSED, ST_APPROVED, ST_SUBMITTED)

SIDE_BUY = "PAPER_BUY"
SIDE_SELL = "PAPER_SELL"


def _lineage(order: Optional[dict]) -> dict:
    return (order or {}).get("rebalance_lineage") or {}


def _s(x: Any) -> str:
    return "" if x is None else str(x)


def order_rows(orders: Any) -> list[dict]:
    """Normalise the desk's folded order state (dict keyed by order id, or a list)."""
    if isinstance(orders, dict):
        rows = list(orders.values())
    elif isinstance(orders, (list, tuple)):
        rows = list(orders)
    else:
        rows = []
    return [o for o in rows if isinstance(o, dict)]


def historical_implementation_fills(orders: Any) -> list[dict]:
    """Filled orders carrying NO rebalance lineage — the book's INITIAL implementation.

    Reported entirely separately from every rebalance cohort. On the live book this is
    the original 25 fills, and mixing them into a rebalance count is exactly the
    "Submitted 29 / Filled 25" misread Stage 19.3 already had to repair once.
    """
    return [o for o in order_rows(orders)
            if o.get("status") == ST_FILLED and not _lineage(o).get("order_plan_id")]


def _fill_rows_by_order(fills: Any) -> dict[str, dict]:
    out: dict[str, dict] = {}
    rows = fills if isinstance(fills, (list, tuple)) else []
    for f in rows:
        if isinstance(f, dict) and f.get("order_id"):
            out[str(f["order_id"])] = f
    return out


def build_plan_lineage(orders: Any, *, fills: Any = None) -> list[dict]:
    """One immutable lineage record per order plan, oldest FIRST (chronological).

    Ordering is by the lineage's recorded ``created_at``. It is NEVER by plan id: the
    id's suffix is a hash, so id ordering is arbitrary and — on the live book — ranks
    the defective 5bf9... plan above the executed 1a198... plan.
    """
    by_order_fill = _fill_rows_by_order(fills)
    plans: dict[str, list[dict]] = {}
    for o in order_rows(orders):
        pid = _lineage(o).get("order_plan_id")
        if pid:
            plans.setdefault(str(pid), []).append(o)

    records = []
    for pid, cohort in plans.items():
        records.append(_plan_record(pid, cohort, by_order_fill))
    records.sort(key=lambda r: (_s(r.get("created_at")), _s(r.get("order_plan_id"))))
    return records


def _plan_record(order_plan_id: str, cohort: list[dict],
                 by_order_fill: dict[str, dict]) -> dict:
    lin = _lineage(cohort[0]) if cohort else {}

    def _n(*statuses) -> int:
        return sum(1 for o in cohort if o.get("status") in statuses)

    filled = _n(ST_FILLED)
    cancelled = _n(*_TERMINAL_DEAD)
    live_unfilled = _n(*_LIVE_UNFILLED)
    total = len(cohort)

    if not total:
        state = STATE_EMPTY
    elif filled == total:
        # Every order of the plan filled — terminal and fully reconciled.
        state = STATE_EXECUTED
    elif filled:
        # Some filled; the rest are either still live or were cancelled. Either way the
        # plan is NOT a faithful full implementation and is reported as partial.
        state = STATE_PARTIAL
    elif cancelled == total:
        # Nothing filled and everything dead: a defective / replaced plan. It can never
        # be presented as the current or executed rebalance.
        state = STATE_SUPERSEDED_CANCELLED
    else:
        state = STATE_PENDING

    fill_dates = sorted({_s(by_order_fill.get(_s(o.get("order_id")), {}).get("fill_date"))
                         for o in cohort if o.get("status") == ST_FILLED} - {""})
    approvals = sorted({_s(o.get("approval_date")) for o in cohort} - {""})
    # The order plan hash is read from the orders' OWN recorded lineage — it is the
    # hash the operator actually confirmed, never a re-derivation of a current target.
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "order_plan_id": order_plan_id,
        "order_plan_id_short": order_plan_id[-12:],
        "order_plan_hash": lin.get("order_plan_hash"),
        "proposal_id": lin.get("proposal_id"),
        "proposal_hash": lin.get("proposal_hash"),
        "decision_id": lin.get("decision_id"),
        "paper_book_id": lin.get("paper_book_id"),
        "eligible_market_date": lin.get("eligible_market_date"),
        "execution_model": lin.get("execution_model"),
        "created_at": lin.get("created_at"),
        "approval_date": approvals[-1] if approvals else None,
        "state": state,
        "order_count": total,
        "submitted_count": _n(ST_APPROVED, ST_SUBMITTED),
        "filled_count": filled,
        "cancelled_count": cancelled,
        "live_unfilled_count": live_unfilled,
        "buy_count": sum(1 for o in cohort if o.get("side") == SIDE_BUY),
        "sell_count": sum(1 for o in cohort if o.get("side") == SIDE_SELL),
        "first_fill_date": fill_dates[0] if fill_dates else None,
        "final_fill_date": fill_dates[-1] if fill_dates else None,
        "settlement_market_date": fill_dates[-1] if fill_dates else None,
        "order_ids": sorted(_s(o.get("order_id")) for o in cohort),
        "fill_ids": sorted(
            _s(by_order_fill.get(_s(o.get("order_id")), {}).get("fill_id"))
            for o in cohort if o.get("status") == ST_FILLED
            and by_order_fill.get(_s(o.get("order_id")))),
        "executed": state in (STATE_EXECUTED, STATE_PARTIAL),
        "fully_reconciled": state == STATE_EXECUTED,
        "superseded": state == STATE_SUPERSEDED_CANCELLED,
        "immutable_source": "paper_trading_desk order + fill ledgers",
        "derived_from_current_target": False,
    }


def latest_completed_rebalance(orders: Any, *, fills: Any = None,
                               resulting_portfolio: Optional[dict] = None) -> Optional[dict]:
    """The most recent rebalance that actually EXECUTED, or ``None``.

    "Executed" means at least one order of the plan filled. A plan whose orders were all
    cancelled is never eligible, no matter how recent and no matter how its id sorts.
    Selection is by settlement date, then by the plan's chronological ``created_at``.

    ``resulting_portfolio`` (optional) supplies the post-settlement holdings/cash/NAV
    the caller already read from the canonical valuation owner. This module never values
    a portfolio — it only reports what it was handed.
    """
    executed = [r for r in build_plan_lineage(orders, fills=fills) if r["executed"]]
    if not executed:
        return None
    executed.sort(key=lambda r: (_s(r.get("settlement_market_date")),
                                 _s(r.get("created_at")), _s(r.get("order_plan_id"))))
    latest = executed[-1]
    rp = resulting_portfolio or {}
    return {
        **latest,
        "resulting_holdings_count": rp.get("holdings_count"),
        "resulting_cash": rp.get("cash"),
        "resulting_nav": rp.get("nav"),
        "resulting_portfolio_owner": rp.get("owner"),
    }


def superseded_plans(orders: Any, *, fills: Any = None) -> list[dict]:
    """Every plan that never executed — kept as separate, auditable evidence."""
    return [r for r in build_plan_lineage(orders, fills=fills) if r["superseded"]]


def build_execution_lineage(orders: Any, *, fills: Any = None,
                            resulting_portfolio: Optional[dict] = None) -> dict:
    """The complete, read-only lineage view: every plan, the latest executed one, the
    superseded ones and the historical initial implementation — never mixed."""
    plans = build_plan_lineage(orders, fills=fills)
    latest = latest_completed_rebalance(orders, fills=fills,
                                        resulting_portfolio=resulting_portfolio)
    hist = historical_implementation_fills(orders)
    return {
        "schema_version": SCHEMA_VERSION,
        "calculation_owner": CALCULATION_OWNER,
        "phase": PHASE,
        "state_vocabulary": list(LINEAGE_STATE_VOCAB),
        "latest_completed_rebalance": latest,
        "order_plans": plans,
        "order_plan_count": len(plans),
        "superseded_plans": [r for r in plans if r["superseded"]],
        "superseded_plan_ids": [r["order_plan_id"] for r in plans if r["superseded"]],
        "historical_implementation_fill_count": len(hist),
        "historical_implementation_label": (
            "Existing operational holdings from the initial implementation: "
            "%d filled order(s)" % len(hist)),
        "cohorts_are_separated": True,
        "recovered_from_immutable_ledger": True,
        "recomputed_from_current_target": False,
    }


__all__ = [
    "CALCULATION_OWNER", "SCHEMA_VERSION", "PHASE",
    "STATE_EXECUTED", "STATE_PARTIAL", "STATE_PENDING",
    "STATE_SUPERSEDED_CANCELLED", "STATE_EMPTY", "LINEAGE_STATE_VOCAB",
    "order_rows", "historical_implementation_fills", "build_plan_lineage",
    "latest_completed_rebalance", "superseded_plans", "build_execution_lineage",
]
