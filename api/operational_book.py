"""
api/operational_book.py — Phase 27B: ONE operational portfolio, everywhere.

The application exposes three different portfolio concepts:

    1. Alpha Paper Book #1        (desk ledgers, book_id ``alpha_paper_book_1``)
    2. the legacy paper portfolio (DB-backed signal workflow: CDW/HUM, fills,
       snapshots — ARCHIVED, read-only, never the default portfolio again)
    3. research / multi-horizon books (Top25 / Top50 / champion / challenger /
       composite — evidence, never the user's operational holdings)

This module is the single READ-ONLY aggregation that every operational screen
(Command Center, Portfolio, Daily Workflow, Paper Desk, Portfolio Manager)
uses to describe the ONE operational book: **Alpha Paper Book #1**.

It composes EXISTING service functions only (alpha_book status + desk NAV
replay + alpha-target readiness). It performs NO writes, creates NO orders /
signals / decisions / fills, never initializes the book, never confirms a
snapshot, and never calls a prediction service. Cash / NAV / holdings /
pending orders each have exactly ONE producer (the desk ledger replay), so no
two pages can ever disagree about the operational book.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional

from paper_trader.api import alpha_book as ab
from paper_trader.api import alpha_target as at
from paper_trader.api import paper_trading_desk as desk

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "27B"

OPERATIONAL_BOOK_ID = ab.ALPHA_BOOK_ID              # "alpha_paper_book_1"
OPERATIONAL_BOOK_LABEL = "Alpha Paper Book #1"

ROLE_OPERATIONAL = "OPERATIONAL_BOOK"
ROLE_HISTORICAL = "HISTORICAL_BOOK"
ROLE_RESEARCH = "RESEARCH_BOOK"

STATUS_OK = "OPERATIONAL_BOOK_OK"
ARCHIVE_STATUS_OK = "HISTORICAL_BOOKS_OK"

LEGACY_BOOK_ID = "legacy_paper_portfolio"
LEGACY_BOOK_LABEL = "Legacy Paper Portfolio (Archived)"

# Non-terminal desk order statuses = "pending" from the operator's viewpoint.
_PENDING_ORDER_STATUSES = (desk.ST_PROPOSED, desk.ST_APPROVED, desk.ST_SUBMITTED)

SINGLE_SOURCE_NOTE = (
    "Alpha Paper Book #1 is the ONE operational portfolio. Cash, holdings, "
    "pending orders and NAV are produced exactly once, by replaying the "
    "append-only desk ledgers, and every operational page reads this same "
    "endpoint. The legacy paper portfolio is a read-only historical archive; "
    "research books are evidence and are never the user's holdings."
)

# Research books (identity metadata only — the numbers live in Research & Audit).
RESEARCH_BOOKS = [
    {"book_id": "current_alpha_top25", "label": "Champion composite_sn — Top 25",
     "classification": ROLE_RESEARCH, "route": "research-audit/paper-books"},
    {"book_id": "current_alpha_top50", "label": "Champion composite_sn — Top 50",
     "classification": ROLE_RESEARCH, "route": "research-audit/paper-books"},
    {"book_id": "challenger_top25", "label": "Challenger composite_sn_repaired — Top 25",
     "classification": ROLE_RESEARCH, "route": "research-audit/tournament"},
    {"book_id": "challenger_top50", "label": "Challenger composite_sn_repaired — Top 50",
     "classification": ROLE_RESEARCH, "route": "research-audit/tournament"},
    {"book_id": "fundamental_momentum_50_50_top25",
     "label": "Multi-horizon 50/50 ensemble model book (target source)",
     "classification": ROLE_RESEARCH, "route": "multi-horizon"},
]

RESEARCH_BOOKS_NOTE = (
    "Research books are frozen paper evidence (champion / challenger / model "
    "books). They inform the operational target but are never shown as the "
    "user's operational holdings."
)


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _default_valuation_loader():
    from paper_trader.api.portfolio_valuation import load_portfolio_valuation
    return load_portfolio_valuation()


# Injectable seam (mirrors alpha_book._VALUATION_LOADER) so tests never need a DB.
_VALUATION_LOADER = _default_valuation_loader


def _safety() -> dict:
    """Read-only safety contract carried by every GET in this module."""
    return {
        "paper_only": True,
        "read_only": True,
        "broker_enabled": False,
        "automation_enabled": False,
        "live_orders_enabled": False,
        "performed_write": False,
        "safety_badges": ["OPERATIONAL BOOK", "PAPER ONLY", "MANUAL REVIEW",
                          "NO ORDERS CREATED", "NO BROKER", "AUTOMATION OFF"],
    }


def _f(x: Any) -> Optional[float]:
    if x is None or isinstance(x, bool):
        return None
    try:
        return float(str(x))
    except (TypeError, ValueError):
        return None


def _pending_orders(desk_dir=None) -> dict:
    """Fold the desk order ledger for the operational book (read-only replay)."""
    sdir = desk._desk_dir(desk_dir)
    orders = [o for o in desk._orders_state(sdir).values()
              if o.get("book_id") == OPERATIONAL_BOOK_ID]
    by_status: dict[str, int] = {}
    for o in orders:
        by_status[o["status"]] = by_status.get(o["status"], 0) + 1
    pending = [o for o in orders if o.get("status") in _PENDING_ORDER_STATUSES]
    return {
        "pending_count": len(pending),
        "awaiting_manual_confirmation": by_status.get(desk.ST_PROPOSED, 0),
        "awaiting_fill": (by_status.get(desk.ST_APPROVED, 0)
                          + by_status.get(desk.ST_SUBMITTED, 0)),
        "filled_count": by_status.get(desk.ST_FILLED, 0),
        "total_orders": len(orders),
        "by_status": by_status,
    }


# --------------------------------------------------------------------------- #
# The operational book (single source of truth)
# --------------------------------------------------------------------------- #

def load_operational_book(*, desk_dir=None, ledger_dir=None, today: Optional[str] = None,
                          panel_path=None, inputs_dir=None) -> dict:
    """ONE payload describing Alpha Paper Book #1 for every operational page."""
    warnings: list[str] = []

    # 1. Alpha-book workflow status + ledger-replayed valuation (the ONE producer).
    status: dict = {}
    try:
        status = ab.load_alpha_status(desk_dir=desk_dir, ledger_dir=ledger_dir,
                                      today=today)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Alpha book status unavailable: {str(exc)[:160]}")

    book = status.get("book")
    valuation = status.get("book_valuation") or {}
    initialized = book is not None

    # 2. Pending paper orders for this book (same ledgers, read-only fold).
    orders = {"pending_count": 0, "awaiting_manual_confirmation": 0,
              "awaiting_fill": 0, "filled_count": 0, "total_orders": 0,
              "by_status": {}}
    try:
        orders = _pending_orders(desk_dir)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Desk order ledger unavailable: {str(exc)[:160]}")

    # 3. Current target readiness (six-date contract from Phase 27A.2).
    target: Optional[dict] = None
    try:
        r = at.load_readiness(panel_path=panel_path, inputs_dir=inputs_dir,
                              ledger_dir=ledger_dir)
        dates = r.get("dates") or {}
        target = {
            "state": r.get("state"),
            "alpha_market_date": dates.get("alpha_market_date"),
            "latest_completed_market_date": dates.get("latest_completed_market_date"),
            "alpha_market_aligned": r.get("alpha_market_aligned"),
            "snapshot_confirmation_allowed": r.get("snapshot_confirmation_allowed"),
            "confirmation_blockers": r.get("confirmation_blockers") or [],
            "required_next_action": r.get("required_next_action"),
        }
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Alpha target readiness unavailable: {str(exc)[:160]}")

    current_status = status.get("current_state") or "UNAVAILABLE"
    next_action = status.get("next_required_action") or (
        "Backend unavailable — reconnect, then reload the operational book.")

    operational_book = {
        "book_id": OPERATIONAL_BOOK_ID,
        "book_label": OPERATIONAL_BOOK_LABEL,
        "classification": ROLE_OPERATIONAL,
        "initialized": initialized,
        "policy_version": status.get("policy_version"),
        "initial_capital": (_f(book.get("initial_capital")) if book else
                            _f((status.get("policy") or {}).get("initial_capital"))),
        "currency": (book or {}).get("currency") or desk.BOOK_CURRENCY,
        # The ONE producer of these numbers is desk.book_nav (ledger replay).
        "cash": _f(valuation.get("cash")),
        "invested": _f(valuation.get("invested")),
        "nav": _f(valuation.get("nav")),
        "nav_as_of_date": valuation.get("as_of_date"),
        "holdings_count": valuation.get("holdings_count") or 0,
        "holdings": valuation.get("holdings") or {},
        "pending_orders": orders,
        "fills_count": status.get("n_alpha_fills") or 0,
        "current_target": target,
        "current_status": current_status,
        "next_action": next_action,
        "latest_desk_mark_date": status.get("latest_desk_mark_date"),
        "initialization": status.get("initialization"),
        "not_initialized_note": (None if initialized else
                                 "Alpha Paper Book #1 is not initialized yet: cash, "
                                 "NAV and holdings are honestly empty until the first "
                                 "manual initialization (token-gated, user approval)."),
    }

    return {
        "status": STATUS_OK,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "operational_book": operational_book,
        "single_source_of_truth": {
            "endpoint": "/v1/operational-book",
            "value_producer": "paper_trading_desk.book_nav (append-only ledger replay)",
            "note": SINGLE_SOURCE_NOTE,
        },
        "other_books": {
            "legacy_paper_portfolio": {
                "book_id": LEGACY_BOOK_ID,
                "label": LEGACY_BOOK_LABEL,
                "classification": ROLE_HISTORICAL,
                "read_only": True,
                "archived": True,
                "route": "portfolio/archive",
            },
            "research_books_note": RESEARCH_BOOKS_NOTE,
        },
        "warnings": warnings,
        **_safety(),
    }


# --------------------------------------------------------------------------- #
# Historical Paper Books (archive)
# --------------------------------------------------------------------------- #

def _legacy_archive_entry() -> dict:
    """Read-only snapshot of the ARCHIVED legacy paper portfolio. Degrades."""
    entry = {
        "book_id": LEGACY_BOOK_ID,
        "label": LEGACY_BOOK_LABEL,
        "classification": ROLE_HISTORICAL,
        "read_only": True,
        "archived": True,
        "available": False,
        "positions_count": None,
        "tickers": [],
        "cash": None,
        "total_value": None,
        "as_of_market_date": None,
        "note": ("The legacy signal-workflow paper portfolio (existing positions, "
                 "fills, snapshots and P&L). Preserved read-only in the archive — "
                 "never shown as the default portfolio, never modified by the "
                 "operational Alpha Paper Book #1 workflow."),
    }
    try:
        v = _VALUATION_LOADER()
        mark = v.get("current_mark") or {}
        positions = [p.get("ticker") for p in (v.get("positions") or [])
                     if p.get("ticker")]
        entry.update({
            "available": True,
            "positions_count": len(positions),
            "tickers": positions,
            "cash": _f(mark.get("current_cash")),
            "total_value": _f(mark.get("current_total_value")),
            "as_of_market_date": mark.get("as_of_market_date"),
        })
    except Exception as exc:  # noqa: BLE001
        entry["note"] += f" (valuation currently unavailable: {str(exc)[:120]})"
    return entry


def _completed_desk_books(desk_dir=None) -> list[dict]:
    """Any non-operational or closed desk paper books (future completed books)."""
    out: list[dict] = []
    try:
        sdir = desk._desk_dir(desk_dir)
        for b in desk._books(sdir):
            closed = b.get("status") not in (None, "OPEN")
            if b.get("book_id") == OPERATIONAL_BOOK_ID and not closed:
                continue  # the ACTIVE operational book is not an archive entry
            out.append({
                "book_id": b.get("book_id"),
                "label": b.get("label") or b.get("book_id"),
                "classification": ROLE_HISTORICAL,
                "read_only": True,
                "archived": True,
                "status": b.get("status"),
                "initial_capital": _f(b.get("initial_capital")),
                "note": "Completed / non-operational desk paper book (archived).",
            })
    except Exception:  # noqa: BLE001
        pass
    return out


def load_historical_books(*, desk_dir=None) -> dict:
    """Historical Paper Books: the legacy archive + past/completed paper books."""
    historical = [_legacy_archive_entry()] + _completed_desk_books(desk_dir)
    return {
        "status": ARCHIVE_STATUS_OK,
        "phase": PHASE,
        "generated_at": _now_iso(),
        "operational_book_id": OPERATIONAL_BOOK_ID,
        "operational_book_label": OPERATIONAL_BOOK_LABEL,
        "historical_books": historical,
        "research_books": [dict(b) for b in RESEARCH_BOOKS],
        "research_books_note": RESEARCH_BOOKS_NOTE,
        "note": ("Historical Paper Books are read-only. The single operational "
                 "portfolio is Alpha Paper Book #1 (see /v1/operational-book)."),
        **_safety(),
    }


__all__ = [
    "PHASE", "OPERATIONAL_BOOK_ID", "OPERATIONAL_BOOK_LABEL",
    "ROLE_OPERATIONAL", "ROLE_HISTORICAL", "ROLE_RESEARCH",
    "STATUS_OK", "ARCHIVE_STATUS_OK", "LEGACY_BOOK_ID", "LEGACY_BOOK_LABEL",
    "SINGLE_SOURCE_NOTE", "RESEARCH_BOOKS",
    "load_operational_book", "load_historical_books",
]
