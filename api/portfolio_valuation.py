"""
api/portfolio_valuation.py — Phase 14-C canonical portfolio valuation service.

The single, read-only source of truth for CURRENT portfolio valuation. Before
this module, the UI mixed three different valuation timestamps:

    * Portfolio.cached_total_value    (reconciler cache — last fill cycle)
    * sum(qty * latest PriceSnapshot) (independently re-marked — newer)
    * latest PortfolioSnapshot        (last official post-market snapshot)

Combining a cached total with independently re-marked positions produced the
``cash + invested != total`` drift the user observed ($6.67).

``load_portfolio_valuation()`` fixes this by computing ONE internally consistent
current mark and keeping the official snapshot strictly separate:

    current_cash            = Portfolio.cached_cash
    current_positions_value = sum(position.qty * latest owned EOD price)
    current_total_value     = current_cash + current_positions_value
    current_total_return_pct= (current_total_value - initial_capital)/initial * 100
    current_unrealized_pnl  = sum(market_value - cost_basis)

Every current-mark field shares one ``as_of_market_date`` / ``price_source`` /
``valuation_type = CURRENT_MARKED_EOD`` / ``freshness_status``. The latest
official snapshot is returned under ``valuation_type = OFFICIAL_PORTFOLIO_SNAPSHOT``
and is NEVER merged into the current mark. A reconciliation block proves the
invariant ``abs(cash + positions - total) <= 0.01`` and compares the current
mark against both the cache and the latest snapshot (comparisons, not
interchangeable values).

It makes NO loopback HTTP calls, performs NO database writes, calls NO
prediction provider, and creates NO orders / signals / decisions. A single
failing dependency degrades to a ``warnings[]`` entry and a partial result
instead of an HTTP 500. This module imports only the DB + config layers so it
stays a dependency-cycle-free leaf that command_center / portfolio_terminal can
both reuse.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from sqlalchemy import select

from paper_trader.config import get_settings
from paper_trader.db.models import Portfolio, PortfolioSnapshot, Position, PriceSnapshot
from paper_trader.db.session import get_session

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #

PHASE = "14-C"

VALUATION_CURRENT = "CURRENT_MARKED_EOD"
VALUATION_SNAPSHOT = "OFFICIAL_PORTFOLIO_SNAPSHOT"

# Position status enum (mirrors /v1/portfolio/analytics + portfolio_terminal).
POS_HOLD = "HOLD"
POS_WATCH = "WATCH"
POS_REVIEW_FOR_EXIT = "REVIEW_FOR_EXIT"
POS_PRICE_UNAVAILABLE = "PRICE_UNAVAILABLE"

# Freshness vocabulary for the current mark.
FRESH = "FRESH"
STALE = "STALE"
NO_PRICE = "NO_PRICE"
NO_POSITIONS = "NO_POSITIONS"

# A mark within this many calendar days of "today" is FRESH (covers a weekend).
_FRESH_MAX_CALENDAR_DAYS = 4

_DOLLARS = Decimal("0.01")
_PRICE = Decimal("0.000001")
_PCT = Decimal("0.0001")
_TOLERANCE = Decimal("0.01")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    try:
        return ts.isoformat()
    except Exception:  # noqa: BLE001
        return str(ts)


def _dstr(d: Decimal) -> str:
    return str(d.quantize(_DOLLARS))


def _pct(d: Decimal) -> float:
    return float(d.quantize(_PCT))


def _latest_price_row(session, ticker: str) -> Optional[PriceSnapshot]:
    """Return the most recent PriceSnapshot row for ticker (or None).

    Mirrors the fill-cycle selection: newest snapshot_ts wins. The full row is
    returned so the caller can record as_of market_date and the price source.
    """
    return session.execute(
        select(PriceSnapshot)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.snapshot_ts.desc())
        .limit(1)
    ).scalars().first()


def _position_status(upnl_pct: Optional[float], weight_pct: Optional[float]) -> tuple[str, str]:
    """Return (status, reason) using the shared position-monitor rules."""
    if upnl_pct is None:
        return (POS_PRICE_UNAVAILABLE,
                "Latest price unavailable; position cannot be fully evaluated.")
    if upnl_pct <= -5.0:
        return (POS_REVIEW_FOR_EXIT,
                f"Unrealized P&L of {upnl_pct:.1f}% is below the stop-loss review "
                f"threshold (-5.0%). Manual review recommended.")
    if upnl_pct <= -2.0:
        return (POS_WATCH,
                f"Unrealized P&L of {upnl_pct:.1f}% is in the watch range "
                f"(-2.0% to -5.0%). Monitor closely.")
    if weight_pct is not None and weight_pct > 25.0:
        return (POS_WATCH,
                f"Portfolio weight of {weight_pct:.1f}% exceeds the concentration "
                f"threshold (25.0%). Consider position sizing.")
    return (POS_HOLD, "Position is within healthy parameters. No action required.")


def _freshness(as_of: Optional[date], today: date) -> tuple[str, Optional[int]]:
    """Classify current-mark freshness from the newest owned market_date."""
    if as_of is None:
        return NO_PRICE, None
    age = (today - as_of).days
    return (FRESH if age <= _FRESH_MAX_CALENDAR_DAYS else STALE), age


def _price_source(sources: list[Optional[str]]) -> Optional[str]:
    distinct = sorted({(s or "unknown") for s in sources})
    if not distinct:
        return None
    if len(distinct) == 1:
        return distinct[0]
    return "MIXED"


# --------------------------------------------------------------------------- #
# Current mark (positions re-marked at the latest owned EOD price)
# --------------------------------------------------------------------------- #

def _compute_current_positions(session) -> dict[str, Any]:
    """Re-mark every open position at its latest owned EOD price (read-only).

    Weight is intentionally computed against the CANONICAL current total value
    (cash + covered positions) so the weights are internally consistent — never
    against the reconciler cache.
    """
    positions = session.execute(
        select(Position).order_by(Position.opened_at)
    ).scalars().all()

    covered: list[dict[str, Any]] = []
    total_positions_value = Decimal("0")
    total_unrealized = Decimal("0")
    market_dates: list[date] = []
    sources: list[Optional[str]] = []
    missing = 0

    for pos in positions:
        qty = Decimal(str(pos.qty))
        cost_basis = Decimal(str(pos.cost_basis))
        row = _latest_price_row(session, pos.ticker)

        rec: dict[str, Any] = {
            "ticker": pos.ticker,
            "qty": str(qty),
            "avg_cost": str(pos.avg_cost),
            "cost_basis": _dstr(cost_basis),
            "latest_price": None,
            "market_value": None,
            "unrealized_pnl": None,
            "unrealized_pnl_pct": None,
            "weight_pct": None,          # filled in a second pass (needs total)
            "status": POS_PRICE_UNAVAILABLE,
            "reason": "Latest price unavailable; position cannot be fully evaluated.",
            "price_as_of_market_date": None,
            "price_source": None,
            "opened_at": _iso(pos.opened_at),
            "last_updated": _iso(pos.last_updated),
            "_upnl_pct_f": None,
            "paper_only": True,
        }

        if row is None:
            missing += 1
            covered.append(rec)
            continue

        price = Decimal(str(row.price)).quantize(_PRICE)
        market_value = qty * price
        unrealized = market_value - cost_basis
        unrealized_pct = (
            unrealized / cost_basis * Decimal("100")
            if cost_basis != Decimal("0") else Decimal("0")
        )
        total_positions_value += market_value
        total_unrealized += unrealized
        if row.market_date is not None:
            market_dates.append(row.market_date)
        sources.append(row.data_source)

        rec.update({
            "latest_price": str(price),
            "market_value": _dstr(market_value),
            "unrealized_pnl": _dstr(unrealized),
            "unrealized_pnl_pct": str(unrealized_pct.quantize(_DOLLARS)),
            "price_as_of_market_date": _iso(row.market_date),
            "price_source": row.data_source,
            "_upnl_pct_f": float(unrealized_pct),
        })
        covered.append(rec)

    return {
        "rows": covered,
        "total_position_count": len(positions),
        "covered_position_count": len(positions) - missing,
        "missing_price_count": missing,
        "positions_value": total_positions_value,
        "unrealized_pnl": total_unrealized,
        "as_of_market_date": max(market_dates) if market_dates else None,
        "price_source": _price_source(sources),
    }


def _finalise_positions(rows: list[dict[str, Any]], *, total_value: Decimal) -> list[dict[str, Any]]:
    """Second pass: weight against the canonical total, then classify status."""
    out: list[dict[str, Any]] = []
    for rec in rows:
        weight_f: Optional[float] = None
        if rec["market_value"] is not None and total_value > Decimal("0"):
            weight = Decimal(rec["market_value"]) / total_value * Decimal("100")
            rec["weight_pct"] = str(weight.quantize(_DOLLARS))
            weight_f = float(weight)
        status, reason = _position_status(rec.pop("_upnl_pct_f"), weight_f)
        rec["status"] = status
        rec["reason"] = reason
        out.append(rec)
    out.sort(key=lambda r: (Decimal(r["weight_pct"]) if r["weight_pct"] else Decimal("0")),
             reverse=True)
    return out


# --------------------------------------------------------------------------- #
# Latest official snapshot (kept strictly separate from the current mark)
# --------------------------------------------------------------------------- #

def _latest_snapshot(session, *, initial: Optional[Decimal]) -> Optional[dict[str, Any]]:
    snap = session.execute(
        select(PortfolioSnapshot)
        .order_by(PortfolioSnapshot.market_date.desc())
        .limit(1)
    ).scalars().first()
    if snap is None:
        return None

    total = Decimal(str(snap.total_value))
    cumret = None
    if initial and initial != Decimal("0"):
        cumret = _pct((total - initial) / initial * Decimal("100"))

    benchmark_return_pct = None
    excess_return_pct = None
    if snap.benchmark_value is not None and initial and initial != Decimal("0"):
        bv = Decimal(str(snap.benchmark_value))
        benchmark_return_pct = _pct((bv - initial) / initial * Decimal("100"))
        if cumret is not None:
            excess_return_pct = round(cumret - benchmark_return_pct, 4)

    return {
        "valuation_type": VALUATION_SNAPSHOT,
        "market_date": _iso(snap.market_date),
        "total_value": _dstr(total),
        "cash": _dstr(Decimal(str(snap.cash))),
        "positions_value": _dstr(Decimal(str(snap.positions_value))),
        "realized_pnl_cumulative": _dstr(Decimal(str(snap.realized_pnl_cumulative))),
        "unrealized_pnl": _dstr(Decimal(str(snap.unrealized_pnl))),
        "cumulative_return_pct": cumret,
        "open_position_count": snap.open_position_count,
        "benchmark_ticker": snap.benchmark_ticker,
        "benchmark_value": (_dstr(Decimal(str(snap.benchmark_value)))
                            if snap.benchmark_value is not None else None),
        "benchmark_return_pct": benchmark_return_pct,
        "excess_return_pct": excess_return_pct,
    }


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #

def _provenance() -> dict[str, Any]:
    return {
        "phase": PHASE,
        "generated_at": _now_iso(),
        "read_only": True,
        "wrote_to_database": False,
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "invoked_daily_refresh": False,
        "called_prediction_service": False,
        "called_external_provider": False,
        "made_loopback_http_calls": False,
        "sources": [
            "db:portfolio.cached_cash",
            "db:positions",
            "db:price_snapshots (latest owned EOD)",
            "db:portfolio_snapshots (latest official)",
        ],
    }


def _empty_current_mark() -> dict[str, Any]:
    return {
        "valuation_type": VALUATION_CURRENT,
        "valuation_complete": False,
        "current_cash": None,
        "current_positions_value": None,
        "current_total_value": None,
        "current_total_return_pct": None,
        "current_unrealized_pnl": None,
        "initial_capital": None,
        "as_of_market_date": None,
        "calculated_at": _now_iso(),
        "price_source": None,
        "freshness_status": NO_PRICE,
        "age_calendar_days": None,
        "total_position_count": 0,
        "covered_position_count": 0,
        "missing_price_count": 0,
        "open_position_count": 0,
    }


# --------------------------------------------------------------------------- #
# Public entry point
# --------------------------------------------------------------------------- #

def load_portfolio_valuation() -> dict[str, Any]:
    """Return the canonical read-only portfolio valuation view model.

    Sections: status, current_mark, latest_snapshot, reconciliation, positions,
    warnings, provenance, safety. Never raises: a failing dependency degrades to
    a warning and a partial (still internally consistent) result.
    """
    warnings: list[str] = []
    current_mark = _empty_current_mark()
    latest_snapshot: Optional[dict[str, Any]] = None
    positions: list[dict[str, Any]] = []
    reconciliation: dict[str, Any] = {
        "cash_plus_positions": None,
        "reported_current_total": None,
        "reconciliation_delta": None,
        "reconciled": None,
        "vs_cached_total_value": {"cached_total_value": None, "delta": None},
        "vs_latest_snapshot_total": {"snapshot_total_value": None, "delta": None},
        "note": (
            "Current positions are marked using the latest owned EOD prices. "
            "Historical performance uses the latest completed portfolio snapshot. "
            "These are compared here, never interchanged."
        ),
    }
    seeded = False

    try:
        with get_session() as session:
            portfolio = session.execute(select(Portfolio)).scalars().first()
            if portfolio is None:
                warnings.append("Portfolio not seeded.")
            else:
                seeded = True
                cash = Decimal(str(portfolio.cached_cash))
                cached_total = Decimal(str(portfolio.cached_total_value))
                initial = Decimal(str(portfolio.initial_capital))
                today = date.today()

                # --- current positions (re-marked, isolated failure) --------- #
                try:
                    pos = _compute_current_positions(session)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Current position marking unavailable: {str(exc)[:160]}")
                    pos = {
                        "rows": [], "total_position_count": 0, "covered_position_count": 0,
                        "missing_price_count": 0, "positions_value": Decimal("0"),
                        "unrealized_pnl": Decimal("0"), "as_of_market_date": None,
                        "price_source": None,
                    }

                positions_value = pos["positions_value"]
                total_value = cash + positions_value
                positions = _finalise_positions(pos["rows"], total_value=total_value)

                total_pos = pos["total_position_count"]
                covered = pos["covered_position_count"]
                missing = pos["missing_price_count"]
                complete = (missing == 0)

                return_pct = None
                if initial and initial != Decimal("0"):
                    return_pct = _pct((total_value - initial) / initial * Decimal("100"))

                freshness_status, age = _freshness(pos["as_of_market_date"], today)
                if total_pos == 0:
                    freshness_status = NO_POSITIONS

                current_mark = {
                    "valuation_type": VALUATION_CURRENT,
                    "valuation_complete": complete,
                    "current_cash": _dstr(cash),
                    "current_positions_value": _dstr(positions_value),
                    "current_total_value": _dstr(total_value),
                    "current_total_return_pct": return_pct,
                    "current_unrealized_pnl": _dstr(pos["unrealized_pnl"]),
                    "initial_capital": _dstr(initial),
                    "as_of_market_date": _iso(pos["as_of_market_date"]),
                    "calculated_at": _now_iso(),
                    "price_source": pos["price_source"],
                    "freshness_status": freshness_status,
                    "age_calendar_days": age,
                    "total_position_count": total_pos,
                    "covered_position_count": covered,
                    "missing_price_count": missing,
                    "open_position_count": total_pos,
                }

                if not complete:
                    warnings.append(
                        f"{missing} of {total_pos} position(s) have no current owned "
                        f"price; the current mark is partial (covered "
                        f"{covered}/{total_pos})."
                    )

                # --- latest official snapshot (kept separate) ---------------- #
                try:
                    latest_snapshot = _latest_snapshot(session, initial=initial)
                except Exception as exc:  # noqa: BLE001
                    warnings.append(f"Latest snapshot unavailable: {str(exc)[:160]}")

                # --- reconciliation ------------------------------------------ #
                cash_plus_positions = cash + positions_value
                delta = (cash_plus_positions - total_value).quantize(_DOLLARS)
                cached_delta = (total_value - cached_total).quantize(_DOLLARS)
                snap_total = (Decimal(latest_snapshot["total_value"])
                              if latest_snapshot else None)
                snap_delta = ((total_value - snap_total).quantize(_DOLLARS)
                              if snap_total is not None else None)

                reconciliation = {
                    "cash_plus_positions": _dstr(cash_plus_positions),
                    "reported_current_total": _dstr(total_value),
                    "reconciliation_delta": str(delta),
                    "reconciled": bool(complete and abs(delta) <= _TOLERANCE),
                    "valuation_complete": complete,
                    "vs_cached_total_value": {
                        "cached_total_value": _dstr(cached_total),
                        "delta": str(cached_delta),
                    },
                    "vs_latest_snapshot_total": {
                        "snapshot_total_value": (_dstr(snap_total)
                                                 if snap_total is not None else None),
                        "delta": str(snap_delta) if snap_delta is not None else None,
                    },
                    "note": reconciliation["note"],
                }
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Backend/database unavailable: {str(exc)[:160]}")

    status = "OK" if seeded else "DEGRADED"
    return {
        "status": status,
        "seeded": seeded,
        "current_mark": current_mark,
        "latest_snapshot": latest_snapshot,
        "reconciliation": reconciliation,
        "positions": positions,
        "warnings": warnings,
        "safety": {
            "paper_only": True,
            "read_only": True,
            "no_broker_execution": True,
            "automation_off": True,
            "no_live_orders": True,
        },
        "provenance": _provenance(),
    }


__all__ = [
    "load_portfolio_valuation",
    "_position_status",
    "_compute_current_positions",
    "_finalise_positions",
    "_latest_snapshot",
    "_freshness",
    "_price_source",
    "VALUATION_CURRENT",
    "VALUATION_SNAPSHOT",
    "POS_HOLD", "POS_WATCH", "POS_REVIEW_FOR_EXIT", "POS_PRICE_UNAVAILABLE",
    "FRESH", "STALE", "NO_PRICE", "NO_POSITIONS",
]
