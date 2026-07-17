"""
api/daily_operating_run.py — Phase 15-A canonical one-click daily operating run.

Phase 14-C exposed a market-date alignment defect: the champion current-alpha
paper mark advances to a recent completed EOD date (EODHD research artifact on
the D: drive) while the portfolio ``price_snapshots`` state stays stale (Yahoo
prices written into Postgres, last refreshed weeks earlier). The two pipelines
share no provider, store, or market-date semantics, so Command Center, Portfolio
Terminal and the current-alpha books can silently disagree about "today".

This module adds ONE explicitly-manual orchestrator that drives BOTH pipelines to
the same resolved completed US market date and reports a single alignment /
reconciliation contract. It NEVER runs on a schedule and NEVER runs automatically
— a run happens only when the user clicks Preview / Run.

Two modes:

    run_daily_operating_session(execute=False)  — PREVIEW: no DB writes at all.
    run_daily_operating_session(execute=True)   — EXECUTE: the permitted writes.

Permitted writes (and ONLY these):
    1. completed market-price snapshots  (db.price_snapshots + db.benchmark_prices)
    2. exactly one official portfolio snapshot per market date (db.portfolio_snapshots)
    3. the existing current-alpha paper-mark history (local JSON store)
    4. existing JobRun / audit records (db.job_runs)

Explicitly NOT touched: positions, cash_ledger, signals, trade_decisions, orders,
trades. No Create Orders, no order execution, no broker, no automation/scheduling,
no new tables/migrations, no prediction call, no loopback HTTP. A single ticker
failure never fails the run; a failing stage degrades to a controlled status.

The provider and the alpha runner are the EXISTING abstractions
(``engine.market_data.fetch_latest_prices`` and
``current_alpha_daily_refresh.run_current_alpha_daily_refresh``) — reused, never
re-implemented. Both are module globals so tests can monkeypatch them; both can
also be injected directly. ``PAPER_TRADER_DAILY_RUN_STUB=1`` selects a
deterministic in-process price stub for safe write-path browser acceptance.
"""
from __future__ import annotations

import os
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func, select

from paper_trader.constants import JobRunStatus, PriceType, SessionType, WorkflowType
from paper_trader.db.models import (
    BenchmarkPrice,
    JobRun,
    PortfolioSnapshot,
    Position,
    PriceSnapshot,
)
from paper_trader.db.session import get_session

# Existing abstractions — reused, not re-implemented. Bound as module globals so
# tests can monkeypatch them (and callers may inject overrides directly).
from paper_trader.engine.market_data import fetch_latest_prices
from paper_trader.api.portfolio_valuation import load_portfolio_valuation
from paper_trader.api.current_alpha_daily_refresh import (
    load_current_alpha_daily_status,
    run_current_alpha_daily_refresh,
)
from paper_trader.api.current_alpha_book import (
    CurrentAlphaPreviewError,
    preview_or_create_current_alpha_book,
)
from paper_trader.workflows.snapshot import MissingPricesError, run_snapshot_workflow

_ET = ZoneInfo("America/New_York")

PHASE = "15-A"

# --------------------------------------------------------------------------- #
# Enums / constants
# --------------------------------------------------------------------------- #

# Explicit confirmation token required by the execute endpoint.
EXECUTE_CONFIRMATION = "RUN_MANUAL_DAILY_OPERATING_SESSION"

# Overall run / alignment status.
ST_ALIGNED = "ALIGNED"
ST_PREVIEW_WOULD_ALIGN = "PREVIEW_WOULD_ALIGN"
ST_STALE = "STALE"
ST_PARTIAL_COVERAGE = "PARTIAL_COVERAGE"
ST_BLOCKED = "BLOCKED"
ST_NO_COMPLETED_MARKET_DATE = "NO_COMPLETED_MARKET_DATE"
ST_ALREADY_COMPLETE = "ALREADY_COMPLETE"

# Per-stage result enum.
STG_CREATED = "CREATED"
STG_UPDATED = "UPDATED"
STG_REUSED = "REUSED"
STG_SKIPPED = "SKIPPED"
STG_FAILED = "FAILED"
STG_BLOCKED = "BLOCKED"
STG_PLANNED = "PLANNED"

# Canonical stage keys (ordered).
STAGE_RESOLVE = "RESOLVE_MARKET_DATE"
STAGE_PRICES = "PRICE_REFRESH"
STAGE_VALUATION = "VALUATION"
STAGE_SNAPSHOT = "PORTFOLIO_SNAPSHOT"
STAGE_ALPHA = "ALPHA_MARK"
STAGE_DECISION = "DECISION_GATE"
STAGE_ALIGNMENT = "ALIGNMENT"
STAGE_ORDER = [
    STAGE_RESOLVE, STAGE_PRICES, STAGE_VALUATION, STAGE_SNAPSHOT,
    STAGE_ALPHA, STAGE_DECISION, STAGE_ALIGNMENT,
]

# The benchmark ticker (kept in benchmark_prices, never traded).
SPY = "SPY"

# Run-audit JobRun idempotency-key prefix (distinguishes our audit rows from the
# snapshot workflow's own POST_MARKET JobRuns).
RUN_KEY_PREFIX = "daily-operating-run-"

# Env seam: force the deterministic in-process price stub (browser write-path).
STUB_ENV = "PAPER_TRADER_DAILY_RUN_STUB"
_STUB_PRICE = "100.00"

SAFETY_BADGES = [
    "MANUAL REVIEW",
    "PREVIEW ONLY",
    "NO ORDERS",
    "ORDERS DISABLED",
    "NO BROKER EXECUTION",
    "AUTOMATION OFF",
    "MANUAL DAILY OPERATING RUN",
]

# Tables this run may write — reported with before/after counts (Part E).
_WRITE_TABLES = ("price_snapshots", "benchmark_prices", "portfolio_snapshots", "job_runs")
# Forbidden side-effect tables — asserted unchanged (reported for transparency).
_GUARDED_TABLES = ("positions", "cash_ledger", "signals", "trade_decisions", "orders", "trades")


# --------------------------------------------------------------------------- #
# Small helpers
# --------------------------------------------------------------------------- #

def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _iso(d: Any) -> Optional[str]:
    if d is None:
        return None
    try:
        return d.isoformat()
    except Exception:  # noqa: BLE001
        return str(d)


def _parse_date(value: Any) -> Optional[date]:
    if isinstance(value, date):
        return value
    if not value:
        return None
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def latest_completed_market_date(now: datetime) -> date:
    """Resolve the latest COMPLETED US market date from the clock (pure).

    Regular session closes 16:00 US/Eastern on weekdays. If ``now`` (ET) is a
    weekday at/after the close, today is completed; otherwise the most recent
    prior weekday. No NYSE holiday calendar is consulted — this matches the
    existing ``engine/market_hours.py`` limitation and is a conservative,
    deterministic resolution (a holiday only makes the date one session too new,
    never fabricates data — the price provider simply returns the last real bar).
    """
    et = now.astimezone(_ET)
    if et.weekday() < 5 and et.timetz().replace(tzinfo=None) >= time(16, 0):
        candidate = et.date()
    else:
        candidate = et.date() - timedelta(days=1)
    while candidate.weekday() >= 5:  # walk back over the weekend
        candidate -= timedelta(days=1)
    return candidate


def _safety() -> dict[str, Any]:
    return {
        "manual_review": True,
        "preview_only_default": True,
        "paper_only": True,
        "creates_orders": False,
        "creates_signals": False,
        "creates_trade_decisions": False,
        "creates_fills": False,
        "no_broker_execution": True,
        "automation_off": True,
        "is_scheduled": False,
        "is_automation": False,
        "manual_user_triggered": True,
        "prediction_checked": False,
        "safety_badges": list(SAFETY_BADGES),
    }


def _provenance(*, wrote: bool) -> dict[str, Any]:
    return {
        "phase": PHASE,
        "generated_at": _now_iso(),
        "read_only": not wrote,
        "wrote_to_database": bool(wrote),
        "allowed_write_tables": list(_WRITE_TABLES),
        "guarded_unchanged_tables": list(_GUARDED_TABLES),
        "created_orders": False,
        "created_signals": False,
        "created_trade_decisions": False,
        "created_fills": False,
        "modified_positions": False,
        "modified_cash": False,
        "called_prediction_service": False,
        "made_loopback_http_calls": False,
        "sources": [
            "engine.market_data.fetch_latest_prices (owned EOD provider)",
            "workflows.snapshot.run_snapshot_workflow (portfolio snapshot)",
            "current_alpha_daily_refresh.run_current_alpha_daily_refresh (alpha mark)",
            "portfolio_valuation.load_portfolio_valuation (canonical current mark)",
            "current_alpha_daily_refresh.load_current_alpha_daily_status (alpha dates)",
        ],
    }


def _stage(stage: str, status: str, detail: str, **extra: Any) -> dict[str, Any]:
    row = {"stage": stage, "status": status, "detail": detail}
    row.update(extra)
    return row


# --------------------------------------------------------------------------- #
# Read helpers — current known dates (no writes)
# --------------------------------------------------------------------------- #

def _valuation_dates(valuation: dict[str, Any]) -> dict[str, Any]:
    """Extract the canonical portfolio dates from a portfolio-valuation view."""
    cm = valuation.get("current_mark") or {}
    snap = valuation.get("latest_snapshot") or None
    return {
        "price_snapshot_market_date": cm.get("as_of_market_date"),
        "portfolio_snapshot_market_date": (snap.get("market_date") if snap else None),
        "coverage_complete": bool(cm.get("valuation_complete")),
        "covered_position_count": cm.get("covered_position_count"),
        "total_position_count": cm.get("total_position_count"),
        "price_source": cm.get("price_source"),
        "freshness_status": cm.get("freshness_status"),
        "current_total_value": cm.get("current_total_value"),
    }


def _alpha_dates(alpha_status: dict[str, Any]) -> dict[str, Any]:
    """Extract the current-alpha mark dates (Top25/Top50/SPY share one mark)."""
    valid = bool(alpha_status.get("latest_valid_mark_available"))
    mark_date = alpha_status.get("latest_valid_mark_date") if valid else None
    blocked = bool(alpha_status.get("last_run_blocked"))
    return {
        "alpha_top25_market_date": mark_date,
        "alpha_top50_market_date": mark_date,
        "spy_market_date": mark_date,
        "alpha_available": valid,
        "alpha_blocked": blocked,
        "alpha_freshness": alpha_status.get("latest_valid_mark_freshness"),
        "last_run_result": alpha_status.get("last_run_result"),
        "last_run_at": alpha_status.get("last_run_at"),
    }


# --------------------------------------------------------------------------- #
# Alignment contract (Part D) — pure
# --------------------------------------------------------------------------- #

def compute_market_date_alignment(
    *,
    required_market_date: Optional[Any],
    price_snapshot_market_date: Optional[Any] = None,
    portfolio_snapshot_market_date: Optional[Any] = None,
    alpha_top25_market_date: Optional[Any] = None,
    alpha_top50_market_date: Optional[Any] = None,
    spy_market_date: Optional[Any] = None,
    command_center_market_date: Optional[Any] = None,
    portfolio_terminal_market_date: Optional[Any] = None,
    coverage_complete: bool = True,
    alpha_blocked: bool = False,
) -> dict[str, Any]:
    """Return the Part D alignment dict from the observed dataset dates (pure).

    ``aligned`` is true only when every PRESENT required dataset represents the
    same market date as ``required_market_date``. Empty / unsupported datasets
    (None) are excluded — never silently treated as current. Coverage gaps and a
    blocked alpha mark are surfaced as blocking mismatches and downgrade the
    status to PARTIAL_COVERAGE / BLOCKED so mixed dates are never labelled
    current.
    """
    req = _iso(required_market_date)
    datasets = {
        "price_snapshot_market_date": _iso(price_snapshot_market_date),
        "portfolio_snapshot_market_date": _iso(portfolio_snapshot_market_date),
        "alpha_top25_market_date": _iso(alpha_top25_market_date),
        "alpha_top50_market_date": _iso(alpha_top50_market_date),
        "spy_market_date": _iso(spy_market_date),
        "command_center_market_date": _iso(command_center_market_date),
        "portfolio_terminal_market_date": _iso(portfolio_terminal_market_date),
    }
    present = {k: v for k, v in datasets.items() if v}
    mismatches: list[dict[str, Any]] = []
    blocking: list[dict[str, Any]] = []

    if req is None:
        status = ST_NO_COMPLETED_MARKET_DATE
        aligned = False
    else:
        for k, v in present.items():
            if v != req:
                mismatches.append({"dataset": k, "market_date": v, "expected": req})
        aligned = len(mismatches) == 0 and bool(present)
        if not coverage_complete:
            blocking.append({
                "dataset": "price_snapshot_market_date",
                "reason": "One or more open positions have no completed EOD price "
                          "for the required market date (partial coverage).",
            })
        if alpha_blocked:
            blocking.append({
                "dataset": "alpha_mark",
                "reason": "The current-alpha daily mark refresh is blocked (provider "
                          "entitlement / error); the paper mark cannot be advanced.",
            })
        if blocking:
            status = ST_BLOCKED if any(b["dataset"] == "alpha_mark" for b in blocking) \
                else ST_PARTIAL_COVERAGE
        elif aligned:
            status = ST_ALIGNED
        else:
            status = ST_STALE

    return {
        "required_market_date": req,
        "price_snapshot_market_date": datasets["price_snapshot_market_date"],
        "portfolio_snapshot_market_date": datasets["portfolio_snapshot_market_date"],
        "alpha_top25_market_date": datasets["alpha_top25_market_date"],
        "alpha_top50_market_date": datasets["alpha_top50_market_date"],
        "spy_market_date": datasets["spy_market_date"],
        "command_center_market_date": datasets["command_center_market_date"],
        "portfolio_terminal_market_date": datasets["portfolio_terminal_market_date"],
        "aligned": aligned,
        "status": status,
        "mismatches": mismatches,
        "blocking_mismatches": blocking,
        "coverage_complete": bool(coverage_complete),
    }


# --------------------------------------------------------------------------- #
# Row-count helpers (Part E)
# --------------------------------------------------------------------------- #

_TABLE_MODELS = {
    "price_snapshots": PriceSnapshot,
    "benchmark_prices": BenchmarkPrice,
    "portfolio_snapshots": PortfolioSnapshot,
    "job_runs": JobRun,
    "positions": Position,
}


def _count(session, model) -> int:
    return int(session.execute(select(func.count()).select_from(model)).scalar() or 0)


def _snapshot_counts(session) -> dict[str, int]:
    """Row counts for the write + a representative guarded table (positions)."""
    out: dict[str, int] = {}
    for name, model in _TABLE_MODELS.items():
        try:
            out[name] = _count(session, model)
        except Exception:  # noqa: BLE001
            out[name] = -1
    return out


# --------------------------------------------------------------------------- #
# Price universe (Part C)
# --------------------------------------------------------------------------- #

def _owned_tickers(session) -> list[str]:
    rows = session.execute(select(Position.ticker)).scalars().all()
    return [str(t).upper() for t in rows]


def _champion_tickers(package_dir, book_dir) -> tuple[list[str], list[str], Optional[str]]:
    """Read-only champion Top25 + Top50 tickers (commit=False writes nothing).

    Returns (top25, top50, warning). A missing Phase 13-A package is a normal
    state → ([], [], reason).
    """
    top25: list[str] = []
    top50: list[str] = []
    warning: Optional[str] = None
    try:
        created25 = preview_or_create_current_alpha_book(
            package_dir, book_size=25, commit=False, book_dir=book_dir)
        top25 = [str(p.get("ticker")).upper()
                 for p in (created25.get("book") or {}).get("positions", [])
                 if p.get("ticker")]
        created50 = preview_or_create_current_alpha_book(
            package_dir, book_size=50, commit=False, book_dir=book_dir)
        top50 = [str(p.get("ticker")).upper()
                 for p in (created50.get("book") or {}).get("positions", [])
                 if p.get("ticker")]
    except CurrentAlphaPreviewError as exc:
        warning = ("Champion Top25/Top50 tickers unavailable (Phase 13-A package "
                   f"missing): {exc}. Refreshing owned positions + SPY only.")
    except Exception as exc:  # noqa: BLE001
        warning = f"Champion tickers unavailable: {str(exc)[:160]}"
    return top25, top50, warning


def _dedupe(seq: list[str]) -> list[str]:
    seen: list[str] = []
    for x in seq:
        if x and x not in seen:
            seen.append(x)
    return seen


def _stub_fetch(tickers: list[str]) -> tuple[list[dict], list[dict]]:
    """Deterministic in-process price stub (browser write-path / offline)."""
    return ([{"ticker": t.upper(), "price": _STUB_PRICE} for t in tickers], [])


def _resolve_price_fetcher(price_fetcher: Optional[Callable]) -> Callable:
    if price_fetcher is not None:
        return price_fetcher
    if os.environ.get(STUB_ENV, "").lower() in ("1", "true", "on", "yes"):
        return _stub_fetch
    return fetch_latest_prices


# --------------------------------------------------------------------------- #
# Idempotent price-snapshot upsert (Part E)
# --------------------------------------------------------------------------- #

def _existing_equity_row(session, ticker: str, market_date: date) -> bool:
    """True if an equivalent completed-EOD price row already exists."""
    row = session.execute(
        select(PriceSnapshot.id).where(
            PriceSnapshot.ticker == ticker,
            PriceSnapshot.market_date == market_date,
            PriceSnapshot.price_type == PriceType.CLOSE,
            PriceSnapshot.session_type == SessionType.REGULAR,
        ).limit(1)
    ).first()
    return row is not None


def _existing_benchmark_row(session, ticker: str, market_date: date) -> bool:
    row = session.execute(
        select(BenchmarkPrice.id).where(
            BenchmarkPrice.ticker == ticker,
            BenchmarkPrice.market_date == market_date,
            BenchmarkPrice.session_type == SessionType.REGULAR,
        ).limit(1)
    ).first()
    return row is not None


def _to_price(value: Any) -> Optional[Decimal]:
    try:
        p = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    return p if p > 0 else None


# --------------------------------------------------------------------------- #
# EXECUTE — price refresh stage
# --------------------------------------------------------------------------- #

def _refresh_prices(
    session, *, market_date: date, now: datetime, fetcher: Callable,
    package_dir, book_dir,
) -> dict[str, Any]:
    """Fetch + idempotently upsert the deduped owned/champion/SPY price universe.

    Writes ONLY price_snapshots (equities) + benchmark_prices (SPY). A per-ticker
    failure never aborts the run. Returns a coverage + stage report.
    """
    owned = _owned_tickers(session)
    top25, top50, champ_warn = _champion_tickers(package_dir, book_dir)
    equities = _dedupe(owned + top25 + top50)  # SPY handled separately
    requested = _dedupe(equities + [SPY])

    successful, failures = fetcher(requested)
    price_by_ticker: dict[str, Decimal] = {}
    fetch_failures = {str(f.get("ticker", "")).upper(): f.get("reason", "unknown")
                      for f in (failures or [])}
    for row in (successful or []):
        t = str(row.get("ticker", "")).upper()
        p = _to_price(row.get("price"))
        if p is None:
            fetch_failures.setdefault(t, "invalid or non-positive price")
            continue
        price_by_ticker[t] = p

    created = reused = 0
    written_tickers: list[str] = []
    for t in equities:
        p = price_by_ticker.get(t)
        if p is None:
            fetch_failures.setdefault(t, "no price returned")
            continue
        if _existing_equity_row(session, t, market_date):
            reused += 1
            continue
        session.add(PriceSnapshot(
            ticker=t, price=p, session_type=SessionType.REGULAR,
            price_type=PriceType.CLOSE, exchange=None, data_source="yahoo_finance",
            snapshot_ts=now, market_date=market_date, job_run_id=None,
        ))
        created += 1
        written_tickers.append(t)

    # SPY -> benchmark_prices (its proper home; feeds snapshot benchmark fields).
    spy_created = spy_reused = 0
    spy_price = price_by_ticker.get(SPY)
    if spy_price is not None:
        if _existing_benchmark_row(session, SPY, market_date):
            spy_reused = 1
        else:
            session.add(BenchmarkPrice(
                ticker=SPY, price=spy_price, session_type=SessionType.REGULAR,
                snapshot_ts=now, market_date=market_date, job_run_id=None,
            ))
            spy_created = 1
    else:
        fetch_failures.setdefault(SPY, "no price returned")

    successful_count = sum(1 for t in requested if t in price_by_ticker)
    failed_tickers = [{"ticker": t, "reason": r} for t, r in sorted(fetch_failures.items())]
    coverage_pct = round(100.0 * successful_count / len(requested), 2) if requested else 0.0

    status = STG_CREATED if (created or spy_created) else (
        STG_REUSED if (reused or spy_reused) else STG_SKIPPED)
    if fetch_failures and successful_count == 0:
        status = STG_FAILED

    return {
        "stage_status": status,
        "requested_ticker_count": len(requested),
        "successful_ticker_count": successful_count,
        "failed_ticker_count": len(failed_tickers),
        "failed_tickers": failed_tickers,
        "market_date": _iso(market_date),
        "provider": "yahoo_finance",
        "coverage_pct": coverage_pct,
        "owned_count": len(owned),
        "champion_top25_count": len(top25),
        "champion_top50_count": len(top50),
        "spy_included": True,
        "price_snapshots_created": created,
        "price_snapshots_reused": reused,
        "benchmark_created": spy_created,
        "benchmark_reused": spy_reused,
        "champion_warning": champ_warn,
    }


# --------------------------------------------------------------------------- #
# EXECUTE — portfolio snapshot stage
# --------------------------------------------------------------------------- #

def _run_portfolio_snapshot(*, market_date: date, now: datetime) -> dict[str, Any]:
    """Create exactly one PortfolioSnapshot for market_date, or reuse it.

    ``run_snapshot_workflow`` is idempotent on market_date (returns the existing
    row without a new JobRun) — so a rerun reports REUSED and writes nothing.
    """
    key = f"daily-run-snapshot-{market_date}-{uuid.uuid4().hex[:8]}"
    existed = None
    try:
        with get_session() as session:
            existed = session.execute(
                select(PortfolioSnapshot.id).where(
                    PortfolioSnapshot.market_date == market_date).limit(1)
            ).first() is not None
    except Exception:  # noqa: BLE001
        existed = None
    try:
        summary = run_snapshot_workflow(idempotency_key=key, market_date=market_date, now=now)
        return {
            "stage_status": STG_REUSED if existed else STG_CREATED,
            "market_date": _iso(market_date),
            "total_value": summary.get("total_value"),
            "cash": summary.get("cash"),
            "positions_value": summary.get("positions_value"),
            "open_position_count": summary.get("open_position_count"),
        }
    except MissingPricesError as exc:
        return {"stage_status": STG_BLOCKED, "market_date": _iso(market_date),
                "reason": str(exc)}
    except RuntimeError as exc:
        return {"stage_status": STG_FAILED, "market_date": _iso(market_date),
                "reason": str(exc)}


# --------------------------------------------------------------------------- #
# EXECUTE — alpha mark stage
# --------------------------------------------------------------------------- #

def _run_alpha_mark(*, execute: bool, alpha_runner: Optional[Callable]) -> dict[str, Any]:
    """Run the existing current-alpha daily mark (Top25/Top50/SPY), idempotent."""
    runner = alpha_runner if alpha_runner is not None else run_current_alpha_daily_refresh
    try:
        result = runner(commit=execute)
    except Exception as exc:  # noqa: BLE001
        return {"stage_status": STG_FAILED, "reason": str(exc)[:200]}

    action = result.get("action")
    refresh_result = result.get("refresh_result") or result.get("status")
    if result.get("blocked") or (isinstance(refresh_result, str)
                                 and refresh_result.startswith("BLOCKED")):
        stage_status = STG_BLOCKED
    elif action == "SNAPSHOTS_WRITTEN":
        stage_status = STG_CREATED
    elif refresh_result in ("NO_NEW_MARK_DATE", "REFRESH_UNAVAILABLE") or action == "NO_SNAPSHOT":
        stage_status = STG_REUSED if refresh_result == "NO_NEW_MARK_DATE" else STG_SKIPPED
    elif action == "SNAPSHOTS_PREVIEWED":
        stage_status = STG_PLANNED
    else:
        stage_status = STG_SKIPPED

    snaps = result.get("snapshots") or {}
    return {
        "stage_status": stage_status,
        "refresh_result": refresh_result,
        "mark_date": result.get("mark_date") or result.get("latest_valid_mark_date"),
        "committed": bool(result.get("committed")),
        "top25": (snaps.get("top25") or {}).get("status") if isinstance(snaps.get("top25"), dict) else None,
        "top50": (snaps.get("top50") or {}).get("status") if isinstance(snaps.get("top50"), dict) else None,
        "guidance": result.get("guidance"),
    }


# --------------------------------------------------------------------------- #
# Run-audit JobRun (Part B step 10 / Part E)
# --------------------------------------------------------------------------- #

def _find_run_job(session, market_date: date) -> Optional[JobRun]:
    return session.execute(
        select(JobRun).where(JobRun.idempotency_key == f"{RUN_KEY_PREFIX}{market_date}")
    ).scalars().first()


# --------------------------------------------------------------------------- #
# Public — read-only status (Part F GET)
# --------------------------------------------------------------------------- #

def _last_run_audit(session, market_date: date) -> Optional[dict[str, Any]]:
    row = session.execute(
        select(JobRun)
        .where(JobRun.idempotency_key.like(f"{RUN_KEY_PREFIX}%"))
        .order_by(JobRun.started_at.desc())
        .limit(1)
    ).scalars().first()
    if row is None:
        return None
    return {
        "market_date": _iso(row.market_date),
        "status": row.status,
        "started_at": _iso(row.started_at),
        "completed_at": _iso(row.completed_at),
        "result_summary": row.result_summary,
    }


def load_daily_operating_run_status(
    *,
    now: Optional[datetime] = None,
    valuation: Optional[dict[str, Any]] = None,
    alpha_status: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Read-only current state of the daily operating run (Part F GET).

    Reports the last successful run, the latest completed market date, freshness,
    the current alignment, latest stage results, blockers and safety. Performs NO
    writes, NO loopback HTTP, NO prediction / provider call.
    """
    warnings: list[str] = []
    now = now or datetime.now(tz=timezone.utc)
    required = latest_completed_market_date(now)

    if valuation is None:
        try:
            valuation = load_portfolio_valuation()
        except Exception as exc:  # noqa: BLE001
            valuation = {"current_mark": {}, "latest_snapshot": None}
            warnings.append(f"Canonical valuation unavailable: {str(exc)[:160]}")
    if alpha_status is None:
        try:
            alpha_status = load_current_alpha_daily_status()
        except Exception as exc:  # noqa: BLE001
            alpha_status = {}
            warnings.append(f"Current-alpha status unavailable: {str(exc)[:160]}")

    vd = _valuation_dates(valuation)
    ad = _alpha_dates(alpha_status)

    alignment = compute_market_date_alignment(
        required_market_date=required,
        price_snapshot_market_date=vd["price_snapshot_market_date"],
        portfolio_snapshot_market_date=vd["portfolio_snapshot_market_date"],
        alpha_top25_market_date=ad["alpha_top25_market_date"],
        alpha_top50_market_date=ad["alpha_top50_market_date"],
        spy_market_date=ad["spy_market_date"],
        command_center_market_date=vd["price_snapshot_market_date"],
        portfolio_terminal_market_date=vd["price_snapshot_market_date"],
        coverage_complete=vd["coverage_complete"],
        alpha_blocked=ad["alpha_blocked"],
    )

    last_run: Optional[dict[str, Any]] = None
    try:
        with get_session() as session:
            last_run = _last_run_audit(session, required)
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Run audit unavailable: {str(exc)[:160]}")

    blockers = [b["reason"] for b in alignment["blocking_mismatches"]]

    return {
        "status": alignment["status"],
        "required_market_date": _iso(required),
        "freshness_status": vd["freshness_status"],
        "coverage_complete": vd["coverage_complete"],
        "covered_position_count": vd["covered_position_count"],
        "total_position_count": vd["total_position_count"],
        "alignment": alignment,
        "alpha": {
            "available": ad["alpha_available"],
            "latest_valid_mark_date": ad["alpha_top25_market_date"],
            "freshness": ad["alpha_freshness"],
            "blocked": ad["alpha_blocked"],
            "last_run_result": ad["last_run_result"],
            "last_run_at": ad["last_run_at"],
        },
        "last_run": last_run,
        "blockers": blockers,
        "confirmation_required": EXECUTE_CONFIRMATION,
        "warnings": warnings,
        "safety": _safety(),
        "provenance": _provenance(wrote=False),
        "loaded_at": _now_iso(),
    }


# --------------------------------------------------------------------------- #
# Public — the daily operating session (Part B)
# --------------------------------------------------------------------------- #

def _final_outcome(*, status: str, alignment: dict[str, Any], prices: dict[str, Any],
                   snapshot: dict[str, Any], alpha: dict[str, Any]) -> str:
    date_txt = alignment.get("required_market_date") or "unknown date"
    if status in (ST_ALIGNED, ST_ALREADY_COMPLETE):
        return f"DAILY RUN COMPLETE — ALL OPERATING DATA ALIGNED TO {date_txt}"
    parts: list[str] = []
    succ = prices.get("successful_ticker_count")
    req = prices.get("requested_ticker_count")
    if req:
        parts.append(f"{succ}/{req} PRICES LOADED")
    if snapshot.get("stage_status") in (STG_CREATED, STG_REUSED):
        parts.append("PORTFOLIO ALIGNED")
    elif snapshot.get("stage_status") == STG_BLOCKED:
        parts.append("PORTFOLIO SNAPSHOT BLOCKED")
    if alpha.get("stage_status") == STG_BLOCKED:
        parts.append("ALPHA PAPER MARK BLOCKED")
    return "DAILY RUN PARTIAL — " + "; ".join(parts) if parts else \
        f"DAILY RUN {status} — see stage results"


def run_daily_operating_session(
    *,
    execute: bool,
    requested_by: str = "manual_ui",
    now: Optional[datetime] = None,
    price_fetcher: Optional[Callable] = None,
    alpha_runner: Optional[Callable] = None,
    package_dir: Optional[Any] = None,
    book_dir: Optional[Any] = None,
) -> dict[str, Any]:
    """Preview or execute the one-click daily operating run (Part B).

    ``execute=False`` (PREVIEW) performs NO database writes: it resolves the
    completed market date, reports currently-known dates and alignment, and
    returns the full planned stage list. ``execute=True`` performs ONLY the
    permitted writes (price snapshots, one portfolio snapshot, the existing alpha
    paper-mark history, and JobRun audit rows), then returns the alignment and
    reconciliation report. Never raises — a failing stage degrades to a controlled
    status.
    """
    started = datetime.now(tz=timezone.utc)
    now = now or started
    warnings: list[str] = []
    fetcher = _resolve_price_fetcher(price_fetcher)

    required = latest_completed_market_date(now)
    stages: list[dict[str, Any]] = [
        _stage(STAGE_RESOLVE, STG_REUSED if execute else STG_PLANNED,
               f"Latest completed US market date resolved to {required.isoformat()}.",
               market_date=required.isoformat())
    ]

    prices: dict[str, Any] = {}
    snapshot: dict[str, Any] = {}
    alpha: dict[str, Any] = {}
    row_counts: dict[str, Any] = {}
    run_job_id: Optional[str] = None

    # ------------------------------------------------------------------ #
    # PREVIEW: read-only planning only.
    # ------------------------------------------------------------------ #
    if not execute:
        try:
            valuation = load_portfolio_valuation()
        except Exception as exc:  # noqa: BLE001
            valuation = {"current_mark": {}, "latest_snapshot": None}
            warnings.append(f"Valuation unavailable: {str(exc)[:160]}")
        try:
            alpha_status = load_current_alpha_daily_status()
        except Exception as exc:  # noqa: BLE001
            alpha_status = {}
            warnings.append(f"Alpha status unavailable: {str(exc)[:160]}")
        vd = _valuation_dates(valuation)
        ad = _alpha_dates(alpha_status)

        try:
            with get_session() as session:
                owned = _owned_tickers(session)
        except Exception as exc:  # noqa: BLE001
            owned = []
            warnings.append(f"Owned tickers unavailable: {str(exc)[:160]}")
        top25, top50, champ_warn = _champion_tickers(package_dir, book_dir)
        if champ_warn:
            warnings.append(champ_warn)
        requested_universe = _dedupe(owned + top25 + top50 + [SPY])

        prices = {
            "stage_status": STG_PLANNED,
            "requested_ticker_count": len(requested_universe),
            "owned_count": len(owned),
            "champion_top25_count": len(top25),
            "champion_top50_count": len(top50),
            "spy_included": True,
            "market_date": required.isoformat(),
            "provider": "yahoo_finance",
        }
        stages.append(_stage(
            STAGE_PRICES, STG_PLANNED,
            f"Would fetch + idempotently upsert {len(requested_universe)} price(s) "
            f"(owned {len(owned)} + champion {len(top25)}/{len(top50)} + SPY) at "
            f"{required.isoformat()}.", **prices))
        stages.append(_stage(
            STAGE_VALUATION, STG_PLANNED,
            "Would recompute the canonical portfolio valuation at the completed date."))
        stages.append(_stage(
            STAGE_SNAPSHOT, STG_PLANNED,
            f"Would create/reuse exactly one PortfolioSnapshot for {required.isoformat()}."))
        stages.append(_stage(
            STAGE_ALPHA, STG_PLANNED,
            "Would run the existing current-alpha daily mark (Top25/Top50/SPY), "
            "appending paper history idempotently."))
        stages.append(_stage(
            STAGE_DECISION, STG_PLANNED,
            "Would refresh the decision-gate / performance outputs (read-only)."))

        alignment = compute_market_date_alignment(
            required_market_date=required,
            price_snapshot_market_date=vd["price_snapshot_market_date"],
            portfolio_snapshot_market_date=vd["portfolio_snapshot_market_date"],
            alpha_top25_market_date=ad["alpha_top25_market_date"],
            alpha_top50_market_date=ad["alpha_top50_market_date"],
            spy_market_date=ad["spy_market_date"],
            command_center_market_date=vd["price_snapshot_market_date"],
            portfolio_terminal_market_date=vd["price_snapshot_market_date"],
            coverage_complete=vd["coverage_complete"],
            alpha_blocked=ad["alpha_blocked"],
        )
        _align_note = ("Already aligned." if alignment["aligned"]
                       else f"A run would align to {required.isoformat()}.")
        stages.append(_stage(
            STAGE_ALIGNMENT, STG_PLANNED,
            f"Current alignment: {alignment['status']}. {_align_note}"))

        if alignment["status"] == ST_NO_COMPLETED_MARKET_DATE:
            overall = ST_NO_COMPLETED_MARKET_DATE
        elif alignment["aligned"] and alignment["coverage_complete"]:
            overall = ST_ALREADY_COMPLETE
        else:
            overall = ST_PREVIEW_WOULD_ALIGN

        completed = datetime.now(tz=timezone.utc)
        return {
            "status": overall,
            "mode": "PREVIEW",
            "execute": False,
            "requested_by": requested_by,
            "required_market_date": required.isoformat(),
            "started_at": _iso(started),
            "completed_at": _iso(completed),
            "duration_ms": int((completed - started).total_seconds() * 1000),
            "stages": stages,
            "price_universe": prices,
            "alignment": alignment,
            "reconciliation": _reconciliation(valuation, alignment),
            "row_counts": {},
            "alpha": {"available": ad["alpha_available"],
                      "latest_valid_mark_date": ad["alpha_top25_market_date"],
                      "blocked": ad["alpha_blocked"]},
            "blockers": [b["reason"] for b in alignment["blocking_mismatches"]],
            "final_outcome": ("ALREADY ALIGNED TO " + required.isoformat()
                              if overall == ST_ALREADY_COMPLETE
                              else "PREVIEW — A DAILY RUN WOULD ALIGN TO " + required.isoformat()),
            "run_job_run_id": None,
            "confirmation_required": EXECUTE_CONFIRMATION,
            "warnings": warnings,
            "safety": _safety(),
            "provenance": _provenance(wrote=False),
            "loaded_at": _now_iso(),
        }

    # ------------------------------------------------------------------ #
    # EXECUTE: the permitted writes.
    # ------------------------------------------------------------------ #
    before: dict[str, int] = {}
    job_row_id: Optional[uuid.UUID] = None
    run_already_complete = False
    try:
        with get_session() as session:
            before = _snapshot_counts(session)
            existing = _find_run_job(session, required)
            if existing is not None and existing.status == JobRunStatus.RUNNING:
                # Controlled concurrency guard — no writes.
                after = _snapshot_counts(session)
                warnings.append("A daily operating run is already RUNNING for this date.")
                return _blocked_execute_payload(
                    required=required, requested_by=requested_by, started=started,
                    stages=stages, before=before, after=after, warnings=warnings)
            if existing is not None and existing.status == JobRunStatus.COMPLETED:
                run_already_complete = True
                job_row_id = existing.id
            elif existing is not None:  # FAILED — reuse the row, retry.
                existing.status = JobRunStatus.RUNNING
                existing.started_at = now
                existing.completed_at = None
                existing.error_detail = None
                session.commit()
                job_row_id = existing.id
            else:
                job = JobRun(
                    idempotency_key=f"{RUN_KEY_PREFIX}{required}",
                    workflow_type=WorkflowType.POST_MARKET,
                    market_date=required, status=JobRunStatus.RUNNING, started_at=now,
                )
                session.add(job)
                session.commit()
                job_row_id = job.id
            run_job_id = str(job_row_id) if job_row_id else None
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Run-audit setup failed: {str(exc)[:160]}")

    # Stage 2: price refresh (own session so writes commit even if a later stage
    # degrades).
    try:
        with get_session() as session:
            prices = _refresh_prices(
                session, market_date=required, now=now, fetcher=fetcher,
                package_dir=package_dir, book_dir=book_dir)
        if prices.get("champion_warning"):
            warnings.append(prices["champion_warning"])
    except Exception as exc:  # noqa: BLE001
        prices = {"stage_status": STG_FAILED, "reason": str(exc)[:200],
                  "requested_ticker_count": 0, "successful_ticker_count": 0,
                  "failed_ticker_count": 0, "failed_tickers": [], "coverage_pct": 0.0}
        warnings.append(f"Price refresh failed: {str(exc)[:160]}")
    stages.append(_stage(
        STAGE_PRICES, prices.get("stage_status", STG_FAILED),
        f"Refreshed {prices.get('successful_ticker_count', 0)}/"
        f"{prices.get('requested_ticker_count', 0)} prices "
        f"(created {prices.get('price_snapshots_created', 0)}, "
        f"reused {prices.get('price_snapshots_reused', 0)}; SPY "
        f"created {prices.get('benchmark_created', 0)}, reused "
        f"{prices.get('benchmark_reused', 0)}).",
        **{k: v for k, v in prices.items() if k != "stage_status"}))

    # Stage 3: canonical valuation (read-only recompute).
    try:
        valuation = load_portfolio_valuation()
    except Exception as exc:  # noqa: BLE001
        valuation = {"current_mark": {}, "latest_snapshot": None}
        warnings.append(f"Valuation recompute failed: {str(exc)[:160]}")
    vd = _valuation_dates(valuation)
    stages.append(_stage(
        STAGE_VALUATION, STG_UPDATED,
        f"Recomputed canonical valuation — total {vd.get('current_total_value')}, "
        f"as-of {vd.get('price_snapshot_market_date')}, coverage "
        f"{vd.get('covered_position_count')}/{vd.get('total_position_count')}.",
        as_of_market_date=vd.get("price_snapshot_market_date"),
        coverage_complete=vd.get("coverage_complete")))

    # Stage 4: portfolio snapshot.
    snapshot = _run_portfolio_snapshot(market_date=required, now=now)
    stages.append(_stage(
        STAGE_SNAPSHOT, snapshot.get("stage_status", STG_FAILED),
        f"PortfolioSnapshot for {required.isoformat()}: "
        f"{snapshot.get('stage_status')}."
        + (f" {snapshot.get('reason')}" if snapshot.get("reason") else ""),
        **{k: v for k, v in snapshot.items() if k != "stage_status"}))

    # Stage 5: alpha mark.
    alpha = _run_alpha_mark(execute=True, alpha_runner=alpha_runner)
    stages.append(_stage(
        STAGE_ALPHA, alpha.get("stage_status", STG_SKIPPED),
        f"Current-alpha daily mark: {alpha.get('refresh_result')} "
        f"({alpha.get('stage_status')}).",
        **{k: v for k, v in alpha.items() if k != "stage_status"}))

    # Stage 6: decision-gate / performance recompute (read-only; reported).
    stages.append(_stage(
        STAGE_DECISION, STG_UPDATED,
        "Decision-gate / performance outputs recompute on read from the refreshed "
        "artifacts (read-only)."))

    # Re-read alpha dates after the mark.
    try:
        alpha_status = load_current_alpha_daily_status()
    except Exception as exc:  # noqa: BLE001
        alpha_status = {}
        warnings.append(f"Alpha status unavailable: {str(exc)[:160]}")
    ad = _alpha_dates(alpha_status)

    alpha_blocked = ad["alpha_blocked"] or alpha.get("stage_status") == STG_BLOCKED
    alignment = compute_market_date_alignment(
        required_market_date=required,
        price_snapshot_market_date=vd["price_snapshot_market_date"],
        portfolio_snapshot_market_date=vd["portfolio_snapshot_market_date"]
        if snapshot.get("stage_status") in (STG_CREATED, STG_REUSED, STG_UPDATED)
        else vd["portfolio_snapshot_market_date"],
        alpha_top25_market_date=ad["alpha_top25_market_date"],
        alpha_top50_market_date=ad["alpha_top50_market_date"],
        spy_market_date=ad["spy_market_date"],
        command_center_market_date=vd["price_snapshot_market_date"],
        portfolio_terminal_market_date=vd["price_snapshot_market_date"],
        coverage_complete=vd["coverage_complete"],
        alpha_blocked=alpha_blocked,
    )
    stages.append(_stage(
        STAGE_ALIGNMENT,
        STG_REUSED if alignment["aligned"] else STG_UPDATED,
        f"Alignment after run: {alignment['status']} "
        f"(aligned={alignment['aligned']}).",
        aligned=alignment["aligned"], alignment_status=alignment["status"]))

    # Finalise counts + run-audit JobRun.
    after: dict[str, int] = {}
    try:
        with get_session() as session:
            after = _snapshot_counts(session)
            if job_row_id is not None:
                job = session.get(JobRun, job_row_id)
                if job is not None:
                    job.status = JobRunStatus.COMPLETED
                    job.completed_at = datetime.now(tz=timezone.utc)
                    job.result_summary = {
                        "required_market_date": required.isoformat(),
                        "alignment_status": alignment["status"],
                        "aligned": alignment["aligned"],
                        "prices_created": prices.get("price_snapshots_created", 0),
                        "prices_reused": prices.get("price_snapshots_reused", 0),
                        "snapshot_status": snapshot.get("stage_status"),
                        "alpha_status": alpha.get("stage_status"),
                        "requested_by": requested_by,
                    }
                    session.commit()
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Run-audit finalise failed: {str(exc)[:160]}")

    for t in _WRITE_TABLES:
        row_counts[t] = {"before": before.get(t), "after": after.get(t)}
    row_counts["positions"] = {"before": before.get("positions"),
                               "after": after.get("positions")}

    # Overall status.
    any_created = (
        prices.get("price_snapshots_created", 0) or prices.get("benchmark_created", 0)
        or snapshot.get("stage_status") == STG_CREATED
        or alpha.get("stage_status") == STG_CREATED
    )
    if alignment["status"] == ST_NO_COMPLETED_MARKET_DATE:
        overall = ST_NO_COMPLETED_MARKET_DATE
    elif any(b["dataset"] == "alpha_mark" for b in alignment["blocking_mismatches"]):
        overall = ST_BLOCKED
    elif not alignment["coverage_complete"]:
        overall = ST_PARTIAL_COVERAGE
    elif alignment["aligned"]:
        # A same-date rerun that wrote nothing new is ALREADY_COMPLETE.
        overall = (ST_ALREADY_COMPLETE
                   if (run_already_complete and not any_created) else ST_ALIGNED)
    else:
        overall = ST_STALE

    completed = datetime.now(tz=timezone.utc)
    return {
        "status": overall,
        "mode": "EXECUTE",
        "execute": True,
        "requested_by": requested_by,
        "required_market_date": required.isoformat(),
        "started_at": _iso(started),
        "completed_at": _iso(completed),
        "duration_ms": int((completed - started).total_seconds() * 1000),
        "run_already_complete": run_already_complete,
        "stages": stages,
        "price_universe": prices,
        "alignment": alignment,
        "reconciliation": _reconciliation(valuation, alignment),
        "row_counts": row_counts,
        "alpha": {"available": ad["alpha_available"],
                  "latest_valid_mark_date": ad["alpha_top25_market_date"],
                  "blocked": alpha_blocked,
                  "stage_status": alpha.get("stage_status")},
        "blockers": [b["reason"] for b in alignment["blocking_mismatches"]],
        "final_outcome": _final_outcome(status=overall, alignment=alignment,
                                        prices=prices, snapshot=snapshot, alpha=alpha),
        "run_job_run_id": run_job_id,
        "confirmation_required": EXECUTE_CONFIRMATION,
        "warnings": warnings,
        "safety": _safety(),
        "provenance": _provenance(wrote=True),
        "loaded_at": _now_iso(),
    }


def _reconciliation(valuation: dict[str, Any], alignment: dict[str, Any]) -> dict[str, Any]:
    recon = (valuation or {}).get("reconciliation") or {}
    return {
        "cash_plus_positions": recon.get("cash_plus_positions"),
        "reported_current_total": recon.get("reported_current_total"),
        "reconciliation_delta": recon.get("reconciliation_delta"),
        "reconciled": recon.get("reconciled"),
        "market_dates_aligned": alignment.get("aligned"),
        "alignment_status": alignment.get("status"),
        "note": (
            "Portfolio value reconciliation (cash + positions == total) is combined "
            "here with market-date alignment: both must hold for the operating data "
            "to be current. They are reported together, never conflated."
        ),
    }


def _blocked_execute_payload(*, required, requested_by, started, stages, before, after,
                             warnings) -> dict[str, Any]:
    row_counts = {t: {"before": before.get(t), "after": after.get(t)}
                  for t in _WRITE_TABLES}
    row_counts["positions"] = {"before": before.get("positions"),
                               "after": after.get("positions")}
    completed = datetime.now(tz=timezone.utc)
    return {
        "status": ST_BLOCKED,
        "mode": "EXECUTE",
        "execute": True,
        "requested_by": requested_by,
        "required_market_date": required.isoformat(),
        "started_at": _iso(started),
        "completed_at": _iso(completed),
        "duration_ms": int((completed - started).total_seconds() * 1000),
        "stages": stages,
        "price_universe": {},
        "alignment": {"required_market_date": required.isoformat(), "aligned": False,
                      "status": ST_BLOCKED, "mismatches": [], "blocking_mismatches": [
                          {"dataset": "run", "reason": "A run is already in progress."}]},
        "reconciliation": {},
        "row_counts": row_counts,
        "blockers": ["A daily operating run is already in progress for this date."],
        "final_outcome": "DAILY RUN BLOCKED — a run is already in progress for this date",
        "run_job_run_id": None,
        "confirmation_required": EXECUTE_CONFIRMATION,
        "warnings": warnings,
        "safety": _safety(),
        "provenance": _provenance(wrote=False),
        "loaded_at": _now_iso(),
    }


__all__ = [
    "run_daily_operating_session",
    "load_daily_operating_run_status",
    "compute_market_date_alignment",
    "latest_completed_market_date",
    "EXECUTE_CONFIRMATION",
    "SAFETY_BADGES",
    "STAGE_ORDER",
    "ST_ALIGNED", "ST_PREVIEW_WOULD_ALIGN", "ST_STALE", "ST_PARTIAL_COVERAGE",
    "ST_BLOCKED", "ST_NO_COMPLETED_MARKET_DATE", "ST_ALREADY_COMPLETE",
    "STG_CREATED", "STG_UPDATED", "STG_REUSED", "STG_SKIPPED", "STG_FAILED",
    "STG_BLOCKED", "STG_PLANNED",
]
