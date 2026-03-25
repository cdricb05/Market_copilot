"""
workflows/decision.py — Signal-ingestion and decision-making workflow.

run_decision_workflow() is the single entry point. It:
    1. Enforces idempotency via JobRun.idempotency_key.
    2. Acquires the portfolio advisory lock for the duration of the run.
    3. Processes each incoming signal through evaluate_signal() and records
       a TradeDecision row with full risk audit state.
    4. Creates a PENDING Order for every BUY or SELL decision.
    5. Marks the JobRun COMPLETED with a result summary, or FAILED on error.

Idempotency contract:
    COMPLETED → return cached result_summary immediately (no work).
    RUNNING   → raise RuntimeError (concurrent execution guard).
    FAILED    → raise RuntimeError; caller must supply a new idempotency_key
                or manually clean up the failed run before retrying.

Per-signal isolation:
    Each signal is processed inside a PostgreSQL savepoint. A failure on one
    signal (including a unique-constraint violation on Signal INSERT) rolls
    back that savepoint only; the rest of the batch continues. Failed signals
    are recorded with status=ERROR where possible.
"""
from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from paper_trader.constants import (
    PORTFOLIO_ADVISORY_LOCK_KEY,
    DecisionType,
    JobRunStatus,
    OrderStatus,
    SignalStatus,
)
from paper_trader.db.models import (
    JobRun,
    Order,
    PriceSnapshot,
    Signal,
    TradeDecision,
)
from paper_trader.db.session import get_dedicated_session
from paper_trader.engine.portfolio import get_portfolio
from paper_trader.engine.risk import evaluate_signal

_PRICE = Decimal("0.000001")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _latest_price(session: Session, ticker: str) -> Decimal | None:
    """Return the most recent snapshot price for ticker, or None."""
    result = session.execute(
        select(PriceSnapshot.price)
        .where(PriceSnapshot.ticker == ticker)
        .order_by(PriceSnapshot.snapshot_ts.desc())
        .limit(1)
    ).scalar()
    return Decimal(str(result)).quantize(_PRICE) if result is not None else None


def _decimal_or_none(value: Decimal) -> Decimal | None:
    """Return None when value is zero (sizing fields are null for HOLD/early REJECTED)."""
    return None if value == Decimal("0") else value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_decision_workflow(
    *,
    idempotency_key: str,
    workflow_type: str,
    market_date: date,
    signals: list[dict],
    now: datetime,
) -> dict:
    """
    Ingest signals and produce trade decisions for a single workflow run.

    signals is a list of dicts with keys:
        ticker      str
        direction   str         — BUY | SELL | HOLD
        confidence  str|Decimal
        signal_ts   datetime
        source_run  str
        raw_payload dict|None   (optional)

    Returns the result_summary dict on success. Raises RuntimeError on
    idempotency conflicts or lock contention. All other exceptions mark the
    JobRun FAILED before re-raising.
    """
    with get_dedicated_session() as session:

        # Fix 3: initialise before the try block so the outer except can
        # safely reference it regardless of where an exception is raised.
        job_run: JobRun | None = None

        try:
            # ------------------------------------------------------------------
            # Idempotency check
            # ------------------------------------------------------------------
            existing = session.execute(
                select(JobRun).where(JobRun.idempotency_key == idempotency_key)
            ).scalar_one_or_none()

            if existing is not None:
                if existing.status == JobRunStatus.COMPLETED:
                    return existing.result_summary or {}

                if existing.status == JobRunStatus.RUNNING:
                    raise RuntimeError(
                        f"Job run {idempotency_key!r} is currently RUNNING. "
                        "Another worker may be processing this workflow. "
                        "If the run is stale, update its status manually."
                    )

                # Fix 1: FAILED runs are not retried automatically. The caller
                # must supply a new idempotency_key or manually remove the
                # failed row before retrying.
                if existing.status == JobRunStatus.FAILED:
                    raise RuntimeError(
                        f"Job run {idempotency_key!r} previously FAILED "
                        f"(id={existing.id}). Inspect job_runs.error_detail, "
                        "fix the underlying issue, then retry with a new "
                        "idempotency_key or manually delete the failed row."
                    )

            # ------------------------------------------------------------------
            # Create the JobRun and commit before acquiring the lock so that
            # a concurrent caller sees RUNNING and backs off immediately.
            # ------------------------------------------------------------------
            job_run = JobRun(
                idempotency_key=idempotency_key,
                workflow_type=workflow_type,
                market_date=market_date,
                status=JobRunStatus.RUNNING,
                started_at=now,
            )
            session.add(job_run)
            session.commit()

            # ------------------------------------------------------------------
            # Acquire portfolio advisory lock (connection-scoped).
            # ------------------------------------------------------------------
            acquired = session.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
            ).scalar()
            if not acquired:
                raise RuntimeError(
                    "Could not acquire portfolio advisory lock. "
                    "Another workflow is currently running."
                )

            try:
                portfolio = get_portfolio(session)

                counts = {
                    "signals_ingested": 0,
                    "decisions_made":   0,
                    "orders_created":   0,
                    "errors":           0,
                }

                # --------------------------------------------------------------
                # Per-signal processing.
                #
                # Fix 2: the Signal INSERT and every subsequent step for that
                # signal are inside the savepoint. A unique-constraint violation
                # on signals(source_run, ticker, direction) — or any other
                # per-signal failure — rolls back that savepoint only. The batch
                # continues with the next signal.
                # --------------------------------------------------------------
                for signal_data in signals:
                    sp = session.begin_nested()
                    try:
                        ticker    = signal_data["ticker"]
                        direction = signal_data["direction"]

                        signal = Signal(
                            job_run_id=job_run.id,
                            ticker=ticker,
                            direction=direction,
                            confidence=Decimal(str(signal_data["confidence"])),
                            signal_ts=signal_data["signal_ts"],
                            market_date=market_date,
                            source_run=signal_data["source_run"],
                            status=SignalStatus.RECEIVED,
                            raw_payload=signal_data.get("raw_payload"),
                        )
                        session.add(signal)
                        session.flush()  # materialise signal.id; raises on constraint violation

                        signal.status = SignalStatus.PROCESSING

                        snapshot_price = _latest_price(session, ticker)

                        rd = evaluate_signal(
                            session,
                            portfolio=portfolio,
                            direction=direction,
                            ticker=ticker,
                            confidence=signal.confidence,
                            snapshot_price=snapshot_price,
                            market_date=market_date,
                            now=now,
                        )

                        trade_decision = TradeDecision(
                            signal_id=signal.id,
                            job_run_id=job_run.id,
                            ticker=ticker,
                            signal_direction=direction,
                            decision=rd.decision,
                            reason_code=rd.reason_code,
                            requested_notional=_decimal_or_none(rd.requested_notional),
                            approved_notional=_decimal_or_none(rd.approved_notional),
                            requested_qty=_decimal_or_none(rd.requested_qty),
                            approved_qty=_decimal_or_none(rd.approved_qty),
                            risk_snapshot=rd.risk_snapshot or None,
                            sizing_adjustments=rd.sizing_adjustments or None,
                            market_date=market_date,
                        )
                        session.add(trade_decision)
                        session.flush()  # materialise trade_decision.id

                        counts["decisions_made"] += 1

                        if rd.decision in (DecisionType.BUY, DecisionType.SELL):
                            session.add(Order(
                                trade_decision_id=trade_decision.id,
                                job_run_id=job_run.id,
                                ticker=ticker,
                                side=rd.decision,
                                order_type="MARKET",
                                status=OrderStatus.PENDING,
                                market_date=market_date,
                                requested_qty=rd.approved_qty,
                                requested_at=now,
                            ))
                            counts["orders_created"] += 1

                        signal.status = SignalStatus.DECISION_MADE
                        counts["signals_ingested"] += 1
                        sp.commit()

                    except Exception as exc:
                        sp.rollback()
                        counts["errors"] += 1
                        # Attempt to record an ERROR signal for auditability.
                        # If this insert also fails (e.g., the same unique
                        # constraint), silently discard — the error count in
                        # result_summary is the audit record.
                        err_sp = session.begin_nested()
                        try:
                            session.add(Signal(
                                job_run_id=job_run.id,
                                ticker=signal_data.get("ticker", "UNKNOWN"),
                                direction=signal_data.get("direction", "UNKNOWN"),
                                confidence=Decimal(
                                    str(signal_data.get("confidence", "0"))
                                ),
                                signal_ts=signal_data.get("signal_ts", now),
                                market_date=market_date,
                                source_run=signal_data.get("source_run", ""),
                                status=SignalStatus.ERROR,
                                error_detail=str(exc)[:2000],
                                raw_payload=signal_data.get("raw_payload"),
                            ))
                            err_sp.commit()
                        except Exception:
                            err_sp.rollback()

                # --------------------------------------------------------------
                # Finalise JobRun
                # --------------------------------------------------------------
                result_summary = {
                    "signals_ingested": counts["signals_ingested"],
                    "decisions_made":   counts["decisions_made"],
                    "orders_created":   counts["orders_created"],
                    "errors":           counts["errors"],
                }
                job_run.status         = JobRunStatus.COMPLETED
                job_run.completed_at   = now
                job_run.result_summary = result_summary
                session.commit()

                return result_summary

            finally:
                # Release the advisory lock regardless of success or failure.
                try:
                    session.execute(
                        text("SELECT pg_advisory_unlock(:key)"),
                        {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                    )
                    session.commit()
                except Exception:
                    session.rollback()

        except Exception as exc:
            # Fix 3: job_run is None when the exception originates before the
            # JobRun row was created (idempotency conflict, lock contention, etc).
            # Only attempt a FAILED transition when we own the row.
            if job_run is not None:
                try:
                    job_run.status       = JobRunStatus.FAILED
                    job_run.error_detail = str(exc)[:2000]
                    job_run.completed_at = now
                    session.commit()
                except Exception:
                    session.rollback()
            raise
