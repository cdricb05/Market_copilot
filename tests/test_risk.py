"""
tests/test_risk.py — Unit tests for engine/risk.py evaluate_signal().

Covers all early-gate rejections, BUY-path rejections, BUY approval math,
and SELL-path rejections and approval.

DUPLICATE_SIGNAL and DAILY_EXPOSURE_LIMIT require a full order-chain fixture
and are left for a future integration test.

Requires PAPER_TRADER_TEST_DATABASE_URL (auto-skipped when absent).
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from paper_trader.constants import (
    CashEntryType,
    DecisionType,
    RejectionReason,
    SignalDirection,
)
from paper_trader.db.models import Portfolio
from paper_trader.engine.portfolio import append_cash_entry, open_position
from paper_trader.engine.risk import evaluate_signal

_NOW  = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_DATE = _NOW.date()

# ---------------------------------------------------------------------------
# Fixture: portfolio with fixed, deterministic risk parameters
#
# Capital          = $10,000 (all cash, seeded_portfolio)
# max_concentration = 10%  → cap  = $1,000 / ticker
# min_cash_pct      =  5%  → reserve = $500 ; available_cash = $9,500
# daily_exposure    = 20%  → daily limit = $2,000
# confidence_threshold = 0.60
# max_positions     = 5
# min_order_notional = $10.00
# cooldown_hours    = 0    (disabled — no Trade rows needed)
# allow_averaging_down = False
# ---------------------------------------------------------------------------

@pytest.fixture
def risk_portfolio(db_session: Session, seeded_portfolio: Portfolio) -> Portfolio:
    seeded_portfolio.config = {
        "max_positions":              5,
        "max_concentration_pct":      "0.10",
        "min_cash_pct":               "0.05",
        "max_daily_new_exposure_pct": "0.20",
        "confidence_threshold":       "0.60",
        "min_order_notional":         "10.00",
        "ticker_cooldown_hours":      0,
        "allow_averaging_down":       False,
    }
    db_session.flush()
    return seeded_portfolio


# ---------------------------------------------------------------------------
# Helpers to reduce boilerplate
# ---------------------------------------------------------------------------

def _buy(
    session: Session,
    portfolio: Portfolio,
    *,
    ticker: str = "AAPL",
    confidence: Decimal = Decimal("0.80"),
    price: Decimal | None = Decimal("100.00"),
):
    return evaluate_signal(
        session,
        portfolio=portfolio,
        direction=SignalDirection.BUY,
        ticker=ticker,
        confidence=confidence,
        snapshot_price=price,
        market_date=_DATE,
        now=_NOW,
    )


def _sell(
    session: Session,
    portfolio: Portfolio,
    *,
    ticker: str = "AAPL",
    confidence: Decimal = Decimal("0.80"),
    price: Decimal | None = Decimal("100.00"),
):
    return evaluate_signal(
        session,
        portfolio=portfolio,
        direction=SignalDirection.SELL,
        ticker=ticker,
        confidence=confidence,
        snapshot_price=price,
        market_date=_DATE,
        now=_NOW,
    )


# ---------------------------------------------------------------------------
# Early-gate rejections  (checked before BUY / SELL branching)
# ---------------------------------------------------------------------------

class TestEarlyGates:
    def test_strategy_disabled(self, db_session: Session, risk_portfolio: Portfolio) -> None:
        risk_portfolio.strategy_enabled = False
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.STRATEGY_DISABLED

    def test_trading_disabled(self, db_session: Session, risk_portfolio: Portfolio) -> None:
        risk_portfolio.trading_enabled = False
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.TRADING_DISABLED

    def test_hold_signal(self, db_session: Session, risk_portfolio: Portfolio) -> None:
        rd = evaluate_signal(
            db_session,
            portfolio=risk_portfolio,
            direction=SignalDirection.HOLD,
            ticker="AAPL",
            confidence=Decimal("0.90"),
            snapshot_price=Decimal("100.00"),
            market_date=_DATE,
            now=_NOW,
        )
        assert rd.decision == DecisionType.HOLD
        assert rd.reason_code == RejectionReason.HOLD_SIGNAL

    def test_low_confidence(self, db_session: Session, risk_portfolio: Portfolio) -> None:
        """confidence 0.50 is below the 0.60 threshold."""
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.50"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.CONFIDENCE_BELOW_THRESHOLD

    def test_risk_snapshot_always_present_on_rejection(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """risk_snapshot must be a non-empty dict on every rejection path."""
        risk_portfolio.strategy_enabled = False
        rd = _buy(db_session, risk_portfolio)
        assert isinstance(rd.risk_snapshot, dict)
        for key in ("cash", "total_value", "open_position_count"):
            assert key in rd.risk_snapshot, f"missing snapshot key: {key!r}"


# ---------------------------------------------------------------------------
# BUY-path rejections
# ---------------------------------------------------------------------------

class TestBuyRejections:
    def test_new_positions_disabled(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        risk_portfolio.allow_new_positions = False
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.NEW_POSITIONS_DISABLED

    def test_max_positions_reached(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """Fill all 5 slots; the next BUY on a new ticker is rejected."""
        for tkr in ["AA", "BB", "CC", "DD", "EE"]:
            open_position(
                db_session, ticker=tkr, qty=Decimal("1"),
                fill_price=Decimal("10.00"), now=_NOW,
            )
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, ticker="AAPL")
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.MAX_POSITIONS_REACHED

    def test_averaging_down_blocked(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """BUY on a ticker already held is unconditionally rejected when
        allow_averaging_down is False — no price comparison."""
        open_position(
            db_session, ticker="AAPL", qty=Decimal("5"),
            fill_price=Decimal("120.00"), now=_NOW,
        )
        db_session.flush()
        # Signal price is lower than cost — still blocked, no comparison made
        rd = _buy(db_session, risk_portfolio, ticker="AAPL", price=Decimal("90.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.AVERAGING_DOWN_BLOCKED

    def test_no_price_snapshot_none(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        rd = _buy(db_session, risk_portfolio, price=None)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.NO_PRICE_SNAPSHOT

    def test_no_price_snapshot_zero(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        rd = _buy(db_session, risk_portfolio, price=Decimal("0"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.NO_PRICE_SNAPSHOT

    def test_cash_reserve_breach(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """Leave only $300 cash — below the $500 reserve (5% of $10,000)."""
        append_cash_entry(
            db_session,
            portfolio_id=risk_portfolio.id,
            entry_type=CashEntryType.BUY_DEBIT,
            amount=Decimal("-9700.00"),
        )
        db_session.flush()
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.CASH_RESERVE_BREACH

    def test_concentration_limit_price_too_high(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """price=$2,000 → floor(approved_notional / price) = floor(800/2000) = 0."""
        rd = _buy(db_session, risk_portfolio, price=Decimal("2000.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.CONCENTRATION_LIMIT

    def test_min_order_too_small(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """Raise min_order_notional above what the concentration cap allows.
        approved_qty=8, final_notional=8*$100=$800 < $1,000 threshold."""
        risk_portfolio.config = {
            **risk_portfolio.config,
            "min_order_notional": "1000.00",
        }
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, price=Decimal("100.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.MIN_ORDER_TOO_SMALL


# ---------------------------------------------------------------------------
# BUY approval — correct sizing math
# ---------------------------------------------------------------------------

class TestBuyApproval:
    def test_correct_qty_and_notional(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.80, price=$100, no existing position.

        requested_notional = min(0.80 * 0.10 * 10_000, 1_000) = $800
        approved_qty       = floor(800 / 100)                 = 8
        approved_notional  = 8 * 100                          = $800
        """
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("800.00")
        assert rd.approved_notional  == Decimal("800.00")
        assert rd.approved_qty       == Decimal("8")

    def test_qty_floors_to_whole_shares(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        price=$130 → approved_qty = floor(800 / 130) = 6
        final_notional = 6 * 130 = $780
        """
        rd = _buy(db_session, risk_portfolio, price=Decimal("130.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.approved_qty      == Decimal("6")
        assert rd.approved_notional == Decimal("780.00")

    def test_buy_snapshot_contains_extended_keys(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """Approved BUY must include the sizing-context keys."""
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.BUY
        for key in (
            "cash", "total_value", "available_cash", "daily_remaining",
            "snapshot_price", "concentration_cap", "concentration_room",
        ):
            assert key in rd.risk_snapshot, f"missing BUY snapshot key: {key!r}"

    def test_no_sizing_adjustments_when_within_limits(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """$800 request fits inside both daily limit ($2,000) and available
        cash ($9,500) so sizing_adjustments must be empty."""
        rd = _buy(db_session, risk_portfolio)
        assert rd.decision == DecisionType.BUY
        assert rd.sizing_adjustments == []


# ---------------------------------------------------------------------------
# SELL-path rejections and approval
# ---------------------------------------------------------------------------

class TestSellPath:
    def test_no_position_to_sell(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        rd = _sell(db_session, risk_portfolio, ticker="AAPL")
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.NO_POSITION_TO_SELL

    def test_no_price_snapshot(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        open_position(
            db_session, ticker="AAPL", qty=Decimal("10"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, ticker="AAPL", price=None)
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.NO_PRICE_SNAPSHOT

    def test_approved_sell_full_position(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        10 shares held, current price $120.
        approved_qty      = 10 (full position)
        approved_notional = 10 * 120 = $1,200
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("10"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, ticker="AAPL", price=Decimal("120.00"))
        assert rd.decision == DecisionType.SELL
        assert rd.reason_code is None
        assert rd.approved_qty       == Decimal("10.00000000")
        assert rd.approved_notional  == Decimal("1200.00")
        assert rd.requested_qty      == Decimal("10.00000000")
        assert rd.requested_notional == Decimal("1200.00")
