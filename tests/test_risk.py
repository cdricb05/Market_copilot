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
        """Raise min_order_notional above what the confidence tier allows.
        confidence=0.70 → 5% target = $500 < $1,000 threshold."""
        risk_portfolio.config = {
            **risk_portfolio.config,
            "min_order_notional": "1000.00",
        }
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.70"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.MIN_ORDER_TOO_SMALL


# ---------------------------------------------------------------------------
# BUY approval — correct sizing math
# ---------------------------------------------------------------------------

class TestBuyApproval:
    def test_buy_confidence_0_70_allocates_5pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.70, price=$100, no existing position.

        Target tier: 0.65 <= 0.70 < 0.75 → 5% target
        target_notional = 0.05 * 10_000 = $500
        incremental_notional_needed = $500 - $0 = $500
        requested_notional = min($500, concentration_room=$1,000) = $500
        approved_qty = floor(500 / 100) = 5
        """
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.70"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("500.00")
        assert rd.approved_notional  == Decimal("500.00")
        assert rd.approved_qty       == Decimal("5")

    def test_buy_confidence_0_80_allocates_10pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.80, price=$100, no existing position.

        Target tier: 0.75 <= 0.80 < 0.85 → 10% target
        target_notional = 0.10 * 10_000 = $1,000
        incremental_notional_needed = $1,000 - $0 = $1,000
        requested_notional = min($1,000, concentration_room=$1,000) = $1,000
        approved_qty = floor(1000 / 100) = 10
        """
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("1000.00")
        assert rd.approved_notional  == Decimal("1000.00")
        assert rd.approved_qty       == Decimal("10")

    def test_buy_confidence_0_90_allocates_15pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.90, price=$100, no existing position.

        Target tier: 0.90 >= 0.85 → 15% target
        target_notional = 0.15 * 10_000 = $1,500
        incremental_notional_needed = $1,500 - $0 = $1,500
        concentration_cap = 0.10 * 10_000 = $1,000
        concentration_room = $1,000
        requested_notional = min($1,500, concentration_room=$1,000) = $1,000
        approved_qty = floor(1000 / 100) = 10
        """
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.90"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("1000.00")
        assert rd.approved_notional  == Decimal("1000.00")
        assert rd.approved_qty       == Decimal("10")

    def test_buy_confidence_0_90_allocates_15pct_when_uncapped(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.90, price=$100, no existing position, uncapped concentration.

        Target tier: 0.90 >= 0.85 → 15% target
        target_notional = 0.15 * 10_000 = $1,500
        incremental_notional_needed = $1,500 - $0 = $1,500
        concentration_cap = 0.20 * 10_000 = $2,000
        concentration_room = $2,000
        requested_notional = min($1,500, concentration_room=$2,000) = $1,500
        approved_qty = floor(1500 / 100) = 15
        """
        risk_portfolio.config["max_concentration_pct"] = "0.20"
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.90"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("1500.00")
        assert rd.approved_notional  == Decimal("1500.00")
        assert rd.approved_qty       == Decimal("15")

    def test_buy_at_target_exposure_rejected(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        Position already at target exposure → REJECT.

        confidence=0.80 → 10% target = $1,000
        existing position: 10 shares @ fill_price=$50, current price=$100
        current market value = 10 * $100 = $1,000 (equals target)
        incremental_notional_needed = $1,000 - $1,000 = $0
        → CONCENTRATION_LIMIT (at or above target)
        """
        risk_portfolio.config["allow_averaging_down"] = True
        db_session.flush()
        open_position(
            db_session, ticker="AAPL", qty=Decimal("10"),
            fill_price=Decimal("50.00"), now=_NOW,
        )
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.CONCENTRATION_LIMIT

    def test_buy_below_target_incremental_only(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        Position below target → buy only incremental amount.

        confidence=0.80 → 10% target = $1,000
        existing position: 5 shares @ fill_price=$50, current price=$100
        current market value = 5 * $100 = $500 (below target)
        incremental_notional_needed = $1,000 - $500 = $500
        requested_notional = min($500, concentration_room=$1,000) = $500
        approved_qty = floor(500 / 100) = 5
        """
        risk_portfolio.config["allow_averaging_down"] = True
        db_session.flush()
        open_position(
            db_session, ticker="AAPL", qty=Decimal("5"),
            fill_price=Decimal("50.00"), now=_NOW,
        )
        db_session.flush()
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("100.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.reason_code is None
        assert rd.requested_notional == Decimal("500.00")
        assert rd.approved_notional  == Decimal("500.00")
        assert rd.approved_qty       == Decimal("5")

    def test_qty_floors_to_whole_shares(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.80 → 10% target = $1,000, price=$130.
        approved_qty = floor(1000 / 130) = 7
        final_notional = 7 * 130 = $910
        """
        rd = _buy(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("130.00"))
        assert rd.decision == DecisionType.BUY
        assert rd.approved_qty      == Decimal("7")
        assert rd.approved_notional == Decimal("910.00")

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

    def test_sell_confidence_0_70_sells_25pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.70, position 100 shares, price=$120.

        Exit tier: 0.65 <= 0.70 < 0.75 → sell 25%
        sell_qty = floor(100 * 0.25) = 25
        sell_notional = 25 * 120 = $3,000
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("100"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, confidence=Decimal("0.70"), price=Decimal("120.00"))
        assert rd.decision == DecisionType.SELL
        assert rd.reason_code is None
        assert rd.approved_qty       == Decimal("25")
        assert rd.approved_notional  == Decimal("3000.00")

    def test_sell_confidence_0_80_sells_50pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.80, position 100 shares, price=$120.

        Exit tier: 0.75 <= 0.80 < 0.85 → sell 50%
        sell_qty = floor(100 * 0.50) = 50
        sell_notional = 50 * 120 = $6,000
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("100"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, confidence=Decimal("0.80"), price=Decimal("120.00"))
        assert rd.decision == DecisionType.SELL
        assert rd.reason_code is None
        assert rd.approved_qty       == Decimal("50")
        assert rd.approved_notional  == Decimal("6000.00")

    def test_sell_confidence_0_90_sells_100pct(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.90, position 100 shares, price=$120.

        Exit tier: 0.90 >= 0.85 → sell 100%
        sell_qty = 100
        sell_notional = 100 * 120 = $12,000
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("100"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, confidence=Decimal("0.90"), price=Decimal("120.00"))
        assert rd.decision == DecisionType.SELL
        assert rd.reason_code is None
        assert rd.approved_qty       == Decimal("100")
        assert rd.approved_notional  == Decimal("12000.00")

    def test_sell_confidence_0_70_tiny_position_floors_to_zero(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.70, position 1 share, price=$120.

        Exit tier: 0.70 → sell 25%
        sell_qty = floor(1 * 0.25) = floor(0.25) = 0
        → CONCENTRATION_LIMIT (qty floors to 0)
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("1"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, confidence=Decimal("0.70"), price=Decimal("120.00"))
        assert rd.decision == DecisionType.REJECTED
        assert rd.reason_code == RejectionReason.CONCENTRATION_LIMIT

    def test_approved_sell_full_position(
        self, db_session: Session, risk_portfolio: Portfolio
    ) -> None:
        """
        confidence=0.90, 10 shares held, price=$120.

        Exit tier: 0.90 >= 0.85 → sell 100%
        approved_qty = 10
        approved_notional = 10 * 120 = $1,200
        """
        open_position(
            db_session, ticker="AAPL", qty=Decimal("10"),
            fill_price=Decimal("100.00"), now=_NOW,
        )
        db_session.flush()
        rd = _sell(db_session, risk_portfolio, confidence=Decimal("0.90"), price=Decimal("120.00"))
        assert rd.decision == DecisionType.SELL
        assert rd.reason_code is None
        assert rd.approved_qty       == Decimal("10")
        assert rd.approved_notional  == Decimal("1200.00")
        assert rd.requested_qty      == Decimal("10")
        assert rd.requested_notional == Decimal("1200.00")
