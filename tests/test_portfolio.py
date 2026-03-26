"""
tests/test_portfolio.py — Unit tests for engine/portfolio.py.

Covers the WAC accounting math, cash ledger immutability, and position
lifecycle. Every test requires a live PostgreSQL test DB via
PAPER_TRADER_TEST_DATABASE_URL (auto-skipped when absent).

Tests are grouped by function. Each test uses db_session, which is rolled
back after each test — no cleanup required.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest
from sqlalchemy.orm import Session

from paper_trader.constants import CashEntryType
from paper_trader.engine.portfolio import (
    append_cash_entry,
    compute_cash,
    get_position,
    open_position,
    reduce_position,
    refresh_portfolio_cache,
    update_position_wac,
)

_NOW     = datetime(2025, 1, 15, 14, 30, 0, tzinfo=timezone.utc)
_CAPITAL = Decimal("10000.00")


# ---------------------------------------------------------------------------
# compute_cash
# ---------------------------------------------------------------------------

class TestComputeCash:
    def test_empty_ledger_returns_zero(self, db_session: Session) -> None:
        """No ledger rows → 0.00."""
        assert compute_cash(db_session) == Decimal("0.00")

    def test_initial_capital_only(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        """Seeded portfolio contributes exactly $10,000."""
        assert compute_cash(db_session) == _CAPITAL

    def test_debit_reduces_running_total(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        append_cash_entry(
            db_session,
            portfolio_id=seeded_portfolio.id,
            entry_type=CashEntryType.BUY_DEBIT,
            amount=Decimal("-500.00"),
        )
        db_session.flush()
        assert compute_cash(db_session) == Decimal("9500.00")


# ---------------------------------------------------------------------------
# append_cash_entry
# ---------------------------------------------------------------------------

class TestAppendCashEntry:
    def test_zero_amount_raises(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        with pytest.raises(ValueError, match="non-zero"):
            append_cash_entry(
                db_session,
                portfolio_id=seeded_portfolio.id,
                entry_type=CashEntryType.BUY_DEBIT,
                amount=Decimal("0"),
            )

    def test_positive_entry_stored(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        entry = append_cash_entry(
            db_session,
            portfolio_id=seeded_portfolio.id,
            entry_type=CashEntryType.SELL_CREDIT,
            amount=Decimal("750.50"),
        )
        db_session.flush()
        assert entry.amount == Decimal("750.50")
        assert entry.entry_type == CashEntryType.SELL_CREDIT

    def test_negative_entry_stored(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        entry = append_cash_entry(
            db_session,
            portfolio_id=seeded_portfolio.id,
            entry_type=CashEntryType.COMMISSION_DEBIT,
            amount=Decimal("-1.50"),
        )
        db_session.flush()
        assert entry.amount == Decimal("-1.50")


# ---------------------------------------------------------------------------
# open_position
# ---------------------------------------------------------------------------

class TestOpenPosition:
    def test_creates_with_correct_wac(self, db_session: Session) -> None:
        """First fill: avg_cost == fill_price, cost_basis == fill_price * qty."""
        pos = open_position(
            db_session,
            ticker="AAPL",
            qty=Decimal("10"),
            fill_price=Decimal("150.00"),
            now=_NOW,
        )
        db_session.flush()
        assert pos.ticker == "AAPL"
        assert pos.qty == Decimal("10.00000000")
        assert pos.avg_cost == Decimal("150.000000")
        assert pos.cost_basis == Decimal("1500.00")

    def test_zero_qty_raises(self, db_session: Session) -> None:
        with pytest.raises(ValueError, match="qty must be > 0"):
            open_position(
                db_session,
                ticker="AAPL",
                qty=Decimal("0"),
                fill_price=Decimal("150.00"),
                now=_NOW,
            )

    def test_zero_price_raises(self, db_session: Session) -> None:
        with pytest.raises(ValueError, match="fill_price must be > 0"):
            open_position(
                db_session,
                ticker="AAPL",
                qty=Decimal("10"),
                fill_price=Decimal("0"),
                now=_NOW,
            )

    def test_duplicate_ticker_raises(self, db_session: Session) -> None:
        open_position(
            db_session,
            ticker="AAPL",
            qty=Decimal("5"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="already exists"):
            open_position(
                db_session,
                ticker="AAPL",
                qty=Decimal("5"),
                fill_price=Decimal("100.00"),
                now=_NOW,
            )


# ---------------------------------------------------------------------------
# update_position_wac
# ---------------------------------------------------------------------------

class TestUpdatePositionWac:
    def test_wac_formula(self, db_session: Session) -> None:
        """10 @ $100 then 10 more @ $120 → avg_cost = $110, cost_basis = $2200."""
        pos = open_position(
            db_session,
            ticker="MSFT",
            qty=Decimal("10"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        db_session.flush()
        update_position_wac(
            pos,
            fill_qty=Decimal("10"),
            fill_price=Decimal("120.00"),
            now=_NOW,
        )
        assert pos.qty == Decimal("20.00000000")
        assert pos.avg_cost == Decimal("110.000000")
        assert pos.cost_basis == Decimal("2200.00")

    def test_unequal_lots_wac(self, db_session: Session) -> None:
        """20 @ $50 then 5 more @ $100 → avg_cost = $60, cost_basis = $1500."""
        pos = open_position(
            db_session,
            ticker="TSLA",
            qty=Decimal("20"),
            fill_price=Decimal("50.00"),
            now=_NOW,
        )
        db_session.flush()
        update_position_wac(
            pos,
            fill_qty=Decimal("5"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        assert pos.qty == Decimal("25.00000000")
        assert pos.avg_cost == Decimal("60.000000")
        assert pos.cost_basis == Decimal("1500.00")

    def test_zero_fill_qty_raises(self, db_session: Session) -> None:
        pos = open_position(
            db_session,
            ticker="GOOG",
            qty=Decimal("5"),
            fill_price=Decimal("200.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="fill_qty must be > 0"):
            update_position_wac(
                pos, fill_qty=Decimal("0"), fill_price=Decimal("200.00"), now=_NOW
            )

    def test_zero_fill_price_raises(self, db_session: Session) -> None:
        pos = open_position(
            db_session,
            ticker="NVDA",
            qty=Decimal("5"),
            fill_price=Decimal("200.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="fill_price must be > 0"):
            update_position_wac(
                pos, fill_qty=Decimal("1"), fill_price=Decimal("0"), now=_NOW
            )


# ---------------------------------------------------------------------------
# reduce_position
# ---------------------------------------------------------------------------

class TestReducePosition:
    def test_partial_close_preserves_avg_cost(self, db_session: Session) -> None:
        """WAC must not change on SELL. Only qty and cost_basis are updated."""
        pos = open_position(
            db_session,
            ticker="NVDA",
            qty=Decimal("10"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        db_session.flush()
        closed = reduce_position(db_session, pos, fill_qty=Decimal("4"), now=_NOW)
        assert closed is False
        assert pos.qty == Decimal("6.00000000")
        assert pos.avg_cost == Decimal("100.000000")   # unchanged
        assert pos.cost_basis == Decimal("600.00")

    def test_full_close_deletes_row(self, db_session: Session) -> None:
        pos = open_position(
            db_session,
            ticker="AMD",
            qty=Decimal("5"),
            fill_price=Decimal("80.00"),
            now=_NOW,
        )
        db_session.flush()
        closed = reduce_position(db_session, pos, fill_qty=Decimal("5"), now=_NOW)
        assert closed is True
        db_session.flush()
        assert get_position(db_session, "AMD") is None

    def test_oversell_raises(self, db_session: Session) -> None:
        pos = open_position(
            db_session,
            ticker="META",
            qty=Decimal("3"),
            fill_price=Decimal("300.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="only 3"):
            reduce_position(db_session, pos, fill_qty=Decimal("5"), now=_NOW)

    def test_zero_fill_qty_raises(self, db_session: Session) -> None:
        pos = open_position(
            db_session,
            ticker="AMZN",
            qty=Decimal("10"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="fill_qty must be > 0"):
            reduce_position(db_session, pos, fill_qty=Decimal("0"), now=_NOW)


# ---------------------------------------------------------------------------
# refresh_portfolio_cache
# ---------------------------------------------------------------------------

class TestRefreshPortfolioCache:
    def test_no_positions_cash_only(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        refresh_portfolio_cache(
            db_session, seeded_portfolio, price_map={}, now=_NOW
        )
        assert seeded_portfolio.cached_cash == _CAPITAL
        assert seeded_portfolio.cached_total_value == _CAPITAL
        assert seeded_portfolio.cached_as_of_ts == _NOW

    def test_mark_to_market_with_open_position(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        """total_value = remaining cash + positions marked at current price."""
        append_cash_entry(
            db_session,
            portfolio_id=seeded_portfolio.id,
            entry_type=CashEntryType.BUY_DEBIT,
            amount=Decimal("-1000.00"),
        )
        open_position(
            db_session,
            ticker="SPY",
            qty=Decimal("5"),
            fill_price=Decimal("200.00"),
            now=_NOW,
        )
        db_session.flush()

        refresh_portfolio_cache(
            db_session,
            seeded_portfolio,
            price_map={"SPY": Decimal("220.00")},
            now=_NOW,
        )
        # cash = 10000 - 1000 = 9000
        # positions_value = 5 * 220 = 1100
        assert seeded_portfolio.cached_cash == Decimal("9000.00")
        assert seeded_portfolio.cached_total_value == Decimal("10100.00")

    def test_missing_ticker_in_price_map_raises(
        self, db_session: Session, seeded_portfolio
    ) -> None:
        open_position(
            db_session,
            ticker="COIN",
            qty=Decimal("3"),
            fill_price=Decimal("100.00"),
            now=_NOW,
        )
        db_session.flush()
        with pytest.raises(ValueError, match="missing prices"):
            refresh_portfolio_cache(
                db_session, seeded_portfolio, price_map={}, now=_NOW
            )
