"""
scripts/seed.py — One-time portfolio bootstrap for paper_trader.

Creates the single portfolio row and seeds the INITIAL_CAPITAL cash ledger
entry. Safe to run multiple times: exits cleanly if already fully seeded,
raises RuntimeError if the database is in a partially-seeded state.

Usage:
    python scripts/seed.py
    python scripts/seed.py --initial-capital 25000.00
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import select

from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import CashLedger, Portfolio
from paper_trader.db.session import get_session
from paper_trader.engine.portfolio import append_cash_entry

_DOLLARS = Decimal("0.01")
_EASTERN = ZoneInfo("America/New_York")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed the paper_trader portfolio.")
    parser.add_argument(
        "--initial-capital",
        type=str,
        default=None,
        help=(
            "Starting capital in dollars (e.g. 10000.00). "
            "Overrides PAPER_TRADER_INITIAL_CAPITAL env var."
        ),
    )
    return parser.parse_args()


def seed(initial_capital: Decimal) -> None:
    """
    Create the portfolio row and INITIAL_CAPITAL ledger entry.

    Idempotent: exits cleanly if already fully seeded with the same capital.
    Raises RuntimeError on any inconsistency or capital mismatch.
    Does not commit; get_session() commits on clean exit.
    Success is printed only after the with-block exits cleanly.
    """
    now            = datetime.now(tz=timezone.utc)
    inception_date = now.astimezone(_EASTERN).date()
    amount         = initial_capital.quantize(_DOLLARS)

    seeded_id: int | None = None

    with get_session() as session:
        portfolio = session.execute(
            select(Portfolio)
        ).scalar_one_or_none()

        existing_entry = session.execute(
            select(CashLedger).where(
                CashLedger.entry_type == CashEntryType.INITIAL_CAPITAL
            )
        ).scalar_one_or_none()

        # Already fully seeded — validate consistency before accepting
        if portfolio is not None and existing_entry is not None:
            if existing_entry.portfolio_id != portfolio.id:
                raise RuntimeError(
                    f"Inconsistent seed state: INITIAL_CAPITAL ledger entry "
                    f"portfolio_id={existing_entry.portfolio_id} does not match "
                    f"portfolio id={portfolio.id}."
                )
            if existing_entry.amount != portfolio.initial_capital:
                raise RuntimeError(
                    f"Inconsistent seed state: INITIAL_CAPITAL ledger amount "
                    f"{existing_entry.amount} does not match portfolio "
                    f"initial_capital={portfolio.initial_capital}."
                )
            if amount != portfolio.initial_capital:
                raise RuntimeError(
                    f"Already seeded with initial_capital={portfolio.initial_capital}. "
                    f"Requested {amount} differs. Re-seeding is not supported."
                )
            seeded_id = portfolio.id
            return

        # Partially inconsistent state — do not attempt recovery
        if portfolio is not None and existing_entry is None:
            raise RuntimeError(
                "Inconsistent seed state: portfolio row exists but no "
                "INITIAL_CAPITAL ledger entry found. Manual inspection required."
            )
        if portfolio is None and existing_entry is not None:
            raise RuntimeError(
                "Inconsistent seed state: INITIAL_CAPITAL ledger entry exists "
                "but no portfolio row found. Manual inspection required."
            )

        # Fresh seed
        portfolio = Portfolio(
            inception_date=inception_date,
            initial_capital=amount,
            strategy_enabled=True,
            trading_enabled=True,
            allow_new_positions=True,
            config={},
            cached_cash=amount,
            cached_total_value=amount,
            cached_as_of_ts=now,
        )
        session.add(portfolio)
        session.flush()  # materialise portfolio.id before ledger entry

        append_cash_entry(
            session,
            portfolio_id=portfolio.id,
            entry_type=CashEntryType.INITIAL_CAPITAL,
            amount=amount,
            description=f"Initial capital: {amount}",
        )

        seeded_id = portfolio.id

    # Printed only after get_session() exits and commits successfully
    if seeded_id is not None:
        print(f"Seeded portfolio id={seeded_id} with initial_capital={amount}.")
    else:
        print(f"Already seeded. Portfolio initial_capital={amount}.")


def main() -> None:
    args     = _parse_args()
    settings = get_settings()

    raw = args.initial_capital if args.initial_capital is not None else settings.initial_capital
    try:
        initial_capital = Decimal(str(raw)).quantize(_DOLLARS)
    except Exception as exc:
        print(f"Error: invalid initial_capital value {raw!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    if initial_capital <= Decimal("0"):
        print(
            f"Error: initial_capital must be > 0, got {initial_capital}.",
            file=sys.stderr,
        )
        sys.exit(1)

    seed(initial_capital)


if __name__ == "__main__":
    main()
