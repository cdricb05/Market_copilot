"""
engine/market_screener.py — Market candidate screener using price snapshots.

Generates a ranked list of candidate tickers based on momentum, volatility, and
relative strength versus a benchmark (SPY). Read-only; does not create orders or
change portfolio state. Does not call external APIs (no GCP, no yfinance).

Scoring uses only data available in the database:
- PriceSnapshot: price, market_date, ticker
- BenchmarkPrice: price for SPY/benchmark
- No volume data (not available in PriceSnapshot).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from paper_trader.db.models import BenchmarkPrice, PriceSnapshot


@dataclass
class CandidateScore:
    """Screened and ranked candidate ticker."""

    rank: int
    ticker: str
    score: str  # Decimal as string for JSON serialization
    latest_price: str
    latest_market_date: str
    price_count: int
    momentum_5d_pct: str | None
    momentum_20d_pct: str | None
    volatility_20d_pct: str | None
    relative_strength_vs_spy_20d: str | None
    reason_codes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON response."""
        return {
            "rank": self.rank,
            "ticker": self.ticker,
            "score": self.score,
            "latest_price": self.latest_price,
            "latest_market_date": self.latest_market_date,
            "price_count": self.price_count,
            "momentum_5d_pct": self.momentum_5d_pct,
            "momentum_20d_pct": self.momentum_20d_pct,
            "volatility_20d_pct": self.volatility_20d_pct,
            "relative_strength_vs_spy_20d": self.relative_strength_vs_spy_20d,
            "reason_codes": self.reason_codes,
        }


@dataclass
class SkippedTicker:
    """Ticker skipped due to insufficient or missing data."""

    ticker: str
    reason: str
    price_count: int

    def to_dict(self) -> dict[str, Any]:
        """Convert to dict for JSON response."""
        return {
            "ticker": self.ticker,
            "reason": self.reason,
            "price_count": self.price_count,
        }


def scan_market(
    session: Session,
    tickers: list[str] | None = None,
    universe: str = "SP500",
    benchmark_ticker: str = "SPY",
    lookback_days: int = 20,
    top_n: int = 25,
    min_price_points: int = 5,
) -> tuple[list[CandidateScore], list[SkippedTicker], date | None]:
    """
    Screen market candidates using price snapshot data.

    Args:
        session: Database session for price data access.
        tickers: Explicit list of tickers to scan. If provided, overrides universe.
        universe: Universe name ("SP500"). Ignored if tickers is provided.
        benchmark_ticker: Benchmark ticker for relative strength (default "SPY").
        lookback_days: Number of days of history to consider.
        top_n: Return top N candidates (capped at 100).
        min_price_points: Minimum number of price points required per ticker.

    Returns:
        (candidates, skipped_tickers, scan_date)
        candidates: List of CandidateScore objects, ranked by score descending.
        skipped_tickers: List of SkippedTicker objects (tickers with insufficient data).
        scan_date: Latest market_date from evaluated price data, or None if no data.

    Behavior:
        - Returns 200 OK with empty candidates and populated skipped_tickers if data is insufficient.
        - Does not call external APIs.
        - Does not modify portfolio or create orders.
    """
    # Validate and cap top_n
    top_n = min(max(1, top_n), 100)

    # Determine which tickers to evaluate
    if not tickers:
        # Use universe loader (currently supports "SP500" only)
        from paper_trader.engine.universe import get_sp500_universe

        if universe != "SP500":
            universe = "SP500"
        tickers = get_sp500_universe()

    if not tickers:
        return [], [], None

    # Normalize tickers to uppercase
    tickers_to_scan = [t.upper() for t in tickers]

    # Fetch latest market_date from price snapshots
    latest_date_result = session.execute(
        select(func.max(PriceSnapshot.market_date))
    ).scalar()
    latest_market_date = latest_date_result if latest_date_result is not None else date.today()

    # Calculate cutoff date for lookback window
    from datetime import timedelta

    cutoff_date = latest_market_date - timedelta(days=lookback_days)

    # Fetch benchmark (SPY) price history for relative strength calculation
    benchmark_prices = {}
    if benchmark_ticker:
        benchmark_data = session.execute(
            select(BenchmarkPrice.market_date, BenchmarkPrice.price)
            .where(BenchmarkPrice.ticker == benchmark_ticker.upper())
            .where(BenchmarkPrice.market_date >= cutoff_date)
            .where(BenchmarkPrice.market_date <= latest_market_date)
            .order_by(BenchmarkPrice.market_date.desc())
        ).all()

        benchmark_prices = {row[0]: Decimal(str(row[1])) for row in benchmark_data}

    # Score each ticker
    candidates = []
    skipped_tickers = []
    scan_date_result = None

    for ticker in tickers_to_scan:
        # Fetch price history for this ticker
        price_data = session.execute(
            select(PriceSnapshot.market_date, PriceSnapshot.price)
            .where(PriceSnapshot.ticker == ticker)
            .where(PriceSnapshot.market_date >= cutoff_date)
            .where(PriceSnapshot.market_date <= latest_market_date)
            .order_by(PriceSnapshot.market_date.desc())
        ).all()

        price_count = len(price_data)

        # Check minimum price points requirement
        if price_count < min_price_points:
            skipped_tickers.append(
                SkippedTicker(
                    ticker=ticker,
                    reason="INSUFFICIENT_PRICE_HISTORY",
                    price_count=price_count,
                )
            )
            continue

        # Extract prices and dates
        prices = [Decimal(str(row[1])) for row in price_data]
        dates = [row[0] for row in price_data]

        if not prices:
            skipped_tickers.append(
                SkippedTicker(
                    ticker=ticker,
                    reason="NO_PRICE_DATA",
                    price_count=0,
                )
            )
            continue

        # Update scan_date to latest date with data for this ticker
        if scan_date_result is None or dates[0] > scan_date_result:
            scan_date_result = dates[0]

        latest_price = prices[0]  # Most recent (DESC order)
        oldest_price = prices[-1]  # Oldest in window

        # Calculate momentum (5-day and 20-day)
        momentum_5d = None
        momentum_20d = None
        volatility_20d = None
        reason_codes = []

        # 5-day momentum: if we have at least 5 prices
        if len(prices) >= 5:
            price_5d_ago = prices[4]
            momentum_5d_pct = ((latest_price - price_5d_ago) / price_5d_ago * Decimal("100")).quantize(
                Decimal("0.01")
            )
            momentum_5d = momentum_5d_pct
            if momentum_5d > Decimal("0"):
                reason_codes.append("POSITIVE_5D_MOMENTUM")

        # 20-day momentum: if we have at least 20 prices
        if len(prices) >= 20:
            price_20d_ago = prices[19]
            momentum_20d_pct = ((latest_price - price_20d_ago) / price_20d_ago * Decimal("100")).quantize(
                Decimal("0.01")
            )
            momentum_20d = momentum_20d_pct
            if momentum_20d > Decimal("0"):
                reason_codes.append("POSITIVE_20D_MOMENTUM")

            # Volatility: standard deviation of 20-day returns
            returns = [
                ((prices[i] - prices[i + 1]) / prices[i + 1] * Decimal("100"))
                for i in range(min(19, len(prices) - 1))
            ]
            if returns:
                mean_return = sum(returns) / len(returns)
                variance = sum((r - mean_return) ** 2 for r in returns) / len(returns)
                volatility_20d = (variance.sqrt()).quantize(Decimal("0.01"))
            else:
                volatility_20d = Decimal("0")

        # Relative strength vs SPY (if benchmark data exists)
        relative_strength_vs_spy = None
        if benchmark_prices and len(prices) >= 20:
            # Compare 20-day momentum of ticker vs SPY
            spy_prices_for_dates = [benchmark_prices.get(d) for d in dates[:20]]
            spy_prices_valid = [p for p in spy_prices_for_dates if p is not None]

            if len(spy_prices_valid) >= 5:
                # Latest SPY price in our window
                spy_latest = spy_prices_valid[0]
                # SPY price 20 days ago (or as far back as we have)
                spy_oldest = spy_prices_valid[-1] if len(spy_prices_valid) >= 20 else spy_prices_valid[-1]

                if spy_latest and spy_oldest and spy_oldest > 0:
                    spy_momentum_pct = (spy_latest - spy_oldest) / spy_oldest * Decimal("100")
                    ticker_momentum_pct = momentum_20d if momentum_20d is not None else Decimal("0")
                    relative_strength_vs_spy = (ticker_momentum_pct - spy_momentum_pct).quantize(Decimal("0.01"))

                    if relative_strength_vs_spy > Decimal("0"):
                        reason_codes.append("OUTPERFORMING_SPY")
            else:
                reason_codes.append("BENCHMARK_MISSING")
        else:
            if len(prices) >= 20:
                reason_codes.append("BENCHMARK_MISSING")

        # Calculate final score (simple combination of positive indicators)
        score = Decimal("0")

        if momentum_5d is not None and momentum_5d > Decimal("0"):
            score += momentum_5d * Decimal("0.3")

        if momentum_20d is not None and momentum_20d > Decimal("0"):
            score += momentum_20d * Decimal("0.4")

        if relative_strength_vs_spy is not None and relative_strength_vs_spy > Decimal("0"):
            score += relative_strength_vs_spy * Decimal("0.3")

        # Volatility penalty: reduce score for high volatility
        if volatility_20d is not None and volatility_20d > Decimal("5"):
            score = score * (Decimal("1") - (volatility_20d / Decimal("100")))
            reason_codes.append("HIGH_VOLATILITY_PENALTY")

        score = score.quantize(Decimal("0.01"))

        # Build candidate
        candidate = CandidateScore(
            rank=0,  # Will be set after sorting
            ticker=ticker,
            score=str(score),
            latest_price=str(latest_price.quantize(Decimal("0.01"))),
            latest_market_date=str(dates[0]),
            price_count=price_count,
            momentum_5d_pct=str(momentum_5d.quantize(Decimal("0.01"))) if momentum_5d is not None else None,
            momentum_20d_pct=str(momentum_20d.quantize(Decimal("0.01")))
            if momentum_20d is not None
            else None,
            volatility_20d_pct=str(volatility_20d.quantize(Decimal("0.01")))
            if volatility_20d is not None
            else None,
            relative_strength_vs_spy_20d=str(relative_strength_vs_spy.quantize(Decimal("0.01")))
            if relative_strength_vs_spy is not None
            else None,
            reason_codes=reason_codes,
        )

        candidates.append(candidate)

    # Sort: score descending, then ticker ascending (deterministic tie-break)
    candidates.sort(
        key=lambda c: (Decimal(c.score) * Decimal("-1"), c.ticker),
    )

    # Apply top_n limit and assign ranks
    candidates = candidates[:top_n]
    for i, candidate in enumerate(candidates, start=1):
        candidate.rank = i

    return candidates, skipped_tickers, scan_date_result
