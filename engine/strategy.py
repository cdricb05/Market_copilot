"""
engine/strategy.py — Strategy signal generation from price snapshots.

This module generates deterministic trading signals by analyzing historical
price data using configurable moving average (SMA) rules.

Signal generation process:
    1. Fetch all unique tickers from price_snapshots.
    2. For each ticker, fetch the most recent long_window prices (DESC).
    3. Compute SMA(short_window) and SMA(long_window).
    4. Compare latest price to SMAs to classify: BUY | SELL | HOLD.
    5. Scale confidence based on trend strength.
    6. Return a list of signal dicts ready for ingest via /v1/signals.

Each signal includes raw_payload metadata for explainability:
    - latest_price, short_window, long_window
    - short_sma, long_sma
    - trend_reason: human-readable explanation
    - skipped_reason: if ticker was skipped
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

from sqlalchemy import distinct, select
from sqlalchemy.orm import Session

from paper_trader.constants import SignalDirection
from paper_trader.db.models import PriceSnapshot

_EASTERN = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Signal generation
# ---------------------------------------------------------------------------

def generate_signals(
    session: Session,
    market_date,
    now: datetime,
    short_window: int = 3,
    long_window: int = 5,
    tickers: list[str] | None = None,
) -> tuple[list[dict], dict[str, str]]:
    """
    Generate strategy signals from price_snapshots.

    Args:
        session: Active database session.
        market_date: US-Eastern trading date (date object).
        now: Current timestamp (datetime with timezone).
        short_window: Number of periods for short SMA (default 3).
        long_window: Number of periods for long SMA (default 5).
        tickers: Optional list of tickers to process. If None, process all.

    Returns:
        (signals, skipped_reasons)
        signals: list of dicts {ticker, direction, confidence, signal_ts, source_run, raw_payload}
        skipped_reasons: dict {ticker: reason} for tickers that were skipped

    Raises:
        ValueError: If short_window >= long_window or windows are <= 0.
    """
    if short_window >= long_window:
        raise ValueError(
            f"short_window ({short_window}) must be < long_window ({long_window})"
        )
    if short_window <= 0 or long_window <= 0:
        raise ValueError("short_window and long_window must be > 0")

    signals = []
    skipped_reasons = {}

    # Fetch all unique tickers from price_snapshots, or filter to requested list
    all_tickers_query = session.execute(
        select(distinct(PriceSnapshot.ticker))
    ).scalars().all()

    if not all_tickers_query:
        return signals, skipped_reasons

    tickers_to_process = tickers if tickers else all_tickers_query

    for ticker in tickers_to_process:
        # Fetch the most recent long_window prices for this ticker (up to market_date)
        prices_result = session.execute(
            select(PriceSnapshot.price)
            .where(PriceSnapshot.ticker == ticker)
            .where(PriceSnapshot.market_date <= market_date)
            .order_by(PriceSnapshot.snapshot_ts.desc())
            .limit(long_window)
        ).scalars().all()

        prices = [Decimal(str(p)) for p in prices_result]

        # Check if we have enough history
        if len(prices) < long_window:
            skipped_reasons[ticker] = (
                f"Insufficient price history: {len(prices)} < {long_window}"
            )
            continue

        # Reverse to chronological order (oldest first) for SMA calculation
        prices = list(reversed(prices))

        latest_price = prices[-1]
        short_sma = _compute_sma(prices, short_window)
        long_sma = _compute_sma(prices, long_window)

        # Classify signal
        direction, confidence, trend_reason = _classify_signal(
            latest_price, short_sma, long_sma
        )

        # Build signal dict
        signal = {
            "ticker": ticker,
            "direction": direction,
            "confidence": confidence,
            "signal_ts": now,
            "source_run": "strategy_v1",
            "raw_payload": {
                "latest_price": str(latest_price),
                "short_window": short_window,
                "long_window": long_window,
                "short_sma": str(short_sma),
                "long_sma": str(long_sma),
                "trend_reason": trend_reason,
            },
        }
        signals.append(signal)

    return signals, skipped_reasons


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_sma(prices: list[Decimal], window: int) -> Decimal:
    """
    Compute simple moving average over the last 'window' prices.

    Assumes prices are in chronological order (oldest first).
    Returns the average of the most recent 'window' prices.
    """
    if len(prices) < window:
        raise ValueError(f"Not enough prices ({len(prices)}) for window {window}")

    recent = prices[-window:]
    return sum(recent) / Decimal(window)


def _classify_signal(
    latest_price: Decimal,
    short_sma: Decimal,
    long_sma: Decimal,
) -> tuple[str, Decimal, str]:
    """
    Classify a signal based on price and moving average position.

    Returns:
        (direction, confidence, trend_reason)
        direction: "BUY" | "SELL" | "HOLD"
        confidence: Decimal between 0.0 and 1.0
        trend_reason: Human-readable explanation
    """
    # Compute distance ratios for confidence scaling
    price_to_short_ratio = (latest_price - short_sma) / short_sma if short_sma > 0 else Decimal(0)
    short_to_long_ratio = (short_sma - long_sma) / long_sma if long_sma > 0 else Decimal(0)

    # BUY: price > short_sma AND short_sma > long_sma
    if latest_price > short_sma and short_sma > long_sma:
        # Scale confidence from 0.75 to 0.85+ based on trend strength
        # Use the weaker of the two ratios (price-to-short vs short-to-long)
        trend_strength = min(price_to_short_ratio, short_to_long_ratio)
        # Map trend_strength to confidence: 0% → 0.75, 5%+ → 0.85+
        base_confidence = Decimal("0.75")
        strength_bonus = trend_strength * Decimal("0.10")  # 5% → +0.005 ... 0.10 → +0.01
        confidence = base_confidence + strength_bonus
        confidence = min(confidence, Decimal("0.90"))  # Cap at 0.90
        confidence = max(confidence, Decimal("0.75"))  # Floor at 0.75

        trend_reason = (
            f"Uptrend: price ${latest_price:.2f} > short_SMA ${short_sma:.2f} "
            f"> long_SMA ${long_sma:.2f}"
        )
        return SignalDirection.BUY, confidence, trend_reason

    # SELL: price < short_sma OR short_sma < long_sma
    if latest_price < short_sma or short_sma < long_sma:
        # Scale confidence from 0.70 based on how far below
        trend_strength = min(
            abs((latest_price - short_sma) / short_sma) if short_sma > 0 else Decimal(0),
            abs((short_sma - long_sma) / long_sma) if long_sma > 0 else Decimal(0),
        )
        base_confidence = Decimal("0.70")
        strength_bonus = trend_strength * Decimal("0.10")
        confidence = base_confidence + strength_bonus
        confidence = min(confidence, Decimal("0.85"))  # Cap at 0.85
        confidence = max(confidence, Decimal("0.70"))  # Floor at 0.70

        if latest_price < short_sma and short_sma < long_sma:
            trend_reason = (
                f"Downtrend: price ${latest_price:.2f} < short_SMA ${short_sma:.2f} "
                f"< long_SMA ${long_sma:.2f}"
            )
        elif latest_price < short_sma:
            trend_reason = (
                f"Price reversal: price ${latest_price:.2f} < short_SMA ${short_sma:.2f}"
            )
        else:
            trend_reason = (
                f"Momentum loss: short_SMA ${short_sma:.2f} < long_SMA ${long_sma:.2f}"
            )
        return SignalDirection.SELL, confidence, trend_reason

    # HOLD: otherwise (sideways, neutral)
    confidence = Decimal("0.60")
    trend_reason = (
        f"Neutral: price ${latest_price:.2f} near short_SMA ${short_sma:.2f} "
        f"and short_SMA near long_SMA ${long_sma:.2f}"
    )
    return SignalDirection.HOLD, confidence, trend_reason
