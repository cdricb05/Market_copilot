"""
constants.py — All enum types and global constants for the paper_trader system.

Every enum is a str subclass so values serialize directly to their string value
without requiring .value access.
"""
from __future__ import annotations


class OrderStatus(str):
    PENDING = "PENDING"
    FILLED = "FILLED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


class OrderSide(str):
    BUY = "BUY"
    SELL = "SELL"


class SignalDirection(str):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class SignalStatus(str):
    RECEIVED = "RECEIVED"
    PROCESSING = "PROCESSING"
    DECISION_MADE = "DECISION_MADE"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"


class DecisionType(str):
    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"
    REJECTED = "REJECTED"


class RejectionReason(str):
    STRATEGY_DISABLED = "STRATEGY_DISABLED"
    TRADING_DISABLED = "TRADING_DISABLED"
    NEW_POSITIONS_DISABLED = "NEW_POSITIONS_DISABLED"
    CONFIDENCE_BELOW_THRESHOLD = "CONFIDENCE_BELOW_THRESHOLD"
    MAX_POSITIONS_REACHED = "MAX_POSITIONS_REACHED"
    AVERAGING_DOWN_BLOCKED = "AVERAGING_DOWN_BLOCKED"
    TICKER_IN_COOLDOWN = "TICKER_IN_COOLDOWN"
    DAILY_EXPOSURE_LIMIT = "DAILY_EXPOSURE_LIMIT"
    CASH_RESERVE_BREACH = "CASH_RESERVE_BREACH"
    INSUFFICIENT_CASH = "INSUFFICIENT_CASH"
    MIN_ORDER_TOO_SMALL = "MIN_ORDER_TOO_SMALL"
    CONCENTRATION_LIMIT = "CONCENTRATION_LIMIT"
    NO_PRICE_SNAPSHOT = "NO_PRICE_SNAPSHOT"
    NO_POSITION_TO_SELL = "NO_POSITION_TO_SELL"
    DUPLICATE_SIGNAL = "DUPLICATE_SIGNAL"
    HOLD_SIGNAL = "HOLD_SIGNAL"


class WorkflowType(str):
    PRE_MARKET = "PRE_MARKET"
    MIDDAY = "MIDDAY"
    POST_MARKET = "POST_MARKET"


class JobRunStatus(str):
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class SessionType(str):
    PREMARKET = "PREMARKET"
    REGULAR = "REGULAR"
    POSTMARKET = "POSTMARKET"
    EXTENDED = "EXTENDED"
    MANUAL = "MANUAL"


class PriceType(str):
    OPEN = "OPEN"
    CLOSE = "CLOSE"
    LAST = "LAST"
    BID = "BID"
    ASK = "ASK"
    MID = "MID"
    VWAP = "VWAP"


class CashEntryType(str):
    INITIAL_CAPITAL = "INITIAL_CAPITAL"
    BUY_DEBIT = "BUY_DEBIT"
    SELL_CREDIT = "SELL_CREDIT"
    COMMISSION_DEBIT = "COMMISSION_DEBIT"
    DIVIDEND_CREDIT = "DIVIDEND_CREDIT"
    ADJUSTMENT = "ADJUSTMENT"


# Advisory lock key used for portfolio-level PostgreSQL session advisory locks.
PORTFOLIO_ADVISORY_LOCK_KEY: int = 987654321
