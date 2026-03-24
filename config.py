"""
config.py — Application configuration via pydantic-settings 2.x.

All settings are loaded from environment variables with the prefix PAPER_TRADER_.
Required: DATABASE_URL, SERVICE_API_KEY.
All monetary/percentage defaults are stored as strings to be safely parsed into Decimal.

Do not import a module-level settings singleton from here. Instead call
get_settings() which is lru_cache'd so the Settings object is only constructed
once per process (and can be easily overridden in tests via cache_clear).
"""
from __future__ import annotations

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Central settings object. Loaded from environment variables prefixed with PAPER_TRADER_.

    Required environment variables:
        PAPER_TRADER_DATABASE_URL      PostgreSQL connection string.
        PAPER_TRADER_SERVICE_API_KEY   API key used to authenticate calls to the paper_trader
                                       service endpoints.

    All monetary defaults are Decimal-safe strings; convert with Decimal(str(value)).
    """

    model_config = SettingsConfigDict(
        env_prefix="PAPER_TRADER_",
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )

    # --- Required ---
    database_url: str = Field(..., description="PostgreSQL DSN, e.g. postgresql+psycopg2://user:pass@host/db")
    service_api_key: str = Field(..., description="API key for authenticating paper_trader service endpoints")

    # --- Portfolio seeding defaults ---
    initial_capital: str = Field(
        default="10000.00",
        description="Starting capital in dollars (Decimal-safe string)",
    )

    # --- Risk engine defaults ---
    max_positions: int = Field(default=5, description="Maximum number of concurrent open positions")
    max_concentration_pct: str = Field(
        default="0.20",
        description="Max fraction of portfolio in a single position (e.g. 0.20 = 20%)",
    )
    min_cash_pct: str = Field(
        default="0.10",
        description="Minimum fraction of portfolio to keep as cash (e.g. 0.10 = 10%)",
    )
    max_daily_new_exposure_pct: str = Field(
        default="0.40",
        description="Max fraction of portfolio that can be deployed in new buys per day",
    )
    confidence_threshold: str = Field(
        default="0.55",
        description="Minimum signal confidence to approve a BUY (e.g. 0.55 = 55%)",
    )
    min_order_notional: str = Field(
        default="50.00",
        description="Minimum dollar size of a single approved order",
    )
    cooldown_hours: int = Field(
        default=48,
        description="Hours after a SELL trade during which re-buying the same ticker is blocked",
    )
    allow_averaging_down: bool = Field(
        default=False,
        description="Whether to allow adding to an existing position (averaging down)",
    )

    # --- Benchmark ---
    benchmark_ticker: str = Field(default="SPY", description="Ticker used for benchmark comparison")

    # --- Logging ---
    log_level: str = Field(default="INFO", description="Python logging level name")

    # --- Test database ---
    test_database_url: str | None = Field(
        default=None,
        description=(
            "Optional separate DSN used by the test suite. "
            "Maps to env var PAPER_TRADER_TEST_DATABASE_URL. "
            "DB tests are skipped when this is not set."
        ),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Return the application Settings singleton.

    The object is constructed once and cached. To force re-construction (e.g.
    in tests that patch environment variables) call get_settings.cache_clear()
    before the call that needs the fresh settings.
    """
    return Settings()
