"""
tests/conftest.py — pytest fixtures for the paper_trader test suite.

Database tests require PAPER_TRADER_TEST_DATABASE_URL to be set. Any test
that accepts db_session or seeded_portfolio is automatically skipped when the
env var is absent.

Isolation strategy:
    Each test runs inside a SAVEPOINT nested within a connection-level
    transaction that is never committed. After the test, the outer transaction
    is rolled back, leaving the schema intact for the next test. No TRUNCATE
    or DROP between tests.

Usage:
    PAPER_TRADER_TEST_DATABASE_URL=postgresql+psycopg2://user:pass@host/test_db \\
        pytest
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

# ---------------------------------------------------------------------------
# sys.path bootstrap
#
# conftest.py lives at:  <repo_root>/tests/conftest.py
# parents[0] = tests/
# parents[1] = <repo_root>/          ← the paper_trader package dir
# parents[2] = <repo_root's parent>  ← must be on sys.path for `import paper_trader`
#
# This mirrors the same fix applied in db/migrations/env.py (parents[3] there
# because env.py is two levels deeper).
# ---------------------------------------------------------------------------
_pkg_parent = str(Path(__file__).resolve().parents[2])
if _pkg_parent not in sys.path:
    sys.path.insert(0, _pkg_parent)

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import Base, Portfolio
from paper_trader.engine.portfolio import append_cash_entry


# ---------------------------------------------------------------------------
# Settings cache management
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session", autouse=True)
def _declare_pytest_process_hermetic() -> None:
    """Stage 22 — the pytest process IS a hermetic fixture world; say so explicitly.

    ``api.app`` runs ``environment_isolation.assert_production_store_roots()`` at import
    time and refuses to serve when a canonical store points at a temp root. The autouse
    fixture below deliberately redirects the corporate-action registry into a pytest temp
    root, so importing the app inside a test body raised RuntimeError — while the SAME
    import succeeded when some earlier module imported the app at collection time, before
    any fixture ran. The suite therefore passed as a whole and failed file-by-file, which
    is the worst possible failure mode for "run the affected tests yourself".

    This is the same explicit per-process opt-in the hermetic acceptance backend uses
    (``scripts/stage20_acceptance_server.py``). It is set only inside the pytest process,
    never in the operator's shell, and the production guard is untouched: a real backend
    start with fixture roots still fails closed, and the restart regression explicitly
    strips this flag from its child environment before exercising that path.
    """
    os.environ.setdefault("PAPER_TRADER_ACCEPTANCE_MODE", "1")
    yield


@pytest.fixture(autouse=True)
def _hermetic_corporate_action_registry(tmp_path_factory, monkeypatch) -> None:
    """Stage 19.1 — never let the LIVE corporate-action registry leak into a test.

    ``api.corporate_actions`` falls back to a production root when
    ``PAPER_TRADER_CORPORATE_ACTIONS_DIR`` is unset, and the Stage-19.1 current-state
    reads consult that registry BY DEFAULT (``desk.book_nav`` / ``book_cash_holdings`` /
    ``current_fills`` / ``load_performance``). Without this fixture a hermetic temp-root
    desk test would silently apply the operator's real registered corporate actions to its
    own fixture fills. Every test therefore starts against an EMPTY registry in its own
    temp root — which is also the exact backward-compatibility case (an empty registry is
    a no-op). A test that needs registered actions sets the env var itself; that
    assignment happens inside the test body and wins over this fixture.
    """
    current = os.environ.get("PAPER_TRADER_CORPORATE_ACTIONS_DIR")
    production_default = str(Path(r"D:\Stock_Prediction_app_data\corporate_actions"))
    if not current or Path(current) == Path(production_default):
        monkeypatch.setenv(
            "PAPER_TRADER_CORPORATE_ACTIONS_DIR",
            str(tmp_path_factory.mktemp("corporate_actions_hermetic")))
    yield


@pytest.fixture(autouse=True)
def _clear_settings_cache() -> None:
    """
    Clear the lru_cache on get_settings() before every test.

    Required so that tests which monkeypatch environment variables receive a
    fresh Settings object instead of the cached one from a previous test.
    """
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


# ---------------------------------------------------------------------------
# Database engine (session-scoped: created once per pytest run)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def _test_db_url() -> str:
    """
    Return the test database URL, skipping the entire session if not set.

    Reads PAPER_TRADER_TEST_DATABASE_URL directly from the environment so
    that this fixture has no dependency on config.py or get_settings().
    """
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip(
            "PAPER_TRADER_TEST_DATABASE_URL is not set — skipping all DB tests."
        )
    return url


@pytest.fixture(scope="session")
def db_engine(_test_db_url: str) -> Engine:
    """
    Create the test SQLAlchemy engine, build all tables, yield, then drop all.

    Table creation and teardown happen once per pytest invocation, not per test.
    """
    engine = create_engine(_test_db_url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    Base.metadata.drop_all(engine)
    engine.dispose()


# ---------------------------------------------------------------------------
# Transactional test session (function-scoped: rolled back after every test)
# ---------------------------------------------------------------------------

@pytest.fixture
def db_session(db_engine: Engine) -> Session:
    """
    Yield a Session that is rolled back after each test.

    Pattern:
        1. Check out a connection and begin an outer transaction.
        2. Open a SAVEPOINT (begin_nested) — the session operates within it.
        3. After the test, roll back the outer transaction; the DB is pristine.

    The Session is bound to the connection using the same Session(bind=...)
    pattern established in db/session.py:get_dedicated_session().
    """
    connection = db_engine.connect()
    outer_tx = connection.begin()
    session = Session(bind=connection, autoflush=False, expire_on_commit=False)
    nested = connection.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def _restart_savepoint(sess: Session, trans) -> None:  # noqa: ANN001
        if trans.nested and not trans._parent.nested:
            sess.expire_all()
            nonlocal nested
            nested = connection.begin_nested()

    yield session

    session.close()
    outer_tx.rollback()
    connection.close()


# ---------------------------------------------------------------------------
# Seeded portfolio fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def seeded_portfolio(db_session: Session) -> Portfolio:
    """
    Insert a Portfolio row and its INITIAL_CAPITAL ledger entry.

    Uses $10,000 starting capital. Flushes but does not commit — rollback
    isolation in db_session will undo everything after the test.
    """
    now = datetime.now(tz=timezone.utc)
    portfolio = Portfolio(
        inception_date=now.date(),
        initial_capital=Decimal("10000.00"),
        strategy_enabled=True,
        trading_enabled=True,
        allow_new_positions=True,
        config={},
        cached_cash=Decimal("10000.00"),
        cached_total_value=Decimal("10000.00"),
        cached_as_of_ts=now,
    )
    db_session.add(portfolio)
    db_session.flush()

    append_cash_entry(
        db_session,
        portfolio_id=portfolio.id,
        entry_type=CashEntryType.INITIAL_CAPITAL,
        amount=Decimal("10000.00"),
        description="Test initial capital",
    )
    db_session.flush()
    return portfolio
