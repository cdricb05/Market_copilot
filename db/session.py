"""
db/session.py — SQLAlchemy engine and session factories for paper_trader.

Two context managers are provided:

  get_session()
      Standard pooled session for reads and non-workflow writes.
      Commits on clean exit, rolls back on exception, returns the
      connection to the pool automatically.

  get_dedicated_session()
      Session bound to a single connection checked out from the pool
      and held for the full duration of the context block.
      Required for session-level advisory locking workflows:
      pg_try_advisory_lock / pg_advisory_unlock are scoped to the
      *connection*, not the transaction. The same physical connection
      must remain checked out across all transactions within a workflow
      run. The caller is responsible for acquiring and releasing the
      advisory lock; this context manager guarantees only that the
      connection is returned to the pool in the finally block.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from paper_trader.config import get_settings


# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

def _build_engine() -> Engine:
    settings = get_settings()
    return create_engine(
        settings.database_url,
        pool_pre_ping=True,  # drop stale connections before use
    )


_engine: Engine | None = None


def get_engine() -> Engine:
    """Return the module-level engine singleton, creating it on first call."""
    global _engine
    if _engine is None:
        _engine = _build_engine()
    return _engine


def reset_engine_state() -> None:
    """
    Dispose the current engine and reset both module-level singletons to None.

    Intended for use in tests and controlled reloads where a fresh engine and
    sessionmaker are needed (e.g. after patching DATABASE_URL via
    get_settings.cache_clear() + environment variable override).

    Safe to call even if the engine has never been initialised.
    """
    global _engine, _SessionLocal
    if _engine is not None:
        _engine.dispose()
        _engine = None
    _SessionLocal = None


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

_SessionLocal: sessionmaker[Session] | None = None


def _get_session_local() -> sessionmaker[Session]:
    """
    Return the module-level sessionmaker singleton, creating it on first call.

    Lazily initialised so tests can call reset_engine_state() and patch
    DATABASE_URL before the engine is constructed.
    """
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(
            bind=get_engine(),
            autoflush=False,
            expire_on_commit=False,
        )
    return _SessionLocal


# ---------------------------------------------------------------------------
# Public context managers
# ---------------------------------------------------------------------------

@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Yield a pooled SQLAlchemy Session.

    Commits on clean exit. Rolls back and re-raises on any exception.
    The connection is returned to the pool in both cases.

    Usage::

        with get_session() as session:
            session.add(some_model)
    """
    session = _get_session_local()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


@contextmanager
def get_dedicated_session() -> Generator[Session, None, None]:
    """
    Yield a Session bound to a single connection checked out from the pool
    and held for the full duration of the context block.

    Unlike get_session(), this connection is NOT returned to the pool between
    transactions — it stays checked out until the finally block. This is
    required for session-level PostgreSQL advisory locks
    (pg_try_advisory_lock / pg_advisory_unlock), which are scoped to the
    *connection*. Committing or rolling back a transaction does not release
    them; only closing the connection does.

    The caller is responsible for:
      - Acquiring the advisory lock immediately after entering the context.
      - Explicitly calling session.commit() / session.rollback() to control
        transaction boundaries within the workflow.
      - Releasing the advisory lock before exiting the context.

    The connection is returned to the pool in the finally block, which also
    releases any unreleased session-level advisory locks as a safety net.

    Usage::

        with get_dedicated_session() as session:
            acquired = session.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
            ).scalar()
            if not acquired:
                raise RuntimeError("Could not acquire portfolio advisory lock")
            try:
                # --- transaction 1 ---
                session.execute(...)
                session.commit()

                # --- transaction 2 ---
                session.execute(...)
                session.commit()
            finally:
                session.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": PORTFOLIO_ADVISORY_LOCK_KEY},
                )
                session.commit()
    """
    connection = get_engine().connect()
    session = Session(
        bind=connection,
        autoflush=False,
        expire_on_commit=False,
    )
    try:
        yield session
    finally:
        session.close()
        connection.close()
