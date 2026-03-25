"""
db/migrations/env.py — Alembic environment for paper_trader.

The repo root (paper_trader/) is itself the package, so its parent directory
must be on sys.path for `import paper_trader` to resolve. This file inserts
that parent automatically before any application import.

Database URL is read from PAPER_TRADER_DATABASE_URL via get_settings();
sqlalchemy.url in alembic.ini is intentionally blank.
"""
from __future__ import annotations

import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# env.py lives at:  <repo_root>/db/migrations/env.py
# parents[0] = db/migrations/
# parents[1] = db/
# parents[2] = <repo_root>/          ← the paper_trader package dir
# parents[3] = <repo_root's parent>  ← needs to be on sys.path
_pkg_parent = str(Path(__file__).resolve().parents[3])
if _pkg_parent not in sys.path:
    sys.path.insert(0, _pkg_parent)

from paper_trader.config import get_settings  # noqa: E402
from paper_trader.db.models import Base        # noqa: E402

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _db_url() -> str:
    return get_settings().database_url


def run_migrations_offline() -> None:
    """Run migrations without a live DB connection (generates SQL to stdout)."""
    context.configure(
        url=_db_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live DB connection."""
    cfg = config.get_section(config.config_ini_section, {})
    cfg["sqlalchemy.url"] = _db_url()
    connectable = engine_from_config(
        cfg,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
