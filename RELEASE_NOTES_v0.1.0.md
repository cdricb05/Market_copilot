# Release Notes — v0.1.0

**paper_trader v0.1.0** is the initial MVP release of a Python paper-trading system that ingests trading signals, enforces portfolio risk constraints, simulates order execution against price snapshots, and maintains an immutable audit ledger with weighted-average cost position tracking.

## What's Included

### Core Trading Workflow

- **Signal ingestion** via `POST /v1/signals`: accept BUY/SELL/HOLD signals with confidence scores; run risk checks; atomically create PENDING orders for approved signals only
- **Risk engine** enforcement:
  - Position limits (max 5 open positions by default)
  - Concentration caps (max 20% notional per ticker by default)
  - Daily exposure limits (max 40% of capital deployed in new positions per market date)
  - Minimum cash reserves (min 10% portfolio in cash by default)
  - Confidence threshold (min 55% confidence by default)
  - Ticker cooldown (48 hours between signals to same ticker by default)
  - Duplicate signal detection (rejects duplicate tickers in same batch)
- **Order reconciliation** via `POST /v1/fill`: match PENDING orders against price snapshots with configurable slippage (basis points) and flat commission per trade; expire stale orders by TTL
- **Portfolio accounting**:
  - Immutable append-only cash ledger (source of truth for cash balance)
  - Weighted-average cost (WAC) position tracking
  - Portfolio cache refresh after each fill cycle
- **Portfolio snapshots** via `POST /v1/snapshot`: post-market portfolio state capture with holdings, cash, and performance metrics
- **HTTP API** with 14 endpoints total (12 authenticated, 2 public health/readiness checks) covering signals, fills, prices, positions, orders, snapshots, portfolio state, and performance history (CSV export supported)
- **Idempotency** on all write workflows: keyed by `idempotency_key`; COMPLETED runs return cached result; RUNNING or FAILED status raises error immediately
- **Weekday guard** on signal ingestion: endpoint rejects calls when the server's current US/Eastern date is a weekend (expected behavior, not a bug)

### Database and Audit

- PostgreSQL 16 with SQLAlchemy 2.x ORM
- Alembic schema migrations with transactional integrity
- Audit trail: immutable Signal, TradeDecision, Order, Trade, and PortfolioSnapshot tables
- Advisory locking (PostgreSQL `pg_try_advisory_lock`) for safe concurrent fill-cycle execution
- Numeric precision throughout: 18.2 for dollars, 18.6 for prices, 18.8 for quantities (no float arithmetic on money)

### Testing and CI

- Comprehensive automated test suite covering API contracts, engine logic, database models, migrations, and edge cases
- SAVEPOINT-based test isolation for fast, safe database-backed tests
- GitHub Actions CI: automated install, migrations, and test runs on every push and pull request
- CI green on `main` branch

### Developer Experience

- Editable install: `pip install -e ".[test]"` for active development
- Local API server startup: `python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001`
- Seed script for portfolio initialization: `python scripts/seed.py`
- Clear environment variable configuration (`.env.example` provided)
- PYTHONPATH bootstrap documented in CONTRIBUTING.md

## Verifying This Release

For a quick sanity check:

1. **Local build**: `pip install -e ".[test]"` completes without errors
2. **Tests**: `python -m pytest tests/ -v` — zero failures
3. **Database migration**: `python -m alembic -c alembic.ini upgrade head` applies cleanly from zero
4. **API health**: start the server and curl the `/v1/health` and `/v1/ready` endpoints
5. **Smoke test**: run at least one signal ingestion and fill cycle against a seeded portfolio

For complete pre-release verification, follow **[MVP_RELEASE_CHECKLIST.md](MVP_RELEASE_CHECKLIST.md)** (9 sections, ~150 items).

## Known Non-Blocking Limitations

These do not block the v0.1.0 release. Log them for the next iteration.

| Issue | Location | Impact | Mitigation |
|-------|----------|--------|-----------|
| SQLAlchemy 2.0 legacy pattern | `tests/conftest.py` | `Session(bind=connection)` will break on future SQLAlchemy major upgrade | Refactor to newer Session API before upgrading |
| ValueError → HTTP 500 mapping | `api/app.py` fill endpoint | Unstructured error response; worth auditing against all reconciler error conditions | Audit error conditions and map to appropriate HTTP codes |
| Fragile test ordering | `tests/test_api.py` | Test class order held by fixture dependencies, not pytest configuration; easy to break on reordering | Either enforce order via pytest markers or restructure fixtures to be order-independent |
| Version decoupling | `pyproject.toml` vs `app.py` | Package version (0.1.0) and API version (1.0.0) are independent with no documentation explaining the distinction | Document design decision or synchronize versions in next major bump |

## Intentionally Out of Scope for v0.1.0

The following features are planned for Phase 2 and are **not** included in this release:

- **Market hours validation**: No market-hours validation beyond the existing weekday guard on signal ingestion. No intraday market-hours enforcement for order submission.
- **Order types**: Only simple market orders are supported. No limit orders, stop-loss orders, or other conditional order types.
- **Corporate actions**: No dividend handling, stock splits, or other corporate events. Position quantities and cash are treated as static snapshots.
- **Live market data feed**: No real-time price connectors. All prices are ingested via HTTP API (`POST /v1/prices`, `POST /v1/benchmark-prices`).

## Deployment Notes

- **Python**: Requires 3.12+
- **Database**: Requires PostgreSQL 12+; tested against PostgreSQL 16
- **Network**: No public internet dependencies for core operation (but optional extensions may add them in Phase 2)
- **Environment**: All configuration via environment variables in `.env` (no code edits required)

## API Version Note

This release ships with **API version 1.0.0** (returned in `/v1/health` responses) while the **package version is 0.1.0**. These versions are currently decoupled; the relationship between package and API versions should be clarified or synchronized in a future release. This is documented in MVP_RELEASE_CHECKLIST.md §9.

## Next Steps

See [README.md](README.md) for setup instructions, endpoint documentation, and environment variable reference. See [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow and pre-push checklist.

For questions or issues, open a GitHub issue or PR.
