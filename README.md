# paper_trader

A Python paper-trading system that processes trading signals, evaluates pre-trade risk
constraints, and simulates order execution against price snapshots with a
PostgreSQL-backed audit ledger.

## Phase 1 Capabilities

- **Signal ingestion**: Accept BUY/SELL/HOLD signals via HTTP; run risk checks; create
  PENDING orders for approved signals
- **Risk engine**: Enforces position limits, concentration caps, daily exposure limits,
  minimum cash reserves, confidence thresholds, ticker cooldowns, and duplicate signal
  detection
- **Order reconciliation**: Match PENDING orders against price snapshots with configurable
  slippage (basis points) and flat commission; expire stale orders by TTL
- **Portfolio accounting**: Immutable append-only cash ledger; weighted-average cost (WAC)
  position tracking; portfolio cache refresh after each fill cycle
- **HTTP API**: Six authenticated endpoints covering signals, fills, prices, positions,
  orders, and portfolio state
- **Idempotency**: All write workflows are keyed by `idempotency_key`; a COMPLETED run
  returns its cached result; RUNNING or FAILED status raises an error immediately

## Project Structure

```text
paper_trader/          <- repo root and package directory
├── __init__.py
├── alembic.ini
├── config.py
├── constants.py
├── pyproject.toml
├── .env.example
├── .gitignore
├── api/
│   └── app.py
├── db/
│   ├── __init__.py
│   ├── models.py
│   ├── session.py
│   └── migrations/
│       ├── __init__.py
│       ├── env.py
│       ├── script.py.mako
│       └── versions/
│           ├── __init__.py
│           └── 0001_initial_schema.py
├── engine/
│   ├── __init__.py
│   ├── portfolio.py
│   ├── reconciler.py
│   └── risk.py
├── schemas/
│   └── __init__.py
├── scripts/
│   ├── __init__.py
│   └── seed.py
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_api.py
│   ├── test_decision.py
│   ├── test_portfolio.py
│   ├── test_reconciler.py
│   └── test_risk.py
└── workflows/
    └── decision.py
```

## Prerequisites

- Python 3.12+
- PostgreSQL 12+

## Environment Variables

Copy `.env.example` to `.env` and fill in values before running anything.

Required:

```text
PAPER_TRADER_DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/paper_trader
PAPER_TRADER_SERVICE_API_KEY=change-me-before-use
```

Optional (defaults shown):

```text
PAPER_TRADER_INITIAL_CAPITAL=10000.00
PAPER_TRADER_MAX_POSITIONS=5
PAPER_TRADER_MAX_CONCENTRATION_PCT=0.20
PAPER_TRADER_MIN_CASH_PCT=0.10
PAPER_TRADER_MAX_DAILY_NEW_EXPOSURE_PCT=0.40
PAPER_TRADER_CONFIDENCE_THRESHOLD=0.55
PAPER_TRADER_MIN_ORDER_NOTIONAL=50.00
PAPER_TRADER_COOLDOWN_HOURS=48
PAPER_TRADER_ALLOW_AVERAGING_DOWN=false
PAPER_TRADER_BENCHMARK_TICKER=SPY
PAPER_TRADER_LOG_LEVEL=INFO
```

Test suite only (database tests are skipped when absent):

```text
PAPER_TRADER_TEST_DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/paper_trader_test
```

## Python Path

The repo root is the `paper_trader` package directory. With this layout,
`import paper_trader` requires the parent directory of `paper_trader/` on `PYTHONPATH`.
This does not happen automatically — not from activating a virtualenv and not from
`pip install -e .` in this specific repo layout.

Set it explicitly before running Python commands:

```bash
export PYTHONPATH=/path/to/parent:$PYTHONPATH
```

The test suite (`tests/conftest.py`) sets the path automatically at runtime, so
`python -m pytest` works once PYTHONPATH is set or the parent is otherwise reachable.

## Setup

1. Create and activate a virtual environment.

```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

2. Install dependencies in editable mode.

```bash
pip install -e ".[test]"
```

3. Copy the environment template and fill in your credentials.

```bash
cp .env.example .env
```

4. Apply database migrations.

```bash
python -m alembic upgrade head
```

5. Seed the portfolio with starting capital.

```bash
python scripts/seed.py
```

## Testing

```bash
python -m pytest tests/ -v
```

Database tests require `PAPER_TRADER_TEST_DATABASE_URL` in `.env` and are skipped
automatically when it is absent.

## Starting the API Server

```bash
python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001
```

Interactive docs available at `http://127.0.0.1:8001/docs`.

All requests require the header `X-API-Key` set to the value of
`PAPER_TRADER_SERVICE_API_KEY`.

## API Endpoints

### POST /v1/prices — ingest price snapshots

```bash
curl -s -X POST http://127.0.0.1:8001/v1/prices \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"snapshots": [{"ticker": "AAPL", "price": "182.50"}]}'
```

### POST /v1/signals — ingest a signal batch and run the decision workflow

```bash
curl -s -X POST http://127.0.0.1:8001/v1/signals \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "pre_market_2025-01-15",
    "signals": [{
      "ticker": "AAPL",
      "direction": "BUY",
      "confidence": "0.80",
      "signal_ts": "2025-01-15T14:00:00Z",
      "source_run": "strategy-run-001"
    }]
  }'
```

### POST /v1/fill — execute PENDING orders for a market date

```bash
curl -s -X POST http://127.0.0.1:8001/v1/fill \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key": "fill_2025-01-15"}'
```

### GET /v1/positions — list open positions

```bash
curl -s http://127.0.0.1:8001/v1/positions \
  -H "X-API-Key: change-me-before-use"
```

### GET /v1/orders — list orders with optional filters

```bash
curl -s "http://127.0.0.1:8001/v1/orders?status=PENDING&market_date=2025-01-15" \
  -H "X-API-Key: change-me-before-use"
```

### GET /v1/portfolio — current portfolio state

```bash
curl -s http://127.0.0.1:8001/v1/portfolio \
  -H "X-API-Key: change-me-before-use"
```

## Phase 2 Ideas

- Market hours validation (block orders outside the regular session)
- Limit orders and stop-loss order types
- Historical portfolio snapshots for performance reporting
- Dividend and corporate action handling
- Real market data feed connectors
- Benchmark comparison and P&L attribution reporting
