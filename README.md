# paper_trader

[![CI](https://github.com/cdricb05/Market_copilot/actions/workflows/ci.yml/badge.svg)](https://github.com/cdricb05/Market_copilot/actions/workflows/ci.yml)

For developer setup and contribution workflow, see [CONTRIBUTING.md](CONTRIBUTING.md).

A Python paper-trading system that processes trading signals, evaluates pre-trade risk
constraints, and simulates order execution against price snapshots with a
PostgreSQL-backed audit ledger.

## Capabilities

- **Signal ingestion**: Accept BUY/SELL/HOLD signals via HTTP; run risk checks; create
  PENDING orders for approved signals
- **Risk engine**: Enforces position limits, concentration caps, daily exposure limits,
  minimum cash reserves, confidence thresholds, ticker cooldowns, and duplicate signal
  detection
- **Order reconciliation**: Match PENDING orders against price snapshots with configurable
  slippage (basis points) and flat commission; expire stale orders by TTL
- **Portfolio accounting**: Immutable append-only cash ledger; weighted-average cost (WAC)
  position tracking; portfolio cache refresh after each fill cycle
- **HTTP API**: Authenticated endpoints covering signals, fills, prices, snapshots,
  positions, orders, portfolio state, and performance history
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
│   ├── market_hours.py
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
│   ├── test_market_hours.py
│   ├── test_portfolio.py
│   ├── test_reconciler.py
│   ├── test_risk.py
│   └── test_snapshot.py
└── workflows/
    ├── decision.py
    └── snapshot.py
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

Test suite only: `PAPER_TRADER_TEST_DATABASE_URL` is read from the process
environment of the pytest shell, never from `.env`. Database tests create,
truncate and drop every table, so the R85 gate (`tests/_pg_test_database_gate.py`)
admits the URL only when it names a separate, disposable database whose server
identity differs from `PAPER_TRADER_DATABASE_URL` and which carries
`COMMENT ON DATABASE <name> IS 'PAPER_TRADER_DISPOSABLE_TEST_DATABASE'`. A name
containing "test" proves nothing. Otherwise every database test skips.

## Python Path

The repo root is the `paper_trader` package directory. With this layout,
`import paper_trader` requires the parent directory of `paper_trader/` on `PYTHONPATH`.
This does not happen automatically — not from activating a virtualenv and not from
`pip install -e .` in this specific repo layout.

**Bash / macOS / Linux:**

```bash
export PYTHONPATH=/path/to/parent:$PYTHONPATH
```

**Windows PowerShell:**

```powershell
$env:PYTHONPATH = "C:\path\to\parent;$env:PYTHONPATH"
```

The test suite (`tests/conftest.py`) sets the path automatically at runtime, so
`python -m pytest` works once PYTHONPATH is set or the parent is otherwise reachable.

## Setup

### Bash (macOS / Linux)

1. Create and activate a virtual environment.

```bash
python -m venv .venv
source .venv/bin/activate
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

### Windows PowerShell

1. Create and activate a virtual environment.

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

2. Install dependencies in editable mode.

```powershell
pip install -e ".[test]"
```

3. Copy the environment template and fill in your credentials.

```powershell
Copy-Item .env.example .env
```

4. Apply database migrations.

```powershell
python -m alembic upgrade head
```

5. Seed the portfolio with starting capital.

```powershell
python scripts/seed.py
```

## Testing

```bash
python -m pytest tests/ -v
```

Database tests require `PAPER_TRADER_TEST_DATABASE_URL` in the pytest process
environment (not `.env`) naming a database the R85 gate admits; they are skipped
automatically when it is absent or refused.

## Starting the API Server

```bash
python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001
```

Interactive docs available at `http://127.0.0.1:8001/docs`.

## Authentication

All endpoints except `/v1/health` and `/v1/ready` require the header:

```
X-API-Key: <value of PAPER_TRADER_SERVICE_API_KEY>
```

Missing or invalid key returns `401 Unauthorized`.

## Quick Verification

Use these commands immediately after starting the server to confirm the stack is healthy.

**Bash:**

```bash
# No auth required — lightweight process check
curl -s http://127.0.0.1:8001/v1/health

# No auth required — confirms database is reachable
curl -s http://127.0.0.1:8001/v1/ready

# Auth required — confirms API key works and portfolio is seeded
curl -s http://127.0.0.1:8001/v1/portfolio \
  -H "X-API-Key: change-me-before-use"
```

**Windows PowerShell (`curl.exe` — not `curl`):**

```powershell
# No auth required — lightweight process check
curl.exe -s http://127.0.0.1:8001/v1/health

# No auth required — confirms database is reachable
curl.exe -s http://127.0.0.1:8001/v1/ready

# Auth required — confirms API key works and portfolio is seeded
curl.exe -s http://127.0.0.1:8001/v1/portfolio `
  -H "X-API-Key: change-me-before-use"
```

Expected `/v1/health` response:

```json
{"status": "ok", "service": "paper_trader", "version": "1.0.0"}
```

Expected `/v1/ready` response (database reachable):

```json
{"status": "ok", "service": "paper_trader", "version": "1.0.0", "database": "ok"}
```

## API Endpoints

### Health and Readiness

`/v1/health` and `/v1/ready` serve different purposes and require no `X-API-Key`:

| Endpoint | Auth | DB check | Purpose |
|---|---|---|---|
| `GET /v1/health` | No | No | Process is alive; safe for load-balancer liveness probes |
| `GET /v1/ready` | No | Yes | Database is reachable; use for readiness probes; returns `503` if DB is down |

### POST /v1/prices — ingest price snapshots

```bash
curl -s -X POST http://127.0.0.1:8001/v1/prices \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"snapshots": [{"ticker": "AAPL", "price": "182.50"}]}'
```

### POST /v1/benchmark-prices — ingest benchmark price snapshots

```bash
curl -s -X POST http://127.0.0.1:8001/v1/benchmark-prices \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"prices": [{"ticker": "SPY", "price": "510.00"}]}'
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

Weekend calls return 422.

### POST /v1/fill — execute PENDING orders for a market date

```bash
curl -s -X POST http://127.0.0.1:8001/v1/fill \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key": "fill_2025-01-15"}'
```

### POST /v1/snapshot — run the post-market portfolio snapshot workflow

```bash
curl -s -X POST http://127.0.0.1:8001/v1/snapshot \
  -H "X-API-Key: change-me-before-use" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key": "snapshot_2025-01-15"}'
```

Returns 422 if price data for the current market date is missing.

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

`status` and `market_date` are both optional.

### GET /v1/snapshots — list all portfolio snapshots

```bash
curl -s http://127.0.0.1:8001/v1/snapshots \
  -H "X-API-Key: change-me-before-use"
```

Returns snapshots newest first.

### GET /v1/snapshots/{market_date} — single snapshot by date

```bash
curl -s http://127.0.0.1:8001/v1/snapshots/2025-01-15 \
  -H "X-API-Key: change-me-before-use"
```

Returns 404 if no snapshot exists for that date.

### GET /v1/portfolio — current portfolio state

```bash
curl -s http://127.0.0.1:8001/v1/portfolio \
  -H "X-API-Key: change-me-before-use"
```

Returns 503 if the portfolio has not been seeded.

### GET /v1/performance — inception-to-date performance summary

```bash
curl -s http://127.0.0.1:8001/v1/performance \
  -H "X-API-Key: change-me-before-use"
```

Returns 404 if no snapshots exist yet.

### GET /v1/performance/history — chronological performance snapshots

```bash
# All history
curl -s http://127.0.0.1:8001/v1/performance/history \
  -H "X-API-Key: change-me-before-use"

# Date-filtered window
curl -s "http://127.0.0.1:8001/v1/performance/history?start_date=2025-01-01&end_date=2025-01-31" \
  -H "X-API-Key: change-me-before-use"
```

`start_date` and `end_date` are both optional.

### GET /v1/performance/history.csv — performance history as a CSV download

```bash
curl -s "http://127.0.0.1:8001/v1/performance/history.csv" \
  -H "X-API-Key: change-me-before-use" \
  -o performance.csv
```

With a date window:

```bash
curl -s "http://127.0.0.1:8001/v1/performance/history.csv?start_date=2025-01-01&end_date=2025-01-31" \
  -H "X-API-Key: change-me-before-use" \
  -o performance_jan.csv
```

**Windows PowerShell:**

```powershell
curl.exe -s "http://127.0.0.1:8001/v1/performance/history.csv" `
  -H "X-API-Key: change-me-before-use" `
  -o performance.csv
```

Null numeric values are exported as empty strings. Returns 404 if no history exists.

## Phase 2 Ideas

- Market hours validation (block orders outside the regular session)
- Limit orders and stop-loss order types
- Dividend and corporate action handling
- Real market data feed connectors
