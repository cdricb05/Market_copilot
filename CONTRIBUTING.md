# Contributing

This file is for developers. For API usage see [README.md](README.md).

## Repo layout caveat

The repo root (`paper_trader/`) **is** the `paper_trader` package directory — it contains
both `pyproject.toml` and `__init__.py`. As a result, `import paper_trader` requires the
**parent** directory of the repo root on `PYTHONPATH`.

**Bash / macOS / Linux** — from inside the repo root:

```bash
export PYTHONPATH="$(dirname "$PWD"):$PYTHONPATH"
```

**Windows PowerShell** — from inside the repo root:

```powershell
$env:PYTHONPATH = (Split-Path -Parent $PWD) + ";" + $env:PYTHONPATH
```

The test suite (`tests/conftest.py`) sets this automatically at pytest runtime, so
`python -m pytest` works once the venv is active and dependencies are installed. The API
server does **not** set it automatically — export it before running uvicorn.

## Environment setup

**Bash / macOS / Linux:**

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[test]"
cp .env.example .env
```

**Windows PowerShell:**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e ".[test]"
Copy-Item .env.example .env
```

Edit `.env` and fill in at minimum:

```text
PAPER_TRADER_DATABASE_URL=postgresql+psycopg2://postgres:postgres@localhost:5432/paper_trader
PAPER_TRADER_SERVICE_API_KEY=dev-key
```

## Required environment variables

| Variable | Purpose | Required for |
|---|---|---|
| PAPER_TRADER_DATABASE_URL | Main application database | API server, seed script, migrations |
| PAPER_TRADER_SERVICE_API_KEY | API authentication header | API server |
| PAPER_TRADER_TEST_DATABASE_URL | Separate test database | Database tests |

Use a **separate, dedicated database** for `PAPER_TRADER_TEST_DATABASE_URL`. Tests may
create, drop, or modify schema and data. Never point `PAPER_TRADER_TEST_DATABASE_URL` at
your main or development database.

## Migrations

Always run migrations with the `-c alembic.ini` flag. Bare `alembic` without `-c` may not
find the config depending on your working directory.

```bash
# Apply all pending migrations
python -m alembic -c alembic.ini upgrade head

# Check current revision
python -m alembic -c alembic.ini current

# Generate a new migration after model changes
python -m alembic -c alembic.ini revision --autogenerate -m "description"
```

After generating a migration, inspect the file in `db/migrations/versions/` before
committing — autogenerate is not always correct.

## Running tests locally

`PAPER_TRADER_TEST_DATABASE_URL` must be set for database-backed tests. Alembic reads
`PAPER_TRADER_DATABASE_URL`, not `PAPER_TRADER_TEST_DATABASE_URL`, so temporarily point
`PAPER_TRADER_DATABASE_URL` at the test database for the migration step.

**Bash / macOS / Linux:**

```bash
export PAPER_TRADER_DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/paper_trader_test
python -m alembic -c alembic.ini upgrade head
export PAPER_TRADER_TEST_DATABASE_URL=postgresql+psycopg2://user:password@localhost:5432/paper_trader_test
python -m pytest tests/ -v
```

**Windows PowerShell:**

```powershell
$env:PAPER_TRADER_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/paper_trader_test"
python -m alembic -c alembic.ini upgrade head
$env:PAPER_TRADER_TEST_DATABASE_URL = "postgresql+psycopg2://user:password@localhost:5432/paper_trader_test"
python -m pytest tests/ -v
```

Tests not requiring a database run even without `PAPER_TRADER_TEST_DATABASE_URL`.

## Running the API locally

Set `PYTHONPATH` first (see [Repo layout caveat](#repo-layout-caveat)). `scripts/seed.py`
only needs to run once to seed starting capital.

**Bash / macOS / Linux:**

```bash
python scripts/seed.py
python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001
```

**Windows PowerShell:**

```powershell
python scripts/seed.py
python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001
```

Confirm the server is up:

```bash
curl -s http://127.0.0.1:8001/v1/health
curl -s http://127.0.0.1:8001/v1/ready
```

On Windows PowerShell, use `curl.exe` instead of `curl`.

## Before pushing

CI runs on every push and pull request. Before pushing:

1. All tests must pass: `python -m pytest tests/ -v`
2. No new deprecation warnings — `pyproject.toml` promotes unfiltered warnings to errors
   in the test suite.
3. If you added or changed models, ensure a migration file is committed and
   `alembic upgrade head` applies cleanly against a fresh database.

## Workflow file changes

Direct HTTPS pushes that modify `.github/workflows/*` require a GitHub token with the
`workflow` scope. Without it, Git will reject the push.

Fork contributors may propose workflow changes via pull request; those are reviewed and
merged by a maintainer with the required permissions.

If you change action versions in the workflow, verify that the exact pinned tag
(e.g., `actions/checkout@v6.0.2`) exists in the upstream action repository before
committing — a non-existent tag causes an immediate CI failure with no useful error message.
