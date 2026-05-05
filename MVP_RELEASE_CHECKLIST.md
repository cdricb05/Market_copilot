# MVP Release Checklist — paper_trader v0.1.0

Work through each section top to bottom before tagging the release.
Check every box; do not skip sections.

---

## 1. Release Readiness

- [ ] `main` is the intended release branch: `git branch --show-current`
- [ ] No uncommitted changes: `git status` is clean
- [ ] `pyproject.toml` version is `0.1.0` — no pending version bump needed
- [ ] All known non-blocking limitations reviewed and accepted (see §9)

---

## 2. Local Verification

Run from the repo root with the virtualenv active
(`PYTHONPATH` must include the parent of this directory — see CONTRIBUTING.md).

- [ ] `pip install -e ".[test]"` completes without errors or conflicts
- [ ] `python -m pytest tests/ -v` — zero failures, zero unexpected warnings
- [ ] `python scripts/seed.py` succeeds against the dev database
- [ ] `python -m uvicorn paper_trader.api.app:app --host 127.0.0.1 --port 8001`
      starts cleanly with no import errors

---

## 3. CI Verification

- [ ] GitHub Actions CI badge on `main` is green
- [ ] Latest workflow run on `main` shows all four steps green:
      Install dependencies → Run database migrations → Run tests
- [ ] No flaky failures in the last 3 workflow runs on `main`

---

## 4. Migration Verification

Test against a **fresh, empty** database (not your dev or test database).
Temporarily set `PAPER_TRADER_DATABASE_URL` to point at the fresh DB.

- [ ] `python -m alembic -c alembic.ini upgrade head` applies cleanly from zero
- [ ] `python -m alembic -c alembic.ini current` reports `0001_initial_schema (head)`
- [ ] `python -m alembic -c alembic.ini downgrade base` succeeds without errors
- [ ] Re-running `upgrade head` after downgrade succeeds (round-trip clean)

---

## 5. API Smoke Tests

Start the server (§2, step 4) and run these against it.
Replace `change-me-before-use` with the actual value of
`PAPER_TRADER_SERVICE_API_KEY` in your `.env`.

**No auth required:**
- [ ] `GET /v1/health` → `{"status":"ok","service":"paper_trader","version":"1.0.0"}`
- [ ] `GET /v1/ready` → `{"status":"ok",...,"database":"ok"}` (not 503)

**Auth required:**
- [ ] `GET /v1/portfolio` → 200 with non-zero cash (confirms seed ran)
- [ ] `POST /v1/prices` with `{"snapshots":[{"ticker":"AAPL","price":"182.50"}]}` → 200
- [ ] `POST /v1/benchmark-prices` with `{"prices":[{"ticker":"SPY","price":"510.00"}]}` → 200
- [ ] `POST /v1/signals` with valid payload and `confidence >= 0.55` → 200
      _(run on a weekday; the endpoint returns 422 whenever the server's current
      US/Eastern date is a weekend, regardless of payload content — that is expected
      behavior, not a bug)_
- [ ] `GET /v1/orders?status=PENDING` → 200, valid list (may be empty)
- [ ] `POST /v1/fill` with a fresh `idempotency_key` → 200
- [ ] `POST /v1/snapshot` → 200, or 422 if price data is missing (both correct)
- [ ] `GET /v1/positions` → 200
- [ ] `GET /v1/snapshots` → 200, valid list
- [ ] `GET /v1/performance` → 200 or 404 if no snapshots exist (both correct)
- [ ] `GET /v1/performance/history` → 200 or 404 if no history exists (both correct)
- [ ] `GET /v1/performance/history.csv` → 200 with CSV download, or 404 if no
      history exists (both correct)
- [ ] Any request with a wrong API key → 401

---

## 6. Documentation Sanity

- [ ] `README.md` CI badge URL matches the actual GitHub repo path
      (`cdricb05/Market_copilot/actions/workflows/ci.yml`)
- [ ] Every endpoint in `README.md` exists as a route in `api/app.py`
- [ ] `.env.example` contains every variable listed in `README.md`'s
      Environment Variables section
- [ ] `CONTRIBUTING.md` migration command (`-c alembic.ini`) matches `alembic.ini`
      location at the repo root
- [ ] No placeholder text remaining in docs
      (e.g. `your-username`, `your-database`, `TODO`)

---

## 7. Git / GitHub Release Steps

Run these only after §1–§6 are all checked.

- [ ] `git tag -a v0.1.0 -m "MVP release v0.1.0"`
- [ ] `git push origin v0.1.0`
- [ ] Create a GitHub Release from tag `v0.1.0`:
  - Title: `v0.1.0 — Initial MVP`
  - Body: one-paragraph summary of what the system does
  - Attach no build artifacts (pure Python, no binary distribution)

---

## 8. Post-Push Verification

- [ ] CI workflow triggered by the tag push completes green
- [ ] GitHub Release page is visible and the tag resolves to the correct commit
- [ ] CI badge on `main` is still green after the tag push

---

## 9. Known Non-Blocking Limitations (Accepted for MVP)

These do not block the release. Log them for the next iteration.

| # | Location | Issue |
|---|---|---|
| 1 | `tests/conftest.py` | `Session(bind=connection)` is a SQLAlchemy 2.0 legacy pattern; will break on a future SQLAlchemy major version upgrade |
| 2 | `api/app.py` fill endpoint | `ValueError` maps to HTTP 500; worth auditing against reconciler error conditions |
| 3 | `tests/test_api.py` | Test class ordering is held by fixture dependencies, not enforced by pytest; fragile under reordering |
| 4 | `pyproject.toml` vs `app.py` | Package version (`0.1.0`) and API version (`1.0.0`) are decoupled with no documentation explaining the distinction |
