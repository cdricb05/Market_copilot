"""
tests/test_daily_operating_run_endpoint.py — Phase 15-A daily-run routes.

DB-backed contract tests for the one-click daily operating run:

    GET  /v1/operations/daily-run/status
    POST /v1/operations/daily-run/preview
    POST /v1/operations/daily-run/execute

The provider (owned EOD fetch), the current-alpha runner and the alpha status
loader are ALL stubbed via monkeypatch on the daily_operating_run module — no
Yahoo call, no EODHD key, no research subprocess, no prediction call. The tests
prove: preview writes nothing; execute needs the exact confirmation; the deduped
owned/SPY universe; per-ticker failure tolerance; PriceSnapshot / BenchmarkPrice
/ PortfolioSnapshot / alpha idempotency; a same-date rerun is ALREADY_COMPLETE;
no positions / cash / signals / decisions / orders / fills change; before/after
counts; alignment + mixed-date detection; controlled provider failure; the
read-only status endpoint; and the CROSS-ENDPOINT invariant that after a stubbed
run the portfolio-valuation, portfolio-terminal, command-center and daily-workflow
market dates plus the alpha Top25/Top50/SPY marks all align.

Every write-path test restores the stale baseline in a finally block, so the
module's read-only "misaligned" tests are order-independent.

Skipped entirely without PAPER_TRADER_TEST_DATABASE_URL.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import Session

from paper_trader.api import daily_operating_run as dor
from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.constants import CashEntryType
from paper_trader.db.models import (
    Base, BenchmarkPrice, CashLedger, JobRun, Order, Portfolio, PortfolioSnapshot,
    Position, PriceSnapshot, Signal, Trade, TradeDecision,
)
from paper_trader.db.session import reset_engine_state
from paper_trader.engine.portfolio import append_cash_entry

_STATUS = "/v1/operations/daily-run/status"
_PREVIEW = "/v1/operations/daily-run/preview"
_EXECUTE = "/v1/operations/daily-run/execute"
_VAL = "/v1/dashboard/portfolio-valuation"
_CC = "/v1/dashboard/command-center"
_PT = "/v1/dashboard/portfolio-terminal"
_DW = "/v1/dashboard/daily-workflow"

_TEST_API_KEY = "daily-operating-run-test-key"
_AUTH = {"X-API-Key": _TEST_API_KEY}
_NOW = datetime(2026, 7, 16, 20, 0, 0, tzinfo=timezone.utc)
_STALE = date(2026, 5, 1)
_CONFIRM = {"confirmation": "RUN_MANUAL_DAILY_OPERATING_SESSION"}


def _required() -> str:
    """Latest completed market date resolved from the real clock (as the app does)."""
    return dor.latest_completed_market_date(datetime.now(tz=timezone.utc)).isoformat()


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def api_engine():
    url = os.environ.get("PAPER_TRADER_TEST_DATABASE_URL")
    if not url:
        pytest.skip("PAPER_TRADER_TEST_DATABASE_URL not set — skipping daily-run endpoint tests.")
    engine = create_engine(url, pool_pre_ping=True)
    Base.metadata.create_all(engine)
    yield engine
    try:
        with engine.begin() as conn:
            for table in reversed(Base.metadata.sorted_tables):
                conn.execute(table.delete())
    finally:
        engine.dispose()


@pytest.fixture(scope="module")
def client(api_engine):
    db_url = api_engine.url.render_as_string(hide_password=False)
    os.environ["PAPER_TRADER_DATABASE_URL"] = db_url
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = _TEST_API_KEY
    get_settings.cache_clear()
    reset_engine_state()
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        get_settings.cache_clear()
        reset_engine_state()


@pytest.fixture(scope="module")
def seeded_client(client, api_engine):
    with Session(api_engine, autoflush=False, expire_on_commit=False) as session:
        if session.query(Portfolio).first() is None:
            portfolio = Portfolio(
                inception_date=_STALE, initial_capital=Decimal("10000.00"),
                strategy_enabled=True, trading_enabled=True, allow_new_positions=True,
                config={"max_positions": 5},
                cached_cash=Decimal("10000.00"), cached_total_value=Decimal("10000.00"),
                cached_as_of_ts=_NOW,
            )
            session.add(portfolio)
            session.flush()
            append_cash_entry(
                session, portfolio_id=portfolio.id,
                entry_type=CashEntryType.INITIAL_CAPITAL, amount=Decimal("10000.00"),
                description="Daily-run test initial capital")
            session.add(Position(ticker="AAA", qty=Decimal("10"), avg_cost=Decimal("100.00"),
                                 cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW))
            session.add(Position(ticker="BBB", qty=Decimal("5"), avg_cost=Decimal("200.00"),
                                 cost_basis=Decimal("1000.00"), opened_at=_NOW, last_updated=_NOW))
            # STALE prices (an old completed date) so the baseline state is misaligned.
            for t, px in (("AAA", "90.000000"), ("BBB", "180.000000")):
                session.add(PriceSnapshot(
                    ticker=t, price=Decimal(px), session_type="REGULAR", price_type="CLOSE",
                    data_source="seed", snapshot_ts=datetime(2026, 5, 1, 20, tzinfo=timezone.utc),
                    market_date=_STALE, job_run_id=None))
            session.commit()
    yield client


def _reset_run_state(api_engine, required: str):
    """Delete everything a run may have written for ``required`` — restores the
    stale baseline so the module's tests are order-independent."""
    req = date.fromisoformat(required)
    with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
        s.query(PortfolioSnapshot).filter(PortfolioSnapshot.market_date == req).delete()
        s.query(PriceSnapshot).filter(PriceSnapshot.market_date == req).delete()
        s.query(BenchmarkPrice).filter(BenchmarkPrice.market_date == req).delete()
        s.query(JobRun).filter(JobRun.market_date == req).delete()
        s.commit()


# --------------------------------------------------------------------------- #
# Stub helpers (no live provider / subprocess / prediction)
# --------------------------------------------------------------------------- #

def _make_fetcher(fail=()):
    fail = {t.upper() for t in fail}

    def _fetch(tickers):
        ok, bad = [], []
        for t in tickers:
            tt = str(t).upper()
            if tt in fail:
                bad.append({"ticker": tt, "reason": "stub failure"})
            else:
                ok.append({"ticker": tt, "price": "150.00"})
        return ok, bad
    return _fetch


def _make_alpha_runner(required, counter):
    def _run(commit=False, **kw):
        counter["n"] += 1
        if commit and counter["n"] == 1:
            return {
                "status": "DAILY_REFRESH_COMPLETE", "action": "SNAPSHOTS_WRITTEN",
                "committed": True, "refresh_result": "DAILY_REFRESH_COMPLETE",
                "mark_date": required, "latest_valid_mark_date": required,
                "snapshots": {"top25": {"status": "SNAPSHOT_WRITTEN"},
                              "top50": {"status": "SNAPSHOT_WRITTEN"}},
            }
        # Subsequent same-date runs advance nothing (mirrors the real dedup).
        return {"status": "NO_NEW_MARK_DATE", "action": "NO_SNAPSHOT",
                "refresh_result": "NO_NEW_MARK_DATE", "snapshots": {},
                "latest_valid_mark_date": required}
    return _run


def _alpha_status(required, *, blocked=False, available=True):
    def _status(**kw):
        return {
            "status": "DAILY_STATUS_READY",
            "latest_valid_mark_available": available,
            "latest_valid_mark_date": required if available else None,
            "latest_valid_mark_freshness": "FRESH_MARK",
            "last_run_result": "DAILY_REFRESH_COMPLETE",
            "last_run_blocked": blocked,
            "last_run_at": _NOW.isoformat(),
        }
    return _status


def _stub_all(monkeypatch, required, *, fail=(), counter=None, blocked=False):
    counter = counter if counter is not None else {"n": 0}
    monkeypatch.setattr(dor, "fetch_latest_prices", _make_fetcher(fail))
    monkeypatch.setattr(dor, "_champion_tickers", lambda *a, **k: ([], [], None))
    monkeypatch.setattr(dor, "run_current_alpha_daily_refresh",
                        _make_alpha_runner(required, counter))
    monkeypatch.setattr(dor, "load_current_alpha_daily_status",
                        _alpha_status(required, blocked=blocked))
    return counter


def _counts(api_engine):
    with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
        return {
            "price_snapshots": s.query(PriceSnapshot).count(),
            "benchmark_prices": s.query(BenchmarkPrice).count(),
            "portfolio_snapshots": s.query(PortfolioSnapshot).count(),
            "job_runs": s.query(JobRun).count(),
            "positions": s.query(Position).count(),
            "signals": s.query(Signal).count(),
            "trade_decisions": s.query(TradeDecision).count(),
            "orders": s.query(Order).count(),
            "trades": s.query(Trade).count(),
            "cash_sum": s.execute(select(func.coalesce(func.sum(CashLedger.amount), 0))).scalar(),
        }


# --------------------------------------------------------------------------- #
# Auth + wiring + shape
# --------------------------------------------------------------------------- #

def test_status_requires_api_key(seeded_client):
    assert seeded_client.get(_STATUS).status_code in (401, 403)


def test_execute_requires_api_key(seeded_client):
    assert seeded_client.post(_EXECUTE, json=_CONFIRM).status_code in (401, 403)


def test_app_wires_daily_run_routes():
    from pathlib import Path
    src = (Path(__file__).resolve().parents[1] / "api" / "app.py").read_text(encoding="utf-8")
    assert '"/v1/operations/daily-run/status"' in src
    assert '"/v1/operations/daily-run/preview"' in src
    assert '"/v1/operations/daily-run/execute"' in src


def test_status_shape_read_only(seeded_client):
    body = seeded_client.get(_STATUS, headers=_AUTH).json()
    for k in ("status", "required_market_date", "alignment", "alpha", "blockers",
              "safety", "provenance"):
        assert k in body
    assert body["provenance"]["read_only"] is True
    assert body["provenance"]["wrote_to_database"] is False
    assert body["safety"]["prediction_checked"] is False


# --------------------------------------------------------------------------- #
# Read-only "misaligned" state (stale baseline — order-independent)
# --------------------------------------------------------------------------- #

def test_status_detects_mixed_dates(seeded_client, monkeypatch):
    # Portfolio price is stale (2026-05-01) but alpha reports a fresh mark.
    monkeypatch.setattr(dor, "load_current_alpha_daily_status",
                        _alpha_status(_required()))
    body = seeded_client.get(_STATUS, headers=_AUTH).json()
    align = body["alignment"]
    assert align["aligned"] is False
    assert body["status"] in ("STALE", "PARTIAL_COVERAGE")
    ds = {m["dataset"] for m in align["mismatches"]}
    assert "price_snapshot_market_date" in ds


def test_daily_workflow_data_stage_stale_when_misaligned(seeded_client, monkeypatch):
    # Fresh alpha but stale portfolio price -> DATA stage NEEDS_ACTION, Preview Daily Run.
    monkeypatch.setattr(dor, "load_current_alpha_daily_status", _alpha_status(_required()))
    dw = seeded_client.get(_DW, headers=_AUTH).json()
    data_stage = next(s for s in dw["stages"] if s["stage"] == "DATA")
    assert data_stage["status"] in ("NEEDS_ACTION", "BLOCKED")
    assert data_stage["action_label"] == "Preview Daily Run"
    assert data_stage["action_target"] == "command-center"


# --------------------------------------------------------------------------- #
# Preview writes nothing
# --------------------------------------------------------------------------- #

def test_preview_writes_nothing(seeded_client, api_engine, monkeypatch):
    _stub_all(monkeypatch, _required())
    before = _counts(api_engine)
    body = seeded_client.post(_PREVIEW, headers=_AUTH).json()
    assert body["mode"] == "PREVIEW"
    assert body["execute"] is False
    assert body["provenance"]["wrote_to_database"] is False
    assert all(s["status"] == "PLANNED" for s in body["stages"])
    assert _counts(api_engine) == before


def test_preview_reports_planned_stages(seeded_client, monkeypatch):
    _stub_all(monkeypatch, _required())
    body = seeded_client.post(_PREVIEW, headers=_AUTH).json()
    stages = [s["stage"] for s in body["stages"]]
    assert stages == dor.STAGE_ORDER
    assert body["status"] in ("PREVIEW_WOULD_ALIGN", "ALREADY_COMPLETE")


# --------------------------------------------------------------------------- #
# Execute confirmation gate
# --------------------------------------------------------------------------- #

def test_execute_rejects_missing_confirmation(seeded_client):
    assert seeded_client.post(_EXECUTE, headers=_AUTH, json={}).status_code == 422


def test_execute_rejects_wrong_confirmation(seeded_client):
    resp = seeded_client.post(_EXECUTE, headers=_AUTH, json={"confirmation": "yes"})
    assert resp.status_code == 400


# --------------------------------------------------------------------------- #
# Execute happy path + idempotency + no forbidden side effects
# --------------------------------------------------------------------------- #

def test_execute_aligns_and_is_idempotent(seeded_client, api_engine, monkeypatch):
    required = _required()
    counter = _stub_all(monkeypatch, required)
    before = _counts(api_engine)
    try:
        # --- first execute: creates prices + snapshot + alpha, aligns -------
        body = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM).json()
        assert body["mode"] == "EXECUTE" and body["execute"] is True
        assert body["required_market_date"] == required
        assert body["status"] == "ALIGNED"
        assert body["alignment"]["aligned"] is True
        assert body["provenance"]["wrote_to_database"] is True
        st = {s["stage"]: s["status"] for s in body["stages"]}
        assert st["PRICE_REFRESH"] == "CREATED"
        assert st["PORTFOLIO_SNAPSHOT"] == "CREATED"
        assert st["ALPHA_MARK"] == "CREATED"
        assert "ALL OPERATING DATA ALIGNED" in body["final_outcome"]

        after1 = _counts(api_engine)
        assert after1["price_snapshots"] > before["price_snapshots"]
        assert after1["benchmark_prices"] == before["benchmark_prices"] + 1
        assert after1["portfolio_snapshots"] == before["portfolio_snapshots"] + 1
        assert after1["job_runs"] > before["job_runs"]
        for guarded in ("positions", "signals", "trade_decisions", "orders", "trades"):
            assert after1[guarded] == before[guarded], guarded
        assert after1["cash_sum"] == before["cash_sum"]

        # --- second execute (same date): ALREADY_COMPLETE, no new rows ------
        body2 = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM).json()
        assert body2["status"] == "ALREADY_COMPLETE"
        st2 = {s["stage"]: s["status"] for s in body2["stages"]}
        assert st2["PRICE_REFRESH"] == "REUSED"
        assert st2["PORTFOLIO_SNAPSHOT"] == "REUSED"
        assert st2["ALPHA_MARK"] == "REUSED"

        after2 = _counts(api_engine)
        assert after2["price_snapshots"] == after1["price_snapshots"]
        assert after2["benchmark_prices"] == after1["benchmark_prices"]
        assert after2["portfolio_snapshots"] == after1["portfolio_snapshots"]
        assert after2["job_runs"] == after1["job_runs"]  # run-audit reused, no new row
        for guarded in ("positions", "signals", "trade_decisions", "orders", "trades"):
            assert after2[guarded] == before[guarded], guarded
        assert after2["cash_sum"] == before["cash_sum"]
        assert counter["n"] == 2
    finally:
        _reset_run_state(api_engine, required)


def test_execute_reports_before_after_row_counts(seeded_client, api_engine, monkeypatch):
    required = _required()
    _stub_all(monkeypatch, required)
    try:
        body = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM).json()
        rc = body["row_counts"]
        for t in ("price_snapshots", "benchmark_prices", "portfolio_snapshots",
                  "job_runs", "positions"):
            assert t in rc and "before" in rc[t] and "after" in rc[t]
        assert rc["positions"]["before"] == rc["positions"]["after"]  # guarded
    finally:
        _reset_run_state(api_engine, required)


# --------------------------------------------------------------------------- #
# Partial per-ticker failure -> coverage + snapshot blocked
# --------------------------------------------------------------------------- #

def test_partial_ticker_failure(seeded_client, api_engine, monkeypatch):
    required = _required()
    # A third position with NO price whose fetch fails -> partial coverage + blocked snapshot.
    with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
        s.add(Position(ticker="CCC", qty=Decimal("2"), avg_cost=Decimal("300.00"),
                       cost_basis=Decimal("600.00"), opened_at=_NOW, last_updated=_NOW))
        s.commit()
    try:
        _stub_all(monkeypatch, required, fail=("CCC",))
        before = _counts(api_engine)
        body = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM).json()
        assert body["status"] in ("PARTIAL_COVERAGE", "STALE")
        pu = body["price_universe"]
        assert any(f["ticker"] == "CCC" for f in pu["failed_tickers"])
        assert pu["failed_ticker_count"] >= 1
        st = {s2["stage"]: s2["status"] for s2 in body["stages"]}
        assert st["PORTFOLIO_SNAPSHOT"] == "BLOCKED"
        after = _counts(api_engine)
        for guarded in ("positions", "signals", "trade_decisions", "orders", "trades"):
            assert after[guarded] == before[guarded], guarded
        assert after["cash_sum"] == before["cash_sum"]
    finally:
        with Session(api_engine, autoflush=False, expire_on_commit=False) as s:
            s.query(Position).filter(Position.ticker == "CCC").delete()
            s.commit()
        _reset_run_state(api_engine, required)


def test_controlled_provider_failure(seeded_client, api_engine, monkeypatch):
    required = _required()

    def _boom(tickers):
        raise RuntimeError("provider down")
    monkeypatch.setattr(dor, "fetch_latest_prices", _boom)
    monkeypatch.setattr(dor, "_champion_tickers", lambda *a, **k: ([], [], None))
    monkeypatch.setattr(dor, "run_current_alpha_daily_refresh",
                        _make_alpha_runner(required, {"n": 0}))
    monkeypatch.setattr(dor, "load_current_alpha_daily_status", _alpha_status(required))
    try:
        resp = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM)
        assert resp.status_code == 200
        body = resp.json()
        st = {s["stage"]: s["status"] for s in body["stages"]}
        assert st["PRICE_REFRESH"] == "FAILED"
        assert any("Price refresh failed" in w for w in body["warnings"])
    finally:
        _reset_run_state(api_engine, required)


# --------------------------------------------------------------------------- #
# Cross-endpoint alignment after a stubbed run
# --------------------------------------------------------------------------- #

def test_cross_endpoint_alignment_after_run(seeded_client, api_engine, monkeypatch):
    required = _required()
    _stub_all(monkeypatch, required)
    try:
        run = seeded_client.post(_EXECUTE, headers=_AUTH, json=_CONFIRM).json()
        assert run["status"] in ("ALIGNED", "ALREADY_COMPLETE")

        val = seeded_client.get(_VAL, headers=_AUTH).json()["current_mark"]["as_of_market_date"]
        term = seeded_client.get(_PT, headers=_AUTH).json()["current_mark"]["as_of_market_date"]
        ccpf = seeded_client.get(_CC, headers=_AUTH).json()
        dw = seeded_client.get(_DW, headers=_AUTH).json()
        st = seeded_client.get(_STATUS, headers=_AUTH).json()

        # DB-backed portfolio dates all equal the required completed date.
        assert val == term == ccpf["portfolio"]["as_of_market_date"] == required

        # Command Center market_data slice aligns, alpha marks equal required.
        md = ccpf["market_data"]
        assert md["aligned"] is True
        assert md["alpha_top25_market_date"] == md["alpha_top50_market_date"] == required
        assert md["spy_market_date"] == required

        # Daily Workflow DATA stage reflects the aligned completed date.
        assert dw["market_data"]["required_market_date"] == required
        data_stage = next(s for s in dw["stages"] if s["stage"] == "DATA")
        assert data_stage["status"] == "COMPLETE"

        # Status endpoint agrees.
        assert st["alignment"]["aligned"] is True
        assert st["alignment"]["alpha_top25_market_date"] == required
    finally:
        _reset_run_state(api_engine, required)
