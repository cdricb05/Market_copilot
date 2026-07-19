"""
tests/test_current_alpha_tournament_sync_endpoint.py — Phase 19 tournament data-sync routes.

Exercises the upgraded manual data sync + the alignment block through the real FastAPI app,
fully offline: synthetic champion / challenger packages, an offline price fixture (the
``PAPER_TRADER_TOURNAMENT_SYNC_FIXTURE`` seam — never network, never a key) and a synthetic
system-mark manifest are wired via env overrides. Verifies auth, that a preview performs no
provider call and no write, that a committing sync requires the confirmation token (400
otherwise), that a confirmed sync advances the dedicated LOCAL store to the completed common
date and is idempotent, that the read-only GET carries the alignment block (STALE before the
sync, ALIGNED after) with refreshed coverage and zero unresolved tickers, that the GET writes
nothing and is GET-only, that no status approves live trading, and that the EODHD key never
leaks into a response.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.api.current_alpha_tournament_sync import _SYNC_STATE_FILE

from tests.test_current_alpha_tournament import _write_forward_report
from tests.test_current_alpha_tournament_sync import (
    _write_champion_pkg, _write_challenger_pkg, _price_table, DATES, UNION)

_GET = "/v1/research/current-alpha/tournament"
_POST = "/v1/research/current-alpha/tournament/refresh"
_TOKEN = "RUN_MANUAL_TOURNAMENT_REFRESH"
_KEY = "tournament-sync-test-key"
_AUTH = {"X-API-Key": _KEY}
_SYSTEM_MARK = DATES[-1]  # the system market mark the sync will align to


def _write_system_mark(mark_dir: Path, mark: str):
    (mark_dir / "latest").mkdir(parents=True, exist_ok=True)
    (mark_dir / "latest" / "refresh_manifest.json").write_text(
        json.dumps({"mark_date": mark, "blocked": False}), encoding="utf-8")


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    fdir = tmp_path / "forward"
    tdir = tmp_path / "store"
    sysd = tmp_path / "sysmark"
    champ = tmp_path / "champ"
    chall = tmp_path / "chall"
    # pre-sync tournament mark (static report) is BEFORE the system mark -> STALE
    _write_forward_report(fdir, latest="2026-05-26", elapsed=3)
    _write_champion_pkg(champ)
    _write_challenger_pkg(chall)
    _write_system_mark(sysd, _SYSTEM_MARK)
    fixture = tmp_path / "bars.json"
    fixture.write_text(json.dumps(_price_table()), encoding="utf-8")
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_FORWARD_DIR", str(fdir))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_DIR", str(tdir))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_DAILY_MARK_DIR", str(sysd))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_CHAMPION_PKG_DIR", str(champ))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_CHALLENGER_PKG_DIR", str(chall))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_SYNC_FIXTURE", str(fixture))
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        test_client._tdir = tdir  # type: ignore[attr-defined]
        yield test_client


def test_auth_required(client: TestClient):
    assert client.get(_GET).status_code in (401, 403)
    assert client.post(_POST).status_code in (401, 403)


def test_get_carries_alignment_stale_before_sync(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    al = body["alignment"]
    assert al["tournament_alignment"] == "STALE"
    assert al["latest_system_market_mark"] == _SYSTEM_MARK
    assert al["latest_tournament_common_mark"] == "2026-05-26"
    assert body["tournament_alignment"] == "STALE"
    assert body["synced_tournament"]["available"] is False
    assert body["current_view_source"] == "STATIC_FORWARD_REPORT"


def test_preview_no_provider_call_no_write(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": False}).json()
    assert body["status"] == "TOURNAMENT_SYNC_PREVIEW"
    assert body["performed_provider_call"] is False
    assert body["wrote_store"] is False
    assert body["union"]["union_size"] == len(UNION)
    assert not (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_commit_without_confirmation_is_400(client: TestClient):
    resp = client.post(_POST, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 400
    assert not (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_confirmed_sync_advances_store_and_is_idempotent(client: TestClient):
    body = client.post(_POST, headers=_AUTH,
                       json={"commit": True, "confirm": _TOKEN}).json()
    assert body["status"] == "TOURNAMENT_REFRESH_COMPLETE"
    assert body["wrote_store"] is True and body["wrote_to_database"] is False
    assert body["latest_tournament_common_mark"] == _SYSTEM_MARK
    assert body["n_tickers_ok"] == len(UNION) + 1 and body["n_tickers_failed"] == 0
    assert (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]
    # idempotent rerun
    body2 = client.post(_POST, headers=_AUTH,
                        json={"commit": True, "confirm": _TOKEN}).json()
    assert body2["status"] == "NO_NEW_COMPLETED_EOD_DATE"
    assert body2["wrote_store"] is False


def test_get_aligned_after_full_sync(client: TestClient):
    client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN})
    body = client.get(_GET, headers=_AUTH).json()
    al = body["alignment"]
    assert al["tournament_alignment"] == "ALIGNED"
    assert al["latest_tournament_common_mark"] == _SYSTEM_MARK
    assert al["mark_date_delta"] == 0
    assert al["unresolved_ticker_count"] == 0
    for k in ("champion_top25", "challenger_top25", "champion_top50", "challenger_top50"):
        assert al["four_book_coverage"][k]["coverage_pct"] == 100.0
    assert body["synced_tournament"]["available"] is True
    assert body["current_view_source"] == "SYNCED_LOCAL_STORE"


def test_get_is_read_only_no_store_written(client: TestClient):
    client.get(_GET, headers=_AUTH)
    assert not (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_get_no_live_trading_block(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["champion_replaced"] is False and body["wrote_to_database"] is False


def test_get_route_is_get_only(client: TestClient):
    assert client.put(_GET, headers=_AUTH).status_code == 405
    assert client.delete(_GET, headers=_AUTH).status_code == 405


def test_sync_result_carries_no_live_block(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN}).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["champion_replaced"] is False and body["promotes_to_live"] is False
    assert body["calls_prediction_service"] is False


def test_no_eodhd_key_leak_in_response(client: TestClient, monkeypatch):
    monkeypatch.setenv("EODHD_API_KEY", "SECRET-ENDPOINT-KEY-987")
    resp = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN})
    assert "SECRET-ENDPOINT-KEY-987" not in resp.text


def test_decision_stays_monitoring_mid_cycle(client: TestClient):
    # a mid-cycle sync (few marks, 63-day horizon) never names a winner
    body = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN}).json()
    assert body["reconstructed"]["decision"] == "MONITORING_MID_CYCLE"
