"""
tests/test_current_alpha_tournament_endpoint.py — Phase 18 tournament routes.

Contract tests for:
    GET  /v1/research/current-alpha/tournament            (read-only)
    POST /v1/research/current-alpha/tournament/refresh    (manual; commit needs confirmation)

Fully offline: a synthetic Phase 18-A forward report is written to a tmp forward dir and a tmp
dedicated tournament store is used, both pointed at via the env overrides. Verifies auth, the
read-only GET, that a committing POST without the confirmation token is rejected (400), that a
confirmed POST writes only the local store and is idempotent, that the GET performs no writes,
carries the no-live-trading block, and that GET is GET-only (405 on POST to the GET route).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.api.current_alpha_tournament import _STATE_FILE
from paper_trader.api.current_alpha_tournament_sync import _SYNC_STATE_FILE

from tests.test_current_alpha_tournament import _write_forward_report
from tests.test_current_alpha_tournament_sync import (
    _write_champion_pkg, _write_challenger_pkg, _price_table)

_GET = "/v1/research/current-alpha/tournament"
_POST = "/v1/research/current-alpha/tournament/refresh"
_KEY = "current-alpha-tournament-test-key"
_AUTH = {"X-API-Key": _KEY}


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    fdir = tmp_path / "forward"
    tdir = tmp_path / "store"
    _write_forward_report(fdir)
    # Phase 19: the POST now performs a real (offline, fixture-backed) data sync, so the frozen
    # champion / challenger packages and an offline price fixture are wired via env seams.
    champ = tmp_path / "champ"
    chall = tmp_path / "chall"
    _write_champion_pkg(champ)
    _write_challenger_pkg(chall)
    fixture = tmp_path / "bars.json"
    fixture.write_text(json.dumps(_price_table()), encoding="utf-8")
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_FORWARD_DIR", str(fdir))
    monkeypatch.setenv("PAPER_TRADER_CURRENT_ALPHA_TOURNAMENT_DIR", str(tdir))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_CHAMPION_PKG_DIR", str(champ))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_CHALLENGER_PKG_DIR", str(chall))
    monkeypatch.setenv("PAPER_TRADER_TOURNAMENT_SYNC_FIXTURE", str(fixture))
    get_settings.cache_clear()
    with TestClient(app) as test_client:
        test_client._tdir = tdir  # type: ignore[attr-defined]
        yield test_client


def test_get_requires_api_key(client: TestClient):
    assert client.get(_GET).status_code in (401, 403)


def test_post_requires_api_key(client: TestClient):
    assert client.post(_POST).status_code in (401, 403)


def test_get_returns_controlled_200(client: TestClient):
    resp = client.get(_GET, headers=_AUTH)
    assert resp.status_code == 200
    body = resp.json()
    assert body["phase"] == "18"
    assert body["decision"] == "MONITORING_MID_CYCLE"
    assert body["current_paper_champion"]["signal"] == "composite_sn"
    assert body["sector_repaired_paper_challenger"]["signal"] == "composite_sn_repaired"
    assert set(body["book_summaries"]) == {"champion_top25", "challenger_top25",
                                           "champion_top50", "challenger_top50"}


def test_get_is_read_only_no_store_written(client: TestClient):
    client.get(_GET, headers=_AUTH)
    assert not (client._tdir / _STATE_FILE).exists()  # type: ignore[attr-defined]


def test_get_no_live_trading_block(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["champion_replaced"] is False
    assert body["wrote_to_database"] is False and body["creates_orders"] is False


def test_get_route_is_get_only(client: TestClient):
    assert client.post(_GET, headers=_AUTH).status_code == 405
    assert client.put(_GET, headers=_AUTH).status_code == 405
    assert client.delete(_GET, headers=_AUTH).status_code == 405


def test_post_preview_writes_nothing(client: TestClient):
    resp = client.post(_POST, headers=_AUTH, json={"commit": False})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "TOURNAMENT_SYNC_PREVIEW"
    assert body["wrote_store"] is False
    assert body["performed_provider_call"] is False
    assert not (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_post_commit_without_confirmation_is_rejected(client: TestClient):
    resp = client.post(_POST, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 400
    assert not (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_post_commit_with_confirmation_writes_store_and_is_idempotent(client: TestClient):
    resp = client.post(_POST, headers=_AUTH,
                       json={"commit": True, "confirm": "RUN_MANUAL_TOURNAMENT_REFRESH"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "TOURNAMENT_REFRESH_COMPLETE"
    assert body["wrote_store"] is True
    assert body["wrote_to_database"] is False
    assert (client._tdir / _SYNC_STATE_FILE).exists()  # type: ignore[attr-defined]
    # rerun -> idempotent (no newer completed date)
    resp2 = client.post(_POST, headers=_AUTH,
                        json={"commit": True, "confirm": "RUN_MANUAL_TOURNAMENT_REFRESH"})
    assert resp2.json()["status"] == "NO_NEW_COMPLETED_EOD_DATE"


def test_no_new_write_routes_beyond_tournament_refresh():
    app_src = (Path(__file__).parent.parent / "api" / "app.py").read_text(encoding="utf-8")
    # the GET route is declared with @app.get
    gi = app_src.index('"/v1/research/current-alpha/tournament"')
    assert "@app.get(" in app_src[gi - 200:gi]
    # the only POST route is the explicit /refresh
    pi = app_src.index('"/v1/research/current-alpha/tournament/refresh"')
    assert "@app.post(" in app_src[pi - 200:pi]
