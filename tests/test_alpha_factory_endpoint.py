"""
tests/test_alpha_factory_endpoint.py — Phase 20 Alpha Factory routes.

Contract tests for the read-only dashboard / registry / leaderboard / correlation GETs and the
manual, confirmation-gated build POST, exercised through the real FastAPI app, fully offline: a
synthetic owned-style panel and a synthetic committed Phase 17-A report are wired via env seams and
the dedicated store is redirected to a tmp dir. Verifies auth, the read-only GETs, that a preview
performs no write, that a committing build requires the confirmation token (400 otherwise), that a
confirmed build writes only the local store, that GET is GET-only, and that no status approves live
trading / replaces the champion / writes the database.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.api import alpha_factory as af

from tests.test_alpha_factory import _write_panel, _write_reval_report

_GET = "/v1/research/alpha-factory"
_POST = "/v1/research/alpha-factory/run"
_TOKEN = "RUN_ALPHA_FACTORY_BUILD"
_KEY = "alpha-factory-test-key"
_AUTH = {"X-API-Key": _KEY}


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    panel = tmp_path / "panel.csv"
    report = tmp_path / "reval.json"
    store = tmp_path / "store"
    _write_panel(panel)
    _write_reval_report(report)
    monkeypatch.setenv("PAPER_TRADER_ALPHA_FACTORY_PANEL", str(panel))
    monkeypatch.setenv("PAPER_TRADER_ALPHA_FACTORY_REVAL_REPORT", str(report))
    monkeypatch.setenv("PAPER_TRADER_ALPHA_FACTORY_DIR", str(store))
    af.clear_cache()
    get_settings.cache_clear()
    with TestClient(app) as c:
        c._store = store  # type: ignore[attr-defined]
        yield c
    af.clear_cache()


def test_auth_required(client: TestClient):
    assert client.get(_GET).status_code in (401, 403)
    assert client.post(_POST).status_code in (401, 403)


def test_get_dashboard_shape(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["phase"] == "20"
    assert body["status"] == "ALPHA_FACTORY_READY"
    assert body["champion"]["name"] == "composite_sn"
    assert body["challenger"]["name"] == "composite_sn_repaired"
    assert len(body["families"]) == 10
    assert body["registry"]["counts"]["total"] == 18
    assert body["correlation"]["signals"][0] == "composite_sn"


def test_get_carries_no_live_trading_block(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["replaces_champion"] is False
    assert body["wrote_to_database"] is False and body["creates_orders"] is False


def test_get_is_read_only_no_store_written(client: TestClient):
    client.get(_GET, headers=_AUTH)
    assert not (client._store / af._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_registry_leaderboard_correlation_slices(client: TestClient):
    reg = client.get(_GET + "/registry", headers=_AUTH).json()
    assert reg["registry"]["counts"]["total"] == 18
    assert len(reg["registry"]["schema"]) >= 18
    lb = client.get(_GET + "/leaderboard", headers=_AUTH).json()
    assert lb["leaderboard"][0]["is_champion"] is True
    co = client.get(_GET + "/correlation", headers=_AUTH).json()
    assert co["correlation"]["signals"][0] == "composite_sn"


def test_get_route_is_get_only(client: TestClient):
    assert client.put(_GET, headers=_AUTH).status_code == 405
    assert client.delete(_GET, headers=_AUTH).status_code == 405


def test_preview_no_write(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": False}).json()
    assert body["status"] == "ALPHA_FACTORY_BUILD_PREVIEW"
    assert body["wrote_store"] is False and body["performed_write"] is False
    assert len(body["would_write_files"]) == 8
    assert not (client._store / af._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_commit_without_token_is_400(client: TestClient):
    resp = client.post(_POST, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 400
    assert not (client._store / af._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_confirmed_build_writes_only_local_store(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN}).json()
    assert body["status"] == "ALPHA_FACTORY_BUILD_COMPLETE"
    assert body["wrote_store"] is True and body["wrote_to_database"] is False
    assert body["replaces_champion"] is False and body["promotes_to_live"] is False
    assert body["calls_prediction_service"] is False
    assert (client._store / af._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]
    # a subsequent read shows the persisted store
    after = client.get(_GET, headers=_AUTH).json()
    assert after["persisted"]["has_artifacts"] is True


def test_build_result_carries_no_live_block(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN}).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["mutates_champion"] is False


def test_decision_never_approves_live_and_champion_untouched(client: TestClient):
    for a in client.get(_GET + "/registry", headers=_AUTH).json()["registry"]["alphas"]:
        assert a["status"] in ("RESEARCH", "ACTIVE", "CHALLENGER", "CHAMPION", "REJECTED", "ARCHIVED")
    body = client.get(_GET, headers=_AUTH).json()
    assert body["champion"]["status"] == "CHAMPION"
    assert body["champion"]["name"] == "composite_sn"
