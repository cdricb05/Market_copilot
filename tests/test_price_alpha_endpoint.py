"""
tests/test_price_alpha_endpoint.py — Phase 21 Price-Alpha Factory routes.

Contract tests for the read-only dashboard / registry / leaderboard / correlation / combinations GETs
and the manual, confirmation-gated build POST, exercised through the real FastAPI app fully offline:
synthetic owned-style fundamental + price panels and a synthetic Phase 17-A report are wired via env
seams and the dedicated store is redirected to a tmp dir. Verifies auth, the read-only GETs, that a
preview performs no write, that a committing build requires the confirmation token (400 otherwise),
that a confirmed build writes only the local store, and that no status approves live trading /
replaces the champion / writes the database.
"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.api import price_alpha_factory as paf
from paper_trader.api import alpha_factory as af
from paper_trader.api import price_panel as pp

from tests.test_alpha_factory import _write_panel, _write_reval_report
from tests.test_price_alpha_factory import _write_price_csv

_GET = "/v1/research/price-alpha-factory"
_POST = "/v1/research/price-alpha-factory/run"
_TOKEN = "RUN_PRICE_ALPHA_FACTORY_BUILD"
_KEY = "price-alpha-test-key"
_AUTH = {"X-API-Key": _KEY}


@pytest.fixture
def client(monkeypatch, tmp_path) -> TestClient:
    monkeypatch.setenv("PAPER_TRADER_SERVICE_API_KEY", _KEY)
    monkeypatch.setenv("PAPER_TRADER_DATABASE_URL",
                       "postgresql+psycopg2://unused:unused@localhost:5432/unused")
    fund = tmp_path / "fund.csv"
    px = tmp_path / "px.csv"
    report = tmp_path / "reval.json"
    store = tmp_path / "store"
    _write_panel(fund)
    _write_price_csv(px)
    _write_reval_report(report)
    monkeypatch.setenv(af.PANEL_ENV, str(fund))
    monkeypatch.setenv(pp.PRICE_ENV, str(px))
    monkeypatch.setenv(af.REVAL_REPORT_ENV, str(report))
    monkeypatch.setenv(paf.STORE_ENV, str(store))
    paf.clear_cache()
    af.clear_cache()
    get_settings.cache_clear()
    with TestClient(app) as c:
        c._store = store  # type: ignore[attr-defined]
        yield c
    paf.clear_cache()
    af.clear_cache()


def test_auth_required(client: TestClient):
    assert client.get(_GET).status_code in (401, 403)
    assert client.post(_POST).status_code in (401, 403)


def test_get_dashboard_shape(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["phase"] == "21"
    assert body["status"] == "PRICE_ALPHA_FACTORY_READY"
    assert body["champion"]["name"] == "composite_sn"
    assert body["challenger"]["name"] == "composite_sn_repaired"
    assert len(body["families"]) == 5
    assert all(f["data_ready"] for f in body["families"])
    assert body["registry"]["counts"]["total"] == 31
    assert body["correlation"]["signals"][0] == "composite_sn"
    assert body["price_panel"]["readiness"] == "READY"


def test_get_carries_no_live_trading_block(client: TestClient):
    body = client.get(_GET, headers=_AUTH).json()
    assert body["live_trading_status"] == "NOT_APPROVED_FOR_LIVE_TRADING"
    assert body["no_decision_approves_live_trading"] is True
    assert body["replaces_champion"] is False
    assert body["wrote_to_database"] is False and body["creates_orders"] is False


def test_get_is_read_only_no_store_written(client: TestClient):
    client.get(_GET, headers=_AUTH)
    assert not (client._store / paf._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_registry_leaderboard_correlation_combination_slices(client: TestClient):
    reg = client.get(_GET + "/registry", headers=_AUTH).json()
    assert reg["registry"]["counts"]["total"] == 31
    assert len(reg["registry"]["schema"]) >= 18
    lb = client.get(_GET + "/leaderboard", headers=_AUTH).json()
    assert lb["leaderboard"][0]["is_champion"] is True
    assert "horizon_summary" in lb and "best_by_family" in lb
    co = client.get(_GET + "/correlation", headers=_AUTH).json()
    assert co["correlation"]["signals"][0] == "composite_sn"
    cb = client.get(_GET + "/combinations", headers=_AUTH).json()
    assert "combinations" in cb["combinations"]


def test_get_route_is_get_only(client: TestClient):
    assert client.put(_GET, headers=_AUTH).status_code == 405
    assert client.delete(_GET, headers=_AUTH).status_code == 405


def test_preview_no_write(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": False}).json()
    assert body["status"] == "PRICE_ALPHA_FACTORY_BUILD_PREVIEW"
    assert body["wrote_store"] is False and body["performed_write"] is False
    assert len(body["would_write_files"]) == 13
    assert not (client._store / paf._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_commit_without_token_is_400(client: TestClient):
    resp = client.post(_POST, headers=_AUTH, json={"commit": True})
    assert resp.status_code == 400
    assert not (client._store / paf._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]


def test_confirmed_build_writes_only_local_store(client: TestClient):
    body = client.post(_POST, headers=_AUTH, json={"commit": True, "confirm": _TOKEN}).json()
    assert body["status"] == "PRICE_ALPHA_FACTORY_BUILD_COMPLETE"
    assert body["wrote_store"] is True and body["wrote_to_database"] is False
    assert body["replaces_champion"] is False and body["promotes_to_live"] is False
    assert body["calls_prediction_service"] is False
    assert (client._store / paf._RUN_STATE_FILE).exists()  # type: ignore[attr-defined]
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
