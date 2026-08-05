"""tests/test_slice1_data_freshness.py — Slice 1 (Phase 29B) data-freshness contract.

Deterministic, offline tests for the canonical cross-source data-freshness owner
(``api.data_freshness``), its cadence-aware classifier, the month-boundary
condition, the read-only endpoint, the single UI freshness loader, and the safety
invariants. Every clock and data date is injected; no network, database,
provider, prediction, or write occurs.

Covers the directive's FRESHNESS (14–27), month-boundary (Workstream H), API/UI
(36–45) and SAFETY (46–51) requirements.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from paper_trader.api import data_freshness as df

UI = Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html"

# All-fresh injected date map (eligible session = 2026-08-04 via reference 2026-08-05).
_ALL = {k: "2026-08-04" for k in (
    "owned_price_date", "desk_mark_date", "benchmark_date", "research_mark_date",
    "target_calc_date", "monthly_input_date", "valuation_date",
    "daily_close_date", "true_forward_date", "prediction_mark_date")}


def _fresh(ref="2026-08-05", **overrides):
    ov = dict(_ALL)
    ov.update(overrides)
    return df.load_data_freshness(reference_today=ref, state={},
                                  daily_close_status={}, forward_status={},
                                  date_overrides=ov)


def _row(r, sid):
    return next(x for x in r["source_freshness"] if x["source_id"] == sid)


# --------------------------------------------------------------------------- #
# 14–21 classifier vocabulary
# --------------------------------------------------------------------------- #
def test_14_daily_price_fresh():
    assert df.classify_source(cadence=df.DAILY, as_of="2026-08-04",
                              anchor="2026-08-04")["status"] == df.FRESH


def test_15_daily_price_stale():
    c = df.classify_source(cadence=df.DAILY, as_of="2026-07-30", anchor="2026-08-04")
    assert c["status"] == df.STALE and c["lag_sessions"] >= 1


def test_16_monthly_current_within_cadence():
    assert df.classify_source(cadence=df.MONTHLY, as_of="2026-08-01",
                              anchor="2026-08-04")["status"] == df.FRESH


def test_17_monthly_due_after_month_transition():
    assert df.classify_source(cadence=df.MONTHLY, as_of="2026-07-31",
                              anchor="2026-08-03")["status"] == df.STALE


def test_18_quarterly_not_yet_due():
    # Prior-quarter fundamentals during the current quarter are NOT_DUE.
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2026-06-30",
                              anchor="2026-08-04")["status"] == df.NOT_DUE


def test_19_missing_required_source():
    assert df.classify_source(cadence=df.DAILY, as_of=None,
                              anchor="2026-08-04")["status"] == df.MISSING


def test_20_future_dated_source():
    assert df.classify_source(cadence=df.DAILY, as_of="2026-08-10",
                              anchor="2026-08-04")["status"] == df.FUTURE_DATED


def test_21_unknown_is_not_fresh():
    c = df.classify_source(cadence=df.DAILY, as_of="2026-08-04", anchor=None)
    assert c["status"] == df.UNKNOWN
    assert c["status"] != df.FRESH


def test_18b_quarterly_stale_more_than_one_quarter():
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2025-12-31",
                              anchor="2026-08-04")["status"] == df.STALE


# --------------------------------------------------------------------------- #
# 22–27 blocking behaviour + Workstream H month boundary
# --------------------------------------------------------------------------- #
def test_H_month_boundary_daily_fresh_monthly_due():
    # Daily current to 2026-08-03; monthly momentum still July.
    r = df.load_data_freshness(
        reference_today="2026-08-04", state={}, daily_close_status={}, forward_status={},
        date_overrides={**{k: "2026-08-03" for k in _ALL}, "monthly_input_date": "2026-07-31"})
    assert r["all_daily_inputs_fresh"] is True
    assert r["slower_inputs_due"] == ["momentum_monthly"]
    assert r["signal_refresh_ready"] is False
    assert r["true_forward_capture_ready"] is False
    assert r["operational_close_ready"] is True         # close NOT invalidated
    assert r["weakest_gate"] == "SOURCE:momentum_monthly"


def test_22_stale_research_blocks_true_forward():
    r = _fresh(research_mark_date="2026-07-20")
    assert _row(r, "research_model_mark")["status"] == df.STALE
    assert r["true_forward_capture_ready"] is False


def test_23_stale_research_does_not_invalidate_completed_close():
    r = _fresh(research_mark_date="2026-07-20")
    assert r["operational_close_ready"] is True
    assert any("remains valid" in w for w in r["warnings"])


def test_24_daily_price_staleness_blocks_signal_refresh():
    # Owned prices/desk lag -> the session gate is owned-data lag -> signal blocked.
    r = _fresh(owned_price_date="2026-07-30", desk_mark_date="2026-07-30")
    assert r["signal_refresh_ready"] is False
    assert r["market_session"]["session_status"] == "WAITING_FOR_OWNED_DATA"
    assert r["weakest_gate"] == "MARKET_SESSION:OWNED_DATA_LAG"


def test_25_required_actions_name_the_weakest_exact_source():
    r = _fresh(benchmark_date=None)
    assert r["weakest_gate"] == "SOURCE:benchmark"
    assert any(a.get("source_id") == "benchmark" for a in r["required_actions"])


def test_26_multiple_stale_sources_reported_without_hiding():
    r = _fresh(benchmark_date=None, research_mark_date="2026-07-01")
    ids = {a.get("source_id") for a in r["required_actions"]}
    assert "benchmark" in ids and "research_model_mark" in ids


def test_27_provenance_and_owner_present_per_source():
    r = _fresh()
    for s in r["source_freshness"]:
        assert s["authoritative_owner"] and s["provenance"]
        assert set(("cadence", "status", "as_of_date", "reason",
                    "blocks_current_operation")).issubset(s)


def test_33_legacy_owned_data_authority_labelling():
    r = _fresh()
    # Owned-provider-confirmed sessions are the authority (never the legacy Yahoo
    # weekday calendar); owned prices are labelled as an owned-EOD source.
    assert r["market_session"]["confirmation_source"] == "OWNED_EOD_PROVIDER_CONFIRMED_SESSIONS"
    assert "owned" in _row(r, "owned_daily_prices")["authoritative_owner"].lower()


# --------------------------------------------------------------------------- #
# 36–39 API endpoint
# --------------------------------------------------------------------------- #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_36_endpoint_requires_api_key():
    c = _client()
    r = c.get("/v1/operations/data-freshness")           # no key
    assert r.status_code in (401, 403)


def test_37_endpoint_is_get_only():
    c = _client()
    r = c.post("/v1/operations/data-freshness", headers={"X-API-Key": _key()})
    assert r.status_code == 405


def test_38_endpoint_returns_contract_read_only(monkeypatch):
    canned = {
        "status": "OK", "market_session": {"session_status": "SESSION_READY",
                                            "eligible_market_date": "2026-08-04"},
        "expected_completed_market_date": "2026-08-04",
        "eligible_market_date": "2026-08-04", "source_freshness": [],
        "all_daily_inputs_fresh": True, "slower_inputs_due": [],
        "signal_refresh_ready": True, "portfolio_reassessment_ready": True,
        "true_forward_capture_ready": True, "operational_close_ready": True,
        "weakest_gate": "NONE", "required_actions": [], "warnings": [],
        "safety": {"read_only": True, "wrote_to_database": False,
                   "called_provider": False, "called_prediction": False,
                   "ran_daily_close": False},
    }
    monkeypatch.setattr("paper_trader.api.app.load_data_freshness", lambda: canned)
    c = _client()
    r = c.get("/v1/operations/data-freshness", headers={"X-API-Key": _key()})
    assert r.status_code == 200
    body = r.json()
    for k in ("market_session", "expected_completed_market_date", "eligible_market_date",
              "source_freshness", "signal_refresh_ready", "portfolio_reassessment_ready",
              "true_forward_capture_ready", "operational_close_ready", "weakest_gate",
              "required_actions"):
        assert k in body
    assert body["safety"]["read_only"] is True


def test_39_dependency_failure_degrades_honestly(monkeypatch):
    def _boom():
        raise RuntimeError("state loader unavailable")
    monkeypatch.setattr(
        "paper_trader.api.current_operating_state.load_current_operating_state", _boom)
    r = df.load_data_freshness(reference_today="2026-08-05",
                               daily_close_status={}, forward_status={},
                               date_overrides=_ALL)
    assert r["status"] == "OK"
    assert any("unavailable" in w for w in r["warnings"])


# --------------------------------------------------------------------------- #
# 40–45 UI (substring / structural, like the existing UI-consistency tests)
# --------------------------------------------------------------------------- #
def test_40_ui_has_exactly_one_freshness_loader():
    html = UI.read_text(encoding="utf-8")
    assert html.count("function loadDataFreshness") == 1
    assert html.count("/v1/operations/data-freshness") >= 1


def test_41_ui_performs_no_market_date_arithmetic():
    html = UI.read_text(encoding="utf-8")
    start = html.find("function loadDataFreshness")
    end = html.find("window.renderDataFreshness")
    assert start != -1 and end != -1 and end > start
    region = html[start:end]
    for bad in ("new Date(", "Date.now(", ".getTime("):
        assert bad not in region


def test_42_ui_shows_operational_and_research_dates_separately():
    html = UI.read_text(encoding="utf-8")
    assert 'id="df-opval"' in html and 'id="df-research"' in html
    assert "Operational Valuation" in html and "Research / Model Date" in html


def test_43_ui_explains_slower_cadence_staleness():
    html = UI.read_text(encoding="utf-8")
    assert 'id="df-slower"' in html and "Slower Inputs Due" in html


def test_44_ui_research_staleness_not_operational_close_failure():
    html = UI.read_text(encoding="utf-8")
    assert "completed operational close remains valid" in html


def test_45_existing_daily_close_ui_intact():
    html = UI.read_text(encoding="utf-8")
    assert 'id="cc-dc-card"' in html and "Run Daily Close" in html


# --------------------------------------------------------------------------- #
# 46–51 safety
# --------------------------------------------------------------------------- #
def test_46_47_50_safety_flags_read_only():
    r = _fresh()
    s = r["safety"]
    assert s["read_only"] is True
    assert s["wrote_to_database"] is False
    assert s["wrote_to_ledger"] is False
    assert s["created_orders"] is False and s["created_signals"] is False
    assert s["created_trade_decisions"] is False and s["created_fills"] is False


def test_48_freshness_uses_probe_free_close_loader_not_probing_loader():
    src = (Path(__file__).resolve().parent.parent / "api" / "data_freshness.py").read_text(encoding="utf-8")
    assert "load_close_progress" in src
    # The probing loader is never CALLED (a comment may explain why it is avoided).
    assert "dc.load_daily_close(" not in src
    assert "daily_close.load_daily_close(" not in src


def test_49_no_prediction_call_flag():
    r = _fresh()
    assert r["safety"]["called_prediction"] is False
    assert r["safety"]["called_provider"] is False
    assert r["safety"]["ran_daily_close"] is False
