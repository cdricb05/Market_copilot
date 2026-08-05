"""tests/test_slice1_data_freshness.py — Slice 1 (Phase 29B / 29B.1) data-freshness.

Deterministic, offline tests for the canonical cross-source data-freshness owner
(``api.data_freshness``), its cadence-aware classifier, the month-boundary
condition, the ACTIVE-operational-book alignment (Phase 29B.1 corrective patch),
the active-book identity contract, the cross-surface consistency validator, the
read-only endpoint, the single UI freshness loader, and the safety invariants.

Every clock and data date is injected; no network, database, provider, prediction
or write occurs. Requirement #32 (operational-ledger fingerprint unchanged) is
enforced by the handoff ``validate.ps1`` (fingerprint before/after the full
pytest run), not by hashing the user's live ledger inside a unit test.
"""
from __future__ import annotations

import copy
from pathlib import Path

from paper_trader.api import data_freshness as df

UI = Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html"

# --------------------------------------------------------------------------- #
# Regression fixture reproducing the observed live defect (Workstream H).
# Active operational book @ 2026-08-04; dormant current-alpha mark @ 2026-07-20;
# target/price-score @ 2026-07-31; frozen monthly momentum month = 2026-07;
# eligible session = 2026-08-04 (reference_today 2026-08-05). No date invented.
# --------------------------------------------------------------------------- #
_OP = {
    "operational_book": {
        "book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
        "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
        "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
        "latest_desk_mark_date": "2026-08-04",
        "current_target": {"alpha_market_date": "2026-07-31",
                           "latest_completed_market_date": "2026-08-04"},
    }
}
_INPUTS = {"market_as_of_date": "2026-07-31", "momentum_month": "2026-07",
           "fundamental_as_of_date": "2026-05-22"}
_DAILY = {"latest_valid_mark_date": "2026-07-20"}          # dormant champion mark
_DESK = {"series": {"SPY": [["2026-07-31", 747.03], ["2026-08-03", 757.67],
                            ["2026-08-04", 771.33]]},
         "latest_completed_date": "2026-08-04"}
_CLOSE = {"market_date": "2026-08-04"}
_FWD = {"latest_snapshot_date": "2026-07-31"}


def _regression(**kw):
    args = dict(reference_today="2026-08-05", operational=copy.deepcopy(_OP),
                inputs=dict(_INPUTS), daily_status=dict(_DAILY),
                desk_marks=copy.deepcopy(_DESK), daily_close_status=dict(_CLOSE),
                forward_status=dict(_FWD))
    args.update(kw)
    return df.load_data_freshness(**args)


def _row(r, sid):
    return next(x for x in r["source_freshness"] if x["source_id"] == sid)


# --------------------------------------------------------------------------- #
# Classifier vocabulary (frozen contract).
# --------------------------------------------------------------------------- #
def test_classifier_daily_fresh():
    assert df.classify_source(cadence=df.DAILY, as_of="2026-08-04",
                              anchor="2026-08-04")["status"] == df.FRESH


def test_classifier_daily_stale():
    c = df.classify_source(cadence=df.DAILY, as_of="2026-07-30", anchor="2026-08-04")
    assert c["status"] == df.STALE and c["lag_sessions"] >= 1


def test_classifier_monthly_current_within_cadence():
    assert df.classify_source(cadence=df.MONTHLY, as_of="2026-08-01",
                              anchor="2026-08-04")["status"] == df.FRESH


def test_classifier_monthly_due_after_month_transition():
    assert df.classify_source(cadence=df.MONTHLY, as_of="2026-07-31",
                              anchor="2026-08-03")["status"] == df.STALE


def test_classifier_quarterly_not_yet_due():
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2026-06-30",
                              anchor="2026-08-04")["status"] == df.NOT_DUE


def test_classifier_quarterly_stale_more_than_one_quarter():
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2025-12-31",
                              anchor="2026-08-04")["status"] == df.STALE


def test_classifier_missing_and_future_and_unknown():
    assert df.classify_source(cadence=df.DAILY, as_of=None,
                              anchor="2026-08-04")["status"] == df.MISSING
    assert df.classify_source(cadence=df.DAILY, as_of="2026-08-10",
                              anchor="2026-08-04")["status"] == df.FUTURE_DATED
    c = df.classify_source(cadence=df.DAILY, as_of="2026-08-04", anchor=None)
    assert c["status"] == df.UNKNOWN and c["status"] != df.FRESH


# --------------------------------------------------------------------------- #
# Workstream H — regression fixture reproducing the live bug and its correction.
# --------------------------------------------------------------------------- #
def test_H_regression_active_book_confirms_session():
    r = _regression()
    ms = r["market_session"]
    # 13/14 — session ready from active owned data; no false WAITING_FOR_OWNED_DATA.
    assert ms["session_status"] == "SESSION_READY"
    assert r["eligible_market_date"] == "2026-08-04"
    # Operational concepts equal the active operational mark (2026-08-04).
    assert _row(r, "operational_valuation")["as_of_date"] == "2026-08-04"
    assert _row(r, "desk_marks")["as_of_date"] == "2026-08-04"
    assert _row(r, "benchmark")["as_of_date"] == "2026-08-04"
    assert _row(r, "latest_daily_close")["as_of_date"] == "2026-08-04"
    for sid in ("owned_daily_prices", "desk_marks", "benchmark", "operational_valuation"):
        assert _row(r, sid)["status"] == df.FRESH
    # Dormant 2026-07-20 does NOT override operational state and does not confirm.
    assert ms["latest_confirmed_owned_data_date"] == "2026-08-04"
    assert _row(r, "champion_research_mark")["as_of_date"] == "2026-07-20"
    assert _row(r, "champion_research_mark")["blocks_current_operation"] is False


def test_H_regression_distinct_research_dates_and_monthly_due():
    r = _regression()
    assert _row(r, "target_calculation")["as_of_date"] == "2026-07-31"
    assert _row(r, "price_score_refresh")["as_of_date"] == "2026-07-31"
    assert _row(r, "momentum_monthly")["status"] == df.STALE
    assert r["slower_inputs_due"] == ["momentum_monthly"]
    assert r["signal_refresh_ready"] is False
    assert r["portfolio_reassessment_ready"] is False
    assert r["true_forward_capture_ready"] is False
    assert r["operational_close_ready"] is True                 # close NOT invalidated
    assert r["weakest_gate"] == "SOURCE:momentum_monthly"
    assert r["consistency_status"] == df.CONSISTENT
    # 15 — required action never says to wait for owned data.
    for a in r["required_actions"]:
        assert "wait for owned market data" not in (a.get("action") or "").lower()


# 1 — active operational book overrides dormant legacy research state everywhere.
def test_01_active_book_overrides_dormant_for_operational_concepts():
    # Even if the dormant champion mark is far in the past, ONLY operational
    # owners drive the operational concepts.
    r = _regression(daily_status={"latest_valid_mark_date": "2026-01-05"})
    for sid in ("owned_daily_prices", "desk_marks", "benchmark", "operational_valuation"):
        assert _row(r, sid)["as_of_date"] == "2026-08-04"
    assert r["active_book"]["active_book_id"] == "alpha_paper_book_1"
    assert r["active_book"]["operational_mark_date"] == "2026-08-04"


# 2 — dormant current-alpha mark cannot supply owned-data confirmation.
def test_02_dormant_mark_cannot_confirm_owned_data():
    r = _regression()
    assert r["market_session"]["latest_confirmed_owned_data_date"] == "2026-08-04"
    assert r["eligible_market_date"] != "2026-07-20"


# 3 — operational valuation matches active portfolio/operational valuation.
def test_03_operational_valuation_matches_active_valuation():
    r = _regression()
    assert (_row(r, "operational_valuation")["as_of_date"]
            == _OP["operational_book"]["nav_as_of_date"])


# 4 — desk mark matches active desk state.
def test_04_desk_mark_matches_active_desk():
    r = _regression()
    assert (_row(r, "desk_marks")["as_of_date"]
            == _OP["operational_book"]["desk_mark_date"])


# 5 — benchmark matches active operational benchmark (SPY attached to the mark).
def test_05_benchmark_matches_active_operational_benchmark():
    r = _regression()
    assert _row(r, "benchmark")["as_of_date"] == "2026-08-04"


# 6 — Daily Close date matches the probe-free close-progress source.
def test_06_daily_close_matches_probe_free_progress():
    r = _regression()
    assert _row(r, "latest_daily_close")["as_of_date"] == _CLOSE["market_date"]


# 7 — target date comes from the target owner (alpha_target alpha_market_date).
def test_07_target_date_from_target_owner():
    r = _regression()
    tc = _row(r, "target_calculation")
    assert tc["as_of_date"] == "2026-07-31"
    assert "alpha_target" in tc["authoritative_owner"]


# 8 — price/score refresh date comes from the model-input (research) owner.
def test_08_price_score_refresh_from_research_owner():
    r = _regression()
    ps = _row(r, "price_score_refresh")
    assert ps["as_of_date"] == "2026-07-31"
    assert ps["kind"] == df.KIND_RESEARCH
    assert "multi_horizon_engine" in ps["authoritative_owner"]


# 9 — monthly input uses a directly persisted source (its OWN month_label).
def test_09_monthly_input_uses_directly_persisted_source():
    r = _regression(inputs={**_INPUTS, "momentum_month": "2026-08"})
    mm = _row(r, "momentum_monthly")
    assert mm["status"] == df.FRESH                    # August input for August month
    assert "month_label" in mm["provenance"]


# 10 — monthly input is MISSING when no direct source exists.
def test_10_monthly_input_missing_without_direct_source():
    r = _regression(inputs={"market_as_of_date": "2026-07-31",
                            "fundamental_as_of_date": "2026-05-22"})  # no momentum_month
    assert _row(r, "momentum_monthly")["as_of_date"] is None
    assert _row(r, "momentum_monthly")["status"] == df.MISSING


# 11 — monthly input is NEVER proxied from target/valuation/champion/session.
def test_11_monthly_input_never_proxied():
    r = _regression(inputs={"market_as_of_date": "2026-07-31",
                            "fundamental_as_of_date": "2026-05-22"})
    mm = _row(r, "momentum_monthly")["as_of_date"]
    # It must not borrow the target (07-31), valuation/close (08-04) or champion (07-20).
    assert mm not in ("2026-07-31", "2026-08-04", "2026-07-20")
    assert mm is None


# 12 — month-boundary state identifies momentum_monthly as due.
def test_12_month_boundary_identifies_momentum_due():
    r = _regression()
    assert "momentum_monthly" in r["slower_inputs_due"]
    assert r["weakest_gate"] == "SOURCE:momentum_monthly"


# 13 — session remains SESSION_READY when active owned data confirms the session.
def test_13_session_ready_when_owned_data_confirms():
    assert _regression()["market_session"]["session_status"] == "SESSION_READY"


# 14 — no false WAITING_FOR_OWNED_DATA.
def test_14_no_false_waiting_for_owned_data():
    assert _regression()["market_session"]["session_status"] != "WAITING_FOR_OWNED_DATA"


# 15 — no false "wait for owned data" operator action.
def test_15_no_false_wait_for_owned_data_action():
    r = _regression()
    blob = " ".join((a.get("action") or "") for a in r["required_actions"]).lower()
    assert "wait for owned market data" not in blob


# 16 — signal refresh is blocked by the exact research source.
def test_16_signal_refresh_blocked_by_research_source():
    r = _regression()
    assert r["signal_refresh_ready"] is False
    blocked = {a["source_id"] for a in r["required_actions"] if a.get("source_id")}
    assert "momentum_monthly" in blocked
    assert _row(r, "momentum_monthly")["kind"] == df.KIND_RESEARCH


# 17 — portfolio reassessment readiness follows its declared required-source policy.
def test_17_reassessment_readiness_follows_required_sources():
    # With the monthly source fresh AND daily research fresh, reassessment is ready.
    r = _regression(inputs={"market_as_of_date": "2026-08-04",
                            "momentum_month": "2026-08",
                            "fundamental_as_of_date": "2026-05-22"},
                    operational={"operational_book": {**_OP["operational_book"],
                        "current_target": {"alpha_market_date": "2026-08-04",
                                           "latest_completed_market_date": "2026-08-04"}}})
    assert r["portfolio_reassessment_ready"] is True
    # Making the monthly source stale again blocks reassessment.
    r2 = _regression()
    assert r2["portfolio_reassessment_ready"] is False


# 18 — TRUE_FORWARD capture is blocked by a stale required research input.
def test_18_true_forward_blocked_by_stale_research():
    r = _regression()
    assert r["true_forward_capture_ready"] is False
    assert _row(r, "momentum_monthly")["required_for_true_forward_capture"] is True


# 19 — completed operational close remains valid regardless of research staleness.
def test_19_completed_close_remains_valid():
    r = _regression()
    assert r["operational_close_ready"] is True
    assert any("remains valid" in w for w in r["warnings"])


# 20 — cross-surface consistency status is CONSISTENT for matching inputs.
def test_20_consistency_consistent_for_matching_inputs():
    r = _regression()
    assert r["consistency_status"] == df.CONSISTENT
    assert r["consistency_violations"] == []


# 21 — deliberate mismatch produces INCONSISTENT and names each violation.
def test_21_deliberate_mismatch_is_inconsistent_and_named():
    # Freshness rows diverge from the authoritative operational payload.
    r = _regression(date_overrides={"valuation_date": "2026-07-20",
                                    "target_calc_date": "2026-07-01"})
    assert r["consistency_status"] == df.INCONSISTENT
    codes = {v["code"] for v in r["consistency_violations"]}
    assert "OPERATIONAL_VALUATION_MISMATCH" in codes
    assert "TARGET_DATE_MISMATCH" in codes
    assert any("consistency" in w.lower() for w in r["warnings"])


# 22 — multiple active-book candidates degrade honestly, not silently.
def test_22_multiple_active_book_candidates_degrade_honestly():
    cand_a = _OP
    cand_b = {"operational_book": {**_OP["operational_book"],
                                   "book_id": "some_other_book"}}
    r = _regression(active_book_override=[cand_a, cand_b])
    assert r["active_book"]["ambiguous"] is True
    assert r["active_book"]["active_book_status"] == df.INCONSISTENT
    assert r["active_book"]["active_book_id"] is None
    assert r["consistency_status"] == df.INCONSISTENT
    assert any(v["code"] == "MULTIPLE_ACTIVE_BOOK_CANDIDATES"
               for v in r["consistency_violations"])


def test_active_book_identity_contract_fields():
    ab = _regression()["active_book"]
    for k in ("active_book_id", "active_book_name", "active_book_status",
              "active_book_authoritative_owner", "operational_mark_date"):
        assert k in ab
    assert ab["active_book_authoritative_owner"] == "api.operational_book"


def test_research_dates_labelled_research_only():
    r = _regression()
    for sid in ("price_score_refresh", "momentum_monthly", "champion_research_mark",
                "fundamental_quarterly", "latest_true_forward"):
        assert _row(r, sid)["kind"] == df.KIND_RESEARCH
    for sid in ("owned_daily_prices", "desk_marks", "benchmark",
                "operational_valuation", "latest_daily_close", "target_calculation"):
        assert _row(r, sid)["kind"] == df.KIND_OPERATIONAL


def test_every_source_has_owner_and_provenance():
    r = _regression()
    for s in r["source_freshness"]:
        assert s["authoritative_owner"] and s["provenance"]
        assert set(("cadence", "kind", "status", "as_of_date", "reason",
                    "blocks_current_operation")).issubset(s)


def test_degrades_honestly_when_operational_loader_unavailable(monkeypatch):
    def _boom():
        raise RuntimeError("operational book loader unavailable")
    monkeypatch.setattr(
        "paper_trader.api.operational_book.load_operational_book", _boom)
    r = df.load_data_freshness(reference_today="2026-08-05", inputs=dict(_INPUTS),
                               daily_status=dict(_DAILY), desk_marks=copy.deepcopy(_DESK),
                               daily_close_status=dict(_CLOSE), forward_status=dict(_FWD))
    assert r["status"] == "OK"
    assert any("unavailable" in w for w in r["warnings"])
    # Without an operational book, operational concepts are honestly absent — the
    # dormant book is NOT substituted.
    assert _row(r, "operational_valuation")["as_of_date"] is None


# --------------------------------------------------------------------------- #
# 23–26 API endpoint (authenticated, GET-only, read-only, no provider/prediction).
# --------------------------------------------------------------------------- #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_23_endpoint_authenticated_and_get_only():
    c = _client()
    assert c.get("/v1/operations/data-freshness").status_code in (401, 403)
    assert c.post("/v1/operations/data-freshness",
                  headers={"X-API-Key": _key()}).status_code == 405


def test_24_25_26_endpoint_read_only_no_provider_no_prediction(monkeypatch):
    canned = {
        "status": "OK", "phase": df.PHASE,
        "market_session": {"session_status": "SESSION_READY",
                           "eligible_market_date": "2026-08-04"},
        "expected_completed_market_date": "2026-08-04",
        "eligible_market_date": "2026-08-04",
        "active_book": {"active_book_id": "alpha_paper_book_1"},
        "source_freshness": [], "all_daily_inputs_fresh": True,
        "slower_inputs_due": [], "signal_refresh_ready": False,
        "portfolio_reassessment_ready": False, "true_forward_capture_ready": False,
        "operational_close_ready": True, "weakest_gate": "SOURCE:momentum_monthly",
        "required_actions": [], "consistency_status": "CONSISTENT",
        "consistency_violations": [], "warnings": [],
        "safety": {"read_only": True, "wrote_to_database": False,
                   "called_provider": False, "called_prediction": False,
                   "ran_daily_close": False},
    }
    monkeypatch.setattr("paper_trader.api.app.load_data_freshness", lambda: canned)
    c = _client()
    resp = c.get("/v1/operations/data-freshness", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    body = resp.json()
    for k in ("market_session", "eligible_market_date", "active_book",
              "source_freshness", "consistency_status", "consistency_violations",
              "signal_refresh_ready", "operational_close_ready", "weakest_gate"):
        assert k in body
    s = body["safety"]
    assert s["read_only"] is True and s["wrote_to_database"] is False
    assert s["called_provider"] is False and s["called_prediction"] is False
    assert s["ran_daily_close"] is False


def test_probe_free_close_loader_not_probing_loader():
    src = (Path(__file__).resolve().parent.parent / "api" / "data_freshness.py").read_text(encoding="utf-8")
    assert "load_close_progress" in src
    assert "dc.load_daily_close(" not in src
    assert "daily_close.load_daily_close(" not in src


def test_safety_flags_read_only():
    s = _regression()["safety"]
    assert s["read_only"] is True and s["wrote_to_database"] is False
    assert s["wrote_to_ledger"] is False
    assert s["created_orders"] is False and s["created_signals"] is False
    assert s["created_trade_decisions"] is False and s["created_fills"] is False
    assert s["called_provider"] is False and s["called_prediction"] is False
    assert s["ran_daily_close"] is False


# --------------------------------------------------------------------------- #
# 27–30 UI (substring / structural, like the existing UI-consistency tests).
# --------------------------------------------------------------------------- #
def test_27_ui_has_exactly_one_freshness_loader():
    html = UI.read_text(encoding="utf-8")
    assert html.count("function loadDataFreshness") == 1
    assert html.count("/v1/operations/data-freshness") >= 1


def test_28_ui_performs_no_market_date_arithmetic():
    html = UI.read_text(encoding="utf-8")
    start = html.find("function loadDataFreshness")
    end = html.find("window.renderDataFreshness")
    assert start != -1 and end != -1 and end > start
    region = html[start:end]
    for bad in ("new Date(", "Date.now(", ".getTime("):
        assert bad not in region


def test_29_ui_shows_operational_and_research_dates_separately():
    html = UI.read_text(encoding="utf-8")
    # Operational dates
    for anchor in ('id="df-eligible"', 'id="df-opval"', 'id="df-desk"',
                   'id="df-benchmark"', "Operational Valuation", "Desk Mark", "Benchmark"):
        assert anchor in html
    # Distinct research dates (never collapsed into one "research date")
    for anchor in ('id="df-target"', 'id="df-pricescore"', 'id="df-momentum"',
                   'id="df-fundamental"', 'id="df-trueforward"',
                   "Latest Price / Score Refresh", "Target Calculation",
                   "Monthly Momentum Input", "Fundamental Data", "TRUE_FORWARD Snapshot"):
        assert anchor in html


def test_30_ui_research_staleness_not_operational_close_failure():
    html = UI.read_text(encoding="utf-8")
    assert "completed operational close remains valid" in html


def test_existing_daily_close_ui_intact():
    html = UI.read_text(encoding="utf-8")
    assert 'id="cc-dc-card"' in html and "Run Daily Close" in html
