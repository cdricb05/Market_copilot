"""Slice 5 (Phase 29F) — canonical operational portfolio-state tests.

Deterministic and hermetic: every contributing read model is injected as a small
dict fixture, so no DB / provider / prediction / ledger / real cycle is touched.
Covers the Workstream-K matrix: active-book selection, dormant-legacy rejection,
capital/positions/orders/fills/target/assessment/evidence normalization, the
consistency engine (CONSISTENT / DEGRADED / INCONSISTENT / UNAVAILABLE), the
stable state hash (generated_at excluded), field provenance, endpoint auth/schema,
the single UI loader (no JS NAV/active-book computation), architecture guards, and
the compatibility of the existing Daily Close / Daily Research Cycle / universe
scoring surfaces.
"""
from __future__ import annotations

import importlib
from datetime import datetime, timezone
from pathlib import Path

from paper_trader.api import portfolio_state as ps

UI = Path(__file__).resolve().parent.parent / "api" / "ui" / "index.html"
SRC = (Path(__file__).resolve().parent.parent / "api" / "portfolio_state.py").read_text(
    encoding="utf-8")


# --------------------------------------------------------------------------- #
# Deterministic fixtures — the active Alpha Paper Book #1 (clean reconciling
# numbers) + the dormant legacy DB book at 2026-07-20.
# --------------------------------------------------------------------------- #
def _pos(ticker, sector, qty, avg, price, cb, mv, upnl, uret, cw, tw, drift, dpnl):
    return {"ticker": ticker, "sector": sector, "quantity": qty, "average_cost": avg,
            "latest_price": price, "cost_basis": cb, "market_value": mv,
            "unrealized_pnl": upnl, "unrealized_pnl_pct": uret, "current_weight": cw,
            "target_weight": tw, "weight_drift": drift, "daily_pnl": dpnl,
            "status": "HOLD", "opened_date": "2026-07-22", "valuation_date": "2026-08-05"}


_HD = [
    _pos("AAA", "Tech", 600, 96.67, 100.0, 58000.0, 60000.0, 2000.0, 0.0345, 0.6, 0.5, 0.1, 100.0),
    _pos("BBB", "Energy", 350, 97.14, 100.0, 34000.0, 35000.0, 1000.0, 0.0294, 0.35, 0.5, -0.15, -50.0),
]


def _operational(*, book_id="alpha_paper_book_1", initialized=True,
                 holdings_count=2, pending_count=0, nav=100000.0):
    cs = {
        "operational_book_id": book_id, "operational_book_name": "Alpha Paper Book #1",
        "operational_book_status": "FORWARD_TRACKING_ACTIVE",
        "nav": nav, "cash": 5000.0, "invested_value": 95000.0, "cost_basis": 92000.0,
        "unrealized_pnl": 3000.0, "unrealized_pnl_pct": 0.0326, "daily_pnl": 50.0,
        "daily_pnl_pct": 0.0005, "holdings_count": holdings_count,
        "pending_order_count": pending_count, "fill_count": 2,
        "valuation_date": "2026-08-05", "desk_mark_date": "2026-08-05",
        "confirmed_target_date": "2026-07-21", "confirmed_target_count": 2,
        "target_confirmation_status": "READY_TO_CONFIRM", "execution_model": "NEXT_CLOSE",
        "lifecycle_stage": "FILLED",
        "safety_mode": {"manual_review": True, "paper_orders_only": True,
                        "broker_execution": False, "automation": False},
        "legacy_archive_summary": {"book_id": "legacy_paper_portfolio",
                                   "label": "Legacy Paper Portfolio (Archived)",
                                   "as_of_market_date": "2026-07-20", "positions_count": 2},
        "holdings_detail": [dict(r) for r in _HD],
    }
    ob = {
        "book_id": book_id, "book_label": "Alpha Paper Book #1",
        "initialized": initialized, "current_status": "FORWARD_TRACKING_ACTIVE",
        "execution_model": "NEXT_CLOSE", "holdings_count": holdings_count,
        "pending_order_count": pending_count, "fills_count": 2, "fill_count": 2,
        "nav_as_of_date": "2026-08-05", "desk_mark_date": "2026-08-05",
        "starting_capital": 100000.0, "initial_capital": 100000.0, "currency": "USD",
        "current_target": {"state": "READY_TO_CONFIRM", "alpha_market_date": "2026-08-05"},
        "target_market_date": "2026-07-21", "target_confirmation_status": "READY_TO_CONFIRM",
        "target_count": 2, "target_name": "fundamental_momentum_50_50_top25",
        "frozen_target_weights": {"AAA": 0.5, "BBB": 0.5},
        "pending_orders": {"pending_count": pending_count,
                           "by_status": ({"FILLED": 2} if pending_count == 0
                                         else {"PROPOSED": pending_count, "FILLED": 2}),
                           "total_orders": 2 + pending_count, "filled_count": 2,
                           "latest_submission_date": "2026-07-22"},
        "canonical_state": cs,
    }
    return {"operational_book": ob, "canonical_state": cs}


def _freshness(*, ambiguous=False, valuation="2026-08-05", benchmark="2026-08-05",
               close="2026-08-05", target="2026-08-05",
               book_id="alpha_paper_book_1", consistency="CONSISTENT"):
    return {
        "eligible_market_date": "2026-08-05",
        "active_book": {"active_book_id": book_id, "active_book_name": "Alpha Paper Book #1",
                        "active_book_status": "FORWARD_TRACKING_ACTIVE",
                        "active_book_authoritative_owner": "api.operational_book",
                        "ambiguous": ambiguous},
        "consistency_status": consistency, "consistency_violations": [],
        "source_freshness": [
            {"source_id": "operational_valuation", "as_of_date": valuation},
            {"source_id": "desk_marks", "as_of_date": "2026-08-05"},
            {"source_id": "benchmark", "as_of_date": benchmark},
            {"source_id": "latest_daily_close", "as_of_date": close},
            {"source_id": "target_calculation", "as_of_date": target},
        ],
    }


def _performance(*, book_id="alpha_paper_book_1"):
    return {
        "book_id": book_id,
        "summary": {"cumulative_return_pct": 0.5, "benchmark_cumulative_return_pct": 2.0,
                    "excess_vs_benchmark_pct_points": -1.5, "max_drawdown_pct": -1.8},
        "rows": [{"date": "2026-08-05", "nav": 100000.0, "benchmark_close": 770.0,
                  "benchmark_ticker": "SPY", "cumulative_return_pct": 0.5,
                  "benchmark_cumulative_return_pct": 2.0, "drawdown_pct": -1.2}],
    }


def _gate():
    return {"latest_completed_market_date": "2026-08-05", "outcome": "PROPOSAL_READY",
            "outcome_label": "PORTFOLIO CHANGES PROPOSED", "target_state": "PROPOSAL_READY",
            "proposed_change_count": 17,
            "proposed_additions": [{"ticker": "T%d" % i} for i in range(9)],
            "proposed_removals": [{"ticker": "R%d" % i} for i in range(8)],
            "proposed_resizes": [], "estimated_turnover": 0.338,
            "headline": "PORTFOLIO CHANGES PROPOSED — MANUAL REVIEW REQUIRED"}


def _forward():
    return {"evidence_state": "PRELIMINARY_EVIDENCE", "interpretation": "First window.",
            "latest_snapshot_date": "2026-08-05", "snapshot_count": 42,
            "matured_outcome_count": 30}


def _fills():
    return {"fills": [{"book_id": "alpha_paper_book_1", "fill_date": "2026-07-22"},
                      {"book_id": "alpha_paper_book_1", "fill_date": "2026-07-22"},
                      {"book_id": "other_book", "fill_date": "2026-07-01"}]}


def _load(**over):
    kw = dict(operational=_operational(), freshness=_freshness(), performance=_performance(),
              gate=_gate(), forward_status=_forward(), fills=_fills())
    kw.update(over)
    return ps.load_portfolio_state(**kw)


# --------------------------------------------------------------------------- #
# 1–4. Active-book selection / dormant rejection / missing / multiple books.
# --------------------------------------------------------------------------- #
def test_01_active_alpha_book_selected():
    r = _load()
    ab = r["active_book"]
    assert ab["book_id"] == "alpha_paper_book_1"
    assert ab["book_label"] == "Alpha Paper Book #1"
    assert ab["initialized"] is True
    assert ab["owner_module"] == "api.operational_book"
    assert ab["is_dormant_legacy_book"] is False


def test_02_dormant_legacy_book_reported_but_ignored():
    r = _load()
    ig = r["active_book"]["legacy_book_ignored"]
    assert ig["book_id"] == "legacy_paper_portfolio"
    assert ig["as_of_market_date"] == "2026-07-20"
    # The active book is NOT the 2026-07-20 dormant book.
    assert r["dates"]["valuation_date"] == "2026-08-05"


def test_03_dormant_legacy_book_never_selected_as_active():
    # Even if the operational payload IS the legacy book, it is flagged, not blessed.
    r = _load(operational=_operational(book_id="legacy_paper_portfolio"),
              freshness=_freshness(book_id="legacy_paper_portfolio"))
    assert r["active_book"]["is_dormant_legacy_book"] is True
    assert "rejected" in r["active_book"]["selection_reason"].lower()


def test_04_missing_active_book():
    r = _load(operational={}, freshness=_freshness(book_id=None))
    assert r["state"] == ps.STATE_NO_ACTIVE_BOOK
    assert r["consistency"]["status"] == ps.UNAVAILABLE
    assert r["active_book"]["initialized"] is False


def test_05_multiple_active_books_flagged():
    r = _load(freshness=_freshness(ambiguous=True))
    assert r["active_book"]["ambiguous"] is True
    assert "candidate" in r["active_book"]["selection_reason"].lower()


# --------------------------------------------------------------------------- #
# 5–17. Holdings / capital normalization.
# --------------------------------------------------------------------------- #
def test_06_holdings_normalized_and_sorted():
    r = _load()
    assert len(r["positions"]) == 2
    p = r["positions"][0]
    for k in ("ticker", "sector", "quantity", "average_cost", "cost_basis", "price",
              "market_value", "unrealized_pnl", "unrealized_return", "portfolio_weight",
              "target_weight", "drift", "operational_status"):
        assert k in p
    # Sorted by portfolio weight descending (AAA 0.6 before BBB 0.35).
    assert r["positions"][0]["ticker"] == "AAA"


def test_07_decimal_precision_and_cash():
    r = _load()
    assert r["capital"]["cash"] == 5000.0
    assert r["capital"]["nav"] == 100000.0


def test_08_invested_value_nav_costbasis():
    r = _load()
    assert r["capital"]["invested_value"] == 95000.0
    assert r["capital"]["nav"] == 100000.0
    assert r["capital"]["cost_basis"] == 92000.0


def test_09_unrealized_and_daily_pnl():
    r = _load()
    assert r["capital"]["unrealized_pnl"] == 3000.0
    assert r["capital"]["daily_pnl"] == 50.0


def test_10_cumulative_pnl_and_return():
    r = _load(operational=_operational(nav=100327.99))
    # nav - initial_capital
    assert abs(r["capital"]["cumulative_pnl"] - 327.99) < 1e-6
    assert r["capital"]["cumulative_return"] == 0.5


def test_11_benchmark_value_and_date():
    r = _load()
    assert r["capital"]["benchmark_value"] == 770.0
    assert r["capital"]["benchmark_date"] == "2026-08-05"
    assert r["dates"]["benchmark_date"] == "2026-08-05"


def test_12_excess_and_drawdown():
    r = _load()
    assert r["capital"]["cumulative_excess_vs_benchmark"] == -1.5
    assert r["capital"]["drawdown"] == -1.2
    assert r["capital"]["max_drawdown"] == -1.8


def test_13_dates_block():
    r = _load()
    d = r["dates"]
    assert d["eligible_market_date"] == "2026-08-05"
    assert d["valuation_date"] == "2026-08-05"
    assert d["desk_mark_date"] == "2026-08-05"
    assert d["latest_daily_close_date"] == "2026-08-05"
    assert d["target_calculation_date"] == "2026-08-05"
    assert d["portfolio_assessment_date"] == "2026-08-05"


def test_14_position_weights_and_drift():
    r = _load()
    aaa = next(p for p in r["positions"] if p["ticker"] == "AAA")
    assert aaa["portfolio_weight"] == 0.6
    assert aaa["target_weight"] == 0.5
    assert abs(aaa["drift"] - 0.1) < 1e-9


# --------------------------------------------------------------------------- #
# 18–24. Orders / fills / target / assessment / evidence references.
# --------------------------------------------------------------------------- #
def test_15_order_summary():
    r = _load()
    o = r["orders"]
    assert o["pending_count"] == 0
    assert o["filled"] == 2
    assert o["total_orders"] == 2


def test_16_fill_summary():
    r = _load()
    f = r["fills"]
    assert f["count"] == 2
    assert f["rows_count"] == 2   # only alpha_paper_book_1 fills counted
    assert f["latest_fill_date"] == "2026-07-22"


def test_17_target_reference_membership_and_weights():
    r = _load()
    t = r["target"]
    assert t["target_state"] == "READY_TO_CONFIRM"
    assert t["target_calculation_date"] == "2026-08-05"
    assert t["confirmed_target_date"] == "2026-07-21"
    assert t["membership_count"] == 2
    assert {m["ticker"] for m in t["members"]} == {"AAA", "BBB"}


def test_18_assessment_reference_review_only():
    r = _load()
    a = r["assessment"]
    assert a["assessment_date"] == "2026-08-05"
    assert a["outcome"] == "PROPOSAL_READY"
    assert a["proposed_change_count"] == 17
    assert a["is_preliminary_proposal"] is True
    assert a["confirmation_allowed"] is False
    # Slice 7 (Phase 29H) relabelled the review-only banner now that both the Holding
    # Opportunity-Cost review and the Reallocation Proposal engine exist (review-only).
    assert "NOT YET IMPLEMENTED" not in a["preliminary_proposal_label"]
    assert "REVIEW ONLY" in a["preliminary_proposal_label"]


def test_19_evidence_reference():
    r = _load()
    e = r["evidence"]
    assert e["evidence_state"] == "PRELIMINARY_EVIDENCE"
    assert e["latest_snapshot_date"] == "2026-08-05"
    assert e["snapshot_count"] == 42


def test_20_safety_state():
    r = _load()
    s = r["safety"]
    for flag in ("read_only", "paper_only", "manual_review"):
        assert s[flag] is True
    for flag in ("wrote_to_database", "wrote_to_ledger", "called_provider",
                 "called_prediction", "ran_daily_close", "ran_portfolio_reassessment",
                 "created_orders", "created_fills", "promoted_model",
                 "automatic_promotion_allowed"):
        assert s[flag] is False


def test_21_field_provenance_present():
    r = _load()
    assert r["capital"]["provenance"]["nav"].startswith("api.paper_trading_desk")
    assert r["positions"][0]["provenance"].startswith("api.operational_book")
    assert r["dates"]["provenance"]["eligible_market_date"].startswith("api.data_freshness")
    assert r["provenance"]["portfolio_state_owner"] == "api.portfolio_state"
    assert "portfolio_valuation" in r["provenance"]["source_of_truth"]["legacy_archive"]


# --------------------------------------------------------------------------- #
# 26–31. State hash / idempotency / mutation-free / no provider-prediction-order.
# --------------------------------------------------------------------------- #
def test_22_stable_state_hash():
    assert _load()["state_hash"] == _load()["state_hash"]


def test_23_generated_at_excluded_from_state_hash():
    a = ps.load_portfolio_state(operational=_operational(), freshness=_freshness(),
                                performance=_performance(), gate=_gate(),
                                forward_status=_forward(), fills=_fills(),
                                now=datetime(2026, 8, 6, 9, 0, tzinfo=timezone.utc))
    b = ps.load_portfolio_state(operational=_operational(), freshness=_freshness(),
                                performance=_performance(), gate=_gate(),
                                forward_status=_forward(), fills=_fills(),
                                now=datetime(2026, 8, 6, 23, 30, tzinfo=timezone.utc))
    assert a["generated_at"] != b["generated_at"]
    assert a["state_hash"] == b["state_hash"]
    # Stage 19.1 added "corporate_actions": the registered corporate-action registry is a
    # first-class, non-volatile part of the state (a registration must change state_hash).
    assert set(a["source_hashes"]) == {"operational_book", "data_freshness", "performance",
                                       "assessment_gate", "forward_evidence", "fills",
                                       "corporate_actions"}


def test_24_read_is_mutation_free():
    op = _operational()
    before = _HD[0]["market_value"]
    ps.load_portfolio_state(operational=op, freshness=_freshness(), performance=_performance(),
                            gate=_gate(), forward_status=_forward(), fills=_fills())
    # The injected fixtures are not mutated by the read composition.
    assert _HD[0]["market_value"] == before
    assert op["canonical_state"]["nav"] == 100000.0


def test_25_owner_makes_no_provider_prediction_order_call():
    forbidden = ("requests.get(", "requests.post(", "urlopen(", "httpx.", "predict(",
                 "place_order(", "submit_order(", "create_order(", "run_fill_cycle(",
                 "run_daily_close(", "promote_model(", ".commit(")
    for tok in forbidden:
        assert tok not in SRC, tok


# --------------------------------------------------------------------------- #
# 32–38. Consistency engine.
# --------------------------------------------------------------------------- #
def test_26_consistent_result():
    r = _load()
    assert r["consistency"]["status"] == ps.CONSISTENT
    assert r["consistency"]["counts"]["fail"] == 0
    assert r["state"] == ps.STATE_READY


def test_27_valuation_date_mismatch():
    op = _operational()
    op["canonical_state"]["valuation_date"] = "2026-08-04"   # ≠ desk mark 2026-08-05
    r = _load(operational=op)
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    # valuation (2026-08-04) vs desk mark (2026-08-05) mismatch.
    assert chk["VALUATION_VS_DESK_MARK"]["result"] == "FAIL"
    assert r["consistency"]["status"] == ps.INCONSISTENT
    assert r["state"] == ps.STATE_INCONSISTENT


def test_28_nav_reconciliation_mismatch():
    r = _load(operational=_operational(nav=123456.0))
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["NAV_RECONCILIATION"]["result"] == "FAIL"
    assert r["consistency"]["status"] == ps.INCONSISTENT


def test_29_holdings_count_mismatch():
    r = _load(operational=_operational(holdings_count=5))
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["HOLDINGS_COUNT_VS_ROWS"]["result"] == "FAIL"


def test_30_pending_order_mismatch():
    op = _operational(pending_count=0)
    op["operational_book"]["pending_orders"]["pending_count"] = 3   # rows say 0 pending
    r = _load(operational=op)
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["PENDING_ORDER_COUNT_VS_ROWS"]["result"] == "FAIL"


def test_31_active_book_identity_mismatch():
    r = _load(performance=_performance(book_id="some_other_book"))
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["ACTIVE_BOOK_IDENTITY"]["result"] == "FAIL"
    assert r["consistency"]["status"] == ps.INCONSISTENT


def test_32_degraded_but_readable():
    # A loaded-but-empty assessment gate → the assessment-vs-eligible check is
    # UNKNOWN → DEGRADED, but the endpoint still returns a readable payload. (An
    # injected {} means "loaded, no data"; None would trigger the real loader.)
    r = _load(gate={})
    chk = {c["code"]: c for c in r["consistency"]["checks"]}
    assert chk["ASSESSMENT_DATE_VS_REFERENCE"]["result"] == "UNKNOWN"
    assert r["consistency"]["status"] == ps.DEGRADED
    assert r["state"] == ps.STATE_DEGRADED
    # The payload is still fully populated (degraded, not an exception).
    assert r["capital"]["nav"] == 100000.0
    assert len(r["positions"]) == 2


def test_33_inconsistent_freshness_inherited():
    fr = _freshness(consistency="INCONSISTENT")
    fr["consistency_violations"] = [{"code": "DESK_MARK_MISMATCH"}]
    r = _load(freshness=fr)
    codes = [c["code"] for c in r["consistency"]["checks"]]
    assert "UPSTREAM_FRESHNESS_INCONSISTENT" in codes
    assert r["consistency"]["status"] == ps.INCONSISTENT


# --------------------------------------------------------------------------- #
# 40. Contract / schema.
# --------------------------------------------------------------------------- #
def test_34_top_level_contract():
    r = _load()
    for k in ("schema_version", "generated_at", "state", "consistency", "active_book",
              "dates", "capital", "positions", "orders", "fills", "target", "assessment",
              "evidence", "safety", "provenance", "state_hash", "source_hashes"):
        assert k in r, k
    assert r["schema_version"] == ps.SCHEMA_VERSION
    for f in ("book_id", "book_label", "status", "initialized", "execution_model",
              "holdings_count", "pending_order_count"):
        assert f in r["active_book"], f
    for f in ("cash", "invested_value", "nav", "cost_basis", "unrealized_pnl",
              "unrealized_return", "daily_pnl", "cumulative_pnl", "cumulative_return",
              "cumulative_excess_vs_benchmark", "drawdown"):
        assert f in r["capital"], f


# --------------------------------------------------------------------------- #
# 39–40, 44–47. Endpoint auth / schema / read-only.
# --------------------------------------------------------------------------- #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_35_endpoint_requires_auth():
    c = _client()
    assert c.get("/v1/operations/portfolio-state").status_code in (401, 403)


def test_36_endpoint_get_only():
    c = _client()
    assert c.post("/v1/operations/portfolio-state",
                  headers={"X-API-Key": _key()}).status_code == 405


def test_37_endpoint_read_only_schema(monkeypatch):
    canned = _load()
    # Release 50: the route fans out from the ONE decision snapshot, which hands the
    # owner the shared operational read (`operational=`) and memoises per identity;
    # the canned owner accepts the hint and the memo is reset so THIS owner is served.
    monkeypatch.setattr("paper_trader.api.app._pstate.load_portfolio_state",
                        lambda **_kw: canned)
    from paper_trader.api import decision_snapshot as _snap
    _snap.reset()
    c = _client()
    resp = c.get("/v1/operations/portfolio-state", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    body = resp.json()
    assert body["schema_version"] == ps.SCHEMA_VERSION
    assert body["safety"]["read_only"] is True
    assert body["active_book"]["book_id"] == "alpha_paper_book_1"


# --------------------------------------------------------------------------- #
# 41–43. UI: one loader, no JS NAV/active-book computation.
# --------------------------------------------------------------------------- #
def _ui():
    return UI.read_text(encoding="utf-8")


def test_38_one_ui_portfolio_state_loader():
    ui = _ui()
    assert ui.count("function loadPortfolioState") == 1
    assert ui.count("function renderPortfolioState") == 1


def test_39_ui_region_has_no_nav_or_active_book_computation():
    ui = _ui()
    start = ui.find("function loadPortfolioState")
    end = ui.find("window.renderPortfolioState")
    assert start != -1 and end != -1 and end > start
    region = ui[start:end]
    for tok in ("new Date(", "Date.now(", ".getTime(", ".reduce(",
                "|| 'fundamental", "book_id ||", "cached_total_value"):
        assert tok not in region, tok


def test_40_ui_canonical_nodes_guarded():
    ui = _ui()
    assert "PS_CANONICAL_NODES" in ui
    assert "_psIsCanonicalNode" in ui
    assert "data-ps-owned" in ui
    # _obSet and renderPmStatusbar refuse the canonical valuation nodes.
    assert "if (_psIsCanonicalNode(id)) return;" in ui


def test_41_ui_fetches_canonical_endpoint():
    assert "/v1/operations/portfolio-state" in _ui()


# --------------------------------------------------------------------------- #
# 48–54. Compatibility + architecture + slice boundaries.
# --------------------------------------------------------------------------- #
def _audit():
    return importlib.import_module("scripts.audit_architecture")


def test_42_architecture_zero_drift_and_ownership():
    rep = _audit().run_audit()
    d = rep["inventory_drift"]
    assert d["status"] == "OK"
    assert d["on_disk_not_in_inventory"] == []
    assert d["in_inventory_not_on_disk"] == []
    p = rep["portfolio_state_ownership"]
    assert p["owner_present"] is True
    assert p["missing_delegation"] == []
    assert p["forbidden_execution_calls"] == []
    assert p["portfolio_state_is_writer"] is False
    assert p["rejects_dormant_legacy_book"] is True
    assert p["second_portfolio_state_owner_modules"] == []
    assert p["canonical_route_methods"] == ["GET"]
    assert p["ui_portfolio_state_loader_count"] == 1
    assert p["ui_portfolio_state_renderer_count"] == 1
    assert p["ui_portfolio_state_computation"] == []
    assert p["ui_missing_guard_tokens"] == []


def _route_paths():
    from paper_trader.api.app import app
    return {r.path for r in app.routes if hasattr(r, "path")}


def test_43_existing_operational_surfaces_still_present():
    paths = _route_paths()
    for p in ("/v1/operational-book", "/v1/operations/daily-close",
              "/v1/operations/data-freshness", "/v1/operations/workflow-state",
              "/v1/operations/daily-research-cycle/status",
              "/v1/research/universe-scoring", "/v1/paper-desk/performance"):
        assert p in paths, p


def test_44_slice8_not_implemented():
    root = Path(__file__).resolve().parent.parent
    # Slice 6 (Holding Opportunity-Cost) and Slice 7 (Reallocation Proposal) are now
    # LANDED under their canonical owners; the Slice 8 Persistent Alpha Research Agent
    # registry remains NOT implemented, and the superseded-naming modules stay absent.
    for missing in ("api/opportunity_cost_engine.py", "api/portfolio_proposal.py",
                    "api/model_registry.py"):
        assert not (root / missing).exists(), missing
    paths = _route_paths()
    # Slice 7 landed the read route; only the never-built alternative stays absent.
    assert "/v1/operations/reallocation-proposal" in paths
    for absent in ("/v1/operations/opportunity-cost",
                   "/v1/operations/portfolio-proposal"):
        assert absent not in paths, absent


def test_45_preliminary_proposal_unapproved_in_payload():
    r = _load()
    assert r["assessment"]["proposal_status"] == "PRELIMINARY_REVIEW_ONLY_UNAPPROVED"
    assert r["assessment"]["confirmation_allowed"] is False
