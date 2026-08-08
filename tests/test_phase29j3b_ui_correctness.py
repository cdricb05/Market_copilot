"""Phase 29J.3B — bounded UI-correctness / market-context cleanup.

Deterministic, network-free, DB-free focused regressions for the four bounded
fixes of Phase 29J.3B:

  1. The dead "US Dollar (DXY)" tile (no owned source) is replaced by the Federal
     Reserve Nominal Broad U.S. Dollar Index (FRED DTWEXBGS), served through the
     SAME owned FRED integration already used for US 10Y / US 2Y — no new provider,
     no direct-from-JS FRED call, honest UNAVAILABLE when the key is absent.
  2. The Portfolio decision-bar "Proposed N names" metric means the number of
     POSITIVE-WEIGHT proposed holdings (portfolio.proposed_holding_count), never the
     count of allocation rows (which also carry zero-weight EXIT / REPLACE_OUT rows).
  3. A single transient service-probe timeout no longer falsely presents the whole
     service as unavailable while authoritative health is good; a genuine failure is
     still surfaced.
  4. The large "LEGACY MEMBERSHIP-COMPARISON SUMMARY — REVIEW-ONLY COMPATIBILITY"
     daily-close headline is demoted to collapsed progressive disclosure, with every
     word still reachable.

The endpoint tests monkeypatch the yfinance + FRED helpers with deterministic
synthetic data, so no provider / API key / DB / prediction service is touched. The
kernel test drives the pure reallocation engine with an explicit input contract. The
UI-behaviour assertions read api/ui/index.html source (the established pattern; the UI
performs no market/allocation math and calls no provider directly).
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import paper_trader.api.app as appmod
from paper_trader.api.app import app
from paper_trader.config import get_settings
from paper_trader.engine import reallocation_proposal as R

ROOT = Path(__file__).resolve().parent.parent
UI = (ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")
APP_SRC = (ROOT / "api" / "app.py").read_text(encoding="utf-8")

_KEY = "phase29j3b-test-key"
_AUTH = {"X-API-Key": _KEY}
_INDICATORS = "/v1/market/indicators"


# --------------------------------------------------------------------------- #
# Fixtures / deterministic provider stubs
# --------------------------------------------------------------------------- #
@pytest.fixture()
def client(monkeypatch):
    os.environ["PAPER_TRADER_SERVICE_API_KEY"] = _KEY
    os.environ.setdefault(
        "PAPER_TRADER_DATABASE_URL",
        "postgresql+psycopg2://u:p@localhost:5432/paper_trader_test_unused",
    )
    os.environ.setdefault("PAPER_TRADER_STOCK_PREDICTION_API_URL", "http://127.0.0.1:9000")
    get_settings.cache_clear()

    # yfinance side — deterministic, no network.
    monkeypatch.setattr(
        appmod, "fetch_latest_prices",
        lambda syms: ([{"ticker": s, "price": "100.00"} for s in syms], []),
    )
    monkeypatch.setattr(appmod, "_batch_indicator_changes", lambda syms: {})
    c = TestClient(app)
    try:
        yield c
    finally:
        c.close()
        get_settings.cache_clear()


def _usd_broad_ok():
    # A realistic latest DTWEXBGS observation (index level ~122) + change vs prior obs.
    return {
        "value": "122.34", "as_of": "2026-08-07",
        "status": "FRED latest observation 2026-08-07",
        "change": "0.15", "change_pct": "0.122",
        "previous_value": "122.19", "previous_as_of": "2026-08-06",
    }


# =========================================================================== #
# 1. Broad USD reuses the owned FRED owner — no new provider (backend contract)
# =========================================================================== #
def test_broad_usd_uses_existing_fred_owner_no_new_provider(client, monkeypatch):
    captured = {}

    def fake_fred(series_map, api_key):
        captured["series_map"] = dict(series_map)
        return {k: (_usd_broad_ok() if k == "usd_broad" else None) for k in series_map}

    monkeypatch.setattr(appmod, "_batch_fred_with_prior", fake_fred)
    r = client.get(_INDICATORS, headers=_AUTH)
    assert r.status_code == 200
    # The Broad USD card is fetched through the SAME FRED helper (one owner) as the
    # US 10Y / US 2Y rates, and maps to the canonical DTWEXBGS series id.
    assert captured["series_map"].get("usd_broad") == "DTWEXBGS"
    assert captured["series_map"].get("us10y") == "DGS10"
    ph = {p["key"]: p for p in r.json()["placeholders"]}
    assert "usd_broad" in ph
    card = ph["usd_broad"]
    assert card["label"] == "USD Broad"
    assert card["source"] == "fred"
    assert card["available"] is True
    assert card["value"] == "122.34"
    assert card["as_of"] == "2026-08-07"
    # change vs prior observation is carried through the existing contract.
    assert card["change"] == "0.15"
    assert card["previous_as_of"] == "2026-08-06"


def test_backend_series_map_is_dtwexbgs_only_no_direct_dxy_source(client):
    # The FRED series map (the owner) declares DTWEXBGS for the dollar tile; DXY (which
    # has no owned source) is not fabricated from any provider.
    assert "DTWEXBGS" in APP_SRC
    assert '"usd_broad"' in APP_SRC


# =========================================================================== #
# 2. DXY dead tile is removed/replaced by Broad USD in the UI
# =========================================================================== #
def test_ui_dxy_dead_tile_removed_and_usd_broad_present():
    assert 'data-key="dxy"' not in UI
    assert "No owned data source" not in UI
    assert 'data-key="usd_broad"' in UI
    # the tile renders as a normal LOADING placeholder (filled by the single loader),
    # not a hard-coded "Unavailable" dead tile.
    m = re.search(r'data-key="usd_broad".*?</div>\s*</div>', UI, re.S)
    assert m and "USD Broad" in m.group(0)
    assert "LOADING" in m.group(0)


# =========================================================================== #
# 3. Broad USD missing state remains honest if unavailable
# =========================================================================== #
def test_broad_usd_missing_is_honest_unavailable(client, monkeypatch):
    # FRED returns nothing for any series (e.g. key absent / provider down).
    monkeypatch.setattr(appmod, "_batch_fred_with_prior",
                        lambda series_map, api_key: {k: None for k in series_map})
    r = client.get(_INDICATORS, headers=_AUTH)
    ph = {p["key"]: p for p in r.json()["placeholders"]}
    assert "usd_broad" in ph
    card = ph["usd_broad"]
    assert card["available"] is False
    assert card["reason"]                    # an explicit, honest reason — never a number
    assert card.get("value") in (None, "")   # no fabricated value


# =========================================================================== #
# 4. No direct-from-JS provider call for the dollar tile
# =========================================================================== #
def test_ui_makes_no_direct_fred_provider_call():
    # FRED is only ever called by the backend owner; the UI must not call it directly.
    assert "stlouisfed.org" not in UI
    assert "api.stlouisfed" not in UI
    # the Broad USD tile is populated by the single market loader from the backend
    # placeholders (the generic data-key mapping), not by any browser-side fetch.
    assert "formatFredValue" in UI


# =========================================================================== #
# 5 & 6. "Proposed names" == positive-weight holdings; zero-weight exits excluded
#         (authoritative owner: engine.reallocation_proposal)
# =========================================================================== #
def _pos(t, s, w, mv):
    return {"ticker": t, "sector": s, "current_weight": w, "market_value": mv,
            "quantity": 100, "price": mv / 100.0}


def _urow(t, rank, pct, sector="Tech", adv=5e7, eligible=True):
    return {"ticker": t, "rank": rank, "percentile": pct, "combined_score": pct,
            "sector": sector, "adv_dollar": adv, "eligible": eligible}


def _review(t, rec, rank, pct, sector="Tech", repl=None):
    return {"ticker": t, "recommendation": rec, "current_rank": rank,
            "current_score": pct, "signal_strength": pct,
            "strongest_replacement_ticker": repl, "drawdown_60d": -0.1,
            "liquidity_state": "LIQUID", "switching_cost_usd": 10.0, "net_improvement": 0.5}


def _aligned(tickers, n=80):
    dates = ["d%03d" % i for i in range(n)]
    series = {tk: [(((i * 7 + (j + 1) * 13) % 21) - 10) / 1000.0 for i in range(n)]
              for j, tk in enumerate(tickers)}
    return {"dates": dates, "series": series}


def _contract():
    holdings = ["AAA", "BBB", "CCC", "DDD"]
    cands = ["EEE", "FFF", "GGG"]
    return {
        "schema_version": R.INPUT_SCHEMA_VERSION,
        "eligible_market_date": "2026-08-06",
        "active_book_id": "alpha_paper_book_1", "active_book_label": "Alpha Paper Book #1",
        "valuation_date": "2026-08-06", "nav": 100000.0, "cash": 0.0,
        "portfolio_state_hash": "PSH", "universe_scoring_hash": "USH",
        "universe_input_contract_hash": "UIC",
        "hoc_assessment_hash": "HOC1", "hoc_assessment_state": "READY",
        "hoc_available": True, "hoc_data_gaps": [],
        "hoc_recommendation_counts": {"HOLD": 1, "REDUCE": 1, "EXIT": 1, "REPLACE": 1, "ADD": 0},
        "positions": [_pos("AAA", "Tech", 0.25, 25000.0), _pos("BBB", "Tech", 0.25, 25000.0),
                      _pos("CCC", "Fin", 0.25, 25000.0), _pos("DDD", "Fin", 0.25, 25000.0)],
        "hoc_reviews": [_review("AAA", "HOLD", 5, 0.90), _review("BBB", "REDUCE", 12, 0.60),
                        _review("CCC", "EXIT", 80, 0.10, sector="Fin"),
                        _review("DDD", "REPLACE", 40, 0.30, sector="Fin", repl="EEE")],
        "universe_rows": [_urow("AAA", 5, 0.90), _urow("BBB", 12, 0.60),
                          _urow("DDD", 40, 0.30, sector="Fin"),
                          _urow("EEE", 2, 0.95, sector="Health"),
                          _urow("FFF", 3, 0.93, sector="Energy"),
                          _urow("GGG", 4, 0.92, sector="Energy")],
        "aligned_returns": _aligned(holdings + cands),
    }


def _policy():
    p = dict(R.default_policy())
    p.update({"target_position_count": 4, "candidate_rank_max": 50, "max_name_weight": 0.5,
              "sector_cap_fraction": 1.0, "min_covariance_obs": 20, "covariance_lookback": 60,
              "min_volatility_coverage": 0.5})
    return p


def test_proposed_holding_count_is_positive_weights_only():
    res = R.build_proposal(input_contract=_contract(), policy=_policy())
    allocs = res["allocations"]
    positive = sum(1 for a in allocs if (a.get("proposed_weight") or 0) > 0)
    assert res["portfolio"]["proposed_holding_count"] == positive
    # not the count of allocation rows.
    assert res["portfolio"]["proposed_holding_count"] == 4


def test_zero_weight_exits_do_not_inflate_proposed_count():
    res = R.build_proposal(input_contract=_contract(), policy=_policy())
    allocs = res["allocations"]
    zero_weight = [a for a in allocs
                   if (a.get("proposed_weight") or 0) == 0 and a["action"] in ("EXIT", "REPLACE_OUT")]
    assert zero_weight, "scenario must contain zero-weight EXIT / REPLACE_OUT rows"
    # the allocation set is strictly larger than the positive-weight holding count
    # because the exits/replace-outs are carried but never counted as holdings.
    assert len(allocs) > res["portfolio"]["proposed_holding_count"]
    assert res["portfolio"]["proposed_holding_count"] == sum(
        1 for a in allocs if (a.get("proposed_weight") or 0) > 0)


def test_ui_decision_bar_uses_proposed_holding_count_not_row_count():
    # The Portfolio decision-bar reallocation chip reads the canonical positive-weight
    # count from the owner, and no longer counts allocation rows.
    assert "proposed_holding_count" in UI
    assert "allocations.length + ' names'" not in UI
    # the chip renders "<count> names" from proposed_holding_count.
    assert "proposedNames + ' names'" in UI


# =========================================================================== #
# 7 & 8. Transient probe timeout tolerated; genuine failure surfaced
# =========================================================================== #
def test_ui_transient_service_probe_timeout_is_retried_not_terminal():
    # ONE retrying service-health owner exists, with bounded transient retries and a
    # non-terminal "re-checking" state — a single timeout does not stick on unavailable.
    assert "__serviceHealthProbe" in UI
    assert "MAX_TRANSIENT_RETRIES" in UI
    assert "Checking…" in UI
    # a transient result triggers a retry (state 'transient') before any Timeout/Offline.
    assert "'transient'" in UI
    # authoritative SERVICE readiness vocabulary preserved (audit tokens).
    assert 'id="health-status"' in UI
    assert "checkServiceReady" in UI


def test_ui_genuine_service_failure_still_surfaced():
    # A real dependency failure (health non-OK / ready reporting a genuine reason) is
    # surfaced as Not Ready — never hidden by the transient-tolerant path.
    assert "'notready'" in UI
    assert "Not Ready" in UI
    # after the bounded retries are exhausted a genuine outage still shows Timeout/Offline.
    assert "after " in UI and "retries" in UI


# =========================================================================== #
# 9 & 10. Legacy membership comparison collapsed by default; content reachable
# =========================================================================== #
def test_ui_legacy_membership_comparison_collapsed_by_default():
    # the daily-close legacy compatibility headline is demoted to a native <details>
    # (closed by default) with the concise summary text.
    assert "_dcApplyHeadline" in UI
    assert "Legacy membership comparison &mdash; compatibility only" in UI
    m = re.search(r"function _dcApplyHeadline[\s\S]*?\n}\n", UI)
    assert m, "helper must be present"
    body = m.group(0)
    assert "<details" in body
    # the detail is NOT force-opened (no `open` attribute / `.open = true`) in the helper.
    assert "<details open" not in body
    # the DAG-card legacy comparison remains collapsed as before (audit contract).
    assert 'id="pm-dag-legacy"' in UI


def test_ui_legacy_membership_full_text_remains_reachable():
    # every word stays reachable on expand: both the full headline and explanation are
    # rendered inside the collapsed detail.
    m = re.search(r"function _dcApplyHeadline[\s\S]*?\n}\n", UI)
    body = m.group(0)
    assert "escapeHtml(d.headline" in body
    assert "escapeHtml(d.explanation" in body


# =========================================================================== #
# 11. No order / broker / automation / model-promotion behaviour changed
# =========================================================================== #
def test_market_indicators_remains_read_only(client, monkeypatch):
    monkeypatch.setattr(appmod, "_batch_fred_with_prior",
                        lambda series_map, api_key: {k: None for k in series_map})
    # read-only surface: POST is not allowed on the indicators route.
    assert client.post(_INDICATORS, headers=_AUTH).status_code == 405


def test_reallocation_kernel_creates_no_order_or_target():
    res = R.build_proposal(input_contract=_contract(), policy=_policy())
    safety = res.get("safety") or {}
    # the reallocation proposal is review-only: it never confirms a target or creates an order.
    assert safety.get("created_target_weights_authority") is False


def test_ui_safety_posture_preserved():
    # The bounded 29J.3B fixes are presentation/market-context only; the preview-first,
    # review-only, no-orders, automation-off safety posture is untouched.
    assert "NO ORDERS" in UI
    assert "PREVIEW ONLY" in UI
    assert "AUTOMATION OFF" in UI
    # no NEW enabled create-order / automation surface was introduced by these fixes:
    # the market-indicators + reallocation-count + health-probe + legacy-collapse edits
    # add no order/broker/automation/promotion route or handler.
    assert "createLiveOrder" not in UI
    assert "enableAutomation" not in UI
