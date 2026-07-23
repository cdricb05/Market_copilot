"""
tests/test_phase27b8_operational_portfolio.py — Phase 27B.8

The normal Portfolio route is the AUTHORITATIVE operational holdings dashboard
for the ONE operational book (Alpha Paper Book #1): actual filled holdings,
per-position valuation, P&L, allocation, target-vs-actual drift and monitoring
status — sourced from the same canonical /v1/operational-book payload every
other operator surface uses.

Fully offline: reuses the Phase 27A/27B.1/27B.5 harness (owned-style CSV
fixtures, tmp desk / ledger dirs, injectable marks downloader, deterministic
clock seams). Order creation and fills happen ONLY against the isolated tmp
stores — never the user's real development book; no live broker activity, no
automation. UI checks read api/ui/index.html statically.

Proves:
  * the Portfolio route renders a REAL active-holdings table (not the old
    compressed ticker sentence) with every required column;
  * holdings + KPIs come from Alpha Paper Book #1 (holdings_detail /
    portfolio_summary / canonical_state KPIs), one source of truth;
  * totals reconcile (cash / invested / NAV / unrealized P&L) and current
    weights sum to ~1 − cash weight;
  * current vs target weight are distinguished and drift is exposed;
  * valuation date and model-target date are labelled independently and a
    target-date mismatch after fills is informational, never a red blocker;
  * the pre-fill state still shows the awaiting-close empty message;
  * no legacy CDW/HUM portfolio, no legacy UI phrases, no native dialogs;
  * loading the page performs no writes and introduces no broker / automation /
    live-order capability.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import alpha_book as ab
from paper_trader.api import operational_book as ob_mod
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)
from tests.test_phase27b5_operator_flow import (  # 27B.5 world builders
    _filled_world, _submitted_world, _proposed_world, _cs, _load, _N,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# Every field the holdings dashboard needs per position.
_HOLDING_KEYS = (
    "ticker", "name", "sector", "quantity", "average_cost", "latest_price",
    "cost_basis", "market_value", "unrealized_pnl", "unrealized_pnl_pct",
    "current_weight", "target_weight", "weight_drift", "status",
    "opened_date", "valuation_date",
)


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


@pytest.fixture(scope="module")
def portfolio_dom(html) -> str:
    """Just the static #tab-portfolio markup (script bodies excluded)."""
    start = html.index('id="tab-portfolio"')
    end = html.index("end tab-portfolio", start)
    seg = html[start:end]
    return re.sub(r"(?s)<script[^>]*>.*?</script>", "", seg)


# --------------------------------------------------------------------------- #
# 1. Backend: real per-holding detail sourced from Alpha Paper Book #1
# --------------------------------------------------------------------------- #
class TestBackendHoldingsDetail:
    def test_holdings_detail_present_and_counted(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        ob = d["operational_book"]
        hd = ob["holdings_detail"]
        assert isinstance(hd, list) and len(hd) == _N
        assert d["canonical_state"]["holdings_count"] == _N
        # exposed on the canonical contract too (one source for every surface)
        assert d["canonical_state"]["holdings_detail"] == hd

    def test_every_required_column_is_populated(self, env27b1):
        _filled_world()
        hd = _load(today="2026-07-21")["operational_book"]["holdings_detail"]
        for r in hd:
            for k in _HOLDING_KEYS:
                assert k in r, k
            # the numeric spine is present for a filled name
            for k in ("quantity", "average_cost", "latest_price", "cost_basis",
                      "market_value", "current_weight", "target_weight"):
                assert r[k] is not None, k
            assert r["status"] in ("HOLD", "WATCH", "REVIEW")

    def test_holdings_are_the_alpha_book_names_only(self, env27b1):
        _filled_world()
        book = _load(today="2026-07-21")["operational_book"]
        held = {r["ticker"] for r in book["holdings_detail"]}
        # holdings are exactly the ledger-replayed book holdings (no legacy names)
        assert held == set(book["holdings"].keys())
        assert "CDW" not in held and "HUM" not in held

    def test_totals_reconcile(self, env27b1):
        _filled_world()
        cs = _cs(today="2026-07-21")
        nav, cash, inv = cs["nav"], cs["cash"], cs["invested_value"]
        # NAV = cash + current market value (subject only to rounding)
        assert nav == pytest.approx(cash + inv, abs=0.05)
        # unrealized P&L = market value − cost basis
        assert cs["unrealized_pnl"] == pytest.approx(inv - cs["cost_basis"], abs=0.05)

    def test_current_weights_sum_to_one_minus_cash(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        hd = d["operational_book"]["holdings_detail"]
        ps = d["operational_book"]["portfolio_summary"]
        pos_w = sum(r["current_weight"] for r in hd
                    if r["current_weight"] is not None)
        assert pos_w + ps["cash_weight"] == pytest.approx(1.0, abs=0.01)
        assert ps["invested_weight"] == pytest.approx(1.0 - ps["cash_weight"], abs=1e-6)

    def test_current_and_target_weights_are_distinguished(self, env27b1):
        _filled_world()
        hd = _load(today="2026-07-21")["operational_book"]["holdings_detail"]
        for r in hd:
            assert r["current_weight"] is not None
            assert r["target_weight"] is not None
            # drift is exactly the signed difference (not conflated)
            assert r["weight_drift"] == pytest.approx(
                r["current_weight"] - r["target_weight"], abs=1e-6)
        # a confirmed target weight is not the executed weight — they differ
        assert any(r["current_weight"] != r["target_weight"] for r in hd)

    def test_valuation_and_target_dates_are_labelled_independently(self, env27b1):
        _filled_world()
        cs = _cs(today="2026-07-21")
        assert cs["valuation_date"] == cs["desk_mark_date"]     # desk mark date
        assert cs["target_date"] is not None
        # the two concepts are separate values (here the target predates the mark)
        assert cs["valuation_date"] != cs["target_date"]
        for r in _load(today="2026-07-21")["operational_book"]["holdings_detail"]:
            assert r["valuation_date"] == cs["valuation_date"]

    def test_daily_pnl_is_honest_never_a_fabricated_zero(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        ps = d["operational_book"]["portfolio_summary"]
        # positions opened at the valuation close have no prior holding day →
        # daily P&L is honestly unavailable (None), never an incorrect 0.
        assert isinstance(ps["daily_pnl_available"], bool)
        if not ps["daily_pnl_available"]:
            assert ps["daily_pnl"] is None
            assert d["canonical_state"]["daily_pnl"] is None

    def test_portfolio_summary_shape(self, env27b1):
        _filled_world()
        ps = _load(today="2026-07-21")["operational_book"]["portfolio_summary"]
        for k in ("invested_value", "cost_basis_total", "unrealized_pnl",
                  "unrealized_return", "cash_weight", "invested_weight",
                  "sector_exposure", "largest_positions", "best_performers",
                  "worst_performers", "drift_summary"):
            assert k in ps, k
        assert ps["drift_summary"]["implemented_count"] == _N
        assert ps["drift_summary"]["target_count"] == _N


# --------------------------------------------------------------------------- #
# 2. Filled state shows holdings; pre-fill state shows the awaiting-close message
# --------------------------------------------------------------------------- #
class TestFilledVersusPrefill:
    def test_filled_state_has_holdings(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        assert d["canonical_state"]["lifecycle_stage"] == "FILLED"
        assert len(d["operational_book"]["holdings_detail"]) == _N

    def test_submitted_state_has_empty_detail(self, env27b1):
        _submitted_world()
        d = _load()
        assert d["canonical_state"]["lifecycle_stage"] == "SUBMITTED"
        assert d["operational_book"]["holdings_detail"] == []
        assert d["operational_book"]["portfolio_summary"] is None
        assert d["canonical_state"]["holdings_count"] == 0

    def test_proposed_state_has_empty_detail(self, env27b1):
        _proposed_world()
        d = _load()
        assert d["operational_book"]["holdings_detail"] == []
        assert d["canonical_state"]["holdings_count"] == 0

    def test_ui_renders_the_awaiting_close_empty_state(self, js):
        # The renderer falls back to the required awaiting-close sentence when
        # submitted orders exist but no fills have landed.
        assert ("submitted paper orders are awaiting the next eligible "
                "completed close.") in js


# --------------------------------------------------------------------------- #
# 3. Target-date mismatch after fills is informational, never a red blocker
# --------------------------------------------------------------------------- #
class TestTargetDateMismatchInformational:
    def test_no_operational_blockers_when_target_predates_mark(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        cs = d["canonical_state"]
        assert cs["target_date"] < cs["valuation_date"]         # 1-cycle stale
        assert cs["blockers"] == []                             # never red
        assert cs["lifecycle_stage"] == "FILLED"
        assert cs["next_action_label"] == "Monitor Holdings and Performance"

    def test_ui_target_note_is_informational_not_error(self, js, portfolio_dom):
        # the amber informational note element exists and is styled pdash-info
        assert 'id="pdash-target-note"' in portfolio_dom
        assert "pdash-info" in portfolio_dom
        # the friendly operator sentence (never a raw code) is emitted
        assert "The active paper holdings remain valid; the next model review" in js
        # raw blocker codes never appear as static operator text on the page
        for code in ("ALPHA_MARKET_DATE_BEHIND_LATEST_COMPLETED_MARKET_DATE",
                     "DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT"):
            assert code not in portfolio_dom


# --------------------------------------------------------------------------- #
# 4. Read-only + safety invariants (loading the page writes nothing)
# --------------------------------------------------------------------------- #
class TestReadOnlyAndSafety:
    def test_operational_book_is_read_only(self, env27b1, client):
        _filled_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["performed_write"] is False
        assert d["read_only"] is True
        assert d["broker_enabled"] is False
        assert d["automation_enabled"] is False
        assert d["live_orders_enabled"] is False

    def test_loading_the_portfolio_payload_writes_nothing(self, env27b1, client):
        _filled_world()
        before_orders = desk.load_orders()["counts_by_status"]
        before_fills = desk.load_fills()["n_fills"]
        for _ in range(3):
            r = client.get("/v1/operational-book", headers=_AUTH)
            assert r.status_code == 200
            assert r.json()["operational_book"]["holdings_detail"]
        assert desk.load_orders()["counts_by_status"] == before_orders
        assert desk.load_fills()["n_fills"] == before_fills

    def test_holdings_detail_degrades_without_breaking_payload(self, env27b1,
                                                               monkeypatch):
        _filled_world()
        monkeypatch.setattr(ob_mod, "build_holdings_detail",
                            lambda **k: (_ for _ in ()).throw(RuntimeError("boom")))
        d = _load(today="2026-07-21")
        # canonical payload still loads; detail degrades to [] with a warning
        assert d["status"] == ob_mod.STATUS_OK
        assert d["operational_book"]["holdings_detail"] == []
        assert d["operational_book"]["portfolio_summary"] is None
        assert any("Holdings detail unavailable" in w for w in d["warnings"])


# --------------------------------------------------------------------------- #
# 5. Canonical KPI contract (cross-surface single source)
# --------------------------------------------------------------------------- #
class TestCanonicalKpiContract:
    def test_canonical_state_exposes_the_kpi_fields(self, env27b1):
        _filled_world()
        cs = _cs(today="2026-07-21")
        for k in ("invested_value", "cost_basis", "unrealized_pnl",
                  "unrealized_pnl_pct", "daily_pnl", "daily_pnl_available",
                  "cash_weight", "invested_weight", "valuation_date",
                  "holdings_detail", "portfolio_summary"):
            assert k in cs, k

    def test_endpoint_canonical_matches_operational_book_canonical(self, env27b1,
                                                                    client):
        _filled_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["canonical_state"] == d["operational_book"]["canonical_state"]
        assert d["canonical_state"]["invested_value"] == \
            d["operational_book"]["portfolio_summary"]["invested_value"]


# --------------------------------------------------------------------------- #
# 6. UI: a real, sortable, sticky holdings table with every required column
# --------------------------------------------------------------------------- #
class TestUiHoldingsTable:
    def test_real_table_elements_exist(self, portfolio_dom, js):
        assert 'id="pdash-table"' in portfolio_dom
        assert 'id="pdash-tbody"' in portfolio_dom
        assert 'id="pdash-thead-row"' in portfolio_dom
        assert "function renderPortfolioDashboard" in js

    def test_all_required_columns_present(self, portfolio_dom):
        # Phase 27B.9 removed the empty NAME column (no trusted owned name source —
        # it was dashes only); every other required column remains.
        for key in ("ticker", "sector", "quantity", "average_cost",
                    "latest_price", "cost_basis", "market_value",
                    "unrealized_pnl", "unrealized_pnl_pct", "current_weight",
                    "target_weight", "weight_drift", "status"):
            assert ('data-key="%s"' % key) in portfolio_dom, key
        assert 'data-key="name"' not in portfolio_dom     # 27B.9: NAME column gone

    def test_compressed_ticker_sentence_is_gone(self, js, portfolio_dom):
        # the old "ALAB × 12 · AMD × 7 …" builder must not exist anywhere
        assert "+ ' × ' + ob.holdings[tk]" not in js
        assert 'id="ptob-holdings-table"' not in portfolio_dom

    def test_table_is_sortable(self, js):
        assert "function _pdInitSortHandlers" in js
        assert "addEventListener('click'" in js
        assert "window._pdSort" in js

    def test_header_is_sticky(self, portfolio_dom):
        assert "position:sticky" in portfolio_dom

    def test_positive_and_negative_pnl_are_distinguished(self, js):
        assert "pdash-pos" in js and "pdash-neg" in js

    def test_kpi_cards_present(self, portfolio_dom):
        for eid in ("pdash-kpi-nav", "pdash-kpi-cash", "pdash-kpi-invested",
                    "pdash-kpi-holdings", "pdash-kpi-upnl", "pdash-kpi-uret",
                    "pdash-kpi-dpnl", "pdash-kpi-valdate"):
            assert ('id="%s"' % eid) in portfolio_dom, eid

    def test_summary_cards_present(self, portfolio_dom):
        for eid in ("pdash-sum-cashinv", "pdash-sum-sector", "pdash-sum-largest",
                    "pdash-sum-best", "pdash-sum-worst", "pdash-sum-drift"):
            assert ('id="%s"' % eid) in portfolio_dom, eid

    def test_header_title_and_as_of_labels(self, portfolio_dom):
        assert "Portfolio &mdash; Alpha Paper Book #1" in portfolio_dom
        assert "Holdings valued as of" in portfolio_dom
        assert "Model target calculated as of" in portfolio_dom
        assert 'id="ptob-state"' in portfolio_dom               # BOOK ACTIVE badge

    def test_book_active_label_is_reachable(self, js):
        # FILLED lifecycle → "BOOK ACTIVE" operator label (single vocabulary)
        assert "'BOOK ACTIVE'" in js


# --------------------------------------------------------------------------- #
# 7. No legacy portfolio, no legacy phrases, no native dialogs, correct safety
# --------------------------------------------------------------------------- #
class TestUiNoLegacyNoDialogs:
    _BANNED = (
        "Historical Paper Books", "Legacy Portfolio Terminal",
        "Legacy Paper Book", "Legacy Signal Portfolio", "reconciler cache",
        "Quant Model Methodology", "$9,999", "$7,374",
    )

    def test_no_legacy_phrases_in_the_file(self, html):
        low = html.lower()
        for s in self._BANNED:
            assert s.lower() not in low, s

    def test_no_cdw_or_hum_ticker_in_the_dom(self, html):
        # word-boundary so "Human-readable" etc. never trips it
        assert re.search(r"\bCDW\b", html) is None
        assert re.search(r"\bHUM\b", html) is None

    def test_no_native_dialogs(self, js):
        assert not re.search(r"(?<![A-Za-z0-9_.])alert\(", js)
        assert not re.search(r"(?<![A-Za-z0-9_.])confirm\(", js)
        assert not re.search(r"(?<![A-Za-z0-9_.])prompt\(", js)

    def test_safety_wording_distinguishes_broker_from_paper_orders(self, html):
        assert "NO LIVE BROKER ORDERS" in html
        # the ambiguous badge must not suggest paper orders don't exist
        assert ">NO LIVE ORDERS</span>" not in html

    def test_alpha_portfolio_model_target_banner_retained(self, html):
        assert "MODEL TARGET" in html                           # 27B.7 requirement
