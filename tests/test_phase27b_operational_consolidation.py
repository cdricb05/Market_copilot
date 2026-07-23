"""
tests/test_phase27b_operational_consolidation.py - Phase 27B operational book consolidation.

ONE operational portfolio: Alpha Paper Book #1. Fully offline (reuses the Phase 27A
harness: Phase 25 owned-style CSV fixtures, tmp desk/ledger dirs, injectable marks,
deterministic clock seams). Covers: the read-only /v1/operational-book single source of
truth (identity, safety, honest uninitialized state, single-producer cash/NAV equality
with the desk ledger replay, pending-order counts), the /v1/operational-book/archive
Historical Paper Books payload (legacy = HISTORICAL_BOOK read-only archived; research
books = RESEARCH_BOOK, never operational), auth, read-only guarantees, and the UI static
contract: every operational page (Command Center, Portfolio, Daily Workflow, right panel)
references Alpha Paper Book #1 from the SAME payload, the legacy paper portfolio appears
only inside archive/history sections, research stays under Research & Audit, and no
native dialogs are introduced.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import alpha_book as ab
from paper_trader.api import alpha_target as at
from paper_trader.api import multi_horizon_ledger as ledger
from paper_trader.api import operational_book as ob_mod
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api.app import app

from tests.test_phase27a_paper_operations import (  # reuse the Phase 27A offline harness
    _AUTH, _D0, _TICKS, _confirm_snapshot, _dl, _marks_table, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# Friday 2026-07-17 after the close -> latest completed market date == the fixture
# market date (2026-07-17), so the fixture target is ALIGNED and deterministic.
_FRI_AFTER_CLOSE = datetime(2026, 7, 17, 21, 0, tzinfo=timezone.utc)
_MON_AFTER_CLOSE = datetime(2026, 7, 20, 21, 0, tzinfo=timezone.utc)


def _fake_valuation():
    return {
        "status": "OK",
        "current_mark": {"current_cash": "7374.81", "current_total_value": "99120.00",
                         "as_of_market_date": "2026-07-17"},
        "positions": [{"ticker": "CDW"}, {"ticker": "HUM"}],
    }


@pytest.fixture
def env27b(env, monkeypatch):
    """Layer the deterministic Phase 27B seams over the shared Phase 27A world."""
    monkeypatch.setattr(at, "_now_override", _FRI_AFTER_CLOSE)
    monkeypatch.setattr(at, "REQUIRED_TARGET_COUNT", len(_TICKS))
    monkeypatch.setattr(at, "_VALUATION_LOADER", lambda: {"current_mark": {}})
    monkeypatch.setattr(ab, "_VALUATION_LOADER", _fake_valuation)
    monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _fake_valuation)
    yield env


def _init_book(today="2026-07-18"):
    out = ab.initialize_book(confirm=ab.INIT_CONFIRM_TOKEN, today=today)
    assert out["status"] == ab.A_OK, out
    return out


# --------------------------------------------------------------------------- #
# Single source of truth: /v1/operational-book
# --------------------------------------------------------------------------- #
class TestOperationalBookPayload:
    def test_identity_is_alpha_paper_book_1(self, env27b):
        d = ob_mod.load_operational_book(today="2026-07-18")
        assert d["status"] == ob_mod.STATUS_OK
        o = d["operational_book"]
        assert o["book_id"] == "alpha_paper_book_1"
        assert o["book_label"] == "Alpha Paper Book #1"
        assert o["classification"] == "OPERATIONAL_BOOK"
        assert d["single_source_of_truth"]["endpoint"] == "/v1/operational-book"
        assert "ONE operational portfolio" in d["single_source_of_truth"]["note"]

    def test_read_only_safety_contract(self, env27b):
        d = ob_mod.load_operational_book(today="2026-07-18")
        assert d["paper_only"] is True
        assert d["read_only"] is True
        assert d["broker_enabled"] is False
        assert d["automation_enabled"] is False
        assert d["live_orders_enabled"] is False
        assert d["performed_write"] is False

    def test_uninitialized_book_is_honest(self, env27b):
        d = ob_mod.load_operational_book(today="2026-07-18")
        o = d["operational_book"]
        assert o["initialized"] is False
        assert o["cash"] is None and o["nav"] is None
        assert o["holdings_count"] == 0 and o["holdings"] == {}
        assert o["pending_orders"]["pending_count"] == 0
        assert o["current_status"] == "NO_CONFIRMED_TARGET"
        assert "not initialized" in o["not_initialized_note"]

    def test_target_confirmation_flows_through(self, env27b):
        _confirm_snapshot()
        d = ob_mod.load_operational_book(today="2026-07-18")
        o = d["operational_book"]
        assert o["current_status"] == "TARGET_CONFIRMED"
        t = o["current_target"]
        assert t is not None
        assert t["alpha_market_date"] == "2026-07-17"
        assert t["latest_completed_market_date"] == "2026-07-17"
        assert t["alpha_market_aligned"] is True

    def test_stale_target_state_flows_through(self, env27b, monkeypatch):
        monkeypatch.setattr(at, "_now_override", _MON_AFTER_CLOSE)
        d = ob_mod.load_operational_book(today="2026-07-18")
        t = d["operational_book"]["current_target"]
        assert t["state"] == "STALE_TARGET"
        assert at.B_ALPHA_STALE in t["confirmation_blockers"]

    def test_initialized_book_single_producer_equality(self, env27b):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        d = ob_mod.load_operational_book(today="2026-07-18")
        o = d["operational_book"]
        assert o["initialized"] is True
        assert o["cash"] == 100000.0 and o["nav"] == 100000.0
        assert o["holdings_count"] == 0
        # The SAME numbers must come out of the one producer (desk ledger replay).
        sdir = desk._desk_dir(None)
        book = desk.open_book(sdir)
        fills = [f for f in desk._fills(sdir) if f.get("book_id") == o["book_id"]]
        nav = desk.book_nav(book, fills, desk.read_marks(None))
        assert o["cash"] == nav["cash"] and o["nav"] == nav["nav"]
        assert o["holdings_count"] == nav["holdings_count"]

    def test_pending_order_counts_from_the_same_ledgers(self, env27b):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        plan = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, today="2026-07-18")
        assert plan["status"] == ab.A_OK, plan
        n = plan["n_orders_created"]
        assert n > 0
        d = ob_mod.load_operational_book(today="2026-07-18")
        po = d["operational_book"]["pending_orders"]
        assert po["pending_count"] == n
        assert po["awaiting_manual_confirmation"] == n
        assert po["awaiting_fill"] == 0
        # after the manual desk confirmation the same orders await their close
        c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, today="2026-07-18")
        assert c["status"] == desk.S_OK, c
        d2 = ob_mod.load_operational_book(today="2026-07-18")
        po2 = d2["operational_book"]["pending_orders"]
        assert po2["awaiting_manual_confirmation"] == 0
        assert po2["awaiting_fill"] == n
        assert po2["pending_count"] == n

    def test_loading_writes_nothing(self, env27b):
        ob_mod.load_operational_book(today="2026-07-18")
        ob_mod.load_historical_books()
        assert not (env27b["ledger"] / ledger.SNAPSHOTS_FILE).exists()
        assert not env27b["desk"].exists() or not list(env27b["desk"].iterdir())


# --------------------------------------------------------------------------- #
# Historical Paper Books archive
# --------------------------------------------------------------------------- #
class TestHistoricalBooksArchive:
    def test_legacy_book_is_archived_read_only(self, env27b):
        d = ob_mod.load_historical_books()
        assert d["status"] == ob_mod.ARCHIVE_STATUS_OK
        legacy = d["historical_books"][0]
        assert legacy["book_id"] == "legacy_paper_portfolio"
        assert legacy["classification"] == "HISTORICAL_BOOK"
        assert legacy["read_only"] is True and legacy["archived"] is True
        # the existing legacy holdings are preserved, never dropped
        assert legacy["positions_count"] == 2
        assert legacy["tickers"] == ["CDW", "HUM"]
        assert legacy["cash"] == 7374.81

    def test_active_operational_book_never_in_the_archive(self, env27b):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        d = ob_mod.load_historical_books()
        ids = [b["book_id"] for b in d["historical_books"]]
        assert "alpha_paper_book_1" not in ids
        assert d["operational_book_id"] == "alpha_paper_book_1"

    def test_research_books_stay_research(self, env27b):
        d = ob_mod.load_historical_books()
        assert len(d["research_books"]) >= 4
        for b in d["research_books"]:
            assert b["classification"] == "RESEARCH_BOOK", b
            assert b["book_id"] != "alpha_paper_portfolio"
        assert "never" in d["research_books_note"]

    def test_archive_carries_read_only_safety(self, env27b):
        d = ob_mod.load_historical_books()
        assert d["read_only"] is True and d["performed_write"] is False
        assert d["broker_enabled"] is False and d["automation_enabled"] is False


# --------------------------------------------------------------------------- #
# HTTP endpoints
# --------------------------------------------------------------------------- #
class TestEndpoints:
    def test_auth_required(self, client):
        assert client.get("/v1/operational-book").status_code in (401, 403)
        assert client.get("/v1/operational-book/archive").status_code in (401, 403)

    def test_operational_book_over_http(self, client, env27b):
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["status"] == ob_mod.STATUS_OK
        assert d["operational_book"]["book_label"] == "Alpha Paper Book #1"
        assert d["performed_write"] is False

    def test_archive_over_http(self, client, env27b):
        d = client.get("/v1/operational-book/archive", headers=_AUTH).json()
        assert d["status"] == ob_mod.ARCHIVE_STATUS_OK
        assert d["historical_books"][0]["classification"] == "HISTORICAL_BOOK"

    def test_routes_are_get_only(self):
        methods = {}
        for r in app.routes:
            if getattr(r, "path", "").startswith("/v1/operational-book"):
                methods[r.path] = set(r.methods)
        assert set(methods) == {"/v1/operational-book", "/v1/operational-book/archive"}
        for path, m in methods.items():
            assert "POST" not in m and "PUT" not in m and "DELETE" not in m, path


# --------------------------------------------------------------------------- #
# UI static contract
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


def _scripts(html: str) -> str:
    return "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))


def _cc_primary(html: str) -> str:
    return html[html.index('id="cc-root"'):html.index('id="cc-legacy-overview"')]


class TestUiCommandCenterOperationalBook:
    def test_operational_panel_first_class_before_kpis(self, html):
        assert html.index('id="cc-ob-panel"') < html.index('id="cc-research-strip"')
        panel = html[html.index('id="cc-ob-panel"'):html.index("CURRENT OPERATIONAL BOOK END")]
        assert "Current Operational Book" in panel
        assert "Alpha Paper Book #1" in panel
        for label in ("Cash", "NAV", "Holdings", "Pending Orders",
                      "Current Target", "Current Status"):
            assert ">" + label + "<" in panel, label
        assert 'id="cc-ob-next"' in panel      # Next Action surface


class TestUiPortfolioDefaultsToOperationalBook:
    def test_operational_card_content(self, html):
        # Phase 27B.8: the Portfolio route is the operational holdings dashboard
        # (KPI row + real holdings table), from the same canonical payload.
        card = html[html.index('id="tab-portfolio"'):html.index('id="tab-portfolio-manager"')]
        assert "Alpha Paper Book #1" in card
        for label in ("NAV", "Cash", "Invested Value", "Holdings",
                      "Unrealized P&amp;L", "Valuation Date"):
            assert ">" + label + "<" in card, label
        assert 'id="pdash-table"' in card

    def test_portfolio_deep_links_target_the_dashboard(self, html):
        # Phase 27B.8: the legacy archive deep-link is gone; portfolio sub-routes
        # resolve to the operational holdings dashboard.
        js = _scripts(html)
        assert "_PT_SUB_TARGET[sub || ''] || 'pdash-kpis'" in js
        assert "getElementById('pt-archive')" not in js


class TestUiDailyWorkflowOperationalSteps:
    def test_operational_workflow_card_first(self, html):
        # Phase 27C: the orphaned legacy tabs were removed; the daily-workflow route
        # runs from tab-prediction-cockpit to the next tab (tab-portfolio). The
        # operational workflow card still precedes the next route's DOM.
        assert html.index('id="dwob-card"') < html.index('id="tab-portfolio"')
        card = html[html.index('id="dwob-card"'):html.index("OPERATIONAL BOOK WORKFLOW END")]
        assert "Today's Operating Workflow" in card
        assert "Alpha Paper Book #1" in card

    def test_five_operational_steps_in_order(self, html):
        # Phase 27B.1 cutover: the operational stages are Refresh Desk Marks ->
        # Verify Alpha Target -> Generate Order Plan -> Review & Confirm Paper
        # Orders -> Monitor (statuses come from the canonical payload).
        card = html[html.index('id="dwob-card"'):html.index("OPERATIONAL BOOK WORKFLOW END")]
        labels = ["Refresh Desk Marks", "Verify Alpha Target", "Generate Order Plan",
                  "Review &amp; Confirm Paper Orders", "Monitor Fills, Holdings &amp; Performance"]
        idx = [card.index(lb) for lb in labels]
        assert idx == sorted(idx)
        for sid in ("dwob-step-refresh-data", "dwob-step-refresh-target", "dwob-step-review",
                    "dwob-step-order-plan", "dwob-step-monitor"):
            assert 'id="%s"' % sid in card, sid


class TestUiRightPanelDescribesOperationalBook:
    def test_operational_capacity_present(self, html):
        # Phase 27C: the legacy order-controls block was removed from the right panel;
        # the operational book capacity section remains the right-panel detail.
        assert "Operational Book &mdash; Alpha Paper Book #1 Capacity" in html

    def test_live_operational_state_surfaces(self, html):
        assert 'id="right-ob-state"' in html
        assert 'id="right-ob-nav"' in html

    def test_legacy_order_controls_removed_from_right_panel(self, html):
        # Phase 27C hard cutover: the legacy Create/Fill/Cancel paper-order controls
        # are gone from the always-visible operator right panel.
        assert "Advanced order controls (legacy paper workflow)" not in html
        assert 'id="right-create-orders-btn"' not in html


class TestUiSingleSourceOfTruth:
    def test_exactly_one_operational_book_fetch(self, html):
        js = _scripts(html)
        assert js.count("/v1/operational-book") == 1

    def test_one_renderer_feeds_every_surface(self, html):
        js = _scripts(html)
        start = js.index("function renderOperationalBook")
        body = js[start:js.index("window.renderOperationalBook")]
        for sid in ("cc-ob", "ptob", "dwob-state", "right-ob-state", "right-ob-nav"):
            assert sid in body, sid

    def test_every_operational_loader_reuses_the_one_loader(self, html):
        # Phase 27B.1/27B.2: command center, daily workflow, portfolio manager,
        # the paper-desk action path and the Alpha Portfolio page all reuse the
        # ONE coalesced loader (never a second fetch path). Phase 27B.8: the
        # Portfolio route activation loads the same coalesced loader directly
        # (replacing the legacy portfolio-terminal fetch) — a 7th reuse site.
        js = _scripts(html)
        assert js.count("try { loadOperationalBook(); } catch (e) {}") == 7

    def test_concurrent_loads_coalesce(self, html):
        js = _scripts(html)
        assert "_obInFlight" in js

    def test_portfolio_manager_statusbar_names_the_legacy_book(self, html):
        assert "Legacy portfolio (archived)" in html
        assert "Legacy positions" in html          # Phase 27A.1 pinned literal kept


class TestUiResearchStaysResearch:
    def test_research_view_carries_no_operational_book_panel(self, html):
        research = html[html.index('id="tab-multi-horizon"'):html.index("end tab-audit-advanced")]
        assert 'id="cc-ob-panel"' not in research
        assert 'id="ptob-card"' not in research
        assert 'id="dwob-card"' not in research

    def test_no_native_dialogs_introduced(self, html):
        js = _scripts(html)
        for pat in (r"(?<![A-Za-z0-9_])alert\s*\(", r"(?<![A-Za-z0-9_])confirm\s*\(",
                    r"(?<![A-Za-z0-9_])prompt\s*\("):
            assert not re.search(pat, js), pat

    def test_no_blank_buttons_on_new_panels(self, html):
        for start_marker, end_marker in (
                ('id="cc-ob-panel"', "CURRENT OPERATIONAL BOOK END"),
                ('id="tab-portfolio"', 'id="tab-portfolio-manager"'),
                ('id="dwob-card"', "OPERATIONAL BOOK WORKFLOW END")):
            region = html[html.index(start_marker):html.index(end_marker)]
            for m in re.finditer(r"<button[^>]*>(.*?)</button>", region, re.DOTALL):
                label = re.sub(r"<[^>]+>", "", m.group(1))
                label = re.sub(r"&[a-z#0-9]+;", "x", label)
                assert label.strip(), m.group(0)[:120]
            assert "Connect to load" not in region
