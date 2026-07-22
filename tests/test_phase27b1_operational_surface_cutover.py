"""
tests/test_phase27b1_operational_surface_cutover.py - Phase 27B.1 true operational
surface cutover + desk mark repair.

Fully offline (reuses the Phase 27A harness: Phase 25 owned-style CSV fixtures, tmp
desk/ledger dirs, injectable marks downloader, deterministic clock seams). Covers:

* CANONICAL PAYLOAD - the /v1/operational-book flat contract (book identity, cash /
  NAV / holdings / orders / fills, target vs implemented counts, implementation
  percentage, desk-mark state, five-stage workflow, ONE next action, blockers).
* DESK MARKS - the root-cause repair: an empty initialized book acquires sizing
  marks for the confirmed target; the fetch window can never collapse to empty;
  success requires a non-null persisted date equal to the required latest completed
  market date; requested/priced/missing reconcile; a null-date result is an explicit
  DESK_MARK_REFRESH_BLOCKED (never a green success); the refresh creates no order /
  fill / holding and never reconfirms the target; order-plan readiness gates on
  aligned complete marks; GET /v1/alpha-book/desk-mark-readiness.
* PORTFOLIO MANAGER - "target unchanged" never implies "implemented"; 0-of-N is
  explicit; legacy valuation health never populates Alpha Book health; model HOLD
  is distinguished from executed holdings.
* UI STATIC - Daily Workflow legacy archive collapsed, operational stepper stage
  labels, header ownership, right-panel operational values, Portfolio empty states,
  research terminology (RESEARCH CHAMPION vs OPERATIONAL STRATEGY), no native
  dialogs, no blank buttons on the new panels.
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
from paper_trader.api import portfolio_manager as pm
from paper_trader.api.app import app

from tests.test_phase27a_paper_operations import (  # reuse the Phase 27A offline harness
    _AUTH, _D0, _TICKS, _confirm_snapshot, _dl, _marks_table, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# Friday 2026-07-17 after the US close -> the clock-resolved latest completed market
# date equals the fixture market date, so target AND desk marks can genuinely align.
_FRI_AFTER_CLOSE = datetime(2026, 7, 17, 21, 0, tzinfo=timezone.utc)


def _fake_book_valuation():
    return {
        "status": "OK",
        "current_mark": {"current_cash": "7374.81", "current_total_value": "99120.00",
                         "as_of_market_date": "2026-07-17"},
        "positions": [{"ticker": "CDW"}, {"ticker": "HUM"}],
    }


def _pm_valuation():
    return {
        "seeded": True,
        "current_mark": {"current_total_value": 99120.0, "current_cash": 7374.81,
                         "current_positions_value": 91745.19,
                         "current_unrealized_pnl": 120.0,
                         "current_total_return_pct": -0.88,
                         "open_position_count": 2,
                         "as_of_market_date": "2026-07-17",
                         "freshness_status": "FRESH", "age_calendar_days": 0},
        "positions": [
            {"ticker": "CDW", "weight_pct": 12.0, "market_value": 45000.0,
             "unrealized_pnl": 100.0, "unrealized_pnl_pct": 0.2,
             "status": "OPEN", "reason": None},
            {"ticker": "HUM", "weight_pct": 11.0, "market_value": 46745.19,
             "unrealized_pnl": 20.0, "unrealized_pnl_pct": 0.1,
             "status": "OPEN", "reason": None},
        ],
        "warnings": [],
    }


@pytest.fixture
def env27b1(env, monkeypatch):
    """Layer the deterministic Phase 27B.1 seams over the shared Phase 27A world."""
    monkeypatch.setattr(at, "_now_override", _FRI_AFTER_CLOSE)
    monkeypatch.setattr(at, "REQUIRED_TARGET_COUNT", len(_TICKS))
    monkeypatch.setattr(at, "_VALUATION_LOADER", lambda: {"current_mark": {}})
    monkeypatch.setattr(ab, "_VALUATION_LOADER", _fake_book_valuation)
    monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _fake_book_valuation)
    monkeypatch.setattr(pm, "_VALUATION_LOADER", _pm_valuation)
    yield env


def _init_book(today="2026-07-18"):
    out = ab.initialize_book(confirm=ab.INIT_CONFIRM_TOKEN, today=today)
    assert out["status"] == ab.A_OK, out
    return out


def _html():
    return _UI.read_text(encoding="utf-8")


def _scripts(html):
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# CANONICAL PAYLOAD (Workstream A)
# --------------------------------------------------------------------------- #
class TestCanonicalPayload:
    def test_flat_contract_fields_present(self, env27b1):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        o = ob_mod.load_operational_book(today="2026-07-18")["operational_book"]
        for key in ("book_id", "book_label", "book_type", "current_status",
                    "starting_capital", "cash", "nav", "holdings_count",
                    "pending_order_count", "fill_count", "target_name", "target_count",
                    "target_market_date", "target_confirmation_status",
                    "desk_mark_date", "desk_mark_status", "implementation_count",
                    "implementation_percentage", "workflow_stage", "workflow_stages",
                    "next_action_code", "next_action_label", "blockers",
                    "header_status", "order_plan_ready", "ledger_integrity_ok"):
            assert key in o, key
        assert o["book_type"] == "OPERATIONAL_BOOK"
        assert len(o["workflow_stages"]) == 5
        assert [s["code"] for s in o["workflow_stages"]] == [
            "REFRESH_DESK_MARKS", "VERIFY_ALPHA_TARGET", "GENERATE_ORDER_PLAN",
            "CONFIRM_PAPER_ORDERS", "MONITOR"]

    def test_confirmed_unimplemented_truthful_with_marks(self, env27b1):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        o = ob_mod.load_operational_book(today="2026-07-18")["operational_book"]
        assert o["target_count"] == len(_TICKS)
        assert o["implementation_count"] == 0
        assert o["implementation_percentage"] == 0.0
        assert o["desk_mark_status"] == "DESK_MARK_READY"
        assert o["desk_mark_date"] == "2026-07-17"
        assert o["order_plan_ready"] is True
        assert o["header_status"]["code"] == "ORDER_PLAN_READY"
        assert o["workflow_stage"] == "GENERATE_ORDER_PLAN"
        assert o["next_action_code"] in ("GENERATE_ORDER_PLAN", "CONFIRM_ORDER_PLAN")

    def test_marks_missing_state_is_explicit(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        o = ob_mod.load_operational_book(today="2026-07-18")["operational_book"]
        assert o["desk_mark_status"] == "DESK_MARK_MISSING"
        assert o["desk_mark_date"] is None
        assert o["order_plan_ready"] is False
        assert o["next_action_code"] == "REFRESH_DESK"
        assert o["workflow_stage"] == "REFRESH_DESK_MARKS"
        assert o["header_status"]["code"] == "DESK_MARK_REQUIRED"
        st = {s["code"]: s["status"] for s in o["workflow_stages"]}
        assert st["REFRESH_DESK_MARKS"] == "NEEDS_ACTION"
        assert st["VERIFY_ALPHA_TARGET"] == "COMPLETE"
        assert st["GENERATE_ORDER_PLAN"] == "BLOCKED"
        assert st["CONFIRM_PAPER_ORDERS"] == "PENDING"
        assert st["MONITOR"] == "PENDING"
        assert any("DESK_MARKS_MISSING" in b for b in o["blockers"])

    def test_implementation_never_inferred_from_target(self, env27b1):
        _confirm_snapshot()
        _refresh("2026-07-18")
        _init_book("2026-07-18")
        o = ob_mod.load_operational_book(today="2026-07-18")["operational_book"]
        # confirmed 8-name target, zero fills -> zero implemented, empty holdings
        assert o["target_count"] == len(_TICKS)
        assert o["implementation_count"] == 0
        assert o["holdings"] == {} and o["holdings_count"] == 0
        assert "not" in o["implementation_note"].lower()

    def test_one_next_action_code_matches_sentence(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        o = ob_mod.load_operational_book(today="2026-07-18")["operational_book"]
        assert o["next_action"].startswith(o["next_action_code"])


# --------------------------------------------------------------------------- #
# DESK MARKS (Workstream B) - root-cause repair
# --------------------------------------------------------------------------- #
class TestDeskMarkRefreshRepair:
    def test_empty_initialized_book_acquires_sizing_marks(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        r = _refresh("2026-07-18")
        assert r["status"] == desk.S_OK
        assert r["resulting_desk_mark_date"] == "2026-07-17"
        assert r["latest_completed_market_date"] == "2026-07-17"
        assert r["requested_ticker_count"] == len(_TICKS)
        assert r["priced_ticker_count"] == len(_TICKS)
        assert r["missing_ticker_count"] == 0 and r["missing_tickers"] == []
        assert r["priced_ticker_count"] + r["missing_ticker_count"] == \
            r["requested_ticker_count"]
        assert r["benchmark_priced"] is True
        assert "None" not in r["message"]
        # the persisted store returns the SAME date on a fresh read
        assert desk.marks_latest_date(desk.read_marks(None)) == "2026-07-17"

    def test_fetch_window_can_never_collapse(self, env27b1):
        # ROOT CAUSE regression: the live failure fetched from the snapshot market
        # date ON the refresh day -> zero completed sessions. The fetch start must
        # always reach at least MIN_MARK_WINDOW_DAYS before the required date.
        _confirm_snapshot()
        starts = []
        table = _marks_table(_D0)

        def spy(symbol, start):
            starts.append(start)
            return table.get(symbol, [])

        r = desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN, downloader=spy,
                              today="2026-07-18")
        assert r["status"] == desk.S_OK
        assert starts, "downloader was never called"
        floor = "2026-07-03"  # required 2026-07-17 minus MIN_MARK_WINDOW_DAYS (14)
        assert all(s <= floor for s in starts), starts

    def test_null_date_is_blocked_never_green(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        r = desk.refresh_desk(confirm=desk.REFRESH_CONFIRM_TOKEN,
                              downloader=_dl({}), today="2026-07-18")
        assert r["status"] == "DESK_MARK_REFRESH_BLOCKED"
        assert r["resulting_desk_mark_date"] is None
        assert r["performed_write"] is False
        assert r["priced_ticker_count"] == 0
        assert r["missing_ticker_count"] == len(_TICKS)
        assert r["blockers"], "blockers must be explicit"
        assert r["next_action"] == "REPAIR_OR_REFRESH_MARK_SOURCE"
        assert "BLOCKED" in r["message"]
        assert "settlement" not in r and "performance" not in r
        # nothing was written anywhere
        assert desk.marks_latest_date(desk.read_marks(None)) is None
        sdir = desk._desk_dir(None)
        assert desk._fills(sdir) == []
        assert desk._orders_state(sdir) == {}

    def test_behind_required_date_is_blocked_with_honest_write_note(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        # today Wed 2026-07-22 -> required Tue 2026-07-21; the table only reaches
        # Mon 2026-07-20 -> the store is written but the refresh must NOT be green.
        r = _refresh("2026-07-22", _marks_table(_D0 + ["2026-07-20"]))
        assert r["status"] == "DESK_MARK_REFRESH_BLOCKED"
        assert r["resulting_desk_mark_date"] == "2026-07-20"
        assert r["latest_completed_market_date"] == "2026-07-21"
        assert r["performed_write"] is True
        assert "write_note" in r
        assert any("DESK_MARK_DATE_BEHIND_REQUIRED" in b for b in r["blockers"])
        assert r["next_action"] == "REPAIR_OR_REFRESH_MARK_SOURCE"
        assert "settlement" not in r

    def test_missing_ticker_blockers_are_explicit(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        r = _refresh("2026-07-18", _marks_table(_D0, drop=("HHH",)))
        assert r["status"] == desk.S_OK          # date criteria pass; coverage honest
        assert r["missing_tickers"] == ["HHH"]
        assert r["missing_ticker_count"] == 1
        assert r["priced_ticker_count"] == len(_TICKS) - 1
        assert r["coverage_complete"] is False
        assert any(b.startswith("TICKER_MARKS_MISSING: HHH") for b in r["blockers"])

    def test_refresh_writes_no_orders_fills_holdings_and_never_reconfirms(self, env27b1):
        _confirm_snapshot()
        n_before = ledger.list_snapshots(None)["n_confirmed"]
        _init_book("2026-07-18")
        r = _refresh("2026-07-18")
        assert r["status"] == desk.S_OK
        sdir = desk._desk_dir(None)
        assert desk._fills(sdir) == []
        assert desk._orders_state(sdir) == {}
        book = desk.open_book(sdir)
        _cash, held = desk.book_cash_holdings(book, [])
        assert held == {}
        assert ledger.list_snapshots(None)["n_confirmed"] == n_before
        # offline transport only - no GCP / prediction dependency
        assert r["marks"]["source"] == "INJECTED"

    def test_order_plan_readiness_gates_on_aligned_complete_marks(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        rd = ab.load_desk_mark_readiness()
        assert rd["desk_mark_status"] == "DESK_MARK_MISSING"
        assert rd["order_plan_ready"] is False
        assert rd["next_action"] == "REFRESH_DESK"
        _refresh("2026-07-18")
        rd2 = ab.load_desk_mark_readiness()
        assert rd2["desk_mark_status"] == "DESK_MARK_READY"
        assert rd2["desk_mark_date"] == "2026-07-17"
        assert rd2["latest_completed_market_date"] == "2026-07-17"
        assert rd2["priced_ticker_count"] == len(_TICKS)
        assert rd2["order_plan_ready"] is True
        assert rd2["next_action"] == "GENERATE_ORDER_PLAN"

    def test_partial_coverage_keeps_order_plan_not_ready(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        _refresh("2026-07-18", _marks_table(_D0, drop=("HHH",)))
        rd = ab.load_desk_mark_readiness()
        assert rd["missing_tickers"] == ["HHH"]
        assert rd["order_plan_ready"] is False
        assert rd["next_action"] == "REFRESH_DESK"

    def test_endpoint_refresh_contract_and_persistence(self, env27b1, client):
        _confirm_snapshot()
        _init_book("2026-07-18")
        r = client.post("/v1/paper-desk/refresh",
                        json={"confirm": desk.REFRESH_CONFIRM_TOKEN}, headers=_AUTH)
        assert r.status_code == 200
        d = r.json()
        assert d["status"] == "PAPER_DESK_OK"
        assert d["resulting_desk_mark_date"] == "2026-07-17"
        assert d["latest_completed_market_date"] == "2026-07-17"
        assert d["priced_ticker_count"] == len(_TICKS)
        # the persisted state survives a fresh service read
        g = client.get("/v1/alpha-book/desk-mark-readiness", headers=_AUTH)
        assert g.status_code == 200
        gd = g.json()
        assert gd["status"] == "ALPHA_DESK_MARK_READINESS_OK"
        assert gd["desk_mark_date"] == "2026-07-17"
        assert gd["desk_mark_status"] == "DESK_MARK_READY"
        assert gd["order_plan_ready"] is True

    def test_readiness_endpoint_requires_auth_and_is_get_only(self, env27b1, client):
        assert client.get("/v1/alpha-book/desk-mark-readiness").status_code in (401, 403)
        methods = [sorted(r.methods - {"HEAD", "OPTIONS"}) for r in app.routes
                   if getattr(r, "path", "") == "/v1/alpha-book/desk-mark-readiness"]
        assert methods == [["GET"]]

    def test_readiness_is_read_only(self, env27b1):
        _confirm_snapshot()
        before = desk.read_marks(None)
        rd = ab.load_desk_mark_readiness()
        assert rd["performed_write"] is False
        assert desk.read_marks(None) == before


# --------------------------------------------------------------------------- #
# PORTFOLIO MANAGER (Workstream C)
# --------------------------------------------------------------------------- #
class TestPortfolioManagerSemantics:
    def test_confirmed_unimplemented_never_no_change(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        s = pm.load_summary()
        assert s["decision_headline"] == pm.HEADLINE_IMPLEMENTATION_PENDING
        assert s["decision_headline"] != pm.HEADLINE_NO_CHANGE
        assert "0 of %d" % len(_TICKS) in s["decision_reason"]
        assert "does NOT mean" in s["decision_reason"]
        assert s["operational_book"]["next_action_code"] == "REFRESH_DESK"
        assert s["implementation_state"]["implementation_count"] == 0
        assert s["implementation_state"]["order_plan_generated"] is False

    def test_ready_for_order_plan_after_valid_marks(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        _refresh("2026-07-18")
        s = pm.load_summary()
        assert s["decision_headline"] == pm.HEADLINE_READY_FOR_ORDER_PLAN
        assert s["operational_book"]["order_plan_ready"] is True

    def test_headline_vocabulary_extended(self, env27b1):
        assert pm.HEADLINE_IMPLEMENTATION_PENDING in pm.ALL_HEADLINES
        assert pm.HEADLINE_READY_FOR_ORDER_PLAN in pm.ALL_HEADLINES

    def test_legacy_risk_never_populates_alpha_health(self, env27b1, monkeypatch):
        _confirm_snapshot()
        _init_book("2026-07-18")
        _refresh("2026-07-18")

        def _boom():
            raise RuntimeError("db unreachable")

        monkeypatch.setattr(pm, "_VALUATION_LOADER", _boom)
        h = pm.load_health()
        assert h["default_health_scope"] == "ALPHA_BOOK"
        assert all(i["scope"] == "ALPHA_BOOK" for i in h["alpha_book_items"])
        # the legacy valuation failure blocks the LEGACY rollup, never the alpha one
        assert any(i["status"] == pm.HEALTH_BLOCKED and i["scope"] == "LEGACY_ARCHIVE"
                   for i in h["items"])
        assert h["alpha_book_health_status"] != pm.HEALTH_BLOCKED
        assert all(i["scope"] in ("LEGACY_ARCHIVE", "MODEL_TARGET") for i in h["items"])

    def test_alpha_health_flags_implementation_and_marks(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        h = pm.load_health()
        by_key = {i["key"]: i for i in h["alpha_book_items"]}
        assert by_key["alpha_implementation"]["status"] == pm.HEALTH_REVIEW
        assert "0 of %d" % len(_TICKS) in by_key["alpha_implementation"]["value"]
        assert by_key["alpha_desk_marks"]["status"] == pm.HEALTH_REVIEW
        assert by_key["alpha_ledger_integrity"]["status"] == pm.HEALTH_HEALTHY

    def test_model_hold_distinguished_from_executed_holdings(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        a = pm.load_actions()
        assert "hold_semantics_note" in a
        assert "does NOT mean" in a["hold_semantics_note"]
        rows = a["actions"]
        assert rows, "expected model action rows"
        assert all(r["in_operational_holdings"] is False for r in rows)


# --------------------------------------------------------------------------- #
# UI STATIC - the true cutover (Workstreams D/E/F/G/H/I)
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def html():
    return _html()


class TestUiDailyWorkflowCutover:
    def test_operational_stage_labels(self, html):
        card = html[html.index('id="dwob-card"'):html.index("OPERATIONAL BOOK WORKFLOW END")]
        for label in ("Refresh Desk Marks", "Verify Alpha Target", "Generate Order Plan",
                      "Review &amp; Confirm Paper Orders",
                      "Monitor Fills, Holdings &amp; Performance"):
            assert label in card, label

    def test_legacy_workflow_collapsed_in_archive(self, html):
        i_arc = html.index('<details id="dw-legacy-archive">')
        i_dw = html.index('id="dw-terminal"')
        i_end = html.index("end dw-legacy-archive")
        assert i_arc < i_dw < i_end
        assert "Legacy Signal Workflow Archive" in html
        # collapsed by default - the details tag carries no open attribute
        assert '<details id="dw-legacy-archive" open' not in html

    def test_legacy_deep_links_open_the_archive(self, html):
        js = _scripts(html)
        assert "getElementById('dw-legacy-archive')" in js
        assert "dwArc.open = true" in js

    def test_no_remote_prediction_cta_in_operational_card(self, html):
        card = html[html.index('id="dwob-card"'):html.index("OPERATIONAL BOOK WORKFLOW END")]
        for bad in ("prediction", "GCP", "tunnel"):
            assert bad.lower() not in card.lower(), bad

    def test_stepper_statuses_come_from_canonical_payload(self, html):
        js = _scripts(html)
        body = js[js.index("function renderOperationalBook"):js.index("window.renderOperationalBook")]
        assert "workflow_stages" in body
        for code in ("REFRESH_DESK_MARKS", "VERIFY_ALPHA_TARGET", "GENERATE_ORDER_PLAN",
                     "CONFIRM_PAPER_ORDERS", "MONITOR"):
            assert code in body, code


class TestUiHeaderAndRightPanel:
    def test_header_owned_by_operational_payload(self, html):
        js = _scripts(html)
        fn = js[js.index("function _renderMarketDataBadge"):]
        fn = fn[:fn.index("}\n")] if "}\n" in fn else fn
        assert "window._obData" in fn
        body = js[js.index("function renderOperationalBook"):js.index("window.renderOperationalBook")]
        assert "'OPERATIONAL: '" in body
        assert "header_status" in body

    def test_right_panel_operational_values(self, html):
        for rid in ("right-ob-state", "right-ob-nav", "right-ob-holdings",
                    "right-ob-pending", "right-ob-target", "right-ob-mark"):
            assert 'id="%s"' % rid in html, rid

    def test_legacy_capacity_collapsed(self, html):
        assert '<details class="panel-section" id="right-legacy-capacity">' in html
        arc = html[html.index('id="right-legacy-capacity"'):html.index('id="cap-status-note"')]
        for cid in ("cap-open-positions", "cap-max-positions", "cap-available-slots"):
            assert cid in arc, cid

    def test_legacy_position_status_scoped(self, html):
        assert "Legacy: Paper position open (archived)" in html
        assert ">Paper position open<" not in html

    def test_legacy_task_writers_yield_to_operational_payload(self, html):
        js = _scripts(html)
        assert js.count("window._obData ? null : document.getElementById('right-current-task')") >= 3
        assert "function _obTaskLabel" in js
        fn = js[js.index("function applyCanonicalToActionPanel"):]
        fn = fn[:fn.index("// ----- Single navigation contract")]
        assert "if (window._obData) return;" in fn


class TestUiPortfolioAndCommandCenter:
    def test_portfolio_operational_body_fields(self, html):
        card = html[html.index('id="ptob-card"'):html.index('<details id="pt-archive">')]
        for pid in ("ptob-cash", "ptob-nav", "ptob-holdings", "ptob-pending",
                    "ptob-fills", "ptob-target", "ptob-mark", "ptob-impl",
                    "ptob-status", "ptob-blockers", "ptob-holdings-table"):
            assert pid in card, pid
        for empty in ("No Alpha holdings yet", "No Alpha orders yet",
                      "No Alpha fills yet", "initial implementation pending"):
            assert empty in card, empty

    def test_portfolio_primary_body_has_no_legacy_positions(self, html):
        card = html[html.index('id="ptob-card"'):html.index('<details id="pt-archive">')]
        assert "CDW" not in card and "HUM" not in card

    def test_command_center_panel_fields(self, html):
        panel = html[html.index('id="cc-ob-panel"'):html.index("CURRENT OPERATIONAL BOOK END")]
        for pid in ("cc-ob-cash", "cc-ob-nav", "cc-ob-holdings", "cc-ob-pending",
                    "cc-ob-fills", "cc-ob-target", "cc-ob-mark", "cc-ob-impl",
                    "cc-ob-status"):
            assert pid in panel, pid

    def test_pm_implementation_strip(self, html):
        strip = html[html.index('id="pm-impl-strip"'):html.index('id="pm-statusbar"')]
        for pid in ("pm-impl-target", "pm-impl-count", "pm-impl-marks",
                    "pm-impl-holdings", "pm-impl-next"):
            assert pid in strip, pid
        assert "Alpha Paper Book #1" in strip

    def test_desk_refresh_blocked_never_rendered_green(self, html):
        js = _scripts(html)
        fn = js[js.index("async function pdDoAction"):js.index("async function pdExecutionPreview")]
        assert "DESK_MARK_REFRESH_BLOCKED" in fn
        assert "resulting_desk_mark_date" in fn
        assert "REPAIR_OR_REFRESH_MARK_SOURCE" in fn


class TestUiResearchTerminology:
    def test_research_champion_never_a_visible_current_paper_champion_badge(self, html):
        # the literal survives only inside explanatory title attributes
        assert ">CURRENT PAPER CHAMPION<" not in html
        assert "RESEARCH CHAMPION" in html

    def test_operational_strategy_identity_strip(self, html):
        strip = html[html.index('id="ra-identity-strip"'):]
        strip = strip[:strip.index("</div>\n        </div>") + 20]
        assert "RESEARCH CHAMPION" in strip
        assert "OPERATIONAL STRATEGY" in strip and "fundamental_momentum_50_50_v1" in strip
        assert "OPERATIONAL TARGET" in strip and "fundamental_momentum_50_50_top25" in strip

    def test_alpha_portfolio_model_target_not_executed(self, html):
        strip = html[html.index('id="mhz-safety-strip"'):html.index('id="mhz-load-note"')]
        assert "MODEL TARGET / RESEARCH PORTFOLIO" in strip
        assert "NOT EXECUTED HOLDINGS" in strip


class TestUiQuality:
    def test_no_native_dialogs(self, html):
        js = _scripts(html)
        assert len(re.findall(r"(?<![A-Za-z0-9_.])alert\(", js)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_.])confirm\(", js)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_.])prompt\(", js)) == 0

    def test_no_blank_buttons_in_new_regions(self, html):
        for start, end in ((('id="dwob-card"'), "OPERATIONAL BOOK WORKFLOW END"),
                           (('id="ptob-card"'), '<details id="pt-archive">'),
                           (('id="cc-ob-panel"'), "CURRENT OPERATIONAL BOOK END")):
            region = html[html.index(start):html.index(end)]
            for m in re.finditer(r"<button[^>]*>(.*?)</button>", region, re.S):
                assert m.group(1).strip(), "blank button in region %s" % start

    def test_operational_book_endpoint_still_single_fetch(self, html):
        js = _scripts(html)
        assert js.count("/v1/operational-book") == 1
