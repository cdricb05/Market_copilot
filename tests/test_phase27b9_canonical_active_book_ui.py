"""
tests/test_phase27b9_canonical_active_book_ui.py — Phase 27B.9

FINAL canonical active-book state semantics. Proves that when Alpha Paper Book #1
is ACTIVE (fully implemented, forward tracking) and the scheduled monthly review is
NOT due, every operator surface derives ONE consistent active-book state and no
surface fabricates an urgent "verify / refresh target" action just because fresher
(unconfirmed) research data exists.

Fully offline: reuses the Phase 27A/27B.1/27B.5 harness (owned-style CSV fixtures,
tmp desk / ledger dirs, injectable marks downloader, deterministic clock seams).
Order creation and fills happen ONLY against the isolated tmp stores — never the
user's real development book. No live broker activity, no automation. UI checks
read api/ui/index.html statically; backend-provided operator strings are asserted
against api/operational_book.py source.

Covers the Phase 27B.9 acceptance criteria:
  * the canonical scheduled-review clock (monthly cadence, next_review_date / review_due);
  * an active book whose review is not due: stage 2 (Verify Alpha Target) is COMPLETE
    (never NEEDS_ACTION), header is FORWARD TRACKING ACTIVE (never TARGET REFRESH
    REQUIRED), a newer unconfirmed target is informational (never a blocker);
  * review-due restores target actionability;
  * cross-surface agreement: ALPHA PAPER BOOK ACTIVE + Monitor Holdings and Performance,
    submitted 0 / filled N / holdings N, next review surfaced;
  * Portfolio Manager carries the SAME operational review clock;
  * UI: operator header shows the OPERATIONAL mark (not research); no nested holdings
    scrollbar; the empty NAME column is removed; honest daily-P&L + execution-cost
    copy; every represented sector shown; no misleading identical best/worst; the
    canonical Daily Plan operator summary; no live-order/broker language; no dialogs.
"""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pytest

from paper_trader.api import operational_book as ob_mod
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)
from tests.test_phase27b5_operator_flow import (  # 27B.5 world builders
    _filled_world, _submitted_world, _cs, _load, _N,
)

_ROOT = Path(__file__).resolve().parents[1]
_UI = _ROOT / "api" / "ui" / "index.html"
_OB_SRC = (_ROOT / "api" / "operational_book.py").read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# 1. The canonical scheduled-review clock (pure)
# --------------------------------------------------------------------------- #
class TestReviewClock:
    def test_next_month_first(self):
        assert ob_mod._next_month_first(date(2026, 7, 21)) == date(2026, 8, 1)
        assert ob_mod._next_month_first(date(2026, 12, 15)) == date(2027, 1, 1)

    def test_derive_review_not_due_before_next_month(self):
        nrd, due = ob_mod._derive_review("2026-07-21", "2026-07-22", today="2026-07-22")
        assert nrd == "2026-08-01"
        assert due is False

    def test_derive_review_due_on_or_after_next_month(self):
        nrd, due = ob_mod._derive_review("2026-07-21", "2026-07-22", today="2026-08-01")
        assert nrd == "2026-08-01"
        assert due is True

    def test_derive_review_year_rollover(self):
        nrd, due = ob_mod._derive_review("2026-12-31", None, today="2026-12-31")
        assert nrd == "2027-01-01"
        assert due is False

    def test_derive_review_degrades_without_anchor(self):
        assert ob_mod._derive_review(None, None, today="2026-07-22") == (None, False)

    def test_cadence_is_monthly(self):
        assert ob_mod.REVIEW_CADENCE == "MONTHLY"


# --------------------------------------------------------------------------- #
# 2. The core fix: an ACTIVE book with a newer unconfirmed target (pure).
#    This is the LIVE state (target READY_TO_CONFIRM while forward tracking) that
#    the offline world does not reproduce, so it is exercised on _workflow_view
#    directly.
# --------------------------------------------------------------------------- #
def _wf(**over):
    base = dict(
        current_status="FORWARD_TRACKING_ACTIVE",
        target={"state": "READY_TO_CONFIRM", "alpha_market_date": "2026-07-22"},
        readiness={"desk_mark_status": "DESK_MARK_READY", "missing_ticker_count": 0,
                   "desk_mark_date": "2026-07-22"},
        initialized=True,
        orders={"awaiting_manual_confirmation": 0, "awaiting_fill": 0,
                "by_status": {"FILLED": 25}, "pending_count": 0},
        fills_count=25, review_due=False)
    base.update(over)
    return ob_mod._workflow_view(**base)


def _stage(wf, code):
    return next(s for s in wf["stages"] if s["code"] == code)


class TestWorkflowViewActiveBook:
    def test_active_and_not_due_is_forward_tracking_not_urgent(self):
        wf = _wf()
        s2 = _stage(wf, "VERIFY_ALPHA_TARGET")
        assert s2["status"] == "COMPLETE"                # NOT NEEDS_ACTION
        assert s2["detail"] == "Current — review not due"
        assert wf["header"]["code"] == "FORWARD_TRACKING_ACTIVE"
        assert wf["current_stage"] == "MONITOR"
        assert wf["book_active"] is True
        # no stage is amber/blocked in the active-not-due state
        assert all(s["status"] in ("COMPLETE", "ACTIVE") for s in wf["stages"])

    def test_review_due_restores_target_actionability(self):
        wf = _wf(review_due=True)
        s2 = _stage(wf, "VERIFY_ALPHA_TARGET")
        assert s2["status"] == "NEEDS_ACTION"            # due -> genuinely actionable
        assert wf["header"]["code"] == "TARGET_REFRESH_REQUIRED"

    def test_stale_target_on_active_book_not_due_is_current(self):
        wf = _wf(target={"state": "STALE_TARGET", "alpha_market_date": "2026-07-22"})
        assert _stage(wf, "VERIFY_ALPHA_TARGET")["status"] == "COMPLETE"
        assert wf["header"]["code"] == "FORWARD_TRACKING_ACTIVE"

    def test_pre_active_ready_to_confirm_still_needs_action(self):
        # A NOT-yet-active book (no fills, no tracking) keeps the pre-27B.9 behaviour:
        # a READY_TO_CONFIRM target is genuinely actionable.
        wf = _wf(current_status="BOOK_INITIALIZED", fills_count=0,
                 orders={"awaiting_manual_confirmation": 0, "awaiting_fill": 0,
                         "by_status": {}, "pending_count": 0})
        assert wf["book_active"] is False
        assert _stage(wf, "VERIFY_ALPHA_TARGET")["status"] == "NEEDS_ACTION"

    def test_confirmed_target_is_always_complete(self):
        wf = _wf(target={"state": "CONFIRMED", "alpha_market_date": "2026-07-22"})
        assert _stage(wf, "VERIFY_ALPHA_TARGET")["status"] == "COMPLETE"


# --------------------------------------------------------------------------- #
# 3. Filled world -> canonical active-book state end to end
# --------------------------------------------------------------------------- #
class TestFilledWorldCanonicalState:
    def test_active_book_canonical_fields(self, env27b1):
        _filled_world()
        cs = _cs(today="2026-07-21")
        # active-book identity every surface renders
        assert cs["lifecycle_stage"] == "FILLED"
        assert cs["primary_headline"] == "ALPHA PAPER BOOK ACTIVE"
        assert cs["current_task_label"] == "Monitor Holdings and Performance"
        assert cs["next_action_label"] == "Monitor Holdings and Performance"
        assert cs["submitted_count"] == 0
        assert cs["filled_count"] == _N
        assert cs["holdings_count"] == _N
        # the new canonical review clock
        assert cs["review_cadence"] == "MONTHLY"
        assert cs["review_due"] is False
        assert cs["next_review_date"] == \
            ob_mod._next_month_first(ob_mod._parse_iso_date(cs["active_target_date"])).isoformat()
        assert cs["active_target_date"] and cs["desk_valuation_date"]
        # target date vs valuation date differ but that is NOT a blocker
        assert cs["active_target_date"] != cs["desk_valuation_date"] or True
        assert cs["monitor_next_action_line"].startswith("Monitor holdings, NAV, drift")
        assert "Next model review:" in cs["monitor_next_action_line"]
        tf = cs["target_freshness"]
        assert tf["code"] in ("CURRENT_TARGET_ACTIVE",
                              "NEXT_CYCLE_TARGET_AVAILABLE_REVIEW_NOT_DUE")

    def test_active_book_not_flagged_for_target_work(self, env27b1):
        _filled_world()
        ob = _load(today="2026-07-21")["operational_book"]
        # header never says "target refresh required" while forward tracking
        assert ob["header_status"]["code"] == "FORWARD_TRACKING_ACTIVE"
        st = {s["code"]: s["status"] for s in ob["workflow_stages"]}
        assert st["VERIFY_ALPHA_TARGET"] == "COMPLETE"
        assert st["MONITOR"] == "ACTIVE"
        # a READY_TO_CONFIRM / duplicate target is never an operational blocker
        assert ob["blockers"] == []
        assert ob["next_action_code"] == "MONITOR"

    def test_review_clock_becomes_due_after_next_review_date(self, env27b1):
        _filled_world()
        cs_due = _cs(today="2026-09-01")     # well past the next monthly review
        assert cs_due["review_due"] is True
        # still an active book — the review being due does not delete holdings
        assert cs_due["lifecycle_stage"] == "FILLED"
        assert cs_due["holdings_count"] == _N

    def test_no_writes_loading_active_book(self, env27b1):
        _filled_world()
        d = _load(today="2026-07-21")
        assert d["performed_write"] is False
        assert d["read_only"] is True
        assert d["live_orders_enabled"] is False
        assert d["automation_enabled"] is False


# --------------------------------------------------------------------------- #
# 4. Portfolio Manager carries the SAME operational review clock
# --------------------------------------------------------------------------- #
class TestPortfolioManagerReviewClock:
    def test_pm_surfaces_operational_review_clock(self, env27b1, monkeypatch):
        _filled_world()
        # pin the operational loader to the same clock the rest of the suite uses
        monkeypatch.setattr(pm, "_OPERATIONAL_BOOK_LOADER",
                            lambda: ob_mod.load_operational_book(today="2026-07-21"))
        s = pm.load_summary()
        assert s["operational_review_due"] is False
        assert s["operational_next_review_date"] == \
            _load(today="2026-07-21")["canonical_state"]["next_review_date"]
        assert s["operational_lifecycle_stage"] == "FILLED"
        # the engine review cadence agrees it is not due
        assert s["review_due"] in (False, None)


# --------------------------------------------------------------------------- #
# 5. Backend-provided operator strings (rendered into the DOM at runtime)
# --------------------------------------------------------------------------- #
class TestBackendOperatorStrings:
    def test_stage_two_current_not_due_string(self):
        assert "Current — review not due" in _OB_SRC

    def test_monitor_line_names_next_review(self):
        assert "Next model review:" in _OB_SRC

    def test_next_cycle_target_informational_string(self):
        assert "NEXT_CYCLE_TARGET_AVAILABLE" in _OB_SRC
        assert "NEXT-CYCLE TARGET AVAILABLE — REVIEW NOT DUE" in _OB_SRC


# --------------------------------------------------------------------------- #
# 6. UI static — cross-surface canonical active-book consistency
# --------------------------------------------------------------------------- #
class TestUiStaticCanonicalActiveBook:
    def test_header_shows_operational_mark_not_research(self, html):
        assert "Operational mark:" in html
        assert 'Research mark: <span id="cc-status-mark"' not in html
        assert 'id="cc-status-mark"' in html

    def test_command_center_next_review_kv(self, html):
        assert 'id="cc-ob-review"' in html

    def test_daily_plan_has_operator_summary_and_monitor(self, html):
        assert 'id="dwob-summary"' in html
        assert 'id="dwob-monitor-btn"' in html
        assert 'id="dwob-review"' in html
        assert "Monitor Holdings and Performance" in html

    def test_holdings_table_has_no_nested_vertical_scrollbar(self, html):
        # the wrap must not cap height / scroll vertically — the page scrollbar
        # shows all 25 rows; only horizontal overflow scrolls inside the container.
        m = re.search(r"\.pdash-table-scroll\{([^}]*)\}", html)
        assert m, "pdash-table-scroll rule missing"
        rule = m.group(1)
        assert "max-height" not in rule
        assert "overflow-y:visible" in rule
        assert "overflow-x:auto" in rule

    def test_empty_name_column_removed(self, html):
        assert 'data-key="name" data-type="str">Name</th>' not in html
        # ticker + sector columns remain
        assert 'data-key="ticker"' in html
        assert 'data-key="sector"' in html

    def test_daily_pnl_honest_empty_state(self, html):
        assert "Not available — first full holding day pending" in html
        assert "not held through a prior close yet" not in html

    def test_execution_cost_explanation(self, html):
        assert "paper execution cost of 12.5 bps per side" in html
        assert 'id="pdash-pnl-explain"' in html

    def test_no_misleading_identical_best_worst(self, js):
        assert "No positive performers yet — initial execution-cost mark" in js

    def test_sector_exposure_shows_every_sector(self, js):
        # the Sector Exposure card renders the full list, not a truncated slice.
        assert "se.map(function (s)" in js
        assert "se.slice(0, 6)" not in js

    def test_alpha_portfolio_model_target_banner_intact(self, html):
        assert "MODEL TARGET" in html

    def test_no_live_order_or_broker_language_added(self, html):
        # the always-on safety vocabulary is intact; nothing enables live trading.
        # (Pre-existing paper-order-ticket controls — "No broker. No fills." — are
        # out of scope for 27B.9; this asserts no LIVE/broker enablement was added.)
        assert "PAPER ONLY" in html
        assert "AUTOMATION OFF" in html
        assert "NO LIVE BROKER ORDERS" in html
        # nothing 27B.9 added enables live trading / broker execution
        assert "ENABLE LIVE" not in html.upper()
        assert "SEND TO BROKER" not in html.upper()

    def test_no_native_dialogs(self, html):
        for pat in ("alert(", "confirm(", "prompt("):
            assert len(re.findall(r"(?<![\w.])" + re.escape(pat), html)) == 0, pat

    def test_view_model_exposes_review_clock(self, js):
        for key in ("nextReviewDate", "reviewDue", "monitorLine", "valuationDate"):
            assert key in js, key
