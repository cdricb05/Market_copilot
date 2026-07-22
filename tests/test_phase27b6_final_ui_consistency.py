"""
tests/test_phase27b6_final_ui_consistency.py - Phase 27B.6 final operator UI
consistency and pruning.

Fully offline (Phase 27A/27B.1 harness: owned-style CSV fixtures, tmp desk /
ledger dirs, injectable marks downloader, deterministic clock seams). Order
creation and submission happen ONLY against the isolated tmp desk stores of
this harness - never against the user's real development book; no live broker
activity, no automation, no signals.

Covers the 18 acceptance requirements of the 27B.6 brief:

 1. SUBMITTED lifecycle produces the standard display vocabulary.
 2. The right panel never says "No Orders" while submitted orders exist.
 3. "ORDERS DISABLED" is invisible in operator-facing submitted-state content.
 4. Command Center has ONE canonical dominant next action.
 5. Command Center never visibly recommends "Monitor the paper portfolio".
 6. The Daily Workflow legacy section is closed by default.
 7. Start Daily Review lives inside the collapsed legacy/research section.
 8. Completed Alpha workflow stages carry no visible CTA.
 9. Portfolio Manager has exactly one expanded submitted-order table.
10. Duplicate desk/implementation controls sit inside a collapsed advanced
    section ("Advanced Order & Execution Details").
11. The Portfolio legacy terminal is collapsed by default.
12. The right panel compact state agrees with the canonical counts.
13. Alpha Portfolio has one operational-book link and no operational
    confirmation controls.
14. Research & Audit cannot override the operational next action.
15. No native alert(), confirm() or prompt().
16. Page loads perform no writes.
17. The submitted orders remain untouched by read-only surfaces.
18. Legacy CDW/HUM positions are never counted as Alpha holdings.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import operational_book as ob_mod
from paper_trader.api import paper_trading_desk as desk

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _TICKS,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1,  # noqa: F401
)
from tests.test_phase27b5_operator_flow import (  # canonical world builders
    _submitted_world, _cs,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"
_N = len(_TICKS)


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


@pytest.fixture(scope="module")
def static_html(html) -> str:
    """Markup with script bodies blanked (same length, indices preserved) so
    text-visibility assertions never match JS string literals."""
    return re.sub(
        r"(?s)(<script[^>]*>)(.*?)(</script>)",
        lambda m: m.group(1) + " " * len(m.group(2)) + m.group(3),
        html)


def _region(html: str, start: str, end: str) -> str:
    return html[html.index(start):html.index(end)]


def _span(html: str, start: str, end: str):
    return html.index(start), html.index(end)


# --------------------------------------------------------------------------- #
# 1. Standard display vocabulary for the SUBMITTED lifecycle
# --------------------------------------------------------------------------- #
class TestStandardVocabulary:
    def test_backend_submitted_vocabulary(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["primary_headline"] == (
            "%d PAPER ORDERS SUBMITTED — AWAITING NEXT ELIGIBLE CLOSE" % _N)
        assert cs["next_action_label"] == "Refresh After Market Close"
        assert cs["current_task_label"] == "Await Next Eligible Close"

    def test_ui_maps_status_to_orders_submitted(self, js):
        # the ONE view model owns the standardized wording
        assert "var statusLabel = stage === 'SUBMITTED' ? 'ORDERS SUBMITTED'" in js
        assert "'AWAITING NEXT ELIGIBLE CLOSE'" in js
        assert "? 'PAPER ORDERS SUBMITTED'" in js

    def test_every_state_badge_renders_the_display_vocabulary(self, js):
        assert "var displayStateTxt = (ob && view.statusLabel) ? view.statusLabel : stateTxt;" in js
        assert "badge.textContent = displayStateTxt;" in js          # cc-ob-state / ptob-state
        assert "dwState.textContent = displayStateTxt;" in js        # dwob-state
        assert "_obSet('right-ob-state', displayStateTxt);" in js    # right panel
        assert "_obSet(prefix + '-status', displayStatusTxt);" in js  # status rows

    def test_header_badge_states_the_lifecycle(self, js):
        # "OPERATIONAL: PAPER ORDERS SUBMITTED", never the pending/confirmed jargon
        assert "hb.textContent = 'OPERATIONAL: ' + view.headerLabel;" in js

    def test_internal_workflow_state_not_renamed(self, env27b1):
        # the persisted backend state stays ORDERS_CONFIRMED - display only
        _submitted_world()
        cs = _cs()
        assert cs["workflow_state"] == "ORDERS_CONFIRMED"


# --------------------------------------------------------------------------- #
# 2. Right panel / footer never claim "No Orders" while orders exist
# --------------------------------------------------------------------------- #
class TestNeverNoOrders:
    def test_old_static_footer_is_gone(self, html):
        assert "Manual Review &nbsp;|&nbsp; No Orders" not in html

    def test_footer_is_dynamic_and_lifecycle_aware(self, html, js):
        assert 'id="right-safety-footer"' in html
        assert "' Paper Orders Submitted  |  No Live Orders  |  Automation Off')" in js
        assert "_obSet('right-safety-footer', view.footerLine);" in js

    def test_footer_default_never_implies_no_paper_orders(self, static_html):
        region = _region(static_html, 'id="right-safety-footer"', 'id="toast"')
        assert "Paper Orders Only" in region and "No Live Orders" in region
        assert "No Orders " not in region

    def test_right_panel_static_markup_has_no_no_orders(self, static_html):
        panel = _region(static_html, '<div class="right-panel">', 'id="toast"')
        assert ">No Orders<" not in panel
        assert ">NO ORDERS<" not in panel


# --------------------------------------------------------------------------- #
# 3. "ORDERS DISABLED" invisible in operator-facing content
# --------------------------------------------------------------------------- #
class TestOrdersDisabledInvisible:
    def test_no_visible_orders_disabled_anywhere(self, static_html):
        # Phase 27B.7: the legacy CC archive that held the one visible ">ORDERS
        # DISABLED<" node was removed; the phrase now survives only inside title=
        # tooltips, never as visible text.
        assert ">ORDERS DISABLED<" not in static_html

    def test_pm_and_alpha_pages_say_no_live_broker_orders(self, static_html):
        # Phase 27B.7 text rule: NO LIVE ORDERS -> NO LIVE BROKER ORDERS.
        pm = _region(static_html, 'id="tab-portfolio-manager"', 'id="tab-multi-horizon"')
        mhz = _region(static_html, 'id="tab-multi-horizon"', 'id="tab-audit-advanced"')
        assert ">NO LIVE BROKER ORDERS</span>" in pm
        assert ">NO LIVE BROKER ORDERS</span>" in mhz
        assert ">ORDERS DISABLED<" not in pm
        assert ">ORDERS DISABLED<" not in mhz

    def test_underlying_safety_behavior_unchanged(self, env27b1, client):
        _submitted_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["live_orders_enabled"] is False
        assert d["broker_enabled"] is False


# --------------------------------------------------------------------------- #
# 4/5. Command Center: one dominant action, no competing recommendation
# --------------------------------------------------------------------------- #
class TestCommandCenterOneAction:
    def test_exactly_one_canonical_primary_action(self, html):
        panel = _region(html, 'id="cc-ob-panel"', "CURRENT OPERATIONAL BOOK END")
        assert panel.count('onclick="obPrimaryAction()"') == 1
        assert 'id="cc-ob-primary-btn"' in panel

    def test_no_monitor_the_paper_portfolio_recommendation(self, html):
        assert "Monitor the paper portfolio" not in html

    def test_no_monitor_portfolio_recommendation_anywhere(self, static_html):
        # Phase 27B.7: the legacy "Monitor Portfolio" recommendation was removed
        # with the legacy archives.
        assert ">Monitor Portfolio<" not in static_html

    def test_lifecycle_headline_owns_the_card(self, js):
        assert "if (ob && view.ordersExist && view.headline) obHeadline.textContent = view.headline;" in js


# --------------------------------------------------------------------------- #
# 6/7. Daily Workflow: legacy collapsed, Start Daily Review inside it
# --------------------------------------------------------------------------- #
class TestDailyWorkflowLegacyCollapsed:
    def test_legacy_archive_removed(self, html):
        # Phase 27B.7 hard cutover: the legacy signal-workflow archive is gone.
        assert 'id="dw-legacy-archive"' not in html

    def test_no_start_daily_review_control_on_the_route(self, html):
        # The visible Start Daily Review control lived in the removed archive.
        assert 'id="dp-review-control-card"' not in html
        assert 'id="start-daily-review-workspace"' not in html

    def test_operational_stepper_is_the_only_workflow(self, html):
        assert 'id="dwob-card"' in html
        assert 'id="dw-legacy-archive"' not in html


# --------------------------------------------------------------------------- #
# 8. Completed stages carry no visible CTA
# --------------------------------------------------------------------------- #
class TestCompletedStagesRecede:
    def test_complete_stage_buttons_hide(self, js):
        assert "if (row.status === 'COMPLETE') {" in js
        assert "sbtn.style.display = 'none';" in js

    def test_active_stage_carries_the_one_primary_label(self, js):
        assert "if (row.status === 'ACTIVE' && view.ordersExist)" in js
        assert "sbtn.textContent = primaryLabel;" in js


# --------------------------------------------------------------------------- #
# 9/10. Portfolio Manager: one order table, advanced details collapsed
# --------------------------------------------------------------------------- #
class TestPmSingleOrderTableAndAdvanced:
    def test_lifecycle_order_table_outside_the_advanced_section(self, html):
        assert html.index('id="pm-lc-orders"') < html.index('id="pm-adv-exec"')

    def test_advanced_order_and_execution_details_collapsed(self, html):
        m = re.search(r'<details[^>]*id="pm-adv-exec"[^>]*>', html)
        assert m and " open" not in m.group(0)
        assert "Advanced Order &amp; Execution Details" in html

    def test_duplicated_bands_live_inside_the_advanced_section(self, html):
        lo, hi = _span(html, 'id="pm-adv-exec"', "end pm-adv-exec")
        # Phase 27B.7: pd-audit (raw payload / ledger-integrity JSON) was removed.
        for el in ('id="otr-band"', 'id="ab-band"', 'id="pd-band"',
                   'id="pd-act-refresh"', 'id="pd-act-confirm"',
                   'id="pd-act-preview"', 'id="pd-act-cancel"',
                   'id="otr-act-refresh"', 'id="otr-act-confirm"',
                   'id="pd-pills"', 'id="pd-panel"'):
            assert lo < html.index(el) < hi, el

    def test_lifecycle_actions_auto_open_the_advanced_section(self, js):
        assert "function pmOpenAdvExec" in js
        assert js.count("pmOpenAdvExec();") >= 3   # primary, plan path, cancel

    def test_desk_functionality_preserved_not_deleted(self, html):
        lo, hi = _span(html, 'id="pm-adv-exec"', "end pm-adv-exec")
        adv = html[lo:hi]
        # Phase 27B.7: the raw Desk Audit dump was removed; the desk actions stay.
        for kept in ("pdAskAction('refresh')", "pdAskAction('cancel')",
                     "otrAskRefresh()", "abGeneratePlan()"):
            assert kept in adv, kept


# --------------------------------------------------------------------------- #
# 11. Portfolio legacy terminal collapsed + compact summary
# --------------------------------------------------------------------------- #
class TestPortfolioLegacyCollapsed:
    def test_legacy_terminal_removed(self, html):
        # Phase 27B.7 hard cutover: the legacy Portfolio Terminal archive is gone.
        assert 'id="pt-archive"' not in html
        assert 'id="pt-terminal"' not in html

    def test_operational_card_is_the_portfolio(self, html):
        # Phase 27B.8: the Portfolio route is the operational holdings dashboard
        # (Alpha Paper Book #1) — a real holdings table, not the legacy archive.
        assert 'id="pdash-table"' in html
        assert 'id="ptob-state"' in html
        assert 'id="pt-archive"' not in html


# --------------------------------------------------------------------------- #
# 12. Right panel compact card agrees with the canonical counts
# --------------------------------------------------------------------------- #
class TestRightPanelCompactAgrees:
    def test_counts_agree_with_the_canonical_payload(self, env27b1, client):
        _submitted_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        cs = d["canonical_state"]
        assert cs["submitted_count"] == _N
        assert cs["fill_count"] == 0
        assert cs["holdings_count"] == 0

    def test_statement_built_from_the_same_view_model(self, js):
        assert "' paper orders submitted'" in js
        assert "' Alpha holdings'" in js
        assert "_obSet('right-safety-footer', view.footerLine);" in js

    def test_detail_rows_collapsed_into_operational_details(self, html):
        m = re.search(r'<details[^>]*id="right-op-details"[^>]*>', html)
        assert m and " open" not in m.group(0)
        lo, hi = _span(html, 'id="right-op-details"', "end right-op-details")
        # Phase 27B.7: right-legacy-capacity was removed; operational detail rows stay.
        for el in ('id="right-ob-nav"', 'id="right-ob-cash"', 'id="right-ob-target"',
                   'id="right-ob-mark"', 'id="right-ob-impl"',
                   'id="right-create-orders-section"'):
            assert lo < html.index(el) < hi, el

    def test_compact_card_stays_visible_above_the_details(self, html):
        for el in ('id="right-ob-statement"', 'id="right-current-task"',
                   'id="right-next-action"', 'id="right-primary-action-btn"',
                   'id="right-safety-line"'):
            assert html.index(el) < html.index('id="right-op-details"'), el


# --------------------------------------------------------------------------- #
# 13. Alpha Portfolio: one operational link, no operational confirmations
# --------------------------------------------------------------------------- #
class TestAlphaPortfolioOneLink:
    def test_one_open_operational_book_link(self, html):
        mhz = _region(html, 'id="tab-multi-horizon"', 'id="tab-audit-advanced"')
        assert mhz.count(">Open Operational Book</button>") == 1

    def test_no_operational_confirmation_controls(self, html):
        mhz = _region(html, 'id="tab-multi-horizon"', 'id="tab-audit-advanced"')
        for bad in ('obPrimaryAction()', "pdAskAction(", "abAskAction(",
                    "otrAskConfirm("):
            assert bad not in mhz, bad

    def test_model_target_labelling_kept(self, html):
        mhz = _region(html, 'id="tab-multi-horizon"', 'id="tab-audit-advanced"')
        assert "MODEL TARGET / RESEARCH PORTFOLIO" in mhz
        assert "NOT EXECUTED HOLDINGS" in mhz


# --------------------------------------------------------------------------- #
# 14. Research & Audit cannot override the operational next action
# --------------------------------------------------------------------------- #
class TestResearchNeverOverrides:
    def test_research_failure_keeps_the_lifecycle(self, env27b1, monkeypatch):
        _submitted_world()
        def _boom():
            raise RuntimeError("legacy/research store unavailable")
        monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _boom)
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["next_action_label"] == "Refresh After Market Close"

    def test_research_page_has_no_operational_cta(self, html):
        ra = _region(html, 'id="tab-audit-advanced"', "end tab-audit-advanced")
        assert 'obPrimaryAction()' not in ra
        assert "pdAskAction(" not in ra


# --------------------------------------------------------------------------- #
# 15. No native browser dialogs
# --------------------------------------------------------------------------- #
class TestNoNativeDialogs:
    def test_no_alert_confirm_prompt(self, js):
        for pat in (r"(?<![A-Za-z0-9_.])alert\(",
                    r"(?<![A-Za-z0-9_.])confirm\(",
                    r"(?<![A-Za-z0-9_.])prompt\("):
            assert not re.search(pat, js), pat


# --------------------------------------------------------------------------- #
# 16/17/18. Read-only loads, untouched orders, legacy separation
# --------------------------------------------------------------------------- #
class TestReadOnlyAndLegacySeparation:
    def test_page_loads_write_nothing_and_orders_stay_untouched(self, env27b1, client):
        _submitted_world()
        before = desk.load_orders()["counts_by_status"]
        fills_before = desk.load_fills()["n_fills"]
        for path in ("/v1/operational-book", "/v1/alpha-book/status",
                     "/v1/paper-desk/status", "/v1/paper-desk/orders",
                     "/v1/paper-desk/fills", "/v1/portfolio-manager/summary"):
            r = client.get(path, headers=_AUTH)
            assert r.status_code == 200, path
            body = r.json()
            if "performed_write" in body:
                assert body["performed_write"] is False, path
        after = desk.load_orders()["counts_by_status"]
        assert after == before
        assert after.get("SUBMITTED", 0) == _N
        assert desk.load_fills()["n_fills"] == fills_before == 0

    def test_legacy_positions_never_counted_as_alpha(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["holdings_count"] == 0
        legacy = cs["legacy_archive_summary"]
        assert legacy["positions_count"] == 2
        assert set(legacy["tickers"]) == {"CDW", "HUM"}
        assert cs["submitted_count"] == _N     # alpha counts exclude legacy

    def test_footer_counts_come_from_alpha_orders_only(self, js):
        # the footer line is built exclusively from canonical submitted/proposed
        # counts - the legacy archive payload never feeds it
        i = js.index("var footerLine =")
        block = js[i:i + 600]
        assert "submitted_count" in js[js.index("var nSubmitted"):js.index("var footerLine")]
        assert "legacy" not in block
