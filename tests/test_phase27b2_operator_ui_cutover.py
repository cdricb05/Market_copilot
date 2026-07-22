"""
tests/test_phase27b2_operator_ui_cutover.py - Phase 27B.2 final operator UI
single-source-of-truth cutover.

Fully offline (reuses the Phase 27A/27B.1 harness: owned-style CSV fixtures, tmp
desk/ledger dirs, injectable marks downloader, deterministic clock seams). Proves:

* CANONICAL STATE - /v1/operational-book carries the flat canonical_state contract
  (book name/status, NAV/cash, holdings/pending/fills, confirmed target, implemented
  counts, desk mark, order_plan_status, ONE next_required_action, blockers,
  safety_mode, legacy_archive_summary) and every operator surface agrees.
* PLAN-AWARE WORKFLOW - once the deterministic executable plan exists, stage 3 is
  COMPLETE ("Order plan ready"), stage 4 is NEEDS_ACTION, and the one canonical
  next action is REVIEW_AND_CONFIRM_ORDER_PLAN on every surface.
* DUPLICATE SNAPSHOT - on an already-CONFIRMED target the duplicate-snapshot code
  is informational, never an operational blocker.
* LEGACY SEPARATION - the operational book has zero holdings while the legacy
  archive summary reports the two historical CDW/HUM positions; legacy NAV stays
  separate from the operational $100,000.
* PORTFOLIO MANAGER - headline "ORDER PLAN READY FOR REVIEW"; research/legacy
  warnings never change the operational next action.
* UI STATIC - primary Command Center is the canonical card + five-stage strip +
  RESEARCH ONLY strip (context/research/legacy panels collapsed into the archive);
  PM first viewport carries the one primary button and a compact plan summary with
  the 25-name table and diagnostics collapsed; right panel carries the canonical
  rows and a one-line collapsed legacy archive; the order-plan review table shows
  commission/net/execution-model/plan-date; no native dialogs; no blank buttons.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest  # noqa: F401  (fixtures resolved by name)

from paper_trader.api import alpha_book as ab
from paper_trader.api import operational_book as ob_mod
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _TICKS, _confirm_snapshot, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

_CANONICAL_FIELDS = (
    "operational_book_name", "operational_book_status", "operational_nav",
    "operational_cash", "operational_holdings_count",
    "operational_pending_order_count", "operational_fill_count",
    "confirmed_target_name", "confirmed_target_date", "confirmed_target_count",
    "implemented_target_count", "implementation_percentage",
    "desk_mark_status", "desk_mark_date", "order_plan_status",
    "next_required_action", "blockers", "safety_mode", "legacy_archive_summary",
)


def _ready_world():
    """Confirmed target + initialized book + valid desk marks -> plan exists."""
    _confirm_snapshot()
    _init_book("2026-07-18")
    _refresh("2026-07-18")


def _load(today="2026-07-18"):
    return ob_mod.load_operational_book(today=today)


def _html():
    return _UI.read_text(encoding="utf-8")


def _scripts(html):
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


# --------------------------------------------------------------------------- #
# A. Canonical operational state contract
# --------------------------------------------------------------------------- #
class TestCanonicalStateContract:
    def test_canonical_state_present_with_all_fields(self, env27b1):
        _ready_world()
        d = _load()
        assert "canonical_state" in d
        cs = d["canonical_state"]
        for field in _CANONICAL_FIELDS:
            assert field in cs, field
        # embedded in the operational_book too (one payload, two access paths)
        assert d["operational_book"]["canonical_state"] == cs

    def test_canonical_values_of_the_ready_state(self, env27b1):
        _ready_world()
        cs = _load()["canonical_state"]
        assert cs["operational_book_name"] == "Alpha Paper Book #1"
        assert cs["operational_book_status"] == "ORDER_PLAN_READY"
        assert cs["operational_nav"] == 100000.0
        assert cs["operational_cash"] == 100000.0
        assert cs["operational_holdings_count"] == 0
        assert cs["operational_pending_order_count"] == 0
        assert cs["operational_fill_count"] == 0
        assert cs["confirmed_target_count"] == len(_TICKS)
        assert cs["target_confirmation_status"] == "CONFIRMED"
        assert cs["confirmed_target_date"] is not None
        assert cs["implemented_target_count"] == 0
        assert cs["desk_mark_status"] == "DESK_MARK_READY"
        assert cs["desk_mark_date"] == "2026-07-17"
        assert cs["order_plan_status"] == "ORDER_PLAN_READY"
        assert cs["next_required_action"] == "REVIEW_AND_CONFIRM_ORDER_PLAN"
        assert cs["header_status"]["code"] == "ORDER_PLAN_READY"

    def test_safety_mode_block(self, env27b1):
        _ready_world()
        sm = _load()["canonical_state"]["safety_mode"]
        assert sm["manual_review"] is True
        assert sm["paper_orders_only"] is True
        assert sm["broker_execution"] is False
        assert sm["automation"] is False

    def test_marks_missing_state_blocks_the_plan_status(self, env27b1):
        _confirm_snapshot()
        _init_book("2026-07-18")
        cs = _load()["canonical_state"]
        assert cs["order_plan_status"] == "BLOCKED_DESK_MARKS_REQUIRED"
        assert cs["next_required_action"] == "REFRESH_DESK"


# --------------------------------------------------------------------------- #
# B. Plan-aware five-stage workflow + ONE next action
# --------------------------------------------------------------------------- #
class TestPlanAwareWorkflow:
    def test_stage_statuses_when_plan_exists(self, env27b1):
        _ready_world()
        o = _load()["operational_book"]
        st = {s["code"]: s for s in o["workflow_stages"]}
        assert st["REFRESH_DESK_MARKS"]["status"] == "COMPLETE"
        assert st["VERIFY_ALPHA_TARGET"]["status"] == "COMPLETE"
        assert st["GENERATE_ORDER_PLAN"]["status"] == "COMPLETE"
        assert st["GENERATE_ORDER_PLAN"]["detail"] == "Order plan ready"
        assert st["CONFIRM_PAPER_ORDERS"]["status"] == "NEEDS_ACTION"
        assert "confirm" in st["CONFIRM_PAPER_ORDERS"]["detail"].lower()
        assert st["MONITOR"]["status"] == "PENDING"
        assert o["workflow_stage"] == "CONFIRM_PAPER_ORDERS"

    def test_one_canonical_next_action_everywhere(self, env27b1):
        _ready_world()
        d = _load()
        o = d["operational_book"]
        assert o["next_action_code"] == "REVIEW_AND_CONFIRM_ORDER_PLAN"
        assert d["canonical_state"]["next_required_action"] == \
            "REVIEW_AND_CONFIRM_ORDER_PLAN"
        assert o["next_action"].startswith("REVIEW_AND_CONFIRM_ORDER_PLAN:")

    def test_header_badge_is_order_plan_ready(self, env27b1):
        _ready_world()
        o = _load()["operational_book"]
        assert o["header_status"] == {"code": "ORDER_PLAN_READY",
                                      "label": "ORDER PLAN READY"}

    def test_monitor_never_the_next_action_with_zero_holdings(self, env27b1):
        _ready_world()
        o = _load()["operational_book"]
        assert o["holdings_count"] == 0
        assert o["next_action_code"] != "MONITOR"
        assert o["workflow_stage"] != "MONITOR"

    def test_refresh_desk_not_the_next_action_when_marks_ready(self, env27b1):
        _ready_world()
        o = _load()["operational_book"]
        assert o["desk_mark_status"] == "DESK_MARK_READY"
        assert o["next_action_code"] != "REFRESH_DESK"


# --------------------------------------------------------------------------- #
# C. Duplicate confirmed snapshot is informational, never a blocker
# --------------------------------------------------------------------------- #
class TestDuplicateSnapshotInformational:
    def test_duplicate_not_in_operational_blockers(self, env27b1):
        _ready_world()
        d = _load()
        o = d["operational_book"]
        # the readiness layer reports the duplicate on a CONFIRMED target ...
        raw = (o["current_target"] or {}).get("confirmation_blockers") or []
        assert any("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in str(b) for b in raw)
        # ... but it is NOT an operational blocker anywhere
        for blockers in (o["blockers"], d["canonical_state"]["blockers"]):
            assert not any("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in str(b)
                           for b in blockers)

    def test_duplicate_re_rendered_as_informational(self, env27b1):
        _ready_world()
        d = _load()
        info = d["operational_book"]["informational"]
        assert info == d["canonical_state"]["informational"]
        assert any("no further confirmation required" in str(i) for i in info)

    def test_blockers_empty_in_the_clean_ready_state(self, env27b1):
        _ready_world()
        assert _load()["operational_book"]["blockers"] == []


# --------------------------------------------------------------------------- #
# D. Legacy archive separation (never the operational book)
# --------------------------------------------------------------------------- #
class TestLegacySeparation:
    def test_operational_zero_holdings_while_legacy_has_two(self, env27b1):
        _ready_world()
        d = _load()
        o = d["operational_book"]
        legacy = d["canonical_state"]["legacy_archive_summary"]
        assert o["holdings_count"] == 0 and o["holdings"] == {}
        assert legacy["positions_count"] == 2
        assert legacy["line"] == "Legacy paper book archive: 2 historical positions"

    def test_legacy_tickers_never_operational_holdings(self, env27b1):
        _ready_world()
        d = _load()
        legacy = d["canonical_state"]["legacy_archive_summary"]
        assert set(legacy["tickers"]) == {"CDW", "HUM"}
        for tk in legacy["tickers"]:
            assert tk not in d["operational_book"]["holdings"]
            assert tk not in _TICKS

    def test_operational_nav_and_legacy_value_stay_separate(self, env27b1):
        _ready_world()
        d = _load()
        assert d["canonical_state"]["operational_nav"] == 100000.0
        # the archived legacy book keeps its own (different) valuation
        entry = ob_mod._legacy_archive_entry()
        assert entry["total_value"] == 99120.0
        assert entry["total_value"] != d["canonical_state"]["operational_nav"]

    def test_legacy_archive_degrades_without_blocking(self, env27b1, monkeypatch):
        _ready_world()
        def _boom():
            raise RuntimeError("legacy valuation offline")
        monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _boom)
        cs = _load()["canonical_state"]
        assert cs["next_required_action"] == "REVIEW_AND_CONFIRM_ORDER_PLAN"
        assert cs["legacy_archive_summary"]["positions_count"] is None


# --------------------------------------------------------------------------- #
# E. Every operator surface returns the same operational status
# --------------------------------------------------------------------------- #
class TestSurfaceAgreement:
    def test_endpoints_agree_on_status_and_next_action(self, env27b1, client):
        _ready_world()
        ob_d = client.get("/v1/operational-book", headers=_AUTH).json()
        ab_d = client.get("/v1/alpha-book/status", headers=_AUTH).json()
        assert ob_d["canonical_state"]["operational_book_status"] == "ORDER_PLAN_READY"
        assert ab_d["current_state"] == "ORDER_PLAN_READY"
        s = pm.load_summary()
        assert s["operational_book"]["current_status"] == "ORDER_PLAN_READY"
        assert s["operational_book"]["next_action_code"] == \
            ob_d["canonical_state"]["next_required_action"]
        assert s["operational_book"]["canonical_state"]["order_plan_status"] == \
            "ORDER_PLAN_READY"

    def test_pm_headline_is_order_plan_ready_for_review(self, env27b1):
        _ready_world()
        s = pm.load_summary()
        assert s["decision_headline"] == "ORDER PLAN READY FOR REVIEW"
        assert "REVIEW_AND_CONFIRM_ORDER_PLAN" in s["decision_reason"]

    def test_research_or_legacy_warnings_never_change_the_next_action(
            self, env27b1, monkeypatch):
        _ready_world()
        def _boom():
            raise RuntimeError("legacy/research valuation offline")
        monkeypatch.setattr(pm, "_VALUATION_LOADER", _boom)
        s = pm.load_summary()
        # legacy/research trouble degrades its own scoped panels, never the
        # canonical operational decision or next action
        assert s["decision_headline"] == "ORDER PLAN READY FOR REVIEW"
        assert s["operational_book"]["next_action_code"] == \
            "REVIEW_AND_CONFIRM_ORDER_PLAN"


# --------------------------------------------------------------------------- #
# F. UI static - Command Center primary vs archive
# --------------------------------------------------------------------------- #
class TestUiCommandCenterCutover:
    def _primary(self, html):
        return html[html.index('id="cc-root"'):html.index('id="cc-legacy-overview"')]

    def _archive(self, html):
        return html[html.index('id="cc-legacy-overview"'):
                    html.index("end cc-legacy-overview")]

    def test_primary_is_canonical_card_plus_research_strip(self):
        html = _html()
        primary = self._primary(html)
        for el in ('id="cc-ob-panel"', 'id="cc-ob-headline"',
                   'id="cc-ob-primary-btn"', 'id="cc-ob-workflow"',
                   'id="cc-research-strip"', "RESEARCH ONLY"):
            assert el in primary, el
        for i in range(1, 6):
            assert 'id="cc-wf-stage-%d"' % i in primary, i

    def test_context_research_legacy_panels_archived(self):
        html = _html()
        primary = self._primary(html)
        archive = self._archive(html)
        for el in ('id="dor-card"', "cc-kpi-row", "Today's Workflow",
                   'id="cc-tournament"', "Next Best Action", 'id="cc-rq-preview"'):
            assert el not in primary, "still on primary CC: " + el
            assert el in archive, "missing from CC archive: " + el

    def test_archive_titled_and_collapsed(self):
        html = _html()
        m = re.search(r'<details[^>]*id="cc-legacy-overview"[^>]*>', html)
        assert m and "open" not in m.group(0)
        assert "HISTORICAL PAPER BOOKS &mdash; ARCHIVE" in html

    def test_five_stage_strip_rendered_from_canonical_stages(self):
        js = _scripts(_html())
        assert "cc-wf-stage-" in js
        assert js.count("workflow_stages") >= 1

    def test_top_bar_mark_labeled_research(self):
        html = _html()
        assert "Research mark:" in html


# --------------------------------------------------------------------------- #
# G. UI static - Portfolio Manager first viewport + collapsed detail
# --------------------------------------------------------------------------- #
class TestUiPortfolioManagerCutover:
    def test_decision_card_has_primary_button_and_plan_line(self):
        html = _html()
        card = html[html.index('id="pm-decision-card"'):html.index('id="otr-band"')]
        assert 'id="pm-plan-line"' in card
        assert 'id="pm-primary-next-btn"' in card

    def test_target_constituents_collapsed_by_default(self):
        html = _html()
        for did in ("otr-detail", "otr-constituents"):
            m = re.search(r'<details[^>]*id="%s"[^>]*>' % did, html)
            assert m and "open" not in m.group(0), did
        # the pinned literal survives inside the collapsed constituents section
        band = html[html.index('id="otr-constituents"'):html.index('id="otr-table"')]
        assert "COMPLETE TARGET PORTFOLIO" in band

    def test_confirmed_target_shows_informational_not_red(self):
        html = _html()
        assert 'id="otr-confirmed-note"' in html
        assert "Target already confirmed" in html
        assert "no further confirmation required" in html
        js = _scripts(html)
        assert "DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in js
        assert "otr-actions-wrap" in js   # confirmation controls hidden when CONFIRMED

    def test_statusbar_diagnostics_live_in_advanced_audit(self):
        html = _html()
        adv = html[html.index('id="pm-advanced"'):html.index('id="pm-actions"')]
        assert 'id="pm-statusbar"' in adv
        assert "Advanced / Audit" in html

    def test_order_plan_review_table_columns(self):
        js = _scripts(_html())
        for col in ("Est. commission", "Est. net", "Execution model NEXT_CLOSE",
                    "Resulting cash", "Plan of target snapshot"):
            assert col in js, col

    def test_confirm_plan_highlighted_generate_demoted(self):
        js = _scripts(_html())
        assert "_abEmph('ab-act-confirm-plan', reviewPhase)" in js
        assert "_abEmph('ab-act-plan', !planReady" in js


# --------------------------------------------------------------------------- #
# H. UI static - right panel, primary action, Alpha Portfolio gating
# --------------------------------------------------------------------------- #
class TestUiRightPanelAndPrimaryAction:
    def test_right_panel_canonical_rows(self):
        html = _html()
        for rid in ("right-ob-state", "right-ob-nav", "right-ob-cash",
                    "right-ob-holdings", "right-ob-pending", "right-ob-fills",
                    "right-ob-target", "right-ob-mark", "right-ob-impl"):
            assert 'id="%s"' % rid in html, rid

    def test_legacy_archive_one_collapsed_line(self):
        html = _html()
        m = re.search(r'<details[^>]*id="right-legacy-capacity"[^>]*>', html)
        assert m and "open" not in m.group(0)
        assert 'id="right-legacy-archive-line"' in html
        assert "Legacy paper book archive" in html
        # the archived legacy position status is INSIDE the collapsed details
        region = html[html.index('id="right-legacy-capacity"'):]
        region = region[:region.index("</details>")]
        assert 'id="right-completed-summary"' in region

    def test_primary_action_labels_and_navigation(self):
        js = _scripts(_html())
        assert "function obPrimaryAction" in js
        # Final 27B.2 cutover: ONE navigation label everywhere.
        assert "'Review Order Plan'" in js
        assert "'Review Paper Order Plan'" not in js
        assert "'Review & Confirm Paper Orders'" not in js
        assert "REVIEW_AND_CONFIRM_ORDER_PLAN" in js
        fn = js[js.index("function obPrimaryAction"):js.index("window.obPrimaryAction")]
        assert "navigateToRoute('portfolio-manager')" in fn
        assert "ab-band" in fn

    def test_alpha_portfolio_snapshot_area_read_only_when_confirmed(self):
        html = _html()
        assert 'id="mhz-snapshot-confirmed-note"' in html
        js = _scripts(html)
        assert "mhz-snapshot-confirmed-note" in js
        assert "loadMultiHorizon" in js
        fn = js[js.index("async function loadMultiHorizon"):]
        fn = fn[:fn.index("window._mhzData")]
        assert "loadOperationalBook" in fn

    def test_pm_headline_green_for_order_plan_review(self):
        js = _scripts(_html())
        assert "if (headline === 'ORDER PLAN READY FOR REVIEW') return 'var(--ok" in js


# --------------------------------------------------------------------------- #
# I. UI quality - no dialogs, no blank buttons on the new surfaces
# --------------------------------------------------------------------------- #
class TestUiQuality27B2:
    def test_no_native_dialogs(self):
        js = _scripts(_html())
        for pat in (r"(?<![A-Za-z0-9_.])alert\(", r"(?<![A-Za-z0-9_.])confirm\(",
                    r"(?<![A-Za-z0-9_.])prompt\("):
            assert not re.search(pat, js), pat

    def test_no_blank_buttons_on_reworked_regions(self):
        html = _html()
        for start, end in ((('id="cc-root"'), 'id="cc-legacy-overview"'),
                           (('id="pm-decision-card"'), 'id="otr-band"'),
                           (('id="otr-band"'), 'id="ab-band"')):
            region = html[html.index(start):html.index(end)]
            for m in re.finditer(r"<button[^>]*>(.*?)</button>", region, re.DOTALL):
                label = re.sub(r"<[^>]+>", "", m.group(1))
                label = re.sub(r"&[a-z#0-9]+;", "x", label)
                assert label.strip(), "blank button in region %s" % start

    def test_operational_book_endpoint_still_single_fetch(self):
        js = _scripts(_html())
        assert js.count("/v1/operational-book") == 1
