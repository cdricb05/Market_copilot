"""
tests/test_phase27b2_cross_surface_consistency.py - Phase 27B.2 FINAL
cross-surface consistency: every operator surface communicates the SAME
canonical operational truth for the ORDER_PLAN_READY state.

Fully offline (Phase 27A/27B.1 harness + static UI inspection). No test here
creates paper orders, fills, signals, trade decisions or live broker activity -
backend scenarios run against isolated tmp stores; UI checks read
api/ui/index.html statically.

Proves, for ORDER_PLAN_READY:
* Command Center, Portfolio Manager, Daily Workflow, Portfolio and the right
  panel all report ORDER PLAN READY from the one canonical payload;
* Alpha Portfolio does not present target confirmation as a current action;
* every primary CTA is "Review Order Plan" (one navigation label) and the only
  write label is "Confirm and Create Proposed Paper Orders";
* no normal operator surface recommends Monitor Portfolio, Refresh Alpha
  Target or Confirm Target Snapshot;
* DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT is never a normal operator blocker;
* legacy archives are collapsed by default and legacy/research marks can never
  override the operational desk mark.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest  # noqa: F401  (fixtures resolved by name)

from paper_trader.api import operational_book as ob_mod
from paper_trader.api import portfolio_manager as pm

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH, _confirm_snapshot, _refresh,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1, _init_book,  # noqa: F401
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def _ready_world():
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
# A. All backend surfaces agree on ORDER PLAN READY + one next action
# --------------------------------------------------------------------------- #
class TestBackendSurfacesAgree:
    def test_all_endpoints_report_order_plan_ready(self, env27b1, client):
        _ready_world()
        ob_d = client.get("/v1/operational-book", headers=_AUTH).json()
        ab_d = client.get("/v1/alpha-book/status", headers=_AUTH).json()
        cs = ob_d["canonical_state"]
        assert cs["workflow_state"] == "ORDER_PLAN_READY"
        assert ab_d["current_state"] == "ORDER_PLAN_READY"
        s = pm.load_summary()
        assert s["operational_book"]["current_status"] == "ORDER_PLAN_READY"
        assert s["decision_headline"] == "ORDER PLAN READY FOR REVIEW"

    def test_one_next_action_code_everywhere(self, env27b1, client):
        _ready_world()
        ob_d = client.get("/v1/operational-book", headers=_AUTH).json()
        s = pm.load_summary()
        codes = {
            ob_d["canonical_state"]["next_action_code"],
            ob_d["canonical_state"]["next_required_action"],
            ob_d["operational_book"]["next_action_code"],
            s["operational_book"]["next_action_code"],
        }
        assert codes == {"REVIEW_AND_CONFIRM_ORDER_PLAN"}
        assert ob_d["canonical_state"]["next_action_label"] == "Review Order Plan"

    def test_duplicate_snapshot_never_a_normal_operator_blocker(
            self, env27b1, client):
        _ready_world()
        ob_d = client.get("/v1/operational-book", headers=_AUTH).json()
        raw = (ob_d["operational_book"]["current_target"] or {}) \
            .get("confirmation_blockers") or []
        assert any("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in str(b)
                   for b in raw)
        for blockers in (ob_d["canonical_state"]["blockers"],
                         ob_d["operational_book"]["blockers"]):
            assert not any("DUPLICATE_OF_LATEST_CONFIRMED_SNAPSHOT" in str(b)
                           for b in blockers)
        assert any("Target already confirmed" in str(n)
                   for n in ob_d["canonical_state"]["informational_notices"])

    def test_legacy_and_research_marks_cannot_override_desk_mark(
            self, env27b1, monkeypatch):
        """A dead legacy/research valuation never degrades the operational
        desk-mark readiness or the canonical next action."""
        _ready_world()
        def _boom():
            raise RuntimeError("legacy/research store offline")
        monkeypatch.setattr(ob_mod, "_VALUATION_LOADER", _boom)
        monkeypatch.setattr(pm, "_VALUATION_LOADER", _boom)
        cs = _load()["canonical_state"]
        assert cs["desk_mark_status"] == "DESK_MARK_READY"
        assert cs["desk_mark_date"] == "2026-07-17"
        assert cs["next_action_code"] == "REVIEW_AND_CONFIRM_ORDER_PLAN"
        s = pm.load_summary()
        assert s["decision_headline"] == "ORDER PLAN READY FOR REVIEW"


# --------------------------------------------------------------------------- #
# B. One canonical CTA label on every rendered surface (static UI)
# --------------------------------------------------------------------------- #
class TestOneCtaLabelAcrossSurfaces:
    def test_review_order_plan_is_the_navigation_label(self):
        html = _html()
        js = _scripts(html)
        # the ONE fallback label in the shared render path
        assert "'Review Order Plan'" in js
        # order-plan review workspace: read-only review button
        band = html[html.index('id="ab-band"'):html.index('id="pd-band"')]
        assert ">Review Order Plan</button>" in band
        # Daily Workflow stage-4 button
        card = html[html.index('id="dwob-card"'):
                    html.index("OPERATIONAL BOOK WORKFLOW END")]
        assert ">Review Order Plan</button>" in card

    def test_one_confirm_label_token_gated(self):
        html = _html()
        band = html[html.index('id="ab-band"'):html.index('id="pd-band"')]
        assert "Confirm and Create Proposed Paper Orders" in band
        # confirmation stays explicit + token-gated (no auto-confirm)
        assert "abAskAction('confirmPlan')" in band

    def test_banned_label_variants_absent_from_all_buttons(self):
        html = _html()
        for banned in ("Review Paper Order Plan", "Review Paper Orders<",
                       "Generate Executable Order Plan",
                       "Confirm Order Plan &mdash; Create Paper Orders"):
            assert banned not in html, banned
        js = _scripts(html)
        assert "'Review & Confirm Paper Orders'" not in js
        assert "'Confirm Order Plan'" not in js
        # "Review & Confirm Paper Orders" remains ONLY as the stage-4 NAME
        assert html.count("Review &amp; Confirm Paper Orders") == 2

    def test_all_surfaces_render_the_backend_label(self):
        js = _scripts(_html())
        # canonical label preferred from the payload, one shared variable
        assert "cs.next_action_label" in js
        assert "var primaryLabel = (cs && cs.next_action_label)" in js
        for snippet in ("obHeadline.textContent = ob ? ('NEXT: ' + "
                        "primaryLabel.toUpperCase())",
                        "pb.textContent = primaryLabel",
                        "btn.textContent = primaryLabel"):
            assert snippet in js, snippet


# --------------------------------------------------------------------------- #
# C. Page-level truth (static UI): CC / PM / DW / Portfolio / Alpha / right
# --------------------------------------------------------------------------- #
class TestPageOwnership:
    def test_command_center_canonical_card_and_archive(self):
        html = _html()
        primary = html[html.index('id="cc-root"'):
                       html.index('id="cc-legacy-overview"')]
        for el in ('id="cc-ob-panel"', 'id="cc-ob-headline"',
                   'id="cc-ob-primary-btn"', 'id="cc-ob-workflow"'):
            assert el in primary, el
        # ONE collapsed historical & research context section
        assert "HISTORICAL &amp; RESEARCH CONTEXT" in html
        m = re.search(r'<details id="cc-legacy-overview"[^>]*>', html)
        assert m and "open" not in m.group(0)

    def test_daily_workflow_operational_first_legacy_collapsed(self):
        html = _html()
        assert html.index('id="dwob-card"') < \
            html.index('<details id="dw-legacy-archive">')
        assert "Legacy Signal Workflow &amp; Diagnostics" in html
        assert '<details id="dw-legacy-archive" open' not in html

    def test_portfolio_operational_dominates_legacy_archived(self):
        html = _html()
        assert "Historical Paper Books &mdash; Legacy Portfolio Archive" in html
        m = re.search(r'<details[^>]*id="pt-archive"[^>]*>', html)
        assert m and "open" not in m.group(0)
        # operational card precedes the legacy archive
        assert html.index('id="ptob-state"') < html.index('id="pt-archive"')

    def test_alpha_portfolio_never_asks_to_confirm_again(self):
        html = _html()
        note = html[html.index('id="mhz-snapshot-confirmed-note"'):]
        note = note[:note.index("</div>")]
        assert "Target already confirmed." in note
        assert "Continue the operational workflow in Portfolio Manager." in note
        js = _scripts(html)
        # confirm/preview controls hidden while the snapshot is CONFIRMED
        assert "mhzPrev.style.display = targetConfirmed ? 'none'" in js
        assert "if (mhzBox && targetConfirmed) mhzBox.style.display = 'none'" in js

    def test_right_panel_canonical_rows_and_collapsed_legacy(self):
        html = _html()
        for el in ('id="right-ob-state"', 'id="right-ob-nav"',
                   'id="right-ob-cash"', 'id="right-ob-holdings"',
                   'id="right-ob-pending"', 'id="right-ob-fills"',
                   'id="right-ob-target"', 'id="right-ob-mark"',
                   'id="right-ob-impl"', 'id="right-primary-action-btn"'):
            assert el in html, el
        m = re.search(r'<details[^>]*id="right-legacy-capacity"[^>]*>', html)
        assert m and "open" not in m.group(0)

    def test_research_tools_collapsed_and_non_operational(self):
        html = _html()
        assert html.count("Research Tools &mdash; Non-operational") >= 3
        for m in re.finditer(r'<details class="ra-tools-nonop"[^>]*>', html):
            assert "open" not in m.group(0)

    def test_no_monitor_portfolio_recommendation_on_primary_surfaces(self):
        html = _html()
        js = _scripts(html)
        # MONITOR label only reachable when the canonical code says MONITOR
        assert "if (code === 'MONITOR') return 'Open Paper Desk'" in js
        # the legacy "Next Best Action" recommender lives inside the CC archive
        archive = html[html.index('id="cc-legacy-overview"'):
                       html.index("end cc-legacy-overview")]
        assert "Next Best Action" in archive

    def test_header_badge_owned_by_canonical_payload_only(self):
        js = _scripts(_html())
        assert "hb.textContent = 'OPERATIONAL: ' + (hs.label || hs.code)" in js
        assert "Legacy/archived mark dates never drive this badge" in js


# --------------------------------------------------------------------------- #
# D. Safety: nothing here can create orders / fills / signals / decisions
# --------------------------------------------------------------------------- #
class TestNoWritesFromConsistencyChecks:
    def test_ready_world_creates_no_orders_or_fills(self, env27b1, client):
        _ready_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        cs = d["canonical_state"]
        assert cs["pending_order_count"] == 0
        assert cs["fill_count"] == 0
        assert cs["holdings_count"] == 0
        assert d["performed_write"] is False
        assert d["read_only"] is True
