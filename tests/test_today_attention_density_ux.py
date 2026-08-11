"""tests/test_today_attention_density_ux.py — Today attention hero ADAPTIVE
DENSITY refinement contract.

Fully offline and hermetic: static / structural assertions over
``api/ui/index.html`` plus the strict architecture audit, in the same style as
``test_operator_decision_flow_ux`` and ``test_phase29j1_operator_ux``. No server /
provider / prediction / DOM / real market call is touched. The real per-viewport
rendering (hero bounding-box height <= 180px, recovered Market Context real
estate, no console errors) is proven separately by the browser acceptance run;
these tests lock the adaptive-density CONTRACT in the source so it cannot silently
regress.

Design principle under test: the component visually scales with urgency.

  * NO_ACTION / MONITOR                 -> COMPACT confirmation strip
  * REVIEW_AVAILABLE (optional reviews) -> COMPACT + quiet secondary links
  * REVIEW_REQUIRED / ACTION_REQUIRED   -> EXPANDED
  * BLOCKED / ERROR                     -> EXPANDED

The density is derived from the SAME canonical operator action (execution
availability + action code) that the Operator Decision Flow introduced — never
from client-side date or priority math — and the right rail no longer duplicates
the optional review links that already sit adjacent in the hero.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_UI = _ROOT / "api" / "ui" / "index.html"


@pytest.fixture(scope="module")
def ui() -> str:
    return _UI.read_text(encoding="utf-8")


def _region(text: str, start_marker: str, end_marker: str) -> str:
    a = text.find(start_marker)
    assert a != -1, f"start marker not found: {start_marker!r}"
    b = text.find(end_marker, a + len(start_marker))
    assert b != -1, f"end marker not found after start: {end_marker!r}"
    return text[a:b]


def _hero(ui: str) -> str:
    return _region(ui, "function _wsRenderTodayHero(d)", "window._wsRenderTodayHero")


def _compact(ui: str) -> str:
    h = _hero(ui)
    ci = h.index("if (noAction) {")
    ei = h.index("// ---- EXPANDED mode")
    return h[ci:ei]


def _expanded(ui: str) -> str:
    h = _hero(ui)
    ei = h.index("// ---- EXPANDED mode")
    return h[ei:]


def _banner(ui: str) -> str:
    return _region(ui, "function _wsBannerHtml(d)", "function _wsApplyRightPanel")


def _right_panel(ui: str) -> str:
    return _region(ui, "function _wsApplyRightPanel(d)", "_wsApplyAssessmentFraming")


# --------------------------------------------------------------------------- #
# 1-2. NO_ACTION and REVIEW_AVAILABLE stay COMPACT (one confirmation strip)
# --------------------------------------------------------------------------- #
def test_1_no_action_renders_compact_marker(ui):
    compact = _compact(ui)
    # stable DOM markers (not brittle pixel strings): density + mode class.
    assert "host.setAttribute('data-density', 'compact');" in compact
    assert "'today-hero state-ok th-compact'" in compact
    # quiet Title-Case confirmation, not a shouting all-caps banner.
    assert "You&rsquo;re all set" in compact
    assert "YOU'RE ALL SET" not in ui  # the shouting variant is fully removed


def test_2_review_available_optional_state_stays_compact(ui):
    compact = _compact(ui)
    # presence of optional reviews only changes the op-state label, NEVER the density.
    assert "reviews.length ? 'REVIEW_AVAILABLE' : 'NO_ACTION'" in compact
    assert "host.setAttribute('data-op-state', opStateC);" in compact


# --------------------------------------------------------------------------- #
# 3-5. REVIEW_REQUIRED / ACTION_REQUIRED / BLOCKED render EXPANDED
# --------------------------------------------------------------------------- #
def test_3_review_required_renders_expanded(ui):
    expanded = _expanded(ui)
    assert "host.setAttribute('data-density', 'expanded');" in expanded
    assert "'REVIEW_REQUIRED'" in expanded


def test_4_action_required_renders_expanded(ui):
    expanded = _expanded(ui)
    assert "host.setAttribute('data-density', 'expanded');" in expanded
    assert "'ACTION_REQUIRED'" in expanded


def test_5_blocked_requiring_action_renders_expanded(ui):
    expanded = _expanded(ui)
    assert "opState = 'BLOCKED';" in expanded
    # blocked keeps the danger accent and the expanded markup (eyebrow + headline).
    assert "state-danger" in expanded
    assert "th-eyebrow" in expanded


# --------------------------------------------------------------------------- #
# 6. NO_ACTION hero has NO primary CTA
# --------------------------------------------------------------------------- #
def test_6_no_action_hero_has_no_primary_cta(ui):
    compact = _compact(ui)
    assert "th-cta" not in compact          # the primary action button is expanded-only
    assert "th-cta-row" not in compact
    # the expanded branch is where the single primary CTA lives.
    assert 'class="th-cta ' in _expanded(ui)


# --------------------------------------------------------------------------- #
# 7-8. NO_ACTION retains the two optional links, demoted to SECONDARY
# --------------------------------------------------------------------------- #
def test_7_no_action_retains_holding_and_portfolio_links(ui):
    hero = _hero(ui)
    # the human labels + full-screen routes are defined once and reused by both densities.
    assert "route: 'holding-review', t: 'View holding changes'" in hero
    assert "route: 'proposed-portfolio', t: 'View proposed portfolio'" in hero
    compact = _compact(ui)
    # the compact branch actually renders those optional links (routes via reviewMap).
    assert "reviewMap[q.action_code]" in compact
    assert "navigateToRoute" in compact and "m.route" in compact


def test_8_optional_links_are_secondary_not_primary(ui):
    compact = _compact(ui)
    # quiet inline text links (thc-link) with a muted "Optional" cue — NOT primary buttons.
    assert 'class="thc-link"' in compact
    assert "thc-links-lab" in compact and "Optional" in compact
    assert "th-cta" not in compact


# --------------------------------------------------------------------------- #
# 9-10. Right rail: no duplicate optional links in NO_ACTION; primary action
#       still surfaced when action is truly required
# --------------------------------------------------------------------------- #
def test_9_right_rail_does_not_duplicate_optional_links_when_no_action(ui):
    banner = _banner(ui)
    # the review-only optional chips are suppressed in a no-op state (shown once in the hero).
    assert "if (fm && _wsNoAction) return '';" in banner


def test_10_right_rail_surfaces_primary_action_when_required(ui):
    banner = _banner(ui)
    # the canonical primary button is shown when there IS a real action (not a no-op).
    # (Operator Action Integrity: an executable primary also earns the CTA.)
    assert "(pa.destination || _wsExecutes) && !_wsNoAction" in banner
    panel = _right_panel(ui)
    # the Action/Safety panel keeps the primary button and only hides it for a no-op.
    assert "right-primary-action-btn" in panel
    assert "btn.style.display = 'none'" in panel
    assert "btn.style.display = ''" in panel
    assert "No action required right now." in panel


# --------------------------------------------------------------------------- #
# 11. Canonical operator-state semantics unchanged (no client date/priority math)
# --------------------------------------------------------------------------- #
def test_11_density_keys_off_canonical_state_not_dates(ui):
    hero = _hero(ui)
    assert "execution_available !== true" in hero
    assert "MONITOR_PORTFOLIO" in hero and "WAIT_FOR_SESSION_CLOSE" in hero
    for forbidden in ("new Date(", "Date.now(", ".getTime("):
        assert forbidden not in hero, f"client date math in hero: {forbidden}"
    assert "NO ACTION TODAY" not in hero
    # density is a pure function of noAction (canonical) — one derivation, no second owner.
    assert hero.count("var noAction = (pa.execution_available !== true)") == 1


# --------------------------------------------------------------------------- #
# 12-14. Dedicated routes + legacy deep links intact (no regression)
# --------------------------------------------------------------------------- #
def test_12_holding_review_route_intact(ui):
    assert 'id="tab-holding-review"' in ui
    assert "tabName === 'holding-review'" in ui
    assert "window.loadHoldingReviewView" in ui
    assert "'holding-review': 'holding-review'" in ui


def test_13_proposed_portfolio_route_intact(ui):
    assert 'id="tab-proposed-portfolio"' in ui
    assert "tabName === 'proposed-portfolio'" in ui
    assert "window.loadProposedPortfolioView" in ui
    assert "'proposed-portfolio': 'proposed-portfolio'" in ui


def test_14_legacy_deep_links_reachable(ui):
    assert "'opportunity-cost': 'hoc-card'" in ui
    assert "'reallocation': 'realloc-card'" in ui
    # full-screen reviews keep the Portfolio nav item active (parent = Portfolio).
    assert ("base === 'holding-review' || base === 'proposed-portfolio') "
            "base = 'portfolio-manager'") in ui


# --------------------------------------------------------------------------- #
# 15. No order / apply / confirm / promotion control introduced by this phase
# --------------------------------------------------------------------------- #
def test_15_no_forbidden_action_controls_in_hero(ui):
    hero = _hero(ui)
    for banned in ("Create Order", "Apply proposal", "Approve proposal",
                   "Promote", "Run Daily Close", "Confirm"):
        assert banned not in hero, f"forbidden control in hero: {banned}"
    # no native dialogs in the hero surface.
    assert "alert(" not in hero and "confirm(" not in hero and "prompt(" not in hero


def test_15b_no_forbidden_apply_confirm_routes_ui_wide(ui):
    for route in ("/v1/operations/reallocation-proposal/apply",
                  "/v1/operations/reallocation-proposal/confirm",
                  "/v1/operations/apply-reallocation",
                  "/v1/operations/promote-model"):
        assert route not in ui, f"forbidden route present in UI: {route}"


# --------------------------------------------------------------------------- #
# CSS: the compact strip exists and is materially denser than the expanded card
# --------------------------------------------------------------------------- #
def test_compact_css_present_and_denser(ui):
    for cls in (".today-hero.th-compact", ".thc-row", ".thc-check", ".thc-lead",
                ".thc-title", ".thc-sub", ".thc-meta", ".thc-links", ".thc-link"):
        assert cls in ui, f"missing compact css class: {cls}"
    # the compact strip uses reduced padding vs the expanded card (26px 30px).
    assert ".today-hero.th-compact{padding:14px 20px" in ui
    assert "padding:26px 30px" in ui  # the base/expanded padding is retained
    # the compact confirmation icon is a small status check, not the 30px expanded mark.
    compact = _compact(ui)
    assert "thc-check" in compact and "th-mark" not in compact


def test_compact_title_smaller_than_expanded_headline(ui):
    # the routine confirmation title is ~23px (quiet), well under the 30px expanded headline.
    assert ".thc-title{font-size:23px" in ui
    assert ".th-state{font-size:30px" in ui


# --------------------------------------------------------------------------- #
# Architecture guard — the refinement stays audit-green
# --------------------------------------------------------------------------- #
def test_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_operator_ux_guard_still_green():
    aud = importlib.import_module("scripts.audit_architecture")
    ux = aud.check_operator_ux_consolidation_ownership(aud._iter_source_files())
    # the single canonical workflow-state owner and safety strip are untouched.
    assert ux["workflow_next_action_renderer_count"] == 1
    assert ux["missing_safety_tokens"] == []
    assert ux["market_context_present"] is True
    assert ux["primary_areas_present"] and ux["legacy_views_demoted"]
    assert ux["forbidden_new_routes_present"] == []


def test_single_workflow_state_owner_retained(ui):
    # the density refinement must not fork the canonical owner.
    assert ui.count("function loadWorkflowState") == 1
    assert ui.count("function _wsRenderTodayHero") == 1
    assert ui.count("function _wsBannerHtml") == 1
    assert "try { _wsRenderTodayHero(d); } catch (e) {}" in ui
