"""
tests/test_operator_decision_flow_ux.py — Operator Decision Flow & Portfolio
Review UX consistency contract.

Fully offline: static/structural assertions over ``api/ui/index.html`` plus a
light check that the canonical operator-state owner remains a single source, in
the style of the existing UI-consistency tests (test_phase27b6, test_slice2_ui).

The phase reconciles operator-state semantics and adds two dedicated full-screen
review experiences (Holding Review / Proposed Portfolio) WITHOUT inventing any
backend decision. These tests lock the contract that was verified live in the
browser (1920/1440/1366/1000) so it cannot silently regress:

  * the Today hero, the right rail and every canonical banner render the SAME
    canonical workflow-state — when the primary action is a no-op (MONITOR /
    WAIT) there is NO urgent CTA and NO conflicting instruction;
  * "Review …" is NOT used as a primary action for the review-only HOC /
    reallocation surfaces (no acknowledgement owner exists) — they read
    "View holding changes" / "View proposed portfolio";
  * the two reviews are dedicated in-app full-screen routes whose parent is
    Portfolio, with working Back navigation and preserved legacy deep links;
  * no order / apply / confirm / approve control is introduced; safety chrome
    is consolidated but the full canonical safety set is retained;
  * the UI performs no client-side date / priority math in the workflow region.
"""
from __future__ import annotations

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


# --------------------------------------------------------------------------- #
# Navigation: two dedicated full-screen routes, parent = Portfolio, deep links
# --------------------------------------------------------------------------- #
def test_full_screen_routes_registered(ui):
    assert "'holding-review': 'holding-review'" in ui
    assert "'proposed-portfolio': 'proposed-portfolio'" in ui
    assert 'id="tab-holding-review"' in ui
    assert 'id="tab-proposed-portfolio"' in ui


def test_full_screen_reviews_keep_portfolio_nav_active(ui):
    # _highlightNav aliases both full-screen reviews back to the Portfolio nav item.
    assert ("base === 'holding-review' || base === 'proposed-portfolio') "
            "base = 'portfolio-manager'") in ui


def test_full_screen_lazy_loaders_wired(ui):
    assert "tabName === 'holding-review'" in ui
    assert "tabName === 'proposed-portfolio'" in ui
    assert "window.loadHoldingReviewView" in ui
    assert "window.loadProposedPortfolioView" in ui


def test_back_to_portfolio_present_for_both_views(ui):
    assert ui.count("Back to Portfolio") >= 2


def test_legacy_deep_links_preserved(ui):
    # Old in-tab scroll-focus deep links must still resolve (no dead link).
    assert "'reallocation': 'realloc-card'" in ui
    assert "'opportunity-cost': 'hoc-card'" in ui


# --------------------------------------------------------------------------- #
# Today hero: one state, at most one primary action, market-closed no-action
# --------------------------------------------------------------------------- #
def test_today_hero_owned_by_canonical_renderer(ui):
    assert "function _wsRenderTodayHero(d)" in ui
    # rendered from the ONE canonical renderWorkflowState (call site present).
    assert "try { _wsRenderTodayHero(d); } catch (e) {}" in ui
    # Adaptive-density refinement: the routine no-action confirmation uses quiet Title
    # Case ("You're all set") in the COMPACT hero rather than a shouting all-caps banner.
    assert "You&rsquo;re all set" in ui


def test_hero_no_action_keys_off_canonical_state_not_dates(ui):
    hero = _region(ui, "function _wsRenderTodayHero(d)", "window._wsRenderTodayHero")
    # The no-action state is derived from the canonical primary action, never client dates.
    assert "execution_available !== true" in hero
    assert "MONITOR_PORTFOLIO" in hero
    assert "WAIT_FOR_SESSION_CLOSE" in hero
    for forbidden in ("new Date(", "Date.now(", ".getTime("):
        assert forbidden not in hero, f"client date math in hero: {forbidden}"
    # No stale gate label leaks into the operator hero.
    assert "NO ACTION TODAY" not in hero


def test_hero_secondary_review_links_are_human_and_route_to_full_screen(ui):
    hero = _region(ui, "function _wsRenderTodayHero(d)", "window._wsRenderTodayHero")
    assert "View holding changes" in hero
    assert "View proposed portfolio" in hero
    assert "holding-review" in hero and "proposed-portfolio" in hero
    # secondary analyses are OPTIONAL, not urgent
    assert "optional" in hero


# --------------------------------------------------------------------------- #
# Right rail + banners summarise the SAME state — no conflicting instruction
# --------------------------------------------------------------------------- #
def test_right_rail_says_no_action_and_hides_button_when_noop(ui):
    panel = _region(ui, "function _wsApplyRightPanel(d)", "_wsApplyAssessmentFraming")
    assert "No action required right now." in panel
    assert "MONITOR_PORTFOLIO" in panel and "WAIT_FOR_SESSION_CLOSE" in panel
    assert "btn.style.display = 'none'" in panel


def test_canonical_banner_suppresses_noop_button_and_humanises_reviews(ui):
    banner = _region(ui, "function _wsBannerHtml(d)", "function _wsApplyRightPanel")
    # The prominent action button is suppressed for a no-op (monitor / wait) state.
    assert "_wsNoAction" in banner
    assert "pa.destination && !_wsNoAction" in banner
    # Review-only follow-ups become human, full-screen links (never urgent "Review …").
    assert "'REVIEW_HOLDING_OPPORTUNITY_COST'" in banner
    assert "route: 'holding-review'" in banner
    assert "route: 'proposed-portfolio'" in banner
    assert "View holding changes" in banner


def test_portfolio_decision_bar_opens_full_screen_reviews(ui):
    assert "navigateToRoute('holding-review')" in ui
    assert "navigateToRoute('proposed-portfolio')" in ui
    assert "View holding changes &rarr;" in ui
    assert "View proposed portfolio &rarr;" in ui


# --------------------------------------------------------------------------- #
# No invented decision: Holding Review is review-only (no acknowledgement owner)
# --------------------------------------------------------------------------- #
def test_holding_review_is_review_only_no_fake_approve(ui):
    view = _region(ui, "function renderHoldingReviewView(d)", "window.renderHoldingReviewView")
    assert "NO ACTION REQUIRED" in view
    assert "no confirmation step exists" in view
    # grouping: changed holdings prioritised, holds secondary, add candidates separate
    assert "Needs attention" in view
    assert "Unchanged" in view
    assert "Ideas / replacements" in view
    # no fabricated approve/apply control anywhere near the review
    assert "Approve holding" not in view
    assert "Apply changes" not in view


def test_proposed_portfolio_is_review_only_no_apply(ui):
    view = _region(ui, "function renderProposedPortfolioView(d)", "window.renderProposedPortfolioView")
    # Operator-clarity polish (final tranche): the review-only safety context is ONE quiet
    # chip in the dedicated hero (informational, not a CTA). The render function no longer
    # paints a second, large REVIEW ONLY status pill that duplicated it, so the header shows
    # exactly one review-only treatment.
    tab = _region(ui, 'id="tab-proposed-portfolio"', 'id="tab-multi-horizon"')
    assert tab.count("REVIEW ONLY") == 1
    assert "NO ORDERS" in tab
    # the render function must not repaint the duplicate large status pill.
    assert "'REVIEW ONLY'; stateEl.className = 'opx-state ok'" not in view
    # the five-part story
    for section in ("What would change", "Why", "What it would cost",
                    "What we don", "What the portfolio would look like"):
        assert section in view, f"missing proposed-portfolio section: {section}"
    # honest gaps, never fabricated (human copy defined for the canonical gap codes)
    assert "Expected return is not calibrated" in ui
    # no invented apply/approve/rebalance control
    assert "Apply proposal" not in view
    assert "Approve proposal" not in view


def test_no_forbidden_apply_confirm_reallocation_routes(ui):
    for route in ("/v1/operations/reallocation-proposal/apply",
                  "/v1/operations/reallocation-proposal/confirm",
                  "/v1/operations/apply-reallocation",
                  "/v1/operations/promote-model"):
        assert route not in ui, f"forbidden route present in UI: {route}"


def test_no_forbidden_primary_review_strings(ui):
    # RC6 forbidden primary strings must not be reintroduced by this phase.
    assert "Review Proposed Changes" not in ui
    assert "LATEST PORTFOLIO ASSESSMENT" not in ui


# --------------------------------------------------------------------------- #
# Safety chrome consolidated, full canonical set retained; no native dialogs
# --------------------------------------------------------------------------- #
def test_safety_chrome_consolidated_but_full_set_retained(ui):
    # ONE persistent consolidated safety line is present ...
    assert "Paper mode" in ui and "Manual review" in ui and "Automation off" in ui
    # ... and the full canonical safety token set is still present in the UI.
    for token in ("PAPER ONLY", "MANUAL REVIEW", "AUTOMATION OFF",
                  "NO BROKER EXECUTION", "NO MODEL PROMOTION"):
        assert token in ui, f"safety token removed: {token}"


def test_no_native_dialog_call_sites_in_new_surfaces(ui):
    # The platform neutralises the natives; the new surfaces must not call them.
    assert "window.alert = function" in ui  # override retained
    assert "window.confirm = function" in ui  # override retained
    for marker in ("function _wsRenderTodayHero(d)",
                   "function renderHoldingReviewView(d)",
                   "function renderProposedPortfolioView(d)"):
        end = {"function _wsRenderTodayHero(d)": "window._wsRenderTodayHero",
               "function renderHoldingReviewView(d)": "window.renderHoldingReviewView",
               "function renderProposedPortfolioView(d)": "window.renderProposedPortfolioView"}[marker]
        body = _region(ui, marker, end)
        assert "alert(" not in body
        assert "confirm(" not in body


def test_no_horizontal_overflow_guard_present(ui):
    # Page-level horizontal clip so off-canvas overlays never force a scrollbar.
    assert "overflow-x: hidden" in ui


# --------------------------------------------------------------------------- #
# The canonical operator-state owner remains a single source (no duplication)
# --------------------------------------------------------------------------- #
def test_single_workflow_state_loader(ui):
    assert ui.count("function loadWorkflowState") == 1


def test_single_hoc_and_reallocation_loaders(ui):
    assert ui.count("function loadHoldingOpportunityCost") == 1
    assert ui.count("function loadReallocationProposal") == 1
    # the new full-screen views reuse those loaders — they never add a second one
    assert ui.count("function loadHoldingReviewView") == 1
    assert ui.count("function loadProposedPortfolioView") == 1
