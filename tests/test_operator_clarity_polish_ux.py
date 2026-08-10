"""tests/test_operator_clarity_polish_ux.py — Operator Clarity Polish (final UX
tranche) contract.

Fully offline / hermetic: static-structural assertions over ``api/ui/index.html``
plus the strict architecture audit, in the same style as
``test_operator_decision_flow_ux`` and ``test_today_attention_density_ux``. The
real per-viewport rendering is proven separately by the browser acceptance run;
these tests lock the CONTRACT of three presentation fixes so they cannot silently
regress. NO backend semantics change — the UI keeps reading the canonical
workflow-state payload verbatim (``primary_action`` / ``execution_available`` /
``queued_actions`` / ``current_session`` / ``portfolio.proposed_holding_count`` /
``allocations``) with no client date/priority math and no second next-action owner.

Three fixes under test:

  A. Right rail — a queued FUTURE action that is itself not executable
     (``execution_available !== true``) is shown PASSIVELY ("Up next") in a no-op
     (monitor / wait) state, never as a click-now CTA. Executable queued items and
     the single canonical primary CTA are preserved.
  B. Proposed Portfolio header — ONE quiet review-only safety chip (informational),
     no duplicate large "REVIEW ONLY" status pill that read like a CTA.
  C. "25 proposed holdings" stays the canonical target count; the 33-row detail is
     relabelled as the allocation transition / change ledger (and stays reachable).
"""
from __future__ import annotations

import importlib
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


def _banner(ui: str) -> str:
    return _region(ui, "function _wsBannerHtml(d)", "function _wsApplyRightPanel")


def _right_panel(ui: str) -> str:
    return _region(ui, "function _wsApplyRightPanel(d)", "_wsApplyAssessmentFraming")


def _pp_render(ui: str) -> str:
    return _region(ui, "function renderProposedPortfolioView(d)",
                   "window.renderProposedPortfolioView")


def _pp_tab(ui: str) -> str:
    return _region(ui, 'id="tab-proposed-portfolio"', 'id="tab-multi-horizon"')


def _hero(ui: str) -> str:
    return _region(ui, "function _wsRenderTodayHero(d)", "window._wsRenderTodayHero")


# --------------------------------------------------------------------------- #
# A / 1. No-op state: a non-executable queued future action is NOT a click-now CTA
# --------------------------------------------------------------------------- #
def test_1_future_action_not_clickable_cta_in_noop(ui):
    banner = _banner(ui)
    # optional review analyses stay suppressed in a no-op (shown once in the hero) ...
    assert "if (fm && _wsNoAction) return '';" in banner
    # ... and a non-executable, non-review queued item is diverted OUT of the chip map
    # (returns '') into the passive up-next list, so it never renders a role=button chip.
    assert "if (_wsNoAction && !fm && q.execution_available !== true) {" in banner
    assert "upNext.push(_wsUpNextLabel[q.action_code] || q.label);" in banner
    # the reassessment is given a passive NOUN label, not the imperative CTA label.
    assert "'RUN_PORTFOLIO_REASSESSMENT': 'Portfolio reassessment'" in banner


# --------------------------------------------------------------------------- #
# 2. The future reassessment remains visible as passive "Up next" info
# --------------------------------------------------------------------------- #
def test_2_future_action_visible_as_passive_up_next(ui):
    banner = _banner(ui)
    assert "var upNextHtml = upNext.length" in banner
    assert ">Up next</span>" in banner
    # timing wording is read from the canonical primary action_code (never client dates).
    assert "pa.action_code === 'WAIT_FOR_SESSION_CLOSE') ? 'After session close'" in banner
    # the passive block is muted text, never a button/anchor that routes anywhere.
    up = _region(banner, "var upNextHtml = upNext.length", "var btn =")
    assert "onclick" not in up
    assert 'role="button"' not in up
    assert "navigateToRoute" not in up


# --------------------------------------------------------------------------- #
# 3. A genuine executable action still yields exactly one primary CTA
# --------------------------------------------------------------------------- #
def test_3_executable_action_still_gets_one_primary_cta(ui):
    banner = _banner(ui)
    # the single canonical primary button is shown only when there IS a real action.
    assert "var btn = (pa.destination && !_wsNoAction)" in banner
    # executable queued items keep their clickable chip (gated on execution_available).
    assert "q.execution_available !== true" in banner
    panel = _right_panel(ui)
    assert "right-primary-action-btn" in panel
    assert "btn.style.display = ''" in panel  # the primary CTA is shown when action required


# --------------------------------------------------------------------------- #
# 4. Right-rail current-task / next-action do not contradict the future presentation
# --------------------------------------------------------------------------- #
def test_4_right_rail_language_consistent(ui):
    panel = _right_panel(ui)
    assert "No action required right now." in panel
    assert "btn.style.display = 'none'" in panel
    # the no-op derivation is the SAME canonical expression the banner/hero use.
    assert "pa.execution_available !== true" in panel
    banner = _banner(ui)
    assert "pa.execution_available !== true" in banner
    # both derive the no-op flag from the same monitor/wait action codes.
    assert "MONITOR_PORTFOLIO" in panel and "WAIT_FOR_SESSION_CLOSE" in panel
    assert "MONITOR_PORTFOLIO" in banner and "WAIT_FOR_SESSION_CLOSE" in banner


# --------------------------------------------------------------------------- #
# 5. Today compact hero remains intact (not redesigned)
# --------------------------------------------------------------------------- #
def test_5_today_compact_hero_intact(ui):
    hero = _hero(ui)
    assert "host.setAttribute('data-density', 'compact');" in hero
    assert "'today-hero state-ok th-compact'" in hero
    assert "You&rsquo;re all set" in hero
    # the accepted compact geometry contract is untouched.
    assert ".today-hero.th-compact{padding:14px 20px" in ui
    assert ".thc-title{font-size:23px" in ui


# --------------------------------------------------------------------------- #
# 6 & 7. Proposed Portfolio shows exactly ONE review-only treatment (no duplicate)
# --------------------------------------------------------------------------- #
def test_6_proposed_portfolio_single_review_only_context(ui):
    tab = _pp_tab(ui)
    # exactly one REVIEW ONLY treatment in the dedicated header ...
    assert tab.count("REVIEW ONLY") == 1
    # ... consolidated into one quiet informational chip.
    assert "REVIEW ONLY &middot; NO ORDERS" in tab


def test_7_no_duplicate_review_only_pill(ui):
    tab = _pp_tab(ui)
    # the large status pill is hidden by default and never repainted as a second REVIEW ONLY.
    assert 'id="pp-state" style="display:none;"' in tab
    render = _pp_render(ui)
    assert "stateEl.style.display = 'none';" in render
    assert "'REVIEW ONLY'; stateEl.className = 'opx-state ok'" not in render


# --------------------------------------------------------------------------- #
# 8. "N proposed holdings" is the canonical target count
# --------------------------------------------------------------------------- #
def test_8_proposed_holding_count_is_target(ui):
    render = _pp_render(ui)
    assert "proposed_holding_count" in render
    assert " proposed holdings</b> &middot; as of " in render


# --------------------------------------------------------------------------- #
# 9. The 33-row detail is labelled transition / change ledger, NOT proposed holdings
# --------------------------------------------------------------------------- #
def test_9_transition_ledger_label(ui):
    render = _pp_render(ui)
    assert "'Allocation transition &mdash; ' + allocations.length" in render
    assert "allocations.length + ' rows</summary>'" in render
    # the old confusing label is gone.
    assert "Full proposed allocation (" not in render
    # explicit disambiguation subtitle ties the rows to the target count.
    assert "Includes current, added, increased, reduced, and exited positions" in render
    assert "-holding target." in render
    # the row count reads allocations.length (transition rows), never the holding count.
    assert "' + allocations.length + '" in render


# --------------------------------------------------------------------------- #
# 10. The detailed transition rows remain reachable
# --------------------------------------------------------------------------- #
def test_10_detail_rows_reachable(ui):
    render = _pp_render(ui)
    assert "<details" in render
    assert "_ppAllocTable(allocations)" in render


# --------------------------------------------------------------------------- #
# 11. No backend semantic change: UI reads canonical fields, no second next-action calc
# --------------------------------------------------------------------------- #
def test_11_no_backend_semantic_change_ui_reads_canonical(ui):
    banner = _banner(ui)
    # no client date / priority math is introduced in the future-action presentation.
    for forbidden in ("new Date(", "Date.now(", ".getTime("):
        assert forbidden not in banner, f"client date math in banner: {forbidden}"
    # the up-next reads the canonical queued-action execution flag verbatim.
    assert "q.execution_available" in banner
    # it never fabricates a second next-action owner: the no-op flag is the shared
    # canonical expression, and the primary action drives the one CTA.
    assert "var btn = (pa.destination && !_wsNoAction)" in banner


def test_11b_workflow_state_owner_not_forked(ui):
    # the density/decision-flow single-owner invariant is preserved by this tranche.
    assert ui.count("function _wsBannerHtml") == 1
    assert ui.count("function _wsApplyRightPanel") == 1
    assert ui.count("function renderProposedPortfolioView") == 1
    assert ui.count("function loadWorkflowState") == 1


# --------------------------------------------------------------------------- #
# 12. No execution controls introduced by this tranche
# --------------------------------------------------------------------------- #
def test_12_no_execution_controls_introduced(ui):
    for route in ("/v1/operations/reallocation-proposal/apply",
                  "/v1/operations/reallocation-proposal/confirm",
                  "/v1/operations/apply-reallocation",
                  "/v1/operations/promote-model"):
        assert route not in ui, f"forbidden route in UI: {route}"
    banner = _banner(ui)
    render = _pp_render(ui)
    for banned in ("Create Order", "Apply proposal", "Approve proposal",
                   "Confirm target", "Run Daily Close"):
        assert banned not in banner, f"forbidden control in banner: {banned}"
        assert banned not in render, f"forbidden control in proposed view: {banned}"
    # the passive up-next must not contain an execute/apply/confirm verb either.
    up = _region(banner, "var upNextHtml = upNext.length", "var btn =")
    for verb in ("Apply", "Confirm", "Execute", "Rebalance"):
        assert verb not in up, f"actionable verb leaked into passive up-next: {verb}"


# --------------------------------------------------------------------------- #
# Architecture guard — the polish stays audit-green (same guard the phase runs)
# --------------------------------------------------------------------------- #
def test_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_operator_ux_guard_still_green():
    aud = importlib.import_module("scripts.audit_architecture")
    ux = aud.check_operator_ux_consolidation_ownership(aud._iter_source_files())
    assert ux["workflow_next_action_renderer_count"] == 1
    assert ux["missing_safety_tokens"] == []
    assert ux["market_context_present"] is True
    assert ux["primary_areas_present"] and ux["legacy_views_demoted"]
    assert ux["forbidden_new_routes_present"] == []
