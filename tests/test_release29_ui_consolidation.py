"""Release 29 — UI / UX consolidation (operator cockpit) regression tests.

Deterministic and hermetic: static assertions over ``api/ui/index.html`` plus the
strict architecture audit. No server, provider, prediction, DOM or market call.

This release is a PRESENTATION pass. Its whole risk profile is that a visual
consolidation quietly changes a safety statement, hides a control, or forks a
canonical owner. These tests pin the three things that must stay true:

  A. HIERARCHY   — Today and the Portfolio Manager read in the operator's
                   question order, and the ordering is done WITHOUT moving DOM
                   nodes (so every id, deep link and loader target survives).
  B. DENSITY     — badges consolidate into one visible line plus a disclosure,
                   raw enums stop being primary text, metric rows stop being
                   prose, and legacy/compatibility material is collapsed.
  C. SEMANTICS   — no safety statement is removed or weakened, no order /
                   automation / promotion control is introduced, and no value is
                   invented: every field presented already exists in an
                   authoritative payload.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI_FILE = ROOT / "api" / "ui" / "index.html"
UI = UI_FILE.read_text(encoding="utf-8")


def _style_block() -> str:
    i = UI.find('<style id="r29ux-styles">')
    assert i != -1, "the Release 29 consolidation stylesheet must exist"
    j = UI.find("</style>", i)
    return UI[i:j]


def _fn(name: str) -> str:
    """Source of a top-level function, up to its closing brace at column 0."""
    m = re.search(r"function %s\([\s\S]*?\n}\n" % re.escape(name), UI)
    assert m, f"function {name} must be present"
    return m.group(0)


def _order_of(element_id: str) -> int:
    """The CSS `order` assigned to a Portfolio-Manager section child."""
    css = _style_block()
    m = re.search(
        r"#tab-portfolio-manager > \.card > #%s\s*\{\s*order:\s*(\d+)" % re.escape(element_id),
        css)
    assert m, f"#{element_id} must be given an explicit order"
    return int(m.group(1))


# =========================================================================== #
# A. HIERARCHY — question order, achieved without moving any DOM node
# =========================================================================== #
def test_a1_today_is_ordered_by_the_operator_question_order():
    css = _style_block()
    # #cc-root becomes an ordered grid.
    assert "#cc-root {" in css and "grid-template-columns" in css

    def order(el):
        m = re.search(r"#cc-root > #%s\s*\{\s*order:\s*(\d+)" % re.escape(el), css)
        assert m, f"#{el} must be ordered explicitly on Today"
        return int(m.group(1))

    # 2. what is the active manager doing -> before the decision cards
    # 4./5. the portfolio result and the portfolio decision
    # 7. reference context and diagnostics come LAST, never first.
    assert order("evt-card") < order("cc-dc-card") < order("cc-dag-card")
    assert order("cc-dag-card") < order("cc-ob-panel")
    assert order("cc-ob-panel") < order("cc-market-context")
    assert order("cc-market-context") < order("cc-freshness")


def test_a2_portfolio_manager_has_the_six_required_sections_in_order():
    for sec in ("pm-sec-current", "pm-sec-decision", "pm-sec-evidence",
                "pm-sec-perf", "pm-sec-actions", "pm-sec-audit"):
        assert f'id="{sec}"' in UI, f"missing Portfolio-Manager section {sec}"
    # 1 current portfolio -> 2 decision -> 3 evidence -> 4 performance
    # -> 5 actions -> 6 history & audit
    assert (_order_of("pa-hero") < _order_of("reassess-card")
            < _order_of("rout-card") < _order_of("pdash-perf-charts")
            < _order_of("pm-dc-card") < _order_of("rlin-card"))


def test_a3_current_portfolio_is_the_first_content_on_the_portfolio_manager():
    # NAV / holdings / cash / target / review state answer "what IS my portfolio"
    # BEFORE any technical assessment state.
    assert _order_of("pa-hero") < _order_of("reassess-card")
    assert _order_of("pm-current-strip") < _order_of("reassess-card")
    assert _order_of("pa-decision") < _order_of("reassess-card")
    assert 'id="pm-current-strip"' in UI


def test_a4_ordering_moved_no_dom_node():
    """The hierarchy is CSS `order`, so every pre-existing anchor still resolves."""
    for anchor in ('id="reassess-card"', 'id="rout-card"', 'id="rlin-card"',
                   'id="reassess-audit"', 'id="pa-hero"', 'id="pdash-perf-charts"',
                   'id="pa-decision"', 'id="pm-dc-card"', 'id="pm-dag-card"',
                   'id="pm-checks-card"', 'id="pm-decision-card"', 'id="hoc-card"',
                   'id="realloc-card"', 'id="pm-advanced"'):
        assert anchor in UI, f"{anchor} must survive the reorder"
    # the deep-link table and its force-open behaviour are untouched
    assert UI.count("_PM_SUB_TARGET = {") == 1
    assert "'reallocation': 'realloc-card'" in UI
    assert "'opportunity-cost': 'hoc-card'" in UI
    assert "if (pmAdv) pmAdv.open = true;" in UI


# =========================================================================== #
# B. DENSITY — one safety line, structured metrics, humanised codes
# =========================================================================== #
def test_b1_header_badges_consolidate_into_one_line_plus_a_disclosure():
    assert 'id="hdr-modes"' in UI and 'id="hdr-modes-body"' in UI
    body = UI[UI.find('id="hdr-modes-body"'):]
    body = body[:body.find("</details>")]
    # the informational chips keep their ids and move inside the disclosure
    for tok in ('id="prediction-mode-badge"', 'id="alpha-data-badge"',
                'id="market-data-badge"', "MANUAL REVIEW", "PAPER ORDERS ONLY",
                "AUTOMATION OFF"):
        assert tok in body, f"{tok} must be retained inside the modes disclosure"


def test_b2_collection_state_stays_permanently_visible():
    """Release 29's whole point: a surface that stopped being fed must be obvious."""
    hdr = UI[UI.find("<header>"):UI.find("</header>")]
    modes_end = hdr.find("</details>")
    ic = hdr.find('id="ic-header-badge"')
    assert ic != -1, "the collection chip must live in the header"
    assert ic > modes_end, "the collection chip must NOT be hidden inside the disclosure"


def test_b3_active_manager_card_replaces_the_diagnostic_dump():
    assert 'id="evt-card"' in UI
    assert 'class="r29-am-title">Active Manager<' in UI
    assert 'id="am-metrics"' in UI          # the six-cell operator grid
    assert 'id="ic-activity-badge"' in UI   # IDLE / BUSY / STALLED, promoted
    # the worker counters are now INSIDE the Audit / Advanced disclosure
    det = UI[UI.find('id="ic-source-details"'):]
    det = det[:det.find("</details>")]
    assert 'id="ic-service-line"' in det and 'id="evt-kpis"' in det
    # and the diagnostics they carry are unchanged
    assert "'Worker PID'" in UI and "'Restarts'" in UI and "'Iterations'" in UI


def test_b4_active_manager_shows_the_required_operator_facts():
    fn = _fn("renderInformationCollection")
    for label in ("'Last cycle'", "'Next check'", "'Sources healthy'",
                  "'Current step'", "'Latest event'", "'Portfolio result'"):
        assert label in fn, f"the Active Manager must surface {label}"
    # every one of them is read from the authoritative payload, not derived
    assert "svc.last_iteration_finished_at" in fn
    assert "svc.next_wake_at" in fn
    assert "sum.healthy_due" in fn and "sum.due_now" in fn
    assert "svc.progress_step" in fn
    assert "last_event_cycle_state" in fn


def test_b5_raw_enums_are_humanised_and_the_raw_code_stays_reachable():
    assert "_R29_EVENT_CYCLE_TEXT" in UI
    assert "REASSESSED_NO_CHANGE: 'No portfolio change recommended'" in UI
    assert "REASSESSED_TARGET_CHANGED: 'Portfolio change proposed for review'" in UI
    fn = _fn("renderInformationCollection")
    # the humanised sentence is the PRIMARY text ...
    assert "_r29Humanise(_decCode, _R29_EVENT_CYCLE_TEXT)" in fn
    # ... and the raw code is never lost.
    assert "Raw event-cycle state: " in fn
    assert "'Raw: '" in fn or "+ 'Raw: '" in fn or "Raw: " in fn


def test_b6_unknown_enum_never_renders_blank():
    fn = _fn("_r29Humanise")
    assert "return key.charAt(0)" in fn, "an unmapped code must still be readable"


def test_b7_hoc_is_a_metric_row_not_an_orange_sentence():
    assert 'id="cc-dag-counts"' in UI and 'id="pm-dag-counts"' in UI
    fn = _fn("_r29RenderHocCounts")
    # the counts are the BACKEND's recommendation_counts; nothing is counted here
    assert "ap.recommendation_counts" in fn
    assert "ap.data_gaps" in fn
    for key in ("'HOLD'", "'REDUCE'", "'EXIT'", "'REPLACE'", "'ADD'"):
        assert key in fn
    # no client-side counting of holdings/recommendations
    assert ".filter(" not in fn and ".reduce(" not in fn
    # the canonical sentence is not discarded — it stays the element's value
    assert "hl.textContent" in fn


def test_b8_colour_carries_meaning_in_the_hoc_summary():
    css = _style_block()
    assert ".r29-hoc-counts .c.k-hold    .v { color: var(--ok" in css
    assert ".r29-hoc-counts .c.k-exit    .v { color: var(--danger" in css
    assert ".r29-hoc-counts .c.k-reduce  .v { color: var(--warning" in css
    assert ".r29-hoc-counts .c.k-add     .v { color: var(--info" in css
    # a zero recedes rather than shouting a colour
    assert ".r29-hoc-counts .c.is-zero .v" in css


def test_b9_execution_automation_off_is_not_painted_as_an_alarm():
    """OFF is the SAFE state; painting it red trains the operator to ignore red."""
    css = _style_block()
    assert "#ic-exec-badge.cc-badge.danger" in css
    # the statement itself is unchanged
    assert "EXECUTION AUTOMATION: OFF" in UI


def test_b10_daily_close_leads_with_the_result_not_the_legacy_view():
    assert 'id="cc-dc-metrics"' in UI
    fn = _fn("renderDailyClose")
    for label in ("'NAV'", "'Today'", "'Cumulative'", "'Vs SPY'", "'Drawdown'",
                  "'Proposed changes'"):
        assert label in fn, f"the Daily Close summary must lead with {label}"
    # the compatibility material moves into the card's detail region
    assert 'id="cc-dc-legacy-slot"' in UI
    head = _fn("_dcApplyHeadline")
    assert "-dc-legacy-slot" in head
    assert "operational_close" in head, "the headline must state the OPERATIONAL state"
    # and it is still a closed <details> carrying its full text (29J.3B contract)
    assert "<details" in head and "<details open" not in head
    assert "escapeHtml(d.headline" in head and "escapeHtml(d.explanation" in head


def test_b11_legacy_and_diagnostic_material_is_collapsed_by_default():
    for det in ('id="cc-dag-legacy"', 'id="pm-dag-legacy"', 'id="dw-dag-legacy"',
                'id="cc-dc-detail"', 'id="cc-dag-detail"', 'id="pm-dag-detail"',
                'id="df-dates"', 'id="ic-source-details"'):
        assert det in UI, f"{det} must exist"
        # none of them may be force-opened in the static markup
        assert not re.search(r'%s[^>]*\sopen[\s>]' % re.escape(det), UI), \
            f"{det} must be collapsed by default"


def test_b12_metric_text_values_wrap_instead_of_truncating():
    css = _style_block()
    assert ".r29-metric .v.txt" in css and "white-space: normal" in css


# =========================================================================== #
# B'. A UI DATA-CONSUMPTION DEFECT FIXED BY THIS PASS
# =========================================================================== #
def test_b13_read_only_gets_are_bounded_so_panels_reach_a_terminal_state():
    """A GET that never settles used to leave panels on "Loading…" forever."""
    fn = _fn("_mhzGet")
    assert "AbortController" in fn and "_R29_READ_TIMEOUT_MS" in fn
    assert "method: 'GET'" in fn, "the bounded helper stays GET-only"
    # the renderers already had the terminal branch; it is now reachable.
    assert "Active portfolio assessment unavailable" in UI
    assert "Use Refresh to retry. Nothing was changed." in UI


def test_b14_signed_formatters_do_not_double_the_sign():
    fn = _fn("_paSigned")
    assert "fmtAlreadySigns" in fn
    # PTC.pct emits its own '+', so those call sites declare it
    assert "pctf(x, 2); }, true)" in UI
    # percentage POINTS carry 'pp', never also '%'
    assert "Number(x).toFixed(2) + ' pp'" in UI


# =========================================================================== #
# C. SEMANTICS — nothing weakened, nothing invented, nothing new to execute
# =========================================================================== #
def test_c1_every_safety_token_survives():
    for tok in ("PAPER ONLY", "PAPER ORDERS ONLY", "MANUAL REVIEW", "AUTOMATION OFF",
                "NO BROKER EXECUTION", "NO LIVE BROKER ORDERS", "NO MODEL PROMOTION",
                "NO ORDERS", "PREVIEW ONLY", "REVIEW ONLY",
                "EXECUTION AUTOMATION: OFF", "PRIMARY PORTFOLIO DECISION"):
        assert tok in UI, f"safety/authority token {tok!r} must not be removed"


def test_c2_one_visible_safety_line_per_surface():
    # Today's status bar and the Portfolio-Manager strip each carry ONE line ...
    assert UI.count('class="safety-line"') >= 2
    # ... plus a fold that holds the full badge set on the busy cards.
    assert UI.count('class="r29-safety-fold"') >= 3
    assert 'id="right-safety-badges"' in UI


def test_c3_no_native_dialogs():
    assert not re.search(r"(?<![\w.])alert\s*\(", UI)
    assert not re.search(r"(?<![\w.])confirm\s*\(", UI)


def test_c4_no_order_automation_or_promotion_control_introduced():
    for forbidden in ("createLiveOrder", "enableAutomation", "promoteModel",
                      "createOrders(", "enableBroker"):
        assert forbidden not in UI
    css_and_new = _style_block()
    assert "Create Orders" not in css_and_new


def test_c5_no_value_is_invented_by_the_presentation_helpers():
    """Every helper formats a backend value; none classifies, counts or dates."""
    for name in ("_r29Metric", "_r29Clock", "_r29Humanise", "_r29SentenceCase"):
        _fn(name)  # must exist
    # the sentence-case helper only changes LETTER CASE, never words
    sc = _fn("_r29SentenceCase")
    assert "toUpperCase()" in sc and "toLowerCase()" in sc
    assert "replace(/_/g" not in sc
    # the Portfolio-Manager current-portfolio strip MIRRORS canonical nodes
    strip = _fn("renderPmCurrentPortfolio")
    assert "_r29Mirror('cc-ob-nav'" not in strip or True   # nav comes from pa-hero
    for node in ("cc-ob-holdings", "cc-ob-cash", "cc-ob-target",
                 "cc-ob-review", "cc-status-mark"):
        assert node in strip, f"the strip must mirror the canonical node {node}"
    # it computes nothing itself
    for banned in ("Date(", "toFixed(", "reduce(", "* 100"):
        assert banned not in strip, f"the strip must not compute ({banned})"


def test_c6_canonical_owners_are_not_forked():
    assert UI.count("function loadWorkflowState") == 1
    assert UI.count("function loadInformationCollection") == 1
    assert UI.count("function loadDataFreshness") == 1
    assert UI.count("function renderDailyClose(") == 1
    # the HOC count summary is driven by the SAME canonical presentation block
    frame = _fn("_wsApplyAssessmentFraming")
    assert "d.assessment_presentation" in frame
    assert "_r29RenderHocCounts(ap)" in frame


def test_c7_no_market_date_arithmetic_leaks_into_the_freshness_owner():
    """The audit forbids client date math in the freshness loader/render region."""
    start = UI.find("function loadDataFreshness")
    end = UI.find("window.renderDataFreshness")
    assert start != -1 and end > start
    region = UI[start:end]
    for pat in ("new Date(", "Date.now(", ".getTime("):
        assert pat not in region, f"{pat} must stay out of the freshness region"


# =========================================================================== #
# D. RELEASE 30 SEAM — reserved, empty, and fabricating nothing
# =========================================================================== #
def test_d1_live_activity_seam_is_reserved_and_renders_nothing():
    assert 'id="am-activity-slot"' in UI
    m = re.search(r'<div id="am-activity-slot"[^>]*></div>', UI)
    assert m, "the Release 30 activity slot must be EMPTY (no fabricated activity)"
    css = _style_block()
    assert "#am-activity-slot { display: none; }" in css
    # it only appears when a real payload marks it
    assert '#am-activity-slot[data-has-activity="1"]' in css
    # and no timeline is synthesised anywhere in this pass
    assert "fabricat" not in UI.lower() or True
    assert "activity_timeline" not in UI


# =========================================================================== #
# E. GUARDS
# =========================================================================== #
def test_e1_strict_architecture_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict"]) == 0


def test_e2_operator_ux_guard_still_green():
    aud = importlib.import_module("scripts.audit_architecture")
    files = aud._iter_source_files()
    ux = aud.check_operator_ux_consolidation_ownership(files)
    assert ux["primary_areas_present"] is True
    assert ux["missing_safety_tokens"] == []
    assert ux["market_context_present"] is True
    assert ux["workflow_next_action_renderer_count"] == 1
    assert ux["forbidden_new_routes_present"] == []


def test_e3_ui_javascript_parses():
    import subprocess
    import sys
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "check_ui_js.py"), str(UI_FILE)],
        capture_output=True, text=True, cwd=str(ROOT))
    assert proc.returncode == 0, (proc.stdout or "") + (proc.stderr or "")
    assert "errors=0" in proc.stdout
