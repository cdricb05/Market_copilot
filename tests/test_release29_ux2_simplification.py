"""Release 29 UX2 — RADICAL OPERATOR SIMPLIFICATION regression tests.

The previous pass improved hierarchy and user acceptance still FAILED, because Today and
Portfolio carried far too much information. The standing product rule this file defends:

    IF THE OPERATOR CANNOT ACT ON IT, AND DOES NOT NEED IT TO MAKE A PORTFOLIO DECISION,
    IT DOES NOT BELONG ON TODAY OR PORTFOLIO.

A removal pass carries the mirror image of a consolidation pass's risk. Reordering can
hide something; REMOVING can lose it, can fork a canonical owner to feed the new surface,
or can quietly mean "deleted from the DOM" and break a loader's write target. So these
tests pin four things:

  A. MARKETS   — a new read-only reference area exists, it OWNS nothing, and the whole
                 market dashboard is there rather than on Today.
  B. TODAY     — only the five permitted regions remain, and everything removed MOVED to
                 exactly one destination with its id intact.
  C. PORTFOLIO — evidence, checks, lineage, raw history and the duplicate Daily Close left
                 the primary route; performance & risk stayed, complete.
  D. INTEGRITY — no id was deleted, no market owner was forked, no safety statement was
                 removed, and the right diagnostic rail is gone from the operating screens
                 while its markup (and every write target in it) survives.

Hermetic and deterministic: static assertions over ``api/ui/index.html`` plus the strict
architecture audit. No server, browser, provider or market call.
"""
from __future__ import annotations

import importlib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI_FILE = ROOT / "api" / "ui" / "index.html"
UI = UI_FILE.read_text(encoding="utf-8")


def _region(start: str, end: str) -> str:
    i = UI.find(start)
    assert i != -1, f"missing region start: {start}"
    j = UI.find(end, i)
    assert j != -1, f"missing region end: {end}"
    return UI[i:j]


TODAY = _region('<div id="tab-overview" class="tab-content active">',
                "<!-- end tab-overview -->")
MARKETS = _region('<div id="tab-markets" class="tab-content">', "<!-- end tab-markets -->")
AUDIT_TAB = _region('<div id="tab-audit-advanced" class="tab-content">',
                    "<!-- end tab-audit-advanced -->")
SYSOPS = AUDIT_TAB[AUDIT_TAB.find('<div class="card" id="sysops-panel"'):
                   AUDIT_TAB.find("<!-- One page-level safety strip")]


def _ux2_style() -> str:
    i = UI.find('<style id="r29ux2-styles">')
    assert i != -1, "the Release 29 UX2 stylesheet must exist"
    return UI[i:UI.find("</style>", i)]


def _fn(name: str) -> str:
    m = re.search(r"function %s\([\s\S]*?\n}\n" % re.escape(name), UI)
    assert m, f"function {name} must be present"
    return m.group(0)


def _audit():
    return importlib.import_module("scripts.audit_architecture")


# =========================================================================== #
# A. MARKETS — a new read-only reference area that owns nothing
# =========================================================================== #
def test_a1_markets_is_a_primary_operator_area():
    assert 'id="nav-markets"' in UI and 'data-route="markets"' in UI
    assert "'markets': 'markets'" in UI, "the markets route must resolve to its own tab"
    assert 'id="tab-markets"' in UI


def test_a2_navigation_is_operate_research_system():
    nav = _region('<div class="sidebar">', "</div>\n\n  <main")
    for label in ('<div class="sidebar-label">Operate</div>',
                  '<div class="sidebar-label">Research</div>',
                  '<div class="sidebar-label">System</div>'):
        assert label in nav, f"missing primary navigation group: {label}"
    operate = nav[nav.find("Operate</div>"):nav.find("Research</div>")]
    for item in ('id="nav-command-center"', 'id="nav-portfolio-manager"', 'id="nav-markets"'):
        assert item in operate, f"{item} belongs in OPERATE"


def test_a3_markets_carries_market_now_trend_and_regime():
    # MARKET NOW — equities, cross-asset, rates / dollar
    for key in ("sp500", "nasdaq", "dow", "vix", "eurusd", "gold",
                "wti", "brent", "us10y", "us2y", "usd_broad"):
        assert ('data-key="%s"' % key) in MARKETS, f"Markets must show {key}"
    # MARKET TREND + MARKET REGIME
    assert 'id="cc-mv-spark-grid"' in MARKETS
    assert 'id="cc-mv-regime"' in MARKETS
    assert "Market Trend" in MARKETS and "Market Regime" in MARKETS


def test_a4_markets_states_it_is_reference_context_not_a_signal():
    assert "REFERENCE CONTEXT &mdash; NOT A PORTFOLIO SIGNAL" in MARKETS
    assert "Read only" in MARKETS


def test_a5_markets_introduces_no_new_owner_and_no_provider_call():
    """It renders the SAME authoritative payloads; it fetches nothing of its own."""
    assert UI.count("async function loadMarketDashboard") == 1
    assert UI.count("function loadMarketContext(") == 1
    assert UI.count("call('GET', '/v1/market/indicators')") == 1
    for host in ("query1.finance.yahoo", "finance.yahoo.com", "stlouisfed.org", "fredgraph"):
        assert host not in UI, "the browser must never call a market provider directly"
    # the tab itself declares no fetch
    assert "call('GET'" not in MARKETS and "fetch(" not in MARKETS


def test_a6_markets_carries_no_infrastructure_diagnostics():
    for tok in ("Worker PID", "heartbeat", "restart_count", "store path",
                "orchestrator", "iteration"):
        assert tok.lower() not in MARKETS.lower(), f"Markets must not show {tok}"


# =========================================================================== #
# B. TODAY — only what the operator can act on
# =========================================================================== #
def test_b1_today_keeps_only_the_five_permitted_regions():
    for keep in ('id="evt-card"',          # 2 Active Manager
                 'id="am-metrics"',
                 'id="cc-dc-card"',        # 3 Portfolio snapshot
                 'id="cc-dag-card"',       # 4 Opportunity cost
                 'id="cc-dag-counts"',
                 'id="cc-ob-panel"',
                 'id="today-market-strip"'):  # 5 one compact market strip
        assert keep in TODAY, f"Today must keep {keep}"
    # 1 Operator command is the ONE page-level command bar, above the tabs
    assert 'id="operator-command"' in UI


def test_b2_the_market_dashboard_left_today():
    for gone in ('id="cc-market-context"', 'id="cc-market-visuals"',
                 'id="cc-mv-spark-grid"', 'id="cc-mv-regime"',
                 'data-key="nasdaq"', 'data-key="gold"'):
        assert gone not in TODAY, f"{gone} must not be on Today any more"


def test_b3_diagnostics_left_today_for_system_audit():
    for gone in ('id="cc-freshness"', 'id="df-dates"', 'id="cc-research-strip"',
                 'id="ic-source-details"', 'id="ic-sources"', 'id="ic-service-line"',
                 'id="evt-kpis"', 'id="evt-events"', 'id="evt-affected"',
                 'id="ic-decision"'):
        assert gone not in TODAY, f"{gone} must not be on Today any more"
        assert gone in SYSOPS, f"{gone} must be reachable in System · Audit"


def test_b4_worker_counters_are_off_today_but_not_lost():
    """Worker PID / restarts / iterations / heartbeat: removed from Today, kept in audit."""
    for tok in ("'Worker PID'", "'Restarts'", "'Iterations'", "'Heartbeat'"):
        assert tok in UI, f"the diagnostic {tok} must not be deleted"
    assert 'id="ic-service-line"' in SYSOPS


def test_b5_the_moved_panel_is_routed_only_under_system_audit():
    assert "'diagnostics':      { panels: ['sysops-panel'" in UI
    assert "var _RA_ALL_PANELS = ['sysops-panel'" in UI


def test_b6_today_keeps_one_compact_market_strip_of_four_instruments():
    strip = _region('id="today-market-strip"', "</div>\n\n          <div class=\"cc-updated\"")
    keys = re.findall(r'data-mkey="([a-z0-9_]+)"', strip)
    assert keys == ["sp500", "vix", "us10y", "wti"], keys
    assert "navigateToRoute('markets')" in strip, "the strip must link to Markets"


def test_b7_the_today_strip_is_a_mirror_not_a_second_owner():
    fn = _fn("_r29ux2RenderTodayMarketStrip")
    assert ".ov-market-card[data-key=" in fn, "it must read the authoritative tiles"
    for banned in ("call('GET'", "fetch(", "_mhzGet(", "parseFloat(", "toFixed(",
                   "Number(", "Date("):
        assert banned not in fn, f"the mirror must not {banned}"


def test_b8_duplicate_state_statements_are_removed_from_today():
    css = _ux2_style()
    for dup in ("#today-hero", "#cc-root > #wf-cc", "#cc-root > #cc-realloc-card"):
        assert dup in css, f"{dup} must be removed from the Today surface"
    # the elements themselves survive, so their canonical writers still resolve
    for el in ('id="today-hero"', 'id="wf-cc"', 'id="cc-realloc-card"'):
        assert el in UI


def test_b9_active_manager_shows_the_current_step_only_while_busy():
    fn = _fn("renderInformationCollection")
    assert "if (_amBusy) {" in fn
    assert "_amCells.splice(3, 0, _r29Metric('Current step'" in fn
    # the grid still matches the number of cells actually rendered
    assert "'r29-metrics cols-' + _amCells.length" in fn
    assert ".r29-metrics.cols-5" in UI


def test_b10_opportunity_cost_links_to_portfolio():
    assert 'id="cc-dag-open-portfolio"' in TODAY
    assert "navigateToRoute('portfolio-manager')" in TODAY


# =========================================================================== #
# C. PORTFOLIO — decision first, evidence elsewhere
# =========================================================================== #
def test_c1_evidence_and_history_left_the_primary_portfolio_route():
    css = _ux2_style()
    for gone in ("#pm-sec-evidence", "#rout-card", "#pm-checks-card",
                 "#pm-sec-audit", "#rlin-card", "#reassess-audit",
                 "#reassess-alternatives-card", "#pm-dc-card"):
        assert ("#tab-portfolio-manager > .card > %s" % gone) in css, \
            f"{gone} must leave the primary Portfolio route"
        assert gone.lstrip("#") in UI, f"{gone} must not be deleted"


def test_c2_performance_and_risk_is_kept_complete():
    for chart in ("Performance vs SPY", "Daily &amp; Cumulative P&amp;L", "Drawdown",
                  "Allocation &amp; Concentration", "Contributors / Detractors",
                  "Drift vs Target"):
        assert chart in UI, f"Performance & risk must keep {chart}"
    assert 'id="pdash-perf-charts"' in UI and 'id="pm-sec-perf"' in UI


def test_c3_current_portfolio_answers_the_first_question():
    for node in ('id="pa-hero"', 'id="pm-current-strip"', 'id="pa-decision"'):
        assert node in UI
    strip = _fn("renderPmCurrentPortfolio")
    for mirrored in ("cc-ob-holdings", "cc-ob-cash", "cc-ob-target",
                     "cc-ob-review", "cc-status-mark"):
        assert mirrored in strip, f"section 1 must mirror the canonical node {mirrored}"


def test_c4_the_decision_is_one_human_statement_plus_hoc_counts():
    css = _ux2_style()
    # artifact ids, policy versions, the scope essay and the duplicate blocking code go
    assert "#reassess-card #reassess-meta" in css
    assert "#reassess-card #reassess-scope" in css
    assert "#reassess-card #reassess-blockers" in css
    # the HOC card contributes COUNTS; its sentence would be a second headline
    assert "#pm-dag-card #pm-dag-headline" in css
    assert 'id="pm-dag-counts"' in UI
    # the blocked reason itself is still rendered
    assert 'id="reassess-explanation"' in UI


def test_c5_only_assisting_decision_metrics_are_drawn():
    fn = _fn("renderPortfolioReassessment")
    assert "var _rsKept = kpis.filter(" in fn
    assert "if (i === 0) return true;" in fn, "the decision cell always renders"
    assert "_setHtml('reassess-kpis', _rsKept.map(" in fn


def test_c6_the_action_section_exists_only_when_an_action_exists():
    src = _region("['cc-ob-primary-btn', 'pm-primary-next-btn'].forEach",
                  "// Phase 27B.8")
    assert "_r29ux2HasAction = !!(ob && nextCode)" in src
    assert "['pm-sec-actions', 'pm-decision-card']" in src


def test_c7_raw_order_history_and_model_state_left_the_action_card():
    css = _ux2_style()
    for gone in ("#pm-decision-card #pm-lc-strip", "#pm-decision-card #pm-impl-strip",
                 "#pm-decision-card #pm-plan-line"):
        assert gone in css
    # and the audit material is still reachable in the intentional drill-downs
    assert 'id="pm-order-fill-history"' in UI
    assert 'id="pm-adv-exec"' in UI and 'id="pm-advanced"' in UI


# =========================================================================== #
# D. INTEGRITY — nothing lost, nothing forked, nothing weakened
# =========================================================================== #
def test_d1_the_right_diagnostic_rail_is_gone_from_the_operating_screens():
    css = _ux2_style()
    for route in ("command-center", "portfolio-manager", "markets",
                  "holding-review", "proposed-portfolio"):
        assert ('body[data-route="%s"] .right-panel' % route) in css, route
    assert "document.body.setAttribute('data-route', base)" in UI


def test_d2_the_rail_markup_and_every_write_target_survive():
    assert '<div class="right-panel">' in UI
    for node in ('id="right-current-task"', 'id="right-next-action"',
                 'id="right-ob-nav"', 'id="right-ob-cash"', 'id="right-safety-footer"',
                 'id="right-dc-badge"', 'id="right-dag-badge"'):
        assert node in UI, f"{node} must stay a live write target"


def test_d3_every_moved_id_still_exists_exactly_once():
    for tok in ('id="cc-market-context"', 'id="cc-market-visuals"', 'id="cc-freshness"',
                'id="cc-research-strip"', 'id="ic-source-details"', 'id="evt-events"',
                'id="evt-affected"', 'id="ic-decision"', 'id="ic-sources"',
                'id="evt-kpis"', 'id="ic-service-line"'):
        assert UI.count(tok) == 1, f"{tok} must exist exactly once after the move"


def test_d4_no_safety_statement_was_removed():
    for tok in ("PAPER ONLY", "PAPER ORDERS ONLY", "MANUAL REVIEW", "AUTOMATION OFF",
                "NO BROKER EXECUTION", "NO LIVE BROKER ORDERS", "NO MODEL PROMOTION",
                "NO ORDERS", "PREVIEW ONLY", "REVIEW ONLY",
                "EXECUTION AUTOMATION: OFF", "PRIMARY PORTFOLIO DECISION"):
        assert tok in UI, f"safety/authority token {tok!r} must not be removed"
    # the persistent global chrome still states the operating mode and collection state
    hdr = _region("<header>", "</header>")
    assert "Paper" in hdr and "Manual review" in hdr and "Automation off" in hdr
    assert 'id="ic-header-badge"' in hdr


def test_d5_no_order_automation_or_promotion_control_introduced():
    for forbidden in ("createLiveOrder", "enableAutomation", "promoteModel",
                      "createOrders(", "enableBroker"):
        assert forbidden not in UI
    assert "Create Orders" not in MARKETS


def test_d6_no_native_dialogs():
    assert not re.search(r"(?<![\w.])alert\s*\(", UI)
    assert not re.search(r"(?<![\w.])confirm\s*\(", UI)


def test_d7_every_legacy_route_still_resolves():
    for route in ("'command-center':", "'portfolio':", "'research-audit':",
                  "'daily-workflow':", "'multi-horizon':", "'portfolio-manager':",
                  "'alpha-portfolio':", "'today':", "'research':", "'system-audit':",
                  "'holding-review':", "'proposed-portfolio':", "'markets':"):
        assert route in UI, f"route {route} must still resolve"


# =========================================================================== #
# E. GUARDS
# =========================================================================== #
def test_e1_ux2_architecture_guard_is_green():
    aud = _audit()
    rep = aud.check_release29_ux2_simplification(aud._iter_source_files())
    assert rep["regions_still_on_today"] == []
    assert rep["regions_missing_on_markets"] == []
    assert rep["regions_missing_on_system_audit"] == []
    assert rep["moved_ids_duplicated_or_lost"] == []
    assert rep["today_market_strip_forbidden_calls"] == []
    assert rep["market_dashboard_owner_count"] == 1
    assert rep["market_context_owner_count"] == 1
    assert rep["portfolio_regions_not_removed"] == []
    assert rep["portfolio_regions_lost"] == []
    assert rep["rail_markup_retained"] is True


def test_e2_ux2_guard_is_wired_into_the_strict_gate():
    aud = _audit()
    fields = {f for k, f, _ in aud.BLOCKING_INVARIANTS
              if k == "release29_ux2_simplification"}
    for required in ("regions_still_on_today", "regions_missing_on_markets",
                     "regions_missing_on_system_audit", "moved_ids_duplicated_or_lost",
                     "today_market_strip_is_a_mirror", "rail_markup_retained",
                     "portfolio_regions_lost"):
        assert required in fields, f"{required} must make --strict fail"


def test_e3_strict_architecture_audit_exit_zero():
    assert _audit().main(["--strict"]) == 0


def test_e4_ui_javascript_parses():
    proc = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "check_ui_js.py"), str(UI_FILE)],
        capture_output=True, text=True, cwd=str(ROOT))
    assert proc.returncode == 0, (proc.stdout or "") + (proc.stderr or "")
    assert "errors=0" in proc.stdout
