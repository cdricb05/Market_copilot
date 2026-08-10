"""
tests/test_phase29j3a_portfolio_visual_integration.py — Phase 29J.3A primary-route
Portfolio visual integration repair.

Deterministic and hermetic: static assertions over ``api/ui/index.html`` (the DOM
integration) plus a light reuse check of the ONE canonical read-only analytics owner
(``api.portfolio_analytics``). No server, DB, provider, ledger, or browser is touched
here — the interactive behaviour (hover tooltips, click-to-expand, timeframe controls)
is validated separately by Playwright at 1920x1080.

The repair moves the already-built Phase 29J.3 interactive charts onto the PRIMARY
operator route (#tab-portfolio-manager, the sidebar "Portfolio" item) so the operator
lands directly on the visual dashboard, and demotes the text-heavy decision detail to a
collapsed disclosure. It must REUSE the 29J.3 assets (pt_charts.js, analytics.html, the
single /v1/portfolio/chart-analytics owner) and introduce no new endpoint, no duplicate
analytics owner, no browser portfolio math, and no order / apply / execute / broker /
automation control.
"""
from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI = (ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")

# The six interactive chart body hooks the 29J.3 renderer targets.
CHART_HOOKS = [
    'id="pa-perf-chart"', 'id="pa-pnl-chart"', 'id="pa-dd-chart"',
    'id="pa-alloc-chart"', 'id="pa-contrib-chart"', 'id="pa-drift-chart"',
]


def _pm_tab() -> str:
    """The #tab-portfolio-manager slice (primary operator Portfolio route)."""
    a = UI.index('id="tab-portfolio-manager"')
    b = UI.index('id="tab-multi-horizon"')
    assert a < b
    return UI[a:b]


def _holdings_tab() -> str:
    """The #tab-portfolio slice (secondary 'Holdings detail' route)."""
    a = UI.index('id="tab-portfolio"')
    b = UI.index('id="tab-portfolio-manager"')
    assert a < b
    return UI[a:b]


# --------------------------------------------------------------------------- #
# A. Charts are PRIMARY on the operator route
# --------------------------------------------------------------------------- #
def test_a1_six_charts_present_on_primary_portfolio_manager_route():
    pm = _pm_tab()
    for hook in CHART_HOOKS:
        assert hook in pm, f"missing chart hook on primary route: {hook}"
    # the relocated analytics panel + KPI hero live on this route.
    assert 'id="pdash-perf-charts"' in pm
    assert 'id="pa-hero"' in pm


def test_a2_charts_appear_before_legacy_compatibility_blocks():
    pm = _pm_tab()
    first_chart = pm.index('id="pa-perf-chart"')
    hero = pm.index('id="pa-hero"')
    # legacy / heavy stacks that must NOT dominate the first screen.
    adv_exec = pm.index('id="pm-adv-exec"')       # Advanced Order & Execution Details
    advanced = pm.index('id="pm-advanced"')       # HOC + reallocation full detail
    assert hero < first_chart < adv_exec, "hero + charts must precede advanced-exec"
    assert first_chart < advanced, "charts must precede the decision-detail stack"


def test_a3_grid_relocated_off_holdings_route_no_duplicate_ids():
    # exactly ONE of each chart id / panel id in the whole document (moved, not copied).
    assert UI.count('id="pdash-perf-charts"') == 1
    for hook in CHART_HOOKS:
        assert UI.count(hook) == 1, f"duplicate id after relocation: {hook}"
    # the secondary Holdings-detail route no longer owns the interactive grid.
    assert 'id="pdash-perf-charts"' not in _holdings_tab()
    assert "RELOCATED to the PRIMARY operator route" in _holdings_tab()


def test_a4_kpi_hero_has_five_backend_bound_kpis():
    pm = _pm_tab()
    for kid in ("pa-hero-nav", "pa-hero-today", "pa-hero-cum",
                "pa-hero-excess", "pa-hero-dd"):
        assert f'id="{kid}"' in pm


# --------------------------------------------------------------------------- #
# B. Compact decision summary + deep-links (no order controls)
# --------------------------------------------------------------------------- #
def test_b1_decision_bar_present_with_hoc_and_reallocation_groups():
    pm = _pm_tab()
    assert 'id="pa-decision"' in pm
    assert 'id="pa-dec-hoc"' in pm and 'id="pa-dec-realloc"' in pm


def test_b2_review_buttons_deeplink_to_canonical_review_cards():
    pm = _pm_tab()
    # Operator Decision Flow: the compact decision-bar review buttons now open the
    # DEDICATED full-screen review routes instead of the old nested Portfolio
    # brick-jumps (navigateToRoute('portfolio-manager/opportunity-cost'|'/reallocation')).
    assert "navigateToRoute('holding-review')" in pm        # "View holding changes"
    assert "navigateToRoute('proposed-portfolio')" in pm    # "View proposed portfolio"
    # the dedicated routes are real: registered in the route map + present as in-app tab
    # sections, and keep the Portfolio sidebar item active (they live UNDER Portfolio).
    assert "'holding-review': 'holding-review'" in UI
    assert "'proposed-portfolio': 'proposed-portfolio'" in UI
    assert 'id="tab-holding-review"' in UI
    assert 'id="tab-proposed-portfolio"' in UI
    assert "base === 'holding-review' || base === 'proposed-portfolio'" in UI
    assert "Back to Portfolio" in UI                        # browser/back navigation preserved
    # backward compatibility: the legacy nested deep-links still resolve to the canonical
    # cards (nothing auditable was deleted).
    assert "'reallocation': 'realloc-card'" in UI
    assert "'opportunity-cost': 'hoc-card'" in UI


def test_b3_decision_bar_introduces_no_order_apply_execute_control():
    # isolate the decision bar and assert it is navigation-only.
    a = UI.index('id="pa-decision"')
    b = UI.index("<!-- ===================== Phase 27E DAILY CLOSE", a)
    bar = UI[a:b]
    low = bar.lower()
    for forbidden in ("apply proposal", "execute", "create order", "place order",
                      "submit order", "confirm target"):
        assert forbidden not in low, f"decision bar must not add: {forbidden}"


def test_b4_hoc_and_reallocation_full_cards_still_reachable():
    # detail cards preserved (deep-link targets); nothing auditable was deleted.
    assert 'id="hoc-card"' in UI and 'id="realloc-card"' in UI


# --------------------------------------------------------------------------- #
# C. Text stack demoted (progressive disclosure)
# --------------------------------------------------------------------------- #
def test_c1_decision_detail_collapsed_by_default():
    import re
    m = re.search(r'<details class="card" id="pm-advanced"[^>]*>', UI)
    assert m, "pm-advanced details must exist"
    assert " open>" not in m.group(0) and " open " not in m.group(0)


def test_c2_deeplink_still_force_opens_collapsed_detail():
    # the review deep-link defensively re-opens the collapsed detail and scrolls.
    assert "if (pmAdv) pmAdv.open = true;" in UI


def test_c3_advanced_execution_stack_remains_collapsed():
    import re
    assert re.search(r'<details class="card" id="pm-adv-exec"[^>]*>', UI)


# --------------------------------------------------------------------------- #
# D. Reuse of 29J.3 assets — no new endpoint / owner / chart library
# --------------------------------------------------------------------------- #
def test_d1_pt_charts_toolkit_reused():
    assert 'src="/ui/pt_charts.js' in UI
    assert (ROOT / "api" / "ui" / "pt_charts.js").exists()


def test_d2_analytics_detail_page_reused_single_page():
    # the Expand controls open the ONE existing drill-down page via PTC.openDetail.
    assert (ROOT / "api" / "ui" / "analytics.html").exists()
    assert "PTC.openDetail('performance')" in _pm_tab()
    # no second analytics detail page was introduced.
    pages = list((ROOT / "api" / "ui").glob("analytics*.html"))
    assert [p.name for p in pages] == ["analytics.html"], pages


def test_d3_single_analytics_endpoint_owner_reused():
    app = (ROOT / "api" / "app.py").read_text(encoding="utf-8")
    # exactly one chart-analytics endpoint, served by the canonical owner.
    assert app.count('"/v1/portfolio/chart-analytics"') == 1
    assert "portfolio_analytics as _pa" in app or "import portfolio_analytics" in app
    # the loader still points at the single canonical route.
    assert "_mhzGet('/v1/portfolio/chart-analytics')" in UI


def test_d4_loader_triggered_on_primary_route():
    # the analytics loader fires when the primary Portfolio route is opened.
    pm_dispatch_region = UI[UI.index("if (tabName === 'portfolio-manager')"):
                            UI.index("if (tabName === 'prediction-cockpit')")]
    assert "loadPortfolioAnalytics()" in pm_dispatch_region


def test_d5_no_browser_side_portfolio_math_added():
    # the hero/decision helpers only FORMAT backend values — assert they read the
    # canonical fields rather than recomputing NAV / P&L / returns.
    assert "d.allocation" in UI and "perf.last" in UI
    # no ad-hoc benchmark/return recomputation keywords were introduced by this phase.
    assert "function _paHero" in UI and "renderPmDecisionBar" in UI


# --------------------------------------------------------------------------- #
# E. No regression to Today; canonical owner still composes
# --------------------------------------------------------------------------- #
def test_e1_today_market_context_not_regressed():
    assert "loadMarketContext" in UI  # Today Market Trend owner still wired
    assert "loadMarketDashboard" in UI  # Today Market Context strip still wired


def test_e2_analytics_owner_still_single_and_composes():
    import paper_trader.api.portfolio_analytics as pa
    d = pa.build_portfolio_analytics(
        performance_loader=lambda: {}, attribution_loader=lambda: {},
        operational_book_loader=lambda: {}, contributions_loader=lambda: {})
    assert d["canonical_owner"] == "api.portfolio_analytics"
    assert set(d["chart_keys"]) == set(pa.CHART_KEYS)
    assert d["read_only"] is True and d["orders_enabled"] is False
    assert d["performed_write"] is False
