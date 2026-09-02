"""Market Interaction & Research Navigation UX — static acceptance matrix.

Deterministic and hermetic: static assertions over ``api/ui/index.html`` and
``api/ui/pt_charts.js`` plus the strict architecture audit. No server / provider /
prediction / DOM / real market call is touched.

Covers the four interaction/navigation areas of this tranche:

  1. Interactive Market Trend sparklines (shared PTC owner, no fabricated interpolation,
     card click → the ONE shared Market Detail).
  2. Deterministic Market Regime semantic-state color system.
  3. Research navigation consolidation (five primary + collapsed Advanced Research;
     nothing deleted; every deep link preserved; Research Bridge demoted but reachable).
  4. Market Context tile hierarchy + clickable tiles → ONE shared Market Detail drill-down.

Plus: one canonical frontend chart owner (pt_charts.js), read-only safety, and the strict
architecture audit staying green.
"""
from __future__ import annotations

import importlib
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
UI = (ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")
PTC = (ROOT / "api" / "ui" / "pt_charts.js").read_text(encoding="utf-8")
APP = (ROOT / "api" / "app.py").read_text(encoding="utf-8")

_TREND_KEYS = ("sp500", "nasdaq", "vix", "wti", "gold", "eurusd")
_TILE_KEYS = ("sp500", "nasdaq", "dow", "vix", "eurusd", "gold", "wti", "brent",
              "us10y", "us2y", "usd_broad")


# =========================================================================== #
# 1. INTERACTIVE MARKET TREND SPARKLINES
# =========================================================================== #
def test_trend_uses_shared_ptc_sparkline():
    # The interactive trend grid renders via the shared PTC owner — not a bespoke engine.
    assert "function renderMarketTrendGrid" in UI
    assert "PTC.sparkline(" in UI
    # The old static-only sparkline path no longer drives the trend grid render.
    assert "renderMarketTrendGrid(hist)" in UI


def test_sparkline_hover_maps_to_real_observation_no_interpolation():
    fn = _fn(PTC, "PTC.sparkline = function", "PTC.sparkline = function")  # locate start
    body = _slice(PTC, "PTC.sparkline = function", "PTC.marketFetch")
    # nearest REAL observation snap + explicit "never interpolate" contract.
    assert "nearestIdx" in body
    assert "snap to the nearest REAL" in body or "never interpolate" in body
    # hover tooltip carries DATE, VALUE and CHANGE from the previous observation.
    assert "shortDate" in body and "Value" in body and "Change" in body


def test_trend_cards_are_clickable_to_shared_detail():
    body = _slice(UI, "function renderMarketTrendGrid", "window.renderMarketTrendGrid")
    assert 'class="va-spark-card va-spark-click"' in body
    assert 'role="button"' in body and 'tabindex="0"' in body
    assert 'data-mkey="' in body
    assert "_openMarketDetailFromEl" in body
    # keyboard activation (Enter / Space).
    assert "Enter" in body and ("' '" in body or "Spacebar" in body)


# =========================================================================== #
# 2. MARKET REGIME SEMANTIC COLOR SYSTEM
# =========================================================================== #
def test_regime_semantic_mapping_deterministic():
    fn = _slice(UI, "function _regimeSemantic", "function _mktFmt")
    # UP green, DOWN red, HIGH amber, LOW cyan, NORMAL blue, FLAT gray, unknown NOT green.
    assert "case 'UP': return 'up'" in fn
    assert "case 'DOWN': return 'down'" in fn
    assert "case 'HIGH':" in fn and "return 'warn'" in fn
    assert "'ELEVATED'" in fn and "'INVERTED'" in fn      # mapped intentionally (amber)
    assert "case 'LOW': return 'cool'" in fn
    assert "'NORMAL'" in fn and "'MODERATE'" in fn and "return 'info'" in fn
    assert "'FLAT'" in fn and "'MIXED'" in fn and "return 'neutral'" in fn
    assert "default: return 'neutral'" in fn              # unknown → neutral, never green


def test_regime_semantic_classes_defined():
    for cls, needle in (("up", ".va-regime-tile.up"), ("down", ".va-regime-tile.down"),
                        ("warn", ".va-regime-tile.warn"), ("cool", ".va-regime-tile.cool"),
                        ("info", ".va-regime-tile.info"), ("neutral", ".va-regime-tile.neutral"),
                        ("na", ".va-regime-tile.na")):
        assert needle in UI, "missing regime accent class: " + cls
    for tone in ("up", "down", "warn", "cool", "info", "neutral", "na"):
        assert ".va-tone-" + tone in UI, "missing tone color class: " + tone
    # distinct colors: cyan for LOW, blue for NORMAL, amber for HIGH, green UP, red DOWN.
    assert "#22d3ee" in UI  # cyan (LOW)
    assert ".va-regime-tile.info{border-left-color:#3b82f6;}" in UI  # blue (NORMAL)


def test_regime_render_uses_semantic_and_preserves_reference_labels():
    assert "_regimeSemantic(r.tone, r.available)" in UI
    assert "va-tone-' + sem" in UI
    # REFERENCE ONLY / NOT A SIGNAL context is preserved on the regime block.
    assert "REFERENCE ONLY" in UI and "NOT A SIGNAL" in UI


# =========================================================================== #
# 3. RESEARCH NAVIGATION CONSOLIDATION
# =========================================================================== #
def _ra_nav() -> str:
    i = UI.find('id="ra-nav"')
    assert i != -1
    return UI[i:UI.find("</div>", i)]


def test_five_primary_operator_choices():
    nav = _ra_nav()
    primary_region = nav[:nav.find('<details id="ra-advanced"')]
    for pid in ('id="ra-nav-overview"', 'id="ra-nav-research-agent"',
                'id="ra-nav-performance"', 'id="ra-nav-data-expansion"'):
        assert pid in primary_region, "primary nav item missing: " + pid
    # The fifth primary choice is the Advanced Research disclosure (collapsed by default).
    assert '<details id="ra-advanced"' in nav
    assert ">Advanced Research</summary>" in nav
    assert not re.search(r'<details id="ra-advanced"[^>]*\sopen', nav)  # collapsed by default


def test_advanced_surfaces_preserved_and_demoted():
    nav = _ra_nav()
    adv_region = nav[nav.find('<details id="ra-advanced"'):]
    for aid in ('id="ra-nav-research-bridge"', 'id="ra-nav-daily-operations"',
                'id="ra-nav-paper-books"', 'id="ra-nav-revalidation"',
                'id="ra-nav-tournament"', 'id="ra-nav-alpha-factory"',
                'id="ra-nav-price-alpha"', 'id="ra-nav-alpha-agent"',
                'id="ra-nav-stage11"', 'id="ra-nav-stage12"',
                'id="ra-nav-stage13a"', 'id="ra-nav-diagnostics"'):
        assert aid in adv_region, "advanced surface not under disclosure: " + aid


def test_old_deep_links_all_preserved():
    for sub in ("", "research-agent", "research-bridge", "performance", "daily-operations",
                "paper-books", "revalidation", "tournament", "alpha-factory", "price-alpha",
                "alpha-agent", "stage11", "stage12", "stage13a", "data-expansion", "diagnostics"):
        assert 'data-rasub="%s"' % sub in UI, "deep link lost: " + sub
    # Section routing map + panels are unchanged (nothing deleted).
    assert "_RA_SECTIONS" in UI and "_raApplySection" in UI


def test_no_duplicate_research_nav_panels():
    # each research nav link id appears exactly once (no duplicated panel/link).
    for aid in ("ra-nav-research-bridge", "ra-nav-data-expansion", "ra-nav-performance"):
        assert UI.count('id="%s"' % aid) == 1, "duplicate research nav link: " + aid


def test_advanced_auto_expands_on_deep_link():
    fn = _slice(UI, "function _raHighlightNav", "window._raHighlightNav")
    assert "ra-advanced" in fn
    assert "adv.open = true" in fn
    assert "primarySubs" in fn


def test_research_bridge_status_on_research_agent_view():
    # Research Bridge is demoted, but its authoritative state is summarized on the Research
    # Agent view (research warranted? mandate? researching? concluded? manual review?).
    assert 'id="rag-bridge-strip"' in UI
    fn = _slice(UI, "function _renderRagBridgeStrip", "window._renderRagBridgeStrip")
    for label in ("Research warranted", "Mandate", "AlphaAgent", "Concluded", "Manual review"):
        assert label in fn, "bridge strip missing: " + label
    assert "research-audit/research-bridge" in fn  # reachable (deep-link to full bridge detail)
    # Loaded from the SAME bridge read model when the Research Agent section opens.
    # Assert the OWNERSHIP — the section's loader list declares the bridge loader —
    # not a byte-exact list: the list legitimately grew after this tranche (27e2470
    # added the leaderboard, 3cc02b1 the prospective tournament) and now wraps
    # across two lines, while loadResearchBridge stayed wired exactly as intended.
    lmap = _slice(UI, "function _raLoadSection", "loaders[sub]")
    entry = re.search(r"'research-agent':\s*\[(.*?)\]", lmap, re.S)
    assert entry, "the research-agent section must declare its loader list"
    for loader in ("'loadResearchAgent'", "'loadResearchBridge'"):
        assert loader in entry.group(1), "research-agent loader lost: " + loader


# =========================================================================== #
# 4. MARKET CONTEXT TILES — hierarchy + clickable → shared Market Detail
# =========================================================================== #
def test_tile_label_hierarchy_stronger():
    css = _slice(UI, ".ov-mkt-label {", "}")
    assert "font-size: 13px" in css       # up from 10px
    assert "font-weight: 800" in css      # heavier
    assert "--text-primary" in css        # stronger contrast


def test_every_supported_tile_clickable():
    for k in _TILE_KEYS:
        assert 'ov-market-card mkt-clickable" data-key="%s"' % k in UI, "tile not clickable: " + k
    # exactly the eleven Market Context tiles are made clickable (no more, no fewer).
    assert UI.count("ov-market-card mkt-clickable") == len(_TILE_KEYS)
    # the button role + keyboard focusability + accessible name are wired in JS (kept out of
    # the static markup so the tile region stays compact), and open the ONE shared detail.
    wiring = _slice(UI, "function _initMarketTileClicks", "window._initMarketTileClicks")
    assert "setAttribute('role', 'button')" in wiring
    assert "tabindex" in wiring and "aria-label" in wiring
    assert "_openMarketDetailFromEl" in wiring


def test_one_shared_market_detail_component():
    assert PTC.count("PTC.openMarketDetail = function") == 1
    assert "PTC.closeMarketDetail" in PTC
    # per-instrument opener funnels every tile/card through the ONE component.
    assert "function _openMarketDetailFromEl" in UI


def test_market_detail_windows_and_honesty():
    assert 'data-win="30D"' in PTC and 'data-win="90D"' in PTC and 'data-win="1Y"' in PTC
    assert "Historical series unavailable for this window." in PTC or \
           "Historical series unavailable for this window." in APP
    # provider + as-of shown, reference disclaimer preserved.
    assert "source_label" in PTC and "Latest observation" in PTC
    assert "Reference only — not a signal or recommendation." in PTC


def test_market_detail_uses_authoritative_backend_history():
    assert "/v1/market/history" in PTC        # the shared history owner
    assert "@app.get(" in APP and '"/v1/market/history"' in APP
    # fed by the SAME owners as the other market surfaces (yfinance + FRED range helper).
    assert "_MARKET_HISTORY_YF" in APP and "_MARKET_HISTORY_FRED" in APP
    assert "def _fred_history" in APP


# =========================================================================== #
# CHART ARCHITECTURE — ONE canonical frontend chart interaction owner
# =========================================================================== #
def test_single_canonical_chart_owner():
    # Only pt_charts.js exists as the chart owner — no competing engine files.
    js = sorted(p.name for p in (ROOT / "api" / "ui").glob("*.js"))
    assert js == ["pt_charts.js"], "unexpected chart JS files: " + str(js)
    for banned in ("market_charts2.js", "market_charts.js"):
        assert not (ROOT / "api" / "ui" / banned).exists()


def test_portfolio_and_market_reuse_same_timeseries_owner():
    # Portfolio charts use PTC.timeSeries; the Market Detail chart reuses the SAME helper.
    assert "PTC.timeSeries(" in UI          # portfolio analytics
    assert "PTC.timeSeries(chart" in PTC    # market detail drill-down
    # the sparkline + detail live in the SAME owner file (no second tooltip/crosshair engine).
    assert "PTC.sparkline = function" in PTC
    assert PTC.count("id = 'ptc-tooltip'") + PTC.count('id="ptc-tooltip"') <= 2  # one shared tip


# =========================================================================== #
# SAFETY — read-only, no orders / promotion / apply / cadence
# =========================================================================== #
def test_no_native_dialogs_added():
    assert not re.search(r"(?<![A-Za-z0-9_.])alert\s*\(", UI)
    assert not re.search(r"(?<![A-Za-z0-9_.])confirm\s*\(", UI)
    assert not re.search(r"(?<![A-Za-z0-9_.])prompt\s*\(", UI)
    assert not re.search(r"(?<![A-Za-z0-9_.])alert\s*\(", PTC)
    assert not re.search(r"(?<![A-Za-z0-9_.])confirm\s*\(", PTC)


def test_history_endpoint_is_read_only_get():
    seg = APP[APP.index('"/v1/market/history"') - 200: APP.index('"/v1/market/history"')]
    assert "@app.get(" in seg
    # no mutate/order/promote/apply/purchase market route introduced by this tranche.
    for forbidden in ("/v1/market/purchase", "/v1/market/subscribe", "/v1/market/history/apply"):
        assert ('"%s"' % forbidden) not in APP


def test_market_detail_has_no_action_controls():
    md = _slice(PTC, "PTC.openMarketDetail = function", "PTC.detailBase")
    body = PTC[PTC.index("function _mdEnsure"):PTC.index("PTC.detailBase")]
    for banned in ("Create Order", "Submit", "Apply", "Promote", "Execute", "Buy", "Sell"):
        assert banned not in body, "Market Detail must carry no action control: " + banned


# =========================================================================== #
# ARCHITECTURE guard — strict audit stays green
# =========================================================================== #
def test_strict_audit_exit_zero():
    aud = importlib.import_module("scripts.audit_architecture")
    assert aud.main(["--strict", "--json-only"]) == 0


def test_operator_ux_guard_still_green():
    aud = importlib.import_module("scripts.audit_architecture")
    ux = aud.check_operator_ux_consolidation_ownership(aud._iter_source_files())
    assert ux["market_loader_count"] == 1
    assert ux["market_owner_fetch"] is True
    assert ux["market_route_get_only"] is True
    assert ux["ui_direct_provider_hosts"] == []
    assert ux["market_region_market_math"] == []
    assert ux["market_reference_only"] is True
    assert ux["primary_areas_present"] and ux["legacy_views_demoted"]
    assert ux["missing_safety_tokens"] == []
    assert ux["forbidden_new_routes_present"] == []


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _slice(text: str, start_needle: str, end_needle: str) -> str:
    s = text.index(start_needle)
    e = text.find(end_needle, s + len(start_needle))
    return text[s:(e if e != -1 else len(text))]


def _fn(text: str, start_needle: str, _unused: str) -> str:
    return text[text.index(start_needle):]
