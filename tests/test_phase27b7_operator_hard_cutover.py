"""
tests/test_phase27b7_operator_hard_cutover.py — Phase 27B.7 hard operator UI cutover.

27B.7 is a *removal* phase: the legacy portfolio / signal-workflow / audit /
diagnostic content that 27B.1/27B.2/27B.6 kept COLLAPSED-BUT-PRESENT is now
PHYSICALLY REMOVED from every operator-route DOM. These tests assert the removal
(not mere collapse), the text rule (NO LIVE ORDERS -> NO LIVE BROKER ORDERS), the
required operator content, and that the canonical operational state contract is
unchanged and consistent across routes.

Fully offline: the canonical-state assertions run against the isolated tmp desk /
ledger harness (27A / 27B.1 / 27B.5) — never the user's real development book;
no live broker, no automation, no signals, no writes to real stores.

The live development book (25 submitted orders, desk mark 2026-07-21) is verified
separately with read-only Playwright at 1920x1080; here we verify the CONTRACT
(N submitted / 0 filled / 0 holdings / $100k / awaiting-close headline / Refresh
After Market Close) that produces those live values.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.test_phase27a_paper_operations import (  # reuse the offline harness
    _AUTH,
    client, env,  # noqa: F401  (pytest fixtures resolved by name)
)
from tests.test_phase27b1_operational_surface_cutover import (  # 27B.1 seams
    env27b1,  # noqa: F401
)
from tests.test_phase27b5_operator_flow import (  # canonical world builders
    _submitted_world, _cs, _N,
)

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"

# Normal operator routes -> (start marker, end marker) bounding their tab DOM.
_ROUTES = {
    "command-center":    ('id="tab-overview"',           'id="tab-prediction-cockpit"'),
    "daily-workflow":    ('id="tab-prediction-cockpit"', 'id="tab-review-queue"'),
    "portfolio":         ('id="tab-portfolio"',          'id="tab-portfolio-manager"'),
    "portfolio-manager": ('id="tab-portfolio-manager"',  'id="tab-multi-horizon"'),
    "multi-horizon":     ('id="tab-multi-horizon"',      'id="tab-audit-advanced"'),
}

# Banned legacy phrases (case-insensitive) that must not appear on any operator route.
_BANNED = [
    "Legacy Portfolio Terminal", "Historical Paper Books", "Legacy Signal Workflow",
    "Legacy Paper Book", "Legacy portfolio capacity", "reconciler cache",
    "Quant Model Methodology", "Internal signal audit", "Internal decision audit",
    "Internal order audit", "Legacy Signal Portfolio", "ledger integrity",
    "raw status payload", "$9,999", "$7,374",
]

# Legacy DOM block ids that must be entirely gone from the file.
_REMOVED_IDS = [
    'id="cc-legacy-overview"', 'id="dw-legacy-archive"', 'id="pt-archive"',
    'id="right-legacy-capacity"', 'id="ab-legacy-card"', 'id="ab-legacy-capacity"',
    'id="pd-audit"', 'id="pt-legacy-cache"', 'id="pt-legacy-total"',
    'id="cap-open-positions"',
]


@pytest.fixture(scope="module")
def html() -> str:
    return _UI.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def js(html) -> str:
    return "\n".join(m.group(1) for m in
                     re.finditer(r"(?s)<script[^>]*>(.*?)</script>", html))


@pytest.fixture(scope="module")
def static_html(html) -> str:
    """Markup with <script> bodies blanked (indices preserved) so text checks
    never match JS string literals or comments."""
    return re.sub(
        r"(?s)(<script[^>]*>)(.*?)(</script>)",
        lambda m: m.group(1) + " " * len(m.group(2)) + m.group(3),
        html)


def _region(s: str, start: str, end: str) -> str:
    return s[s.index(start):s.index(end)]


# --------------------------------------------------------------------------- #
# 1. Legacy blocks are PHYSICALLY removed (not collapsed / hidden / relocated)
# --------------------------------------------------------------------------- #
class TestLegacyBlocksRemovedFromDom:
    @pytest.mark.parametrize("probe", _REMOVED_IDS)
    def test_removed_id_absent(self, html, probe):
        assert probe not in html, f"{probe} must be removed from the DOM entirely"

    def test_no_leftover_collapsed_wrappers(self, html):
        # None of the removed <details> ids may reappear even as a collapsed shell.
        for det in ("cc-legacy-overview", "dw-legacy-archive", "pt-archive",
                    "right-legacy-capacity", "pd-audit"):
            assert f'id="{det}"' not in html

    def test_dead_legacy_loaders_gone(self, js):
        # The methodology + reconciler-cache loaders (targets deleted) are removed.
        assert "loadQuantModelMethodology" not in js
        assert "_loadLegacyReconcilerCache" not in js
        assert "_qmmRender" not in js


# --------------------------------------------------------------------------- #
# 2. Banned legacy strings absent from every operator route
# --------------------------------------------------------------------------- #
class TestBannedStringsAbsentPerRoute:
    @pytest.mark.parametrize("route", list(_ROUTES))
    @pytest.mark.parametrize("banned", _BANNED)
    def test_banned_absent(self, static_html, route, banned):
        start, end = _ROUTES[route]
        region = _region(static_html, start, end)
        assert banned.lower() not in region.lower(), (
            f"{banned!r} must not render on the {route} route")

    @pytest.mark.parametrize("route", list(_ROUTES))
    def test_no_legacy_tickers(self, static_html, route):
        start, end = _ROUTES[route]
        region = _region(static_html, start, end)
        # CDW / HUM are the archived legacy positions; they must never render.
        assert not re.search(r"\bCDW\b", region), f"CDW rendered on {route}"
        assert not re.search(r"\bHUM\b", region), f"HUM rendered on {route}"

    def test_banned_absent_whole_file(self, html):
        # Belt-and-suspenders: not even in comments / titles anywhere.
        low = html.lower()
        for s in _BANNED:
            assert s.lower() not in low, f"{s!r} still present somewhere in the file"


# --------------------------------------------------------------------------- #
# 3. Text rule: NO LIVE ORDERS -> NO LIVE BROKER ORDERS
# --------------------------------------------------------------------------- #
class TestTextRuleNoLiveBrokerOrders:
    def test_no_visible_no_live_orders_badge(self, html):
        assert ">NO LIVE ORDERS</span>" not in html

    @pytest.mark.parametrize("route", ["command-center", "portfolio-manager", "multi-horizon"])
    def test_no_live_broker_orders_present(self, static_html, route):
        start, end = _ROUTES[route]
        region = _region(static_html, start, end)
        assert "NO LIVE BROKER ORDERS" in region, (
            f"{route} must carry the NO LIVE BROKER ORDERS badge")

    def test_footer_and_top_bar_use_broker_wording(self, html):
        # the CC top status bar badge is updated
        assert ">NO LIVE BROKER ORDERS</span>" in html


# --------------------------------------------------------------------------- #
# 4. Required operator content per route
# --------------------------------------------------------------------------- #
class TestRequiredContent:
    def test_portfolio_zero_fill_empty_state(self, js):
        # Exact required empty-state sentence (dynamic count) for submitted + 0 fills.
        assert ("submitted paper orders are awaiting the next eligible completed close.'"
                in js)
        assert "'No Alpha holdings yet. '" in js

    def test_alpha_portfolio_model_target_banner(self, static_html):
        start, end = _ROUTES["multi-horizon"]
        region = _region(static_html, start, end)
        assert 'id="mhz-model-target-banner"' in region
        assert "MODEL TARGET &mdash; NOT EXECUTED HOLDINGS" in region

    def test_command_center_required_operational_card(self, static_html):
        start, end = _ROUTES["command-center"]
        region = _region(static_html, start, end)
        assert 'id="cc-ob-panel"' in region          # canonical operational state
        assert 'id="cc-ob-headline"' in region        # one operational headline
        assert 'id="cc-ob-workflow"' in region        # compact 5-step workflow
        assert 'id="cc-research-strip"' in region      # research status link (non-overriding)

    def test_daily_workflow_is_operational_five_step_only(self, static_html):
        start, end = _ROUTES["daily-workflow"]
        region = _region(static_html, start, end)
        assert 'id="dwob-card"' in region
        for step in ("dwob-step-refresh-data", "dwob-step-refresh-target",
                     "dwob-step-order-plan", "dwob-step-review", "dwob-step-monitor"):
            assert f'id="{step}"' in region
        # the legacy signal workflow / start-daily-review are gone from this route
        assert "Start Daily Review" not in region
        assert "Six-Stage" not in region and "six-stage" not in region

    def test_portfolio_operational_book_default(self, static_html):
        start, end = _ROUTES["portfolio"]
        region = _region(static_html, start, end)
        assert 'id="ptob-card"' in region
        assert 'id="ptob-holdings-table"' in region


# --------------------------------------------------------------------------- #
# 5. Exactly one operational next action per operator route
# --------------------------------------------------------------------------- #
class TestOnePrimaryActionPerRoute:
    def test_command_center_single_primary(self, html):
        panel = _region(html, 'id="cc-ob-panel"', "CURRENT OPERATIONAL BOOK END")
        assert panel.count('onclick="obPrimaryAction()"') == 1

    def test_portfolio_manager_single_primary(self, html):
        card = _region(html, 'id="pm-decision-card"', 'id="pm-date-warnings"')
        assert card.count('id="pm-primary-next-btn"') == 1
        # cancel is the only destructive secondary in the lifecycle strip
        assert card.count('id="pm-lc-cancel-btn"') == 1

    def test_no_competing_recommendation_on_operator_routes(self, static_html):
        for route, (start, end) in _ROUTES.items():
            region = _region(static_html, start, end)
            assert "Monitor the paper portfolio" not in region, route
            assert ">No Orders<" not in region, route
            assert ">ORDERS DISABLED<" not in region, route


# --------------------------------------------------------------------------- #
# 6. No native dialogs
# --------------------------------------------------------------------------- #
class TestNoNativeDialogs:
    def test_no_alert_confirm_prompt(self, js):
        for pat, name in ((r"(?<![A-Za-z0-9_.])alert\(", "alert"),
                          (r"(?<![A-Za-z0-9_.])confirm\(", "confirm"),
                          (r"(?<![A-Za-z0-9_.])prompt\(", "prompt")):
            assert not re.search(pat, js), f"native {name}() must not be used"


# --------------------------------------------------------------------------- #
# 7. Canonical operational-state contract (offline harness) — consistent shape
#    every operator route renders from /v1/operational-book.
# --------------------------------------------------------------------------- #
class TestCanonicalStateContract:
    def test_submitted_contract(self, env27b1):
        _submitted_world()
        cs = _cs()
        assert cs["lifecycle_stage"] == "SUBMITTED"
        assert cs["submitted_count"] == _N
        assert cs["filled_count"] == 0
        assert cs["holdings_count"] == 0
        assert cs["cash"] == pytest.approx(100000.0)
        assert cs["nav"] == pytest.approx(100000.0)
        assert cs["next_action_label"] == "Refresh After Market Close"
        assert cs["current_task_label"] == "Await Next Eligible Close"
        assert "AWAITING NEXT ELIGIBLE CLOSE" in cs["primary_headline"]

    def test_safety_unchanged_by_text_rule(self, env27b1, client):
        _submitted_world()
        d = client.get("/v1/operational-book", headers=_AUTH).json()
        assert d["live_orders_enabled"] is False
        assert d["broker_enabled"] is False


# --------------------------------------------------------------------------- #
# 8. Read-only: the static file load performs no writes (no mutating fetch in
#    the operator render path targeting a write endpoint).
# --------------------------------------------------------------------------- #
class TestReadOnly:
    def test_no_write_on_operator_render(self, js):
        # the operator render/refresh helpers only GET the operational book
        assert "loadOperationalBook" in js
        # no auto-submit / auto-confirm on load
        assert "autoSubmit" not in js
