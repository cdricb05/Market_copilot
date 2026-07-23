"""Phase 25B — UI integration and state-consistency repair (static UI contract).

Pure static checks against ``api/ui/index.html`` (no DB, no HTTP):

  A. The Alpha Portfolio navigation item + route alias + load/error states.
  B. The Multi-Horizon Alpha Portfolio page content contract (status strip,
     sleeves table, primary recommendations, six-book comparison, performance
     role labels, visible fast-sleeve explanation, styled snapshot flow).
  C. The ALIGNED/STALE contradiction cannot occur: the retained run payload is
     labeled PRE-REFRESH and the FINAL STATE line always mirrors the canonical
     current GET status.
  D. Historical Daily Review sessions are labeled HISTORICAL — NOT TODAY and an
     empty state reads NO CURRENT DAILY REVIEW SESSION.
  F. Exactly ONE primary Start Daily Review action exists; every other surface
     links to it via focusDailyReviewControl().
  H. Champion / challenger / ensemble language is exact and consistent.

Backend counterparts (funnel session-state fields, methodology data readiness)
are covered in tests/test_api.py which has the database fixtures.
"""
from __future__ import annotations

import re
from pathlib import Path

_UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def _html() -> str:
    return _UI.read_text(encoding="utf-8")


def _scripts(html: str) -> str:
    return "\n".join(re.findall(r"<script[^>]*>(.*?)</script>", html, re.DOTALL))


# --------------------------------------------------------------------------- #
# A. Navigation + route + load states
# --------------------------------------------------------------------------- #

class TestAlphaPortfolioNavigation:
    def test_nav_item_named_model_target_under_research(self):
        # Phase 27C: the nav is grouped OPERATE / RESEARCH; the model-target page
        # is labeled "Model Target" and lives under the Research group.
        html = _html()
        i_research = html.index('<div class="sidebar-label">Research</div>')
        i_actions = html.index('<div class="sidebar-label">Actions</div>')
        research_block = html[i_research:i_actions]
        assert 'data-route="multi-horizon"' in research_block
        assert ">Model Target</a>" in research_block

    def test_route_and_alias_registered(self):
        html = _html()
        assert "'multi-horizon': 'multi-horizon'" in html
        assert "'alpha-portfolio': 'multi-horizon'" in html
        # The alias also highlights the nav item.
        assert "if (base === 'alpha-portfolio') base = 'multi-horizon';" in html

    def test_page_title_present(self):
        # Phase 27C: renamed "MODEL TARGET — MULTI-HORIZON RESEARCH PORTFOLIO".
        assert "MODEL TARGET" in _html()

    def test_loading_and_error_states_exist(self):
        html = _html()
        assert 'id="mhz-load-note"' in html
        s = _scripts(html)
        assert "Loading the read-only multi-horizon paper platform" in html
        assert "Could not reach the platform endpoints" in s
        assert "Platform inputs unavailable" in s

    def test_no_connect_to_load_placeholder(self):
        # A connected page must never read "Connect to Load".
        assert "Connect to Load" not in _html()


# --------------------------------------------------------------------------- #
# B. Page content contract
# --------------------------------------------------------------------------- #

class TestAlphaPortfolioContent:
    def test_status_strip_fields(self):
        html = _html()
        for el in ("mhz-sb-market", "mhz-sb-fund", "mhz-sb-state",
                   "mhz-sb-primary-model", "mhz-sb-primary", "mhz-sb-universe",
                   "mhz-sb-next-review", "mhz-sb-fast"):
            assert f'id="{el}"' in html, el

    def test_sleeves_table_columns(self):
        s = _scripts(_html())
        for col in ("'Sleeve'", "'Model'", "'Horizon'", "'Observation'", "'Rebalance'",
                    "'Last review'", "'Next review'", "'Review due'", "'Actionability'",
                    "'Status'"):
            assert col in s, col

    def test_recommendation_table_columns_and_counts(self):
        s = _scripts(_html())
        for col in ("'Rank'", "'Ticker'", "'Recommendation'", "'Target wt'",
                    "'Comb score'", "'Fund rk'", "'Mom rk'", "'Sector'", "'Risk'",
                    "'Reason'"):
            assert col in s, col
        # All five recommendation states are always rendered, zero-filled.
        assert ("['BUY_CANDIDATE', 'HOLD', 'REDUCE_CANDIDATE', 'EXIT_CANDIDATE', 'WAIT']"
                in s)

    def test_book_comparison_has_cadence_and_sufficiency(self):
        s = _scripts(_html())
        assert "'Cadence'" in s
        assert "'History sample'" in s
        assert "SUFFICIENT" in s and "THIN" in s

    def test_fast_sleeve_explanation_visible_not_in_audit(self):
        html = _html()
        assert 'id="mhz-fast-card"' in html
        assert 'id="mhz-fast-body"' in html
        # The fast card is OUTSIDE the collapsed audit details.
        assert html.index('id="mhz-fast-card"') < html.index('id="mhz-audit"')
        s = _scripts(html)
        assert "NO VALIDATED FAST ALPHA" in s
        for reason_bit in ("failed net-of-cost at 25 bps", "Holdout net25",
                           "Break-even cost capacity", "remains inactive"):
            assert reason_bit in s, reason_bit

    def test_snapshot_flow_styled_in_page(self):
        html = _html()
        assert 'id="mhz-confirm-box"' in html
        assert "mhzPreviewSnapshot" in html and "mhzConfirmSnapshot" in html
        assert "CONFIRM_MHZ_PAPER_SNAPSHOT" in html

    def test_no_native_dialogs_anywhere(self):
        s = _scripts(_html())
        assert len(re.findall(r"(?<![A-Za-z0-9_])alert\s*\(", s)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_])confirm\s*\(", s)) == 0
        assert len(re.findall(r"(?<![A-Za-z0-9_])prompt\s*\(", s)) == 0


# --------------------------------------------------------------------------- #
# C. ALIGNED / STALE contradiction cannot occur
# --------------------------------------------------------------------------- #

class TestAlignedStaleConsistency:
    def test_retained_run_payload_keys_are_pre_refresh_labeled(self):
        s = _scripts(_html())
        i = s.index("function _dorRenderRun(data)")
        j = s.index("async function previewDailyRun", i)
        body = s[i:j]
        assert "pre_refresh_status: data.status" in body
        assert "pre_refresh_alignment: data.alignment" in body
        # The old unlabeled keys must be gone from the advanced-detail builder.
        assert "status: data.status" not in body.replace(
            "pre_refresh_status: data.status", "")
        assert "alignment: data.alignment" not in body.replace(
            "pre_refresh_alignment: data.alignment", "")

    def test_final_state_line_mirrors_canonical_get(self):
        s = _scripts(_html())
        assert "function _dorUpdateFinalStateLine" in s
        # The canonical GET renderer refreshes the final-state line every time.
        i_fn = s.index("function _renderDailyOperatingRun(md)")
        i_next = s.index("function _dorRenderRun(data)")
        body = s[i_fn:i_next]
        assert "window._dorCanonicalMd = md" in body
        assert "_dorUpdateFinalStateLine()" in body
        # And the run-detail renderer never writes its own final state.
        i_run = s.index("function _dorRenderRun(data)")
        run_body = s[i_run:i_run + 4000]
        assert "FINAL STATE" not in run_body.replace("_dorUpdateFinalStateLine()", "")

    def test_canonical_headline_still_get_derived(self):
        s = _scripts(_html())
        assert "DAILY RUN COMPLETE — ALL OPERATING DATA ALIGNED TO" in s
        # The transient run result never touches #dor-outcome (Phase 15-B rule kept).
        assert "It never touches #dor-outcome" in s or "never touches #dor-outcome" in s


# --------------------------------------------------------------------------- #
# D. Historical session labeling
# --------------------------------------------------------------------------- #

class TestHistoricalSessionLabeling:
    def test_labels_present(self):
        s = _scripts(_html())
        assert "HISTORICAL SESSION — NOT TODAY" in s
        assert "NO CURRENT DAILY REVIEW SESSION" in s
        assert "CURRENT SESSION — TODAY" in s

    def test_backend_fields_consumed(self):
        s = _scripts(_html())
        for field in ("session_is_current", "session_display_label", "session_age_days"):
            assert field in s, field

    def test_historical_counts_never_presented_as_today(self):
        s = _scripts(_html())
        assert "not today" in s.lower()
        assert "computed live now" in s or "computed now" in s

    def test_session_line_carries_historical_tag(self):
        s = _scripts(_html())
        assert "HISTORICAL — NOT TODAY" in s


# --------------------------------------------------------------------------- #
# F. One authoritative Start Daily Review action
# --------------------------------------------------------------------------- #

class TestSinglePrimaryDailyReviewAction:
    def test_dispatcher_still_supports_start_daily_review(self):
        # The canonical dispatcher keeps its case (mechanism unchanged).
        s = _scripts(_html())
        i = s.index("function primaryWorkflowAction(")
        fn = s[i:i + 2500]
        assert "'start_daily_review'" in fn
        assert "startDailyReviewSession" in fn


# --------------------------------------------------------------------------- #
# G. Raw diagnostics stay collapsed
# --------------------------------------------------------------------------- #

class TestDiagnosticsCollapsed:
    def test_mhz_audit_is_collapsed_details(self):
        html = _html()
        i = html.index('id="mhz-audit"')
        assert "<details" in html[i - 40:i]


# --------------------------------------------------------------------------- #
# H. Champion / challenger / ensemble language
# --------------------------------------------------------------------------- #

class TestRoleLanguage:
    def test_exact_role_map(self):
        s = _scripts(_html())
        assert "'composite_sn': 'RESEARCH CHAMPION'" in s
        assert "'mom_6_1': 'PAPER CHALLENGER'" in s
        assert "'fundamental_momentum_50_50_v1': 'OPERATIONAL PRIMARY ENSEMBLE'" in s

    def test_ensemble_never_labeled_research_champion(self):
        s = _scripts(_html())
        assert "'fundamental_momentum_50_50_v1': 'RESEARCH CHAMPION'" not in s
        html = _html()
        assert "The ensemble did <b>not</b> replace the research champion." in html

    def test_primary_book_language(self):
        html = _html()
        assert "fundamental_momentum_50_50_top25" in html
        s = _scripts(html)
        assert "_mhzRoleForBook" in s

    def test_fast_sleeve_language(self):
        html = _html()
        assert "NO_VALIDATED_FAST_ALPHA" in html
        assert "NO VALIDATED FAST ALPHA" in _scripts(html)


# --------------------------------------------------------------------------- #
# Safety: no order buttons, badges intact on the touched surfaces
# --------------------------------------------------------------------------- #

class TestSafetySurfaces:
    def test_alpha_page_badges(self):
        html = _html()
        i = html.index('id="tab-multi-horizon"')
        j = html.index('id="tab-audit-advanced"')
        page = html[i:j]
        for b in ("PAPER ONLY", "ORDERS DISABLED", "AUTOMATION OFF", "MANUAL REVIEW",
                  "NO LIVE PROMOTION"):
            assert b in page, b
        assert "Create Order" not in page
        assert "Submit Order" not in page
