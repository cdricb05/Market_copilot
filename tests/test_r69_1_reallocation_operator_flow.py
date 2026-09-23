"""R69.1 - the Portfolio -> Reallocation operator flow.

The operator pressed "Select minimum repair". The UI said "Recording selection..."
and never said anything else. The backend access log recorded six consecutive
``POST /v1/operations/portfolio-decision/select-target -> 401 Unauthorized`` while
every GET on the same screen answered 200, and the governed selection ledger was
never created.

Two independent defects produced one symptom:

1. the POST hand-rolled its headers and omitted ``X-API-Key``, so it never reached
   the governed write at all;
2. the refusal it did receive was written into an element that the reload the same
   function triggers destroys, so the only durable thing on screen was the verb.

A third defect was latent: the write gate read the review kernel's pre-freshness
copy of the option list, in which a historical proposal is still ``selectable``.

These tests are hermetic. They read the shipped source for the browser contract
and drive the real owners with injected worlds for the governed contract. Nothing
here touches the live operational book.
"""
from __future__ import annotations

import json
import pathlib
import re
import unittest

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as pdr
from paper_trader.api import rebalance_execution as rbx

ROOT = pathlib.Path(__file__).resolve().parents[1]
UI = ROOT / "api" / "ui" / "index.html"


def _ui() -> str:
    return UI.read_text(encoding="utf-8", errors="replace")


#: The render function's UNAMBIGUOUS anchor. Bare "function _pdrRender" also
#: matches "function _pdrRenderLoading", which is defined earlier in the file -
#: the prefix collision that silently emptied an R63 slice (see test_90).
RENDER = "function _pdrRender(d, err)"


def _slice(src: str, start: str, end: str) -> str:
    a, b = src.index(start), src.index(end)
    assert a < b, "inverted slice: %r came after %r" % (start, end)
    return src[a:b]


def _render(src: str) -> str:
    return src[src.index(RENDER):]


# --------------------------------------------------------------------------- #
# 1. The browser contract
# --------------------------------------------------------------------------- #
class TestSelectionRequestIsAuthenticated(unittest.TestCase):
    """THE defect. Six 401s, and no selection was ever recorded."""

    def test_01_the_select_target_post_sends_the_api_key(self):
        sel = _slice(_ui(), "function _pdrSelect(", "window._pdrSelect = _pdrSelect")
        self.assertIn("/v1/operations/portfolio-decision/select-target", sel)
        self.assertIn("'X-API-Key': key()", sel,
                      "the governed write must carry the same auth header as every "
                      "authenticated read on this screen")

    def test_02_the_header_precedes_the_body(self):
        """A regression guard with teeth: assert the key is inside THIS fetch's
        header object, not merely somewhere in the function."""
        sel = _slice(_ui(), "function _pdrSelect(", "window._pdrSelect = _pdrSelect")
        fetch_at = sel.index("fetch('/v1/operations/portfolio-decision/select-target'")
        body_at = sel.index("body: JSON.stringify", fetch_at)
        headers = sel[fetch_at:body_at]
        self.assertIn("X-API-Key", headers)

    def test_03_every_authenticated_post_on_this_surface_carries_the_key(self):
        """The class of defect, not just the instance: no fetch() anywhere in the
        proposal-decision-review region may post to /v1/ without the header."""
        src = _ui()
        region = _slice(src, "function _pdrSelBtn", "function _r47Num")
        for m in re.finditer(r"fetch\((.{0,4000}?)\}\)", region, flags=re.S):
            chunk = m.group(1)
            if "method: 'POST'" not in chunk:
                continue
            self.assertIn("X-API-Key", chunk,
                          "an authenticated POST without the key: %s" % chunk[:160])


class TestSelectionResultSurvivesRerender(unittest.TestCase):
    """The result must outlive the reload the selection itself triggers."""

    def test_10_the_result_lives_outside_the_dom(self):
        src = _ui()
        self.assertIn("window._pdrSelectUi", src)
        self.assertIn("function _pdrSelectionResult", src)

    def test_11_the_render_paints_the_result_from_state(self):
        src = _ui()
        render = _render(src)
        self.assertIn("_pdrSelectionResult(d)", render,
                      "every paint must re-render the selection result from state")

    def test_12_there_is_no_terminal_recording_selection_state(self):
        sel = _slice(_ui(), "function _pdrSelect(", "window._pdrSelect = _pdrSelect")
        self.assertNotIn("Recording selection", sel)
        self.assertIn("phase: 'PENDING'", sel)

    def test_13_pending_always_resolves_to_ok_or_error(self):
        sel = _slice(_ui(), "function _pdrSelect(", "window._pdrSelect = _pdrSelect")
        self.assertIn("phase: 'OK'", sel)
        self.assertIn("phase: 'ERROR'", sel)
        self.assertIn(".catch(", sel, "a rejected promise must land in ERROR, "
                                     "never leave PENDING on screen")

    def test_14_a_failure_reports_http_status_detail_and_a_retry(self):
        src = _ui()
        result = _slice(src, "function _pdrSelectionResult", "function _pdrApproveCta")
        for token in ("httpStatus", "ui.detail", "ui.retry", "SELECTION REFUSED"):
            self.assertIn(token, result, token)

    def test_15_a_401_names_the_authentication_remedy(self):
        sel = _slice(_ui(), "function _pdrSelect(", "window._pdrSelect = _pdrSelect")
        self.assertIn("401", sel)
        self.assertIn("not authenticated", sel)

    def test_16_a_refusal_states_that_nothing_was_written(self):
        result = _slice(_ui(), "function _pdrSelectionResult", "function _pdrApproveCta")
        self.assertIn("NOTHING WAS WRITTEN", result)


class TestOneObviousNextAction(unittest.TestCase):
    """P0-B: the five facts at the TOP, in every terminal state."""

    REQUIRED = ("Session", "Proposal status", "Current actionability",
                "Selected target", "Next required action")

    def test_20_the_status_bar_carries_all_five_facts(self):
        bar = _slice(_ui(), "function _pdrStatusBar", "function _pdrStaleExplainer")
        for label in self.REQUIRED:
            self.assertIn("cell('%s'" % label, bar, label)

    def test_21_the_status_bar_renders_on_the_unreadable_branch_too(self):
        """An unreadable review is a state the operator must still act on."""
        src = _ui()
        render = _render(src)
        head = render[:render.index("host.setAttribute('data-pdr-state', state)")]
        self.assertIn("_pdrStatusBar(d, err)", head)

    def test_22_the_status_bar_precedes_step_one(self):
        src = _ui()
        render = _render(src)
        self.assertLess(render.index("_pdrStatusBar(d, null)"),
                        render.index("_pdrSelection(d)"))

    def test_23_both_steps_precede_every_evidence_section(self):
        src = _ui()
        render = _render(src)
        first_details = render.index("_pdrMore(")
        self.assertLess(render.index("_pdrSelection(d)"), first_details)
        self.assertLess(render.index("_pdrSelectionResult(d)"), first_details)

    def test_24_evidence_and_diagnostics_are_collapsed(self):
        src = _ui()
        render = _slice(src, RENDER, "function _pdrAudit")
        for title in ("Why this review path",
                      "Compare the three states",
                      "What the proposal changes",
                      "Historical evidence relevant to this proposal",
                      "Audit / advanced"):
            self.assertIn("_pdrMore('%s" % title, render, title)

    def test_25_details_sections_are_closed_by_default(self):
        src = _ui()
        more = _slice(src, "function _pdrMore(", RENDER)
        self.assertIn("openByDefault ? ' open' : ''", more)
        render = _slice(src, RENDER, "function _pdrAudit")
        for call in render.split("_pdrMore(")[1:]:
            self.assertNotIn(", true)", call[:600],
                             "no evidence section may open by default")


class TestHistoricalProposalOffersNoDeadControls(unittest.TestCase):

    def test_30_step_one_is_withheld_when_not_actionable(self):
        block = _slice(_ui(), "/* Release 69.1 - STEP 1", "/* Release 69.1 - STEP 2")
        self.assertIn("if (f.actionable === false) return '';", block,
                      "a historical proposal must not render three dead buttons")

    def test_31_the_explainer_names_both_sessions_in_plain_english(self):
        ex = _slice(_ui(), "function _pdrStaleExplainer", "/* A collapsed evidence")
        self.assertIn("This proposal belongs to", ex)
        self.assertIn("is now the latest eligible", ex)
        self.assertIn("preserved for review but cannot be selected", ex)

    def test_32_the_explainer_carries_the_authoritative_next_action(self):
        ex = _slice(_ui(), "function _pdrStaleExplainer", "/* A collapsed evidence")
        self.assertIn("RUN PORTFOLIO CYCLE", ex)
        self.assertIn("runPortfolioCycle(this)", ex)

    def test_33_the_explainer_says_history_is_preserved(self):
        ex = _slice(_ui(), "function _pdrStaleExplainer", "/* A collapsed evidence")
        self.assertIn("immutable historical evidence", ex)
        self.assertIn("deleted, rewritten or re-scored", ex)


class TestForbiddenBrowserAffordances(unittest.TestCase):

    def test_40_no_alert_or_confirm_on_the_whole_region(self):
        region = _slice(_ui(), "function _pdrSelBtn", "function _r47Num")
        self.assertNotIn("alert(", region)
        self.assertNotIn("confirm(", region)

    def test_41_the_region_creates_no_orders_and_no_automation(self):
        region = _slice(_ui(), "function _pdrSelBtn", "function _r47Num")
        for forbidden in ("create-orders", "createOrders", "confirm-order-plan",
                          "autoTrade", "setInterval("):
            self.assertNotIn(forbidden, region, forbidden)

    def test_42_the_approve_control_is_armed_in_two_steps(self):
        arm = _slice(_ui(), "function _pdrArmApprove", "window._pdrArmApprove")
        self.assertIn("CONFIRM APPROVAL", arm)
        self.assertIn("Cancel", arm)
        self.assertNotIn("confirm(", arm)

    def test_43_approve_is_offered_only_after_a_selection_exists(self):
        result = _slice(_ui(), "function _pdrSelectionResult", "function _pdrApproveCta")
        self.assertIn("canApprove", result)
        self.assertIn("actionable && sel.selected_target !== 'CURRENT'", result)

    def test_44_the_approve_control_states_it_creates_no_order(self):
        cta = _slice(_ui(), "function _pdrApproveCta", "function _pdrSelClear")
        self.assertIn("no order", cta)

    def test_45_safety_badges_are_present_on_the_result(self):
        result = _slice(_ui(), "function _pdrSelectionResult", "function _pdrApproveCta")
        for badge in ("CREATES TRADE DECISIONS ONLY", "NO ORDERS",
                      "NO LIVE BROKER ORDERS", "AUTOMATION OFF"):
            self.assertIn(badge, result, badge)

    def test_46_the_banned_orders_disabled_wording_is_not_reintroduced(self):
        """Paper orders are REAL here and run under a governed manual workflow;
        only LIVE BROKERAGE orders are structurally disabled. The repo forbids the
        misleading wording globally
        (test_phase27b6_final_ui_consistency::TestOrdersDisabledInvisible), and
        CLAUDE.md's badge list does not override it."""
        self.assertNotIn(">ORDERS DISABLED<", _ui())


# --------------------------------------------------------------------------- #
# 2. The governed contract
# --------------------------------------------------------------------------- #
def _review_envelope(*, actionable: bool, target="MINIMUM_REPAIR"):
    """A minimal envelope shaped exactly like the live one, including BOTH copies
    of the option list and the disagreement between them that freshness creates."""
    raw_opts = [
        {"target": "CURRENT", "label": "Current portfolio (do nothing)",
         "selectable": True, "blockers": [], "blocker_codes": [], "positions": 25},
        {"target": "MINIMUM_REPAIR", "label": "Minimum constraint repair",
         "selectable": True, "blockers": [], "blocker_codes": [], "positions": 14,
         "one_way_turnover": 0.210798, "estimated_cost": 52.01, "score": 0.934961,
         "cash_weight": 0.467016, "concentration": 0.020908,
         "obligations_remaining": [], "mandatory_obligations_remaining": 0},
        {"target": "FULL_TARGET", "label": "Full zero-base target (the proposal)",
         "selectable": True, "blockers": [], "blocker_codes": [], "positions": 20},
    ]
    kernel_block = {"options": raw_opts, "recommended_target": target,
                    "option_order": ["CURRENT", "MINIMUM_REPAIR", "FULL_TARGET"]}
    if actionable:
        reconciled = dict(kernel_block)
    else:
        blocked = []
        for o in raw_opts:
            o = dict(o)
            o["selectable"] = False
            o["blockers"] = [{"code": pdec.PDS_SESSION_STALE, "detail": "stale"}]
            o["blocker_codes"] = [pdec.PDS_SESSION_STALE]
            blocked.append(o)
        reconciled = dict(kernel_block)
        reconciled["options"] = blocked
        reconciled["selectable_targets"] = []
        reconciled["blocked_by_session_freshness"] = True
    review = {
        "target_selection": kernel_block,
        "reviewed_proposal": {
            "proposal_hash": "ph_" + ("cur" if actionable else "old"),
            "eligible_market_date": "2026-09-22",
            "active_book_id": "book_t",
            "hoc_assessment_hash": "hoc_1",
        },
        "review_verdict": {"verdict": "MINIMAL_REPAIR_PREFERRED"},
    }
    return {
        "status": "OK",
        "proposal_id": "reap_2026-09-22_book_t_ph",
        "proposal_hash": review["reviewed_proposal"]["proposal_hash"],
        "review_hash": "rh_1",
        "eligible_market_date": "2026-09-22",
        "review": review,
        "target_selection": reconciled,
        "governance": {"target_selection": reconciled, "actionable": actionable},
        "inputs": {"hoc_assessment_hash_used": "hoc_1"},
    }


class TestTheWriteGateReadsTheReconciledCopy(unittest.TestCase):
    """The latent third defect: the guard read the one copy that holds no clock."""

    def test_50_the_two_copies_genuinely_disagree(self):
        env = _review_envelope(actionable=False)
        kernel = env["review"]["target_selection"]["options"]
        reconciled = env["target_selection"]["options"]
        self.assertTrue(all(o["selectable"] for o in kernel))
        self.assertTrue(all(not o["selectable"] for o in reconciled))

    def test_51_a_stale_target_is_refused_as_not_selectable(self):
        """With session freshness explicitly disabled, the ONLY thing that can
        refuse is the selectability guard - so this isolates which copy it read."""
        out = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=False),
            enforce_session_freshness=False,
            decision_dir=self.tmp)
        self.assertEqual(out["status"], pdec.TS_NOT_SELECTABLE)
        self.assertFalse(out["recorded"])
        self.assertIn(pdec.PDS_SESSION_STALE, out["blocker_codes"])

    def test_52_a_current_target_is_still_recorded(self):
        out = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=True),
            enforce_session_freshness=False,
            decision_dir=self.tmp)
        self.assertEqual(out["status"], pdec.TS_CREATED)
        self.assertTrue(out["recorded"] and out["selected"])
        self.assertEqual(out["record"]["selected_target"], "MINIMUM_REPAIR")

    def test_53_the_envelope_names_its_authoritative_copy(self):
        src = (ROOT / "api" / "proposal_decision_review.py").read_text(
            encoding="utf-8", errors="replace")
        self.assertIn('"selectability_authority": "target_selection"', src)
        self.assertIn("selectability_authority_doc", src)

    def test_54_the_gate_reads_the_top_level_copy_first(self):
        src = (ROOT / "api" / "portfolio_decision.py").read_text(
            encoding="utf-8", errors="replace")
        block = src[src.index("def record_target_selection"):]
        block = block[:block.index("options = {")]
        self.assertIn('env.get("target_selection")', block)
        self.assertLess(block.index('env.get("target_selection")'),
                        block.index('rev.get("target_selection")'))

    def setUp(self):
        import tempfile
        self._td = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._td.name)

    def tearDown(self):
        self._td.cleanup()


class TestSelectionIsNotApproval(unittest.TestCase):

    def setUp(self):
        import tempfile
        self._td = tempfile.TemporaryDirectory()
        self.tmp = pathlib.Path(self._td.name)

    def tearDown(self):
        self._td.cleanup()

    def test_60_a_recorded_selection_creates_nothing_executable(self):
        out = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=True),
            enforce_session_freshness=False, decision_dir=self.tmp)
        for flag in ("is_an_approval", "approves_proposal", "creates_order_plan",
                     "creates_orders", "creates_fills", "executes",
                     "deploys_capital", "mutates_proposal", "decided_by_llm"):
            self.assertFalse(out[flag], flag)
        self.assertTrue(out["manual_approval_still_required"])

    def test_61_the_selection_survives_a_read_back(self):
        pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=True),
            enforce_session_freshness=False, decision_dir=self.tmp)
        back = pdec.load_target_selection(
            active_book_id="book_t", eligible_market_date="2026-09-22",
            decision_dir=self.tmp)
        self.assertIsNotNone(back)
        self.assertEqual(back["selected_target"], "MINIMUM_REPAIR")
        self.assertEqual(back["binding"]["proposal_hash"], "ph_cur")

    def test_62_the_ledger_is_a_real_file_on_disk(self):
        pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=True),
            enforce_session_freshness=False, decision_dir=self.tmp)
        rows = json.loads((self.tmp / "target_selections.json").read_text("utf-8"))
        index = json.loads((self.tmp / "target_selection_index.json").read_text("utf-8"))
        self.assertEqual(len(rows), 1)
        self.assertIn("book_t|2026-09-22", index)

    def test_63_selecting_twice_is_idempotent(self):
        kw = dict(target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
                  review_envelope=_review_envelope(actionable=True),
                  enforce_session_freshness=False, decision_dir=self.tmp)
        first = pdec.record_target_selection(**kw)
        second = pdec.record_target_selection(**kw)
        self.assertEqual(first["status"], pdec.TS_CREATED)
        self.assertEqual(second["status"], pdec.TS_REUSED)
        rows = json.loads((self.tmp / "target_selections.json").read_text("utf-8"))
        self.assertEqual(len(rows), 1, "no duplicate artifact")

    def test_64_a_changed_selection_is_an_auditable_revision(self):
        env = _review_envelope(actionable=True)
        pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=env, enforce_session_freshness=False,
            decision_dir=self.tmp)
        out = pdec.record_target_selection(
            target="FULL_TARGET", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=env, enforce_session_freshness=False,
            decision_dir=self.tmp)
        self.assertEqual(out["status"], pdec.TS_REVISED)
        rows = json.loads((self.tmp / "target_selections.json").read_text("utf-8"))
        self.assertEqual(len(rows), 2, "history is preserved, never overwritten")
        self.assertEqual(rows[1]["supersedes_selection_id"], rows[0]["selection_id"])

    def test_65_the_session_gate_refuses_before_anything_is_written(self):
        out = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope=_review_envelope(actionable=False),
            latest_session="2026-09-23", decision_dir=self.tmp)
        self.assertEqual(out["status"], pdec.TS_SESSION_STALE)
        self.assertEqual(out["next_required_action"], "RUN_PORTFOLIO_CYCLE")
        self.assertFalse((self.tmp / "target_selections.json").exists())

    def test_66_a_wrong_confirmation_token_writes_nothing(self):
        out = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm="CONFIRM_PORTFOLIO_REBALANCE_DECISION",
            review_envelope=_review_envelope(actionable=True),
            enforce_session_freshness=False, decision_dir=self.tmp)
        self.assertEqual(out["status"], pdec.TS_NOT_RECORDED)
        self.assertFalse((self.tmp / "target_selections.json").exists())


class TestOrderPlanCannotImplementAnUnselectedTarget(unittest.TestCase):
    """R69.1 - the gap this release found while proving the governed path.

    ``api.rebalance_execution`` reconciles the desk against the proposal
    artifact's ``allocations``, which are the FULL TARGET. It never read the R63
    governed selection, so an approved MINIMUM_REPAIR selection produced the full
    target's orders - 20 names and 35% turnover in place of the 14 names and 21%
    the operator chose. It now refuses instead.
    """

    def test_70_the_blocked_state_exists_and_is_non_confirmable(self):
        self.assertIn(rbx.RB_SELECTED_TARGET_NOT_IMPLEMENTABLE, rbx.STATE_VOCAB)
        self.assertIn(rbx.RB_SELECTED_TARGET_NOT_IMPLEMENTABLE,
                      rbx.NON_CONFIRMABLE_STATES)

    def test_71_only_the_full_target_is_implementable(self):
        self.assertEqual(rbx.IMPLEMENTABLE_TARGET, "FULL_TARGET")

    def test_72_the_gate_sits_before_the_plan_is_built(self):
        src = (ROOT / "api" / "rebalance_execution.py").read_text(
            encoding="utf-8", errors="replace")
        self.assertLess(src.index("RB_SELECTED_TARGET_NOT_IMPLEMENTABLE, \"bound\""),
                        src.index("plan = _reconcile_order_plan("),
                        "the refusal must precede plan construction")

    def test_73_the_gate_reads_the_governed_selection(self):
        src = (ROOT / "api" / "rebalance_execution.py").read_text(
            encoding="utf-8", errors="replace")
        self.assertIn("pdec.load_target_selection(", src)

    def test_74_the_refusal_names_the_selected_target(self):
        src = (ROOT / "api" / "rebalance_execution.py").read_text(
            encoding="utf-8", errors="replace")
        block = src[src.index("RB_SELECTED_TARGET_NOT_IMPLEMENTABLE, \"bound\""):]
        block = block[:block.index("# Current desk state")]
        self.assertIn('"selected_target": _sel_target', block)
        self.assertIn("The governed selection for this session is %s", block)
        self.assertIn("nothing was written", block)


class TestSourceSliceAnchorsAreUnambiguous(unittest.TestCase):
    """R69.1 - the trap that hid a vacuous guard for a whole release.

    Several UI tests assert a property over a slice of the shipped file, bounded
    by two function names. R69 introduced ``function _pdrRenderLoading``, which
    ``"function _pdrRender"`` matches as a prefix, and which is defined ABOVE the
    slice's start anchor. ``str.index`` returned that earlier offset, the slice
    inverted to ``""``, and ``assert "alert(" not in block`` passed against an
    empty string for every run.

    A guard that cannot fail is worse than no guard: it reports safety it never
    checked. These tests make the anchors themselves the thing under test.
    """

    def test_90_the_render_anchor_matches_exactly_one_definition(self):
        src = _ui()
        self.assertEqual(src.count(RENDER), 1, RENDER)
        self.assertGreater(src.count("function _pdrRender"), 1,
                           "the prefix collision that caused this is still present, "
                           "which is exactly why the anchor carries a signature")

    def test_91_the_selection_path_slice_is_non_empty(self):
        src = _ui()
        self.assertLess(src.index("function _pdrSelBtn"), src.index(RENDER))
        self.assertGreater(len(_slice(src, "function _pdrSelBtn", RENDER)), 2000)

    def test_92_every_anchor_this_suite_uses_is_unique(self):
        src = _ui()
        for anchor in ("function _pdrSelBtn", "function _pdrSelect(",
                       "window._pdrSelect = _pdrSelect",
                       "function _pdrSelectionResult", "function _pdrApproveCta",
                       "function _pdrSelClear", "function _pdrStatusBar",
                       "function _pdrStaleExplainer", "function _pdrMore(",
                       "function _pdrArmApprove", "window._pdrArmApprove",
                       "function _pdrAudit", "function _r47Num", RENDER):
            self.assertEqual(src.count(anchor), 1, "ambiguous anchor: %s" % anchor)

    def test_93_the_r63_guard_now_slices_real_source(self):
        """The repaired R63 assertion must cover the selection path, not ''."""
        t = (ROOT / "tests" / "test_r63_governed_target_selection.py").read_text(
            encoding="utf-8", errors="replace")
        self.assertIn('ui.index("function _pdrRender(d, err)")', t)
        self.assertIn("the selection-path slice must not be empty", t)


class TestReviewStateVocabularyIsComplete(unittest.TestCase):

    def test_80_historical_and_current_are_distinct_states(self):
        self.assertIn(pdr.REVIEW_STATE_CURRENT, pdr.REVIEW_STATE_VOCAB)
        self.assertIn(pdr.REVIEW_STATE_HISTORICAL, pdr.REVIEW_STATE_VOCAB)
        self.assertNotEqual(pdr.REVIEW_STATE_CURRENT, pdr.REVIEW_STATE_HISTORICAL)

    def test_81_a_stale_governance_block_yields_the_historical_state(self):
        env = pdr._envelope(
            status=pdr.STATUS_OK, generated_at="2026-09-23T00:00:00+00:00",
            message="m", review={"target_selection": {}},
            governance={"actionable": False})
        self.assertEqual(env["review_state"], pdr.REVIEW_STATE_HISTORICAL)

    def test_82_a_current_governance_block_yields_the_current_state(self):
        env = pdr._envelope(
            status=pdr.STATUS_OK, generated_at="2026-09-23T00:00:00+00:00",
            message="m", review={"target_selection": {}},
            governance={"actionable": True})
        self.assertEqual(env["review_state"], pdr.REVIEW_STATE_CURRENT)


if __name__ == "__main__":
    unittest.main()
