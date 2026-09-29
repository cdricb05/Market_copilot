r"""R82.2 - THE PRE-RULING APPROVAL-STATE CONTRADICTION.

THE DEFECT, as it appeared on 2026-09-28 against the frozen 2026-09-25
``MINIMUM_REPAIR``. One rendered Proposal decision review panel said, in ONE paint:

  * Step 3 - ``RISK POLICY REVIEW REQUIRED`` / ``APPROVAL WITHHELD``;
  * Step 4 - ``RISK POLICY DECISION (yours to make)``, unresolved;
  * the status bar - ``NEXT REQUIRED ACTION: APPROVE SELECTED TARGET``;
  * Step 2 - an armed ``APPROVE MINIMUM REPAIR`` button.

The backend was right the whole time. ``api.portfolio_decision`` had been refusing
that approval at ``SELECTED_TARGET_REQUIRES_RISK_POLICY_REVIEW`` since R69.5. What
it never did was publish that refusal on a READ, so two browser surfaces each took
a SUBSET of the gate's inputs and called the result a verdict:

  * ``_pdrStatusBar``: ``f.next_required_action ? ... : (actionable ? (sel ?
    'APPROVE SELECTED TARGET' : 'CHOOSE A TARGET') : 'RUN PORTFOLIO CYCLE')`` -
    and ``freshness`` names an action only when the SESSION has moved on, so on a
    current session the fallback always won;
  * ``_pdrApproveBlock``: session actionable + ``implementable !== false`` was
    treated as sufficient to arm the control.

R82.2 publishes the gate. ``selected_target_approval_gate`` is the read counterpart
of the APPROVE branch of ``record_decision``, evaluated in that branch's own order,
and ``risk_policy_approval_gate`` is consumed by BOTH - so the read and the write
cannot drift by editing one of them.

Every world below is hermetic. No live endpoint, store, ledger, holding, cash or NAV
is touched; no threshold, cap or economic is edited; nothing here rules, approves,
orders or executes against anything real.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as apdr
from paper_trader.engine import selected_target as stgt

from tests.test_r69_5_risk_policy_gate import (
    SESSION, _approve6, _env6, _select6, _world6,
)
from tests.test_r82_risk_policy_ruling import _rule, _rule_unverified

JUDGE = stgt.RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
ACCEPT = stgt.RULING_ACCEPT_AS_IS
SUCCESSOR = pdec.TARGET_POLICY_COMPLIANT_REPAIR
UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


# --------------------------------------------------------------------------- #
# Helpers. Each one composes an EXISTING owner and adds no rule of its own.
# --------------------------------------------------------------------------- #
def _gate(sel, ddir, *, latest=SESSION, proposal_hash="HASH_R695"):
    fresh = pdec.decision_freshness(bound_session=SESSION, latest_session=latest)
    return pdec.selected_target_approval_gate(
        selection=sel, freshness=fresh, current_proposal_hash=proposal_hash,
        decision_dir=ddir)


def _store_bytes(ddir) -> dict:
    return {p.name: p.read_bytes() for p in sorted(Path(ddir).glob("*.json"))}


def _ui() -> str:
    return UI.read_text(encoding="utf-8", errors="replace")


def _rp_region(src: str) -> str:
    """The region ``scripts/audit_architecture.py`` guards."""
    start = src.find("function loadReallocationProposal")
    end = src.find("window.renderReallocationProposal")
    assert start != -1 and end > start
    return src[start:end]


def _fn(src: str, name: str, end_name: str) -> str:
    a = src.find("function %s(" % name)
    b = src.find("function %s(" % end_name)
    assert a != -1 and b > a, (name, end_name)
    return src[a:b]


def _code(src: str) -> str:
    """The same text with JavaScript COMMENTS removed.

    The comments in this region deliberately quote the defective line they replaced -
    that history is the most useful thing in the file - so an assertion that a
    literal is GONE has to be made against the code, not against the record of why
    it went. ``//`` preceded by ``:`` is left alone so a URL survives.
    """
    out = re.sub(r"/\*.*?\*/", " ", src, flags=re.S)
    return re.sub(r"(?<!:)//[^\n]*", " ", out)


# =========================================================================== #
# 1. THE REPORTED CONTRADICTION - reproduced, then proved closed
# =========================================================================== #
def test_01_the_exact_reported_contradiction_is_reproduced(tmp_path):
    """The live shape, rebuilt: a current session, an implementable frozen book, a
    PRIOR ruling whose provenance is unverified, and no authoritative ruling.

    Every one of the four surfaces' inputs is asserted here, in ONE test, because
    the defect was not any one of them being wrong - it was the four disagreeing.
    """
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule_unverified(sel, ddir, ruling=JUDGE)

    ruling = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    review = pdec.selection_policy_review(sel)
    decision = pdec.risk_policy_decision(selection=sel, ruling_state=ruling,
                                         decision_dir=ddir)
    gate = _gate(sel, ddir)

    # The live preconditions, restated so a future reader can see what world this is.
    assert ruling["state"] == stgt.RULED_UNVERIFIED
    assert ruling["authoritative"] is False
    assert ruling["satisfies_the_policy_review"] is False
    assert ruling["approval_blocked_by_the_ruling"] is False
    assert gate["session_actionable"] is True
    assert pdec.recorded_selection_implementability(sel)["implementable"] is True

    # SURFACE 1 - Step 3 still demands the review.            (criterion 1)
    assert review["required"] is True
    # SURFACE 2 - Step 4 still asks for the decision.          (criterion 2)
    assert decision["required"] is True
    assert decision["state"] == pdec.RPD_REQUIRED_PRIOR_UNVERIFIED
    assert decision["prior_unverified_ruling"]["governs_anything"] is False
    # SURFACE 3 - the next required action is NOT approval.    (criterion 3)
    assert gate["next_required_action"] == pdec.NEXT_ACTION_RECORD_RISK_POLICY_RULING
    assert gate["next_required_action"] != pdec.NEXT_ACTION_APPROVE_SELECTED_TARGET
    assert gate["next_required_action_label"] == "Risk policy decision"
    # SURFACE 4 - approval is unavailable.                     (criterion 4)
    assert gate["available"] is False
    assert gate["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert gate["approval_state_label"] == "APPROVAL WITHHELD"
    assert gate["approval_withheld_because"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED

    # ONE state behind all four.                               (criterion 5)
    assert gate["risk_policy"]["status"] == gate["status"]
    assert gate["risk_policy"]["prior_ruling_unverified"] is True
    assert gate["owner"] == pdec.OWNER == decision["owner"]


def test_02_the_read_gate_and_the_write_gate_cannot_disagree(tmp_path):
    """The invariant that makes criterion 5 structural rather than incidental.

    For the pre-ruling world the READ says unavailable with a status, and the WRITE
    refuses with THAT SAME status. There is no third answer, because
    ``risk_policy_approval_gate`` is the only place the order and the words live.
    """
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    gate = _gate(sel, ddir)
    wrote = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert gate["available"] is False
    assert wrote["recorded"] is False
    assert wrote["status"] == gate["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert wrote["next_required_action"] == gate["next_required_action"]
    # The write path carries the SAME gate object it was refused by.
    assert wrote["approval_gate"]["status"] == gate["risk_policy"]["status"]


def test_03_a_prior_unverified_ruling_never_makes_approval_available(tmp_path):
    """Criterion 6. Fail closed in BOTH directions: an unverified record neither
    governs (it must not block with a cap it has no authority to set) nor grants."""
    for ruling in (JUDGE, ACCEPT):
        _, art, ddir = _world6(tmp_path / ruling)
        sel = _select6("FULL_TARGET", ddir, art)["record"]
        _rule_unverified(sel, ddir, ruling=ruling)
        gate = _gate(sel, ddir)
        assert gate["available"] is False, ruling
        assert gate["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED, ruling
        assert gate["next_required_action"] == (
            pdec.NEXT_ACTION_RECORD_RISK_POLICY_RULING), ruling
        # ...and it is disclosed rather than substituted for a decision.
        assert gate["risk_policy"]["prior_ruling_unverified"] is True, ruling


# =========================================================================== #
# 2. THE POST-RULING TRANSITIONS
# =========================================================================== #
def test_04_accept_as_is_records_a_ruling_and_approves_nothing(tmp_path):
    """Criteria 7 and 8. The ruling answers the REVIEW; it does not answer the
    APPROVAL. Every ordinary term of the gate is re-run above the risk-policy one,
    and approval becomes available only because they all clear - never because a
    ruling was recorded."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    before = _gate(sel, ddir)
    assert before["available"] is False

    out = _rule(sel, ddir, ruling=ACCEPT)
    assert out["recorded"] is True
    # Recording is not approving: the ruling writer says so, and no decision exists.
    assert out["approves_proposal"] is False
    assert out["is_an_approval"] is False
    assert out["unblocks_approval"] is False
    assert out["manual_approval_still_required"] is True
    assert pdec.load_decision_record(
        active_book_id=(sel.get("binding") or {}).get("active_book_id"),
        eligible_market_date=SESSION, decision_dir=ddir) is None

    after = _gate(sel, ddir)
    assert after["risk_policy"]["withholds_approval"] is False
    assert after["risk_policy"]["ruling_answers_the_review"] is True
    assert after["available"] is True
    assert after["next_required_action"] == pdec.NEXT_ACTION_APPROVE_SELECTED_TARGET
    # Which is the write path's answer too, and only now.
    assert _approve6(ddir, art,
                     expected_selection_id=sel["selection_id"])["recorded"] is True


def test_05_accept_as_is_exposes_approval_only_if_every_ordinary_gate_clears(tmp_path):
    """Criterion 8, at the term that is NOT the risk-policy one. A ruling must never
    reach past the gates above it in the order."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir, ruling=ACCEPT)
    # The risk-policy term is satisfied...
    assert _gate(sel, ddir)["risk_policy"]["withholds_approval"] is False
    # ...and the SESSION term still removes the approval on its own.
    stale = _gate(sel, ddir, latest="2026-09-30")
    assert stale["available"] is False
    assert stale["status"] == pdec.PDS_SESSION_STALE
    assert stale["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE
    # ...as does the IDENTITY term.
    other = _gate(sel, ddir, proposal_hash="A_DIFFERENT_PROPOSAL")
    assert other["available"] is False
    assert other["status"] == pdec.PDS_STALE


def test_06_judge_exposes_no_approval_for_the_original_target(tmp_path):
    """Criteria 9 and 10. The ruling exists and the ORIGINAL frozen book fails it, so
    the refusal is substantive. The next action names the solved successor rather than
    a requirement nobody can satisfy."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir, ruling=JUDGE)

    gate = _gate(sel, ddir)
    assert gate["available"] is False
    assert gate["status"] == pdec.PDS_REFERENCE_LIMIT_BREACH_RULED
    assert gate["next_required_action"] == (
        pdec.NEXT_ACTION_REVIEW_THE_POLICY_COMPLIANT_SUCCESSOR)
    assert gate["policy_compliant_successor_target"] == SUCCESSOR
    # The write path refuses with the same status, and REJECT / HOLD stay available.
    wrote = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert wrote["recorded"] is False
    assert wrote["status"] == gate["status"]
    assert wrote["next_required_action"] == gate["next_required_action"]
    assert _approve6(ddir, art, decision=pdec.DECISION_REJECT,
                     expected_selection_id=sel["selection_id"])["recorded"] is True


def test_07_the_successor_needs_its_own_selection_and_its_own_approval(tmp_path):
    """Criteria 11 and 12. Two separate manual acts, and the gate says so at each
    step: the successor being SOLVED does not make it SELECTED, and its selection
    does not make it APPROVED."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir, ruling=JUDGE)
    env = _env6(art, ddir)
    suc = env["policy_compliant_successor"]
    assert suc["state"] == apdr.SUCCESSOR_SOLVED
    assert suc["is_an_approval"] is False
    assert suc["manual_approval_still_required"] is True
    # Still bound to the OLD selection, so the gate still withholds.
    assert env["approval_gate"]["available"] is False
    assert env["approval_gate"]["status"] == pdec.PDS_REFERENCE_LIMIT_BREACH_RULED

    # ACT ONE: select it. A separate governed write through the ordinary lane.
    picked = _select6(SUCCESSOR, ddir, art)
    assert picked["selected"] is True
    new_sel = picked["record"]
    assert new_sel["selected_target"] == SUCCESSOR
    assert new_sel["selected_target_implementation_hash"] != sel[
        "selected_target_implementation_hash"]

    # ACT TWO: and only now may approval become available - never automatically.
    gate = _gate(new_sel, ddir)
    assert gate["selected_target"] == SUCCESSOR
    assert gate["available"] is True
    assert gate["next_required_action"] == pdec.NEXT_ACTION_APPROVE_SELECTED_TARGET
    assert gate["is_an_approval"] is False and gate["approves_anything"] is False
    # Selecting wrote no decision of its own.
    assert pdec.load_decision_record(
        active_book_id=(new_sel.get("binding") or {}).get("active_book_id"),
        eligible_market_date=SESSION, decision_dir=ddir) is None


# =========================================================================== #
# 3. FAIL-CLOSED STATES
# =========================================================================== #
def test_08_a_stale_identity_fails_closed(tmp_path):
    """Criterion 13. A selection bound to another proposal can never arm an
    approval affordance, whatever its risk-policy state."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir, ruling=ACCEPT)
    gate = _gate(sel, ddir, proposal_hash="pprop_something_else")
    assert gate["available"] is False
    assert gate["status"] == pdec.PDS_STALE
    assert gate["next_required_action"] == (
        pdec.NEXT_ACTION_SELECT_AGAINST_THE_CURRENT_REVIEW)
    assert gate["selection_bound_proposal_hash"] == "HASH_R695"
    assert gate["current_proposal_hash"] == "pprop_something_else"


def test_09_no_selection_and_no_target_arm_nothing(tmp_path):
    """The two states that precede a frozen book. Neither may name approval."""
    _, art, ddir = _world6(tmp_path)
    empty = _gate(None, ddir)
    assert empty["available"] is False
    assert empty["status"] == pdec.PDS_TARGET_SELECTION_REQUIRED
    assert empty["next_required_action"] == pdec.NEXT_ACTION_SELECT_A_TARGET

    cur = _select6("CURRENT", ddir, art)["record"]
    gate = _gate(cur, ddir)
    assert gate["available"] is False
    assert gate["status"] == pdec.PDS_SELECTION_IS_NO_CHANGE
    assert gate["next_required_action"] == (
        pdec.NEXT_ACTION_RECORD_THE_NO_CHANGE_DECISION)


def test_10_the_vocabulary_is_declared_once_and_spelled_once():
    """No second vocabulary, and no literal left behind in the write path."""
    assert set(pdec.NEXT_ACTION_VOCAB) == set(pdec.NEXT_ACTION_LABELS)
    assert len(set(pdec.NEXT_ACTION_VOCAB)) == len(pdec.NEXT_ACTION_VOCAB)
    for action in pdec.NEXT_ACTION_VOCAB:
        assert pdec.NEXT_ACTION_LABELS[action], action
    for status in pdec.APPROVAL_GATE_STATUS_VOCAB:
        assert status in pdec.DECISION_STATE_VOCAB, status
    # The R82/R82.1 literals are now constants, and the module holds ONE copy of
    # each: the definition. A second occurrence would be a second spelling.
    src = (Path(pdec.__file__)).read_text(encoding="utf-8")
    for literal in ('"RECORD_RISK_POLICY_RULING"',
                    '"REVIEW_AND_SELECT_THE_POLICY_COMPLIANT_SUCCESSOR_TARGET"'):
        assert src.count(literal) == 1, literal
    # And the gate's "available" word is the canonical manual-review state, reused.
    assert pdec.AG_AVAILABLE == pdec.PDS_REVIEW_REQUIRED
    assert pdec.AG_AVAILABLE in pdec.APPROVABLE_DECISION_STATES


# =========================================================================== #
# 4. THE READ MODELS - one verdict, published, and nothing written
# =========================================================================== #
def test_11_the_decision_lane_cannot_contradict_the_gate(tmp_path):
    """The cockpit's Approval line reads ``derive_decision_state``. It consumes the
    gate on the same contract it already gives session freshness: an absent verdict
    is "no information", and a supplied one can only REMOVE approvability."""
    summary = {"reallocation_proposal_available": True,
               "reallocation_proposal_hash": "HASH_R695",
               "reallocation_action_counts": {"EXIT": 3, "ADD": 1},
               "reallocation_one_way_turnover": 0.30}
    base = dict(has_active_book=True, proposal_summary=summary, decision_record=None)

    # No gate supplied -> the pre-R82.2 answer, unchanged.
    plain = pdec.derive_decision_state(**base)
    assert plain["approvable"] is True
    assert plain["approval_gate"] is None

    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    withheld = pdec.derive_decision_state(**base, approval_gate=_gate(sel, ddir))
    assert withheld["approvable"] is False
    assert withheld["next_required_action"] == (
        pdec.NEXT_ACTION_RECORD_RISK_POLICY_RULING)
    assert withheld["approval_gate"]["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert withheld["approval_gate_owner"] == pdec.OWNER

    _rule(sel, ddir, ruling=ACCEPT)
    cleared = pdec.derive_decision_state(**base, approval_gate=_gate(sel, ddir))
    assert cleared["approvable"] is True
    assert cleared["next_required_action"] == (
        pdec.NEXT_ACTION_APPROVE_SELECTED_TARGET)


def test_12_the_review_envelope_publishes_one_gate(tmp_path):
    """The panel's four surfaces all read this envelope, so the gate must be ON it,
    once, and identically wherever it appears."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule_unverified(sel, ddir, ruling=JUDGE)
    env = _env6(art, ddir)

    gate = env["approval_gate"]
    assert gate == env["governance"]["approval_gate"], "one object, two names"
    assert env["approval_gate_owner"] == pdec.OWNER
    assert env["approval_available"] is False
    assert env["next_required_action"] == pdec.NEXT_ACTION_RECORD_RISK_POLICY_RULING
    assert gate["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    # And it agrees with the two blocks Step 3 and Step 4 render, on the SAME read.
    assert env["risk_policy_decision"]["required"] is True
    assert env["risk_policy_ruling"]["satisfies_the_policy_review"] is False
    assert (sel["selected_target_implementation"]["risk_contribution"]
            ["policy_review"]["required"]) is True
    # The gate never claims an authority it does not have.
    for flag in ("is_an_approval", "approves_anything", "creates_order_plan",
                 "creates_orders", "creates_fills"):
        assert gate[flag] is False, flag
    assert gate["writes_nothing"] is True
    assert gate["reject_and_hold_remain_available"] is True


def test_13_reading_the_gate_writes_nothing(tmp_path):
    """Criteria 15, 16 and 17. Ten reads, byte-for-byte no change to the store."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule_unverified(sel, ddir, ruling=JUDGE)
    before = _store_bytes(ddir)
    for _ in range(10):
        _gate(sel, ddir)
        pdec.risk_policy_approval_gate(
            policy_review=pdec.selection_policy_review(sel),
            ruling_state=pdec.risk_policy_ruling_state(selection=sel,
                                                       decision_dir=ddir))
    assert _store_bytes(ddir) == before
    # No decision and no NEW ruling came into existence.
    assert json.loads((Path(ddir) / "risk_policy_rulings.json").read_text(
        encoding="utf-8")).__len__() == 1
    assert not (Path(ddir) / "decisions.json").exists()


def test_14_the_frozen_review_and_the_frozen_selection_are_untouched(tmp_path):
    """Criterion 18. The gate is published BESIDE the frozen artifacts, so neither
    ``review_hash`` nor the frozen book's identity may move because it exists."""
    _, art, ddir = _world6(tmp_path)
    plain = _env6(art, ddir)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule_unverified(sel, ddir, ruling=JUDGE)
    ruled = _env6(art, ddir)
    assert ruled["review_hash"] == plain["review_hash"]
    assert ruled["review"] == plain["review"]
    assert "approval_gate" not in json.dumps(ruled["review"])
    # ...and re-reading the selection returns the same frozen book.
    again = pdec.load_target_selection(
        active_book_id=(sel.get("binding") or {}).get("active_book_id"),
        eligible_market_date=SESSION, decision_dir=ddir)
    assert again["selected_target_implementation_hash"] == sel[
        "selected_target_implementation_hash"]


# =========================================================================== #
# 5. THE BROWSER - it renders the verdict and derives none of it
# =========================================================================== #
def test_20_the_status_bar_reads_the_gate_and_invents_no_action():
    """THE defect, at the exact line that produced it. The fallback chain that
    answered ``APPROVE SELECTED TARGET`` from ``actionable && sel`` is gone, and the
    bar reads ``approval_gate`` for both cells."""
    src = _ui()
    bar = _fn(src, "_pdrStatusBar", "_pdrStaleExplainer")
    assert "d.approval_gate" in bar, "the bar reads the backend gate"
    assert "g.next_required_action" in bar
    assert "g.next_required_action_label" in bar
    assert "g.approval_withheld_because" in bar
    # The browser's own answer is GONE - the whole fallback, not just its wording.
    # Asserted against the CODE: the comment above it quotes the defective line on
    # purpose, and that record is worth keeping.
    code = _code(bar)
    assert "'APPROVE SELECTED TARGET'" not in code
    assert "'CHOOSE A TARGET'" not in code
    assert "APPROVE SELECTED TARGET" not in _code(src), (
        "no surface may spell the action; it is the backend's word")
    assert "actionable ? (sel ?" not in code, "the fallback chain itself is gone"
    # An absent gate is a STATED unknown, never an assumed action.
    assert "NOT PUBLISHED" in bar
    # Both new cells exist and neither is blank.
    assert "cell('Approval'" in bar
    assert "cell('Next required action'" in bar


def test_21_the_approve_control_is_armed_only_by_the_backend_gate():
    """Criterion 4 and 5 in the browser. ``available === true`` is necessary, and the
    withheld path renders a DISABLED control carrying the backend's own reason."""
    src = _ui()
    block = _fn(src, "_pdrApproveBlock", "_pdrSelectedTarget")
    assert "d.approval_gate" in block
    assert "if (g.available === true) return _pdrApproveCta(t);" in block, (
        "the armed control has exactly ONE precondition, and it is the gate's")
    # The two inputs that used to BE the verdict no longer decide availability.
    assert "return _pdrApproveWithheld(g, t);" in block
    # Fail closed when the backend published no gate at all.
    assert "APPROVAL STATE UNKNOWN" in block
    assert "if (!g) {" in block

    withheld = _fn(src, "_pdrApproveWithheld", "_pdrApproveBlock")
    assert 'id="pdr-approve-cta" disabled' in withheld, "rendered, and inert"
    assert "onclick" not in withheld.split('id="pdr-approve-cta"')[1].split(
        "</button>")[0], "a disabled control carries no handler"
    assert "g.approval_withheld_because" in withheld
    assert "g.detail" in withheld
    assert "g.next_required_action_label" in withheld
    assert "APPROVE ' + escapeHtml(t)" in withheld, "the button is never blank"
    for badge in ("APPROVAL UNAVAILABLE", "NO ORDERS", "MANUAL REVIEW"):
        assert badge in withheld, badge


def test_22_the_approve_write_refuses_to_even_ask():
    """Defence in depth, as ``_pdrSelect`` and ``_pdrRecordRuling`` already have it:
    the function is reachable by name and the request it makes is a WRITE."""
    src = _ui()
    send = _fn(src, "_pdrApprove", "_pdrStatusBar")
    assert "gate.available !== true" in send
    assert "APPROVAL_GATE_NOT_PUBLISHED" in send
    # The refusal happens BEFORE anything is sent.
    assert send.index("gate.available !== true") < send.index("fetch(")


def test_23_the_four_surfaces_name_one_status():
    """Criterion 5, provable from a screenshot: the bar, the Step 2 block and the
    Step 3 policy block all print the SAME field, which only the backend sets."""
    src = _ui()
    token = "approval_withheld_because"
    for name, end in (("_pdrStatusBar", "_pdrStaleExplainer"),
                      ("_pdrApproveWithheld", "_pdrApproveBlock"),
                      ("_pdrPolicyReview", "_pdrScopeLine")):
        assert token in _fn(src, name, end), name
    # And Step 4 is driven by the decision contract the gate composes.
    assert "dec.required" in _fn(src, "_pdrRiskPolicyDecision", "_pdrRuleReset")


def test_24_the_region_holds_no_risk_policy_arithmetic_and_no_dialog():
    """Criterion 14, at the exact boundary the audit enforces."""
    region = _rp_region(_ui())
    for pat in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.",
                "cost_rate", "COST_BPS", "compute"):
        assert pat not in region, pat
    for banned in ("alert(", "confirm("):
        assert banned not in region, banned
    # No cap, limit or excess is derived here: the gate's own numbers are read.
    for pat in ("0.12", "21.428571", "3 / n", "3/n"):
        assert pat not in _fn(_ui(), "_pdrApproveWithheld", "_pdrApproveBlock"), pat


def test_25_no_create_orders_or_automation_was_added():
    """The standing hard-failure lines, re-checked at the surface this release
    touched."""
    for name, end in (("_pdrApproveWithheld", "_pdrApproveBlock"),
                      ("_pdrApproveBlock", "_pdrSelectedTarget"),
                      ("_pdrStatusBar", "_pdrStaleExplainer")):
        block = _fn(_ui(), name, end)
        for banned in ("createOrders", "create_orders", "placeOrder", "runFillCycle",
                       "setInterval(", "alert(", "confirm("):
            assert banned not in block, (name, banned)


def test_26_the_wireframe_was_produced_before_the_code():
    doc = (Path(__file__).resolve().parents[1] / "docs"
           / "R82_2_PRE_RULING_APPROVAL_STATE_WIREFRAME.md")
    assert doc.exists(), "CLAUDE.md requires a wireframe before UI work"
    text = doc.read_text(encoding="utf-8")
    for section in ("## 1. SCAN", "## 2. REVIEW", "## 3. PLAN",
                    "## 4. Acceptance criteria"):
        assert section in text, section
    assert "1920x1080" in text
    # The critique that justified the work, and the state table it planned.
    assert "writeControlsDerivedFromBackendGate = 0" in text
    assert "NEXT REQUIRED ACTION" in text
    assert pdec.PDS_RISK_POLICY_REVIEW_REQUIRED in text
    assert pdec.NEXT_ACTION_RECORD_RISK_POLICY_RULING in text
    assert SUCCESSOR in text
