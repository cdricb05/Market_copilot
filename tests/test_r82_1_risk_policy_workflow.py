r"""R82.1 - THE RISK POLICY DECISION, END TO END.

R82 made the ruling recordable and durable, and left two holes wide enough to walk
through.

  1. **Nobody could rule.** The panel demanded a decision and shipped no control of
     any kind: the only route to the governed writer was a hand-rolled HTTP request.
     A store whose sole author is a script is not an operator decision store, and
     R82's own live record proves it - written during implementation, by a direct
     call, labelled ``ruled_by: operator``.
  2. **JUDGE was a dead end.** It reopened AMD and DDOG against the 12% cap and
     answered ``SELECT_A_COMPLIANT_TARGET_OR_REJECT`` while no compliant target
     existed anywhere in the system. That is a requirement, not a workflow.

This suite proves both are closed, and proves the shape of each fix:

  * PROVENANCE is DERIVED, never asserted. A ruling is authoritative only with the
    single-use token a governed read minted for that exact frozen book plus the
    operator's explicit confirmation. Without them the record is kept as readable
    audit evidence and binds NOTHING - it neither blocks an approval (a development
    artifact must not govern) nor clears one (it must not grant). No ruling id is
    special-cased anywhere; the rule is the absence of evidence.
  * The SUCCESSOR is SOLVED, by the same repair owner, against the cap the ruling
    made binding, with the portfolio's risk re-measured by the canonical covariance
    owner between rounds. The first-order indicative weights R69.5 publishes for one
    name in isolation are never passed off as solved weights, and a repair that
    cannot reach compliance fails closed with the open breach named.
  * The ordinary review is returned BYTE-IDENTICAL whether or not a ruling exists,
    because ``review_hash`` is computed over it and every governed selection binds
    it.

Every world is hermetic. No live endpoint, store, ledger, holding, cash or NAV is
touched, no threshold is edited, and nothing here approves, orders or executes
against anything real.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as apdr
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import proposal_decision_review as kernel
from paper_trader.engine import selected_target as stgt

from tests.test_r69_5_risk_policy_gate import (
    SESSION, _approve6, _env6, _hoc6, _proposal6, _returns6, _select6, _world6,
)
from tests.test_r82_risk_policy_ruling import _rule, _rule_unverified, _rulings

JUDGE = stgt.RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
ACCEPT = stgt.RULING_ACCEPT_AS_IS
FLOOR = stgt.RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR
SUCCESSOR = pdec.TARGET_POLICY_COMPLIANT_REPAIR


def _ruled_world(tmp, ruling=JUDGE, verified=True):
    """A hermetic world with a governed selection and ONE ruling on it."""
    _, art, ddir = _world6(tmp)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    out = (_rule(sel, ddir, ruling=ruling) if verified
           else _rule_unverified(sel, ddir, ruling=ruling))
    return art, ddir, sel, out


# =========================================================================== #
# 1. PROVENANCE - the correction is general, and special-cases no record
# =========================================================================== #
def test_01_a_ruling_with_no_provenance_block_fails_closed():
    """Every R82 record on disk looks exactly like this. The rule is the ABSENCE of
    evidence, so it reaches all of them without naming one."""
    for rec in ({}, {"ruling": JUDGE}, {"ruling": JUDGE, "ruled_by": "operator"},
                {"ruling": JUDGE, "provenance": {}},
                {"ruling": JUDGE, "provenance": None}):
        got = pdec.ruling_provenance_state(rec)
        assert got["verified"] is False, rec
        assert got["operational_use"] == pdec.RULING_USE_UNVERIFIED
        assert got["channel"] == pdec.RULING_CHANNEL_ABSENT
        assert got["reason"] == pdec.PROV_MISSING
        # It is still evidence, and a later verified ruling supersedes it.
        assert got["readable_as_audit_evidence"] is True
        assert got["supersedable_by_a_verified_ruling"] is True


def test_02_ruled_by_operator_is_narration_and_proves_nothing():
    """THE defect. ``ruled_by`` is a free string the caller supplies, so a record
    saying "operator" is not evidence of one."""
    rec = {"ruling": JUDGE, "ruled_by": "operator",
           "provenance": {"channel": pdec.RULING_CHANNEL_OPERATOR_UI,
                          "verified": False, "reason": pdec.PROV_NO_TOKEN}}
    assert pdec.ruling_provenance_state(rec)["verified"] is False
    # And a caller cannot declare its own channel verified either.
    rec2 = {"ruling": JUDGE,
            "provenance": {"channel": "SOMETHING_I_MADE_UP", "verified": True}}
    assert pdec.ruling_provenance_state(rec2)["verified"] is False


def test_03_the_channel_is_derived_from_evidence_not_from_the_caller():
    """R82.1.1. The evidence is a SPENT confirmation this backend issued - not a
    token any caller can derive, and not a boolean any caller can set."""
    args = dict(selection_id="psel_x", selected_target_implementation_hash="bookhash",
                reference_limit=0.12)
    token = pdec.ruling_submission_token(**args)
    cer = pdec.open_ruling_confirmation(
        ruling=JUDGE, confirm=pdec.RULING_CONFIRM_TOKEN, submission_token=token,
        **args)
    assert cer["issued"] is True and cer["confirmation"]
    cons = pdec.consume_ruling_confirmation(
        confirmation=cer["confirmation"], ruling=JUDGE, **args)
    ok = pdec.ruling_provenance(submission_token=token, confirmed_in_ui=True,
                                consumption=cons, ruling=JUDGE, **args)
    assert ok["verified"] is True
    assert ok["channel"] == pdec.RULING_CHANNEL_OPERATOR_UI
    assert ok["channel_asserted_by_caller"] is False
    assert ok["operational_use"] == pdec.RULING_USE_AUTHORITATIVE
    # Each missing piece of evidence lands on its OWN named reason.
    def _fresh():
        c = pdec.open_ruling_confirmation(
            ruling=JUDGE, confirm=pdec.RULING_CONFIRM_TOKEN,
            submission_token=token, **args)["confirmation"]
        return pdec.consume_ruling_confirmation(confirmation=c, ruling=JUDGE, **args)

    assert pdec.ruling_provenance(submission_token=None, confirmed_in_ui=True,
                                  consumption=_fresh(), ruling=JUDGE,
                                  **args)["reason"] == pdec.PROV_NO_TOKEN
    assert pdec.ruling_provenance(submission_token="nope", confirmed_in_ui=True,
                                  consumption=_fresh(), ruling=JUDGE,
                                  **args)["reason"] == pdec.PROV_TOKEN_MISMATCH
    # THE R82.1 BYPASS: the served token plus the caller's own confirmation claim,
    # and no ceremony. It proves nothing and it says so.
    bypass = pdec.ruling_provenance(submission_token=token, confirmed_in_ui=True,
                                    ruling=JUDGE, **args)
    assert bypass["verified"] is False
    assert bypass["channel"] == pdec.RULING_CHANNEL_API_DIRECT
    assert bypass["reason"] == pdec.PROV_NO_CONFIRMATION
    assert bypass["asserted_confirmation_is_evidence"] is False
    assert bypass["submission_token_alone_is_evidence"] is False
    # It never overclaims what it proves.
    assert "does not claim" in ok["does_not_prove"]


def test_04_a_token_for_one_book_cannot_submit_a_ruling_about_another():
    a = dict(selection_id="psel_a", selected_target_implementation_hash="book_a",
             reference_limit=0.12)
    b = dict(selection_id="psel_b", selected_target_implementation_hash="book_b",
             reference_limit=0.12)
    tok_a = pdec.ruling_submission_token(**a)
    assert pdec.ruling_provenance(submission_token=tok_a, confirmed_in_ui=True,
                                  **b)["verified"] is False
    # The cap is bound too: the same book at a different reference limit is a
    # different question.
    c = dict(a, reference_limit=0.09)
    assert pdec.ruling_provenance(submission_token=tok_a, confirmed_in_ui=True,
                                  **c)["verified"] is False
    # And the ceremony refuses to even OPEN against a book the token does not name,
    # so no confirmation for book B can come into existence from book A's token.
    refused = pdec.open_ruling_confirmation(
        ruling=JUDGE, confirm=pdec.RULING_CONFIRM_TOKEN, submission_token=tok_a, **b)
    assert refused["issued"] is False
    assert refused["reason"] == pdec.PROV_TOKEN_MISMATCH
    assert refused["confirmation"] is None
    # No frozen book, no submittable ruling, and no ceremony either.
    assert pdec.ruling_submission_token(
        selection_id=None, selected_target_implementation_hash="x",
        reference_limit=0.12) is None
    assert pdec.open_ruling_confirmation(
        ruling=JUDGE, confirm=pdec.RULING_CONFIRM_TOKEN, submission_token="anything",
        selection_id=None, selected_target_implementation_hash="x",
        reference_limit=0.12)["reason"] == pdec.CEREMONY_NO_BOOK


def test_05_an_unattributed_ruling_is_no_longer_labelled_operator(tmp_path):
    """R82 defaulted ``ruled_by`` to "operator", which is how a development call came
    to be recorded as an operator's decision."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _rule(sel, ddir, actor=None)
    assert got["record"]["ruled_by"] == pdec.RULING_ACTOR_UNATTRIBUTED
    assert got["record"]["ruled_by_is_evidence"] is False


def test_06_an_unverified_ruling_binds_nothing_and_grants_nothing(tmp_path):
    """Fail closed in BOTH directions. This is the whole treatment of the live R82
    record: readable, disclosed, and without authority."""
    art, ddir, sel, out = _ruled_world(tmp_path, verified=False)
    assert out["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert out["recorded"] is True, "it is preserved as audit evidence"
    assert out["governs_this_book"] is False
    assert "GOVERNS" in out["message"]

    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["ruled"] is True, "the operator must be told the record is there"
    assert state["authoritative"] is False
    assert state["state"] == stgt.RULED_UNVERIFIED
    # It does not GOVERN...
    assert state["binding_limit"] is None
    assert state["reopened_obligations"] == []
    assert state["approval_blocked_by_the_ruling"] is False
    assert state["derived"] is None
    # ...and it does not GRANT. Approval stays exactly where R69.5 left it.
    assert state["satisfies_the_policy_review"] is False
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert got["recorded"] is False
    # What it WOULD mean is published under a name no governed consumer reads.
    assert state["derived_if_confirmed"]["binding_limit"] is not None


def test_07_history_is_never_deleted_or_rewritten(tmp_path):
    art, ddir, sel, first = _ruled_world(tmp_path, verified=False)
    before = (Path(ddir) / "risk_policy_rulings.json").read_bytes()
    second = _rule(sel, ddir)
    rows = _rulings(ddir)
    assert len(rows) == 2
    assert json.dumps(rows[0]) == json.dumps(json.loads(before)[0]), (
        "the superseded record is byte-identical")
    assert rows[1]["supersedes_ruling_id"] == first["ruling_id"]
    assert second["status"] == pdec.RULING_REVISED


def test_08_the_new_state_and_vocabularies_are_declared():
    assert stgt.RULED_UNVERIFIED in stgt.RULED_STATE_VOCAB
    assert pdec.RULING_RECORDED_UNVERIFIED in pdec.RULING_STATUS_VOCAB
    assert pdec.RULING_CHANNEL_TRUSTED == (pdec.RULING_CHANNEL_OPERATOR_UI,)
    assert set(pdec.RULING_USE_VOCAB) == {pdec.RULING_USE_AUTHORITATIVE,
                                          pdec.RULING_USE_UNVERIFIED}
    # No ruling id appears anywhere in the owner: the correction is general.
    src = (Path(__file__).resolve().parents[1] / "api" / "portfolio_decision.py"
           ).read_text(encoding="utf-8")
    assert "b099e628c030" not in src
    assert "prul_2026" not in src


# =========================================================================== #
# 2. THE OPERATOR DECISION CONTRACT - what a write surface renders
# =========================================================================== #
def test_10_an_unruled_book_publishes_the_decision_contract(tmp_path):
    """Acceptance criteria 1-3. The screen can build a control because the backend
    publishes one, vocabulary and availability included."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    dec = _env6(art, ddir)["risk_policy_decision"]
    assert dec["required"] is True
    assert dec["state"] == pdec.RPD_REQUIRED
    assert dec["route"] == pdec.RULING_ROUTE
    assert dec["confirm_token"] == pdec.RULING_CONFIRM_TOKEN
    assert dec["operator_confirmation_required"] is True
    assert dec["submission_token"]
    assert dec["approves_nothing"] is True
    assert dec["prior_unverified_ruling"] is None
    # The options are the KERNEL's, in the kernel's order, with the kernel's
    # availability. A screen holding its own copy is the defect this prevents.
    assert [o["ruling"] for o in dec["options"]] == list(stgt.RULING_VOCAB)
    by = {o["ruling"]: o for o in dec["options"]}
    assert by[ACCEPT]["available"] is True
    assert by[JUDGE]["available"] is True
    assert by[FLOOR]["available"] is False
    assert by[FLOOR]["unavailable_reason"] == "RULING_NOT_AVAILABLE_IN_THIS_RELEASE"
    # Every option states, in plain English, that it approves nothing.
    for o in dec["options"]:
        assert o["does_not_approve"] is True
        assert o["detail"], o["ruling"]
    assert by[JUDGE]["builds_a_successor_target"] is True
    assert by[ACCEPT]["builds_a_successor_target"] is False


def test_11_the_submission_binds_the_exact_frozen_identity(tmp_path):
    """Acceptance criterion 6. Everything the operator is shown is what the
    submission names."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    env = _env6(art, ddir)
    b = env["risk_policy_decision"]["binds"]
    review = pdec.selection_policy_review(sel)
    assert b["proposal_id"] == env["proposal_id"]
    assert b["selection_id"] == sel["selection_id"]
    assert b["selected_target_implementation_hash"] == sel[
        "selected_target_implementation_hash"]
    assert b["reference_limit"] == review["reference_limit"]
    assert b["governed_limit"] == review["governed_limit"]
    assert b["instruments"] == sorted(review["instruments"])
    # And the token the envelope publishes opens a ceremony against exactly that
    # identity - which is all the token does. It is the SPENT confirmation, not the
    # token, that verifies.
    ident = dict(selection_id=b["selection_id"],
                 selected_target_implementation_hash=b[
                     "selected_target_implementation_hash"],
                 reference_limit=b["reference_limit"])
    tok = env["risk_policy_decision"]["submission_token"]
    cer = pdec.open_ruling_confirmation(
        ruling=JUDGE, confirm=pdec.RULING_CONFIRM_TOKEN, submission_token=tok,
        **ident)
    assert cer["issued"] is True
    prov = pdec.ruling_provenance(
        submission_token=tok, confirmed_in_ui=True, ruling=JUDGE,
        consumption=pdec.consume_ruling_confirmation(
            confirmation=cer["confirmation"], ruling=JUDGE, **ident),
        **ident)
    assert prov["verified"] is True
    # The read publishes no confirmation. It cannot: a confirmation a read handed
    # out would be evidence of having read, which is what R82.1 already had.
    assert "confirmation" not in env["risk_policy_decision"]


def test_12_a_prior_unverified_record_is_disclosed_not_substituted(tmp_path):
    """Acceptance criteria 1 and 2 on the LIVE shape: a record exists, the decision
    is still owed, and the controls are still offered."""
    art, ddir, sel, _ = _ruled_world(tmp_path, verified=False)
    dec = _env6(art, ddir)["risk_policy_decision"]
    assert dec["required"] is True
    assert dec["state"] == pdec.RPD_REQUIRED_PRIOR_UNVERIFIED
    prior = dec["prior_unverified_ruling"]
    assert prior["ruling"] == JUDGE
    assert prior["governs_anything"] is False
    assert prior["provenance_state"]["verified"] is False
    assert "supersedes" in dec["detail"]


def test_13_a_taken_decision_is_not_re_offered_as_a_form(tmp_path):
    art, ddir, sel, _ = _ruled_world(tmp_path)
    dec = _env6(art, ddir)["risk_policy_decision"]
    assert dec["required"] is False
    assert dec["state"] == pdec.RPD_TAKEN
    assert dec["submission_token"] is None


def test_14_a_book_that_owes_no_ruling_offers_no_decision(tmp_path):
    from tests.test_r69_2_selected_target_lifecycle import _select, _world
    _, _, art, ddir, _ = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    dec = pdec.risk_policy_decision(selection=sel, decision_dir=ddir)
    assert dec["required"] is False
    assert dec["state"] == pdec.RPD_NOT_APPLICABLE


def test_15_the_unavailable_ruling_cannot_be_submitted(tmp_path):
    """Acceptance criterion 4, at the WRITE gate rather than only on screen."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _rule(sel, ddir, ruling=FLOOR)
    assert got["status"] == pdec.RULING_NOT_AVAILABLE
    assert got["recorded"] is False
    assert _rulings(ddir) == []
    # And no absolute threshold was invented on the way past.
    assert got["creates_absolute_companion_floor"] is False


def test_16_reading_the_panel_writes_nothing(tmp_path):
    """Acceptance criterion 2: no ruling exists before the human action."""
    _, art, ddir = _world6(tmp_path)
    _select6("FULL_TARGET", ddir, art)
    before = sorted((p.name, p.read_bytes()) for p in Path(ddir).iterdir())
    for _ in range(3):
        env = _env6(art, ddir)
        assert env["risk_policy_decision"]["required"] is True
        assert env["read_only"] is True and env["writes_nothing"] is True
    assert sorted((p.name, p.read_bytes()) for p in Path(ddir).iterdir()) == before
    assert not (Path(ddir) / "risk_policy_rulings.json").exists()


# =========================================================================== #
# 3. THE KERNEL - a repair solved against a HELD cap
# =========================================================================== #
def _kernel_inputs(tmp_path):
    _, art, _ddir = _world6(tmp_path)
    proposal = art["proposal"]
    policy = kernel.review_policy(proposal)
    obligations = kernel.repair_obligations(proposal=proposal,
                                            hoc_assessment=_hoc6(), policy=policy)
    return proposal, policy, obligations, _returns6()


def test_20_the_default_repair_is_byte_identical(tmp_path):
    """THE regression that protects every governed selection ever made: ``review_hash``
    is computed over the review, and an unconditional new key would change the
    identity of every review in history without one input moving."""
    proposal, policy, obligations, ar = _kernel_inputs(tmp_path)
    a = kernel.solve_minimum_repair(proposal=proposal, obligations=obligations,
                                    policy=policy, aligned_returns=ar)
    b = kernel.solve_minimum_repair(proposal=proposal, obligations=obligations,
                                    policy=policy, aligned_returns=ar,
                                    binding_risk_contribution_limit=None,
                                    max_risk_repair_rounds=None)
    assert cr.stable_hash(a) == cr.stable_hash(b)
    for key in ("binding_risk_contribution_limit", "risk_repair_round_budget",
                "risk_repair_convergence", "risk_repair_converged",
                "risk_repair_rounds_measured", "binding_limit_basis"):
        assert key not in a, key

    kw = dict(proposal=proposal, hoc_assessment=_hoc6(), outcome_evidence={},
              aligned_returns=ar, read_state="READY", identity={})
    assert cr.stable_hash(kernel.build_review(**kw)) == cr.stable_hash(
        kernel.build_review(**kw, binding_risk_contribution_limit=None))


def test_21_a_held_cap_is_published_beside_the_cap_it_replaced(tmp_path):
    proposal, policy, obligations, ar = _kernel_inputs(tmp_path)
    held = kernel.held_book_risk_state(proposal)["limit"]
    got = kernel.solve_minimum_repair(
        proposal=proposal, obligations=obligations, policy=policy,
        aligned_returns=ar, binding_risk_contribution_limit=held,
        max_risk_repair_rounds=kernel.RULED_REPAIR_MAX_ROUNDS)
    assert got["binding_risk_contribution_limit"] == pytest.approx(held)
    assert got["binding_limit_basis"] == kernel.RULED_LIMIT_BASIS
    assert got["binding_limit_is_a_new_threshold"] is False
    block = (got["risk_repair_rounds"] or [{}])[0].get("limit") or {}
    assert block["limit"] == pytest.approx(held)
    assert block["held_constant"] is True
    # The governed 3/N figure for the SAME universe travels with it, so a reader can
    # always see both the cap that binds and the cap that would have.
    assert block["governed_limit_at_this_universe"] is not None
    assert block["owner"] == "engine.holding_opportunity_cost"


def test_22_the_round_budget_is_an_iteration_budget_not_a_threshold(tmp_path):
    """Against the moving 3/N cap the repair converges in one round because exiting
    names RAISES the cap. Against a held cap it cannot: reducing a name shrinks the
    invested base, which raises every remaining name's share. A budget of one round
    therefore stops mid-descent - and reports the open breach rather than a clean
    book."""
    proposal, policy, obligations, ar = _kernel_inputs(tmp_path)
    held = kernel.held_book_risk_state(proposal)["limit"]
    stopped = kernel.solve_minimum_repair(
        proposal=proposal, obligations=obligations, policy=policy,
        aligned_returns=ar, binding_risk_contribution_limit=held,
        max_risk_repair_rounds=0)
    assert stopped["risk_repair_converged"] is False
    assert stopped["risk_repair_convergence"] == kernel.REPAIR_NOT_CONVERGED
    assert stopped["risk_contribution_breaches_remaining"], (
        "an exhausted budget names the open breach")
    full = kernel.solve_minimum_repair(
        proposal=proposal, obligations=obligations, policy=policy,
        aligned_returns=ar, binding_risk_contribution_limit=held,
        max_risk_repair_rounds=kernel.RULED_REPAIR_MAX_ROUNDS)
    assert full["risk_repair_converged"] is True
    assert full["risk_contribution_breaches_remaining"] == []
    # More rounds can only ever make the book MORE compliant, never less.
    assert sum(full["weights"].values()) <= sum(stopped["weights"].values()) + 1e-12


def test_23_the_solve_reduces_the_breaching_name(tmp_path):
    """Acceptance criteria 7 and 8. The obligation is closed by a REDUCTION, which is
    precisely what the ruling demands, and the weights are the solved ones."""
    proposal, policy, obligations, ar = _kernel_inputs(tmp_path)
    held = kernel.held_book_risk_state(proposal)["limit"]
    breached = [b["ticker"]
                for b in kernel.held_book_risk_state(proposal)["breaches"]]
    assert breached, "the hermetic world must raise a breach to rule on"
    base = kernel.solve_minimum_repair(proposal=proposal, obligations=obligations,
                                       policy=policy, aligned_returns=ar)
    ruled = kernel.solve_minimum_repair(
        proposal=proposal, obligations=obligations, policy=policy,
        aligned_returns=ar, binding_risk_contribution_limit=held,
        max_risk_repair_rounds=kernel.RULED_REPAIR_MAX_ROUNDS)
    for tk in breached:
        assert ruled["weights"].get(tk, 0.0) < base["weights"].get(tk, 0.0), tk
    assert any(a.get("constraint") == "RISK_CONTRIBUTION_CAP"
               and a.get("action") == "REDUCE_TO_LIMIT"
               for a in ruled["adjustments"])
    # Every released dollar goes to cash. It is not redistributed - that would make
    # the repair a second optimiser.
    assert {a["released_to"] for a in ruled["adjustments"]} == {"CASH"}
    assert ruled["is_an_optimiser"] is False
    assert ruled["adds_positions"] is False and ruled["increases_positions"] is False


# =========================================================================== #
# 4. THE SUCCESSOR TARGET - solved, remeasured, published, fail-closed
# =========================================================================== #
def test_30_an_authoritative_judge_ruling_publishes_a_solved_successor(tmp_path):
    """Acceptance criteria 7-9, on the seam the screen actually reads."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    s = env["policy_compliant_successor"]
    assert s is not None
    assert s["state"] == apdr.SUCCESSOR_SOLVED
    assert s["available"] is True
    assert s["solved_by"] == kernel.CALCULATION_OWNER
    assert s["risk_measured_by"] == "engine.reallocation_proposal.portfolio_volatility"
    assert s["verified_by"] == "engine.constrained_reallocation.verify_feasibility"
    assert s["uses_first_order_indicative_weights"] is False
    assert s["weights_are_solved_and_remeasured"] is True
    assert s["second_optimiser"] is False and s["second_risk_engine"] is False
    assert s["is_an_approval"] is False and s["creates_order_plan"] is False

    st = s["revised_statistics"]
    # Step 9's list, in full. Every one of them present and measured.
    for key in ("positions", "weights", "changes", "one_way_turnover",
                "estimated_cost", "cash_weight", "invested_weight",
                "portfolio_volatility", "portfolio_volatility_capital_basis",
                "concentration", "largest_position", "risk_contributions",
                "risk_contribution_limit", "binding_cap_breaches",
                "mandatory_obligations_remaining"):
        assert st.get(key) is not None, key
    assert st["binding_cap_breaches"] == 0
    assert st["mandatory_obligations_remaining"] == 0
    assert st["constraint_status_valid"] is True
    assert st["risk_measurement_state"] == kernel.RISK_MEASURED
    assert st["risk_repair_convergence"] == kernel.REPAIR_CONVERGED
    assert st["risk_contribution_limit"]["held_constant"] is True
    # NO name breaches the cap the ruling made binding.
    cap = float(st["risk_contribution_limit"]["limit"])
    assert max(st["risk_contributions"].values()) <= cap + 1e-9


def test_31_the_successor_reduces_the_names_the_ruling_reopened(tmp_path):
    """Acceptance criterion 7, per name, and the collateral moves disclosed with it:
    reducing one name raises every other name's share, so the solve may have to trim
    a name that was never in breach. That is published, not hidden."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    s = env["policy_compliant_successor"]
    reopened = set(env["risk_policy_ruling"][
        "instruments_in_breach_of_the_binding_limit"])
    assert reopened
    rows = {c["ticker"]: c for c in s["comparison"]}
    assert reopened <= set(rows)
    for tk in reopened:
        c = rows[tk]
        assert c["reopened_by_the_ruling"] is True
        assert c["weight_solved"] < c["weight_selected"], tk
        assert c["risk_contribution_solved"] < c["risk_contribution_selected"], tk
        assert c["complies_at_the_binding_limit"] is True, tk
        assert c["obligation"] == "CLOSED_BY_REDUCTION"
    for c in s["comparison"]:
        if c["ticker"] not in reopened:
            assert c["reopened_by_the_ruling"] is False
            assert c["obligation"] == "NOT_REOPENED_BY_THE_RULING"


def test_32_the_indicative_weights_are_never_the_solved_weights(tmp_path):
    """Acceptance criterion 8, stated as a number. R69.5's
    ``indicative_weight_at_reference`` scales ONE name in isolation; the solve moves
    the whole book and re-measures, so the two must not agree."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    impl = env["selection"]["selected_target_implementation"]
    ref = (impl["risk_contribution"] or {})["reference_compliance"]
    indicative = {b["ticker"]: b["indicative_weight_at_reference"]
                  for b in ref["breaches"]}
    solved = env["policy_compliant_successor"]["revised_statistics"]["weights"]
    assert indicative
    differs = [tk for tk, w in indicative.items()
               if w is not None and solved.get(tk) is not None
               and abs(float(w) - float(solved[tk])) > 1e-9]
    assert differs, ("the solved book must not merely replay the first-order "
                     "figures: %s" % indicative)


def test_33_accept_as_is_builds_no_successor(tmp_path):
    """Problem 4: do not create a new target merely for the sake of creating one."""
    art, ddir, sel, _ = _ruled_world(tmp_path, ruling=ACCEPT)
    env = _env6(art, ddir)
    assert env["policy_compliant_successor"] is None
    assert SUCCESSOR not in env["selected_targets"]
    assert SUCCESSOR not in [o["target"] for o in env["target_selection"]["options"]]
    ruling = env["risk_policy_ruling"]
    assert ruling["authoritative"] is True
    assert ruling["state"] == stgt.RULED_GOVERNED_STANDS
    assert ruling["reopened_obligation_count"] == 0


def test_34_an_unverified_ruling_builds_no_successor(tmp_path):
    art, ddir, sel, _ = _ruled_world(tmp_path, verified=False)
    env = _env6(art, ddir)
    assert env["policy_compliant_successor"] is None
    assert SUCCESSOR not in env["selected_targets"]


def test_35_an_infeasible_repair_fails_closed(tmp_path, monkeypatch):
    """Acceptance step 10. If no reachable book complies, the open breach is named
    and NO target is fabricated."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    monkeypatch.setattr(kernel, "RULED_REPAIR_MAX_ROUNDS", 0)
    env = _env6(art, ddir)
    s = env["policy_compliant_successor"]
    assert s["state"] == apdr.SUCCESSOR_INFEASIBLE
    assert s["available"] is False
    assert s["implementation"] is None
    assert s["implementation_hash"] is None
    assert s["revised_statistics"]["binding_cap_breaches"] > 0
    assert "No book reachable" in s["detail"]
    # It is NOT offered for selection, and it is not in the implementable map.
    assert SUCCESSOR not in env["selected_targets"]
    assert SUCCESSOR not in [o["target"] for o in env["target_selection"]["options"]]


def test_36_the_ordinary_review_is_untouched_by_the_successor(tmp_path):
    """The successor is published BESIDE the review. Folding it in would change the
    identity of the review every governed selection binds."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    before = _env6(art, ddir)
    _rule(sel, ddir)
    after = _env6(art, ddir)
    assert after["review_hash"] == before["review_hash"]
    assert after["review"] == before["review"]
    assert after["selection"]["selected_target_implementation"] == before[
        "selection"]["selected_target_implementation"]
    assert "policy_compliant_successor" not in after["review"]
    # The ORIGINAL three targets are unchanged too.
    for t in ("CURRENT", "MINIMUM_REPAIR", "FULL_TARGET"):
        assert after["selected_targets"][t] == before["selected_targets"][t]


def test_37_the_frozen_proposal_and_selection_are_immutable(tmp_path):
    """Acceptance criterion 16."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    art_json = json.dumps(art, sort_keys=True)
    sels = (Path(ddir) / "target_selections.json").read_bytes()
    _rule(sel, ddir)
    _env6(art, ddir)
    assert json.dumps(art, sort_keys=True) == art_json
    assert (Path(ddir) / "target_selections.json").read_bytes() == sels


# =========================================================================== #
# 5. THE WORKFLOW - review, select, approve, all through existing gates
# =========================================================================== #
def test_40_the_refusal_names_the_successor_instead_of_a_requirement(tmp_path):
    art, ddir, sel, _ = _ruled_world(tmp_path)
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["status"] == pdec.PDS_REFERENCE_LIMIT_BREACH_RULED
    assert got["recorded"] is False
    assert got["next_required_action"] == (
        "REVIEW_AND_SELECT_THE_POLICY_COMPLIANT_SUCCESSOR_TARGET")
    assert got["policy_compliant_successor_target"] == SUCCESSOR
    assert SUCCESSOR in got["message"]
    assert got["declared_policy_changed"] is False
    assert got["exception_granted"] is False


def test_41_the_successor_is_offered_through_the_existing_selection_lane(tmp_path):
    """Acceptance criterion 10. No second selection writer, no second order-plan
    owner: one more entry in the option list the governed write gate already reads."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    assert SUCCESSOR in pdec.TARGET_VOCAB
    opt = {o["target"]: o for o in env["target_selection"]["options"]}[SUCCESSOR]
    assert opt["selectable"] is True
    assert opt["blockers"] == []
    assert opt["mandatory_obligations_remaining"] == 0
    assert opt["recommended"] is False, "the backend identifies, never pre-selects"
    assert env["target_selection"]["policy_compliant_successor_offered"] is True
    assert env["selected_targets"][SUCCESSOR]["implementable"] is True
    assert SUCCESSOR in env["implementable_targets"]


def test_42_selecting_the_successor_freezes_the_solved_book(tmp_path):
    art, ddir, sel, ruling = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    s = env["policy_compliant_successor"]
    got = pdec.record_target_selection(
        target=SUCCESSOR, confirm=pdec.SELECTION_CONFIRM_TOKEN,
        review_envelope=env, decision_dir=ddir, latest_session=SESSION,
        expected_review_hash=env["review_hash"], actor="test-operator")
    assert got["selected"] is True
    rec = got["record"]
    assert rec["selected_target"] == SUCCESSOR
    assert rec["selected_target_implementation_hash"] == s["implementation_hash"]
    impl = rec["selected_target_implementation"]
    assert impl["weights"] == s["revised_statistics"]["weights"]
    assert impl["implementable"] is True
    # The frozen book names the ruling it was solved under, so the reason it exists
    # travels with it for the life of the record.
    assert impl["derived_under"]["ruling_id"] == ruling["ruling_id"]
    assert impl["derived_under"]["binding_limit"] == s["derived_under"][
        "binding_limit"]
    # History preserved: the superseded selection is still on disk.
    rows = json.loads((Path(ddir) / "target_selections.json").read_text(
        encoding="utf-8"))
    assert len(rows) == 2
    assert rows[1]["supersedes_selection_id"] == sel["selection_id"]


def test_43_the_successor_selection_reaches_the_ordinary_approval_gate(tmp_path):
    """Acceptance criteria 9 and 10. Approval was unavailable while an obligation was
    open; it becomes available on a book that closes them - through the SAME gate,
    with its own confirmation, and never automatically."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    blocked = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert blocked["recorded"] is False

    new = pdec.record_target_selection(
        target=SUCCESSOR, confirm=pdec.SELECTION_CONFIRM_TOKEN,
        review_envelope=env, decision_dir=ddir, latest_session=SESSION,
        actor="test-operator")["record"]
    # The solved book raises no policy question of its own and carries no ruling,
    # because a ruling is about ONE book.
    assert pdec.selection_policy_review(new)["required"] is False
    assert pdec.risk_policy_ruling_state(
        selection=new, decision_dir=ddir)["approval_blocked_by_the_ruling"] is False
    # Approval still needs its own token. Nothing is automatic.
    refused = _approve6(ddir, art, confirm="NOT_THE_TOKEN",
                        expected_selection_id=new["selection_id"])
    assert refused["recorded"] is False
    ok = _approve6(ddir, art, expected_selection_id=new["selection_id"])
    assert ok["recorded"] is True, ok.get("message")
    assert ok["record"]["selected_target"] == SUCCESSOR
    assert ok["created_orders"] is False and ok["changed_holdings"] is False


def test_44_an_order_plan_for_the_successor_uses_the_frozen_book(tmp_path):
    """No new order-plan owner: the existing resolver already builds any non-full
    target from the book frozen with the selection."""
    from paper_trader.api import rebalance_execution as rbx
    art, ddir, sel, _ = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    new = pdec.record_target_selection(
        target=SUCCESSOR, confirm=pdec.SELECTION_CONFIRM_TOKEN,
        review_envelope=env, decision_dir=ddir, latest_session=SESSION,
        actor="test-operator")["record"]
    got = rbx.resolve_target(artifact=art, selection=new,
                             decision_record={"selected_target": SUCCESSOR,
                                              "selection_id": new["selection_id"]})
    assert got["implementable"] is True
    assert got["target"] == SUCCESSOR
    assert got["allocations"] == new["selected_target_implementation"]["allocations"]
    assert got["frozen_position_count"] == new[
        "selected_target_implementation"]["position_count"]


def test_45_reject_and_hold_stay_available_throughout(tmp_path):
    art, ddir, sel, _ = _ruled_world(tmp_path)
    for decision in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
        got = _approve6(ddir, art, decision=decision,
                        expected_selection_id=sel["selection_id"])
        assert got["recorded"] is True, decision


def test_46_a_ruling_cannot_be_reused_against_a_different_book(tmp_path):
    """Acceptance criteria 12 and 13."""
    art, ddir, sel, _ = _ruled_world(tmp_path)
    other = _select6("MINIMUM_REPAIR", ddir, art)["record"]
    assert other["selected_target_implementation_hash"] != sel[
        "selected_target_implementation_hash"]
    state = pdec.risk_policy_ruling_state(selection=other, decision_dir=ddir)
    assert state["ruled"] is False and state["state"] == stgt.RULED_UNRULED
    # And a submission naming another book is refused outright.
    for over, status in (
            ({"expected_selected_target_implementation_hash": "another_book"},
             pdec.RULING_WRONG_BOOK),
            ({"expected_selection_id": "psel_elsewhere"},
             pdec.RULING_WRONG_SELECTION),
            ({"expected_reference_limit": 0.011}, pdec.RULING_WRONG_LIMIT)):
        assert _rule(other, ddir, **over)["status"] == status


def test_47_nothing_in_this_lane_executes(tmp_path):
    """Acceptance criterion 17."""
    art, ddir, sel, out = _ruled_world(tmp_path)
    env = _env6(art, ddir)
    for flag in ("is_an_approval", "approves_proposal", "creates_order_plan",
                 "creates_orders", "creates_fills", "executes", "deploys_capital",
                 "mutates_proposal", "mutates_selection",
                 "changes_declared_policy", "grants_standing_exception",
                 "creates_absolute_companion_floor", "unblocks_approval"):
        assert out[flag] is False, flag
    s = env["policy_compliant_successor"]
    assert s["approves_nothing"] is True
    assert s["manual_approval_still_required"] is True
    assert env["manual_approval_required"] is True


# =========================================================================== #
# 6. THE UI CONTRACT - a real write control, and no forbidden construct
# =========================================================================== #
UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def _ui() -> str:
    return UI.read_text(encoding="utf-8", errors="replace")


def _rp_region(src: str) -> str:
    """The region ``scripts/audit_architecture.py`` guards."""
    start = src.find("function loadReallocationProposal")
    end = src.find("window.renderReallocationProposal")
    assert start != -1 and end > start
    return src[start:end]


def test_50_the_decision_step_exists_and_carries_a_real_write_control():
    """Acceptance criterion 1, and criterion 20's correction: for the UNRULED state,
    ``writeControlsInDecisionStep == 0`` is a FAILURE, not a success."""
    src = _ui()
    assert "function _pdrRiskPolicyDecision(" in src
    assert "function _pdrRecordRuling(" in src
    assert "_pdrRiskPolicyDecision(d)" in src, "wired into the panel render"
    # The step block itself: radios, a confirmation and a submit button.
    block = src[src.find("function _pdrRiskPolicyDecision("):
                src.find("function _pdrRuleReset(")]
    assert 'type="radio"' in block and 'name="pdr-rpd-choice"' in block
    assert 'type="checkbox"' in block and 'id="pdr-rpd-confirm"' in block
    assert 'id="pdr-rpd-go"' in block and "_pdrRecordRuling()" in block
    assert "RECORD RISK POLICY RULING" in block, "the button is never blank"
    # Inert until the operator has both chosen and confirmed.
    assert "function _pdrRuleArm(" in src
    assert "btn.disabled = !(picked && conf && conf.checked)" in src
    assert " disabled onclick=\"_pdrRecordRuling()\"" in block


def test_51_the_options_are_read_from_the_backend_not_held_in_the_browser():
    """Acceptance criterion 3. The browser must hold NO copy of the vocabulary: an
    option list in JavaScript is a rule nobody can test and nobody can audit."""
    region = _rp_region(_ui())
    block = region[region.find("function _pdrRiskPolicyDecision("):
                   region.find("function _pdrRuleReset(")]
    assert "dec.options" in block or "(dec.options || [])" in block
    assert "o.available !== true" in block, "availability is the backend's"
    # No ruling token is spelled as a literal anywhere in the decision step.
    for token in stgt.RULING_VOCAB:
        assert token not in block, token
    assert "UNAVAILABLE IN THIS RELEASE" in block


def test_52_the_unavailable_option_cannot_be_chosen_or_sent():
    """Acceptance criterion 4, in the browser as well as at the write gate."""
    src = _ui()
    block = src[src.find("function _pdrRiskPolicyDecision("):
                src.find("function _pdrRuleReset(")]
    assert "(off ? ' disabled' : ' onchange=\"_pdrRuleArm()\"')" in block
    send = src[src.find("function _pdrRecordRuling("):
               src.find("window._pdrRecordRuling = _pdrRecordRuling;")]
    assert "opt.available !== true" in send, "it refuses to even ask"
    assert "RULING_NOT_AVAILABLE" in send


def test_53_the_submission_binds_the_identity_and_the_tokens():
    """Acceptance criteria 5 and 6 at the browser seam."""
    src = _ui()
    send = src[src.find("function _pdrRecordRuling("):
               src.find("window._pdrRecordRuling = _pdrRecordRuling;")]
    for field in ("expected_selection_id",
                  "expected_selected_target_implementation_hash",
                  "expected_reference_limit", "instruments", "submission_token",
                  "operator_confirmed", "confirmation"):
        assert field in send, field
    # R82.1.1 - and the evidence that actually makes it stand: the screen opens the
    # backend's confirmation ceremony and SPENDS what it issues. The token and the
    # asserted confirmation above are claims that travel beside it, not instead.
    assert "confirmation_route" in send
    assert "operator_confirmation: cj.confirmation" in send
    assert "dec.confirm_token" in send and "CONFIRM_RISK_POLICY_RULING" in send
    assert "'X-API-Key': key()" in send, "R69.1's 401 defect must not return"
    assert "_pdrRuleSetUi({ phase: 'PENDING'" in send
    assert "return _pdrLoad();" in send


def test_54_the_successor_step_exists_and_offers_the_ordinary_selection():
    """Acceptance criterion 10: the panel EXPOSES the ordinary controls and invokes
    neither."""
    src = _ui()
    assert "function _pdrSuccessorTarget(" in src
    assert "_pdrSuccessorTarget(d)" in src
    block = src[src.find("function _pdrSuccessorTarget("):
                src.find("function _pdrSelClear(")]
    assert "policy_compliant_successor" in block
    assert "revised_statistics" in block
    assert "SELECT THIS TARGET" in block
    assert "_pdrSelect(" in block, "the EXISTING governed selection write"
    assert "_pdrApprove(" not in block, "the panel must not invoke an approval"
    assert "opt && opt.selectable" in block, "selectability stays the backend's"
    # Fail-closed rendering names the state rather than an empty block.
    assert "NO COMPLIANT TARGET" in block
    assert "SOLVED_AND_COMPLIANT_AT_THE_RULED_CAP" in block


def test_55_the_new_blocks_carry_the_mandatory_safety_badges():
    src = _ui()
    dec = src[src.find("function _pdrRiskPolicyDecision("):
              src.find("function _pdrRuleReset(")]
    suc = src[src.find("function _pdrSuccessorTarget("):
              src.find("function _pdrSelClear(")]
    for badge in ("NO ORDERS", "MANUAL REVIEW"):
        assert badge in dec, badge
        assert badge in suc, badge
    assert "APPROVAL WITHHELD" in dec
    assert "CREATES TRADE DECISIONS ONLY" in dec
    assert "PREVIEW ONLY" in suc
    assert "NOT APPROVED" in suc


def test_56_an_unverified_ruling_is_not_painted_as_a_governing_one():
    """The ruled statement block must render only for an AUTHORITATIVE ruling, or an
    operator would be told a development artifact had governed their book."""
    src = _ui()
    assert "if (ruling && ruling.authoritative === true && ruling.state" in src
    assert "if (!r || r.authoritative !== true) return null;" in src
    prior = src[src.find("function _pdrRulePrior("):
                src.find("function _pdrRiskPolicyDecision(")]
    assert "UNVERIFIED" in prior and "AUDIT EVIDENCE ONLY" in prior
    assert "BINDS NOTHING" in prior


def test_57_the_region_adds_no_forbidden_construct():
    """Acceptance criterion 18, at the exact boundary the audit enforces."""
    region = _rp_region(_ui())
    for pat in ("new Date(", "Date.now(", ".getTime(", ".reduce(", "Math.",
                "cost_rate", "COST_BPS", "compute"):
        assert pat not in region, pat
    for banned in ("alert(", "confirm("):
        assert banned not in region, banned


def test_58_the_wireframe_was_produced_before_the_code():
    doc = (Path(__file__).resolve().parents[1] / "docs"
           / "R82_1_RISK_POLICY_DECISION_WIREFRAME.md")
    assert doc.exists(), "CLAUDE.md requires a wireframe before UI work"
    text = doc.read_text(encoding="utf-8")
    for section in ("## 1. SCAN", "## 2. REVIEW", "## 3. PLAN",
                    "## 4. Acceptance criteria"):
        assert section in text, section
    assert "1920x1080" in text
    assert "RISK POLICY DECISION" in text
    assert "POLICY-COMPLIANT SUCCESSOR" in text.upper()
    # The critique that justified the work is recorded, not implied.
    assert re.search(r"writeControls\w* = 0", text)
