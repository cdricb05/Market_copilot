r"""R82 - the governed RULING on the moving denominator.

R69.1 raised the question. R69.2 made the effect visible. R69.5 made it answerable
and withheld approval until an operator ruled - and then offered exactly ONE
recordable outcome: a per-request acknowledgement that, once accepted, let the
approval through. Two things followed from that.

  * Only ACCEPT_AS_IS was reachable. ``JUDGE_AGAINST_THE_BEFORE_UNIVERSE`` - the
    ruling that says a risk-contribution breach must be repaired by reducing the
    breaching name, not by shrinking the universe around it - had no recordable
    form, because the only door available unblocks the very approval it refuses.
  * No ruling was DURABLE. The ack lived inside the approve request, so a book
    nobody approved carried no ruling at all.

This suite proves the ruling is now recordable, durable, bound to ONE frozen book,
and that with ``JUDGE_AGAINST_THE_BEFORE_UNIVERSE`` on record a breach that existed
under the frozen pre-repair covariance universe CANNOT disappear because the target
holds fewer names. It also proves the ruling can only ever make approval less
available: no new path to approval is created anywhere.

Every world is hermetic. No live endpoint, store, ledger, holding, cash or NAV is
touched, no threshold is edited, and nothing here approves, orders or executes.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as apdr
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import holding_opportunity_cost as hoc
from paper_trader.engine import selected_target as stgt

from tests.test_r69_5_risk_policy_gate import (
    SESSION, _approve6, _env6, _ruling, _select6, _world6,
)

JUDGE = stgt.RULING_JUDGE_AGAINST_THE_BEFORE_UNIVERSE
ACCEPT = stgt.RULING_ACCEPT_AS_IS
FLOOR = stgt.RULING_ADD_AN_ABSOLUTE_COMPANION_FLOOR


# =========================================================================== #
# 1. THE KERNEL - what a ruling MEANS over one frozen book
# =========================================================================== #
#: The 2026-09-25 minimum repair, in the shape the frozen selection publishes it.
#: Every number is the live artifact's own (proposal eaee484fa4a0).
LIVE_2026_09_25 = {
    "before": {"limit": 0.12, "n_covariance_names": 25, "breach_count": 2,
               "breached_instruments": ["AMD", "DDOG"],
               "contributions": {"AMD": 0.150732, "DDOG": 0.152802}},
    "after": {"limit": 0.21428571, "n_covariance_names": 14, "breach_count": 0,
              "breached_instruments": [],
              "contributions": {"AMD": 0.203344, "DDOG": 0.214065}},
    "limit_changed": True, "limit_relaxed": True,
    "reference_compliance": {
        "state": "AVAILABLE", "reference_limit": 0.12,
        "governed_limit": 0.21428571, "complies_with_reference": False,
        "breach_count": 2, "breached_instruments": ["AMD", "DDOG"],
        "opened_against_reference": [],
        "basis": stgt.REFERENCE_LIMIT_BASIS,
        "breaches": [
            {"ticker": "AMD", "code": "CARRIED_FROM_THE_BEFORE_BOOK",
             "risk_contribution_before": 0.150732, "risk_contribution": 0.203344,
             "reference_limit": 0.12, "governed_limit": 0.21428571,
             "excess_over_reference": 0.083344, "compliant_on_governed_limit": True,
             "weight_before": 0.051069, "weight_after": 0.051069,
             "weight_change": 0.0, "weight_moved": False,
             "indicative_weight_at_reference": 0.0301375,
             "indicative_basis": "FIRST_ORDER_PROPORTIONAL_SCALING_OF_THIS_NAME_ONLY"
                                 "_NOT_A_REOPTIMISATION"},
            {"ticker": "DDOG", "code": "CARRIED_FROM_THE_BEFORE_BOOK",
             "risk_contribution_before": 0.152802, "risk_contribution": 0.214065,
             "reference_limit": 0.12, "governed_limit": 0.21428571,
             "excess_over_reference": 0.094065, "compliant_on_governed_limit": True,
             "weight_before": 0.043427, "weight_after": 0.04237491,
             "weight_change": -0.00105209, "weight_moved": True,
             "indicative_weight_at_reference": 0.02375442,
             "indicative_basis": "FIRST_ORDER_PROPORTIONAL_SCALING_OF_THIS_NAME_ONLY"
                                 "_NOT_A_REOPTIMISATION"},
        ]},
}


def test_01_the_vocabulary_declares_three_courses_and_offers_two():
    """R69.1 put three courses to the operator. Two are recordable here, and the
    third is REFUSED by name rather than silently missing."""
    assert stgt.RULING_VOCAB == (ACCEPT, JUDGE, FLOOR)
    assert stgt.RULING_AVAILABLE == (ACCEPT, JUDGE)
    assert stgt.RULING_NOT_AVAILABLE_THIS_RELEASE == (FLOOR,)
    # Every course R69.5 printed on the screen is in the vocabulary, so the panel
    # cannot offer a ruling the recorder has never heard of.
    for option in stgt.POLICY_REVIEW_OPTIONS:
        assert option.split(" - ")[0].strip() in stgt.RULING_VOCAB


def test_02_an_absolute_floor_is_refused_and_invents_no_threshold():
    """The instruction R82 was given: do NOT invent a companion threshold. The
    ruling is declared so it cannot be coined elsewhere, and refused here."""
    eff = stgt.ruling_effect(FLOOR)
    assert eff["known"] is True and eff["available"] is False
    assert eff["reason"] == "RULING_NOT_AVAILABLE_IN_THIS_RELEASE"
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=FLOOR)
    assert out["applied"] is False
    assert out["absolute_companion_floor_created"] is False
    assert out["new_threshold_invented"] is False
    assert out["approval_blocked_by_this_ruling"] is False


def test_03_an_unknown_ruling_is_refused():
    eff = stgt.ruling_effect("JUDGE_AGAINST_SOMETHING_ELSE")
    assert eff["known"] is False and eff["available"] is False
    assert eff["reason"] == "RULING_NOT_IN_VOCABULARY"
    assert stgt.ruling_effect(None)["known"] is False


def test_04_the_ruling_binds_the_before_cap_on_the_live_shape():
    """THE case. On the 2026-09-25 minimum repair the ruling makes 12.00% binding,
    and both names the shrinking universe discharged are OPEN again."""
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=JUDGE)
    assert out["state"] == stgt.RULED_REFERENCE_BINDS
    assert out["binds"] == stgt.BINDS_REFERENCE_LIMIT
    assert out["binding_limit"] == 0.12
    assert out["governed_limit"] == 0.21428571
    assert out["reopened_obligation_count"] == 2
    assert out["instruments_in_breach_of_the_binding_limit"] == ["AMD", "DDOG"]
    assert out["complies_with_the_binding_limit"] is False
    assert out["approval_blocked_by_this_ruling"] is True
    # The frozen book's own verdict is preserved beside the ruled one, never
    # overwritten by it: both numbers stay legible.
    assert out["obligations_open_on_the_governed_limit"] == 0
    assert out["obligations_open_after_ruling"] == 2


def test_05_a_pre_existing_breach_cannot_die_of_a_smaller_universe():
    """The invariant this release exists for, stated as an invariant.

    AMD's weight never moves and its share of portfolio risk RISES by 5.3 points;
    DDOG is trimmed by 0.11 points of NAV and its share rises by 6.1. Under the
    target's own cap both are compliant. Under the ruling neither is, and the reason
    is named for each."""
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=JUDGE)
    by = {t["ticker"]: t for t in out["treatment"]}
    assert set(by) == {"AMD", "DDOG"}

    assert by["AMD"]["weight_before"] == by["AMD"]["weight_after"] == 0.051069
    assert by["AMD"]["weight_moved"] is False
    assert by["AMD"]["risk_contribution_before"] == 0.150732
    assert by["AMD"]["risk_contribution_after"] == 0.203344

    assert by["DDOG"]["weight_before"] == 0.043427
    assert by["DDOG"]["weight_after"] == 0.04237491
    assert by["DDOG"]["weight_moved"] is True
    assert by["DDOG"]["risk_contribution_after"] == 0.214065

    for tk in ("AMD", "DDOG"):
        # Compliant on the cap that moved; in breach on the cap the breach was
        # raised against. Both facts on one row, for both names.
        assert by[tk]["compliant_on_governed_limit"] is True
        assert by[tk]["compliant_on_binding_limit"] is False
        assert by[tk]["obligation_open"] is True
        assert by[tk]["code"] == stgt.TREATMENT_OBLIGATION_REOPENED
        assert by[tk]["required_action"] == hoc.REQUIRED_ACTION_REDUCE
        # Neither name is the target's own creation - the denominator argument is
        # the whole of what discharged them.
        assert by[tk]["opened_by_this_target"] is False


def test_06_the_reopened_obligations_use_canonical_vocabulary_only():
    """A ruled obligation is an ordinary mandatory repair, spelled in the owners'
    own tokens. No private vocabulary is forked for it."""
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=JUDGE)
    for o in out["reopened_obligations"]:
        assert o["tier"] == hoc.OBLIGATION_TIER_HARD
        assert o["reason_code"] == hoc.OBLIGATION_REASON_MANDATORY
        assert o["constraint_code"] == cr.C_RISK_CONTRIBUTION
        assert o["required_action"] in hoc.REQUIRED_ACTION_VOCAB
        assert o["source_owner"] == hoc.CALCULATION_OWNER
        # The indicative reduction is labelled indicative and NOT solved.
        assert o["solved"] is False
        assert "NOT_A_REOPTIMISATION" in o["max_valid_weight_basis"]
        assert o["max_valid_weight"] < o["current_weight"]


def test_07_accept_as_is_leaves_the_governed_cap_binding_and_opens_nothing():
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=ACCEPT)
    assert out["state"] == stgt.RULED_GOVERNED_STANDS
    assert out["binds"] == stgt.BINDS_GOVERNED_LIMIT
    assert out["binding_limit"] == 0.21428571
    assert out["reopened_obligation_count"] == 0
    assert out["approval_blocked_by_this_ruling"] is False
    # And it still does not GRANT anything - the R69.5 gate is untouched by it.
    assert out["unblocks_approval"] is False


def test_08_a_ruling_changes_no_threshold_and_grants_no_exception():
    for ruling in (ACCEPT, JUDGE):
        out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25,
                                       ruling=ruling)
        assert out["declared_policy_changed"] is False
        assert out["standing_exception_granted"] is False
        assert out["absolute_companion_floor_created"] is False
        assert out["new_threshold_invented"] is False
        assert out["unblocks_approval"] is False
        assert out["target_approved_here"] is False
        assert out["creates_order_plan"] is False
        assert out["creates_orders"] is False
        assert out["measured_here"] is False
        assert out["measured_by"] == hoc.CALCULATION_OWNER
        assert out["binding_limit_is_a_new_threshold"] is False


def test_09_the_binding_limit_is_a_limit_the_owner_already_published():
    """Neither cap is invented: 0.12 is 3/25 and 0.21428571 is 3/14, both from the
    canonical limit function on the two covariance universes."""
    assert hoc.risk_contribution_limit(n_covariance_names=25)["limit"] == 0.12
    assert hoc.risk_contribution_limit(n_covariance_names=14)["limit"] == 0.21428571
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=JUDGE)
    assert out["binding_limit"] == hoc.risk_contribution_limit(
        n_covariance_names=25)["limit"]


def test_10_the_breaches_come_from_the_canonical_breach_function():
    """The ruled obligations are exactly what the owner's own breach function says
    about the target's contributions at the ruled cap. Nothing is re-measured."""
    canonical = hoc.risk_contribution_breaches(
        contributions=LIVE_2026_09_25["after"]["contributions"], limit=0.12)
    out = stgt.apply_policy_ruling(risk_contribution=LIVE_2026_09_25, ruling=JUDGE)
    assert [b["ticker"] for b in canonical] == out[
        "instruments_in_breach_of_the_binding_limit"]
    for b, o in zip(canonical, out["reopened_obligations"]):
        assert o["excess_over_binding_limit"] == pytest.approx(b["excess"], abs=1e-9)


def test_11_a_book_with_no_reference_limit_binds_nothing_and_says_so():
    """Fail-closed in BOTH directions: an unestablishable ruled cap must not fall
    back to the relaxed cap, which would read as permission."""
    rc = {"before": {"limit": None}, "after": {"limit": 0.2},
          "reference_compliance": {"state": stgt.REFERENCE_UNAVAILABLE,
                                   "reference_limit": None}}
    out = stgt.apply_policy_ruling(risk_contribution=rc, ruling=JUDGE)
    assert out["applied"] is False
    assert out["binding_limit"] is None
    assert out["complies_with_the_binding_limit"] is None
    assert out["approval_blocked_by_this_ruling"] is False
    assert out["reason"] == stgt.REFERENCE_UNAVAILABLE


def test_12_a_ruled_target_that_actually_complies_opens_no_obligation():
    """The counter-case. A repair that genuinely cut the name satisfies the ruled
    cap too, so the ruling costs it nothing."""
    rc = {
        "before": {"limit": 0.12, "n_covariance_names": 25},
        "after": {"limit": 0.15, "n_covariance_names": 20, "breach_count": 0},
        "reference_compliance": {"state": "AVAILABLE", "reference_limit": 0.12,
                                 "governed_limit": 0.15,
                                 "complies_with_reference": True, "breaches": [],
                                 "breach_count": 0, "breached_instruments": []}}
    out = stgt.apply_policy_ruling(risk_contribution=rc, ruling=JUDGE)
    assert out["state"] == stgt.RULED_REFERENCE_SATISFIED
    assert out["reopened_obligation_count"] == 0
    assert out["complies_with_the_binding_limit"] is True
    assert out["approval_blocked_by_this_ruling"] is False


# =========================================================================== #
# 2. THE GOVERNED WRITER - durable, bound, idempotent, and not an approval
# =========================================================================== #
def _rulings(ddir: Path) -> list:
    p = Path(ddir) / "risk_policy_rulings.json"
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else []


def _book_token(sel, review=None):
    """The book token a governed READ publishes, re-derived exactly as it mints it."""
    review = review if review is not None else pdec.selection_policy_review(sel)
    return pdec.ruling_submission_token(
        selection_id=sel.get("selection_id"),
        selected_target_implementation_hash=review["implementation_hash"],
        reference_limit=review["reference_limit"])


def _ceremony(sel, ruling=JUDGE, review=None, token=None, **over):
    """ACT ONE of the R82.1.1 ceremony, performed exactly as the screen performs it.

    Returns the whole issuance so a test can inspect the refusal as well as the
    confirmation. Nothing durable is written by it.
    """
    review = review if review is not None else pdec.selection_policy_review(sel)
    kw = dict(ruling=ruling, confirm=pdec.RULING_CONFIRM_TOKEN,
              submission_token=(token if token is not None
                                else _book_token(sel, review)),
              selection_id=sel.get("selection_id"),
              selected_target_implementation_hash=review["implementation_hash"],
              reference_limit=review["reference_limit"],
              surface="hermetic-test", actor="test-operator")
    kw.update(over)
    return pdec.open_ruling_confirmation(**kw)


def _rule(sel, ddir, ruling=JUDGE, ceremony=True, **over):
    """A ruling with VERIFIED operator provenance - the ordinary path.

    R82.1.1: the verified path performs the REAL ceremony. A fresh single-use
    confirmation is issued by the backend for this exact ruling on this exact frozen
    book and is spent by the write, because that - not a boolean this helper could
    set - is what makes a ruling authoritative. ``actor`` is never "operator": a
    machine-generated decision does not get to narrate itself as one.
    """
    review = pdec.selection_policy_review(sel)
    token = _book_token(sel, review)
    confirmation = (_ceremony(sel, ruling=ruling, review=review,
                              token=token).get("confirmation")
                    if ceremony else None)
    kw = dict(ruling=ruling, confirm=pdec.RULING_CONFIRM_TOKEN, selection=sel,
              expected_selection_id=sel.get("selection_id"),
              expected_selected_target_implementation_hash=review[
                  "implementation_hash"],
              expected_reference_limit=review["reference_limit"],
              instruments=list(review["instruments"]),
              submission_token=token,
              operator_confirmation=confirmation,
              confirmed_in_ui=True, surface="hermetic-test",
              actor="test-operator", decision_dir=ddir)
    kw.update(over)
    return pdec.record_risk_policy_ruling(**kw)


def _rule_unverified(sel, ddir, ruling=JUDGE, **over):
    """A ruling submitted WITHOUT the governed ceremony - what every R82 record on
    disk looks like, and what a direct API or script call produces.

    It still asserts ``confirmed_in_ui`` and still presents the served book token,
    because that is exactly the R82.1 bypass: the claim and the token are not the
    evidence, so this must land UNVERIFIED anyway.
    """
    kw = dict(ceremony=False, confirmed_in_ui=True)
    kw.update(over)
    return _rule(sel, ddir, ruling=ruling, **kw)


def test_20_the_ruling_is_recorded_durably_and_bound_to_the_frozen_book(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _rule(sel, ddir)
    assert got["status"] == pdec.RULING_RECORDED
    assert got["recorded"] is True
    rows = _rulings(ddir)
    assert len(rows) == 1
    rec = rows[0]
    assert rec["ruling"] == JUDGE
    assert rec["selection_id"] == sel["selection_id"]
    assert rec["selected_target_implementation_hash"] == sel[
        "selected_target_implementation_hash"]
    assert rec["proposal_hash"] == sel["binding"]["proposal_hash"]
    assert rec["binding_limit"] == rec["reference_limit"]
    # It is a governance artifact and says so on every axis.
    for k in ("is_an_approval", "changes_declared_policy",
              "grants_standing_exception", "creates_absolute_companion_floor",
              "unblocks_approval", "creates_order_plan", "creates_orders"):
        assert rec[k] is False


def test_21_the_ruling_is_readable_back_and_never_crosses_to_another_book(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    binding = sel["binding"]
    got = pdec.load_risk_policy_ruling(
        active_book_id=binding["active_book_id"],
        eligible_market_date=binding["eligible_market_date"],
        selected_target_implementation_hash=sel[
            "selected_target_implementation_hash"], decision_dir=ddir)
    assert got is not None and got["ruling"] == JUDGE
    # A ruling is about ONE book. Asked about a different frozen book it answers
    # None rather than lending its authority to one nobody ruled on.
    assert pdec.load_risk_policy_ruling(
        active_book_id=binding["active_book_id"],
        eligible_market_date=binding["eligible_market_date"],
        selected_target_implementation_hash="A_DIFFERENT_BOOK",
        decision_dir=ddir) is None


def test_22_the_ruling_does_not_move_the_frozen_selection_or_its_hash(tmp_path):
    """Acceptance criterion 6: the frozen book is byte-unchanged by a ruling."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    sel_path = Path(ddir) / "target_selections.json"
    before = sel_path.read_bytes()
    _rule(sel, ddir)
    assert sel_path.read_bytes() == before


@pytest.mark.parametrize("over,status", [
    ({"confirm": "CONFIRM_PORTFOLIO_REBALANCE_DECISION"}, pdec.RULING_NOT_RECORDED),
    ({"confirm": pdec.SELECTION_CONFIRM_TOKEN}, pdec.RULING_NOT_RECORDED),
    ({"ruling": FLOOR}, pdec.RULING_NOT_AVAILABLE),
    ({"ruling": "SOMETHING_ELSE"}, pdec.RULING_UNKNOWN),
    ({"selection": None}, pdec.RULING_NO_SELECTION),
    ({"expected_selection_id": "psel_somewhere_else"}, pdec.RULING_WRONG_SELECTION),
    ({"expected_selected_target_implementation_hash": "deadbeef"},
     pdec.RULING_WRONG_BOOK),
    ({"expected_reference_limit": 0.09}, pdec.RULING_WRONG_LIMIT),
    ({"instruments": ["AMD"]}, pdec.RULING_WRONG_INSTRUMENTS),
])
def test_23_a_ruling_about_something_else_is_refused_and_writes_nothing(
        tmp_path, over, status):
    """Fail closed on every axis. The approval token and the selection token are
    among the refusals: neither may be replayed as a ruling."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _rule(sel, ddir, **over)
    assert got["status"] == status
    assert got["recorded"] is False
    assert _rulings(ddir) == []


def test_24_the_same_ruling_twice_writes_one_artifact(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    assert _rule(sel, ddir)["status"] == pdec.RULING_RECORDED
    again = _rule(sel, ddir)
    assert again["status"] == pdec.RULING_REUSED
    assert again["reused"] is True
    assert len(_rulings(ddir)) == 1


def test_24b_a_verified_ruling_is_never_absorbed_by_an_unverified_one(tmp_path):
    """R82.1. Reuse turns on the ruling AND its authority. If the same token from an
    unverified record could answer a verified submission, the operator's actual
    decision would be met with "already on record" by an artifact that governs
    nothing - and the ruling they made would never reach the store."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    first = _rule_unverified(sel, ddir)
    assert first["status"] == pdec.RULING_RECORDED_UNVERIFIED
    second = _rule(sel, ddir)
    assert second["status"] == pdec.RULING_REVISED
    rows = _rulings(ddir)
    assert len(rows) == 2, "history is preserved, never rewritten"
    assert rows[0]["provenance_verified"] is False
    assert rows[1]["provenance_verified"] is True
    assert rows[1]["supersedes_ruling_id"] == first["ruling_id"]
    # The AUTHORITATIVE record is the one that governs from here on.
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["authoritative"] is True
    assert state["ruling_id"] == rows[1]["ruling_id"]


def test_25_a_changed_ruling_is_an_auditable_revision(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    first = _rule(sel, ddir, ruling=JUDGE)
    second = _rule(sel, ddir, ruling=ACCEPT)
    assert second["status"] == pdec.RULING_REVISED
    rows = _rulings(ddir)
    assert len(rows) == 2
    assert rows[0]["ruling"] == JUDGE and rows[1]["ruling"] == ACCEPT
    # History is preserved, not rewritten.
    assert rows[1]["supersedes_ruling_id"] == first["ruling_id"]
    assert rows[1]["revision"] == 1


def test_26_a_target_that_owes_no_ruling_cannot_be_ruled_on(tmp_path):
    """No ruling exists where no question was raised, so a ruling cannot be
    manufactured for a compliant target and carried anywhere."""
    from tests.test_r69_2_selected_target_lifecycle import _select, _world
    _, _, art, ddir, _ = _world(tmp_path)
    sel = _select("MINIMUM_REPAIR", ddir, art)["record"]
    assert pdec.selection_policy_review(sel)["required"] is False
    got = _rule(sel, ddir)
    assert got["status"] == pdec.RULING_NOT_REQUIRED
    assert _rulings(ddir) == []


# =========================================================================== #
# 3. THE APPROVAL GATE - a ruling can only ever make approval LESS available
# =========================================================================== #
def test_30_a_ruled_reference_breach_refuses_the_approval(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["status"] == pdec.PDS_REFERENCE_LIMIT_BREACH_RULED
    assert got["recorded"] is False
    assert got["risk_policy_ruling"]["ruling"] == JUDGE
    assert got["risk_policy_ruling"]["approval_blocked_by_the_ruling"] is True
    assert got["declared_policy_changed"] is False
    assert got["exception_granted"] is False
    # R82.1 - it used to say SELECT_A_COMPLIANT_TARGET_OR_REJECT while the system
    # held no compliant target to select. The review now solves one and the refusal
    # names it, so the next action is something the operator can actually do.
    assert got["next_required_action"] == (
        "REVIEW_AND_SELECT_THE_POLICY_COMPLIANT_SUCCESSOR_TARGET")
    assert got["policy_compliant_successor_target"] == (
        pdec.TARGET_POLICY_COMPLIANT_REPAIR)


def test_31_an_acknowledgement_cannot_override_a_recorded_ruling(tmp_path):
    """THE load-bearing test. R69.5's ack clears the "nobody ruled" state. It must
    not clear a state whose cause is that somebody DID rule, and ruled against this
    book - or the ruling would be advisory."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    # Without a ruling the very same ack DOES let the approval through (R69.5).
    ok = _approve6(ddir, art, expected_selection_id=sel["selection_id"],
                   risk_policy_acknowledgement=_ruling(sel))
    assert ok["recorded"] is True, ok.get("message")
    assert ok["record"]["decision"] == pdec.DECISION_APPROVE

    # A fresh world, ruled first. The same ack now changes nothing.
    _, art2, ddir2 = _world6(tmp_path / "second")
    sel2 = _select6("FULL_TARGET", ddir2, art2)["record"]
    _rule(sel2, ddir2)
    blocked = _approve6(ddir2, art2, expected_selection_id=sel2["selection_id"],
                        risk_policy_acknowledgement=_ruling(sel2))
    assert blocked["status"] == pdec.PDS_REFERENCE_LIMIT_BREACH_RULED
    assert blocked["recorded"] is False


def test_32_the_refused_approval_writes_no_decision(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert not (Path(ddir) / "decisions.json").exists()
    assert pdec.load_decision_record(
        active_book_id=sel["binding"]["active_book_id"],
        eligible_market_date=SESSION, decision_dir=ddir) is None


def test_33_reject_and_hold_stay_available_under_a_ruling(tmp_path):
    """A target that may not be approved must still be refusable and holdable, or
    the ruling would strand the operator with no governed action at all."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    for decision in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
        got = _approve6(ddir, art, decision=decision,
                        expected_selection_id=sel["selection_id"])
        assert got["recorded"] is True, got.get("message")
        assert got["record"]["decision"] == decision


def test_34_accept_as_is_answers_the_review_and_approves_nothing(tmp_path):
    """R82 refused to let ACCEPT_AS_IS satisfy the R69.5 acknowledgement, on the
    grounds that the release must add no path to approval. That was the right
    instinct applied to the wrong object: it left the operator able to record a
    ruling that answered the question and STILL be told to answer the question.

    R82.1 separates the two claims. A ruling with verified operator provenance IS
    the answer the review was waiting for, so the review stops blocking. It is not
    an approval: the decision still needs its own confirmation token at its own
    gate, and every other gate still runs. What must never happen is a ruling
    granting an approval by itself - pinned below.
    """
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    assert _rule(sel, ddir, ruling=ACCEPT)["status"] == pdec.RULING_RECORDED
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["authoritative"] is True
    assert state["state"] == stgt.RULED_GOVERNED_STANDS
    assert state["satisfies_the_policy_review"] is True
    assert state["approval_blocked_by_the_ruling"] is False
    # The review no longer blocks, and NO acknowledgement was presented.
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["recorded"] is True, got.get("message")
    assert got["record"]["decision"] == pdec.DECISION_APPROVE
    # ...but the ruling itself approved nothing: the decision is a separate record,
    # written only because this call carried the approval token.
    assert state["target_approved_here"] is False
    refused = _approve6(ddir / "x", art, confirm="NOT_THE_APPROVAL_TOKEN",
                        expected_selection_id=sel["selection_id"])
    assert refused["recorded"] is False


def test_34b_an_unverified_accept_as_is_answers_nothing(tmp_path):
    """The other half. A ruling recorded without the governed review's evidence -
    every R82 record on disk, and any direct API call - satisfies nothing. Approval
    stays withheld exactly where it was, which is the only fail-closed answer: an
    unverified record may neither govern nor grant."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _rule_unverified(sel, ddir, ruling=ACCEPT)
    assert got["status"] == pdec.RULING_RECORDED_UNVERIFIED
    assert got["recorded"] is True and got["provenance_verified"] is False
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["ruled"] is True and state["authoritative"] is False
    assert state["satisfies_the_policy_review"] is False
    approve = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert approve["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED
    assert approve["recorded"] is False


def test_35_the_new_state_is_in_the_declared_vocabulary():
    assert pdec.PDS_REFERENCE_LIMIT_BREACH_RULED in pdec.DECISION_STATE_VOCAB
    # And it is NOT an approvable state.
    assert pdec.PDS_REFERENCE_LIMIT_BREACH_RULED not in \
        pdec.APPROVABLE_DECISION_STATES


def test_36_a_ruling_does_not_survive_a_reselected_book(tmp_path):
    """A selection revised underneath a ruling leaves the NEW book unruled. The
    ruling is not silently portable, and the R69.5 gate takes over again."""
    _, art, ddir = _world6(tmp_path)
    full = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(full, ddir)
    repair = _select6("MINIMUM_REPAIR", ddir, art)["record"]
    assert repair["selected_target_implementation_hash"] != full[
        "selected_target_implementation_hash"]
    state = pdec.risk_policy_ruling_state(selection=repair, decision_dir=ddir)
    assert state["ruled"] is False
    assert state["state"] == stgt.RULED_UNRULED
    assert state["approval_blocked_by_the_ruling"] is False


def test_37_an_unruled_book_is_not_read_as_permission(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    state = pdec.risk_policy_ruling_state(selection=sel, decision_dir=ddir)
    assert state["ruled"] is False
    assert state["state"] == stgt.RULED_UNRULED
    assert pdec.PDS_RISK_POLICY_REVIEW_REQUIRED in state["detail"]
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"])
    assert got["status"] == pdec.PDS_RISK_POLICY_REVIEW_REQUIRED


def test_38_a_recorded_decision_names_the_ruling_it_was_taken_under(tmp_path):
    """Approvals become findable by the ruling that was standing when they were
    taken - including "none", which is the honest answer before this lane existed."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    got = _approve6(ddir, art, expected_selection_id=sel["selection_id"],
                    risk_policy_acknowledgement=_ruling(sel))
    rec = got["record"]
    assert rec["risk_policy_ruling_contract"] == \
        "R82_RULING_BOUND_TO_ONE_PROPOSAL_AND_TARGET"
    assert rec["risk_policy_ruling_id"] is None
    assert rec["risk_policy_ruling"]["state"] == stgt.RULED_UNRULED


# =========================================================================== #
# 4. THE READ SEAM - the ruling travels where the screen reads it
# =========================================================================== #
def test_40_the_ruling_rides_the_review_envelope(tmp_path):
    """The panel reads /v1/operations/proposal-decision-review. If the ruling does
    not arrive on THAT envelope it does not exist as far as the operator is
    concerned."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    env = _env6(art, ddir)
    for block in (env["risk_policy_ruling"],
                  env["governance"]["risk_policy_ruling"],
                  env["governance"]["selection"]["risk_policy_ruling"],
                  env["selection"]["risk_policy_ruling"]):
        assert block["ruled"] is True
        assert block["ruling"] == JUDGE
        assert block["binding_limit"] == block["reference_limit"]
        assert block["approval_blocked_by_the_ruling"] is True
        assert [t["ticker"] for t in block["treatment"]] == block["instruments"]
    assert env["risk_policy_ruling_confirm_token"] == pdec.RULING_CONFIRM_TOKEN


def test_41_the_ruling_is_published_beside_the_frozen_book_never_inside_it(tmp_path):
    """The frozen book's identity must not move because a ruling was recorded: the
    hash is bound by the selection record and by every order plan built from it."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    before = _env6(art, ddir)["selection"]["selected_target_implementation"]
    _rule(sel, ddir)
    after_env = _env6(art, ddir)
    after = after_env["selection"]["selected_target_implementation"]
    assert after == before
    assert "risk_policy_ruling" not in after
    assert after_env["selection"]["selected_target_implementation_hash"] == sel[
        "selected_target_implementation_hash"]


def test_42_the_review_hash_does_not_move_when_a_ruling_is_recorded(tmp_path):
    """``review_hash`` is computed over ``review``; a governed selection binds it.
    A ruling is a governance layer and must not change the identity of a review."""
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    before = _env6(art, ddir)["review_hash"]
    _rule(sel, ddir)
    assert _env6(art, ddir)["review_hash"] == before


def test_43_an_unruled_envelope_says_unruled_rather_than_nothing(tmp_path):
    _, art, ddir = _world6(tmp_path)
    _select6("FULL_TARGET", ddir, art)
    env = _env6(art, ddir)
    block = env["risk_policy_ruling"]
    assert block["ruled"] is False
    assert block["state"] == stgt.RULED_UNRULED
    # The recordable courses travel with the question, so the screen never offers a
    # ruling the recorder would refuse.
    assert block["available_rulings"] == list(stgt.RULING_AVAILABLE)
    assert block["unavailable_rulings"] == [FLOOR]


def test_44_the_read_seam_approves_nothing(tmp_path):
    _, art, ddir = _world6(tmp_path)
    sel = _select6("FULL_TARGET", ddir, art)["record"]
    _rule(sel, ddir)
    env = _env6(art, ddir)
    assert env["read_only"] is True and env["writes_nothing"] is True
    block = env["risk_policy_ruling"]
    assert block["target_approved_here"] is False
    assert block["declared_policy_changed"] is False
    assert block["standing_exception_granted"] is False
    assert block["absolute_companion_floor_created"] is False


# =========================================================================== #
# 5. THE UI CONTRACT - stated, and no forbidden construct introduced
# =========================================================================== #
UI = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def test_50_the_ruled_panel_exists_and_reads_the_backend():
    src = UI.read_text(encoding="utf-8", errors="replace")
    assert "function _pdrRuledPolicy(" in src
    # R82.2 added a THIRD argument: the backend approval gate, so the policy block
    # can print the same blocking status the status bar and the Step 2 control print.
    # The first two - the frozen risk block and the ruling - are unchanged.
    assert "_pdrRiskContribution(impl.risk_contribution, sel.risk_policy_ruling," in src
    assert "function _pdrRiskContribution(risk, ruling, gate)" in src
    assert "RISK POLICY RULED" in src
    assert "RULING ON RECORD" in src
    assert "APPROVAL UNAVAILABLE" in src
    # The headline count now comes from the attribution block rather than from the
    # narrower "did the weight move?" list.
    assert "CLOSED_ONLY_BECAUSE_THE_LIMIT_ROSE" in src
    assert "DISCHARGED BY THE CAP MOVING, NOT BY A REDUCTION" in src


def test_51_the_ruled_panel_adds_no_forbidden_construct():
    src = UI.read_text(encoding="utf-8", errors="replace")
    start = src.index("function _pdrRuledPolicy(")
    end = src.index("function _pdrRuledObligations(")
    block = src[start:end]
    for forbidden in ("alert(", "confirm(", "Create Order", "create_order",
                      "automation"):
        assert forbidden not in block
    # Display only: the ruled panel gains no write control of any kind.
    for w in ("fetch(", "XMLHttpRequest", "onclick", "<button", "<form"):
        assert w not in block
    # And no threshold, cap or excess is computed in the browser.
    for arith in ("* 3", "/ 14", "/ 25", "3 /", "3.0 /"):
        assert arith not in block


def test_52_the_safety_badges_the_panel_must_carry_are_present():
    src = UI.read_text(encoding="utf-8", errors="replace")
    block = src[src.index("function _pdrRuledPolicy("):
                src.index("function _pdrRuledObligations(")]
    for badge in ("PREVIEW ONLY", "NO ORDERS", "MANUAL REVIEW",
                  "NO THRESHOLD CHANGED"):
        assert badge in block


def test_53_the_wireframe_was_produced_before_the_code():
    """CLAUDE.md hard-failure condition: no wireframe, no UI work."""
    doc = (Path(__file__).resolve().parents[1] / "docs"
           / "R82_RISK_POLICY_RULING_WIREFRAME.md")
    text = doc.read_text(encoding="utf-8", errors="replace")
    assert "1920x1080" in text
    for section in ("## 1. SCAN", "## 2. REVIEW", "## 3. PLAN",
                    "## 4. Acceptance criteria"):
        assert section in text
    assert JUDGE in text
