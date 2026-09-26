"""tests/test_r74_withheld_terminal_contract.py — R74: the two September-24 defects.

Deterministic, offline coverage of:

  DEFECT 1 — ``api.daily_research_cycle`` could not express a governed reallocation
  WITHHELD as a valid terminal outcome. WITHHELD is a COMPLETED, fail-closed verdict of
  ``api.reallocation_proposal``: the complete target was built, re-optimised under the
  breached limit and still breaches it, so it is never persisted and never approvable.
  The cycle recorded the step FAILED and then demanded that the operator's proposal
  review read back a proposal the owner had correctly refused to write; the review's
  correct NO_PROPOSAL answer downgraded the whole run to INCONSISTENT.

  DEFECT 2 — the durable run manifest AND the run index said INCONSISTENT for
  2026-09-24 while the status endpoint answered NOT_STARTED with a null run id and
  ``executable = True``, inviting a fresh full-universe cycle over research already on
  disk. The same run was erased a second way by the NEXT session's owned-data gate.

Every read model / write boundary is injected; NO network, provider, prediction, real
cycle, Daily Close, operational-ledger write, order / fill, NAV write or model
promotion occurs. Manifests are written under a per-test ``tmp_path``, never a
production root.
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import data_freshness as df
from paper_trader.api import proposal_decision_review as pdr
from paper_trader.api import reallocation_proposal as rp
from paper_trader.engine import reallocation_proposal as rpk

# Reuse the verified deterministic Slice-3 harness (``tests`` is a package).
from tests.test_slice3_daily_research_cycle import (  # noqa: E402
    Fakes, _freshness, _inputs, _run, _status)

ROOT = Path(__file__).resolve().parent.parent
ROUTE = "/v1/operations/proposal-decision-review"

# The live 2026-09-24 shape: the complete target still gives AMD and DDOG more than the
# governed per-name risk-contribution cap after re-optimisation, so the feasible set is
# empty and the owner fails closed.
WITHHELD_HASH = "withheld_target_hash_r74"
_RC_BREACHES = [{"ticker": "AMD", "risk_contribution_pct": 0.1412},
                {"ticker": "DDOG", "risk_contribution_pct": 0.1188}]


# --------------------------------------------------------------------------- #
# Seams.
# --------------------------------------------------------------------------- #
def _withheld_proposal(*, hash_=WITHHELD_HASH, rc_breaches=None):
    """The reallocation owner's governed WITHHELD result, persisted-refused."""
    breaches = _RC_BREACHES if rc_breaches is None else rc_breaches
    return {
        "proposal": {
            "proposal_state": rpk.STATE_WITHHELD,
            "proposal_hash": hash_,
            "eligible_market_date": "2026-08-05",
            "action_counts": {"RETAIN": 7, "INCREASE": 7, "REDUCE": 0, "EXIT": 11,
                              "ADD": 6, "REPLACE_OUT": 0, "REPLACE_IN": 0},
            "portfolio": {"proposed_holding_count": 20},
            "signal": {"score_improvement": 0.083569,
                       "score_improvement_net_of_cost": 0.066069},
            "turnover": {"one_way_turnover": 0.35,
                         "estimated_transaction_cost": 86.09},
            "approvable": False,
            "withheld_reasons": [{
                "code": rpk.CT_RISK_CONTRIBUTION, "object": "COMPLETE_TARGET",
                "value": [b["ticker"] for b in breaches], "limit": 0.12,
                "detail": "The complete target gives 2 name(s) more than the "
                          "governed per-name risk-contribution limit."}],
            "complete_target_limits": {
                "withheld": True,
                "withheld_codes": [rpk.CT_RISK_CONTRIBUTION],
                "risk_contribution_breaches": breaches,
                "breaches": [{"code": rpk.CT_RISK_CONTRIBUTION}],
                "all_ok": False},
            "data_gaps": []},
        # The authoritative owner refuses to persist a withheld proposal.
        "persistence": {"status": "NOT_PERSISTED",
                        "reason": "STATE_WITHHELD_NOT_PERSISTABLE",
                        "proposal_id": None, "persisted": False,
                        "reused": False, "superseded": False},
    }


def _withheld_fn(counter=None, **kw):
    def _fn(*, scoring=None, hoc_assessment=None, reallocation_dir=None, hoc_dir=None,
            hoc_binding=None):
        if counter is not None:
            counter["reallocation"] = counter.get("reallocation", 0) + 1
        return _withheld_proposal(**kw)
    return _fn


def _withheld_run(tmp, counter=None, **kw):
    return _run(tmp, reallocation_proposal_fn=_withheld_fn(counter), **kw)


def _manifest(tmp, run_id):
    return drc._load_run(run_id, str(tmp))


def _persist_state(tmp, run_id, state):
    """Rewrite a REAL manifest's durable state, exactly as the live store held it."""
    rec = _manifest(tmp, run_id)
    rec["state"] = state
    drc._save_run(rec, str(tmp))
    drc._update_index(eligible_date=rec["eligible_market_date"],
                      idempotency_key=rec["idempotency_key"],
                      input_contract_hash=rec["input_contract_hash"],
                      run_id=run_id, state=state, drc_dir=str(tmp))
    return rec


# =========================================================================== #
# 1. THE AUTHORITATIVE STATES THIS CONTRACT IS BUILT ON
#
#    The literals are mirrored so the module stays import-pure. If an owner ever
#    renames one, these pins fail here rather than silently in a live cycle.
# =========================================================================== #
def test_01_withheld_state_literal_matches_both_authoritative_owners():
    assert drc.REALLOC_STATE_WITHHELD == rp.STATE_WITHHELD == rpk.STATE_WITHHELD


def test_02_persistable_states_match_the_owners_approvable_set():
    assert tuple(drc.REALLOC_PERSISTABLE_STATES) == tuple(rp.APPROVABLE_READ_STATES)
    assert tuple(drc.REALLOC_PERSISTABLE_STATES) == tuple(rpk.APPROVABLE_STATES)


def test_03_review_absence_literals_match_the_review_owner():
    assert drc.REVIEW_STATUS_NO_PROPOSAL == pdr.STATUS_NO_PROPOSAL
    assert drc.REVIEW_STATE_PROPOSAL_ABSENT == pdr.REVIEW_STATE_NO_PROPOSAL


def test_04_a_withheld_proposal_is_never_persistable_by_its_owner():
    """The premise of the whole fix, asserted against the owner rather than assumed."""
    out = rp.persist_proposal(result={"proposal_state": rpk.STATE_WITHHELD},
                              input_contract={}, reallocation_dir=None)
    assert out["persisted"] is False and out["proposal_id"] is None
    assert out["status"] == "NOT_PERSISTED"
    assert rpk.STATE_WITHHELD not in rp.APPROVABLE_READ_STATES


# =========================================================================== #
# 2. DEFECT 1 — A GOVERNED WITHHELD IS A VALID TERMINAL OUTCOME
# =========================================================================== #
def test_05_governed_withheld_reaches_a_terminal_complete_run(tmp_path):
    r, _ = _withheld_run(tmp_path)
    assert r["state"] == drc.COMPLETE, r.get("warnings")
    assert r["terminal"] is True
    assert r["reallocation_proposal_state"] == drc.REALLOC_STATE_WITHHELD
    assert r["reallocation_proposal_withheld"] is True
    assert r["reallocation_governed_outcome_complete"] is True


def test_06_the_withheld_step_is_ok_not_failed(tmp_path):
    """The engine COMPLETED. Recording it FAILED was the first half of defect 1."""
    r, _ = _withheld_run(tmp_path)
    step = next(s for s in r["step_results"]
                if s["step_id"] == drc.STEP_BUILD_REALLOCATION)
    assert step["status"] == drc.S_OK
    assert step["error_code"] is None and step["error_detail"] is None
    assert "WITHHELD" in step["reason"]
    assert r["failed_step"] is None
    assert not any("did not complete" in w for w in r["warnings"])


def test_07_withheld_is_never_approved_selected_or_given_an_id(tmp_path):
    r, _ = _withheld_run(tmp_path)
    assert r["reallocation_proposal_id"] is None
    assert r["reallocation_proposal_selected"] is False
    assert r["reallocation_proposal_approvable"] is False
    assert r["reallocation_proposal"]["persistence_status"] == "NOT_PERSISTED"
    # The hash names the target that was JUDGED; it is not an artifact reference.
    assert r["reallocation_proposal_hash"] == WITHHELD_HASH


def test_08_the_withheld_outcome_states_an_outstanding_governance_requirement(tmp_path):
    r, _ = _withheld_run(tmp_path)
    req = r["reallocation_outstanding_governance_requirement"]
    assert req and "UNRESOLVED" in req
    assert "AMD" in req and "DDOG" in req
    assert any("WITHHELD" in w and "outstanding governance" in w for w in r["warnings"])


def test_09_hard_risk_exceptions_remain_visible_on_the_durable_manifest(tmp_path):
    """AMD/DDOG survive the round trip to disk — not only the in-memory response."""
    r, _ = _withheld_run(tmp_path)
    back = _manifest(tmp_path, r["run_id"])
    assert back["reallocation_withheld_breaching_tickers"] == ["AMD", "DDOG"]
    assert back["reallocation_withheld_codes"] == [rpk.CT_RISK_CONTRIBUTION]
    per_name = back["reallocation_withheld_risk_contribution_breaches"]
    assert [b["ticker"] for b in per_name] == ["AMD", "DDOG"]
    assert all(b["risk_contribution_pct"] for b in per_name)


def test_10_withheld_reasons_keep_the_shape_every_consumer_already_reads(tmp_path):
    """``api.portfolio_decision`` joins this list into prose; a dict list would break it.

    It is also what api.workflow_state and api.daily_action_gate forward, and what
    api.reallocation_proposal's own summary publishes.
    """
    r, _ = _withheld_run(tmp_path)
    reasons = r["reallocation_withheld_reasons"]
    assert reasons == [rpk.CT_RISK_CONTRIBUTION]
    assert all(isinstance(x, str) for x in reasons)
    assert ", ".join(reasons) == rpk.CT_RISK_CONTRIBUTION
    # The structured detail travels beside it, under its own name.
    assert r["reallocation_withheld_reason_detail"][0]["code"] == rpk.CT_RISK_CONTRIBUTION


def test_11_a_ready_proposal_is_unaffected_by_the_withheld_path(tmp_path):
    r, _ = _run(tmp_path)  # the harness default is a READY, persisted proposal
    assert r["state"] == drc.COMPLETE
    assert r["reallocation_proposal_state"] == "READY"
    assert r["reallocation_proposal_withheld"] is False
    assert r["reallocation_proposal_id"] == "realloc_stub"
    assert r["reallocation_proposal_selected"] is True
    assert r["reallocation_proposal_approvable"] is True
    assert r["reallocation_outstanding_governance_requirement"] is None


# =========================================================================== #
# 3. DEFECT 1 — THE TERMINAL VALIDATOR, PER GOVERNED OUTCOME
# =========================================================================== #
def _rec(state, **kw):
    base = {
        "run_id": "r", "idempotency_key": "k", "session_contract_hash": "s",
        "active_book_id": "b", "eligible_market_date": "2026-09-24",
        "started_at": "t0", "completed_at": "t1", "state": drc.COMPLETE,
        "input_contract_hash": "i", "completed_steps": ["X"],
        "step_results": [{"step_id": s, "status": drc.S_SKIPPED}
                         for s in drc.STEP_SEQUENCE],
        "prospective_tournament_state": "T",
        "reallocation_proposal_state": state,
    }
    base["step_results"] = [
        ({"step_id": s, "status": drc.S_OK}
         if s == drc.STEP_BUILD_REALLOCATION else {"step_id": s, "status": drc.S_SKIPPED})
        for s in drc.STEP_SEQUENCE]
    base.update(kw)
    return base


def test_12_withheld_requires_the_target_hash_it_judged():
    problems = drc._validate_terminal_manifest(
        _rec(drc.REALLOC_STATE_WITHHELD, reallocation_proposal_hash=None))
    assert any("WITHHELD but proposal hash missing" in p for p in problems)


def test_13_withheld_with_its_hash_is_a_valid_terminal_manifest():
    problems = drc._validate_terminal_manifest(
        _rec(drc.REALLOC_STATE_WITHHELD, reallocation_proposal_hash="h"))
    assert problems == []


def test_14_an_invented_proposal_id_on_a_withheld_run_is_refused():
    """"Do not invent a proposal ID." A withheld verdict has none, by contract."""
    problems = drc._validate_terminal_manifest(
        _rec(drc.REALLOC_STATE_WITHHELD, reallocation_proposal_hash="h",
             reallocation_proposal_id="realloc_fabricated"))
    assert any("WITHHELD but a proposal id is recorded" in p for p in problems)


def test_15_a_withheld_run_marked_selected_is_refused():
    problems = drc._validate_terminal_manifest(
        _rec(drc.REALLOC_STATE_WITHHELD, reallocation_proposal_hash="h",
             reallocation_proposal_selected=True))
    assert any("never approvable" in p for p in problems)


def test_16_a_proposal_genuinely_missing_when_required_is_still_refused():
    """The original lie-detector is intact: READY/DEGRADED must carry id AND hash."""
    for state in drc.REALLOC_PERSISTABLE_STATES:
        problems = drc._validate_terminal_manifest(
            _rec(state, reallocation_proposal_hash="h", reallocation_proposal_id=None))
        assert any("proposal reference missing" in p for p in problems), state
        problems = drc._validate_terminal_manifest(
            _rec(state, reallocation_proposal_hash=None, reallocation_proposal_id="p"))
        assert any("proposal reference missing" in p for p in problems), state
        assert drc._validate_terminal_manifest(
            _rec(state, reallocation_proposal_hash="h",
                 reallocation_proposal_id="p")) == []


# =========================================================================== #
# 4. DEFECT 1 — THE OPERATOR-READINESS ACCEPTANCE, INVERTED FOR WITHHELD
# =========================================================================== #
def test_17_withheld_plus_no_proposal_is_the_correct_answer(tmp_path):
    """THE 2026-09-24 SHAPE. The review said NO_PROPOSAL and was RIGHT."""
    review = {"status": pdr.STATUS_NO_PROPOSAL, "review": None,
              "review_state": pdr.REVIEW_STATE_NO_PROPOSAL, "proposal_hash": None,
              "message": "There is no reallocation proposal to review.",
              "route": ROUTE, "owner": "api.proposal_decision_review"}
    out = drc.verify_proposal_review_readable(
        expected_proposal_hash=WITHHELD_HASH,
        reallocation_state=drc.REALLOC_STATE_WITHHELD, loader=lambda: review)
    assert out["outcome"] == drc.REVIEW_WITHHELD_COHERENT
    # Only ``False`` raises a blocker; there is no persisted proposal to read back.
    assert out["readable"] is not False
    assert out["governed_reallocation_withheld"] is True
    assert out["withheld_target_hash"] == WITHHELD_HASH


def test_18_a_withheld_session_that_still_offers_a_proposal_is_refused():
    """The failure the inversion EXPOSES, which nothing could previously detect."""
    review = {"status": pdr.STATUS_OK, "review": {"r": 1},
              "review_state": "COMPLETE_CURRENT", "proposal_hash": "some_stale_hash",
              "route": ROUTE}
    out = drc.verify_proposal_review_readable(
        expected_proposal_hash=WITHHELD_HASH,
        reallocation_state=drc.REALLOC_STATE_WITHHELD, loader=lambda: review)
    assert out["readable"] is False
    assert out["outcome"] == drc.REVIEW_WITHHELD_CONTRADICTED
    assert "never leave an approvable proposal standing" in out["detail"]


def test_19_a_real_proposal_still_needs_a_matching_review_hash():
    """R69 unchanged for a PERSISTED proposal: identity is still proven."""
    ok = drc.verify_proposal_review_readable(
        expected_proposal_hash="HASH_A", reallocation_state="READY",
        loader=lambda: {"status": "OK", "review": {"r": 1}, "proposal_hash": "HASH_A",
                        "review_hash": "RH", "route": ROUTE})
    assert ok["readable"] is True and ok["outcome"] == drc.REVIEW_VERIFIED
    assert ok["proposal_hash_matched"] is True

    bad = drc.verify_proposal_review_readable(
        expected_proposal_hash="HASH_A", reallocation_state="READY",
        loader=lambda: {"status": "OK", "review": {"r": 1}, "proposal_hash": "HASH_B"})
    assert bad["readable"] is False and bad["proposal_hash_matched"] is False


def test_20_the_r69_defect_shape_is_still_caught():
    """A persisted proposal whose review will not load remains INCONSISTENT."""
    bad = drc.verify_proposal_review_readable(
        expected_proposal_hash="HASH_A", reallocation_state="READY",
        loader=lambda: {"status": "UNAVAILABLE", "review": None,
                        "message": "the read model did not answer"})
    assert bad["readable"] is False and bad["review_status"] == "UNAVAILABLE"


def test_21_the_acceptance_still_never_crashes_the_run_it_verifies():
    def boom():
        raise RuntimeError("store offline")

    for state in (None, "READY", drc.REALLOC_STATE_WITHHELD):
        out = drc.verify_proposal_review_readable(
            expected_proposal_hash="HASH_A", reallocation_state=state, loader=boom)
        assert out["readable"] is False and out["outcome"] == "REVIEW_RAISED"


def test_22_the_withheld_state_is_handed_to_the_acceptance_check():
    with io.open(ROOT / "api" / "daily_research_cycle.py",
                 encoding="utf-8", newline="") as fh:
        src = fh.read()
    assert "reallocation_state=rec.get(\"reallocation_proposal_state\")" in src
    assert "REVIEW_WITHHELD_CONTRADICTED" in src


# =========================================================================== #
# 5. DEFECT 2 — A PERSISTED TERMINAL RUN IS NEVER ERASED
# =========================================================================== #
def test_23_persisted_inconsistent_is_read_back_not_replaced_by_not_started(tmp_path):
    """THE REPORTED DEFECT: index + manifest INCONSISTENT, status NOT_STARTED."""
    r, _ = _withheld_run(tmp_path)
    original = r["run_id"]
    _persist_state(tmp_path, original, drc.INCONSISTENT)

    s = _status(tmp_path)
    assert s["state"] == drc.INCONSISTENT
    assert s["state"] != drc.NOT_STARTED
    assert s["run_id"] == original
    assert s["terminal"] is True
    assert s["reflected_persisted_terminal_run"] is True
    assert s["recovery_run_id"] == original
    assert s["recovery_available"] is True


def test_24_the_reflected_run_keeps_every_immutable_artifact_reference(tmp_path):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    s = _status(tmp_path)
    assert s["opportunity_cost_artifact_id"] == r["opportunity_cost_artifact_id"]
    assert s["opportunity_cost_assessment_hash"] == r["opportunity_cost_assessment_hash"]
    assert s["portfolio_reassessment_id"] == r["portfolio_reassessment_id"]
    assert s["research_agent_id"] == r["research_agent_id"]
    assert s["completed_steps"] == r["completed_steps"]


def test_25_the_action_is_a_recovery_of_that_run_never_a_new_cycle(tmp_path):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    s = _status(tmp_path)
    action = s["required_actions"][0]
    assert action["gate"] == "daily_research_cycle"
    assert action["recovery"] is True
    assert action["run_id"] == r["run_id"]
    assert r["run_id"] in action["action"]
    assert "REUSED, not recomputed" in action["action"]
    assert action["confirmation_required"] == drc.EXECUTE_CONFIRMATION


@pytest.mark.parametrize("state", [drc.INCONSISTENT, drc.BLOCKED, drc.FAILED])
def test_26_every_unfinished_terminal_state_is_reflected(tmp_path, state):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], state)
    s = _status(tmp_path)
    assert s["state"] == state and s["run_id"] == r["run_id"]


def test_27_a_drifted_input_contract_hash_no_longer_erases_the_run(tmp_path):
    """The cycle refreshes the very inputs that hash is derived from, so it drifts."""
    r, _ = _withheld_run(tmp_path)
    rec = _manifest(tmp_path, r["run_id"])
    rec["state"] = drc.INCONSISTENT
    rec["input_contract_hash"] = "a_hash_the_refresh_has_since_moved"
    drc._save_run(rec, str(tmp_path))
    drc._update_index(eligible_date=rec["eligible_market_date"],
                      idempotency_key=rec["idempotency_key"],
                      input_contract_hash=rec["input_contract_hash"],
                      run_id=r["run_id"], state=drc.INCONSISTENT, drc_dir=str(tmp_path))

    s = _status(tmp_path)
    assert s["state"] == drc.INCONSISTENT and s["run_id"] == r["run_id"]
    assert s["prior_run_identity"]["same_contract"] is False
    assert s["prior_run_identity"]["same_session"] is True
    assert any("not an identity" in w for w in s["warnings"])


def test_28_a_completed_run_is_still_reused_verbatim_not_recovered(tmp_path):
    r, _ = _withheld_run(tmp_path)
    s = _status(tmp_path)
    assert s["state"] == drc.COMPLETE
    assert s["reused_existing_run"] is True
    assert s["executable"] is False
    assert not s.get("reflected_persisted_terminal_run")


def test_29_an_unfinished_run_is_not_reported_as_reusable_research(tmp_path):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    s = _status(tmp_path)
    assert s["reused_existing_run"] is False
    assert s["resumed_existing_run"] is False
    assert any("NOT finished" in w for w in s["warnings"])


def test_30_inconsistent_inputs_still_outrank_a_reflected_run(tmp_path):
    """``pre == INCONSISTENT`` is a claim about the INPUTS and keeps its precedence.

    It says the authoritative surfaces disagree, which a finished-or-not run does not
    answer, so the reflection must not speak over it.
    """
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    fr = _freshness()
    fr["consistency_status"] = df.INCONSISTENT
    s = _status(tmp_path, freshness=fr)
    assert s["state"] == drc.INCONSISTENT
    assert any(b["code"] == "CROSS_SURFACE_INCONSISTENCY" for b in s["blockers"])
    assert not s.get("reflected_persisted_terminal_run")


def test_31_recoverable_prior_run_refuses_a_foreign_contract(tmp_path):
    facts = {"input_contract_hash": "i", "session_contract_hash": "s"}
    assert drc.recoverable_prior_run(None, facts) is False
    assert drc.recoverable_prior_run({"state": drc.COMPLETE, "run_id": "r"}, facts) is False
    assert drc.recoverable_prior_run(
        {"state": drc.INCONSISTENT, "run_id": "r",
         "input_contract_hash": "other", "session_contract_hash": "other"}, facts) is False
    assert drc.recoverable_prior_run(
        {"state": drc.INCONSISTENT, "run_id": "r",
         "input_contract_hash": "i", "session_contract_hash": "other"}, facts) is True


# =========================================================================== #
# 6. DEFECT 2 — IDEMPOTENT RECOVERY OF THE ORIGINAL RUN
# =========================================================================== #
def test_32_recovery_returns_the_original_run_id(tmp_path):
    r, _ = _withheld_run(tmp_path)
    original = r["run_id"]
    _persist_state(tmp_path, original, drc.INCONSISTENT)

    again, _ = _withheld_run(tmp_path)
    assert again["run_id"] == original
    assert again["resumed_existing_run"] is True
    assert again["state"] == drc.COMPLETE


def test_33_recovery_never_recomputes_the_full_universe(tmp_path):
    """"without another expensive full-universe computation"."""
    r, fakes = _withheld_run(tmp_path)
    assert fakes.calls["score"] == 1
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)

    fresh = Fakes()
    again, _ = _run(tmp_path, fresh,
                    reallocation_proposal_fn=_withheld_fn())
    assert again["run_id"] == r["run_id"]
    assert fresh.calls["score"] == 0, "the universe was re-scored on recovery"
    assert fresh.calls["target"] == 0
    step = next(s for s in again["step_results"]
                if s["step_id"] == drc.STEP_SCORE_UNIVERSE)
    assert step["status"] in (drc.S_OK, drc.S_REUSED)
    assert again["scored_count"] == r["scored_count"]


def test_34_recovery_creates_no_retrospective_true_forward_evidence(tmp_path):
    """The forward-evidence capture is REUSED. A recovery can never mint a snapshot."""
    r, fakes = _withheld_run(tmp_path)
    assert fakes.calls["evidence"] == 1
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)

    fresh = Fakes()
    again, _ = _run(tmp_path, fresh, reallocation_proposal_fn=_withheld_fn())
    assert fresh.calls["evidence"] == 0, "recovery captured forward evidence again"
    # The SAME immutable bundle is carried forward, not a second one for the session.
    assert again["snapshot_bundle_id"] == r["snapshot_bundle_id"]
    assert again["captured_snapshot_count"] == r["captured_snapshot_count"]
    assert again["evidence_status"] == r["evidence_status"]
    step = next(s for s in again["step_results"]
                if s["step_id"] == drc.STEP_CAPTURE_EVIDENCE)
    assert step["status"] in (drc.S_OK, drc.S_REUSED)


def test_35_recovery_duplicates_no_downstream_artifact(tmp_path):
    """No duplicate opportunity-cost, reassessment, proposal or research artifact."""
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)

    fresh = Fakes()
    again, _ = _run(tmp_path, fresh, reallocation_proposal_fn=_withheld_fn())
    assert fresh.calls.get("holding_opp", 0) == 0
    assert fresh.calls.get("reassessment", 0) == 0
    assert fresh.calls.get("research_agent", 0) == 0
    assert again["opportunity_cost_artifact_id"] == r["opportunity_cost_artifact_id"]
    assert again["portfolio_reassessment_id"] == r["portfolio_reassessment_id"]
    assert again["research_agent_id"] == r["research_agent_id"]
    # ...and a withheld proposal is still not an artifact.
    assert again["reallocation_proposal_id"] is None


def test_36_recovery_leaves_exactly_one_manifest_and_one_index_row(tmp_path):
    r, _ = _withheld_run(tmp_path)
    eligible = r["eligible_market_date"]
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    _withheld_run(tmp_path)
    _withheld_run(tmp_path)

    manifests = sorted(p.name for p in (tmp_path / "runs").glob("*.json"))
    assert manifests == ["%s.json" % r["run_id"]], manifests
    idx = drc._load_index(str(tmp_path))
    assert list(idx) == [eligible]
    assert idx[eligible]["run_id"] == r["run_id"]
    assert idx[eligible]["state"] == drc.COMPLETE


def test_37_recovery_is_idempotent_after_it_completes(tmp_path):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    first, _ = _withheld_run(tmp_path)
    assert first["state"] == drc.COMPLETE

    fresh = Fakes()
    second, _ = _run(tmp_path, fresh, reallocation_proposal_fn=_withheld_fn())
    assert second["run_id"] == r["run_id"]
    assert second["reused_existing_run"] is True
    assert fresh.calls["score"] == 0 and fresh.calls["evidence"] == 0


def test_38_recovery_writes_no_lock_behind_it(tmp_path):
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    _withheld_run(tmp_path)
    assert drc._read_lock(str(tmp_path)) is None


# =========================================================================== #
# 7. THE RECOVERY WINDOW — A LATER SESSION'S GATE
# =========================================================================== #
def test_39_a_later_sessions_data_gate_no_longer_hides_the_run(tmp_path):
    """R55.2.1's erasure, one state further along. The later gate is still reported."""
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)

    # A LATER session has closed without publishing its owned data; the ELIGIBLE
    # session's own data is confirmed, so the gate says nothing about this run.
    s = _status(tmp_path, reference_today="2026-08-06")
    assert s["state"] == drc.INCONSISTENT
    assert s["run_id"] == r["run_id"]
    assert s["pending_session_gate"]["gate"] == "market_session"
    assert any("says nothing about the unfinished run" in w for w in s["warnings"])


def test_40_the_eligible_sessions_own_missing_data_still_waits(tmp_path):
    """Case (a): no eligible session at all, so there is nothing to reflect."""
    s = _status(tmp_path, operational={"desk": None, "nav": None}, inputs=_inputs())
    assert s["state"] == drc.WAITING_FOR_OWNED_DATA
    assert s["executable"] is False
    assert not s.get("reflected_persisted_terminal_run")


def test_41_only_a_recovery_is_exempt_from_the_later_session_gate(tmp_path):
    """A brand-new cycle still waits; there is no run to finish."""
    out, _ = _withheld_run(tmp_path, reference_today="2026-08-06")
    assert out["state"] == drc.WAITING_FOR_OWNED_DATA
    assert out["executable"] is False
    assert not (tmp_path / "runs").exists() or not list((tmp_path / "runs").glob("*.json"))


def test_42_a_recovery_proceeds_through_the_later_session_gate(tmp_path):
    r, _ = _withheld_run(tmp_path)
    original = r["run_id"]
    _persist_state(tmp_path, original, drc.INCONSISTENT)

    fresh = Fakes()
    again, _ = _run(tmp_path, fresh, reference_today="2026-08-06",
                    reallocation_proposal_fn=_withheld_fn())
    assert again["run_id"] == original
    assert again["state"] == drc.COMPLETE
    assert fresh.calls["score"] == 0 and fresh.calls["evidence"] == 0
    assert any("Recovering Daily Research Cycle run" in w for w in again["warnings"])


def test_43_an_unclosed_session_still_refuses_a_recovery(tmp_path):
    """Only the later-session DATA gate is lifted, and only for a recovery."""
    r, _ = _withheld_run(tmp_path)
    _persist_state(tmp_path, r["run_id"], drc.INCONSISTENT)
    out, _ = _withheld_run(tmp_path, operational={"desk": None, "nav": None},
                           inputs=_inputs())
    assert out["state"] in (drc.WAITING_FOR_OWNED_DATA, drc.WAITING_FOR_SESSION_CLOSE)
    assert out["executable"] is False


# =========================================================================== #
# 8. SAFETY — nothing here approves, orders, automates or promotes
# =========================================================================== #
def test_44_a_withheld_run_keeps_every_safety_boundary(tmp_path):
    r, _ = _withheld_run(tmp_path)
    assert r["target_operationally_approved"] is False
    assert r["reallocation_proposal_approvable"] is False
    assert "PREVIEW ONLY" in drc.SAFETY_BADGES
    assert "NO LIVE ORDERS" in drc.SAFETY_BADGES
    assert "AUTOMATION OFF" in drc.SAFETY_BADGES
    assert "MANUAL REVIEW" in drc.SAFETY_BADGES


def test_45_the_module_declares_no_order_or_automation_path():
    with io.open(ROOT / "api" / "daily_research_cycle.py",
                 encoding="utf-8", newline="") as fh:
        src = fh.read()
    for forbidden in ("create_order", "submit_order", "place_order", "execute_order"):
        assert forbidden not in src, forbidden
