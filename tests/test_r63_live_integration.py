"""R63 LIVE INTEGRATION — the mandatory-repair contract binds on every SEAM.

R63 put the reviewability invariant in two places: ``engine.reallocation_proposal``
decides it at construction, and ``engine.proposal_decision_review`` refuses a
non-compliant FULL_TARGET at selection. The live 2026-09-22 operational run showed
that neither of those covered the READ and APPROVE seams:

  * ``api.reallocation_proposal`` dropped ``mandatory_repair`` and
    ``full_target_reviewable`` from its read contract entirely and defaulted
    ``approvable`` to ``True`` (``p.get("approvable", True)``), so an artifact
    that carried no verdict was republished as approvable AND executable;
  * ``api.portfolio_decision.record_decision`` bound the governed selection only
    ``if selection is not None``, so a caller that approved without selecting
    skipped the one gate that would have refused the target.

Together those produced the observed contradiction: the review screen refused the
09:03 FULL_TARGET by name (LH, VLO) while the cockpit beside it rendered the SAME
proposal as approvable, and an approval of it was RECORDED.

The invariant proved here:

    A FULL TARGET MAY NOT BE PUBLISHED AS REVIEWABLE, EXPOSED AS APPROVABLE, OR
    APPROVED WHILE ANY AUTHORITATIVE MANDATORY REPAIR OBLIGATION REMAINS
    UNRESOLVED — OR WHILE THE ARTIFACT CARRIES NO VERDICT ON THEM AT ALL.

"We could not tell" must never read as "yes", so a pre-contract artifact is
refused exactly like an open obligation.

Nothing here approves an operational proposal, selects a target, creates an order
plan, an order or a fill. Every write goes to a pytest ``tmp_path``.
"""
from __future__ import annotations

import copy

import pytest

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import reallocation_proposal as arp
from paper_trader.engine import holding_opportunity_cost as hoc
from paper_trader.engine import proposal_decision_review as pdr
from paper_trader.engine import reallocation_proposal as rpk

SESSION = "2026-09-21"
BOOK = "alpha_paper_book_1"


# =========================================================================== #
# helpers — the real failure SHAPE, spelled hermetically
# =========================================================================== #
def _obligation(instrument, *, weight, max_valid=0.0, asset_class=None,
                sleeve=None):
    return {"instrument_id": instrument, "ticker": instrument,
            "asset_class": asset_class, "sleeve_id": sleeve,
            "tier": hoc.OBLIGATION_TIER_GOVERNANCE,
            "obligation_type": hoc.OBLIGATION_TIER_GOVERNANCE,
            "reason_code": hoc.OBLIGATION_REASON_RETENTION,
            "source_owner": hoc.CALCULATION_OWNER,
            "required_action": hoc.REQUIRED_ACTION_EXIT,
            "current_weight": weight, "max_valid_weight": max_valid,
            "required_exit": True, "evidence_id": None,
            "evidence_hash": "EV_HASH", "detail": "ruled BROKEN"}


def _proposal(*, obligations, final_weights, state="READY", approvable=True,
              carry_contract=True):
    """A proposal artifact's ``proposal`` block, shaped like the live one.

    ``carry_contract=False`` reproduces a PRE-R63 artifact: the block is absent
    and the target's compliance is unknowable from the artifact.
    """
    open_rows = hoc.obligations_open_against(obligations=obligations,
                                             weights=final_weights)
    p = {
        "proposal_state": state,
        "proposal_hash": "PHASH_%s" % state,
        "approvable": approvable,
        "outcome": "PROPOSAL_READY",
        "reallocation_outcome": {"headline": "READY", "reason_codes": []},
        "eligible_market_date": SESSION,
        "active_book_id": BOOK,
        "withheld_reasons": [],
        "allocations": [{"ticker": t, "action": "RETAIN", "current_weight": w,
                         "proposed_weight": w} for t, w in final_weights.items()],
        "action_counts": {"EXIT": 2, "ADD": 2, "RETAIN": 1},
        "turnover": {"one_way_turnover": 0.2, "estimated_transaction_cost": 50.0},
        "signal": {}, "risk": {}, "constraints": {}, "diagnostics": {},
        "data_gaps": [], "policy": {},
        "constraint_reoptimization": {},
    }
    if carry_contract:
        p["mandatory_repair"] = {
            "contract_version": hoc.MANDATORY_REPAIR_CONTRACT_VERSION,
            "owner": hoc.CALCULATION_OWNER,
            "obligations": obligations,
            "obligation_count": len(obligations),
            "obligations_open_against_target": open_rows,
            "obligations_open_count": len(open_rows),
            "obligations_resolved": not open_rows,
        }
        p["full_target_reviewable"] = not open_rows
    return p


def _artifact(proposal):
    return {"proposal_id": "reap_%s_%s_test" % (SESSION, BOOK),
            "generated_at": "2026-09-22T16:18:08+00:00",
            "proposal": proposal,
            "identity": {"active_book_id": BOOK,
                         "eligible_market_date": SESSION}}


# --- the exact live failure: an EXIT_TO_ZERO ruling left half-taken ---------- #
LH_VLO_OBLIGATIONS = [_obligation("LH", weight=0.038858),
                      _obligation("VLO", weight=0.047608)]
#: what the 09:03 target actually published — LH halved, VLO retained untouched.
LH_VLO_UNREPAIRED = {"LH": 0.024667, "VLO": 0.047608, "NVDA": 0.05}
#: what a compliant target publishes — both gone.
LH_VLO_REPAIRED = {"NVDA": 0.05, "MSFT": 0.05}


# =========================================================================== #
# 1. THE READ SEAM — it renders the verdict and fails closed
# =========================================================================== #
class TestProposalReadSeam:

    def test_01_an_unrepaired_target_is_not_approvable_or_executable(self):
        """The live LH/VLO failure. Both flags go False and the reason is named."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        payload = arp.load_reallocation_proposal(artifact=art)

        assert payload["approvable"] is False
        assert payload["executable"] is False
        assert payload["full_target_reviewable"] is False
        v = payload["mandatory_repair_verdict"]
        assert v["code"] == hoc.OBLIGATIONS_UNRESOLVED
        assert v["verifiable"] is True
        assert v["instruments"] == ["LH", "VLO"]
        assert "LH" in v["detail"] and "VLO" in v["detail"]

    def test_02_a_repaired_target_stays_approvable(self):
        """The correction may not cost a compliant proposal its approvability."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_REPAIRED))
        payload = arp.load_reallocation_proposal(artifact=art)

        assert payload["approvable"] is True
        assert payload["executable"] is True
        assert payload["full_target_reviewable"] is True
        assert payload["mandatory_repair_verdict"]["code"] == hoc.OBLIGATIONS_RESOLVED

    def test_03_a_pre_contract_artifact_is_refused_not_assumed_clean(self):
        """The actual 09:03 defect: no verdict at all, and the old seam said yes."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED,
                                  carry_contract=False))
        payload = arp.load_reallocation_proposal(artifact=art)

        assert payload["approvable"] is False
        assert payload["executable"] is False
        assert payload["full_target_reviewable"] is False
        v = payload["mandatory_repair_verdict"]
        assert v["code"] == hoc.OBLIGATIONS_UNVERIFIABLE
        assert v["verifiable"] is False
        # It is UNVERIFIABLE, never "resolved by default".
        assert v["code"] != hoc.OBLIGATIONS_RESOLVED

    def test_04_the_read_seam_publishes_the_contract_it_judges_on(self):
        """A surface must be able to show WHY, not just that it was refused."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        payload = arp.load_reallocation_proposal(artifact=art)

        assert payload["mandatory_repair"]["obligation_count"] == 2
        assert payload["mandatory_repair"]["obligations_resolved"] is False
        assert {r["ticker"] for r in
                payload["mandatory_repair"]["obligations_open_against_target"]} == {
                    "LH", "VLO"}

    def test_05_the_compact_summary_reaches_the_same_verdict(self):
        """The decision owner reads the SUMMARY; it may not disagree with the read."""
        for weights, approvable, code in (
                (LH_VLO_UNREPAIRED, False, hoc.OBLIGATIONS_UNRESOLVED),
                (LH_VLO_REPAIRED, True, hoc.OBLIGATIONS_RESOLVED)):
            art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                      final_weights=weights))
            summ = arp.load_proposal_summary(artifact=art)
            assert summ["reallocation_proposal_approvable"] is approvable
            assert summ["reallocation_full_target_reviewable"] is approvable
            assert summ["reallocation_mandatory_repair_code"] == code

    def test_06_a_withheld_state_still_outranks_everything(self):
        """The new term ADDS to the existing gates; it never relaxes one."""
        art = _artifact(_proposal(obligations=[], final_weights=LH_VLO_REPAIRED,
                                  state="WITHHELD"))
        payload = arp.load_reallocation_proposal(artifact=art)
        assert payload["approvable"] is False


# =========================================================================== #
# 2. THE APPROVE SEAM — the write path refuses, and writes nothing
# =========================================================================== #
class TestApprovalSeam:

    def _approve(self, tmp_path, artifact, decision=None):
        return pdec.record_decision(
            decision=decision or pdec.DECISION_APPROVE,
            confirm=pdec.CONFIRM_TOKEN, artifact=artifact,
            actor="test", decision_dir=tmp_path, latest_session=SESSION)

    def test_10_an_unrepaired_target_cannot_be_approved(self, tmp_path):
        """The defect: with NO selection recorded, approval used to fall through."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        res = self._approve(tmp_path, art)

        assert res["status"] == pdec.PDS_REPAIR_OBLIGATIONS_OPEN
        assert res["recorded"] is False
        assert res["obligations_open"] == ["LH", "VLO"]
        assert "LH" in res["message"] and "VLO" in res["message"]

    def test_11_the_refusal_writes_absolutely_nothing(self, tmp_path):
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        res = self._approve(tmp_path, art)

        assert res["created_orders"] is False
        assert res["created_fills"] is False
        assert res["changed_holdings"] is False
        assert res["changed_cash"] is False
        assert res["changed_nav"] is False
        assert list(tmp_path.iterdir()) == []

    def test_12_a_pre_contract_artifact_cannot_be_approved(self, tmp_path):
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED,
                                  carry_contract=False))
        res = self._approve(tmp_path, art)

        assert res["status"] == pdec.PDS_REPAIR_OBLIGATIONS_OPEN
        assert res["recorded"] is False
        assert res["mandatory_repair_verdict"]["code"] == hoc.OBLIGATIONS_UNVERIFIABLE
        assert list(tmp_path.iterdir()) == []

    def test_13_a_compliant_target_is_still_approvable(self, tmp_path):
        """The gate must not break the normal path.

        R69.2 - an APPROVE now has to name the target it approves, so the
        governed FULL_TARGET selection is recorded first. That is the contract,
        not a workaround: an approval that names nothing used to be executed as
        the standing full target.
        """
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_REPAIRED))
        block = {"options": [{"target": t, "label": t, "selectable": True,
                              "blockers": [], "blocker_codes": [], "is_defer": False}
                             for t in pdec.TARGET_VOCAB],
                 "recommended_target": "FULL_TARGET"}
        sel = pdec.record_target_selection(
            target=pdec.TARGET_FULL_TARGET, confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope={
                "status": "OK", "proposal_id": art["proposal_id"],
                "eligible_market_date": SESSION, "target_selection": block,
                "review": {"target_selection": block,
                           "reviewed_proposal": {
                               "eligible_market_date": SESSION,
                               "active_book_id": BOOK}}},
            decision_dir=tmp_path, latest_session=SESSION)
        assert sel["selected"] is True

        res = self._approve(tmp_path, art)

        assert res["recorded"] is True
        assert res["status"] == "CREATED"
        assert res["record"]["selected_target"] == pdec.TARGET_FULL_TARGET
        # Approval still creates no order, no fill and moves no capital.
        assert res["created_orders"] is False
        assert res["created_fills"] is False

    def test_13b_an_approval_that_names_no_target_is_refused(self, tmp_path):
        """R69.2 - the silent fallback to the standing full target is gone."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_REPAIRED))
        res = self._approve(tmp_path, art)
        assert res["status"] == pdec.PDS_TARGET_SELECTION_REQUIRED
        assert res["recorded"] is False
        assert res["next_required_action"] == "SELECT_TARGET"
        assert list(tmp_path.iterdir()) == []

    def test_14_reject_and_hold_stay_available_on_an_unrepaired_target(self, tmp_path):
        """Refusing those too would leave the operator no way to record a judgement."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        for decision in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
            res = self._approve(tmp_path, art, decision=decision)
            assert res["status"] != pdec.PDS_REPAIR_OBLIGATIONS_OPEN
            assert res["created_orders"] is False
            assert res["created_fills"] is False

    def test_15_the_refused_state_is_a_declared_member_of_the_vocabulary(self):
        """A state no projection declares is a state a surface renders as blank."""
        assert pdec.PDS_REPAIR_OBLIGATIONS_OPEN in pdec.DECISION_STATE_VOCAB
        assert pdec.PDS_REPAIR_OBLIGATIONS_OPEN not in pdec.APPROVABLE_DECISION_STATES


# =========================================================================== #
# 3. ONE VOCABULARY — the refusal is spelled once, by the contract owner
# =========================================================================== #
class TestOneVocabulary:

    def test_20_every_consumer_spells_the_refusal_identically(self):
        """The review's selection blocker, the decision state and the read verdict
        are the SAME code, re-exported from the contract owner - never forked."""
        assert pdr.SELECT_BLOCK_OBLIGATIONS_OPEN is hoc.OBLIGATIONS_UNRESOLVED
        assert pdec.PDS_REPAIR_OBLIGATIONS_OPEN is hoc.OBLIGATIONS_UNRESOLVED

    def test_21_the_read_verdict_vocabulary_is_declared(self):
        assert hoc.OBLIGATIONS_RESOLVED in hoc.OBLIGATION_READ_VERDICT_VOCAB
        assert hoc.OBLIGATIONS_UNRESOLVED in hoc.OBLIGATION_READ_VERDICT_VOCAB
        assert hoc.OBLIGATIONS_UNVERIFIABLE in hoc.OBLIGATION_READ_VERDICT_VOCAB

    def test_22_the_read_verdict_derives_nothing_of_its_own(self):
        """It carries the kernel's published answer. Flip only the kernel's own
        field and the verdict must follow it, without re-deriving from weights."""
        p = _proposal(obligations=LH_VLO_OBLIGATIONS,
                      final_weights=LH_VLO_REPAIRED)
        assert rpk.mandatory_repair_read_verdict(p)["reviewable"] is True

        tampered = copy.deepcopy(p)
        tampered["mandatory_repair"]["obligations_resolved"] = False
        assert rpk.mandatory_repair_read_verdict(tampered)["reviewable"] is False

    def test_23_no_second_classifier_was_introduced(self):
        """The seam reads the artifact's block; it never calls the obligation
        builders itself, which is what a second classifier would look like."""
        import inspect
        src = inspect.getsource(rpk.mandatory_repair_read_verdict)
        for forbidden in ("governance_repair_obligations(",
                          "obligations_from_ruled_rows(",
                          "retention_obligation(", "obligations_open_against("):
            assert forbidden not in src


# =========================================================================== #
# 4. ASSET-AGNOSTIC — the seam never assumes an equity
# =========================================================================== #
class TestAssetAgnostic:

    @pytest.mark.parametrize("instrument,asset_class,sleeve", [
        ("ESZ6", "FUTURES", "ts_trend"),
        ("EURUSD", "FX", "fx_carry"),
        ("BTC-PERP", "CRYPTO", "funding_carry"),
        ("GCZ6", "COMMODITIES", "basis_momentum"),
    ])
    def test_30_a_non_equity_obligation_blocks_the_same_way(
            self, tmp_path, instrument, asset_class, sleeve):
        obl = [_obligation(instrument, weight=0.30, asset_class=asset_class,
                           sleeve=sleeve)]
        art = _artifact(_proposal(obligations=obl,
                                  final_weights={instrument: 0.18, "CASH_PROXY": 0.1}))

        payload = arp.load_reallocation_proposal(artifact=art)
        assert payload["approvable"] is False
        assert payload["full_target_reviewable"] is False
        assert payload["mandatory_repair_verdict"]["instruments"] == [instrument]

        res = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=art, actor="test", decision_dir=tmp_path,
            latest_session=SESSION)
        assert res["status"] == pdec.PDS_REPAIR_OBLIGATIONS_OPEN
        assert res["obligations_open"] == [instrument]
        assert list(tmp_path.iterdir()) == []

    def test_31_a_mixed_asset_book_names_every_open_instrument(self, tmp_path):
        obl = [_obligation("ESZ6", weight=0.20, asset_class="FUTURES"),
               _obligation("LH", weight=0.04, asset_class="US_EQUITY"),
               _obligation("EURUSD", weight=0.10, asset_class="FX")]
        # only the equity leg is actually taken to zero
        art = _artifact(_proposal(
            obligations=obl, final_weights={"ESZ6": 0.20, "EURUSD": 0.05}))

        v = arp.load_reallocation_proposal(artifact=art)["mandatory_repair_verdict"]
        assert v["instruments"] == ["ESZ6", "EURUSD"]
        assert v["code"] == hoc.OBLIGATIONS_UNRESOLVED

    def test_32_the_seam_reads_instrument_id_not_only_ticker(self):
        """An instrument spelled only by instrument_id must still be named."""
        obl = [{"instrument_id": "6E.CME.Z6", "asset_class": "FUTURES",
                "tier": hoc.OBLIGATION_TIER_GOVERNANCE,
                "required_action": hoc.REQUIRED_ACTION_EXIT,
                "current_weight": 0.2, "max_valid_weight": 0.0}]
        art = _artifact(_proposal(obligations=obl,
                                  final_weights={"6E.CME.Z6": 0.2}))
        v = arp.load_reallocation_proposal(artifact=art)["mandatory_repair_verdict"]
        assert v["instruments"] == ["6E.CME.Z6"]


# =========================================================================== #
# 5. THE SURFACES AGREE — one proposal, one answer
# =========================================================================== #
class TestSurfaceCoherence:

    @staticmethod
    def _semantics(**kw):
        """check_decision_semantics with a coherent, violation-free baseline, so a
        test only has to state the ONE thing it is varying."""
        from paper_trader.api import workflow_state as ws
        base = dict(
            reallocation_operator_state=ws.RPS_READY,
            reallocation_approvable=True,
            reassessment_state=None, reassessment_proposal_required=True,
            portfolio_decision_state=pdec.PDS_REVIEW_REQUIRED,
            portfolio_decision_requires_review=True,
            portfolio_decision_approvable=True,
            proposal_bound_reassessment_hash="HOC_HASH",
            current_reassessment_hash="HOC_HASH",
            mandatory_exit_tickers=[],
            mandatory_exit_obligation=None)
        base.update(kw)
        return ws.check_decision_semantics(**base)

    def test_40_the_workflow_invariant_catches_the_contradiction(self):
        """I9: an unrepaired target exposed as approvable is a REPORTED violation,
        not something an operator has to notice by comparing two cards."""
        violations = self._semantics(
            full_target_reviewable=False,
            mandatory_repair_code=hoc.OBLIGATIONS_UNRESOLVED,
            mandatory_obligations_open=["LH", "VLO"])
        codes = [v["code"] for v in violations]
        assert "UNREPAIRED_TARGET_EXPOSED_AS_APPROVABLE" in codes
        hit = next(v for v in violations
                   if v["code"] == "UNREPAIRED_TARGET_EXPOSED_AS_APPROVABLE")
        assert hit["obligations_open"] == ["LH", "VLO"]
        assert "engine.holding_opportunity_cost" in hit["authoritative_owners"]

    def test_41_a_compliant_proposal_raises_no_violation(self):
        violations = self._semantics(
            full_target_reviewable=True,
            mandatory_repair_code=hoc.OBLIGATIONS_RESOLVED,
            mandatory_obligations_open=[])
        assert "UNREPAIRED_TARGET_EXPOSED_AS_APPROVABLE" not in [
            v["code"] for v in violations]

    def test_42_absent_information_does_not_fire_the_invariant(self):
        """None means "no proposal / no information", which is not a contradiction."""
        violations = self._semantics(full_target_reviewable=None)
        assert "UNREPAIRED_TARGET_EXPOSED_AS_APPROVABLE" not in [
            v["code"] for v in violations]

    def test_43_no_surface_may_be_told_a_target_satisfied_what_it_did_not(self):
        """The report the operator reads must never claim satisfaction falsely."""
        art = _artifact(_proposal(obligations=LH_VLO_OBLIGATIONS,
                                  final_weights=LH_VLO_UNREPAIRED))
        payload = arp.load_reallocation_proposal(artifact=art)
        assert payload["mandatory_repair"]["obligations_resolved"] is False
        assert payload["full_target_reviewable"] is False
        assert payload["approvable"] is False
