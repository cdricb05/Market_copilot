"""Release 54.4 — ONE GOVERNED PORTFOLIO DECISION WRITER.

Daily DRC + intraday authority consolidation.

The governed portfolio decision is ONE business concept. Before this release it
had two persistence realities: an intraday decision was APPENDED to an immutable
ledger owned by ``api.portfolio_decision``, while the session-terminal DAILY
decision was never written at all — it lived inside the Daily Research Cycle's
run manifest and was RE-DERIVED at every read from three separately mutable
inputs. A decision that is recomputed on read is not a decision the system ever
MADE, has no record id, and cannot be named in a supersession lineage.

R54.4 makes the daily cycle a PRODUCER that delegates its governed write to the
one decision owner. Producer is not authority. This suite proves the forty
invariants that consolidation must satisfy — and, just as importantly, the
safety boundaries it may never cross: no order, no fill, no broker call, no
automation, no model promotion, no operational-mark advance, and no rewriting of
immutable history.

Every write-path test uses a hermetic store (``tmp_path``). Nothing here touches
a production root, the live backend, or the operational paper desk.
"""
from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.engine import constrained_reallocation as cr

BOOK = "alpha_paper_book_1"
SESSION = "2026-09-02"
NEXT_SESSION = "2026-09-03"
HOC_AID = "hoc_2026-09-02_alpha_paper_book_1_a162fca9"
RUN_ID = "drc_2026-09-02_15abfb01856f"

T_EARLY = datetime(2026, 9, 2, 14, 5, 0, tzinfo=timezone.utc)
T_LATE = datetime(2026, 9, 2, 23, 51, 52, tzinfo=timezone.utc)
STAMP_EARLY = "2026-09-02T14:05:00+00:00"
STAMP_LATE = "2026-09-02T23:51:52+00:00"

PD_SRC = Path(pdec.__file__).read_text(encoding="utf-8")
DRC_SRC = Path(drc.__file__).read_text(encoding="utf-8")
AMS_SRC = Path(ams.__file__).read_text(encoding="utf-8")


# =========================================================================== #
# Fixtures — every field is the shape its real owner publishes.
# =========================================================================== #
def _ps(**kw):
    d = {"active_book": {"book_id": BOOK},
         "dates": {"eligible_market_date": SESSION,
                   "desk_mark_date": SESSION,
                   "latest_daily_close_date": SESSION},
         "state_hash": "PSH_DOC", "economic_state_hash": "ESH1"}
    d.update(kw)
    return d


def _man(**kw):
    """api.daily_research_cycle's own terminal-COMPLETE run manifest."""
    d = {"run_id": RUN_ID, "state": "COMPLETE",
         "active_book_id": BOOK, "eligible_market_date": SESSION,
         "completed_at": STAMP_LATE,
         "session_contract_hash": "SCH1", "input_contract_hash": "ICH1",
         "portfolio_reassessment_id": "ra_1",
         "portfolio_reassessment_hash": "RA1",
         "portfolio_reassessment_state": "CURRENT_NO_CHANGE",
         "reallocation_proposal_id": "", "reallocation_proposal_hash": "",
         "reallocation_proposal_state": "NOT_REQUIRED",
         "opportunity_cost_artifact_id": HOC_AID,
         "opportunity_cost_assessment_hash": "HOC1"}
    d.update(kw)
    return d


def _reas(state="CURRENT_NO_CHANGE", **kw):
    d = {"state": state, "eligible_market_date": SESSION,
         "active_book": {"book_id": BOOK},
         "reassessment_id": "ra_1", "reassessment_hash": "RA1",
         "artifact": {"reassessment_id": "ra_1", "generated_at": STAMP_LATE,
                      "identity": {"economic_state_hash": "ESH1"}},
         "proposal_binding": {"reassessment_id": "ra_1",
                              "reassessment_hash": "RA1",
                              "hoc_assessment_hash": "HOC1",
                              "hoc_artifact_id": HOC_AID,
                              "hoc_assessment_evidence_hash": "HOCEV1",
                              "hoc_persisted": True,
                              "universe_scoring_hash": "US1",
                              "universe_input_contract_hash": "UIC1",
                              "portfolio_state_hash": "PSH1",
                              "corporate_actions_hash": "CA1",
                              "eligible_market_date": SESSION,
                              "active_book_id": BOOK}}
    d.update(kw)
    return d


def _summ(outcome=None, with_proposal=False, **kw):
    d = {"reallocation_proposal_available": with_proposal,
         "reallocation_proposal_stale": False,
         "reallocation_proposal_withheld": False,
         "reallocation_corporate_actions_hash": "CA1",
         "reallocation_proposal_hash": "PR1" if with_proposal else None,
         "reallocation_proposal_id": "prop_1" if with_proposal else None,
         "reallocation_outcome": outcome,
         "reallocation_data_gaps": []}
    d.update(kw)
    return d


def _con(outcome=None, **kw):
    econ = {"switching_hurdle": 0.02,
            "clears_switching_hurdle": outcome == cr.OUTCOME_PROPOSAL_READY,
            "score_improvement_net_of_cost": 0.004, "one_way_turnover": 0.11,
            "estimated_transaction_cost": 214.5}
    d = {"outcome": outcome, "feasible_target_exists": bool(outcome),
         "calculation_owner": cr.CALCULATION_OWNER, "switching_economics": econ,
         "ideal_target": {"zero_base_owner": "api.zero_base_target"},
         "multi_asset": {"current_holdings_privileged": False},
         "best_feasible_target": {"allocations": [
             {"ticker": "T00", "action": "REDUCE", "current_weight": 0.06,
              "proposed_weight": 0.03, "delta_weight": -0.03,
              "capital_change": -2900.0}]}}
    d.update(kw)
    return d


def _hocb(**kw):
    d = {"hoc_artifact_id": HOC_AID, "hoc_persisted": True,
         "hoc_persistence_status": "CREATED",
         "hoc_assessment_evidence_hash": "HOCEV1",
         "hoc_artifact_retrievable": True,
         "hoc_artifact_identity_matches": True,
         "hoc_binding_detail": "artifact opened by id",
         "hoc_binding_resolved_by": "api.holding_opportunity_cost.resolve_binding"}
    d.update(kw)
    return d


def _sc(**kw):
    d = {"ranking_date": SESSION, "input_contract_hash": "UIC1"}
    d.update(kw)
    return d


def _daily(*, man=None, reas=None, summ=None, con=None, hocb=None, ps=None,
           standing=None, now=None):
    """Build the DAILY candidate and run the DAILY gate. Pure; no io."""
    cand = pdec.build_daily_cycle_candidate(
        portfolio_state=ps if ps is not None else _ps(),
        drc_manifest=man if man is not None else _man(),
        reassessment=reas if reas is not None else _reas(),
        proposal_summary=summ if summ is not None else _summ(),
        constrained=con if con is not None else _con(),
        scoring_identity=_sc(), hoc_binding=hocb if hocb is not None else _hocb(),
        now=now)
    gate = pdec.evaluate_daily_cycle_governance(
        candidate=cand, drc_manifest=man if man is not None else _man(),
        portfolio_state=ps if ps is not None else _ps(),
        reassessment=reas if reas is not None else _reas(),
        proposal_summary=summ if summ is not None else _summ(),
        constrained=con if con is not None else _con(),
        current_governed=standing)
    return cand, gate


def _change_kit():
    """The CHANGE-shaped daily evidence set (a proposal WAS requested)."""
    return dict(
        man=_man(portfolio_reassessment_state="PROPOSAL_READY",
                 reallocation_proposal_id="prop_1",
                 reallocation_proposal_hash="PR1",
                 reallocation_proposal_state="READY"),
        reas=_reas(state="PROPOSAL_READY"),
        summ=_summ(outcome=cr.OUTCOME_PROPOSAL_READY, with_proposal=True),
        con=_con(outcome=cr.OUTCOME_PROPOSAL_READY))


def _record(cand, gate, tmp_path, now=None):
    return pdec.record_governed_decision(
        candidate=cand, gate=gate,
        provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=now)


# =========================================================================== #
# 1-4. ONE writer, ONE authority, two producers.
# =========================================================================== #
def test_01_daily_drc_delegates_governed_persistence_to_the_decision_owner():
    """The producer names the owner, delegates, and reports — it does not persist."""
    assert '_DECISION_OWNER = "api.portfolio_decision"' in DRC_SRC
    assert "def _delegate_governed_decision(" in DRC_SRC
    assert "pdec.govern_daily_cycle_decision(" in DRC_SRC
    assert 'rec["governed_portfolio_decision"] = delegated' in DRC_SRC
    # The producer defines NO decision concept of its own.
    for forbidden in ("def record_governed_decision(",
                      "def governed_decision_ordering_key(",
                      "def load_governed_decision_record(",
                      "def candidate_identity_hash(",
                      "def evaluate_intraday_governance(",
                      "GOVERNED_DECISION_VOCAB = ", "_PROVENANCE_RANK = "):
        assert forbidden not in DRC_SRC, forbidden


def test_02_intraday_still_delegates_to_the_same_owner():
    esr = Path(
        __import__("paper_trader.api.event_signal_refresh", fromlist=["x"])
        .__file__).read_text(encoding="utf-8")
    assert "pdec.govern_latest_intraday_assessment(" in esr
    assert "def record_governed_decision(" not in esr
    assert "def evaluate_intraday_governance(" not in esr
    # Both composed entry points route to the ONE writer.
    assert "provenance=PROV_GOVERNED_INTRADAY," in PD_SRC
    assert "provenance=PROV_GOVERNED_DAILY_CYCLE," in PD_SRC


def test_03_exactly_one_governed_decision_writer_exists():
    assert PD_SRC.count("def record_governed_decision(") == 1
    # The governed ledger is written in exactly ONE place, and only its own two
    # files are written there.
    assert PD_SRC.count("_atomic_write_json(_governed_records_path(") == 1
    assert PD_SRC.count("_atomic_write_json(_governed_index_path(") == 1
    writer = PD_SRC.split("def record_governed_decision(")[1].split("\ndef ")[0]
    assert writer.count("_atomic_write_json(") == 2
    # The governed lane never writes the MANUAL operator-decision pointer.
    assert "_atomic_write_json(_index_path(" not in writer
    assert "_atomic_write_json(_records_path(" not in writer


def test_04_exactly_one_governed_decision_index_authority_exists():
    assert PD_SRC.count('_GOVERNED_RECORDS_FILE = "governed_decisions.json"') == 1
    assert PD_SRC.count('_GOVERNED_INDEX_FILE = "governed_index.json"') == 1
    assert PD_SRC.count("def governed_decision_ordering_key(") == 1
    assert "governed_decisions.json" not in DRC_SRC
    assert "governed_index.json" not in DRC_SRC


# =========================================================================== #
# 5-8. Both conclusions, from both producers, persist canonically.
# =========================================================================== #
def test_05_daily_hold_persists_canonically(tmp_path):
    kit = dict(man=_man(portfolio_reassessment_state="HOLD_CURRENT_BOOK"),
               reas=_reas(state="HOLD_CURRENT_BOOK"),
               summ=_summ(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK),
               con=_con(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK))
    cand, gate = _daily(**kit)
    assert cand["decision"] == pdec.GD_HOLD_CURRENT_BOOK
    assert gate["eligible"] is True, gate["withheld_reason_codes"]
    out = _record(cand, gate, tmp_path)
    assert out["recorded"] is True and out["status"] == "CREATED"
    rec = out["record"]
    assert rec["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
    assert rec["decision"] == pdec.GD_HOLD_CURRENT_BOOK
    assert rec["manual_review_required"] is False
    assert json.loads((tmp_path / "governed_decisions.json").read_text())


def test_06_daily_change_persists_canonically(tmp_path):
    cand, gate = _daily(**_change_kit())
    assert cand["decision"] == pdec.GD_CHANGE_RECOMMENDED
    assert gate["eligible"] is True, gate["withheld_reason_codes"]
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
    assert rec["manual_review_required"] is True
    assert rec["identity"]["proposal_hash"] == "PR1"


def test_07_daily_current_no_change_persists_canonically(tmp_path):
    """The production case: the session concluded no proposal was needed."""
    cand, gate = _daily()
    assert cand["decision"] == pdec.GD_NO_CHANGE
    assert gate["eligible"] is True, gate["withheld_reason_codes"]
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["decision"] == pdec.GD_NO_CHANGE
    assert rec["manual_review_required"] is False
    assert rec["identity"]["proposal_hash"] is None


def test_08_intraday_hold_and_change_still_persist_canonically(tmp_path):
    """R54.1 behaviour is untouched by the daily consolidation."""
    for outcome, word in ((cr.OUTCOME_HOLD_CURRENT_BOOK,
                           pdec.GD_HOLD_CURRENT_BOOK),
                          (cr.OUTCOME_PROPOSAL_READY,
                           pdec.GD_CHANGE_RECOMMENDED)):
        cand = pdec.build_intraday_candidate(
            portfolio_state=_ps(), event_cycle={"active_book_id": BOOK,
                                                "eligible_market_date": SESSION},
            reassessment=_reas(state="PROPOSAL_READY"),
            proposal_summary=_summ(outcome=outcome, with_proposal=True),
            constrained=_con(outcome=outcome), scoring_identity=_sc(),
            hoc_binding=_hocb())
        assert cand["decision"] == word
        assert cand["provenance"] == pdec.PROV_GOVERNED_INTRADAY


# =========================================================================== #
# 9-11. Idempotency — no retry and no dual production creates a second authority.
# =========================================================================== #
def test_09_daily_identical_retry_is_idempotent(tmp_path):
    cand, gate = _daily()
    first = _record(cand, gate, tmp_path)
    assert first["status"] == "CREATED"
    cand2, gate2 = _daily(standing=first["record"])
    assert cand2["candidate_identity_hash"] == cand["candidate_identity_hash"]
    again = _record(cand2, gate2, tmp_path)
    assert again["status"] == "REUSED_EXISTING"
    assert again["idempotent"] is True
    rows = json.loads((tmp_path / "governed_decisions.json").read_text())
    assert len(rows) == 1


def test_10_intraday_identical_retry_is_idempotent(tmp_path):
    cand = pdec.build_intraday_candidate(
        portfolio_state=_ps(), event_cycle={"active_book_id": BOOK,
                                            "eligible_market_date": SESSION},
        reassessment=_reas(state="PROPOSAL_READY"),
        proposal_summary=_summ(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK),
        constrained=_con(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK),
        scoring_identity=_sc(), hoc_binding=_hocb())
    gate = {"eligible": True, "verdict": pdec.GATE_ELIGIBLE, "checks_passed": 45,
            "checks_total": 45, "evaluated_at": STAMP_EARLY}
    first = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T_EARLY)
    assert first["status"] == "CREATED"
    again = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T_EARLY)
    assert again["status"] == "REUSED_EXISTING"
    assert len(json.loads((tmp_path / "governed_decisions.json").read_text())) == 1


def test_11_daily_and_intraday_identical_evidence_create_one_authority(tmp_path):
    """The SAME evidence seen by BOTH producers is ONE decision, not two.

    This is the invariant that the shared identity contract exists to guarantee:
    the two lanes compute the same ``candidate_identity_hash``, so the writer
    recognises the second as a duplicate instead of appending a rival authority.
    """
    # Both a priced HOLD and a priced CHANGE — the CHANGE case is the one where
    # a real proposal identity has to agree across the two lanes.
    for outcome, state in ((cr.OUTCOME_HOLD_CURRENT_BOOK, "HOLD_CURRENT_BOOK"),
                           (cr.OUTCOME_PROPOSAL_READY, "PROPOSAL_READY")):
        common = dict(reassessment=_reas(state=state),
                      proposal_summary=_summ(outcome=outcome, with_proposal=True),
                      constrained=_con(outcome=outcome),
                      scoring_identity=_sc(), hoc_binding=_hocb())
        intra = pdec.build_intraday_candidate(
            portfolio_state=_ps(), event_cycle=None, **common)
        daily = pdec.build_daily_cycle_candidate(
            portfolio_state=_ps(),
            drc_manifest=_man(portfolio_reassessment_state=state,
                              reallocation_proposal_id="prop_1",
                              reallocation_proposal_hash="PR1",
                              reallocation_proposal_state="READY"),
            **common)
        assert intra["identity"] == daily["identity"], outcome
        assert intra["candidate_identity_hash"] == \
            daily["candidate_identity_hash"], outcome
        assert daily["identity"]["proposal_hash"] == "PR1"

    outcome = cr.OUTCOME_HOLD_CURRENT_BOOK
    common = dict(reassessment=_reas(state="HOLD_CURRENT_BOOK"),
                  proposal_summary=_summ(outcome=outcome, with_proposal=True),
                  constrained=_con(outcome=outcome),
                  scoring_identity=_sc(), hoc_binding=_hocb())
    intra = pdec.build_intraday_candidate(
        portfolio_state=_ps(), event_cycle=None, **common)
    daily = pdec.build_daily_cycle_candidate(
        portfolio_state=_ps(),
        drc_manifest=_man(portfolio_reassessment_state="HOLD_CURRENT_BOOK",
                          reallocation_proposal_id="prop_1",
                          reallocation_proposal_hash="PR1",
                          reallocation_proposal_state="READY"),
        **common)

    gate = {"eligible": True, "verdict": pdec.GATE_ELIGIBLE, "checks_passed": 45,
            "checks_total": 45, "evaluated_at": STAMP_EARLY}
    pdec.record_governed_decision(
        candidate=intra, gate=gate, provenance=pdec.PROV_GOVERNED_INTRADAY,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
        now=T_EARLY)
    second = pdec.record_governed_decision(
        candidate=daily, gate=gate, provenance=pdec.PROV_GOVERNED_DAILY_CYCLE,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
        now=T_LATE)
    assert second["status"] == "REUSED_EXISTING"
    assert len(json.loads((tmp_path / "governed_decisions.json").read_text())) == 1


# =========================================================================== #
# 12-15. Authority ordering.
# =========================================================================== #
def test_12_same_session_newer_evidence_may_supersede_older(tmp_path):
    cand, gate = _daily(now=T_EARLY)
    first = _record(cand, gate, tmp_path, now=T_EARLY)
    assert first["status"] == "CREATED"
    # A genuinely NEWER same-session assessment: different evidence, later stamp.
    newer_reas = _reas()
    newer_reas["reassessment_hash"] = "RA2"
    newer_reas["artifact"] = {**newer_reas["artifact"],
                              "generated_at": "2026-09-02T23:59:00+00:00"}
    newer_reas["proposal_binding"] = {**newer_reas["proposal_binding"],
                                      "reassessment_hash": "RA2"}
    cand2, gate2 = _daily(man=_man(portfolio_reassessment_hash="RA2"),
                          reas=newer_reas, standing=first["record"])
    assert gate2["eligible"] is True, gate2["withheld_reason_codes"]
    out = _record(cand2, gate2, tmp_path, now=T_LATE)
    assert out["status"] == "CREATED"
    assert out["record"]["supersedes_decision_id"] == first["record"]["record_id"]


def test_13_older_evidence_cannot_supersede_newer(tmp_path):
    newer_reas = _reas()
    newer_reas["reassessment_hash"] = "RA2"
    newer_reas["artifact"] = {**newer_reas["artifact"],
                              "generated_at": "2026-09-02T23:59:00+00:00"}
    newer_reas["proposal_binding"] = {**newer_reas["proposal_binding"],
                                      "reassessment_hash": "RA2"}
    cand_new, gate_new = _daily(man=_man(portfolio_reassessment_hash="RA2"),
                                reas=newer_reas)
    standing = _record(cand_new, gate_new, tmp_path, now=T_LATE)["record"]
    # The EARLIER assessment now arrives (a late retry). It must be refused.
    old_reas = _reas()
    old_reas["artifact"] = {**old_reas["artifact"],
                            "generated_at": STAMP_EARLY}
    cand_old, gate_old = _daily(reas=old_reas, standing=standing)
    assert gate_old["eligible"] is False
    assert pdec.WR_SUPERSEDED in gate_old["withheld_reason_codes"]
    refused = _record(cand_old, gate_old, tmp_path, now=T_EARLY)
    assert refused.get("recorded") is not True
    assert len(json.loads((tmp_path / "governed_decisions.json").read_text())) == 1


def test_14_newer_eligible_session_outranks_older_session():
    older = {"eligible_market_session": SESSION, "decided_at": STAMP_LATE,
             "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
             "candidate_identity_hash": "zzz"}
    newer = {"eligible_market_session": NEXT_SESSION,
             "decided_at": "2026-09-03T09:00:00+00:00",
             "provenance": pdec.PROV_GOVERNED_INTRADAY,
             "candidate_identity_hash": "aaa"}
    key = pdec.governed_decision_ordering_key
    assert key(newer) > key(older)
    # A later INTRADAY decision beats an earlier DAILY one within one session too:
    # provenance is a tie-break, never a lane privilege.
    early_daily = {"eligible_market_session": SESSION, "decided_at": STAMP_EARLY,
                   "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
                   "candidate_identity_hash": "a"}
    late_intraday = {"eligible_market_session": SESSION, "decided_at": STAMP_LATE,
                     "provenance": pdec.PROV_GOVERNED_INTRADAY,
                     "candidate_identity_hash": "a"}
    assert key(late_intraday) > key(early_daily)


def test_15_exact_tie_producer_precedence_is_deterministic_and_documented():
    """On an EXACT tie of session AND timestamp, the session-terminal daily cycle
    outranks an intraday promotion — a tie-break only, and stated in the order."""
    daily = {"eligible_market_session": SESSION, "decided_at": STAMP_LATE,
             "provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
             "candidate_identity_hash": "same"}
    intraday = {"eligible_market_session": SESSION, "decided_at": STAMP_LATE,
                "provenance": pdec.PROV_GOVERNED_INTRADAY,
                "candidate_identity_hash": "same"}
    key = pdec.governed_decision_ordering_key
    assert key(daily) > key(intraday)
    assert key(daily) == key(daily)  # total and reproducible
    order = " ".join(pdec.DECISION_AUTHORITY_ORDER)
    assert "provenance, never authority" in order
    assert "TIE-BREAK only" in order
    assert "never reorders decisions that differ in time" in order


# =========================================================================== #
# 16-20. Proposal relationship and manual review.
# =========================================================================== #
def test_16_hold_supersedes_an_older_proposal_correctly():
    """A governed CURRENT_NO_CHANGE says the session requested no proposal, so a
    standing proposal at the key is not endorsed by the decision of record."""
    sup = pdec.assess_proposal_supersession(
        proposal_summary=_summ(outcome=cr.OUTCOME_PROPOSAL_READY,
                               with_proposal=True),
        assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                    "eligible_market_date": SESSION, "reassessment_hash": "RA1",
                    "artifact_id": "ra_1", "generated_at": STAMP_LATE,
                    "is_governed": True,
                    "governed_provenance": pdec.PROV_GOVERNED_DAILY_CYCLE})
    assert sup["superseded"] is True
    authority = pdec.resolve_decision_authority(
        assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                    "is_governed": True, "eligible_market_date": SESSION},
        proposal_summary=_summ(outcome=cr.OUTCOME_PROPOSAL_READY,
                               with_proposal=True),
        supersession=sup)
    assert authority["current_reviewable_proposal_id"] is None


def test_17_change_binds_the_exact_current_proposal(tmp_path):
    cand, gate = _daily(**_change_kit())
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["identity"]["proposal_id"] == "prop_1"
    assert rec["identity"]["proposal_hash"] == "PR1"
    # The bound proposal is the one THIS RUN built, read from the manifest — so
    # a CHANGE can never be bound to an artifact the run did not produce.
    assert rec["evidence_provenance"]["manifest_proposal_hash"] == "PR1"
    # A CHANGE whose run recorded NO proposal has nothing to recommend, and is
    # refused rather than being handed whatever sits at the live proposal key.
    kit = _change_kit()
    kit["man"] = _man(portfolio_reassessment_state="PROPOSAL_READY",
                      reallocation_proposal_state="READY")
    _, bad = _daily(**kit)
    assert bad["eligible"] is False
    assert pdec.WR_TARGET_IDENTITY in bad["withheld_reason_codes"]


def test_18_superseded_proposal_remains_immutable_history(tmp_path):
    cand, gate = _daily(**_change_kit())
    first = _record(cand, gate, tmp_path)
    before = json.loads((tmp_path / "governed_decisions.json").read_text())
    # A newer decision for a LATER session supersedes by APPENDING.
    later = _daily(man=_man(eligible_market_date=NEXT_SESSION),
                   reas=_reas(eligible_market_date=NEXT_SESSION),
                   ps=_ps(dates={"eligible_market_date": NEXT_SESSION}),
                   standing=first["record"])
    out = _record(*later, tmp_path, now=T_LATE)
    assert out["status"] == "CREATED"
    after = json.loads((tmp_path / "governed_decisions.json").read_text())
    assert after[0] == before[0], "the prior row was mutated"
    assert len(after) == len(before) + 1


def test_19_superseded_proposal_remains_unapprovable():
    sup = {"superseded": True, "proposal_id": "prop_1",
           "reason": "A newer governed decision stands"}
    authority = pdec.resolve_decision_authority(
        assessment={"available": True, "decision": "CURRENT_NO_CHANGE",
                    "is_governed": True},
        proposal_summary=_summ(outcome=cr.OUTCOME_PROPOSAL_READY,
                               with_proposal=True),
        supersession=sup)
    assert authority["current_reviewable_proposal_id"] is None
    assert "prop_1" in authority["superseded_proposal_ids"]


def test_20_current_change_remains_manual_review_only(tmp_path):
    cand, gate = _daily(**_change_kit())
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["manual_review_required"] is True
    assert rec["approval_required_token"] == pdec.CONFIRM_TOKEN
    for flag in ("approved_anything", "automatic_approval_allowed",
                 "created_orders", "created_order_plan", "created_fills"):
        assert rec["safety"][flag] is False


# =========================================================================== #
# 21-24. The decision record binds its exact evidence and its producer.
# =========================================================================== #
def test_21_decision_record_binds_the_exact_reassessment_artifact(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["identity"]["reassessment_id"] == "ra_1"
    assert rec["identity"]["reassessment_hash"] == "RA1"
    # A reassessment that is not the manifest's is refused.
    _, bad = _daily(man=_man(portfolio_reassessment_hash="RA_OTHER"))
    assert bad["eligible"] is False
    assert pdec.WR_REASSESSMENT_IDENTITY in bad["withheld_reason_codes"]


def test_22_decision_record_binds_the_exact_hoc_artifact(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["identity"]["hoc_artifact_id"] == HOC_AID
    assert rec["identity"]["hoc_assessment_evidence_hash"] == "HOCEV1"
    # R54.3 parity: an unretrievable or mismatched artifact fails CLOSED.
    _, unretrievable = _daily(hocb=_hocb(hoc_artifact_retrievable=False))
    assert pdec.WR_HOC_NOT_PERSISTED in unretrievable["withheld_reason_codes"]
    _, mismatch = _daily(hocb=_hocb(hoc_artifact_identity_matches=False))
    assert pdec.WR_HOC_ARTIFACT_MISMATCH in mismatch["withheld_reason_codes"]
    _, absent = _daily(man=_man(opportunity_cost_artifact_id=None),
                       reas=_reas(proposal_binding={"reassessment_hash": "RA1",
                                                    "reassessment_id": "ra_1"}),
                       hocb={})
    assert pdec.WR_HOC_NOT_PERSISTED in absent["withheld_reason_codes"]


def test_23_daily_source_metadata_is_retained(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
    ev = rec["evidence_provenance"]
    assert ev["daily_cycle_run_id"] == RUN_ID
    assert ev["daily_cycle_state"] == "COMPLETE"
    assert ev["producer_owner"] == "api.daily_research_cycle"
    assert cand["producer_contract_version"] == pdec.DAILY_PRODUCER_CONTRACT_VERSION


def test_24_intraday_source_metadata_is_retained(tmp_path):
    cand = pdec.build_intraday_candidate(
        portfolio_state=_ps(),
        event_cycle={"active_book_id": BOOK, "eligible_market_date": SESSION,
                     "run_id": "evt_1", "state": "REASSESSED_NO_CHANGE",
                     "materiality_trigger_fingerprint": "FP1"},
        reassessment=_reas(state="PROPOSAL_READY"),
        proposal_summary=_summ(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK),
        constrained=_con(outcome=cr.OUTCOME_HOLD_CURRENT_BOOK),
        scoring_identity=_sc(), hoc_binding=_hocb())
    gate = {"eligible": True, "verdict": pdec.GATE_ELIGIBLE, "checks_passed": 45,
            "checks_total": 45, "evaluated_at": STAMP_EARLY}
    rec = pdec.record_governed_decision(
        candidate=cand, gate=gate, confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
        decision_dir=tmp_path, now=T_EARLY)["record"]
    assert rec["provenance"] == pdec.PROV_GOVERNED_INTRADAY
    assert rec["evidence_provenance"]["event_cycle_run_id"] == "evt_1"
    assert rec["evidence_provenance"]["materiality_trigger_fingerprint"] == "FP1"


# =========================================================================== #
# 25-28. Read projections: one canonical Lane A, a separate Lane B, no JS authority.
# =========================================================================== #
def test_25_active_manager_lane_a_reads_the_canonical_record(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    read = pdec.load_governed_portfolio_decision(
        workflow=None, reassessment=None, proposal_summary=None,
        active_book_id=BOOK, decision_dir=tmp_path)
    assert read["available"] is True
    assert read["record_id"] == rec["record_id"]
    assert read["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
    assert read["persisted"] is True
    # Lane A is composed by the decision owner and merely READ by the manager.
    assert "pdec.load_governed_portfolio_decision(" in AMS_SRC
    assert "def record_governed_decision(" not in AMS_SRC
    assert "def governed_decision_ordering_key(" not in AMS_SRC


def test_26_active_manager_lane_b_remains_research_visibility():
    """Lane B is the live/intraday reassessment — separate from Lane A, and it
    never advances the governed decision."""
    assert "live_reassessment_lane" in AMS_SRC
    assert '"advances_governed_decision": False' in AMS_SRC
    assert "live_reassessment_lane" in ams.COMPONENTS
    assert ams.COMPONENT_OWNERS["live_reassessment_lane"] == \
        "api.active_manager_state"


def test_27_workflow_state_does_not_derive_a_competing_portfolio_decision():
    ws = Path(
        __import__("paper_trader.api.workflow_state", fromlist=["x"])
        .__file__).read_text(encoding="utf-8")
    assert "_import_portfolio_decision().resolve_decision_authority(" in ws
    for forbidden in ("def record_governed_decision(",
                      "def governed_decision_ordering_key(",
                      "def build_daily_cycle_candidate(",
                      "def evaluate_daily_cycle_governance(",
                      "def load_persisted_daily_decision("):
        assert forbidden not in ws, forbidden


def test_28_ui_does_not_derive_authority():
    ui = Path(pdec.__file__).parent.joinpath("ui", "index.html").read_text(
        encoding="utf-8")
    import re
    assert not re.findall(r"GOVERNED_DAILY_CYCLE\s*[!=]==?", ui)
    assert not re.findall(r"GOVERNED_INTRADAY\s*[!=]==?", ui)
    assert not re.findall(r"provenance\s*[!=]==?\s*[\"']GOVERNED", ui)


# =========================================================================== #
# 29-34. Safety boundaries the consolidation may never cross.
# =========================================================================== #
def test_29_to_31_no_order_no_fill_no_broker_call(tmp_path):
    cand, gate = _daily(**_change_kit())
    rec = _record(cand, gate, tmp_path)["record"]
    s = rec["safety"]
    assert s["created_orders"] is False
    assert s["created_order_plan"] is False
    assert s["created_fills"] is False
    assert s["broker_enabled"] is False
    assert s["paper_only"] is True
    # Structural, not a runtime preference: the daily lane has no such reach.
    lane = PD_SRC.split("R54.4")[-1].split("\n# The governed READ")[0]
    for token in ("submit_order", "create_order", "place_order", "record_fill",
                  "confirm_order_plan", "broker_client", "requests.", "httpx",
                  "subprocess", "schtasks"):
        assert token not in lane, token
    # And only the decision store was written.
    assert sorted(p.name for p in Path(tmp_path).iterdir()) == [
        "governed_decisions.json", "governed_index.json"]


def test_32_automation_remains_off(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["safety"]["automation_enabled"] is False
    assert rec["safety"]["automatic_approval_allowed"] is False
    assert "AUTOMATION OFF" in rec["safety"]["safety_badges"]
    assert "MANUAL REVIEW" in rec["safety"]["safety_badges"]


def test_33_no_model_promotion(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["safety"]["promoted_model"] is False
    assert rec["safety"]["automatic_model_promotion_allowed"] is False
    assert rec["safety"]["activated_sleeve"] is False


def test_34_operational_mark_is_not_advanced_by_decision_persistence(tmp_path):
    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    s = rec["safety"]
    assert s["advances_operational_mark"] is False
    assert s["operational_mark_advanced_only_by"] == "api.daily_close"
    assert s["ran_daily_close"] is False
    assert s["changed_holdings"] is False
    assert s["changed_cash"] is False
    assert s["changed_nav"] is False


# =========================================================================== #
# 35-39. Close separation, evidence honesty, legacy compatibility, immutability.
# =========================================================================== #
def test_35_daily_close_remains_a_separate_owner_and_workflow():
    close = Path(
        __import__("paper_trader.api.daily_close", fromlist=["x"])
        .__file__).read_text(encoding="utf-8")
    for forbidden in ("def record_governed_decision(",
                      "def govern_daily_cycle_decision(",
                      "def build_daily_cycle_candidate(",
                      "governed_decisions.json"):
        assert forbidden not in close, forbidden
    # The producer is the RESEARCH cycle, not the close.
    assert "pdec.govern_daily_cycle_decision(" in DRC_SRC
    assert "def run_daily_close(" not in DRC_SRC


def test_36_true_forward_evidence_is_not_fabricated():
    """The delegation carries evidence it was GIVEN; it mints none."""
    lane = PD_SRC.split("R54.4")[-1].split("\n# The governed READ")[0]
    assert "TRUE_FORWARD" not in lane
    assert "def capture_forward" not in lane
    # And the daily candidate names a missing stage instead of inventing one.
    cand, _ = _daily(hocb={})
    assert cand["evidence"]["hoc_artifact_retrievable"] is None


def test_37_legacy_daily_terminal_artifacts_remain_readable():
    """A pre-R54.4 session has no ledger row; the projection still reads it."""
    proj = pdec.project_governed_daily_cycle_decision(
        workflow={"research_cycle_state": {
            "governed_research_evidence_current": True,
            "governed_manifest_run_id": RUN_ID}},
        reassessment=_reas(), proposal_summary=_summ(), constrained=_con())
    assert proj is not None
    assert proj["decision"] == pdec.GD_NO_CHANGE
    assert proj["persisted"] is False
    assert proj["legacy_compatibility_projection"] is True
    assert proj["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE


def test_38_legacy_intraday_artifacts_remain_readable(tmp_path):
    """A row written by the pre-R54.4 intraday writer still loads and orders."""
    legacy = {"record_id": "gdec_legacy", "record_kind":
              "GOVERNED_PORTFOLIO_DECISION",
              "provenance": pdec.PROV_GOVERNED_INTRADAY,
              "decision": pdec.GD_HOLD_CURRENT_BOOK,
              "decided_at": STAMP_EARLY, "active_book_id": BOOK,
              "eligible_market_session": SESSION,
              "candidate_identity_hash": "legacyhash",
              "identity": {"active_book_id": BOOK,
                           "eligible_market_session": SESSION}}
    (tmp_path / "governed_decisions.json").write_text(json.dumps([legacy]))
    (tmp_path / "governed_index.json").write_text(
        json.dumps({BOOK: {"record_id": "gdec_legacy", "record": legacy}}))
    got = pdec.load_governed_decision_record(active_book_id=BOOK,
                                             decision_dir=tmp_path)
    assert got["record_id"] == "gdec_legacy"
    assert pdec.governed_decision_ordering_key(got)[0] == SESSION
    # And a legacy row is NOT a daily row, so it never retires the projection.
    assert pdec.load_persisted_daily_decision(
        active_book_id=BOOK, eligible_market_session=SESSION,
        decision_dir=tmp_path) is None


def test_39_immutable_history_is_never_rewritten_or_deleted(tmp_path):
    cand, gate = _daily(now=T_EARLY)
    first = _record(cand, gate, tmp_path, now=T_EARLY)
    original = json.loads((tmp_path / "governed_decisions.json").read_text())
    later = _daily(man=_man(eligible_market_date=NEXT_SESSION),
                   reas=_reas(eligible_market_date=NEXT_SESSION),
                   ps=_ps(dates={"eligible_market_date": NEXT_SESSION}),
                   standing=first["record"])
    _record(*later, tmp_path, now=T_LATE)
    rows = json.loads((tmp_path / "governed_decisions.json").read_text())
    assert rows[:len(original)] == original
    # The writer deletes nothing and rewrites nothing above the append.
    lane = PD_SRC.split("def record_governed_decision(")[1].split(
        "\ndef ")[0]
    assert "records.append(record)" in lane
    for token in ("records.pop", "records.remove", "del records", "unlink("):
        assert token not in lane, token


# =========================================================================== #
# 40. The architecture audit proves one writer / one authority.
# =========================================================================== #
def test_40_architecture_audit_proves_one_writer_and_one_authority():
    import importlib.util
    root = Path(pdec.__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "audit_architecture_r544", root / "scripts" / "audit_architecture.py")
    audit = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(audit)
    # The "no execution reach / no economics in the daily lane" guards are only
    # meaningful if the lane extraction actually finds the lane. An empty lane
    # would make both invariants pass vacuously, so pin the extraction itself.
    lane = audit._r544_daily_lane(PD_SRC)
    assert len(lane) > 10_000, "daily-lane extraction collapsed"
    for present in ("def build_daily_cycle_candidate(",
                    "def evaluate_daily_cycle_governance(",
                    "def govern_daily_cycle_decision("):
        assert present in lane, present
    for absent in ("def project_governed_daily_cycle_decision(",
                   "def load_governed_portfolio_decision("):
        assert absent not in lane, absent

    rep = audit.run_audit()
    # The R54.4 invariants are DECLARED blocking, not merely computed.
    declared = {(g, k) for g, k, _ in audit.BLOCKING_INVARIANTS}
    assert ("release54_4_single_governed_decision_writer",
            "duplicate_governed_decision_writers") in declared
    assert ("release54_4_single_governed_decision_writer",
            "daily_producer_owns_decision_concepts") in declared
    gw = rep["release54_4_single_governed_decision_writer"]
    assert gw["writer_defs_missing"] == []
    assert gw["duplicate_governed_decision_writers"] == []
    assert gw["second_governed_decision_store"] == []
    assert gw["daily_producer_delegates"] is True
    assert gw["daily_producer_owns_decision_concepts"] == []
    assert gw["delegates_only_after_durable_manifest"] is True
    assert gw["both_producers_use_one_writer"] is True
    assert gw["producer_is_not_authority"] is True
    assert gw["shared_identity_contract"] is True
    assert gw["projection_declared_legacy"] is True
    assert gw["projection_retired_by_persisted_row"] is True
    assert gw["daily_lane_execution_reach"] == []
    assert gw["daily_lane_defines_economics"] == []
    assert gw["daily_gate_fails_closed_on_hoc"] is True
    assert gw["daily_gate_opens_a_store"] == []
    assert gw["manifest_rewritten_with_decision"] is False
    assert gw["ui_derives_producer_authority"] == []
    assert audit._blocking_invariant_failures(rep) == []


def _load_audit():
    import importlib.util
    root = Path(pdec.__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location(
        "audit_architecture_r544_neg", root / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_40b_the_guard_actually_fails_when_the_producer_stops_delegating(tmp_path):
    """A guard that cannot fail is decoration. Prove this one bites."""
    import shutil
    audit = _load_audit()
    repo = Path(pdec.__file__).resolve().parents[1]
    (tmp_path / "api").mkdir(parents=True)
    broken = DRC_SRC.replace("pdec.govern_daily_cycle_decision(",
                             "pdec.no_such_call(")
    (tmp_path / "api" / "daily_research_cycle.py").write_text(
        broken, encoding="utf-8")
    shutil.copy(repo / "api" / "portfolio_decision.py", tmp_path / "api")
    original = audit.REPO_ROOT
    try:
        audit.REPO_ROOT = tmp_path
        out = audit.check_release54_4_single_governed_decision_writer([])
    finally:
        audit.REPO_ROOT = original
    assert out["daily_producer_delegates"] is False


def test_40c_the_guard_actually_fails_on_a_second_writer(tmp_path):
    import shutil
    audit = _load_audit()
    repo = Path(pdec.__file__).resolve().parents[1]
    (tmp_path / "api").mkdir(parents=True)
    for name in ("daily_research_cycle.py", "portfolio_decision.py"):
        shutil.copy(repo / "api" / name, tmp_path / "api")
    rogue = tmp_path / "api" / "rogue_decisions.py"
    rogue.write_text(
        "def record_governed_decision(*, candidate, gate):\n"
        "    return {'governed_decisions.json': True}\n", encoding="utf-8")
    original = audit.REPO_ROOT
    try:
        audit.REPO_ROOT = tmp_path
        out = audit.check_release54_4_single_governed_decision_writer([rogue])
    finally:
        audit.REPO_ROOT = original
    assert out["duplicate_governed_decision_writers"] == [
        "api/rogue_decisions.py:def record_governed_decision("]
    assert out["second_governed_decision_store"] == [
        "api/rogue_decisions.py:governed_decisions.json"]


# =========================================================================== #
# Consolidation-specific behaviour: the gate, the delegation and the retirement.
# =========================================================================== #
def test_51_each_gate_stamps_its_own_verdict_word(tmp_path):
    """A governed row must show WHICH gate admitted it: a daily decision is
    never stamped with the intraday verdict literal."""
    cand, gate = _daily()
    assert gate["verdict"] == pdec.DAILY_GATE_ELIGIBLE
    assert gate["verdict"] == "GOVERNED_DAILY_DECISION_ELIGIBLE"
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["gate"]["verdict"] == pdec.DAILY_GATE_ELIGIBLE
    _, withheld = _daily(man=_man(state="BLOCKED"))
    assert withheld["verdict"] == pdec.DAILY_GATE_WITHHELD
    # A refusal by the writer echoes the refusing gate's own word.
    refused = _record(cand, withheld, tmp_path)
    assert refused["status"] == pdec.DAILY_GATE_WITHHELD
    # The intraday words are unchanged, and all four are one vocabulary.
    assert pdec.GATE_ELIGIBLE == "GOVERNED_INTRADAY_DECISION_ELIGIBLE"
    assert pdec.GATE_WITHHELD == "INTRADAY_DECISION_WITHHELD"
    assert set(pdec.GATE_VERDICT_VOCAB) == {
        pdec.GATE_ELIGIBLE, pdec.GATE_WITHHELD,
        pdec.DAILY_GATE_ELIGIBLE, pdec.DAILY_GATE_WITHHELD}


def test_41_non_terminal_manifest_is_never_governed():
    for state in ("RUNNING_RESEARCH", "BLOCKED", "FAILED", "INCONSISTENT", ""):
        _, gate = _daily(man=_man(state=state))
        assert gate["eligible"] is False, state
        assert pdec.WR_DAILY_MANIFEST_NOT_GOVERNED in gate["withheld_reason_codes"]
    # Both of the cycle owner's OWN terminal-complete words are governed
    # evidence — a documented forward-evidence gap is attention-level, and does
    # not invalidate the session's portfolio conclusion.
    assert set(pdec.DAILY_TERMINAL_COMPLETE_STATES) == {
        "COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP"}
    for state in pdec.DAILY_TERMINAL_COMPLETE_STATES:
        assert state in drc._COMPLETED, state
        _, ok = _daily(man=_man(state=state))
        assert ok["eligible"] is True, (state, ok["withheld_reason_codes"])


def test_42_delegation_requires_the_system_token():
    out = pdec.govern_daily_cycle_decision(confirm=None, drc_manifest=_man())
    assert out["recorded"] is False
    assert out["status"] == "GOVERNED_DECISION_CONFIRMATION_REQUIRED"
    assert out["confirm_required_token"] == pdec.GOVERNED_DECISION_CONFIRM_TOKEN
    # The system token is deliberately NOT the operator approval token.
    assert pdec.GOVERNED_DECISION_CONFIRM_TOKEN != pdec.CONFIRM_TOKEN


def test_43_persisted_daily_row_retires_the_legacy_projection(tmp_path):
    wf = {"research_cycle_state": {"governed_research_evidence_current": True,
                                   "governed_manifest_run_id": RUN_ID}}
    before = pdec.load_governed_portfolio_decision(
        workflow=wf, reassessment=_reas(), proposal_summary=_summ(),
        constrained=_con(), active_book_id=BOOK, decision_dir=tmp_path)
    assert before["projected_daily_cycle_present"] is True
    assert before["legacy_daily_projection_suppressed"] is False
    assert before["persisted"] is False

    cand, gate = _daily()
    rec = _record(cand, gate, tmp_path)["record"]
    after = pdec.load_governed_portfolio_decision(
        workflow=wf, reassessment=_reas(), proposal_summary=_summ(),
        constrained=_con(), active_book_id=BOOK, decision_dir=tmp_path)
    assert after["legacy_daily_projection_suppressed"] is True
    assert after["projected_daily_cycle_present"] is False
    assert after["persisted"] is True
    assert after["record_id"] == rec["record_id"]
    assert after["decision"] == before["decision"], (
        "the ledger row must agree with the projection it retires")


def test_44_delegation_reports_but_never_persists_in_the_producer(tmp_path):
    """The producer's report block is a REPORT; the decision lives in the ledger."""
    calls = {}

    def _fake():
        calls["hit"] = True
        return {"verdict": pdec.GATE_ELIGIBLE, "eligible": True, "recorded": True,
                "record": {"record_id": "gdec_x", "decision": pdec.GD_NO_CHANGE,
                           "decided_at": STAMP_LATE,
                           "manual_review_required": False},
                "persist_status": "CREATED",
                "gate": {"checks_passed": 19, "checks_total": 19,
                         "withheld_reason_codes": []}}

    block, warns = drc._delegate_governed_decision(
        manifest=_man(), drc_dir=str(tmp_path), governed_decision_fn=_fake)
    assert calls.get("hit") is True
    assert block["delegated"] is True
    assert block["owner"] == "api.portfolio_decision"
    assert block["decision_owner_is_authority"] is True
    assert block["read_the_decision_from"] == "api.portfolio_decision"
    assert block["decision_record_id"] == "gdec_x"
    assert warns == []
    # The producer wrote no decision store of its own.
    assert not list(Path(tmp_path).glob("governed_*.json"))


def test_45_delegation_never_breaks_a_completed_research_run(tmp_path):
    def _boom():
        raise RuntimeError("decision owner unavailable")

    block, warns = drc._delegate_governed_decision(
        manifest=_man(), drc_dir=str(tmp_path), governed_decision_fn=_boom)
    assert block["recorded"] is False
    assert "decision owner unavailable" in block["error"]
    assert warns and "research outputs remain valid" in warns[0]


def test_46_a_non_terminal_run_delegates_nothing(tmp_path):
    block, warns = drc._delegate_governed_decision(
        manifest=_man(state="BLOCKED"), drc_dir=str(tmp_path),
        governed_decision_fn=lambda: pytest.fail("must not be called"))
    assert block["delegated"] is False
    assert warns == []


def test_47_the_cycle_exposes_the_delegation_seam():
    sig = inspect.signature(drc.run_daily_research_cycle)
    assert "governed_decision_fn" in sig.parameters
    # And the decision owner exposes every store as an injectable seam, so a
    # hermetic caller can never fall through to a production root.
    gsig = inspect.signature(pdec.govern_daily_cycle_decision)
    for seam in ("decision_dir", "reallocation_dir", "hoc_dir",
                 "reassessment_dir", "loaders"):
        assert seam in gsig.parameters, seam


def test_49_a_stale_live_proposal_never_enters_a_no_change_identity(tmp_path):
    """R54.2.3.2 parity, caught by replaying the REAL 2026-09-02 manifest.

    Production's live reallocation key held a proposal from an earlier event
    cycle while the governed manifest recorded `NOT_REQUIRED`. Sourcing the
    proposal identity from the live store would have laundered that stale target
    into a CURRENT_NO_CHANGE decision. The manifest is the authority on which
    proposal the run actually built.
    """
    stale = _summ(outcome=cr.OUTCOME_PROPOSAL_READY, with_proposal=True,
                  reallocation_proposal_hash="PR_FROM_AN_EARLIER_CYCLE",
                  reallocation_proposal_id="prop_stale")
    cand, gate = _daily(summ=stale, con=_con(outcome=None))
    assert cand["decision"] == pdec.GD_NO_CHANGE
    assert cand["identity"]["proposal_hash"] is None
    assert cand["identity"]["proposal_id"] is None
    assert cand["identity"]["target_outcome"] is None
    assert gate["eligible"] is True, gate["withheld_reason_codes"]
    rec = _record(cand, gate, tmp_path)["record"]
    assert rec["identity"]["proposal_hash"] is None
    assert rec["position_recommendations"] == []
    assert rec["manual_review_required"] is False


def test_50_a_change_bound_to_a_foreign_proposal_is_refused():
    """The live-store proposal must BE the manifest's before a CHANGE is admitted."""
    kit = _change_kit()
    kit["summ"] = _summ(outcome=cr.OUTCOME_PROPOSAL_READY, with_proposal=True,
                        reallocation_proposal_hash="PR_FOREIGN")
    kit["man"] = _man(portfolio_reassessment_state="PROPOSAL_READY",
                      reallocation_proposal_id="prop_1",
                      reallocation_proposal_hash="PR1",
                      reallocation_proposal_state="READY")
    cand, gate = _daily(**kit)
    # Identity takes the MANIFEST's proposal, so the candidate is coherent...
    assert cand["identity"]["proposal_hash"] == "PR1"
    assert gate["eligible"] is True
    # ...and a manifest that names NO proposal cannot support a CHANGE at all.
    kit["man"] = _man(portfolio_reassessment_state="PROPOSAL_READY",
                      reallocation_proposal_state="READY")
    _, bad = _daily(**kit)
    assert bad["eligible"] is False
    assert pdec.WR_TARGET_IDENTITY in bad["withheld_reason_codes"]


def test_48_producer_label_is_reported_truthfully(tmp_path):
    cand, gate = _daily()
    _record(cand, gate, tmp_path)
    gov = pdec.load_governed_portfolio_decision(
        active_book_id=BOOK, decision_dir=tmp_path)
    authority = pdec.resolve_decision_authority(
        assessment={}, proposal_summary=_summ(), governed_decision=gov)
    assert authority["current_authoritative_decision_producer"] == \
        pdec.PROV_GOVERNED_DAILY_CYCLE
    assert authority["producer_label"] == "Daily DRC"
    assert set(authority["producer_vocabulary"]) == {
        pdec.PROV_GOVERNED_DAILY_CYCLE, pdec.PROV_GOVERNED_INTRADAY}
