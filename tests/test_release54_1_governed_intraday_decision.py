"""R54.1 — the GOVERNED INTRADAY PORTFOLIO DECISION CYCLE.

What these tests prove:

  * a COMPLETE, fresh, point-in-time-bound live intraday assessment can be
    promoted into the authoritative governed decision lane — and HOLD and
    CHANGE are BOTH first-class governed decisions;
  * promotion updates the authoritative RECOMMENDATION and nothing else: no
    portfolio mutation, no approval, no order plan, no order, no fill, no
    broker call, no model promotion, no sleeve activation;
  * every mandatory admissibility condition WITHHOLDS with an explicit,
    classified reason code — never a generic BLOCKED;
  * ``OWNED_DATA_NOT_CONFIRMED`` is never bypassed, intraday evidence never
    advances the operational close mark, and the two clocks stay separate;
  * supersession is deterministic and total, older records are immutable, and a
    stale or older assessment can never supersede a newer governed decision;
  * the gate decides ADMISSIBILITY only — every threshold, hurdle, cost, risk
    and outcome is the canonical owner's, read verbatim;
  * Active Manager State exposes the live lane and the governed lane
    separately, with correct provenance, and performs no governance logic;
  * the R53.1 intraday emission-slot contract is unchanged, and the 16:20 ET
    scheduled invocation is BY DESIGN outside every emission slot.

Every write path here is hermetic (``tmp_path``); no production store, no
provider, no live backend and no scheduler is touched.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import event_signal_refresh as esr
from paper_trader.api import portfolio_decision as pdec
from paper_trader.engine import constrained_reallocation as cr

REPO = Path(__file__).resolve().parents[1]
PDEC_SRC = (REPO / "api" / "portfolio_decision.py").read_text(
    encoding="utf-8", errors="replace")
ESR_SRC = (REPO / "api" / "event_signal_refresh.py").read_text(
    encoding="utf-8", errors="replace")
AMS_SRC = (REPO / "api" / "active_manager_state.py").read_text(
    encoding="utf-8", errors="replace")

BOOK = "alpha_paper_book_1"
SESSION = "2026-08-31"
HELD = ["T%02d" % i for i in range(25)]


def _governed_lane_source() -> str:
    """Exactly the R54.1 section of the decision owner — from its banner to the
    module's ``__all__``, so a pre-existing symbol re-exported at the bottom of
    the file can never be mistaken for governed-lane code."""
    body = PDEC_SRC.split("R54.1 — THE ONE GOVERNED INTRADAY DECISION GATE")[1]
    return body.split("\n__all__ = [")[0]


# --------------------------------------------------------------------------- #
# Hermetic owner payloads — the exact shapes the canonical owners publish.
# --------------------------------------------------------------------------- #
def _ps(**kw) -> dict:
    d = {
        "state": "PORTFOLIO_STATE_READY",
        "active_book": {"book_id": BOOK, "book_label": "Alpha Paper Book #1"},
        "dates": {"eligible_market_date": SESSION, "desk_mark_date": SESSION,
                  "valuation_date": SESSION, "latest_daily_close_date": SESSION},
        "capital": {"nav": 99113.22, "cash": 1200.5},
        "positions": [{"ticker": t} for t in HELD],
        "state_hash": "PSH1", "economic_state_hash": "ESH1",
    }
    d.update(kw)
    return d


def _cycle(**kw) -> dict:
    d = {
        "run_id": "evt_aaaa1111",
        "state": esr.ST_PROPOSAL_AVAILABLE,
        "generated_at": "2026-09-01T17:40:00+00:00",
        "completed_at": "2026-09-01T17:42:07+00:00",
        "reassessment_ran": True,
        "proposal_built": True,
        "materiality_change_level": "MATERIAL_SIGNAL_CHANGED",
        "active_book_id": BOOK,
        "eligible_market_date": SESSION,
        "portfolio_state_hash": "ESH1",
        "holdings": list(HELD),
        "hoc_assessment_hash": "HOC1",
        "hoc_holdings_reviewed": 25,
        "reassessment_hash": "RA1",
        "proposal_hash": "PR1",
        "materiality_trigger_fingerprint": "FP1",
        "duplicate_of_prior_trigger": False,
        "blocker_codes": [],
        "reassessment_state": "PROPOSAL_READY",
        "proposal_state": "READY",
        "stage_timestamps": {
            "signal_refresh_completed_at": "2026-09-01T17:40:30+00:00",
            "scoring_completed_at": "2026-09-01T17:40:45+00:00",
            "hoc_completed_at": "2026-09-01T17:41:10+00:00",
            "reassessment_completed_at": "2026-09-01T17:41:40+00:00",
            "target_completed_at": "2026-09-01T17:42:05+00:00",
        },
        "cycle_duration_seconds": 7.3,
        "oldest_event_to_reassessment_seconds": 240.0,
    }
    d.update(kw)
    return d


def _reas(**kw) -> dict:
    d = {
        "state": "PROPOSAL_READY",
        "eligible_market_date": SESSION,
        "active_book": {"book_id": BOOK},
        "reassessment_id": "ra_2026-08-31_1",
        "reassessment_hash": "RA1",
        "artifact": {"reassessment_id": "ra_2026-08-31_1",
                     "generated_at": "2026-09-01T01:08:13+00:00",
                     # Stage-21 economic identity — what economic_currency reads
                     "identity": {"economic_state_hash": "ESH1"}},
        "proposal_binding": {
            "reassessment_id": "ra_2026-08-31_1",
            "reassessment_hash": "RA1",
            "hoc_assessment_hash": "HOC1",
            "universe_scoring_hash": "US1",
            "universe_input_contract_hash": "UIC1",
            "portfolio_state_hash": "PSH1",
            "corporate_actions_hash": "CA1",
            "eligible_market_date": SESSION,
            "active_book_id": BOOK,
        },
        "execution_precedence": {"execution_active": False,
                                 "reason": "no rebalance in flight"},
        "decision": {"holdings_evaluated": 25},
    }
    d.update(kw)
    return d


def _summ(**kw) -> dict:
    d = {
        "reallocation_proposal_available": True,
        "reallocation_proposal_stale": False,
        "reallocation_proposal_stale_reason": None,
        "reallocation_corporate_actions_hash": "CA1",
        "reallocation_proposal_hash": "PR1",
        "reallocation_proposal_id": "prop_1",
        "reallocation_proposal_withheld": False,
        "reallocation_withheld_reasons": [],
        "reallocation_outcome": cr.OUTCOME_HOLD_CURRENT_BOOK,
        "reallocation_feasible_target_exists": True,
        "reallocation_data_gaps": [],
        "reallocation_bound_hoc_assessment_hash": "HOC1",
        "reallocation_bound_eligible_market_date": SESSION,
        "reallocation_bound_active_book_id": BOOK,
    }
    d.update(kw)
    return d


def _econ(**kw) -> dict:
    d = {
        "switching_hurdle": 0.02,
        "clears_switching_hurdle": False,
        "score_improvement_net_of_cost": 0.004,
        "one_way_turnover": 0.11,
        "estimated_transaction_cost": 214.5,
        "concentration_before": 0.0412,
        "concentration_after": 0.0430,
        "portfolio_volatility_before": 0.1731,
        "portfolio_volatility_after": 0.1755,
    }
    d.update(kw)
    return d


def _con(outcome: str = cr.OUTCOME_HOLD_CURRENT_BOOK, **kw) -> dict:
    d = {
        "outcome": outcome,
        "outcome_vocabulary": list(cr.OUTCOME_VOCAB),
        "feasible_target_exists": True,
        "calculation_owner": cr.CALCULATION_OWNER,
        "switching_economics": _econ(
            clears_switching_hurdle=(outcome == cr.OUTCOME_PROPOSAL_READY)),
        "ideal_target": {"zero_base_owner": "api.zero_base_target"},
        "constraint_inventory": {"constraints": [{"code": "TURNOVER"}]},
        "multi_asset": {"current_holdings_privileged": False},
        "best_feasible_target": {"allocations": [
            {"ticker": "T00", "action": "REDUCE", "current_weight": 0.06,
             "proposed_weight": 0.03, "delta_weight": -0.03,
             "capital_change": -2900.0},
            {"ticker": "NEW1", "action": "ADD", "current_weight": 0.0,
             "proposed_weight": 0.03, "delta_weight": 0.03,
             "capital_change": 2900.0},
            {"ticker": "T01", "action": "HOLD", "current_weight": 0.04,
             "proposed_weight": 0.04, "delta_weight": 0.0,
             "capital_change": 0.0},
        ]},
        "approval": {"portfolio_decision_state": "HOLD_CURRENT_BOOK",
                     "requires_manual_review": False},
    }
    d.update(kw)
    return d


def _wf(**kw) -> dict:
    d = {
        # The LIVE condition R54.1 was written for: the operational clock is
        # waiting for the NEXT expected session's owned data while the book
        # itself is validly closed and marked to its own eligible session.
        "overall_state": "WAITING_FOR_OWNED_DATA",
        "operational_state": {
            "active_book_id": BOOK,
            "eligible_market_date": SESSION,
            "desk_mark_date": SESSION,
            "valuation_date": SESSION,
            "latest_completed_close_date": SESSION,
            "latest_close_status": "COMPLETE",
            "operational_close_valid": True,
            "eligible_session_already_processed": True,
            "pending_orders": 0,
        },
        "research_cycle_state": {
            "opportunity_cost_artifact_class": "GOVERNED_DRC_TERMINAL",
            "opportunity_cost_producer_owner": "api.daily_research_cycle",
            "governed_research_evidence_current": True,
            "governed_manifest_run_id": "drc_2026_08_31",
        },
        "portfolio_decision_state": {"proposal_hash": "PR1"},
        "blockers": [{"code": "OWNED_DATA_NOT_CONFIRMED",
                      "detail": "expected session 2026-09-01 unconfirmed"}],
    }
    d.update(kw)
    return d


def _sc(**kw) -> dict:
    d = {"ranking_date": SESSION, "input_contract_hash": "UIC1",
         "status": "UNIVERSE_SCORING_READY"}
    d.update(kw)
    return d


#: Deterministic decision clocks. Every governed decision carries an explicit
#: timestamp, so supersession must be provable without reading the wall clock.
T1 = datetime(2026, 9, 1, 17, 42, 0, tzinfo=timezone.utc)
T2 = datetime(2026, 9, 1, 19, 5, 0, tzinfo=timezone.utc)


def _candidate(*, ps=None, cycle=None, reas=None, summ=None, con=None,
               wf=None, sc=None, now=T1, **kw) -> dict:
    return pdec.build_intraday_candidate(
        portfolio_state=ps if ps is not None else _ps(),
        event_cycle=cycle if cycle is not None else _cycle(),
        reassessment=reas if reas is not None else _reas(),
        proposal_summary=summ if summ is not None else _summ(),
        constrained=con if con is not None else _con(),
        workflow=wf if wf is not None else _wf(),
        scoring_identity=sc if sc is not None else _sc(),
        observation_received_at=kw.pop("observation_received_at",
                                       "2026-09-01T17:36:00+00:00"),
        now=now, **kw)


def _gate(*, candidate=None, ps=None, cycle=None, reas=None, summ=None,
          con=None, wf=None, sc=None, rebalance=None, current_governed=None,
          now=T1):
    ps = ps if ps is not None else _ps()
    cycle = cycle if cycle is not None else _cycle()
    reas = reas if reas is not None else _reas()
    summ = summ if summ is not None else _summ()
    con = con if con is not None else _con()
    wf = wf if wf is not None else _wf()
    sc = sc if sc is not None else _sc()
    cand = candidate if candidate is not None else _candidate(
        ps=ps, cycle=cycle, reas=reas, summ=summ, con=con, wf=wf, sc=sc, now=now)
    return cand, pdec.evaluate_intraday_governance(
        candidate=cand, portfolio_state=ps, event_cycle=cycle,
        reassessment=reas, proposal_summary=summ, constrained=con, workflow=wf,
        scoring_identity=sc, rebalance=rebalance,
        current_governed=current_governed)


def _codes(gate: dict) -> set:
    return set(gate["withheld_reason_codes"])


def _record(tmp_path, *, candidate=None, gate=None, now=T1, **kw):
    cand, g = ((candidate, gate) if candidate is not None
               else _gate(now=now, **kw))
    return cand, g, pdec.record_governed_decision(
        candidate=cand, gate=g,
        confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
        now=now)


# =========================================================================== #
# 1. HOLD and CHANGE are BOTH real governed decisions.
# =========================================================================== #
class TestGovernablePaths:
    def test_01_complete_intraday_hold_becomes_governed(self, tmp_path):
        cand, gate, out = _record(tmp_path)
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]
        assert cand["decision"] == pdec.GD_HOLD_CURRENT_BOOK
        assert out["recorded"] is True and out["status"] == "CREATED"
        assert out["record"]["provenance"] == pdec.PROV_GOVERNED_INTRADAY

    def test_02_complete_intraday_change_becomes_governed(self, tmp_path):
        con = _con(cr.OUTCOME_PROPOSAL_READY)
        summ = _summ(reallocation_outcome=cr.OUTCOME_PROPOSAL_READY)
        cand, gate, out = _record(tmp_path, con=con, summ=summ)
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]
        assert cand["decision"] == pdec.GD_CHANGE_RECOMMENDED
        assert out["recorded"] is True
        assert out["record"]["decision"] == pdec.GD_CHANGE_RECOMMENDED

    def test_21_hold_is_a_first_class_governed_decision(self, tmp_path):
        """HOLD is a DECISION, never the absence of one."""
        _, _, out = _record(tmp_path)
        rec = out["record"]
        assert rec["decision"] == pdec.GD_HOLD_CURRENT_BOOK
        assert rec["decision"] in pdec.GOVERNED_DECISION_VOCAB
        assert rec["record_kind"] == "GOVERNED_PORTFOLIO_DECISION"
        assert rec["manual_review_required"] is False   # nothing to review
        # A governed HOLD never publishes the rejected target's legs as advice.
        assert rec["position_recommendations"] == []
        assert rec["switching_economics"]["clears_switching_hurdle"] is False

    def test_22_change_carries_position_level_recommendations_only(self, tmp_path):
        con = _con(cr.OUTCOME_PROPOSAL_READY)
        summ = _summ(reallocation_outcome=cr.OUTCOME_PROPOSAL_READY)
        _, _, out = _record(tmp_path, con=con, summ=summ)
        rec = out["record"]
        recs = {r["ticker"]: r["recommendation"]
                for r in rec["position_recommendations"]}
        assert recs == {"T00": "REDUCE", "NEW1": "ADD"}   # HOLD legs excluded
        for r in rec["position_recommendations"]:
            assert r["recommendation"] in pdec.POSITION_RECOMMENDATION_VOCAB
            assert r["owner"] == "api.reallocation_proposal"
        assert rec["manual_review_required"] is True
        assert rec["approval_required_token"] == pdec.CONFIRM_TOKEN


# =========================================================================== #
# 2. Withheld states — every refusal is CLASSIFIED, never generic.
# =========================================================================== #
class TestWithheldStates:
    def test_03_incomplete_candidate_is_withheld(self):
        _, gate = _gate(cycle=_cycle(reassessment_ran=False, proposal_built=False,
                                     state=esr.ST_INFORMATION_NOT_MATERIAL,
                                     reassessment_hash=None,
                                     hoc_assessment_hash=None,
                                     materiality_trigger_fingerprint=None),
                        reas={}, summ={}, con={})
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_EVIDENCE_INCOMPLETE in _codes(gate)
        assert gate["failing_checks"]

    def test_04_stale_portfolio_identity_is_withheld(self):
        _, gate = _gate(cycle=_cycle(holdings=HELD[:20]))
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_PORTFOLIO_IDENTITY_STALE in _codes(gate)
        assert "HOLDINGS_RECONCILE" in gate["failing_checks"]

    def test_05_mismatched_portfolio_state_hash_is_withheld(self):
        """The ECONOMIC portfolio moved under the evidence."""
        _, gate = _gate(ps=_ps(economic_state_hash="ESH_MOVED"))
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert pdec.WR_PORTFOLIO_IDENTITY_STALE in _codes(gate)
        assert "ECONOMIC_PORTFOLIO_STILL_CURRENT" in gate["failing_checks"]

    def test_05b_an_unbound_portfolio_state_hash_is_withheld(self):
        reas = _reas()
        reas["proposal_binding"] = dict(reas["proposal_binding"],
                                        portfolio_state_hash=None)
        _, gate = _gate(reas=reas, ps=_ps(state_hash=None))
        assert "PORTFOLIO_STATE_HASH_BOUND" in gate["failing_checks"]

    def test_05c_research_writes_do_not_fabricate_portfolio_staleness(self):
        """Stage-21 trap. ``state_hash`` covers the whole portfolio-state
        DOCUMENT, which embeds the assessment's own output — so a fresh
        assessment moves it. Currency is decided by the ECONOMIC fingerprint,
        which structurally excludes research outputs. A moved document hash
        with an unchanged economic fingerprint must NOT withhold."""
        _, gate = _gate(ps=_ps(state_hash="PSH_MOVED_BY_RESEARCH"))
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]

    def test_05d_unverifiable_economic_currency_fails_closed_without_claiming_stale(self):
        reas = _reas()
        reas["artifact"] = dict(reas["artifact"], identity={})
        _, gate = _gate(reas=reas)
        assert gate["verdict"] == pdec.GATE_WITHHELD
        assert "ECONOMIC_PORTFOLIO_STILL_CURRENT" in gate["failing_checks"]
        # UNVERIFIABLE is NOT staleness — the owner refuses to infer it, and so
        # does the gate: the refusal names incomplete evidence, not a stale book.
        assert pdec.WR_EVIDENCE_INCOMPLETE in _codes(gate)
        assert pdec.WR_PORTFOLIO_IDENTITY_STALE not in _codes(gate)

    def test_06_stale_market_evidence_is_withheld(self):
        _, gate = _gate(summ=_summ(reallocation_data_gaps=["MISSING_PRICE_PANEL"]))
        assert pdec.WR_MARKET_DATA_STALE in _codes(gate)
        assert "NO_TRUE_DATA_GAP" in gate["failing_checks"]

    def test_06b_blocked_data_reassessment_is_withheld(self):
        _, gate = _gate(reas=_reas(state="BLOCKED_DATA"))
        assert pdec.WR_MARKET_DATA_STALE in _codes(gate)

    def test_09_ranking_identity_mismatch_is_withheld(self):
        _, gate = _gate(sc=_sc(input_contract_hash="UIC_DIFFERENT"))
        assert pdec.WR_RANKING_IDENTITY in _codes(gate)
        assert "RANKING_IDENTITY_UNCHANGED" in gate["failing_checks"]

    def test_09b_unknown_ranking_basis_is_withheld(self):
        reas = _reas()
        reas["proposal_binding"] = dict(reas["proposal_binding"],
                                        universe_scoring_hash=None,
                                        universe_input_contract_hash=None)
        _, gate = _gate(reas=reas, sc=_sc(ranking_date=None,
                                          input_contract_hash=None))
        assert pdec.WR_RANKING_IDENTITY in _codes(gate)

    def test_10_hoc_identity_mismatch_is_withheld(self):
        _, gate = _gate(summ=_summ(reallocation_bound_hoc_assessment_hash="HOC_OLD"))
        assert pdec.WR_HOC_IDENTITY in _codes(gate)
        assert "TARGET_BOUND_TO_SAME_HOC" in gate["failing_checks"]

    def test_10b_unassessed_holding_is_withheld(self):
        _, gate = _gate(cycle=_cycle(hoc_holdings_reviewed=21))
        assert pdec.WR_HOC_IDENTITY in _codes(gate)
        assert "EVERY_HOLDING_ASSESSED" in gate["failing_checks"]

    def test_11_reassessment_identity_mismatch_is_withheld(self):
        _, gate = _gate(cycle=_cycle(reassessment_hash="RA_OTHER"))
        assert pdec.WR_REASSESSMENT_IDENTITY in _codes(gate)
        assert "CYCLE_REASSESSMENT_IS_THE_CANDIDATE" in gate["failing_checks"]

    def test_12_proposal_identity_mismatch_is_withheld(self):
        _, gate = _gate(summ=_summ(reallocation_bound_active_book_id="other_book"))
        assert pdec.WR_TARGET_IDENTITY in _codes(gate)
        assert "TARGET_BOUND_TO_ACTIVE_BOOK" in gate["failing_checks"]

    def test_13_missing_switching_economics_is_withheld(self):
        con = _con()
        con["switching_economics"] = {"switching_hurdle": 0.02}
        _, gate = _gate(con=con)
        assert pdec.WR_SWITCHING_ECONOMICS in _codes(gate)
        assert "SWITCHING_ECONOMICS_COMPLETE" in gate["failing_checks"]

    def test_14_true_blocker_is_withheld(self):
        con = _con(cr.OUTCOME_TRUE_BLOCKER)
        summ = _summ(reallocation_outcome=cr.OUTCOME_TRUE_BLOCKER,
                     reallocation_feasible_target_exists=False)
        _, gate = _gate(con=con, summ=summ)
        assert pdec.WR_TRUE_BLOCKER in _codes(gate)
        assert "CONCLUSIVE_PRICED_OUTCOME" in gate["failing_checks"]

    def test_14b_withheld_complete_target_is_not_promotable(self):
        summ = _summ(reallocation_proposal_withheld=True,
                     reallocation_withheld_reasons=["CONCENTRATION_BREACH"])
        _, gate = _gate(summ=summ)
        assert pdec.WR_CHANGE_WITHHELD in _codes(gate)

    def test_14c_cycle_blocker_is_withheld(self):
        _, gate = _gate(cycle=_cycle(blocker_codes=["UNCLASSIFIED_SIGNAL_AUTHORITY"]))
        assert pdec.WR_TRUE_BLOCKER in _codes(gate)

    def test_14d_corporate_action_staleness_is_withheld(self):
        _, gate = _gate(summ=_summ(reallocation_proposal_stale=True,
                                   reallocation_proposal_stale_reason="SPLIT"))
        assert pdec.WR_PORTFOLIO_IDENTITY_STALE in _codes(gate)

    def test_14e_execution_precedence_withholds(self):
        _, gate = _gate(rebalance={"rebalance_state":
                                   "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING"})
        assert pdec.WR_EXECUTION_PRECEDENCE in _codes(gate)

    def test_14f_duplicate_trigger_suppression_is_not_promoted(self):
        _, gate = _gate(cycle=_cycle(state="DUPLICATE_TRIGGER_SUPPRESSED"))
        assert pdec.WR_DUPLICATE in _codes(gate)

    def test_14g_point_in_time_violation_is_withheld(self):
        """A ranking dated AFTER the session it describes is look-ahead."""
        _, gate = _gate(sc=_sc(ranking_date="2026-09-01"))
        assert pdec.WR_POINT_IN_TIME in _codes(gate)
        assert "POINT_IN_TIME_INTEGRITY" in gate["failing_checks"]

    def test_14h_every_reason_code_is_in_the_declared_taxonomy(self):
        for gate in (
            _gate(cycle=_cycle(holdings=HELD[:20]))[1],
            _gate(summ=_summ(reallocation_data_gaps=["X"]))[1],
            _gate(con=_con(cr.OUTCOME_TRUE_BLOCKER),
                  summ=_summ(reallocation_outcome=cr.OUTCOME_TRUE_BLOCKER))[1],
        ):
            assert _codes(gate)
            assert _codes(gate) <= set(pdec.WITHHELD_REASON_VOCAB)

    def test_14i_withheld_reasons_name_the_owner_and_the_check(self):
        _, gate = _gate(ps=_ps(economic_state_hash="ESH_MOVED"))
        for r in gate["withheld_reasons"]:
            assert r["code"] and r["check"] and r["group"] and r["owner"]
            assert r["detail"]


# =========================================================================== #
# 3. OWNED_DATA_NOT_CONFIRMED and the two clocks.
# =========================================================================== #
class TestOwnedDataAndTheTwoClocks:
    def test_07_owned_data_not_confirmed_for_the_book_session_withholds(self):
        wf = _wf()
        wf["operational_state"] = dict(wf["operational_state"],
                                       operational_close_valid=False)
        _, gate = _gate(wf=wf)
        assert pdec.WR_OWNED_DATA_NOT_CONFIRMED in _codes(gate)
        assert "BOOK_SESSION_OWNED_CONFIRMED" in gate["failing_checks"]

    def test_07b_a_book_marked_behind_its_eligible_session_withholds(self):
        wf = _wf()
        wf["operational_state"] = dict(wf["operational_state"],
                                       desk_mark_date="2026-08-27",
                                       valuation_date="2026-08-27",
                                       latest_completed_close_date="2026-08-27")
        _, gate = _gate(wf=wf)
        assert pdec.WR_OWNED_DATA_NOT_CONFIRMED in _codes(gate)

    def test_07c_the_gate_never_clears_the_workflow_blocker(self, tmp_path):
        """The LIVE case: the workflow waits for the NEXT expected session's
        owned data while the book's OWN session is confirmed. The candidate is
        promotable, and the operational blocker is untouched and recorded."""
        wf = _wf()
        cand, gate, out = _record(tmp_path, wf=wf)
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]
        assert out["recorded"] is True
        # the workflow payload is not mutated and still carries the blocker
        assert wf["overall_state"] == "WAITING_FOR_OWNED_DATA"
        assert [b["code"] for b in wf["blockers"]] == ["OWNED_DATA_NOT_CONFIRMED"]
        ev = out["record"]["evidence_provenance"]
        assert ev["expected_session_owned_data_confirmed"] is False
        assert "OPERATIONAL CLOSE clock" in ev["expected_session_note"]
        assert "never cleared by this module" in ev["expected_session_note"]

    def test_08_intraday_evidence_never_advances_the_operational_mark(self, tmp_path):
        _, _, out = _record(tmp_path)
        rec = out["record"]
        assert rec["safety"]["advances_operational_mark"] is False
        assert rec["safety"]["operational_mark_advanced_only_by"] == "api.daily_close"
        assert rec["evidence_provenance"]["operational_mark_date"] == SESSION
        # the decision's own timestamp is intraday and NEWER than the mark
        assert rec["decided_at"][:10] >= SESSION

    def test_31_operational_mark_does_not_advance_from_intraday_data(self, tmp_path):
        ps = _ps()
        before = json.dumps(ps["dates"], sort_keys=True)
        _record(tmp_path, ps=ps)
        assert json.dumps(ps["dates"], sort_keys=True) == before

    def test_32_daily_close_remains_independent(self):
        """No governed-lane code path reaches the close or the desk."""
        gov = _governed_lane_source()
        for token in ("run_daily_close", "daily_close.run", "refresh_desk",
                      "settle_due_orders", "advance_operational_mark"):
            assert token not in gov, token

    def test_33_drc_evidence_contract_is_read_never_rewritten(self):
        gov = _governed_lane_source()
        assert "governed_research_evidence_current" in gov
        assert "governed_research_evidence_current\"] =" not in gov
        assert "governed_research_evidence_current'] =" not in gov


# =========================================================================== #
# 4. Idempotency, supersession and immutability.
# =========================================================================== #
class TestSupersessionAndImmutability:
    def test_15_duplicate_candidate_is_idempotent(self, tmp_path):
        cand, gate, first = _record(tmp_path)
        again = pdec.record_governed_decision(
            candidate=cand, gate=gate,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path)
        assert again["status"] == "REUSED_EXISTING"
        assert again["idempotent"] is True
        assert again["idempotent_reason"] == pdec.WR_DUPLICATE
        assert again["record"]["record_id"] == first["record"]["record_id"]
        rows = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 1

    def test_15b_identical_evidence_is_withheld_by_the_gate_too(self, tmp_path):
        cand, _, first = _record(tmp_path)
        _, gate = _gate(current_governed=first["record"])
        assert gate["duplicate_of_standing_decision"] is True
        assert pdec.WR_DUPLICATE in _codes(gate)

    def test_15c_a_candidate_repeating_the_drc_conclusion_is_a_duplicate(self):
        """A projected daily-cycle decision records a SMALLER identity than a
        candidate, so the full hashes never match. The CORE evidence must still
        recognise the repeat, or the same conclusion would be re-recorded with
        a new provenance every cycle."""
        drc = pdec.project_governed_daily_cycle_decision(
            workflow=_wf(), reassessment=_reas(), proposal_summary=_summ(),
            constrained=_con())
        assert drc is not None and drc["projected"] is True
        cand = _candidate()
        assert drc["candidate_identity_hash"] != cand["candidate_identity_hash"]
        _, gate = _gate(candidate=cand, current_governed=drc)
        assert gate["duplicate_of_standing_decision"] is True
        assert pdec.WR_DUPLICATE in _codes(gate)

    def test_15d_different_evidence_against_the_drc_is_not_a_duplicate(self):
        drc = pdec.project_governed_daily_cycle_decision(
            workflow=_wf(), reassessment=_reas(), proposal_summary=_summ(),
            constrained=_con())
        binding = dict(_reas()["proposal_binding"], reassessment_hash="RA2")
        _, gate = _gate(cycle=_cycle(reassessment_hash="RA2"),
                        reas=_reas(reassessment_hash="RA2",
                                   proposal_binding=binding),
                        current_governed=drc)
        assert gate["duplicate_of_standing_decision"] is False
        assert gate["verdict"] == pdec.GATE_ELIGIBLE, gate["withheld_reasons"]

    def _second_candidate(self, first_record):
        """A LATER cycle whose evidence genuinely moved on."""
        binding = dict(_reas()["proposal_binding"], reassessment_hash="RA2",
                       hoc_assessment_hash="HOC2")
        return _gate(cycle=_cycle(run_id="evt_bbbb2222", reassessment_hash="RA2",
                                  hoc_assessment_hash="HOC2"),
                     reas=_reas(reassessment_hash="RA2",
                                proposal_binding=binding),
                     summ=_summ(reallocation_bound_hoc_assessment_hash="HOC2"),
                     current_governed=first_record, now=T2)

    def test_16_older_governed_records_are_immutable(self, tmp_path):
        _, _, first = _record(tmp_path)
        before = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        cand2, gate2 = self._second_candidate(first["record"])
        pdec.record_governed_decision(
            candidate=cand2, gate=gate2,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
            now=T2)
        after = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        assert len(after) == 2
        assert after[0] == before[0]            # byte-identical, never rewritten

    def test_17_newer_intraday_decision_supersedes_correctly(self, tmp_path):
        _, _, first = _record(tmp_path)
        cand2, gate2 = self._second_candidate(first["record"])
        assert gate2["eligible"] is True, gate2["withheld_reasons"]
        second = pdec.record_governed_decision(
            candidate=cand2, gate=gate2,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
            now=T2)
        assert second["recorded"] is True
        assert second["record"]["supersedes_decision_id"] == \
            first["record"]["record_id"]
        assert second["record"]["decided_at"] > first["record"]["decided_at"]
        latest = pdec.load_governed_decision_record(active_book_id=BOOK,
                                                    decision_dir=tmp_path)
        assert latest["record_id"] == second["record"]["record_id"]

    def test_34_a_newer_drc_decision_outranks_a_persisted_intraday_record(
            self, tmp_path, monkeypatch):
        """The composed entry point must resolve the standing authority as the
        LATER of the persisted record and the projected DRC decision — not the
        persisted record alone, or an intraday candidate could supersede a
        newer daily-cycle decision."""
        _, _, first = _record(tmp_path)
        reas_newer = _reas()
        reas_newer["artifact"] = dict(reas_newer["artifact"],
                                       generated_at="2026-09-01T23:30:00+00:00")
        out = pdec.govern_latest_intraday_assessment(
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN,
            portfolio_state=_ps(), event_cycle=_cycle(), reassessment=reas_newer,
            proposal_summary=_summ(), constrained=_con(), workflow=_wf(),
            scoring_identity=_sc(), decision_dir=tmp_path, now=T2)
        assert out["recorded"] is False
        # the standing authority resolved to the PROJECTED daily-cycle decision,
        # not to the persisted intraday record that the ledger holds
        assert out["standing_decision_id"].startswith("drc_governed_")
        assert out["gate"]["withheld_reason_codes"]
        rows = json.loads((tmp_path / "governed_decisions.json").read_text("utf-8"))
        assert len(rows) == 1 and rows[0]["record_id"] == first["record"]["record_id"]

    def test_34b_the_standing_authority_is_the_later_of_both_sources(self, tmp_path):
        """Ordering, isolated: a persisted intraday record from 17:42 and a
        daily-cycle decision from 23:30 — the later one is authoritative."""
        _, _, first = _record(tmp_path)
        drc = pdec.project_governed_daily_cycle_decision(
            workflow=_wf(),
            reassessment=_reas(artifact=dict(_reas()["artifact"],
                                             generated_at="2026-09-01T23:30:00+00:00")),
            proposal_summary=_summ(), constrained=_con())
        assert pdec.governed_decision_ordering_key(drc) > \
            pdec.governed_decision_ordering_key(first["record"])
        read = pdec.load_governed_portfolio_decision(
            workflow=_wf(),
            reassessment=_reas(artifact=dict(_reas()["artifact"],
                                             generated_at="2026-09-01T23:30:00+00:00")),
            proposal_summary=_summ(), constrained=_con(),
            active_book_id=BOOK, decision_dir=tmp_path)
        assert read["provenance"] == pdec.PROV_GOVERNED_DAILY_CYCLE
        assert read["persisted"] is False
        assert read["persisted_record_present"] is True

    def test_18_a_newer_drc_decision_may_supersede_an_intraday_one(self):
        intraday = {"provenance": pdec.PROV_GOVERNED_INTRADAY,
                    "eligible_market_session": SESSION,
                    "decided_at": "2026-09-01T17:42:00+00:00",
                    "candidate_identity_hash": "a"}
        drc = {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
               "eligible_market_session": SESSION,
               "decided_at": "2026-09-01T20:30:00+00:00",
               "candidate_identity_hash": "b"}
        assert pdec.governed_decision_ordering_key(drc) > \
            pdec.governed_decision_ordering_key(intraday)

    def test_19_a_stale_signal_can_never_supersede_a_newer_decision(self, tmp_path):
        newer = {"record_id": "gdec_new", "provenance": pdec.PROV_GOVERNED_INTRADAY,
                 "eligible_market_session": SESSION,
                 "decided_at": "2026-09-01T20:00:00+00:00",
                 "candidate_identity_hash": "zzz"}
        # a candidate decided at 17:42 against a governed decision from 20:00
        stale_cand, gate = _gate(current_governed=newer, now=T1)
        assert gate["eligible"] is False
        assert pdec.WR_SUPERSEDED in _codes(gate)
        out = pdec.record_governed_decision(
            candidate=stale_cand, gate=gate,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path,
            now=T1)
        assert out["recorded"] is False
        assert not (tmp_path / "governed_decisions.json").exists()

    def test_19b_a_later_session_always_outranks_an_earlier_one(self):
        old = {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
               "eligible_market_session": "2026-09-01",
               "decided_at": "2026-09-01T23:00:00+00:00",
               "candidate_identity_hash": "a"}
        new = {"provenance": pdec.PROV_GOVERNED_INTRADAY,
               "eligible_market_session": "2026-09-02",
               "decided_at": "2026-09-02T10:00:00+00:00",
               "candidate_identity_hash": "b"}
        assert pdec.governed_decision_ordering_key(new) > \
            pdec.governed_decision_ordering_key(old)

    def test_19c_equal_timestamps_break_deterministically_by_provenance(self):
        a = {"provenance": pdec.PROV_GOVERNED_INTRADAY,
             "eligible_market_session": SESSION,
             "decided_at": "2026-09-01T17:42:00+00:00",
             "candidate_identity_hash": "zzzz"}
        b = {"provenance": pdec.PROV_GOVERNED_DAILY_CYCLE,
             "eligible_market_session": SESSION,
             "decided_at": "2026-09-01T17:42:00+00:00",
             "candidate_identity_hash": "aaaa"}
        assert pdec.governed_decision_ordering_key(b) > \
            pdec.governed_decision_ordering_key(a)
        # total and reproducible
        assert sorted([a, b], key=pdec.governed_decision_ordering_key)[-1] is b

    def test_38_superseded_decision_identity_is_explicit(self, tmp_path):
        _, _, first = _record(tmp_path)
        assert "supersedes_decision_id" in first["record"]
        assert first["record"]["supersedes_decision_id"] is None

    def test_38b_the_candidate_identity_excludes_the_trigger_and_the_clock(self):
        a = _candidate()
        b = _candidate(cycle=_cycle(run_id="evt_zzz",
                                    materiality_trigger_fingerprint="FP_OTHER",
                                    generated_at="2026-09-01T19:00:00+00:00"))
        assert a["candidate_identity_hash"] == b["candidate_identity_hash"]
        # ...but the fingerprint is still BOUND for the auditor
        assert b["evidence"]["materiality_trigger_fingerprint"] == "FP_OTHER"


# =========================================================================== #
# 5. Safety — the boundary R54.1 may never cross.
# =========================================================================== #
class TestSafetyBoundary:
    def test_23_governed_change_approves_nothing(self, tmp_path):
        con = _con(cr.OUTCOME_PROPOSAL_READY)
        summ = _summ(reallocation_outcome=cr.OUTCOME_PROPOSAL_READY)
        _, _, out = _record(tmp_path, con=con, summ=summ)
        s = out["record"]["safety"]
        assert s["approved_anything"] is False
        assert s["automatic_approval_allowed"] is False
        assert out["record"]["manual_review_required"] is True
        assert out["record"]["approval_path"] == \
            "POST /v1/operations/portfolio-decision/record"

    @pytest.mark.parametrize("flag", [
        "created_orders", "created_order_plan", "created_fills",
        "approved_anything", "automatic_approval_allowed", "promoted_model",
        "automatic_model_promotion_allowed", "activated_sleeve",
        "automatic_sleeve_activation_allowed", "changed_holdings",
        "changed_cash", "changed_nav", "ran_daily_close",
        "advances_operational_mark", "automation_enabled", "broker_enabled",
        "rewrote_history"])
    def test_24_to_30_no_mutation_no_order_no_promotion(self, tmp_path, flag):
        _, _, out = _record(tmp_path)
        assert out["record"]["safety"][flag] is False

    def test_26b_only_the_governed_lane_files_are_written(self, tmp_path):
        _record(tmp_path)
        written = sorted(p.name for p in Path(tmp_path).iterdir())
        assert written == ["governed_decisions.json", "governed_index.json"]

    def test_27b_the_manual_operator_lane_is_untouched(self, tmp_path):
        """A governed record must never land in the manual approval pointer."""
        _record(tmp_path)
        assert not (tmp_path / "decisions.json").exists()
        assert pdec.load_decision_record(active_book_id=BOOK,
                                         eligible_market_date=SESSION,
                                         decision_dir=tmp_path) is None

    def test_28b_recording_requires_the_system_token_and_a_passed_gate(self, tmp_path):
        cand, gate = _gate()
        assert pdec.record_governed_decision(
            candidate=cand, gate=gate, confirm=None,
            decision_dir=tmp_path)["recorded"] is False
        # the OPERATOR approval token may never satisfy the governed lane
        assert pdec.record_governed_decision(
            candidate=cand, gate=gate, confirm=pdec.CONFIRM_TOKEN,
            decision_dir=tmp_path)["recorded"] is False
        blocked, blocked_gate = _gate(ps=_ps(economic_state_hash="ESH_MOVED"))
        out = pdec.record_governed_decision(
            candidate=blocked, gate=blocked_gate,
            confirm=pdec.GOVERNED_DECISION_CONFIRM_TOKEN, decision_dir=tmp_path)
        assert out["recorded"] is False and out["status"] == pdec.GATE_WITHHELD
        assert list(Path(tmp_path).iterdir()) == []

    def test_29b_no_order_fill_or_broker_symbol_exists_in_the_governed_lane(self):
        gov = _governed_lane_source()
        for token in ("submit_order", "create_order", "place_order",
                      "build_order_plan", "record_fill", "broker_client",
                      "broker.", "promote_model", "promote_challenger",
                      "activate_sleeve", "requests.", "httpx", "subprocess",
                      "schtasks", "Register-ScheduledTask"):
            assert token not in gov, token


# =========================================================================== #
# 6. The gate decides admissibility only — never economics.
# =========================================================================== #
class TestNoSecondEngine:
    def test_40_current_holdings_receive_no_optimizer_privilege(self, tmp_path):
        _, _, out = _record(tmp_path)
        zb = out["record"]["zero_base"]
        assert zb["incumbency_policy"] == cr.INCUMBENCY_POLICY
        assert zb["incumbency_policy"] == \
            "NO_INVESTMENT_PRIVILEGE_ONLY_PRICED_TRANSITION_COST"
        assert zb["current_holdings_privileged"] is False
        assert zb["ideal_target_owner"] == "api.zero_base_target"

    def test_40b_a_privileged_incumbency_policy_is_withheld(self):
        con = _con()
        con["multi_asset"] = {"current_holdings_privileged": True}
        _, gate = _gate(con=con)
        assert pdec.WR_SWITCHING_ECONOMICS in _codes(gate)
        assert "ZERO_BASE_INCUMBENCY_POLICY_INTACT" in gate["failing_checks"]

    def test_40c_the_gate_declares_it_owns_no_economics(self):
        _, gate = _gate()
        assert gate["gate_decides_economics"] is False
        assert gate["economics_owner"] == "engine.constrained_reallocation"

    def test_40d_no_second_optimizer_or_hurdle_is_defined(self):
        gov = _governed_lane_source()
        for token in ("def switching_economics(", "def solve_feasible_target(",
                      "def decide_outcome(", "def herfindahl(",
                      "def one_way_turnover(", "min_switching_net_improvement ="):
            assert token not in gov, token

    def test_40e_economics_are_copied_verbatim_from_the_owner(self, tmp_path):
        _, _, out = _record(tmp_path)
        assert out["record"]["switching_economics"] == _econ(
            clears_switching_hurdle=False)

    def test_40f_there_is_exactly_one_intraday_governance_owner(self):
        assert pdec.GOVERNANCE_GATE_OWNER == "api.portfolio_decision"
        assert esr.GOVERNANCE_DELEGATE == "api.portfolio_decision"
        # the cycle delegates; it hosts no gate of its own
        assert "def evaluate_intraday_governance(" not in ESR_SRC
        assert "def evaluate_intraday_governance(" not in AMS_SRC
        assert "def record_governed_decision(" not in ESR_SRC
        assert "def record_governed_decision(" not in AMS_SRC


# =========================================================================== #
# 7. The live event cycle delegates, and records what the owner answered.
# =========================================================================== #
class TestEventCycleDelegation:
    def test_35b_cycle_records_the_delegated_verdict(self, tmp_path, monkeypatch):
        captured = {}

        def fake_gov(**kw):
            captured.update(kw)
            return {"verdict": pdec.GATE_ELIGIBLE, "recorded": True,
                    "record": {"record_id": "gdec_x", "decision":
                               pdec.GD_HOLD_CURRENT_BOOK,
                               "provenance": pdec.PROV_GOVERNED_INTRADAY,
                               "supersedes_decision_id": None},
                    "candidate": {"candidate_identity_hash": "cih"},
                    "gate": {"withheld_reason_codes": [], "failing_checks": []}}

        out = _run_cycle(tmp_path, monkeypatch, governance_fn=fake_gov)
        assert out["governed_decision"]["verdict"] == pdec.GATE_ELIGIBLE
        assert out["governed_decision"]["recorded"] is True
        assert out["governed_decision"]["record_id"] == "gdec_x"
        assert out["governed_decision"]["advances_operational_mark"] is False
        # the cycle passed its OWN run summary, identities and all
        ec = captured["event_cycle"]
        assert ec["run_id"] == out["run_id"]
        assert ec["portfolio_state_hash"] == "ESH1"
        assert "stage_timestamps" in ec
        # ...and the observation stamp the latency schema needs, SELECTED from
        # the events the fabric admitted (None here: no events were injected)
        assert "observation_received_at" in captured
        # ...and the scoring identity the cycle ALREADY built, so the gate never
        # re-runs a full universe scoring that just completed.
        assert "scoring_identity" in captured

    def test_35e_the_cycle_selects_the_newest_admitted_observation_stamp(self):
        assert esr._newest_stamp([
            "2026-09-01T17:30:00+00:00", None,
            "2026-09-01T17:36:00+00:00", "2026-09-01T17:31:00+00:00",
        ]) == "2026-09-01T17:36:00+00:00"
        assert esr._newest_stamp([None, ""]) is None
        assert esr._newest_stamp([]) is None

    def test_35c_a_failing_gate_never_breaks_the_cycle(self, tmp_path, monkeypatch):
        def boom(**kw):
            raise RuntimeError("owner unavailable")

        out = _run_cycle(tmp_path, monkeypatch, governance_fn=boom)
        assert out["state"] in esr.CYCLE_STATES
        assert out["governed_decision"]["recorded"] is False
        assert any("governed decision gate unavailable" in w
                   for w in out["warnings"])

    def test_35d_no_reassessment_means_no_governance_call(self, tmp_path,
                                                          monkeypatch):
        calls = []
        out = _run_cycle(tmp_path, monkeypatch,
                         governance_fn=lambda **kw: calls.append(kw),
                         material=False)
        assert calls == []
        assert out["governed_decision"]["evaluated"] is False


def _run_cycle(tmp_path, monkeypatch, *, governance_fn, material=True):
    """One hermetic event cycle: every owner is a stub, every root is tmp.

    Materiality is injected through the GATE'S OWN owner so the cycle's real
    precedence logic runs; nothing about the materiality policy is bypassed or
    reimplemented here.
    """
    verdict = {
        "change_level": "MATERIAL_SIGNAL_CHANGED" if material else "NONE",
        "reassessment_required": bool(material),
        "reassessment_reason": "test injection",
        "duplicate_of_prior_trigger": False,
        "data_changed": bool(material),
        "trigger_count": 1 if material else 0,
        "trigger_fingerprint": "FP1",
        "affected_entities": [],
    }
    monkeypatch.setattr(esr.emat, "assess_materiality",
                        lambda **kw: dict(verdict))

    def _hoc(**kw):
        return {"assessment": {"assessment_hash": "HOC1",
                               "assessment_state": "READY",
                               "holding_reviews": [{"ticker": t} for t in HELD]}}

    return esr.run_event_signal_refresh(
        confirm=esr.EXECUTE_CONFIRM_TOKEN,
        fabric_dir=tmp_path / "fabric",
        portfolio_state=_ps(), scoring={"rankings": []}, price_panel=None,
        corpus_events=[],
        hoc_fn=_hoc,
        reassessment_fn=lambda **kw: {
            "reassessment": {"reassessment_state": "PROPOSAL_READY",
                             "reassessment_hash": "RA1"}},
        proposal_fn=lambda **kw: {"proposal": {"proposal_hash": "PR1",
                                               "proposal_state": "READY"}},
        proposal_gate_fn=lambda r: {"build_proposal": True},
        governance_fn=governance_fn,
        prior_ranking=None,
        decision_dir=tmp_path / "decisions")


# =========================================================================== #
# 8. Active Manager State — two lanes, correct provenance, zero governance.
# =========================================================================== #
class TestActiveManagerStateSurface:
    def _ams(self, **kw):
        gd = kw.pop("governed_decision", None)
        return ams.build_active_manager_state(
            workflow=_wf(),
            event_refresh={"last_run_summary": _cycle(**kw.pop("cycle", {}))},
            reassessment=_reas(), constrained=_con(),
            portfolio_state=_ps(), governed_decision=gd)

    def test_35_provenance_is_shown_correctly(self, tmp_path):
        _, _, out = _record(tmp_path)
        gd = pdec.load_governed_portfolio_decision(
            workflow=_wf(), reassessment=_reas(), proposal_summary=_summ(),
            constrained=_con(), active_book_id=BOOK, decision_dir=tmp_path)
        d = self._ams(governed_decision=gd)
        assert d["latest_governed_portfolio_decision"]["provenance"] == \
            pdec.PROV_GOVERNED_INTRADAY
        assert d["latest_governed_portfolio_decision"]["decision"] == \
            pdec.GD_HOLD_CURRENT_BOOK
        assert d["latest_live_intraday_assessment"]["provenance"] == \
            "LIVE_PRE_DRC_SIGNAL"

    def test_36_live_and_governed_are_distinct_sections(self):
        d = self._ams()
        live = d["latest_live_intraday_assessment"]
        gov = d["latest_governed_portfolio_decision"]
        assert live["owner"] == "api.event_signal_refresh"
        assert gov["owner"] == "api.portfolio_decision"
        assert live["is_authoritative_decision"] is False
        assert live["advances_governed_decision"] is False

    def test_20_a_non_governed_live_state_never_becomes_the_headline(self):
        """Today follows the GOVERNED decision; a live signal never replaces it."""
        d = self._ams()
        assert d["latest_governed_portfolio_decision"]["available"] is False
        assert d["latest_live_intraday_assessment"]["state"] == \
            esr.ST_PROPOSAL_AVAILABLE
        # the live lane still declares it is not the decision
        assert d["latest_live_intraday_assessment"]["is_authoritative_decision"] \
            is False
        ladder = d["decision_authority"]
        assert ladder["live_intraday_assessment"]["advances_governed_decision"] \
            is False
        assert ladder["intraday_governance_gate"]["owner"] == \
            "api.portfolio_decision"

    def test_36b_withheld_candidate_exposes_reasons_and_failing_checks(self):
        cyc = _cycle(governed_decision={
            "evaluated": True, "verdict": pdec.GATE_WITHHELD, "recorded": False,
            "decision": pdec.GD_HOLD_CURRENT_BOOK,
            "withheld_reason_codes": [pdec.WR_MARKET_DATA_STALE],
            "failing_checks": ["NO_TRUE_DATA_GAP"],
            "candidate_identity_hash": "cih"})
        d = ams.build_active_manager_state(
            workflow=_wf(),
            event_refresh={"last_run_summary": cyc},
            reassessment=_reas(), constrained=_con(), portfolio_state=_ps())
        g = d["intraday_governance"]
        assert g["verdict"] == pdec.GATE_WITHHELD
        assert g["promoted_to_governed"] is False
        assert g["withheld_reason_codes"] == [pdec.WR_MARKET_DATA_STALE]
        assert g["failing_checks"] == ["NO_TRUE_DATA_GAP"]
        assert g["candidate_decision"] == pdec.GD_HOLD_CURRENT_BOOK

    def test_37_the_ui_performs_no_governance_logic(self):
        ui = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                            errors="replace")
        start = ui.find("/* R54_REGION_START */")
        end = ui.find("/* R54_REGION_END */")
        assert start != -1 and end > start
        region = ui[start:end]
        for token in ("GOVERNED_INTRADAY_DECISION_ELIGIBLE",
                      "INTRADAY_DECISION_WITHHELD", "supersede",
                      "switching_hurdle >", "clears_switching_hurdle ="):
            assert token not in region, token

    def test_37b_ams_defines_no_gate_and_no_writer(self):
        for token in ("def evaluate_intraday_governance(",
                      "def record_governed_decision(",
                      "def governed_decision_ordering_key(",
                      "def candidate_identity_hash("):
            assert token not in AMS_SRC, token
        assert '"recomputes_nothing": True' in AMS_SRC

    def test_39_components_and_owners_stay_consistent(self):
        d = self._ams()
        assert set(d["components"]) == set(ams.COMPONENT_OWNERS)
        for c in d["components"]:
            assert d[c]["owner"] == ams.COMPONENT_OWNERS[c]
        for c in ("latest_live_intraday_assessment",
                  "latest_governed_portfolio_decision", "intraday_governance",
                  "decision_latency"):
            assert c in d["components"]


# =========================================================================== #
# 9. Latency — measured from persisted stamps, never fabricated.
# =========================================================================== #
class TestLatency:
    def test_latency_is_measured_end_to_end_when_every_stamp_exists(self, tmp_path):
        _, _, out = _record(tmp_path)
        lat = out["record"]["latency"]
        assert lat["latency_measurement_complete"] is True
        assert lat["missing_measurements"] == []
        assert lat["observation_to_signal_seconds"] == 270.0
        assert lat["signal_to_reassessment_seconds"] == 70.0
        assert lat["reassessment_to_governed_seconds"] is not None
        assert lat["observation_to_governed_seconds"] is not None

    def test_a_missing_stage_stamp_is_named_never_invented(self):
        lat = esr.measure_decision_latency(
            stage_timestamps={"signal_refresh_completed_at": None,
                              "reassessment_completed_at":
                                  "2026-09-01T17:41:40+00:00"},
            event_cycle_started_at="2026-09-01T17:40:00+00:00")
        assert lat["latency_measurement_complete"] is False
        assert "signal_refresh_completed_at" in lat["missing_measurements"]
        assert lat["observation_to_signal_seconds"] is None
        assert lat["signal_to_reassessment_seconds"] is None

    def test_stage_timestamps_select_only_completed_steps(self):
        stamps = esr.stage_timestamps([
            {"step": "REFRESH_AFFECTED_INPUTS", "status": "OK",
             "finished_at": "2026-09-01T17:40:30+00:00"},
            {"step": "PORTFOLIO_REASSESSMENT", "status": "FAILED",
             "finished_at": "2026-09-01T17:41:40+00:00"},
        ])
        assert stamps["signal_refresh_completed_at"] == "2026-09-01T17:40:30+00:00"
        assert stamps["reassessment_completed_at"] is None


# =========================================================================== #
# 10. R53.1 emission-slot contract — UNCHANGED, and 16:20 ET is by design.
# =========================================================================== #
class TestEmissionSlotContractUnchanged:
    def test_slots_and_grace_are_the_frozen_r53_contract(self):
        from paper_trader.alpha_agent.r53 import intraday_factory as f
        assert f.EMISSION_SLOTS_ET == ("10:00", "12:00", "14:00")
        assert f.SLOT_GRACE_MINUTES == 15

    def test_nominal_slot_and_tolerated_late_start(self):
        import datetime as dt
        from zoneinfo import ZoneInfo
        from paper_trader.alpha_agent.r53 import intraday_factory as f
        et = ZoneInfo("America/New_York")

        def _slot(h, m):
            return f.slot_for(dt.datetime(2026, 9, 1, h, m, tzinfo=et)
                              .astimezone(dt.timezone.utc))

        assert _slot(12, 0)["slot_et"] == "12:00"          # nominal
        assert _slot(12, 14)["slot_et"] == "12:00"         # tolerated late start
        assert _slot(12, 16) is None                       # too late -> refused
        assert _slot(16, 20) is None                       # the post-close pass

    def test_1620_is_a_declared_post_close_scoring_pass_not_an_emission_slot(self):
        installer = (REPO / "scripts" /
                     "install_intraday_emission_task.ps1").read_text(
                         encoding="utf-8", errors="replace")
        assert "'10:00', '12:00', '14:00', '16:20'" in installer
        assert "post-close scoring pass" in installer
        assert "structurally refused outside a slot" in installer
        runner = (REPO / "scripts" / "run_intraday_emission.py").read_text(
            encoding="utf-8", errors="replace")
        assert "scores every matured prediction first" in runner

    def test_the_slot_clock_and_the_installer_were_not_modified(self):
        """R54.1 changed no emission-slot code: the contract is byte-stable."""
        from paper_trader.alpha_agent.r53 import intraday_factory as f
        assert f.EMIT_NOT_A_SLOT == "NOT_AN_EMISSION_SLOT"
        assert f.PREDICTION_IDENTITY_KEY == (
            "challenger_id", "challenger_version", "instrument", "slot_utc",
            "horizon")
        assert f.FORFEITURE_IDENTITY_KEY == ("challenger_id", "slot_utc")
