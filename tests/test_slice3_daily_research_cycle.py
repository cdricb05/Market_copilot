"""tests/test_slice3_daily_research_cycle.py — Slice 3 (Phase 29D) Persistent Daily
Research Cycle.

Deterministic, offline tests for the canonical orchestration owner
(``api.daily_research_cycle``): planning, execution, idempotency / concurrency /
resume, failure & partial-success policy, point-in-time safeguards, the API,
the workflow-state integration, the UI, and the safety invariants.

Every read model is injected and every provider / write boundary is an injectable
seam, so NO network, database, provider, prediction, real cycle, Daily Close,
operational-ledger write, order / signal / decision / fill, or model promotion
occurs. Run manifests are written under a per-test ``tmp_path`` (never the
operational ledger root).
"""
from __future__ import annotations

import copy
import json
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import data_freshness as df

ET = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parent.parent
SRC = (ROOT / "api" / "daily_research_cycle.py").read_text(encoding="utf-8")
UI = (ROOT / "api" / "ui" / "index.html")


# --------------------------------------------------------------------------- #
# Fixtures (inject read models; real data_freshness computes the plan).
# --------------------------------------------------------------------------- #
def _op(desk="2026-08-04", nav="2026-08-04", target="2026-08-04", **kw):
    ob = {"book_id": "alpha_paper_book_1", "book_label": "Alpha Paper Book #1",
          "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
          "nav_as_of_date": nav, "desk_mark_date": desk, "latest_desk_mark_date": desk,
          "nav": 102000.0, "cash": 1500.0, "holdings_count": 25, "pending_order_count": 0,
          "current_target": {"alpha_market_date": target, "latest_completed_market_date": desk}}
    ob.update(kw)
    return {"operational_book": ob}


_DESK = {"series": {"SPY": [["2026-07-31", 747.0], ["2026-08-04", 771.3]]},
         "latest_completed_date": "2026-08-04"}
_CLOSE = {"market_date": "2026-08-04", "done": True,
          "final_close_status": "DAILY_CLOSE_COMPLETE_HOLD", "status": "CLOSE_FINISHED"}
_FWD = {"latest_snapshot_date": "2026-08-04", "snapshot_count": 6, "evidence_state": "X"}
_DAILY = {"status": "DAILY_STATUS_READY", "latest_valid_mark_date": "2026-08-04"}
_REGISTRY = [("fundamental_momentum_50_50_v1", "fundamental_momentum_50_50_top25", 25, "ACTIVE"),
             ("fundamental_momentum_50_50_v1", "fundamental_momentum_50_50_top50", 50, "SHADOW"),
             ("mom_6_1", "mom_6_1_top25", 25, "SHADOW"), ("mom_6_1", "mom_6_1_top50", 50, "SHADOW"),
             ("composite_sn", "composite_sn_top25", 25, "SHADOW"),
             ("composite_sn", "composite_sn_top50", 50, "SHADOW")]


def _inputs(price="2026-08-04", month="2026-08", fundamental="2026-05-22"):
    return {"market_as_of_date": price, "momentum_month": month,
            "fundamental_as_of_date": fundamental}


def _models(**kw):
    m = dict(operational=copy.deepcopy(_op()), inputs=_inputs(), daily_status=dict(_DAILY),
             desk_marks=copy.deepcopy(_DESK), close_progress=dict(_CLOSE),
             forward_status=copy.deepcopy(_FWD))
    m.update(kw)
    return m


def _freshness(reference_today="2026-08-05", now=None, **kw):
    m = _models(**kw)
    m["daily_close_status"] = m.pop("close_progress")   # data_freshness kwarg name
    return df.load_data_freshness(reference_today=(None if now else reference_today),
                                  now=now, **m)


class Fakes:
    def __init__(self, *, refresh_status="ALPHA_TARGET_REFRESHED",
                 evidence_status="FORWARD_EVIDENCE_COMPLETE", created=6, mandatory=True,
                 scoring_ok=True, target_state="READY_TO_CONFIRM",
                 registry=None, monthly=False,
                 reassessment_decision="PROPOSAL_READY"):
        self.calls = {"refresh": 0, "monthly": 0, "score": 0, "target": 0,
                      "evidence": 0, "assess": 0}
        # Stage 20: the economic-change-gate verdict the stubbed reassessment returns.
        self.reassessment_decision = reassessment_decision
        self.refresh_status = refresh_status
        self.evidence_status = evidence_status
        self.created = created
        self.mandatory = mandatory
        self.scoring_ok = scoring_ok
        self.target_state = target_state
        self.registry = registry if registry is not None else list(_REGISTRY)
        self.monthly = monthly

    def refresh(self, *, confirm, downloader, completed_through):
        self.calls["refresh"] += 1
        if self.refresh_status == "RAISE":
            raise RuntimeError("provider boom")
        wrote = self.refresh_status in ("ALPHA_TARGET_REFRESHED",)  # faithful: only a
        return {"status": self.refresh_status, "performed_write": wrote,  # real refresh writes
                "required_next_action": (drc.MONTHLY_EMITTER_ACTION
                                         if "NEEDS_MONTHLY" in self.refresh_status else None)}

    def monthly_emit(self, *, eligible):
        self.calls["monthly"] += 1
        return {"status": "MONTHLY_INPUT_EMITTED", "performed_write": True}

    def score(self):
        self.calls["score"] += 1
        if not self.scoring_ok:
            return {"status": "MHZ_INPUTS_UNAVAILABLE"}
        return {"status": "MHZ_READY", "market_as_of_date": "2026-08-04",
                "momentum_month": "2026-08", "fundamental_as_of_date": "2026-05-22",
                "scores": {"composite_sn": {t: {"eligible": True} for t in ("A", "B", "C", "D")},
                           "counts": {}},
                "combined": {"n_common": 4},
                "books": {"primary_book_id": "fundamental_momentum_50_50_top25", "books": {
                    "fundamental_momentum_50_50_top25": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 0.5, "sector": "Tech"},
                        {"ticker": "B", "rank": 2, "weight": 0.5, "sector": "Fin"}]},
                    "fundamental_momentum_50_50_top50": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 0.4, "sector": "Tech"},
                        {"ticker": "B", "rank": 2, "weight": 0.3, "sector": "Fin"},
                        {"ticker": "C", "rank": 3, "weight": 0.3, "sector": "Ind"}]}}}}

    def target(self):
        self.calls["target"] += 1
        return {"state": self.target_state, "dates": {"alpha_market_date": "2026-08-04"},
                "required_next_action": "PREVIEW_CONFIRM",
                "confirm_required_token": "CONFIRM_MHZ_PAPER_SNAPSHOT"}

    def capture(self, *, market_date, current, ops, downloader):
        self.calls["evidence"] += 1
        n = len(self.registry)
        return {"snapshots_expected": n, "snapshots_created": self.created,
                "snapshots_already_present": 0,
                "snapshots_unavailable": max(0, n - self.created),
                "mandatory_active_snapshot_persisted": self.mandatory,
                "evidence_status": self.evidence_status,
                "artifact_bundle_id": "fca_%s" % market_date,
                "artifact_hash": "hash_%s" % market_date, "performed_write": True}

    def assess(self, *, today):
        self.calls["assess"] += 1
        return {"latest_completed_market_date": today, "evaluation_date": today,
                "outcome": "NO_ACTION_TODAY", "target_state": "CURRENT_ALIGNED",
                "headline": "No portfolio change required."}

    def holding_opp(self, *, scoring=None, hoc_dir=None):
        # Slice 6: hermetic stub of the opportunity-cost engine seam (no I/O), mirroring
        # how every other write boundary is stubbed in this harness.
        self.calls["holding_opp"] = self.calls.get("holding_opp", 0) + 1
        return {"assessment": {"assessment_state": "READY",
                               "assessment_hash": "hoc_stub_hash",
                               "eligible_market_date": "2026-08-05",
                               "holding_reviews": [],
                               "recommendation_counts": {"HOLD": 0, "REDUCE": 0, "EXIT": 0,
                                                         "REPLACE": 0, "ADD": 0},
                               "data_quality": {"data_gaps": []}},
                "persistence": {"status": "CREATED", "artifact_id": "hoc_stub",
                                "persisted": True}}

    def reassessment(self, *, scoring=None, hoc_assessment=None, freshness=None,
                     reassessment_dir=None, hoc_dir=None):
        # Stage 20: hermetic stub of the portfolio-reassessment (economic change gate)
        # seam (no I/O). ``reassessment_decision`` lets a test drive the gate: only
        # PROPOSAL_READY authorises the Slice-7 target engine to run.
        self.calls["reassessment"] = self.calls.get("reassessment", 0) + 1
        dec = self.reassessment_decision
        return {"reassessment": {"reassessment_state": dec,
                                 "reassessment_hash": "prs_stub_hash",
                                 "eligible_market_date": "2026-08-05",
                                 "holding_assessments": [],
                                 "attention": {"exit": [], "replace": [], "reduce": [],
                                               "count": 0},
                                 "decision": {"decision": dec,
                                              "proposal_required": dec == "PROPOSAL_READY",
                                              "holdings_evaluated": 0,
                                              "actionable_holding_count": 0,
                                              "expected_net_improvement": 0.0,
                                              "expected_one_way_turnover": 0.0,
                                              "blockers": [], "reason_codes": []},
                                 "explanation": "stub", "data_gaps": []},
                "persistence": {"status": "CREATED", "artifact_id": "prs_stub",
                                "persisted": True, "history_appended": True}}

    def reallocation(self, *, scoring=None, hoc_assessment=None, reallocation_dir=None,
                     hoc_dir=None):
        # Slice 7: hermetic stub of the reallocation-proposal engine seam (no I/O).
        self.calls["reallocation"] = self.calls.get("reallocation", 0) + 1
        return {"proposal": {"proposal_state": "READY",
                             "proposal_hash": "realloc_stub_hash",
                             "eligible_market_date": "2026-08-05",
                             "action_counts": {"RETAIN": 0, "INCREASE": 0, "REDUCE": 0,
                                               "EXIT": 0, "ADD": 0, "REPLACE_OUT": 0,
                                               "REPLACE_IN": 0},
                             "portfolio": {"proposed_holding_count": 0},
                             "signal": {}, "turnover": {}, "data_gaps": []},
                "persistence": {"status": "CREATED", "proposal_id": "realloc_stub",
                                "persisted": True, "superseded_proposal_id": None}}

    def research_agent(self, *, scoring=None, reallocation=None, research_agent_dir=None,
                       hoc_dir=None, reallocation_dir=None, desk_dir=None):
        # Slice 8: hermetic stub of the Persistent Alpha Research Agent seam (no I/O).
        self.calls["research_agent"] = self.calls.get("research_agent", 0) + 1
        return {"assessment": {"research_state": "INSUFFICIENT_EVIDENCE",
                               "assessment_hash": "ra_stub_hash",
                               "eligible_market_date": "2026-08-05",
                               "evidence_quality": {"state": "INSUFFICIENT"},
                               "degradation": {"categories": []},
                               "recalibration": {"state": "INSUFFICIENT_EVIDENCE",
                                                 "recommended": False},
                               "challenger": {"state": "NOT_EVALUATED"},
                               "research_opportunities": [],
                               "data_gaps": []},
                "persistence": {"status": "CREATED", "assessment_id": "ra_stub",
                                "persisted": True, "superseded_assessment_id": None}}

    def tournament(self, *, eligible_market_date=None):
        # Release 46.2: hermetic stub of the prospective-tournament seam. The real
        # owner (alpha_agent.r46.advance) writes into the R46 research root, so a
        # sandboxed run must never reach it.
        self.calls["tournament"] = self.calls.get("tournament", 0) + 1
        return {"available": True, "state": "TOURNAMENT_LIVE_NOTHING_DUE",
                "campaign_id": "stub", "eligible_market_date": eligible_market_date,
                "tournament_predictions_matured": 0,
                "tournament_outcomes_scored": 0,
                "tournament_predictions_emitted": 0,
                "tournament_challengers_active": 10,
                "tournament_forward_evidence_count": 11,
                "tournament_next_maturity": "2026-08-27",
                "pending_predictions": 11, "challengers_registered": 10,
                "challengers_blocked": 0, "ledger_chain_intact": True,
                "stage_failures": [], "emission": {}, "leaderboard": {}}

    def kw(self):
        return dict(daily_refresh_fn=self.refresh, scoring_fn=self.score,
                    target_loader=self.target, evidence_capture_fn=self.capture,
                    evidence_registry=self.registry, assessment_loader=self.assess,
                    holding_opp_cost_fn=self.holding_opp,
                    reassessment_fn=self.reassessment,
                    reallocation_proposal_fn=self.reallocation,
                    research_agent_fn=self.research_agent,
                    tournament_fn=self.tournament,
                    refresh_confirm_token="CONFIRM_ALPHA_TARGET_REFRESH",
                    monthly_emitter_fn=(self.monthly_emit if self.monthly else None))


def _run(tmp, fakes=None, *, reference_today="2026-08-05", now=None, confirm=None, **kw):
    fakes = fakes or Fakes()
    args = dict(confirm=confirm or drc.EXECUTE_CONFIRMATION, drc_dir=str(tmp),
                reference_today=(None if now else reference_today), now=now, **_models())
    args.update(fakes.kw())
    args.update(kw)
    return drc.run_daily_research_cycle(**args), fakes


def _status(tmp, *, reference_today="2026-08-05", now=None, **kw):
    args = dict(drc_dir=str(tmp), reference_today=(None if now else reference_today),
                now=now, **_models())
    args.update(kw)
    return drc.load_daily_research_cycle_status(**args)


# =========================================================================== #
# PLANNING (required 1–10)
# =========================================================================== #
def test_01_session_open_waits_only_when_no_eligible_session_is_workable(tmp_path):
    # RELEASE 54.2.2. This gate used to answer "has TODAY's session closed?", which is
    # not the question the cycle is bound to: the cycle works on the ELIGIBLE COMPLETED
    # session, and an open market is no reason to hide governed research still owed for
    # a session that finished yesterday. Close precedence is unaffected — the eligible
    # session is the OWNED-DATA-CONFIRMED one, which only advances when a close runs.
    s = _status(tmp_path, reference_today=None, now=datetime(2026, 8, 5, 10, 0, tzinfo=ET))
    assert s["eligible_market_date"] == "2026-08-04"
    assert s["state"] == drc.NOT_STARTED and s["executable"] is True
    # With no confirmed owned data there is no eligible completed session to work on,
    # so the gate still refuses (its precedence over WAITING_FOR_OWNED_DATA while the
    # market is open is unchanged) and the cycle stays non-executable.
    s2 = _status(tmp_path, reference_today=None,
                 now=datetime(2026, 8, 5, 10, 0, tzinfo=ET),
                 operational=_op(desk=None, nav=None), inputs=_inputs())
    assert s2["state"] == drc.WAITING_FOR_SESSION_CLOSE and s2["executable"] is False


def test_02_owned_data_missing_returns_waiting_for_owned_data(tmp_path):
    s = _status(tmp_path, operational=_op(desk=None, nav=None), inputs=_inputs())
    assert s["state"] == drc.WAITING_FOR_OWNED_DATA and s["executable"] is False


def test_03_all_inputs_current_creates_minimal_plan(tmp_path):
    fr = _freshness()
    plan = drc.build_execution_plan(fr)
    assert plan["minimal"] is True and plan["refresh_steps"] == []
    assert "price_score_refresh" in plan["current_inputs"]


def test_04_daily_source_stale_creates_a_daily_refresh_step(tmp_path):
    fr = _freshness(inputs=_inputs(price="2026-08-03", month="2026-08"))
    plan = drc.build_execution_plan(fr)
    sids = [s["source_id"] for s in plan["refresh_steps"]]
    assert "price_score_refresh" in sids and plan["plan_blocked"] is False
    step = next(s for s in plan["refresh_steps"] if s["source_id"] == "price_score_refresh")
    assert step["can_refresh_automatically"] is True
    assert step["authoritative_owner"] == "api.alpha_target.run_refresh"


def test_05_monthly_source_due_creates_the_monthly_emitter_step(tmp_path):
    fr = _freshness(inputs=_inputs(price="2026-07-31", month="2026-07"))
    plan = drc.build_execution_plan(fr, monthly_emitter_available=True)
    step = next(s for s in plan["refresh_steps"] if s["source_id"] == "momentum_monthly")
    assert step["cadence"] == df.MONTHLY and step["can_refresh_automatically"] is True
    assert "momentum_monthly" in plan["slower_inputs_due"]


def test_06_quarterly_source_not_due_is_skipped(tmp_path):
    fr = _freshness()  # fundamental 2026-05 vs eligible 2026-08 → prior quarter, NOT_DUE
    plan = drc.build_execution_plan(fr)
    assert "fundamental_quarterly" in plan["current_inputs"]
    assert "fundamental_quarterly" not in plan["required_stale_inputs"]


def test_07_missing_required_source_blocks_the_plan(tmp_path):
    fr = _freshness(inputs=_inputs(price=None, month="2026-08"))
    plan = drc.build_execution_plan(fr)
    assert "price_score_refresh" in plan["missing_inputs"]
    assert plan["plan_blocked"] is True


def test_08_future_dated_source_blocks_the_plan(tmp_path):
    fr = _freshness(inputs=_inputs(price="2026-08-10", month="2026-08"))
    plan = drc.build_execution_plan(fr)
    assert "price_score_refresh" in plan["future_dated_inputs"]
    assert plan["plan_blocked"] is True


def test_09_inconsistent_active_book_blocks_the_plan(tmp_path):
    s = _status(tmp_path, active_book_override=[_op(), _op(book_id="other_book")])
    assert s["state"] == drc.INCONSISTENT and s["executable"] is False


def test_10_dependency_order_is_deterministic(tmp_path):
    fr = _freshness(inputs=_inputs(price="2026-08-03", month="2026-08"))
    p1 = drc.build_execution_plan(fr)
    p2 = drc.build_execution_plan(fr)
    assert p1 == p2
    assert drc.STEP_SEQUENCE[0] == drc.STEP_RESOLVE_SESSION
    assert drc.STEP_SEQUENCE[-1] == drc.STEP_RUN_ASSESSMENT


# =========================================================================== #
# EXECUTION (required 11–28)
# =========================================================================== #
def test_11_complete_successful_cycle(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE
    assert r["completed_steps"] == list(drc.STEP_SEQUENCE)
    # Stage 20: REASSESS_PORTFOLIO runs on every cycle; BUILD_REALLOCATION_PROPOSAL runs
    # only because the stubbed economic change gate returned PROPOSAL_READY.
    assert f.calls == {"refresh": 1, "monthly": 0, "score": 1, "target": 1,
                       "evidence": 1, "assess": 1, "holding_opp": 1, "reassessment": 1,
                       "reallocation": 1, "research_agent": 1, "tournament": 1}


def test_11b_stage20_no_change_gate_skips_the_reallocation_proposal(tmp_path):
    """Stage 20 boundary: a CURRENT_NO_CHANGE reassessment must NOT build a target.

    Before Stage 20 the cycle produced a reallocation proposal on EVERY signal refresh.
    The economic change gate is now the only thing that authorises the Slice-7 target
    engine, so a no-change verdict leaves the target engine uncalled and the step
    explicitly SKIPPED with the gate's own reason."""
    f = Fakes(reassessment_decision="CURRENT_NO_CHANGE")
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE
    assert f.calls["reassessment"] == 1
    assert f.calls.get("reallocation", 0) == 0        # the target engine never ran
    assert drc.STEP_REASSESS_PORTFOLIO in r["completed_steps"]
    assert drc.STEP_BUILD_REALLOCATION in r["skipped_steps"]
    step = next(s for s in r["step_results"]
                if s["step_id"] == drc.STEP_BUILD_REALLOCATION)
    assert step["status"] == drc.S_SKIPPED
    assert "CURRENT_NO_CHANGE" in (step["reason"] or "")
    assert r["portfolio_reassessment_decision"] == "CURRENT_NO_CHANGE"
    assert r["portfolio_reassessment_proposal_required"] is False
    assert r["reallocation_proposal"]["state"] == "NOT_REQUIRED"


def test_11c_stage20_change_candidate_gate_skips_the_reallocation_proposal(tmp_path):
    """A CHANGE_CANDIDATE verdict is deliberately NOT a proposal: attractive
    replacements exist but portfolio economics/controls withhold the change."""
    f = Fakes(reassessment_decision="CHANGE_CANDIDATE")
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE
    assert f.calls.get("reallocation", 0) == 0
    assert drc.STEP_BUILD_REALLOCATION in r["skipped_steps"]


def test_11d_stage20_gate_fails_closed_when_reassessment_unavailable(tmp_path):
    """A reassessment that did not complete NEVER authorises a proposal (fail closed)."""

    def _boom(**kw):
        raise RuntimeError("reassessment engine unavailable")

    f = Fakes()
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"),
                reassessment_fn=_boom)
    assert f.calls.get("reallocation", 0) == 0
    assert drc.STEP_BUILD_REALLOCATION in r["skipped_steps"]
    prs_step = next(s for s in r["step_results"]
                    if s["step_id"] == drc.STEP_REASSESS_PORTFOLIO)
    assert prs_step["status"] == drc.S_FAILED


def test_12_successful_cycle_with_monthly_source_due(tmp_path):
    f = Fakes(monthly=True)
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-07-31", month="2026-07"))
    assert r["state"] == drc.COMPLETE
    assert f.calls["monthly"] == 1


def test_13_successful_cycle_with_no_slower_source_due(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE and "momentum_monthly" not in (
        r["input_plan"]["slower_inputs_due"])


def test_14_15_input_refresh_output_date_and_provenance_validated(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    refresh_step = next(s for s in r["step_results"]
                        if s["step_id"] == drc.STEP_REFRESH_INPUTS)
    assert refresh_step["as_of_date"] == "2026-08-04"
    assert refresh_step["owner"] == "api.alpha_target.run_refresh"


def test_16_date_alignment_gate_passes(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["alignment_status"] in (drc.ALIGNED, drc.ALIGNED_WITH_SLOWER_CADENCE)


# A freshness loader that ALWAYS reports a stale required price/score input,
# regardless of what the refresh claimed — simulating a provenance/validation
# failure where the owner reported success but the artifact did not advance.
def _stale_price_loader(**kw):
    kw2 = {k: v for k, v in kw.items()
           if k not in ("now", "reference_today", "close_cutoff_et")}
    kw2["inputs"] = {"market_as_of_date": "2026-08-01", "momentum_month": "2026-08",
                     "fundamental_as_of_date": "2026-05-22"}
    return df.load_data_freshness(reference_today="2026-08-05", **kw2)


def test_17_alignment_failure_prevents_scoring(tmp_path):
    f = Fakes()  # refresh reports OK, but the loader still sees the input as stale
    r, _ = _run(tmp_path, f, inputs=_inputs(price="2026-08-01", month="2026-08"),
                freshness_loader=_stale_price_loader)
    assert r["state"] == drc.BLOCKED
    assert r["alignment"]["status"] == drc.BLOCKED_STALE_REQUIRED_INPUT
    assert f.calls["score"] == 0


def test_18_full_universe_scoring_invoked_once(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert f.calls["score"] == 1


def test_19_scoring_exclusions_recorded(tmp_path):
    def score_with_excl():
        return {"status": "MHZ_READY", "market_as_of_date": "2026-08-04",
                "momentum_month": "2026-08", "fundamental_as_of_date": "2026-05-22",
                "scores": {"composite_sn": {
                    "A": {"eligible": True}, "B": {"eligible": True},
                    "X": {"eligible": False, "exclusion_reason": "LIQUIDITY_FILTER_FAILED"}},
                    "counts": {}},
                "combined": {"n_common": 2},
                "books": {"primary_book_id": "fundamental_momentum_50_50_top25", "books": {
                    "fundamental_momentum_50_50_top25": {"constituents": [
                        {"ticker": "A", "rank": 1, "weight": 1.0, "sector": "Tech"}]},
                    "fundamental_momentum_50_50_top50": {"constituents": []}}}}
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"),
                scoring_fn=score_with_excl)
    assert r["excluded_count"] == 1
    assert r["scoring"]["exclusions"]["X"] == "LIQUIDITY_FILTER_FAILED"


def test_20_top25_and_top50_produced(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert [c["ticker"] for c in r["top25"]] == ["A", "B"]
    assert len(r["top50"]) == 3


def test_21_22_target_owner_invoked_once_policy_preserved(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert f.calls["target"] == 1
    assert r["target_status"] == "READY_TO_CONFIRM"
    assert r["target_operationally_approved"] is False  # never auto-confirmed


def test_23_snapshot_registry_resolved_dynamically(tmp_path):
    f = Fakes(registry=[("m", "b1", 25, "ACTIVE"), ("m", "b2", 50, "SHADOW")], created=2)
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["required_snapshot_count"] == 2  # derived from the registry, not 6


def test_24_25_exact_expected_snapshot_set_and_active_book(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["required_snapshot_count"] == 6 and r["captured_snapshot_count"] == 6
    assert r["evidence"]["mandatory_active_snapshot_persisted"] is True
    assert r["snapshot_bundle_id"] == "fca_2026-08-04"


def test_26_27_portfolio_assessment_bridge_invoked_once_for_eligible(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert f.calls["assess"] == 1
    assert r["assessment_date"] == "2026-08-04"


def test_28_milestone2_limitation_recorded(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert "Milestone-2" in r["milestone2_limitation"]


# =========================================================================== #
# IDEMPOTENCY / CONCURRENCY / RESUME (required 29–40)
# =========================================================================== #
def _complete(tmp):
    return _run(tmp, inputs=_inputs(price="2026-08-03", month="2026-08"))


def test_29_to_34_repeated_completed_request_reuses_run(tmp_path):
    r1, f1 = _complete(tmp_path)
    assert r1["state"] == drc.COMPLETE
    f2 = Fakes()
    r2, f2 = _run(tmp_path, f2, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r2["reused_existing_run"] is True
    # 30–34: no duplicate refresh / ranking / target / snapshot / assessment.
    assert f2.calls == {"refresh": 0, "monthly": 0, "score": 0, "target": 0,
                        "evidence": 0, "assess": 0}
    assert r2["run_id"] == r1["run_id"]


def test_35_36_safe_incomplete_run_resumes_without_repeating_steps(tmp_path):
    r1, _ = _complete(tmp_path)
    rec = drc._load_run(r1["run_id"], str(tmp_path))
    # Truncate to an incomplete run (steps done through alignment only).
    keep = {drc.STEP_RESOLVE_SESSION, drc.STEP_VALIDATE_CONSISTENCY, drc.STEP_PLAN,
            drc.STEP_REFRESH_INPUTS, drc.STEP_VALIDATE_ALIGNMENT}
    rec["step_results"] = [s for s in rec["step_results"] if s["step_id"] in keep]
    rec["state"] = drc.SCORING_UNIVERSE
    rec.pop("scoring", None); rec.pop("target", None); rec.pop("evidence", None)
    drc._save_run(rec, str(tmp_path))
    drc._update_index(eligible_date=rec["eligible_market_date"],
                      idempotency_key=rec["idempotency_key"],
                      input_contract_hash=rec["input_contract_hash"],
                      run_id=rec["run_id"], state=drc.SCORING_UNIVERSE, drc_dir=str(tmp_path))
    f = Fakes()
    r2, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r2["state"] == drc.COMPLETE and r2["resumed_existing_run"] is True
    # Refresh reused (not re-invoked); scoring/target/evidence/assess re-run.
    assert f.calls["refresh"] == 0
    assert f.calls["score"] == 1 and f.calls["evidence"] == 1


def test_37_concurrent_identical_request_returns_run_in_progress(tmp_path):
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    drc._atomic_write_json(drc._lock_path(str(tmp_path)), {
        "idempotency_key": s["idempotency_key"], "eligible_date": s["eligible_market_date"],
        "run_id": "drc_other", "input_contract_hash": s["input_contract_hash"],
        "started_at": datetime.now(tz=timezone.utc).isoformat(), "pid": 999})
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.RUN_IN_PROGRESS


def test_38_conflicting_concurrent_request_returns_inconsistent(tmp_path):
    s = _status(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    drc._atomic_write_json(drc._lock_path(str(tmp_path)), {
        "idempotency_key": "different_key", "eligible_date": s["eligible_market_date"],
        "run_id": "drc_other", "input_contract_hash": "DIFFERENT_HASH",
        "started_at": datetime.now(tz=timezone.utc).isoformat(), "pid": 999})
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.INCONSISTENT


def test_39_stale_lock_is_classified_and_run_proceeds(tmp_path):
    drc._atomic_write_json(drc._lock_path(str(tmp_path)), {
        "idempotency_key": "old", "eligible_date": "2026-08-04", "run_id": "drc_old",
        "input_contract_hash": "OLD", "started_at": "2020-01-01T00:00:00+00:00", "pid": 1})
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE  # stale lock ignored


def test_40_atomic_manifest_write_and_no_duplicate_records(tmp_path):
    r1, _ = _complete(tmp_path)
    runs = list((Path(tmp_path) / "runs").glob("*.json"))
    assert len(runs) == 1
    _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert len(list((Path(tmp_path) / "runs").glob("*.json"))) == 1  # reused, not duplicated


# =========================================================================== #
# FAILURES (required 41–50)
# =========================================================================== #
def test_41_provider_refresh_failure_blocks(tmp_path):
    f = Fakes(refresh_status="ALPHA_TARGET_REFRESH_PROVIDER_BLOCKED")
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.BLOCKED and r["failed_step"] == drc.STEP_REFRESH_INPUTS
    assert f.calls["score"] == 0


def test_42_monthly_emitter_failure_blocks(tmp_path):
    # Month boundary with NO emitter → BLOCKED, no scoring / evidence.
    r, f = _run(tmp_path, inputs=_inputs(price="2026-07-31", month="2026-07"))
    assert r["state"] == drc.BLOCKED and r["failed_step"] == drc.STEP_REFRESH_INPUTS
    assert any(b.get("missing_implementation") == drc.MONTHLY_EMITTER_ACTION
               for b in r["blockers"])
    assert f.calls["score"] == 0 and f.calls["evidence"] == 0


def test_43_scoring_failure(tmp_path):
    f = Fakes(scoring_ok=False)
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.FAILED and r["failed_step"] == drc.STEP_SCORE_UNIVERSE
    assert f.calls["target"] == 0 and f.calls["evidence"] == 0


def test_44_target_failure_stops_before_evidence(tmp_path):
    def bad_target():
        raise RuntimeError("target boom")
    f = Fakes()
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"),
                target_loader=bad_target)
    # Workstream Q rule 5: target preparation failure → no evidence bundle / assessment.
    assert r["state"] == drc.FAILED and r["failed_step"] == drc.STEP_PREPARE_TARGET
    assert r["target_operationally_approved"] is False
    assert f.calls["evidence"] == 0 and f.calls["assess"] == 0


def test_45_snapshot_partial_failure_is_evidence_gap_not_complete(tmp_path):
    f = Fakes(evidence_status="FORWARD_EVIDENCE_PARTIAL", created=4, mandatory=True)
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE_WITH_EVIDENCE_GAP
    assert r["captured_snapshot_count"] == 4 and r["required_snapshot_count"] == 6
    assert r["evidence"]["documented_gap"] is True


def test_46_snapshot_registry_mismatch_flagged(tmp_path):
    f = Fakes(registry=[("m", "b1", 25, "ACTIVE")], created=0, mandatory=False,
              evidence_status="FORWARD_EVIDENCE_BLOCKED")
    r, f = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["required_snapshot_count"] == 1
    assert r["state"] == drc.COMPLETE_WITH_EVIDENCE_GAP


def test_47_assessment_failure_keeps_research_valid(tmp_path):
    def bad_assess(*, today):
        raise RuntimeError("gate boom")
    f = Fakes()
    r, _ = _run(tmp_path, f, inputs=_inputs(price="2026-08-03", month="2026-08"),
                assessment_loader=bad_assess)
    assert r["state"] in (drc.COMPLETE, drc.COMPLETE_WITH_EVIDENCE_GAP)
    assert r["top25"] and any("reassessment" in w.lower() for w in r["warnings"])


def test_48_persistence_writes_one_atomic_manifest_and_index(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["state"] == drc.COMPLETE
    assert (tmp_path / "index.json").exists()
    assert (tmp_path / "runs" / ("%s.json" % r["run_id"])).exists()
    assert not list(tmp_path.glob("*.tmp"))  # no leftover temp files (atomic replace)


def test_49_unknown_source_status_is_not_fresh(tmp_path):
    # No anchor (owned data missing) → UNKNOWN classification is never satisfied.
    fr = _freshness(operational=_op(desk=None, nav=None))
    aln = drc.evaluate_alignment(fr)
    assert aln["status"] != drc.ALIGNED


def test_50_prior_valid_operational_close_unchanged(tmp_path):
    # A blocked research run never touches the operational close read model.
    r, f = _run(tmp_path, inputs=_inputs(price="2026-07-31", month="2026-07"))
    assert r["state"] == drc.BLOCKED
    assert r["safety"]["ran_daily_close"] is False
    assert r["safety"]["wrote_to_operational_ledger"] is False


# =========================================================================== #
# POINT-IN-TIME (required 51–58)
# =========================================================================== #
def test_51_52_53_no_backdate_no_future_no_substitution(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    # Evidence capture is keyed to the ONE eligible date (no backdating / future).
    assert r["evidence"]["market_date"] == "2026-08-04"
    assert r["snapshot_bundle_id"] == "fca_2026-08-04"


def test_54_stale_required_input_not_used_for_scoring(tmp_path):
    # (mirror of 17) a stale required input blocks before scoring.
    f = Fakes()
    r, _ = _run(tmp_path, f, inputs=_inputs(price="2026-08-01", month="2026-08"),
                freshness_loader=_stale_price_loader)
    assert f.calls["score"] == 0 and r["state"] == drc.BLOCKED


def test_55_56_57_input_contract_hash_strategy_universe_frozen(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    assert r["input_contract_hash"]
    assert r["strategy_id"] == drc.STRATEGY_ID and r["strategy_version"] == drc.STRATEGY_VERSION
    assert r["universe_id"] == drc.UNIVERSE_ID


def test_58_different_contract_cannot_overwrite_existing_bundle(tmp_path):
    r1, _ = _complete(tmp_path)  # contract A for 2026-08-04
    # A DIFFERENT contract for the SAME date (different fundamental as-of) → INCONSISTENT.
    r2, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08",
                                          fundamental="2026-02-01"))
    assert r2["state"] == drc.INCONSISTENT
    assert any(b["code"] == "DIFFERENT_CONTRACT_SAME_DATE" for b in r2["blockers"])


# =========================================================================== #
# API (required 59–70)
# =========================================================================== #
def _client():
    from fastapi.testclient import TestClient
    from paper_trader.api.app import app
    return TestClient(app, raise_server_exceptions=False)


def _key():
    from paper_trader.config import get_settings
    return get_settings().service_api_key


def test_59_60_status_endpoint_authenticated_get_only():
    c = _client()
    assert c.get("/v1/operations/daily-research-cycle/status").status_code in (401, 403)
    assert c.post("/v1/operations/daily-research-cycle/status",
                  headers={"X-API-Key": _key()}).status_code == 405


def test_60b_status_endpoint_read_only_returns_vocab(monkeypatch):
    canned = {"status": "OK", "state": drc.WAITING_FOR_SESSION_CLOSE,
              "state_vocabulary": list(drc.RUN_STATES), "executable": False,
              "safety": drc._safety(False)}
    monkeypatch.setattr("paper_trader.api.app._drc.load_daily_research_cycle_status",
                        lambda: canned)
    c = _client()
    resp = c.get("/v1/operations/daily-research-cycle/status", headers={"X-API-Key": _key()})
    assert resp.status_code == 200
    body = resp.json()
    assert body["state"] in drc.RUN_STATES
    assert body["safety"]["read_only"] is True and body["safety"]["ran_daily_close"] is False


def test_61_62_run_endpoint_authenticated_post_only():
    c = _client()
    assert c.post("/v1/operations/daily-research-cycle/run",
                  json={"confirmation": "x"}).status_code in (401, 403)
    assert c.get("/v1/operations/daily-research-cycle/run",
                 headers={"X-API-Key": _key()}).status_code == 405


def test_63_invalid_confirmation_token_rejected():
    c = _client()
    resp = c.post("/v1/operations/daily-research-cycle/run",
                  headers={"X-API-Key": _key()}, json={"confirmation": "WRONG"})
    assert resp.status_code == 400


def test_64_67_valid_token_wires_to_owner(monkeypatch):
    seen = {}
    def fake_run(*, confirm, requested_by):
        seen["confirm"] = confirm
        return {"status": "OK", "state": drc.COMPLETE, "reused_existing_run": False,
                "safety": drc._safety(True)}
    monkeypatch.setattr("paper_trader.api.app._drc.run_daily_research_cycle", fake_run)
    c = _client()
    resp = c.post("/v1/operations/daily-research-cycle/run", headers={"X-API-Key": _key()},
                  json={"confirmation": "RUN_DAILY_RESEARCH_CYCLE"})
    assert resp.status_code == 200 and seen["confirm"] == "RUN_DAILY_RESEARCH_CYCLE"
    assert resp.json()["state"] == drc.COMPLETE


def test_65_66_run_without_a_workable_eligible_session_writes_nothing(tmp_path):
    # RELEASE 54.2.2 — the refusal is scoped to what it is actually protecting. With no
    # confirmed owned data there is no eligible completed session, so the run persists
    # NOTHING. (An open market alone no longer refuses: see test_01. The cycle is bound
    # to the eligible completed session, whose research it may legitimately produce
    # while the next session is still forming.)
    r_before, _ = _run(tmp_path, now=datetime(2026, 8, 5, 10, 0, tzinfo=ET),
                       operational=_op(desk=None, nav=None), inputs=_inputs())
    assert r_before["state"] == drc.WAITING_FOR_SESSION_CLOSE
    assert r_before.get("performed_write") is not True
    assert not list((tmp_path / "runs").glob("*.json"))  # nothing persisted


def test_68_69_70_api_exposes_no_secret_no_close_no_orders():
    from paper_trader.api import app as appmod
    src = Path(appmod.__file__).read_text(encoding="utf-8")
    # The run endpoint wires to the DRC owner and validates the token.
    assert "return _drc.run_daily_research_cycle(" in src
    assert "if body.confirmation != _drc.EXECUTE_CONFIRMATION:" in src
    # The DRC module never runs Daily Close nor creates a legacy Signal/order/fill.
    for tok in ("run_fill_cycle(", "place_order(", "submit_order(", "run_daily_close("):
        assert tok not in SRC


# =========================================================================== #
# WORKFLOW (required 71–77)
# =========================================================================== #
def test_71_to_77_workflow_state_consumes_cycle_status(tmp_path):
    from paper_trader.api import workflow_state as ws
    op = {"operational_book": {"book_id": "alpha_paper_book_1", "book_label": "B1",
          "current_status": "FORWARD_TRACKING_ACTIVE", "initialized": True,
          "nav_as_of_date": "2026-08-04", "desk_mark_date": "2026-08-04",
          "latest_desk_mark_date": "2026-08-04", "nav": 1.0, "cash": 1.0,
          "holdings_count": 25, "pending_order_count": 0,
          "current_target": {"alpha_market_date": "2026-08-04", "latest_completed_market_date": "2026-08-04"}}}
    common = dict(reference_today="2026-08-05", operational=op, desk_marks=copy.deepcopy(_DESK),
                  daily_status=dict(_DAILY), forward_status=copy.deepcopy(_FWD),
                  gate={"latest_completed_market_date": "2026-08-04", "outcome": "NO_ACTION_TODAY",
                        "target_state": "CURRENT_ALIGNED", "next_scheduled_full_review": "2026-09-01",
                        "scheduled_review_due": False},
                  target_readiness={"dates": {"alpha_market_date": "2026-08-04"}})
    stale = {"market_as_of_date": "2026-08-03", "momentum_month": "2026-08",
             "fundamental_as_of_date": "2026-05-22"}
    # 72 RUNNING
    r = ws.load_workflow_state(inputs=dict(stale),
        close_progress={"market_date": "2026-08-04", "done": False, "status": "X"},
        research_cycle={"state": "SCORING_UNIVERSE"}, **common)
    assert r["overall_state"] == ws.RESEARCH_CYCLE_RUNNING
    # 73 BLOCKED
    r = ws.load_workflow_state(inputs=dict(stale),
        close_progress={"market_date": "2026-08-04", "done": False, "status": "X"},
        research_cycle={"state": "BLOCKED", "blockers": [{"code": "UNSUPPORTED_AUTOMATIC_REFRESH"}]},
        **common)
    assert r["overall_state"] == ws.RESEARCH_CYCLE_BLOCKED
    assert r["research_cycle_state"]["cycle_blocked"] is True
    # 74/75/76 COMPLETE + fresh inputs → READY_FOR_DAILY_CLOSE; assessment current
    r = ws.load_workflow_state(inputs=_inputs(price="2026-08-04", month="2026-08"),
        close_progress={"market_date": "2026-08-04", "done": False, "status": "X"},
        research_cycle={"state": "COMPLETE"}, **common)
    assert r["overall_state"] == ws.READY_FOR_DAILY_CLOSE
    assert r["research_cycle_state"]["state"] == "COMPLETE"
    # 77 a completed close read model is unaffected by the research cycle status.
    assert r["operational_state"]["active_book_id"] == "alpha_paper_book_1"


# =========================================================================== #
# UI (required 78–92) — substring / structural.
# =========================================================================== #
def _ui():
    return UI.read_text(encoding="utf-8")


def test_78_79_exactly_one_status_loader_and_execution_function():
    html = _ui()
    assert html.count("function loadDailyResearchCycle") == 1
    assert html.count("function runDailyResearchCycle") == 1


def test_80_81_82_83_button_governed_by_backend_and_no_duplicate_submit():
    html = _ui()
    assert 'id="drc-run-btn"' in html and "onclick=\"runDailyResearchCycle(this)\"" in html
    # Disabled by default in markup; enabled only by the backend `executable` flag.
    assert 'id="drc-run-btn"' in html
    start = html.find("function runDailyResearchCycle")
    end = html.find("window.runDailyResearchCycle = runDailyResearchCycle")
    region = html[start:end]
    assert "window._drcRunning" in region       # duplicate-submission guard
    # Operator Action Integrity: the guard covers BOTH origins — the Daily Workflow
    # run button (mirrors the backend `executable` flag) and a canonical CTA
    # dispatched only when primary_action.execution_available is true. A disabled
    # origin never submits.
    assert "if (btn && btn.disabled) return;" in region
    assert "if (!btn && b && b.disabled) return;" in region
    render_start = html.find("function renderDailyResearchCycle")
    render = html[render_start:start]
    assert "btn.disabled = !d.executable" in render


def test_84_85_86_87_88_inputs_steps_top_snapshot_assessment_blocker_shown():
    html = _ui()
    for anchor in ('id="drc-state"', 'id="drc-steps"', 'id="drc-stale"', 'id="drc-top25"',
                   'id="drc-top50"', 'id="drc-snapshots"', 'id="drc-assessment"',
                   'id="drc-blockers"', 'id="drc-next"'):
        assert anchor in html, anchor


def test_89_90_ui_performs_no_date_or_priority_logic():
    html = _ui()
    start = html.find("function loadDailyResearchCycle")
    end = html.find("window.runDailyResearchCycle")
    region = html[start:end]
    for bad in ("new Date(", "Date.now(", ".getTime(", "build_execution_plan",
                "evaluate_alignment"):
        assert bad not in region


def test_91_no_hidden_month_boundary_primary_action_remains():
    html = _ui()
    # There is exactly ONE canonical daily research primary action (the DRC run).
    # No separate month-boundary / monthly-input primary button exists.
    assert html.count("function runDailyResearchCycle") == 1
    assert "runMonthlyInputEmitter" not in html
    assert "month-boundary-btn" not in html


def test_92_canonical_surfaces_remain_consistent():
    html = _ui()
    assert html.count("function loadWorkflowState") == 1        # Slice 2 intact
    assert html.count("function loadDataFreshness") == 1        # Slice 1 intact
    assert "function loadDailyResearchCycle" in html
    assert "try { loadDailyResearchCycle(); } catch (e) {}" in html  # wired to fan-out


# =========================================================================== #
# SAFETY (required 93–102)
# =========================================================================== #
def test_93_94_95_no_ledger_provider_or_prediction_in_source():
    forbidden = ("prediction_client", "yfinance", "httpx", "requests.get",
                 "urllib.request", "_eod_live_get", ":9000")
    for tok in forbidden:
        assert tok not in SRC, tok


def test_96_97_98_99_100_no_close_recovery_promotion_proposal_orders(tmp_path):
    r, f = _run(tmp_path, inputs=_inputs(price="2026-08-03", month="2026-08"))
    s = r["safety"]
    for flag in ("ran_daily_close", "created_orders", "created_signals",
                 "created_trade_decisions", "created_fills", "created_proposals",
                 "promoted_model", "recalibrated_model", "automatic_promotion_allowed",
                 "changed_holdings", "changed_cash_or_nav", "called_prediction",
                 "wrote_to_operational_ledger", "wrote_to_database"):
        assert s[flag] is False, flag


def test_101_no_scheduler_change_referenced():
    assert "ScheduledTask" not in SRC and "schtasks" not in SRC


def test_102_architecture_audit_ownership_holds():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "audit_arch", ROOT / "scripts" / "audit_architecture.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    files = mod._iter_source_files()
    dr = mod.check_daily_research_cycle_ownership(files)
    assert dr["owner_present"] and dr["sole_orchestrator"]
    assert dr["competing_orchestrators"] == [] and dr["missing_delegation"] == []
    assert dr["forbidden_execution_calls"] == []
    assert dr["status_endpoint_present"] and dr["run_endpoint_present"]
    assert dr["ui_status_loader_count"] == 1 and dr["ui_execution_function_count"] == 1
    assert dr["ui_planning_derivation"] == []
    assert mod.check_inventory_drift(files)["on_disk_not_in_inventory"] == []


# =========================================================================== #
# Vocabulary invariants.
# =========================================================================== #
def test_frozen_state_vocabulary_stable():
    assert set(drc.RUN_STATES) == {
        "NOT_STARTED", "WAITING_FOR_SESSION_CLOSE", "WAITING_FOR_OWNED_DATA", "PLANNING",
        "REFRESHING_REQUIRED_INPUTS", "VALIDATING_INPUT_ALIGNMENT", "SCORING_UNIVERSE",
        "PREPARING_TARGET", "CAPTURING_FORWARD_EVIDENCE", "RUNNING_PORTFOLIO_ASSESSMENT",
        "COMPLETE", "COMPLETE_WITH_EVIDENCE_GAP", "BLOCKED", "FAILED", "INCONSISTENT",
        "RUN_IN_PROGRESS"}
    assert drc.EXECUTE_CONFIRMATION == "RUN_DAILY_RESEARCH_CYCLE"


def test_bad_token_writes_nothing(tmp_path):
    r, f = _run(tmp_path, confirm="NOPE")
    assert r["status"] == "DAILY_RESEARCH_CYCLE_CONFIRM_REQUIRED"
    assert not list((tmp_path / "runs").glob("*.json"))
