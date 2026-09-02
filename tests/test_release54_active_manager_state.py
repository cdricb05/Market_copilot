"""R54 Slice 1 — the ONE Active Manager Operating State.

What these tests prove:

  * ``api.active_manager_state`` is a COMPOSITION/PROJECTION only: every value
    it publishes is the composed owner's own value verbatim (NAV, hashes,
    timestamps, decisions, economics, guidance); it defines no business
    calculation, calls no execution/orchestration path, owns no write path and
    reaches no scheduler;
  * the OPERATIONAL clock and the LIVE/INTRADAY research clock stay explicitly
    distinct, an intraday observation never backfills an operational mark, and
    only the Daily Close owner may advance the operational mark;
  * missing / stale components are explicit, each in its owner's OWN state
    vocabulary — never a fabricated value, never silence;
  * point-in-time identities (state hash, reassessment id/hash, proposal hash,
    scoring input-contract hash) pass through bound and verbatim;
  * the endpoint is GET-only; no order/fill/approval/promotion route exists;
  * Today consumes it through exactly ONE UI loader inside a marked region that
    performs no client-side date / freshness / decision math;
  * the R54 Slice-1 consolidation holds: the Today operational-mark pill
    (``cc-status-mark``) has exactly ONE unguarded writer (renderPortfolioState
    via ``_psOwnSet``); the legacy guard-free command-center write — whose
    fallback was the dormant legacy DB book's date — is gone and the strict
    architecture audit fails the build if it returns;
  * the R53.1 scheduler contracts and the R52 research runtime are untouched by
    this module, and the Release-50 decision-snapshot section contract is
    unchanged for existing consumers.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams

REPO = Path(__file__).resolve().parents[1]
SRC = (REPO / "api" / "active_manager_state.py").read_text(encoding="utf-8",
                                                           errors="replace")
UI = (REPO / "api" / "ui" / "index.html").read_text(encoding="utf-8",
                                                    errors="replace")
APP = (REPO / "api" / "app.py").read_text(encoding="utf-8", errors="replace")


# --------------------------------------------------------------------------- #
# Fixtures — compact owner payloads with the exact shapes the owners publish.
# --------------------------------------------------------------------------- #
def _wf() -> dict:
    return {
        "overall_state": "DAILY_CYCLE_COMPLETE",
        "current_task": "Monitor the portfolio",
        "headline": "All caught up",
        "primary_action": {"action_code": "MONITOR_PORTFOLIO", "label": "Monitor",
                           "execution_available": False,
                           "manual_confirmation_required": False},
        "operator_command": {"state": "DAILY_CYCLE_COMPLETE"},
        "queued_actions": [], "blockers": [], "warnings": [],
        "current_session": {"latest_eligible_completed_market_date": "2026-08-31"},
        "operational_state": {
            "active_book_id": "alpha_paper_book_1",
            "active_book_name": "Alpha Paper Book #1",
            "eligible_market_date": "2026-08-31",
            "desk_mark_date": "2026-08-31", "valuation_date": "2026-08-31",
            "latest_completed_close_date": "2026-08-31",
            "latest_close_status": "COMPLETE", "operational_close_valid": True,
            "nav": 99113.0, "cash": 1200.5, "holdings_count": 25,
            "pending_orders": 0,
            "operational_consistency_status": "CONSISTENT"},
        "research_state": {"latest_price_score_refresh_date": "2026-08-31",
                           "research_inputs_current": True,
                           "stale_source_ids": [], "missing_source_ids": []},
        "research_cycle_state": {
            "opportunity_cost_artifact_class": "GOVERNED_DRC_TERMINAL",
            "opportunity_cost_producer_owner": "api.daily_research_cycle",
            "governed_research_evidence_current": True},
        "portfolio_assessment_state": {
            "assessment_status": "CURRENT",
            "latest_assessment_current_for_eligible_session": True,
            "assessment_age_sessions": 0,
            "next_scheduled_review_date": "2026-09-01",
            "review_due": False, "review_overdue": False},
        "evidence_state": {"latest_snapshot_date": "2026-08-31"},
        "model_governance_state": {"champion": "composite_sn"},
        "model_review_state": "MODEL_HEALTHY",
        "model_review": {"model_review_state": "MODEL_HEALTHY"},
        "portfolio_reassessment": {"state": "HOLD_CURRENT_BOOK"},
        # Track B — the settled-aware presentation the reassessment owner
        # builds WITH the canonical decision lane (via api.workflow_state).
        "portfolio_reassessment_presentation": {
            "state": "PROPOSAL_READY",
            "operator_state": "PORTFOLIO_DECISION_SETTLED",
            "decision_settled": True,
            "settled_decision_state": "HOLD_CURRENT_BOOK",
            "settled_decision_owner": "api.portfolio_decision",
            "task": ("The requested change was priced by the governed "
                     "decision owner: the feasible alternative does not clear "
                     "the switching hurdle after cost; holding the current "
                     "book IS the decision"),
            "next_action": "No action required — the governed decision stands"},
        "holding_opportunity_cost_presentation": {
            "state": "AVAILABLE", "recommendation_counts": {"HOLD": 25},
            "assessment_hash": "hoc123"},
        "portfolio_decision_state": {"proposal_hash": "prop123",
                                     "decision_provenance": "GOVERNED_DRC_TERMINAL"},
        "reallocation_operator_state": "REALLOCATION_HOLD",
        "canonical_portfolio_decision": {"state": "HOLD_CURRENT_BOOK"},
        "portfolio_attention": {"attention_kind": "NONE", "review_required": False},
        "consistency_status": "CONSISTENT",
    }


def _ps() -> dict:
    return {"state": "PORTFOLIO_STATE_READY",
            "dates": {"eligible_market_date": "2026-08-31"},
            "capital": {"nav": 99113.0, "cash": 1200.5},
            "positions": [{"ticker": "T%02d" % i} for i in range(25)],
            "state_hash": "sh1", "economic_state_hash": "eh1",
            "active_book": {"book_id": "alpha_paper_book_1",
                            "book_name": "Alpha Paper Book #1"}}


def _esr() -> dict:
    return {"state": "REASSESSED_NO_CHANGE",
            "last_run": {"run_id": "evt_1", "state": "REASSESSED_NO_CHANGE",
                         "generated_at": "2026-09-01T14:54:37+00:00",
                         "reassessment_ran": True, "proposal_built": False,
                         "materiality": {"change_level": "MATERIAL"},
                         "portfolio_reassessment": {
                             "reassessment_state": "CURRENT_NO_CHANGE"},
                         "target_portfolio": None,
                         "calculations_refreshed": ["UNIVERSE_SCORING",
                                                    "MARKET_RISK_STATE"],
                         "affected_entities": ["T01", "T02"],
                         "rank_deltas": {"rows": [{"ticker": "T01"}],
                                         "prior_available": True}},
            "recent_events": [{"ingested_at": "2026-09-01T14:54:00+00:00"},
                              {"ingested_at": "2026-09-01T13:00:00+00:00"}],
            "material_events": [{"ingested_at": "2026-09-01T14:30:00+00:00"}],
            "material_event_count": 1,
            "affected_holdings": ["T01"]}


def _intraday() -> dict:
    return {"owner": "api.research_runtime",
            "state": "EMITTED",
            "evidence_class": "PROSPECTIVE_INTRADAY",
            "forward_evidence_type": "TRUE_FORWARD",
            "distinct_from_daily_governed_true_forward": True,
            "daily_governed_bundle_owner": "api.forward_prediction_skill",
            "last_attempt_at_utc": "2026-09-01T20:21:00Z",
            "last_attempt_appended": 0,
            "last_emission": {"emitted_at_utc": "2026-09-01T18:00:05Z",
                              "slot_utc": "2026-09-01T18:00:00Z",
                              "forward_evidence_type": "TRUE_FORWARD",
                              "evidence_class": "PROSPECTIVE_INTRADAY"},
            "ledger_totals": {"predictions": 72, "outcomes": 72,
                              "forfeitures": 0},
            "research_only": True}


def _reas() -> dict:
    return {"state": "NO_MATERIAL_CHANGE", "eligible_market_date": "2026-08-31",
            "reassessment_id": "ra_1", "reassessment_hash": "rh1",
            "artifact": {"generated_at": "2026-09-01T14:10:00+00:00"},
            "decision": {"holdings_evaluated": 25,
                         "reassessment_state": "NO_MATERIAL_CHANGE",
                         "expected_net_improvement": 0.012,
                         "net_improvement_hurdle": 0.05,
                         "expected_one_way_turnover": 0.1,
                         "expected_transaction_cost_usd": 25.0,
                         "proposal_required": False},
            "strongest_alternatives": [{"ticker": "ALT1"}],
            "explanation": "No change clears the hurdle."}


def _constr() -> dict:
    return {"outcome": "HOLD_CURRENT_BOOK",
            "outcome_vocabulary": ["PROPOSAL_READY", "HOLD_CURRENT_BOOK",
                                   "TRUE_BLOCKER"],
            "feasible_target_exists": True,
            "switching_economics": {"expected_net_improvement": 0.012,
                                    "net_improvement_hurdle": 0.05,
                                    "clears_hurdle": False,
                                    "expected_transaction_cost_usd": 25.0},
            "turnover": {"one_way": 0.1}, "risk": {"before": 0.1, "after": 0.09},
            "best_feasible_target": {"position_count": 25},
            "approval": {"portfolio_decision_state": "HOLD_CURRENT_BOOK",
                         "requires_manual_review": False},
            "execution": {"execution_active": False,
                          "rebalance_state": "REBALANCE_IDLE"}}


def _coll() -> dict:
    return {"service": {"service_state": "RUNNING", "worker_activity": "IDLE",
                        "reason": "healthy"}}


def _scoring() -> dict:
    return {"primary_model_id": "fundamental_momentum_50_50_v1",
            "champion_model_id": "composite_sn",
            "challenger_model_ids": ["mom_6_1"],
            "ranking_date": "2026-08-31", "input_contract_hash": "ich1",
            "scored_count": 430, "status": "UNIVERSE_SCORING_READY"}


def _full() -> dict:
    return ams.build_active_manager_state(
        workflow=_wf(), portfolio_state=_ps(), constrained=_constr(),
        information_collection=_coll(), rebalance={"rebalance_state": "REBALANCE_IDLE"},
        event_refresh=_esr(), reassessment=_reas(), scoring=_scoring(),
        runtime_health={"state": "HEALTHY", "owner": "api.research_runtime",
                        "n_runs_total": 12},
        intraday_emission=_intraday())


# --------------------------------------------------------------------------- #
# 1. Delegation is verbatim — the composed owners' own values, never recomputed.
# --------------------------------------------------------------------------- #
class TestVerbatimDelegation:
    def test_operational_book_verbatim(self):
        d = _full()
        ob = d["operational_book"]
        assert ob["nav"] == 99113.0
        assert ob["cash"] == 1200.5
        assert ob["holdings_count"] == 25
        assert ob["eligible_market_session"] == "2026-08-31"
        assert ob["operational_mark_date"] == "2026-08-31"
        assert ob["portfolio_state_hash"] == "sh1"
        assert ob["economic_state_hash"] == "eh1"
        assert ob["book_freshness"] == "CONSISTENT"

    def test_live_information_verbatim(self):
        d = _full()
        li = d["live_information"]
        assert li["collection_running"] is True
        assert li["worker_activity"] == "IDLE"
        assert li["last_event_cycle"]["run_id"] == "evt_1"
        assert li["last_observation_at"] == "2026-09-01T14:54:00+00:00"
        assert li["last_material_event_at"] == "2026-09-01T14:30:00+00:00"
        # ONE material event, stamped AFTER the persisted reassessment stamp.
        assert li["material_events_since_last_reassessment"] == 1
        assert li["affected_current_holdings"] == ["T01"]

    def test_signal_state_verbatim(self):
        sg = _full()["signal_state"]
        assert sg["last_signal_refresh_at"] == "2026-09-01T14:54:37+00:00"
        assert sg["last_scoring_ranking_date"] == "2026-08-31"
        assert sg["scored_universe_count"] == 430
        assert sg["ranking_snapshot_id"] == "ich1"
        assert sg["last_prediction_emission_date"] == "2026-08-31"

    def test_reassessment_verbatim(self):
        rs = _full()["portfolio_reassessment"]
        assert rs["holdings_evaluated"] == 25
        assert rs["alternatives_evaluated"] == 1
        assert rs["expected_net_improvement"] == 0.012
        assert rs["net_improvement_hurdle"] == 0.05
        assert rs["reassessment_id"] == "ra_1"
        assert rs["reassessment_hash"] == "rh1"
        assert rs["reassessment_trigger"]["artifact_class"] == "GOVERNED_DRC_TERMINAL"
        assert rs["reassessment_trigger"]["governed_daily_cycle"] is True
        assert rs["reassessment_freshness"] == "CURRENT"
        assert rs["hoc_summary"]["assessment_hash"] == "hoc123"

    def test_target_proposal_verbatim(self):
        tp = _full()["target_proposal"]
        assert tp["target_state"] == "HOLD_CURRENT_BOOK"
        assert tp["expected_improvement"] == 0.012
        assert tp["switching_hurdle"] == 0.05
        assert tp["clears_hurdle"] is False
        assert tp["estimated_cost"] == 25.0
        assert tp["target_snapshot_id"] == "prop123"
        assert tp["decision_provenance"] == "GOVERNED_DRC_TERMINAL"

    def test_operator_guidance_verbatim(self):
        gd = _full()["operator_guidance"]
        assert gd["overall_state"] == "DAILY_CYCLE_COMPLETE"
        assert gd["current_task"] == "Monitor the portfolio"
        assert gd["next_action"]["action_code"] == "MONITOR_PORTFOLIO"
        assert gd["consistency_status"] == "CONSISTENT"

    def test_component_owner_map_complete(self):
        d = _full()
        assert set(d["components"]) == set(ams.COMPONENT_OWNERS)
        for c in d["components"]:
            assert d[c]["owner"] == ams.COMPONENT_OWNERS[c]


# --------------------------------------------------------------------------- #
# 2. The time-state distinction — two clocks, never conflated.
# --------------------------------------------------------------------------- #
class TestTimeStateDistinction:
    def test_operational_and_live_clocks_are_distinct(self):
        ts = _full()["time_state"]
        assert ts["distinct"] is True
        assert ts["operational"]["eligible_market_session"] == "2026-08-31"
        assert ts["live_research"]["last_observation_at"].startswith("2026-09-01")
        assert ts["operational_mark_advanced_only_by"] == "api.daily_close"
        assert "never becomes an operational close mark" in ts["statement"]

    def test_intraday_observation_never_backfills_operational_mark(self):
        # No operational mark exists; live observations are newer. The
        # operational block must stay honestly empty — never borrow a live stamp.
        d = ams.build_active_manager_state(
            workflow={"operational_state": {}}, portfolio_state={},
            event_refresh=_esr())
        ob = d["operational_book"]
        assert ob["operational_mark_date"] is None
        assert ob["eligible_market_session"] is None
        assert d["time_state"]["operational"]["operational_mark_date"] is None
        assert (d["time_state"]["live_research"]["last_observation_at"]
                == "2026-09-01T14:54:00+00:00")

    def test_material_count_absent_without_reassessment_stamp(self):
        d = ams.build_active_manager_state(event_refresh=_esr())
        assert d["live_information"]["material_events_since_last_reassessment"] is None
        assert d["live_information"]["since_basis"] is None


# --------------------------------------------------------------------------- #
# 3. Missing / stale components are explicit, in each owner's own words.
# --------------------------------------------------------------------------- #
class TestStaleAndMissing:
    def test_empty_build_reports_every_component_missing(self):
        d = ams.build_active_manager_state()
        rows = {r["component"]: r["owner_state"] for r in d["stale_components"]}
        for c in ("operational_book", "live_information", "signal_state",
                  "portfolio_reassessment", "target_proposal",
                  "research_governance"):
            assert rows.get(c) == "MISSING"

    def test_stopped_collection_quotes_the_owner_state(self):
        d = ams.build_active_manager_state(
            information_collection={"service": {"service_state": "STOPPED",
                                                "reason": "operator stopped it"}})
        rows = [r for r in d["stale_components"]
                if r["component"] == "live_information"]
        assert rows and rows[0]["owner_state"] == "STOPPED"
        assert rows[0]["detail"] == "operator stopped it"

    def test_stale_reassessment_quotes_workflow_currency(self):
        wf = _wf()
        wf["portfolio_assessment_state"]["assessment_status"] = "OVERDUE"
        d = ams.build_active_manager_state(workflow=wf, reassessment=_reas())
        rows = [r for r in d["stale_components"]
                if r["component"] == "portfolio_reassessment"]
        assert rows and rows[0]["owner_state"] == "OVERDUE"

    def test_full_healthy_build_reports_no_stale_components(self):
        assert _full()["stale_components"] == []


# --------------------------------------------------------------------------- #
# 4. Composition-only: no calculation, no execution, no write, no scheduler.
# --------------------------------------------------------------------------- #
class TestCompositionOnly:
    FORBIDDEN_DEFS = (
        "def book_nav(", "def compute_scores(", "def _percentiles(",
        "def solve_feasible_target(", "def switching_economics(",
        "def decide_outcome(", "def build_assessment(", "def build_proposal(",
        "def settle_due_orders(", "def _append_ledger(", "def score_universe(",
        "def rank_universe(", "def assess_materiality(")
    FORBIDDEN_CALLS = (
        "run_event_signal_refresh(", "run_reassessment(", "run_proposal(",
        "run_daily_close(", "run_daily_research_cycle(", "run_refresh(",
        "run_portfolio_cycle(", "subprocess", "schtasks",
        "Register-ScheduledTask", "requests.", "httpx")
    FORBIDDEN_WRITES = ("write_text", "json.dump", "os.replace", "mkdir(",
                        "open(", "unlink(", "rename(")

    def test_no_business_calculation_defs(self):
        hits = [d for d in self.FORBIDDEN_DEFS if d in SRC]
        assert hits == []

    def test_no_execution_or_scheduler_reach(self):
        hits = [c for c in self.FORBIDDEN_CALLS if c in SRC]
        assert hits == []

    def test_no_write_path(self):
        hits = [w for w in self.FORBIDDEN_WRITES if w in SRC]
        assert hits == []

    def test_no_forked_owner_imports(self):
        # The module composes READ contracts only. Its import surface is
        # closed: any import of a calculation kernel, an execution owner, the
        # desk, the close writer or a research package is a fork.
        import_lines = [ln.strip() for ln in SRC.splitlines()
                        if ln.strip().startswith(("import ", "from "))]
        allowed = (
            "from __future__ import annotations",
            "from datetime import datetime, timezone",
            "from typing import Any, Callable, Optional",
            "from paper_trader.api import decision_snapshot as snap",
            "from paper_trader.api import event_signal_refresh as esr",
            "from paper_trader.api import portfolio_reassessment as prs",
            "from paper_trader.api import universe_scoring as us",
            "from paper_trader.api import research_runtime as rr",
            # R54.1 — the governed portfolio decision is READ from the ONE
            # decision owner. It is a read contract like the others above; this
            # module still performs no governance logic and resolves no
            # supersession (guarded by test_37b in the R54.1 suite and by the
            # strict audit's read_model_defines_gate invariant).
            "from paper_trader.api import portfolio_decision as pdec",
        )
        unexpected = [ln for ln in import_lines if ln not in allowed]
        assert unexpected == []

    def test_payload_declares_composition_only(self):
        d = _full()
        assert d["read_only"] is True
        assert d["business_calculation_owner"] is False
        assert d["recomputes_nothing"] is True
        assert d["safety"]["performed_write"] is False
        assert d["safety"]["automation_enabled"] is False
        assert d["safety"]["broker_enabled"] is False

    def test_no_automatic_promotion_or_approval(self):
        d = _full()
        assert d["research_governance"]["automatic_promotion_allowed"] is False
        assert d["execution_safety"]["manual_approval_required"] is True
        assert d["execution_safety"]["automation_enabled"] is False
        assert d["execution_safety"]["order_routes_exist"] is False

    def test_loader_composes_injected_owners_without_side_effects(self):
        calls = []

        def _mk(name, payload):
            def _fn():
                calls.append(name)
                return payload
            return _fn

        d = ams.load_active_manager_state(loaders={
            "workflow": _mk("workflow", _wf()),
            "portfolio_state": _mk("portfolio_state", _ps()),
            "constrained": _mk("constrained", _constr()),
            "information_collection": _mk("collection", _coll()),
            "rebalance": _mk("rebalance", {"rebalance_state": "REBALANCE_IDLE"}),
            "event_refresh": _mk("event_refresh", _esr()),
            "reassessment": _mk("reassessment", _reas()),
            "scoring": _mk("scoring", _scoring()),
            "runtime_health": _mk("runtime", {"state": "HEALTHY"}),
            "intraday_emission": _mk("intraday_emission", _intraday()),
        })
        assert d["operational_book"]["nav"] == 99113.0
        assert sorted(calls) == sorted([
            "workflow", "portfolio_state", "constrained", "collection",
            "rebalance", "event_refresh", "reassessment", "scoring", "runtime",
            "intraday_emission"])

    def test_loader_degrades_per_owner_never_crashes(self):
        def _boom():
            raise RuntimeError("owner down")

        d = ams.load_active_manager_state(loaders={
            "workflow": _boom, "portfolio_state": _boom, "constrained": _boom,
            "information_collection": _boom, "rebalance": _boom,
            "event_refresh": _boom, "reassessment": _boom, "scoring": _boom,
            "runtime_health": _boom, "intraday_emission": _boom})
        assert d["stale_component_count"] >= 6
        assert any("workflow unavailable" in w for w in d["warnings"])


# --------------------------------------------------------------------------- #
# 4b. R54 finalization — governed-vs-live decision authority semantics.
# --------------------------------------------------------------------------- #
def _esr_live_proposal() -> dict:
    e = _esr()
    e["last_run"].update({
        "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
        "reassessment_ran": True, "proposal_built": True,
        "portfolio_reassessment": {"reassessment_state": "PROPOSAL_READY"},
        "target_portfolio": {"proposal_state": "READY"}})
    e["state"] = "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW"
    return e


class TestDecisionAuthoritySemantics:
    def _settled(self) -> dict:
        return ams.build_active_manager_state(
            workflow=_wf(), portfolio_state=_ps(), constrained=_constr(),
            information_collection=_coll(), event_refresh=_esr_live_proposal(),
            reassessment=dict(_reas(), state="PROPOSAL_READY"),
            scoring=_scoring(), intraday_emission=_intraday())

    def test_governed_hold_and_live_proposal_coexist_without_contradiction(self):
        # The exact live 2026-09-01 configuration: raw reassessment state
        # PROPOSAL_READY, live event cycle PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW,
        # governed target HOLD_CURRENT_BOOK below the hurdle. The payload must
        # carry the owner's settled reconciliation, never just the raw token.
        d = self._settled()
        rs = d["portfolio_reassessment"]
        assert rs["state"] == "PROPOSAL_READY"                    # raw, verbatim
        assert rs["operator_state"] == "PORTFOLIO_DECISION_SETTLED"
        assert rs["decision_settled"] is True
        assert rs["settled_decision_state"] == "HOLD_CURRENT_BOOK"
        assert rs["settled_decision_owner"] == "api.portfolio_decision"
        assert "holding the current book IS the decision" in rs["operator_task"]
        assert rs["operator_next_action"].startswith("No action required")
        assert d["target_proposal"]["target_state"] == "HOLD_CURRENT_BOOK"
        assert d["target_proposal"]["clears_hurdle"] is False

    def test_decision_authority_ladder_names_five_owners(self):
        da = self._settled()["decision_authority"]
        assert da["live_intraday_assessment"]["owner"] == "api.event_signal_refresh"
        assert (da["governed_portfolio_reassessment"]["owner"]
                == "api.portfolio_reassessment")
        assert da["governed_target"]["owner"] == "api.reallocation_proposal"
        assert da["manual_review_candidate"]["owner"] == "api.portfolio_decision"
        assert da["approved_decision"]["owner"] == "api.portfolio_decision"
        assert "reassessed many times per trading day" in da["statement"]

    def test_live_cycle_never_advances_governed_decision_or_mark(self):
        da = self._settled()["decision_authority"]
        live = da["live_intraday_assessment"]
        assert live["state"] == "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW"
        assert live["advances_governed_decision"] is False
        assert live["advances_operational_mark"] is False
        assert "does not, by itself, recommend a portfolio change" in \
            live["state_note"]

    def test_proposal_available_token_carries_the_target_owners_own_outcome(self):
        lc = self._settled()["live_information"]["last_event_cycle"]
        assert lc["state"] == "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW"
        assert lc["proposal_built"] is True
        assert lc["reassessment_state"] == "PROPOSAL_READY"
        assert lc["proposal_state"] == "READY"
        assert lc["advances_governed_decision"] is False

    def test_nothing_is_auto_approved(self):
        d = self._settled()
        assert (d["decision_authority"]["approved_decision"]
                ["recorded_only_by_manual_confirmation"] is True)
        assert d["execution_safety"]["manual_approval_required"] is True
        assert d["safety"]["approved_anything"] is False

    def test_canonical_current_decision_is_the_workflow_owners_verbatim(self):
        wf = _wf()
        wf["canonical_portfolio_decision"] = {
            "state": "HOLD_CURRENT_BOOK", "headline": "HOLD THE CURRENT BOOK"}
        d = ams.build_active_manager_state(workflow=wf, constrained=_constr())
        assert (d["decision_authority"]["canonical_current_decision"]
                == wf["canonical_portfolio_decision"])

    def test_overdue_governed_with_newer_live_cycle_is_self_explaining(self):
        wf = _wf()
        wf["portfolio_assessment_state"] = {
            "assessment_status": "OVERDUE",
            "latest_assessment_current_for_eligible_session": True,
            "assessment_age_sessions": 0,
            "next_scheduled_review_date": "2026-08-01",
            "review_due": True, "review_overdue": True}
        d = ams.build_active_manager_state(
            workflow=wf, portfolio_state=_ps(),
            event_refresh=_esr_live_proposal(), reassessment=_reas())
        rs = d["portfolio_reassessment"]
        fd = rs["reassessment_freshness_detail"]
        assert rs["reassessment_freshness"] == "OVERDUE"
        assert fd["current_for_eligible_session"] is True
        assert fd["next_scheduled_review_date"] == "2026-08-01"
        assert fd["review_overdue"] is True
        assert fd["advanced_by_live_event_cycles"] is False
        rows = [r for r in d["stale_components"]
                if r["component"] == "portfolio_reassessment"]
        assert rows and rows[0]["owner_state"] == "OVERDUE"
        assert "current for the eligible session" in rows[0]["detail"]
        assert "2026-08-01" in rows[0]["detail"]
        # The newer live cycle is visible on the live clock and the
        # operational mark is untouched — two clocks, still distinct.
        assert (d["time_state"]["live_research"]["last_event_cycle_at"]
                == "2026-09-01T14:54:37+00:00")
        assert (d["time_state"]["operational"]["operational_mark_date"]
                == "2026-08-31")


# --------------------------------------------------------------------------- #
# 4c. R54 finalization — full-universe vs incremental scoring semantics.
# --------------------------------------------------------------------------- #
class TestScoringSemantics:
    def test_scoring_scope_and_basis_are_explicit(self):
        sg = _full()["signal_state"]
        assert sg["scoring_basis"]["scope"] == "FULL_UNIVERSE_RECOMPUTE"
        assert sg["scoring_basis"]["owner"] == "api.universe_scoring"
        assert "point-in-time data basis" in sg["scoring_basis"]["basis"]
        assert sg["last_full_universe_scoring"]["ranking_basis_date"] == "2026-08-31"
        assert sg["last_full_universe_scoring"]["scored_count"] == 430
        assert sg["last_full_universe_scoring"]["input_contract_hash"] == "ich1"

    def test_incremental_refresh_facts_are_the_cycle_owners_verbatim(self):
        sg = _full()["signal_state"]
        inc = sg["last_incremental_signal_refresh"]
        assert inc["at"] == "2026-09-01T14:54:37+00:00"
        assert inc["calculations_refreshed"] == ["UNIVERSE_SCORING",
                                                 "MARKET_RISK_STATE"]
        assert inc["affected_names_refreshed"] == ["T01", "T02"]
        assert inc["held_rank_delta_rows"] == 1
        assert inc["prior_ranking_available"] is True

    def test_unpersisted_scoring_facts_are_declared_not_fabricated(self):
        sg = _full()["signal_state"]
        joined = " ".join(sg["not_persisted_facts"])
        assert "affected_names_rescored" in joined
        assert "latest_rank_change_timestamp" in joined

    def test_absent_cycle_reports_absent_facts_never_borrowed(self):
        d = ams.build_active_manager_state(scoring=_scoring())
        inc = d["signal_state"]["last_incremental_signal_refresh"]
        assert inc["at"] is None
        assert inc["calculations_refreshed"] is None
        assert inc["held_rank_delta_rows"] is None

    def test_owner_run_summary_is_preferred_over_the_pointer(self):
        # Production shape: last_run is the 4-field latest.json POINTER; the
        # cycle owner's last_run_summary (built from its own persisted run
        # payload) carries the decision facts. The projection layers the
        # summary over the pointer — verbatim, no re-derivation.
        e = {"state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
             "last_run": {"run_id": "evt_9",
                          "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                          "generated_at": "2026-09-01T20:14:27+00:00",
                          "run_dir": "x"},
             "last_run_summary": {
                 "run_id": "evt_9",
                 "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                 "generated_at": "2026-09-01T20:14:27+00:00",
                 "reassessment_ran": True, "proposal_built": True,
                 "materiality_change_level": "MATERIAL",
                 "calculations_refreshed": ["UNIVERSE_SCORING"],
                 "affected_entities": ["T05"],
                 "held_rank_delta_rows": 25,
                 "prior_ranking_available": True,
                 "reassessment_state": "PROPOSAL_READY",
                 "proposal_state": "READY"},
             "recent_events": [], "material_events": []}
        d = ams.build_active_manager_state(event_refresh=e)
        lc = d["live_information"]["last_event_cycle"]
        assert lc["proposal_built"] is True
        assert lc["reassessment_state"] == "PROPOSAL_READY"
        assert lc["proposal_state"] == "READY"
        assert lc["materiality_change_level"] == "MATERIAL"
        inc = d["signal_state"]["last_incremental_signal_refresh"]
        assert inc["held_rank_delta_rows"] == 25
        assert inc["prior_ranking_available"] is True
        assert inc["affected_names_refreshed"] == ["T05"]


# --------------------------------------------------------------------------- #
# 4d. R54 finalization — the two forward-evidence identities stay distinct.
# --------------------------------------------------------------------------- #
class TestEvidenceIdentity:
    def test_two_distinct_identities_never_interchanged(self):
        sg = _full()["signal_state"]
        assert sg["latest_governed_true_forward_date"] == "2026-08-31"
        assert sg["governed_true_forward_owner"] == "api.forward_prediction_skill"
        ie = sg["latest_intraday_prospective_emission"]
        assert ie["evidence_class"] == "PROSPECTIVE_INTRADAY"
        assert ie["distinct_from_daily_governed_true_forward"] is True
        assert ie["last_emission"]["slot_utc"] == "2026-09-01T18:00:00Z"
        assert ie["ledger_totals"]["predictions"] == 72

    def test_absent_intraday_lane_never_borrows_the_governed_date(self):
        d = ams.build_active_manager_state(workflow=_wf())
        sg = d["signal_state"]
        assert sg["latest_governed_true_forward_date"] == "2026-08-31"
        assert sg["latest_intraday_prospective_emission"] is None
        assert (d["time_state"]["live_research"]
                ["last_intraday_prospective_emission_at"] is None)

    def test_intraday_emission_stamp_lives_on_the_live_clock_only(self):
        d = _full()
        assert (d["time_state"]["live_research"]
                ["last_intraday_prospective_emission_at"]
                == "2026-09-01T18:00:05Z")
        assert d["time_state"]["operational"]["operational_mark_date"] == "2026-08-31"

    def test_read_surface_reports_no_attempt_hermetically(self, tmp_path,
                                                          monkeypatch):
        import paper_trader.alpha_agent.r53_1 as R1
        from paper_trader.api import research_runtime as rr
        monkeypatch.setattr(R1, "RESEARCH_ROOT", tmp_path)
        st = rr.load_intraday_emission_status()
        assert st["state"] == "NO_EMISSION_ATTEMPT_RECORDED"
        assert st["distinct_from_daily_governed_true_forward"] is True
        assert st["research_only"] is True
        # A read surface must not create research directories on a GET.
        assert list(tmp_path.iterdir()) == []

    def test_read_surface_projects_the_artifact_verbatim(self, tmp_path,
                                                         monkeypatch):
        import json as _json

        import paper_trader.alpha_agent.r53_1 as R1
        import paper_trader.alpha_agent.r53.intraday_factory as factory
        from paper_trader.api import research_runtime as rr
        monkeypatch.setattr(R1, "RESEARCH_ROOT", tmp_path)
        d = tmp_path / R1.CAMPAIGN_ID
        d.mkdir(parents=True)
        (d / rr.INTRADAY_EMISSION_ARTIFACT).write_text(_json.dumps({
            "attempted_at_utc": "2026-09-01T20:21:00Z",
            "lane_state": "AVAILABLE_NOW",
            "emission": {"state": "NOT_AN_EMISSION_SLOT", "n_appended": 0},
            "scoring": {"state": "SCORED", "n_scored": 0},
            "ledger_totals": {"predictions": 72, "outcomes": 72,
                              "forfeitures": 0}}), encoding="utf-8")
        monkeypatch.setattr(factory, "predictions", lambda: [
            {"emitted_at_utc": "2026-09-01T18:00:05Z",
             "slot_utc": "2026-09-01T18:00:00Z",
             "forward_evidence_type": "TRUE_FORWARD",
             "evidence_class": "PROSPECTIVE_INTRADAY"}])
        st = rr.load_intraday_emission_status()
        assert st["state"] == "NOT_AN_EMISSION_SLOT"
        assert st["last_attempt_at_utc"] == "2026-09-01T20:21:00Z"
        assert st["lane_state"] == "AVAILABLE_NOW"
        assert st["last_emission"]["slot_utc"] == "2026-09-01T18:00:00Z"
        assert st["last_emission"]["evidence_class"] == "PROSPECTIVE_INTRADAY"
        assert st["ledger_totals"]["predictions"] == 72
        assert st["daily_governed_bundle_owner"] == "api.forward_prediction_skill"

    def test_read_surface_has_no_write_or_mkdir_path(self):
        import inspect

        from paper_trader.api import research_runtime as rr
        src = inspect.getsource(rr.load_intraday_emission_status)
        for banned in ("mkdir(", "write_text", "json.dump", "os.replace",
                       "unlink", "rename(", "research_dir()", "ledger_dir()"):
            assert banned not in src, banned


# --------------------------------------------------------------------------- #
# 5. Route contract: GET-only, no new mutation surface.
# --------------------------------------------------------------------------- #
class TestRouteContract:
    def test_route_declared_get_exactly_once(self):
        assert APP.count('"/v1/operations/active-manager-state"') == 1
        i = APP.find('"/v1/operations/active-manager-state"')
        assert "@app.get(" in APP[i - 80:i]

    def test_no_mutating_route_for_the_owner(self):
        for verb in ("@app.post", "@app.put", "@app.delete", "@app.patch"):
            seg = APP
            j = 0
            while True:
                j = seg.find(verb, j)
                if j == -1:
                    break
                assert "active-manager-state" not in seg[j:j + 200]
                j += 1


# --------------------------------------------------------------------------- #
# 6. Today: ONE loader, a marked region, no client-side state math.
# --------------------------------------------------------------------------- #
class TestTodayConsumer:
    def _region(self) -> str:
        i = UI.find("/* R54_REGION_START */")
        j = UI.find("/* R54_REGION_END */", i)
        assert i != -1 and j > i
        return UI[i:j]

    def test_one_canonical_loader_and_fetch(self):
        assert UI.count("function loadActiveManagerState(") == 1
        assert UI.count("_opFetch('/v1/operations/active-manager-state')") == 1

    def test_today_section_declares_the_owner(self):
        assert ('id="today-operating-state" data-owner='
                '"api.active_manager_state"') in UI

    def test_region_performs_no_client_state_math(self):
        region = self._region()
        for banned in ("Math.", "new Date(", "Date.now(", "toLocaleString(",
                       "reduce("):
            assert banned not in region, banned

    def test_region_renders_backend_words_only(self):
        region = self._region()
        # The renderer reads composed fields; it must not re-derive a decision
        # or a freshness verdict from raw sub-owner payloads.
        for banned in ("clears_hurdle ?", "reassessment_required",
                       "getTime(", "setHours("):
            assert banned not in region, banned


# --------------------------------------------------------------------------- #
# 6b. R54 finalization — the UI renders backend words; it interprets nothing.
# --------------------------------------------------------------------------- #
class TestNoUIDecisionInterpretation:
    def _region(self) -> str:
        i = UI.find("/* R54_REGION_START */")
        j = UI.find("/* R54_REGION_END */", i)
        assert i != -1 and j > i
        return UI[i:j]

    def test_renderer_never_branches_on_decision_tokens(self):
        # The strip must render whatever the backend says. A literal decision
        # token in the region means the UI started authoring interpretation.
        region = self._region()
        for banned in ("'HOLD_CURRENT_BOOK'", '"HOLD_CURRENT_BOOK"',
                       "'PROPOSAL_READY'", '"PROPOSAL_READY"',
                       "'PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW'",
                       "'PORTFOLIO_DECISION_SETTLED'",
                       "'OVERDUE'", "'GOVERNED_DRC_TERMINAL'"):
            assert banned not in region, banned

    def test_renderer_uses_the_backend_semantic_fields_verbatim(self):
        region = self._region()
        for needed in ("rs.operator_state || rs.state",
                       "rs.operator_next_action",
                       "lc.state_note",
                       "sb.basis",
                       "latest_governed_true_forward_date",
                       "latest_intraday_prospective_emission",
                       "da.statement",
                       "canonical_current_decision"):
            assert needed in region, needed

    def test_stale_detail_is_surfaced_not_reworded(self):
        region = self._region()
        assert "s.detail || ''" in region


# --------------------------------------------------------------------------- #
# 7. The Slice-1 consolidation: cc-status-mark has ONE unguarded writer.
# --------------------------------------------------------------------------- #
class TestOperationalMarkSingleWriter:
    def test_legacy_guard_free_writer_removed(self):
        assert "_ccSetText('cc-status-mark'" not in UI

    def test_exactly_one_canonical_writer(self):
        assert UI.count("_psOwnSet('cc-status-mark'") == 1

    def test_guarded_early_writer_respects_ownership(self):
        assert "_obSet('cc-status-mark'" in UI
        i = UI.find("function _obSet(")
        body = UI[i:i + 400]
        assert "_psIsCanonicalNode(id)" in body and "return" in body

    def test_node_is_registered_canonical(self):
        assert "'cc-status-mark': 1" in UI


# --------------------------------------------------------------------------- #
# 8. Audit guard: the strict audit fails the build on any regression.
# --------------------------------------------------------------------------- #
class TestAuditGuard:
    def _aud(self):
        return importlib.import_module("scripts.audit_architecture")

    def test_check_is_green_now(self):
        aud = self._aud()
        rep = aud.check_release54_active_manager_state([])
        assert rep["owner_present"] is True
        assert rep["declares_owner"] is True
        assert rep["composition_only_declared"] is True
        assert rep["composes_decision_snapshot"] is True
        assert rep["forbidden_calculation_defs"] == []
        assert rep["forbidden_execution_tokens"] == []
        assert rep["time_state_distinction_declared"] is True
        assert rep["route_get_count"] == 1
        assert rep["non_get_route_present"] is False
        assert rep["ui_loader_count"] == 1
        assert rep["ui_fetch_count"] == 1
        assert rep["ui_region_present"] is True
        assert rep["ui_region_forbidden"] == []
        assert rep["legacy_status_mark_writer_present"] is False
        assert rep["canonical_status_mark_writer_count"] == 1
        assert rep["status_mark_guarded_early_writer_present"] is True
        assert rep["decision_authority_declared"] is True
        assert rep["evidence_identities_distinct"] is True

    def test_every_field_is_a_blocking_invariant(self):
        aud = self._aud()
        keys = {(k, f) for (k, f, _v) in aud.BLOCKING_INVARIANTS}
        for field in ("owner_present", "declares_owner",
                      "composition_only_declared", "composes_decision_snapshot",
                      "forbidden_calculation_defs", "forbidden_execution_tokens",
                      "time_state_distinction_declared", "route_get_count",
                      "non_get_route_present", "ui_loader_count",
                      "ui_fetch_count", "ui_region_present",
                      "ui_region_forbidden", "legacy_status_mark_writer_present",
                      "canonical_status_mark_writer_count",
                      "status_mark_guarded_early_writer_present",
                      "decision_authority_declared",
                      "evidence_identities_distinct",
                      "automatic_model_promotion_allowed",
                      "automatic_approval_allowed", "cadence_enabled"):
            assert ("release54_active_manager_state", field) in keys, field

    def test_reintroduced_legacy_writer_is_caught(self, monkeypatch):
        aud = self._aud()
        real_read = aud._read

        def _tampered(rel_path):
            out = real_read(rel_path)
            if rel_path == aud.UI_FILE:
                out += "\n_ccSetText('cc-status-mark', pf.as_of_market_date);\n"
            return out

        monkeypatch.setattr(aud, "_read", _tampered)
        rep = aud.check_release54_active_manager_state([])
        assert rep["legacy_status_mark_writer_present"] is True


# --------------------------------------------------------------------------- #
# 9. Neighbours untouched: R50 snapshot contract, R52 runtime, R53.1 tasks.
# --------------------------------------------------------------------------- #
class TestNeighboursUntouched:
    def test_decision_snapshot_section_contract_unchanged(self):
        from paper_trader.api import decision_snapshot as snap
        assert snap.SECTIONS == (
            "operational", "portfolio_state", "workflow", "daily_close",
            "decision_lane", "rebalance", "constrained", "capital_pool",
            "material_information", "decision_outcomes",
            "information_collection", "presentation")

    def test_operator_presentation_still_importable_and_owner(self):
        from paper_trader.api import operator_presentation as op
        assert op.OWNER == "api.operator_presentation"
        assert callable(op.build_operator_presentation)

    def test_scheduler_scripts_not_touched_by_the_owner(self):
        for script in ("install_information_collection_task.ps1",
                       "install_intraday_emission_task.ps1",
                       "install_research_runtime_task.ps1"):
            assert (REPO / "scripts" / script).exists()
            assert script not in SRC

    def test_r52_runtime_package_not_imported(self):
        assert "alpha_agent" not in SRC
        assert "from paper_trader.alpha_agent" not in SRC


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
