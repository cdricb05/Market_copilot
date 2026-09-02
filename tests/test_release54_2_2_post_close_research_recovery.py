"""Release 54.2.2 — POST-CLOSE RESEARCH RECOVERY + CLOSE-ATTRIBUTION INTEGRITY.

R54.2.1 taught the workflow owner to remember a completed session that was never
CLOSED. The Sep-1 recovery then worked, and immediately exposed the same defect one
stage further down: the moment the close completed, the SAME gate claimed the session
was "fully processed" and returned WAITING_FOR_SESSION_CLOSE for the next open
session — while the same payload reported two stale research inputs, no governed
Daily Research Cycle manifest for the closed session, and a portfolio decision built
on a LIVE_PRE_DRC signal artifact.

This suite proves:

  * a completed Daily Close does NOT settle the governed research owed for that
    session, and the outstanding obligation outranks "wait for the next close";
  * recovery resumes through the ONE portfolio cycle, which does not repeat the
    completed close, takes no date from the operator and adds no second route;
  * every stale research input is CLASSIFIED by its owner, and a safe one, a
    dependency-blocked one and a true blocker are three different answers;
  * later data can never enter an earlier session's governed research;
  * the documented TRUE_FORWARD gap stays documented and never invalidates a close;
  * a research-only condition is ATTENTION, not a red service-wide BLOCKED banner;
  * attribution FAILS CLOSED — an unreconciled decomposition is UNAVAILABLE, never
    "every holding contributed $0" — and no historical row is ever rewritten.

Hermetic: every write-path assertion runs against a temp store. Nothing here reads
or mutates the production ledger, runs a close, a cycle or an emitter, or touches the
live backend.
"""
from __future__ import annotations

import io
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import daily_close as dclose
from paper_trader.api import daily_research_cycle as drc
from paper_trader.api import forward_evidence as fe
from paper_trader.api import operator_presentation as opres
from paper_trader.api import portfolio_cycle as pcycle
from paper_trader.api import workflow_state as ws

REPO = Path(__file__).resolve().parents[1]
SEP1, AUG31, SEP2 = "2026-09-01", "2026-08-31", "2026-09-02"


_SRC_CACHE: dict[str, str] = {}


def _src(rel: str) -> str:
    if rel not in _SRC_CACHE:
        with io.open(REPO / rel, encoding="utf-8") as fh:
            _SRC_CACHE[rel] = fh.read()
    return _SRC_CACHE[rel]


def _code_only(src: str) -> str:
    """Strip comments and string literals so a token search matches CODE, not prose.

    Carried forward from the R54.2.1 suite: this repository documents its reasoning
    inline, so a naive substring search finds the sentence explaining why something
    is forbidden and reports it as present.
    """
    src = re.sub(r'"""(?:.|\n)*?"""', '""', src)
    src = re.sub(r"'''(?:.|\n)*?'''", "''", src)
    src = re.sub(r"(?m)#.*$", "", src)
    src = re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', src)
    src = re.sub(r"'(?:[^'\\\n]|\\.)*'", "''", src)
    return src


# --------------------------------------------------------------------------- #
# Fixtures — pure inputs to the pure owners.
# --------------------------------------------------------------------------- #
def _plan(*, eligible=SEP1, stale=("momentum_monthly", "price_score_refresh")):
    return {
        "eligible_market_date": eligible,
        "required_stale_inputs": list(stale),
        "slower_inputs_due": ["momentum_monthly"] if "momentum_monthly" in stale else [],
        "refresh_steps": [{"source_id": s, "display_name": s} for s in stale],
        "blockers": [], "plan_blocked": False,
    }


def _monthly(*, available=True, covered=True, panel="2026-09-01",
            cur="2026-08", req="2026-09"):
    return {"owner": "api.monthly_momentum_input", "available": available,
            "source_panel_covered": covered, "source_panel_date": panel,
            "current_month": cur, "required_month": req,
            "missing_implementation": None if available
            else drc.MONTHLY_EMITTER_ACTION}


def _obligation(**kw):
    base = dict(latest_completed_close_date=SEP1, operational_close_valid=True,
                eligible_market_date=SEP1, governed_research_session=SEP1,
                governed_research_current=False, governed_decision_session=SEP1,
                decision_state="HOLD_CURRENT_BOOK", research_current=False,
                research_cycle_due_after_close=True,
                stale_input_ids=["momentum_monthly", "price_score_refresh"],
                input_classification={}, evidence_gap=True)
    base.update(kw)
    return ws.build_research_obligation(**base)


def _classified(**kw):
    return drc.classify_stale_inputs(plan=_plan(), monthly_owner=_monthly(**kw))


# =========================================================================== #
# POST-CLOSE RESEARCH OBLIGATION (1-12)
# =========================================================================== #
class TestPostCloseObligation:

    def test_01_close_complete_drc_absent_keeps_the_obligation(self):
        ob = _obligation()
        assert ob["research_obligation_state"] in (
            ws.RESEARCH_OBLIGATION_OUTSTANDING, ws.RESEARCH_OBLIGATION_BLOCKED)
        assert ob["obligation_outstanding"] is True
        assert ob["outstanding_research_session"] == SEP1
        assert ob["latest_closed_session"] == SEP1
        assert ob["latest_governed_research_session"] is None

    def test_02_next_open_session_does_not_hide_prior_session_research(self):
        # The obligation is anchored on the CLOSE JOURNAL's completed session; no
        # wall clock, calendar date or open session appears in its inputs.
        ob = _obligation()
        assert ob["outstanding_research_session"] == SEP1
        assert SEP2 not in json.dumps(ob)

    def test_03_wait_gate_never_outranks_recoverable_outstanding_research(self):
        d = dict(inconsistent=False, session_status="BEFORE_SESSION_CLOSE",
                 has_confirmed_eligible=True, eligible_session_closed=True,
                 owned_data_lag=False, research_current=False,
                 assessment_status=ws.ASSESS_CURRENT, manual_review_required=False,
                 evidence_gap=False)
        assert ws._decide_overall(**d, research_obligation_outstanding=False) \
            == ws.WAITING_FOR_SESSION_CLOSE
        assert ws._decide_overall(**d, research_obligation_outstanding=True) \
            == ws.RESEARCH_CYCLE_REQUIRED

    def test_04_completed_close_is_not_rerun(self):
        # The cycle's plan maps the workflow owner's OWN primary action to a step. A
        # research-cycle action can never produce a Daily Close step.
        wf = {"overall_state": ws.RESEARCH_CYCLE_REQUIRED,
              "primary_action": {"action_code": ws.ACTION_RUN_RESEARCH_CYCLE,
                                 "execution_kind": "DAILY_RESEARCH_CYCLE",
                                 "execution_available": True}}
        plan = pcycle.plan_next_step(wf)
        assert plan["step"] == pcycle.STEP_DAILY_RESEARCH_CYCLE
        assert plan["step"] != pcycle.STEP_DAILY_CLOSE

    def test_05_same_portfolio_cycle_resumes_from_the_research_step(self):
        calls = []
        states = [
            {"overall_state": ws.RESEARCH_CYCLE_REQUIRED,
             "primary_action": {"action_code": ws.ACTION_RUN_RESEARCH_CYCLE,
                                "execution_kind": "DAILY_RESEARCH_CYCLE",
                                "execution_available": True}},
            {"overall_state": "DAILY_CYCLE_COMPLETE", "primary_action": {}},
        ]

        def loader():
            return states[min(len(calls), len(states) - 1)]

        def drc_runner(*, requested_by):
            calls.append(("DRC", requested_by))
            return {"state": "COMPLETE", "performed_write": True}

        def close_runner(*, requested_by, target_market_date=None):
            calls.append(("CLOSE", requested_by))
            return {"status": "DAILY_CLOSE_OK"}

        out = pcycle.run_portfolio_cycle(
            confirm=pcycle.EXECUTE_CONFIRMATION, workflow_loader=loader,
            drc_runner=drc_runner, close_runner=close_runner)
        assert [c[0] for c in calls] == ["DRC"]          # the close is NOT re-run
        assert out["steps_taken"] == [pcycle.STEP_DAILY_RESEARCH_CYCLE]
        assert out["stop_reason"] == pcycle.STOP_DECISION_PRESENTED

    def test_06_operator_supplies_no_date(self):
        ob = _obligation()
        assert ob["operator_supplies_no_date"] is True
        app = _code_only(_src("api/app.py"))
        for tok in ("research_market_date", "outstanding_research_session:",
                    "research_session=payload"):
            assert tok not in app

    def test_07_no_recovery_specific_post_route(self):
        ob = _obligation()
        assert ob["research_specific_route"] is None
        assert ob["orchestration_path"] == pcycle.RUN_ROUTE
        app = _src("api/app.py")
        for route in ("/v1/operations/daily-research-cycle/backfill",
                      "/v1/operations/research-recovery",
                      "/v1/operations/research/backfill"):
            assert route not in app

    def test_08_no_second_workflow_owner(self):
        assert ws.RESEARCH_OBLIGATION_OWNER == "api.workflow_state"
        for rel in ("api/active_manager_state.py", "api/operator_presentation.py",
                    "api/portfolio_cycle.py", "api/daily_research_cycle.py"):
            assert "def build_research_obligation(" not in _src(rel)

    def test_09_drc_idempotency_contract_is_untouched(self):
        src = _src("api/daily_research_cycle.py")
        assert "def compute_idempotency_key(" in _code_only(src)
        # Contract KEYS live in string literals, so they are matched on the raw source.
        assert "reused_existing_run" in src and "resumed_existing_run" in src
        # A completed run for the eligible session is REUSED, never recomputed.
        assert "_COMPLETED" in _code_only(src)

    def test_10_a_completed_cycle_still_waits_while_the_market_is_open(self):
        facts = {"consistency_status": "CONSISTENT",
                 "session_status": "BEFORE_SESSION_CLOSE",
                 "owned_data_confirmed": True, "eligible": SEP1}
        assert drc._pre_run_state(facts, eligible_cycle_complete=True) \
            == drc.WAITING_FOR_SESSION_CLOSE
        # ... and an eligible session whose cycle never ran is workable.
        assert drc._pre_run_state(facts, eligible_cycle_complete=False) is None

    def test_11_no_eligible_session_still_refuses(self):
        facts = {"consistency_status": "CONSISTENT",
                 "session_status": "BEFORE_SESSION_CLOSE",
                 "owned_data_confirmed": False, "eligible": None}
        assert drc._pre_run_state(facts) == drc.WAITING_FOR_SESSION_CLOSE

    def test_12_completed_decision_is_not_duplicated(self):
        # A session whose governed research IS current raises no obligation, so no
        # surface asks for the cycle (or the decision) a second time.
        ob = _obligation(governed_research_current=True, research_current=True,
                         research_cycle_due_after_close=False)
        assert ob["research_obligation_state"] == ws.NO_RESEARCH_OBLIGATION
        assert ob["obligation_outstanding"] is False
        assert ob["outstanding_research_session"] is None


# =========================================================================== #
# STALE-INPUT CLASSIFICATION (13-20)
# =========================================================================== #
class TestStaleInputClassification:

    def test_13_momentum_monthly_is_explicitly_classified(self):
        c = _classified(covered=False, panel="2026-08-05")
        row = next(r for r in c["inputs"] if r["source_id"] == "momentum_monthly")
        assert row["classification"] == drc.RECOVERY_TRUE_BLOCKER
        assert row["code"] == drc.MONTHLY_PANEL_BEHIND
        assert row["source_panel_date"] == "2026-08-05"
        assert row["recoverable_now"] is False
        assert row["point_in_time_safe"] is True
        assert "momentum_monthly" in c["true_blocker_input_ids"]

    def test_14_price_score_refresh_is_explicitly_classified(self):
        row = next(r for r in _classified()["inputs"]
                   if r["source_id"] == "price_score_refresh")
        # The completed session is in a new month vs the frozen input month, so the
        # target owner's own R_MONTH_BOUNDARY refusal applies: reproducible in
        # principle, NOT independently recoverable now.
        assert row["classification"] == drc.RECOVERY_CURRENT_REFRESH
        assert row["code"] == drc.PRICE_SCORE_MONTH_BOUNDARY
        assert row["depends_on"] == ["momentum_monthly"]
        assert row["point_in_time_safe"] is True

    def test_15_a_safely_reproducible_input_may_be_regenerated(self):
        # Same month on both sides: the daily input is bound to the eligible session
        # by its owner and is safely reproducible point-in-time.
        c = drc.classify_stale_inputs(
            plan=_plan(stale=("price_score_refresh",)),
            monthly_owner=_monthly(cur="2026-09", req="2026-09"))
        row = next(r for r in c["inputs"] if r["source_id"] == "price_score_refresh")
        assert row["classification"] == drc.RECOVERY_SAFE_PIT
        assert row["recoverable_now"] is True
        assert c["safe_work_remains"] is True
        assert c["blocked_by_true_blocker"] is False

    def test_16_an_unrecoverable_input_is_never_fabricated(self):
        c = _classified(covered=False, panel="2026-08-05")
        assert c["fabricates_nothing"] is True
        # A true blocker names an ACTION, never a substitute value.
        row = next(r for r in c["inputs"] if r["source_id"] == "momentum_monthly")
        assert row["operator_action"]
        assert "approximat" not in str(row.get("value", ""))
        assert "value" not in row

    def test_17_a_slow_moving_valid_input_is_not_called_blocking(self):
        c = drc.classify_stale_inputs(
            plan={"eligible_market_date": SEP1,
                  "required_stale_inputs": [], "slower_inputs_due": [],
                  "refresh_steps": []},
            monthly_owner=_monthly())
        assert c["stale_input_ids"] == []
        assert c["blocked_by_true_blocker"] is False
        assert c["true_blockers"] == []

    def test_18_later_session_data_cannot_enter_the_earlier_session(self):
        # The monthly emitter's own source-panel policy REFUSES a panel dated ahead
        # of the eligible session, so a run started on a later calendar day cannot
        # see data the session did not have.
        from paper_trader.api import monthly_momentum_emitter as mme
        cfg = mme.resolve_config({"panel_manifest": "does-not-exist",
                                  "panel_npz": "does-not-exist"})
        code = _code_only(_src("api/monthly_momentum_emitter.py"))
        assert "MONTHLY_PANEL_FUTURE_DATED" in _src("api/monthly_momentum_emitter.py")
        assert "last > elig_d" in code           # future data is refused outright
        assert "last < elig_d" in code           # a behind panel blocks, never guesses
        assert cfg.panel_manifest == "does-not-exist"

    def test_19_the_cycle_binds_every_refresh_to_the_eligible_session(self):
        code = _code_only(_src("api/daily_research_cycle.py"))
        # Both refresh owners are called with the ELIGIBLE session, never a clock.
        assert 'completed_through=facts[' in code
        assert "emit(eligible=facts[" in code
        for tok in ("date.today()", "datetime.now().date()"):
            assert tok not in code

    def test_20_producer_retry_is_idempotent(self):
        src = _src("api/monthly_momentum_input.py")
        assert "idempot" in src.lower()
        # The adapter keys reuse on the emitted artifact's content hash, so a retry
        # promotes the same artifact rather than writing a second one.
        assert "content_hash" in src


# =========================================================================== #
# TRUE_FORWARD (21-23)
# =========================================================================== #
class TestTrueForward:

    def test_21_the_documented_gap_remains(self):
        ob = _obligation()
        assert ob["documented_forward_evidence_gap"] is True
        assert ob["forward_evidence_gap_invalidates_close"] is False

    def test_22_no_true_forward_backfill_exists(self):
        for rel in ("api/workflow_state.py", "api/daily_research_cycle.py",
                    "api/operator_presentation.py", "api/forward_evidence.py",
                    "api/portfolio_cycle.py"):
            code = _code_only(_src(rel))
            for tok in ("def backfill_true_forward(", "def fabricate_snapshot(",
                        "def synthesize_forward_evidence("):
                assert tok not in code
        # The capture owner states the rule itself.
        assert "never retroactively backfilled" in _src("api/daily_close.py")

    def test_23_the_gap_does_not_invalidate_the_close(self):
        ob = _obligation()
        assert ob["invalidates_operational_close"] is False
        assert ws.RESEARCH_OBLIGATION_INVALIDATES_CLOSE is False
        assert ob["operational_close_valid"] is True


# =========================================================================== #
# WORKFLOW / UX (24-29)
# =========================================================================== #
class TestWorkflowAndUx:

    def test_24_close_valid_while_governed_research_incomplete(self):
        ob = _obligation()
        assert ob["operational_close_valid"] is True
        assert ob["governed_research_current"] is False
        assert ob["obligation_outstanding"] is True
        assert ob["invalidates_operational_close"] is False

    def test_25_research_only_condition_is_not_a_red_blocked_banner(self):
        wf = {"status": "OK", "overall_state": ws.RESEARCH_CYCLE_REQUIRED,
              "operational_state": {"operational_close_valid": True, "nav": 100.0,
                                    "valuation_date": SEP1,
                                    "eligible_market_date": SEP1},
              "blockers": [{"code": "RESEARCH_INPUT_STALE",
                            "source_id": "momentum_monthly",
                            "severity": "ATTENTION",
                            "scope": ws.BLOCKER_SCOPE_GOVERNED_RESEARCH,
                            "blocks_portfolio_decision": False,
                            "detail": "The frozen monthly input is due."}],
              "evidence_state": {}, "model_review": {}, "data_gap_taxonomy": {},
              "evidence_classification": {}}
        sr = opres._system_readiness(wf, None, None, None)
        assert sr["state"] == opres.SYSTEM_DEGRADED
        assert sr["blocking_reasons"] == []
        assert sr["portfolio_decision_remains_valid"] is True
        assert sr["operational_book_valid"] is True
        # ... and the reason is a sentence, not a Python dict repr.
        joined = " ".join(sr["degraded_reasons"]) + sr["summary"]
        assert "{'code'" not in joined and '{"code"' not in joined
        assert "RESEARCH_INPUT_STALE (momentum_monthly)" in joined

    def test_25b_a_real_blocker_still_blocks(self):
        wf = {"status": "OK", "overall_state": ws.WAITING_FOR_OWNED_DATA,
              "operational_state": {"operational_close_valid": True},
              "blockers": [{"code": "OWNED_DATA_NOT_CONFIRMED",
                            "severity": "BLOCKED",
                            "scope": ws.BLOCKER_SCOPE_OPERATIONAL,
                            "blocks_portfolio_decision": True,
                            "detail": "Owned market data is not confirmed."}],
              "evidence_state": {}, "model_review": {}, "data_gap_taxonomy": {},
              "evidence_classification": {}}
        sr = opres._system_readiness(wf, None, None, None)
        assert sr["state"] == opres.SYSTEM_BLOCKED
        assert sr["portfolio_decision_remains_valid"] is False

    def test_26_active_manager_state_exposes_three_separate_clocks(self):
        blk = ams._research_obligation_block({"research_obligation": _obligation()})
        assert blk["available"] is True
        assert blk["delegated"] is True and blk["computed_here"] is False
        for k in ("latest_closed_session", "latest_governed_research_session",
                  "latest_governed_decision_session"):
            assert k in blk
        assert blk["latest_closed_session"] == SEP1
        assert blk["latest_governed_research_session"] is None
        # An absent contract is UNAVAILABLE, never inferred.
        assert ams._research_obligation_block({})["available"] is False

    def test_27_today_shows_the_outstanding_research_not_wait_for_next_close(self):
        wf = {"research_obligation": _obligation(),
              "operational_state": {"operational_close_valid": True, "nav": 97906.63},
              "evidence_state": {"documented_gap": True}}
        g = opres._governed_research(wf, {"forward_evidence": {
            "status": "FORWARD_EVIDENCE_BLOCKED", "market_date": SEP1,
            "recovery_classification": "EVIDENCE_GAP_MUST_REMAIN"}})
        assert g["available"] is True and g["active"] is True
        assert g["outstanding_research_session"] == SEP1
        assert "VALID" in g["operational_book_line"]
        assert "incomplete" in g["governed_research_line"]
        assert "does not invalidate the close" in g["forward_evidence_line"]
        assert g["next_action_kind"] in ("RESUME_PORTFOLIO_CYCLE",
                                         "RESOLVE_NAMED_RESEARCH_BLOCKER")
        assert g["backfill_control_offered"] is False
        assert g["research_specific_route"] is None
        assert "Wait for" not in json.dumps(g)

    def test_28_javascript_performs_no_workflow_ordering(self):
        ui = _src("api/ui/index.html")
        i = ui.find("function _opRenderGovernedResearch(")
        assert i != -1
        body = ui[i:ui.find("\n}", i)]
        # The renderer reads the backend's verdict; it derives no state and no date.
        for tok in ("Date(", "new Date", "getTime(", "setDate(", "Math.",
                    "> today", "sort(", "RESEARCH_OBLIGATION_OUTSTANDING ="):
            assert tok not in body, tok
        assert "p.governed_research" in ui
        assert "opresPrimaryAction" not in body       # ONE CTA render site is kept

    def test_29_one_canonical_portfolio_cycle_action(self):
        ob = _obligation()
        assert ob["orchestration_path"] == pcycle.RUN_ROUTE
        assert pcycle.EXECUTE_CONFIRMATION == "RUN_PORTFOLIO_CYCLE"
        assert list(pcycle.STEP_VOCABULARY) == [pcycle.STEP_DAILY_CLOSE,
                                                pcycle.STEP_DAILY_RESEARCH_CYCLE]


# =========================================================================== #
# ATTRIBUTION (30-38)
# =========================================================================== #
_PERF = {"rows": [
    {"date": AUG31, "nav": 99113.22, "cash": 4482.71},
    {"date": SEP1, "nav": 97906.63, "cash": 4482.71},
]}
_OPS = {"canonical_state": {"holdings_detail": [
    {"ticker": "AAA", "quantity": 10.0, "sector": "Tech", "average_cost": 90.0},
]}}


def _sources(*, ledger_sep1: bool, cache_sep1: bool = True):
    """(ledger, cache) mark series. The immutable ledger stops at Aug-31 unless the
    TRUE_FORWARD capture ran; the Daily Close always refreshes the cache."""
    ledger = {"AAA": [[AUG31, 100.0]] + ([[SEP1, 90.0]] if ledger_sep1 else [])}
    cache = {"AAA": [[AUG31, 100.0]] + ([[SEP1, 90.0]] if cache_sep1 else [])}
    return ledger, cache


class TestAttribution:

    def test_30_the_nav_source_is_identified(self):
        code = _code_only(_src("api/forward_evidence.py"))
        # NAV comes from the append-only forward-performance rows.
        assert "def _perf_rows(" in code and "_PERF_LOADER" in code
        assert "load_performance" in _src("api/forward_evidence.py")

    def test_31_the_position_mark_source_is_identified(self):
        assert fe.MARK_SOURCE_LEDGER == "IMMUTABLE_COMPLETED_CLOSE_LEDGER"
        assert fe.MARK_SOURCE_CACHE_FALLBACK == "DESK_MARK_CACHE_FALLBACK"
        assert "forward_prediction_prices.json" in _src("api/forward_evidence.py")

    def test_32_the_current_mark_date_must_equal_the_close_date(self):
        # THE DEFECT. The ledger has no Sep-1 row (its capture was blocked), so
        # "greatest date <= as_of" returned the Aug-31 close for BOTH legs and the
        # contribution compared a price with itself.
        ledger, cache = _sources(ledger_sep1=False)
        hit = fe.mark_at(ledger, cache, "AAA", SEP1)
        assert hit[0] == SEP1                       # the exact session, not Aug-31
        assert hit[2] == fe.MARK_SOURCE_CACHE_FALLBACK
        # An EXACT ledger row still wins over the cache (Phase 8.1B is untouched:
        # a re-adjusted cache price may never displace a recorded prior close).
        led2 = {"AAA": [[AUG31, 311.71]]}
        cac2 = {"AAA": [[AUG31, 310.51]]}
        hit2 = fe.mark_at(led2, cac2, "AAA", AUG31)
        assert hit2 == (AUG31, 311.71, fe.MARK_SOURCE_LEDGER)

    def test_32b_a_stale_leg_is_reported_when_neither_store_has_the_date(self):
        ledger, cache = _sources(ledger_sep1=False, cache_sep1=False)
        hit = fe.mark_at(ledger, cache, "AAA", SEP1)
        assert hit[0] == AUG31                      # resolved, but NOT the session
        assert fe._exact(hit, SEP1) is False

    def test_33_reconciliation_decides_availability(self):
        ok = fe.attribution_availability(reconciles=True, priced=25, total=25)
        assert ok["available"] is True and ok["status"] == fe.ATTRIB_READY
        bad = fe.attribution_availability(reconciles=False, priced=25, total=25,
                                          stale_legs=["AAA"], diagnostic="X")
        assert bad["available"] is False
        assert bad["status"] == fe.ATTRIB_UNRECONCILED
        assert bad["decomposition_trustworthy"] is False
        assert bad["unavailable_reason"] == "X"

    def test_34_unreconciled_attribution_fails_closed(self):
        ledger, cache = _sources(ledger_sep1=False, cache_sep1=False)
        a = fe.build_daily_attribution(
            market_date=SEP1, ops=_OPS,
            perf_loader=lambda _d: _PERF,
            mark_ledger_loader=lambda _d: {"series": ledger})
        assert a["available"] is False
        assert a["status"] == fe.ATTRIB_UNRECONCILED
        assert a["decomposition_trustworthy"] is False
        assert a["unavailable_reason"]
        assert a["winners"] == [] and a["losers"] == [] and a["sectors"] == []
        assert a["reconciliation"]["reconciles"] is False

    def test_34b_a_reconciling_decomposition_is_published(self):
        ledger, cache = _sources(ledger_sep1=True)
        a = fe.build_daily_attribution(
            market_date=SEP1, ops=_OPS,
            perf_loader=lambda _d: {"rows": [
                {"date": AUG31, "nav": 1000.0, "cash": 0.0},
                {"date": SEP1, "nav": 900.0, "cash": 0.0}]},
            marks_loader=lambda _d: {"series": ledger})
        assert a["available"] is True
        assert a["reconciliation"]["reconciles"] is True
        assert a["reconciliation"]["residual"] == 0.0
        assert a["holdings"][0]["current_mark_date"] == SEP1

    def test_35_the_ui_cannot_present_zero_contributors_as_valid(self):
        ui = _src("api/ui/index.html")
        assert "ATTRIBUTION UNAVAILABLE" in ui
        i = ui.find("var at = d.attribution, attrWrap")
        assert i != -1
        block = ui[i:i + 3000]
        # The renderer keys on the OWNER's availability verdict, not on row count.
        assert "at.available" in block
        assert "at.attribution_status" in block
        assert "NAV RECONCILIATION FAILED" in block

    def test_36_no_historical_row_is_rewritten(self):
        for rel in ("api/forward_evidence.py", "api/daily_close.py"):
            code = _code_only(_src(rel))
            for tok in ("def rewrite_attribution_history(",
                        "def restate_nav(", "def amend_close_row("):
                assert tok not in code
        # The price store's first-write-wins contract is named and unchanged.
        assert "first_write_wins" in _src("api/forward_prediction_skill.py")

    def test_37_corporate_action_projection_semantics_preserved(self):
        ca = _src("api/corporate_actions.py")
        assert "def " in ca
        # This release adds no corporate-action reinterpretation anywhere.
        for rel in ("api/forward_evidence.py", "api/daily_close.py"):
            code = _code_only(_src(rel))
            assert "def reinterpret_corporate_action(" not in code
            assert "def adjust_historical_marks(" not in code

    def test_38_daily_close_nav_history_is_unchanged(self):
        # This release touches the DECOMPOSITION only. NAV, the P&L block and the
        # recorded decision keep their own owner and their own values.
        src = _src("api/daily_close.py")
        assert '"beginning_nav": _r2(nav0)' in src
        assert '"market_movement_pnl": _r2(market_movement)' in src
        assert "def _attribution_block(" in _code_only(src)
        assert "fe.attribution_availability(" in _code_only(src)

    def test_38b_the_close_and_the_evidence_owner_share_one_availability_rule(self):
        assert "fe.attribution_availability(" in _src("api/daily_close.py")
        assert "def attribution_availability(" in _src("api/forward_evidence.py")
        assert "def attribution_availability(" not in _src("api/daily_close.py")


# =========================================================================== #
# SAFETY (39-45)
# =========================================================================== #
class TestSafety:

    _MODULES = ("api/workflow_state.py", "api/daily_research_cycle.py",
                "api/portfolio_cycle.py", "api/operator_presentation.py",
                "api/active_manager_state.py", "api/forward_evidence.py")

    def test_39_no_order_is_created(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("create_order(", "submit_order(", "place_order("):
                assert tok not in code, "%s in %s" % (tok, rel)

    def test_40_no_fill_is_created(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("create_fill(", "record_fill(", "run_fill_cycle("):
                assert tok not in code, "%s in %s" % (tok, rel)

    def test_41_no_broker_call(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("broker_client", "BrokerClient", "send_to_broker("):
                assert tok not in code, "%s in %s" % (tok, rel)

    def test_42_automation_remains_off(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("schedule.every", "CronCreate", "register_task",
                        "auto_run_cycle", "def _autorun"):
                assert tok not in code, "%s in %s" % (tok, rel)
        assert '"automation": "OFF"' in _src("api/portfolio_cycle.py")

    def test_43_no_automatic_model_promotion(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("promote_champion(", "promote_model(", "def _promote("):
                assert tok not in code, "%s in %s" % (tok, rel)
        assert '"promotes_models": False' in _src("api/portfolio_cycle.py")

    def test_44_no_sleeve_activation(self):
        for rel in self._MODULES:
            code = _code_only(_src(rel))
            for tok in ("activate_sleeve(", "enable_sleeve("):
                assert tok not in code, "%s in %s" % (tok, rel)

    def test_45_no_live_execution_behaviour_is_added(self):
        pc = _src("api/portfolio_cycle.py")
        for tok in ('"creates_orders": False', '"creates_fills": False',
                    '"approves_proposals": False', '"executes_rebalance": False',
                    '"manual_review_required_for_portfolio_mutation": True'):
            assert tok in pc
        ob = _obligation()
        assert ob["repeats_the_completed_close"] is False


# =========================================================================== #
# CONTRACT / OWNERSHIP
# =========================================================================== #
class TestContract:

    def test_46_frozen_vocabularies(self):
        assert ws.RESEARCH_OBLIGATION_STATES == (
            "NO_RESEARCH_OBLIGATION", "RESEARCH_OBLIGATION_OUTSTANDING",
            "RESEARCH_OBLIGATION_BLOCKED", "RESEARCH_OBLIGATION_EVIDENCE_GAP")
        assert drc.INPUT_RECOVERY_STATES == (
            "SAFE_RECOVERABLE_POINT_IN_TIME", "CURRENT_REFRESH_REQUIRED",
            "SLOW_MOVING_VALID_BUT_OLDER", "UNRECOVERABLE_HISTORICAL_GAP",
            "TRUE_BLOCKER")
        assert ws.BLOCKER_SCOPES == ("OPERATIONAL", "GOVERNED_RESEARCH",
                                     "PORTFOLIO_DECISION")

    def test_47_the_obligation_is_pure(self):
        a = _obligation()
        b = _obligation()
        assert a == b                                   # deterministic
        assert isinstance(a["summary"], str)

    def test_48_a_blocked_obligation_names_its_cause(self):
        ob = _obligation(input_classification=_classified(covered=False,
                                                          panel="2026-08-05"))
        assert ob["research_obligation_state"] == ws.RESEARCH_OBLIGATION_BLOCKED
        assert ob["true_blockers"]
        assert ob["true_blockers"][0]["source_id"] == "momentum_monthly"
        assert "momentum_monthly" in ob["summary"]
        assert "remains valid" in ob["summary"]

    def test_49_an_unrecoverable_session_becomes_a_documented_gap(self):
        cls = {"true_blockers": [], "safely_recoverable_input_ids": [],
               "unrecoverable_gap_ids": ["momentum_monthly"], "inputs": []}
        ob = _obligation(input_classification=cls)
        assert ob["research_obligation_state"] == ws.RESEARCH_OBLIGATION_EVIDENCE_GAP
        # A permanent gap is NOT work: it must not suppress the wait gate forever.
        assert ob["obligation_outstanding"] is False
        assert "never fabricated" in ob["summary"]

    def test_50_a_missed_close_outranks_a_missed_research_cycle(self):
        ob = _obligation(catch_up_required=True)
        assert ob["research_obligation_state"] == ws.NO_RESEARCH_OBLIGATION
        assert ob["obligation_outstanding"] is False

    def test_51_a_decision_on_pre_cycle_evidence_is_stated_as_such(self):
        ob = _obligation()
        assert ob["latest_governed_decision_session"] == SEP1
        assert ob["decision_rests_on_governed_research"] is False


if __name__ == "__main__":                                   # pragma: no cover
    raise SystemExit(pytest.main([__file__, "-q"]))
