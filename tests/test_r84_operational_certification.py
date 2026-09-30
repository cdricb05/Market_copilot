"""R84 - final operational certification: the defects repaired in this release.

Every test is hermetic (pure functions, tmp_path desk ledgers, the FastAPI
TestClient against the retirement guard only). Nothing here reads or writes a
production store, and no test creates an order, a fill, a selection, a ruling
or an approval outside a temporary directory.

    D1  a governed WITHHELD complete target names its retention exits; the
        decision reconciliation no longer turns consistency INCONSISTENT
    D2  the Today hero, the hero guidance and the hero NEXT label are the ONE
        operator action, not a per-state constant
    D4  every legacy-archive order / fill / decision write fails closed (410)
    D5  the pre-governance desk / alpha-book order paths refuse a LIVE book
    D6  the "current authoritative decision" card answers with the CURRENT
        session's withheld verdict and names the older ledger row historical
    D7  a live cycle that built a target the owner WITHHELD is not "proposal
        available"
    D8  the published collection-restart remediation binds -RepoRoot
    D10 the withheld target's priced economics travel with its counts
    D13/D14 the collection and material-information reads use the event
        owner's run SUMMARY, not its four-key pointer
    D15 no surface claims the Reallocation Proposal engine is unimplemented
    D16 the frontier names admitted / blocked non-equity counts explicitly
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import active_manager_state as ams
from paper_trader.api import alpha_book as ab
from paper_trader.api import material_information as mi
from paper_trader.api import operator_presentation as op
from paper_trader.api import opportunity_frontier as ofr
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import workflow_state as ws

ROOT = Path(__file__).resolve().parents[1]
UI = (ROOT / "api" / "ui" / "index.html").read_text(encoding="utf-8")


# --------------------------------------------------------------------------- #
# Fixtures: the 2026-09-28 governed withheld verdict, in the owners' own shape
# --------------------------------------------------------------------------- #
def _withheld_outcome(session="2026-09-28"):
    return {
        "state": "WITHHELD", "withheld": True, "approvable": False,
        "executable": False, "governed_outcome_complete": True,
        "governed_run_id": "drc_%s_ca2db31a2209" % session,
        "governed_run_state": "COMPLETE", "eligible_market_date": session,
        "completed_at": "%sT23:44:59.703680+00:00" % session,
        "proposal_id": None, "persisted": False,
        "withheld_codes": ["RISK_CONTRIBUTION_CAP_BREACH_BLOCKS_CHANGE"],
        "withheld_breaching_tickers": ["AMD", "SNDK"],
        "withheld_risk_contribution_breaches": [
            {"ticker": "AMD", "risk_contribution_pct": 0.150305, "limit": 0.15,
             "excess": 0.000305},
            {"ticker": "SNDK", "risk_contribution_pct": 0.154483, "limit": 0.15,
             "excess": 0.004483}],
        "proposed_holding_count": 20,
        "action_counts": {"ADD": 6, "EXIT": 11, "INCREASE": 6, "REDUCE": 0,
                          "REPLACE_IN": 0, "REPLACE_OUT": 0, "RETAIN": 8},
        "one_way_turnover": 0.35, "estimated_transaction_cost": 85.97,
        "score_improvement_net_of_cost": 0.067127,
        "next_required_action": "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT",
        "terminal": True, "selectable_targets": 0,
    }


def _canonical(session="2026-09-28"):
    return {
        "state": "CHANGE_CANDIDATE_WITHHELD",
        "headline": "PORTFOLIO CHANGE WITHHELD",
        "explanation": ("A complete target is requested because a held name breaches "
                        "a hard portfolio constraint (AMD, DDOG) - pre-proposal."),
        "no_proposal_reason": "a complete alternative portfolio over 20 holdings WAS built",
        "no_proposal_reason_is_authoritative": True,
        "eligible_market_date": session,
        "complete_target_withheld_on_portfolio_limits": True,
        "governed_withheld_outcome": _withheld_outcome(session),
        "mandatory_exit_obligation": "NONE",
        "expected_one_way_turnover": 0.0, "expected_transaction_cost_usd": 0.0,
        "expected_net_improvement": 0.0, "net_improvement_hurdle": 0.05,
    }


def _operator_action():
    return {"action": "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT",
            "action_label": "Review the portfolio limit that withheld the change",
            "action_detail": "The daily cycle is complete ...",
            "requires_operator_work": True, "destination": "portfolio-manager",
            "focus": "realloc-card", "severity": "ATTENTION",
            "priority_owner": "api.workflow_state._decide_overall"}


# --------------------------------------------------------------------------- #
# D1 - the retention exits are named by the withheld verdict
# --------------------------------------------------------------------------- #
def test_d1_withheld_target_names_its_retention_exits():
    assert ws._withheld_retention_exits(None, _canonical()) == 11
    v = ws.check_decision_reconciliation(
        hoc_exit_count=11, proposal_exit_count=None, mandatory_exit_obligation="NONE",
        withheld_retention_exits=ws._withheld_retention_exits(None, _canonical()),
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states=None, cross_asset_liquidity_states=None)
    assert v == []


def test_d1_the_churn_ledger_still_outranks_and_silence_is_still_a_violation():
    pres = {"withholding_reconciliation": {"retention_exits_withheld": 4}}
    assert ws._withheld_retention_exits(pres, _canonical()) == 4
    # No withheld verdict and no ledger: the R77 violation still fires.
    assert ws._withheld_retention_exits(None, {"state": "NO_CHANGE"}) is None
    # A PUBLISHED proposal that exits none of them is still the R77 violation.
    v = ws.check_decision_reconciliation(
        hoc_exit_count=11, proposal_exit_count=0, mandatory_exit_obligation="NONE",
        withheld_retention_exits=None, proposal_deferred_trade_count=None,
        adjustment_log_deferral_counts=None, hoc_liquidity_states=None,
        cross_asset_liquidity_states=None)
    assert [x["code"] for x in v] == [ws.V_RETENTION_EXIT_UNACCOUNTED]
    # No proposal published yet (pre-cycle): an unreadable input is skipped.
    v = ws.check_decision_reconciliation(
        hoc_exit_count=11, proposal_exit_count=None, mandatory_exit_obligation="NONE",
        withheld_retention_exits=None, proposal_deferred_trade_count=None,
        adjustment_log_deferral_counts=None, hoc_liquidity_states=None,
        cross_asset_liquidity_states=None)
    assert v == []


def test_d1_an_incomplete_withheld_verdict_names_nothing():
    cd = _canonical()
    cd["governed_withheld_outcome"]["governed_outcome_complete"] = False
    assert ws._withheld_retention_exits(None, cd) is None


# --------------------------------------------------------------------------- #
# D2 - one operator action on the Today hero
# --------------------------------------------------------------------------- #
def test_d2_hero_cta_becomes_the_one_operator_action():
    hero = {"focus_lane": "OPERATIONAL", "cta_action_code": "WAIT_FOR_SESSION_CLOSE",
            "cta_label": "Wait for the market session to close",
            "headline": "Waiting for the current session to close."}
    out = ws.align_today_hero_with_operator_action(hero, _operator_action())
    assert out["cta_action_code"] == "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT"
    assert out["cta_label"] == "Review the portfolio limit that withheld the change"
    assert out["focus_lane"] == "PORTFOLIO"
    assert out["cta_execution_available"] is False and out["cta_creates_orders"] is False
    assert out["cta_matches_operator_action"] is True


def test_d2_passive_operator_action_leaves_the_hero_alone():
    hero = {"focus_lane": "OPERATIONAL", "cta_action_code": "WAIT_FOR_SESSION_CLOSE"}
    act = {"action": "WAIT_FOR_SESSION_CLOSE", "requires_operator_work": False}
    out = ws.align_today_hero_with_operator_action(hero, act)
    assert out["cta_action_code"] == "WAIT_FOR_SESSION_CLOSE"
    assert out["focus_lane"] == "OPERATIONAL"


def test_d2_hold_guidance_is_the_operator_action_when_work_is_owed():
    wf = {"canonical_portfolio_decision": _canonical(),
          "portfolio_decision_state": {"materiality": {"action_counts":
                                        _withheld_outcome()["action_counts"]}},
          "operator_action": _operator_action()}
    s = op._decision_summary(wf, {}, {"state": op.PD_HOLD})
    assert s["current_decision"]["guidance"] == \
        "Review the portfolio limit that withheld the change"
    # the hold's own economics stay definitional zeros
    assert s["current_decision"]["turnover"] == 0.0


def test_d2_hold_guidance_unchanged_without_operator_work():
    wf = {"canonical_portfolio_decision": {"state": "NO_CHANGE"},
          "operator_action": {"action": "MONITOR_PORTFOLIO",
                              "requires_operator_work": False}}
    s = op._decision_summary(wf, {}, {"state": op.PD_HOLD})
    assert s["current_decision"]["guidance"] == "Monitor portfolio"


# --------------------------------------------------------------------------- #
# D10 - the withheld target's priced economics travel with its counts
# --------------------------------------------------------------------------- #
def test_d10_withheld_counts_and_economics_come_from_the_same_target():
    wf = {"canonical_portfolio_decision": _canonical(),
          "portfolio_decision_state": {"materiality": {"action_counts":
                                        _withheld_outcome()["action_counts"]}},
          "operator_action": _operator_action()}
    s = op._decision_summary(wf, {}, {"state": op.PD_HOLD})
    assert s["exits"] == 11
    assert s["turnover"] == pytest.approx(0.35)
    assert s["estimated_cost"] == pytest.approx(85.97)
    assert s["net_improvement"] == pytest.approx(0.067127)


# --------------------------------------------------------------------------- #
# D6 - the current decision card
# --------------------------------------------------------------------------- #
def _governed_sep25():
    return {"available": True, "decision": "CHANGE_RECOMMENDED",
            "timestamp": "2026-09-26T15:00:28.458624Z",
            "provenance": "GOVERNED_DAILY_CYCLE",
            "record_id": "gdec_2026-09-25_alpha_paper_book_1_6ec35e45da43",
            "eligible_market_session": "2026-09-25", "persisted": True,
            "persistence_status": "LEDGER_ROW", "is_ledger_row": True}


def _answer(governed, canonical):
    return ams._operator_answer_block(
        governed=governed, canonical=canonical, lane={}, live_information={},
        operational_book={"operational_mark_date": "2026-09-28", "nav": 98246.56},
        operator_guidance={"operator_action": _operator_action()})


def test_d6_newer_withheld_verdict_is_the_current_answer():
    d = _answer(_governed_sep25(), _canonical())["current_decision"]
    assert d["session"] == "2026-09-28"
    assert d["session_is_current"] is True
    assert d["decided_at"].startswith("2026-09-28T23:44")
    assert d["record_id"] == "drc_2026-09-28_ca2db31a2209"
    assert d["persisted"] is False and d["is_ledger_row"] is False
    assert d["persistence_status"] == ams.PERSISTENCE_GOVERNED_RUN_MANIFEST
    assert d["explanation"].startswith("a complete alternative portfolio")
    hist = d["standing_ledger_record"]
    assert hist["session"] == "2026-09-25" and hist["historical"] is True
    assert hist["record_id"].startswith("gdec_2026-09-25")


def test_d6_same_session_ledger_row_is_unchanged():
    g = dict(_governed_sep25(), eligible_market_session="2026-09-28")
    d = _answer(g, _canonical())["current_decision"]
    assert d["record_id"].startswith("gdec_")
    assert d["standing_ledger_record"] is None
    assert d["current_verdict_is_non_persisted_withheld"] is False


def test_d6_without_a_withheld_verdict_the_ledger_row_answers():
    cd = {"state": "HOLD_CURRENT_BOOK", "headline": "HOLD", "eligible_market_date":
          "2026-09-25"}
    d = _answer(_governed_sep25(), cd)["current_decision"]
    assert d["session"] == "2026-09-25"
    assert d["standing_ledger_record"] is None


# --------------------------------------------------------------------------- #
# D7 - the live lane
# --------------------------------------------------------------------------- #
def _lane(cycle):
    return ams._live_reassessment_lane_block(
        live_information={"last_event_cycle": cycle}, signal_state={},
        reassessment={}, workflow={}, governed_decision={})


def test_d7_a_withheld_built_target_is_not_proposal_available():
    lane = _lane({"run_id": "evt_x", "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                  "reassessment_state": "PROPOSAL_READY", "proposal_built": True,
                  "proposal_state": "WITHHELD", "reassessment_ran": True})
    assert lane["candidate_conclusion"] == ams.LANE_CONCLUSION_TARGET_WITHHELD
    assert "withheld" in lane["headline"]
    assert lane["reassessment_ran"] is True


def test_d7_a_reviewable_target_still_reads_proposal_available():
    lane = _lane({"run_id": "evt_y", "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                  "reassessment_state": "PROPOSAL_READY", "proposal_built": True,
                  "proposal_state": "READY", "reassessment_ran": True})
    assert lane["candidate_conclusion"] == ams.LANE_CONCLUSION_PROPOSAL_AVAILABLE


# --------------------------------------------------------------------------- #
# D14 - material information reads the run summary
# --------------------------------------------------------------------------- #
def test_d14_material_information_reads_the_recorded_reassessment():
    ev = {"last_run": {"run_id": "evt_z", "state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW",
                       "generated_at": "2026-09-29T14:19:03Z"},
          "last_run_summary": {"reassessment_ran": True, "proposal_state": "WITHHELD"}}
    out = mi.build(event_refresh=ev, decision={
        "portfolio_decision_state": "CHANGE_CANDIDATE_WITHHELD"})
    assert out["portfolio_reassessed"] is True
    assert out["last_event_cycle"]["proposal_state"] == "WITHHELD"


def test_d14_without_a_summary_the_state_fallback_still_counts_a_built_target():
    ev = {"last_run": {"state": "PROPOSAL_AVAILABLE_FOR_MANUAL_REVIEW"}}
    assert mi.build(event_refresh=ev)["portfolio_reassessed"] is True
    ev = {"last_run": {"state": "INFORMATION_NOT_MATERIAL"}}
    assert mi.build(event_refresh=ev)["portfolio_reassessed"] is False


# --------------------------------------------------------------------------- #
# D4 - the legacy archive's execution writes are retired, server-side
# --------------------------------------------------------------------------- #
def test_d4_every_retired_route_answers_410_before_any_handler(monkeypatch):
    from fastapi.testclient import TestClient
    from paper_trader.api import app as app_mod
    from paper_trader.config import get_settings
    monkeypatch.delenv("PAPER_TRADER_LEGACY_ARCHIVE_EXECUTION_ENABLED", raising=False)
    get_settings.cache_clear()
    key = get_settings().service_api_key
    client = TestClient(app_mod.app)
    try:
        for path in app_mod.LEGACY_EXECUTION_RETIRED_ROUTES:
            url = path.replace("{candidate_id}", "00000000-0000-0000-0000-000000000000")
            r = client.post(url, json={}, headers={"X-API-Key": key})
            assert r.status_code == 410, (path, r.status_code, r.text[:200])
            body = r.json()["detail"]
            assert body["code"] == app_mod.LEGACY_EXECUTION_RETIRED_CODE
            assert body["performed_write"] is False and body["creates_orders"] is False
    finally:
        client.close()
        get_settings.cache_clear()


def test_d4_the_guard_is_attached_to_exactly_the_declared_routes():
    from paper_trader.api import app as app_mod
    guarded = set()
    for r in app_mod.app.routes:
        deps = [d.call for d in getattr(getattr(r, "dependant", None), "dependencies", [])]
        if app_mod._legacy_archive_execution_retired in deps:
            guarded.add(r.path)
    assert guarded == set(app_mod.LEGACY_EXECUTION_RETIRED_ROUTES)


def test_d4_the_opt_in_is_explicit_and_default_off(monkeypatch):
    from paper_trader.api import app as app_mod
    from paper_trader.config import get_settings
    monkeypatch.delenv("PAPER_TRADER_LEGACY_ARCHIVE_EXECUTION_ENABLED", raising=False)
    get_settings.cache_clear()
    try:
        assert get_settings().legacy_archive_execution_enabled is False
        monkeypatch.setenv("PAPER_TRADER_LEGACY_ARCHIVE_EXECUTION_ENABLED", "true")
        get_settings.cache_clear()
        assert app_mod._legacy_archive_execution_retired() is None
    finally:
        monkeypatch.delenv("PAPER_TRADER_LEGACY_ARCHIVE_EXECUTION_ENABLED", raising=False)
        get_settings.cache_clear()


def test_d4_the_ui_never_sends_a_retired_post():
    from paper_trader.api import app as app_mod
    m = re.search(r"const _R84_RETIRED_POSTS = \[(.*?)\];", UI, re.S)
    assert m, "the UI retired-route list is missing"
    listed = set(re.findall(r"'([^']+)'", m.group(1)))
    declared = {p for p in app_mod.LEGACY_EXECUTION_RETIRED_ROUTES if "{" not in p}
    assert listed == declared
    assert "paper-trade" in UI[UI.index("function _r84IsRetiredPost"):][:600]
    assert 'onclick="createOrdersFromDecisions()"' not in UI
    # the research-diagnostics legacy signal / fill controls are disabled markup
    for gone in ('onclick="dtrSubmitSignal(this)"', "() => dtrRunFill(this)",
                 'onclick="submitSignal(this)"', "() => triggerFill(this)"):
        assert gone not in UI, gone
    for step in ("create-orders", "create-decisions"):
        row = re.search(r"\{ id: '%s'[^\n]*\}" % step, UI).group(0)
        assert "retired: true" in row, step


# --------------------------------------------------------------------------- #
# D5 - the bootstrap order paths refuse a live book
# --------------------------------------------------------------------------- #
def _live_book(tmp_path):
    sdir = tmp_path / "desk"
    sdir.mkdir()
    book = {"book_id": ab.ALPHA_BOOK_ID, "status": "OPEN", "book_number": 1}
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": book}])
    desk._append_ledger(sdir, desk.FILLS_FILE, [{"fill": {
        "book_id": ab.ALPHA_BOOK_ID, "order_id": "ord_1", "ticker": "AAA",
        "side": desk.SIDE_BUY, "quantity": 1}}])
    return sdir


def test_d5_a_funded_book_refuses_every_bootstrap_order_path(tmp_path):
    sdir = _live_book(tmp_path)
    before = (sdir / desk.ORDERS_FILE).exists()
    g = desk.generate_orders(confirm=desk.GEN_CONFIRM_TOKEN, desk_dir=sdir)
    c = desk.confirm_orders(confirm=desk.EXEC_CONFIRM_TOKEN, desk_dir=sdir)
    p = ab.confirm_order_plan(confirm=ab.PLAN_CONFIRM_TOKEN, desk_dir=sdir)
    for out in (g, c, p):
        assert out["status"] == desk.S_LIVE_BOOK_BOOTSTRAP_CLOSED
        assert out["performed_write"] is False
        assert "rebalance/confirm-order-plan" in out["governed_execution_path"]
    assert (sdir / desk.ORDERS_FILE).exists() == before


def test_d5_an_empty_book_may_still_be_funded(tmp_path):
    sdir = tmp_path / "desk"
    sdir.mkdir()
    desk._append_ledger(sdir, desk.BOOKS_FILE, [{"event": "BOOK_CREATED", "book": {
        "book_id": ab.ALPHA_BOOK_ID, "status": "OPEN"}}])
    assert desk.live_book_bootstrap_refusal(sdir) is None
    assert desk.live_book_bootstrap_refusal(tmp_path / "none") is None


def test_d5_alpha_status_publishes_the_verdict_and_the_ui_gates_on_it():
    assert "bootstrap_order_path_open" in UI and "bootstrap_order_path_refusal" in UI
    src = (ROOT / "api" / "alpha_book.py").read_text(encoding="utf-8")
    assert '"bootstrap_order_path_open"' in src


# --------------------------------------------------------------------------- #
# D8 / D15 / D16 / UI contracts
# --------------------------------------------------------------------------- #
def test_d8_every_collection_restart_remediation_binds_repo_root():
    for f in ("runtime_identity.py", "information_collection.py",
              "holding_opportunity_cost.py"):
        src = (ROOT / "api" / f).read_text(encoding="utf-8")
        flat = re.sub(r'"\s*\n\s*"', "", src)
        for m in re.finditer(r"manage_information_collection\.ps1([^\"']*)", flat):
            tail = m.group(1)
            if "-Action" in tail:
                assert "-RepoRoot" in tail, (f, tail)


def test_d15_no_surface_claims_the_proposal_engine_is_unimplemented():
    src = (ROOT / "api" / "portfolio_state.py").read_text(encoding="utf-8")
    assert "is not implemented yet" not in re.sub(r'"\s*\n\s*"', "", src)


def test_d16_frontier_publishes_explicit_non_equity_counts():
    fr = {"eligible_non_equity_count": 0, "non_equity_admission_ledger": [
        {"sleeve_id": "sleeve_rates_futures", "asset_class": "RATES_FUTURES",
         "blocker": "NO_APPROVED_OPERATIONAL_SIGNAL", "instruments_admitted": 0},
        {"sleeve_id": "sleeve_fx_futures", "asset_class": "FX_FUTURES",
         "blocker": "NO_APPROVED_OPERATIONAL_SIGNAL", "instruments_admitted": 0}]}
    s = ofr.non_equity_count_summary(fr)
    assert s["frontier_eligible_non_equity_count"] == 0
    assert s["admitted_non_equity_count"] == 0
    assert s["blocked_non_equity_count"] == 2
    assert s["non_equity_gap_class"] == "ALPHA_DATA_EVIDENCE_GAP"
    assert {b["sleeve_id"] for b in s["non_equity_blockers"]} == {
        "sleeve_rates_futures", "sleeve_fx_futures"}


def test_d16_an_admitted_sleeve_is_not_a_gap():
    fr = {"eligible_non_equity_count": 3, "non_equity_admission_ledger": [
        {"sleeve_id": "s1", "blocker": None, "instruments_admitted": 3}]}
    s = ofr.non_equity_count_summary(fr)
    assert s["admitted_non_equity_count"] == 3 and s["blocked_non_equity_count"] == 0
    assert s["non_equity_gap_class"] is None


# --------------------------------------------------------------------------- #
# D21 - the approval gate never names an act that cannot be done
# --------------------------------------------------------------------------- #
def test_d21_withheld_session_gate_names_the_limit_review_not_selection():
    from paper_trader.api import portfolio_decision as pdec
    g = pdec.selected_target_approval_gate(
        selection=None, change_withheld=True,
        withheld_reasons=["RISK_CONTRIBUTION_CAP_BREACH_BLOCKS_CHANGE"])
    assert g["available"] is False
    assert g["status"] == pdec.PDS_CHANGE_WITHHELD
    assert g["next_required_action"] == "REVIEW_THE_WITHHELDING_PORTFOLIO_LIMIT"
    assert g["next_required_action_label"] == \
        "Review the portfolio limit that withheld the change"
    assert g["withheld_reasons"] == ["RISK_CONTRIBUTION_CAP_BREACH_BLOCKS_CHANGE"]
    assert g["approves_anything"] is False and g["creates_orders"] is False


def test_d21_without_a_withheld_target_selection_is_still_required():
    from paper_trader.api import portfolio_decision as pdec
    g = pdec.selected_target_approval_gate(selection=None)
    assert g["status"] == pdec.PDS_TARGET_SELECTION_REQUIRED
    assert g["next_required_action"] == pdec.NEXT_ACTION_SELECT_A_TARGET


def test_d21_a_stale_session_still_outranks_the_withheld_step():
    from paper_trader.api import portfolio_decision as pdec
    g = pdec.selected_target_approval_gate(
        selection=None, change_withheld=True,
        freshness={"approval_allowed": False,
                   "next_required_action": pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE})
    assert g["status"] == pdec.PDS_SESSION_STALE


def test_d21_vocabulary_stays_single_and_labelled():
    from paper_trader.api import portfolio_decision as pdec
    assert set(pdec.NEXT_ACTION_VOCAB) == set(pdec.NEXT_ACTION_LABELS)
    assert pdec.PDS_CHANGE_WITHHELD in pdec.APPROVAL_GATE_STATUS_VOCAB
    # the word is the workflow owner's ONE action for the same state
    assert pdec.NEXT_ACTION_REVIEW_THE_WITHHELDING_LIMIT == \
        ws.OP_ACTION_REVIEW_WITHHELDING_LIMIT


def test_d20_daily_workflow_book_card_is_the_lifecycle_not_the_action():
    assert "'Book lifecycle: ' + view.currentTask" in UI
    assert "'Current task: ' + view.currentTask + ' · Next action: '" not in UI
    assert "Operator action (one): " in UI


# --------------------------------------------------------------------------- #
# D22 - the market-session contract states whether a holiday calendar exists
# --------------------------------------------------------------------------- #
def test_d22_supplied_exchange_calendar_is_not_reported_missing():
    from datetime import datetime, timezone
    from paper_trader.engine import market_session as ms
    now = datetime(2026, 9, 29, 23, 0, tzinfo=timezone.utc)   # after the ET cutoff
    with_cal = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-09-29",
                                   authoritative_non_sessions=["2026-09-07"],
                                   exchange_calendar_available=True)
    assert ms._DEGRADED_WARNING not in with_cal.warnings
    assert with_cal.calendar_policy == ms.CALENDAR_POLICY_WITH_EXCHANGE_CALENDAR
    without = ms.evaluate_session(now=now, latest_confirmed_owned_data_date="2026-09-29")
    assert ms._DEGRADED_WARNING in without.warnings
    assert without.calendar_policy == ms.CALENDAR_POLICY


# --------------------------------------------------------------------------- #
# D23 - a quarterly input past its filing deadline is STALE, never NOT_DUE
# --------------------------------------------------------------------------- #
def test_d23_quarterly_input_past_the_filing_deadline_is_stale():
    from datetime import date
    from paper_trader.api import data_freshness as df
    assert df.quarterly_required_as_of(date(2026, 9, 28)) == date(2026, 8, 14)
    assert df.quarterly_required_as_of(date(2026, 8, 4)) == date(2026, 5, 15)
    live = df.classify_source(cadence=df.QUARTERLY, as_of="2026-05-22", anchor="2026-09-28")
    assert live["status"] == df.STALE
    assert live["status"] not in df._SATISFIED
    # before the deadline the prior-quarter panel is still legitimately not due
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2026-05-22",
                              anchor="2026-08-04")["status"] == df.NOT_DUE
    assert df.classify_source(cadence=df.QUARTERLY, as_of="2026-08-20",
                              anchor="2026-09-28")["status"] == df.FRESH


# --------------------------------------------------------------------------- #
# D25 - an intraday re-version never un-supersedes an older-session proposal
# --------------------------------------------------------------------------- #
def _supersession_world(monkeypatch, *, head_hash, manifest_hash):
    from paper_trader.api import daily_research_cycle as drc
    from paper_trader.api import portfolio_decision as pdec
    from paper_trader.api import portfolio_reassessment as prs
    monkeypatch.setattr(prs, "load_latest_assessment_pointer", lambda **kw: {
        "eligible_market_date": "2026-09-28", "reassessment_hash": head_hash,
        "artifact_id": "prs_head", "generated_at": "2026-09-29T18:44:04Z",
        "decision": "PROPOSAL_READY", "hoc_assessment_hash": "HOC_NEW"})
    monkeypatch.setattr(drc, "load_governed_manifest_reference", lambda **kw: {
        "run_id": "drc_2026-09-28_x", "governed": True,
        "portfolio_reassessment_hash": manifest_hash,
        "portfolio_reassessment_id": "prs_gov",
        "portfolio_reassessment_state": "PROPOSAL_READY"})
    monkeypatch.setattr(pdec, "load_governed_decision_record", lambda **kw: None)
    return pdec


def _summary(session):
    return {"reallocation_proposal_available": True,
            "reallocation_proposal_id": "reap_%s" % session,
            "reallocation_proposal_hash": "P", "reallocation_bound_eligible_market_date": session,
            "reallocation_bound_hoc_assessment_hash": "HOC_OLD"}


def test_d25_governed_newer_session_supersedes_after_an_intraday_reversion(monkeypatch):
    pdec = _supersession_world(monkeypatch, head_hash="NEW_VERSION", manifest_hash="GOVERNED")
    s = pdec.load_decision_supersession(active_book_id="b", proposal_summary=_summary("2026-09-18"))
    assert s["superseded"] is True
    assert s["reason"] == pdec.SUP_NEWER_SESSION_DECISION
    assert s["superseded_by"]["reassessment_hash"] == "GOVERNED"


def test_d25_same_session_keeps_the_strict_head_rule(monkeypatch):
    pdec = _supersession_world(monkeypatch, head_hash="NEW_VERSION", manifest_hash="GOVERNED")
    s = pdec.load_decision_supersession(active_book_id="b", proposal_summary=_summary("2026-09-28"))
    assert s["superseded"] is False
    assert s["reason"] == pdec.SUP_AUTHORITY_UNPROVEN


# --------------------------------------------------------------------------- #
# D26 - the normal evening close is "due", not a missed session / DEGRADED
# --------------------------------------------------------------------------- #
def _recovery(calendar_date):
    return ws.build_session_recovery(
        expected_completed_market_date="2026-09-29", eligible_market_date="2026-09-28",
        latest_completed_close_date="2026-09-28", operational_close_valid=True,
        latest_confirmed_owned_data_date="2026-09-28",
        provider_readiness={"ready": True, "status": "READY",
                            "provider_latest_date": "2026-09-29",
                            "expected_market_date": "2026-09-29"},
        calendar_date=calendar_date)


def test_d26_the_same_evening_close_is_due_not_missed():
    r = _recovery("2026-09-29")
    assert r["missed_completed_sessions"] == ["2026-09-29"]
    assert r["normal_close_due"] is True
    assert "never closed" not in r["summary"] and "is due" in r["summary"]
    # the obligation and the ONE action are unchanged
    assert r["recovery_state"] == "CATCH_UP_REQUIRED"


def test_d26_the_next_calendar_day_is_a_catch_up():
    r = _recovery("2026-09-30")
    assert r["normal_close_due"] is False
    assert "never closed" in r["summary"]


def test_d26_the_workflow_headline_words_the_normal_evening_as_due():
    # The workflow owner's primary headline (which the Today hero reads) said
    # "Catch up - 2026-09-29 was not closed." on the session's own evening.
    primary = {"action_code": "RUN_DAILY_CLOSE", "label": "Run the Daily Close"}
    due = ws.word_session_close_action(primary, _recovery("2026-09-29"))
    assert due["headline"] == "Daily Close due — 2026-09-29 has completed."
    assert "never closed" not in due["explanation"]
    assert "missed" not in due["current_task"]
    assert due["action_code"] == "RUN_DAILY_CLOSE"          # action unchanged
    late = ws.word_session_close_action(primary, _recovery("2026-09-30"))
    assert late["headline"] == "Catch up — 2026-09-29 was not closed."
    assert "never closed" in late["explanation"]


def test_d26_presentation_words_the_normal_evening_and_stays_ready():
    wf = {"session_recovery": _recovery("2026-09-29")}
    panel = op._session_recovery(wf, {})
    assert panel["headline"] == "DAILY CLOSE DUE"
    assert panel["session_label"] == "Session due"
    assert "ready to close" in panel["detail"]


def test_ui_withheld_reallocation_contracts():
    # D10: the withheld target's priced economics and per-name breaches render.
    assert "r84-withheld-breach-table" in UI
    assert "withheld_risk_contribution_breaches" in UI
    # D9: step 2 never points at a step 1 that is not rendered.
    assert 'data-no-selectable-target="1"' in UI
    # D6: the historical ledger row is labelled on the current decision card.
    assert "Last ledger record (historical)" in UI and "CURRENT SESSION" in UI
    # D24: the heavy composed reads use the heavy-read budget, not the 45 s default.
    for path in ("/v1/operations/workflow-state", "/v1/operations/information-collection",
                 "/v1/operations/material-information"):
        assert "_mhzGet('%s', _R601_HEAVY_READ_TIMEOUT_MS)" % path in UI, path
    # D11: the service badge re-checks after a load-contention timeout.
    assert "for (var slow = 0; slow < 20; slow++)" in UI
    # D12: the assessment card renders the lane-aware workflow composition.
    assert "_wsPres && _wsHash && d.reassessment_hash" in UI
    # the forbidden browser dialogs are still never called
    assert not re.search(r"(?<![\w.])alert\(\s*['\"]", UI)
    assert not re.search(r"(?<![\w.])confirm\(\s*['\"]", UI)


_WRITE_CONTROLS = ("ab-act-init", "ab-act-confirm-plan", "pd-act-generate",
                   "pd-act-confirm", "pd-act-cancel")


def _button_tag(control_id):
    m = re.search(r'<button[^>]*\bid="%s"[^>]*>' % re.escape(control_id), UI)
    assert m, control_id
    return m.group(0)


def test_d30_every_book_and_order_write_control_is_disabled_in_markup():
    """D30: on a live funded book the desk reads land ~34 s after a Portfolio
    visit; until then (or forever, on a timed-out read) the markup defaults
    were the UI, and they rendered Create / Confirm Paper Orders ENABLED."""
    for cid in _WRITE_CONTROLS:
        assert re.search(r"\sdisabled[\s>]", _button_tag(cid)), cid
    assert 'style="display:none"' in _button_tag("pd-act-generate")
    # the pre-read label never claims there is no book
    assert 'id="pd-book-name">BOOK LOADING<' in UI


def test_d30_an_unavailable_read_disarms_the_write_controls():
    desk_unavail = UI[UI.index("function renderPaperDesk()"):]
    desk_unavail = desk_unavail[:desk_unavail.index("return;")]
    for cid in ("pd-act-generate", "pd-act-confirm", "pd-act-cancel"):
        assert cid in desk_unavail, cid
    assert "b.disabled = true" in desk_unavail
    book_unavail = UI[UI.index("function renderAlphaBook()"):]
    book_unavail = book_unavail[:book_unavail.index("return;")]
    for cid in ("ab-act-init", "ab-act-confirm-plan"):
        assert "_abBtn('%s', false" % cid in book_unavail, cid
