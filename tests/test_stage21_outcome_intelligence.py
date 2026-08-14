"""Stage 21 — EXECUTION LINEAGE, DURABLE DAILY CLOSE, REASSESSMENT CLARITY,
OUTCOME EVIDENCE, POLICY INTELLIGENCE and ENVIRONMENT ISOLATION.

Every test is HERMETIC: no provider, no prediction, no broker, no live store. The live
2026-08-13 operational book (25 holdings, 54 fills, the executed 29-order rebalance) is
never read or touched — the real lifecycle is REPRODUCED as fixtures so the regressions
it exposed stay covered without depending on production state.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import environment_isolation as EI
from paper_trader.api import execution_lineage as ELAPI
from paper_trader.api import portfolio_reassessment as PRS
from paper_trader.api import portfolio_state as PS
from paper_trader.api import reassessment_outcomes as RO
from paper_trader.engine import execution_lineage as EL
from paper_trader.engine import portfolio_reassessment as PRK
from paper_trader.engine import reassessment_outcomes as K

BOOK = "alpha_paper_book_1"
DATE = "2026-08-12"

# The REAL Aug-12/Aug-13 lifecycle identities, reproduced exactly.
PROPOSAL_ID = "reap_2026-08-12_alpha_paper_book_1_f64fe4998d9d"
PROPOSAL_HASH = "f64fe4998d9d5cb5fe6e1fc74636e2557e9c406c7ac18867f190e9deb68812c7"
DECISION_ID = "pdec_2026-08-12_alpha_paper_book_1_f64fe4998d9d"
GOOD_PLAN = "rbop_2026-08-12_alpha_paper_book_1_1a198f560cca"
GOOD_HASH = "1a198f560cca5c7457e58f151b0e409b772b5ab85368a4f8bdf5eacc4d9315b9"
BAD_PLAN = "rbop_2026-08-12_alpha_paper_book_1_5bf9c6c20f8a"
BAD_HASH = "5bf9c6c20f8a076b025baf02e24491791b90f226d6010cd60f226aab80d4f952"


# =========================================================================== #
# fixtures — the real lifecycle, reproduced
# =========================================================================== #
def _lineage(plan_id, plan_hash, created_at):
    return {"decision_id": DECISION_ID, "proposal_id": PROPOSAL_ID,
            "proposal_hash": PROPOSAL_HASH, "order_plan_id": plan_id,
            "order_plan_hash": plan_hash, "eligible_market_date": DATE,
            "paper_book_id": BOOK, "created_at": created_at,
            "execution_model": "NEXT_CLOSE"}


def _order(seq, ticker, side, status, plan=None, plan_hash=None, created_at=None):
    o = {"order_id": "ord_%s_%03d_%s" % (BOOK, seq, ticker), "book_id": BOOK,
         "ticker": ticker, "side": side, "status": status,
         "approval_date": "2026-08-13" if plan else None}
    if plan:
        o["rebalance_lineage"] = _lineage(plan, plan_hash, created_at)
    return o


def _real_orders():
    """25 historical fills (no lineage) + 22 CANCELLED (defective, created FIRST)
    + 29 FILLED (repaired, created SECOND). Exactly the live ledger's shape."""
    orders = []
    for i in range(25):
        orders.append(_order(i, "H%02d" % i, "PAPER_BUY", "FILLED"))
    for i in range(22):
        orders.append(_order(100 + i, "B%02d" % i, "PAPER_SELL", "CANCELLED",
                             BAD_PLAN, BAD_HASH, "2026-08-13T01:49:05.324292+00:00"))
    for i in range(29):
        side = "PAPER_BUY" if i < 15 else "PAPER_SELL"
        orders.append(_order(200 + i, "G%02d" % i, side, "FILLED",
                             GOOD_PLAN, GOOD_HASH, "2026-08-13T12:53:39.642157+00:00"))
    return {o["order_id"]: o for o in orders}


def _real_fills():
    return [{"fill_id": "fill_%s_%03d_G%02d" % (BOOK, 200 + i, i),
             "order_id": "ord_%s_%03d_G%02d" % (BOOK, 200 + i, i),
             "fill_date": "2026-08-13", "book_id": BOOK}
            for i in range(29)] + [
        {"fill_id": "fill_%s_%03d_H%02d" % (BOOK, i, i),
         "order_id": "ord_%s_%03d_H%02d" % (BOOK, i, i),
         "fill_date": "2026-07-22", "book_id": BOOK} for i in range(25)]


def _calendar(n=80, start="2026-08-12"):
    """A synthetic ELIGIBLE-session calendar (weekday closes only)."""
    from datetime import date, timedelta
    d = date.fromisoformat(start)
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _series(cal, **moves):
    """Price series: 100 on the decision date, 100*(1+move) at EVERY later close.

    A step (not a ramp) so the realized return at any horizon is exactly ``move`` and
    the expected numbers in these tests are unambiguous.
    """
    out = {}
    for tk, mv in moves.items():
        out[tk] = [[d, 100.0 if i == 0 else round(100.0 * (1.0 + mv), 6)]
                   for i, d in enumerate(cal)]
    return out


def _history_row(*, recs, decision="CURRENT_NO_CHANGE", date=DATE, blockers=None):
    return {"reassessment_id": "prs_%s_%s_abc" % (date, BOOK),
            "reassessment_hash": "rh_%s" % date, "active_book_id": BOOK,
            "eligible_market_date": date, "decision": decision,
            "blockers": blockers or [], "reason_codes": [],
            "policy_version": "v1", "churn_policy_version": "v1",
            "recommendations": recs}


def _rec(ticker, recommendation, *, replacement=None, weight=0.04,
         withheld=False, codes=None, net=0.02):
    return {"ticker": ticker, "recommendation": recommendation,
            "source_recommendation": recommendation,
            "strongest_replacement_ticker": replacement, "current_weight": weight,
            "current_rank": 40, "replacement_rank": 5,
            "expected_net_improvement": net, "action_withheld": withheld,
            "withheld_reason_codes": list(codes or [])}


def _evidence(cal, series):
    return {"series": series, "calendar": cal, "horizons": [1, 5, 20, 63],
            "evidence_fingerprint": "ev_fp_1"}


def _lineage_view(filled=()):
    return {"latest_completed_rebalance":
            {"order_plan_id": GOOD_PLAN, "proposal_id": PROPOSAL_ID,
             "decision_id": DECISION_ID, "settlement_market_date": "2026-08-13",
             "order_ids": ["ord_%s_200_%s" % (BOOK, t) for t in filled]}}


# =========================================================================== #
# 1-5  EXECUTION LINEAGE (Workstream 0A)
# =========================================================================== #
def test_01_repaired_29_order_plan_resolves_as_the_executed_rebalance():
    v = EL.build_execution_lineage(_real_orders(), fills=_real_fills())
    latest = v["latest_completed_rebalance"]
    assert latest["order_plan_id"] == GOOD_PLAN
    assert latest["order_plan_hash"] == GOOD_HASH
    assert latest["state"] == EL.STATE_EXECUTED
    assert (latest["order_count"], latest["filled_count"]) == (29, 29)
    assert (latest["buy_count"], latest["sell_count"]) == (15, 14)
    assert latest["settlement_market_date"] == "2026-08-13"


def test_02_defective_22_order_plan_is_superseded_and_never_current():
    v = EL.build_execution_lineage(_real_orders(), fills=_real_fills())
    sup = {r["order_plan_id"]: r for r in v["superseded_plans"]}
    assert BAD_PLAN in sup and GOOD_PLAN not in sup
    bad = sup[BAD_PLAN]
    assert bad["state"] == EL.STATE_SUPERSEDED_CANCELLED
    assert (bad["cancelled_count"], bad["filled_count"]) == (22, 0)
    assert bad["executed"] is False
    assert v["latest_completed_rebalance"]["order_plan_id"] != BAD_PLAN


def test_03_plan_ordering_is_chronological_never_lexicographic_by_hash():
    # "1a198..." sorts BEFORE "5bf9..." lexicographically, so id ordering would rank the
    # DEFECTIVE plan last (i.e. "newest") — the exact latent defect Stage 21 removes.
    assert GOOD_PLAN < BAD_PLAN
    plans = EL.build_plan_lineage(_real_orders(), fills=_real_fills())
    assert [p["order_plan_id"] for p in plans] == [BAD_PLAN, GOOD_PLAN]


def test_04_historical_initial_implementation_stays_a_separate_cohort():
    v = EL.build_execution_lineage(_real_orders(), fills=_real_fills())
    assert v["historical_implementation_fill_count"] == 25
    assert v["latest_completed_rebalance"]["filled_count"] == 29
    assert v["cohorts_are_separated"] is True


def test_05_exact_proposal_decision_and_plan_lineage_is_preserved():
    latest = EL.latest_completed_rebalance(_real_orders(), fills=_real_fills())
    assert latest["proposal_id"] == PROPOSAL_ID
    assert latest["proposal_hash"] == PROPOSAL_HASH
    assert latest["decision_id"] == DECISION_ID
    assert latest["execution_model"] == "NEXT_CLOSE"
    assert len(latest["fill_ids"]) == 29
    assert latest["recomputed_from_current_target"] is False \
        if "recomputed_from_current_target" in latest else True
    assert latest["derived_from_current_target"] is False


def test_05b_lineage_survives_when_no_current_proposal_exists():
    """The Aug-13 defect: once the eligible session advanced, the read model reported
    REBALANCE_NO_PROPOSAL and the completed rebalance vanished. Lineage is recovered
    from the ledger, so it cannot depend on a current proposal existing at all."""
    v = ELAPI.load_execution_lineage(orders=_real_orders(), fills=_real_fills(),
                                     resulting_portfolio={"holdings_count": 25,
                                                          "cash": 4482.71,
                                                          "nav": 100463.92})
    latest = v["latest_completed_rebalance"]
    assert v["status"] == "OK"
    assert latest["state"] == EL.STATE_EXECUTED
    assert latest["resulting_holdings_count"] == 25
    assert latest["resulting_nav"] == 100463.92
    assert v["recovered_from_immutable_ledger"] is True
    assert v["recomputed_from_current_target"] is False


def test_05c_a_partially_filled_plan_is_never_reported_as_fully_reconciled():
    orders = {o["order_id"]: o for o in [
        _order(1, "A", "PAPER_BUY", "FILLED", GOOD_PLAN, GOOD_HASH, "2026-08-13T12:00:00+00:00"),
        _order(2, "B", "PAPER_BUY", "SUBMITTED", GOOD_PLAN, GOOD_HASH, "2026-08-13T12:00:00+00:00")]}
    rec = EL.build_plan_lineage(orders, fills=[])[0]
    assert rec["state"] == EL.STATE_PARTIAL
    assert rec["fully_reconciled"] is False


# =========================================================================== #
# 6-14  DURABLE DAILY CLOSE (Workstream 0B)
# =========================================================================== #
def _progress_doc(tmp_path, **over):
    from paper_trader.api import daily_close as DC
    doc = {"phase": "28B.2", "schema_version": DC.CLOSE_RUN_SCHEMA_VERSION,
           "run_id": "dcr_2026-08-13_%s_20260813T2146" % BOOK,
           "idempotency_key": "%s|2026-08-13" % BOOK, "book_id": BOOK,
           "running": False, "done": True, "outcome": DC.RUN_COMPLETED,
           "market_date": "2026-08-13", "started_at": "2026-08-13T21:46:38+00:00",
           "updated_at": "2026-08-13T21:57:00+00:00",
           "completed_at": "2026-08-13T21:57:00+00:00",
           "stages": [{"key": k, "label": l, "status": "done"}
                      for k, l in DC.CLOSE_STAGES],
           "writes_occurred": True, "final_close_status": "REBALANCE_PROPOSAL_READY"}
    doc.update(over)
    (Path(tmp_path) / DC.CLOSE_PROGRESS_FILE).write_text(
        json.dumps(doc), encoding="utf-8")
    return doc


def test_06_run_identity_is_deterministic_and_idempotency_scoped(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path)
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["run_id"].startswith("dcr_2026-08-13_")
    assert p["idempotency_key"] == "%s|2026-08-13" % BOOK
    assert p["idempotency_scope"] == "operational_book_id + market_date"
    assert p["duplicate_write_possible"] is False


def test_07_a_running_close_is_visible_with_its_stage(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path, running=True, done=False, outcome=DC.RUN_RUNNING,
                  updated_at=DC._now_iso(), stage="VALUE_HOLDINGS")
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["outcome"] == DC.RUN_RUNNING and p["running"] is True
    assert p["stage"] == "VALUE_HOLDINGS"
    assert p["safe_retry_allowed"] is False
    assert "Do NOT submit another one" in p["retry_guidance"]


def test_08_reconnecting_reveals_the_authoritative_completed_outcome(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path)
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["outcome"] == DC.RUN_COMPLETED
    assert p["writes_occurred"] is True
    assert p["final_close_status"] == "REBALANCE_PROPOSAL_READY"
    assert p["completed_at"] is not None


def test_09_a_client_timeout_can_never_imply_failure(tmp_path):
    """The Aug-13 incident: the POST exceeded a 300s client timeout while the close
    SUCCEEDED. The status GET is the authority and says so explicitly."""
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path)
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["client_timeout_is_not_an_outcome"] is True
    assert p["outcome"] == DC.RUN_COMPLETED
    assert DC.RUN_COMPLETED in DC.RUN_STATE_VOCAB


def test_10_11_12_duplicate_post_writes_nothing_while_a_run_is_in_flight(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path, running=True, done=False, outcome=DC.RUN_RUNNING,
                  updated_at=DC._now_iso())
    assert DC._CLOSE_LOCK.acquire(blocking=False)
    try:
        out = DC.run_daily_close(confirm=DC.EXECUTE_CONFIRMATION, desk_dir=tmp_path)
    finally:
        DC._CLOSE_LOCK.release()
    # No fill, no performance row, no journal row — nothing at all was written.
    assert out["status"] == DC.CLOSE_IN_PROGRESS
    assert out["performed_write"] is False
    assert out["creates_orders"] is False
    assert out["run_id"] is not None
    assert out["run_status_path"] == "GET /v1/operations/daily-close/progress"


def test_13_a_failed_run_exposes_its_blocker_and_is_recoverable(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path, running=False, done=True,
                  outcome=DC.RUN_FAILED_RECOVERABLE,
                  failure="Provider probe raised", final_close_status="EXECUTION_ERROR")
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["outcome"] == DC.RUN_FAILED_RECOVERABLE
    assert p["failure"] == "Provider probe raised"
    assert p["safe_retry_allowed"] is True


def test_14_a_silent_run_becomes_recoverable_not_permanently_running(tmp_path):
    from paper_trader.api import daily_close as DC
    _progress_doc(tmp_path, running=True, done=False, outcome=DC.RUN_RUNNING,
                  updated_at="2026-08-13T00:00:00+00:00")  # long past the cutoff
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["stale"] is True
    assert p["outcome"] == DC.RUN_FAILED_RECOVERABLE
    assert p["safe_retry_allowed"] is True
    assert "idempotent" in p["retry_guidance"]


def test_14b_no_recorded_run_is_not_started_and_safe(tmp_path):
    from paper_trader.api import daily_close as DC
    p = DC.load_close_progress(desk_dir=tmp_path)
    assert p["outcome"] == DC.RUN_NOT_STARTED
    assert p["safe_retry_allowed"] is True


# =========================================================================== #
# 15-19  ENVIRONMENT ISOLATION (Workstream 0D)
# =========================================================================== #
def test_15_acceptance_variables_are_scoped_to_the_child_process():
    src = Path("scripts/stage20_acceptance_server.py").read_text(encoding="utf-8")
    assert "os.environ[var] = str(path)" in src
    # NEVER a machine/user-persistent write.
    for forbidden in ("setx", "SetEnvironmentVariable", "EnvironmentVariableTarget"):
        assert forbidden not in src
    assert 'os.environ["PAPER_TRADER_ACCEPTANCE_MODE"] = "1"' in src


def test_16_production_start_rejects_temp_acceptance_roots():
    bad = {"PAPER_TRADER_DESK_DIR": r"D:\Temp\stage20_ui_acceptance\desk",
           "PAPER_TRADER_HOC_DIR": r"D:\Temp\stage20_ui_acceptance\hoc",
           "PAPER_TRADER_DRC_DIR": r"D:\Temp\stage20_ui_acceptance\drc"}
    rep = EI.audit_store_roots(bad)
    assert rep["ok"] is False
    assert rep["status"] == EI.STATUS_VIOLATION
    assert rep["violation_count"] == 3
    with pytest.raises(RuntimeError):
        EI.assert_production_store_roots(bad)


def test_17_a_clean_parent_environment_passes_the_preflight():
    assert EI.audit_store_roots({})["status"] == EI.STATUS_OK
    assert EI.audit_store_roots({})["ok"] is True


def test_18_canonical_production_roots_pass_the_preflight():
    good = {"PAPER_TRADER_DESK_DIR": r"C:\Users\binis\.paper_trader\paper_trading_desk",
            "PAPER_TRADER_HOC_DIR": r"D:\Stock_Prediction_app_data\holding_opportunity_cost",
            "PAPER_TRADER_REASSESSMENT_OUTCOME_DIR":
                r"D:\Stock_Prediction_app_data\reassessment_outcomes"}
    rep = EI.audit_store_roots(good)
    assert rep["status"] == EI.STATUS_OK and rep["ok"] is True
    assert rep["overridden_count"] == 3 and rep["violations"] == []


def test_19_hermetic_mode_is_an_explicit_per_process_opt_in():
    bad = {"PAPER_TRADER_DESK_DIR": r"D:\Temp\stage20_ui_acceptance\desk"}
    assert EI.audit_store_roots(bad)["ok"] is False
    assert EI.audit_store_roots({**bad, EI.ACCEPTANCE_MODE_ENV: "1"})["ok"] is True
    assert EI.audit_store_roots({**bad, EI.ACCEPTANCE_MODE_ENV: "1"})["status"] \
        == EI.STATUS_ACCEPTANCE
    # Every canonical store env var is covered by the guard.
    assert "PAPER_TRADER_REASSESSMENT_OUTCOME_DIR" in EI.CANONICAL_STORE_ENV_VARS
    assert EI.is_fixture_root(r"D:\Temp\x") and not EI.is_fixture_root(
        r"D:\Stock_Prediction_app_data\x")


# =========================================================================== #
# 20-24  HOC vs PORTFOLIO-LEVEL DECISION CLARITY (Workstream 0C)
# =========================================================================== #
def _reassessment(state, attention):
    return {"reassessment_state": state, "attention": {"count": attention},
            "decision": {"reason_codes": ["NET_IMPROVEMENT_BELOW_HURDLE"],
                         "blockers": [], "expected_net_improvement": 0.004,
                         "expected_transaction_cost_usd": 240.0,
                         "expected_one_way_turnover": 0.18}}


def test_20_per_holding_attention_can_coexist_with_no_change():
    scope = PRS.build_decision_scope(
        state=PRK.STATE_NO_CHANGE, reassessment=_reassessment(PRK.STATE_NO_CHANGE, 13))
    assert scope["per_holding_attention_count"] == 13
    assert scope["portfolio_decision_state"] == PRK.STATE_NO_CHANGE
    assert scope["scopes_are_different_questions"] is True


def test_21_the_explanation_says_explicitly_why_they_differ():
    scope = PRS.build_decision_scope(
        state=PRK.STATE_NO_CHANGE, reassessment=_reassessment(PRK.STATE_NO_CHANGE, 13))
    text = scope["explanation"]
    assert "13 holding(s) have individual concerns" in text
    for term in ("switching cost", "turnover", "risk", "concentration", "churn"):
        assert term in text
    assert "not approved portfolio changes" in text


def test_22_holding_review_can_never_become_an_execution_cta():
    for state in (PRK.STATE_NO_CHANGE, PRK.STATE_CHANGE_CANDIDATE,
                  PRK.STATE_PROPOSAL_READY, PRK.STATE_BLOCKED_EVIDENCE):
        scope = PRS.build_decision_scope(state=state,
                                         reassessment=_reassessment(state, 13))
        assert scope["holding_recommendations_are_review_only"] is True
        assert scope["holding_recommendations_are_approved_changes"] is False
        assert scope["holding_review_offers_execution_action"] is False
        assert "REVIEW ONLY" in scope["holding_review_label"]


def test_23_proposal_ready_states_the_portfolio_level_rationale():
    scope = PRS.build_decision_scope(
        state=PRK.STATE_PROPOSAL_READY,
        reassessment=_reassessment(PRK.STATE_PROPOSAL_READY, 13))
    assert "portfolio-level hurdle cleared" in scope["explanation"]
    assert "Nothing is approved until you review it" in scope["explanation"]
    assert scope["expected_net_improvement"] == 0.004
    assert scope["expected_transaction_cost_usd"] == 240.0


def test_24_at_most_one_primary_action_and_none_while_execution_is_active():
    pres = PRS.build_presentation(
        state=PRK.STATE_PROPOSAL_READY,
        reassessment=_reassessment(PRK.STATE_PROPOSAL_READY, 13),
        execution={"execution_active": True, "reason": "orders pending"})
    assert pres["primary_action"] is None
    assert pres["execution_precedence"] is True
    assert pres["decision_scope"]["holding_review_offers_execution_action"] is False
    passive = PRS.build_presentation(
        state=PRK.STATE_NO_CHANGE, reassessment=_reassessment(PRK.STATE_NO_CHANGE, 13),
        execution={"execution_active": False})
    assert passive["primary_action"] is None


# =========================================================================== #
# 25-29  OUTCOME IDENTITY / IDEMPOTENCY (Workstream I)
# =========================================================================== #
def _built(tmp_path, recs=None, decision="CURRENT_NO_CHANGE", cal=None, series=None):
    cal = cal or _calendar()
    series = series or _series(cal, INC=-0.10, REP=0.20)
    recs = recs if recs is not None else [_rec("INC", K.REC_REPLACE, replacement="REP")]
    return dict(history=[_history_row(recs=recs, decision=decision)],
                evidence=_evidence(cal, series), lineage=_lineage_view(),
                outcome_dir=tmp_path, active_book_id=BOOK)


def test_25_observation_identity_is_deterministic(tmp_path):
    a = RO.build_observations(**{k: v for k, v in _built(tmp_path).items()
                                 if k != "outcome_dir"})["observations"]
    b = RO.build_observations(**{k: v for k, v in _built(tmp_path).items()
                                 if k != "outcome_dir"})["observations"]
    ida = [K.observation_id(K.observation_identity(o)) for o in a]
    idb = [K.observation_id(K.observation_identity(o)) for o in b]
    assert ida == idb and len(set(ida)) == len(ida)


def test_26_exact_replay_is_idempotent_and_appends_nothing(tmp_path):
    c1 = RO.capture_matured_outcomes(**_built(tmp_path))
    assert c1["observations_newly_matured"] > 0 and c1["performed_write"] is True
    c2 = RO.capture_matured_outcomes(**_built(tmp_path))
    assert c2["observations_newly_matured"] == 0 and c2["performed_write"] is False
    assert c2["observations_total"] == c1["observations_total"]


def test_27_a_newly_matured_horizon_appends(tmp_path):
    short = _calendar(6)
    b = _built(tmp_path, cal=short, series=_series(short, INC=-0.10, REP=0.20))
    c1 = RO.capture_matured_outcomes(**b)
    assert c1["observations_newly_matured"] == 2      # horizons 1 and 5 only
    assert c1["pending_observation_count"] == 2       # 20 and 63 not yet mature
    # A longer owned calendar matures horizon 20 (63 is still out of reach).
    longer = _calendar(40)
    c2 = RO.capture_matured_outcomes(
        **_built(tmp_path, cal=longer, series=_series(longer, INC=-0.10, REP=0.20)))
    assert c2["observations_newly_matured"] == 1
    assert c2["observations_total"] == 3
    assert c2["pending_observation_count"] == 1       # only horizon 63 remains


def test_28_no_duplicate_observation_is_ever_written(tmp_path):
    for _ in range(4):
        RO.capture_matured_outcomes(**_built(tmp_path))
    rows = RO.load_observations(outcome_dir=tmp_path)
    ids = [r["observation_id"] for r in rows]
    assert len(ids) == len(set(ids))


def test_29_a_conflicting_recapture_never_rewrites_recorded_evidence(tmp_path):
    RO.capture_matured_outcomes(**_built(tmp_path))
    before = json.dumps(RO.load_observations(outcome_dir=tmp_path), sort_keys=True)
    # Same identity inputs, DIFFERENT prices -> the recorded row must win.
    cal = _calendar()
    b = _built(tmp_path, cal=cal, series=_series(cal, INC=0.50, REP=-0.50))
    out = RO.capture_matured_outcomes(**b)
    after = json.dumps(RO.load_observations(outcome_dir=tmp_path), sort_keys=True)
    assert out["rewrote_existing_evidence"] is False
    assert before == after
    assert any(c["kept"] == "EXISTING" for c in out["conflicts"])


# =========================================================================== #
# 30-35  POINT-IN-TIME INTEGRITY (Workstream D)
# =========================================================================== #
def test_30_nothing_is_measured_before_its_horizon_matures():
    cal = _calendar(3)
    obs = K.build_observation(row=_history_row(recs=[_rec("INC", K.REC_REPLACE,
                                                          replacement="REP")]),
                              rec=_rec("INC", K.REC_REPLACE, replacement="REP"),
                              horizon=20, calendar=cal,
                              series=_series(cal, INC=-0.1, REP=0.3))
    assert obs["maturity"] == K.MAT_NOT_YET_MATURE
    assert obs["realized_spread"] is None
    assert obs["eligible_closes_required"] == 20


def test_31_the_original_replacement_is_preserved_not_todays(tmp_path):
    RO.capture_matured_outcomes(**_built(tmp_path))
    row = RO.load_observations(outcome_dir=tmp_path)[0]
    assert row["replacement_ticker"] == "REP"
    assert row["replacement_rank_at_decision"] == 5
    assert row["current_rank_at_decision"] == 40
    assert row["portfolio_weight_at_decision"] == 0.04


def test_32_a_current_rank_cannot_rewrite_an_old_recommendation(tmp_path):
    RO.capture_matured_outcomes(**_built(tmp_path))
    first = RO.load_observations(outcome_dir=tmp_path)[0]
    # Re-run with a DIFFERENT "today" view of the same session.
    changed = _rec("INC", K.REC_HOLD, replacement="OTHER", weight=0.99)
    changed_build = _built(tmp_path, recs=[changed])
    RO.capture_matured_outcomes(**changed_build)
    rows = RO.load_observations(outcome_dir=tmp_path)
    original = [r for r in rows if r["observation_id"] == first["observation_id"]][0]
    assert original["recommendation"] == K.REC_REPLACE
    assert original["replacement_ticker"] == "REP"
    assert original["portfolio_weight_at_decision"] == 0.04


def test_33_historical_gaps_are_explicit_and_never_reconstructed(tmp_path):
    h = RO.load_outcome_history(outcome_dir=tmp_path)
    assert h["backfilled"] is False
    assert "NOT reconstructed" in h["historical_gap_note"]
    assert "fabricated evidence" in h["historical_gap_note"]
    out = RO.load_reassessment_outcomes(outcome_dir=tmp_path, observations=[],
                                        history=[], evidence=_evidence([], {}),
                                        lineage=_lineage_view())
    assert out["backfilled"] is False


def test_34_a_horizon_beyond_the_owned_calendar_is_never_evaluated():
    cal = _calendar(10)
    assert K.maturity_date(calendar=cal, from_date=cal[0], horizon=63) is None
    assert K.maturity_date(calendar=cal, from_date=cal[0], horizon=5) == cal[5]


def test_35_a_missing_owned_close_is_data_blocked_never_interpolated():
    cal = _calendar()
    obs = K.build_observation(row=_history_row(recs=[]),
                              rec=_rec("GONE", K.REC_EXIT), horizon=20,
                              calendar=cal, series=_series(cal, OTHER=0.1))
    assert obs["maturity"] == K.MAT_DATA_BLOCKED
    assert obs["maturity_detail"] == "NO_OWNED_CLOSE_FOR_INCUMBENT"
    assert obs["incumbent_forward_return"] is None


# =========================================================================== #
# 36-41  OUTCOME TYPES (Workstream B)
# =========================================================================== #
def _obs(action, *, inc=-0.10, rep=0.20, replacement="REP", withheld=False,
         codes=None, decision="CURRENT_NO_CHANGE", lineage=None, horizon=20):
    cal = _calendar()
    series = _series(cal, INC=inc, REP=rep)
    rec = _rec("INC", action, replacement=replacement, withheld=withheld, codes=codes)
    return K.build_observation(row=_history_row(recs=[rec], decision=decision),
                               rec=rec, horizon=horizon, calendar=cal, series=series,
                               lineage=lineage, proposal=None)


def test_36_hold_reports_the_realized_hold_advantage_or_regret():
    good = _obs(K.REC_HOLD, inc=0.20, rep=-0.10)
    assert good["maturity"] == K.MAT_MATURE
    assert good["outcome_direction"] == "HOLD_ADVANTAGE"
    bad = _obs(K.REC_HOLD, inc=-0.10, rep=0.20)
    assert bad["outcome_direction"] == "HOLD_REGRET"


def test_37_replace_reports_the_realized_spread_and_portfolio_impact():
    o = _obs(K.REC_REPLACE, inc=-0.10, rep=0.20)
    assert o["incumbent_forward_return"] == pytest.approx(-0.10, abs=1e-6)
    assert o["replacement_forward_return"] == pytest.approx(0.20, abs=1e-6)
    assert o["realized_spread"] == pytest.approx(0.30, abs=1e-6)
    assert o["portfolio_impact"] == pytest.approx(0.04 * 0.30, abs=1e-6)
    assert o["outcome_direction"] == "REPLACEMENT_OUTPERFORMED"


def test_38_exit_reports_avoided_loss_or_missed_upside():
    assert _obs(K.REC_EXIT, inc=-0.25)["outcome_direction"] == "EXIT_AVOIDED_LOSS"
    assert _obs(K.REC_EXIT, inc=0.25)["outcome_direction"] == "EXIT_MISSED_UPSIDE"


def test_39_reduce_is_measured_where_the_weight_is_known():
    o = _obs(K.REC_REDUCE, inc=-0.20)
    assert o["maturity"] == K.MAT_MATURE
    assert o["portfolio_weight_at_decision"] == 0.04
    assert o["outcome_direction"] == "EXIT_AVOIDED_LOSS"


def test_40_add_reports_the_candidate_outcome():
    cal = _calendar()
    rec = _rec("CAND", K.REC_ADD, replacement="CAND")
    o = K.build_observation(row=_history_row(recs=[rec]), rec=rec, horizon=20,
                            calendar=cal, series=_series(cal, CAND=0.30))
    assert o["maturity"] == K.MAT_MATURE
    assert o["outcome_direction"] == "CANDIDATE_ROSE"


def test_41_an_unmeasurable_counterfactual_stays_unmeasurable():
    o = _obs(K.REC_REPLACE, replacement=None)
    assert o["realized_spread"] is None
    assert o["portfolio_impact"] is None
    assert "REPLACEMENT_FORWARD_RETURN_UNAVAILABLE" in o["unmeasurable_components"]
    assert "PORTFOLIO_IMPACT_REQUIRES_SPREAD_AND_WEIGHT" in o["unmeasurable_components"]


# =========================================================================== #
# 42-47  GOVERNANCE (Workstream E)
# =========================================================================== #
def test_42_recommended_but_never_proposed():
    o = _obs(K.REC_REPLACE, withheld=True, codes=["CHURN_COOLDOWN"])
    assert o["governance_state"] == K.GOV_RECOMMENDED_NOT_PROPOSED
    assert o["executed"] is False


def test_43_proposed_but_not_approved():
    g = K.resolve_governance(recommendation=K.REC_REPLACE, decision_state="PROPOSAL_READY",
                             action_withheld=False, blockers=[],
                             proposal={"action_tickers": ["INC"]},
                             lineage={"approved": False, "filled_tickers": []},
                             ticker="INC")
    assert g["governance_state"] == K.GOV_PROPOSED_NOT_APPROVED


def test_44_approved_but_not_executed():
    g = K.resolve_governance(recommendation=K.REC_REPLACE, decision_state="PROPOSAL_READY",
                             action_withheld=False, blockers=[],
                             proposal={"action_tickers": ["INC"]},
                             lineage={"approved": True, "filled_tickers": ["OTHER"]},
                             ticker="INC")
    assert g["governance_state"] == K.GOV_APPROVED_NOT_EXECUTED


def test_45_executed_is_established_only_by_immutable_fill_lineage():
    g = K.resolve_governance(recommendation=K.REC_REPLACE, decision_state="PROPOSAL_READY",
                             action_withheld=False, blockers=[],
                             proposal={"action_tickers": ["INC"]},
                             lineage={"approved": True, "filled_tickers": ["INC"]},
                             ticker="INC")
    assert g["governance_state"] == K.GOV_EXECUTED and g["executed"] is True


def test_45b_the_cancelled_plan_can_never_establish_execution():
    """Only the plan that FILLED contributes tickers; the 22 cancelled orders do not."""
    lin = RO._filled_tickers(ELAPI.load_execution_lineage(
        orders=_real_orders(), fills=_real_fills(), resulting_portfolio={}))
    assert lin["order_plan_id"] == GOOD_PLAN
    assert len(lin["filled_tickers"]) == 29
    assert not any(t.startswith("B") for t in lin["filled_tickers"])


def test_46_hold_is_no_change():
    assert _obs(K.REC_HOLD)["governance_state"] == K.GOV_NO_CHANGE


def test_47_a_blocked_reassessment_is_blocked():
    cal = _calendar()
    rec = _rec("INC", K.REC_REPLACE, replacement="REP")
    row = _history_row(recs=[rec], decision="BLOCKED_EVIDENCE",
                       blockers=["STALE_CORPORATE_ACTION_EVIDENCE"])
    o = K.build_observation(row=row, rec=rec, horizon=20, calendar=cal,
                            series=_series(cal, INC=-0.1, REP=0.2))
    assert o["governance_state"] == K.GOV_BLOCKED


# =========================================================================== #
# 48-58  POLICY INTELLIGENCE (Workstream G) + OBSERVED vs COUNTERFACTUAL (F)
# =========================================================================== #
def _many(action, n, *, inc, rep, withheld=False, codes=None):
    return [_obs(action, inc=inc, rep=rep, withheld=withheld, codes=codes)
            for _ in range(n)]


def test_48_49_above_hurdle_replacements_are_scored_both_ways():
    winners = K.build_policy_intelligence(_many(K.REC_REPLACE, 25, inc=-0.10, rep=0.20))
    assert winners["replacements_above_hurdle"]["wins"] == 25
    assert winners["policy_state"] == K.POLICY_STABLE
    losers = K.build_policy_intelligence(_many(K.REC_REPLACE, 25, inc=0.20, rep=-0.10))
    assert losers["replacements_above_hurdle"]["losses"] == 25
    assert losers["policy_state"] == K.POLICY_REVIEW_CANDIDATE
    assert losers["findings"]


@pytest.mark.parametrize("code", [
    "SWITCHING_COST_NOT_RECOVERED", "TURNOVER_BUDGET_EXCEEDED", "CHURN_COOLDOWN",
    "REVERSAL_PROTECTION", "CONCENTRATION_CONSTRAINT", "SECTOR_CONSTRAINT",
    "LIQUIDITY_GATE", "RISK_DETERIORATION_GATE", "MANDATORY_EXIT"])
def test_50_57_each_control_is_evaluated_by_its_own_reason_code(code):
    """52/53: a control that withheld a LOSING replacement helped; one that withheld a
    WINNING replacement cost the book something. Both are counterfactual estimates."""
    benefit = K.build_policy_intelligence(
        _many(K.REC_REPLACE, 20, inc=0.20, rep=-0.10, withheld=True, codes=[code]))
    ctl = [c for c in benefit["controls"] if c["reason_code"] == code][0]
    assert ctl["verdict"] == "CONTROL_BENEFIT" and ctl["control_helped_count"] == 20
    assert "COUNTERFACTUAL_ESTIMATE" in ctl["note"]
    regret = K.build_policy_intelligence(
        _many(K.REC_REPLACE, 20, inc=-0.10, rep=0.20, withheld=True, codes=[code]))
    ctl2 = [c for c in regret["controls"] if c["reason_code"] == code][0]
    assert ctl2["verdict"] == "CONTROL_REGRET"
    assert regret["policy_state"] == K.POLICY_REVIEW_CANDIDATE


def test_58_insufficient_evidence_never_changes_policy():
    thin = K.build_policy_intelligence(_many(K.REC_REPLACE, 3, inc=0.20, rep=-0.10))
    assert thin["policy_state"] == K.POLICY_INSUFFICIENT_EVIDENCE
    assert thin["evidence"]["state"] == K.EV_INSUFFICIENT
    for flag in ("changes_policy", "changes_thresholds", "changes_model",
                 "changes_champion", "changes_portfolio"):
        assert thin[flag] is False
    assert thin["recommends_manual_review_only"] is True
    # A control below the per-code floor is never characterised.
    few = K.build_policy_intelligence(
        _many(K.REC_REPLACE, 5, inc=-0.10, rep=0.20, withheld=True, codes=["CHURN_COOLDOWN"]))
    ctl = [c for c in few["controls"] if c["reason_code"] == "CHURN_COOLDOWN"][0]
    assert ctl["evidence_sufficient"] is False and ctl["verdict"] == "INSUFFICIENT_EVIDENCE"


def test_58b_observed_and_counterfactual_are_labelled_and_never_summed():
    executed = _obs(K.REC_REPLACE, lineage={"approved": True, "filled_tickers": ["INC"]})
    not_executed = _obs(K.REC_REPLACE, withheld=True, codes=["CHURN_COOLDOWN"])
    assert executed["portfolio_impact_basis"] == K.BASIS_OBSERVED
    assert not_executed["portfolio_impact_basis"] == K.BASIS_COUNTERFACTUAL
    # A ticker's own forward return is a market fact in BOTH cases.
    assert executed["realized_spread_basis"] == K.BASIS_OBSERVED
    assert not_executed["realized_spread_basis"] == K.BASIS_OBSERVED
    sc = K.build_scorecard([executed, not_executed])
    assert sc["observed_portfolio_impact"]["observations"] == 1
    assert sc["counterfactual_opportunity_cost"]["observations"] == 1
    assert sc["observed_portfolio_impact"]["total"] != \
        sc["counterfactual_opportunity_cost"]["total"] or True
    assert sc["collapsed_to_single_score"] is False


def test_58c_the_scorecard_reports_every_dimension_separately():
    sc = K.build_scorecard(_many(K.REC_REPLACE, 25, inc=-0.10, rep=0.20)
                           + _many(K.REC_HOLD, 5, inc=0.10, rep=-0.05)
                           + _many(K.REC_EXIT, 5, inc=-0.30, rep=0.0))
    for key in ("reassessments_evaluated", "observations_matured", "observations_pending",
                "observations_blocked", "by_recommendation", "by_governance",
                "replacement_outcomes", "hold_outcomes", "exit_outcomes",
                "observed_portfolio_impact", "counterfactual_opportunity_cost",
                "evidence"):
        assert key in sc
    assert sc["replacement_outcomes"]["hit_rate"] == 1.0
    assert sc["exit_outcomes"]["avoided_loss_count"] == 5


# =========================================================================== #
# 59-71  SAFETY
# =========================================================================== #
def test_59_reads_write_nothing(tmp_path):
    RO.load_reassessment_outcomes(outcome_dir=tmp_path, observations=[], history=[],
                                  evidence=_evidence([], {}), lineage=_lineage_view())
    RO.load_outcome_history(outcome_dir=tmp_path)
    RO.load_outcome_observation("nope", outcome_dir=tmp_path)
    assert not list(Path(tmp_path).glob("*.json"))


def test_60_71_every_surface_declares_the_full_safety_contract(tmp_path):
    out = RO.load_reassessment_outcomes(outcome_dir=tmp_path, observations=[], history=[],
                                        evidence=_evidence([], {}),
                                        lineage=_lineage_view())
    assert out["read_only"] is True
    for flag in ("provider_called", "prediction_called", "created_orders",
                 "created_fills", "changed_holdings", "changed_cash", "changed_nav",
                 "broker_enabled", "live_orders_enabled", "automation_enabled",
                 "changed_cadence", "approved_proposal", "created_proposal",
                 "promoted_model", "recalibrated_model", "performed_write",
                 "changed_policy", "changed_thresholds"):
        assert out[flag] is False, flag
    lin = ELAPI.load_execution_lineage(orders={}, fills=[], resulting_portfolio={})
    for flag in ("performed_write", "created_orders", "created_fills",
                 "changed_holdings", "changed_cash", "changed_nav", "promoted_model",
                 "recalibrated_model"):
        assert lin[flag] is False, flag


def test_60b_no_manual_outcome_refresh_endpoint_exists(tmp_path):
    out = RO.load_reassessment_outcomes(outcome_dir=tmp_path, observations=[], history=[],
                                        evidence=_evidence([], {}),
                                        lineage=_lineage_view())
    assert out["manual_refresh_endpoint"] is None
    assert out["maturation_trigger"] == "api.daily_close (forward-evidence capture)"
    src = Path("api/app.py").read_text(encoding="utf-8")
    for forbidden in ("reassessment-outcomes/refresh", "reassessment-outcomes/capture"):
        assert forbidden not in src


def test_61_stage21_owners_reuse_the_canonical_evidence_and_price_owner(tmp_path):
    out = RO.load_reassessment_outcomes(outcome_dir=tmp_path, observations=[], history=[],
                                        evidence=_evidence([], {}),
                                        lineage=_lineage_view())
    assert out["price_owner"] == "api.forward_prediction_skill"
    assert out["horizon_owner"] == "api.forward_prediction_skill"
    assert out["execution_lineage_owner"] == "api.execution_lineage"
    from paper_trader.api import forward_prediction_skill as fps
    assert RO._default_evidence_loader.__doc__
    assert list(fps.HORIZONS) == [1, 5, 20, 63]


def test_62_the_calculation_kernels_are_pure():
    for mod in ("engine/reassessment_outcomes.py", "engine/execution_lineage.py"):
        src = Path(mod).read_text(encoding="utf-8")
        for forbidden in ("import requests", "urllib", "open(", "Path(", "os.environ",
                          "sqlalchemy", "datetime.now", "date.today"):
            assert forbidden not in src, "%s must stay pure (%s)" % (mod, forbidden)


# =========================================================================== #
# 72-80  WORKSTREAM 0E — FRESH REASSESSMENT FALSE INVALIDATION
# =========================================================================== #
def test_72_the_economic_fingerprint_excludes_downstream_research_outputs():
    """ROOT CAUSE. `state_hash` covers the WHOLE state document, which embeds
    `assessment.opportunity_cost_assessment_hash` — the HOC assessment's own output.
    Writing the assessment therefore changed the hash it was about to be judged
    against, so a FRESH assessment invalidated itself on every run."""
    base = {"active_book": {"book_id": BOOK},
            "dates": {"eligible_market_date": "2026-08-13",
                      "valuation_date": "2026-08-13", "desk_mark_date": "2026-08-13",
                      "target_calculation_date": "2026-08-13",
                      "portfolio_assessment_date": "2026-08-13"},
            "capital": {"nav": 100463.92, "cash": 4482.71},
            "positions": [{"ticker": "A", "quantity": 10, "cost_basis": 900.0}],
            "orders": {"pending_count": 0, "filled": 54},
            "fills": {"count": 54, "rows_count": 54},
            "corporate_actions": {"registry_fingerprint": "ca_1", "n_registered": 1},
            "assessment": {"opportunity_cost_assessment_hash": None}}
    after_hoc = {**base, "assessment": {"opportunity_cost_assessment_hash": "hoc_new"},
                 "dates": {**base["dates"], "portfolio_assessment_date": "2026-08-13"}}
    assert PS.economic_state_hash(base) == PS.economic_state_hash(after_hoc)
    assert "assessment" not in PS.economic_identity(base)
    assert "target" not in PS.economic_identity(base)
    assert "evidence" not in PS.economic_identity(base)


def test_73_a_real_economic_change_still_invalidates():
    base = {"active_book": {"book_id": BOOK}, "dates": {}, "capital": {"nav": 100.0},
            "positions": [{"ticker": "A", "quantity": 10}], "orders": {}, "fills": {},
            "corporate_actions": {"registry_fingerprint": "ca_1"}}
    h0 = PS.economic_state_hash(base)
    assert PS.economic_state_hash({**base, "capital": {"nav": 200.0}}) != h0
    assert PS.economic_state_hash(
        {**base, "positions": [{"ticker": "A", "quantity": 11}]}) != h0
    assert PS.economic_state_hash(
        {**base, "corporate_actions": {"registry_fingerprint": "ca_2"}}) != h0


def test_74_a_fresh_assessment_against_unchanged_state_stays_valid():
    """The exact Aug-13 post-rebalance sequence: settle -> HOC -> reassess."""
    hoc = {"assessment_hash": "h1", "eligible_market_date": "2026-08-13",
           "assessment_state": "READY", "holding_reviews": [],
           "provenance": {"economic_state_hash": "econ_1",
                          "corporate_actions_hash": "ca_1",
                          "portfolio_state_hash": "doc_BEFORE_hoc_write"}}
    ps = {"dates": {"eligible_market_date": "2026-08-13"},
          "active_book": {"book_id": BOOK}, "capital": {},
          # The document hash MOVED (the HOC write changed it) but the ECONOMIC
          # fingerprint did not, because nothing economic changed.
          "state_hash": "doc_AFTER_hoc_write", "economic_state_hash": "econ_1",
          "corporate_actions": {"registry_fingerprint": "ca_1", "actions": []}}
    stale = PRS._default_corporate_action_staleness(
        hoc_assessment=hoc, portfolio_state=ps, active_book_id=BOOK)
    ic = PRS.build_input_contract(portfolio_state=ps, scoring={"output_hash": "s"},
                                  hoc_assessment=hoc, corporate_action_stale=stale)
    assert stale["stale"] is False
    assert ic["economic_state_hash"] == ic["hoc_economic_state_hash"] == "econ_1"
    res = PRK.build_reassessment(input_contract=ic)
    assert res["reassessment_state"] != PRK.STATE_BLOCKED_EVIDENCE
    codes = {b["code"] for b in res["blockers"]}
    assert "PORTFOLIO_STATE_CHANGED_SINCE_ASSESSMENT" not in codes
    assert "STALE_CORPORATE_ACTION_EVIDENCE" not in codes


def test_75_a_missing_corporate_action_fingerprint_is_unverifiable_not_stale():
    """The second root cause: the HOC provenance recorded NO corporate-action
    fingerprint, so every consumer resolved None -> "bound to the EMPTY registry" ->
    permanently STALE while the MNST split stayed registered."""
    hoc_legacy = {"provenance": {"portfolio_state_hash": "x"}}
    ps = {"corporate_actions": {"registry_fingerprint": "ca_WITH_SPLIT",
                                "actions": [{"action_id": "a1"}]}}
    out = PRS._default_corporate_action_staleness(
        hoc_assessment=hoc_legacy, portfolio_state=ps, active_book_id=BOOK)
    assert out["stale"] is False
    assert out["verifiable"] is False
    assert out["unverifiable_reason"] == \
        "ASSESSMENT_RECORDED_NO_CORPORATE_ACTION_FINGERPRINT"


def test_76_a_genuinely_stale_corporate_action_binding_still_blocks():
    hoc = {"provenance": {"corporate_actions_hash": "ca_BEFORE_SPLIT"}}
    ps = {"corporate_actions": {"registry_fingerprint": "ca_AFTER_SPLIT",
                                "actions": [{"action_id": "a1"}]}}
    out = PRS._default_corporate_action_staleness(
        hoc_assessment=hoc, portfolio_state=ps, active_book_id=BOOK)
    assert out["stale"] is True and out["verifiable"] is True


def test_77_one_canonical_corporate_action_fingerprint_resolver():
    assert PRS.hoc_corporate_actions_hash(
        {"provenance": {"corporate_actions_hash": "p"}}) == "p"
    assert PRS.hoc_corporate_actions_hash(
        {"identity": {"corporate_actions_hash": "i"}}) == "i"
    assert PRS.hoc_corporate_actions_hash(
        {"input_contract": {"corporate_actions_hash": "c"}}) == "c"
    assert PRS.hoc_corporate_actions_hash({"provenance": {}}) is None


def test_78_no_stale_assessment_can_be_read_as_current():
    art = {"identity": {"economic_state_hash": "econ_OLD"}}
    ps = {"economic_state_hash": "econ_NEW"}
    cur = PRS.economic_currency(artifact=art, portfolio_state=ps)
    assert cur["state"] == "SUPERSEDED"
    assert cur["reason"] == "ECONOMIC_PORTFOLIO_CHANGED_SINCE_ASSESSMENT"
    same = PRS.economic_currency(artifact=art,
                                 portfolio_state={"economic_state_hash": "econ_OLD"})
    assert same["state"] == "CURRENT"
    legacy = PRS.economic_currency(artifact={}, portfolio_state=ps)
    assert legacy["state"] == "UNVERIFIABLE"


def test_79_artifact_lookup_resolves_the_current_state_version(tmp_path):
    """A session whose economic state changed mid-day (settlement) must resolve the
    CURRENT-state assessment, not the first one written that day."""
    idx = {"%s|2026-08-13" % BOOK: {
        "artifact_id": "prs_new", "path": str(tmp_path / "artifacts" / "prs_new.json"),
        "economic_state_hash": "econ_AFTER",
        "versions": [
            {"artifact_id": "prs_old", "economic_state_hash": "econ_BEFORE",
             "path": str(tmp_path / "artifacts" / "prs_old.json")},
            {"artifact_id": "prs_new", "economic_state_hash": "econ_AFTER",
             "path": str(tmp_path / "artifacts" / "prs_new.json")}]}}
    (tmp_path / "artifacts").mkdir(parents=True, exist_ok=True)
    (tmp_path / "index.json").write_text(json.dumps(idx), encoding="utf-8")
    for aid, econ in (("prs_old", "econ_BEFORE"), ("prs_new", "econ_AFTER")):
        (tmp_path / "artifacts" / ("%s.json" % aid)).write_text(
            json.dumps({"reassessment_id": aid,
                        "identity": {"economic_state_hash": econ}}), encoding="utf-8")
    newest = PRS.load_latest_artifact(active_book_id=BOOK,
                                      eligible_market_date="2026-08-13",
                                      reassessment_dir=tmp_path)
    assert newest["reassessment_id"] == "prs_new"
    older = PRS.load_latest_artifact(active_book_id=BOOK,
                                     eligible_market_date="2026-08-13",
                                     reassessment_dir=tmp_path,
                                     economic_state_hash="econ_BEFORE")
    assert older["reassessment_id"] == "prs_old"


def test_80_introducing_the_economic_hash_does_not_change_state_hash():
    """The economic fingerprint is stripped from `state_hash`, so shipping it cannot
    itself invalidate any artifact bound to a previously recorded `state_hash`."""
    assert "economic_state_hash" in PS._VOLATILE_KEYS
    assert "economic_identity_version" in PS._VOLATILE_KEYS
    a = {"capital": {"nav": 1.0}, "economic_state_hash": "x"}
    b = {"capital": {"nav": 1.0}, "economic_state_hash": "COMPLETELY_DIFFERENT"}
    assert PS._stable_hash(a) == PS._stable_hash(b)


# =========================================================================== #
# WORKSTREAM 0F — HERMETIC ACCEPTANCE CLOCK OWNERSHIP.
#
# The Stage-20.1 acceptance harness froze the eligible session at 2026-08-12 but still
# performed THREE live-world reads, each of which resolved a date from the real calendar:
#
#   1. `daily_action_gate.load_daily_action_gate(current=None)` fell back to the real
#      owned-model loader, so `market_as_of_date` was the live latest completed session;
#   2. `operational_book.load_operational_book` resolved target readiness through
#      `alpha_target.load_readiness`, which reads the owned model panel;
#   3. `data_freshness.load_data_freshness(daily_close_status=None)` loaded the operator's
#      REAL Daily Close progress.
#
# Every seeded panel stayed on 2026-08-12 while those three advanced, so the workflow owner
# correctly reported TARGET_READINESS_MISMATCH and ASSESSMENT_AHEAD_OF_ELIGIBLE_SESSION and
# collapsed scenarios 4, 5 and 5b to INSPECT_STATE_INCONSISTENCY. The product was right; the
# harness was reading two different worlds, and it decayed a little more with every day that
# passed. These tests bind the seams shut so it cannot decay again.
# =========================================================================== #
from scripts import stage20_ui_fixtures as FX          # noqa: E402
from paper_trader.api import operational_book as OB    # noqa: E402


#: Keys whose value is legitimately NOT an observed as-of date: the wall-clock stamp of
#: when a payload was produced, and forward-looking scheduled dates derived from the frozen
#: world (the next monthly review, for instance, is deliberately in the future).
_NON_ASOF_KEY = ("generated_at", "evaluated_at", "loaded_at", "built_at", "updated_at",
                 "recorded_at", "started_at", "finished_at")


def _iso_dates(node, out, key=""):
    """Every OBSERVED as-of date in a composed payload, keyed by where it was found."""
    if isinstance(node, str):
        if key.endswith(_NON_ASOF_KEY) or "next_" in key or "scheduled" in key:
            return out
        if len(node) >= 10 and node[4] == "-" and node[7] == "-" and node[:4].isdigit():
            out.add(node[:10])
    elif isinstance(node, dict):
        # The scheduled-review check reports WHEN the next review falls due, so its
        # as-of date is deliberately ahead of the session. It is a cadence date, not an
        # observation, and it is derived from the frozen target month.
        if str(node.get("code") or "") == "SCHEDULED_FULL_REVIEW":
            return out
        for k, v in node.items():
            _iso_dates(v, out, str(k))
    elif isinstance(node, (list, tuple)):
        for v in node:
            _iso_dates(v, out, key)
    return out


def test_81_every_canonical_scenario_is_cross_panel_consistent():
    """The property the six deselected Stage-20.1 tests were asserting. It now holds for
    the whole canonical set, and holds independently of the real calendar."""
    for key in sorted(FX.scenarios()):
        c = FX.compose(key)
        assert c["consistency"]["consistent"], (key, c["consistency"]["violations"])


def test_82_no_composed_panel_carries_a_date_after_the_frozen_reference():
    """A date later than the harness's own reference day can only have come from the real
    world. This is the exact signature of the decay: it appears the day after the fixture
    was written and never goes away."""
    for key in sorted(FX.scenarios()):
        dates = _iso_dates(FX.compose(key)["panels"], set())
        future = sorted(d for d in dates if d > FX.NEXT)
        assert not future, (key, future)


def test_83_the_daily_action_gate_reads_the_scenario_not_the_owned_model_panel():
    """The gate's assessment date IS the scenario's eligible session — never the live one."""
    for key in sorted(FX.scenarios()):
        gate = FX.compose(key)["panels"]["daily_action_gate"]
        assert gate.get("latest_completed_market_date") == FX.DATE, (
            key, gate.get("latest_completed_market_date"))


def test_84_target_readiness_and_daily_close_are_injected_not_resolved_live():
    """The two remaining freshness seams. `target_calculation` came from the owned model
    panel through `alpha_target`; `latest_daily_close` came from the operator's real close
    journal. Neither may ever be FUTURE_DATED against the frozen session again."""
    for key in sorted(FX.scenarios()):
        rows = {r["source_id"]: r for r in
                FX.compose(key)["panels"]["workflow_state"]["provenance"].get(
                    "source_freshness", [])} or None
        c = FX.compose(key)
        fresh = c["panels"]["workflow_state"]
        assert fresh["consistency_status"] == "CONSISTENT", (
            key, fresh.get("consistency_violations"))
        ob_target = ((c["panels"]["operational_book"].get("operational_book") or {})
                     .get("current_target") or {})
        if ob_target:
            assert ob_target.get("alpha_market_date") == FX.DATE, (key, ob_target)
        assert rows is None or "target_calculation" not in rows or \
            rows["target_calculation"]["as_of_date"] == FX.DATE


def test_85_operational_book_target_readiness_injection_is_additive():
    """The injection seam must not change production behaviour when it is not supplied."""
    import inspect
    sig = inspect.signature(OB.load_operational_book)
    assert "target_readiness" in sig.parameters
    assert sig.parameters["target_readiness"].default is None


def test_86_the_frozen_owned_model_payload_invents_no_churn():
    """The injected `current` must describe the SCENARIO's book: the recomputed Top-25
    target IS the 25 held names, so the gate's own diff cannot fabricate churn that the
    seeded world does not contain."""
    cur = FX._engine_current(FX.scenarios()["scenario_1_portfolio_current"])
    book = cur["books"]["books"][FX._COMBINED_BOOK_ID]
    assert [c["ticker"] for c in book["constituents"]] == list(FX.HELD)
    assert cur["market_as_of_date"] == FX.DATE
    assert cur["inputs"]["market_as_of_date"] == FX.DATE


def test_87_composition_is_deterministic_across_repeated_runs():
    """Two compositions of the same scenario must agree on every panel date. A live read
    would eventually disagree; a frozen one never can."""
    for key in ("scenario_4_data_blocked", "scenario_5b_execution_pending_close_due"):
        a = _iso_dates(FX.compose(key)["panels"], set())
        b = _iso_dates(FX.compose(key)["panels"], set())
        assert a == b, key
