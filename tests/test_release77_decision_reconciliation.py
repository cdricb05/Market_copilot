r"""Release 77 - the three owner contradictions that all read CONSISTENT.

For eligible session 2026-09-25 (NAV 98,788.22, proposal
``reap_2026-09-25_alpha_paper_book_1_eaee484fa4a0``) three surfaces disagreed and
``workflow_state.consistency_status`` reported CONSISTENT with zero violations:

1. The opportunity-cost owner ruled ELEVEN holdings outside the retention rules;
   ``/v1/operations/portfolio-reassessment`` published ``EXIT: 0``.
2. The proposal's adjustment log recorded 15 deferred trades; its deferral ledger
   recorded 0, and the proposal review reported nothing withheld.
3. Cross-asset risk reported liquidity UNAVAILABLE for all 25 holdings; the
   opportunity-cost owner reported all 25 LIQUID.

None was an arithmetic error. Each was a MISSING PROJECTION of a fact an owner
already held. These tests pin the projections and the invariant that now fails
when two owners answer one question differently.

Hermetic: pure functions over inline fixtures. No store, no network, no clock.
"""
from __future__ import annotations

from paper_trader.api import cross_asset_risk as xapi
from paper_trader.api import portfolio_reassessment as prm
from paper_trader.api import workflow_state as ws
from paper_trader.engine import proposal_decision_review as pdr


def _holding(tk, published, source, withheld, codes=()):
    return {"ticker": tk, "recommendation": published,
            "source_recommendation": source,
            "action_withheld": withheld,
            "withheld_reason_codes": list(codes),
            "churn_protected": withheld}


def _sept25_reassessment():
    """The 2026-09-25 shape: 15 actions withheld under CHURN_COOLDOWN_ACTIVE,
    of which 11 were retention EXIT verdicts."""
    rows = []
    for i in range(11):
        rows.append(_holding("EX%d" % i, "HOLD", "EXIT", True,
                             ["CHURN_COOLDOWN_ACTIVE"]))
    for i in range(3):
        rows.append(_holding("RP%d" % i, "HOLD", "REPLACE", True,
                             ["CHURN_COOLDOWN_ACTIVE"]))
    rows.append(_holding("RD0", "HOLD", "REDUCE", True,
                         ["CHURN_COOLDOWN_ACTIVE"]))
    rows.append(_holding("RD1", "REDUCE", "REDUCE", False))
    for i in range(9):
        rows.append(_holding("HD%d" % i, "HOLD", "HOLD", False))
    return {"holding_assessments": rows}


# ----------------------------------------------------------------- defect 1 --
def test_the_reassessment_publishes_the_retention_exits_churn_withheld():
    rec = prm.withholding_reconciliation(_sept25_reassessment())
    assert rec["state"] == prm.WITHHOLDING_APPLIED
    assert rec["holdings_evaluated"] == 25
    assert rec["published_recommendation_counts"]["EXIT"] == 0
    assert rec["retention_verdict_counts"]["EXIT"] == 11
    assert rec["retention_exits_withheld"] == 11
    assert rec["withheld_action_count"] == 15
    assert rec["withheld_reason_counts"] == {"CHURN_COOLDOWN_ACTIVE": 15}
    assert rec["counts_reconcile"] is True
    # EXIT 0 may never be published without the ledger that explains it.
    assert "11 retention EXIT" in rec["statement"]


def test_the_published_counts_carry_their_basis_and_their_scope():
    rec = prm.withholding_reconciliation(_sept25_reassessment())
    assert rec["published_counts_basis"] == prm.PUBLISHED_COUNTS_BASIS
    assert rec["source_counts_basis"] == prm.SOURCE_COUNTS_BASIS
    # The kernel's own counts also span addition candidates; these do not. Saying
    # so is the difference between two views and two contradicting numbers.
    assert rec["counts_scope"] == prm.COUNTS_SCOPE_HELD


def test_no_holding_rows_is_not_a_clean_bill_of_health():
    """The workflow composer passes a decision summary with no per-holding rows.
    That must read NOT_EVALUATED, never "nothing was withheld"."""
    rec = prm.withholding_reconciliation({"decision": {}})
    assert rec["state"] == prm.WITHHOLDING_NOT_EVALUATED
    assert rec["retention_exits_withheld"] is None
    assert rec["withheld_action_count"] is None
    assert rec["counts_reconcile"] is None
    assert "not a statement" in rec["statement"]


def test_a_book_with_nothing_withheld_says_so_positively():
    clean = {"holding_assessments": [_holding("A", "HOLD", "HOLD", False)]}
    rec = prm.withholding_reconciliation(clean)
    assert rec["state"] == prm.WITHHOLDING_NONE
    assert rec["withheld_action_count"] == 0
    assert rec["retention_exits_withheld"] == 0


# ----------------------------------------------------------------- defect 2 --
def _sept25_proposal():
    """One repair round: the log keeps round 0's 15-trade deferral note, the
    ledger is replaced by the repaired round, which deferred none."""
    return {"constraint_reoptimization": {
        "risk_contribution_repair_rounds": [{"applied": True}],
        "constraint_adjustments": [
            {"constraint": "RISK_CONTRIBUTION_CAP", "action": "CAPPED_TO_LIMIT",
             "ticker": "ALAB"},
            {"constraint": "TURNOVER_BUDGET",
             "action": pdr._ADJ_TRADES_DEFERRED, "deferred_trades": 15},
        ],
        "turnover": {"deferred_trade_count": 0, "deferred_trades": [],
                     "accepted_trade_count": 24, "budget_binds": True}}}


def test_the_review_names_the_superseded_deferral_note():
    out = pdr.superseded_deferral_notes(_sept25_proposal())
    assert out["final_ledger_deferred_trade_count"] == 0
    assert out["deferral_notes_in_adjustment_log"] == 1
    assert out["superseded_deferred_trade_counts"] == [15]
    assert out["repair_rounds_applied"] == 1
    assert out["reconciled"] is False
    assert out["adjustment_log_is_cumulative_across_rounds"] is True
    assert out["final_ledger_describes_last_round_only"] is True


def test_withheld_changes_carries_the_reconciliation_with_its_zero():
    out = pdr.withheld_changes(_sept25_proposal())
    assert out["deferred_trade_count"] == 0
    # A bare zero was indistinguishable from "the log says fifteen".
    notes = out["superseded_deferral_notes"]
    assert notes["superseded_deferred_trade_counts"] == [15]


def test_an_agreeing_proposal_reconciles():
    prop = {"constraint_reoptimization": {
        "risk_contribution_repair_rounds": [],
        "constraint_adjustments": [
            {"constraint": "TURNOVER_BUDGET",
             "action": pdr._ADJ_TRADES_DEFERRED, "deferred_trades": 3}],
        "turnover": {"deferred_trade_count": 3,
                     "deferred_trades": [{"ticker": "A"}, {"ticker": "B"},
                                         {"ticker": "C"}]}}}
    out = pdr.superseded_deferral_notes(prop)
    assert out["reconciled"] is True
    assert out["superseded_notes"] == []


# ----------------------------------------------------------------- defect 3 --
def test_liquidity_reads_the_owned_panel_when_scoring_is_absent():
    """The GET route passes only portfolio_state, so ``scoring`` is None. Every
    other input on that path has a loader fallback; this one had none, and all 25
    holdings read UNAVAILABLE off an empty adv map."""
    positions = [{"instrument_id": "AMD", "notional_usd": 4000.0,
                  "instrument_type": None}]
    # 20 owned bars: ``trailing_median_dollar_volume`` needs at least max(1, k//2)
    # valid bars in the window or it returns None by design, which is the guard
    # that stops liquidity being invented from a thin panel.
    dates = ["2026-09-%02d" % d for d in range(6, 26)]
    panel = {"series": {"AMD": {"dates": dates,
                                "dollar_vol": [1.0e9] * len(dates)}}}
    liq = xapi._liquidity(positions, None,
                          {"liquidity_participation_rate": 0.10},
                          price_panel=panel, as_of="2026-09-25")
    assert liq["AMD"]["days_to_liquidate"] is not None
    assert liq["AMD"]["dollar_volume_source"] == xapi.ADV_SOURCE_OWNED_PANEL


def test_a_name_the_panel_does_not_carry_stays_unavailable():
    """The fix reads an input that was there; it never invents one."""
    positions = [{"instrument_id": "ZZZZ", "notional_usd": 4000.0,
                  "instrument_type": None}]
    liq = xapi._liquidity(positions, None,
                          {"liquidity_participation_rate": 0.10},
                          price_panel={"series": {}}, as_of="2026-09-25")
    assert liq["ZZZZ"]["days_to_liquidate"] is None
    assert liq["ZZZZ"]["dollar_volume_source"] == xapi.ADV_SOURCE_NONE


def test_scoring_still_wins_when_it_carries_the_name():
    positions = [{"instrument_id": "AMD", "notional_usd": 4000.0,
                  "instrument_type": None}]
    scoring = {"rankings": [{"ticker": "AMD", "adv_dollar": 5.0e8}]}
    liq = xapi._liquidity(positions, scoring,
                          {"liquidity_participation_rate": 0.10},
                          price_panel={"series": {}}, as_of="2026-09-25")
    assert liq["AMD"]["dollar_volume_source"] == xapi.ADV_SOURCE_SCORING


# ------------------------------------------------------------- the invariant --
def test_a_retention_exit_that_no_owner_accounts_for_is_a_violation():
    v = ws.check_decision_reconciliation(
        hoc_exit_count=11, proposal_exit_count=0,
        mandatory_exit_obligation="NONE", withheld_retention_exits=0,
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states=None, cross_asset_liquidity_states=None)
    assert [x["code"] for x in v] == [ws.V_RETENTION_EXIT_UNACCOUNTED]
    assert v[0]["retention_exits_ruled"] == 11


def test_the_sept25_book_is_reconciled_because_the_proposal_exits_all_eleven():
    """The real 2026-09-25 proposal carries all 11 as mandatory obligations with
    ceiling 0.0, so there is nothing outstanding and no violation."""
    v = ws.check_decision_reconciliation(
        hoc_exit_count=11, proposal_exit_count=11,
        mandatory_exit_obligation="NONE", withheld_retention_exits=11,
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states=None, cross_asset_liquidity_states=None)
    assert v == []


def test_a_named_withholding_or_a_declared_obligation_accounts_for_the_exits():
    for kw in ({"mandatory_exit_obligation": "ELEVEN_EXITS_OUTSTANDING",
                "withheld_retention_exits": 0},
               {"mandatory_exit_obligation": "NONE",
                "withheld_retention_exits": 11}):
        v = ws.check_decision_reconciliation(
            hoc_exit_count=11, proposal_exit_count=0,
            proposal_deferred_trade_count=None,
            adjustment_log_deferral_counts=None,
            hoc_liquidity_states=None, cross_asset_liquidity_states=None, **kw)
        assert v == [], kw


def test_the_deferral_contradiction_is_a_violation():
    v = ws.check_decision_reconciliation(
        hoc_exit_count=None, proposal_exit_count=None,
        mandatory_exit_obligation=None, withheld_retention_exits=None,
        proposal_deferred_trade_count=0, adjustment_log_deferral_counts=[15],
        hoc_liquidity_states=None, cross_asset_liquidity_states=None)
    assert [x["code"] for x in v] == [ws.V_DEFERRAL_LEDGER_CONTRADICTED]


def test_two_liquidity_owners_disagreeing_on_held_names_is_a_violation():
    held = ["A", "B", "C"]
    v = ws.check_decision_reconciliation(
        hoc_exit_count=None, proposal_exit_count=None,
        mandatory_exit_obligation=None, withheld_retention_exits=None,
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states={t: "LIQUID" for t in held},
        cross_asset_liquidity_states={t: "UNAVAILABLE" for t in held})
    assert [x["code"] for x in v] == [ws.V_LIQUIDITY_OWNERS_DISAGREE]
    assert v[0]["disputed_name_count"] == 3


def test_agreeing_liquidity_owners_raise_nothing():
    held = ["A", "B"]
    v = ws.check_decision_reconciliation(
        hoc_exit_count=None, proposal_exit_count=None,
        mandatory_exit_obligation=None, withheld_retention_exits=None,
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states={t: "LIQUID" for t in held},
        cross_asset_liquidity_states={t: "LIQUID" for t in held})
    assert v == []


def test_an_unreadable_input_is_skipped_never_scored_as_agreement():
    """Do not silently reinterpret a missing input as PASS."""
    v = ws.check_decision_reconciliation(
        hoc_exit_count=None, proposal_exit_count=None,
        mandatory_exit_obligation=None, withheld_retention_exits=None,
        proposal_deferred_trade_count=None, adjustment_log_deferral_counts=None,
        hoc_liquidity_states=None, cross_asset_liquidity_states=None)
    assert v == []
    assert ws._coerce_int(None) is None
    assert ws._coerce_int("nope") is None
    assert ws._coerce_int(True) is None      # a bool is not a count
    assert ws._coerce_int("11") == 11
