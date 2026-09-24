"""R63 — GOVERNED TARGET SELECTION & MANDATORY REPAIR ALIGNMENT.

Three invariants, each proved against the owners that already exist:

  1. MANDATORY MEANS MANDATORY. The opportunity-cost owner's BROKEN verdict is
     carried into the reallocation kernel's EXISTING mandatory tier, so a
     governed retention or eligibility failure is taken BEFORE any discretionary
     trade and the turnover budget can never defer it.
  2. A FULL TARGET IS NOT REVIEWABLE while it knowingly leaves an obligation
     open, and it is not SELECTABLE either. Fail closed.
  3. A GOVERNED SELECTION sits between Review and Approve, binds every identity
     that makes the choice meaningful, and is refused when any of them moved -
     including the SESSION (R63 addendum: a persisted proposal does not stay
     actionable merely by continuing to exist).

Nothing here approves, orders, fills or executes.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import proposal_decision_review as pdrev
from paper_trader.engine import constrained_reallocation as cr
from paper_trader.engine import holding_opportunity_cost as hoc
from paper_trader.engine import proposal_decision_review as pdr

ROOT = Path(__file__).resolve().parent.parent

SESSION = "2026-09-18"
LATER_SESSION = "2026-09-21"
BOOK = "alpha_paper_book_1"


# =========================================================================== #
# helpers — a hermetic, asset-agnostic world
# =========================================================================== #
def _review_row(ticker, *, det=hoc.DET_STABLE, codes=(), liq=hoc.LIQ_LIQUID,
                weight=0.10, rec=hoc.REC_HOLD, asset_class=None, sleeve=None):
    return {"ticker": ticker, "instrument_id": ticker,
            "deterioration_state": det, "deterioration_reason_codes": list(codes),
            "liquidity_state": liq, "current_weight": weight,
            "recommendation": rec, "reason_codes": [], "current_rank": 7,
            "asset_class": asset_class, "sleeve_id": sleeve,
            "estimated_days_to_liquidate": 1.0}


def _candidates(scores, *, adv=5.0e8):
    """One candidate row per ticker, each in a sector of its own so the sector cap
    never binds and the ONLY thing under test is the mandatory/discretionary split."""
    return [{"ticker": t, "sector": "S_%s" % t, "adv_dollar": adv,
             "score": s, "rank": i + 1} for i, (t, s) in enumerate(sorted(scores.items()))]


# =========================================================================== #
# 1. THE MANDATORY-REPAIR CONTRACT — one owner, one interpretation
# =========================================================================== #
class TestMandatoryRepairContract:

    def test_01_broken_retention_becomes_an_obligation_with_required_exit(self):
        rows = [_review_row("LH", det=hoc.DET_BROKEN,
                            codes=["FELL_BELOW_EXIT_BUFFER"], weight=0.05)]
        obl = hoc.governance_repair_obligations(holding_reviews=rows,
                                                current_weights={"LH": 0.05},
                                                evidence_hash="hoc_hash_1")
        assert len(obl) == 1
        o = obl[0]
        assert o["reason_code"] == hoc.OBLIGATION_REASON_RETENTION
        assert o["required_action"] == hoc.REQUIRED_ACTION_EXIT
        assert o["required_exit"] is True and o["max_valid_weight"] == 0.0
        assert o["source_owner"] == hoc.CALCULATION_OWNER
        assert o["evidence_hash"] == "hoc_hash_1"
        # every declared field is present
        for f in hoc.MANDATORY_REPAIR_OBLIGATION_FIELDS:
            assert f in o, f

    def test_02_universe_ineligibility_is_a_distinct_reason(self):
        rows = [_review_row("VLO", det=hoc.DET_BROKEN, codes=["NOT_ELIGIBLE"])]
        o = hoc.governance_repair_obligations(holding_reviews=rows)[0]
        assert o["reason_code"] == hoc.OBLIGATION_REASON_UNIVERSE
        assert o["required_action"] == hoc.REQUIRED_ACTION_EXIT

    def test_03_a_healthy_holding_raises_no_obligation(self):
        rows = [_review_row("AAA"), _review_row("BBB", det=hoc.DET_DETERIORATING)]
        assert hoc.governance_repair_obligations(holding_reviews=rows) == []

    def test_04_illiquid_is_named_but_never_promoted_to_an_exit(self):
        rows = [_review_row("CCC", liq=hoc.LIQ_ILLIQUID)]
        o = hoc.governance_repair_obligations(holding_reviews=rows)[0]
        assert o["required_action"] == hoc.REQUIRED_ACTION_NOT_SIZEABLE
        assert o["required_exit"] is False
        assert o["max_valid_weight"] is None, "unsized must never read as exit"
        assert hoc.forced_weight_ceilings([o]) == {}

    def test_05_the_review_kernel_delegates_and_does_not_re_decide(self):
        """R62's classifier is now an adapter over the ONE owner."""
        row = _review_row("LH", det=hoc.DET_BROKEN, codes=["FELL_BELOW_EXIT_BUFFER"])
        tier, reason, code, detail = pdr._retention_obligation(row)
        owner = hoc.retention_obligation(row)
        assert tier == owner["obligation_type"] == pdr.TIER_GOVERNANCE
        assert reason == owner["reason_code"] == pdr.REASON_RETENTION
        assert code == owner["constraint_code"] == "RETENTION_EXIT_BUFFER"
        assert detail == owner["detail"]

    def test_06_vocabularies_are_re_exported_never_forked(self):
        assert pdr.TIER_VOCAB == tuple(hoc.OBLIGATION_TIER_VOCAB)
        assert pdr.REPAIR_ACTION_VOCAB == tuple(hoc.REQUIRED_ACTION_VOCAB)
        assert pdr.REASON_RETENTION is hoc.OBLIGATION_REASON_RETENTION
        assert pdr.REASON_UNIVERSE is hoc.OBLIGATION_REASON_UNIVERSE

    def test_07_openness_is_judged_on_the_resulting_book_not_on_trades(self):
        """A breach closed by COMPOSITION is closed. R62 finding #2, kept."""
        obl = hoc.governance_repair_obligations(
            holding_reviews=[_review_row("LH", det=hoc.DET_BROKEN,
                                         codes=["FELL_BELOW_EXIT_BUFFER"])])
        assert hoc.obligations_open_against(obligations=obl, weights={"LH": 0.05})
        assert hoc.obligations_open_against(obligations=obl, weights={}) == []
        assert hoc.obligations_open_against(obligations=obl,
                                            weights={"AAA": 0.9}) == []

    def test_08_contract_is_asset_agnostic(self):
        """An owner that rules on a future/FX/rates sleeve publishes identically."""
        rows = [_review_row("ESZ6", det=hoc.DET_BROKEN, codes=["NOT_ELIGIBLE"],
                            asset_class="FUTURES", sleeve="ts_trend"),
                _review_row("EURUSD", det=hoc.DET_BROKEN,
                            codes=["FELL_BELOW_EXIT_BUFFER"],
                            asset_class="FX", sleeve="fx_carry")]
        obl = hoc.obligations_from_ruled_rows(rows=rows,
                                              current_weights={"ESZ6": 0.2,
                                                               "EURUSD": 0.1},
                                              source_owner="engine.some_other_owner")
        assert {o["instrument_id"] for o in obl} == {"ESZ6", "EURUSD"}
        assert {o["asset_class"] for o in obl} == {"FUTURES", "FX"}
        assert all(o["source_owner"] == "engine.some_other_owner" for o in obl)
        assert all(o["required_exit"] for o in obl)
        assert hoc.required_exit_instruments(obl) == ["ESZ6", "EURUSD"]


# =========================================================================== #
# 2. MANDATORY PRECEDENCE — the turnover budget may not defer a repair
# =========================================================================== #
class TestMandatoryPrecedence:

    #: Every held name sits UNDER the 0.10 name cap and in its own sector, so no
    #: capacity rule can make any of them mandatory. LH and VLO also carry HIGH
    #: scores, so selling them looks bad on the kernel's own density ordering and
    #: a tight budget ranks their exits last. That is precisely the 2026-09-18
    #: shape: a holding the retention rules no longer admit, which the budget is
    #: happy to defer because nothing told the kernel it was mandatory.
    _SCORES = {"LH": 0.95, "VLO": 0.95, "NEW1": 0.99, "NEW2": 0.99,
               **{"A%d" % i: 0.50 for i in range(1, 10)}}
    _CURRENT = {"LH": 0.08, "VLO": 0.08, **{"A%d" % i: 0.08 for i in range(1, 10)}}
    _IDEAL = {"NEW1": 0.08, "NEW2": 0.08, **{"A%d" % i: 0.08 for i in range(1, 10)}}

    def _solve(self, *, obligations, budget=0.20):
        pol = dict(cr.default_policy())
        pol["max_one_way_turnover"] = budget
        return cr.solve_feasible_target(
            current_weight=dict(self._CURRENT), ideal_weight=dict(self._IDEAL),
            candidates=_candidates(self._SCORES),
            nav=1.0e7, policy=pol, mandatory_obligations=obligations)

    def _obligations(self, *tickers):
        return hoc.governance_repair_obligations(
            holding_reviews=[_review_row(t, det=hoc.DET_BROKEN,
                                         codes=["FELL_BELOW_EXIT_BUFFER"],
                                         weight=0.08) for t in tickers],
            current_weights={t: 0.08 for t in tickers})

    def test_10_without_obligations_the_budget_defers_the_retention_exit(self):
        """The PRE-R63 behaviour, pinned so the change is visible and real."""
        s = self._solve(obligations=[], budget=0.05)
        assert s["governance_mandatory_exits"] == []
        assert s["turnover"]["mandatory_turnover"] == 0.0, (
            "nothing here is mandatory by CAPACITY - that is the whole point")
        # LH/VLO still have capacity, so their exits are discretionary, rank last
        # on density, and the budget defers them.
        deferred = {d["ticker"] for d in s["turnover"]["deferred_trades"]}
        assert {"LH", "VLO"} & deferred, deferred
        assert s["best_feasible_target"].get("LH", 0.0) > 0.0
        assert s["best_feasible_target"].get("VLO", 0.0) > 0.0

    def test_11_an_obligation_makes_the_exit_mandatory_and_it_is_taken(self):
        """THE fix: same book, same tight budget, but the owner's ruling travels."""
        obl = self._obligations("LH", "VLO")
        s = self._solve(obligations=obl, budget=0.05)
        assert set(s["governance_mandatory_exits"]) == {"LH", "VLO"}
        assert s["capacity_mandatory_exits"] == [], (
            "the kernel must not take credit for the owner's decision")
        assert s["best_feasible_target"].get("LH", 0.0) == 0.0
        assert s["best_feasible_target"].get("VLO", 0.0) == 0.0
        assert hoc.obligations_open_against(
            obligations=obl, weights=s["best_feasible_target"]) == []
        assert s["obligation_ceilings_applied"] == {"LH": 0.0, "VLO": 0.0}

    def test_12_mandatory_legs_precede_discretionary_ones(self):
        obl = self._obligations("LH")
        # A budget that BINDS (the full ideal needs 0.16 one-way) but still leaves
        # room after the repair, so the ordering itself is what is under test.
        s = self._solve(obligations=obl, budget=0.10)
        tb = s["turnover"]
        assert tb["budget_binds"] is True
        acc = tb["accepted_trades"]
        lh = [a for a in acc if a["ticker"] == "LH"]
        assert lh, "the mandatory exit must be accepted, never deferred"
        assert lh[0]["mandatory"] is True
        assert lh[0]["mandatory_basis"] == "OWNER_RULED_REPAIR_OBLIGATION"
        assert tb["mandatory_turnover"] > 0.0
        assert tb["governance_mandatory_trades"] == ["LH"]
        # the budget is spent on the repair FIRST; discretionary trades use only
        # what is left, and none of them can displace the repair.
        assert tb["mandatory_turnover"] <= tb["normal_turnover_budget"]
        assert all(d["ticker"] != "LH" for d in tb["deferred_trades"])

    def test_13_mandatory_turnover_may_exceed_the_budget_explicitly(self):
        obl = self._obligations("LH", "VLO")
        s = self._solve(obligations=obl, budget=0.01)
        tb = s["turnover"]
        assert tb["turnover_budget_subordinated_to_mandatory_repair"] is True
        assert tb["budget_subordinated_to_mandatory_constraints"] is True
        assert tb["mandatory_turnover"] > tb["normal_turnover_budget"]
        assert tb["excess_required_by_mandatory_repair"] > 0.0
        # the repairs are KEPT and the discretionary trades are the ones deferred
        assert s["best_feasible_target"].get("LH", 0.0) == 0.0
        assert s["best_feasible_target"].get("VLO", 0.0) == 0.0

    def test_14_an_unsized_obligation_is_never_silently_exited(self):
        obl = hoc.governance_repair_obligations(
            holding_reviews=[_review_row("LH", liq=hoc.LIQ_ILLIQUID, weight=0.08)],
            current_weights={"LH": 0.08})
        s = self._solve(obligations=obl)
        assert s["governance_mandatory_exits"] == []
        assert s["obligations_unsized_by_owner"] == ["LH"]
        assert s["obligation_ceilings_applied"] == {}

    def test_15_the_kernel_carries_the_owner_and_never_invents_one(self):
        obl = self._obligations("LH")
        s = self._solve(obligations=obl)
        assert s["mandatory_obligation_owners"] == [hoc.CALCULATION_OWNER]
        assert s["mandatory_obligations_supplied"] == 1


# =========================================================================== #
# 3. SELECTABILITY — the backend decides, and it fails closed
# =========================================================================== #
def _fake_states(*, full_open):
    def st(name, **kw):
        base = {"positions": 20, "changes": 3, "one_way_turnover": 0.1,
                "estimated_cost": 10.0, "score": 0.9, "cash_weight": 0.05,
                "concentration": 0.03, "largest_position": 0.06,
                "score_improvement_net_of_cost": 0.06,
                "portfolio_volatility": 0.11,
                "portfolio_volatility_capital_basis": 0.11,
                "constraint_status": {"obligations_remaining": []}}
        base.update(kw)
        return base
    return {
        pdr.STATE_CURRENT: st("CURRENT", changes=0),
        pdr.STATE_MINIMUM_REPAIR: st("MINIMUM_REPAIR"),
        pdr.STATE_FULL_TARGET: st("FULL_TARGET"),
    }


class TestSelectability:

    def _opts(self, *, full_open=(), repair_open=(), read_state="READY",
              verdict=pdr.VERDICT_MINIMAL_REPAIR_PREFERRED, measured=True):
        states = _fake_states(full_open=full_open)
        states[pdr.STATE_MINIMUM_REPAIR]["constraint_status"] = {
            "obligations_remaining": list(repair_open)}
        margins = {"full_target_vs_minimum_repair": {
            "obligations_left_open_by_full_target": list(full_open),
            "clears_incremental_hurdle": False}}
        repair = {"risk_measurement_state": (pdr.RISK_MEASURED if measured
                                             else "NOT_MEASURED"),
                  "adjustments": [{"ticker": "X"}]}
        return pdr.target_selection_options(
            states=states, verdict={"verdict": verdict}, margins=margins,
            repair=repair, read_state=read_state)

    def test_20_full_target_with_open_obligations_is_not_selectable(self):
        sel = self._opts(full_open=[{"ticker": "LH", "constraint_code": "RETENTION_EXIT_BUFFER"},
                                    {"ticker": "VLO", "constraint_code": "ELIGIBLE_UNIVERSE"}])
        full = [o for o in sel["options"] if o["target"] == "FULL_TARGET"][0]
        assert full["selectable"] is False
        assert pdr.SELECT_BLOCK_OBLIGATIONS_OPEN in full["blocker_codes"]
        assert full["blockers"][0]["instruments"] == ["LH", "VLO"]
        assert "LH, VLO" in full["blockers"][0]["detail"]
        assert sel["full_target_reviewable"] is False
        assert "FULL_TARGET" not in sel["selectable_targets"]

    def test_21_current_and_repair_stay_selectable(self):
        sel = self._opts(full_open=[{"ticker": "LH"}])
        assert sel["selectable_targets"] == ["CURRENT", "MINIMUM_REPAIR"]

    def test_22_an_unverified_repair_is_not_selectable(self):
        sel = self._opts(repair_open=[{"ticker": "ZZZ"}])
        rep = [o for o in sel["options"] if o["target"] == "MINIMUM_REPAIR"][0]
        assert rep["selectable"] is False
        assert pdr.SELECT_BLOCK_REPAIR_UNVERIFIED in rep["blocker_codes"]

    def test_23_an_unmeasured_repair_is_not_selectable(self):
        sel = self._opts(measured=False)
        rep = [o for o in sel["options"] if o["target"] == "MINIMUM_REPAIR"][0]
        assert pdr.SELECT_BLOCK_REPAIR_UNMEASURED in rep["blocker_codes"]

    def test_24_a_non_reviewable_proposal_blocks_every_target(self):
        rs = sorted(pdr.NON_REVIEWABLE_READ_STATES)[0]
        sel = self._opts(read_state=rs)
        assert sel["selectable_targets"] == []
        assert all(pdr.SELECT_BLOCK_NOT_REVIEWABLE in o["blocker_codes"]
                   for o in sel["options"])

    def test_25_recommended_is_identified_but_never_auto_selected(self):
        sel = self._opts()
        assert sel["recommended_target"] == "MINIMUM_REPAIR"
        assert sel["auto_selected"] is False
        rec = [o for o in sel["options"] if o["recommended"]]
        assert [o["target"] for o in rec] == ["MINIMUM_REPAIR"]
        assert sel["selection_is_approval"] is False
        assert sel["selection_creates_order_plan"] is False
        assert sel["selection_creates_orders"] is False

    def test_26_every_option_carries_the_facts_the_operator_compares(self):
        sel = self._opts()
        for o in sel["options"]:
            for f in ("positions", "one_way_turnover", "estimated_cost",
                      "score_improvement_net_of_cost", "concentration",
                      "cash_weight", "mandatory_obligations_remaining"):
                assert f in o, f

    def test_27_verdict_cannot_be_full_target_reviewable_with_open_obligations(self):
        """THE invariant, at the verdict ladder itself."""
        states = _fake_states(full_open=True)
        margins = {"full_target_vs_minimum_repair": {
            "obligations_left_open_by_full_target": [
                {"ticker": "LH", "constraint_code": "RETENTION_EXIT_BUFFER"}],
            "clears_incremental_hurdle": True}}  # economics WOULD clear
        v = pdr.decide(read_state="READY", proposal={}, obligations=[{"ticker": "LH"}],
                       repair={"risk_measurement_state": pdr.RISK_MEASURED,
                               "adjustments": []},
                       states=states, margins=margins)
        assert v["verdict"] != pdr.VERDICT_FULL_TARGET_REVIEWABLE
        assert v["verdict"] == pdr.VERDICT_MINIMAL_REPAIR_PREFERRED
        assert "FULL_TARGET_NOT_REVIEWABLE_OBLIGATIONS_OPEN" in v["reason_codes"]
        assert "UNRESOLVED_RETENTION_EXIT_BUFFER" in v["reason_codes"]
        assert v["inputs"]["full_target_reviewable"] is False

    def test_28_with_no_open_obligations_the_ladder_is_unchanged(self):
        states = _fake_states(full_open=False)
        margins = {"full_target_vs_minimum_repair": {
            "obligations_left_open_by_full_target": [],
            "clears_incremental_hurdle": True}}
        v = pdr.decide(read_state="READY", proposal={}, obligations=[{"ticker": "LH"}],
                       repair={"risk_measurement_state": pdr.RISK_MEASURED,
                               "adjustments": []},
                       states=states, margins=margins)
        assert v["verdict"] == pdr.VERDICT_FULL_TARGET_REVIEWABLE


# =========================================================================== #
# 4. DECISION FRESHNESS (R63 addendum) — pure, and fails closed
# =========================================================================== #
class TestDecisionFreshness:

    def test_30_same_session_is_current_and_actionable(self):
        f = pdec.decision_freshness(bound_session=SESSION, latest_session=SESSION)
        assert f["state"] == pdec.FRESHNESS_CURRENT
        assert f["actionable"] is True
        assert f["target_selection_allowed"] is True
        assert f["approval_allowed"] is True
        assert f["order_plan_confirmation_allowed"] is True
        assert f["next_required_action"] is None

    def test_31_an_older_bound_session_is_stale_and_blocks_everything(self):
        f = pdec.decision_freshness(bound_session=SESSION,
                                    latest_session=LATER_SESSION)
        assert f["state"] == pdec.FRESHNESS_STALE
        assert f["actionable"] is False
        assert f["target_selection_allowed"] is False
        assert f["approval_allowed"] is False
        assert f["order_plan_confirmation_allowed"] is False
        assert f["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE
        # the historical record is untouched and stays readable
        assert f["readable_as_history"] is True and f["immutable"] is True

    @pytest.mark.parametrize("bound,latest", [
        (None, LATER_SESSION), (SESSION, None), (None, None),
        (LATER_SESSION, SESSION),  # ahead of the world == inconsistent
    ])
    def test_32_unknown_or_inconsistent_fails_closed(self, bound, latest):
        f = pdec.decision_freshness(bound_session=bound, latest_session=latest)
        assert f["state"] == pdec.FRESHNESS_UNVERIFIABLE
        assert f["actionable"] is False
        assert f["approval_allowed"] is False

    def test_33_no_second_calendar_or_session_owner_is_introduced(self):
        src = (ROOT / "api" / "portfolio_decision.py").read_text(
            encoding="utf-8", errors="replace")
        assert "action_session_market_date" in src, "must DELEGATE to the owner"
        assert pdec.SESSION_CALENDAR_OWNER == "engine.market_session"
        assert pdec.SESSION_AUTHORITY_OWNER == "api.workflow_state"
        # this module must not build a calendar of its own
        for forbidden in ("holidays_for_year", "is_trading_day", "non_sessions_between",
                          "walk_back_to_trading_day", "completed_sessions_after"):
            assert forbidden not in src, forbidden


# =========================================================================== #
# 5. THE GOVERNED SELECTION — idempotent, bound, and it creates nothing
# =========================================================================== #
def _envelope(*, target_blockers=None, session=SESSION, phash="P_HASH",
              rhash="R_HASH", hocs="HOC_HASH"):
    """A minimal review envelope of exactly the shape the read owner publishes."""
    blockers = target_blockers or {}

    def opt(name):
        b = blockers.get(name) or []
        return {"target": name, "label": name.replace("_", " ").title(),
                "recommended": name == "MINIMUM_REPAIR",
                "selectable": not b, "blockers": b,
                "blocker_codes": sorted({x["code"] for x in b}),
                "positions": 14, "changes": 11, "one_way_turnover": 0.2156,
                "estimated_cost": 52.99, "score": 0.9344,
                "score_improvement_net_of_cost": 0.0704,
                "portfolio_volatility": 0.155,
                "portfolio_volatility_capital_basis": 0.081,
                "concentration": 0.0202, "largest_position": 0.05,
                "cash_weight": 0.4766, "mandatory_obligations_remaining": 0,
                "obligations_remaining": [], "is_defer": name == "CURRENT"}

    return {
        "proposal_id": "reap_%s_%s_abc" % (session, BOOK),
        "proposal_hash": phash,
        "review_hash": rhash,
        "eligible_market_date": session,
        "proposal_read_state": "READY",
        "active_book": {"id": BOOK},
        "inputs": {"hoc_assessment_hash_used": hocs},
        "review": {
            "reviewed_proposal": {"proposal_hash": phash,
                                  "eligible_market_date": session,
                                  "active_book_id": BOOK,
                                  "hoc_assessment_hash": hocs,
                                  "portfolio_state_hash": "PS",
                                  "corporate_actions_hash": "CA",
                                  "universe_scoring_hash": "US"},
            "review_verdict": {"verdict": pdr.VERDICT_MINIMAL_REPAIR_PREFERRED},
            "target_selection": {
                "recommended_target": "MINIMUM_REPAIR",
                "options": [opt("CURRENT"), opt("MINIMUM_REPAIR"), opt("FULL_TARGET")],
            },
        },
    }


def _select(tmp_path, target="MINIMUM_REPAIR", **kw):
    kw.setdefault("review_envelope", _envelope())
    kw.setdefault("latest_session", SESSION)
    return pdec.record_target_selection(
        target=target, confirm=pdec.SELECTION_CONFIRM_TOKEN,
        decision_dir=str(tmp_path), **kw)


class TestGovernedSelection:

    def test_40_minimum_repair_can_be_selected(self, tmp_path):
        r = _select(tmp_path)
        assert r["status"] == pdec.TS_CREATED and r["selected"] is True
        rec = r["record"]
        assert rec["selected_target"] == "MINIMUM_REPAIR"
        assert rec["artifact_kind"] == "proposal_review_selection"
        assert rec["followed_recommendation"] is True

    def test_41_current_is_selectable_as_the_defer_path(self, tmp_path):
        r = _select(tmp_path, target="CURRENT")
        assert r["selected"] is True
        assert r["record"]["is_defer"] is True
        # a defer creates no plan and no order
        assert r["creates_order_plan"] is False and r["creates_orders"] is False

    def test_42_a_blocked_full_target_is_refused_with_its_reason(self, tmp_path):
        env = _envelope(target_blockers={"FULL_TARGET": [
            {"code": pdr.SELECT_BLOCK_OBLIGATIONS_OPEN,
             "detail": "2 mandatory repair obligation(s) remain unresolved: LH, VLO.",
             "instruments": ["LH", "VLO"]}]})
        r = _select(tmp_path, target="FULL_TARGET", review_envelope=env)
        assert r["status"] == pdec.TS_NOT_SELECTABLE
        assert r["selected"] is False and r["recorded"] is False
        assert "LH, VLO" in r["message"]
        assert not (tmp_path / "target_selections.json").exists()

    def test_43_selection_binds_proposal_review_and_hoc_hashes(self, tmp_path):
        b = _select(tmp_path)["record"]["binding"]
        assert b["proposal_hash"] == "P_HASH"
        assert b["review_hash"] == "R_HASH"
        assert b["hoc_assessment_hash"] == "HOC_HASH"
        assert b["eligible_market_date"] == SESSION
        assert b["active_book_id"] == BOOK
        assert b["selected_target"] == "MINIMUM_REPAIR"
        assert b["selected_target_hash"]

    @pytest.mark.parametrize("kw,code", [
        ({"expected_proposal_hash": "OTHER"}, "PROPOSAL_HASH_MISMATCH"),
        ({"expected_review_hash": "OTHER"}, "REVIEW_HASH_MISMATCH"),
        ({"expected_hoc_assessment_hash": "OTHER"}, "HOC_ASSESSMENT_HASH_MISMATCH"),
    ])
    def test_44_a_moved_identity_fails_closed(self, tmp_path, kw, code):
        r = _select(tmp_path, **kw)
        assert r["status"] == pdec.TS_STALE
        assert r["reason_code"] == code
        assert r["recorded"] is False
        assert not (tmp_path / "target_selections.json").exists()

    def test_45_selection_is_idempotent(self, tmp_path):
        a = _select(tmp_path)
        b = _select(tmp_path)
        assert a["status"] == pdec.TS_CREATED
        assert b["status"] == pdec.TS_REUSED and b["revised"] is False
        rows = json.loads((tmp_path / "target_selections.json").read_text(encoding="utf-8"))
        assert len(rows) == 1, "an identical selection must not duplicate the artifact"

    def test_46_a_conflicting_reselection_is_an_audited_revision(self, tmp_path):
        _select(tmp_path, target="MINIMUM_REPAIR")
        r = _select(tmp_path, target="CURRENT")
        assert r["status"] == pdec.TS_REVISED and r["revised"] is True
        rows = json.loads((tmp_path / "target_selections.json").read_text(encoding="utf-8"))
        assert len(rows) == 2, "both immutable records must be preserved"
        assert rows[1]["supersedes_selection_id"] == rows[0]["selection_id"]
        latest = pdec.load_target_selection(active_book_id=BOOK,
                                            eligible_market_date=SESSION,
                                            decision_dir=str(tmp_path))
        assert latest["selected_target"] == "CURRENT"

    def test_47_a_stale_session_refuses_every_selection(self, tmp_path):
        for target in pdec.TARGET_VOCAB:
            r = _select(tmp_path, target=target, latest_session=LATER_SESSION)
            assert r["status"] == pdec.TS_SESSION_STALE, target
            assert r["recorded"] is False and r["selected"] is False
            assert r["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE
        assert not (tmp_path / "target_selections.json").exists()

    def test_48_the_wrong_token_records_nothing(self, tmp_path):
        r = pdec.record_target_selection(
            target="MINIMUM_REPAIR", confirm="NOPE",
            review_envelope=_envelope(), decision_dir=str(tmp_path),
            latest_session=SESSION)
        assert r["status"] == pdec.TS_NOT_RECORDED and r["recorded"] is False
        assert not (tmp_path / "target_selections.json").exists()

    def test_49_selection_creates_no_order_and_is_not_an_approval(self, tmp_path):
        r = _select(tmp_path)
        for flag in ("is_an_approval", "approves_proposal", "creates_order_plan",
                     "creates_orders", "creates_fills", "executes",
                     "deploys_capital", "mutates_proposal", "decided_by_llm"):
            assert r[flag] is False, flag
        assert r["manual_approval_still_required"] is True
        # the ONLY files written are the governance selection ledger
        assert sorted(p.name for p in tmp_path.iterdir()) == [
            "target_selection_index.json", "target_selections.json"]

    def test_50_no_review_means_nothing_to_select(self, tmp_path):
        r = pdec.record_target_selection(
            target="CURRENT", confirm=pdec.SELECTION_CONFIRM_TOKEN,
            review_envelope={"review": {}}, decision_dir=str(tmp_path),
            latest_session=SESSION)
        assert r["status"] == pdec.TS_NO_REVIEW and r["recorded"] is False


# =========================================================================== #
# 6. APPROVAL CONSUMES THE SELECTION — and a stale session never approves
# =========================================================================== #
def _artifact(phash="P_HASH", session=SESSION, obligations_resolved=True):
    """A proposal artifact as ``build_proposal`` actually emits one.

    It carries the mandatory-repair contract, because every proposal the kernel
    produces does. An artifact WITHOUT it can only be a pre-R63 one, and the read
    and approval seams refuse those as UNVERIFIABLE — which is the whole subject
    of ``test_r63_live_integration``.
    """
    return {"proposal_id": "reap_%s_%s_abc" % (session, BOOK),
            "identity": {"proposal_hash": phash, "eligible_market_date": session,
                         "active_book_id": BOOK, "portfolio_state_hash": "PS",
                         "corporate_actions_hash": None, "hoc_assessment_hash": "HOC_HASH",
                         "universe_scoring_hash": "US",
                         "allocation_policy_version": "v1"},
            "input_contract": {"eligible_market_date": session, "active_book_id": BOOK,
                               "universe_input_contract_hash": "UIC"},
            "proposal": {
                "proposal_hash": phash,
                "mandatory_repair": {
                    "contract_version": hoc.MANDATORY_REPAIR_CONTRACT_VERSION,
                    "owner": hoc.CALCULATION_OWNER,
                    "obligations": [],
                    "obligation_count": 0,
                    "obligations_open_against_target": (
                        [] if obligations_resolved
                        else [{"ticker": "LH", "instrument_id": "LH"}]),
                    "obligations_open_count": 0 if obligations_resolved else 1,
                    "obligations_resolved": obligations_resolved,
                },
                "full_target_reviewable": obligations_resolved,
            }}


def _summary(phash="P_HASH"):
    return {"has_proposal": True, "proposal_hash": phash,
            "reallocation_proposal_state": "READY",
            "reallocation_proposal_withheld": False,
            "reallocation_outcome": "PROPOSAL_READY",
            "reallocation_one_way_turnover": 0.21,
            # materiality is read from the proposal's own action counts
            "reallocation_action_counts": {"EXIT": 1, "ADD": 1},
            "proposed_allocations": [
                {"ticker": "AAA", "action": "EXIT"},
                {"ticker": "BBB", "action": "ADD"}]}


class TestApprovalConsumesSelection:

    def test_60_a_stale_session_can_never_be_approved(self, tmp_path):
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=LATER_SESSION)
        assert r["status"] == pdec.PDS_SESSION_STALE
        assert r["recorded"] is False
        assert r["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE
        assert not (tmp_path / "decisions.json").exists()

    def test_61_approval_after_a_matching_selection_proceeds(self, tmp_path):
        # R69.2 — FULL_TARGET, because it is implementable from the proposal
        # artifact's own allocations and needs no frozen book. The minimum
        # repair's contract is test_61b below.
        _select(tmp_path, target="FULL_TARGET")
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["recorded"] is True and r["status"] == "CREATED"
        assert r["record"]["selected_target"] == "FULL_TARGET"

    def test_61b_a_repair_selection_without_a_frozen_book_is_refused(self, tmp_path):
        """R69.2 — a MINIMUM_REPAIR selected against a review that published no
        implementable book (every selection recorded before R69.2) is refused
        BEFORE the approval exists, and the reason is named.

        R69.1 let this approval through and refused three steps later, at the
        order plan. The operator had already been told their choice was approved.
        """
        _select(tmp_path, target="MINIMUM_REPAIR")
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["status"] == pdec.PDS_SELECTED_TARGET_NOT_IMPLEMENTABLE
        assert r["recorded"] is False
        assert r["selected_target"] == "MINIMUM_REPAIR"
        assert r["not_implementable_reason"] in r["not_implementable_vocabulary"]
        assert r["next_required_action"] == "SELECT_AN_IMPLEMENTABLE_TARGET"
        assert not (tmp_path / "decisions.json").exists()

    def test_61c_an_approve_without_any_selection_is_refused(self, tmp_path):
        """R69.2 — the silent fallback is gone. No selection, no approval."""
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["status"] == pdec.PDS_TARGET_SELECTION_REQUIRED
        assert r["recorded"] is False
        assert r["next_required_action"] == "SELECT_TARGET"
        assert not (tmp_path / "decisions.json").exists()
        # REJECT and HOLD stay available without a selection: an operator must
        # always be able to record a judgement on a proposal they cannot approve.
        for dec in (pdec.DECISION_REJECT, pdec.DECISION_HOLD):
            h = pdec.record_decision(
                decision=dec, confirm=pdec.CONFIRM_TOKEN, artifact=_artifact(),
                proposal_summary=_summary(), decision_dir=str(tmp_path),
                latest_session=SESSION)
            assert h["recorded"] is True, dec

    def test_62_approval_is_refused_when_the_selection_binds_another_proposal(self, tmp_path):
        _select(tmp_path)  # bound to P_HASH
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(phash="DIFFERENT"),
            proposal_summary=_summary(phash="DIFFERENT"),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["status"] == pdec.PDS_STALE and r["recorded"] is False
        assert any(m[0] == "proposal_hash" for m in r["mismatches"])

    def test_63_a_current_selection_has_no_target_to_approve(self, tmp_path):
        _select(tmp_path, target="CURRENT")
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["status"] == pdec.PDS_SELECTION_IS_NO_CHANGE
        assert r["recorded"] is False

    def test_65_a_stale_session_cannot_confirm_an_order_plan(self, tmp_path):
        """The gate binds at the LAST step before the first write too.

        An approval recorded while the session was current must not be able to
        create paper orders after the world has moved on.
        """
        from paper_trader.api import rebalance_execution as rb

        dec = {"record_id": "pdec_r63", "decision": pdec.DECISION_APPROVE,
               "proposal_id": "reap_%s_%s_abc" % (SESSION, BOOK),
               "proposal_hash": "P_HASH",
               "binding": {"active_book_id": BOOK,
                           "eligible_market_date": SESSION,
                           "proposal_hash": "P_HASH"}}
        res = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, active_book_id=BOOK,
            eligible_market_date=SESSION, artifact=_artifact(),
            decision_record=dec, desk_dir=tmp_path / "desk",
            plan_dir=tmp_path / "plans", actions_dir=tmp_path / "ca",
            latest_session=LATER_SESSION)
        assert res["created_orders"] is False and res["created_fills"] is False
        assert res["performed_write"] is False
        assert res["status"] != rb.C_CREATED

    def test_65b_the_freshness_gate_is_reached_once_the_prior_guards_pass(
            self, tmp_path, monkeypatch):
        """Placement proof: with an approved, non-stale plan in front of it, the
        session gate is what refuses - and it refuses BEFORE the first write.

        Only the upstream state composition is stubbed; the gate under test is the
        real one.
        """
        from paper_trader.api import rebalance_execution as rb

        def _approved_plan(**kw):
            return {"state": rb.RB_PLAN_REVIEW_REQUIRED,
                    "bound": {"active_book_id": BOOK,
                              "eligible_market_date": SESSION,
                              "proposal_hash": "P_HASH"},
                    "active_book_id": BOOK, "message": None,
                    "plan": {"order_plan_buildable": True, "orders": [],
                             "order_plan_id": "plan_r63", "blocked": []}}

        monkeypatch.setattr(rb, "_base_plan", _approved_plan)
        res = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, active_book_id=BOOK,
            eligible_market_date=SESSION, desk_dir=tmp_path / "desk",
            plan_dir=tmp_path / "plans", actions_dir=tmp_path / "ca",
            latest_session=LATER_SESSION)
        assert res["status"] == pdec.PDS_SESSION_STALE
        assert res["freshness"]["state"] == pdec.FRESHNESS_STALE
        assert res["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE
        assert res["created_orders"] is False and res["performed_write"] is False
        assert not (tmp_path / "desk").exists(), "nothing may be written"

        # and with a CURRENT session the gate lets it through to the real path
        res2 = rb.confirm_rebalance_order_plan(
            confirm=rb.CONFIRM_TOKEN, active_book_id=BOOK,
            eligible_market_date=SESSION, desk_dir=tmp_path / "desk2",
            plan_dir=tmp_path / "plans2", actions_dir=tmp_path / "ca2",
            latest_session=SESSION)
        assert res2["status"] != pdec.PDS_SESSION_STALE

    def test_66_the_rebalance_read_model_publishes_the_same_verdict(self, tmp_path):
        """A surface must never have to derive confirmability for itself."""
        from paper_trader.api import rebalance_execution as rb

        st = rb.load_rebalance_state(
            active_book_id=BOOK, eligible_market_date=SESSION,
            artifact=_artifact(), decision_record=None,
            desk_dir=tmp_path / "desk", plan_dir=tmp_path / "plans",
            actions_dir=tmp_path / "ca", latest_session=LATER_SESSION)
        assert st["order_plan_confirmation_allowed"] is False
        assert st["freshness"]["state"] == pdec.FRESHNESS_STALE
        assert st["order_plan_buildable"] is False

    def test_64_approval_creates_no_order_or_fill(self, tmp_path):
        _select(tmp_path)
        r = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact=_artifact(), proposal_summary=_summary(),
            decision_dir=str(tmp_path), latest_session=SESSION)
        assert r["created_orders"] is False and r["created_fills"] is False
        assert r["changed_holdings"] is False and r["changed_nav"] is False


# =========================================================================== #
# 7. THE LIVE SEP-18 CASE — historical evidence, preserved exactly
# =========================================================================== #
class TestSep18AcceptanceCase:
    """Reads the LIVE persisted Sep-18 proposal. Read-only: it regenerates
    nothing, mutates nothing, selects nothing and approves nothing.

    It pins that EXACT immutable artifact rather than asking for "the standing
    proposal". The standing proposal is whatever the operator most recently ran -
    on 2026-09-22 it became a Sep-21 one - and a test that asserts a fixed
    identity against a moving pointer fails for the healthiest possible reason.
    The subject here is a specific historical artifact, so it is named.
    """

    PROPOSAL_ID = "reap_2026-09-18_alpha_paper_book_1_9bd6e73a2ef6"

    @pytest.fixture(scope="class")
    def live(self):
        from paper_trader.api import reallocation_proposal as arp

        path = (arp._artifacts_dir() / ("%s.json" % self.PROPOSAL_ID))
        if not path.exists():
            pytest.skip("the Sep-18 acceptance artifact is not in this environment")
        art = json.loads(path.read_text(encoding="utf-8"))
        d = pdrev.load_proposal_decision_review(
            proposal_payload=arp.load_reallocation_proposal(artifact=art),
            artifact_loader=lambda **kw: art)
        if d.get("status") != "OK":
            pytest.skip("the Sep-18 artifact cannot be adjudicated here")
        return d

    def test_70_the_proposal_is_unchanged(self, live):
        assert live["proposal_id"] == self.PROPOSAL_ID
        assert live["mutates_proposal"] is False and live["writes_nothing"] is True

    def test_71_the_superseded_proposal_still_reports_its_open_obligations(self, live):
        """Since the 2026-09-21 cycle ran, this proposal is superseded history, so
        the ladder answers at rung 1 and names WHY. What must not be lost is the
        finding itself: a superseded proposal still reports that its full target
        left obligations open. Supersession changes whether it can be ACTED on,
        never what it says about itself.
        """
        v = (live["review"]["review_verdict"] or {})
        assert v["verdict"] == pdr.VERDICT_BLOCKED_CONSTRAINT_OR_DATA
        assert ("PROPOSAL_NOT_REVIEWABLE_SUPERSEDED_BY_NEWER_DECISION"
                in v["reason_codes"])
        assert "FULL_TARGET_LEAVES_OBLIGATIONS_OPEN" in v["reason_codes"]

    def test_72_the_full_target_is_blocked_on_LH_and_VLO(self, live):
        ts = live["review"]["target_selection"]
        full = [o for o in ts["options"] if o["target"] == "FULL_TARGET"][0]
        assert full["selectable"] is False
        b = [x for x in full["blockers"]
             if x["code"] == pdr.SELECT_BLOCK_OBLIGATIONS_OPEN][0]
        assert b["instruments"] == ["LH", "VLO"]
        assert ts["full_target_reviewable"] is False

    def test_73_the_session_gate_makes_every_target_non_actionable(self, live):
        f = live["freshness"]
        assert f["bound_session"] == "2026-09-18"
        assert f["state"] == pdec.FRESHNESS_STALE
        assert live["actionable"] is False
        assert live["target_selection"]["selectable_targets"] == []
        assert f["next_required_action"] == pdec.NEXT_ACTION_RUN_PORTFOLIO_CYCLE

    def test_74_nothing_is_selected_and_nothing_is_approved(self, live):
        assert (live.get("governance") or {}).get("selected_target") is None
        assert live["manual_approval_required"] is True

    def test_75_the_review_has_a_reproducible_identity(self):
        """The review is a PROJECTION, so the same inputs must give the same
        identity - otherwise a governed selection could never bind one.

        Both loads happen inside this test so they see the same environment: the
        class-scoped fixture above is built outside the per-test store fixtures,
        and a hash taken across two different store roots would compare nothing.
        """
        a = pdrev.load_proposal_decision_review()
        b = pdrev.load_proposal_decision_review()
        if a.get("status") != "OK":
            pytest.skip("no live standing proposal in this environment")
        assert a["review_hash"], "a selection must have an identity to bind"
        assert a["review_hash"] == b["review_hash"]
        assert a["review_hash"] == pdrev.review_hash(a["review"])


# =========================================================================== #
# 8. NO RUNTIME LLM, AND NO SECOND OWNER
# =========================================================================== #
class TestSafetyAndOwnership:

    def test_80_no_llm_in_any_r63_path(self):
        for rel in ("engine/holding_opportunity_cost.py",
                    "engine/constrained_reallocation.py",
                    "engine/proposal_decision_review.py",
                    "api/portfolio_decision.py",
                    "api/proposal_decision_review.py"):
            src = (ROOT / rel).read_text(encoding="utf-8", errors="replace").lower()
            for token in ("anthropic", "openai", "claude(", "llm_call",
                          "chat.completions", "prompt_template"):
                assert token not in src, "%s in %s" % (token, rel)

    def test_81_no_second_optimizer_or_proposal_store(self):
        src = (ROOT / "api" / "portfolio_decision.py").read_text(
            encoding="utf-8", errors="replace")
        # the selection references the proposal; it never builds a target
        for forbidden in ("solve_feasible_target", "build_proposal",
                          "build_review", "solve_minimum_repair"):
            assert forbidden not in src, forbidden

    def test_82_selection_and_approval_tokens_are_distinct(self):
        assert pdec.SELECTION_CONFIRM_TOKEN != pdec.CONFIRM_TOKEN

    def test_83_the_ui_does_not_decide_selectability(self):
        ui = (ROOT / "api" / "ui" / "index.html").read_text(
            encoding="utf-8", errors="replace")
        assert "_pdrSelect" in ui and "pdr-select-" in ui
        # the browser reads the backend's verdict; it never computes one
        assert "o.selectable" in ui
        assert "obligations_left_open_by_full_target" not in ui
        # and the forbidden dialogs are absent from the selection path.
        #
        # R69.1 - this slice used to end at "function _pdrRender", which R69's new
        # "function _pdrRenderLoading" matches as a PREFIX. That loading skeleton is
        # defined ABOVE _pdrSelBtn, so str.index returned the earlier offset, the
        # slice inverted to the empty string, and this assertion passed while
        # testing nothing at all. The end anchor now carries the signature, so it
        # can only match the real definition.
        start, end = ui.index("function _pdrSelBtn"), ui.index("function _pdrRender(d, err)")
        assert start < end, "the selection-path slice must not be empty"
        block = ui[start:end]
        assert len(block) > 2000, "the slice must actually cover the selection path"
        assert "alert(" not in block and "confirm(" not in block

    def test_83b_a_non_selectable_option_carries_no_path_to_the_write(self):
        """Browser-acceptance finding: the handler is attached ONLY when the
        backend says the target is selectable, and `_pdrSelect` re-checks before
        it sends. The guard is structural, not one `disabled` attribute."""
        ui = (ROOT / "api" / "ui" / "index.html").read_text(
            encoding="utf-8", errors="replace")
        block = ui[ui.index("function _pdrSelBtn"):ui.index("function _pdrFreshnessLine")]
        # onclick is inside the selectable branch, never emitted unconditionally
        assert "o.selectable\n       ? ' onclick=\"_pdrSelect(" in block, block[-900:]
        # NB: the assignment, not the bare name - the function's own comment
        # mentions `window._pdrSelect` and would truncate the slice.
        sel = ui[ui.index("function _pdrSelect("):
                 ui.index("window._pdrSelect = _pdrSelect")]
        assert "!opt || !opt.selectable" in sel, "the handler must re-check"
        assert sel.index("!opt || !opt.selectable") < sel.index("fetch("), (
            "the re-check must precede the request")

    def test_83c_the_disabled_tooltip_fallback_can_actually_fire(self):
        """Browser-acceptance finding: `+` binds tighter than `||`, so an
        unparenthesised fallback could never fire and would show 'undefined'."""
        ui = (ROOT / "api" / "ui" / "index.html").read_text(
            encoding="utf-8", errors="replace")
        bad = "'Not selectable. ' + ((o.blockers || [])[0] || {}).detail || "
        assert bad not in ui, "the precedence bug must stay fixed"
        assert "firstBlocker.detail || firstBlocker.code" in ui

    def test_83d_actionability_is_surfaced_at_the_top_of_the_card(self):
        """Browser-acceptance finding: the operator must not have to scroll past
        the comparison to learn that nothing is selectable."""
        ui = (ROOT / "api" / "ui" / "index.html").read_text(
            encoding="utf-8", errors="replace")
        assert "function _pdrFreshnessLine" in ui
        # R69.1 - anchored on the signature; "function _pdrRender" alone also
        # matches "function _pdrRenderLoading", which is defined earlier.
        render = ui[ui.index("function _pdrRender(d, err)"):]
        assert render.index("_pdrFreshnessLine(d)") < render.index("pdr-states"), (
            "the actionability line must render BEFORE the three-state comparison")
        assert "NEXT REQUIRED ACTION" in ui

    def test_84_the_proposal_publishes_the_mandatory_repair_contract(self):
        src = (ROOT / "engine" / "reallocation_proposal.py").read_text(
            encoding="utf-8", errors="replace")
        assert "mandatory_repair" in src
        assert "full_target_reviewable" in src
        assert "governance_repair_obligations" in src
