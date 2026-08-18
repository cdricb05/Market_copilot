r"""Release 29.3 — PORTFOLIO DECISION INTEGRITY + POLICY SEMANTICS.

Everything here is HERMETIC: pure kernels, pure composition helpers and static reads of
``api/ui/index.html``. No store, no provider, no prediction service, no network, no live
backend, no write.

The live 2026-08-17 Daily Research Cycle exposed four defects that this suite pins:

D1  Three surfaces published a proposal conclusion they do not own. The legacy
    rank-membership gate emitted ``outcome="PROPOSAL_READY"`` and the headline
    "PORTFOLIO CHANGES PROPOSED - MANUAL REVIEW REQUIRED"; ``api.daily_close`` derived
    ``close_status=REBALANCE_PROPOSAL_READY`` from it; ``api.daily_research_cycle``
    republished it as ``assessment_status``. Meanwhile the canonical owners reported
    REALLOCATION_PROPOSAL_NOT_RUN / PORTFOLIO_DECISION_NO_PROPOSAL.

D2  The consistency validator compared only DATES, so that payload said CONSISTENT.

D3  The mandatory eligibility-exit override tested ``not blockers`` while the
    sub-hurdle blocker was itself in ``blockers`` - contradicting the policy documented
    directly above it, and trapping ineligible AIZ / SPG in the book.

D4  Concentration / sector / post-change risk / turnover were judged on the RETAINED
    stub renormalised to 1.0. On 2026-08-17 the release set freed ~49.6% of the book
    (``retained_invested_weight = 0.504258``), so every surviving weight was scaled by
    ~1.98x and the gate fired on a renormalisation artifact of a portfolio nobody will
    ever hold.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from paper_trader.api import daily_action_gate as dag
from paper_trader.api import daily_close as dc
from paper_trader.api import portfolio_decision as pdec
from paper_trader.api import reallocation_proposal as arp
from paper_trader.api import workflow_state as ws
from paper_trader.engine import holding_opportunity_cost as hoc_kernel
from paper_trader.engine import portfolio_reassessment as prs_kernel
from paper_trader.engine import reallocation_proposal as rp_kernel

UI_FILE = Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html"


def _ui() -> str:
    return UI_FILE.read_text(encoding="utf-8")


def _src(*parts) -> str:
    return Path(__file__).resolve().parents[1].joinpath(*parts).read_text(encoding="utf-8")


# =========================================================================== #
# The real 2026-08-17 evidence, as constants. Nothing below re-derives them.
# =========================================================================== #
AUG17 = "2026-08-17"
AUG17_REASSESSMENT_ID = "prs_2026-08-17_alpha_paper_book_1_7edb4353341f"
AUG17_NET_IMPROVEMENT = 0.028081
AUG17_NET_HURDLE = 0.050000
AUG17_ONE_WAY_TURNOVER = 0.451049
AUG17_TURNOVER_BUDGET = 0.350000
AUG17_COST_USD = 113.10
AUG17_ATTENTION = 15
AUG17_MANDATORY_EXITS = ["AIZ", "SPG"]
#: The four blockers the live artifact recorded, every one of them a complete-target
#: constraint judged against the retained-only stub.
AUG17_LEGACY_BLOCKERS = [
    "CONCENTRATION_DETERIORATION_BLOCKS_CHANGE",
    "RISK_DETERIORATION_BLOCKS_CHANGE",
    "SECTOR_CAP_BREACH_BLOCKS_CHANGE",
    "TURNOVER_BUDGET_EXCEEDED",
]
#: The renormalisation evidence, verbatim from the live artifact's concentration block.
AUG17_RETAINED_INVESTED_WEIGHT = 0.504258
AUG17_MAX_NAME_BEFORE = 0.044184          # DVN
AUG17_MAX_NAME_AFTER_RETAINED = 0.081571  # FANG - not one dollar moved into it
AUG17_MAX_SECTOR_BEFORE = 0.325195        # bucket "Unknown"
AUG17_MAX_SECTOR_AFTER_RETAINED = 0.374216


def _aug17_reassessment_summary(**over) -> dict:
    """The compact reassessment summary the composition layer reads, carrying the real
    2026-08-17 values."""
    base = {
        "reassessment_available": True,
        "reassessment_state": "CHANGE_CANDIDATE",
        "reassessment_id": AUG17_REASSESSMENT_ID,
        "reassessment_hash": "7edb4353341f53f7b428c8f1179ab08a39d4e030d6771b3b2168eb64d8f20121",
        "hoc_assessment_hash": "7a96efc2f95e39f9a89fe1b2905eda6281c06f8fd4af704462335eba70b4ebb1",
        "reassessment_date": AUG17,
        "decision": "CHANGE_CANDIDATE",
        "proposal_required": False,
        "attention_count": AUG17_ATTENTION,
        "holdings_evaluated": 25,
        "expected_net_improvement": AUG17_NET_IMPROVEMENT,
        "net_improvement_hurdle": AUG17_NET_HURDLE,
        "expected_one_way_turnover": AUG17_ONE_WAY_TURNOVER,
        "turnover_budget": AUG17_TURNOVER_BUDGET,
        "expected_transaction_cost_usd": AUG17_COST_USD,
        "blockers": ["BELOW_PORTFOLIO_NET_IMPROVEMENT_HURDLE"],
        "reason_codes": [],
        "explanation": ("15 holding(s) have attractive alternatives, but no portfolio "
                        "change is proposed."),
        "mandatory_exit_tickers": list(AUG17_MANDATORY_EXITS),
        "mandatory_exit_policy": {
            "policy": prs_kernel.MANDATORY_EXIT_POLICY,
            "obligation": "REQUIRED_IF_REALLOCATION_PROCEEDS",
            "authorizes_order": False,
            "statement": "AIZ, SPG no longer meet the eligibility rule.",
        },
    }
    base.update(over)
    return base


# =========================================================================== #
# 1. D1 - NO SURFACE MAY SPEAK THE PROPOSAL OWNER'S VOCABULARY
# =========================================================================== #
class TestProposalVocabularyOwnership:

    def test_01_legacy_gate_no_longer_emits_a_proposal_outcome(self):
        assert dag.OUTCOME_MEMBERSHIP_DRIFT == "MEMBERSHIP_DRIFT_DETECTED"
        assert dag.OUTCOME_PROPOSAL_READY == dag.OUTCOME_MEMBERSHIP_DRIFT
        assert "PROPOSAL" not in dag.OUTCOME_MEMBERSHIP_DRIFT
        assert dag.LEGACY_OUTCOME_PROPOSAL_READY == "PROPOSAL_READY"
        assert dag.LEGACY_OUTCOME_PROPOSAL_READY not in dag.ALL_OUTCOMES

    def test_02_legacy_gate_target_state_is_membership_drift(self):
        assert dag.TARGET_STATE_MEMBERSHIP_DRIFT == "MEMBERSHIP_DRIFT"
        assert "PROPOSAL" not in dag.TARGET_STATE_MEMBERSHIP_DRIFT
        assert dag.LEGACY_TARGET_STATE_PROPOSAL_READY not in dag.ALL_TARGET_STATES

    def test_03_legacy_gate_headline_never_claims_changes_are_proposed(self):
        pres = dag._PRESENTATION[dag.OUTCOME_MEMBERSHIP_DRIFT]  # noqa: SLF001
        assert "PORTFOLIO CHANGES PROPOSED" not in pres["headline"].upper()
        assert "PROPOSAL READY" not in pres["headline"].upper()
        assert "LEGACY MEMBERSHIP" in pres["headline"].upper()
        assert "COMPATIBILITY ONLY" in pres["headline"].upper()

    def test_04_a_membership_difference_is_not_operator_work(self):
        """A compatibility-only comparison must never raise action_required: the
        canonical portfolio owners decide what the operator does."""
        src = _src("api", "daily_action_gate.py")
        m = re.search(r"action_required = outcome in \(([^)]*)\)", src, re.S)
        assert m, "action_required derivation not found"
        assert "OUTCOME_MEMBERSHIP_DRIFT" not in m.group(1)
        assert "OUTCOME_PROPOSAL_READY" not in m.group(1)

    def test_05_daily_close_status_describes_close_semantics_only(self):
        assert dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT == "DAILY_CLOSE_COMPLETE_MEMBERSHIP_DRIFT"
        assert "REBALANCE" not in dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
        assert "PROPOSAL" not in dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
        assert dc.LEGACY_REBALANCE_PROPOSAL_READY not in dc.ALL_CLOSE_STATUSES

    def test_06_historical_journal_rows_are_normalised_on_read_never_rewritten(self):
        """The live Aug-17 journal row carries the legacy token. History is immutable, so
        the migration happens on READ and every consumer sees ONE vocabulary."""
        assert dc.normalize_close_status("REBALANCE_PROPOSAL_READY") == \
            dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT
        assert dc.normalize_close_decision("REBALANCE_PROPOSAL_READY") == \
            dc.DECISION_MEMBERSHIP_DRIFT
        # Unknown / already-canonical values pass through untouched.
        assert dc.normalize_close_status(dc.CLOSE_COMPLETE_HOLD) == dc.CLOSE_COMPLETE_HOLD
        assert dc.normalize_close_status(None) is None

    def test_07_daily_close_derives_the_branch_from_the_membership_outcome(self):
        src = _src("api", "daily_close.py")
        assert "DECISION_MEMBERSHIP_DRIFT, CLOSE_COMPLETE_MEMBERSHIP_DRIFT" in src
        assert "dag.OUTCOME_MEMBERSHIP_DRIFT" in src

    def test_08_drc_scopes_its_legacy_assessment_block(self):
        src = _src("api", "daily_research_cycle.py")
        assert '"assessment_scope": "LEGACY_RANK_MEMBERSHIP_COMPARISON"' in src
        assert '"is_portfolio_proposal": False' in src


# =========================================================================== #
# 2. D2 - SEMANTIC CONSISTENCY INVARIANTS
# =========================================================================== #
class TestSemanticConsistencyInvariants:

    def _args(self, **over) -> dict:
        base = dict(
            reallocation_operator_state=ws.RPS_NOT_RUN,
            reallocation_approvable=False,
            reassessment_state="CHANGE_CANDIDATE",
            reassessment_proposal_required=False,
            portfolio_decision_state=ws.PDS_NO_PROPOSAL,
            portfolio_decision_requires_review=False,
            portfolio_decision_approvable=False,
            proposal_bound_reassessment_hash=None,
            current_reassessment_hash=None,
            mandatory_exit_tickers=[],
            mandatory_exit_obligation="NONE",
            summary_claims={},
        )
        base.update(over)
        return base

    def test_10_the_live_aug17_payload_is_now_INCONSISTENT(self):
        """The exact contradictory field set observed live must be caught, at the exact
        fields that produced it."""
        v = ws.check_decision_semantics(**self._args(summary_claims={
            "operational_state.latest_close_status": "REBALANCE_PROPOSAL_READY",
            "research_cycle_state.assessment_status": "PROPOSAL_READY",
            "portfolio_assessment_state.latest_assessment_result": "PROPOSAL_READY",
            "portfolio_assessment_state.latest_assessment_recommendation":
                "PORTFOLIO CHANGES PROPOSED - MANUAL REVIEW REQUIRED",
            "completed_summary.latest_completed_close.status": "REBALANCE_PROPOSAL_READY",
            "completed_summary.latest_portfolio_assessment.result": "PROPOSAL_READY",
        }))
        codes = {x["code"] for x in v}
        assert codes == {"PROPOSAL_CLAIMED_WITHOUT_PROPOSAL_OWNER"}
        assert len(v) == 6
        fields = {x["field"] for x in v}
        assert "operational_state.latest_close_status" in fields
        assert "research_cycle_state.assessment_status" in fields
        assert all(x["canonical_proposal_state"] == ws.RPS_NOT_RUN for x in v)

    def test_11_the_repaired_aug17_payload_is_CONSISTENT(self):
        """With every field re-sourced from its owner, the same world is clean."""
        assert ws.check_decision_semantics(**self._args(summary_claims={
            "operational_state.latest_close_status":
                dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
            "research_cycle_state.assessment_status": dag.OUTCOME_MEMBERSHIP_DRIFT,
            "portfolio_assessment_state.latest_assessment_result":
                dag.OUTCOME_MEMBERSHIP_DRIFT,
            "portfolio_assessment_state.gate_target_state":
                dag.TARGET_STATE_MEMBERSHIP_DRIFT,
            "portfolio_assessment_state.portfolio_decision_state": ws.PDS_NO_PROPOSAL,
        })) == []

    def test_12_change_candidate_without_proposal_cannot_require_review(self):
        v = ws.check_decision_semantics(**self._args(
            portfolio_decision_state=ws.PDS_REVIEW_REQUIRED,
            portfolio_decision_requires_review=True))
        assert "PROPOSAL_REQUIRED_CONTRADICTS_PORTFOLIO_DECISION" in {x["code"] for x in v}

    def test_13_no_proposal_can_never_be_approvable(self):
        v = ws.check_decision_semantics(**self._args(portfolio_decision_approvable=True))
        assert "APPROVABLE_WITHOUT_CANONICAL_PROPOSAL" in {x["code"] for x in v}

    def test_14_a_proposal_must_bind_to_the_current_session_evidence(self):
        v = ws.check_decision_semantics(**self._args(
            reallocation_operator_state=ws.RPS_READY,
            reassessment_state="PROPOSAL_READY", reassessment_proposal_required=True,
            portfolio_decision_state=ws.PDS_REVIEW_REQUIRED,
            portfolio_decision_requires_review=True,
            proposal_bound_reassessment_hash="hoc_YESTERDAY",
            current_reassessment_hash="hoc_TODAY"))
        assert "PROPOSAL_NOT_BOUND_TO_CURRENT_REASSESSMENT" in {x["code"] for x in v}
        # ...and a correctly-bound proposal is clean.
        assert ws.check_decision_semantics(**self._args(
            reallocation_operator_state=ws.RPS_READY,
            reassessment_state="PROPOSAL_READY", reassessment_proposal_required=True,
            portfolio_decision_state=ws.PDS_REVIEW_REQUIRED,
            portfolio_decision_requires_review=True,
            proposal_bound_reassessment_hash="hoc_TODAY",
            current_reassessment_hash="hoc_TODAY")) == []

    def test_15_a_withheld_target_can_never_be_exposed_as_approvable(self):
        v = ws.check_decision_semantics(**self._args(
            reallocation_operator_state=ws.RPS_WITHHELD,
            portfolio_decision_state=ws.PDS_CHANGE_WITHHELD,
            portfolio_decision_approvable=True))
        assert "WITHHELD_PROPOSAL_EXPOSED_AS_APPROVABLE" in {x["code"] for x in v}

    def test_16_a_mandatory_exit_is_never_an_executable_obligation(self):
        v = ws.check_decision_semantics(**self._args(
            mandatory_exit_tickers=AUG17_MANDATORY_EXITS,
            mandatory_exit_obligation="MUST_EXIT_NOW"))
        assert "MANDATORY_EXIT_PRESENTED_AS_EXECUTABLE_OBLIGATION" in {x["code"] for x in v}
        # The canonical obligation phrasing is clean.
        assert ws.check_decision_semantics(**self._args(
            mandatory_exit_tickers=AUG17_MANDATORY_EXITS,
            mandatory_exit_obligation="REQUIRED_IF_REALLOCATION_PROCEEDS")) == []

    def test_17_the_invariants_compare_owners_and_recompute_no_economics(self):
        """A consistency check that re-derived an owner's economics would be a SECOND
        calculation of that concept - the exact thing this release exists to end."""
        src = _src("api", "workflow_state.py")
        start = src.index("def check_decision_semantics(")
        body = src[start:src.index("\ndef ", start + 10)]
        for forbidden in ("hurdle", "turnover_budget", "herfindahl", "* nav",
                          "cost_rate", "expected_net_improvement >"):
            assert forbidden not in body, "recomputed economics: %s" % forbidden


# =========================================================================== #
# 3. D3 - MANDATORY ELIGIBILITY-EXIT POLICY (decision: bounded override)
# =========================================================================== #
class TestMandatoryEligibilityExitPolicy:

    def test_20_the_policy_is_explicit_versioned_and_bounded(self):
        assert prs_kernel.MANDATORY_EXIT_POLICY == \
            "ELIGIBILITY_EXIT_OVERRIDES_ECONOMIC_GATES_ONLY"
        assert prs_kernel.MANDATORY_EXIT_POLICY_VERSION == \
            "mandatory_eligibility_exit_policy.v1"
        # It overrides the ECONOMIC gates...
        assert set(prs_kernel.MANDATORY_EXIT_OVERRIDES) == {
            prs_kernel.GATE_BELOW_NET_HURDLE, prs_kernel.GATE_IMPROVEMENT_UNMEASURABLE}
        # ...and NEVER a hard feasibility blocker.
        assert prs_kernel.GATE_LIQUIDITY in prs_kernel.MANDATORY_EXIT_HARD_BLOCKERS
        # The two sets are disjoint: no code can be both overridable and hard.
        assert not (set(prs_kernel.MANDATORY_EXIT_OVERRIDES)
                    & set(prs_kernel.MANDATORY_EXIT_HARD_BLOCKERS))

    def test_21_it_never_overrides_a_complete_target_constraint(self):
        block = prs_kernel.mandatory_exit_policy_block(
            mandatory_exits=list(AUG17_MANDATORY_EXITS), hard_blockers=[], cleared=True)
        assert set(block["never_overrides_complete_target_constraints"]) == \
            set(prs_kernel.COMPLETE_TARGET_CONSTRAINT_CODES)
        assert block["requires_complete_target"] is True

    def test_22_clearing_the_ask_authorises_nothing(self):
        block = prs_kernel.mandatory_exit_policy_block(
            mandatory_exits=list(AUG17_MANDATORY_EXITS), hard_blockers=[], cleared=True)
        assert block["override_applied"] is True and block["withheld"] is False
        assert block["authorizes_order"] is False
        assert block["authorizes_sell_only_plan"] is False
        assert block["manual_review_required"] is True
        assert block["obligation"] == "REQUIRED_IF_REALLOCATION_PROCEEDS"

    def test_23_a_withheld_exit_says_required_if_never_must_exit_now(self):
        block = prs_kernel.mandatory_exit_policy_block(
            mandatory_exits=list(AUG17_MANDATORY_EXITS),
            hard_blockers=[prs_kernel.GATE_LIQUIDITY], cleared=False)
        assert block["withheld"] is True
        assert block["obligation"] == "REQUIRED_IF_REALLOCATION_PROCEEDS"
        txt = block["statement"].lower()
        assert "required if a reallocation proceeds" in txt
        for forbidden in ("must exit now", "required exit", "sell now", "exit now"):
            assert forbidden not in txt

    def test_24_no_operator_wording_anywhere_says_must_exit_now(self):
        """The per-holding EXIT sentence is what the operator actually reads for AIZ/SPG."""
        review = {"ticker": "AIZ", "recommendation": prs_kernel.REC_EXIT,
                  "current_rank": 33, "rank_change": 0,
                  "deterioration_state": hoc_kernel.DET_BROKEN,
                  "deterioration_reason_codes": ["FELL_BELOW_EXIT_BUFFER"]}
        txt = prs_kernel.explain_holding(review, universe_size=199,
                                         policy=prs_kernel.default_policy(),
                                         churn_codes=[], actionable=True,
                                         below_min_weight=False).lower()
        assert "required if a reallocation proceeds" in txt
        assert "never as a standalone sell" in txt
        for forbidden in ("must exit now", "required exit", "must be sold"):
            assert forbidden not in txt

    def test_25_no_parallel_mandatory_exit_policy_exists(self):
        """Exactly ONE module may decide what "mandatory" authorises."""
        root = Path(__file__).resolve().parents[1]
        owners = []
        for pyf in list((root / "api").glob("*.py")) + list((root / "engine").glob("*.py")):
            src = pyf.read_text(encoding="utf-8")
            if "MANDATORY_EXIT_OVERRIDES" in src and "= (" in src:
                if re.search(r"^MANDATORY_EXIT_OVERRIDES\s*=", src, re.M):
                    owners.append(pyf.name)
        assert owners == ["portfolio_reassessment.py"], owners


# =========================================================================== #
# 4. D4 - CONSTRAINT OWNERSHIP: RELEASE SET vs COMPLETE TARGET
# =========================================================================== #
class TestConstraintOwnership:

    def test_30_the_ownership_split_is_declared_structurally(self):
        own = prs_kernel.constraint_ownership()
        assert own["duplicated"] is False
        assert own["decided_here"]["object"] == "RELEASE_SET"
        assert own["decided_here"]["owner"] == prs_kernel.CALCULATION_OWNER
        assert own["deferred_to_complete_target"]["object"] == "COMPLETE_TARGET"
        assert own["deferred_to_complete_target"]["owner"] == \
            prs_kernel.TARGET_ENGINE_OWNER == "engine.reallocation_proposal"
        # The four moved constraints, and nothing else, are deferred.
        assert set(own["deferred_to_complete_target"]["constraints"]) == set(
            AUG17_LEGACY_BLOCKERS)

    def test_31_the_two_owners_agree_on_the_moved_constraint_codes(self):
        """A moved constraint must be the SAME code on both sides - a rename on one side
        only would silently create two concepts."""
        assert set(prs_kernel.COMPLETE_TARGET_CONSTRAINT_CODES) == \
            set(rp_kernel.COMPLETE_TARGET_CONSTRAINT_CODES)
        assert rp_kernel.ASK_GATE_OWNER == prs_kernel.CALCULATION_OWNER

    def test_32_the_reassessment_never_raises_a_complete_target_constraint(self):
        """The Aug-17 shape: a big release set whose retained stub looks concentrated."""
        rows = [_replace_row("A%d" % i, weight=0.04) for i in range(10)]
        rows += [_review("BIG", current_weight=0.50, market_value=50000.0)]
        res = _run_reassessment(rows)
        d = res["decision"]
        for code in AUG17_LEGACY_BLOCKERS:
            assert code not in d["blockers"], code
        assert d["turnover_budget_binding_here"] is False
        assert d["expected_turnover_basis"] == "PRE_PROPOSAL_RELEASE_SET_ESTIMATE"
        assert d["concentration_basis"] == "PRE_PROPOSAL_RETAINED_BOOK_RENORMALISED"

    def test_33_the_retained_stub_is_a_renormalisation_artifact(self):
        """Reproduces the Aug-17 arithmetic that made the gate fire: releasing ~half the
        book scales every SURVIVING weight even though no capital moved into it."""
        current = {"A": 0.10, "B": 0.10, "C": 0.10, "D": 0.10, "E": 0.10}
        released = {"A": 0.10, "B": 0.10, "C": 0.10}   # 60% of the book freed
        conc = prs_kernel.retained_concentration(
            current_weight=current, released=released,
            sector_of={k: "Tech" for k in current})
        assert conc["basis"] == "RETAINED_BOOK_RENORMALISED"
        assert conc["retained_invested_weight"] == pytest.approx(0.20)
        # D and E never changed by a single dollar...
        assert current["D"] == current["E"] == 0.10
        # ...yet the renormalised stub reports them at 50% each.
        assert conc["max_name_weight_after_retained"] == pytest.approx(0.50)
        assert conc["herfindahl_change"] > 0
        # The live Aug-17 numbers show exactly this shape.
        assert AUG17_RETAINED_INVESTED_WEIGHT < 0.55
        assert AUG17_MAX_NAME_AFTER_RETAINED > AUG17_MAX_NAME_BEFORE * 1.8

    def test_34_the_complete_target_owner_decides_the_moved_constraints(self):
        limits = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": 0.20},
            risk={"concentration_before": 0.05, "concentration_after": 0.045,
                  "sector_concentration_before": 0.30, "sector_concentration_after": 0.22},
            policy=rp_kernel.default_policy())
        assert limits["owner"] == "engine.reallocation_proposal"
        assert limits["object"] == "COMPLETE_TARGET"
        assert limits["evaluated_once"] is True
        assert limits["all_ok"] is True and limits["withheld"] is False

    def test_35_a_complete_target_that_FIXES_concentration_is_not_blocked(self):
        """The fixture the reassessment could never see: the retained stub looks worse,
        but the complete replacement target is BETTER diversified. Only the complete-target
        owner can tell, and it says yes."""
        # Retained-only view: releasing 3 of 5 names concentrates the stub.
        current = {"A": 0.20, "B": 0.20, "C": 0.20, "D": 0.20, "E": 0.20}
        stub = prs_kernel.retained_concentration(
            current_weight=current, released={"A": 0.20, "B": 0.20, "C": 0.20},
            sector_of={k: "Tech" for k in current})
        assert stub["herfindahl_change"] > 0.02        # the stub "deteriorates"
        # Complete target: the released capital buys three NEW names, so HHI improves.
        limits = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": 0.30},
            risk={"concentration_before": 0.20, "concentration_after": 0.125,
                  "sector_concentration_before": 1.0, "sector_concentration_after": 0.40},
            policy=rp_kernel.default_policy())
        assert limits["all_ok"] is True
        assert limits["withheld_codes"] == []

    def test_36_a_complete_target_that_cannot_satisfy_the_limit_is_withheld_safely(self):
        limits = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": AUG17_ONE_WAY_TURNOVER},
            risk={"concentration_before": 0.04, "concentration_after": 0.09,
                  "sector_concentration_before": 0.20, "sector_concentration_after": 0.38},
            policy=rp_kernel.default_policy())
        assert limits["withheld"] is True
        assert set(limits["withheld_codes"]) == {
            rp_kernel.CT_TURNOVER_BUDGET, rp_kernel.CT_CONCENTRATION,
            rp_kernel.CT_RISK_DETERIORATION, rp_kernel.CT_SECTOR_CAP}
        # Every breach names the object it was judged on - never the retained stub.
        assert all(b["object"] == "COMPLETE_TARGET" for b in limits["breaches"])

    def test_37_deterioration_not_a_standing_breach(self):
        """A book already above the sector cap is a standing condition the operator owns;
        it must not permanently freeze every future reallocation."""
        limits = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": 0.10},
            risk={"concentration_before": 0.10, "concentration_after": 0.10,
                  "sector_concentration_before": 0.40, "sector_concentration_after": 0.35},
            policy=rp_kernel.default_policy())
        assert limits["all_ok"] is True     # 0.35 > 0.25 cap, but it IMPROVED


# =========================================================================== #
# 5. TURNOVER - applied ONCE, on the complete target
# =========================================================================== #
class TestTurnoverAppliedOnce:

    def test_40_the_budget_binds_on_the_complete_target_only(self):
        pol = rp_kernel.default_policy()
        assert pol["max_one_way_turnover"] == 0.35
        risk = {"concentration_before": 0.05, "concentration_after": 0.05,
                "sector_concentration_before": 0.20, "sector_concentration_after": 0.20}
        under = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": 0.35}, risk=risk, policy=pol)
        over = rp_kernel.evaluate_complete_target_limits(
            turnover={"one_way_turnover": 0.3500001}, risk=risk, policy=pol)
        assert under["all_ok"] is True                      # exactly AT the budget passes
        assert over["withheld_codes"] == [rp_kernel.CT_TURNOVER_BUDGET]

    def test_41_the_reassessment_verdict_is_invariant_to_the_budget(self):
        rows = [_replace_row("A%d" % i, weight=0.04) for i in range(5)]
        tight = _run_reassessment(rows, policy={"max_one_way_turnover_per_reassessment": 0.10})
        loose = _run_reassessment(rows, policy={"max_one_way_turnover_per_reassessment": 0.90})
        assert tight["reassessment_state"] == loose["reassessment_state"]
        assert prs_kernel.CHURN_TURNOVER_BUDGET not in tight["decision"]["blockers"]

    def test_42_transaction_cost_is_never_double_counted(self):
        """Both owners publish a cost, but only ONE of them is a gate. The reassessment
        marks its own number non-binding, so no cost is charged twice."""
        rows = [_replace_row("A%d" % i, weight=0.04) for i in range(5)]
        d = _run_reassessment(rows)["decision"]
        assert d["transaction_cost_counted_once"] is True
        assert d["turnover_budget_binding_here"] is False


# =========================================================================== #
# 6. AUG-17 REPLAY: CHANGE CANDIDATE / NO PROPOSAL through the composition layer
# =========================================================================== #
class TestAug17ChangeCandidateNoProposal:

    def _decision(self, **over):
        return ws.build_canonical_portfolio_decision(
            reassessment_summary=_aug17_reassessment_summary(**over),
            reallocation_operator_state=ws.RPS_NOT_RUN,
            portfolio_decision_lane={
                "portfolio_decision_state": ws.PDS_NO_PROPOSAL,
                "requires_manual_review": False, "approvable": False,
                "material": False, "withheld_reasons": []},
            attention_count=AUG17_ATTENTION, eligible_date=AUG17)

    def test_50_the_canonical_decision_is_change_withheld_not_no_proposal(self):
        """"No proposal yet" reads as "nothing happened". Fifteen holdings were flagged
        and the change was WITHHELD - the operator must be told which."""
        d = self._decision()
        assert d["state"] == "CHANGE_CANDIDATE_WITHHELD"
        assert d["headline"] == "PORTFOLIO CHANGE WITHHELD"
        assert d["state"] in ws.CANONICAL_PORTFOLIO_DECISION_STATES

    def test_51_hoc_attention_and_the_portfolio_verdict_are_different_questions(self):
        d = self._decision()
        assert d["holding_attention_count"] == AUG17_ATTENTION
        assert d["holding_attention_scope"] == "INDIVIDUAL_HOLDING_REVIEW_SIGNAL"
        assert d["portfolio_scope"] == "WHOLE_PORTFOLIO_ECONOMIC_VERDICT"
        assert d["scopes_are_different_questions"] is True

    def test_52_no_operator_proposal_action_exists(self):
        d = self._decision()
        assert d["operator_action_available"] is False
        assert d["approvable"] is False
        assert d["proposal_state"] == ws.RPS_NOT_RUN
        assert d["proposal_hash"] is None
        assert d["creates_orders"] is False and d["automation_off"] is True

    def test_53_the_numbers_are_the_owners_numbers_verbatim(self):
        d = self._decision()
        assert d["expected_net_improvement"] == AUG17_NET_IMPROVEMENT
        assert d["net_improvement_hurdle"] == AUG17_NET_HURDLE
        assert d["expected_one_way_turnover"] == AUG17_ONE_WAY_TURNOVER
        assert d["turnover_budget"] == AUG17_TURNOVER_BUDGET
        assert d["expected_transaction_cost_usd"] == AUG17_COST_USD
        assert d["reassessment_hash"] == _aug17_reassessment_summary()["reassessment_hash"]

    def test_54_the_eligibility_exits_are_reported_as_required_if(self):
        d = self._decision()
        assert d["mandatory_exit_tickers"] == AUG17_MANDATORY_EXITS
        assert d["mandatory_exit_obligation"] == "REQUIRED_IF_REALLOCATION_PROCEEDS"

    def test_55_nothing_in_the_decision_object_says_proposal_ready(self):
        d = self._decision()
        for key in ("state", "headline"):
            assert not ws._claims_proposal(d[key]), key   # noqa: SLF001

    def test_56_the_composed_world_is_semantically_CONSISTENT(self):
        assert ws.check_decision_semantics(
            reallocation_operator_state=ws.RPS_NOT_RUN, reallocation_approvable=False,
            reassessment_state="CHANGE_CANDIDATE", reassessment_proposal_required=False,
            portfolio_decision_state=ws.PDS_NO_PROPOSAL,
            portfolio_decision_requires_review=False,
            portfolio_decision_approvable=False,
            proposal_bound_reassessment_hash=None,
            current_reassessment_hash=_aug17_reassessment_summary()["hoc_assessment_hash"],
            mandatory_exit_tickers=AUG17_MANDATORY_EXITS,
            mandatory_exit_obligation="REQUIRED_IF_REALLOCATION_PROCEEDS",
            summary_claims={
                "operational_state.latest_close_status":
                    dc.CLOSE_COMPLETE_MEMBERSHIP_DRIFT,
                "research_cycle_state.assessment_status": dag.OUTCOME_MEMBERSHIP_DRIFT,
            }) == []


# =========================================================================== #
# 7. PROPOSAL-READY replay
# =========================================================================== #
class TestProposalReviewRequired:

    def _decision(self):
        return ws.build_canonical_portfolio_decision(
            reassessment_summary=_aug17_reassessment_summary(
                reassessment_state="PROPOSAL_READY", proposal_required=True,
                expected_net_improvement=0.166, blockers=[],
                mandatory_exit_tickers=[], mandatory_exit_policy={"obligation": "NONE"},
                explanation="A portfolio change is economically justified."),
            reallocation_operator_state=ws.RPS_READY,
            portfolio_decision_lane={
                "portfolio_decision_state": ws.PDS_REVIEW_REQUIRED,
                "requires_manual_review": True, "approvable": True, "material": True,
                "proposal_hash": "prop_abc123", "withheld_reasons": []},
            attention_count=4, eligible_date=AUG17)

    def test_60_a_real_proposal_yields_manual_review_required(self):
        d = self._decision()
        assert d["state"] == "PROPOSAL_REVIEW_REQUIRED"
        assert d["headline"] == "PORTFOLIO PROPOSAL — MANUAL REVIEW REQUIRED"
        assert d["operator_action_available"] is True
        assert d["approvable"] is True
        assert d["proposal_hash"] == "prop_abc123"

    def test_61_it_still_creates_no_order_and_no_automation(self):
        d = self._decision()
        assert d["creates_orders"] is False
        assert d["automation_off"] is True
        assert d["manual_review_only"] is True

    def test_62_this_is_the_only_actionable_decision_state(self):
        assert ws.CANONICAL_ACTIONABLE_DECISION_STATES == ("PROPOSAL_REVIEW_REQUIRED",)

    def test_63_a_requested_but_unproduced_proposal_is_not_no_decision_yet(self):
        d = ws.build_canonical_portfolio_decision(
            reassessment_summary=_aug17_reassessment_summary(
                reassessment_state="PROPOSAL_READY", proposal_required=True),
            reallocation_operator_state=ws.RPS_NOT_RUN,
            portfolio_decision_lane={"portfolio_decision_state": ws.PDS_NO_PROPOSAL,
                                     "requires_manual_review": False, "approvable": False},
            attention_count=4, eligible_date=AUG17)
        assert d["proposal_requested_not_produced"] is True
        assert d["headline"] == "PORTFOLIO PROPOSAL REQUESTED — NOT YET PRODUCED"
        assert d["operator_action_available"] is False


# =========================================================================== #
# 8. WITHHELD proposals are fail-closed everywhere
# =========================================================================== #
class TestWithheldIsFailClosed:

    def _summary(self, **over):
        s = {"reallocation_proposal_available": True,
             "reallocation_proposal_state": rp_kernel.STATE_WITHHELD,
             "reallocation_proposal_hash": "p1",
             "reallocation_proposal_withheld": True,
             "reallocation_withheld_reasons": [rp_kernel.CT_TURNOVER_BUDGET],
             "reallocation_action_counts": {"EXIT": 3, "ADD": 3, "REPLACE_IN": 0,
                                            "REPLACE_OUT": 0, "INCREASE": 0,
                                            "REDUCE": 0, "RETAIN": 19}}
        s.update(over)
        return s

    def test_70_the_decision_owner_reports_change_withheld(self):
        st = pdec.derive_decision_state(has_active_book=True,
                                        proposal_summary=self._summary(),
                                        decision_record=None)
        assert st["portfolio_decision_state"] == pdec.PDS_CHANGE_WITHHELD
        assert st["approvable"] is False
        assert st["requires_manual_review"] is False
        assert st["change_withheld"] is True
        assert st["withheld_reasons"] == [rp_kernel.CT_TURNOVER_BUDGET]

    def test_71_recording_a_decision_on_a_withheld_target_is_refused(self):
        out = pdec.record_decision(
            decision=pdec.DECISION_APPROVE, confirm=pdec.CONFIRM_TOKEN,
            artifact={"proposal_id": "p1", "identity": {"proposal_hash": "p1"},
                      "proposal": {"proposal_hash": "p1"}},
            proposal_summary=self._summary())
        assert out["recorded"] is False
        assert out["status"] == pdec.PDS_CHANGE_WITHHELD
        assert out["created_orders"] is False and out["changed_holdings"] is False

    def test_72_withheld_is_not_an_approvable_state_at_any_layer(self):
        assert rp_kernel.STATE_WITHHELD not in rp_kernel.APPROVABLE_STATES
        assert arp.STATE_WITHHELD not in arp.APPROVABLE_READ_STATES
        assert ws.RPS_WITHHELD not in ws.REALLOCATION_APPROVABLE_STATES
        assert pdec.PDS_CHANGE_WITHHELD not in pdec.APPROVABLE_DECISION_STATES

    def test_73_a_withheld_kernel_result_publishes_the_full_target_for_review(self):
        """Withholding must EXPLAIN, not hide: the operator sees what was rejected."""
        assert rp_kernel.STATE_WITHHELD in rp_kernel.PROPOSAL_STATE_VOCAB
        assert rp_kernel.STATE_WITHHELD in arp.READ_STATE_VOCAB


# =========================================================================== #
# 9. UI decision semantics (static; the renderers read the backend verbatim)
# =========================================================================== #
class TestUiDecisionSemantics:

    def test_80_today_is_the_only_full_operator_hero(self):
        ui = _ui()
        assert 'body[data-route="markets"] #operator-command' in ui
        assert 'body[data-route="system-audit"] #operator-command' in ui
        assert 'body[data-route="portfolio-manager"] #operator-command' in ui
        # Portfolio collapses the hero to one line - the eyebrow/why/meta/CTA go away.
        for part in ("opc-eyebrow", "opc-why", "opc-meta", "opc-next"):
            assert 'body[data-route="portfolio-manager"] #operator-command .%s' % part in ui

    def test_81_research_shows_a_workflow_action_only_when_research_related(self):
        ui = _ui()
        assert 'body[data-route="research"] #operator-command[data-op-research="1"]' in ui
        # The flag is set from the BACKEND action code; the UI classifies nothing else.
        assert "data-op-research" in ui
        assert "'RUN_DAILY_RESEARCH_CYCLE'" in ui
        assert "'RESOLVE_RESEARCH_CYCLE_BLOCKER'" in ui

    def test_82_the_verdict_block_renders_the_canonical_decision_verbatim(self):
        ui = _ui()
        assert 'id="cc-verdict"' in ui
        assert "function _wsRenderPortfolioVerdict(d)" in ui
        assert "d.canonical_portfolio_decision" in ui
        # It rides the ONE workflow-state renderer - no second loader.
        assert "try { _wsRenderPortfolioVerdict(d); } catch (e) {}" in ui
        assert ui.count("try { _wsRenderPortfolioVerdict(d); } catch (e) {}") == 1

    def test_83_the_ui_derives_no_portfolio_verdict_of_its_own(self):
        """The renderer may FORMAT the owner's numbers; it must never compute a hurdle,
        a turnover budget or a decision state."""
        ui = _ui()
        start = ui.index("function _wsRenderPortfolioVerdict(d)")
        body = ui[start:ui.index("\nwindow._wsRenderPortfolioVerdict", start)]
        for forbidden in ("herfindahl", "* nav", "cost_rate", "0.05", "0.35"):
            assert forbidden not in body, forbidden
        # Every displayed state token comes from the backend payload.
        assert "cd.state" in body and "cd.headline" in body

    def test_84_the_verdict_never_synthesises_an_approve_or_order_control(self):
        ui = _ui()
        start = ui.index("function _wsRenderPortfolioVerdict(d)")
        body = ui[start:ui.index("\nwindow._wsRenderPortfolioVerdict", start)]
        # The words may appear in SAFETY copy ("nothing is approved"); what must not
        # exist is a control that performs any of them.
        for forbidden in ("onclick=\"approve", "dispatchCanonicalPrimaryAction",
                          "createOrder", "confirmTarget", "recordDecision",
                          "CONFIRM_PORTFOLIO_REBALANCE_DECISION"):
            assert forbidden not in body, forbidden
        # The only affordance is navigation, gated on the CANONICAL owner's flag.
        assert "cd.operator_action_available" in body
        assert "navigateToRoute(" in body

    def test_85_change_candidate_wording_is_accurate_never_no_change_recommended(self):
        ui = _ui()
        start = ui.index("function _wsRenderPortfolioVerdict(d)")
        body = ui[start:ui.index("\nwindow._wsRenderPortfolioVerdict", start)]
        assert "no proposal cleared the gates" in body
        assert "No portfolio change recommended" not in body

    def test_86_the_money_lane_no_longer_labels_a_membership_diff_as_proposed(self):
        ui = _ui()
        assert "_r29Metric('Proposed changes'" not in ui
        assert "'Portfolio changes proposed by this close" not in ui

    def test_87_no_alert_or_confirm_dialog_is_introduced(self):
        ui = _ui()
        start = ui.index("function _wsRenderPortfolioVerdict(d)")
        body = ui[start:ui.index("\nwindow._wsRenderPortfolioVerdict", start)]
        assert "alert(" not in body and "confirm(" not in body

    def test_88_the_today_status_row_is_one_balanced_full_width_area(self):
        ui = _ui()
        assert "grid-template-columns: repeat(12, minmax(0, 1fr));" in ui
        assert "#cc-root > #cc-dc-card    { order: 20; grid-column: span 5; }" in ui
        assert "#cc-root > #cc-dag-card   { order: 21; grid-column: span 7; }" in ui


# =========================================================================== #
# 10. The strict architecture audit protects all of the above
# =========================================================================== #
class TestStrictArchitectureAudit:

    def test_90_strict_audit_exits_zero(self):
        import subprocess
        import sys
        root = Path(__file__).resolve().parents[1]
        r = subprocess.run([sys.executable, str(root / "scripts" / "audit_architecture.py"),
                            "--strict"], capture_output=True, text=True, cwd=str(root))
        assert r.returncode == 0, r.stdout[-4000:]

    def test_91_the_release29_3_section_is_reported_and_blocking(self):
        import importlib.util
        import sys
        root = Path(__file__).resolve().parents[1]
        spec = importlib.util.spec_from_file_location(
            "_aa_r293", root / "scripts" / "audit_architecture.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules["_aa_r293"] = mod
        spec.loader.exec_module(mod)
        keys = {k for k, _f, _v in mod.BLOCKING_INVARIANTS}
        assert "release29_3_decision_integrity" in keys
        fields = {f for k, f, _v in mod.BLOCKING_INVARIANTS
                  if k == "release29_3_decision_integrity"}
        for required in ("constraint_codes_agree", "semantic_check_wired",
                         "withheld_not_approvable", "close_normalises_history",
                         "mandatory_exit_policy_owner_count", "ui_hero_scoped"):
            assert required in fields, required


# =========================================================================== #
# Kernel harness (hermetic). It DELEGATES to the Stage-20 suite's own scenario
# builders so the two suites can never drift apart on what a valid input contract is.
# =========================================================================== #
def _stage20():
    import importlib
    import sys
    tests_dir = str(Path(__file__).resolve().parent)
    if tests_dir not in sys.path:
        sys.path.insert(0, tests_dir)
    return importlib.import_module("test_stage20_active_reassessment")


def _review(tk, **kw):
    return _stage20()._review(tk, **kw)          # noqa: SLF001


def _replace_row(tk, *, gross=0.90, net=0.85, **kw):
    return _stage20()._replace_row(tk, gross=gross, net=net, **kw)   # noqa: SLF001


def _run_reassessment(reviews, *, policy=None):
    s20 = _stage20()
    kw = {"hoc": s20._hoc(reviews=s20._book(list(reviews)))}   # noqa: SLF001
    if policy:
        kw["policy"] = policy
    return s20._res(**kw)                                       # noqa: SLF001
