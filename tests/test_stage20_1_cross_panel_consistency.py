r"""Stage 20.1 — CROSS-PANEL STATE CONSISTENCY of the hermetic acceptance environment.

THE DEFECT THIS PINS
====================
Stage 20's acceptance seeding wrote ONE store (the portfolio-reassessment artifact) into an
otherwise EMPTY acceptance root. Every other canonical surface read its own empty store and
fell back to an unrelated default world, so a single rendered page could claim, all at once:

    Operator Command ......... RUN THE DAILY CLOSE          (empty world: no owned data)
    Active Assessment ........ PROPOSAL_READY               (the ONE seeded artifact)
    ... with a visible REVIEW PORTFOLIO PROPOSAL button     (no execution context)
    Holding Opportunity Cost . NOT_RUN                      (empty store)
    Reallocation ............. NOT_RUN                      (empty store)
    Controlled Rebalance ..... NO_PROPOSAL_YET              (empty store)
    Operational Book ......... NOT INITIALIZED, 0 pending, 0 fills   (empty ledgers)

— two live mutation CTAs describing two different worlds, while the real book had 29
SUBMITTED orders pending.

Stage 20.1 makes ONE ``World`` the sole source of truth per scenario and derives every
panel from it through the REAL canonical owners. These tests assert the panels agree.

Every test is hermetic: temp roots only, no live store, no provider, no prediction service,
no port, no write outside ``tmp_path``. The live 29-order rebalance is never touched.
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import pytest

from paper_trader.api import operational_book as ob
from paper_trader.api import paper_trading_desk as desk
from paper_trader.api import rebalance_execution as rbx
from paper_trader.engine import portfolio_reassessment as kernel

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import stage20_ui_fixtures as fx           # noqa: E402
import stage20_acceptance_server as srv    # noqa: E402

S5 = "scenario_5_execution_pending"
S5B = "scenario_5b_execution_pending_close_due"


@pytest.fixture(autouse=True)
def _no_store_env_leak():
    """STRUCTURAL GUARD. This suite is the only one that touches the hermetic server's
    store-redirection, which mutates the process environment by design. If a single test
    forgets to put it back, every LATER test file in the session silently reads empty
    stores — the failure surfaces far away from the cause (it landed on
    test_slice4_universe_scoring.py). Restore unconditionally, per test."""
    before = {v: os.environ.get(v) for v in srv._STORE_ENV_VARS}
    try:
        yield
    finally:
        for var, prior in before.items():
            if prior is None:
                os.environ.pop(var, None)
            else:
                os.environ[var] = prior


@pytest.fixture(scope="module")
def composed():
    """Compose every scenario ONCE (the composition is deterministic and read-only)."""
    root = Path(__file__).resolve().parent.parent / ".pytest_cache" / "stage20_1"
    return {k: fx.compose(k, root=root / k) for k in fx.SCENARIO_KEYS}


def _op(payload):
    return payload.get("operational_book") or {}


def _lineage(payload):
    return (_op(payload).get("pending_orders") or {}).get("current_rebalance") or {}


# =========================================================================== #
# A. THE SHARED SCENARIO CONTRACT (Workstream B)
# =========================================================================== #
class TestSharedScenarioContract:
    def test_1_every_scenario_declares_one_world(self):
        for key, spec in fx.scenarios().items():
            assert spec["scenario_id"] == key
            assert spec["owner"] == fx.SCENARIO_OWNER
            for field in ("book_id", "eligible_market_date", "execution", "close",
                          "current_plan", "superseded_plan", "expect"):
                assert field in spec, "%s is missing %s" % (key, field)

    def test_2_one_scenario_owner_for_the_whole_environment(self):
        assert fx.SCENARIO_OWNER == "scripts/stage20_ui_fixtures.py"

    def test_3_execution_vocabulary_is_closed(self):
        assert {s["execution"] for s in fx.scenarios().values()} <= {
            "NONE", "PENDING", "EXECUTED"}

    def test_4_close_vocabulary_is_closed(self):
        assert {s["close"] for s in fx.scenarios().values()} <= {"PROCESSED", "DUE"}

    def test_5_every_panel_is_produced_for_every_scenario(self, composed):
        expected = {"portfolio_state", "operational_book", "rebalance",
                    "holding_opportunity_cost", "reallocation_proposal",
                    "portfolio_reassessment", "daily_action_gate", "workflow_state"}
        for key, c in composed.items():
            assert set(c["panels"]) == expected, key

    def test_6_composition_is_deterministic(self, tmp_path):
        a = fx.compose(S5, root=tmp_path / "a")["consistency"]
        b = fx.compose(S5, root=tmp_path / "b")["consistency"]
        assert a == b

    def test_7_every_scenario_is_cross_panel_consistent(self, composed):
        for key, c in composed.items():
            assert c["consistency"]["consistent"], (key, c["consistency"]["violations"])

    def test_8_the_checker_is_not_vacuous(self, composed):
        """A deliberately incoherent panel set MUST be reported, not passed."""
        c = composed[S5]
        broken = dict(c["panels"])
        broken["portfolio_reassessment"] = dict(
            broken["portfolio_reassessment"],
            execution_precedence={"execution_active": False})
        verdict = fx.cross_panel_consistency(broken, fx.scenarios()[S5])
        assert not verdict["consistent"]
        assert any("EXECUTION_PRECEDENCE_MISMATCH" in v for v in verdict["violations"])


# =========================================================================== #
# B. SCENARIO 5 — the live shape (Workstream B/D/F)
# =========================================================================== #
class TestScenario5ExecutionPending:
    def test_9_rebalance_state_is_execution_pending(self, composed):
        assert composed[S5]["panels"]["rebalance"]["rebalance_state"] == (
            rbx.RB_PLAN_CONFIRMED)
        assert rbx.RB_PLAN_CONFIRMED == "ORDER_PLAN_CONFIRMED_PAPER_EXECUTION_PENDING"

    def test_10_operational_book_is_initialized(self, composed):
        op = _op(composed[S5]["panels"]["operational_book"])
        assert op["initialized"] is True
        assert op["holdings_count"] == 25
        assert (composed[S5]["panels"]["portfolio_state"]["active_book"]["initialized"]
                is True)

    def test_11_never_renders_not_initialized_or_initialize_book(self, composed):
        op = _op(composed[S5]["panels"]["operational_book"])
        assert op.get("not_initialized_note") is None
        assert op["next_action_code"] != "INITIALIZE_ALPHA_BOOK"
        assert (op.get("canonical_state") or {}).get("next_action_code") != (
            "INITIALIZE_ALPHA_BOOK")

    def test_12_current_rebalance_is_29_submitted_15_buy_14_sell(self, composed):
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["submitted_count"] == 29
        assert lin["buy_count"] == 15
        assert lin["sell_count"] == 14

    def test_13_current_plan_has_zero_fills(self, composed):
        assert _lineage(composed[S5]["panels"]["operational_book"])["filled_count"] == 0

    def test_14_current_plan_identity_matches_the_live_plan(self, composed):
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["order_plan_id"] == fx.LIVE_PLAN
        assert lin["order_plan_hash"] == fx.LIVE_PLAN_HASH

    def test_15_pending_order_count_is_29_not_zero(self, composed):
        op = _op(composed[S5]["panels"]["operational_book"])
        assert op["pending_order_count"] == 29

    def test_16_execution_precedence_is_true(self, composed):
        prsp = composed[S5]["panels"]["portfolio_reassessment"]
        assert prsp["execution_precedence"]["execution_active"] is True
        assert prsp["execution_precedence"]["reassessment_outranked"] is True

    def test_17_reassessment_stays_readable_as_evidence(self, composed):
        assert composed[S5]["panels"]["portfolio_reassessment"]["state"] == (
            kernel.STATE_PROPOSAL_READY)

    def test_18_proposal_action_is_suppressed(self, composed):
        pres = composed[S5]["panels"]["portfolio_reassessment"]["presentation"]
        assert pres.get("primary_action") != "REVIEW_PORTFOLIO_PROPOSAL"

    def test_19_no_daily_close_action_without_a_new_eligible_close(self, composed):
        primary = composed[S5]["panels"]["workflow_state"]["primary_action"]
        assert primary["action_code"] != "RUN_DAILY_CLOSE"
        assert not primary.get("execution_available")

    def test_20_zero_mutation_actions_across_the_whole_page(self, composed):
        cons = composed[S5]["consistency"]
        assert cons["mutation_action_count"] == 0, cons["mutation_actions"]

    def test_21_no_panel_reports_no_proposal_yet(self, composed):
        assert composed[S5]["panels"]["rebalance"]["rebalance_state"] != (
            rbx.RB_NO_PROPOSAL)

    def test_22_hoc_and_reallocation_are_not_NOT_RUN(self, composed):
        p = composed[S5]["panels"]
        assert p["holding_opportunity_cost"]["state"] != "NOT_RUN"
        assert p["reallocation_proposal"]["state"] != "NOT_RUN"

    def test_23_gate_agrees_with_the_hoc_and_reallocation_panels(self, composed):
        p = composed[S5]["panels"]
        gate = p["daily_action_gate"]
        assert gate["opportunity_cost_available"] is True
        assert gate["reallocation_proposal_available"] is True
        assert gate["reallocation_proposal_hash"] == (
            p["reallocation_proposal"]["proposal_hash"])


# =========================================================================== #
# C. LINEAGE COHORTS (Workstream D)
# =========================================================================== #
class TestLineageAwareCounts:
    def test_24_historical_fills_are_a_separate_labelled_cohort(self, composed):
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["historical_implementation_fill_count"] == 25
        assert lin["filled_count"] == 0

    def test_25_superseded_defective_plan_is_a_separate_cohort(self, composed):
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["superseded_order_count"] == 22
        assert lin["superseded_plan_ids"] == [fx.SUPERSEDED_PLAN]
        assert lin["cancelled_count"] == 0, "cancelled orders belong to the OLD plan"

    def test_26_counts_are_declared_lineage_scoped(self, composed):
        for key in (S5, S5B):
            assert _lineage(composed[key]["panels"]["operational_book"])[
                "counts_are_lineage_scoped"] is True

    def test_27_three_cohorts_never_sum_into_one_unlabelled_number(self, composed):
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["order_count"] == 29, "the current cohort is ONLY the current plan"
        assert lin["historical_implementation_order_count"] == 25
        assert lin["superseded_order_count"] == 22

    def test_28_executed_scenario_moves_fills_into_the_current_cohort(self, composed):
        lin = _lineage(composed["scenario_1_portfolio_current"]["panels"]
                       ["operational_book"])
        assert lin["filled_count"] == 29
        assert lin["submitted_count"] == 0
        assert lin["historical_implementation_fill_count"] == 25

    def test_29_no_rebalance_scenario_has_no_current_cohort(self, composed):
        lin = _lineage(composed["scenario_3_proposal_review"]["panels"]
                       ["operational_book"])
        assert lin["submitted_count"] == 0 and lin["filled_count"] == 0
        assert lin["historical_implementation_fill_count"] == 25


# =========================================================================== #
# D. DAILY CLOSE SEMANTICS — 5 vs 5b (Workstream C)
# =========================================================================== #
class TestDailyCloseSemantics:
    def test_30_scenario_5b_exists_and_is_distinct(self):
        assert S5B in fx.SCENARIO_KEYS
        assert fx.scenarios()[S5B]["close"] == "DUE"
        assert fx.scenarios()[S5]["close"] == "PROCESSED"

    def test_31_5b_execution_is_still_pending(self, composed):
        lin = _lineage(composed[S5B]["panels"]["operational_book"])
        assert lin["submitted_count"] == 29 and lin["filled_count"] == 0
        assert composed[S5B]["panels"]["rebalance"]["rebalance_state"] == (
            rbx.RB_PLAN_CONFIRMED)

    def test_32_5b_daily_close_is_the_one_primary_action(self, composed):
        primary = composed[S5B]["panels"]["workflow_state"]["primary_action"]
        assert primary["action_code"] == "RUN_DAILY_CLOSE"
        assert primary["execution_available"] is True
        assert composed[S5B]["consistency"]["mutation_action_count"] == 1

    def test_33_5b_close_explains_that_it_settles_eligible_next_close_orders(self,
                                                                            composed):
        text = composed[S5B]["panels"]["workflow_state"]["primary_action"]["explanation"]
        assert "Daily Close" in text

    def test_34_5b_proposal_review_stays_suppressed(self, composed):
        pres = composed[S5B]["panels"]["portfolio_reassessment"]["presentation"]
        assert pres.get("primary_action") != "REVIEW_PORTFOLIO_PROPOSAL"
        assert composed[S5B]["panels"]["portfolio_reassessment"][
            "execution_precedence"]["execution_active"] is True

    def test_35_the_two_scenarios_are_never_conflated(self, composed):
        a = composed[S5]["panels"]["workflow_state"]["primary_action"]["action_code"]
        b = composed[S5B]["panels"]["workflow_state"]["primary_action"]["action_code"]
        assert a != b

    def test_36_pending_orders_alone_never_manufacture_a_close_action(self, composed):
        """The close is due because a NEW eligible session exists — not because orders
        are pending. Stage 19.3's resolver decides; this pins the fixture to it."""
        from paper_trader.api import daily_close as dc
        passive = dc.resolve_daily_close_status(
            initialized=True, book_active=False, forward_tracking=True, pending_orders=29,
            latest_eligible=fx.DATE, last_processed_date=fx.DATE,
            processed_decision_for_latest=dc.DECISION_ORDERS_PENDING)
        due = dc.resolve_daily_close_status(
            initialized=True, book_active=False, forward_tracking=True, pending_orders=29,
            latest_eligible=fx.DATE, last_processed_date=fx.PREV,
            processed_decision_for_latest=None)
        assert passive == dc.PAPER_ORDERS_SUBMITTED
        assert due != dc.PAPER_ORDERS_SUBMITTED


# =========================================================================== #
# E. ONE PRIMARY ACTION (Workstream F)
# =========================================================================== #
class TestOnePrimaryAction:
    def test_37_at_most_one_mutation_action_in_every_scenario(self, composed):
        for key, c in composed.items():
            assert c["consistency"]["mutation_action_count"] <= 1, (
                key, c["consistency"]["mutation_actions"])

    def test_38_scenario_5_expects_exactly_zero(self, composed):
        assert composed[S5]["consistency"]["mutation_action_count"] == 0

    def test_39_the_forbidden_set_is_never_simultaneously_actionable(self, composed):
        forbidden = {"RUN_DAILY_CLOSE", "REVIEW_PORTFOLIO_PROPOSAL",
                     "INITIALIZE_ALPHA_BOOK", "REFRESH_DESK", "CONFIRM_REBALANCE"}
        for key, c in composed.items():
            offered = {m["action"] for m in c["consistency"]["mutation_actions"]}
            assert len(offered & forbidden) <= 1, (key, offered)

    def test_40_rebalance_confirmation_is_never_available_while_pending(self, composed):
        for key in (S5, S5B):
            assert not composed[key]["panels"]["rebalance"].get("confirmation_available")

    def test_41_each_scenario_matches_its_declared_expectation(self, composed):
        for key, c in composed.items():
            expected = c["expect"]["primary_action"]
            offered = [m["action"] for m in c["consistency"]["mutation_actions"]]
            if expected is None:
                assert offered == [], (key, offered)
            else:
                assert expected in offered, (key, offered)


# =========================================================================== #
# F. TRUTHFUL PROVENANCE (Workstream E)
# =========================================================================== #
class TestTruthfulProvenance:
    def test_42_no_proposal_is_invented_to_tidy_the_ui(self, composed):
        """Scenario 3 has no confirmed order plan, so no reallocation artifact exists
        and the panel says so honestly rather than fabricating one."""
        assert fx.scenarios()["scenario_3_proposal_review"]["execution"] == "NONE"
        assert fx._reallocation_artifact(
            fx.scenarios()["scenario_3_proposal_review"], fx.NAV) is None

    def test_43_hoc_provenance_matches_the_reassessment_input(self, composed):
        p = composed[S5]["panels"]
        assert p["daily_action_gate"]["opportunity_cost_assessment_hash"] == (
            composed[S5]["artifacts"]["hoc_assessment"]["assessment_hash"])

    def test_44_reallocation_binds_to_the_confirmed_plans_proposal_hash(self, composed):
        art = composed[S5]["artifacts"]["reallocation"]
        assert art["identity"]["proposal_hash"] == fx.LIVE_PROPOSAL_HASH
        lin = _lineage(composed[S5]["panels"]["operational_book"])
        assert lin["proposal_hash"] == fx.LIVE_PROPOSAL_HASH

    def test_45_data_blocked_scenario_declares_an_absent_input(self):
        spec = fx.scenarios()["scenario_4_data_blocked"]
        assert fx._research_inputs(spec)["market_as_of_date"] is None

    def test_46_reassessment_state_is_the_declared_one_everywhere(self, composed):
        for key, c in composed.items():
            assert c["panels"]["portfolio_reassessment"]["state"] == c["expect"]["state"]


# =========================================================================== #
# G. HERMETICITY + SAFETY
# =========================================================================== #
class TestHermeticSafety:
    def test_47_seeding_writes_only_inside_the_given_root(self, tmp_path):
        fx.seed_desk(fx.scenarios()[S5], tmp_path)
        fx.seed_ledger(fx.scenarios()[S5], tmp_path)
        written = [p for p in tmp_path.rglob("*") if p.is_file()]
        assert written
        for p in written:
            assert str(p).startswith(str(tmp_path))

    def test_48_seeded_ledgers_pass_their_real_chain_hash_verification(self, tmp_path):
        sdir = fx.seed_desk(fx.scenarios()[S5], tmp_path)
        report = desk.verify_all_ledgers(sdir)
        assert report["all_intact"] is True

    def test_48b_seeding_twice_into_one_root_is_idempotent(self, tmp_path):
        """The desk ledgers are APPEND-ONLY. The acceptance harness composes a scenario
        once to verify it and again to serve it, so a non-idempotent seeder would append a
        SECOND copy of the world: 50 historical fills, doubled holdings and negative cash."""
        spec = fx.scenarios()[S5]
        first = fx.seed_desk(spec, tmp_path)
        n1, o1 = len(desk._fills(first)), len(desk._orders_state(first))
        nav1 = desk.book_nav(desk.open_book(first), desk._fills(first),
                             desk.read_marks(first))
        second = fx.seed_desk(spec, tmp_path)
        assert len(desk._fills(second)) == n1 == 25
        assert len(desk._orders_state(second)) == o1
        nav2 = desk.book_nav(desk.open_book(second), desk._fills(second),
                             desk.read_marks(second))
        assert nav2["cash"] == nav1["cash"]
        assert nav2["cash"] > 0, "a doubled world produces nonsense negative cash"
        assert desk.verify_all_ledgers(second)["all_intact"] is True

    def test_49_acceptance_server_refuses_the_live_backend_port(self, capsys):
        rc = srv.main(["--scenario", S5, "--port", "8001"])
        assert rc == 2
        assert "REFUSED" in capsys.readouterr().err

    def test_50_acceptance_server_refuses_an_unknown_scenario(self, capsys):
        rc = srv.main(["--scenario", "scenario_does_not_exist", "--print-only"])
        assert rc == 2
        assert "REFUSED" in capsys.readouterr().err

    def test_51_acceptance_server_redirects_every_persistent_store(self, tmp_path):
        """redirect_stores() mutates the PROCESS environment on purpose — it is how the
        hermetic server detaches from the live stores. Exercising it inside pytest must
        therefore restore every variable, or the acceptance root leaks into every later
        test in the session and their real stores read back empty."""
        before = {v: os.environ.get(v) for v in srv._STORE_ENV_VARS}
        try:
            mapping = srv.redirect_stores(tmp_path)
            assert len(mapping) == len(srv._STORE_ENV_VARS)
            for path in mapping.values():
                assert str(path).startswith(str(tmp_path))
        finally:
            for var, prior in before.items():
                if prior is None:
                    os.environ.pop(var, None)
                else:
                    os.environ[var] = prior
        for var, prior in before.items():
            assert os.environ.get(var) == prior

    def test_52_live_operational_paths_are_never_referenced(self):
        src = (Path(__file__).resolve().parent.parent / "scripts"
               / "stage20_ui_fixtures.py").read_text(encoding="utf-8")
        assert "paper_trading_desk\\" not in src
        assert ".paper_trader" not in src
        assert "127.0.0.1:8001" not in src

    def test_53_fixtures_create_no_live_order_and_no_live_fill(self):
        """The fixture module writes synthetic ledger ROWS into a temp root through the
        append-only primitive. It never invokes an order-creating or settling entry point."""
        src = (Path(__file__).resolve().parent.parent / "scripts"
               / "stage20_ui_fixtures.py").read_text(encoding="utf-8")
        for forbidden in ("generate_orders(", "confirm_orders(", "settle_due_orders(",
                          "refresh_desk(", "confirm_order_plan(", "run_daily_close("):
            assert forbidden not in src, forbidden

    def test_54_safety_badges_survive_on_the_reassessment_panel(self, composed):
        badges = composed[S5]["panels"]["portfolio_reassessment"][
            "presentation"]["safety_badges"]
        assert "PREVIEW ONLY" in badges and "MANUAL REVIEW" in badges
        assert "NO LIVE ORDERS" in badges

    def test_55_no_scenario_enables_automation_or_a_broker(self, composed):
        for key, c in composed.items():
            payload = c["panels"]["operational_book"]
            assert payload.get("automation_enabled") in (False, None), key
            assert payload.get("broker_enabled") in (False, None), key
            assert payload.get("live_orders_enabled") in (False, None), key

    def test_56_the_composed_payloads_are_json_serialisable(self, composed):
        for key, c in composed.items():
            json.dumps(c["panels"], default=str)


# =========================================================================== #
# H. THE STAGE-20 REGRESSION ITSELF
# =========================================================================== #
class TestStage20RegressionIsClosed:
    def test_57_seeding_no_longer_leaves_other_panels_empty(self, tmp_path):
        """The Stage-20 seed wrote ONE store. Seeding now produces a coherent world."""
        info = fx.seed(reassessment_dir=tmp_path / "reassessment", scenario=S5)
        assert info["consistency"]["consistent"], info["consistency"]["violations"]
        assert info["consistency"]["current_rebalance"]["submitted_count"] == 29

    def test_58_seed_writes_every_artifact_store_not_just_the_reassessment(self,
                                                                          tmp_path):
        fx.seed(reassessment_dir=tmp_path / "reassessment", scenario=S5)
        for sub in ("reassessment", "hoc", "realloc", "decisions"):
            assert (tmp_path / sub / "index.json").exists(), sub
        assert (tmp_path / "desk" / desk.ORDERS_FILE).exists()
        assert (tmp_path / "mhz").exists()

    def test_59_the_seven_original_contradictions_are_all_gone(self, composed):
        p = composed[S5]["panels"]
        op = _op(p["operational_book"])
        lin = _lineage(p["operational_book"])
        assert op["initialized"] is True                     # was NOT INITIALIZED
        assert op["pending_order_count"] == 29               # was 0 pending orders
        assert lin["historical_implementation_fill_count"] == 25   # was 0 fills
        assert p["rebalance"]["rebalance_state"] == rbx.RB_PLAN_CONFIRMED  # was NO_PROPOSAL
        assert p["holding_opportunity_cost"]["state"] != "NOT_RUN"         # was NOT_RUN
        assert p["reallocation_proposal"]["state"] != "NOT_RUN"            # was NOT_RUN
        assert p["workflow_state"]["primary_action"]["action_code"] != "RUN_DAILY_CLOSE"
        assert composed[S5]["consistency"]["mutation_action_count"] == 0

    def test_59b_the_static_guard_is_registered_and_not_vacuous(self):
        """The Stage 20.1 audit guard must actually run AND actually be able to fail."""
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
        import audit_architecture as audit

        files = audit._iter_source_files()          # noqa: SLF001
        rep = audit.check_acceptance_scenario_ownership(files)
        assert rep["single_scenario_owner"] is True
        assert rep["missing_panels"] == [] and rep["missing_delegation"] == []
        registered = [inv for inv in audit.BLOCKING_INVARIANTS
                      if inv[0] == "acceptance_scenario_ownership"]
        assert len(registered) >= 17, "the guard must be BLOCKING, not merely reported"
        # Not vacuous: a report missing a panel must be reported as a failure.
        broken = dict(rep, missing_panels=["workflow_state"])
        failures = audit._blocking_invariant_failures(   # noqa: SLF001
            {"acceptance_scenario_ownership": broken})
        assert any("missing_panels" in f for f in failures)

    def test_60_production_lineage_owner_is_reused_not_reimplemented(self):
        """Stage 20.1 adds no second lineage calculation: the cohort split comes from
        api.operational_book.current_rebalance_lineage (Stage 19.3)."""
        src = (Path(__file__).resolve().parent.parent / "scripts"
               / "stage20_ui_fixtures.py").read_text(encoding="utf-8")
        assert "def current_rebalance_lineage" not in src
        assert callable(ob.current_rebalance_lineage)
