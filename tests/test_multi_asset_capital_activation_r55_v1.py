r"""MULTI_ASSET_CAPITAL_ACTIVATION_R55_V1 - the release's own regression.

WORKSTREAM A  proposal assurance: ONE risk-contribution field / threshold owner, the
              AFTER-target per-name gate (repaired, then judged, then withheld only
              when unrepairable), outcome-evidence economic dedup, workflow economics
              ownership, runtime identity for the persistent research worker.
WORKSTREAM C  the FX carry cadence HORIZON CONTRACT (label 1 / hold 5) and the
              fail-closed coherence guards in the producer and the accrual.
WORKSTREAM D  the capital-eligibility gate owner, its derivation into the registry,
              and the frontier's non-equity admission ledger.
WORKSTREAM E  the managed-futures time-series trend challenger: score, universe,
              validation, frozen record, prospective runtime and accrual seams.
WORKSTREAM F  the multi-asset acceptance: equity + cash + ONE qualified non-equity
              sleeve reach the frontier and the allocator; an UNQUALIFIED sleeve is
              excluded fail-closed; manual review only; no order, no fill.

Every test runs against private temp roots. Nothing reads the vendor, the live
research roots or the operational stores.
"""
from __future__ import annotations

import csv
import inspect
import json
import random
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pytest

from paper_trader.alpha_agent import alpha_recovery as AR
from paper_trader.alpha_agent.alpha_recovery import futures_trend_challenger as FTC
from paper_trader.alpha_agent.alpha_recovery import futures_trend_runtime as FTR
from paper_trader.alpha_agent.alpha_recovery import fx_carry_cadence_challenger as FXC
from paper_trader.alpha_agent.alpha_recovery import fx_carry_cadence_runtime as FXR
from paper_trader.alpha_agent.alpha_recovery import next_open_challenger as NOC
from paper_trader.alpha_agent.alpha_recovery import prospective_decision as PD
from paper_trader.alpha_agent.r46 import contract as R46C
from paper_trader.api import canonical_forward_accrual as CFA
from paper_trader.api import capital_eligibility_gate as CEG
from paper_trader.api import investability_registry as ir
from paper_trader.api import market_reference_data as mrd
from paper_trader.api import opportunity_frontier as of_api
from paper_trader.api import portfolio_reassessment as prs_api
from paper_trader.api import reallocation_proposal as arp
from paper_trader.api import reassessment_outcomes as RO
from paper_trader.api import research_runtime as rr
from paper_trader.api import runtime_identity as rid
from paper_trader.api import workflow_state as ws
from paper_trader.engine import constrained_reallocation as CR
from paper_trader.engine import cross_asset_risk as XR
from paper_trader.engine import holding_opportunity_cost as HOC
from paper_trader.engine import opportunity_frontier as OF
from paper_trader.engine import portfolio_reassessment as PRSK
from paper_trader.engine import reallocation_proposal as RP
from paper_trader.engine import reassessment_outcomes as ROK
from paper_trader.engine import zero_base_allocator as ZB

REPO = Path(__file__).resolve().parents[1]
DATE = "2026-09-17"


# =========================================================================== #
# helpers
# =========================================================================== #
def _rets(n, seed, scale=0.01):
    rnd = random.Random(seed)
    return [rnd.gauss(0.0, scale) for _ in range(n)]


def _aligned(tickers, n=90, big=None, big_scale=0.10):
    dates = ["d%03d" % i for i in range(n)]
    series = {}
    for i, tk in enumerate(tickers):
        series[tk] = _rets(n, 100 + i, big_scale if tk == big else 0.01)
    return {"dates": dates, "series": series}


def _urow(tk, rank, pct, sector="Tech", adv=5e8, **inst):
    return dict({"ticker": tk, "rank": rank, "percentile": pct, "combined_score": pct,
                 "sector": sector, "adv_dollar": adv, "eligible": True}, **inst)


def _review(tk, rec="HOLD", pct=0.5, rc=None):
    return {"ticker": tk, "recommendation": rec, "current_rank": 1, "current_score": pct,
            "signal_strength": pct, "strongest_replacement_ticker": None, "replacement_rank": None,
            "replacement_score": None, "gross_score_improvement": None, "net_improvement": None,
            "switching_cost_usd": None, "deterioration_state": "STABLE", "drawdown_60d": -0.05,
            "volatility_60d": 0.2, "liquidity_state": "LIQUID", "risk_contribution_pct": rc}


def _equity_world(n=25, big=None):
    sectors = ["S%d" % (i % 6) for i in range(n)]
    tickers = ["A%02d" % i for i in range(n)]
    if big:
        tickers[0] = big
    positions = [{"ticker": tk, "sector": sectors[i], "quantity": 100, "current_weight": 1.0 / n,
                  "market_value": 1e6 / n, "price": 100.0} for i, tk in enumerate(tickers)]
    rows = [_urow(tk, i + 1, 1 - i / 60.0, sector=sectors[i]) for i, tk in enumerate(tickers)]
    rows += [_urow("N%02d" % i, n + 1 + i, 1 - (n + i) / 60.0, sector=sectors[i % 6]) for i in range(n)]
    return positions, rows, tickers


def _contract(*, positions, rows, reviews, aligned, nav=1e6):
    return {"schema_version": RP.INPUT_SCHEMA_VERSION, "eligible_market_date": DATE,
            "active_book_id": "b", "nav": nav, "cash": nav * (1 - sum(p["current_weight"] for p in positions)),
            "portfolio_state_hash": "ps", "universe_scoring_hash": "us", "hoc_assessment_hash": "hoc",
            "hoc_assessment_state": "READY", "hoc_available": True, "hoc_data_gaps": [],
            "positions": positions, "hoc_reviews": reviews, "universe_rows": rows,
            "aligned_returns": aligned}


# =========================================================================== #
# A1 / A2 - ONE risk-contribution contract
# =========================================================================== #
class TestRiskContributionContract:

    def test_one_field_one_limit_owner(self):
        assert HOC.RISK_CONTRIBUTION_FIELD == "risk_contribution_pct"
        assert HOC.review_risk_contribution({"risk_contribution_pct": 0.13}) == 0.13
        # the legacy spelling the repair kernel used to read is NOT a contract field
        assert HOC.review_risk_contribution({"risk_contribution": 0.13}) is None
        assert HOC.risk_contribution_limit(n_covariance_names=25)["limit"] == pytest.approx(0.12)
        assert HOC.risk_contribution_limit(n_covariance_names=24)["limit"] == pytest.approx(0.125)
        assert HOC.risk_contribution_limit(n_covariance_names=0)["limit"] is None
        # the repair kernel's default MIRRORS the owner at the default N; it forks nothing
        assert CR.default_policy()["max_name_risk_contribution"] == pytest.approx(
            HOC.risk_contribution_limit(n_covariance_names=25)["limit"])
        assert RP.default_policy()["risk_contribution_excess_multiple"] == \
            HOC.default_policy()["risk_contribution_excess_multiple"]
        # the constraint inventory names the owner
        inv = {r["code"]: r for r in CR.constraint_inventory(CR.default_policy())["constraints"]}
        assert inv[CR.C_RISK_CONTRIBUTION]["owner"] == "engine.holding_opportunity_cost"

    def test_publishers_and_consumers_spell_the_same_key(self):
        rp_src = Path(RP.__file__).read_text(encoding="utf-8")
        assert 'r.get("risk_contribution")' not in rp_src        # the 2026-09-17 defect
        assert "review_risk_contribution(" in rp_src
        assert "risk_contribution_limit(" in rp_src
        of_src = Path(of_api.__file__).read_text(encoding="utf-8")
        assert '"risk_contribution_pct"' in of_src
        # the HOC review row publishes the canonical key and the applied limit
        assert '"risk_contribution_pct"' in Path(HOC.__file__).read_text(encoding="utf-8")

    def test_repair_inputs_prefer_the_after_target_shares(self):
        measured = {"risk": {"risk_contributions_after": {"A": 0.3, "B": 0.1},
                             "risk_contribution_limit_after": {"limit": 0.12}}}
        rc = RP._repair_risk_inputs(measured=measured, hoc_reviews=[_review("A", rc=0.9)])
        assert rc["source"] == RP.RC_SOURCE_AFTER_TARGET
        assert rc["contributions"] == {"A": 0.3, "B": 0.1} and rc["limit"] == 0.12
        # no after-target shares -> the held book's rows, read through the ONE adapter
        rc2 = RP._repair_risk_inputs(measured={"risk": {}},
                                     hoc_reviews=[_review("A", rc=0.9), {"ticker": "B", "risk_contribution": 0.5}])
        assert rc2["source"] == RP.RC_SOURCE_HOC_HELD_BOOK and rc2["contributions"] == {"A": 0.9}

    def test_repair_kernel_verification_is_honest_about_what_it_can_check(self):
        cands = [{"ticker": "A", "sector": "T", "adv_dollar": 1e9, "score": 0.9, "rank": 1}]
        sol = CR.solve_feasible_target(current_weight={}, ideal_weight={"A": 0.05}, candidates=cands,
                                       nav=1e6)
        ver = sol["verification"]
        assert CR.C_RISK_CONTRIBUTION not in ver["checked"]
        assert ver["re_measured_by_covariance_owner"] == [CR.C_RISK_CONTRIBUTION]


# =========================================================================== #
# A3 - the AFTER-target gate
# =========================================================================== #
class TestAfterTargetRiskGate:

    def _world(self):
        positions, rows, tickers = _equity_world(25, big="BIG")
        reviews = [_review(tk) for tk in tickers]
        aligned = _aligned(tickers + ["N%02d" % i for i in range(25)], big="BIG", big_scale=0.12)
        return _contract(positions=positions, rows=rows, reviews=reviews, aligned=aligned)

    def test_the_target_is_measured_repaired_and_judged_again(self):
        out = RP.build_proposal(input_contract=self._world(), policy=arp.resolve_policy())
        risk = out["risk"]
        assert risk["risk_contribution_policy_owner"] == "engine.holding_opportunity_cost"
        lim = risk["risk_contribution_limit_after"]
        # the limit follows the covariance universe of the TARGET being judged (3/N_after)
        assert lim["n_covariance_names"] in (24, 25)
        assert lim["limit"] == pytest.approx(3.0 / lim["n_covariance_names"])
        ideal = out["constraint_reoptimization"]["ideal_limits"]
        assert RP.CT_RISK_CONTRIBUTION in ideal["withheld_codes"]
        assert "BIG" in next(b for b in ideal["breaches"] if b["code"] == RP.CT_RISK_CONTRIBUTION)["value"]
        assert out["constraint_reoptimization"]["applied"] is True
        assert CR.C_RISK_CONTRIBUTION in out["constraint_reoptimization"]["constraints_that_reshaped"]
        assert out["constraint_reoptimization"]["risk_contribution_inputs"]["source"] == RP.RC_SOURCE_AFTER_TARGET
        # the FINAL target honours the same governed policy the held book was judged by
        assert out["complete_target_limits"]["risk_contribution_breaches"] == []
        assert out["risk_contribution_policy"]["after_target_breaches"] == []
        assert out["risk_contribution_policy"]["after_target_gate_evaluated"] is True
        assert out["proposal_state"] != RP.STATE_WITHHELD
        big = next(a for a in out["allocations"] if a["ticker"] == "BIG")
        assert big["proposed_weight"] < 1.0 / 25
        assert RP.CT_RISK_CONTRIBUTION in RP.COMPLETE_TARGET_CONSTRAINT_CODES
        assert set(RP.COMPLETE_TARGET_CONSTRAINT_CODES) == set(PRSK.COMPLETE_TARGET_CONSTRAINT_CODES)

    def test_an_unrepairable_target_is_withheld_never_approved(self, monkeypatch):
        world = self._world()

        def _no_feasible(**kw):
            return {"feasible": False, "best_feasible_target": {}, "constraint_adjustments": [],
                    "constraints_that_reshaped": [], "mandatory_exits": [], "released_weight": 0.0,
                    "redistributed_weight": 0.0, "turnover": {}, "verification": {"valid": False,
                    "violations": [{"code": CR.C_RISK_CONTRIBUTION}]}, "solution_hash": "x",
                    "blockers": [{"code": CR.B_NO_FEASIBLE_PORTFOLIO, "kind": CR.KIND_TRUE_BLOCKER}]}
        monkeypatch.setattr(RP._cr, "solve_feasible_target", _no_feasible)
        out = RP.build_proposal(input_contract=world, policy=arp.resolve_policy())
        assert out["proposal_state"] == RP.STATE_WITHHELD
        assert out["approvable"] is False
        assert RP.CT_RISK_CONTRIBUTION in out["complete_target_limits"]["withheld_codes"]

    def test_evaluate_limits_reads_the_risk_block_and_never_recomputes(self):
        risk = {"risk_contribution_breaches_after": [{"ticker": "X", "risk_contribution_pct": 0.2,
                                                      "limit": 0.12, "excess": 0.08}],
                "risk_contribution_limit_after": {"limit": 0.12},
                "risk_contribution_policy_owner": "engine.holding_opportunity_cost"}
        lim = RP.evaluate_complete_target_limits(turnover={}, risk=risk, policy=RP.default_policy())
        assert lim["withheld"] and lim["withheld_codes"] == [RP.CT_RISK_CONTRIBUTION]
        assert lim["risk_contribution_limit"] == 0.12
        clean = RP.evaluate_complete_target_limits(turnover={}, risk={}, policy=RP.default_policy())
        assert clean["withheld"] is False

    def test_dilution_never_opens_a_position_beyond_the_count_cap(self):
        # the latent kernel defect the gate exposed: a 1% dilution transfer into a NEW
        # name when the book already held the maximum made the solver verify its own
        # target infeasible (MAX_POSITION_COUNT) and withhold a feasible decision.
        cands = [{"ticker": t, "sector": "T", "adv_dollar": 1e9, "score": s, "rank": i + 1}
                 for i, (t, s) in enumerate([("A", 0.9), ("B", 0.8), ("C", 0.7), ("D", 0.6), ("E", 0.5)])]
        pol = dict(CR.default_policy(), target_position_count=4, max_name_weight=0.5,
                   sector_cap_fraction=1.0, max_concentration_increase=0.0)
        sol = CR.solve_feasible_target(current_weight={"A": 0.25, "B": 0.25, "C": 0.25, "D": 0.25},
                                       ideal_weight={"A": 0.45, "B": 0.25, "C": 0.15, "D": 0.15},
                                       candidates=cands, nav=1e6, policy=pol)
        assert sol["feasible"] is True, sol["verification"]["violations"]
        assert len([v for v in sol["best_feasible_target"].values() if v > 0]) <= 4


# =========================================================================== #
# A4 - evidence dedup
# =========================================================================== #
def _obs(oid, fp, rec_at, spread=0.01):
    return {"observation_id": oid, "reassessment_id": "prs_1", "reassessment_hash": "rh1",
            "active_book_id": "b", "eligible_market_date": "2026-08-17", "ticker": "SNDK",
            "recommendation": "REPLACE", "horizon_eligible_closes": 20, "realized_spread": spread,
            "recorded_at": rec_at, "identity": {"evidence_fingerprint": fp}, "maturity": ROK.MAT_MATURE}


class TestEvidenceDedup:

    def test_economic_identity_collapses_repeated_captures_and_keeps_history(self):
        rows = [_obs("o1", "fp1", "2026-09-01T00:00:00+00:00"),
                _obs("o2", "fp2", "2026-09-02T00:00:00+00:00"),
                _obs("o3", "fp3", "2026-09-03T00:00:00+00:00", spread=0.02)]
        d = ROK.deduplicate_observations(rows)
        assert d["distinct_economic_observations"] == 1 and d["duplicate_rows"] == 2
        assert d["rows"][0]["observation_id"] == "o1"           # first recorded wins
        assert d["conflicts"] and d["conflicts"][0]["observation_id"] == "o3"
        assert d["history_preserved"] is True and d["max_multiplicity"] == 3

    def test_the_fingerprint_is_a_source_identity_not_a_store_state(self):
        a = RO.evidence_source_fingerprint(price_source="p.json", evidence_owner="o", horizons=[1, 5])
        b = RO.evidence_source_fingerprint(price_source="p.json", evidence_owner="o", horizons=[5, 1])
        assert a == b
        src = Path(RO.__file__).read_text(encoding="utf-8")
        assert '"updated_at": store.get("updated_at")' not in src

    def test_capture_is_idempotent_across_fingerprint_changes(self, tmp_path):
        import tests.test_stage21_outcome_intelligence as S21
        kw = S21._built(tmp_path)
        c1 = RO.capture_matured_outcomes(**kw)
        assert c1["observations_newly_matured"] > 0
        # a refreshed store used to mint a new fingerprint and re-append every row
        kw2 = dict(kw)
        kw2["evidence"] = dict(kw["evidence"], evidence_fingerprint="a-different-fingerprint")
        c2 = RO.capture_matured_outcomes(**kw2)
        assert c2["observations_newly_matured"] == 0
        assert c2["economically_identical_skipped_this_run"] == c1["observations_newly_matured"]
        assert c2["observations_total"] == c1["observations_total"]
        assert c2["distinct_economic_observations"] == c2["observations_total"]
        hist = RO.load_outcome_history(outcome_dir=kw["outcome_dir"])
        assert hist["duplicate_rows_preserved"] == 0
        assert all(r["is_first_recorded_for_economic_identity"] for r in hist["rows"])

    def test_governance_reads_the_deduplicated_view(self, tmp_path):
        rows = [_obs("o%d" % i, "fp%d" % i, "2026-09-%02dT00:00:00+00:00" % (i + 1)) for i in range(9)]
        out = RO.load_reassessment_outcomes(observations=rows, history=[], evidence={"series": {}, "calendar": [],
                                                                                    "horizons": [20]},
                                            lineage={"latest_completed_rebalance": None})
        assert out["evidence_store"]["persisted_rows"] == 9
        assert out["evidence_store"]["distinct_economic_observations"] == 1
        assert out["scorecard"]["observations_total"] == 1


# =========================================================================== #
# A5 - workflow economics ownership
# =========================================================================== #
class TestWorkflowEconomics:

    def test_the_proposal_owns_the_numbers_when_a_proposal_exists(self):
        prs = {"expected_net_improvement": 0.0, "expected_one_way_turnover": 0.0,
               "expected_transaction_cost_usd": 0.0}
        lane = {"proposal_hash": "3f0b", "proposal_available": True,
                "score_improvement_net_of_cost": 0.054047, "one_way_turnover": 0.35,
                "estimated_transaction_cost": 86.15}
        econ = ws.governed_proposal_economics(reassessment_summary=prs, portfolio_decision_lane=lane)
        assert econ["economics_binding"] is True
        assert econ["expected_net_improvement"] == 0.054047
        assert econ["expected_one_way_turnover"] == 0.35
        assert econ["expected_transaction_cost_usd"] == 86.15
        assert econ["reassessment_pre_proposal_estimate"]["expected_net_improvement"] == 0.0
        assert econ["reassessment_pre_proposal_estimate"]["non_binding"] is True
        none = ws.governed_proposal_economics(reassessment_summary=prs, portfolio_decision_lane={})
        assert none["economics_binding"] is False and none["expected_net_improvement"] == 0.0

    def test_the_canonical_decision_and_the_card_carry_the_owner(self):
        d = ws.build_canonical_portfolio_decision(
            reassessment_summary={"reassessment_state": "PROPOSAL_READY", "proposal_required": True,
                                  "expected_net_improvement": 0.0, "expected_one_way_turnover": 0.0,
                                  "expected_transaction_cost_usd": 0.0, "blockers": [],
                                  "mandatory_exit_tickers": [], "mandatory_exit_policy": {"obligation": "NONE"}},
            reallocation_operator_state=ws.RPS_READY,
            portfolio_decision_lane={"portfolio_decision_state": ws.PDS_REVIEW_REQUIRED,
                                     "requires_manual_review": True, "approvable": True,
                                     "proposal_hash": "3f0b", "proposal_available": True,
                                     "score_improvement_net_of_cost": 0.054, "one_way_turnover": 0.35,
                                     "estimated_transaction_cost": 86.15},
            attention_count=1, eligible_date=DATE)
        assert d["expected_net_improvement"] == 0.054 and d["expected_one_way_turnover"] == 0.35
        assert d["economics_binding"] is True
        assert d["economics_owner"] == ws.ECONOMICS_OWNER_PROPOSAL
        assert d["reassessment_pre_proposal_estimate"]["expected_net_improvement"] == 0.0
        card = prs_api.build_presentation(
            state="PROPOSAL_READY",
            reassessment={"decision": {"expected_net_improvement": 0.054, "economics_owner": "X",
                                       "economics_basis": "Y"}, "attention": {"count": 1}})
        assert card["economics_owner"] == "X" and card["economics_basis"] == "Y"


# =========================================================================== #
# A6 - runtime identity for the persistent research worker
# =========================================================================== #
class TestRuntimeIdentity:

    def test_the_research_worker_identity_is_read_from_what_it_wrote(self, monkeypatch):
        from paper_trader.alpha_agent.r46 import runlock as RL
        from paper_trader.alpha_agent.r59 import runtime as R59RT
        status = {"worker_state": "MATURING", "started_at": "2026-09-17T08:46:18+00:00",
                  "worker_identity": {"instance_id": "d83e", "pid": 23176, "host": "h",
                                      "started_at": "2026-09-17T08:46:18+00:00",
                                      "owner": "alpha_agent.r59.runtime",
                                      "source": {"commit": "47b9" * 10, "commit_short": "47b947b947b9",
                                                 "branch": "b", "dirty": False,
                                                 "resolved_from": "GIT_DIRECTORY_READ", "repo_root": "R"}}}
        monkeypatch.setattr(R59RT, "read_status", lambda: status)
        monkeypatch.setattr(RL, "state_path", lambda p: {"held": True, "pid": 23176, "pid_alive": True})
        out = rr.load_research_worker_identity()
        assert out["available"] is True
        assert out["loaded"]["commit"] == "47b9" * 10 and out["loaded"]["pid"] == 23176
        assert out["process"]["lease_held"] is True
        row = rid.classify_alignment(loaded=out["loaded"], source={"commit": "47b9" * 10})
        assert row["verdict"] == rid.ALIGNMENT_ALIGNED
        monkeypatch.setattr(R59RT, "read_status", lambda: {})
        assert rr.load_research_worker_identity()["loaded"] is None

    def test_the_readiness_route_serves_the_loaded_commit(self):
        app_src = (REPO / "api" / "app.py").read_text(encoding="utf-8")
        assert "loaded_commit: Optional[str] = None" in app_src
        assert 'loaded_commit=_ident.get("commit")' in app_src
        ps1 = (REPO / "scripts" / "restart_paper_trader_backend.ps1").read_text(encoding="utf-8")
        assert "LOADED RELEASE IDENTITY" in ps1 and "function Get-SourceCommit" in ps1
        assert "a stale runtime, not a current one" in ps1
        ams = (REPO / "api" / "active_manager_state.py").read_text(encoding="utf-8")
        assert "load_research_worker_identity()" in ams
        assert '{"runtime": rid.RUNTIME_RESEARCH, "loaded": None, "process": {}}' not in ams


# =========================================================================== #
# C - the FX horizon contract
# =========================================================================== #
FX_SCOPE = ["6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S", "DX"]
FX_REG = "2026-09-14"
FX_IDENT = {"challenger_id": FXC.CHALLENGER_ID, "freeze_record_hash": FXC.FREEZE_RECORD_HASH,
            "identity_hash": "fx-identity", "release": "ALPHA_RECOVERY_OFFENSIVE",
            "registration_session": FX_REG, "model_spec_hash": "spec", "horizon_sessions": 1}


@pytest.fixture()
def research_root(tmp_path, monkeypatch):
    monkeypatch.setenv(AR.RESEARCH_ROOT_ENV, str(tmp_path / "research"))
    monkeypatch.setattr(FXR, "cost_policy", lambda scope: {"bps_per_side": 2.0,
                                                           "per_market_bps_per_side": {m: 2.0 for m in scope}})
    monkeypatch.setattr(FTR, "cost_policy", lambda scope: {"bps_per_side": 2.0,
                                                           "per_market_bps_per_side": {m: 2.0 for m in scope}})
    return tmp_path


def _fx_registration(horizon=1):
    return {"challenger_id": FXC.CHALLENGER_ID, "registration_session": FX_REG,
            "freeze_record_hash": FXC.FREEZE_RECORD_HASH, "asset_class": "FX_FUTURES",
            "horizon_sessions": horizon, "instrument_scope": list(FX_SCOPE),
            "identity": {"challenger_id": FXC.CHALLENGER_ID, "release": "ALPHA_RECOVERY_OFFENSIVE",
                         "identity_hash": "fx-identity", "freeze_record_hash": FXC.FREEZE_RECORD_HASH},
            "observation_clock": {"first_eligible_observation_session": None}}


class TestFxHorizonContract:

    def test_the_contract_names_every_consumer_and_its_role(self):
        c = FXC.HORIZON_CONTRACT
        assert c["information_label_horizon_sessions"] == 1
        assert c["holding_horizon_sessions"] == c["evaluation_horizon_sessions"] == 5
        assert c["consumers"]["registration.horizon_sessions"]["role"] == FXC.HORIZON_ROLE_LABEL
        assert c["consumers"]["policy.evaluation_horizon_sessions"]["role"] == FXC.HORIZON_ROLE_HOLDING
        assert c["immutable_artifacts_rewritten"] is False and c["new_challenger_identity_created"] is False

    def test_the_live_shape_is_consistent_and_a_conflict_is_named(self):
        rec = {"forward_specification": {"horizon_sessions": 1, "trade_every_sessions": 5}}
        pol = {"evaluation_horizon_sessions": 5, "rebalance_cadence_sessions": 5,
               "execution_contract": {"holding_sessions": 5}}
        ok = FXC.horizon_coherence(record=rec, registration={"horizon_sessions": 1}, policy=pol,
                                   accrual={"horizon_sessions": 5})
        assert ok["state"] == FXC.HC_CONSISTENT and ok["conflicts"] == []
        assert ok["observed_consumers"] == 7
        bad = FXC.horizon_coherence(record=rec, policy=dict(pol, evaluation_horizon_sessions=1))
        assert bad["state"] == FXC.HC_INCOHERENT
        assert bad["conflicts"][0]["consumer"] == "policy.evaluation_horizon_sessions"
        assert FXC.horizon_coherence()["state"] == FXC.HC_UNOBSERVED

    def test_the_producer_refuses_to_freeze_under_an_incoherent_policy(self, research_root):
        decl = FXR.policy_declaration(FX_IDENT, scope=FX_SCOPE)
        decl["evaluation_horizon_sessions"] = 1                 # a tampered declaration
        assert PD.declare_policy(**decl, now="2026-09-14T20:00:00+00:00")["outcome"] == PD.FROZEN
        res = FXR.advance(now="2026-09-22T05:00:00+00:00", scorer=lambda p, w: {}, refresher=lambda k: {"ok": True})
        assert res["state"] == FXR.ST_BLOCKED
        assert res["horizon_contract"]["state"] == FXC.HC_INCOHERENT
        assert "horizon contract is incoherent" in res["detail"]
        assert PD.list_decisions(FXC.CHALLENGER_ID) == []

    def test_the_coherent_policy_advances_and_publishes_the_contract(self, research_root):
        assert PD.declare_policy(**FXR.policy_declaration(FX_IDENT, scope=FX_SCOPE),
                                 now="2026-09-14T20:00:00+00:00")["outcome"] == PD.FROZEN
        res = FXR.advance(now="2026-09-16T23:00:00+00:00", scorer=lambda p, w: {},
                          refresher=lambda k: {"ok": True})
        assert res["horizon_contract"]["state"] in (FXC.HC_CONSISTENT,)
        assert res["horizon_contract"]["canonical"]["evaluation_horizon_sessions"] == 5

    def test_the_accrual_publishes_the_roles_and_fails_closed_on_disagreement(self, research_root, tmp_path):
        assert PD.declare_policy(**FXR.policy_declaration(FX_IDENT, scope=FX_SCOPE),
                                 now="2026-09-14T20:00:00+00:00")["outcome"] == PD.FROZEN
        row = CFA.assess_registration(registration=_fx_registration(horizon=1), series={},
                                      now="2026-09-16T23:00:00+00:00", today="2026-09-16",
                                      store_dir_override=tmp_path / "acc")
        roles = row["horizon_roles"]
        assert roles["registration_horizon_sessions"] == 1
        assert roles["registration_horizon_role"] == CFA.HORIZON_ROLE_LABEL
        assert roles["evaluation_horizon_sessions"] == 5 and roles["coherent"] is True
        assert row["horizon_sessions"] == 5
        # a release whose declared holding period disagrees with its evaluation horizon
        bad = FXR.policy_declaration(dict(FX_IDENT, challenger_id="FX_BAD"), scope=FX_SCOPE)
        bad["challenger_id"] = "FX_BAD"
        bad["evaluation_horizon_sessions"] = 1
        PD.declare_policy(**bad, now="2026-09-14T20:00:00+00:00")
        reg = _fx_registration()
        reg["challenger_id"] = "FX_BAD"
        reg["identity"] = dict(reg["identity"], challenger_id="FX_BAD", identity_hash="fx-bad")
        blocked = CFA.assess_registration(registration=reg, series={}, now="2026-09-16T23:00:00+00:00",
                                          today="2026-09-16", store_dir_override=tmp_path / "acc")
        assert blocked["state"] == CFA.ACC_INTEGRITY_BLOCKED
        assert blocked["latest_blocker"] == CFA.INTEGRITY_HORIZON_INCOHERENT


# =========================================================================== #
# D - the capital-eligibility gate
# =========================================================================== #
FX_IH = "436a13011b67" + "0" * 52


def _fx_reg_full(ih=FX_IH):
    return {"challenger_id": CEG.FX_CARRY_CADENCE_CHALLENGER_ID, "registration_session": FX_REG,
            "registered_at": "2026-09-14T13:59:55+00:00", "asset_class": "FX_FUTURES",
            "horizon_sessions": 1, "identity": {"identity_hash": ih, "challenger_id": CEG.FX_CARRY_CADENCE_CHALLENGER_ID}}


def _projection(ih=FX_IH, *, matured=0, effective=0, rets=None, state="NOT_DUE"):
    rets = list(rets or [])
    n = len(rets)
    mean = (sum(rets) / n) if n else None
    t = None
    if n >= 3:
        var = sum((x - mean) ** 2 for x in rets) / (n - 1)
        t = mean / ((var / n) ** 0.5) if var > 0 else None
    return {ih: {"current_accrual_state": state, "predictions_emitted": matured, "forfeitures": 0,
                 "matured_observations": matured, "pending_observations": 0,
                 "effective_independent_observations": effective, "latest_blocker": None,
                 "horizon_sessions": 5,
                 "forward_economics": {"matured_decisions": n, "mean_net_return_per_decision": mean,
                                       "t_stat_net_vs_zero": t, "positive_share": None,
                                       "first_decision_session": "2026-09-22" if n else None,
                                       "last_maturity_session": "2027-04-30" if n else None}}}


def _approval(ih=FX_IH, cid=CEG.FX_CARRY_CADENCE_CHALLENGER_ID):
    return {"sleeve_id": "sleeve_fx_futures", "challenger_id": cid, "registration_identity_hash": ih,
            "declared_by": "operator", "declared_at": "2026-09-18T00:00:00+00:00"}


class TestCapitalEligibilityGate:

    def test_the_thresholds_mirror_the_frozen_r46_forward_gates(self):
        g = R46C.FORWARD_EVIDENCE_GATES
        assert CEG.FORWARD_GATE_MIRROR["min_effective_independent"] == g["min_effective_independent"]
        assert CEG.FORWARD_GATE_MIRROR["min_raw_matured"] == g["min_raw_matured"]
        assert CEG.FORWARD_GATE_MIRROR["min_calendar_days"] == g["min_calendar_days"]
        assert CEG.FORWARD_GATE_MIRROR["min_t_stat_net_vs_control"] == g["min_t_stat_net_vs_control"]
        assert CEG.gate_thresholds(5)["min_effective_independent"] == 40
        assert CEG.gate_thresholds(21)["gate_row_horizon"] == 20      # a longer hold, never a stricter count
        assert CEG.gate_thresholds(21)["min_effective_independent"] == 24

    def test_the_candidates_bind_the_registered_challengers(self):
        ids = {c["sleeve_id"]: c["challenger_id"] for c in CEG.declared_candidates()}
        assert ids["sleeve_fx_futures"] == FXC.CHALLENGER_ID
        assert ids["sleeve_managed_futures_trend"] == FTC.CHALLENGER_ID
        assert CEG.candidate_for_sleeve("sleeve_rates_futures") is None

    def test_zero_evidence_is_not_passed_and_the_exact_gate_is_named(self):
        cand = CEG.candidate_for_sleeve("sleeve_fx_futures")
        ev = CEG.evaluate_gate(cand, registrations=[_fx_reg_full()], accrual_projection=_projection(),
                               approval=None)
        assert ev["state"] == CEG.G_NOT_PASSED and ev["passed"] is False
        codes = set(ev["remaining_codes"])
        assert {CEG.REQ_EFFECTIVE, CEG.REQ_RAW, CEG.REQ_CALENDAR, CEG.REQ_EDGE, CEG.REQ_T_STAT,
                CEG.REQ_APPROVAL} <= codes
        eff = next(r for r in ev["remaining"] if r["code"] == CEG.REQ_EFFECTIVE)
        assert eff == {"code": CEG.REQ_EFFECTIVE, "required": 40, "observed": 0}
        assert ev["automatic_promotion"] is False

    def test_no_registration_is_evidence_unavailable_never_passed(self):
        cand = CEG.candidate_for_sleeve("sleeve_managed_futures_trend")
        ev = CEG.evaluate_gate(cand, registrations=[], accrual_projection={}, approval=None)
        assert ev["state"] in (CEG.G_NOT_PASSED, CEG.G_EVIDENCE_UNAVAILABLE) and ev["passed"] is False
        assert CEG.REQ_REGISTRATION in ev["remaining_codes"]

    def test_the_gate_passes_only_with_evidence_and_a_bound_approval(self):
        cand = CEG.candidate_for_sleeve("sleeve_fx_futures")
        rets = [0.004 + 0.002 * ((i % 5) - 2) for i in range(60)]        # positive, t >> 2.5
        proj = _projection(matured=60, effective=45, rets=rets, state="EMITTED")
        ev = CEG.evaluate_gate(cand, registrations=[_fx_reg_full()], accrual_projection=proj,
                               approval=_approval())
        assert ev["state"] == CEG.G_PASSED and ev["remaining"] == [], ev["remaining"]
        # the SAME evidence without the human pre-authorisation is NOT passed
        no = CEG.evaluate_gate(cand, registrations=[_fx_reg_full()], accrual_projection=proj, approval=None)
        assert no["state"] == CEG.G_NOT_PASSED and no["remaining_codes"] == [CEG.REQ_APPROVAL]
        # an approval bound to a different registration identity does not count
        other = CEG.evaluate_gate(cand, registrations=[_fx_reg_full()], accrual_projection=proj,
                                  approval=_approval(ih="deadbeef"))
        assert other["remaining_codes"] == [CEG.REQ_APPROVAL_BOUND]

    def test_the_operator_write_requires_a_token_and_is_immutable(self, tmp_path):
        out = CEG.declare_conditional_operational_approval(
            sleeve_id="sleeve_fx_futures", challenger_id=FXC.CHALLENGER_ID,
            registration_identity_hash=FX_IH, declared_by="op", statement="conditional",
            gate_dir_override=tmp_path)
        assert out["outcome"] == "CONFIRMATION_REQUIRED" and out["written"] is False
        ok = CEG.declare_conditional_operational_approval(
            sleeve_id="sleeve_fx_futures", challenger_id=FXC.CHALLENGER_ID,
            registration_identity_hash=FX_IH, declared_by="op", statement="conditional",
            confirm=CEG.APPROVAL_CONFIRM_TOKEN, gate_dir_override=tmp_path)
        assert ok["outcome"] == "DECLARED" and ok["record"]["grants_eligibility_today"] is False
        again = CEG.declare_conditional_operational_approval(
            sleeve_id="sleeve_fx_futures", challenger_id=FXC.CHALLENGER_ID,
            registration_identity_hash="other", declared_by="op", statement="x",
            confirm=CEG.APPROVAL_CONFIRM_TOKEN, gate_dir_override=tmp_path)
        assert again["outcome"] == "ALREADY_DECLARED" and again["written"] is False
        wrong = CEG.declare_conditional_operational_approval(
            sleeve_id="sleeve_fx_futures", challenger_id="SOMETHING_ELSE",
            registration_identity_hash=FX_IH, declared_by="op", statement="x",
            confirm=CEG.APPROVAL_CONFIRM_TOKEN, gate_dir_override=tmp_path)
        assert wrong["outcome"] == "REFUSED_CHALLENGER_MISMATCH"
        assert CEG.load_conditional_approval("sleeve_fx_futures", gate_dir_override=tmp_path)["registration_identity_hash"] == FX_IH

    def test_the_operational_signal_is_long_legs_rank_normalised(self):
        decisions = [{"eligible_session": "2026-09-22", "record_hash": "r",
                      "weights": {"6A": 0.3, "6B": -0.2, "6J": 0.25, "DX": -0.35}}]
        sig = CEG.operational_signal_scores(CEG.candidate_for_sleeve("sleeve_fx_futures"),
                                            decision_loader=lambda cid: decisions)
        assert sig["state"] == "OPERATIONAL_SIGNAL_AVAILABLE"
        assert sig["scores"] == {"&6A": 1.0, "&6J": 0.5}
        assert [x["market"] for x in sig["not_expressible"]] == ["6B", "DX"]
        assert sig["score_basis"] == OF.SB_SLEEVE_RANK
        empty = CEG.operational_signal_scores(CEG.candidate_for_sleeve("sleeve_fx_futures"),
                                              decision_loader=lambda cid: [])
        assert empty["state"] == "NO_FROZEN_DECISION_YET" and empty["scores"] == {}


# --------------------------------------------------------------------------- #
# the hermetic reference-data fixture (the R50 one, reused)
# --------------------------------------------------------------------------- #
@pytest.fixture()
def refdata(tmp_path, monkeypatch):
    import tests.test_release50_multi_asset_operational_manager as R50
    p = tmp_path / "refdata_fixture.json"
    p.write_text(json.dumps(R50.FIXTURE), encoding="utf-8")
    monkeypatch.setenv(mrd.FIXTURE_ENV, str(p))
    mrd.reset_cache()
    yield p
    mrd.reset_cache()


def _passed_gate(sleeve="sleeve_fx_futures", cid=FXC.CHALLENGER_ID):
    return {"state": CEG.G_PASSED, "passed": True, "remaining": [], "remaining_codes": [],
            "thresholds": CEG.gate_thresholds(5), "challenger_id": cid,
            "evidence": {"registration_identity_hash": FX_IH, "forward_economics": {"matured_decisions": 60},
                         "conditional_approval": {"declared_by": "op"}},
            "evaluated_at": "2026-09-18T00:00:00+00:00", "detail": "passed"}


def _not_passed_gate(sleeve="sleeve_fx_futures", cid=FXC.CHALLENGER_ID):
    return {"state": CEG.G_NOT_PASSED, "passed": False,
            "remaining": [{"code": CEG.REQ_EFFECTIVE, "required": 40, "observed": 0}],
            "remaining_codes": [CEG.REQ_EFFECTIVE, CEG.REQ_APPROVAL],
            "thresholds": CEG.gate_thresholds(5), "challenger_id": cid, "evidence": {},
            "evaluated_at": "2026-09-18T00:00:00+00:00", "detail": "2 remain"}


def _fx_signal_loader(cand):
    return {"scores": {"&6E": 1.0}, "state": "OPERATIONAL_SIGNAL_AVAILABLE",
            "score_basis": OF.SB_SLEEVE_RANK, "decision_session": "2026-09-22",
            "decision_record_hash": "r", "not_expressible": []}


class TestRegistryDerivation:

    def test_a_passed_gate_derives_approval_and_admits_the_sleeve(self, refdata):
        reg = ir.load_investability_registry(
            probe=True, as_of="2026-08-28", nav=5e6,
            gate_evaluations={"sleeve_fx_futures": _passed_gate(),
                              "sleeve_managed_futures_trend": _not_passed_gate("sleeve_managed_futures_trend",
                                                                               FTC.CHALLENGER_ID)},
            signal_loader=_fx_signal_loader)
        fx = ir.sleeve_map(reg)["sleeve_fx_futures"]
        assert fx["capital_eligible"] is True and fx["approval_derived_from_gate"] is True
        assert fx["model_approval_state"] == ir.APPROVED and fx["approval_injected"] is False
        assert fx["approval_evidence"]["state"] == "CAPITAL_ELIGIBILITY_GATE_PASSED"
        assert reg["non_equity_eligible_sleeve_ids"] == ["sleeve_fx_futures"]
        inst = ir.eligible_non_equity_instruments(reg, nav=5e6, as_of="2026-08-28")
        e6 = next(i for i in inst if i["instrument_id"] == "&6E")
        assert e6["executable_at_nav"] and e6["opportunity_score"] == 1.0
        trend = ir.sleeve_map(reg)["sleeve_managed_futures_trend"]
        assert trend["capital_eligible"] is False
        assert trend["capital_ineligible_reason"] == ir.R_GATE_NOT_PASSED
        assert trend["capital_eligibility_blocker"]["remaining"] == [CEG.REQ_EFFECTIVE, CEG.REQ_APPROVAL]
        ledger = {l["sleeve_id"]: l for l in reg["non_equity_admission_ledger"]}
        assert ledger["sleeve_fx_futures"]["capital_eligible"] is True
        assert ledger["sleeve_managed_futures_trend"]["gate_state"] == CEG.G_NOT_PASSED

    def test_a_not_passed_gate_is_fail_closed_and_names_the_exact_gate(self, refdata):
        reg = ir.load_investability_registry(
            probe=True, as_of="2026-08-28", nav=5e6,
            gate_evaluations={"sleeve_fx_futures": _not_passed_gate()})
        fx = ir.sleeve_map(reg)["sleeve_fx_futures"]
        assert fx["capital_eligible"] is False
        assert fx["capital_ineligible_reason"] == ir.R_GATE_NOT_PASSED
        assert fx["capital_eligibility_blocker"]["exact_gate"][0]["code"] == CEG.REQ_EFFECTIVE
        assert reg["non_equity_eligible_sleeve_ids"] == []
        assert ir.eligible_non_equity_instruments(reg, nav=5e6, as_of="2026-08-28") == []

    def test_an_unavailable_gate_owner_admits_nothing(self, refdata):
        reg = ir.load_investability_registry(probe=True, as_of="2026-08-28",
                                             gate_evaluations={"__error__": "boom"})
        fx = ir.sleeve_map(reg)["sleeve_fx_futures"]
        assert fx["capital_eligible"] is False
        assert fx["capital_eligibility_gate"]["state"] == "GATE_UNAVAILABLE"

    def test_the_registry_still_imports_no_research_package(self):
        src = (REPO / "api" / "investability_registry.py").read_text(encoding="utf-8")
        assert "alpha_agent" not in src and "def promote" not in src


# =========================================================================== #
# F - the multi-asset acceptance
# =========================================================================== #
def _portfolio_state(nav=5e6):
    n = 25
    positions = [{"ticker": "A%02d" % i, "sector": "S%d" % (i % 6), "quantity": 100,
                  "portfolio_weight": 0.036, "exposure_weight": 0.036, "market_value": nav * 0.036,
                  "price": 100.0, "notional_usd": nav * 0.036} for i in range(n)]
    return {"dates": {"eligible_market_date": "2026-08-28", "valuation_date": "2026-08-28"},
            "active_book": {"book_id": "b", "book_label": "B"},
            "capital": {"nav": nav, "cash": nav * (1 - 0.036 * n)}, "positions": positions,
            "state_hash": "psh"}


def _scoring():
    rows = [_urow("A%02d" % i, i + 1, 1 - i / 60.0, sector="S%d" % (i % 6)) for i in range(25)]
    rows += [_urow("N%02d" % i, 26 + i, 1 - (25 + i) / 60.0, sector="S%d" % (i % 6)) for i in range(25)]
    return {"rankings": rows, "output_hash": "us"}


class TestMultiAssetAcceptance:

    def _frontier(self, gate):
        reg = ir.load_investability_registry(probe=True, as_of="2026-08-28", nav=5e6,
                                             gate_evaluations={"sleeve_fx_futures": gate},
                                             signal_loader=_fx_signal_loader)
        return reg, of_api.load_opportunity_frontier(portfolio_state=_portfolio_state(), scoring=_scoring(),
                                                     registry=reg, probe=True)

    def test_equity_cash_and_one_qualified_non_equity_sleeve_share_one_frontier(self, refdata):
        reg, fr = self._frontier(_passed_gate())
        assert fr["eligible_non_equity_count"] >= 1
        assert {"US_EQUITY", "CASH", "FX_FUTURES"} <= set(fr["asset_classes_eligible"])
        rows = {r["instrument_id"]: r for r in fr["rows"]}
        assert rows["&6E"]["eligible"] and rows["&6E"]["score_basis"] == OF.SB_SLEEVE_RANK
        assert rows["USD_CASH"]["eligible"] and rows["A00"]["eligible"]
        assert fr["incumbency_privilege"] is False and fr["forced_diversification"] is False
        assert [r["ticker"] for r in fr["candidate_rows_for_proposal"]] == ["&6E"]
        ledger = {l["sleeve_id"]: l for l in fr["non_equity_admission_ledger"]}
        assert ledger["sleeve_fx_futures"]["instruments_admitted"] >= 1
        assert "eligible" in fr["eligible_non_equity_count_explanation"]

    def test_the_proposal_admits_the_row_and_the_allocator_may_fund_it(self, refdata):
        reg, fr = self._frontier(_passed_gate())
        ps = _portfolio_state()
        sc = _scoring()
        exits = {"A20", "A21", "A22", "A23", "A24"}
        hoc = {"assessment_state": "READY", "assessment_hash": "hoc",
               "holding_reviews": [_review(p["ticker"], rec=("EXIT" if p["ticker"] in exits else "HOLD"),
                                           pct=0.5) for p in ps["positions"]]}
        ic = arp.build_input_contract(portfolio_state=ps, scoring=sc, hoc_assessment=hoc,
                                      price_panel=None, frontier=fr)
        assert ic["frontier_rows_admitted"] == ["&6E"]
        assert ic["frontier_eligible_non_equity_count"] >= 1
        assert ic["frontier_non_equity_admission"][0]["sleeve_id"] in {l["sleeve_id"] for l in fr["non_equity_admission_ledger"]}
        out = RP.build_proposal(input_contract=ic, policy=arp.resolve_policy())
        assert out["proposal_state"] in (RP.STATE_READY, RP.STATE_DEGRADED)
        e6 = next(a for a in out["allocations"] if a["ticker"] == "&6E")
        assert e6["action"] == RP.ACT_ADD and e6["proposed_weight"] > 0
        assert e6["asset_class"] == "FX_FUTURES" and e6["sleeve_id"] == "sleeve_fx_futures"
        assert e6["instrument_type"] == "FUTURE" and e6["currency"] == "USD"
        assert "FX_FUTURES" in out["portfolio"]["asset_classes_in_target"]
        assert len(out["portfolio"]["asset_classes_in_target"]) > 1
        assert out["portfolio"]["non_equity_position_count_in_target"] >= 1
        assert out["portfolio"]["proposed_collateral_weight"] > 0
        assert out["constraint_reoptimization"]["incumbency_policy"] == CR.INCUMBENCY_POLICY
        assert out["switching_economics"]["incumbency_advantage_applied"] == "TRANSITION_COST_ONLY"
        # manual review only; nothing executed
        assert out["safety"]["created_orders"] is False and out["safety"]["created_fills"] is False
        assert out["safety"]["manual_review"] is True and out["safety"]["preview_only"] is True

    def test_cross_asset_risk_and_currency_collateral_constraints_are_evaluated(self):
        import tests.test_release50_multi_asset_operational_manager as R50
        pos = [{"instrument_id": "AAA", "exposure_weight": 0.5, "asset_class": "US_EQUITY",
                "sleeve_id": "eq", "currency": "USD", "notional_usd": 50.0, "collateral_usd": 0.0,
                "capital_usage_usd": 50.0, "instrument_type": "CASH_EQUITY"},
               {"instrument_id": "&ZN", "exposure_weight": 0.3, "asset_class": "RATES_FUTURES",
                "sleeve_id": "rates", "currency": "USD", "notional_usd": 30.0, "collateral_usd": 0.6,
                "capital_usage_usd": 0.6, "instrument_type": "FUTURE"}]
        rs = XR.build_risk_state(positions=pos, aligned_returns=R50._returns(), nav=100.0, cash=49.4)
        assert rs["state"] == XR.STATE_AVAILABLE
        assert set(rs["risk_contribution_by_asset_class"]) == {"US_EQUITY", "RATES_FUTURES"}
        # a non-USD future and futures collateral are capped by the repair kernel
        cands = [R50._cand("A1", score=0.9),
                 R50._cand("&FDAX", score=0.99, sector="Intl", unit_notional_usd=750000.0, **R50._FDAX),
                 R50._cand("&FESX", score=0.98, sector="Intl", unit_notional_usd=60000.0, **R50._FDAX)]
        pol = dict(CR.default_policy(), target_position_count=12, min_position_weight=0.01,
                   non_usd_currency_cap=0.12, collateral_cap_fraction=0.01,
                   sleeve_weight_caps={"DEFAULT_NON_EQUITY": 1.0},
                   asset_class_weight_caps={"US_EQUITY": 1.0, "DEFAULT_NON_EQUITY": 1.0})
        sol = CR.solve_feasible_target(current_weight={}, ideal_weight={"&FDAX": 0.10, "&FESX": 0.10, "A1": 0.10},
                                       candidates=cands, nav=1e7, policy=pol)
        g = sol["cross_asset"]["group_weights"]
        assert g["non_usd"] <= 0.12 + 1e-9 and g["collateral"] <= 0.01 + 1e-9
        assert {CR.C_CURRENCY_CAP, CR.C_COLLATERAL_CAP} & set(sol["constraints_that_reshaped"])

    def test_the_zero_base_allocator_may_choose_any_mix_the_policy_permits(self):
        import tests.test_release50_multi_asset_operational_manager as R50
        cands = [R50._cand("A1", score=0.5), R50._cand("A2", score=0.5, sector="Health"),
                 dict(R50._cand("&ZN", score=0.9, sector="Rates Futures", **R50._ZN), unit_notional_usd=108000.0)]
        # NAV large enough that ONE contract fits the name cap (unit granularity is a
        # real capacity limit: at $1M a $108k contract is not a 10 % position)
        ic = {"eligible_market_date": "2026-08-28", "active_book_id": "b", "nav": 5e6,
              "candidates": cands, "mu": {"A1": 0.02, "A2": 0.015, "&ZN": 0.05},
              "sigma_forecast": {"A1": 0.01, "A2": 0.01, "&ZN": 0.01},
              "downside": {}, "aligned_returns": {"dates": [], "series": {}},
              "forecast_model_spec_hash": "h", "current_weights": {}}
        zb = ZB.build_allocation(input_contract=ic)["zero_base_target"]
        assert zb["allocation_by_asset_class"].get("RATES_FUTURES", 0.0) > 0
        assert zb["allocation_by_asset_class"].get("RATES_FUTURES", 0.0) <= 0.25 + 1e-9
        assert zb["constraints"]["valid"]

    def test_an_unqualified_non_equity_sleeve_is_excluded_fail_closed(self, refdata):
        reg, fr = self._frontier(_not_passed_gate())
        assert fr["eligible_non_equity_count"] == 0
        assert fr["candidate_rows_for_proposal"] == []
        assert set(fr["asset_classes_eligible"]) == {"US_EQUITY", "CASH"}
        ledger = {l["sleeve_id"]: l for l in fr["non_equity_admission_ledger"]}
        assert ledger["sleeve_fx_futures"]["gate_state"] == CEG.G_NOT_PASSED
        assert ledger["sleeve_fx_futures"]["blocker"] == ir.R_GATE_NOT_PASSED
        assert "sleeve_fx_futures" in fr["eligible_non_equity_count_explanation"]
        ic = arp.build_input_contract(portfolio_state=_portfolio_state(), scoring=_scoring(),
                                      hoc_assessment={}, price_panel=None, frontier=fr)
        assert ic["frontier_rows_admitted"] == [] and ic["frontier_eligible_non_equity_count"] == 0
        assert any(l["sleeve_id"] == "sleeve_fx_futures" for l in ic["frontier_non_equity_admission"])
        assert "0 non-equity" in ic["frontier_non_equity_count_explanation"]


# =========================================================================== #
# E - the managed-futures trend challenger
# =========================================================================== #
TREND_SCOPE = ["CL", "GC", "ZN", "6E", "ES", "ZW", "HG", "ZB", "6J", "NG", "SI", "ZC"]
TREND_CLASS = {"CL": "COMMODITY_FUTURES", "GC": "COMMODITY_FUTURES", "ZN": "RATES_FUTURES",
               "6E": "FX_FUTURES", "ES": "EQUITY_INDEX_FUTURES", "ZW": "COMMODITY_FUTURES",
               "HG": "COMMODITY_FUTURES", "ZB": "RATES_FUTURES", "6J": "FX_FUTURES",
               "NG": "COMMODITY_FUTURES", "SI": "COMMODITY_FUTURES", "ZC": "COMMODITY_FUTURES"}


def _trend_meta(m):
    ac = TREND_CLASS.get(m)
    return None if ac is None else {"asset_class": ac, "economic_group": "G", "cost_bps_per_side": 2.0}


def _business_days(n, start="2010-01-04"):
    from datetime import date, timedelta
    d = date.fromisoformat(start)
    out = []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def _write_trend_store(store: Path, n=2600, drift=0.0004):
    store.mkdir(parents=True, exist_ok=True)
    days = _business_days(n)
    for k, m in enumerate(TREND_SCOPE):
        rnd = random.Random(1000 + k)
        sign = 1.0 if k % 2 == 0 else -1.0
        with (store / ("%s_daily.csv" % m)).open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "ret1", "c1", "oi1", "v1"])
            level = 100.0
            for i, d in enumerate(days):
                r = sign * drift + rnd.gauss(0.0, 0.01)
                level *= 1.0 + r
                last = i == len(days) - 1
                w.writerow([d, r, level, 0.0 if last else 1000.0 + i, 500.0 + i])
    return days


@pytest.fixture()
def trend_env(research_root, monkeypatch):
    # The alpha_recovery owners import the research substrate as the TOP-LEVEL
    # ``alpha_agent`` package (never through ``paper_trader.``), so the seam is
    # patched on that module object - patching the ``paper_trader.alpha_agent``
    # copy would leave the code under test reading the real R38 panel.
    import alpha_agent.r63.panels as P_top
    monkeypatch.setattr(P_top, "futures_market_meta", _trend_meta)
    monkeypatch.setattr(P_top, "MUST_TRADE_ON_OR_AFTER", "2000-01-01")
    return research_root


class TestFuturesTrendChallenger:

    def test_the_score_is_pit_arithmetic_and_refuses_short_history(self):
        rnd = random.Random(3)
        rets = [rnd.gauss(0.0005, 0.01) for _ in range(400)]
        s = FTC.trend_score(rets, 399)
        assert s is not None and set(s) >= {"score", "vol_annualised", "cum_log_return_12_1"}
        assert FTC.trend_score(rets, 200) is None
        # uses ONLY returns at or before p: a later change does not move it
        rets2 = list(rets) + [10.0]
        assert FTC.trend_score(rets2, 399) == s

    def test_the_universe_rule_and_the_validation_run_on_a_private_store(self, trend_env, tmp_path):
        store = tmp_path / "frozen"
        _write_trend_store(store)
        uni = FTC.scope_universe(store)
        assert sorted(a["market"] for a in uni["admitted"]) == sorted(TREND_SCOPE)
        v = FTC.validate(store=store, start="2011-01-01", lockbox_start="2018-01-01")
        assert v["ok"] and v["decisions"] > 50
        assert v["classification"] in FTC.CLASSIFICATIONS
        assert v["full"]["periods"] == v["decisions"]
        assert v["selection"]["periods"] + v["lockbox"]["periods"] == v["decisions"]
        assert v["historical_result_is_forward_evidence"] is False
        assert v["control"] == "ZERO_RETURN_CASH"

    def test_the_frozen_record_verifies_and_is_written_once(self, trend_env, tmp_path):
        store = tmp_path / "frozen"
        _write_trend_store(store)
        v = FTC.validate(store=store, start="2011-01-01", lockbox_start="2018-01-01")
        rec = FTC.build_record(v, inception="2026-09-18")
        assert FTC.verify_record(rec) == []
        assert rec["forward_specification"]["horizon_sessions"] == 21
        assert rec["forward_specification"]["trade_every_sessions"] == 21
        assert rec["promotion_allowed"] is False and rec["live_registration_performed"] is False
        assert rec["freeze_record_hash"] == AR.stable_hash(rec["forward_specification"])
        out = FTC.write_record(rec)
        assert out["outcome"] == "FROZEN"
        assert FTC.write_record(rec)["outcome"] == "ALREADY_FROZEN"
        other = dict(rec, record_hash="ff" * 32)
        assert FTC.write_record(other)["outcome"] == "REFUSED_A_DIFFERENT_IDENTITY_IS_FROZEN"
        res = FTC.resolve()
        assert res["ok"] is True
        row = FTC.freeze_row(res)
        assert row["asset_class"] == "MULTI_ASSET_FUTURES" and row["horizon_sessions"] == 21
        assert row["forward_challenger"]["challenger_id"] == FTC.CHALLENGER_ID
        assert row["forward_challenger"]["record_hash"] == rec["freeze_record_hash"]
        assert sorted(row["spec_json"]["instrument_scope"]) == sorted(TREND_SCOPE)
        tampered = dict(rec, forward_specification=dict(rec["forward_specification"], horizon_sessions=5))
        assert any("MISMATCH" in p or "HASH" in p for p in FTC.verify_record(tampered))

    def test_the_gate_candidate_names_this_challenger_and_the_horizon_is_one_number(self):
        assert CEG.FUTURES_TREND_CHALLENGER_ID == FTC.CHALLENGER_ID
        assert FTC.HORIZON_CONTRACT["label_equals_holding"] is True
        assert FTR.EVALUATION_HORIZON_SESSIONS == FTR.TRADE_EVERY == 21


TREND_IDENT = {"challenger_id": FTC.CHALLENGER_ID, "freeze_record_hash": "frz", "identity_hash": "ft-identity",
               "release": "ALPHA_RECOVERY_OFFENSIVE", "registration_session": "2026-09-18",
               "model_spec_hash": "spec", "horizon_sessions": 21}


def _trend_registration():
    return {"challenger_id": FTC.CHALLENGER_ID, "registration_session": "2026-09-18",
            "freeze_record_hash": "frz", "asset_class": "MULTI_ASSET_FUTURES",
            "horizon_sessions": 21, "instrument_scope": list(TREND_SCOPE),
            "identity": {"challenger_id": FTC.CHALLENGER_ID, "release": "ALPHA_RECOVERY_OFFENSIVE",
                         "identity_hash": "ft-identity", "freeze_record_hash": "frz"},
            "observation_clock": {"first_eligible_observation_session": None}}


def _write_forward_store(through: str):
    """Sessions on the exchange calendar from 2026-06-01 through ``through``."""
    store = FTR.curves_dir()
    store.mkdir(parents=True, exist_ok=True)
    days = []
    d = "2026-06-01"
    while d <= through:
        if NOC.is_eligible_session(d):
            days.append(d)
        d = NOC.next_eligible_session(d) if NOC.is_eligible_session(d) else \
            NOC.next_eligible_session(__import__("datetime").date.fromisoformat(d) - __import__("datetime").timedelta(days=1))
    for k, m in enumerate(TREND_SCOPE):
        with (store / ("%s_daily.csv" % m)).open("w", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            w.writerow(["date", "ret1", "c1", "oi1", "v1"])
            for i, dd in enumerate(days):
                last = i == len(days) - 1
                w.writerow([dd, 0.001 * ((i + k) % 5 - 2), 1.0 + 0.01 * i, 0.0 if last else 1000.0 + i, 500.0 + i])
    return days


def _fake_trend_scorer(calls):
    def scorer(plan, prev):
        calls.append((dict(plan), dict(prev)))
        return {"ok": True, "entry_session": plan["entry_session"],
                "information_session": plan["information_session"],
                "newest_published_session": plan["newest_session_required"],
                "weights": {"CL": 0.2, "GC": 0.15, "ZN": -0.3, "ES": 0.1},
                "source_data_hash": "src", "feature_state_hash": "feat", "leverage": 1.2,
                "leverage_state": "TARGETED", "gross_unlevered": 0.6, "gross_levered": 0.75,
                "names_held_by_the_band": [], "checks": {"ok": True}}
    return scorer


class TestFuturesTrendRuntime:

    def _declare(self):
        return PD.declare_policy(**FTR.policy_declaration(TREND_IDENT, scope=TREND_SCOPE),
                                 now="2026-09-18T20:00:00+00:00")

    def test_the_policy_declares_a_21_session_cadence_and_the_marks(self, trend_env):
        assert self._declare()["outcome"] == PD.FROZEN
        pol = PD.load_policy(FTC.CHALLENGER_ID)
        assert pol["rebalance_cadence_sessions"] == 21 and pol["evaluation_horizon_sessions"] == 21
        ec = pol["execution_contract"]
        assert ec["holding_sessions"] == 21 and ec["accrual_arms_the_owner_frozen_entry_session"] is True
        assert ec["valuation_marks"]["store_relative_to_research_root"] == FTR.marks_store_relative()
        assert ec["execution_boundary"] != "NEXT_ELIGIBLE_SESSION_OPEN"
        assert self._declare()["outcome"] == PD.ALREADY_FROZEN

    def test_the_producer_and_the_accrual_name_the_same_21_session_boundaries(self, trend_env):
        days = _write_forward_store("2026-11-30")
        rows = {m: FTR.daily_rows(m) for m in TREND_SCOPE}
        pub = FTR.published_sessions(TREND_SCOPE, rows_by_market=rows)
        grid = CFA.decision_grid(_trend_registration(), sessions=pub, cadence_sessions=21)
        after = [d for d in pub if d > "2026-09-18"]
        assert grid[:2] == [after[0], after[21]]
        entry = grid[1]
        plan = FTR.plan_entry(entry_session=entry, registration_session="2026-09-18", scope=TREND_SCOPE,
                              published=[d for d in pub if d < entry], rows_by_market=rows)
        assert plan["state"] == "DUE" and plan["rebalance_index"] % 21 == 0
        assert plan["information_session"] == NOC.previous_eligible_session(NOC.previous_eligible_session(entry))
        early = FTR.plan_entry(entry_session=after[3], registration_session="2026-09-18", scope=TREND_SCOPE,
                               published=[d for d in pub if d < after[3]], rows_by_market=rows)
        assert early["state"] == FTR.ST_NOT_A_REBALANCE

    def test_one_decision_inside_the_window_and_the_accrual_emits_it(self, trend_env, tmp_path):
        self._declare()
        days = _write_forward_store("2026-12-31")
        after = [d for d in days if d > "2026-09-18"]
        entry = after[21]
        _write_forward_store(after[20])                      # the newest published session is t = after[20]
        calls = []
        window_open = NOC._et_instant(entry, (5, 0))
        res = FTR.advance(now=window_open, scorer=_fake_trend_scorer(calls), refresher=lambda k: {"ok": True})
        assert res["state"] == FTR.ST_FROZEN, res
        rec = PD.load_decision(FTC.CHALLENGER_ID, entry)
        assert rec["evaluation_horizon_sessions"] == 21 and rec["is_true_forward"] is True
        assert rec["decision_context"]["holding_sessions"] == 21
        assert rec["weights"]["ZN"] < 0                       # a short leg is a legitimate forward book
        again = FTR.advance(now=NOC._et_instant(entry, (6, 0)), scorer=_fake_trend_scorer(calls),
                            refresher=lambda k: {"ok": True})
        assert again["state"] == FTR.ST_ALREADY_FROZEN and len(calls) == 1
        adv = CFA.advance_canonical_forward_accrual(
            now=datetime.fromisoformat(NOC._et_instant(entry, (5, 30))), registrations=[_trend_registration()],
            price_panel={"series": {}}, store_dir_override=tmp_path / "acc")
        row = adv["challengers"][0]
        assert adv["n_emitted_this_run"] == 1, row
        assert row["horizon_sessions"] == 21 and row["horizon_roles"]["coherent"] is True
        assert row["horizon_roles"]["registration_horizon_role"] == CFA.HORIZON_ROLE_HOLDING
        assert row["armed_owner_decided_session"] == entry

    def test_a_missed_boundary_is_never_backfilled(self, trend_env):
        self._declare()
        days = _write_forward_store("2026-12-31")
        after = [d for d in days if d > "2026-09-18"]
        entry = after[21]
        _write_forward_store(after[20])
        calls = []
        res = FTR.advance(now=NOC._et_instant(entry, (10, 0)), scorer=_fake_trend_scorer(calls),
                          refresher=lambda k: {"ok": True})
        assert res["state"] == FTR.ST_MISSED and res["backfill_refused"] is True
        assert PD.load_decision(FTC.CHALLENGER_ID, entry) is None and calls == []

    def test_in_process_scoring_builds_the_r64_book_from_the_store(self, trend_env):
        _write_trend_store(FTR.curves_dir(), n=1500)
        rows = {m: FTR.daily_rows(m) for m in TREND_SCOPE}
        pub = FTR.published_sessions(TREND_SCOPE, rows_by_market=rows)
        u, t, s = pub[-3], pub[-2], pub[-1]
        out = FTR.score_decision(entry_session=s, information_session=u, newest_session=t,
                                 prev_weights={}, scope=TREND_SCOPE, rows_by_market=rows)
        assert out["ok"] is True, out
        assert set(out["weights"]) <= set(TREND_SCOPE) and out["gross_levered"] > 0
        assert out["checks"]["markets_scored"] == len(TREND_SCOPE)
        assert out["leverage_state"] in ("WARMUP", "TARGETED", "AT_CAP")
        # deterministic on the same information
        again = FTR.score_decision(entry_session=s, information_session=u, newest_session=t,
                                   prev_weights={}, scope=TREND_SCOPE, rows_by_market=rows)
        assert again["feature_state_hash"] == out["feature_state_hash"]

    def test_the_canonical_runtime_owns_the_stage_between_fx_and_accrual(self):
        src = (REPO / "alpha_agent" / "r52" / "runtime.py").read_text(encoding="utf-8")
        assert '"futures_trend_prospective_decision"' in src and "FTR.advance(" in src
        assert src.index("fx_carry_cadence_prospective_decision") < src.index("futures_trend_prospective_decision")
        assert src.index("futures_trend_prospective_decision") < src.index("advance_canonical_forward_accrual(")
        for token in ("place_order", "submit_order", "create_order", "apply_fill", "promote_model(",
                      "activate_sleeve(", "approve_proposal(", "adopt_prospective_freeze(",
                      "register_forward_challenger("):
            assert token not in Path(FTR.__file__).read_text(encoding="utf-8"), token
            assert token not in Path(FTC.__file__).read_text(encoding="utf-8"), token
        door = (REPO / "scripts" / "register_futures_trend_challenger.py").read_text(encoding="utf-8")
        assert door.count("PA.adopt_prospective_freeze(") == 1
        assert "current_prospective_boundary()" in door
        assert "ADOPT_PROSPECTIVE_FORWARD_CLOCKS" not in door


# =========================================================================== #
# B - the pipeline audit is pure over its inputs
# =========================================================================== #
def test_b_the_pipeline_audit_prints_every_column_and_explains_the_zero(refdata):
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "audit_pipeline", REPO / "scripts" / "audit_multi_asset_capital_pipeline.py")
    A = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(A)
    reg = ir.load_investability_registry(probe=True, as_of="2026-08-28",
                                         gate_evaluations={"sleeve_fx_futures": _not_passed_gate()})
    fr = of_api.load_opportunity_frontier(portfolio_state=_portfolio_state(), scoring=_scoring(),
                                          registry=reg, probe=True)
    rep = A.build_report(registry=reg, registrations=[_fx_reg_full()],
                         accrual_projection=_projection(), frontier=fr,
                         r46_leaderboard={"rows": [{"challenger_id": "r46_fut_ts_mom_252",
                                                    "asset_class": "MULTI_ASSET_FUTURES",
                                                    "state": "FORWARD_PENDING", "effective_independent": 0}]},
                         s25={"strategy_name": "s25_operating_profitability", "state": "NOT_DUE",
                              "raw_marks": 1, "valid_marks_before": 1})
    assert rep["columns"] == list(A.COLUMNS)
    ids = {r["SLEEVE_ID"] for r in rep["rows"]}
    assert {"us_equity_fundamental_momentum_50_50_v1", "cash_usd", "sleeve_fx_futures",
            "sleeve_managed_futures_trend", "r46_fut_ts_mom_252", "s25_operating_profitability"} <= ids
    fx = next(r for r in rep["rows"] if r["SLEEVE_ID"] == "sleeve_fx_futures")
    assert fx["CAPITAL_ELIGIBLE"] is False and fx["FORWARD_EMISSIONS"] == 0
    assert ir.R_GATE_NOT_PASSED in fx["CAPITAL_ELIGIBILITY_BLOCKER"]
    assert fx["RISK_MODEL_AVAILABLE"] is True and fx["EXECUTION_MODEL_AVAILABLE"] is True
    assert rep["explanation"]["frontier_eligible_non_equity_count"] == 0
    text = A.render(rep)
    for c in A.COLUMNS:
        assert c in text
    assert "frontier_eligible_non_equity_count = 0" in text
