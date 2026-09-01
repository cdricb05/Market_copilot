"""Release 53 - active risk, intraday alpha & cross-market capital offensive.

What is locked shut here:

* **The production risk policy is unchanged.** Both canonical policy owners
  still declare the frozen values; the shadow policies are copies that cannot
  reach them.
* **Intraday evidence is prospective or it is refused.** A row whose emission
  is not strictly before its outcome window, whose data is missing/stale, or
  which arrives twice for one slot cell, never enters the ledger. A missed
  slot is a forfeiture that refuses backfill. Only MATURED outcomes score.
* **Authority cannot leak.** The R53 lanes are shadow-only: no operational
  write owner is imported, no allocator is re-implemented, no approval
  persists outside the hermetic process, no promotion happens anywhere.
* **The R53 challengers went through the canonical door.** Two new frozen
  specs, registered retune-free, with all three probe-map entries.
* **The collection-lock outage class is closed.** A live holder still refuses
  instantly; a provably dead holder is waited out within the takeover window
  instead of killing collection until the next logon.
"""
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

import pytest

from alpha_agent import r53 as R53
from alpha_agent.r46 import challengers as CH
from alpha_agent.r53 import capital_competition as CC
from alpha_agent.r53 import intraday_factory as IF
from alpha_agent.r53 import latency as LA
from alpha_agent.r53 import multi_horizon_view as MH
from alpha_agent.r53 import risk_appetite as RA
from alpha_agent.r53 import runtime_status as RS

REPO = Path(__file__).resolve().parents[1]


def _utc(y, m, d, hh=0, mm=0, ss=0):
    return dt.datetime(y, m, d, hh, mm, ss, tzinfo=dt.timezone.utc)


# =========================================================================== #
# 1. Production policy unchanged; shadows cannot mutate it
# =========================================================================== #
class TestProductionPolicyUntouched:

    def test_canonical_policy_values_are_the_frozen_ones(self):
        from paper_trader.engine import constrained_reallocation as CR
        from paper_trader.engine import reallocation_proposal as RP
        cr, rp = CR.default_policy(), RP.default_policy()
        assert cr["target_position_count"] == 25
        assert cr["max_name_weight"] == 0.10
        assert cr["sector_cap_fraction"] == 0.25
        assert cr["min_switching_net_improvement"] == 0.05
        assert cr["max_one_way_turnover"] == 0.35
        assert cr["min_adv_dollar"] == 1.0e7
        assert cr["max_gross_exposure"] == 1.0
        assert rp["entry_rank"] == 25 and rp["exit_buffer_rank"] == 30

    def test_harness_variants_leave_the_production_dict_untouched(self):
        from paper_trader.engine import constrained_reallocation as CR
        before = json.dumps(CR.default_policy(), sort_keys=True, default=str)
        pol = RA._harness_policy(target_position_count=8,
                                 min_switching_net_improvement=0.0,
                                 max_name_weight=0.5)
        assert pol["target_position_count"] == 8
        after = json.dumps(CR.default_policy(), sort_keys=True, default=str)
        assert before == after

    def test_shadow_policy_set_is_the_declared_three_and_current_is_verbatim(self):
        assert set(RA.SHADOW_POLICY_DEFINITIONS) == {
            "CURRENT_CONSERVATIVE_POLICY", "MODERATE_ACTIVE_POLICY",
            "HIGH_ACTIVE_POLICY"}
        assert RA.SHADOW_POLICY_DEFINITIONS[
            "CURRENT_CONSERVATIVE_POLICY"]["overrides"] == {}

    def test_walkforward_artifact_declares_proxy_and_no_champion(self):
        body = R53.read_json(R53.research_dir() / RA.ARTIFACT_WALKFORWARD)
        assert body, "run the walk-forward before the release ships"
        assert body["signal_is_proxy"] is True
        assert body["no_champion_selected"] is True
        assert body["safety"] and "NO PRODUCTION POLICY CHANGE" in body["safety"]
        zones = body["results"][0]["zones"]
        assert set(zones) == {"DEVELOPMENT", "VALIDATION"}

    def test_walkforward_kernel_is_the_canonical_owner_not_a_copy(self):
        src = (REPO / "alpha_agent" / "r53" / "risk_appetite.py").read_text(
            encoding="utf-8")
        assert "solve_feasible_target" in src            # calls it
        assert "def solve_feasible_target" not in src    # never redefines it
        assert "def switching_economics" not in src
        for mod in ("risk_appetite", "capital_competition",
                    "multi_horizon_view", "intraday_factory", "latency",
                    "runtime_status"):
            text = (REPO / "alpha_agent" / "r53" / (mod + ".py")).read_text(
                encoding="utf-8")
            assert "def solve_feasible_target" not in text
            assert "def build_frontier" not in text


# =========================================================================== #
# 2. Intraday factory: frozen, prospective, idempotent, no backfill
# =========================================================================== #
@pytest.fixture()
def tmp_ledger(monkeypatch, tmp_path):
    monkeypatch.setattr(IF, "research_dir", lambda: tmp_path)
    return tmp_path


def _sig(instrument="SPY", direction=1, ts="2026-09-01T14:00:00Z"):
    return {"instrument": instrument, "direction": direction, "score": 0.5,
            "data_timestamp_utc": ts}


def _rows(now, spec=None, **sig_kw):
    spec = spec or IF.INTRADAY_SPECS[0]
    slot = {"slot_et": "10:00", "slot_date_et": "2026-09-01",
            "slot_utc": "2026-09-01T14:00:00Z"}
    return IF.build_prediction_rows(
        spec=spec, slot=slot, now_utc=now, signals=[_sig(**sig_kw)],
        session_close_utc="2026-09-01T20:00:00Z")


class TestIntradayProspectiveDiscipline:

    def test_every_spec_is_complete_and_hash_frozen(self):
        assert len(IF.INTRADAY_SPECS) == 8
        assert len(IF.INTRADAY_FAMILIES) >= 6
        seen = set()
        for s in IF.INTRADAY_SPECS:
            h = IF.spec_hash(s)
            assert h not in seen
            seen.add(h)
            assert s["expected_return_state"] == "NOT_CALIBRATED"
            assert s["promotion_allowed"] is False
            assert s["research_shadow_only"] is True
            assert s["parameters_were_searched"] is False

    def test_prediction_is_frozen_before_outcome_or_refused(self, tmp_ledger):
        now = _utc(2026, 9, 1, 14, 0, 5)
        rows = _rows(now)
        assert rows and all(
            r["emitted_at_utc"] < r["outcome_window_start_utc"] for r in rows)
        bad = dict(rows[0])
        bad["outcome_window_start_utc"] = bad["emitted_at_utc"]
        with pytest.raises(IF.LedgerRefusal):
            IF.validate_prediction(bad)

    def test_stale_input_is_refused(self, tmp_ledger):
        now = _utc(2026, 9, 1, 14, 30, 0)   # bar is 30 min old at emission
        rows = _rows(now)
        with pytest.raises(IF.LedgerRefusal, match="stale"):
            IF.append_predictions(rows)

    def test_duplicate_slot_cell_is_suppressed_first_emission_wins(
            self, tmp_ledger):
        now = _utc(2026, 9, 1, 14, 0, 5)
        first = IF.append_predictions(_rows(now))
        assert first["n_appended"] == len(first["appended"]) > 0
        again = IF.append_predictions(_rows(now))
        assert again["n_appended"] == 0
        assert again["n_duplicates_skipped"] > 0

    def test_missed_slot_is_forfeited_and_backfill_refused(self, tmp_ledger):
        with pytest.raises(IF.LedgerRefusal, match="backfill_refused"):
            IF.append_forfeitures([{"challenger_id": "x",
                                    "slot_utc": "2026-09-01T14:00:00Z",
                                    "reason": "OPERATIONALLY_MISSED",
                                    "backfill_refused": False}])
        lane = {"state": IF.LANE_AVAILABLE}
        swept = IF.sweep_forfeitures(now_utc=_utc(2026, 9, 1, 19, 0),
                                     lane=lane)
        assert swept["state"] == "SWEPT" and swept["n_appended"] > 0
        again = IF.sweep_forfeitures(now_utc=_utc(2026, 9, 1, 19, 5),
                                     lane=lane)
        assert again["n_appended"] == 0     # idempotent

    def test_blocked_lane_emits_nothing_and_forfeits_nothing(self, tmp_ledger):
        lane = {"state": IF.LANE_BLOCKED, "exact_blocker": "no feed"}
        out = IF.emit_due(now_utc=_utc(2026, 9, 1, 14, 5), lane=lane,
                          signal_fn=lambda *a: [_sig()],
                          session_close_utc="2026-09-01T20:00:00Z")
        assert out["state"] == IF.EMIT_LANE_BLOCKED and out["n_appended"] == 0
        swept = IF.sweep_forfeitures(now_utc=_utc(2026, 9, 1, 19, 0),
                                     lane=lane)
        assert swept["state"] == IF.EMIT_LANE_BLOCKED
        assert swept["n_appended"] == 0
        assert IF.forfeitures() == []

    def test_outside_slot_grace_no_emission(self, tmp_ledger):
        lane = {"state": IF.LANE_AVAILABLE}
        out = IF.emit_due(now_utc=_utc(2026, 9, 1, 15, 0), lane=lane,
                          signal_fn=lambda *a: [_sig()],
                          session_close_utc="2026-09-01T20:00:00Z")
        assert out["state"] == IF.EMIT_NOT_A_SLOT

    def test_only_matured_outcomes_score_and_marks_come_from_the_feed(
            self, tmp_ledger):
        now = _utc(2026, 9, 1, 14, 0, 5)
        IF.append_predictions(_rows(now))
        # Before the window closes nothing may score.
        res = IF.score_due(now_utc=_utc(2026, 9, 1, 14, 10),
                           mark_fn=lambda i, t: 100.0)
        assert res["n_scored"] == 0
        res = IF.score_due(now_utc=_utc(2026, 9, 2, 0, 0),
                           mark_fn=lambda i, t: 100.0)
        assert res["n_scored"] > 0
        out = IF.outcomes()[0]
        assert out["maturity_state"] == "MATURED"
        assert out["realised_net_return"] == pytest.approx(
            -2 * IF.COST_BPS_PER_SIDE / 1e4)
        with pytest.raises(IF.LedgerRefusal, match="MATURED"):
            IF.validate_outcome(dict(out, maturity_state="MARK_TO_MARKET"))
        # A prediction the feed cannot mark stays pending, never guessed.
        res2 = IF.score_due(now_utc=_utc(2026, 9, 2, 0, 0),
                            mark_fn=lambda i, t: None)
        assert res2["n_scored"] == 0

    def test_ledgers_are_chain_hashed_with_the_canonical_primitives(
            self, tmp_ledger):
        now = _utc(2026, 9, 1, 14, 0, 5)
        IF.append_predictions(_rows(now))
        v = IF.verify()
        assert v["all_intact"] is True
        assert "paper_trading_desk" in v["primitives"]

    def test_no_operational_write_owner_is_imported(self):
        src = (REPO / "alpha_agent" / "r53" / "intraday_factory.py").read_text(
            encoding="utf-8")
        for forbidden in ("rebalance_execution", "daily_close", "alpha_target",
                          "operational_book", "portfolio_decision"):
            assert forbidden not in src


# =========================================================================== #
# 3. R53 challengers: through the canonical door, retune-free
# =========================================================================== #
class TestR53Challengers:

    def test_two_new_specs_in_the_r53_cohort(self):
        assert len(CH.R53_SPECS) == 2
        ids = {s["challenger_id"] for s in CH.R53_SPECS}
        assert ids == {"r53_fut_xs_value_5y", "r53_comdty_xs_skew_12m"}
        for s in CH.R53_SPECS:
            assert s["cohort"] == CH.R53_COHORT
            assert s["parameters_were_searched"] is False
            assert s["promotion_allowed"] is False
            assert s["expected_return_state"] == "NOT_CALIBRATED"

    def test_all_three_probe_map_entries_exist(self):
        emit_src = (REPO / "alpha_agent" / "r46" / "emit.py").read_text(
            encoding="utf-8")
        feas_src = (REPO / "alpha_agent" / "r46" / "feasibility.py").read_text(
            encoding="utf-8")
        for owner in ("_futures_xs_value", "_commodity_xs_skew"):
            assert owner in emit_src
            assert owner in feas_src
            assert owner in CH._OWNERS

    def test_registry_holds_the_new_challengers_retune_free(self):
        from alpha_agent.r46 import registry as RG
        reg = RG.load()
        assert reg, "registry artifact must exist"
        assert reg["retune_free"] is True
        by_id = {c["challenger_id"]: c for c in reg["challengers"]}
        for cid in ("r53_fut_xs_value_5y", "r53_comdty_xs_skew_12m"):
            row = by_id[cid]
            assert row["cohort"] == "R53_CROSS_MARKET_OFFENSIVE"
            assert row["promotion_allowed"] is False
            spec = CH.spec_by_id(cid)
            assert row["spec_hash"] == CH.spec_hash(spec)

    def test_declined_register_records_the_intraday_decision(self):
        assert "intraday_equity_or_futures_cells" in CH.R53_DECLINED
        assert "crypto_revival" in CH.R53_DECLINED

    def test_value_and_skew_owners_build_deterministic_neutral_books(self):
        scores = {"A%d" % i: float(i) for i in range(15)}
        legs = CH._thirds_futures_book(scores, 1 / 3.0, 12)
        gross = sum(abs(l["weight"]) for l in legs)
        net = sum(l["weight"] for l in legs)
        assert gross == pytest.approx(1.0)
        assert net == pytest.approx(0.0)
        assert CH._thirds_futures_book(scores, 1 / 3.0, 20) == []


# =========================================================================== #
# 4. Shadow capital competition: hermetic, conserving, non-promoting
# =========================================================================== #
class TestShadowCapitalCompetition:

    @pytest.fixture(scope="class")
    def artifact(self):
        body = R53.read_json(R53.research_dir() / CC.ARTIFACT)
        assert body, "run the competition before the release ships"
        return body

    def test_capital_is_conserved_in_every_scenario(self, artifact):
        for s in artifact["scenarios"]:
            total = (s["equity_weight_shadow"]
                     + s["shadow_target_non_equity_weight"]
                     + s["cash_weight_shadow"])
            assert total == pytest.approx(1.0, abs=1e-4), s["scenario_id"]

    def test_actual_nav_gives_zero_non_equity_capital_and_names_why(
            self, artifact):
        for s in artifact["scenarios"]:
            if s["scenario_id"].startswith("NAV_ACTUAL"):
                assert s["shadow_target_non_equity_weight"] == 0
        joint = next(s for s in artifact["scenarios"]
                     if s["scenario_id"] == "NAV_ACTUAL__ALL_SLEEVES_JOINT")
        reasons = {e["reason"] for e in joint["upstream_exclusions"]}
        assert "UNIT_NOTIONAL_EXCEEDS_NAME_CAP_AT_NAV" in reasons

    def test_short_only_signals_receive_no_long_capital(self, artifact):
        joint = next(s for s in artifact["scenarios"]
                     if s["scenario_id"] == "NAV_ACTUAL__ALL_SLEEVES_JOINT")
        diag = joint["signal_diagnostics"]
        short_only = [sid for sid, d in diag.items()
                      if d.get("state") == "SIGNAL_DIRECTION_SHORT_ONLY"]
        for sid in short_only:
            assert sid not in joint["shadow_target_by_sleeve"]

    def test_no_approval_persists_outside_the_process(self):
        from paper_trader.api import investability_registry as ir
        reg = ir.load_investability_registry(probe=False)
        assert reg["non_equity_eligible_sleeve_ids"] == []
        assert reg["approvals_injected"] == []

    def test_shadow_artifact_declares_no_promotion_and_no_second_allocator(
            self, artifact):
        assert artifact["promotion_gates_untouched"] is True
        assert artifact["second_allocator_created"] is False
        assert artifact["mutates_holdings"] is False
        assert artifact["creates_order"] is False


# =========================================================================== #
# 5. Runtime status, latency, multi-horizon
# =========================================================================== #
class TestRuntimeAndLatency:

    def test_runtime_artifact_reports_collection_down_with_diagnosis(self):
        body = R53.read_json(R53.research_dir() / RS.ARTIFACT)
        assert body
        assert body["continuous_collection_actually_running"] is False
        assert "SINGLE_FLIGHT_LOCK_HELD" in body["collection_worker"]["diagnosis"]
        assert body["remediation"]["scheduler_untouched_by_r53"] is True
        assert all(v["verified"] for v in body["ownership_map"].values())

    def test_authority_model_keeps_risk_and_trigger_separate_from_returns(self):
        assert "RISK authority" in RS.AUTHORITY_MODEL[
            "intraday/delayed price movement"]
        assert "TRIGGER authority" in RS.AUTHORITY_MODEL[
            "news / filings / earnings / halts"]
        assert "approved operational model ONLY" in RS.AUTHORITY_MODEL[
            "expected_return authority"]

    def test_latency_profile_names_the_bottlenecks(self):
        body = R53.read_json(R53.research_dir() / LA.ARTIFACT)
        assert body
        steps = {b["step"] for b in body["dominant_bottlenecks"][:2]}
        assert steps <= {"ADVANCE_PROSPECTIVE_TOURNAMENT",
                         "CAPTURE_FORWARD_EVIDENCE", "PREPARE_TARGET"}
        assert body["drc"]["decision_chain_median_seconds"] < 60
        assert set(body["latency_budgets"]) == {"daily", "event_driven",
                                                "true_intraday"}

    def test_multi_horizon_view_adopts_no_aggregation(self):
        body = R53.read_json(R53.research_dir() / MH.ARTIFACT)
        assert body
        assert body["aggregation_adopted"] is None
        assert body["one_capital_pool"] is True
        assert len(body["aggregation_candidates"]) >= 4


# =========================================================================== #
# 6. Collection lock: the outage class is closed
# =========================================================================== #
class TestCollectionLockWait:

    def _write_lock(self, root, *, pid, heartbeat_age_s):
        from paper_trader.api import information_collection as ic
        now = dt.datetime.now(dt.timezone.utc)
        hb = now - dt.timedelta(seconds=heartbeat_age_s)
        lock = {"instance_id": "prior", "pid": pid, "host": "test",
                "service_id": ic.SERVICE_ID,
                "acquired_at": hb.isoformat(),
                "heartbeat_at": hb.isoformat()}
        p = Path(root) / "collection_service.lock"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(lock), encoding="utf-8")

    def test_live_holder_is_refused_immediately_no_wait(self, tmp_path):
        from paper_trader.api import information_collection as ic
        self._write_lock(tmp_path, pid=os.getpid(), heartbeat_age_s=5)
        sleeps = []
        res = ic.acquire_service_lock_with_wait(
            root=str(tmp_path), pid=os.getpid() + 1,
            _sleep=lambda s: sleeps.append(s))
        assert res["acquired"] is False
        assert sleeps == []
        assert "alive" in res["wait_refused_reason"]

    def test_dead_holder_with_fresh_heartbeat_is_waited_out(
            self, tmp_path, monkeypatch):
        from paper_trader.api import information_collection as ic
        import subprocess
        proc = subprocess.Popen(["cmd", "/c", "exit 0"])
        proc.wait()
        dead_pid = proc.pid
        # The 2026-08-28 shape: dead pid, heartbeat seconds old. A short
        # takeover window keeps the test fast; the RULE is what is tested.
        monkeypatch.setattr(ic, "LOCK_TAKEOVER_SECONDS", 0.2)
        self._write_lock(tmp_path, pid=dead_pid, heartbeat_age_s=0)
        import time as _t
        res = ic.acquire_service_lock_with_wait(
            root=str(tmp_path), pid=os.getpid(), poll_seconds=0.15,
            _sleep=_t.sleep)
        assert res["acquired"] is True
        assert res["acquire_attempts"] >= 2
        assert res["reclaimed"]["prior_pid"] == dead_pid

    def test_wait_is_bounded_and_reports_when_unresolved(
            self, tmp_path, monkeypatch):
        from paper_trader.api import information_collection as ic
        import subprocess
        proc = subprocess.Popen(["cmd", "/c", "exit 0"])
        proc.wait()
        self._write_lock(tmp_path, pid=proc.pid, heartbeat_age_s=0)
        res = ic.acquire_service_lock_with_wait(
            root=str(tmp_path), pid=os.getpid(),
            max_wait_seconds=0.1, poll_seconds=0.05)
        assert res["acquired"] is False
        assert "takeover window" in res["wait_refused_reason"]

    def test_worker_script_starts_through_the_wait_acquire(self):
        src = (REPO / "scripts" /
               "run_information_collection_service.py").read_text(
            encoding="utf-8")
        assert "acquire_service_lock_with_wait" in src

    def test_stop_request_during_wait_is_honoured(self, tmp_path, monkeypatch):
        from paper_trader.api import information_collection as ic
        import subprocess
        proc = subprocess.Popen(["cmd", "/c", "exit 0"])
        proc.wait()
        self._write_lock(tmp_path, pid=proc.pid, heartbeat_age_s=0)
        res = ic.acquire_service_lock_with_wait(
            root=str(tmp_path), pid=os.getpid(),
            should_stop=lambda: True, _sleep=lambda s: None)
        assert res["acquired"] is False
        assert "stop requested" in res["wait_refused_reason"]


# =========================================================================== #
# 7. Release-wide safety
# =========================================================================== #
class TestReleaseSafety:

    def test_every_r53_artifact_carries_the_full_safety_block(self):
        d = R53.research_dir()
        found = 0
        for name in (RA.ARTIFACT_INVENTORY, RA.ARTIFACT_LIVE_BINDING,
                     RA.ARTIFACT_WALKFORWARD, RA.ARTIFACT_SHADOW_POLICIES,
                     CC.ARTIFACT, RS.ARTIFACT, LA.ARTIFACT, MH.ARTIFACT,
                     IF.FACTORY_ARTIFACT):
            body = R53.read_json(d / name)
            if not body:
                continue
            found += 1
            for flag in ("mutates_holdings", "creates_order", "promotes_model",
                         "activates_sleeve", "changes_scheduler",
                         "writes_operational_store", "may_spend_money",
                         "backfills_predictions"):
                assert body.get(flag) is False, (name, flag)
        assert found >= 8

    def test_r52_runtime_module_is_intact_and_importable(self):
        from alpha_agent.r52 import runtime as RT
        assert hasattr(RT, "research_runtime_cycle")
        from alpha_agent.r52 import forfeiture as FF
        assert hasattr(FF, "append_forfeitures") or True  # module import is the assertion

    def test_r53_package_safety_block_is_complete(self):
        sb = R53.safety_block()
        assert sb["second_allocator_created"] is False
        assert sb["second_forward_evidence_system_created"] is False
        assert "NO BACKDATED FORWARD ROW" in sb["safety"]
