"""Stage 23 — hermetic regressions for the unified alpha research owner.

Every test builds its own synthetic panel on disk under tmp_path and points the
module at it through the documented env seams, so nothing here reads an owned
research panel, an operational store, the network, PostgreSQL or the prediction
service, and nothing here can promote a model.

The invariants under test are the ones Stage 23 introduced and that a later
change could silently break:

  * the point-in-time boundary of the trailing-return reconstruction
  * the (ticker, month) deduplication of the staggered fundamental panel
  * the frozen 50/50 blend and the shared joint universe
  * gate DELEGATION — Stage 23 never overrides a released verdict
  * sector neutralisation stays BLOCKED and never uses the look-ahead map
  * artifact idempotency
  * the two Stage-23 analyst-contract invariants
"""
from __future__ import annotations

import csv
import json
import math
from pathlib import Path

import pytest

from paper_trader.alpha_agent import analyst_revisions as ar
from paper_trader.alpha_agent import stage23_unified as s23
from paper_trader.alpha_agent import tournament as tt

# --------------------------------------------------------------------------- #
# Synthetic panel fixtures.
# --------------------------------------------------------------------------- #
MOM_COLS = ["month", "market_date", "ticker", "mom_6_1", "fwd_1m_return",
            "is_member", "adv_dollar", "realized_vol_63d", "eligible_history",
            "sector"]

FUND_COLS = ["as_of_date", "rebalance_date", "ticker", "sector", "liquidity_proxy",
             "fcf_to_assets_sector_neutral_z",
             "operating_accruals_oriented_sector_neutral_z",
             "composite_sn", "forward_63d_return"]


def _months(n: int, start_year: int = 2010) -> list:
    out = []
    y, m = start_year, 1
    for _ in range(n):
        out.append("%04d-%02d" % (y, m))
        m += 1
        if m > 12:
            m, y = 1, y + 1
    return out


def write_momentum_panel(path: Path, *, n_months: int = 60, n_names: int = 40,
                         signal_strength: float = 0.0) -> Path:
    """A deterministic panel. ``signal_strength`` injects a controlled relation
    between mom_6_1 and the NEXT month's return so a positive control exists."""
    months = _months(n_months)
    rows = []
    for mi, month in enumerate(months):
        for ni in range(n_names):
            t = "T%03d" % ni
            # deterministic pseudo-noise, no randomness (byte-reproducible)
            mom = math.sin(mi * 0.7 + ni * 1.3)
            noise = math.cos(mi * 1.1 + ni * 0.37) * 0.05
            fwd = signal_strength * mom * 0.05 + noise
            rows.append({
                "month": month,
                "market_date": "%s-28" % month,
                "ticker": t,
                "mom_6_1": "%.8f" % mom,
                "fwd_1m_return": "%.8f" % fwd,
                "is_member": "1",
                "adv_dollar": "%.2f" % (1.0e7 * (1.0 + ni) * (1.0 + 0.01 * mi)),
                "realized_vol_63d": "%.6f" % (0.2 + 0.01 * (ni % 7)),
                "eligible_history": "1",
                "sector": "Unknown",
            })
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=MOM_COLS)
        w.writeheader()
        w.writerows(rows)
    return path


def write_fundamental_panel(path: Path, *, n_months: int = 40, n_names: int = 40,
                            seed_month_duplicates: int = 5,
                            start_year: int = 2010) -> Path:
    """A staggered panel whose FIRST month deliberately repeats each ticker, to
    reproduce the real panel's bulk history-seed month.

    ``start_year`` defaults to the momentum fixture's start so the joint-universe
    tests have a real overlap.
    """
    months = _months(n_months, start_year=start_year)
    rows = []
    for mi, month in enumerate(months):
        reps = seed_month_duplicates if mi == 0 else 1
        for rep in range(reps):
            for ni in range(n_names):
                cs = math.sin(mi * 0.41 + ni * 0.83)
                fwd = math.cos(mi * 0.29 + ni * 0.61) * 0.04
                rows.append({
                    "as_of_date": "2026-06-26",
                    "rebalance_date": "%s-%02d" % (month, 5 + rep),
                    "ticker": "T%03d" % ni,
                    "sector": "Unknown",
                    "liquidity_proxy": "1.0e8",
                    "fcf_to_assets_sector_neutral_z": "%.6f" % cs,
                    "operating_accruals_oriented_sector_neutral_z": "%.6f" % (-cs),
                    "composite_sn": "%.6f" % cs,
                    "forward_63d_return": "%.6f" % fwd,
                })
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FUND_COLS)
        w.writeheader()
        w.writerows(rows)
    return path


def write_sector_map(path: Path, n_names: int = 10) -> Path:
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["ticker", "repaired_sector"])
        w.writeheader()
        for ni in range(n_names):
            w.writerow({"ticker": "T%03d" % ni, "repaired_sector": "Industrials"})
    return path


@pytest.fixture()
def gate_cfg() -> dict:
    cfg_path = (Path(__file__).resolve().parents[1] / "configs" / "alpha_agent"
                / "stage9_tournament.json")
    return json.loads(cfg_path.read_text(encoding="utf-8-sig"))


@pytest.fixture()
def panels(tmp_path):
    mom = write_momentum_panel(tmp_path / "mom.csv")
    fund = write_fundamental_panel(tmp_path / "fund.csv")
    sec = write_sector_map(tmp_path / "sec.csv")
    return {"mom": mom, "fund": fund, "sec": sec}


# =========================================================================== #
# POINT-IN-TIME BOUNDARY — the invariant a leakage bug would break first.
# =========================================================================== #
def test_trailing_returns_never_include_the_target_month(panels):
    """``fwd_1m_return(m)`` is the TARGET at month m and must never appear in a
    feature evaluated at m. The newest permissible trailing return is the one
    realised over (m-1 -> m)."""
    panel = s23.load_momentum_panel(panels["mom"])
    month = panel.months[30]
    prev = panel.months[29]
    ticker = panel.by_month[month][0]["ticker"]

    target = next(r["fwd_1m_return"] for r in panel.by_month[month]
                  if r["ticker"] == ticker)
    newest_trailing = next(r["fwd_1m_return"] for r in panel.by_month[prev]
                           if r["ticker"] == ticker)

    trailing = panel.trailing_returns(ticker, month, lookback=6)
    assert trailing, "expected a trailing history"
    assert trailing[-1] == pytest.approx(newest_trailing), (
        "the newest trailing return must be the one realised over (m-1 -> m)")
    assert all(v != pytest.approx(target) for v in trailing if v is not None), (
        "LEAKAGE: the target month's forward return appeared in the trailing window")


def test_skip_recent_shifts_the_window_back(panels):
    panel = s23.load_momentum_panel(panels["mom"])
    month = panel.months[30]
    ticker = panel.by_month[month][0]["ticker"]
    full = panel.trailing_returns(ticker, month, lookback=6, skip_recent=0)
    skipped = panel.trailing_returns(ticker, month, lookback=6, skip_recent=1)
    assert full[-1] != skipped[-1]
    assert full[:-1] == skipped[1:] or full[-2] == pytest.approx(skipped[-1])


def test_every_feature_is_computable_without_the_target(panels):
    """Recomputing a feature after BLANKING the target column must not change it."""
    panel = s23.load_momentum_panel(panels["mom"])
    month = panel.months[40]
    row = panel.by_month[month][3]
    before = {h["hypothesis_id"]: h["feature_fn"](panel, row, month)
              for h in s23.stage23_hypotheses()}
    for r in panel.by_month[month]:
        r["fwd_1m_return"] = None
    after = {h["hypothesis_id"]: h["feature_fn"](panel, row, month)
             for h in s23.stage23_hypotheses()}
    assert before == after, (
        "a feature changed when the target month's forward return was removed, "
        "which means it was reading the target")


# =========================================================================== #
# PANEL ADAPTERS.
# =========================================================================== #
def test_fundamental_panel_deduplicates_the_seed_month(panels):
    fund = s23.load_fundamental_panel(panels["fund"])
    d = fund["diagnostics"]
    assert d["duplicate_ticker_month_rows_dropped"] > 0
    seen = set()
    for m, rows in fund["by_month"].items():
        for r in rows:
            key = (r["ticker"], m)
            assert key not in seen, "duplicate (ticker, month) survived dedupe"
            seen.add(key)


def test_fundamental_panel_is_labelled_survivor_biased(panels):
    fund = s23.load_fundamental_panel(panels["fund"])
    assert fund["diagnostics"]["survivorship_safe"] is False
    assert "SURVIVOR-BIASED" in fund["diagnostics"]["survivorship_basis"]


def test_fundamental_periods_exclude_the_seed_month(panels):
    fund = s23.load_fundamental_panel(panels["fund"])
    periods, cov = s23.build_fundamental_periods(fund, min_names=5)
    assert cov["seed_month_excluded"] == fund["months"][0]
    assert all(p["as_of"] != fund["months"][0] for p in periods)


def test_sector_map_is_never_usable_for_neutralisation(panels):
    sector = s23.load_sector_map(panels["sec"])
    assert sector["point_in_time"] is False
    assert sector["usable_for_neutralisation"] is False
    assert sector["map"], "the map should still load so the wall can be MEASURED"


# =========================================================================== #
# JOINT UNIVERSE + FROZEN BLEND.
# =========================================================================== #
def test_joint_blend_uses_the_frozen_operational_weights(panels):
    panel = s23.load_momentum_panel(panels["mom"])
    fund = s23.load_fundamental_panel(panels["fund"])
    ens, _ = s23.build_joint_periods(panel, fund, min_names=5)
    fnd, _ = s23.build_joint_periods(panel, fund, min_names=5,
                                     leg=s23.COMPONENT_FUNDAMENTAL)
    mom, _ = s23.build_joint_periods(panel, fund, min_names=5,
                                     leg=s23.COMPONENT_MOMENTUM)
    assert ens and fnd and mom
    d = ens[0]["as_of"]
    ev = {t: v for t, v, _f in ens[0]["names"]}
    fv = {t: v for t, v, _f in next(p for p in fnd if p["as_of"] == d)["names"]}
    mv = {t: v for t, v, _f in next(p for p in mom if p["as_of"] == d)["names"]}
    for t in ev:
        expected = (s23.COMPONENT_WEIGHTS[s23.COMPONENT_FUNDAMENTAL] * fv[t]
                    + s23.COMPONENT_WEIGHTS[s23.COMPONENT_MOMENTUM] * mv[t])
        assert ev[t] == pytest.approx(expected, abs=1e-9)


def test_joint_universe_is_the_same_names_on_every_leg(panels):
    panel = s23.load_momentum_panel(panels["mom"])
    fund = s23.load_fundamental_panel(panels["fund"])
    fnd, _ = s23.build_joint_periods(panel, fund, min_names=5,
                                     leg=s23.COMPONENT_FUNDAMENTAL)
    mom, _ = s23.build_joint_periods(panel, fund, min_names=5,
                                     leg=s23.COMPONENT_MOMENTUM)
    fby = {p["as_of"]: {t for t, _v, _f in p["names"]} for p in fnd}
    mby = {p["as_of"]: {t for t, _v, _f in p["names"]} for p in mom}
    assert fby and fby.keys() == mby.keys()
    for d in fby:
        assert fby[d] == mby[d], "legs must be compared on identical cross-sections"


# =========================================================================== #
# GATE DELEGATION — Stage 23 reports a verdict, it never issues one.
# =========================================================================== #
def test_classification_follows_the_released_gate(gate_cfg):
    keep = {"gate": {"target_state": tt.KEEP_FOR_RESEARCH},
            "metrics": {}, "fdr": {"survives_fdr_alpha_005": True}}
    assert s23.classify_stage23_result(keep) == s23.CLS_PROMISING

    keep_no_fdr = {"gate": {"target_state": tt.KEEP_FOR_RESEARCH},
                   "metrics": {}, "fdr": {"survives_fdr_alpha_005": False}}
    assert s23.classify_stage23_result(keep_no_fdr) == s23.CLS_NEEDS_MORE

    redundant = {"gate": {"target_state": tt.KEEP_FOR_RESEARCH}, "metrics": {},
                 "fdr": {"survives_fdr_alpha_005": True},
                 "correlation_vs_operational_momentum_leg": 0.95}
    assert s23.classify_stage23_result(redundant) == s23.CLS_REDUNDANT

    held = {"gate": {"target_state": tt.DATA_HOLD,
                     "blocker": "DATA_HOLD_POINT_IN_TIME_UNAVAILABLE"},
            "metrics": {}, "fdr": {}}
    assert s23.classify_stage23_result(held) == s23.CLS_FAILED_PIT


def test_no_classification_maps_to_an_auto_promoted_state():
    """No Stage-23 classification may map to a state that deploys capital."""
    forbidden = {tt.READY_FOR_MANUAL_REVIEW, tt.SHADOW_BOOK_ACTIVE}
    for cls, state in s23.CLASSIFICATION_TO_LIFECYCLE.items():
        assert state not in forbidden, (
            "%s maps to %s, which would advance a candidate without the manual "
            "review gate" % (cls, state))


def test_campaign_runs_end_to_end_and_preserves_nulls(panels, gate_cfg):
    panel = s23.load_momentum_panel(panels["mom"])
    campaign = s23.run_owned_campaign(panel, gate_cfg)
    assert campaign["summary"]["family_size"] == len(s23.stage23_hypotheses())
    assert campaign["summary"]["evaluated"] == campaign["summary"]["family_size"]
    assert campaign["summary"]["no_automatic_promotion"] is True
    for r in campaign["results"]:
        assert r["classification"] in s23.CLASSIFICATION_TO_LIFECYCLE
        assert "economic_rationale" in r and r["economic_rationale"]
        assert "rejection_criteria" in r
        assert r["fdr"]["family_size"] == campaign["summary"]["family_size"]


def test_fdr_is_applied_across_the_whole_family(panels, gate_cfg):
    panel = s23.load_momentum_panel(panels["mom"])
    campaign = s23.run_owned_campaign(panel, gate_cfg)
    qs = [r["fdr"]["bh_q_value"] for r in campaign["results"]
          if r["fdr"].get("bh_q_value") is not None]
    ps = [r["fdr"]["p_value_two_sided_normal_approx"] for r in campaign["results"]
          if r["fdr"].get("p_value_two_sided_normal_approx") is not None]
    assert qs, "expected q-values"
    assert all(0.0 <= q <= 1.0 for q in qs)
    assert all(q >= p - 1e-12 for q, p in zip(qs, ps)), (
        "a BH q-value can never be smaller than its own p-value")


def test_hypotheses_declare_prior_research_and_duplicates():
    """The mandate forbids re-running correlated variants unknowingly."""
    hyps = s23.stage23_hypotheses()
    assert hyps
    for h in hyps:
        assert h["economic_rationale"], "%s has no economic rationale" % h["hypothesis_id"]
        assert h["expected_mechanism"]
        assert h["primary_metric"] == "rank_ic_t"
    replications = [h for h in hyps if h.get("near_duplicate_of")]
    for h in replications:
        assert h["duplicate_note"], (
            "%s is declared a near-duplicate but does not say why it is still "
            "worth running" % h["hypothesis_id"])


# =========================================================================== #
# NEUTRALISATION — sector must stay blocked.
# =========================================================================== #
def test_sector_neutralisation_is_always_reported_blocked(panels):
    panel = s23.load_momentum_panel(panels["mom"])
    rep = s23.neutralisation_report(panel, s23.f_mom_6_1,
                                    sector_blocked_reason={"reason": "no PIT sector"})
    assert rep["status"] == "MEASURED"
    assert rep["sector_neutral"]["status"] == "BLOCKED_NO_POINT_IN_TIME_SECTOR"
    assert "look-ahead" in rep["sector_neutral"]["not_substituted"]
    for name in ("log_adv", "realized_vol", "market_beta"):
        assert name in rep["neutralised"], "%s neutralisation should be measurable" % name


def test_concentration_and_subperiod_reports_degrade_honestly():
    thin = {"dates": ["2020-01"], "ic": [0.1], "ls": [0.01]}
    assert s23.concentration_report(thin)["status"] == "INSUFFICIENT_PERIODS"
    assert s23.subperiod_report(thin)["status"] == "INSUFFICIENT_PERIODS"


# =========================================================================== #
# REPORTING SURFACES.
# =========================================================================== #
def test_capability_matrix_uses_the_declared_vocabulary(panels):
    panel = s23.load_momentum_panel(panels["mom"])
    fund = s23.load_fundamental_panel(panels["fund"])
    sector = s23.load_sector_map(panels["sec"])
    cap = s23.build_capability_matrix(panel, fund, sector)
    assert cap["families"]
    for f in cap["families"]:
        assert f["status"] in cap["vocabulary"], (
            "%s uses a status outside the declared vocabulary" % f["family"])
    sec_fam = next(f for f in cap["families"] if f["family"] == "SECTOR_NEUTRAL")
    assert sec_fam["status"] == s23.CAP_REJECTED_BASIS
    analyst = next(f for f in cap["families"] if f["family"] == "ANALYST_REVISIONS")
    assert analyst["status"] == s23.CAP_WAITING_INTRINIO


def test_priority_queue_orders_ready_work_above_blocked_work(panels, gate_cfg):
    panel = s23.load_momentum_panel(panels["mom"])
    campaign = s23.run_owned_campaign(panel, gate_cfg)
    held = [{"candidate_id": "c9_x", "name": "Earnings-estimate revisions",
             "family": "ANALYST_EARNINGS", "blocker": "DATA_HOLD_INSUFFICIENT_OBSERVATIONS",
             "data_dependencies": ["eodhd_analyst_vintages"]}]
    q = s23.build_priority_queue(campaign=campaign, held_candidates=held)
    assert q["no_opaque_composite_score"] is True
    analyst = next(e for e in q["entries"] if e["kind"] == "EXISTING_TOURNAMENT_DATA_HOLD")
    assert analyst["resolved_by_historical_analyst_vendor"] is True
    assert analyst["classification"] == s23.CLS_WAITING_INTRINIO
    order = {s23.PRIORITY_HIGH: 0, s23.PRIORITY_MEDIUM: 1,
             s23.PRIORITY_WAITING: 2, s23.PRIORITY_LOW: 3}
    ranks = [order[e["priority"]] for e in q["entries"]]
    assert ranks == sorted(ranks), "queue must be ordered by priority bucket"
    for e in q["entries"]:
        assert e["priority_reason"], "every queue entry must justify its position"


def test_queue_supersedes_a_data_hold_that_stage23_actually_evaluated(panels, gate_cfg):
    """A DATA_HOLD Stage 23 tested must stop being reported as unexplored work."""
    panel = s23.load_momentum_panel(panels["mom"])
    campaign = s23.run_owned_campaign(panel, gate_cfg)
    target = next(r["resolves_existing_data_hold"] for r in campaign["results"]
                  if r.get("resolves_existing_data_hold"))
    held = [{"candidate_id": target, "name": "Dollar-volume shock (21d)",
             "family": "PRICE_MOMENTUM",
             "blocker": "DATA_HOLD_REQUIRES_VOLUME_TURNOVER_DATA",
             "data_dependencies": ["owned_volume_turnover"]}]
    q = s23.build_priority_queue(campaign=campaign, held_candidates=held)
    entry = next(e for e in q["entries"] if e.get("candidate_id") == target)
    assert entry["priority"] != s23.PRIORITY_WAITING
    assert entry["superseded_by_stage23_hypothesis"]
    assert "still unowned" in entry["priority_reason"], (
        "superseding must be honest that the DAILY specification remains untested")


def test_intrinio_readiness_contract_is_complete_and_invents_nothing():
    r = s23.build_intrinio_readiness()
    assert r["no_provider_schema_invented"] is True
    assert r["provider_data_present_today"]["historical_analyst_vintages"] is False
    assert r["contract_complete"] is True, (
        "required validations missing: %s" % r["validations_missing_from_contract"])
    for required, invariant in s23.REQUIRED_VALIDATIONS.items():
        if invariant.startswith("adequacy gate"):
            continue
        assert invariant in r["pit_invariants"], (
            "%r has no detecting invariant" % required)
    assert r["safety"]["no_promotion"] is True


def test_preregistration_adds_the_incremental_requirement():
    pre = s23.build_analyst_preregistration()
    assert pre["family_size"] == 6
    assert pre["registry_version"], "the frozen registry must be content-addressed"
    assert pre["frozen_before_any_trial_data"] is True
    add = pre["stage23_addition"]
    assert add["baseline_model"] == s23.OPERATIONAL_STRATEGY_ID
    assert "ADDS" in add["comparison"]
    for h in pre["hypotheses"]:
        assert h["stage23_incremental_requirement"], (
            "%s must be judged on incremental value, not isolation" % h["id"])
        assert h["stage23_baseline_model"] == s23.OPERATIONAL_STRATEGY_ID
    xv = pre["cross_validation_with_prospective_evidence"]
    assert xv["evidence_classes_must_not_be_mixed"] is True


def test_preregistration_carries_the_measured_baseline_when_available():
    attribution = {
        "stage23_version": s23.STAGE23_VERSION,
        "joint_universe": {"ensemble_50_50": {"metrics": {
            "scored_periods": 117, "rank_ic": 0.043, "rank_ic_t": 2.83,
            "spread_t": 2.23, "net25_spread": 0.074}}},
        "redundancy": {"partial_rank_ic": {
            "composite_sn_controlling_for_mom_6_1": {"t_stat": 3.09},
            "mom_6_1_controlling_for_composite_sn": {"t_stat": 1.28}}},
    }
    pre = s23.build_analyst_preregistration(attribution=attribution)
    mb = pre["stage23_addition"]["measured_baseline"]
    assert mb["ensemble_spread_t"] == 2.23
    assert mb["composite_sn_partial_t"] == 3.09
    assert "SURVIVOR-BIASED" in mb["caveat"], (
        "the baseline's survivorship caveat must travel with it")


def test_decision_link_refuses_to_conclude_without_forward_evidence():
    link = s23.build_decision_link({}, forward_records=[])
    assert link["status"] == "INSUFFICIENT_FORWARD_EVIDENCE"
    assert link["conclusion"]
    assert link["never_rewrites_history"] is True
    assert link["operational_mutation"] is False
    counterfactuals = [m for m in link["measures"]
                       if m["evidence_class"] == "COUNTERFACTUAL_NOT_PROOF"]
    assert counterfactuals, "re-ranking past holdings must be labelled counterfactual"
    for m in link["measures"]:
        assert m["evidence_class"] in ("COUNTERFACTUAL_NOT_PROOF", "TRUE_FORWARD")


def test_artifacts_are_idempotent_and_fingerprinted(tmp_path):
    docs = {"alpha": {"b": 2, "a": 1}, "beta": {"x": [3, 1, 2]}}
    rid = s23.run_id_for({"k": "v"})
    first = s23.write_artifacts(tmp_path, docs, run_id=rid)
    second = s23.write_artifacts(tmp_path, docs, run_id=rid)
    assert first["run_dir"] == second["run_dir"]
    for name in docs:
        assert first["documents"][name]["sha256"] == second["documents"][name]["sha256"]
    assert first["operational_mutation"] is False
    assert first["automatic_promotion"] is False
    assert json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))["run_id"] == rid


def test_run_id_is_a_deterministic_function_of_its_inputs():
    a = s23.run_id_for({"x": 1, "y": 2})
    b = s23.run_id_for({"y": 2, "x": 1})
    c = s23.run_id_for({"x": 1, "y": 3})
    assert a == b, "run_id must not depend on key order"
    assert a != c


# =========================================================================== #
# SAFETY.
# =========================================================================== #
def test_module_names_no_operational_store():
    """Stage 23 must not reference a desk/book/order/decision store path."""
    src = Path(s23.__file__).read_text(encoding="utf-8")
    for forbidden in ("paper_trading_desk", "PAPER_TRADER_DESK_DIR",
                      "rebalance_order_plans", "portfolio_decisions",
                      "PAPER_TRADER_BOOK_DIR", "daily_close"):
        assert forbidden not in src, (
            "Stage 23 must not reach into the operational store %r" % forbidden)


def test_module_opens_no_network_or_database():
    src = Path(s23.__file__).read_text(encoding="utf-8")
    for forbidden in ("import requests", "urllib.request", "psycopg", "socket",
                      "http://", "https://"):
        assert forbidden not in src, "Stage 23 must stay offline (%r)" % forbidden


def test_stage23_is_not_a_second_research_agent_calculation_owner():
    """``engine/research_agent.py`` is the SOLE Persistent Research Agent
    calculation owner. ``scripts/audit_architecture.py`` proves that by requiring
    the token ``def evaluate(`` to appear in no other module, so Stage 23 must
    never reintroduce a bare ``evaluate``. Its signal evaluation is a different
    concept and carries a domain-specific name."""
    src = Path(s23.__file__).read_text(encoding="utf-8")
    assert "def evaluate(" not in src, (
        "Stage 23 would collide with the Persistent Research Agent calculation "
        "owner (engine/research_agent.evaluate); name the Stage-23 function for "
        "what it actually evaluates instead.")
    assert hasattr(s23, "evaluate_cross_sectional_signal")
    assert not hasattr(s23, "evaluate")


def test_stage23_signal_evaluation_owns_no_statistic_and_no_threshold():
    """The rename must not have hidden a duplicated calculation: Stage 23 has to
    keep delegating the math to ``signal_evaluation.evaluate_periods`` and the
    verdict to the released tournament gate."""
    import inspect

    src = inspect.getsource(s23.evaluate_cross_sectional_signal)
    for delegated in ("evaluate_periods(", "row_to_contract_metrics(",
                      "classify_evidence("):
        assert delegated in src, (
            "Stage 23 must delegate %r to the released owner" % delegated)
    for forked in ("math.sqrt", "def ", "for ", "while ", "0.25", "25bps"):
        assert forked not in src.split("\n", 1)[1].split('"""')[-1], (
            "Stage 23 must not compute a statistic or restate a threshold (%r)"
            % forked)


def test_registration_helper_seeds_and_never_promotes(tmp_path, panels, gate_cfg):
    panel = s23.load_momentum_panel(panels["mom"])
    campaign = s23.run_owned_campaign(panel, gate_cfg)
    reg = tt.CandidateRegistry(tmp_path / "t.sqlite")
    try:
        out = s23.register_campaign_candidates(reg, gate_cfg, campaign)
        assert out["no_automatic_promotion"] is True
        assert len(out["seeded"]) == len(campaign["results"])
        # Re-registering the identical campaign must be idempotent.
        again = s23.register_campaign_candidates(reg, gate_cfg, campaign)
        assert [s["candidate_id"] for s in again["seeded"]] == \
               [s["candidate_id"] for s in out["seeded"]]
        states = {c["lifecycle_state"] for c in reg.list()}
        assert not (states & {tt.READY_FOR_MANUAL_REVIEW, tt.SHADOW_BOOK_ACTIVE}), (
            "registration must never advance a candidate past the gate")
    finally:
        reg.close()


# =========================================================================== #
# STAGE-23 ANALYST-CONTRACT ADDITIONS (Workstream G gap fill).
# =========================================================================== #
def _event(**over) -> dict:
    rec = {
        "record_id": "r1", "provider": "TEST", "provider_event_id": "e1",
        "security_id": "SEC1", "estimate_type": "EPS",
        "fiscal_period_type": "QUARTERLY", "fiscal_period_end": "2026-03-31",
        "observation_timestamp": "2026-02-01T00:00:00Z",
        "provider_effective_timestamp": "2026-02-01T00:00:00Z",
        "revised_estimate": 1.25,
        "source_availability_timestamp": "2026-02-01T12:00:00Z",
        "ingestion_timestamp": "2026-02-02T00:00:00Z",
        "analyst_or_broker_id": "B1",
    }
    rec.update(over)
    return rec


def test_duplicate_revision_is_detected():
    seen: set = set()
    first = ar.pit_validate_event(_event(), seen_logical_keys=seen)
    assert ar.PIT_DUPLICATE_REVISION not in first
    # Same economic event redelivered under a different provider row id.
    second = ar.pit_validate_event(_event(record_id="r2", provider_event_id="e2"),
                                   seen_logical_keys=seen)
    assert ar.PIT_DUPLICATE_REVISION in second


def test_distinct_revisions_are_not_flagged_as_duplicates():
    seen: set = set()
    ar.pit_validate_event(_event(), seen_logical_keys=seen)
    other = ar.pit_validate_event(
        _event(record_id="r2", observation_timestamp="2026-02-08T00:00:00Z"),
        seen_logical_keys=seen)
    assert ar.PIT_DUPLICATE_REVISION not in other


def test_per_share_estimate_must_declare_a_corporate_action_basis():
    missing = ar.pit_validate_event(_event(), require_corporate_action_basis=True)
    assert ar.PIT_CORPORATE_ACTION_BASIS_UNDECLARED in missing
    declared = ar.pit_validate_event(
        _event(corporate_action_basis="AS_REPORTED_VINTAGE"),
        require_corporate_action_basis=True)
    assert ar.PIT_CORPORATE_ACTION_BASIS_UNDECLARED not in declared


def test_corporate_action_basis_check_is_off_by_default():
    """The prospective Stage-13B ledger predates the field and must keep validating."""
    assert ar.PIT_CORPORATE_ACTION_BASIS_UNDECLARED not in ar.pit_validate_event(_event())


def test_new_invariants_are_registered_and_counted():
    assert ar.PIT_DUPLICATE_REVISION in ar.PIT_INVARIANTS
    assert ar.PIT_CORPORATE_ACTION_BASIS_UNDECLARED in ar.PIT_INVARIANTS
    scan = ar.pit_scan(events=[_event(), _event(record_id="r2")])
    assert scan["violation_counts"][ar.PIT_DUPLICATE_REVISION] == 1
    assert scan["no_silent_repair"] is True


def test_new_schema_fields_are_optional_so_old_records_still_validate():
    assert ar.validate_record("ESTIMATE_REVISION_EVENT", _event()) == []
    rich = _event(fiscal_year=2026, fiscal_quarter=1,
                  corporate_action_basis="SPLIT_ADJUSTED_TO_CURRENT")
    assert ar.validate_record("ESTIMATE_REVISION_EVENT", rich) == []
