"""
Stage 9.4 - orthogonal alpha discovery, PIT companyfacts and selection-bias
control. Deterministic, offline, targeted tests for the reusable capabilities the
AlphaAgent uses AUTONOMOUSLY from its canonical collect loop. No network, no
operational-ledger write, no prediction service.

The 30 numbered properties from the Stage 9.4 contract are covered by test_p01..
test_p30; the trailing test_a* cases prove the agent-integration wiring (the
catalogue is seeded, enriched and classified through the canonical tournament).
The test_rc* cases cover the Stage 9.4 RELEASE-CLOSURE corrections: genuine
forward-only walk-forward folds, cross-sectional regime semantics, the canonical
queued-experiment loop (bounded revalidation generation + ingestible row +
narrow drain allowlist) and the deferred-companyfacts decision.
"""
from __future__ import annotations

import datetime as dt
import json
import os
import random
import subprocess
from pathlib import Path

import pytest

from alpha_agent import (orthogonality as orth, regimes as rg,
                         selection_controls as sctl, price_factors as pf,
                         price_factor_catalogue as cat,
                         factor_combinations as fc, sec_companyfacts as cf,
                         fundamental_signals as fs, pit_fundamentals as pfd,
                         stage9_4 as s94, tournament as tt,
                         experiment_runner as er, acquisition_campaign as ac,
                         autonomous_research as ar)

_STAGE8_CFG = json.loads((_REPO_MARK := Path(__file__).resolve().parents[1])
                         .joinpath("configs", "alpha_agent",
                                   "stage8_autonomy.json")
                         .read_text(encoding="utf-8-sig"))

_REPO = Path(__file__).resolve().parents[1]
_CFG = json.loads((_REPO / "configs" / "alpha_agent" /
                   "stage9_tournament.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- #
# Deterministic synthetic owned-like price panel.
# --------------------------------------------------------------------------- #
def _panel(*, names: int = 60, days: int = 520, seed: int = 7) -> dict:
    rnd = random.Random(seed)
    dates = []
    d = dt.date(2016, 1, 4)
    while len(dates) < days:
        if d.weekday() < 5:
            dates.append(d.isoformat())
        d += dt.timedelta(days=1)
    panel = {}
    for n in range(names):
        px = 40.0 + rnd.random() * 60.0
        series = []
        for ds in dates:
            px *= (1.0 + rnd.gauss(0.0004, 0.013))
            series.append((ds, round(px, 4)))
        panel["S%02d" % n] = series
    return panel


def _companyfacts_doc(cik=320193):
    return {"cik": cik, "facts": {"us-gaap": {
        "Assets": {"units": {"USD": [
            {"end": "2020-12-31", "val": 1000, "accn": "a1", "fy": 2020,
             "fp": "FY", "form": "10-K", "filed": "2021-02-01"},
            {"end": "2020-12-31", "val": 1050, "accn": "a2", "fy": 2020,
             "fp": "FY", "form": "10-K/A", "filed": "2021-08-01"},
            {"end": "2020-12-31", "val": 1000, "accn": "a1", "fy": 2020,
             "fp": "FY", "form": "10-K", "filed": "2021-02-01"},
            {"end": "2019-12-31", "val": 900, "accn": "a0", "fy": 2019,
             "fp": "FY", "form": "10-K", "filed": "2020-02-01"}]}},
        "Revenues": {"units": {"USD": [
            {"end": "2020-12-31", "val": 500, "accn": "a1", "fy": 2020,
             "fp": "FY", "form": "10-K", "filed": "2021-02-01"}]}},
        "CostOfRevenue": {"units": {"USD": [
            {"end": "2020-12-31", "val": 300, "accn": "a1", "fy": 2020,
             "fp": "FY", "form": "10-K", "filed": "2021-02-01"}]}},
        "SharesOutstanding": {"units": {"shares": [
            {"end": "2020-12-31", "val": 1e9, "accn": "a1",
             "filed": "2021-02-01"}]}}}}}


# ===== Track A: pre-registered catalogue ==================================== #
def test_p01_candidate_specs_deterministic():
    assert [c["spec"] for c in cat.catalogue_specs()] == \
        [c["spec"] for c in cat.catalogue_specs()]
    assert cat.CATALOGUE_VERSION


def test_p02_existing_factors_not_duplicated():
    existing = {(s["family"], tt.spec_hash(s["spec"]))
                for s in tt.default_candidate_specs()}
    for c in cat.catalogue_specs():
        assert (c["family"], tt.spec_hash(c["spec"])) not in existing


def test_p03_family_variant_caps_enforced():
    for h in cat.PRE_REGISTERED:
        assert len(h["horizons"]) <= 3
    # every seeded candidate id is unique (no accidental variant collision)
    ids = [tt.candidate_id_for(c["family"], c["spec"]) for c in cat.catalogue_specs()]
    assert len(ids) == len(set(ids))


def test_p04_feature_calc_uses_only_data_through_t():
    panel = _panel()
    t = "S00"
    closes = [c for _, c in panel[t]]
    dates = [d for d, _ in panel[t]]
    mret = pf.build_market_return_series(panel)
    idx = 300
    for feat in ("trend_slope_t", "market_residual_momentum", "low_beta",
                 "channel_breakout"):
        v0 = pf.factor_value(feat, name_dates=dates, name_closes=closes,
                             idx=idx, mret_by_date=mret)
        # Perturb ONLY the future (strictly after idx); value at idx must not move.
        future = closes[:idx + 1] + [c * 9.9 for c in closes[idx + 1:]]
        v1 = pf.factor_value(feat, name_dates=dates, name_closes=future,
                             idx=idx, mret_by_date=mret)
        assert v0 == v1


# ===== Track B: orthogonality ============================================== #
def test_p05_residualization_removes_market_exposure():
    rnd = random.Random(1)
    mkt = [rnd.gauss(0, 1) for _ in range(200)]
    sig = [2.0 * mkt[i] + rnd.gauss(0, 0.3) for i in range(200)]
    res = orth.residualize(sig, [mkt])
    idx = [i for i, v in enumerate(res) if v is not None]
    corr = orth.factor_correlation([res[i] for i in idx], [mkt[i] for i in idx])
    assert abs(corr) < 1e-6


def test_p06_partial_rank_ic_correct():
    rnd = random.Random(3)
    f = [rnd.gauss(0, 1) for _ in range(90)]
    noise = [rnd.gauss(0, 1) for _ in range(90)]
    fwd = [0.6 * f[i] + rnd.gauss(0, 0.5) for i in range(90)]
    plain = orth.rank_correlation(f, fwd)
    # An independent control barely changes the partial IC.
    assert abs(plain - orth.partial_rank_ic(f, fwd, [noise])) < 0.1
    # A control nearly equal to the factor strips the IC toward zero.
    strong = [0.95 * f[i] + 0.05 * rnd.gauss(0, 1) for i in range(90)]
    assert orth.partial_rank_ic(f, fwd, [strong]) < plain


def test_p07_independent_information_rewards_orthogonality():
    lo = orth.independent_information_score(corr_champion=0.05)
    hi = orth.independent_information_score(corr_champion=0.85)
    assert lo["independent_information_score"] > hi["independent_information_score"]


def test_p08_duplicate_champion_exposure_penalized():
    dup = orth.independent_information_score(corr_champion=0.98)
    assert dup["independent_information_score"] < 0.1
    assert dup["redundancy_penalty_applied"]


# ===== Track C: regimes ==================================================== #
def test_p09_regime_definitions_fixed_before_evaluation():
    # Axes + version are constant; classification is a pure function of market
    # features and NEVER of forward returns.
    assert rg.REGIME_AXIS_KEYS == ("market_trend", "market_volatility",
                                   "participation", "dispersion", "risk_state",
                                   "rates")
    assert rg.regime_version_hash() == rg.regime_version_hash()
    mfeat = [{"market_return": (i % 5) - 2.0, "market_vol": 0.4 + 0.2 * (i % 3),
              "breadth": 0.3 + 0.1 * (i % 4)} for i in range(30)]
    assert rg.classify_regimes(mfeat) == rg.classify_regimes(mfeat)


def test_p10_regime_sample_size_enforced():
    labels = [{"market_trend": "TREND_UP" if i % 2 else "TREND_DOWN"}
              for i in range(8)]
    ics = [0.1] * 8
    cond = rg.regime_conditioned_metrics(labels, ic_series=ics,
                                         cfg={"min_periods_for_regime_claim": 12})
    for state in cond["market_trend"].values():
        assert state["sufficient"] is False  # 4 per bucket < 12


# ===== Track D: selection-bias controls ==================================== #
def test_p11_purged_folds_no_leakage():
    folds = sctl.purged_walk_forward_folds(40, n_folds=4, embargo=2, purge=2)
    assert sctl.verify_no_leakage(folds, embargo=2, purge=2)


def test_p12_embargo_windows_enforced():
    folds = sctl.purged_walk_forward_folds(40, n_folds=4, embargo=3, purge=0)
    for f in folds:
        hi = max(f["test"])
        assert all(j > hi + 3 or j < min(f["test"]) for j in f["train"])
        # the 3 periods immediately after each test fold are embargoed, not train
        for j in range(hi + 1, min(hi + 4, 40)):
            assert j not in f["train"]


def test_p13_block_bootstrap_deterministic():
    s = [random.Random(0).gauss(0.01, 0.05) for _ in range(120)]
    a = sctl.block_bootstrap_ci(s, seed=42)
    b = sctl.block_bootstrap_ci(s, seed=42)
    c = sctl.block_bootstrap_ci(s, seed=43)
    assert a == b and a != c


def test_p14_multiple_testing_increases_with_family_size():
    p = 0.02
    assert (sctl.bonferroni_adjust(p, 1) < sctl.bonferroni_adjust(p, 5)
            < sctl.bonferroni_adjust(p, 50))
    assert (sctl.selection_adjusted_pvalue(p, family_size=1)
            < sctl.selection_adjusted_pvalue(p, family_size=20))


def test_p15_final_holdout_cannot_be_reused(tmp_path):
    ledger = sctl.HoldoutLedger(tmp_path / "holdout.json")
    ledger.reserve("final_v1", description="single-use")
    # a second distinct holdout is refused
    with pytest.raises(sctl.HoldoutReuseError):
        ledger.reserve("other")
    assert ledger.can_use("final_v1")
    ledger.mark_used("final_v1")
    assert not ledger.can_use("final_v1")
    with pytest.raises(sctl.HoldoutReuseError):
        ledger.mark_used("final_v1")


# ===== Track E: SEC companyfacts =========================================== #
def test_p16_companyfacts_campaign_bounded(tmp_path):
    store = ac.CampaignStore(tmp_path / "c.sqlite",
                             clock=lambda: "2026-08-01T00:00:00+00:00")
    cf.ensure_campaign(store, ["%010d" % i for i in range(10)],
                       universe_source="t", batch_size=3)
    # daily cap reached -> STOPPED even with pending CIKs
    for _ in range(6):
        store.record_batch("sec_companyfacts", run_date="2026-08-01",
                           progress=True, origin="campaign-continuation")
    res = cf.run_batch(store, fetch_fn=lambda c: None, run_date="2026-08-01",
                       batch_size=3, daily_cap=6, no_progress_max=2)
    assert res["status"] == "STOPPED"
    assert res["stop_reason"] == "DAILY_BATCH_CAP_REACHED"


def test_p17_companyfacts_campaign_restart_safe(tmp_path):
    store = ac.CampaignStore(tmp_path / "c.sqlite",
                             clock=lambda: "2026-08-01T00:00:00+00:00")
    cf.ensure_campaign(store, ["0000000001", "0000000002", "0000000003"],
                       universe_source="t", batch_size=2)
    doc = _companyfacts_doc(cik=1)
    fetch = {"0000000001": doc}
    r1 = cf.run_batch(store, fetch_fn=lambda c: fetch.get(c),
                      run_date="2026-08-01", batch_size=2,
                      pit_store=pfd.PitFundamentalsStore())
    assert r1["ciks_completed"] == 1
    # a re-run resumes at the cursor; the COMPLETED CIK is never re-attempted
    nxt = store.next_batch("sec_companyfacts", batch_size=5)
    assert "0000000001" not in nxt


def test_p18_accessions_deduplicated():
    payloads = cf.parse_companyfacts(_companyfacts_doc())
    keys = [(p["accession"], p["concept"], p["period_end"]) for p in payloads]
    assert len(keys) == len(set(keys))
    # the exact duplicate accession row (a1 Assets 2020) appears once
    assert sum(1 for p in payloads
               if p["accession"] == "a1" and p["concept"] == "Assets") == 1


def test_p19_filing_dates_control_availability():
    st = pfd.PitFundamentalsStore()
    st.add_records(cf.to_pit_records(cf.parse_companyfacts(_companyfacts_doc())))
    # before the original 10-K filed date nothing is available
    assert st.as_of("0000320193", "assets", "2020-FY", "2021-01-01") is None
    assert st.as_of("0000320193", "assets", "2020-FY", "2021-03-01") is not None


def test_p20_restatements_do_not_leak_backward():
    st = pfd.PitFundamentalsStore()
    st.add_records(cf.to_pit_records(cf.parse_companyfacts(_companyfacts_doc())))
    before = st.as_of("0000320193", "assets", "2020-FY", "2021-03-01")
    after = st.as_of("0000320193", "assets", "2020-FY", "2021-09-01")
    assert before.value == 1000 and after.value == 1050


def test_p21_units_normalized_deterministically():
    assert cf.normalize_unit("USD") == ("USD", True)
    assert cf.normalize_unit("shares") == ("shares", False)
    assert cf.normalize_unit(None) == ("UNKNOWN", False)
    # non-USD monetary facts are excluded from the parsed monetary payloads
    assert all(p["unit"] == "USD" for p in cf.parse_companyfacts(_companyfacts_doc()))


def test_p22_missing_concepts_explicit():
    st = pfd.PitFundamentalsStore()
    st.add_records(cf.to_pit_records(cf.parse_companyfacts(_companyfacts_doc())))
    bsq = fs.balance_sheet_quality(st, cik="0000320193", fiscal_key="2020-FY",
                                   as_of="2021-03-01")
    assert bsq["value"] is None
    assert "stockholders_equity" in bsq["missing"]


# ===== Track F / H: lifecycle ============================================== #
def test_p23_incomplete_pit_data_stays_data_hold():
    st = pfd.PitFundamentalsStore()
    st.add_records(cf.to_pit_records(cf.parse_companyfacts(_companyfacts_doc())))
    ready = fs.coverage_ready(st, "gross_profitability", scored_periods=3,
                              min_ciks=50, min_periods=12)
    assert ready["sufficient"] is False and ready["blocker"] is not None
    # and the tournament gate turns incomplete evidence into DATA_HOLD
    hold = tt.classify_evidence(
        {"coverage_pct": None, "scored_periods": None,
         "min_names_per_period": None, "point_in_time_valid": True,
         "survivorship_safe": True}, _CFG)
    assert hold["target_state"] == tt.DATA_HOLD


def test_p24_complete_weak_becomes_rejected():
    weak = {"coverage_pct": 100.0, "scored_periods": 60,
            "min_names_per_period": 60, "point_in_time_valid": True,
            "survivorship_safe": True, "lookahead_contamination": False,
            "rank_ic": 0.001, "rank_ic_t": 0.3, "positive_ic_hit_rate": 0.50,
            "spread_t": 0.2, "net25_spread": -0.01}
    g = tt.classify_evidence(weak, _CFG)
    assert g["target_state"] == tt.REJECTED and g["complete"] is True


def test_p25_complete_strong_may_keep():
    strong = {"coverage_pct": 100.0, "scored_periods": 60,
              "min_names_per_period": 60, "point_in_time_valid": True,
              "survivorship_safe": True, "lookahead_contamination": False,
              "rank_ic": 0.05, "rank_ic_t": 4.0, "positive_ic_hit_rate": 0.62,
              "spread_t": 3.5, "net25_spread": 0.05,
              "subperiod_consistency": 1.0, "regime_consistency": 1.0,
              "max_drawdown_pct": -10.0, "turnover_per_rebalance": 0.5,
              "sector_concentration_pct": 10.0, "worst_period_return_pct": -5.0}
    g = tt.classify_evidence(strong, _CFG)
    assert g["target_state"] == tt.KEEP_FOR_RESEARCH


def test_p26_no_automatic_model_promotion():
    assert "PROMOTED" not in tt.LIFECYCLE_STATES
    # KEEP_FOR_RESEARCH can never transition straight to an operating/promoted
    # state; the strongest state is a manual-review flag that changes nothing.
    assert tt.KEEP_FOR_RESEARCH not in tt.ALLOWED_TRANSITIONS[tt.PROPOSED]


def test_p27_no_shadow_book_backfill(tmp_path):
    book = tt.ShadowBook(tmp_path, "sb1")
    book.inception(candidate_id="c", inception_date="2026-08-01", membership=[],
                   benchmark="SPY", cost_bps=50.0, spec={})
    with pytest.raises(tt.RetroactiveError):
        book.record_mark(date="2026-07-01", nav=100000.0)  # before inception


def test_p28_no_operational_portfolio_state_change(tmp_path):
    # A full synthetic Stage 9.4 cycle writes ONLY the research tournament store;
    # a fingerprinted stand-in operational ledger is byte-identical afterwards.
    opdir = tmp_path / "op_ledger"
    opdir.mkdir()
    (opdir / "book.json").write_text('{"nav": 100000}', encoding="utf-8")
    before = (opdir / "book.json").read_bytes()
    panel = _panel()
    base = er.run_price_factor_campaign(panel)
    merged = tt._merge_stage9_4_new_factors(base, panel,
                                            stage9_4_cfg=_CFG.get("stage9_4"))
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sb")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "2026-08-01T00:00:00")
    tt.run_tournament_cycle(reg, cfg, campaign_result=merged,
                            evidence_date="2026-08-01", max_candidates=200)
    reg.close()
    assert (opdir / "book.json").read_bytes() == before


def test_p29_cadence_tasks_remain_disabled():
    try:
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "Get-ScheduledTask -TaskName 'AlphaAgent-Collect',"
             "'AlphaAgent-Morning-Report','AlphaAgent-PostClose-Report',"
             "'AlphaAgent-Watchdog' | ForEach-Object { $_.State }"],
            capture_output=True, text=True, timeout=60)
    except Exception:
        pytest.skip("scheduled-task query unavailable in this environment")
    if out.returncode != 0 or not out.stdout.strip():
        pytest.skip("AlphaAgent scheduled tasks not present")
    states = [s.strip() for s in out.stdout.split() if s.strip()]
    assert all(s == "Disabled" for s in states), states


def test_p30_stage9_2_and_9_3_behaviour_intact():
    # Stage 9.3 controlled-continuation contract unchanged.
    from alpha_agent import runtime as _rt
    assert _rt.sec_continuation_stop_reason(
        is_complete=False, completed_batches_today=6, consecutive_no_progress=0,
        daily_cap=6, no_progress_max=2) == "DAILY_BATCH_CAP_REACHED"
    assert _rt.sec_continuation_stop_reason(
        is_complete=True, completed_batches_today=0, consecutive_no_progress=0,
        daily_cap=6, no_progress_max=2) == "CAMPAIGN_COMPLETE"
    # Stage 9.3 batch-log derivation unchanged.
    import tempfile
    store = ac.CampaignStore(Path(tempfile.mkdtemp()) / "c.sqlite",
                             clock=lambda: "2026-08-01T00:00:00+00:00")
    store.ensure_campaign("x", kind="k", universe=["A"], universe_source="s")
    store.record_batch("x", run_date="2026-08-01", progress=False)
    store.record_batch("x", run_date="2026-08-01", progress=False)
    assert store.consecutive_no_progress("x") == 2


# ===== Agent-integration wiring (course correction) ======================== #
def test_a01_agent_seeds_catalogue_idempotently(tmp_path):
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "t")
    n1 = len(tt.seed_stage9_4_catalogue(reg))
    n2 = len(tt.seed_stage9_4_catalogue(reg))
    assert n1 == n2 and n1 == len(cat.catalogue_specs())
    total = sum(reg.counts_by_state().values())
    tt.seed_stage9_4_catalogue(reg)  # third seed adds nothing
    assert sum(reg.counts_by_state().values()) == total
    reg.close()


def test_a02_independent_information_in_scoring_decomposition():
    m = {"rank_ic_t": 3.0, "spread_t": 3.0, "net25_spread": 0.02,
         "subperiod_consistency": 0.8, "regime_consistency": 0.7}
    s = tt.score_candidate(m, _CFG, corr_champion=0.05)
    assert "independent_information_score" in s
    assert s["independent_information_score"] is not None
    # weight defaults to 0 -> combined is the six-subscore value (gate-safe)
    assert 0.0 <= s["combined_score"] <= 1.0


def test_a03_enriched_row_carries_all_track_evidence():
    panel = _panel()
    camp = pf.run_new_price_factor_campaign(panel, features=["trend_slope_t"],
                                            min_periods=5)
    row = [r for r in camp["results"] if r.get("rank_ic_t") is not None][0]
    row = s94.enrich_price_factor_row(row, family_size=2, cfg=_CFG.get("stage9_4"))
    for k in ("stage9_4_regime", "stage9_4_bootstrap", "stage9_4_walk_forward",
              "stage9_4_selection", "stage9_4_independent_information"):
        assert k in row
    assert row["stage9_4_walk_forward"]["no_leakage"] is True


def test_a04_non_computable_factor_routes_to_specific_data_hold(tmp_path):
    # A pre-registered factor needing volume/intraday/GICS/event data is seeded
    # and classified DATA_HOLD with a SPECIFIC blocker by the canonical tick.
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sb")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "t")
    tt.seed_stage9_4_catalogue(reg)
    tt.run_tournament_cycle(reg, cfg, campaign_result={"results": []},
                            evidence_date="2026-08-01", max_candidates=300,
                            seed=False)
    blockers = {c["blocker"] for c in reg.list(state="DATA_HOLD")}
    assert tt.BLOCK_REQUIRES_VOLUME in blockers
    assert tt.BLOCK_REQUIRES_INTRADAY in blockers
    reg.close()


def test_a05_decision_artifact_recorded_for_catalogue(tmp_path):
    panel = _panel()
    base = er.run_price_factor_campaign(panel)
    merged = tt._merge_stage9_4_new_factors(base, panel,
                                            stage9_4_cfg=_CFG.get("stage9_4"))
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sb")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "t")
    tt.run_tournament_cycle(reg, cfg, campaign_result=merged,
                            evidence_date="2026-08-01", max_candidates=300)
    arts = [c for c in reg.recent_changes(limit=400)
            if c["kind"] == "STAGE94_DECISION"]
    assert arts
    a = arts[0]["detail"]
    for k in ("hypothesis", "family", "search_family_size", "raw_statistics",
              "selection_adjusted_statistics", "orthogonality_evidence",
              "regime_evidence", "failed_gates", "next_required_evidence"):
        assert k in a
    reg.close()


def test_a06_no_forced_retention_on_synthetic_noise(tmp_path):
    # Random price noise must NOT produce a KEEP_FOR_RESEARCH candidate.
    panel = _panel(seed=99)
    base = er.run_price_factor_campaign(panel)
    merged = tt._merge_stage9_4_new_factors(base, panel,
                                            stage9_4_cfg=_CFG.get("stage9_4"))
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sb")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "t")
    tt.run_tournament_cycle(reg, cfg, campaign_result=merged,
                            evidence_date="2026-08-01", max_candidates=300)
    assert reg.counts_by_state().get(tt.KEEP_FOR_RESEARCH, 0) == 0
    assert reg.list_shadow_books() == []
    reg.close()


def test_a07_combinations_data_hold_when_leg_unavailable():
    reports = fc.evaluate_all(_panel(), horizon_days=21, min_periods=5)
    held = [r for r in reports if r["status"] == "DATA_HOLD"]
    assert any(r["held_leg"] == "gross_profitability" for r in held)
    scored = [r for r in reports if r["status"] == "EVALUATED"]
    assert scored  # the two both-legs-computable combos are evaluated


# ===== RELEASE CLOSURE ===================================================== #
# ---- Blocker 2: genuine forward-only walk-forward folds -------------------- #
def test_rc01_walk_forward_train_strictly_before_test():
    folds = sctl.purged_walk_forward_folds(80, n_folds=4, embargo=1, purge=1)
    assert folds
    for f in folds:
        assert f["train"]
        assert max(f["train"]) < min(f["test"])          # forward-only


def test_rc02_no_label_window_overlap():
    lh = 3
    folds = sctl.purged_walk_forward_folds(120, n_folds=4, embargo=1, purge=1,
                                           label_horizon=lh, test_size=12)
    for f in folds:
        lo = min(f["test"])
        # every training obs's forward label window ends strictly before lo
        assert max(f["train"]) + lh <= lo


def test_rc03_purge_gap_enforced():
    for purge in (1, 2, 3):
        folds = sctl.purged_walk_forward_folds(120, n_folds=4, embargo=1,
                                               purge=purge, test_size=12)
        for f in folds:
            lo = min(f["test"])
            assert max(f["train"]) <= lo - 1 - purge      # gap >= purge
            assert len(f["purged"]) >= purge


def test_rc04_embargo_reserved_and_disjoint_from_train():
    emb = 2
    folds = sctl.purged_walk_forward_folds(120, n_folds=4, embargo=emb,
                                           purge=1, test_size=12)
    for f in folds:
        hi = max(f["test"])
        for j in f["embargoed"]:
            assert hi < j <= hi + emb                     # reserved after test
        assert not (set(f["embargoed"]) & set(f["train"]))
    assert sctl.verify_no_leakage(folds, embargo=emb, purge=1)


def test_rc05_future_data_cannot_change_an_earlier_fold():
    a = sctl.purged_walk_forward_folds(80, test_size=10, purge=1,
                                       label_horizon=1, embargo=1)
    b = sctl.purged_walk_forward_folds(200, test_size=10, purge=1,
                                       label_horizon=1, embargo=1)
    amap = {f["fold"]: (f["test"], f["train"], f["purged"]) for f in a}
    bmap = {f["fold"]: (f["test"], f["train"], f["purged"]) for f in b}
    assert amap  # front-anchored, so every earlier fold recurs unchanged
    for k, v in amap.items():
        assert bmap[k] == v
    assert len(b) > len(a)                                # future only ADDS folds


def test_rc06_insufficient_history_folds_omitted():
    folds = sctl.purged_walk_forward_folds(80, test_size=10, min_train=30,
                                           purge=1)
    assert folds
    for f in folds:
        assert len(f["train"]) >= 30
    # the earliest fold ids are skipped honestly (ids are not renumbered)
    assert min(f["fold"] for f in folds) >= 1


def test_rc07_candidate_evidence_only_from_forward_folds():
    # enrich uses forward-only folds; the reported no_leakage assertion holds and
    # the OOS evidence is drawn only from legitimate (forward) test blocks.
    ic = [0.02 * ((i % 5) - 2) for i in range(60)]
    row = {"feature": "x", "rank_ic_t": 1.1, "sharpe": 0.3, "periods": 60,
           "_periods_series": {"rank_ic": ic, "spread": [0.0] * 60,
                               "portfolio_returns": [0.0] * 60,
                               "turnovers": [0.0] * 60, "market_features": []}}
    out = s94.enrich_price_factor_row(row, family_size=2,
                                      cfg=_CFG.get("stage9_4"))
    wf = out["stage9_4_walk_forward"]
    assert wf["forward_only"] is True and wf["no_leakage"] is True
    folds = sctl.purged_walk_forward_folds(
        60, n_folds=int(_CFG["stage9_4"]["walk_forward_folds"]),
        embargo=1, purge=1, label_horizon=1)
    assert wf["folds"] == len(folds)
    for f in folds:
        assert max(f["train"]) < min(f["test"])


# ---- Blocker 3: cross-sectional regime semantics --------------------------- #
def test_rc08_breadth_is_cross_sectional_participation():
    # 7 of 10 names have a positive trailing return -> breadth = 0.7 exactly.
    ntr = [0.1, 0.2, 0.05, 0.3, -0.1, -0.2, 0.15, -0.05, 0.02, 0.08]
    mf = pf._market_features_at(300, list(range(400)), [0.0] * 400, ntr,
                                min_names=5)
    assert abs(mf["breadth"] - 0.7) < 1e-9
    assert mf["breadth_name_count"] == 10
    # dispersion is the cross-sectional stdev of those same name returns
    import statistics
    assert abs(mf["dispersion"] - statistics.pstdev(ntr)) < 1e-6 or \
        abs(mf["dispersion"] - statistics.stdev(ntr)) < 1e-6


def test_rc09_regime_features_are_candidate_independent():
    panel = _panel(names=40, days=420)
    b1 = pf.build_factor_cross_sections(panel, feature="trend_slope_t",
                                        horizon_days=21, rebalance="monthly")
    b2 = pf.build_factor_cross_sections(panel, feature="low_beta",
                                        horizon_days=21, rebalance="monthly")

    def _bd(b):
        return [(m["breadth"], m["dispersion"]) for m in b["market_features"]]
    assert b1["periods"] and _bd(b1) == _bd(b2)           # identical => no leak


def test_rc10_regime_unknown_below_min_coverage():
    mf = pf._market_features_at(300, list(range(400)), [0.0] * 400,
                                [0.1, -0.1, 0.2], min_names=20)
    assert mf["breadth"] is None and mf["dispersion"] is None
    labels = rg.classify_regimes([{"market_return": 0.01, "market_vol": 0.1,
                                    "breadth": None, "dispersion": None}])
    assert labels[0]["participation"] == "UNKNOWN"
    assert labels[0]["dispersion"] == "UNKNOWN"


# ---- Blocker 1: canonical queued-experiment loop --------------------------- #
def _seed_rejected(tmp_path, seed=7):
    """Seed the catalogue and classify it once so computable candidates are on
    COMPLETE (REJECTED) evidence - the precondition for a revalidation."""
    panel = _panel(seed=seed)
    base = er.run_price_factor_campaign(panel)
    merged = tt._merge_stage9_4_new_factors(base, panel,
                                            stage9_4_cfg=_CFG.get("stage9_4"))
    cfg = dict(_CFG)
    cfg["tournament_db"] = str(tmp_path / "t.sqlite")
    cfg["shadow_book_root"] = str(tmp_path / "sb")
    reg = tt.CandidateRegistry(cfg["tournament_db"], clock=lambda: "t")
    tt.run_tournament_cycle(reg, cfg, campaign_result=merged,
                            evidence_date="2026-08-01", max_candidates=300)
    return reg, cfg, merged


def test_rc11_followup_generates_canonical_experiment_job(tmp_path):
    reg, cfg, _ = _seed_rejected(tmp_path)
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=lambda: "t")
    out = tt.generate_stage9_4_followups(reg, cfg, queue=q)
    assert out["enabled"] and out["count"] == 1            # max_per_cycle == 1
    jobs = q.list_jobs(state=ar.STATE_QUEUED)
    exp = [j for j in jobs if j.category == ar.CAT_EXPERIMENT]
    assert len(exp) == 1
    j = exp[0]
    assert j.origin == "stage9-tournament"
    assert j.lane == "tournament.stage9_4_revalidation"
    assert j.payload.get("tournament") is True
    assert j.payload.get("feature") in pf.COMPUTABLE_FEATURES
    assert j.attempts == 0
    reg.close()


def test_rc12_followup_is_deduplicated(tmp_path):
    reg, cfg, _ = _seed_rejected(tmp_path)
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=lambda: "t")
    seen = set()
    for _ in range(6):
        out = tt.generate_stage9_4_followups(reg, cfg, queue=q)
        for g in out["generated"]:
            assert g["feature"] not in seen                # never regenerated
            seen.add(g["feature"])
    # all distinct features; strictly bounded, no endless duplication
    assert len(seen) == len({j.payload.get("feature")
                             for j in q.list_jobs(state=ar.STATE_QUEUED)
                             if j.category == ar.CAT_EXPERIMENT})
    reg.close()


def test_rc13_followup_disabled_generates_nothing(tmp_path):
    reg, cfg, _ = _seed_rejected(tmp_path)
    cfg2 = dict(cfg)
    cfg2["stage9_4"] = {**cfg["stage9_4"], "followups": {"enabled": False}}
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=lambda: "t")
    out = tt.generate_stage9_4_followups(reg, cfg2, queue=q)
    assert out["count"] == 0 and out["enabled"] is False
    assert not q.list_jobs(state=ar.STATE_QUEUED)
    reg.close()


def test_rc14_completed_experiment_row_is_ingestible(tmp_path):
    reg, cfg, merged = _seed_rejected(tmp_path)
    # a computable feature's row from the same panel, as the handler would emit
    feat = sorted(set(pf.COMPUTABLE_FEATURES))[0]
    rows = [r for r in merged["results"]
            if r.get("feature") == feat and r.get("rank_ic_t") is not None]
    assert rows, "expected a computable row with rank_ic_t"
    compact = {k: v for k, v in rows[0].items()
               if k not in ("_periods_series", "market_features")}
    assert compact.get("rank_ic_t") is not None            # ingestible contract
    cand = tt._candidate_for_feature(reg, feat)
    prev = cand["lifecycle_state"]
    # shape matches runtime._collect_completed_tournament_jobs: result IS the row
    job = {"job_id": "job_rc14", "feature": feat, "spec": {"feature": feat},
           "result": compact}
    res = tt.ingest_completed_experiments(reg, cfg, completed=[job],
                                          evidence_date="2026-08-02")
    assert res["imported"] == 1
    # a REJECTED candidate's evidence is updated but it is NOT auto-promoted
    assert reg.get(cand["candidate_id"])["lifecycle_state"] == prev
    # importing the SAME completed job again is a no-op (import-once)
    res2 = tt.ingest_completed_experiments(reg, cfg, completed=[job],
                                           evidence_date="2026-08-02")
    assert res2["imported"] == 0 and res2["skipped"] >= 1
    reg.close()


def test_rc15_drain_allowlist_prefers_experiment_over_weakest_gate(tmp_path):
    drain = _STAGE8_CFG["autonomy"]["collect_drain"]
    assert "EXPERIMENT" in drain["allowed_categories"]     # narrow extension
    cats = drain["allowed_categories"]
    orgs = drain["allowed_origins"]
    lpfx = drain["allowed_lane_prefixes"]
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=lambda: "t")
    q.enqueue(ar.CAT_DATA_VALIDATION, lane="tournament.address_weakest_gate",
              payload={"tournament": True}, priority=2, origin="stage9-tournament")
    exp_id = q.enqueue(ar.CAT_EXPERIMENT, lane="tournament.stage9_4_revalidation",
                       payload={"tournament": True, "feature": "trend_slope_t"},
                       priority=3, origin="stage9-tournament")
    claimed = q.claim_next(categories=cats, origins=orgs, lane_prefixes=lpfx)
    assert claimed is not None and claimed.job_id == exp_id  # priority 3 first
    assert claimed.category == ar.CAT_EXPERIMENT
    assert claimed.attempts == 1                            # attempts 0 -> 1


def test_rc16_drain_allowlist_never_releases_legacy_experiment(tmp_path):
    drain = _STAGE8_CFG["autonomy"]["collect_drain"]
    q = ar.ResearchQueue(str(tmp_path / "q.sqlite"), clock=lambda: "t")
    # a legacy EXPERIMENT job on a non-tournament origin/lane
    q.enqueue(ar.CAT_EXPERIMENT, lane="campaign.price_factor",
              payload={}, priority=5, origin="seed")
    claimed = q.claim_next(categories=drain["allowed_categories"],
                           origins=drain["allowed_origins"],
                           lane_prefixes=drain["allowed_lane_prefixes"])
    assert claimed is None                                 # origin+lane excluded


# ---- Blocker 4: companyfacts deferred honestly (Option B) ------------------ #
def test_rc17_companyfacts_campaign_is_deferred_but_capability_retained():
    cc = _CFG["stage9_4"]["companyfacts_campaign"]
    assert cc["enabled"] is False and cc.get("deferred") is True
    # the parser capability is retained and still functions (inactive, not gone)
    recs = cf.to_pit_records(cf.parse_companyfacts(_companyfacts_doc()))
    assert recs and all("filed" in (r.get("normalized_payload") or {})
                        for r in recs)
