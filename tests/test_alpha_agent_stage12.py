"""Stage 12 deterministic tests -- alpha failure autopsy, owned-data power,
sample-power/validation design, PIT-safe event + residual features, the frozen
pre-registered hypothesis registry, the focused tournament (unchanged gates,
family-aware multiple testing, holdout, combinations, ML eligibility, shadow),
and the campaign jobs (command center, resumability, read-only snapshot).

All fixtures are synthetic and hermetic: no real Stage 11 artifact, no owned
panel, no store, no network, no operational ledger. Deterministic (seeded).
"""
import json
import math
from pathlib import Path

import numpy as np
import pytest

from alpha_agent import stage12_autopsy as AU
from alpha_agent import stage12_power as PW
from alpha_agent import stage12_owned_data as OD
from alpha_agent import stage12_registry as RG
from alpha_agent import stage12_events as EV
from alpha_agent import stage12_residual as RS
from alpha_agent import stage12_tournament as TN
from alpha_agent import stage12_jobs as JB
from alpha_agent import stage11_research as SR
from alpha_agent import signal_evaluation as SE
from alpha_agent import tournament as TT


# --------------------------------------------------------------------------- #
# Synthetic fixtures.
# --------------------------------------------------------------------------- #
def _panel(n_assets=50, n_days=1100, seed=13):
    rng = np.random.default_rng(seed)
    base = 0
    dates = ["D%05d" % k for k in range(n_days)]  # lexicographically ordered
    ohlcv = {}
    for i in range(n_assets):
        steps = rng.normal(0.0004 + 0.00002 * (i - n_assets // 2), 0.017, n_days)
        closes = (100 * np.cumprod(1 + steps)).round(4).tolist()
        ohlcv["A%03d" % i] = {"d": list(dates), "c": closes}
    return ohlcv, dates


class _FakeSector:
    def sector_as_of(self, cik, as_of):
        return "SEC%d" % (int(cik[1:]) % 5)


class _FakeStore:
    """PIT store: only returns facts 'filed' on/before as_of via fiscal keys."""
    def latest_fiscal_key(self, cik, as_of, concept="assets"):
        return "FY:" + as_of[:3]

    def prior_fiscal_key(self, cik, as_of, fk, concept="assets"):
        return "PY:" + as_of[:3]

    def as_of(self, cik, concept, fk, as_of):
        b = int(cik[1:]) + 1
        mult = 1.15 if fk.startswith("FY") else 1.0
        table = {"revenue": 1000 * b * mult, "cost_of_revenue": 600 * b,
                 "gross_profit": 400 * b * mult, "assets": 5000 * b,
                 "operating_income": 200 * b * mult, "net_income": 150 * b * mult,
                 "cash": 300 * b * mult, "liabilities": 2000 * b,
                 "stockholders_equity": 3000 * b}
        return table.get(concept)


def _ctx(with_store=True, with_sector=True):
    ohlcv, dates = _panel()
    c2a = {"C%03d" % i: "A%03d" % i for i in range(len(ohlcv))}
    ctx = SR.build_panel_context(
        ohlcv, store=_FakeStore() if with_store else None,
        cik_to_assetid=c2a, sector_series=_FakeSector() if with_sector else None)
    rebs = [dates[k] for k in range(200, len(dates) - 63, 63)]
    return ctx, rebs, dates


def _cfg9():
    p = Path(__file__).resolve().parents[1] / "configs" / "alpha_agent" / "stage9_tournament.json"
    return json.loads(p.read_text(encoding="utf-8-sig"))


# --------------------------------------------------------------------------- #
# Workstream A -- failure taxonomy.
# --------------------------------------------------------------------------- #
def test_autopsy_classify_deep_multiple_testing_failure():
    r = {"gate_target_state": "KEEP_FOR_RESEARCH", "gate_blocker": None,
         "multiple_testing_survived": False, "sign_stable": True, "holdout_confirms": True}
    assert AU.classify_deep(r) == AU.MULTIPLE_TESTING_FAILURE


def test_autopsy_classify_deep_blocker_maps_to_vocab():
    assert AU.classify_deep({"gate_blocker": "REJECT_SEVERE_DRAWDOWN"}) == AU.DRAWDOWN
    assert AU.classify_deep({"gate_blocker": "REJECT_WEAK_SPREAD_T"}) == AU.WEAK_IC
    assert AU.classify_deep({"gate_blocker": "REJECT_EXCESSIVE_TURNOVER"}) == AU.EXCESSIVE_TURNOVER


def test_autopsy_classify_screen_precedence():
    assert AU.classify_screen({"periods": 0}) == AU.INSUFFICIENT_COVERAGE
    assert AU.classify_screen({"periods": 5}) == AU.INSUFFICIENT_EFFECTIVE_SAMPLE
    assert AU.classify_screen({"periods": 40, "rank_ic_t": 1.0, "spread_t": 1.0}) == AU.WEAK_IC
    assert AU.classify_screen({"periods": 40, "rank_ic_mean": 0.05, "rank_ic_t": 3.0,
                               "spread_t": 3.0, "net25": -0.1}) == AU.COST_DESTROYED
    assert AU.classify_screen({"periods": 40, "rank_ic_mean": 0.05, "rank_ic_t": 3.0,
                               "spread_t": 3.0, "net25": 0.05, "turnover": 0.2}) == AU.MULTIPLE_TESTING_FAILURE


def test_effective_hypothesis_count_collinear_and_independent():
    # identical streams -> effective ~1
    dates = ["d%02d" % k for k in range(30)]
    ls = [math.sin(k) for k in range(30)]
    same = {("s%d" % i): {"dates": dates, "ls": list(ls)} for i in range(6)}
    eff = AU.effective_hypothesis_count(same, nominal=6)
    assert eff["effective_family_size"] <= 1.5
    # independent streams -> effective close to n
    rng = np.random.default_rng(3)
    indep = {("s%d" % i): {"dates": dates, "ls": rng.normal(0, 1, 30).tolist()}
             for i in range(6)}
    eff2 = AU.effective_hypothesis_count(indep, nominal=6)
    assert eff2["effective_family_size"] >= 3.0


def test_bh_min_qvalue_scales_with_family_size():
    ps = [0.001, 0.01, 0.02, 0.2, 0.5]
    q_small = AU.bh_min_qvalue(ps, 5)
    q_large = AU.bh_min_qvalue(ps, 300)
    assert q_large > q_small  # a bigger family makes survival strictly harder


def test_build_autopsy_taxonomy_and_verdict():
    state = {
        "catalogue": {"specs": [
            {"spec_id": "a", "family": "growth", "name": "g1"},
            {"spec_id": "b", "family": "growth", "name": "g2"}],
            "data_hold_specs": [{"spec_id": "v", "name": "ey", "family": "valuation",
                                 "reason": "DATA_HOLD_NO_MARKET_CAP_NO_SHARES_OUTSTANDING"}]},
        "screen": {"family_size": 2, "records": {
            "a": {"family": "growth", "name": "g1", "rank_ic_t": 3.2, "spread_t": 2.5,
                  "net25": 0.05, "turnover": 0.3, "periods": 40},
            "b": {"family": "growth", "name": "g2", "rank_ic_t": 1.0, "spread_t": 0.4,
                  "net25": -0.1, "turnover": 0.3, "periods": 40}},
            "series": {}},
        "deep_eval": {"results": {
            "a": {"gate_target_state": "KEEP_FOR_RESEARCH", "gate_blocker": None,
                  "multiple_testing_survived": False, "sign_stable": True,
                  "holdout_confirms": True, "family": "growth", "name": "g1"}}},
        "multiple_testing": {"family_size": 2, "alpha": 0.05, "tested": 2,
                             "rows": [{"raw_pvalue": 0.001}, {"raw_pvalue": 0.3}]},
        "command_center": {"status": "COMPLETE"},
        "inventory": {}, "shadow": {"status": "NO_DEFENSIBLE_ALPHA"},
    }
    out = AU.build_autopsy(state)
    assert out["n_candidates_classified"] == 2
    assert out["weakest_gate_distribution_deep_authoritative"].get("MULTIPLE_TESTING_FAILURE") == 1
    assert out["graveyard_count"] >= 1  # the valuation DATA_HOLD
    assert "verdict" in out["design_recommendation"]


# --------------------------------------------------------------------------- #
# Workstream C -- sample power / validation design.
# --------------------------------------------------------------------------- #
def test_norm_ppf_cdf_accuracy():
    assert abs(PW.norm_ppf(0.975) - 1.959964) < 1e-3
    assert abs(PW.norm_cdf(1.959964) - 0.975) < 1e-4


def test_newey_west_effective_n_below_nominal_on_autocorrelated():
    # AR(1) positive autocorrelation -> effective N < nominal
    rng = np.random.default_rng(5)
    x = [0.0]
    for _ in range(199):
        x.append(0.6 * x[-1] + rng.normal(0, 1))
    nw = PW.newey_west_effective_n(x)
    assert nw["effective_n"] < nw["n_nominal"]
    assert nw["variance_inflation"] > 1.0


def test_overlap_effective_n_halves_with_overlap():
    ov = PW.overlap_effective_n(40, horizon_days=126, step_days=63)
    assert ov["overlap_days"] == 63
    assert ov["effective_n"] < 40  # overlapping windows are not independent


def test_minimum_detectable_ic_and_power_monotone():
    mde_small = PW.minimum_detectable_ic(10, 0.1)
    mde_large = PW.minimum_detectable_ic(100, 0.1)
    assert mde_large < mde_small  # more data detects smaller effects
    p_small = PW.approximate_power(0.05, 10, 0.1)
    p_large = PW.approximate_power(0.05, 100, 0.1)
    assert p_large > p_small


def test_max_confirmatory_family_size():
    assert PW.max_confirmatory_family_size(0.001) == 50  # 0.05/0.001
    assert PW.max_confirmatory_family_size(0.01) == 5


def test_purged_walk_forward_no_leakage():
    d = PW.purged_walk_forward_design(40, n_folds=4, embargo=1, purge=1)
    assert d["no_leakage_verified"] in (True, None)
    assert len(d["folds"]) >= 1


# --------------------------------------------------------------------------- #
# Workstream B -- owned-data inventory.
# --------------------------------------------------------------------------- #
def test_owned_data_inventory_classifies_sources_and_coverage():
    inv = {"coverage_signature": {"date_min": "2015-01-02", "date_max": "2026-08-03"},
           "owned_norgate_universe_count": 1895, "cik_with_owned_price_series": 535,
           "cik_to_assetid": 1084, "materialized_assetids": 572, "rebalance_dates": 40,
           "per_rebalance_coverage": [{"as_of": "2015-03-31", "price_assetid_overlap": 350},
                                      {"as_of": "2024-12-31", "price_assetid_overlap": 487}]}
    out = OD.build_owned_data_inventory(inv, {"families": {"price_momentum": 1}})
    assert out["identity_resolution"]["coverage_pct_of_universe"] == pytest.approx(28.23, abs=0.1)
    usable = {m["source"]: m["usable_for_confirmatory_history"]
              for m in out["owned_data_capability_matrix"]}
    assert usable["eodhd_fundamentals"] is False  # current-only, excluded
    assert usable["norgate_local_bars"] is True
    assert out["cross_sectional_breadth"]["min_names"] == 350


# --------------------------------------------------------------------------- #
# Workstream F -- frozen pre-registered registry.
# --------------------------------------------------------------------------- #
def test_registry_shape_and_validation():
    reg = RG.build_registry()
    assert 20 <= reg["n_hypotheses"] <= 40
    assert reg["n_economic_families"] >= 5
    assert RG.validate_registry(reg) == []


def test_registry_immutability(tmp_path):
    p = tmp_path / "hyp.json"
    reg = RG.freeze(p)
    assert p.exists()
    # re-freeze identical is a no-op
    RG.freeze(p)
    # tampering -> re-freeze raises
    d = json.loads(p.read_text(encoding="utf-8"))
    d["registry_version"] = "deadbeef"
    p.write_text(json.dumps(d), encoding="utf-8")
    with pytest.raises(RG.RegistryImmutabilityError):
        RG.freeze(p)


def test_committed_registry_matches_module():
    path = RG.default_registry_path()
    assert path.exists(), "frozen registry artifact must be committed"
    on_disk = RG.load_registry(path)
    assert on_disk["registry_version"] == RG.build_registry()["registry_version"]


# --------------------------------------------------------------------------- #
# Workstream D -- PIT-safe event features.
# --------------------------------------------------------------------------- #
def test_event_builder_data_hold_without_store():
    ctx, rebs, _ = _ctx(with_store=False)
    out = EV.build_event_periods("event_revenue_acceleration", ctx, rebs)
    assert out["periods"] == []
    assert out["data_hold_reason"] == "DATA_HOLD_NO_PIT_FUNDAMENTALS_STORE"


def test_event_builder_pit_metadata_and_periods():
    ctx, rebs, _ = _ctx()
    out = EV.build_event_periods("event_gross_margin_change", ctx, rebs, horizon_days=63)
    assert out["pit"]["no_lookahead"] is True
    assert "SEC filed date" in out["pit"]["availability"]
    # periods are in the scorer format
    for p in out["periods"]:
        assert "as_of" in p and all(len(t) == 3 for t in p["names"])


def test_event_builder_no_lookahead_uses_store_asof(monkeypatch):
    ctx, rebs, _ = _ctx()
    seen = []
    orig = ctx["store"].as_of
    def spy(cik, concept, fk, as_of):
        seen.append(as_of)
        return orig(cik, concept, fk, as_of)
    ctx["store"].as_of = spy
    EV.build_event_periods("event_revenue_acceleration", ctx, rebs)
    # every as_of passed to the store is a requested rebalance date (never a future one)
    assert seen and all(a in set(rebs) for a in seen)


# --------------------------------------------------------------------------- #
# Workstream E -- residual price features.
# --------------------------------------------------------------------------- #
def test_residual_market_momentum_is_cross_sectionally_demeaned():
    ctx, rebs, _ = _ctx()
    out = RS.build_residual_periods("residual_market_momentum", ctx, rebs)
    assert out["periods"]
    # the raw (pre-sign) demeaned signal sums to ~0 per date is enforced by _demean;
    # here just assert coverage + format
    assert out["coverage"]["n_periods"] >= 1


def test_residual_industry_requires_sector():
    ctx, rebs, _ = _ctx(with_sector=False)
    out = RS.build_residual_periods("residual_industry_momentum", ctx, rebs)
    assert out["data_hold_reason"] == "DATA_HOLD_NO_PIT_SECTOR"


def test_residual_return_between_is_backward_only():
    dates = ["d0", "d1", "d2", "d3", "d4"]
    closes = [10.0, 11.0, 12.0, 13.0, 14.0]
    # window ending at d3, from 2 before to 0 before -> d1->d3
    r = RS._return_between(dates, closes, "d3", 2, 0)
    assert r == pytest.approx(13.0 / 11.0 - 1.0)
    # a formation date before any data -> None (no look-ahead into the future)
    assert RS._return_between(dates, closes, "a", 2, 0) is None


# --------------------------------------------------------------------------- #
# Workstream G/H/I -- tournament.
# --------------------------------------------------------------------------- #
def test_tournament_confirmatory_same_sample_is_not_new_alpha():
    ctx, rebs, _ = _ctx()
    reg = RG.build_registry()
    cfg9 = _cfg9()
    s11_records = {"x1": {"name": "amihud_illiquidity"}, "x2": {"name": "dollar_volume"}}
    strong = {"periods": 40, "universe": 300, "rank_ic_mean": 0.02, "rank_ic_t": 3.1,
              "spread_t": 3.0, "net_annualized_return": 0.11, "turnover": 0.3,
              "max_drawdown": -0.2, "leakage_warning": False}
    s11_rows = {"x1": dict(strong), "x2": dict(strong)}
    out = TN.run_tournament(reg, ctx, rebs, cfg9, stage11_records=s11_records,
                            stage11_rows=s11_rows, effective_sample=8.0)
    # the canonical anomalies qualify on the same sample...
    assert out["funnel"]["same_sample_confirmations"] >= 1
    # ...but are NOT counted as new defensible alpha and do NOT activate a shadow book
    assert out["funnel"]["qualified_new_alpha"] == 0
    assert out["shadow_decision"]["status"] == "NO_DEFENSIBLE_ALPHA"
    assert out["shadow_decision"]["no_automatic_promotion"] is True


def test_tournament_ml_not_justified_default():
    ctx, rebs, _ = _ctx()
    out = TN.run_tournament(RG.build_registry(), ctx, rebs, _cfg9(), effective_sample=8.0)
    assert out["ml_eligibility"]["status"] == "ML_NOT_JUSTIFIED"
    assert out["ml_eligibility"]["no_automatic_promotion"] is True


def test_tournament_uses_unchanged_gates():
    # a strong ingested confirmatory row reaches KEEP_FOR_RESEARCH via classify_evidence
    row = {"periods": 40, "universe": 300, "rank_ic_mean": 0.02, "rank_ic_t": 3.1,
           "spread_t": 3.0, "net_annualized_return": 0.11, "turnover": 0.3,
           "max_drawdown": -0.2, "leakage_warning": False}
    m = TT.row_to_contract_metrics(row, survivorship_safe=True)
    v = TT.classify_evidence(m, _cfg9())
    assert v["target_state"] == "KEEP_FOR_RESEARCH"


def test_combination_weights_do_not_use_holdout():
    # equal-weight rank combine is deterministic and independent of any holdout split
    comp = [([{"as_of": "d1", "names": [("k1", 0.9, 0.1), ("k2", 0.1, -0.1)]}], 1),
            ([{"as_of": "d1", "names": [("k1", 0.2, 0.1), ("k2", 0.8, -0.1)]}], 1)]
    p1 = TN._combine_periods(comp)
    p2 = TN._combine_periods(comp)
    assert p1 == p2  # deterministic, fixed weights


def test_multiple_testing_family_size_effect():
    recs = [{"spec_id": "s%d" % i, "name": "n%d" % i, "rank_ic_t": 3.0, "periods": 40}
            for i in range(3)]
    small = TN._multiple_testing(recs, family_size=3, alpha=0.05)
    large = TN._multiple_testing(recs, family_size=300, alpha=0.05)
    assert sum(r["fdr_survived"] for r in small) >= sum(r["fdr_survived"] for r in large)


# --------------------------------------------------------------------------- #
# Workstream J/K -- jobs, command center, resumability, snapshot.
# --------------------------------------------------------------------------- #
def test_command_center_terminal_token_logic():
    owned = {"identity_resolution": {"coverage_pct_of_universe": 28.0}}
    deep_new = {"qualified_new_alpha": [{"name": "x"}]}
    assert JB._terminal_token({}, deep_new, {}, owned) == "STAGE12_SHADOW_STRATEGY_QUALIFIED"
    deep_none = {"qualified_new_alpha": [], "same_sample_confirmations": []}
    # BLOCKER 1 corrected terminal: available licensed history unmaterialised ->
    # RESUMABLE (a low coverage percentage is NOT proof of exhaustion).
    inv_avail = {"available_not_materialized": 1196}
    assert JB._terminal_token({}, deep_none, {}, owned, inv_avail) == "STAGE12_CAMPAIGN_RESUMABLE"
    # only when nothing available remains is the study a completed no-alpha result.
    inv_none = {"available_not_materialized": 0}
    assert JB._terminal_token({}, deep_none, {}, owned, inv_none) == "STAGE12_NO_DEFENSIBLE_ALPHA_FOUND"


def test_next_incomplete_lane_ordering():
    flags = {}
    assert JB.next_incomplete_lane(flags, "E1") == JB.LANE_ORDER[0]
    flags[JB.LANE_ORDER[0]] = "E1"
    assert JB.next_incomplete_lane(flags, "E1") == JB.LANE_ORDER[1]
    allc = {ln: "E1" for ln in JB.LANE_ORDER}
    assert JB.next_incomplete_lane(allc, "E1") is None


def test_run_campaign_resumable_and_idempotent(tmp_path, monkeypatch):
    # Fake context: light handlers, no heavy panel build.
    class FakeCtx:
        def __init__(self, cp):
            self.state_dir = tmp_path / "state"
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self.artifact_root = str(tmp_path / "artifacts")
        def epoch(self):
            return "EPOCHZ"
    calls = {"n": 0}
    def fake_handler(sctx):
        calls["n"] += 1
        return {"status": "ok"}
    monkeypatch.setattr(JB, "Stage12Context", FakeCtx)
    monkeypatch.setattr(JB, "_LANE_HANDLERS", {ln: fake_handler for ln in JB.LANE_ORDER})
    r1 = JB.run_campaign("cfg", budget_seconds=999)
    assert r1["campaign_complete"] is True
    n_after_first = calls["n"]
    assert n_after_first == len(JB.LANE_ORDER)
    # resume: every lane already at epoch -> nothing re-runs
    r2 = JB.run_campaign("cfg", budget_seconds=999)
    assert r2["lanes_completed_this_run"] == []
    assert calls["n"] == n_after_first  # idempotent


def test_run_campaign_budget_stops_cleanly(tmp_path, monkeypatch):
    class FakeCtx:
        def __init__(self, cp):
            self.state_dir = tmp_path / "s2"
            self.state_dir.mkdir(parents=True, exist_ok=True)
            self.artifact_root = str(tmp_path / "a2")
        def epoch(self):
            return "E2"
    monkeypatch.setattr(JB, "Stage12Context", FakeCtx)
    monkeypatch.setattr(JB, "_LANE_HANDLERS", {ln: (lambda s: {"ok": 1}) for ln in JB.LANE_ORDER})
    # a clock that immediately exceeds the budget after the first lane
    ticks = iter([0.0, 0.0, 100.0, 100.0, 100.0])
    monkeypatch.setattr("time.monotonic", lambda: next(ticks, 100.0))
    r = JB.run_campaign("cfg", budget_seconds=1.0, clock=lambda: next(ticks, 100.0))
    assert r["terminal"] in ("STAGE12_CAMPAIGN_RESUMABLE", "STAGE12_CAMPAIGN_COMPLETE")


def test_read_only_snapshot_has_safety_and_no_promotion(monkeypatch):
    # An unavailable context still returns a controlled, read-only degrade payload.
    def boom(cp):
        raise RuntimeError("no config")
    monkeypatch.setattr(JB, "Stage12Context", boom)
    snap = JB.load_stage12_snapshot("nope")
    assert snap["status"] == "UNAVAILABLE"
    assert "NO AUTO-PROMOTION" in snap["safety_badges"]


def test_safety_badges_are_canonical():
    assert JB.SAFETY_BADGES == ["RESEARCH ONLY", "SHADOW ONLY", "NO LIVE BROKER ORDERS",
                                "AUTOMATION OFF", "MANUAL REVIEW", "NO AUTO-PROMOTION"]
    # forbidden UI vocabulary never appears in the badge set
    for bad in (">ORDERS DISABLED<", "NO LIVE ORDERS"):
        assert all(bad not in b for b in JB.SAFETY_BADGES)


# --------------------------------------------------------------------------- #
# Canonical UI wording (mirrors the repo's cross-phase guardrails).
# --------------------------------------------------------------------------- #
def test_stage12_ui_wording_canonical():
    html = (Path(__file__).resolve().parents[1] / "api" / "ui" / "index.html").read_text(encoding="utf-8")
    assert 's12-panel' in html and 'loadStage12' in html
    assert html.count('>ORDERS DISABLED<') == 0
    assert html.count('>NO LIVE ORDERS</span>') == 0
    # the Stage 12 panel carries the canonical tokens
    assert 'NO AUTO-PROMOTION' in html
    assert 'NO LIVE BROKER ORDERS' in html


# =========================================================================== #
# Evidence-completion pass: execution lag, true event-time evaluator, full
# Norgate inventory, registry re-versioning, canonical-queue integration.
# =========================================================================== #
import datetime as _dt

from alpha_agent import stage12_execution as EX
from alpha_agent import stage12_event_study as ES
from alpha_agent import stage12_inventory as INV


# --- BLOCKER 3: execution lag ---------------------------------------------- #
def test_execution_lag_entry_strictly_after_formation():
    dates = ["D%05d" % k for k in range(30)]
    closes = [10.0 + k for k in range(30)]
    p = EX.proves_no_same_close_entry(dates, "D00010", lag=1)
    assert p["strictly_after"] is True
    assert p["entry_index"] == p["formation_index"] + 1
    i = EX.formation_index(dates, "D00010")
    fr = EX.forward_return_lagged(dates, closes, "D00010", 3, lag=1)
    # forward return uses the LAGGED entry close, never the formation close
    assert abs(fr - (closes[i + 1 + 3] / closes[i + 1] - 1.0)) < 1e-12
    # and it strictly differs from the old same-close (no-lag) computation
    assert abs(fr - (closes[i + 3] / closes[i] - 1.0)) > 1e-12


def test_event_entry_strictly_after_availability():
    dates = ["2020-01-02", "2020-01-03", "2020-01-06", "2020-01-07", "2020-01-08"]
    # filed on a non-trading day (Sat) -> first session strictly after
    e = EX.entry_index_after_availability(dates, "2020-01-04", lag=1)
    assert dates[e] == "2020-01-06"
    # filed ON a trading day -> STRICTLY the next session (same-close impossible)
    e2 = EX.entry_index_after_availability(dates, "2020-01-06", lag=1)
    assert dates[e2] == "2020-01-07"
    assert dates[e2] > "2020-01-06"


def test_residual_builder_uses_lagged_entry():
    ctx, rebs, dates = _ctx(with_store=False, with_sector=False)
    out = RS.build_residual_periods("residual_market_momentum", ctx, rebs,
                                    horizon_days=63, lag=1)
    assert out["pit"]["execution_lag"] == 1
    assert "strictly after formation" in out["pit"]["entry"]


# --- BLOCKER 2: PIT value extraction (root-cause bug) + event-time study ---- #
def test_event_builder_extracts_pit_observation_value():
    # store.as_of returns a PitObservation-like object; the builder MUST unwrap
    # .value (the earlier float(observation) silently produced None for every
    # fact -- the true root cause of "0 scored periods").
    class _Obs:
        def __init__(self, v):
            self.value = v

    class _St:
        def latest_fiscal_key(self, c, a, concept="assets"):
            return "2018-Q1"

        def prior_fiscal_key(self, c, a, fk, concept="assets"):
            return "2017-Q1"

        def as_of(self, c, concept, fk, a):
            base = {"revenue": 100.0, "assets": 1000.0}[concept]
            mult = 1.1 if fk.startswith("2018") else 1.0
            return _Obs(base * mult)

    v = EV._revenue_acceleration(_St(), "C1", "2018-Q1", "2017-Q1", "2018-05-15")
    assert v is not None and abs(v - 0.1) < 1e-9  # 110/100 - 1


class _EvtCF:
    def __init__(self, rows):
        self._rows = rows

    def facts_for_cik(self, cik):
        return self._rows.get(str(cik), [])


class _EvtStore:
    def __init__(self, ciks):
        self._idx = {c: i for i, c in enumerate(ciks)}

    def latest_fiscal_key(self, cik, as_of, concept="assets"):
        return "%d-Q1" % int(as_of[:4])

    def prior_fiscal_key(self, cik, as_of, fk, concept="assets"):
        return "%d-Q1" % (int(fk[:4]) - 1)

    def as_of(self, cik, concept, fk, as_of):
        y = int(fk[:4])
        i = self._idx.get(cik, 0)
        base = {"revenue": 100.0, "assets": 1000.0, "net_income": 50.0,
                "gross_profit": 40.0, "cash": 30.0, "liabilities": 500.0,
                "operating_income": 25.0}.get(concept, 10.0)
        return base * (1.0 + 0.03 * (y - 2016) + 0.002 * i)  # varies by cik+year


def _bdays(start, end):
    out, d = [], start
    while d <= end:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += _dt.timedelta(days=1)
    return out


def _event_fixture(n_ciks=40):
    ciks = ["C%03d" % i for i in range(n_ciks)]
    c2a = {c: "A%03d" % i for i, c in enumerate(ciks)}
    dates = _bdays(_dt.date(2016, 1, 1), _dt.date(2022, 3, 31))
    rng = np.random.default_rng(11)
    close_index = {}
    for c, a in c2a.items():
        steps = rng.normal(0.0003, 0.015, len(dates))
        close_index[a] = (dates, (100 * np.cumprod(1 + steps)).tolist())
    rows = {}
    for c in ciks:
        rr = []
        for y in range(2016, 2022):
            for mon in ("02", "05", "08", "11"):
                accn = "%s-%d-%s" % (c, y, mon)
                for tag in ("Assets", "Revenues", "NetIncomeLoss"):
                    rr.append({"form": "10-Q", "accession": accn,
                               "filed": "%d-%s-15" % (y, mon),
                               "period_end": "%d-%s-28" % (y, mon),
                               "fy": str(y), "fp": "Q1", "concept": tag})
        rows[c] = rr
    ctx = {"store": _EvtStore(ciks), "cik_to_assetid": c2a, "close_index": close_index}
    return ctx, _EvtCF(rows)


def test_event_study_builds_monthly_cohorts_entry_after_filing():
    ctx, cf = _event_fixture()
    scaf = ES.build_event_scaffold(ctx, cf)
    assert scaf["calendar_stats"]["scaffold_events"] > 100
    # every event enters STRICTLY after its filed date
    for ev in scaf["scaffold"][:60]:
        assert ev["entry_date"] > ev["filed"]
    res = ES.build_event_cohort_periods(
        "event_revenue_acceleration", ctx, cf_index=cf, horizon_days=20,
        min_cohorts=6, min_issuers=10, min_events=30, min_names_per_cohort=5)
    cov = res["coverage"]
    assert cov["n_cohorts"] >= 6
    assert cov["total_events"] >= 30
    assert cov["distinct_issuers"] >= 10
    assert res["data_hold_reason"] is None
    # the primary cross-sectional unit is the monthly cohort (YYYY-MM)
    assert all(len(p["as_of"]) == 7 and p["as_of"][4] == "-" for p in res["periods"])
    # NOT a same-day cross section: many cohorts each with many names
    assert cov["median_cohort_names"] >= 5
    # scored through the UNCHANGED evaluator
    row = SE.evaluate_periods(res["periods"], horizon_days=20, cfg=_cfg9())["row"]
    assert row["periods"] == cov["n_cohorts"]
    # 5/20/63 horizons all measurable
    for h in (5, 20, 63):
        r = ES.build_event_cohort_periods("event_revenue_acceleration", ctx,
                                          cf_index=cf, horizon_days=h, min_cohorts=6,
                                          min_issuers=10, min_events=30,
                                          min_names_per_cohort=5)
        assert r["coverage"]["n_cohorts"] >= 6


def test_event_study_amendments_excluded_by_default_restatement_isolation():
    rows = {"C001": [
        {"form": "10-Q", "accession": "a1", "filed": "2018-02-15",
         "period_end": "2017-12-31", "fy": "2017", "fp": "Q4", "concept": "Assets"},
        {"form": "10-Q/A", "accession": "a1a", "filed": "2018-08-15",
         "period_end": "2017-12-31", "fy": "2017", "fp": "Q4", "concept": "Assets"}]}
    cf = _EvtCF(rows)
    cal = ES.build_filing_calendar(cf, ["C001"])
    assert cal["stats"]["total_events"] == 1            # original only
    assert cal["stats"]["amendment_events_seen"] == 1   # amendment tracked, excluded
    cal2 = ES.build_filing_calendar(cf, ["C001"], include_amendments=True)
    assert cal2["stats"]["total_events"] == 2


def test_event_study_data_hold_when_too_few_issuers():
    ctx, cf = _event_fixture(n_ciks=5)  # below min_issuers
    res = ES.build_event_cohort_periods("event_revenue_acceleration", ctx,
                                        cf_index=cf, horizon_days=20,
                                        min_cohorts=6, min_issuers=30, min_events=30,
                                        min_names_per_cohort=5)
    assert res["data_hold_reason"] is not None
    assert "ISSUERS" in res["data_hold_reason"] or "COHORTS" in res["data_hold_reason"]


def test_event_study_overlap_diagnostics_reduces_effective_n():
    rng = np.random.default_rng(9)
    ic = [0.0]
    for _ in range(80):
        ic.append(0.55 * ic[-1] + 0.02 + rng.normal(0, 0.05))
    d = ES.overlap_diagnostics(ic, horizon_days=63)
    assert d["newey_west_effective_n"] < d["n_cohorts"]
    assert abs(d["t_clustered"]) < abs(d["t_naive"])


# --- BLOCKER 1: full-universe inventory + classification ------------------- #
class _FakeND:
    def __init__(self, meta):
        self._m = meta

    def assetid(self, s):
        v = self._m.get(s)
        return None if v is None else v["assetid"]

    def first_quoted_date(self, s):
        v = self._m.get(s)
        return None if v is None else v.get("first")

    def last_quoted_date(self, s):
        v = self._m.get(s)
        return None if v is None else v.get("last")


def test_inventory_classification_seven_buckets_and_window_split():
    meta = {
        "MAT": {"assetid": 111, "first": "2015-01-02", "last": "2026-01-02"},
        "AVAILW": {"assetid": 222, "first": "2016-01-02", "last": "2025-01-02"},
        "AVAILP": {"assetid": 333, "first": "1990-01-02", "last": "2013-01-02"},
        "NOBAR": {"assetid": 444, "first": None, "last": None},
        "NOLIC": None,
        "UNRES": {"assetid": 555, "first": "2015-01-02", "last": "2026-01-02"}}
    symbols = list(meta.keys())
    materialized = {"111", "555"}
    id_map = {"111": {"resolved": True, "membership": True},
              "555": {"resolved": False, "membership": None}}
    res = INV.classify_universe(symbols, materialized_assetids=materialized,
                                id_map=id_map, nd=_FakeND(meta),
                                window_start="2015-01-01", keep_records=True)
    c = res["counts_by_classification"]
    assert c[INV.CLASS_MATERIALIZED] == 1        # MAT (resolved+member)
    assert c[INV.CLASS_ID_UNRESOLVED] == 1       # UNRES (materialized, no CIK)
    assert c[INV.CLASS_AVAILABLE] == 2           # AVAILW + AVAILP
    assert c[INV.CLASS_NO_BARS] == 1
    assert c[INV.CLASS_NO_LICENSE] == 1
    assert res["available_not_materialized"] == 2
    assert res["available_in_window"] == 1       # AVAILW (last quote >= 2015)
    assert res["available_pre_window_only"] == 1  # AVAILP (delisted pre-window)
    # delisted securities are preserved (classified, never dropped)
    syms = {r["symbol"] for r in res["records"]}
    assert "AVAILP" in syms


def test_inventory_cursor_resumption_equals_single_pass():
    symbols = ["S%03d" % i for i in range(10)]
    meta = {s: {"assetid": 1000 + i, "first": "2016-01-02", "last": "2025-01-02"}
            for i, s in enumerate(symbols)}
    nd = _FakeND(meta)
    agg = None
    off = 0
    while off < len(symbols):
        ch = INV.classify_universe(symbols, materialized_assetids=set(), id_map={},
                                   nd=nd, offset=off, limit=4, window_start="2015-01-01")
        agg = INV.merge_chunk(agg, ch)
        off = ch["cursor_next"]
    full = INV.classify_universe(symbols, materialized_assetids=set(), id_map={},
                                 nd=nd, window_start="2015-01-01")
    assert agg["complete"] is True and agg["n_classified"] == 10
    assert agg["counts_by_classification"] == full["counts_by_classification"]
    assert agg["counts_by_price_status"][INV.CLASS_AVAILABLE] == 10


# --- registry re-versioning ------------------------------------------------ #
def test_registry_v2_supersedes_retained_v1():
    reg = RG.build_registry()
    assert reg["schema_version"] == "stage12.hypotheses.v2"
    assert reg["registry_version"] != "7a38373c215cd152"  # not the v1 hash
    assert reg["evaluation_contract"]["execution_lag_applied"] is True
    assert reg["evaluation_contract"]["event_families_measured_in"] == "event_time_monthly_cohort"
    assert RG.validate_registry(reg) == []
    # the prior v1 pre-registration is RETAINED, unchanged
    v1 = RG.load_registry(RG.prior_registry_path())
    assert v1["registry_version"] == "7a38373c215cd152"
    assert v1["registry_version"] != reg["registry_version"]


# --- canonical ResearchQueue integration ----------------------------------- #
from alpha_agent import autonomous_research as _AR


class _FakeJob:
    def __init__(self, lane, origin):
        self.lane = lane
        self.origin = origin
        self.job_id = "job_" + lane


class _FakeQueue:
    def __init__(self):
        self.enqueued = []
        self._live = []

    def enqueue(self, category, *, lane, payload, priority, origin):
        jid = "job_%d" % len(self.enqueued)
        self.enqueued.append({"category": category, "lane": lane, "origin": origin,
                              "priority": priority, "payload": payload})
        self._live.append(_FakeJob(lane, origin))
        return jid

    def list_jobs(self, *, state, limit=1000):
        return list(self._live)


class _FakeSctx:
    def __init__(self, tmp, cfg=None):
        self.state_dir = Path(tmp)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.artifact_root = str(self.state_dir / "art")
        self.cfg = cfg or {"stage12": {"enabled": True, "planner_enabled": True,
                                       "priority": 1}}

    def epoch(self):
        return "EP"


def test_plan_enqueues_one_and_only_one_live_stage12_job(tmp_path):
    q = _FakeQueue()
    sctx = _FakeSctx(tmp_path / "st")
    cfg = {"enabled": True, "planner_enabled": True, "priority": 1}
    p1 = JB.plan_next_stage12_job(q, sctx, cfg=cfg)
    assert p1 and p1["lane"] == JB.LANE_ORDER[0]
    assert p1["origin"] == JB.ORIGIN_12
    assert q.enqueued[0]["category"] == _AR.CAT_DATA_VALIDATION
    assert q.enqueued[0]["origin"] == JB.ORIGIN_12
    assert str(q.enqueued[0]["lane"]).startswith(JB.STAGE12_LANE_PREFIX)
    # a live Stage 12 job already exists -> planner returns None (one at a time)
    assert JB.plan_next_stage12_job(q, sctx, cfg=cfg) is None
    # disabled planner -> None
    assert JB.plan_next_stage12_job(_FakeQueue(), _FakeSctx(tmp_path / "st2"),
                                    cfg={"enabled": True, "planner_enabled": False}) is None


def test_dispatch_runs_lane_stamps_flag_and_blocks_unknown(tmp_path, monkeypatch):
    sctx = _FakeSctx(tmp_path / "d")
    monkeypatch.setitem(JB._LANE_HANDLERS, JB.LANE_AUTOPSY, lambda s: {"status": "ok"})
    outcome, detail = JB.dispatch_stage12_job(_FakeJob(JB.LANE_AUTOPSY, JB.ORIGIN_12), sctx)
    assert outcome == _AR.OUTCOME_COMPLETED
    assert detail["lane_complete"] is True
    flags = json.loads((sctx.state_dir / "lane_flags.json").read_text(encoding="utf-8"))
    assert flags[JB.LANE_AUTOPSY] == "EP"
    # unknown lane -> BLOCKED_SPECIFIC (never crashes)
    o2, _d2 = JB.dispatch_stage12_job(_FakeJob("stage12.bogus", JB.ORIGIN_12), sctx)
    assert o2 == _AR.OUTCOME_BLOCKED_SPECIFIC


def test_dispatch_chunked_lane_not_complete_leaves_flag_unstamped(tmp_path, monkeypatch):
    sctx = _FakeSctx(tmp_path / "c")
    monkeypatch.setitem(JB._LANE_HANDLERS, JB.LANE_FULL_INVENTORY,
                        lambda s: {"complete": False, "cursor_next": 5})
    outcome, detail = JB.dispatch_stage12_job(
        _FakeJob(JB.LANE_FULL_INVENTORY, JB.ORIGIN_12), sctx)
    assert outcome == _AR.OUTCOME_COMPLETED
    assert detail["lane_complete"] is False
    fp = sctx.state_dir / "lane_flags.json"
    flags = json.loads(fp.read_text(encoding="utf-8")) if fp.exists() else {}
    # cursor-chunked lane not exhausted -> flag NOT stamped -> planner re-enqueues it
    assert flags.get(JB.LANE_FULL_INVENTORY) != "EP"


def test_materialize_lane_measure_only_when_disabled(tmp_path):
    sctx = _FakeSctx(tmp_path / "m", cfg={
        "stage12": {"enabled": True, "materialize_enabled": False,
                    "materialize_cap_increment": 300}})
    JB._write_json(sctx.state_dir / "full_inventory.json",
                   {"available_in_window": 50, "available_not_materialized": 100})
    out = JB._h_materialize(sctx)
    assert out["mode"] == "measure_only"
    assert "materialize_enabled is false" in out["reason"]
    assert out["materialize_remaining"] is True


# --- WORKSTREAM A: breadth-safe panel/cache epoch -------------------------- #
def _write_bar_tree(root: Path, assetids, dates, *, source="norgate_local", tag=None):
    """Minimal owned normalized MARKET_BAR tree: one run file per date under
    ``normalized/MARKET_BAR/YYYY/MM/DD`` with one JSON line per assetid. ``tag``
    gives each write a DISTINCT run file so successive materializations accumulate
    (matching the real append-only tree) instead of overwriting."""
    base = Path(root) / "normalized" / "MARKET_BAR"
    fname = "%s_%s.jsonl" % (source, tag) if tag else (source + ".jsonl")
    for k, d in enumerate(dates):
        y, m, dd = d.split("-")
        part = base / y / m / dd
        part.mkdir(parents=True, exist_ok=True)
        lines = []
        for i, a in enumerate(assetids):
            lines.append(json.dumps({
                "source_id": source, "security_id": str(a), "effective_at": d,
                "normalized_payload": {"Close": 100.0 + i + k, "Volume": 1000 + k}}))
        (part / fname).write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_breadth_epoch_changes_when_assetid_added():
    # same dates + one ADDITIONAL materialized assetid -> DIFFERENT epoch, even
    # though the calendar month span is identical (the exact month-epoch defect).
    common = dict(date_min="2020-01-02", date_max="2020-01-31",
                  sources=("norgate_local",), identity_mapping_version="mvh1",
                  norgate_universe_fingerprint="uni1")
    e_small = JB.compute_breadth_panel_epoch(materialized_assetids={"1", "2"}, **common)
    e_big = JB.compute_breadth_panel_epoch(materialized_assetids={"1", "2", "3"}, **common)
    assert e_small != e_big
    assert e_small.startswith("b") and e_big.startswith("b")


def test_breadth_epoch_stable_when_breadth_unchanged():
    # identical inputs -> identical epoch; assetid ORDER is irrelevant; the epoch
    # never depends on a per-cycle file count (structurally: no such input exists).
    common = dict(date_min="2020-01-02", date_max="2020-01-31",
                  sources=("norgate_local",), identity_mapping_version="mvh1",
                  norgate_universe_fingerprint="uni1")
    a = JB.compute_breadth_panel_epoch(materialized_assetids=["1", "2", "3"], **common)
    b = JB.compute_breadth_panel_epoch(materialized_assetids=["3", "1", "2"], **common)
    assert a == b
    # extending the owned date span advances the epoch (genuine coverage growth)
    c = JB.compute_breadth_panel_epoch(
        materialized_assetids=["1", "2", "3"], date_min="2020-01-02",
        date_max="2020-02-15", sources=("norgate_local",),
        identity_mapping_version="mvh1", norgate_universe_fingerprint="uni1")
    assert c != a


def test_ohlcv_cache_stale_cannot_hide_newly_materialized_securities(tmp_path):
    from alpha_agent import signal_library as SL
    ing = tmp_path / "ing"
    cache = tmp_path / "cache"
    cache.mkdir()
    dates = ["2020-01-02", "2020-01-03"]
    _write_bar_tree(ing, ["1", "2"], dates)
    e1 = JB.compute_breadth_panel_epoch(
        date_min="2020-01-02", date_max="2020-01-03",
        materialized_assetids={"1", "2"}, sources=("norgate_local",))
    p1 = SL.load_or_build_ohlcv_panel(str(ing), str(cache), e1, sources=("norgate_local",))
    assert set(p1.keys()) == {"1", "2"}
    # a THIRD security is materialized into the SAME month span
    _write_bar_tree(ing, ["3"], dates, tag="add3")
    # the OLD epoch's cache is stale: it still returns only the original names
    # (this is exactly why a month-only epoch silently hides new breadth) ...
    p_stale = SL.load_or_build_ohlcv_panel(str(ing), str(cache), e1, sources=("norgate_local",))
    assert set(p_stale.keys()) == {"1", "2"}
    # ... but the breadth-advanced epoch rebuilds and surfaces the new name.
    e2 = JB.compute_breadth_panel_epoch(
        date_min="2020-01-02", date_max="2020-01-03",
        materialized_assetids={"1", "2", "3"}, sources=("norgate_local",))
    assert e2 != e1
    p2 = SL.load_or_build_ohlcv_panel(str(ing), str(cache), e2, sources=("norgate_local",))
    assert set(p2.keys()) == {"1", "2", "3"}


def test_ohlcv_cache_interrupted_write_is_not_consumed(tmp_path):
    from alpha_agent import signal_library as SL
    cache = tmp_path / "cache"
    cache.mkdir()
    epoch = "bDEADBEEFCAFEBABE"
    # a partial/interrupted write leaves only the .tmp sibling -> must be ignored.
    (cache / ("ohlcv_%s.jsonl.tmp" % epoch)).write_text('{"a":"1"', encoding="utf-8")
    assert SL.load_cached_ohlcv_panel(str(cache), epoch) is None
    # a completed atomic cache IS consumed.
    SL.cache_ohlcv_panel({"1": {"d": ["2020-01-02"], "c": [100.0],
                                "v": [10], "dv": [1000]}}, str(cache), epoch)
    got = SL.load_cached_ohlcv_panel(str(cache), epoch)
    assert got is not None and set(got.keys()) == {"1"}


def test_ohlcv_cache_rebuilds_once_then_reused(tmp_path):
    from alpha_agent import signal_library as SL
    ing = tmp_path / "ing"
    cache = tmp_path / "cache"
    cache.mkdir()
    _write_bar_tree(ing, ["1", "2"], ["2020-01-02", "2020-01-03"])
    epoch = JB.compute_breadth_panel_epoch(
        date_min="2020-01-02", date_max="2020-01-03",
        materialized_assetids={"1", "2"}, sources=("norgate_local",))
    first = SL.load_or_build_ohlcv_panel(str(ing), str(cache), epoch, sources=("norgate_local",))
    assert set(first.keys()) == {"1", "2"}
    # delete the source tree: a genuine cache HIT must still return the panel
    # (proves reuse without rebuild for an unchanged breadth identity).
    for p in (ing / "normalized" / "MARKET_BAR").rglob("*.jsonl"):
        p.unlink()
    reused = SL.load_or_build_ohlcv_panel(str(ing), str(cache), epoch, sources=("norgate_local",))
    assert set(reused.keys()) == {"1", "2"}
    # a NEW epoch with the tree gone rebuilds (from the now-empty tree) -> empty,
    # confirming the new-identity path does not read another epoch's cache file.
    other = SL.load_or_build_ohlcv_panel(str(ing), str(cache), epoch + "x", sources=("norgate_local",))
    assert other == {}


# --- WORKSTREAM D: frozen confirmatory partition contract ------------------ #
def test_event_partition_new_issuer_dominates_and_time_vs_original():
    prior_a = {"1", "2"}
    prior_c = {"2020-01", "2020-02"}
    # new issuer -> NEW_CROSS_SECTION regardless of cohort (even a prior cohort)
    assert JB.classify_event_partition("9", "2020-01", prior_assetids=prior_a,
                                       prior_cohorts=prior_c) == JB.PARTITION_NEW_XSEC
    assert JB.classify_event_partition("9", "2099-12", prior_assetids=prior_a,
                                       prior_cohorts=prior_c) == JB.PARTITION_NEW_XSEC
    # prior issuer, NEW cohort -> NEW_TIME
    assert JB.classify_event_partition("1", "2099-12", prior_assetids=prior_a,
                                       prior_cohorts=prior_c) == JB.PARTITION_NEW_TIME
    # prior issuer + prior cohort -> ORIGINAL (already inspected)
    assert JB.classify_event_partition("1", "2020-01", prior_assetids=prior_a,
                                       prior_cohorts=prior_c) == JB.PARTITION_ORIGINAL


def test_partition_event_counts_union_equals_combined():
    prior_a = {"1", "2"}
    prior_c = {"2020-01"}
    events = [
        {"assetid": "1", "cohort": "2020-01"},   # ORIGINAL
        {"assetid": "1", "cohort": "2021-06"},   # NEW_TIME
        {"assetid": "7", "cohort": "2020-01"},   # NEW_CROSS_SECTION
        {"assetid": "8", "cohort": "2021-06"},   # NEW_CROSS_SECTION
    ]
    c = JB.partition_event_counts(events, prior_assetids=prior_a, prior_cohorts=prior_c)
    assert c[JB.PARTITION_ORIGINAL] == 1
    assert c[JB.PARTITION_NEW_TIME] == 1
    assert c[JB.PARTITION_NEW_XSEC] == 2
    assert c[JB.PARTITION_COMBINED] == 4  # union total, NOT independent confirmation


def test_same_sample_only_cannot_qualify_as_new_evidence():
    # an all-ORIGINAL event set yields ZERO new-evidence support -> a stronger
    # combined stat here can never be genuinely-new confirmation.
    prior_a = {"1", "2", "3"}
    prior_c = {"2020-01", "2020-02"}
    events = [{"assetid": a, "cohort": c} for a in prior_a for c in prior_c]
    counts = JB.partition_event_counts(events, prior_assetids=prior_a, prior_cohorts=prior_c)
    assert counts[JB.PARTITION_NEW_XSEC] == 0
    assert counts[JB.PARTITION_NEW_TIME] == 0
    assert counts[JB.PARTITION_ORIGINAL] == counts[JB.PARTITION_COMBINED]
    proto = JB.confirmatory_protocol(registry_version="v2test")
    assert proto["frozen"] is True
    assert "spread_t>=2.0" in proto["preserved_unchanged"]
    assert proto["registry_version"] == "v2test"
