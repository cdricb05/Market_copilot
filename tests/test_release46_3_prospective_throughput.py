"""Release 46.3 - prospective throughput, information-set expansion, velocity.

Four claims are locked shut here.

**The original evidence is untouchable.** The ten seed specifications' hashes
are pinned to the exact values the production registry froze on 2026-08-25;
registering the expanded field preserves every seed challenger's freeze
verbatim and detects any in-place edit as a retune.

**Breadth never weakens the boundary.** Every expansion challenger enters
through the same frozen door: complete specification, canonical constants
declared before the rule ran, TRUE_FORWARD-only emission, idempotent by the
declared identity key, and a horizon or model change is MATERIAL and forces a
new version with a new forward clock.

**Raw rows are not evidence.** The velocity owner discounts overlapping
horizons inside a cell, assumes perfect dependence inside a declared cluster,
and reports the refused amount as an explicit dependence penalty. The
information-set gate cannot fire early.

**Nothing confers capital.** The planner nominates and never registers; the
lanes report and never block each other; a blocked intraday probe is a named
fact, not a failure.
"""
from __future__ import annotations

import datetime as dt
import json

import pytest

from alpha_agent import r46 as R46
from alpha_agent.r46 import advance as AD
from alpha_agent.r46 import challengers as CH
from alpha_agent.r46 import clock as CK
from alpha_agent.r46 import contract as C
from alpha_agent.r46 import emit as EM
from alpha_agent.r46 import intraday as IN
from alpha_agent.r46 import ledger as LG
from alpha_agent.r46 import marketdata as MD
from alpha_agent.r46 import planner as PL
from alpha_agent.r46 import registry as RG
from alpha_agent.r46 import velocity as VL
from api import prospective_tournament as PT

TEST_CAMPAIGN = "r46_3_pytest_campaign"

FROZEN_AT = "2026-08-25T21:33:09Z"
FROZEN_AT_2 = "2026-08-26T16:00:00Z"

#: The production registry's seed hashes, frozen 2026-08-25T21:33:09Z. These
#: are historical facts; if any of them moves, seed economics were edited.
PRODUCTION_SEED_SPEC_HASHES = {
    "r46_eq_xs_mom_12_1": "c2aeb90d2e79ffb1c2666d6e46a6377a1d13a5a3325ffd6c60c4f9b436010c09",
    "r46_eq_xs_rev_5d": "45b6c2838a93a29949c5272142fb50624a79ac7383233ae8bee30669b1609518",
    "r46_eq_xs_lowvol_60d": "64cea1d3e8522554769a3c189e11e258b62bba7d6e4acb45911a92460fe28d0e",
    "r46_eq_xs_resid_mom_12_1": "5a65aaa2e6310af6b36a43266cf87d5085db4ff33c6f013fc71def0f1c10a4f9",
    "r46_fut_ts_mom_252": "6e9f9947fde8d4e4a6323cbaec50666a8a7f165a36598d124d73b8b98a9e6f3b",
    "r46_fx_xs_mom_252": "4307af0f9e03e1df841078f42cfbf3aa2ab5bf210115d0bc841fc9dddd1a83a7",
    "r46_vx_term_carry_5d": "62625f0dead4fc1f31cf27833fcde67a53e1246a38be082a20e663c3a6be0082",
    "r46_rates_curve_rv_5d": "6fc519beefb568abd88bf5387496a4412acac488ab2b1a82f511ed5a946933de",
    "r46_comdty_xs_mom_252": "492581a34daa731e04fa2a432d280acdb26af9bdc2642fb112668b92a8ccfd45",
    "r46_spx_trend_200d": "36ca1f2776d3e2979197f93869b43291c2b55829770d2c4531e8263759a46400",
}


@pytest.fixture()
def sandbox(tmp_path, monkeypatch):
    """Point every R46 write at a temp root. The real ledger is never touched."""
    monkeypatch.setattr(R46, "RESEARCH_ROOT", tmp_path / "r46root")
    monkeypatch.setattr(C, "ARTIFACT_DIR", tmp_path / "r46root" / TEST_CAMPAIGN)
    return tmp_path


def _fresh(monkeypatch, day=dt.date(2026, 8, 25)):
    monkeypatch.setattr(MD, "last_session", lambda s: day)


def _stub_books(monkeypatch):
    """Every challenger builds the same tiny valid book - emission mechanics
    are under test here, not signal content."""
    def tiny(spec):
        return {"state": "OK",
                "legs": [{"instrument": "SPY", "weight": 1.0, "score": 1.0,
                          "side": "LONG", "cost_class": "US_EQUITY"}],
                "gross_notional": 1.0, "net_notional": 1.0, "n_legs": 1,
                "market_state_snapshot_hash": "h", "input_evidence_hash": "h"}
    monkeypatch.setattr(EM.CH, "build", tiny)


# =========================================================================== #
# 1. The original Release-46 evidence is preserved
# =========================================================================== #
def test_seed_spec_hashes_match_the_production_freeze_exactly():
    for cid, frozen_hash in PRODUCTION_SEED_SPEC_HASHES.items():
        spec = CH.spec_by_id(cid)
        assert spec is not None, cid
        assert CH.spec_hash(spec) == frozen_hash, (
            "%s: the seed specification's economics moved" % cid)


def test_seed_specs_tuple_is_still_exactly_ten():
    assert len(CH.SEED_SPECS) == 10
    assert all(s.get("cohort") is None for s in CH.SEED_SPECS), (
        "seed spec dicts must stay byte-frozen; cohort is resolved by the "
        "registry, not written into them")


def test_registering_the_union_preserves_every_seed_freeze(sandbox,
                                                           monkeypatch):
    _fresh(monkeypatch)
    seed_only = RG.register(TEST_CAMPAIGN, specs=CH.SEED_SPECS,
                            frozen_at=FROZEN_AT)
    union = RG.register(TEST_CAMPAIGN, frozen_at=FROZEN_AT_2)
    seed_before = {c["challenger_id"]: c for c in seed_only["challengers"]}
    for c in union["challengers"]:
        cid = c["challenger_id"]
        if cid in seed_before:
            assert c["frozen_at"] == FROZEN_AT, cid
            assert c["spec_hash"] == seed_before[cid]["spec_hash"], cid
            assert c["cohort"] == "R46_SEED"
        else:
            assert c["frozen_at"] == FROZEN_AT_2, cid
            # Releases 46.4 and 46.5 register their cohorts through the same door.
            assert c["cohort"] in (CH.EXPANSION_COHORT, CH.R46_4_COHORT,
                                   CH.R46_5_COHORT)
    assert union["retune_free"] is True
    assert union["n_r46_challengers"] == len(CH.ALL_SPECS)


# =========================================================================== #
# 2. The expansion cohort enters through the same frozen door
# =========================================================================== #
REQUIRED_SPEC_FIELDS = ("challenger_id", "challenger_version", "family",
                        "asset_class", "instrument", "prediction_type",
                        "horizons", "control", "benchmark", "cost_class",
                        "universe", "thesis", "parameters", "signal_owner",
                        "cohort", "information_family", "dependence_cluster")


def test_every_expansion_spec_is_complete_and_unsearched():
    assert len(CH.EXPANSION_SPECS) == 11
    for spec in CH.EXPANSION_SPECS:
        for f in REQUIRED_SPEC_FIELDS:
            assert f in spec, "%s missing %s" % (spec.get("challenger_id"), f)
        assert spec["challenger_version"] == "v1"
        assert spec["parameters_were_searched"] is False
        assert spec["promotion_allowed"] is False
        assert spec["research_shadow_only"] is True
        assert spec["cohort"] == CH.EXPANSION_COHORT
        assert set(spec["horizons"]) <= {1, 5, 20}
        assert spec["cost_class"] in C.COST_BPS_PER_SIDE or \
            spec["cost_class"] == "MIXED_FUTURES"
        assert spec["signal_owner"] in CH._OWNERS


def test_no_duplicate_ids_and_no_duplicate_identity_slots():
    ids = [s["challenger_id"] for s in CH.ALL_SPECS]
    # 10 seed + 11 expansion (R46.3) + 9 P&L-offensive (R46.4)
    # + 3 forward-harvest (R46.5: earnings drift, insider cluster, insider NPR).
    assert len(ids) == len(set(ids)) == 33
    slots = [(s["challenger_id"], s["challenger_version"], s["instrument"])
             for s in CH.ALL_SPECS]
    assert len(slots) == len(set(slots))


def test_a_horizon_change_is_material_and_needs_a_new_version():
    spec = dict(CH.spec_by_id("r46_3_eq_xs_max_lottery"))
    changed = dict(spec)
    changed["horizons"] = (5,)
    verdict = RG.classify_change(spec, changed)
    assert verdict["classification"] == "MATERIAL"
    assert verdict["requires_new_version"] is True


def test_an_ml_model_change_is_material():
    spec = dict(CH.spec_by_id("r46_3_ml_eq_xs_ridge"))
    changed = dict(spec)
    changed["parameters"] = dict(spec["parameters"], ridge_lambda=2.0)
    verdict = RG.classify_change(spec, changed)
    assert verdict["classification"] == "MATERIAL"


def test_ml_specs_freeze_the_full_training_protocol():
    for cid in ("r46_3_ml_eq_xs_ridge", "r46_3_ml_eq_xs_gbt"):
        p = CH.spec_by_id(cid)["parameters"]
        for f in ("model_class", "features", "preprocessing",
                  "training_sessions", "training_stride_sessions",
                  "target_sessions", "retraining_policy", "random_seed"):
            assert f in p, "%s missing %s" % (cid, f)
        assert p["features"] == list(CH.ML_FEATURES)
        assert "refit deterministically at each emission" in \
            p["retraining_policy"]


def test_ensemble_weights_are_frozen_thirds():
    p = CH.spec_by_id("r46_3_ens_eq_xs_equal")["parameters"]
    assert p["weights"] == [1 / 3.0, 1 / 3.0, 1 / 3.0]


def test_same_mechanism_cells_share_a_dependence_cluster():
    assert CH.cluster_for(CH.spec_by_id("r46_3_vx_term_carry_1d")) == \
        CH.cluster_for(CH.spec_by_id("r46_vx_term_carry_5d")) == "VX_CARRY"
    assert CH.cluster_for(CH.spec_by_id("r46_3_ml_eq_xs_gbt")) == \
        CH.cluster_for(CH.spec_by_id("r46_eq_xs_mom_12_1")) == "EQ_XS_PRICE"
    assert CH.cluster_for(CH.spec_by_id("r46_3_comdty_curve_carry")) == \
        "COMMODITY_CURVE"


# =========================================================================== #
# 3. Emission across the expanded field: TRUE_FORWARD, idempotent, isolated
# =========================================================================== #
def _expected_cells():
    return sum(len(s["horizons"]) for s in CH.ALL_SPECS)


def test_expanded_emission_is_true_forward_and_idempotent(sandbox,
                                                          monkeypatch):
    _fresh(monkeypatch)
    reg = RG.register(TEST_CAMPAIGN, frozen_at=FROZEN_AT)
    _stub_books(monkeypatch)
    now = dt.datetime(2026, 8, 25, 22, 0, tzinfo=dt.timezone.utc)
    first = EM.emit(TEST_CAMPAIGN, reg, now)
    # 23 cells at R46.3; Release 46.4 added nine challengers / nine cells;
    # Release 46.5 added three challengers / three cells.
    assert first["n_appended"] == _expected_cells() == 35
    second = EM.emit(TEST_CAMPAIGN, reg, now)
    assert second["n_appended"] == 0
    assert second["n_duplicates_skipped"] == first["n_appended"]
    for row in LG.predictions(TEST_CAMPAIGN):
        assert row["forward_evidence_type"] == C.TRUE_FORWARD
        assert row["emitted_at_utc"] < row["outcome_window_start_utc"]
        assert row["freeze_before_emission_evidence"]["strictly_ordered"] \
            is True


def test_one_broken_expansion_challenger_never_blocks_the_field(sandbox,
                                                                monkeypatch):
    _fresh(monkeypatch)
    reg = RG.register(TEST_CAMPAIGN, frozen_at=FROZEN_AT)

    def flaky(spec):
        if spec["challenger_id"] == "r46_3_ml_eq_xs_gbt":
            raise ZeroDivisionError("training exploded")
        return {"state": "OK",
                "legs": [{"instrument": "SPY", "weight": 1.0, "score": 1.0,
                          "side": "LONG", "cost_class": "US_EQUITY"}],
                "gross_notional": 1.0, "net_notional": 1.0, "n_legs": 1,
                "market_state_snapshot_hash": "h", "input_evidence_hash": "h"}

    monkeypatch.setattr(EM.CH, "build", flaky)
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 22, 0,
                                       tzinfo=dt.timezone.utc))
    reasons = {s["challenger_id"]: s["reason"] for s in batch["skipped"]}
    assert reasons["r46_3_ml_eq_xs_gbt"] == EM.REASON_BUILD_FAILED
    assert batch["n_predictions"] == _expected_cells() - 1


def test_an_ml_row_states_its_training_cutoff(sandbox, monkeypatch):
    _fresh(monkeypatch)
    reg = RG.register(TEST_CAMPAIGN, frozen_at=FROZEN_AT)

    def with_cutoff(spec):
        book = {"state": "OK",
                "legs": [{"instrument": "SPY", "weight": 1.0, "score": 1.0,
                          "side": "LONG", "cost_class": "US_EQUITY"}],
                "gross_notional": 1.0, "net_notional": 1.0, "n_legs": 1,
                "market_state_snapshot_hash": "h", "input_evidence_hash": "h"}
        if spec["signal_owner"] == "_ml_eq_cross_section":
            book["training_data_cutoff"] = "2026-07-28"
        return book

    monkeypatch.setattr(EM.CH, "build", with_cutoff)
    batch = EM.build_batch(TEST_CAMPAIGN, reg,
                           dt.datetime(2026, 8, 25, 22, 0,
                                       tzinfo=dt.timezone.utc))
    by_cid = {}
    for r in batch["rows"]:
        by_cid.setdefault(r["challenger_id"], r)
    assert by_cid["r46_3_ml_eq_xs_ridge"]["training_data_cutoff"] == \
        "2026-07-28"
    assert by_cid["r46_eq_xs_mom_12_1"]["training_data_cutoff"] is None


def test_active_specs_respect_a_seed_only_registry(sandbox, monkeypatch):
    """A hermetic registry that froze only the seed ten emits exactly them."""
    _fresh(monkeypatch)
    reg = RG.register(TEST_CAMPAIGN, specs=CH.SEED_SPECS, frozen_at=FROZEN_AT)
    active = RG.active_specs(reg)
    assert {s["challenger_id"] for s in active} <= \
        {s["challenger_id"] for s in CH.SEED_SPECS}


# =========================================================================== #
# 4. The signal conventions that can be proved without a data estate
# =========================================================================== #
def test_turn_of_month_window_membership():
    assert CH._tom_window_membership(dt.date(2026, 8, 31)) is True   # last
    assert CH._tom_window_membership(dt.date(2026, 9, 1)) is True    # +1
    assert CH._tom_window_membership(dt.date(2026, 9, 3)) is True    # +3
    assert CH._tom_window_membership(dt.date(2026, 9, 4)) is False   # +4
    assert CH._tom_window_membership(dt.date(2026, 8, 27)) is False  # middle


def test_dated_contract_parse():
    assert MD._parse_dated("CL-2026Z") == ("CL", 2026, 12)
    assert MD._parse_dated("ZC-2027H") == ("ZC", 2027, 3)
    assert MD._parse_dated("NOT_A_CONTRACT") is None


def test_curve_carry_sign_and_annualisation(monkeypatch):
    import pandas as pd
    monkeypatch.setattr(MD, "dated_futures_symbols",
                        lambda: ("CL-2026X", "CL-2026Z", "CL-1990F"))
    series = {
        "CL-2026X": pd.Series([102.0], index=pd.DatetimeIndex(["2026-08-25"])),
        "CL-2026Z": pd.Series([100.0], index=pd.DatetimeIndex(["2026-08-25"])),
    }
    monkeypatch.setattr(MD, "closes",
                        lambda sym, start=None: series.get(sym))
    out = MD.futures_curve_carry("CL", dt.date(2026, 8, 26))
    assert out["state"] == "OK"
    assert out["front"]["symbol"] == "CL-2026X"
    assert out["months_between"] == 1
    import math
    assert out["carry_annualised"] == pytest.approx(
        math.log(102.0 / 100.0) * 12.0)


def test_curve_carry_skips_the_spot_month(monkeypatch):
    import pandas as pd
    monkeypatch.setattr(MD, "dated_futures_symbols",
                        lambda: ("CL-2026Q", "CL-2026U", "CL-2026V"))
    series = {s: pd.Series([100.0], index=pd.DatetimeIndex(["2026-08-25"]))
              for s in ("CL-2026Q", "CL-2026U", "CL-2026V")}
    monkeypatch.setattr(MD, "closes",
                        lambda sym, start=None: series.get(sym))
    out = MD.futures_curve_carry("CL", dt.date(2026, 8, 26))
    # August (Q) and anything earlier are out; only Sep (U) and Oct (V) count.
    assert out["state"] == "OK"
    assert out["front"]["symbol"] == "CL-2026U"
    assert out["next"]["symbol"] == "CL-2026V"


# =========================================================================== #
# 5. Velocity: raw rows are not evidence
# =========================================================================== #
def _reg_stub(entries):
    return {"challengers": entries}


def _outcome(cid, horizon, as_of):
    return {"challenger_id": cid, "horizon": horizon,
            "effective_as_of": as_of, "net_alpha_vs_control": 0.001}


def test_cell_effective_is_overlap_discounted_and_date_capped():
    reg = _reg_stub([{"challenger_id": "a", "challenger_version": "v1",
                      "family": "F", "asset_class": "US_EQUITY",
                      "horizons": [20], "state": C.FORWARD_PENDING,
                      "dependence_cluster": "X"}])
    outs = [_outcome("a", 20, "2026-0%d-01" % (i % 9 + 1)) for i in range(40)]
    cells = VL._cells(reg, [], outs)
    assert len(cells) == 1
    # 40 raw / 20 horizon = 2, and 9 distinct dates does not lift it above 2.
    assert cells[0]["raw_matured"] == 40
    assert cells[0]["effective_independent"] == 2


def test_a_cluster_counts_its_best_cell_once():
    cells = [
        {"dependence_cluster": "X", "challenger_id": "a", "horizon": 1,
         "asset_class": "US_EQUITY", "information_family": "PRICE_STATE",
         "raw_matured": 10, "effective_independent": 5,
         "state": C.FORWARD_PENDING, "expected_effective_per_session": 1.0},
        {"dependence_cluster": "X", "challenger_id": "b", "horizon": 5,
         "asset_class": "US_EQUITY", "information_family": "PRICE_STATE",
         "raw_matured": 15, "effective_independent": 3,
         "state": C.FORWARD_PENDING, "expected_effective_per_session": 0.2},
    ]
    clusters = VL._clusters(cells)
    assert len(clusters) == 1
    assert clusters[0]["effective_independent"] == 5
    assert clusters[0]["naive_sum_of_cell_effectives"] == 8
    assert clusters[0]["within_cluster_discount"] == 3


def test_velocity_reports_the_dependence_penalty(sandbox, monkeypatch):
    _fresh(monkeypatch)
    reg = _reg_stub([
        {"challenger_id": "a", "challenger_version": "v1", "family": "F1",
         "asset_class": "US_EQUITY", "horizons": [1],
         "state": C.FORWARD_PENDING, "dependence_cluster": "X"},
        {"challenger_id": "b", "challenger_version": "v1", "family": "F1",
         "asset_class": "US_EQUITY", "horizons": [1],
         "state": C.FORWARD_PENDING, "dependence_cluster": "X"},
        {"challenger_id": "c", "challenger_version": "v1", "family": "F2",
         "asset_class": "FX", "horizons": [1],
         "state": C.FORWARD_PENDING, "dependence_cluster": "Y"},
    ])
    outs = ([_outcome("a", 1, "2026-08-0%d" % (i + 1)) for i in range(4)]
            + [_outcome("b", 1, "2026-08-0%d" % (i + 1)) for i in range(4)]
            + [_outcome("c", 1, "2026-08-0%d" % (i + 1)) for i in range(2)])
    monkeypatch.setattr(VL.LG, "predictions", lambda cid: [])
    monkeypatch.setattr(VL.LG, "outcomes", lambda cid: outs)
    body = VL.build(TEST_CAMPAIGN, reg)
    # clusters: X = max(4, 4) = 4, Y = 2 -> tournament 6; naive sum 10.
    assert body["effective_independent_observations"] == 6
    assert body["naive_sum_of_cell_effectives"] == 10
    assert body["dependence_penalty"] == 4
    assert body["raw_matured_rows"] == 10


def test_velocity_bottleneck_before_first_maturity(sandbox, monkeypatch):
    _fresh(monkeypatch)
    reg = _reg_stub([{"challenger_id": "a", "challenger_version": "v1",
                      "family": "F", "asset_class": "US_EQUITY",
                      "horizons": [20], "state": C.FORWARD_PENDING,
                      "dependence_cluster": "X"}])
    pred = {"prediction_id": "p1", "challenger_id": "a", "horizon": 20,
            "effective_as_of": "2026-08-26", "n_legs": 100,
            "horizon_end_expected": "2026-09-23"}
    monkeypatch.setattr(VL.LG, "predictions", lambda cid: [pred])
    monkeypatch.setattr(VL.LG, "outcomes", lambda cid: [])
    body = VL.build(TEST_CAMPAIGN, reg)
    assert body["current_evidence_bottleneck"]["binding"]["code"] == \
        "AWAITING_FIRST_MATURITY"
    assert body["information_set_state"] == VL.INFO_SET_TOO_EARLY
    assert body["next_maturity"] == "2026-09-23"
    assert body["effective_independent_observations"] == 0
    assert body["raw_predictions_emitted"] == 1


def test_information_set_insufficient_cannot_fire_early(sandbox, monkeypatch):
    _fresh(monkeypatch)
    reg = _reg_stub([{"challenger_id": "a", "challenger_version": "v1",
                      "family": "F%d" % i, "asset_class": "US_EQUITY",
                      "horizons": [1], "state": C.FORWARD_PENDING,
                      "dependence_cluster": "C%d" % i}
                     for i in range(7)])
    outs = [_outcome("a", 1, "2026-08-01")]
    monkeypatch.setattr(VL.LG, "predictions", lambda cid: [])
    monkeypatch.setattr(VL.LG, "outcomes", lambda cid: outs)
    body = VL.build(TEST_CAMPAIGN, reg)
    assert body["information_set_state"] != VL.INFO_SET_INSUFFICIENT


def test_turn_of_month_cluster_is_not_projected_at_a_daily_clock():
    rate = VL.EXPECTED_EMISSION_RATE_BY_CLUSTER["CALENDAR_TOM"]
    assert rate == pytest.approx(4.0 / 21.0)


# =========================================================================== #
# 6. The advance rebuilds velocity and the plan, fail-soft
# =========================================================================== #
def test_advance_writes_velocity_and_plan_artifacts(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING,
                         "challenger_version": "v1", "family": "F",
                         "asset_class": "US_EQUITY", "horizons": [2]}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(AD.LB, "build", lambda *a, **k: {"rows": []})
    res = AD.advance(TEST_CAMPAIGN)
    assert not [f for f in res["stage_failures"]
                if f["stage"] in ("evidence_velocity", "throughput_plan")], \
        res["stage_failures"]
    cdir = R46.campaign_dir(TEST_CAMPAIGN)
    assert (cdir / VL.ARTIFACT).exists()
    assert (cdir / PL.ARTIFACT).exists()
    assert res["evidence_velocity"]["information_set_state"] == \
        VL.INFO_SET_TOO_EARLY


def test_a_velocity_failure_never_stops_the_advance(sandbox, monkeypatch):
    monkeypatch.setattr(RG, "load", lambda cid=TEST_CAMPAIGN: {
        "challengers": [{"challenger_id": "c", "state": C.FORWARD_PENDING,
                         "challenger_version": "v1", "family": "F",
                         "asset_class": "US_EQUITY", "horizons": [2]}]})
    monkeypatch.setattr(EM, "emit", lambda *a, **k: {"n_appended": 0})
    monkeypatch.setattr(AD.LB, "build", lambda *a, **k: {"rows": []})

    def boom(*a, **k):
        raise RuntimeError("velocity exploded")

    monkeypatch.setattr(AD.VL, "build", boom)
    res = AD.advance(TEST_CAMPAIGN)          # must not raise
    assert any(f["stage"] == "evidence_velocity"
               for f in res["stage_failures"])
    assert res["state"] in (AD.STATE_NOTHING_DUE, AD.STATE_ADVANCED)


# =========================================================================== #
# 7. Planner and lanes
# =========================================================================== #
def test_planner_nominates_and_never_registers(sandbox, monkeypatch):
    _fresh(monkeypatch)
    body = PL.build(TEST_CAMPAIGN, registry={"registry_hash": "x"},
                    velocity={})
    assert body["nominates_but_never_registers"] is True
    assert body["allocates_no_capital"] is True
    assert body["purchases_nothing"] is True
    assert body["ranked_candidates"]
    assert body["information_set_frontier"]
    assert body["frontier_is_planning_only"] is True
    scores = [c["priority_score"] for c in body["ranked_candidates"]]
    assert scores == sorted(scores, reverse=True)


def test_intraday_probe_reports_blocked_with_the_exact_blocker(sandbox):
    body = IN.probe(TEST_CAMPAIGN, live_probe=False)
    assert body["state"] == IN.LANE_BLOCKED
    assert body["exact_blocker"]
    assert body["horizon_verdicts"]["30m"] == IN.LANE_BLOCKED
    assert body["horizon_verdicts"]["session_close"] == "SERVED_BY_DAILY_H1"
    assert body["money_spent_usd"] == 0.0
    assert body["a_blocked_lane_stops_nothing_else"] is True
    assert (R46.campaign_dir(TEST_CAMPAIGN) / IN.ARTIFACT).exists()


def test_a_blocked_intraday_lane_scores_zero_in_the_planner(sandbox):
    IN.probe(TEST_CAMPAIGN, live_probe=False)
    body = PL.build(TEST_CAMPAIGN, registry={}, velocity={})
    intr = next(c for c in body["ranked_candidates"]
                if c["candidate"] == "intraday_event_cohort")
    assert intr["feasibility"] == "DATA_BLOCKED"
    assert intr["priority_score"] == 0.0
    assert intr["detail"]


# =========================================================================== #
# 8. The read model serves velocity, plan and lanes read-only
# =========================================================================== #
def test_read_model_serves_velocity_plan_and_lane_blocks(sandbox, monkeypatch,
                                                         tmp_path):
    _fresh(monkeypatch)
    reg = _reg_stub([])
    monkeypatch.setattr(VL.LG, "predictions", lambda cid: [])
    monkeypatch.setattr(VL.LG, "outcomes", lambda cid: [])
    vel = VL.build(TEST_CAMPAIGN, reg)
    PL.build(TEST_CAMPAIGN, reg, vel)
    IN.probe(TEST_CAMPAIGN, live_probe=False)
    monkeypatch.setenv(PT.RESEARCH_ROOT_ENV, str(tmp_path / "r46root"))
    payload = PT.load_prospective_tournament(TEST_CAMPAIGN)
    assert payload["evidence_velocity"]["available"] is True
    assert payload["evidence_velocity"][
        "raw_and_effective_always_shown_together"] is True
    assert payload["throughput_plan"]["available"] is True
    assert payload["lanes"]["intraday"]["state"] == IN.LANE_BLOCKED
    assert "challengers_by_asset_class" in payload
    assert "challengers_by_information_family" in payload
    # The R46.3 artifacts are optional: none of them may become a warning.
    assert not [w for w in payload["warnings"]
                if "EVIDENCE_VELOCITY" in w or "THROUGHPUT" in w
                or "INTRADAY" in w]


def test_read_model_degrades_honestly_without_the_new_artifacts(monkeypatch,
                                                                tmp_path):
    monkeypatch.setenv(PT.RESEARCH_ROOT_ENV, str(tmp_path))
    payload = PT.load_prospective_tournament("empty_campaign")
    assert payload["evidence_velocity"]["available"] is False
    assert payload["throughput_plan"]["available"] is False


# =========================================================================== #
# 9. Governance: nothing confers capital, burden never moves
# =========================================================================== #
def test_expansion_charges_zero_new_historical_trials():
    from alpha_agent.r46 import burden as BD
    assert BD.new_trials() == 0
    assert BD.R46_NEW_TRIALS_BY_FAMILY["R46_3_EXPANSION_COHORT_SELECTION"] == 0
    assert BD.R46_NEW_TRIALS_BY_FAMILY["R46_3_ML_HYPERPARAMETERS"] == 0


def test_no_expansion_artifact_may_read_proven(sandbox, monkeypatch):
    _fresh(monkeypatch)
    reg = _reg_stub([])
    monkeypatch.setattr(VL.LG, "predictions", lambda cid: [])
    monkeypatch.setattr(VL.LG, "outcomes", lambda cid: [])
    vel = VL.build(TEST_CAMPAIGN, reg)
    plan = PL.build(TEST_CAMPAIGN, reg, vel)
    lane = IN.probe(TEST_CAMPAIGN, live_probe=False)
    for body in (vel, plan, lane):
        for key in ("creates_order", "promotes_model", "mutates_holdings"):
            assert body["safety_block"][key] is False
        assert "PROVEN_ALPHA" not in json.dumps(body)


def test_velocity_gate_arithmetic_matches_the_canonical_owner():
    from alpha_agent.r46 import evidence as EV
    assert EV.effective_independent(40, 20, 9) == 2
    assert EV.effective_independent(10, 1, 4) == 4
    assert EV.effective_independent(0, 20) == 0
