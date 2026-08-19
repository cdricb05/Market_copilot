"""Release 30 - forward-return forecasting layer: point-in-time integrity,
deterministic replay, uncertainty, evaluation protocol and safety.

The point-in-time tests are deliberately adversarial: rather than asserting that
a slice "looks right", they POISON every row after the decision date with values
that would wreck any statistic touching them, and require the answer to be
unchanged. A leak cannot survive that.
"""
from __future__ import annotations

import ast
import json
import math
from pathlib import Path

import pytest

from paper_trader.engine import return_forecast as fk

REPO = Path(__file__).resolve().parents[1]

np = pytest.importorskip("numpy")
rp = pytest.importorskip("paper_trader.alpha_agent.release30_panel")
rm = pytest.importorskip("paper_trader.alpha_agent.release30_models")
rf = pytest.importorskip("paper_trader.alpha_agent.release30_forecast_research")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #
def _panel(n_dates=400, n_syms=80, seed=3):
    rng = np.random.default_rng(seed)
    dates = np.array(["2020-01-01"], dtype="datetime64[D]")[0] + np.arange(n_dates)
    steps = rng.normal(0.0005, 0.015, size=(n_dates, n_syms))
    close = 100.0 * np.exp(np.cumsum(steps, axis=0))
    dvol = np.full((n_dates, n_syms), 5.0e8)
    member = np.ones((n_dates, n_syms), dtype=bool)
    return rp.PricePanel(dates=dates, symbols=np.array(
        ["S%02d" % i for i in range(n_syms)]), close=close, dvol=dvol,
        member=member, source={"synthetic": True})


def _cross_section(n=60, as_of="2026-08-05", seed=5):
    rng = np.random.default_rng(seed)
    names = list(rp.PRICE_FEATURE_NAMES)
    rows = []
    for i in range(n):
        rows.append({"ticker": "T%02d" % i, "adv_dollar": 5.0e8,
                     "sector": "S%d" % (i % 5),
                     "features": {k: float(rng.normal()) for k in names}})
    return {"input_schema_version": fk.INPUT_SCHEMA_VERSION,
            "as_of_date": as_of, "feature_names": names, "rows": rows,
            "point_in_time_status": fk.PIT_OK,
            "generated_at": "2026-08-06T00:00:00+00:00"}


def _artifact(feature_names, horizons=(5, 20, 60), seed=7):
    rng = np.random.default_rng(seed)
    hz = {}
    for h in horizons:
        hz[str(h)] = {
            "horizon_sessions": h,
            "model": {"kind": rm.KIND_ENSEMBLE, "members": [
                {"model_id": "ridge", "weight": 0.6,
                 "spec": {"kind": rm.KIND_LINEAR,
                          "coef": [float(v) for v in rng.normal(size=len(feature_names))]}},
                {"model_id": "blend", "weight": 0.4,
                 "spec": rm.rank_blend_spec({feature_names[0]: 1.0})},
            ]},
            "weights": {"ridge": 0.6, "blend": 0.4},
            "weighting_method": "TEST",
            "calibration": {"state": "CALIBRATED", "slope": 0.004,
                            "residual_sigma": 0.09, "residual_q05": -0.15,
                            "basis": "TEST", "n_rows": 1000},
            "training_cutoff": "2025-01-01",
        }
    art = {"contract": rm.MODEL_CONTRACT, "feature_names": list(feature_names),
           "feature_transform": fk.FEATURE_TRANSFORM,
           "target": fk.TARGET_QUANTITY, "horizons": hz,
           "automatic_promotion_allowed": False}
    art["model_spec_hash"] = rp.sha(art)
    return art


# =========================================================================== #
# POINT IN TIME
# =========================================================================== #
def test_01_features_ignore_every_future_row():
    """Poison every session after t; each feature must be bit-identical."""
    p = _panel()
    t = 300
    before = rp.price_features_at(p, t)
    p2 = _panel()
    p2.close[t + 1:] = 1.0e9
    p2.dvol[t + 1:] = 0.0
    p2.member[t + 1:] = False
    after = rp.price_features_at(p2, t)
    for name in rp.PRICE_FEATURE_NAMES:
        assert np.allclose(before[name], after[name], equal_nan=True), name


def test_02_features_ignore_future_membership():
    p = _panel()
    t = 300
    before = rp.price_features_at(p, t)
    p2 = _panel()
    p2.member[t + 1:, :] = False
    assert np.allclose(before["beta_252"], rp.price_features_at(p2, t)["beta_252"],
                       equal_nan=True)


def test_03_label_uses_only_the_forward_window():
    p = _panel()
    t, h = 300, 20
    fwd, _ = rp.forward_returns_at(p, t, h)
    p2 = _panel()
    p2.close[t + h + 1:] = 1.0e9
    fwd2, _ = rp.forward_returns_at(p2, t, h)
    assert np.allclose(fwd, fwd2, equal_nan=True)


def test_04_delisted_name_is_labelled_to_last_close_not_dropped():
    """A name that stops trading inside the window keeps a label. Dropping it
    would put survivorship back into the LABEL."""
    p = _panel()
    t, h = 300, 20
    p.close[t + 6:, 0] = np.nan
    fwd, truncated = rp.forward_returns_at(p, t, h)
    assert np.isfinite(fwd[0]), "a delisted name must still carry a label"
    assert bool(truncated[0]) is True
    expected = p.close[t + 5, 0] / p.close[t, 0] - 1.0
    assert fwd[0] == pytest.approx(expected)
    assert not truncated[1]


def test_05_rank_normalisation_is_per_date_only():
    """The transform must be invariant to any other date's distribution."""
    a = rf.rank_normalise(np.array([1.0, 2.0, 3.0, 4.0]))
    b = rf.rank_normalise(np.array([1.0, 2.0, 3.0, 4.0]) * 1000.0 + 5.0)
    assert np.allclose(a, b)


def test_06_missing_feature_is_neutral_not_extreme():
    v = rf.rank_normalise(np.array([1.0, np.nan, 3.0]))
    assert v[1] == 0.0


def test_07_kernel_rank_transform_matches_research_transform():
    """The stdlib operational transform and the numpy research transform must
    agree exactly, or the frozen coefficients mean nothing."""
    rng = np.random.default_rng(11)
    vals = rng.normal(size=40)
    a = np.array(fk.rank_normalise(list(vals)))
    b = rf.rank_normalise(vals)
    assert np.allclose(a, b, atol=1e-12)


def test_08_fundamentals_use_the_released_reporting_lag_policy():
    s24 = pytest.importorskip("paper_trader.alpha_agent.stage24_pit_fundamental")
    assert s24.pit_as_of("2020-06-30") == "2020-06-28"
    assert s24.REPORTING_LAG_DAYS == 2
    src = (REPO / "alpha_agent" / "release30_forecast_research.py").read_text(
        encoding="utf-8")
    assert "pit_as_of(" in src, "the research lane must inherit the lag policy"
    assert "_shift_days" not in src, "no private copy of the date arithmetic"


def test_09_sector_is_not_a_historical_feature():
    assert "sector" not in rp.ALL_FEATURE_NAMES
    assert not any("sector" in n for n in rp.ALL_FEATURE_NAMES)


# =========================================================================== #
# WALK-FORWARD PROTOCOL
# =========================================================================== #
def test_10_embargo_separates_train_validation_and_test():
    for h in (5, 20, 60):
        e = rf.embargo_dates(h)
        assert e == math.ceil(h / rp.STEP_DAYS)
        for f in rf.folds(300, h):
            assert f["valid"][0] - f["train"][1] == e
            assert f["test"][0] - f["valid"][1] == e


def test_11_blocks_are_ordered_and_never_overlap():
    for f in rf.folds(300, 20):
        assert f["train"][1] <= f["valid"][0] < f["valid"][1] <= f["test"][0]
        assert f["test"][0] < f["test"][1]


def test_12_test_block_is_strictly_later_than_training():
    for f in rf.folds(300, 60):
        assert f["test"][0] > f["train"][1]


def test_13_newey_west_discounts_overlapping_observations():
    """Overlapping per-date statistics must not be counted as independent."""
    rng = np.random.default_rng(4)
    base = rng.normal(0.02, 0.05, size=60)
    x = np.repeat(base, 3)[:60]          # heavily autocorrelated
    naive = float(x.mean() / (x.std(ddof=1) / math.sqrt(x.size)))
    adjusted = rf.newey_west_t(x, 2)
    assert abs(adjusted) < abs(naive)


def test_14_book_simulation_charges_the_canonical_cost_rate():
    desk = pytest.importorskip("paper_trader.api.paper_trading_desk")
    assert rf.COST_BPS_PER_SIDE == desk.COST_BPS_PER_SIDE
    assert rf.COST_RATE_PER_SIDE == pytest.approx(desk.COST_RATE_PER_SIDE)


def _series(mean, n=60, sd=0.04, seed=1):
    """A realistic per-date IC series. A CONSTANT series has no computable
    t-statistic, and the weighting fails closed on one by design."""
    rng = np.random.default_rng(seed)
    return list(mean + rng.normal(0.0, sd, size=n))


def test_15_ensemble_weights_come_only_from_validation_evidence():
    roles = {"a": "CANDIDATE", "b": "CANDIDATE", "bench": "BENCHMARK"}
    pooled = {"a": _series(0.05, seed=1), "b": _series(0.004, seed=2),
              "bench": _series(0.9, seed=3)}
    ens = rf.ensemble_from_validation(pooled, roles, 20)
    assert "bench" not in ens["weights"], "a benchmark is measured, not blended"
    assert ens["weights"]["a"] > ens["weights"]["b"]
    assert pytest.approx(1.0) == sum(ens["weights"].values())


def test_16_a_negative_out_of_sample_component_gets_exactly_zero_weight():
    roles = {"good": "CANDIDATE", "bad": "CANDIDATE"}
    pooled = {"good": _series(0.04, seed=4), "bad": _series(-0.04, seed=5)}
    ens = rf.ensemble_from_validation(pooled, roles, 20)
    assert ens["weights"]["bad"] == 0.0
    assert "bad" in ens["zeroed"]


def test_17_shrinkage_is_continuous_and_reliability_weighted():
    assert rf.shrunk_ic(0.05, 0.0) == 0.0
    assert rf.shrunk_ic(0.05, 1.0) == pytest.approx(0.05 * 0.5)
    assert rf.shrunk_ic(0.05, 2.0) == pytest.approx(0.05 * 0.8)
    assert rf.shrunk_ic(-0.05, 10.0) == 0.0


def test_18_weights_are_never_hand_picked_or_assumed_equal():
    roles = {"a": "CANDIDATE", "b": "CANDIDATE", "c": "CANDIDATE"}
    pooled = {"a": _series(0.06, seed=6), "b": _series(0.03, seed=7),
              "c": _series(0.01, seed=8)}
    w = rf.ensemble_from_validation(pooled, roles, 20)["weights"]
    assert len({round(v, 6) for v in w.values()}) == 3


def test_19_risk_prices_are_derived_from_validation_not_chosen():
    src = (REPO / "alpha_agent" / "release30_forecast_research.py").read_text(
        encoding="utf-8")
    assert "def calibrate_risk_prices" in src
    assert "WALK_FORWARD_VALIDATION_BLOCKS" in src
    assert "mean / variance of the validation book" in src


# =========================================================================== #
# FORECASTS
# =========================================================================== #
def test_20_forecast_is_multi_horizon_and_exposes_uncertainty():
    ic = _cross_section()
    art = _artifact(ic["feature_names"])
    out = fk.build_forecast(cross_section=ic, artifact=art)
    assert out["horizons"] == [5, 20, 60]
    for h in ("5", "20", "60"):
        rows = out["by_horizon"][h]["forecasts"]
        assert len(rows) == len(ic["rows"])
        for r in rows:
            for key in ("expected_return", "expected_excess_return",
                        "forecast_uncertainty", "downside_return_q05", "rank"):
                assert r[key] is not None
            assert r["forecast_uncertainty"] > 0
            assert r["downside_return_q05"] < r["expected_excess_return"]


def test_21_replay_is_deterministic_including_every_hash():
    ic, art = _cross_section(), None
    art = _artifact(ic["feature_names"])
    a = fk.build_forecast(cross_section=ic, artifact=art)
    b = fk.build_forecast(cross_section=ic, artifact=art)
    for key in ("model_spec_hash", "feature_snapshot_hash", "by_horizon"):
        assert a[key] == b[key]


def test_22_feature_snapshot_hash_tracks_inputs_not_timestamps():
    ic = _cross_section()
    other = json.loads(json.dumps(ic))
    other["generated_at"] = "2099-01-01T00:00:00+00:00"
    assert fk.feature_snapshot_hash(ic) == fk.feature_snapshot_hash(other)
    changed = json.loads(json.dumps(ic))
    changed["rows"][0]["features"][ic["feature_names"][0]] += 1.0
    assert fk.feature_snapshot_hash(ic) != fk.feature_snapshot_hash(changed)


def test_23_target_is_a_return_and_the_market_level_is_not_forecast():
    out = fk.build_forecast(cross_section=_cross_section(),
                            artifact=_artifact(rp.PRICE_FEATURE_NAMES))
    assert out["target_quantity"] == "FORWARD_EXCESS_RETURN_VS_CROSS_SECTIONAL_MEAN"
    assert out["market_baseline"] == 0.0
    assert out["market_baseline_policy"] == "MARKET_LEVEL_NOT_FORECAST"
    assert "price" not in out["target_quantity"].lower()


def test_24_a_feature_the_model_needs_but_the_input_lacks_blocks():
    ic = _cross_section()
    art = _artifact(list(ic["feature_names"]) + ["a_feature_not_emitted"])
    out = fk.build_forecast(cross_section=ic, artifact=art)
    assert out["state"] == fk.STATE_BLOCKED
    assert any(b["code"] == "FEATURE_SET_MISMATCH" for b in out["blockers"])


def test_25_an_artifact_declaring_auto_promotion_is_refused():
    art = _artifact(rp.PRICE_FEATURE_NAMES)
    art["automatic_promotion_allowed"] = True
    chk = fk.validate_artifact(art)
    assert not chk["ok"]
    assert any(r["code"] == "AUTOMATIC_PROMOTION_DECLARED" for r in chk["reasons"])


def test_26_an_uncalibrated_horizon_is_refused():
    art = _artifact(rp.PRICE_FEATURE_NAMES)
    art["horizons"]["20"]["calibration"] = {"state": "UNCALIBRATED"}
    chk = fk.validate_artifact(art)
    assert not chk["ok"]
    assert any(r["code"] == "HORIZON_NOT_CALIBRATED" for r in chk["reasons"])


def test_27_a_point_in_time_violation_blocks_the_forecast():
    ic = _cross_section()
    ic["point_in_time_status"] = fk.PIT_VIOLATED
    out = fk.build_forecast(cross_section=ic,
                            artifact=_artifact(ic["feature_names"]))
    assert out["state"] == fk.STATE_BLOCKED
    assert any(b["code"] == "POINT_IN_TIME_VIOLATED" for b in out["blockers"])


def test_28_ensemble_members_are_standardised_before_weighting():
    """A member must not gain influence by predicting on a larger scale."""
    ic = _cross_section()
    names = ic["feature_names"]
    small = {"kind": rm.KIND_LINEAR, "coef": [1.0] + [0.0] * (len(names) - 1)}
    huge = {"kind": rm.KIND_LINEAR,
            "coef": [0.0, 1000.0] + [0.0] * (len(names) - 2)}
    art = _artifact(names)
    art["horizons"]["20"]["model"] = {"kind": rm.KIND_ENSEMBLE, "members": [
        {"model_id": "small", "weight": 0.5, "spec": small},
        {"model_id": "huge", "weight": 0.5, "spec": huge}]}
    matrix = [[r["features"][n] for n in names] for r in ic["rows"]]
    norm = {n: fk.rank_normalise([r["features"][n] for r in ic["rows"]])
            for n in names}
    matrix = [[norm[n][i] for n in names] for i in range(len(ic["rows"]))]
    a = fk.standardise(fk.apply_model(small, matrix, names))
    b = fk.standardise(fk.apply_model(huge, matrix, names))
    blend = fk.apply_model(art["horizons"]["20"]["model"], matrix, names)
    for i in range(len(blend)):
        assert blend[i] == pytest.approx(0.5 * a[i] + 0.5 * b[i], abs=1e-9)


def test_29_uncertainty_combines_measured_dispersion_and_disagreement():
    ic = _cross_section()
    art = _artifact(ic["feature_names"])
    out = fk.build_forecast(cross_section=ic, artifact=art)
    sigma = art["horizons"]["20"]["calibration"]["residual_sigma"]
    for r in out["by_horizon"]["20"]["forecasts"]:
        assert r["forecast_uncertainty"] >= sigma - 1e-9
        assert r["member_disagreement"] is not None


def test_30_stdlib_kernel_reproduces_the_numpy_learners():
    """The operational kernel and the research learners must agree, or a frozen
    artifact means something different in production than it did in research."""
    rng = np.random.default_rng(21)
    X = rng.normal(size=(300, 6))
    y = X[:, 0] * 0.3 + rng.normal(size=300) * 0.5
    names = ["f%d" % i for i in range(6)]
    for spec in (rm.fit_ridge(X, y, alpha=10.0),
                 rm.fit_gbrt(X, y, n_trees=12, max_depth=2,
                             learning_rate=0.1, seed=1),
                 rm.fit_extra_trees(X, y, n_trees=8, max_depth=3,
                                    min_leaf=20, seed=1)):
        expected = rm.predict(spec, X[:20], names)
        got = fk.apply_model(json.loads(json.dumps(spec)),
                             [list(row) for row in X[:20]], names)
        assert np.allclose(expected, got, atol=1e-9), spec["kind"]


def test_31_learners_are_deterministic_for_a_fixed_seed():
    rng = np.random.default_rng(2)
    X = rng.normal(size=(200, 5))
    y = rng.normal(size=200)
    a = rm.fit_gbrt(X, y, n_trees=8, max_depth=2, learning_rate=0.1, seed=9)
    b = rm.fit_gbrt(X, y, n_trees=8, max_depth=2, learning_rate=0.1, seed=9)
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)


# =========================================================================== #
# SAFETY / GOVERNANCE
# =========================================================================== #
def test_32_activation_fails_closed():
    api = pytest.importorskip("paper_trader.api.return_forecast")
    act = api.activation_state(evidence=REPO / "does" / "not" / "exist")
    assert act["state"] == api.ACTIVATION_NOT_ACTIVATED
    assert act["automatic_promotion_allowed"] is False


def test_33_no_code_path_can_write_an_activation_record():
    src = (REPO / "api" / "return_forecast.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = getattr(fn, "attr", getattr(fn, "id", ""))
            if name == "_atomic_write_json":
                arg = ast.unparse(node.args[0]) if node.args else ""
                assert "ACTIVATION_FILE" not in arg, (
                    "activation must be written by a human, never by code")


def test_34_reading_a_forecast_writes_nothing():
    src = (REPO / "api" / "return_forecast.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    fns = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    for name in ("build", "load_return_forecast", "summary", "activation_state"):
        body = ast.unparse(fns[name])
        for forbidden in ("_atomic_write_json", "write_text", "mkdir", "replace("):
            assert forbidden not in body, "%s must not write (%s)" % (name, forbidden)


def test_35_forecast_snapshot_is_immutable_and_first_write_wins(tmp_path):
    api = pytest.importorskip("paper_trader.api.return_forecast")
    out = fk.build_forecast(cross_section=_cross_section(),
                            artifact=_artifact(rp.PRICE_FEATURE_NAMES))
    out["eligible_market_date"] = "2026-08-05"
    first = api.capture_forecast_snapshot(forecast=out, evidence=tmp_path)
    assert first["state"] == "CAPTURED" and first["immutable"] is True
    again = api.capture_forecast_snapshot(forecast=out, evidence=tmp_path)
    assert again["state"] == "ALREADY_CAPTURED"
    doc = json.loads(Path(first["path"]).read_text(encoding="utf-8"))
    assert doc["outcomes_appended"] is False
    assert doc["append_only"] is True


def test_36_the_kernel_is_pure_stdlib_and_does_no_io():
    src = (REPO / "engine" / "return_forecast.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    imported = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported |= {a.name.split(".")[0] for a in node.names}
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".")[0])
    assert not (imported & {"numpy", "pandas", "requests", "sqlite3", "urllib"})
    for forbidden in ("open(", "Path(", "requests.", "os.environ"):
        assert forbidden not in src, forbidden


def test_37_forecast_creates_no_order_decision_or_mutation():
    out = fk.build_forecast(cross_section=_cross_section(),
                            artifact=_artifact(rp.PRICE_FEATURE_NAMES))
    s = out["safety"]
    assert s["creates_orders"] is False
    assert s["creates_decisions"] is False
    assert s["mutates_holdings"] is False
    assert s["promotes_models"] is False
    assert fk.AUTOMATIC_PROMOTION_ALLOWED is False


def test_38_research_lane_never_imports_the_operational_api():
    for name in ("release30_panel", "release30_models",
                 "release30_forecast_research", "release30_forecast_emitter"):
        src = (REPO / "alpha_agent" / (name + ".py")).read_text(encoding="utf-8")
        assert "from paper_trader.api" not in src, name
        assert "import paper_trader.api" not in src, name


def test_39_the_emitter_declares_staleness_instead_of_extrapolating():
    em = pytest.importorskip("paper_trader.alpha_agent.release30_forecast_emitter")
    p = _panel()
    out = em.emit_cross_section(as_of_date="2021-12-31", panel=p,
                                tickers=[str(s) for s in p.symbols])
    assert out["as_of_date"] <= "2021-12-31"
    assert out["requested_eligible_market_date"] == "2021-12-31"
    assert "feature_panel_behind_eligible_session" in out
    assert out["point_in_time_status"] == fk.PIT_OK


def test_40_horizons_are_declared_once_and_shared():
    assert fk.HORIZONS == rp.HORIZONS == (5, 20, 60)
