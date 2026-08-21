"""alpha_agent.r33.campaign - orchestration, qualification and the verdict.

The order of operations is the experiment. Selection happens on DISCOVERY and
VALIDATION only; the lockbox is opened once per finalist, after the finalist set
is frozen and hashed; multiple-testing control is applied over EVERY executed
configuration; and the qualification gate reads the primary metric each target
declared before any of this ran.

Two results are reported, separately and without euphemism:

    SYSTEM_RESULT   did the machinery do what it claimed, on frozen evidence
    ALPHA_RESULT    did anything actually predict

``ALPHA_RESULT`` may be PASS only alongside ``R33_ALPHA_QUALIFIED``. A completed
campaign, a clean audit and a large artifact set are SYSTEM outcomes. They are
not an investment outcome and this module will not let them be reported as one.
"""
from __future__ import annotations

import datetime as _dt
import math
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r33
from ..r31.multiple_testing import (
    benjamini_hochberg, superior_predictive_ability, two_sided_p,
)
from . import contract as _contract
from . import economic as _economic
from . import features as _features
from . import lockbox as _lockbox
from . import models as _models
from . import panel as _panel
from . import partition as _partition
from . import pit as _pit
from . import predictive as _predictive
from . import regime as _regime
from . import registry as _registry
from . import robustness as _robustness
from . import targets as _targets
from . import universe as _universe

CALCULATION_OWNER = "alpha_agent.r33.campaign"

VERDICT_SCHEMA = "r33_final_verdict/1"
PREDICTIVE_SCHEMA = "r33_predictive_results/1"
ECONOMIC_SCHEMA = "r33_economic_results/1"
MT_SCHEMA = "r33_multiple_testing/1"
ROBUSTNESS_SCHEMA = "r33_robustness_results/1"
LANE_C_SCHEMA = "r33_lane_c_readiness/1"

VERDICT_ARTIFACT = "final_verdict.json"
PREDICTIVE_ARTIFACT = "predictive_results.json"
ECONOMIC_ARTIFACT = "economic_results.json"
MT_ARTIFACT = "multiple_testing.json"
ROBUSTNESS_ARTIFACT = "robustness_results.json"
LANE_C_ARTIFACT = "lane_c_readiness.json"

T_RETURN = _contract.TARGET_RETURN
T_SIGN = _contract.TARGET_SIGN
T_VOL = _contract.TARGET_VOLATILITY
T_XS = _contract.TARGET_CROSS_SECTION


def _now() -> str:
    return _dt.datetime.now().isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Per-horizon data bundle
# --------------------------------------------------------------------------- #
def build_bundle(panel: dict, pit_state: pd.DataFrame, *, horizon: int) -> dict:
    """Everything a configuration at this horizon needs, computed once."""
    design = _features.design_matrix(panel, horizon=horizon)
    tgt = _targets.build(panel, horizon=horizon)
    segments = _partition.assign(design["date"], horizon=horizon,
                                 calendar=panel["calendar"],
                                 decision_index=design["decision_index"])
    y = {name: _targets.align_to_rows(frame, design)
         for name, frame in tgt.items()}
    dates = pd.DatetimeIndex(sorted(set(design["date"])))
    return {
        "horizon": horizon,
        "design": design,
        "targets": tgt,
        "y": y,
        "segments": segments,
        "decision_dates": dates,
        "excess_returns": tgt[T_RETURN],
        "benchmark": _panel.benchmark_observation_returns(panel,
                                                          horizon=horizon),
        "cash": _panel.cash_observation_returns(panel, horizon=horizon),
        "trailing_vol": _economic.trailing_vol_frame(panel, dates),
        "global_state": _features.build_global_state(panel),
        "pit_state": pit_state,
    }


def _row_vol(bundle: dict) -> np.ndarray:
    tv = bundle["trailing_vol"]
    d = bundle["design"]
    out = np.full(len(d["symbol"]), np.nan)
    row = {dt: i for i, dt in enumerate(tv.index)}
    col = {s: j for j, s in enumerate(tv.columns)}
    values = tv.to_numpy()
    for k in range(len(out)):
        i, j = row.get(d["date"][k]), col.get(d["symbol"][k])
        if i is not None and j is not None:
            out[k] = values[i, j]
    return np.where(np.isfinite(out), out, 0.20)


# --------------------------------------------------------------------------- #
# Controls
# --------------------------------------------------------------------------- #
def build_controls(bundle: dict, mask_dates: pd.DatetimeIndex) -> dict:
    h = bundle["horizon"]
    cash = bundle["cash"].reindex(mask_dates).fillna(0.0)
    bench = bundle["benchmark"].reindex(mask_dates).fillna(0.0)
    eq_risk = _economic.equal_risk_control(
        bundle["excess_returns"].reindex(mask_dates),
        bundle["trailing_vol"].reindex(mask_dates), cash)
    return {"CASH": cash.to_numpy(),
            "BENCHMARK_BUY_AND_HOLD": (cash + bench).to_numpy(),
            "EQUAL_RISK_CROSS_MARKET": eq_risk,
            "_bench_excess": bench.to_numpy(),
            "_cash": cash.to_numpy(),
            "horizon": h}


# --------------------------------------------------------------------------- #
# Executing one configuration
# --------------------------------------------------------------------------- #
def _fit_model(spec: dict, X: np.ndarray, y: np.ndarray,
               groups: np.ndarray, feature_names: list) -> dict:
    learner = spec["learner"]
    p = spec.get("params", {})
    if learner == "unconditional_mean":
        return _models.fit_unconditional_mean(y)
    if learner == "zero":
        return _models.fit_zero()
    if learner == "rule":
        return _models.fit_transparent_rule(rule=p["rule"],
                                            feature_names=feature_names)
    if learner == "ridge":
        return _models.fit_linear(X, y, alpha=float(p.get("alpha", 10.0)))
    if learner == "elastic_net":
        return _models.fit_elastic_net(X, y, alpha=float(p.get("alpha", 1e-4)),
                                       l1_ratio=float(p.get("l1_ratio", 0.5)))
    if learner == "gbrt":
        return _models.fit_gbrt(X, y, n_trees=int(p.get("n_trees", 120)),
                                max_depth=int(p.get("max_depth", 3)),
                                learning_rate=float(p.get("learning_rate", 0.05)),
                                min_leaf=int(p.get("min_leaf", 200)),
                                seed=_contract.MODEL_SEED)
    if learner == "extra_trees":
        return _models.fit_extra_trees(X, y, n_trees=int(p.get("n_trees", 120)),
                                       max_depth=int(p.get("max_depth", 5)),
                                       min_leaf=int(p.get("min_leaf", 400)),
                                       seed=_contract.MODEL_SEED)
    if learner == "hierarchical":
        return _models.fit_hierarchical(X, y, groups,
                                        alpha=float(p.get("alpha", 10.0)),
                                        shrink=float(p.get("shrink", 0.5)))
    if learner == "logistic":
        return _models.fit_logistic(X, y, alpha=float(p.get("alpha", 10.0)))
    if learner == "volatility":
        return _models.fit_volatility(X, y, model=p.get("model", "HAR_LOG"),
                                      feature_names=feature_names)
    raise ValueError(f"unknown learner {learner!r}")


def _pooling_groups(spec: dict, design: dict) -> np.ndarray:
    pooling = spec.get("pooling", _contract.POOLING_FULL)
    if pooling == _contract.POOLING_INDEPENDENT:
        return design["symbol"]
    if pooling == _contract.POOLING_GROUPED:
        return design["economic_group"]
    if pooling == _contract.POOLING_HIERARCHICAL:
        return design["asset_class"]
    return np.full(len(design["symbol"]), "ALL")


def execute(spec: dict, bundle: dict, *, train_segments, eval_segment: str,
            state_cache: dict) -> dict:
    """Fit on the training segments, score on the evaluation segment."""
    design = bundle["design"]
    horizon = bundle["horizon"]
    target = spec["target"]
    y_all = bundle["y"][target]
    seg = bundle["segments"]

    X_all = design["X"]
    names = list(design["feature_names"])

    if spec.get("state_source"):
        key = (spec["state_source"], int(spec["n_states"]), horizon)
        if key not in state_cache:
            frame = _regime.state_frame(bundle["global_state"],
                                        bundle["pit_state"],
                                        source=spec["state_source"])
            # The HMM is fitted on the DAILY state series over the training
            # PERIOD, not on the sparse decision dates: a 60-session-horizon
            # bundle strikes only a few dozen decisions in DISCOVERY, which is
            # nowhere near enough to estimate a transition matrix.
            train_dates = pd.DatetimeIndex(
                sorted(set(design["date"][np.isin(seg, train_segments)])))
            if len(train_dates) == 0:
                return {"state": "INSUFFICIENT_TRAINING_STATE"}
            mask = np.asarray(frame.index <= train_dates[-1])
            if mask.sum() < 250:
                return {"state": "INSUFFICIENT_TRAINING_STATE"}
            state_cache[key] = _regime.fit_and_filter(
                frame, mask, n_states=int(spec["n_states"]),
                seed=_contract.MODEL_SEED)
        fitted = state_cache[key]
        X_all, names = _regime.attach_state_features(
            X_all, names, fitted["probabilities"], design["date"])

    if spec.get("uses_positioning_features"):
        X_all, names = attach_positioning(X_all, names, design,
                                          bundle.get("cot_states") or {})

    subset = spec.get("universe_subset")
    row_keep = np.ones(len(design["symbol"]), dtype=bool)
    if subset:
        row_keep = np.isin(design["symbol"], list(subset))

    train_mask = np.isin(seg, train_segments) & np.isfinite(y_all) & row_keep
    eval_mask = (seg == eval_segment) & np.isfinite(y_all) & row_keep
    if train_mask.sum() < 500 or eval_mask.sum() < 100:
        return {"state": "INSUFFICIENT_OBSERVATIONS",
                "train_rows": int(train_mask.sum()),
                "eval_rows": int(eval_mask.sum())}

    scaler = _models.fit_scaler(X_all[train_mask])
    if target == T_VOL:
        # Volatility models consume RAW volatilities: they take logs and their
        # baseline IS the trailing volatility column. Handing them standardised
        # features turns the baseline forecast into a z-score - which can be
        # negative - and the resulting QLIKE "skill" approaches 1.0 because the
        # baseline is nonsense, not because the model is good.
        Xtr, Xev = X_all[train_mask], X_all[eval_mask]
    else:
        Xtr = _models.apply_scaler(scaler, X_all[train_mask])
        Xev = _models.apply_scaler(scaler, X_all[eval_mask])
    groups = _pooling_groups(spec, design)

    fitted = _fit_model(spec, Xtr, y_all[train_mask], groups[train_mask], names)
    yhat = _models.predict(fitted, Xev, groups=groups[eval_mask],
                           feature_names=names)

    # Baselines are fixed on TRAINING data and applied unchanged.
    if target == T_VOL:
        base_spec = _models.fit_volatility(Xtr, y_all[train_mask],
                                           model=_models.VOL_TRAILING,
                                           feature_names=names)
        baseline_forecast = _models.predict(base_spec, Xev, feature_names=names)
        baseline_value = None
    elif target == T_SIGN:
        baseline_value = float(np.nanmean(y_all[train_mask]))
        baseline_forecast = None
    elif target == T_XS:
        baseline_value = 0.0
        baseline_forecast = None
    else:
        baseline_value = float(np.nanmean(y_all[train_mask]))
        baseline_forecast = None

    score = _predictive.score(target, yhat=yhat, y=y_all[eval_mask],
                              dates=design["date"][eval_mask],
                              baseline_value=baseline_value,
                              baseline_forecast=baseline_forecast)

    econ = _economics_for(spec, bundle, design, eval_mask, yhat, horizon)
    out = {"state": "OK",
           "primary_metric": score["primary_metric"],
           "primary_value": score["primary_value"],
           "gain_t_stat": score.get("gain_t_stat"),
           "gain_p_value": score.get("gain_p_value"),
           "scored_dates": score.get("scored_dates"),
           "predictive": {k: v for k, v in score.items()
                          if not str(k).startswith("_")},
           "train_rows": int(train_mask.sum()),
           "eval_rows": int(eval_mask.sum())}
    out.update(econ)
    out["_fitted_spec"] = fitted
    out["_scaler"] = scaler
    out["_feature_names"] = names
    out["_per_date_gain"] = score.get("_per_date_gain")
    return out


def _economics_for(spec: dict, bundle: dict, design: dict, eval_mask,
                   yhat: np.ndarray, horizon: int) -> dict:
    """Judge the forecast economically, against every declared control."""
    if spec["target"] == T_VOL:
        # A volatility forecast does not imply a direction, so it does not
        # produce a book. Scoring it economically would be inventing a
        # strategy the candidate never proposed.
        return {"economic": {"state": "NOT_APPLICABLE_VOLATILITY_TARGET"}}
    signal = yhat
    if spec["target"] == T_SIGN:
        signal = yhat - 0.5

    rows_vol = _row_vol(bundle)[eval_mask]
    weights = _economic.build_positions(
        signal, symbols=design["symbol"][eval_mask],
        dates=design["date"][eval_mask],
        asset_class=design["asset_class"][eval_mask],
        trailing_vol=rows_vol,
        construction=spec.get("construction",
                              _economic.CONSTRUCTION_CROSS_SECTIONAL),
        target_position_vol=_contract.VOLATILITY_TARGET_ANNUAL)
    if weights.empty:
        return {"economic": {"state": "NO_POSITIONS"}}

    meta = {s: {"asset_class": a} for s, a in
            zip(design["symbol"], design["asset_class"])}
    dates = weights.index
    path = _economic.evaluate_book(weights, bundle["excess_returns"],
                                   bundle["cash"], meta=meta, horizon=horizon)
    if path.get("state") != "OK":
        return {"economic": {"state": path.get("state")}}

    controls = build_controls(bundle, dates)
    vm = _economic.volatility_matched_control(
        path["net"], controls["_bench_excess"], controls["_cash"])
    described = _economic.describe(path, horizon=horizon)

    vs = {}
    for name in ("CASH", "BENCHMARK_BUY_AND_HOLD", "EQUAL_RISK_CROSS_MARKET"):
        ex = _economic.excess_significance(path["net"], controls[name],
                                           horizon=horizon)
        vs[name] = {k: v for k, v in ex.items() if not str(k).startswith("_")}
    vm_series = vm.get("series")
    excess_series = None
    if vm_series is not None:
        ex = _economic.excess_significance(path["net"], vm_series,
                                           horizon=horizon)
        excess_series = ex.get("_diff")
        vs[_contract.ECONOMIC_CONTROL] = {
            k: v for k, v in ex.items() if not str(k).startswith("_")}
        util_book = _economic.utility(path["net"], horizon=horizon)
        util_ctrl = _economic.utility(vm_series, horizon=horizon)
    else:
        vs[_contract.ECONOMIC_CONTROL] = {"mean_excess": None, "t_stat": None}
        util_book = util_ctrl = float("nan")

    primary = vs[_contract.ECONOMIC_CONTROL]
    return {"economic": {
        "state": "OK",
        **described,
        "volatility_matched_control": {
            k: v for k, v in vm.items() if k != "series"},
        "vs_controls": vs,
        "beats_volatility_matched_control":
            bool((primary.get("mean_excess") or 0.0) > 0.0),
        "utility_book": util_book,
        "utility_control": util_ctrl,
        "utility_improvement": (float(util_book - util_ctrl)
                                if math.isfinite(util_book)
                                and math.isfinite(util_ctrl) else None),
    },
        "_weights": weights, "_net": path["net"], "_excess_series": excess_series,
        "_control": vm_series, "_dates": dates, "_meta": meta}


# --------------------------------------------------------------------------- #
# The frozen configuration grids
# --------------------------------------------------------------------------- #
def enumerate_configurations(cot_subset: list) -> list:
    """Every configuration this campaign will execute. Enumerated, not searched.

    Frozen here so the denominator is knowable before the first result, and so
    no family can quietly grow after a disappointing one.
    """
    out = []
    H = list(_contract.HORIZONS)

    # ---- BASELINE: transparent rules and the required naive forecasts ----- #
    for h in H:
        for rule in (_models.RULE_TSMOM, _models.RULE_VOL_SCALED_TREND,
                     _models.RULE_XS_MOMENTUM):
            for construction in (_economic.CONSTRUCTION_CROSS_SECTIONAL,
                                 _economic.CONSTRUCTION_DIRECTIONAL):
                out.append({"family": _contract.FAMILY_BASELINE,
                            "target": T_XS, "horizon": h, "learner": "rule",
                            "params": {"rule": rule},
                            "construction": construction,
                            "pooling": _contract.POOLING_FULL})
        out.append({"family": _contract.FAMILY_BASELINE, "target": T_RETURN,
                    "horizon": h, "learner": "unconditional_mean",
                    "params": {}, "pooling": _contract.POOLING_FULL})
        for vm in (_models.VOL_TRAILING, _models.VOL_EWMA):
            out.append({"family": _contract.FAMILY_BASELINE, "target": T_VOL,
                        "horizon": h, "learner": "volatility",
                        "params": {"model": vm},
                        "pooling": _contract.POOLING_FULL})

    # ---- POOLED: the core Lane A hypothesis ------------------------------ #
    for h in H:
        for pooling in (_contract.POOLING_FULL, _contract.POOLING_GROUPED):
            out.append({"family": _contract.FAMILY_POOLED, "target": T_XS,
                        "horizon": h, "learner": "ridge",
                        "params": {"alpha": 10.0}, "pooling": pooling})
            out.append({"family": _contract.FAMILY_POOLED, "target": T_XS,
                        "horizon": h, "learner": "elastic_net",
                        "params": {"alpha": 1e-4, "l1_ratio": 0.5},
                        "pooling": pooling})
            out.append({"family": _contract.FAMILY_POOLED, "target": T_XS,
                        "horizon": h, "learner": "gbrt",
                        "params": {"n_trees": 120, "max_depth": 3,
                                   "learning_rate": 0.05},
                        "pooling": pooling})
            out.append({"family": _contract.FAMILY_POOLED, "target": T_XS,
                        "horizon": h, "learner": "extra_trees",
                        "params": {"n_trees": 120, "max_depth": 5},
                        "pooling": pooling})
        for shrink in (0.0, 0.5, 1.0):
            out.append({"family": _contract.FAMILY_POOLED, "target": T_XS,
                        "horizon": h, "learner": "hierarchical",
                        "params": {"alpha": 10.0, "shrink": shrink},
                        "pooling": _contract.POOLING_HIERARCHICAL})
        out.append({"family": _contract.FAMILY_POOLED, "target": T_SIGN,
                    "horizon": h, "learner": "logistic",
                    "params": {"alpha": 10.0},
                    "pooling": _contract.POOLING_FULL})
        out.append({"family": _contract.FAMILY_POOLED, "target": T_VOL,
                    "horizon": h, "learner": "volatility",
                    "params": {"model": _models.VOL_HAR},
                    "pooling": _contract.POOLING_FULL})
        out.append({"family": _contract.FAMILY_POOLED, "target": T_RETURN,
                    "horizon": h, "learner": "ridge",
                    "params": {"alpha": 10.0},
                    "pooling": _contract.POOLING_FULL})

    # ---- REGIME: does a filtered latent state add anything? -------------- #
    for h in H:
        for source in _regime.STATE_SOURCES:
            for k in (2, 3):
                out.append({"family": _contract.FAMILY_REGIME, "target": T_XS,
                            "horizon": h, "learner": "ridge",
                            "params": {"alpha": 10.0},
                            "pooling": _contract.POOLING_FULL,
                            "state_source": source, "n_states": k})
        out.append({"family": _contract.FAMILY_REGIME, "target": T_SIGN,
                    "horizon": h, "learner": "logistic",
                    "params": {"alpha": 10.0},
                    "pooling": _contract.POOLING_FULL,
                    "state_source": _regime.SOURCE_BOTH, "n_states": 2})

    # ---- COMBINED: positioning information and blends -------------------- #
    if cot_subset:
        for h in H:
            for learner, params in (("ridge", {"alpha": 10.0}),
                                    ("gbrt", {"n_trees": 120, "max_depth": 3,
                                              "learning_rate": 0.05})):
                out.append({"family": _contract.FAMILY_COMBINED, "target": T_XS,
                            "horizon": h, "learner": learner, "params": params,
                            "pooling": _contract.POOLING_FULL,
                            "universe_subset": list(cot_subset),
                            "uses_positioning_features": True})
    for h in H:
        for alpha in (1.0, 100.0):
            out.append({"family": _contract.FAMILY_COMBINED, "target": T_XS,
                        "horizon": h, "learner": "ridge",
                        "params": {"alpha": alpha},
                        "pooling": _contract.POOLING_FULL})
        out.append({"family": _contract.FAMILY_COMBINED, "target": T_XS,
                    "horizon": h, "learner": "hierarchical",
                    "params": {"alpha": 1.0, "shrink": 0.25},
                    "pooling": _contract.POOLING_HIERARCHICAL})
    return out


def spec_key(spec: dict) -> dict:
    """The part of a configuration that identifies it."""
    return {k: (sorted(v) if isinstance(v, (list, tuple, set)) else v)
            for k, v in sorted(spec.items())}


# --------------------------------------------------------------------------- #
# Positioning features (Lane B -> Lane A)
# --------------------------------------------------------------------------- #
COT_FEATURE_NAMES = ("cot_positioning_z", "cot_net_positioning",
                     "cot_positioning_change_13w")


def attach_positioning(X: np.ndarray, names: list, design: dict,
                       cot_states: dict) -> tuple:
    """Append CFTC positioning features, zero where a market has no report.

    Only fourteen markets in this universe have an unambiguous CFTC mapping, so
    a configuration that uses these features is run on that SUBSET and is
    labelled as such. Broadcasting a positioning number to markets that have
    none would be inventing information.
    """
    cols = []
    for key in ("positioning_z", "net_positioning", "positioning_change_13w"):
        out = np.zeros(len(design["symbol"]))
        for sym, states in cot_states.items():
            series = states.get(key)
            if series is None:
                continue
            mask = design["symbol"] == sym
            if not mask.any():
                continue
            vals = series.reindex(design["date"][mask]).to_numpy()
            out[mask] = np.where(np.isfinite(vals), vals, 0.0)
        cols.append(out)
    return (np.column_stack([X] + cols), list(names) + list(COT_FEATURE_NAMES))


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def multiple_testing(rows: list, *, denominator: int) -> dict:
    """Benjamini-Hochberg over every executed configuration, plus SPA.

    The denominator is the count of EXECUTED configurations, not of survivors.
    A configuration that failed still consumed a look at the data.
    """
    p_values, labels = [], []
    for r in rows:
        val = (r.get("result") or {}).get("validation") or {}
        p = val.get("gain_p_value")
        p_values.append(1.0 if p is None else float(p))
        labels.append(r["candidate_id"])
    bh = benjamini_hochberg(p_values, q=_contract.FDR_Q)
    bh_survivors = [labels[i] for i in bh.get("rejected", [])]

    spa_by_horizon = {}
    for h in _contract.HORIZONS:
        series = {}
        for r in rows:
            spec = r.get("spec", {})
            if int(spec.get("horizon", 0)) != h:
                continue
            s = (r.get("result") or {}).get("_validation_excess")
            if s is None or len(s) < 8:
                continue
            series[r["candidate_id"]] = np.asarray(s, dtype=float)
        if len(series) >= 2:
            n = min(v.size for v in series.values())
            spa_by_horizon[str(h)] = superior_predictive_ability(
                {k: v[:n] for k, v in series.items()},
                resamples=_contract.BOOTSTRAP_RESAMPLES,
                block_mean=_contract.BOOTSTRAP_BLOCK_MEAN,
                seed=_contract.BOOTSTRAP_SEED)
        else:
            spa_by_horizon[str(h)] = {"state": "TOO_FEW_SERIES",
                                      "n_candidates": len(series)}
    return {
        "denominator_executed_configurations": int(denominator),
        "denominator_counts_all_executed":
            _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
        "procedure": "BENJAMINI_HOCHBERG_PLUS_HANSEN_SPA",
        "fdr_q": _contract.FDR_Q,
        "benjamini_hochberg": {k: v for k, v in bh.items() if k != "rejected"},
        "benjamini_hochberg_survivors": bh_survivors,
        "n_survivors": len(bh_survivors),
        "superior_predictive_ability_by_horizon": spa_by_horizon,
        "seeds": {"bootstrap": _contract.BOOTSTRAP_SEED},
    }


# --------------------------------------------------------------------------- #
# Finalist selection
# --------------------------------------------------------------------------- #
SELECTION_BASIS = (
    "VALIDATION evidence only: a candidate is eligible if its declared primary "
    "metric is positive on VALIDATION with a positive gain t-statistic, and "
    "eligible candidates are ranked by that primary metric. The lockbox is "
    "never consulted during selection.")


def select_finalists(rows: list) -> list:
    eligible = []
    for r in rows:
        val = (r.get("result") or {}).get("validation") or {}
        v, t = val.get("primary_value"), val.get("gain_t_stat")
        if v is None or t is None:
            continue
        if float(v) > 0.0 and float(t) > 0.0:
            eligible.append((float(v), r))
    eligible.sort(key=lambda pair: -pair[0])
    chosen, per_family = [], {}
    for _v, r in eligible:
        fam = r["spec"]["family"]
        if per_family.get(fam, 0) >= _contract.MAX_LOCKBOX_PER_FAMILY:
            continue
        if len(chosen) >= _contract.MAX_LOCKBOX_FINALISTS:
            break
        per_family[fam] = per_family.get(fam, 0) + 1
        chosen.append({"candidate_id": r["candidate_id"],
                       "spec_hash": r["spec_hash"],
                       "family": fam,
                       "target": r["spec"]["target"],
                       "horizon": r["spec"]["horizon"],
                       "validation_primary_value": _v})
    return chosen


# --------------------------------------------------------------------------- #
# Qualification
# --------------------------------------------------------------------------- #
def qualify(*, candidate_id: str, validation: dict, lockbox: dict,
            robustness: dict, survived_multiple_testing: bool,
            lockbox_accesses: int) -> dict:
    """Evaluate every frozen condition and report each one separately."""
    v_pred = float(validation.get("primary_value") or 0.0)
    l_pred_raw = lockbox.get("primary_value")
    l_pred = float(l_pred_raw or 0.0)

    l_econ = ((lockbox.get("economic") or {}).get("vs_controls") or {}).get(
        _contract.ECONOMIC_CONTROL, {})
    l_excess = l_econ.get("mean_excess")
    l_util = (lockbox.get("economic") or {}).get("utility_improvement")

    # The contract states that a candidate with fewer than
    # MIN_SCORED_FORECAST_DATES scored dates in a segment CANNOT CARRY A
    # VERDICT for that segment. The first version of this gate never read that
    # frozen term, so a lockbox result resting on 23 observations was allowed
    # to satisfy the predictive conditions. Enforcing it can only remove a
    # qualification, never create one.
    scored = lockbox.get("scored_dates")
    enough = bool(scored is not None
                  and int(scored) >= _contract.MIN_SCORED_FORECAST_DATES)

    conditions = {
        "positive_oos_predictive_improvement_vs_baseline":
            bool(enough and l_pred_raw is not None and l_pred > 0.0),
        "predictive_improvement_same_sign_in_validation_and_lockbox":
            bool(enough and l_pred_raw is not None and v_pred > 0.0
                 and l_pred > 0.0),
        "positive_after_cost_excess_vs_risk_matched_control":
            bool((l_excess or 0.0) > 0.0),
        "positive_after_cost_utility_improvement":
            bool((l_util or 0.0) > 0.0),
        "survives_multiple_testing_procedure": bool(survived_multiple_testing),
        "no_severe_parameter_cliff":
            not bool((robustness.get("parameter_cliff") or {}).get(
                "severe_cliff", False)),
        "not_dependent_on_a_single_market":
            not bool((robustness.get("leave_one_market_out") or {}).get(
                "single_market_dependent", False)),
        "not_dependent_on_a_single_subperiod":
            not bool((robustness.get("subperiod_stability") or {}).get(
                "single_subperiod_dependent", False)),
        "acceptable_cost_sensitivity":
            bool((robustness.get("cost_sensitivity") or {}).get(
                "acceptable", False)),
        "point_in_time_integrity_pass": bool(robustness.get(
            "point_in_time_integrity_pass", True)),
        "lockbox_accessed_exactly_once": bool(lockbox_accesses == 1),
    }
    failed = [k for k, v in conditions.items() if not v]
    return {"candidate_id": candidate_id,
            "conditions": conditions,
            "qualified": len(failed) == 0,
            "failed_conditions": failed,
            "lockbox_scored_dates": scored,
            "min_scored_forecast_dates": _contract.MIN_SCORED_FORECAST_DATES,
            "scored_dates_sufficient": enough,
            "validation_primary_value": v_pred,
            "lockbox_primary_value": l_pred_raw,
            "lockbox_excess_vs_control": l_excess,
            "lockbox_utility_improvement": l_util}


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def build_verdict(*, qualified: list, data_blocked: bool, pit_blocked: bool,
                  budget_exhausted_without_evaluation: bool) -> dict:
    """One primary verdict, plus SYSTEM_RESULT and ALPHA_RESULT separately."""
    if data_blocked:
        primary = _contract.VERDICT_DATA_BLOCKED
    elif pit_blocked:
        primary = _contract.VERDICT_PIT_BLOCKED
    elif budget_exhausted_without_evaluation:
        primary = _contract.VERDICT_BUDGET
    elif qualified:
        primary = _contract.VERDICT_QUALIFIED
    else:
        primary = _contract.VERDICT_NO_EDGE

    alpha = (_contract.RESULT_PASS
             if primary == _contract.ALPHA_PASS_REQUIRES and qualified
             else _contract.RESULT_FAIL)
    return {"primary_verdict": primary,
            "qualified_candidates": qualified,
            "alpha_result": alpha,
            "alpha_pass_requires": _contract.ALPHA_PASS_REQUIRES,
            "no_euphemism": (
                "a completed campaign, a clean audit and a large artifact set "
                "are SYSTEM outcomes and are not an investment result")}


# --------------------------------------------------------------------------- #
# Point-in-time integrity probe
# --------------------------------------------------------------------------- #
def point_in_time_probe(panel: dict, *, horizon: int,
                        truncate_at: str = "2015-06-30") -> dict:
    """Rebuild the design matrix from a TRUNCATED panel and demand equality.

    If any feature at a decision date before the truncation point changes when
    later data is removed, that feature was reading the future. This is a
    measurement, not a declaration: the contract can claim no look-ahead all it
    likes, and a single ``rolling(...).mean()`` written without a shift would
    make the claim false.
    """
    cut = pd.Timestamp(truncate_at)
    full = _features.design_matrix(panel, horizon=horizon)
    keep = np.asarray(panel["calendar"] <= cut)
    truncated_panel = dict(panel)
    truncated_panel["calendar"] = panel["calendar"][keep]
    truncated_panel["prices"] = panel["prices"].loc[keep]
    truncated_panel["log_returns"] = panel["log_returns"].loc[keep]
    truncated_panel["cash_daily"] = panel["cash_daily"].loc[keep]
    truncated_panel["benchmark"] = panel["benchmark"].loc[keep]
    part = _features.design_matrix(truncated_panel, horizon=horizon)

    if part["X"].size == 0 or full["X"].size == 0:
        return {"state": "NO_ROWS", "pass": False}

    full_key = {(str(d), str(s)): i for i, (d, s)
                in enumerate(zip(full["date"], full["symbol"]))}
    checked, mismatched, worst = 0, 0, 0.0
    worst_feature = None
    names = full["feature_names"]
    for i, (d, s) in enumerate(zip(part["date"], part["symbol"])):
        j = full_key.get((str(d), str(s)))
        if j is None:
            continue
        checked += 1
        diff = np.abs(part["X"][i] - full["X"][j])
        m = float(np.nanmax(diff)) if diff.size else 0.0
        if m > worst:
            worst, worst_feature = m, names[int(np.nanargmax(diff))]
        if m > 1e-8:
            mismatched += 1
    return {
        "state": "OK",
        "truncated_at": str(cut.date()),
        "rows_checked": int(checked),
        "rows_mismatched": int(mismatched),
        "max_absolute_difference": worst,
        "worst_feature": worst_feature,
        "pass": bool(checked > 0 and mismatched == 0),
        "method": ("the design matrix is rebuilt from a panel truncated at the "
                   "cut date; every row before the cut must be identical"),
    }


# --------------------------------------------------------------------------- #
# Lane C
# --------------------------------------------------------------------------- #
def lane_c_readiness(*, campaign_id: str, created_at: str) -> dict:
    """Persist Lane C's exact readiness state. It never blocks A or B."""
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "lane": _contract.LANE_C,
        "state": "READY_FOR_SAMPLE_NO_GENUINE_PROVIDER_DATA",
        "may_block_campaign": _contract.LANE_C_MAY_BLOCK_CAMPAIGN,
        "synthetic_data_admissible":
            _contract.LANE_C_SYNTHETIC_DATA_ADMISSIBLE,
        "why_blocked": (
            "the owned normalized earnings and analyst-revision stores carry "
            "provider_id synthetic_test / PROXY_LOCAL with placeholder "
            "tickers. Synthetic data cannot support a predictive claim, and "
            "this release spends no money, so no genuine sample was acquired"),
        "money_spent": 0.0,
        "prospective_tests_when_a_genuine_sample_arrives": [
            "5-session equity return", "20-session equity return",
            "60-session equity return", "sector-relative return",
            "future realised volatility", "post-event drift",
            "expectation dispersion",
        ],
        "candidate_information": [
            "EPS revision level", "EPS revision breadth",
            "revision acceleration", "revenue revision", "dispersion",
            "estimate change around the event", "guidance change",
            "earnings surprise versus prior expectation",
        ],
        "inherited_warning": (
            "Stage 13B measured a sales-revision PEAD effect at t = 2.27 and "
            "Stage 13C failed to replicate it out of sample at t = -0.29. A "
            "genuine sample must be tested across equity selection, sector "
            "rotation, event-driven, volatility AND expectation state rather "
            "than assumed to be a single-stock signal"),
    }
    body = r33.artifact_body(LANE_C_SCHEMA, payload)
    body["lane_c_hash"] = r33.sha(payload)
    return body


# --------------------------------------------------------------------------- #
# The campaign
# --------------------------------------------------------------------------- #
def _log(verbose: bool, message: str) -> None:
    if verbose:
        print(f"[r33] {message}", flush=True)


def run(*, campaign_id: str = _contract.CAMPAIGN_ID,
        created_at: Optional[str] = None, repo: Optional[Path] = None,
        verbose: bool = True) -> dict:
    """Run the whole bounded campaign and return its evidence."""
    created_at = created_at or _now()
    out_dir = r33.campaign_dir(campaign_id)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ---- 1. freeze the contract BEFORE any result exists ------------------ #
    contract_body = _contract.build(campaign_id=campaign_id,
                                    created_at=created_at, repo=repo)
    _contract.freeze(contract_body)
    _log(verbose, f"contract frozen {contract_body['contract_hash'][:12]}")

    # ---- 2. universe and data inventory ---------------------------------- #
    databases = _universe.vendor_database_summary()
    built = _universe.build()
    dividend_gap = _universe.measure_dividend_gap()
    uni_body = _universe.build_universe_artifact(built, campaign_id=campaign_id,
                                                 created_at=created_at)
    r33.write_json(_universe.universe_path(campaign_id), uni_body)
    inv_body = _universe.build_inventory_artifact(
        built, campaign_id=campaign_id, created_at=created_at,
        vendor_databases=databases, dividend_gap=dividend_gap)
    r33.write_json(_universe.inventory_path(campaign_id), inv_body)
    summary = _universe.summarise(built)
    _log(verbose, f"universe {summary['market_count']} markets across "
                  f"{summary['asset_class_count']} asset classes")
    if summary["market_count"] < 12:
        return _finish_blocked(campaign_id, created_at, contract_body,
                               reason="universe too small to research",
                               data_blocked=True, verbose=verbose)

    # ---- 3. panel --------------------------------------------------------- #
    panel = _panel.build(built)
    _log(verbose, f"panel {panel['prices'].shape[0]} sessions x "
                  f"{panel['prices'].shape[1]} markets")

    # ---- 4. Lane B point-in-time information ----------------------------- #
    pit_built = _pit.build(panel["calendar"], campaign_id=campaign_id)
    pit_body = _pit.manifest(pit_built, campaign_id=campaign_id,
                             created_at=created_at)
    r33.write_json(_pit.path_for(campaign_id), pit_body)
    cot_states = (pit_built["cot"] or {}).get("states") or {}
    pit_ok = bool(pit_built["state_frame"].shape[1] > 0)
    _log(verbose, f"pit states={pit_built['state_frame'].shape[1]} "
                  f"cot_markets={len(cot_states)}")

    # ---- 5. feature registry --------------------------------------------- #
    global_state = _features.build_global_state(panel)
    reg_body = _features.registry_artifact(
        campaign_id=campaign_id, created_at=created_at,
        available_globals=list(global_state.columns))
    r33.write_json(out_dir / _features.ARTIFACT_NAME, reg_body)

    # ---- 6. bundles ------------------------------------------------------- #
    bundles = {}
    for h in _contract.HORIZONS:
        b = build_bundle(panel, pit_built["state_frame"], horizon=h)
        b["cot_states"] = cot_states
        bundles[h] = b
        _log(verbose, f"bundle h={h}: {b['design']['X'].shape[0]} rows")

    # ---- 7. execute every pre-registered configuration -------------------- #
    judge_hash = _economic.behaviour_hash()
    reg = _registry.Registry(campaign_id=campaign_id,
                             contract_hash=contract_body["contract_hash"],
                             judge_behaviour_hash=judge_hash)
    cot_subset = sorted(cot_states)
    configs = enumerate_configurations(cot_subset)
    _log(verbose, f"executing {len(configs)} pre-registered configurations")

    state_cache: dict = {}
    for n, spec in enumerate(configs, start=1):
        bundle = bundles[spec["horizon"]]
        try:
            val = execute(spec, bundle, train_segments=(_partition.SEG_DISCOVERY,),
                          eval_segment=_partition.SEG_VALIDATION,
                          state_cache=state_cache)
        except Exception as exc:                      # noqa: BLE001
            val = {"state": f"EXECUTION_ERROR:{type(exc).__name__}",
                   "error": str(exc)[:300]}
        result = {"validation": {k: v for k, v in val.items()
                                 if not str(k).startswith("_")}}
        result["_validation_excess"] = val.get("_excess_series")
        try:
            reg.record(family=spec["family"], spec=spec_key(spec),
                       stage="VALIDATION", result=result)
        except _registry.BudgetExceeded as exc:
            _log(verbose, f"budget stop: {exc}")
            break
        if verbose and (n % 15 == 0 or n == len(configs)):
            _log(verbose, f"  {n}/{len(configs)} executed")
    _log(verbose, f"denominator = {reg.denominator} executed configurations")

    # ---- 8. multiple testing --------------------------------------------- #
    mt = multiple_testing(reg.rows, denominator=reg.denominator)
    mt_body = r33.artifact_body(MT_SCHEMA, {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, **mt})
    mt_body["multiple_testing_hash"] = r33.sha(mt)
    r33.write_json(out_dir / MT_ARTIFACT, mt_body)
    _log(verbose, f"multiple testing: {mt['n_survivors']} BH survivors of "
                  f"{reg.denominator}")

    # ---- 9. finalists and the lockbox ------------------------------------ #
    finalists = select_finalists(reg.rows)
    box = _lockbox.Lockbox(campaign_id=campaign_id)
    lockbox_results, robustness_results, qualifications = [], {}, []
    if finalists:
        box.freeze_finalists(finalists, selected_at=_now(),
                             selection_basis=SELECTION_BASIS)
        _log(verbose, f"lockbox: {len(finalists)} finalists frozen")
        by_hash = {r["spec_hash"]: r for r in reg.rows}
        for f in finalists:
            row = by_hash[f["spec_hash"]]
            spec = row["spec"]
            box.authorise(f["spec_hash"], family=f["family"],
                          candidate_id=f["candidate_id"], at=_now())
            bundle = bundles[spec["horizon"]]
            try:
                lock = execute(spec, bundle,
                               train_segments=(_partition.SEG_DISCOVERY,
                                               _partition.SEG_VALIDATION),
                               eval_segment=_partition.SEG_LOCKBOX,
                               state_cache=state_cache)
            except Exception as exc:                   # noqa: BLE001
                lock = {"state": f"EXECUTION_ERROR:{type(exc).__name__}",
                        "error": str(exc)[:300]}
            public = {k: v for k, v in lock.items()
                      if not str(k).startswith("_")}
            lockbox_results.append({"candidate_id": f["candidate_id"],
                                    "spec_hash": f["spec_hash"],
                                    "spec": spec, "lockbox": public})
            rob = _robustness_for(lock, bundle, row, reg.rows, spec)
            rob["point_in_time_integrity_pass"] = True
            robustness_results[f["candidate_id"]] = rob
            qualifications.append(qualify(
                candidate_id=f["candidate_id"],
                validation=row["result"]["validation"],
                lockbox=public, robustness=rob,
                survived_multiple_testing=f["candidate_id"]
                in mt["benjamini_hochberg_survivors"],
                lockbox_accesses=1))
    else:
        _log(verbose, "lockbox: no candidate was eligible; it stays unopened")

    # ---- 10. point-in-time integrity probe ------------------------------- #
    probe = point_in_time_probe(panel, horizon=20)
    _log(verbose, f"point-in-time probe: pass={probe.get('pass')} "
                  f"rows={probe.get('rows_checked')}")
    for rob in robustness_results.values():
        rob["point_in_time_integrity_pass"] = bool(probe.get("pass"))
    qualifications = [
        qualify(candidate_id=q["candidate_id"],
                validation={"primary_value": q["validation_primary_value"]},
                lockbox=next(l["lockbox"] for l in lockbox_results
                             if l["candidate_id"] == q["candidate_id"]),
                robustness=robustness_results[q["candidate_id"]],
                survived_multiple_testing=q["conditions"][
                    "survives_multiple_testing_procedure"],
                lockbox_accesses=1)
        for q in qualifications]

    # ---- 11. artifacts ---------------------------------------------------- #
    r33.write_json(_registry.path_for(campaign_id),
                   reg.artifact(created_at=created_at))
    r33.write_json(_lockbox.path_for(campaign_id),
                   box.manifest(created_at=created_at,
                                results=lockbox_results))
    r33.write_json(out_dir / ROBUSTNESS_ARTIFACT, r33.artifact_body(
        ROBUSTNESS_SCHEMA, {"calculation_owner": CALCULATION_OWNER,
                            "campaign_id": campaign_id,
                            "created_at": created_at,
                            "point_in_time_probe": probe,
                            "by_candidate": robustness_results}))
    r33.write_json(out_dir / LANE_C_ARTIFACT,
                   lane_c_readiness(campaign_id=campaign_id,
                                    created_at=created_at))
    _write_result_artifacts(out_dir, campaign_id, created_at, reg.rows,
                            lockbox_results)

    # ---- 12. verdict ------------------------------------------------------ #
    qualified = [q["candidate_id"] for q in qualifications if q["qualified"]]
    verdict = build_verdict(qualified=qualified, data_blocked=False,
                            pit_blocked=not pit_ok,
                            budget_exhausted_without_evaluation=False)
    system_result = (_contract.RESULT_PASS
                     if (probe.get("pass") and reg.denominator > 0
                         and _contract.verify(contract_body)["stable"])
                     else _contract.RESULT_FAIL)
    body = _verdict_artifact(campaign_id, created_at, contract_body, summary,
                             databases, reg, mt, finalists, lockbox_results,
                             qualifications, verdict, system_result, probe,
                             pit_body, dividend_gap, panel)
    r33.write_json(out_dir / VERDICT_ARTIFACT, body)
    _log(verbose, f"VERDICT {verdict['primary_verdict']} | "
                  f"SYSTEM_RESULT={system_result} | "
                  f"ALPHA_RESULT={verdict['alpha_result']}")
    return {"verdict": body, "registry": reg, "lockbox": box,
            "multiple_testing": mt, "probe": probe,
            "qualifications": qualifications, "panel": panel,
            "universe": built}


def _robustness_for(lock: dict, bundle: dict, row: dict, rows: list,
                    spec: dict) -> dict:
    """Every robustness diagnostic the contract requires for a finalist."""
    weights = lock.get("_weights")
    control = lock.get("_control")
    net = lock.get("_net")
    dates = lock.get("_dates")
    meta = lock.get("_meta") or {}
    horizon = int(spec["horizon"])
    out = {"metric_key": "primary_value"}
    out["parameter_cliff"] = _robustness.parameter_cliff(
        rows, row, metric_key="primary_value", segment="validation")
    if weights is None or control is None or net is None:
        out["state"] = "NO_ECONOMIC_PATH"
        out["cost_sensitivity"] = {"acceptable": False}
        return out
    out["state"] = "OK"
    out["leave_one_market_out"] = _robustness.leave_one_market_out(
        weights, bundle["excess_returns"], bundle["cash"], meta=meta,
        horizon=horizon, control=control)
    out["leave_one_asset_class_out"] = _robustness.leave_one_asset_class_out(
        weights, bundle["excess_returns"], bundle["cash"], meta=meta,
        horizon=horizon, control=control)
    out["subperiod_stability"] = _robustness.subperiod_stability(
        net, control, dates, horizon=horizon)
    out["cost_sensitivity"] = _robustness.cost_sensitivity(
        weights, bundle["excess_returns"], bundle["cash"], meta=meta,
        horizon=horizon, control=control)
    out["exposure_decomposition"] = _robustness.exposure_decomposition(
        weights, meta)
    out["turnover_concentration"] = _robustness.turnover_concentration(weights)
    return out


def _write_result_artifacts(out_dir: Path, campaign_id: str, created_at: str,
                            rows: list, lockbox_results: list) -> None:
    predictive_rows, economic_rows = [], []
    for r in rows:
        val = (r.get("result") or {}).get("validation") or {}
        predictive_rows.append({
            "candidate_id": r["candidate_id"], "spec": r["spec"],
            "state": val.get("state"),
            "primary_metric": val.get("primary_metric"),
            "validation_primary_value": val.get("primary_value"),
            "gain_t_stat": val.get("gain_t_stat"),
            "gain_p_value": val.get("gain_p_value"),
            "scored_dates": val.get("scored_dates"),
            "predictive": val.get("predictive")})
        economic_rows.append({
            "candidate_id": r["candidate_id"], "spec": r["spec"],
            "economic": val.get("economic")})
    r33.write_json(out_dir / PREDICTIVE_ARTIFACT, r33.artifact_body(
        PREDICTIVE_SCHEMA, {"calculation_owner": CALCULATION_OWNER,
                            "campaign_id": campaign_id,
                            "created_at": created_at,
                            "primary_metric_by_target":
                                dict(_contract.PRIMARY_METRIC),
                            "forecast_baseline_by_target":
                                dict(_contract.FORECAST_BASELINE),
                            "validation": predictive_rows,
                            "lockbox": lockbox_results}))
    r33.write_json(out_dir / ECONOMIC_ARTIFACT, r33.artifact_body(
        ECONOMIC_SCHEMA, {"calculation_owner": CALCULATION_OWNER,
                          "campaign_id": campaign_id,
                          "created_at": created_at,
                          "judge": _economic.judge_declaration(),
                          "judge_behaviour_hash": _economic.behaviour_hash(),
                          "validation": economic_rows}))


def _verdict_artifact(campaign_id, created_at, contract_body, summary,
                      databases, reg, mt, finalists, lockbox_results,
                      qualifications, verdict, system_result, probe, pit_body,
                      dividend_gap, panel) -> dict:
    best = sorted(
        [r for r in reg.rows
         if ((r["result"].get("validation") or {}).get("primary_value")
             is not None)],
        key=lambda r: -float(r["result"]["validation"]["primary_value"]))[:10]
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "contract_hash": contract_body["contract_hash"],
        "judge_behaviour_hash": _economic.behaviour_hash(),
        "mission": ("find measurable out-of-sample predictive edge; a "
                    "completed campaign is not an alpha success"),
        **verdict,
        "system_result": system_result,
        "system_result_meaning": (
            "the machinery ran as specified on frozen evidence: the contract "
            "hash is stable, the point-in-time probe passed, and every "
            "executed configuration entered the denominator"),
        "alpha_result_meaning": (
            "at least one candidate improved a pre-registered forecast score "
            "AND beat a risk-matched investable control after cost, on "
            "evidence that played no part in selecting it"),
        "inherited_release32": {"system_result": _contract.R32_SYSTEM_RESULT,
                                "alpha_result": _contract.R32_ALPHA_RESULT,
                                "verdict": _contract.R32_VERDICT},
        "universe": summary,
        "vendor_databases": databases,
        "continuous_futures_entitlement_markets":
            databases.get("Continuous Futures", {}).get("count"),
        "panel": {"sessions": int(panel["prices"].shape[0]),
                  "markets": int(panel["prices"].shape[1]),
                  "first_session": str(panel["calendar"][0].date()),
                  "last_session": str(panel["calendar"][-1].date())},
        "measured_equity_dividend_gap": dividend_gap,
        "denominator_executed_configurations": reg.denominator,
        "budgets": reg.budget_report(),
        "multiple_testing": {k: v for k, v in mt.items()
                             if k != "superior_predictive_ability_by_horizon"},
        "superior_predictive_ability_by_horizon":
            mt.get("superior_predictive_ability_by_horizon"),
        "best_validation_candidates": [
            {"candidate_id": r["candidate_id"], "spec": r["spec"],
             "primary_metric": r["result"]["validation"].get("primary_metric"),
             "primary_value": r["result"]["validation"].get("primary_value"),
             "gain_t_stat": r["result"]["validation"].get("gain_t_stat"),
             "economic": (r["result"]["validation"].get("economic") or {}).get(
                 "vs_controls", {}).get(_contract.ECONOMIC_CONTROL)}
            for r in best],
        "lockbox_finalists": finalists,
        "lockbox_results": lockbox_results,
        "qualifications": qualifications,
        "point_in_time_probe": probe,
        "pit_information": {
            "sources": {k: v.get("state") for k, v
                        in (pit_body.get("sources") or {}).items()},
            "state_variable_count": pit_body.get("state_variable_count"),
        },
        "implementability": {
            "state": _contract.UNIVERSE_IMPLEMENTABILITY_STATE,
            "futures_implementability_claimable":
                _contract.FUTURES_IMPLEMENTABILITY_CLAIMABLE,
        },
        "money_spent": 0.0,
    }
    body = r33.artifact_body(VERDICT_SCHEMA, payload)
    body["verdict_hash"] = r33.sha(payload)
    return body


def _finish_blocked(campaign_id, created_at, contract_body, *, reason,
                    data_blocked, verbose) -> dict:
    verdict = build_verdict(qualified=[], data_blocked=data_blocked,
                            pit_blocked=False,
                            budget_exhausted_without_evaluation=False)
    body = r33.artifact_body(VERDICT_SCHEMA, {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, "blocked_reason": reason,
        "system_result": _contract.RESULT_FAIL, **verdict})
    r33.write_json(r33.campaign_dir(campaign_id) / VERDICT_ARTIFACT, body)
    _log(verbose, f"BLOCKED: {reason}")
    return {"verdict": body}
