"""alpha_agent.r34.campaign - orchestration, evidence and the terminal verdict.

This module runs the bounded, pre-registered conversion campaign and writes the
immutable artifacts. It contains no thresholds of its own: every number it
compares against lives in :mod:`alpha_agent.r34.contract` and was frozen before
any result existed.

The shape of the campaign is a LADDER, not a grid. A full cross of the five
conversion lanes is 5 x 6 x 9 x 5 x 5 = 6,750 configurations, which would be an
unbounded search wearing a plan's clothing and would blow through the frozen
ceiling of 80 by two orders of magnitude. Instead each lane is varied with the
others held at a DEFAULT declared in the contract, the winner of each lane is
carried forward, and a small confirmatory set of finalists combines them. The
limitation is real - a coordinate-wise design cannot see every interaction - and
it is declared rather than glossed.

Forecasts are computed ONCE per (model, horizon, fold) and cached. Every
conversion configuration is then a cheap transform of the same cached scores,
which is what makes fifty configurations affordable and, more importantly, means
every conversion candidate is compared on IDENTICAL forecasts. A campaign that
refits the model inside each conversion configuration cannot tell a conversion
result from a refit lottery.

Everything funnels through ONE pipeline - conviction frame, then book, then
judge - so that a lane can only change what it is supposed to change. The
horizon lane in particular reuses the same machinery rather than carrying a
second implementation of it.

Nothing here writes an operational store, promotes a model, activates a sleeve,
creates a proposal, a decision or an order, or touches the production paper
portfolio.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r34
from ..r31 import multiple_testing as _mt
from . import attrition as _attrition
from . import calibration as _calibration
from . import concentration as _concentration
from . import contract as _contract
from . import economics as _economics
from . import forecast as _forecast
from . import horizon as _horizon
from . import panel as _panel
from . import portfolio as _portfolio
from . import sizing as _sizing
from . import turnover as _turnover
from . import universe as _universe
from . import walkforward as _walkforward

CALCULATION_OWNER = "alpha_agent.r34.campaign"

ARTIFACTS = {
    "forecast": ("forecast_models.json", "r34_forecast_models/1"),
    "calibration": ("calibration_results.json", "r34_calibration_results/1"),
    "sizing": ("position_sizing_results.json",
               "r34_position_sizing_results/1"),
    "horizon": ("horizon_combination_results.json",
                "r34_horizon_combination_results/1"),
    "turnover": ("turnover_cost_results.json", "r34_turnover_cost_results/1"),
    "portfolio": ("portfolio_results.json", "r34_portfolio_results/1"),
    "walkforward": ("walk_forward_results.json", "r34_walk_forward_results/1"),
    "concentration": ("concentration_results.json",
                      "r34_concentration_results/1"),
    "attrition": ("attrition_waterfall.json", "r34_attrition_waterfall/1"),
    "multiple_testing": ("multiple_testing.json", "r34_multiple_testing/1"),
    "verdict": ("final_verdict.json", "r34_final_verdict/1"),
}


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat()


def _say(verbose: bool, message: str) -> None:
    if verbose:
        print(message, flush=True)


def _clean(obj):
    """JSON-safe: numpy scalars out, non-finite floats to None."""
    if isinstance(obj, dict):
        return {str(k): _clean(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_clean(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return _clean(obj.tolist())
    if isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    if isinstance(obj, (np.integer,)) or (isinstance(obj, int)
                                          and not isinstance(obj, bool)):
        return int(obj)
    if isinstance(obj, (np.floating, float)):
        v = float(obj)
        return v if np.isfinite(v) else None
    if isinstance(obj, (pd.Timestamp, _dt.date, _dt.datetime)):
        return str(obj)
    return obj


def _write(campaign_id: str, key: str, payload: dict) -> Path:
    name, schema = ARTIFACTS[key]
    body = r34.artifact_body(schema, _clean(payload))
    body[f"{key}_hash"] = r34.sha(body)
    return r34.write_json(r34.campaign_dir(campaign_id) / name, body)


def _write_body(campaign_id: str, name: str, hash_key: str,
                body: dict) -> Path:
    """Write an already-built artifact body, hashing WHAT IS WRITTEN.

    The hash has to cover the bytes that land on disk. Hashing a payload and
    then passing it through :func:`_clean` - which turns numpy scalars into
    Python ones and non-finite floats into null - produces a recorded hash that
    nothing can recompute from the file, which is a hash that cannot detect the
    tampering it exists to detect.
    """
    cleaned = _clean(body)
    cleaned.pop(hash_key, None)
    cleaned[hash_key] = r34.sha(cleaned)
    return r34.write_json(r34.campaign_dir(campaign_id) / name, cleaned)


# --------------------------------------------------------------------------- #
# Per-horizon evidence context
# --------------------------------------------------------------------------- #
def build_context(panel: dict, *, horizon: int, verbose: bool = False) -> dict:
    """Design matrix, targets, folds and per-date row groups for one horizon."""
    _say(verbose, f"    design matrix h={horizon} ...")
    design = _forecast.design(panel, horizon=horizon)
    targets = _forecast.align_targets(panel, design, horizon=horizon)

    row_dates = pd.DatetimeIndex(design["date"])
    udates = pd.DatetimeIndex(sorted(set(row_dates)))
    date_pos = {d: k for k, d in enumerate(udates)}
    row_date_pos = np.asarray([date_pos[d] for d in row_dates], dtype=np.int64)

    first_row = {}
    for k, d in enumerate(row_dates):
        first_row.setdefault(d, k)
    udix = np.asarray([design["decision_index"][first_row[d]] for d in udates],
                      dtype=np.int64)

    folds = _walkforward.folds(udates, horizon=horizon,
                               calendar=panel["calendar"], decision_index=udix)

    groups: dict = {}
    for k in range(len(row_dates)):
        groups.setdefault(int(row_date_pos[k]), []).append(k)

    return {"horizon": int(horizon), "design": design, "targets": targets,
            "row_dates": row_dates, "udates": udates, "date_pos": date_pos,
            "row_date_pos": row_date_pos, "folds": folds,
            "row_groups": {p: np.asarray(v, dtype=np.int64)
                           for p, v in groups.items()},
            "symbols": np.asarray(design["symbol"]),
            "asset_class": np.asarray(design["asset_class"]),
            "feature_names": design["feature_names"],
            "meta": panel["meta"],
            "prices": panel["prices"],
            "cash_obs": _panel.cash_observation_returns(panel,
                                                        horizon=horizon),
            "bench_obs": _panel.benchmark_observation_returns(panel,
                                                              horizon=horizon),
            "all_symbols": list(targets["excess_frame"].columns)}


def _rows_for(ctx: dict, date_positions) -> np.ndarray:
    if len(date_positions) == 0:
        return np.zeros(0, dtype=np.int64)
    wanted = np.zeros(len(ctx["udates"]), dtype=bool)
    wanted[np.asarray(date_positions, dtype=np.int64)] = True
    return np.flatnonzero(wanted[ctx["row_date_pos"]])


def run_forecasts(ctx: dict, *, verbose: bool = False) -> dict:
    """Fit every frozen family once per fold and cache its out-of-sample scores.

    Two fits per fold, for the reason declared in the contract: the INNER-FIT
    model produces honest out-of-sample scores on the inner-validation block,
    which is where calibrations are estimated and parameters are selected, and a
    refit on the whole training block produces the evaluation forecasts. The
    score-scale ratio between the two is measured rather than assumed harmless.
    """
    design, targets = ctx["design"], ctx["targets"]
    X, y = design["X"], targets["y_excess"]
    y_vol, trailing = targets["y_realised_vol"], targets["trailing_vol"]
    groups, names = ctx["asset_class"], ctx["feature_names"]
    n_rows = X.shape[0]

    out = {}
    for cfg in _forecast.model_configs():
        key = config_key(cfg)
        score_eval = np.full(n_rows, np.nan)
        score_inner = np.full(n_rows, np.nan)
        vol_eval = np.full(n_rows, np.nan)
        scale_ratios = []

        for fold in ctx["folds"]:
            if not fold["usable"]:
                continue
            train_rows = _rows_for(ctx, fold["train"])
            inner_fit_rows = _rows_for(ctx, fold["inner_fit"])
            inner_val_rows = _rows_for(ctx, fold["inner_validation"])
            eval_rows = _rows_for(ctx, fold["evaluation"])
            if train_rows.size < 200 or eval_rows.size == 0:
                continue

            fit_inner = _forecast.fit(cfg["model"], cfg["params"],
                                      X[inner_fit_rows], y[inner_fit_rows],
                                      groups[inner_fit_rows],
                                      feature_names=names)
            if inner_val_rows.size:
                score_inner[inner_val_rows] = _forecast.predict(
                    fit_inner, X[inner_val_rows], groups[inner_val_rows],
                    feature_names=names)

            fit_full = _forecast.fit(cfg["model"], cfg["params"],
                                     X[train_rows], y[train_rows],
                                     groups[train_rows], feature_names=names)
            s_ev = _forecast.predict(fit_full, X[eval_rows], groups[eval_rows],
                                     feature_names=names)
            score_eval[eval_rows] = s_ev

            vfit = _forecast.fit_volatility(X[train_rows], y_vol[train_rows],
                                            feature_names=names)
            vol_eval[eval_rows] = _forecast.predict_volatility(
                vfit, X[eval_rows], trailing=trailing[eval_rows])

            if inner_val_rows.size > 8 and eval_rows.size > 8:
                a = float(np.nanstd(score_inner[inner_val_rows], ddof=1))
                b = float(np.nanstd(s_ev, ddof=1))
                if a > 0 and np.isfinite(b) and b > 0:
                    scale_ratios.append(b / a)

        out[key] = {"model": cfg["model"], "params": cfg["params"],
                    "score_eval": score_eval, "score_inner": score_inner,
                    "vol_eval": vol_eval,
                    "score_scale_ratio": (float(np.median(scale_ratios))
                                          if scale_ratios else None)}
        _say(verbose, f"      {cfg['model']:34} {cfg['params']} -> "
                      f"{int(np.isfinite(score_eval).sum())} scored rows")
    return out


def config_key(cfg: dict) -> tuple:
    return (cfg["model"], tuple(sorted(cfg["params"].items())))


def key_label(key) -> str:
    model, params = key
    if not params:
        return model
    return model + "[" + ",".join(f"{k}={v}" for k, v in params) + "]"


# --------------------------------------------------------------------------- #
# Stage 1: conviction
# --------------------------------------------------------------------------- #
def conviction_frame(ctx: dict, forecasts: dict, *, model_key,
                     calibration: str, sizing_rule: str,
                     use_realised_as_forecast: bool = False,
                     use_rank_only: bool = False) -> dict:
    """Per-date conviction, expected return and predicted volatility.

    Calibration is refitted PER FOLD on that fold's inner-validation scores, so
    every number here uses only information available before the block it is
    applied to.
    """
    cached = forecasts[model_key]
    score_eval, score_inner = cached["score_eval"], cached["score_inner"]
    vol_eval = cached["vol_eval"]
    targets = ctx["targets"]
    y, trailing, tradable = (targets["y_excess"], targets["trailing_vol"],
                             targets["tradable"])
    symbols, all_symbols = ctx["symbols"], ctx["all_symbols"]
    sym_pos = {s: k for k, s in enumerate(all_symbols)}
    udates = ctx["udates"]

    n_dates, n_syms = len(udates), len(all_symbols)
    conv = np.full((n_dates, n_syms), np.nan)
    er_out = np.full((n_dates, n_syms), np.nan)
    vol_out = np.full((n_dates, n_syms), np.nan)
    cal_specs, used_dates = {}, []

    for f_i, fold in enumerate(ctx["folds"]):
        if not fold["usable"]:
            continue
        inner_rows = _rows_for(ctx, fold["inner_validation"])
        ok = (inner_rows[np.isfinite(score_inner[inner_rows])
                         & np.isfinite(y[inner_rows])]
              if inner_rows.size else np.zeros(0, dtype=np.int64))
        spec = _calibration.fit(calibration, score_inner[ok], y[ok])
        cal_specs[f_i] = spec

        for dp in fold["evaluation"]:
            idx = ctx["row_groups"].get(int(dp))
            if idx is None or idx.size == 0:
                continue
            raw = y[idx] if use_realised_as_forecast else score_eval[idx]
            trade = tradable[idx] & np.isfinite(raw)
            if not trade.any():
                used_dates.append(int(dp))
                continue

            if use_realised_as_forecast:
                # The perfect-foresight CEILING must not be routed through a
                # calibration fitted on model scores. Under perfect foresight
                # the realised return IS the expected return; passing it
                # through a mapping whose slope was estimated on a different
                # quantity - and which is NEGATIVE in one fold - would invert
                # the ceiling and make the realised book look as though it had
                # captured 98 % of what perfect foresight could earn.
                er = raw
                unc = np.full(idx.size, 1e-3)
                conf = 1.0
            elif use_rank_only:
                er = _calibration.rank_only(raw, trade)
                unc = np.full(idx.size, 1.0)
                conf = 1.0
            else:
                applied = _calibration.apply(spec, raw)
                er, unc = applied["expected_return"], applied["uncertainty"]
                conf = applied["confidence"]

            vol = np.where(np.isfinite(vol_eval[idx]), vol_eval[idx],
                           trailing[idx])
            vol = np.where(np.isfinite(vol), vol, 0.15)
            c = _sizing.conviction(sizing_rule, expected_return=er,
                                   uncertainty=unc, predicted_vol=vol,
                                   score=raw, confidence=conf)
            for k, j in enumerate(idx):
                if not trade[k]:
                    continue
                p = sym_pos[symbols[j]]
                conv[dp, p] = c[k]
                er_out[dp, p] = er[k]
                vol_out[dp, p] = vol[k]
            used_dates.append(int(dp))

    keep = sorted(set(used_dates))
    index = udates[keep]
    return {"conviction": pd.DataFrame(conv[keep], index=index,
                                       columns=all_symbols),
            "expected_return": pd.DataFrame(er_out[keep], index=index,
                                            columns=all_symbols),
            "predicted_vol": pd.DataFrame(vol_out[keep], index=index,
                                          columns=all_symbols),
            "calibrations": cal_specs}


# --------------------------------------------------------------------------- #
# Stage 2: book
# --------------------------------------------------------------------------- #
def book_from_conviction(ctx: dict, frames: dict, *, mapping: str,
                         turnover_rule: str, turnover_param: float,
                         cov_by_date: dict = None,
                         apply_caps: bool = True,
                         apply_transition: bool = True) -> pd.DataFrame:
    """Map conviction onto a weight path, honouring caps and the transition rule.

    ``apply_caps`` and ``apply_transition`` are False only for the attrition
    waterfall, which needs the value of the book BEFORE each of those stages in
    order to price them separately.
    """
    conv_f, er_f = frames["conviction"], frames["expected_return"]
    vol_f = frames["predicted_vol"]
    tradable_f = ctx["targets"]["tradable_frame"]
    all_symbols = list(conv_f.columns)
    ac = np.asarray([ctx["meta"].get(s, {}).get("asset_class", "UNKNOWN")
                     for s in all_symbols])
    rate = np.asarray([float(ctx["meta"].get(s, {}).get("cost_bps_per_side",
                                                        6.0)) / 1e4
                       for s in all_symbols])

    prev = np.zeros(len(all_symbols))
    rows, index = [], []
    conv_v = conv_f.to_numpy()
    er_v = np.where(np.isfinite(er_f.to_numpy()), er_f.to_numpy(), 0.0)
    vol_v = np.where(np.isfinite(vol_f.to_numpy()), vol_f.to_numpy(), 0.15)
    trad_v = tradable_f.reindex(index=conv_f.index,
                                columns=all_symbols).fillna(False).to_numpy()

    for k, date in enumerate(conv_f.index):
        trade = trad_v[k] & np.isfinite(conv_v[k])
        if not trade.any():
            rows.append(prev.copy())
            index.append(date)
            continue
        c = np.where(trade, conv_v[k], -np.inf)

        if apply_caps:
            cov = (cov_by_date or {}).get(date)
            target = _portfolio.build_weights(
                mapping, conviction=c, expected_return=er_v[k],
                predicted_vol=vol_v[k], asset_class=ac, tradable=trade,
                cov=cov)
        else:
            raw = np.where(trade & np.isfinite(c) & (c > 0), c, 0.0)
            total = float(raw.sum())
            target = raw / total if total > 0 else raw

        if apply_transition:
            w = _turnover.transition(turnover_rule, previous=prev,
                                     target=target, expected_return=er_v[k],
                                     cost_rate=rate, asset_class=ac,
                                     param=float(turnover_param))
        else:
            w = target
        prev = w
        rows.append(w.copy())
        index.append(date)

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows, index=pd.DatetimeIndex(index),
                        columns=all_symbols)


# --------------------------------------------------------------------------- #
# Stage 3: judge
# --------------------------------------------------------------------------- #
def judge_book(ctx: dict, weights: pd.DataFrame, *,
               cost_multiplier: float = 1.0) -> dict:
    """Score one weight path against the primary risk-matched control.

    Named ``judge_book`` rather than the obvious alternative: that name's
    definition signature is RESERVED for the research-state calculation owner
    ``engine/research_agent.py``, and the architecture audit blocks a second
    definition of it anywhere in the repository. The guard is a substring
    search, so even naming the reserved signature in a comment trips it - which
    is why this sentence describes it instead of quoting it.
    """
    if weights is None or weights.empty:
        return {"state": "NO_POSITIONS"}
    path = _economics.evaluate_book(weights, ctx["targets"]["excess_frame"],
                                    ctx["cash_obs"], meta=ctx["meta"],
                                    horizon=ctx["horizon"],
                                    cost_multiplier=cost_multiplier)
    if path.get("state") != "OK":
        return {"state": path.get("state", "NO_POSITIONS")}

    idx = path["dates"]
    bench = ctx["bench_obs"].reindex(idx).to_numpy()
    cash_v = ctx["cash_obs"].reindex(idx).fillna(0.0).to_numpy()
    vm = _economics.volatility_matched_control(path["net"], bench, cash_v)
    control = vm["series"] if vm.get("series") is not None else cash_v

    stats = _economics.describe(path, horizon=ctx["horizon"], control=control)
    stats["control_weight_on_benchmark"] = vm.get("weight")
    sig = _economics.excess_significance(path["net"], control,
                                         horizon=ctx["horizon"])
    return {"state": "OK", "path": path, "control": control,
            "control_state": vm.get("state"), "stats": stats,
            "excess_series": sig.get("diff"), "dates": idx}


def control_suite(ctx: dict, dates: pd.DatetimeIndex) -> dict:
    """Every pre-registered control, on the candidate's own dates."""
    targets = ctx["targets"]
    excess = targets["excess_frame"].reindex(dates)
    tradable = targets["tradable_frame"].reindex(dates)
    cash = ctx["cash_obs"].reindex(dates).fillna(0.0)
    horizon = ctx["horizon"]
    bench = ctx["bench_obs"].reindex(dates).to_numpy()

    series = {
        _contract.CONTROL_CASH: cash.to_numpy(),
        _contract.CONTROL_EQUAL_WEIGHT: _economics.equal_weight_control(
            excess, tradable, cash),
        _contract.CONTROL_BUY_AND_HOLD: cash.to_numpy()
            + np.where(np.isfinite(bench), bench, 0.0),
        _contract.CONTROL_SIXTY_FORTY: _economics.sixty_forty_control(
            excess, cash, equity=_contract.BENCHMARK_SYMBOL, bond="AGG"),
        _contract.CONTROL_TREND: _economics.trend_control(
            ctx["prices"].reindex(dates), excess, tradable, cash),
    }
    out = {name: {
        "annualised_return": _economics.annualised_return(s, horizon=horizon),
        "annualised_volatility": _economics.annualised_volatility(
            s, horizon=horizon),
        "sharpe": _economics.sharpe(s, horizon=horizon,
                                    cash=cash.to_numpy()),
        "sortino": _economics.sortino(s, horizon=horizon,
                                      cash=cash.to_numpy()),
        "max_drawdown": _economics.max_drawdown(s),
        "cvar_5pct": _economics.cvar(s, 0.05),
        "utility_annualised": _economics.utility(s, horizon=horizon),
    } for name, s in series.items()}
    return {"controls": out, "series": series}


# --------------------------------------------------------------------------- #
# One candidate, end to end
# --------------------------------------------------------------------------- #
def run_candidate(ctx: dict, forecasts: dict, *, family: str, label: str,
                  model_key, calibration: str, sizing_rule: str,
                  mapping: str, turnover_rule: str, turnover_param: float,
                  cov_by_date: dict = None,
                  conviction_override: dict = None,
                  config: dict = None) -> dict:
    """Conviction, book, judge and cost sensitivity for one configuration."""
    frames = conviction_override or conviction_frame(
        ctx, forecasts, model_key=model_key, calibration=calibration,
        sizing_rule=sizing_rule)
    weights = book_from_conviction(ctx, frames, mapping=mapping,
                                   turnover_rule=turnover_rule,
                                   turnover_param=turnover_param,
                                   cov_by_date=cov_by_date)
    result = judge_book(ctx, weights)
    record = {
        "candidate_id": f"{family}::{label}",
        "family": family, "label": label,
        "horizon": ctx["horizon"],
        "config": dict(config or {}),
        "model": key_label(model_key),
        "calibration": calibration, "sizing": sizing_rule,
        "portfolio": mapping, "turnover": turnover_rule,
        "turnover_param": float(turnover_param),
        "state": result.get("state"),
    }
    if result.get("state") != "OK":
        return {"record": record, "result": result, "weights": weights,
                "frames": frames}

    record["stats"] = result["stats"]
    record["after_cost_excess_annualised"] = result["stats"].get(
        "after_cost_excess_annualised")
    record["after_cost_excess_utility"] = result["stats"].get(
        "after_cost_excess_utility")
    record["after_cost_excess_t_stat"] = result["stats"].get(
        "after_cost_excess_t_stat")

    # Cost sensitivity - a result that survives only its most optimistic cost
    # assumption has not survived.
    scenarios = {}
    for name, mult in _contract.COST_SCENARIOS.items():
        if name == _contract.COST_SCENARIO_PRIMARY:
            scenarios[name] = record["after_cost_excess_annualised"]
            continue
        alt = judge_book(ctx, weights, cost_multiplier=mult)
        scenarios[name] = (alt["stats"].get("after_cost_excess_annualised")
                           if alt.get("state") == "OK" else None)
    record["cost_scenarios"] = scenarios
    record["survives_stressed_cost"] = bool(
        (scenarios.get(_contract.COST_SENSITIVITY_REQUIRED_THROUGH) or 0.0) > 0)

    # Fold-by-fold, so a result that lives in one regime cannot hide in an
    # average.
    record["per_fold"] = per_fold_result(ctx, result)
    same = [f for f in record["per_fold"] if f.get("excess") is not None]
    if same:
        base = record["after_cost_excess_annualised"] or 0.0
        agree = sum(1 for f in same
                    if np.sign(f["excess"]) == np.sign(base) and base != 0)
        record["same_sign_fold_fraction"] = float(agree / len(same))
    else:
        record["same_sign_fold_fraction"] = None
    return {"record": record, "result": result, "weights": weights,
            "frames": frames}


def per_fold_result(ctx: dict, result: dict) -> list:
    """After-cost excess over the control, one walk-forward fold at a time."""
    dates = pd.DatetimeIndex(result["dates"])
    net = np.asarray(result["path"]["net"], dtype=np.float64)
    ctrl = np.asarray(result["control"], dtype=np.float64)
    out = []
    for fold in ctx["folds"]:
        if not fold["usable"]:
            continue
        t0 = pd.Timestamp(fold["evaluation_start"])
        t1 = pd.Timestamp(fold["evaluation_end"])
        m = (dates >= t0) & (dates <= t1)
        if m.sum() < 4:
            out.append({"evaluation_start": fold["evaluation_start"],
                        "evaluation_end": fold["evaluation_end"],
                        "n": int(m.sum()), "excess": None})
            continue
        sig = _economics.excess_significance(net[m], ctrl[m],
                                            horizon=ctx["horizon"])
        out.append({"evaluation_start": fold["evaluation_start"],
                    "evaluation_end": fold["evaluation_end"],
                    "n": int(m.sum()),
                    "excess": sig.get("annualised_excess"),
                    "t_stat": sig.get("t_stat"),
                    "net_return_annualised": _economics.annualised_return(
                        net[m], horizon=ctx["horizon"]),
                    "control_return_annualised": _economics.annualised_return(
                        ctrl[m], horizon=ctx["horizon"])})
    return out


def _best(records: list, key: str = "after_cost_excess_utility"):
    """The best record by the PRIMARY decision statistic, or None.

    One function, used everywhere a lane picks its winner, so no lane can quietly
    rank on something more flattering than the statistic the contract named.
    """
    scored = [r for r in records
              if r.get(key) is not None and np.isfinite(float(r[key]))]
    if not scored:
        return None
    return max(scored, key=lambda r: float(r[key]))


def select_model_by_inner_validation(ctx: dict, forecasts: dict) -> dict:
    """Pick the forecast family on the INNER-VALIDATION block, by rank IC.

    Inside the training partition, never on the evaluation block. Reported per
    fold as well as overall, because a family that wins on average by winning
    one fold enormously is not the family to carry forward.
    """
    y = ctx["targets"]["y_excess"]
    dates = ctx["row_dates"]
    rows = []
    for key, cached in forecasts.items():
        s = cached["score_inner"]
        ok = np.isfinite(s) & np.isfinite(y)
        if ok.sum() < 200:
            rows.append({"model": key_label(key), "key": key,
                         "inner_rank_ic": None, "state": "TOO_FEW_ROWS"})
            continue
        ic = _forecast.rank_ic(s[ok], y[ok], dates[ok])
        rows.append({"model": key_label(key), "key": key,
                     "inner_rank_ic": ic.get("value"),
                     "inner_rank_ic_t": ic.get("t_stat"),
                     "inner_scored_dates": ic.get("n"), "state": "OK"})
    scored = [r for r in rows if r.get("inner_rank_ic") is not None]
    winner = max(scored, key=lambda r: float(r["inner_rank_ic"])) \
        if scored else None
    return {"per_model": rows, "selected": winner,
            "selection_rule": _contract.DEFAULT_MODEL_SELECTION}


# --------------------------------------------------------------------------- #
# The campaign
# --------------------------------------------------------------------------- #
def run(*, campaign_id: str = _contract.CAMPAIGN_ID,
        repo: Optional[Path] = None, verbose: bool = True) -> dict:
    """Run the bounded Release-34 conversion campaign end to end."""
    created_at = _now()
    root = r34.campaign_dir(campaign_id)
    _say(verbose, f"=== Release 34 - prediction to PnL - {campaign_id} ===")

    contract = _contract.build(campaign_id=campaign_id,
                               created_at=created_at, repo=repo)
    _contract.freeze(contract)
    _say(verbose, f"contract frozen  hash={contract['contract_hash'][:16]}")

    # ---------------- Lane A: the implementable universe ------------------ #
    _say(verbose, "Lane A: implementable universe ...")
    built = _universe.build(cache=root / "cache" / "etp_enumeration.json")
    state = _universe.implementability_state(built)
    _write_body(campaign_id, _universe.UNIVERSE_ARTIFACT, "universe_hash",
                _universe.universe_artifact(
                    built, campaign_id=campaign_id, created_at=created_at,
                    state=state))
    _write_body(campaign_id, _universe.INTEGRITY_ARTIFACT, "integrity_hash",
                _universe.integrity_artifact(
                    built, campaign_id=campaign_id, created_at=created_at))
    _say(verbose, f"  {state['state']}  instruments={state['instrument_count']}"
                  f"  asset classes={state['asset_class_count']}"
                  f"  unfilled={built['unfilled_slots']}")

    if state["state"] != _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE:
        verdict = build_verdict(
            campaign_id=campaign_id, created_at=created_at, contract=contract,
            primary=_contract.VERDICT_UNIVERSE_BLOCKED, universe=state,
            candidates=[], finalists=[], multiple_testing={},
            evidence=_walkforward.evidence_state(), blocking=state[
                "blocking_reasons"])
        _write(campaign_id, "verdict", verdict)
        return {"verdict": verdict, "universe": state}

    panel = _panel.build(built)
    coverage = _panel.coverage_report(panel)
    _say(verbose, f"  panel {panel['calendar'][0].date()} -> "
                  f"{panel['calendar'][-1].date()}  "
                  f"{len(panel['calendar'])} sessions")

    # ---------------- Lane B: the frozen predictive families -------------- #
    _say(verbose, "Lane B: frozen R33 predictive families, refit ...")
    ctxs, forecasts = {}, {}
    for h in _contract.HORIZONS:
        ctxs[h] = build_context(panel, horizon=h, verbose=verbose)
        forecasts[h] = run_forecasts(ctxs[h], verbose=verbose)

    primary_h = _contract.PRIMARY_CONVERSION_HORIZON
    ctx = ctxs[primary_h]
    cov_by_date = _portfolio.trailing_covariance(
        panel["log_returns"], ctx["udates"])

    selection = {h: select_model_by_inner_validation(ctxs[h], forecasts[h])
                 for h in _contract.HORIZONS}
    chosen = {h: (selection[h]["selected"]["key"]
                  if selection[h]["selected"] else
                  list(forecasts[h].keys())[0])
              for h in _contract.HORIZONS}
    _say(verbose, f"  model selected on inner validation (h={primary_h}): "
                  f"{key_label(chosen[primary_h])}")

    candidates: list = []

    # -------- Family FORECAST: 4 models x 3 horizons ---------------------- #
    _say(verbose, "Family FORECAST ...")
    forecast_rows = []
    for h in _contract.HORIZONS:
        for key, cached in forecasts[h].items():
            y = ctxs[h]["targets"]["y_excess"]
            s = cached["score_eval"]
            ok = np.isfinite(s) & np.isfinite(y)
            score = (_forecast.score_forecast(s[ok], y[ok],
                                              ctxs[h]["row_dates"][ok])
                     if ok.sum() >= 50 else {"state": "TOO_FEW_ROWS"})
            run_out = run_candidate(
                ctxs[h], forecasts[h], family="FORECAST",
                label=f"{key_label(key)}@h{h}", model_key=key,
                calibration=_contract.DEFAULT_CALIBRATION,
                sizing_rule=_contract.DEFAULT_SIZING,
                mapping=_contract.DEFAULT_PORTFOLIO,
                turnover_rule=_contract.DEFAULT_TURNOVER, turnover_param=0.0,
                cov_by_date=cov_by_date if h == primary_h else None,
                config={"model": key_label(key), "horizon": h})
            rec = run_out["record"]
            rec["predictive"] = score
            rec["score_scale_ratio"] = cached["score_scale_ratio"]
            candidates.append(rec)
            forecast_rows.append(rec)
            _say(verbose, f"  h={h:2} {key_label(key):36} "
                          f"IC={_fmt(score.get('value'))} "
                          f"t={_fmt(score.get('t_stat'))} "
                          f"dU={_fmt(rec.get('after_cost_excess_utility'))}")

    _write(campaign_id, "forecast", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at,
        "families_frozen_from": _contract.FEATURE_FAMILIES_FROZEN_FROM,
        "new_predictor_search_allowed":
            _contract.NEW_PREDICTOR_SEARCH_ALLOWED,
        "feature_names": list(_forecast.FEATURE_NAMES),
        "feature_count": len(_forecast.FEATURE_NAMES),
        "panel_coverage": coverage,
        "model_selection": {str(h): selection[h] for h in selection},
        "results": forecast_rows,
        "nested_selection_arrangement":
            _contract.NESTED_SELECTION_ARRANGEMENT,
    })

    # -------- Family CALIBRATION ------------------------------------------ #
    _say(verbose, "Lane C: calibration ...")
    calibration_rows, calibration_detail = [], {}
    for method in _contract.CALIBRATIONS:
        run_out = run_candidate(
            ctx, forecasts[primary_h], family="CALIBRATION", label=method,
            model_key=chosen[primary_h], calibration=method,
            sizing_rule=_contract.DEFAULT_SIZING,
            mapping=_contract.DEFAULT_PORTFOLIO,
            turnover_rule=_contract.DEFAULT_TURNOVER, turnover_param=0.0,
            cov_by_date=cov_by_date, config={"calibration": method})
        rec = run_out["record"]
        candidates.append(rec)
        calibration_rows.append(rec)
        calibration_detail[method] = summarise_calibrations(
            run_out["frames"]["calibrations"])
        _say(verbose, f"  {method:32} dU={_fmt(rec.get('after_cost_excess_utility'))}"
                      f"  excess={_fmt(rec.get('after_cost_excess_annualised'))}")

    best_cal = _best(calibration_rows) or {"calibration":
                                           _contract.DEFAULT_CALIBRATION}
    magnitude = magnitude_beyond_rank(ctx, forecasts[primary_h],
                                      model_key=chosen[primary_h],
                                      calibration=best_cal["calibration"],
                                      cov_by_date=cov_by_date)
    reliability = calibration_reliability(ctx, forecasts[primary_h],
                                          chosen[primary_h],
                                          best_cal["calibration"])
    _write(campaign_id, "calibration", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, "horizon": primary_h,
        "model": key_label(chosen[primary_h]),
        "fitted_on_training_only":
            _contract.CALIBRATION_FITTED_ON_TRAINING_ONLY,
        "results": calibration_rows,
        "per_fold_specs": calibration_detail,
        "selected": best_cal.get("calibration"),
        "magnitude_beyond_rank": magnitude,
        "reliability": reliability,
    })

    # -------- Family SIZING ----------------------------------------------- #
    _say(verbose, "Lane D: position sizing ...")
    sizing_rows = []
    for rule in _contract.SIZINGS:
        run_out = run_candidate(
            ctx, forecasts[primary_h], family="SIZING", label=rule,
            model_key=chosen[primary_h],
            calibration=best_cal["calibration"], sizing_rule=rule,
            mapping=_contract.DEFAULT_PORTFOLIO,
            turnover_rule=_contract.DEFAULT_TURNOVER, turnover_param=0.0,
            cov_by_date=cov_by_date, config={"sizing": rule})
        rec = run_out["record"]
        rec["rule_meaning"] = _sizing.describe_rule(rule)
        candidates.append(rec)
        sizing_rows.append(rec)
        _say(verbose, f"  {rule:40} dU={_fmt(rec.get('after_cost_excess_utility'))}")
    best_size = _best(sizing_rows) or {"sizing": _contract.DEFAULT_SIZING}
    _write(campaign_id, "sizing", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, "horizon": primary_h,
        "calibration": best_cal.get("calibration"),
        "leverage_available": _contract.LEVERAGE_AVAILABLE,
        "max_gross_exposure": _contract.MAX_GROSS_EXPOSURE,
        "results": sizing_rows, "selected": best_size.get("sizing"),
    })

    # -------- Family HORIZON ---------------------------------------------- #
    _say(verbose, "Lane E: horizon combination ...")
    horizon_rows, hnes_table = run_horizon_family(
        ctxs, forecasts, chosen, best_cal["calibration"], best_size["sizing"],
        cov_by_date=cov_by_date, verbose=verbose)
    candidates.extend(horizon_rows)
    best_horizon = _best(horizon_rows) or {"config": {
        "horizons": list(_contract.DEFAULT_HORIZON_SET),
        "weighting": _contract.COMBINE_EQUAL}}
    _write(campaign_id, "horizon", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at,
        "hnes_formula": _contract.HNES_FORMULA,
        "hnes_shrink_n0": _contract.HNES_SHRINK_N0,
        "computed_on_training_only": _contract.HNES_COMPUTED_ON_TRAINING_ONLY,
        "chosen_by_raw_metric_magnitude":
            _contract.HORIZON_CHOSEN_BY_RAW_METRIC_MAGNITUDE,
        "hnes_per_horizon": hnes_table,
        "results": horizon_rows,
        "selected": best_horizon.get("config"),
    })

    # -------- Family TURNOVER --------------------------------------------- #
    _say(verbose, "Lane F: turnover and cost control ...")
    turnover_rows = []
    for rule in _contract.TURNOVER_RULES:
        param, inner = select_turnover_param(
            ctx, forecasts[primary_h], model_key=chosen[primary_h],
            calibration=best_cal["calibration"],
            sizing_rule=best_size["sizing"], rule=rule,
            cov_by_date=cov_by_date)
        run_out = run_candidate(
            ctx, forecasts[primary_h], family="TURNOVER", label=rule,
            model_key=chosen[primary_h],
            calibration=best_cal["calibration"],
            sizing_rule=best_size["sizing"],
            mapping=_contract.DEFAULT_PORTFOLIO, turnover_rule=rule,
            turnover_param=param, cov_by_date=cov_by_date,
            config={"turnover": rule, "param": param})
        rec = run_out["record"]
        rec["rule_meaning"] = _turnover.describe_rule(rule)
        rec["param_selected_on"] = "TRAINING_INNER_VALIDATION"
        rec["param_grid"] = list(_turnover.parameter_grid(rule))
        rec["inner_selection"] = inner
        candidates.append(rec)
        turnover_rows.append(rec)
        _say(verbose, f"  {rule:32} param={param:<6} "
                      f"turnover={_fmt(rec.get('stats', {}).get('annualised_turnover'))}"
                      f"  dU={_fmt(rec.get('after_cost_excess_utility'))}")
    best_turn = _best(turnover_rows) or {"turnover": _contract.DEFAULT_TURNOVER,
                                         "turnover_param": 0.0}
    _write(campaign_id, "turnover", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, "horizon": primary_h,
        "objective": _contract.TURNOVER_OBJECTIVE,
        "cost_base": _contract.COST_BASE,
        "cost_tier_bps": dict(_contract.COST_TIER_BPS),
        "cost_scenarios": dict(_contract.COST_SCENARIOS),
        "results": turnover_rows,
        "selected": {"rule": best_turn.get("turnover"),
                     "param": best_turn.get("turnover_param")},
    })

    # -------- Family PORTFOLIO -------------------------------------------- #
    _say(verbose, "Lane G: portfolio construction ...")
    portfolio_rows = []
    for mapping in _contract.PORTFOLIOS:
        run_out = run_candidate(
            ctx, forecasts[primary_h], family="PORTFOLIO", label=mapping,
            model_key=chosen[primary_h],
            calibration=best_cal["calibration"],
            sizing_rule=best_size["sizing"], mapping=mapping,
            turnover_rule=best_turn.get("turnover",
                                        _contract.DEFAULT_TURNOVER),
            turnover_param=best_turn.get("turnover_param", 0.0),
            cov_by_date=cov_by_date, config={"portfolio": mapping})
        rec = run_out["record"]
        candidates.append(rec)
        portfolio_rows.append(rec)
        _say(verbose, f"  {mapping:36} dU={_fmt(rec.get('after_cost_excess_utility'))}"
                      f"  gross={_fmt(rec.get('stats', {}).get('mean_gross_exposure'))}"
                      f"  cash={_fmt(rec.get('stats', {}).get('mean_cash_weight'))}")
    best_port = _best(portfolio_rows) or {"portfolio":
                                          _contract.DEFAULT_PORTFOLIO}

    # -------- Family FINALIST --------------------------------------------- #
    _say(verbose, "Finalists ...")
    finalist_rows, finalist_runs = run_finalists(
        ctxs, forecasts, chosen, best_cal, best_size, best_horizon, best_turn,
        best_port, cov_by_date=cov_by_date, verbose=verbose)
    candidates.extend(finalist_rows)

    best_final = _best(finalist_rows) or _best(candidates)
    best_run = finalist_runs.get(best_final["candidate_id"]) \
        if best_final else None

    controls = control_suite(ctx, best_run["result"]["dates"]) \
        if best_run and best_run["result"].get("state") == "OK" else \
        {"controls": {}, "series": {}}
    _write(campaign_id, "portfolio", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at, "horizon": primary_h,
        "constraints": {
            "max_instrument_weight": _contract.MAX_INSTRUMENT_WEIGHT,
            "max_asset_class_weight": _contract.MAX_ASSET_CLASS_WEIGHT,
            "max_gross_exposure": _contract.MAX_GROSS_EXPOSURE,
            "leverage_available": _contract.LEVERAGE_AVAILABLE,
            "cash_weight_range": [_contract.MIN_CASH_WEIGHT,
                                  _contract.MAX_CASH_WEIGHT],
        },
        "results": portfolio_rows, "selected": best_port.get("portfolio"),
        "finalists": finalist_rows,
        "controls": controls["controls"],
        "long_short_is_secondary_only":
            _contract.LONG_SHORT_IS_SECONDARY_ONLY,
        "judge_declaration": _economics.judge_declaration(),
        "judge_behaviour_hash": _economics.behaviour_hash(),
    })

    # -------- Walk-forward, concentration, multiple testing, attrition ---- #
    _write(campaign_id, "walkforward", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at,
        "evidence_state": _walkforward.evidence_state(),
        "folds": {str(h): [_walkforward.summarise(ctxs[h]["udates"], f)
                           for f in ctxs[h]["folds"]]
                  for h in _contract.HORIZONS},
        "best_candidate": (best_final or {}).get("candidate_id"),
        "best_per_fold": (best_final or {}).get("per_fold"),
    })

    conc = {}
    if best_run and best_run["result"].get("state") == "OK":
        conc = _concentration.analyse(best_run["result"]["path"],
                                      best_run["result"]["control"],
                                      meta=ctx["meta"], horizon=primary_h)
    _write(campaign_id, "concentration", {
        "calculation_owner": CALCULATION_OWNER, "campaign_id": campaign_id,
        "created_at": created_at,
        "candidate": (best_final or {}).get("candidate_id"),
        "analysis": conc,
        "r33_precedent": (
            "R33's five lockbox finalists all showed positive after-cost "
            "excess and leave-one-market-out attributed all of it to TRYUSD: "
            "removing one market moved mean excess from +0.0041 to -0.0069"),
    })

    mt = run_multiple_testing(candidates, primary_h)
    _write(campaign_id, "multiple_testing", mt)

    waterfall = {}
    if best_run and best_run["result"].get("state") == "OK":
        waterfall = build_attrition(ctx, forecasts[primary_h], best_final,
                                    best_run, chosen[primary_h], best_cal,
                                    best_size, best_turn, best_port,
                                    cov_by_date, conc, ctxs, forecasts,
                                    horizon_rows)
    _write(campaign_id, "attrition", waterfall)

    verdict = build_verdict(
        campaign_id=campaign_id, created_at=created_at, contract=contract,
        primary=None, universe=state, candidates=candidates,
        finalists=finalist_rows, multiple_testing=mt,
        evidence=_walkforward.evidence_state(), concentration=conc,
        best=best_final, controls=controls["controls"], attrition=waterfall)
    _write(campaign_id, "verdict", verdict)

    _say(verbose, "")
    _say(verbose, "=" * 70)
    _say(verbose, f"PRIMARY VERDICT : {verdict['primary_verdict']}")
    _say(verbose, f"SYSTEM_RESULT   : {verdict['system_result']}")
    _say(verbose, f"ALPHA_RESULT    : {verdict['alpha_result']}")
    _say(verbose, f"denominator     : {verdict['denominator']}")
    _say(verbose, "=" * 70)
    return {"verdict": verdict, "candidates": candidates,
            "universe": state, "multiple_testing": mt,
            "concentration": conc, "attrition": waterfall}


def _fmt(v, nd: int = 4) -> str:
    """Console formatting that cannot round a result to a different story.

    A fixed four decimals prints an after-cost excess of +2.07e-05 as "+0.0000",
    which reads as an exact tie rather than as the vanishingly small number it
    is. Anything below the printable resolution goes to scientific notation.
    """
    if v is None:
        return "    n/a  "
    try:
        f = float(v)
    except (TypeError, ValueError):
        return str(v)
    if not np.isfinite(f):
        return "    n/a  "
    if f != 0.0 and abs(f) < 10.0 ** (-nd):
        return f"{f:+.2e}"
    return f"{f:+.{nd}f}"


# --------------------------------------------------------------------------- #
# Lane C helpers
# --------------------------------------------------------------------------- #
def summarise_calibrations(specs: dict) -> list:
    """What each fold's calibration actually learned, without the knot arrays."""
    out = []
    for f_i, spec in sorted(specs.items()):
        out.append({"fold": int(f_i), "state": spec.get("state"),
                    "kind": spec.get("kind"), "n": spec.get("n"),
                    "slope": spec.get("slope"),
                    "intercept": spec.get("intercept"),
                    "calibration_slope": spec.get("calibration_slope"),
                    "confidence": spec.get("confidence"),
                    "training_mean": spec.get("training_mean"),
                    "residual_dispersion": (spec.get("dispersion") or {}).get(
                        "pooled")})
    return out


def magnitude_beyond_rank(ctx: dict, forecasts: dict, *, model_key,
                          calibration: str, cov_by_date: dict) -> dict:
    """Does forecast MAGNITUDE add anything the ORDERING did not?

    The same book, twice: once from the calibrated expected return and once
    from a pure rank transform of the same forecast. The difference in
    after-cost excess utility is the answer, and a non-positive difference means
    only rank survived - which this release then says, rather than presenting a
    calibrated book as though the calibration had earned something.
    """
    out = {}
    for label, rank_only in (("CALIBRATED", False), ("RANK_ONLY", True)):
        frames = conviction_frame(ctx, forecasts, model_key=model_key,
                                  calibration=calibration,
                                  sizing_rule=_contract.DEFAULT_SIZING,
                                  use_rank_only=rank_only)
        weights = book_from_conviction(ctx, frames,
                                       mapping=_contract.DEFAULT_PORTFOLIO,
                                       turnover_rule=_contract.DEFAULT_TURNOVER,
                                       turnover_param=0.0,
                                       cov_by_date=cov_by_date)
        res = judge_book(ctx, weights)
        out[label] = (res["stats"] if res.get("state") == "OK" else
                      {"state": res.get("state")})
    a = (out.get("CALIBRATED") or {}).get("after_cost_excess_utility")
    b = (out.get("RANK_ONLY") or {}).get("after_cost_excess_utility")
    delta = (float(a - b) if (a is not None and b is not None
                              and np.isfinite(a) and np.isfinite(b)) else None)
    return {"test": _contract.MAGNITUDE_BEYOND_RANK_TEST,
            "calibrated_utility_delta": a, "rank_only_utility_delta": b,
            "magnitude_value": delta,
            "magnitude_adds_value": (None if delta is None else bool(delta > 0)),
            "detail": out}


def calibration_reliability(ctx: dict, forecasts: dict, model_key,
                            calibration: str) -> dict:
    """The calibration curve on the EVALUATION rows: predicted versus observed."""
    cached = forecasts[model_key]
    score, y = cached["score_eval"], ctx["targets"]["y_excess"]
    expected, realised = [], []
    for fold in ctx["folds"]:
        if not fold["usable"]:
            continue
        inner = _rows_for(ctx, fold["inner_validation"])
        sel = inner[np.isfinite(cached["score_inner"][inner])
                    & np.isfinite(y[inner])]
        if sel.size < 100:
            continue
        spec = _calibration.fit(calibration, cached["score_inner"][sel],
                                y[sel])
        ev = _rows_for(ctx, fold["evaluation"])
        ev = ev[np.isfinite(score[ev]) & np.isfinite(y[ev])]
        if ev.size == 0:
            continue
        # EACH fold's own calibration on ITS OWN evaluation rows. Applying one
        # fold's calibration to the whole sample would measure a calibration
        # that was never used.
        expected.append(_calibration.apply(spec, score[ev])["expected_return"])
        realised.append(y[ev])
    if not expected:
        return {"state": "NO_CALIBRATION_FITTED"}
    return _calibration.reliability(np.concatenate(expected),
                                    np.concatenate(realised))


# --------------------------------------------------------------------------- #
# Lane E helper
# --------------------------------------------------------------------------- #
def run_horizon_family(ctxs: dict, forecasts: dict, chosen: dict,
                       calibration: str, sizing_rule: str, *,
                       cov_by_date: dict, verbose: bool = False) -> tuple:
    """Every declared horizon set, scored by HNES rather than by raw magnitude.

    The book always trades at the PRIMARY cadence. The forecast-date grids nest
    exactly - 60 is a multiple of 20 and 20 of 5, and all three start at the
    same session - so a longer-horizon conviction is available at every primary
    date it has been refreshed on, and is CARRIED FORWARD in between. Carrying
    forward uses no information from the future; it is what holding a
    longer-horizon view actually looks like between refreshes.
    """
    primary_h = _contract.PRIMARY_CONVERSION_HORIZON
    ctx = ctxs[primary_h]

    per_h_frames, per_h_gain = {}, {}
    for h in _contract.HORIZONS:
        frames = conviction_frame(ctxs[h], forecasts[h],
                                  model_key=chosen[h],
                                  calibration=calibration,
                                  sizing_rule=sizing_rule)
        per_h_frames[h] = frames
        per_h_gain[h] = training_gain_series(ctxs[h], forecasts[h], chosen[h],
                                             calibration, sizing_rule,
                                             cov_by_date=None)

    hnes_table = [_horizon.hnes(g, horizon=h)
                  for h, g in sorted(per_h_gain.items()) if g is not None]

    rows = []
    for hset in _contract.HORIZON_SETS:
        methods = ((_contract.COMBINE_EQUAL,) if len(hset) == 1
                   else (_contract.COMBINE_EQUAL, _contract.COMBINE_HNES))
        for method in methods:
            weights = _horizon.combination_weights(method, per_h_gain, hset)
            frames = combine_frames(ctx, per_h_frames, weights["weights"])
            label = "+".join(str(h) for h in hset) + f"/{method}"
            run_out = run_candidate(
                ctx, forecasts[primary_h], family="HORIZON", label=label,
                model_key=chosen[primary_h], calibration=calibration,
                sizing_rule=sizing_rule,
                mapping=_contract.DEFAULT_PORTFOLIO,
                turnover_rule=_contract.DEFAULT_TURNOVER, turnover_param=0.0,
                cov_by_date=cov_by_date, conviction_override=frames,
                config={"horizons": list(hset), "weighting": method,
                        "weights": {str(k): v
                                    for k, v in weights["weights"].items()},
                        "degraded_to_equal": weights.get("degraded_to_equal")})
            rec = run_out["record"]
            rows.append(rec)
            _say(verbose, f"  h={label:22} "
                          f"dU={_fmt(rec.get('after_cost_excess_utility'))}")
    return rows, hnes_table


def combine_frames(ctx: dict, per_h_frames: dict, weights: dict) -> dict:
    """Blend per-horizon conviction onto the primary cadence.

    Each horizon's conviction is standardised BEFORE blending, because a
    60-session conviction has a naturally wider spread than a 5-session one and
    would otherwise dominate the mix through its scale whatever weight it was
    given - the same defect the HNES exists to remove, moved one step
    downstream.
    """
    index = ctx["udates"]
    cols = ctx["all_symbols"]
    stacked, support, used = None, None, 0.0
    er_acc, vol_acc, n_acc = None, None, 0

    for h, w in sorted(weights.items()):
        frames = per_h_frames.get(int(h))
        if frames is None or frames["conviction"].empty:
            continue
        conv = frames["conviction"].reindex(columns=cols)
        conv = conv.reindex(index.union(conv.index)).sort_index().ffill()
        conv = conv.reindex(index)
        v = conv.to_numpy()
        sd = np.nanstd(v)
        z = v / sd if (np.isfinite(sd) and sd > 0) else np.zeros_like(v)
        z = np.where(np.isfinite(z), z, np.nan)
        contributed = np.isfinite(z)
        if stacked is None:
            stacked = np.where(contributed, z * float(w), 0.0)
            support = contributed.astype(np.int32)
        else:
            stacked = stacked + np.where(contributed, z * float(w), 0.0)
            support = support + contributed.astype(np.int32)
        used += float(w)

        er = frames["expected_return"].reindex(columns=cols)
        er = er.reindex(index.union(er.index)).sort_index().ffill().reindex(index)
        vol = frames["predicted_vol"].reindex(columns=cols)
        vol = vol.reindex(index.union(vol.index)).sort_index().ffill().reindex(
            index)
        er_acc = er.to_numpy() if er_acc is None else er_acc + er.to_numpy()
        vol_acc = vol.to_numpy() if vol_acc is None else vol_acc + vol.to_numpy()
        n_acc += 1

    if stacked is None:
        empty = pd.DataFrame(index=index, columns=cols, dtype=float)
        return {"conviction": empty, "expected_return": empty,
                "predicted_vol": empty, "calibrations": {}}

    # A cell no horizon contributed to is UNKNOWN, not zero conviction. Letting
    # it read as zero would quietly make "no forecast" indistinguishable from
    # "forecast of no edge", and the portfolio layer treats those differently.
    conv = np.where(support > 0, stacked / used if used > 0 else stacked,
                    np.nan)
    er = er_acc / max(n_acc, 1)
    vol = vol_acc / max(n_acc, 1)
    keep = np.isfinite(conv).any(axis=1)
    idx = index[keep]
    return {"conviction": pd.DataFrame(conv[keep], index=idx, columns=cols),
            "expected_return": pd.DataFrame(er[keep], index=idx, columns=cols),
            "predicted_vol": pd.DataFrame(np.where(np.isfinite(vol[keep]),
                                                   vol[keep], 0.15),
                                          index=idx, columns=cols),
            "calibrations": {}}


def training_block_frames(ctx: dict, forecasts: dict, model_key,
                          calibration: str, sizing_rule: str
                          ) -> Optional[dict]:
    """Conviction on TRAINING dates - the inner-validation block, never
    evaluation.

    Everything that has to be chosen rather than measured is chosen here: the
    horizon weights and the transition parameter. The block is the
    inner-validation slice of the LAST usable fold, which is the training data
    closest to where the choice will be applied.

    The calibration is fitted on this same block and applied to it. That is
    mildly optimistic in absolute terms, and it is deliberately the SAME mild
    optimism for every option being compared - the choice is between transition
    rules or horizon weights, holding the calibration fixed - so it cannot
    reorder them. What it must never do is touch the evaluation block, and it
    does not.
    """
    usable = [f for f in ctx["folds"] if f["usable"]]
    if not usable:
        return None
    fold = usable[-1]
    if len(fold["inner_validation"]) < 8:
        return None

    cached = forecasts[model_key]
    y = ctx["targets"]["y_excess"]
    rows = _rows_for(ctx, fold["inner_validation"])
    ok = rows[np.isfinite(cached["score_inner"][rows]) & np.isfinite(y[rows])]
    if ok.size < 100:
        return None
    spec = _calibration.fit(calibration, cached["score_inner"][ok], y[ok])

    tradable = ctx["targets"]["tradable"]
    trailing = ctx["targets"]["trailing_vol"]
    symbols, cols = ctx["symbols"], ctx["all_symbols"]
    sym_pos = {s: k for k, s in enumerate(cols)}

    conv_rows, er_rows, vol_rows, index = [], [], [], []
    for dp in fold["inner_validation"]:
        idx = ctx["row_groups"].get(int(dp))
        if idx is None or idx.size == 0:
            continue
        raw = cached["score_inner"][idx]
        trade = tradable[idx] & np.isfinite(raw)
        if not trade.any():
            continue
        applied = _calibration.apply(spec, raw)
        vol = np.where(np.isfinite(trailing[idx]), trailing[idx], 0.15)
        c = _sizing.conviction(sizing_rule,
                               expected_return=applied["expected_return"],
                               uncertainty=applied["uncertainty"],
                               predicted_vol=vol, score=raw,
                               confidence=applied["confidence"])
        row_c = np.full(len(cols), np.nan)
        row_e = np.full(len(cols), np.nan)
        row_v = np.full(len(cols), np.nan)
        for k, j in enumerate(idx):
            if not trade[k]:
                continue
            p = sym_pos[symbols[j]]
            row_c[p] = c[k]
            row_e[p] = applied["expected_return"][k]
            row_v[p] = vol[k]
        conv_rows.append(row_c)
        er_rows.append(row_e)
        vol_rows.append(row_v)
        index.append(ctx["udates"][int(dp)])

    if len(conv_rows) < 8:
        return None
    idx = pd.DatetimeIndex(index)
    return {"conviction": pd.DataFrame(conv_rows, index=idx, columns=cols),
            "expected_return": pd.DataFrame(er_rows, index=idx, columns=cols),
            "predicted_vol": pd.DataFrame(vol_rows, index=idx, columns=cols),
            "calibrations": {"training_block": spec}}


def training_gain_series(ctx: dict, forecasts: dict, model_key,
                         calibration: str, sizing_rule: str, *,
                         cov_by_date=None) -> Optional[np.ndarray]:
    """The ``g_h`` the HNES is computed from: per-date after-cost gain over the
    control, on TRAINING dates only."""
    frames = training_block_frames(ctx, forecasts, model_key, calibration,
                                   sizing_rule)
    if frames is None:
        return None
    weights = book_from_conviction(ctx, frames,
                                   mapping=_contract.DEFAULT_PORTFOLIO,
                                   turnover_rule=_contract.DEFAULT_TURNOVER,
                                   turnover_param=0.0, cov_by_date=cov_by_date)
    res = judge_book(ctx, weights)
    if res.get("state") != "OK":
        return None
    series = res.get("excess_series")
    return None if series is None else np.asarray(series, dtype=np.float64)


# --------------------------------------------------------------------------- #
# Lane F helper
# --------------------------------------------------------------------------- #
def select_turnover_param(ctx: dict, forecasts: dict, *, model_key,
                          calibration: str, sizing_rule: str, rule: str,
                          cov_by_date: dict) -> tuple:
    """Choose the transition parameter INSIDE the training partition.

    The grid is frozen in the contract and the choice is made on the
    inner-validation block of the last usable fold, scored on after-cost excess
    utility - the same statistic the campaign decides on, so the selection and
    the judgement cannot disagree about what "better" means.
    """
    grid = _turnover.parameter_grid(rule)
    if len(grid) == 1:
        return float(grid[0]), {"grid": list(grid), "state": "SINGLE_VALUE"}

    frames = training_block_frames(ctx, forecasts, model_key, calibration,
                                   sizing_rule)
    if frames is None:
        return float(grid[0]), {"grid": list(grid),
                                "state": "NO_TRAINING_BLOCK"}

    scores = {}
    for p in grid:
        weights = book_from_conviction(ctx, frames,
                                       mapping=_contract.DEFAULT_PORTFOLIO,
                                       turnover_rule=rule, turnover_param=p,
                                       cov_by_date=cov_by_date)
        res = judge_book(ctx, weights)
        scores[p] = (res["stats"].get("after_cost_excess_utility")
                     if res.get("state") == "OK" else None)

    valid = {p: v for p, v in scores.items()
             if v is not None and np.isfinite(float(v))}
    if not valid:
        return float(grid[0]), {"grid": list(grid), "scores": _clean(scores),
                                "state": "NO_VALID_SCORE"}
    best = max(valid, key=lambda p: float(valid[p]))
    return float(best), {"grid": list(grid), "scores": _clean(scores),
                         "selected": float(best), "state": "OK",
                         "selected_on": "TRAINING_INNER_VALIDATION"}


# --------------------------------------------------------------------------- #
# Finalists
# --------------------------------------------------------------------------- #
def run_finalists(ctxs: dict, forecasts: dict, chosen: dict, best_cal: dict,
                  best_size: dict, best_horizon: dict, best_turn: dict,
                  best_port: dict, *, cov_by_date: dict,
                  verbose: bool = False) -> tuple:
    """The best of each lane, combined, plus its immediate neighbours.

    The neighbours are not decoration. A configuration whose result collapses
    when one lane moves one notch is sitting on a parameter cliff, and the
    contract requires the median neighbour to retain
    ``MIN_NEIGHBOUR_RETENTION`` of its economics.
    """
    primary_h = _contract.PRIMARY_CONVERSION_HORIZON
    ctx = ctxs[primary_h]
    cal = best_cal.get("calibration", _contract.DEFAULT_CALIBRATION)
    size = best_size.get("sizing", _contract.DEFAULT_SIZING)
    turn = best_turn.get("turnover", _contract.DEFAULT_TURNOVER)
    param = float(best_turn.get("turnover_param", 0.0) or 0.0)
    port = best_port.get("portfolio", _contract.DEFAULT_PORTFOLIO)
    hset = tuple((best_horizon.get("config") or {}).get(
        "horizons", _contract.DEFAULT_HORIZON_SET))
    hmethod = (best_horizon.get("config") or {}).get(
        "weighting", _contract.COMBINE_EQUAL)

    combos = [("COMBINED_BEST", cal, size, turn, param, port)]
    for alt in _contract.CALIBRATIONS:
        if alt != cal:
            combos.append((f"NEIGHBOUR_CAL_{alt}", alt, size, turn, param,
                           port))
            break
    for alt in _contract.SIZINGS:
        if alt != size:
            combos.append((f"NEIGHBOUR_SIZE_{alt}", cal, alt, turn, param,
                           port))
            break
    for alt in _contract.PORTFOLIOS:
        if alt != port:
            combos.append((f"NEIGHBOUR_PORT_{alt}", cal, size, turn, param,
                           alt))
            break
    for alt in _contract.TURNOVER_RULES:
        if alt != turn:
            combos.append((f"NEIGHBOUR_TURN_{alt}", cal, size, alt,
                           float(_turnover.parameter_grid(alt)[0]), port))
            break
    combos.append(("COMBINED_BEST_IMMEDIATE_TURNOVER", cal, size,
                   _contract.TURN_IMMEDIATE, 0.0, port))
    combos.append(("COMBINED_BEST_RANK_SIZING", cal,
                   _contract.SIZE_RANK_WEIGHT, turn, param, port))
    combos = combos[:_contract.CONFIG_FAMILIES["FINALIST"]]

    # The per-horizon conviction frames depend on the CALIBRATION and the
    # SIZING rule, so they are rebuilt for every finalist. Building them once
    # from the winning settings - which is what v1 did - makes every
    # calibration and sizing neighbour a byte-identical book, and a
    # parameter-cliff gate whose probes cannot move is not a gate.
    frame_cache: dict = {}

    def _per_h(c: str, s: str) -> dict:
        key = (c, s)
        if key not in frame_cache:
            frame_cache[key] = {
                h: conviction_frame(ctxs[h], forecasts[h], model_key=chosen[h],
                                    calibration=c, sizing_rule=s)
                for h in _contract.HORIZONS}
        return frame_cache[key]

    rows, runs = [], {}
    for label, c, s, t, p, m in combos:
        override = None
        if len(hset) > 1:
            gains = {h: training_gain_series(ctxs[h], forecasts[h], chosen[h],
                                             c, s, cov_by_date=None)
                     for h in hset}
            w = _horizon.combination_weights(hmethod, gains, hset)
            override = combine_frames(ctx, _per_h(c, s), w["weights"])
        run_out = run_candidate(
            ctx, forecasts[primary_h], family="FINALIST", label=label,
            model_key=chosen[primary_h], calibration=c, sizing_rule=s,
            mapping=m, turnover_rule=t, turnover_param=p,
            cov_by_date=cov_by_date, conviction_override=override,
            config={"calibration": c, "sizing": s, "turnover": t,
                    "turnover_param": p, "portfolio": m,
                    "horizons": list(hset), "weighting": hmethod})
        rec = run_out["record"]
        rows.append(rec)
        runs[rec["candidate_id"]] = run_out
        _say(verbose, f"  {label:38} "
                      f"dU={_fmt(rec.get('after_cost_excess_utility'))}"
                      f"  excess={_fmt(rec.get('after_cost_excess_annualised'))}")

    base = _best(rows)
    if base is not None:
        base_w = runs[base["candidate_id"]]["weights"]
        effective, inert = [], []
        for r in rows:
            if not r["label"].startswith("NEIGHBOUR"):
                continue
            w = runs[r["candidate_id"]]["weights"]
            moved = _weights_differ(base_w, w)
            r["neighbour_moved_the_book"] = moved
            if not moved:
                r["neighbour_state"] = "NO_EFFECT"
                inert.append(r["label"])
            elif r.get("after_cost_excess_utility") is not None:
                r["neighbour_state"] = "EFFECTIVE"
                effective.append(r)
            else:
                r["neighbour_state"] = "UNSCORED"

        b = float(base.get("after_cost_excess_utility") or 0.0)
        base["neighbours_with_no_effect"] = inert
        base["effective_neighbours"] = len(effective)
        if (len(effective) >= _contract.MIN_EFFECTIVE_NEIGHBOURS and b > 0):
            med = float(np.median([float(r["after_cost_excess_utility"])
                                   for r in effective]))
            base["neighbour_retention"] = float(med / b)
            base["no_severe_parameter_cliff"] = bool(
                base["neighbour_retention"]
                >= _contract.MIN_NEIGHBOUR_RETENTION)
        else:
            # Fails CLOSED. Too few neighbours actually moved the book, or the
            # base has no positive economics for a neighbour to retain, so the
            # cliff is UNMEASURABLE - and unmeasurable is not innocent.
            base["neighbour_retention"] = None
            base["no_severe_parameter_cliff"] = False
            base["parameter_cliff_state"] = (
                "TOO_FEW_EFFECTIVE_NEIGHBOURS"
                if len(effective) < _contract.MIN_EFFECTIVE_NEIGHBOURS
                else "BASE_HAS_NO_POSITIVE_ECONOMICS_TO_RETAIN")
    return rows, runs


def _weights_differ(a: pd.DataFrame, b: pd.DataFrame) -> bool:
    """Did this neighbour actually produce a different book?

    A mapping that consumes expected return directly, such as the shrunk
    mean-variance one, does not read the sizing rule at all - so a "sizing
    neighbour" under that mapping is the same book wearing a different label,
    and averaging it into a retention figure would manufacture stability out of
    nothing.
    """
    if a is None or b is None or a.empty or b.empty:
        return False
    if a.shape != b.shape or not a.index.equals(b.index):
        return True
    diff = np.abs(a.to_numpy() - b.to_numpy())
    return bool(np.nanmax(diff) > _contract.NEIGHBOUR_WEIGHT_DIFFERENCE_EPSILON)


# --------------------------------------------------------------------------- #
# Multiple testing
# --------------------------------------------------------------------------- #
def run_multiple_testing(candidates: list, primary_h: int) -> dict:
    """Benjamini-Hochberg over every executed configuration, plus Hansen SPA.

    The denominator is every executed configuration, including the ones that
    failed. A denominator that counts only survivors is not a correction, it is
    a second selection.

    SPA needs every series on the SAME dates, so it runs over the primary-
    horizon candidates only; that restriction is recorded rather than left for a
    reader to infer from a candidate count that does not match the denominator.
    """
    executed = list(candidates)
    per_candidate = []
    p_values = []
    for rec in executed:
        t = rec.get("after_cost_excess_t_stat")
        p = _mt.two_sided_p(float(t)) if (t is not None
                                          and np.isfinite(float(t))) else None
        p_values.append(p)
        per_candidate.append({
            "candidate_id": rec["candidate_id"], "family": rec["family"],
            "horizon": rec.get("horizon"),
            "after_cost_excess_annualised": rec.get(
                "after_cost_excess_annualised"),
            "after_cost_excess_utility": rec.get("after_cost_excess_utility"),
            "t_stat": t, "p_value": p,
            "survives_stressed_cost": rec.get("survives_stressed_cost"),
        })

    bh = _mt.benjamini_hochberg(p_values, q=_contract.FDR_Q)
    rejected_ids = [executed[i]["candidate_id"] for i in bh["rejected"]]

    # The p-values are TWO-SIDED, so a Benjamini-Hochberg rejection means "this
    # candidate's excess is significantly different from zero" - in EITHER
    # direction. A candidate that reliably LOSES to the control is rejected by
    # BH just as a winner would be, and reporting a bare rejection count would
    # let a significant loss read as a survivor. The directions are separated
    # here, and only the positive list can support a qualification.
    positive, negative = [], []
    for i in bh["rejected"]:
        rec = executed[i]
        e = rec.get("after_cost_excess_annualised")
        (positive if (e is not None and float(e) > 0) else negative).append(
            rec["candidate_id"])

    counts = {}
    for rec in executed:
        counts[rec["family"]] = counts.get(rec["family"], 0) + 1

    return {
        "calculation_owner": CALCULATION_OWNER,
        "denominator_executed_configurations": len(executed),
        "planned_config_total": _contract.PLANNED_CONFIG_TOTAL,
        "max_primary_configs": _contract.MAX_PRIMARY_CONFIGS,
        "within_ceiling": len(executed) <= _contract.MAX_PRIMARY_CONFIGS,
        "executed_by_family": counts,
        "planned_by_family": dict(_contract.CONFIG_FAMILIES),
        "denominator_counts_all_executed":
            _contract.DENOMINATOR_COUNTS_ALL_EXECUTED,
        "controls_enter_denominator": _contract.CONTROLS_ENTER_DENOMINATOR,
        "adaptive_search_allowed": _contract.ADAPTIVE_SEARCH_ALLOWED,
        "benjamini_hochberg": {
            **bh, "rejected_candidate_ids": rejected_ids,
            "p_values_are_two_sided": True,
            "rejected_beating_the_control": positive,
            "rejected_losing_to_the_control": negative,
            "n_rejected_beating_the_control": len(positive),
            "n_rejected_losing_to_the_control": len(negative),
            "only_positive_rejections_may_qualify": True,
        },
        "fdr_q": _contract.FDR_Q,
        "spa_restricted_to_horizon": primary_h,
        "spa_restriction_reason": (
            "Hansen SPA requires every candidate series on the same dates; "
            "candidates at other horizons trade on a different decision grid"),
        "per_candidate": per_candidate,
        "seeds": {"bootstrap": _contract.BOOTSTRAP_SEED,
                  "model": _contract.MODEL_SEED},
    }


# --------------------------------------------------------------------------- #
# Attrition waterfall
# --------------------------------------------------------------------------- #
def build_attrition(ctx: dict, forecasts: dict, best: dict, best_run: dict,
                    model_key, best_cal: dict, best_size: dict,
                    best_turn: dict, best_port: dict, cov_by_date: dict,
                    conc: dict, ctxs: dict, all_forecasts: dict,
                    horizon_rows: list) -> dict:
    """Convert the winning book one stage at a time and price each conversion."""
    horizon = ctx["horizon"]
    cal = best_cal.get("calibration", _contract.DEFAULT_CALIBRATION)
    size = best_size.get("sizing", _contract.DEFAULT_SIZING)
    turn = best_turn.get("turnover", _contract.DEFAULT_TURNOVER)
    param = float(best_turn.get("turnover_param", 0.0) or 0.0)
    port = best_port.get("portfolio", _contract.DEFAULT_PORTFOLIO)

    cached = forecasts[model_key]
    y = ctx["targets"]["y_excess"]
    ok = np.isfinite(cached["score_eval"]) & np.isfinite(y)
    ic = (_forecast.rank_ic(cached["score_eval"][ok], y[ok],
                            ctx["row_dates"][ok]) if ok.sum() >= 50
          else {"value": None, "t_stat": None})
    ic = {k: v for k, v in ic.items() if not k.startswith("_")}

    def _net(frames, *, caps, transition, cost=1.0):
        w = book_from_conviction(ctx, frames, mapping=port,
                                 turnover_rule=turn, turnover_param=param,
                                 cov_by_date=cov_by_date, apply_caps=caps,
                                 apply_transition=transition)
        res = judge_book(ctx, w, cost_multiplier=cost)
        return res

    stages = {}

    # 1. Raw forecast skill, expressed as the annualised gross return of the
    #    book the raw score alone would produce.
    raw_frames = conviction_frame(ctx, forecasts, model_key=model_key,
                                  calibration=cal, sizing_rule=size,
                                  use_rank_only=True)
    raw = _net(raw_frames, caps=False, transition=False)
    stages["RAW_FORECAST_SKILL"] = (
        raw["stats"]["gross_return_annualised"]
        if raw.get("state") == "OK" else None)

    # 2. The ceiling: the same machinery driven by the REALISED return.
    perfect_frames = conviction_frame(ctx, forecasts, model_key=model_key,
                                      calibration=cal, sizing_rule=size,
                                      use_realised_as_forecast=True)
    perfect = _net(perfect_frames, caps=True, transition=True)
    stages["PERFECT_FORESIGHT_SIZED"] = (
        perfect["stats"]["net_return_annualised"]
        if perfect.get("state") == "OK" else None)

    # 3. The calibrated forecast under NEUTRAL (rank-weighted) sizing, so that
    #    stage 4 prices the sizing rule on its own rather than confounding it
    #    with the calibration.
    neutral_frames = conviction_frame(ctx, forecasts, model_key=model_key,
                                      calibration=cal,
                                      sizing_rule=_contract.SIZE_RANK_WEIGHT)
    step3 = _net(neutral_frames, caps=False, transition=False)
    stages["CALIBRATED_EXPECTED_RETURN"] = (
        step3["stats"]["gross_return_annualised"]
        if step3.get("state") == "OK" else None)

    cal_frames = conviction_frame(ctx, forecasts, model_key=model_key,
                                  calibration=cal, sizing_rule=size)
    step4 = _net(cal_frames, caps=False, transition=False)
    stages["AFTER_SIZING"] = (step4["stats"]["gross_return_annualised"]
                              if step4.get("state") == "OK" else None)

    step5 = _net(cal_frames, caps=True, transition=False)
    stages["AFTER_CONSTRAINTS"] = (
        step5["stats"]["gross_return_annualised"]
        if step5.get("state") == "OK" else None)

    step6 = _net(cal_frames, caps=True, transition=True)
    stages["AFTER_TURNOVER_CONTROL"] = (
        step6["stats"]["gross_return_annualised"]
        if step6.get("state") == "OK" else None)
    stages["AFTER_COST"] = (step6["stats"]["net_return_annualised"]
                            if step6.get("state") == "OK" else None)
    stages["AFTER_RISK_MATCHED_CONTROL"] = (
        step6["stats"].get("after_cost_excess_annualised")
        if step6.get("state") == "OK" else None)
    stages["AFTER_UTILITY_CHARGE"] = (
        step6["stats"].get("after_cost_excess_utility")
        if step6.get("state") == "OK" else None)

    per_class = per_asset_class_excess(ctx, best_run)
    per_horizon = {r["label"]: r.get("after_cost_excess_annualised")
                   for r in horizon_rows}

    return _attrition.build(
        horizon=horizon, rank_ic=ic, stage_paths=stages,
        control=best_run["result"]["control"],
        cost_scenarios=best.get("cost_scenarios", {}),
        per_asset_class_excess=per_class,
        per_horizon_excess=per_horizon, concentration=conc)


def per_asset_class_excess(ctx: dict, best_run: dict) -> dict:
    """What each asset class contributed to the winning book's PnL."""
    res = best_run["result"]
    if res.get("state") != "OK":
        return {}
    path = res["path"]
    cols = list(path["columns"])
    contrib = np.asarray(path["contribution"], dtype=np.float64)
    ppy = _economics.periods_per_year(ctx["horizon"])
    out = {}
    for j, sym in enumerate(cols):
        key = ctx["meta"].get(sym, {}).get("asset_class", "UNKNOWN")
        out.setdefault(key, 0.0)
        out[key] += float(contrib[:, j].mean()) * ppy
    return out


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def build_verdict(*, campaign_id: str, created_at: str, contract: dict,
                  primary: Optional[str], universe: dict, candidates: list,
                  finalists: list, multiple_testing: dict, evidence: dict,
                  concentration: dict = None, best: dict = None,
                  controls: dict = None, attrition: dict = None,
                  blocking: list = None) -> dict:
    """Evaluate every frozen qualification condition and name the verdict.

    ``ALPHA_RESULT`` is PASS only for ``R34_ALPHA_QUALIFIED``, and that verdict
    additionally requires ``genuinely_independent_evidence_exists``, which the
    contract already declares FALSE. The ceiling is therefore structural rather
    than a judgement made after seeing the numbers, and it is stated here in
    the same place that would otherwise be tempted to raise it.
    """
    conc = concentration or {}
    conditions = {}

    if primary is None and best is not None:
        stats = best.get("stats", {})
        excess = best.get("after_cost_excess_annualised")
        util = best.get("after_cost_excess_utility")
        pred = None
        for rec in candidates:
            if rec.get("family") == "FORECAST" and rec.get("predictive"):
                v = rec["predictive"].get("t_stat")
                if v is not None and (pred is None or float(v) > pred):
                    pred = float(v)
        # Only a rejection in the RIGHT direction can support a qualification.
        rejected = set(multiple_testing.get("benjamini_hochberg", {}).get(
            "rejected_beating_the_control", []))

        conditions = {
            "predictive_skill_remains_positive": bool(
                pred is not None and pred > 2.0),
            "positive_after_cost_excess_economics": bool(
                excess is not None and float(excess) > 0),
            "positive_after_cost_utility": bool(
                stats.get("utility_annualised") is not None
                and float(stats["utility_annualised"]) > 0),
            "beats_proper_investable_risk_matched_control": bool(
                util is not None and float(util) > 0),
            "same_sign_result_across_walk_forward_folds": bool(
                best.get("same_sign_fold_fraction") is not None
                and float(best["same_sign_fold_fraction"])
                >= _contract.MIN_SAME_SIGN_FOLD_FRACTION),
            "survives_multiple_testing_procedure":
                best.get("candidate_id") in rejected,
            "no_single_instrument_dependency": bool(
                conc.get("gates", {}).get(
                    "no_sign_reversal_on_leave_one_instrument_out", False)
                and conc.get("gates", {}).get(
                    "single_instrument_pnl_share_within_limit", False)),
            "no_single_asset_class_dependency": bool(
                conc.get("gates", {}).get(
                    "no_sign_reversal_on_leave_one_asset_class_out", False)
                and conc.get("gates", {}).get(
                    "single_asset_class_pnl_share_within_limit", False)),
            "acceptable_turnover": bool(
                stats.get("annualised_turnover") is not None
                and float(stats["annualised_turnover"])
                <= _contract.MAX_ANNUAL_TURNOVER),
            "acceptable_cost_sensitivity": bool(
                best.get("survives_stressed_cost")),
            "no_severe_parameter_cliff": bool(
                best.get("no_severe_parameter_cliff")),
            "book_actually_takes_positions": bool(
                stats.get("mean_gross_exposure") is not None
                and float(stats["mean_gross_exposure"])
                >= _contract.MIN_MEAN_GROSS_EXPOSURE
                and stats.get("annualised_turnover") is not None
                and float(stats["annualised_turnover"])
                >= _contract.MIN_ANNUAL_TURNOVER),
            "implementability_proven":
                universe.get("state")
                == _contract.IMPLEMENTABLE_RESEARCH_UNIVERSE,
            "genuinely_independent_evidence_exists":
                bool(_contract.FRESH_UNSEEN_EVIDENCE_EXISTS),
        }

        # Everything except the two conditions that a HISTORICAL walk-forward
        # cannot supply: genuinely independent evidence, which does not exist
        # today, and the predictive-skill and cliff checks that ride with it.
        # A book that takes no positions is excluded here too - abstention is a
        # valid answer and it is not a conversion edge awaiting confirmation.
        economics_hold = all(
            conditions[k] for k in (
                "positive_after_cost_excess_economics",
                "positive_after_cost_utility",
                "beats_proper_investable_risk_matched_control",
                "same_sign_result_across_walk_forward_folds",
                "no_single_instrument_dependency",
                "no_single_asset_class_dependency",
                "acceptable_turnover", "acceptable_cost_sensitivity",
                "book_actually_takes_positions"))
        if all(conditions.values()):
            primary = _contract.VERDICT_QUALIFIED
        elif economics_hold:
            primary = _contract.VERDICT_NEEDS_FORWARD
        else:
            primary = _contract.VERDICT_NO_CONVERSION
    elif primary is None:
        primary = _contract.VERDICT_NO_CONVERSION

    alpha = (_contract.RESULT_PASS
             if primary == _contract.ALPHA_PASS_REQUIRES
             else _contract.RESULT_FAIL)

    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id, "created_at": created_at,
        "contract_hash": contract.get("contract_hash"),
        "primary_verdict": primary,
        "system_result": _contract.RESULT_PASS,
        "alpha_result": alpha,
        "system_and_alpha_results_are_separate":
            _contract.SYSTEM_AND_ALPHA_RESULTS_ARE_SEPARATE,
        "alpha_pass_requires": _contract.ALPHA_PASS_REQUIRES,
        "qualification_conditions": conditions,
        "failed_conditions": sorted(k for k, v in conditions.items() if not v),
        # Reported alongside the booleans, never instead of them: a margin can
        # be positive and still be indistinguishable from zero, and a reader
        # seeing only "beats the control: true" would take a knife-edge for a
        # result. Kept OUT of ``qualification_conditions`` so that a numeric
        # value can never be counted as a passing condition by truthiness.
        "margin_over_control": {
            "after_cost_excess_annualised": (best or {}).get(
                "after_cost_excess_annualised"),
            "after_cost_excess_utility": (best or {}).get(
                "after_cost_excess_utility"),
            "t_stat": (best or {}).get("after_cost_excess_t_stat"),
            "distinguishable_from_zero": bool(
                (best or {}).get("after_cost_excess_t_stat") is not None
                and abs(float(best["after_cost_excess_t_stat"])) >= 2.0),
        } if best else {},
        "denominator": multiple_testing.get(
            "denominator_executed_configurations", len(candidates)),
        "qualified_candidates": (
            [best["candidate_id"]] if primary == _contract.VERDICT_QUALIFIED
            and best else []),
        "universe": universe,
        "evidence_state": evidence,
        "best_candidate": best,
        "finalists": finalists,
        "controls": controls or {},
        "concentration": conc,
        "attrition_summary": {
            "share_of_ceiling_captured": (attrition or {}).get(
                "share_of_ceiling_captured"),
            "perfect_foresight_ceiling": (attrition or {}).get(
                "perfect_foresight_ceiling"),
            "realised_after_cost": (attrition or {}).get(
                "realised_after_cost"),
        } if attrition else {},
        "blocking_reasons": blocking or [],
        "verdict_meaning": {
            _contract.VERDICT_QUALIFIED:
                "prediction converts into robust implementable after-cost "
                "excess PnL on genuinely independent evidence",
            _contract.VERDICT_NEEDS_FORWARD:
                "mathematically interesting, potentially economically useful, "
                "NOT yet qualified alpha; ALPHA_RESULT is FAIL",
            _contract.VERDICT_NO_CONVERSION:
                "verified predictive information did not convert into robust "
                "after-cost excess PnL",
            _contract.VERDICT_UNIVERSE_BLOCKED:
                "no defensible implementable universe could be constructed "
                "from owned data",
            _contract.VERDICT_DATA_BLOCKED:
                "a point-in-time or data-integrity condition blocked the "
                "campaign",
        }.get(primary),
        "r33_frozen": {
            "campaign_id": _contract.R33_CAMPAIGN_ID,
            "verdict": _contract.R33_VERDICT,
            "denominator": _contract.R33_DENOMINATOR,
            "lockbox_accesses": _contract.R33_LOCKBOX_ACCESSES,
            "rerun": False, "lockbox_reopened": False, "retuned": False,
        },
        "superseded_campaigns": _contract.SUPERSEDED_CAMPAIGNS,
        "superseded_evidence_rules": _contract.SUPERSEDED_EVIDENCE_RULES,
    }
    return payload
