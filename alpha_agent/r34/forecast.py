"""alpha_agent.r34.forecast - the FROZEN Release 33 predictive families, refit.

Release 34 does not search for predictors. Release 33 already established, over
105 bounded configurations, that this estate's binding constraint is information
rather than method, and that a wider search buys data-mining risk instead of
knowledge. What changed is the INSTRUMENT DOMAIN - the panel is now 47
exchange-traded funds on total-return prices instead of 66 indices and spot
currencies - so the same families are REFIT on the new returns and nothing else.

The feature families are imported from :mod:`alpha_agent.r33.features` and the
learners from :mod:`alpha_agent.r33.models`. There is exactly one
implementation of each in this repository and this module adds none.

Three details decide whether a forecast produced here means anything:

**Standardisation uses training statistics only.** Scaling an evaluation block
by its own mean and standard deviation tells the model something about the
distribution of the period it is being tested on.

**The elastic net is fitted on a centred target.** The released
``fit_elastic_net`` deliberately has no intercept, because Release 31's target
was cross-sectionally demeaned by construction. This release predicts RAW excess
returns, whose unconditional mean is not zero, so the target is centred on the
TRAINING mean before fitting and that same training mean is added back at
prediction time. Fitting a no-intercept model on an uncentred target would push
the whole level into the slopes.

**The volatility model is a RISK input, not a return candidate.** Release 33
measured genuine QLIKE skill and none of it converted on its own. Here a
variance forecast earns its place by sizing positions or it does not appear in
the release at all.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from ..r33 import features as _r33_features
from ..r33 import models as _r33_models
from ..r33 import predictive as _r33_predictive
from . import contract as _contract
from . import panel as _panel

CALCULATION_OWNER = "alpha_agent.r34.forecast"

#: Re-exported so the rest of the release has ONE name for each.
FEATURE_NAMES = _r33_features.FEATURE_NAMES
rank_ic = _r33_predictive.rank_ic
per_date_rank_ic = _r33_predictive.per_date_rank_ic
oos_r2 = _r33_predictive.oos_r2
qlike_skill = _r33_predictive.qlike_skill
newey_west_t = _r33_predictive.newey_west_t


def design(panel: dict, *, horizon: int) -> dict:
    """The frozen R33 design matrix, computed on the R34 panel."""
    return _r33_features.design_matrix(_panel.r33_feature_panel(panel),
                                       horizon=horizon)


def align_targets(panel: dict, design_rows: dict, *, horizon: int) -> dict:
    """Attach the excess return, realised volatility and tradability per row.

    The design matrix drops rows with no finite feature at all, so the row set
    is a subset of ``dates x instruments``. Everything downstream is aligned by
    the (date, symbol) IDENTITY rather than by position, because a positional
    join that silently slips by one row would be undetectable and fatal.
    """
    excess = _panel.observation_returns(panel, horizon=horizon)
    realised = _panel.realised_volatility(panel, horizon=horizon)
    tradable = _panel.tradable_frame(panel, horizon=horizon)
    trailing = _panel.trailing_volatility(panel, excess.index)

    dates = pd.DatetimeIndex(design_rows["date"])
    syms = np.asarray(design_rows["symbol"])
    col = {s: k for k, s in enumerate(excess.columns)}
    row = {d: k for k, d in enumerate(excess.index)}

    def _pick(frame):
        values = frame.to_numpy()
        out = np.full(len(dates), np.nan)
        for k in range(len(dates)):
            i = row.get(dates[k])
            j = col.get(syms[k])
            if i is not None and j is not None:
                out[k] = values[i, j]
        return out

    trade = np.zeros(len(dates), dtype=bool)
    tvalues = tradable.to_numpy()
    for k in range(len(dates)):
        i, j = row.get(dates[k]), col.get(syms[k])
        if i is not None and j is not None:
            trade[k] = bool(tvalues[i, j])

    return {"y_excess": _pick(excess),
            "y_realised_vol": _pick(realised),
            "trailing_vol": _pick(trailing),
            "tradable": trade,
            "excess_frame": excess,
            "tradable_frame": tradable,
            "trailing_vol_frame": trailing}


# --------------------------------------------------------------------------- #
# The frozen model families
# --------------------------------------------------------------------------- #
def model_configs() -> list:
    """Every frozen forecast configuration, enumerated from the contract."""
    out = []
    out.append({"model": _contract.MODEL_TSMOM, "params": {}})
    for a in _contract.RIDGE_ALPHAS:
        out.append({"model": _contract.MODEL_RIDGE, "params": {"alpha": a}})
    for a in _contract.ELASTIC_NET_ALPHAS:
        out.append({"model": _contract.MODEL_ELASTIC_NET,
                    "params": {"alpha": a,
                               "l1_ratio": _contract.ELASTIC_NET_L1_RATIO}})
    for s in _contract.HIERARCHICAL_SHRINK:
        out.append({"model": _contract.MODEL_HIERARCHICAL,
                    "params": {"alpha": _contract.RIDGE_ALPHAS[0],
                               "shrink": s}})
    return out


def fit(model: str, params: dict, X: np.ndarray, y: np.ndarray,
        groups: np.ndarray, *, feature_names) -> dict:
    """Fit one frozen family on TRAINING rows and return a JSON-able spec."""
    ok = np.isfinite(y)
    Xf, yf, gf = X[ok], y[ok], np.asarray(groups)[ok]
    if Xf.shape[0] < 50:
        return {"kind": _r33_models.KIND_ZERO, "state": "INSUFFICIENT_ROWS",
                "scaler": None, "model": model}

    if model == _contract.MODEL_TSMOM:
        spec = _r33_models.fit_transparent_rule(
            rule=_r33_models.RULE_TSMOM, feature_names=list(feature_names),
            scale=float(np.nanstd(yf)) if np.isfinite(np.nanstd(yf)) else 1.0)
        return {"spec": spec, "scaler": None, "model": model,
                "centre": 0.0, "n": int(Xf.shape[0])}

    scaler = _r33_models.fit_scaler(Xf)
    Z = _r33_models.apply_scaler(scaler, Xf)

    if model == _contract.MODEL_RIDGE:
        spec = _r33_models.fit_linear(Z, yf, alpha=float(params["alpha"]))
        centre = 0.0
    elif model == _contract.MODEL_ELASTIC_NET:
        centre = float(np.mean(yf))
        spec = _r33_models.fit_elastic_net(
            Z, yf - centre, alpha=float(params["alpha"]),
            l1_ratio=float(params["l1_ratio"]))
    elif model == _contract.MODEL_HIERARCHICAL:
        spec = _r33_models.fit_hierarchical(
            Z, yf, gf, alpha=float(params["alpha"]),
            shrink=float(params["shrink"]))
        centre = 0.0
    else:
        raise ValueError("unknown frozen model family %r" % (model,))
    return {"spec": spec, "scaler": scaler, "model": model,
            "centre": float(centre), "n": int(Xf.shape[0])}


def predict(fitted: dict, X: np.ndarray, groups: np.ndarray, *,
            feature_names) -> np.ndarray:
    """Apply a fitted spec to evaluation rows."""
    if fitted.get("spec") is None:
        return np.zeros(len(X))
    scaler = fitted.get("scaler")
    Z = _r33_models.apply_scaler(scaler, X) if scaler else np.asarray(
        X, dtype=np.float64)
    out = _r33_models.predict(fitted["spec"], Z, groups=groups,
                              feature_names=list(feature_names))
    return np.asarray(out, dtype=np.float64) + float(fitted.get("centre", 0.0))


# --------------------------------------------------------------------------- #
# Volatility - a RISK input
# --------------------------------------------------------------------------- #
def fit_volatility(X: np.ndarray, y_vol: np.ndarray, *, feature_names) -> dict:
    """Fit the HAR log-volatility model on TRAINING rows."""
    ok = np.isfinite(y_vol) & (y_vol > 0)
    if ok.sum() < 100:
        return {"spec": None, "model": _r33_models.VOL_TRAILING}
    spec = _r33_models.fit_volatility(
        X[ok], y_vol[ok], model=_r33_models.VOL_HAR,
        feature_names=list(feature_names))
    return {"spec": spec, "model": _r33_models.VOL_HAR}


def predict_volatility(fitted: dict, X: np.ndarray, *, trailing: np.ndarray
                       ) -> np.ndarray:
    """Forecast volatility, degrading to the trailing estimate when unfitted.

    The degradation is recorded rather than silent: a missing model becomes the
    pre-registered TRAILING_REALISED baseline, which is exactly what the
    contract names as the volatility baseline.
    """
    if fitted.get("spec") is None:
        out = np.asarray(trailing, dtype=np.float64)
    else:
        out = _r33_models.predict(fitted["spec"], np.asarray(X, np.float64))
    out = np.where(np.isfinite(out) & (out > 1e-4), out,
                   np.asarray(trailing, dtype=np.float64))
    return np.clip(np.where(np.isfinite(out), out, 0.15), 0.01, 2.0)


# --------------------------------------------------------------------------- #
# Predictive scoring
# --------------------------------------------------------------------------- #
def score_forecast(yhat: np.ndarray, y: np.ndarray, dates) -> dict:
    """Cross-sectional rank IC plus its Newey-West inference.

    Rank IC is the primary predictive statistic of this release because the
    conversion question begins with ordering: a book that cannot rank
    instruments cannot allocate between them, whatever its R-squared.
    """
    res = _r33_predictive.rank_ic(yhat, y, dates)
    out = {k: v for k, v in res.items() if not k.startswith("_")}
    ok = np.isfinite(np.asarray(yhat, float)) & np.isfinite(np.asarray(y, float))
    if ok.sum() >= 8:
        a, b = np.asarray(yhat, float)[ok], np.asarray(y, float)[ok]
        sse_model = float(np.sum((b - a) ** 2))
        sse_base = float(np.sum((b - float(np.mean(b))) ** 2))
        out["oos_r2_vs_realised_mean"] = (
            float(1.0 - sse_model / sse_base) if sse_base > 0 else None)
        out["forecast_dispersion"] = float(np.std(a, ddof=1))
        out["realised_dispersion"] = float(np.std(b, ddof=1))
    return out
