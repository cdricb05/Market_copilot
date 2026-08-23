"""alpha_agent.r39.trade_space - the TRADE_SPACE_REGISTRY owner.

Alpha is not assumed to be "predict one asset, buy that asset". This module
turns a prediction column into an ECONOMIC EXPRESSION - a dated weight path
over instruments - and prices it: every expression is observable at decision
time, implementable, charged on traded notional at each instrument's
declared cost, and paired with its declared control.

Expressions:

* ``TS_OUTRIGHT``       - sign-of-prediction outright book, equal weight
                          across the scope, versus the vol-matched passive
                          equal-weight basket of the SAME scope.
* ``XS_LONG_SHORT``     - long the top tercile, short the bottom tercile of
                          predictions within the scope, self-financed,
                          versus risk-matched cash (zero).
* ``CALENDAR_CURVE``    - front-versus-second-contract book (futures only),
                          direction from the prediction, versus cash.
* ``GROUP_SPREAD``      - within-economic-group pair book: long the
                          highest-prediction member, short the lowest, per
                          group with >= 2 valid members, versus cash.
* ``REGIME_CONDITIONAL``- any base expression gated by an observable regime
                          column (weights zeroed outside the regime).
* ``ABSTAIN_OVERLAY``   - any base expression with weights zeroed where
                          |prediction| is below a declared cost hurdle; the
                          engine is allowed to learn that NO TRADE is best.

There is no unconstrained pair explosion: GROUP_SPREAD pairs exist only
inside declared economic groups, and every expression is registered.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.trade_space"
SCHEMA = "r39_trade_space_registry/1"
ARTIFACT_NAME = "trade_space_registry.json"

TOP_FRACTION = 1.0 / 3.0


def _pivot(panel: pd.DataFrame, col: str) -> pd.DataFrame:
    return panel.pivot_table(index="decision_date", columns="market_id",
                             values=col, aggfunc="last")


def _book_path(weights: pd.DataFrame, fwd: pd.DataFrame,
               cost_bps: pd.Series, *, cost_multiplier: float = 1.0) -> dict:
    """Weights + forward returns -> after-cost book path.

    Cost is charged on traded notional (|dw|, buys AND sells) at each
    instrument's per-side rate - the Release-31 lesson, kept.
    """
    W = weights.fillna(0.0)
    R = fwd.reindex(index=W.index, columns=W.columns)
    contrib = W.to_numpy() * np.where(np.isfinite(R.to_numpy()),
                                      R.to_numpy(), 0.0)
    gross = contrib.sum(axis=1)
    rates = np.asarray([float(cost_bps.get(c, 10.0)) / 1e4
                        for c in W.columns]) * float(cost_multiplier)
    Wn = W.to_numpy()
    prev = np.zeros(Wn.shape[1])
    costs = np.empty(Wn.shape[0])
    traded = np.empty(Wn.shape[0])
    for k in range(Wn.shape[0]):
        dw = np.abs(Wn[k] - prev)
        traded[k] = dw.sum()
        costs[k] = float((dw * rates).sum())
        prev = Wn[k]
    net = gross - costs
    return {"dates": W.index, "gross": gross, "net": net, "costs": costs,
            "traded_notional": traded, "weights": W,
            "gross_exposure": np.abs(Wn).sum(axis=1)}


def ts_outright(pred: pd.DataFrame, fwd: pd.DataFrame, cost_bps: pd.Series,
                *, cost_multiplier: float = 1.0,
                gate: pd.DataFrame = None,
                hurdle: pd.DataFrame = None) -> dict:
    """Sign-of-prediction outright book, equal weight over valid names."""
    P = pred.copy()
    if hurdle is not None:
        P = P.where(P.abs() >= hurdle.reindex_like(P), 0.0)
    if gate is not None:
        P = P.where(gate.reindex_like(P).fillna(False), 0.0)
    sign = np.sign(P.where(np.isfinite(P)))
    n = sign.abs().sum(axis=1).replace(0.0, np.nan)
    W = sign.div(n, axis=0).fillna(0.0)
    out = _book_path(W, fwd, cost_bps, cost_multiplier=cost_multiplier)
    out["expression"] = "TS_OUTRIGHT"
    return out


def passive_ew_control(fwd: pd.DataFrame, cost_bps: pd.Series,
                       *, cost_multiplier: float = 1.0) -> dict:
    """The passive equal-weight basket of the same scope: same dates, same
    instruments, same cost model, none of the timing."""
    valid = np.isfinite(fwd)
    n = valid.sum(axis=1).replace(0, np.nan)
    W = valid.astype(float).div(n, axis=0).fillna(0.0)
    out = _book_path(W, fwd, cost_bps, cost_multiplier=cost_multiplier)
    out["expression"] = "PASSIVE_EW_CONTROL"
    return out


def xs_long_short(pred: pd.DataFrame, fwd: pd.DataFrame, cost_bps: pd.Series,
                  *, top_fraction: float = TOP_FRACTION,
                  cost_multiplier: float = 1.0,
                  gate: pd.DataFrame = None) -> dict:
    """Long top tercile, short bottom tercile of predictions, per date."""
    P = pred.where(np.isfinite(pred) & np.isfinite(fwd))
    if gate is not None:
        P = P.where(gate.reindex_like(P).fillna(False))
    ranks = P.rank(axis=1, pct=True)
    long = (ranks >= 1.0 - top_fraction)
    short = (ranks <= top_fraction)
    nl = long.sum(axis=1).replace(0, np.nan)
    ns = short.sum(axis=1).replace(0, np.nan)
    W = long.astype(float).div(2.0 * nl, axis=0).fillna(0.0) \
        - short.astype(float).div(2.0 * ns, axis=0).fillna(0.0)
    # a cross-section needs a cross-section
    enough = (long.sum(axis=1) >= 2) & (short.sum(axis=1) >= 2)
    W.loc[~enough] = 0.0
    out = _book_path(W, fwd, cost_bps, cost_multiplier=cost_multiplier)
    out["expression"] = "XS_LONG_SHORT"
    return out


def calendar_curve(pred: pd.Series, f1: pd.Series, f2: pd.Series,
                   cost_bps: float, *, cost_multiplier: float = 1.0) -> dict:
    """Front-vs-second book on ONE market: +1 front / -1 second when the
    prediction is positive, reversed when negative."""
    idx = pred.index
    direction = np.sign(pred.reindex(idx))
    spread = (f1.reindex(idx) - f2.reindex(idx))
    valid = np.isfinite(spread) & np.isfinite(direction)
    w = direction.where(valid, 0.0)
    gross = (w * spread.fillna(0.0)).to_numpy(dtype=float)
    dw = np.abs(np.diff(np.concatenate([[0.0], w.to_numpy(dtype=float)])))
    # two legs trade
    costs = 2.0 * dw * float(cost_bps) / 1e4 * float(cost_multiplier)
    return {"dates": idx, "gross": gross, "net": gross - costs,
            "costs": costs, "traded_notional": 2.0 * dw,
            "weights": pd.DataFrame({"F1_minus_F2": w}),
            "gross_exposure": np.abs(w.to_numpy(dtype=float)) * 2.0,
            "expression": "CALENDAR_CURVE"}


def group_spread(pred: pd.DataFrame, fwd: pd.DataFrame, cost_bps: pd.Series,
                 groups: dict, *, cost_multiplier: float = 1.0) -> dict:
    """Within each declared economic group: long the best-prediction member,
    short the worst, equal risk budget per group."""
    W = pd.DataFrame(0.0, index=pred.index, columns=pred.columns)
    live_groups = []
    for gname, members in groups.items():
        cols = [c for c in members if c in pred.columns]
        if len(cols) < 2:
            continue
        live_groups.append(gname)
        sub = pred[cols].where(np.isfinite(fwd[cols]))
        ok = sub.notna().sum(axis=1) >= 2
        sub = sub[ok]  # pandas 3: idxmax raises on all-NaN rows
        best = sub.idxmax(axis=1)
        worst = sub.idxmin(axis=1)
        for dt in sub.index:
            b, w_ = best.loc[dt], worst.loc[dt]
            if isinstance(b, str) and isinstance(w_, str) and b != w_:
                W.loc[dt, b] += 0.5
                W.loc[dt, w_] -= 0.5
    ng = max(len(live_groups), 1)
    W = W / ng
    out = _book_path(W, fwd, cost_bps, cost_multiplier=cost_multiplier)
    out["expression"] = "GROUP_SPREAD"
    out["groups_used"] = live_groups
    return out


EXPRESSION_CONTROLS = {
    "TS_OUTRIGHT": "VOL_MATCHED_PASSIVE_EW_SAME_SCOPE",
    "XS_LONG_SHORT": "RISK_MATCHED_CASH",
    "CALENDAR_CURVE": "RISK_MATCHED_CASH",
    "GROUP_SPREAD": "RISK_MATCHED_CASH",
    "REGIME_CONDITIONAL": "CONTROL_OF_THE_BASE_EXPRESSION",
    "ABSTAIN_OVERLAY": "CONTROL_OF_THE_BASE_EXPRESSION",
}


def build_registry(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "expressions": {
            name: {"control": ctrl,
                   "costed_on": C.COST_BASE,
                   "cost_state": C.COST_MODEL_STATE}
            for name, ctrl in EXPRESSION_CONTROLS.items()},
        "top_fraction": TOP_FRACTION,
        "pair_explosion_bounded": C.NO_UNCONSTRAINED_PAIR_EXPLOSION,
        "every_expression_is_costed": C.EVERY_EXPRESSION_IS_COSTED,
        "every_expression_has_a_control": C.EVERY_EXPRESSION_HAS_A_CONTROL,
    })
    body["trade_space_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
