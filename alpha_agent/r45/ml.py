"""alpha_agent.r45.ml - Tracks H and I. State conditioning, then bounded models.

The contract forbids starting here, and for a good reason: a model fitted on
an effect that is not there will find one anyway. So this module runs last,
it fits on zone A, it CHOOSES on zone B, and it is judged on zone C, which it
sees exactly once.

The bar every model has to clear is not "positive". It is:

    beat the frozen transparent rule, on zone C, after the same costs.

A model that is merely less bad than a losing rule has not added economic
value, and this module says so in those words.

Every state variable is causal by construction - each is computed from bars
strictly before the entry decision. The shock itself is a legitimate feature:
entry is at +5 minutes and the shock is measured to +5 minutes, so it is
known when the position is taken.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import bars as B
from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.ml"

SEED = 45_000_045
#: Declared before fitting. Nothing is added later.
STATE_VARIABLES = (
    "abs_shock_bps", "pre_event_vol_bps", "shock_over_pre_vol",
    "trailing_5d_vol_bps", "pre_event_range_bps", "half_spread_bps",
)
MODEL_FAMILIES = ("RIDGE", "LOGISTIC", "GRADIENT_BOOSTING", "RANDOM_FOREST")


# --------------------------------------------------------------------------- #
# Track H - state
# --------------------------------------------------------------------------- #
def add_state(symbol: str, ev: pd.DataFrame) -> pd.DataFrame:
    """Pre-event state, every column computed from bars before the entry."""
    df = B.panel(symbol)
    if df is None or ev is None:
        return None
    idx, px = df.index, df["close"]
    logp = np.log(px)
    r1 = logp.diff()
    tol = ES._tolerance_min(df)

    pre_vol, pre_rng, tr_vol = [], [], []
    for _, r in ev.iterrows():
        t0 = r["stamp_utc"]
        lo = idx.searchsorted(t0 - pd.Timedelta(minutes=30), side="left")
        hi = idx.searchsorted(t0 - pd.Timedelta(minutes=1), side="right")
        win = r1.iloc[lo:hi].to_numpy(dtype=float)
        win = win[np.isfinite(win)]
        pre_vol.append(float(np.std(win, ddof=1) * 1e4)
                       if win.size > 5 else np.nan)
        pw = px.iloc[lo:hi].to_numpy(dtype=float)
        pw = pw[np.isfinite(pw) & (pw > 0)]
        pre_rng.append(float((pw.max() / pw.min() - 1.0) * 1e4)
                       if pw.size > 5 else np.nan)
        dlo = idx.searchsorted(t0 - pd.Timedelta(days=5), side="left")
        dw = r1.iloc[dlo:hi].to_numpy(dtype=float)
        dw = dw[np.isfinite(dw)]
        tr_vol.append(float(np.std(dw, ddof=1) * 1e4)
                      if dw.size > 100 else np.nan)

    out = ev.copy()
    out.attrs.update(ev.attrs)
    out["abs_shock_bps"] = out["shock"].abs() * 1e4
    out["pre_event_vol_bps"] = pre_vol
    out["pre_event_range_bps"] = pre_rng
    out["trailing_5d_vol_bps"] = tr_vol
    out["half_spread_bps"] = out["half_in_bps"]
    out["shock_over_pre_vol"] = (out["abs_shock_bps"]
                                 / out["pre_event_vol_bps"].replace(0, np.nan))
    out["_tol"] = tol
    return out


def state_conditioning(symbol: str, stamps, *, zone: str = "BC",
                       charge=None) -> dict:
    """Does the effect live in a particular pre-event state?"""
    ev = ES.event_book(symbol, stamps)
    if ev is None:
        return {"state": "NO_EVENTS"}
    ev = add_state(symbol, ev)
    sub = ES.slice_zone(ev, zone) if zone else ev
    rows, charged = [], []
    for var in STATE_VARIABLES:
        v = pd.to_numeric(sub[var], errors="coerce")
        ok = sub[np.isfinite(v)].copy()
        ok.attrs.update(sub.attrs)
        if len(ok) < 90:
            continue
        vv = pd.to_numeric(ok[var], errors="coerce")
        q = vv.quantile([0.0, 1 / 3, 2 / 3, 1.0]).to_numpy()
        cells = []
        for i, (lo, hi) in enumerate(zip(q[:-1], q[1:])):
            sel = (vv >= lo) & (vv <= hi) if i == 2 else (vv >= lo) & (vv < hi)
            g = ok[sel].copy()
            g.attrs.update(ok.attrs)
            if len(g) < 25:
                continue
            card = ES.score(g, label=f"{var}_T{i + 1}")
            cells.append({"tercile": i + 1, "range": [float(lo), float(hi)],
                          "n_events": card["n_events"],
                          "net_bps_per_event": card["net_bps_per_event"],
                          "net_t_cluster": card["net_t_cluster"],
                          "hit_rate": card["hit_rate"]})
        if not cells:
            continue
        best = max(cells, key=lambda c: c["net_bps_per_event"])
        rows.append({"state_variable": var, "zone": zone, "cells": cells,
                     "best_tercile": best["tercile"],
                     "best_net_bps": best["net_bps_per_event"],
                     "best_net_t": best["net_t_cluster"],
                     "monotone": bool(
                         np.all(np.diff([c["net_bps_per_event"]
                                         for c in cells]) > 0)
                         or np.all(np.diff([c["net_bps_per_event"]
                                            for c in cells]) < 0))})
        if charge is not None:
            charged.append(charge(
                {"lane": "STATE_CONDITIONING", "symbol": symbol,
                 "state_variable": var, "zone": zone, "buckets": 3},
                family="EVENT_STATE_CONDITIONING", lane="L10_STATE",
                label=f"{symbol} conditioned on {var}"))
    rows.sort(key=lambda r: -(r["best_net_t"] or -9))
    return {"state": "MEASURED", "symbol": symbol, "zone": zone,
            "n_state_variables": len(rows), "rows": rows,
            "burden_charged": charged,
            "warning": "the BEST tercile of the BEST variable is a maximum "
                       "over a search and is charged as one trial per "
                       "variable; it is not a finding"}


# --------------------------------------------------------------------------- #
# Track I - bounded models
# --------------------------------------------------------------------------- #
def _design(ev: pd.DataFrame):
    X = ev[list(STATE_VARIABLES)].apply(pd.to_numeric, errors="coerce")
    X["signed_shock_bps"] = ev["shock"] * 1e4
    fam = pd.get_dummies(ev["event"].astype(str), prefix="ev")
    X = pd.concat([X.reset_index(drop=True), fam.reset_index(drop=True)],
                  axis=1)
    ok = np.isfinite(X.to_numpy(dtype=float)).all(axis=1)
    return X[ok].to_numpy(dtype=float), ok, list(X.columns)


def run_models(symbol: str, stamps, *, charge=None) -> dict:
    ev = ES.event_book(symbol, stamps)
    if ev is None:
        return {"state": "NO_EVENTS"}
    ev = add_state(symbol, ev)
    X, ok, cols = _design(ev)
    ev = ev[ok].copy()
    ev.attrs.update(ES.event_book(symbol, stamps).attrs)
    gross, cost, net = ES.net_series(ev)

    z = ES.zone_of(ev)
    d = pd.to_datetime(ev["date"])
    a_end = pd.Timestamp(z["a_range"][1])
    b_end = pd.Timestamp(z["b_range"][1])
    fit = (d <= a_end).to_numpy()
    sel = ((d > a_end) & (d <= b_end)).to_numpy()
    jdg = (d > b_end).to_numpy()
    if fit.sum() < 120 or sel.sum() < 60 or jdg.sum() < 60:
        return {"state": "DATA_INSUFFICIENT",
                "n": [int(fit.sum()), int(sel.sum()), int(jdg.sum())]}

    mu, sd = X[fit].mean(axis=0), X[fit].std(axis=0)
    sd[sd == 0] = 1.0
    Xs = (X - mu) / sd
    y_ret = net
    y_cls = (net > 0).astype(int)

    try:
        from sklearn.ensemble import (GradientBoostingRegressor,
                                      RandomForestRegressor)
        from sklearn.linear_model import LogisticRegression, Ridge
    except Exception as exc:                            # pragma: no cover
        return {"state": "IRREPARABLE_TECHNICAL_FAILURE",
                "error": str(exc)[:160]}

    def _positions(pred, kind):
        if kind == "prob":
            return np.where(pred > 0.5, 1.0, 0.0)
        return np.where(pred > 0.0, 1.0, 0.0)

    results = {}
    for name in MODEL_FAMILIES:
        try:
            if name == "RIDGE":
                m = Ridge(alpha=10.0, random_state=None)
                m.fit(Xs[fit], y_ret[fit])
                p_sel, p_jdg = m.predict(Xs[sel]), m.predict(Xs[jdg])
                kind = "ret"
            elif name == "LOGISTIC":
                m = LogisticRegression(max_iter=2000, C=0.1)
                m.fit(Xs[fit], y_cls[fit])
                p_sel = m.predict_proba(Xs[sel])[:, 1]
                p_jdg = m.predict_proba(Xs[jdg])[:, 1]
                kind = "prob"
            elif name == "GRADIENT_BOOSTING":
                m = GradientBoostingRegressor(
                    n_estimators=120, max_depth=2, learning_rate=0.05,
                    subsample=0.8, random_state=SEED)
                m.fit(Xs[fit], y_ret[fit])
                p_sel, p_jdg = m.predict(Xs[sel]), m.predict(Xs[jdg])
                kind = "ret"
            else:
                m = RandomForestRegressor(
                    n_estimators=250, max_depth=4, min_samples_leaf=20,
                    random_state=SEED, n_jobs=1)
                m.fit(Xs[fit], y_ret[fit])
                p_sel, p_jdg = m.predict(Xs[sel]), m.predict(Xs[jdg])
                kind = "ret"
        except Exception as exc:                        # pragma: no cover
            results[name] = {"state": "ERROR", "error": str(exc)[:160]}
            continue

        pos_sel, pos_jdg = _positions(p_sel, kind), _positions(p_jdg, kind)
        sel_net = float(np.mean(pos_sel * net[sel]) * 1e4)
        jdg_net = float(np.mean(pos_jdg * net[jdg]) * 1e4)
        cl = ES.cluster_t(pos_jdg * net[jdg], ev["date"].to_numpy()[jdg])
        results[name] = {
            "state": "MEASURED",
            "select_zone_b_net_bps": sel_net,
            "judge_zone_c_net_bps": jdg_net,
            "judge_zone_c_net_t_cluster": cl.get("t"),
            "judge_participation": float(np.mean(pos_jdg)),
            "n_judge": int(jdg.sum()),
        }
        if charge is not None:
            charge({"lane": "EVENT_ML", "symbol": symbol, "model": name,
                    "features": sorted(cols)},
                   family="EVENT_ML", lane="L11_ML", label=f"{symbol}/{name}")

    base_sel = float(np.mean(net[sel]) * 1e4)
    base_jdg = float(np.mean(net[jdg]) * 1e4)
    base_cl = ES.cluster_t(net[jdg], ev["date"].to_numpy()[jdg])
    measured = {k: v for k, v in results.items()
                if v.get("state") == "MEASURED"}
    chosen = (max(measured, key=lambda k: measured[k]["select_zone_b_net_bps"])
              if measured else None)
    beats = (chosen is not None
             and measured[chosen]["judge_zone_c_net_bps"] > base_jdg
             and measured[chosen]["judge_zone_c_net_bps"] > 0)
    return {
        "state": "EXECUTED", "symbol": symbol,
        "features": cols, "seed": SEED,
        "zones": {"fit_n": int(fit.sum()), "select_n": int(sel.sum()),
                  "judge_n": int(jdg.sum())},
        "frozen_rule_baseline": {
            "select_zone_b_net_bps": base_sel,
            "judge_zone_c_net_bps": base_jdg,
            "judge_zone_c_net_t_cluster": base_cl.get("t")},
        "models": results,
        "model_chosen_on_zone_b": chosen,
        "chosen_beats_frozen_rule_on_zone_c": bool(beats),
        "ML_ADDED_ECONOMIC_VALUE": bool(beats),
        "protocol": "fit on A, choose on B, judged once on C",
    }


def run(symbol: str = None, stamps=None, *, charge=None,
        gate_open: bool = True) -> dict:
    symbol = symbol or C.FROZEN_RULE["instrument_of_origin"]
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "H+I", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    out = {"track": "H+I", "state": "EXECUTED",
           "calculation_owner": CALCULATION_OWNER, "symbol": symbol,
           "gate_open": bool(gate_open),
           "gate_note": "the contract forbids starting here; models run only "
                        "after the frozen rule has been measured everywhere "
                        "it can be"}
    out["state_conditioning"] = state_conditioning(
        symbol, stamps, zone="BC", charge=charge)
    out["models"] = run_models(symbol, stamps, charge=charge)
    m = out["models"]
    out["ML_ADDED_ECONOMIC_VALUE"] = bool(m.get("ML_ADDED_ECONOMIC_VALUE"))
    return out
