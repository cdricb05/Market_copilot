"""alpha_agent.r41.vol_lab - Track 6: volatility term structure and premium.

Tradeable object: the DATED Cboe VX curve (owned via Norgate, 2004->, daily
settlements, R38 roll policy). Information: the curve itself plus the FREE
Cboe index term structure acquired this release (VIX9D / VIX / VIX3M /
VIX6M / VVIX / SKEW, daily) and ES realised volatility (owned).

Declared books (daily decisions, multi-horizon):

* VX_CAL_<signal> x h - front-second VX calendar spread portfolio (one
  structure; self-financed; control zero), signals: BASIS (VX1 - VIX,
  vol-scaled), TERM (VIX3M - VIX slope z), RVGAP (VIX - realised 21d ES vol,
  z), VVIX_Z, SKEW_Z, DTE (roll pressure).
* VX_SHORT_GATED x h - short the front contract ONLY when the curve is in
  contango AND the variance premium is positive (an abstention-capable
  timing book). Control: the volatility-matched ALWAYS-short-front passive
  (beating the unconditional premium is the claim).
* Pooled RIDGE / LGBM over the signal set on the calendar spread, h in
  {5, 21}.

The R39 VX carry shadow is weekly and unconditional; these books are daily,
conditional, and mostly SPREAD-expressed - the forward-freeze duplication
rule (|corr| < 0.90) is checked before any freeze.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import curve_state as CS
from . import evidence as EV
from . import burden as BURDEN
from . import sample_acquisition as SA
from .rates_rv_lab import book_stream

CALCULATION_OWNER = "alpha_agent.r41.vol_lab"
FAMILY = "VOLATILITY_OPTIONS"
BPS = C.COST_BPS_PER_SIDE["VIX_FUTURES_TERM_STRUCTURE"]
HORIZONS = (1, 2, 5, 10, 21)
ADVANCE_T = 1.5
RULE_CAP = 8
MODEL_CAP = 4
VOL_WIN = 60


def load_inputs() -> dict:
    vx = CS.load_daily("VX")
    es = CS.load_daily("ES")
    idx = vx.index
    vix = SA.load_cboe_index("VIX").reindex(idx).ffill(limit=3)
    vix3m = SA.load_cboe_index("VIX3M").reindex(idx).ffill(limit=3)
    vix9d = SA.load_cboe_index("VIX9D").reindex(idx).ffill(limit=3)
    vvix = SA.load_cboe_index("VVIX").reindex(idx).ffill(limit=3)
    skew = SA.load_cboe_index("SKEW").reindex(idx).ffill(limit=3)
    rv21 = es["ret1"].reindex(idx).rolling(21).std() * np.sqrt(252) * 100.0
    return {"vx": vx, "idx": idx, "VIX": vix, "VIX3M": vix3m,
            "VIX9D": vix9d, "VVIX": vvix, "SKEW": skew, "RV21": rv21}


def _z(s: pd.Series, win: int = 250) -> pd.Series:
    return ((s - s.rolling(win, min_periods=win // 2).mean())
            / s.rolling(win, min_periods=win // 2).std()).shift(1)


def build(inp: dict) -> tuple:
    vx = inp["vx"]
    spread = vx["ret1"] - vx["ret2"]          # long front vs short second
    svol = spread.rolling(VOL_WIN, min_periods=30).std().shift(1)
    cal = {"VX_CAL": {
        "kind": "CALENDAR", "market": "VX", "tag": "VX",
        "legs": ("VX_c1", "VX_c2"), "spread": spread,
        "gross": pd.Series(2.0, index=vx.index), "vol": svol,
        "carry": vx["slope_ann"],
        "legs_meta": [("VX_c1", 1.0, BPS), ("VX_c2", 1.0, BPS)],
        "countries": ["VX"]}}
    outright = {"VX_TS": {
        "kind": "TS", "market": "VX", "tag": "VX",
        "legs": ("VX_c1",), "spread": vx["ret1"],
        "gross": pd.Series(1.0, index=vx.index),
        "vol": vx["ret1"].rolling(VOL_WIN, min_periods=30).std().shift(1),
        "carry": vx["slope_ann"],
        "legs_meta": [("VX_c1", 1.0, BPS)], "countries": ["VX"]}}
    basis = vx["c1"] - inp["VIX"]
    sigs = pd.DataFrame({
        "BASIS": _z(basis / inp["VIX"]).clip(-2, 2),
        "TERM": _z((inp["VIX3M"] - inp["VIX"]) / inp["VIX"]).clip(-2, 2),
        "RVGAP": _z(inp["VIX"] - inp["RV21"]).clip(-2, 2),
        "VVIX_Z": _z(inp["VVIX"]).clip(-2, 2),
        "SKEW_Z": _z(inp["SKEW"]).clip(-2, 2),
        "DTE": np.sign(20.0 - vx["dte1"]).shift(1),
        "CONTANGO": np.sign(vx["slope_ann"]).shift(1),
    }, index=vx.index)
    return cal, outright, sigs


def short_gated_positions(outright, sigs, inp) -> dict:
    st = outright["VX_TS"]
    contango = (inp["vx"]["slope_ann"].shift(1) > 0)
    vrp = (inp["VIX"] - inp["RV21"]).shift(1) > 0
    gate = (contango & vrp).astype(float)
    return {"VX_TS": -gate / st["vol"].replace(0, np.nan)}


def always_short_control(outright) -> dict:
    st = outright["VX_TS"]
    return {"VX_TS": -1.0 / st["vol"].replace(0, np.nan)
            * pd.Series(1.0, index=st["spread"].index)}


def run(*, progress=None) -> dict:
    inp = load_inputs()
    cal, outright, sigs = build(inp)
    dates = inp["idx"]
    zones = EV.zone_split(dates, embargo=max(HORIZONS))
    ppy = 252.0
    factors = pd.DataFrame({
        "EQUITY_ES": CS.load_daily("ES")["ret1"].reindex(dates),
        "VX_FRONT": inp["vx"]["ret1"],
        "RATES_ZN": CS.load_daily("ZN")["ret1"].reindex(dates)},
        index=dates)

    def eval_zone(stream, zone_idx, h, control=None):
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        b = np.zeros(len(zone_idx)) if control is None else \
            control.reindex(zone_idx).fillna(0.0).to_numpy()
        to = float(stream["turnover"].reindex(zone_idx).fillna(0.0).mean())
        return EV.scorecard(g, k, b, periods_per_year=ppy, overlap=h,
                            turnover_per_period=to)

    screen = []

    def add(label, expr, universe, pos, h, control=None, parents=None):
        stream = book_stream(universe, pos, horizon=h)
        if stream is None:
            return
        card_a = eval_zone(stream, zones["A"], h, control)
        t_a = card_a.get("excess_t_hac")
        sign = 1
        if t_a is not None and t_a < 0 and control is None:
            sign = -1
            stream = book_stream(universe, {n: -p for n, p in pos.items()},
                                 horizon=h)
            card_a = eval_zone(stream, zones["A"], h, control)
            t_a = card_a.get("excess_t_hac")
        spec = {"information_family": "VOLATILITY_OPTIONS",
                "asset_family": "VX_CURVE",
                "horizon": "%ds" % h, "economic_expression": expr,
                "representation": label + ("_NEG" if sign < 0 else ""),
                "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                "hyperparameter_budget": 1,
                "parent_hypotheses": parents or
                ["R38 VX term carry t 2.28 (BH-killed)"],
                "validation_touches": 1}
        screen.append({"label": label, "horizon": h, "sign": sign,
                       "spec": spec, "zone_a_t": t_a,
                       "zone_a": EV.summarise(card_a), "stream": stream,
                       "control": control})
        if progress:
            progress("A %s h=%d sign=%+d t=%.2f" % (label, h, sign,
                                                    t_a or float("nan")))

    for sig in ("BASIS", "TERM", "RVGAP", "VVIX_Z", "SKEW_Z", "DTE"):
        st = cal["VX_CAL"]
        pos = {"VX_CAL": sigs[sig] / st["vol"].replace(0, np.nan)}
        for h in HORIZONS:
            add("VX_CAL_%s" % sig, "VX_CALENDAR_SPREAD", cal, pos, h)

    # gated short-front vs the always-short control
    ctrl_stream = book_stream(outright, always_short_control(outright),
                              horizon=1)
    ctrl_net = ctrl_stream["gross"] - ctrl_stream["cost"]
    pos = short_gated_positions(outright, sigs, inp)
    for h in (1, 2, 5):
        add("VX_SHORT_GATED", "GATED_SHORT_VOL_TS", outright, pos, h,
            control=ctrl_net,
            parents=["R39 VX carry shadow (weekly, unconditional)"])

    screen.extend(_pooled(cal, sigs, zones, progress=progress))

    rules = [r for r in screen if not r.get("zone_a_is_in_sample_for_model")
             and r["zone_a_t"] is not None and r["zone_a_t"] >= ADVANCE_T]
    rules.sort(key=lambda r: -(r["zone_a_t"] or 0))
    advanced = rules[:RULE_CAP] + \
        [r for r in screen if r.get("zone_a_is_in_sample_for_model")][:MODEL_CAP]

    results = []
    for r in advanced:
        h = r["horizon"]
        card_b = eval_zone(r["stream"], zones["B"], h, r.get("control"))
        cid = BURDEN.record_zone_b(r["spec"], family=FAMILY)
        fr = EV.factor_residual(card_b["diff_stream"],
                                factors.reindex(zones["B"]), overlap=h)
        gate = EV.research_candidate_gate(card_b,
                                          residual_t=fr.get("alpha_t_hac"))
        results.append({"candidate_id": cid, "label": r["label"],
                        "horizon": h, "spec": r["spec"],
                        "zone_a_t": r["zone_a_t"],
                        "zone_b": EV.summarise(card_b),
                        "factor_residual": dict(fr), "gate": gate,
                        "stream": r["stream"], "control": r.get("control")})
        if progress:
            progress("B %s h=%d t=%.2f gate=%s" % (
                r["spec"]["representation"], h,
                card_b.get("excess_t_hac") or float("nan"), gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")},
            "screened": [{k: v for k, v in r.items()
                          if k not in ("stream", "control")}
                         for r in screen],
            "advanced": results, "family_bh": bh}


def _pooled(cal, sigs, zones, progress=None) -> list:
    out = []
    st = cal["VX_CAL"]
    s = st["spread"]
    cols = ["BASIS", "TERM", "RVGAP", "VVIX_Z", "SKEW_Z", "DTE", "CONTANGO"]
    for h in (5, 21):
        fwd = (1.0 + s.fillna(0.0)).rolling(h).apply(
            lambda x: np.prod(1 + x) - 1, raw=True).shift(-h)
        y = (fwd / (st["vol"] * np.sqrt(h))).clip(-5, 5)
        F = sigs[cols].fillna(0.0)
        mask_a = F.index.isin(zones["A"])
        ok = mask_a & y.notna() & F.notna().all(axis=1)
        if ok.sum() < 300:
            continue
        Xtr, ytr = F[ok].to_numpy(), y[ok].to_numpy()
        mu, sd = Xtr.mean(0), Xtr.std(0) + 1e-9
        for model_name in ("RIDGE", "LGBM"):
            if model_name == "RIDGE":
                Z = (Xtr - mu) / sd
                lam = 10.0 * len(Z)
                w = np.linalg.solve(Z.T @ Z + lam * np.eye(Z.shape[1]),
                                    Z.T @ ytr)

                def predict(Xn, w=w, mu=mu, sd=sd):
                    return ((Xn - mu) / sd) @ w
            else:
                try:
                    import lightgbm as lgb
                except Exception:
                    continue
                mm = lgb.LGBMRegressor(n_estimators=200, num_leaves=7,
                                       learning_rate=0.05, subsample=0.7,
                                       colsample_bytree=0.7, verbose=-1,
                                       min_child_samples=100, random_state=7,
                                       deterministic=True)
                mm.fit(Xtr, ytr)

                def predict(Xn, mm=mm):
                    return mm.predict(Xn)
            pred = pd.Series(predict(F.to_numpy()), index=F.index)
            pos = {"VX_CAL": pred.clip(-2, 2) / st["vol"].replace(0, np.nan)}
            stream = book_stream(cal, pos, horizon=h)
            if stream is None:
                continue
            g = stream["gross"].reindex(zones["A"]).to_numpy()
            k = stream["cost"].reindex(zones["A"]).to_numpy()
            card_a = EV.scorecard(g, k, np.zeros(len(zones["A"])),
                                  periods_per_year=252.0, overlap=h)
            spec = {"information_family": "VOLATILITY_OPTIONS",
                    "asset_family": "VX_CURVE", "horizon": "%ds" % h,
                    "economic_expression": "VX_CALENDAR_SPREAD",
                    "representation": "POOLED_FEATURES_7",
                    "model": model_name, "hyperparameter_budget": 1,
                    "parent_hypotheses": ["R41 vol rule family"],
                    "validation_touches": 1}
            out.append({"label": "POOLED_%s" % model_name, "horizon": h,
                        "sign": 1, "spec": spec,
                        "zone_a_t": card_a.get("excess_t_hac"),
                        "zone_a": EV.summarise(card_a), "stream": stream,
                        "zone_a_is_in_sample_for_model": True,
                        "control": None})
            if progress:
                progress("A-model %s h=%d t(IS)=%.2f" % (
                    model_name, h, card_a.get("excess_t_hac") or float("nan")))
    return out
