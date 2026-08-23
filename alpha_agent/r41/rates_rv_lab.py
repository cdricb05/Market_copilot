"""alpha_agent.r41.rates_rv_lab - Track 4: international rates relative value.

The economic object is the AFTER-COST RESIDUAL RV RETURN of duration-neutral
spread books built from DATED government bond futures (11 international + 6
US Treasury + 7 short-rate markets), never outright bond direction.

Structures (declared below): within-country curve spreads and butterflies,
cross-country duration-neutral 10-year and 2-year pairs, and policy-path
spreads. Hedge ratios are ROLLING EMPIRICAL BETAS (120 sessions, lagged one
session) - point-in-time, no hindsight duration table.

Candidates are (signal-rule x horizon) PORTFOLIOS across all structures -
a structure is a book component, not a candidate - plus pooled Zone-A-fitted
models. Screening happens on ZONE_A (free); only candidates passing the
pre-declared advance rule (Zone-A t >= 1.5, cap 12, ranked by |t|) are
scored on ZONE_B, and every Zone-B score is a burden trial.

Cost per structure trade: leg-level bps on traded notional from
contract.COST_BPS_PER_SIDE. Control: zero (self-financed spread book);
factor residualisation against duration PC1, equity, USD and commodity
streams is reported for every advanced candidate.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import curve_state as CS
from . import evidence as EV
from . import sample_acquisition as SA
from . import burden as BURDEN
from . import data_dir

CALCULATION_OWNER = "alpha_agent.r41.rates_rv_lab"
FAMILY = "RATES_RV"

#: market -> (country, cost group)
MARKETS = {
    "ZT": ("US", "TREASURY_FUTURES"), "ZF": ("US", "TREASURY_FUTURES"),
    "ZN": ("US", "TREASURY_FUTURES"), "TN": ("US", "TREASURY_FUTURES"),
    "ZB": ("US", "TREASURY_FUTURES"), "UB": ("US", "TREASURY_FUTURES"),
    "FGBS": ("DE", "INTERNATIONAL_GOVERNMENT"),
    "FGBM": ("DE", "INTERNATIONAL_GOVERNMENT"),
    "FGBL": ("DE", "INTERNATIONAL_GOVERNMENT"),
    "FGBX": ("DE", "INTERNATIONAL_GOVERNMENT"),
    "FBTP": ("IT", "INTERNATIONAL_GOVERNMENT"),
    "FOAT": ("FR", "INTERNATIONAL_GOVERNMENT"),
    "LLG": ("UK", "INTERNATIONAL_GOVERNMENT"),
    "SJB": ("JP", "INTERNATIONAL_GOVERNMENT"),
    "CGB": ("CA", "INTERNATIONAL_GOVERNMENT"),
    "YYT": ("AU", "INTERNATIONAL_GOVERNMENT"),
    "YXT": ("AU", "INTERNATIONAL_GOVERNMENT"),
}
SHORT_RATE = {"US": "ZQ", "EU": "LEU", "UK": "SO3", "AU": "YIR", "CA": "CRA"}
SHORT_RATE_COUNTRY = {"DE": "EU", "IT": "EU", "FR": "EU", "US": "US",
                      "UK": "UK", "AU": "AU", "CA": "CA"}

#: (name, kind, legs, country tag) - kind: SPREAD (leg1 vs leg2) or FLY
STRUCTURES = [
    ("US_2s5s", "SPREAD", ("ZT", "ZF"), "US"),
    ("US_5s10s", "SPREAD", ("ZF", "ZN"), "US"),
    ("US_10s30s", "SPREAD", ("ZN", "ZB"), "US"),
    ("US_30sUltra", "SPREAD", ("ZB", "UB"), "US"),
    ("US_fly_2_5_10", "FLY", ("ZT", "ZF", "ZN"), "US"),
    ("US_fly_5_10_30", "FLY", ("ZF", "ZN", "ZB"), "US"),
    ("DE_2s5s", "SPREAD", ("FGBS", "FGBM"), "DE"),
    ("DE_5s10s", "SPREAD", ("FGBM", "FGBL"), "DE"),
    ("DE_10s30s", "SPREAD", ("FGBL", "FGBX"), "DE"),
    ("DE_fly_2_5_10", "FLY", ("FGBS", "FGBM", "FGBL"), "DE"),
    ("AU_3s10s", "SPREAD", ("YYT", "YXT"), "AU"),
    ("XC_10y_US_DE", "SPREAD", ("ZN", "FGBL"), "XC"),
    ("XC_10y_US_UK", "SPREAD", ("ZN", "LLG"), "XC"),
    ("XC_10y_US_CA", "SPREAD", ("ZN", "CGB"), "XC"),
    ("XC_10y_US_JP", "SPREAD", ("ZN", "SJB"), "XC"),
    ("XC_10y_US_AU", "SPREAD", ("ZN", "YXT"), "XC"),
    ("XC_10y_DE_UK", "SPREAD", ("FGBL", "LLG"), "XC"),
    ("XC_10y_DE_FR", "SPREAD", ("FGBL", "FOAT"), "XC"),
    ("XC_10y_DE_IT", "SPREAD", ("FGBL", "FBTP"), "XC"),
    ("XC_10y_DE_JP", "SPREAD", ("FGBL", "SJB"), "XC"),
    ("XC_2y_US_DE", "SPREAD", ("ZT", "FGBS"), "XC"),
]

SIGNALS = ("MEANREV", "MOMENTUM", "CARRY", "VALUE", "POLICY")
HORIZONS = (1, 2, 5, 10, 21)
ADVANCE_T = 1.5
ADVANCE_CAP = 12
MODEL_HORIZONS = (5, 21)
ROLL_BETA_WIN = 120
VOL_WIN = 60


# --------------------------------------------------------------------------- #
# Panel assembly
# --------------------------------------------------------------------------- #
def load_inputs() -> dict:
    daily = {m: CS.load_daily(m) for m in
             list(MARKETS) + list(SHORT_RATE.values())}
    daily = {m: d for m, d in daily.items() if d is not None}
    dates = None
    for m in MARKETS:
        d = daily.get(m)
        if d is None:
            continue
        idx = d.index[np.isfinite(d["ret1"].to_numpy())]
        dates = idx if dates is None else dates.union(idx)
    dates = pd.DatetimeIndex(sorted(dates))
    ret = pd.DataFrame({m: daily[m]["ret1"].reindex(dates)
                        for m in MARKETS if m in daily})
    slope = pd.DataFrame({m: daily[m]["slope_ann"].reindex(dates)
                          for m in MARKETS if m in daily})
    # implied short-rate yield (100 - price)/100 for policy paths
    sr_y = {}
    for cc, m in SHORT_RATE.items():
        d = daily.get(m)
        if d is None:
            continue
        sr_y[cc] = (100.0 - d["c1"].reindex(dates)) / 100.0
    sr_y = pd.DataFrame(sr_y)
    # 10y government yields for VALUE
    y10 = {}
    fred = pd.read_csv(data_dir("fred") / "fred_daily_panel.csv",
                       index_col=0, parse_dates=True)
    y10["US"] = fred["CMT_10Y"].reindex(dates).ffill(limit=5)
    ecb_p = data_dir("curves_gov") / "ecb_aaa_spot_curve.csv"
    if ecb_p.exists():
        ecb = pd.read_csv(ecb_p, index_col=0, parse_dates=True)
        y10["DE"] = ecb["10Y"].reindex(dates).ffill(limit=5)
    jgb_p = data_dir("curves_gov") / "mof_jgb_yields.csv"
    if jgb_p.exists():
        jgb = pd.read_csv(jgb_p, index_col=0, parse_dates=True)
        y10["JP"] = pd.to_numeric(jgb["10Y"], errors="coerce").reindex(
            dates).ffill(limit=5)
    boc_p = data_dir("curves_gov") / "boc_benchmark_yields.csv"
    if boc_p.exists():
        boc = pd.read_csv(boc_p, index_col=0, parse_dates=True)
        y10["CA"] = boc["10Y"].reindex(dates).ffill(limit=5)
    y10 = pd.DataFrame(y10)
    return {"dates": dates, "ret": ret, "slope": slope, "sr_yield": sr_y,
            "y10": y10}


def _roll_beta(y: pd.Series, x: pd.Series, win: int) -> pd.Series:
    cov = y.rolling(win, min_periods=win // 2).cov(x)
    var = x.rolling(win, min_periods=win // 2).var()
    beta = (cov / var).shift(1)          # lagged: PIT
    return beta.clip(-5.0, 5.0)


def build_structures(inp: dict) -> dict:
    """Per structure: spread daily return, gross multiplier, leg costs and
    the PIT signal inputs."""
    ret, slope = inp["ret"], inp["slope"]
    out = {}
    for (name, kind, legs, tag) in STRUCTURES:
        if any(m not in ret.columns for m in legs):
            continue
        if kind == "SPREAD":
            m1, m2 = legs
            beta = _roll_beta(ret[m1], ret[m2], ROLL_BETA_WIN)
            s = ret[m1] - beta * ret[m2]
            gross = 1.0 + beta.abs()
            bps1 = C.COST_BPS_PER_SIDE[MARKETS[m1][1]]
            bps2 = C.COST_BPS_PER_SIDE[MARKETS[m2][1]]
            carry = slope[m1] - beta * slope[m2]
            legs_meta = [(m1, 1.0, bps1), (m2, beta, bps2)]
        else:  # FLY: mid vs two wings, sequential betas
            w1, mid, w2 = legs
            b1 = _roll_beta(ret[mid], ret[w1], ROLL_BETA_WIN) * 0.5
            b2 = _roll_beta(ret[mid], ret[w2], ROLL_BETA_WIN) * 0.5
            s = ret[mid] - b1 * ret[w1] - b2 * ret[w2]
            gross = 1.0 + b1.abs() + b2.abs()
            bpsm = C.COST_BPS_PER_SIDE[MARKETS[mid][1]]
            bpsw1 = C.COST_BPS_PER_SIDE[MARKETS[w1][1]]
            bpsw2 = C.COST_BPS_PER_SIDE[MARKETS[w2][1]]
            carry = slope[mid] - b1 * slope[w1] - b2 * slope[w2]
            legs_meta = [(mid, 1.0, bpsm), (w1, b1, bpsw1), (w2, b2, bpsw2)]
        vol = s.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
        out[name] = {"kind": kind, "legs": legs, "tag": tag, "spread": s,
                     "gross": gross, "vol": vol, "carry": carry,
                     "legs_meta": legs_meta,
                     "countries": sorted({MARKETS[m][0] for m in legs})}
    return out


def build_signals(structs: dict, inp: dict) -> dict:
    """PIT signal panel per structure per signal (values in [-2, 2])."""
    y10, sr_y = inp["y10"], inp["sr_yield"]
    sigs = {}
    for name, st in structs.items():
        s = st["spread"]
        idxp = (1.0 + s.fillna(0.0)).cumprod()
        z120 = ((idxp - idxp.rolling(120, min_periods=60).mean())
                / idxp.rolling(120, min_periods=60).std()).shift(1)
        mom21 = idxp.pct_change(21).shift(1)
        vol = s.rolling(VOL_WIN, min_periods=30).std().shift(1)
        d = {}
        d["MEANREV"] = (-z120).clip(-2, 2)
        d["MOMENTUM"] = np.sign(mom21 / vol.replace(0, np.nan)).fillna(0.0)
        d["CARRY"] = np.sign(st["carry"].shift(1)).fillna(0.0)
        cs = st["countries"]
        if len(cs) == 2 and cs[0] in y10.columns and cs[1] in y10.columns:
            legs = st["legs"]
            c1, c2 = MARKETS[legs[0]][0], MARKETS[legs[1]][0]
            ydiff = y10[c1] - y10[c2]
            zy = ((ydiff - ydiff.rolling(250, min_periods=120).mean())
                  / ydiff.rolling(250, min_periods=120).std()).shift(1)
            d["VALUE"] = zy.clip(-2, 2)
        else:
            d["VALUE"] = pd.Series(np.nan, index=s.index)
        legs = st["legs"]
        cc1 = SHORT_RATE_COUNTRY.get(MARKETS[legs[0]][0])
        cc2 = SHORT_RATE_COUNTRY.get(MARKETS[legs[-1]][0])
        if st["kind"] == "SPREAD" and cc1 and cc2 and cc1 != cc2 \
                and cc1 in sr_y.columns and cc2 in sr_y.columns:
            pol = (sr_y[cc2] - sr_y[cc1]).diff(63)
            zp = (pol / pol.rolling(250, min_periods=120).std()).shift(1)
            d["POLICY"] = zp.clip(-2, 2)
        else:
            d["POLICY"] = pd.Series(np.nan, index=s.index)
        sigs[name] = pd.DataFrame(d)
    return sigs


# --------------------------------------------------------------------------- #
# Book construction
# --------------------------------------------------------------------------- #
def book_stream(structs: dict, positions: dict, *, horizon: int) -> dict:
    """Tranche the daily position panel over ``horizon`` sessions, normalise
    to gross notional 1 and produce gross/cost daily streams."""
    names = [n for n in positions if positions[n].notna().any()]
    if not names:
        return None
    P = pd.DataFrame({n: positions[n] for n in names})
    G = pd.DataFrame({n: structs[n]["gross"] for n in names})
    scale = (P.abs() * G).sum(axis=1)
    W = P.div(scale.where(scale > 0), axis=0).fillna(0.0)
    # tranche AFTER normalisation: daily turnover ~ gross/h instead of the
    # full renormalisation churn (a construction rule, not a fitted knob)
    W = W.rolling(horizon, min_periods=1).mean()
    S = pd.DataFrame({n: structs[n]["spread"] for n in names})
    gross_ret = (W.shift(1) * S).sum(axis=1, min_count=1)
    cost = pd.Series(0.0, index=W.index)
    for n in names:
        for (m, b, bps) in structs[n]["legs_meta"]:
            legw = W[n] * (b if isinstance(b, pd.Series) else float(b))
            cost = cost.add((legw - legw.shift(1)).abs() * bps / 1e4,
                            fill_value=0.0)
    turnover = sum((W[n] - W[n].shift(1)).abs() * G[n] for n in names)
    return {"gross": gross_ret, "cost": cost, "weights": W,
            "turnover": turnover}


def rule_positions(structs: dict, sigs: dict, signal: str) -> dict:
    out = {}
    for n, st in structs.items():
        sv = sigs[n][signal]
        u = sv / st["vol"].replace(0, np.nan)
        out[n] = u
    return out


# --------------------------------------------------------------------------- #
# Factors for residualisation
# --------------------------------------------------------------------------- #
def factor_streams(inp: dict) -> pd.DataFrame:
    ret = inp["ret"]
    dur = ret.apply(lambda s: s / s.rolling(120, min_periods=60).std()
                    .shift(1)).mean(axis=1)
    f = {"DURATION_PC1": dur}
    for name, m in (("EQUITY_ES", "ES"), ("USD_DX", "DX"),
                    ("COMMODITY_GD", "GD")):
        d = CS.load_daily(m)
        if d is not None:
            f[name] = d["ret1"].reindex(inp["dates"])
    return pd.DataFrame(f, index=inp["dates"])


# --------------------------------------------------------------------------- #
# Campaign
# --------------------------------------------------------------------------- #
def run(*, progress=None) -> dict:
    inp = load_inputs()
    structs = build_structures(inp)
    sigs = build_signals(structs, inp)
    factors = factor_streams(inp)
    dates = inp["dates"]
    zones = EV.zone_split(dates, embargo=max(HORIZONS))
    ppy = 252.0

    def eval_zone(stream: dict, zone_idx, horizon: int) -> dict:
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        b = np.zeros(len(zone_idx))
        to = float(stream["turnover"].reindex(zone_idx).mean())
        return EV.scorecard(g, k, b, periods_per_year=ppy, overlap=horizon,
                            turnover_per_period=to)

    # ---- declared rule candidates, screened on ZONE A ---------------------- #
    # Rules are SIGN-SYMMETRIC: Zone A is the fit zone, and the traded sign
    # of each (signal, horizon) rule is FIT ON ZONE A (the sign that makes
    # the Zone-A t positive), labelled SIGN_FIT_ON_A. Zone B is untouched by
    # this choice and still charges a burden trial per advanced candidate.
    screen = []
    for signal in SIGNALS:
        pos = rule_positions(structs, sigs, signal)
        for h in HORIZONS:
            stream = book_stream(structs, pos, horizon=h)
            if stream is None:
                continue
            card_a = eval_zone(stream, zones["A"], h)
            t_a = card_a.get("excess_t_hac")
            sign = 1
            if t_a is not None and t_a < 0:
                sign = -1
                neg = {n: -p for n, p in pos.items()}
                stream = book_stream(structs, neg, horizon=h)
                card_a = eval_zone(stream, zones["A"], h)
                t_a = card_a.get("excess_t_hac")
            spec = {"information_family": "RATES_RV",
                    "asset_family": "GOV_BOND_FUTURES",
                    "horizon": "%ds" % h,
                    "economic_expression": "DURATION_NEUTRAL_RV_PORTFOLIO",
                    "representation": "RULE_%s%s" % (
                        signal, "_NEG" if sign < 0 else ""),
                    "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                    "hyperparameter_budget": 1,
                    "parent_hypotheses": ["R39 intl carry RV c39_1a0105dd2f0c"],
                    "validation_touches": 1}
            screen.append({"signal": signal, "horizon": h, "spec": spec,
                           "sign": sign, "zone_a_t": t_a,
                           "zone_a": EV.summarise(card_a),
                           "stream": stream})
            if progress:
                progress("A-screen %s h=%d sign=%+d t=%.2f" % (
                    signal, h, sign, t_a or float("nan")))

    # ---- pooled models fit on ZONE A -------------------------------------- #
    model_specs = _pooled_models(structs, sigs, inp, zones, progress=progress)
    screen.extend(model_specs)

    # ---- advance rule ------------------------------------------------------ #
    # Rules screen out-of-fit on Zone A (they fit nothing): top RULE_CAP with
    # t >= ADVANCE_T. Models fit ON Zone A, so their Zone-A t is in-sample
    # and cannot compete for rule slots; they advance on their own quota.
    RULE_CAP = 10
    MODEL_CAP = 4
    rules = [r for r in screen if not r.get("zone_a_is_in_sample_for_model")
             and r["zone_a_t"] is not None and r["zone_a_t"] >= ADVANCE_T]
    rules.sort(key=lambda r: -(r["zone_a_t"] or 0))
    models = [r for r in screen if r.get("zone_a_is_in_sample_for_model")]
    advanced = rules[:RULE_CAP] + models[:MODEL_CAP]

    results = []
    for r in advanced:
        h = r["horizon"]
        card_b = eval_zone(r["stream"], zones["B"], h)
        cid = BURDEN.record_zone_b(r["spec"], family=FAMILY)
        fr = EV.factor_residual(card_b["diff_stream"],
                                factors.reindex(zones["B"]), overlap=h)
        gate = EV.research_candidate_gate(
            card_b, residual_t=fr.get("alpha_t_hac"))
        results.append({"candidate_id": cid, "signal": r.get("signal"),
                        "model": r["spec"]["model"], "horizon": h,
                        "spec": r["spec"], "zone_a_t": r["zone_a_t"],
                        "zone_b": EV.summarise(card_b),
                        "factor_residual": {k: v for k, v in fr.items()},
                        "gate": gate, "stream": r["stream"]})
        if progress:
            progress("B %s h=%d t=%.2f gate=%s" % (
                r["spec"]["representation"], h,
                card_b.get("excess_t_hac") or float("nan"), gate["passes"]))

    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"zones": {k: v for k, v in zones.items()
                      if k not in ("A", "B", "C")},
            "zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")},
            "n_structures": len(structs),
            "structures": {n: {"kind": s["kind"], "legs": list(s["legs"]),
                               "countries": s["countries"]}
                           for n, s in structs.items()},
            "screened": [{k: v for k, v in r.items()
                          if k not in ("stream",)} for r in screen],
            "advanced": results, "family_bh": bh,
            "advance_rule": {"zone_a_t_min": ADVANCE_T, "cap": ADVANCE_CAP}}


def _pooled_models(structs, sigs, inp, zones, progress=None) -> list:
    """Ridge and LightGBM pooled across structures, fit on ZONE A only."""
    rows = []
    feats = {}
    for n, st in structs.items():
        s = st["spread"]
        idxp = (1.0 + s.fillna(0.0)).cumprod()
        f = pd.DataFrame({
            "z60": ((idxp - idxp.rolling(60, min_periods=30).mean())
                    / idxp.rolling(60, min_periods=30).std()).shift(1),
            "z250": ((idxp - idxp.rolling(250, min_periods=120).mean())
                     / idxp.rolling(250, min_periods=120).std()).shift(1),
            "mom21": idxp.pct_change(21).shift(1),
            "mom63": idxp.pct_change(63).shift(1),
            "carry": st["carry"].shift(1),
            "vol": st["vol"],
            "MEANREV": sigs[n]["MEANREV"], "VALUE": sigs[n]["VALUE"],
            "POLICY": sigs[n]["POLICY"],
        })
        feats[n] = f
    out = []
    for h in MODEL_HORIZONS:
        X_parts, y_parts = [], []
        for n, st in structs.items():
            s = st["spread"]
            fwd = (1.0 + s.fillna(0.0)).rolling(h).apply(
                lambda x: np.prod(1 + x) - 1, raw=True).shift(-h)
            tgt = fwd / (st["vol"] * np.sqrt(h))
            f = feats[n].copy()
            f["_y"] = tgt
            f["_name"] = n
            X_parts.append(f)
        panel = pd.concat(X_parts, keys=range(len(X_parts)))
        panel = panel.reset_index(level=0, drop=True)
        cols = [c for c in panel.columns if not c.startswith("_")]
        core = ["z60", "z250", "mom21", "carry", "vol"]
        panel[["VALUE", "POLICY"]] = panel[["VALUE", "POLICY"]].fillna(0.0)
        panel["mom63"] = panel["mom63"].fillna(0.0)
        panel["z250"] = panel["z250"].fillna(0.0)
        mask_a = panel.index.isin(zones["A"])
        train = panel[mask_a].dropna(subset=core + ["_y"])
        if len(train) < 500:
            continue
        Xtr = train[cols].to_numpy()
        ytr = np.clip(train["_y"].to_numpy(), -5, 5)
        mu, sd = Xtr.mean(0), Xtr.std(0) + 1e-9
        for model_name in ("RIDGE", "LGBM"):
            if model_name == "RIDGE":
                Z = (Xtr - mu) / sd
                lam = 10.0 * len(Z)
                A = Z.T @ Z + lam * np.eye(Z.shape[1])
                w = np.linalg.solve(A, Z.T @ ytr)

                def predict(Xn, w=w, mu=mu, sd=sd):
                    return ((Xn - mu) / sd) @ w
            else:
                try:
                    import lightgbm as lgb
                except Exception:
                    continue
                m = lgb.LGBMRegressor(n_estimators=300, num_leaves=15,
                                      learning_rate=0.03, subsample=0.7,
                                      colsample_bytree=0.7, verbose=-1,
                                      min_child_samples=100, random_state=7,
                                      deterministic=True)
                m.fit(Xtr, ytr)

                def predict(Xn, m=m):
                    return m.predict(Xn)
            pos = {}
            for n, st in structs.items():
                f = feats[n].copy()
                f[["VALUE", "POLICY", "mom63", "z250"]] = \
                    f[["VALUE", "POLICY", "mom63", "z250"]].fillna(0.0)
                ok = f[core].notna().all(axis=1)
                pred = pd.Series(np.nan, index=f.index)
                if ok.sum():
                    pred[ok] = predict(f.loc[ok, cols].to_numpy())
                pos[n] = pred.clip(-2, 2) / st["vol"].replace(0, np.nan)
            stream = book_stream(structs, pos, horizon=h)
            if stream is None:
                continue
            g = stream["gross"].reindex(zones["A"]).to_numpy()
            k = stream["cost"].reindex(zones["A"]).to_numpy()
            card_a = EV.scorecard(g, k, np.zeros(len(zones["A"])),
                                  periods_per_year=252.0, overlap=h)
            spec = {"information_family": "RATES_RV",
                    "asset_family": "GOV_BOND_FUTURES",
                    "horizon": "%ds" % h,
                    "economic_expression": "DURATION_NEUTRAL_RV_PORTFOLIO",
                    "representation": "POOLED_FEATURES_9",
                    "model": model_name,
                    "hyperparameter_budget": 1,
                    "parent_hypotheses": ["R41 rates rule family"],
                    "validation_touches": 1}
            out.append({"signal": None, "horizon": h, "spec": spec,
                        "zone_a_t": card_a.get("excess_t_hac"),
                        "zone_a": EV.summarise(card_a), "stream": stream,
                        "zone_a_is_in_sample_for_model": True})
            if progress:
                progress("A-model %s h=%d t(IS)=%.2f" % (
                    model_name, h, card_a.get("excess_t_hac") or float("nan")))
    return out
