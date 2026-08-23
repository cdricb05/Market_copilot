"""alpha_agent.r41.commodity_curve_lab - Track 5: the commodity curve as the
economic object.

Everything here trades CURVE STRUCTURE, not outright direction: per-market
calendar spreads (long tenor 1 / short tenor 2 of the SAME commodity, both
dated contracts under the frozen R38 roll policy) and butterflies
(1x tenor1 - 2x tenor2 + 1x tenor3). A calendar spread is self-financed and
nearly flat the commodity's direction, so the control is zero and the R38
finding ("trend +3.85 %/yr, t 2.48, dies at BH") is a DIFFERENT object.

Candidate books (declared):
* CAL_<signal> x horizon - within-market calendar-spread portfolio, signals:
  CARRY (front slope), DCARRY (63-session slope change), SEASON (same
  calendar month's historical spread mean, walk-forward), SPREAD_MOM
  (21-session spread momentum), SPREAD_REV (120-session z fade),
  ROLLPRESS (days-to-expiry of the front).
* XS_CARRY / XS_MOM x horizon - cross-market RANK of the signal traded in
  calendar spreads (relative value: long the steep-carry markets' spreads,
  short the flat ones').
* FLY_REV x horizon - butterfly dislocation fade.
* EIA_WINDOW - the CARRY book confined to the two sessions after the weekly
  EIA petroleum report (event-driven cadence, energy markets only).
* Pooled RIDGE / LGBM on the spread panel (fit ZONE A only).

Screening on ZONE A is free; the advance rule and every ZONE_B burden rule
are identical to the rates lab. Costs: both legs at the market's
contract.COST_BPS_PER_SIDE group.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import curve_state as CS
from . import evidence as EV
from . import burden as BURDEN
from .rates_rv_lab import book_stream

CALCULATION_OWNER = "alpha_agent.r41.commodity_curve_lab"
FAMILY = "COMMODITY_CURVE"

MARKETS = {
    # energy
    "CL": "ENERGY", "BRN": "ENERGY", "HO": "ENERGY", "RB": "ENERGY",
    "NG": "ENERGY", "GAS": "ENERGY", "GWM": "ENERGY", "WBS": "ENERGY",
    # grains
    "ZC": "GRAINS_AND_OILSEEDS", "ZS": "GRAINS_AND_OILSEEDS",
    "ZW": "GRAINS_AND_OILSEEDS", "KE": "GRAINS_AND_OILSEEDS",
    "ZM": "GRAINS_AND_OILSEEDS", "ZL": "GRAINS_AND_OILSEEDS",
    "ZO": "GRAINS_AND_OILSEEDS", "RS": "GRAINS_AND_OILSEEDS",
    "MWE": "GRAINS_AND_OILSEEDS",
    # softs
    "SB": "SOFTS", "KC": "SOFTS", "CC": "SOFTS", "CT": "SOFTS",
    "LSU": "SOFTS", "LRC": "SOFTS", "LCC": "SOFTS", "OJ": "SOFTS",
    # livestock
    "LE": "LIVESTOCK", "GF": "LIVESTOCK", "HE": "LIVESTOCK",
    # metals
    "GC": "PRECIOUS_METALS", "SI": "PRECIOUS_METALS", "HG": "INDUSTRIAL_METALS",
    "PL": "PRECIOUS_METALS", "PA": "PRECIOUS_METALS",
}
ENERGY_EIA = ("CL", "HO", "RB", "NG", "WBS")

CAL_SIGNALS = ("CARRY", "DCARRY", "SEASON", "SPREAD_MOM", "SPREAD_REV",
               "ROLLPRESS")
XS_SIGNALS = ("XS_CARRY", "XS_MOM")
HORIZONS = (1, 2, 5, 10, 21)
XS_HORIZONS = (5, 10, 21)
ADVANCE_T = 1.5
RULE_CAP = 10
MODEL_CAP = 4
VOL_WIN = 60


def load_structs() -> dict:
    """Calendar-spread and butterfly structures from the daily curve layer."""
    structs = {}
    for m, group in MARKETS.items():
        d = CS.load_daily(m)
        if d is None or len(d) < 500:
            continue
        bps = C.COST_BPS_PER_SIDE[group]
        s = d["ret1"] - d["ret2"]
        vol = s.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
        structs["CAL_" + m] = {
            "kind": "CALENDAR", "market": m, "group": group, "tag": m,
            "legs": (m + "_c1", m + "_c2"), "spread": s,
            "gross": pd.Series(2.0, index=d.index), "vol": vol,
            "carry": d["slope_ann"],
            "carry2": d["slope23_ann"],
            "dte1": d["dte1"],
            "legs_meta": [(m + "_c1", 1.0, bps), (m + "_c2", 1.0, bps)],
            "countries": [m],
        }
        f = d["ret1"] - 2.0 * d["ret2"] + d["ret3"]
        fvol = f.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std().shift(1)
        structs["FLY_" + m] = {
            "kind": "FLY", "market": m, "group": group, "tag": m,
            "legs": (m + "_c1", m + "_c2", m + "_c3"), "spread": f,
            "gross": pd.Series(4.0, index=d.index), "vol": fvol,
            "carry": d["slope_ann"] - d["slope23_ann"],
            "legs_meta": [(m + "_c1", 1.0, bps), (m + "_c2", 2.0, bps),
                          (m + "_c3", 1.0, bps)],
            "countries": [m],
        }
    return structs


def _season_signal(s: pd.Series) -> pd.Series:
    """Walk-forward month-of-year mean of the spread return (>= 3 prior
    years before a month contributes), sign only."""
    df = pd.DataFrame({"r": s})
    df["m"] = df.index.month
    df["y"] = df.index.year
    monthly = df.groupby(["y", "m"])["r"].mean()
    out = pd.Series(np.nan, index=s.index)
    hist: dict = {}
    for (y, m), v in monthly.items():
        hist.setdefault(m, []).append((y, v))
    med = {}
    for m, rows in hist.items():
        rows.sort()
        for i, (y, v) in enumerate(rows):
            if i >= 3:
                med[(y, m)] = float(np.mean([x[1] for x in rows[:i]]))
    key = list(zip(s.index.year, s.index.month))
    vals = np.array([med.get(k, np.nan) for k in key])
    return pd.Series(np.sign(vals), index=s.index)


def build_signals(structs: dict) -> dict:
    sigs = {}
    for name, st in structs.items():
        s = st["spread"]
        idxp = (1.0 + s.fillna(0.0)).cumprod()
        z120 = ((idxp - idxp.rolling(120, min_periods=60).mean())
                / idxp.rolling(120, min_periods=60).std()).shift(1)
        d = {}
        if st["kind"] == "CALENDAR":
            d["CARRY"] = np.sign(st["carry"].shift(1)).fillna(0.0)
            d["DCARRY"] = np.sign(st["carry"].diff(63).shift(1)).fillna(0.0)
            d["SEASON"] = _season_signal(s).fillna(0.0)
            mom = idxp.pct_change(21).shift(1)
            d["SPREAD_MOM"] = np.sign(mom).fillna(0.0)
            d["SPREAD_REV"] = (-z120).clip(-2, 2)
            # roll pressure: front near expiry with contango -> spread cheapens
            d["ROLLPRESS"] = (np.sign(st["carry"].shift(1))
                              * (st["dte1"].shift(1) < 20)).fillna(0.0)
        else:
            d["FLY_REV"] = (-z120).clip(-2, 2)
        sigs[name] = pd.DataFrame(d)
    return sigs


def xs_positions(structs: dict, kind: str) -> dict:
    """Cross-market rank positions on calendar spreads."""
    cal = {n: st for n, st in structs.items() if st["kind"] == "CALENDAR"}
    if kind == "XS_CARRY":
        raw = pd.DataFrame({n: st["carry"].shift(1) for n, st in cal.items()})
    else:
        raw = pd.DataFrame({n: (1.0 + st["spread"].fillna(0.0)).cumprod()
                            .pct_change(63).shift(1) for n, st in cal.items()})
    rank = raw.rank(axis=1)
    k = rank.count(axis=1)
    z = (rank.sub(k / 2 + 0.5, axis=0)).div(k.clip(lower=2), axis=0) * 2
    out = {}
    for n, st in cal.items():
        out[n] = z[n] / st["vol"].replace(0, np.nan)
    return out


def eia_gate(index: pd.DatetimeIndex) -> pd.Series:
    """1 on the two sessions after the weekly Wednesday EIA petroleum
    report (10:30 ET; the report is observable before the next session)."""
    is_thu_fri = index.dayofweek.isin([3, 4])
    return pd.Series(is_thu_fri.astype(float), index=index)


def run(*, progress=None) -> dict:
    structs = load_structs()
    sigs = build_signals(structs)
    dates = None
    for st in structs.values():
        idx = st["spread"].dropna().index
        dates = idx if dates is None else dates.union(idx)
    dates = pd.DatetimeIndex(sorted(dates))
    zones = EV.zone_split(dates, embargo=max(HORIZONS))
    ppy = 252.0

    def eval_zone(stream, zone_idx, h):
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        to = float(stream["turnover"].reindex(zone_idx).mean())
        return EV.scorecard(g, k, np.zeros(len(zone_idx)),
                            periods_per_year=ppy, overlap=h,
                            turnover_per_period=to)

    cal = {n: st for n, st in structs.items() if st["kind"] == "CALENDAR"}
    fly = {n: st for n, st in structs.items() if st["kind"] == "FLY"}
    screen = []

    def add_screen(label, expr, universe, pos, h, extra=None):
        stream = book_stream(universe, pos, horizon=h)
        if stream is None:
            return
        card_a = eval_zone(stream, zones["A"], h)
        t_a = card_a.get("excess_t_hac")
        sign = 1
        if t_a is not None and t_a < 0:
            sign = -1
            stream = book_stream(universe, {n: -p for n, p in pos.items()},
                                 horizon=h)
            card_a = eval_zone(stream, zones["A"], h)
            t_a = card_a.get("excess_t_hac")
        spec = {"information_family": "COMMODITY_CURVE",
                "asset_family": "COMMODITY_FUTURES_CURVE",
                "horizon": "%ds" % h, "economic_expression": expr,
                "representation": label + ("_NEG" if sign < 0 else ""),
                "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                "hyperparameter_budget": 1,
                "parent_hypotheses": ["R38 native futures campaign"],
                "validation_touches": 1}
        row = {"label": label, "horizon": h, "sign": sign, "spec": spec,
               "zone_a_t": t_a, "zone_a": EV.summarise(card_a),
               "stream": stream}
        if extra:
            row.update(extra)
        screen.append(row)
        if progress:
            progress("A %s h=%d sign=%+d t=%.2f" % (label, h, sign,
                                                    t_a or float("nan")))

    for sig in CAL_SIGNALS:
        pos = {n: sigs[n][sig] / st["vol"].replace(0, np.nan)
               for n, st in cal.items()}
        for h in HORIZONS:
            add_screen("CAL_%s" % sig, "CALENDAR_SPREAD_PORTFOLIO", cal,
                       pos, h)
    for sig in XS_SIGNALS:
        pos = xs_positions(structs, sig)
        for h in XS_HORIZONS:
            add_screen(sig, "XS_CALENDAR_SPREAD_RV", cal, pos, h)
    pos = {n: sigs[n]["FLY_REV"] / st["vol"].replace(0, np.nan)
           for n, st in fly.items()}
    for h in XS_HORIZONS:
        add_screen("FLY_REV", "BUTTERFLY_DISLOCATION", fly, pos, h)
    # EIA event window (energy only, carry signal, 2-session hold)
    energy = {n: st for n, st in cal.items() if st["market"] in ENERGY_EIA}
    if energy:
        gate = eia_gate(dates)
        pos = {n: (sigs[n]["CARRY"] / st["vol"].replace(0, np.nan))
               .reindex(dates).mul(gate) for n, st in energy.items()}
        add_screen("EIA_WINDOW_CARRY", "EVENT_WINDOW_CALENDAR_SPREAD",
                   energy, pos, 2,
                   extra={"event": "EIA weekly petroleum report"})

    models = _pooled_models(cal, sigs, zones, progress=progress)
    screen.extend(models)

    rules = [r for r in screen if not r.get("zone_a_is_in_sample_for_model")
             and r["zone_a_t"] is not None and r["zone_a_t"] >= ADVANCE_T]
    rules.sort(key=lambda r: -(r["zone_a_t"] or 0))
    advanced = rules[:RULE_CAP] + \
        [r for r in screen if r.get("zone_a_is_in_sample_for_model")][:MODEL_CAP]

    factors = _factor_streams(dates)
    results = []
    for r in advanced:
        h = r["horizon"]
        card_b = eval_zone(r["stream"], zones["B"], h)
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
                        "stream": r["stream"]})
        if progress:
            progress("B %s h=%d t=%.2f gate=%s" % (
                r["spec"]["representation"], h,
                card_b.get("excess_t_hac") or float("nan"), gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")},
            "n_structures": len(structs),
            "screened": [{k: v for k, v in r.items() if k != "stream"}
                         for r in screen],
            "advanced": results, "family_bh": bh,
            "advance_rule": {"zone_a_t_min": ADVANCE_T, "rule_cap": RULE_CAP,
                            "model_cap": MODEL_CAP}}


def run_wave2(*, progress=None) -> dict:
    """Wave 2 (routed from wave 1's measured cost dominance, declared before
    any wave-2 Zone-B access): the same information expressed cost-aware.

    * CAL_COMPOSITE - CARRY + SEASON + spread-TREND (the sign-corrected
      reversal) combined per structure BEFORE normalisation, so one trade
      carries three signals (signals share costs); h in {21, 42, 63}.
    * CAL_CARRY at h in {42, 63} - longer holds amortise the round trip.
    * ENERGY_FLY_REV at h in {10, 21} - butterfly dislocation fade confined
      to the cheapest, most liquid energy curves.
    """
    structs = load_structs()
    sigs = build_signals(structs)
    cal = {n: st for n, st in structs.items() if st["kind"] == "CALENDAR"}
    fly_e = {n: st for n, st in structs.items()
             if st["kind"] == "FLY" and st["market"] in ENERGY_EIA}
    dates = None
    for st in structs.values():
        idx = st["spread"].dropna().index
        dates = idx if dates is None else dates.union(idx)
    dates = pd.DatetimeIndex(sorted(dates))
    zones = EV.zone_split(dates, embargo=63)

    def eval_zone(stream, zone_idx, h):
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        to = float(stream["turnover"].reindex(zone_idx).fillna(0.0).mean())
        return EV.scorecard(g, k, np.zeros(len(zone_idx)),
                            periods_per_year=252.0, overlap=h,
                            turnover_per_period=to)

    books = []
    comp_pos = {}
    for n, st in cal.items():
        comp = (sigs[n]["CARRY"] + sigs[n]["SEASON"]
                - sigs[n]["SPREAD_REV"] / 2.0)
        comp_pos[n] = comp / st["vol"].replace(0, np.nan)
    for h in (21, 42, 63):
        books.append(("CAL_COMPOSITE", "COMPOSITE_CALENDAR_SPREAD", cal,
                      comp_pos, h))
    carry_pos = {n: sigs[n]["CARRY"] / st["vol"].replace(0, np.nan)
                 for n, st in cal.items()}
    for h in (42, 63):
        books.append(("CAL_CARRY_LONG", "CALENDAR_SPREAD_PORTFOLIO", cal,
                      carry_pos, h))
    flyrev = {n: sigs[n]["FLY_REV"] / st["vol"].replace(0, np.nan)
              for n, st in fly_e.items()}
    for h in (10, 21):
        books.append(("ENERGY_FLY_REV", "BUTTERFLY_DISLOCATION_ENERGY",
                      fly_e, flyrev, h))

    factors = _factor_streams(dates)
    screen, results = [], []
    for (label, expr, universe, pos, h) in books:
        stream = book_stream(universe, pos, horizon=h)
        if stream is None:
            continue
        card_a = eval_zone(stream, zones["A"], h)
        t_a = card_a.get("excess_t_hac")
        sign = 1
        if t_a is not None and t_a < 0:
            sign = -1
            stream = book_stream(universe, {n: -p for n, p in pos.items()},
                                 horizon=h)
            card_a = eval_zone(stream, zones["A"], h)
            t_a = card_a.get("excess_t_hac")
        spec = {"information_family": "COMMODITY_CURVE",
                "asset_family": "COMMODITY_FUTURES_CURVE",
                "horizon": "%ds" % h, "economic_expression": expr,
                "representation": label + ("_NEG" if sign < 0 else ""),
                "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                "hyperparameter_budget": 1,
                "parent_hypotheses": ["R41 commodity wave 1: gross "
                                      "information real, cost dominated"],
                "validation_touches": 1}
        row = {"label": label, "horizon": h, "sign": sign, "spec": spec,
               "zone_a_t": t_a, "zone_a": EV.summarise(card_a)}
        screen.append(row)
        if progress:
            progress("A2 %s h=%d sign=%+d t=%.2f" % (label, h, sign,
                                                     t_a or float("nan")))
        if t_a is None or t_a < ADVANCE_T:
            continue
        card_b = eval_zone(stream, zones["B"], h)
        cid = BURDEN.record_zone_b(spec, family=FAMILY)
        fr = EV.factor_residual(card_b["diff_stream"],
                                factors.reindex(zones["B"]), overlap=h)
        gate = EV.research_candidate_gate(card_b,
                                          residual_t=fr.get("alpha_t_hac"))
        results.append({"candidate_id": cid, "label": label, "horizon": h,
                        "spec": spec, "zone_a_t": t_a,
                        "zone_b": EV.summarise(card_b),
                        "factor_residual": dict(fr), "gate": gate,
                        "stream": stream})
        if progress:
            progress("B2 %s h=%d t=%.2f gate=%s" % (
                label, h, card_b.get("excess_t_hac") or float("nan"),
                gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"screened": screen, "advanced": results, "family_bh": bh,
            "routed_from": "wave 1 cost dominance",
            "zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")}}


def _factor_streams(dates) -> pd.DataFrame:
    f = {}
    for name, m in (("COMMODITY_GD", "GD"), ("EQUITY_ES", "ES"),
                    ("USD_DX", "DX"), ("RATES_ZN", "ZN"), ("ENERGY_CL", "CL")):
        d = CS.load_daily(m)
        if d is not None:
            f[name] = d["ret1"].reindex(dates)
    return pd.DataFrame(f, index=dates)


def _pooled_models(cal: dict, sigs: dict, zones, progress=None) -> list:
    out = []
    feats = {}
    for n, st in cal.items():
        s = st["spread"]
        idxp = (1.0 + s.fillna(0.0)).cumprod()
        feats[n] = pd.DataFrame({
            "carry": st["carry"].shift(1),
            "carry2": st["carry2"].shift(1),
            "dcarry": st["carry"].diff(63).shift(1),
            "z120": ((idxp - idxp.rolling(120, min_periods=60).mean())
                     / idxp.rolling(120, min_periods=60).std()).shift(1),
            "mom21": idxp.pct_change(21).shift(1),
            "dte": st["dte1"].shift(1) / 100.0,
            "vol": st["vol"],
            "month": pd.Series(s.index.month / 12.0, index=s.index),
        })
    core = ["carry", "z120", "mom21", "vol"]
    for h in (5, 21):
        parts = []
        for n, st in cal.items():
            f = feats[n].copy()
            s = st["spread"]
            fwd = (1.0 + s.fillna(0.0)).rolling(h).apply(
                lambda x: np.prod(1 + x) - 1, raw=True).shift(-h)
            f["_y"] = fwd / (st["vol"] * np.sqrt(h))
            parts.append(f)
        panel = pd.concat(parts)
        panel[["carry2", "dcarry", "dte"]] = panel[
            ["carry2", "dcarry", "dte"]].fillna(0.0)
        mask_a = panel.index.isin(zones["A"])
        train = panel[mask_a].dropna(subset=core + ["_y"])
        if len(train) < 1000:
            continue
        cols = [c for c in panel.columns if not c.startswith("_")]
        Xtr = train[cols].to_numpy()
        ytr = np.clip(train["_y"].to_numpy(), -5, 5)
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
                mm = lgb.LGBMRegressor(n_estimators=300, num_leaves=15,
                                       learning_rate=0.03, subsample=0.7,
                                       colsample_bytree=0.7, verbose=-1,
                                       min_child_samples=200, random_state=7,
                                       deterministic=True)
                mm.fit(Xtr, ytr)

                def predict(Xn, mm=mm):
                    return mm.predict(Xn)
            pos = {}
            for n, st in cal.items():
                f = feats[n].copy()
                f[["carry2", "dcarry", "dte"]] = f[
                    ["carry2", "dcarry", "dte"]].fillna(0.0)
                ok = f[core].notna().all(axis=1)
                pred = pd.Series(np.nan, index=f.index)
                if ok.sum():
                    pred[ok] = predict(f.loc[ok, cols].to_numpy())
                pos[n] = pred.clip(-2, 2) / st["vol"].replace(0, np.nan)
            stream = book_stream(cal, pos, horizon=h)
            if stream is None:
                continue
            g = stream["gross"].reindex(zones["A"]).to_numpy()
            k = stream["cost"].reindex(zones["A"]).to_numpy()
            card_a = EV.scorecard(g, k, np.zeros(len(zones["A"])),
                                  periods_per_year=252.0, overlap=h)
            spec = {"information_family": "COMMODITY_CURVE",
                    "asset_family": "COMMODITY_FUTURES_CURVE",
                    "horizon": "%ds" % h,
                    "economic_expression": "CALENDAR_SPREAD_PORTFOLIO",
                    "representation": "POOLED_FEATURES_8",
                    "model": model_name, "hyperparameter_budget": 1,
                    "parent_hypotheses": ["R41 commodity rule family"],
                    "validation_touches": 1}
            out.append({"label": "POOLED_%s" % model_name, "horizon": h,
                        "sign": 1, "spec": spec,
                        "zone_a_t": card_a.get("excess_t_hac"),
                        "zone_a": EV.summarise(card_a), "stream": stream,
                        "zone_a_is_in_sample_for_model": True})
            if progress:
                progress("A-model %s h=%d t(IS)=%.2f" % (
                    model_name, h, card_a.get("excess_t_hac") or float("nan")))
    return out
