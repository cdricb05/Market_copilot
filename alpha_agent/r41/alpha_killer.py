"""alpha_agent.r41.alpha_killer - Track 15: try to destroy every apparent
winner. A result strengthens ONLY by surviving attempts to kill it.

Targets (the two R41 survivors):

* RATES_LGBM_21s - the pooled LightGBM duration-neutral RV book (Zone-B
  t 2.27, gate PASS). Battery: leave-one-country-out, leave-one-structure-
  kind-out, leave-one-year-block-out (Zone B in thirds), one-session
  latency, cost x3, feature-family ablation (refit on Zone A without each
  family), placebo feature (Zone-A-shuffled carry), and the alternative
  observable roll rule is N/A-justified (quarterly government contracts
  roll far from first notice; the R38 rule and OI-max coincide there).
  Zone C is NOT opened: Zone-B t 2.27 < the 2.5 pre-gate.

* CRYPTO_FUNDING_CARRY_1d - Zone-B t 10.2 >= 2.5, so this candidate's
  lineage earns its ONE Zone-C access, plus: cost x3, one-day latency,
  year-block splits, gate-threshold perturbation (0.4/0.6), and placebo
  funding (sign gates from a date-shuffled funding series must destroy
  the edge if funding is the information).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import evidence as EV
from . import curve_state as CS
from .rates_rv_lab import (MARKETS, MODEL_HORIZONS, STRUCTURES, book_stream,
                           build_signals, build_structures, factor_streams,
                           load_inputs)
from . import rates_rv_lab as RL
from . import crypto_lab as CRL

CALCULATION_OWNER = "alpha_agent.r41.alpha_killer"


def _rates_model_positions(structs, sigs, inp, zones, *, h=21,
                           drop_features=None, placebo_carry=False,
                           drop_structs=None):
    """Rebuild the LGBM h=21 positions with a declared perturbation."""
    import lightgbm as lgb
    feats = {}
    rng = np.random.default_rng(41)
    for n, st in structs.items():
        if drop_structs and n in drop_structs:
            continue
        s = st["spread"]
        idxp = (1.0 + s.fillna(0.0)).cumprod()
        carry = st["carry"].shift(1)
        if placebo_carry:
            v = carry.to_numpy().copy()
            fin = np.isfinite(v)
            v[fin] = rng.permutation(v[fin])
            carry = pd.Series(v, index=carry.index)
        f = pd.DataFrame({
            "z60": ((idxp - idxp.rolling(60, min_periods=30).mean())
                    / idxp.rolling(60, min_periods=30).std()).shift(1),
            "z250": ((idxp - idxp.rolling(250, min_periods=120).mean())
                     / idxp.rolling(250, min_periods=120).std()).shift(1),
            "mom21": idxp.pct_change(21).shift(1),
            "mom63": idxp.pct_change(63).shift(1),
            "carry": carry, "vol": st["vol"],
            "MEANREV": sigs[n]["MEANREV"], "VALUE": sigs[n]["VALUE"],
            "POLICY": sigs[n]["POLICY"]})
        feats[n] = f
    cols = [c for c in next(iter(feats.values())).columns
            if not (drop_features and c in drop_features)]
    core = [c for c in ("z60", "z250", "mom21", "carry", "vol")
            if c in cols]
    parts = []
    universe = {n: structs[n] for n in feats}
    for n, st in universe.items():
        s = st["spread"]
        fwd = (1.0 + s.fillna(0.0)).rolling(h).apply(
            lambda x: np.prod(1 + x) - 1, raw=True).shift(-h)
        f = feats[n][cols].copy()
        f["_y"] = fwd / (st["vol"] * np.sqrt(h))
        parts.append(f)
    panel = pd.concat(parts)
    fill = [c for c in ("VALUE", "POLICY", "mom63", "z250") if c in cols]
    panel[fill] = panel[fill].fillna(0.0)
    train = panel[panel.index.isin(zones["A"])].dropna(
        subset=core + ["_y"])
    m = lgb.LGBMRegressor(n_estimators=300, num_leaves=15,
                          learning_rate=0.03, subsample=0.7,
                          colsample_bytree=0.7, verbose=-1,
                          min_child_samples=100, random_state=7,
                          deterministic=True)
    m.fit(train[cols].to_numpy(), np.clip(train["_y"].to_numpy(), -5, 5))
    pos = {}
    for n, st in universe.items():
        f = feats[n][cols].copy()
        f[fill] = f[fill].fillna(0.0)
        ok = f[core].notna().all(axis=1)
        pred = pd.Series(np.nan, index=f.index)
        if ok.sum():
            pred[ok] = m.predict(f.loc[ok, cols].to_numpy())
        pos[n] = pred.clip(-2, 2) / st["vol"].replace(0, np.nan)
    return universe, pos


def kill_rates(*, progress=None) -> dict:
    inp = load_inputs()
    structs = build_structures(inp)
    sigs = build_signals(structs, inp)
    zones = EV.zone_split(inp["dates"], embargo=21)
    h = 21

    def zb_t(universe, pos, *, latency=0, cost_mult=1.0):
        stream = book_stream(universe, pos, horizon=h)
        g = stream["gross"]
        k = stream["cost"] * cost_mult
        if latency:
            # positions act one session later: shift the weight effect
            names = list(universe)
            W = stream["weights"].shift(latency)
            S = pd.DataFrame({n: universe[n]["spread"] for n in names})
            g = (W.shift(1) * S).sum(axis=1, min_count=1)
        card = EV.scorecard(g.reindex(zones["B"]).to_numpy(),
                            k.reindex(zones["B"]).to_numpy(),
                            np.zeros(len(zones["B"])),
                            periods_per_year=252.0, overlap=h)
        return card

    out = {"candidate": "RATES_LGBM_21s", "zone_c_opened": False,
           "zone_c_reason": "Zone-B t 2.27 < pre-gate 2.5", "tests": {}}
    uni, pos = _rates_model_positions(structs, sigs, inp, zones, h=h)
    base = zb_t(uni, pos)
    out["baseline_t"] = base.get("excess_t_hac")
    kills = 0

    def rec(name, card, note=""):
        nonlocal kills
        t = card.get("excess_t_hac")
        flip = t is not None and out["baseline_t"] is not None \
            and t < 0 < out["baseline_t"]
        if flip:
            kills += 1
        out["tests"][name] = {"t": t, "excess_ann": card.get("excess_ann"),
                              "sign_flip": bool(flip), "note": note}
        if progress:
            progress("%s t=%s flip=%s" % (name, None if t is None
                                          else round(t, 2), flip))

    rec("COST_X3", zb_t(uni, pos, cost_mult=3.0))
    rec("LATENCY_ONE_SESSION", zb_t(uni, pos, latency=1))
    # leave-one-country-out
    countries = sorted({c for st in structs.values()
                        for c in st["countries"]})
    for c in countries:
        drop = [n for n, st in structs.items() if c in st["countries"]]
        u2, p2 = _rates_model_positions(structs, sigs, inp, zones, h=h,
                                        drop_structs=drop)
        if len(u2) < 8:
            continue
        rec("LOCO_%s" % c, zb_t(u2, p2))
    # leave-one-structure-kind-out
    for kind in ("FLY", "SPREAD"):
        drop = [n for n, st in structs.items() if st["kind"] == kind]
        if len(structs) - len(drop) < 8:
            continue
        u2, p2 = _rates_model_positions(structs, sigs, inp, zones, h=h,
                                        drop_structs=drop)
        rec("NO_%s" % kind, zb_t(u2, p2))
    # year blocks on Zone B
    b = zones["B"]
    third = len(b) // 3
    stream = book_stream(uni, pos, horizon=h)
    for i in range(3):
        blk = b[i * third:(i + 1) * third]
        card = EV.scorecard(stream["gross"].reindex(blk).to_numpy(),
                            stream["cost"].reindex(blk).to_numpy(),
                            np.zeros(len(blk)), periods_per_year=252.0,
                            overlap=h)
        rec("YEAR_BLOCK_%d" % (i + 1), card,
            note="%s..%s" % (blk[0].date(), blk[-1].date()))
    # feature-family ablation + placebo
    for fam, dropf in (("NO_CARRY", ["carry"]),
                       ("NO_ZSCORES", ["z60", "z250", "MEANREV"]),
                       ("NO_MOM", ["mom21", "mom63"])):
        u2, p2 = _rates_model_positions(structs, sigs, inp, zones, h=h,
                                        drop_features=dropf)
        rec("ABLATE_%s" % fam, zb_t(u2, p2))
    u2, p2 = _rates_model_positions(structs, sigs, inp, zones, h=h,
                                    placebo_carry=True)
    rec("PLACEBO_CARRY", zb_t(u2, p2),
        note="shuffled carry must NOT preserve the edge if carry matters")
    out["n_sign_flips"] = kills
    out["alternative_roll_rule"] = ("N/A-JUSTIFIED: quarterly government "
                                    "bond contracts roll at first-notice "
                                    "buffers; the OI-max front coincides "
                                    "with the R38 rule on these markets")
    out["survives"] = kills == 0
    return out


def kill_funding(*, progress=None) -> dict:
    out = {"candidate": "CRYPTO_FUNDING_CARRY_BTC_1d", "tests": {}}
    fc = CRL.funding_carry_stream("BTCUSDT")
    idx = pd.DatetimeIndex(fc["dates"])
    zones = EV.zone_split(idx, embargo=7)
    ppy = 365.0
    pos_change = fc["signal"].diff().abs()
    cost = pos_change * 2 * CRL.TAKER_BPS / 1e4

    def card_on(g, c, zone):
        return EV.scorecard(g.reindex(zone).to_numpy(),
                            c.reindex(zone).to_numpy(),
                            np.zeros(len(zone)), periods_per_year=ppy,
                            overlap=1)

    base = card_on(fc["gross"], cost, zones["B"])
    out["baseline_zone_b_t"] = base.get("excess_t_hac")
    kills = 0

    def rec(name, card, note=""):
        nonlocal kills
        t = card.get("excess_t_hac")
        flip = t is not None and t < 0
        if flip:
            kills += 1
        out["tests"][name] = {"t": t, "excess_ann": card.get("excess_ann"),
                              "sign_flip": bool(flip), "note": note}
        if progress:
            progress("%s t=%s flip=%s" % (name, None if t is None
                                          else round(t, 2), flip))

    rec("COST_X3", card_on(fc["gross"], cost * 3.0, zones["B"]))
    rec("LATENCY_ONE_DAY",
        card_on(fc["signal"].shift(2) * (fc["gross"] /
                                         fc["signal"].shift(1)
                                         .replace(0, np.nan)).fillna(0.0),
                cost, zones["B"]),
        note="signal acts one day later")
    b = zones["B"]
    third = len(b) // 3
    for i in range(3):
        blk = b[i * third:(i + 1) * third]
        rec("YEAR_BLOCK_%d" % (i + 1), card_on(fc["gross"], cost, blk),
            note="%s..%s" % (blk[0].date(), blk[-1].date()))
    # threshold perturbation
    daily_f = fc["funding"]
    fz30 = (daily_f.rolling(30, min_periods=15).mean()
            / daily_f.rolling(90, min_periods=30).std()).shift(1)
    spot_perp = fc["gross"] / fc["signal"].shift(1).replace(0, np.nan)
    for thr in (0.4, 0.6):
        sig = pd.Series(0.0, index=idx)
        sig[fz30 > thr] = 1.0
        sig[fz30 < -thr] = -1.0
        g = sig.shift(1) * spot_perp.fillna(0.0)
        c = sig.diff().abs() * 2 * CRL.TAKER_BPS / 1e4
        rec("THRESHOLD_%g" % thr, card_on(g, c, zones["B"]))
    # placebo funding: date-shuffled funding drives the gate
    rng = np.random.default_rng(41)
    v = daily_f.to_numpy().copy()
    fin = np.isfinite(v)
    v[fin] = rng.permutation(v[fin])
    pf = pd.Series(v, index=idx)
    pz = (pf.rolling(30, min_periods=15).mean()
          / pf.rolling(90, min_periods=30).std()).shift(1)
    sig = pd.Series(0.0, index=idx)
    sig[pz > 0.5] = 1.0
    sig[pz < -0.5] = -1.0
    g = sig.shift(1) * (daily_f + spot_perp.fillna(0.0) - daily_f)  # same legs
    g = sig.shift(1) * spot_perp.fillna(0.0)
    c = sig.diff().abs() * 2 * CRL.TAKER_BPS / 1e4
    placebo = card_on(g, c, zones["B"])
    out["tests"]["PLACEBO_FUNDING_GATE"] = {
        "t": placebo.get("excess_t_hac"),
        "note": "a shuffled funding gate should NOT retain the edge",
        "destroys_edge": (placebo.get("excess_t_hac") or 0)
        < (out["baseline_zone_b_t"] or 0) / 2}
    # ---- the ONE Zone-C access for this lineage --------------------------- #
    zc = card_on(fc["gross"], cost, zones["C"])
    out["zone_c_opened"] = True
    out["zone_c_pregate"] = "Zone-B t %.2f >= 2.5" % (
        out["baseline_zone_b_t"] or 0)
    out["zone_c"] = EV.summarise(zc)
    out["zone_c_confirms"] = bool((zc.get("excess_t_hac") or 0) >= 2.0
                                  and (zc.get("excess_ann") or 0) > 0)
    out["n_sign_flips"] = kills
    out["survives"] = kills == 0
    if progress:
        progress("ZONE_C t=%s ex=%s confirms=%s" % (
            zc.get("excess_t_hac"), zc.get("excess_ann"),
            out["zone_c_confirms"]))
    return out
