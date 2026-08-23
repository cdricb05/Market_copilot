"""alpha_agent.r41.fx_credit_lab - Track 8: FX futures multi-horizon and
credit ETF relative value.

FX: the nine delivered FX futures. R36/R38 proved the carry PREDICTION
(rank IC 0.148, t 6.3) at MONTHLY cadence and found the after-cost excess
below the bar. The R41 question is CADENCE: does carry/momentum/reversal
pay at 1-21 session horizons where the monthly releases never looked?
Carry signal: the futures calendar slope (covered interest parity makes it
the rate differential - measured by R38, not assumed).

CREDIT: the ONE $0 native-ish lane left open by the ICE licence wall (FRED
OAS history is now capped at ~3 years): duration-hedged credit ETF spreads
(HYG - beta*LQD, LQD - beta*IEF; Norgate total-return series, 2007->),
with spread momentum / reversal / VIX-conditioning signals. LEVEL 2 proxy
of the credit market, honestly labelled.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from . import contract as C
from . import curve_state as CS
from . import evidence as EV
from . import burden as BURDEN
from . import sample_acquisition as SA
from .rates_rv_lab import book_stream, _roll_beta

logging.disable(logging.CRITICAL)
CALCULATION_OWNER = "alpha_agent.r41.fx_credit_lab"

FX_MARKETS = ("6A", "6B", "6C", "6E", "6J", "6M", "6N", "6S")
FX_BPS = C.COST_BPS_PER_SIDE["FX_FUTURES"]
FX_HORIZONS = (1, 2, 5, 10, 21)
ADVANCE_T = 1.5


def run_fx(*, progress=None) -> dict:
    daily = {m: CS.load_daily(m) for m in FX_MARKETS}
    daily = {m: d for m, d in daily.items() if d is not None}
    dates = None
    for d in daily.values():
        idx = d.index[np.isfinite(d["ret1"].to_numpy())]
        dates = idx if dates is None else dates.union(idx)
    dates = pd.DatetimeIndex(sorted(dates))
    structs = {}
    for m, d in daily.items():
        r = d["ret1"].reindex(dates)
        structs[m] = {"kind": "TS", "market": m, "tag": m, "legs": (m,),
                      "spread": r,
                      "gross": pd.Series(1.0, index=dates),
                      "vol": r.rolling(60, min_periods=30).std().shift(1),
                      "carry": d["slope_ann"].reindex(dates),
                      "legs_meta": [(m, 1.0, FX_BPS)], "countries": [m]}
    zones = EV.zone_split(dates, embargo=max(FX_HORIZONS))

    # XS rank positions: carry (NEGATIVE slope = high foreign rate -> long),
    # momentum, reversal
    def xs_pos(kind):
        if kind == "XS_CARRY":
            raw = pd.DataFrame({m: -structs[m]["carry"].shift(1)
                                for m in structs})
        elif kind == "XS_MOM":
            raw = pd.DataFrame({m: structs[m]["spread"].rolling(63).sum()
                                .shift(1) for m in structs})
        else:
            raw = pd.DataFrame({m: -structs[m]["spread"].rolling(5).sum()
                                .shift(1) for m in structs})
        rank = raw.rank(axis=1)
        k = rank.count(axis=1)
        z = (rank.sub(k / 2 + 0.5, axis=0)).div(k.clip(lower=2), axis=0) * 2
        return {m: z[m] / structs[m]["vol"].replace(0, np.nan)
                for m in structs}

    def eval_zone(stream, zone_idx, h):
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        to = float(stream["turnover"].reindex(zone_idx).fillna(0.0).mean())
        return EV.scorecard(g, k, np.zeros(len(zone_idx)),
                            periods_per_year=252.0, overlap=h,
                            turnover_per_period=to)

    factors = pd.DataFrame({
        "USD_DX": CS.load_daily("DX")["ret1"].reindex(dates),
        "EQUITY_ES": CS.load_daily("ES")["ret1"].reindex(dates)},
        index=dates)
    screen, results = [], []
    for kind in ("XS_CARRY", "XS_MOM", "XS_REV_5"):
        pos = xs_pos(kind)
        for h in FX_HORIZONS:
            stream = book_stream(structs, pos, horizon=h)
            card_a = eval_zone(stream, zones["A"], h)
            t_a = card_a.get("excess_t_hac")
            sign = 1
            if t_a is not None and t_a < 0:
                sign = -1
                stream = book_stream(structs,
                                     {n: -p for n, p in pos.items()},
                                     horizon=h)
                card_a = eval_zone(stream, zones["A"], h)
                t_a = card_a.get("excess_t_hac")
            spec = {"information_family": "FX_CARRY_STRUCTURE",
                    "asset_family": "FX_FUTURES", "horizon": "%ds" % h,
                    "economic_expression": "XS_LONG_SHORT",
                    "representation": kind + ("_NEG" if sign < 0 else ""),
                    "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                    "hyperparameter_budget": 1,
                    "parent_hypotheses": ["R38 FX carry IC 0.148 monthly"],
                    "validation_touches": 1}
            screen.append({"label": kind, "horizon": h, "sign": sign,
                           "zone_a_t": t_a, "zone_a": EV.summarise(card_a)})
            if progress:
                progress("A %s h=%d sign=%+d t=%.2f" % (
                    kind, h, sign, t_a or float("nan")))
            if t_a is None or t_a < ADVANCE_T:
                continue
            card_b = eval_zone(stream, zones["B"], h)
            cid = BURDEN.record_zone_b(spec, family="FX")
            fr = EV.factor_residual(card_b["diff_stream"],
                                    factors.reindex(zones["B"]), overlap=h)
            gate = EV.research_candidate_gate(
                card_b, residual_t=fr.get("alpha_t_hac"))
            results.append({"candidate_id": cid, "label": kind,
                            "horizon": h, "spec": spec, "zone_a_t": t_a,
                            "zone_b": EV.summarise(card_b),
                            "factor_residual": dict(fr), "gate": gate})
            if progress:
                progress("B %s h=%d t=%.2f gate=%s" % (
                    kind, h, card_b.get("excess_t_hac") or float("nan"),
                    gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")},
            "screened": screen, "advanced": results, "family_bh": bh}


# --------------------------------------------------------------------------- #
# Credit ETF RV
# --------------------------------------------------------------------------- #
CREDIT_BPS = C.COST_BPS_PER_SIDE["CREDIT_ETF"]


def _etf_returns(symbols) -> pd.DataFrame:
    import norgatedata as nd
    out = {}
    for s in symbols:
        try:
            df = nd.price_timeseries(
                s, stock_price_adjustment_setting=
                nd.StockPriceAdjustmentType.TOTALRETURN,
                timeseriesformat="pandas-dataframe")
            out[s] = df["Close"].astype(float).pct_change()
        except Exception:
            continue
    return pd.DataFrame(out)


def run_credit(*, progress=None) -> dict:
    R = _etf_returns(["HYG", "LQD", "IEF", "JNK"])
    if R.empty or "HYG" not in R or "LQD" not in R:
        return {"state": "ETF_DATA_UNAVAILABLE"}
    R = R.dropna(how="all")
    dates = R.index
    structs = {}
    for name, y, x in (("HY_IG", "HYG", "LQD"), ("IG_TSY", "LQD", "IEF")):
        if x not in R.columns:
            continue
        beta = _roll_beta(R[y], R[x], 120)
        s = R[y] - beta * R[x]
        structs[name] = {"kind": "SPREAD", "market": name, "tag": name,
                         "legs": (y, x), "spread": s,
                         "gross": 1.0 + beta.abs(),
                         "vol": s.rolling(60, min_periods=30).std().shift(1),
                         "carry": pd.Series(np.nan, index=dates),
                         "legs_meta": [(y, 1.0, CREDIT_BPS),
                                       (x, beta, CREDIT_BPS)],
                         "countries": [name]}
    vix = SA.load_cboe_index("VIX")
    vixz = ((vix - vix.rolling(250, min_periods=120).mean())
            / vix.rolling(250, min_periods=120).std()).reindex(dates)\
        .ffill(limit=3).shift(1)
    zones = EV.zone_split(dates, embargo=21)
    sigs = {}
    for n, st in structs.items():
        idxp = (1.0 + st["spread"].fillna(0.0)).cumprod()
        sigs[n] = pd.DataFrame({
            "SPREAD_MOM": np.sign(idxp.pct_change(21).shift(1)).fillna(0.0),
            "SPREAD_REV": (-((idxp - idxp.rolling(120, min_periods=60).mean())
                             / idxp.rolling(120, min_periods=60).std())
                           .shift(1)).clip(-2, 2),
            "VIX_FADE": (-vixz).clip(-2, 2),
        })

    def eval_zone(stream, zone_idx, h):
        g = stream["gross"].reindex(zone_idx).to_numpy()
        k = stream["cost"].reindex(zone_idx).to_numpy()
        return EV.scorecard(g, k, np.zeros(len(zone_idx)),
                            periods_per_year=252.0, overlap=h)

    screen, results = [], []
    for sig in ("SPREAD_MOM", "SPREAD_REV", "VIX_FADE"):
        pos = {n: sigs[n][sig] / st["vol"].replace(0, np.nan)
               for n, st in structs.items()}
        for h in (2, 5, 21):
            stream = book_stream(structs, pos, horizon=h)
            card_a = eval_zone(stream, zones["A"], h)
            t_a = card_a.get("excess_t_hac")
            sign = 1
            if t_a is not None and t_a < 0:
                sign = -1
                stream = book_stream(structs,
                                     {n: -p for n, p in pos.items()},
                                     horizon=h)
                card_a = eval_zone(stream, zones["A"], h)
                t_a = card_a.get("excess_t_hac")
            spec = {"information_family": "CREDIT_RV",
                    "asset_family": "CREDIT_ETF", "horizon": "%ds" % h,
                    "economic_expression": "DURATION_HEDGED_CREDIT_SPREAD",
                    "representation": sig + ("_NEG" if sign < 0 else ""),
                    "model": "TRANSPARENT_RULE_SIGN_FIT_ON_A",
                    "hyperparameter_budget": 1,
                    "parent_hypotheses": ["ICE OAS deep history is "
                                          "licence-walled; ETF proxy"],
                    "validation_touches": 1}
            screen.append({"label": sig, "horizon": h, "sign": sign,
                           "zone_a_t": t_a})
            if progress:
                progress("A %s h=%d sign=%+d t=%.2f" % (
                    sig, h, sign, t_a or float("nan")))
            if t_a is None or t_a < ADVANCE_T:
                continue
            card_b = eval_zone(stream, zones["B"], h)
            cid = BURDEN.record_zone_b(spec, family="CREDIT")
            gate = EV.research_candidate_gate(card_b)
            results.append({"candidate_id": cid, "label": sig, "horizon": h,
                            "spec": spec, "zone_a_t": t_a,
                            "zone_b": EV.summarise(card_b), "gate": gate})
            if progress:
                progress("B %s h=%d t=%.2f gate=%s" % (
                    sig, h, card_b.get("excess_t_hac") or float("nan"),
                    gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"zone_ranges": {z: zones["%s_range" % z.lower()]
                            for z in ("A", "B", "C")},
            "proxy_level": "LEVEL_2_ETF_PROXY",
            "screened": screen, "advanced": results, "family_bh": bh}
