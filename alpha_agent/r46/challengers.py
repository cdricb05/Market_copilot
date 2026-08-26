"""alpha_agent.r46.challengers - the frozen seed cohort.

Ten economically distinct challengers across five asset classes and three
horizons. Every parameter in this file is a canonical constant taken from the
published asset-pricing literature and written down before
:mod:`alpha_agent.r46.marketdata` was first called: 12-1 momentum, five-day
reversal, sixty-day volatility, 252-day trend, the 200-day filter, a
one-sigma reversion band, decile portfolios. Nothing here was chosen by
sweeping this estate's data.

That is the whole point. Release 45 re-ran Release 44's sixty-cell screen
separately on three event zones and found a different winner every time - the
last one bigger than the published headline - because the maximum of a noisy
grid always looks locally peaked from the inside. A release that picks no cell
cannot be fooled that way, and it is why R46 charges essentially no new
historical search burden.

Two things this module deliberately does NOT do:

* it does not predict a magnitude. These are transparent rules, not calibrated
  return forecasts, so ``expected_return`` is emitted as ``None`` with
  ``expected_return_state = NOT_CALIBRATED``. The cost, which IS known before
  the fact, is emitted as a number. Inventing an expected return to fill a
  schema field would be the first lie in an evidence chain built to prevent
  them.
* it does not combine anything. Release 44 measured the combination frontier
  over twelve streams and the answer did not depend on the weighting scheme.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import marketdata as MD
from . import sha

CALCULATION_OWNER = "alpha_agent.r46.challengers"

K = C.SEED_PARAMETERS_WERE_NOT_SEARCHED["canonical_constants"]

BENCHMARK_EQUITY = "SPY"

#: Declared liquid futures markets, grouped. The grouping is market structure,
#: not return data: an exchange's flagship contract is the flagship contract
#: whatever it returned last year.
FUTURES_GROUPS = {
    "EQUITY_INDEX_FUTURES": ("&ES", "&NQ", "&YM", "&RTY", "&EMD", "&NKD",
                             "&FDAX", "&FESX"),
    "RATES_FUTURES": ("&ZT", "&ZF", "&ZN", "&ZB", "&UB", "&FGBL", "&FGBM",
                      "&FGBS", "&CGB"),
    "FX_FUTURES": ("&6A", "&6B", "&6C", "&6E", "&6J", "&6M", "&6N", "&6S",
                   "&DX"),
    "COMMODITY_FUTURES": ("&CL", "&NG", "&HO", "&RB", "&BRN", "&GC", "&SI",
                          "&HG", "&PL", "&PA", "&ZC", "&ZS", "&ZW", "&ZM",
                          "&ZL", "&KC", "&CT", "&SB", "&CC", "&LE", "&HE"),
    "VOLATILITY_FUTURES": ("&VX",),
}

#: The G10 crosses, all quoted USD-per-foreign so a rise always means the
#: foreign currency appreciated. Mixing AUDUSD with USDJPY in one
#: cross-section is a sign error waiting to happen.
G10_USD_PER_FOREIGN = ("AUDUSD", "CADUSD", "CHFUSD", "EURUSD", "GBPUSD",
                       "JPYUSD", "NZDUSD", "NOKUSD", "SEKUSD")

COMMODITY_MARKETS = FUTURES_GROUPS["COMMODITY_FUTURES"]

MIN_CROSS_SECTION = 30      # names needed before a decile book means anything
MIN_FUTURES_MARKETS = 12
MIN_FX_PAIRS = 6


# --------------------------------------------------------------------------- #
# The frozen specifications
# --------------------------------------------------------------------------- #
def _spec(**kw) -> dict:
    spec = {
        "challenger_version": "v1",
        "promotion_allowed": False,
        "research_shadow_only": True,
        "origin": "R46_SEED",
        "parameters_were_searched": False,
        "expected_return_state": "NOT_CALIBRATED",
    }
    spec.update(kw)
    return spec


SEED_SPECS = (
    _spec(
        challenger_id="r46_eq_xs_mom_12_1",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(5, 20),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="twelve-month winners keep winning over one to four weeks; the "
               "one-month skip removes the short-horizon reversal that would "
               "otherwise contaminate the signal",
        parameters={"formation_days": K["momentum_formation_days"],
                    "skip_days": K["momentum_skip_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_rev_5d",
        family="CROSS_SECTIONAL_REVERSAL",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(1,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="a week of one-sided pressure in a large-cap name is mostly "
               "liquidity provision, and it is paid back over the next session",
        parameters={"reversal_days": K["reversal_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_lowvol_60d",
        family="LOW_RISK_ANOMALY",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="leverage-constrained investors bid up high-beta names, so "
               "low-volatility stocks earn more per unit of risk than the "
               "CAPM allows",
        parameters={"volatility_days": K["volatility_days"],
                    "decile_fraction": K["decile_fraction"]},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_eq_xs_resid_mom_12_1",
        family="RESIDUAL_MOMENTUM",
        asset_class="US_EQUITY",
        instrument="BOOK:SP500_LS_DECILE",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="US_EQUITY",
        universe="S&P 500 index membership observed at emission",
        thesis="momentum measured on market-beta residuals carries the same "
               "continuation without the embedded market-timing bet, so it "
               "should survive where raw momentum crashes",
        parameters={"formation_days": K["momentum_formation_days"],
                    "skip_days": K["momentum_skip_days"],
                    "beta_days": K["beta_days"],
                    "decile_fraction": K["decile_fraction"],
                    "market": BENCHMARK_EQUITY},
        signal_owner="_eq_cross_section",
    ),
    _spec(
        challenger_id="r46_fut_ts_mom_252",
        family="TIME_SERIES_TREND",
        asset_class="MULTI_ASSET_FUTURES",
        instrument="BOOK:FUTURES_TS_TREND",
        prediction_type="TIME_SERIES_DIRECTIONAL_BASKET",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="MIXED_FUTURES",
        universe="declared liquid continuous futures across equity index, "
                 "rates, FX and commodities",
        thesis="risk transfer from hedgers to speculators pays a premium that "
               "shows up as twelve-month trend persistence, and it is the one "
               "premium documented in every futures market simultaneously",
        parameters={"trend_days": K["trend_days"],
                    "volatility_days": K["volatility_days"],
                    "sizing": "inverse realised volatility, gross notional 1"},
        signal_owner="_futures_trend",
    ),
    _spec(
        challenger_id="r46_fx_xs_mom_252",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="FX",
        instrument="BOOK:G10_FX_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="FX_SPOT",
        universe="G10 crosses quoted USD per foreign unit",
        thesis="currency trends persist because central banks move slowly and "
               "in the same direction for years at a time",
        parameters={"formation_days": K["trend_days"], "n_per_leg": 3},
        signal_owner="_fx_cross_section",
    ),
    _spec(
        challenger_id="r46_vx_term_carry_5d",
        family="VOLATILITY_TERM_CARRY",
        asset_class="VOLATILITY",
        instrument="&VX",
        prediction_type="DIRECTIONAL_SINGLE_INSTRUMENT",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="VOLATILITY_FUTURES",
        universe="front VIX future against VIX spot",
        thesis="the VIX curve is in contango most of the time because "
               "variance is insurance, and a short front future earns the "
               "roll-down when it is",
        parameters={"basis": "front &VX close / $VIX close - 1",
                    "position": "short when basis > 0, long when basis < 0"},
        signal_owner="_vx_carry",
        economic_overlap_with=("R39:shadow_vx_carry_ts",),
        overlap_note="economically related to the adopted R39 shadow, which "
                     "decides on VX Fridays with its own expression; the two "
                     "must not be counted as independent evidence",
    ),
    _spec(
        challenger_id="r46_rates_curve_rv_5d",
        family="RATES_RELATIVE_VALUE",
        asset_class="RATES",
        instrument="BOOK:ZN_ZT_DURATION_NEUTRAL",
        prediction_type="RELATIVE_VALUE_SPREAD",
        horizons=(5,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="RATES_FUTURES",
        universe="&ZN against &ZT, volatility-neutral",
        thesis="the belly of the Treasury curve is pushed around by hedging "
               "flow that reverses within a week once the flow clears",
        parameters={"spread_z_days": K["spread_z_days"],
                    "entry_z": 1.0,
                    "hedge": "inverse realised volatility on each leg"},
        signal_owner="_rates_rv",
        hedge_definition="long &ZN / short &ZT scaled to equal realised "
                         "volatility; both legs charged full cost",
    ),
    _spec(
        challenger_id="r46_comdty_xs_mom_252",
        family="CROSS_SECTIONAL_MOMENTUM",
        asset_class="COMMODITY",
        instrument="BOOK:COMMODITY_LS",
        prediction_type="CROSS_SECTIONAL_LONG_SHORT",
        horizons=(20,),
        control=C.CONTROL_CASH,
        benchmark="CASH",
        cost_class="COMMODITY_FUTURES",
        universe="declared liquid energy, metal and agricultural futures",
        thesis="commodity trends are carried by slow physical inventory "
               "adjustment, which no announcement resets",
        parameters={"formation_days": K["trend_days"], "leg_fraction": 1 / 3.0},
        signal_owner="_commodity_cross_section",
    ),
    _spec(
        challenger_id="r46_spx_trend_200d",
        family="INDEX_TREND_TIMING",
        asset_class="EQUITY_INDEX",
        instrument="SPY",
        prediction_type="DIRECTIONAL_VS_BENCHMARK",
        horizons=(20,),
        control=C.CONTROL_BENCHMARK,
        benchmark="SPY",
        cost_class="US_ETF",
        universe="SPY",
        thesis="the 200-day filter is the oldest published trend rule there "
               "is; if trend timing adds anything over simply owning the "
               "index, this is where it shows",
        parameters={"filter_days": K["trend_filter_days"],
                    "position": "long SPY above the average, cash below"},
        signal_owner="_index_trend",
    ),
)


def spec_by_id(challenger_id: str):
    for s in SEED_SPECS:
        if s["challenger_id"] == challenger_id:
            return s
    return None


def spec_hash(spec: dict) -> str:
    """Hash over everything that changes the challenger's ECONOMICS.

    Deliberately excludes nothing that matters and includes nothing that does
    not: two specs with the same hash must produce the same decisions from the
    same data, and any change to universe, parameters, horizon, control, cost
    class or expression must change it.
    """
    core = {
        "challenger_id": spec["challenger_id"],
        "challenger_version": spec["challenger_version"],
        "family": spec["family"],
        "asset_class": spec["asset_class"],
        "instrument": spec["instrument"],
        "prediction_type": spec["prediction_type"],
        "horizons": sorted(spec["horizons"]),
        "control": spec["control"],
        "benchmark": spec["benchmark"],
        "cost_class": spec["cost_class"],
        "universe": spec["universe"],
        "parameters": spec["parameters"],
        "signal_owner": spec["signal_owner"],
        "hedge_definition": spec.get("hedge_definition"),
    }
    return sha(core)


def parameters_hash(spec: dict) -> str:
    return sha(spec["parameters"])


def feature_set_hash(spec: dict) -> str:
    return sha({"signal_owner": spec["signal_owner"],
                "universe": spec["universe"],
                "inputs": "owned Norgate daily bars, adjusted"})


# --------------------------------------------------------------------------- #
# Book construction helpers
# --------------------------------------------------------------------------- #
def _decile_book(scores: dict, fraction: float,
                 cost_class: str = "US_EQUITY") -> list:
    """Dollar-neutral decile book, gross notional 1.0.

    Long the top ``fraction``, short the bottom ``fraction``, equal weight
    inside each leg, each leg carrying half the gross.

    ``cost_class`` is written onto EVERY leg rather than left to a default.
    The judge reads the leg's own class when it charges the round trip, and a
    leg that does not carry one would be charged at whatever the fallback
    happens to be - correct for equities today and silently wrong the first
    time a non-equity cross-section is added.
    """
    clean = {k: float(v) for k, v in scores.items()
             if v is not None and np.isfinite(v)}
    n = len(clean)
    if n < MIN_CROSS_SECTION:
        return []
    k = max(1, int(round(n * float(fraction))))
    order = sorted(clean.items(), key=lambda kv: kv[1])
    shorts, longs = order[:k], order[-k:]
    legs = []
    for sym, sc in longs:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": cost_class})
    for sym, sc in shorts:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": cost_class})
    return legs


def _normalise_gross(legs: list, gross: float = 1.0) -> list:
    tot = sum(abs(float(l["weight"])) for l in legs)
    if tot <= 0:
        return []
    f = float(gross) / tot
    for l in legs:
        l["weight"] = float(l["weight"]) * f
    return legs


# --------------------------------------------------------------------------- #
# Signal owners
# --------------------------------------------------------------------------- #
def _eq_universe() -> tuple:
    """S&P 500 membership OBSERVED AT EMISSION.

    For a forward prediction this is exactly right and carries no survivorship
    bias: the index's constituents today are a fact known today. The
    survivorship-safe ``Current & Past`` construction exists in
    :func:`alpha_agent.r46.marketdata.sp500_pit` and is reserved for anything
    labelled HISTORICAL_SIMULATION.
    """
    return MD._watchlist("S&P 500")


def _eq_cross_section(spec: dict) -> dict:
    syms = _eq_universe()
    if not syms:
        return {"state": "NO_UNIVERSE", "legs": []}
    p = spec["parameters"]
    cid = spec["challenger_id"]
    market = MD.closes(BENCHMARK_EQUITY) if "resid" in cid else None
    mkt_mom = None
    if market is not None:
        mkt_mom = MD.total_return(market, p.get("formation_days", 252),
                                  p.get("skip_days", 0))
    scores, marks, n_seen = {}, {}, 0
    for sym in syms:
        s = MD.closes(sym)
        if s is None or len(s) < 5:
            continue
        n_seen += 1
        marks[sym] = float(s.iloc[-1])
        if cid == "r46_eq_xs_mom_12_1":
            v = MD.total_return(s, p["formation_days"], p["skip_days"])
        elif cid == "r46_eq_xs_rev_5d":
            r = MD.total_return(s, p["reversal_days"])
            v = None if r is None else -r
        elif cid == "r46_eq_xs_lowvol_60d":
            vol = MD.realised_vol(s, p["volatility_days"])
            v = None if vol is None else -vol
        elif cid == "r46_eq_xs_resid_mom_12_1":
            raw = MD.total_return(s, p["formation_days"], p["skip_days"])
            b = MD.beta_to(s, market, p["beta_days"])
            v = (None if (raw is None or b is None or mkt_mom is None)
                 else raw - b * mkt_mom)
        else:                                   # pragma: no cover
            v = None
        if v is not None:
            scores[sym] = v
    legs = _decile_book(scores, p["decile_fraction"], spec["cost_class"])
    return {"state": "OK" if legs else "INSUFFICIENT_CROSS_SECTION",
            "legs": legs, "n_universe": len(syms), "n_priced": n_seen,
            "n_scored": len(scores), "marks": marks,
            "cost_class_by_leg": {l["instrument"]: "US_EQUITY" for l in legs}}


def _futures_group(sym: str) -> str:
    for grp, members in FUTURES_GROUPS.items():
        if sym in members:
            return grp
    return "COMMODITY_FUTURES"


def _futures_trend(spec: dict) -> dict:
    p = spec["parameters"]
    declared = [s for grp, members in FUTURES_GROUPS.items()
                for s in members if grp != "VOLATILITY_FUTURES"]
    available = set(MD.continuous_futures())
    legs, marks, skipped = [], {}, []
    for sym in declared:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        if MD.has_non_positive(s, p["trend_days"] + 1):
            skipped.append({"instrument": sym, "why": MD.NON_POSITIVE_PRICE,
                            "detail": "a percentage return is undefined "
                                      "across this window"})
            continue
        tr = MD.total_return(s, p["trend_days"])
        vol = MD.realised_vol(s, p["volatility_days"])
        if tr is None or vol is None or vol <= 0:
            skipped.append({"instrument": sym, "why": "SHORT_HISTORY"})
            continue
        direction = 1.0 if tr > 0 else -1.0
        legs.append({"instrument": sym, "weight": direction / vol,
                     "score": tr, "side": "LONG" if direction > 0 else "SHORT",
                     "cost_class": _futures_group(sym)})
    if len(legs) < MIN_FUTURES_MARKETS:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_markets": len(legs), "skipped": skipped}
    legs = _normalise_gross(legs, 1.0)
    return {"state": "OK", "legs": legs, "n_markets": len(legs),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: l["cost_class"]
                                  for l in legs}}


def _fx_cross_section(spec: dict) -> dict:
    p = spec["parameters"]
    available = set(MD.fx_spot_symbols())
    scores, marks, skipped = {}, {}, []
    for sym in G10_USD_PER_FOREIGN:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        v = MD.total_return(s, p["formation_days"])
        if v is not None:
            scores[sym] = v
    if len(scores) < MIN_FX_PAIRS:
        return {"state": "INSUFFICIENT_PAIRS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = int(p["n_per_leg"])
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": "FX_SPOT"})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": "FX_SPOT"})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: "FX_SPOT" for l in legs}}


def _vx_carry(spec: dict) -> dict:
    front = MD.closes("&VX")
    spot = MD.closes("$VIX")
    if front is None or spot is None:
        return {"state": "NO_DATA", "legs": []}
    j = front.align(spot, join="inner")
    if not len(j[0]):
        return {"state": "NO_OVERLAP", "legs": []}
    f, sp = float(j[0].iloc[-1]), float(j[1].iloc[-1])
    if sp <= 0:
        return {"state": "BAD_SPOT", "legs": []}
    basis = f / sp - 1.0
    direction = -1.0 if basis > 0 else 1.0
    legs = [{"instrument": "&VX", "weight": direction, "score": basis,
             "side": "SHORT" if direction < 0 else "LONG",
             "cost_class": "VOLATILITY_FUTURES"}]
    return {"state": "OK", "legs": legs, "basis": basis,
            "front": f, "spot": sp, "marks": {"&VX": f, "$VIX": sp},
            "cost_class_by_leg": {"&VX": "VOLATILITY_FUTURES"}}


def _rates_rv(spec: dict) -> dict:
    p = spec["parameters"]
    zn, zt = MD.closes("&ZN"), MD.closes("&ZT")
    if zn is None or zt is None:
        return {"state": "NO_DATA", "legs": []}
    v_zn = MD.realised_vol(zn, K["volatility_days"])
    v_zt = MD.realised_vol(zt, K["volatility_days"])
    if not v_zn or not v_zt:
        return {"state": "SHORT_HISTORY", "legs": []}
    a = np.log(zn).diff()
    b = np.log(zt).diff()
    j = a.align(b, join="inner")
    spread_ret = (j[0] / v_zn - j[1] / v_zt).dropna()
    win = int(p["spread_z_days"])
    if len(spread_ret) < win + 1:
        return {"state": "SHORT_HISTORY", "legs": []}
    level = spread_ret.rolling(win).sum().dropna()
    z = MD.zscore_last(level, win)
    if z is None:
        return {"state": "NO_ZSCORE", "legs": []}
    if abs(z) < float(p["entry_z"]):
        return {"state": "FLAT_NO_SIGNAL", "legs": [], "z": z,
                "marks": {"&ZN": float(zn.iloc[-1]), "&ZT": float(zt.iloc[-1])}}
    direction = -1.0 if z > 0 else 1.0          # fade the stretched spread
    legs = [{"instrument": "&ZN", "weight": direction / v_zn, "score": z,
             "side": "LONG" if direction > 0 else "SHORT",
             "cost_class": "RATES_FUTURES"},
            {"instrument": "&ZT", "weight": -direction / v_zt, "score": z,
             "side": "SHORT" if direction > 0 else "LONG",
             "cost_class": "RATES_FUTURES"}]
    legs = _normalise_gross(legs, 1.0)
    return {"state": "OK", "legs": legs, "z": z,
            "marks": {"&ZN": float(zn.iloc[-1]), "&ZT": float(zt.iloc[-1])},
            "cost_class_by_leg": {"&ZN": "RATES_FUTURES",
                                  "&ZT": "RATES_FUTURES"}}


def _commodity_cross_section(spec: dict) -> dict:
    p = spec["parameters"]
    available = set(MD.continuous_futures())
    scores, marks, skipped = {}, {}, []
    for sym in COMMODITY_MARKETS:
        if sym not in available:
            skipped.append({"instrument": sym, "why": "NOT_IN_DATABASE"})
            continue
        s = MD.closes(sym)
        if s is None:
            skipped.append({"instrument": sym, "why": "NO_BARS"})
            continue
        marks[sym] = float(s.iloc[-1])
        if MD.has_non_positive(s, p["formation_days"] + 1):
            skipped.append({"instrument": sym, "why": MD.NON_POSITIVE_PRICE,
                            "detail": "a percentage return is undefined "
                                      "across this window"})
            continue
        v = MD.total_return(s, p["formation_days"])
        if v is not None:
            scores[sym] = v
    if len(scores) < 9:
        return {"state": "INSUFFICIENT_MARKETS", "legs": [],
                "n_scored": len(scores), "skipped": skipped}
    k = max(1, int(round(len(scores) * float(p["leg_fraction"]))))
    order = sorted(scores.items(), key=lambda kv: kv[1])
    legs = []
    for sym, sc in order[-k:]:
        legs.append({"instrument": sym, "weight": 0.5 / k, "score": sc,
                     "side": "LONG", "cost_class": "COMMODITY_FUTURES"})
    for sym, sc in order[:k]:
        legs.append({"instrument": sym, "weight": -0.5 / k, "score": sc,
                     "side": "SHORT", "cost_class": "COMMODITY_FUTURES"})
    return {"state": "OK", "legs": legs, "n_scored": len(scores),
            "marks": marks, "skipped": skipped,
            "cost_class_by_leg": {l["instrument"]: "COMMODITY_FUTURES"
                                  for l in legs}}


def _index_trend(spec: dict) -> dict:
    p = spec["parameters"]
    s = MD.closes(BENCHMARK_EQUITY)
    if s is None or len(s) < int(p["filter_days"]) + 1:
        return {"state": "SHORT_HISTORY", "legs": []}
    ma = float(s.iloc[-int(p["filter_days"]):].mean())
    px = float(s.iloc[-1])
    invested = px > ma
    legs = ([{"instrument": BENCHMARK_EQUITY, "weight": 1.0,
              "score": px / ma - 1.0, "side": "LONG",
              "cost_class": "US_ETF"}] if invested else [])
    return {"state": "OK", "legs": legs, "invested": invested,
            "price": px, "moving_average": ma,
            "marks": {BENCHMARK_EQUITY: px},
            "cost_class_by_leg": {BENCHMARK_EQUITY: "US_ETF"}}


_OWNERS = {
    "_eq_cross_section": _eq_cross_section,
    "_futures_trend": _futures_trend,
    "_fx_cross_section": _fx_cross_section,
    "_vx_carry": _vx_carry,
    "_rates_rv": _rates_rv,
    "_commodity_cross_section": _commodity_cross_section,
    "_index_trend": _index_trend,
}


def build(spec: dict) -> dict:
    """Run one challenger's frozen rule over the latest owned data.

    Returns the book it would hold, its per-leg diagnostics and the hashes
    that pin the inputs. Never writes anything.
    """
    owner = _OWNERS.get(spec["signal_owner"])
    if owner is None:                            # pragma: no cover
        return {"state": "NO_SIGNAL_OWNER", "legs": []}
    out = owner(spec)
    legs = out.get("legs") or []
    out["gross_notional"] = float(sum(abs(float(l["weight"])) for l in legs))
    out["net_notional"] = float(sum(float(l["weight"]) for l in legs))
    out["n_legs"] = len(legs)
    out["market_state_snapshot_hash"] = sha(out.get("marks") or {})
    out["input_evidence_hash"] = sha(
        [(l["instrument"], round(float(l["score"]), 10),
          round(float(l["weight"]), 10)) for l in legs])
    out.pop("marks", None)
    return out


def expected_cost_bps(book: dict, spec: dict) -> float:
    """Cost of OPENING the book, in bps of gross notional.

    Charged on traded notional - Release 31's correction - and only for the
    entry side here; the exit side is charged again by the judge when the
    position is closed at maturity.
    """
    legs = book.get("legs") or []
    if not legs:
        return 0.0
    by_leg = book.get("cost_class_by_leg") or {}
    total = 0.0
    for l in legs:
        klass = (l.get("cost_class") or by_leg.get(l["instrument"])
                 or spec.get("cost_class") or "US_EQUITY")
        half = C.COST_BPS_PER_SIDE.get(klass, 5.0) + C.SLIPPAGE_BPS_PER_SIDE
        total += abs(float(l["weight"])) * half
    return float(total)
