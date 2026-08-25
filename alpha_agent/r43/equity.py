"""alpha_agent.r43.equity - Track L: market-neutral residual equity.

Three decisions define this lane, and each one is a refusal to take a
shortcut the estate has taken before.

**Survivorship.** The universe is the Norgate ``S&P 500 Current & Past``
watchlist - 1,897 symbols including every delisted constituent - and
membership is resolved PER DATE from Norgate's own point-in-time
``index_constituent_timeseries``. A name enters the cross-section on the day
it entered the index and leaves on the day it left. Prices are TOTALRETURN
adjusted, so a dividend is neither a gap nor a phantom loss.

**Neutralisation without look-ahead.** The obvious way to be
"sector-neutral" is to demean by GICS sector. The estate's own Stage-8 work
already labelled the current Norgate GICS map
``PROVISIONAL_CLASSIFICATION_LOOKAHEAD``: a company's CURRENT sector is not
its sector in 2004, and using it to neutralise a 2004 cross-section leaks.
So this lane does not use sectors at all. It residualises each name against
the equal-weight universe return with a ROLLING, LAGGED beta, which is
causal by construction, and reports itself as beta- and dollar-neutral
rather than claiming a sector neutrality it cannot honestly deliver.

**Capital.** A cash-neutral long/short does not immobilise nothing. It posts
Reg-T style capital equal to the long side's notional, earns a short rebate
BELOW the risk-free rate and pays general-collateral borrow. The frozen
contract's ``FUNDED_LONG_SHORT_EQUITY`` class encodes exactly that: rho =
0.60, so the book is charged the 40% of the risk-free rate its capital does
not earn. This is the only lane in Release 43 whose control is neither zero
nor the whole risk-free rate.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd

from . import contract as C
from . import burden as B
from . import data_dir
from . import judge as J
from .rv import apply_expression
from ..r41 import evidence as EV

warnings.filterwarnings("ignore")

CALCULATION_OWNER = "alpha_agent.r43.equity"

WATCHLIST = "S&P 500 Current & Past"
INDEX_NAME = "S&P 500"
START = "1998-01-01"
BETA_WIN = 252
VOL_WIN = 60
MOM_SKIP = 21
MOM_WIN = 252
REV_WIN = 5
MIN_NAMES = 50
EMBARGO = 21
DECILE = 0.20            # long the top 20%, short the bottom 20%
EXPRESSIONS = {"CONTINUOUS": None, "BAND_15_05": (1.5, 0.5)}
SIGNALS = ("RESIDUAL_REVERSAL", "RESIDUAL_MOMENTUM", "LOW_RESIDUAL_VOL",
           "ILLIQUIDITY")
COST_GROUP = "US_EQUITY_SINGLE_NAME"
CACHE = "r43_equity_panel.pkl"


# --------------------------------------------------------------------------- #
# Panel
# --------------------------------------------------------------------------- #
def build_panel(*, limit: int = None, refresh: bool = False) -> dict:
    """Survivorship-safe total-return panel with PIT index membership."""
    path = data_dir("equity") / CACHE
    if path.exists() and not refresh:
        try:
            return pd.read_pickle(path)
        except Exception:
            pass
    import norgatedata as nd
    syms = nd.watchlist_symbols(WATCHLIST)
    if limit:
        syms = syms[:limit]
    px, mem, dv = {}, {}, {}
    for s in syms:
        try:
            d = nd.price_timeseries(
                s, stock_price_adjustment_setting=(
                    nd.StockPriceAdjustmentType.TOTALRETURN),
                padding_setting=nd.PaddingType.NONE,
                start_date=START, format="pandas-dataframe")
        except Exception:
            continue
        if d is None or len(d) < 300:
            continue
        px[s] = pd.to_numeric(d["Close"], errors="coerce")
        dv[s] = pd.to_numeric(d.get("Turnover"), errors="coerce")
        try:
            m = nd.index_constituent_timeseries(
                s, INDEX_NAME, timeseriesformat="pandas-dataframe",
                start_date=START)
            mem[s] = pd.to_numeric(m.iloc[:, 0], errors="coerce")
        except Exception:
            continue
    if not px:
        return None
    close = pd.DataFrame(px).sort_index()
    close.index = pd.DatetimeIndex(close.index).tz_localize(None).normalize()
    member = pd.DataFrame(mem).reindex(close.index).fillna(0.0) > 0
    turnover = pd.DataFrame(dv).reindex(close.index)
    # Returns are computed ONLY across consecutive valid closes. pandas'
    # pct_change pads by default, which turns a delisting gap or a trading
    # halt into a single enormous "return" - and a cross-sectional book that
    # sorts on those fabricated moves produces an impossible t-statistic
    # rather than an edge. Requiring both endpoints to exist is the fix.
    prev = close.shift(1)
    ret = (close / prev - 1.0).where(close.notna() & prev.notna())
    ret = ret.where(member)
    body = {"close": close, "member": member, "turnover": turnover,
            "ret": ret, "symbols": list(close.columns),
            "n_symbols": close.shape[1],
            "max_abs_daily_return": float(np.nanmax(np.abs(ret.to_numpy()))),
            "return_construction": "consecutive valid closes only; no "
                                   "padding across gaps",
            "span": [str(close.index[0].date()), str(close.index[-1].date())]}
    try:
        pd.to_pickle(body, path)
    except Exception:
        pass
    return body


def residuals(panel: dict) -> pd.DataFrame:
    """Beta-residual returns against the equal-weight universe.

    beta is a rolling 252-session regression LAGGED one session, so the
    residual on date t uses a hedge ratio computable on t-1.
    """
    ret = panel["ret"]
    mkt = ret.mean(axis=1)
    cov = ret.rolling(BETA_WIN, min_periods=BETA_WIN // 2).cov(mkt)
    var = mkt.rolling(BETA_WIN, min_periods=BETA_WIN // 2).var()
    beta = cov.div(var.replace(0.0, np.nan), axis=0).shift(1) \
        .clip(-3.0, 3.0)
    return (ret - beta.mul(mkt, axis=0)).where(panel["member"])


def signal_frame(panel: dict, resid: pd.DataFrame, signal: str
                 ) -> pd.DataFrame:
    if signal == "RESIDUAL_REVERSAL":
        raw = -resid.rolling(REV_WIN, min_periods=REV_WIN).sum()
    elif signal == "RESIDUAL_MOMENTUM":
        raw = (resid.rolling(MOM_WIN, min_periods=MOM_WIN // 2).sum()
               - resid.rolling(MOM_SKIP, min_periods=MOM_SKIP).sum())
    elif signal == "LOW_RESIDUAL_VOL":
        raw = -resid.rolling(VOL_WIN, min_periods=VOL_WIN // 2).std()
    elif signal == "ILLIQUIDITY":
        dv = panel["turnover"].rolling(VOL_WIN, min_periods=VOL_WIN // 2) \
            .mean()
        raw = (panel["ret"].abs()
               / dv.replace(0.0, np.nan)).rolling(
            VOL_WIN, min_periods=VOL_WIN // 2).mean()
    else:
        raise ValueError("unknown signal %r" % signal)
    raw = raw.where(panel["member"])
    n = raw.notna().sum(axis=1)
    mu, sd = raw.mean(axis=1), raw.std(axis=1).replace(0.0, np.nan)
    return raw.sub(mu, axis=0).div(sd, axis=0).where(n >= MIN_NAMES)


def book(panel: dict, resid: pd.DataFrame, signal: str, expression: str, *,
         extra_lag: int = 0, frame_override: pd.DataFrame = None) -> dict:
    z = (signal_frame(panel, resid, signal) if frame_override is None
         else frame_override)
    ret = panel["ret"]
    held = {}
    for c in z.columns:
        held[c] = apply_expression(z[c], expression, extra_lag=extra_lag)
    pos = pd.DataFrame(held, index=z.index).where(panel["member"], 0.0)
    # Keep only the conviction tails so the book is a sparse long/short, not
    # a 500-name index tracker with a tilt.
    #
    # The tails MUST be ranked on the LAGGED signal, exactly as the position
    # sign is. Ranking on the same-day signal while holding a lagged sign
    # selects the names that moved today and then takes yesterday's view of
    # them - which is look-ahead in the SELECTION even though every position
    # is lagged. It shows up as an impossible t-statistic, not as a subtle
    # bias: this bug produced -304%/yr at t -50 before it was found.
    zl = z.shift(1 + int(extra_lag))
    rank = zl.rank(axis=1, pct=True)
    tails = ((rank >= 1.0 - DECILE) | (rank <= DECILE))
    pos = pos.where(tails, 0.0)
    # Dollar-neutralise, then scale to unit gross notional.
    net = pos.sum(axis=1)
    active = (pos != 0).sum(axis=1).replace(0, np.nan)
    pos = pos.sub(net.div(active), axis=0).where(pos != 0, 0.0)
    gross_n = pos.abs().sum(axis=1).replace(0.0, np.nan)
    pos = pos.div(gross_n, axis=0).fillna(0.0)
    if pos.abs().to_numpy().sum() < 10:
        return None
    gross = (pos * ret).sum(axis=1).rename("gross")
    bps = float(C.COST_BPS_PER_SIDE[COST_GROUP])
    cost = (pos.diff().abs().sum(axis=1).fillna(0.0)
            * (bps / 1e4)).rename("cost")
    # Reg-T: capital equals the LONG side's notional, i.e. half of a unit
    # gross book. That is 2:1 gross exposure on committed capital.
    K = float(C.COLLATERAL_CLASSES["FUNDED_LONG_SHORT_EQUITY"][
        "committed_capital"]) * 0.5
    return {"gross": gross, "cost": cost,
            "turnover": pos.diff().abs().sum(axis=1).fillna(0.0),
            "committed_capital": K, "index": pos.index,
            "n_names_mean": float(np.nanmean((pos != 0).sum(axis=1))),
            "positions": pos}


def score(bk: dict, dates=None, *, capital: float = None) -> dict:
    K = float(capital if capital is not None else bk["committed_capital"])
    b = J.implementable_book(
        bk["gross"], pd.Series(1.0, index=bk["index"]), committed_capital=K,
        collateral_class="FUNDED_LONG_SHORT_EQUITY", cost=bk["cost"],
        day_count=J.TRADING_DAYS)
    card = J.score(b, dates, day_count=J.TRADING_DAYS)
    d = b if dates is None else b.reindex(pd.DatetimeIndex(dates))
    card["turnover_ann"] = float(
        np.nanmean(bk["turnover"].reindex(d.index)) * J.TRADING_DAYS)
    card["cost_share_of_gross"] = (
        float(card["cost_ann_on_notional"] / card["gross_ann_on_notional"])
        if card.get("gross_ann_on_notional") else None)
    card["n_names_mean"] = bk["n_names_mean"]
    card.pop("diff_stream", None)
    return card


def _gate(card: dict) -> dict:
    g = C.RESEARCH_CANDIDATE_GATE
    t = card.get("excess_t_hac")
    cs = (card.get("cost_stress") or {}).get("x2") or {}
    checks = {
        "t_min": bool(t is not None
                      and t >= g["after_cost_excess_t_hac_min"]),
        "same_sign_halves": bool(card.get("same_sign_halves")),
        "positive_at_2x_cost": bool((cs.get("excess_ann") or 0) > 0),
        "min_effective_decisions": bool(
            (card.get("effective_sample") or {}).get("ess", 0)
            >= g["min_effective_decisions"]),
        "positive_on_committed_capital": bool(
            (card.get("excess_ann") or 0) > 0),
    }
    return {"checks": checks, "passes": all(checks.values())}


def run_lane(lane: str = "L_EQUITY_NEUTRAL", *, limit: int = None) -> dict:
    spec_lane = C.LANES[lane]
    panel = build_panel(limit=limit)
    if panel is None:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE"}
    resid = residuals(panel)
    live = panel["member"].sum(axis=1)
    idx = panel["ret"].index[live >= MIN_NAMES]
    if len(idx) < 750:
        return {"lane": lane, "state": "HISTORICAL_DATA_UNAVAILABLE",
                "reason": "membership coverage under 750 sessions"}
    zones = EV.zone_split(idx, embargo=EMBARGO)

    screened, advanced, cache = [], [], {}
    for signal in SIGNALS:
        for expression in EXPRESSIONS:
            bk = book(panel, resid, signal, expression)
            if bk is None:
                continue
            cache[(signal, expression)] = bk
            a = score(bk, zones["A"])
            screened.append({
                "signal": signal, "expression": expression,
                "zone_a_excess_ann": a.get("excess_ann"),
                "zone_a_t": a.get("excess_t_hac"),
                "zone_a_sharpe": a.get("sharpe"),
                "zone_a_turnover_ann": a.get("turnover_ann"),
                "zone_a_cost_share_of_gross": a.get("cost_share_of_gross"),
                "n_names_mean": a.get("n_names_mean")})

    adv = sorted([r for r in screened if (r["zone_a_t"] or -9)
                  >= spec_lane["advance_t"]],
                 key=lambda r: -r["zone_a_t"])[:spec_lane["cap"]]
    for r in adv:
        bk = cache[(r["signal"], r["expression"])]
        spec = {"information_family": "EQUITY_RESIDUAL",
                "asset_family": "US_LARGE_CAP_EQUITY",
                "horizon": "1s_hold_to_exit"
                if r["expression"] != "CONTINUOUS" else "1s",
                "economic_expression": "EQUITY_MARKET_NEUTRAL",
                "representation": "%s_%s" % (r["signal"], r["expression"]),
                "model": "TRANSPARENT_RULE", "hyperparameter_budget": 0,
                "parent_hypotheses": ["R39 equity residual",
                                      "R41 horizon engine"],
                "validation_touches": 1, "lane": lane}
        cid = B.record_zone_b(spec, family="EQUITY_RESIDUAL", lane=lane)
        card = score(bk, zones["B"])
        advanced.append({"candidate_id": cid, "spec": spec,
                         "signal": r["signal"], "expression": r["expression"],
                         "zone_a_t": r["zone_a_t"], "zone_b": card,
                         "gate": _gate(card),
                         "committed_capital": bk["committed_capital"]})

    return {
        "lane": lane, "state": "EXECUTED",
        "question": spec_lane["question"],
        "universe": {"watchlist": WATCHLIST,
                     "symbols_pulled": panel["n_symbols"],
                     "span": panel["span"],
                     "price_adjustment": "TOTALRETURN",
                     "membership": "Norgate index_constituent_timeseries, "
                                   "resolved PER DATE",
                     "survivorship_safe": True,
                     "delisted_included": True},
        "neutralisation": {
            "beta": "rolling %d-session, lagged one session" % BETA_WIN,
            "dollar": True, "sector": False,
            "sector_omitted_because": "the only owned sector map is current "
                                      "Norgate GICS, which the estate itself "
                                      "labelled PROVISIONAL_CLASSIFICATION_"
                                      "LOOKAHEAD; neutralising a 2004 "
                                      "cross-section with a 2026 sector map "
                                      "leaks"},
        "collateral_class": "FUNDED_LONG_SHORT_EQUITY",
        "rho": C.COLLATERAL_CLASSES["FUNDED_LONG_SHORT_EQUITY"][
            "collateral_earns_rf"],
        "zones": {k: zones["%s_range" % k.lower()] for k in ("A", "B", "C")},
        "screened": screened, "advanced": advanced,
        "advance_rule": {"advance_t": spec_lane["advance_t"],
                         "cap": spec_lane["cap"], "signed": True},
    }
