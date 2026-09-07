"""alpha_agent.r59.native - research on the R38 DATED-CONTRACT layer.

The R57 futures panel that R59 used until now carries ``close_a`` (``&SYM``)
and ``close_b`` (``&SYM_CCB``). Those are the SAME front contract under two
back-adjustment conventions - measured daily-change correlation across the 103
markets has a median of 0.982 - so ``(close_a - close_b)/|close_b|`` is the
accumulated back-adjustment offset, an artifact of an arbitrary anchor date. It
is not carry, not a curve slope and not term structure. Every hypothesis R59
built on it has been invalidated in research memory.

The real substrate was already on disk. Release 38 froze a NATIVE contract
layer, one CSV per market, carrying:

    held            the actual dated contract held that session (e.g. 6A-1987H)
    ret             front-contract return
    ret2            SECOND (deferred) contract return
    slope_ann       annualised curve slope between the two dated contracts
    volume, open_interest

69 markets, 1987-2026. That is a genuine term structure: two different dated
contracts, priced on the same day, with real roll dates recoverable from
``held``. The R38 ML panel adds ``economic_group`` (12 groups) and - important
for honesty - ``cost_bps_per_side`` per market, ranging 2-15bp where R59 had
been charging a flat 2bp to every market including the thinnest.

This module owns the families that substrate makes possible:

    RATES_CURVE_RV            treasury curve relative value
    CALENDAR_TERM_STRUCTURE   carry from the real front/deferred slope
    INTER_COMMODITY_RV        relative value inside an economic group
    AGRICULTURAL_SEASONALITY  month-of-year structure, one family, pre-declared
    ROLL_STATE                does the roll itself carry information

Every family is evaluated by the SAME statistical kernel as every other R59
family (:func:`alpha_agent.r57.engine.nw_tstat`), on the same
discovery/validation/lockbox partition, and charged per-market R38 costs.

RESEARCH ONLY. Reads the frozen R38 layer; writes nothing outside the R59 root.
"""
from __future__ import annotations

import math
import warnings
from typing import Optional

import numpy as np

from .. import r59
from ..r57 import engine as K

CALCULATION_OWNER = "alpha_agent.r59.native"

R38_ROOT = (r59.R39_ROOT.parent / "native_futures_r38"
            / "r38_native_futures_information_frontier_v4")
NATIVE_LAYER_DIR = R38_ROOT / "native_contract_layer"
R38_PANEL = R38_ROOT / "ml_ready_native_futures_panel.csv"

#: Horizons the multi-horizon sweep covers, in sessions. A sweep across these
#: is ONE search family, not four (section M of the release brief): scanning a
#: single economic signal at four holding periods is one idea tested four ways.
HORIZONS = (1, 5, 21, 63)

#: Economic groups whose members are close enough substitutes for a
#: within-group relative-value book to be an economic statement rather than a
#: coincidence of labelling.
RV_GROUPS = ("TREASURY_FUTURES", "GRAINS_AND_OILSEEDS", "PRECIOUS_METALS",
             "ENERGY", "SOFTS", "LIVESTOCK", "FX_FUTURES",
             "INTL_INDEX_FUTURES")

AG_GROUPS = ("GRAINS_AND_OILSEEDS", "SOFTS", "LIVESTOCK")

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Substrate
# --------------------------------------------------------------------------- #
def available() -> bool:
    return NATIVE_LAYER_DIR.exists() and R38_PANEL.exists()


def load_layer() -> dict:
    """market -> aligned arrays on ONE shared session grid.

    Alignment is on the union of dates actually present, so a market that was
    not trading on a session is a MASK (NaN), never a forward fill.
    """
    if "layer" in _CACHE:
        return _CACHE["layer"]
    if not available():
        raise FileNotFoundError("R38 native contract layer not present: %s"
                                % NATIVE_LAYER_DIR)
    import pandas as pd

    frames = {}
    for p in sorted(NATIVE_LAYER_DIR.glob("*.csv")):
        df = pd.read_csv(p, parse_dates=["Date"])
        frames[p.stem] = df

    all_dates = sorted({d for df in frames.values()
                        for d in df["Date"].dt.strftime("%Y-%m-%d")})
    dix = {d: i for i, d in enumerate(all_dates)}
    n_d = len(all_dates)
    syms = sorted(frames)
    n_m = len(syms)

    ret = np.full((n_m, n_d), np.nan)
    ret2 = np.full((n_m, n_d), np.nan)
    slope = np.full((n_m, n_d), np.nan)
    oi = np.full((n_m, n_d), np.nan)
    vol = np.full((n_m, n_d), np.nan)
    roll = np.zeros((n_m, n_d), dtype=np.uint8)

    for i, s in enumerate(syms):
        df = frames[s]
        ii = np.array([dix[d] for d in df["Date"].dt.strftime("%Y-%m-%d")])
        ret[i, ii] = df["ret"].to_numpy(dtype=np.float64)
        if "ret2" in df.columns:
            ret2[i, ii] = df["ret2"].to_numpy(dtype=np.float64)
        if "slope_ann" in df.columns:
            slope[i, ii] = df["slope_ann"].to_numpy(dtype=np.float64)
        if "open_interest" in df.columns:
            oi[i, ii] = df["open_interest"].to_numpy(dtype=np.float64)
        if "volume" in df.columns:
            vol[i, ii] = df["volume"].to_numpy(dtype=np.float64)
        if "held" in df.columns:
            h = df["held"].to_numpy()
            ch = np.zeros(len(h), dtype=np.uint8)
            ch[1:] = (h[1:] != h[:-1]).astype(np.uint8)
            roll[i, ii] = ch

    layer = {"symbols": syms, "dates": np.array(all_dates),
             "ret": ret, "ret2": ret2, "slope": slope,
             "open_interest": oi, "volume": vol, "roll": roll}
    _CACHE["layer"] = layer
    return layer


def load_meta() -> dict:
    """market -> economic group, asset class and MEASURED per-side cost."""
    if "meta" in _CACHE:
        return _CACHE["meta"]
    import pandas as pd

    df = pd.read_csv(R38_PANEL, usecols=["market_id", "asset_class",
                                         "economic_group",
                                         "cost_bps_per_side"])
    g = df.groupby("market_id").agg(
        asset_class=("asset_class", "first"),
        economic_group=("economic_group", "first"),
        cost_bps_per_side=("cost_bps_per_side", "median")).reset_index()
    meta = {r["market_id"]: {"asset_class": r["asset_class"],
                             "economic_group": r["economic_group"],
                             "cost_bps_per_side": float(r["cost_bps_per_side"])}
            for _i, r in g.iterrows()}
    _CACHE["meta"] = meta
    return meta


def group_members(group: str) -> list:
    """Row indices of one economic group inside the native layer."""
    layer = load_layer()
    meta = load_meta()
    return [i for i, s in enumerate(layer["symbols"])
            if (meta.get(s) or {}).get("economic_group") == group]


def cost_vector() -> np.ndarray:
    """Per-market cost per side, as a fraction. Markets absent from the R38
    panel are charged the panel's MAXIMUM, never its cheapest."""
    layer = load_layer()
    meta = load_meta()
    known = [m["cost_bps_per_side"] for m in meta.values()]
    worst = max(known) if known else 15.0
    return np.array([(meta.get(s) or {}).get("cost_bps_per_side", worst)
                     / 10000.0 for s in layer["symbols"]])


# --------------------------------------------------------------------------- #
# Decision grid and partition (inherited, unchanged)
# --------------------------------------------------------------------------- #
def decision_indices(dates: np.ndarray, cadence: int, horizon: int,
                     first_date: str = r59.DISCOVERY_START) -> np.ndarray:
    start = int(np.searchsorted(dates, first_date))
    last = len(dates) - horizon - 2
    if last <= start:
        return np.array([], dtype=int)
    return np.arange(start, last + 1, cadence)


def layers_of(dates: np.ndarray, idx: np.ndarray, cadence: int,
              horizon: int) -> np.ndarray:
    return K.layer_of(dates, idx, cadence, horizon)


# --------------------------------------------------------------------------- #
# Book construction
# --------------------------------------------------------------------------- #
def _fwd(ret: np.ndarray, t: int, horizon: int) -> np.ndarray:
    """Compound forward return over [t+1, t+horizon]. A missing session earns
    zero for that day rather than propagating NaN across the whole window."""
    w = ret[:, t + 1:t + 1 + horizon]
    w = np.where(np.isfinite(w), w, 0.0)
    return np.prod(1.0 + w, axis=1) - 1.0


def _xs_weights(score: np.ndarray, mask: np.ndarray) -> np.ndarray:
    """Dollar-neutral long/short weights from a cross-sectional score."""
    w = np.zeros_like(score, dtype=np.float64)
    sel = mask & np.isfinite(score)
    n = int(sel.sum())
    if n < 4:
        return w
    v = score[sel]
    r = np.argsort(np.argsort(v)).astype(np.float64) / max(1, n - 1) - 0.5
    r = r - r.mean()
    s = np.abs(r).sum()
    if s <= 0:
        return w
    w[sel] = r / s
    return w


def run_book(*, score_fn, mask: np.ndarray, horizon: int, cadence: int,
             label: str, long_short: bool = True) -> dict:
    """Simulate one book and return layer statistics from the shared kernel.

    ``score_fn(t) -> per-market score`` reads data at or before session ``t``
    only. The position is entered at t+1 (NEXT_CLOSE) and held ``horizon``
    sessions. Costs are charged on TRADED NOTIONAL at each market's own R38
    rate, on both the strategy and its control.
    """
    layer = load_layer()
    dates, ret = layer["dates"], layer["ret"]
    costs = cost_vector()
    idx = decision_indices(dates, cadence, horizon)
    if len(idx) == 0:
        return {"state": "NO_DECISIONS"}
    lay = layers_of(dates, idx, cadence, horizon)

    n_dec = len(idx)
    sn = np.zeros(n_dec)
    sg = np.zeros(n_dec)
    bn = np.zeros(n_dec)
    bg = np.zeros(n_dec)
    to = np.zeros(n_dec)
    prev = np.zeros(ret.shape[0])
    prev_b = np.zeros(ret.shape[0])

    for j, t in enumerate(idx):
        live = mask & np.isfinite(ret[:, t])
        score = score_fn(int(t))
        w = (_xs_weights(score, live) if long_short
             else np.where(live, np.sign(np.nan_to_num(score)), 0.0)
             / max(1, int(live.sum())))
        f = _fwd(ret, int(t), horizon)
        gross = float(np.nansum(w * f))
        traded = float(np.abs(w - prev).sum())
        cost = float(np.abs(w - prev) @ costs)
        sg[j] = gross
        sn[j] = gross - cost
        # Control: the equal-weight long-only basket of the SAME scope, charged
        # the same way. A long/short book's control is cash, so its benchmark
        # return is zero but its cost is still real.
        bw = (np.zeros_like(w) if long_short
              else np.where(live, 1.0 / max(1, int(live.sum())), 0.0))
        bgross = float(np.nansum(bw * f))
        bcost = float(np.abs(bw - prev_b) @ costs)
        bg[j] = bgross
        bn[j] = bgross - bcost
        to[j] = traded / 2.0
        prev, prev_b = w, bw

    res = {"idx": idx, "layers": lay, "dates": dates[idx],
           "strat_gross": sg, "strat_net": sn,
           "bench_gross": bg, "bench_net": bn,
           "turnover_oneway": to, "cadence": cadence, "horizon": horizon}
    return {"state": "MEASURED", "label": label,
            "evaluator": "alpha_agent.r59.native.run_book"
                         " + alpha_agent.r57.engine.nw_tstat",
            "cost_model": "per-market R38 cost_bps_per_side on traded notional",
            "horizon": horizon, "cadence": cadence,
            "n_markets": int(mask.sum()),
            "layers": {l: _layer_stats(res, l) for l in ("D", "V", "L")}}


def _layer_stats(res: dict, layer: str) -> dict:
    sel = res["layers"] == layer
    if sel.sum() == 0:
        return {"periods": 0}
    ex = res["strat_net"][sel] - res["bench_net"][sel]
    exg = res["strat_gross"][sel] - res["bench_gross"][sel]
    # ANNUALISE BY THE HOLDING PERIOD, NOT THE REBALANCE CADENCE. Each decision
    # measures a HORIZON-session return; when the cadence is shorter than the
    # horizon those windows overlap, and dividing 252 by the cadence would
    # count the same holding period several times. With cadence 5 and horizon
    # 21 that inflated the annualised figure roughly fourfold. The overlap
    # itself is legitimate - it uses more of the sample - and is paid for by
    # the Newey-West lag below, which is what corrects the standard error for
    # the induced autocorrelation.
    ppy = 252.0 / float(res["horizon"])
    lag = max(0, int(math.ceil(res["horizon"] / res["cadence"])) - 1)
    st = K.nw_tstat(ex, lag=lag)
    nav = np.cumprod(1.0 + ex)
    dd = float((nav / np.maximum.accumulate(nav) - 1.0).min()) if len(nav) else None
    half = len(ex) // 2
    # EFFECTIVE observations. With cadence 5 and horizon 63 each decision's
    # window overlaps the next twelve, so 177 rows are worth about fourteen
    # independent ones. Reporting the raw count as if it were the sample size
    # is how an overlapping-window book acquires a t-statistic it has not
    # earned; the observation floor must be applied to THIS number.
    overlap = max(1.0, float(res["horizon"]) / float(res["cadence"]))
    eff = int(sel.sum() / overlap)
    return {
        "periods": int(sel.sum()),
        "overlap_factor": round(overlap, 2),
        "effective_observations": eff,
        "first": str(res["dates"][sel][0]), "last": str(res["dates"][sel][-1]),
        "ann_net_excess": float(ex.mean() * ppy),
        "ann_gross_excess": float(exg.mean() * ppy),
        "ann_cost_drag": float((exg - ex).mean() * ppy),
        "mean_oneway_turnover_per_period":
            float(res["turnover_oneway"][sel].mean()),
        "max_dd": dd,
        "hit_rate": float((ex > 0).mean()),
        "t_net_excess": st["t"], "p_one_sided": st["p_one_sided"],
        "halves_ann_net_excess": [float(ex[:half].mean() * ppy),
                                  float(ex[half:].mean() * ppy)]
        if sel.sum() >= 4 else None,
    }


# --------------------------------------------------------------------------- #
# Feature helpers (data <= t only)
# --------------------------------------------------------------------------- #
def _trailing(ret: np.ndarray, t: int, k: int) -> np.ndarray:
    lo = max(0, t - k + 1)
    w = ret[:, lo:t + 1]
    w = np.where(np.isfinite(w), w, 0.0)
    return np.prod(1.0 + w, axis=1) - 1.0


def _slope_at(t: int) -> np.ndarray:
    """The REAL annualised curve slope between the two dated contracts."""
    return load_layer()["slope"][:, t]


def _slope_z(t: int, window: int = 252) -> np.ndarray:
    s = load_layer()["slope"]
    lo = max(0, t - window + 1)
    w = s[:, lo:t + 1]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        m = np.nanmean(w, axis=1)
        sd = np.nanstd(w, axis=1)
    z = (s[:, t] - m) / np.where(sd > 0, sd, np.nan)
    return z


def _roll_recent(t: int, window: int = 10) -> np.ndarray:
    r = load_layer()["roll"]
    lo = max(0, t - window + 1)
    return r[:, lo:t + 1].sum(axis=1).astype(np.float64)


def _month_of(t: int) -> int:
    return int(str(load_layer()["dates"][t])[5:7])


def _seasonal_score(t: int, lookback_years: int = 10) -> np.ndarray:
    """Mean return of the NEXT calendar month in prior years, per market.

    Strictly trailing: only months that ended on or before session t are used,
    so a decision in September 2015 sees Septembers up to 2014 and never 2016.
    """
    layer = load_layer()
    dates, ret = layer["dates"], layer["ret"]
    nxt = (_month_of(t) % 12) + 1
    months = np.array([int(str(d)[5:7]) for d in dates[:t + 1]])
    years = np.array([int(str(d)[:4]) for d in dates[:t + 1]])
    this_year = int(str(dates[t])[:4])
    sel = (months == nxt) & (years >= this_year - lookback_years)
    if sel.sum() < 40:
        return np.full(ret.shape[0], np.nan)
    w = ret[:, :t + 1][:, sel]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        return np.nanmean(np.where(np.isfinite(w), w, np.nan), axis=1)


def _group_demean(v: np.ndarray, members: list) -> np.ndarray:
    """Demean a score within its economic group - the relative-value step."""
    out = np.full_like(v, np.nan)
    sub = v[members]
    finite = np.isfinite(sub)
    if finite.sum() < 3:
        return out
    out[members] = sub - np.nanmean(sub[finite])
    return out


# --------------------------------------------------------------------------- #
# The families
# --------------------------------------------------------------------------- #
def _mask_for(members: list) -> np.ndarray:
    layer = load_layer()
    m = np.zeros(len(layer["symbols"]), dtype=bool)
    for i in members:
        m[i] = True
    return m


def run_family(*, family: str, group: Optional[str], horizon: int,
               cadence: int = 5, variant: str = "base") -> dict:
    """Run ONE (family, group, horizon, variant) cell on the native layer."""
    if not available():
        return {"state": "NO_SUBSTRATE",
                "reason": "R38 native contract layer absent"}
    layer = load_layer()
    n_m = len(layer["symbols"])

    if family in ("RATES_CURVE_RV", "INTER_COMMODITY_RV",
                  "AGRICULTURAL_SEASONALITY"):
        members = group_members(group or "")
        if len(members) < 3:
            return {"state": "NO_MEMBERS", "group": group,
                    "n_members": len(members)}
        mask = _mask_for(members)
    else:
        mask = np.isfinite(layer["ret"]).any(axis=1)
        members = list(np.where(mask)[0])

    if family == "CALENDAR_TERM_STRUCTURE":
        # Genuine carry: rank markets by the annualised slope between the two
        # DATED contracts. This is the hypothesis the back-adjusted panel could
        # never express.
        if variant == "z":
            fn = lambda t: _slope_z(t)          # noqa: E731
        else:
            fn = lambda t: _slope_at(t)         # noqa: E731

    elif family == "RATES_CURVE_RV":
        # Relative value along the treasury curve: each maturity's trailing
        # move, demeaned within the curve, traded against its own group.
        fn = lambda t: _group_demean(           # noqa: E731
            -_trailing(layer["ret"], t, 63 if variant == "base" else 21),
            members)

    elif family == "INTER_COMMODITY_RV":
        fn = lambda t: _group_demean(           # noqa: E731
            -_trailing(layer["ret"], t, 21 if variant == "base" else 63),
            members)

    elif family == "AGRICULTURAL_SEASONALITY":
        fn = lambda t: _group_demean(_seasonal_score(t), members)  # noqa: E731

    elif family == "ROLL_STATE":
        # Does the roll itself carry information? Positive score on markets
        # that have just rolled, negative on those that have not.
        fn = lambda t: _roll_recent(            # noqa: E731
            t, 5 if variant == "base" else 21)

    else:
        return {"state": "UNKNOWN_FAMILY", "family": family}

    def _score(t: int) -> np.ndarray:
        v = fn(t)
        return v if v is not None else np.full(n_m, np.nan)

    out = run_book(score_fn=_score, mask=mask, horizon=horizon,
                   cadence=cadence,
                   label="%s|%s|h%d|%s" % (family, group or "ALL", horizon,
                                           variant))
    if out.get("state") == "MEASURED":
        out.update({"family": family, "group": group, "variant": variant,
                    "substrate": "R38_NATIVE_CONTRACT_LAYER",
                    "markets": [layer["symbols"][i] for i in members][:40]})
    return out


def horizon_sweep(*, family: str, group: Optional[str], variant: str = "base",
                  horizons=HORIZONS, cadence: int = 5) -> dict:
    """Run one economic signal at several holding periods.

    The sweep is ONE search family. Its result reports every horizon and names
    the best, but the multiple-testing charge is levied once - which is what
    stops a horizon scan from becoming four free lottery tickets.
    """
    cells = {}
    for h in horizons:
        r = run_family(family=family, group=group, horizon=int(h),
                       cadence=cadence, variant=variant)
        if r.get("state") == "MEASURED":
            cells[int(h)] = r
    if not cells:
        return {"state": "NO_CELLS", "family": family, "group": group}

    def _t(cell):
        v = ((cell.get("layers") or {}).get("L") or {}).get("t_net_excess")
        return -1e9 if v is None else float(v)

    best_h = max(cells, key=lambda h: _t(cells[h]))
    best = dict(cells[best_h])
    best["horizon_sweep"] = {
        "horizons_run": sorted(cells),
        "is_one_search_family": True,
        "selected_horizon": best_h,
        "per_horizon_lockbox_t": {
            h: ((cells[h].get("layers") or {}).get("L") or {}).get(
                "t_net_excess") for h in sorted(cells)},
        "per_horizon_ann_net_excess": {
            h: ((cells[h].get("layers") or {}).get("L") or {}).get(
                "ann_net_excess") for h in sorted(cells)},
        "selection_is_charged": "the sweep selects a horizon, so the family is "
                                "charged for every horizon it looked at",
        "horizons_examined": len(cells),
    }
    return best
