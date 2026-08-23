"""alpha_agent.r41.crypto_lab - Track 8 crypto market structure + Track 9
microstructure on the Binance public archive.

Universe: BTCUSDT and ETHUSDT only - both were the two largest, most liquid
pairs at the START of the sample (2017-08), so their selection is
PIT-defensible; the survivorship caveat (today's list did not pick them) is
recorded on every artifact. All data: the public exchange archive (spot and
USD-M perp 1-minute klines with SIGNED taker volume, the full funding-rate
history, daily open-interest / long-short metrics from 2021).

Declared candidates:
* FUNDING_CARRY x {1,3,7}d - delta-neutral basis book (short perp / long
  spot when the trailing funding z-score is positive, reversed when
  negative, flat in between). Daily return = funding accrual + (spot - perp)
  return on the held sign. Costs: two legs x 5 bps per side per change.
* TS_MOM / TS_REV x {1,7,21}d - directional daily momentum/reversal on the
  two coins (equal risk), 5 bps per side.
* OFI_<h> - intraday order-flow imbalance (2 x taker_buy/volume - 1) at the
  5-minute grid, aggregated 5/30/60 minutes, ridge and LGBM fit on ZONE A,
  traded with a conviction threshold at the NEXT bar for {5m, 15m, 60m}
  holds; judged at taker costs (5 bps/side) and at 2x / 3x stress. This is
  the estate's first SIGNED-FLOW information family.

Zones per stream family (contract.ZONE_SPLIT on each stream's own dates).
Crypto trades 7 days a week: periods_per_year = 365 daily / 105120 5-minute.
"""
from __future__ import annotations

import numpy as np
import pandas as pd

from . import contract as C
from . import data_dir
from . import evidence as EV
from . import burden as BURDEN

CALCULATION_OWNER = "alpha_agent.r41.crypto_lab"
FAMILY_DAILY = "CRYPTO"
FAMILY_MICRO = "MICROSTRUCTURE"
TAKER_BPS = C.COST_BPS_PER_SIDE["CRYPTO_SPOT_INTRADAY"]
SURVIVORSHIP_NOTE = ("BTC/ETH were the two largest pairs at sample start "
                     "(2017-08); selection is PIT-defensible but recorded "
                     "as a caveat - the broad altcoin cross-section is NOT "
                     "researched here because listing/delisting-safe "
                     "universes need the full archive listing")


def _load_months(market: str, symbol: str, interval: str) -> pd.DataFrame:
    d = data_dir("binance") / market / symbol
    frames = []
    for p in sorted(d.glob("%s_%s_*.csv.gz" % (symbol, interval))):
        try:
            df = pd.read_csv(p, index_col=0, parse_dates=True)
        except Exception:
            continue
        if len(df):
            frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames)
    out.index = pd.to_datetime(out.index, errors="coerce", utc=True,
                               format="mixed")
    out = out[out.index.notna()]
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.sort_index()
    return out[~out.index.duplicated(keep="last")]


def load_daily(symbol: str) -> pd.DataFrame:
    return _load_months("spot", symbol, "1d")


def load_minute(symbol: str, market: str = "spot") -> pd.DataFrame:
    return _load_months(market, symbol, "1m")


def load_funding(symbol: str) -> pd.Series:
    d = data_dir("binance") / "funding" / symbol
    frames = []
    for p in sorted(d.glob("*_funding_*.csv")):
        try:
            frames.append(pd.read_csv(p))
        except Exception:
            continue
    if not frames:
        return pd.Series(dtype=float)
    df = pd.concat(frames)
    tcol = "calc_time" if "calc_time" in df.columns else "fundingTime"
    rcol = "last_funding_rate" if "last_funding_rate" in df.columns \
        else "fundingRate"
    ts = pd.to_numeric(df[tcol], errors="coerce")
    unit = "us" if ts.iloc[0] > 1e14 else "ms"
    idx = pd.to_datetime(ts, unit=unit, utc=True)
    s = pd.Series(pd.to_numeric(df[rcol], errors="coerce").values, index=idx)
    return s.sort_index()


# --------------------------------------------------------------------------- #
# Daily books
# --------------------------------------------------------------------------- #
def funding_carry_stream(symbol: str) -> dict:
    """Daily PnL of the funding-conditioned basis book (unit notional)."""
    spot = load_daily(symbol)["close"]
    perp = load_minute(symbol, "um")
    if not len(perp):
        return None
    perp_d = perp["close"].resample("1D").last()
    spot.index = pd.to_datetime(spot.index, utc=True)
    daily = pd.DataFrame({"spot": spot.resample("1D").last(),
                          "perp": perp_d})
    fr = load_funding(symbol)
    daily["funding"] = fr.resample("1D").sum()
    daily = daily.dropna(subset=["spot", "perp"])
    fz30 = (daily["funding"].rolling(30, min_periods=15).mean()
            / daily["funding"].rolling(90, min_periods=30).std()).shift(1)
    # position +1 = short perp / long spot (collect positive funding)
    sig = pd.Series(0.0, index=daily.index)
    sig[fz30 > 0.5] = 1.0
    sig[fz30 < -0.5] = -1.0
    spot_ret = daily["spot"].pct_change()
    perp_ret = daily["perp"].pct_change()
    return {"gross": sig.shift(1) * (daily["funding"]
                                     + (spot_ret - perp_ret)),
            "signal": sig, "funding": daily["funding"],
            "dates": daily.index}


def daily_ts_stream(kind: str, lookback: int) -> dict:
    rets = {}
    for sym in ("BTCUSDT", "ETHUSDT"):
        px = load_daily(sym)["close"]
        px.index = pd.to_datetime(px.index, utc=True)
        px = px.resample("1D").last()
        rets[sym] = px.pct_change()
    R = pd.DataFrame(rets)
    vol = R.rolling(30, min_periods=15).std().shift(1)
    mom = (R.rolling(lookback).sum() / (R.rolling(lookback).std()
                                        * np.sqrt(lookback))).shift(1)
    sig = np.sign(mom) if kind == "TS_MOM" else -np.sign(mom)
    pos = (sig / vol).div((sig / vol).abs().sum(axis=1).replace(0, np.nan),
                          axis=0)
    gross = (pos.shift(1) * R).sum(axis=1, min_count=1)
    cost = (pos - pos.shift(1)).abs().sum(axis=1) * TAKER_BPS / 1e4
    return {"gross": gross, "cost": cost, "dates": R.index}


def run_daily(*, progress=None) -> dict:
    results, screens = [], []
    ppy = 365.0

    def judge(name, expr, gross, cost, dates, h, spec_extra=None,
              parents=None):
        idx = pd.DatetimeIndex(dates)
        zones = EV.zone_split(idx, embargo=h)
        g_a = gross.reindex(zones["A"]).to_numpy()
        c_a = cost.reindex(zones["A"]).to_numpy() if cost is not None \
            else np.zeros(len(zones["A"]))
        card_a = EV.scorecard(g_a, c_a, np.zeros(len(zones["A"])),
                              periods_per_year=ppy, overlap=h)
        t_a = card_a.get("excess_t_hac")
        spec = {"information_family": "CRYPTO_MARKET_STRUCTURE",
                "asset_family": "CRYPTO_SPOT_PERP",
                "horizon": "%dd" % h, "economic_expression": expr,
                "representation": name,
                "model": "TRANSPARENT_RULE",
                "hyperparameter_budget": 1,
                "parent_hypotheses": parents or ["R41 crypto lane opening"],
                "validation_touches": 1, **(spec_extra or {})}
        row = {"label": name, "horizon_days": h, "zone_a_t": t_a,
               "zone_a": EV.summarise(card_a),
               "zone_ranges": {z: zones["%s_range" % z.lower()]
                               for z in ("A", "B", "C")}}
        screens.append(row)
        if progress:
            progress("A %s h=%dd t=%s" % (name, h,
                                          None if t_a is None
                                          else round(t_a, 2)))
        if t_a is None or abs(t_a) < 1.5:
            return
        sgn = 1.0 if t_a > 0 else -1.0
        g_b = sgn * gross.reindex(zones["B"]).to_numpy()
        c_b = cost.reindex(zones["B"]).to_numpy() if cost is not None \
            else np.zeros(len(zones["B"]))
        card_b = EV.scorecard(g_b, c_b, np.zeros(len(zones["B"])),
                              periods_per_year=ppy, overlap=h)
        cid = BURDEN.record_zone_b(spec, family=FAMILY_DAILY)
        gate = EV.research_candidate_gate(card_b)
        results.append({"candidate_id": cid, "label": name,
                        "sign_fit_on_a": sgn,
                        "horizon_days": h, "zone_a_t": t_a,
                        "zone_b": EV.summarise(card_b), "gate": gate})
        if progress:
            progress("B %s h=%dd t=%s gate=%s" % (
                name, h, round(card_b.get("excess_t_hac") or 0, 2),
                gate["passes"]))

    for sym in ("BTCUSDT", "ETHUSDT"):
        fc = funding_carry_stream(sym)
        if fc is None:
            continue
        pos_change = fc["signal"].diff().abs()
        cost = pos_change * 2 * TAKER_BPS / 1e4
        for h in (1, 3, 7):
            g = fc["gross"].rolling(h, min_periods=1).mean()
            judge("FUNDING_CARRY_%s" % sym[:3], "DELTA_NEUTRAL_BASIS",
                  g, cost.rolling(h, min_periods=1).mean(), fc["dates"], h,
                  parents=["perp funding is a paid-to-hold flow"])
    for kind in ("TS_MOM", "TS_REV"):
        for lb in (7, 30):
            st = daily_ts_stream(kind, lb)
            for h in (1, 7):
                g = st["gross"].rolling(h, min_periods=1).mean()
                c = st["cost"].rolling(h, min_periods=1).mean()
                judge("%s_%d" % (kind, lb), "TS_DIRECTIONAL", g, c,
                      st["dates"], h)
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in results})
    return {"survivorship_note": SURVIVORSHIP_NOTE, "screened": screens,
            "advanced": results, "family_bh": bh}


# --------------------------------------------------------------------------- #
# Intraday microstructure (5-minute grid)
# --------------------------------------------------------------------------- #
def build_micro_panel(symbol: str, market: str = "spot") -> pd.DataFrame:
    m = load_minute(symbol, market)
    if not len(m):
        return pd.DataFrame()
    g = m.resample("5min").agg(
        {"open": "first", "high": "max", "low": "min", "close": "last",
         "volume": "sum", "taker_buy_base": "sum", "trades": "sum"})
    g = g.dropna(subset=["close"])
    ret = g["close"].pct_change()
    vol_share = g["taker_buy_base"] / g["volume"].replace(0, np.nan)
    ofi = (2.0 * vol_share - 1.0).fillna(0.0)
    rv12 = ret.rolling(12).std()
    panel = pd.DataFrame({
        "ret": ret,
        "ofi5": ofi,
        "ofi30": ofi.rolling(6).mean(),
        "ofi60": ofi.rolling(12).mean(),
        "ret30": g["close"].pct_change(6),
        "ret60": g["close"].pct_change(12),
        "rv60": rv12,
        "rvz": (rv12 / rv12.rolling(288).mean()).clip(0, 5),
        "volz": (g["volume"] / g["volume"].rolling(288).mean()).clip(0, 10),
        "hour": g.index.hour,
    })
    return panel


def run_micro(*, progress=None) -> dict:
    """OFI models per symbol; hold k bars at the next bar's open; taker
    costs. Every (model, hold) is one declared candidate."""
    out = {"survivorship_note": SURVIVORSHIP_NOTE, "results": [],
           "screens": []}
    for symbol in ("BTCUSDT", "ETHUSDT"):
        panel = build_micro_panel(symbol)
        if not len(panel):
            continue
        feats = ["ofi5", "ofi30", "ofi60", "ret30", "ret60", "rvz", "volz"]
        dates = panel.index
        zones = EV.zone_split(dates, embargo=288)
        ppy = 365.0 * 288.0
        for hold in (1, 3, 12):     # 5m / 15m / 60m
            fwd = panel["close"] if "close" in panel else None
            y = panel["ret"].rolling(hold).sum().shift(-hold)
            X = panel[feats].fillna(0.0)
            mask_a = dates.isin(zones["A"])
            ok = mask_a & y.notna()
            Xtr = X[ok].to_numpy()
            ytr = np.clip(y[ok].to_numpy(), -0.05, 0.05)
            if len(Xtr) < 5000:
                continue
            mu, sd = Xtr.mean(0), Xtr.std(0) + 1e-12
            Z = (Xtr - mu) / sd
            lam = 100.0 * len(Z)
            w = np.linalg.solve(Z.T @ Z + lam * np.eye(Z.shape[1]),
                                Z.T @ ytr)
            pred = pd.Series(((X.to_numpy() - mu) / sd) @ w, index=dates)
            sca = pred[dates.isin(zones["A"])].std()
            for thr_label, thr in (("T1", 1.0), ("T2", 2.0)):
                sig = pd.Series(0.0, index=dates)
                sig[pred > thr * sca] = 1.0
                sig[pred < -thr * sca] = -1.0
                pos = sig.rolling(hold, min_periods=1).mean()
                gross = pos.shift(1) * panel["ret"]
                cost = (pos - pos.shift(1)).abs() * TAKER_BPS / 1e4
                spec = {"information_family": "SIGNED_ORDER_FLOW",
                        "asset_family": "CRYPTO_SPOT",
                        "horizon": "%dx5m" % hold,
                        "economic_expression": "INTRADAY_TS_THRESHOLD",
                        "representation": "OFI_RIDGE_%s_%s" % (symbol[:3],
                                                               thr_label),
                        "model": "RIDGE",
                        "hyperparameter_budget": 2,
                        "parent_hypotheses": ["first signed-flow family"],
                        "validation_touches": 1}
                g_a = gross[dates.isin(zones["A"])].to_numpy()
                c_a = cost[dates.isin(zones["A"])].to_numpy()
                card_a = EV.scorecard(g_a, c_a, np.zeros(len(g_a)),
                                      periods_per_year=ppy, overlap=hold)
                row = {"symbol": symbol, "hold_bars": hold,
                       "threshold": thr,
                       "zone_a_t_IS": card_a.get("excess_t_hac"),
                       "zone_a_gross_ann": card_a.get("gross_ann"),
                       "zone_a_cost_ann": card_a.get("cost_ann"),
                       "trade_share": float((sig != 0).mean())}
                out["screens"].append(row)
                if progress:
                    progress("A(IS) %s hold=%d thr=%s t=%s share=%.3f" % (
                        symbol, hold, thr_label,
                        None if card_a.get("excess_t_hac") is None
                        else round(card_a["excess_t_hac"], 1),
                        row["trade_share"]))
                # Zone B (out of fit)
                g_b = gross[dates.isin(zones["B"])].to_numpy()
                c_b = cost[dates.isin(zones["B"])].to_numpy()
                card_b = EV.scorecard(g_b, c_b, np.zeros(len(g_b)),
                                      periods_per_year=ppy, overlap=hold)
                cid = BURDEN.record_zone_b(spec, family=FAMILY_MICRO)
                gate = EV.research_candidate_gate(card_b)
                out["results"].append({
                    "candidate_id": cid, "symbol": symbol,
                    "hold_bars": hold, "threshold": thr,
                    "zone_a_t_IS": card_a.get("excess_t_hac"),
                    "zone_b": EV.summarise(card_b), "gate": gate,
                    "zone_ranges": {z: zones["%s_range" % z.lower()]
                                    for z in ("A", "B", "C")}})
                if progress:
                    progress("B %s hold=%d thr=%s t=%s gross=%s cost=%s "
                             "gate=%s" % (
                                 symbol, hold, thr_label,
                                 None if card_b.get("excess_t_hac") is None
                                 else round(card_b["excess_t_hac"], 2),
                                 round(card_b.get("gross_ann") or 0, 3),
                                 round(card_b.get("cost_ann") or 0, 3),
                                 gate["passes"]))
    bh = EV.family_bh({x["candidate_id"]: x["zone_b"].get("excess_t_hac")
                       for x in out["results"]})
    out["family_bh"] = bh
    return out
