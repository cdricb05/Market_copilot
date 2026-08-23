"""alpha_agent.r41.curve_state - the dated-contract CURVE STATE.

Release 38 built ONE front-roll series per market. Relative-value research
needs the whole curve: on every session, the ordered set of live dated
contracts (front, second, third, ... K-th) with settlement, volume, open
interest and days-to-expiry, and the forward return of holding EACH tenor's
specific contract for h sessions (identity fixed at the decision date, no
roll inside the window, no hindsight).

Roll policy: the frozen observable R38 rule (``alpha_agent.r38.research_layer
.roll_exit_date``: exit at the earlier of first-notice - 2 business days and
last-quoted - 5 business days). The front on date t is the first contract
in delivery order whose roll-exit day is >= t and which has a settlement on
t. An ALTERNATIVE observable rule (front = highest open interest among the
next three delivery months, as of t) is provided for the alpha-killer's
ALTERNATIVE_ROLL_RULE test only.

Storage: one compressed long table of bars per market and one contract
metadata table, under ``<research root>/_data_curves``; consumers bind by
content hash. Reads Norgate through the installed 1.0.74 package; nothing
is written outside the research drive.
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from ..r38 import enumeration as EN
from ..r38 import research_layer as RL
from . import data_dir, sha_file

CALCULATION_OWNER = "alpha_agent.r41.curve_state"
R38_ROOT = Path(r"D:\Stock_Prediction_app_data\native_futures_r38"
                r"\r38_native_futures_information_frontier_v4")
MAX_TENORS = 8

logging.disable(logging.CRITICAL)


def store_dir() -> Path:
    return data_dir("curves")


def _nd():
    import norgatedata as nd
    return nd


def load_registry() -> dict:
    reg = json.loads((R38_ROOT / "dated_contract_registry.json").read_text(
        encoding="utf-8"))
    return reg["contract_symbols"]


def load_market_registry() -> dict:
    reg = json.loads((R38_ROOT / "futures_market_registry.json").read_text(
        encoding="utf-8"))
    return reg["markets"]


def primary_symbols(market: str, registry: dict = None) -> list:
    registry = registry or load_registry()
    lists = registry.get(market, {})
    primary = (sorted(lists, key=lambda s: (len(s), s)) or [None])[0]
    return list(lists.get(primary, [])) if primary else []


def bars_path(market: str) -> Path:
    return store_dir() / ("%s_bars.csv.gz" % market)


def meta_path(market: str) -> Path:
    return store_dir() / ("%s_contracts.csv" % market)


def build_market_store(market: str, *, registry: dict = None,
                       force: bool = False) -> dict:
    """Persist every primary-session dated contract of ``market``."""
    if bars_path(market).exists() and meta_path(market).exists() and not force:
        return {"market": market, "state": "CACHED"}
    nd = _nd()
    syms = primary_symbols(market, registry)
    parsed = sorted((p for p in (EN.parse_contract_symbol(s) for s in syms)
                     if p["parsed"]),
                    key=lambda p: (p["delivery_year"], p["delivery_month"]))
    frames, meta = [], []
    for p in parsed:
        sym = p["symbol"]
        try:
            df = nd.price_timeseries(sym, timeseriesformat="pandas-dataframe")
        except Exception:
            df = None
        try:
            lq = nd.last_quoted_date(sym)
        except Exception:
            lq = None
        try:
            fn = nd.first_notice_date(sym)
        except Exception:
            fn = None
        exit_day = RL.roll_exit_date(fn, lq)
        meta.append({"contract": sym, "delivery_year": p["delivery_year"],
                     "delivery_month": p["delivery_month"],
                     "last_quoted": str(lq)[:10] if lq else None,
                     "first_notice": str(fn)[:10] if fn else None,
                     "roll_exit": str(exit_day.date()) if exit_day is not None
                     else None,
                     "rows": 0 if df is None else int(len(df))})
        if df is None or not len(df):
            continue
        df = df[~df.index.duplicated(keep="last")].sort_index()
        out = pd.DataFrame({
            "date": df.index.strftime("%Y-%m-%d"),
            "contract": sym,
            "open": df["Open"].astype(float).values if "Open" in df else np.nan,
            "high": df["High"].astype(float).values if "High" in df else np.nan,
            "low": df["Low"].astype(float).values if "Low" in df else np.nan,
            "close": df["Close"].astype(float).values,
            "volume": df["Volume"].astype(float).values if "Volume" in df
            else np.nan,
            "oi": df["Open Interest"].astype(float).values
            if "Open Interest" in df else np.nan,
        })
        frames.append(out)
    store_dir().mkdir(parents=True, exist_ok=True)
    pd.DataFrame(meta).to_csv(meta_path(market), index=False)
    if not frames:
        pd.DataFrame(columns=["date", "contract", "open", "high", "low",
                              "close", "volume", "oi"]).to_csv(
            bars_path(market), index=False, compression="gzip")
        return {"market": market, "state": "NO_BARS", "contracts": len(meta)}
    bars = pd.concat(frames, ignore_index=True)
    bars.to_csv(bars_path(market), index=False, compression="gzip")
    return {"market": market, "state": "OK", "contracts": len(meta),
            "bars": int(len(bars)),
            "first": bars["date"].min(), "last": bars["date"].max()}


def build_store(markets: list, *, force: bool = False,
                progress=None) -> dict:
    registry = load_registry()
    manifest = {}
    for m in markets:
        t = time.time()
        r = build_market_store(m, registry=registry, force=force)
        r["seconds"] = round(time.time() - t, 1)
        if r["state"] != "CACHED":
            r["bars_sha256"] = sha_file(bars_path(m))
        manifest[m] = r
        if progress:
            progress("%s %s" % (m, r))
    path = store_dir() / "store_manifest.json"
    body = {"calculation_owner": CALCULATION_OWNER,
            "built_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "roll_policy": "R38 observable (first_notice-2bd, last_quoted-5bd)",
            "markets": manifest}
    existing = {}
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8")).get(
                "markets", {})
        except Exception:
            existing = {}
    for k, v in manifest.items():
        if v.get("state") == "CACHED" and k in existing:
            manifest[k] = existing[k]
    path.write_text(json.dumps(body, indent=2, default=str), encoding="utf-8")
    return body


# --------------------------------------------------------------------------- #
# Curve panels
# --------------------------------------------------------------------------- #
class MarketCurve:
    """Wide matrices (dates x contracts in delivery order) for one market."""

    def __init__(self, market: str):
        self.market = market
        bars = pd.read_csv(bars_path(market))
        meta = pd.read_csv(meta_path(market))
        meta = meta.sort_values(["delivery_year", "delivery_month"])
        meta = meta[meta["rows"] > 0]
        self.meta = meta.reset_index(drop=True)
        order = {c: i for i, c in enumerate(self.meta["contract"])}
        bars = bars[bars["contract"].isin(order)]
        bars["j"] = bars["contract"].map(order)
        bars["date"] = pd.to_datetime(bars["date"])
        self.dates = pd.DatetimeIndex(sorted(bars["date"].unique()))
        self.contracts = list(self.meta["contract"])
        n, m = len(self.dates), len(self.contracts)
        self.close = np.full((n, m), np.nan)
        self.volume = np.full((n, m), np.nan)
        self.oi = np.full((n, m), np.nan)
        self.high = np.full((n, m), np.nan)
        self.low = np.full((n, m), np.nan)
        di = {d: i for i, d in enumerate(self.dates)}
        ii = bars["date"].map(di).to_numpy()
        jj = bars["j"].to_numpy()
        self.close[ii, jj] = bars["close"].to_numpy()
        self.volume[ii, jj] = bars["volume"].to_numpy()
        self.oi[ii, jj] = bars["oi"].to_numpy()
        self.high[ii, jj] = bars["high"].to_numpy()
        self.low[ii, jj] = bars["low"].to_numpy()
        re = pd.to_datetime(self.meta["roll_exit"], errors="coerce")
        self.roll_exit = re.to_numpy()
        lq = pd.to_datetime(self.meta["last_quoted"], errors="coerce")
        self.last_quoted = lq.to_numpy()
        # monotone roll exits (a later delivery with an earlier exit is not
        # a roll target) - same rule as the R38 layer
        mono = np.zeros(m, dtype=bool)
        last = None
        for j in range(m):
            e = self.roll_exit[j]
            if pd.isna(e):
                continue
            if last is None or e > last:
                mono[j] = True
                last = e
        self.roll_eligible = mono

    def tenor_matrix(self, K: int = MAX_TENORS, *, rule: str = "R38") -> dict:
        """For every date, the column index of tenor 1..K (or -1)."""
        n, m = self.close.shape
        idx = np.full((n, K), -1, dtype=int)
        d64 = self.dates.to_numpy()
        fin = np.isfinite(self.close)
        j0 = 0
        for i in range(n):
            t = d64[i]
            if rule == "R38":
                while j0 < m and (not self.roll_eligible[j0]
                                  or (not pd.isna(self.roll_exit[j0])
                                      and self.roll_exit[j0] < t)):
                    j0 += 1
                front = j0
            else:  # OI_MAX among next three eligible delivery months
                cands = [j for j in range(j0, min(m, j0 + 12))
                         if self.roll_eligible[j] and fin[i, j]
                         and (pd.isna(self.roll_exit[j])
                              or self.roll_exit[j] >= t)][:3]
                if not cands:
                    continue
                oi = [self.oi[i, j] if np.isfinite(self.oi[i, j]) else -1
                      for j in cands]
                front = cands[int(np.argmax(oi))]
            k = 0
            j = front
            while j < m and k < K:
                if fin[i, j] and (pd.isna(self.last_quoted[j])
                                  or self.last_quoted[j] >= t):
                    idx[i, k] = j
                    k += 1
                j += 1
        return {"idx": idx, "dates": self.dates}

    def panel(self, K: int = MAX_TENORS, *, rule: str = "R38") -> pd.DataFrame:
        tm = self.tenor_matrix(K, rule=rule)
        idx = tm["idx"]
        n = len(self.dates)
        rows = np.arange(n)
        out = {}
        lq = pd.to_datetime(self.meta["last_quoted"], errors="coerce")
        lq64 = lq.to_numpy()
        d64 = self.dates.to_numpy()
        for k in range(K):
            j = idx[:, k]
            ok = j >= 0
            jj = np.where(ok, j, 0)
            c = np.where(ok, self.close[rows, jj], np.nan)
            out["c%d" % (k + 1)] = c
            out["v%d" % (k + 1)] = np.where(ok, self.volume[rows, jj], np.nan)
            out["oi%d" % (k + 1)] = np.where(ok, self.oi[rows, jj], np.nan)
            exp = lq64[jj]
            dte = (exp - d64).astype("timedelta64[D]").astype(float)
            out["dte%d" % (k + 1)] = np.where(ok & np.isfinite(dte), dte,
                                              np.nan)
            out["id%d" % (k + 1)] = np.where(
                ok, np.array(self.contracts, dtype=object)[jj], None)
        df = pd.DataFrame(out, index=self.dates)
        df.index.name = "date"
        return df

    def forward_return(self, idx: np.ndarray, k: int, h: int) -> np.ndarray:
        """Return of holding the tenor-k contract chosen at t for h sessions
        (same contract, no roll), NaN if no settlement at t+h or expired."""
        n = len(self.dates)
        j = idx[:, k - 1]
        out = np.full(n, np.nan)
        rows = np.arange(n - h)
        jj = j[:n - h]
        ok = jj >= 0
        jj2 = np.where(ok, jj, 0)
        p0 = self.close[rows, jj2]
        p1 = self.close[rows + h, jj2]
        r = p1 / p0 - 1.0
        out[:n - h] = np.where(ok & np.isfinite(p0) & np.isfinite(p1)
                               & (p0 > 0), r, np.nan)
        return out

    def daily_tenor_returns(self, idx: np.ndarray, k: int) -> np.ndarray:
        """Daily return earned by a position that holds tenor k under the
        roll rule: on each date the return of the contract held SINCE the
        previous date (own prior settlement), so a roll never books PnL."""
        n = len(self.dates)
        j = idx[:, k - 1]
        out = np.full(n, np.nan)
        for i in range(1, n):
            jp = j[i - 1]
            if jp < 0:
                continue
            p0, p1 = self.close[i - 1, jp], self.close[i, jp]
            if np.isfinite(p0) and np.isfinite(p1) and p0 > 0:
                out[i] = p1 / p0 - 1.0
        return out


def load_curve(market: str) -> Optional[MarketCurve]:
    if not bars_path(market).exists():
        return None
    return MarketCurve(market)


def panel_path(market: str) -> Path:
    return store_dir() / ("%s_panel.csv" % market)


def daily_path(market: str) -> Path:
    return store_dir() / ("%s_daily.csv" % market)


def build_daily_series(markets: list, progress=None) -> dict:
    """Per-market daily research series: roll-aware tenor-1/2 returns, the
    annualised front-to-second calendar slope, prices, OI and volume."""
    out = {}
    for m in markets:
        if daily_path(m).exists():
            out[m] = {"state": "CACHED"}
            continue
        mc = load_curve(m)
        if mc is None:
            out[m] = {"state": "NO_STORE"}
            continue
        tm = mc.tenor_matrix(4)
        idx = tm["idx"]
        n = len(mc.dates)
        rows = np.arange(n)
        j1 = np.where(idx[:, 0] >= 0, idx[:, 0], 0)
        j2 = np.where(idx[:, 1] >= 0, idx[:, 1], 0)
        c1 = np.where(idx[:, 0] >= 0, mc.close[rows, j1], np.nan)
        c2 = np.where(idx[:, 1] >= 0, mc.close[rows, j2], np.nan)
        lq = pd.to_datetime(mc.meta["last_quoted"], errors="coerce").to_numpy()
        d64 = mc.dates.to_numpy()
        gap = (lq[j2] - lq[j1]).astype("timedelta64[D]").astype(float)
        with np.errstate(all="ignore"):
            slope = np.log(c1 / c2) * (365.25 / np.maximum(gap, 1.0))
        j3 = np.where(idx[:, 2] >= 0, idx[:, 2], 0)
        c3 = np.where(idx[:, 2] >= 0, mc.close[rows, j3], np.nan)
        gap23 = (lq[j3] - lq[j2]).astype("timedelta64[D]").astype(float)
        with np.errstate(all="ignore"):
            slope23 = np.log(c2 / c3) * (365.25 / np.maximum(gap23, 1.0))
        df = pd.DataFrame({
            "ret1": mc.daily_tenor_returns(idx, 1),
            "ret2": mc.daily_tenor_returns(idx, 2),
            "ret3": mc.daily_tenor_returns(idx, 3),
            "c1": c1, "c2": c2, "c3": c3,
            "slope_ann": np.where(np.isfinite(slope), slope, np.nan),
            "slope23_ann": np.where(np.isfinite(slope23), slope23, np.nan),
            "oi3": np.where(idx[:, 2] >= 0, mc.oi[rows, j3], np.nan),
            "dte1": np.where(idx[:, 0] >= 0,
                             (lq[j1] - d64).astype("timedelta64[D]"
                                                   ).astype(float), np.nan),
            "oi1": np.where(idx[:, 0] >= 0, mc.oi[rows, j1], np.nan),
            "oi2": np.where(idx[:, 1] >= 0, mc.oi[rows, j2], np.nan),
            "v1": np.where(idx[:, 0] >= 0, mc.volume[rows, j1], np.nan),
            "v2": np.where(idx[:, 1] >= 0, mc.volume[rows, j2], np.nan),
        }, index=mc.dates)
        df.index.name = "date"
        df.to_csv(daily_path(m))
        out[m] = {"state": "OK", "rows": int(len(df))}
        if progress:
            progress("%s %d" % (m, len(df)))
    return out


def load_daily(market: str) -> Optional[pd.DataFrame]:
    p = daily_path(market)
    if not p.exists():
        return None
    return pd.read_csv(p, index_col=0, parse_dates=True)


def build_panels(markets: list, K: int = MAX_TENORS, progress=None) -> dict:
    out = {}
    for m in markets:
        mc = load_curve(m)
        if mc is None:
            out[m] = {"state": "NO_STORE"}
            continue
        df = mc.panel(K)
        df.to_csv(panel_path(m))
        out[m] = {"state": "OK", "rows": int(len(df)),
                  "first": str(df.index.min().date()),
                  "last": str(df.index.max().date()),
                  "tenor_coverage": {("c%d" % (k + 1)):
                                     float(df["c%d" % (k + 1)].notna().mean())
                                     for k in range(K)},
                  "sha256": sha_file(panel_path(m))}
        if progress:
            progress("%s %s" % (m, out[m]["rows"]))
    return out
