"""alpha_agent.r41.sample_acquisition - FREE, PUBLIC, ACCOUNT-FREE samples.

Every acquisition here satisfies the eight conditions in
``contract.SAMPLE_ACQUISITION_CONDITIONS``: $0, no account, no payment
detail, no click-through, public terms, rate limits respected, research
drive only, provenance (URL, time, bytes, SHA-256) recorded. Nothing is
purchased; existing entitlement keys are used READ-ONLY within their tier.

Sources (measured by probe, not by marketing claim):

* Dukascopy public datafeed - tick bid/ask (20-byte LZMA records, one file
  per hour) for FX spot, metals and index/commodity/bond CFDs; converted
  here to 1-minute bars with OBSERVED spreads. FX spot is LEVEL 3 native;
  the CFDs are LEVEL 2 proxies of the futures they track.
* Binance public archive (data.binance.vision) - 1-minute spot/perp klines,
  funding rates, open interest / long-short metrics; all symbols ever
  listed are enumerable from the bucket listing (survivorship-safe
  universe construction is the consumer's job).
* Tardis.dev free tier - the first day of every month of exchange-native
  trades / quotes / L2 for crypto derivatives (no key).
* Cboe - daily histories of VIX, VIX9D, VIX3M, VIX6M, VIX1D, VVIX, SKEW.
* Central-bank curves - ECB euro-area AAA zero curve (daily, 2004->), Bank
  of Canada benchmark yields, Japan MoF JGB yields (1974->), RBA F2.
* FRED - ICE BofA OAS family, Treasury CMT, breakevens (existing key).
* Vendor samples - FirstRate (1-minute SPY / VIX samples).
"""
from __future__ import annotations

import concurrent.futures as _fut
import datetime as _dt
import hashlib
import io
import json
import lzma
import os
import time
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import requests

from . import data_dir

CALCULATION_OWNER = "alpha_agent.r41.sample_acquisition"
UA = "paper-trader-research/1.0 (research use; contact binisti@gmail.com)"

# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def _manifest_path(kind: str) -> Path:
    return data_dir(kind) / "acquisition_manifest.json"


def record(kind: str, key: str, entry: dict) -> None:
    p = _manifest_path(kind)
    body = {}
    if p.exists():
        try:
            body = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            body = {}
    entry = dict(entry)
    entry["recorded_at"] = _dt.datetime.now(_dt.timezone.utc).isoformat()
    body[key] = entry
    p.write_text(json.dumps(body, indent=1, default=str), encoding="utf-8")


def load_manifest(kind: str) -> dict:
    p = _manifest_path(kind)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def fetch(url: str, *, timeout: int = 60, retries: int = 3,
          session: requests.Session = None) -> Optional[bytes]:
    s = session or requests.Session()
    s.headers["User-Agent"] = UA
    last = None
    for i in range(retries):
        try:
            r = s.get(url, timeout=timeout)
            if r.status_code == 200:
                return r.content
            if r.status_code in (403, 404):
                return None
            last = "HTTP %s" % r.status_code
        except Exception as e:
            last = str(e)[:120]
        time.sleep(1.0 + i)
    raise RuntimeError("fetch failed %s: %s" % (url, last))


def save_bytes(kind: str, name: str, url: str, data: bytes,
               extra: dict = None) -> Path:
    p = data_dir(kind) / name
    p.write_bytes(data)
    record(kind, name, {"url": url, "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                        **(extra or {})})
    return p


# --------------------------------------------------------------------------- #
# Dukascopy ticks -> 1-minute bars
# --------------------------------------------------------------------------- #
DUKA_URL = "https://datafeed.dukascopy.com/datafeed/{sym}/{y}/{m:02d}/{d:02d}/{h:02d}h_ticks.bi5"
DUKA_SCALE = {"EURUSD": 1e5, "GBPUSD": 1e5, "AUDUSD": 1e5, "USDCAD": 1e5,
              "USDCHF": 1e5, "NZDUSD": 1e5, "USDJPY": 1e3, "XAUUSD": 1e3,
              "XAGUSD": 1e3, "USA500IDXUSD": 1e3, "DEUIDXEUR": 1e3,
              "BUNDTREUR": 1e3, "USTBONDTRUSD": 1e3, "LIGHTCMDUSD": 1e3,
              "BRENTCMDUSD": 1e3, "BTCUSD": 1e1}
DUKA_CLASS = {"EURUSD": "FX_SPOT", "GBPUSD": "FX_SPOT", "AUDUSD": "FX_SPOT",
              "USDCAD": "FX_SPOT", "USDCHF": "FX_SPOT", "NZDUSD": "FX_SPOT",
              "USDJPY": "FX_SPOT", "XAUUSD": "METAL_SPOT",
              "XAGUSD": "METAL_SPOT", "USA500IDXUSD": "INDEX_CFD",
              "DEUIDXEUR": "INDEX_CFD", "BUNDTREUR": "BOND_CFD",
              "USTBONDTRUSD": "BOND_CFD", "LIGHTCMDUSD": "ENERGY_CFD",
              "BRENTCMDUSD": "ENERGY_CFD", "BTCUSD": "CRYPTO_SPOT"}
TICK_DTYPE = np.dtype([("ms", ">i4"), ("ask", ">i4"), ("bid", ">i4"),
                       ("av", ">f4"), ("bv", ">f4")])


def decode_ticks(raw: bytes, scale: float):
    if not raw:
        return None
    data = lzma.decompress(raw)
    n = len(data) // 20
    if n == 0:
        return None
    arr = np.frombuffer(data, dtype=TICK_DTYPE, count=n)
    return (arr["ms"].astype(np.int64), arr["ask"] / scale,
            arr["bid"] / scale, arr["av"].astype(float),
            arr["bv"].astype(float))


def ticks_to_minute_bars(day: _dt.date, hour: int, ticks) -> pd.DataFrame:
    ms, ask, bid, av, bv = ticks
    mid = (ask + bid) / 2.0
    minute = (ms // 60000).astype(int)
    base = pd.Timestamp(day) + pd.Timedelta(hours=hour)
    df = pd.DataFrame({"minute": minute, "mid": mid, "ask": ask, "bid": bid,
                       "spread": ask - bid, "av": av, "bv": bv})
    g = df.groupby("minute")
    out = pd.DataFrame({
        "open": g["mid"].first(), "high": g["mid"].max(),
        "low": g["mid"].min(), "close": g["mid"].last(),
        "ticks": g["mid"].size(), "spread_mean": g["spread"].mean(),
        "ask_close": g["ask"].last(), "bid_close": g["bid"].last(),
        "ask_vol": g["av"].sum(), "bid_vol": g["bv"].sum(),
    })
    out.index = [base + pd.Timedelta(minutes=int(m)) for m in out.index]
    out.index.name = "ts_utc"
    return out


def duka_day(sym: str, day: _dt.date, session: requests.Session,
             scale: float) -> tuple:
    """Download and aggregate one UTC day; returns (bars, n_files_ok,
    n_empty, n_failed)."""
    frames, ok, empty, failed = [], 0, 0, 0
    for h in range(24):
        url = DUKA_URL.format(sym=sym, y=day.year, m=day.month - 1,
                              d=day.day, h=h)
        try:
            raw = fetch(url, timeout=30, retries=3, session=session)
        except Exception:
            failed += 1
            continue
        if not raw:
            empty += 1
            continue
        try:
            t = decode_ticks(raw, scale)
        except Exception:
            failed += 1
            continue
        if t is None:
            empty += 1
            continue
        frames.append(ticks_to_minute_bars(day, h, t))
        ok += 1
    bars = pd.concat(frames) if frames else None
    return bars, ok, empty, failed


CANDLE_DTYPE = np.dtype([("t", ">i4"), ("o", ">i4"), ("c", ">i4"),
                         ("l", ">i4"), ("h", ">i4"), ("v", ">f4")])


def duka_candles_day(sym: str, day: _dt.date, session: requests.Session,
                     scale: float):
    """One UTC day of 1-minute candles from the per-day BID and ASK candle
    files (24x fewer requests than tick mode; volume is Dukascopy lot flow).
    Returns a DataFrame or None; raises only on repeated transport failure."""
    base = "https://datafeed.dukascopy.com/datafeed/%s/%04d/%02d/%02d/" % (
        sym, day.year, day.month - 1, day.day)
    out = {}
    for side in ("BID", "ASK"):
        raw = fetch(base + "%s_candles_min_1.bi5" % side, timeout=30,
                    retries=3, session=session)
        if not raw:
            return None
        data = lzma.decompress(raw)
        arr = np.frombuffer(data, dtype=CANDLE_DTYPE,
                            count=len(data) // 24)
        out[side] = arr
    bid, ask = out["BID"], out["ASK"]
    n = min(len(bid), len(ask))
    bid, ask = bid[:n], ask[:n]
    active = (bid["v"] > 0) | (ask["v"] > 0) | (bid["o"] != bid["c"]) \
        | (bid["h"] != bid["l"])
    ts = pd.Timestamp(day) + pd.to_timedelta(bid["t"].astype(np.int64),
                                             unit="s")
    mid_o = (bid["o"] + ask["o"]) / (2.0 * scale)
    mid_c = (bid["c"] + ask["c"]) / (2.0 * scale)
    mid_h = (bid["h"] + ask["h"]) / (2.0 * scale)
    mid_l = (bid["l"] + ask["l"]) / (2.0 * scale)
    df = pd.DataFrame({"open": mid_o, "high": mid_h, "low": mid_l,
                       "close": mid_c,
                       "spread": (ask["c"] - bid["c"]) / scale,
                       "bid_vol": bid["v"].astype(float),
                       "ask_vol": ask["v"].astype(float),
                       "active": active}, index=ts)
    df = df[df["active"]]
    df.index.name = "ts_utc"
    return df.drop(columns=["active"]) if len(df) else None


def acquire_dukascopy_candles(sym: str, start: str, end: str, *,
                              workers: int = 12, progress=None) -> dict:
    """Candle-mode acquisition: 2 requests per day instead of 24."""
    scale = DUKA_SCALE[sym]
    (data_dir("dukascopy") / sym).mkdir(parents=True, exist_ok=True)
    s0, s1 = pd.Timestamp(start).date(), pd.Timestamp(end).date()
    months = sorted({(d.year, d.month) for d in pd.date_range(s0, s1)})
    summary = {"symbol": sym, "mode": "CANDLES", "months": 0,
               "months_cached": 0, "bars": 0, "days_ok": 0, "days_empty": 0}
    for (y, m) in months:
        path = duka_month_path(sym, y, m)
        if path.exists():
            summary["months_cached"] += 1
            continue
        days = [d.date() for d in pd.date_range(
            max(pd.Timestamp(y, m, 1), pd.Timestamp(s0)),
            min(pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0),
                pd.Timestamp(s1))) if d.weekday() < 6]
        sessions = [requests.Session() for _ in range(workers)]
        results = {}
        with _fut.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {}
            for i, d in enumerate(days):
                futs[ex.submit(duka_candles_day, sym, d,
                               sessions[i % workers], scale)] = d
            for f in _fut.as_completed(futs):
                try:
                    results[futs[f]] = f.result()
                except Exception:
                    results[futs[f]] = None
        frames = [results[d] for d in sorted(results)
                  if results[d] is not None]
        summary["days_ok"] += len(frames)
        summary["days_empty"] += len(days) - len(frames)
        if frames:
            month = pd.concat(frames).sort_index()
            month = month[~month.index.duplicated(keep="last")]
            month.to_csv(path, compression="gzip")
            summary["bars"] += int(len(month))
        else:
            pd.DataFrame().to_csv(path, compression="gzip")
        summary["months"] += 1
        record("dukascopy", "%s_%04d-%02d" % (sym, y, m), {
            "mode": "CANDLES_MIN1_BID_ASK", "scale": scale,
            "days": len(days), "bars": int(len(month)) if frames else 0,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "asset_class": DUKA_CLASS.get(sym)})
        if progress:
            progress("%s %04d-%02d bars=%d ok_days=%d" % (
                sym, y, m, int(len(month)) if frames else 0, len(frames)))
    return summary


def duka_month_path(sym: str, year: int, month: int) -> Path:
    return data_dir("dukascopy") / sym / ("%s_%04d-%02d_1min.csv.gz"
                                          % (sym, year, month))


def acquire_dukascopy(sym: str, start: str, end: str, *, workers: int = 8,
                      progress=None) -> dict:
    """Acquire ``sym`` ticks from ``start`` to ``end`` (YYYY-MM-DD, UTC days),
    month by month, as 1-minute bars. Idempotent per month file."""
    scale = DUKA_SCALE[sym]
    (data_dir("dukascopy") / sym).mkdir(parents=True, exist_ok=True)
    s0 = pd.Timestamp(start).date()
    s1 = pd.Timestamp(end).date()
    months = sorted({(d.year, d.month) for d in pd.date_range(s0, s1)})
    summary = {"symbol": sym, "months": 0, "months_cached": 0, "bars": 0,
               "files_ok": 0, "files_empty": 0, "files_failed": 0}
    for (y, m) in months:
        path = duka_month_path(sym, y, m)
        if path.exists():
            summary["months_cached"] += 1
            continue
        days = [d.date() for d in pd.date_range(
            max(pd.Timestamp(y, m, 1), pd.Timestamp(s0)),
            min(pd.Timestamp(y, m, 1) + pd.offsets.MonthEnd(0),
                pd.Timestamp(s1)))]
        days = [d for d in days if d.weekday() < 6]  # Sunday evening opens
        sessions = [requests.Session() for _ in range(workers)]
        results = {}
        with _fut.ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(duka_day, sym, d, sessions[i % workers], scale):
                    d for i, d in enumerate(days)}
            for f in _fut.as_completed(futs):
                results[futs[f]] = f.result()
        frames = []
        for d in sorted(results):
            bars, ok, empty, failed = results[d]
            summary["files_ok"] += ok
            summary["files_empty"] += empty
            summary["files_failed"] += failed
            if bars is not None:
                frames.append(bars)
        if frames:
            month = pd.concat(frames).sort_index()
            month = month[~month.index.duplicated(keep="last")]
            month.to_csv(path, compression="gzip")
            summary["bars"] += int(len(month))
        else:
            pd.DataFrame().to_csv(path, compression="gzip")
        summary["months"] += 1
        record("dukascopy", "%s_%04d-%02d" % (sym, y, m), {
            "url_pattern": DUKA_URL, "scale": scale, "days": len(days),
            "bars": int(len(month)) if frames else 0,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "asset_class": DUKA_CLASS.get(sym)})
        if progress:
            progress("%s %04d-%02d bars=%d ok=%d empty=%d failed=%d" % (
                sym, y, m, int(len(month)) if frames else 0,
                summary["files_ok"], summary["files_empty"],
                summary["files_failed"]))
    return summary


def load_dukascopy(sym: str) -> Optional[pd.DataFrame]:
    d = data_dir("dukascopy") / sym
    if not d.exists():
        return None
    frames = []
    for p in sorted(d.glob("%s_*_1min.csv.gz" % sym)):
        try:
            df = pd.read_csv(p, index_col=0, parse_dates=True)
        except Exception:
            continue
        if len(df):
            frames.append(df)
    if not frames:
        return None
    out = pd.concat(frames).sort_index()
    out = out[~out.index.duplicated(keep="last")]
    if "spread" not in out.columns and "spread_mean" in out.columns:
        out["spread"] = out["spread_mean"]
    elif "spread_mean" in out.columns:
        out["spread"] = out["spread"].fillna(out["spread_mean"])
    return out


# --------------------------------------------------------------------------- #
# Binance public archive
# --------------------------------------------------------------------------- #
BV = "https://data.binance.vision/data"


def binance_monthly_klines(symbol: str, interval: str, year: int, month: int,
                           *, market: str = "spot") -> Optional[pd.DataFrame]:
    base = "spot" if market == "spot" else "futures/um"
    url = "%s/%s/monthly/klines/%s/%s/%s-%s-%04d-%02d.zip" % (
        BV, base, symbol, interval, symbol, interval, year, month)
    raw = fetch(url, timeout=120)
    if not raw:
        return None
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        name = z.namelist()[0]
        df = pd.read_csv(z.open(name), header=None)
    if isinstance(df.iloc[0, 0], str):
        df = df.iloc[1:]
    df = df.iloc[:, :11]
    df.columns = ["open_time", "open", "high", "low", "close", "volume",
                  "close_time", "quote_volume", "trades", "taker_buy_base",
                  "taker_buy_quote"]
    ot = pd.to_numeric(df["open_time"])
    unit = "us" if ot.iloc[0] > 1e14 else "ms"
    df.index = pd.to_datetime(ot, unit=unit, utc=True)
    df = df.drop(columns=["open_time", "close_time"]).astype(float)
    return df


def acquire_binance_klines(symbol: str, interval: str, start: str, end: str,
                           *, market: str = "spot", progress=None) -> dict:
    out_dir = data_dir("binance") / market / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    months = sorted({(d.year, d.month) for d in pd.date_range(start, end,
                                                               freq="MS")})
    n_ok = n_missing = rows = 0
    for (y, m) in months:
        path = out_dir / ("%s_%s_%04d-%02d.csv.gz" % (symbol, interval, y, m))
        if path.exists():
            n_ok += 1
            continue
        df = binance_monthly_klines(symbol, interval, y, m, market=market)
        if df is None:
            n_missing += 1
            continue
        df.to_csv(path, compression="gzip")
        rows += len(df)
        n_ok += 1
        record("binance", "%s_%s_%s_%04d-%02d" % (market, symbol, interval,
                                                   y, m),
               {"rows": int(len(df)),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        if progress:
            progress("%s %s %04d-%02d rows=%d" % (symbol, interval, y, m,
                                                  len(df)))
    return {"symbol": symbol, "interval": interval, "market": market,
            "months_ok": n_ok, "months_missing": n_missing, "rows": rows}


def acquire_binance_funding(symbol: str, start: str, end: str) -> dict:
    out_dir = data_dir("binance") / "funding" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    frames, missing = [], 0
    for d in pd.date_range(start, end, freq="MS"):
        path = out_dir / ("%s_funding_%04d-%02d.csv" % (symbol, d.year,
                                                         d.month))
        if path.exists():
            frames.append(pd.read_csv(path))
            continue
        url = "%s/futures/um/monthly/fundingRate/%s/%s-fundingRate-%04d-%02d.zip" % (
            BV, symbol, symbol, d.year, d.month)
        raw = fetch(url, timeout=60)
        if not raw:
            missing += 1
            continue
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            df = pd.read_csv(z.open(z.namelist()[0]))
        df.to_csv(path, index=False)
        frames.append(df)
    allf = pd.concat(frames) if frames else pd.DataFrame()
    return {"symbol": symbol, "rows": int(len(allf)), "months_missing": missing}


def binance_symbol_listing(market: str = "spot") -> list:
    """Every symbol with a klines folder in the public archive - the
    survivorship-safe universe (delisted symbols keep their history)."""
    base = "data/spot/monthly/klines/" if market == "spot" \
        else "data/futures/um/monthly/klines/"
    url = ("https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"
           "?delimiter=/&prefix=%s" % base)
    syms, marker = [], None
    s = requests.Session()
    s.headers["User-Agent"] = UA
    for _ in range(50):
        u = url + ("&marker=%s" % marker if marker else "")
        r = s.get(u, timeout=60)
        txt = r.text
        import re
        found = re.findall(r"<Prefix>%s([^<]+)/</Prefix>" % re.escape(base),
                           txt)
        syms.extend(found)
        if "<IsTruncated>true</IsTruncated>" in txt and found:
            marker = base + found[-1] + "/"
        else:
            break
    return sorted(set(syms))


def acquire_binance_metrics(symbol: str, start: str, end: str,
                            progress=None) -> dict:
    """Daily futures metrics files (OI, long/short ratios, taker ratio)."""
    out_dir = data_dir("binance") / "metrics" / symbol
    out_dir.mkdir(parents=True, exist_ok=True)
    frames, missing, got = [], 0, 0
    for d in pd.date_range(start, end, freq="D"):
        path = out_dir / ("%s_metrics_%s.csv" % (symbol, d.date()))
        if path.exists():
            frames.append(pd.read_csv(path))
            got += 1
            continue
        url = "%s/futures/um/daily/metrics/%s/%s-metrics-%s.zip" % (
            BV, symbol, symbol, d.date())
        try:
            raw = fetch(url, timeout=60, retries=2)
        except Exception:
            raw = None
        if not raw:
            missing += 1
            continue
        with zipfile.ZipFile(io.BytesIO(raw)) as z:
            df = pd.read_csv(z.open(z.namelist()[0]))
        df.to_csv(path, index=False)
        frames.append(df)
        got += 1
        if progress and got % 50 == 0:
            progress("%s metrics %s got=%d missing=%d" % (symbol, d.date(),
                                                          got, missing))
    allf = pd.concat(frames) if frames else pd.DataFrame()
    p = data_dir("binance") / ("%s_metrics_all.csv.gz" % symbol)
    allf.to_csv(p, index=False, compression="gzip")
    return {"symbol": symbol, "days": got, "days_missing": missing,
            "rows": int(len(allf))}


# --------------------------------------------------------------------------- #
# Cboe volatility indices
# --------------------------------------------------------------------------- #
CBOE_INDICES = ("VIX", "VIX9D", "VIX3M", "VIX6M", "VIX1D", "VVIX", "SKEW")
CBOE_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/{idx}_History.csv"


def acquire_cboe_indices() -> dict:
    out = {}
    for idx in CBOE_INDICES:
        url = CBOE_URL.format(idx=idx)
        raw = fetch(url)
        if not raw:
            out[idx] = {"state": "UNAVAILABLE"}
            continue
        p = save_bytes("cboe", "%s_History.csv" % idx, url, raw)
        df = pd.read_csv(p)
        out[idx] = {"state": "OK", "rows": int(len(df)),
                    "first": str(df.iloc[0, 0]), "last": str(df.iloc[-1, 0]),
                    "columns": list(df.columns)}
    return out


def load_cboe_index(idx: str) -> Optional[pd.Series]:
    p = data_dir("cboe") / ("%s_History.csv" % idx)
    if not p.exists():
        return None
    df = pd.read_csv(p)
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    col = "CLOSE" if "CLOSE" in df.columns else df.columns[-1]
    return df[col].astype(float).rename(idx)


# --------------------------------------------------------------------------- #
# Government yield curves (free, no account)
# --------------------------------------------------------------------------- #
ECB_TENORS = ("3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "15Y", "20Y",
              "30Y")


def acquire_ecb_curve(start: str = "2004-09-06") -> dict:
    out = {}
    frames = []
    for t in ECB_TENORS:
        url = ("https://data-api.ecb.europa.eu/service/data/YC/"
               "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_%s?format=csvdata&startPeriod=%s"
               % (t, start))
        raw = fetch(url, timeout=120)
        if not raw:
            out[t] = "UNAVAILABLE"
            continue
        df = pd.read_csv(io.BytesIO(raw))
        s = pd.Series(df["OBS_VALUE"].values,
                      index=pd.to_datetime(df["TIME_PERIOD"]), name=t)
        frames.append(s)
        out[t] = int(len(s))
    if frames:
        curve = pd.concat(frames, axis=1).sort_index()
        p = data_dir("curves_gov") / "ecb_aaa_spot_curve.csv"
        curve.to_csv(p)
        record("curves_gov", "ecb_aaa_spot_curve", {
            "url": "ECB Data Portal YC dataset (AAA euro area, Svensson)",
            "rows": int(len(curve)), "first": str(curve.index.min().date()),
            "last": str(curve.index.max().date()),
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    return out


def acquire_boc_curve() -> dict:
    series = {"2Y": "BD.CDN.2YR.DQ.YLD", "3Y": "BD.CDN.3YR.DQ.YLD",
              "5Y": "BD.CDN.5YR.DQ.YLD", "7Y": "BD.CDN.7YR.DQ.YLD",
              "10Y": "BD.CDN.10YR.DQ.YLD", "30Y": "BD.CDN.LONG.DQ.YLD"}
    url = ("https://www.bankofcanada.ca/valet/observations/%s/csv?"
           "start_date=1990-01-01" % ",".join(series.values()))
    raw = fetch(url, timeout=120)
    if not raw:
        return {"state": "UNAVAILABLE"}
    txt = raw.decode("utf-8", "replace")
    i = txt.find('"date"')
    if i < 0:
        i = txt.find("date")
    df = pd.read_csv(io.StringIO(txt[i:]))
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.rename(columns={v: k for k, v in series.items()})
    df = df.apply(pd.to_numeric, errors="coerce")
    p = data_dir("curves_gov") / "boc_benchmark_yields.csv"
    df.to_csv(p)
    record("curves_gov", "boc_benchmark_yields", {
        "url": url, "rows": int(len(df)), "first": str(df.index.min().date()),
        "last": str(df.index.max().date()),
        "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    return {"state": "OK", "rows": int(len(df))}


def acquire_mof_jgb() -> dict:
    url = ("https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/"
           "historical/jgbcme_all.csv")
    raw = fetch(url, timeout=120)
    if not raw:
        return {"state": "UNAVAILABLE"}
    txt = raw.decode("shift_jis", "replace")
    lines = txt.splitlines()
    start = 0
    for i, ln in enumerate(lines[:5]):
        if ln.startswith("Date") or ln.startswith("﻿Date"):
            start = i
    df = pd.read_csv(io.StringIO("\n".join(lines[start:])))
    df.columns = [c.strip() for c in df.columns]
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
    df = df.apply(pd.to_numeric, errors="coerce")
    p = data_dir("curves_gov") / "mof_jgb_yields.csv"
    df.to_csv(p)
    record("curves_gov", "mof_jgb_yields", {
        "url": url, "rows": int(len(df)), "first": str(df.index.min().date()),
        "last": str(df.index.max().date()), "columns": list(df.columns),
        "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    return {"state": "OK", "rows": int(len(df)), "columns": list(df.columns)}


def acquire_rba_f2() -> dict:
    url = "https://www.rba.gov.au/statistics/tables/csv/f2-data.csv"
    raw = fetch(url, timeout=120)
    if not raw:
        return {"state": "UNAVAILABLE"}
    p = save_bytes("curves_gov", "rba_f2_raw.csv", url, raw)
    return {"state": "OK", "bytes": len(raw), "path": str(p)}


def acquire_fred(series: dict, key_env: str = "FRED_API_KEY") -> dict:
    key = os.environ.get(key_env) or os.environ.get("PAPER_TRADER_FRED_API_KEY")
    out = {}
    frames = []
    for name, sid in series.items():
        url = ("https://api.stlouisfed.org/fred/series/observations?series_id=%s"
               "&api_key=%s&file_type=json&observation_start=1990-01-01"
               % (sid, key))
        try:
            raw = fetch(url, timeout=60)
        except Exception as e:
            out[name] = {"state": "ERROR", "error": str(e)[:80]}
            continue
        if not raw:
            out[name] = {"state": "UNAVAILABLE"}
            continue
        obs = json.loads(raw).get("observations", [])
        s = pd.Series({pd.Timestamp(o["date"]): (float(o["value"])
                                                 if o["value"] != "." else np.nan)
                       for o in obs}, name=name)
        frames.append(s)
        out[name] = {"state": "OK", "rows": int(s.notna().sum()),
                     "first": str(s.first_valid_index())[:10],
                     "last": str(s.last_valid_index())[:10], "series_id": sid}
    if frames:
        df = pd.concat(frames, axis=1).sort_index()
        p = data_dir("fred") / "fred_daily_panel.csv"
        df.to_csv(p)
        record("fred", "fred_daily_panel", {
            "series": series, "rows": int(len(df)),
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest(),
            "note": "current-vintage daily market series (not revised "
                    "macro); ALFRED vintages are the R39 owner's concern"})
    return out


FRED_MARKET_SERIES = {
    # credit (ICE BofA OAS, daily)
    "OAS_IG": "BAMLC0A0CM", "OAS_HY": "BAMLH0A0HYM2", "OAS_AAA": "BAMLC0A1CAAA",
    "OAS_AA": "BAMLC0A2CAA", "OAS_A": "BAMLC0A3CA", "OAS_BBB": "BAMLC0A4CBBB",
    "OAS_BB": "BAMLH0A1HYBB", "OAS_B": "BAMLH0A2HYB", "OAS_CCC": "BAMLH0A3HYC",
    "OAS_IG_1_3": "BAMLC1A0C13Y", "OAS_IG_3_5": "BAMLC2A0C35Y",
    "OAS_IG_5_7": "BAMLC3A0C57Y", "OAS_IG_7_10": "BAMLC4A0C710Y",
    "OAS_IG_10_15": "BAMLC7A0C1015Y", "OAS_IG_15P": "BAMLC8A0C15PY",
    "OAS_EM": "BAMLEMCBPIOAS", "OAS_EUR_HY": "BAMLHE00EHYIOAS",
    "OAS_EUR_IG": "BAMLEMRECRPIEMEAOAS",
    "TRI_IG": "BAMLCC0A0CMTRIV", "TRI_HY": "BAMLHYH0A0HYM2TRIV",
    # US Treasury CMT (daily)
    "CMT_3M": "DGS3MO", "CMT_2Y": "DGS2", "CMT_5Y": "DGS5", "CMT_10Y": "DGS10",
    "CMT_30Y": "DGS30", "CMT_1Y": "DGS1", "CMT_7Y": "DGS7", "CMT_20Y": "DGS20",
    # breakevens / real
    "BE_5Y": "T5YIE", "BE_10Y": "T10YIE", "REAL_10Y": "DFII10",
    "REAL_5Y": "DFII5",
    # vol / fx references
    "VIXCLS": "VIXCLS", "OVX": "OVXCLS", "GVZ": "GVZCLS",
    "DTWEXBGS": "DTWEXBGS",
    # policy
    "EFFR": "EFFR", "SOFR": "SOFR",
}


# --------------------------------------------------------------------------- #
# Vendor samples and free-tier probes
# --------------------------------------------------------------------------- #
FIRSTRATE_SAMPLES = {
    "SPY": "https://frd001.s3-us-east-2.amazonaws.com/SPY_1min_sample_firstratedata.zip",
    "VIX": "https://frd001.s3-us-east-2.amazonaws.com/VIX_1min_sample_firstratedata.zip",
    "SPX": "https://frd001.s3-us-east-2.amazonaws.com/SPX_1min_sample_firstratedata.zip",
}


def acquire_firstrate_samples() -> dict:
    out = {}
    for sym, url in FIRSTRATE_SAMPLES.items():
        try:
            raw = fetch(url, timeout=120)
        except Exception as e:
            out[sym] = {"state": "ERROR", "error": str(e)[:80]}
            continue
        if not raw:
            out[sym] = {"state": "UNAVAILABLE"}
            continue
        p = save_bytes("vendor_samples", "firstrate_%s_1min_sample.zip" % sym,
                       url, raw)
        try:
            with zipfile.ZipFile(p) as z:
                name = [n for n in z.namelist() if n.endswith(".txt")
                        or n.endswith(".csv")][0]
                df = pd.read_csv(z.open(name), header=None)
            out[sym] = {"state": "OK", "rows": int(len(df)),
                        "first": str(df.iloc[0, 0]), "last": str(df.iloc[-1, 0]),
                        "bytes": len(raw)}
        except Exception as e:
            out[sym] = {"state": "DOWNLOADED_UNPARSED", "error": str(e)[:80]}
    return out


def acquire_tardis_free_day(exchange: str, channel: str, symbol: str,
                            year: int, month: int) -> dict:
    """Tardis.dev free tier: the FIRST day of every month, no key."""
    url = "https://datasets.tardis.dev/v1/%s/%s/%04d/%02d/01/%s.csv.gz" % (
        exchange, channel, year, month, symbol)
    try:
        raw = fetch(url, timeout=300, retries=2)
    except Exception as e:
        return {"state": "ERROR", "error": str(e)[:80], "url": url}
    if not raw:
        return {"state": "UNAVAILABLE", "url": url}
    p = save_bytes("tardis", "%s_%s_%s_%04d-%02d-01.csv.gz" % (
        exchange, channel, symbol, year, month), url, raw)
    return {"state": "OK", "bytes": len(raw), "path": str(p)}


def probe_entitled_api(name: str, url: str, *, timeout: int = 60) -> dict:
    """ONE read-only call against an existing entitlement; records status,
    byte count and a 300-character preview. Never changes a tier."""
    s = requests.Session()
    s.headers["User-Agent"] = UA
    try:
        r = s.get(url, timeout=timeout)
        body = r.content
        preview = body[:300].decode("utf-8", "replace")
        return {"provider": name, "status": r.status_code, "bytes": len(body),
                "preview": preview, "content_type": r.headers.get(
                    "Content-Type", "")[:40]}
    except Exception as e:
        return {"provider": name, "status": None, "error": str(e)[:120]}
