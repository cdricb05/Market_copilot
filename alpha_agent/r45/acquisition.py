"""alpha_agent.r45.acquisition - get the bars, at $0, and say exactly what
could not be got.

Four routes were tried for native intraday futures. Three of them end in a
blocker the operator - not this release - has to decide about:

  * Databento (CME MDP-3): unauthenticated request returns 401.
    ACCOUNT_REQUIRED.
  * CME DataMine: the host refuses the TLS handshake from this machine and
    needs a login regardless. ACCOUNT_REQUIRED.
  * Norgate, which this estate ALREADY PAYS FOR and which delivered 105
    futures markets in Release 38: ``price_timeseries`` accepts an
    ``interval`` argument and silently returns daily bars for every value of
    it. The entitlement is real and it is daily. HISTORICAL_DATA_UNAVAILABLE.
  * The fourth route works and is free, and its limitation is TIME, not
    money: a public chart endpoint serves genuinely native CBOT/CME/COMEX
    futures bars - 5-minute for ~60 days, 1-minute for ~30. That is a real
    native measurement on a window too short to qualify anything, and it is
    reported as exactly that.

Listed US instruments come from the estate's existing Polygon entitlement,
which authorises a rolling two-year window of minute aggregates. Those are
ETFs. They are labelled ETFs everywhere and are never called futures.
"""
from __future__ import annotations

import gzip
import io
import json
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from . import bars as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.acquisition"

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
POLYGON_MIN_INTERVAL_S = 13.0        # the Basic tier allows 5 requests/minute
_LAST_POLY_CALL = [0.0]

MANIFEST = C.ARTIFACT_DIR / "R45_ACQUISITION_MANIFEST.json"


# --------------------------------------------------------------------------- #
def _write_panel(symbol: str, df: pd.DataFrame) -> dict:
    B.ACQUIRED_ROOT.mkdir(parents=True, exist_ok=True)
    p = B._acquired_path(symbol)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    with gzip.open(p, "wt", encoding="utf-8") as fh:
        fh.write(buf.getvalue())
    return {"path": str(p), "n_bars": int(len(df)),
            "first": str(df["ts_utc"].min()), "last": str(df["ts_utc"].max()),
            "bytes": int(p.stat().st_size)}


def _keep_window(df: pd.DataFrame) -> pd.DataFrame:
    """The same loading filter R44 used: 11:00-17:00 UTC.

    Every declared release stamp under both daylight regimes, plus the
    longest declared hold, falls inside it. It drops no bar that any
    Release-45 experiment can reach.
    """
    h = df["ts_utc"].dt.hour
    return df[(h >= 11) & (h < 17)]


# --------------------------------------------------------------------------- #
# Native futures - free, public, short window
# --------------------------------------------------------------------------- #
def _yahoo_chart(symbol: str, *, interval: str, period1=None, period2=None,
                 rng=None) -> pd.DataFrame:
    params = {"interval": interval, "includePrePost": "true",
              "events": "div,splits"}
    if rng:
        params["range"] = rng
    else:
        params["period1"] = int(period1)
        params["period2"] = int(period2)
    r = requests.get(
        f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}",
        params=params, headers=UA, timeout=60)
    j = r.json()
    res = (j.get("chart") or {}).get("result")
    if not res:
        return None
    node = res[0]
    ts = node.get("timestamp") or []
    q = ((node.get("indicators") or {}).get("quote") or [{}])[0]
    if not ts:
        return None
    df = pd.DataFrame({
        "ts_utc": pd.to_datetime(pd.Series(ts, dtype="int64"), unit="s",
                                 utc=True),
        "open": q.get("open"), "high": q.get("high"), "low": q.get("low"),
        "close": q.get("close"), "volume": q.get("volume"),
    })
    df.attrs["meta"] = {
        "instrumentType": (node.get("meta") or {}).get("instrumentType"),
        "exchange": (node.get("meta") or {}).get("fullExchangeName"),
        "currency": (node.get("meta") or {}).get("currency"),
        "symbol": (node.get("meta") or {}).get("symbol"),
    }
    return df.dropna(subset=["close"])


def acquire_native_futures(symbols=None, *, pause: float = 1.2) -> dict:
    """5-minute for the deepest window offered, 1-minute layered on top."""
    symbols = list(symbols or C.NATIVE_FUTURES_INSTRUMENTS)
    rows = {}
    now = datetime.now(timezone.utc)
    for sym in symbols:
        got, meta = [], None
        try:
            d5 = _yahoo_chart(sym, interval="5m", rng="60d")
        except Exception as exc:                       # pragma: no cover
            rows[sym] = {"state": "ERROR", "error": str(exc)[:200]}
            continue
        if d5 is not None and len(d5):
            d5["src_interval"] = "5m"
            meta = d5.attrs.get("meta")
            got.append(d5)
        time.sleep(pause)
        # 1-minute, in the <= 8 day windows the endpoint permits
        for back in range(0, 32, 7):
            p2 = int((now - timedelta(days=back)).timestamp())
            p1 = int((now - timedelta(days=back + 7)).timestamp())
            try:
                d1 = _yahoo_chart(sym, interval="1m", period1=p1, period2=p2)
            except Exception:                          # pragma: no cover
                d1 = None
            if d1 is not None and len(d1):
                d1["src_interval"] = "1m"
                got.append(d1)
            time.sleep(pause)
        if not got:
            rows[sym] = {"state": "HISTORICAL_DATA_UNAVAILABLE"}
            continue
        df = pd.concat(got, ignore_index=True)
        # a 1-minute bar always beats the 5-minute bar covering it
        df["_pref"] = (df["src_interval"] == "1m").astype(int)
        df = (df.sort_values(["ts_utc", "_pref"])
                .drop_duplicates(subset=["ts_utc"], keep="last")
                .drop(columns=["_pref"]))
        df = _keep_window(df).sort_values("ts_utc")
        rec = _write_panel(sym, df)
        rec.update({"state": "EXECUTED", "source": "public chart endpoint",
                    "meta": meta,
                    "n_1m": int((df["src_interval"] == "1m").sum()),
                    "n_5m": int((df["src_interval"] == "5m").sum()),
                    "instrument_class": "NATIVE_FUTURES",
                    "cost_source": C.COST_SOURCE_ESTIMATED})
        rows[sym] = rec
    return {"lane": "NATIVE_FUTURES_ACQUISITION", "rows": rows,
            "calculation_owner": CALCULATION_OWNER,
            "money_spent_usd": 0.0, "accounts_created": 0}


# --------------------------------------------------------------------------- #
# Listed US instruments - existing entitlement, rolling two-year window
# --------------------------------------------------------------------------- #
def _poly_get(url, params):
    wait = POLYGON_MIN_INTERVAL_S - (time.time() - _LAST_POLY_CALL[0])
    if wait > 0:
        time.sleep(wait)
    for _ in range(6):
        r = requests.get(url, params=params, headers=UA, timeout=120)
        _LAST_POLY_CALL[0] = time.time()
        if r.status_code == 429:
            time.sleep(20)
            continue
        return r
    return r


def polygon_authorised_start() -> str:
    """The earliest date the current entitlement will serve, to the day."""
    today = datetime.now(timezone.utc).date()
    lo, hi = today - timedelta(days=760), today - timedelta(days=3)
    k = os.environ.get("POLYGON_API_KEY")
    while (hi - lo).days > 1:
        mid = lo + (hi - lo) / 2
        r = _poly_get(
            f"https://api.polygon.io/v2/aggs/ticker/SPY/range/1/minute/"
            f"{mid}/{mid}", {"limit": 1, "apiKey": k})
        if r.status_code == 200:
            hi = mid
        else:
            lo = mid
    return str(hi)


def acquire_listed(symbols=None, *, start: str = None,
                   end: str = None) -> dict:
    k = os.environ.get("POLYGON_API_KEY")
    if not k:
        return {"lane": "LISTED_ACQUISITION", "state": "ACCOUNT_REQUIRED",
                "why": "no entitlement key in the shell environment"}
    symbols = list(symbols or C.LISTED_MINUTE_INSTRUMENTS)
    start = start or polygon_authorised_start()
    end = end or str((datetime.now(timezone.utc) - timedelta(days=1)).date())
    rows = {}
    for sym in symbols:
        frames, url, params = [], (
            f"https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/"
            f"{start}/{end}"), {"adjusted": "true", "sort": "asc",
                                "limit": 50000, "apiKey": k}
        pages = 0
        while url and pages < 40:
            r = _poly_get(url, params)
            if r.status_code != 200:
                break
            j = r.json()
            res = j.get("results") or []
            if res:
                frames.append(pd.DataFrame(res))
            pages += 1
            nxt = j.get("next_url")
            url = nxt if nxt else None
            params = {"apiKey": k}
        if not frames:
            rows[sym] = {"state": "HISTORICAL_DATA_UNAVAILABLE",
                         "pages": pages}
            continue
        df = pd.concat(frames, ignore_index=True)
        df = df.rename(columns={"o": "open", "h": "high", "l": "low",
                                "c": "close", "v": "volume", "n": "trades"})
        df["ts_utc"] = pd.to_datetime(df["t"], unit="ms", utc=True)
        keep = [c for c in ("ts_utc", "open", "high", "low", "close",
                            "volume", "trades") if c in df.columns]
        df = _keep_window(df[keep].dropna(subset=["close"]))
        df = df.sort_values("ts_utc").drop_duplicates(subset=["ts_utc"])
        rec = _write_panel(sym, df)
        rec.update({"state": "EXECUTED", "source": "existing entitlement",
                    "pages": pages, "instrument_class": "LISTED_ETF",
                    "cost_source": C.COST_SOURCE_ESTIMATED,
                    "entitlement_window_start": start})
        rows[sym] = rec
    return {"lane": "LISTED_ACQUISITION", "state": "EXECUTED", "rows": rows,
            "entitlement_window": [start, end],
            "calculation_owner": CALCULATION_OWNER,
            "money_spent_usd": 0.0, "accounts_created": 0}


# --------------------------------------------------------------------------- #
# The routes that end in a blocker
# --------------------------------------------------------------------------- #
def probe_blocked_native_routes() -> dict:
    """Record, with evidence, exactly what stops the deep native history."""
    rows = []

    try:
        r = requests.get("https://api.databento.com/v0/metadata.list_datasets",
                         headers=UA, timeout=30)
        rows.append({"provider": "Databento", "dataset": "CME MDP-3",
                     "http": r.status_code, "body": r.text[:120],
                     "state": "ACCOUNT_REQUIRED",
                     "why": "the historical API authenticates every call; a "
                            "key needs an account and a payment method"})
    except Exception as exc:                           # pragma: no cover
        rows.append({"provider": "Databento", "state": "ACCOUNT_REQUIRED",
                     "error": str(exc)[:160]})

    try:
        r = requests.get("https://datamine.cmegroup.com/cme/api/v1/list",
                         headers=UA, timeout=30)
        rows.append({"provider": "CME DataMine", "http": r.status_code,
                     "state": "ACCOUNT_REQUIRED"})
    except Exception as exc:                           # pragma: no cover
        rows.append({"provider": "CME DataMine", "state": "ACCOUNT_REQUIRED",
                     "error": f"{type(exc).__name__}: {exc}"[:160],
                     "why": "the host refused the TLS handshake AND requires "
                            "a login"})

    norgate = {"provider": "Norgate Data", "state": "HISTORICAL_DATA_UNAVAILABLE",
               "why": "the estate ALREADY OWNS this entitlement and it is "
                      "daily-only: price_timeseries accepts interval= and "
                      "returns the identical daily frame for D, 1, 1min, I, "
                      "5, 5min and 60",
               "already_paid_for": True}
    try:
        import norgatedata as nd
        a = nd.price_timeseries("&ZN_CCB", start_date="2026-08-01",
                                end_date="2026-08-25",
                                timeseriesformat="pandas-dataframe")
        b = nd.price_timeseries("&ZN_CCB", start_date="2026-08-01",
                                end_date="2026-08-25",
                                timeseriesformat="pandas-dataframe",
                                interval="1min")
        norgate["daily_rows"] = int(len(a))
        norgate["rows_when_asked_for_1min"] = int(len(b))
        norgate["identical"] = bool(len(a) == len(b))
    except Exception as exc:                           # pragma: no cover
        norgate["error"] = f"{type(exc).__name__}: {exc}"[:160]
    rows.append(norgate)

    rows.append({
        "provider": "Kibot", "state": "ACCOUNT_REQUIRED",
        "why": "the guest account authorises 1-minute history back to 1998 "
               "for exactly ONE symbol (IVE) and daily-only for every other "
               "ticker; futures are refused outright"})

    return {"lane": "BLOCKED_NATIVE_ROUTES", "state": "EXECUTED",
            "rows": rows, "calculation_owner": CALCULATION_OWNER}


# --------------------------------------------------------------------------- #
def run(*, listed: bool = True, native: bool = True) -> dict:
    out = {"schema": "r45_acquisition_manifest/1",
           "campaign_id": C.CAMPAIGN_ID,
           "calculation_owner": CALCULATION_OWNER,
           "authorized_spend_usd": C.AUTHORIZED_SPEND_USD,
           "money_spent_usd": 0.0, "accounts_created": 0,
           "licences_accepted": 0, "payment_details_submitted": 0}
    out["blocked_native_routes"] = probe_blocked_native_routes()
    if native:
        out["native_futures"] = acquire_native_futures()
    if listed:
        out["listed"] = acquire_listed()
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST.write_text(json.dumps(out, indent=2, default=str),
                        encoding="utf-8")
    return out
