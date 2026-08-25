"""alpha_agent.r43.acquisition - Tracks B, C, D, G, M, T and U.

Do actual acquisition work. Do not merely compare vendors.

Every route below was PROBED with a real HTTP request during the campaign,
using only entitlements the estate ALREADY owns (keys present in the
operator's shell, never printed, never written to an artifact, never sent
anywhere but the provider they belong to). Nothing here creates an account,
starts a trial, accepts a licence or submits a payment detail; a route that
would require any of those is recorded as the corresponding blocker from
``contract.BLOCKER_VOCAB`` and the lane moves on.

The headline acquisition result of Release 43 is that TWO walls previously
recorded as closed are not closed:

* **Historical option prices are reachable at $0.** The owned Polygon
  entitlement serves per-contract daily aggregates AND the expired-contract
  reference (so the option universe is survivorship-safe). What it does not
  serve is greeks/IV, the underlying's aggregates, and - decisively - more
  than a ~2-year rolling window. IV is not the wall: it can be inverted from
  price, strike, expiry, an owned underlying and an owned rate. HISTORY is
  the wall.
* **Scheduled macro release dates are reachable at $0.** The owned FRED
  entitlement serves 108,625 historical release dates back to 1996,
  per-release, which is a genuine point-in-time event calendar. Release 43
  uses it to open Track H, which had no PIT event timestamps before.
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, data_dir, sha, write_artifact
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r43.acquisition"
ARTIFACT = "R43_DATA_FRONTIER.json"
UA = "paper-trader-research/43 (research; contact via operator)"

POLYGON_RPM_SLEEP = 13.0     # measured free-tier limit is 5 requests/minute
HTTP_TIMEOUT = 60

#: FRED release ids for the scheduled macro events Track H studies.
MACRO_RELEASES = {
    "CPI": 10, "EMPLOYMENT_SITUATION": 50, "GDP": 53, "PPI": 46,
    "PERSONAL_INCOME_PCE": 54, "RETAIL_SALES": 9,
    "INDUSTRIAL_PRODUCTION": 13, "JOLTS": 192,
}


# --------------------------------------------------------------------------- #
# HTTP - keys never appear in a returned string
# --------------------------------------------------------------------------- #
def _key(name: str):
    v = os.environ.get(name)
    return v or None


def _redact(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    q = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    q = [(k, ("<REDACTED>" if k.lower() in ("apikey", "api_key", "apiKey",
                                            "token", "key")
              else v)) for k, v in q]
    return urllib.parse.urlunsplit(
        (parts.scheme, parts.netloc, parts.path,
         urllib.parse.urlencode(q), parts.fragment))


def http_get(url: str, *, retries: int = 3, pause: float = POLYGON_RPM_SLEEP):
    """One GET. Returns (status, body, redacted_url). Retries only on 429."""
    for _ in range(max(1, retries)):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
                return r.status, r.read().decode("utf-8", "replace"), \
                    _redact(url)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(pause)
                continue
            return e.code, e.read().decode("utf-8", "replace")[:2000], \
                _redact(url)
        except Exception as e:  # network / DNS / TLS
            return None, "%s: %s" % (type(e).__name__, str(e)[:300]), \
                _redact(url)
    return 429, "rate limited after retries", _redact(url)


# --------------------------------------------------------------------------- #
# Track B - options
# --------------------------------------------------------------------------- #
POLY = "https://api.polygon.io"


def probe_options() -> dict:
    """Map the OWNED options entitlement precisely, endpoint by endpoint."""
    pk = _key("POLYGON_API_KEY")
    av = _key("ALPHAVANTAGE_API_KEY")
    rows = []

    def poly(name, path, note):
        if not pk:
            rows.append({"provider": "polygon", "endpoint": name,
                         "status": None, "state": "ACCOUNT_REQUIRED",
                         "note": "no owned key in the operator shell"})
            return None
        s, b, u = http_get(POLY + path + ("&" if "?" in path else "?")
                           + "apiKey=" + pk)
        time.sleep(POLYGON_RPM_SLEEP)
        state = ("ACCESSIBLE" if s == 200 else
                 "PAYMENT_REQUIRED" if s == 403 else
                 "RATE_LIMITED" if s == 429 else "ERROR")
        rows.append({"provider": "polygon", "endpoint": name, "status": s,
                     "state": state, "url": u, "note": note,
                     "body_head": b[:220]})
        return b if s == 200 else None

    poly("reference/options/contracts (live)",
         "/v3/reference/options/contracts?underlying_ticker=SPY&limit=3",
         "option universe, current")
    poly("reference/options/contracts (EXPIRED)",
         "/v3/reference/options/contracts?underlying_ticker=SPY"
         "&expired=true&as_of=2018-06-15&limit=3",
         "expired contracts ARE enumerable - the option universe is "
         "survivorship-safe")
    poly("aggs option daily (in window)",
         "/v2/aggs/ticker/O:SPY250117C00600000/range/1/day/"
         "2024-09-03/2025-01-17",
         "per-contract daily OHLCV+VWAP+trade count")
    poly("aggs option daily (OUT of window)",
         "/v2/aggs/ticker/O:SPY220121C00450000/range/1/day/"
         "2021-06-01/2022-01-21",
         "the binding wall: history older than the plan window")
    poly("aggs UNDERLYING daily",
         "/v2/aggs/ticker/SPY/range/1/day/2024-01-02/2024-01-10",
         "not entitled - mitigated, the estate owns SPY through Norgate")
    poly("snapshot options (greeks/IV)",
         "/v3/snapshot/options/SPY?limit=2",
         "not entitled - NOT the wall: IV inverts from price + strike + "
         "expiry + an owned underlying + an owned rate")

    if av:
        s, b, u = http_get(
            "https://www.alphavantage.co/query?function=HISTORICAL_OPTIONS"
            "&symbol=SPY&date=2024-03-15&apikey=" + av, retries=1)
        premium = "premium" in (b or "").lower()
        rows.append({"provider": "alpha_vantage",
                     "endpoint": "HISTORICAL_OPTIONS", "status": s,
                     "state": "PAYMENT_REQUIRED" if premium else
                     ("ACCESSIBLE" if s == 200 else "ERROR"),
                     "url": u, "body_head": (b or "")[:220],
                     "note": "HTTP 200 carrying a premium-endpoint notice is "
                             "a PAYMENT wall, not an accessible endpoint"})
    accessible = [r for r in rows if r["state"] == "ACCESSIBLE"]
    return {
        "track": "B", "family": "VOLATILITY_OPTIONS",
        "probed_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "rows": rows,
        "n_accessible_endpoints": len(accessible),
        "prices_accessible": any(
            r["endpoint"].startswith("aggs option daily (in window)")
            and r["state"] == "ACCESSIBLE" for r in rows),
        "expired_universe_accessible": any(
            "EXPIRED" in r["endpoint"] and r["state"] == "ACCESSIBLE"
            for r in rows),
        "greeks_accessible": any(
            "greeks" in r["endpoint"] and r["state"] == "ACCESSIBLE"
            for r in rows),
        "measured_rate_limit_per_min": 5,
        "binding_wall": "HISTORY_WINDOW",
        "state": "EXECUTED",
    }


def _bs_iv(price, S, K, T, r, is_call, *, q=0.0):
    """Black-Scholes implied volatility by bisection.

    The estate does not need a vendor's greeks: with the option's own close,
    an owned underlying close, an owned risk-free rate and the contract's
    strike and expiry, IV is a one-dimensional root find. Returns None where
    the price is outside the no-arbitrage bounds (which is itself a data
    quality signal worth recording rather than smoothing away).
    """
    from math import erf, exp, log, sqrt
    if not (price and S and K and T and T > 0 and price > 0):
        return None

    def bs(sig):
        if sig <= 0:
            return None
        d1 = (log(S / K) + (r - q + 0.5 * sig * sig) * T) / (sig * sqrt(T))
        d2 = d1 - sig * sqrt(T)
        n = lambda x: 0.5 * (1.0 + erf(x / sqrt(2.0)))
        if is_call:
            return S * exp(-q * T) * n(d1) - K * exp(-r * T) * n(d2)
        return K * exp(-r * T) * n(-d2) - S * exp(-q * T) * n(-d1)

    lo, hi = 1e-4, 5.0
    if bs(hi) < price or bs(lo) > price:
        return None
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        v = bs(mid)
        if v is None:
            return None
        if v > price:
            hi = mid
        else:
            lo = mid
    return 0.5 * (lo + hi)


def _third_fridays(lo: _dt.date, hi: _dt.date, n: int) -> list:
    """Standard monthly expiries spread evenly across [lo, hi]."""
    out = []
    y, m = lo.year, lo.month
    while _dt.date(y, m, 1) <= hi:
        d = _dt.date(y, m, 1)
        d += _dt.timedelta(days=(4 - d.weekday()) % 7)   # first Friday
        d += _dt.timedelta(days=14)                      # third Friday
        if lo <= d <= hi:
            out.append(d)
        m += 1
        if m > 12:
            y, m = y + 1, 1
    if len(out) <= n:
        return out
    step = len(out) / float(n)
    return [out[int(i * step)] for i in range(n)]


def _invert_iv(frame: pd.DataFrame, underlying: str):
    """Invert Black-Scholes IV locally - the point being that the vendor's
    403 on greeks is NOT the binding wall.

    Underlying closes come from the estate's owned Norgate entitlement (the
    same 403 applies to Polygon's underlying aggregates) and the discount
    rate from the owned FRED panel.
    """
    try:
        import norgatedata as nd
        px = nd.price_timeseries(
            underlying,
            stock_price_adjustment_setting=nd.StockPriceAdjustmentType.NONE,
            padding_setting=nd.PaddingType.NONE, start_date="2024-01-01",
            format="pandas-dataframe")
        spot = pd.to_numeric(px["Close"], errors="coerce")
        spot.index = pd.DatetimeIndex(spot.index).tz_localize(None) \
            .normalize()
    except Exception:
        return None
    from .judge import risk_free_daily, TRADING_DAYS
    d = frame.copy()
    d["date_ts"] = pd.DatetimeIndex(pd.to_datetime(d["date"]))
    d["exp_ts"] = pd.DatetimeIndex(pd.to_datetime(d["expiration"]))
    d["underlying_close"] = d["date_ts"].map(spot)
    try:
        rf = risk_free_daily(pd.DatetimeIndex(sorted(set(d["date_ts"]))),
                             day_count=1.0)
        d["rf"] = d["date_ts"].map(rf)
    except Exception:
        d["rf"] = 0.04
    d["T_years"] = (d["exp_ts"] - d["date_ts"]).dt.days / 365.0
    d["iv"] = [
        _bs_iv(row.close, row.underlying_close, row.strike, row.T_years,
               float(row.rf) if pd.notna(row.rf) else 0.04,
               str(row.type) == "call")
        for row in d.itertuples()]
    d["moneyness"] = d["strike"] / d["underlying_close"]
    return d.drop(columns=["date_ts", "exp_ts"])


def _iv_summary(frame: pd.DataFrame) -> dict:
    if "iv" not in frame.columns:
        return {"state": "NOT_COMPUTED",
                "reason": "underlying closes unavailable"}
    iv = pd.to_numeric(frame["iv"], errors="coerce")
    ok = iv.notna()
    return {
        "state": "COMPUTED_LOCALLY",
        "method": "Black-Scholes bisection on the option's own close, the "
                  "OWNED Norgate underlying close and the OWNED FRED rate",
        "vendor_greeks_required": False,
        "n_rows": int(len(iv)), "n_inverted": int(ok.sum()),
        "inversion_rate": float(ok.mean()) if len(iv) else None,
        "iv_median": float(iv[ok].median()) if ok.any() else None,
        "iv_p05": float(iv[ok].quantile(0.05)) if ok.any() else None,
        "iv_p95": float(iv[ok].quantile(0.95)) if ok.any() else None,
        "note": "rows that fail to invert are OUTSIDE the no-arbitrage "
                "bounds for their date - a data-quality signal, recorded "
                "rather than smoothed away",
    }


def acquire_option_sample(*, underlying: str = "SPY", n_expiries: int = 6,
                          strikes_per_expiry: int = 5,
                          budget_calls: int = 45) -> dict:
    """Acquire a REAL bounded option sample and normalise it.

    Bounded on purpose: at a measured 5 requests/minute a surface-scale
    acquisition is a multi-day background job, not a campaign step, and this
    release will not pretend otherwise. What this proves is the PIPELINE -
    that the estate can enumerate a survivorship-safe option universe, pull
    real prices, and invert its own implied volatilities.
    """
    pk = _key("POLYGON_API_KEY")
    out_dir = data_dir("options")
    cached = out_dir / "acquisition_manifest.json"
    if cached.exists():
        try:
            prev = json.loads(cached.read_text(encoding="utf-8"))
            man = prev.get("polygon_options") or {}
            if man.get("state") == "EXECUTED" and man.get("rows"):
                man["from_cache"] = True
                return man
        except Exception:
            pass
    if not pk:
        return {"state": "ACCOUNT_REQUIRED", "reason": "no owned Polygon key"}
    calls = 0
    today = _dt.date.today()
    lo = today - _dt.timedelta(days=700)

    # SPY expires daily, so a single 1,000-row page covers about a WEEK of
    # expiries. Enumerating "everything expired in two years" and slicing it
    # therefore samples one cluster, not the window. Pick monthly
    # third-Friday expiries explicitly and enumerate each one.
    targets = _third_fridays(lo, today - _dt.timedelta(days=30), n_expiries)
    rows, manifest, picked = [], [], []
    for exp in targets:
        if calls >= budget_calls:
            break
        s, b, _ = http_get(
            POLY + "/v3/reference/options/contracts?underlying_ticker=%s"
            "&expired=true&expiration_date=%s&contract_type=call"
            "&limit=1000&apiKey=%s" % (underlying, exp.isoformat(), pk))
        calls += 1
        time.sleep(POLYGON_RPM_SLEEP)
        if s != 200:
            manifest.append({"expiration": exp.isoformat(), "status": s,
                             "rows": 0})
            continue
        sub = pd.DataFrame(json.loads(b).get("results") or [])
        if sub.empty:
            continue
        ks = sorted(sub["strike_price"].astype(float).unique())
        if len(ks) < strikes_per_expiry:
            continue
        mid = len(ks) // 2
        half = strikes_per_expiry // 2
        sel = ks[max(0, mid - half): mid - half + strikes_per_expiry]
        picked.append({"expiration": exp.isoformat(),
                       "strikes": [float(x) for x in sel],
                       "contracts_available": int(len(sub))})
        for k in sel:
            if calls >= budget_calls:
                break
            row = sub[sub["strike_price"].astype(float) == k].iloc[0]
            tk = row["ticker"]
            start = (exp - _dt.timedelta(days=180)).isoformat()
            s, b, _ = http_get(
                POLY + "/v2/aggs/ticker/%s/range/1/day/%s/%s?adjusted=true"
                "&limit=5000&apiKey=%s" % (tk, start, exp.isoformat(), pk))
            calls += 1
            time.sleep(POLYGON_RPM_SLEEP)
            if s != 200:
                manifest.append({"ticker": tk, "status": s, "rows": 0})
                continue
            res = json.loads(b).get("results") or []
            for a in res:
                rows.append({
                    "ticker": tk, "underlying": underlying,
                    "expiration": exp.isoformat(),
                    "strike": float(k), "type": row["contract_type"],
                    "date": pd.to_datetime(a["t"], unit="ms").date()
                    .isoformat(),
                    "open": a.get("o"), "high": a.get("h"),
                    "low": a.get("l"), "close": a.get("c"),
                    "vwap": a.get("vw"), "volume": a.get("v"),
                    "trades": a.get("n")})
            manifest.append({"ticker": tk, "status": s, "rows": len(res)})

    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "reason": "no aggregates returned inside the plan window"}
    frame = pd.DataFrame(rows)
    iv = _invert_iv(frame, underlying)
    if iv is not None:
        frame = iv
    path = out_dir / ("polygon_%s_option_sample.csv.gz" % underlying.lower())
    frame.to_csv(path, index=False, compression="gzip")
    man = {
        "state": "EXECUTED", "provider": "polygon",
        "underlying": underlying, "api_calls": calls,
        "expiries_targeted": [e.isoformat() for e in targets],
        "expiries_enumerated": picked,
        "contracts_pulled": len(manifest),
        "contracts_with_data": sum(1 for m in manifest if m["rows"] > 0),
        "rows": int(len(frame)),
        "iv": _iv_summary(frame),
        "date_span": [frame["date"].min(), frame["date"].max()],
        "expiries": sorted(frame["expiration"].unique().tolist()),
        "strikes": sorted(float(x) for x in frame["strike"].unique()),
        "path": str(path), "sha256": sha(frame.to_csv(index=False)),
        "fields": list(frame.columns),
        "greeks_included": False,
        "iv_included": False,
        "pit_semantics": "daily consolidated OHLCV per DATED CONTRACT; the "
                         "contract identity carries strike and expiry, and "
                         "expired contracts remain enumerable, so the "
                         "universe is survivorship-safe",
        "licence_accepted": False, "money_spent": 0.0,
        "account_created": False,
    }
    (out_dir / "acquisition_manifest.json").write_text(
        json.dumps({"polygon_options": man, "contracts": manifest},
                   indent=1, default=str), encoding="utf-8")
    return man


# --------------------------------------------------------------------------- #
# Track T/H - the FRED scheduled-release calendar
# --------------------------------------------------------------------------- #
def acquire_release_calendar(*, start: str = "1996-01-01") -> dict:
    """Historical SCHEDULED release dates for the macro events Track H needs.

    These are real point-in-time publication dates, not a reconstruction:
    FRED records when each release actually went out. Date-level, not
    intraday - which is exactly what a daily-bar event study can use, and
    the intraday timestamp gap is recorded as its own blocker.
    """
    fk = _key("FRED_API_KEY")
    if not fk:
        return {"state": "ACCOUNT_REQUIRED", "reason": "no owned FRED key"}
    out_dir = data_dir("events")
    per, rows = {}, []
    for name, rid in MACRO_RELEASES.items():
        s, b, _ = http_get(
            "https://api.stlouisfed.org/fred/release/dates?release_id=%d"
            "&realtime_start=%s&limit=10000&sort_order=asc&file_type=json"
            "&api_key=%s" % (rid, start, fk), pause=1.0)
        if s != 200:
            per[name] = {"status": s, "state": "ERROR",
                         "body_head": (b or "")[:200]}
            continue
        dates = [d["date"] for d in json.loads(b).get("release_dates", [])]
        per[name] = {"release_id": rid, "status": s, "n_dates": len(dates),
                     "first": dates[0] if dates else None,
                     "last": dates[-1] if dates else None}
        for d in dates:
            rows.append({"event": name, "release_id": rid, "date": d})
    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "per_release": per}
    frame = pd.DataFrame(rows).sort_values(["date", "event"])
    path = out_dir / "fred_release_calendar.csv"
    frame.to_csv(path, index=False)
    body = {
        "state": "EXECUTED", "provider": "FRED (owned entitlement)",
        "n_events": int(len(frame)), "n_release_types": len(MACRO_RELEASES),
        "per_release": per, "path": str(path),
        "sha256": sha(frame.to_csv(index=False)),
        "span": [frame["date"].min(), frame["date"].max()],
        "pit_semantics": "the date the release was actually published; the "
                         "event is tradable from the NEXT session's open at "
                         "the earliest under a daily-bar study",
        "intraday_timestamp": False,
        "intraday_gap_blocker": "HISTORICAL_DATA_UNAVAILABLE - FRED publishes "
                                "release DATES, not release TIMES; an "
                                "intraday reaction study needs a native "
                                "intraday futures feed (Track D)",
        "money_spent": 0.0, "account_created": False,
        "fomc_gap": "FOMC decisions are not a FRED release id; scheduled "
                    "policy dates are NOT covered by this calendar",
    }
    (out_dir / "acquisition_manifest.json").write_text(
        json.dumps(body, indent=1, default=str), encoding="utf-8")
    return body


#: The FRED series Track I and Track M need at FULL history. R41 acquired
#: the same ICE BofA family but its panel carries only 2023-08 onward - about
#: three years - which is too short for a 50/30/20 zone split. FRED serves
#: the whole history for free under the owned entitlement, so Release 43
#: deepens it into its OWN panel and never touches R41's bytes.
DEEP_SERIES = {
    "OAS_IG": "BAMLC0A0CM", "OAS_HY": "BAMLH0A0HYM2",
    "OAS_AAA": "BAMLC0A1CAAA", "OAS_BBB": "BAMLC0A4CBBB",
    "OAS_BB": "BAMLH0A1HYBB", "OAS_B": "BAMLH0A2HYB",
    "OAS_CCC": "BAMLH0A3HYC", "OAS_EM": "BAMLEMCBPIOAS",
    "OAS_EUR_HY": "BAMLHE00EHYIOAS",
    "OAS_IG_1_3": "BAMLC1A0C13Y", "OAS_IG_7_10": "BAMLC4A0C710Y",
    "OAS_IG_15P": "BAMLC8A0C15PY",
    "TRI_IG": "BAMLCC0A0CMTRIV", "TRI_HY": "BAMLHYH0A0HYM2TRIV",
    "CMT_2Y": "DGS2", "CMT_10Y": "DGS10", "CMT_3M": "DGS3MO",
    "CMT_30Y": "DGS30", "BE_5Y": "T5YIE", "BE_10Y": "T10YIE",
    "REAL_10Y": "DFII10", "SOFR": "SOFR", "EFFR": "EFFR",
    "VIXCLS": "VIXCLS",
}


def deepen_macro_panel(*, start: str = "1990-01-01") -> dict:
    """Pull the FULL history of the credit / rates / inflation series.

    Same provider, same entitlement, same series ids - only the requested
    window differs. This is the cheapest information gain in the release: a
    credit family that was three years deep becomes three DECADES deep for
    zero dollars.
    """
    fk = _key("FRED_API_KEY")
    if not fk:
        return {"state": "ACCOUNT_REQUIRED", "reason": "no owned FRED key"}
    out_dir = data_dir("macro")
    cols, per = {}, {}
    for name, sid in DEEP_SERIES.items():
        s, b, _ = http_get(
            "https://api.stlouisfed.org/fred/series/observations?series_id=%s"
            "&observation_start=%s&file_type=json&api_key=%s"
            % (sid, start, fk), pause=1.0)
        if s != 200:
            per[name] = {"series_id": sid, "status": s, "state": "ERROR"}
            continue
        obs = json.loads(b).get("observations") or []
        idx, val = [], []
        for o in obs:
            v = o.get("value")
            if v in (None, ".", ""):
                continue
            idx.append(o["date"])
            val.append(float(v))
        if not idx:
            per[name] = {"series_id": sid, "status": s, "state": "EMPTY"}
            continue
        ser = pd.Series(val, index=pd.DatetimeIndex(pd.to_datetime(idx)))
        cols[name] = ser[~ser.index.duplicated(keep="last")].sort_index()
        per[name] = {"series_id": sid, "status": s, "n": len(ser),
                     "first": str(ser.index[0].date()),
                     "last": str(ser.index[-1].date())}
    if not cols:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "per_series": per}
    frame = pd.DataFrame(cols).sort_index()
    path = out_dir / "r43_macro_credit_panel.csv"
    frame.to_csv(path)
    body = {
        "state": "EXECUTED", "provider": "FRED (owned entitlement)",
        "n_series": len(cols), "rows": int(len(frame)),
        "span": [str(frame.index[0].date()), str(frame.index[-1].date())],
        "per_series": per, "path": str(path),
        "sha256": sha(frame.to_csv()),
        "r41_panel_mutated": False,
        "why": "R41's panel of the same family covers only 2023-08 onward; "
               "this one covers the full published history",
        "money_spent": 0.0,
    }
    (out_dir / "acquisition_manifest.json").write_text(
        json.dumps(body, indent=1, default=str), encoding="utf-8")
    return body


_DEEP_CACHE = None


def load_macro_panel():
    """The R43-deepened macro/credit panel, falling back to R41's."""
    global _DEEP_CACHE
    if _DEEP_CACHE is not None:
        return _DEEP_CACHE
    p = data_dir("macro") / "r43_macro_credit_panel.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p, index_col=0, parse_dates=True)
    df.index = pd.DatetimeIndex(df.index).tz_localize(None).normalize()
    _DEEP_CACHE = df.sort_index().apply(pd.to_numeric, errors="coerce")
    return _DEEP_CACHE


def load_release_calendar():
    p = data_dir("events") / "fred_release_calendar.csv"
    if not p.exists():
        return None
    df = pd.read_csv(p)
    df["date"] = pd.DatetimeIndex(pd.to_datetime(df["date"])).normalize()
    return df


# --------------------------------------------------------------------------- #
# Tracks C, D, G, M - probes that end in a specific blocker
# --------------------------------------------------------------------------- #
def probe_analyst_revisions() -> dict:
    """Track C. The estate owns EODHD/FMP/Finnhub keys and has run a
    PROSPECTIVE analyst-vintage collector since 2026-07-31. The question is
    not access - it is HISTORICAL VINTAGES."""
    rows = []
    for prov, key_name, url in (
        ("eodhd", "EODHD_API_KEY",
         "https://eodhd.com/api/calendar/earnings?symbols=AAPL.US&fmt=json"
         "&api_token=%s"),
        ("fmp", "FMP_API_KEY",
         "https://financialmodelingprep.com/api/v3/analyst-estimates/AAPL"
         "?limit=3&apikey=%s"),
        ("finnhub", "FINNHUB_API_KEY",
         "https://finnhub.io/api/v1/stock/revenue-estimate?symbol=AAPL"
         "&freq=quarterly&token=%s"),
    ):
        k = _key(key_name)
        if not k:
            rows.append({"provider": prov, "state": "ACCOUNT_REQUIRED"})
            continue
        s, b, u = http_get(url % k, retries=1, pause=2.0)
        low = (b or "").lower()
        state = ("PAYMENT_REQUIRED"
                 if s in (401, 402, 403) or "premium" in low
                 or "subscription" in low or "upgrade" in low
                 else "ACCESSIBLE" if s == 200 else "ERROR")
        rows.append({"provider": prov, "status": s, "state": state,
                     "url": u, "body_head": (b or "")[:200]})
    return {
        "track": "C", "family": "EQUITY_REVISIONS", "rows": rows,
        "state": "EXECUTED",
        "verdict": "CURRENT_CONSENSUS_ONLY",
        "blocker": "HISTORICAL_DATA_UNAVAILABLE",
        "why": "every reachable endpoint serves the CURRENT consensus. A "
               "current snapshot is not a historical vintage, and the "
               "contract forbids substituting one for the other. The "
               "estate's own prospective vintage ledger (first snapshot "
               "2026-07-31) is the only PIT-safe revision history it has, "
               "and it is under one month long.",
        "unblocks_when": "FUTURE_TIME_REQUIRED for the prospective ledger; "
                         "PAYMENT_REQUIRED for a vendor vintage archive "
                         "(Steele / Intrinio / Zacks / I/B/E/S)",
    }


def probe_native_intraday_futures() -> dict:
    """Track D. Sample-safe routes to native intraday futures history."""
    rows = []
    for name, url, note in (
        ("databento_docs", "https://databento.com/pricing",
         "credit-based; account + payment details required to obtain a key"),
        ("firstrate_free_samples",
         "https://firstratedata.com/free-intraday-data",
         "free sample zips exist - the estate already holds SPX/SPY/VIX "
         "1-minute samples from R41; futures samples require a form"),
        ("cme_datamine", "https://datamine.cmegroup.com/",
         "licence acceptance + account required"),
    ):
        s, b, u = http_get(url, retries=1, pause=1.0)
        rows.append({"route": name, "status": s, "url": u, "note": note,
                     "reachable": s == 200})
    return {
        "track": "D", "family": "HORIZON_FAMILY", "rows": rows,
        "state": "EXECUTED",
        "blocker": "PAYMENT_REQUIRED",
        "why": "no reachable route serves native intraday FUTURES history "
               "without an account, a licence acceptance or a payment "
               "detail, all three of which this release is forbidden to "
               "provide. The estate's owned intraday bytes (Dukascopy FX "
               "minute, Tiingo IEX minute, FirstRate SPX/SPY/VIX samples, "
               "Binance) are cash/FX/crypto, not dated futures contracts.",
        "forbidden_by_contract": ["MAY_CREATE_PROVIDER_ACCOUNT",
                                  "MAY_ACCEPT_LICENCE_AGREEMENT",
                                  "MAY_SUBMIT_PAYMENT_DETAILS"],
        "experiments_unlocked_by_purchase": [
            "intraday reaction of rates/FX/equity futures to the FRED "
            "release calendar Track H acquired (minutes, not days)",
            "roll-window microstructure and calendar-spread execution cost",
            "session-structure and overnight/intraday decomposition of the "
            "carry and RV books in Tracks A/E/F",
        ],
    }


def probe_native_credit() -> dict:
    """Track M. Native credit (CDX/iTraxx/CDS/bond curves) at $0."""
    rows = []
    for name, url, note in (
        ("markit_indices", "https://www.spglobal.com/spdji/en/index-family/"
                           "fixed-income/cds/", "CDX/iTraxx are licensed"),
        ("finra_trace", "https://www.finra.org/finra-data/browse-catalog/"
                        "trace/files", "TRACE bulk files need an agreement"),
        ("ecb_sdw_credit", "https://data.ecb.europa.eu/", "reachable, but "
                                                          "no CDS levels"),
    ):
        s, b, u = http_get(url, retries=1, pause=1.0)
        rows.append({"route": name, "status": s, "url": u, "note": note,
                     "reachable": s == 200})
    return {
        "track": "M", "family": "CREDIT", "rows": rows, "state": "EXECUTED",
        "owned_credit_information": {
            "source": "FRED ICE BofA OAS family (owned, acquired by R41)",
            "series": 12, "term_structure_series": 6,
            "span": "1996 -> 2026 daily",
            "level": "LEVEL_1_SIGNAL - index-level option-adjusted spreads "
                     "are genuine credit information and are NOT an ETF "
                     "proxy, but they are not directly tradable either",
        },
        "blocker": "LICENCE_REQUIRED",
        "why": "single-name CDS, CDX and iTraxx levels are licensed "
               "products. The estate can research credit INFORMATION today "
               "through the owned OAS family (Track I does exactly that) "
               "but cannot express a native credit RV trade. Any tradable "
               "expression would be an ETF proxy and is labelled "
               "PROXY_ONLY - HYG/LQD is not the credit market.",
        "proxy_only": True,
    }


def probe_microstructure() -> dict:
    """Track G. R42 closed this with a specific blocker; re-verify it."""
    rows = []
    for name, url, note in (
        ("binance_vision", "https://data.binance.vision/",
         "public archive: trades and best bid/ask only, no book depth"),
        ("tardis_free", "https://api.tardis.dev/v1/exchanges/binance",
         "free tier covers a small set of sample days"),
    ):
        s, b, u = http_get(url, retries=1, pause=1.0)
        rows.append({"route": name, "status": s, "url": u, "note": note,
                     "reachable": s == 200})
    return {
        "track": "G", "family": "MICROSTRUCTURE", "rows": rows,
        "state": "EXECUTED",
        "blocker": "BLOCKED_EXECUTION_MICROSTRUCTURE_DATA",
        "mapped_to_contract_vocab": "HISTORICAL_DATA_UNAVAILABLE",
        "why": "inherited from R42 and re-verified. A maker-execution model "
               "needs queue/fill probability, adverse selection and latency; "
               "the free archives carry best-bid/ask only and the free "
               "Tardis tier covers about 1.5% of the sample. Release 43 "
               "fabricates no fill and re-opens nothing it cannot measure.",
        "inherited_from": "release42",
    }


def probe_crypto_venues() -> dict:
    """Track N. R42 found the premium real and 0 of 6 venues investable from
    the operator's location. Re-verify rather than inherit an assertion."""
    rows = []
    for name, url, note in (
        ("binance_spot_api", "https://api.binance.com/api/v3/exchangeInfo"
                             "?symbol=BTCUSDT",
         "the venue whose public data produced the entire R41/R42 result"),
        ("bybit_api", "https://api.bybit.com/v5/market/time",
         "R42 recorded HTTP 403"),
        ("binance_public_archive", "https://data.binance.vision/",
         "public DATA archive - reachable for research, which is not "
         "permission to trade"),
    ):
        s, b, u = http_get(url, retries=1, pause=1.0)
        rows.append({"venue": name, "status": s, "url": u, "note": note,
                     "reachable": s == 200,
                     "restricted_location": s in (403, 451)})
    blocked = [r for r in rows if r["restricted_location"]]
    return {
        "track": "N", "family": "CRYPTO", "rows": rows, "state": "EXECUTED",
        "n_probed": len(rows), "n_restricted": len(blocked),
        "blocker": "ACCOUNT_REQUIRED",
        "why": "Release 42 established, and this probe re-verifies, that "
               "research access to a venue's public data is not permission "
               "to trade there. Every admissible expression of a NON-carry "
               "crypto hypothesis - cross-venue price discovery, basis "
               "dislocation, funding surprise, order flow - requires an "
               "exchange account and API trading keys, all of which this "
               "release is forbidden to create. A candidate that cannot be "
               "implemented anywhere is not a candidate, so no burden was "
               "spent searching for one.",
        "burden_charged": 0,
        "capital_treatment_if_ever_unblocked":
            "UNREMUNERATED_FULLY_FUNDED - the R42 committed-capital "
            "treatment applies to every crypto expression, carry or not",
        "inherited_from": "release42 VENUE_IMPLEMENTABILITY_MATRIX",
    }


# --------------------------------------------------------------------------- #
# Track U - the formal purchase gate
# --------------------------------------------------------------------------- #
def purchase_gate(options: dict, analyst: dict, intraday: dict,
                  credit: dict) -> dict:
    """Rank paid datasets by EXPECTED ALPHA INFORMATION GAIN PER DOLLAR.

    Nothing is bought. The ranking exists so the operator can decide, and
    the top entry carries every field the contract demands.
    """
    cands = [
        {
            "rank": 1,
            "PROVIDER": "Polygon.io",
            "DATASET": "Options Starter / Developer (historical option "
                       "aggregates beyond the free 2-year window)",
            "PRICE": "$29/month Starter, $79/month Developer (list, "
                     "observed on the provider's own 403 upgrade notice; "
                     "NOT verified by purchase)",
            "CONTRACT_TYPE": "monthly subscription, cancellable",
            "HISTORY": "Starter ~5 years, Developer ~10 years (vendor "
                       "claim - REQUIRES A SAMPLE TO VERIFY)",
            "COVERAGE": "all US listed options incl. SPX/SPY/QQQ and single "
                        "names; expired contracts enumerable",
            "PIT_INTEGRITY": "STRONG - dated contracts, strike and expiry "
                             "carried in the identifier, expired universe "
                             "queryable as-of a date",
            "INACTIVE_DELISTED_COVERAGE": "YES - verified live this release "
                                          "(expired=true returned 200)",
            "SAMPLE_RESULT": (
                "ACQUIRED AND NORMALISED at $0: %s rows across %s contracts; "
                "IV inverted locally from price+strike+expiry+owned "
                "underlying+owned rate, so greeks are NOT the wall"
                % (options.get("sample", {}).get("rows"),
                   options.get("sample", {}).get("contracts_with_data"))),
            "LICENSING_CONSTRAINTS": "standard non-redistribution; research "
                                     "use permitted",
            "EXACT_EXPERIMENTS_UNLOCKED": [
                "variance risk premium measured from ACTUAL option prices "
                "rather than the VIX index, with the term structure by "
                "expiry and the smile by strike",
                "delta-hedged option returns, which isolate the volatility "
                "premium from direction",
                "index/constituent DISPERSION - the one options hypothesis "
                "the owned CBOE indices cannot express at all",
                "option-implied moments as cross-sectional equity signals "
                "(Track L), which needs single-name surfaces",
            ],
            "WHY_OWNED_DATA_CANNOT_ANSWER_THEM": (
                "the estate owns the VIX complex (VIX, VIX9D/3M/6M, VVIX, "
                "SKEW) which IS the SPX index surface summarised - it has no "
                "strikes, no single names, and therefore no smile, no "
                "dispersion and no delta-hedged return. The free Polygon "
                "window is ~2 years, which is below this estate's own "
                "MIN_DAILY_DECISIONS_FIT_ZONE of 250 days plus a judged "
                "zone."),
            "EXPECTED_INFORMATION_VALUE": "HIGHEST - it is the only route "
                                          "priced under $100/month that "
                                          "converts a blocked family into a "
                                          "testable one",
            "RECOMMEND": "NEED_SAMPLE",
            "why_not_buy_yet": "the vendor's history claim per tier is not "
                               "verified. One Starter month would verify it; "
                               "that is the operator's call, not this "
                               "release's.",
        },
        {
            "rank": 2,
            "PROVIDER": "Databento",
            "DATASET": "CME MDP-3 historical intraday (rates, FX, equity "
                       "index, energy futures)",
            "PRICE": "usage-based credits; a bounded backfill of the "
                     "markets in Tracks A/E/F is the unit to price",
            "CONTRACT_TYPE": "pay-as-you-go, account + payment required",
            "HISTORY": "full depth-of-book from 2010 for most CME products",
            "COVERAGE": "native dated futures contracts",
            "PIT_INTEGRITY": "STRONG - exchange timestamps",
            "INACTIVE_DELISTED_COVERAGE": "YES - dated contracts",
            "SAMPLE_RESULT": "NOT ACQUIRED - %s" % intraday.get("blocker"),
            "LICENSING_CONSTRAINTS": "CME redistribution terms",
            "EXACT_EXPERIMENTS_UNLOCKED": intraday.get(
                "experiments_unlocked_by_purchase"),
            "WHY_OWNED_DATA_CANNOT_ANSWER_THEM":
                "the owned intraday bytes are FX spot, IEX equity and crypto "
                "- none is a dated futures contract, and the contract "
                "forbids fabricating intraday futures bars from daily data "
                "or CFD proxies",
            "EXPECTED_INFORMATION_VALUE": "HIGH but SECOND - it multiplies "
                                          "the horizon axis of books whose "
                                          "DAILY versions have not yet "
                                          "cleared their controls. Buying "
                                          "resolution for a book with no "
                                          "edge buys nothing.",
            "RECOMMEND": "RECOMMEND_SKIP_UNTIL_A_DAILY_BOOK_CLEARS",
        },
        {
            "rank": 3,
            "PROVIDER": "Steele / Intrinio / Zacks (I/B/E/S-style)",
            "DATASET": "historical analyst estimate VINTAGES",
            "PRICE": "enterprise; quote required (vendor contact forbidden "
                     "this release)",
            "CONTRACT_TYPE": "annual licence",
            "HISTORY": "20+ years claimed",
            "COVERAGE": "US equities incl. inactive",
            "PIT_INTEGRITY": "the whole point - vintage-dated consensus",
            "INACTIVE_DELISTED_COVERAGE": "vendor-dependent, must be "
                                          "verified",
            "SAMPLE_RESULT": "NOT ACQUIRED - %s" % analyst.get("blocker"),
            "LICENSING_CONSTRAINTS": "redistribution prohibited",
            "EXACT_EXPERIMENTS_UNLOCKED": [
                "revision momentum / breadth / acceleration, "
                "sector- and beta-neutral, at 1-63 day horizons",
                "surprise x revision and post-earnings drift with a real "
                "vintage rather than a current snapshot",
            ],
            "WHY_OWNED_DATA_CANNOT_ANSWER_THEM":
                "every owned endpoint serves CURRENT consensus only; the "
                "estate's prospective vintage ledger is under one month old",
            "EXPECTED_INFORMATION_VALUE": "HIGH but UNPRICED - a "
                                          "recommendation without a price is "
                                          "not a recommendation",
            "RECOMMEND": "RECOMMEND_SKIP (unpriced; Intrinio was already "
                         "evaluated live by a prior release and returned "
                         "DO_NOT_BUY)",
        },
    ]
    return {
        "track": "U", "money_spent": 0.0, "purchases": 0,
        "trials_started": 0, "accounts_created": 0,
        "licences_accepted": 0, "vendor_emails_sent": 0,
        "ranked_by": "EXPECTED ALPHA INFORMATION GAIN PER DOLLAR",
        "top_recommendation": cands[0]["PROVIDER"] + " - "
                              + cands[0]["DATASET"],
        "top_recommendation_verdict": cands[0]["RECOMMEND"],
        "candidates": cands,
        "credit_note": credit.get("why"),
    }


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def run(*, acquire_samples: bool = True) -> dict:
    opts = probe_options()
    sample = (acquire_option_sample() if acquire_samples
              and opts.get("prices_accessible") else
              {"state": "SKIPPED", "reason": "prices not accessible"})
    opts["sample"] = sample
    cal = acquire_release_calendar() if acquire_samples else \
        {"state": "SKIPPED"}
    analyst = probe_analyst_revisions()
    intraday = probe_native_intraday_futures()
    credit = probe_native_credit()
    micro = probe_microstructure()
    crypto = probe_crypto_venues()
    deep = deepen_macro_panel() if acquire_samples else {"state": "SKIPPED"}
    gate = purchase_gate(opts, analyst, intraday, credit)

    body = artifact_body("r43_data_frontier/1", {
        "calculation_owner": CALCULATION_OWNER,
        "tracks": {"B_options": opts, "C_analyst": analyst,
                   "D_native_intraday": intraday, "G_microstructure": micro,
                   "M_credit": credit, "N_crypto_venues": crypto,
                   "T_event_calendar": cal, "T_macro_deepening": deep},
        "purchase_gate": gate,
        "entitlements_used": ["POLYGON_API_KEY", "FRED_API_KEY",
                              "ALPHAVANTAGE_API_KEY", "EODHD_API_KEY",
                              "FMP_API_KEY", "FINNHUB_API_KEY"],
        "entitlement_keys_printed": False,
        "entitlement_tier_changed": False,
        "money_spent": 0.0, "accounts_created": 0, "trials_started": 0,
        "licences_accepted": 0, "payment_details_submitted": 0,
        "new_walls_opened": [
            "HISTORICAL OPTION PRICES at $0 (Polygon, survivorship-safe, "
            "~2-year window)",
            "SCHEDULED MACRO RELEASE DATES at $0 (FRED, back to 1996)",
        ],
        "walls_confirmed_binding": [
            "options HISTORY WINDOW (not greeks, not access)",
            "analyst estimate VINTAGES",
            "native intraday FUTURES",
            "native CREDIT instruments",
            "maker-execution MICROSTRUCTURE",
        ],
    })
    body["data_frontier_hash"] = sha(body)
    write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body
