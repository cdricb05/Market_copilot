"""alpha_agent.r42.acquisition - $0, account-free, read-only data acquisition.

Every byte this module fetches comes from a PUBLIC endpoint that requires
no account, no key, no payment and no licence acceptance. Provenance (URL,
UTC fetch time, byte count, SHA-256) is recorded for everything. Nothing is
written outside the R42 research root on D:.

Sources:
* ``data.binance.vision`` - the Binance public archive: monthly spot and
  USD-M perpetual klines and the full realised funding-rate history, for
  EVERY symbol the archive preserves (including delisted ones, which is
  what makes a survivorship-safe universe possible);
* OKX / Deribit / BitMEX / Hyperliquid / Kraken-Futures / Coinbase-INTX
  public market-data endpoints, for the cross-venue replication.

It creates no exchange account, holds no API key, places no order and
touches no operational store.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import io
import json
import re
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import requests

from . import data_dir, read_json, write_json

CALCULATION_OWNER = "alpha_agent.r42.acquisition"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) PaperTraderResearch/R42 "
      "(read-only public market-data acquisition; no account; no orders)")
BV = "https://data.binance.vision/data"
S3 = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

_SESSION = None


def session() -> requests.Session:
    global _SESSION
    if _SESSION is None:
        s = requests.Session()
        s.headers["User-Agent"] = UA
        _SESSION = s
    return _SESSION


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
def _manifest_path(kind: str) -> Path:
    return data_dir(kind) / "acquisition_manifest.json"


def record(kind: str, key: str, entry: dict) -> None:
    p = _manifest_path(kind)
    man = read_json(p) or {"schema": "r42_acquisition_manifest/1",
                           "calculation_owner": CALCULATION_OWNER,
                           "entries": {}}
    man.setdefault("entries", {})[key] = {**entry, "fetched_at": _now()}
    man["n_entries"] = len(man["entries"])
    write_json(p, man, immutable=False)


def load_manifest(kind: str) -> dict:
    return read_json(_manifest_path(kind)) or {}


def fetch(url: str, *, timeout: int = 60, retries: int = 3,
          method: str = "GET", body: dict = None):
    """One read-only HTTP request with retries. Returns bytes or None."""
    s = session()
    for i in range(retries):
        try:
            if method == "GET":
                r = s.get(url, timeout=timeout)
            else:
                r = s.post(url, json=body, timeout=timeout)
            if r.status_code == 200:
                return r.content
            if r.status_code in (403, 404, 451):
                return None
        except Exception:
            pass
        time.sleep(0.4 * (i + 1))
    return None


# --------------------------------------------------------------------------- #
# Binance public archive
# --------------------------------------------------------------------------- #
def s3_prefixes(prefix: str, *, max_pages: int = 80) -> list:
    """Immediate sub-prefixes of an archive prefix (the symbol listing)."""
    out, marker = [], None
    s = session()
    for _ in range(max_pages):
        u = "%s?delimiter=/&prefix=%s" % (S3, prefix)
        if marker:
            u += "&marker=%s" % marker
        try:
            r = s.get(u, timeout=60)
        except Exception:
            break
        found = re.findall(r"<Prefix>%s([^<]+)/</Prefix>" % re.escape(prefix),
                           r.text)
        out.extend(found)
        if "<IsTruncated>true</IsTruncated>" in r.text and found:
            marker = prefix + found[-1] + "/"
        else:
            break
    return sorted(set(out))


def s3_keys(prefix: str, *, max_pages: int = 40) -> list:
    """Every object key under a prefix (the months a symbol actually has)."""
    out, marker = [], None
    s = session()
    for _ in range(max_pages):
        u = "%s?prefix=%s" % (S3, prefix)
        if marker:
            u += "&marker=%s" % marker
        try:
            r = s.get(u, timeout=60)
        except Exception:
            break
        found = re.findall(r"<Key>([^<]+)</Key>", r.text)
        out.extend(found)
        if "<IsTruncated>true</IsTruncated>" in r.text and found:
            marker = found[-1]
        else:
            break
    return out


ARCHIVE_LISTING = "binance_archive_listing.json"


def archive_listing(*, refresh: bool = False) -> dict:
    """Survivorship-safe symbol listing: every symbol the archive keeps."""
    p = data_dir("binance") / ARCHIVE_LISTING
    cached = read_json(p)
    if cached and not refresh:
        return cached
    perp_funding = s3_prefixes("data/futures/um/monthly/fundingRate/")
    perp_klines = s3_prefixes("data/futures/um/monthly/klines/")
    spot_klines = s3_prefixes("data/spot/monthly/klines/")
    body = {"schema": "r42_binance_archive_listing/1",
            "calculation_owner": CALCULATION_OWNER,
            "listed_at": _now(),
            "perp_funding_symbols": perp_funding,
            "perp_kline_symbols": perp_klines,
            "spot_kline_symbols": spot_klines,
            "n_perp_funding": len(perp_funding),
            "n_perp_klines": len(perp_klines),
            "n_spot_klines": len(spot_klines),
            "survivorship_note":
                "the archive preserves a folder for every symbol ever "
                "listed, including delisted ones; membership is therefore "
                "decided by DATA EXISTENCE, never by today's active list"}
    write_json(p, body, immutable=False)
    return body


MONTH_RE = re.compile(r"(\d{4})-(\d{2})\.zip$")


def symbol_months(symbol: str, kind: str) -> list:
    """Months present in the archive for one symbol and data kind."""
    if kind == "funding":
        pre = "data/futures/um/monthly/fundingRate/%s/" % symbol
    elif kind == "perp_1d":
        pre = "data/futures/um/monthly/klines/%s/1d/" % symbol
    elif kind == "spot_1d":
        pre = "data/spot/monthly/klines/%s/1d/" % symbol
    else:
        raise ValueError(kind)
    months = []
    for k in s3_keys(pre):
        m = MONTH_RE.search(k)
        if m:
            months.append("%s-%s" % (m.group(1), m.group(2)))
    return sorted(set(months))


COVERAGE = "binance_symbol_coverage.json"


def survey_coverage(symbols: list, *, workers: int = 12,
                    progress=None) -> dict:
    """One S3 listing per (symbol, kind): what history actually exists.

    This is METADATA ONLY. It is what the frozen eligibility rule consumes;
    no strategy outcome is computed here and none may be.
    """
    p = data_dir("binance") / COVERAGE
    cached = read_json(p) or {"schema": "r42_binance_symbol_coverage/1",
                              "calculation_owner": CALCULATION_OWNER,
                              "symbols": {}}
    todo = [s for s in symbols if s not in cached.get("symbols", {})]
    done = 0

    def one(sym):
        return sym, {k: symbol_months(sym, k)
                     for k in ("funding", "perp_1d", "spot_1d")}

    if todo:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(one, s): s for s in todo}
            for f in as_completed(futs):
                try:
                    sym, cov = f.result()
                except Exception:
                    continue
                cached["symbols"][sym] = cov
                done += 1
                if progress and done % 25 == 0:
                    progress("coverage %d/%d" % (done, len(todo)))
                if done % 100 == 0:
                    write_json(p, cached, immutable=False)
    cached["surveyed_at"] = _now()
    cached["n_symbols"] = len(cached["symbols"])
    write_json(p, cached, immutable=False)
    return cached


def _kline_frame(raw: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        df = pd.read_csv(z.open(z.namelist()[0]), header=None)
    if isinstance(df.iloc[0, 0], str):
        df = df.iloc[1:]
    df = df.iloc[:, :11]
    df.columns = ["open_time", "open", "high", "low", "close", "volume",
                  "close_time", "quote_volume", "trades", "taker_buy_base",
                  "taker_buy_quote"]
    ot = pd.to_numeric(df["open_time"])
    unit = "us" if ot.iloc[0] > 1e14 else "ms"
    df.index = pd.to_datetime(ot, unit=unit, utc=True)
    return df.drop(columns=["open_time", "close_time"]).astype(float)


def _dest(symbol: str, kind: str, month: str) -> Path:
    root = data_dir("binance_universe")
    if kind == "funding":
        return root / "funding" / symbol / ("%s_funding_%s.csv" % (symbol,
                                                                  month))
    market = "um" if kind == "perp_1d" else "spot"
    return root / market / symbol / ("%s_1d_%s.csv.gz" % (symbol, month))


def acquire_symbol(symbol: str, coverage: dict, *, kinds=("funding",
                                                          "perp_1d",
                                                          "spot_1d")) -> dict:
    got = {k: 0 for k in kinds}
    miss = {k: 0 for k in kinds}
    for kind in kinds:
        for month in coverage.get(kind, []):
            dest = _dest(symbol, kind, month)
            if dest.exists():
                got[kind] += 1
                continue
            y, m = month.split("-")
            if kind == "funding":
                url = ("%s/futures/um/monthly/fundingRate/%s/"
                       "%s-fundingRate-%s-%s.zip" % (BV, symbol, symbol, y, m))
            elif kind == "perp_1d":
                url = ("%s/futures/um/monthly/klines/%s/1d/%s-1d-%s-%s.zip"
                       % (BV, symbol, symbol, y, m))
            else:
                url = ("%s/spot/monthly/klines/%s/1d/%s-1d-%s-%s.zip"
                       % (BV, symbol, symbol, y, m))
            raw = fetch(url, timeout=90, retries=2)
            if not raw:
                miss[kind] += 1
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            try:
                if kind == "funding":
                    with zipfile.ZipFile(io.BytesIO(raw)) as z:
                        df = pd.read_csv(z.open(z.namelist()[0]))
                    df.to_csv(dest, index=False)
                else:
                    _kline_frame(raw).to_csv(dest, compression="gzip")
            except Exception:
                miss[kind] += 1
                continue
            got[kind] += 1
    return {"symbol": symbol, "got": got, "missing": miss}


def acquire_universe(symbols: list, coverage: dict, *, workers: int = 10,
                     progress=None) -> dict:
    res, done = [], 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(acquire_symbol, s,
                          coverage.get("symbols", {}).get(s, {})): s
                for s in symbols}
        for f in as_completed(futs):
            try:
                res.append(f.result())
            except Exception as exc:
                res.append({"symbol": futs[f], "error": str(exc)})
            done += 1
            if progress and done % 10 == 0:
                progress("acquired %d/%d symbols" % (done, len(symbols)))
    tot = {"n_symbols": len(res),
           "files": sum(sum(r.get("got", {}).values()) for r in res),
           "missing": sum(sum(r.get("missing", {}).values()) for r in res)}
    record("binance_universe", "acquire_universe",
           {"n_symbols": len(res), "files": tot["files"],
            "missing": tot["missing"], "source": BV,
            "licence": "public archive, no account, no payment"})
    return {"totals": tot, "per_symbol": res}


def load_universe_daily(symbol: str) -> pd.DataFrame:
    """Daily spot/perp/funding panel for one universe symbol."""
    root = data_dir("binance_universe")

    def klines(market):
        d = root / market / symbol
        frames = []
        for p in sorted(d.glob("%s_1d_*.csv.gz" % symbol)):
            try:
                frames.append(pd.read_csv(p, index_col=0, parse_dates=True))
            except Exception:
                continue
        if not frames:
            return pd.DataFrame()
        out = pd.concat(frames)
        out.index = pd.to_datetime(out.index, utc=True, errors="coerce",
                                   format="mixed")
        out = out[out.index.notna()].sort_index()
        return out[~out.index.duplicated(keep="last")]

    spot, perp = klines("spot"), klines("um")
    fd = root / "funding" / symbol
    fr = []
    for p in sorted(fd.glob("%s_funding_*.csv" % symbol)):
        try:
            fr.append(pd.read_csv(p))
        except Exception:
            continue
    if not len(spot) or not len(perp) or not fr:
        return pd.DataFrame()
    f = pd.concat(fr, ignore_index=True)
    tcol = "calc_time" if "calc_time" in f.columns else "fundingTime"
    rcol = "last_funding_rate" if "last_funding_rate" in f.columns \
        else "fundingRate"
    ts = pd.to_numeric(f[tcol], errors="coerce").dropna()
    if not len(ts):
        return pd.DataFrame()
    unit = "us" if ts.iloc[0] > 1e14 else "ms"
    fs = pd.Series(pd.to_numeric(f[rcol], errors="coerce").values,
                   index=pd.to_datetime(pd.to_numeric(f[tcol],
                                                      errors="coerce"),
                                        unit=unit, utc=True)).sort_index()
    fs = fs[~fs.index.duplicated(keep="last")]
    df = pd.DataFrame({
        "spot": spot["close"].resample("1D").last(),
        "perp": perp["close"].resample("1D").last(),
        "spot_quote_volume": spot["quote_volume"].resample("1D").sum(),
        "perp_quote_volume": perp["quote_volume"].resample("1D").sum(),
    })
    df["funding"] = fs.resample("1D").sum()
    df["n_funding_events"] = fs.resample("1D").count()
    return df.dropna(subset=["spot", "perp"])


# --------------------------------------------------------------------------- #
# Other venues (public market data only)
# --------------------------------------------------------------------------- #
VENUE_ENDPOINTS = {
    "BINANCE_REST": {
        "probe": "https://fapi.binance.com/fapi/v1/fundingRate"
                 "?symbol=BTCUSDT&limit=5", "method": "GET"},
    "BYBIT": {
        "probe": "https://api.bybit.com/v5/market/funding/history"
                 "?category=linear&symbol=BTCUSDT&limit=5", "method": "GET"},
    "OKX": {
        "probe": "https://www.okx.com/api/v5/public/funding-rate-history"
                 "?instId=BTC-USDT-SWAP&limit=5", "method": "GET"},
    "DERIBIT": {
        "probe": "https://www.deribit.com/api/v2/public/"
                 "get_funding_rate_history?instrument_name=BTC-PERPETUAL"
                 "&start_timestamp=1700000000000&end_timestamp=1700086400000",
        "method": "GET"},
    "BITMEX": {
        "probe": "https://www.bitmex.com/api/v1/funding?symbol=XBTUSD"
                 "&count=5&reverse=true", "method": "GET"},
    "KRAKEN_FUTURES": {
        "probe": "https://futures.kraken.com/derivatives/api/v4/"
                 "historicalfundingrates?symbol=PF_XBTUSD", "method": "GET"},
    "HYPERLIQUID": {
        "probe": "https://api.hyperliquid.xyz/info", "method": "POST",
        "body": {"type": "fundingHistory", "coin": "BTC",
                 "startTime": 1700000000000, "endTime": 1700086400000}},
    "COINBASE_INTX": {
        "probe": "https://api.international.coinbase.com/api/v1/instruments/"
                 "BTC-PERP/funding?result_limit=5", "method": "GET"},
}


def probe_venue(name: str) -> dict:
    spec = VENUE_ENDPOINTS[name]
    s = session()
    t0 = time.time()
    rec = {"venue": name, "url": spec["probe"], "method": spec["method"],
           "probed_at": _now()}
    try:
        if spec["method"] == "GET":
            r = s.get(spec["probe"], timeout=45)
        else:
            r = s.post(spec["probe"], json=spec.get("body"), timeout=45)
        rec["status"] = r.status_code
        rec["bytes"] = len(r.content)
        rec["ms"] = int((time.time() - t0) * 1000)
        rec["sha256"] = hashlib.sha256(r.content).hexdigest()
        body = r.text[:400]
        rec["body_head"] = body
        if r.status_code == 200:
            rec["state"] = "PUBLIC_OK"
        elif r.status_code == 451:
            rec["state"] = "VENUE_GEO_RESTRICTED"
        elif r.status_code == 403:
            rec["state"] = "BLOCKED_403"
        else:
            rec["state"] = "HTTP_%s" % r.status_code
    except Exception as exc:
        rec["state"] = "EXCEPTION"
        rec["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:200])
    record("venues", "probe_%s" % name, {k: rec.get(k) for k in
                                         ("url", "status", "bytes",
                                          "sha256", "state")})
    return rec


# --- per-venue funding history downloaders (public, paginated) ------------- #
def okx_funding(inst: str = "BTC-USDT-SWAP", *, max_pages: int = 60) -> pd.Series:
    url = ("https://www.okx.com/api/v5/public/funding-rate-history"
           "?instId=%s&limit=100" % inst)
    rows, after = [], None
    s = session()
    for _ in range(max_pages):
        u = url + ("&after=%s" % after if after else "")
        try:
            r = s.get(u, timeout=45)
            d = r.json().get("data") or []
        except Exception:
            break
        if not d:
            break
        rows.extend(d)
        after = d[-1]["fundingTime"]
        time.sleep(0.12)
    if not rows:
        return pd.Series(dtype=float)
    idx = pd.to_datetime([int(x["fundingTime"]) for x in rows], unit="ms",
                         utc=True)
    val = [float(x.get("realizedRate") or x.get("fundingRate") or 0.0)
           for x in rows]
    return pd.Series(val, index=idx).sort_index()


def deribit_funding(inst: str = "BTC-PERPETUAL", start: str = "2020-01-01",
                    end: str = None) -> pd.Series:
    """Deribit publishes HOURLY rows carrying BOTH ``interest_1h`` (the
    rate actually accrued in that hour) and ``interest_8h`` (a trailing
    8-hour rate). Summing ``interest_8h`` over 24 hourly rows overstates
    the daily cashflow eightfold. This returns the per-hour accrual, which
    is what a position actually pays or receives."""
    s = session()
    t0 = pd.Timestamp(start, tz="UTC")
    t1 = pd.Timestamp(end or _dt.datetime.now(_dt.timezone.utc))
    if t1.tzinfo is None:
        t1 = t1.tz_localize("UTC")
    out = {}
    cur = t0
    while cur < t1:
        nxt = min(cur + pd.Timedelta(days=25), t1)
        u = ("https://www.deribit.com/api/v2/public/get_funding_rate_history"
             "?instrument_name=%s&start_timestamp=%d&end_timestamp=%d"
             % (inst, int(cur.timestamp() * 1000), int(nxt.timestamp() * 1000)))
        try:
            r = s.get(u, timeout=60)
            res = r.json().get("result") or []
        except Exception:
            res = []
        for x in res:
            out[int(x["timestamp"])] = float(x.get("interest_1h") or 0.0)
        cur = nxt
        time.sleep(0.1)
    if not out:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(sorted(out), unit="ms", utc=True)
    return pd.Series([out[k] for k in sorted(out)], index=idx)


def bitmex_funding(symbol: str = "XBTUSD", *, max_pages: int = 80) -> pd.Series:
    s = session()
    rows, start = [], 0
    for _ in range(max_pages):
        u = ("https://www.bitmex.com/api/v1/funding?symbol=%s&count=500"
             "&reverse=false&start=%d" % (symbol, start))
        try:
            r = s.get(u, timeout=60)
            if r.status_code != 200:
                break
            d = r.json()
        except Exception:
            break
        if not d:
            break
        rows.extend(d)
        start += len(d)
        time.sleep(1.15)          # BitMEX public rate limit is strict
        if len(d) < 500:
            break
    if not rows:
        return pd.Series(dtype=float)
    idx = pd.to_datetime([x["timestamp"] for x in rows], utc=True,
                         format="ISO8601")
    return pd.Series([float(x["fundingRate"]) for x in rows],
                     index=idx).sort_index()


def hyperliquid_funding(coin: str = "BTC", start: str = "2023-06-01",
                        end: str = None) -> pd.Series:
    s = session()
    t0 = pd.Timestamp(start, tz="UTC")
    t1 = pd.Timestamp(end or _dt.datetime.now(_dt.timezone.utc))
    if t1.tzinfo is None:
        t1 = t1.tz_localize("UTC")
    out = {}
    cur = t0
    while cur < t1:
        nxt = min(cur + pd.Timedelta(days=20), t1)
        try:
            r = s.post("https://api.hyperliquid.xyz/info",
                       json={"type": "fundingHistory", "coin": coin,
                             "startTime": int(cur.timestamp() * 1000),
                             "endTime": int(nxt.timestamp() * 1000)},
                       timeout=60)
            d = r.json() if r.status_code == 200 else []
        except Exception:
            d = []
        for x in d or []:
            out[int(x["time"])] = float(x["fundingRate"])
        cur = nxt
        time.sleep(0.12)
    if not out:
        return pd.Series(dtype=float)
    idx = pd.to_datetime(sorted(out), unit="ms", utc=True)
    return pd.Series([out[k] for k in sorted(out)], index=idx)


def kraken_funding(symbol: str = "PF_XBTUSD") -> pd.Series:
    raw = fetch("https://futures.kraken.com/derivatives/api/v4/"
                "historicalfundingrates?symbol=%s" % symbol, timeout=60)
    if not raw:
        return pd.Series(dtype=float)
    try:
        d = json.loads(raw.decode("utf-8")).get("rates") or []
    except Exception:
        return pd.Series(dtype=float)
    if not d:
        return pd.Series(dtype=float)
    idx = pd.to_datetime([x["timestamp"] for x in d], utc=True,
                         format="ISO8601")
    # Kraken publishes an absolute funding rate; relativeFundingRate is the
    # per-interval fraction actually exchanged.
    val = [float(x.get("relativeFundingRate", x.get("fundingRate", 0.0)))
           for x in d]
    return pd.Series(val, index=idx).sort_index()


def coinbase_intx_funding(inst: str = "BTC-PERP", *,
                          max_pages: int = 40) -> pd.Series:
    s = session()
    rows, off = [], 0
    for _ in range(max_pages):
        u = ("https://api.international.coinbase.com/api/v1/instruments/"
             "%s/funding?result_limit=1000&result_offset=%d" % (inst, off))
        try:
            r = s.get(u, timeout=60)
            if r.status_code != 200:
                break
            d = r.json().get("results") or []
        except Exception:
            break
        if not d:
            break
        rows.extend(d)
        off += len(d)
        time.sleep(0.15)
        if len(d) < 1000:
            break
    if not rows:
        return pd.Series(dtype=float)
    idx = pd.to_datetime([x["event_time"] for x in rows], utc=True,
                         format="ISO8601")
    return pd.Series([float(x["funding_rate"]) for x in rows],
                     index=idx).sort_index()


def save_venue_series(venue: str, name: str, s: pd.Series) -> str:
    d = data_dir("venues") / venue
    d.mkdir(parents=True, exist_ok=True)
    p = d / ("%s_funding.csv.gz" % name)
    s.to_frame("rate").to_csv(p, compression="gzip")
    record("venues", "%s_%s" % (venue, name),
           {"rows": int(len(s)),
            "first": str(s.index.min()) if len(s) else None,
            "last": str(s.index.max()) if len(s) else None,
            "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    return str(p)


def load_venue_series(venue: str, name: str) -> pd.Series:
    p = data_dir("venues") / venue / ("%s_funding.csv.gz" % name)
    if not p.exists():
        return pd.Series(dtype=float)
    df = pd.read_csv(p, index_col=0)
    idx = pd.to_datetime(df.index, utc=True, errors="coerce", format="mixed")
    s = pd.Series(pd.to_numeric(df["rate"], errors="coerce").to_numpy(),
                  index=idx)
    s = s[s.index.notna() & s.notna()]
    return s[~s.index.duplicated(keep="last")].sort_index()


#: Published funding cadence per venue. Aggregating a venue's rows without
#: knowing which interval each row's rate refers to is exactly the error
#: this release exists to catch, so the cadence is asserted and verified.
VENUE_FUNDING_CADENCE = {
    "BINANCE": {"rows_per_day": 3, "rate_interval_hours": 8},
    "OKX": {"rows_per_day": 3, "rate_interval_hours": 8},
    "BITMEX": {"rows_per_day": 3, "rate_interval_hours": 8},
    "DERIBIT": {"rows_per_day": 24, "rate_interval_hours": 1},
    "HYPERLIQUID": {"rows_per_day": 24, "rate_interval_hours": 1},
    "COINBASE_INTX": {"rows_per_day": 24, "rate_interval_hours": 1},
    "KRAKEN_FUTURES": {"rows_per_day": 24, "rate_interval_hours": 1},
}


def cadence_audit(venue: str, s: pd.Series) -> dict:
    """Verify a venue series matches its declared cadence."""
    exp = VENUE_FUNDING_CADENCE.get(venue, {})
    if not len(s):
        return {"state": "NO_DATA", "expected": exp}
    gaps = pd.Series(s.index).diff().dt.total_seconds() / 3600.0
    modal_gap = float(gaps.round(2).mode().iloc[0]) if len(gaps.dropna()) \
        else None
    per_day = s.resample("1D").count()
    modal_rows = int(per_day[per_day > 0].mode().iloc[0]) if len(per_day) \
        else None
    ok = (modal_rows == exp.get("rows_per_day")
          and modal_gap is not None
          and abs(modal_gap - (24.0 / max(exp.get("rows_per_day", 1), 1)))
          < 0.51)
    return {"state": "OK" if ok else "CADENCE_MISMATCH",
            "expected_rows_per_day": exp.get("rows_per_day"),
            "expected_rate_interval_hours": exp.get("rate_interval_hours"),
            "observed_modal_rows_per_day": modal_rows,
            "observed_modal_gap_hours": modal_gap,
            "n_rows": int(len(s)),
            "matches": bool(ok)}
