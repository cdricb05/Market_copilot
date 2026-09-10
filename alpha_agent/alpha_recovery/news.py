"""alpha_agent.alpha_recovery.news - a BOUNDED, point-in-time news sample and the
NEWS_INTENSITY block.

Workstream 5: prove what is true about news. The audit (a read-only depth
probe of the entitled EODHD news endpoint) found: nothing before 2018,
single items in 2018, hundreds per quarter for large caps from 2021, tens for
mid caps, and coverage of names later delisted (TWTR, ATVI) inside that
window. So a universe-wide ten-year history is NOT owned; a PIT-plausible
2021-onward sample IS entitled. This module acquires exactly that, bounded:

    sample rule (pre-registered before any fetch): the PIT S&P 500 members
    on the first session of 2021 that pass the R63 eligibility rule that day
    and carry a resolved CIK, ordered by stable hash of the symbol, first
    SAMPLE_N; later-delisted names are kept (no survivorship).
    window: 2020-10-01 to the last panel session, quarterly pages.
    stored fields: publication timestamp, title, symbols, tags, vendor
    sentiment; never the article body. Local analytical use only.

PIT: an item counts on the first session whose close follows its publication
timestamp (16:00 ET cut-off), then the R63 broadcast lag applies through the
dataset convention. Vendor sentiment is computed by the provider at an
unknown time and is labelled PIT_UNVERIFIED; article counts are PIT-plausible.
Nothing here is a subscription, a purchase or a live write.
"""
from __future__ import annotations

import gzip
import json
import os
import sqlite3
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_agent.r63 import IDENTITY_DB, features as FE
from alpha_agent.r63 import experiments as X

from . import now_iso, research_root, stable_hash

CALCULATION_OWNER = "alpha_agent.alpha_recovery.news"
BLOCK = "NEWS_INTENSITY"
BLOCK_SENT_ONLY = "NEWS_SENTIMENT_ONLY"
BLOCK_COUNT_ONLY = "NEWS_COUNT_ONLY"
STORE_SUBDIR = "_data_eodhd_news"
MANIFEST = "acquisition_manifest.json"
SAMPLE_N = 120
SAMPLE_SESSION = "2021-01-04"
WINDOW_START = "2020-10-01"
API_BASE = "https://eodhd.com/api/news"
API_KEY_ENV = "EODHD_API_KEY"
PAGE = 1000
MIN_INTERVAL_S = 0.3
PIT_SENTIMENT = "PIT_UNVERIFIED (vendor-computed at an unknown time)"
PIT_COUNTS = "PIT_PLAUSIBLE (publication timestamps)"

_CACHE: dict = {}


def store_dir() -> Path:
    return research_root() / STORE_SUBDIR


def _ro(path) -> sqlite3.Connection:
    return sqlite3.connect("file:%s?immutable=1" % str(path).replace("\\", "/"), uri=True)


def ticker_map() -> dict:
    """{norgate_symbol: ticker} from the identity layer (a delisted Norgate
    symbol such as TWTR-202210 maps to its ticker TWTR)."""
    if "tickers" in _CACHE:
        return _CACHE["tickers"]
    out = {}
    try:
        con = _ro(IDENTITY_DB)
        for sym, tk in con.execute("SELECT norgate_symbol, ticker FROM securities"):
            if sym and tk:
                out[str(sym)] = str(tk)
        con.close()
    except sqlite3.Error:
        out = {}
    _CACHE["tickers"] = out
    return out


def sample(E: dict, elig: np.ndarray, *, n: int = SAMPLE_N, session: str = SAMPLE_SESSION) -> list:
    """The pre-registered sample: deterministic, survivorship-free."""
    dates = E["dates"]
    t = int(np.searchsorted(dates, session))
    tk = ticker_map()
    cands = []
    for si, sym in enumerate(E["symbols"]):
        if not elig[si, t] or sym not in E["sym2cik"] or sym not in tk:
            continue
        cands.append((stable_hash(str(sym)), str(sym), tk[sym]))
    cands.sort()
    return [{"symbol": s, "ticker": k, "eodhd": "%s.US" % k} for _h, s, k in cands[:n]]


# --------------------------------------------------------------------------- #
# Acquisition (bounded, resumable, read-only against the provider)
# --------------------------------------------------------------------------- #
def _quarters(start: str, end: str) -> list:
    out = []
    d = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    while d < e:
        q_end = (d.replace(day=1) + timedelta(days=93)).replace(day=1) - timedelta(days=1)
        out.append((d.strftime("%Y-%m-%d"), min(q_end, e).strftime("%Y-%m-%d")))
        d = q_end + timedelta(days=1)
    return out


def _fetch(sym: str, frm: str, to: str, offset: int, key: str) -> list:
    q = urllib.parse.urlencode({"s": sym, "from": frm, "to": to, "limit": PAGE, "offset": offset,
                                "api_token": key, "fmt": "json"})
    req = urllib.request.Request(API_BASE + "?" + q,
                                 headers={"User-Agent": "paper-trader-alpha-recovery-research"})
    with urllib.request.urlopen(req, timeout=60) as r:
        body = json.loads(r.read().decode("utf-8"))
    return body if isinstance(body, list) else []


def _slim(item: dict) -> dict:
    sent = item.get("sentiment") if isinstance(item.get("sentiment"), dict) else {}
    return {"date": item.get("date"), "title": item.get("title"), "symbols": item.get("symbols"),
            "tags": item.get("tags"), "polarity": sent.get("polarity"), "neg": sent.get("neg"),
            "neu": sent.get("neu"), "pos": sent.get("pos"), "link": item.get("link")}


def acquire(names: list, *, end: str, max_requests: int = 6000, max_seconds: float = 5400.0,
            verbose: bool = True) -> dict:
    """Fetch the sample, one gzip-jsonl per ticker, resumable. Returns the
    manifest. A missing key is a DATA_HOLD, never an error."""
    key = os.environ.get(API_KEY_ENV)
    d = store_dir()
    d.mkdir(parents=True, exist_ok=True)
    man_p = d / MANIFEST
    man = json.loads(man_p.read_text(encoding="utf-8")) if man_p.exists() else {
        "schema": "alpha_recovery_news_acquisition/1", "owner": CALCULATION_OWNER,
        "sample_rule": "PIT S&P 500 members on %s passing R63 eligibility with a resolved CIK, "
                       "ordered by stable hash of the symbol, first %d" % (SAMPLE_SESSION, SAMPLE_N),
        "window": [WINDOW_START, end], "fields": "date,title,symbols,tags,polarity,neg,neu,pos,link",
        "licence": "EODHD subscriber-entitled; local analytical use; not redistributed",
        "purchases": 0, "subscriptions": 0, "complete": {}, "requests": 0, "items": 0,
        "started_at": now_iso()}
    if not key:
        man["state"] = "DATA_HOLD_NO_API_KEY"
        man_p.write_text(json.dumps(man, indent=1), encoding="utf-8")
        return man
    t0 = time.time()
    quarters = _quarters(WINDOW_START, end)
    for row in names:
        tk = row["eodhd"]
        if man["complete"].get(tk):
            continue
        if man["requests"] >= max_requests or time.time() - t0 > max_seconds:
            man["state"] = "BOUNDED_STOP"
            break
        items = []
        ok = True
        for frm, to in quarters:
            offset = 0
            while True:
                if man["requests"] >= max_requests or time.time() - t0 > max_seconds:
                    ok = False
                    break
                try:
                    page = _fetch(tk, frm, to, offset, key)
                except Exception as exc:                          # noqa: BLE001
                    man.setdefault("errors", []).append({"ticker": tk, "window": [frm, to],
                                                         "error": str(exc)[:160]})
                    ok = False
                    break
                man["requests"] += 1
                items.extend(_slim(i) for i in page)
                time.sleep(MIN_INTERVAL_S)
                if len(page) < PAGE:
                    break
                offset += PAGE
            if not ok:
                break
        if ok:
            with gzip.open(d / ("%s.jsonl.gz" % tk.replace("/", "_")), "wt", encoding="utf-8") as fh:
                for it in items:
                    fh.write(json.dumps(it) + "\n")
            man["complete"][tk] = {"items": len(items), "retrieved_at": now_iso(),
                                   "symbol": row["symbol"]}
            man["items"] += len(items)
            if verbose:
                print("[news] %s items=%d requests=%d" % (tk, len(items), man["requests"]), flush=True)
        man_p.write_text(json.dumps(man, indent=1), encoding="utf-8")
    man["state"] = man.get("state") if man.get("state") == "BOUNDED_STOP" and len(man["complete"]) < len(names) \
        else ("COMPLETE" if len(man["complete"]) >= len(names) else "PARTIAL")
    man["n_complete"] = len(man["complete"])
    man["n_requested"] = len(names)
    man["finished_at"] = now_iso()
    man_p.write_text(json.dumps(man, indent=1), encoding="utf-8")
    return man


def manifest() -> dict | None:
    p = store_dir() / MANIFEST
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


# --------------------------------------------------------------------------- #
# The block
# --------------------------------------------------------------------------- #
def _load_items(tk: str) -> pd.DataFrame:
    p = store_dir() / ("%s.jsonl.gz" % tk.replace("/", "_"))
    if not p.exists():
        return pd.DataFrame(columns=["ts", "polarity", "neg"])
    rows = []
    with gzip.open(p, "rt", encoding="utf-8") as fh:
        for line in fh:
            try:
                it = json.loads(line)
            except ValueError:
                continue
            rows.append((it.get("date"), it.get("polarity"), it.get("neg")))
    df = pd.DataFrame(rows, columns=["ts", "polarity", "neg"])
    df["ts"] = pd.to_datetime(df["ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["ts"])
    df["polarity"] = pd.to_numeric(df["polarity"], errors="coerce")
    df["neg"] = pd.to_numeric(df["neg"], errors="coerce")
    return df


def build_blocks(E: dict, elig: np.ndarray | None = None) -> dict:
    """NEWS_INTENSITY (count z, net sentiment 5d, negative-intensity z) on the
    daily grid for the acquired sample; NaN elsewhere."""
    key = ("blocks", id(E))
    if key in _CACHE:
        return _CACHE[key]
    dates = E["dates"]
    n_sym, n_d = E["price"]["tr"].shape
    cnt_z = np.full((n_sym, n_d), np.nan)
    sent5 = np.full((n_sym, n_d), np.nan)
    neg_z = np.full((n_sym, n_d), np.nan)
    man = manifest() or {}
    done = man.get("complete") or {}
    tk_by_sym = {v.get("symbol"): tk for tk, v in done.items() if isinstance(v, dict)}
    cal = pd.to_datetime(dates)
    covered = 0
    for si, sym in enumerate(E["symbols"]):
        tk = tk_by_sym.get(sym)
        if not tk:
            continue
        df = _load_items(tk)
        if len(df) == 0:
            continue
        # an item published after 16:00 ET belongs to the next calendar day
        et = df["ts"].dt.tz_convert("America/New_York")
        day = (et + pd.Timedelta(hours=8)).dt.normalize().dt.tz_localize(None)
        daily = pd.DataFrame({"day": day, "polarity": df["polarity"], "neg": df["neg"]})
        full = pd.date_range(cal[0], cal[-1], freq="D")
        c = daily.groupby("day").size().reindex(full, fill_value=0.0).astype(float)
        pol = daily.groupby("day")["polarity"].sum().reindex(full, fill_value=0.0)
        ng = daily.groupby("day")["neg"].sum().reindex(full, fill_value=0.0)
        c5, c63 = c.rolling(5).sum(), c.rolling(63).sum()
        exp5 = c63 * 5.0 / 63.0
        z = (c5 - exp5) / np.sqrt(exp5 + 1.0)
        s5 = (pol.rolling(5).sum() / c5.replace(0.0, np.nan))
        n5, n63 = ng.rolling(5).sum(), ng.rolling(63).sum()
        nz = (n5 - n63 * 5.0 / 63.0) / np.sqrt(n63 * 5.0 / 63.0 + 1.0)
        first = pd.Timestamp(WINDOW_START) + pd.Timedelta(days=70)
        z[z.index < first] = np.nan
        s5[s5.index < first] = np.nan
        nz[nz.index < first] = np.nan
        cnt_z[si] = np.asarray(z.reindex(cal).to_numpy(), dtype=float)
        sent5[si] = np.asarray(s5.reindex(cal).to_numpy(), dtype=float)
        neg_z[si] = np.asarray(nz.reindex(cal).to_numpy(), dtype=float)
        covered += 1
    out = {BLOCK: FE._stack(cnt_z, sent5, neg_z), BLOCK_SENT_ONLY: FE._stack(sent5),
           BLOCK_COUNT_ONLY: FE._stack(cnt_z),
           "_report": {"symbols_covered": covered, "sample_n": SAMPLE_N,
                       "pit": {"counts": PIT_COUNTS, "sentiment": PIT_SENTIMENT},
                       "manifest_state": man.get("state"), "items": man.get("items")}}
    _CACHE[key] = out
    return out


def plan(E: dict, elig: np.ndarray, *, end: str | None = None) -> dict:
    names = sample(E, elig)
    return {"names": names, "n": len(names), "end": end or str(E["dates"][-1]),
            "quarters": len(_quarters(WINDOW_START, end or str(E["dates"][-1]))),
            "estimated_requests": len(names) * len(_quarters(WINDOW_START, end or str(E["dates"][-1]))) * 1.5}
