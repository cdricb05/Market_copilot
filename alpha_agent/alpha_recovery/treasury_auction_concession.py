"""alpha_agent.alpha_recovery.treasury_auction_concession - the preregistered
Treasury auction supply-concession executor (``TREASURY_AUCTION_SUPPLY_CONCESSION_V1``).

Executes ``research/preregistration/TREASURY_AUCTION_CONCESSION_PREREGISTRATION.md``
and changes nothing in it.

    mechanism    the Treasury sells a pre-announced size of one tenor on a fixed
                 date regardless of price; balance-sheet-limited primary dealers
                 absorb it, so the auctioned tenor cheapens into the auction
                 (Lou, Yan and Zhang 2013)
    position     SHORT the matched-tenor Treasury future, DV01-matched, from the
                 close of the entry session to the auction-day close
    direction    the short EARNS, pre-specified; a contradicted sign is a kill
    PIT          the entry session is strictly after the auction's
                 announcementDate; a 5-session window that would open earlier is
                 SHORTENED to the next session, never opened before the news
    data         TreasuryDirect TA_WS auction history (free, cached once, with a
                 fetch manifest) and the owned R38 native contract layer (ZF / ZN /
                 ZB front-contract returns, held contracts, per-market cost)
    legs         5y -> ZF, 10y -> ZN, 30y -> ZB. The 2y leg was EXCLUDED before any
                 result existed (PIT-truncated window, cost share; preregistration s3)
    statistics   alpha_agent.r63.sensitivity.nw_tstat, bh_fdr and _max_dd - reused

The session calendar is the futures bar dates themselves (this module imports
no ``api``, ``engine`` or ``db``). The event CALENDAR (placement, PIT, census)
never reads a return; returns are read only in :func:`evaluate`.

RESEARCH ONLY. No purchase, no subscription, no promotion, no registration, no
capital allocation, no portfolio mutation, no proposal, no order, no fill, no
live write. Every write lands under the campaign research root.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
import time
import urllib.request
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_agent.r59 import native as N
from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, now_iso, research_root,
               stable_hash, write_artifact)

CALCULATION_OWNER = "alpha_agent.alpha_recovery.treasury_auction_concession"
MECHANISM_ID = "TREASURY_AUCTION_SUPPLY_CONCESSION_V1"
ARTIFACT_NAME = "treasury_auction_concession.json"
CENSUS_ARTIFACT_NAME = "treasury_auction_census.json"
PREREGISTRATION = "research/preregistration/TREASURY_AUCTION_CONCESSION_PREREGISTRATION.md"
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat + alpha_agent.r63.sensitivity.bh_fdr "
          "(event-calendar book; run_cell has no cross-section or baseline block to score here)")

#: The catalog's frozen kill rule, verbatim. An executor handed a different rule
#: refuses to run rather than silently judging a changed contract.
KILL_RULE_FROZEN = (
    "Kill if the per-auction DV01-matched short book has NW t < 2.0 or net below 1.5 %/yr "
    "at the R38 per-market cost (2 bp per side); or the concession is already complete "
    "between announcement and entry (announcement-to-entry return carries the whole effect); "
    "or the 5y/10y/30y tenor legs disagree in sign; or it fails BH q=0.10 at m=1 with "
    "PRIMARY_DEALER_POSITIONS and RATES_CARRY_CURVE_RV inherited at p=1.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
#: Section 3 - auctioned remaining term (nearest whole year) -> R38 market.
TENOR_MARKET = {5: "ZF", 10: "ZN", 30: "ZB"}
MARKETS = tuple(TENOR_MARKET.values())
#: Section 3 - coupon tenors excluded BEFORE results, with the reason.
EXCLUDED_TENORS = {
    2: "PIT-truncated 1-2 session window (84.5% of qualification events, all confirmation "
       "events) and 59% of round-trip cost under DV01 matching (census 2026-09-13)",
    3: "no owned contract delivers a 3-year note (ZT takes <= 2y remaining, ZF >= 4y2m)",
    7: "deliverable into ZN, but a second auctioned tenor on the 10y leg doubles that leg and "
       "overlaps the 5y week; the frozen legs are 5y/10y/30y",
    20: "re-introduced 2020-05 only: no qualification-window history",
}
#: Section 7 - approximate constant futures modified durations (CTD-based,
#: typical of 2000-2026). They set DV01 weights and cost scaling ACROSS tenors;
#: every per-tenor sign and t-statistic is invariant to them.
APPROX_DURATION = {"ZF": 4.3, "ZN": 6.2, "ZB": 13.0}
REFERENCE_MARKET = "ZN"
HOLD_SESSIONS = 5                      # section 6: at most five return sessions
POST_AUCTION_SESSIONS = 5              # section 8: descriptive only
QUALIFICATION = ("2000-01-01", "2016-12-31")
CONFIRMATION = ("2017-01-01", "2026-12-31")
WINDOWS = {"QUALIFICATION": QUALIFICATION, "CONFIRMATION": CONFIRMATION}
KILL_RULE_COST_BPS = 1.0
COST_LADDER_BPS = (1.0, 2.0, 5.0)
T_FLOOR = STANDALONE_T_FLOOR           # 2.0
MAX_PIT_MISSING_SHARE = 0.05
MAX_UNUSABLE_SHARE = 0.02
MIN_TENOR_EVENTS = 12
INHERITED_NULLS = ("PRIMARY_DEALER_POSITIONS", "RATES_CARRY_CURVE_RV")

# Executor verdict vocabulary (alpha_agent.r59.mechanisms.EXECUTOR_VERDICTS).
V_QUALIFIED = "QUALIFIED"
V_NO_EDGE = "NO_EDGE"
V_PRICED = "KILLED_PRICED_BEFORE_ENTRY"
V_SAMPLE = "KILLED_INSUFFICIENT_SAMPLE"
V_PIT = "KILLED_PIT_UNESTABLISHED"
V_MATERIALITY = "KILLED_BELOW_MATERIALITY"
V_NONINCREMENTAL = "KILLED_NONINCREMENTAL"
V_UNSTABLE = "KILLED_UNSTABLE"
V_WRONG_SIGN = "KILLED_WRONG_SIGN"
V_MULTIPLICITY = "KILLED_MULTIPLICITY"
V_DATA_HOLD = "DATA_HOLD"
V_NEED_MORE = "NEED_MORE_EVIDENCE"
VERDICTS_USED = (V_QUALIFIED, V_NO_EDGE, V_PRICED, V_SAMPLE, V_PIT, V_MATERIALITY,
                 V_NONINCREMENTAL, V_UNSTABLE, V_WRONG_SIGN, V_MULTIPLICITY, V_DATA_HOLD,
                 V_NEED_MORE)

# --------------------------------------------------------------------------- #
# Auction history: fetched ONCE, then read from the cache only
# --------------------------------------------------------------------------- #
TA_WS_URL = "https://www.treasurydirect.gov/TA_WS/securities/search"
FETCH_YEARS = tuple(range(2000, 2027))
FETCH_TYPES = ("Note", "Bond")
DATA_SUBDIR = "_data_treasury_auctions"
CACHE_FILE = "ta_ws_notes_bonds_2000_2026.json"
MANIFEST_FILE = "fetch_manifest.json"
USER_AGENT = "paper-trader-research/1.0 (research only)"


def data_dir() -> Path:
    return research_root() / DATA_SUBDIR


def _http_get(url: str, timeout: float = 60.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as r:          # noqa: S310 - fixed https host
        return r.read()


def _atomic_bytes(p: Path, blob: bytes) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_bytes(blob)
    tmp.replace(p)


def fetch_auctions(dest: Path | None = None, *, years=FETCH_YEARS, getter=None) -> dict:
    """GET the TA_WS Note and Bond auction history one calendar year at a time
    (one large request times out) and cache it with a manifest of every request."""
    d = Path(dest or data_dir())
    get = getter or _http_get
    records, requests = [], []
    for y in years:
        for typ in FETCH_TYPES:
            url = ("%s?format=json&type=%s&dateFieldName=auctionDate&startDate=%d-01-01"
                   "&endDate=%d-12-31" % (TA_WS_URL, typ, y, y))
            body = None
            for attempt in range(3):
                try:
                    body = get(url)
                    break
                except OSError:
                    if attempt == 2:
                        raise
                    time.sleep(2.0 * (attempt + 1))
            rows = json.loads(body)
            requests.append({"url": url, "fetched_at_utc": now_iso(), "rows": len(rows),
                             "sha256": hashlib.sha256(body).hexdigest()})
            records.extend(rows)
    blob = json.dumps(records, sort_keys=True).encode("utf-8")
    manifest = {"source": "TreasuryDirect TA_WS securities/search (free public GET, no key)",
                "calculation_owner": CALCULATION_OWNER, "file": CACHE_FILE,
                "fetched_at_utc": now_iso(), "rows": len(records),
                "sha256": hashlib.sha256(blob).hexdigest(), "requests": requests,
                "paid_dollars": 0}
    _atomic_bytes(d / CACHE_FILE, blob)
    _atomic_bytes(d / MANIFEST_FILE, json.dumps(manifest, indent=1, sort_keys=True).encode("utf-8"))
    return manifest


def load_auctions(*, allow_fetch: bool = True) -> dict:
    """The cached auction history. A present cache is NEVER refetched; a cache
    that disagrees with its manifest is a DATA_HOLD, never a silent substitute."""
    d = data_dir()
    f, m = d / CACHE_FILE, d / MANIFEST_FILE
    if f.exists() and m.exists():
        manifest = json.loads(m.read_text(encoding="utf-8"))
        blob = f.read_bytes()
        sha = hashlib.sha256(blob).hexdigest()
        if sha != manifest.get("sha256"):
            return {"state": V_DATA_HOLD,
                    "why": "auction cache %s sha256 %s does not match its fetch manifest (%s); "
                           "it is neither refetched nor substituted" % (f, sha, manifest.get("sha256"))}
        return {"state": "OK", "records": json.loads(blob), "manifest": manifest, "from_cache": True}
    if f.exists() or m.exists():
        return {"state": V_DATA_HOLD, "why": "partial auction cache in %s (file %s, manifest %s)"
                                             % (d, f.exists(), m.exists())}
    if not allow_fetch:
        return {"state": V_DATA_HOLD, "why": "no auction cache in %s and fetching is disabled" % d}
    try:
        fetch_auctions(d)
    except (OSError, ValueError) as e:
        return {"state": V_DATA_HOLD, "why": "TA_WS auction fetch failed: %r" % (e,)}
    out = load_auctions(allow_fetch=False)
    out["from_cache"] = False
    return out


# --------------------------------------------------------------------------- #
# Classification (no price, no return)
# --------------------------------------------------------------------------- #
_TERM = re.compile(r"^\s*(\d+)-Year(?:\s+(\d+)-Month)?\s*$")


def _day(v) -> str | None:
    s = str(v or "").strip()
    return s[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", s) else None


def tenor_of(term) -> int | None:
    """Remaining term at auction, rounded to the nearest whole year (half up):
    '9-Year 11-Month' -> 10, '29-Year 6-Month' -> 30, '6-Year 4-Month' -> 6."""
    m = _TERM.match(str(term or ""))
    if not m:
        return None
    years = int(m.group(1)) + (int(m.group(2)) if m.group(2) else 0) / 12.0
    return int(math.floor(years + 0.5))


def window_of(day: str | None) -> str | None:
    for name, (lo, hi) in WINDOWS.items():
        if day and lo <= day <= hi:
            return name
    return None


def classify(rec: dict) -> dict:
    typ = str(rec.get("type") or "")
    sec = str(rec.get("securityType") or "")
    out = {"cusip": rec.get("cusip"), "auction_date": _day(rec.get("auctionDate")),
           "announcement_date": _day(rec.get("announcementDate")),
           "security_type": sec, "security_term": rec.get("securityTerm"),
           "original_security_term": rec.get("originalSecurityTerm"),
           "reopening": str(rec.get("reopening")) == "Yes",
           "tenor": None, "market": None, "exclusion": None}
    out["window"] = window_of(out["auction_date"])
    t = tenor_of(out["security_term"])
    if str(rec.get("tips")) == "Yes" or typ == "TIPS":
        out["exclusion"] = "TIPS"
    elif str(rec.get("floatingRate")) == "Yes" or typ == "FRN":
        out["exclusion"] = "FRN"
    elif sec not in FETCH_TYPES:
        out["exclusion"] = "NOT_A_NOMINAL_COUPON_SECURITY"
    elif t is None:
        out["exclusion"] = "UNPARSEABLE_TERM"
    elif t in TENOR_MARKET:
        out["tenor"], out["market"] = t, TENOR_MARKET[t]
    elif t in EXCLUDED_TENORS:
        out["exclusion"] = "EXCLUDED_TENOR_%dY" % t
    else:
        out["exclusion"] = "OFF_CURVE_REMAINING_TERM_%dY" % t
    return out


def auction_table(records: list) -> list:
    """Classified auctions, one per (CUSIP, auction date), in auction order."""
    seen, out = set(), []
    for r in records:
        c = classify(r)
        key = (c["cusip"], c["auction_date"])
        if key in seen or not c["auction_date"]:
            continue
        seen.add(key)
        out.append(c)
    return sorted(out, key=lambda a: (a["auction_date"], str(a["tenor"]), str(a["cusip"])))


# --------------------------------------------------------------------------- #
# PIT placement on the futures session calendar (no return is read)
# --------------------------------------------------------------------------- #
ST_OK = "OK"
ST_NO_ANNOUNCEMENT = "NO_ANNOUNCEMENT_DATE"
ST_ANN_NOT_BEFORE = "ANNOUNCEMENT_NOT_BEFORE_AUCTION"
ST_NOT_SESSION = "AUCTION_DATE_NOT_A_FUTURES_SESSION"
ST_BEFORE_DATA = "BEFORE_FUTURES_DATA"
ST_BEYOND_DATA = "AFTER_FUTURES_DATA"
ST_NO_HOLD = "NO_HOLDING_SESSION_AFTER_PIT"
ST_MISSING_RETURN = "MISSING_RETURN_IN_WINDOW"
PIT_FAILURES = (ST_NO_ANNOUNCEMENT, ST_ANN_NOT_BEFORE)
UNUSABLE = (ST_NOT_SESSION, ST_BEFORE_DATA, ST_MISSING_RETURN)


def pit_entry(dates: np.ndarray, auction_idx: int, announcement_date: str) -> tuple:
    """(entry session E, last session on or before the announcement S_ann).
    E = max(A - 5, first session STRICTLY after announcementDate)."""
    first_after = int(np.searchsorted(dates, announcement_date, side="right"))
    return max(int(auction_idx) - HOLD_SESSIONS, first_after), first_after - 1


def place_event(a: dict, dates: np.ndarray, held: np.ndarray | None = None) -> dict:
    ev = dict(a)
    ann, auc = a.get("announcement_date"), a.get("auction_date")
    if not ann:
        ev["status"] = ST_NO_ANNOUNCEMENT
        return ev
    if ann >= auc:
        ev["status"] = ST_ANN_NOT_BEFORE
        return ev
    if len(dates) == 0 or auc > str(dates[-1]):
        ev["status"] = ST_BEYOND_DATA
        return ev
    A = int(np.searchsorted(dates, auc, side="left"))
    if str(dates[A]) != auc:
        ev["status"] = ST_NOT_SESSION
        return ev
    E, s_ann = pit_entry(dates, A, ann)
    if s_ann < 0:
        ev["status"] = ST_BEFORE_DATA
        return ev
    ev.update({"A": A, "E": E, "S_ann": s_ann, "entry_date": str(dates[E]) if E < len(dates) else None,
               "lead_calendar_days": (date.fromisoformat(auc) - date.fromisoformat(ann)).days,
               "lead_sessions": A - s_ann, "hold_sessions": A - E,
               "deferred_by_pit": E > A - HOLD_SESSIONS})
    if E >= A:
        ev["status"] = ST_NO_HOLD
        return ev
    rolls = 0
    if held is not None:
        h = np.asarray(held)[E:A + 1]
        rolls = int((h[1:] != h[:-1]).sum())
    ev.update({"rolls_in_window": rolls, "status": ST_OK})
    return ev


def build_events(auctions: list, calendars: dict) -> list:
    """Place every mapped-tenor auction on its market's own session calendar.
    ``calendars[market]`` needs ``dates`` (ISO strings, sorted) and ``held``."""
    out = []
    for a in auctions:
        if a.get("market") is None or a.get("window") is None:
            continue
        cal = calendars[a["market"]]
        out.append(place_event(a, np.asarray(cal["dates"]), cal.get("held")))
    return out


def clusters_of(events: list) -> list:
    """Section 6 aggregation: events whose return sessions (entry, auction]
    intersect are chained into ONE cluster; clusters never share a session."""
    order = sorted(range(len(events)), key=lambda i: (events[i]["entry_date"],
                                                     events[i]["auction_date"]))
    out, cur, cur_end = [], [], None
    for i in order:
        e = events[i]
        if cur and e["entry_date"] < cur_end:
            cur.append(i)
            cur_end = max(cur_end, e["auction_date"])
        else:
            if cur:
                out.append(cur)
            cur, cur_end = [i], e["auction_date"]
    if cur:
        out.append(cur)
    return out


# --------------------------------------------------------------------------- #
# The owned R38 substrate
# --------------------------------------------------------------------------- #
def layer_manifest() -> dict:
    try:
        return json.loads((N.NATIVE_LAYER_DIR / "layer_manifest.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def load_bars(markets=MARKETS) -> dict:
    """Front-contract returns and held contracts per market, read from the R38
    native contract layer. A missing market or history is a DATA_HOLD that names
    the exact gap - never a substitute series."""
    man = layer_manifest().get("markets") or {}
    bars, identity, gaps = {}, {}, []
    for m in markets:
        p = N.NATIVE_LAYER_DIR / ("%s.csv" % m)
        if not p.exists():
            gaps.append("R38 native contract layer has no %s market file (%s)" % (m, p))
            continue
        df = pd.read_csv(p, usecols=["Date", "ret", "held"], dtype={"Date": str, "held": str})
        df = df.sort_values("Date")
        dates = df["Date"].str[:10].to_numpy().astype("<U10")
        first = str(dates[0]) if len(dates) else None
        if first is None or first > QUALIFICATION[0]:
            gaps.append("%s history starts %s, after the qualification start %s"
                        % (m, first, QUALIFICATION[0]))
        bars[m] = {"dates": dates, "ret": df["ret"].to_numpy(dtype=float),
                   "held": df["held"].to_numpy()}
        identity[m] = {"rows": int(len(df)), "first": first,
                       "last": str(dates[-1]) if len(dates) else None,
                       "layer_manifest_sha256": (man.get(m) or {}).get("sha256")}
    if gaps:
        return {"state": V_DATA_HOLD, "why": "; ".join(gaps), "identity": identity}
    return {"state": "OK", "bars": bars, "identity": identity}


def load_costs(markets=MARKETS) -> dict:
    """R38 per-market cost per side (median over the ML panel, the R59 owner's
    rule); the gate charges max(kill-rule 1 bp, R38 cost)."""
    meta = N.load_meta()
    known = [v["cost_bps_per_side"] for v in meta.values()]
    worst = max(known) if known else 15.0
    r38, absent = {}, []
    for m in markets:
        if m in meta:
            r38[m] = float(meta[m]["cost_bps_per_side"])
        else:
            r38[m] = float(worst)
            absent.append(m)
    return {"source": str(N.R38_PANEL),
            "rule": "median cost_bps_per_side per market via alpha_agent.r59.native.load_meta; "
                    "a market absent from the panel is charged the panel maximum",
            "r38_bps_per_side": r38, "absent_from_panel": absent,
            "gate_bps_per_side": {m: max(KILL_RULE_COST_BPS, r38[m]) for m in markets}}


# --------------------------------------------------------------------------- #
# Census - the calendar only, never a return
# --------------------------------------------------------------------------- #
def _dist(vals) -> dict:
    v = [float(x) for x in vals if x is not None]
    if not v:
        return {"n": 0}
    a = np.asarray(v)
    return {"n": len(v), "min": float(a.min()), "p10": float(np.percentile(a, 10)),
            "median": float(np.median(a)), "p90": float(np.percentile(a, 90)), "max": float(a.max())}


def _count(vals) -> dict:
    out: dict = {}
    for v in vals:
        out[str(v)] = out.get(str(v), 0) + 1
    return dict(sorted(out.items()))


def census(auctions: list, events: list) -> dict:
    excl: dict = {}
    for a in auctions:
        w = a["window"] or "OUTSIDE_WINDOWS"
        key = a["exclusion"] or "MAPPED_%dY" % a["tenor"]
        excl.setdefault(w, {})
        excl[w][key] = excl[w].get(key, 0) + 1
    per = {}
    for w in WINDOWS:
        ew = [e for e in events if e["window"] == w]
        in_scope = [e for e in ew if e["status"] != ST_BEYOND_DATA]
        ok = [e for e in ew if e["status"] == ST_OK]
        tenors = {}
        for t, m in TENOR_MARKET.items():
            et = [e for e in ew if e["tenor"] == t]
            ot = [e for e in et if e["status"] == ST_OK]
            tenors["%dY_%s" % (t, m)] = {
                "auctions": len(et), "placed_ok": len(ot),
                "reopening_share": float(np.mean([e["reopening"] for e in et])) if et else None,
                "status": _count(e["status"] for e in et),
                "lead_calendar_days": _dist(e.get("lead_calendar_days") for e in et),
                "lead_sessions": _dist(e.get("lead_sessions") for e in et),
                "hold_sessions": _count(e["hold_sessions"] for e in ot),
                "deferred_by_pit_share": float(np.mean([e["deferred_by_pit"] for e in ot])) if ot else None,
                "roll_inside_window_share": float(np.mean([e["rolls_in_window"] > 0 for e in ot])) if ot else None}
        cl = clusters_of(ok) if ok else []
        n_scope = max(1, len(in_scope))
        per[w] = {"auctions_mapped": len(ew), "status": _count(e["status"] for e in ew),
                  "pit_missing_share": sum(e["status"] in PIT_FAILURES for e in in_scope) / n_scope,
                  "unusable_share": sum(e["status"] in UNUSABLE for e in in_scope) / n_scope,
                  "tenors": tenors, "clusters": len(cl),
                  "events_per_cluster": _count(len(c) for c in cl)}
    by_year: dict = {}
    for e in events:
        if e["status"] == ST_OK:
            y = by_year.setdefault(e["auction_date"][:4], {})
            y["%dY" % e["tenor"]] = y.get("%dY" % e["tenor"], 0) + 1
    return {"exclusions_by_window": excl, "windows": per,
            "placed_ok_by_year_and_tenor": dict(sorted(by_year.items())), "reads_returns": False}


# --------------------------------------------------------------------------- #
# Per-event P&L and the cluster book (read ONLY by evaluate)
# --------------------------------------------------------------------------- #
def dv01_scale(market: str) -> float:
    """Notional per unit of reference (ZN) DV01: a 2y short needs ~3.3x the ZN notional."""
    return APPROX_DURATION[REFERENCE_MARKET] / APPROX_DURATION[market]


def _compound(ret: np.ndarray, lo: int, hi: int):
    """Compound return over sessions lo+1 .. hi (close of lo to close of hi)."""
    w = np.asarray(ret[lo + 1:hi + 1], dtype=float)
    if len(w) != hi - lo or not np.isfinite(w).all():
        return None
    return float(np.prod(1.0 + w) - 1.0)


def returns_available(ev: dict, bars: dict) -> bool:
    r = bars[ev["market"]]["ret"]
    w = np.asarray(r[ev["S_ann"] + 1:ev["A"] + 1], dtype=float)
    return len(w) == ev["A"] - ev["S_ann"] and bool(np.isfinite(w).all())


def passive_mean(bars: dict, market: str, window: str):
    """Mean per-session DV01-scaled SHORT return of the always-short book of the
    same market over the same window (the section 9 passive control)."""
    lo, hi = WINDOWS[window]
    b = bars[market]
    sel = (b["dates"] >= lo) & (b["dates"] <= hi) & np.isfinite(b["ret"])
    if not sel.any():
        return None
    return float(np.mean(-b["ret"][sel] * dv01_scale(market)))


def _ladder_key(c: float) -> str:
    return "%gbp" % c


def event_pnl(ev: dict, bars: dict, gate_bps: float, mu) -> dict:
    b = bars[ev["market"]]
    k = dv01_scale(ev["market"])
    E, A, s = ev["E"], ev["A"], ev["S_ann"]
    hold = _compound(b["ret"], E, A)
    pre = _compound(b["ret"], s, E)
    end = A + POST_AUCTION_SESSIONS
    post = _compound(b["ret"], A, end) if end < len(b["ret"]) else None
    gross = -hold * k
    sides = 2 + 2 * int(ev.get("rolls_in_window") or 0)
    return {"gross": gross,
            "net_gate": gross - sides * gate_bps / 1e4 * k,
            "net_ladder": {_ladder_key(c): gross - sides * c / 1e4 * k for c in COST_LADDER_BPS},
            "pre_entry_gross": -pre * k,
            "post_auction_gross": None if post is None else -post * k,
            "excess_over_passive": None if mu is None else gross - ev["hold_sessions"] * mu}


def _st(x, lag: int = 0) -> dict:
    s = S.nw_tstat(np.asarray(list(x), dtype=float), lag=lag)
    return {"n": s["n"], "mean": s["mean"], "t": s["t"], "p_one_sided": s["p_one_sided"], "nw_lag": lag}


def window_years(bars: dict, window: str) -> float:
    lo, hi = WINDOWS[window]
    d = bars[REFERENCE_MARKET]["dates"]
    d = d[(d >= lo) & (d <= hi)]
    if len(d) < 2:
        return 0.0
    return ((date.fromisoformat(str(d[-1])) - date.fromisoformat(str(d[0]))).days + 1) / 365.25


def summarize(events: list, pnl: list, years: float) -> dict:
    """Cluster book (section 6): one observation per cluster = the SUM of its
    events' DV01-matched P&L (one unit per event, decided at its own entry).
    Clusters share no session, so the Newey-West lag is 0."""
    cl = clusters_of(events)

    def cs(key, sub=None):
        return np.array([sum(pnl[i][key] if sub is None else pnl[i][key][sub] for i in c)
                         for c in cl], dtype=float)

    net, gross, pre = cs("net_gate"), cs("gross"), cs("pre_entry_gross")
    has_passive = all(p["excess_over_passive"] is not None for p in pnl)
    exc = cs("excess_over_passive") if has_passive else None
    legs = {}
    for t, m in TENOR_MARKET.items():
        idx = [i for i, e in enumerate(events) if e["tenor"] == t]
        own = [events[i] for i in idx]
        overlap = bool(own) and len(clusters_of(own)) < len(own)
        lag = HOLD_SESSIONS - 1 if overlap else 0
        legs["%dY" % t] = {"market": m, "events": len(idx), "within_leg_overlap": overlap,
                           "gross": _st((pnl[i]["gross"] for i in idx), lag),
                           "net_gate": _st((pnl[i]["net_gate"] for i in idx), lag)}
    post = [p["post_auction_gross"] for p in pnl if p["post_auction_gross"] is not None]
    ann = (lambda v: float(v.sum() / years) if years > 0 else None)
    return {"events": len(events), "clusters": len(cl), "years": years,
            "clusters_per_year": (len(cl) / years) if years > 0 else None,
            "max_events_in_cluster": max((len(c) for c in cl), default=0),
            "net_gate": _st(net), "gross": _st(gross), "pre_entry_gross": _st(pre),
            "excess_over_passive": _st(exc) if exc is not None else None,
            "ann_net_gate": ann(net), "ann_gross": ann(gross),
            "ann_net_ladder": {_ladder_key(c): ann(cs("net_ladder", _ladder_key(c)))
                               for c in COST_LADDER_BPS},
            "max_dd_net_gate": S._max_dd(net),                                  # noqa: SLF001
            "hit_rate_net_gate": float((net > 0).mean()) if len(net) else None,
            "event_level_net_gate_descriptive": _st((p["net_gate"] for p in pnl), HOLD_SESSIONS - 1),
            "post_auction_gross_descriptive": _st(post, 0),
            "tenor_legs": legs}


# --------------------------------------------------------------------------- #
# Verdict - the preregistered gates, in the preregistered order (section 10)
# --------------------------------------------------------------------------- #
def _v(verdict: str, gate, why: str, kill=None) -> dict:
    return {"verdict": verdict, "gate": gate, "why": why, "kill_rule_fired": kill}


def _below(x, floor: float) -> bool:
    return x is None or not np.isfinite(float(x)) or float(x) < floor


def _r(x, n=2):
    return None if x is None else round(float(x), n)


def qualification_gate(g: dict):
    """Gates 1-10 on the 2000-2016 window. None means every gate passed."""
    if g.get("data_hold"):
        return _v(V_DATA_HOLD, "DATA", g["data_hold"])
    if g["pit_missing_share"] > MAX_PIT_MISSING_SHARE:
        return _v(V_PIT, "PIT", "%.1f%% of in-scope qualification auctions carry no announcementDate "
                                "strictly before the auction (limit %.0f%%)"
                  % (100 * g["pit_missing_share"], 100 * MAX_PIT_MISSING_SHARE), "PIT_UNESTABLISHED")
    legs = g["tenor_events"]
    if g["clusters"] < MIN_EFFECTIVE_PERIODS or min(legs.values()) < MIN_TENOR_EVENTS:
        return _v(V_SAMPLE, "SAMPLE", "%d non-overlapping clusters (floor %d); events per tenor %s "
                                      "(floor %d)" % (g["clusters"], MIN_EFFECTIVE_PERIODS, legs,
                                                      MIN_TENOR_EVENTS), "INSUFFICIENT_SAMPLE")
    if not _below(g["pre_entry_gross_t"], T_FLOOR) and _below(g["gross_t"], T_FLOOR):
        return _v(V_PRICED, "PRICED_BEFORE_ENTRY",
                  "the announcement-to-entry short earns t=%s but the tradable window only t=%s: the "
                  "concession is complete before a legitimate entry"
                  % (_r(g["pre_entry_gross_t"]), _r(g["gross_t"])),
                  "CONCESSION_COMPLETE_BETWEEN_ANNOUNCEMENT_AND_ENTRY")
    if g["gross_mean"] is None or g["gross_mean"] <= 0:
        return _v(V_WRONG_SIGN, "FROZEN_SIGN", "the pre-auction short loses before costs (mean %s per "
                                               "cluster); the sign is never reversed"
                  % _r(g["gross_mean"], 6), "SHORT_DOES_NOT_EARN")
    bad = sorted(k for k, m in g["tenor_gross_means"].items() if m is None or m <= 0)
    if bad:
        return _v(V_UNSTABLE, "TENOR_SIGN", "tenor legs %s do not earn in the frozen direction"
                  % bad, "TENOR_LEGS_DISAGREE_IN_SIGN")
    if _below(g["net_t"], T_FLOOR):
        return _v(V_NO_EDGE, "NW_T", "net cluster book NW t %s below %.1f at the gate cost"
                  % (_r(g["net_t"]), T_FLOOR), "NW_T_BELOW_2")
    if g["ann_net"] is None or g["ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "MATERIALITY", "net %.4f/yr below the %.3f floor at the gate cost"
                  % (g["ann_net"] or 0.0, MATERIALITY_ANN_NET), "NET_BELOW_1.5PCT_PER_YEAR")
    if _below(g["excess_t"], T_FLOOR):
        return _v(V_NONINCREMENTAL, "PASSIVE_SHORT_INCREMENT",
                  "increment over the always-short book of the same tenors has t %s below %.1f"
                  % (_r(g["excess_t"]), T_FLOOR), "NO_INCREMENT_OVER_PASSIVE_SHORT")
    if not g["fdr_family_pass"] or not g["fdr_inherited_pass"]:
        return _v(V_MULTIPLICITY, "MULTIPLICITY", "BH q=%.2f: family pass %s (m=1), inherited pass %s "
                                                  "(m=%d)" % (BH_Q, g["fdr_family_pass"],
                                                              g["fdr_inherited_pass"],
                                                              1 + len(INHERITED_NULLS)),
                  "BH_Q010_FAILS_M1_OR_INHERITED_M3")
    return None


def confirmation_gate(c: dict) -> dict:
    """Gates 11-15 on the untouched 2017-2026 window, reached only after 1-10."""
    if c.get("data_hold"):
        return _v(V_DATA_HOLD, "CONFIRMATION_DATA", c["data_hold"])
    if c.get("pit_missing_share", 0.0) > MAX_PIT_MISSING_SHARE:
        return _v(V_PIT, "CONFIRMATION_PIT", "%.1f%% of confirmation auctions lack a usable "
                                             "announcementDate" % (100 * c["pit_missing_share"]),
                  "PIT_UNESTABLISHED")
    if c["clusters"] < MIN_EFFECTIVE_PERIODS:
        return _v(V_NEED_MORE, "CONFIRMATION_SAMPLE", "%d confirmation clusters, floor %d"
                  % (c["clusters"], MIN_EFFECTIVE_PERIODS))
    if c["net_mean"] is None or c["net_mean"] <= 0:
        return _v(V_UNSTABLE, "CONFIRMATION_SIGN", "the confirmation net book loses (mean %s)"
                  % _r(c["net_mean"], 6), "CONFIRMATION_SIGN_REVERSED")
    if _below(c["lockbox_t"], T_FLOOR):
        return _v(V_NO_EDGE, "CONFIRMATION_NW_T", "confirmation net NW t %s below %.1f"
                  % (_r(c["lockbox_t"]), T_FLOOR), "CONFIRMATION_NW_T_BELOW_2")
    if c["ann_net"] is None or c["ann_net"] < MATERIALITY_ANN_NET:
        return _v(V_MATERIALITY, "CONFIRMATION_MATERIALITY", "confirmation net %.4f/yr below %.3f"
                  % (c["ann_net"] or 0.0, MATERIALITY_ANN_NET), "CONFIRMATION_NET_BELOW_1.5PCT_PER_YEAR")
    return _v(V_QUALIFIED, None, "every preregistered qualification and confirmation gate passed; "
                                 "capital eligibility remains a forward-evidence and human decision")


def verdict_for(g: dict, confirmation: dict | None = None) -> dict:
    v = qualification_gate(g)
    if v is not None:
        return v
    if confirmation is None:
        return _v(V_NEED_MORE, "CONFIRMATION_NOT_EVALUATED",
                  "qualification passed but the confirmation window was not evaluated")
    return confirmation_gate(confirmation)


def multiplicity(p) -> dict:
    fam = S.bh_fdr({MECHANISM_ID: p}, q=BH_Q)
    inh = S.bh_fdr(dict({MECHANISM_ID: p}, **{n: 1.0 for n in INHERITED_NULLS}), q=BH_Q)
    return {"m_declared": 1, "q": BH_Q, "p_value": p,
            "p_value_definition": "one-sided Newey-West p of the qualification net cluster book "
                                  "at the gate cost",
            "benjamini_hochberg_family": fam, "family_pass": MECHANISM_ID in fam["survivors"],
            "inherited_nulls": list(INHERITED_NULLS), "inherited_p_value_policy": "p = 1 each",
            "m_inherited": 1 + len(INHERITED_NULLS), "benjamini_hochberg_inherited": inh,
            "inherited_pass": MECHANISM_ID in inh["survivors"], "denominator_reset": False}


def gate_inputs(q: dict, cen_w: dict, mult: dict) -> dict:
    legs = q["tenor_legs"]
    return {"pit_missing_share": cen_w["pit_missing_share"], "clusters": q["clusters"],
            "tenor_events": {k: v["events"] for k, v in legs.items()},
            "pre_entry_gross_t": q["pre_entry_gross"]["t"], "gross_t": q["gross"]["t"],
            "gross_mean": q["gross"]["mean"],
            "tenor_gross_means": {k: v["gross"]["mean"] for k, v in legs.items()},
            "net_t": q["net_gate"]["t"], "ann_net": q["ann_net_gate"],
            "excess_t": (q.get("excess_over_passive") or {}).get("t"),
            "fdr_family_pass": mult["family_pass"], "fdr_inherited_pass": mult["inherited_pass"]}


# --------------------------------------------------------------------------- #
# Evaluation and the executor contract
# --------------------------------------------------------------------------- #
def artifact_path() -> str:
    return str(research_root() / "results" / ARTIFACT_NAME)


def _result(v: dict, statistic: dict, economics: dict, mult: dict, identity: dict) -> dict:
    return {"verdict": v["verdict"], "why": v["why"], "kill_rule_fired": v.get("kill_rule_fired"),
            "statistic": statistic, "economics": economics, "multiplicity": mult,
            "artifact": artifact_path(), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": stable_hash(identity or {})}


def hold_result(why: str, identity: dict | None = None) -> dict:
    mult = {"m_declared": 1, "m_inherited": 1 + len(INHERITED_NULLS), "q": BH_Q,
            "inherited_nulls": list(INHERITED_NULLS)}
    return _result(_v(V_DATA_HOLD, "DATA", why), {"lockbox_t": None}, {}, mult, identity or {})


def evaluate(records: list, bars: dict, costs: dict, *, identity: dict | None = None) -> dict:
    identity = dict(identity or {})
    identity["cost_bps_per_side_r38"] = costs["r38_bps_per_side"]
    auctions = auction_table(records)
    cal = {m: {"dates": b["dates"], "held": b["held"]} for m, b in bars.items()}
    events = build_events(auctions, cal)
    for e in events:
        if e["status"] == ST_OK and not returns_available(e, bars):
            e["status"] = ST_MISSING_RETURN
    cen = census(auctions, events)
    gate_bps = costs["gate_bps_per_side"]

    def measure(window: str):
        cw = cen["windows"][window]
        if cw["unusable_share"] > MAX_UNUSABLE_SHARE:
            return None, ("%s: %.1f%% of in-scope auctions cannot be placed on the owned futures "
                          "sessions or lack a return (limit %.0f%%); statuses %s"
                          % (window, 100 * cw["unusable_share"], 100 * MAX_UNUSABLE_SHARE, cw["status"]))
        ok = [e for e in events if e["window"] == window and e["status"] == ST_OK]
        mus = {m: passive_mean(bars, m, window) for m in bars}
        pnl = [event_pnl(e, bars, gate_bps[e["market"]], mus[e["market"]]) for e in ok]
        out = summarize(ok, pnl, window_years(bars, window))
        out["passive_short_mean_per_session"] = mus
        return out, None

    q, hold = measure("QUALIFICATION")
    if hold:
        return {"census": cen, "executor_result": hold_result(hold, identity),
                "qualification": None, "confirmation": "UNTOUCHED"}
    mult = multiplicity(q["net_gate"]["p_one_sided"])
    g = gate_inputs(q, cen["windows"]["QUALIFICATION"], mult)
    v = qualification_gate(g)
    conf, cg = None, None
    if v is None:
        conf, hold = measure("CONFIRMATION")
        cg = ({"data_hold": hold} if hold else
              {"clusters": conf["clusters"], "net_mean": conf["net_gate"]["mean"],
               "lockbox_t": conf["net_gate"]["t"], "ann_net": conf["ann_net_gate"],
               "pit_missing_share": cen["windows"]["CONFIRMATION"]["pit_missing_share"]})
        v = confirmation_gate(cg)
    legs = q["tenor_legs"]
    statistic = {
        "lockbox_t": (conf or {}).get("net_gate", {}).get("t") if conf else None,
        "nw_lag_cluster_book": 0,
        "qualification_net_t": q["net_gate"]["t"], "qualification_net_p_one_sided": q["net_gate"]["p_one_sided"],
        "qualification_gross_t": q["gross"]["t"], "qualification_gross_mean_per_cluster": q["gross"]["mean"],
        "pre_entry_gross_t": q["pre_entry_gross"]["t"],
        "excess_over_passive_t": g["excess_t"],
        "tenor_leg_gross_t": {k: x["gross"]["t"] for k, x in legs.items()},
        "tenor_leg_gross_mean": {k: x["gross"]["mean"] for k, x in legs.items()},
        "qualification_clusters": q["clusters"], "qualification_events": q["events"],
        "confirmation_clusters": conf["clusters"] if conf else None,
        "confirmation_events": conf["events"] if conf else None}
    economics = {
        "units": "fraction of one unit of ZN-equivalent DV01 notional per auction, summed per year",
        "cost_bps_per_side_r38": costs["r38_bps_per_side"], "cost_bps_per_side_gate": gate_bps,
        "qualification_ann_net_gate": q["ann_net_gate"], "qualification_ann_gross": q["ann_gross"],
        "qualification_ann_net_ladder": q["ann_net_ladder"],
        "qualification_max_dd_net_gate": q["max_dd_net_gate"],
        "qualification_hit_rate_net_gate": q["hit_rate_net_gate"],
        "confirmation_ann_net_gate": conf["ann_net_gate"] if conf else None,
        "confirmation_ann_net_ladder": conf["ann_net_ladder"] if conf else None,
        "confirmation_max_dd_net_gate": conf["max_dd_net_gate"] if conf else None}
    return {"census": cen, "gate_inputs": g, "confirmation_gate_inputs": cg,
            "qualification": q, "confirmation": conf if conf else "UNTOUCHED",
            "executor_result": _result(v, statistic, economics, mult, identity)}


def design() -> dict:
    return {"tenor_to_market": {"%dY" % t: m for t, m in TENOR_MARKET.items()},
            "excluded_tenors": {"%dY" % t: why for t, why in EXCLUDED_TENORS.items()},
            "excluded_securities": ["TIPS", "FRN", "bills and CMBs (not fetched)"],
            "approx_futures_modified_duration": dict(APPROX_DURATION),
            "dv01_reference_market": REFERENCE_MARKET, "hold_sessions_max": HOLD_SESSIONS,
            "pit_rule": "entry session E = max(A-5, first futures session strictly after "
                        "announcementDate); exit at the auction-day close A; E >= A is dropped",
            "aggregation": "events whose (entry, auction] sessions intersect form one cluster; "
                           "cluster P&L = sum of member events; Newey-West lag 0",
            "windows": {k: list(v) for k, v in WINDOWS.items()},
            "kill_rule_cost_bps_per_side": KILL_RULE_COST_BPS,
            "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "roll_rule": "a held-contract change inside (E, A] costs one extra round trip",
            "session_calendar": "each market's own R38 bar dates (no engine import)"}


def run(*, verbose: bool = True, write: bool = True) -> dict:
    body = {"schema": "alpha_recovery_treasury_auction_concession/1",
            "calculation_owner": CALCULATION_OWNER, "mechanism_id": MECHANISM_ID,
            "preregistration": PREREGISTRATION, "preregistration_altered_after_results": False,
            "kill_rule_frozen": KILL_RULE_FROZEN, "design": design(), "scorer": SCORER,
            "capital_eligible": False}
    identity: dict = {}
    auc = load_auctions()
    identity["auction_cache_sha256"] = (auc.get("manifest") or {}).get("sha256")
    if auc["state"] != "OK":
        detail = {"executor_result": hold_result(auc["why"], identity)}
    else:
        bl = load_bars()
        identity["r38_layer"] = bl.get("identity")
        if bl["state"] != "OK":
            detail = {"executor_result": hold_result(bl["why"], identity)}
        else:
            try:
                costs = load_costs()
            except (OSError, ValueError, KeyError) as e:
                detail = {"executor_result": hold_result("R38 cost panel unreadable: %r" % (e,), identity)}
            else:
                detail = evaluate(auc["records"], bl["bars"], costs, identity=identity)
                body["costs"] = costs
        body["auction_manifest"] = {k: (auc.get("manifest") or {}).get(k)
                                    for k in ("source", "rows", "sha256", "fetched_at_utc")}
    body.update(detail)
    res = body["executor_result"]
    body["verdict"], body["why"] = res["verdict"], res["why"]
    if write:
        write_artifact(ARTIFACT_NAME, body)
    if verbose:
        print("%s %s: %s" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses a different mechanism or a
    kill rule that no longer matches the preregistered one."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    rule = ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE")
    if rule != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor "
                         "does not judge a changed contract")
    return dict(run(verbose=False, write=True)["executor_result"])
