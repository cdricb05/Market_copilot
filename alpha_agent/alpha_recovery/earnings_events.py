"""alpha_agent.alpha_recovery.earnings_events - EARNINGS_EVENT_REACTION from owned SEC data.

Workstream 4/5: earnings event timing and surprises, built from information the
estate already OWNS and had never joined at the event horizon:

    * SEC submissions histories (R63 acquisition): every 8-K with its
      acceptance timestamp for the 842 PANEL-F issuers, 2009-2026;
    * the SEC companyfacts store: NetIncomeLoss facts with real ``filed``
      dates on 10-Q, 10-K and (since XBRL earnings releases) 8-K forms;
    * the R57 PIT price panel and SPY for the reaction window.

Economic hypothesis (protocol section earnings_event_reaction): the market
under-reacts to earnings news; the announcement-window abnormal return and
the standardised earnings surprise predict the cross-section of subsequent
returns (post-earnings-announcement drift). The incumbent's fundamental leg
reads TTM levels weeks after the release and its momentum leg skips the last
21 sessions, so neither carries this information at the event horizon.

Point in time, exactly:
    ear                 known at the close of event + 1 (a market observable)
    sue                 known at the fact's own ``filed`` date, + 1 session
                        (the R63 SEC broadcast lag); the 8-K XBRL release
                        date when one exists, else the 10-Q / 10-K
    sessions_since      known at the event session
No current data is inserted into history; every value is placed on the grid
at its own availability instant and carried for a bounded window.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd

from alpha_agent.r63 import SEC_FACTS_DB, SEC_FILING_BROADCAST_LAG_SESSIONS, features as FE, pit
from alpha_agent.r63 import panels as P

CALCULATION_OWNER = "alpha_agent.alpha_recovery.earnings_events"
BLOCK = "EARNINGS_EVENT_REACTION"
BLOCK_EAR_ONLY = "EARNINGS_REACTION_ONLY"
BLOCK_SUE_ONLY = "EARNINGS_SURPRISE_ONLY"

TAG = "NetIncomeLoss"
Q_MIN, Q_MAX = 80, 100            # a fiscal quarter
YTD9_MIN, YTD9_MAX = 260, 290     # nine months
ANN_MIN, ANN_MAX = 350, 380       # a fiscal year
EVENT_MIN_DAYS = 10               # an earnings 8-K comes at least this long after period end
EVENT_MAX_DAYS = 75               # and at most this long
CARRY_SESSIONS = 63               # the drift window the feature is carried for
SINCE_CAP = 126
SUE_WINDOW, SUE_MIN = 8, 4
PRE_MARKET_CUTOFF = (9, 30)       # acceptance at/before this -> same-session event

_CACHE: dict = {}


def _ro(path) -> sqlite3.Connection:
    return sqlite3.connect("file:%s?immutable=1" % str(path).replace("\\", "/"), uri=True)


def _norm_cik(c) -> str:
    return str(c).strip().upper().replace("CIK", "").lstrip("0") or "0"


def _days(a: str, b: str) -> int | None:
    try:
        return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days
    except (TypeError, ValueError):
        return None


# --------------------------------------------------------------------------- #
# Facts: quarterly net income with availability, Q4 derived, 8-K release dates
# --------------------------------------------------------------------------- #
def load_ni_facts(ciks: set) -> pd.DataFrame:
    """NetIncomeLoss duration facts for the wanted CIKs: cik, ps, pe, filed,
    form, value, duration. Read only."""
    if "ni" in _CACHE:
        return _CACHE["ni"]
    con = _ro(SEC_FACTS_DB)
    rows = []
    q = ("SELECT cik, period_start, period_end, filed, form, value FROM cf_fact "
         "WHERE concept_tag = ? AND period_end >= '2008-06-01' AND period_start IS NOT NULL "
         "AND period_start <> ''")
    for cik, ps, pe, filed, form, val in con.execute(q, (TAG,)):
        c = _norm_cik(cik)
        if c not in ciks or val is None or not filed:
            continue
        d = _days(ps, pe)
        if d is None:
            continue
        rows.append((c, ps[:10], pe[:10], filed[:10], str(form or ""), float(val), d))
    con.close()
    df = pd.DataFrame(rows, columns=["cik", "ps", "pe", "filed", "form", "value", "dur"])
    _CACHE["ni"] = df
    return df


def quarterly_series(df_cik: pd.DataFrame) -> pd.DataFrame:
    """Per fiscal quarter end: the FIRST-FILED quarterly net income, the date
    it became available, and whether an 8-K carried it. Q4 is derived as
    annual minus the nine-month YTD sharing the annual's period start."""
    g = df_cik.sort_values(["filed", "pe"])
    quarters = {}
    for r in g.itertuples(index=False):
        if Q_MIN <= r.dur <= Q_MAX and r.pe not in quarters:
            quarters[r.pe] = {"pe": r.pe, "value": r.value, "available": r.filed,
                              "via_8k": r.form.startswith("8-K"), "source": "QUARTERLY_FACT"}
    ann = g[(g["dur"] >= ANN_MIN) & (g["dur"] <= ANN_MAX)]
    ytd9 = g[(g["dur"] >= YTD9_MIN) & (g["dur"] <= YTD9_MAX)]
    first_ytd = {}
    for r in ytd9.itertuples(index=False):
        first_ytd.setdefault((r.ps, r.pe), (r.value, r.filed))
    for r in ann.itertuples(index=False):
        if r.pe in quarters:
            continue
        cands = [(pe, v) for (ps, pe), v in first_ytd.items() if ps == r.ps and pe < r.pe]
        if not cands:
            continue
        pe9, (v9, f9) = max(cands, key=lambda x: x[0])
        gap = _days(pe9, r.pe)
        if gap is None or not (75 <= gap <= 110):
            continue
        quarters[r.pe] = {"pe": r.pe, "value": r.value - v9, "available": max(r.filed, f9),
                          "via_8k": r.form.startswith("8-K"), "source": "ANNUAL_MINUS_YTD9"}
    out = pd.DataFrame(sorted(quarters.values(), key=lambda q: q["pe"]))
    return out


def sue_series(q: pd.DataFrame) -> pd.DataFrame:
    """Standardised unexpected earnings per quarter, as-of its availability."""
    if len(q) == 0:
        return q.assign(sue=np.nan)
    pes = [date.fromisoformat(p) for p in q["pe"]]
    vals = q["value"].to_numpy()
    diffs = np.full(len(q), np.nan)
    for i, pe in enumerate(pes):
        target = pe - timedelta(days=365)
        best, bestd = None, None
        for j in range(i):
            dd = abs((pes[j] - target).days)
            if dd <= 20 and (bestd is None or dd < bestd):
                best, bestd = j, dd
        if best is not None:
            diffs[i] = vals[i] - vals[best]
    sue = np.full(len(q), np.nan)
    for i in range(len(q)):
        hist = diffs[max(0, i - SUE_WINDOW):i]
        hist = hist[np.isfinite(hist)]
        if np.isfinite(diffs[i]) and len(hist) >= SUE_MIN:
            sd = float(np.std(hist, ddof=1))
            if sd > 0:
                sue[i] = float(diffs[i] / sd)
    return q.assign(seasonal_diff=diffs, sue=sue)


def announcement_dates(q: pd.DataFrame, eightk: pd.DataFrame, periodic_filed: dict) -> list:
    """Per quarter: (pe, ann_date, acceptance, source). XBRL 8-K release date
    when the quarterly fact itself came on an 8-K; else the earliest 8-K in
    [pe + 10d, min(periodic filed, pe + 75d)]."""
    out = []
    k8_dates = eightk["filing_date"].to_numpy() if len(eightk) else np.array([], dtype="datetime64[ns]")
    k8_acc = eightk["acceptance"].to_numpy() if len(eightk) else np.array([], dtype=object)
    for r in q.itertuples(index=False):
        pe = date.fromisoformat(r.pe)
        if bool(r.via_8k):
            ad = r.available
            acc = None
            if len(k8_dates):
                m = np.where(k8_dates == np.datetime64(ad))[0]
                if len(m):
                    acc = k8_acc[m[0]]
            out.append({"pe": r.pe, "ann_date": ad, "acceptance": acc, "source": "8K_XBRL"})
            continue
        lo = pe + timedelta(days=EVENT_MIN_DAYS)
        hi = pe + timedelta(days=EVENT_MAX_DAYS)
        pf = periodic_filed.get(r.pe)
        if pf is not None:
            hi = min(hi, date.fromisoformat(pf))
        if len(k8_dates) == 0:
            continue
        m = np.where((k8_dates >= np.datetime64(lo)) & (k8_dates <= np.datetime64(hi)))[0]
        if not len(m):
            continue
        i = m[np.argmin(k8_dates[m])]
        out.append({"pe": r.pe, "ann_date": str(k8_dates[i])[:10], "acceptance": k8_acc[i],
                    "source": "8K_RULE"})
    return out


def _event_session_index(dates: np.ndarray, ann_date: str, acceptance) -> int | None:
    """The first session that could react: on the filing date when accepted
    at/before 09:30 (as stamped), else the first session strictly after."""
    pre_market = False
    if isinstance(acceptance, str) and "T" in acceptance:
        try:
            hh, mm = int(acceptance[11:13]), int(acceptance[14:16])
            pre_market = (hh, mm) <= PRE_MARKET_CUTOFF
        except ValueError:
            pre_market = False
    side = "left" if pre_market else "right"
    i = int(np.searchsorted(dates, ann_date, side=side))
    return i if i < len(dates) else None


# --------------------------------------------------------------------------- #
# The block
# --------------------------------------------------------------------------- #
def build_blocks(E: dict, *, verbose: bool = False) -> dict:
    """EARNINGS_EVENT_REACTION (ear, sue, sessions_since), the two single-
    feature decompositions, and a coverage report. Cached per process."""
    key = ("blocks", id(E))
    if key in _CACHE:
        return _CACHE[key]
    dates = E["dates"]
    n_sym, n_d = E["price"]["tr"].shape
    tr = E["price"]["tr"]
    spy = E["price"]["spy_tr"]
    ciks = set(E["sym2cik"].values())
    ni = load_ni_facts(ciks)
    sub = P.load_submissions()
    sub = sub[sub["form"].astype(str).str.startswith("8-K")] if len(sub) else sub
    k8_by_cik = {c: g.sort_values("filing_date") for c, g in sub.groupby("cik")} if len(sub) else {}
    ni_by_cik = {c: g for c, g in ni.groupby("cik")} if len(ni) else {}
    ear = np.full((n_sym, n_d), np.nan)
    sue = np.full((n_sym, n_d), np.nan)
    since = np.full((n_sym, n_d), np.nan)
    n_events = n_xbrl = n_rule = n_sue = 0
    n_syms = 0
    for si, sym in enumerate(E["symbols"]):
        cik = E["sym2cik"].get(sym)
        g = ni_by_cik.get(cik) if cik else None
        if g is None or len(g) == 0:
            continue
        n_syms += 1
        q = sue_series(quarterly_series(g))
        if len(q) == 0:
            continue
        periodic = {}
        for r in g[g["form"].isin(["10-Q", "10-K"])].sort_values("filed").itertuples(index=False):
            periodic.setdefault(r.pe, r.filed)
        k8 = k8_by_cik.get(cik)
        if k8 is None:
            k8 = pd.DataFrame(columns=["filing_date", "acceptance"])
        anns = announcement_dates(q, k8, periodic)
        # ear: reaction window [e-1, e+1], known at close e+1
        ev_ix = []
        for a in anns:
            e = _event_session_index(dates, a["ann_date"], a["acceptance"])
            if e is None or e < 2 or e + 1 >= n_d:
                continue
            p0, p1 = tr[si, e - 2], tr[si, e + 1]
            s0, s1 = spy[e - 2], spy[e + 1]
            if not (np.isfinite(p0) and np.isfinite(p1) and p0 > 0 and p1 > 0 and s0 > 0 and s1 > 0):
                continue
            val = float(np.log(p1 / p0) - np.log(s1 / s0))
            a0, a1 = e + 1, min(n_d, e + 1 + CARRY_SESSIONS)
            ear[si, a0:a1] = val
            since[si, e:min(n_d, e + SINCE_CAP)] = np.arange(0, min(n_d, e + SINCE_CAP) - e)
            ev_ix.append(e)
            n_events += 1
            if a["source"] == "8K_XBRL":
                n_xbrl += 1
            else:
                n_rule += 1
        # sue: at its own availability (filed) + broadcast lag, carried 63 sessions
        for r in q.itertuples(index=False):
            if not np.isfinite(r.sue):
                continue
            i = int(np.searchsorted(dates, r.available, side="left")) + SEC_FILING_BROADCAST_LAG_SESSIONS
            if i >= n_d:
                continue
            sue[si, i:min(n_d, i + CARRY_SESSIONS)] = float(r.sue)
            n_sue += 1
        if verbose and si % 200 == 0:
            print("[earnings] %d/%d %s events=%d" % (si, n_sym, sym, len(ev_ix)), flush=True)
    since = np.minimum(since, SINCE_CAP)
    cov_mask = np.isfinite(ear)
    report = {"symbols_with_facts": n_syms, "events": n_events, "events_via_8k_xbrl": n_xbrl,
              "events_via_8k_rule": n_rule, "sue_values": n_sue,
              "share_symbol_sessions_with_ear_2012_on": float(
                  cov_mask[:, np.searchsorted(dates, "2012-01-01"):].mean()),
              "pit": {"ear": "market observable at close event+1", "sue": "fact filed date + 1 session",
                      "sessions_since": "event session"},
              "carry_sessions": CARRY_SESSIONS}
    out = {BLOCK: FE._stack(ear, sue, since), BLOCK_EAR_ONLY: FE._stack(ear),
           BLOCK_SUE_ONLY: FE._stack(sue), "_report": report}
    _CACHE[key] = out
    return out
