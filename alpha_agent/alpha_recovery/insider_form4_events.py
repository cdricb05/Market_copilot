"""alpha_agent.alpha_recovery.insider_form4_events - point-in-time CLUSTERED
open-market insider buying events on the owned survivorship-safe equity
substrate.

Executes the event definition of
``research/preregistration/INSIDER_FORM4_PREREGISTRATION.md`` and changes
nothing in it.

THIS MODULE COMPUTES NO RETURN. It turns the purchase stream into dated,
identified, de-duplicated events and reports what it dropped and why. Keeping
returns out of it is what makes "the events were defined before the returns were
looked at" a property of the code rather than a claim.

THE ONE FROZEN CELL

    CLUSTERED_OPEN_MARKET_INSIDER_BUYING
        at the availability instant of a qualifying purchase filing, at least
        TWO distinct officers or directors of the same issuer have qualifying
        purchase filings that became public in the trailing 30 calendar days
        AND whose purchases were themselves made inside those 30 days; the
        issuer has had no event of this cell in the preceding 30 days.
        Direction POSITIVE, frozen.

A QUALIFYING PURCHASE is a non-derivative transaction coded ``P`` on an ORIGINAL
Form 4 or Form 5, marked Acquired, for a positive number of shares of the
issuer's COMMON equity, not an equity swap, not on a filing that declares Rule
10b5-1 plan reliance, reported by at least one officer or director, whose
transaction date is not after publication and not more than 30 days before it.

RESEARCH ONLY. No purchase, no promotion, no capital, no proposal, no order, no
fill, no backfill, no live write.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import date, timedelta

import numpy as np

from alpha_agent.r63 import EQUITY_DISCOVERY_START

from . import now_iso
from . import control_block_events as CBE
from . import insider_form4_data as D

CALCULATION_OWNER = "alpha_agent.alpha_recovery.insider_form4_events"

SIGN_POSITIVE = 1

CELL_CLUSTER = "CLUSTERED_OPEN_MARKET_INSIDER_BUYING"

#: FROZEN. "Multiple distinct insiders" read literally: more than one.
CLUSTER_MIN_DISTINCT_INSIDERS = 2
#: FROZEN. One canonical grid period (21 sessions) in calendar days. The same
#: number bounds the membership window, the staleness of a purchase and the
#: refractory period, so the cell has exactly one time parameter.
CLUSTER_WINDOW_DAYS = 30

CELL_SPECS = {
    CELL_CLUSTER: {
        "sign": SIGN_POSITIVE,
        "min_distinct_insiders": CLUSTER_MIN_DISTINCT_INSIDERS,
        "window_calendar_days": CLUSTER_WINDOW_DAYS,
        "refractory_calendar_days": CLUSTER_WINDOW_DAYS,
        "reporter_roles": ("DIRECTOR", "OFFICER"),
        "transaction_code": D.PURCHASE_CODE,
        "document_types": ("4", "5"),
    },
}
CELLS = tuple(CELL_SPECS)

QUALIFYING_DOCUMENT_TYPES = ("4", "5")

#: Events are counted from the canonical equity discovery start - the SAME
#: window both measurement arms see. Filings before it build cluster state only.
EVENT_WINDOW_START = EQUITY_DISCOVERY_START

#: Exclusion reasons, in the order they are tested. The first that applies is
#: the one recorded, so the funnel adds up.
X_FORM = "NOT_ORIGINAL_FORM_4_OR_5"
X_FLAG = "NOT_MARKED_ACQUIRED"
X_SHARES = "NO_POSITIVE_SHARES"
X_SWAP = "EQUITY_SWAP"
X_TITLE = "NOT_COMMON_EQUITY"
X_PLAN = "RULE_10B5_1_PLAN_DECLARED"
X_ROLE = "NO_OFFICER_OR_DIRECTOR_REPORTER"
X_INSTANT = "NO_AVAILABILITY_INSTANT"
X_TDATE = "TRANSACTION_DATE_MISSING_OR_AFTER_PUBLICATION"
X_STALE = "PURCHASE_OLDER_THAN_WINDOW_AT_PUBLICATION"
EXCLUSION_ORDER = (X_FORM, X_FLAG, X_SHARES, X_SWAP, X_TITLE, X_PLAN, X_ROLE, X_INSTANT,
                   X_TDATE, X_STALE)


def officer_director_ciks(row: dict) -> list:
    """Reporting owners who are an officer or a director of the issuer. A ten-
    per-cent holder who is neither is a portfolio, not a manager."""
    return sorted({o["cik"] for o in (row.get("owners") or [])
                   if o.get("cik") and (o.get("director") or o.get("officer"))})


def exclusion_reason(row: dict) -> str | None:
    """None when the purchase row qualifies; otherwise the FIRST failing rule."""
    if row.get("document_type") not in QUALIFYING_DOCUMENT_TYPES:
        return X_FORM
    if row.get("acquired_disposed") != "A":
        return X_FLAG
    if not (row.get("shares") or 0.0) > 0.0:
        return X_SHARES
    if row.get("equity_swap") is True:
        return X_SWAP
    if row.get("title_class") != "COMMON":
        return X_TITLE
    if row.get("rule_10b5_1") is True:
        return X_PLAN
    if not officer_director_ciks(row):
        return X_ROLE
    inst = D.availability_instant(row)
    if inst is None:
        return X_INSTANT
    td = row.get("trans_date")
    if not td or date.fromisoformat(td) > inst.date():
        return X_TDATE
    if (inst.date() - date.fromisoformat(td)).days > CLUSTER_WINDOW_DAYS:
        return X_STALE
    return None


def qualifying_filings(rows: list) -> tuple:
    """(one record per (issuer, accession) holding >= 1 qualifying row, funnel).

    A filing is ONE insider act however many rows it has (split fills, a direct
    and an indirect line). Its insider is the lowest officer/director CIK on it,
    so a joint filing by a director and the director's fund is one insider, not
    two - otherwise one person could form a "cluster" alone.
    """
    funnel: Counter = Counter()
    by_key: dict = {}
    for r in rows:
        why = exclusion_reason(r)
        funnel[why or "QUALIFIES"] += 1
        if why:
            continue
        key = (r["issuer_cik"], r["accession"])
        rec = by_key.get(key)
        if rec is None:
            by_key[key] = {"issuer_cik": r["issuer_cik"], "accession": r["accession"],
                           "insider_id": officer_director_ciks(r)[0],
                           "instant": D.availability_instant(r),
                           "acceptance_utc": r.get("acceptance_utc") or "",
                           "acceptance_source": r.get("acceptance_source"),
                           "filing_date": r.get("filing_date"),
                           "last_trans_date": r["trans_date"], "rows": 1}
        else:
            rec["last_trans_date"] = max(rec["last_trans_date"], r["trans_date"])
            rec["rows"] += 1
    out = sorted(by_key.values(), key=lambda f: (f["instant"], f["issuer_cik"], f["accession"]))
    return out, funnel


def cluster_completions(filings: list) -> list:
    """The completing filings of ONE issuer, ``filings`` sorted by instant.

    At filing j (instant a): the members are the filings k <= j accepted in
    (a - 30 days, a] whose latest qualifying purchase is dated after
    date(a) - 30 days. An event fires when the members name at least two
    distinct insiders and the issuer has had no event in the 30 days before a.
    Only filings already public at a are ever consulted.
    """
    W = timedelta(days=CLUSTER_WINDOW_DAYS)
    out, last = [], None
    lo_k = 0
    for j, f in enumerate(filings):
        a = f["instant"]
        while lo_k < j and filings[lo_k]["instant"] <= a - W:
            lo_k += 1
        lo_date = a.date() - W
        members: dict = {}
        for g in filings[lo_k:j + 1]:
            if date.fromisoformat(g["last_trans_date"]) > lo_date:
                members.setdefault(g["insider_id"], g["accession"])
        if len(members) >= CLUSTER_MIN_DISTINCT_INSIDERS and (last is None or a - last >= W):
            out.append({**f, "distinct_insiders": len(members),
                        "member_accessions": sorted(members.values())})
            last = a
    return out


def build_events(E: dict, elig: np.ndarray, *, rows: list | None = None,
                 cik2rows: dict | None = None, verbose: bool = True) -> dict:
    """The frozen cell, point-in-time, with the drop accounting the
    preregistration requires. No forward return is touched.

    ``rows`` and ``cik2rows`` are injectable so the construction can be
    exercised on a known stream in a test without a network call.
    """
    rows = D.load_stream() if rows is None else list(rows)
    if cik2rows is None:
        cik2rows, id_meta = CBE.identity_map(E)
    else:
        id_meta = {"injected": True}
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    filings, funnel = qualifying_filings(rows)
    by_issuer: dict = defaultdict(list)
    for f in filings:
        by_issuer[f["issuer_cik"]].append(f)
    completing = []
    for fs in by_issuer.values():
        completing += cluster_completions(fs)
    st = Counter()
    by_key: dict = {}
    for c in completing:
        t = D.decision_session(dates64, c)
        if t is None:
            st["outside_calendar"] += 1
            continue
        if str(dates64[t])[:10] < EVENT_WINDOW_START:
            st["before_event_window"] += 1
            continue
        prow = cik2rows.get(c["issuer_cik"]) or []
        if not prow:
            st["unidentified_issuer"] += 1
            continue
        erows = [i for i in prow if bool(elig[i, t])]
        if not erows:
            st["not_eligible_at_decision_session"] += 1
            continue
        st["admitted_completions"] += 1
        for i in erows:
            e = by_key.setdefault((int(i), int(t)), {
                "row": int(i), "t": int(t), "cell": CELL_CLUSTER,
                "date": str(dates64[t])[:10], "issuer_cik": c["issuer_cik"],
                "accessions": [], "distinct_insiders": c["distinct_insiders"],
                "acceptance_utc": c["acceptance_utc"],
                "acceptance_source": c["acceptance_source"],
                "sign": SIGN_POSITIVE, "is_13d": False, "n_collapsed": 0})
            e["accessions"].append(c["accession"])
            e["n_collapsed"] += 1
    ev = sorted(by_key.values(), key=lambda e: (e["t"], e["row"]))
    stats = {"purchase_rows": len(rows), "funnel": {k: funnel.get(k, 0) for k in
                                                    EXCLUSION_ORDER + ("QUALIFIES",)},
             "qualifying_filings": len(filings), "qualifying_issuers": len(by_issuer),
             "qualifying_insiders": len({f["insider_id"] for f in filings}),
             "cluster_completions_all_dates": len(completing), **dict(st),
             "events": len(ev), "collapsed_same_row_session": sum(e["n_collapsed"] - 1 for e in ev)}
    if verbose:
        print("  %s: %d events, %s -> %s %s" % (
            CELL_CLUSTER, len(ev), ev[0]["date"] if ev else None,
            ev[-1]["date"] if ev else None, stats), flush=True)
    return {"events": {CELL_CLUSTER: ev}, "stats": stats, "identity": id_meta,
            "cik2rows": cik2rows, "generated_at": now_iso(),
            "calculation_owner": CALCULATION_OWNER}
