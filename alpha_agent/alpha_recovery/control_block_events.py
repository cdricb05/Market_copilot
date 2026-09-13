"""alpha_agent.alpha_recovery.control_block_events - point-in-time Schedule
13D/G events on the owned survivorship-safe equity substrate.

Executes sections 2 to 5 of
``research/preregistration/CONTROL_BLOCK_13DG_PREREGISTRATION.md`` (frozen at
commit 1d3f491, BEFORE any return existed) and changes nothing in it.

THIS MODULE COMPUTES NO RETURN. It turns a stream of filings into dated,
identified, de-duplicated events and reports how much of the universe it could
see. Keeping returns out of it is what makes "the events were defined before the
returns were looked at" a property of the code rather than a claim.

The two frozen cells:

    CELL A  NEW CONTROL BLOCK      an INITIAL Schedule 13D or 13G. Rule 13d-1
                                   requires one when a person FIRST crosses 5%,
                                   so this is the legal definition of a new
                                   block rather than a proxy for one.
    CELL B  MATERIAL BLOCK INCREASE the same reporting person's block rises by
                                   >= 1.00 percentage point. Rule 13d-2(a)
                                   deems 1% material for exactly this quantity,
                                   so the threshold is taken from law and not
                                   from a search over returns.

RESEARCH ONLY. No purchase, no promotion, no capital, no proposal, no order, no
fill, no backfill, no live write.
"""
from __future__ import annotations

import json
import re

import numpy as np

from alpha_agent.r63 import acquire as ACQ

from . import now_iso
from . import control_block_data as CBD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.control_block_events"

CELL_A = "NEW_CONTROL_BLOCK"
CELL_B = "MATERIAL_BLOCK_INCREASE"
CELLS = (CELL_A, CELL_B)

#: FROZEN by the preregistration. Rule 13d-2(a)'s own materiality standard.
MATERIAL_INCREASE_PP = 1.00

#: A US session closes at 16:00 Eastern. Information carried by a filing
#: accepted at or after that instant cannot be in that session's close.
SESSION_CLOSE_ET_HOUR = 16.0

#: The panel's spelling for a security that no longer trades.
_DELISTED = re.compile(r"^(.*)-(\d{6})$")

SEC_TICKER_URL = "https://www.sec.gov/files/company_tickers.json"


# --------------------------------------------------------------------------- #
# Identity: subject-issuer CIK -> panel row
# --------------------------------------------------------------------------- #
def _ticker_variants(sym: str) -> list:
    """Spellings of ONE ticker. The panel writes a class share ``BF.B`` and the
    SEC writes ``BF-B``; comparing them unnormalised makes every dual-class name
    silently unbridgeable - the same punctuation trap the 13F axis hit."""
    s = str(sym).upper()
    out = [s]
    if "." in s:
        out += [s.replace(".", "-"), s.replace(".", "")]
    return out


def identity_map(E: dict, *, use_sec_ticker_map: bool = True) -> tuple:
    """(cik -> [panel rows], meta).

    Two authoritative routes and no third: the owned R58 bridge, then SEC's
    ``company_tickers.json``, which states the ticker and the CIK in ONE record
    and is therefore not a name match. The SEC route is applied ONLY to
    still-listed rows, where the panel and the SEC both spell the security by
    its CURRENT ticker - a delisted row's ticker is not in a current map, and
    guessing one would be exactly the fuzzy matching this axis forbids.
    """
    syms = [str(s) for s in np.asarray(E["symbols"])]
    sym2cik = dict(E.get("sym2cik") or {})
    n_owned = len(sym2cik)
    added, disagreed = 0, 0
    if use_sec_ticker_map:
        try:
            headers = ACQ._headers(ACQ.contact_email() or "")
            headers["Accept"] = "*/*"
            status, body = ACQ._get(SEC_TICKER_URL, headers)
            by_ticker: dict = {}
            if status == 200 and body:
                for rec in json.loads(body.decode("utf-8")).values():
                    t = str(rec.get("ticker") or "").strip().upper()
                    c = str(rec.get("cik_str") or "").lstrip("0")
                    if t and c:
                        by_ticker.setdefault(t, set()).add(c)
            for s in syms:
                if _DELISTED.match(s):
                    continue
                cands: set = set()
                for v in _ticker_variants(s):
                    cands |= by_ticker.get(v, set())
                if len(cands) != 1:
                    continue
                c = next(iter(cands))
                if s in sym2cik:
                    disagreed += int(sym2cik[s] != c)
                else:
                    sym2cik[s] = c
                    added += 1
        except Exception:                                    # noqa: BLE001
            pass
    cik2rows: dict = {}
    for i, s in enumerate(syms):
        c = sym2cik.get(s)
        if c:
            cik2rows.setdefault(str(c), []).append(i)
    multi = {c: r for c, r in cik2rows.items() if len(r) > 1}
    meta = {
        "route_owned_bridge": n_owned,
        "route_sec_ticker_map_added": added,
        "route_sec_ticker_map_disagreements": disagreed,
        "issuer_name_matching_used": False,
        "panel_rows": len(syms),
        "panel_rows_identified": sum(len(v) for v in cik2rows.values()),
        "distinct_ciks": len(cik2rows),
        "ciks_mapping_to_multiple_rows": len(multi),
        "rows_under_multi_row_ciks": sum(len(v) for v in multi.values()),
    }
    return cik2rows, meta


# --------------------------------------------------------------------------- #
# The decision session
# --------------------------------------------------------------------------- #
def decision_index(dates64: np.ndarray, acceptance_utc: str) -> int | None:
    """The FIRST session whose 16:00 Eastern close is STRICTLY after acceptance.

    A filing accepted at 14:00 on a session is in that session's close; one
    accepted at 19:00 is not, and waits for the next. A filing accepted on a
    non-session day waits for the next session. Returns None when the stamp is
    unreadable or the event falls outside the panel - never a guessed date.
    """
    et = CBD.acceptance_et(acceptance_utc)
    if et is None:
        return None
    day = np.datetime64(et.date(), "D")
    if (et.hour + et.minute / 60.0) >= SESSION_CLOSE_ET_HOUR:
        day = day + np.timedelta64(1, "D")
    pos = int(np.searchsorted(dates64, day.astype("datetime64[ns]"), side="left"))
    return pos if pos < len(dates64) else None


# --------------------------------------------------------------------------- #
# Cells
# --------------------------------------------------------------------------- #
def _block_percent(rec: dict) -> float | None:
    """A filing's reported block.

    A joint filing repeats the cover page once per reporting person, so several
    percents are normal. The MAXIMUM is taken: the group's aggregate is at least
    the largest single page, and summing overlapping holdings would double-count
    the same shares. Frozen before results.
    """
    p = [float(x) for x in (rec.get("percents") or []) if 0.0 <= float(x) <= 100.0]
    return max(p) if p else None


def build_events(E: dict, elig: np.ndarray, *, verbose: bool = True,
                 recs: list | None = None) -> dict:
    """Both frozen cells, point-in-time, with the coverage report the
    preregistration requires. No forward return is touched.

    ``recs`` is injectable so the construction can be exercised on a known
    stream in a test, and so Cell A - which needs no document at all - can be
    built before the cover-page acquisition has finished.
    """
    dates64 = np.asarray(E["dates"], dtype="datetime64[ns]")
    cik2rows, id_meta = identity_map(E)
    recs = list(recs) if recs is not None else CBD.parsed_records(verbose=verbose)
    recs.sort(key=lambda r: (str(r.get("acceptance_utc") or ""), r.get("accession") or ""))
    issuer_ciks = set(cik2rows)
    filer_map = CBD.load_filer_map(issuer_ciks)

    stats = {
        "schedules": len(recs), "with_percent": 0, "without_percent": 0,
        "unidentified_issuer": 0, "outside_panel": 0, "unreadable_acceptance": 0,
        "filer_identified": 0, "filer_unidentified": 0,
        "cell_b_first_observation_no_prior": 0,
        "cell_b_decrease_or_flat": 0,
    }
    ev_a: dict = {}
    ev_b: dict = {}
    state: dict = {}
    for r in recs:
        cik = str(r.get("issuer_cik") or "")
        rows = cik2rows.get(cik)
        t = decision_index(dates64, r.get("acceptance_utc") or "")
        if t is None:
            stats["unreadable_acceptance"] += 1
            continue
        pct = _block_percent(r)
        stats["with_percent" if pct is not None else "without_percent"] += 1
        acc = CBD._norm_accession(r.get("accession"))
        filer = filer_map.get(acc)
        if filer is None and r.get("reporting_person_ciks"):
            filer = sorted(r["reporting_person_ciks"])[0]
        stats["filer_identified" if filer else "filer_unidentified"] += 1

        # Cell B state advances on EVERY readable filing, whether or not it
        # triggers, because a decrease still tells us where the block now is.
        delta = None
        if pct is not None and filer:
            key = (cik, str(filer))
            prev = state.get(key)
            state[key] = pct
            if prev is None:
                stats["cell_b_first_observation_no_prior"] += 1
            else:
                delta = pct - prev
                if delta < MATERIAL_INCREASE_PP:
                    stats["cell_b_decrease_or_flat"] += 1

        if not rows:
            stats["unidentified_issuer"] += 1
            continue
        for i in rows:
            if not bool(elig[i, t]):
                continue
            if r.get("form") in ("13D", "13G"):
                ev_a.setdefault((i, t), {
                    "row": int(i), "t": int(t), "cell": CELL_A,
                    "date": str(dates64[t])[:10], "form": r["form"],
                    "issuer_cik": cik, "accession": r.get("accession"),
                    "filing_date": r.get("filing_date"),
                    "acceptance_utc": r.get("acceptance_utc"),
                    "percent": pct, "is_13d": r["form"].startswith("13D"),
                    "n_collapsed": 0})["n_collapsed"] += 1
            if delta is not None and delta >= MATERIAL_INCREASE_PP:
                prior = ev_b.get((i, t))
                if prior is None or delta > prior["delta"]:
                    ev_b[(i, t)] = {
                        "row": int(i), "t": int(t), "cell": CELL_B,
                        "date": str(dates64[t])[:10], "form": r["form"],
                        "issuer_cik": cik, "filer_cik": str(filer),
                        "accession": r.get("accession"),
                        "acceptance_utc": r.get("acceptance_utc"),
                        "percent": pct, "delta": float(delta),
                        "is_13d": str(r["form"]).startswith("13D"),
                        "n_collapsed": 0}
                ev_b[(i, t)]["n_collapsed"] += 1

    events = {CELL_A: sorted(ev_a.values(), key=lambda e: (e["t"], e["row"])),
              CELL_B: sorted(ev_b.values(), key=lambda e: (e["t"], e["row"]))}
    if verbose:
        for c in CELLS:
            n = len(events[c])
            print("  %s: %d events, %s -> %s" % (
                c, n, events[c][0]["date"] if n else None,
                events[c][-1]["date"] if n else None), flush=True)
    return {"events": events, "identity": id_meta, "stats": stats,
            "cik2rows": cik2rows, "generated_at": now_iso(),
            "calculation_owner": CALCULATION_OWNER}


# --------------------------------------------------------------------------- #
# Coverage: what share of the universe could carry an event at all
# --------------------------------------------------------------------------- #
def coverage_report(E: dict, elig: np.ndarray, cik2rows: dict,
                    dec: np.ndarray) -> dict:
    """The assessable share of the eligible cross-section at each decision
    session, and the SURVIVORSHIP DIRECTION of whatever is not assessable.

    The share alone is not the risk. A Schedule 13D is often the first public
    step of a control contest that ends in acquisition, and an acquired company
    leaves the panel - so if the unassessable names are the ones about to leave,
    the experiment is blind exactly where the hypothesis expects its payoff.
    """
    syms = np.asarray(E["symbols"])
    n_i = len(syms)
    identified = np.zeros(n_i, dtype=bool)
    for rows in cik2rows.values():
        for i in rows:
            identified[i] = True
    last_elig = {}
    for i in range(n_i):
        w = np.where(elig[i])[0]
        if len(w):
            last_elig[i] = int(w[-1])
    end_ix = elig.shape[1] - 1
    per, tot_i = [], {"n": 0, "exit": 0}
    tot_u = {"n": 0, "exit": 0}
    for t in np.asarray(dec, dtype=int):
        u = elig[:, t]
        n = int(u.sum())
        if not n:
            continue
        per.append({"date": str(np.asarray(E["dates"])[t])[:10],
                    "universe": n,
                    "assessable": int((u & identified).sum()),
                    "coverage": float((u & identified).sum()) / n})
        for i in np.where(u)[0]:
            leaves = (last_elig.get(i, end_ix) - t <= 252) and last_elig.get(i, end_ix) < end_ix - 5
            d = tot_i if identified[i] else tot_u
            d["n"] += 1
            d["exit"] += int(leaves)
    cov = np.array([p["coverage"] for p in per]) if per else np.array([1.0])
    return {
        "n_decision_sessions": len(per),
        "coverage_mean": float(cov.mean()), "coverage_min": float(cov.min()),
        "n_uncovered_sessions": int((cov < 0.95).sum()),
        "uncovered_share": float((cov < 0.95).mean()),
        "min_date_coverage_rule": 0.95, "max_uncovered_share_rule": 0.20,
        "exit_rate_identified": tot_i["exit"] / max(tot_i["n"], 1),
        "exit_rate_unidentified": tot_u["exit"] / max(tot_u["n"], 1),
        "share_of_exits_unidentified":
            tot_u["exit"] / max(tot_u["exit"] + tot_i["exit"], 1),
        "per_session": per[:400],
    }
