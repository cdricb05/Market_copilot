"""alpha_agent.alpha_recovery.ftd_fails_data - the SEC Fails-to-Deliver stream,
point-in-time by PUBLICATION, for the short-sale-pressure axis.

One source, published by the SEC itself, free, no key, no account, no licence,
ALREADY acquired by :mod:`ownership_data` (409 semi-monthly files, July 2009 -
August 2026). Nothing here spends money and nothing here downloads.

THIS MODULE COMPUTES NO RETURN AND READS NO PRICE OR VOLUME. It turns the fails
files into a per-security, per-half-month balance and states when each half-month
became public. The volume denominator and every return live elsewhere, so "the
signal was defined before any return existed" is a property of the code.

FACTS THIS MODULE ENCODES, each of which silently corrupts the signal if assumed

1. **A file row is a BALANCE, not a flow.** The SEC: "the aggregate net balance of
   shares that failed to be delivered as of a particular settlement date" - new
   fails plus old fails less settled fails. Since 2008-09-16 every non-zero balance
   is published, so a security-date ABSENT from a file is an observed ZERO.

2. **The settlement date is not the publication date.** The SEC posts the first
   half of month M "at the end of the month" and the second half "at about the 15th
   of the next month", and "cannot guarantee that the data will be posted by a
   particular date". Measured Last-Modified: 2024-02a on 03-01, 2026-01a on
   **02-05**, second halves on the 15th-17th. Every file before 2021 carries the
   bulk re-upload stamp 2020-12-19, so historical posting dates are UNOBSERVABLE.
   The availability rule below is therefore a declared, conservative bound with
   slack over every measured posting, applied in exactly one place.

3. **Identity is the owned CUSIP-anchored bridge** (:mod:`ownership_identity`): the
   FTD archive states CUSIP and SYMBOL in one record, and the CUSIP -> panel row
   correspondence is bookkeeping that retains delisted securities. No name match.

RESEARCH ONLY. No purchase, no subscription, no promotion, no capital, no
proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import calendar
import io
import re
import zipfile
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from . import now_iso, research_root
from . import ownership_data as OD
from . import ownership_identity as OI

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ftd_fails_data"

#: Since this settlement date every non-zero balance is published (the SEC's
#: 10,000-share floor applies only before it).
ALL_NONZERO_BALANCES_FROM = "2008-09-16"

#: DECLARED availability. The first half of month M is usable only at a session
#: STRICTLY AFTER day 10 of month M+1 (latest measured posting: day 5); the second
#: half only strictly after day 25 of month M+1 (latest measured: day 17).
FIRST_HALF_USABLE_AFTER_DAY = 10
SECOND_HALF_USABLE_AFTER_DAY = 25

_NAME = re.compile(r"cnsfails(\d{4})(\d{2})([ab])\.zip$", re.I)


def derived_path() -> Path:
    return research_root() / "_derived" / "ftd_fails_by_half_month.npz"


def half_month_window(year: int, month: int, half: str) -> tuple:
    """(first calendar day, last calendar day) a file covers."""
    if half == "a":
        return date(year, month, 1), date(year, month, 15)
    return date(year, month, 16), date(year, month, calendar.monthrange(year, month)[1])


def usable_after(year: int, month: int, half: str) -> date:
    """The calendar day AFTER which the file may be read. A decision session
    dated on or before this day may not see it."""
    ny, nm = (year + 1, 1) if month == 12 else (year, month + 1)
    return date(ny, nm, FIRST_HALF_USABLE_AFTER_DAY if half == "a"
                else SECOND_HALF_USABLE_AFTER_DAY)


def ftd_files() -> list:
    out = []
    for p in sorted(OD.ftd_dir().glob("cnsfails*.zip")):
        m = _NAME.search(p.name)
        if not m:
            continue
        y, mo, half = int(m.group(1)), int(m.group(2)), m.group(3).lower()
        lo, hi = half_month_window(y, mo, half)
        out.append({"name": p.name, "path": p, "period": "%04d-%02d%s" % (y, mo, half),
                    "first_day": lo.isoformat(), "last_day": hi.isoformat(),
                    "usable_after": usable_after(y, mo, half).isoformat()})
    return out


def parse_file(path: Path) -> tuple:
    """(rows: DataFrame[cusip, settlement, quantity], settlement dates in file).

    The settlement-date set is taken from the WHOLE file, so a security absent on
    a date the file covers is a zero balance on that date, not a missing one."""
    zf = zipfile.ZipFile(path)
    with zf.open(zf.namelist()[0]) as fh:
        df = pd.read_csv(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"), sep="|",
                         usecols=[0, 1, 3], names=["settlement", "cusip", "quantity"],
                         header=0, dtype=str, on_bad_lines="skip")
    df["settlement"] = pd.to_datetime(df["settlement"].str.strip(), format="%Y%m%d",
                                      errors="coerce")
    df = df.dropna(subset=["settlement", "cusip"])
    df["cusip"] = df["cusip"].map(OI.cusip_key)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df.dropna(subset=["cusip", "quantity"])
    df = df[df["quantity"] >= 0]
    return df, sorted(df["settlement"].unique())


def half_month_balances(cusip_to_row: dict, n_rows: int, *, verbose: bool = True) -> dict:
    """For every file: per panel row, the MEAN daily fails balance over the file's
    settlement dates (absent = 0) and the number of dates with a non-zero balance.

    Rows are NaN where the row has no bridged CUSIP - unassessable, never zero.
    Several CUSIPs on one row are summed (one security, several lines)."""
    identified = np.zeros(n_rows, dtype=bool)
    identified[sorted(set(int(r) for r in cusip_to_row.values()))] = True
    files = ftd_files()
    bal = np.full((len(files), n_rows), np.nan)
    nz = np.full((len(files), n_rows), np.nan)
    ndays = np.zeros(len(files), dtype=int)
    for k, f in enumerate(files):
        df, sdays = parse_file(f["path"])
        ndays[k] = len(sdays)
        bal[k, identified] = 0.0
        nz[k, identified] = 0.0
        if not len(sdays):
            continue
        df["row"] = df["cusip"].map(cusip_to_row)
        df = df.dropna(subset=["row"])
        if len(df):
            df["row"] = df["row"].astype(int)
            s = df.groupby("row")["quantity"].sum() / float(len(sdays))
            c = df[df["quantity"] > 0].groupby("row")["settlement"].nunique()
            bal[k, s.index.to_numpy()] = s.to_numpy()
            nz[k, c.index.to_numpy()] = c.to_numpy()
        if verbose and (k + 1) % 50 == 0:
            print("  FTD half-months %d/%d" % (k + 1, len(files)), flush=True)
    return {"files": [{kk: v for kk, v in f.items() if kk != "path"} for f in files],
            "mean_balance": bal, "nonzero_days": nz, "settlement_days": ndays,
            "identified": identified, "built_at": now_iso(),
            "calculation_owner": CALCULATION_OWNER}


def visible_file_index(dates, files: list) -> np.ndarray:
    """For each session, the index of the file covering the LATEST half-month
    among those whose ``usable_after`` day is STRICTLY before the session's
    date; -1 when none is visible yet. Only files already usable are consulted."""
    d64 = np.asarray(pd.to_datetime(np.asarray(dates)).values, dtype="datetime64[D]")
    out = np.full(len(d64), -1, dtype=int)
    if not files:
        return out
    ua = np.asarray([np.datetime64(f["usable_after"], "D") for f in files])
    end = np.asarray([np.datetime64(f["last_day"], "D") for f in files])
    order = np.argsort(ua, kind="stable")
    running, run_end, j = -1, None, 0
    for t in range(len(d64)):
        while j < len(order) and ua[order[j]] < d64[t]:
            k = int(order[j])
            if run_end is None or end[k] >= run_end:
                running, run_end = k, end[k]
            j += 1
        out[t] = running
    return out


def save_balances(bal: dict, path: Path | None = None) -> Path:
    import json
    p = Path(path or derived_path())
    p.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(p, mean_balance=bal["mean_balance"], nonzero_days=bal["nonzero_days"],
                        settlement_days=bal["settlement_days"], identified=bal["identified"],
                        meta=np.frombuffer(json.dumps({"files": bal["files"],
                                                       "built_at": bal["built_at"]}).encode("utf-8"),
                                           dtype=np.uint8))
    return p


def load_or_build_balances(cusip_to_row: dict, n_rows: int, *, rebuild: bool = False,
                           verbose: bool = True) -> dict:
    """The per-file balances, cached. The cache is refused - and rebuilt from the
    stored FTD files - whenever its identity layer or file list no longer
    matches, so a stale cache can never stand in for the archive."""
    import json
    p = derived_path()
    ident = np.zeros(n_rows, dtype=bool)
    ident[sorted(set(int(r) for r in cusip_to_row.values()))] = True
    names = [f["name"] for f in ftd_files()]
    if p.exists() and not rebuild:
        z = np.load(p)
        meta = json.loads(bytes(z["meta"]).decode("utf-8"))
        if (z["identified"].shape == ident.shape and bool((z["identified"] == ident).all())
                and [f["name"] for f in meta["files"]] == names):
            return {"files": meta["files"], "mean_balance": z["mean_balance"],
                    "nonzero_days": z["nonzero_days"], "settlement_days": z["settlement_days"],
                    "identified": z["identified"], "built_at": meta["built_at"],
                    "calculation_owner": CALCULATION_OWNER, "from_cache": True}
    bal = half_month_balances(cusip_to_row, n_rows, verbose=verbose)
    save_balances(bal)
    bal["from_cache"] = False
    return bal
