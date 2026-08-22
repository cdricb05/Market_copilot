"""alpha_agent.r35.information - the ONE Release 35 normalisation owner.

Raw third-party payloads in, point-in-time normalised series out. Every series
this module returns carries an ``observable_at`` index: the moment the value was
PUBLIC, not the moment it describes. That distinction is the whole point of the
module and is where every family here could have cheated:

* a Commitments of Traders row is stamped with a Tuesday and was released the
  following Friday, so it is indexed at the release, never at the Tuesday;
* an OECD monthly interbank rate for month M is published in arrears, so it is
  indexed two months on;
* an insider transaction has a TRANSACTION date, which was private, and a
  FILING date, which is when the world could see it. Only the filing date is an
  index here, and ``contract.INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE`` is
  False so that no later reader can quietly change its mind;
* an EIA futures settlement is a dated CONTRACT price. It is a curve because
  contract 1 and contract 4 are different instruments on the same day - never
  because a spot series was differenced against itself.

``as_of_align`` is the single function that projects any of these onto the
panel's session calendar. It carries the declared broadcast lag, it uses a
strict ``observable_at <= session`` rule, and no module outside this one is
allowed to align anything, so there is exactly one place a look-ahead could be
introduced and exactly one place to check.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import pit_sector as _pit_sector
from . import acquisition as _acq
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r35.information"
COVERAGE_SCHEMA = "r35_information_coverage/1"
ARTIFACT_NAME = "information_coverage.json"

#: Derived caches live beside the raw payloads. They are a speed device, never
#: evidence: every one of them is reproducible from the raw bytes the
#: acquisition manifest hashes.
CACHE_DIRNAME = "_derived"


def cache_path(name: str) -> Path:
    from .. import r35
    path = r35.acquisition_root() / CACHE_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path / name


# --------------------------------------------------------------------------- #
# The one alignment rule
# --------------------------------------------------------------------------- #
def as_of_align(values: pd.Series, calendar: pd.DatetimeIndex, *,
                lag_sessions: int = _contract.BROADCAST_LAG_SESSIONS
                ) -> pd.Series:
    """Project an ``observable_at``-indexed series onto the session calendar.

    For each session the result carries the most recent value that was already
    public, and is then shifted by ``lag_sessions`` further sessions. The shift
    is conservative rather than necessary - it is R33's uniform treatment of
    global state, it costs one session, and it removes any argument about
    whether a value published on the close of day D could have been acted on at
    the close of day D.
    """
    if values is None or len(values) == 0:
        return pd.Series(np.nan, index=calendar, dtype=float)
    series = pd.Series(values, dtype=float).dropna()
    series = series[~series.index.duplicated(keep="last")].sort_index()
    if series.empty:
        return pd.Series(np.nan, index=calendar, dtype=float)
    union = series.index.union(calendar)
    aligned = series.reindex(union).ffill().reindex(calendar)
    if lag_sessions:
        aligned = aligned.shift(int(lag_sessions))
    return aligned.astype(float)


def _coverage(series: pd.Series) -> dict:
    values = pd.Series(series, dtype=float)
    present = values.notna()
    first = values.index[present.argmax()] if present.any() else None
    last = values.index[len(values) - 1 - present.values[::-1].argmax()] \
        if present.any() else None
    return {"observations": int(present.sum()),
            "first": str(first)[:10] if first is not None else None,
            "last": str(last)[:10] if last is not None else None}


# --------------------------------------------------------------------------- #
# CFTC Commitments of Traders
# --------------------------------------------------------------------------- #
COT_COLUMNS = {
    "as_of": "As of Date in Form YYYY-MM-DD",
    "code": "CFTC Contract Market Code",
    "open_interest": "Open Interest (All)",
    "nc_long": "Noncommercial Positions-Long (All)",
    "nc_short": "Noncommercial Positions-Short (All)",
    "comm_long": "Commercial Positions-Long (All)",
    "comm_short": "Commercial Positions-Short (All)",
}


def _to_float(text) -> float:
    try:
        cleaned = str(text).strip().replace(",", "")
        return float(cleaned) if cleaned not in ("", ".", "-") else np.nan
    except (TypeError, ValueError):
        return np.nan


def load_cot(files: dict, *, codes=None) -> dict:
    """Weekly per-contract-code positioning, indexed by REPORT date.

    The publication lag is applied by :func:`cot_observable`, not here, so the
    raw report date stays visible in the cache and a reader can see both.
    """
    wanted = set(codes or [c for spec in _contract.COT_MAPPING.values()
                           for c in spec[0]])
    excluded = set(_contract.COT_EXCLUDED_CODES)
    wanted -= excluded
    rows = []
    years_read = []
    for year in sorted(files, key=lambda y: int(y)):
        path = Path(files[year])
        try:
            archive = zipfile.ZipFile(path)
        except (OSError, zipfile.BadZipFile):
            continue
        member = archive.namelist()[0]
        text = archive.read(member).decode("latin-1")
        reader = csv.DictReader(io.StringIO(text))
        header = {(name or "").strip(): name for name in
                  (reader.fieldnames or [])}
        picked = {key: header.get(label) for key, label in COT_COLUMNS.items()}
        if not all(picked.values()):
            continue
        years_read.append(int(year))
        for record in reader:
            code = (record.get(picked["code"]) or "").strip()
            if code not in wanted:
                continue
            rows.append({
                "as_of": (record.get(picked["as_of"]) or "").strip(),
                "code": code,
                "open_interest": _to_float(record.get(picked["open_interest"])),
                "nc_long": _to_float(record.get(picked["nc_long"])),
                "nc_short": _to_float(record.get(picked["nc_short"])),
                "comm_long": _to_float(record.get(picked["comm_long"])),
                "comm_short": _to_float(record.get(picked["comm_short"])),
            })
    if not rows:
        return {"ok": False, "reason": "NO_COT_ROWS_PARSED", "frame": None,
                "years_read": years_read}
    frame = pd.DataFrame(rows)
    frame["as_of"] = pd.to_datetime(frame["as_of"], errors="coerce")
    frame = frame.dropna(subset=["as_of"]).sort_values(["code", "as_of"])
    return {"ok": True, "frame": frame, "years_read": sorted(years_read),
            "codes_present": sorted(frame["code"].unique()),
            "rows": int(len(frame)),
            "first": str(frame["as_of"].min())[:10],
            "last": str(frame["as_of"].max())[:10]}


def cot_instrument_series(frame: pd.DataFrame, codes, *,
                          lag_days: int = _contract.COT_PUBLICATION_LAG_DAYS
                          ) -> pd.DataFrame:
    """Aggregate one instrument's mapped contract codes into a weekly series.

    Positions are SUMMED across codes at each report date, which is what carries
    a market through the big -> e-mini -> micro migration without anyone
    choosing which contract counts. The index is the PUBLICATION date.
    """
    subset = frame[frame["code"].isin(list(codes))]
    if subset.empty:
        return pd.DataFrame(columns=["spec_net_oi", "open_interest"])
    grouped = subset.groupby("as_of").agg(
        open_interest=("open_interest", "sum"),
        nc_long=("nc_long", "sum"),
        nc_short=("nc_short", "sum"))
    grouped = grouped[grouped["open_interest"] > 0]
    out = pd.DataFrame(index=grouped.index)
    out["spec_net_oi"] = ((grouped["nc_long"] - grouped["nc_short"])
                          / grouped["open_interest"])
    out["open_interest"] = grouped["open_interest"]
    out.index = out.index + pd.Timedelta(days=int(lag_days))
    return out.sort_index()


# --------------------------------------------------------------------------- #
# FRED
# --------------------------------------------------------------------------- #
def load_fred(files: dict, *, monthly_ids=None, lag_months=None) -> dict:
    """FRED observation payloads into ``observable_at``-indexed series.

    Daily market observables (constant-maturity yields, TIPS yields, breakevens,
    Moody's yields) are observable on their own date. The OECD MONTHLY interbank
    rates are published in arrears and are therefore stamped forward by
    ``OECD_RATE_PUBLICATION_LAG_MONTHS``; a month-M rate carrying a month-M
    index would be a two-month look-ahead repeated 300 times.

    ``monthly_ids`` and ``lag_months`` let a LATER release declare its own
    published-in-arrears set and per-series lag without a second FRED parser.
    Both default to Release 35's, so an existing caller is unaffected: Release
    36 reads twenty short rates and twenty-one consumer price indices, and a
    quarterly price index needs a longer lag than a monthly interbank rate.
    """
    if monthly_ids is None:
        monthly_ids = set(_contract.FRED_FOREIGN_SHORT_RATES.values())
        monthly_ids.add(_contract.FRED_US_SHORT_RATE)
    else:
        monthly_ids = set(monthly_ids)
    lag_months = dict(lag_months or {})
    series, meta = {}, {}
    for sid, path in sorted(files.items()):
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            meta[sid] = {"ok": False, "reason": "UNREADABLE"}
            continue
        observations = payload.get("observations") or []
        dates, values = [], []
        for record in observations:
            value = record.get("value")
            if value in (None, ".", ""):
                continue
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                continue
            dates.append(record.get("date"))
        if not values:
            meta[sid] = {"ok": False, "reason": "NO_NUMERIC_OBSERVATIONS"}
            continue
        index = pd.to_datetime(pd.Index(dates), errors="coerce")
        line = pd.Series(values, index=index, dtype=float).dropna()
        if sid in monthly_ids:
            months = int(lag_months.get(
                sid, _contract.OECD_RATE_PUBLICATION_LAG_MONTHS))
            shifted = line.index + pd.DateOffset(months=months)
            line = pd.Series(line.values, index=shifted, dtype=float)
            cadence = "MONTHLY_PUBLISHED_IN_ARREARS"
        else:
            months = 0
            cadence = "DAILY_MARKET_OBSERVABLE"
        series[sid] = line.sort_index()
        meta[sid] = {"ok": True, "cadence": cadence,
                     "publication_lag_months": months,
                     "observations": int(len(line)),
                     "first": str(line.index.min())[:10],
                     "last": str(line.index.max())[:10]}
    return {"ok": bool(series), "series": series, "meta": meta}


# --------------------------------------------------------------------------- #
# CBOE
# --------------------------------------------------------------------------- #
def load_cboe(files: dict) -> dict:
    """VIX and VIX3M daily closes, observable on their own session."""
    series, meta = {}, {}
    for name, path in sorted(files.items()):
        try:
            text = Path(path).read_text(encoding="utf-8", errors="replace")
        except OSError:
            meta[name] = {"ok": False, "reason": "UNREADABLE"}
            continue
        frame = pd.read_csv(io.StringIO(text))
        columns = {str(c).strip().upper(): c for c in frame.columns}
        date_col = columns.get("DATE")
        close_col = columns.get("CLOSE")
        if date_col is None or close_col is None:
            meta[name] = {"ok": False, "reason": "UNEXPECTED_COLUMNS"}
            continue
        index = pd.to_datetime(frame[date_col], errors="coerce")
        line = pd.Series(pd.to_numeric(frame[close_col], errors="coerce").values,
                         index=index, dtype=float).dropna()
        line = line[line > 0].sort_index()
        series[name] = line
        meta[name] = {"ok": True, "observations": int(len(line)),
                      "first": str(line.index.min())[:10],
                      "last": str(line.index.max())[:10]}
    return {"ok": len(series) >= 2, "series": series, "meta": meta}


# --------------------------------------------------------------------------- #
# EIA petroleum futures curve
# --------------------------------------------------------------------------- #
def load_eia_curve(path, *, series_ids=_contract.EIA_WTI_CONTRACTS,
                   cache_name: str = "eia_wti_curve.csv") -> dict:
    """Dated NYMEX settlement prices for contracts 1..4.

    These are four DIFFERENT contracts quoted on the same day. That is what
    makes the resulting basis a curve and not a lagged transformation of one
    price, which is the substitution ``contract.PROHIBITED_SUBSTITUTIONS``
    forbids.

    ``cache_name`` exists because the derived cache is keyed by FILE and a
    second market read through this function would otherwise read, fail to
    match, and then OVERWRITE the first market's cache with its own columns.
    Release 36 reads five curves and passes a distinct name for each.
    """
    wanted = set(series_ids)
    cached = cache_path(cache_name)
    if cached.exists():
        frame = pd.read_csv(cached, index_col=0, parse_dates=True)
        if set(frame.columns) >= wanted:
            return {"ok": True, "frame": frame[sorted(wanted)],
                    "from_cache": True,
                    "first": str(frame.index.min())[:10],
                    "last": str(frame.index.max())[:10]}
    try:
        archive = zipfile.ZipFile(Path(path))
    except (OSError, zipfile.BadZipFile) as exc:
        return {"ok": False, "reason": "UNREADABLE_ARCHIVE:%s" % exc,
                "frame": None}
    member = archive.namelist()[0]
    collected = {}
    with archive.open(member) as handle:
        for raw in handle:
            hit = None
            for sid in wanted:
                if b'"%s"' % sid.encode("ascii") in raw:
                    hit = sid
                    break
            if hit is None:
                continue
            try:
                record = json.loads(raw)
            except ValueError:
                continue
            if record.get("series_id") != hit:
                continue
            points = record.get("data") or []
            dates = [p[0] for p in points if len(p) == 2 and p[1] is not None]
            values = [p[1] for p in points if len(p) == 2 and p[1] is not None]
            if not values:
                continue
            index = pd.to_datetime(pd.Index(dates), format="%Y%m%d",
                                   errors="coerce")
            collected[hit] = pd.Series(
                pd.to_numeric(pd.Series(values), errors="coerce").values,
                index=index, dtype=float).dropna().sort_index()
            if len(collected) == len(wanted):
                break
    if not collected:
        return {"ok": False, "reason": "NO_EIA_FUTURES_SERIES_FOUND",
                "frame": None}
    frame = pd.DataFrame(collected).sort_index()
    frame = frame[frame.index.notna()]
    try:
        frame.to_csv(cached)
    except OSError:
        pass
    return {"ok": set(frame.columns) >= wanted, "frame": frame,
            "from_cache": False,
            "series_found": sorted(frame.columns),
            "series_missing": sorted(wanted - set(frame.columns)),
            "first": str(frame.index.min())[:10],
            "last": str(frame.index.max())[:10],
            "source_discontinued_at": str(frame.index.max())[:10]}


# --------------------------------------------------------------------------- #
# Point-in-time SIC sector series from the OWNED financial statement data sets
# --------------------------------------------------------------------------- #
def build_pit_sector_series(files: dict) -> dict:
    """A no-look-ahead CIK -> sector series from owned ``sub.txt`` members.

    The classification a filing carried AT its own SEC acceptance timestamp,
    read through the released :mod:`alpha_agent.pit_sector` owner. A company
    reclassified in 2024 is NOT reclassified in 2011, which is the difference
    between a point-in-time sector and a look-ahead one.
    """
    series = _pit_sector.PitSicSeries()
    quarters, observations = [], 0
    for quarter, path in sorted(files.items()):
        try:
            handle = open(path, "r", encoding="latin-1", newline="")
        except OSError:
            continue
        with handle:
            reader = csv.DictReader(handle, delimiter="\t")
            for record in reader:
                cik = (record.get("cik") or "").strip()
                sic = (record.get("sic") or "").strip()
                accepted = (record.get("accepted") or "").strip()
                if not cik or not sic or not accepted:
                    continue
                series.add(str(int(cik)), sic=sic, available_at=accepted[:19],
                           provenance="FSDS sub.txt assigned SIC")
                observations += 1
        quarters.append(quarter)
    return {"ok": observations > 0, "series": series,
            "quarters": sorted(quarters),
            "observations": observations,
            "covered_ciks": len(series.covered_keys()),
            "mapping_version": _pit_sector.MAPPING_VERSION,
            "mapping_version_hash": _pit_sector.mapping_version_hash()}


# --------------------------------------------------------------------------- #
# SEC insider transactions
# --------------------------------------------------------------------------- #
def _parse_sec_date(text) -> Optional[_dt.date]:
    raw = (text or "").strip()
    if not raw:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d", "%d-%B-%Y"):
        try:
            return _dt.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


#: WHY THIS FAMILY IS COUNTED AND NOT VALUED.
#:
#: The obvious construction is dollar value: shares times price per share,
#: signed by acquisition or disposition. It was built first and MEASURED, and
#: the measurement disqualified it. The largest single (issuer, filing) row in
#: the acquired archives is 2.1e16 dollars and the implied 2008 total is
#: 1.3e16 - roughly a hundred thousand times the true figure - because
#: ``TRANS_SHARES`` and ``TRANS_PRICEPERSHARE`` are filer-entered fields with no
#: validation, and a misplaced field survives into the structured data set. A
#: value-weighted series is therefore a series about typographical errors.
#:
#: A COUNT is immune to that: a filer cannot mistype a share count into a
#: different number of filings. So each (issuer, filing date) is classified by
#: its TRANSACTION CODES alone - the one field whose vocabulary is closed -
#: into BUY (at least one open-market purchase and no sale), SELL (the mirror)
#: or MIXED, and every feature is built from those counts. This decision was
#: taken on the acquired data BEFORE any predictive evaluation, and it is
#: recorded here rather than in a commit message because it is the single most
#: consequential construction choice in the family.
INSIDER_VALUE_WEIGHTING_REJECTED = True
INSIDER_VALUE_REJECTION_REASON = (
    "TRANS_SHARES and TRANS_PRICEPERSHARE are unvalidated filer-entered "
    "fields; the acquired archives contain single filings implying 2.1e16 "
    "dollars, so a value-weighted aggregate measures data-entry error rather "
    "than insider conviction")

FILING_BUY = "BUY"
FILING_SELL = "SELL"
FILING_MIXED = "MIXED"


def load_insider_filings(files: dict) -> dict:
    """Per (issuer, FILING DATE) open-market insider direction.

    Only ``contract.INSIDER_TRANSACTION_CODES`` - open-market purchases and
    sales - are counted. Awards, option exercises, tax withholding and gifts are
    not opinions about value, and including them would turn a sentiment measure
    into a compensation-calendar measure.

    The FILING date is the index. The transaction date inside the document was
    private information on the day it happened and is never used here.
    """
    cached = cache_path("sec_insider_direction_by_cik_filing_date.csv")
    if cached.exists():
        frame = pd.read_csv(cached, dtype={"cik": str}, parse_dates=["filed"])
        return {"ok": True, "frame": frame, "from_cache": True,
                "rows": int(len(frame)),
                "first": str(frame["filed"].min())[:10],
                "last": str(frame["filed"].max())[:10]}
    codes = set(_contract.INSIDER_TRANSACTION_CODES)
    aggregate: dict = {}
    quarters_read = []
    for quarter, path in sorted(files.items()):
        try:
            archive = zipfile.ZipFile(Path(path))
        except (OSError, zipfile.BadZipFile):
            continue
        names = set(archive.namelist())
        if not {"SUBMISSION.tsv", "NONDERIV_TRANS.tsv"} <= names:
            continue
        submissions = {}
        with archive.open("SUBMISSION.tsv") as handle:
            reader = csv.DictReader(
                io.TextIOWrapper(handle, encoding="latin-1", newline=""),
                delimiter="\t")
            for record in reader:
                accession = (record.get("ACCESSION_NUMBER") or "").strip()
                cik = (record.get("ISSUERCIK") or "").strip()
                filed = _parse_sec_date(record.get("FILING_DATE"))
                if accession and cik and filed:
                    try:
                        submissions[accession] = (str(int(cik)), filed)
                    except ValueError:
                        continue
        with archive.open("NONDERIV_TRANS.tsv") as handle:
            reader = csv.DictReader(
                io.TextIOWrapper(handle, encoding="latin-1", newline=""),
                delimiter="\t")
            for record in reader:
                code = (record.get("TRANS_CODE") or "").strip()
                if code not in codes:
                    continue
                found = submissions.get(
                    (record.get("ACCESSION_NUMBER") or "").strip())
                if found is None:
                    continue
                slot = aggregate.get(found)
                if slot is None:
                    aggregate[found] = slot = {"P": 0, "S": 0}
                slot[code] = slot.get(code, 0) + 1
        quarters_read.append(quarter)
    if not aggregate:
        return {"ok": False, "reason": "NO_INSIDER_TRANSACTIONS_PARSED",
                "frame": None, "quarters_read": quarters_read}
    rows = []
    for (cik, filed), counts in aggregate.items():
        buys, sells = int(counts.get("P", 0)), int(counts.get("S", 0))
        direction = (FILING_BUY if buys and not sells else
                     FILING_SELL if sells and not buys else FILING_MIXED)
        rows.append({"cik": cik, "filed": filed, "direction": direction,
                     "purchase_transactions": buys, "sale_transactions": sells})
    frame = pd.DataFrame(rows)
    frame["filed"] = pd.to_datetime(frame["filed"])
    frame = frame.sort_values(["filed", "cik"]).reset_index(drop=True)
    try:
        frame.to_csv(cached, index=False)
    except OSError:
        pass
    return {"ok": True, "frame": frame, "from_cache": False,
            "quarters_read": sorted(quarters_read), "rows": int(len(frame)),
            "value_weighting_rejected": INSIDER_VALUE_WEIGHTING_REJECTED,
            "value_weighting_rejection_reason": INSIDER_VALUE_REJECTION_REASON,
            "first": str(frame["filed"].min())[:10],
            "last": str(frame["filed"].max())[:10]}


def insider_sector_daily(frame: pd.DataFrame, sector_series) -> dict:
    """Daily BUY / SELL filing counts by point-in-time sector.

    Every (issuer, filing date) pair is classified with the sector that issuer
    carried ON THAT DATE, through the released no-look-ahead reader. An issuer
    with no classification on or before the filing is ``Unknown`` and
    contributes only to the market aggregate - it is never guessed into a
    sector, because a guessed sector is a sector look-ahead with extra steps.
    """
    cached = cache_path("sec_insider_sector_daily_counts.csv")
    if cached.exists():
        table = pd.read_csv(cached, parse_dates=["filed"])
        known = table[table["sector"] != _pit_sector.UNKNOWN]
        total = float(table["buy_filings"].sum() + table["sell_filings"].sum())
        return {"ok": True, "frame": table, "from_cache": True,
                "sectors": sorted(table["sector"].unique()),
                "classified_share": float(
                    (known["buy_filings"].sum() + known["sell_filings"].sum())
                    / max(total, 1.0))}
    memo: dict = {}
    rows = []
    for record in frame.itertuples(index=False):
        key = (record.cik, record.filed)
        sector = memo.get(key)
        if sector is None:
            sector = sector_series.sector_as_of(record.cik, record.filed)
            memo[key] = sector
        rows.append((record.filed, sector,
                     1 if record.direction == FILING_BUY else 0,
                     1 if record.direction == FILING_SELL else 0,
                     1 if record.direction == FILING_MIXED else 0))
    table = pd.DataFrame(rows, columns=["filed", "sector", "buy_filings",
                                        "sell_filings", "mixed_filings"])
    table = table.groupby(["filed", "sector"], as_index=False).sum()
    try:
        table.to_csv(cached, index=False)
    except OSError:
        pass
    known = table[table["sector"] != _pit_sector.UNKNOWN]
    total = float(table["buy_filings"].sum() + table["sell_filings"].sum())
    return {"ok": bool(len(table)), "frame": table, "from_cache": False,
            "sectors": sorted(table["sector"].unique()),
            "classified_share": float(
                (known["buy_filings"].sum() + known["sell_filings"].sum())
                / max(total, 1.0))}


# --------------------------------------------------------------------------- #
# Coverage artifact
# --------------------------------------------------------------------------- #
def coverage_artifact(*, campaign_id: str, created_at: str, sources: dict,
                      families: dict) -> dict:
    from .. import r35
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "alignment_rule": {
            "owner": "%s.as_of_align" % CALCULATION_OWNER,
            "rule": "value is visible on a session only if observable_at <= "
                    "that session, then shifted by the declared broadcast lag",
            "broadcast_lag_sessions": _contract.BROADCAST_LAG_SESSIONS,
            "insider_observable_at": _contract.INSIDER_OBSERVABLE_AT,
            "insider_transaction_date_may_be_observable":
                _contract.INSIDER_TRANSACTION_DATE_MAY_BE_OBSERVABLE,
            "cot_publication_lag_days": _contract.COT_PUBLICATION_LAG_DAYS,
            "oecd_rate_publication_lag_months":
                _contract.OECD_RATE_PUBLICATION_LAG_MONTHS,
        },
        "sources": sources,
        "families": families,
        "prohibited_substitutions": list(_contract.PROHIBITED_SUBSTITUTIONS),
    }
    return r35.artifact_body(COVERAGE_SCHEMA, payload)


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    from .. import r35
    return r35.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    from .. import r35
    return r35.write_json(path_for(body.get("campaign_id",
                                            _contract.CAMPAIGN_ID)), body)


__all__ = ["CALCULATION_OWNER", "as_of_align", "load_cot",
           "cot_instrument_series", "load_fred", "load_cboe",
           "load_eia_curve", "build_pit_sector_series", "load_insider_filings",
           "insider_sector_daily", "coverage_artifact", "freeze", "path_for",
           "cache_path"]
