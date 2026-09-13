"""alpha_agent.alpha_recovery.ownership_13f - point-in-time institutional
ownership BREADTH from the free SEC Form 13F structured data sets.

Breadth is the number of DISTINCT reporting managers holding a security in a
13F reporting period. The signal is its quarter-over-quarter change. Both are
trivial to define and easy to get wrong, because 13F is a filing stream, not a
dataset: the same manager refiles, amends, restates, files late, files a notice
with no holdings at all, and reports options on a name it does not own.

The canonical treatments, each declared here and TESTED, are:

``13F-HR``            a holdings report; counted.
``13F-HR/A``          an amendment. ``AMENDMENTTYPE = RESTATEMENT`` REPLACES the
                      manager's table from its own filing date; ``NEW HOLDINGS``
                      ADDS to it. An amendment that declares neither is read as
                      a restatement, because a filer who resubmits a whole table
                      without saying so has replaced it.
``13F-NT``            a notice that another manager reports the holdings. It
                      carries no information table and is NEVER counted; counting
                      it would inflate breadth with managers holding nothing.
duplicate filings     the same manager may file more than one original for a
                      period. The LATEST original before the decision wins; the
                      superseded one is not double-counted.
late filings          included from the moment they are filed and not before.
                      This is the whole point of an acceptance-dated read: the
                      deadline is when a filing is DUE, never when it arrived,
                      and the measured distribution of arrival dates is reported
                      rather than assumed.
zero / exited         a line with no shares and no value is a closed position
                      and is not a holding. A name absent from a table is not
                      held, which is what makes breadth fall.
options               a row carrying ``PUTCALL`` is a derivative position on the
                      name, not ownership of the share, and is excluded.
ticker changes        never touched here; identity is
                      :mod:`ownership_identity`'s single responsibility.
delisted names        retained, because the owned panel retains them; dropping
                      them would rebuild survivorship bias at the join.

POINT IN TIME. For a decision dated D, only accessions whose FILING_DATE is
strictly before D exist. The structured data sets carry a filing date, not an
acceptance instant; a filing accepted after 17:30 ET is disseminated the next
business day, so requiring the filing date to be STRICTLY earlier than the
decision is the conservative reading of the same fact, and it is the estate's
existing SEC convention (``SEC_FILING_BROADCAST_LAG_SESSIONS = 1``).

Deterministic; no network; no live write.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd

from . import now_iso, research_root
from . import ownership_data as OD
from . import ownership_identity as OI

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ownership_13f"

FORM_HOLDINGS = "13F-HR"
FORM_AMENDMENT = "13F-HR/A"
FORM_NOTICE = "13F-NT"
AMEND_RESTATEMENT = "RESTATEMENT"
AMEND_NEW_HOLDINGS = "NEW HOLDINGS"

SUBMISSION_CACHE = "form13f_submissions.csv"
HOLDINGS_SUBDIR = "form13f_holdings"

#: Chunk size for INFOTABLE.tsv, which runs to ~2 million rows a quarter.
CHUNK = 1_000_000


def derived_dir() -> Path:
    return research_root() / "_derived"


def holdings_dir() -> Path:
    return derived_dir() / HOLDINGS_SUBDIR


def _member(zf: zipfile.ZipFile, name: str) -> str:
    """Resolve a member by BASENAME. Most vintages store ``INFOTABLE.tsv`` at
    the archive root; at least one (01jun2025-31aug2025) nests every member
    inside a directory, and an exact-name lookup fails on that one alone."""
    for n in zf.namelist():
        if n.rsplit("/", 1)[-1].upper() == name.upper():
            return n
    raise KeyError("no member named %r in %s" % (name, zf.filename))


def _read_member(zf: zipfile.ZipFile, name: str, **kw) -> pd.DataFrame:
    """A member of a 13F data set as text. ``QUOTE_NONE`` on purpose: issuer
    names carry unbalanced quotes and letting the parser interpret them
    swallows whole rows."""
    with zf.open(_member(zf, name)) as fh:
        return pd.read_csv(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                           sep="\t", dtype=str, quoting=csv.QUOTE_NONE,
                           on_bad_lines="skip", **kw)


def _as_date(s: pd.Series) -> pd.Series:
    """13F data sets spell dates ``31-MAR-2016``; some vintages use ISO."""
    d = pd.to_datetime(s, format="%d-%b-%Y", errors="coerce")
    if d.isna().mean() > 0.5:
        d = pd.to_datetime(s, errors="coerce")
    return d


# --------------------------------------------------------------------------- #
# The submission index: small, and the only place the PIT rule is applied
# --------------------------------------------------------------------------- #
def build_submission_index(*, rebuild: bool = False, verbose: bool = True) -> pd.DataFrame:
    """Every 13F submission in the archive: accession, manager, period, filing
    date, form and amendment semantics. ~400k rows; the holdings are elsewhere."""
    cache = derived_dir() / SUBMISSION_CACHE
    if cache.exists() and not rebuild:
        return pd.read_csv(cache, dtype={"cik": str, "accession": str},
                           parse_dates=["filing_date", "period"])
    files = sorted(OD.f13_dir().glob("*_form13f.zip"))
    parts = []
    for k, p in enumerate(files):
        zf = zipfile.ZipFile(p)
        sub = _read_member(zf, "SUBMISSION.tsv")
        cov = _read_member(zf, "COVERPAGE.tsv")
        keep_c = [c for c in ("ACCESSION_NUMBER", "ISAMENDMENT", "AMENDMENTNO",
                              "AMENDMENTTYPE", "REPORTTYPE") if c in cov.columns]
        df = sub.merge(cov[keep_c], on="ACCESSION_NUMBER", how="left")
        df = df.rename(columns={"ACCESSION_NUMBER": "accession", "CIK": "cik",
                                "SUBMISSIONTYPE": "form", "FILING_DATE": "filing_date",
                                "PERIODOFREPORT": "period", "ISAMENDMENT": "is_amendment",
                                "AMENDMENTNO": "amendment_no",
                                "AMENDMENTTYPE": "amendment_type",
                                "REPORTTYPE": "report_type"})
        df["source_file"] = p.name
        parts.append(df)
        if verbose:
            print("  submissions %2d/%d %-34s %6d" % (k + 1, len(files), p.name, len(df)),
                  flush=True)
    out = pd.concat(parts, ignore_index=True)
    out["filing_date"] = _as_date(out["filing_date"])
    out["period"] = _as_date(out["period"])
    out["cik"] = out["cik"].astype(str).str.strip().str.lstrip("0")
    out["form"] = out["form"].astype(str).str.strip().str.upper()
    out["is_amendment"] = out["is_amendment"].fillna("").str.strip().str.upper().eq("Y")
    out["amendment_type"] = out["amendment_type"].fillna("").str.strip().str.upper()
    out = out.dropna(subset=["filing_date", "period", "accession"])
    out = out.drop_duplicates(subset=["accession"])
    derived_dir().mkdir(parents=True, exist_ok=True)
    cols = ["accession", "cik", "form", "filing_date", "period", "is_amendment",
            "amendment_no", "amendment_type", "report_type", "source_file"]
    out[cols].to_csv(cache, index=False)
    return out[cols]


def effective_accessions(sub: pd.DataFrame, period: pd.Timestamp, cutoff) -> set:
    """The accessions that carry a manager's EFFECTIVE table for ``period`` as
    the world looked at ``cutoff`` (exclusive).

    Per manager: the latest restatement if one exists, else the latest original,
    plus every ``NEW HOLDINGS`` amendment filed at or after that base. A
    superseded original is dropped so one manager is never counted twice, and a
    notice (13F-NT) is never included because it carries no table.
    """
    cut = pd.Timestamp(cutoff)
    s = sub[(sub["period"] == pd.Timestamp(period)) & (sub["filing_date"] < cut)
            & sub["form"].isin([FORM_HOLDINGS, FORM_AMENDMENT])]
    if s.empty:
        return set()
    keep = set()
    for _cik, g in s.groupby("cik", sort=False):
        g = g.sort_values("filing_date", kind="mergesort")
        orig = g[~g["is_amendment"]]
        amd = g[g["is_amendment"]]
        restate = amd[amd["amendment_type"] != AMEND_NEW_HOLDINGS]
        if len(restate):
            base = restate.iloc[-1]
        elif len(orig):
            base = orig.iloc[-1]
        elif len(amd):
            base = amd.iloc[-1]
        else:
            continue
        keep.add(base["accession"])
        add = amd[(amd["amendment_type"] == AMEND_NEW_HOLDINGS)
                  & (amd["filing_date"] >= base["filing_date"])]
        keep.update(add["accession"].tolist())
    return keep


# --------------------------------------------------------------------------- #
# Holdings: (accession, cusip8) pairs, stored per reporting period
# --------------------------------------------------------------------------- #
def _period_tag(p) -> str:
    d = pd.Timestamp(p)
    return "%04d%02d" % (d.year, d.month)


def build_holdings(relevant_cusips: set, *, rebuild: bool = False,
                   verbose: bool = True) -> dict:
    """Distinct (accession, cusip8) ownership pairs, bucketed by reporting
    period, restricted to CUSIPs the owned panel can ever reach.

    The restriction is a storage decision, not a research one: breadth counts
    managers per NAME, so removing names the panel cannot trade changes no
    surviving number, and the point-in-time test of whether a CUSIP is
    resolvable on a given date still happens later, per decision.
    """
    holdings_dir().mkdir(parents=True, exist_ok=True)
    sub = build_submission_index(verbose=verbose)
    acc_period = dict(zip(sub["accession"], sub["period"].map(_period_tag)))
    files = sorted(OD.f13_dir().glob("*_form13f.zip"))
    tags = sorted({t for t in acc_period.values()})
    if not rebuild and all((holdings_dir() / ("holdings_%s.npz" % t)).exists() for t in tags):
        return {"state": "CACHED", "periods": len(tags)}
    buckets: dict = {}
    stats = {"rows_read": 0, "rows_option": 0, "rows_zero": 0, "rows_kept": 0,
             "rows_unknown_accession": 0, "rows_cusip_unparsed": 0,
             "rows_not_relevant": 0}
    for k, p in enumerate(files):
        zf = zipfile.ZipFile(p)
        info = _member(zf, "INFOTABLE.tsv")
        with zf.open(info) as fh:
            head = io.TextIOWrapper(fh, encoding="utf-8", errors="replace").readline()
        have = [c.strip() for c in head.rstrip("\r\n").split("\t")]
        cols = [c for c in ("ACCESSION_NUMBER", "CUSIP", "PUTCALL", "VALUE", "SSHPRNAMT")
                if c in have]
        with zf.open(info) as fh:
            reader = pd.read_csv(io.TextIOWrapper(fh, encoding="utf-8", errors="replace"),
                                 sep="\t", dtype=str, quoting=csv.QUOTE_NONE,
                                 on_bad_lines="skip", usecols=cols, chunksize=CHUNK)
            for chunk in reader:
                stats["rows_read"] += len(chunk)
                if "PUTCALL" in chunk.columns:
                    isopt = chunk["PUTCALL"].fillna("").str.strip().ne("")
                    stats["rows_option"] += int(isopt.sum())
                    chunk = chunk[~isopt]
                val = pd.to_numeric(chunk.get("VALUE"), errors="coerce").fillna(0.0)
                shr = pd.to_numeric(chunk.get("SSHPRNAMT"), errors="coerce").fillna(0.0)
                dead = (val <= 0) & (shr <= 0)
                stats["rows_zero"] += int(dead.sum())
                chunk = chunk[~dead]
                cu = chunk["CUSIP"].map(OI.cusip_key)
                bad = cu.isna()
                stats["rows_cusip_unparsed"] += int(bad.sum())
                chunk = chunk[~bad]
                cu = cu[~bad]
                rel = cu.isin(relevant_cusips)
                stats["rows_not_relevant"] += int((~rel).sum())
                chunk, cu = chunk[rel], cu[rel]
                acc = chunk["ACCESSION_NUMBER"].astype(str)
                tag = acc.map(acc_period)
                unk = tag.isna()
                stats["rows_unknown_accession"] += int(unk.sum())
                acc, cu, tag = acc[~unk], cu[~unk], tag[~unk]
                stats["rows_kept"] += len(acc)
                df = pd.DataFrame({"acc": acc.to_numpy(), "cusip": cu.to_numpy(),
                                   "tag": tag.to_numpy()})
                for t, g in df.groupby("tag", sort=False):
                    buckets.setdefault(t, []).append(
                        g[["acc", "cusip"]].drop_duplicates())
        if verbose:
            print("  holdings %2d/%d %-34s kept=%d" % (k + 1, len(files), p.name,
                                                       stats["rows_kept"]), flush=True)
    written = 0
    for t, parts in buckets.items():
        df = pd.concat(parts, ignore_index=True).drop_duplicates()
        accs, ai = np.unique(df["acc"].to_numpy().astype(str), return_inverse=True)
        cus, ci = np.unique(df["cusip"].to_numpy().astype(str), return_inverse=True)
        np.savez_compressed(holdings_dir() / ("holdings_%s.npz" % t),
                            accessions=accs, cusips=cus,
                            acc_idx=ai.astype(np.int32), cus_idx=ci.astype(np.int32))
        written += 1
    summary = {"state": "OK", "periods_written": written, "stats": stats,
               "built_at": now_iso()}
    (derived_dir() / "form13f_holdings_summary.json").write_text(
        json.dumps(summary, indent=1, sort_keys=True), encoding="utf-8")
    return summary


def load_period(tag: str) -> dict | None:
    p = holdings_dir() / ("holdings_%s.npz" % tag)
    if not p.exists():
        return None
    z = np.load(p, allow_pickle=False)
    return {"accessions": z["accessions"].astype(str), "cusips": z["cusips"].astype(str),
            "acc_idx": z["acc_idx"], "cus_idx": z["cus_idx"]}


def breadth_as_of(sub: pd.DataFrame, period, cutoff) -> pd.Series:
    """cusip8 -> number of DISTINCT managers reporting the name for ``period``,
    counting only filings already made at ``cutoff``."""
    tag = _period_tag(period)
    blk = load_period(tag)
    if blk is None:
        return pd.Series(dtype="int64")
    eff = effective_accessions(sub, period, cutoff)
    if not eff:
        return pd.Series(dtype="int64")
    acc2cik = dict(zip(sub["accession"], sub["cik"]))
    keep_acc = np.isin(blk["accessions"], list(eff))
    mask = keep_acc[blk["acc_idx"]]
    if not mask.any():
        return pd.Series(dtype="int64")
    accs = blk["accessions"][blk["acc_idx"][mask]]
    cus = blk["cusips"][blk["cus_idx"][mask]]
    ciks = np.array([acc2cik.get(a, a) for a in accs])
    df = pd.DataFrame({"cusip": cus, "cik": ciks}).drop_duplicates()
    return df.groupby("cusip")["cik"].size().astype("int64")


# --------------------------------------------------------------------------- #
# Measured filing behaviour (never assumed)
# --------------------------------------------------------------------------- #
def filing_timing(sub: pd.DataFrame) -> dict:
    """How long after period end managers actually file. The 45-day deadline is
    a due date, not an observation, and an experiment that assumes everyone
    files on it would read holdings that did not yet exist."""
    s = sub[sub["form"].isin([FORM_HOLDINGS, FORM_AMENDMENT])].copy()
    s["days"] = (s["filing_date"] - s["period"]).dt.days
    s = s[(s["days"] >= 0) & (s["days"] <= 400)]
    q = s["days"].quantile([0.05, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99]).round(1)
    by_form = s.groupby("form")["days"].agg(["count", "median", "max"])
    return {
        "n_filings": int(len(s)),
        "days_after_period_end": {str(k): float(v) for k, v in q.items()},
        "share_filed_by_day_45": float((s["days"] <= 45).mean()),
        "share_filed_by_day_46": float((s["days"] <= 46).mean()),
        "share_filed_after_day_60": float((s["days"] > 60).mean()),
        "by_form": {str(i): {k: float(v) for k, v in r.items()}
                    for i, r in by_form.iterrows()},
        "periods": int(sub["period"].nunique()),
        "first_period": str(sub["period"].min())[:10],
        "last_period": str(sub["period"].max())[:10],
        "form_counts": {str(k): int(v) for k, v in
                        sub["form"].value_counts().head(10).items()},
        "amendment_type_counts": {str(k or "(none)"): int(v) for k, v in
                                  sub[sub["is_amendment"]]["amendment_type"]
                                  .value_counts().head(10).items()},
    }
