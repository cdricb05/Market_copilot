"""alpha_agent.r46.cftc - the CFTC positioning lane. Free, public, PIT-stamped.

The Commitments of Traders report is the one large free dataset in this
estate that is NOT a price: it is who holds the risk. Release 35 acquired the
annual archives (1986-2026) and parsed them; Release 46.3 nominated a
positioning challenger and never built one. This module builds the lane
through to prospective readiness:

**Raw capture, never overwritten.** Each run checks the CFTC's current annual
archive and current-week file, and stores a NEW copy under the Release-46
research root when the remote ``Last-Modified`` has moved, with the
acquisition instant, the remote stamp and the sha256 in an append-only
capture manifest. Release 35's archives are read for HISTORY only and their
bytes are never touched.

**Point-in-time contract.** A report is dated by its ``As of`` Tuesday and
published on the following Friday at 15:30 ET, with holiday and shutdown
exceptions. Going forward, a report is admissible at an emission instant only
if it appears in a capture acquired BEFORE that instant - a proof, not a
rule. For the history used to nominate and to build risk priors, the R35
rule applies: a report is observable ``COT_PUBLICATION_LAG_DAYS`` (6) calendar
days after its ``As of`` date, and never earlier.

**Economic mapping.** CFTC contract codes are mapped to the owned continuous
futures the challengers can actually express a view in. A code whose report
name does not carry the expected market keyword is refused, not guessed.

**Two bounded, economically motivated challengers**, frozen in
:mod:`alpha_agent.r46.challengers` before their first emission:
crowded-positioning REVERSAL (fade the three-year z-score of speculative net
positioning) and positioning FLOW continuation (follow the 13-week change).
No threshold was swept; the windows are Release 35's declared constants.

Spends nothing. Creates no account.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import io
import json
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.cftc"

ARTIFACT = "R46_4_CFTC_LANE.json"
RAW_DIRNAME = "_data_cftc"
MANIFEST_NAME = "cftc_captures.json"


def raw_dir() -> Path:
    """Resolved at CALL time from the package root, so a hermetic test root
    redirects the captures too. Production: the R46 research root."""
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME

#: Release 35's archives - HISTORY, read-only, never rewritten by this lane.
R35_ARCHIVE_DIR = Path(r"D:\Stock_Prediction_app_data\orthogonal_information_r35"
                       r"\acquired\cftc_commitments_of_traders")
HISTORY_FIRST_YEAR = 2015

ANNUAL_URL = "https://www.cftc.gov/files/dea/history/deacot{year}.zip"
CURRENT_URL = "https://www.cftc.gov/dea/newcot/deafut.txt"
UA = "paper-trader-research/46.4 (research; contact via operator)"
HTTP_TIMEOUT = 90

#: Publication contract (Release 35's declared constants, never re-chosen).
PUBLICATION_LAG_DAYS = 6
PUBLICATION_LAG_STRESS_DAYS = 28
RELEASE_RULE = ("published Friday 15:30 ET for the preceding Tuesday; a "
                "report is observable %d calendar days after its As-of date "
                "for history, and only from a capture acquired before the "
                "emission instant going forward" % PUBLICATION_LAG_DAYS)

#: Signal windows - Release 35's declared constants.
Z_WINDOW_WEEKS = 156
CHANGE_WEEKS = 13
MIN_MARKETS = 12

COLS = {
    "as_of": "As of Date in Form YYYY-MM-DD",
    "code": "CFTC Contract Market Code",
    "name": "Market and Exchange Names",
    "oi": "Open Interest (All)",
    "nc_long": "Noncommercial Positions-Long (All)",
    "nc_short": "Noncommercial Positions-Short (All)",
    "c_long": "Commercial Positions-Long (All)",
    "c_short": "Commercial Positions-Short (All)",
}

#: CFTC contract code -> (owned continuous future, required name keyword,
#: cost class). A keyword miss refuses the mapping rather than guessing.
MARKET_MAP = {
    "13874A": ("&ES", "E-MINI S&P 500", "EQUITY_INDEX_FUTURES"),
    "209742": ("&NQ", "NASDAQ", "EQUITY_INDEX_FUTURES"),
    "124603": ("&YM", "DJIA", "EQUITY_INDEX_FUTURES"),
    "239742": ("&RTY", "RUSSELL", "EQUITY_INDEX_FUTURES"),
    "043602": ("&ZN", "10Y", "RATES_FUTURES"),
    "042601": ("&ZT", "2Y", "RATES_FUTURES"),
    "044601": ("&ZF", "5Y", "RATES_FUTURES"),
    "020601": ("&ZB", "UST BOND", "RATES_FUTURES"),
    "020604": ("&UB", "ULTRA", "RATES_FUTURES"),
    "099741": ("&6E", "EURO FX", "FX_FUTURES"),
    "096742": ("&6B", "BRITISH POUND", "FX_FUTURES"),
    "090741": ("&6C", "CANADIAN DOLLAR", "FX_FUTURES"),
    "097741": ("&6J", "JAPANESE YEN", "FX_FUTURES"),
    "092741": ("&6S", "SWISS FRANC", "FX_FUTURES"),
    "232741": ("&6A", "AUSTRALIAN DOLLAR", "FX_FUTURES"),
    "112741": ("&6N", "NZ DOLLAR", "FX_FUTURES"),
    "095741": ("&6M", "MEXICAN PESO", "FX_FUTURES"),
    "098662": ("&DX", "USD INDEX", "FX_FUTURES"),
    "067651": ("&CL", "WTI", "COMMODITY_FUTURES"),
    "023651": ("&NG", "NAT GAS", "COMMODITY_FUTURES"),
    "022651": ("&HO", "ULSD", "COMMODITY_FUTURES"),
    "111659": ("&RB", "GASOLINE", "COMMODITY_FUTURES"),
    "088691": ("&GC", "GOLD", "COMMODITY_FUTURES"),
    "084691": ("&SI", "SILVER", "COMMODITY_FUTURES"),
    "085692": ("&HG", "COPPER", "COMMODITY_FUTURES"),
    "076651": ("&PL", "PLATINUM", "COMMODITY_FUTURES"),
    "075651": ("&PA", "PALLADIUM", "COMMODITY_FUTURES"),
    "002602": ("&ZC", "CORN", "COMMODITY_FUTURES"),
    "005602": ("&ZS", "SOYBEANS", "COMMODITY_FUTURES"),
    "001602": ("&ZW", "WHEAT", "COMMODITY_FUTURES"),
    "026603": ("&ZM", "SOYBEAN MEAL", "COMMODITY_FUTURES"),
    "007601": ("&ZL", "SOYBEAN OIL", "COMMODITY_FUTURES"),
    "083731": ("&KC", "COFFEE", "COMMODITY_FUTURES"),
    "033661": ("&CT", "COTTON", "COMMODITY_FUTURES"),
    "080732": ("&SB", "SUGAR", "COMMODITY_FUTURES"),
    "073732": ("&CC", "COCOA", "COMMODITY_FUTURES"),
    "057642": ("&LE", "LIVE CATTLE", "COMMODITY_FUTURES"),
    "054642": ("&HE", "LEAN HOGS", "COMMODITY_FUTURES"),
    "1170E1": ("&VX", "VIX", "VOLATILITY_FUTURES"),
}


# --------------------------------------------------------------------------- #
# Capture - append-only, never overwrite
# --------------------------------------------------------------------------- #
def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _manifest() -> dict:
    return read_json(manifest_path(), default=None) or {"schema": "r46_4_cftc_captures/1",
                                                  "captures": []}


def _head(url: str) -> dict:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA},
                                     method="HEAD")
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as fh:
            return {"status": fh.status,
                    "last_modified": fh.headers.get("Last-Modified"),
                    "length": fh.headers.get("Content-Length")}
    except Exception as exc:                    # noqa: BLE001 - reported
        return {"status": None, "error": "%s: %s" % (type(exc).__name__,
                                                     str(exc)[:160])}


def _get(url: str) -> Optional[bytes]:
    try:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as fh:
            return fh.read()
    except Exception:                           # noqa: BLE001
        return None


def acquire(*, acquire: bool = True, today: _dt.date = None) -> dict:
    """Capture the current annual archive and current-week file if they moved."""
    raw_dir().mkdir(parents=True, exist_ok=True)
    now = CK.now_utc()
    today = today or CK.eastern_date(now)
    man = _manifest()
    caps = list(man.get("captures") or [])
    seen = {(c.get("source"), c.get("remote_last_modified")) for c in caps}
    results = []
    for source, url in (("ANNUAL", ANNUAL_URL.format(year=today.year)),
                        ("CURRENT_WEEK", CURRENT_URL)):
        head = _head(url)
        rec = {"source": source, "url": url, "probe": head}
        lm = head.get("last_modified")
        if head.get("status") != 200:
            rec["state"] = "UNREACHABLE"
        elif (source, lm) in seen:
            rec["state"] = "ALREADY_CAPTURED"
        elif not acquire:
            rec["state"] = "NEW_REMOTE_NOT_ACQUIRED"
        else:
            body = _get(url)
            if body is None:
                rec["state"] = "DOWNLOAD_FAILED"
            else:
                stamp = now.strftime("%Y%m%dT%H%M%SZ")
                ext = ".zip" if source == "ANNUAL" else ".txt"
                dest = raw_dir() / ("%s_%d_%s%s" % (source.lower(), today.year,
                                                  stamp, ext))
                dest.write_bytes(body)
                cap = {"source": source, "url": url, "path": str(dest),
                       "bytes": len(body), "sha256": _sha(body),
                       "remote_last_modified": lm,
                       "acquired_at_utc": CK.iso(now),
                       "acquired_at_utc_precise": CK.iso_precise(now),
                       "licence": "US CFTC, public domain"}
                caps.append(cap)
                rec.update(cap)
                rec["state"] = "CAPTURED"
        results.append(rec)
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"results": results, "n_captures": len(caps),
            "money_spent_usd": 0.0}


# --------------------------------------------------------------------------- #
# Loading
# --------------------------------------------------------------------------- #
def _parse_zip(raw: bytes) -> Optional[pd.DataFrame]:
    try:
        z = zipfile.ZipFile(io.BytesIO(raw))
        name = z.namelist()[0]
        df = pd.read_csv(io.BytesIO(z.read(name)), low_memory=False)
    except Exception:                           # noqa: BLE001
        return None
    return _normalise(df)


def _normalise(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    cols = {c.strip(): c for c in df.columns}
    need = {}
    for k, label in COLS.items():
        hit = next((cols[c] for c in cols if c == label), None)
        if hit is None:
            return None
        need[k] = hit
    out = pd.DataFrame({k: df[v] for k, v in need.items()})
    out["as_of"] = pd.to_datetime(out["as_of"], errors="coerce")
    out["code"] = out["code"].astype(str).str.strip()
    for k in ("oi", "nc_long", "nc_short", "c_long", "c_short"):
        out[k] = pd.to_numeric(out[k].astype(str).str.replace(",", ""),
                               errors="coerce")
    out = out.dropna(subset=["as_of", "oi"])
    return out


def load_history() -> pd.DataFrame:
    """R35 archives (read-only) plus every R46.4 annual capture, deduplicated."""
    frames = []
    if R35_ARCHIVE_DIR.is_dir():
        for p in sorted(R35_ARCHIVE_DIR.glob("deacot*.zip")):
            try:
                year = int(p.stem[-4:])
            except ValueError:
                continue
            if year < HISTORY_FIRST_YEAR:
                continue
            df = _parse_zip(p.read_bytes())
            if df is not None:
                df["capture"] = "R35:" + p.name
                frames.append(df)
    for cap in _manifest().get("captures") or ():
        if cap.get("source") != "ANNUAL":
            continue
        p = Path(cap["path"])
        if p.exists():
            df = _parse_zip(p.read_bytes())
            if df is not None:
                df["capture"] = "R46_4:" + p.name
                df["acquired_at_utc"] = cap.get("acquired_at_utc")
                frames.append(df)
    if not frames:
        return pd.DataFrame(columns=list(COLS) + ["capture"])
    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values(["as_of", "code", "capture"])
    return df.drop_duplicates(subset=["as_of", "code"], keep="last")


def mapped_markets(df: pd.DataFrame) -> dict:
    """Codes whose latest report name carries the expected keyword."""
    ok, refused = {}, []
    if df is None or not len(df):
        return {"mapped": ok, "refused": refused}
    latest = df.sort_values("as_of").groupby("code").tail(1)
    names = {str(r.code): str(r.name).upper() for r in latest.itertuples()}
    for code, (sym, kw, klass) in MARKET_MAP.items():
        nm = names.get(code)
        if nm is None:
            refused.append({"code": code, "why": "NOT_IN_REPORT"})
        elif kw.upper() not in nm:
            refused.append({"code": code, "why": "NAME_KEYWORD_MISMATCH",
                            "name": nm})
        else:
            ok[code] = {"instrument": sym, "cost_class": klass, "name": nm}
    return {"mapped": ok, "refused": refused}


def observable_reports(df: pd.DataFrame, as_of: _dt.date,
                       lag_days: int = PUBLICATION_LAG_DAYS) -> pd.DataFrame:
    """Reports whose As-of date is at least ``lag_days`` before ``as_of``."""
    if df is None or not len(df):
        return df
    cutoff = pd.Timestamp(as_of - _dt.timedelta(days=lag_days))
    return df[df["as_of"] <= cutoff]


def positioning(df: pd.DataFrame, as_of: _dt.date) -> dict:
    """Per mapped market: spec net share of OI, its 3y z, its 13w change."""
    obs = observable_reports(df, as_of)
    mm = mapped_markets(obs)
    out = {}
    for code, info in mm["mapped"].items():
        sub = obs[obs["code"] == code].sort_values("as_of")
        sub = sub[sub["oi"] > 0]
        if len(sub) < CHANGE_WEEKS + 2:
            continue
        share = ((sub["nc_long"] - sub["nc_short"]) / sub["oi"]).to_numpy(
            dtype=float)
        w = share[-Z_WINDOW_WEEKS:]
        sd = float(np.std(w, ddof=1)) if len(w) > 10 else float("nan")
        z = (float(share[-1] - w.mean()) / sd) if sd and np.isfinite(sd) \
            and sd > 0 else None
        chg = float(share[-1] - share[-1 - CHANGE_WEEKS])
        comm = ((sub["c_long"] - sub["c_short"]) / sub["oi"]).to_numpy(
            dtype=float)
        out[info["instrument"]] = {
            "code": code, "name": info["name"],
            "cost_class": info["cost_class"],
            "report_as_of": str(sub["as_of"].iloc[-1].date()),
            "observable_from": str(sub["as_of"].iloc[-1].date()
                                   + _dt.timedelta(days=PUBLICATION_LAG_DAYS)),
            "spec_net_share": float(share[-1]),
            "spec_net_share_z": z,
            "spec_net_share_change_13w": chg,
            "commercial_net_share": float(comm[-1]),
            "n_reports_in_window": int(len(w)),
        }
    return {"as_of": str(as_of), "markets": out, "refused": mm["refused"],
            "n_markets": len(out), "publication_lag_days": PUBLICATION_LAG_DAYS}


# --------------------------------------------------------------------------- #
def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of)
    df = load_history()
    n_rows = int(len(df))
    latest_report = (str(df["as_of"].max().date()) if n_rows else None)
    first_report = (str(df["as_of"].min().date()) if n_rows else None)
    pos = positioning(df, as_of) if n_rows else {"markets": {}, "refused": [],
                                                 "n_markets": 0}
    available = pos["n_markets"] >= MIN_MARKETS
    caps = _manifest().get("captures") or []
    state = ("LIVE_PROSPECTIVE" if available and caps else
             "FROZEN_PENDING_EMISSION" if available else "DATA_BLOCKED")
    body = artifact_body(
        "r46_4_cftc_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=state,
        lane_state_vocabulary=["LIVE_PROSPECTIVE", "FROZEN_PENDING_EMISSION",
                               "SAMPLE_ACCRUING", "DATA_BLOCKED",
                               "PIT_BLOCKED", "ENTITLEMENT_BLOCKED",
                               "ECONOMICALLY_REJECTED"],
        acquisition=acq,
        captures=caps,
        raw_root=str(raw_dir()),
        r35_archives_read_only=True,
        r35_archive_dir=str(R35_ARCHIVE_DIR),
        coverage={"n_rows": n_rows, "first_report": first_report,
                  "latest_report": latest_report,
                  "n_markets_mapped": pos["n_markets"],
                  "n_codes_refused": len(pos.get("refused") or []),
                  "refused": pos.get("refused"),
                  "history_first_year": HISTORY_FIRST_YEAR},
        point_in_time={"release_rule": RELEASE_RULE,
                       "publication_lag_days": PUBLICATION_LAG_DAYS,
                       "stress_lag_days": PUBLICATION_LAG_STRESS_DAYS,
                       "latest_observable_report_for_as_of":
                           max((m["report_as_of"] for m in
                                pos["markets"].values()), default=None),
                       "forward_admissibility":
                           "capture acquired_at_utc < emitted_at_utc"},
        economic_mapping={"n_mapped": len(MARKET_MAP),
                          "map": {k: {"instrument": v[0], "keyword": v[1],
                                      "cost_class": v[2]}
                                  for k, v in MARKET_MAP.items()}},
        positioning=pos,
        challengers_frozen=["r46_4_cot_xs_positioning_reversal",
                            "r46_4_cot_xs_positioning_flow"],
        hypotheses={
            "crowded_positioning_reversal": "fade the 156-week z-score of "
                                            "speculative net positioning",
            "positioning_flow_continuation": "follow the 13-week change in "
                                             "speculative net positioning",
            "not_swept": "windows are Release 35's declared constants; no "
                         "threshold was searched"},
        information_family="POSITIONING",
        money_spent_usd=0.0,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir", "MARKET_MAP",
           "PUBLICATION_LAG_DAYS", "Z_WINDOW_WEEKS", "CHANGE_WEEKS",
           "MIN_MARKETS", "acquire", "load_history", "mapped_markets",
           "observable_reports", "positioning", "run"]
