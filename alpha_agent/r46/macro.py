"""alpha_agent.r46.macro - the macro release / surprise lane. First-published only.

Release 45 settled the honest way to build a macro surprise without buying a
consensus history: ALFRED's initial-release vintage (``output_type=4``) is
the value as FIRST PUBLISHED, so a causal forecast from earlier initial
releases and the deviation of the new print from it is a model-based
surprise that cannot have been restated. This lane carries that method into
the prospective tournament:

* **Raw capture, PIT-stamped.** Every run captures, for each release family,
  the initial-release series (each row carrying ``realtime_start`` - the day
  it was published) and the FRED release calendar (past and scheduled future
  dates), under the Release-46 research root with the acquisition instant.
  Nothing is overwritten; a revised value never replaces a first print.
* **Provable release timing.** A print is admissible at an emission instant
  only if its ``realtime_start`` is on or before the emission's Eastern date
  and it appears in a capture acquired before the emission. BLS/BEA releases
  print at 08:30 ET; the tournament emits after the close, so the ordering
  holds by construction and is recorded on the row.
* **One bounded challenger**, frozen before its first emission: on a CPI or
  Employment Situation release day, position the 10-year note future
  AGAINST the surprise (an upside inflation or payrolls surprise is short
  duration) for five sessions. Sign only - no magnitude threshold was chosen.
  Release 45 showed native rates markets complete most of their reaction
  within a minute; whether anything continues at a daily horizon is exactly
  what the forward record is for.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import urllib.request
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.macro"

ARTIFACT = "R46_4_MACRO_LANE.json"
RAW_DIRNAME = "_data_macro"
MANIFEST_NAME = "macro_captures.json"


def raw_dir() -> Path:
    """Resolved at CALL time from the package root (hermetic under a test
    root); production is the R46 research root."""
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME

FRED = "https://api.stlouisfed.org/fred/"
KEY_ENV = ("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY")
HTTP_TIMEOUT = 90

#: Release families: FRED release id, headline series, and the sign a
#: POSITIVE surprise implies for the 10-year note future.
RELEASES = {
    "CPI": {"release_id": 10, "series": "CPIAUCSL", "zn_sign": -1,
            "publisher": "BLS 08:30 ET"},
    "EMPLOYMENT": {"release_id": 50, "series": "PAYEMS", "zn_sign": -1,
                   "publisher": "BLS 08:30 ET"},
    "RETAIL_SALES": {"release_id": 9, "series": "RSAFS", "zn_sign": -1,
                     "publisher": "Census 08:30 ET"},
    "GDP": {"release_id": 53, "series": "GDPC1", "zn_sign": -1,
            "publisher": "BEA 08:30 ET"},
    "PPI": {"release_id": 46, "series": "PPIFIS", "zn_sign": -1,
            "publisher": "BLS 08:30 ET"},
}
#: Families the frozen challenger trades on. Declared, not selected.
TRADED_RELEASES = ("CPI", "EMPLOYMENT")

#: Release 45's declared forecast constants, never re-chosen.
FORECAST_WINDOW = 12
MIN_HISTORY = 24


def _key() -> Optional[str]:
    for n in KEY_ENV:
        v = os.environ.get(n)
        if v:
            return v
    return None


def _sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _manifest() -> dict:
    return read_json(manifest_path(), default=None) or {
        "schema": "r46_4_macro_captures/1", "captures": []}


def _fetch(url: str) -> Optional[bytes]:
    try:
        with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as fh:
            return fh.read()
    except Exception:                           # noqa: BLE001
        return None


def acquire(*, acquire: bool = True, today: _dt.date = None) -> dict:
    """Capture initial releases and release calendars. One per family per day."""
    raw_dir().mkdir(parents=True, exist_ok=True)
    now = CK.now_utc()
    today = today or CK.eastern_date(now)
    key = _key()
    man = _manifest()
    caps = list(man.get("captures") or [])
    have = {(c.get("family"), c.get("kind")) for c in caps
            if str(c.get("acquired_day")) == str(today)}
    results = []
    for fam, meta in RELEASES.items():
        for kind in ("INITIAL_RELEASES", "RELEASE_DATES"):
            rec = {"family": fam, "kind": kind}
            if (fam, kind) in have:
                rec["state"] = "ALREADY_CAPTURED_TODAY"
            elif not key:
                rec["state"] = "KEY_NOT_IN_THIS_SHELL"
            elif not acquire:
                rec["state"] = "NOT_ACQUIRED"
            else:
                if kind == "INITIAL_RELEASES":
                    url = ("%sseries/observations?series_id=%s&api_key=%s"
                           "&file_type=json&output_type=4"
                           "&realtime_start=1776-07-04&realtime_end=9999-12-31"
                           % (FRED, meta["series"], key))
                else:
                    url = ("%srelease/dates?release_id=%d&api_key=%s"
                           "&file_type=json&include_release_dates_with_no_data"
                           "=true&realtime_start=1776-07-04"
                           "&realtime_end=9999-12-31&limit=10000"
                           % (FRED, meta["release_id"], key))
                body = _fetch(url)
                if body is None:
                    rec["state"] = "FETCH_FAILED"
                else:
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except ValueError:
                        payload = {}
                    rows = (payload.get("observations")
                            or payload.get("release_dates") or [])
                    stamp = now.strftime("%Y%m%dT%H%M%SZ")
                    dest = raw_dir() / ("%s_%s_%s.json" % (fam, kind.lower(),
                                                         stamp))
                    dest.write_bytes(body)
                    cap = {"family": fam, "kind": kind, "path": str(dest),
                           "bytes": len(body), "sha256": _sha(body),
                           "n_rows": len(rows), "acquired_day": str(today),
                           "acquired_at_utc": CK.iso(now),
                           "acquired_at_utc_precise": CK.iso_precise(now),
                           "series": meta["series"],
                           "release_id": meta["release_id"],
                           "licence": "FRED/ALFRED, free API, owned key"}
                    caps.append(cap)
                    rec.update({k: v for k, v in cap.items() if k != "path"})
                    rec["state"] = "CAPTURED"
            results.append(rec)
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"results": results, "n_captures": len(caps),
            "key_present": bool(key), "credential_written": False,
            "money_spent_usd": 0.0}


def _latest(fam: str, kind: str) -> Optional[dict]:
    caps = [c for c in (_manifest().get("captures") or ())
            if c.get("family") == fam and c.get("kind") == kind
            and Path(c["path"]).exists()]
    return max(caps, key=lambda c: c["acquired_at_utc"]) if caps else None


def initial_releases(fam: str, as_of: _dt.date) -> Optional[pd.DataFrame]:
    """First-published values PUBLISHED on or before ``as_of``."""
    cap = _latest(fam, "INITIAL_RELEASES")
    if cap is None:
        return None
    try:
        payload = json.loads(Path(cap["path"]).read_text(encoding="utf-8"))
    except Exception:                           # noqa: BLE001
        return None
    rows = []
    for o in payload.get("observations") or []:
        try:
            v = float(o["value"])
        except (KeyError, TypeError, ValueError):
            continue
        rs = str(o.get("realtime_start") or "")[:10]
        if rs and rs <= str(as_of):
            rows.append((pd.Timestamp(o["date"]), pd.Timestamp(rs), v))
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["date", "realtime_start", "value"])
    df = df.sort_values(["date", "realtime_start"]).drop_duplicates(
        subset=["date"], keep="first").sort_values("date")
    return df.reset_index(drop=True)


def release_dates(fam: str) -> list:
    cap = _latest(fam, "RELEASE_DATES")
    if cap is None:
        return []
    try:
        payload = json.loads(Path(cap["path"]).read_text(encoding="utf-8"))
    except Exception:                           # noqa: BLE001
        return []
    out = set()
    for r in payload.get("release_dates") or []:
        d = str(r.get("date") or "")[:10]
        if d:
            out.add(d)
    return sorted(out)


def released_on(day: _dt.date, families=None) -> list:
    fams = list(families or RELEASES)
    return [f for f in fams if str(day) in set(release_dates(f))]


def next_release(fam: str, after: _dt.date) -> Optional[str]:
    return next((d for d in release_dates(fam) if d > str(after)), None)


def surprise(fam: str, as_of: _dt.date) -> dict:
    """Model-based surprise of the LATEST print published on or before as_of.

    Release 45's method: growth of the first-published value against the
    trailing mean growth of earlier first prints, standardised by the past
    growth standard deviation. Not a consensus surprise; never called one.
    """
    df = initial_releases(fam, as_of)
    if df is None or len(df) < MIN_HISTORY + 1:
        return {"family": fam, "state": "INSUFFICIENT_HISTORY",
                "n": 0 if df is None else int(len(df))}
    v = df["value"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        g = np.diff(np.log(np.where(v > 0, v, np.nan)))
    g = np.concatenate([[np.nan], g])
    i = len(df) - 1
    hist = g[max(1, i - FORECAST_WINDOW):i]
    hist = hist[np.isfinite(hist)]
    past = g[1:i]
    past = past[np.isfinite(past)]
    if hist.size < 6 or not np.isfinite(g[i]) or past.size <= 12:
        return {"family": fam, "state": "INSUFFICIENT_HISTORY",
                "n": int(len(df))}
    sd = float(past.std(ddof=1))
    z = (float(g[i] - hist.mean()) / sd) if sd > 0 else None
    return {"family": fam, "state": "OK",
            "period": str(df["date"].iloc[-1].date()),
            "published": str(df["realtime_start"].iloc[-1].date()),
            "value": float(v[-1]), "growth": float(g[i]),
            "forecast_growth": float(hist.mean()), "surprise_z": z,
            "n_history": int(len(df)),
            "model_based_not_consensus": True}


def rates_signal(as_of: _dt.date) -> dict:
    """The frozen rule's view for an emission on ``as_of``."""
    fams = released_on(as_of, TRADED_RELEASES)
    if not fams:
        return {"state": "NO_TRADED_RELEASE_TODAY", "as_of": str(as_of),
                "families": []}
    parts = []
    score = 0.0
    for f in fams:
        s = surprise(f, as_of)
        if s.get("state") != "OK" or s.get("published") != str(as_of):
            parts.append(dict(s, admissible=False,
                              why="print not published today or not captured"))
            continue
        z = s.get("surprise_z")
        if z is None:
            parts.append(dict(s, admissible=False, why="no z"))
            continue
        parts.append(dict(s, admissible=True, zn_sign=RELEASES[f]["zn_sign"]))
        score += RELEASES[f]["zn_sign"] * z
    if not any(p.get("admissible") for p in parts):
        return {"state": "RELEASE_NOT_ADMISSIBLE", "as_of": str(as_of),
                "families": fams, "parts": parts}
    return {"state": "OK", "as_of": str(as_of), "families": fams,
            "parts": parts, "zn_score": score,
            "direction": ("LONG" if score > 0 else "SHORT" if score < 0
                          else "FLAT")}


def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of)
    caps = _manifest().get("captures") or []
    cov = {}
    for fam in RELEASES:
        ir = initial_releases(fam, as_of)
        rd = release_dates(fam)
        cov[fam] = {
            "initial_releases_captured": ir is not None,
            "n_initial_releases": 0 if ir is None else int(len(ir)),
            "first_period": (None if ir is None
                             else str(ir["date"].iloc[0].date())),
            "latest_period": (None if ir is None
                              else str(ir["date"].iloc[-1].date())),
            "latest_published": (None if ir is None else str(
                ir["realtime_start"].iloc[-1].date())),
            "n_release_dates": len(rd),
            "next_release": next_release(fam, as_of),
            "latest_surprise": surprise(fam, as_of),
        }
    sig = rates_signal(as_of)
    ok = any(v["initial_releases_captured"] and v["n_release_dates"]
             for v in cov.values())
    state = ("LIVE_PROSPECTIVE" if ok and caps else
             "FROZEN_PENDING_EMISSION" if ok else "DATA_BLOCKED")
    body = artifact_body(
        "r46_4_macro_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=state,
        acquisition=acq,
        n_captures=len(caps),
        raw_root=str(raw_dir()),
        releases={k: dict(v) for k, v in RELEASES.items()},
        traded_releases=list(TRADED_RELEASES),
        coverage=cov,
        point_in_time={
            "initial_release_only": "ALFRED output_type=4; a revised value "
                                    "never replaces a first print",
            "publication_key": "realtime_start per observation",
            "release_timing": "08:30 ET prints; the tournament emits after "
                              "the close, so emitted_at > release by "
                              "construction and the row records both",
            "admissibility": "print published on the emission's Eastern date "
                             "and present in a capture acquired before the "
                             "emission instant",
            "consensus_surprise": "NOT_OWNED - the surprise is model-based",
        },
        forecast_constants={"window": FORECAST_WINDOW,
                            "min_history": MIN_HISTORY,
                            "source": "Release 45 declared constants"},
        signal_today=sig,
        challengers_frozen=["r46_4_macro_surprise_rates_5d"],
        information_family="MACRO_RELEASE_SURPRISE",
        money_spent_usd=0.0, credential_written=False,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir", "RELEASES",
           "TRADED_RELEASES", "acquire", "initial_releases", "release_dates",
           "released_on", "next_release", "surprise", "rates_signal", "run"]
