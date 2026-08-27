"""alpha_agent.r46.credit - the credit-state lane. Free, keyed, PIT-stamped.

Credit is a distinct market's price of the same risk equities price, and no
active Release-46 cell reads it. Two sources the estate already holds:

* **FRED / ALFRED through the owned key** - the ICE BofA US High Yield and
  Investment Grade option-adjusted spreads (``BAMLH0A0HYM2``, ``BAMLC0A0CM``;
  served for a rolling three-year window only, a licence cap Release 36
  measured), Moody's BAA over the 10-year (``BAA10Y``, free since 1986), the
  Chicago Fed NFCI and the 10y-2y slope. Every observation is requested with
  its ALFRED vintage window, so each row carries ``realtime_start`` - the
  date it was first published - and that date is the point-in-time key.
  The OAS series publish the previous session's value the next business
  day: the value dated ``d`` carries ``realtime_start = d+1``, and a signal
  built at the close of ``d`` may not see it. That lag is enforced here, not
  assumed away.
* **Owned Norgate economic series** as the fallback and cross-check: the
  CCC-and-lower HY OAS (``%CCCHYS``) and Moody's BAA / AAA yields, which the
  estate already reads for the regime owner.

Raw captures are appended under the Release-46 research root with the
acquisition instant and sha256; nothing is overwritten. Revised history is
never substituted for what was contemporaneous.

Two bounded challengers, frozen before their first emission: a credit-regime
equity timing rule (long SPY only when the HY spread sits below its 63-session
mean, against the SPY buy-and-hold control) and a credit momentum rule (long
HYG / short LQD when the 21-session change in the HY spread is negative,
reversed otherwise, against cash).
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import urllib.request
from pathlib import Path
from typing import Optional

import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import clock as CK
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.credit"

ARTIFACT = "R46_4_CREDIT_LANE.json"
RAW_DIRNAME = "_data_credit"
MANIFEST_NAME = "credit_captures.json"

#: ALFRED serves at most 100,000 rows per request; a long, heavily revised
#: series (NFCI is revised weekly) overflows that with vintage rows and the
#: response is silently TRUNCATED at 2015. Each series therefore declares
#: the observation and vintage windows it needs, and a capture that returns
#: exactly the cap is flagged truncated and never treated as complete.
ALFRED_ROW_CAP = 100000
SERIES_WINDOWS = {
    "HY_OAS": ("2010-01-01", "1776-07-04"),
    "IG_OAS": ("2010-01-01", "1776-07-04"),
    "BAA10Y": ("2015-01-01", "2015-01-01"),
    "NFCI": ("2018-01-01", "2018-01-01"),
    "T10Y2Y": ("2015-01-01", "2015-01-01"),
}


def raw_dir() -> Path:
    """Resolved at CALL time from the package root (hermetic under a test
    root); production is the R46 research root."""
    from . import RESEARCH_ROOT as _root
    return Path(_root) / RAW_DIRNAME


def manifest_path() -> Path:
    return raw_dir() / MANIFEST_NAME

FRED_URL = "https://api.stlouisfed.org/fred/series/observations"
KEY_ENV = ("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY")
HTTP_TIMEOUT = 90

SERIES = {
    "HY_OAS": {"id": "BAMLH0A0HYM2", "name": "ICE BofA US High Yield OAS",
               "licence_cap": "rolling three years through the API"},
    "IG_OAS": {"id": "BAMLC0A0CM", "name": "ICE BofA US Corporate OAS",
               "licence_cap": "rolling three years through the API"},
    "BAA10Y": {"id": "BAA10Y", "name": "Moody's BAA minus 10y Treasury",
               "licence_cap": None},
    "NFCI": {"id": "NFCI", "name": "Chicago Fed NFCI (weekly, revised)",
             "licence_cap": None},
    "T10Y2Y": {"id": "T10Y2Y", "name": "10y minus 2y Treasury",
               "licence_cap": None},
}
OWNED_FALLBACK = {"HY_OAS": "%CCCHYS", "BAA10Y": ("%COBAA", "%10YTCM")}

#: Declared windows - canonical constants, not searched.
MEAN_WINDOW = 63
CHANGE_WINDOW = 21
MIN_OBS = MEAN_WINDOW + 5


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
        "schema": "r46_4_credit_captures/1", "captures": []}


def acquire(*, acquire: bool = True, today: _dt.date = None,
            force: bool = False) -> dict:
    """Capture every series with its ALFRED vintage stamps. Append-only.

    One capture per series per calendar day: a second run on the same day
    reads the capture already on disk rather than re-requesting it.
    """
    raw_dir().mkdir(parents=True, exist_ok=True)
    now = CK.now_utc()
    today = today or CK.eastern_date(now)
    key = _key()
    man = _manifest()
    caps = list(man.get("captures") or [])
    have_today = {c.get("series_key") for c in caps
                  if str(c.get("acquired_day")) == str(today)
                  and not c.get("truncated") and not force}
    results = []
    for skey, meta in SERIES.items():
        rec = {"series_key": skey, "series_id": meta["id"]}
        if skey in have_today:
            rec["state"] = "ALREADY_CAPTURED_TODAY"
        elif not key:
            rec["state"] = "KEY_NOT_IN_THIS_SHELL"
        elif not acquire:
            rec["state"] = "NOT_ACQUIRED"
        else:
            obs_start, rt_start = SERIES_WINDOWS.get(
                skey, ("2010-01-01", "1776-07-04"))
            url = ("%s?series_id=%s&api_key=%s&file_type=json"
                   "&realtime_start=%s&realtime_end=9999-12-31"
                   "&observation_start=%s&limit=%d"
                   % (FRED_URL, meta["id"], key, rt_start, obs_start,
                      ALFRED_ROW_CAP))
            try:
                with urllib.request.urlopen(url, timeout=HTTP_TIMEOUT) as fh:
                    body = fh.read()
                payload = json.loads(body.decode("utf-8"))
                obs = payload.get("observations") or []
                stamp = now.strftime("%Y%m%dT%H%M%SZ")
                dest = raw_dir() / ("%s_%s.json" % (meta["id"], stamp))
                dest.write_bytes(body)
                cap = {"series_key": skey, "series_id": meta["id"],
                       "path": str(dest), "bytes": len(body),
                       "sha256": _sha(body), "n_observations": len(obs),
                       "truncated": bool(len(obs) >= ALFRED_ROW_CAP),
                       "observation_start": obs_start,
                       "vintage_start": rt_start,
                       "first_date": obs[0]["date"] if obs else None,
                       "last_date": obs[-1]["date"] if obs else None,
                       "acquired_day": str(today),
                       "acquired_at_utc": CK.iso(now),
                       "acquired_at_utc_precise": CK.iso_precise(now),
                       "vintage_stamped": True,
                       "licence": "FRED/ALFRED, free API, owned key",
                       "licence_cap": meta.get("licence_cap")}
                caps.append(cap)
                rec.update({k: v for k, v in cap.items() if k != "path"})
                rec["state"] = "CAPTURED"
            except Exception as exc:            # noqa: BLE001 - reported
                rec["state"] = "FETCH_FAILED"
                rec["error"] = "%s" % type(exc).__name__
        results.append(rec)
    man["captures"] = caps
    man["n_captures"] = len(caps)
    man["updated_at_utc"] = CK.iso(now)
    write_json(manifest_path(), man)
    return {"results": results, "n_captures": len(caps),
            "key_present": bool(key), "credential_written": False,
            "money_spent_usd": 0.0}


def latest_capture(series_key: str) -> Optional[dict]:
    caps = [c for c in (_manifest().get("captures") or ())
            if c.get("series_key") == series_key and Path(c["path"]).exists()
            and not c.get("truncated")]
    return max(caps, key=lambda c: c["acquired_at_utc"]) if caps else None


def pit_series(series_key: str, as_of: _dt.date) -> Optional[pd.Series]:
    """Observations PUBLISHED on or before ``as_of`` - the vintage current
    at as_of - indexed by observation date. ``None`` if no capture."""
    cap = latest_capture(series_key)
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
        re_ = str(o.get("realtime_end") or "9999-12-31")[:10]
        if rs and rs <= str(as_of) <= re_:
            rows.append((pd.Timestamp(o["date"]), v, rs))
    if not rows:
        return None
    df = pd.DataFrame(rows, columns=["date", "value", "published"])
    df = df.sort_values(["date", "published"]).drop_duplicates(
        "date", keep="last").set_index("date")
    s = df["value"]
    s.attrs["published_latest"] = str(df["published"].max())
    s.attrs["source"] = "FRED/ALFRED vintage current at %s" % as_of
    return s


def owned_series(series_key: str, as_of: _dt.date) -> Optional[pd.Series]:
    from . import marketdata as MD
    fb = OWNED_FALLBACK.get(series_key)
    if fb is None:
        return None
    if isinstance(fb, tuple):
        a, b = MD.closes(fb[0]), MD.closes(fb[1])
        if a is None or b is None:
            return None
        j = a.align(b, join="inner")
        s = (j[0] - j[1]).dropna()
    else:
        s = MD.closes(fb)
    if s is None or not len(s):
        return None
    s = s[[ts.date() <= as_of for ts in s.index]]
    if not len(s):
        return None
    s.attrs["source"] = "owned Norgate economic series %s" % (fb,)
    return s


def state(as_of: _dt.date) -> dict:
    """Credit-state features at ``as_of`` from the PIT vintage, with source."""
    hy = pit_series("HY_OAS", as_of)
    src = "FRED_ALFRED_PIT"
    if hy is None or len(hy) < MIN_OBS:
        hy = owned_series("HY_OAS", as_of)
        src = "OWNED_NORGATE_FALLBACK"
    if hy is None or len(hy) < MIN_OBS:
        return {"state": "DATA_BLOCKED", "as_of": str(as_of),
                "reason": "no credit series with %d observations" % MIN_OBS}
    last = float(hy.iloc[-1])
    mean = float(hy.iloc[-MEAN_WINDOW:].mean())
    chg = float(hy.iloc[-1] - hy.iloc[-1 - CHANGE_WINDOW])
    return {"state": "OK", "as_of": str(as_of), "source": src,
            "series_last_observation": str(hy.index[-1].date()),
            "published_by": hy.attrs.get("published_latest"),
            "hy_oas": last, "hy_oas_mean_63": mean,
            "hy_below_mean": bool(last < mean),
            "hy_oas_change_21": chg, "hy_tightening": bool(chg < 0),
            "n_observations": int(len(hy))}


def run(*, acquire_now: bool = True, campaign_id: str = CAMPAIGN_ID,
        as_of: _dt.date = None) -> dict:
    now = CK.now_utc()
    as_of = as_of or CK.eastern_date(now)
    acq = acquire(acquire=acquire_now, today=as_of)
    st = state(as_of)
    caps = _manifest().get("captures") or []
    coverage = {}
    for skey in SERIES:
        cap = latest_capture(skey)
        coverage[skey] = ({"captured": True, "first_date": cap.get("first_date"),
                           "last_date": cap.get("last_date"),
                           "n_observations": cap.get("n_observations"),
                           "acquired_at_utc": cap.get("acquired_at_utc")}
                          if cap else {"captured": False})
    lane_state = ("LIVE_PROSPECTIVE" if st.get("state") == "OK" and caps
                  else "FROZEN_PENDING_EMISSION" if st.get("state") == "OK"
                  else "DATA_BLOCKED")
    body = artifact_body(
        "r46_4_credit_lane/1", CALCULATION_OWNER,
        as_of=str(as_of),
        state=lane_state,
        acquisition=acq,
        n_captures=len(caps),
        raw_root=str(raw_dir()),
        series={k: dict(v) for k, v in SERIES.items()},
        owned_fallback={k: (list(v) if isinstance(v, tuple) else v)
                        for k, v in OWNED_FALLBACK.items()},
        coverage=coverage,
        point_in_time={
            "key": "ALFRED realtime_start (first publication date) per "
                   "observation; only observations published on or before "
                   "the decision session are used",
            "oas_publication_lag": "one business day - the value dated d is "
                                   "first published d+1 and is not visible "
                                   "at the close of d",
            "revised_history_never_substituted": True,
            "nfci_is_revised": "weekly revisions; the vintage current at the "
                               "decision date is used, never the final",
        },
        credit_state=st,
        economic_mapping={
            "equity_risk_timing": "SPY long only when HY OAS < 63-session "
                                  "mean; control SPY buy-and-hold",
            "credit_momentum": "long HYG / short LQD when the 21-session "
                               "change in HY OAS < 0, reversed otherwise; "
                               "control cash",
            "windows_are_declared_constants": True},
        challengers_frozen=["r46_4_credit_regime_spx_timing",
                            "r46_4_credit_hy_ig_momentum"],
        information_family="CREDIT_SPREADS",
        money_spent_usd=0.0, credential_written=False,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body


__all__ = ["CALCULATION_OWNER", "ARTIFACT", "raw_dir", "SERIES",
           "MEAN_WINDOW", "CHANGE_WINDOW", "acquire", "latest_capture",
           "pit_series", "owned_series", "state", "run"]
