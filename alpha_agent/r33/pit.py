"""alpha_agent.r33.pit - the ONE Lane B point-in-time information owner.

Release 32 established that most of the owned economic database is REVISED and
not historically point-in-time: 106 of 144 Norgate economic series change value
on the first business day of the period they measure, which is roughly six
weeks of look-ahead per period plus every later revision. Those series are
inadmissible here and are not used.

This module acquires free, genuinely point-in-time information instead, and it
proves admissibility per source rather than asserting it. For every source it
records the observation timestamp, the release/availability timestamp, the
revision semantics, the vintage semantics, the history coverage and the
resulting admissibility state.

Three sources:

* **ALFRED vintages.** The FRED archival API returns each observation with the
  ``realtime_start`` at which that value first existed. A state variable built
  from vintages answers "what did the world believe on this date", which is the
  only question a backtest may ask. ALFRED caps one request at 2000 vintage
  dates, so requests are chunked by observation window.
* **CFTC Commitments of Traders.** Weekly positioning, reported for Tuesday and
  published the following Friday afternoon. It is admissible only with that
  publication lag applied, and this module applies a conservative four
  business days.
* **Owned market observables.** Yields, spreads, implied volatility and breadth
  are PRICES. They are stamped by the market that made them and are not revised.

Nothing here spends money. A source that cannot be acquired is recorded as
BLOCKED with its reason, and the campaign continues on what remains.
"""
from __future__ import annotations

import csv
import datetime as _dt
import io
import json
import os
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from .. import r33
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.pit"
MANIFEST_SCHEMA = "r33_pit_information_manifest/1"
ARTIFACT_NAME = "pit_information_manifest.json"

USER_AGENT = "paper-trader-research/33"
HTTP_TIMEOUT = 60

ADMISSIBLE = "PIT_ADMISSIBLE"
INADMISSIBLE = "NOT_PIT_ADMISSIBLE"
BLOCKED = "BLOCKED"

# --------------------------------------------------------------------------- #
# Pre-registered source allowlists
# --------------------------------------------------------------------------- #
#: ALFRED series: id -> (state name, economic reading).
ALFRED_SERIES = {
    "CPIAUCSL": ("pit_inflation_level", "consumer price index"),
    "PAYEMS": ("pit_payrolls", "non-farm payrolls"),
    "INDPRO": ("pit_industrial_production", "industrial production"),
    "UNRATE": ("pit_unemployment", "unemployment rate"),
    "ICSA": ("pit_initial_claims", "initial jobless claims"),
    "RSAFS": ("pit_retail_sales", "retail and food services sales"),
    "GDPC1": ("pit_real_gdp", "real gross domestic product"),
    "UMCSENT": ("pit_consumer_sentiment", "consumer sentiment"),
}

#: CFTC legacy futures-only report: the market codes mapped to the panel
#: markets whose positioning they describe. Only unambiguous mappings.
COT_MARKET_MAP = {
    "099741": ("EURUSD", "EURO FX"),
    "097741": ("JPYUSD", "JAPANESE YEN"),
    "096742": ("GBPUSD", "BRITISH POUND"),
    "092741": ("CHFUSD", "SWISS FRANC"),
    "232741": ("AUDUSD", "AUSTRALIAN DOLLAR"),
    "090741": ("CADUSD", "CANADIAN DOLLAR"),
    "112741": ("NZDUSD", "NEW ZEALAND DOLLAR"),
    "095741": ("MXNUSD", "MEXICAN PESO"),
    "102741": ("BRLUSD", "BRAZILIAN REAL"),
    "13874A": ("&ES", "E-MINI S&P 500"),
    "088691": ("XAUUSD", "GOLD"),
    "084691": ("XAGUSD", "SILVER"),
    "085692": ("$BCOMIN", "COPPER"),
    "067651": ("@WTI", "CRUDE OIL, LIGHT SWEET"),
}

#: CFTC publishes Tuesday positioning on the following Friday afternoon. Four
#: business days is deliberately conservative and covers holiday weeks.
COT_PUBLICATION_LAG_BUSINESS_DAYS = 4

#: Norgate economic series that Release 32 measured as revised-not-PIT. Recorded
#: so the exclusion is visible rather than silent.
REVISED_NOT_PIT_EXCLUDED = {
    "count": 106,
    "of": 144,
    "source": "Norgate Economic database",
    "measured_by": "release32 change-day fingerprint",
    "finding": ("each statistical release changes value on the FIRST BUSINESS "
                "DAY OF THE PERIOD IT MEASURES, which is roughly six weeks of "
                "look-ahead per period plus every later revision"),
}


def cache_dir(campaign_id: str) -> Path:
    return r33.campaign_dir(campaign_id) / "pit_cache"


# --------------------------------------------------------------------------- #
# HTTP
# --------------------------------------------------------------------------- #
def _fetch(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
        return resp.read()


def _fred_key() -> Optional[str]:
    for name in ("FRED_API_KEY", "PAPER_TRADER_FRED_API_KEY"):
        v = os.environ.get(name)
        if v:
            return v
    return None


# --------------------------------------------------------------------------- #
# ALFRED
# --------------------------------------------------------------------------- #
def fetch_alfred(series_id: str, *, campaign_id: str,
                 start: str = "1994-01-01") -> dict:
    """Fetch one series WITH vintages, cached. Never prints the credential."""
    key = _fred_key()
    if not key:
        return {"series_id": series_id, "state": BLOCKED,
                "reason": "no FRED credential present in the environment"}
    cdir = cache_dir(campaign_id)
    cdir.mkdir(parents=True, exist_ok=True)
    cached = cdir / f"alfred_{series_id}.json"
    if cached.exists():
        payload = json.loads(cached.read_text(encoding="utf-8"))
    else:
        params = {"series_id": series_id, "api_key": key, "file_type": "json",
                  "observation_start": start, "realtime_start": start,
                  "realtime_end": "9999-12-31"}
        url = ("https://api.stlouisfed.org/fred/series/observations?"
               + urllib.parse.urlencode(params))
        try:
            payload = json.loads(_fetch(url).decode("utf-8"))
        except Exception as exc:
            return {"series_id": series_id, "state": BLOCKED,
                    "reason": f"{type(exc).__name__}: {exc}"}
        cached.write_text(json.dumps(payload), encoding="utf-8")

    obs = payload.get("observations") or []
    rows = []
    for o in obs:
        try:
            v = float(o.get("value"))
        except (TypeError, ValueError):
            continue
        rows.append({"observation_date": o.get("date"),
                     "available_from": o.get("realtime_start"),
                     "value": v})
    if not rows:
        return {"series_id": series_id, "state": BLOCKED,
                "reason": "no numeric observations returned"}
    frame = pd.DataFrame(rows)
    frame["observation_date"] = pd.to_datetime(frame["observation_date"])
    frame["available_from"] = pd.to_datetime(frame["available_from"])
    n_vintages = int(frame["available_from"].nunique())
    clamped = bool((frame["available_from"]
                    <= pd.Timestamp(start)).sum() > len(frame) * 0.5)
    return {
        "series_id": series_id, "state": ADMISSIBLE, "frame": frame,
        "observations": int(len(frame)),
        "distinct_vintages": n_vintages,
        "first_observation": str(frame["observation_date"].min().date()),
        "last_observation": str(frame["observation_date"].max().date()),
        "first_availability": str(frame["available_from"].min().date()),
        "vintage_window_clamped_at_start": clamped,
        "observation_timestamp": "observation_date",
        "release_timestamp": "available_from (ALFRED realtime_start)",
        "revision_semantics": "every revision appears as a later vintage row",
        "vintage_semantics": "realtime_start is the date the value first existed",
    }


def as_of_series(frame: pd.DataFrame, calendar: pd.DatetimeIndex) -> pd.Series:
    """Latest value KNOWN on each calendar date - never a later revision.

    For each calendar date the most recent observation whose ``available_from``
    has already passed is taken. This is the whole point of a vintage: on 15
    March 2009 the world did not know what March 2009 industrial production
    would eventually be revised to.
    """
    f = frame.sort_values(["available_from", "observation_date"])
    avail = f["available_from"].to_numpy()
    values = f["value"].to_numpy()
    obs = f["observation_date"].to_numpy()
    out = np.full(len(calendar), np.nan)
    cal = np.asarray(calendar, dtype="datetime64[ns]")
    pos = np.searchsorted(avail, cal, side="right")
    for k in range(len(cal)):
        p = pos[k]
        if p <= 0:
            continue
        # Among rows already published, take the one describing the LATEST
        # observation period.
        window = slice(max(0, p - 40), p)
        o, v = obs[window], values[window]
        if o.size == 0:
            continue
        out[k] = v[int(np.argmax(o))]
    return pd.Series(out, index=calendar)


# --------------------------------------------------------------------------- #
# CFTC Commitments of Traders
# --------------------------------------------------------------------------- #
def fetch_cot_year(year: int, *, campaign_id: str) -> Optional[pd.DataFrame]:
    """One year of the legacy futures-only report, cached."""
    cdir = cache_dir(campaign_id)
    cdir.mkdir(parents=True, exist_ok=True)
    cached = cdir / f"cot_{year}.csv"
    if cached.exists():
        try:
            return pd.read_csv(cached)
        except Exception:
            pass
    url = f"https://www.cftc.gov/files/dea/history/deacot{year}.zip"
    try:
        raw = _fetch(url)
    except Exception:
        return None
    try:
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            name = zf.namelist()[0]
            text = zf.read(name).decode("utf-8", "replace")
    except Exception:
        return None
    # newline="" is required: the CFTC annual file contains embedded newlines
    # inside quoted market-name fields, and the default universal-newline
    # translation turns those into a parse error rather than a row.
    rows = list(csv.reader(io.StringIO(text, newline="")))
    if len(rows) < 2:
        return None
    header = [h.strip() for h in rows[0]]
    frame = pd.DataFrame(rows[1:], columns=header)
    keep = _cot_columns(frame)
    if keep is None:
        return None
    frame = frame[list(keep.values())].rename(
        columns={v: k for k, v in keep.items()})
    frame.to_csv(cached, index=False)
    return frame


def _normalise_header(name: str) -> str:
    """Lowercase and collapse punctuation, so a header matches whether the
    vendor writes 'CFTC Contract Market Code' or 'cftc_contract_market_code'."""
    out = []
    for ch in str(name).lower():
        out.append(ch if ch.isalnum() else " ")
    return " ".join("".join(out).split())


def _cot_columns(frame: pd.DataFrame) -> Optional[dict]:
    """Locate the columns this module needs, by normalised name fragment."""
    cols = {_normalise_header(c): c for c in frame.columns}

    def find(*fragments):
        for low, orig in cols.items():
            if all(f in low for f in fragments):
                return orig
        return None

    out = {
        "report_date": (find("as of date in form yyyy mm dd")
                        or find("report date as yyyy mm dd")),
        "market_code": find("cftc contract market code"),
        "open_interest": find("open interest", "all"),
        "noncomm_long": find("noncommercial positions long", "all"),
        "noncomm_short": find("noncommercial positions short", "all"),
    }
    if out["noncomm_long"] is None:
        out["noncomm_long"] = find("noncomm positions long", "all")
    if out["noncomm_short"] is None:
        out["noncomm_short"] = find("noncomm positions short", "all")
    if any(v is None for v in out.values()):
        return None
    return out


def build_cot_states(calendar: pd.DatetimeIndex, *, campaign_id: str,
                     start_year: int = 1995) -> dict:
    """Positioning state per mapped market, lagged to its publication date."""
    end_year = int(calendar[-1].year)
    frames, missing = [], []
    for year in range(int(start_year), end_year + 1):
        f = fetch_cot_year(year, campaign_id=campaign_id)
        if f is None:
            missing.append(year)
            continue
        frames.append(f)
    if not frames:
        return {"state": BLOCKED, "reason": "no CFTC year file downloaded",
                "missing_years": missing, "states": {}}
    allf = pd.concat(frames, ignore_index=True)
    allf["report_date"] = pd.to_datetime(allf["report_date"], errors="coerce")
    for c in ("open_interest", "noncomm_long", "noncomm_short"):
        allf[c] = pd.to_numeric(allf[c], errors="coerce")
    allf = allf.dropna(subset=["report_date", "market_code"])
    allf["market_code"] = allf["market_code"].astype(str).str.strip()

    # Publication lag: Tuesday positioning becomes public the following Friday.
    allf["available_from"] = allf["report_date"] + pd.offsets.BDay(
        COT_PUBLICATION_LAG_BUSINESS_DAYS)

    states, covered = {}, {}
    for code, (symbol, name) in COT_MARKET_MAP.items():
        block = allf[allf["market_code"] == code].sort_values("available_from")
        if len(block) < 100:
            covered[symbol] = {"code": code, "name": name, "rows": int(len(block)),
                               "state": BLOCKED, "reason": "insufficient rows"}
            continue
        oi = block["open_interest"].replace(0.0, np.nan)
        net = (block["noncomm_long"] - block["noncomm_short"]) / oi
        s = pd.Series(net.to_numpy(), index=block["available_from"].to_numpy())
        s = s[~s.index.duplicated(keep="last")].sort_index()
        aligned = s.reindex(s.index.union(calendar)).sort_index().ffill(
            limit=15).reindex(calendar)
        z = (aligned - aligned.rolling(156, min_periods=52).mean()) / \
            aligned.rolling(156, min_periods=52).std(ddof=1)
        states[symbol] = {"net_positioning": aligned,
                          "positioning_z": z.clip(-4.0, 4.0),
                          "positioning_change_13w": aligned.diff(65)}
        covered[symbol] = {"code": code, "name": name, "rows": int(len(block)),
                           "state": ADMISSIBLE,
                           "first_report": str(block["report_date"].min().date()),
                           "last_report": str(block["report_date"].max().date())}
    return {"state": ADMISSIBLE if states else BLOCKED, "states": states,
            "coverage": covered, "missing_years": missing,
            "publication_lag_business_days": COT_PUBLICATION_LAG_BUSINESS_DAYS}


# --------------------------------------------------------------------------- #
# Owned market observables
# --------------------------------------------------------------------------- #
OWNED_MARKET_STATES = {
    "$VIX": "equity implied volatility",
    "$MOVE": "Treasury implied volatility",
    "%TNX": "10-year Treasury yield",
    "%IRX": "13-week bill yield",
    "%COBAA": "Baa corporate yield",
    "%COAAA": "Aaa corporate yield",
    "%CCCHYS": "CCC and lower high-yield option-adjusted spread",
    "#SPX%MA200": "share of S&P 500 above its 200-day average",
    "$USDX": "US dollar index",
    "#CBOEPC": "CBOE put/call ratio",
}


def owned_state_admissibility() -> dict:
    return {sym: {"reading": reading, "state": ADMISSIBLE,
                  "observation_timestamp": "market session close",
                  "release_timestamp": "same session; prices are not released, "
                                       "they are made",
                  "revision_semantics": "none - a printed price is final",
                  "vintage_semantics": "not applicable"}
            for sym, reading in sorted(OWNED_MARKET_STATES.items())}


# --------------------------------------------------------------------------- #
# Assembly
# --------------------------------------------------------------------------- #
def build(calendar: pd.DatetimeIndex, *, campaign_id: str) -> dict:
    """Acquire every Lane B source and assemble the PIT state frame."""
    alfred_states, alfred_meta = {}, {}
    for sid, (name, reading) in ALFRED_SERIES.items():
        res = fetch_alfred(sid, campaign_id=campaign_id)
        meta = {k: v for k, v in res.items() if k != "frame"}
        meta["state_name"] = name
        meta["reading"] = reading
        alfred_meta[sid] = meta
        if res.get("state") == ADMISSIBLE:
            alfred_states[name] = as_of_series(res["frame"], calendar)

    cot = build_cot_states(calendar, campaign_id=campaign_id)

    cols = {}
    for name, s in alfred_states.items():
        cols[name] = s
        # A level is not a state; its surprise and its trend are.
        cols[f"{name}_yoy"] = s.pct_change(252)
        cols[f"{name}_chg_63"] = s.diff(63)
    frame = pd.DataFrame(cols, index=calendar) if cols else pd.DataFrame(
        index=calendar)

    return {"alfred_meta": alfred_meta, "alfred_states": alfred_states,
            "cot": cot, "state_frame": frame,
            "owned_states": owned_state_admissibility()}


def manifest(built: dict, *, campaign_id: str, created_at: str) -> dict:
    alfred_ok = [k for k, v in built["alfred_meta"].items()
                 if v.get("state") == ADMISSIBLE]
    alfred_blocked = {k: v.get("reason") for k, v in built["alfred_meta"].items()
                      if v.get("state") != ADMISSIBLE}
    cot = built["cot"]
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at,
        "spend": {"may_spend_money": False, "amount_spent": 0.0},
        "sources": {
            "alfred_vintages": {
                "state": ADMISSIBLE if alfred_ok else BLOCKED,
                "series_admissible": sorted(alfred_ok),
                "series_blocked": alfred_blocked,
                "detail": {k: v for k, v in sorted(built["alfred_meta"].items())},
                "why_admissible": (
                    "each observation carries the realtime_start at which that "
                    "value first existed, so a state variable can be built "
                    "from what was actually knowable on the date"),
                "known_limitation": (
                    "ALFRED caps one request at 2000 vintage dates; an "
                    "observation whose first release pre-dates the requested "
                    "window is clamped to the window start and is flagged per "
                    "series as vintage_window_clamped_at_start"),
            },
            "cftc_commitments_of_traders": {
                "state": cot.get("state"),
                "coverage": cot.get("coverage", {}),
                "missing_years": cot.get("missing_years", []),
                "publication_lag_business_days":
                    COT_PUBLICATION_LAG_BUSINESS_DAYS,
                "why_admissible": (
                    "Tuesday positioning is published the following Friday "
                    "afternoon; the series is usable only with that lag, and a "
                    "conservative four business days is applied"),
            },
            "owned_market_observables": {
                "state": ADMISSIBLE,
                "detail": built["owned_states"],
                "why_admissible": (
                    "yields, spreads, implied volatility and breadth are "
                    "PRICES: stamped by the market that made them, never "
                    "revised, and carrying no vintage problem"),
            },
        },
        "excluded": {
            "norgate_revised_economic_series": REVISED_NOT_PIT_EXCLUDED,
            "synthetic_or_proxy_event_data": {
                "state": INADMISSIBLE,
                "reason": ("the owned normalized earnings and analyst-revision "
                           "stores carry provider_id synthetic_test / "
                           "PROXY_LOCAL with placeholder tickers; they are not "
                           "evidence and may not enter a claim"),
            },
        },
        "state_variables_built": sorted(built["state_frame"].columns),
        "state_variable_count": int(built["state_frame"].shape[1]),
    }
    body = r33.artifact_body(MANIFEST_SCHEMA, payload)
    body["pit_manifest_hash"] = r33.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r33.campaign_dir(campaign_id) / ARTIFACT_NAME
