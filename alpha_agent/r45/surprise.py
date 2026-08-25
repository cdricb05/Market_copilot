"""alpha_agent.r45.surprise - Track G. Does the response scale with the news?

The honest problem with this track is stated before any number: the estate
does not own a PIT record of what the market EXPECTED. Consensus forecasts
are a paid product, and reconstructing them after the fact is exactly the
hindsight this project refuses to commit.

What IS obtainable free, and is genuinely point-in-time, is the value as
FIRST PUBLISHED - ALFRED's initial-release vintage, which by construction
contains no revision. From that, a causal forecast can be built out of
nothing but earlier initial releases, and the deviation of the release from
that forecast is a MODEL-BASED surprise.

That is not a consensus surprise and this module never calls it one. It
answers a narrower question honestly: was the number unusual relative to its
own recent history, and did the market's reaction scale with that?
"""
from __future__ import annotations

import os
import time

import numpy as np
import pandas as pd
import requests

from . import contract as C
from . import eventstudy as ES

CALCULATION_OWNER = "alpha_agent.r45.surprise"

#: One representative headline series per release family. Chosen because it
#: is the series the release is named for, not because of any result.
SERIES_FOR_EVENT = {
    "CPI": "CPIAUCSL",
    "EMPLOYMENT_SITUATION": "PAYEMS",
    "PPI": "PPIFIS",
    "GDP": "GDPC1",
    "RETAIL_SALES": "RSAFS",
    "PERSONAL_INCOME_PCE": "PCE",
    "INDUSTRIAL_PRODUCTION": "INDPRO",
}
#: The forecast is the mean growth of the trailing window, computed from
#: INITIAL RELEASES ONLY, and it is fixed here so it cannot be tuned.
FORECAST_WINDOW = 12
MIN_HISTORY = 24
SURPRISE_IS_MODEL_BASED_NOT_CONSENSUS = True


def _fred_initial_releases(series_id: str, *, pause: float = 0.4):
    """Every period's FIRST published value - ALFRED output_type=4."""
    key = os.environ.get("FRED_API_KEY") \
        or os.environ.get("PAPER_TRADER_FRED_API_KEY")
    if not key:
        return None, "ACCOUNT_REQUIRED"
    try:
        r = requests.get(
            "https://api.stlouisfed.org/fred/series/observations",
            params={"series_id": series_id, "api_key": key,
                    "file_type": "json", "output_type": 4,
                    "realtime_start": "1776-07-04",
                    "realtime_end": "9999-12-31"},
            timeout=90)
        time.sleep(pause)
        if r.status_code != 200:
            return None, f"HTTP_{r.status_code}"
        obs = r.json().get("observations") or []
    except Exception as exc:                            # pragma: no cover
        return None, f"ERROR {type(exc).__name__}"
    if not obs:
        return None, "HISTORICAL_DATA_UNAVAILABLE"
    df = pd.DataFrame(obs)
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["realtime_start"] = pd.to_datetime(df["realtime_start"])
    df = df.dropna(subset=["value"]).sort_values("realtime_start")
    return df[["date", "realtime_start", "value"]], "EXECUTED"


def standardized_surprise(series_id: str):
    """A causal, model-based surprise per release, in units of its own sd."""
    df, state = _fred_initial_releases(series_id)
    if df is None:
        return None, state
    df = df.sort_values("date").drop_duplicates(subset=["date"], keep="first")
    v = df["value"].to_numpy(dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        g = np.diff(np.log(np.where(v > 0, v, np.nan)))
    g = np.concatenate([[np.nan], g])
    out = []
    for i in range(len(df)):
        if i < MIN_HISTORY:
            out.append(np.nan)
            continue
        hist = g[max(1, i - FORECAST_WINDOW):i]
        hist = hist[np.isfinite(hist)]
        if hist.size < 6 or not np.isfinite(g[i]):
            out.append(np.nan)
            continue
        err = g[i] - float(hist.mean())
        past = g[1:i]
        past = past[np.isfinite(past)]
        sd = float(past.std(ddof=1)) if past.size > 12 else np.nan
        out.append(err / sd if (np.isfinite(sd) and sd > 0) else np.nan)
    df = df.copy()
    df["surprise_z"] = out
    return df[["date", "realtime_start", "value", "surprise_z"]], "EXECUTED"


def attach_surprise(ev: pd.DataFrame) -> pd.DataFrame:
    """Join each event to the surprise of the number it released.

    The join is on the RELEASE date, matched to the vintage whose
    ``realtime_start`` equals it - so an event can only ever see the number
    that was published that morning.
    """
    cache, rows = {}, []
    for name, grp in ev.groupby("event"):
        sid = SERIES_FOR_EVENT.get(name)
        if sid is None:
            continue
        if sid not in cache:
            cache[sid] = standardized_surprise(sid)
        tab, state = cache[sid]
        if tab is None:
            continue
        t = tab.dropna(subset=["surprise_z"]).copy()
        t["rt"] = pd.to_datetime(t["realtime_start"]).dt.normalize()
        # One release day can publish several periods at once (an annual
        # revision alongside the new month). The observation the market is
        # reacting to is the NEWEST period, so that is the one kept - and
        # keeping exactly one makes the join well defined.
        t = (t.sort_values(["rt", "date"])
              .drop_duplicates(subset=["rt"], keep="last"))
        m = t.set_index("rt")["surprise_z"]
        g = grp.copy()
        g["surprise_z"] = pd.to_datetime(g["date"]).dt.normalize().map(m)
        g["surprise_series"] = sid
        rows.append(g)
    if not rows:
        return None
    out = pd.concat(rows).sort_values("stamp_utc")
    out.attrs.update(ev.attrs)
    return out


def run(symbol: str = None, stamps=None, *, charge=None) -> dict:
    symbol = symbol or C.FROZEN_RULE["instrument_of_origin"]
    stamps = stamps if stamps is not None else ES.release_stamps()
    if stamps is None:
        return {"track": "G", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    ev = ES.event_book(symbol, stamps)
    if ev is None:
        return {"track": "G", "state": "HISTORICAL_DATA_UNAVAILABLE"}
    joined = attach_surprise(ev)
    if joined is None:
        return {"track": "G", "state": "PIT_EXPECTATION_DATA_UNAVAILABLE",
                "why": "no initial-release vintage could be reached"}
    have = joined.dropna(subset=["surprise_z"])
    if len(have) < 90:
        return {"track": "G", "state": "PIT_EXPECTATION_DATA_UNAVAILABLE",
                "n_matched": int(len(have)),
                "why": "too few events could be matched to an initial "
                       "release vintage to say anything"}

    z = have["surprise_z"].abs()
    edges = z.quantile([0.0, 1 / 3, 2 / 3, 1.0]).to_numpy()
    buckets = []
    for i, (lo, hi) in enumerate(zip(edges[:-1], edges[1:])):
        sel = (z >= lo) & (z <= hi) if i == 2 else (z >= lo) & (z < hi)
        sub = have[sel].copy()
        sub.attrs.update(have.attrs)
        if len(sub) < 25:
            continue
        card = ES.score(sub, label=f"SURPRISE_TERCILE_{i + 1}")
        buckets.append({
            "tercile": i + 1, "abs_z_range": [float(lo), float(hi)],
            "n_events": card["n_events"],
            "mean_abs_shock_bps": card["shock_bps_mean_abs"],
            "gross_bps_per_event": card["gross_bps_per_event"],
            "net_bps_per_event": card["net_bps_per_event"],
            "net_t_cluster": card["net_t_cluster"],
        })

    shock = have["shock"].abs().to_numpy(dtype=float) * 1e4
    zz = z.to_numpy(dtype=float)
    ok = np.isfinite(shock) & np.isfinite(zz)
    corr = float(np.corrcoef(zz[ok], shock[ok])[0, 1]) if ok.sum() > 30 \
        else None

    charged = []
    if charge is not None:
        charged.append(charge(
            {"lane": "SURPRISE_MAGNITUDE", "symbol": symbol,
             "measure": "model_based_initial_release_z",
             "window": FORECAST_WINDOW, "buckets": 3},
            family="EVENT_STATE_CONDITIONING", lane="L9_SURPRISE",
            label="response scaling in model-based surprise"))

    return {
        "track": "G", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "symbol": symbol,
        "surprise_is_model_based_not_consensus":
            SURPRISE_IS_MODEL_BASED_NOT_CONSENSUS,
        "what_this_is": "the deviation of the FIRST PUBLISHED value from a "
                        "trailing-mean forecast of its own initial releases, "
                        "standardized. It is not a consensus surprise and no "
                        "consensus history was reconstructed.",
        "series_used": SERIES_FOR_EVENT,
        "n_events_matched": int(len(have)),
        "n_events_total": int(len(ev)),
        "abs_surprise_vs_abs_shock_correlation": corr,
        "terciles": buckets,
        "reaction_scales_with_surprise": bool(
            corr is not None and corr > 0.10),
        "burden_charged": charged,
    }
