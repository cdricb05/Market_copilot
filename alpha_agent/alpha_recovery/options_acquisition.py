r"""alpha_agent.alpha_recovery.options_acquisition - the ONE owner of the
MONEYNESS-ANCHORED SPY option surface: what was bought, why that band, and the
surface CSV it normalises to.

WHY THIS EXISTS
    ``options_surface`` - the campaign's axis A - has been blocked twice, and the
    second time the blocker was named precisely: the owned surface is a FIXED
    STRIKE BAND of 654-720 acquired for a single R45 event study. Over its
    sample the underlying rallied from 615 to 751, so the band drifted out of
    the money and only **25 of 264 dates** carry a near-dated expiry whose
    strikes bracket the money, against the frozen floor of 36. An ATM
    implied-volatility series cannot be built from a band the spot has left.

    The fix is not more dates. It is a band ANCHORED ON MONEYNESS that tracks
    the underlying, which is what the recorded requirement always said:
    "a +/-10 % moneyness band with two expiries beyond 18 days over >= 2 years".

WHY IT IS AFFORDABLE NOW, HAVING BEEN DO_NOT_BUY BEFORE
    The earlier verdict priced the WHOLE unfiltered SPY chain, which is $518 for
    two years of ``ohlcv-1d`` on OPRA.PILLAR. That is the wrong instrument: the
    axis never needed every strike, only the ones near the money. Priced with
    ``metadata.get_cost`` - which bills nothing - the band the requirement
    actually describes costs **$4.91** in ``cbbo-1m`` quotes across 24 monthly
    expiries. The axis was not unaffordable; it had been quoted for something it
    did not need.

WHY QUOTES AND NOT TRADES
    ``ohlcv-1d`` carries the day's traded prices, and most strikes in a band do
    not trade on most days, so a trade-based surface is full of holes and stale
    prints. ``cbbo-1m`` carries the consolidated best bid and offer, which
    exists whether or not anyone traded. Implied volatility is taken from the
    quote MIDPOINT, and the bid-ask spread is carried through so no downstream
    reader can mistake a wide quote for a tight one.

WHY THE FORWARD COMES FROM PUT-CALL PARITY
    An implied volatility needs a forward and a discount factor. Taking them
    from an external rate curve would add a second data dependency with its own
    vintage problem, and the estate has been burned by exactly that before (the
    earlier IV attempts drifted because their FRED / Cboe inputs moved). The
    options themselves carry the answer: C - P = D*(F - K) exactly, for every
    strike, so one regression of the call-minus-put midpoint on strike gives the
    discount factor and the forward for that (date, expiry) with no outside
    input at all. It is self-contained and PIT-clean by construction.

RESEARCH ONLY. Free credits only, zero paid dollars, no subscription. Writes
only under the campaign research root.
"""
from __future__ import annotations

import json
import math
import re
from datetime import date, timedelta
from pathlib import Path

from . import research_root, write_artifact
from . import databento_acquisition as DA

CALCULATION_OWNER = "alpha_agent.alpha_recovery.options_acquisition"
ARTIFACT_NAME = "options_acquisition_state.json"

DATASET = "OPRA.PILLAR"
SCHEMA = "cbbo-1m"
UNDERLYING = "SPY"

#: The band the recorded requirement names. Neither number is swept.
MONEYNESS_BAND = 0.10
STRIKE_SPACING = 5.0
#: Each expiry is bought over the 90 days before it, not only the month it is
#: near-dated. Monthlies sit ~30 days apart, so a 90-day window keeps about
#: THREE expiries live on every date - which is what a term-structure slope
#: needs. A near-dated-only window leaves one expiry per date and no slope can
#: be formed at all.
LOOKBACK_DAYS = 90
YEARS = 2

#: One snapshot per date, declared in advance. 15:45 ET is late enough that the
#: book reflects the day and early enough to avoid the closing auction.
SNAPSHOT_ET = (15, 45)

#: US market holidays that landed on a third Friday in the window, pushing the
#: monthly expiry to the Thursday before. Found because the VENUE refused to
#: resolve the Friday symbol, not assumed from a calendar.
EXPIRY_SHIFT = {date(2025, 4, 18): date(2025, 4, 17),    # Good Friday
                date(2026, 6, 19): date(2026, 6, 18)}    # Juneteenth

#: Databento fixed-point price scale and null sentinel, identical to GLBX.
PRICE_SCALE = 1e-9
NULL_I64 = 9223372036854775807

#: The DISCOVERY sample: the exact dates the moneyness-anchored surface covers,
#: and therefore the ONLY dates any sign discovered on this surface was chosen
#: from. Named as data so a confirmation window can be proved disjoint from it
#: by comparison rather than by a comment.
DISCOVERY_WINDOW = ("2024-09-10", "2026-08-20")

#: An INDEPENDENT historical window carries a TAG, and the tag is the only thing
#: that differs: same band, same spacing, same lookback, same snapshot, same
#: schema, same parity forward, same inversion. A tagged surface is written to
#: its own file so a confirmation can never overwrite - or be silently merged
#: into - the sample a hypothesis was discovered on.
CONFIRMATION_TAG = "confirm_2022_2024"

#: The discovery bands rebuilt with the CORRECTED spot dating and the true 16:00
#: close (see ``session_closes``). The original surface is left exactly where it
#: is: it is what the reported discovery result was computed on, and overwriting
#: it would erase the only record of what the defect was worth.
SPOT_CORRECTED_TAG = "spotfix"


def _suffix(tag: str | None) -> str:
    return "" if not tag else "_%s" % re.sub(r"[^A-Za-z0-9_]+", "_", str(tag))


def data_root(tag: str | None = None) -> Path:
    return research_root() / ("_data_options_opra%s" % _suffix(tag))


def surface_path(tag: str | None = None) -> Path:
    return research_root() / "_data_options" / (
        "opra_spy_moneyness_surface%s.csv.gz" % _suffix(tag))


# --------------------------------------------------------------------- symbols
def third_friday(y: int, m: int) -> date:
    d = date(y, m, 1)
    f = [d + timedelta(days=i) for i in range(31)
         if (d + timedelta(days=i)).month == m and (d + timedelta(days=i)).weekday() == 4]
    return EXPIRY_SHIFT.get(f[2], f[2])


def osi(exp: date, right: str, strike: float) -> str:
    """The 21-character OSI symbol OPRA actually uses.

    Verified against ``symbology.resolve`` before any billable call: the root is
    padded to six characters, so ``SPY`` carries three trailing spaces. Two
    other spellings were tried and refused by the venue.
    """
    return "%-6s%s%s%08d" % (UNDERLYING, exp.strftime("%y%m%d"), right,
                             int(round(strike * 1000)))


def band_for(level: float) -> list:
    lo = math.floor(level * (1 - MONEYNESS_BAND) / STRIKE_SPACING) * STRIKE_SPACING
    hi = math.ceil(level * (1 + MONEYNESS_BAND) / STRIKE_SPACING) * STRIKE_SPACING
    n = int(round((hi - lo) / STRIKE_SPACING)) + 1
    return [lo + i * STRIKE_SPACING for i in range(n)]


def underlying_levels() -> "tuple":       # noqa: F821
    """Dates and approximate SPY levels, from the OWNED ES futures panel.

    SPY tracks SPX and ES tracks SPX, so ES/10 centres a +/-10 % band to well
    inside one strike increment. Using owned data to decide WHAT to buy means
    the band costs nothing to design, and an approximate centre is all a 20 %
    wide band needs.
    """
    import numpy as np

    from .futures_intraday import minute_index, panel

    pn = panel()
    j = pn["instruments"].index("ES")
    close = pn["close"][:, minute_index(16, 0), j]
    return np.array(pn["dates"]), close / 10.0


def plan(client: DA.Client, budget_usd: float, years: int = YEARS,
         available_end: str | None = None, *, window: tuple | None = None) -> dict:
    """Price every expiry's band BEFORE anything is downloaded.

    ``window`` names an EXPLICIT ``(start, end)`` instead of "the last ``years``
    the venue serves". It changes WHICH dates are bought and nothing else - the
    band width, the strike spacing, the per-expiry lookback, the snapshot minute
    and the schema are all module constants and none of them is reachable from
    here. That is the point: an independent confirmation window has to be the
    SAME instrument measured somewhere else, or it confirms nothing.
    """
    import numpy as np

    if window:
        start = date.fromisoformat(window[0])
        end = date.fromisoformat(window[1])
        # ``window`` chooses WHICH EXPIRIES are in scope; ``available_end`` caps
        # how far each one may be BOUGHT. They are the same date for a window
        # that ends at an expiry, which is why one value served both until now -
        # but a catch-up needs expiries that mature AFTER the last session the
        # venue serves, bought only up to that session. Defaulting to
        # ``window[1]`` keeps every existing caller byte-identical.
        avail_end = available_end or window[1]
    else:
        rng = client.dataset_range(DATASET)
        avail_end = available_end or DA._available_end(rng, SCHEMA) or DA._available_end(rng)
        end = date.fromisoformat(avail_end)
        start = end - timedelta(days=int(365.25 * years))

    dates, spy = underlying_levels()
    expiries = [third_friday(y, m)
                for y in range(start.year, end.year + 1) for m in range(1, 13)]
    expiries = sorted(e for e in expiries if start <= e <= end)

    requests, errors = [], {}
    for i, exp in enumerate(expiries):
        s0 = max((exp - timedelta(days=LOOKBACK_DAYS)).isoformat(), start.isoformat())
        s1 = min(exp.isoformat(), avail_end)
        if s0 >= s1:
            continue
        w0 = expiries[i - 1] if i else exp - timedelta(days=31)
        m = (dates >= w0.isoformat()) & (dates <= exp.isoformat())
        if not m.any():
            # The expiry's NEAR-DATED window lies in the future, so there is no
            # owned level to take a median of. That happens only when an expiry
            # is bought over a window that PRECEDES its near-dated life - a
            # catch-up, where the September monthly is too close to the money
            # date to serve as the near expiry and the October one must be
            # bought early. The anchor is then the window actually being bought,
            # which is the same intent correctly applied: the band exists to
            # bracket the money over the dates purchased. Every window whose
            # near-dated period has owned levels takes the first branch and is
            # byte-identical to before.
            m = (dates >= s0) & (dates <= s1)
        if not m.any():
            continue
        level = float(np.nanmedian(spy[m]))
        strikes = band_for(level)
        symbols = [osi(exp, r, k) for k in strikes for r in ("C", "P")]
        try:
            usd = client.cost(symbols, s0, s1, dataset=DATASET, schema=SCHEMA)
        except DA.DatabentoError as exc:
            errors[exp.isoformat()] = str(exc)[:200]
            continue
        requests.append({
            "label": "SPY_%s" % exp.strftime("%Y%m%d"),
            "expiry": exp.isoformat(), "start": s0, "end": s1,
            "underlying_level": round(level, 2),
            "strike_low": strikes[0], "strike_high": strikes[-1],
            "symbols": symbols, "n_symbols": len(symbols),
            "cost_usd": round(usd, 6),
            # one signature per REQUEST, and the request is the whole band
            "symbol": "SPY_%s" % exp.strftime("%Y%m%d"),
            "signature": DA._signature("SPY_%s" % exp.strftime("%Y%m%d"), s0, s1,
                                       SCHEMA, DATASET),
        })
    spend = round(sum(r["cost_usd"] for r in requests), 6)
    cap = budget_usd * (1.0 - DA.BUDGET_SAFETY_MARGIN)
    return {
        "dataset": DATASET, "schema": SCHEMA, "stype_in": "raw_symbol",
        "underlying": UNDERLYING,
        "window": {"start": start.isoformat(), "end": avail_end, "years": years},
        "band": {"moneyness": MONEYNESS_BAND, "strike_spacing": STRIKE_SPACING,
                 "lookback_days_per_expiry": LOOKBACK_DAYS,
                 "anchored_on": "the median ES/10 level while each expiry was near-dated"},
        "requests": requests, "n_requests": len(requests),
        "n_symbol_requests": sum(r["n_symbols"] for r in requests),
        "errors": errors,
        "selection": {
            "budget_usd": round(budget_usd, 4),
            "safety_margin": DA.BUDGET_SAFETY_MARGIN,
            "effective_cap_usd": round(cap, 4),
            "estimated_spend_usd": spend,
            "headroom_usd": round(cap - spend, 4),
            "fits_in_free_credit": spend <= cap,
            "paid_dollars_required": 0.0,
        },
        "cost_estimated_before_any_download": True,
    }


def acquire(budget_usd: float, execute: bool = False, client: DA.Client | None = None,
            write: bool = True, *, window: tuple | None = None,
            available_end: str | None = None,
            tag: str | None = None, spot_window: tuple | None = None,
            spot_dataset: str | None = None, spot_schema: str | None = None) -> dict:
    """Acquire an option band, and - when ``spot_window`` is given - the marks it
    is scored against.

    The spot leg belongs in the SAME artifact as the bands. A surface is not
    usable without a close to measure returns against, so an acquisition state
    that reports only the option cost understates what the axis actually spent
    and leaves the mark's provenance in a different file from the data it marks.
    """
    cred = DA.credential_state()
    body = {
        "calculation_owner": CALCULATION_OWNER, "provider": "databento",
        "dataset": DATASET, "schema": SCHEMA, "credential": cred,
        "information_case": information_case(),
        "spending_contract": DA.spending_contract(),
        "tag": tag, "requested_window": list(window) if window else None,
    }
    if not cred["usable"]:
        body["state"] = "BLOCKED_CREDENTIAL_ABSENT"
        if write:
            write_artifact(_artifact_name(tag), body)
        return body

    client = client or DA.Client()
    p = plan(client, budget_usd=budget_usd, window=window,
             available_end=available_end)
    body["plan"] = {k: v for k, v in p.items() if k != "requests"}
    body["plan"]["requests"] = [{k: v for k, v in r.items() if k != "symbols"}
                                for r in p["requests"]]
    body["state"] = ("PLANNED_FITS_FREE_CREDIT" if p["selection"]["fits_in_free_credit"]
                     else "PLANNED_EXCEEDS_FREE_CREDIT")
    if execute and p["selection"]["fits_in_free_credit"]:
        d = DA.download(client, p, out_root=data_root(tag), dry_run=False,
                        schema=SCHEMA, dataset=DATASET)
        body["download"] = d
        body["state"] = "ACQUIRED"
        got = {w["symbol"] for w in d.get("written", [])}
        band_usd = round(sum(r["cost_usd"] for r in p["requests"] if r["label"] in got), 4)
        body["band_acquisition_cost_usd"] = band_usd
        spot_usd = 0.0
        if spot_window:
            legs = []
            for sch in ([spot_schema] if spot_schema else [SPOT_SCHEMA]):
                leg = acquire_spot(client, spot_window[0], spot_window[1], execute=True,
                                   tag=tag, dataset=spot_dataset, schema=sch)
                legs.append(leg)
                spot_usd += float(leg.get("cost_usd") or 0.0)
            body["spot"] = legs
        body["spot_acquisition_cost_usd"] = round(spot_usd, 6)
        body["total_acquisition_cost_usd"] = round(band_usd + spot_usd, 4)
    elif execute:
        body["download"] = {"state": "REFUSED_EXCEEDS_FREE_CREDIT"}
    if write:
        write_artifact(_artifact_name(tag), body)
    return body


def _artifact_name(tag: str | None) -> str:
    if not tag:
        return ARTIFACT_NAME
    return ARTIFACT_NAME.replace(".json", "%s.json" % _suffix(tag))


#: The underlying's own daily bars. Moneyness is strike / spot and the variance
#: risk premium needs realised volatility from spot returns, so a genuine close
#: is required. The owned SPY minute panel stops at 12:59 ET, which is the wrong
#: mark for a 15:45 option snapshot, and ES/10 carries a basis. A consolidated
#: daily bar costs $0.0008 for two years, so approximating was never worth it.
SPOT_DATASET = "EQUS.SUMMARY"
SPOT_SCHEMA = "ohlcv-1d"

#: EQUS.SUMMARY begins 2024-07-01, so it cannot price a window that PRECEDES the
#: discovery sample. SPY's primary listing is NYSE Arca and ``ARCX.PILLAR``
#: serves ``ohlcv-1d`` from 2018, which is the same instrument on the venue that
#: lists it rather than a proxy. The two series are COMPARED on their overlap
#: before the earlier one is used, so the substitution is measured, not assumed.
SPOT_DATASET_PRE_2024 = "ARCX.PILLAR"

#: ``ohlcv-1d`` on a VENUE dataset aggregates the whole UTC day, extended hours
#: included, so its close is the last Arca print near 20:00 ET and not the
#: 16:00 close at all - it disagrees with the consolidated summary by a median
#: of 8.8 bp and by 348 bp on the worst day of April 2025. The HOURLY bars carry
#: the same information without that defect: the bar covering 15:00-16:00 ET
#: closes at the closing print. $0.02 for four years, and it is the only source
#: that spans BOTH the discovery sample and the window that precedes it.
SPOT_SCHEMA_INTRADAY = "ohlcv-1h"
#: The ET hour whose close IS the session close.
SPOT_CLOSE_HOUR_ET = 15


def spot_path(tag: str | None = None, dataset: str | None = None,
              schema: str | None = None) -> Path:
    ds = dataset or SPOT_DATASET
    sch = schema or SPOT_SCHEMA
    stem = "daily" if sch == SPOT_SCHEMA else sch.replace("-", "_")
    return data_root(tag) / ("%s_%s_%s.csv" % (UNDERLYING, ds.replace(".", "_"), stem))


def acquire_spot(client: DA.Client, start: str, end: str, execute: bool = False,
                 *, tag: str | None = None, dataset: str | None = None,
                 schema: str | None = None) -> dict:
    """Buy the underlying's closes, priced first like everything else."""
    ds = dataset or SPOT_DATASET
    sch = schema or SPOT_SCHEMA
    usd = client.cost([UNDERLYING], start, end, dataset=ds, schema=sch)
    out = {"dataset": ds, "schema": sch, "symbol": UNDERLYING,
           "start": start, "end": end, "cost_usd": round(usd, 6), "tag": tag,
           "why": "moneyness is strike/spot and the variance risk premium needs realised "
                  "volatility from spot returns; the owned SPY minute panel stops at 12:59 ET"}
    target = spot_path(tag, ds, sch)
    if target.exists() and target.stat().st_size > 0:
        out["state"] = "REUSED"
        out["path"] = str(target)
        return out
    if not execute:
        out["state"] = "PRICED_NOT_DOWNLOADED"
        return out
    raw = client.get_range_csv([UNDERLYING], start, end, dataset=ds, schema=sch)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".csv.tmp")
    tmp.write_bytes(raw)
    tmp.replace(target)
    out["state"] = "DOWNLOADED"
    out["path"] = str(target)
    out["bytes"] = len(raw)
    return out


def session_closes(*, tag: str | None = None, dataset: str | None = None,
                   schema: str | None = None) -> dict:
    """``session date -> the underlying's close``, dated CORRECTLY.

    Two defects live here and both were found by measurement, not by reading:

    1. A DAILY bar from either dataset is stamped ``00:00 UTC`` **of its own
       session**, so converting that instant to New York lands at 20:00 on the
       day BEFORE and dates every close one session early. The proof is that the
       UTC date carries zero weekend rows while the New-York date carries 97 of
       506 and 188 of 1003. Joined against option dates that shift pushed the
       close of session ``d+1`` onto date ``d`` and - because no session follows
       a Friday to supply one - dropped EVERY FRIDAY from the surface.
    2. A daily bar on a VENUE dataset aggregates the whole UTC day, extended
       hours included, so its close is the last print near 20:00 ET rather than
       the 16:00 close: it disagrees with the consolidated summary by a median
       of 8.8 bp and by 348 bp on the worst day of April 2025.

    The hourly bar fixes both. Its timestamps are real intraday instants, so the
    New-York date is the session date, and the bar covering 15:00-16:00 ET
    closes on the closing print. Measured against the ``EQUS.SUMMARY`` close the
    discovery sample used, over the 506 sessions where both exist: median
    -0.44 bp, 95th percentile 2.88 bp, and a 5-session return correlation of
    0.99953. That is the same series, so ONE spot definition now spans the
    discovery sample and the window that precedes it.
    """
    import numpy as np
    import pandas as pd

    ds = dataset or SPOT_DATASET
    sch = schema or SPOT_SCHEMA
    df = pd.read_csv(spot_path(tag, ds, sch))
    ts = pd.to_datetime(df["ts_event"], unit="ns", utc=True, errors="coerce")
    if ts.isna().all():
        ts = pd.to_datetime(df["ts_event"], utc=True, errors="coerce")
    px = pd.to_numeric(df["close"], errors="coerce")
    if px.abs().median() > 1e6:
        px = px * PRICE_SCALE
    if sch == SPOT_SCHEMA_INTRADAY:
        ny = ts.dt.tz_convert("America/New_York")
        keep = ny.dt.hour == SPOT_CLOSE_HOUR_ET
        dates = ny[keep].dt.date.astype(str)
        vals = px[keep]
    else:
        # the UTC date IS the session date; NEVER convert a midnight stamp
        dates = ts.dt.date.astype(str)
        vals = px
    out = {d: float(v) for d, v in zip(dates, vals) if np.isfinite(v)}
    return out


# ============================================================================
# THE ADAPTER: quotes -> implied volatility -> the surface CSV the existing
# options_surface campaign already reads. It owns no scorer and no gate.
# ============================================================================
_OSI = re.compile(r"^(?P<root>.{6})(?P<y>\d{2})(?P<m>\d{2})(?P<d>\d{2})(?P<cp>[CP])(?P<k>\d{8})$")


def parse_osi(sym: str) -> dict | None:
    m = _OSI.match(sym)
    if not m:
        return None
    return {"expiration": date(2000 + int(m.group("y")), int(m.group("m")), int(m.group("d"))),
            "type": "call" if m.group("cp") == "C" else "put",
            "strike": int(m.group("k")) / 1000.0}


def instrument_map(client: DA.Client, symbols: list, start: str, end: str) -> dict:
    """(date, instrument_id) -> raw symbol.

    OPRA reassigns ``instrument_id`` EVERY DAY, so a single symbol has a
    different numeric handle on every session. The CSV carries only that handle
    and no symbol column, so without this map a band file is 60 interleaved
    contracts with no way to tell a 560 call from a 620 put. ``symbology.resolve``
    bills nothing and answers exactly this.
    """
    out: dict = {}
    for i in range(0, len(symbols), 200):
        batch = symbols[i:i + 200]
        res = client._json("POST", "symbology.resolve",
                           {"dataset": DATASET, "symbols": ",".join(batch),
                            "stype_in": "raw_symbol", "stype_out": "instrument_id",
                            "start_date": start, "end_date": end})
        for sym, spans in (res.get("result") or {}).items():
            for sp in spans or []:
                d0 = date.fromisoformat(sp["d0"])
                d1 = date.fromisoformat(sp["d1"])
                d = d0
                while d < d1:
                    out[(d.isoformat(), int(sp["s"]))] = sym
                    d += timedelta(days=1)
    return out


def _implied_vol(price: float, fwd: float, strike: float, t: float, disc: float,
                 is_call: bool) -> float:
    """Black-76 implied volatility by bisection.

    Bisection rather than Newton because vega collapses in the wings and a
    Newton step there runs away; bisection on a monotone function cannot. The
    price must sit strictly inside its no-arbitrage bounds or there is no
    volatility that produces it and NaN is the honest answer.
    """
    if not (t > 0 and fwd > 0 and strike > 0 and disc > 0 and price > 0):
        return float("nan")
    intrinsic = disc * max(fwd - strike, 0.0) if is_call else disc * max(strike - fwd, 0.0)
    upper = disc * fwd if is_call else disc * strike
    if not (intrinsic + 1e-8 < price < upper - 1e-8):
        return float("nan")

    def bs(vol: float) -> float:
        s = vol * math.sqrt(t)
        if s <= 0:
            return intrinsic
        d1 = (math.log(fwd / strike) + 0.5 * s * s) / s
        d2 = d1 - s
        nd = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))    # noqa: E731
        if is_call:
            return disc * (fwd * nd(d1) - strike * nd(d2))
        return disc * (strike * nd(-d2) - fwd * nd(-d1))

    lo, hi = 1e-4, 5.0
    if bs(hi) < price:
        return float("nan")
    for _ in range(80):
        mid = 0.5 * (lo + hi)
        if bs(mid) < price:
            lo = mid
        else:
            hi = mid
    return 0.5 * (lo + hi)


def _forward_and_discount(g: "pd.DataFrame") -> tuple:              # noqa: F821
    """Forward and discount factor for one (date, expiry), from put-call parity.

    ``C - P = D * (F - K)`` holds exactly for European options, so a least
    squares fit of the call-minus-put midpoint on strike gives slope ``-D`` and
    intercept ``D*F``. No external rate curve, no dividend assumption, and
    therefore no second data vintage that can drift - which is how the estate's
    two earlier implied-volatility attempts went wrong.

    SPY options are American, so parity is an approximation; the error is
    second-order near the money and this fit uses only the strikes within 5 % of
    the median, where early exercise is worth least.
    """
    import numpy as np

    piv = g.pivot_table(index="strike", columns="type", values="mid", aggfunc="last")
    if "call" not in piv or "put" not in piv:
        return float("nan"), float("nan"), 0
    piv = piv.dropna()
    if len(piv) < 4:
        return float("nan"), float("nan"), len(piv)
    k = piv.index.to_numpy(dtype=float)
    y = (piv["call"] - piv["put"]).to_numpy(dtype=float)
    mid_k = float(np.median(k))
    near = np.abs(k / mid_k - 1.0) <= 0.05
    if near.sum() >= 4:
        k, y = k[near], y[near]

    # Two-parameter fit first: slope is -D and intercept is D*F.
    slope, intercept = np.polyfit(k, y, 1)
    disc = -float(slope)
    if 0.95 <= disc <= 1.0:
        return float(intercept) / disc, disc, int(len(k))

    # The two-parameter fit is unstable here and it does not need to be stable.
    # Every expiry bought is at most 90 days out, so at any plausible rate the
    # discount factor is within 1.3 % of 1 and within 0.3 % for the near-dated
    # expiries that carry the ATM series. Fixing D = 1 and taking the forward as
    # the MEDIAN of (C - P + K) over near-money strikes is far more robust to a
    # single wide quote than a least-squares slope through them, and the error
    # it admits is second order in the implied volatility.
    return float(np.median(y + k)), 1.0, int(len(k))


def build_surface(client: DA.Client | None = None, *, write: bool = True,
                  window: tuple | None = None,
                  available_end: str | None = None,
                  tag: str | None = None,
                  spot_dataset: str | None = None,
                  spot_schema: str | None = None,
                  spot_tag: str | None = None,
                  out_tag: str | None = None) -> dict:
    """Every acquired band -> ONE surface CSV in the exact shape
    ``options_surface.features`` already reads.

    Columns: ``date, expiration, type, strike, iv, moneyness, T_years,
    underlying_close`` plus the bid/ask the volatility came from, so no reader
    can mistake a wide quote for a tight one.
    """
    import numpy as np
    import pandas as pd

    client = client or DA.Client()
    # prices nothing new; rebuilds the symbol lists
    p = plan(client, budget_usd=0.0, window=window, available_end=available_end)
    # the spot file may live under a different tag: ONE hourly series spans both
    # the discovery sample and the window before it, and is read by both builds
    spot_close = session_closes(tag=(tag if spot_tag is None else spot_tag),
                                dataset=spot_dataset, schema=spot_schema)

    snap_min = SNAPSHOT_ET[0] * 60 + SNAPSHOT_ET[1]
    frames, skipped = [], {}
    for req in p["requests"]:
        f = data_root(tag) / ("%s_%s_%s_%s.csv" % (req["label"], SCHEMA,
                                                   req["start"], req["end"]))
        if not f.exists():
            skipped[req["label"]] = "not on disk"
            continue
        df = pd.read_csv(f, usecols=lambda c: c in ("ts_recv", "instrument_id",
                                                    "bid_px_00", "ask_px_00",
                                                    "bid_sz_00", "ask_sz_00"))
        if df.empty:
            skipped[req["label"]] = "empty"
            continue
        t = pd.to_datetime(df["ts_recv"], unit="ns", utc=True).dt.tz_convert("America/New_York")
        df["date"] = t.dt.date.astype(str)
        df["minute"] = t.dt.hour * 60 + t.dt.minute
        # ONE snapshot per date: the last quote at or before 15:45 ET
        df = df[df["minute"] <= snap_min]
        if df.empty:
            skipped[req["label"]] = "no quote before the snapshot minute"
            continue
        df = df.sort_values("minute").groupby(["date", "instrument_id"], as_index=False).last()

        imap = instrument_map(client, req["symbols"], req["start"], req["end"])
        df["symbol"] = [imap.get((d, int(i))) for d, i in zip(df["date"], df["instrument_id"])]
        df = df.dropna(subset=["symbol"])
        if df.empty:
            skipped[req["label"]] = "no instrument_id resolved to a symbol"
            continue
        meta = [parse_osi(s) for s in df["symbol"]]
        df["expiration"] = [m["expiration"].isoformat() if m else None for m in meta]
        df["type"] = [m["type"] if m else None for m in meta]
        df["strike"] = [m["strike"] if m else np.nan for m in meta]
        for c in ("bid_px_00", "ask_px_00"):
            v = pd.to_numeric(df[c], errors="coerce").to_numpy()
            df[c] = np.where(v == NULL_I64, np.nan, v * PRICE_SCALE)
        df = df[(df["bid_px_00"] > 0) & (df["ask_px_00"] > df["bid_px_00"])]
        df["mid"] = 0.5 * (df["bid_px_00"] + df["ask_px_00"])
        frames.append(df[["date", "expiration", "type", "strike", "mid",
                          "bid_px_00", "ask_px_00", "bid_sz_00", "ask_sz_00"]])

    if not frames:
        return {"state": "NO_ROWS", "skipped": skipped}
    raw = pd.concat(frames, ignore_index=True)

    rows = []
    for (d, e), g in raw.groupby(["date", "expiration"]):
        fwd, disc, n_par = _forward_and_discount(g)
        if not np.isfinite(fwd):
            continue
        t_years = (date.fromisoformat(e) - date.fromisoformat(d)).days / 365.25
        if t_years <= 0:
            continue
        s = spot_close.get(d)
        if not s or not np.isfinite(s):
            continue
        for _, r in g.iterrows():
            iv = _implied_vol(float(r["mid"]), fwd, float(r["strike"]), t_years, disc,
                              r["type"] == "call")
            rows.append({"date": d, "expiration": e, "type": r["type"],
                         "strike": float(r["strike"]), "iv": iv,
                         # FORWARD moneyness, not spot moneyness. The option
                         # snapshot is 15:45 ET and the equity close is 16:00,
                         # so K/spot carries fifteen minutes of drift that has
                         # nothing to do with the option. K/forward is measured
                         # entirely inside the same snapshot, and "at the
                         # forward" is the standard definition of at-the-money
                         # for an implied-volatility surface anyway.
                         "moneyness": float(r["strike"]) / fwd,
                         "moneyness_spot": float(r["strike"]) / float(s),
                         "T_years": t_years, "underlying_close": float(s),
                         "forward": fwd, "discount": disc, "n_parity_strikes": n_par,
                         "bid": float(r["bid_px_00"]), "ask": float(r["ask_px_00"]),
                         "mid": float(r["mid"])})
    out = pd.DataFrame(rows)
    if out.empty:
        return {"state": "NO_ROWS", "skipped": skipped}
    out = out.sort_values(["date", "expiration", "type", "strike"]).reset_index(drop=True)
    dest = surface_path(tag if out_tag is None else out_tag)
    if write:
        dest.parent.mkdir(parents=True, exist_ok=True)
        out.to_csv(dest, index=False, compression="gzip")
    ok = out[np.isfinite(out["iv"]) & (out["iv"] > 0)]
    return {
        "state": "BUILT", "path": str(dest), "tag": tag,
        "spot_dataset": spot_dataset or SPOT_DATASET,
        "spot_schema": spot_schema or SPOT_SCHEMA,
        "spot_dates": len(spot_close),
        "contract_days": int(len(out)), "with_iv": int(len(ok)),
        "dates": int(out["date"].nunique()),
        "first": out["date"].min(), "last": out["date"].max(),
        "expiries": int(out["expiration"].nunique()),
        "median_contracts_per_date": float(ok.groupby("date").size().median()),
        "median_expiries_per_date": float(ok.groupby("date")["expiration"].nunique().median()),
        "moneyness_range": [float(ok["moneyness"].min()), float(ok["moneyness"].max())],
        "skipped": skipped,
        "snapshot_et": "%02d:%02d" % SNAPSHOT_ET,
    }


def information_case() -> dict:
    return {
        "named_defect_it_removes": (
            "the owned SPY surface is a FIXED strike band of 654-720; the underlying rallied "
            "through it, leaving 25 of 264 dates whose near-dated strikes bracket the money "
            "against a frozen floor of 36. An ATM IV series cannot be built from a band the "
            "spot has left."),
        "why_this_is_not_a_reopened_transform": (
            "both earlier attempts used an INDEX PROXY for implied volatility and one of them "
            "reproduced badly when its FRED / Cboe inputs drifted. This is the actual instrument "
            "the need always named - per-contract quotes with strike, expiry and type."),
        "contract_rule_13_condition_met": "NEW ORTHOGONAL INFORMATION",
        "why_the_earlier_do_not_buy_does_not_apply": (
            "that verdict priced the WHOLE unfiltered SPY chain at $518 for two years, which is "
            "an instrument this axis never needed. The band the requirement describes costs "
            "$4.91."),
        "forward_and_discount_from_put_call_parity": (
            "C - P = D*(F - K) exactly, so one regression per (date, expiry) recovers both with "
            "no external rate curve and therefore no second vintage to get wrong"),
    }
