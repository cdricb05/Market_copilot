"""alpha_agent.r46.options - close a free gap, and predeclare before it closes.

Release 44 read its option surface as 107 sessions short of the 500 a variance
study needs (250 to fit on, 250 never seen) and priced a $29/month purchase
against that gap. Release 45 showed the gap was never a history problem: R44
had sampled only six widely-spaced expiries. Sampling fourteen more inside the
SAME free window - requesting no date past the entitlement boundary - closed
81 of the 107 sessions for $0 and left 26.

Release 46 does two things.

**Closes the rest for nothing.** R44 and R45 both sampled third Fridays only.
SPY has traded weekly expiries for years, and every one of them lies inside
the window the existing entitlement already serves. This module samples those.
No date beyond the entitlement boundary is requested, no existing row is
touched, and the budget is bounded.

**Predeclares the hypotheses BEFORE the confirming sessions arrive.** This is
the part that matters, and it is the whole release in miniature. Once the
surface clears 500 sessions, whoever looks at it first will be able to try
skew, term structure, delta-hedged residuals and dispersion, keep whichever
worked, and report it as a discovery. Release 45 measured exactly what that is
worth: re-running R44's sixty-cell screen on three different event zones gave
a different winner every time, the last one larger than the published
headline. So the hypotheses are written down HERE, with their parameters, their
controls and their costs, hashed into the frozen contract, while nobody can yet
see the answer.

Generic short-volatility premium is NOT alpha and is excluded by name. R45
measured it at 4.50 vol points with t 9.39; it is insurance revenue, it is
available to anyone, and calling it a discovery would be the same mistake in a
different market.

Spends nothing. Creates no account. Starts no trial. Accepts no licence.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import time
from pathlib import Path

import pandas as pd

from . import CAMPAIGN_ID, artifact_body, campaign_dir, sha, write_json
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.options"

ARTIFACT = "R46_OPTIONS_LANE.json"

#: The acquired option surfaces live at each release's RESEARCH ROOT, NOT
#: under its campaign directory - unlike that release's artifacts. Pointing
#: these at the campaign subdirectory is silent: every loader returns None,
#: the combined surface reports zero prior sessions, and the dedup that stops
#: R46 re-sampling an expiry a prior release already paid for never fires.
R44_ROOT = Path(r"D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44")
R45_ROOT = Path(r"D:\Stock_Prediction_app_data\macro_event_alpha_r45")
R44_SURFACE = R44_ROOT / "_data_options" / "polygon_spy_option_surface.csv.gz"
R44_SURFACE_TERM = (R44_ROOT / "_data_options"
                    / "polygon_spy_option_surface_term.csv.gz")
R45_SURFACE = (R45_ROOT / "_data_options"
               / "polygon_spy_option_surface_r45_extension.csv.gz")

OUT_DIR = C.RESEARCH_ROOT / "_data_options"
R46_SURFACE = OUT_DIR / "polygon_spy_option_surface_r46_weeklies.csv.gz"
R46_SURFACE_GLOB = "polygon_spy_option_surface_r46_weeklies*.csv.gz"

#: Release 46.6 - the front-expiry extension batches. Kept under their own
#: glob so the weekly-expiry sample and the session-axis extension can never
#: be confused with one another in an audit.
R46_FRONT_GLOB = "polygon_spy_option_surface_r46_front_*.csv.gz"

UNDERLYING = "SPY"
STRIKES_PER_EXPIRY = 8
CALL_BUDGET = 120

#: Deliberately small. The front extension needs a handful of near-the-money
#: contracts to establish the session dates, not a surface.
FRONT_STRIKES = 4
FRONT_CALL_BUDGET = 8

MIN_FIT_SESSIONS = 250
MIN_JUDGED_SESSIONS = 250
SESSIONS_REQUIRED = MIN_FIT_SESSIONS + MIN_JUDGED_SESSIONS

#: Release 46.6.1 - what the 500-session gate actually decides, said in its own
#: words. It counts DATES. Whether the three predeclared hypotheses have a
#: sufficient sample on those dates is a different question with a different
#: owner and, at R46.6, a different answer.
SESSION_GATE_MET = "SESSION_GATE_MET"
SESSION_GATE_SHORT = "SESSION_GATE_SHORT"
SESSION_GATE_MEASURES = "NUMBER_OF_SESSIONS_ONLY"
SESSION_GATE_DOES_NOT_MEASURE = "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION"

#: Never exceeded. The entitlement boundary is a fact about the plan, not a
#: number to negotiate with.
ENTITLEMENT_LOOKBACK_DAYS = 700

#: A contract must have EXPIRED to be queryable with ``expired=true``. Three
#: days is settlement slack, nothing more. R45 used twenty, which silently
#: put the two most recent expiries out of reach - and those are precisely
#: the ones carrying session dates the surface does not already hold.
ENTITLEMENT_RECENT_EMBARGO_DAYS = 3


# --------------------------------------------------------------------------- #
# THE PREDECLARED HYPOTHESES - frozen before the confirming sessions exist
# --------------------------------------------------------------------------- #
PREDECLARED_HYPOTHESES = (
    {
        "hypothesis_id": "r46_opt_skew_residual",
        "family": "OPTION_SKEW_RESIDUAL",
        "statement": "the 25-delta risk reversal, residualised on contemporaneous "
                     "realised volatility and on the level of ATM implied "
                     "volatility, predicts the next 21 sessions of "
                     "delta-hedged return on the underlying",
        "signal": "z-score of (25d put IV - 25d call IV) after regressing out "
                  "ATM IV and trailing 21-session realised volatility",
        "position": "fade a rich downside: short the risk reversal when z > 1",
        "horizon_sessions": 21,
        "control": C.CONTROL_CASH,
        "cost_model": "declared option half-spread on BOTH legs plus the "
                      "delta-hedge cost in the underlying, charged on traded "
                      "notional",
        "fit_window": "the first 250 usable sessions, chronologically",
        "judge_window": "the LAST 250 usable sessions, never read until the "
                        "fit is frozen",
        "why_not_short_vol": "residualising on ATM IV removes the level; what "
                             "is left is the SHAPE, which is not insurance "
                             "revenue",
    },
    {
        "hypothesis_id": "r46_opt_term_structure_residual",
        "family": "OPTION_TERM_STRUCTURE_RESIDUAL",
        "statement": "the slope of the implied-volatility term structure, "
                     "residualised on the VIX level, predicts the next 21 "
                     "sessions of calendar-spread return",
        "signal": "z-score of (far-dated ATM IV - near-dated ATM IV) after "
                  "regressing out the VIX level",
        "position": "long the calendar when the residual slope is unusually "
                    "flat, short when unusually steep",
        "horizon_sessions": 21,
        "control": C.CONTROL_CASH,
        "cost_model": "two option legs at the declared half-spread, charged "
                      "on traded notional",
        "fit_window": "the first 250 usable sessions, chronologically",
        "judge_window": "the LAST 250 usable sessions",
        "why_not_short_vol": "a calendar spread is close to vega-neutral in "
                             "level and expresses the SLOPE",
    },
    {
        "hypothesis_id": "r46_opt_delta_hedged_residual",
        "family": "DELTA_HEDGED_RESIDUAL_RETURN",
        "statement": "the delta-hedged return of the ATM straddle, after "
                     "subtracting the unconditional variance risk premium "
                     "measured on the FIT window only, has a conditional "
                     "component predictable from the option surface itself",
        "signal": "delta-hedged straddle return minus the fit-window mean "
                  "premium, conditioned on surface skew and slope",
        "position": "scale the delta-hedged straddle by the conditional "
                    "residual, never by its unconditional sign",
        "horizon_sessions": 5,
        "control": C.CONTROL_CASH,
        "cost_model": "option half-spread plus daily delta re-hedge cost in "
                      "the underlying",
        "fit_window": "the first 250 usable sessions, chronologically",
        "judge_window": "the LAST 250 usable sessions",
        "why_not_short_vol": "the UNCONDITIONAL premium is subtracted by "
                             "construction, using only fit-window data; a "
                             "positive result must come from conditioning",
    },
)

EXCLUDED_BY_NAME = {
    "GENERIC_SHORT_VOLATILITY": (
        "the variance risk premium is insurance revenue available to anyone. "
        "R45 measured it at 4.50 vol points, t 9.39, on actual option prices. "
        "It is real and it is not alpha, and the contract excludes it."),
    "DISPERSION": (
        "dispersion needs a single-name option surface this estate does not "
        "own; declared here so that its absence is a recorded data gap rather "
        "than a hypothesis quietly dropped"),
}


def hypotheses_hash() -> str:
    return sha({"hypotheses": [dict(h) for h in PREDECLARED_HYPOTHESES],
                "excluded": EXCLUDED_BY_NAME})


# --------------------------------------------------------------------------- #
def _sha(p: Path) -> str:
    h = hashlib.sha256()
    try:
        with open(p, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
    except OSError:
        return "ABSENT"
    return h.hexdigest()[:16]


def _read(path: Path, tag: str):
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, compression="gzip")
    except Exception:
        return None
    df["_source"] = tag
    return df


def existing_surface():
    """Every option row prior releases legitimately acquired. READ-ONLY."""
    frames = [f for f in (_read(R44_SURFACE, "R44"),
                          _read(R44_SURFACE_TERM, "R44"),
                          _read(R45_SURFACE, "R45")) if f is not None]
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _weekly_expiries(lo: _dt.date, hi: _dt.date) -> list:
    """EVERY Friday in the window - monthlies included.

    An earlier version excluded third Fridays on the grounds that R44 and R45
    had already sampled them. That is true of the ones they sampled and false
    in general: the prior surface is missing 2026-06-19 and 2026-08-21, both
    third Fridays, and excluding them by rule made two of the most useful
    expiries in the window permanently unreachable. Enumerate everything and
    let the ``already`` set remove what was genuinely sampled - a dedup
    against the real data beats a dedup against an assumption about it.
    """
    out = []
    d = lo
    while d.weekday() != 4:
        d += _dt.timedelta(days=1)
    while d <= hi:
        out.append(d)
        d += _dt.timedelta(days=7)
    return out


def r46_batches():
    """Every R46 batch acquired so far, concatenated and DEDUPED.

    Release 46.6 added a second batch shape - the front-expiry extension,
    which re-samples the SAME contract on successive days as new bars print -
    so a plain concat would double-count rows. The surface is keyed by
    (ticker, date) and the last write of a key wins.
    """
    frames = []
    if OUT_DIR.exists():
        for p in sorted(OUT_DIR.glob(R46_SURFACE_GLOB)):
            f = _read(p, "R46")
            if f is not None:
                frames.append(f)
        for p in sorted(OUT_DIR.glob(R46_FRONT_GLOB)):
            f = _read(p, "R46_FRONT")
            if f is not None:
                frames.append(f)
    if not frames:
        return None
    out = pd.concat(frames, ignore_index=True)
    if {"ticker", "date"}.issubset(out.columns):
        out = out.drop_duplicates(subset=["ticker", "date"], keep="last")
    return out.reset_index(drop=True)


# --------------------------------------------------------------------------- #
# Release 46.6 - the front-expiry extension
# --------------------------------------------------------------------------- #
def _front_expiry(after: _dt.date) -> _dt.date:
    """The first Friday strictly after ``after`` - the nearest expiry whose
    contracts are still trading, and therefore the ONLY ones that can print a
    bar on a session the surface does not already hold."""
    d = after + _dt.timedelta(days=1)
    while d.weekday() != 4:
        d += _dt.timedelta(days=1)
    return d


def acquire_front_extension(*, budget_calls: int = FRONT_CALL_BUDGET,
                            strikes: int = FRONT_STRIKES,
                            acquire: bool = True, batch: str = None) -> dict:
    """Extend the SESSION AXIS using the nearest still-trading expiry.

    The 499-session surface stopped dead at the last expiry that had already
    expired, because :func:`acquire_weeklies` only ever considers expiries
    inside ``today - ENTITLEMENT_RECENT_EMBARGO_DAYS``. That embargo was a
    settlement-slack heuristic this estate wrote, not a boundary the provider
    imposes, and it had a consequence nobody measured: **no contract the plan
    serves trades after the last expired Friday**, so the sample could never
    reach 500 no matter how many old expiries were sampled.

    R46.6 measured the real boundary instead of assuming it. The owned plan
    answers ``DELAYED`` and serves daily aggregates through T-1 for a
    CURRENTLY TRADING contract. Those bars are printed, historical and free,
    and they carry exactly the sessions the surface is missing.

    Nothing here requests a date the provider does not return, and nothing is
    interpolated: whatever the aggregate endpoint hands back is what is
    written. If it hands back nothing past the last covered session, the
    function says so and the sample stays where it is.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / ("polygon_spy_option_surface_r46_front_%s.csv.gz"
                          % (batch or "latest"))
    if out_path.exists():
        try:
            df = pd.read_csv(out_path, compression="gzip")
            return {"state": "EXECUTED", "from_cache": True,
                    "rows": int(len(df)),
                    "sessions": int(pd.to_datetime(df["date"]).nunique()),
                    "path": str(out_path), "sha256": _sha(out_path),
                    "api_calls": 0, "money_spent_usd": 0.0}
        except Exception:                               # pragma: no cover
            pass
    if not acquire:
        return {"state": "SKIPPED", "why": "acquisition not requested",
                "api_calls": 0, "money_spent_usd": 0.0}

    from ..r43 import acquisition as R43AQ
    pk = R43AQ._key("POLYGON_API_KEY")
    if not pk:
        return {"state": "ACCOUNT_REQUIRED", "api_calls": 0,
                "money_spent_usd": 0.0,
                "why": "no owned Polygon key in the operator shell"}

    prev, mine = existing_surface(), r46_batches()
    have = [f for f in (prev, mine) if f is not None]
    combined = pd.concat(have, ignore_index=True) if have else None
    covered = (set(pd.to_datetime(combined["date"]).dt.date.unique())
               if combined is not None else set())
    if not covered:
        return {"state": "NO_PRIOR_SURFACE", "api_calls": 0,
                "money_spent_usd": 0.0}
    latest = max(covered)
    exp = _front_expiry(latest)

    calls, rows, errors = 0, [], []
    s, b, _ = R43AQ.http_get(
        R43AQ.POLY + "/v3/reference/options/contracts"
        "?underlying_ticker=%s&expiration_date=%s&contract_type=call"
        "&limit=1000&apiKey=%s" % (UNDERLYING, exp.isoformat(), pk))
    calls += 1
    time.sleep(R43AQ.POLYGON_RPM_SLEEP)
    if s != 200:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "front_expiry": exp.isoformat(), "status": s,
                "money_spent_usd": 0.0}
    res = json.loads(b).get("results") or []
    if not res:
        return {"state": "NO_CONTRACTS_AT_FRONT_EXPIRY", "api_calls": calls,
                "front_expiry": exp.isoformat(), "money_spent_usd": 0.0}

    #: Strikes nearest the MEDIAN listed strike. The median of a listed chain
    #: sits close to the money, and near-the-money contracts are the ones that
    #: actually print a bar every session - which is the whole point here.
    ks = sorted({float(r["strike_price"]) for r in res})
    mid = ks[len(ks) // 2]
    chosen = sorted(ks, key=lambda k: abs(k - mid))[:int(strikes)]
    by_strike = {}
    for r in res:
        by_strike.setdefault(float(r["strike_price"]), r)

    start = (latest - _dt.timedelta(days=4)).isoformat()
    today = _dt.date.today()
    for k in chosen:
        if calls >= budget_calls:
            break
        r = by_strike.get(k)
        if not r:
            continue
        tk = r["ticker"]
        s2, b2, _ = R43AQ.http_get(
            R43AQ.POLY + "/v2/aggs/ticker/%s/range/1/day/%s/%s"
            "?adjusted=true&limit=500&apiKey=%s"
            % (tk, start, today.isoformat(), pk))
        calls += 1
        time.sleep(R43AQ.POLYGON_RPM_SLEEP)
        if s2 != 200:
            errors.append({"ticker": tk, "status": s2})
            continue
        for a in (json.loads(b2).get("results") or []):
            rows.append({
                "ticker": tk, "underlying": UNDERLYING,
                "expiration": exp.isoformat(), "strike": float(k),
                "type": "call",
                "date": pd.to_datetime(a["t"], unit="ms").date().isoformat(),
                "open": a.get("o"), "high": a.get("h"), "low": a.get("l"),
                "close": a.get("c"), "vwap": a.get("vw"),
                "volume": a.get("v"), "trades": a.get("n")})

    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "front_expiry": exp.isoformat(), "errors": errors[:10],
                "money_spent_usd": 0.0,
                "why": "the entitlement returned no aggregate for the front "
                       "expiry"}

    frame = pd.DataFrame(rows)
    try:
        inv = R43AQ._invert_iv(frame, UNDERLYING)
        if inv is not None:
            frame = inv
    except Exception:                                   # pragma: no cover
        pass
    got = set(pd.to_datetime(frame["date"]).dt.date.unique())
    new = sorted(d for d in got if d not in covered)
    if not new:
        return {"state": "NO_NEW_SESSION", "api_calls": calls,
                "front_expiry": exp.isoformat(),
                "latest_date_already_covered": str(latest),
                "money_spent_usd": 0.0,
                "why": "every session the front expiry printed is already in "
                       "the surface"}
    frame.to_csv(out_path, index=False, compression="gzip")
    return {"state": "EXECUTED", "from_cache": False, "api_calls": calls,
            "front_expiry": exp.isoformat(),
            "front_expiry_has_expired": exp <= today,
            "rows": int(len(frame)),
            "contracts": int(frame["ticker"].nunique()),
            "n_strikes": int(frame["strike"].nunique()),
            "sessions": int(pd.to_datetime(frame["date"]).nunique()),
            "new_session_dates": [str(d) for d in new],
            "n_new_sessions": len(new),
            "latest_date_already_covered": str(latest),
            "date_span": [str(frame["date"].min()), str(frame["date"].max())],
            "path": str(out_path), "sha256": _sha(out_path),
            "errors": errors[:10], "money_spent_usd": 0.0,
            "requested_beyond_what_the_entitlement_returns": False,
            "interpolated_bars": 0, "fabricated_sessions": 0}


def acquire_weeklies(*, budget_calls: int = CALL_BUDGET,
                     strikes_per_expiry: int = STRIKES_PER_EXPIRY,
                     acquire: bool = True, batch: str = None) -> dict:
    """Sample SPY WEEKLY expiries inside the window the entitlement serves.

    Targets are ordered **most recent first**, and only expiries whose trading
    window can reach a date the combined surface does not already hold are
    worth a call.

    The first R46 batch got this wrong and it is worth recording why, because
    the failure was invisible: it iterated candidate expiries in ASCENDING
    date order, spent all 120 calls on the oldest fourteen (2024-09 to
    2025-01), and added 106 contracts, 2,195 rows - and **zero new session
    dates**, because the surface already covered every date those contracts
    traded on. A budget can be fully consumed, return real data, report
    success, and buy nothing at all.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = (OUT_DIR / ("polygon_spy_option_surface_r46_weeklies_%s.csv.gz"
                           % batch)) if batch else R46_SURFACE
    if out_path.exists():
        try:
            df = pd.read_csv(out_path, compression="gzip")
            return {"state": "EXECUTED", "from_cache": True,
                    "rows": int(len(df)),
                    "sessions": int(pd.to_datetime(df["date"]).nunique()),
                    "expiries": sorted(df["expiration"].unique().tolist()),
                    "path": str(out_path), "sha256": _sha(out_path),
                    "api_calls": 0, "money_spent_usd": 0.0}
        except Exception:                               # pragma: no cover
            pass
    if not acquire:
        return {"state": "SKIPPED", "why": "acquisition not requested",
                "api_calls": 0, "money_spent_usd": 0.0}

    from ..r43 import acquisition as R43AQ
    pk = R43AQ._key("POLYGON_API_KEY")
    if not pk:
        return {"state": "ACCOUNT_REQUIRED", "api_calls": 0,
                "money_spent_usd": 0.0,
                "why": "no owned Polygon key in the operator shell"}

    prev = existing_surface()
    mine = r46_batches()
    have = [f for f in (prev, mine) if f is not None]
    combined_prior = pd.concat(have, ignore_index=True) if have else None
    already = (set(combined_prior["expiration"].astype(str).unique())
               if combined_prior is not None else set())
    covered = (set(pd.to_datetime(combined_prior["date"]).dt.date.unique())
               if combined_prior is not None else set())
    latest_covered = max(covered) if covered else None

    today = _dt.date.today()
    lo = today - _dt.timedelta(days=ENTITLEMENT_LOOKBACK_DAYS)
    hi = today - _dt.timedelta(days=ENTITLEMENT_RECENT_EMBARGO_DAYS)
    #: MOST RECENT FIRST - an expiry whose whole trading window sits inside
    #: dates the surface already holds cannot add a session, however many
    #: contracts it carries.
    targets = [e for e in reversed(_weekly_expiries(lo, hi))
               if e.isoformat() not in already
               and (latest_covered is None or e > latest_covered)]

    calls, rows, errors, sampled = 0, [], [], []
    for exp in targets:
        if calls >= budget_calls:
            break
        s, b, _ = R43AQ.http_get(
            R43AQ.POLY + "/v3/reference/options/contracts"
            "?underlying_ticker=%s&expired=true&expiration_date=%s"
            "&contract_type=call&limit=1000&apiKey=%s"
            % (UNDERLYING, exp.isoformat(), pk))
        calls += 1
        time.sleep(R43AQ.POLYGON_RPM_SLEEP)
        if s != 200:
            errors.append({"expiration": exp.isoformat(), "status": s})
            continue
        sub = pd.DataFrame(json.loads(b).get("results") or [])
        if sub.empty:
            continue
        ks = sorted(sub["strike_price"].astype(float).unique())
        if len(ks) < strikes_per_expiry:
            continue
        mid, half = len(ks) // 2, strikes_per_expiry // 2
        sel = ks[max(0, mid - half): mid - half + strikes_per_expiry]
        sampled.append({"expiration": exp.isoformat(), "n_strikes": len(sel)})
        for k in sel:
            if calls >= budget_calls:
                break
            row = sub[sub["strike_price"].astype(float) == k].iloc[0]
            tk = row["ticker"]
            start = (exp - _dt.timedelta(days=120)).isoformat()
            s2, b2, _ = R43AQ.http_get(
                R43AQ.POLY + "/v2/aggs/ticker/%s/range/1/day/%s/%s"
                "?adjusted=true&limit=5000&apiKey=%s"
                % (tk, start, exp.isoformat(), pk))
            calls += 1
            time.sleep(R43AQ.POLYGON_RPM_SLEEP)
            if s2 != 200:
                errors.append({"ticker": tk, "status": s2})
                continue
            for a in (json.loads(b2).get("results") or []):
                rows.append({
                    "ticker": tk, "underlying": UNDERLYING,
                    "expiration": exp.isoformat(), "strike": float(k),
                    "type": "call",
                    "date": pd.to_datetime(a["t"], unit="ms").date()
                    .isoformat(),
                    "open": a.get("o"), "high": a.get("h"), "low": a.get("l"),
                    "close": a.get("c"), "vwap": a.get("vw"),
                    "volume": a.get("v"), "trades": a.get("n")})

    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "errors": errors[:15], "money_spent_usd": 0.0,
                "n_targets": len(targets),
                "why": "no aggregates returned inside the entitlement window"}

    frame = pd.DataFrame(rows)
    try:
        inv = R43AQ._invert_iv(frame, UNDERLYING)
        if inv is not None:
            frame = inv
    except Exception:                                   # pragma: no cover
        pass
    frame.to_csv(out_path, index=False, compression="gzip")
    got = set(pd.to_datetime(frame["date"]).dt.date.unique())
    return {"state": "EXECUTED", "from_cache": False, "api_calls": calls,
            "rows": int(len(frame)),
            "contracts": int(frame["ticker"].nunique()),
            "expiries": sorted(frame["expiration"].unique().tolist()),
            "n_strikes": int(frame["strike"].nunique()),
            "sessions": int(pd.to_datetime(frame["date"]).nunique()),
            "new_sessions_not_already_covered": len(got - covered),
            "latest_date_already_covered": (str(latest_covered)
                                            if latest_covered else None),
            "date_span": [str(frame["date"].min()), str(frame["date"].max())],
            "path": str(out_path), "sha256": _sha(out_path),
            "targets_considered": len(targets),
            "target_order": "MOST_RECENT_FIRST",
            "sampled": sampled, "errors": errors[:15],
            "money_spent_usd": 0.0,
            "backfilled_beyond_entitlement": False}


# --------------------------------------------------------------------------- #
def run(*, acquire: bool = True, campaign_id: str = CAMPAIGN_ID,
        batch: str = None, front_batch: str = None) -> dict:
    before = {"r44": _sha(R44_SURFACE), "r44_term": _sha(R44_SURFACE_TERM),
              "r45": _sha(R45_SURFACE)}

    prev = existing_surface()
    prior_sessions = (int(pd.to_datetime(prev["date"]).nunique())
                      if prev is not None else 0)
    prior_expiries = (set(prev["expiration"].astype(str).unique())
                      if prev is not None else set())
    prior_state = {
        "readable": prev is not None,
        "paths": {"r44": str(R44_SURFACE), "r44_term": str(R44_SURFACE_TERM),
                  "r45": str(R45_SURFACE)},
        "exists": {"r44": R44_SURFACE.exists(),
                   "r44_term": R44_SURFACE_TERM.exists(),
                   "r45": R45_SURFACE.exists()},
        "n_prior_sessions": prior_sessions,
        "n_prior_expiries": len(prior_expiries),
    }
    if prev is None:
        prior_state["WARNING"] = (
            "the prior option surfaces could not be read, so the reported "
            "session count is R46's OWN sample only and the expiry dedup "
            "could not fire. This is a defect, not an empty estate.")

    ext = acquire_weeklies(acquire=acquire, batch=batch)
    #: Release 46.6. Run only while the sample is short - once it is judgeable
    #: there is nothing to extend and the call is not made.
    front = None
    if prior_sessions < SESSIONS_REQUIRED:
        front = acquire_front_extension(acquire=acquire, batch=front_batch)

    combined = prev
    new = r46_batches()
    new_expiries = (set(new["expiration"].astype(str).unique())
                    if new is not None else set())
    overlap = sorted(new_expiries & prior_expiries)
    if new is not None:
        combined = (pd.concat([prev, new], ignore_index=True)
                    if prev is not None else new)
    sessions_now = (int(pd.to_datetime(combined["date"]).nunique())
                    if combined is not None else 0)

    after = {"r44": _sha(R44_SURFACE), "r44_term": _sha(R44_SURFACE_TERM),
             "r45": _sha(R45_SURFACE)}

    still_short = max(0, SESSIONS_REQUIRED - sessions_now)
    body = artifact_body(
        "r46_options_lane/1", CALCULATION_OWNER,
        question="did the free option surface reach a judgeable sample, and "
                 "are the hypotheses frozen before it does?",
        prior_release_artifacts_read_only=True,
        sha_before=before, sha_after=after,
        prior_artifacts_unchanged=(before == after),
        prior_surface_state=prior_state,
        acquisition=ext,
        front_extension=front,
        front_extension_rationale=(
            "the 499-session sample stopped at the last EXPIRED Friday "
            "because acquire_weeklies only considers expiries inside a "
            "3-day settlement embargo this estate imposed on itself. No "
            "contract inside that embargo trades after the last expired "
            "Friday, so the sample could not reach 500 however many old "
            "expiries were sampled. R46.6 probed the provider instead of "
            "assuming it: the owned plan serves DELAYED daily aggregates "
            "through T-1 for a currently-trading contract, which is exactly "
            "the missing session axis, free and already printed."),
        expiry_overlap_with_prior_releases=overlap,
        n_expiry_overlap=len(overlap),
        resampled_an_expiry_a_prior_release_already_paid_for=bool(overlap),
        surface={
            "sessions_before_r46": prior_sessions,
            "sessions_after_r46": sessions_now,
            "sessions_added_by_r46": max(0, sessions_now - prior_sessions),
            "n_expiries_added_by_r46": len(new_expiries),
            "n_rows": int(len(combined)) if combined is not None else 0,
            "n_contracts": (int(combined["ticker"].nunique())
                            if combined is not None else 0),
            "n_expiries": (int(combined["expiration"].nunique())
                           if combined is not None else 0),
        },
        judgeable_sample={
            "sessions_required": SESSIONS_REQUIRED,
            "requirement_is": "250 to fit on plus 250 never seen",
            "usable_sessions_now": sessions_now,
            "sessions_still_required": still_short,
            "state": "JUDGEABLE" if still_short == 0 else "STILL_SHORT",
            # Release 46.6.1 - semantic clarity, no science changed. This gate
            # counts DATES and nothing else. It has never measured whether a
            # predeclared hypothesis has the strikes and expiries it needs on
            # those dates; alpha_agent.r46.options_hypotheses answers that, and
            # its answer is reported separately rather than implied by this one.
            "session_gate_state": (SESSION_GATE_MET if still_short == 0
                                   else SESSION_GATE_SHORT),
            "gate_measures": "NUMBER_OF_SESSIONS_ONLY",
            "gate_does_not_measure": "STRIKE_AND_EXPIRY_BREADTH_PER_SESSION",
            "hypothesis_sample_sufficiency_is_answered_by":
                "alpha_agent.r46.options_hypotheses",
            "judgeable_here_means": (
                "the 500-session count is met. It does NOT mean any "
                "predeclared hypothesis has a sufficient sample."),
        },
        predeclared_hypotheses=[dict(h) for h in PREDECLARED_HYPOTHESES],
        n_predeclared=len(PREDECLARED_HYPOTHESES),
        hypotheses_hash=hypotheses_hash(),
        hypotheses_frozen_before_the_confirming_sessions_exist=True,
        why_predeclaration_matters=(
            "once the surface clears 500 sessions, whoever looks first can "
            "try skew, term structure and delta-hedged residuals, keep the "
            "one that worked, and call it a discovery. R45 measured what that "
            "is worth: the same 60-cell screen produced a different winner on "
            "each of three event zones. Writing the hypotheses down now, while "
            "the answer is unobservable, is the only defence."),
        excluded_by_name=EXCLUDED_BY_NAME,
        generic_short_vol_is_not_alpha=True,
        money_spent_usd=0.0,
        new_accounts=0, trials_started=0, licences_accepted=0,
        backfilled_unavailable_dates=False,
        existing_rows_preserved=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
