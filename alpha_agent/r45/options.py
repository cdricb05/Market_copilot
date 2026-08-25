"""alpha_agent.r45.options - Track L. Deepen the free surface, buy nothing.

Release 44 built a real SPY option surface out of the free window - 163
dated contracts, 72 strikes, calls and puts, implied volatility inverted
locally from the estate's own underlying and its own rate - and stopped
about 107 sessions short of the frozen judgeable minimum.

There are two ways to close a session gap. One is to buy history, which is
forbidden here and was already priced by Release 44. The other is free and
was never used: R44 sampled six widely-spaced expiries, so most dates in its
window carry only a thin slice of surface. Sampling MORE expiries inside the
SAME free window adds sessions without reaching past the entitlement
boundary by a single day.

That is what this module does. It never backfills, it never asks for a date
the plan does not serve, and it treats Release 44's file as read-only - the
hash is taken before and after, and it is reported.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import time
from pathlib import Path

import pandas as pd

from ..r43 import acquisition as R43AQ
from ..r44 import options as R44OPT
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.options"

R44_SURFACE = Path(
    r"D:\Stock_Prediction_app_data\orthogonal_portfolio_alpha_r44"
    r"\_data_options\polygon_spy_option_surface.csv.gz")
R44_SURFACE_TERM = R44_SURFACE.with_name(
    "polygon_spy_option_surface_term.csv.gz")
OUT_DIR = C.RESEARCH_ROOT / "_data_options"
R45_SURFACE = OUT_DIR / "polygon_spy_option_surface_r45_extension.csv.gz"

UNDERLYING = "SPY"
STRIKES_PER_EXPIRY = 10
CALL_BUDGET = 130
#: The bar is R44's, and it is the SUM of its two halves: a variance-risk
#: study needs 250 sessions to fit on and 250 more it has never seen. R44
#: recorded itself 107 sessions short against 500; quoting the 250 alone
#: would let this release declare victory by halving the requirement.
MIN_FIT_SESSIONS = 250
MIN_JUDGED_SESSIONS = 250
SESSIONS_REQUIRED = MIN_FIT_SESSIONS + MIN_JUDGED_SESSIONS
R44_SESSIONS_SHORT_BY = 107


def _sha(p: Path):
    if not p.exists():
        return None
    h = hashlib.sha256()
    with open(p, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _read_r44_surface():
    frames = []
    for p in (R44_SURFACE, R44_SURFACE_TERM):
        if p.exists():
            df = pd.read_csv(p, compression="gzip")
            df["_source"] = "R44"
            frames.append(df)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _third_fridays(lo: _dt.date, hi: _dt.date) -> list:
    out, d = [], _dt.date(lo.year, lo.month, 1)
    while d <= hi:
        fri = [x for x in
               (_dt.date(d.year, d.month, k) for k in range(15, 22))
               if x.weekday() == 4]
        if fri and lo <= fri[0] <= hi:
            out.append(fri[0])
        d = _dt.date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
    return out


def acquire_extension(*, budget_calls: int = CALL_BUDGET,
                      strikes_per_expiry: int = STRIKES_PER_EXPIRY) -> dict:
    """Expiries R44 did NOT sample, inside the window the plan already serves."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if R45_SURFACE.exists():
        try:
            df = pd.read_csv(R45_SURFACE, compression="gzip")
            return {"state": "EXECUTED", "from_cache": True,
                    "rows": int(len(df)),
                    "expiries": sorted(df["expiration"].unique().tolist()),
                    "sessions": int(pd.to_datetime(df["date"]).nunique()),
                    "path": str(R45_SURFACE), "sha256": _sha(R45_SURFACE),
                    "api_calls": 0}
        except Exception:                               # pragma: no cover
            pass

    pk = R43AQ._key("POLYGON_API_KEY")
    if not pk:
        return {"state": "ACCOUNT_REQUIRED",
                "why": "no owned Polygon key in the operator shell"}

    prev = _read_r44_surface()
    already = set(prev["expiration"].astype(str).unique()) if prev is not None \
        else set()

    today = _dt.date.today()
    lo = today - _dt.timedelta(days=700)
    targets = [e for e in _third_fridays(lo, today - _dt.timedelta(days=20))
               if e.isoformat() not in already]

    calls, rows, picked, errors = 0, [], [], []
    for exp in targets:
        for ctype in ("call", "put"):
            if calls >= budget_calls:
                break
            s, b, _ = R43AQ.http_get(
                R43AQ.POLY + "/v3/reference/options/contracts"
                "?underlying_ticker=%s&expired=true&expiration_date=%s"
                "&contract_type=%s&limit=1000&apiKey=%s"
                % (UNDERLYING, exp.isoformat(), ctype, pk))
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
            picked.append({"expiration": exp.isoformat(), "type": ctype,
                           "n_strikes": len(sel)})
            for k in sel:
                if calls >= budget_calls:
                    break
                row = sub[sub["strike_price"].astype(float) == k].iloc[0]
                tk = row["ticker"]
                start = (exp - _dt.timedelta(days=200)).isoformat()
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
                        "type": ctype,
                        "date": pd.to_datetime(a["t"], unit="ms").date()
                        .isoformat(),
                        "open": a.get("o"), "high": a.get("h"),
                        "low": a.get("l"), "close": a.get("c"),
                        "vwap": a.get("vw"), "volume": a.get("v"),
                        "trades": a.get("n")})
        if calls >= budget_calls:
            break

    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "errors": errors[:15],
                "why": "no aggregates returned inside the entitlement window"}

    frame = pd.DataFrame(rows)
    inv = R43AQ._invert_iv(frame, UNDERLYING)
    if inv is not None:
        frame = inv
    frame.to_csv(R45_SURFACE, index=False, compression="gzip")
    return {"state": "EXECUTED", "from_cache": False,
            "api_calls": calls, "rows": int(len(frame)),
            "contracts": int(frame["ticker"].nunique()),
            "expiries": sorted(frame["expiration"].unique().tolist()),
            "n_strikes": int(frame["strike"].nunique()),
            "sessions": int(pd.to_datetime(frame["date"]).nunique()),
            "date_span": [str(frame["date"].min()), str(frame["date"].max())],
            "path": str(R45_SURFACE), "sha256": _sha(R45_SURFACE),
            "iv_method": "Black-Scholes bisection on the option's own close, "
                         "the OWNED underlying close and the OWNED rate",
            "errors": errors[:15],
            "backfilled_beyond_entitlement": False}


def combined_surface():
    frames = []
    prev = _read_r44_surface()
    if prev is not None:
        frames.append(prev)
    if R45_SURFACE.exists():
        df = pd.read_csv(R45_SURFACE, compression="gzip")
        df["_source"] = "R45"
        frames.append(df)
    if not frames:
        return None
    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    out["expiration"] = pd.to_datetime(out["expiration"])
    return out.drop_duplicates(subset=["ticker", "date"], keep="first")


def run(*, acquire: bool = True) -> dict:
    before = {"r44_surface": _sha(R44_SURFACE),
              "r44_surface_term": _sha(R44_SURFACE_TERM)}
    acq = acquire_extension() if acquire else {"state": "SKIPPED"}
    after = {"r44_surface": _sha(R44_SURFACE),
             "r44_surface_term": _sha(R44_SURFACE_TERM)}

    df = combined_surface()
    if df is None:
        return {"track": "L", "state": "HISTORICAL_DATA_UNAVAILABLE",
                "acquisition": acq}

    sessions = int(df["date"].nunique())
    r44_only = df[df["_source"] == "R44"]
    r44_sessions = int(r44_only["date"].nunique()) if len(r44_only) else 0

    state = R44OPT.surface_state(df)
    vrp = R44OPT.variance_risk_premium(state)
    skew = R44OPT.skew_diagnostic(state)

    # The number that decides judgeability is the one the VRP diagnostic can
    # actually use, not the count of dates on which any contract traded.
    usable = None
    for node in (vrp, skew):
        if isinstance(node, dict) and node.get("n_sessions") is not None:
            usable = int(node["n_sessions"])
            break
    if usable is None:
        usable = sessions
    short_by = max(0, SESSIONS_REQUIRED - usable)

    return {
        "track": "L", "state": "EXECUTED",
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["L13_OPTIONS"],
        "acquisition": acq,
        "r44_artifacts_read_only": {
            "sha_before": before, "sha_after": after,
            "unchanged": before == after},
        "surface": {
            "n_rows": int(len(df)),
            "n_contracts": int(df["ticker"].nunique()),
            "n_strikes": int(df["strike"].nunique()),
            "n_expiries": int(df["expiration"].nunique()),
            "sessions_total": sessions,
            "sessions_from_r44": r44_sessions,
            "sessions_added_by_r45": sessions - r44_sessions,
            "date_span": [str(df["date"].min().date()),
                          str(df["date"].max().date())],
        },
        "judgeable_sample": {
            "sessions_required": SESSIONS_REQUIRED,
            "requirement_is": f"{MIN_FIT_SESSIONS} to fit on plus "
                              f"{MIN_JUDGED_SESSIONS} never seen",
            "usable_sessions_now": usable,
            "raw_sessions_now": sessions,
            "sessions_still_required": short_by,
            "r44_was_short_by": R44_SESSIONS_SHORT_BY,
            "closed_by_r45_at_zero_cost": max(
                0, R44_SESSIONS_SHORT_BY - short_by),
            "state": "JUDGEABLE" if short_by == 0 else "STILL_SHORT",
        },
        "variance_risk_premium": vrp,
        "skew_diagnostic": skew,
        "generic_short_vol_is_not_alpha": True,
        "blocker": "EXECUTED" if short_by == 0
        else "HISTORICAL_DATA_UNAVAILABLE",
        "money_spent_usd": 0.0,
    }
