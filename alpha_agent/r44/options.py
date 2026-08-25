"""alpha_agent.r44.options - ENGINE 1A, the option surface.

Release 43 proved the PIPELINE: expired option contracts are enumerable at
$0, real prices come back, and implied volatility inverts locally from the
option's own close plus an owned underlying and an owned rate - so vendor
greeks are not the wall. What it did not build was a SURFACE. Thirty
contracts at five strikes per expiry is a probe, not a smile.

This module continues from that working sample rather than restarting from
vendor research, and it deepens the acquisition in the two directions that
turn a probe into a surface:

  * STRIKES, so there is a smile and a skew rather than a single price;
  * PUTS as well as calls, because the risk-reversal - the price of the
    downside relative to the upside - is where the equity index option
    market actually carries information.

What it cannot do is manufacture history. The free entitlement is a rolling
window of roughly two years. The contract requires 250 fitting sessions
plus 250 judged sessions, and the frozen rule
``A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_QUALIFY`` says exactly what that
means here: this lane may measure, and may not qualify. It reports how much
more history each hypothesis needs, in months, so the purchase gate has a
number rather than an intuition.
"""
from __future__ import annotations

import datetime as _dt
import json
import time
from pathlib import Path

import numpy as np
import pandas as pd

from ..r43 import acquisition as AQ
from ..r43 import panels as P
from . import contract as C
from . import data_dir

CALCULATION_OWNER = "alpha_agent.r44.options"
MANIFEST = "acquisition_manifest.json"
SURFACE_CSV = "polygon_spy_option_surface.csv.gz"

UNDERLYING = "SPY"
N_EXPIRIES = 6
STRIKES_PER_EXPIRY = 14
CONTRACT_TYPES = ("call", "put")
CALL_BUDGET = 200


# --------------------------------------------------------------------------- #
# Acquisition
# --------------------------------------------------------------------------- #
def _out_dir() -> Path:
    return data_dir("options")


def _monthly_third_fridays(lo: _dt.date, hi: _dt.date) -> list:
    """EVERY third Friday in the window, not an evenly spaced sample.

    The first acquisition pass took six evenly spaced expiries, which are
    roughly quarterly. Quarterly expiries whose strikes were chosen at
    listing time are almost never both at-the-money on the same date, so
    that surface had a smile and NO term structure. Adjacent monthly
    expiries overlap, which is what a term structure needs.
    """
    out, d = [], lo
    while d <= hi:
        first = _dt.date(d.year, d.month, 1)
        offset = (4 - first.weekday()) % 7
        third = first + _dt.timedelta(days=offset + 14)
        if lo <= third <= hi:
            out.append(third)
        d = _dt.date(d.year + (d.month == 12), (d.month % 12) + 1, 1)
    return out


def acquire_surface(*, underlying: str = UNDERLYING,
                    n_expiries: int = N_EXPIRIES,
                    strikes_per_expiry: int = STRIKES_PER_EXPIRY,
                    budget_calls: int = CALL_BUDGET,
                    expiries: list = None,
                    manifest_key: str = "polygon_option_surface",
                    out_name: str = SURFACE_CSV,
                    refresh: bool = False) -> dict:
    """Deepen R43's bounded sample into a strike x expiry x type surface."""
    out_dir = _out_dir()
    cached = out_dir / MANIFEST
    if cached.exists() and not refresh:
        try:
            prev = json.loads(cached.read_text(encoding="utf-8"))
            man = (prev.get(manifest_key) or {})
            if man.get("state") == "EXECUTED" and man.get("rows"):
                man["from_cache"] = True
                return man
        except Exception:
            pass

    pk = AQ._key("POLYGON_API_KEY")
    if not pk:
        return {"state": "ACCOUNT_REQUIRED",
                "reason": "no owned Polygon key in the operator shell"}

    calls = 0
    today = _dt.date.today()
    lo = today - _dt.timedelta(days=700)
    if expiries is not None:
        targets = list(expiries)
    else:
        targets = AQ._third_fridays(lo, today - _dt.timedelta(days=20),
                                    n_expiries)

    rows, picked, errors = [], [], []
    for exp in targets:
        for ctype in CONTRACT_TYPES:
            if calls >= budget_calls:
                break
            s, b, _ = AQ.http_get(
                AQ.POLY + "/v3/reference/options/contracts"
                "?underlying_ticker=%s&expired=true&expiration_date=%s"
                "&contract_type=%s&limit=1000&apiKey=%s"
                % (underlying, exp.isoformat(), ctype, pk))
            calls += 1
            time.sleep(AQ.POLYGON_RPM_SLEEP)
            if s != 200:
                errors.append({"expiration": exp.isoformat(), "type": ctype,
                               "status": s})
                continue
            sub = pd.DataFrame(json.loads(b).get("results") or [])
            if sub.empty:
                continue
            ks = sorted(sub["strike_price"].astype(float).unique())
            if len(ks) < strikes_per_expiry:
                continue
            # Strikes are taken symmetrically around the middle of the listed
            # range, which is where the money was when the chain was listed.
            # No strike is chosen by looking at what it later did.
            mid = len(ks) // 2
            half = strikes_per_expiry // 2
            sel = ks[max(0, mid - half): mid - half + strikes_per_expiry]
            picked.append({"expiration": exp.isoformat(), "type": ctype,
                           "strikes": [float(x) for x in sel],
                           "contracts_available": int(len(sub))})
            for k in sel:
                if calls >= budget_calls:
                    break
                row = sub[sub["strike_price"].astype(float) == k].iloc[0]
                tk = row["ticker"]
                start = (exp - _dt.timedelta(days=200)).isoformat()
                s2, b2, _ = AQ.http_get(
                    AQ.POLY + "/v2/aggs/ticker/%s/range/1/day/%s/%s"
                    "?adjusted=true&limit=5000&apiKey=%s"
                    % (tk, start, exp.isoformat(), pk))
                calls += 1
                time.sleep(AQ.POLYGON_RPM_SLEEP)
                if s2 != 200:
                    errors.append({"ticker": tk, "status": s2})
                    continue
                for a in (json.loads(b2).get("results") or []):
                    rows.append({
                        "ticker": tk, "underlying": underlying,
                        "expiration": exp.isoformat(), "strike": float(k),
                        "type": ctype,
                        "date": pd.to_datetime(a["t"], unit="ms").date()
                        .isoformat(),
                        "open": a.get("o"), "high": a.get("h"),
                        "low": a.get("l"), "close": a.get("c"),
                        "vwap": a.get("vw"), "volume": a.get("v"),
                        "trades": a.get("n")})

    if not rows:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE", "api_calls": calls,
                "errors": errors[:20],
                "reason": "no aggregates returned inside the plan window"}

    frame = pd.DataFrame(rows)
    inv = AQ._invert_iv(frame, underlying)
    if inv is not None:
        frame = inv
    path = out_dir / out_name
    frame.to_csv(path, index=False, compression="gzip")

    from . import sha_file
    man = {
        "state": "EXECUTED", "provider": "polygon", "underlying": underlying,
        "api_calls": calls, "rows": int(len(frame)),
        "contracts": int(frame["ticker"].nunique()),
        "expiries": sorted(frame["expiration"].unique().tolist()),
        "types": sorted(frame["type"].unique().tolist()),
        "n_strikes": int(frame["strike"].nunique()),
        "date_span": [str(frame["date"].min()), str(frame["date"].max())],
        "sessions": int(pd.to_datetime(frame["date"]).nunique()),
        "iv": AQ._iv_summary(frame) if "iv" in frame.columns else None,
        "path": str(path), "sha256": sha_file(path),
        "picked": picked, "errors": errors[:20],
        "greeks_from_vendor": False,
        "iv_method": C.OPTION_IV_METHOD,
        "pit_semantics": "daily consolidated OHLCV per DATED contract; the "
                         "identifier carries strike and expiry and expired "
                         "contracts stay enumerable, so the universe is "
                         "survivorship-safe",
        "acquired_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
    }
    prev = {}
    if cached.exists():
        try:
            prev = json.loads(cached.read_text(encoding="utf-8"))
        except Exception:
            prev = {}
    prev[manifest_key] = man
    cached.write_text(json.dumps(prev, indent=1, default=str),
                      encoding="utf-8")
    return man


SURFACE_CSV_TERM = "polygon_spy_option_surface_term.csv.gz"


def acquire_term_pass(*, budget_calls: int = 170,
                      strikes_per_expiry: int = 10) -> dict:
    """A second pass over the MONTHLY expiries the first pass skipped."""
    d = _out_dir()
    have = set()
    p = d / SURFACE_CSV
    if p.exists():
        have = set(pd.read_csv(p, usecols=["expiration"])["expiration"]
                   .astype(str).unique())
    today = _dt.date.today()
    lo = today - _dt.timedelta(days=700)
    want = [x for x in _monthly_third_fridays(
        lo, today - _dt.timedelta(days=20)) if x.isoformat() not in have]
    if not want:
        return {"state": "NOTHING_TO_ADD"}
    return acquire_surface(expiries=want,
                           strikes_per_expiry=strikes_per_expiry,
                           budget_calls=budget_calls,
                           manifest_key="polygon_option_surface_term",
                           out_name=SURFACE_CSV_TERM)


def load_surface() -> pd.DataFrame:
    """Both acquisition passes, concatenated and de-duplicated."""
    d = _out_dir()
    frames = []
    for name in (SURFACE_CSV, SURFACE_CSV_TERM):
        p = d / name
        if p.exists():
            frames.append(pd.read_csv(p))
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True)
    df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
    df["date"] = pd.to_datetime(df["date"])
    df["expiration"] = pd.to_datetime(df["expiration"])
    return df


# --------------------------------------------------------------------------- #
# Surface state
# --------------------------------------------------------------------------- #
def surface_state(df: pd.DataFrame = None) -> dict:
    """The canonical local option-state representation, and what it supports.

    Per date this yields, where the data allow: an ATM implied volatility per
    expiry (hence a TERM STRUCTURE), an implied volatility per moneyness
    bucket (hence a SMILE and a SKEW), and the count behind each - because a
    smile computed from two strikes is a line, not a smile.
    """
    df = df if df is not None else load_surface()
    if df is None or df.empty:
        return {"state": "HISTORICAL_DATA_UNAVAILABLE"}
    d = df.dropna(subset=["iv"]).copy()
    if d.empty:
        return {"state": "IV_INVERSION_EMPTY"}
    d["dte"] = (d["expiration"] - d["date"]).dt.days
    d = d[(d["dte"] > 5) & (d["dte"] < 400)]
    d["log_m"] = np.log(d["strike"] / d["underlying_close"])

    atm = d[d["moneyness"].between(0.97, 1.03)]
    term = (atm.groupby(["date", "expiration"])["iv"].mean()
            .rename("atm_iv").reset_index())
    term["dte"] = (term["expiration"] - term["date"]).dt.days

    # Risk reversal: the price of a 5%-out put over a 5%-out call, same date
    # and expiry. This is the number the owned VIX complex cannot express.
    puts = d[(d["type"] == "put") & d["moneyness"].between(0.90, 0.97)]
    calls = d[(d["type"] == "call") & d["moneyness"].between(1.03, 1.10)]
    pv = puts.groupby("date")["iv"].mean().rename("put_iv")
    cv = calls.groupby("date")["iv"].mean().rename("call_iv")
    # sort=True is explicit: these are date-indexed series and chronological
    # order is the intended result, not a pandas default that is changing.
    rr = pd.concat([pv, cv], axis=1, sort=True).dropna()
    rr["risk_reversal"] = rr["put_iv"] - rr["call_iv"]

    buckets = []
    for lo, hi in C.OPTION_MONEYNESS_BUCKETS:
        b = d[d["moneyness"].between(lo, hi)]
        if len(b):
            buckets.append({"bucket": [lo, hi], "n": int(len(b)),
                            "median_iv": float(b["iv"].median()),
                            "n_dates": int(b["date"].nunique())})

    sessions = int(d["date"].nunique())
    need_fit = C.OPTION_MIN_FIT_SESSIONS
    need_judge = C.OPTION_MIN_JUDGED_SESSIONS
    return {
        "state": "MEASURED",
        "calculation_owner": CALCULATION_OWNER,
        "n_rows": int(len(d)),
        "n_sessions": sessions,
        "n_expiries": int(d["expiration"].nunique()),
        "n_strikes": int(d["strike"].nunique()),
        "date_span": [str(d["date"].min())[:10], str(d["date"].max())[:10]],
        "moneyness_buckets": buckets,
        "term_structure_dates": int(term["date"].nunique()),
        "dates_with_two_or_more_expiries": int(
            (term.groupby("date")["expiration"].nunique() >= 2).sum()),
        "risk_reversal_dates": int(len(rr)),
        "risk_reversal_median": (float(rr["risk_reversal"].median())
                                 if len(rr) else None),
        "sessions_required": need_fit + need_judge,
        "sessions_short_by": max(0, need_fit + need_judge - sessions),
        "additional_months_required": round(
            max(0, need_fit + need_judge - sessions) / 21.0, 1),
        "may_qualify": False,
        "may_qualify_reason": "A_SHORT_WINDOW_MAY_DIAGNOSE_AND_MAY_NOT_"
                              "QUALIFY - the free entitlement is a rolling "
                              "~2 year window",
        "_term": term, "_rr": rr,
    }


# --------------------------------------------------------------------------- #
# Bounded diagnostics
# --------------------------------------------------------------------------- #
def variance_risk_premium(state: dict = None) -> dict:
    """ATM implied volatility against the volatility that then happened.

    Measured from ACTUAL option prices rather than from the VIX index, which
    is the only thing this lane can do that the owned CBOE complex cannot.
    It is a DIAGNOSTIC: a positive premium here is the textbook result and
    the contract already forbids counting generic short-vol as Alpha.
    """
    state = state or surface_state()
    if state.get("state") != "MEASURED":
        return {"state": state.get("state")}
    term = state["_term"]
    near = term[term["dte"].between(20, 45)]
    if near.empty:
        return {"state": "INSUFFICIENT_TERM_COVERAGE"}
    iv = near.groupby("date")["atm_iv"].mean().sort_index()

    spy = P.futures_daily("ES")
    px = None
    if spy is not None and "c1" in spy.columns:
        px = pd.to_numeric(spy["c1"], errors="coerce")
        px.index = pd.DatetimeIndex(spy.index)
    if px is None or px.empty:
        return {"state": "NO_OWNED_UNDERLYING"}
    ret = np.log(px).diff()
    fwd_rv = (ret.rolling(21).std().shift(-21) * np.sqrt(252))
    j = pd.concat([iv.rename("iv"), fwd_rv.rename("rv")], axis=1,
                  sort=True).dropna()
    if len(j) < 60:
        return {"state": "INSUFFICIENT_OVERLAP", "n": int(len(j))}
    vrp = j["iv"] - j["rv"]
    from ..r41 import evidence as EV
    hac = EV.hac_t(vrp.to_numpy(dtype=float), lags=21)
    return {
        "state": "MEASURED", "n": int(len(j)),
        "mean_atm_iv": float(j["iv"].mean()),
        "mean_forward_realised_vol": float(j["rv"].mean()),
        "variance_risk_premium_vol_points": float(vrp.mean()),
        "t_hac": hac.get("t"),
        "measured_from": "ACTUAL option prices with locally inverted IV",
        "underlying_proxy": "owned ES front-month close",
        "is_alpha": False,
        "why_not_alpha": "generic short-volatility premium is explicitly "
                         "excluded by the release contract",
    }


def skew_diagnostic(state: dict = None) -> dict:
    """Does the risk reversal move with the forward return of the index?"""
    state = state or surface_state()
    if state.get("state") != "MEASURED":
        return {"state": state.get("state")}
    rr = state["_rr"]
    if len(rr) < 60:
        return {"state": "INSUFFICIENT_RISK_REVERSAL_DATES", "n": int(len(rr))}
    es = P.futures_daily("ES")
    if es is None or "ret1" not in es.columns:
        return {"state": "NO_OWNED_UNDERLYING"}
    r = pd.to_numeric(es["ret1"], errors="coerce")
    r.index = pd.DatetimeIndex(es.index)
    out = {}
    from ..r41 import evidence as EV
    for h in (5, 21):
        fwd = r.rolling(h).sum().shift(-h)
        j = pd.concat([rr["risk_reversal"].rename("rr"),
                       fwd.rename("fwd")], axis=1, sort=True).dropna()
        if len(j) < 60:
            continue
        z = (j["rr"] - j["rr"].expanding(30).mean()) \
            / j["rr"].expanding(30).std()
        pnl = (-np.sign(z.shift(1)) * j["fwd"]).dropna()
        hac = EV.hac_t(pnl.to_numpy(dtype=float), lags=h)
        out["h%d" % h] = {
            "n": int(len(pnl)),
            "corr_rr_fwd": float(j["rr"].corr(j["fwd"])),
            "mean_per_period": float(pnl.mean()),
            "t_hac": hac.get("t"),
        }
    return {"state": "MEASURED", "horizons": out,
            "signal": "fade a rich downside (high risk reversal)",
            "may_qualify": False,
            "note": "bounded window; reported as a direction, not a claim"}


def run(*, acquire: bool = True) -> dict:
    man = (acquire_surface() if acquire
           else {"state": "SKIPPED_BY_CALLER"})
    state = surface_state()
    vrp = variance_risk_premium(state) if state.get("state") == "MEASURED" \
        else {"state": state.get("state")}
    skew = skew_diagnostic(state) if state.get("state") == "MEASURED" \
        else {"state": state.get("state")}
    clean = {k: v for k, v in state.items() if not k.startswith("_")}
    blocked = clean.get("sessions_short_by", 1) > 0
    return {
        "lane": "E1A_OPTIONS_SURFACE",
        "state": ("EXECUTED" if man.get("state") == "EXECUTED"
                  else man.get("state", "HISTORICAL_DATA_UNAVAILABLE")),
        "calculation_owner": CALCULATION_OWNER,
        "question": C.LANES["E1A_OPTIONS_SURFACE"],
        "acquisition": man,
        "surface": clean,
        "variance_risk_premium": vrp,
        "skew": skew,
        "qualification_blocked": bool(blocked),
        "blocker": ("HISTORICAL_DATA_UNAVAILABLE" if blocked else None),
        "burden_charged": False,
        "why_no_burden": "a lane that cannot reach a judged zone cannot "
                         "advance a candidate, and an unadvanced candidate "
                         "costs no burden",
    }
