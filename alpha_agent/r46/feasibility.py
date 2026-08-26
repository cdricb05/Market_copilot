"""alpha_agent.r46.feasibility - can this challenger's stream actually accrue?

The gate this estate discovered and then ignored for three releases.

Release 42 probed the R41 BTC shadow's declared venue and wrote down, in an
artifact, that the Binance public archive publishes funding MONTHLY with a
24-day lag and that the venue's REST API answers HTTP 451 from this location -
so a DAILY shadow reading it cannot produce a daily row. The shadow stayed
nominally live through R43, R44 and R45 and produced nothing. Nothing in the
system was capable of telling an operator that a registered, frozen,
"prospective" model had emitted zero observations, because no registry asked.

R46 asks before it registers. A challenger whose declared data path cannot be
shown, in THIS run, to carry an observation recent enough to decide on is
registered ``DATA_BLOCKED`` with the reason attached, and it is excluded from
emission without blocking any other challenger.

The check is deliberately about the DATA PATH, not about the signal: a
challenger whose rule says "flat today" is perfectly feasible and simply has
nothing to trade.
"""
from __future__ import annotations

import datetime as _dt

from . import clock as CK
from . import contract as C
from . import marketdata as MD

CALCULATION_OWNER = "alpha_agent.r46.feasibility"

CAN_ACCRUE = "CAN_ACCRUE"
DATA_STALE = "DATA_STALE"
NO_DATA = "NO_DATA"
VENUE_BLOCKED = "VENUE_BLOCKED"
NOT_PROBED = "NOT_PROBED"

MAX_LAG = C.FEASIBILITY_RULE["max_data_lag_sessions"]

#: Representative instruments per signal owner. If these are fresh, the
#: challenger's stream is alive; if they are not, nothing it produces is
#: worth registering as active.
_PROBE_SYMBOLS = {
    "_eq_cross_section": ("SPY", "AAPL", "MSFT"),
    "_futures_trend": ("&ES", "&ZN", "&CL", "&GC"),
    "_fx_cross_section": ("EURUSD", "JPYUSD", "GBPUSD"),
    "_vx_carry": ("&VX", "$VIX"),
    "_rates_rv": ("&ZN", "&ZT"),
    "_commodity_cross_section": ("&CL", "&GC", "&ZC"),
    "_index_trend": ("SPY",),
    # Release 46.3 expansion owners. The macro-curve owner probes the OWNED
    # yield series itself: constant-maturity yields publish one session behind
    # prices, which is why the freshness allowance exists at all.
    "_eq_xs_lottery": ("SPY", "AAPL", "MSFT"),
    "_eq_xs_illiquidity": ("SPY", "AAPL", "MSFT"),
    "_eq_xs_seasonal": ("SPY", "AAPL", "MSFT"),
    "_futures_xs_momentum": ("&ES", "&ZN", "&CL", "&GC"),
    "_commodity_curve_carry": ("&CL", "&GC", "&ZC"),
    "_rates_macro_curve": ("&ZN", "%10YTCM", "%2YTCM"),
    "_spx_turn_of_month": ("SPY",),
    "_eq_xs_ensemble": ("SPY", "AAPL", "MSFT"),
    "_ml_eq_cross_section": ("SPY", "AAPL", "MSFT"),
}


def _weekdays_between(a: _dt.date, b: _dt.date) -> int:
    if b <= a:
        return 0
    n, d = 0, a
    while d < b:
        d += _dt.timedelta(days=1)
        if d.weekday() < 5:
            n += 1
    return n


def probe_symbol(symbol: str, reference: _dt.date) -> dict:
    last = MD.last_session(symbol)
    if last is None:
        return {"instrument": symbol, "state": NO_DATA, "last_session": None,
                "lag_sessions": None}
    lag = _weekdays_between(last, reference)
    state = CAN_ACCRUE if lag <= MAX_LAG else DATA_STALE
    return {"instrument": symbol, "state": state, "last_session": str(last),
            "lag_sessions": lag}


def probe(spec: dict, reference: _dt.date = None) -> dict:
    """Probe one challenger's declared data path.

    ``reference`` is the Eastern calendar date the freshness is measured
    against - the emission date, not "today" in some other zone.
    """
    ref = reference or CK.eastern_date(CK.now_utc())
    symbols = _PROBE_SYMBOLS.get(spec["signal_owner"], ())
    if not symbols:
        return {"challenger_id": spec["challenger_id"], "state": NOT_PROBED,
                "reason": "no declared probe symbols", "probes": []}
    probes = [probe_symbol(s, ref) for s in symbols]
    states = {p["state"] for p in probes}
    if NO_DATA in states:
        state = NO_DATA
        reason = "at least one declared instrument returned no bars"
    elif states == {CAN_ACCRUE}:
        state = CAN_ACCRUE
        reason = "every declared instrument carries a bar within %d sessions" \
                 % MAX_LAG
    else:
        state = DATA_STALE
        reason = "at least one declared instrument is more than %d sessions " \
                 "stale" % MAX_LAG
    lags = [p["lag_sessions"] for p in probes if p["lag_sessions"] is not None]
    return {
        "challenger_id": spec["challenger_id"],
        "signal_owner": spec["signal_owner"],
        "state": state,
        "reason": reason,
        "reference_date": str(ref),
        "max_lag_allowed_sessions": MAX_LAG,
        "worst_lag_sessions": max(lags) if lags else None,
        "last_session_seen": max((p["last_session"] for p in probes
                                  if p["last_session"]), default=None),
        "probes": probes,
    }


def probe_all(specs, reference: _dt.date = None) -> dict:
    ref = reference or CK.eastern_date(CK.now_utc())
    results = [probe(s, ref) for s in specs]
    return {
        "schema": "r46_feasibility/1",
        "calculation_owner": CALCULATION_OWNER,
        "rule": C.FEASIBILITY_RULE,
        "reference_date": str(ref),
        "provider_state": MD.provider_state(),
        "n_can_accrue": sum(1 for r in results if r["state"] == CAN_ACCRUE),
        "n_blocked": sum(1 for r in results if r["state"] != CAN_ACCRUE),
        "results": results,
    }


def adopted_stream_state(shadow: dict) -> dict:
    """Why an ADOPTED prior-release shadow is or is not accruing.

    R46 never writes a forward row for an adopted shadow - that stays with its
    own owner. What R46 owes the operator is the plain reason the row count is
    what it is, in one place, instead of spread across five campaign roots.
    """
    sid = str(shadow.get("shadow_id") or shadow.get("id") or "")
    rel = shadow.get("source_release")
    if rel in ("R41", "R42") or "btc" in sid.lower() or "BTC" in sid:
        return {
            "state": VENUE_BLOCKED,
            "reason": "the declared venue's public archive publishes funding "
                      "MONTHLY with a ~24-day lag and its REST API answers "
                      "HTTP 451 from this location; a DAILY shadow reading it "
                      "cannot produce a daily row",
            "measured_by": "alpha_agent.r42.forward (Release 42)",
            "evidence": "r41_stream_feasibility in R42 FORWARD_EVIDENCE.json",
            "can_accrue_today": False,
        }
    if rel in ("R39", "R40"):
        return {
            "state": NOT_PROBED,
            "reason": "decides at per-market month-end or on VX Fridays "
                      "through its own capture owner; no run has called that "
                      "owner since the freeze, so its ledgers hold zero rows",
            "measured_by": "alpha_agent.r40.research_cycle "
                           "(forward_capture_ledger_status.json, n_rows 0)",
            "can_accrue_today": False,
        }
    return {"state": NOT_PROBED, "reason": "no declared probe for this source",
            "can_accrue_today": False}
