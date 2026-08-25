"""alpha_agent.r44.streams - ENGINE 2A, the residual-stream inventory.

Every stream declared in :data:`alpha_agent.r44.contract.STREAMS` is rebuilt
here from its ORIGINAL R43 owner - the same structures, the same windows, the
same cost groups, the same capital models. Nothing is re-implemented, because
a stream that has been quietly re-coded is a new stream wearing an old
lineage.

Three properties matter and are enforced rather than asserted:

1. **The inventory is fixed by economics.** The contract names the streams
   before any of them is scored, and :func:`build_all` builds ALL of them.
   A stream that turns out to lose money is still in the portfolio. That is
   the entire point - the selection step is what makes most published
   "portfolio of alphas" results meaningless.

2. **Every stream is returned as COMPONENTS, not as a scored number.** The
   gross stream, the cost stream, the turnover and the committed capital are
   handed to :mod:`alpha_agent.r44.portfolio` intact, so cost stress at x2
   and x3 acts on the real cost term instead of on a rescaled summary.

3. **Every stream carries its own collateral class**, so the judge charges a
   crypto cash-and-carry for the risk-free rate its collateral does not earn
   and does NOT charge a margin-financed futures spread for the same thing.
   That distinction is R43's central correction and it is inherited here
   rather than re-derived.
"""
from __future__ import annotations

import datetime as _dt
import pickle
from pathlib import Path

import numpy as np
import pandas as pd

from ..r41 import evidence as EV
from ..r43 import acquisition as AQ
from ..r43 import carry as KARRY
from ..r43 import crossasset as XA
from ..r43 import equity as EQ
from ..r43 import judge as J
from ..r43 import rv as RV
from . import contract as C
from . import data_dir

CALCULATION_OWNER = "alpha_agent.r44.streams"
CACHE_NAME = "r44_streams.pkl"

#: The collateral class each economic expression is priced on. Declared in
#: the contract; restated here as a lookup so no builder can pick its own.
COLLATERAL_BY_EXPRESSION = {
    "FUTURES_CROSS_MARKET_RV": "REMUNERATED_MARGIN",
    "FUTURES_CURVE_SPREAD": "REMUNERATED_MARGIN",
    "FUTURES_OUTRIGHT": "REMUNERATED_MARGIN",
    "FX_FUTURES_CARRY": "REMUNERATED_MARGIN",
    "VX_TERM_STRUCTURE": "REMUNERATED_MARGIN",
    "EQUITY_MARKET_NEUTRAL": "FUNDED_LONG_SHORT_EQUITY",
    "CRYPTO_CASH_AND_CARRY": "UNREMUNERATED_FULLY_FUNDED",
}

_MEM: dict = {}


def _naive_days(idx) -> pd.DatetimeIndex:
    """One calendar convention for every stream.

    The futures store is tz-naive session dates; the crypto panel is
    tz-aware UTC. A portfolio cannot be assembled across two calendars, so
    every stream is normalised to tz-naive midnight UTC here, once, in the
    only place that is allowed to do it.
    """
    out = pd.DatetimeIndex(idx)
    if out.tz is not None:
        out = out.tz_convert("UTC").tz_localize(None)
    return out.normalize()


# --------------------------------------------------------------------------- #
# Builders - one per declared `build` kind, each delegating to its R43 owner
# --------------------------------------------------------------------------- #
def _book_rv(kind: str, signal: str) -> dict:
    structures = RV.build_structures(kind)
    if not structures:
        return None
    return RV.book_streams(structures, signal, C.STREAM_EXPRESSION)


def _book_carry(group: str, mode: str) -> dict:
    markets = KARRY.group_markets(group)
    if not markets:
        return None
    panel = KARRY.build_panel(markets)
    if panel is None:
        return None
    bk = KARRY.book(panel, C.STREAM_EXPRESSION, mode=mode)
    if bk is not None:
        bk["_panel"] = panel
    return bk


def _book_relations() -> dict:
    return XA.relation_book(XA.RELATIONS, C.STREAM_EXPRESSION)


def _book_event(rule: str, horizon: int) -> dict:
    cal = AQ.load_release_calendar()
    if cal is None or getattr(cal, "empty", True):
        return None
    structures = RV.build_structures("RATES")
    if not structures:
        return None
    return XA.event_book(structures, cal, rule, horizon, event_days=True)


def _book_equity(signal: str) -> dict:
    panel = EQ.build_panel()
    if panel is None:
        return None
    resid = EQ.residuals(panel)
    return EQ.book(panel, resid, signal, C.STREAM_EXPRESSION)


def _book_vx_premium() -> dict:
    """The variance risk premium as the OWNED VX curve expresses it.

    An unconditional short-front / long-second calendar spread. In contango
    the front contract rolls down toward spot and the short leg earns; the
    second contract hedges most of the vol beta. There is no signal here on
    purpose - this is a PREMIUM sleeve and belongs to the control.
    """
    from ..r43 import panels as P
    d = P.futures_daily("VX")
    if d is None or "ret1" not in d.columns or "ret2" not in d.columns:
        return None
    r1 = pd.to_numeric(d["ret1"], errors="coerce")
    r2 = pd.to_numeric(d["ret2"], errors="coerce")
    ok = r1.notna() & r2.notna()
    idx = pd.DatetimeIndex(d.index[ok])
    if len(idx) < 250:
        return None
    gross = (-r1[ok] + r2[ok]).rename("gross")
    gross.index = idx
    turn = pd.Series(0.0, index=idx)
    turn.iloc[0] = 2.0                      # one round trip across two legs
    groups = ["VIX_FUTURES_TERM_STRUCTURE", "VIX_FUTURES_TERM_STRUCTURE"]
    cost = J.cost_stream(turn, groups, [1.0, 1.0])
    capt = J.futures_committed_capital(groups, [1.0, 1.0])
    return {"gross": gross, "cost": cost, "turnover": turn,
            "committed_capital": capt["committed_capital"], "index": idx,
            "n_markets": 1, "markets": ["VX"]}


def _book_crypto(symbol: str) -> dict:
    """The UNCONDITIONAL cash-and-carry - the premium, not R41's timing rule.

    R42 established that an always-on book beats the z-gated one under R41's
    own scoring, so the honest premium sleeve is the unconditional one. It is
    priced on UNREMUNERATED collateral, which is why R42 found it below cash.
    """
    from ..r42 import pnl_audit as PA
    df = PA.r41_panel(symbol)
    if df is None or df.empty:
        return None
    idx = pd.DatetimeIndex(df.index)
    # Always long the carry: long spot / short perp, every day.
    held = pd.Series(1.0, index=idx)
    gross = (held * (df["funding"].fillna(0.0)
                     + df["basis_ret"].fillna(0.0))).rename("gross")
    # One round trip at entry only; an always-on book does not churn.
    turn = pd.Series(0.0, index=idx)
    turn.iloc[0] = 1.0
    cost = (turn * (C.CRYPTO_ROUND_TRIP_BPS / 1e4)).rename("cost")
    K = float(C.CRYPTO_COMMITTED_CAPITAL)
    return {"gross": gross, "cost": cost, "turnover": turn,
            "committed_capital": K, "index": idx,
            "n_markets": 1, "markets": [symbol]}


_BUILDERS = {
    "rv": lambda p: _book_rv(p["kind"], p["signal"]),
    "carry": lambda p: _book_carry(p["group"], p["mode"]),
    "relations": lambda p: _book_relations(),
    "event": lambda p: _book_event(p["rule"], p["horizon"]),
    "equity": lambda p: _book_equity(p["signal"]),
    "crypto": lambda p: _book_crypto(p["symbol"]),
    "vx": lambda p: _book_vx_premium(),
}


# --------------------------------------------------------------------------- #
# The inventory
# --------------------------------------------------------------------------- #
def _cache_path() -> Path:
    return data_dir("streams") / CACHE_NAME


def build_all(*, refresh: bool = False) -> dict:
    """Build EVERY declared stream. Losers included, by contract."""
    global _MEM
    if _MEM and not refresh:
        return _MEM
    p = _cache_path()
    if p.exists() and not refresh:
        try:
            with open(p, "rb") as fh:
                _MEM = pickle.load(fh)
            return _MEM
        except Exception:                                 # pragma: no cover
            pass

    out = {}
    for spec in C.STREAMS:
        sid = spec["id"]
        kind, params = spec["build"]
        rec = {"id": sid, "role": spec["role"], "family": spec["family"],
               "asset_class": spec["asset_class"],
               "expression": spec["expression"], "why": spec["why"],
               "owner": spec["owner"],
               "collateral_class": COLLATERAL_BY_EXPRESSION[
                   spec["expression"]],
               "build": {"kind": kind, "params": dict(params)}}
        try:
            bk = _BUILDERS[kind](params)
        except Exception as exc:
            rec["state"] = "IRREPARABLE_TECHNICAL_FAILURE"
            rec["error"] = "%s: %s" % (type(exc).__name__, exc)
            out[sid] = rec
            continue
        if bk is None:
            rec["state"] = "HISTORICAL_DATA_UNAVAILABLE"
            out[sid] = rec
            continue
        raw_idx = pd.DatetimeIndex(bk["index"])
        idx = _naive_days(raw_idx)

        def _align(s, fill):
            v = pd.Series(s).reindex(raw_idx)
            v.index = idx
            v = v[~v.index.duplicated(keep="last")]
            return (v.fillna(fill) if fill is not None else v).astype(float)

        idx = pd.DatetimeIndex(pd.Series(idx).drop_duplicates(keep="last"))
        rec.update({
            "state": "BUILT",
            "gross": _align(bk["gross"], None),
            "cost": _align(bk["cost"], 0.0),
            "turnover": _align(
                bk.get("turnover", pd.Series(0.0, index=raw_idx)), 0.0),
            "committed_capital": float(bk["committed_capital"]),
            "index": idx,
            "n_obs": int(len(idx)),
            "first": str(idx[0])[:10] if len(idx) else None,
            "last": str(idx[-1])[:10] if len(idx) else None,
            "n_markets": bk.get("n_markets") or bk.get("n_structures")
            or bk.get("n_relations"),
            "markets": bk.get("markets"),
        })
        out[sid] = rec

    _MEM = out
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "wb") as fh:
        pickle.dump(out, fh, protocol=4)
    return out


def add_conditional(sid: str, rec: dict) -> dict:
    """Register an Engine-1/Engine-3 stream declared in CONDITIONAL_STREAMS.

    Adding a stream is a contractual event: the id must appear in the frozen
    contract, or the call is refused.
    """
    allowed = {s["id"] for s in C.CONDITIONAL_STREAMS}
    if sid not in allowed:
        raise ValueError(
            "%r is not a declared conditional stream; the frozen contract "
            "allows only %r" % (sid, sorted(allowed)))
    inv = build_all()
    inv[sid] = rec
    with open(_cache_path(), "wb") as fh:
        pickle.dump(inv, fh, protocol=4)
    return inv


# --------------------------------------------------------------------------- #
# Excess streams, zones and correlation
# --------------------------------------------------------------------------- #
def excess_series(rec: dict, *, cost_multiplier: float = 1.0) -> pd.Series:
    """The daily EXCESS return on THIS stream's own committed capital."""
    if rec.get("state") != "BUILT":
        return None
    book = J.implementable_book(
        rec["gross"], pd.Series(1.0, index=rec["index"]),
        committed_capital=rec["committed_capital"],
        collateral_class=rec["collateral_class"],
        cost=rec["cost"] * float(cost_multiplier),
        day_count=J.TRADING_DAYS)
    return book["excess"].rename(rec["id"])


def excess_frame(inv: dict = None, *, cost_multiplier: float = 1.0,
                 ids=None) -> pd.DataFrame:
    inv = inv or build_all()
    keys = list(ids) if ids is not None else list(inv)
    cols = {}
    for sid in keys:
        rec = inv.get(sid)
        if rec is None:
            continue
        s = excess_series(rec, cost_multiplier=cost_multiplier)
        if s is not None and s.notna().any():
            cols[sid] = s
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(cols).sort_index()


def excess_frame_signed(inv: dict, signs: dict, *,
                        cost_multiplier: float = 1.0, ids=None
                        ) -> pd.DataFrame:
    """Excess streams with the SIGN applied to gross, never to cost.

    This exists because of a bug this release caught in its own work. The
    first sign-selected diagnostic multiplied each stream's EXCESS series by
    its sign, which flips ``(gross - cost)`` into ``(-gross + cost)`` - and
    turns a transaction-cost drag into a transaction-cost CREDIT. It printed
    a lockbox Sharpe of 1.40 built almost entirely out of costs that would
    have been paid either way. A short position pays the spread exactly like
    a long one, so the sign multiplies gross and the cost is re-charged.
    """
    inv = inv or build_all()
    cols = {}
    for sid in (list(ids) if ids is not None else list(inv)):
        rec = inv.get(sid)
        if rec is None or rec.get("state") != "BUILT":
            continue
        s = float(signs.get(sid, 1.0))
        book = J.implementable_book(
            rec["gross"] * s, pd.Series(1.0, index=rec["index"]),
            committed_capital=rec["committed_capital"],
            collateral_class=rec["collateral_class"],
            cost=rec["cost"] * float(cost_multiplier),
            day_count=J.TRADING_DAYS)
        cols[sid] = book["excess"].rename(sid)
    if not cols:
        return pd.DataFrame()
    return pd.DataFrame(cols).sort_index()


def common_index(frame: pd.DataFrame, *, min_streams: int = 2) -> pd.DatetimeIndex:
    """Dates on which at least ``min_streams`` streams are observable.

    A portfolio cannot be judged on a date where only one sleeve exists; on
    such a date the "portfolio" IS that sleeve.
    """
    if frame.empty:
        return pd.DatetimeIndex([])
    live = frame.notna().sum(axis=1)
    return pd.DatetimeIndex(frame.index[live >= int(min_streams)])


def zones(frame: pd.DataFrame, *, min_streams: int = 2) -> dict:
    idx = common_index(frame, min_streams=min_streams)
    return EV.zone_split(idx, embargo=C.ZONE_EMBARGO_SESSIONS)


def correlations(frame: pd.DataFrame, dates=None) -> pd.DataFrame:
    d = frame if dates is None else frame.reindex(pd.DatetimeIndex(dates))
    return d.corr(min_periods=250)


def independence_report(frame: pd.DataFrame, dates=None) -> dict:
    """Which declared streams are, in fact, independent enough to be counted
    as separate bets."""
    corr = correlations(frame, dates)
    pairs = []
    cols = list(corr.columns)
    for i, a in enumerate(cols):
        for b in cols[i + 1:]:
            r = corr.loc[a, b]
            if pd.notna(r):
                pairs.append({"a": a, "b": b, "corr": float(r)})
    dup = [p for p in pairs
           if abs(p["corr"]) > C.MAX_CORRELATION_FOR_INDEPENDENCE]
    vals = [abs(p["corr"]) for p in pairs]
    return {
        "calculation_owner": CALCULATION_OWNER,
        "n_streams": len(cols),
        "n_pairs": len(pairs),
        "mean_abs_correlation": float(np.mean(vals)) if vals else None,
        "max_abs_correlation": float(np.max(vals)) if vals else None,
        "threshold": C.MAX_CORRELATION_FOR_INDEPENDENCE,
        "pairs_above_threshold": sorted(
            dup, key=lambda p: -abs(p["corr"]))[:20],
        "n_pairs_above_threshold": len(dup),
        "duplicate_rule": C.DUPLICATE_RULE,
    }


def inventory_report(inv: dict = None) -> dict:
    """The stream inventory as an artifact payload - built, blocked and why."""
    inv = inv or build_all()
    frame = excess_frame(inv)
    z = zones(frame)
    rows = []
    for sid, rec in inv.items():
        row = {k: rec.get(k) for k in
               ("id", "role", "family", "asset_class", "expression", "state",
                "owner", "why", "collateral_class", "n_obs", "first", "last",
                "n_markets", "error")}
        row["committed_capital"] = rec.get("committed_capital")
        if rec.get("state") == "BUILT" and sid in frame.columns:
            fit = frame[sid].reindex(
                pd.DatetimeIndex(z["A"]).union(pd.DatetimeIndex(z["B"])))
            fit = fit.dropna()
            if len(fit) > 24:
                mu = float(np.nanmean(fit) * J.TRADING_DAYS)
                sd = float(np.nanstd(fit, ddof=1) * np.sqrt(J.TRADING_DAYS))
                hac = EV.hac_t(fit.to_numpy(dtype=float), lags=21)
                row["fit_excess_ann"] = mu
                row["fit_vol_ann"] = sd
                row["fit_sharpe"] = (mu / sd) if sd else None
                row["fit_t_hac"] = hac.get("t")
                row["fit_n"] = int(len(fit))
                row["turnover_ann"] = float(
                    np.nanmean(rec["turnover"].reindex(fit.index))
                    * J.TRADING_DAYS)
                # The decomposition that decides WHY a stream is weak: a
                # small positive gross eaten by cost is a different finding
                # from a signal that is simply pointing the wrong way.
                K = rec["committed_capital"]
                g = float(np.nanmean(
                    rec["gross"].reindex(fit.index)) / K * J.TRADING_DAYS)
                c = float(np.nanmean(
                    rec["cost"].reindex(fit.index)) / K * J.TRADING_DAYS)
                row["fit_gross_ann_on_capital"] = g
                row["fit_cost_ann_on_capital"] = c
                row["cost_share_of_gross"] = (abs(c / g) if g else None)
                row["diagnosis"] = (
                    "GROSS_NEGATIVE_SIGNAL_POINTS_THE_WRONG_WAY" if g <= 0
                    else ("COST_DOMINATED" if mu <= 0 else "NET_POSITIVE"))
        rows.append(row)
    built = [r for r in rows if r["state"] == "BUILT"]
    return {
        "calculation_owner": CALCULATION_OWNER,
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "n_declared": len(C.STREAMS),
        "n_built": len(built),
        "n_residual_built": len([r for r in built if r["role"] == "RESIDUAL"]),
        "n_premium_built": len([r for r in built if r["role"] == "PREMIUM"]),
        "losers_are_included": C.LOSERS_ARE_INCLUDED,
        "no_threshold_is_chosen": C.NO_THRESHOLD_IS_CHOSEN,
        "expression": C.STREAM_EXPRESSION,
        "streams": sorted(rows, key=lambda r: r["id"]),
        "zones": {"n": z.get("n"), "a_range": z.get("a_range"),
                  "b_range": z.get("b_range"), "c_range": z.get("c_range"),
                  "embargo": z.get("embargo")},
        "independence": independence_report(frame, list(
            pd.DatetimeIndex(z["A"]).union(pd.DatetimeIndex(z["B"])))),
    }
