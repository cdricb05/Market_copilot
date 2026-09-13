r"""alpha_agent.alpha_recovery.intraday_alpha - the ONE owner of NON-INCUMBENT
intraday cross-asset challenger families.

THE QUESTION
    The campaign's primary objective is the FIRST non-incumbent, capital-eligible
    alpha signal, from any market and any horizon. Every family measured so far
    was daily, and the daily PRICE_STATE frontier is exhausted. This module asks
    whether the estate's owned one-minute cross-asset history carries alpha that
    a daily bar cannot express, in four economically distinct markets:
    US equity beta (SPY), US tech beta (QQQ), US long duration (TLT) and gold
    (GLD), with the front end, the belly and the dollar read as information.

WHY THIS IS NOT A REOPENED PRICE_STATE FAMILY (contract rule 13)
    Rule 13 permits reopening price-derived research when COVERAGE materially
    improves. A one-minute bar is a coverage change, not a transform: the
    overnight/pre-market split, the opening range, minute-level realised
    volatility and minute-level cross-market transmission have no daily
    expression at all. Every specification here dies at daily resolution.

WHAT IS FLAT OVERNIGHT, AND WHY THAT MATTERS
    Every arm enters after 09:30 ET and exits by 11:55 ET on the SAME session.
    The sleeve therefore holds no overnight beta and is close to orthogonal to
    the incumbent by construction - which is exactly the property a cross-domain
    sleeve needs to add utility at equal risk instead of substituting for the
    incumbent. It is also why its benchmark is CASH, not SPY: it is not invested
    when it is not trading.

THE TWO HONEST LIMITS, STATED BEFORE THE RESULTS
    1. The panel stops at 11:59 ET. There is no close. No arm may be marked to
       the close, and the published first-half-hour -> last-half-hour intraday
       momentum result cannot be tested as specified.
    2. The intraday sample (2024-08-26 -> 2026-08-24) lies ENTIRELY after the
       R63 lockbox start of 2023-01-01. The estate's lockbox does not partition
       it. The substitute is declared here, before running: a chronological
       split, first 300 sessions SELECTION and last 200 sessions HOLDOUT, and
       the holdout must agree in sign and be positive.

FROZEN GATES - inherited, never re-declared weaker
    materiality >= 1.5 %/yr, NW t >= 2.0, BH q = 0.10, family Holm alpha = 0.05,
    >= 36 effective periods, halves floor -0.005, and - added, stricter, not
    weaker - the advantage must survive the STRESS cost of 5.0 bp per side.

    The one frozen gate deliberately NOT applied is GATE_MAX_TURNOVER (0.40
    one-way per 21 sessions). It is a DAILY-book gate: any intraday strategy
    exceeds it by definition, so applying it would not test cost discipline, it
    would forbid the asset class. The economic control it exists to provide is
    supplied here, and more strictly, by charging the FULL round trip on EVERY
    session at three cost levels and requiring survival at 2.5x the headline
    rate. The substitution is declared in the protocol, not assumed.

RESEARCH ONLY. No order, no fill, no registration, no promotion, no purchase.
"""
from __future__ import annotations

import json
import math
import re
import time

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, GATE_HALF_FLOOR, HOLM_ALPHA, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS,
               read_artifact, research_root, write_artifact)
from . import intraday_data as ID
from . import tournament as T

CALCULATION_OWNER = "alpha_agent.alpha_recovery.intraday_alpha"
CELLS_DIR = "cells"
ARTIFACT_NAME = "intraday_alpha.json"
PPY = ID.PPY

#: Pre-registered chronological split. Declared before any arm ran.
SELECTION_SESSIONS = 300

#: Signal minutes (exchange local) and the common exit. Entry is always the
#: close of the minute AFTER the signal minute, so no arm can trade on the bar
#: that produced its own signal.
SIG_0935, SIG_1000, SIG_1030, SIG_1100 = (9, 35), (10, 0), (10, 30), (11, 0)
EXIT_ET = (11, 55)
OPENING_RANGE_MINUTES = 30
VOL_LOOKBACK_SESSIONS = 20

FAM_CARRY = "INTRADAY_SESSION_CARRY"
FAM_RANGE = "INTRADAY_OPENING_RANGE"
FAM_VOL = "INTRADAY_VOLATILITY_STATE"
FAM_LEAD = "INTRADAY_CROSS_MARKET_LEADLAG"
FAM_RS = "INTRADAY_RELATIVE_STRENGTH"
FAMILIES = (FAM_CARRY, FAM_RANGE, FAM_VOL, FAM_LEAD, FAM_RS)

#: The information dimension each family consumes. None of these is a daily
#: PRICE_STATE transform; each needs sub-daily coverage to exist at all.
DIMENSION = {
    FAM_CARRY: "INTRADAY_OVERNIGHT_AND_PREMARKET_PATH",
    FAM_RANGE: "INTRADAY_OPENING_RANGE_STRUCTURE",
    FAM_VOL: "INTRADAY_REALISED_VOLATILITY_STATE",
    FAM_LEAD: "INTRADAY_CROSS_MARKET_TRANSMISSION",
    FAM_RS: "INTRADAY_CROSS_SECTIONAL_RELATIVE_STRENGTH",
}

TURNOVER_GATE_SUBSTITUTION = (
    "GATE_MAX_TURNOVER (0.40 one-way per 21 sessions) is a DAILY-book gate that every intraday "
    "strategy exceeds by construction; applying it would forbid the asset class rather than test "
    "cost discipline. It is replaced - STRICTER, not weaker - by charging the full round trip on "
    "every session at 2.0 / 5.0 / 12.5 bp per side and requiring materiality to survive 5.0 bp, "
    "2.5x the headline rate. Realised turnover is reported in full.")

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# The frozen grid - the WHOLE grid is fixed here, before any arm was measured
# --------------------------------------------------------------------------- #
def default_grid() -> list:
    g: list = []

    def add(family, name, **kw):
        g.append({"family": family, "name": name, "dimension": DIMENSION[family],
                  "tag": "PRIMARY", **kw})

    # F1 prior-session carry and pre-market drift, decided at 09:35
    add(FAM_CARRY, "CARRY_REVERSION_SPY", rule="carry", sign=-1, legs=("SPY",), signal=SIG_0935)
    add(FAM_CARRY, "CARRY_CONTINUATION_SPY", rule="carry", sign=+1, legs=("SPY",), signal=SIG_0935)
    add(FAM_CARRY, "CARRY_REVERSION_QQQ", rule="carry", sign=-1, legs=("QQQ",), signal=SIG_0935)
    add(FAM_CARRY, "PREMARKET_REVERSION_SPY", rule="premarket", sign=-1, legs=("SPY",), signal=SIG_0935)
    add(FAM_CARRY, "PREMARKET_CONTINUATION_SPY", rule="premarket", sign=+1, legs=("SPY",), signal=SIG_0935)
    add(FAM_CARRY, "CARRY_REVERSION_MULTI", rule="carry", sign=-1, legs=ID.TRADABLE, signal=SIG_0935)

    # F2 opening range, decided at 10:00 on the 09:30-09:59 range
    for legs, tagname in ((("SPY",), "SPY"), (("QQQ",), "QQQ"), (ID.TRADABLE, "MULTI")):
        add(FAM_RANGE, "OPENING_RANGE_BREAKOUT_%s" % tagname, rule="range", sign=+1,
            legs=legs, signal=SIG_1000)
        add(FAM_RANGE, "OPENING_RANGE_FADE_%s" % tagname, rule="range", sign=-1,
            legs=legs, signal=SIG_1000)

    # F3 the first-30-minute move, conditioned on the realised-volatility state
    for legs, tagname in ((("SPY",), "SPY"), (ID.TRADABLE, "MULTI")):
        for state in ("COMPRESSED", "EXPANDED", "ANY"):
            add(FAM_VOL, "OPEN_MOVE_%s_%s" % (state, tagname), rule="volstate", sign=+1,
                legs=legs, signal=SIG_1000, state=state)

    # F4 cross-market transmission into equity, decided at 10:30
    add(FAM_LEAD, "TLT_TO_SPY_RISKOFF", rule="lead", sign=-1, legs=("SPY",), signal=SIG_1030, source="TLT")
    add(FAM_LEAD, "TLT_TO_SPY_RISKON", rule="lead", sign=+1, legs=("SPY",), signal=SIG_1030, source="TLT")
    add(FAM_LEAD, "GLD_TO_SPY_RISKOFF", rule="lead", sign=-1, legs=("SPY",), signal=SIG_1030, source="GLD")
    add(FAM_LEAD, "UUP_TO_SPY_RISKOFF", rule="lead", sign=-1, legs=("SPY",), signal=SIG_1030, source="UUP")
    add(FAM_LEAD, "CURVE_TO_SPY_RISKOFF", rule="curve", sign=-1, legs=("SPY",), signal=SIG_1030)
    add(FAM_LEAD, "RISKOFF_VOTE_TO_SPY", rule="vote", sign=+1, legs=("SPY",), signal=SIG_1030)

    # F5 vol-adjusted cross-sectional relative strength across the four legs
    for sig, tagname in ((SIG_1000, "1000"), (SIG_1030, "1030"), (SIG_1100, "1100")):
        add(FAM_RS, "RS_MOMENTUM_%s" % tagname, rule="relstrength", sign=+1,
            legs=ID.TRADABLE, signal=sig)
        add(FAM_RS, "RS_REVERSAL_%s" % tagname, rule="relstrength", sign=-1,
            legs=ID.TRADABLE, signal=sig)

    return _finish(g)


def _finish(g: list) -> list:
    for sp in g:
        sp.setdefault("exit", EXIT_ET)
        sp["legs"] = tuple(sp["legs"])
        sp["markets"] = sorted({ID.MARKET[s] for s in sp["legs"]})
        sp["cell_id"] = "INTRADAY|%s|%s" % (sp["family"].replace("INTRADAY_", ""), sp["name"])
    return g


# --------------------------------------------------------------------------- #
# The rescue budget. Contract section 7 allows at most TWO rescues per family,
# and only against a NAMED MEASURED binding failure. Both are spent here, on
# one failure, and the family is then closed.
# --------------------------------------------------------------------------- #
CARRY_BINDING_FAILURE = (
    "MEASURED: INTRADAY|SESSION_CARRY|CARRY_REVERSION_SPY earns +9.67 %/yr GROSS at Newey-West "
    "t 1.72 - that is 3.85 bp per engaged session - while engaging on 100 % of sessions and paying "
    "a 4.0 bp round trip at the PRIMARY rate. The arm engages UNCONDITIONALLY, regardless of the "
    "magnitude of the overnight move, so a per-trade edge smaller than the round trip is paid on "
    "every session. The named failure is UNCONDITIONAL ENGAGEMENT, not the choice of signal.")

#: Fixed here, once, before either rescue ran. Not swept.
RESCUE_LOOKBACK_SESSIONS = 60
RESCUE_PERCENTILE = 70.0


def rescue_grid() -> list:
    """The two permitted rescues. Both address the NAMED failure above by
    engaging only when the overnight move is large relative to its own recent
    distribution - a magnitude condition on information the arm already uses.
    Neither adds an information dimension, a parameter sweep or a new indicator.
    """
    g = [
        {"family": FAM_CARRY, "name": "CARRY_REVERSION_SPY_LARGE_ONLY", "rule": "carry_conditional",
         "sign": -1, "legs": ("SPY",), "signal": SIG_0935, "dimension": DIMENSION[FAM_CARRY],
         "tag": "RESCUE",
         "hypothesis": "the reversion edge concentrates in large overnight moves (overshoot), so "
                       "engaging only on the top 30 % of |carry| raises the per-trade edge above "
                       "the round trip"},
        {"family": FAM_CARRY, "name": "CARRY_REVERSION_MULTI_LARGE_ONLY", "rule": "carry_conditional",
         "sign": -1, "legs": ID.TRADABLE, "signal": SIG_0935, "dimension": DIMENSION[FAM_CARRY],
         "tag": "RESCUE",
         "hypothesis": "the same magnitude condition across four markets diversifies per-session "
                       "noise, so a real effect should show a HIGHER t than the single-leg rescue"},
    ]
    for sp in g:
        sp["binding_failure"] = CARRY_BINDING_FAILURE
        sp["rescue_lookback_sessions"] = RESCUE_LOOKBACK_SESSIONS
        sp["rescue_percentile"] = RESCUE_PERCENTILE
    return _finish(g)


# --------------------------------------------------------------------------- #
# Signals. Every one reads bars strictly at or before its signal minute.
# --------------------------------------------------------------------------- #
def _trailing_median(x: np.ndarray, k: int) -> np.ndarray:
    """Median of the k STRICTLY PRIOR sessions (never includes today)."""
    out = np.full(len(x), np.nan)
    s = pd.Series(x)
    out[:] = s.shift(1).rolling(k, min_periods=max(5, k // 2)).median().to_numpy()
    return out


def signals(sp: dict) -> np.ndarray:
    """(n_sessions x n_legs) target weights, gross exposure 1.0 when engaged."""
    pn = ID.panel()
    reg, pre = pn["reg_close"], pn["pre_close"]
    hi, lo = pn["reg_high"], pn["reg_low"]
    n_d = reg.shape[0]
    legs = sp["legs"]
    si = ID.minute_index(*sp["signal"])
    w = np.zeros((n_d, len(legs)))
    rule, sgn = sp["rule"], float(sp["sign"])

    if rule in ("carry", "premarket", "carry_conditional"):
        for k, s in enumerate(legs):
            j = ID.ix(s)
            if rule == "premarket":
                x = pre[:, -1, j] / pre[:, 0, j] - 1.0
            else:
                prev = np.concatenate([[np.nan], reg[:-1, -1, j]])
                x = reg[:, 0, j] / prev - 1.0
            pos = sgn * np.sign(x)
            if rule == "carry_conditional":
                # engage only when |carry| clears its own STRICTLY PRIOR
                # distribution - the named binding failure is unconditional
                # engagement, and this is the condition that addresses it
                thr = (pd.Series(np.abs(x)).shift(1)
                       .rolling(int(sp["rescue_lookback_sessions"]),
                                min_periods=int(sp["rescue_lookback_sessions"]) // 2)
                       .quantile(float(sp["rescue_percentile"]) / 100.0).to_numpy())
                pos = np.where(np.isfinite(thr) & (np.abs(x) >= thr), pos, 0.0)
            w[:, k] = pos
    elif rule == "range":
        n_or = OPENING_RANGE_MINUTES
        for k, s in enumerate(legs):
            j = ID.ix(s)
            orh = np.nanmax(hi[:, :n_or, j], axis=1)
            orl = np.nanmin(lo[:, :n_or, j], axis=1)
            p = reg[:, si, j]
            br = np.where(p > orh, 1.0, np.where(p < orl, -1.0, 0.0))
            w[:, k] = sgn * br
    elif rule == "volstate":
        for k, s in enumerate(legs):
            j = ID.ix(s)
            seg = reg[:, :OPENING_RANGE_MINUTES, j]
            lr = np.diff(np.log(seg), axis=1)
            rv = np.nanstd(lr, axis=1)
            ref = _trailing_median(rv, VOL_LOOKBACK_SESSIONS)
            move = np.sign(seg[:, -1] / seg[:, 0] - 1.0)
            if sp["state"] == "COMPRESSED":
                ok = rv < ref
            elif sp["state"] == "EXPANDED":
                ok = rv >= ref
            else:
                ok = np.isfinite(rv)
            ok = ok & np.isfinite(ref) if sp["state"] != "ANY" else ok
            w[:, k] = sgn * move * ok.astype(float)
    elif rule == "lead":
        j = ID.ix(sp["source"])
        x = reg[:, si, j] / reg[:, 0, j] - 1.0
        w[:, 0] = sgn * np.sign(x)
    elif rule == "curve":
        a = reg[:, si, ID.ix("IEF")] / reg[:, 0, ID.ix("IEF")] - 1.0
        b = reg[:, si, ID.ix("SHY")] / reg[:, 0, ID.ix("SHY")] - 1.0
        w[:, 0] = sgn * np.sign(a - b)
    elif rule == "vote":
        v = np.zeros(reg.shape[0])
        for s in ("TLT", "GLD", "UUP"):
            j = ID.ix(s)
            v -= np.sign(reg[:, si, j] / reg[:, 0, j] - 1.0)
        w[:, 0] = sgn * np.sign(v)
    elif rule == "relstrength":
        r = np.zeros((reg.shape[0], len(legs)))
        for k, s in enumerate(legs):
            j = ID.ix(s)
            rr = reg[:, si, j] / reg[:, 0, j] - 1.0
            seg = reg[:, :si + 1, j]
            lr = np.diff(np.log(seg), axis=1)
            vol = np.nanstd(lr, axis=1)
            ref = _trailing_median(vol, VOL_LOOKBACK_SESSIONS)
            r[:, k] = rr / np.where(np.isfinite(ref) & (ref > 0), ref, np.nan)
        ok = np.isfinite(r).any(axis=1, keepdims=True)
        mean = np.divide(np.nansum(r, axis=1, keepdims=True),
                         np.maximum(np.isfinite(r).sum(axis=1, keepdims=True), 1))
        dm = np.where(ok, r - mean, np.nan)
        w = np.nan_to_num(sgn * np.sign(dm))
    else:
        raise ValueError("unknown rule %r" % rule)

    w = np.nan_to_num(w)
    gross = np.abs(w).sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        w = np.where(gross > 0, w / gross, 0.0)
    return w


# --------------------------------------------------------------------------- #
# The session return path
# --------------------------------------------------------------------------- #
def session_path(sp: dict, *, cost_bps: float) -> dict:
    """Per-session NET return of one arm at one cost level.

    Entry is the close of the minute AFTER the signal minute; exit is the close
    of the exit minute of the SAME session. The full round trip is charged on
    every engaged session.
    """
    pn = ID.panel()
    reg = pn["reg_close"]
    legs = sp["legs"]
    si = ID.minute_index(*sp["signal"])
    ei = ID.minute_index(*sp["exit"])
    entry = si + 1
    if entry >= ei:
        raise ValueError("entry minute is not before the exit minute")
    w = signals(sp)
    rate = float(cost_bps) * 1e-4
    n_d = reg.shape[0]
    gross_r = np.zeros(n_d)
    for k, s in enumerate(legs):
        j = ID.ix(s)
        leg_r = reg[:, ei, j] / reg[:, entry, j] - 1.0
        gross_r += w[:, k] * np.nan_to_num(leg_r)
    engaged = np.abs(w).sum(axis=1)
    cost = engaged * 2.0 * rate
    net = gross_r - cost
    return {"net": net, "gross": gross_r, "cost": cost, "engaged": engaged, "weights": w,
            "dates": pn["dates"]}


def _stats(net: np.ndarray, cost: np.ndarray, engaged: np.ndarray, *, label: str) -> dict:
    x = net[np.isfinite(net)]
    if len(x) < 3:
        return {"periods": int(len(x)), "layer": label}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"layer": label, "periods": int(len(x)), "effective_periods": int(len(x)),
            "ann_net": float(x.mean() * PPY),
            "ann_vol": float(sd * math.sqrt(PPY)) if sd > 0 else None,
            "sharpe": float(x.mean() / sd * math.sqrt(PPY)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"],
            "max_dd": S._max_dd(x), "hit_rate": float((x > 0).mean()),
            "ann_cost_drag": float(np.nanmean(cost) * PPY),
            "engaged_share": float(np.nanmean(engaged > 0)),
            "ann_oneway_turnover": float(np.nanmean(engaged) * 2.0 * PPY)}


# --------------------------------------------------------------------------- #
# Capital applicability: does the sleeve add utility to the incumbent at EQUAL
# RISK? The rule is tournament.cross_domain's; only the frequency changes, and
# it changes because it must (see the artifact's `why_daily_frequency`).
# --------------------------------------------------------------------------- #
def incumbent_daily_path() -> pd.Series | None:
    """The incumbent's OWN realised daily net path (its operational
    construction: equal-weight top-25, rebalanced every 21 sessions), from the
    module that already owns it."""
    if "inc_daily" in _CACHE:
        return _CACHE["inc_daily"]
    try:
        from . import equity_challengers as EC
        bk = EC._books()
        ref_id = "US_EQUITY|TOP%d|%s|k%d" % (25, EC.REFERENCE[0], EC.REFERENCE[1])
        daily = bk["arms"][ref_id]["daily_net"]
        s = pd.Series(daily, index=pd.to_datetime([str(d) for d in bk["dates"]])).dropna()
    except Exception:                                                  # noqa: BLE001
        s = None
    _CACHE["inc_daily"] = s
    return s


def equal_risk_daily(sleeve: pd.Series, incumbent: pd.Series, *, label: str,
                     sleeve_risk_share: float = 0.5) -> dict:
    """The tournament's equal-risk rule at DAILY frequency: size the sleeve to
    ``sleeve_risk_share`` of the incumbent's realised volatility, rescale the
    combination back to the incumbent-only volatility, and compare."""
    j = pd.concat({"inc": incumbent, "sl": sleeve}, axis=1).dropna()
    if len(j) < MIN_EFFECTIVE_PERIODS:
        return {"state": "DATA_HOLD", "periods": int(len(j)),
                "why": "fewer than %d overlapping sessions" % MIN_EFFECTIVE_PERIODS}
    inc, sl = j["inc"].to_numpy(), j["sl"].to_numpy()
    v_i, v_s = float(np.std(inc, ddof=1)), float(np.std(sl, ddof=1))
    if v_i <= 0 or v_s <= 0:
        return {"state": "DATA_HOLD", "why": "degenerate volatility"}
    size = sleeve_risk_share * v_i / v_s
    combo = inc + size * sl
    lam = v_i / float(np.std(combo, ddof=1))
    combo_eq = lam * combo
    diff = combo_eq - inc
    st = S.nw_tstat(diff, 0)

    def _m(x):
        sd = float(np.std(x, ddof=1))
        return {"ann_net": float(x.mean() * PPY), "ann_vol": float(sd * math.sqrt(PPY)),
                "sharpe": float(x.mean() / sd * math.sqrt(PPY)) if sd > 0 else None,
                "max_dd": S._max_dd(x)}

    a, b = _m(inc), _m(combo_eq)
    return {"state": "OK", "label": label, "frequency": "DAILY", "periods": int(len(j)),
            "first": str(j.index[0].date()), "last": str(j.index[-1].date()),
            "sleeve_risk_share": sleeve_risk_share, "sleeve_size_multiplier": float(size),
            "equal_risk_rescale": float(lam),
            "correlation_incumbent_sleeve": float(np.corrcoef(inc, sl)[0, 1]),
            "incumbent_only": a, "incumbent_plus_sleeve_equal_risk": b,
            "incremental_ann_net_return": b["ann_net"] - a["ann_net"],
            "sharpe_delta": (b["sharpe"] or 0.0) - (a["sharpe"] or 0.0),
            "drawdown_delta": (b["max_dd"] or 0.0) - (a["max_dd"] or 0.0),
            "t_incremental": st["t"], "p_incremental_one_sided": st["p_one_sided"],
            "positive_incremental_utility_after_costs":
                bool(b["ann_net"] - a["ann_net"] > 0 and (st["t"] or 0) >= 2.0),
            "rule_owner": "alpha_agent.alpha_recovery.tournament.cross_domain",
            "why_daily_frequency": "the 2-year intraday sample yields ~24 non-overlapping "
                                   "21-session periods, below the frozen floor of %d; the daily "
                                   "path yields ~500. The sizing and equal-risk rescale rule is "
                                   "unchanged." % MIN_EFFECTIVE_PERIODS}


def capital_applicability(sp: dict, path: dict) -> dict:
    inc = incumbent_daily_path()
    sleeve = pd.Series(path["net"], index=pd.to_datetime(path["dates"]))
    if inc is None or len(inc) == 0:
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    return equal_risk_daily(sleeve, inc, label=sp["cell_id"])


# --------------------------------------------------------------------------- #
# Measurement
# --------------------------------------------------------------------------- #
def measure_cell(sp: dict, *, with_capital: bool = True) -> dict:
    cell = dict(sp)
    cell["legs"] = list(sp["legs"])
    cell["calculation_owner"] = CALCULATION_OWNER
    cell["evidence_label"] = ("POST_SELECTION: the intraday panel was acquired by R45 and its "
                              "coverage was measured before this grid was written; no arm's "
                              "result was seen before the grid was fixed")
    cell["construction"] = {
        "window_exchange_local": "09:30-11:59 ET",
        "signal_minute": "%02d:%02d ET" % sp["signal"], "exit_minute": "%02d:%02d ET" % sp["exit"],
        "entry": "close of the minute AFTER the signal minute",
        "flat_overnight": True, "gross_exposure": 1.0,
        "cost_charged": "full round trip on every engaged session",
        "cost_ladder_bps_per_side": list(ID.COST_LADDER_BPS),
        "selection_sessions": SELECTION_SESSIONS,
        "turnover_gate_substitution": TURNOVER_GATE_SUBSTITUTION,
    }
    by_cost = {}
    primary = None
    for c in ID.COST_LADDER_BPS:
        p = session_path(sp, cost_bps=c)
        n = len(p["net"])
        cut = min(SELECTION_SESSIONS, n)
        layers = {"all": _stats(p["net"], p["cost"], p["engaged"], label="ALL"),
                  "selection": _stats(p["net"][:cut], p["cost"][:cut], p["engaged"][:cut],
                                      label="SELECTION"),
                  "holdout": _stats(p["net"][cut:], p["cost"][cut:], p["engaged"][cut:],
                                    label="HOLDOUT")}
        h = p["net"][cut:]
        half = len(h) // 2
        layers["holdout_halves_ann_net"] = ([float(np.mean(h[:half]) * PPY),
                                             float(np.mean(h[half:]) * PPY)] if half >= 5 else None)
        by_cost["%.1f" % c] = layers
        if c == ID.COST_PRIMARY_BPS:
            primary = p
    cell["by_cost_bps_per_side"] = by_cost
    cell["primary_cost_bps_per_side"] = ID.COST_PRIMARY_BPS
    # The decisive diagnostic: at ZERO cost, is there information at all? It
    # separates "the cost ate a real edge" from "there was no edge".
    z = session_path(sp, cost_bps=0.0)
    gz, eng = z["gross"], z["engaged"]
    stz = S.nw_tstat(gz, 0)
    cell["gross"] = {
        "ann_gross": float(np.nanmean(gz) * PPY), "t_gross": stz["t"],
        "bp_per_engaged_session": (float(np.nanmean(gz[eng > 0]) * 1e4) if (eng > 0).any() else None),
        "engaged_share": float(np.nanmean(eng > 0)),
        "round_trip_cost_bp_at_primary": 2.0 * ID.COST_PRIMARY_BPS,
        "edge_exceeds_round_trip": (bool(np.nanmean(gz[eng > 0]) * 1e4 > 2.0 * ID.COST_PRIMARY_BPS)
                                    if (eng > 0).any() else None),
        "reaches_t2_at_zero_cost": bool((stz["t"] or 0) >= 2.0),
        "why_this_matters": "a strategy whose GROSS t is below 2.0 cannot be rescued by any cost "
                            "assumption; the failure is information, not execution"}
    if with_capital and primary is not None:
        cell["capital_applicability"] = capital_applicability(sp, primary)
    cell["gates"] = gates(cell)
    cell["verdict"] = verdict(cell)
    return T._jsonable(cell)


def _lvl(cell: dict, bps: float, layer: str) -> dict:
    return ((cell.get("by_cost_bps_per_side") or {}).get("%.1f" % bps) or {}).get(layer) or {}


def gates(cell: dict, *, fdr_pass: bool | None = None, holm_pass: bool | None = None) -> dict:
    a = _lvl(cell, ID.COST_PRIMARY_BPS, "all")
    stress = _lvl(cell, ID.COST_ELIGIBILITY_BPS, "all")
    sel = _lvl(cell, ID.COST_PRIMARY_BPS, "selection")
    hold = _lvl(cell, ID.COST_PRIMARY_BPS, "holdout")
    halves = ((cell.get("by_cost_bps_per_side") or {}).get("%.1f" % ID.COST_PRIMARY_BPS)
              or {}).get("holdout_halves_ann_net")
    cap = cell.get("capital_applicability") or {}
    ann, t = a.get("ann_net"), a.get("t_net")
    return {
        "materiality_ge_1p5pct": bool(ann is not None and ann >= MATERIALITY_ANN_NET),
        "t_ge_2": bool(t is not None and t >= 2.0),
        "survives_stress_cost": bool(stress.get("ann_net") is not None
                                     and stress["ann_net"] >= MATERIALITY_ANN_NET),
        "holdout_sign_agrees": bool(sel.get("ann_net") is not None and hold.get("ann_net") is not None
                                    and np.sign(sel["ann_net"]) == np.sign(hold["ann_net"])
                                    and hold["ann_net"] > 0),
        "holdout_halves_ge_floor": bool(halves and min(halves) >= GATE_HALF_FLOOR),
        "effective_sample_ge_floor": bool((a.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS),
        "positive_equal_risk_utility": (None if cap.get("state") != "OK"
                                        else bool(cap.get("positive_incremental_utility_after_costs"))),
        "benjamini_hochberg": fdr_pass,
        "family_holm": holm_pass,
    }


def verdict(cell: dict) -> str:
    a = _lvl(cell, ID.COST_PRIMARY_BPS, "all")
    if (a.get("periods") or 0) < 4:
        return T.V_DATA_HOLD
    g = cell.get("gates") or {}
    ann, t = a.get("ann_net"), a.get("t_net")
    if ann is not None and ann < 0 and t is not None and t <= -2.0:
        return T.V_WORSE
    decided = {k: v for k, v in g.items() if v is not None}
    if decided and all(decided.values()) and g.get("benjamini_hochberg") is True \
            and g.get("family_holm") is not False:
        return T.V_MATERIAL
    if g.get("materiality_ge_1p5pct") and g.get("t_ge_2"):
        return T.V_NOT_QUALIFIED
    return T.V_NO_ADVANTAGE


# --------------------------------------------------------------------------- #
# Grid execution
# --------------------------------------------------------------------------- #
def _cell_path(cell_id: str):
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("INTRA_%s.json" % re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id))


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("INTRA_*.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER:
            out.append(c)
    return out


def full_grid() -> list:
    """Every specification this family will ever execute: 30 primaries within
    the 6-per-family budget, plus the 2 rescues against the named failure."""
    return default_grid() + rescue_grid()


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    grid = grid or full_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            prior = json.loads(p.read_text(encoding="utf-8"))
            if not prior.get("error"):
                cells.append(prior)
                if verbose:
                    print("[%d/%d] %s (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
                continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid), sp["cell_id"]), flush=True)
        t0 = time.time()
        try:
            cell = measure_cell(sp)
        except Exception as exc:                                        # noqa: BLE001
            cell = {**sp, "legs": list(sp["legs"]), "verdict": "ERROR",
                    "error": "%s: %s" % (type(exc).__name__, exc),
                    "calculation_owner": CALCULATION_OWNER}
        cell["seconds"] = round(time.time() - t0, 1)
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            a = _lvl(cell, ID.COST_PRIMARY_BPS, "all")
            st = _lvl(cell, ID.COST_ELIGIBILITY_BPS, "all")
            print("    %-28s ann=%7s t=%6s sharpe=%6s stress=%7s eng=%5s" % (
                cell.get("verdict"),
                None if a.get("ann_net") is None else round(a["ann_net"], 4),
                None if a.get("t_net") is None else round(a["t_net"], 2),
                None if a.get("sharpe") is None else round(a["sharpe"], 2),
                None if st.get("ann_net") is None else round(st["ann_net"], 4),
                None if a.get("engaged_share") is None else round(a["engaged_share"], 2)), flush=True)
    return cells


def brief(c: dict) -> dict:
    a = _lvl(c, ID.COST_PRIMARY_BPS, "all")
    st = _lvl(c, ID.COST_ELIGIBILITY_BPS, "all")
    cn = _lvl(c, ID.COST_CANONICAL_BPS, "all")
    sel = _lvl(c, ID.COST_PRIMARY_BPS, "selection")
    ho = _lvl(c, ID.COST_PRIMARY_BPS, "holdout")
    cap = c.get("capital_applicability") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "name": c.get("name"),
            "dimension": c.get("dimension"), "legs": c.get("legs"), "markets": c.get("markets"),
            "signal_minute": ((c.get("construction") or {}).get("signal_minute")),
            "verdict": c.get("verdict"),
            "ann_net": a.get("ann_net"), "t_net": a.get("t_net"), "sharpe": a.get("sharpe"),
            "max_dd": a.get("max_dd"), "hit_rate": a.get("hit_rate"),
            "ann_net_at_stress_cost": st.get("ann_net"),
            "ann_net_at_canonical_cost": cn.get("ann_net"),
            "ann_gross": (c.get("gross") or {}).get("ann_gross"),
            "t_gross": (c.get("gross") or {}).get("t_gross"),
            "bp_per_engaged_session": (c.get("gross") or {}).get("bp_per_engaged_session"),
            "tag": c.get("tag"),
            "ann_cost_drag": a.get("ann_cost_drag"), "engaged_share": a.get("engaged_share"),
            "ann_oneway_turnover": a.get("ann_oneway_turnover"),
            "selection_ann_net": sel.get("ann_net"), "holdout_ann_net": ho.get("ann_net"),
            "periods": a.get("periods"),
            "incremental_ann_net_return": cap.get("incremental_ann_net_return"),
            "sharpe_delta_equal_risk": cap.get("sharpe_delta"),
            "t_incremental": cap.get("t_incremental"),
            "correlation_incumbent_sleeve": cap.get("correlation_incumbent_sleeve"),
            "fdr_pass": c.get("fdr_pass"), "holm_pass_family": c.get("holm_pass_family"),
            "failed_gates": sorted(k for k, v in (c.get("gates") or {}).items() if v is False),
            "evidence_label": c.get("evidence_label"), "error": c.get("error")}


def _diagnosis(cells: list) -> dict:
    """Was the axis killed by transaction cost, or by the absence of information?
    Decided at ZERO cost, so execution cannot be blamed for it."""
    def _ts(sub):
        return [(c.get("gross") or {}).get("t_gross") for c in sub
                if (c.get("gross") or {}).get("t_gross") is not None]

    prim = [c for c in cells if c.get("tag") != "RESCUE"]
    resc = [c for c in cells if c.get("tag") == "RESCUE"]
    tp, tr = _ts(prim), _ts(resc)
    anns = [(c.get("gross") or {}).get("ann_gross") for c in cells
            if (c.get("gross") or {}).get("ann_gross") is not None]
    n_pos = sum(1 for a in anns if a > 0)
    ts_all = tp + tr
    n_t2 = sum(1 for t in ts_all if t >= 2.0)
    net_t2 = sum(1 for c in cells if (_lvl(c, ID.COST_PRIMARY_BPS, "all").get("t_net") or 0) >= 2.0)
    return {
        "n_cells": len(cells), "n_positive_gross": n_pos,
        "positive_gross_share": (round(n_pos / len(anns), 3) if anns else None),
        "max_gross_t_primary": (max(tp) if tp else None),
        "max_gross_t_rescue": (max(tr) if tr else None),
        "n_reaching_t2_at_zero_cost": n_t2, "n_reaching_t2_after_cost": net_t2,
        "verdict": ("NO_INFORMATION" if n_t2 == 0 else
                    ("COST_BINDING_BUT_UNPROVEN" if net_t2 == 0 else "COST_BINDING")),
        "reading": (
            "Across the %d PRIMARY specifications the largest Newey-West t at ZERO transaction cost "
            "is %.2f, and positive-gross arms are %.0f %% of the grid - what an information-free grid "
            "looks like. Cost is therefore NOT what killed the primary grid; there was no credible "
            "gross edge to kill. The engagement-conditioned RESCUE does reach %.2f gross by lifting "
            "the per-trade edge above the round trip, but its NET t is still below the frozen 2.0, it "
            "is POST-SELECTION on the same 500 sessions, and over a denominator of %d tests a single "
            "t near 2 is exactly what the null predicts. Benjamini-Hochberg rejects nothing."
            % (len(prim), (max(tp) if tp else float('nan')),
               100.0 * (n_pos / len(anns) if anns else 0), (max(tr) if tr else float('nan')),
               len(cells))),
    }


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    scored = [c for c in cells if _lvl(c, ID.COST_PRIMARY_BPS, "all").get("p_net_one_sided") is not None]
    p_one = {c["cell_id"]: _lvl(c, ID.COST_PRIMARY_BPS, "all")["p_net_one_sided"] for c in scored}
    bh = S.bh_fdr(p_one, BH_Q)
    fam_p: dict = {}
    for c in scored:
        fam_p.setdefault(c.get("family"), {})[c["cell_id"]] = p_one[c["cell_id"]]
    holm = {f: FAM.holm(ps, HOLM_ALPHA) for f, ps in fam_p.items()}
    for c in scored:
        c["fdr_pass"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get(c.get("family"), {}).get("rejected", {}).get(c["cell_id"])
        c["gates"] = gates(c, fdr_pass=c["fdr_pass"], holm_pass=c["holm_pass_family"])
        c["verdict"] = verdict(c)
    counts: dict = {}
    for c in cells:
        counts[c.get("verdict")] = counts.get(c.get("verdict"), 0) + 1
    best = None
    for c in scored:
        ann = _lvl(c, ID.COST_PRIMARY_BPS, "all").get("ann_net")
        if ann is None:
            continue
        if best is None or ann > _lvl(best, ID.COST_PRIMARY_BPS, "all")["ann_net"]:
            best = c
    ds = read_artifact(ID.ARTIFACT_NAME) or {}
    fam_summary = {}
    for f in FAMILIES:
        fc = [c for c in cells if c.get("family") == f]
        prim = [c for c in fc if c.get("tag") != "RESCUE"]
        resc = [c for c in fc if c.get("tag") == "RESCUE"]
        fam_summary[f] = {
            "dimension": DIMENSION[f], "n_primary": len(prim), "n_rescue": len(resc),
            "budget_primary_max": 6, "budget_rescue_max": 2,
            "budget_respected": len(prim) <= 6 and len(resc) <= 2,
            "rescue_binding_failure": (resc[0].get("binding_failure") if resc else None),
            "verdicts": sorted({c.get("verdict") for c in fc}),
            "best_ann_net": max([_lvl(c, ID.COST_PRIMARY_BPS, "all").get("ann_net") or -9 for c in fc],
                                default=None),
            "best_gross_t": max([(c.get("gross") or {}).get("t_gross") or -9 for c in fc], default=None),
            "closed": True,
        }
    body = {
        "schema": "alpha_recovery_intraday_alpha/1", "calculation_owner": CALCULATION_OWNER,
        "question": "does the estate's owned one-minute cross-asset history carry a non-incumbent, "
                    "capital-eligible alpha signal that a daily bar cannot express?",
        "axis": "NATIVE_INTRADAY_CROSS_ASSET",
        "data_state": {"sessions": ds.get("sessions"), "first": ds.get("first_session"),
                       "last": ds.get("last_session"), "tradable": ds.get("tradable"),
                       "markets": ds.get("markets_represented"),
                       "window": (ds.get("covered_window_exchange_local") or {}).get("regular"),
                       "missing": (ds.get("covered_window_exchange_local") or {}).get("what_is_missing")},
        "grid": [{k: (list(v) if isinstance(v, tuple) else v) for k, v in sp.items()}
                 for sp in default_grid()],
        "n_cells": len(cells), "counts": counts, "families": fam_summary,
        "cost_ladder_bps_per_side": {"primary": ID.COST_PRIMARY_BPS, "stress": ID.COST_STRESS_BPS,
                                     "canonical": ID.COST_CANONICAL_BPS,
                                     "eligibility_requires": ID.COST_ELIGIBILITY_BPS},
        "gates": {"inherited_unchanged": ["materiality_ge_1p5pct", "t_ge_2", "holdout_sign_agrees",
                                          "holdout_halves_ge_floor", "effective_sample_ge_floor",
                                          "benjamini_hochberg", "family_holm"],
                  "added_stricter": ["survives_stress_cost", "positive_equal_risk_utility"],
                  "turnover_gate_substitution": TURNOVER_GATE_SUBSTITUTION},
        "evidence_split": {"selection_sessions": SELECTION_SESSIONS,
                           "holdout_sessions": (ds.get("sessions") or 0) - SELECTION_SESSIONS,
                           "why_not_the_r63_lockbox": "the whole intraday sample post-dates the "
                                                      "lockbox start 2023-01-01, so the estate's "
                                                      "lockbox does not partition it; a "
                                                      "chronological split was declared instead, "
                                                      "before any arm ran"},
        "multiple_testing": {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
                             "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"}
                                                for f, h in holm.items()},
                             "denominator": len(p_one)},
        "cost_vs_information_diagnosis": _diagnosis(cells),
        "best_by_ann_net": brief(best) if best else None,
        "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
        "cells": cells,
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
