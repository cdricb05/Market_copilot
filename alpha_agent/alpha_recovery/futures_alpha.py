r"""alpha_agent.alpha_recovery.futures_alpha - the campaign driver for the
native CME futures intraday panel.

WHAT THIS MODULE OWNS, AND WHAT IT DELIBERATELY DOES NOT
    It owns exactly one thing the estate does not already have: the mapping
    from the FULL 23-hour CME trade date to a set of target weights. That
    mapping has to be new, because three of the eight pre-registered families
    (mark-to-close, overnight -> RTH, the European lead) describe windows that
    did not exist in any panel this project has ever held, and no existing
    signal function can express them.

    Everything downstream of the weights is REUSED, not rebuilt:

      * ``intraday_alpha._stats``          the return statistics
      * ``intraday_alpha.gates``           the frozen gate battery
      * ``intraday_alpha.verdict``         the verdict vocabulary
      * ``intraday_alpha.equal_risk_daily``capital eligibility vs the incumbent
      * ``r63.sensitivity.nw_tstat``       the ONE t-statistic
      * ``r63.sensitivity.bh_fdr``         the ONE FDR correction
      * ``r64.family.holm``                the ONE family-wise correction

    There is no second scorer, no second multiplicity correction and no second
    book here, and `test_futures_campaign_owns_no_second_scorer` enforces it.

THE COST REBINDING, STATED PLAINLY
    ``intraday_alpha.gates`` keys into a cell's cost ladder through
    ``intraday_data``'s module constants, which are ETF basis points. A futures
    cost is a FORMULA of the price level, so the ladder differs per cell. The
    constants are therefore rebound around each gate evaluation, inside a
    context manager, and restored afterwards. This keeps ONE gate
    implementation rather than forking it - which is the whole point - and the
    rebinding is to STRICTER numbers for no cell: every level comes from the
    pre-registered formula in `futures_intraday`, untouched.

RESEARCH ONLY. No order, no fill, no registration, no promotion, no purchase.
"""
from __future__ import annotations

import contextlib
import json
import math
import re
import time
import warnings

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM

from . import (BH_Q, HOLM_ALPHA, MIN_EFFECTIVE_PERIODS, research_root, write_artifact)
from . import futures_intraday as FI
from . import intraday_alpha as IA
from . import intraday_data as ID
from . import tournament as T

CALCULATION_OWNER = "alpha_agent.alpha_recovery.futures_alpha"
ARTIFACT_NAME = "futures_alpha.json"
CELLS_DIR = "cells"
CELL_PREFIX = "FUT_"
PPY = FI.PPY

# --------------------------------------------------------------------------- #
# The evidence split, declared BEFORE any arm ran.
#
# 60 % selection / 40 % holdout, the same proportion the closed ETF axis used
# (300 of 500). It is chronological, and it is fixed here rather than chosen
# per family. The r63 lockbox does not partition this sample either: the whole
# panel post-dates the lockbox start of 2023-01-01.
# --------------------------------------------------------------------------- #
SELECTION_SHARE = 0.60

# Leg groups. The four Treasury contracts are ONE market at four tenors, which
# is why RATES is a single group and never four independent bets.
GROUPS = {
    "ALL10":       ("ES", "NQ", "GC", "CL", "6E", "6J", "ZT", "ZF", "ZN", "ZB"),
    "EQUITY":      ("ES", "NQ"),
    "RATES":       ("ZT", "ZF", "ZN", "ZB"),
    "FX":          ("6E", "6J"),
    "COMMOD":      ("GC", "CL"),
    "CROSS6":      ("ES", "NQ", "GC", "CL", "6E", "6J"),
}

#: Minute bounds used by the grid, in exchange-local wall clock on the trade
#: date. Every one of these is a WINDOW BOUND from the pre-registration, not a
#: swept parameter.
M_EVENING = (18, 0)          # start of the CME trade date's overnight
M_EU_OPEN, M_EU_CLOSE = (3, 0), (8, 0)
M_RTH_OPEN = (9, 30)
M_PRE_OPEN = (9, 29)
M_OR_END = (10, 0)           # opening range = the first 30 minutes of RTH
M_MID = (11, 0)              # the early-session read, inside the ETF panel's reach
M_ETF_MARK = (12, 59)        # the LAST minute the owned ETF panel can see
M_CLOSE = (16, 0)            # the mark this campaign has never been able to trade

VOL_LOOKBACK = 20


# --------------------------------------------------------------------------- #
# Signals
# --------------------------------------------------------------------------- #
def _px(pn, minute, legs):
    """(n_dates x n_legs) close at one exchange-local minute."""
    c = FI.minute_index(*minute)
    return np.column_stack([pn["close"][:, c, pn["instruments"].index(s)] for s in legs])


def _move(pn, a, b, legs):
    """Return from minute ``a`` to minute ``b``, WITHIN one trade date."""
    pa, pb = _px(pn, a, legs), _px(pn, b, legs)
    with np.errstate(invalid="ignore", divide="ignore"):
        return pb / pa - 1.0


def _prior_close(pn, legs):
    """The PRIOR trade date's 16:00 mark, shifted forward by one row."""
    p = _px(pn, M_CLOSE, legs)
    out = np.full_like(p, np.nan)
    out[1:] = p[:-1]
    return out


def _trailing_median(x, k):
    """Median of the k STRICTLY PRIOR trade dates. Never includes today."""
    return (pd.DataFrame(x).shift(1)
            .rolling(k, min_periods=max(5, k // 2)).median().to_numpy())


def raw_score(sp: dict) -> np.ndarray:
    """(n_dates x n_legs) the family's RAW signal, before any sign or condition.

    Every rule reads ONLY minutes strictly before the entry minute. That is the
    whole point-in-time claim of this module, and
    `test_futures_signal_uses_no_information_after_its_entry_minute` measures
    it rather than trusting it.
    """
    pn = FI.panel()
    legs = sp["legs"]
    rule = sp["rule"]
    n_d = pn["close"].shape[0]
    w = np.zeros((n_d, len(legs)))

    if rule == "overnight":
        # 18:00 ET the prior evening -> 09:29 ET. Both stamps belong to the same
        # trade date, but they sit on either side of the 00:00 ET calendar
        # boundary, so a roll can fall between them. Where it does, the
        # difference is a calendar spread and the arm stands flat.
        x = _move(pn, M_EVENING, M_PRE_OPEN, legs)
        w = np.where(FI.same_contract(pn, legs, across="midnight"), x, np.nan)
    elif rule == "europe":
        # 03:00 -> 08:00 ET, both on the trade date's own calendar day: one
        # continuous block of minutes in one contract.
        w = _move(pn, M_EU_OPEN, M_EU_CLOSE, legs)
    elif rule == "carry":
        # prior trade date's CLOSE -> this trade date's RTH open. This one
        # differences two trade dates, so it is the span most exposed to the
        # roll of all.
        prev = _prior_close(pn, legs)
        with np.errstate(invalid="ignore", divide="ignore"):
            x = _px(pn, M_RTH_OPEN, legs) / prev - 1.0
        w = np.where(FI.same_contract(pn, legs, across="trade_date"), x, np.nan)
    elif rule == "early":
        # the early RTH read the owned ETF panel CAN see, traded into a window
        # it cannot
        w = _move(pn, M_RTH_OPEN, M_MID, legs)
    elif rule == "range":
        a, b = FI.minute_index(*M_RTH_OPEN), FI.minute_index(*M_OR_END)
        m = FI.minute_index(*M_MID)
        for k, s in enumerate(legs):
            j = pn["instruments"].index(s)
            # a trade date with no opening-range print at all is not an
            # opportunity, it is an absence; it yields NaN and then zero weight
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                orh = np.nanmax(pn["high"][:, a:b + 1, j].astype(float), axis=1)
                orl = np.nanmin(pn["low"][:, a:b + 1, j].astype(float), axis=1)
            p = pn["close"][:, m, j]
            # the raw score is HOW FAR beyond the range, not merely which side:
            # the magnitude is what a conditional-engagement rescue needs
            with np.errstate(invalid="ignore", divide="ignore"):
                w[:, k] = np.where(p > orh, p / orh - 1.0,
                                   np.where(p < orl, p / orl - 1.0, 0.0))
    elif rule == "volstate":
        a, b = FI.minute_index(*M_RTH_OPEN), FI.minute_index(*M_OR_END)
        for k, s in enumerate(legs):
            j = pn["instruments"].index(s)
            seg = pn["close"][:, a:b + 1, j]
            with np.errstate(invalid="ignore", divide="ignore"), warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                lr = np.diff(np.log(seg), axis=1)
                rv = np.nanstd(lr, axis=1)
            ref = _trailing_median(rv.reshape(-1, 1), VOL_LOOKBACK).ravel()
            with np.errstate(invalid="ignore", divide="ignore"):
                move = seg[:, -1] / seg[:, 0] - 1.0
            ok = (rv < ref) if sp["state"] == "COMPRESSED" else (rv >= ref)
            w[:, k] = move * (ok & np.isfinite(ref)).astype(float)
    elif rule == "lead":
        # one SOURCE market's early move, traded in the equity complex
        x = _move(pn, M_RTH_OPEN, M_MID, [sp["source"]])
        w = np.repeat(x, len(legs), axis=1)
    elif rule == "relstrength":
        x = _move(pn, M_RTH_OPEN, M_MID, legs)
        # scale each leg by its OWN trailing volatility so a cross-sectional
        # rank is not simply a ranking of which contract moves most
        vol = _trailing_median(np.abs(x), VOL_LOOKBACK)
        with np.errstate(invalid="ignore", divide="ignore"):
            z = np.where(np.isfinite(vol) & (vol > 0), x / vol, np.nan)
        n_ok = np.maximum(np.isfinite(z).sum(axis=1, keepdims=True), 1)
        mean = np.nansum(z, axis=1, keepdims=True) / n_ok
        w = np.where(np.isfinite(z), z - mean, np.nan)
    else:
        raise ValueError("unknown rule %r" % rule)
    # The raw score carries NO direction: ``sp["sign"]`` is applied once, in
    # ``signals``. Keeping it out of here is what lets the rescue condition on
    # |signal| without the condition depending on which way the arm leans.
    return w


#: The rescue form, and its two constants, are INHERITED from the closed ETF
#: axis (`intraday_alpha.RESCUE_LOOKBACK_SESSIONS` / `RESCUE_PERCENTILE`)
#: rather than chosen here. That matters: reusing an already-fixed rescue means
#: a rescue adds no new search, only a new application of a rule that was
#: settled before this panel existed.
RESCUE_LOOKBACK = IA.RESCUE_LOOKBACK_SESSIONS
RESCUE_PERCENTILE = IA.RESCUE_PERCENTILE


def signals(sp: dict) -> np.ndarray:
    """(n_dates x n_legs) target weights, gross exposure 1.0 when engaged."""
    x = raw_score(sp)
    w = float(sp["sign"]) * np.sign(x)
    if sp.get("conditional"):
        # The rescue: engage ONLY when the signal's own magnitude clears its
        # STRICTLY PRIOR distribution. This addresses the named, measured
        # binding failure - unconditional engagement - and nothing else; the
        # sign, the windows, the legs and the marks are all untouched.
        thr = (pd.DataFrame(np.abs(x)).shift(1)
               .rolling(RESCUE_LOOKBACK, min_periods=RESCUE_LOOKBACK // 2)
               .quantile(RESCUE_PERCENTILE / 100.0).to_numpy())
        w = np.where(np.isfinite(thr) & (np.abs(x) >= thr), w, 0.0)
    w = np.nan_to_num(w)
    gross = np.abs(w).sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(gross > 0, w / gross, 0.0)


# --------------------------------------------------------------------------- #
# The trade-date return path
# --------------------------------------------------------------------------- #
def session_path(sp: dict, *, cost_bps: float, exit_minute=None) -> dict:
    """Per-trade-date NET return of one arm at one cost level.

    Entry is the close of the minute AFTER the last minute the signal reads;
    exit is the close of the exit minute of the SAME trade date. The full round
    trip is charged on every engaged date, exactly as the ETF axis charged it.
    """
    pn = FI.panel()
    legs = sp["legs"]
    entry = sp["entry"]
    exit_m = exit_minute or sp["exit"]
    ei, ni = FI.minute_index(*exit_m), FI.minute_index(*entry)
    if ni >= ei:
        raise ValueError("entry minute is not before the exit minute")
    w = signals(sp)
    leg_r = _move(pn, entry, exit_m, legs)
    gross_r = np.nansum(w * np.nan_to_num(leg_r), axis=1)
    engaged = np.abs(w).sum(axis=1)
    cost = engaged * 2.0 * float(cost_bps) * 1e-4
    return {"net": gross_r - cost, "gross": gross_r, "cost": cost,
            "engaged": engaged, "weights": w, "dates": pn["dates"]}


@contextlib.contextmanager
def _as_cost_ladder(lad: dict):
    """Point ``intraday_alpha.gates`` at THIS cell's futures cost ladder.

    The gate logic is not copied; only the three numbers it keys on are
    rebound, then restored. Nothing about the thresholds changes.
    """
    keys = ("COST_PRIMARY_BPS", "COST_STRESS_BPS", "COST_CANONICAL_BPS",
            "COST_ELIGIBILITY_BPS", "COST_LADDER_BPS")
    saved = {k: getattr(ID, k) for k in keys}
    try:
        ID.COST_PRIMARY_BPS = lad["PRIMARY"]
        ID.COST_STRESS_BPS = lad["STRESS"]
        ID.COST_CANONICAL_BPS = lad["CANONICAL"]
        ID.COST_ELIGIBILITY_BPS = lad[FI.COST_ELIGIBILITY_LEVEL]
        ID.COST_LADDER_BPS = (lad["PRIMARY"], lad["STRESS"], lad["CANONICAL"])
        yield
    finally:
        for k, v in saved.items():
            setattr(ID, k, v)


def measure_cell(sp: dict, *, with_capital: bool = True) -> dict:
    cell = dict(sp)
    cell["legs"] = list(sp["legs"])
    cell["markets"] = sorted({FI.MARKET[s] for s in sp["legs"]})
    cell["calculation_owner"] = CALCULATION_OWNER
    lad = FI.cost_ladder_bps(sp["legs"])
    cell["cost_ladder_bps_per_side"] = {k: round(v, 4) for k, v in lad.items()}
    cell["evidence_label"] = (
        "PRE-REGISTERED: every window, cost level, family and falsifier in this cell was "
        "committed in futures_intraday.py while the panel was still priced-but-not-downloaded")
    cell["construction"] = {
        "window_exchange_local": "the full CME trade date, 17:00 ET roll",
        "entry_minute": "%02d:%02d ET" % sp["entry"], "exit_minute": "%02d:%02d ET" % sp["exit"],
        "entry": "close of the entry minute, strictly after every minute the signal reads",
        "held_overnight": False, "gross_exposure": 1.0,
        "cost_charged": "full round trip on every engaged trade date",
        "cost_rule": "max over legs of the pre-registered per-side formula at the panel-median price",
        "selection_share": SELECTION_SHARE,
    }

    by_cost, primary = {}, None
    for level in ("PRIMARY", "STRESS", "CANONICAL"):
        c = lad[level]
        p = session_path(sp, cost_bps=c)
        n = len(p["net"])
        cut = int(round(SELECTION_SHARE * n))
        layers = {"all": IA._stats(p["net"], p["cost"], p["engaged"], label="ALL"),
                  "selection": IA._stats(p["net"][:cut], p["cost"][:cut], p["engaged"][:cut],
                                         label="SELECTION"),
                  "holdout": IA._stats(p["net"][cut:], p["cost"][cut:], p["engaged"][cut:],
                                       label="HOLDOUT")}
        h = p["net"][cut:]
        half = len(h) // 2
        layers["holdout_halves_ann_net"] = ([float(np.mean(h[:half]) * PPY),
                                             float(np.mean(h[half:]) * PPY)] if half >= 5 else None)
        by_cost["%.1f" % c] = layers
        if level == "PRIMARY":
            primary = p
    cell["by_cost_bps_per_side"] = by_cost
    cell["primary_cost_bps_per_side"] = lad["PRIMARY"]

    # The decisive diagnostic, inherited from the closed axis: at ZERO cost, is
    # there information at all? It separates "the cost ate a real edge" from
    # "there was no edge", and it is the declared falsifier for five families.
    z = session_path(sp, cost_bps=0.0)
    gz, eng = z["gross"], z["engaged"]
    stz = S.nw_tstat(gz[np.isfinite(gz)], 0)
    cell["gross"] = {
        "ann_gross": float(np.nanmean(gz) * PPY), "t_gross": stz["t"],
        "bp_per_engaged_session": (float(np.nanmean(gz[eng > 0]) * 1e4) if (eng > 0).any() else None),
        "engaged_share": float(np.nanmean(eng > 0)),
        "round_trip_cost_bp_at_primary": 2.0 * lad["PRIMARY"],
        "edge_exceeds_round_trip": (bool(np.nanmean(gz[eng > 0]) * 1e4 > 2.0 * lad["PRIMARY"])
                                    if (eng > 0).any() else None),
        "reaches_t2_at_zero_cost": bool((stz["t"] or 0) >= 2.0),
        "why_this_matters": "a strategy whose GROSS t is below 2.0 cannot be rescued by any cost "
                            "assumption; the failure is information, not execution"}

    # The FUT_MARK_TO_CLOSE falsifier, measured rather than asserted: the SAME
    # arm marked at 12:59 ET, which is the last minute the owned ETF panel can
    # see. This is a CONTROL, not a candidate for capital - it is never
    # promoted and so is not a selectable hypothesis - and it is therefore
    # reported beside the cell rather than entered into the BH denominator.
    if sp.get("etf_mark_twin"):
        tw = session_path(sp, cost_bps=lad["PRIMARY"], exit_minute=M_ETF_MARK)
        st = IA._stats(tw["net"], tw["cost"], tw["engaged"], label="ETF_MARK_CONTROL")
        full = by_cost["%.1f" % lad["PRIMARY"]]["all"]
        cell["etf_mark_control"] = {
            "exit_minute": "%02d:%02d ET" % M_ETF_MARK,
            "is_a_control_not_a_candidate": True,
            "why_not_in_the_bh_denominator": "it is never eligible for capital, so it is not a "
                                             "hypothesis the campaign could select",
            "ann_net_marked_1259": st.get("ann_net"), "t_net_marked_1259": st.get("t_net"),
            "ann_net_marked_1600": full.get("ann_net"), "t_net_marked_1600": full.get("t_net"),
            "close_mark_adds_ann_net": (None if st.get("ann_net") is None or full.get("ann_net") is None
                                        else full["ann_net"] - st["ann_net"])}

    if with_capital and primary is not None:
        inc = IA.incumbent_daily_path()
        sleeve = pd.Series(primary["net"], index=pd.to_datetime(primary["dates"]))
        cell["capital_applicability"] = (
            {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
            if inc is None or len(inc) == 0
            else IA.equal_risk_daily(sleeve, inc, label=sp["cell_id"]))

    with _as_cost_ladder(lad):
        cell["gates"] = IA.gates(cell)
        cell["verdict"] = IA.verdict(cell)
    return T._jsonable(cell)


# --------------------------------------------------------------------------- #
# The pre-registered grid: at most 6 primaries per family
# --------------------------------------------------------------------------- #
def _cell(family, name, rule, group, sign, *, entry, exit_=M_CLOSE, **kw):
    sp = {"cell_id": "%s|%s" % (family, name), "family": family, "name": name,
          "rule": rule, "group": group, "legs": list(GROUPS[group]), "sign": sign,
          "entry": entry, "exit": exit_}
    sp.update(kw)
    return sp


def default_grid() -> list:
    g = []

    # FUT_MARK_TO_CLOSE - the early RTH read traded into the afternoon the ETF
    # panel cannot reach, with the 12:59 mark attached as the control.
    for grp in ("ALL10", "EQUITY", "COMMOD"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_CLOSE, "%s_%s" % (tag, grp), "early", grp, s,
                           entry=(11, 1), etf_mark_twin=True))

    # FUT_OVERNIGHT_TO_RTH - 18:00 ET -> 09:29 ET, traded across the whole RTH.
    for grp in ("ALL10", "EQUITY", "FX"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_OVERNIGHT, "%s_%s" % (tag, grp), "overnight", grp, s,
                           entry=M_RTH_OPEN))

    # FUT_EUROPE_LEAD - the European cash session, invisible to the ETF panel.
    for grp in ("ALL10", "EQUITY", "RATES"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_EUROPE, "%s_%s" % (tag, grp), "europe", grp, s,
                           entry=M_RTH_OPEN))

    # FUT_SESSION_CARRY - the closed axis's own statement, now markable to the
    # close. Its prior falsification is carried forward in the family contract.
    for grp in ("ALL10", "EQUITY", "COMMOD"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_CARRY, "%s_%s" % (tag, grp), "carry", grp, s,
                           entry=(9, 31)))

    # FUT_OPENING_RANGE
    for grp in ("ALL10", "EQUITY", "COMMOD"):
        for tag, s in (("BREAK", 1.0), ("FADE", -1.0)):
            g.append(_cell(FI.FAM_RANGE, "%s_%s" % (tag, grp), "range", grp, s,
                           entry=(11, 1)))

    # FUT_VOLATILITY_STATE
    for st in ("COMPRESSED", "EXPANDED"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_VOL, "%s_%s_ALL10" % (st, tag), "volstate", "ALL10", s,
                           entry=(10, 1), state=st))
    for st in ("COMPRESSED", "EXPANDED"):
        g.append(_cell(FI.FAM_VOL, "%s_MOM_EQUITY" % st, "volstate", "EQUITY", 1.0,
                       entry=(10, 1), state=st))

    # FUT_CROSS_MARKET_LEADLAG - transmission into the equity complex.
    for src in ("ZN", "GC", "CL", "6E"):
        g.append(_cell(FI.FAM_LEAD, "MOM_%s_TO_EQUITY" % src, "lead", "EQUITY", 1.0,
                       entry=(11, 1), source=src))
    for src in ("ZN", "GC"):
        g.append(_cell(FI.FAM_LEAD, "REV_%s_TO_EQUITY" % src, "lead", "EQUITY", -1.0,
                       entry=(11, 1), source=src))

    # FUT_RELATIVE_STRENGTH - cross-sectional, vol-scaled.
    for grp in ("ALL10", "CROSS6", "RATES"):
        for tag, s in (("MOM", 1.0), ("REV", -1.0)):
            g.append(_cell(FI.FAM_RS, "%s_%s" % (tag, grp), "relstrength", grp, s,
                           entry=(11, 1)))
    return g


#: The rescue is permitted by contract section 7 ONLY against a named, measured
#: binding failure. This is that name, and `rescue_grid` refuses to build a
#: rescue for any family in which it is not actually measured.
BINDING_FAILURE = (
    "UNCONDITIONAL_ENGAGEMENT: every primary arm engages on essentially every trade date "
    "(engaged_share >= 0.90), so it pays a full round trip 252 times a year whether the "
    "signal is strong or negligible. On the ten-root arms the pre-registered cost rule "
    "charges the most expensive leg's rate to every leg, which is ~7 %/yr of drag against "
    "a gross edge an order of magnitude smaller. The failure is measured, not supposed.")
RESCUE_MIN_ENGAGED_SHARE = 0.90
#: How many families may be rescued. A budget, not a threshold on the data.
RESCUE_FAMILIES = 3


def rescue_grid(cells: list) -> list:
    """At most 2 rescues each, in the families where the binding failure BINDS.

    Stated plainly, because it matters for how the result may be read: WHICH
    families are rescued is chosen AFTER seeing the primary measurements. That
    is what a rescue is - the contract permits it only against a failure that
    has actually been measured, so it cannot be chosen before. What is NOT
    chosen afterwards is the rescue's form or its constants: both are inherited
    unchanged from the closed ETF axis. Every rescue produced here enters the
    BH denominator exactly like a primary.
    """
    by_fam: dict = {}
    for c in cells:
        if c.get("tag") == "RESCUE" or not c.get("family"):
            continue
        by_fam.setdefault(c["family"], []).append(c)

    ranked = []
    for fam, fc in by_fam.items():
        eng = max((_prim(c).get("engaged_share") or 0.0) for c in fc)
        if eng < RESCUE_MIN_ENGAGED_SHARE:
            continue                       # the named failure does not bind here
        best_t = max(((c.get("gross") or {}).get("t_gross") or -9) for c in fc)
        ranked.append((best_t, fam, fc))
    ranked.sort(reverse=True)

    out = []
    for _t, fam, fc in ranked[:RESCUE_FAMILIES]:
        top = sorted(fc, key=lambda c: -((c.get("gross") or {}).get("t_gross") or -9))
        for c in top[:FI.MAX_RESCUES_PER_FAMILY]:
            sp = {k: v for k, v in c.items()
                  if k in ("family", "rule", "group", "legs", "sign", "entry", "exit",
                           "source", "state", "etf_mark_twin")}
            sp["entry"] = tuple(sp["entry"])
            sp["exit"] = tuple(sp["exit"])
            sp["name"] = "%s_RESCUE_CONDITIONAL" % c["name"]
            sp["cell_id"] = "%s|%s" % (fam, sp["name"])
            sp["tag"] = "RESCUE"
            sp["conditional"] = True
            sp["binding_failure"] = BINDING_FAILURE
            sp["rescue_rule"] = ("engage only when |signal| >= its own %dth percentile over the "
                                 "%d STRICTLY PRIOR trade dates; form and constants inherited "
                                 "from intraday_alpha, not chosen here"
                                 % (RESCUE_PERCENTILE, RESCUE_LOOKBACK))
            out.append(sp)
    return out


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
def _cell_path(cell_id: str):
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("%s%s.json" % (CELL_PREFIX, re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id)))


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("%s*.json" % CELL_PREFIX)):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER:
            out.append(c)
    return out


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    grid = grid if grid is not None else default_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            prior = json.loads(p.read_text(encoding="utf-8"))
            if not prior.get("error"):
                cells.append(prior)
                if verbose:
                    print("[%2d/%d] %-40s (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
                continue
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
            lad = cell.get("cost_ladder_bps_per_side") or {}
            a = _lvl(cell, lad.get("PRIMARY"), "all")
            st = _lvl(cell, lad.get(FI.COST_ELIGIBILITY_LEVEL), "all")
            print("[%2d/%d] %-40s %-14s ann=%8s t=%6s gross_t=%6s stress=%8s eng=%5s  %.0fs"
                  % (i, len(grid), sp["cell_id"], cell.get("verdict"),
                     _r(a.get("ann_net"), 4), _r(a.get("t_net"), 2),
                     _r((cell.get("gross") or {}).get("t_gross"), 2),
                     _r(st.get("ann_net"), 4), _r(a.get("engaged_share"), 2),
                     cell["seconds"]), flush=True)
    return cells


def _r(x, n):
    return None if x is None else round(x, n)


def _lvl(cell: dict, bps, layer: str) -> dict:
    if bps is None:
        return {}
    return ((cell.get("by_cost_bps_per_side") or {}).get("%.1f" % bps) or {}).get(layer) or {}


def _prim(cell: dict, layer: str = "all") -> dict:
    return _lvl(cell, (cell.get("cost_ladder_bps_per_side") or {}).get("PRIMARY"), layer)


def _stress(cell: dict) -> dict:
    lad = cell.get("cost_ladder_bps_per_side") or {}
    return _lvl(cell, lad.get(FI.COST_ELIGIBILITY_LEVEL), "all")


def brief(c: dict) -> dict:
    a, st = _prim(c), _stress(c)
    cap = c.get("capital_applicability") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "name": c.get("name"),
            "group": c.get("group"), "markets": c.get("markets"), "verdict": c.get("verdict"),
            "ann_net": a.get("ann_net"), "t_net": a.get("t_net"), "sharpe": a.get("sharpe"),
            "max_dd": a.get("max_dd"), "hit_rate": a.get("hit_rate"),
            "ann_net_at_stress_cost": st.get("ann_net"),
            "ann_gross": (c.get("gross") or {}).get("ann_gross"),
            "t_gross": (c.get("gross") or {}).get("t_gross"),
            "bp_per_engaged_session": (c.get("gross") or {}).get("bp_per_engaged_session"),
            "round_trip_cost_bp": (c.get("gross") or {}).get("round_trip_cost_bp_at_primary"),
            "engaged_share": a.get("engaged_share"),
            "incremental_ann_net_vs_incumbent": cap.get("incremental_ann_net_return"),
            "t_incremental": cap.get("t_incremental"),
            "gates": c.get("gates"), "fdr_pass": c.get("fdr_pass"),
            "holm_pass_family": c.get("holm_pass_family")}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    scored = [c for c in cells if _prim(c).get("p_net_one_sided") is not None]
    p_one = {c["cell_id"]: _prim(c)["p_net_one_sided"] for c in scored}
    bh = S.bh_fdr(p_one, BH_Q)
    fam_p: dict = {}
    for c in scored:
        fam_p.setdefault(c.get("family"), {})[c["cell_id"]] = p_one[c["cell_id"]]
    holm = {f: FAM.holm(ps, HOLM_ALPHA) for f, ps in fam_p.items()}
    for c in scored:
        c["fdr_pass"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get(c.get("family"), {}).get("rejected", {}).get(c["cell_id"])
        with _as_cost_ladder({k: v for k, v in (c.get("cost_ladder_bps_per_side") or {}).items()}):
            c["gates"] = IA.gates(c, fdr_pass=c["fdr_pass"], holm_pass=c["holm_pass_family"])
            c["verdict"] = IA.verdict(c)

    counts: dict = {}
    for c in cells:
        counts[c.get("verdict")] = counts.get(c.get("verdict"), 0) + 1

    fam_summary = {}
    for f in FI.FAMILIES:
        fc = [c for c in cells if c.get("family") == f]
        prim = [c for c in fc if c.get("tag") != "RESCUE"]
        resc = [c for c in fc if c.get("tag") == "RESCUE"]
        best_t = max([(c.get("gross") or {}).get("t_gross") or -9 for c in fc], default=None)
        fam_summary[f] = {
            "claim": FI.FAMILY_CONTRACT[f]["claim"],
            "falsified_by": FI.FAMILY_CONTRACT[f]["falsified_by"],
            "n_primary": len(prim), "n_rescue": len(resc),
            "budget_primary_max": FI.MAX_PRIMARY_PER_FAMILY,
            "budget_rescue_max": FI.MAX_RESCUES_PER_FAMILY,
            "budget_respected": (len(prim) <= FI.MAX_PRIMARY_PER_FAMILY
                                 and len(resc) <= FI.MAX_RESCUES_PER_FAMILY),
            "verdicts": sorted({str(c.get("verdict")) for c in fc}),
            "best_ann_net": max([_prim(c).get("ann_net") or -9 for c in fc], default=None),
            "best_gross_t": best_t,
            "any_qualified": any(c.get("verdict") == T.V_MATERIAL for c in fc),
            "closed": True,
            "closed_because": ("a candidate qualified" if any(c.get("verdict") == T.V_MATERIAL for c in fc)
                               else "the declared falsifier fired: best gross t %.2f < 2.0 at zero cost"
                                    % (best_t or 0.0) if (best_t or -9) < 2.0
                               else "no arm cleared the frozen gate battery"),
        }

    # FUT_MARK_TO_CLOSE is the one family whose declared falsifier is NOT the
    # gross-t rule, so it is evaluated on its own terms: does the 16:00 mark
    # beat the 12:59 mark the owned ETF panel is limited to? This is the single
    # most consequential question the acquisition can answer, because marking
    # to the close is the capability the credits were spent on.
    deltas = [(c.get("etf_mark_control") or {}).get("close_mark_adds_ann_net")
              for c in cells if c.get("family") == FI.FAM_CLOSE]
    deltas = [d for d in deltas if d is not None]
    if deltas:
        arr = np.array(deltas, dtype=float)
        n_pos = int((arr > 0).sum())
        adds = bool(arr.mean() > 0 and n_pos > len(arr) / 2.0)
        fam_summary[FI.FAM_CLOSE]["mark_to_close_falsifier"] = {
            "question": "does holding to 16:00 ET beat holding to 12:59 ET, the last minute the "
                        "owned ETF panel can see?",
            "n_arms": len(arr), "mean_ann_net_added": float(arr.mean()),
            "median_ann_net_added": float(np.median(arr)),
            "arms_improved_by_the_close_mark": n_pos,
            "falsified": not adds,
            "reading": ("the close mark adds nothing systematic: the gains and losses are "
                        "mirror images across the sign pairs, which is what a zero effect "
                        "looks like when every arm has a sign twin" if not adds else
                        "the close mark adds return beyond the ETF panel's reach"),
            "why_it_matters": "marking to the close is the capability this panel was acquired "
                              "for; this measures whether it paid, independently of whether any "
                              "arm qualified",
        }
        if not adds and not fam_summary[FI.FAM_CLOSE]["any_qualified"]:
            fam_summary[FI.FAM_CLOSE]["closed_because"] = (
                "its OWN declared falsifier fired: across %d arms the 16:00 mark added a median "
                "of %+.4f /yr over the 12:59 mark, with %d of %d arms improved - the signature of "
                "no effect, not of a small one" % (len(arr), float(np.median(arr)), n_pos, len(arr)))

    best = None
    for c in scored:
        ann = _prim(c).get("ann_net")
        if ann is None:
            continue
        if best is None or ann > _prim(best).get("ann_net", -9):
            best = c
    qualified = [brief(c) for c in cells if c.get("verdict") == T.V_MATERIAL]
    pn = FI.panel()

    body = {
        "schema": "alpha_recovery_futures_alpha/1", "calculation_owner": CALCULATION_OWNER,
        "question": "does genuine native CME intraday history across ten economically distinct "
                    "contracts carry a non-incumbent, capital-eligible alpha signal that the "
                    "owned 09:30-12:59 ET ETF panel could not express?",
        "axis": "NATIVE_CME_FUTURES_INTRADAY",
        "panel": {"trade_dates": len(pn["dates"]), "first": pn["dates"][0], "last": pn["dates"][-1],
                  "roots": pn["instruments"],
                  "markets": sorted({FI.MARKET[r] for r in pn["instruments"]}),
                  "buckets": sorted({FI.UNIVERSE[r][1] for r in pn["instruments"]}),
                  "minutes_per_trade_date": FI.TD_MINUTES,
                  "trade_date_rolls_at_et_hour": FI.TRADE_DATE_ROLL_HOUR_ET,
                  "vs_owned_etf_panel": "500 sessions x 150 minutes x 4 markets, 09:30-12:59 ET, "
                                        "never markable to the close"},
        "evidence_split": {"selection_share": SELECTION_SHARE,
                           "selection_dates": int(round(SELECTION_SHARE * len(pn["dates"]))),
                           "holdout_dates": len(pn["dates"]) - int(round(SELECTION_SHARE * len(pn["dates"]))),
                           "declared_before_any_arm_ran": True,
                           "why_not_the_r63_lockbox": "the whole panel post-dates the lockbox "
                                                      "start of 2023-01-01"},
        "n_cells": len(cells), "counts": counts, "families": fam_summary,
        "cost_ladder": {"form": "per_side_bps = 10000*(ticks*tick_size + commission_usd/multiplier)/price",
                        "levels": FI.COST_LADDER,
                        "eligibility_requires_surviving": FI.COST_ELIGIBILITY_LEVEL,
                        "per_root_round_trip_bp_at_primary":
                            {r: round(FI.round_trip_bps(r, FI.representative_price(r), "PRIMARY"), 4)
                             for r in pn["instruments"]},
                        "charged_per_cell": "the MOST EXPENSIVE leg's rate, on every leg"},
        "frozen_gates": FI.FROZEN_GATES,
        "gates_unchanged_by_acquiring_data": True,
        "multiple_testing": {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
                             "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"}
                                                for f, h in holm.items()},
                             "denominator": len(p_one),
                             "controls_excluded_from_denominator":
                                 "the 12:59 ETF-mark twins are controls, never candidates for "
                                 "capital, so they are not selectable hypotheses"},
        "qualified_for_true_forward": qualified,
        "true_forward_ready": bool(qualified),
        "best_by_ann_net": brief(best) if best else None,
        "best_by_gross_t": brief(max(cells, key=lambda c: (c.get("gross") or {}).get("t_gross") or -9))
                           if cells else None,
        "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
        "safety": {"paper_only": True, "research_only": True, "creates_orders": False,
                   "registers_forward_challenger": False, "automatic_promotion": False,
                   "live_checkout_read_only": True},
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


if __name__ == "__main__":                                   # pragma: no cover
    run_grid()
    print(json.dumps({k: v for k, v in merge().items() if k not in ("brief", "cells")},
                     indent=1, default=str))
