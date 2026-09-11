r"""alpha_agent.alpha_recovery.microstructure_alpha - the native CME ORDER-FLOW
campaign.

WHAT THIS MODULE IS, AND WHAT IT DELIBERATELY IS NOT
    It is the MINIMUM ADAPTER that turns a top-of-book panel into the daily
    strategy return series the existing machinery already scores. It owns the
    signal definitions and the intraday entry schedule, and NOTHING else.

    Every statistical decision rule is imported and reused UNCHANGED:
    ``intraday_alpha._stats``, ``.gates``, ``.verdict``, ``.equal_risk_daily``,
    ``.incumbent_daily_path``; ``stats.bh_fdr`` and ``.nw_tstat``;
    ``family.holm``. There is no second scorer, no second gate, no second
    threshold, and ``test_microstructure_owns_no_second_scorer`` pins that.

THE ONE THING THAT IS GENUINELY NEW: MANY ENTRIES PER TRADE DATE
    The OHLCV axis took one position per trade date. Microstructure decays in
    minutes, so an arm at horizon H enters every H minutes through the RTH
    session and holds for exactly H minutes. Two consequences are declared in
    advance because both are ways to manufacture a result:

    1. ENTRIES DO NOT OVERLAP. At horizon H the next entry is H minutes later,
       so no two positions are ever open at once. Overlapping entries would
       multiply the apparent sample without adding independent information,
       which is the easiest way to inflate an intraday t-statistic.

    2. THE DAY'S RETURN IS THE SUM OVER ITS ENTRIES, NOT THE MEAN. Because the
       entries are sequential rather than simultaneous, full notional is
       available to each, so the day trades 390/H round trips - and is charged
       for every one of them. Averaging instead would have quietly charged ONE
       round trip a day however fast the arm traded, which is the single
       assumption that would have made a minute-horizon strategy look cheap.

LATENCY IS CHARGED, NOT ASSUMED AWAY
    A signal read from the book at minute m is entered at the mid of minute
    m+1 and exited at the mid of m+1+H. A full minute separates observation
    from execution. Reading the book at m and filling at m is a half-second
    round trip this estate has no evidence it could achieve.

RESEARCH ONLY. No order, no fill, no promotion, no registration.
"""
from __future__ import annotations

import contextlib
import json

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import family as FAM
from . import research_root, write_artifact
from . import futures_intraday as FI
from . import intraday_alpha as IA
from . import intraday_data as ID
from . import microstructure as MS
from . import tournament as T

PPY = FI.PPY

CALCULATION_OWNER = "alpha_agent.alpha_recovery.microstructure_alpha"
ARTIFACT_NAME = "microstructure_alpha.json"

SELECTION_SHARE = MS.SELECTION_SHARE
RESCUE_LOOKBACK = MS.RESCUE_LOOKBACK_SESSIONS
RESCUE_PERCENTILE = MS.RESCUE_PERCENTILE

#: Leg groups. Same non-additivity rule as the OHLCV axis: four Treasuries are
#: one rates market, so only ZN was bought and RATES is a single leg.
GROUPS = {
    "ALL7": ("ES", "NQ", "GC", "CL", "6E", "6J", "ZN"),
    "EQUITY": ("ES", "NQ"),
    "FX": ("6E", "6J"),
    "COMMOD": ("GC", "CL"),
    "CROSS6": ("ES", "NQ", "GC", "CL", "6E", "6J"),
}

#: The RTH entry schedule, in trade-date columns.
ENTRY_FIRST = MS.ENTRY_FIRST_ET
ENTRY_LAST = MS.ENTRY_LAST_ET
#: Minutes of trailing book history a state signal may look back over. Fixed,
#: not swept: one value, declared, used by every family that needs a baseline.
LOOKBACK_MINUTES = 30


# --------------------------------------------------------------------------- #
# Panel access
# --------------------------------------------------------------------------- #
def _idx(pn, legs) -> list:
    return [pn["instruments"].index(s) for s in legs]


def entry_columns(horizon: int) -> list:
    """Non-overlapping entry columns for one horizon.

    A signal read at column m is entered at m+1 and exited at m+1+H, so the
    last usable m is the one whose exit still lands on or before 16:00 ET.
    """
    first = FI.minute_index(*ENTRY_FIRST)
    last = FI.minute_index(*ENTRY_LAST)
    out, m = [], first
    while m + 1 + horizon <= last:
        out.append(m)
        m += horizon
    return out


def _trailing(x: np.ndarray, k: int) -> np.ndarray:
    """Mean of the k minutes STRICTLY BEFORE each minute, along the minute axis.

    Implemented as a cumulative-sum difference shifted by one, so minute m
    never sees itself. NaNs are treated as absent rather than propagated, which
    matters because a quiet minute is information, not a hole.
    """
    v = np.nan_to_num(x, nan=0.0)
    ok = np.isfinite(x).astype(np.float32)
    cs = np.cumsum(v, axis=1)
    cn = np.cumsum(ok, axis=1)
    lo = np.concatenate([np.zeros_like(cs[:, :1]), cs[:, :-1]], axis=1)
    ln = np.concatenate([np.zeros_like(cn[:, :1]), cn[:, :-1]], axis=1)
    lo_k = np.concatenate([np.zeros_like(cs[:, :k]), cs[:, :-k]], axis=1) if k < cs.shape[1] else np.zeros_like(cs)
    ln_k = np.concatenate([np.zeros_like(cn[:, :k]), cn[:, :-k]], axis=1) if k < cn.shape[1] else np.zeros_like(cn)
    num, den = lo - lo_k, ln - ln_k
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(den > 0, num / den, np.nan)


# --------------------------------------------------------------------------- #
# The signals - one per declared family, and no others
# --------------------------------------------------------------------------- #
def raw_score(sp: dict) -> np.ndarray:
    """(n_dates x 1440 x n_legs) UNSIGNED raw signal for every minute.

    Returns the raw score rather than a weight so that the rescue's magnitude
    condition has something to threshold, and so a sign is applied in exactly
    one place. This is the lesson the OHLCV axis paid for: applying the sign
    inside the score silently made every momentum arm identical to its
    reversion twin.
    """
    pn = MS.panel()
    j = _idx(pn, sp["legs"])
    rule = sp["rule"]

    if rule == "depth":
        return pn["depth_imb"][:, :, j].astype(float)
    if rule == "micro":
        return pn["micro_bps"][:, :, j].astype(float)
    if rule == "count":
        return pn["ct_imb"][:, :, j].astype(float)
    if rule == "aggressor":
        # the sign of the minute's last trade: +1 buyer-initiated, -1 seller
        return np.sign(pn["signed"][:, :, j].astype(float))
    if rule == "flowimb":
        # signed volume over the trailing window, normalised by volume traded
        # in the same window, so it is an IMBALANCE and not a volume proxy
        k = LOOKBACK_MINUTES
        sgn = _trailing(pn["signed"][:, :, j].astype(float), k)
        vol = _trailing(pn["traded"][:, :, j].astype(float), k)
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.where(vol > 0, sgn / vol, 0.0)
    if rule == "withdrawal":
        # one-sided depth stepping away: today's queue asymmetry against its own
        # trailing baseline. A fall in bid depth relative to ask shows up as a
        # NEGATIVE change, which the declared sign then maps to a short.
        d = pn["depth_imb"][:, :, j].astype(float)
        return d - _trailing(d, LOOKBACK_MINUTES)
    if rule == "spread":
        # when the book is abnormally WIDE the mid is a poor estimate of value
        # and recent moves are disproportionately transitory. The score is the
        # recent move, gated by the spread regime; the declared sign is negative
        # (reversion), so a wide book fades the move.
        sprd = pn["spread_t"][:, :, j].astype(float)
        base = _trailing(sprd, LOOKBACK_MINUTES)
        mid = pn["mid"][:, :, j].astype(float)
        k = LOOKBACK_MINUTES
        prev = np.concatenate([np.full_like(mid[:, :k, :], np.nan), mid[:, :-k, :]], axis=1)
        with np.errstate(invalid="ignore", divide="ignore"):
            move = np.where(np.isfinite(prev) & (prev > 0), mid / prev - 1.0, np.nan)
        wide = np.isfinite(base) & (sprd > base)
        return np.where(wide, np.nan_to_num(move), 0.0)
    if rule == "transmission":
        # the SOURCE market's flow pressure, used to trade the TARGET market.
        # The source leg is read at minute m; the target is entered at m+1.
        src = pn["depth_imb"][:, :, [pn["instruments"].index(sp["source"])]].astype(float)
        return np.repeat(src, len(sp["legs"]), axis=2)
    raise ValueError("unknown rule %r" % rule)


def weights_at(sp: dict, x: np.ndarray, m: int) -> np.ndarray:
    """(n_dates x n_legs) target weights from the signal read at minute ``m``."""
    xm = x[:, m, :]
    w = float(sp["sign"]) * np.sign(xm)
    if sp.get("conditional"):
        # The rescue: engage only where the signal's own magnitude clears its
        # STRICTLY PRIOR cross-session distribution at this same minute. It
        # addresses the named measured binding failure - unaffordable turnover -
        # and touches nothing else.
        a = np.abs(xm)
        thr = (pd.DataFrame(a).shift(1)
               .rolling(RESCUE_LOOKBACK, min_periods=RESCUE_LOOKBACK // 2)
               .quantile(RESCUE_PERCENTILE / 100.0).to_numpy())
        w = np.where(np.isfinite(thr) & (a >= thr), w, 0.0)
    w = np.nan_to_num(w)
    gross = np.abs(w).sum(axis=1, keepdims=True)
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(gross > 0, w / gross, 0.0)


# --------------------------------------------------------------------------- #
# The trade-date return path
# --------------------------------------------------------------------------- #
def session_path(sp: dict, *, cost_bps: float) -> dict:
    """Per-trade-date NET return of one arm at one cost level.

    The day's return is the SUM over its non-overlapping entries, and the full
    round trip is charged on every engaged entry - so an arm that trades 390
    times a day pays 390 round trips.
    """
    pn = MS.panel()
    j = _idx(pn, sp["legs"])
    h = int(sp["horizon"])
    mid = pn["mid"][:, :, j].astype(float)
    x = raw_score(sp)

    n_d = mid.shape[0]
    gross = np.zeros(n_d)
    cost = np.zeros(n_d)
    trades = np.zeros(n_d)
    for m in entry_columns(h):
        w = weights_at(sp, x, m)
        p0, p1 = mid[:, m + 1, :], mid[:, m + 1 + h, :]
        with np.errstate(invalid="ignore", divide="ignore"):
            r = np.where(np.isfinite(p0) & (p0 > 0), p1 / p0 - 1.0, np.nan)
        # a leg whose price is missing at either end cannot be traded at all
        w = np.where(np.isfinite(r), w, 0.0)
        eng = np.abs(w).sum(axis=1)
        gross += np.nansum(w * np.nan_to_num(r), axis=1)
        cost += eng * 2.0 * float(cost_bps) * 1e-4
        trades += (eng > 0).astype(float)
    return {"net": gross - cost, "gross": gross, "cost": cost,
            "engaged": (trades > 0).astype(float), "round_trips_per_day": trades,
            "dates": pn["dates"]}


def latency_control(sp: dict) -> dict:
    """THE decisive diagnostic for this axis: how fast does the signal decay?

    Two measurements of the same signal, both at ZERO cost, differing only in
    when the position is opened:

      LAG 0  signal from the book at the end of minute m, return from the mid at
             the end of m to the mid at the end of m+1. This is a ZERO-LATENCY
             idealisation - it fills at the very mid whose book produced the
             signal - so it is NOT tradable and is never a candidate for
             capital. It bounds the information content from above.
      LAG 1  the campaign's actual rule: the same signal, entered one minute
             later, m+1 -> m+2.

    The gap between them is what the minute sampling costs. It is the
    measurement that decides whether finer data would be worth buying, and it
    can only be made because both are computed on the same panel:

      both near zero          the information is not there at minute sampling,
                              and buying mbp-1 would test a different question
                              rather than the same one more precisely
      lag 0 strong, lag 1 not the information is REAL but lives inside the
                              minute, which is a positive result about order
                              flow and a negative one about this panel - and
                              the only condition under which spending further
                              credit on mbp-1 or bbo-1s is justified
      both strong             the signal survives a minute of latency

    Reported beside the cell, never entered into the BH denominator: a
    zero-latency fill is not a hypothesis this campaign could select.
    """
    pn = MS.panel()
    j = _idx(pn, sp["legs"])
    h = int(sp["horizon"])
    mid = pn["mid"][:, :, j].astype(float)
    x = raw_score(sp)

    out = {}
    for lag in (0, 1):
        tot = np.zeros(mid.shape[0])
        for m in entry_columns(h):
            w = weights_at(sp, x, m)
            p0, p1 = mid[:, m + lag, :], mid[:, m + lag + h, :]
            with np.errstate(invalid="ignore", divide="ignore"):
                r = np.where(np.isfinite(p0) & (p0 > 0), p1 / p0 - 1.0, np.nan)
            w = np.where(np.isfinite(r), w, 0.0)
            tot += np.nansum(w * np.nan_to_num(r), axis=1)
        st = S.nw_tstat(tot[np.isfinite(tot)], 0)
        out["lag_%d" % lag] = {"ann_gross": float(np.nanmean(tot) * PPY),
                               "t_gross": st["t"],
                               "bp_per_day": float(np.nanmean(tot) * 1e4)}
    t0 = abs(out["lag_0"]["t_gross"] or 0.0)
    t1 = abs(out["lag_1"]["t_gross"] or 0.0)
    out.update({
        "is_a_control_not_a_candidate": True,
        "why_lag_0_is_not_tradable": "it fills at the very mid whose book produced the signal, "
                                     "which is a zero-latency idealisation",
        "why_not_in_the_bh_denominator": "a non-tradable fill is not a hypothesis the campaign "
                                         "could select for capital",
        "abs_t_lost_to_one_minute_of_latency": round(t0 - t1, 4),
        "share_of_signal_surviving_one_minute": (round(t1 / t0, 4) if t0 > 1e-9 else None),
        "reading": ("no information at minute sampling even with a zero-latency fill"
                    if t0 < 2.0 else
                    "the information is REAL but decays inside the minute: it survives a "
                    "zero-latency fill and not a one-minute one" if t1 < 2.0 else
                    "the signal survives a full minute of latency"),
        "justifies_finer_data_purchase": bool(t0 >= 2.0 and t1 < 2.0),
    })
    return out


@contextlib.contextmanager
def _as_cost_ladder(lad: dict):
    """Point ``intraday_alpha.gates`` at THIS cell's futures cost ladder.

    Identical to the OHLCV axis's rebinding: the gate logic is not copied, only
    the numbers it keys on are rebound, and they are restored in ``finally`` so
    an exception cannot leave the module's thresholds altered for whatever runs
    next.
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
        "PRE-REGISTERED: every family, falsifier, horizon, entry rule, split and threshold "
        "in this cell was committed in microstructure.py before the panel was built")
    n_entries = len(entry_columns(int(sp["horizon"])))
    cell["construction"] = {
        "horizon_minutes": int(sp["horizon"]),
        "entries_per_session": n_entries,
        "entry_window": "%02d:%02d-%02d:%02d ET" % (ENTRY_FIRST + ENTRY_LAST),
        "entries_overlap": False,
        "latency": "signal read at minute m, entered at the mid of m+1, exited at m+1+H",
        "day_return": "SUM over the session's entries, not the mean",
        "cost_charged": "full round trip on EVERY engaged entry (%d per day at this horizon)"
                        % n_entries,
        "gross_exposure_per_entry": 1.0,
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

    # THE decisive measurement for this axis. The brief makes the gross/net
    # distinction mandatory because the two failures mean different things: no
    # gross edge is an INFORMATION verdict, and it cannot be rescued by any cost
    # assumption; a gross edge that dies net is an EXECUTION verdict about this
    # estate's cost structure, not about the information.
    z = session_path(sp, cost_bps=0.0)
    gz = z["gross"]
    stz = S.nw_tstat(gz[np.isfinite(gz)], 0)
    rt = float(np.mean(z["round_trips_per_day"]))
    cost_at_primary = rt * 2.0 * lad["PRIMARY"]
    edge_bp_per_day = float(np.nanmean(gz) * 1e4)
    cell["gross"] = {
        "ann_gross": float(np.nanmean(gz) * PPY), "t_gross": stz["t"],
        "bp_per_day_gross": edge_bp_per_day,
        "round_trips_per_day": rt,
        "cost_bp_per_day_at_primary": cost_at_primary,
        "edge_exceeds_cost": bool(edge_bp_per_day > cost_at_primary),
        "reaches_t2_at_zero_cost": bool((stz["t"] or 0) >= 2.0),
        "verdict_if_gross_t_below_2": "NO_INFORMATION - not an execution problem",
        "verdict_if_gross_t_above_2_but_net_fails": "INFORMATION_PRESENT_BUT_UNAFFORDABLE",
    }

    cell["latency_control"] = latency_control(sp)

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
def _cell(family, name, rule, group, sign, horizon, **kw):
    sp = {"cell_id": "%s|%s" % (family, name), "family": family, "name": name,
          "rule": rule, "group": group, "legs": list(GROUPS[group]),
          "sign": sign, "horizon": int(horizon)}
    sp.update(kw)
    return sp


#: The economically declared direction of each family, fixed in advance. Sign
#: TWINS are deliberately NOT tested: a sign flip is an exact mirror, so its
#: t-statistic is the negative of the original and a two-sided test already
#: covers it. The OHLCV axis spent half its grid learning that.
DECLARED_SIGN = {
    MS.FAM_DEPTH: +1.0,        # more resting bid size -> price rises
    MS.FAM_MICRO: +1.0,        # microprice above mid -> price rises
    MS.FAM_AGGRESSOR: +1.0,    # buyer-initiated print -> price rises
    MS.FAM_FLOWIMB: +1.0,      # net buying pressure -> price rises
    MS.FAM_WITHDRAWAL: +1.0,   # bid depth stepping away -> price falls
    MS.FAM_SPREAD: -1.0,       # a wide book fades the recent move
    MS.FAM_COUNT: +1.0,        # more resting bid orders -> price rises
    MS.FAM_TRANSMISSION: +1.0,  # source-market pressure leads the target
}

RULE_OF = {
    MS.FAM_DEPTH: "depth", MS.FAM_MICRO: "micro", MS.FAM_AGGRESSOR: "aggressor",
    MS.FAM_FLOWIMB: "flowimb", MS.FAM_WITHDRAWAL: "withdrawal",
    MS.FAM_SPREAD: "spread", MS.FAM_COUNT: "count",
    MS.FAM_TRANSMISSION: "transmission",
}

#: The transmission pairs, exactly the ones the brief named plus the two
#: reverse directions that make the lead-lag claim falsifiable.
TRANSMISSION_PAIRS = (("NQ", "ES", 5), ("ES", "NQ", 5), ("GC", "ES", 5),
                      ("6E", "ES", 5), ("NQ", "ES", 30), ("ES", "NQ", 30))


def default_grid() -> list:
    """48 primaries: 6 per family, 8 families. Fixed before any measurement."""
    grid = []
    for fam in MS.FAMILIES:
        if fam == MS.FAM_TRANSMISSION:
            for src, tgt, h in TRANSMISSION_PAIRS:
                grid.append(_cell(fam, "%s_TO_%s_H%d" % (src, tgt, h), "transmission",
                                  "EQUITY" if tgt in ("ES", "NQ") else "ALL7",
                                  DECLARED_SIGN[fam], h, source=src, target=tgt,
                                  legs_override=[tgt]))
            continue
        for h in MS.HORIZONS_MINUTES:
            grid.append(_cell(fam, "ALL7_H%d" % h, RULE_OF[fam], "ALL7",
                              DECLARED_SIGN[fam], h))
        grid.append(_cell(fam, "EQUITY_H5", RULE_OF[fam], "EQUITY",
                          DECLARED_SIGN[fam], 5))
    # the transmission cells trade ONE target leg, not the whole group
    for sp in grid:
        if sp.get("legs_override"):
            sp["legs"] = list(sp.pop("legs_override"))
    return grid


# --------------------------------------------------------------------------- #
# The rescue
# --------------------------------------------------------------------------- #
#: The failure a rescue may address on this axis. It is NAMED and it is
#: MEASURED - ``rescue_grid`` refuses to act unless the measurement shows it
#: actually binds. Unlike the OHLCV axis, the binding failure here is expected
#: to be turnover rather than engagement, and that expectation is not allowed to
#: substitute for the measurement.
BINDING_FAILURE = (
    "UNAFFORDABLE_TURNOVER: the arm enters on every scheduled minute of every session, so it "
    "pays 390/H round trips a day and the cost ladder consumes a gross edge that the zero-cost "
    "measurement shows to be present. The rescue engages only on the arm's own top-conviction "
    "minutes, which is the economically correct response to that specific failure and changes "
    "no sign, horizon, leg or mark.")
#: A rescue is permitted only where the gross edge is real enough to be worth
#: saving. Without this, a rescue is just another specification.
RESCUE_MIN_GROSS_T = 1.5
#: ... and only where cost actually is what killed it.
RESCUE_MIN_COST_TO_EDGE_RATIO = 1.0
RESCUE_FAMILIES = 3


def rescue_grid(cells: list) -> list:
    """At most 2 rescues each, in the families where the binding failure BINDS.

    WHICH families are rescued is necessarily chosen after seeing the primary
    measurements - that is what "against a named MEASURED failure" means. What
    is not chosen afterwards is the rescue's form or its constants: both are
    inherited unchanged. Every rescue enters the BH denominator as a primary.
    """
    by_fam: dict = {}
    for c in cells:
        if c.get("tag") == "RESCUE" or not c.get("family"):
            continue
        by_fam.setdefault(c["family"], []).append(c)

    ranked = []
    for fam, fc in by_fam.items():
        elig = []
        for c in fc:
            g = c.get("gross") or {}
            t = g.get("t_gross")
            edge, cost = g.get("bp_per_day_gross"), g.get("cost_bp_per_day_at_primary")
            if t is None or edge is None or not cost:
                continue
            # the failure binds only if there IS a gross edge and cost exceeds it
            if abs(t) >= RESCUE_MIN_GROSS_T and cost >= RESCUE_MIN_COST_TO_EDGE_RATIO * abs(edge):
                elig.append((abs(t), c))
        if elig:
            ranked.append((max(t for t, _ in elig), fam, elig))
    ranked.sort(key=lambda x: -x[0])

    out = []
    for _t, fam, elig in ranked[:RESCUE_FAMILIES]:
        for _tt, c in sorted(elig, key=lambda p: -p[0])[:MS.MAX_RESCUES_PER_FAMILY]:
            sp = {k: v for k, v in c.items()
                  if k in ("family", "rule", "group", "legs", "sign", "horizon",
                           "source", "target")}
            sp["name"] = "%s_RESCUE_CONDITIONAL" % c["name"]
            sp["cell_id"] = "%s|%s" % (fam, sp["name"])
            sp["tag"] = "RESCUE"
            sp["conditional"] = True
            sp["binding_failure"] = BINDING_FAILURE
            sp["rescue_rule"] = ("engage only when |signal| >= its own %dth percentile over the "
                                 "%d STRICTLY PRIOR trade dates at the same minute; form and "
                                 "constants inherited, not chosen here"
                                 % (RESCUE_PERCENTILE, RESCUE_LOOKBACK))
            out.append(sp)
    return out


# --------------------------------------------------------------------------- #
# Execution
# --------------------------------------------------------------------------- #
import re      # noqa: E402  (kept beside the only function that needs it)
import time    # noqa: E402

CELLS_DIR = "_cells_microstructure"
CELL_PREFIX = "ms_"
BH_Q = MS.FROZEN_GATES["bh_q"]
HOLM_ALPHA = MS.FROZEN_GATES["family_holm_alpha"]


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
                    print("[%2d/%d] %-44s (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
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
            g = cell.get("gross") or {}
            print("[%2d/%d] %-44s %-14s ann=%9s t=%6s gross_t=%6s grossbp/d=%8s cost bp/d=%8s %.0fs"
                  % (i, len(grid), sp["cell_id"], cell.get("verdict"),
                     _r(a.get("ann_net"), 3), _r(a.get("t_net"), 2), _r(g.get("t_gross"), 2),
                     _r(g.get("bp_per_day_gross"), 2), _r(g.get("cost_bp_per_day_at_primary"), 2),
                     cell["seconds"]), flush=True)
    return cells


def brief(c: dict) -> dict:
    a, st = _prim(c), _stress(c)
    g = c.get("gross") or {}
    cap = c.get("capital_applicability") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "name": c.get("name"),
            "group": c.get("group"), "markets": c.get("markets"),
            "horizon_minutes": c.get("horizon"), "verdict": c.get("verdict"),
            "ann_net": a.get("ann_net"), "t_net": a.get("t_net"), "sharpe": a.get("sharpe"),
            "max_dd": a.get("max_dd"), "hit_rate": a.get("hit_rate"),
            "ann_net_at_stress_cost": st.get("ann_net"),
            "ann_gross": g.get("ann_gross"), "t_gross": g.get("t_gross"),
            "bp_per_day_gross": g.get("bp_per_day_gross"),
            "cost_bp_per_day_at_primary": g.get("cost_bp_per_day_at_primary"),
            "round_trips_per_day": g.get("round_trips_per_day"),
            "edge_exceeds_cost": g.get("edge_exceeds_cost"),
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
    for f in MS.FAMILIES:
        fc = [c for c in cells if c.get("family") == f]
        if not fc:
            continue
        prim = [c for c in fc if c.get("tag") != "RESCUE"]
        resc = [c for c in fc if c.get("tag") == "RESCUE"]
        ts = [abs((c.get("gross") or {}).get("t_gross") or 0.0) for c in fc]
        best_t = max(ts, default=None)
        any_q = any(c.get("verdict") == T.V_MATERIAL for c in fc)
        fam_summary[f] = {
            "claim": MS.FAMILY_CONTRACT[f]["claim"],
            "not_in_ohlcv": MS.FAMILY_CONTRACT[f]["not_in_ohlcv"],
            "falsified_by": MS.FAMILY_CONTRACT[f]["falsified_by"],
            "n_primary": len(prim), "n_rescue": len(resc),
            "budget_primary_max": MS.MAX_PRIMARY_PER_FAMILY,
            "budget_rescue_max": MS.MAX_RESCUES_PER_FAMILY,
            "budget_respected": (len(prim) <= MS.MAX_PRIMARY_PER_FAMILY
                                 and len(resc) <= MS.MAX_RESCUES_PER_FAMILY),
            "verdicts": sorted({str(c.get("verdict")) for c in fc}),
            "best_ann_net": max([_prim(c).get("ann_net") or -9 for c in fc], default=None),
            "best_abs_gross_t": best_t,
            "best_gross_bp_per_day": max([abs((c.get("gross") or {}).get("bp_per_day_gross") or 0.0)
                                          for c in fc], default=None),
            "any_qualified": any_q,
            "closed": True,
            "closed_because": ("a candidate qualified" if any_q
                               else "the declared falsifier fired: best |gross t| %.2f < 2.0 at "
                                    "ZERO cost, so the failure is INFORMATION, not execution"
                                    % (best_t or 0.0) if (best_t or 0) < 2.0
                               else "a gross edge was present but no arm cleared the frozen gates "
                                    "after costs"),
            "failure_kind": ("NONE" if any_q
                             else "NO_INFORMATION" if (best_t or 0) < 2.0
                             else "INFORMATION_PRESENT_BUT_UNAFFORDABLE"),
        }

    # The two families whose falsifier is a COMPARISON rather than a threshold.
    fam_summary = _comparison_falsifiers(cells, fam_summary)

    # The axis-level latency verdict, aggregated across every arm. This is the
    # single number that decides whether ANY further order-flow purchase is
    # justified, so it is computed rather than argued.
    lc = [c.get("latency_control") or {} for c in cells]
    t0 = [abs((x.get("lag_0") or {}).get("t_gross") or 0.0) for x in lc if x.get("lag_0")]
    t1 = [abs((x.get("lag_1") or {}).get("t_gross") or 0.0) for x in lc if x.get("lag_1")]
    justify = [c["cell_id"] for c in cells
               if (c.get("latency_control") or {}).get("justifies_finer_data_purchase")]
    latency = None
    if t0:
        latency = {
            "question": "is there information in the book at minute sampling, and does any of it "
                        "survive one minute of latency?",
            "n_arms": len(t0),
            "max_abs_t_at_zero_latency": max(t0),
            "max_abs_t_at_one_minute_latency": max(t1) if t1 else None,
            "median_abs_t_at_zero_latency": float(np.median(t0)),
            "arms_reaching_t2_at_zero_latency": int(sum(1 for v in t0 if v >= 2.0)),
            "arms_reaching_t2_at_one_minute_latency": int(sum(1 for v in t1 if v >= 2.0)),
            "arms_that_would_justify_finer_data": justify,
            "finer_data_purchase_justified": bool(justify),
            "what_a_null_at_zero_latency_means": (
                "the book state sampled once a minute carries no directional information even "
                "when filled at the very mid that produced it. Buying mbp-1 or bbo-1s would then "
                "be testing a DIFFERENT question - sub-minute flow - not the same one more "
                "precisely, and this axis is not evidence for or against it."),
            "what_a_null_at_one_minute_but_not_zero_means": (
                "the information is real and decays inside the minute. That is the ONLY condition "
                "under which further order-flow credit is justified, and it would name exactly "
                "what to buy."),
        }
    body_latency = latency

    best = None
    for c in scored:
        ann = _prim(c).get("ann_net")
        if ann is None:
            continue
        if best is None or ann > _prim(best).get("ann_net", -9):
            best = c
    qualified = [brief(c) for c in cells if c.get("verdict") == T.V_MATERIAL]
    pn = MS.panel()
    best_gross = max(cells, key=lambda c: abs((c.get("gross") or {}).get("t_gross") or -9)) if cells else None

    body = {
        "schema": "alpha_recovery_microstructure_alpha/1", "calculation_owner": CALCULATION_OWNER,
        "question": "can information that does NOT exist in an OHLCV bar - resting depth, queue "
                    "asymmetry, order counts and trade aggressor side - predict short-horizon "
                    "returns strongly enough to produce a capital-eligible strategy?",
        "axis": "NATIVE_CME_FUTURES_MICROSTRUCTURE",
        "panel": {"trade_dates": len(pn["dates"]), "first": pn["dates"][0], "last": pn["dates"][-1],
                  "roots": pn["instruments"],
                  "markets": sorted({FI.MARKET[r] for r in pn["instruments"]}),
                  "buckets": sorted({FI.UNIVERSE[r][1] for r in pn["instruments"]}),
                  "schema": MS.SCHEMA,
                  "fields": list(MS.FIELDS),
                  "roll": "inherited from the OHLCV panel, so the two are contract-identical"},
        "horizons_minutes": list(MS.HORIZONS_MINUTES),
        "evidence_split": {"selection_share": SELECTION_SHARE,
                           "selection_dates": int(round(SELECTION_SHARE * len(pn["dates"]))),
                           "holdout_dates": len(pn["dates"]) - int(round(SELECTION_SHARE * len(pn["dates"]))),
                           "declared_before_any_arm_ran": True},
        "n_cells": len(cells), "counts": counts, "families": fam_summary,
        "cost_ladder": {"form": "per_side_bps = 10000*(ticks*tick_size + commission_usd/multiplier)/price",
                        "levels": FI.COST_LADDER,
                        "eligibility_requires_surviving": FI.COST_ELIGIBILITY_LEVEL,
                        "per_root_round_trip_bp_at_primary":
                            {r: round(FI.round_trip_bps(r, FI.representative_price(r), "PRIMARY"), 4)
                             for r in pn["instruments"]},
                        "charged_per_cell": "the MOST EXPENSIVE leg's rate, on every leg",
                        "charged_per_entry": "a full round trip on every engaged entry, so an arm "
                                             "at horizon H pays 390/H round trips a day"},
        "frozen_gates": MS.FROZEN_GATES,
        "gates_unchanged_by_acquiring_data": True,
        "multiple_testing": {"benjamini_hochberg": {k: v for k, v in bh.items() if k != "per_test"},
                             "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"}
                                                for f, h in holm.items()},
                             "denominator": len(p_one)},
        "qualified_for_true_forward": qualified,
        "true_forward_ready": bool(qualified),
        "best_by_ann_net": brief(best) if best else None,
        "best_by_gross_t": brief(best_gross) if best_gross else None,
        "latency_decay": body_latency,
        "information_not_purchased": dict(MS.INFORMATION_NOT_PURCHASED),
        "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
        "safety": {"paper_only": True, "research_only": True, "creates_orders": False,
                   "registers_forward_challenger": False, "automatic_promotion": False,
                   "live_checkout_read_only": True},
    }
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


def _comparison_falsifiers(cells: list, fam_summary: dict) -> dict:
    """Two families are falsified by a COMPARISON, not by a threshold, and the
    comparison is made here rather than asserted in prose.

    MS_MICROPRICE_PRESSURE     must beat MS_DEPTH_IMBALANCE at the same horizon,
                               or its spread-scaling carries nothing (and the two
                               are algebraically the same signal otherwise).
    MS_ORDER_COUNT_IMBALANCE   must beat MS_DEPTH_IMBALANCE, or counts are a
                               proxy for size rather than information.
    MS_FLOW_IMBALANCE          must beat MS_AGGRESSOR_FLOW, or rolling adds
                               nothing to the instantaneous sign.
    MS_CROSS_MARKET_TRANSMISSION must show a cross-market edge larger than the
                               target's own-book edge at the same horizon.
    """
    def by_h(fam):
        out = {}
        for c in cells:
            if c.get("family") != fam or c.get("tag") == "RESCUE":
                continue
            if c.get("group") != "ALL7":
                continue
            out[int(c.get("horizon") or 0)] = abs((c.get("gross") or {}).get("t_gross") or 0.0)
        return out

    base_depth = by_h(MS.FAM_DEPTH)
    base_aggr = by_h(MS.FAM_AGGRESSOR)
    pairs = ((MS.FAM_MICRO, MS.FAM_DEPTH, base_depth),
             (MS.FAM_COUNT, MS.FAM_DEPTH, base_depth),
             (MS.FAM_FLOWIMB, MS.FAM_AGGRESSOR, base_aggr))
    for fam, ref_fam, ref in pairs:
        if fam not in fam_summary or not ref:
            continue
        mine = by_h(fam)
        wins = {h: round(mine[h] - ref.get(h, 0.0), 3) for h in sorted(mine) if h in ref}
        n_better = sum(1 for v in wins.values() if v > 0)
        fam_summary[fam]["comparison_falsifier"] = {
            "must_beat": ref_fam,
            "abs_gross_t_advantage_by_horizon": wins,
            "horizons_where_it_wins": n_better,
            "horizons_compared": len(wins),
            "falsified": n_better <= len(wins) / 2.0,
            "reading": ("it does not beat %s on a majority of horizons, so the family adds no "
                        "information beyond the one it is built from" % ref_fam)
                       if n_better <= len(wins) / 2.0 else
                       "it beats %s on a majority of horizons" % ref_fam,
        }
    return fam_summary


if __name__ == "__main__":                                   # pragma: no cover
    run_grid()
    print(json.dumps({k: v for k, v in merge().items() if k not in ("brief", "cells")},
                     indent=1, default=str))
