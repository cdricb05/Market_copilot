r"""alpha_agent.alpha_recovery.russell_reconstitution_flow - executor for the Alpha Agent mechanism
``INDEX_MEMBERSHIP_FORCED_FLOW_V1``.

Contract: ``research/preregistration/INDEX_MEMBERSHIP_FORCED_FLOW_PREREGISTRATION.md`` (committed
b45f679 before any event-window return existed). One frozen cell: at each annual Russell 2000
reconstitution, identified ONLY from the point-in-time membership flags (the June 15..July 15 session
with the most Russell 2000 flag changes, at least 100), hold equal-weight LONG the securities that left the
index and equal-weight SHORT the securities that joined it, dollar-neutral, entered at the open of the
session AFTER the effective session and held through the close of the 20th session, at 25 bp per side.
No announcement date is used or inferred: the earliest trade is after the effective membership state is
observable.

Membership rule (the census trap): a change counts only if the security has a quote within 5 calendar days
before the effective session and its first quote after it is the next market session with a finite open.

Existing owners only: ``alpha_agent.r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd``.
RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import BH_Q, MATERIALITY_ANN_NET, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.russell_reconstitution_flow"
MECHANISM_ID = "INDEX_MEMBERSHIP_FORCED_FLOW_V1"
ARTIFACT_NAME = "index_membership_forced_flow.json"
PREREGISTRATION = "research/preregistration/INDEX_MEMBERSHIP_FORCED_FLOW_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "b45f679"
SCORER = "alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd"

KILL_RULE_FROZEN = (
    "Kill if the dollar-neutral Russell 2000 reconstitution book (long tradeable deletions, short tradeable "
    "additions, entered at the open of the session after the effective session, held 20 sessions) has mean "
    "event net <= 0, active-session NW t < 2.0 or mean event net below 1.5 %/yr at 25 bp per side; or its "
    "increment over the Russell 2000 and S&P 500 total-return indices has t < 2.0 or annualised alpha below "
    "1.5 %/yr; or mean event net is non-positive in either qualification half; or its maximum drawdown is "
    "worse than -20 %; or it fails BH q=0.10 at m=3 with S14-B1 and S14-B2 inherited at p=1; or mean event net "
    "at 50 bp per side is below 1.5 %/yr; or the untouched 2013-2026 confirmation has mean event net <= 0, "
    "NW t < 2.0 or mean event net below 1.5 %/yr.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
WATCHLIST = "Russell 2000 Current & Past"                                          # s1
INDEX = "Russell 2000"
DIAGNOSTIC_INDEX = "Russell 1000"
CALENDAR_SYMBOL = "$SPXTR"
CONTROL_SYMBOLS = ("$RUTTR", "$SPXTR")
EVENT_WINDOW = ((6, 15), (7, 15))                                                  # s2
MIN_CHANGES = 100
YEARS = tuple(range(1991, 2027))
STALE_DAYS = 5
HOLD = 20                                                                          # s3
COST_PRIMARY_BPS = 25.0                                                            # s5
COST_LADDER_BPS = (0.0, 12.5, 25.0, 50.0)
COST_STRESS_BPS = 50.0
NW_LAG = 5                                                                         # s6
T_FLOOR = float(STANDALONE_T_FLOOR)
QUALIFICATION = (1991, 2012)                                                       # s4
HALVES = {"H1_1991_2001": (1991, 2001), "H2_2002_2012": (2002, 2012)}
CONFIRMATION = (2013, 2026)
MAX_LOAD_FAIL_SHARE = 0.01                                                         # s7 gate 1
MIN_LEG = 50
MIN_QUALIFICATION_EVENTS, MIN_CONFIRMATION_EVENTS = 20, 12
MIN_CONTROL_FINITE = 0.99
DRAWDOWN_FLOOR = -0.20
INHERITED_NULLS = ("S14-B1", "S14-B2")


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _norm_index(obj):
    obj = obj.copy()
    obj.index = pd.DatetimeIndex(obj.index).normalize()
    return obj.sort_index()


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_inputs() -> dict:
    """Pass 1: membership flags of every watchlist security. Pass 2: total-return opens and closes of the
    securities whose flag changed on an event session, plus the calendar and control indices."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd
    kw = dict(padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    tr = nd.StockPriceAdjustmentType.TOTALRETURN
    cal = _norm_index(nd.price_timeseries(CALENDAR_SYMBOL, stock_price_adjustment_setting=tr, **kw))
    sessions = pd.DatetimeIndex(cal.index)
    controls = pd.DataFrame({s: _norm_index(nd.price_timeseries(s, stock_price_adjustment_setting=tr, **kw))["Close"]
                             .astype(float) for s in CONTROL_SYMBOLS}).reindex(sessions)
    symbols = list(nd.watchlist_symbols(WATCHLIST))
    flags, failed = {}, []
    for sym in symbols:
        try:
            flags[sym] = _norm_index(nd.index_constituent_timeseries(sym, INDEX, **kw).iloc[:, 0].astype(float))
        except Exception:                                        # noqa: BLE001 - counted by the data gate
            failed.append(sym)
    events = find_events(flags, sessions)
    need = set()
    for E in events.values():
        need.update(s for s, _side in flag_changes_on(flags, E))
    prices, diag = {}, {}
    for sym in sorted(need):
        try:
            px = _norm_index(nd.price_timeseries(sym, stock_price_adjustment_setting=tr, **kw))
            prices[sym] = px[["Open", "Close"]].astype(float)
            diag[sym] = _norm_index(nd.index_constituent_timeseries(sym, DIAGNOSTIC_INDEX, **kw).iloc[:, 0].astype(float))
        except Exception:                                        # noqa: BLE001
            failed.append(sym)
    return {"flags": flags, "prices": prices, "r1000": diag, "sessions": sessions, "controls": controls,
            "n_symbols": len(symbols), "n_failed": len(failed), "failed": failed[:50]}


# --------------------------------------------------------------------------- #
# Events and members (s2)
# --------------------------------------------------------------------------- #
def flag_changes_on(flags: dict, E: pd.Timestamp) -> list:
    """(symbol, 'ADD'|'DEL') for every security whose flag on session E differs from its previous flag row."""
    out = []
    for sym, f in flags.items():
        idx = f.index
        j = int(idx.searchsorted(E, side="left"))
        if j <= 0 or j >= len(idx) or idx[j] != E:
            continue
        a, b = float(f.iloc[j - 1]), float(f.iloc[j])
        if a != b:
            out.append((sym, "ADD" if b > a else "DEL"))
    return out


def find_events(flags: dict, sessions) -> dict:
    """{year: effective session} - the in-window session with the most flag changes, at least MIN_CHANGES."""
    counts: dict = {}
    for f in flags.values():
        v = f.to_numpy(dtype=float)
        if len(v) < 2:
            continue
        ch = np.flatnonzero(v[1:] != v[:-1]) + 1
        for d in f.index[ch]:
            if (d.month, d.day) >= EVENT_WINDOW[0] and (d.month, d.day) <= EVENT_WINDOW[1]:
                counts[d] = counts.get(d, 0) + 1
    sess = set(pd.DatetimeIndex(sessions))
    events = {}
    for y in YEARS:
        cands = [(n, d) for d, n in counts.items() if d.year == y and d in sess]
        if cands:
            n, d = max(cands)
            if n >= MIN_CHANGES:
                events[y] = d
    return events


def members(E: pd.Timestamp, flags: dict, prices: dict, sessions) -> dict:
    """Tradeable additions and deletions of one event under the frozen quote rule."""
    sess = pd.DatetimeIndex(sessions)
    ie = int(sess.searchsorted(E, side="left"))
    e1 = sess[ie + 1] if ie + 1 < len(sess) else None
    adds, dels, dropped = [], [], 0
    for sym, side in flag_changes_on(flags, E):
        px = prices.get(sym)
        if px is None or e1 is None:
            dropped += 1
            continue
        q = px.index
        before = q[q < E]
        after = q[q > E]
        if (not len(before) or (E - before[-1]).days > STALE_DAYS or not len(after) or after[0] != e1
                or not np.isfinite(px.loc[e1, "Open"]) or not np.isfinite(px.loc[e1, "Close"])
                or px.loc[e1, "Open"] <= 0.0):
            dropped += 1
            continue
        (adds if side == "ADD" else dels).append(sym)
    return {"E": E, "E_plus_1": e1, "additions": sorted(adds), "deletions": sorted(dels), "dropped": dropped}


# --------------------------------------------------------------------------- #
# The frozen book (s3)
# --------------------------------------------------------------------------- #
def name_growth(px: pd.DataFrame, hold_sessions) -> np.ndarray:
    """Cumulative growth G_1..G_HOLD of one name: open-to-close on the first session, close-to-close on each
    later quoted session, flat on an unquoted session and after the last quote."""
    g = np.ones(len(hold_sessions))
    level, last_close = 1.0, None
    for k, d in enumerate(hold_sessions):
        if d in px.index and np.isfinite(px.at[d, "Close"]):
            c = float(px.at[d, "Close"])
            if k == 0:
                level *= c / float(px.at[d, "Open"])
            elif last_close is not None and last_close > 0.0:
                level *= c / last_close
            last_close = c
        g[k] = level
    return g


def event_book(mem: dict, prices: dict, sessions, cost_bps: float, *, growth: Optional[dict] = None) -> dict:
    sess = pd.DatetimeIndex(sessions)
    ie = int(sess.searchsorted(mem["E"], side="left"))
    hold = sess[ie + 1: ie + 1 + HOLD]
    if len(hold) < HOLD or not mem["additions"] or not mem["deletions"]:
        return {"complete": False, "sessions": hold}
    growth = growth if growth is not None else {}
    for sym in mem["additions"] + mem["deletions"]:
        if sym not in growth:
            growth[sym] = name_growth(prices[sym], hold)
    L = np.mean([growth[s] for s in mem["deletions"]], axis=0)           # long deletions
    Sh = np.mean([growth[s] for s in mem["additions"]], axis=0)           # short additions
    B = 1.0 + L - Sh
    c = float(cost_bps) * 1e-4
    c_in, c_out = 2.0 * c, c * (L[-1] + Sh[-1])
    N = np.r_[1.0, B - c_in]
    N[-1] -= c_out
    net = N[1:] / N[:-1] - 1.0
    gross = np.r_[1.0, B][1:] / np.r_[1.0, B][:-1] - 1.0
    return {"complete": True, "sessions": hold, "gross": gross, "net": net, "event_net": float(N[-1] - 1.0),
            "event_gross": float(B[-1] - 1.0), "long_leg": float(L[-1] - 1.0), "short_leg": float(Sh[-1] - 1.0),
            "turnover_nav": float(2.0 + L[-1] + Sh[-1])}


# --------------------------------------------------------------------------- #
# Evaluation (s6, s7)
# --------------------------------------------------------------------------- #
def stats(events: list) -> dict:
    """Event-level and active-session statistics of a list of complete event books."""
    ev = np.array([e["event_net"] for e in events], dtype=float)
    if len(ev) < 3:
        return {"events": int(len(ev)), "mean_event_net": None, "t_nw": None, "p_one_sided": None,
                "max_dd": None, "event_t": None}
    daily = np.concatenate([e["net"] for e in events])
    st = S.nw_tstat(daily, NW_LAG)
    sd = float(np.std(ev, ddof=1))
    return {"events": int(len(ev)), "active_sessions": int(len(daily)), "mean_event_net": float(ev.mean()),
            "ann_net": float(ev.mean()), "t_nw": _f(st["t"]), "p_one_sided": _f(st["p_one_sided"]),
            "max_dd": _f(S._max_dd(daily)), "event_t": float(ev.mean() / sd * math.sqrt(len(ev))) if sd > 0 else None,
            "event_sharpe_per_year": float(ev.mean() / sd) if sd > 0 else None, "hit_rate": float((ev > 0).mean())}


def increment(events: list, controls: pd.DataFrame) -> dict:
    rets = controls.pct_change()
    y, X = [], []
    for e in events:
        c = rets.reindex(e["sessions"]).to_numpy(dtype=float)
        y.append(e["net"])
        X.append(c)
    y, X = np.concatenate(y), np.vstack(X)
    ok = np.isfinite(y) & np.all(np.isfinite(X), axis=1)
    y, X = y[ok], X[ok]
    if len(y) < 10:
        return {"sessions": int(len(y)), "t": None, "ann": None}
    A = np.column_stack([np.ones(len(y)), X])
    coef = np.linalg.lstsq(A, y, rcond=None)[0]
    a = y - X @ coef[1:]
    st = S.nw_tstat(a, NW_LAG)
    return {"sessions": int(len(y)), "betas": {s: float(b) for s, b in zip(controls.columns, coef[1:])},
            "t": _f(st["t"]), "ann": float(a.mean() * HOLD),
            "definition": "a = net - b1 x $RUTTR - b2 x $SPXTR (OLS on active sessions); nw_tstat(a, %d); "
                          "annualised = mean(a) x %d sessions per event per year" % (NW_LAG, HOLD)}


def multiplicity(p: Optional[float]) -> dict:
    pv = {MECHANISM_ID: 1.0 if _f(p) is None else float(p)}
    pv.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS), "p_one_sided": _f(p),
            "passes": bool(bh["per_test"].get(MECHANISM_ID)), "single_survivor_threshold": BH_Q / bh["m"]}


def _in(years, span) -> list:
    return [y for y in years if span[0] <= y <= span[1]]


def evaluate(inputs: dict) -> dict:
    flags, prices, sessions = inputs["flags"], inputs["prices"], pd.DatetimeIndex(inputs["sessions"])
    out: dict = {"qualification_events": QUALIFICATION, "confirmation_events": CONFIRMATION, "halves": HALVES,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"},
                 "untouched_confirmation": "NOT_READ"}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "kill_rule_fired": gate, "why": why})
        return out

    events = find_events(flags, sessions)
    mems = {y: members(E, flags, prices, sessions) for y, E in events.items()}
    problems = []
    n_sym, n_fail = int(inputs.get("n_symbols") or 0), int(inputs.get("n_failed") or 0)
    if not n_sym or n_fail / n_sym > MAX_LOAD_FAIL_SHARE:
        problems.append("LOAD_FAILURES_%d_OF_%d" % (n_fail, n_sym))
    missing = [y for y in YEARS if y not in events]
    if missing:
        problems.append("NO_EVENT_IN_%s" % missing[:10])
    thin = [y for y, m in mems.items() if len(m["additions"]) < MIN_LEG or len(m["deletions"]) < MIN_LEG]
    if thin:
        problems.append("THIN_LEGS_IN_%s" % thin[:10])
    qy, cy = _in(events, QUALIFICATION), _in(events, CONFIRMATION)
    if len(qy) < MIN_QUALIFICATION_EVENTS:
        problems.append("QUALIFICATION_EVENTS_%d_BELOW_%d" % (len(qy), MIN_QUALIFICATION_EVENTS))
    if len(cy) < MIN_CONFIRMATION_EVENTS:
        problems.append("CONFIRMATION_EVENTS_%d_BELOW_%d" % (len(cy), MIN_CONFIRMATION_EVENTS))
    hold_sessions = []
    for m in mems.values():
        ie = int(sessions.searchsorted(m["E"], side="left"))
        hold_sessions.extend(sessions[ie + 1: ie + 1 + HOLD])
    ctrl = inputs["controls"].pct_change().reindex(pd.DatetimeIndex(hold_sessions)).to_numpy(dtype=float)
    finite = float(np.all(np.isfinite(ctrl), axis=1).mean()) if len(ctrl) else 0.0
    if finite < MIN_CONTROL_FINITE:
        problems.append("CONTROL_FINITE_SHARE_%.4f" % finite)
    out["data"] = {"n_symbols": n_sym, "n_failed": n_fail,
                   "events": {int(y): {"E": str(m["E"].date()), "E_plus_1": str(m["E_plus_1"].date()) if m["E_plus_1"] is not None else None,
                                       "additions": len(m["additions"]), "deletions": len(m["deletions"]),
                                       "dropped": m["dropped"]} for y, m in sorted(mems.items())},
                   "control_finite_share": finite, "problems": problems}
    if problems:
        return done("DATA_HOLD", "DATA", "DATA gate: %s" % "; ".join(problems[:8]))

    growth: dict = {}
    books_q = {k: [event_book(mems[y], prices, sessions, k, growth=growth) for y in qy] for k in COST_LADDER_BPS}
    if not all(b["complete"] for b in books_q[COST_PRIMARY_BPS]):
        return done("DATA_HOLD", "DATA", "an event window is incomplete")
    by_cost = {"%.1f" % k: stats(v) for k, v in books_q.items()}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    qb = books_q[COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["qualification_event_rows"] = [[int(y), b["event_gross"], b["event_net"], b["long_leg"], b["short_leg"],
                                        len(mems[y]["deletions"]), len(mems[y]["additions"])] for y, b in zip(qy, qb)]
    out["diagnostics_qualification"] = {
        "mean_turnover_nav_per_event": float(np.mean([b["turnover_nav"] for b in qb])),
        "mean_long_leg_deletions": float(np.mean([b["long_leg"] for b in qb])),
        "mean_short_leg_additions": float(np.mean([b["short_leg"] for b in qb])),
        "deletions_to_r1000_share": _migrant_share(mems, qy, inputs.get("r1000") or {}, "DEL"),
        "additions_from_r1000_share": _migrant_share(mems, qy, inputs.get("r1000") or {}, "ADD")}
    if prim.get("mean_event_net") is None or prim["mean_event_net"] <= 0.0:
        return done("NO_EDGE", "WRONG_SIGN", "mean event net %s at %.1f bp is not in the frozen direction"
                    % (prim.get("mean_event_net"), COST_PRIMARY_BPS))
    if prim.get("t_nw") is None or prim["t_nw"] < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "active-session NW t %s < %.1f" % (prim.get("t_nw"), T_FLOOR))
    if prim["mean_event_net"] < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "MATERIALITY", "mean event net %.4f < %.3f" % (prim["mean_event_net"], MATERIALITY_ANN_NET))
    inc = increment(qb, inputs["controls"])
    out["increment_over_index_returns"] = inc
    if inc.get("t") is None or inc["t"] < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "INCREMENT", "increment t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {k: stats([b for y, b in zip(qy, qb) if lo <= y <= hi]).get("mean_event_net") for k, (lo, hi) in HALVES.items()}
    out["stability_mean_event_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("NO_EDGE", "STABILITY", "non-positive mean event net in %s" % ", ".join(bad))
    if (prim.get("max_dd") or 0.0) < DRAWDOWN_FLOOR:
        return done("NO_EDGE", "DRAWDOWN", "max drawdown %s worse than %.2f" % (prim.get("max_dd"), DRAWDOWN_FLOOR))
    mult = multiplicity(prim.get("p_one_sided"))
    out["multiplicity"] = mult
    if not mult["passes"]:
        return done("NO_EDGE", "MULTIPLICITY", "fails BH q=%.2f at m=%d (p %s)" % (BH_Q, mult["m"], mult["p_one_sided"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("mean_event_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "COST", "mean event net at %.0f bp %s" % (COST_STRESS_BPS, stress.get("mean_event_net")))
    cb = [event_book(mems[y], prices, sessions, COST_PRIMARY_BPS, growth=growth) for y in cy]
    cs = stats([b for b in cb if b["complete"]])
    out["confirmation"] = {"state": "READ", "stats": cs,
                           "event_rows": [[int(y), b.get("event_gross"), b.get("event_net")] for y, b in zip(cy, cb)]}
    good = (cs.get("mean_event_net") is not None and cs["mean_event_net"] > 0.0 and cs.get("t_nw") is not None
            and cs["t_nw"] >= T_FLOOR and cs["mean_event_net"] >= MATERIALITY_ANN_NET)
    out["untouched_confirmation"] = "CONFIRMED" if good else "FAILED"
    if not good:
        return done("NO_EDGE", "CONFIRMATION", "untouched confirmation mean %s t %s did not reproduce"
                    % (cs.get("mean_event_net"), cs.get("t_nw")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched confirmation; a "
                                   "HUMAN gate governs prospective registration")


def _migrant_share(mems: dict, years: list, r1000: dict, side: str) -> Optional[float]:
    n = k = 0
    for y in years:
        m = mems[y]
        names = m["deletions"] if side == "DEL" else m["additions"]
        for s in names:
            f = r1000.get(s)
            if f is None or not len(f):
                continue
            j = int(f.index.searchsorted(m["E"], side="left"))
            flag = float(f.iloc[min(j, len(f) - 1)]) if side == "DEL" else float(f.iloc[max(j - 1, 0)])
            n += 1
            k += int(flag == 1.0)
    return k / n if n else None


def design() -> dict:
    return {"watchlist": WATCHLIST, "index": INDEX, "calendar": CALENDAR_SYMBOL, "controls": list(CONTROL_SYMBOLS),
            "event_session": "the %s..%s session with the most %s flag changes, at least %d" % (
                EVENT_WINDOW[0], EVENT_WINDOW[1], INDEX, MIN_CHANGES),
            "tradeable": "a quote within %d calendar days before E and the first quote after E on E+1 with a finite "
                         "open" % STALE_DAYS,
            "long": "deletions", "short": "additions", "weights": "equal within each leg, dollar-neutral",
            "entry": "total-return open of E+1", "exit": "total-return close of E+%d" % HOLD,
            "cost_primary_bps_per_side": COST_PRIMARY_BPS, "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "cost_stress_bps_per_side": COST_STRESS_BPS, "nw_lag": NW_LAG, "qualification": QUALIFICATION,
            "halves": HALVES, "confirmation": CONFIRMATION, "drawdown_floor": DRAWDOWN_FLOOR,
            "multiplicity": {"q": BH_Q, "inherited_nulls_at_p1": list(INHERITED_NULLS)}}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None) -> dict:
    inputs = load_inputs() if inputs is None else inputs
    res = evaluate(inputs)
    sessions = pd.DatetimeIndex(inputs["sessions"])
    ident = "NORGATE:%s(%s flags; TOTALRETURN open/close of %d changed securities):%d securities (%d failed);%s;%s..%s" % (
        WATCHLIST, INDEX, len(inputs.get("prices") or {}), int(inputs.get("n_symbols") or 0),
        int(inputs.get("n_failed") or 0), ",".join(CONTROL_SYMBOLS),
        str(sessions.min().date()) if len(sessions) else None, str(sessions.max().date()) if len(sessions) else None)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "preregistration_commit": PREREGISTRATION_COMMIT, "kill_rule_frozen": KILL_RULE_FROZEN,
            "design": design(), "input_data_identity": ident, "result": res, "scorer": SCORER,
            "capital_eligible": False}
    if write:
        body["artifact_path"] = str(write_artifact(ARTIFACT_NAME, body))
    if verbose:
        print("%s -> %s (%s)" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def executor_result(body: dict) -> dict:
    res = body["result"]
    by = res.get("qualification_by_cost_bps_per_side") or {}
    prim, stress = by.get("%.1f" % COST_PRIMARY_BPS) or {}, by.get("%.1f" % COST_STRESS_BPS) or {}
    inc = res.get("increment_over_index_returns") or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("kill_rule_fired"),
            "statistic": {"lockbox_t": prim.get("t_nw"), "lockbox_window": "QUALIFICATION events %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_one_sided"), "events": prim.get("events"),
                          "event_t": prim.get("event_t"), "increment_t": inc.get("t"),
                          "untouched_confirmation": res.get("untouched_confirmation"),
                          "confirmation": {k: v for k, v in (res.get("confirmation") or {}).items() if k != "event_rows"}},
            "economics": {"mean_event_net_25bp": prim.get("mean_event_net"), "max_dd": prim.get("max_dd"),
                          "mean_event_net_50bp": stress.get("mean_event_net"), "increment_ann": inc.get("ann"),
                          "stability_mean_event_net": res.get("stability_mean_event_net"),
                          "qualification_by_cost_bps_per_side": by, "diagnostics": res.get("diagnostics_qualification")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body.get("input_data_identity")}


def run_mechanism(*, mechanism: dict) -> dict:
    """The R59 mechanism-executor contract. Refuses another mechanism or a changed kill rule."""
    mid = (mechanism or {}).get("mechanism_id")
    if mid != MECHANISM_ID:
        raise ValueError("executor for %s was handed %r" % (MECHANISM_ID, mid))
    if ((mechanism or {}).get("pnl_gate") or {}).get("KILL_RULE") != KILL_RULE_FROZEN:
        raise ValueError("the catalog KILL_RULE differs from the preregistered rule; an executor does not "
                         "judge a changed contract")
    try:
        body = run(verbose=True, write=True)
    except (OSError, KeyError, ValueError, ImportError) as exc:
        return {"verdict": "DATA_HOLD", "why": "an owned input is unavailable or unreadable: %r" % (exc,),
                "kill_rule_fired": "DATA", "statistic": {}, "economics": {},
                "multiplicity": {"m": 1 + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
                "artifact": None, "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    return executor_result(body)


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "find_events", "members", "name_growth", "event_book", "evaluate",
           "run", "run_mechanism", "multiplicity", "increment", "load_inputs"]
