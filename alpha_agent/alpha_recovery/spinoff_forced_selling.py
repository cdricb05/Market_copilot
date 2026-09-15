r"""alpha_agent.alpha_recovery.spinoff_forced_selling - executor for the Alpha Agent mechanism
``SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1``.

Contract: ``research/preregistration/SPINOFF_PARENT_HOLDER_FORCED_SELLING_PREREGISTRATION.md`` (committed
029230e before any spinco return existed). One frozen cell: every Form 10-12B registrant whose own filings
describe a pro rata distribution of its shares, linked to exactly one Norgate security, is bought at the
open of its 21st quoted session - after the when-issued market and the first weeks of selling by parent
holders who cannot or will not hold it - and held through the close of its 146th quoted session (126
sessions), in a 2 % NAV slot hedged one-for-one with the Russell 3000 total-return index, at 25 bp per side
on the spinco and 1 bp on the hedge.

Point in time: the distribution classification must be stated in a filing dated on or before the entry
session. The Norgate link is identity resolution only (symbols, EDGAR and Norgate names, first-quote DATES).

Existing owners only: ``alpha_agent.r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd``;
``alpha_agent.alpha_recovery.spinoff_data`` for the filings and links. RESEARCH ONLY; ``capital_eligible`` is
always False.
"""
from __future__ import annotations

import math
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import BH_Q, MATERIALITY_ANN_NET, write_artifact
from . import spinoff_data as SD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.spinoff_forced_selling"
MECHANISM_ID = "SPINOFF_PARENT_HOLDER_FORCED_SELLING_V1"
ARTIFACT_NAME = "spinoff_parent_holder_forced_selling.json"
PREREGISTRATION = "research/preregistration/SPINOFF_PARENT_HOLDER_FORCED_SELLING_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "029230e"
SCORER = "alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd"

KILL_RULE_FROZEN = (
    "Kill if the slot book (2 % NAV per linked Form 10-12B spin-off, long the spinco from the open of its 21st "
    "quoted session for 126 sessions, short the Russell 3000 total-return index) has annualised net <= 0, NW t < "
    "2.0 or annualised net below 1.5 %/yr at 25 bp per side; or its increment over the Russell 2000 and S&P 500 "
    "total-return indices has t < 2.0 or annualised alpha below 1.5 %/yr; or annualised net is non-positive in "
    "either qualification half; or its maximum drawdown is worse than -20 %; or it fails BH q=0.10 at m=4 with "
    "S14-B1, S14-B2 and INDEX_MEMBERSHIP_FORCED_FLOW_V1 inherited at p=1; or annualised net at 50 bp per side is "
    "below 1.5 %/yr; or the untouched 2013-2026 confirmation has annualised net <= 0, NW t < 2.0 or annualised "
    "net below 1.5 %/yr.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
CALENDAR_SYMBOL = "$SPXTR"                                                          # s1
HEDGE_SYMBOL = "$RUATR"
CONTROL_SYMBOLS = ("$RUTTR", "$SPXTR")
ENTRY_QUOTE_INDEX = 20                    # the 21st quoted session (0-based)          s3
HOLD = 126
SLOT = 0.02
MAX_SLOTS = 50
COST_PRIMARY_BPS = 25.0                                                              # s5
COST_LADDER_BPS = (0.0, 12.5, 25.0, 50.0)
COST_STRESS_BPS = 50.0
HEDGE_COST_BPS = 1.0
PPY = 252.0
NW_LAG = 10                                                                          # s6
T_FLOOR = float(STANDALONE_T_FLOOR)
QUALIFICATION = ("1996-01-01", "2012-12-31")                                         # s4 (entry dates)
HALVES = {"H1_1996_2004": ("1996-01-01", "2004-12-31"), "H2_2005_2012": ("2005-01-01", "2012-12-31")}
CONFIRMATION = ("2013-01-01", "2026-12-31")
#: s7 gate 1. The 0.85 floor and the 150/100 event floors were set before the first identity census and never
#: moved. What the 0.85 floor measures was restated twice before any return (preregistration 1.5): it is now the
#: identity resolution of registrants for which a candidate new security exists in the window.
MIN_IDENTITY_RESOLUTION = 0.85
UNRESOLVED_STATES = ("SYMBOL_AMBIGUOUS", "NAME_AMBIGUOUS", "UNCONFIRMED_SYMBOL", "NO_NAME_KEY")
MIN_SUBMISSIONS_SHARE = 0.99
MIN_PARSED_SHARE = 0.99
MIN_QUALIFICATION_EVENTS, MIN_CONFIRMATION_EVENTS = 150, 100
MIN_HEDGE_FINITE = 0.99
DRAWDOWN_FLOOR = -0.20
INHERITED_NULLS = ("S14-B1", "S14-B2", "INDEX_MEMBERSHIP_FORCED_FLOW_V1")


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _norm(obj):
    obj = obj.copy()
    obj.index = pd.DatetimeIndex(obj.index).normalize()
    return obj.sort_index()


# --------------------------------------------------------------------------- #
# Events (s2)
# --------------------------------------------------------------------------- #
def pit_events(rows: list, parsed: dict, universe: dict, quote_dates: dict, names: dict, edgar: dict) -> dict:
    """Linked distribution registrants whose classification was public by the entry session.

    ``quote_dates``: {Norgate symbol: sorted 'YYYY-MM-DD'} for linked securities; ``names``: {Norgate symbol:
    security name}; ``edgar``: ``spinoff_data.edgar_identity`` of the distribution registrants."""
    regs = SD.registrants(rows, parsed)
    spin = [g for g in regs if g["is_spinoff"]]
    linked = SD.link_norgate(spin, universe, names, edgar)
    by_cik: dict = {}
    for r in rows:
        by_cik.setdefault(r["cik"], []).append(r)
    events, states = [], {}
    for g in linked:
        states[g["link_state"]] = states.get(g["link_state"], 0) + 1
        if g["link_state"] not in SD.LINKED_STATES:
            continue
        sym, first, ticker = g["link"]
        q = quote_dates.get(sym) or []
        if len(q) <= ENTRY_QUOTE_INDEX:
            states["TOO_FEW_QUOTES"] = states.get("TOO_FEW_QUOTES", 0) + 1
            continue
        entry = q[ENTRY_QUOTE_INDEX]
        exit_i = min(ENTRY_QUOTE_INDEX + HOLD - 1, len(q) - 1)
        public = SD.registrants([r for r in by_cik[g["cik"]] if r["date"] <= entry], parsed)
        if not (public and public[0]["is_spinoff"]):
            states["NOT_PUBLIC_BY_ENTRY"] = states.get("NOT_PUBLIC_BY_ENTRY", 0) + 1
            continue
        events.append({"cik": g["cik"], "company": g["company"], "symbol": sym, "ticker": ticker,
                       "link_state": g["link_state"], "first_quote": first, "entry": entry, "exit": q[exit_i],
                       "full_hold": exit_i == ENTRY_QUOTE_INDEX + HOLD - 1})
    events.sort(key=lambda e: (e["entry"], e["symbol"]))
    n_linked = sum(states.get(s, 0) for s in SD.LINKED_STATES)
    n_unresolved = sum(states.get(s, 0) for s in UNRESOLVED_STATES)
    records = sum(1 for g in spin if (edgar.get(g["cik"]) or {}).get("record"))
    return {"registrants": len(regs), "spinoff_registrants": len(spin), "link_states": states, "linked": n_linked,
            "unresolved": n_unresolved,
            "identity_resolution": (n_linked / (n_linked + n_unresolved)) if (n_linked + n_unresolved) else 0.0,
            "link_coverage": (n_linked / len(spin)) if spin else 0.0,
            "submissions_share": (records / len(spin)) if spin else 0.0, "events": events}


# --------------------------------------------------------------------------- #
# The frozen book (s3)
# --------------------------------------------------------------------------- #
def event_growth(px: pd.DataFrame, entry: str, exit_: str, sessions) -> pd.Series:
    """Cumulative growth on each market session from the entry open to the exit close; flat when unquoted."""
    sess = pd.DatetimeIndex(sessions)
    hold = sess[(sess >= pd.Timestamp(entry)) & (sess <= pd.Timestamp(exit_))]
    closes = px["Close"].reindex(hold).to_numpy(dtype=float)
    g = np.ones(len(hold))
    level, last = 1.0, None
    for k in range(len(hold)):
        c = closes[k]
        if np.isfinite(c):
            if k == 0:
                o = float(px.at[hold[0], "Open"]) if hold[0] in px.index else float("nan")
                level *= c / o if np.isfinite(o) and o > 0 else 1.0
            elif last is not None and last > 0:
                level *= c / last
            last = c
        g[k] = level
    return pd.Series(g, index=hold)


def book(events: list, prices: dict, hedge: pd.Series, sessions, *, cost_bps: float) -> dict:
    """Additive daily NAV P&L of 2 % slots, each long its spinco and short the hedge index from entry to exit.
    The hedge accrues from the close before the entry session."""
    sess = pd.DatetimeIndex(sessions)
    hedge = hedge.reindex(sess)
    prev = hedge.shift(1)
    pnl = pd.Series(0.0, index=sess)
    active = pd.Series(0, index=sess)
    taken, skipped, rows, ends = 0, 0, [], []
    c, ch = float(cost_bps) * 1e-4, HEDGE_COST_BPS * 1e-4
    for e in events:
        start = pd.Timestamp(e["entry"])
        ends = [x for x in ends if x >= start]
        px = prices.get(e["symbol"])
        if len(ends) >= MAX_SLOTS or px is None:
            skipped += 1
            continue
        g = event_growth(px, e["entry"], e["exit"], sess)
        if not len(g):
            skipped += 1
            continue
        hg = (hedge.reindex(g.index) / prev.reindex(g.index).iloc[0]).to_numpy(dtype=float)
        if not np.all(np.isfinite(hg)):
            skipped += 1
            continue
        spread = g.to_numpy() - hg
        daily = np.diff(np.r_[0.0, spread]) * SLOT
        daily[0] -= SLOT * (c + ch)
        daily[-1] -= SLOT * (c * g.iloc[-1] + ch * hg[-1])
        pnl.loc[g.index] += daily
        active.loc[g.index] += 1
        ends.append(g.index[-1])
        taken += 1
        rows.append([e["entry"], e["symbol"], float(g.iloc[-1] - 1.0), float(hg[-1] - 1.0),
                     float(spread[-1] - (c + ch) - (c * g.iloc[-1] + ch * hg[-1]))])
    span = active[active > 0]
    if len(span):
        pnl = pnl.loc[span.index.min():span.index.max()]
        active = active.loc[pnl.index]
    return {"pnl": pnl, "active": active, "events_taken": taken, "events_skipped": skipped, "event_rows": rows}


def stats(pnl: pd.Series, rows: list) -> dict:
    x = pnl.to_numpy(dtype=float)
    if len(x) < 20 or not rows:
        return {"sessions": int(len(x)), "events": len(rows), "ann_net": None, "t_nw": None, "p_one_sided": None,
                "max_dd": None}
    st = S.nw_tstat(x, NW_LAG)
    sd = float(np.std(x, ddof=1))
    ev = np.array([r[4] for r in rows], dtype=float)
    esd = float(np.std(ev, ddof=1)) if len(ev) > 2 else float("nan")
    return {"sessions": int(len(x)), "events": int(len(rows)), "ann_net": float(x.mean() * PPY),
            "sharpe": float(x.mean() / sd * math.sqrt(PPY)) if sd > 0 else None, "t_nw": _f(st["t"]),
            "p_one_sided": _f(st["p_one_sided"]), "max_dd": _f(S._max_dd(x)),
            "mean_event_net_excess": float(ev.mean()),
            "event_t": float(ev.mean() / esd * math.sqrt(len(ev))) if esd and esd > 0 else None,
            "event_hit_rate": float((ev > 0).mean())}


def increment(pnl: pd.Series, controls: pd.DataFrame) -> dict:
    rets = controls.pct_change().reindex(pnl.index)
    ok = np.isfinite(pnl.to_numpy()) & np.all(np.isfinite(rets.to_numpy()), axis=1)
    y, X = pnl.to_numpy()[ok], rets.to_numpy()[ok]
    if len(y) < 60:
        return {"sessions": int(len(y)), "t": None, "ann": None}
    A = np.column_stack([np.ones(len(y)), X])
    coef = np.linalg.lstsq(A, y, rcond=None)[0]
    a = y - X @ coef[1:]
    st = S.nw_tstat(a, NW_LAG)
    return {"sessions": int(len(y)), "betas": {s: float(b) for s, b in zip(controls.columns, coef[1:])},
            "t": _f(st["t"]), "ann": float(a.mean() * PPY),
            "definition": "a = pnl - b1 x $RUTTR - b2 x $SPXTR (OLS); nw_tstat(a, %d)" % NW_LAG}


def multiplicity(p: Optional[float]) -> dict:
    pv = {MECHANISM_ID: 1.0 if _f(p) is None else float(p)}
    pv.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS), "p_one_sided": _f(p),
            "passes": bool(bh["per_test"].get(MECHANISM_ID)), "single_survivor_threshold": BH_Q / bh["m"]}


def _window(events: list, lo: str, hi: str) -> list:
    return [e for e in events if lo <= e["entry"] <= hi]


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_inputs() -> dict:
    """Filings, EDGAR names and links from ``spinoff_data``; total-return opens and closes of linked securities."""
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import norgatedata as nd
    kw = dict(padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    tr = nd.StockPriceAdjustmentType.TOTALRETURN
    rows, parsed = SD.load_index(), SD.load_parsed()
    universe, names = SD.norgate_universe(), SD.norgate_names()
    spin = [g for g in SD.registrants(rows, parsed) if g["is_spinoff"]]
    edgar = SD.edgar_identity([g["cik"] for g in spin], rows)
    need = sorted({g["link"][0] for g in SD.link_norgate(spin, universe, names, edgar) if g["link"]})
    prices, quote_dates, load_failed = {}, {}, []
    for sym in need:
        try:
            px = _norm(nd.price_timeseries(sym, stock_price_adjustment_setting=tr, **kw))[["Open", "Close"]].astype(float)
            prices[sym] = px
            quote_dates[sym] = [str(d.date()) for d in px.index]
        except Exception:                                        # noqa: BLE001 - counted by the data gate
            load_failed.append(sym)
    sessions = pd.DatetimeIndex(_norm(nd.price_timeseries(CALENDAR_SYMBOL, stock_price_adjustment_setting=tr, **kw)).index)
    series = {s: _norm(nd.price_timeseries(s, stock_price_adjustment_setting=tr, **kw))["Close"].astype(float)
              for s in (HEDGE_SYMBOL,) + CONTROL_SYMBOLS}
    return {"rows": rows, "parsed": parsed, "universe": universe, "names": names, "edgar": edgar, "prices": prices,
            "quote_dates": quote_dates, "sessions": sessions, "hedge": series[HEDGE_SYMBOL].reindex(sessions),
            "controls": pd.DataFrame({s: series[s] for s in CONTROL_SYMBOLS}).reindex(sessions),
            "failed_index_quarters": SD.failed_quarters(), "price_load_failed": load_failed}


# --------------------------------------------------------------------------- #
# Evaluation (s6, s7)
# --------------------------------------------------------------------------- #
def evaluate(inputs: dict) -> dict:
    out: dict = {"qualification_entries": QUALIFICATION, "confirmation_entries": CONFIRMATION, "halves": HALVES,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"},
                 "untouched_confirmation": "NOT_READ"}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "kill_rule_fired": gate, "why": why})
        return out

    rows, parsed = inputs["rows"], inputs["parsed"]
    sessions = pd.DatetimeIndex(inputs["sessions"])
    ev = pit_events(rows, parsed, inputs["universe"], inputs["quote_dates"], inputs["names"], inputs.get("edgar") or {})
    events = [e for e in ev["events"] if e["symbol"] in inputs["prices"]]
    q_events, c_events = _window(events, *QUALIFICATION), _window(events, *CONFIRMATION)
    accs = {r["accession"] for r in rows}
    parsed_share = (sum(1 for a in accs if a in parsed) / len(accs)) if accs else 0.0
    span = sessions[sessions >= pd.Timestamp(QUALIFICATION[0])]
    fin = float(np.isfinite(inputs["hedge"].reindex(span).to_numpy(dtype=float)).mean()) if len(span) else 0.0
    problems = []
    if inputs.get("failed_index_quarters"):
        problems.append("INDEX_QUARTERS_MISSING_%s" % list(inputs["failed_index_quarters"])[:8])
    if parsed_share < MIN_PARSED_SHARE:
        problems.append("PARSED_SHARE_%.4f" % parsed_share)
    if ev["submissions_share"] < MIN_SUBMISSIONS_SHARE:
        problems.append("SUBMISSIONS_SHARE_%.4f_BELOW_%.2f" % (ev["submissions_share"], MIN_SUBMISSIONS_SHARE))
    if ev["identity_resolution"] < MIN_IDENTITY_RESOLUTION:
        problems.append("IDENTITY_RESOLUTION_%.4f_BELOW_%.2f" % (ev["identity_resolution"], MIN_IDENTITY_RESOLUTION))
    if inputs.get("price_load_failed"):
        problems.append("PRICE_LOAD_FAILED_%d" % len(inputs["price_load_failed"]))
    if len(q_events) < MIN_QUALIFICATION_EVENTS:
        problems.append("QUALIFICATION_EVENTS_%d_BELOW_%d" % (len(q_events), MIN_QUALIFICATION_EVENTS))
    if len(c_events) < MIN_CONFIRMATION_EVENTS:
        problems.append("CONFIRMATION_EVENTS_%d_BELOW_%d" % (len(c_events), MIN_CONFIRMATION_EVENTS))
    if fin < MIN_HEDGE_FINITE:
        problems.append("HEDGE_FINITE_SHARE_%.4f" % fin)
    out["data"] = {"index_rows": len(rows), "accessions": len(accs), "parsed_share": parsed_share,
                   "registrants": ev["registrants"], "spinoff_registrants": ev["spinoff_registrants"],
                   "link_states": ev["link_states"], "identity_resolution": ev["identity_resolution"],
                   "raw_link_coverage": ev["link_coverage"], "submissions_share": ev["submissions_share"],
                   "qualification_events": len(q_events), "confirmation_events": len(c_events),
                   "hedge_finite_share": fin, "problems": problems}
    if problems:
        return done("DATA_HOLD", "DATA", "DATA gate: %s" % "; ".join(problems[:8]))

    prices, hedge = inputs["prices"], inputs["hedge"]
    books = {k: book(q_events, prices, hedge, sessions, cost_bps=k) for k in COST_LADDER_BPS}
    by_cost = {"%.1f" % k: stats(b["pnl"], b["event_rows"]) for k, b in books.items()}
    prim, pb = by_cost["%.1f" % COST_PRIMARY_BPS], books[COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["diagnostics_qualification"] = {"events_taken": pb["events_taken"], "events_skipped": pb["events_skipped"],
                                        "mean_active_slots": float(pb["active"].mean()) if len(pb["active"]) else None,
                                        "max_active_slots": int(pb["active"].max()) if len(pb["active"]) else None,
                                        "link_states_of_events": {s: sum(1 for e in q_events if e["link_state"] == s)
                                                                  for s in SD.LINKED_STATES}}
    out["qualification_event_rows"] = pb["event_rows"]
    if prim.get("ann_net") is None or prim["ann_net"] <= 0.0:
        return done("NO_EDGE", "WRONG_SIGN", "annualised net %s at %.1f bp is not in the frozen direction"
                    % (prim.get("ann_net"), COST_PRIMARY_BPS))
    if prim.get("t_nw") is None or prim["t_nw"] < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s < %.1f" % (prim.get("t_nw"), T_FLOOR))
    if prim["ann_net"] < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "MATERIALITY", "annualised net %.4f < %.3f" % (prim["ann_net"], MATERIALITY_ANN_NET))
    inc = increment(pb["pnl"], inputs["controls"])
    out["increment_over_index_returns"] = inc
    if inc.get("t") is None or inc["t"] < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "INCREMENT", "increment t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {}
    for k, (lo, hi) in HALVES.items():
        hb = book(_window(q_events, lo, hi), prices, hedge, sessions, cost_bps=COST_PRIMARY_BPS)
        stab[k] = stats(hb["pnl"], hb["event_rows"]).get("ann_net")
    out["stability_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("NO_EDGE", "STABILITY", "non-positive annualised net in %s" % ", ".join(bad))
    if (prim.get("max_dd") or 0.0) < DRAWDOWN_FLOOR:
        return done("NO_EDGE", "DRAWDOWN", "max drawdown %s worse than %.2f" % (prim.get("max_dd"), DRAWDOWN_FLOOR))
    mult = multiplicity(prim.get("p_one_sided"))
    out["multiplicity"] = mult
    if not mult["passes"]:
        return done("NO_EDGE", "MULTIPLICITY", "fails BH q=%.2f at m=%d (p %s)" % (BH_Q, mult["m"], mult["p_one_sided"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "COST", "annualised net at %.0f bp %s" % (COST_STRESS_BPS, stress.get("ann_net")))
    cb = book(c_events, prices, hedge, sessions, cost_bps=COST_PRIMARY_BPS)
    cs = stats(cb["pnl"], cb["event_rows"])
    out["confirmation"] = {"state": "READ", "stats": cs, "event_rows": cb["event_rows"]}
    good = (cs.get("ann_net") is not None and cs["ann_net"] > 0.0 and cs.get("t_nw") is not None
            and cs["t_nw"] >= T_FLOOR and cs["ann_net"] >= MATERIALITY_ANN_NET)
    out["untouched_confirmation"] = "CONFIRMED" if good else "FAILED"
    if not good:
        return done("NO_EDGE", "CONFIRMATION", "untouched confirmation ann %s t %s did not reproduce"
                    % (cs.get("ann_net"), cs.get("t_nw")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched confirmation; a "
                                   "HUMAN gate governs prospective registration")


def design() -> dict:
    return {"filings": "EDGAR full-index Form 10-12B and 10-12B/A, 1996Q1-2026Q3 (alpha_recovery.spinoff_data)",
            "spinoff": "pro rata distribution language (or the Form 10's Distribution / Spin-Off defined terms) and no "
                       "bankruptcy-emergence, Chapter 11 plan, BDC or blank-check-company language, stated in a filing "
                       "dated on or before entry",
            "link": "SYMBOL (stated listing symbol, first quote in window, Norgate name sharing a token with any EDGAR name) "
                    "else NAME (unique security first quoted in window carrying the current or index name key)",
            "entry": "total-return open of the 21st quoted session", "exit": "total-return close of the 146th quoted session",
            "slot": SLOT, "max_slots": MAX_SLOTS, "hedge": HEDGE_SYMBOL, "controls": list(CONTROL_SYMBOLS),
            "cost_primary_bps_per_side": COST_PRIMARY_BPS, "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "cost_stress_bps_per_side": COST_STRESS_BPS, "hedge_cost_bps_per_side": HEDGE_COST_BPS, "nw_lag": NW_LAG,
            "qualification_entries": QUALIFICATION, "halves": HALVES, "confirmation_entries": CONFIRMATION,
            "data_gate": {"min_identity_resolution": MIN_IDENTITY_RESOLUTION, "unresolved_states": list(UNRESOLVED_STATES),
                          "min_submissions_share": MIN_SUBMISSIONS_SHARE, "min_parsed_share": MIN_PARSED_SHARE,
                          "min_qualification_events": MIN_QUALIFICATION_EVENTS,
                          "min_confirmation_events": MIN_CONFIRMATION_EVENTS, "min_hedge_finite": MIN_HEDGE_FINITE},
            "drawdown_floor": DRAWDOWN_FLOOR, "multiplicity": {"q": BH_Q, "inherited_nulls_at_p1": list(INHERITED_NULLS)}}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None) -> dict:
    inputs = load_inputs() if inputs is None else inputs
    res = evaluate(inputs)
    ident = "EDGAR:Form 10-12B(%d rows, %d parsed, %d submissions records);NORGATE:US Equities+Delisted TOTALRETURN " \
            "open/close of %d linked securities;%s;%s" % (
                len(inputs["rows"]), len(inputs["parsed"]),
                sum(1 for v in (inputs.get("edgar") or {}).values() if v.get("record")), len(inputs["prices"]),
                HEDGE_SYMBOL, ",".join(CONTROL_SYMBOLS))
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
            "statistic": {"lockbox_t": prim.get("t_nw"), "lockbox_window": "QUALIFICATION entries %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_one_sided"), "events": prim.get("events"),
                          "event_t": prim.get("event_t"), "increment_t": inc.get("t"),
                          "untouched_confirmation": res.get("untouched_confirmation"), "data": res.get("data"),
                          "confirmation": {k: v for k, v in (res.get("confirmation") or {}).items() if k != "event_rows"}},
            "economics": {"ann_net_25bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"), "max_dd": prim.get("max_dd"),
                          "ann_net_50bp": stress.get("ann_net"), "increment_ann": inc.get("ann"),
                          "mean_event_net_excess": prim.get("mean_event_net_excess"),
                          "stability_ann_net": res.get("stability_ann_net"),
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


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "pit_events", "event_growth", "book", "stats", "increment",
           "multiplicity", "evaluate", "run", "run_mechanism", "load_inputs"]
