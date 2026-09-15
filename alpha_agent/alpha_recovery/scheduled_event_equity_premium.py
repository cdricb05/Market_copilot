r"""alpha_agent.alpha_recovery.scheduled_event_equity_premium - executor for the Alpha Agent mechanism
``SCHEDULED_EVENT_EQUITY_PREMIUM_V1``.

Contract: ``research/preregistration/SCHEDULED_EVENT_EQUITY_PREMIUM_PREREGISTRATION.md`` (committed
29d9fcd before any SPY return existed). Two frozen cells hold 100 % of NAV long SPY (total return)
over the sessions that carry a scheduled announcement and Treasury bills otherwise:

* ``PRE_FOMC``         - the first session on or after each scheduled FOMC decision day;
* ``ANNOUNCEMENT_DAY`` - the union of the FOMC, headline CPI and headline Employment Situation
  sessions.

Each cell is judged on 1994-2015 against a volatility-matched passive SPY long, and only a cell that
passes every qualification gate reads the untouched 2016-01-01..2026-08-25 confirmation.

Existing owners only: ``alpha_agent.r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd`` and the
dates-only calendars of ``scheduled_event_data``. RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import SPY_PROXY_COST_BPS, STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import BH_Q, GATE_DD_MULTIPLE, MATERIALITY_ANN_NET, write_artifact
from . import scheduled_event_data as SD

CALCULATION_OWNER = "alpha_agent.alpha_recovery.scheduled_event_equity_premium"
MECHANISM_ID = "SCHEDULED_EVENT_EQUITY_PREMIUM_V1"
ARTIFACT_NAME = "scheduled_event_equity_premium.json"
PREREGISTRATION = "research/preregistration/SCHEDULED_EVENT_EQUITY_PREMIUM_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "29d9fcd"

KILL_RULE_FROZEN = (
    "Kill a cell if its long-SPY announcement-session book has mean net excess return <= 0, NW t < 2.0 "
    "or annualised net below 1.5 %/yr at 1 bp per side; or its increment over a volatility-matched "
    "passive SPY long is non-positive or has NW t < 2.0; or annualised net is non-positive in either "
    "qualification half; or its maximum drawdown is worse than 1.5 times the control's; or it fails BH "
    "q=0.10 at m=6 with R32_EVENT_DRIVEN_CALENDAR (3 cells) and MACRO_EVENT_ANNOUNCEMENT_REACTION_R44_R45 "
    "inherited at p=1; or annualised net at 2 bp per side is below 1.5 %/yr; or the untouched 2016-2026 "
    "confirmation has mean <= 0, NW t < 2.0, annualised net below 1.5 %/yr or a non-positive increment.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
FROZEN_SHA256 = {                                                                  # s1
    "CPI_RELEASE_DATES": "119cb6d7f518dac9444a997fabee0671d34a555cd617c88b8d88b9be57baff23",
    "EMPLOYMENT_RELEASE_DATES": "0697e0e34585f9e5517e66d27fc34a5262af24cff757d26aea1b55f3d14261e8",
    "CPI_INITIAL_RELEASES": "874d988920998d0d23a481ef719ca60b7a7f7b8e5a49d1d75e24683d16437374",
    "EMPLOYMENT_INITIAL_RELEASES": "520b03a7205018f70ffd4ecef8808f091d9f8752f1a805ea6e0ea13ae5b859bf",
    "FOMC_CALENDAR": "5bfa5c19e52ed6eafe960c1ebf85c5702ed4c2cfe1749312181cb91b2f512ea3",
}
DTB3_PATH = Path(r"D:\Stock_Prediction_app_data\global_multi_asset_frontier_r36\acquired\fred_st_louis_fed\DTB3.json")
DTB3_SHA256 = "6028a3cbd7faa5e73f92e96e5523119beff50a858bc7030bfd555b5bd24c4faf"

EVENT_SPAN = ("1994-01-01", "2026-08-25")
FOMC_YEARS = tuple(range(1994, 2027))
FOMC_PER_YEAR_BOUNDS = (6, 9)                                                      # s6 gate 0
QUALIFICATION = ("1994-01-01", "2015-12-31")                                       # s3
HALVES = {"H1_1994_2004": ("1994-01-01", "2004-12-31"), "H2_2005_2015": ("2005-01-01", "2015-12-31")}
CONFIRMATION = ("2016-01-01", "2026-08-25")
WINDOWS = {"QUALIFICATION": QUALIFICATION, "CONFIRMATION": CONFIRMATION}

CELLS = ("PRE_FOMC", "ANNOUNCEMENT_DAY")                                           # s2
CELL_FAMILIES = {"PRE_FOMC": ("FOMC",), "ANNOUNCEMENT_DAY": ("FOMC", "CPI", "EMPLOYMENT")}
MIN_WINDOWS = {"PRE_FOMC": {"QUALIFICATION": 150, "CONFIRMATION": 70},
               "ANNOUNCEMENT_DAY": {"QUALIFICATION": 600, "CONFIRMATION": 280}}

COST_PRIMARY_BPS = float(SPY_PROXY_COST_BPS)                                       # s4: 1 bp
COST_LADDER_BPS = (0.0, 1.0, 2.0, 5.0)
COST_SURVIVAL_BPS = 2.0
PPY = 252.0                                                                        # s5
NW_LAG = 5
T_FLOOR = float(STANDALONE_T_FLOOR)                                                # 2.0
DD_MULTIPLE = float(GATE_DD_MULTIPLE)                                              # 1.5
MIN_FINITE_SHARE = 0.99
RF_STALE_DAYS = 10
MAX_RF_MISSING_SHARE = 0.01
INHERITED_NULLS = ("R32_EVENT_DRIVEN_CALENDAR|TURN_OF_MONTH_DUMMY",
                   "R32_EVENT_DRIVEN_CALENDAR|QUARTER_END_DUMMY",
                   "R32_EVENT_DRIVEN_CALENDAR|TRIPLE_WITCHING_DUMMY",
                   "MACRO_EVENT_ANNOUNCEMENT_REACTION_R44_R45")
SCORER = "alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd"

G_WRONG_SIGN, G_STANDALONE, G_MATERIALITY, G_INCREMENT = "WRONG_SIGN", "STANDALONE_T", "MATERIALITY", "INCREMENT"
G_STABILITY, G_DRAWDOWN, G_MULTIPLICITY, G_COST, G_CONFIRMATION = (
    "STABILITY", "DRAWDOWN", "MULTIPLICITY", "COST", "CONFIRMATION")


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if np.isfinite(x) else None


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_spy_total_return() -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries("SPY", stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    s = df["Close"].astype(float)
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.sort_index()


def load_dtb3(path=None) -> pd.Series:
    payload = json.loads(Path(path or DTB3_PATH).read_text(encoding="utf-8"))
    rows = {}
    for o in payload.get("observations") or []:
        v = _f(o.get("value"))
        if v is not None and o.get("date"):
            rows[pd.Timestamp(o["date"])] = v
    return pd.Series(rows, dtype=float).sort_index()


def integrity(*, capture_root=None, history_root=None, dtb3_path=None) -> dict:
    """Gate-0 identity checks: frozen capture hashes and the complete FOMC history manifest."""
    problems, observed = [], {}
    for key, want in FROZEN_SHA256.items():
        got = SD.file_sha256(SD.capture_path(key, capture_root))
        observed[key] = got
        if got != want:
            problems.append("CAPTURE_HASH_%s: observed %s, frozen %s" % (key, got, want))
    got = SD.file_sha256(dtb3_path or DTB3_PATH)
    observed["DTB3"] = got
    if got != DTB3_SHA256:
        problems.append("DTB3_HASH: observed %s, frozen %s" % (got, DTB3_SHA256))
    hroot = Path(history_root or SD.history_dir())
    try:
        man = json.loads((hroot / SD.MANIFEST_NAME).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        man = {}
    pages = man.get("pages") or {}
    for y in SD.HISTORY_YEARS:
        rec, p = pages.get(str(y)) or {}, hroot / ("fomchistorical%d.htm" % y)
        if not rec or not p.exists() or SD.file_sha256(p) != rec.get("sha256"):
            problems.append("FOMC_HISTORY_PAGE_%d_MISSING_OR_ALTERED" % y)
    observed["FOMC_HISTORY_MANIFEST"] = SD.file_sha256(hroot / SD.MANIFEST_NAME)
    return {"problems": problems, "observed_sha256": observed}


def event_calendars(*, capture_root=None, history_root=None) -> dict:
    """The three frozen event families (section 1.1), dates only."""
    ev = SD.fomc_events(history_root=history_root,
                        calendar_path=SD.capture_path("FOMC_CALENDAR", capture_root))
    lo, hi = EVENT_SPAN
    events = {"FOMC": [d for d in ev["scheduled_decision_days"] if lo <= d <= hi]}
    off_calendar = {}
    for fam in ("CPI", "EMPLOYMENT"):
        ini = SD.initial_release_days(fam + "_INITIAL_RELEASES", root=capture_root)
        cal = set(SD.fred_release_dates(fam + "_RELEASE_DATES", root=capture_root))
        heads = {v for v in ini.values() if lo <= v <= hi}
        events[fam] = sorted(heads & cal)
        off_calendar[fam] = sorted(heads - cal)
    per_year = Counter(d[:4] for d in ev["scheduled_decision_days"])
    return {"events": events,
            "fomc_per_year": {str(y): int(per_year.get(str(y), 0)) for y in FOMC_YEARS},
            "fomc_problems": list(ev["problems"]), "merged_headings": ev["merged_headings"],
            "headline_days_off_release_calendar": off_calendar}


def daily_frame(spy_tr: pd.Series, dtb3: pd.Series) -> pd.DataFrame:
    """Session returns and the bill rate of the last observation dated STRICTLY before each session."""
    px = spy_tr.dropna().sort_index()
    idx = pd.DatetimeIndex(px.index)
    r = px.pct_change().to_numpy(dtype=float)
    obs = dtb3.dropna().sort_index()
    oidx = pd.DatetimeIndex(obs.index)
    pos = oidx.searchsorted(idx, side="left") - 1
    ok = pos >= 0
    safe = np.clip(pos, 0, None)
    rate = np.where(ok, obs.to_numpy(dtype=float)[safe] if len(obs) else np.nan, np.nan)
    age = np.where(ok, (idx - oidx[safe]).days if len(obs) else np.inf, np.inf).astype(float)
    c = np.where(age <= RF_STALE_DAYS, rate / 100.0 / PPY, np.nan)
    df = pd.DataFrame({"r": r, "c": c, "rf_age_days": age}, index=idx)
    return df.iloc[1:]


def held_sessions(dates, sessions: pd.DatetimeIndex) -> dict:
    """``session -> [event dates]``: each event is held over the first session on or after its date."""
    out: dict = {}
    for d in dates:
        i = sessions.searchsorted(pd.Timestamp(d), side="left")
        if i < len(sessions):
            out.setdefault(sessions[i], []).append(str(d))
    return out


def cell_sessions(calendars: dict, sessions: pd.DatetimeIndex) -> dict:
    by_family = {f: held_sessions(ds, sessions) for f, ds in (calendars.get("events") or {}).items()}
    cells = {c: set().union(*(set(by_family.get(f) or {}) for f in fams)) for c, fams in CELL_FAMILIES.items()}
    return {"by_family": by_family, "cells": cells}


# --------------------------------------------------------------------------- #
# The frozen book
# --------------------------------------------------------------------------- #
def book(frame: pd.DataFrame, held, cost_bps: float) -> pd.DataFrame:
    p = frame.index.isin(pd.DatetimeIndex(sorted(held))).astype(float)
    prev = np.concatenate([[0.0], p[:-1]])
    nxt = np.concatenate([p[1:], [0.0]])
    entry = (p == 1.0) & (prev == 0.0)
    exit_ = (p == 1.0) & (nxt == 0.0)
    m = frame["r"].to_numpy(dtype=float) - frame["c"].to_numpy(dtype=float)
    x = p * m - float(cost_bps) * 1e-4 * (entry.astype(float) + exit_.astype(float))
    x = np.where(p == 1.0, x, np.where(np.isfinite(m), 0.0, np.nan))
    return pd.DataFrame({"x": x, "m": m, "p": p, "entry": entry, "exit": exit_}, index=frame.index)


def _slice(df: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    return df.loc[pd.Timestamp(lo):pd.Timestamp(hi)]


def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"sessions": int(len(x)), "mean": None, "ann_net": None, "t_net": None,
                "p_net_one_sided": None, "sharpe": None, "max_dd": None}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, NW_LAG)
    return {"sessions": int(len(x)), "mean": float(x.mean()), "ann_net": float(x.mean() * PPY),
            "ann_vol": float(sd * np.sqrt(PPY)), "sharpe": float(x.mean() / sd * np.sqrt(PPY)) if sd > 0 else None,
            "t_net": _f(st["t"]), "p_net_one_sided": _f(st["p_one_sided"]), "max_dd": _f(S._max_dd(x))}


def increment(bk: pd.DataFrame) -> dict:
    """Increment over the volatility-matched passive SPY long ``k * m``, ``k = sd(x) / sd(m)``."""
    ok = bk[np.isfinite(bk["x"].to_numpy()) & np.isfinite(bk["m"].to_numpy())]
    x, m = ok["x"].to_numpy(dtype=float), ok["m"].to_numpy(dtype=float)
    if len(x) < 3 or float(np.std(m, ddof=1)) <= 0:
        return {"state": "UNDEFINED", "ann": None, "t": None, "control_max_dd": None}
    k = float(np.std(x, ddof=1) / np.std(m, ddof=1))
    control = k * m
    d = x - control
    st = S.nw_tstat(d, NW_LAG)
    beta = float(np.cov(x, m, ddof=1)[0, 1] / np.var(m, ddof=1))
    stb = S.nw_tstat(x - beta * m, NW_LAG)
    csd = float(np.std(control, ddof=1))
    return {"state": "OK", "k_vol_matched": k, "ann": float(d.mean() * PPY), "t": _f(st["t"]),
            "p_one_sided": _f(st["p_one_sided"]), "control_ann": float(control.mean() * PPY),
            "control_sharpe": float(control.mean() / csd * np.sqrt(PPY)) if csd > 0 else None,
            "control_max_dd": _f(S._max_dd(control)),
            "definition": "d = x - k*m, k = sd(x)/sd(m) inside the window; nw_tstat(d, %d)" % NW_LAG,
            "beta_matched_diagnostic": {"beta": beta, "ann": float((x - beta * m).mean() * PPY),
                                        "t": _f(stb["t"])}}


def window_level(bk_gross: pd.DataFrame) -> dict:
    held = bk_gross[bk_gross["p"] == 1.0]
    if not len(held):
        return {"windows": 0}
    run = bk_gross["entry"].astype(int).cumsum()
    per = held["x"].groupby(run[held.index]).sum().to_numpy(dtype=float)
    per = per[np.isfinite(per)]
    return {"windows": int(len(per)), "mean_gross_excess_per_window": float(per.mean()) if len(per) else None,
            "hit_rate": float((per > 0).mean()) if len(per) else None}


def family_decomposition(bk_gross: pd.DataFrame, by_family: dict, families) -> dict:
    labels: dict = {}
    for s in bk_gross.index[bk_gross["p"] == 1.0]:
        key = "+".join(sorted(f for f in families if s in (by_family.get(f) or {})))
        labels.setdefault(key, []).append(s)
    out = {}
    for key, ss in sorted(labels.items()):
        m = bk_gross.loc[ss, "m"].to_numpy(dtype=float)
        m = m[np.isfinite(m)]
        out[key] = {"sessions": int(len(m)), "mean_gross_excess": float(m.mean()) if len(m) else None}
    return out


def multiplicity(pvals: dict) -> dict:
    p = {c: (1.0 if _f(pvals.get(c)) is None else float(pvals[c])) for c in CELLS}
    p.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(p, BH_Q)
    return {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
            "p_one_sided": {c: _f(pvals.get(c)) for c in CELLS}, "survivors": bh["survivors"],
            "passes": {c: bool(bh["per_test"].get(c)) for c in CELLS},
            "single_survivor_threshold": BH_Q / bh["m"]}


# --------------------------------------------------------------------------- #
# Evaluation
# --------------------------------------------------------------------------- #
def _years(lo: str, hi: str) -> float:
    return (pd.Timestamp(hi) - pd.Timestamp(lo)).days / 365.25


def data_gate(frame: pd.DataFrame, calendars: dict, sessions: dict, integ: dict) -> dict:
    problems = list(integ.get("problems") or []) + list(calendars.get("fomc_problems") or [])
    lo_b, hi_b = FOMC_PER_YEAR_BOUNDS
    for y, n in (calendars.get("fomc_per_year") or {}).items():
        if not lo_b <= int(n) <= hi_b:
            problems.append("FOMC_%s_HAS_%d_SCHEDULED_DECISION_DAYS" % (y, int(n)))
    coverage, windows = {}, {}
    for w, (lo, hi) in WINDOWS.items():
        sub = _slice(frame, lo, hi)
        fin = float(np.isfinite(sub["r"].to_numpy()).mean()) if len(sub) else 0.0
        rf_missing = float((~np.isfinite(sub["c"].to_numpy())).mean()) if len(sub) else 1.0
        coverage[w] = {"sessions": int(len(sub)), "spy_finite_share": fin, "bill_rate_missing_share": rf_missing}
        if fin < MIN_FINITE_SHARE:
            problems.append("SPY_FINITE_SHARE_%s_%.4f" % (w, fin))
        if rf_missing > MAX_RF_MISSING_SHARE:
            problems.append("BILL_RATE_MISSING_SHARE_%s_%.4f" % (w, rf_missing))
    for cell in CELLS:
        bk = book(frame, sessions["cells"][cell], 0.0)
        windows[cell] = {w: int(_slice(bk, lo, hi)["entry"].sum()) for w, (lo, hi) in WINDOWS.items()}
        for w, floor in MIN_WINDOWS[cell].items():
            if windows[cell][w] < floor:
                problems.append("%s_%s_WINDOWS_%d_BELOW_%d" % (cell, w, windows[cell][w], floor))
    return {"problems": problems, "coverage": coverage, "independent_windows": windows,
            "fomc_per_year": calendars.get("fomc_per_year"),
            "headline_days_off_release_calendar": calendars.get("headline_days_off_release_calendar")}


def _qualification(frame: pd.DataFrame, held: set, by_family: dict, families) -> dict:
    books = {c: book(frame, held, c) for c in COST_LADDER_BPS}
    q = {c: _slice(books[c], *QUALIFICATION) for c in COST_LADDER_BPS}
    prim = q[COST_PRIMARY_BPS]
    years = _years(*QUALIFICATION)
    out = {"families": list(families),
           "qualification_by_cost_bps_per_side": {"%.1f" % c: stats(q[c]["x"]) for c in COST_LADDER_BPS},
           "increment_qualification": increment(prim),
           "halves_ann_net": {k: stats(_slice(books[COST_PRIMARY_BPS], lo, hi)["x"]).get("ann_net")
                              for k, (lo, hi) in HALVES.items()},
           "capital_usage_qualification": float(prim["p"].mean()),
           "turnover_nav_per_year_qualification": float(2.0 * prim["entry"].sum() / years),
           "window_level_gross_qualification": window_level(q[0.0])}
    if len(families) > 1:
        out["family_decomposition_qualification"] = family_decomposition(q[0.0], by_family, families)
    out["_books"] = books
    return out


def _gate(cell: str, res: dict, mult: dict) -> tuple:
    prim = res["qualification_by_cost_bps_per_side"]["%.1f" % COST_PRIMARY_BPS]
    if prim.get("mean") is None or prim["mean"] <= 0.0:
        return G_WRONG_SIGN, "mean net excess %s per session is not in the frozen long direction" % prim.get("mean")
    if prim.get("t_net") is None or prim["t_net"] < T_FLOOR:
        return G_STANDALONE, "NW t %s < %.1f" % (prim.get("t_net"), T_FLOOR)
    if prim["ann_net"] < MATERIALITY_ANN_NET:
        return G_MATERIALITY, "annualised net %.4f < %.3f" % (prim["ann_net"], MATERIALITY_ANN_NET)
    inc = res["increment_qualification"]
    if inc.get("ann") is None or inc["ann"] <= 0.0 or inc.get("t") is None or inc["t"] < T_FLOOR:
        return G_INCREMENT, "increment over the volatility-matched passive long ann %s t %s" % (inc.get("ann"), inc.get("t"))
    bad = sorted(k for k, v in res["halves_ann_net"].items() if v is None or v <= 0.0)
    if bad:
        return G_STABILITY, "non-positive annualised net in %s" % ", ".join(bad)
    if (prim.get("max_dd") or 0.0) < DD_MULTIPLE * (inc.get("control_max_dd") or 0.0):
        return G_DRAWDOWN, "max drawdown %s worse than %.1f x control %s" % (prim.get("max_dd"), DD_MULTIPLE,
                                                                             inc.get("control_max_dd"))
    if not mult["passes"].get(cell):
        return G_MULTIPLICITY, "not a BH survivor at q=%.2f, m=%d (p %s)" % (BH_Q, mult["m"], mult["p_one_sided"].get(cell))
    surv = res["qualification_by_cost_bps_per_side"]["%.1f" % COST_SURVIVAL_BPS]
    if (surv.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return G_COST, "annualised net at %.0f bp %s < %.3f" % (COST_SURVIVAL_BPS, surv.get("ann_net"), MATERIALITY_ANN_NET)
    return None, "every qualification gate passed"


def evaluate(frame: pd.DataFrame, calendars: dict, *, integ: Optional[dict] = None) -> dict:
    sessions = cell_sessions(calendars, pd.DatetimeIndex(frame.index))
    out = {"qualification_window": QUALIFICATION, "confirmation_window": CONFIRMATION,
           "halves": HALVES, "cells": {}}
    dg = data_gate(frame, calendars, sessions, integ or {"problems": []})
    out["data"] = dg
    if dg["problems"]:
        out.update({"verdict": "DATA_HOLD", "gate": "DATA", "kill_rule_fired": "DATA",
                    "why": "DATA gate: %s" % "; ".join(dg["problems"][:12]),
                    "untouched_confirmation": "NOT_READ"})
        return out
    quals = {c: _qualification(frame, sessions["cells"][c], sessions["by_family"], CELL_FAMILIES[c]) for c in CELLS}
    mult = multiplicity({c: quals[c]["qualification_by_cost_bps_per_side"]["%.1f" % COST_PRIMARY_BPS]
                        .get("p_net_one_sided") for c in CELLS})
    out["multiplicity"] = mult
    confirmation_states = []
    for c in CELLS:
        res = quals[c]
        books = res.pop("_books")
        gate, detail = _gate(c, res, mult)
        res["confirmation"] = {"state": "UNREAD", "why": "read only after every qualification gate passes"}
        if gate is None:
            cb = _slice(books[COST_PRIMARY_BPS], *CONFIRMATION)
            cs, ci = stats(cb["x"]), increment(cb)
            res["confirmation"] = {"state": "READ", "stats": cs, "increment": ci,
                                   "stats_by_cost_bps_per_side": {"%.1f" % k: stats(_slice(books[k], *CONFIRMATION)["x"])
                                                                  for k in COST_LADDER_BPS},
                                   "capital_usage": float(cb["p"].mean()),
                                   "independent_windows": int(cb["entry"].sum())}
            ok = (cs.get("mean") is not None and cs["mean"] > 0.0 and cs.get("t_net") is not None
                  and cs["t_net"] >= T_FLOOR and (cs.get("ann_net") or 0.0) >= MATERIALITY_ANN_NET
                  and ci.get("ann") is not None and ci["ann"] > 0.0)
            confirmation_states.append("CONFIRMED" if ok else "FAILED")
            if not ok:
                gate, detail = G_CONFIRMATION, ("untouched confirmation mean %s ann %s t %s increment %s"
                                                % (cs.get("mean"), cs.get("ann_net"), cs.get("t_net"), ci.get("ann")))
        res.update({"gate": gate, "gate_detail": detail, "verdict": "QUALIFIED" if gate is None else "NO_EDGE"})
        out["cells"][c] = res
    qualified = [c for c in CELLS if out["cells"][c]["gate"] is None]
    fired = ";".join("%s:%s" % (c, out["cells"][c]["gate"] or "PASSED") for c in CELLS)
    out["kill_rule_fired"] = None if qualified else fired
    out["gate"] = fired
    out["untouched_confirmation"] = ("CONFIRMED" if "CONFIRMED" in confirmation_states else
                                     "FAILED" if confirmation_states else "NOT_READ")
    if qualified:
        out.update({"verdict": "QUALIFIED",
                    "why": "%s passed every preregistered gate including the untouched confirmation; a HUMAN gate "
                           "governs prospective registration" % ", ".join(qualified)})
    else:
        out.update({"verdict": "NO_EDGE",
                    "why": "; ".join("%s killed at %s (%s)" % (c, out["cells"][c]["gate"], out["cells"][c]["gate_detail"])
                                     for c in CELLS)})
    return out


def design() -> dict:
    return {"cells": {c: list(f) for c, f in CELL_FAMILIES.items()}, "event_span": EVENT_SPAN,
            "qualification": QUALIFICATION, "halves": HALVES, "confirmation": CONFIRMATION,
            "position": "100 % NAV long SPY total return on held sessions, Treasury bills otherwise, no stacking",
            "session_mapping": "first SPY session on or after the event date",
            "bill_rate": "DTB3 of the last observation dated strictly before the session / 100 / 252",
            "cost_primary_bps_per_side": COST_PRIMARY_BPS, "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "cost_survival_bps_per_side": COST_SURVIVAL_BPS, "nw_lag": NW_LAG, "t_floor": T_FLOOR,
            "materiality_ann_net": MATERIALITY_ANN_NET, "drawdown_multiple": DD_MULTIPLE,
            "min_independent_windows": MIN_WINDOWS, "fomc_per_year_bounds": FOMC_PER_YEAR_BOUNDS,
            "control": "volatility-matched passive SPY long inside the evaluated window",
            "multiplicity": {"q": BH_Q, "cells": list(CELLS), "inherited_nulls_at_p1": list(INHERITED_NULLS)}}


def run(*, verbose: bool = True, write: bool = True, spy_tr: Optional[pd.Series] = None,
        dtb3: Optional[pd.Series] = None, calendars: Optional[dict] = None,
        integ: Optional[dict] = None) -> dict:
    spy_tr = load_spy_total_return() if spy_tr is None else spy_tr
    dtb3 = load_dtb3() if dtb3 is None else dtb3
    calendars = event_calendars() if calendars is None else calendars
    integ = integrity() if integ is None else integ
    frame = daily_frame(spy_tr, dtb3)
    res = evaluate(frame, calendars, integ=integ)
    observed = integ.get("observed_sha256") or {}
    ident = "NORGATE:SPY(TOTALRETURN):%s..%s;FRED:DTB3:%s;R46:%s;FED_FOMC_HISTORY_MANIFEST:%s" % (
        str(frame.index.min().date()) if len(frame) else None, str(frame.index.max().date()) if len(frame) else None,
        str(observed.get("DTB3"))[:12],
        ",".join("%s=%s" % (k, str(observed.get(k))[:12]) for k in sorted(FROZEN_SHA256)),
        str(observed.get("FOMC_HISTORY_MANIFEST"))[:12])
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "preregistration_commit": PREREGISTRATION_COMMIT, "kill_rule_frozen": KILL_RULE_FROZEN,
            "design": design(), "input_data_identity": ident, "integrity": integ,
            "event_counts": {f: len(v) for f, v in (calendars.get("events") or {}).items()},
            "result": res, "scorer": SCORER, "capital_eligible": False}
    if write:
        body["artifact_path"] = str(write_artifact(ARTIFACT_NAME, body))
    if verbose:
        print("%s -> %s (%s)" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def hold_result(why: str) -> dict:
    return {"verdict": "DATA_HOLD", "why": why, "kill_rule_fired": "DATA", "statistic": {}, "economics": {},
            "multiplicity": {"m": len(CELLS) + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
            "artifact": None, "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}


def executor_result(body: dict) -> dict:
    res = body["result"]
    cells = res.get("cells") or {}

    def prim(c):
        return ((cells.get(c) or {}).get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}

    def surv(c):
        return ((cells.get(c) or {}).get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_SURVIVAL_BPS) or {}

    best = max(CELLS, key=lambda c: (prim(c).get("t_net") if prim(c).get("t_net") is not None else -1e9))
    stat_cells = {c: {"t_net_1bp": prim(c).get("t_net"), "p_one_sided": prim(c).get("p_net_one_sided"),
                      "sessions": prim(c).get("sessions"),
                      "independent_windows": ((res.get("data") or {}).get("independent_windows") or {}).get(c),
                      "increment_t": ((cells.get(c) or {}).get("increment_qualification") or {}).get("t"),
                      "gate": (cells.get(c) or {}).get("gate"),
                      "confirmation": (cells.get(c) or {}).get("confirmation")} for c in CELLS}
    econ_cells = {c: {"ann_net_1bp": prim(c).get("ann_net"), "sharpe": prim(c).get("sharpe"),
                      "max_dd": prim(c).get("max_dd"), "ann_net_2bp": surv(c).get("ann_net"),
                      "increment_ann": ((cells.get(c) or {}).get("increment_qualification") or {}).get("ann"),
                      "control_ann": ((cells.get(c) or {}).get("increment_qualification") or {}).get("control_ann"),
                      "halves_ann_net": (cells.get(c) or {}).get("halves_ann_net"),
                      "capital_usage": (cells.get(c) or {}).get("capital_usage_qualification"),
                      "turnover_nav_per_year": (cells.get(c) or {}).get("turnover_nav_per_year_qualification")}
                  for c in CELLS}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("kill_rule_fired"),
            "statistic": {"lockbox_t": prim(best).get("t_net"), "lockbox_cell": best,
                          "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "untouched_confirmation": res.get("untouched_confirmation"), "cells": stat_cells},
            "economics": {"ann_net_1bp": prim(best).get("ann_net"), "sharpe": prim(best).get("sharpe"),
                          "max_dd": prim(best).get("max_dd"), "cells": econ_cells},
            "multiplicity": res.get("multiplicity") or {"m": len(CELLS) + len(INHERITED_NULLS), "q": BH_Q,
                                                        "state": "NOT_REACHED"},
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
        return hold_result("an owned input is unavailable or unreadable: %r" % (exc,))
    return executor_result(body)


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "CELLS", "evaluate", "run", "run_mechanism", "book",
           "daily_frame", "held_sessions", "increment", "multiplicity", "integrity", "event_calendars"]
