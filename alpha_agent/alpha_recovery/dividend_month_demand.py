r"""alpha_agent.alpha_recovery.dividend_month_demand - executor for the Alpha Agent mechanism
``DIVIDEND_MONTH_DEMAND_PREMIUM_V1``.

Contract: ``research/preregistration/DIVIDEND_MONTH_DEMAND_PREMIUM_PREREGISTRATION.md`` (committed
b7ef8c4 before any holding-month return existed). One frozen cell: at every month-end close, inside
point-in-time S&P 500 dividend payers (Norgate "S&P 500 Current & Past", delisted names included),
hold equal-weight long the payers that went ex-dividend in calendar month t-12 and equal-weight short
the payers that did not, for calendar month t, dollar-neutral, at the 12.5 bp desk cost on target-
weight turnover. It must beat the same-calendar-month return seasonality book and the untouched
2013-01..2026-08 confirmation.

Membership rule (the census trap): the latest S&P 500 flag on or before formation is 1 AND the
security has a close within 5 calendar days on or before formation.

Existing owners only: ``alpha_agent.r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd``.
RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import EQ_COST_RATE_PER_SIDE, STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import BH_Q, MATERIALITY_ANN_NET, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.dividend_month_demand"
MECHANISM_ID = "DIVIDEND_MONTH_DEMAND_PREMIUM_V1"
ARTIFACT_NAME = "dividend_month_demand_premium.json"
PREREGISTRATION = "research/preregistration/DIVIDEND_MONTH_DEMAND_PREMIUM_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "b7ef8c4"

KILL_RULE_FROZEN = (
    "Kill if the dollar-neutral predicted-payer book has mean net <= 0, NW t < 2.0 or annualised net below "
    "1.5 %/yr at 12.5 bp per side; or its increment over the same-calendar-month return seasonality book has "
    "t < 2.0 or annualised mean below 1.5 %/yr; or annualised net is non-positive in either qualification "
    "half; or its maximum drawdown is worse than -20 %; or it fails BH q=0.10 at m=4 with "
    "SAME_MONTH_SEASONALITY and RETURN_SEASONALITY (2 hypotheses) inherited at p=1; or annualised net at "
    "25 bp per side is below 1.5 %/yr; or the untouched 2013-2026 confirmation has mean <= 0, NW t < 2.0 "
    "or annualised net below 1.5 %/yr.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
WATCHLIST = "S&P 500 Current & Past"                                                 # s1
INDEX = "S&P 500"
STALE_DAYS = 5                                                                        # s1.1
COST_PRIMARY_BPS = float(EQ_COST_RATE_PER_SIDE) * 1e4                                 # s5: 12.5 bp
COST_LADDER_BPS = (0.0, COST_PRIMARY_BPS, 25.0, 50.0)
COST_STRESS_BPS = 25.0
PPY = 12.0                                                                            # s6
NW_LAG = 3
T_FLOOR = float(STANDALONE_T_FLOOR)
QUALIFICATION = ("1994-01", "2012-12")                                                # s4 (holding months)
HALVES = {"H1_1994_2003": ("1994-01", "2003-12"), "H2_2004_2012": ("2004-01", "2012-12")}
CONFIRMATION = ("2013-01", "2026-08")
MAX_LOAD_FAIL_SHARE = 0.01                                                            # s7 gate 1
MEMBERS_BOUNDS = (490.0, 510.0)
MIN_LONG, MIN_SHORT = 30, 100
MAX_UNQUOTED_END_SHARE = 0.02
MIN_QUALIFICATION_MONTHS, MIN_CONFIRMATION_MONTHS = 200, 150
DRAWDOWN_FLOOR = -0.20
INHERITED_NULLS = ("SAME_MONTH_SEASONALITY", "RETURN_SEASONALITY|1", "RETURN_SEASONALITY|2")
SCORER = "alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd"

_DAY = np.timedelta64(1, "D")


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if np.isfinite(x) else None


def _shift(y: int, m: int, k: int) -> tuple:
    total = y * 12 + (m - 1) - k
    return total // 12, total % 12 + 1


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def _norm(s: pd.Series) -> pd.Series:
    s = s.copy()
    s.index = pd.DatetimeIndex(s.index).normalize()
    return s.sort_index()


def load_universe() -> dict:
    """Every security of the watchlist: total-return closes, ex-dividend months and the membership flag."""
    import norgatedata as nd
    kw = dict(padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    spy = _norm(nd.price_timeseries("SPY", stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                                    **kw)["Close"].astype(float))
    symbols = list(nd.watchlist_symbols(WATCHLIST))
    universe, failed = {}, []
    for sym in symbols:
        try:
            tr = nd.price_timeseries(sym, stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN, **kw)
            raw = nd.price_timeseries(sym, stock_price_adjustment_setting=nd.StockPriceAdjustmentType.NONE, **kw)
            ic = nd.index_constituent_timeseries(sym, INDEX, **kw)
        except Exception:                                          # noqa: BLE001 - counted by the data gate
            failed.append(sym)
            continue
        div = raw["Dividend"].fillna(0.0) if "Dividend" in raw.columns else pd.Series(0.0, index=raw.index)
        exd = pd.DatetimeIndex(div[div > 0].index)
        universe[sym] = {"tr": _norm(tr["Close"].astype(float)), "ex": {(d.year, d.month) for d in exd},
                         "flag": _norm(ic.iloc[:, 0].astype(float))}
    return {"universe": universe, "sessions": spy.index, "spy_tr": spy, "n_symbols": len(symbols),
            "n_failed": len(failed), "failed": failed[:50]}


# --------------------------------------------------------------------------- #
# The frozen book
# --------------------------------------------------------------------------- #
def month_ends(sessions) -> pd.DatetimeIndex:
    s = pd.DatetimeIndex(sessions).normalize().sort_values()
    return pd.DatetimeIndex(pd.Series(s, index=s).groupby([s.year, s.month]).max().values)


def monthly_book(universe: dict, sessions, spy_tr: Optional[pd.Series] = None) -> pd.DataFrame:
    ends = month_ends(sessions)
    end_of = {(e.year, e.month): np.datetime64(e.to_datetime64()) for e in ends}
    prep = []
    for sym, rec in universe.items():
        tr = rec["tr"].dropna()
        fl = rec["flag"].dropna()
        prep.append((sym, tr.index.values.astype("datetime64[ns]"), tr.to_numpy(dtype=float),
                     fl.index.values.astype("datetime64[ns]"), fl.to_numpy(dtype=float), rec["ex"]))
    spy_m = None
    if spy_tr is not None and len(spy_tr):
        sp = spy_tr.dropna().sort_index()
        spy_m = {k: float(sp.asof(pd.Timestamp(v))) for k, v in end_of.items()}

    def last(ti, t):
        return int(np.searchsorted(ti, t, side="right")) - 1

    rows, prev_w = [], {}
    for j in range(1, len(ends)):
        f, h = np.datetime64(ends[j - 1].to_datetime64()), np.datetime64(ends[j].to_datetime64())
        t = (ends[j].year, ends[j].month)
        past = {_shift(t[0], t[1], k) for k in range(1, 13)}
        lag12, lag13 = _shift(t[0], t[1], 12), _shift(t[0], t[1], 13)
        e12, e13 = end_of.get(lag12), end_of.get(lag13)
        n_members = unquoted = 0
        r, D, r12 = {}, {}, {}
        for sym, ti, tv, fi, fv, ex in prep:
            k = last(fi, f)
            if k < 0 or fv[k] <= 0:
                continue
            q = last(ti, f)
            if q < 0 or (f - ti[q]) > STALE_DAYS * _DAY:
                continue
            n_members += 1
            qh = last(ti, h)
            if (h - ti[qh]) > STALE_DAYS * _DAY:
                unquoted += 1
            if not (ex & past):
                continue
            r[sym] = float(tv[qh] / tv[q] - 1.0) if qh > q else 0.0
            D[sym] = int(lag12 in ex)
            if e12 is not None and e13 is not None:
                a, b = last(ti, e13), last(ti, e12)
                if (a >= 0 and b > a and (e13 - ti[a]) <= STALE_DAYS * _DAY
                        and (e12 - ti[b]) <= STALE_DAYS * _DAY):
                    r12[sym] = float(tv[b] / tv[a] - 1.0)
        L = [s for s, d in D.items() if d == 1]
        Sh = [s for s, d in D.items() if d == 0]
        w = {}
        gross = np.nan
        if L and Sh:
            w.update({s: 1.0 / len(L) for s in L})
            w.update({s: -1.0 / len(Sh) for s in Sh})
            gross = float(np.mean([r[s] for s in L]) - np.mean([r[s] for s in Sh]))
        turnover = float(sum(abs(w.get(s, 0.0) - prev_w.get(s, 0.0)) for s in set(w) | set(prev_w)))
        z = np.nan
        ranked = sorted((v, s) for s, v in r12.items() if np.isfinite(v))
        n3 = len(ranked) // 3
        if n3 > 0:
            bottom, top = [s for _, s in ranked[:n3]], [s for _, s in ranked[-n3:]]
            z = float(np.mean([r[s] for s in top]) - np.mean([r[s] for s in bottom]))
        spy_ret = (spy_m[t] / spy_m[_shift(t[0], t[1], 1)] - 1.0) if (
            spy_m is not None and t in spy_m and _shift(t[0], t[1], 1) in spy_m) else np.nan
        rows.append({"hold": "%04d-%02d" % t, "calendar_month": t[1], "members": n_members, "payers": len(D),
                     "n_long": len(L), "n_short": len(Sh), "gross": gross, "turnover": turnover if w else np.nan,
                     "Z": z, "unquoted_end_share": unquoted / n_members if n_members else np.nan, "spy": spy_ret})
        if w:
            prev_w = w
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Evaluation
# --------------------------------------------------------------------------- #
def _win(m: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(m):
        return m
    return m[(m["hold"] >= lo) & (m["hold"] <= hi)]


def _valid(m: pd.DataFrame) -> pd.DataFrame:
    return m[np.isfinite(m["gross"].to_numpy(dtype=float))] if len(m) else m


def _net(m: pd.DataFrame, cost_bps: float) -> np.ndarray:
    v = _valid(m)
    if not len(v):
        return np.array([])
    return (v["gross"] - v["turnover"] * cost_bps * 1e-4).to_numpy(dtype=float)


def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"months": int(len(x)), "mean": None, "ann_net": None, "t_net": None, "p_net_one_sided": None,
                "sharpe": None, "max_dd": None}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, NW_LAG)
    return {"months": int(len(x)), "mean": float(x.mean()), "ann_net": float(x.mean() * PPY),
            "sharpe": float(x.mean() / sd * np.sqrt(PPY)) if sd > 0 else None, "t_net": _f(st["t"]),
            "p_net_one_sided": _f(st["p_one_sided"]), "max_dd": _f(S._max_dd(x)), "hit_rate": float((x > 0).mean())}


def increment(m: pd.DataFrame, cost_bps: float) -> dict:
    v = _valid(m)
    v = v[np.isfinite(v["Z"].to_numpy(dtype=float))] if len(v) else v
    net, z = _net(v, cost_bps), (v["Z"].to_numpy(dtype=float) if len(v) else np.array([]))
    if len(net) < 3 or float(np.var(z, ddof=1)) <= 0:
        return {"months": int(len(net)), "t": None, "ann": None}
    beta = float(np.cov(net, z, ddof=1)[0, 1] / np.var(z, ddof=1))
    a = net - beta * z
    st = S.nw_tstat(a, NW_LAG)
    return {"months": int(len(a)), "beta_on_seasonality_book": beta, "t": _f(st["t"]),
            "ann": float(a.mean() * PPY), "seasonality_book_ann_gross": float(z.mean() * PPY),
            "definition": "a = net - beta_hat * Z; nw_tstat(a, %d)" % NW_LAG}


def multiplicity(p: Optional[float]) -> dict:
    pv = {MECHANISM_ID: 1.0 if _f(p) is None else float(p)}
    pv.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS), "p_one_sided": _f(p),
            "passes": bool(bh["per_test"].get(MECHANISM_ID)), "single_survivor_threshold": BH_Q / bh["m"]}


def _rows(m: pd.DataFrame) -> list:
    v = _valid(m)
    return [[r["hold"], int(r["n_long"]), int(r["n_short"]), _f(r["gross"]), _f(r["turnover"]), _f(r["Z"])]
            for _, r in v.iterrows()]


def evaluate(m: pd.DataFrame, *, n_symbols: int, n_failed: int) -> dict:
    q, c = _win(m, *QUALIFICATION), _win(m, *CONFIRMATION)
    qv, cv = _valid(q), _valid(c)
    out: dict = {"qualification_window": QUALIFICATION, "confirmation_window": CONFIRMATION, "halves": HALVES,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"},
                 "untouched_confirmation": "NOT_READ"}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "kill_rule_fired": gate, "why": why})
        return out

    problems = []
    fail_share = n_failed / n_symbols if n_symbols else 1.0
    if fail_share > MAX_LOAD_FAIL_SHARE:
        problems.append("LOAD_FAILURES_%d_OF_%d" % (n_failed, n_symbols))
    if len(qv) < MIN_QUALIFICATION_MONTHS:
        problems.append("QUALIFICATION_MONTHS_%d_BELOW_%d" % (len(qv), MIN_QUALIFICATION_MONTHS))
    if len(cv) < MIN_CONFIRMATION_MONTHS:
        problems.append("CONFIRMATION_MONTHS_%d_BELOW_%d" % (len(cv), MIN_CONFIRMATION_MONTHS))
    mem_mean = float(q["members"].mean()) if len(q) else 0.0
    if not MEMBERS_BOUNDS[0] <= mem_mean <= MEMBERS_BOUNDS[1]:
        problems.append("MEMBERS_MEAN_%.1f_OUTSIDE_%s" % (mem_mean, MEMBERS_BOUNDS))
    both = pd.concat([q, c]) if len(q) or len(c) else m
    thin = both[(both["n_long"] < MIN_LONG) | (both["n_short"] < MIN_SHORT)] if len(both) else both
    if len(thin):
        problems.append("THIN_LEGS_IN_%d_MONTHS_FIRST_%s" % (len(thin), list(thin["hold"].head(3))))
    unq = float(both["unquoted_end_share"].mean()) if len(both) else 1.0
    if not np.isfinite(unq) or unq > MAX_UNQUOTED_END_SHARE:
        problems.append("UNQUOTED_AT_HOLDING_END_%.4f" % unq)
    out["data"] = {"n_symbols": n_symbols, "n_failed": n_failed, "qualification_months": int(len(qv)),
                   "confirmation_months": int(len(cv)), "members_mean_qualification": mem_mean,
                   "long_mean_qualification": float(qv["n_long"].mean()) if len(qv) else None,
                   "short_mean_qualification": float(qv["n_short"].mean()) if len(qv) else None,
                   "thin_months": int(len(thin)), "unquoted_end_share": unq, "problems": problems}
    if problems:
        return done("DATA_HOLD", "DATA", "DATA gate: %s" % "; ".join(problems[:8]))

    by_cost = {"%.1f" % k: stats(_net(q, k)) for k in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    net_q = _net(q, COST_PRIMARY_BPS)
    diag = {"mean_turnover_nav_per_month": float(qv["turnover"].mean()),
            "gross_mean_by_calendar_month": {int(k): float(v) for k, v in qv.groupby("calendar_month")["gross"].mean().items()},
            "seasonality_book_ann_gross": float(np.nanmean(qv["Z"].to_numpy(dtype=float)) * PPY)}
    spy = qv["spy"].to_numpy(dtype=float)
    ok = np.isfinite(spy) & np.isfinite(net_q)
    if ok.sum() > 3 and float(np.var(spy[ok], ddof=1)) > 0:
        diag["beta_to_spy"] = float(np.cov(net_q[ok], spy[ok], ddof=1)[0, 1] / np.var(spy[ok], ddof=1))
    out.update({"qualification_by_cost_bps_per_side": by_cost, "diagnostics_qualification": diag,
                "qualification_monthly_rows": _rows(q)})
    if prim.get("mean") is None or prim["mean"] <= 0.0:
        return done("NO_EDGE", "WRONG_SIGN", "mean net %s per month at %.1f bp is not in the frozen direction"
                    % (prim.get("mean"), COST_PRIMARY_BPS))
    if prim.get("t_net") is None or prim["t_net"] < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s < %.1f" % (prim.get("t_net"), T_FLOOR))
    if prim["ann_net"] < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "MATERIALITY", "annualised net %.4f < %.3f" % (prim["ann_net"], MATERIALITY_ANN_NET))
    inc = increment(q, COST_PRIMARY_BPS)
    out["increment_over_seasonality_book"] = inc
    if inc.get("t") is None or inc["t"] < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "INCREMENT", "increment t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {k: stats(_net(_win(q, lo, hi), COST_PRIMARY_BPS)).get("ann_net") for k, (lo, hi) in HALVES.items()}
    out["stability_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("NO_EDGE", "STABILITY", "non-positive annualised net in %s" % ", ".join(bad))
    if (prim.get("max_dd") or 0.0) < DRAWDOWN_FLOOR:
        return done("NO_EDGE", "DRAWDOWN", "max drawdown %s worse than %.2f" % (prim.get("max_dd"), DRAWDOWN_FLOOR))
    mult = multiplicity(prim.get("p_net_one_sided"))
    out["multiplicity"] = mult
    if not mult["passes"]:
        return done("NO_EDGE", "MULTIPLICITY", "fails BH q=%.2f at m=%d (p %s)" % (BH_Q, mult["m"], mult["p_one_sided"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "COST", "annualised net at %.0f bp %s" % (COST_STRESS_BPS, stress.get("ann_net")))
    cs = stats(_net(c, COST_PRIMARY_BPS))
    out["confirmation"] = {"state": "READ", "stats": cs,
                           "stats_by_cost_bps_per_side": {"%.1f" % k: stats(_net(c, k)) for k in COST_LADDER_BPS},
                           "increment": increment(c, COST_PRIMARY_BPS), "monthly_rows": _rows(c)}
    good = (cs.get("mean") is not None and cs["mean"] > 0.0 and cs.get("t_net") is not None
            and cs["t_net"] >= T_FLOOR and (cs.get("ann_net") or 0.0) >= MATERIALITY_ANN_NET)
    out["untouched_confirmation"] = "CONFIRMED" if good else "FAILED"
    if not good:
        return done("NO_EDGE", "CONFIRMATION", "untouched confirmation ann %s t %s did not reproduce"
                    % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched confirmation; a "
                                   "HUMAN gate governs prospective registration")


def design() -> dict:
    return {"watchlist": WATCHLIST, "index": INDEX, "stale_days": STALE_DAYS,
            "member": "latest S&P 500 flag on or before formation is 1 AND a close within 5 calendar days before",
            "payer": "member with an ex-dividend date in calendar months t-12..t-1",
            "long": "payers with an ex-dividend date in month t-12", "short": "payers without",
            "weights": "equal within each leg, dollar-neutral", "return": "total return, formation close to holding-end close",
            "control": "same-calendar-month return seasonality tercile book inside payers",
            "cost_primary_bps_per_side": COST_PRIMARY_BPS, "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "cost_stress_bps_per_side": COST_STRESS_BPS, "nw_lag": NW_LAG, "qualification": QUALIFICATION,
            "halves": HALVES, "confirmation": CONFIRMATION, "drawdown_floor": DRAWDOWN_FLOOR,
            "multiplicity": {"q": BH_Q, "inherited_nulls_at_p1": list(INHERITED_NULLS)}}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None) -> dict:
    inputs = load_universe() if inputs is None else inputs
    m = monthly_book(inputs["universe"], inputs["sessions"], inputs.get("spy_tr"))
    res = evaluate(m, n_symbols=int(inputs.get("n_symbols") or 0), n_failed=int(inputs.get("n_failed") or 0))
    sessions = pd.DatetimeIndex(inputs["sessions"])
    ident = "NORGATE:%s(TOTALRETURN,Dividend,%s flags):%d securities (%d failed):%s..%s" % (
        WATCHLIST, INDEX, int(inputs.get("n_symbols") or 0), int(inputs.get("n_failed") or 0),
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
    inc = res.get("increment_over_seasonality_book") or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("kill_rule_fired"),
            "statistic": {"lockbox_t": prim.get("t_net"), "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "months": prim.get("months"),
                          "increment_t": inc.get("t"), "untouched_confirmation": res.get("untouched_confirmation"),
                          "confirmation": {k: v for k, v in (res.get("confirmation") or {}).items() if k != "monthly_rows"}},
            "economics": {"ann_net_12_5bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"), "max_dd": prim.get("max_dd"),
                          "ann_net_25bp": stress.get("ann_net"), "increment_ann": inc.get("ann"),
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


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "monthly_book", "evaluate", "run", "run_mechanism",
           "multiplicity", "increment", "load_universe"]
