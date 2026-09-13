r"""alpha_agent.alpha_recovery.month_end_rebalancing_flow - executor for the Alpha Agent
mechanism ``MONTH_END_BALANCED_REBALANCING_FLOW_V1``.

Contract: ``research/preregistration/MONTH_END_REBALANCING_FLOW_PREREGISTRATION.md``.
One frozen cell: sign an equal-volatility ES-versus-ZN futures spread AGAINST the month-to-date
equity-minus-bond return at the close before the rebalancing window, enter at the next close,
hold through the first session of the next month. Leg returns are roll-free
(``diff(_CCB close) / prior unadjusted close``) and validated against SPY and IEF total return.

Existing owners only: ``r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd`` and
``intraday_alpha.equal_risk_daily``. RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.month_end_rebalancing_flow"
MECHANISM_ID = "MONTH_END_BALANCED_REBALANCING_FLOW_V1"
ARTIFACT_NAME = "month_end_rebalancing_flow.json"
PREREGISTRATION = "research/preregistration/MONTH_END_REBALANCING_FLOW_PREREGISTRATION.md"

EQUITY, BOND = "&ES", "&ZN"
VALIDATION = {"&ES": ("SPY", 0.95), "&ZN": ("IEF", 0.85)}
VALIDATION_SPAN = ("2002-08-01", "2014-12-31")
MIN_MONTH_SESSIONS = 8
VOL_LOOKBACK = 63
VOL_TARGET = 0.10
WINDOWS_PER_YEAR = 12.0
COST_PRIMARY_BPS = 1.0
COST_LADDER_BPS = (1.0, 2.0, 5.0)
COST_STRESS_BPS = 5.0
QUALIFICATION = ("1998-01-01", "2014-12-31")
HALVES = {"H1_1998_2006": ("1998-01-01", "2006-12-31"), "H2_2007_2014": ("2007-01-01", "2014-12-31")}
CONFIRMATION = ("2015-01-01", "2026-08-31")
MIN_FINITE_SHARE = 0.95
T_FLOOR = 2.0
BH_Q = 0.10
INHERITED_NULLS = ("R32|TURN_OF_MONTH_DUMMY", "R32|QUARTER_END_DUMMY", "R32|TRIPLE_WITCHING_DUMMY",
                   "STAGE14|SAME_MONTH_SEASONALITY", "R46|r46_3_spx_turn_of_month")
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def leg_returns(unadjusted: pd.Series, ccb: pd.Series) -> pd.Series:
    """Roll-free daily return: the difference-adjusted change over the prior unadjusted close."""
    j = pd.concat({"u": unadjusted, "c": ccb}, axis=1).dropna().sort_index()
    return (j["c"].diff() / j["u"].shift(1)).dropna()


def load_futures(symbol: str) -> pd.Series:
    import norgatedata as nd
    kw = dict(padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    u = nd.price_timeseries(symbol, **kw)["Close"].astype(float)
    c = nd.price_timeseries(symbol + "_CCB", **kw)["Close"].astype(float)
    return leg_returns(u, c)


def load_etf_total_return(symbol: str) -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries(symbol, stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return df["Close"].astype(float).pct_change().dropna()


def validate(eq: pd.Series, bd: pd.Series, etf: dict) -> dict:
    out = {}
    for sym, r in ((EQUITY, eq), (BOND, bd)):
        name, floor = VALIDATION[sym]
        e = etf.get(name)
        if e is None:
            out[sym] = {"against": name, "corr": None, "floor": floor, "passes": False}
            continue
        j = pd.concat({"f": r, "e": e}, axis=1).dropna().loc[VALIDATION_SPAN[0]:VALIDATION_SPAN[1]]
        corr = float(j["f"].corr(j["e"])) if len(j) > 50 else None
        out[sym] = {"against": name, "corr": corr, "floor": floor,
                    "passes": bool(corr is not None and corr >= floor), "sessions": int(len(j))}
    return out


# --------------------------------------------------------------------------- #
# The frozen cell
# --------------------------------------------------------------------------- #
def windows(eq: pd.Series, bd: pd.Series) -> pd.DataFrame:
    j = pd.concat({"eq": eq, "bd": bd}, axis=1).dropna().sort_index()
    idx = (1.0 + j).cumprod()
    vol = j.rolling(VOL_LOOKBACK, min_periods=VOL_LOOKBACK).std() * np.sqrt(252.0)
    dates = j.index
    pos = {d: i for i, d in enumerate(dates)}
    groups = [g for _, g in pd.Series(dates, index=dates).groupby([dates.year, dates.month])]
    rows = []
    for k in range(1, len(groups) - 1):
        L, prev, nxt = groups[k].index, groups[k - 1].index, groups[k + 1].index
        if len(L) < MIN_MONTH_SESSIONS:
            continue
        s, e, x, p = L[-5], L[-4], nxt[0], prev[-1]
        mtd = (idx.at[s, "eq"] / idx.at[p, "eq"] - 1.0) - (idx.at[s, "bd"] / idx.at[p, "bd"] - 1.0)
        se, sb = vol.at[s, "eq"], vol.at[s, "bd"]
        if not (np.isfinite(mtd) and np.isfinite(se) and np.isfinite(sb) and se > 0 and sb > 0):
            rows.append({"signal_date": s, "entry_date": e, "exit_date": x, "finite": False,
                         "quarter_end": s.month in (3, 6, 9, 12), "sessions_held": pos[x] - pos[e]})
            continue
        sign = float(np.sign(mtd))
        w_eq, w_bd = -sign * VOL_TARGET / se, sign * VOL_TARGET / sb
        r_eq = idx.at[x, "eq"] / idx.at[e, "eq"] - 1.0
        r_bd = idx.at[x, "bd"] / idx.at[e, "bd"] - 1.0
        rows.append({"signal_date": s, "entry_date": e, "exit_date": x, "finite": True,
                     "mtd_rel": mtd, "w_eq": w_eq, "w_bd": w_bd, "gross": w_eq * r_eq + w_bd * r_bd,
                     "uncond": VOL_TARGET / se * r_eq - VOL_TARGET / sb * r_bd,
                     "turnover": abs(w_eq) + abs(w_bd), "quarter_end": s.month in (3, 6, 9, 12),
                     "sessions_held": pos[x] - pos[e]})
    return pd.DataFrame(rows)


def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    return d[(d["signal_date"] >= pd.Timestamp(lo)) & (d["signal_date"] <= pd.Timestamp(hi))]


def _net(d: pd.DataFrame, cost_bps: float) -> np.ndarray:
    ok = d[d["finite"]] if len(d) else d
    if not len(ok):
        return np.array([])
    return (ok["gross"] - ok["turnover"] * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


def stats(x: np.ndarray) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"windows": int(len(x))}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"windows": int(len(x)), "ann_net": float(x.mean() * WINDOWS_PER_YEAR),
            "sharpe": float(x.mean() / sd * np.sqrt(WINDOWS_PER_YEAR)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"], "max_dd": S._max_dd(x),
            "hit_rate": float((x > 0).mean())}


def timing_increment(d: pd.DataFrame, cost_bps: float) -> dict:
    ok = d[d["finite"]]
    net, u = _net(ok, cost_bps), ok["uncond"].to_numpy(dtype=float)
    if len(net) < 3 or float(np.var(u)) <= 0:
        return {"windows": int(len(net)), "t": None, "ann": None}
    beta = float(np.cov(net, u, ddof=1)[0, 1] / np.var(u, ddof=1))
    a = net - beta * u
    st = S.nw_tstat(a, 0)
    return {"windows": int(len(a)), "beta_on_unconditional": beta, "t": st["t"],
            "ann": float(a.mean() * WINDOWS_PER_YEAR), "definition": "a = net - beta_hat * U; nw_tstat(a, 0)"}


def _incumbent(d: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    ok = d[d["finite"]]
    parts = []
    for _, row in ok.iterrows():
        n = max(1, int(row["sessions_held"]))
        r = float(row["gross"] - row["turnover"] * 2.0 * COST_PRIMARY_BPS * 1e-4)
        days = pd.bdate_range(row["entry_date"], periods=n + 1)[1:]
        parts.append(pd.Series((1.0 + r) ** (1.0 / n) - 1.0, index=days))
    if not parts:
        return {"state": "DATA_HOLD", "why": "no finite window"}
    sleeve = pd.concat(parts).groupby(level=0).sum()
    sleeve = sleeve.reindex(pd.bdate_range(sleeve.index.min(), sleeve.index.max())).fillna(0.0)
    return equal_risk_daily(sleeve, incumbent_daily, label=MECHANISM_ID)


def evaluate(eq: pd.Series, bd: pd.Series, *, etf: dict,
             incumbent_daily: Optional[pd.Series] = None) -> dict:
    d = windows(eq, bd)
    q = _window(d, *QUALIFICATION)
    out: dict = {"n_windows_total": int(len(d)), "qualification_window": QUALIFICATION,
                 "confirmation_window": CONFIRMATION,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    val = validate(eq, bd, etf)
    finite_share = float(q["finite"].astype(bool).mean()) if len(q) else 0.0
    fin = q["finite"].astype(bool) if len(q) else None
    qe = q["quarter_end"].fillna(False).astype(bool) if len(q) else None
    groups = {"QUARTER_END": q[fin & qe] if len(q) else q,
              "OTHER_MONTHS": q[fin & ~qe] if len(q) else q}
    out["data"] = {"qualification_windows": int(len(q)), "finite_share": finite_share,
                   "group_windows": {k: int(len(v)) for k, v in groups.items()}, "validation": val}
    if (finite_share < MIN_FINITE_SHARE or min(len(v) for v in groups.values()) < MIN_EFFECTIVE_PERIODS
            or not all(v["passes"] for v in val.values())):
        return done("DATA_HOLD", "DATA", "finite share %.3f, group windows %s, validation %s"
                    % (finite_share, {k: len(v) for k, v in groups.items()},
                       {k: v["corr"] for k, v in val.items()}))

    by_cost = {"%.1f" % c: stats(_net(q, c)) for c in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["qualification_gross"] = stats(_net(q, 0.0))
    net1 = _net(q, COST_PRIMARY_BPS)
    if float(np.mean(net1)) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN",
                    "mean net %.6f per window is not in the frozen direction" % float(np.mean(net1)))
    if prim.get("t_net") is None or float(prim["t_net"]) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s below %.1f" % (prim.get("t_net"), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY", "annualised net %.4f" % prim["ann_net"])
    inc = timing_increment(q, COST_PRIMARY_BPS)
    out["increment_over_unconditional_month_end"] = inc
    if inc.get("t") is None or float(inc["t"]) < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "UNCONDITIONAL_INCREMENT",
                    "timing increment t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {k: stats(_net(v, COST_PRIMARY_BPS)).get("ann_net") for k, v in groups.items()}
    stab.update({k: stats(_net(_window(q, *w), COST_PRIMARY_BPS)).get("ann_net") for k, w in HALVES.items()})
    out["stability_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("KILLED_UNSTABLE", "STABILITY", "non-positive annualised net in %s" % ", ".join(bad))
    pvals = {MECHANISM_ID: prim.get("p_net_one_sided")}
    pvals.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pvals, BH_Q)
    out["multiplicity"] = {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
                           "p_one_sided": prim.get("p_net_one_sided"),
                           "passes": bool(bh["per_test"].get(MECHANISM_ID))}
    if not out["multiplicity"]["passes"]:
        return done("KILLED_MULTIPLICITY", "BH_INHERITED", "fails BH q=%.2f at m=%d" % (BH_Q, bh["m"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST", "annualised net at 5 bp %.4f"
                    % (stress.get("ann_net") or 0.0))
    ir = _incumbent(q, incumbent_daily)
    out["incumbent_equal_risk"] = ir
    if ir.get("state") == "OK" and not ir.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT",
                    "equal-risk increment %s t %s" % (ir.get("incremental_ann_net_return"),
                                                     ir.get("t_incremental")))
    c = _window(d, *CONFIRMATION)
    cs = stats(_net(c, COST_PRIMARY_BPS))
    out["confirmation"] = {"state": "READ", "stats": cs}
    ok = (cs.get("t_net") is not None and float(np.mean(_net(c, COST_PRIMARY_BPS))) > 0.0
          and cs["ann_net"] >= MATERIALITY_ANN_NET and float(cs["t_net"]) >= T_FLOOR
          and (cs.get("p_net_one_sided") or 1.0) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION",
                    "confirmation ann %s t %s did not reproduce" % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched "
                                   "confirmation; a HUMAN gate governs prospective registration")


def run(*, verbose: bool = True, write: bool = True, eq: Optional[pd.Series] = None,
        bd: Optional[pd.Series] = None, etf: Optional[dict] = None,
        incumbent_daily="LOAD") -> dict:
    eq = load_futures(EQUITY) if eq is None else eq
    bd = load_futures(BOND) if bd is None else bd
    etf = {"SPY": load_etf_total_return("SPY"), "IEF": load_etf_total_return("IEF")} if etf is None else etf
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    res = evaluate(eq, bd, etf=etf, incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"legs": [EQUITY, BOND], "leg_return": "diff(_CCB close) / prior unadjusted close",
                       "window": "signal L[-5], entry L[-4], exit first session of next month",
                       "vol_lookback": VOL_LOOKBACK, "vol_target": VOL_TARGET,
                       "cost_ladder_bps_per_side": list(COST_LADDER_BPS)},
            "input_data_identity": "NORGATE:%s,%s(+_CCB):%s..%s" % (
                EQUITY, BOND, str(min(eq.index.min(), bd.index.min()).date()),
                str(max(eq.index.max(), bd.index.max()).date())),
            "result": res, "capital_eligible": False}
    if write:
        body["artifact_path"] = str(write_artifact(ARTIFACT_NAME, body))
    if verbose:
        print("%s -> %s (%s)" % (MECHANISM_ID, res["verdict"], res["why"]), flush=True)
    return body


def run_mechanism(*, mechanism: dict) -> dict:
    """The Alpha Agent executor contract (``alpha_agent.r59.mechanisms.validate_result``)."""
    if (mechanism or {}).get("mechanism_id") != MECHANISM_ID:
        raise ValueError("this executor implements %s only" % MECHANISM_ID)
    try:
        body = run(verbose=True, write=True)
    except ImportError as exc:
        return {"verdict": "DATA_HOLD", "why": "the Norgate source is unavailable: %s" % exc,
                "kill_rule_fired": "DATA", "statistic": {}, "economics": {}, "multiplicity": {},
                "artifact": None, "capital_eligible": False, "scorer": SCORER,
                "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
            "statistic": {"lockbox_t": prim.get("t_net"),
                          "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "windows": prim.get("windows"),
                          "unconditional_increment_t": (res.get("increment_over_unconditional_month_end")
                                                        or {}).get("t"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_1bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                          "max_dd": prim.get("max_dd"), "stability_ann_net": res.get("stability_ann_net"),
                          "qualification_gross": res.get("qualification_gross")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q,
                                                        "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body["input_data_identity"]}
