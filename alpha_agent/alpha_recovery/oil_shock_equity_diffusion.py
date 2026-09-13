r"""alpha_agent.alpha_recovery.oil_shock_equity_diffusion - executor for the Alpha Agent mechanism
``OIL_SHOCK_EQUITY_SLOW_DIFFUSION_V1``.

Contract: ``research/preregistration/OIL_SHOCK_EQUITY_DIFFUSION_PREREGISTRATION.md``.
One frozen cell: the sign of last month's roll-free ICE Brent futures return, negated, decides a
one-month SPY position entered at the first close of the next month. Gates run in the
preregistered order; gate 6 separates diffusion from monthly index reversal, which the census
showed is confounded with the oil shock from 2006.

Existing owners only: ``r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd`` and
``intraday_alpha.equal_risk_daily``. RESEARCH ONLY; ``capital_eligible`` is always False.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.oil_shock_equity_diffusion"
MECHANISM_ID = "OIL_SHOCK_EQUITY_SLOW_DIFFUSION_V1"
ARTIFACT_NAME = "oil_shock_equity_diffusion.json"
PREREGISTRATION = "research/preregistration/OIL_SHOCK_EQUITY_DIFFUSION_PREREGISTRATION.md"

OIL = "&BRN"
FROZEN_SIGN = -1.0
PERIODS_PER_YEAR = 12.0
COST_PRIMARY_BPS = 1.0
COST_LADDER_BPS = (1.0, 2.0, 5.0)
COST_STRESS_BPS = 5.0
QUALIFICATION = ("1993-03-01", "2012-12-31")
PARTITIONS = {"P1_1993_2005": ("1993-03-01", "2005-12-31"), "P2_2006_2012": ("2006-01-01", "2012-12-31")}
CONFIRMATION = ("2013-01-01", "2026-09-11")
MIN_FINITE_SHARE = 0.95
T_FLOOR = 2.0
EX2008_T_FLOOR = 1.0
BH_Q = 0.10
INHERITED_NULLS = ("R33|CROSS_MARKET_PANEL", "R34|ETF_PREDICTION_TO_PNL", "R63|CROSS_ASSET_TRANSMISSION",
                   "R63|INVENTORY_SURPRISE_EIA")
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


def leg_returns(unadjusted: pd.Series, ccb: pd.Series) -> pd.Series:
    j = pd.concat({"u": unadjusted, "c": ccb}, axis=1).dropna().sort_index()
    return (j["c"].diff() / j["u"].shift(1)).dropna()


def load_oil_returns() -> pd.Series:
    import norgatedata as nd
    kw = dict(padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return leg_returns(nd.price_timeseries(OIL, **kw)["Close"].astype(float),
                       nd.price_timeseries(OIL + "_CCB", **kw)["Close"].astype(float))


def load_spy_total_return() -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries("SPY", stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return df["Close"].astype(float).sort_index()


def periods(oil_r: pd.Series, spy_tr: pd.Series) -> pd.DataFrame:
    spy = spy_tr.dropna().sort_index()
    sd = spy.index
    months = sorted({(d.year, d.month) for d in sd})
    first = {ym: sd[(sd.year == ym[0]) & (sd.month == ym[1])][0] for ym in months}
    last = {ym: sd[(sd.year == ym[0]) & (sd.month == ym[1])][-1] for ym in months}
    rows = []
    for k in range(1, len(months) - 2):
        prev, m, n1, n2 = months[k - 1], months[k], months[k + 1], months[k + 2]
        o = oil_r[(oil_r.index.year == m[0]) & (oil_r.index.month == m[1])]
        oil_m = float(np.prod(1.0 + o.to_numpy()) - 1.0) if len(o) else np.nan
        spy_m = float(spy[last[m]] / spy[last[prev]] - 1.0)
        entry, exit_ = first[n1], first[n2]
        r = float(spy[exit_] / spy[entry] - 1.0)
        pos = FROZEN_SIGN * float(np.sign(oil_m)) if np.isfinite(oil_m) else np.nan
        rows.append({"signal_month": pd.Timestamp(year=m[0], month=m[1], day=1), "entry_date": entry,
                     "exit_date": exit_, "oil_m": oil_m, "spy_m": spy_m, "r_spy": r, "pos": pos,
                     "rev": -float(np.sign(spy_m)) * r})
    return pd.DataFrame(rows)


def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    return d[(d["entry_date"] >= pd.Timestamp(lo)) & (d["entry_date"] <= pd.Timestamp(hi))]


def _finite(d: pd.DataFrame) -> pd.DataFrame:
    return d[np.isfinite(d["pos"].to_numpy(dtype=float))] if len(d) else d


def net(d: pd.DataFrame, cost_bps: float) -> np.ndarray:
    f = _finite(d)
    if not len(f):
        return np.array([])
    return (f["pos"] * f["r_spy"] - f["pos"].abs() * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


def stats(x: np.ndarray) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"periods": int(len(x))}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"periods": int(len(x)), "ann_net": float(x.mean() * PERIODS_PER_YEAR),
            "sharpe": float(x.mean() / sd * np.sqrt(PERIODS_PER_YEAR)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"], "max_dd": S._max_dd(x),
            "hit_rate": float((x > 0).mean())}


def increment(d: pd.DataFrame, cost_bps: float, controls: tuple) -> dict:
    f = _finite(d)
    y = net(f, cost_bps)
    if len(y) < 5:
        return {"periods": int(len(y)), "t": None, "ann": None}
    X = np.column_stack([f[c].to_numpy(dtype=float) for c in controls])
    beta, *_ = np.linalg.lstsq(X - X.mean(axis=0), y - y.mean(), rcond=None)
    a = y - X @ beta
    st = S.nw_tstat(a, 0)
    return {"periods": int(len(a)), "controls": list(controls), "betas": [float(b) for b in beta],
            "t": st["t"], "ann": float(a.mean() * PERIODS_PER_YEAR),
            "definition": "a = net - X beta_hat (slopes from demeaned least squares); nw_tstat(a, 0)"}


def _incumbent(d: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    f = _finite(d)
    s = pd.Series(net(f, COST_PRIMARY_BPS), index=pd.to_datetime(f["entry_date"]))
    daily = s.apply(lambda r: (1.0 + r) ** (1.0 / 21.0) - 1.0)
    daily = daily.reindex(pd.bdate_range(daily.index.min(), daily.index.max())).ffill(limit=20).dropna()
    return equal_risk_daily(daily, incumbent_daily, label=MECHANISM_ID)


def evaluate(oil_r: pd.Series, spy_tr: pd.Series, *, incumbent_daily: Optional[pd.Series] = None) -> dict:
    d = periods(oil_r, spy_tr)
    q = _window(d, *QUALIFICATION)
    out: dict = {"n_periods_total": int(len(d)), "qualification_window": QUALIFICATION,
                 "confirmation_window": CONFIRMATION,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    finite_share = float(np.isfinite(q["pos"].to_numpy(dtype=float)).mean()) if len(q) else 0.0
    parts = {k: _finite(_window(q, *w)) for k, w in PARTITIONS.items()}
    out["data"] = {"qualification_periods": int(len(q)), "finite_share": finite_share,
                   "partition_periods": {k: int(len(v)) for k, v in parts.items()}}
    if finite_share < MIN_FINITE_SHARE or min(len(v) for v in parts.values()) < MIN_EFFECTIVE_PERIODS:
        return done("DATA_HOLD", "DATA", "finite share %.3f, partition periods %s"
                    % (finite_share, {k: len(v) for k, v in parts.items()}))
    by_cost = {"%.1f" % c: stats(net(q, c)) for c in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["qualification_gross"] = stats(net(q, 0.0))
    n1 = net(q, COST_PRIMARY_BPS)
    if float(np.mean(n1)) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN",
                    "mean net %.6f per month is not in the frozen NEGATIVE direction" % float(np.mean(n1)))
    if prim.get("t_net") is None or float(prim["t_net"]) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s below %.1f" % (prim.get("t_net"), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY", "annualised net %.4f" % prim["ann_net"])
    inc_p = increment(q, COST_PRIMARY_BPS, ("r_spy",))
    out["increment_over_passive_long"] = inc_p
    if inc_p.get("t") is None or float(inc_p["t"]) < T_FLOOR or (inc_p.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "PASSIVE_LONG_INCREMENT",
                    "increment over a passive long t %s ann %s" % (inc_p.get("t"), inc_p.get("ann")))
    inc_r = increment(q, COST_PRIMARY_BPS, ("r_spy", "rev"))
    out["increment_over_passive_and_reversal"] = inc_r
    if inc_r.get("t") is None or float(inc_r["t"]) < T_FLOOR or (inc_r.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "REVERSAL_CONFOUND",
                    "increment after controlling for prior-month SPY reversal t %s ann %s"
                    % (inc_r.get("t"), inc_r.get("ann")))
    stab = {k: stats(net(v, COST_PRIMARY_BPS)).get("ann_net") for k, v in parts.items()}
    out["partition_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("KILLED_UNSTABLE", "PARTITION_STABILITY", "non-positive annualised net in %s" % ", ".join(bad))
    ex08 = stats(net(_finite(q)[_finite(q)["entry_date"].dt.year != 2008], COST_PRIMARY_BPS))
    out["excluding_2008"] = ex08
    if ex08.get("t_net") is None or float(ex08["t_net"]) < EX2008_T_FLOOR:
        return done("KILLED_UNSTABLE", "NOT_2008", "t excluding 2008 %s below %.1f" % (ex08.get("t_net"), EX2008_T_FLOOR))
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
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST", "annualised net at 5 bp %.4f" % (stress.get("ann_net") or 0.0))
    ir = _incumbent(q, incumbent_daily)
    out["incumbent_equal_risk"] = ir
    if ir.get("state") == "OK" and not ir.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT",
                    "equal-risk increment %s t %s" % (ir.get("incremental_ann_net_return"), ir.get("t_incremental")))
    c = _window(d, *CONFIRMATION)
    c = c[c["exit_date"] <= pd.Timestamp(CONFIRMATION[1])] if len(c) else c
    cs = stats(net(c, COST_PRIMARY_BPS))
    out["confirmation"] = {"state": "READ", "stats": cs}
    ok = (cs.get("t_net") is not None and float(np.mean(net(c, COST_PRIMARY_BPS))) > 0.0
          and cs["ann_net"] >= MATERIALITY_ANN_NET and float(cs["t_net"]) >= T_FLOOR
          and (cs.get("p_net_one_sided") or 1.0) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION",
                    "confirmation ann %s t %s did not reproduce" % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched "
                                   "confirmation; a HUMAN gate governs prospective registration")


def run(*, verbose: bool = True, write: bool = True, oil_r: Optional[pd.Series] = None,
        spy_tr: Optional[pd.Series] = None, incumbent_daily="LOAD") -> dict:
    oil_r = load_oil_returns() if oil_r is None else oil_r
    spy_tr = load_spy_total_return() if spy_tr is None else spy_tr
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    res = evaluate(oil_r, spy_tr, incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"oil": OIL, "oil_return": "diff(&BRN_CCB close) / prior &BRN close",
                       "sign": FROZEN_SIGN, "entry": "first SPY session of the next month",
                       "exit": "first SPY session of the month after", "cost_ladder_bps_per_side": list(COST_LADDER_BPS)},
            "input_data_identity": "NORGATE:%s(+_CCB):%s..%s|NORGATE_SPY_TOTALRETURN:%s..%s" % (
                OIL, str(oil_r.index.min().date()), str(oil_r.index.max().date()),
                str(spy_tr.index.min().date()), str(spy_tr.index.max().date())),
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
                "artifact": None, "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
            "statistic": {"lockbox_t": prim.get("t_net"), "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "periods": prim.get("periods"),
                          "reversal_controlled_t": (res.get("increment_over_passive_and_reversal") or {}).get("t"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_1bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                          "max_dd": prim.get("max_dd"), "partition_ann_net": res.get("partition_ann_net"),
                          "qualification_gross": res.get("qualification_gross")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body["input_data_identity"]}
