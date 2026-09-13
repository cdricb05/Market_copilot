r"""alpha_agent.alpha_recovery.relative_index_implied_vol - executor for the Alpha Agent mechanism
``RELATIVE_INDEX_IMPLIED_VOL_HEDGING_DEMAND_V1``.

Contract: ``research/preregistration/RELATIVE_INDEX_IMPLIED_VOL_PREREGISTRATION.md``.
One book, two declared legs: for the Nasdaq-100 (VXN, QQQ) and the Russell 2000 (RVX, IWM), the
implied-minus-realised volatility premium RELATIVE to the S&P 500 (VIX, SPY) is z-scored against
strictly prior observations; a rich relative premium shorts that index against SPY for 21
sessions, entered at the next close. Gates run in the preregistered order.

Existing owners only: ``options_surface._z``, ``r63.sensitivity.nw_tstat`` / ``bh_fdr`` /
``_max_dd`` and ``intraday_alpha.equal_risk_daily``. RESEARCH ONLY; ``capital_eligible`` is
always False.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.relative_index_implied_vol"
MECHANISM_ID = "RELATIVE_INDEX_IMPLIED_VOL_HEDGING_DEMAND_V1"
ARTIFACT_NAME = "relative_index_implied_vol.json"
PREREGISTRATION = "research/preregistration/RELATIVE_INDEX_IMPLIED_VOL_PREREGISTRATION.md"

IV_CACHE_SUBDIR = "_data_implied_vol_indices"
CBOE_VIX = Path(r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41\_data_cboe\VIX_History.csv")
LEGS = {"NDX": ("VXNCLS", "QQQ"), "RUT": ("RVXCLS", "IWM")}
BASE_IV, BASE_ETF = "VIXCLS", "SPY"
HORIZON = 21
ENTRY_DELAY = 1
RV_LOOKBACK = 21
PERIODS_PER_YEAR = 252.0 / HORIZON
LEG_WEIGHT = 0.5
COST_PRIMARY_BPS = 2.0
COST_LADDER_BPS = (1.0, 2.0, 5.0)
COST_STRESS_BPS = 5.0
QUALIFICATION = ("2004-01-01", "2019-12-31")
CONFIRMATION = ("2020-01-01", "2026-08-31")
MIN_INTEGRITY_CORR = 0.999
MIN_FINITE_SHARE = 0.95
T_FLOOR = 2.0
BH_Q = 0.10
INHERITED_NULLS = ("SPY_OPRA|ATM_IV_LEVEL|h1", "SPY_OPRA|ATM_IV_LEVEL|h5", "SPY_OPRA|PUT_CALL_SKEW|h1",
                   "SPY_OPRA|PUT_CALL_SKEW|h5", "SPY_OPRA|VARIANCE_RISK_PREMIUM|h1",
                   "SPY_OPRA|VARIANCE_RISK_PREMIUM|h5")
SCORER = ("alpha_agent.alpha_recovery.options_surface._z + alpha_agent.r63.sensitivity."
          "nw_tstat/bh_fdr/_max_dd + alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_fred(series_id: str) -> pd.Series:
    p = research_root() / IV_CACHE_SUBDIR / ("%s.csv" % series_id)
    df = pd.read_csv(p)
    return pd.Series(pd.to_numeric(df["value"], errors="coerce").to_numpy(),
                     index=pd.to_datetime(df["date"])).sort_index()


def load_cboe_vix(path: Path = CBOE_VIX) -> pd.Series:
    df = pd.read_csv(path)
    col = "CLOSE" if "CLOSE" in df.columns else [c for c in df.columns if c != "DATE"][-1]
    return pd.Series(df[col].astype(float).to_numpy(),
                     index=pd.to_datetime(df["DATE"], format="%m/%d/%Y")).sort_index()


def load_total_return(symbol: str) -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries(symbol, stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return df["Close"].astype(float).sort_index()


def integrity(fred_vix: pd.Series, cboe_vix: pd.Series) -> dict:
    j = pd.concat({"f": fred_vix, "c": cboe_vix}, axis=1).dropna()
    corr = float(j["f"].corr(j["c"])) if len(j) > 250 else None
    return {"sessions": int(len(j)), "corr": corr, "floor": MIN_INTEGRITY_CORR,
            "passes": bool(corr is not None and corr >= MIN_INTEGRITY_CORR)}


def build_panel(iv: dict, tr: dict) -> pd.DataFrame:
    """The SPY calendar with each leg's relative premium z-score (strictly prior)."""
    from .options_surface import _z
    sessions = tr[BASE_ETF].dropna().index
    px = pd.concat({k: v.reindex(sessions) for k, v in tr.items()}, axis=1)
    lr = np.log(px).diff()
    rv = lr.shift(1).rolling(RV_LOOKBACK, min_periods=RV_LOOKBACK).std() * np.sqrt(252.0)
    base = iv[BASE_IV].reindex(sessions) / 100.0 - rv[BASE_ETF]
    out = pd.DataFrame({"tr_" + k: px[k] for k in px.columns}, index=sessions)
    for leg, (iv_id, etf) in LEGS.items():
        rip = (iv[iv_id].reindex(sessions) / 100.0 - rv[etf]) - base
        out["rip_" + leg] = rip
        out["z_" + leg] = _z(rip)
    return out


def decisions(panel: pd.DataFrame) -> pd.DataFrame:
    zcols = ["z_" + k for k in LEGS]
    both = np.isfinite(panel[zcols].to_numpy()).all(axis=1)
    first = np.where(both)[0]
    rows = []
    if len(first):
        i, n = int(first[0]), len(panel)
        while i + ENTRY_DELAY + HORIZON < n:
            e, x = i + ENTRY_DELAY, i + ENTRY_DELAY + HORIZON
            spy = panel["tr_" + BASE_ETF].iloc[x] / panel["tr_" + BASE_ETF].iloc[e] - 1.0
            row = {"decision_date": panel.index[i], "entry_date": panel.index[e],
                   "exit_date": panel.index[x], "both_finite": bool(both[i])}
            book = passive = turnover = 0.0
            for leg, (_, etf) in LEGS.items():
                z = panel["z_" + leg].iloc[i]
                s = -float(np.sign(z)) if np.isfinite(z) else 0.0
                rel = panel["tr_" + etf].iloc[x] / panel["tr_" + etf].iloc[e] - 1.0 - spy
                row["s_" + leg], row["rel_" + leg], row["leg_" + leg] = s, rel, s * rel
                book += LEG_WEIGHT * s * rel
                passive += LEG_WEIGHT * rel
                turnover += LEG_WEIGHT * abs(s) * 2.0
            row.update({"book_gross": book, "passive": passive, "turnover": turnover})
            rows.append(row)
            i += HORIZON
    return pd.DataFrame(rows)


def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    return d[(d["entry_date"] >= pd.Timestamp(lo)) & (d["entry_date"] <= pd.Timestamp(hi))]


def book_net(d: pd.DataFrame, cost_bps: float) -> np.ndarray:
    if not len(d):
        return np.array([])
    return (d["book_gross"] - d["turnover"] * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


def leg_net(d: pd.DataFrame, leg: str, cost_bps: float) -> np.ndarray:
    if not len(d):
        return np.array([])
    s = d["s_" + leg]
    return (d["leg_" + leg] - s.abs() * 2.0 * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


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


def timing_increment(d: pd.DataFrame, cost_bps: float) -> dict:
    net, p = book_net(d, cost_bps), d["passive"].to_numpy(dtype=float) if len(d) else np.array([])
    if len(net) < 3 or float(np.var(p)) <= 0:
        return {"periods": int(len(net)), "t": None, "ann": None}
    beta = float(np.cov(net, p, ddof=1)[0, 1] / np.var(p, ddof=1))
    a = net - beta * p
    st = S.nw_tstat(a, 0)
    return {"periods": int(len(a)), "beta_on_passive_relative_book": beta, "t": st["t"],
            "ann": float(a.mean() * PERIODS_PER_YEAR), "definition": "a = net - beta_hat * P; nw_tstat(a, 0)"}


def _incumbent(d: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    s = pd.Series(book_net(d, COST_PRIMARY_BPS), index=pd.to_datetime(d["entry_date"]))
    daily = s.apply(lambda r: (1.0 + r) ** (1.0 / HORIZON) - 1.0)
    daily = daily.reindex(pd.bdate_range(daily.index.min(), daily.index.max()))
    daily = daily.ffill(limit=HORIZON - 1).dropna()
    return equal_risk_daily(daily, incumbent_daily, label=MECHANISM_ID)


def evaluate(panel: pd.DataFrame, *, integrity_check: dict,
             incumbent_daily: Optional[pd.Series] = None) -> dict:
    d = decisions(panel)
    q = _window(d, *QUALIFICATION)
    out: dict = {"n_decisions_total": int(len(d)), "qualification_window": QUALIFICATION,
                 "confirmation_window": CONFIRMATION, "integrity": integrity_check,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    finite_share = float(q["both_finite"].astype(bool).mean()) if len(q) else 0.0
    leg_counts = {k: int((q["s_" + k] != 0).sum()) if len(q) else 0 for k in LEGS}
    out["data"] = {"qualification_decisions": int(len(q)), "both_finite_share": finite_share,
                   "leg_engaged_decisions": leg_counts}
    if (not integrity_check.get("passes") or finite_share < MIN_FINITE_SHARE
            or min(leg_counts.values()) < MIN_EFFECTIVE_PERIODS):
        return done("DATA_HOLD", "DATA", "integrity %s, both-finite share %.3f, leg decisions %s"
                    % (integrity_check.get("corr"), finite_share, leg_counts))

    by_cost = {"%.1f" % c: stats(book_net(q, c)) for c in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    out["qualification_gross"] = stats(book_net(q, 0.0))
    net = book_net(q, COST_PRIMARY_BPS)
    if float(np.mean(net)) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN",
                    "book mean net %.6f is not in the frozen direction" % float(np.mean(net)))
    if prim.get("t_net") is None or float(prim["t_net"]) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "book NW t %s below %.1f" % (prim.get("t_net"), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY", "annualised net %.4f" % prim["ann_net"])
    legs = {k: stats(leg_net(q, k, COST_PRIMARY_BPS)) for k in LEGS}
    out["legs"] = legs
    bad = sorted(k for k, v in legs.items() if (v.get("ann_net") or 0.0) <= 0.0)
    if bad:
        return done("KILLED_UNSTABLE", "LEG_AGREEMENT", "non-positive annualised net in leg %s" % ", ".join(bad))
    inc = timing_increment(q, COST_PRIMARY_BPS)
    out["timing_increment"] = inc
    if inc.get("t") is None or float(inc["t"]) < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "TIMING_INCREMENT",
                    "increment over the passive relative book t %s ann %s" % (inc.get("t"), inc.get("ann")))
    pvals = {k: legs[k].get("p_net_one_sided") for k in LEGS}
    pvals.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pvals, BH_Q)
    out["multiplicity"] = {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
                           "per_leg_p_one_sided": {k: pvals[k] for k in LEGS},
                           "passes": all(bool(bh["per_test"].get(k)) for k in LEGS)}
    if not out["multiplicity"]["passes"]:
        return done("KILLED_MULTIPLICITY", "BH_INHERITED", "a leg fails BH q=%.2f at m=%d" % (BH_Q, bh["m"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST", "annualised net at 5 bp %.4f"
                    % (stress.get("ann_net") or 0.0))
    ir = _incumbent(q, incumbent_daily)
    out["incumbent_equal_risk"] = ir
    if ir.get("state") == "OK" and not ir.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT",
                    "equal-risk increment %s t %s" % (ir.get("incremental_ann_net_return"), ir.get("t_incremental")))
    c = _window(d, *CONFIRMATION)
    c = c[c["exit_date"] <= pd.Timestamp(CONFIRMATION[1])] if len(c) else c
    cs = stats(book_net(c, COST_PRIMARY_BPS))
    out["confirmation"] = {"state": "READ", "stats": cs}
    ok = (cs.get("t_net") is not None and float(np.mean(book_net(c, COST_PRIMARY_BPS))) > 0.0
          and cs["ann_net"] >= MATERIALITY_ANN_NET and float(cs["t_net"]) >= T_FLOOR
          and (cs.get("p_net_one_sided") or 1.0) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION",
                    "confirmation ann %s t %s did not reproduce" % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched "
                                   "confirmation; a HUMAN gate governs prospective registration")


def run(*, verbose: bool = True, write: bool = True, iv: Optional[dict] = None,
        tr: Optional[dict] = None, cboe_vix: Optional[pd.Series] = None,
        incumbent_daily="LOAD") -> dict:
    iv = {k: load_fred(k) for k in (BASE_IV,) + tuple(v[0] for v in LEGS.values())} if iv is None else iv
    tr = {k: load_total_return(k) for k in (BASE_ETF,) + tuple(v[1] for v in LEGS.values())} if tr is None else tr
    cboe_vix = load_cboe_vix() if cboe_vix is None else cboe_vix
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    panel = build_panel(iv, tr)
    res = evaluate(panel, integrity_check=integrity(iv[BASE_IV], cboe_vix), incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"legs": LEGS, "base": [BASE_IV, BASE_ETF], "horizon": HORIZON,
                       "entry_delay": ENTRY_DELAY, "rv_lookback": RV_LOOKBACK, "leg_weight": LEG_WEIGHT,
                       "cost_ladder_bps_per_side": list(COST_LADDER_BPS)},
            "input_data_identity": "FRED:%s|NORGATE_TR:%s" % (
                ",".join("%s..%s" % (str(v.index.min().date()), str(v.index.max().date())) for v in iv.values()),
                ",".join(sorted(tr))),
            "informs_live_candidate": False, "result": res, "capital_eligible": False}
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
    except (ImportError, FileNotFoundError) as exc:
        return {"verdict": "DATA_HOLD", "why": "an input is unavailable: %s" % exc, "kill_rule_fired": "DATA",
                "statistic": {}, "economics": {}, "multiplicity": {}, "artifact": None,
                "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
            "statistic": {"lockbox_t": prim.get("t_net"),
                          "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "periods": prim.get("periods"),
                          "timing_increment_t": (res.get("timing_increment") or {}).get("t"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_2bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                          "max_dd": prim.get("max_dd"), "legs": res.get("legs"),
                          "qualification_gross": res.get("qualification_gross")},
            "multiplicity": res.get("multiplicity") or {"m": len(LEGS) + len(INHERITED_NULLS), "q": BH_Q,
                                                        "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body["input_data_identity"]}
