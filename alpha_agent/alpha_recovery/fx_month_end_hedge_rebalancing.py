r"""alpha_agent.alpha_recovery.fx_month_end_hedge_rebalancing - executor for the Alpha Agent mechanism
``FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1``.

Contract: ``research/preregistration/FX_MONTH_END_EQUITY_HEDGE_REBALANCING_PREREGISTRATION.md``
(committed 05a74f3 before any currency return existed). One frozen cell: a basket of CME euro, yen,
Swiss franc and Canadian dollar futures, each signed AGAINST its equity market's month-to-date return
relative to the S&P 500 at the close three sessions before month end, entered at the next close and
held over the last session of the month (the session of the 4 pm London fix). Legs are roll-free
(``month_end_rebalancing_flow.leg_returns``) and scaled to 10 % annualised volatility per pair.

Existing owners only: ``month_end_rebalancing_flow.load_futures`` / ``load_etf_total_return`` and
``alpha_agent.r63.sensitivity.nw_tstat`` / ``bh_fdr`` / ``_max_dd``. RESEARCH ONLY;
``capital_eligible`` is always False.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import sensitivity as S

from . import BH_Q, GATE_DD_MULTIPLE, MATERIALITY_ANN_NET, write_artifact
from . import month_end_rebalancing_flow as MEF

CALCULATION_OWNER = "alpha_agent.alpha_recovery.fx_month_end_hedge_rebalancing"
MECHANISM_ID = "FX_MONTH_END_EQUITY_HEDGE_REBALANCING_V1"
ARTIFACT_NAME = "fx_month_end_equity_hedge_rebalancing.json"
PREREGISTRATION = "research/preregistration/FX_MONTH_END_EQUITY_HEDGE_REBALANCING_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "05a74f3"

KILL_RULE_FROZEN = (
    "Kill if the signed currency basket has mean net <= 0, NW t < 2.0 or annualised net below 1.5 %/yr at "
    "3 bp per side per leg; or its increment over the unconditional month-end dollar basket has t < 2.0 or "
    "annualised mean below 1.5 %/yr; or annualised net is non-positive in either qualification half or in "
    "any leave-one-currency-out basket; or its maximum drawdown is worse than 1.5 times the unconditional "
    "basket's; or it fails BH q=0.10 at m=5 with MONTH_END_BALANCED_REBALANCING_FLOW_V1 and "
    "R32_EVENT_DRIVEN_CALENDAR (3 cells) inherited at p=1; or annualised net at 6 bp per side is below "
    "1.5 %/yr; or the untouched 2015-2026 confirmation has mean <= 0, NW t < 2.0 or annualised net below "
    "1.5 %/yr.")

# --------------------------------------------------------------------------- #
# Frozen design (section numbers refer to the preregistration)
# --------------------------------------------------------------------------- #
PAIRS = {"EUR": ("&6E", "&FESX"), "JPY": ("&6J", "&NKD"), "CHF": ("&6S", "&FSMI"), "CAD": ("&6C", "&SXF")}  # s1
US_EQUITY = "&ES"
VALIDATION = {"EUR": ("FXE", 0.90, ("2006-01-01", "2014-12-31")),                   # s7 gate 1
              "JPY": ("FXY", 0.90, ("2007-03-01", "2014-12-31"))}
MIN_MONTH_SESSIONS = 8                                                              # s2
VOL_LOOKBACK = 63
VOL_TARGET = 0.10
STALE_DAYS = 5
WINDOWS_PER_YEAR = 12.0                                                             # s6
COST_PRIMARY_BPS = 3.0                                                              # s5 (R38 FX_FUTURE)
COST_LADDER_BPS = (0.0, 1.0, 3.0, 6.0)
COST_STRESS_BPS = 6.0
QUALIFICATION = ("2000-01-01", "2014-12-31")                                        # s4
HALVES = {"H1_2000_2007": ("2000-01-01", "2007-12-31"), "H2_2008_2014": ("2008-01-01", "2014-12-31")}
CONFIRMATION = ("2015-01-01", "2026-08-31")
MIN_QUALIFICATION_MONTHS = 150
MIN_CONFIRMATION_MONTHS = 120
MIN_FINITE_MONTH_SHARE = 0.95
MIN_FINITE_PAIRS = 3
T_FLOOR = float(STANDALONE_T_FLOOR)
DD_MULTIPLE = float(GATE_DD_MULTIPLE)
INHERITED_NULLS = ("MONTH_END_BALANCED_REBALANCING_FLOW_V1",
                   "R32_EVENT_DRIVEN_CALENDAR|TURN_OF_MONTH_DUMMY",
                   "R32_EVENT_DRIVEN_CALENDAR|QUARTER_END_DUMMY",
                   "R32_EVENT_DRIVEN_CALENDAR|TRIPLE_WITCHING_DUMMY")
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.month_end_rebalancing_flow.leg_returns")


def _f(v) -> Optional[float]:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if np.isfinite(x) else None


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_inputs() -> dict:
    """Roll-free daily returns of every frozen leg; a missing series is recorded, never guessed."""
    fx, eq, etf, problems = {}, {}, {}, []

    def get(sym, loader):
        try:
            s = loader(sym)
            s.index = pd.DatetimeIndex(s.index).normalize()
            return s.sort_index()
        except Exception as exc:                                  # noqa: BLE001 - reported as DATA
            problems.append("SERIES_MISSING_%s: %s" % (sym, str(exc)[:120] or type(exc).__name__))
            return None

    es = get(US_EQUITY, MEF.load_futures)
    for k, (fx_sym, eq_sym) in PAIRS.items():
        fx[k], eq[k] = get(fx_sym, MEF.load_futures), get(eq_sym, MEF.load_futures)
    for k, (etf_sym, _floor, _span) in VALIDATION.items():
        etf[k] = get(etf_sym, MEF.load_etf_total_return)
    return {"fx": fx, "eq": eq, "es": es, "etf": etf, "problems": problems}


def _index(r: pd.Series) -> pd.Series:
    return (1.0 + r.dropna().sort_index()).cumprod()


def _last_on_or_before(idx: pd.Series, t: pd.Timestamp) -> tuple:
    pos = int(idx.index.searchsorted(t, side="right")) - 1
    if pos < 0:
        return np.nan, None
    return float(idx.iloc[pos]), idx.index[pos]


def validate(fx: dict, etf: dict) -> dict:
    out = {}
    for k, (name, floor, (lo, hi)) in VALIDATION.items():
        f, e = fx.get(k), etf.get(k)
        if f is None or e is None:
            out[k] = {"against": name, "corr": None, "floor": floor, "passes": False}
            continue
        j = pd.concat({"f": f, "e": e}, axis=1).dropna().loc[lo:hi]
        corr = float(j["f"].corr(j["e"])) if len(j) > 50 else None
        out[k] = {"against": name, "corr": corr, "floor": floor, "sessions": int(len(j)),
                  "passes": bool(corr is not None and corr >= floor)}
    return out


# --------------------------------------------------------------------------- #
# The frozen cell
# --------------------------------------------------------------------------- #
def calendar(fx: dict, es: pd.Series) -> pd.DatetimeIndex:
    cal = pd.DatetimeIndex(es.dropna().index)
    for k in PAIRS:
        cal = cal.intersection(pd.DatetimeIndex(fx[k].dropna().index))
    return cal.sort_values()


def windows(fx: dict, eq: dict, es: pd.Series) -> pd.DataFrame:
    cal = calendar(fx, es)
    es_idx = _index(es)
    fx_idx = {k: _index(fx[k]) for k in PAIRS}
    eq_idx = {k: _index(eq[k]) for k in PAIRS}
    vol = {k: fx[k].dropna().sort_index().rolling(VOL_LOOKBACK, min_periods=VOL_LOOKBACK).std() * np.sqrt(252.0)
           for k in PAIRS}
    groups = [g.index for _, g in pd.Series(cal, index=cal).groupby([cal.year, cal.month])]
    rows = []
    for m in range(1, len(groups)):
        L, prev = groups[m], groups[m - 1]
        if len(L) < MIN_MONTH_SESSIONS:
            continue
        s, e, x, p = L[-3], L[-2], L[-1], prev[-1]
        us = float(es_idx.at[s] / es_idx.at[p] - 1.0)
        row = {"signal_date": s, "entry_date": e, "exit_date": x, "base_date": p,
               "quarter_end": s.month in (3, 6, 9, 12)}
        gross = uncond = turnover = 0.0
        active = 0
        for k in PAIRS:
            a, ta = _last_on_or_before(eq_idx[k], s)
            b, tb = _last_on_or_before(eq_idx[k], p)
            fresh = (ta is not None and tb is not None and (s - ta).days <= STALE_DAYS
                     and (p - tb).days <= STALE_DAYS)
            d = (a / b - 1.0) - us if fresh and np.isfinite(a) and np.isfinite(b) and b != 0 else np.nan
            sig = float(vol[k].at[s]) if s in vol[k].index else np.nan
            R = float(fx_idx[k].at[x] / fx_idx[k].at[e] - 1.0)
            ok = np.isfinite(d) and d != 0.0 and np.isfinite(sig) and sig > 0 and np.isfinite(R)
            w = float(-np.sign(d) * VOL_TARGET / sig) if ok else 0.0
            row.update({"d_%s" % k: d, "sigma_%s" % k: sig, "R_%s" % k: R if np.isfinite(R) else 0.0,
                        "w_%s" % k: w})
            if ok:
                active += 1
                gross += w * R
                uncond += -VOL_TARGET / sig * R
                turnover += abs(w)
        row.update({"n_active": active, "gross": gross if active else np.nan,
                    "uncond": uncond if active else np.nan, "turnover": turnover})
        rows.append(row)
    return pd.DataFrame(rows)


def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    return d[(d["signal_date"] >= pd.Timestamp(lo)) & (d["signal_date"] <= pd.Timestamp(hi))]


def _net(d: pd.DataFrame, cost_bps: float, *, only: Optional[str] = None,
         exclude: Optional[str] = None) -> np.ndarray:
    ok = d[d["n_active"] > 0] if len(d) else d
    if not len(ok):
        return np.array([])
    keys = [k for k in PAIRS if (only is None or k == only) and k != exclude]
    g = sum(ok["w_%s" % k] * ok["R_%s" % k] for k in keys)
    t = sum(ok["w_%s" % k].abs() for k in keys)
    return (g - t * 2.0 * cost_bps * 1e-4).to_numpy(dtype=float)


def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"windows": int(len(x)), "mean": None, "ann_net": None, "t_net": None,
                "p_net_one_sided": None, "sharpe": None, "max_dd": None}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"windows": int(len(x)), "mean": float(x.mean()), "ann_net": float(x.mean() * WINDOWS_PER_YEAR),
            "sharpe": float(x.mean() / sd * np.sqrt(WINDOWS_PER_YEAR)) if sd > 0 else None,
            "t_net": _f(st["t"]), "p_net_one_sided": _f(st["p_one_sided"]), "max_dd": _f(S._max_dd(x)),
            "hit_rate": float((x > 0).mean())}


def increment(d: pd.DataFrame, cost_bps: float) -> dict:
    ok = d[d["n_active"] > 0]
    net, u = _net(ok, cost_bps), ok["uncond"].to_numpy(dtype=float)
    if len(net) < 3 or float(np.var(u, ddof=1)) <= 0:
        return {"windows": int(len(net)), "t": None, "ann": None, "uncond_max_dd": None}
    beta = float(np.cov(net, u, ddof=1)[0, 1] / np.var(u, ddof=1))
    a = net - beta * u
    st = S.nw_tstat(a, 0)
    return {"windows": int(len(a)), "beta_on_unconditional": beta, "t": _f(st["t"]),
            "ann": float(a.mean() * WINDOWS_PER_YEAR), "uncond_ann": float(u.mean() * WINDOWS_PER_YEAR),
            "uncond_max_dd": _f(S._max_dd(u)), "definition": "a = net - beta_hat * U; nw_tstat(a, 0)"}


def multiplicity(p: Optional[float]) -> dict:
    pv = {MECHANISM_ID: 1.0 if _f(p) is None else float(p)}
    pv.update({n: 1.0 for n in INHERITED_NULLS})
    bh = S.bh_fdr(pv, BH_Q)
    return {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS), "p_one_sided": _f(p),
            "passes": bool(bh["per_test"].get(MECHANISM_ID)), "single_survivor_threshold": BH_Q / bh["m"]}


def evaluate(d: pd.DataFrame, *, validation: dict, load_problems=()) -> dict:
    q, c = _window(d, *QUALIFICATION), _window(d, *CONFIRMATION)
    out: dict = {"n_windows_total": int(len(d)), "qualification_window": QUALIFICATION,
                 "confirmation_window": CONFIRMATION, "halves": HALVES,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"},
                 "untouched_confirmation": "NOT_READ"}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "kill_rule_fired": gate, "why": why})
        return out

    qf = int((q["n_active"] >= MIN_FINITE_PAIRS).sum()) if len(q) else 0
    cf = int((c["n_active"] >= MIN_FINITE_PAIRS).sum()) if len(c) else 0
    share = qf / len(q) if len(q) else 0.0
    problems = list(load_problems)
    if qf < MIN_QUALIFICATION_MONTHS:
        problems.append("QUALIFICATION_MONTHS_WITH_%d_FINITE_PAIRS_%d_BELOW_%d" % (MIN_FINITE_PAIRS, qf,
                                                                                  MIN_QUALIFICATION_MONTHS))
    if share < MIN_FINITE_MONTH_SHARE:
        problems.append("FINITE_MONTH_SHARE_%.3f_BELOW_%.2f" % (share, MIN_FINITE_MONTH_SHARE))
    if cf < MIN_CONFIRMATION_MONTHS:
        problems.append("CONFIRMATION_MONTHS_WITH_%d_FINITE_PAIRS_%d_BELOW_%d" % (MIN_FINITE_PAIRS, cf,
                                                                                 MIN_CONFIRMATION_MONTHS))
    for k, v in (validation or {}).items():
        if not v.get("passes"):
            problems.append("VALIDATION_%s_%s_CORR_%s_BELOW_%.2f" % (k, v.get("against"), v.get("corr"), v.get("floor")))
    out["data"] = {"qualification_months": int(len(q)), "qualification_months_finite": qf,
                   "confirmation_months": int(len(c)), "confirmation_months_finite": cf,
                   "finite_month_share": share, "validation": validation, "problems": problems}
    if problems:
        return done("DATA_HOLD", "DATA", "DATA gate: %s" % "; ".join(problems[:10]))

    by_cost = {"%.1f" % k: stats(_net(q, k)) for k in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    qa = q[q["n_active"] > 0]
    years = len(qa) / WINDOWS_PER_YEAR if len(qa) else 1.0
    out.update({
        "qualification_by_cost_bps_per_side": by_cost,
        "diagnostics_qualification": {
            "per_currency_ann_net": {k: stats(_net(q, COST_PRIMARY_BPS, only=k)).get("ann_net") for k in PAIRS},
            "quarter_end_ann_net": stats(_net(q[q["quarter_end"]], COST_PRIMARY_BPS)).get("ann_net"),
            "other_months_ann_net": stats(_net(q[~q["quarter_end"]], COST_PRIMARY_BPS)).get("ann_net"),
            "turnover_nav_per_year": float(2.0 * qa["turnover"].sum() / years) if len(qa) else None,
            "mean_gross_notional": float(qa["turnover"].mean()) if len(qa) else None,
            "sessions_held_per_year": WINDOWS_PER_YEAR}})
    if prim.get("mean") is None or prim["mean"] <= 0.0:
        return done("NO_EDGE", "WRONG_SIGN", "mean net %s per window at %.0f bp is not in the frozen direction"
                    % (prim.get("mean"), COST_PRIMARY_BPS))
    if prim.get("t_net") is None or prim["t_net"] < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s < %.1f" % (prim.get("t_net"), T_FLOOR))
    if prim["ann_net"] < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "MATERIALITY", "annualised net %.4f < %.3f" % (prim["ann_net"], MATERIALITY_ANN_NET))
    inc = increment(q, COST_PRIMARY_BPS)
    out["increment_over_unconditional_month_end_dollar"] = inc
    if inc.get("t") is None or inc["t"] < T_FLOOR or (inc.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("NO_EDGE", "INCREMENT", "increment t %s ann %s" % (inc.get("t"), inc.get("ann")))
    stab = {k: stats(_net(_window(q, lo, hi), COST_PRIMARY_BPS)).get("ann_net") for k, (lo, hi) in HALVES.items()}
    stab.update({"LOCO_EX_%s" % k: stats(_net(q, COST_PRIMARY_BPS, exclude=k)).get("ann_net") for k in PAIRS})
    out["stability_ann_net"] = stab
    bad = sorted(k for k, v in stab.items() if v is None or v <= 0.0)
    if bad:
        return done("NO_EDGE", "STABILITY", "non-positive annualised net in %s" % ", ".join(bad))
    if (prim.get("max_dd") or 0.0) < DD_MULTIPLE * (inc.get("uncond_max_dd") or 0.0):
        return done("NO_EDGE", "DRAWDOWN", "max drawdown %s worse than %.1f x unconditional %s"
                    % (prim.get("max_dd"), DD_MULTIPLE, inc.get("uncond_max_dd")))
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
                           "increment": increment(c, COST_PRIMARY_BPS)}
    ok = (cs.get("mean") is not None and cs["mean"] > 0.0 and cs.get("t_net") is not None
          and cs["t_net"] >= T_FLOOR and (cs.get("ann_net") or 0.0) >= MATERIALITY_ANN_NET)
    out["untouched_confirmation"] = "CONFIRMED" if ok else "FAILED"
    if not ok:
        return done("NO_EDGE", "CONFIRMATION", "untouched confirmation ann %s t %s did not reproduce"
                    % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched confirmation; a "
                                   "HUMAN gate governs prospective registration")


def design() -> dict:
    return {"pairs": {k: list(v) for k, v in PAIRS.items()}, "us_equity_leg": US_EQUITY,
            "window": "signal L[-3], entry close L[-2], exit close L[-1] (the fix session)",
            "leg_return": "diff(_CCB close) / prior unadjusted close",
            "weight": "-sign(foreign minus US month-to-date equity return) * 0.10 / sigma_63d per pair",
            "control": "unconditional month-end dollar basket U = sum(-0.10/sigma * R) over active pairs",
            "stale_days": STALE_DAYS, "vol_lookback": VOL_LOOKBACK, "vol_target": VOL_TARGET,
            "cost_primary_bps_per_side": COST_PRIMARY_BPS, "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
            "cost_stress_bps_per_side": COST_STRESS_BPS, "qualification": QUALIFICATION, "halves": HALVES,
            "confirmation": CONFIRMATION, "validation": {k: [v[0], v[1], list(v[2])] for k, v in VALIDATION.items()},
            "multiplicity": {"q": BH_Q, "inherited_nulls_at_p1": list(INHERITED_NULLS)}}


def run(*, verbose: bool = True, write: bool = True, inputs: Optional[dict] = None) -> dict:
    inputs = load_inputs() if inputs is None else inputs
    fx, eq, es, etf = inputs["fx"], inputs["eq"], inputs["es"], inputs["etf"]
    problems = list(inputs.get("problems") or [])
    if es is None or any(fx.get(k) is None or eq.get(k) is None for k in PAIRS):
        d = pd.DataFrame(columns=["signal_date", "n_active"])
        res = evaluate(d, validation=validate(fx, etf), load_problems=problems or ["SERIES_MISSING"])
        span = "UNAVAILABLE"
    else:
        d = windows(fx, eq, es)
        res = evaluate(d, validation=validate(fx, etf), load_problems=problems)
        lo = min(s.index.min() for s in [es] + list(fx.values()) + list(eq.values()))
        hi = max(s.index.max() for s in [es] + list(fx.values()) + list(eq.values()))
        span = "%s..%s" % (lo.date(), hi.date())
    ident = "NORGATE:%s,%s(+_CCB):%s;VALIDATION:%s" % (
        US_EQUITY, ",".join("%s/%s" % v for v in PAIRS.values()), span,
        ",".join(v[0] for v in VALIDATION.values()))
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
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    stress = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_STRESS_BPS) or {}
    inc = res.get("increment_over_unconditional_month_end_dollar") or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("kill_rule_fired"),
            "statistic": {"lockbox_t": prim.get("t_net"), "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "windows": prim.get("windows"),
                          "increment_t": inc.get("t"), "untouched_confirmation": res.get("untouched_confirmation"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_3bp": prim.get("ann_net"), "ann_net_1bp": prim.get("ann_net"),
                          "sharpe": prim.get("sharpe"), "max_dd": prim.get("max_dd"),
                          "ann_net_6bp": stress.get("ann_net"), "increment_ann": inc.get("ann"),
                          "stability_ann_net": res.get("stability_ann_net"),
                          "qualification_by_cost_bps_per_side": res.get("qualification_by_cost_bps_per_side"),
                          "diagnostics": res.get("diagnostics_qualification")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q,
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
        return {"verdict": "DATA_HOLD", "why": "an owned input is unavailable or unreadable: %r" % (exc,),
                "kill_rule_fired": "DATA", "statistic": {}, "economics": {},
                "multiplicity": {"m": 1 + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
                "artifact": None, "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    return executor_result(body)


__all__ = ["MECHANISM_ID", "KILL_RULE_FROZEN", "PAIRS", "windows", "evaluate", "run", "run_mechanism",
           "multiplicity", "validate", "increment"]
