r"""alpha_agent.alpha_recovery.commodity_index_roll_window - executor for the Alpha Agent
mechanism ``COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1``.

Contract: ``research/preregistration/COMMODITY_INDEX_ROLL_WINDOW_PREREGISTRATION.md``.
One frozen cell: in each of 17 commodity markets whose R38 layer rolls after business day 10,
hold long the deferred / short the nearby contract from the business-day-3 close to the
business-day-10 close; the monthly book is the equal-weight mean across eligible markets, net of
the owned R38 per-market cost on two legs and two sides. Gates run in the preregistered order,
including a passive-spread control that separates roll pressure from ordinary spread drift.

Existing owners only: ``r59.native`` (paths and per-market costs), ``r63.sensitivity.nw_tstat`` /
``bh_fdr`` / ``_max_dd`` and ``intraday_alpha.equal_risk_daily``. RESEARCH ONLY;
``capital_eligible`` is always False.
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r59 import native as N
from alpha_agent.r63 import sensitivity as S

from . import MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.commodity_index_roll_window"
MECHANISM_ID = "COMMODITY_INDEX_ROLL_WINDOW_PRESSURE_V1"
ARTIFACT_NAME = "commodity_index_roll_window.json"
PREREGISTRATION = "research/preregistration/COMMODITY_INDEX_ROLL_WINDOW_PREREGISTRATION.md"

UNIVERSE = ("HO", "RB", "NG", "GC", "SI", "HG", "PL", "PA", "ZC", "ZS", "ZW", "KE", "ZL", "ZM",
            "KC", "SB", "CT")
EXCLUDED = {"CL": "rolls inside the window", "BRN": "rolls inside the window",
            "HE": "rolls inside the window", "GAS": "rolls before the window",
            "LE": "rolls before the window", "CC": "mixed roll timing", "GF": "mixed roll timing"}
ENTRY_INDEX = 2              # business day 3
EXIT_INDEX = 9               # business day 10
WINDOW_SESSIONS = EXIT_INDEX - ENTRY_INDEX
MIN_MONTH_SESSIONS = 10
MIN_MARKETS = 10
PERIODS_PER_YEAR = 12.0
STRESS_MULTIPLE = 2.0
QUALIFICATION = ("1995-01", "2014-12")
HALVES = {"H1_1995_2009": ("1995-01", "2009-12"), "H2_2010_2014": ("2010-01", "2014-12")}
CONFIRMATION = ("2015-01", "2026-08")
MIN_MONTH_SHARE = 0.95
T_FLOOR = 2.0
LOO_T_FLOOR = 1.0
BH_Q = 0.10
INHERITED_NULLS = ("CALENDAR_TERM_STRUCTURE_ROLL_STATE_R59", "COMMODITY_CURVE_CARRY_SPREADS")
SCORER = ("alpha_agent.r63.sensitivity.nw_tstat/bh_fdr/_max_dd + "
          "alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_frames(universe=UNIVERSE) -> dict:
    out = {}
    for sym in universe:
        p = N.NATIVE_LAYER_DIR / ("%s.csv" % sym)
        df = pd.read_csv(p, usecols=["Date", "ret", "ret2", "held"], parse_dates=["Date"])
        out[sym] = df.sort_values("Date").reset_index(drop=True)
    return out


def load_costs(universe=UNIVERSE) -> dict:
    meta = N.load_meta()
    missing = [s for s in universe if s not in meta]
    if missing:
        raise KeyError("no R38 cost for %s" % missing)
    return {s: float(meta[s]["cost_bps_per_side"]) for s in universe}


# --------------------------------------------------------------------------- #
# The frozen cell
# --------------------------------------------------------------------------- #
def market_months(df: pd.DataFrame) -> tuple:
    """Eligible market-months (spread, window dates) and the passive daily spread drift inputs."""
    d = df.copy()
    d["ym"] = d["Date"].dt.to_period("M")
    held = d["held"].astype(str).to_numpy()
    change = np.zeros(len(d), dtype=bool)
    change[1:] = held[1:] != held[:-1]
    in_window = np.zeros(len(d), dtype=bool)
    rows = []
    for ym, g in d.groupby("ym", sort=True):
        if len(g) < MIN_MONTH_SESSIONS:
            continue
        idx = g.index.to_numpy()
        in_window[idx[ENTRY_INDEX + 1:EXIT_INDEX + 1]] = True
        h = held[idx[ENTRY_INDEX:EXIT_INDEX + 1]]
        w = g.iloc[ENTRY_INDEX + 1:EXIT_INDEX + 1]
        if (h[1:] != h[:-1]).any():
            continue
        r, r2 = w["ret"].to_numpy(dtype=float), w["ret2"].to_numpy(dtype=float)
        if not (np.isfinite(r).all() and np.isfinite(r2).all()):
            continue
        rows.append({"ym": ym, "entry_date": g["Date"].iloc[ENTRY_INDEX],
                     "exit_date": g["Date"].iloc[EXIT_INDEX],
                     "spread": float(np.prod(1.0 + r2) - np.prod(1.0 + r))})
    daily = pd.DataFrame({"ym": d["ym"], "diff": (d["ret2"] - d["ret"]).to_numpy(dtype=float),
                          "passive_ok": ~in_window & ~change})
    return pd.DataFrame(rows), daily


def build(frames: dict, costs: dict) -> dict:
    per_market, drift_inputs = {}, {}
    for sym, df in frames.items():
        mm, daily = market_months(df)
        if len(mm):
            mm["cost"] = 4.0 * costs[sym] / 1e4
        per_market[sym] = mm
        drift_inputs[sym] = daily
    return {"per_market": per_market, "drift": drift_inputs}


def _in(ym_series: pd.Series, lo: str, hi: str) -> pd.Series:
    return (ym_series >= pd.Period(lo, "M")) & (ym_series <= pd.Period(hi, "M"))


def passive_drift(built: dict, lo: str, hi: str) -> dict:
    out = {}
    for sym, daily in built["drift"].items():
        sel = daily["passive_ok"] & _in(daily["ym"], lo, hi) & np.isfinite(daily["diff"])
        out[sym] = float(daily.loc[sel, "diff"].mean()) if sel.any() else np.nan
    return out


def book(built: dict, lo: str, hi: str, *, cost_multiple: float = 1.0, drift: Optional[dict] = None,
         exclude: Optional[str] = None) -> pd.DataFrame:
    rows = []
    for sym, mm in built["per_market"].items():
        if sym == exclude or not len(mm):
            continue
        sub = mm[_in(mm["ym"], lo, hi)]
        if not len(sub):
            continue
        net = sub["spread"] - cost_multiple * sub["cost"]
        if drift is not None:
            net = net - WINDOW_SESSIONS * drift.get(sym, np.nan)
        rows.append(pd.DataFrame({"ym": sub["ym"].to_numpy(), "sym": sym, "net": net.to_numpy(),
                                  "exit_date": sub["exit_date"].to_numpy()}))
    if not rows:
        return pd.DataFrame(columns=["ym", "net", "markets", "exit_date"])
    allm = pd.concat(rows, ignore_index=True)
    g = allm.groupby("ym")
    return pd.DataFrame({"net": g["net"].mean(), "markets": g["net"].count(),
                         "exit_date": g["exit_date"].max()}).reset_index()


def stats(x) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    if len(x) < 3:
        return {"months": int(len(x))}
    sd = float(np.std(x, ddof=1))
    st = S.nw_tstat(x, 0)
    return {"months": int(len(x)), "ann_net": float(x.mean() * PERIODS_PER_YEAR),
            "sharpe": float(x.mean() / sd * np.sqrt(PERIODS_PER_YEAR)) if sd > 0 else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"], "max_dd": S._max_dd(x),
            "hit_rate": float((x > 0).mean())}


def _incumbent(b: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    parts = []
    for _, row in b.iterrows():
        days = pd.bdate_range(end=pd.Timestamp(row["exit_date"]), periods=WINDOW_SESSIONS)
        parts.append(pd.Series((1.0 + float(row["net"])) ** (1.0 / WINDOW_SESSIONS) - 1.0, index=days))
    if not parts:
        return {"state": "DATA_HOLD", "why": "no month"}
    sleeve = pd.concat(parts).groupby(level=0).sum()
    sleeve = sleeve.reindex(pd.bdate_range(sleeve.index.min(), sleeve.index.max())).fillna(0.0)
    return equal_risk_daily(sleeve, incumbent_daily, label=MECHANISM_ID)


def evaluate(frames: dict, costs: dict, *, incumbent_daily: Optional[pd.Series] = None) -> dict:
    built = build(frames, costs)
    q = book(built, *QUALIFICATION)
    out: dict = {"qualification_window": QUALIFICATION, "confirmation_window": CONFIRMATION,
                 "universe": list(frames), "excluded_before_results": EXCLUDED, "costs_bps_per_side": costs,
                 "confirmation": {"state": "UNREAD", "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    n_months = int(len(pd.period_range(QUALIFICATION[0], QUALIFICATION[1], freq="M")))
    full = q[q["markets"] >= MIN_MARKETS] if len(q) else q
    share = len(full) / n_months if n_months else 0.0
    out["data"] = {"qualification_months": n_months, "months_with_min_markets": int(len(full)),
                   "share": share, "markets_per_month": {"min": int(q["markets"].min()) if len(q) else 0,
                                                         "median": float(q["markets"].median()) if len(q) else 0}}
    if len(full) < MIN_EFFECTIVE_PERIODS or share < MIN_MONTH_SHARE:
        return done("DATA_HOLD", "DATA", "months with >= %d markets: %d (share %.3f)" % (MIN_MARKETS, len(full), share))
    prim = stats(q["net"])
    out["qualification_primary"] = prim
    out["qualification_gross"] = stats(book(built, *QUALIFICATION, cost_multiple=0.0)["net"])
    stress = stats(book(built, *QUALIFICATION, cost_multiple=STRESS_MULTIPLE)["net"])
    out["qualification_stress"] = stress
    if float(np.mean(q["net"])) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN", "mean monthly net %.6f is not in the frozen direction"
                    % float(np.mean(q["net"])))
    if prim.get("t_net") is None or float(prim["t_net"]) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "NW t %s below %.1f" % (prim.get("t_net"), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY", "annualised net %.4f" % prim["ann_net"])
    drift = passive_drift(built, *QUALIFICATION)
    exc = stats(book(built, *QUALIFICATION, drift=drift)["net"])
    out["passive_spread_drift_per_session"] = drift
    out["excess_over_passive_spread"] = exc
    if exc.get("t_net") is None or float(exc["t_net"]) < T_FLOOR or (exc.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "PASSIVE_SPREAD", "excess over the ordinary spread drift t %s ann %s"
                    % (exc.get("t_net"), exc.get("ann_net")))
    halves = {k: stats(book(built, *w)["net"]).get("ann_net") for k, w in HALVES.items()}
    out["halves_ann_net"] = halves
    bad = sorted(k for k, v in halves.items() if v is None or v <= 0.0)
    if bad:
        return done("KILLED_UNSTABLE", "HALVES", "non-positive annualised net in %s" % ", ".join(bad))
    loo = {s: stats(book(built, *QUALIFICATION, exclude=s)["net"]).get("t_net") for s in frames}
    out["leave_one_market_out_t"] = loo
    weak = sorted(s for s, t in loo.items() if t is None or float(t) < LOO_T_FLOOR)
    if weak:
        return done("KILLED_UNSTABLE", "LEAVE_ONE_MARKET_OUT", "t below %.1f without %s" % (LOO_T_FLOOR, ", ".join(weak)))
    pvals = {MECHANISM_ID: prim.get("p_net_one_sided")}
    pvals.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pvals, BH_Q)
    out["multiplicity"] = {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
                           "p_one_sided": prim.get("p_net_one_sided"),
                           "passes": bool(bh["per_test"].get(MECHANISM_ID))}
    if not out["multiplicity"]["passes"]:
        return done("KILLED_MULTIPLICITY", "BH_INHERITED", "fails BH q=%.2f at m=%d" % (BH_Q, bh["m"]))
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST", "annualised net at %.0fx cost %.4f"
                    % (STRESS_MULTIPLE, stress.get("ann_net") or 0.0))
    ir = _incumbent(q, incumbent_daily)
    out["incumbent_equal_risk"] = ir
    if ir.get("state") == "OK" and not ir.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT", "equal-risk increment %s t %s"
                    % (ir.get("incremental_ann_net_return"), ir.get("t_incremental")))
    c = book(built, *CONFIRMATION)
    cs = stats(c["net"])
    out["confirmation"] = {"state": "READ", "stats": cs}
    p_conf = cs.get("p_net_one_sided")
    # An explicit None test: a p-value that underflows to exactly 0.0 is the strongest possible
    # evidence, and ``p or 1.0`` would have read it as missing.
    ok = (cs.get("t_net") is not None and float(np.mean(c["net"])) > 0.0 and cs["ann_net"] >= MATERIALITY_ANN_NET
          and float(cs["t_net"]) >= T_FLOOR and p_conf is not None and float(p_conf) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION", "confirmation ann %s t %s did not reproduce"
                    % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched confirmation; "
                                   "a HUMAN gate governs prospective registration")


def run(*, verbose: bool = True, write: bool = True, frames: Optional[dict] = None,
        costs: Optional[dict] = None, incumbent_daily="LOAD") -> dict:
    frames = load_frames() if frames is None else frames
    costs = load_costs(tuple(frames)) if costs is None else costs
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    res = evaluate(frames, costs, incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"universe": list(UNIVERSE), "entry": "close of business day 3",
                       "exit": "close of business day 10", "spread": "prod(1+ret2) - prod(1+ret)",
                       "cost": "4 x R38 cost per side", "stress_multiple": STRESS_MULTIPLE},
            "input_data_identity": "R38_NATIVE_LAYER:%s" % ",".join(
                "%s:%s..%s" % (s, str(f["Date"].min().date()), str(f["Date"].max().date())) for s, f in frames.items()),
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
    except (OSError, KeyError) as exc:
        return {"verdict": "DATA_HOLD", "why": "an owned input is unavailable: %s" % exc, "kill_rule_fired": "DATA",
                "statistic": {}, "economics": {}, "multiplicity": {}, "artifact": None,
                "capital_eligible": False, "scorer": SCORER, "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = res.get("qualification_primary") or {}
    return {"verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
            "statistic": {"lockbox_t": prim.get("t_net"), "lockbox_window": "QUALIFICATION %s..%s" % QUALIFICATION,
                          "p_one_sided": prim.get("p_net_one_sided"), "months": prim.get("months"),
                          "excess_over_passive_t": (res.get("excess_over_passive_spread") or {}).get("t_net"),
                          "confirmation": res.get("confirmation")},
            "economics": {"ann_net_r38_cost": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                          "max_dd": prim.get("max_dd"), "qualification_gross": res.get("qualification_gross"),
                          "halves_ann_net": res.get("halves_ann_net")},
            "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q, "state": "NOT_REACHED"},
            "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
            "input_data_identity": body["input_data_identity"]}
