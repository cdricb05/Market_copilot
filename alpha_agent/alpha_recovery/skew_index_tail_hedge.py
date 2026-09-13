r"""alpha_agent.alpha_recovery.skew_index_tail_hedge - executor for the Alpha Agent mechanism
``SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_1993_2022_V1``.

The contract is ``research/preregistration/SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_PREREGISTRATION.md``;
this module implements it and nothing else. It was selected by the mechanism frontier
(``alpha_agent.r59.mechanisms``) and is dispatched to by the R59 handler through its pinned
``run_mechanism`` callable.

One frozen cell: ``pos_t = -sign(z_t)`` where ``z_t`` is the strictly trailing 60-observation
z-score of the Cboe SKEW index on the SPY session calendar; a weekly, non-overlapping grid;
entry at the SPY total-return close of t+1; exit five sessions later. Gates run in the
preregistered order and map onto the executor verdict vocabulary. The untouched confirmation
window is read ONLY when every qualification gate passes.

Existing owners only: ``options_surface._z`` / ``_stats`` (the SPY sign-book statistics),
``r63.sensitivity.nw_tstat`` / ``bh_fdr``, ``intraday_alpha.equal_risk_daily`` /
``incumbent_daily_path``. RESEARCH ONLY: no registration, promotion, order or live write, and
``capital_eligible`` is always False.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from alpha_agent.r63 import sensitivity as S

from . import MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, SPY_PROXY_COST_BPS, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.skew_index_tail_hedge"
MECHANISM_ID = "SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_1993_2022_V1"
ARTIFACT_NAME = "spx_tail_hedge_skew_index.json"
PREREGISTRATION = "research/preregistration/SPX_TAIL_HEDGE_DEMAND_SKEW_INDEX_PREREGISTRATION.md"

SKEW_PATH = Path(r"D:\Stock_Prediction_app_data\multi_horizon_alpha_r41\_data_cboe\SKEW_History.csv")

FROZEN_SIGN = -1.0
HORIZON = 5
ENTRY_DELAY = 1
PPY = 252.0
COST_PRIMARY_BPS = float(SPY_PROXY_COST_BPS)       # 1 bp per side
COST_LADDER_BPS = (COST_PRIMARY_BPS, 2.0, 5.0)
COST_STRESS_BPS = 5.0

QUALIFICATION = ("1993-01-29", "2022-07-29")
PARTITIONS = {"P1_1993_1999": ("1993-01-29", "1999-12-31"),
              "P2_2000_2009": ("2000-01-01", "2009-12-31"),
              "P3_2010_2019": ("2010-01-01", "2019-12-31"),
              "P4_2020_2022_07": ("2020-01-01", "2022-07-29")}
CONFIRMATION = ("2022-08-01", "2026-08-31")
MIN_FINITE_SHARE = 0.95
T_FLOOR = 2.0
MAX_NONPOSITIVE_PARTITIONS = 1
BH_Q = 0.10
#: The 12 prior cells on this information, inherited at p = 1 (preregistration section 7).
INHERITED_NULLS = (
    "SPY_OPRA|ATM_IV_LEVEL|h1", "SPY_OPRA|ATM_IV_LEVEL|h5", "SPY_OPRA|PUT_CALL_SKEW|h1",
    "SPY_OPRA|PUT_CALL_SKEW|h5", "SPY_OPRA|VARIANCE_RISK_PREMIUM|h1",
    "SPY_OPRA|VARIANCE_RISK_PREMIUM|h5", "R32|EQUITY_BETA_TIMING", "R32|VOLATILITY_RISK_REGIME",
    "R41|VX_CONDITIONAL_SHORT_VOL_SKEW_Z", "R63|VOLATILITY_EXPECTATIONS_IV|h1",
    "R63|VOLATILITY_EXPECTATIONS_IV|h5", "R63|VOLATILITY_EXPECTATIONS_IV|h21")

SCORER = ("alpha_agent.alpha_recovery.options_surface._stats + alpha_agent.r63.sensitivity."
          "nw_tstat/bh_fdr + alpha_agent.alpha_recovery.intraday_alpha.equal_risk_daily")


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #
def load_skew(path: Path = SKEW_PATH) -> pd.Series:
    df = pd.read_csv(path)
    s = pd.Series(df["SKEW"].astype(float).to_numpy(),
                  index=pd.to_datetime(df["DATE"], format="%m/%d/%Y"))
    return s.sort_index()


def load_spy_total_return() -> pd.Series:
    import norgatedata as nd
    df = nd.price_timeseries("SPY", stock_price_adjustment_setting=nd.StockPriceAdjustmentType.TOTALRETURN,
                             padding_setting=nd.PaddingType.NONE, timeseriesformat="pandas-dataframe")
    return df["Close"].astype(float).sort_index()


def build_panel(skew: pd.Series, tr: pd.Series) -> pd.DataFrame:
    """The SPY session calendar with the aligned SKEW (never forward-filled) and its z."""
    from .options_surface import _z
    tr = tr.dropna().sort_index()
    aligned = skew.reindex(tr.index)
    return pd.DataFrame({"tr": tr.to_numpy(), "skew": aligned.to_numpy(),
                         "z": _z(aligned)}, index=tr.index)


def decisions(panel: pd.DataFrame) -> pd.DataFrame:
    """The frozen weekly grid. The position is read at t, entered at the close of t+1 and
    exited at the close of t+1+HORIZON; the passive SPY return covers the same span."""
    z = panel["z"].to_numpy()
    tr = panel["tr"].to_numpy()
    finite = np.where(np.isfinite(z))[0]
    rows = []
    if len(finite):
        i = int(finite[0])
        n = len(panel)
        while i + ENTRY_DELAY + HORIZON < n:
            e, x = i + ENTRY_DELAY, i + ENTRY_DELAY + HORIZON
            pos = FROZEN_SIGN * np.sign(z[i]) if np.isfinite(z[i]) else np.nan
            rows.append({"decision_date": panel.index[i], "entry_date": panel.index[e],
                         "exit_date": panel.index[x], "z": z[i], "pos": pos,
                         "spy_r": tr[x] / tr[e] - 1.0})
            i += HORIZON
    d = pd.DataFrame(rows)
    if len(d):
        d["r"] = d["pos"] * d["spy_r"]
    return d


def _window(d: pd.DataFrame, lo: str, hi: str) -> pd.DataFrame:
    if not len(d):
        return d
    m = (d["entry_date"] >= pd.Timestamp(lo)) & (d["entry_date"] <= pd.Timestamp(hi))
    return d[m]


def _net(d: pd.DataFrame, cost_bps: float) -> np.ndarray:
    ok = d[np.isfinite(d["pos"])]
    return (ok["pos"] * ok["spy_r"] - ok["pos"].abs() * 2.0 * cost_bps * 1e-4).to_numpy()


def _stats(net: np.ndarray, label: str, cost_bps: float) -> dict:
    from .options_surface import _stats as os_stats
    return os_stats(np.asarray(net, dtype=float), h=HORIZON, label=label, cost_bps=cost_bps)


def timing_alpha(d: pd.DataFrame, cost_bps: float) -> dict:
    ok = d[np.isfinite(d["pos"])]
    net = _net(ok, cost_bps)
    spy = ok["spy_r"].to_numpy()
    if len(net) < 3 or float(np.var(spy)) <= 0:
        return {"periods": int(len(net)), "t": None, "ann": None, "beta": None}
    beta = float(np.cov(net, spy, ddof=1)[0, 1] / np.var(spy, ddof=1))
    a = net - beta * spy
    st = S.nw_tstat(a, 0)
    return {"periods": int(len(a)), "beta": beta, "t": st["t"],
            "ann": float(np.mean(a) * PPY / HORIZON), "p_one_sided": st["p_one_sided"],
            "definition": "a_t = net_t - beta_hat * spy_r_t; nw_tstat(a, 0)"}


def _incumbent_increment(d: pd.DataFrame, incumbent_daily: Optional[pd.Series]) -> dict:
    if incumbent_daily is None or not len(incumbent_daily):
        return {"state": "DATA_HOLD", "why": "the incumbent daily path is unavailable"}
    from .intraday_alpha import equal_risk_daily
    ok = d[np.isfinite(d["pos"])]
    s = pd.Series(_net(ok, COST_PRIMARY_BPS), index=pd.to_datetime(ok["entry_date"]))
    daily = s.apply(lambda r: (1.0 + r) ** (1.0 / HORIZON) - 1.0)
    daily = daily.reindex(pd.date_range(daily.index.min(), daily.index.max(), freq="B"))
    daily = daily.ffill(limit=HORIZON - 1).dropna()
    return equal_risk_daily(daily, incumbent_daily, label=MECHANISM_ID)


# --------------------------------------------------------------------------- #
# The gates, in the preregistered order
# --------------------------------------------------------------------------- #
def evaluate(panel: pd.DataFrame, *, incumbent_daily: Optional[pd.Series] = None) -> dict:
    d = decisions(panel)
    q = _window(d, *QUALIFICATION)
    out: dict = {"n_decisions_total": int(len(d)), "qualification_window": QUALIFICATION,
                 "confirmation_window": CONFIRMATION,
                 "confirmation": {"state": "UNREAD",
                                  "why": "read only if every qualification gate passes"}}

    def done(verdict: str, gate: Optional[str], why: str) -> dict:
        out.update({"verdict": verdict, "gate": gate, "why": why})
        return out

    finite_share = float(np.isfinite(q["pos"]).mean()) if len(q) else 0.0
    parts = {k: _window(q, *w) for k, w in PARTITIONS.items()}
    part_counts = {k: int(np.isfinite(v["pos"]).sum()) for k, v in parts.items()}
    out["data"] = {"qualification_decisions": int(len(q)), "finite_share": finite_share,
                   "partition_finite_decisions": part_counts}
    if finite_share < MIN_FINITE_SHARE or min(part_counts.values() or [0]) < MIN_EFFECTIVE_PERIODS:
        return done("DATA_HOLD", "DATA", "finite share %.3f (floor %.2f), smallest partition %d "
                    "(floor %d)" % (finite_share, MIN_FINITE_SHARE,
                                    min(part_counts.values() or [0]), MIN_EFFECTIVE_PERIODS))

    by_cost = {"%.1f" % c: _stats(_net(q, c), "QUALIFICATION", c) for c in COST_LADDER_BPS}
    prim = by_cost["%.1f" % COST_PRIMARY_BPS]
    out["qualification_by_cost_bps_per_side"] = by_cost
    net1 = _net(q, COST_PRIMARY_BPS)
    out["partition_ann_net"] = {k: (float(np.mean(_net(v, COST_PRIMARY_BPS)) * PPY / HORIZON)
                                    if len(_net(v, COST_PRIMARY_BPS)) else None)
                                for k, v in parts.items()}
    gross = _stats(_net(q, 0.0), "QUALIFICATION_GROSS", 0.0)
    out["qualification_gross"] = {"ann_gross": gross.get("ann_net"), "t_gross": gross.get("t_net")}

    if float(np.mean(net1)) <= 0.0:
        return done("KILLED_WRONG_SIGN", "FROZEN_SIGN",
                    "qualification mean net %.6f per period is not in the frozen NEGATIVE-sign "
                    "direction; the sign is never reversed" % float(np.mean(net1)))
    t = prim.get("t_net")
    if t is None or float(t) < T_FLOOR:
        return done("NO_EDGE", "STANDALONE_T", "qualification NW t %s below %.1f"
                    % (None if t is None else round(float(t), 3), T_FLOOR))
    if (prim.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "MATERIALITY",
                    "annualised net %.4f below %.3f" % (prim.get("ann_net") or 0.0,
                                                        MATERIALITY_ANN_NET))
    nonpos = sorted(k for k, v in out["partition_ann_net"].items() if v is None or v <= 0.0)
    if len(nonpos) > MAX_NONPOSITIVE_PARTITIONS:
        return done("KILLED_UNSTABLE", "PARTITION_STABILITY",
                    "non-positive annualised net in %s" % ", ".join(nonpos))
    ta = timing_alpha(q, COST_PRIMARY_BPS)
    out["timing_alpha"] = ta
    if ta.get("t") is None or float(ta["t"]) < T_FLOOR or (ta.get("ann") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_NONINCREMENTAL", "TIMING_ALPHA",
                    "timing alpha over a passive SPY book t %s ann %s"
                    % (ta.get("t"), ta.get("ann")))
    pvals = {MECHANISM_ID: prim.get("p_net_one_sided")}
    pvals.update({k: 1.0 for k in INHERITED_NULLS})
    bh = S.bh_fdr(pvals, BH_Q)
    out["multiplicity"] = {"m": bh["m"], "q": BH_Q, "inherited_nulls": list(INHERITED_NULLS),
                           "p_one_sided": prim.get("p_net_one_sided"),
                           "passes": bool(bh["per_test"].get(MECHANISM_ID))}
    if not out["multiplicity"]["passes"]:
        return done("KILLED_MULTIPLICITY", "BH_INHERITED",
                    "one-sided p %s fails BH q=%.2f at m=%d" % (prim.get("p_net_one_sided"),
                                                               BH_Q, bh["m"]))
    stress = by_cost["%.1f" % COST_STRESS_BPS]
    if (stress.get("ann_net") or 0.0) < MATERIALITY_ANN_NET:
        return done("KILLED_BELOW_MATERIALITY", "STRESS_COST",
                    "annualised net at %.0f bp %.4f below %.3f" % (COST_STRESS_BPS,
                                                                  stress.get("ann_net") or 0.0,
                                                                  MATERIALITY_ANN_NET))
    inc = _incumbent_increment(q, incumbent_daily)
    out["incumbent_equal_risk"] = inc
    if inc.get("state") == "OK" and not inc.get("positive_incremental_utility_after_costs"):
        return done("NO_INCREMENTAL_INFORMATION_EDGE", "INCUMBENT",
                    "equal-risk increment %.4f t %s" % (inc.get("incremental_ann_net_return") or 0.0,
                                                       inc.get("t_incremental")))

    c = _window(d, *CONFIRMATION)
    c = c[c["exit_date"] <= pd.Timestamp(CONFIRMATION[1])] if len(c) else c
    cs = _stats(_net(c, COST_PRIMARY_BPS), "CONFIRMATION", COST_PRIMARY_BPS)
    out["confirmation"] = {"state": "READ", "stats": cs}
    ok = (cs.get("ann_net") is not None and cs.get("t_net") is not None
          and float(np.mean(_net(c, COST_PRIMARY_BPS))) > 0.0
          and cs["ann_net"] >= MATERIALITY_ANN_NET and float(cs["t_net"]) >= T_FLOOR
          and (cs.get("p_net_one_sided") or 1.0) <= BH_Q)
    if not ok:
        return done("KILLED_UNSTABLE", "UNTOUCHED_CONFIRMATION",
                    "confirmation ann %s t %s did not reproduce" % (cs.get("ann_net"), cs.get("t_net")))
    return done("QUALIFIED", None, "every preregistered gate passed, including the untouched "
                                   "confirmation; a HUMAN gate governs prospective registration")


# --------------------------------------------------------------------------- #
# Executor contract
# --------------------------------------------------------------------------- #
def _identity(skew_path: Path, tr: pd.Series) -> str:
    try:
        sha = hashlib.sha256(Path(skew_path).read_bytes().replace(b"\r\n", b"\n")).hexdigest()[:16]
    except OSError:
        sha = "UNREADABLE"
    return "CBOE_SKEW:%s|NORGATE_SPY_TOTALRETURN:%s..%s:%d" % (
        sha, str(tr.index.min().date()) if len(tr) else None,
        str(tr.index.max().date()) if len(tr) else None, int(len(tr)))


def run(*, verbose: bool = True, write: bool = True, skew: Optional[pd.Series] = None,
        tr: Optional[pd.Series] = None, incumbent_daily: Optional[pd.Series] = "LOAD") -> dict:
    skew = load_skew() if skew is None else skew
    tr = load_spy_total_return() if tr is None else tr
    if isinstance(incumbent_daily, str):
        from .intraday_alpha import incumbent_daily_path
        incumbent_daily = incumbent_daily_path()
    panel = build_panel(skew, tr)
    res = evaluate(panel, incumbent_daily=incumbent_daily)
    body = {"schema": "alpha_agent_mechanism_result/1", "calculation_owner": CALCULATION_OWNER,
            "mechanism_id": MECHANISM_ID, "preregistration": PREREGISTRATION,
            "frozen": {"sign": FROZEN_SIGN, "horizon": HORIZON, "entry_delay": ENTRY_DELAY,
                       "zscore": "alpha_agent.alpha_recovery.options_surface._z (60, strictly prior)",
                       "cost_ladder_bps_per_side": list(COST_LADDER_BPS)},
            "input_data_identity": _identity(SKEW_PATH, tr),
            "informs_live_candidate": False,
            "why_not_informative_for_live_candidate": (
                "signal-only census: SKEW z vs the live SPY OPRA skew z correlates -0.109 / +0.026 "
                "(< 0.30 preregistered)"),
            "result": res, "capital_eligible": False}
    if write:
        p = write_artifact(ARTIFACT_NAME, body)
        body["artifact_path"] = str(p)
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
        return {"verdict": "DATA_HOLD", "why": "the SPY total-return source is unavailable: %s" % exc,
                "kill_rule_fired": "DATA", "statistic": {}, "economics": {}, "multiplicity": {},
                "artifact": None, "capital_eligible": False, "scorer": SCORER,
                "input_data_identity": "UNAVAILABLE"}
    res = body["result"]
    prim = (res.get("qualification_by_cost_bps_per_side") or {}).get("%.1f" % COST_PRIMARY_BPS) or {}
    return {
        "verdict": res["verdict"], "why": res["why"], "kill_rule_fired": res.get("gate"),
        "statistic": {"lockbox_t": prim.get("t_net"),
                      "lockbox_window": "QUALIFICATION %s..%s (never used for this purpose before)"
                                        % QUALIFICATION,
                      "p_one_sided": prim.get("p_net_one_sided"),
                      "periods": prim.get("periods"),
                      "timing_alpha_t": (res.get("timing_alpha") or {}).get("t"),
                      "confirmation": res.get("confirmation")},
        "economics": {"ann_net_1bp": prim.get("ann_net"), "sharpe": prim.get("sharpe"),
                      "max_dd": prim.get("max_dd"),
                      "partition_ann_net": res.get("partition_ann_net"),
                      "qualification_gross": res.get("qualification_gross")},
        "multiplicity": res.get("multiplicity") or {"m": 1 + len(INHERITED_NULLS), "q": BH_Q,
                                                    "state": "NOT_REACHED"},
        "artifact": body.get("artifact_path"), "capital_eligible": False, "scorer": SCORER,
        "input_data_identity": body["input_data_identity"]}
