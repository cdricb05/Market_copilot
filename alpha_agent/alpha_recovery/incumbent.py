"""alpha_agent.alpha_recovery.incumbent - how good is fundamental_momentum_50_50_v1, really?

Workstream 1. Two evidence types, measured SEPARATELY and never pooled:

    A. HISTORICAL OOS   the incumbent's shape (0.5 z(fundamental) + 0.5 z(momentum),
                        the R58 B0 diagnostic) reconstructed on the R63 equity
                        substrate: the R57 PIT S&P 500 panel joined to PANEL-F
                        (owned SEC companyfacts, real filed dates, delisted names
                        retained). The momentum leg is EXACT; the fundamental leg
                        is the PIT-faithful R58 rebuild, because the operational
                        composite_sn panel is survivorship-biased and stale
                        (R58 finding) and cannot be evidence. The blend has no
                        fitted parameter, so every period is out of sample; the
                        selection (< 2023) / lockbox (>= 2023) split is kept so
                        the numbers are comparable with 8,380 prior hypotheses.
    B. TRUE_FORWARD     the desk ledgers, READ ONLY: forward_prediction_outcomes
                        (matured sessions since 2026-07-24 at h = 1, 5, 20 for
                        the blend AND its two legs) and forward_performance (the
                        realised alpha_paper_book_1 path since 2026-07-22).

For each supported horizon (1, 5, 21, 63 sessions): per-period rank IC,
top-minus-bottom decile spread, top-25 / top-50 equal-weight books against the
equal-weight scored universe (12.5bp per side, NEXT_CLOSE), net and gross excess,
Sharpe, drawdown, turnover, hit rate, Newey-West t, effective sample, 3-year
blocks, trend regimes, leg attribution, and a decile calibration curve estimated
on selection periods and TESTED on the lockbox - the only way a rank score can
be translated into economic units without pretending it is an expected return.

The verdict rule is frozen in the protocol. Research only; no live write.
"""
from __future__ import annotations

import math
import os
from pathlib import Path

import numpy as np
import pandas as pd

from alpha_agent.r63 import EQUITY_DISCOVERY_START, features as FE, pit
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import sensitivity as S

from . import (EQ_COST_RATE_PER_SIDE, EQ_TOP_N_OPERATIONAL, EQ_TOP_N_SHADOW, HORIZONS,
               INCUMBENT_BLEND, INCUMBENT_LEGS, INCUMBENT_MODEL_ID, IV_MATERIAL, IV_NEGATIVE,
               IV_WEAK, LOCKBOX_START, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS, read_json,
               write_artifact)
from . import forecast_contract as FC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.incumbent"
ARTIFACT_NAME = "incumbent_baseline.json"
PPY = 252.0
OPERATIONAL_HORIZON = 21
WINSOR = (0.01, 0.99)
MIN_NAMES = 50

DESK_DIR_ENV = "PAPER_TRADER_ALPHA_RECOVERY_DESK_DIR"
DEFAULT_DESK_DIR = Path(r"C:\Users\binis\.paper_trader\paper_trading_desk")
OUTCOMES_LEDGER = "forward_prediction_outcomes.json"
PERFORMANCE_LEDGER = "forward_performance.json"
SNAPSHOTS_LEDGER = "forward_prediction_snapshots.json"
FORWARD_MODELS = (INCUMBENT_MODEL_ID, INCUMBENT_LEGS["fundamental"], INCUMBENT_LEGS["momentum"])
FORWARD_HORIZONS = (1, 5, 20)

BLOCK_SCORE = "INCUMBENT_SCORE"
BLOCK_FUND = "INCUMBENT_FUNDAMENTAL_LEG"
BLOCK_MOM = "INCUMBENT_MOMENTUM_LEG"

_CACHE: dict = {}


def desk_dir() -> Path:
    return Path(os.environ.get(DESK_DIR_ENV) or DEFAULT_DESK_DIR)


# --------------------------------------------------------------------------- #
# The incumbent's score on the daily grid
# --------------------------------------------------------------------------- #
def xs_z_columns(v: np.ndarray, mask: np.ndarray, *, min_names: int = MIN_NAMES) -> np.ndarray:
    """Winsorised cross-sectional z-score per column inside ``mask`` (R58's
    xs_z, vectorised over the date axis). NaN outside the mask."""
    out = np.full(v.shape, np.nan)
    ok = mask & np.isfinite(v)
    cols = np.where(ok.sum(axis=0) >= min_names)[0]
    for j in cols:
        m = ok[:, j]
        x = v[m, j].astype(np.float64)
        lo, hi = np.quantile(x, WINSOR[0]), np.quantile(x, WINSOR[1])
        x = np.clip(x, lo, hi)
        sd = x.std()
        if not np.isfinite(sd) or sd < 1e-12:
            continue
        out[m, j] = (x - x.mean()) / sd
    return out


def incumbent_blocks(E: dict, elig: np.ndarray) -> dict:
    """INCUMBENT_SCORE (the fixed blend), and its two legs, on the daily grid.
    ``elig`` is the R63 equity eligibility; a PANEL-F core record is required
    exactly as R58's eligibility requires it."""
    key = ("blocks", id(E))
    if key in _CACHE:
        return _CACHE[key]
    fcf = FE._panel_f_on_grid(E, "fcf_to_assets")
    acc = FE._panel_f_on_grid(E, "accruals_to_assets")
    core = FE._panel_f_on_grid(E, "has_core")
    tr = E["price"]["tr"]
    r = tr[:, 1:] / tr[:, :-1] - 1.0
    r = np.concatenate([np.full((tr.shape[0], 1), np.nan), r], axis=1)
    mom = FE._ret_over(r, 126, 21)
    mask = elig & np.isfinite(core) & (core > 0)
    zf = xs_z_columns(fcf, mask)
    za = xs_z_columns(-acc, mask)
    fund_raw = np.where(np.isfinite(zf) & np.isfinite(za), 0.5 * (zf + za), np.nan)
    z_fund = xs_z_columns(fund_raw, mask)
    z_mom = xs_z_columns(mom, mask)
    both = np.isfinite(z_fund) & np.isfinite(z_mom)
    score = np.where(both, INCUMBENT_BLEND["fundamental"] * z_fund
                     + INCUMBENT_BLEND["momentum"] * z_mom, np.nan)
    out = {BLOCK_SCORE: FE._stack(score), BLOCK_FUND: FE._stack(z_fund), BLOCK_MOM: FE._stack(z_mom),
           "_mask": mask, "_ret": r}
    _CACHE[key] = out
    return out


def equity_substrate() -> tuple:
    """(E, elig) - the R63 equity substrate, assembled once per process."""
    if "substrate" in _CACHE:
        return _CACHE["substrate"]
    E = P.load_equity()
    elig = X._equity_eligibility(E)
    _CACHE["substrate"] = (E, elig)
    return E, elig


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def _max_dd(r: np.ndarray) -> float | None:
    return S._max_dd(np.asarray(r, dtype=float)) if len(r) else None


def _halves(x: np.ndarray, ppy: float):
    if len(x) < 4:
        return None
    h = len(x) // 2
    return [float(x[:h].mean() * ppy), float(x[h:].mean() * ppy)]


def _period_stats(rows: pd.DataFrame, *, h: int, cad: int, label: str) -> dict:
    """Statistics over the per-period rows (already filtered)."""
    if len(rows) == 0:
        return {"label": label, "periods": 0}
    # a period's return covers h sessions (windows overlap when h > cadence);
    # annualise by the horizon, as R63's _econ_summary does, and let the
    # Newey-West lag carry the overlap
    ppy = PPY / h
    lag = pit.nw_lag(h, cad)
    ex_net = rows["net_excess"].to_numpy()
    ex_gross = rows["gross_excess"].to_numpy()
    st = S.nw_tstat(ex_net, lag)
    ic = rows["rank_ic"].to_numpy()
    st_ic = S.nw_tstat(ic[np.isfinite(ic)], lag)
    spread = rows["decile_spread"].to_numpy()
    st_sp = S.nw_tstat(spread[np.isfinite(spread)], lag)
    sd = float(np.std(ex_net, ddof=1)) if len(ex_net) > 2 else float("nan")
    strat_net = rows["strat_net"].to_numpy()
    bench_net = rows["bench_net"].to_numpy()
    return {
        "label": label, "periods": int(len(rows)),
        "effective_periods": int(len(rows) * min(1.0, cad / float(h))),
        "first": str(rows["date"].iloc[0]), "last": str(rows["date"].iloc[-1]),
        "ann_net_excess": float(ex_net.mean() * ppy),
        "ann_gross_excess": float(ex_gross.mean() * ppy),
        "ann_strat_net": float(strat_net.mean() * ppy),
        "ann_bench_net": float(bench_net.mean() * ppy),
        "ann_cost_drag": float((ex_gross - ex_net).mean() * ppy),
        "t_net_excess": st["t"], "p_one_sided": st["p_one_sided"],
        "sharpe_excess": (float(ex_net.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None),
        "strat_max_dd": _max_dd(strat_net), "bench_max_dd": _max_dd(bench_net),
        "excess_max_dd": _max_dd(ex_net),
        "hit_rate": float((ex_net > 0).mean()),
        "mean_oneway_turnover_per_period": float(rows["turnover"].mean()),
        "mean_rank_ic": st_ic["mean"], "t_rank_ic": st_ic["t"],
        "mean_decile_spread": st_sp["mean"], "t_decile_spread": st_sp["t"],
        "halves_ann_net_excess": _halves(ex_net, ppy),
        "mean_n_scored": float(rows["n_scored"].mean()),
    }


def _blocks_and_regimes(rows: pd.DataFrame, *, ppy: float) -> dict:
    years = rows["date"].str.slice(0, 4).astype(int).to_numpy()
    ex = rows["net_excess"].to_numpy()
    blocks = {}
    if len(years):
        for y0 in range(int(years.min()) - int(years.min()) % 3, int(years.max()) + 1, 3):
            m = (years >= y0) & (years < y0 + 3)
            if m.sum() >= 6:
                blocks["%d-%d" % (y0, y0 + 2)] = float(ex[m].mean() * ppy)
    reg = {}
    tv = rows["trend_up"].to_numpy()
    up = tv > 0
    reg["trend_up"] = float(ex[up].mean() * ppy) if up.sum() >= 6 else None
    reg["trend_down"] = float(ex[~up].mean() * ppy) if (~up).sum() >= 6 else None
    return {"blocks": blocks,
            "share_blocks_positive": (float(np.mean([v > 0 for v in blocks.values()]))
                                      if blocks else None),
            "regime": reg}


# --------------------------------------------------------------------------- #
# One horizon, one score
# --------------------------------------------------------------------------- #
def run_score(score: np.ndarray, *, E: dict, elig: np.ndarray, ret: np.ndarray, h: int,
              top_n: int, label: str, spy_trend: np.ndarray,
              cost: float = EQ_COST_RATE_PER_SIDE) -> dict:
    """The per-period record of a long-only top-N book built from ``score``
    (n_sym x n_dates) at the R63 cadence for horizon ``h``."""
    dates = E["dates"]
    cad = X.cadence_for(h)
    dec = pit.decision_indices(dates, EQUITY_DISCOVERY_START, cad, h)
    y = pit.forward_compound(ret, h)
    prev_w: dict = {}
    prev_b: dict = {}
    recs = []
    deciles = []
    for t in dec:
        s = score[:, t]
        ok = elig[:, t] & np.isfinite(s) & np.isfinite(y[:, t])
        n = int(ok.sum())
        if n < MIN_NAMES:
            prev_w, prev_b = {}, {}
            continue
        ix = np.where(ok)[0]
        sv, yv = s[ix], y[ix, t]
        order = np.argsort(-sv, kind="mergesort")
        k = min(top_n, n)
        held = ix[order[:k]]
        w = {int(i): 1.0 / k for i in held}
        wb = {int(i): 1.0 / n for i in ix}
        traded = sum(abs(w.get(i, 0.0) - prev_w.get(i, 0.0)) for i in set(w) | set(prev_w))
        btraded = sum(abs(wb.get(i, 0.0) - prev_b.get(i, 0.0)) for i in set(wb) | set(prev_b))
        gross = float(y[held, t].mean())
        bgross = float(yv.mean())
        net = gross - traded * cost
        bnet = bgross - btraded * cost
        ic = S.spearman(sv, yv)
        dq = pd.qcut(pd.Series(sv).rank(method="first"), 10, labels=False).to_numpy()
        dmeans = np.array([float(yv[dq == d].mean() - bgross) for d in range(10)])
        deciles.append(dmeans)
        recs.append({"date": str(dates[t]), "t": int(t), "n_scored": n,
                     "strat_gross": gross, "strat_net": net, "bench_gross": bgross, "bench_net": bnet,
                     "gross_excess": gross - bgross, "net_excess": net - bnet,
                     "turnover": traded / 2.0, "rank_ic": ic,
                     "decile_spread": float(dmeans[9] - dmeans[0]),
                     "trend_up": float(spy_trend[t]) if np.isfinite(spy_trend[t]) else 0.0,
                     "layer": "LOCKBOX" if str(dates[t]) >= LOCKBOX_START else "SELECTION"})
        prev_w, prev_b = w, wb
    rows = pd.DataFrame(recs)
    ppy = PPY / h
    out = {"label": label, "horizon": h, "cadence": cad, "top_n": top_n, "cost_per_side": cost,
           "book": "EQ_LONG_ONLY_TOPN_EW vs EW scored universe", "n_decisions": int(len(dec)),
           "all": _period_stats(rows, h=h, cad=cad, label="ALL_OOS"),
           "selection": _period_stats(rows[rows["layer"] == "SELECTION"] if len(rows) else rows,
                                      h=h, cad=cad, label="SELECTION_PRE_2023"),
           "lockbox": _period_stats(rows[rows["layer"] == "LOCKBOX"] if len(rows) else rows,
                                    h=h, cad=cad, label="LOCKBOX_2023_ON"),
           "stability": _blocks_and_regimes(rows, ppy=ppy) if len(rows) else None,
           "_rows": rows, "_deciles": np.array(deciles) if deciles else None}
    return out


def calibration_curve(res: dict, *, h: int) -> dict:
    """Score decile -> mean forward excess return: estimated on SELECTION
    periods, tested on LOCKBOX periods. A monotone, lockbox-confirmed mapping
    is the only licence for reporting the incumbent's ranks in economic
    units; otherwise expected returns stay UNAVAILABLE."""
    D = res.get("_deciles")
    rows = res.get("_rows")
    if D is None or rows is None or len(rows) == 0:
        return {"state": "UNAVAILABLE", "reason": "no periods"}
    sel = (rows["layer"] == "SELECTION").to_numpy()
    lock = ~sel
    cad = int(res["cadence"])
    lag = pit.nw_lag(h, cad)

    def _curve(mask):
        if mask.sum() < 6:
            return None
        m = D[mask]
        return {"decile_mean_excess": [float(v) for v in m.mean(axis=0)],
                "decile_se": [float(S.nw_tstat(m[:, d], lag)["se"] or 0.0) for d in range(10)],
                "decile_t": [S.nw_tstat(m[:, d], lag)["t"] for d in range(10)],
                "decile_period_std": [float(m[:, d].std(ddof=1)) if len(m) > 2 else None
                                      for d in range(10)],
                "periods": int(mask.sum())}
    c_sel, c_lock = _curve(sel), _curve(lock)
    mono_sel = (S.spearman(np.arange(10), np.array(c_sel["decile_mean_excess"]))
                if c_sel else float("nan"))
    mono_lock = (S.spearman(np.arange(10), np.array(c_lock["decile_mean_excess"]))
                 if c_lock else float("nan"))
    agree = (S.spearman(np.array(c_sel["decile_mean_excess"]), np.array(c_lock["decile_mean_excess"]))
             if c_sel and c_lock else float("nan"))
    calibrated = bool(c_sel and c_lock and np.isfinite(mono_sel) and mono_sel >= 0.5
                      and np.isfinite(agree) and agree >= 0.5
                      and c_lock["decile_mean_excess"][9] > c_lock["decile_mean_excess"][0])
    return {"state": "VALUE" if calibrated else "UNAVAILABLE",
            "method": "score decile -> mean forward excess return over the scored universe; "
                      "estimated on SELECTION periods, tested once on LOCKBOX periods",
            "selection": c_sel, "lockbox": c_lock,
            "monotonicity_selection": None if not np.isfinite(mono_sel) else float(mono_sel),
            "monotonicity_lockbox": None if not np.isfinite(mono_lock) else float(mono_lock),
            "selection_lockbox_agreement": None if not np.isfinite(agree) else float(agree),
            "calibrated": calibrated,
            "reason": None if calibrated else
            "the decile-to-return mapping is not monotone on selection AND confirmed on the lockbox; "
            "the incumbent's rank score cannot be translated into an expected return",
            "units": "fraction over %d sessions, excess over the equal-weight scored universe" % h}


# --------------------------------------------------------------------------- #
# Historical OOS, all horizons
# --------------------------------------------------------------------------- #
def historical(horizons: tuple = HORIZONS, *, verbose: bool = True) -> dict:
    E, elig = equity_substrate()
    blk = incumbent_blocks(E, elig)
    ret = blk["_ret"]
    spy = E["price"]["spy_tr"]
    spy_r = np.full(len(E["dates"]), np.nan)
    spy_r[1:] = spy[1:] / spy[:-1] - 1.0
    spy_trend = FE._ret_over(spy_r[None, :], 252, 21)[0]
    score = blk[BLOCK_SCORE][..., 0]
    zf, zm = blk[BLOCK_FUND][..., 0], blk[BLOCK_MOM][..., 0]
    out = {"horizons": {}, "attribution": {}, "calibration": {}}
    for h in horizons:
        if verbose:
            print("[incumbent] horizon %d ..." % h, flush=True)
        per = {}
        for top_n in (EQ_TOP_N_OPERATIONAL, EQ_TOP_N_SHADOW):
            r = run_score(score, E=E, elig=elig, ret=ret, h=h, top_n=top_n,
                          label="incumbent_top%d" % top_n, spy_trend=spy_trend)
            per["top%d" % top_n] = {k: v for k, v in r.items() if not k.startswith("_")}
            if top_n == EQ_TOP_N_OPERATIONAL:
                out["calibration"][str(h)] = calibration_curve(r, h=h)
        out["horizons"][str(h)] = per
        att = {}
        for name, sc in (("fundamental_only", zf), ("momentum_only", zm), ("blend", score)):
            r = run_score(sc, E=E, elig=elig, ret=ret, h=h, top_n=EQ_TOP_N_OPERATIONAL,
                          label=name, spy_trend=spy_trend)
            att[name] = {"all": r["all"], "lockbox": r["lockbox"]}
        out["attribution"][str(h)] = att
    out["universe"] = {"symbols": int(len(E["symbols"])), "dates": [str(E["dates"][0]), str(E["dates"][-1])],
                       "rule": "R63 equity eligibility AND PANEL-F core record; PIT S&P 500 members, "
                               "delisted names retained"}
    out["reconstruction_faithfulness"] = {
        "momentum_leg": "EXACT (126-session total return skipping 21)",
        "fundamental_leg": "PIT_FAITHFUL_PROXY (R58 a1_composite on PANEL-F: z(fcf_to_assets) + "
                           "z(-accruals_to_assets)); the operational composite_sn panel is "
                           "survivorship-biased and stale and is not evidence",
        "blend": "EXACT shape (0.5 / 0.5 on cross-sectional z-scores)",
        "construction": "equal-weight top-N, monthly-or-faster cadence by horizon, 12.5bp per side; "
                        "the operational 5 % name cap and 25 % sector cap are not binding for an "
                        "equal-weight top-25 (4 %) and are not modelled",
    }
    return out


# --------------------------------------------------------------------------- #
# TRUE_FORWARD, from the desk ledgers (read only)
# --------------------------------------------------------------------------- #
def true_forward() -> dict:
    d = desk_dir()
    out = {"source_dir": str(d), "read_only": True, "evidence_type": "TRUE_FORWARD",
           "never_pooled_with_historical": True}
    j = read_json(d / OUTCOMES_LEDGER)
    rows = (j or {}).get("rows") or []
    outs = [r for r in rows if r.get("kind") == "OUTCOME" and r.get("status") == "MATURED"]
    per = {}
    for model in FORWARD_MODELS:
        for h in FORWARD_HORIZONS:
            ics, top25, spread, dates = [], [], [], []
            for o in outs:
                if o.get("model_id") != model or o.get("horizon") != h:
                    continue
                m = o.get("metrics") or {}
                if m.get("rank_ic_spearman") is not None:
                    ics.append(float(m["rank_ic_spearman"]))
                if m.get("top25_excess_pp") is not None:
                    top25.append(float(m["top25_excess_pp"]) / 100.0)
                if m.get("top_minus_bottom_pp") is not None:
                    spread.append(float(m["top_minus_bottom_pp"]) / 100.0)
                dates.append(str(o.get("market_date")))
            if not ics and not top25:
                continue
            lag = max(0, h // 5)
            st_ic = S.nw_tstat(np.array(ics), lag) if ics else {}
            st_25 = S.nw_tstat(np.array(top25), lag) if top25 else {}
            st_sp = S.nw_tstat(np.array(spread), lag) if spread else {}
            per["%s_h%d" % (model, h)] = {
                "model": model, "horizon": h, "n_matured_sessions": len(dates),
                "effective_independent_observations": int(math.floor(len(dates) / h)) if h else len(dates),
                "first": min(dates) if dates else None, "last": max(dates) if dates else None,
                "mean_rank_ic": st_ic.get("mean"), "t_rank_ic": st_ic.get("t"),
                "mean_top25_excess_vs_spy": st_25.get("mean"), "t_top25_excess": st_25.get("t"),
                "hit_rate_top25_excess": float(np.mean([v > 0 for v in top25])) if top25 else None,
                "mean_top_minus_bottom_decile": st_sp.get("mean"), "t_top_minus_bottom": st_sp.get("t"),
                "sample_warning": "TINY forward sample; a separate gate, never merged with history"}
    out["outcomes_by_model_horizon"] = per
    perf = read_json(d / PERFORMANCE_LEDGER)
    prow = [r.get("row") or {} for r in ((perf or {}).get("rows") or [])]
    prow = [r for r in prow if r.get("nav") is not None]
    if prow:
        nav = np.array([float(r["nav"]) for r in prow])
        bench = np.array([float(r.get("benchmark_close") or np.nan) for r in prow])
        daily = np.array([float(r.get("daily_return_pct") or 0.0) / 100.0 for r in prow])
        b_daily = np.full(len(bench), np.nan)
        b_daily[1:] = bench[1:] / bench[:-1] - 1.0
        ex = daily[1:] - b_daily[1:]
        sd = float(np.std(daily[1:], ddof=1)) if len(daily) > 3 else float("nan")
        sde = float(np.std(ex, ddof=1)) if len(ex) > 3 else float("nan")
        out["realised_book"] = {
            "book_id": prow[-1].get("book_id"), "first": prow[0].get("date"), "last": prow[-1].get("date"),
            "sessions": len(prow), "nav_first": float(nav[0]), "nav_last": float(nav[-1]),
            "cumulative_return": float(prow[-1].get("cumulative_return_pct") or 0.0) / 100.0,
            "benchmark_cumulative_return": float(prow[-1].get("benchmark_cumulative_return_pct") or 0.0) / 100.0,
            "excess_cumulative": (float(prow[-1].get("cumulative_return_pct") or 0.0)
                                  - float(prow[-1].get("benchmark_cumulative_return_pct") or 0.0)) / 100.0,
            "ann_vol": float(sd * math.sqrt(PPY)) if np.isfinite(sd) else None,
            "sharpe_annualised": (float(daily[1:].mean() / sd * math.sqrt(PPY)) if sd and sd > 0 else None),
            "excess_sharpe_annualised": (float(np.nanmean(ex) / sde * math.sqrt(PPY)) if sde and sde > 0 else None),
            "max_drawdown": float(min(float(r.get("drawdown_pct") or 0.0) for r in prow)) / 100.0,
            "total_turnover_oneway": float(sum(float(r.get("turnover_pct") or 0.0) for r in prow)) / 100.0 / 2.0,
            "total_transaction_cost": float(sum(float(r.get("transaction_cost") or 0.0) for r in prow)),
            "holdings_last": int(prow[-1].get("holdings_count") or 0),
            "evidence_maturity": "TRUE_FORWARD_ACCRUING (%d sessions; effective independent monthly "
                                 "observations %d)" % (len(prow), len(prow) // 21),
        }
    return out


def predicts_today(hist: dict | None = None) -> dict:
    """What the incumbent predicts NOW, in economic units where the lockbox-
    tested calibration licenses it, else UNAVAILABLE. Read from the latest
    desk snapshot (read only)."""
    j = read_json(desk_dir() / SNAPSHOTS_LEDGER)
    rows = [r for r in ((j or {}).get("rows") or []) if r.get("model_id") == INCUMBENT_MODEL_ID]
    if not rows:
        return {"state": "NO_SNAPSHOT"}
    snap = rows[-1]
    tw = snap.get("target_weights") or {}
    members = sorted(tw, key=lambda k: -float(tw[k]))
    cal = ((hist or {}).get("calibration") or {}).get(str(OPERATIONAL_HORIZON)) or {}
    licensed = bool(cal.get("calibrated"))
    top_dec = (cal.get("selection") or {}).get("decile_mean_excess", [None] * 10)[9] if licensed else None
    top_sd = (cal.get("selection") or {}).get("decile_period_std", [None] * 10)[9] if licensed else None
    rec = FC.calibration_record(calibrated=licensed, method=cal.get("method") or "none",
                                oos_test={"lockbox_agreement": cal.get("selection_lockbox_agreement"),
                                          "lockbox_monotonicity": cal.get("monotonicity_lockbox")},
                                n_oos_periods=int(((cal.get("lockbox") or {}).get("periods")) or 0))
    products = []
    for i, tk in enumerate(members[:EQ_TOP_N_OPERATIONAL]):
        products.append(FC.equity_forecast(
            ticker=tk, as_of=str(snap.get("market_date")), horizon_sessions=OPERATIONAL_HORIZON,
            model_id=INCUMBENT_MODEL_ID, rank=i + 1, n_ranked=int(snap.get("eligible_universe_count") or 0),
            expected_excess_return=(FC.expected_return(top_dec, calibration=rec,
                                                       units="fraction over 21 sessions vs EW universe")
                                    if licensed and top_dec is not None else
                                    FC.unavailable(cal.get("reason") or "no lockbox-confirmed calibration")),
            uncertainty=(FC.field(top_sd, units="per-period std of the top decile's excess")
                         if licensed and top_sd is not None else FC.unavailable("no calibration")),
            probability_positive=FC.unavailable("the incumbent emits no probability"),
            downside=FC.unavailable("the incumbent emits no downside estimate"),
            information_attribution={"fundamental": INCUMBENT_BLEND["fundamental"],
                                     "momentum": INCUMBENT_BLEND["momentum"]},
            evidence_maturity="TRUE_FORWARD_ACCRUING",
            score_field=FC.score(None, name="fundamental_momentum_50_50_rank", scale="rank"),
            information=["FUNDAMENTAL_LEVELS", "FREE_CASH_FLOW", "MOMENTUM"]))
    return {"snapshot_id": snap.get("snapshot_id"), "market_date": snap.get("market_date"),
            "fundamental_data_as_of": snap.get("fundamental_data_as_of"),
            "eligible_universe_count": snap.get("eligible_universe_count"),
            "top25": members[:EQ_TOP_N_OPERATIONAL], "n_target_weights": len(tw),
            "economic_units_licensed": licensed,
            "top_decile_expected_excess_21s": top_dec, "top_decile_period_std": top_sd,
            "statement": ("the incumbent ranks %d names; its rank score is NOT an expected return; "
                          "%s" % (len(tw), ("the lockbox-confirmed decile mapping puts the top decile at "
                                           "%.4f expected 21-session excess over the scored universe "
                                           "(period std %.4f)" % (top_dec, top_sd)) if licensed and top_dec is not None
                                  else "no calibrated economic translation exists, so expected returns are UNAVAILABLE")),
            "forecast_products_sample": products[:3], "n_products": len(products),
            "every_product_valid": all(FC.validate(p)["valid"] for p in products)}


def verdict(hist: dict) -> dict:
    """The frozen verdict rule (protocol incumbent_baseline.verdict_rule)."""
    op = ((hist.get("horizons") or {}).get(str(OPERATIONAL_HORIZON)) or {}).get("top%d" % EQ_TOP_N_OPERATIONAL) or {}
    a, sel, lock = op.get("all") or {}, op.get("selection") or {}, op.get("lockbox") or {}
    ne, t = a.get("ann_net_excess"), a.get("t_net_excess")
    t_ic = a.get("t_rank_ic")
    sign_ok = (sel.get("ann_net_excess") is not None and lock.get("ann_net_excess") is not None
               and np.sign(sel["ann_net_excess"]) == np.sign(lock["ann_net_excess"]))
    checks = {"net_excess_ge_materiality": bool(ne is not None and ne >= MATERIALITY_ANN_NET),
              "t_net_excess_ge_2": bool(t is not None and t >= 2.0),
              "lockbox_sign_agrees": bool(sign_ok),
              "t_rank_ic_ge_2": bool(t_ic is not None and t_ic >= 2.0),
              "effective_periods_ge_floor": bool((a.get("effective_periods") or 0) >= MIN_EFFECTIVE_PERIODS)}
    if all(checks.values()):
        v = IV_MATERIAL
    elif (ne is not None and ne < 0 and t is not None and t <= -2.0) or (t_ic is not None and t_ic <= -2.0):
        v = IV_NEGATIVE
    else:
        v = IV_WEAK
    return {"verdict": v, "checks": checks, "operational_horizon": OPERATIONAL_HORIZON,
            "top_n": EQ_TOP_N_OPERATIONAL, "ann_net_excess": ne, "t_net_excess": t,
            "t_rank_ic": t_ic, "lockbox_ann_net_excess": lock.get("ann_net_excess"),
            "selection_ann_net_excess": sel.get("ann_net_excess"),
            "rule": "protocol incumbent_baseline.verdict_rule (frozen)",
            "true_forward_moves_the_verdict": False}


def run(*, verbose: bool = True, write: bool = True) -> dict:
    hist = historical(verbose=verbose)
    fwd = true_forward()
    today = predicts_today(hist)
    v = verdict(hist)
    body = {"schema": "alpha_recovery_incumbent_baseline/1", "calculation_owner": CALCULATION_OWNER,
            "model_id": INCUMBENT_MODEL_ID, "role": "BENCHMARK",
            "historical_oos": hist, "true_forward": fwd, "predicts_today": today, "verdict": v,
            "evidence_separation": "historical_oos and true_forward are separate blocks and are never "
                                   "pooled, averaged or compared as if commensurable"}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
