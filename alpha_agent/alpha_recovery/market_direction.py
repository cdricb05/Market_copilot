"""alpha_agent.alpha_recovery.market_direction - where is the market going?

Workstream 8. The estate has never produced an explicit, CALIBRATED directional
forecast for the broad US equity market. This module builds and tests one on
the canonical target (SPY total return, owned Norgate history from 1993) at
1, 5, 21 and 63 sessions, from genuinely distinct owned information: the
price state, implied-volatility level and term structure, credit conditions,
the Treasury term structure and rates expectations, inflation expectations,
funding conditions, ALFRED-vintage macro change and surprises, the risk-
appetite composite (all from ``alpha_agent.r63.features.market_conditioner_blocks``,
PIT by declared lag / vintage) and CFTC ES speculative positioning (six-day
publication lag).

Models (protocol section market_direction), refit on the R63 yearly expanding
folds with purge and embargo, lockbox 2023+:

    climatology     the training base rate: the honest no-information baseline
    probability     L2-regularised logistic regression, penalty by blocked
                    inner cross-validation on log loss
    expected return ridge regression on the same features; uncertainty = the
                    training residual standard deviation

Two distinctions are kept visible: a high hit rate without economic P&L is
not alpha; a profitable overlay with uncalibrated probabilities is not a
calibrated forecast. The output is written in the forecast contract, where a
probability is VALUE only with its out-of-sample calibration record.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from alpha_agent.r63 import COT_PUBLICATION_LAG_DAYS, FUTURES_DISCOVERY_START, LOCKBOX_START
from alpha_agent.r63 import features as FE
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import HORIZONS, SPY_PROXY_COST_BPS, write_artifact
from . import forecast_contract as FC

CALCULATION_OWNER = "alpha_agent.alpha_recovery.market_direction"
ARTIFACT_NAME = "market_direction.json"
PPY = 252.0
TARGET = "SPY"
COST = SPY_PROXY_COST_BPS / 1e4
L2_GRID = (0.1, 1.0, 10.0, 100.0)
#: complete rows (every conditioner finite) a horizon needs; the effective-
#: period floor (36) still applies to every verdict through the folds
MIN_ROWS = 120
CAL_BINS = 10
ECE_MAX = 0.05
BLOCKS = ("VOLATILITY_EXPECTATIONS_IV", "CREDIT_CONDITIONS", "TERM_STRUCTURE", "RATES_EXPECTATIONS",
          "INFLATION_EXPECTATIONS", "FUNDING_LIQUIDITY_CONDITIONS", "MACRO_CHANGE", "MACRO_SURPRISES",
          "RISK_APPETITE")

V_CALIBRATED = "CALIBRATED_DIRECTIONAL_SKILL"
V_PROFITABLE_NOT_CAL = "PROFITABLE_NOT_CALIBRATED"
V_CAL_NOT_PROFITABLE = "CALIBRATED_NOT_PROFITABLE"
V_NONE = "NO_DIRECTIONAL_SKILL"
VERDICTS = (V_CALIBRATED, V_PROFITABLE_NOT_CAL, V_CAL_NOT_PROFITABLE, V_NONE)

_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Substrate
# --------------------------------------------------------------------------- #
def load_target() -> tuple:
    """(dates, simple session returns) of SPY total return, own calendar."""
    if "spy" in _CACHE:
        return _CACHE["spy"]
    s = P.load_norgate_total_return((TARGET,)).get(TARGET)
    if s is None or len(s) < 2000:
        raise FileNotFoundError("SPY total-return series unavailable")
    dates = np.array([d.strftime("%Y-%m-%d") for d in s.index])
    lvl = s.to_numpy(dtype=float)
    r = np.full(len(lvl), np.nan)
    r[1:] = lvl[1:] / lvl[:-1] - 1.0
    _CACHE["spy"] = (dates, r)
    return _CACHE["spy"]


def es_positioning(dates: np.ndarray) -> np.ndarray:
    """CFTC ES speculative net positioning (z over 252 sessions), six-day lag."""
    try:
        cot = P.load_cot()
    except Exception:                                    # noqa: BLE001
        return np.full(len(dates), np.nan)
    g = cot[cot["market"] == "ES"] if len(cot) else cot
    if len(g) == 0:
        return np.full(len(dates), np.nan)
    s = pd.Series(g["spec_net"].to_numpy(dtype=float), index=pd.to_datetime(g["as_of"]))
    on_grid = pit.as_of(s, dates, lag_sessions=0, publication_lag_days=COT_PUBLICATION_LAG_DAYS)
    return FE._z1(on_grid, 252)


def features(dates: np.ndarray, r: np.ndarray) -> tuple:
    """(X, names) on the session grid; every column known at close t."""
    key = "features"
    if key in _CACHE:
        return _CACHE[key]
    rr = r[None, :]
    cum = FE._cum(rr)
    price = {"ret_21": FE._ret_over(rr, 21)[0], "ret_63": FE._ret_over(rr, 63)[0],
             "trend_252_21": FE._ret_over(rr, 252, 21)[0],
             "vol_ratio_21_252": (FE._roll(rr, 21, "std") / FE._roll(rr, 252, "std"))[0],
             "drawdown_252": np.expm1(cum - FE._roll(cum, 252, "max"))[0]}
    cond = FE.market_conditioner_blocks(dates, equity_ret=r, scope_returns=None)
    cols, names = [], []
    for k, v in price.items():
        cols.append(v)
        names.append("PRICE_STATE:%s" % k)
    for b in BLOCKS:
        blk = cond[b]
        blk = blk[0] if blk.ndim == 3 else blk
        for j in range(blk.shape[-1]):
            cols.append(blk[:, j])
            names.append("%s:%d" % (b, j))
    cols.append(es_positioning(dates))
    names.append("POSITIONING_COMMITMENTS:es_spec_net_z")
    X = np.column_stack(cols)
    _CACHE[key] = (X, names)
    return X, names


# --------------------------------------------------------------------------- #
# Models
# --------------------------------------------------------------------------- #
def _logistic_fit(X: np.ndarray, y: np.ndarray, lam: float, iters: int = 50) -> np.ndarray:
    """L2 logistic regression by Newton's method (intercept unpenalised)."""
    n, k = X.shape
    Z = np.column_stack([np.ones(n), X])
    w = np.zeros(k + 1)
    reg = np.full(k + 1, lam)
    reg[0] = 0.0
    for _ in range(iters):
        z = Z @ w
        p = 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))
        g = Z.T @ (p - y) + reg * w
        W = p * (1.0 - p)
        H = (Z * W[:, None]).T @ Z + np.diag(reg) + 1e-9 * np.eye(k + 1)
        step = np.linalg.solve(H, g)
        w_new = w - step
        if np.max(np.abs(w_new - w)) < 1e-8:
            w = w_new
            break
        w = w_new
    return w


def _logistic_predict(X: np.ndarray, w: np.ndarray) -> np.ndarray:
    z = np.column_stack([np.ones(len(X)), X]) @ w
    return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))


def _log_loss(p: np.ndarray, y: np.ndarray) -> float:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def _select_lambda(X: np.ndarray, y: np.ndarray, grid: tuple = L2_GRID) -> float:
    folds = pit.blocked_inner_folds(len(y), 3, gap=63)
    if not folds:
        return grid[-1]
    best, best_loss = grid[-1], None
    for lam in grid:
        losses = []
        for fit, hold in folds:
            w = _logistic_fit(X[fit], y[fit], lam)
            losses.append(_log_loss(_logistic_predict(X[hold], w), y[hold]))
        m = float(np.mean(losses))
        if best_loss is None or m < best_loss:
            best, best_loss = lam, m
    return best


def _select_ridge(X: np.ndarray, y: np.ndarray, grid: tuple = L2_GRID) -> float:
    folds = pit.blocked_inner_folds(len(y), 3, gap=63)
    if not folds:
        return grid[-1]
    best, best_err = grid[-1], None
    for lam in grid:
        errs = []
        for fit, hold in folds:
            b = S._ridge_fit(X[fit], y[fit], lam)
            errs.append(float(np.mean((y[hold] - S._ridge_predict(X[hold], b)) ** 2)))
        m = float(np.mean(errs))
        if best_err is None or m < best_err:
            best, best_err = lam, m
    return best


# --------------------------------------------------------------------------- #
# Calibration and economics
# --------------------------------------------------------------------------- #
def calibration_bins(p: np.ndarray, y: np.ndarray, bins: int = CAL_BINS) -> dict:
    edges = np.linspace(0.0, 1.0, bins + 1)
    rows = []
    ece = 0.0
    n = len(p)
    for i in range(bins):
        m = (p >= edges[i]) & (p < edges[i + 1] if i < bins - 1 else p <= edges[i + 1])
        if m.sum() == 0:
            rows.append({"bin": i, "lo": float(edges[i]), "hi": float(edges[i + 1]), "n": 0})
            continue
        mp, my = float(p[m].mean()), float(y[m].mean())
        ece += m.sum() / n * abs(mp - my)
        rows.append({"bin": i, "lo": float(edges[i]), "hi": float(edges[i + 1]), "n": int(m.sum()),
                     "mean_forecast": mp, "observed_frequency": my})
    return {"bins": rows, "expected_calibration_error": float(ece)}


def overlay_returns(p: np.ndarray, y_ret: np.ndarray, base_rate: np.ndarray, *, mode: str) -> tuple:
    """Per-period net return of a long-only SPY exposure rule and its
    turnover. A: hold when p >= base rate else cash; B: exposure = clip(p /
    base rate, 0, 1)."""
    if mode == "A":
        w = (p >= base_rate).astype(float)
    else:
        w = np.clip(p / np.where(base_rate > 0, base_rate, 0.5), 0.0, 1.0)
    prev = np.concatenate([[0.0], w[:-1]])
    traded = np.abs(w - prev)
    net = w * y_ret - traded * COST
    return net, traded


def _econ(net: np.ndarray, bh: np.ndarray, *, ppy: float, lag: int) -> dict:
    diff = net - bh
    st = S.nw_tstat(diff, lag)
    sd_n = float(np.std(net, ddof=1)) if len(net) > 2 else float("nan")
    sd_b = float(np.std(bh, ddof=1)) if len(bh) > 2 else float("nan")
    return {"overlay_ann_net": float(net.mean() * ppy), "buy_hold_ann_net": float(bh.mean() * ppy),
            "overlay_sharpe": float(net.mean() / sd_n * math.sqrt(ppy)) if sd_n and sd_n > 0 else None,
            "buy_hold_sharpe": float(bh.mean() / sd_b * math.sqrt(ppy)) if sd_b and sd_b > 0 else None,
            "overlay_max_dd": S._max_dd(net), "buy_hold_max_dd": S._max_dd(bh),
            "ann_increment": float(diff.mean() * ppy), "t_increment": st["t"],
            "p_increment_one_sided": st["p_one_sided"]}


# --------------------------------------------------------------------------- #
# One horizon
# --------------------------------------------------------------------------- #
#: RESCUE (protocol family budget: max 2, each with a named measured binding
#: failure). Binding failure measured by the primary cells: complete rows
#: begin 2011-06-27 because the NFCI vintages (FUNDING_LIQUIDITY_CONDITIONS)
#: and the ICSA vintages begin then, leaving 113-115 OOS decisions at 21 and 63
#: sessions. The rescue drops every feature whose history begins after
#: 2003-08-08 (the breakeven-inflation start) and re-runs the SAME models at
#: the two horizons the sample bound most. Nothing else changes.
RESCUE_FEATURE_CUTOFF = "2003-08-08"
RESCUE_HORIZONS = (21, 63)


def run_horizon(h: int, *, verbose: bool = True, rescue: bool = False) -> dict:
    dates, r = load_target()
    X, names = features(dates, r)
    if rescue:
        keep = []
        for j in range(X.shape[1]):
            fin = np.where(np.isfinite(X[:, j]))[0]
            keep.append(bool(len(fin)) and str(dates[fin[0]]) <= RESCUE_FEATURE_CUTOFF)
        X = X[:, np.array(keep)]
        names = [n for n, k in zip(names, keep) if k]
    cad = int(min(h, 21))
    y_ret = pit.forward_compound(r[None, :], h)[0]
    dec = pit.decision_indices(dates, FUTURES_DISCOVERY_START, cad, h)
    first_finite = {}
    for j, nm in enumerate(names):
        fin = np.where(np.isfinite(X[:, j]))[0]
        first_finite[nm] = str(dates[fin[0]]) if len(fin) else None
    rows_ok = np.isfinite(X[dec]).all(axis=1) & np.isfinite(y_ret[dec])
    dec = dec[rows_ok]
    if len(dec) < MIN_ROWS:
        return {"horizon": h, "state": "DATA_HOLD", "why": "too few complete rows (%d < %d)" % (len(dec), MIN_ROWS),
                "feature_first_finite": first_finite}
    Xd, yr = X[dec], y_ret[dec]
    yb = (yr > 0).astype(float)
    folds = pit.walk_forward(dates, dec, horizon=h)
    pos = np.arange(len(dec))
    p_hat = np.full(len(dec), np.nan)
    p_clim = np.full(len(dec), np.nan)
    mu_hat = np.full(len(dec), np.nan)
    sig_hat = np.full(len(dec), np.nan)
    kind = np.array([""] * len(dec), dtype=object)
    per_fold = []
    # fold floors scale with the cadence: 200 daily rows or 60 monthly rows
    min_tr = 200 if cad == 1 else 60
    for f in folds:
        tr, te = f["train"], f["test"]
        if len(tr) < min_tr or len(te) < 5:
            continue
        sc = S._fit_scaler(Xd[tr])
        Xtr, Xte = S._apply_scaler(Xd[tr], sc), S._apply_scaler(Xd[te], sc)
        lam = _select_lambda(Xtr, yb[tr])
        w = _logistic_fit(Xtr, yb[tr], lam)
        p_hat[te] = _logistic_predict(Xte, w)
        p_clim[te] = float(yb[tr].mean())
        lo, hi = np.nanpercentile(yr[tr], [1, 99])
        ytr = np.clip(yr[tr], lo, hi)
        lam_r = _select_ridge(Xtr, ytr)
        b = S._ridge_fit(Xtr, ytr, lam_r)
        mu_hat[te] = S._ridge_predict(Xte, b)
        sig_hat[te] = float(np.std(ytr - S._ridge_predict(Xtr, b), ddof=1))
        kind[te] = f["kind"]
        bs = float(np.mean((p_hat[te] - yb[te]) ** 2))
        bc = float(np.mean((p_clim[te] - yb[te]) ** 2))
        per_fold.append({"kind": f["kind"], "test_year": f["test_year"], "lambda_logit": lam,
                         "lambda_ridge": lam_r, "brier": bs, "brier_climatology": bc,
                         "brier_skill": 1.0 - bs / bc if bc > 0 else None, "n": int(len(te))})
    ok = np.isfinite(p_hat)
    if ok.sum() < 100:
        return {"horizon": h, "state": "DATA_HOLD", "why": "no scored folds"}
    p, pc, yy, ret, mu, sg = p_hat[ok], p_clim[ok], yb[ok], yr[ok], mu_hat[ok], sig_hat[ok]
    kd = kind[ok]
    lag = pit.nw_lag(h, cad)
    ppy = PPY / h
    brier_diff = (pc - yy) ** 2 - (p - yy) ** 2       # positive = model better
    st_b = S.nw_tstat(brier_diff, lag)
    bs, bc = float(np.mean((p - yy) ** 2)), float(np.mean((pc - yy) ** 2))
    cal = calibration_bins(p, yy)
    hit_model = float(np.mean((p >= 0.5) == (yy > 0.5)))
    hit_up = float(np.mean(yy))
    # AUC by rank statistic
    order = np.argsort(p)
    ranks = np.empty(len(p))
    ranks[order] = np.arange(1, len(p) + 1)
    n_pos, n_neg = float(yy.sum()), float((1 - yy).sum())
    auc = float((ranks[yy > 0.5].sum() - n_pos * (n_pos + 1) / 2) / (n_pos * n_neg)) if n_pos and n_neg else None
    # expected-return calibration: realised on predicted
    okm = np.isfinite(mu)
    slope = corr = None
    if okm.sum() > 30 and np.std(mu[okm]) > 0:
        A = np.column_stack([np.ones(okm.sum()), mu[okm]])
        beta = np.linalg.lstsq(A, ret[okm], rcond=None)[0]
        slope = float(beta[1])
        corr = float(np.corrcoef(mu[okm], ret[okm])[0, 1])
    # realised return by forecast tercile
    terc = pd.qcut(pd.Series(p).rank(method="first"), 3, labels=False).to_numpy()
    by_terc = {int(t): {"n": int((terc == t).sum()), "mean_forecast": float(p[terc == t].mean()),
                        "realised_mean_return": float(ret[terc == t].mean()),
                        "realised_up_frequency": float(yy[terc == t].mean())} for t in range(3)}
    # economics
    econ = {}
    bh = ret
    for mode in ("A", "B"):
        net, traded = overlay_returns(p, ret, pc, mode=mode)
        e = _econ(net, bh, ppy=ppy, lag=lag)
        e["mean_oneway_turnover"] = float(traded.mean() / 2.0)
        lock = kd == "LOCKBOX"
        e["lockbox"] = _econ(net[lock], bh[lock], ppy=ppy, lag=lag) if lock.sum() > 5 else None
        e["selection"] = _econ(net[~lock], bh[~lock], ppy=ppy, lag=lag) if (~lock).sum() > 5 else None
        econ["overlay_%s" % mode] = e
    skill_folds = [f for f in per_fold if f.get("brier_skill") is not None]
    share_pos = float(np.mean([f["brier_skill"] > 0 for f in skill_folds])) if skill_folds else None
    bss = 1.0 - bs / bc if bc > 0 else None
    calibrated = bool(bss is not None and bss > 0 and (st_b["t"] or 0) >= 2.0
                      and cal["expected_calibration_error"] <= ECE_MAX and (share_pos or 0) >= 0.6)
    profitable = any(((e.get("ann_increment") or 0) > 0 and (e.get("t_increment") or 0) >= 2.0)
                     for e in econ.values())
    v = (V_CALIBRATED if calibrated and profitable else V_PROFITABLE_NOT_CAL if profitable
         else V_CAL_NOT_PROFITABLE if calibrated else V_NONE)
    lock = kd == "LOCKBOX"
    out = {"horizon": h, "cadence": cad, "state": "OK", "target": TARGET, "n_oos": int(len(p)),
           "tag": "RESCUE" if rescue else "PRIMARY",
           "rescue_binding_failure": ("EFFECTIVE_SAMPLE: complete rows begin 2011-06-27 (NFCI / ICSA vintages)"
                                      if rescue else None),
           "n_lockbox": int(lock.sum()), "n_features": int(Xd.shape[1]), "features": names,
           "first_complete_decision": str(dates[dec[0]]), "n_complete_decisions": int(len(dec)),
           "feature_first_finite": first_finite,
           "brier": bs, "brier_climatology": bc, "brier_skill_score": bss, "t_brier_improvement": st_b["t"],
           "log_loss": _log_loss(p, yy), "log_loss_climatology": _log_loss(pc, yy),
           "calibration": cal, "hit_rate_model": hit_model, "hit_rate_always_up": hit_up, "auc": auc,
           "share_folds_with_positive_skill": share_pos, "folds": per_fold,
           "expected_return_calibration": {"slope_realised_on_predicted": slope, "correlation": corr,
                                           "mean_uncertainty": float(np.nanmean(sg)) if okm.sum() else None},
           "realised_by_forecast_tercile": by_terc, "economics": econ,
           "lockbox": {"brier_skill_score": (1.0 - float(np.mean((p[lock] - yy[lock]) ** 2))
                                             / float(np.mean((pc[lock] - yy[lock]) ** 2))) if lock.sum() > 5 else None,
                       "hit_rate_model": float(np.mean((p[lock] >= 0.5) == (yy[lock] > 0.5))) if lock.sum() > 5 else None,
                       "hit_rate_always_up": float(yy[lock].mean()) if lock.sum() > 5 else None},
           "verdicts": {"calibrated": calibrated, "profitable_overlay": profitable, "verdict": v},
           "latest": {"date": str(dates[dec[ok][-1]]), "probability_up": float(p[-1]),
                      "climatology": float(pc[-1]), "expected_return": float(mu[-1]) if np.isfinite(mu[-1]) else None,
                      "uncertainty": float(sg[-1]) if np.isfinite(sg[-1]) else None}}
    return out


def today_product(res: dict) -> dict:
    """The forecast-contract product for the latest decision: probabilities
    are VALUE only when the horizon's OOS calibration passed."""
    v = res.get("verdicts") or {}
    latest = res.get("latest") or {}
    h = int(res["horizon"])
    cal_ok = bool(v.get("calibrated"))
    rec = FC.calibration_record(calibrated=cal_ok, method="L2 logistic, walk-forward, lockbox 2023+",
                                oos_test={"brier_skill_score": res.get("brier_skill_score"),
                                          "expected_calibration_error": (res.get("calibration") or {}).get("expected_calibration_error"),
                                          "t_brier_improvement": res.get("t_brier_improvement")},
                                n_oos_periods=int(res.get("n_oos") or 0))
    erc = res.get("expected_return_calibration") or {}
    er_ok = bool(erc.get("slope_realised_on_predicted") is not None and erc["slope_realised_on_predicted"] > 0
                 and erc.get("correlation") is not None and erc["correlation"] > 0 and cal_ok)
    rec_er = FC.calibration_record(calibrated=er_ok, method="ridge, walk-forward; slope of realised on predicted",
                                   oos_test={"slope": erc.get("slope_realised_on_predicted"), "correlation": erc.get("correlation")},
                                   n_oos_periods=int(res.get("n_oos") or 0))
    p_up = (FC.probability(latest.get("probability_up"), calibration=rec) if cal_ok and latest.get("probability_up") is not None
            else FC.unavailable("OOS calibration failed for this horizon (verdict %s); the raw model "
                                "probability is not reported as a forecast" % v.get("verdict"), "probability"))
    er = (FC.expected_return(latest.get("expected_return"), calibration=rec_er) if er_ok and latest.get("expected_return") is not None
          else FC.unavailable("no OOS-calibrated expected-return mapping", "fraction over horizon"))
    return FC.market_forecast(
        instrument=TARGET, as_of=str(latest.get("date")), horizon_sessions=h, model_id="alpha_recovery_market_direction_v1",
        probability_up=p_up, expected_return=er,
        expected_excess_return=FC.unavailable("excess over cash not modelled at this stage"),
        uncertainty=(FC.field(latest.get("uncertainty"), units="training residual std") if latest.get("uncertainty") is not None
                     else FC.unavailable("no ridge fit")),
        downside_probability=FC.unavailable("no calibrated downside model"),
        tail_probability=FC.unavailable("no calibrated tail model"),
        regime_probabilities=None, evidence_maturity="HISTORICAL_OOS_ONLY",
        information=["PRICE_STATE"] + list(BLOCKS) + ["POSITIONING_COMMITMENTS"],
        notes="raw_probability_up=%.4f climatology=%.4f (reported for transparency; not a forecast unless VALUE)"
              % (latest.get("probability_up") or float("nan"), latest.get("climatology") or float("nan")))


def run(horizons: tuple = HORIZONS, *, verbose: bool = True, write: bool = True) -> dict:
    res = {}
    products = {}
    for h in horizons:
        if verbose:
            print("[direction] horizon %d ..." % h, flush=True)
        r = run_horizon(int(h), verbose=verbose)
        res[str(h)] = r
        if r.get("state") == "OK":
            products[str(h)] = today_product(r)
            if verbose:
                print("    bss=%.4f t=%.2f ece=%.3f hit=%.3f/%.3f verdict=%s" % (
                    r["brier_skill_score"] or 0, r["t_brier_improvement"] or 0,
                    r["calibration"]["expected_calibration_error"], r["hit_rate_model"], r["hit_rate_always_up"],
                    r["verdicts"]["verdict"]), flush=True)
    rescue = {}
    for h in RESCUE_HORIZONS:
        if verbose:
            print("[direction] RESCUE horizon %d (features with history from <= %s) ..." % (h, RESCUE_FEATURE_CUTOFF), flush=True)
        rescue[str(h)] = run_horizon(int(h), verbose=verbose, rescue=True)
    body = {"schema": "alpha_recovery_market_direction/1", "calculation_owner": CALCULATION_OWNER,
            "target": TARGET, "horizons": res, "rescue": rescue,
            "family_budget": {"primary": len(res), "rescue": len(rescue), "primary_max": 6, "rescue_max": 2},
            "forecast_products_today": products,
            "every_product_valid": all(FC.validate(p)["valid"] for p in products.values()),
            "distinctions": {"hit_rate_is_not_alpha": True, "profit_is_not_calibration": True},
            "data_staleness_note": "FRED / Cboe conditioners in the owned panel end 2026-08-21; the latest "
                                   "decision row is the last complete one"}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
