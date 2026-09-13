"""alpha_agent.r63.sensitivity - the mathematical information-sensitivity engine.

One cell = (scope, mode, horizon, information dimension). For every cell the
engine measures, with IDENTICAL rows, dates, realised returns, splits, costs
and a FORCED model configuration on both arms:

    standalone      OOS rank IC of a DIMENSION-ONLY arm (descriptive)
    conditional     PAIRED per-period increment of OOS rank IC (XS) or of the
                    unit-risk timing statistic (TS): BASELINE + DIMENSION
                    minus BASELINE, Newey-West t, one-sided p
    secondary       partial rank IC after residualising on the baseline,
                    OOS R-squared increment per fold, OOS permutation drop
    redundancy      residual share of the dimension composite on the baseline
                    (training rows), max |rank corr| with any baseline feature
    stability       increments by 3-year block, by two PIT regimes, by
                    instrument, and lockbox-versus-selection sign agreement
    economics       a book built from each arm's OOS score, charged the
                    scope's costs on turnover; the increment in net return,
                    Sharpe, drawdown, turnover and cost drag, paired NW t

Everything is walk-forward: expanding yearly folds with purge and embargo,
the ridge penalty chosen by blocked inner CV on the BASELINE arm inside the
training block, the 2023+ block a LOCKBOX evaluated once. Pure numpy/pandas.
"""
from __future__ import annotations

import math

import numpy as np
import pandas as pd

from . import (BH_Q, CONDITIONAL_T_FLOOR, INNER_CV_FOLDS, MATERIALITY_ANN_NET,
               MIN_EFFECTIVE_PERIODS, PARTIAL_RESIDUAL_SHARE_MAX,
               REDUNDANT_RESIDUAL_SHARE_MAX, RIDGE_ALPHAS, STANDALONE_T_FLOOR)
from . import pit

CALCULATION_OWNER = "alpha_agent.r63.sensitivity"
PPY = 252.0
MIN_XS_NAMES = 5
MIN_ROWS = 200                      # a single-instrument scope has ~1 row per slot
COVERAGE_FLOOR = 0.60
WINSOR = (0.01, 0.99)
PERM_SEED = 6301

V_REDUNDANT = "REDUNDANT"
V_NO_VALUE = "NO_CONDITIONAL_VALUE"
V_NOT_ECON = "CONDITIONAL_VALUE_NOT_ECONOMIC"
V_UNSTABLE = "CONDITIONAL_VALUE_UNSTABLE"
V_NOT_FDR = "CONDITIONAL_VALUE_NOT_FDR_SIGNIFICANT"
V_CANDIDATE = "INCREMENTAL_INFORMATION_CANDIDATE"
V_DATA_HOLD = "DATA_HOLD"
V_NO_RESPONSE = "NO_RESPONSE"
VERDICTS = (V_REDUNDANT, V_NO_VALUE, V_NOT_ECON, V_UNSTABLE, V_NOT_FDR,
            V_CANDIDATE, V_DATA_HOLD, V_NO_RESPONSE)

#: (scope, mode, horizon, baseline dims, fold year, fold kind) -> ridge penalty
_ALPHA_CACHE: dict = {}


# --------------------------------------------------------------------------- #
# Statistics
# --------------------------------------------------------------------------- #
def nw_tstat(x, lag: int = 0) -> dict:
    x = np.asarray(x, dtype=float)
    x = x[np.isfinite(x)]
    n = len(x)
    if n < 3:
        return {"n": n, "mean": None, "t": None, "p_one_sided": None, "se": None}
    m = float(x.mean())
    e = x - m
    s2 = float((e * e).sum()) / n
    for k in range(1, min(int(lag), n - 1) + 1):
        w = 1.0 - k / (lag + 1.0)
        s2 += 2.0 * w * float((e[k:] * e[:-k]).sum()) / n
    se = math.sqrt(max(s2, 1e-18) / n)
    t = m / se
    p = 0.5 * math.erfc(t / math.sqrt(2.0))
    return {"n": n, "mean": m, "t": t, "p_one_sided": p, "se": se}


def bh_fdr(pvals: dict, q: float = BH_Q) -> dict:
    items = sorted([(k, float(v)) for k, v in pvals.items()
                    if v is not None and np.isfinite(v)], key=lambda kv: kv[1])
    m = len(items)
    max_i, thresh = 0, None
    for i, (_k, p) in enumerate(items, 1):
        if p <= q * i / m:
            max_i, thresh = i, p
    passed = {k for i, (k, _p) in enumerate(items, 1) if i <= max_i}
    return {"m": m, "q": q, "threshold": thresh, "n_rejected": len(passed),
            "survivors": sorted(passed),
            "per_test": {k: (k in passed) for k, _p in items}}


def _rank(a: np.ndarray) -> np.ndarray:
    return pd.Series(a).rank().to_numpy()


def spearman(a, b) -> float:
    a = np.asarray(a, float)
    b = np.asarray(b, float)
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < MIN_XS_NAMES:
        return np.nan
    ra, rb = _rank(a[ok]), _rank(b[ok])
    if ra.std() == 0 or rb.std() == 0:
        return np.nan
    return float(np.corrcoef(ra, rb)[0, 1])


def grouped_rank_ic(pred: np.ndarray, y: np.ndarray, gid: np.ndarray) -> pd.Series:
    """Per-group Spearman rank correlation, vectorised (groups with fewer than
    MIN_XS_NAMES members are NaN)."""
    df = pd.DataFrame({"g": gid, "p": pred, "y": y})
    df = df[np.isfinite(df["p"]) & np.isfinite(df["y"])]
    if df.empty:
        return pd.Series(dtype=float)
    g = df.groupby("g", sort=True)
    df["rp"] = g["p"].rank()
    df["ry"] = g["y"].rank()
    g = df.groupby("g", sort=True)
    n = g.size()
    mp = df["rp"] - g["rp"].transform("mean")
    my = df["ry"] - g["ry"].transform("mean")
    num = (mp * my).groupby(df["g"]).sum()
    den = np.sqrt((mp * mp).groupby(df["g"]).sum() * (my * my).groupby(df["g"]).sum())
    ic = num / den.replace(0, np.nan)
    ic[n < MIN_XS_NAMES] = np.nan
    return ic


def grouped_timing_stat(pred: np.ndarray, y_scaled: np.ndarray, gid: np.ndarray,
                        pred_scale: float) -> pd.Series:
    """Per-date unit-risk timing P&L: mean over instruments of
    tanh(pred / scale) x vol-scaled realised return.

    tanh, not a hard clip: a clip saturates at +/-1 whenever the score leaves
    the training range (a new regime), both arms then hold the identical
    saturated position and the paired increment is identically zero - which
    the first grid pass measured as t_lockbox = 0.00 on several TS cells."""
    s = np.tanh(pred / (pred_scale if pred_scale > 0 else 1.0))
    df = pd.DataFrame({"g": gid, "v": s * y_scaled})
    df = df[np.isfinite(df["v"])]
    return df.groupby("g", sort=True)["v"].mean()


# --------------------------------------------------------------------------- #
# Preprocessing
# --------------------------------------------------------------------------- #
def _fit_scaler(X: np.ndarray) -> dict:
    lo = np.nanpercentile(X, WINSOR[0] * 100, axis=0)
    hi = np.nanpercentile(X, WINSOR[1] * 100, axis=0)
    Xc = np.clip(X, lo, hi)
    mu = np.nanmean(Xc, axis=0)
    sd = np.nanstd(Xc, axis=0)
    sd = np.where(np.isfinite(sd) & (sd > 1e-12), sd, 1.0)
    return {"lo": lo, "hi": hi, "mu": mu, "sd": sd}


def _apply_scaler(X: np.ndarray, sc: dict) -> np.ndarray:
    Z = (np.clip(X, sc["lo"], sc["hi"]) - sc["mu"]) / sc["sd"]
    return np.where(np.isfinite(Z), Z, 0.0)


def _ridge_fit(X: np.ndarray, y: np.ndarray, alpha: float) -> np.ndarray:
    n, k = X.shape
    Xa = np.column_stack([np.ones(n), X])
    A = Xa.T @ Xa
    # features are training-standardised, so X'X ~ n on the diagonal and the
    # penalty alpha * n is a RELATIVE shrinkage that does not drift with fold size
    A[1:, 1:] += alpha * n * np.eye(k)
    b = Xa.T @ y
    try:
        return np.linalg.solve(A, b)
    except np.linalg.LinAlgError:
        return np.linalg.lstsq(A, b, rcond=None)[0]


def _ridge_predict(X: np.ndarray, beta: np.ndarray) -> np.ndarray:
    return beta[0] + X @ beta[1:]


def _select_alpha(X: np.ndarray, y: np.ndarray, gid: np.ndarray, mode: str,
                  y_scaled: np.ndarray | None) -> float:
    """Blocked inner CV on the baseline arm; the score is the per-period mean
    of the cell's own primary statistic. Deterministic."""
    order = np.argsort(gid, kind="stable")
    Xo, yo, go = X[order], y[order], gid[order]
    yso = y_scaled[order] if y_scaled is not None else None
    n = len(yo)
    # split by GROUP boundaries so a date never straddles fit / hold
    ug = np.unique(go)
    if len(ug) < 2 * INNER_CV_FOLDS:
        return RIDGE_ALPHAS[len(RIDGE_ALPHAS) // 2]
    edges = np.linspace(0, len(ug), INNER_CV_FOLDS + 1).astype(int)
    best, best_score = RIDGE_ALPHAS[0], -np.inf
    for a in RIDGE_ALPHAS:
        scores = []
        for i in range(INNER_CV_FOLDS):
            hold_g = ug[edges[i]:edges[i + 1]]
            hold = np.isin(go, hold_g)
            fit = ~hold
            if fit.sum() < 50 or hold.sum() < 20:
                continue
            sc = _fit_scaler(Xo[fit])
            beta = _ridge_fit(_apply_scaler(Xo[fit], sc), yo[fit], a)
            p = _ridge_predict(_apply_scaler(Xo[hold], sc), beta)
            if mode == "XS":
                s = grouped_rank_ic(p, yo[hold], go[hold])
            else:
                s = grouped_timing_stat(p, yso[hold], go[hold], float(np.std(p)) or 1.0)
            if len(s.dropna()):
                scores.append(float(s.mean()))
        if scores and np.mean(scores) > best_score:
            best, best_score = a, float(np.mean(scores))
    return best


def _residual_share(D: np.ndarray, B: np.ndarray) -> dict:
    """Var(D - OLS(D ~ B)) / Var(D) per dimension feature, on the given rows;
    plus the max |Spearman| between any D feature and any B feature."""
    ok = np.isfinite(D).all(axis=1) & np.isfinite(B).all(axis=1)
    if ok.sum() < 200:
        return {"residual_share": None, "max_abs_rank_corr": None,
                "rows": int(ok.sum()), "state": "TOO_FEW_REAL_ROWS"}
    Dm, Bm = D[ok], B[ok]
    if Dm.shape[0] > 20000:
        stride = int(np.ceil(Dm.shape[0] / 20000))
        Dm, Bm = Dm[::stride], Bm[::stride]
    sc = _fit_scaler(Bm)
    Bz = np.column_stack([np.ones(len(Bm)), _apply_scaler(Bm, sc)])
    shares, corrs = [], []
    for j in range(Dm.shape[1]):
        d = Dm[:, j]
        v = float(np.var(d, ddof=1))
        if not np.isfinite(v) or v <= 0:
            continue
        beta = np.linalg.lstsq(Bz, d, rcond=None)[0]
        res = d - Bz @ beta
        shares.append(float(np.var(res, ddof=1) / v))
        rd = _rank(d)
        corrs.append(max(abs(float(np.corrcoef(rd, _rank(Bm[:, q]))[0, 1]))
                         for q in range(Bm.shape[1])))
    if not shares:
        return {"residual_share": None, "max_abs_rank_corr": None,
                "rows": int(ok.sum()), "state": "NO_VARIANCE"}
    rs = float(np.median(shares))
    label = ("REDUNDANT" if rs < REDUNDANT_RESIDUAL_SHARE_MAX else
             "PARTIALLY_REDUNDANT" if rs < PARTIAL_RESIDUAL_SHARE_MAX else "DISTINCT")
    return {"residual_share": rs, "residual_share_min": float(min(shares)),
            "max_abs_rank_corr": float(max(corrs)), "rows": int(ok.sum()),
            "redundancy": label, "state": "OK"}


# --------------------------------------------------------------------------- #
# Books (economic value)
# --------------------------------------------------------------------------- #
def _book_returns(pred: np.ndarray, y: np.ndarray, gid: np.ndarray, iid: np.ndarray,
                  vol: np.ndarray, cost: np.ndarray, mode: str, kind: str,
                  pred_scale: float, top_n: int = 50) -> dict:
    """Per-period gross/net return and one-way turnover of the book built
    from ``pred`` on OOS rows. ``kind``: XS_LONG_SHORT | TS_VOL_TARGET |
    EQ_LONG_ONLY_TOPN (with EW-universe benchmark)."""
    order = np.lexsort((iid, gid))
    p, yy, g, ii, v, c = pred[order], y[order], gid[order], iid[order], vol[order], cost[order]
    periods = np.unique(g)
    gross, net, turn, costs, maxw = [], [], [], [], []
    bgross, bnet = [], []
    prev: dict = {}
    prev_b: dict = {}
    starts = np.searchsorted(g, periods, side="left")
    ends = np.searchsorted(g, periods, side="right")
    for a, b in zip(starts, ends):
        pp, yv, inst, vv, cc = p[a:b], yy[a:b], ii[a:b], v[a:b], c[a:b]
        ok = np.isfinite(pp) & np.isfinite(yv)
        pp, yv, inst, vv, cc = pp[ok], yv[ok], inst[ok], vv[ok], cc[ok]
        w = {}
        if kind == "XS_LONG_SHORT":
            n = len(pp)
            if n < 6:
                continue
            # quintile books need >= 10 names; a small cross-section (FX has
            # nine markets) uses terciles so the scope is judged, not skipped
            k = max(1, int(round(n * (0.2 if n >= 10 else 1.0 / 3.0))))
            o = np.argsort(pp)
            lo, hi = o[:k], o[-k:]
            iv = 1.0 / np.where(np.isfinite(vv) & (vv > 0), vv, np.nan)
            iv = np.where(np.isfinite(iv), iv, np.nanmedian(iv) if np.isfinite(np.nanmedian(iv)) else 1.0)
            # inverse-vol weights are CAPPED per name: an uncapped book hands
            # itself to whichever market has the lowest volatility (a
            # short-rate future at 0.3%/yr took 99.7% of the first-pass
            # cross-asset book and a -100% drawdown with it)
            wl = _capped(iv[hi] / iv[hi].sum(), max(1.0 / k, MAX_NAME_WEIGHT))
            ws = _capped(iv[lo] / iv[lo].sum(), max(1.0 / k, MAX_NAME_WEIGHT))
            for j, x in zip(inst[hi], wl):
                w[int(j)] = float(x)
            for j, x in zip(inst[lo], ws):
                w[int(j)] = w.get(int(j), 0.0) - float(x)
        elif kind == "TS_VOL_TARGET":
            n = len(pp)
            if n == 0:
                continue
            s = np.tanh(pp / (pred_scale if pred_scale > 0 else 1.0))
            lev = 0.10 / np.where(np.isfinite(vv) & (vv > 0.02), vv, np.nan)
            lev = np.where(np.isfinite(lev), np.minimum(lev, 3.0), 0.0)
            for j, x in zip(inst, s * lev / n):
                w[int(j)] = float(x)
        else:  # EQ_LONG_ONLY_TOPN
            n = len(pp)
            if n < MIN_XS_NAMES:
                continue
            k = min(top_n, n)
            hi = np.argsort(-pp)[:k]
            for j in inst[hi]:
                w[int(j)] = 1.0 / k
            wb = {int(j): 1.0 / n for j in inst}
            btr = sum(abs(wb.get(x, 0.0) - prev_b.get(x, 0.0)) for x in set(wb) | set(prev_b))
            bcost = sum(abs(wb.get(x, 0.0) - prev_b.get(x, 0.0)) * cc[np.where(inst == x)[0][0]]
                        if x in set(inst) else abs(prev_b.get(x, 0.0)) * float(np.median(cc))
                        for x in set(wb) | set(prev_b))
            bg = float(yv.mean())
            bgross.append(bg)
            bnet.append(bg - bcost)
            prev_b = wb
        cost_map = {int(j): float(x) for j, x in zip(inst, cc)}
        ret_map = {int(j): float(x) for j, x in zip(inst, yv)}
        traded = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) for x in set(w) | set(prev))
        cost_t = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) * cost_map.get(x, float(np.median(cc)))
                     for x in set(w) | set(prev))
        gr = sum(w[x] * ret_map.get(x, 0.0) for x in w)
        gross.append(gr)
        net.append(gr - cost_t)
        turn.append(traded / 2.0)
        costs.append(cost_t)
        maxw.append(max(abs(x) for x in w.values()) if w else 0.0)
        prev = w
    out = {"gross": np.array(gross), "net": np.array(net), "turnover": np.array(turn),
           "cost": np.array(costs), "max_weight": np.array(maxw)}
    if kind == "EQ_LONG_ONLY_TOPN":
        out["bench_gross"] = np.array(bgross)
        out["bench_net"] = np.array(bnet)
        out["net"] = out["net"] - out["bench_net"] if len(bnet) == len(net) else out["net"]
        out["gross"] = out["gross"] - out["bench_gross"] if len(bgross) == len(gross) else out["gross"]
    return out


MAX_NAME_WEIGHT = 0.25
DEGENERATE_DD = -0.90


def _capped(w: np.ndarray, cap: float, iters: int = 20) -> np.ndarray:
    """Renormalise weights so no name exceeds ``cap`` (sum stays 1)."""
    w = np.asarray(w, dtype=float).copy()
    for _ in range(iters):
        over = w > cap + 1e-12
        if not over.any():
            break
        excess = float((w[over] - cap).sum())
        w[over] = cap
        under = ~over
        if under.sum() == 0 or w[under].sum() <= 0:
            break
        w[under] += excess * w[under] / w[under].sum()
    w = np.minimum(w, cap)
    short = 1.0 - float(w.sum())
    under = w < cap - 1e-12
    if short > 1e-12 and under.any():
        room = cap - w[under]
        w[under] += np.minimum(room, short * room / room.sum())
    return w


def _max_dd(r: np.ndarray) -> float | None:
    if len(r) == 0:
        return None
    nav = np.cumprod(1.0 + r)
    peak = np.maximum.accumulate(nav)
    return float((nav / peak - 1.0).min())


def _econ_summary(book: dict, horizon: int, lag: int) -> dict:
    net, gross = book["net"], book["gross"]
    if len(net) == 0:
        return {"periods": 0}
    ppy = PPY / horizon
    st = nw_tstat(net, lag=lag)
    sd = float(np.std(net, ddof=1)) if len(net) > 2 else np.nan
    return {"periods": int(len(net)),
            "ann_net": float(net.mean() * ppy), "ann_gross": float(gross.mean() * ppy),
            "sharpe": float(net.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None,
            "max_dd": _max_dd(net), "mean_oneway_turnover": float(book["turnover"].mean()),
            "ann_cost_drag": float(book["cost"].mean() * ppy),
            "max_weight": float(book["max_weight"].max()) if len(book["max_weight"]) else None,
            "t_net": st["t"], "p_net_one_sided": st["p_one_sided"]}


# --------------------------------------------------------------------------- #
# The cell
# --------------------------------------------------------------------------- #
def _flatten(ds: dict, dims_b: tuple, dim_d: str) -> dict:
    """Rows (instrument, decision slot) with finite target and finite baseline;
    the dimension block may be NaN (coverage is measured, not filled)."""
    dec = ds["dec"]
    n_i = len(ds["inst"])
    elig = ds["elig"][:, dec]
    y = ds["y"][:, dec]
    ok = elig & np.isfinite(y)
    B = np.concatenate([ds["blocks"][d][:, dec, :] for d in dims_b], axis=-1)
    ok &= np.isfinite(B).all(axis=-1)
    D = ds["blocks"][dim_d][:, dec, :]
    ii, jj = np.where(ok)
    return {"i": ii, "j": jj, "gid": jj, "y": y[ii, jj],
            "B": B[ii, jj, :], "D": D[ii, jj, :],
            "vol": ds["vol"][:, dec][ii, jj] if ds.get("vol") is not None else np.full(len(ii), np.nan),
            "cost": np.asarray(ds["cost"])[ii],
            "y_scaled": ds["y_scaled"][:, dec][ii, jj] if ds.get("y_scaled") is not None else None,
            "n_slots": len(dec), "n_inst": n_i}


def _target(rows: dict, mode: str) -> np.ndarray:
    y = rows["y"].astype(float)
    if mode == "XS":
        df = pd.DataFrame({"g": rows["gid"], "y": y})
        y = (df["y"] - df.groupby("g")["y"].transform("mean")).to_numpy()
    else:
        y = rows["y_scaled"].astype(float)
    return y


def run_cell(ds: dict, dims_b: tuple, dim_d: str, *, top_n: int = 50,
             keep_predictions: bool = False) -> dict:
    """Measure ONE cell. ``ds`` is the dataset from experiments.assemble_*.

    ``keep_predictions`` (R64, additive, default off) attaches the out-of-sample
    scores of BOTH arms and the aligned row facts under ``_predictions`` so a
    later release can price the SAME scores through a different book without
    a second scorer. Every statistic and every default output byte is unchanged.
    """
    mode, h, cad = ds["mode"], int(ds["horizon"]), int(ds["cadence"])
    lag = pit.nw_lag(h, cad)
    rows = _flatten(ds, dims_b, dim_d)
    n_rows = len(rows["y"])
    covered = np.isfinite(rows["D"]).all(axis=1)
    coverage = float(covered.mean()) if n_rows else 0.0
    base = {"scope": ds["scope"], "mode": mode, "horizon": h, "cadence": cad,
            "dimension": dim_d, "baseline": list(dims_b), "rows_total": int(n_rows),
            "rows_covered": int(covered.sum()), "coverage": coverage,
            "coverage_flag": "FULL" if coverage >= COVERAGE_FLOOR else "COVERED_ROWS_ONLY",
            "n_instruments": int(len(np.unique(rows["i"][covered]))) if n_rows else 0}
    if covered.sum() < MIN_ROWS or len(np.unique(rows["gid"][covered])) < 2 * MIN_EFFECTIVE_PERIODS:
        base.update({"verdict": V_DATA_HOLD, "why": "too few covered rows or periods"})
        return base
    sel = covered
    y_all = _target(rows, mode)
    keep = sel & np.isfinite(y_all)
    B, D, y, gid, iid = rows["B"][keep], rows["D"][keep], y_all[keep], rows["gid"][keep], rows["i"][keep]
    y_raw, vol, cost = rows["y"][keep], rows["vol"][keep], rows["cost"][keep]
    ys = rows["y_scaled"][keep] if rows["y_scaled"] is not None else None
    # winsorise the target on the whole row set's percentiles is a leak; do it
    # per fold on training rows instead (below).
    folds = pit.walk_forward(ds["dates"], ds["dec"], horizon=h)
    if not folds:
        base.update({"verdict": V_DATA_HOLD, "why": "no walk-forward fold"})
        return base
    per = {"B": [], "D": [], "BD": [], "partial": [], "perm": []}
    per_fold = []
    preds = {"B": np.full(len(y), np.nan), "D": np.full(len(y), np.nan),
             "BD": np.full(len(y), np.nan)}
    fold_kind = np.array([""] * len(y), dtype=object)
    pred_scale = {"B": 1.0, "BD": 1.0, "D": 1.0}
    rng = np.random.default_rng(PERM_SEED)
    responded = False
    # a one- or two-instrument scope has ~1 row per decision slot; the fold
    # floors scale with the cross-section so a single-market scope is judged
    small_scope = len(np.unique(iid)) <= 2
    min_tr, min_te = (60, 8) if small_scope else (200, 20)
    for f in folds:
        tr = np.isin(gid, f["train"])
        te = np.isin(gid, f["test"])
        if tr.sum() < min_tr or te.sum() < min_te:
            continue
        ylo, yhi = np.nanpercentile(y[tr], [1, 99])
        ytr = np.clip(y[tr], ylo, yhi)
        scB, scD = _fit_scaler(B[tr]), _fit_scaler(D[tr])
        Btr, Bte = _apply_scaler(B[tr], scB), _apply_scaler(B[te], scB)
        Dtr, Dte = _apply_scaler(D[tr], scD), _apply_scaler(D[te], scD)
        # The penalty is a property of the BASELINE arm, chosen by blocked
        # inner CV inside this fold's training block; it is cached per
        # (dataset, baseline, fold) so every dimension sharing a baseline is
        # graded with the identical forced configuration.
        akey = (ds["scope"], mode, h, tuple(dims_b), f["test_year"], f["kind"])
        alpha = _ALPHA_CACHE.get(akey)
        if alpha is None:
            alpha = _select_alpha(B[tr], ytr, gid[tr], mode, ys[tr] if ys is not None else None)
            _ALPHA_CACHE[akey] = alpha
        bB = _ridge_fit(Btr, ytr, alpha)
        bBD = _ridge_fit(np.column_stack([Btr, Dtr]), ytr, alpha)
        bD = _ridge_fit(Dtr, ytr, alpha)
        pB = _ridge_predict(Bte, bB)
        pBD = _ridge_predict(np.column_stack([Bte, Dte]), bBD)
        pD = _ridge_predict(Dte, bD)
        if np.max(np.abs(pBD - pB)) > 1e-12:
            responded = True
        preds["B"][te], preds["BD"][te], preds["D"][te] = pB, pBD, pD
        fold_kind[te] = f["kind"]
        scale = {k: float(np.std(_ridge_predict(Btr, bB))) for k in ("B",)}
        scale["BD"] = float(np.std(_ridge_predict(np.column_stack([Btr, Dtr]), bBD)))
        scale["D"] = float(np.std(_ridge_predict(Dtr, bD)))
        pred_scale = scale
        if mode == "XS":
            sB = grouped_rank_ic(pB, y[te], gid[te])
            sBD = grouped_rank_ic(pBD, y[te], gid[te])
            sD = grouped_rank_ic(pD, y[te], gid[te])
        else:
            sB = grouped_timing_stat(pB, ys[te], gid[te], scale["B"])
            sBD = grouped_timing_stat(pBD, ys[te], gid[te], scale["BD"])
            sD = grouped_timing_stat(pD, ys[te], gid[te], scale["D"])
        # partial rank IC: residualise composite D and y on B (train-fitted)
        Bz_tr = np.column_stack([np.ones(tr.sum()), Btr])
        Bz_te = np.column_stack([np.ones(te.sum()), Bte])
        comp_tr, comp_te = Dtr.mean(axis=1), Dte.mean(axis=1)
        beta_c = np.linalg.lstsq(Bz_tr, comp_tr, rcond=None)[0]
        beta_y = np.linalg.lstsq(Bz_tr, ytr, rcond=None)[0]
        rc, ry = comp_te - Bz_te @ beta_c, y[te] - Bz_te @ beta_y
        sP = grouped_rank_ic(rc, ry, gid[te]) if mode == "XS" else \
            grouped_timing_stat(rc, ys[te], gid[te], float(np.std(comp_tr - Bz_tr @ beta_c)) or 1.0)
        # permutation: shuffle D within the test block
        perm = rng.permutation(te.sum())
        pPerm = _ridge_predict(np.column_stack([Bte, Dte[perm]]), bBD)
        sPerm = grouped_rank_ic(pPerm, y[te], gid[te]) if mode == "XS" else \
            grouped_timing_stat(pPerm, ys[te], gid[te], scale["BD"])
        # OOS R-squared (pooled in the test block, around zero)
        sst = float(np.sum(np.clip(y[te], ylo, yhi) ** 2)) or np.nan
        r2B = 1.0 - float(np.sum((np.clip(y[te], ylo, yhi) - pB) ** 2)) / sst
        r2BD = 1.0 - float(np.sum((np.clip(y[te], ylo, yhi) - pBD) ** 2)) / sst
        for k, s in (("B", sB), ("D", sD), ("BD", sBD), ("partial", sP), ("perm", sPerm)):
            per[k].append(s)
        per_fold.append({"kind": f["kind"], "test_year": f["test_year"], "alpha": alpha,
                         "oos_r2_B": r2B, "oos_r2_BD": r2BD, "oos_r2_increment": r2BD - r2B,
                         "rows_test": int(te.sum())})
    if not per_fold:
        base.update({"verdict": V_DATA_HOLD, "why": "no fold produced a score"})
        return base
    if not responded:
        base.update({"verdict": V_NO_RESPONSE,
                     "why": "the augmented arm's scores are identical to the baseline arm's"})
        return base
    S = {k: pd.concat(v).sort_index() for k, v in per.items()}
    common = S["B"].dropna().index.intersection(S["BD"].dropna().index)
    if len(common) < 2 * MIN_EFFECTIVE_PERIODS:
        base.update({"verdict": V_DATA_HOLD,
                     "why": "too few periods carry a scorable cross-section (%d)" % len(common)})
        return base
    delta = (S["BD"].loc[common] - S["B"].loc[common]).astype(float)
    slot_dates = np.array(ds["dates"])[np.array(ds["dec"])]
    kind_by_slot = {}
    for f in folds:
        for pos in f["test"]:
            kind_by_slot[int(pos)] = f["kind"]
    kinds = np.array([kind_by_slot.get(int(g), "") for g in common])
    sel_d = delta[kinds == "SELECTION"]
    lock_d = delta[kinds == "LOCKBOX"]
    st_all = nw_tstat(delta.values, lag)
    st_sel = nw_tstat(sel_d.values, lag)
    st_lock = nw_tstat(lock_d.values, lag)
    perm_drop = float((S["BD"].loc[common] - S["perm"].loc[common]).mean()) \
        if len(S["perm"]) else None
    partial = nw_tstat(S["partial"].dropna().values, lag)
    stand = nw_tstat(S["D"].dropna().values, lag)
    basel = nw_tstat(S["B"].dropna().values, lag)
    # stability
    years = np.array([int(str(slot_dates[int(g)])[:4]) for g in common])
    blocks = {}
    for y0 in range(int(years.min()) - int(years.min()) % 3, int(years.max()) + 1, 3):
        m = (years >= y0) & (years < y0 + 3)
        if m.sum() >= 6:
            blocks["%d-%d" % (y0, y0 + 2)] = float(delta.values[m].mean())
    share_blocks_pos = float(np.mean([v > 0 for v in blocks.values()])) if blocks else None
    regime = {}
    if ds.get("regime_vix") is not None:
        v = np.asarray(ds["regime_vix"])[np.array(ds["dec"])]
        win = max(4, 252 // max(1, cad))
        med = pd.Series(v).rolling(win, min_periods=min(20, win)).median().to_numpy()
        hi = v[np.array(list(common), dtype=int)] > med[np.array(list(common), dtype=int)]
        regime["iv_high"] = float(delta.values[hi].mean()) if hi.sum() >= 6 else None
        regime["iv_low"] = float(delta.values[~hi].mean()) if (~hi).sum() >= 6 else None
    if ds.get("regime_trend") is not None:
        tv = np.asarray(ds["regime_trend"])[np.array(ds["dec"])]
        up = tv[np.array(list(common), dtype=int)] > 0
        regime["trend_up"] = float(delta.values[up].mean()) if up.sum() >= 6 else None
        regime["trend_down"] = float(delta.values[~up].mean()) if (~up).sum() >= 6 else None
    inst_pos = None
    if mode == "TS":
        okp = np.isfinite(preds["B"]) & np.isfinite(preds["BD"])
        contrib = {}
        sB_ = np.tanh(preds["B"] / (pred_scale["B"] or 1.0)) * ys
        sBD_ = np.tanh(preds["BD"] / (pred_scale["BD"] or 1.0)) * ys
        for i_ in np.unique(iid[okp]):
            m = okp & (iid == i_)
            contrib[int(i_)] = float(np.mean(sBD_[m] - sB_[m]))
        inst_pos = float(np.mean([v > 0 for v in contrib.values()])) if contrib else None
    # economics
    okp = np.isfinite(preds["B"]) & np.isfinite(preds["BD"])
    kind_book = ("EQ_LONG_ONLY_TOPN" if ds.get("book") == "EQ_LONG_ONLY_TOPN"
                 else "XS_LONG_SHORT" if mode == "XS" else "TS_VOL_TARGET")
    bookB = _book_returns(preds["B"][okp], y_raw[okp], gid[okp], iid[okp], vol[okp], cost[okp],
                          mode, kind_book, pred_scale["B"], top_n)
    bookBD = _book_returns(preds["BD"][okp], y_raw[okp], gid[okp], iid[okp], vol[okp], cost[okp],
                           mode, kind_book, pred_scale["BD"], top_n)
    econB, econBD = _econ_summary(bookB, h, lag), _econ_summary(bookBD, h, lag)
    n_e = min(len(bookB["net"]), len(bookBD["net"]))
    ediff = bookBD["net"][:n_e] - bookB["net"][:n_e]
    st_e = nw_tstat(ediff, lag)
    ppy = PPY / h
    econ = {"baseline": econB, "augmented": econBD,
            "ann_net_increment": float(ediff.mean() * ppy) if n_e else None,
            "sharpe_increment": ((econBD.get("sharpe") or 0.0) - (econB.get("sharpe") or 0.0))
            if econB.get("sharpe") is not None and econBD.get("sharpe") is not None else None,
            "t_increment": st_e["t"], "p_increment_one_sided": st_e["p_one_sided"],
            "book": kind_book,
            "ann_net_increment_at_2x_cost": float((ediff - (bookBD["cost"][:n_e] - bookB["cost"][:n_e])).mean() * ppy) if n_e else None}
    # redundancy on the largest selection training block
    last_sel = [f for f in folds if f["kind"] == "SELECTION"]
    tr_rows = np.isin(gid, (last_sel[-1] if last_sel else folds[-1])["train"])
    red = _residual_share(D[tr_rows], B[tr_rows])
    eff = int(len(common) * min(1.0, cad / float(h)))
    out = dict(base)
    out.update({
        "n_periods": int(len(common)), "effective_periods": eff,
        "n_selection_periods": int(len(sel_d)), "n_lockbox_periods": int(len(lock_d)),
        "standalone": {"mean": stand["mean"], "t": stand["t"], "p": stand["p_one_sided"]},
        "baseline_score": {"mean": basel["mean"], "t": basel["t"]},
        "conditional": {"increment": st_all["mean"], "t": st_all["t"],
                        "p_one_sided": st_all["p_one_sided"], "se": st_all["se"],
                        "increment_selection": st_sel["mean"], "t_selection": st_sel["t"],
                        "increment_lockbox": st_lock["mean"], "t_lockbox": st_lock["t"],
                        "positive_fraction": float((delta.values > 0).mean()),
                        "minimum_detectable_increment": (2.0 * st_all["se"]) if st_all["se"] else None,
                        "lockbox_sign_agrees": (bool(np.sign(st_lock["mean"]) == np.sign(st_sel["mean"]))
                                                if st_lock["mean"] is not None and st_sel["mean"] is not None else None)},
        "secondary": {"partial_rank_ic": partial["mean"], "partial_t": partial["t"],
                      "oos_r2_increment_mean": float(np.mean([f["oos_r2_increment"] for f in per_fold])),
                      "permutation_drop": perm_drop},
        "redundancy": red,
        "stability": {"blocks": blocks, "share_blocks_positive": share_blocks_pos,
                      "regime": regime, "share_instruments_positive": inst_pos},
        "economics": econ,
        "folds": per_fold,
    })
    out["verdict"] = verdict(out)
    if keep_predictions:
        # R64: the identical OOS scores the books above were priced from, with
        # the row facts they align to. Not part of the persisted cell.
        out["_predictions"] = {
            "pred_B": preds["B"], "pred_BD": preds["BD"], "pred_D": preds["D"],
            "gid": gid, "iid": iid, "y_raw": y_raw, "vol": vol, "cost": cost,
            "y_scaled": ys, "fold_kind": fold_kind, "pred_scale": dict(pred_scale),
            "slot_dates": slot_dates, "inst": list(ds["inst"]),
        }
    return out


def verdict(cell: dict, *, fdr_pass: bool | None = None) -> str:
    if cell.get("verdict") in (V_DATA_HOLD, V_NO_RESPONSE) and "conditional" not in cell:
        return cell["verdict"]
    if cell.get("effective_periods", 0) < MIN_EFFECTIVE_PERIODS:
        return V_DATA_HOLD
    red = cell.get("redundancy") or {}
    if red.get("redundancy") == "REDUNDANT":
        return V_REDUNDANT
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or t < CONDITIONAL_T_FLOOR or (c.get("increment") or 0.0) <= 0:
        return V_NO_VALUE
    e = cell.get("economics") or {}
    inc, sh = e.get("ann_net_increment"), e.get("sharpe_increment")
    aug = e.get("augmented") or {}
    # a book that lost (almost) everything or sits in one name measures its
    # own construction, not the information: no economic claim can rest on it.
    # The single-name rule is for CROSS-SECTIONAL books; a TS position is
    # vol-targeted and legitimately reaches 3x notional on a low-vol market.
    if aug.get("max_dd") is not None and aug["max_dd"] < DEGENERATE_DD:
        return V_NOT_ECON
    if e.get("book") == "XS_LONG_SHORT" and aug.get("max_weight") is not None \
            and aug["max_weight"] > 0.5 + 1e-9:
        return V_NOT_ECON
    if not ((inc is not None and inc >= MATERIALITY_ANN_NET) or (sh is not None and sh >= 0.15)):
        return V_NOT_ECON
    s = cell.get("stability") or {}
    if (s.get("share_blocks_positive") or 0.0) < 0.60 or c.get("lockbox_sign_agrees") is False:
        return V_UNSTABLE
    if fdr_pass is False:
        return V_NOT_FDR
    if fdr_pass is None:
        return V_NOT_FDR
    return V_CANDIDATE
