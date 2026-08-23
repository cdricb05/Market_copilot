"""alpha_agent.r39.representation_factory - the REPRESENTATION_REGISTRY owner.

Release 39 must not depend only on hand-engineered factors. This module
materialises parallel representation families on the universal state, every
one computed from trailing windows only (a feature at decision date d reads
observations dated <= d):

* RAW / CLASSICAL       - the base state columns (returns, vol, carry,
                          seasonality, positioning, macro overlay).
* AUTO_TRANSFORM        - a seeded random grammar over base columns (lags,
                          deltas, rolling z-scores, ratios, interactions);
                          the GENERATOR names the features, not a human.
* SPECTRAL              - trailing autocorrelation and variance-ratio
                          structure of each market's own monthly returns.
* LATENT_PCA            - walk-forward principal factors of the cross-market
                          return matrix: loadings and residual momentum.
* GRAPH_LEAD_LAG        - a trailing lead-lag graph, re-estimated annually,
                          sparsified to each market's strongest leaders;
                          the feature is the leaders' lagged-return signal.
* SYMBOLIC              - seeded random expression trees (depth <= 3) over
                          base columns; lineage keeps every tree.
* MARKET_STRUCTURE      - trend geometry from the daily layer: moving-average
                          distances, breakout, volatility contraction.
* FIBONACCI / PLACEBO   - retracement-proximity features from REAL-TIME
                          CONFIRMED pivots (a pivot exists only 10 sessions
                          after its extremum), computed for the Fibonacci
                          levels AND for placebo levels, so the family can
                          only ever prove PULLBACK STRUCTURE unless the
                          named levels beat their placebos.

Deferred families (SELF_SUPERVISED, VISUAL_CHART) are declared in the model
registry's compute classification, not silently dropped.
"""
from __future__ import annotations

import hashlib
import math

import numpy as np
import pandas as pd

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.representation_factory"
SCHEMA = "r39_representation_registry/1"
ARTIFACT_NAME = "representation_registry.json"

BASE_FUT = ("ret_1m", "ret_3m", "ret_6m", "ret_12m", "mom_12_1", "vol_63",
            "carry_slope_ann", "seasonality_next_month", "high_252_prox",
            "volume_z_252", "oi_z_252", "cal_spread_ret_21",
            "cot_commercial_z")
MACRO_COLS = ("credit_baa10y", "curve_10_2", "DTB3", "T10YIE", "DFII10",
              "vix", "vix_term", "btc_ret_21", "cpi_vintage_yoy")
CLASSICAL_FUT = ("ret_3m", "ret_12m", "mom_12_1", "vol_63",
                 "carry_slope_ann", "seasonality_next_month",
                 "high_252_prox", "cot_commercial_z")
EQ_FEATURES_BASE = ("mom_12_1", "mom_6_1", "mom_3_1", "rev_1m", "trend_200",
                    "high_52w_prox", "vol_63", "vol_252", "downside_vol_63",
                    "max_drawdown_252", "log_adv_20", "adv_trend_20_252",
                    "beta_252", "ivol_63")


# --------------------------------------------------------------------------- #
# AUTO_TRANSFORM - the seeded grammar
# --------------------------------------------------------------------------- #
_UNARY = ("id", "lag1", "delta1", "delta3", "rollz12", "tanh", "sign", "abs")
_BINARY = ("sub", "mul", "ratio", "min", "max")


def _apply_unary(g: pd.core.groupby.SeriesGroupBy, s: pd.Series,
                 op: str) -> pd.Series:
    if op == "id":
        return s
    if op == "lag1":
        return g.shift(1)
    if op == "delta1":
        return s - g.shift(1)
    if op == "delta3":
        return s - g.shift(3)
    if op == "rollz12":
        m = g.transform(lambda x: x.rolling(12, min_periods=8).mean())
        sd = g.transform(lambda x: x.rolling(12, min_periods=8).std())
        return (s - m) / sd.replace(0.0, np.nan)
    if op == "tanh":
        return np.tanh(s / (s.abs().median() + 1e-9))
    if op == "sign":
        return np.sign(s)
    if op == "abs":
        return s.abs()
    raise ValueError(op)


def _apply_binary(a: pd.Series, b: pd.Series, op: str) -> pd.Series:
    if op == "sub":
        return a - b
    if op == "mul":
        return a * b
    if op == "ratio":
        return a / (b.abs() + 1e-9)
    if op == "min":
        return np.minimum(a, b)
    if op == "max":
        return np.maximum(a, b)
    raise ValueError(op)


def generate_auto_transforms(panel: pd.DataFrame, *, base_cols: tuple,
                             k: int, seed: int,
                             id_col: str = "market_id") -> tuple:
    """Sample ``k`` machine-generated transforms; returns (panel, names,
    lineage)."""
    rng = np.random.default_rng(seed)
    cols = [c for c in base_cols if c in panel.columns]
    panel = panel.sort_values([id_col, "decision_date"])
    names, lineage = [], []
    made = set()
    tries = 0
    while len(names) < k and tries < k * 8:
        tries += 1
        if rng.random() < 0.45:  # unary
            c = cols[int(rng.integers(len(cols)))]
            u = _UNARY[int(rng.integers(1, len(_UNARY)))]
            spec = ("U", u, c)
        else:
            c1 = cols[int(rng.integers(len(cols)))]
            c2 = cols[int(rng.integers(len(cols)))]
            if c1 == c2:
                continue
            u1 = _UNARY[int(rng.integers(len(_UNARY)))]
            u2 = _UNARY[int(rng.integers(len(_UNARY)))]
            b = _BINARY[int(rng.integers(len(_BINARY)))]
            spec = ("B", b, (u1, c1), (u2, c2))
        if spec in made:
            continue
        made.add(spec)
        name = "auto_" + hashlib.sha256(repr(spec).encode()).hexdigest()[:10]
        try:
            if spec[0] == "U":
                g = panel.groupby(id_col)[spec[2]]
                panel[name] = _apply_unary(g, panel[spec[2]], spec[1])
            else:
                g1 = panel.groupby(id_col)[spec[2][1]]
                g2 = panel.groupby(id_col)[spec[3][1]]
                a = _apply_unary(g1, panel[spec[2][1]], spec[2][0])
                b_ = _apply_unary(g2, panel[spec[3][1]], spec[3][0])
                panel[name] = _apply_binary(a, b_, spec[1])
        except Exception:
            continue
        names.append(name)
        lineage.append({"name": name, "spec": repr(spec),
                        "generation_method": "AUTO_TRANSFORM_GRAMMAR",
                        "seed": seed})
    return panel, names, lineage


# --------------------------------------------------------------------------- #
# SYMBOLIC - seeded expression trees
# --------------------------------------------------------------------------- #
_SYM_UNARY = ("tanh", "sign", "abs", "neg", "rankxs")
_SYM_BINARY = ("add", "sub", "mul", "ratio", "min", "max")


def _sym_tree(rng, cols, depth):
    if depth <= 0 or rng.random() < 0.3:
        return ("col", cols[int(rng.integers(len(cols)))])
    if rng.random() < 0.4:
        return ("u", _SYM_UNARY[int(rng.integers(len(_SYM_UNARY)))],
                _sym_tree(rng, cols, depth - 1))
    return ("b", _SYM_BINARY[int(rng.integers(len(_SYM_BINARY)))],
            _sym_tree(rng, cols, depth - 1),
            _sym_tree(rng, cols, depth - 1))


def _sym_eval(tree, panel: pd.DataFrame) -> pd.Series:
    kind = tree[0]
    if kind == "col":
        return panel[tree[1]]
    if kind == "u":
        x = _sym_eval(tree[2], panel)
        op = tree[1]
        if op == "tanh":
            return np.tanh(x / (x.abs().median() + 1e-9))
        if op == "sign":
            return np.sign(x)
        if op == "abs":
            return x.abs()
        if op == "neg":
            return -x
        if op == "rankxs":
            return x.groupby(panel["decision_date"]).rank(pct=True) - 0.5
    x = _sym_eval(tree[2], panel)
    y = _sym_eval(tree[3], panel)
    op = tree[1]
    if op == "add":
        return x + y
    if op == "sub":
        return x - y
    if op == "mul":
        return x * y
    if op == "ratio":
        return x / (y.abs() + 1e-9)
    if op == "min":
        return np.minimum(x, y)
    return np.maximum(x, y)


def generate_symbolic(panel: pd.DataFrame, *, base_cols: tuple, k: int,
                      seed: int) -> tuple:
    rng = np.random.default_rng(seed)
    cols = [c for c in base_cols if c in panel.columns]
    names, lineage = [], []
    for _ in range(k * 4):
        if len(names) >= k:
            break
        tree = _sym_tree(rng, cols, 3)
        name = "sym_" + hashlib.sha256(repr(tree).encode()).hexdigest()[:10]
        if name in panel.columns:
            continue
        try:
            v = _sym_eval(tree, panel)
        except Exception:
            continue
        arr = pd.to_numeric(v, errors="coerce")
        if np.isfinite(arr.to_numpy(dtype=float)).mean() < 0.3:
            continue
        panel[name] = arr
        names.append(name)
        lineage.append({"name": name, "spec": repr(tree),
                        "generation_method": "SYMBOLIC_TREE_SEARCH",
                        "seed": seed})
    return panel, names, lineage


# --------------------------------------------------------------------------- #
# SPECTRAL
# --------------------------------------------------------------------------- #
def add_spectral(panel: pd.DataFrame, *, id_col: str = "market_id") -> tuple:
    panel = panel.sort_values([id_col, "decision_date"])
    g = panel.groupby(id_col)["ret_1m"]

    def _ac(lag):
        return g.transform(
            lambda x: x.rolling(24, min_periods=18).apply(
                lambda w: pd.Series(w).autocorr(lag), raw=False))

    panel["spec_ac1"] = _ac(1)
    panel["spec_ac2"] = _ac(2)

    def _vr(x):
        # variance ratio of 3-month sums to 3x monthly variance
        s = pd.Series(x)
        if s.notna().sum() < 24:
            return np.nan
        v1 = s.var()
        v3 = s.rolling(3).sum().var()
        if not v1 or not math.isfinite(v1) or v1 <= 0:
            return np.nan
        return float(v3 / (3.0 * v1))

    panel["spec_vr3"] = g.transform(
        lambda x: x.rolling(36, min_periods=24).apply(_vr, raw=True))
    names = ["spec_ac1", "spec_ac2", "spec_vr3"]
    return panel, names, [{"name": n, "generation_method": "SPECTRAL"}
                          for n in names]


# --------------------------------------------------------------------------- #
# LATENT_PCA + GRAPH_LEAD_LAG (cross-market, walk-forward)
# --------------------------------------------------------------------------- #
def add_latent_and_graph(panel: pd.DataFrame, *, window: int = 60,
                         n_factors: int = 3, top_k: int = 5) -> tuple:
    wide = panel.pivot_table(index="decision_date", columns="market_id",
                             values="ret_1m", aggfunc="last").sort_index()
    dates = wide.index
    X = wide.to_numpy(dtype=float)
    n_dates, n_mkts = X.shape
    load = np.full((n_dates, n_mkts, n_factors), np.nan)
    resid_mom = np.full((n_dates, n_mkts), np.nan)
    leadlag = np.full((n_dates, n_mkts), np.nan)
    graph_edges = None
    graph_year = None
    for t in range(window, n_dates):
        W = X[t - window: t]  # strictly BEFORE t's forward period
        ok = np.isfinite(W).sum(axis=0) >= int(window * 0.8)
        if ok.sum() < 12:
            continue
        Wo = W[:, ok]
        mu = np.nanmean(Wo, axis=0)
        sd = np.nanstd(Wo, axis=0, ddof=1)
        sd[sd <= 0] = np.nan
        Z = (Wo - mu) / sd
        Z = np.where(np.isfinite(Z), Z, 0.0)
        try:
            U, S, Vt = np.linalg.svd(Z, full_matrices=False)
        except np.linalg.LinAlgError:
            continue
        V = Vt[:n_factors].T  # (n_ok, n_factors) loadings
        idx_ok = np.flatnonzero(ok)
        load[t, idx_ok, :] = V[:, :n_factors]
        # residual momentum: trailing 12m sum of residual returns
        F = Z @ V  # factor scores over window
        recon = F @ V.T
        resid = Z - recon
        rm = resid[-12:].sum(axis=0)
        resid_mom[t, idx_ok] = rm
        # lead-lag graph, re-estimated once per calendar year
        yr = dates[t].year
        if graph_year != yr:
            graph_year = yr
            A = np.full((n_mkts, n_mkts), np.nan)
            Wl = X[t - window: t]
            for j in range(n_mkts):
                xj = Wl[:-1, j]
                for i in range(n_mkts):
                    if i == j:
                        continue
                    yi = Wl[1:, i]
                    m = np.isfinite(xj) & np.isfinite(yi)
                    if m.sum() < 36:
                        continue
                    sx = xj[m].std(ddof=1)
                    sy = yi[m].std(ddof=1)
                    if sx <= 0 or sy <= 0:
                        continue
                    A[i, j] = float(np.corrcoef(xj[m], yi[m])[0, 1])
            graph_edges = []
            for i in range(n_mkts):
                row = A[i]
                order = np.argsort(-np.abs(np.where(np.isfinite(row), row,
                                                    0.0)))[:top_k]
                graph_edges.append([(int(j), float(row[j])) for j in order
                                    if np.isfinite(row[j])])
        if graph_edges is not None:
            last = X[t - 1]
            for i in range(n_mkts):
                s = 0.0
                wsum = 0.0
                for j, cij in graph_edges[i]:
                    if np.isfinite(last[j]):
                        s += cij * last[j]
                        wsum += abs(cij)
                leadlag[t, i] = s / wsum if wsum > 0 else np.nan

    cols = list(wide.columns)
    frames = {"latent_resid_mom": pd.DataFrame(resid_mom, index=dates,
                                               columns=cols),
              "graph_leadlag": pd.DataFrame(leadlag, index=dates,
                                            columns=cols)}
    for f in range(n_factors):
        frames["latent_load_pc%d" % (f + 1)] = pd.DataFrame(
            load[:, :, f], index=dates, columns=cols)
    names = []
    for name, frame in frames.items():
        longf = frame.stack().rename(name).reset_index()
        longf.columns = ["decision_date", "market_id", name]
        panel = panel.merge(longf, on=["decision_date", "market_id"],
                            how="left")
        names.append(name)
    lineage = [{"name": n,
                "generation_method": "LATENT_PCA" if n.startswith("latent")
                else "GRAPH_LEAD_LAG",
                "window_months": window,
                "graph_reestimated": "annually"} for n in names]
    return panel, names, lineage


# --------------------------------------------------------------------------- #
# MARKET_STRUCTURE + FIBONACCI/PLACEBO (from the daily layer)
# --------------------------------------------------------------------------- #
def _pivots(close: np.ndarray, half: int = 10):
    """(index, kind) of confirmed pivots; a pivot at i is usable only from
    i + half (its confirmation date)."""
    n = close.size
    out = []
    for i in range(half, n - half):
        w = close[i - half: i + half + 1]
        if not np.all(np.isfinite(w)):
            continue
        c = close[i]
        if c >= w.max():
            out.append((i, "H"))
        elif c <= w.min():
            out.append((i, "L"))
    return out


def add_structure_and_fib(panel: pd.DataFrame, layer: dict) -> tuple:
    """Market-structure and retracement features at each decision row."""
    feat_names = (["ms_ma50_dist", "ms_ma200_dist", "ms_breakout55",
                   "ms_vol_contraction", "ms_range_pos", "fib_pullback_r",
                   "fib_swing_age"]
                  + ["fib_prox_%d" % int(l * 1000) for l in C.FIB_LEVELS]
                  + ["plc_prox_%d" % int(l * 1000) for l in C.PLACEBO_LEVELS])
    store = {name: {} for name in feat_names}
    for mkt, df in layer.items():
        d = df.reset_index(drop=True)
        close = d["close"].to_numpy(dtype=float)
        rets = d["ret"].to_numpy(dtype=float)
        dates = d["Date"]
        n = close.size
        piv = _pivots(close)
        piv_idx = np.array([p[0] for p in piv], dtype=int) if piv else \
            np.zeros(0, dtype=int)
        # rolling structure
        s = pd.Series(close)
        ma50 = s.rolling(50, min_periods=40).mean().to_numpy()
        ma200 = s.rolling(200, min_periods=150).mean().to_numpy()
        r = pd.Series(rets)
        vol20 = r.rolling(20, min_periods=15).std().to_numpy()
        vol100 = r.rolling(100, min_periods=70).std().to_numpy()
        hi55 = s.rolling(55, min_periods=40).max().to_numpy()
        hi250 = s.rolling(250, min_periods=150).max().to_numpy()
        lo250 = s.rolling(250, min_periods=150).min().to_numpy()
        period = dates.dt.to_period("M")
        month_end = period.ne(period.shift(-1)).to_numpy()
        for pos in np.flatnonzero(month_end):
            if pos < 260:
                continue
            dt = dates.iloc[pos]
            key = (dt, mkt)
            v63 = vol20[pos]
            denom = (v63 * math.sqrt(50) * close[pos]) if v63 and \
                np.isfinite(v63) else np.nan
            store["ms_ma50_dist"][key] = \
                (close[pos] - ma50[pos]) / denom if np.isfinite(denom) and \
                denom > 0 and np.isfinite(ma50[pos]) else np.nan
            denom2 = (v63 * math.sqrt(200) * close[pos]) if v63 and \
                np.isfinite(v63) else np.nan
            store["ms_ma200_dist"][key] = \
                (close[pos] - ma200[pos]) / denom2 if np.isfinite(denom2) \
                and denom2 > 0 and np.isfinite(ma200[pos]) else np.nan
            store["ms_breakout55"][key] = float(
                np.isfinite(hi55[pos]) and close[pos] >= hi55[pos] - 1e-12)
            store["ms_vol_contraction"][key] = \
                (vol20[pos] / vol100[pos]) if vol100[pos] and \
                np.isfinite(vol100[pos]) and vol100[pos] > 0 else np.nan
            rng_ = hi250[pos] - lo250[pos]
            store["ms_range_pos"][key] = \
                (close[pos] - lo250[pos]) / rng_ if np.isfinite(rng_) and \
                rng_ > 0 else np.nan
            # confirmed pivots only: extremum index + 10 <= pos
            usable = piv_idx[piv_idx + 10 <= pos] if piv_idx.size else \
                np.zeros(0, dtype=int)
            r_val, age = np.nan, np.nan
            if usable.size >= 2:
                kinds = {i: k for i, k in piv}
                lastH = next((i for i in usable[::-1]
                              if kinds[i] == "H"), None)
                lastL = next((i for i in usable[::-1]
                              if kinds[i] == "L"), None)
                if lastH is not None and lastL is not None:
                    H, L = close[lastH], close[lastL]
                    if H > L:
                        if lastH > lastL:
                            r_val = (H - close[pos]) / (H - L)
                        else:
                            r_val = (close[pos] - L) / (H - L)
                        age = pos - max(lastH, lastL)
            if np.isfinite(r_val):
                r_val = float(np.clip(r_val, -0.5, 1.5))
            store["fib_pullback_r"][key] = r_val
            store["fib_swing_age"][key] = age
            for lev in C.FIB_LEVELS:
                store["fib_prox_%d" % int(lev * 1000)][key] = \
                    math.exp(-((r_val - lev) / 0.04) ** 2) \
                    if np.isfinite(r_val) else np.nan
            for lev in C.PLACEBO_LEVELS:
                store["plc_prox_%d" % int(lev * 1000)][key] = \
                    math.exp(-((r_val - lev) / 0.04) ** 2) \
                    if np.isfinite(r_val) else np.nan
    for name in feat_names:
        m = store[name]
        panel[name] = [
            m.get((dt, mk), np.nan)
            for dt, mk in zip(panel["decision_date"], panel["market_id"])]
    lineage = [{"name": n,
                "generation_method": "FIB_CONFIRMED_PIVOTS"
                if n.startswith(("fib", "plc")) else "MARKET_STRUCTURE",
                "pivot_confirmation_sessions": 10}
               for n in feat_names]
    return panel, feat_names, lineage


FIB_FEATURES = tuple("fib_prox_%d" % int(l * 1000) for l in C.FIB_LEVELS) + \
    ("fib_pullback_r", "fib_swing_age", "ms_ma200_dist")
PLACEBO_FEATURES = tuple("plc_prox_%d" % int(l * 1000)
                         for l in C.PLACEBO_LEVELS) + \
    ("fib_pullback_r", "fib_swing_age", "ms_ma200_dist")
MSTRUCT_FEATURES = ("ms_ma50_dist", "ms_ma200_dist", "ms_breakout55",
                    "ms_vol_contraction", "ms_range_pos")
SPECTRAL_FEATURES = ("spec_ac1", "spec_ac2", "spec_vr3")
LATENT_FEATURES = ("latent_load_pc1", "latent_load_pc2", "latent_load_pc3",
                   "latent_resid_mom")
GRAPH_FEATURES = ("graph_leadlag",)


def build_registry(bundles: dict, lineage: list,
                   campaign_id: str = C.CAMPAIGN_ID) -> dict:
    body = r39.artifact_body(SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "families_implemented": list(C.REPRESENTATION_FAMILIES),
        "families_deferred": list(C.DEFERRED_REPRESENTATION_FAMILIES),
        "bundles": {k: list(v) for k, v in bundles.items()},
        "generated_feature_lineage": lineage,
        "n_generated_features": len(lineage),
        "fib_levels": list(C.FIB_LEVELS),
        "placebo_levels": list(C.PLACEBO_LEVELS),
        "fib_requires_confirmed_pivots": C.FIB_REQUIRES_CONFIRMED_PIVOTS,
        "trailing_windows_only": True,
    })
    body["representation_registry_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
