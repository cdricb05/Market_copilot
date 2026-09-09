"""alpha_agent.r63.experiments - baseline versus baseline + information.

Assembles one DATASET per (scope, mode, horizon) from the owned substrates and
runs the sensitivity engine over every information dimension the scope can
observe, under identical universe, dates, costs, construction, splits,
eligibility and point-in-time treatment. A BASELINE dimension is measured by
ABLATION (baseline minus the dimension versus the full baseline), which is the
marginal value of information the estate already uses; a NEW dimension is
measured by AUGMENTATION (baseline versus baseline + dimension).

Modes: XS (cross-sectional ranking inside the scope) for instrument-level
dimensions; TS (timing of each instrument, pooled) for every dimension,
including market-level conditioners that are constant across a cross-section
and therefore can only ever carry TIMING information.

Writes:
    information_sensitivity_matrix.json   every cell, every statistic
    orthogonality_matrix.json             redundancy against the baseline and
                                          pairwise dimension correlations
    r63_experiment_results.json           the campaign-level summary with BH
"""
from __future__ import annotations

import time
from pathlib import Path

import numpy as np
import pandas as pd

from . import (AC_COMMODITY, AC_CREDIT, AC_CROSS_ASSET, AC_EQUITY_INDEX, AC_FX,
               AC_RATES, AC_US_EQUITY, AC_VOLATILITY, BH_Q, CREDIT_PROXY_COST_BPS,
               EQUITY_DISCOVERY_START, EQ_COST_RATE_PER_SIDE,
               FUTURES_DISCOVERY_START, HORIZONS, LOCKBOX_START, now_iso,
               read_artifact, write_artifact)
from . import features as FE
from . import ontology as ONT
from . import panels as P
from . import pit
from . import sensitivity as S

CALCULATION_OWNER = "alpha_agent.r63.experiments"
MATRIX_ARTIFACT = "information_sensitivity_matrix.json"
ORTHO_ARTIFACT = "orthogonality_matrix.json"
RESULTS_ARTIFACT = "r63_experiment_results.json"
PPY = 252.0

FUTURES_SCOPES = (AC_COMMODITY, AC_FX, AC_RATES, AC_EQUITY_INDEX, AC_VOLATILITY,
                  AC_CROSS_ASSET)
XS_SCOPES = (AC_COMMODITY, AC_FX, AC_RATES, AC_EQUITY_INDEX, AC_CROSS_ASSET)

#: Instrument-level futures dimensions (vary across the cross-section).
FUT_INSTRUMENT_DIMS = ("CARRY", "POSITIONING_COMMITMENTS", "INVENTORY",
                       "CALENDAR_SEASONALITY", "VOLATILITY_EXPECTATIONS_IV")
#: Market-level conditioners (TS only).
FUT_MARKET_DIMS = ("CREDIT_CONDITIONS", "TERM_STRUCTURE", "CURVE_SHAPE",
                   "RATES_EXPECTATIONS", "POLICY_EXPECTATIONS",
                   "INFLATION_EXPECTATIONS", "FUNDING_LIQUIDITY_CONDITIONS",
                   "MACRO_LEVELS", "MACRO_CHANGE", "MACRO_SURPRISES",
                   "RISK_APPETITE", "DISPERSION", "CROSS_ASSET_TRANSMISSION")
EQ_NEW_DIMS = ("FUNDAMENTAL_CHANGE", "CORPORATE_DISCLOSURES", "INSIDER_BEHAVIOUR",
               "DISCLOSURE_INTENSITY_LANGUAGE", "EVENT_INFORMATION")

_DS_CACHE: dict = {}


def cadence_for(horizon: int) -> int:
    return int(min(int(horizon), 21))


# --------------------------------------------------------------------------- #
# Dataset assembly
# --------------------------------------------------------------------------- #
def _scope_rows(F: dict, scope: str) -> list:
    if scope == AC_CROSS_ASSET:
        return list(range(len(F["markets"])))
    return [i for i, m in enumerate(F["markets"]) if F["meta"][m]["asset_class"] == scope]


def _futures_features(scope: str) -> dict:
    key = ("fut_feats", scope)
    if key in _DS_CACHE:
        return _DS_CACHE[key]
    F = P.load_futures()
    rows = _scope_rows(F, scope)
    feats = FE.build_futures_features(F, rows)
    feats["_rows"] = rows
    _DS_CACHE[key] = feats
    return feats


def assemble_futures(scope: str, mode: str, horizon: int) -> dict:
    F = P.load_futures()
    feats = _futures_features(scope)
    rows = feats["_rows"]
    dates = F["dates"]
    ret = F["ret1"][rows]
    h = int(horizon)
    cad = cadence_for(h)
    y = pit.forward_compound(ret, h)
    vol63 = feats["_vol_63"]
    hvol = vol63 * np.sqrt(h / PPY)
    y_scaled = y / np.where(hvol > 0, hvol, np.nan)
    hist = pd.DataFrame(np.isfinite(ret).T).rolling(260, min_periods=1).sum().to_numpy().T
    elig = np.isfinite(ret) & np.isfinite(vol63) & (vol63 > 0) & (hist >= 234)
    dec = pit.decision_indices(dates, FUTURES_DISCOVERY_START, cad, h)
    cost = np.array([F["meta"][F["markets"][i]]["cost_bps_per_side"] / 1e4 for i in rows])
    trend = feats["TREND"][..., 0]
    regime_trend = np.nanmean(trend, axis=0)
    blocks = {k: v for k, v in feats.items()
              if isinstance(v, np.ndarray) and v.ndim == 3 and not k.startswith("_")}
    return {"scope": scope, "mode": mode, "horizon": h, "cadence": cad,
            "dates": dates, "dec": dec, "inst": [F["markets"][i] for i in rows],
            "y": y, "y_scaled": y_scaled, "elig": elig, "vol": vol63, "cost": cost,
            "blocks": blocks, "market_level": set(feats["_market_level"]),
            "regime_vix": feats["_regime_vix"], "regime_trend": regime_trend,
            "book": "XS_LONG_SHORT" if mode == "XS" else "TS_VOL_TARGET"}


CREDIT_PROXY_SYMBOLS = ("HYG", "LQD")


def assemble_credit_proxy(horizon: int) -> dict:
    """CREDIT_PROXY: the owned Norgate total-return series of the two liquid
    credit ETFs (HYG from 2007, LQD from 2002), excess over the 3-month bill,
    charged 5bp/side. The ICE BofA total-return index in the FRED panel is
    licence-capped to three years and is used only as a fallback."""
    fr = P.load_fred_daily()
    F = P.load_futures()
    dates = F["dates"]
    bill = pit.as_of(fr["CMT_3M"].dropna(), dates, lag_sessions=0) / 100.0 / PPY
    series = P.load_norgate_total_return(CREDIT_PROXY_SYMBOLS)
    names, rows = [], []
    for sym in CREDIT_PROXY_SYMBOLS:
        s = series.get(sym)
        if s is None or len(s) < 500:
            continue
        lvl = pit.as_of(s, dates, lag_sessions=0)
        r = np.full(len(dates), np.nan)
        r[1:] = lvl[1:] / lvl[:-1] - 1.0
        r = np.where(np.isfinite(lvl) & np.isfinite(np.concatenate([[np.nan], lvl[:-1]])), r, np.nan)
        # a session where the level did not print is a gap, never a zero
        held = np.concatenate([[False], np.isfinite(s.reindex(pd.to_datetime(dates)).to_numpy())[1:]])
        r = np.where(held, r, np.nan)
        rows.append(r - np.where(np.isfinite(bill), bill, 0.0))
        names.append(sym)
    if not rows:
        if "TRI_HY" not in fr.columns:
            raise FileNotFoundError("no credit proxy series available")
        tri = pit.as_of(fr["TRI_HY"].dropna(), dates, lag_sessions=0)
        r = np.full(len(dates), np.nan)
        r[1:] = tri[1:] / tri[:-1] - 1.0
        rows, names = [r - np.where(np.isfinite(bill), bill, 0.0)], ["HY_TRI_EXCESS"]
    ret = np.vstack(rows)
    h = int(horizon)
    cad = cadence_for(h)
    price = FE.futures_price_blocks({"ret1": ret, "c1": np.ones_like(ret),
                                     "v1": np.full_like(ret, np.nan),
                                     "oi1": np.full_like(ret, np.nan)})
    es = F["ret1"][F["markets"].index("ES")] if "ES" in F["markets"] else None
    cond = FE.market_conditioner_blocks(dates, equity_ret=es, scope_returns=None)
    # an index has no volume or open interest: those baseline blocks would be
    # all-NaN and would empty every row, so the proxy's baseline omits them
    blocks = {k: v for k, v in price.items() if isinstance(v, np.ndarray) and v.ndim == 3
              and k not in ("LIQUIDITY", "VOLUME_PARTICIPATION")}
    n_i = ret.shape[0]
    for dim in FUT_MARKET_DIMS:
        if dim == "CROSS_ASSET_TRANSMISSION":
            blocks[dim] = np.repeat(FE.cross_asset_block(F)[:1], n_i, axis=0)
            continue
        blk = cond[dim]
        blk = blk[None, :, :] if blk.ndim == 2 else blk
        blocks[dim] = np.repeat(blk, n_i, axis=0)
    blocks["VOLATILITY_EXPECTATIONS_IV"] = np.repeat(cond["VOLATILITY_EXPECTATIONS_IV"][None, :, :], n_i, axis=0)
    y = pit.forward_compound(ret, h)
    vol63 = price["_vol_63"]
    hvol = vol63 * np.sqrt(h / PPY)
    hist = pd.DataFrame(np.isfinite(ret).T).rolling(260, min_periods=1).sum().to_numpy().T
    return {"scope": AC_CREDIT, "mode": "TS", "horizon": h, "cadence": cad,
            "dates": dates, "dec": pit.decision_indices(dates, FUTURES_DISCOVERY_START, cad, h),
            "inst": names, "y": y, "y_scaled": y / np.where(hvol > 0, hvol, np.nan),
            "elig": np.isfinite(ret) & np.isfinite(vol63) & (vol63 > 0) & (hist >= 234), "vol": vol63,
            "cost": np.full(n_i, CREDIT_PROXY_COST_BPS / 1e4), "blocks": blocks,
            "market_level": set(FUT_MARKET_DIMS) | {"VOLATILITY_EXPECTATIONS_IV"},
            "regime_vix": cond["_vix"], "regime_trend": None, "book": "TS_VOL_TARGET"}


def _equity_eligibility(E: dict) -> np.ndarray:
    pr = E["price"]
    tr, un, vol, mem = pr["tr"], pr["un"], pr["vol"], pr["mem"]
    dv = pd.DataFrame((un * vol).T)
    med = dv.rolling(63, min_periods=30).median().to_numpy().T
    hist = pd.DataFrame(np.isfinite(tr).T).rolling(260, min_periods=1).sum().to_numpy().T
    ok = (mem > 0) & np.isfinite(un) & (un >= 5.0) & np.isfinite(med) & (med >= 1.0e7)
    ok &= (hist >= 234) & np.isfinite(tr)
    return ok


def assemble_equity(horizon: int) -> dict:
    key = ("eq_feats",)
    if key not in _DS_CACHE:
        E = P.load_equity()
        feats = FE.build_equity_features(E)
        elig = _equity_eligibility(E)
        _DS_CACHE[key] = (E, feats, elig)
    E, feats, elig = _DS_CACHE[key]
    dates = E["dates"]
    ret = feats["_ret"]
    h = int(horizon)
    cad = cadence_for(h)
    y = pit.forward_compound(ret, h)
    vol63 = feats["_vol_63"]
    dec = pit.decision_indices(dates, EQUITY_DISCOVERY_START, cad, h)
    blocks = {k: v for k, v in feats.items()
              if isinstance(v, np.ndarray) and v.ndim == 3 and not k.startswith("_")}
    n = len(E["symbols"])
    # regime: SPY total return trend and VIX
    spy = E["price"]["spy_tr"]
    spy_r = np.full(len(dates), np.nan)
    spy_r[1:] = spy[1:] / spy[:-1] - 1.0
    cond = FE.market_conditioner_blocks(dates, equity_ret=spy_r, scope_returns=None)
    return {"scope": AC_US_EQUITY, "mode": "XS", "horizon": h, "cadence": cad,
            "dates": dates, "dec": dec, "inst": list(E["symbols"]),
            "y": y, "y_scaled": None, "elig": elig, "vol": vol63,
            "cost": np.full(n, EQ_COST_RATE_PER_SIDE), "blocks": blocks,
            "market_level": set(), "regime_vix": cond["_vix"],
            "regime_trend": FE._ret_over(spy_r[None, :], 252, 21)[0],
            "book": "EQ_LONG_ONLY_TOPN"}


def assemble(scope: str, mode: str, horizon: int) -> dict:
    key = (scope, mode, int(horizon))
    if key in _DS_CACHE:
        return _DS_CACHE[key]
    if scope == AC_US_EQUITY:
        ds = assemble_equity(horizon)
    elif scope == AC_CREDIT:
        ds = assemble_credit_proxy(horizon)
    else:
        ds = assemble_futures(scope, mode, horizon)
    _DS_CACHE[key] = ds
    return ds


# --------------------------------------------------------------------------- #
# The grid
# --------------------------------------------------------------------------- #
def cells_for(scope: str, mode: str) -> list:
    """(dimension, kind) pairs a scope/mode can run. kind: ABLATION for a
    baseline dimension, AUGMENTATION for a new one."""
    base = ONT.baseline_for(scope)
    out = [(d, "ABLATION") for d in base]
    if scope == AC_US_EQUITY:
        out += [(d, "AUGMENTATION") for d in EQ_NEW_DIMS]
        return out
    if scope == AC_CREDIT:
        return [(d, "ABLATION") for d in base if d not in ("LIQUIDITY", "VOLUME_PARTICIPATION")] + \
               [(d, "AUGMENTATION") for d in FUT_MARKET_DIMS + ("VOLATILITY_EXPECTATIONS_IV",)]
    inst = [d for d in FUT_INSTRUMENT_DIMS
            if not (d == "INVENTORY" and scope not in (AC_COMMODITY, AC_CROSS_ASSET))]
    out += [(d, "AUGMENTATION") for d in inst]
    if mode == "TS":
        out += [(d, "AUGMENTATION") for d in FUT_MARKET_DIMS]
    return out


def run_one(scope: str, mode: str, horizon: int, dimension: str, kind: str) -> dict:
    ds = assemble(scope, mode, horizon)
    base = tuple(d for d in ONT.baseline_for(scope) if d in ds["blocks"])
    if kind == "ABLATION":
        dims_b = tuple(d for d in base if d != dimension)
    else:
        dims_b = base
    if dimension not in ds["blocks"]:
        return {"scope": scope, "mode": mode, "horizon": int(horizon), "dimension": dimension,
                "kind": kind, "verdict": S.V_DATA_HOLD, "why": "dimension has no block in this scope"}
    if scope == AC_CREDIT or (scope == AC_VOLATILITY and mode == "XS"):
        mode = "TS"
    t0 = time.time()
    cell = S.run_cell(ds, dims_b, dimension)
    cell["kind"] = kind
    cell["seconds"] = round(time.time() - t0, 1)
    cell["cell_id"] = "%s|%s|%d|%s" % (scope, mode, int(horizon), dimension)
    return cell


def default_grid() -> list:
    grid = []
    for scope in XS_SCOPES:
        for mode in ("XS", "TS"):
            for h in HORIZONS:
                for dim, kind in cells_for(scope, mode):
                    grid.append((scope, mode, h, dim, kind))
    for h in HORIZONS:
        for dim, kind in cells_for(AC_VOLATILITY, "TS"):
            grid.append((AC_VOLATILITY, "TS", h, dim, kind))
        for dim, kind in cells_for(AC_CREDIT, "TS"):
            grid.append((AC_CREDIT, "TS", h, dim, kind))
        for dim, kind in cells_for(AC_US_EQUITY, "XS"):
            grid.append((AC_US_EQUITY, "XS", h, dim, kind))
    return grid


def grid_for_scopes(scopes: tuple, horizons: tuple = HORIZONS) -> list:
    return [g for g in default_grid() if g[0] in scopes and g[2] in horizons]


def partial_name(part: str) -> str:
    return "partial_%s.json" % part


def run_grid(grid: list | None = None, *, verbose: bool = True,
             checkpoint_every: int = 10, part: str | None = None) -> list:
    """Run cells, checkpointing so a crash resumes. With ``part`` the cells
    are checkpointed to ``partials/partial_<part>.json`` so several processes
    can each own a slice of the grid; ``merge_partials`` combines them."""
    grid = grid or default_grid()
    if part:
        prior = read_artifact(partial_name(part), subdir="partials") or {}
    else:
        prior = read_artifact(MATRIX_ARTIFACT) or {}
    done = {c["cell_id"]: c for c in (prior.get("cells") or [])}
    cells = list(done.values())
    n_new = 0
    for k, (scope, mode, h, dim, kind) in enumerate(grid):
        cid = "%s|%s|%d|%s" % (scope, mode, int(h), dim)
        if cid in done:
            continue
        try:
            cell = run_one(scope, mode, h, dim, kind)
        except Exception as exc:                          # noqa: BLE001
            cell = {"cell_id": cid, "scope": scope, "mode": mode, "horizon": int(h),
                    "dimension": dim, "kind": kind, "verdict": S.V_DATA_HOLD,
                    "why": "ERROR %s: %s" % (type(exc).__name__, str(exc)[:200])}
        cell["cell_id"] = cid
        cells.append(cell)
        done[cid] = cell
        n_new += 1
        if verbose:
            c = cell.get("conditional") or {}
            e = cell.get("economics") or {}
            print("[%d/%d] %s -> %s inc=%s t=%s econ=%s %.0fs" % (
                k + 1, len(grid), cid, cell.get("verdict"),
                None if c.get("increment") is None else round(c["increment"], 4),
                None if c.get("t") is None else round(c["t"], 2),
                None if e.get("ann_net_increment") is None else round(e["ann_net_increment"], 4),
                cell.get("seconds", 0)), flush=True)
        if n_new % checkpoint_every == 0:
            _checkpoint(cells, part)
    _checkpoint(cells, part)
    return cells


def _checkpoint(cells: list, part: str | None) -> None:
    if part:
        write_artifact(partial_name(part), {"schema": "r63_matrix_partial/1", "part": part,
                                            "n_cells": len(cells),
                                            "cells": sorted([_slim(c) for c in cells], key=lambda c: c["cell_id"])},
                       subdir="partials")
    else:
        write_matrix(cells)


def merge_partials() -> list:
    """Combine every partial into the ONE matrix artifact (BH across all)."""
    import glob as _glob
    from . import research_root
    cells: dict = {}
    for p in sorted(_glob.glob(str(research_root() / "partials" / "partial_*.json"))):
        body = read_artifact(Path(p).name, subdir="partials") or {}
        for c in body.get("cells") or []:
            cells[c["cell_id"]] = c
    out = list(cells.values())
    write_matrix(out)
    return out


def _slim(cell: dict) -> dict:
    c = dict(cell)
    c.pop("folds", None)
    return c


def write_matrix(cells: list) -> None:
    fdr = apply_fdr(cells)
    write_artifact(MATRIX_ARTIFACT, {
        "schema": "r63_information_sensitivity_matrix/1",
        "calculation_owner": CALCULATION_OWNER,
        "n_cells": len(cells),
        "cells": sorted([_slim(c) for c in cells], key=lambda c: c["cell_id"]),
        "fdr": {k: v for k, v in fdr.items() if k != "per_test"},
    })


def apply_fdr(cells: list) -> dict:
    """Benjamini-Hochberg across every conditional primary p-value, then the
    final verdict per cell (mutates the cells' 'verdict' and 'fdr_pass')."""
    p_cond = {c["cell_id"]: (c.get("conditional") or {}).get("p_one_sided") for c in cells
              if c.get("conditional")}
    p_econ = {c["cell_id"]: (c.get("economics") or {}).get("p_increment_one_sided") for c in cells
              if c.get("economics")}
    bh_c = S.bh_fdr(p_cond, BH_Q)
    bh_e = S.bh_fdr(p_econ, BH_Q)
    raw_c = sum(1 for v in p_cond.values() if v is not None and v < 0.05)
    raw_e = sum(1 for v in p_econ.values() if v is not None and v < 0.05)
    for c in cells:
        if c.get("conditional"):
            fp = bh_c["per_test"].get(c["cell_id"], False)
            c["fdr_pass_conditional"] = bool(fp)
            c["fdr_pass_economic"] = bool(bh_e["per_test"].get(c["cell_id"], False))
            c["verdict"] = S.verdict(c, fdr_pass=fp)
    return {"conditional": {"m": bh_c["m"], "q": BH_Q, "raw_p_below_0_05": raw_c,
                            "n_rejected": bh_c["n_rejected"], "threshold": bh_c["threshold"],
                            "survivors": bh_c["survivors"]},
            "economic": {"m": bh_e["m"], "q": BH_Q, "raw_p_below_0_05": raw_e,
                         "n_rejected": bh_e["n_rejected"], "threshold": bh_e["threshold"],
                         "survivors": bh_e["survivors"]},
            "not_counted": [{"cell_id": c["cell_id"], "why": c.get("why")} for c in cells
                            if not c.get("conditional")]}


# --------------------------------------------------------------------------- #
# Orthogonality matrix
# --------------------------------------------------------------------------- #
def orthogonality(scopes: tuple = XS_SCOPES + (AC_VOLATILITY, AC_US_EQUITY)) -> dict:
    """Per scope: each dimension's residual share against the baseline and the
    pairwise |Spearman| between dimension composites, on rows before the
    lockbox only."""
    out = {}
    for scope in scopes:
        mode = "XS" if scope in XS_SCOPES or scope == AC_US_EQUITY else "TS"
        try:
            ds = assemble(scope, mode, 21)
        except Exception as exc:                          # noqa: BLE001
            out[scope] = {"state": "UNAVAILABLE", "error": type(exc).__name__}
            continue
        dec = ds["dec"]
        lock = int(np.searchsorted(ds["dates"], LOCKBOX_START))
        pre = dec[dec < lock]
        base = tuple(d for d in ONT.baseline_for(scope) if d in ds["blocks"])
        dims = [d for d in ds["blocks"] if d not in base]
        elig = ds["elig"][:, pre]
        comp = {}
        for d, blk in ds["blocks"].items():
            X = blk[:, pre, :]
            # composite = mean of per-feature z-scores over the pre-lockbox rows
            z = (X - np.nanmean(X, axis=(0, 1))) / (np.nanstd(X, axis=(0, 1)) + 1e-12)
            comp[d] = np.where(elig, np.nanmean(z, axis=-1), np.nan)
        B = np.concatenate([ds["blocks"][d][:, pre, :] for d in base], axis=-1)
        Bf = B.reshape(-1, B.shape[-1])
        res = {}
        for d in dims:
            D = ds["blocks"][d][:, pre, :].reshape(-1, ds["blocks"][d].shape[-1])
            res[d] = S._residual_share(D, Bf)
        names = sorted(comp)
        M = np.full((len(names), len(names)), np.nan)
        flat = {d: comp[d].ravel() for d in names}
        for a in range(len(names)):
            for b in range(a, len(names)):
                x, y = flat[names[a]], flat[names[b]]
                ok = np.isfinite(x) & np.isfinite(y)
                if ok.sum() > 500:
                    if ok.sum() > 50000:
                        stride = int(np.ceil(ok.sum() / 50000))
                        xs, ys = x[ok][::stride], y[ok][::stride]
                    else:
                        xs, ys = x[ok], y[ok]
                    r = S.spearman(xs, ys)
                    M[a, b] = M[b, a] = abs(r) if np.isfinite(r) else np.nan
        out[scope] = {"state": "OK", "baseline": list(base), "dimensions": names,
                      "residual_share_vs_baseline": res,
                      "pairwise_abs_spearman": {names[a]: {names[b]: (None if not np.isfinite(M[a, b]) else round(float(M[a, b]), 4))
                                                           for b in range(len(names))}
                                                for a in range(len(names))},
                      "rows_pre_lockbox": int(elig.sum())}
    body = {"schema": "r63_orthogonality_matrix/1", "calculation_owner": CALCULATION_OWNER,
            "rule": "residual share = Var(D - OLS(D ~ baseline)) / Var(D) on pre-lockbox rows; "
                    "REDUNDANT < 0.10, PARTIALLY_REDUNDANT < 0.35, DISTINCT otherwise",
            "scopes": out}
    write_artifact(ORTHO_ARTIFACT, body)
    return body


# --------------------------------------------------------------------------- #
# Campaign summary
# --------------------------------------------------------------------------- #
def summarise(cells: list) -> dict:
    fdr = apply_fdr(cells)
    by_verdict: dict = {}
    for c in cells:
        by_verdict.setdefault(c.get("verdict"), []).append(c["cell_id"])
    scored = [c for c in cells if c.get("conditional")]
    top = sorted(scored, key=lambda c: -(c["conditional"].get("t") or -99))[:40]
    strongest_by_scope: dict = {}
    for c in scored:
        k = c["scope"]
        cur = strongest_by_scope.get(k)
        if cur is None or (c["conditional"].get("t") or -99) > (cur["conditional"].get("t") or -99):
            strongest_by_scope[k] = c
    body = {
        "schema": "r63_experiment_results/1",
        "calculation_owner": CALCULATION_OWNER,
        "n_cells": len(cells), "n_scored": len(scored),
        "by_verdict": {k: len(v) for k, v in sorted(by_verdict.items(), key=lambda kv: str(kv[0]))},
        "fdr": fdr,
        "strongest_by_scope": {k: _brief(v) for k, v in sorted(strongest_by_scope.items())},
        "top_conditional_cells": [_brief(c) for c in top],
        "candidates": [_brief(c) for c in cells if c.get("verdict") == S.V_CANDIDATE],
        "generated_at": now_iso(),
    }
    write_artifact(RESULTS_ARTIFACT, body)
    return body


def _brief(c: dict) -> dict:
    co, e, r, s = (c.get("conditional") or {}), (c.get("economics") or {}), \
        (c.get("redundancy") or {}), (c.get("stability") or {})
    return {"cell_id": c["cell_id"], "verdict": c.get("verdict"), "kind": c.get("kind"),
            "coverage": c.get("coverage"), "effective_periods": c.get("effective_periods"),
            "standalone_t": (c.get("standalone") or {}).get("t"),
            "increment": co.get("increment"), "t": co.get("t"), "p": co.get("p_one_sided"),
            "t_selection": co.get("t_selection"), "t_lockbox": co.get("t_lockbox"),
            "lockbox_sign_agrees": co.get("lockbox_sign_agrees"),
            "partial_t": (c.get("secondary") or {}).get("partial_t"),
            "permutation_drop": (c.get("secondary") or {}).get("permutation_drop"),
            "residual_share": r.get("residual_share"), "redundancy": r.get("redundancy"),
            "ann_net_increment": e.get("ann_net_increment"),
            "sharpe_increment": e.get("sharpe_increment"), "t_econ": e.get("t_increment"),
            "turnover_augmented": (e.get("augmented") or {}).get("mean_oneway_turnover"),
            "share_blocks_positive": s.get("share_blocks_positive"),
            "fdr_pass_conditional": c.get("fdr_pass_conditional")}
