"""alpha_agent.alpha_recovery.cadence - signal observation frequency != trading frequency.

Workstream 7. R63/R64 found the strongest conditional information in the
estate at the ONE-session horizon (FX carry, cross-asset trend) and watched
the daily book that carried it lose money to cost drag. The hypothesis
tested here, on a SMALL pre-registered grid:

    a daily-observed cross-sectional signal, traded every k sessions with a
    no-trade band, keeps most of its information and sheds most of its cost.

The conditional statistic is the R63 cell's own and is NOT re-tested by
cadence. The ONE scorer keeps its out-of-sample scores for both arms; the
R64 construction pieces (inverse-vol selection, class risk budget,
instrument cap, vol target, no-trade band, costs) are applied identically to
both arms; the only new degree of freedom is the rebalance cadence, and it is
fixed in the protocol (1, 5, 10, 21 sessions; band 0.25 and 0.50).

Verdicts use the R64 ladder verbatim; multiplicity is Holm within the family
and Benjamini-Hochberg across the campaign's economic p-values. The lockboxes
of these families were viewed by R63/R64: their results here are
POST-SELECTION evidence and are labelled so.
"""
from __future__ import annotations

import json
import math
import re
import time

import numpy as np
import pandas as pd

from alpha_agent.r63 import AC_CROSS_ASSET, AC_FX, LOCKBOX_START, ontology as ONT, pit
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import DIM_CURVE_CARRY, MATERIALITY_ANN_NET, MATERIALITY_SHARPE, V_DATA_HOLD
from alpha_agent.r64 import V_ECON, V_NO_VALUE, V_NOT_ECON, V_NOT_FDR, V_UNSTABLE
from alpha_agent.r64 import construction as B
from alpha_agent.r64 import experiments as R64X
from alpha_agent.r64 import family as FAM

from . import BH_Q, CONDITIONAL_T_FLOOR, HOLM_ALPHA, MIN_EFFECTIVE_PERIODS, research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.cadence"
CELLS_DIR = "cells"
ARTIFACT_NAME = "cadence_grid.json"
PPY = 252.0
POST_SELECTION = "POST_SELECTION: the R63/R64 lockbox of this family was viewed before this campaign"

TRADE_EVERY = (1, 5, 10, 21)
BANDS = (0.25, 0.50)


def default_grid() -> list:
    g = []
    for k in TRADE_EVERY:
        g.append({"scope": AC_FX, "dimension": DIM_CURVE_CARRY, "kind": "AUGMENTATION",
                  "family": "FX_CARRY_CADENCE", "trade_every": k, "band": 0.25})
    for k, b in ((5, 0.50), (21, 0.50)):
        g.append({"scope": AC_FX, "dimension": DIM_CURVE_CARRY, "kind": "AUGMENTATION",
                  "family": "FX_CARRY_CADENCE", "trade_every": k, "band": b})
    for k in TRADE_EVERY:
        g.append({"scope": AC_CROSS_ASSET, "dimension": "TREND", "kind": "ABLATION",
                  "family": "CROSS_ASSET_TREND_CADENCE", "trade_every": k, "band": 0.25})
    for sp in g:
        sp["horizon"] = 1
        sp["mode"] = "XS"
        sp["cell_id"] = "%s|XS|1|%s|k%d|b%.2f" % (sp["scope"], sp["dimension"], sp["trade_every"], sp["band"])
    return g


# --------------------------------------------------------------------------- #
# The book with a rebalance cadence
# --------------------------------------------------------------------------- #
def build_book_cadence(pred: np.ndarray, y: np.ndarray, gid: np.ndarray, iid: np.ndarray,
                       vol: np.ndarray, cost: np.ndarray, *, horizon: int, trade_every: int,
                       band: float, class_of: np.ndarray | None = None,
                       policy: dict | None = None) -> dict:
    """R64's risk-controlled cross-sectional book, recomputing targets only
    every ``trade_every`` periods; between rebalances the previous weights are
    held (returns accrue on them, no cost). Identical code for both arms."""
    pol = dict(B.default_policy())
    pol["no_trade_band"] = float(band)
    if policy:
        pol.update(policy)
    order = np.lexsort((iid, gid))
    p, yy, g, ii, v, c = pred[order], y[order], gid[order], iid[order], vol[order], cost[order]
    cls_all = np.asarray(class_of)[order] if class_of is not None else None
    periods = np.unique(g)
    starts = np.searchsorted(g, periods, side="left")
    ends = np.searchsorted(g, periods, side="right")
    gross, net, turn, costs, maxw, glev, lev_state, dates_ix = [], [], [], [], [], [], [], []
    unlev_hist: list = []
    prev: dict = {}
    k_since = trade_every  # rebalance on the first period
    for gi, (a, b) in enumerate(zip(starts, ends)):
        pp, yv, inst, vv, cc = p[a:b], yy[a:b], ii[a:b], v[a:b], c[a:b]
        cl = cls_all[a:b] if cls_all is not None else None
        ok = np.isfinite(pp) & np.isfinite(yv)
        pp, yv, inst, vv, cc = pp[ok], yv[ok], inst[ok], vv[ok], cc[ok]
        cl = cl[ok] if cl is not None else None
        ret_by = {int(j): float(x) for j, x in zip(inst, yv)}
        vol_by = {int(j): float(x) for j, x in zip(inst, vv)}
        cost_by = {int(j): float(x) for j, x in zip(inst, cc)}
        med_cost = float(np.median(cc)) if len(cc) else 0.0
        w_u = B.xs_unlevered(pp, vv, inst, max_name_weight=pol["max_name_weight"])
        if not w_u:
            continue
        if cl is not None and pol.get("equal_asset_class_risk_budget", True):
            cls_by = {int(j): str(x) for j, x in zip(inst, cl)}
            w_u = B.apply_class_risk_budget(w_u, vol_by, cls_by)
        w_u = B.apply_instrument_cap(w_u, vol_by, float(pol["max_instrument_risk_share"]))
        gross_u = sum(abs(x) for x in w_u.values())
        r_u = sum(w_u[x] * ret_by.get(x, 0.0) for x in w_u)
        lev, state = B.leverage_from_history(unlev_hist, gross_u, horizon, pol)
        unlev_hist.append(r_u)
        if k_since >= trade_every or not prev:
            w_t = {x: lev * val for x, val in w_u.items()}
            w = B.apply_no_trade_band(w_t, prev, float(pol["no_trade_band"]))
            k_since = 1
        else:
            # hold: a name that left the tradable set is closed (it cannot be held)
            w = {x: val for x, val in prev.items() if x in ret_by}
            k_since += 1
        traded = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) for x in set(w) | set(prev))
        cost_t = sum(abs(w.get(x, 0.0) - prev.get(x, 0.0)) * cost_by.get(x, med_cost)
                     for x in set(w) | set(prev))
        gr = sum(w[x] * ret_by.get(x, 0.0) for x in w)
        g_all = sum(abs(x) for x in w.values())
        gross.append(gr)
        net.append(gr - cost_t)
        turn.append(traded / 2.0)
        costs.append(cost_t)
        maxw.append(max(abs(x) for x in w.values()) if w else 0.0)
        glev.append(g_all)
        lev_state.append(state)
        dates_ix.append(int(periods[gi]))
        prev = w
    return {"gross": np.array(gross), "net": np.array(net), "turnover": np.array(turn),
            "cost": np.array(costs), "max_weight": np.array(maxw), "gross_leverage": np.array(glev),
            "max_class_share": np.ones(len(net)), "leverage_state": np.array(lev_state, dtype=object),
            "unlevered_gross": np.array(unlev_hist), "gid": np.array(dates_ix),
            "book": "XS_LONG_SHORT_RISK_CONTROLLED_CADENCE_%d" % trade_every, "policy": pol}


def _stats(book_b: dict, book_bd: dict, *, horizon: int, lag: int, slot_dates: np.ndarray) -> dict:
    inc = B.paired_increment(book_b, book_bd, horizon, lag)
    n = min(len(book_b["net"]), len(book_bd["net"]))
    gids = book_bd["gid"][:n]
    dates = np.array([str(slot_dates[int(g)]) for g in gids])
    lock = dates >= LOCKBOX_START
    diff = book_bd["net"][:n] - book_b["net"][:n]
    ppy = PPY / horizon
    st_sel = S.nw_tstat(diff[~lock], lag) if (~lock).sum() > 3 else {"mean": None, "t": None}
    st_lock = S.nw_tstat(diff[lock], lag) if lock.sum() > 3 else {"mean": None, "t": None}

    def _sub(book, mask):
        r = book["net"][:n][mask]
        sd = float(np.std(r, ddof=1)) if len(r) > 2 else float("nan")
        return {"periods": int(mask.sum()), "ann_net": float(r.mean() * ppy) if len(r) else None,
                "sharpe": float(r.mean() / sd * math.sqrt(ppy)) if sd and sd > 0 else None,
                "max_dd": S._max_dd(r) if len(r) else None}
    inc["selection"] = {"increment_ann": (st_sel["mean"] or 0.0) * ppy if st_sel["mean"] is not None else None,
                        "t": st_sel["t"], "augmented": _sub(book_bd, ~lock), "baseline": _sub(book_b, ~lock)}
    inc["lockbox"] = {"increment_ann": (st_lock["mean"] or 0.0) * ppy if st_lock["mean"] is not None else None,
                      "t": st_lock["t"], "augmented": _sub(book_bd, lock), "baseline": _sub(book_b, lock)}
    inc["lockbox_sign_agrees"] = (bool(np.sign(st_lock["mean"]) == np.sign(st_sel["mean"]))
                                  if st_lock["mean"] is not None and st_sel["mean"] is not None else None)
    inc["evidence_label"] = POST_SELECTION
    return inc


def verdict(cell: dict, *, fdr_pass: bool | None = None) -> str:
    c = cell.get("conditional") or {}
    if not c or (cell.get("effective_periods") or 0) < MIN_EFFECTIVE_PERIODS:
        return V_DATA_HOLD
    t = c.get("t")
    if t is None or t < CONDITIONAL_T_FLOOR or (c.get("increment") or 0.0) <= 0:
        return V_NO_VALUE
    e = cell.get("cadence_economics") or {}
    aug = e.get("augmented") or {}
    if not e or aug.get("degenerate_under_controls"):
        return V_NOT_ECON
    inc, sh = e.get("ann_net_increment"), e.get("sharpe_increment")
    if not ((inc is not None and inc >= MATERIALITY_ANN_NET) or (sh is not None and sh >= MATERIALITY_SHARPE)):
        return V_NOT_ECON
    s = cell.get("stability") or {}
    if (s.get("share_blocks_positive") or 0.0) < 0.60 or e.get("lockbox_sign_agrees") is False:
        return V_UNSTABLE
    if fdr_pass is False:
        return V_NOT_FDR
    return V_ECON


# --------------------------------------------------------------------------- #
# Cells
# --------------------------------------------------------------------------- #
_SCORED: dict = {}


def _scored(scope: str, dimension: str, kind: str) -> tuple:
    key = (scope, dimension, kind)
    if key in _SCORED:
        return _SCORED[key]
    ds = R64X.assemble(scope, "XS", 1)
    base = tuple(d for d in ONT.baseline_for(scope) if d in ds["blocks"])
    dims_b = tuple(d for d in base if d != dimension) if kind == "ABLATION" else base
    t0 = time.time()
    cell = S.run_cell(ds, dims_b, dimension, keep_predictions=True)
    preds = cell.pop("_predictions", None)
    cell["seconds_scoring"] = round(time.time() - t0, 1)
    cell.pop("folds", None)
    _SCORED[key] = (ds, cell, preds)
    return _SCORED[key]


def measure_cell(sp: dict, *, verbose: bool = True) -> dict:
    ds, base_cell, preds = _scored(sp["scope"], sp["dimension"], sp["kind"])
    cell = dict(base_cell)
    cell.update({k: v for k, v in sp.items()})
    cell["calculation_owner"] = CALCULATION_OWNER
    if preds is None or not cell.get("conditional"):
        cell["cadence_verdict"] = V_DATA_HOLD
        return R64X._jsonable(cell)
    h = 1
    lag = pit.nw_lag(h, int(ds["cadence"]))
    okp = np.isfinite(preds["pred_B"]) & np.isfinite(preds["pred_BD"])
    cls = np.asarray(ds["class_of"])[preds["iid"]] if sp["scope"] == AC_CROSS_ASSET else None
    common = dict(y=preds["y_raw"][okp], gid=preds["gid"][okp], iid=preds["iid"][okp],
                  vol=preds["vol"][okp], cost=preds["cost"][okp], horizon=h,
                  trade_every=int(sp["trade_every"]), band=float(sp["band"]),
                  class_of=(cls[okp] if cls is not None else None))
    t0 = time.time()
    book_b = build_book_cadence(preds["pred_B"][okp], **common)
    book_bd = build_book_cadence(preds["pred_BD"][okp], **common)
    cell["cadence_economics"] = _stats(book_b, book_bd, horizon=h, lag=lag, slot_dates=preds["slot_dates"])
    cell["seconds_books"] = round(time.time() - t0, 1)
    cell["cadence_verdict"] = verdict(cell)
    return R64X._jsonable(cell)


def _cell_path(cell_id: str):
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "_", cell_id)
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("%s.json" % safe)


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        try:
            c = json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if c.get("calculation_owner") == CALCULATION_OWNER:
            out.append(c)
    return out


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    grid = grid or default_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            cells.append(json.loads(p.read_text(encoding="utf-8")))
            if verbose:
                print("[%d/%d] %s (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
            continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid), sp["cell_id"]), flush=True)
        try:
            cell = measure_cell(sp, verbose=verbose)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "cadence_verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc),
                    "calculation_owner": CALCULATION_OWNER}
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            e = cell.get("cadence_economics") or {}
            aug = e.get("augmented") or {}
            print("    verdict=%s inc=%s sharpe_inc=%s t=%s aug_net=%s aug_sharpe=%s aug_dd=%s turn=%s" % (
                cell.get("cadence_verdict"),
                None if e.get("ann_net_increment") is None else round(e["ann_net_increment"], 4),
                None if e.get("sharpe_increment") is None else round(e["sharpe_increment"], 3),
                None if e.get("t_increment") is None else round(e["t_increment"], 2),
                None if aug.get("ann_net") is None else round(aug["ann_net"], 4),
                None if aug.get("sharpe") is None else round(aug["sharpe"], 3),
                None if aug.get("max_dd") is None else round(aug["max_dd"], 3),
                None if aug.get("mean_oneway_turnover") is None else round(aug["mean_oneway_turnover"], 3)),
                flush=True)
    return cells


def brief(c: dict) -> dict:
    e = c.get("cadence_economics") or {}
    aug, bas = e.get("augmented") or {}, e.get("baseline") or {}
    return {"cell_id": c.get("cell_id"), "family": c.get("family"), "trade_every": c.get("trade_every"),
            "band": c.get("band"), "cadence_verdict": c.get("cadence_verdict"),
            "conditional_t": (c.get("conditional") or {}).get("t"),
            "ann_net_increment": e.get("ann_net_increment"),
            "ann_net_increment_at_2x_cost": e.get("ann_net_increment_at_2x_cost"),
            "sharpe_increment": e.get("sharpe_increment"), "t_increment": e.get("t_increment"),
            "p_increment": e.get("p_increment_one_sided"),
            "augmented_ann_net": aug.get("ann_net"), "augmented_sharpe": aug.get("sharpe"),
            "augmented_max_dd": aug.get("max_dd"), "augmented_turnover": aug.get("mean_oneway_turnover"),
            "augmented_cost_drag": aug.get("ann_cost_drag"), "augmented_degenerate": aug.get("degenerate_under_controls"),
            "baseline_ann_net": bas.get("ann_net"), "baseline_sharpe": bas.get("sharpe"),
            "selection_augmented_sharpe": ((e.get("selection") or {}).get("augmented") or {}).get("sharpe"),
            "lockbox_augmented_sharpe": ((e.get("lockbox") or {}).get("augmented") or {}).get("sharpe"),
            "lockbox_increment_ann": (e.get("lockbox") or {}).get("increment_ann"),
            "lockbox_sign_agrees": e.get("lockbox_sign_agrees"),
            "fdr_pass_economic": c.get("fdr_pass_economic"), "holm_pass_family": c.get("holm_pass_family"),
            "effective_periods": c.get("effective_periods"), "periods": e.get("periods"),
            "evidence_label": e.get("evidence_label"), "error": c.get("error")}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    p_econ = {c["cell_id"]: (c.get("cadence_economics") or {}).get("p_increment_one_sided") for c in cells
              if c.get("cadence_economics")}
    bh = S.bh_fdr(p_econ, BH_Q)
    fam_p: dict = {}
    for c in cells:
        if c.get("cadence_economics"):
            fam_p.setdefault(c.get("family"), {})[c["cell_id"]] = p_econ[c["cell_id"]]
    holm = {f: FAM.holm(ps, HOLM_ALPHA) for f, ps in fam_p.items()}
    for c in cells:
        if not c.get("cadence_economics"):
            continue
        c["fdr_pass_economic"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get(c.get("family"), {}).get("rejected", {}).get(c["cell_id"])
        c["cadence_verdict"] = verdict(c, fdr_pass=(c["fdr_pass_economic"] and c["holm_pass_family"]))
    # the pre-registered selection rule: highest augmented Sharpe on SELECTION periods
    chosen = {}
    for fam in fam_p:
        rows = [c for c in cells if c.get("family") == fam and c.get("cadence_economics")]
        if not rows:
            continue
        best = max(rows, key=lambda c: ((((c["cadence_economics"].get("selection") or {}).get("augmented") or {}).get("sharpe")) or -9))
        chosen[fam] = {"cell_id": best["cell_id"], "trade_every": best.get("trade_every"), "band": best.get("band"),
                       "selection_augmented_sharpe": (((best["cadence_economics"].get("selection") or {}).get("augmented") or {}).get("sharpe")),
                       "lockbox_augmented_sharpe": (((best["cadence_economics"].get("lockbox") or {}).get("augmented") or {}).get("sharpe")),
                       "lockbox_augmented_ann_net": (((best["cadence_economics"].get("lockbox") or {}).get("augmented") or {}).get("ann_net")),
                       "cadence_verdict": best.get("cadence_verdict")}
    counts: dict = {}
    for c in cells:
        counts[c.get("cadence_verdict")] = counts.get(c.get("cadence_verdict"), 0) + 1
    body = {"schema": "alpha_recovery_cadence_grid/1", "calculation_owner": CALCULATION_OWNER,
            "hypothesis": "signal observation frequency != required trading frequency",
            "grid": default_grid(), "n_cells": len(cells), "counts": counts,
            "multiple_testing": {"benjamini_hochberg_economic": {k: v for k, v in bh.items() if k != "per_test"},
                                 "holm_by_family": {f: {k: v for k, v in h.items() if k != "adjusted"} for f, h in holm.items()}},
            "selection_rule": "highest augmented Sharpe on SELECTION periods; lockbox reported once",
            "chosen_by_family": chosen, "evidence_label": POST_SELECTION,
            "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])), "cells": cells}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body


def sleeve_series(cell_id: str) -> pd.Series | None:
    """The augmented book's per-session NET returns of a scored cadence cell,
    indexed by date, for the cross-domain test. Rebuilt from the kept
    predictions in this process (the persisted cell carries no rows)."""
    sp = next((s for s in default_grid() if s["cell_id"] == cell_id), None)
    if sp is None:
        return None
    ds, cell, preds = _scored(sp["scope"], sp["dimension"], sp["kind"])
    if preds is None:
        return None
    okp = np.isfinite(preds["pred_B"]) & np.isfinite(preds["pred_BD"])
    cls = np.asarray(ds["class_of"])[preds["iid"]] if sp["scope"] == AC_CROSS_ASSET else None
    book = build_book_cadence(preds["pred_BD"][okp], y=preds["y_raw"][okp], gid=preds["gid"][okp],
                              iid=preds["iid"][okp], vol=preds["vol"][okp], cost=preds["cost"][okp],
                              horizon=1, trade_every=int(sp["trade_every"]), band=float(sp["band"]),
                              class_of=(cls[okp] if cls is not None else None))
    dates = [str(preds["slot_dates"][int(g)]) for g in book["gid"]]
    return pd.Series(book["net"], index=pd.to_datetime(dates))
