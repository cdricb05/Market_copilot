"""alpha_agent.r64.experiments - the R64 cells.

One scorer, two books. Every conditional statistic here is produced by
``alpha_agent.r63.sensitivity.run_cell`` on the identical rows, folds and
forced ridge penalty R63 used; R64 asks that owner to keep its out-of-sample
scores (``keep_predictions``) and prices the SAME scores through the
risk-controlled book of ``alpha_agent.r64.construction``, for BOTH arms. A cell
R63 already ran is REPRODUCED and compared to the persisted R63 matrix to
1e-9; a mismatch is REPRODUCTION_FAILED and no economic claim is made on it.

Grid (protocol section "cells"):

    REPRODUCTION          FX_FUTURES x {XS, TS} x {1,5,21,63} x CARRY
    CARRY_FRONTIER        RATES / COMMODITY / EQUITY_INDEX / CROSS_ASSET x XS
                          x {1,5,21,63} x CARRY; VOLATILITY x TS x {..} x CARRY
    CARRY_VARIANT         FX x XS x {..} x CARRY_TO_RISK;
                          CROSS_ASSET x XS x {..} x CARRY_CLASS_NEUTRAL
    CONSTRUCTION_RESCUE   CROSS_ASSET x XS x {..} x TREND (ablation);
                          RATES x XS x 63 x REALISED_VOLATILITY (ablation)

Every cell is checkpointed under ``<research root>/cells`` so a run resumes;
``merge`` applies the campaign Benjamini-Hochberg families and writes the ONE
R64 matrix. Research only; reads owned stores; writes only under the R64 root.
"""
from __future__ import annotations

import json
import re
import time

import numpy as np

from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import ontology as ONT
from alpha_agent.r63 import panels as P
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (AC_COMMODITY, AC_CROSS_ASSET, AC_EQUITY_INDEX, AC_FX, AC_RATES,
               AC_VOLATILITY, CARRY_FAMILY, CARRY_VARIANTS, CONDITIONAL_T_FLOOR,
               DIM_CARRY_CLASS_NEUTRAL, DIM_CARRY_TO_RISK, DIM_CURVE_CARRY, HORIZONS,
               MATERIALITY_ANN_NET, MATERIALITY_SHARPE, MIN_EFFECTIVE_PERIODS,
               V_DATA_HOLD, V_ECON, V_NO_VALUE, V_NOT_ECON, V_NOT_FDR, V_REPRO_FAILED,
               V_UNSTABLE, read_r63_artifact, research_root, write_artifact)
from . import carry as C
from . import construction as B
from . import handoff_validation as HV

CALCULATION_OWNER = "alpha_agent.r64.experiments"
CELLS_DIR = "cells"
MATRIX_ARTIFACT = "r64_carry_matrix.json"

TAG_REPRODUCTION = "REPRODUCTION"
TAG_CARRY_FRONTIER = "CARRY_FRONTIER"
TAG_VARIANT = "CARRY_VARIANT"
TAG_RESCUE = "CONSTRUCTION_RESCUE"
TAGS = (TAG_REPRODUCTION, TAG_CARRY_FRONTIER, TAG_VARIANT, TAG_RESCUE)

_DS: dict = {}
_R63_MATRIX: dict = {}


def spec(scope: str, mode: str, horizon: int, dimension: str, kind: str, tag: str) -> dict:
    return {"scope": scope, "mode": mode, "horizon": int(horizon), "dimension": dimension,
            "kind": kind, "tag": tag, "cell_id": "%s|%s|%d|%s" % (scope, mode, int(horizon),
                                                                    dimension)}


def default_grid() -> list:
    g = []
    for h in HORIZONS:
        for mode in ("XS", "TS"):
            g.append(spec(AC_FX, mode, h, DIM_CURVE_CARRY, "AUGMENTATION", TAG_REPRODUCTION))
    for scope in (AC_RATES, AC_COMMODITY, AC_EQUITY_INDEX, AC_CROSS_ASSET):
        for h in HORIZONS:
            g.append(spec(scope, "XS", h, DIM_CURVE_CARRY, "AUGMENTATION", TAG_CARRY_FRONTIER))
    for h in HORIZONS:
        g.append(spec(AC_VOLATILITY, "TS", h, DIM_CURVE_CARRY, "AUGMENTATION", TAG_CARRY_FRONTIER))
    for h in HORIZONS:
        g.append(spec(AC_FX, "XS", h, DIM_CARRY_TO_RISK, "AUGMENTATION", TAG_VARIANT))
    for h in HORIZONS:
        g.append(spec(AC_CROSS_ASSET, "XS", h, DIM_CARRY_CLASS_NEUTRAL, "AUGMENTATION",
                      TAG_VARIANT))
    for h in HORIZONS:
        g.append(spec(AC_CROSS_ASSET, "XS", h, "TREND", "ABLATION", TAG_RESCUE))
    g.append(spec(AC_RATES, "XS", 63, "REALISED_VOLATILITY", "ABLATION", TAG_RESCUE))
    return g


# --------------------------------------------------------------------------- #
# Datasets: the R63 dataset plus the R64 carry variants as extra blocks
# --------------------------------------------------------------------------- #
def assemble(scope: str, mode: str, horizon: int) -> dict:
    key = (scope, mode, int(horizon))
    if key in _DS:
        return _DS[key]
    ds = X.assemble(scope, mode, horizon)
    F = P.load_futures()
    rows = [F["markets"].index(m) for m in ds["inst"]]
    class_of = np.array([F["meta"][m]["asset_class"] for m in ds["inst"]])
    out = C.inject_variant_blocks(ds, F, rows, class_of)
    out["cost_bps_per_side"] = {m: F["meta"][m]["cost_bps_per_side"] for m in ds["inst"]}
    _DS[key] = out
    return out


def r63_reference(cell_id: str) -> dict | None:
    if "matrix" not in _R63_MATRIX:
        _R63_MATRIX["matrix"] = read_r63_artifact("information_sensitivity_matrix.json")
    return HV.find_cell(_R63_MATRIX["matrix"], cell_id)


def _jsonable(o):
    if isinstance(o, dict):
        return {str(k): _jsonable(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_jsonable(v) for v in o]
    if isinstance(o, np.ndarray):
        return [_jsonable(v) for v in o.tolist()]
    if isinstance(o, (np.bool_,)):
        return bool(o)
    if isinstance(o, (np.integer,)):
        return int(o)
    if isinstance(o, (np.floating,)):
        v = float(o)
        return v if np.isfinite(v) else None
    if isinstance(o, float) and not np.isfinite(o):
        return None
    return o


# --------------------------------------------------------------------------- #
# One cell
# --------------------------------------------------------------------------- #
def measure_cell(sp: dict) -> dict:
    scope, mode, h, dim, kind = sp["scope"], sp["mode"], int(sp["horizon"]), sp["dimension"], sp["kind"]
    ds = assemble(scope, mode, h)
    base = tuple(d for d in ONT.baseline_for(scope) if d in ds["blocks"])
    dims_b = tuple(d for d in base if d != dim) if kind == "ABLATION" else base
    if dim not in ds["blocks"]:
        return {**sp, "verdict": S.V_DATA_HOLD, "r64_verdict": V_DATA_HOLD,
                "why": "dimension has no block in this scope"}
    t0 = time.time()
    cell = S.run_cell(ds, dims_b, dim, keep_predictions=True)
    preds = cell.pop("_predictions", None)
    cell.update({"kind": kind, "cell_id": sp["cell_id"], "r64_tag": sp["tag"],
                 "seconds": round(time.time() - t0, 1), "calculation_owner": CALCULATION_OWNER,
                 "family": CARRY_FAMILY if dim in CARRY_VARIANTS else dim,
                 "base_dimension": DIM_CURVE_CARRY if dim in CARRY_VARIANTS else dim,
                 "instruments": list(ds["inst"]),
                 "cost_bps_per_side": ds.get("cost_bps_per_side")})
    # Reproduction against the persisted R63 matrix (cells R63 ran).
    if dim not in (DIM_CARRY_TO_RISK, DIM_CARRY_CLASS_NEUTRAL):
        ref = r63_reference(sp["cell_id"])
        if ref is not None:
            cell["r63_reference"] = {"verdict": ref.get("verdict"),
                                     "conditional": ref.get("conditional"),
                                     "economics": ref.get("economics"),
                                     "fdr_pass_conditional": ref.get("fdr_pass_conditional"),
                                     "fdr_pass_economic": ref.get("fdr_pass_economic")}
            if cell.get("conditional"):
                cell["reproduction"] = HV.compare_conditional(ref, cell)
    # R64 economics: the SAME scores through the risk-controlled book, both arms.
    if preds is not None and cell.get("conditional"):
        lag = pit.nw_lag(h, int(ds["cadence"]))
        okp = np.isfinite(preds["pred_B"]) & np.isfinite(preds["pred_BD"])
        cls = None
        if scope == AC_CROSS_ASSET:
            cls = np.asarray(ds["class_of"])[preds["iid"]]
        common = dict(y=preds["y_raw"][okp], gid=preds["gid"][okp], iid=preds["iid"][okp],
                      vol=preds["vol"][okp], cost=preds["cost"][okp], mode=mode, horizon=h,
                      class_of=(cls[okp] if cls is not None else None))
        book_b = B.build_book(preds["pred_B"][okp], pred_scale=preds["pred_scale"]["B"], **common)
        book_bd = B.build_book(preds["pred_BD"][okp], pred_scale=preds["pred_scale"]["BD"], **common)
        cell["r64_economics"] = B.paired_increment(book_b, book_bd, h, lag)
    cell["r64_verdict"] = verdict(cell)
    cell["n_folds"] = len(cell.get("folds") or [])
    cell.pop("folds", None)
    return _jsonable(cell)


def verdict(cell: dict, *, fdr_pass: bool | None = None) -> str:
    if "conditional" not in cell or cell.get("verdict") in (S.V_DATA_HOLD, S.V_NO_RESPONSE):
        return V_DATA_HOLD
    rep = cell.get("reproduction")
    if rep is not None and not rep.get("matches"):
        return V_REPRO_FAILED
    if (cell.get("effective_periods") or 0) < MIN_EFFECTIVE_PERIODS:
        return V_DATA_HOLD
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or t < CONDITIONAL_T_FLOOR or (c.get("increment") or 0.0) <= 0:
        return V_NO_VALUE
    e = cell.get("r64_economics") or {}
    aug = e.get("augmented") or {}
    if not e or aug.get("degenerate_under_controls"):
        return V_NOT_ECON
    inc, sh = e.get("ann_net_increment"), e.get("sharpe_increment")
    if not ((inc is not None and inc >= MATERIALITY_ANN_NET)
            or (sh is not None and sh >= MATERIALITY_SHARPE)):
        return V_NOT_ECON
    s = cell.get("stability") or {}
    if (s.get("share_blocks_positive") or 0.0) < 0.60 or c.get("lockbox_sign_agrees") is False:
        return V_UNSTABLE
    if fdr_pass is False:
        return V_NOT_FDR
    return V_ECON


# --------------------------------------------------------------------------- #
# The grid, checkpointed and resumable
# --------------------------------------------------------------------------- #
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
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except ValueError:
            continue
    return out


def run_grid(grid: list | None = None, *, verbose: bool = True, resume: bool = True) -> list:
    grid = grid or default_grid()
    cells = []
    for i, sp in enumerate(grid, 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            cells.append(json.loads(p.read_text(encoding="utf-8")))
            if verbose:
                print("[%d/%d] %s  (checkpoint)" % (i, len(grid), sp["cell_id"]), flush=True)
            continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid), sp["cell_id"]), flush=True)
        try:
            cell = measure_cell(sp)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "verdict": "ERROR", "r64_verdict": V_DATA_HOLD,
                    "error": "%s: %s" % (type(exc).__name__, exc)}
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            c, e = cell.get("conditional") or {}, cell.get("r64_economics") or {}
            print("    verdict=%s r64=%s t=%s net_inc=%s sharpe_inc=%s repro=%s %ss"
                  % (cell.get("verdict"), cell.get("r64_verdict"),
                     None if c.get("t") is None else round(c["t"], 2),
                     None if e.get("ann_net_increment") is None else round(e["ann_net_increment"], 4),
                     None if e.get("sharpe_increment") is None else round(e["sharpe_increment"], 3),
                     (cell.get("reproduction") or {}).get("matches"), cell.get("seconds")),
                  flush=True)
    return cells


# --------------------------------------------------------------------------- #
# Merge: campaign BH families, inherited R63 FDR, the ONE R64 matrix
# --------------------------------------------------------------------------- #
def base_cell_id(cell: dict) -> str | None:
    """The R63 cell a formula VARIANT belongs to: same scope, mode and horizon,
    the base CARRY dimension. A variant is the same information and inherits
    the base cell's campaign-wide FDR; it never gets a smaller family."""
    if cell.get("dimension") not in (DIM_CARRY_TO_RISK, DIM_CARRY_CLASS_NEUTRAL):
        return None
    return "%s|%s|%d|%s" % (cell.get("scope"), cell.get("mode"), int(cell.get("horizon") or 0),
                            DIM_CURVE_CARRY)


def apply_multiple_testing(cells: list) -> dict:
    """Conditional FDR is INHERITED from the R63 campaign (m = 978) for every
    cell R63 ran AND for every formula variant of such a cell (the variant is
    the same information at the same scope, mode and horizon); the R64
    campaign BH decides only a cell with no R63 base, and is reported for all
    cells for transparency. This is the protocol's rule that the R63
    campaign-wide result is quoted and never re-run on a smaller family."""
    from . import family as FAM
    bh = FAM.campaign_bh(cells)
    by_id = {c.get("cell_id"): c for c in cells}
    for c in cells:
        cid = c.get("cell_id")
        ref = c.get("r63_reference")
        r64_pass = bh["conditional"]["per_test"].get(cid)
        inherited_from = None
        if (ref is None or ref.get("fdr_pass_conditional") is None) and base_cell_id(c):
            base = by_id.get(base_cell_id(c))
            base_ref = (base or {}).get("r63_reference") or r63_reference(base_cell_id(c))
            if base_ref is not None and base_ref.get("fdr_pass_conditional") is not None:
                ref = base_ref
                inherited_from = base_cell_id(c)
        if ref is not None and ref.get("fdr_pass_conditional") is not None:
            c["fdr_pass_conditional"] = bool(ref["fdr_pass_conditional"])
            c["fdr_family"] = ("R63_CAMPAIGN_BH_m978" if inherited_from is None
                               else "R63_CAMPAIGN_BH_m978 (inherited from %s)" % inherited_from)
        elif r64_pass is not None:
            c["fdr_pass_conditional"] = bool(r64_pass)
            c["fdr_family"] = "R64_CAMPAIGN_BH_m%d" % bh["conditional"]["m"]
        else:
            c["fdr_pass_conditional"] = None
            c["fdr_family"] = None
        c["fdr_pass_conditional_r64_campaign"] = r64_pass
        c["fdr_pass_economic_r64_campaign"] = bh["economic"]["per_test"].get(cid)
        if c.get("conditional"):
            c["r64_verdict"] = verdict(c, fdr_pass=c["fdr_pass_conditional"])
    return bh


def summarise(cells: list) -> dict:
    by_verdict: dict = {}
    by_tag: dict = {}
    for c in cells:
        by_verdict[c.get("r64_verdict")] = by_verdict.get(c.get("r64_verdict"), 0) + 1
        by_tag.setdefault(c.get("r64_tag"), {})
        by_tag[c.get("r64_tag")][c.get("r64_verdict")] = \
            by_tag[c.get("r64_tag")].get(c.get("r64_verdict"), 0) + 1
    repro = [c for c in cells if c.get("reproduction") is not None]
    return {"n_cells": len(cells), "by_verdict": by_verdict, "by_tag": by_tag,
            "n_reproduced": len(repro),
            "n_reproduction_matches": sum(1 for c in repro if c["reproduction"].get("matches")),
            "reproduction_failures": sorted(c["cell_id"] for c in repro
                                            if not c["reproduction"].get("matches"))}


def _brief(c: dict) -> dict:
    co, e, r = c.get("conditional") or {}, c.get("r64_economics") or {}, c.get("r63_reference") or {}
    aug = e.get("augmented") or {}
    return {"cell_id": c.get("cell_id"), "tag": c.get("r64_tag"), "kind": c.get("kind"),
            "r63_verdict": c.get("verdict"), "r64_verdict": c.get("r64_verdict"),
            "conditional_t": co.get("t"), "conditional_increment": co.get("increment"),
            "lockbox_sign_agrees": co.get("lockbox_sign_agrees"),
            "fdr_pass_conditional": c.get("fdr_pass_conditional"), "fdr_family": c.get("fdr_family"),
            "r63_ann_net_increment": (r.get("economics") or {}).get("ann_net_increment"),
            "r63_max_dd_augmented": ((r.get("economics") or {}).get("augmented") or {}).get("max_dd"),
            "r64_ann_net_increment": e.get("ann_net_increment"),
            "r64_ann_net_increment_at_2x_cost": e.get("ann_net_increment_at_2x_cost"),
            "r64_sharpe_increment": e.get("sharpe_increment"), "r64_t_increment": e.get("t_increment"),
            "r64_augmented_ann_net": aug.get("ann_net"), "r64_augmented_sharpe": aug.get("sharpe"),
            "r64_augmented_max_dd": aug.get("max_dd"), "r64_augmented_ann_vol": aug.get("ann_vol"),
            "r64_augmented_turnover": aug.get("mean_oneway_turnover"),
            "r64_augmented_mean_gross_leverage": aug.get("mean_gross_leverage"),
            "r64_augmented_at_cap_share": aug.get("share_periods_leverage_at_cap"),
            "r64_degenerate": aug.get("degenerate_under_controls"),
            "effective_periods": c.get("effective_periods"),
            "reproduction_matches": (c.get("reproduction") or {}).get("matches")}


def merge(*, cells: list | None = None) -> dict:
    cells = cells if cells is not None else load_cells()
    bh = apply_multiple_testing(cells)
    summary = summarise(cells)
    body = {"schema": "r64_carry_matrix/1", "calculation_owner": CALCULATION_OWNER,
            "scorer": "alpha_agent.r63.sensitivity.run_cell (keep_predictions=True)",
            "book": "alpha_agent.r64.construction (both arms)",
            "grid": [dict(sp) for sp in default_grid()],
            "n_cells": len(cells), "summary": summary, "multiple_testing": bh,
            "fdr_rule": ("conditional FDR inherited from the R63 campaign (m = 978) for every "
                         "cell R63 ran; the R64 campaign BH decides only the carry variants"),
            "cells": cells, "brief": sorted((_brief(c) for c in cells),
                                            key=lambda b: str(b["cell_id"]))}
    write_artifact(MATRIX_ARTIFACT, body)
    return body
