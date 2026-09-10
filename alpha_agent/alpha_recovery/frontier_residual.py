"""alpha_agent.alpha_recovery.frontier_residual - the owned information needs the
campaign left on the table.

Workstream 13. Before ``OWNED_FREE_INFORMATION_EXHAUSTED`` can be claimed, every
OWNED need the measured frontier ranks above the others has to be measured or
have a named, non-negotiable reason why it cannot be. After the first pass three
of the top six owned needs were still open:

    rank 2  US_EQUITY|1|FREE_CASH_FLOW          recorded NOT EXECUTED because it
            (remaining value 0.065)             is a baseline dimension of the
                                                incumbent - a reason to measure
                                                it, not to skip it: it asks how
                                                much of the incumbent's own
                                                fundamental leg is information.
    rank 3  VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV   DATA_HOLD: 196 covered
            (remaining value 0.054)                    rows against the R63
                                                       floor of 200, because the
                                                       scope holds ONE contract
                                                       (VX) and a 21-session
                                                       cadence yields only 250
                                                       decision slots.
    rank 6  CREDIT_PROXY|21|INFLATION_EXPECTATIONS     recorded NOT EXECUTED for
            (remaining value 0.035)                    "no credit-proxy substrate
                                                       in the R64 book" - true of
                                                       the R64 futures assembler,
                                                       false of the R63 one, which
                                                       owns the credit proxy.

THE FLOOR IS NOT MOVED
    ``sensitivity.MIN_ROWS`` stays at 200. The DATA_HOLD is answered by asking
    the SAME information in the SAME scope at a horizon whose cadence produces
    enough decision slots (5 sessions -> about 1,050 slots; 1 session -> about
    5,250), which is a distinct implementation resolving a NAMED binding
    failure, the only reopening reason that applies. Lowering the floor to reach
    196 rows would be exactly the threshold relaxation the contract forbids.

OWNERS
    The ONE scorer is ``alpha_agent.r63.sensitivity.run_cell`` and the ONE book
    is ``alpha_agent.r64.construction``. Futures-scope cells go through
    ``alpha_agent.r64.experiments.measure_cell`` verbatim. The equity and
    credit-proxy scopes have no R64 assembler (it injects futures variant
    blocks), so those cells compose the SAME two owners around the R63
    assembler instead - no second scorer, no second book, no second verdict.
"""
from __future__ import annotations

import json
import time

import numpy as np

from alpha_agent.r63 import AC_CREDIT, AC_US_EQUITY, AC_VOLATILITY, ontology as ONT, pit
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import sensitivity as S
from alpha_agent.r64 import construction as B
from alpha_agent.r64 import experiments as R64X
from alpha_agent.r64 import family as FAM

from . import BH_Q, HOLM_ALPHA, research_root, write_artifact

CALCULATION_OWNER = "alpha_agent.alpha_recovery.frontier_residual"
CELLS_DIR = "cells"
ARTIFACT_NAME = "frontier_residual.json"
FAMILY = "FRONTIER_MANDATES_RISK_CONTROLLED"

#: the named binding failure the VOLATILITY rescue resolves, quoted from the
#: first-pass cell record so it can never be re-described later
VOL_BINDING_FAILURE = ("VOLATILITY|TS|21|VOLATILITY_EXPECTATIONS_IV returned DATA_HOLD with "
                       "rows_covered=196 against sensitivity.MIN_ROWS=200 and n_instruments=1; "
                       "the floor is NOT moved, the cadence is")

CELLS = (
    {"scope": AC_US_EQUITY, "mode": "XS", "horizon": 1, "dimension": "FREE_CASH_FLOW",
     "kind": "ABLATION", "tag": "PRIMARY", "cell_key": "US_EQUITY|1|FREE_CASH_FLOW",
     "frontier_rank": 2, "why": "the largest remaining OWNED need on the frontier and the "
                                "incumbent's own fundamental leg"},
    {"scope": AC_VOLATILITY, "mode": "TS", "horizon": 5, "dimension": "VOLATILITY_EXPECTATIONS_IV",
     "kind": "AUGMENTATION", "tag": "PRIMARY", "cell_key": "VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV",
     "frontier_rank": 3, "why": "the same information and scope at a cadence that clears the row floor"},
    {"scope": AC_CREDIT, "mode": "TS", "horizon": 21, "dimension": "INFLATION_EXPECTATIONS",
     "kind": "AUGMENTATION", "tag": "PRIMARY", "cell_key": "CREDIT_PROXY|21|INFLATION_EXPECTATIONS",
     "frontier_rank": 6, "why": "the R63 assembler owns the credit proxy the R64 one does not"},
    {"scope": AC_VOLATILITY, "mode": "TS", "horizon": 1, "dimension": "VOLATILITY_EXPECTATIONS_IV",
     "kind": "AUGMENTATION", "tag": "RESCUE", "cell_key": "VOLATILITY|21|VOLATILITY_EXPECTATIONS_IV",
     "frontier_rank": 3, "why": VOL_BINDING_FAILURE,
     "reopening_reason": "DISTINCT_IMPLEMENTATION_RESOLVES_NAMED_BINDING_FAILURE",
     "binding_failure": VOL_BINDING_FAILURE},
)

_FUTURES_SCOPES = (AC_VOLATILITY,)


def grid() -> list:
    out = []
    for c in CELLS:
        sp = dict(c)
        sp["cell_id"] = "%s|%s|%d|%s" % (c["scope"], c["mode"], c["horizon"], c["dimension"])
        sp["family"] = FAMILY
        out.append(sp)
    return out


# --------------------------------------------------------------------------- #
# One cell on a scope the R64 assembler does not cover
# --------------------------------------------------------------------------- #
def measure_non_futures_cell(sp: dict) -> dict:
    """The R63 dataset, the ONE scorer, the R64 risk-controlled book on BOTH
    arms and the R64 verdict ladder - composed, never reimplemented."""
    scope, mode, h, dim = sp["scope"], sp["mode"], int(sp["horizon"]), sp["dimension"]
    ds = X.assemble(scope, mode, h)
    base = tuple(d for d in ONT.baseline_for(scope) if d in ds["blocks"])
    dims_b = tuple(d for d in base if d != dim) if sp["kind"] == "ABLATION" else base
    if dim not in ds["blocks"]:
        return {**sp, "verdict": S.V_DATA_HOLD, "r64_verdict": R64X.V_DATA_HOLD,
                "why": "dimension has no block in this scope"}
    t0 = time.time()
    cell = S.run_cell(ds, dims_b, dim, keep_predictions=True)
    preds = cell.pop("_predictions", None)
    cell.update({k: v for k, v in sp.items()})
    cell["seconds"] = round(time.time() - t0, 1)
    cell["instruments"] = list(ds["inst"])[:60]
    cell["n_instruments_dataset"] = len(ds["inst"])
    if preds is not None and cell.get("conditional"):
        lag = pit.nw_lag(h, int(ds["cadence"]))
        okp = np.isfinite(preds["pred_B"]) & np.isfinite(preds["pred_BD"])
        common = dict(y=preds["y_raw"][okp], gid=preds["gid"][okp], iid=preds["iid"][okp],
                      vol=preds["vol"][okp], cost=preds["cost"][okp], mode=mode, horizon=h,
                      class_of=None)
        book_b = B.build_book(preds["pred_B"][okp], pred_scale=preds["pred_scale"]["B"], **common)
        book_bd = B.build_book(preds["pred_BD"][okp], pred_scale=preds["pred_scale"]["BD"], **common)
        cell["r64_economics"] = B.paired_increment(book_b, book_bd, h, lag)
    cell["r64_verdict"] = R64X.verdict(cell)
    cell["n_folds"] = len(cell.get("folds") or [])
    cell.pop("folds", None)
    return R64X._jsonable(cell)


def measure_cell(sp: dict) -> dict:
    if sp["scope"] in _FUTURES_SCOPES:
        spec = R64X.spec(sp["scope"], sp["mode"], sp["horizon"], sp["dimension"], sp["kind"],
                         "FRONTIER_RESIDUAL")
        cell = R64X.measure_cell(spec)
        cell.update({k: v for k, v in sp.items() if k not in ("cell_id",)})
        cell["priced_by"] = "alpha_agent.r64.experiments.measure_cell (verbatim)"
    else:
        cell = measure_non_futures_cell(sp)
        cell["priced_by"] = ("alpha_agent.r63.sensitivity.run_cell + alpha_agent.r64.construction "
                             "(the R64 assembler covers futures scopes only)")
    cell["calculation_owner"] = CALCULATION_OWNER
    cell["family"] = FAMILY
    return cell


def _cell_path(cell_id: str):
    d = research_root() / CELLS_DIR
    d.mkdir(parents=True, exist_ok=True)
    return d / ("RESIDUAL_%s.json" % cell_id.replace("|", "_"))


def load_cells() -> list:
    d = research_root() / CELLS_DIR
    if not d.exists():
        return []
    out = []
    for p in sorted(d.glob("RESIDUAL_*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except ValueError:
            continue
    return out


def run_grid(*, verbose: bool = True, resume: bool = True) -> list:
    cells = []
    for i, sp in enumerate(grid(), 1):
        p = _cell_path(sp["cell_id"])
        if resume and p.exists():
            prior = json.loads(p.read_text(encoding="utf-8"))
            if not prior.get("error"):
                cells.append(prior)
                if verbose:
                    print("[%d/%d] %s (checkpoint)" % (i, len(grid()), sp["cell_id"]), flush=True)
                continue
        if verbose:
            print("[%d/%d] %s ..." % (i, len(grid()), sp["cell_id"]), flush=True)
        try:
            cell = measure_cell(sp)
        except Exception as exc:                                  # noqa: BLE001
            cell = {**sp, "r64_verdict": "ERROR", "error": "%s: %s" % (type(exc).__name__, exc),
                    "calculation_owner": CALCULATION_OWNER, "family": FAMILY}
        p.write_text(json.dumps(cell, indent=1, sort_keys=True, default=str), encoding="utf-8")
        cells.append(cell)
        if verbose:
            c = cell.get("conditional") or {}
            e = cell.get("r64_economics") or {}
            print("    %s cond_t=%s inc=%s rows=%s/%s folds=%s %ss" % (
                cell.get("r64_verdict"),
                None if c.get("t") is None else round(c["t"], 2),
                None if e.get("ann_net_increment") is None else round(e["ann_net_increment"], 4),
                cell.get("rows_covered"), cell.get("rows_total"), cell.get("n_folds"),
                cell.get("seconds")), flush=True)
    return cells


def brief(c: dict) -> dict:
    cond, e = c.get("conditional") or {}, c.get("r64_economics") or {}
    aug = e.get("augmented") or {}
    return {"cell_id": c.get("cell_id"), "cell_key": c.get("cell_key"), "tag": c.get("tag"),
            "frontier_rank": c.get("frontier_rank"), "r64_verdict": c.get("r64_verdict"),
            "r63_verdict": c.get("verdict"), "why_run": c.get("why"),
            "conditional_t": cond.get("t"), "conditional_increment": cond.get("increment"),
            "residual_share": (c.get("redundancy") or {}).get("residual_share"),
            "ann_net_increment": e.get("ann_net_increment"),
            "sharpe_increment": e.get("sharpe_increment"), "t_increment": e.get("t_increment"),
            "augmented_ann_net": aug.get("ann_net"), "augmented_sharpe": aug.get("sharpe"),
            "augmented_max_dd": aug.get("max_dd"),
            "rows_covered": c.get("rows_covered"), "rows_total": c.get("rows_total"),
            "n_instruments": c.get("n_instruments"), "n_folds": c.get("n_folds"),
            "effective_periods": c.get("effective_periods"),
            "fdr_pass_economic": c.get("fdr_pass_economic"), "holm_pass_family": c.get("holm_pass_family"),
            "why": c.get("why_hold") or c.get("why"), "error": c.get("error")}


def merge(*, cells: list | None = None, write: bool = True) -> dict:
    cells = cells if cells is not None else load_cells()
    scored = [c for c in cells if (c.get("r64_economics") or {}).get("p_increment_one_sided") is not None]
    p_econ = {c["cell_id"]: c["r64_economics"]["p_increment_one_sided"] for c in scored}
    bh = S.bh_fdr(p_econ, BH_Q) if p_econ else {"per_test": {}}
    holm = FAM.holm(p_econ, HOLM_ALPHA) if p_econ else {"rejected": {}}
    for c in scored:
        c["fdr_pass_economic"] = bh["per_test"].get(c["cell_id"])
        c["holm_pass_family"] = holm.get("rejected", {}).get(c["cell_id"])
        c["r64_verdict"] = R64X.verdict(c, fdr_pass=(c["fdr_pass_economic"] and c["holm_pass_family"]))
    counts: dict = {}
    for c in cells:
        counts[c.get("r64_verdict")] = counts.get(c.get("r64_verdict"), 0) + 1
    body = {"schema": "alpha_recovery_frontier_residual/1", "calculation_owner": CALCULATION_OWNER,
            "family": FAMILY,
            "question": "are the OWNED information needs the frontier still ranks highest actually "
                        "worth anything, measured rather than assumed?",
            "threshold_discipline": {"sensitivity_min_rows": S.MIN_ROWS,
                                     "moved": False,
                                     "binding_failure_answered_by": "cadence, not by a lower floor",
                                     "named_binding_failure": VOL_BINDING_FAILURE},
            "grid": grid(), "n_cells": len(cells), "counts": counts,
            "multiple_testing": {"benjamini_hochberg_economic": {k: v for k, v in bh.items()
                                                                 if k != "per_test"},
                                 "holm_within_family": {k: v for k, v in holm.items()
                                                        if k != "adjusted"}},
            "brief": sorted((brief(c) for c in cells), key=lambda b: str(b["cell_id"])),
            "cells": cells}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
