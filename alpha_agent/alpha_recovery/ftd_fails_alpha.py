"""alpha_agent.alpha_recovery.ftd_fails_alpha - the preregistered SEC
FAILS-TO-DELIVER relative-to-volume experiment.

Executes ``research/preregistration/FTD_FAILS_PREREGISTRATION.md`` (frozen
BEFORE any return existed) and changes nothing in it.

    hypothesis   a security whose outstanding settlement fails are large relative
                 to its own trading volume faces short-sale pressure beyond what
                 the borrow market clears; binding short-sale constraints leave
                 the price above value, so its subsequent return is LOWER
    direction    NEGATIVE, pre-specified. A contradicted sign closes the cell
    PIT          a fails file is used only at a session STRICTLY AFTER its
                 declared usable day (day 10 / day 25 of the following month)
    scorer       alpha_agent.r63.sensitivity.run_cell - the ONE scorer
    burden       alpha_agent.r31.multiple_testing over the family, AND over the
                 family plus the estate's prior short-interest tests

A STATE signal, not an event: no event study exists. The standalone measure is
the signed per-period rank IC on the canonical grid; the residualiser, the books
and the missingness gate are the frozen 13D/G owners, called unchanged, with the
owned US_EQUITY baseline as the control set.

RESEARCH ONLY. No purchase, no promotion, no capital allocation, no portfolio
mutation, no proposal, no order, no fill, no backfill, no live write.
"""
from __future__ import annotations

import numpy as np

from alpha_agent.r31 import multiple_testing as MT
from alpha_agent.r63 import STANDALONE_T_FLOOR
from alpha_agent.r63 import experiments as X
from alpha_agent.r63 import pit
from alpha_agent.r63 import sensitivity as S

from . import (BH_Q, CONDITIONAL_T_FLOOR, MATERIALITY_ANN_NET, MIN_EFFECTIVE_PERIODS,
               now_iso, write_artifact)
from . import control_block_alpha as CBA
from . import incumbent as INC
from . import ftd_fails_signal as FS

CALCULATION_OWNER = "alpha_agent.alpha_recovery.ftd_fails_alpha"
ARTIFACT_NAME = "ftd_fails.json"

FAMILY_ID = "SHORT_POSITIONING_FTD_FAILS_TO_VOLUME_V1"
ONTOLOGY_DIMENSION = "SHORT_POSITIONING"
BLOCK = "SEC_FTD_FAILS_TO_VOLUME"

PREREGISTRATION = "research/preregistration/FTD_FAILS_PREREGISTRATION.md"
PREREGISTRATION_COMMIT = "bc7da89"

HORIZONS_FROZEN = (21, 63)
COST_LADDER_BPS = CBA.COST_LADDER_BPS
DESK_EQUITY_COST_BPS = CBA.DESK_EQUITY_COST_BPS
MIN_DATE_COVERAGE = CBA.MIN_DATE_COVERAGE
MAX_UNCOVERED_SHARE = CBA.MAX_UNCOVERED_SHARE
PPY = CBA.PPY

V_QUALIFIED = CBA.V_QUALIFIED
V_NO_EDGE = CBA.V_NO_EDGE
V_NO_INCREMENTAL = CBA.V_NO_INCREMENTAL
V_DATA_HOLD = CBA.V_DATA_HOLD
V_NEED_MORE = CBA.V_NEED_MORE
VERDICTS = (V_QUALIFIED, V_NO_EDGE, V_NO_INCREMENTAL, V_DATA_HOLD, V_NEED_MORE)

#: The short-positioning tests the estate has already run. Both are recorded in
#: the estate's memory only (legacy Stock_Prediction_app_push research); both
#: failed. They enter the inherited burden at p = 1.
PRIOR_SHORT_POSITIONING_TESTS = (
    {"release": "PHASE_10A", "id": "polygon_short_interest_family",
     "result": "545 current tickers; best feature t 1.56, FAILED the 8-X gate",
     "source": "estate memory record phase10a-missing-alpha-data"},
    {"release": "PHASE_11BC", "id": "polygon_short_interest_change",
     "result": "weak and turnover cost-killed at 63d",
     "source": "estate memory record phase11bc-autonomous-data-acquisition-run"},
)


def declared_m() -> int:
    return len(FS.CELLS) * len(HORIZONS_FROZEN)


def inherited_m() -> int:
    return declared_m() + len(PRIOR_SHORT_POSITIONING_TESTS)


_BASE_CACHE: dict = {}


def _base_dataset(h: int) -> dict:
    if h not in _BASE_CACHE:
        _BASE_CACHE[h] = X.assemble_equity(int(h))
    return _BASE_CACHE[h]


def assemble(h: int, sig: np.ndarray) -> dict:
    base = _base_dataset(int(h))
    ds = dict(base)
    ds["blocks"] = dict(base["blocks"])
    ds["blocks"][BLOCK] = sig[:, :, None]
    return ds


def coverage_report(elig: np.ndarray, identified: np.ndarray, dec: np.ndarray) -> dict:
    """The identified share of the eligible cross-section at each decision
    session, against the frozen 95% / 20% stopping rule."""
    cov = []
    for t in np.asarray(dec, dtype=int):
        n = int(elig[:, t].sum())
        if n:
            cov.append(float((elig[:, t] & identified).sum()) / n)
    c = np.asarray(cov) if cov else np.array([1.0])
    return {"n_decision_sessions": len(cov), "coverage_mean": float(c.mean()),
            "coverage_min": float(c.min()), "n_uncovered_sessions": int((c < MIN_DATE_COVERAGE).sum()),
            "uncovered_share": float((c < MIN_DATE_COVERAGE).mean()),
            "min_date_coverage_rule": MIN_DATE_COVERAGE, "max_uncovered_share_rule": MAX_UNCOVERED_SHARE}


def ic_by_layer(desc: dict, lag: int) -> dict:
    per = desc.get("per_period") or []
    out = {}
    for lay in ("SELECTION", "LOCKBOX"):
        v = [p["rank_ic_raw"] for p in per if p.get("layer") == lay]
        out[lay] = CBA._stat(v, lag)
    return out


# --------------------------------------------------------------------------- #
# Verdict - only the preregistered gates, in the preregistered order
# --------------------------------------------------------------------------- #
def verdict_for(cell: dict, inc_cell: dict, desc: dict, coverage: dict, bias: dict, *,
                fdr_pass: bool | None, inherited_fdr_pass: bool | None) -> dict:
    """Section 10 of the preregistration, in order. The signal is already
    multiplied by the frozen sign, so a positive statistic is the predicted
    direction. Every threshold already exists in the estate."""
    if coverage.get("uncovered_share", 0.0) > MAX_UNCOVERED_SHARE:
        return {"verdict": V_DATA_HOLD, "gate": "COVERAGE",
                "why": "%.0f%% of decision sessions resolve under %.0f%% of the universe "
                       "(limit %.0f%%)" % (100 * coverage["uncovered_share"],
                                           100 * MIN_DATE_COVERAGE, 100 * MAX_UNCOVERED_SHARE)}
    if bias.get("biased"):
        return {"verdict": V_DATA_HOLD, "gate": "MISSINGNESS_BIAS",
                "why": "the names that cannot be assessed earn %.2f%%/yr differently from those "
                       "that can (floor %.1f%%)" % (100 * (bias.get("ann_diff") or 0.0),
                                                    100 * MATERIALITY_ANN_NET)}
    eff = cell.get("effective_periods")
    if cell.get("verdict") == S.V_DATA_HOLD or eff is None:
        return {"verdict": V_NEED_MORE, "gate": "SCORER_FLOOR",
                "why": "the canonical scorer returned DATA_HOLD (%s)" % cell.get("why")}
    if int(eff) < MIN_EFFECTIVE_PERIODS:
        return {"verdict": V_NEED_MORE, "gate": "EFFECTIVE_PERIODS",
                "why": "%d effective independent periods, floor %d" % (eff, MIN_EFFECTIVE_PERIODS)}
    ic = desc.get("rank_ic_raw") or {}
    mean, t_ic = ic.get("mean"), ic.get("t")
    if mean is None or float(mean) <= 0.0:
        return {"verdict": V_NO_EDGE, "gate": "FROZEN_SIGN",
                "why": "signed rank IC %s is not in the frozen NEGATIVE direction; the sign is "
                       "not reversed" % (None if mean is None else round(float(mean), 5))}
    if t_ic is None or float(t_ic) < STANDALONE_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "STANDALONE_T",
                "why": "signed rank IC t %s below the %.1f floor"
                       % (None if t_ic is None else round(float(t_ic), 2), STANDALONE_T_FLOOR)}
    c = cell.get("conditional") or {}
    t = c.get("t")
    if t is None or float(t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_EDGE, "gate": "CONDITIONAL_T",
                "why": "conditional t %s below the %.1f floor"
                       % (None if t is None else round(float(t), 2), CONDITIONAL_T_FLOOR)}
    if fdr_pass is False:
        return {"verdict": V_NO_EDGE, "gate": "MULTIPLICITY",
                "why": "does not survive BH q=%.2f over the declared m=%d" % (BH_Q, declared_m())}
    if inherited_fdr_pass is False:
        return {"verdict": V_NO_EDGE, "gate": "INHERITED_MULTIPLICITY",
                "why": "does not survive BH q=%.2f with the %d prior short-interest tests "
                       "(m=%d)" % (BH_Q, len(PRIOR_SHORT_POSITIONING_TESTS), inherited_m())}
    ic_o = (desc.get("rank_ic_orthogonalised") or {}).get("t")
    red = (cell.get("redundancy") or {}).get("redundancy")
    inc_t = ((inc_cell or {}).get("conditional") or {}).get("t")
    if red == "REDUNDANT" or ic_o is None or float(ic_o) < CONDITIONAL_T_FLOOR \
            or inc_t is None or float(inc_t) < CONDITIONAL_T_FLOOR:
        return {"verdict": V_NO_INCREMENTAL, "gate": "INCREMENTAL",
                "why": "does not survive the controls and the incumbent in the frozen direction "
                       "(redundancy=%s, orthogonalised IC t=%s, vs-incumbent t=%s)"
                       % (red, None if ic_o is None else round(float(ic_o), 2),
                          None if inc_t is None else round(float(inc_t), 2))}
    e = cell.get("economics") or {}
    if (e.get("ann_net_increment") or 0.0) < MATERIALITY_ANN_NET:
        return {"verdict": V_NO_EDGE, "gate": "MATERIALITY",
                "why": "net annual increment %.4f below the %.3f floor"
                       % (e.get("ann_net_increment") or 0.0, MATERIALITY_ANN_NET)}
    return {"verdict": V_QUALIFIED, "gate": None, "why": "every preregistered gate passed"}


def _bh(pvals: list, extra_nulls: int = 0) -> tuple:
    ps = [1.0 if p is None or not np.isfinite(float(p)) else float(p) for p in pvals]
    res = MT.benjamini_hochberg(ps + [1.0] * int(extra_nulls), BH_Q)
    rej = set(int(i) for i in res["rejected"])
    return res, [i in rej for i in range(len(ps))]


# --------------------------------------------------------------------------- #
# Runner
# --------------------------------------------------------------------------- #
def run(*, verbose: bool = True, write: bool = True) -> dict:
    E, elig = INC.equity_substrate()
    built = FS.build(E, elig, verbose=verbose)
    identified = np.asarray(built["balances"]["identified"], dtype=bool)
    ident_rows = {"FTD_CUSIP_ANCHORED": [int(i) for i in np.where(identified)[0]]}
    blocks = INC.incumbent_blocks(E, elig)
    inc_score = blocks[INC.BLOCK_SCORE][:, :, 0]
    sign = FS.CELL_SPECS[FS.CELL]["sign"]

    pvals, pkeys = [], []
    cells, cells_inc, descs, covs, biases, sigs = ({} for _ in range(6))
    for h in HORIZONS_FROZEN:
        ds0 = _base_dataset(h)
        dec = np.asarray(ds0["dec"], dtype=int)
        sig, per = FS.rank_signal(built["M"], elig, dec, sign)
        ds = assemble(h, sig)
        dims = CBA.baseline_dims(ds)
        key = (FS.CELL, h)
        if verbose:
            print("%s h=%d cadence=%d controls=%d decisions=%d ranked_sessions=%d"
                  % (FS.CELL, h, int(ds["cadence"]), len(dims), len(dec), len(per)), flush=True)
        covs[key] = coverage_report(elig, identified, dec)
        cells[key] = S.run_cell(ds, dims, BLOCK, keep_predictions=False)
        ds_i = assemble(h, sig)
        ds_i["blocks"][INC.BLOCK_SCORE] = blocks[INC.BLOCK_SCORE]
        cells_inc[key] = S.run_cell(ds_i, (INC.BLOCK_SCORE,), BLOCK, keep_predictions=False)
        descs[key] = CBA.raw_and_orthogonal(ds, sig, h=h, incumbent_score=inc_score)
        descs[key]["incremental_vs_incumbent"] = CBA.incremental_vs_incumbent(
            ds, sig, h=h, incumbent_score=inc_score)
        descs[key]["rank_ic_raw_by_layer"] = ic_by_layer(descs[key], pit.nw_lag(h, int(ds["cadence"])))
        descs[key]["controls"] = list(dims)
        biases[key] = CBA.missingness_bias(ds, ident_rows, h=h)
        sigs[key] = {"ranked_sessions": len(per),
                     "mean_ranked_names": float(np.mean([p["ranked"] for p in per])) if per else None}
        pvals.append((descs[key].get("rank_ic_raw") or {}).get("p_two_sided"))
        pkeys.append(key)

    bh, fdr_list = _bh(pvals)
    bh_inh, inh_list = _bh(pvals, extra_nulls=len(PRIOR_SHORT_POSITIONING_TESTS))
    fdr, fdr_inh = dict(zip(pkeys, fdr_list)), dict(zip(pkeys, inh_list))

    results = {}
    for key in pkeys:
        cell_name, h = key
        kw = dict(fdr_pass=fdr[key], inherited_fdr_pass=fdr_inh[key])
        v = verdict_for(cells[key], cells_inc[key], descs[key], covs[key], biases[key], **kw)
        v_merits = verdict_for(cells[key], cells_inc[key], descs[key], {"uncovered_share": 0.0},
                               {"biased": False}, **kw)
        m = (descs[key].get("rank_ic_raw") or {}).get("mean")
        results["%s|%d" % (cell_name, h)] = {
            "cell": cell_name, "horizon": h, "sign_frozen": sign,
            "verdict": v["verdict"], "gate": v["gate"], "why": v["why"],
            "verdict_if_data_gates_passed": v_merits,
            "signed_rank_ic_in_frozen_direction": None if m is None else bool(m > 0),
            "coverage": covs[key], "signal": sigs[key],
            "scorer_cell": cells[key], "scorer_cell_vs_incumbent": cells_inc[key],
            "descriptive": descs[key], "missingness_bias": biases[key],
            "fdr_pass": fdr[key], "inherited_fdr_pass": fdr_inh[key]}

    bal = built["balances"]
    body = {
        "schema": "alpha_recovery_ftd_fails/1",
        "calculation_owner": CALCULATION_OWNER, "family_id": FAMILY_ID,
        "ontology_dimension": ONTOLOGY_DIMENSION, "block": BLOCK,
        "preregistration": PREREGISTRATION, "preregistration_commit": PREREGISTRATION_COMMIT,
        "preregistration_altered_after_results": False,
        "direction": "NEGATIVE_PRESPECIFIED",
        "sign_policy": "a contradicted sign closes the cell; it is never reversed",
        "cells": {c: dict(s) for c, s in FS.CELL_SPECS.items()},
        "horizons": list(HORIZONS_FROZEN),
        "pit_boundary": "a fails file is used only at sessions strictly after day %d (first half) "
                        "or day %d (second half) of the following month; the volume denominator "
                        "covers the file's own half-month; forward return from close t+1"
                        % (FS.D.FIRST_HALF_USABLE_AFTER_DAY, FS.D.SECOND_HALF_USABLE_AFTER_DAY),
        "cost_ladder_bps_per_side": list(COST_LADDER_BPS),
        "desk_single_name_cost_bps_per_side": DESK_EQUITY_COST_BPS,
        "scorer": S.CALCULATION_OWNER, "pit_engine": pit.CALCULATION_OWNER,
        "multiplicity_owner": MT.CALCULATION_OWNER,
        "multiplicity": {"family": FAMILY_ID, "m_declared": declared_m(),
                         "benjamini_hochberg": bh, "q": BH_Q, "denominator_reset": False,
                         "cells_added_after_results": False, "p_value": "signed rank IC, two-sided",
                         "m_inherited": inherited_m(), "benjamini_hochberg_inherited": bh_inh,
                         "prior_tests": list(PRIOR_SHORT_POSITIONING_TESTS),
                         "prior_p_value_policy": "each prior test enters at p = 1"},
        "identity": {"mode": "CUSIP_ANCHORED", "rows_identified": int(identified.sum()),
                     "cusips_indexed": built["cix"]["n_cusips"]},
        "files": {"count": len(bal["files"]), "first": bal["files"][0]["period"],
                  "last": bal["files"][-1]["period"]},
        "results": results, "capital_eligible": False, "generated_at": now_iso()}
    if write:
        write_artifact(ARTIFACT_NAME, body)
    return body
