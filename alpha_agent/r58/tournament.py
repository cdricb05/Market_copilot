"""alpha_agent.r58.tournament - validation selection, then ONE untouched lockbox.

The discipline is the whole point and it runs in this order, enforced by
artifact timestamps and asserted by tests:

    1. every family's whole variant grid is evaluated on DISCOVERY + VALIDATION
    2. one variant per family is selected on VALIDATION ALONE and PERSISTED
    3. only then is the lockbox evaluated, once per family
    4. gates and Benjamini-Hochberg are applied with the denominator the
       protocol fixed in advance (13 FDR-counted families)

Nothing selects on the lockbox. Nothing re-selects after seeing it.
"""
from __future__ import annotations

import numpy as np

from . import (BH_Q, FDR_FAMILIES, GATE_DD_MULTIPLE, GATE_HALF_FLOOR,
               GATE_MATERIALITY, GATE_MAX_TURNOVER, MIN_SCORED_DATE_FRACTION,
               MIN_SCORED_FRACTION, OBS_FLOOR, PRIOR_SEARCH_BURDEN, now_iso,
               read_artifact, write_artifact)
from . import engine as E
from . import families as F

VALIDATION_ARTIFACT = "r58_validation_selection.json"
LOCKBOX_ARTIFACT = "r58_lockbox_results.json"
VERDICT_ARTIFACT = "r58_campaign_verdicts.json"


# --------------------------------------------------------------------------- #
def _build_fn(fam_id, variant, reg, regime):
    """Resolve one (family, variant) into a score function and a hold band."""
    spec = reg[fam_id]
    if spec.get("needs_regime"):
        return F.b4_regime(variant, regime), 0
    fn = spec["grid"][variant]
    band = (spec.get("hold_band") or {}).get(variant, 0)
    return fn, band


def _sector_excluded_fn(fn, pf):
    """The same score with the largest GICS sector removed at each decision date.

    GICS history is not point-in-time here (the current classification is used),
    which the protocol discloses; the check is still the standard test of whether
    one sector is carrying the whole result.
    """
    sectors = pf["price"]["sectors"]

    def wrapped(p, j, elig):
        s = fn(p, j, elig)
        scored = elig & np.isfinite(s)
        if scored.sum() < 50:
            return s
        vals, counts = np.unique(sectors[scored], return_counts=True)
        biggest = vals[int(np.argmax(counts))]
        return np.where(sectors == biggest, np.nan, s)
    return wrapped


# --------------------------------------------------------------------------- #
def run_validation(pf, regime, progress=True) -> dict:
    """Evaluate every variant of every family on DISCOVERY + VALIDATION only."""
    reg = F.registry()
    out = {"families": {}, "selection": {}, "selected_at": now_iso(),
           "layers_evaluated": ["D", "V"],
           "lockbox_untouched_at_selection_time": True}
    for fam_id in list(FDR_FAMILIES) + list(("B0",)):
        spec = reg[fam_id]
        rows = {}
        for variant in spec["grid"]:
            fn, band = _build_fn(fam_id, variant, reg, regime)
            res = E.run_topn(pf, fn, hold_band=band)
            rows[variant] = {
                "D": E.layer_stats(res, "D"),
                "V": E.layer_stats(res, "V"),
                "buy_sell_V": E.buy_sell_t(res, "V"),
            }
            if progress:
                v = rows[variant]["V"]
                print("  %-3s %-16s V ann_net_excess %+7.4f  t %6.2f  n %d"
                      % (fam_id, variant, v.get("ann_net_excess", float("nan")),
                         v.get("t_net_excess") or float("nan"),
                         v.get("periods", 0)), flush=True)
        # selection: best VALIDATION annualised net excess. Mechanical, no judgement.
        best = max(rows, key=lambda k: (rows[k]["V"].get("ann_net_excess")
                                        if rows[k]["V"].get("ann_net_excess")
                                        is not None else -1e9))
        neigh_signs = [np.sign(rows[k]["V"].get("ann_net_excess") or 0.0)
                       for k in rows if k != best]
        out["families"][fam_id] = {"label": spec["label"], "group": spec["group"],
                                   "variants": rows}
        out["selection"][fam_id] = {
            "selected_variant": best,
            "selection_rule": "highest VALIDATION annualised net excess; the "
                              "whole grid is evaluated, the lockbox is not",
            "validation_ann_net_excess": rows[best]["V"].get("ann_net_excess"),
            "neighbour_validation_signs": [float(s) for s in neigh_signs],
            "neighbours_share_sign": bool(
                all(s == np.sign(rows[best]["V"].get("ann_net_excess") or 0.0)
                    for s in neigh_signs)),
        }
    write_artifact(VALIDATION_ARTIFACT, out)
    return out


# --------------------------------------------------------------------------- #
def run_lockbox(pf, regime, selection=None, progress=True) -> dict:
    """Evaluate the SELECTED variant of each family on the untouched lockbox."""
    sel = selection or read_artifact(VALIDATION_ARTIFACT)
    if sel is None:
        raise RuntimeError("validation selection must be persisted before the "
                           "lockbox is evaluated")
    reg = F.registry()
    out = {"families": {}, "evaluated_at": now_iso(),
           "selection_artifact_generated_at": sel.get("generated_at"),
           "selection_read_from": VALIDATION_ARTIFACT}
    for fam_id, s in sel["selection"].items():
        variant = s["selected_variant"]
        fn, band = _build_fn(fam_id, variant, reg, regime)
        res = E.run_topn(pf, fn, hold_band=band)
        L = E.layer_stats(res, "L")
        row = {
            "label": reg[fam_id]["label"], "group": reg[fam_id]["group"],
            "selected_variant": variant,
            "V": sel["families"][fam_id]["variants"][variant]["V"],
            "L": L,
            "buy_sell_L": E.buy_sell_t(res, "L"),
            "neighbours_share_validation_sign": s["neighbours_share_sign"],
        }
        # sector-exclusion robustness, same selected variant
        res_sx = E.run_topn(pf, _sector_excluded_fn(fn, pf), hold_band=band)
        row["L_ex_largest_sector_ann_net_excess"] = \
            E.layer_stats(res_sx, "L").get("ann_net_excess")
        # coverage stability over the lockbox
        lay = res["layers"] == "L"
        frac = np.where(res["n_eligible"][lay] > 0,
                        res["n_scored"][lay] / np.maximum(res["n_eligible"][lay], 1),
                        0.0)
        row["lockbox_dates_meeting_coverage"] = float((frac >= MIN_SCORED_FRACTION).mean())
        row["lockbox_median_scored_fraction"] = float(np.median(frac)) if len(frac) else 0.0
        out["families"][fam_id] = row
        if progress:
            print("  %-3s %-16s L ann_net_excess %+7.4f  t %6.2f  n %d  cov %.2f"
                  % (fam_id, variant, L.get("ann_net_excess", float("nan")),
                     L.get("t_net_excess") or float("nan"), L.get("periods", 0),
                     row["lockbox_median_scored_fraction"]), flush=True)
    write_artifact(LOCKBOX_ARTIFACT, out)
    return out


# --------------------------------------------------------------------------- #
def verdicts(lockbox=None) -> dict:
    """Apply the pre-registered gates, then Benjamini-Hochberg across the 13."""
    lb = lockbox or read_artifact(LOCKBOX_ARTIFACT)
    fams = lb["families"]

    pvals = {k: (fams[k]["L"].get("p_one_sided")) for k in FDR_FAMILIES
             if k in fams}
    bh = E.bh_fdr(pvals, BH_Q)

    out = {"campaign_verdicts": {}, "bh_q": BH_Q,
           "bh_denominator": len(FDR_FAMILIES),
           "bh_denominator_fixed_before_lockbox": True,
           "prior_search_burden_disclosed": PRIOR_SEARCH_BURDEN,
           "obs_floor": OBS_FLOOR}
    for fam_id, row in fams.items():
        L, V = row["L"], row["V"]
        failed = []
        n = L.get("periods", 0)
        diagnostic = fam_id in ("B0",)

        coverage_ok = (row.get("lockbox_dates_meeting_coverage", 0.0)
                       >= MIN_SCORED_DATE_FRACTION)
        if not coverage_ok:
            verdict = "DATA_HOLD_COVERAGE"
            out["campaign_verdicts"][fam_id] = {
                "label": row["label"], "group": row["group"],
                "selected_variant": row["selected_variant"],
                "verdict": verdict,
                "reason": ("scores only %.1f%% of the eligible universe at the "
                           "median lockbox date; the pre-registered coverage "
                           "gate requires >=%.0f%% on >=%.0f%% of dates"
                           % (100 * row.get("lockbox_median_scored_fraction", 0),
                              100 * MIN_SCORED_FRACTION,
                              100 * MIN_SCORED_DATE_FRACTION)),
                "lockbox_ann_net_excess": L.get("ann_net_excess"),
                "lockbox_periods": n,
                "no_alpha_verdict_issued": True,
                "diagnostic_only": True,
            }
            continue

        if n < OBS_FLOOR:
            failed.append("INSUFFICIENT_SAMPLE")
        if (L.get("ann_net_excess") or -1) < GATE_MATERIALITY:
            failed.append("MATERIALITY")
        vs, ls = V.get("ann_net_excess"), L.get("ann_net_excess")
        if vs is None or ls is None or np.sign(vs) != np.sign(ls):
            failed.append("SIGN_FLIP_VALIDATION_TO_LOCKBOX")
        halves = L.get("halves_ann_net_excess")
        if not halves or min(halves) < GATE_HALF_FLOOR:
            failed.append("LOCKBOX_HALVES")
        if not row.get("neighbours_share_validation_sign"):
            failed.append("GRID_NEIGHBOUR_SIGN")
        sx = row.get("L_ex_largest_sector_ann_net_excess")
        if sx is None or (ls is not None and np.sign(sx) != np.sign(ls)):
            failed.append("SECTOR_CONCENTRATION")
        if (L.get("mean_oneway_turnover_per_period") or 1.0) > GATE_MAX_TURNOVER:
            failed.append("TURNOVER")
        sdd, bdd = L.get("strat_max_dd"), L.get("bench_max_dd")
        if sdd is not None and bdd is not None and sdd < GATE_DD_MULTIPLE * bdd:
            failed.append("DRAWDOWN")
        if not diagnostic and not bh.get(fam_id, False):
            failed.append("BENJAMINI_HOCHBERG")

        if diagnostic:
            verdict = "DIAGNOSTIC_REFERENCE_NOT_AN_ALPHA_CLAIM"
        else:
            verdict = ("HISTORICAL_ALPHA_CANDIDATE" if not failed
                       else "NO_ALPHA_EVIDENCE")
        out["campaign_verdicts"][fam_id] = {
            "label": row["label"], "group": row["group"],
            "selected_variant": row["selected_variant"],
            "verdict": verdict, "failed_gates": failed,
            "validation_ann_net_excess": vs,
            "lockbox_ann_net_excess": ls,
            "lockbox_t": L.get("t_net_excess"),
            "lockbox_p_one_sided": L.get("p_one_sided"),
            "lockbox_periods": n,
            "lockbox_halves": halves,
            "lockbox_turnover_oneway_per_month": L.get("mean_oneway_turnover_per_period"),
            "lockbox_max_dd": sdd, "bench_max_dd": bdd,
            "lockbox_ex_largest_sector": sx,
            "buy_side_ann_excess_L": L.get("buy_side_ann_excess"),
            "sell_side_ann_excess_L": L.get("sell_side_ann_excess"),
            "buy_t_L": row["buy_sell_L"].get("buy_t"),
            "sell_skill_t_L": row["buy_sell_L"].get("sell_skill_t"),
            "mean_rank_ic_L": row["buy_sell_L"].get("mean_rank_ic"),
            "bh_survived": bool(bh.get(fam_id, False)) if not diagnostic else None,
            "median_scored_fraction_L": row.get("lockbox_median_scored_fraction"),
        }
    out["n_historical_alpha_candidates"] = sum(
        1 for v in out["campaign_verdicts"].values()
        if v.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE")
    write_artifact(VERDICT_ARTIFACT, out)
    return out
