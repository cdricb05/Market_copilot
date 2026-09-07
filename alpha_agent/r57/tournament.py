"""alpha_agent.r57.tournament - the equity buy-side tournament runner.

Discipline, mechanically enforced by ordering of PERSISTED artifacts:

    1. every variant of every family runs on DISCOVERY + VALIDATION
    2. the per-family selection (one variant, chosen on VALIDATION alone,
       with its neighbour-sign robustness recorded) is WRITTEN TO DISK
    3. only then does the lockbox pass run, once per family, on the selected
       variant only
    4. Benjamini-Hochberg runs across the campaign's full lockbox test list
    5. gates produce the named verdict per family

The selection artifact's timestamp precedes the lockbox artifact's, so "the
lockbox was touched once, after selection" is checkable evidence rather than a
promise.
"""
from __future__ import annotations

import numpy as np

from . import BH_Q, now_iso, protocol, read_artifact, write_artifact
from . import engine as E
from .families import EQUITY_FAMILIES

SELECTION_ARTIFACT = "equity_validation_selection.json"
LOCKBOX_ARTIFACT = "equity_lockbox_results.json"

# Pre-registered gates (mirrors the protocol; the protocol is authoritative).
MATERIALITY_ANN_EXCESS = 0.015
MIN_LOCKBOX_PERIODS = {21: 36, 5: 120}
MAX_MONTHLY_ONEWAY_TURNOVER = 0.40
HALF_FLOOR = -0.005
DD_MULTIPLE = 1.5


def run_validation_pass(panel: dict) -> dict:
    """Stage 1+2: every variant on D+V; persist one selection per family."""
    existing = read_artifact(SELECTION_ARTIFACT)
    if existing:
        return existing
    out = {"stage": "VALIDATION_SELECTION", "families": {}}
    for fam_id, spec in EQUITY_FAMILIES.items():
        rows = {}
        for vname, fn in spec["variants"].items():
            res = E.run_topn(panel, fn, spec["cadence"], spec["horizon"])
            rows[vname] = {
                "discovery": E.layer_stats(res, "D"),
                "validation": E.layer_stats(res, "V"),
            }
            print("  %s / %-14s  V ann net excess %+.4f (t %.2f, n %d)  D %+.4f"
                  % (fam_id, vname,
                     rows[vname]["validation"].get("ann_net_excess", float("nan")),
                     rows[vname]["validation"].get("t_net_excess") or float("nan"),
                     rows[vname]["validation"].get("periods", 0),
                     rows[vname]["discovery"].get("ann_net_excess", float("nan"))),
                  flush=True)
        # selection: highest VALIDATION net excess
        best = max(rows, key=lambda v: rows[v]["validation"].get("ann_net_excess",
                                                                 -9e9))
        order = spec["neighbour_order"]
        bi = order.index(best) if best in order else -1
        neighbours = [order[j] for j in (bi - 1, bi + 1)
                      if 0 <= j < len(order)] if bi >= 0 else []
        best_sign = np.sign(rows[best]["validation"].get("ann_net_excess", 0.0))
        neigh_ok = all(
            np.sign(rows[nb]["validation"].get("ann_net_excess", 0.0)) == best_sign
            for nb in neighbours) if neighbours else True
        out["families"][fam_id] = {
            "selected_variant": best,
            "validation_ann_net_excess": rows[best]["validation"].get("ann_net_excess"),
            "validation_positive": bool(
                (rows[best]["validation"].get("ann_net_excess") or 0) > 0),
            "neighbour_sign_ok": bool(neigh_ok),
            "variants_evaluated": len(rows),
            "all_variants": rows,
        }
    out["selection_completed_at"] = now_iso()
    out["lockbox_not_yet_evaluated"] = True
    write_artifact(SELECTION_ARTIFACT, out)
    return out


def _combo_fn(panel_families, selection):
    """E9: rank-average of validation-positive families' selected variants."""
    members = [(fid, EQUITY_FAMILIES[fid]["variants"][sel["selected_variant"]])
               for fid, sel in selection["families"].items()
               if sel["validation_positive"]
               and EQUITY_FAMILIES[fid]["cadence"] == 21]

    def f(panel, t):
        if not members:
            return np.full(panel["tr"].shape[0], np.nan)
        acc = np.zeros(panel["tr"].shape[0])
        cnt = np.zeros(panel["tr"].shape[0])
        for _fid, fn in members:
            s = fn(panel, t)
            fin = np.isfinite(s)
            if fin.sum() < 10:
                continue
            r = np.full_like(s, np.nan)
            order = np.argsort(np.argsort(s[fin]))
            r[fin] = order / max(1, fin.sum() - 1)
            acc[fin] += r[fin]
            cnt[fin] += 1
        with np.errstate(invalid="ignore", divide="ignore"):
            out = acc / cnt
        return np.where(cnt > 0, out, np.nan)
    f.__name__ = "combo_" + "_".join(fid for fid, _ in members)
    return f, [fid for fid, _ in members]


def run_lockbox_pass(panel: dict) -> dict:
    """Stage 3+4+5: selected variant per family, once, then BH and gates."""
    existing = read_artifact(LOCKBOX_ARTIFACT)
    if existing:
        return existing
    selection = read_artifact(SELECTION_ARTIFACT)
    assert selection, "validation selection must exist before the lockbox runs"

    tests = {}
    results = {}
    for fam_id, sel in selection["families"].items():
        spec = EQUITY_FAMILIES[fam_id]
        fn = spec["variants"][sel["selected_variant"]]
        res = E.run_topn(panel, fn, spec["cadence"], spec["horizon"])
        L = E.layer_stats(res, "L")
        results[fam_id] = {"selected_variant": sel["selected_variant"],
                           "lockbox": L,
                           "validation_ann_net_excess": sel["validation_ann_net_excess"],
                           "neighbour_sign_ok": sel["neighbour_sign_ok"],
                           "sector_check": _sector_check(panel, fn, spec)}
        tests[fam_id] = L.get("p_one_sided")
        print("LOCKBOX %s / %s  ann net excess %+.4f (t %.2f, n %d)"
              % (fam_id, sel["selected_variant"],
                 L.get("ann_net_excess", float("nan")),
                 L.get("t_net_excess") or float("nan"), L.get("periods", 0)),
              flush=True)

    # E9 combo, built only from validation-positive families
    combo_fn, combo_members = _combo_fn(EQUITY_FAMILIES, selection)
    if combo_members:
        res = E.run_topn(panel, combo_fn, 21, 21)
        L = E.layer_stats(res, "L")
        results["E9_COMBO"] = {"selected_variant": combo_fn.__name__,
                               "members": combo_members, "lockbox": L,
                               "validation_ann_net_excess": None,
                               "neighbour_sign_ok": True,
                               "sector_check": _sector_check(panel, combo_fn,
                                                             {"cadence": 21,
                                                              "horizon": 21})}
        tests["E9_COMBO"] = L.get("p_one_sided")
        print("LOCKBOX E9_COMBO(%s)  ann net excess %+.4f (t %.2f)"
              % (",".join(combo_members), L.get("ann_net_excess", float("nan")),
                 L.get("t_net_excess") or float("nan")), flush=True)

    out = {"stage": "LOCKBOX", "results": results,
           "bh_denominator_equity": len(tests),
           "p_values": tests,
           "lockbox_evaluated_at": now_iso(),
           "selection_completed_at": selection.get("selection_completed_at")}
    write_artifact(LOCKBOX_ARTIFACT, out)
    return out


def _sector_check(panel: dict, fn, spec) -> dict:
    """Robustness: drop the largest GICS sector at each decision date."""
    sec = panel["sectors"]

    def masked(pnl, t):
        s = fn(pnl, t)
        fin = np.isfinite(s)
        if fin.sum() == 0:
            return s
        secs, cnts = np.unique(sec[fin], return_counts=True)
        big = secs[np.argmax(cnts)]
        return np.where(sec == big, np.nan, s)

    res = E.run_topn(panel, masked, spec["cadence"], spec["horizon"])
    L = E.layer_stats(res, "L")
    return {"ex_largest_sector_ann_net_excess": L.get("ann_net_excess"),
            "sign_unchanged": None}      # resolved by the verdict pass


def verdicts(lockbox: dict, futures_pvals: dict) -> dict:
    """BH across equity + futures lockbox tests, then the named gates."""
    all_p = dict(lockbox["p_values"])
    all_p.update(futures_pvals)
    bh = E.bh_fdr(all_p, BH_Q)
    out = {}
    for fam_id, r in lockbox["results"].items():
        L = r["lockbox"]
        cadence = EQUITY_FAMILIES.get(fam_id, {"cadence": 21})["cadence"]
        floor = MIN_LOCKBOX_PERIODS[cadence]
        gates = {
            "effective_observations": (L.get("periods", 0) >= floor),
            "bh_fdr_q10": bool(bh.get(fam_id, False)),
            "materiality_1p5pct": (L.get("ann_net_excess") or -9) >= MATERIALITY_ANN_EXCESS,
            "validation_lockbox_same_sign": (
                np.sign(r.get("validation_ann_net_excess") or 0)
                == np.sign(L.get("ann_net_excess") or 0)
                if r.get("validation_ann_net_excess") is not None else
                (L.get("ann_net_excess") or 0) > 0),
            "halves_floor": all(h >= HALF_FLOOR
                                for h in (L.get("halves_ann_net_excess") or [-1])),
            "neighbour_sign": bool(r.get("neighbour_sign_ok")),
            "ex_largest_sector_sign": (
                np.sign(r["sector_check"].get(
                    "ex_largest_sector_ann_net_excess") or 0)
                == np.sign(L.get("ann_net_excess") or 0)),
            "turnover_cap": (cadence != 21 or
                             (L.get("mean_oneway_turnover_per_period") or 1)
                             <= MAX_MONTHLY_ONEWAY_TURNOVER),
            "drawdown": ((L.get("strat_max_dd") or -1)
                         >= DD_MULTIPLE * (L.get("bench_max_dd") or -1)),
        }
        ok = all(gates.values())
        failed = sorted(k for k, v in gates.items() if not v)
        out[fam_id] = {
            "verdict": ("HISTORICAL_ALPHA_CANDIDATE" if ok
                        else "NO_ALPHA_EVIDENCE"),
            "failed_gates": failed, "gates": gates,
            "lockbox_ann_net_excess": L.get("ann_net_excess"),
            "lockbox_t": L.get("t_net_excess"),
            "lockbox_periods": L.get("periods"),
        }
    return out
