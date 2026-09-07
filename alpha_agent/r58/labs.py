"""alpha_agent.r58.labs - the labelled diagnostics that explain the verdicts.

Nothing in this module may produce an alpha verdict, be promoted, or consume
FDR budget. Each function exists because a pre-registered result needs an
explanation the campaign table cannot carry:

WITHIN-COVERAGE      the protocol's coverage-blocked-family rule, applied to the
                     families that hit it. A family that can only score 43% of
                     the universe is judged against the universe it CAN score,
                     clearly labelled, so the number is neither hidden nor
                     dressed up as a campaign result.

MOMENTUM ATTRIBUTION why did the incumbent 50/50 shape beat every
                     fundamental-only family in the lockbox? Post-hoc, stated as
                     post-hoc, and never claimable: R57 already prosecuted this
                     momentum construct as family E1 and rejected it.

COMPONENT ABLATION   Track 7: which piece of the fundamental composite actually
                     carries what, layer by layer, on the honest panel.

CALIBRATION          Track 8, whose pre-registered answer when no family clears
                     its gate is to NOT fit anything and say so.
"""
from __future__ import annotations

import numpy as np

from . import FDR_FAMILIES, read_artifact, write_artifact
from . import engine as E
from . import families as F
from .tournament import LOCKBOX_ARTIFACT, VALIDATION_ARTIFACT, _build_fn

ARTIFACT = "r58_labs.json"


# --------------------------------------------------------------------------- #
def within_coverage(pf, regime, fam_ids) -> dict:
    """Judge a coverage-blocked family against the universe it CAN score."""
    reg = F.registry()
    sel = read_artifact(VALIDATION_ARTIFACT)
    out = {}
    for fam_id in fam_ids:
        variant = sel["selection"][fam_id]["selected_variant"]
        fn, band = _build_fn(fam_id, variant, reg, regime)
        res = E.run_topn(pf, fn, hold_band=band, benchmark_scope="scored")
        row = {"label": reg[fam_id]["label"], "selected_variant": variant,
               "benchmark": "equal weight of the names this family can score, "
                            "same cadence, same costs",
               "status": "WITHIN_COVERAGE_DIAGNOSTIC_NOT_AN_ALPHA_VERDICT"}
        for lay in ("D", "V", "L"):
            s = E.layer_stats(res, lay)
            row[lay] = {k: s.get(k) for k in
                        ("periods", "ann_net_excess", "t_net_excess",
                         "ann_strat_net", "ann_bench_net", "halves_ann_net_excess",
                         "mean_oneway_turnover_per_period", "mean_scored",
                         "mean_eligible", "buy_side_ann_excess",
                         "sell_side_ann_excess", "mean_rank_ic",
                         "strat_max_dd", "bench_max_dd")}
            row[lay + "_buy_sell"] = E.buy_sell_t(res, lay)
        out[fam_id] = row
    return out


# --------------------------------------------------------------------------- #
def momentum_attribution(pf) -> dict:
    """Post-hoc: what does the momentum leg alone do on the R58 universe?

    Declared post-hoc because it was run AFTER the lockbox table, to explain why
    the incumbent 50/50 shape out-earned every fundamental-only family there.
    R57 prosecuted this construct as family E1 and rejected it (validation
    -0.19%/yr, lockbox +9.27%/yr, sign-flip); nothing here can change that.
    """
    def mom_only(p, j, elig):
        return E.xs_z(F.momentum(p, j), elig)

    res = E.run_topn(pf, mom_only)
    out = {"status": "POST_HOC_DIAGNOSTIC_NOT_AN_ALPHA_CLAIM",
           "construct": "126-session total return skipping the last 21 sessions",
           "already_prosecuted_as": "R57 family E1 (rejected: sign-flip, BH, drawdown)"}
    for lay in ("D", "V", "L"):
        s = E.layer_stats(res, lay)
        out[lay] = {k: s.get(k) for k in
                    ("periods", "ann_net_excess", "t_net_excess",
                     "ann_strat_net", "ann_bench_net", "halves_ann_net_excess",
                     "mean_oneway_turnover_per_period", "buy_side_ann_excess",
                     "sell_side_ann_excess", "mean_rank_ic")}
    return out


# --------------------------------------------------------------------------- #
def component_ablation(pf) -> dict:
    """Track 7: what each piece of the fundamental composite contributes."""
    variants = {
        "fcf_only": F.a2_fcf("level"),
        "accruals_only": F.a3_accruals("level"),
        "composite_1_1": F.a1_composite(1.0, 1.0),
        "composite_2_1_fcf_heavy": F.a1_composite(2.0, 1.0),
        "composite_1_2_accrual_heavy": F.a1_composite(1.0, 2.0),
    }
    out = {"note": "same universe, same judge, same costs; the only difference "
                   "is which piece of the construct is used"}
    for name, fn in variants.items():
        res = E.run_topn(pf, fn)
        out[name] = {}
        for lay in ("D", "V", "L"):
            s = E.layer_stats(res, lay)
            out[name][lay] = {
                "ann_net_excess": s.get("ann_net_excess"),
                "t": s.get("t_net_excess"),
                "buy_side_ann_excess": s.get("buy_side_ann_excess"),
                "sell_side_ann_excess": s.get("sell_side_ann_excess"),
                "mean_rank_ic": s.get("mean_rank_ic"),
                "periods": s.get("periods"),
            }
    return out


# --------------------------------------------------------------------------- #
def coverage_blocked_robustness(pf, regime, fam_ids) -> dict:
    """Put a coverage-blocked family's diagnostic through the SAME robustness
    checks the campaign gates would have applied, plus a sector-composition
    census.

    A signal whose universe is defined by "reports R&D" is a universe of
    technology and pharmaceutical companies, and 2023-2026 was an extraordinary
    period for exactly those. If the excess dies when the largest sector is
    removed at each decision date, it is a sector bet wearing a factor's name.
    """
    reg = F.registry()
    sel = read_artifact(VALIDATION_ARTIFACT)
    sectors = pf["price"]["sectors"]
    out = {}
    for fam_id in fam_ids:
        variant = sel["selection"][fam_id]["selected_variant"]
        fn, band = _build_fn(fam_id, variant, reg, regime)

        def ex_sector(p, j, elig, _fn=fn):
            s = _fn(p, j, elig)
            scored = elig & np.isfinite(s)
            if scored.sum() < 50:
                return s
            vals, counts = np.unique(sectors[scored], return_counts=True)
            biggest = vals[int(np.argmax(counts))]
            return np.where(sectors == biggest, np.nan, s)

        res_full = E.run_topn(pf, fn, hold_band=band, benchmark_scope="scored")
        res_ex = E.run_topn(pf, ex_sector, hold_band=band, benchmark_scope="scored")

        # sector census of the held book in the lockbox
        census = {}
        held_counts = {}
        layers = E.layer_of(pf["dec_dates"])
        for j in range(len(pf["dec"])):
            if layers[j] != "L":
                continue
            elig = E.eligibility(pf, j)
            s = fn(pf, j, elig)
            scored = elig & np.isfinite(s)
            if scored.sum() < 50:
                continue
            ix = np.where(scored)[0]
            top = ix[np.argsort(-s[ix])[:50]]
            for sec in sectors[top]:
                held_counts[sec] = held_counts.get(sec, 0) + 1
        tot = sum(held_counts.values()) or 1
        census = {k: round(v / tot, 4) for k, v in
                  sorted(held_counts.items(), key=lambda kv: -kv[1])}

        row = {"selected_variant": variant,
               "status": "WITHIN_COVERAGE_DIAGNOSTIC_NOT_AN_ALPHA_VERDICT",
               "lockbox_top50_sector_shares": census}
        for lay in ("D", "V", "L"):
            a, b = E.layer_stats(res_full, lay), E.layer_stats(res_ex, lay)
            row[lay] = {
                "ann_net_excess": a.get("ann_net_excess"),
                "t": a.get("t_net_excess"),
                "ann_net_excess_ex_largest_sector": b.get("ann_net_excess"),
                "t_ex_largest_sector": b.get("t_net_excess"),
                "sign_survives_sector_exclusion": (
                    a.get("ann_net_excess") is not None
                    and b.get("ann_net_excess") is not None
                    and np.sign(a["ann_net_excess"]) == np.sign(b["ann_net_excess"])),
                "halves": a.get("halves_ann_net_excess"),
                "turnover": a.get("mean_oneway_turnover_per_period"),
                "max_dd": a.get("strat_max_dd"),
                "bench_max_dd": a.get("bench_max_dd"),
            }
        signs = [row[l]["ann_net_excess"] for l in ("D", "V", "L")]
        row["sign_consistent_all_three_layers"] = bool(
            all(x is not None for x in signs)
            and len({int(np.sign(x)) for x in signs}) == 1)
        out[fam_id] = row
    return out


# --------------------------------------------------------------------------- #
def calibration(verdicts: dict) -> dict:
    """Track 8. The pre-registered answer when nothing clears its gate."""
    survivors = [k for k, v in verdicts["campaign_verdicts"].items()
                 if v.get("verdict") == "HISTORICAL_ALPHA_CANDIDATE"]
    if survivors:
        return {"status": "SURVIVORS_PRESENT", "survivors": survivors,
                "next": "fit monotonic bins / isotonic / linear shrinkage and "
                        "compare lockbox MAE against a ZERO forecast"}
    return {
        "status": "CALIBRATION_NOT_ATTEMPTED_NO_QUALIFIED_SIGNAL",
        "rule": "the protocol permits score->expected-return calibration ONLY "
                "for a family that has already cleared its historical OOS gate. "
                "Zero families cleared, so nothing was fitted.",
        "expected_return_state": "NOT_CALIBRATED",
        "why_not_repeated": "R57 already MEASURED that calibration cannot repair "
                            "unstable ordering (lockbox MAE 0.00282 against "
                            "0.00276 for a zero forecast, Kendall tau -0.47). "
                            "Refitting a calibrator on another set of rejected "
                            "signals would consume the sample and learn nothing.",
    }


# --------------------------------------------------------------------------- #
def buy_sell_table(lockbox: dict) -> dict:
    """Track 6: BUY and SELL sides reported separately, never blended."""
    rows = {}
    for fam_id, r in lockbox["families"].items():
        bs = r.get("buy_sell_L") or {}
        rows[fam_id] = {
            "label": r["label"],
            "selected_variant": r["selected_variant"],
            "buy_side_ann_excess_L": r["L"].get("buy_side_ann_excess"),
            "buy_t_L": bs.get("buy_t"),
            "buy_hit_rate_L": bs.get("buy_hit_rate"),
            "sell_side_ann_excess_L": r["L"].get("sell_side_ann_excess"),
            "sell_skill_t_L": bs.get("sell_skill_t"),
            "sell_hit_rate_L": bs.get("sell_hit_rate"),
            "mean_rank_ic_L": bs.get("mean_rank_ic"),
            "rank_ic_t_L": bs.get("rank_ic_t"),
        }
    best_buy = max((k for k in rows if rows[k]["buy_side_ann_excess_L"] is not None),
                   key=lambda k: rows[k]["buy_side_ann_excess_L"], default=None)
    best_sell = min((k for k in rows if rows[k]["sell_side_ann_excess_L"] is not None),
                    key=lambda k: rows[k]["sell_side_ann_excess_L"], default=None)
    return {"rows": rows,
            "strongest_buy_side_lockbox": best_buy,
            "strongest_sell_side_lockbox": best_sell,
            "rule": "a family strong on the SELL side and weak on the BUY side "
                    "is NOT reported with one blended score; neither ranking is "
                    "an alpha verdict, both are descriptive of the lockbox"}


# --------------------------------------------------------------------------- #
def run(pf, regime, verdicts) -> dict:
    lb = read_artifact(LOCKBOX_ARTIFACT)
    blocked = [k for k, v in verdicts["campaign_verdicts"].items()
               if v.get("verdict") == "DATA_HOLD_COVERAGE"]
    body = {
        "track": "R58_LABS",
        "within_coverage_diagnostics": within_coverage(pf, regime, blocked),
        "coverage_blocked_robustness": coverage_blocked_robustness(pf, regime, blocked),
        "coverage_blocked_families": blocked,
        "momentum_attribution": momentum_attribution(pf),
        "component_ablation": component_ablation(pf),
        "calibration": calibration(verdicts),
        "buy_sell_table": buy_sell_table(lb),
        "fdr_family_set": list(FDR_FAMILIES),
    }
    write_artifact(ARTIFACT, body)
    return body
