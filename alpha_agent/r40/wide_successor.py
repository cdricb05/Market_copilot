"""alpha_agent.r40.wide_successor - CORRECTED_WIDE_SUCCESSOR (Track E).

The original WIDE shadow (``c39_c9233eccaa74``) is immutable and is never
touched here. This module builds a potential SUCCESSOR - a new research
object with a new candidate id, new spec hash and its own evidence clock -
whose construction removes the feature-availability defect:

1. the availability report runs FIRST (no model is evaluated before it);
2. only features admissible under the declared selection-coverage rule may
   enter the successor bundle; inadmissible families (the v1 era-patchy
   LATENT / GRAPH, and any macro column absent through selection) are
   excluded by rule, not by judgement;
3. the repaired calendar-period latent/graph features (walk-forward,
   month-grid) are admitted only if THEY pass the same rule;
4. every admitted feature is accompanied by its causal availability mask,
   so train-only median imputation can no longer silently become a
   different model when a family switches on;
5. the successor candidates are constructed and selected on Zone-A fit /
   Zone-B judgement only; Zone C is structurally unreachable (masked
   evaluation) and is NOT inspected to choose among them.

Every Zone-B evaluation is a distinct trial in the cumulative ledger.
"""
from __future__ import annotations

import numpy as np

from .. import r39 as _r39
from ..r39.continuation_director import new_cand
from ..r39.wide_prosecution import WIDE_ID, WIDE_SPEC, wide_candidate
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import availability as AV
from . import contract as C
from . import director as D

CALCULATION_OWNER = "alpha_agent.r40.wide_successor"
ARTIFACT_NAME = "corrected_wide_successor.json"
STAGE = "R40_WIDE_SUCCESSOR"

#: Declared before any evaluation: at most three successor constructions.
SUCCESSOR_DESIGNS = (
    ("WIDE_ADMISSIBLE_MASKED",
     "admissible frozen-WIDE columns + causal masks (ridge alpha=10, the "
     "frozen WIDE learner)"),
    ("WIDE_ADMISSIBLE_MASKED_REPAIRED",
     "as above + repaired calendar-grid latent/graph (if admissible) + "
     "their masks"),
    ("WIDE_ADMISSIBLE_UNMASKED",
     "admissible frozen-WIDE columns without masks - isolates the "
     "contribution of the masks"),
)


def build(d2=None, campaign_id: str = CAMPAIGN_ID) -> dict:
    d2 = d2 or D.session()
    fut = d2.state["fut"]
    frozen_feats = [c for c in d2.bundles["FUT_WIDE"] if c in fut.columns]
    report = AV.write_report(fut, frozen_features=frozen_feats,
                             campaign_id=campaign_id)
    cov = report["features"]
    admissible = [c for c in frozen_feats if cov.get(c, {}).get(
        "selection_admissible")]
    excluded = [c for c in frozen_feats if c not in admissible]
    repaired = [c for fam in AV.REPAIRED_FAMILIES.values() for c in fam
                if c in fut.columns and cov.get(c, {}).get(
                    "selection_admissible")]
    repaired_excluded = [c for fam in AV.REPAIRED_FAMILIES.values()
                         for c in fam if c in fut.columns
                         and c not in repaired]
    masks = lambda cols: [c + AV.MASK_SUFFIX for c in cols  # noqa: E731
                          if (c + AV.MASK_SUFFIX) in fut.columns
                          and fut[c + AV.MASK_SUFFIX].std() > 0]
    d2.bundles["WIDE_ADMISSIBLE_MASKED"] = admissible + masks(admissible)
    d2.bundles["WIDE_ADMISSIBLE_MASKED_REPAIRED"] = \
        admissible + repaired + masks(admissible + repaired)
    d2.bundles["WIDE_ADMISSIBLE_UNMASKED"] = list(admissible)

    results, streams = {}, {}
    for bundle, note in SUCCESSOR_DESIGNS:
        cand = new_cand("FUT", "ALL_FUT", bundle, "FUT:WIDE_SUCCESSOR",
                        "ridge", "XS_LONG_SHORT")
        rep = D.zone_b(cand, stage=STAGE, d2=d2)
        row = {"candidate_id": cand["candidate_id"],
               "spec_hash": d2.spec_hash(cand),
               "bundle": bundle, "n_features": len(d2.bundles[bundle]),
               "features": list(d2.bundles[bundle]),
               "design_note": note, "zone_b": D.summarise(rep)}
        if rep.get("state") == "OK":
            row["halves"] = D.halves_same_sign(rep)
            row["cost_2x"] = D.cost_stress(cand, d2=d2)
            streams[cand["candidate_id"]] = D.stream(rep)
        results[bundle] = row

    # the ORIGINAL WIDE re-scored under its own id (reuse count only) for
    # the paired comparison on the SAME Zone-B dates
    wide = wide_candidate()
    rep_w = D.zone_b(wide, stage=STAGE, d2=d2)
    streams[WIDE_ID] = D.stream(rep_w)
    paired = {}
    for bundle, row in results.items():
        cid = row["candidate_id"]
        if cid in streams and not streams[cid].empty:
            s, w = streams[cid], streams[WIDE_ID]
            j = s.to_frame("s").join(w.to_frame("w"), how="inner").dropna()
            diff = (j["s"] - j["w"]).to_numpy()
            from ..r34 import economics as _econ
            sig = _econ.excess_significance(j["s"].to_numpy(),
                                            j["w"].to_numpy(), horizon=21)
            paired[bundle] = {
                "n_shared_periods": int(len(j)),
                "correlation_with_original_wide": D.correlation(s, w),
                "paired_increment_annualised": sig.get("annualised_excess"),
                "paired_increment_t": sig.get("t_stat"),
                "mean_diff_per_period": float(diff.mean()) if len(diff)
                else None,
            }

    ok = [(b, r) for b, r in results.items()
          if r["zone_b"].get("state") == "OK"]
    best = max(ok, key=lambda br: br[1]["zone_b"].get(
        "after_cost_excess_t_stat") or -9.9, default=None)
    wide_b_t = (rep_w.get("after_cost_excess_t_stat")
                if rep_w.get("state") == "OK" else None)
    body = artifact_body("r40_corrected_wide_successor/1", {
        "calculation_owner": CALCULATION_OWNER,
        "original_wide": {"candidate_id": WIDE_ID, "spec": WIDE_SPEC,
                          "untouched": True,
                          "zone_b_rescored": D.summarise(rep_w)},
        "availability_rule": {"min_selection_coverage":
                              AV.MIN_SELECTION_COVERAGE,
                              "zones": list(AV.SELECTION_ZONES)},
        "frozen_wide_features": frozen_feats,
        "admissible_features": admissible,
        "excluded_by_rule": excluded,
        "excluded_reasons": {c: cov[c]["admissibility_state"]
                             for c in excluded},
        "repaired_features_admitted": repaired,
        "repaired_features_excluded": repaired_excluded,
        "availability_report_hash": report["availability_report_hash"],
        "designs": [{"bundle": b, "note": n} for b, n in SUCCESSOR_DESIGNS],
        "results": results,
        "paired_vs_original_wide_zone_b": paired,
        "best_successor": None if best is None else {
            "bundle": best[0], "candidate_id": best[1]["candidate_id"],
            "spec_hash": best[1]["spec_hash"],
            "zone_b_t": best[1]["zone_b"].get("after_cost_excess_t_stat"),
            "original_wide_zone_b_t": wide_b_t,
            "preserves_or_improves_zone_b_economics": bool(
                (best[1]["zone_b"].get("after_cost_excess_t_stat") or -9)
                >= (wide_b_t or -9))},
        "defect_removed": {
            "rule_applied_before_evaluation": True,
            "inadmissible_families_excluded": sorted({
                fam for fam, cols in AV.WIDE_FAMILIES.items()
                if any(c in excluded for c in cols)}),
            "masks_make_availability_explicit": True,
            "successor_is_new_object_with_new_hash": True,
        },
        "zone_c_inspected_for_selection": False,
        "evidence": "fit ZONE_A, judged ZONE_B, R40 ledger-counted; "
                    "Zone C untouched",
        "slot_5_eligibility_input": True,
    })
    body["successor_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    body["_streams"] = streams
    return body
