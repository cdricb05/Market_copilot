"""alpha_agent.r40.compute_escalation - R40_COMPUTE_ESCALATION_REQUEST
(Track M).

$0 local resources were used first; every branch that could run on this
workstation ran. The branches below are blocked ONLY by compute, and each
request records the exact experiment, model, why it is scientifically
distinct, the local blocker, the hardware needed, the estimated hours and
cost, the research decision it would unlock, and the evidence already
obtained cheaply that justifies (or fails to justify) the spend. Nothing
here spends money; ``MAY_PURCHASE_COMPUTE`` stays False.
"""
from __future__ import annotations

from .. import r39 as _r39
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.compute_escalation"
ARTIFACT_NAME = "R40_COMPUTE_ESCALATION_REQUEST.json"

LOCAL = {"gpu": "NVIDIA GTX 1650, 4 GB VRAM (below every training "
                "footprint below)",
         "cpu": "desktop CPU, torch 2.13 CPU-only",
         "ram": "16+ GB", "disk": "C: ~5 GB free (unusable for weights); "
                                  "D: ~700 GB free"}


def build(campaign_id: str = CAMPAIGN_ID) -> dict:
    models = _r39.read_json(campaign_dir(campaign_id)
                            / "r40_model_results.json") or {}
    table = {r["key"]: r for r in (models.get("comparison") or [])}
    tcn_t = (models.get("baselines") or {}).get("tcn_zone_b_t")
    cheap = {"tcn_zone_b_t": tcn_t,
             "ssm_lite_zone_b_t": (table.get("ssm_lite_seq_xs") or {}).get(
                 "zone_b_t"),
             "patchtst_lite_zone_b_t": (table.get("patchtst_lite_seq_xs")
                                        or {}).get("zone_b_t"),
             "tabpfn_v2_zone_b_t": (table.get("tabpfn_v2_classical_xs")
                                    or {}).get("zone_b_t"),
             "chronos_best_zone_b_t": max((v.get("zone_b_t") or -9
                                          for k, v in table.items()
                                          if k.startswith("chronos")),
                                         default=None)}
    requests = [
        {
            "request_id": "R40_CE_01_FULL_CONTEXT_TABPFN",
            "exact_experiment": "TabPFN-v2 regressor with the FULL Zone-A "
                                "context (13,858 rows) and n_estimators=8 "
                                "feature-permutation ensembling, XS book on "
                                "ALL_FUT, fit Zone A / judge Zone B",
            "exact_model": "Prior-Labs/TabPFN-v2-reg "
                           "tabpfn-v2-regressor.ckpt (sha256 recorded in "
                           "model_weight_provenance.json)",
            "scientifically_distinct_because": "the only foundation model "
                                               "whose pretraining is "
                                               "provably free of the target "
                                               "series (synthetic prior)",
            "local_blocker": "CPU attention over a 13.9k context is ~1 s/row "
                             "per estimator; 7.5k Zone-B rows x 8 estimators "
                             "~ 17 CPU-hours and the 4 GB GPU cannot hold "
                             "the context",
            "required_vram_gb": 16, "gpu_class": "NVIDIA L4 / A10 / T4-16GB",
            "estimated_hours": 1.5, "estimated_cost_usd": "1-3",
            "expected_research_decision_unlocked":
                "whether the clean-prior tabular foundation model's Zone-B "
                "t of %.2f at a 5k context is a context-size artefact or a "
                "model-class result" % (cheap["tabpfn_v2_zone_b_t"] or 0.0),
            "evidence_already_obtained_cheaply": cheap,
            "justified": bool((cheap["tabpfn_v2_zone_b_t"] or 0) >= 1.0),
            "justification_note": "NOT justified by the cheap evidence: the "
                                  "5k-context result is indistinguishable "
                                  "from zero; a 3x context rarely turns "
                                  "t 0.15 into t 3",
        },
        {
            "request_id": "R40_CE_02_DEEP_SEQUENCE_SCALE",
            "exact_experiment": "the R39 TCN and the R40 SSM-lite / "
                                "PatchTST-lite at 4-8x width and 36-step "
                                "windows with 10-seed ensembles, same "
                                "protocol, <= 3 configs each (hierarchical "
                                "screen on Zone A)",
            "exact_model": "from-scratch torch nets (no pretrained weights)",
            "scientifically_distinct_because": "tests whether the "
                                               "model-capability frontier "
                                               "(TCN t 2.07 > ridge 1.62) "
                                               "is capacity-limited",
            "local_blocker": "CPU training at that width/window is "
                             "~6-10 hours per config; the GTX 1650 lacks "
                             "memory for the batch",
            "required_vram_gb": 16, "gpu_class": "NVIDIA L4 / A10",
            "estimated_hours": 6, "estimated_cost_usd": "5-12",
            "expected_research_decision_unlocked":
                "whether a larger sequence model clears the declared "
                "Zone-C pre-gate (t >= 3.0) - the only way a deep model "
                "can become a Zone-C finalist",
            "evidence_already_obtained_cheaply": cheap,
            "justified": bool((tcn_t or 0) >= 2.0),
            "justification_note": "weakly justified: three small sequence "
                                  "architectures cluster at t 1.8-2.1 above "
                                  "every linear/boosted baseline; scale is "
                                  "the unexecuted axis",
        },
        {
            "request_id": "R40_CE_03_CHART_VISION",
            "exact_experiment": "render ~30k monthly chart images of each "
                                "market's trailing 252 sessions, encode with "
                                "CLIP ViT-B/32, ridge head, XS book, same "
                                "protocol; PRETRAINING_UNKNOWN label",
            "exact_model": "openai/clip-vit-base-patch32",
            "scientifically_distinct_because": "the pretrained-visual-prior "
                                               "hypothesis cannot be tested "
                                               "numerically",
            "local_blocker": "rendering + encoding 30k images on CPU is "
                             "~12-20 hours; evidence could never be clean",
            "required_vram_gb": 8, "gpu_class": "any 8 GB CUDA GPU",
            "estimated_hours": 2, "estimated_cost_usd": "2-5",
            "expected_research_decision_unlocked":
                "closes the VISUAL_CHART representation family one way or "
                "the other (as MODEL_CAPABILITY_RESEARCH only)",
            "evidence_already_obtained_cheaply": {
                "market_structure_family_zone_b_max_t": "< 1.0 (R39)",
                "fibonacci_vs_placebo": "indistinguishable (R39)"},
            "justified": False,
            "justification_note": "not justified: every numeric geometric "
                                  "family failed and the evidence label "
                                  "would be PRETRAINING_UNKNOWN",
        },
    ]
    body = artifact_body("r40_compute_escalation_request/1", {
        "calculation_owner": CALCULATION_OWNER,
        "local_resources": LOCAL,
        "zero_cost_branches_executed_first": True,
        "may_purchase_compute": C.MAY_PURCHASE_COMPUTE,
        "may_purchase_cloud_compute": C.MAY_PURCHASE_CLOUD_COMPUTE,
        "requests": requests,
        "positive_expected_value_action": next(
            (r["request_id"] for r in requests if r["justified"]), None),
        "release_continues_on_every_independent_branch": True,
        "money_spent": 0.0, "cloud_compute_spend": 0.0,
    })
    body["escalation_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body,
                    immutable=False)
    return body
