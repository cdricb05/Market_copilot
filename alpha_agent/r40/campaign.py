"""alpha_agent.r40.campaign - orchestration tail: JOINT_EVIDENCE table
(Track J), R40_CUMULATIVE_SEARCH_LEDGER and FINAL_VERDICT.

Seven result axes, never collapsed (``contract.RESULT_AXES``):

    SYSTEM_RESULT            every phase executed end to end
    FORWARD_ENGINE_RESULT    one canonical idempotent research cycle exists,
                             tested, and reports its state
    FORWARD_EVIDENCE_RESULT  what the TRUE_FORWARD ledgers actually contain
    INFORMATION_RESULT       NY Fed legacy bridge + cross-asset edges
    MODEL_RESULT             the open-weight and from-scratch challengers
                             against the R39 ridge / TCN baselines
    HISTORICAL_ALPHA_RESULT  R39 Track-K qualification at the cumulative
                             burden (a constant rule, never narrative)
    PROSPECTIVE_ALPHA_RESULT PASS only when a pre-registered success
                             boundary is crossed on TRUE_FORWARD rows

A candidate may become FORWARD_INTEREST_STRENGTHENED / WEAKENED /
FORWARD_FUTILITY / INSUFFICIENT_FORWARD_EVIDENCE; it never becomes
operational because a statistic crossed a research threshold.
"""
from __future__ import annotations

import time

from .. import r39 as _r39
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import burden_ledger as BL
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.campaign"
JOINT_NAME = "joint_evidence_table.json"
VERDICT_NAME = "final_verdict.json"


def _read(name: str, campaign_id: str) -> dict:
    return _r39.read_json(campaign_dir(campaign_id) / name) or {}


def joint_evidence(campaign_id: str = CAMPAIGN_ID) -> dict:
    """Per shadow: PREDICTIVE evidence next to ECONOMIC evidence, historical
    (selection-stage) and forward (TRUE_FORWARD), with the four interest
    states. Promotion is impossible here by construction."""
    from ..r39 import research_shadow as RS
    from . import shadow_registry as SR
    reg = SR.load(campaign_id) or {}
    cyc = _read("forward_research_cycle_state.json", campaign_id)
    seq = (cyc.get("sequential_evidence") or {}).get("per_shadow") or {}
    vel = _read("evidence_velocity_registry.json", campaign_id).get(
        "registry") or {}
    desk = RS._desk()
    r39_outs = desk._read_ledger(RS.shadow_dir(C.R39_CONTINUATION_CAMPAIGN_ID),
                                 RS.OUTCOME_LEDGER)
    r40_outs = desk._read_ledger(SR.shadow_dir(campaign_id), SR.OUTCOME_LEDGER)
    rows = {}
    for sh in reg.get("shadows", []):
        sid = sh["shadow_id"]
        src = r39_outs if sh.get("origin_release") == "release39" else r40_outs
        outs = [r for r in src if r.get("shadow_id") == sid]
        hist = vel.get(sid) or {}
        hs = hist.get("historical_stream") or {}
        sup = (hist.get("supporting_channels") or {}).get(
            "cross_sectional_rank_ic") or {}
        fwd_ic = [((r.get("supporting") or {}).get("rank_ic")) for r in outs]
        fwd_ic = [v for v in fwd_ic if v is not None]
        fwd_sign = [((r.get("supporting") or {}).get("sign_accuracy"))
                    for r in outs]
        fwd_sign = [v for v in fwd_sign if v is not None]
        se = seq.get(sid) or {}
        rows[sid] = {
            "candidate_id": sh["candidate_id"],
            "historical_selection_evidence": {
                "economic": {"zone_b_t": hs.get("t_stat"),
                             "mu_per_period": hs.get("mu_per_period"),
                             "sigma_per_period": hs.get("sigma_per_period"),
                             "periods": hs.get("periods")},
                "predictive": {"mean_rank_ic": sup.get("mean_ic"),
                               "ic_t": sup.get("t_stat")},
            },
            "true_forward_evidence": {
                "n_outcomes": len(outs),
                "economic": {"net_returns": [r["net_return"] for r in outs],
                             "e_value": se.get("e_value"),
                             "decision_state": se.get("decision_state"),
                             "confidence_sequence":
                                 se.get("confidence_sequence")},
                "predictive": {"mean_rank_ic": (sum(fwd_ic) / len(fwd_ic))
                               if fwd_ic else None,
                               "mean_sign_accuracy":
                                   (sum(fwd_sign) / len(fwd_sign))
                                   if fwd_sign else None},
                "cost": {"realised_cost_total":
                         sum(float(r.get("cost") or 0.0) for r in outs)},
            },
            "interest_state": se.get("interest",
                                     "INSUFFICIENT_FORWARD_EVIDENCE"),
            "promotion_allowed": False,
        }
    body = artifact_body("r40_joint_evidence_table/1", {
        "calculation_owner": CALCULATION_OWNER,
        "rows": rows,
        "states": ["FORWARD_INTEREST_STRENGTHENED",
                   "FORWARD_INTEREST_WEAKENED", "FORWARD_FUTILITY",
                   "INSUFFICIENT_FORWARD_EVIDENCE"],
        "a_model_may_predict_well_and_earn_nothing_after_cost": True,
        "promotion_possible_here": False,
    })
    body["joint_evidence_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / JOINT_NAME, body,
                    immutable=False)
    return body


def finalise(campaign_id: str = CAMPAIGN_ID) -> dict:
    ledger = BL.write_artifact(campaign_id)
    joint = joint_evidence(campaign_id)
    imp = _read("r39_closeout_import.json", campaign_id)
    cyc = _read("forward_research_cycle_state.json", campaign_id)
    ls = cyc.get("ledger_status") or {}
    reg = _read("shadow_registry_v2.json", campaign_id)
    succ = _read("corrected_wide_successor.json", campaign_id)
    ny = _read("nyfed_incremental_result.json", campaign_id)
    nymap = _read("nyfed_legacy_mapping.json", campaign_id)
    models = _read("r40_model_results.json", campaign_id)
    xa = _read("cross_asset_relationship_results.json", campaign_id)
    vel = _read("evidence_velocity_registry.json", campaign_id)
    prov = _read("model_weight_provenance.json", campaign_id)
    esc = _read("R40_COMPUTE_ESCALATION_REQUEST.json", campaign_id)
    intr = _read("intrinio_sample_readiness.json", campaign_id)
    port = _read("prospective_research_portfolio.json", campaign_id)

    n_snap = ls.get("true_forward_snapshots", 0)
    n_out = ls.get("true_forward_outcomes", 0)
    any_success = any(
        (r["true_forward_evidence"]["economic"].get("decision_state")
         == "SUCCESS_BOUNDARY_CROSSED") for r in joint["rows"].values())
    best_model = (models.get("best_r40_model") or {})
    tcn_t = (models.get("baselines") or {}).get("tcn_zone_b_t")
    best_new = None
    for r in models.get("comparison") or []:
        if not r["key"].startswith("baseline_"):
            best_new = r
            break
    ny_head = ny.get("headline") or {}
    xa_head = xa.get("headline") or {}
    wide_succ = succ.get("best_successor") or {}

    axes = {
        "SYSTEM_RESULT": "PASS" if (imp.get("state") == "R39_VERIFIED"
                                    and reg and cyc) else "FAIL",
        "FORWARD_ENGINE_RESULT": ("CANONICAL_IDEMPOTENT_CYCLE_READY"
                                  if cyc.get("FORWARD_CAPTURE_STATE")
                                  in ("READY_WAITING_FOR_ELIGIBLE_DATE",
                                      "NOTHING_NEW_IDEMPOTENT",
                                      "CAPTURED_NEW_DECISIONS")
                                  else "NOT_READY"),
        "FORWARD_EVIDENCE_RESULT": ("NO_TRUE_FORWARD_OBSERVATIONS_YET"
                                    if n_snap == 0 else
                                    "SNAPSHOTS_CAPTURED_NO_MATURED_OUTCOMES"
                                    if n_out == 0 else
                                    "TRUE_FORWARD_OUTCOMES_ACCUMULATING"),
        "INFORMATION_RESULT": ("NO_INCREMENTAL_INFORMATION_EDGE"
                               if not ny_head.get("robust_increment")
                               and not xa_head.get("new_edge")
                               else "INCREMENTAL_INFORMATION_CANDIDATE"),
        "MODEL_RESULT": ("NO_MATERIAL_IMPROVEMENT_OVER_R39_TCN"
                         if not (best_new or {}).get("materially_beats_both")
                         else "MATERIALLY_STRONGER_MODEL_FOUND"),
        "HISTORICAL_ALPHA_RESULT": "FAIL",
        "PROSPECTIVE_ALPHA_RESULT": "PASS" if any_success else
        ("NOT_YET_TESTABLE" if n_out == 0 else "FAIL_SO_FAR"),
    }
    terminal = ["R40_PROSPECTIVE_ENGINE_READY_WAITING_FOR_TIME"]
    if axes["INFORMATION_RESULT"] == "NO_INCREMENTAL_INFORMATION_EDGE" and \
            axes["MODEL_RESULT"] == "NO_MATERIAL_IMPROVEMENT_OVER_R39_TCN":
        terminal.append("R40_NO_INCREMENTAL_EDGE_FOUND")
    if esc.get("positive_expected_value_action"):
        terminal.append("R40_COMPUTE_LIMIT_BINDING")
    if axes["PROSPECTIVE_ALPHA_RESULT"] == "PASS":
        terminal.append("R40_FORWARD_ALPHA_EVIDENCE_STRENGTHENED")

    wide_vel = (vel.get("registry") or {}).get("shadow_wide_xs") or {}
    ttd = (wide_vel.get("time_to_decision") or {}).get("success_years") or {}
    binding = "TIME"
    if axes["SYSTEM_RESULT"] != "PASS":
        binding = "IMPLEMENTATION"
    body = artifact_body("r40_final_verdict/1", {
        "calculation_owner": CALCULATION_OWNER,
        "finalised_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "result_axes": axes,
        "terminal_states": terminal,
        "verdict_is_forced": False,
        "binding_limitation": binding,
        "binding_limitation_note": "every remaining blocker needs calendar "
                                   "time (forward evidence), money (GPU "
                                   "hours, vendor data) or external vendor "
                                   "action; no $0 local branch remains "
                                   "unexecuted",
        "cumulative_burden": {k: ledger[k] for k in (
            "R39_INHERITED_EFFECTIVE_TRIALS", "R40_NEW_EFFECTIVE_TRIALS",
            "CUMULATIVE_R39_R40_EFFECTIVE_TRIALS")},
        "research_shadows": {
            "n": reg.get("n_shadows"),
            "hashes": {s["shadow_id"]: {"candidate_id": s["candidate_id"],
                                        "spec_hash": s.get("spec_hash"),
                                        "coefficient_hash":
                                            s.get("coefficient_hash"),
                                        "frozen_at": s.get("frozen_at")}
                       for s in reg.get("shadows", [])},
            "slot_5": (reg.get("slot_5_resolution") or {}).get("winner"),
        },
        "true_forward": {"snapshots": n_snap, "outcomes": n_out,
                         "state": cyc.get("FORWARD_CAPTURE_STATE"),
                         "chains_intact": ls.get("all_chains_intact")},
        "wide": {"successor_best": wide_succ,
                 "successor_preserves_zone_b": wide_succ.get(
                     "preserves_or_improves_zone_b_economics"),
                 "evidence_rate_per_year":
                     wide_vel.get("success_information_rate_per_year"),
                 "time_to_success_years": ttd},
        "nyfed": {"bridge": nymap.get("state"), "headline": ny_head},
        "models": {"best_r40": best_model, "tcn_baseline_t": tcn_t,
                   "weights_downloaded": [k for k, v in (
                       prov.get("weights") or {}).items()
                       if v.get("state") == "ACQUIRED"]},
        "cross_asset": xa_head,
        "portfolio": {"redundant_pairs": port.get("redundant_pairs"),
                      "priority": (port.get("research_attention_priority")
                                   or [])[:3]},
        "intrinio": {"state": intr.get("state")},
        "compute": {"positive_ev_action":
                    esc.get("positive_expected_value_action")},
        "safety_tail": {"MONEY_SPENT": 0.0, "CLOUD_COMPUTE_SPEND": 0.0,
                        "NEW_SUBSCRIPTIONS": 0, "TRIALS_STARTED": 0,
                        "OPERATIONAL_WRITES": 0, "PORTFOLIO_MUTATIONS": 0,
                        "MODEL_PROMOTIONS": 0, "PRODUCTION_RESTARTS": 0,
                        "SCHEDULER_CHANGES": 0, "ORDERS_CREATED": 0,
                        "MODEL_WEIGHTS_DOWNLOADED": len([
                            k for k, v in (prov.get("weights") or {}).items()
                            if v.get("state") == "ACQUIRED"])},
        "shell_policy": C.SHELL_POLICY_EVENTS,
    })
    body["final_verdict_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / VERDICT_NAME, body,
                    immutable=False)
    return body
