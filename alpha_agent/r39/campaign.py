"""alpha_agent.r39.campaign - orchestration and the Release-39 verdict.

Runs the whole engine in order and refuses to conflate its result axes:

    SYSTEM_RESULT            - did the engine execute end to end?
    DATA_RESULT              - what did the universal state actually hold?
    DISCOVERY_RESULT         - how much machine search really ran?
    HISTORICAL_ALPHA_RESULT  - did anything survive every historical gate?
    FORWARD_CANDIDATE_RESULT - what was frozen for TRUE_FORWARD evidence?

``ALPHA_RESULT`` may be PASS only alongside
``contract.ALPHA_PASS_REQUIRES_VERDICT`` - a constant enforced here in
``build_verdict``, not a sentence in a document. No success state is
forced; the decision tree reads the evidence and says what it finds.
"""
from __future__ import annotations

import time
import traceback

import numpy as np

from .. import r39
from ..r31 import multiple_testing as _mt
from . import burden as B
from . import contract as C
from . import estate as E
from . import frontier as F
from . import handoff as H
from . import integrity as I
from . import judge as J  # noqa: F401  (re-exported for the runner/tests)
from . import model_registry as M
from . import representation_factory as R
from . import target_factory as TF
from . import trade_space as TS
from . import universal_state as US
from . import zones
from .discovery_director import Director

CALCULATION_OWNER = "alpha_agent.r39.campaign"
VERDICT_SCHEMA = "r39_final_verdict/1"
VERDICT_NAME = "final_verdict.json"
ECONOMIC_NAME = "economic_results.json"
ESCALATION_NAME = "compute_escalation_request.json"


def log(msg: str) -> None:
    print("[r39 %s] %s" % (time.strftime("%H:%M:%S"), msg), flush=True)


# --------------------------------------------------------------------------- #
# Phase runner
# --------------------------------------------------------------------------- #
def run(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    t0 = time.time()
    r39.campaign_dir(campaign_id).mkdir(parents=True, exist_ok=True)

    log("phase 0: estate inventory + R38 integrity map")
    est = E.build()
    E.freeze(est)
    imap = I.build()
    I.freeze(imap)
    log("integrity map: %s" % imap["counts_by_research_status"])

    log("phase 1: universal PIT state")
    state = US.assemble(campaign_id)
    for lane in ("fut", "vx", "eq", "etf"):
        df = state[lane]
        log("  %s rows=%s" % (lane, 0 if df is None or df.empty
                              else len(df)))

    log("phase 2: targets")
    state["fut"] = TF.materialise(state["fut"], scope_col="asset_class")
    state["vx"] = TF.materialise(state["vx"], scope_col="asset_class")
    state["eq"] = TF.materialise(state["eq"], scope_col="economic_group")
    if state["etf"] is not None and not state["etf"].empty:
        state["etf"] = TF.materialise(state["etf"], scope_col="asset_class")
    TF.build_registry(campaign_id)
    TS.build_registry(campaign_id)
    M.build_registry(campaign_id)

    log("phase 3: representations")
    director = Director(state, campaign_id)
    bundles = director.prepare_representations()
    R.build_registry(bundles, director.lineage, campaign_id)
    log("  bundles=%d generated_features=%d"
        % (len(bundles), len(director.lineage)))

    log("phase 4: candidate generation")
    director.generate_candidates()
    log("  candidates=%d" % len(director.candidates))

    log("phase 5: stage-1 Zone-A screen (purged CV)")
    stage1 = director.stage1_screen()
    ok1 = sum(1 for s in stage1 if s.get("score") is not None)
    log("  screened=%d scored=%d" % (len(stage1), ok1))
    r39.write_json(r39.campaign_dir(campaign_id) / "discovery_results.json",
                   r39.artifact_body("r39_discovery_results/1", {
                       "campaign_id": campaign_id,
                       "calculation_owner": CALCULATION_OWNER,
                       "zone": "ZONE_A", "rows": stage1,
                       "n_screened": len(stage1), "n_scored": ok1}),
                   immutable=False)

    log("phase 6: stage-2 Zone-B validation")
    promoted = director.promote_stage1()
    log("  promoted=%d" % len(promoted))
    director.stage2_validate(promoted)

    log("phase 7: stage-3 refinement (ensembles, regime, tail)")
    pool3 = director.stage3_refine(promoted)
    log("  stage-3 pool=%d" % len(pool3))
    _persist_validation(director, campaign_id)

    log("phase 8: freeze finalists + locked Zone-C confirmation")
    finalists = director.select_finalists(pool3)
    combo_spec = F.combination_spec(director, finalists)
    extra = ()
    if combo_spec.get("state") == "OK":
        extra = ({"candidate_id": F.COMBINATION_CANDIDATE_ID,
                  "spec_hash": combo_spec["spec_hash"],
                  "family": "COMBINATION", "sample": "MULTI",
                  "horizon_sessions": 21},)
    director.freeze_finalists(finalists, extra_entries=extra)
    log("  finalists=%d + combination=%s (budget %d)"
        % (len(finalists), bool(extra), C.MAX_LOCKBOX_CANDIDATES))
    for cand in finalists:
        rep = director.confirm_finalist(cand)
        log("  zone C %s: excess=%.4f t=%s"
            % (cand["candidate_id"],
               rep.get("after_cost_excess_annualised") or float("nan"),
               rep.get("after_cost_excess_t_stat")))

    log("phase 9: robustness")
    for cand in finalists:
        director.robustness(cand)

    log("phase 10: combination book (reserved lockbox slot)")
    combo = F.diversified_combination(director, finalists, combo_spec,
                                      campaign_id)
    combo_t = None
    if combo.get("_sig"):
        combo_t = combo["_sig"].get("t_stat")

    log("phase 11: search-burden correction")
    mburden = _multiple_discovery(director, finalists, combo, campaign_id,
                                  stage1_screened=len(stage1))

    log("phase 12: frontier + economics artifacts")
    qual = _qualify(director, finalists, mburden)
    dsr_by = {row["candidate_id"]: row for row in mburden["deflated_sharpe"]}
    F.correlation_matrix(finalists, director.results, campaign_id)
    frontier_body = F.build_frontier(director, finalists,
                                     qualification=qual,
                                     dsr_by_candidate=dsr_by,
                                     campaign_id=campaign_id)
    _persist_economics(director, campaign_id)
    _persist_registry(director, campaign_id)
    derive_confirmation_artifacts(campaign_id)

    log("phase 13: forward handoff + parallel lanes")
    qualified = [c for c in finalists
                 if qual.get(c["candidate_id"], {}).get("qualified")]
    fwd = H.freeze_forward_candidates(
        director, qualified,
        state_contract_hash=state["contract"]["state_contract_hash"],
        campaign_id=campaign_id)
    H.intrinio_status(campaign_id)
    escalation = _compute_escalation(campaign_id)

    log("phase 14: verdict")
    verdict = build_verdict(
        campaign_id=campaign_id, director=director, finalists=finalists,
        qualification=qual, mburden=mburden, combo_t=combo_t,
        n_forward=fwd["n_frozen"], state=state, integrity=imap,
        estate=est, escalation=escalation,
        runtime_seconds=round(time.time() - t0, 1))
    r39.write_json(r39.campaign_dir(campaign_id) / VERDICT_NAME, verdict,
                   immutable=False)
    log("VERDICT: %s" % verdict["verdict"])
    return {"verdict": verdict, "frontier": frontier_body,
            "director": director, "state": state}


# --------------------------------------------------------------------------- #
# Persistence helpers
# --------------------------------------------------------------------------- #
def _persist_validation(director: Director, campaign_id: str) -> None:
    rows = []
    for c in director.candidates:
        res = director.results.get(c["candidate_id"], {})
        if "stage2" in res:
            rows.append({"candidate_id": c["candidate_id"],
                         "family": c["family"], "origin": c["origin"],
                         "zone": "ZONE_B", **(res["stage2"] or {})})
    r39.write_json(
        r39.campaign_dir(campaign_id) / "validation_results.json",
        r39.artifact_body("r39_validation_results/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "rows": rows, "n_rows": len(rows),
            "reuse": zones.reuse_summary(campaign_id)}),
        immutable=False)


def _persist_economics(director: Director, campaign_id: str) -> None:
    rows = []
    for c in director.candidates:
        res = director.results.get(c["candidate_id"], {})
        entry = {"candidate_id": c["candidate_id"], "lane": c["lane"],
                 "scope": c["scope"], "family": c["family"],
                 "model": c["model"], "expression": c["expression"],
                 "origin": c["origin"]}
        if "stage2" in res:
            entry["zone_b"] = res["stage2"]
        if "zone_c" in res:
            entry["zone_c"] = res["zone_c"]
        if "robustness" in res:
            entry["robustness"] = res["robustness"]
        if len(entry) > 7:
            rows.append(entry)
    r39.write_json(
        r39.campaign_dir(campaign_id) / ECONOMIC_NAME,
        r39.artifact_body("r39_economic_results/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "economic_judge_owner": "alpha_agent.r34.economics via "
                                    "alpha_agent.r39.judge",
            "cost_model_state": C.COST_MODEL_STATE,
            "rows": rows, "n_rows": len(rows)}),
        immutable=False)


def _persist_registry(director: Director, campaign_id: str) -> None:
    cands = []
    for c in director.candidates:
        row = {k: v for k, v in c.items() if not k.startswith("_")}
        row["stage1"] = (director.results.get(c["candidate_id"], {})
                         .get("stage1"))
        cands.append(row)
    r39.write_json(
        r39.campaign_dir(campaign_id) / "autonomous_candidate_registry.json",
        r39.artifact_body("r39_autonomous_candidate_registry/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "candidates": cands, "n_candidates": len(cands),
            "no_silent_deletion_of_failures": True}),
        immutable=False)
    r39.write_json(
        r39.campaign_dir(campaign_id) / "candidate_lineage.json",
        r39.artifact_body("r39_candidate_lineage/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "generated_feature_lineage": director.lineage,
            "candidate_parents": {
                c["candidate_id"]: c.get("parent_candidate_ids", [])
                for c in director.candidates}}),
        immutable=False)


def derive_confirmation_artifacts(campaign_id: str) -> None:
    """The standalone LOCKED_CONFIRMATION_RESULTS and ROBUSTNESS_RESULTS
    views, derived from the economics table plus the lockbox logs so they
    can never disagree with either."""
    econ = r39.read_json(r39.campaign_dir(campaign_id) / ECONOMIC_NAME) or {}
    finalists_body = zones.load_finalists(campaign_id) or {}
    access = r39.read_json(
        r39.campaign_dir(campaign_id) / zones.ACCESS_NAME) or {}
    confirmed_ids = {a["candidate_id"] for a in access.get("accesses", [])}
    conf_rows, rob_rows = [], []
    for row in econ.get("rows", []):
        if row.get("candidate_id") in confirmed_ids and "zone_c" in row:
            conf_rows.append({
                "candidate_id": row["candidate_id"],
                "family": row.get("family"), "lane": row.get("lane"),
                "model": row.get("model"),
                "expression": row.get("expression"),
                "origin": row.get("origin"),
                "zone_c": row["zone_c"]})
        if "robustness" in row:
            rob_rows.append({"candidate_id": row["candidate_id"],
                             "family": row.get("family"),
                             "robustness": row["robustness"]})
    r39.write_json(
        r39.campaign_dir(campaign_id) / "locked_confirmation_results.json",
        r39.artifact_body("r39_locked_confirmation_results/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "zone_c_label": C.ZONE_C_EVIDENCE_LABEL,
            "finalist_set_hash": finalists_body.get("finalist_set_hash"),
            "access_count": access.get("access_count"),
            "one_execution_per_finalist": C.ONE_EXECUTION_PER_FINALIST,
            "no_redesign_and_retry": C.NO_REDESIGN_AND_RETRY,
            "rows": conf_rows, "n_rows": len(conf_rows)}),
        immutable=False)
    r39.write_json(
        r39.campaign_dir(campaign_id) / "robustness_results.json",
        r39.artifact_body("r39_robustness_results/1", {
            "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "checks": ["sign_splits", "cost_stress_x2",
                       "leave_one_market_out", "regime_slices"],
            "rows": rob_rows, "n_rows": len(rob_rows)}),
        immutable=False)


# --------------------------------------------------------------------------- #
# Multiple discovery + qualification
# --------------------------------------------------------------------------- #
def _multiple_discovery(director: Director, finalists: list, combo: dict,
                        campaign_id: str, *, stage1_screened: int) -> dict:
    per, series, p_values, ids = [], {}, [], []
    for c in finalists:
        rep = director.results[c["candidate_id"]].get("zone_c_series") or {}
        t = rep.get("after_cost_excess_t_stat")
        p = _mt.two_sided_p(t) if t is not None else None
        ids.append(c["candidate_id"])
        p_values.append(p if p is not None else 1.0)
        per.append({"candidate_id": c["candidate_id"],
                    "zone_c_t_stat": t, "p_value": p})
        diff = rep.get("excess_diff_series")
        if diff is not None:
            series[c["candidate_id"]] = np.asarray(diff, dtype=float)
    if combo.get("_sig") is not None:
        t = combo["_sig"].get("t_stat")
        p = _mt.two_sided_p(t) if t is not None else None
        ids.append(F.COMBINATION_CANDIDATE_ID)
        p_values.append(p if p is not None else 1.0)
        per.append({"candidate_id": F.COMBINATION_CANDIDATE_ID,
                    "zone_c_t_stat": t, "p_value": p})
        s = combo.get("_zone_c_series")
        if s is not None and len(s):
            series[F.COMBINATION_CANDIDATE_ID] = s.to_numpy(dtype=float)
    bh = _mt.benjamini_hochberg(p_values)
    bh["candidate_ids"] = ids
    bh["survivors"] = [ids[i] for i in bh.get("rejected", [])]
    spa = _mt.superior_predictive_ability(series) if series else \
        {"state": "NO_SERIES"}
    # DSR: effective trials from the Zone-B reuse ledger; trial variance
    # from the cross-candidate spread of Zone-B per-period Sharpes
    srs = []
    for c in director.candidates:
        rep = director.results.get(c["candidate_id"], {}) \
            .get("stage2_series") or {}
        d = rep.get("excess_diff_series")
        if d is None:
            continue
        d = np.asarray(d, dtype=float)
        d = d[np.isfinite(d)]
        if d.size >= 24 and d.std(ddof=1) > 0:
            srs.append(float(d.mean() / d.std(ddof=1)))
    trial_var = float(np.var(srs, ddof=1)) if len(srs) >= 3 else 1e-4
    burden_summary = B.effective_search_burden(
        stage1_screened=stage1_screened, campaign_id=campaign_id)
    n_trials = burden_summary["dsr_effective_trials"]
    dsr_rows = []
    for cid, s in series.items():
        d = B.deflated_sharpe(s, n_trials=n_trials,
                              trial_sharpe_variance=trial_var)
        d["candidate_id"] = cid
        dsr_rows.append(d)
    return B.build(campaign_id=campaign_id, per_candidate=per, bh=bh,
                   spa=spa, dsr_rows=dsr_rows,
                   burden_summary={**burden_summary,
                                   "trial_sharpe_variance": trial_var})


def _qualify(director: Director, finalists: list, mburden: dict) -> dict:
    bh_survivors = set(mburden["benjamini_hochberg"].get("survivors", []))
    dsr_by = {r["candidate_id"]: r for r in mburden["deflated_sharpe"]}
    out = {}
    for c in finalists:
        cid = c["candidate_id"]
        zc = director.results[cid].get("zone_c") or {}
        rb = director.results[cid].get("robustness") or {}
        excess = zc.get("after_cost_excess_annualised")
        dsr = (dsr_by.get(cid) or {}).get("dsr")
        halves = ((rb.get("sign_splits") or {}).get("halves_same_sign"))
        stress = ((rb.get("cost_stress_x2") or {})
                  .get("after_cost_excess_annualised"))
        lomo = rb.get("leave_one_market_out") or {}
        lomo_ok = lomo.get("n_sign_flips", 0) == 0 \
            if "n_sign_flips" in lomo else True
        checks = {
            "positive_excess": bool(excess is not None and excess > 0),
            "bh_survivor": cid in bh_survivors,
            "deflated_sharpe": bool(
                dsr is not None and dsr >= C.DSR_QUALIFICATION_THRESHOLD),
            "sign_stability": bool(halves),
            "cost_stress": bool(stress is not None and stress > 0),
            "concentration": bool(lomo_ok),
        }
        out[cid] = {"qualified": all(checks.values()), "checks": checks}
    return out


# --------------------------------------------------------------------------- #
# Compute escalation
# --------------------------------------------------------------------------- #
def _compute_escalation(campaign_id: str) -> dict:
    body = r39.artifact_body("r39_compute_escalation_request/1", {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "blocked_branch": "DEEP_SEQUENCE + FOUNDATION_TIME_SERIES + "
                          "TABULAR_FOUNDATION + SELF_SUPERVISED + "
                          "VISION_CHART model families",
        "exact_models": ["PatchTST / TFT / Mamba-class temporal models",
                         "Chronos-Bolt-class and TimesFM-class zero-shot "
                         "forecasters", "TabPFN-v2-class tabular "
                         "in-context learner", "chart-image encoder"],
        "exact_experiment": "re-run the frozen R39 candidate protocol "
                            "(same universal state, same zones, same "
                            "economic judge, same lockbox) with the deep/"
                            "foundation families as additional Stage-2 "
                            "entrants",
        "why_local_hardware_is_insufficient": [
            "the venv drive has ~6 GB free; a torch stack needs 2-4 GB "
            "plus weights", "GTX 1650 4 GB VRAM is below common "
            "TFT/PatchTST training footprints",
            "MAY_DOWNLOAD_MODEL_WEIGHTS = False is a frozen contract flag "
            "requiring an operator decision"],
        "required_gpu_class": "one 16-24 GB GPU (RTX 4090 / A10 class)",
        "required_vram_gb": 16,
        "estimated_gpu_hours": 40,
        "estimated_dollar_cost_usd": "40-90 at typical 2026 spot rates",
        "zero_cost_alternative": "operator frees ~10 GB on the venv drive "
                                 "and approves a CPU-only torch install; "
                                 "slow (days) but $0",
        "expected_research_decision_unlocked":
            "whether modern deep/foundation sequence models extract "
            "signal the admitted CPU zoo cannot - the only model families "
            "R39 leaves unexecuted",
        "what_cheaper_experiment_justified_this":
            "the completed R39 CPU campaign itself: it prices exactly "
            "what the admitted families can and cannot find on this "
            "estate",
        "spend_state": "REQUEST_ONLY - no compute purchased; only this "
                       "branch stops; every independent branch continued",
    })
    body["escalation_hash"] = r39.sha(body)
    r39.write_json(r39.campaign_dir(campaign_id) / ESCALATION_NAME, body,
                   immutable=False)
    return body


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
def build_verdict(*, campaign_id: str, director: Director, finalists: list,
                  qualification: dict, mburden: dict, combo_t,
                  n_forward: int, state: dict, integrity: dict,
                  estate: dict, escalation: dict,
                  runtime_seconds: float) -> dict:
    counters = director.ledger.counters
    qualified = [cid for cid, q in qualification.items() if q["qualified"]]
    machine_qualified = [
        cid for cid in qualified
        for c in director.candidates
        if c["candidate_id"] == cid and c["origin"] == "MACHINE_GENERATED"]
    near_misses = []
    for c in finalists:
        zc = director.results[c["candidate_id"]].get("zone_c") or {}
        t = zc.get("after_cost_excess_t_stat")
        if t is not None and t >= 2.0 and \
                c["candidate_id"] not in qualified:
            near_misses.append({"candidate_id": c["candidate_id"],
                                "t": t, "family": c["family"]})
    if qualified and machine_qualified:
        verdict = "R39_AUTONOMOUS_ALPHA_DISCOVERED"
    elif qualified:
        verdict = "R39_HISTORICAL_ALPHA_CANDIDATES_DISCOVERED"
    else:
        verdict = "R39_NO_ROBUST_ALPHA_DESPITE_UNIVERSAL_SEARCH"
    alpha_result = "PASS" if verdict == C.ALPHA_PASS_REQUIRES_VERDICT \
        else "FAIL"
    historical = "PASS" if qualified else "FAIL"
    body = r39.artifact_body(VERDICT_SCHEMA, {
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "verdict": verdict,
        "verdict_is_forced": False,
        "SYSTEM_RESULT": "PASS",
        "DATA_RESULT": {
            "state": "UNIVERSAL_STATE_ASSEMBLED",
            "families_used": estate["families_used_in_state"],
            "families_excluded": estate["excluded_families"],
            "lanes": state["contract"]["lanes"],
            "etf_lane_available": state["contract"]["etf_lane_available"]},
        "DISCOVERY_RESULT": {
            "state": "EXECUTED" if counters["CANDIDATES_SCREENED"] else
            "FAIL", **counters},
        "HISTORICAL_ALPHA_RESULT": historical,
        "ALPHA_RESULT": alpha_result,
        "FORWARD_CANDIDATE_RESULT": ("%d_FROZEN" % n_forward)
        if n_forward else "NONE_FROZEN",
        "qualified_candidates": qualified,
        "machine_generated_qualified": machine_qualified,
        "near_misses_zone_c_t_ge_2": near_misses,
        "combination_zone_c_t": combo_t,
        "bh": {k: mburden["benjamini_hochberg"].get(k)
               for k in ("m", "q", "n_rejected", "survivors")},
        "spa_p_value": (mburden["superior_predictive_ability"] or {})
        .get("p_value"),
        "effective_search_burden": mburden["effective_search_burden"],
        "r38_integrity_counts": integrity["counts_by_research_status"],
        "zone_c_label": C.ZONE_C_EVIDENCE_LABEL,
        "compute_escalation_exists": bool(escalation),
        "runtime_seconds": runtime_seconds,
        "safety_tail": {
            "MONEY_SPENT": 0.0, "CLOUD_COMPUTE_SPEND": 0.0,
            "NEW_SUBSCRIPTIONS": 0, "SUBSCRIPTION_CHANGES": 0,
            "TRIALS_STARTED": 0, "OPERATIONAL_WRITES": 0,
            "PORTFOLIO_MUTATIONS": 0, "MODEL_PROMOTIONS": 0,
            "PRODUCTION_RESTARTS": 0},
        "purchase_authority": C.purchase_authority(),
    })
    body["final_verdict_hash"] = r39.sha(body)
    return body


def safe_run(campaign_id: str = C.CAMPAIGN_ID) -> int:
    try:
        run(campaign_id)
        return 0
    except Exception:
        traceback.print_exc()
        return 1
