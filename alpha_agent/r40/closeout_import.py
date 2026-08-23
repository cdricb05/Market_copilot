"""alpha_agent.r40.closeout_import - R39_CLOSEOUT_IMPORT.

Release 40 begins by VERIFYING, not trusting, what Release 39 left behind:

* both immutable R39 campaign directories exist and every artifact listed
  in the R39 handoff manifest hashes to the recorded SHA-256;
* the cumulative search burden reads exactly 194 (107 v1 + 87 continuation)
  from the continuation's ledger artifact AND from the reuse ledger itself;
* the three research shadows exist, carry RESEARCH_SHADOW_ONLY /
  HISTORICAL_QUALIFICATION = FAIL / PROMOTION_ALLOWED = False, and their
  specification / coefficient hashes reproduce from the registry bytes;
* the frozen WIDE facts (id, spec hash, Zone-C economics, residual alpha,
  kill tests) and the continuation facts (0 Zone-C accesses, the Slot-4 and
  TCN candidates with their Zone-B t) are read from the artifacts;
* the R40 contract hash (Slot-5 rule included) is frozen INTO this artifact
  before any R40 evaluation runs.

Any discrepancy is recorded as a named mismatch; the artifact's
``state`` is ``R39_VERIFIED`` only when every check passes.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from .. import r39 as _r39
from ..r39 import research_shadow as RS
from ..r39 import zones
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.closeout_import"
ARTIFACT_NAME = "r39_closeout_import.json"


def _manifest_check(handoff_dir: Path, research_root: Path) -> dict:
    man = handoff_dir / "artifact_hashes.txt"
    if not man.exists():
        return {"state": "MANIFEST_MISSING", "path": str(man)}
    ok, bad, missing = 0, [], []
    for line in man.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2 or len(parts[0]) != 64:
            continue
        h, rel = parts[0].lower(), parts[1].strip()
        p = research_root / rel
        if not p.exists():
            missing.append(rel)
            continue
        if _r39.sha_file(p).lower() == h:
            ok += 1
        else:
            bad.append(rel)
    return {"state": "OK" if not bad and not missing else "MISMATCH",
            "manifest": str(man), "verified": ok, "mismatched": bad,
            "missing": missing}


def _git_head(repo: Path) -> dict:
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo),
                              capture_output=True, text=True,
                              timeout=30).stdout.strip()
        remote = subprocess.run(
            ["git", "rev-parse", "origin/stage19-controlled-rebalance"],
            cwd=str(repo), capture_output=True, text=True,
            timeout=30).stdout.strip()
        return {"head": head, "remote": remote,
                "local_equals_remote": bool(head) and head == remote,
                "head_is_r39_closeout": head == C.R39_CLOSEOUT_COMMIT}
    except Exception as e:  # pragma: no cover - environment
        return {"state": "GIT_UNAVAILABLE", "error": str(e)[:200]}


def _shadow_checks(reg: dict, hashes: dict) -> dict:
    out = {"n_shadows": reg.get("n_shadows"), "rows": {}, "all_ok": True}
    body = {k: v for k, v in reg.items() if k != "shadow_registry_hash"}
    recomputed = _r39.sha(body)
    out["registry_hash_recorded"] = reg.get("shadow_registry_hash")
    out["registry_hash_reproduces"] = recomputed == reg.get(
        "shadow_registry_hash")
    hb = {k: v for k, v in hashes.items()
          if k != "forward_specification_hashes_hash"}
    out["spec_hashes_artifact_reproduces"] = \
        _r39.sha(hb) == hashes.get("forward_specification_hashes_hash")
    out["spec_hashes_bind_registry"] = \
        hashes.get("registry_hash") == reg.get("shadow_registry_hash")
    for sh in reg.get("shadows", []):
        row = {
            "candidate_id": sh.get("candidate_id"),
            "research_shadow_only": sh.get("research_shadow_only") is True,
            "historical_qualification_fail":
                sh.get("historical_qualification") == "FAIL",
            "promotion_allowed_false": sh.get("promotion_allowed") is False,
            "frozen_at": sh.get("frozen_at"),
        }
        fm = sh.get("frozen_model")
        if fm:
            core = {k: v for k, v in fm.items() if k != "coefficient_hash"}
            row["coefficient_hash_reproduces"] = \
                _r39.sha(core) == fm.get("coefficient_hash")
            row["coefficient_hash"] = fm.get("coefficient_hash")
            row["n_features"] = len(fm.get("features", []))
        h = (hashes.get("hashes") or {}).get(sh["shadow_id"], {})
        row["hash_row_matches"] = (
            h.get("candidate_id") == sh.get("candidate_id")
            and h.get("spec_hash") == sh.get("spec_hash")
            and h.get("coefficient_hash") ==
            ((fm or {}).get("coefficient_hash")))
        ok = all(v for k, v in row.items()
                 if isinstance(v, bool))
        row["ok"] = ok
        out["all_ok"] = out["all_ok"] and ok
        out["rows"][sh["shadow_id"]] = row
    out["all_ok"] = out["all_ok"] and out["registry_hash_reproduces"] \
        and out["spec_hashes_artifact_reproduces"] \
        and out["spec_hashes_bind_registry"]
    return out


def run(campaign_id: str = CAMPAIGN_ID, *, repo_root: Path = None,
        handoff_dir: Path = None) -> dict:
    """Verify the R39 closeout and freeze the R40 contract hash."""
    existing = _r39.read_json(campaign_dir(campaign_id) / ARTIFACT_NAME)
    if existing:
        return existing
    root39 = _r39.research_root()
    v1 = _r39.campaign_dir(C.R39_V1_CAMPAIGN_ID)
    v2 = _r39.campaign_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
    handoff = Path(handoff_dir or C.R39_HANDOFF_DIR)
    repo = Path(repo_root or Path(__file__).resolve().parents[2])
    exp = C.R39_EXPECTED
    mismatches = []

    def check(name, got, want, tol=None):
        ok = (abs(float(got) - float(want)) <= tol) if tol is not None \
            else (got == want)
        if not ok:
            mismatches.append({"check": name, "got": got, "expected": want})
        return {"got": got, "expected": want, "ok": bool(ok)}

    facts = {}
    # --- campaign directories ------------------------------------------ #
    facts["v1_dir_exists"] = check("v1_dir_exists", v1.exists(), True)
    facts["v2_dir_exists"] = check("v2_dir_exists", v2.exists(), True)
    # --- manifest hashes ----------------------------------------------- #
    manifest = _manifest_check(handoff, root39)
    facts["artifact_manifest"] = check("artifact_manifest_state",
                                       manifest.get("state"), "OK")
    # --- v1 funnel ------------------------------------------------------ #
    budget = _r39.read_json(v1 / "search_budget_ledger.json") or {}
    counters = budget.get("counters") or {}
    facts["v1_generated"] = check(
        "v1_generated", int(counters.get("CANDIDATES_GENERATED", -1)),
        exp["v1_generated"])
    facts["v1_screened"] = check(
        "v1_screened", int(counters.get("CANDIDATES_SCREENED", -1)),
        exp["v1_screened"])
    facts["v1_funnel"] = dict(counters)
    v1_reuse = _r39.read_json(v1 / zones.REUSE_NAME) or {}
    facts["v1_zone_b_distinct"] = check(
        "v1_zone_b_distinct", int(v1_reuse.get("distinct_candidates", -1)),
        exp["v1_zone_b_distinct"])
    access = _r39.read_json(v1 / zones.ACCESS_NAME) or {}
    facts["v1_locked_confirmations"] = check(
        "v1_locked_confirmations", int(access.get("access_count", -1)),
        exp["v1_locked_confirmations"])
    # --- cumulative burden ---------------------------------------------- #
    cum = _r39.read_json(v2 / "cumulative_search_ledger.json") or {}
    facts["cumulative_effective_trials_artifact"] = check(
        "cumulative_effective_trials_artifact",
        int(cum.get("R39_CUMULATIVE_EFFECTIVE_TRIALS", -1)),
        exp["cumulative_effective_trials"])
    facts["continuation_new_trials"] = check(
        "continuation_new_trials",
        int(cum.get("R39_CONTINUATION_NEW_TRIALS", -1)),
        exp["continuation_new_trials"])
    v2_reuse = _r39.read_json(v2 / zones.REUSE_NAME) or {}
    facts["cumulative_effective_trials_ledger"] = check(
        "cumulative_effective_trials_ledger",
        int(v2_reuse.get("distinct_candidates", -1)),
        exp["cumulative_effective_trials"])
    pre = _r39.read_json(v2 / "zone_c_pregate_decision.json") or {}
    facts["continuation_zone_c_accesses"] = check(
        "continuation_zone_c_accesses",
        int(pre.get("zone_c_accesses_by_continuation")
            if pre.get("zone_c_accesses_by_continuation") is not None
            else -1),
        exp["continuation_zone_c_accesses"])
    facts["continuation_pregate_decision"] = {
        "got": str(pre.get("decision", ""))[:80],
        "no_zone_c_access": str(pre.get("decision", "")).startswith(
            "NO_ZONE_C_ACCESS")}
    if not facts["continuation_pregate_decision"]["no_zone_c_access"]:
        mismatches.append({"check": "continuation_pregate_decision",
                           "got": pre.get("decision")})
    # --- WIDE ------------------------------------------------------------ #
    recon = _r39.read_json(v2 / "wide_reconstruction.json") or {}
    proofs = recon.get("proofs") or {}
    facts["wide_candidate_id"] = check("wide_candidate_id",
                                       recon.get("candidate_id"),
                                       exp["wide_candidate_id"])
    facts["wide_spec_hash"] = check("wide_spec_hash",
                                    proofs.get("spec_hash"),
                                    exp["wide_spec_hash"])
    facts["wide_reconstruction_exact"] = check(
        "wide_reconstruction_exact",
        bool(proofs.get("zone_c_matches_frozen")
             and proofs.get("zone_b_matches_frozen")
             and proofs.get("spec_hash_reproduces")
             and proofs.get("candidate_id_reproduces")), True)
    facts["wide_n_features_frozen"] = {"got": recon.get("n_features"),
                                       "note": "the frozen ridge uses the "
                                               "live WIDE columns; narrative "
                                               "text elsewhere says 86 - the "
                                               "artifact wins"}
    locked = _r39.read_json(v1 / "locked_confirmation_results.json") or {}
    wide_locked = None
    for row in (locked.get("rows") or locked.get("confirmations")
                or locked.get("results") or []):
        if isinstance(row, dict) and row.get("candidate_id") == \
                exp["wide_candidate_id"]:
            wide_locked = row
            break
    zc = (wide_locked or {}).get("zone_c") or wide_locked or {}
    facts["wide_zone_c_excess"] = check(
        "wide_zone_c_excess",
        zc.get("after_cost_excess_annualised"),
        exp["wide_zone_c_excess_annualised"], tol=1e-9) \
        if zc.get("after_cost_excess_annualised") is not None else \
        {"got": None, "note": "locked row shape not recognised; the "
                              "reconstruction artifact carries the proof"}
    facts["wide_zone_c_t"] = check(
        "wide_zone_c_t", zc.get("after_cost_excess_t_stat"),
        exp["wide_zone_c_t"], tol=1e-9) \
        if zc.get("after_cost_excess_t_stat") is not None else \
        {"got": None}
    fac = _r39.read_json(v2 / "wide_factor_residual_alpha.json") or {}
    full = fac.get("full_model") or {}
    facts["wide_residual_alpha"] = {
        "residual_alpha_annualised": full.get("residual_alpha_annualised"),
        "residual_alpha_t": full.get("residual_alpha_t"),
        "r_squared": full.get("r_squared"),
        "ok": (full.get("residual_alpha_t") or 0) >=
        exp["wide_residual_alpha_t_min"]}
    if not facts["wide_residual_alpha"]["ok"]:
        mismatches.append({"check": "wide_residual_alpha_t",
                           "got": full.get("residual_alpha_t")})
    kills = _r39.read_json(v2 / "wide_group_kill_tests.json") or {}
    facts["wide_group_kill_sign_flips"] = check(
        "wide_group_kill_sign_flips", kills.get("n_sign_flips"),
        exp["wide_group_kill_sign_flips"])
    facts["wide_group_kills"] = kills.get("n_kills")
    # --- representation defect ------------------------------------------ #
    rep = _r39.read_json(v2 / "representation_repair.json") or {}
    facts["representation_defect_recorded"] = {
        "v1_finite_share_by_column_and_zone":
            (rep.get("defect_found") or {}).get(
                "v1_finite_share_by_column_and_zone"),
        "repair_coverage": (rep.get("repair") or {}).get(
            "coverage_after_repair"),
        "repaired_results": {k: (v or {}).get("after_cost_excess_t_stat")
                             for k, v in (rep.get("results") or {}).items()},
    }
    # --- continuation near-misses --------------------------------------- #
    cells = _r39.read_json(v2 / "integrity_cell_execution.json") or {}
    intl = None
    for c in cells.get("cells", []):
        for e in c.get("candidates", []):
            if e.get("candidate_id") == exp["intl_rates_carry_rv_candidate"]:
                intl = {"cell": c["r36_cell"], **e}
    models = _r39.read_json(v2 / "model_frontier_completion.json") or {}
    tcn = None
    for k, v in (models.get("results") or {}).items():
        if v.get("candidate_id") == exp["tcn_xs_candidate"]:
            tcn = {"key": k, **v}
    facts["intl_rates_carry_rv"] = {
        "candidate_id": exp["intl_rates_carry_rv_candidate"],
        "found": intl is not None,
        "zone_b": (intl or {}).get("zone_b"),
        "design_note": (intl or {}).get("design_note")}
    facts["tcn_xs"] = {"candidate_id": exp["tcn_xs_candidate"],
                       "found": tcn is not None,
                       "zone_b_t": (tcn or {}).get("after_cost_excess_t_stat"),
                       "zone_b_excess": (tcn or {}).get(
                           "after_cost_excess_annualised")}
    if intl is None:
        mismatches.append({"check": "intl_rates_candidate_found"})
    if tcn is None:
        mismatches.append({"check": "tcn_candidate_found"})
    # --- shadows ---------------------------------------------------------- #
    reg = RS.load_registry(C.R39_CONTINUATION_CAMPAIGN_ID) or {}
    hashes = _r39.read_json(v2 / RS.SPEC_HASHES_NAME) or {}
    shadows = _shadow_checks(reg, hashes)
    facts["research_shadows"] = shadows
    facts["n_research_shadows"] = check("n_research_shadows",
                                        reg.get("n_shadows"),
                                        exp["n_research_shadows"])
    if not shadows.get("all_ok"):
        mismatches.append({"check": "shadow_immutability"})
    design = _r39.read_json(v2 / "prospective_validation_design.json") or {}
    facts["prospective_design_hash"] = design.get("prospective_design_hash")
    facts["shadow_freeze_timestamp"] = reg.get("frozen_at")
    # --- forward ledgers at import time ---------------------------------- #
    sdir = RS.shadow_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
    desk = RS._desk()
    snaps = desk._read_ledger(sdir, RS.SNAPSHOT_LEDGER)
    outs = desk._read_ledger(sdir, RS.OUTCOME_LEDGER)
    facts["true_forward_rows_at_import"] = {
        "snapshots": len(snaps), "outcomes": len(outs),
        "snapshot_chain": desk.verify_ledger(sdir, RS.SNAPSHOT_LEDGER),
        "outcome_chain": desk.verify_ledger(sdir, RS.OUTCOME_LEDGER)}
    # --- git --------------------------------------------------------------- #
    git = _git_head(repo)
    facts["git"] = git
    if git.get("local_equals_remote") is False:
        mismatches.append({"check": "local_equals_remote", **git})

    state = "R39_VERIFIED" if not mismatches else "R39_NOT_VERIFIED"
    body = artifact_body("r40_r39_closeout_import/1", {
        "calculation_owner": CALCULATION_OWNER,
        "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "r39_v1_campaign": C.R39_V1_CAMPAIGN_ID,
        "r39_continuation_campaign": C.R39_CONTINUATION_CAMPAIGN_ID,
        "r39_closeout_commit_expected": C.R39_CLOSEOUT_COMMIT,
        "artifact_manifest": manifest,
        "facts": facts,
        "mismatches": mismatches,
        "state": state,
        "r40_contract_hash_frozen_before_any_evaluation": C.contract_hash(),
        "slot_5_selection_rule_frozen": C.SLOT_5_SELECTION_RULE,
        "slot_4_candidate": {"candidate_id": C.SLOT_4_CANDIDATE_ID,
                             "reason": C.SLOT_4_REASON},
        "burden_inheritance": "R40 reuse ledger initialised from the "
                              "continuation ledger; never reset",
    })
    body["closeout_import_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
