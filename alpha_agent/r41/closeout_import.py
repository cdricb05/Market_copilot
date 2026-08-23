"""alpha_agent.r41.closeout_import - R40_CLOSEOUT_IMPORT.

Release 41 begins by VERIFYING, not trusting, what Release 40 left behind:

* the R40 campaign directory exists and every artifact (and every weight
  file) listed in the R40 handoff manifest hashes to the recorded SHA-256;
* every released repository file in the R40 evidence manifest hashes to the
  committed bytes (the R40 closeout commit is HEAD and local == remote);
* the cumulative search burden reads exactly 230 (194 + 36);
* five research shadows exist, all RESEARCH_SHADOW_ONLY / PROMOTION_ALLOWED
  False, frozen before any outcome, and the TRUE_FORWARD ledgers are intact;
* the R41 contract hash is frozen INTO this artifact before any R41
  evaluation runs.
"""
from __future__ import annotations

import subprocess
import time
from pathlib import Path

from .. import r39 as _r39
from ..r39 import research_shadow as RS
from . import CAMPAIGN_ID, artifact_body, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r41.closeout_import"
ARTIFACT_NAME = "r40_closeout_import.json"


def _manifest_check(man: Path, base: Path) -> dict:
    if not man.exists():
        return {"state": "MANIFEST_MISSING", "path": str(man)}
    ok, bad, missing, weights = 0, [], [], 0
    for line in man.read_text(encoding="utf-8").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2 or len(parts[0]) != 64:
            continue
        h, rel = parts[0].lower(), parts[1].strip()
        if rel.startswith("WEIGHT:"):
            p = Path(rel[len("WEIGHT:"):].strip())
            weights += 1
        else:
            p = base / rel
        if not p.exists():
            missing.append(rel)
            continue
        if _r39.sha_file(p).lower() == h:
            ok += 1
        else:
            bad.append(rel)
    return {"state": "OK" if not bad and not missing else "MISMATCH",
            "manifest": str(man), "verified": ok, "weights_listed": weights,
            "mismatched": bad, "missing": missing}


def _git(repo: Path) -> dict:
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
                "head_is_r40_closeout": head == C.R40_CLOSEOUT_COMMIT}
    except Exception as e:  # pragma: no cover - environment
        return {"state": "GIT_UNAVAILABLE", "error": str(e)[:200]}


def run(campaign_id: str = CAMPAIGN_ID, *, repo_root: Path = None,
        handoff_dir: Path = None) -> dict:
    existing = _r39.read_json(campaign_dir(campaign_id) / ARTIFACT_NAME)
    if existing:
        return existing
    handoff = Path(handoff_dir or C.R40_HANDOFF_DIR)
    root40 = Path(C.R40_RESEARCH_ROOT) / C.R40_CAMPAIGN_ID
    repo = Path(repo_root or Path(__file__).resolve().parents[2])
    exp = C.R40_EXPECTED
    mismatches = []

    def check(name, got, want):
        ok = got == want
        if not ok:
            mismatches.append({"check": name, "got": got, "expected": want})
        return {"got": got, "expected": want, "ok": bool(ok)}

    facts = {}
    facts["r40_dir_exists"] = check("r40_dir_exists", root40.exists(), True)
    art = _manifest_check(handoff / "artifact_hashes.txt", root40)
    facts["artifact_manifest"] = check("artifact_manifest_state",
                                       art.get("state"), "OK")
    facts["artifact_manifest_detail"] = art
    rep = _manifest_check(handoff / "evidence_hashes.txt", repo)
    facts["repo_manifest"] = check("repo_manifest_state", rep.get("state"),
                                   "OK")
    facts["repo_manifest_detail"] = rep
    led = _r39.read_json(root40 / "r40_cumulative_search_ledger.json") or {}
    facts["cumulative_effective_trials"] = check(
        "cumulative_effective_trials",
        int(led.get("CUMULATIVE_R39_R40_EFFECTIVE_TRIALS", -1)),
        exp["cumulative_effective_trials"])
    facts["r39_inherited"] = check(
        "r39_inherited", int(led.get("R39_INHERITED_EFFECTIVE_TRIALS", -1)),
        exp["r39_inherited_effective_trials"])
    facts["r40_new"] = check("r40_new",
                             int(led.get("R40_NEW_EFFECTIVE_TRIALS", -1)),
                             exp["r40_new_effective_trials"])
    fv = _r39.read_json(root40 / "final_verdict.json") or {}
    facts["terminal_states"] = check("terminal_states",
                                     sorted(fv.get("terminal_states") or []),
                                     sorted(exp["terminal_states"]))
    reg = _r39.read_json(root40 / "shadow_registry_v2.json") or {}
    shadows = reg.get("shadows") or []
    facts["n_research_shadows"] = check("n_research_shadows",
                                        int(reg.get("n_shadows", -1)),
                                        exp["n_research_shadows"])
    facts["shadow_ids"] = check("shadow_ids",
                                [s.get("shadow_id") for s in shadows],
                                exp["shadow_ids"])
    facts["shadow_candidates"] = check(
        "shadow_candidates", [s.get("candidate_id") for s in shadows],
        exp["shadow_candidates"])
    flags = all(s.get("research_shadow_only") is True
                and s.get("promotion_allowed") is False
                and s.get("historical_qualification") == "FAIL"
                for s in shadows)
    facts["shadow_flags_non_promotable"] = check(
        "shadow_flags_non_promotable", flags, True)
    body_reg = {k: v for k, v in reg.items()
                if k != "shadow_registry_v2_hash"}
    facts["shadow_registry_hash_reproduces"] = check(
        "shadow_registry_hash_reproduces",
        _r39.sha(body_reg) == reg.get("shadow_registry_v2_hash"), True)
    hashes = _r39.read_json(root40 / "shadow_specification_hashes.json") or {}
    facts["spec_hashes_bind_registry"] = check(
        "spec_hashes_bind_registry",
        hashes.get("registry_hash") == reg.get("shadow_registry_v2_hash"),
        True)
    facts["shadow_freeze_times"] = {s.get("shadow_id"): s.get("frozen_at")
                                    for s in shadows}
    facts["shadow_cadences"] = {s.get("shadow_id"): s.get("decision_cadence")
                                for s in shadows}
    st = _r39.read_json(root40 / "forward_capture_ledger_status.json") or {}
    facts["true_forward_chains_intact"] = check(
        "true_forward_chains_intact", st.get("all_chains_intact"), True)
    facts["true_forward_rows"] = {
        "snapshots_at_r40_close": st.get("true_forward_snapshots"),
        "outcomes_at_r40_close": st.get("true_forward_outcomes")}
    # live ledger verification through the canonical desk primitives
    try:
        sdir = RS.shadow_dir(C.R39_CONTINUATION_CAMPAIGN_ID)
        desk = RS._desk()
        facts["r39_ledgers_live"] = {
            "snapshot_chain": desk.verify_ledger(sdir, RS.SNAPSHOT_LEDGER),
            "outcome_chain": desk.verify_ledger(sdir, RS.OUTCOME_LEDGER),
            "snapshots": len(desk._read_ledger(sdir, RS.SNAPSHOT_LEDGER)),
            "outcomes": len(desk._read_ledger(sdir, RS.OUTCOME_LEDGER))}
    except Exception as e:  # pragma: no cover - environment
        facts["r39_ledgers_live"] = {"error": str(e)[:200]}
    fs = _r39.read_json(root40 / "forward_research_cycle_state.json") or {}
    facts["forward_capture_state"] = fs.get("FORWARD_CAPTURE_STATE")
    facts["first_eligible_forward_date"] = exp["first_eligible_forward_date"]
    git = _git(repo)
    facts["git"] = git
    if git.get("local_equals_remote") is False:
        mismatches.append({"check": "local_equals_remote", **git})
    if git.get("head_is_r40_closeout") is False:
        mismatches.append({"check": "head_is_r40_closeout", **git})

    state = "R40_VERIFIED" if not mismatches else "R40_NOT_VERIFIED"
    body = artifact_body("r41_r40_closeout_import/1", {
        "calculation_owner": CALCULATION_OWNER,
        "imported_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "r40_campaign": C.R40_CAMPAIGN_ID,
        "r40_closeout_commit_expected": C.R40_CLOSEOUT_COMMIT,
        "facts": facts,
        "mismatches": mismatches,
        "state": state,
        "r41_may_start": state == "R40_VERIFIED",
        "r41_contract_hash_frozen_before_any_evaluation": C.contract_hash(),
        "burden_inheritance": "GLOBAL burden initialised at 230; family "
                              "ledgers start at 0 and every ZONE_B "
                              "evaluation adds to both; never reset",
    })
    body["closeout_import_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / ARTIFACT_NAME, body)
    return body
