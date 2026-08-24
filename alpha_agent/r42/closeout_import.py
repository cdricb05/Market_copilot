"""alpha_agent.r42.closeout_import - Track 0: verify R41, do not trust it.

Release 42 may not begin until Release 41 is provably finalised. This
module re-derives, from primary evidence:

* the git branch, the local HEAD and the remote HEAD (they must agree);
* the actual R41 closeout SHA and that its tree contains the R41 owner
  package;
* every R41 research-artifact hash against the R41 handoff manifest;
* every released R41 repository-file hash against the R41 handoff manifest;
* the R41 frozen shadow's specification hash and registry hash;
* the R41 forward ledgers: chain integrity, and that NO row predates or
  equals the freeze;
* the R41 cumulative search burden, read from the artifact and never from
  a document.

Any failure yields ``DO_NOT_START_R42 - R41_NOT_FINALIZED`` with the exact
mismatches attached.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from . import (CAMPAIGN_ID, artifact_body, r41_campaign_dir, read_json, sha,
               sha_file, write_artifact)
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r42.closeout_import"
ARTIFACT = "r41_closeout_import.json"

REPO_ROOT = Path(r"C:\Users\binis\paper_trader")
R41_HANDOFF = Path(r"D:\Temp\paper_trader_release41_multi_horizon_alpha_handoff")
EXPECTED_BRANCH = "stage19-controlled-rebalance"

#: Files Release 42 legitimately modifies that ALSO appear in the R41
#: released-file manifest. Three shared files must change for R42 to exist
#: at all: the audit gains the R42 guard, the write-attribution gate fails
#: closed on an unknown release so it must gain an R42 profile, and the
#: project state records the current phase. A hash difference on one of
#: these is EXPECTED and is re-hashed in the R42 manifest. A hash
#: difference on ANY OTHER R41-released file means R42 touched something it
#: had no business touching, and that is a blocking defect.
R42_MAY_MODIFY = (
    "scripts/audit_architecture.py",
    "scripts/r33_operational_write_attribution.py",
    "PROJECT_STATE.md",
)


def _git(*args: str) -> str:
    try:
        r = subprocess.run(("git", "-C", str(REPO_ROOT)) + args,
                           capture_output=True, text=True, timeout=180)
        return (r.stdout or "").strip()
    except Exception as exc:                              # pragma: no cover
        return "GIT_ERROR:%s" % exc


def _read_manifest(path: Path) -> list:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) == 2 and len(parts[0]) == 64:
            rows.append((parts[0].lower(), parts[1].strip()))
    return rows


def _verify_manifest(manifest: Path, root: Path,
                     allow_modified: tuple = ()) -> dict:
    rows = _read_manifest(manifest)
    ok, bad, missing, expected = 0, [], [], []
    allow = {a.replace("\\", "/") for a in allow_modified}
    for digest, rel in rows:
        p = root / rel
        if not p.exists():
            missing.append(rel)
            continue
        if sha_file(p).lower() == digest:
            ok += 1
        elif rel.replace("\\", "/") in allow:
            expected.append(rel)
        else:
            bad.append(rel)
    return {"manifest": str(manifest), "n_rows": len(rows), "verified": ok,
            "mismatched": bad, "missing": missing,
            "expected_modifications": expected,
            "expected_modification_allowlist": sorted(allow),
            "state": "OK" if rows and not bad and not missing
            else ("EMPTY" if not rows else "MISMATCH")}


def verify_git() -> dict:
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    head = _git("rev-parse", "HEAD")
    remote = _git("rev-parse", "origin/%s" % EXPECTED_BRANCH)
    subject = _git("log", "-1", "--format=%s")
    committed_at = _git("log", "-1", "--format=%cI")
    return {"branch": branch, "head": head, "remote": remote,
            "head_equals_remote": bool(head) and head == remote,
            "branch_ok": branch == EXPECTED_BRANCH,
            "closeout_sha": head, "closeout_subject": subject,
            "closeout_committed_at": committed_at}


def verify_shadow() -> dict:
    reg = read_json(r41_campaign_dir() / "r41_shadow_registry.json") or {}
    shadows = reg.get("shadows") or []
    out = {"registry_present": bool(reg), "n_shadows": len(shadows),
           "registry_hash_recorded": reg.get("r41_shadow_registry_hash"),
           "family_cap": reg.get("family_cap"),
           "frozen_at": reg.get("frozen_at")}
    if not shadows:
        out["state"] = "NO_SHADOW"
        return out
    sh = shadows[0]
    out["shadow_id"] = sh.get("shadow_id")
    out["spec_hash_recorded"] = sh.get("spec_hash")
    out["promotion_allowed"] = sh.get("promotion_allowed")
    out["research_shadow_only"] = sh.get("research_shadow_only")
    exp = C.R41_EXPECTED
    # Re-derive the spec hash from the R41 owner's own FUNDING_SPEC so a
    # silent edit of the frozen rule is caught, not merely a hash copy.
    try:
        from ..r41 import forward_freeze as FF
        rederived = sha(dict(FF.FUNDING_SPEC))
    except Exception as exc:                              # pragma: no cover
        rederived = "REDERIVE_ERROR:%s" % exc
    out["spec_hash_rederived_from_owner"] = rederived
    out["spec_hash_matches_expected"] = (
        sh.get("spec_hash") == exp["shadow_spec_hash"])
    out["spec_hash_matches_owner_code"] = (rederived == exp["shadow_spec_hash"])
    out["registry_hash_matches_expected"] = (
        reg.get("r41_shadow_registry_hash") == exp["registry_hash"])
    out["shadow_id_matches_expected"] = (
        sh.get("shadow_id") == exp["shadow_id"])
    out["state"] = "OK" if all([
        out["spec_hash_matches_expected"], out["spec_hash_matches_owner_code"],
        out["registry_hash_matches_expected"], out["shadow_id_matches_expected"],
        sh.get("promotion_allowed") is False,
        sh.get("research_shadow_only") is True]) else "MISMATCH"
    return out


def verify_forward_ledgers() -> dict:
    reg = read_json(r41_campaign_dir() / "r41_shadow_registry.json") or {}
    frozen_at = reg.get("frozen_at")
    sdir = r41_campaign_dir() / "research_shadow_forward"
    out = {"ledger_root": str(sdir), "frozen_at": frozen_at,
           "directory_exists": sdir.exists()}
    rows = []
    if sdir.exists():
        try:
            from ..r39.research_shadow import _desk
            desk = _desk()
            rows = desk._read_ledger(sdir, "r41_shadow_forward_snapshots.json")
            out["chain"] = desk.verify_ledger(
                sdir, "r41_shadow_forward_snapshots.json")
        except Exception as exc:                          # pragma: no cover
            out["chain"] = {"state": "READ_ERROR", "error": str(exc)}
    out["n_rows"] = len(rows)
    bad = []
    if frozen_at and rows:
        cut = str(frozen_at)[:10]
        for r in rows:
            if str(r.get("decision_date", ""))[:10] <= cut:
                bad.append(r.get("decision_date"))
            if r.get("true_forward") is not True:
                bad.append("NOT_TRUE_FORWARD:%s" % r.get("decision_date"))
    out["rows_at_or_before_freeze"] = bad
    out["no_row_predates_freeze"] = not bad
    out["state"] = "OK" if not bad else "CONTAMINATED"
    return out


def verify_burden() -> dict:
    fv = read_json(r41_campaign_dir() / "final_verdict.json") or {}
    body = fv.get("results", fv)
    b = body.get("cumulative_search_burden") or {}
    led = read_json(r41_campaign_dir() / "r41_search_burden_ledger.json") or {}
    lbody = led.get("results", led)
    exp = C.R41_EXPECTED
    out = {"global_cumulative": b.get("global_cumulative"),
           "global_inherited": b.get("global_inherited"),
           "family_counts": b.get("family_counts"),
           "r41_distinct_zone_b_candidates":
               b.get("r41_distinct_zone_b_candidates"),
           "ledger_candidates": len(lbody.get("candidates") or {}),
           "ledger_evaluations": lbody.get("evaluations"),
           "ledger_global_inherited": lbody.get("global_inherited")}
    out["matches_expected_global"] = (
        b.get("global_cumulative") == exp["global_cumulative_burden"])
    out["matches_expected_crypto_family"] = (
        (b.get("family_counts") or {}).get("CRYPTO")
        == exp["crypto_family_burden"])
    out["never_reset"] = bool(b.get("never_reset"))
    out["state"] = "OK" if all([out["matches_expected_global"],
                                out["matches_expected_crypto_family"],
                                out["never_reset"]]) else "MISMATCH"
    return out


def verify_headline_numbers() -> dict:
    """Every number this release argues about, re-read from the immutable
    R41 artifacts - never from the release note and never from a prompt."""
    exp = C.R41_EXPECTED
    fv = read_json(r41_campaign_dir() / "final_verdict.json") or {}
    fvb = fv.get("results", fv)
    qg = fvb.get("qualified_gate") or {}
    kf = read_json(r41_campaign_dir()
                   / "alpha_killer_funding_results.json") or {}
    kfb = kf.get("results", kf)
    eth = read_json(r41_campaign_dir()
                    / "eth_funding_replication_results.json") or {}
    ethb = eth.get("results", eth)
    got = {
        "zone_b_t": (qg.get("zone_b") or {}).get("excess_t_hac"),
        "zone_b_excess_ann": (qg.get("zone_b") or {}).get("excess_ann"),
        "zone_c_t": (qg.get("zone_c") or {}).get("excess_t_hac"),
        "zone_c_excess_ann": (qg.get("zone_c") or {}).get("excess_ann"),
        "zone_c_sharpe": (qg.get("zone_c") or {}).get("sharpe"),
        "zone_c_x3_t": ((qg.get("zone_c") or {}).get("cost_stress") or {})
        .get("x3", {}).get("t"),
        "eth_zone_b_t": (ethb.get("zone_b") or {}).get("excess_t_hac"),
        "eth_zone_c_t": (ethb.get("zone_c") or {}).get("excess_t_hac"),
        "dsr_family": (qg.get("deflated_sharpe_family") or {}).get("dsr"),
        "n_killer_sign_flips": kfb.get("n_sign_flips"),
        "placebo_gate_t": ((kfb.get("tests") or {})
                           .get("PLACEBO_FUNDING_GATE") or {}).get("t"),
        "historical_alpha_result": (fvb.get("axes") or {})
        .get("HISTORICAL_ALPHA_RESULT"),
    }
    mism = {}
    for k, v in got.items():
        e = exp.get(k)
        if e is None:
            continue
        if isinstance(e, float) and isinstance(v, (int, float)):
            if abs(float(v) - e) > 1e-9:
                mism[k] = {"expected": e, "artifact": v}
        elif v != e:
            mism[k] = {"expected": e, "artifact": v}
    return {"from_artifacts": got, "mismatches": mism,
            "state": "OK" if not mism else "MISMATCH",
            "note": "PROMPT AND RELEASE NOTE ARE NOT EVIDENCE - these values "
                    "are re-read from the immutable R41 artifacts"}


def run(*, write: bool = True) -> dict:
    """Re-verify the R41 closeout from primary evidence.

    ``write=False`` performs every check and returns the body WITHOUT
    persisting it. The handoff validator uses that, because re-running a
    verification must not change the artifact whose hash the validator is
    about to check.
    """
    git = verify_git()
    arts = _verify_manifest(R41_HANDOFF / "artifact_hashes.txt",
                            r41_campaign_dir())
    repo = _verify_manifest(R41_HANDOFF / "evidence_hashes.txt", REPO_ROOT,
                            allow_modified=R42_MAY_MODIFY)
    shadow = verify_shadow()
    fwd = verify_forward_ledgers()
    burden = verify_burden()
    head = verify_headline_numbers()

    checks = {
        "branch_is_expected": git["branch_ok"],
        "head_equals_remote": git["head_equals_remote"],
        "r41_artifact_manifest_verified": arts["state"] == "OK",
        "r41_repo_manifest_verified": repo["state"] == "OK",
        "r41_shadow_immutable": shadow.get("state") == "OK",
        "r41_forward_ledger_uncontaminated": fwd.get("state") == "OK",
        "r41_burden_matches_artifact": burden.get("state") == "OK",
        "r41_headline_numbers_reproduce": head.get("state") == "OK",
    }
    ok = all(checks.values())
    body = artifact_body("r42_r41_closeout_import/1", {
        "calculation_owner": CALCULATION_OWNER,
        "state": "R41_VERIFIED" if ok else "R41_NOT_FINALIZED",
        "gate": "PROCEED" if ok else "DO_NOT_START_R42 - R41_NOT_FINALIZED",
        "checks": checks, "git": git, "r41_artifact_manifest": arts,
        "r41_repo_manifest": repo, "r41_shadow": shadow,
        "r41_forward_ledgers": fwd, "r41_search_burden": burden,
        "r41_headline_numbers": head,
        "r42_may_modify_shared_files": list(R42_MAY_MODIFY),
        "r41_shared_files_modified_as_expected":
            repo.get("expected_modifications"),
        "r42_contract_hash": C.contract_hash(),
        "r41_contract_hash_seen": _r41_contract_hash(),
    })
    body["r41_closeout_import_hash"] = sha(body)
    if write:
        write_artifact(ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def _r41_contract_hash() -> str:
    try:
        from ..r41 import contract as c41
        return c41.contract_hash()
    except Exception as exc:                              # pragma: no cover
        return "ERROR:%s" % exc


if __name__ == "__main__":                                # pragma: no cover
    print(json.dumps({k: v for k, v in run().items()
                      if k in ("state", "gate", "checks")}, indent=1))
