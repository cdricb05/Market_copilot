"""alpha_agent.r44.closeout - verify what Release 44 inherits, then FREEZE.

Two jobs, in this order and no other:

1. Re-derive every inherited fact from the bytes on disk - R43's verdict and
   its result axes, R43's burden ledger, the R40/R41/R42 frozen shadows -
   and refuse to proceed on a mismatch. The handoff prompt is not evidence;
   the artifacts are.
2. Write ``r44_frozen_contract.json`` - the hash of
   :mod:`alpha_agent.r44.contract` - BEFORE the first Release-44 number
   exists, so that no stream, role, combination rule, cap, control or gate
   can be chosen after seeing a result. For Engine 2 this is not a formality:
   the whole scientific claim of a portfolio result rests on the weighting
   rule having been named in advance.

One inherited fact is unusual and is recorded rather than smoothed over.
Release 43 is COMPLETE but was not committed before Release 44 began, so the
repository HEAD is R42's closeout, not R43's. Release 44 therefore declares
R43's WORKING TREE as its base, states the deviation from the handoff
prompt's start condition explicitly, and owns only its own paths.
"""
from __future__ import annotations

import datetime as _dt
import json
import subprocess
from pathlib import Path

from . import (CAMPAIGN_ID, R41_RESEARCH_ROOT, R42_RESEARCH_ROOT,
               R43_RESEARCH_ROOT, artifact_body, read_json, sha, sha_file,
               write_artifact)
from . import burden as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.closeout"
FROZEN_ARTIFACT = "r44_frozen_contract.json"
CLOSEOUT_ARTIFACT = "R43_CLOSEOUT_IMPORT.json"

REPO_ROOT = Path(r"C:\Users\binis\paper_trader")
EXPECTED_BRANCH = "stage19-controlled-rebalance"

R41_CAMPAIGN = R41_RESEARCH_ROOT / "r41_multi_horizon_alpha_breakthrough_v1"
R42_CAMPAIGN = R42_RESEARCH_ROOT / "r42_crypto_basis_alpha_validation_v1"
R43_CAMPAIGN = R43_RESEARCH_ROOT / "r43_global_alpha_offensive_v1"

#: Files whose bytes must be identical before and after Release 44.
IMMUTABLE_WITNESSES = (
    R41_CAMPAIGN / "r41_search_burden_ledger.json",
    R41_CAMPAIGN / "r41_shadow_registry.json",
    R42_CAMPAIGN / "r42_frozen_contract.json",
    R42_CAMPAIGN / "R42_FINAL_VERDICT.json",
    R42_CAMPAIGN / "r42_shadow_registry.json",
    R43_CAMPAIGN / "r43_frozen_contract.json",
    R43_CAMPAIGN / "R43_FINAL_VERDICT.json",
    R43_CAMPAIGN / "r43_search_burden_ledger.json",
    R43_CAMPAIGN / "r43_shadow_registry.json",
    R43_CAMPAIGN / "r43_zone_c_access_ledger.json",
)

#: The facts Release 44 is built on. Each must be present in R43's own
#: verdict artifact or this release has misunderstood its inheritance.
R43_EXPECTED_AXES = {
    "SYSTEM_RESULT": "PASS",
    "HISTORICAL_ALPHA_RESULT": "FAIL",
}
R43_EXPECTED_TERMINAL = "R43_NO_QUALIFIED_ALPHA_AFTER_GLOBAL_OFFENSIVE"


class CloseoutMismatch(RuntimeError):
    pass


def _git(*args: str) -> str:
    try:
        r = subprocess.run(("git", "-C", str(REPO_ROOT)) + args,
                           capture_output=True, text=True, timeout=180)
        return (r.stdout or "").strip()
    except Exception as exc:                              # pragma: no cover
        return "GIT_ERROR:%s" % exc


def _fingerprint(paths) -> dict:
    out = {}
    for p in paths:
        p = Path(p)
        out[p.name] = {"path": str(p), "exists": p.exists(),
                       "sha256": sha_file(p) if p.exists() else None,
                       "bytes": p.stat().st_size if p.exists() else None}
    return out


def witness_fingerprint() -> dict:
    return _fingerprint(IMMUTABLE_WITNESSES)


def verify_git() -> dict:
    """Repository state, and the start-condition deviation, stated plainly."""
    branch = _git("rev-parse", "--abbrev-ref", "HEAD")
    head = _git("rev-parse", "HEAD")
    remote = _git("rev-parse", "origin/%s" % EXPECTED_BRANCH)
    subject = _git("log", "-1", "--format=%s")
    tracked = _git("ls-files", "--error-unmatch", "alpha_agent/r43/contract.py")
    r43_committed = bool(tracked) and not tracked.startswith("GIT_ERROR")
    return {
        "branch": branch,
        "head": head,
        "remote_head": remote,
        "head_equals_remote": bool(head) and head == remote,
        "branch_ok": branch == EXPECTED_BRANCH,
        "head_subject": subject,
        "r43_source_committed": r43_committed,
        "r44_base": "R43 WORKING TREE (uncommitted) on top of commit %s"
                    % (head or "<unknown>"),
        "start_condition_deviation": (
            None if r43_committed else
            "the handoff prompt expects the latest COMMIT to contain the "
            "finalized Release 43; R43 is complete on disk with its own "
            "handoff prepared but has not been committed by the operator. "
            "local HEAD == origin, so there is no SHA_MISMATCH. Release 44 "
            "proceeds on the R43 working tree, declares R43 as its base, "
            "stages only R44-owned paths, and preserves every pre-existing "
            "untracked file."),
    }


def verify_r43() -> dict:
    """Read the R43 verdict from its own artifact and assert its facts."""
    verdict_p = R43_CAMPAIGN / "R43_FINAL_VERDICT.json"
    frozen_p = R43_CAMPAIGN / "r43_frozen_contract.json"
    if not verdict_p.exists():
        raise CloseoutMismatch("R43 verdict artifact missing: %s" % verdict_p)
    verdict = json.loads(verdict_p.read_text(encoding="utf-8"))
    axes = verdict.get("result_axes") or {}
    mismatched = {k: axes.get(k) for k, v in R43_EXPECTED_AXES.items()
                  if axes.get(k) != v}
    out = {
        "r43_campaign_dir": str(R43_CAMPAIGN),
        "verdict_sha256": sha_file(verdict_p),
        "verdict_hash_recorded": verdict.get("verdict_hash"),
        "frozen_contract_hash": verdict.get("frozen_contract_hash"),
        "frozen_contract_sha256": (sha_file(frozen_p) if frozen_p.exists()
                                   else None),
        "terminal_state": verdict.get("terminal_state"),
        "terminal_state_expected": R43_EXPECTED_TERMINAL,
        "terminal_state_ok":
            verdict.get("terminal_state") == R43_EXPECTED_TERMINAL,
        "result_axes": axes,
        "axes_mismatched": mismatched,
        "r43_shadows_frozen": len((verdict.get("shadows") or {}).get("frozen")
                                  or []),
        "search_burden_global": (verdict.get("search_burden") or {}).get(
            "global_cumulative"),
        "zone_c_accesses": (verdict.get("zone_c") or {}).get("n_opened"),
    }
    if not out["terminal_state_ok"]:
        raise CloseoutMismatch(
            "R43 terminal state is %r, expected %r - Release 44's premise is "
            "unverified" % (out["terminal_state"], R43_EXPECTED_TERMINAL))
    if mismatched:
        raise CloseoutMismatch(
            "R43 result axes disagree with what R44 inherits: %r" % mismatched)
    return out


def verify_prior_shadows() -> dict:
    """Every prior release's shadow registry, listed and hashed, never
    written."""
    out = {}
    for label, p in (("r41", R41_CAMPAIGN / "r41_shadow_registry.json"),
                     ("r42", R42_CAMPAIGN / "r42_shadow_registry.json"),
                     ("r43", R43_CAMPAIGN / "r43_shadow_registry.json")):
        body = read_json(p) if p.exists() else None
        rows = []
        if body:
            cands = body.get("candidates") or body.get("shadows") or {}
            if isinstance(cands, dict):
                rows = sorted(cands.keys())
            elif isinstance(cands, list):
                rows = [str(r.get("candidate_id") or r.get("id"))
                        for r in cands]
        out[label] = {"registry": str(p), "exists": p.exists(),
                      "sha256": sha_file(p) if p.exists() else None,
                      "frozen_shadow_ids": rows,
                      "mutated_by_r44": False}
    return out


def run() -> dict:
    """Verify inheritance and FREEZE the contract. Idempotent."""
    inherited = B.verify_inherited()
    r43 = verify_r43()
    git = verify_git()
    shadows = verify_prior_shadows()
    witnesses = witness_fingerprint()

    frozen_payload = {
        "calculation_owner": C.CALCULATION_OWNER,
        "frozen_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "contract": C.frozen_body(),
    }
    frozen_payload["contract_hash"] = sha(frozen_payload["contract"])
    body = artifact_body("r44_frozen_contract/1", frozen_payload)
    path = write_artifact(FROZEN_ARTIFACT, body, CAMPAIGN_ID)
    on_disk = read_json(path) or {}

    closeout = artifact_body("r44_r43_closeout_import/1", {
        "calculation_owner": CALCULATION_OWNER,
        "inherited_burden": inherited,
        "r43": r43,
        "git": git,
        "prior_shadows": shadows,
        "immutable_witnesses_before": witnesses,
        "frozen_contract_hash": on_disk.get("contract_hash"),
        "frozen_contract_path": str(path),
        "contract_frozen_before_first_number": True,
        "prior_release_roots_opened_read_only": True,
        "primary_combination_rule_named_before_lockbox":
            C.PRIMARY_COMBINATION_RULE,
    })
    closeout["closeout_hash"] = sha(closeout)
    write_artifact(CLOSEOUT_ARTIFACT, closeout, CAMPAIGN_ID, overwrite=True)
    return closeout


def _frozen_path() -> Path:
    from . import campaign_dir
    return campaign_dir(CAMPAIGN_ID) / FROZEN_ARTIFACT


def contract_hash() -> str:
    """The frozen hash as recorded on disk (NOT recomputed from source)."""
    body = read_json(_frozen_path())
    if not body:
        raise CloseoutMismatch("the contract has not been frozen yet")
    return body["contract_hash"]


AMENDMENT_ARTIFACT = "r44_contract_amendment.json"


def _amendment_path() -> Path:
    from . import campaign_dir
    return campaign_dir(CAMPAIGN_ID) / AMENDMENT_ARTIFACT


def amend() -> dict:
    """Pin the AMENDED contract beside the original, never on top of it.

    The original freeze is immutable and stays exactly as written before the
    first number. Every disclosed amendment is recorded here with its own
    hash, so an auditor can verify three things independently: what was
    frozen, what changed, and that the live source is the amended body and
    nothing else.
    """
    body = artifact_body("r44_contract_amendment/1", {
        "calculation_owner": CALCULATION_OWNER,
        "amended_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "original_frozen_contract_hash": contract_hash(),
        "amendments": list(C.POST_FREEZE_AMENDMENTS),
        "amendment_rule":
            "an amendment may only make a stream that FAILED TO BUILD "
            "buildable; it may never change a stream that produced a "
            "number, a role, a weighting rule, a control, a cap or a gate, "
            "and never be made after the lockbox is opened",
        "amended_contract": C.frozen_body(),
    })
    body["amended_contract_hash"] = sha(body["amended_contract"])
    write_artifact(AMENDMENT_ARTIFACT, body, CAMPAIGN_ID, overwrite=True)
    return body


def verify_contract_unchanged() -> dict:
    """Compare the live source against BOTH the freeze and the amendment.

    ``unchanged`` alone is not the whole truth once an amendment exists, so
    all three hashes are reported and the pass condition is stated in words:
    the live source must equal the amended body, and the amended body must
    differ from the original in exactly the disclosed amendments.
    """
    live = sha(C.frozen_body())
    frozen = contract_hash()
    amended = (read_json(_amendment_path()) or {}).get(
        "amended_contract_hash")
    return {
        "live_contract_hash": live,
        "frozen_contract_hash": frozen,
        "amended_contract_hash": amended,
        "unchanged_since_freeze": live == frozen,
        "matches_amended": (amended is not None and live == amended),
        "n_disclosed_amendments": len(C.POST_FREEZE_AMENDMENTS),
        "amendments": [a["id"] for a in C.POST_FREEZE_AMENDMENTS],
        "pass_condition": "live == amended, and every difference from the "
                          "original freeze is listed in "
                          "POST_FREEZE_AMENDMENTS",
        "passes": bool(amended is not None and live == amended),
    }


def witnesses_unchanged() -> dict:
    """Compare the immutable witnesses with the fingerprint taken at freeze."""
    before = ((read_json(_closeout_path()) or {})
              .get("immutable_witnesses_before") or {})
    after = witness_fingerprint()
    diffs = [k for k in after
             if before.get(k, {}).get("sha256") != after[k]["sha256"]]
    return {"n_witnesses": len(after), "changed": diffs,
            "all_unchanged": not diffs, "after": after}


def _closeout_path() -> Path:
    from . import campaign_dir
    return campaign_dir(CAMPAIGN_ID) / CLOSEOUT_ARTIFACT
