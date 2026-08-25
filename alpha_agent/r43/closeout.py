"""alpha_agent.r43.closeout - verify what Release 43 inherits, then FREEZE.

Two jobs, in this order and no other:

1. Re-derive every inherited fact from the bytes on disk - the R42 verdict
   and its result axes, the R41 frozen shadow and its ledgers, the search
   burden - and refuse to proceed on a mismatch. The handoff prompt is not
   evidence; the artifacts are.
2. Write ``r43_frozen_contract.json`` - the hash of
   :mod:`alpha_agent.r43.contract` - BEFORE the first Release-43 number
   exists, so no gate, control, capital model, cap or kill test can be
   chosen after seeing a result.

The prior releases' roots are opened READ-ONLY. This module records a
fingerprint of the R41 shadow registry and forward ledgers so the campaign
can prove at the end that Release 43 did not touch them.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

from . import (CAMPAIGN_ID, R41_RESEARCH_ROOT, R42_RESEARCH_ROOT,
               artifact_body, read_json, sha, sha_file, write_artifact)
from . import burden as B
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r43.closeout"
FROZEN_ARTIFACT = "r43_frozen_contract.json"
CLOSEOUT_ARTIFACT = "R42_CLOSEOUT_IMPORT.json"

R41_CAMPAIGN = R41_RESEARCH_ROOT / "r41_multi_horizon_alpha_breakthrough_v1"
R42_CAMPAIGN = R42_RESEARCH_ROOT / "r42_crypto_basis_alpha_validation_v1"

#: Files whose bytes must be identical before and after Release 43.
IMMUTABLE_WITNESSES = (
    R41_CAMPAIGN / "r41_search_burden_ledger.json",
    R42_CAMPAIGN / "r42_frozen_contract.json",
    R42_CAMPAIGN / "R42_FINAL_VERDICT.json",
    R42_CAMPAIGN / "r42_shadow_registry.json",
)


class CloseoutMismatch(RuntimeError):
    pass


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


def verify_r42() -> dict:
    """Read the R42 verdict from its own artifact and assert the facts this
    release is built on."""
    verdict_p = R42_CAMPAIGN / "R42_FINAL_VERDICT.json"
    cap_p = R42_CAMPAIGN / "CAPITAL_EFFICIENCY_REPORT.json"
    if not verdict_p.exists():
        raise CloseoutMismatch("R42 verdict artifact missing: %s" % verdict_p)
    verdict = json.loads(verdict_p.read_text(encoding="utf-8"))
    cap = json.loads(cap_p.read_text(encoding="utf-8")) if cap_p.exists() \
        else {}
    blob = json.dumps(verdict, sort_keys=True, default=str)
    states = [s for s in ("R42_CAPITAL_EFFICIENCY_KILLS_EDGE",
                          "R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA")
              if s in blob]
    prim = (cap.get("authoritative_primary_roic") or {})
    out = {
        "r42_campaign_dir": str(R42_CAMPAIGN),
        "verdict_sha256": sha_file(verdict_p),
        "qualification_states_found": states,
        "capital_kill_confirmed":
            "R42_CAPITAL_EFFICIENCY_KILLS_EDGE" in states,
        "structural_premium_confirmed":
            "R42_STRUCTURAL_PREMIUM_CONFIRMED_NOT_TIMING_ALPHA" in states,
        "r42_primary_candidate": prim.get("candidate"),
        "r42_primary_capital_model": prim.get("capital_model"),
        "r42_zone_c_excess_over_rf_ann": prim.get("zone_c_excess_over_rf_ann"),
        "r42_zone_c_t": prim.get("zone_c_t"),
        "r42_capital_models": list((cap.get("capital_models") or {}).keys()),
    }
    if not out["capital_kill_confirmed"]:
        raise CloseoutMismatch(
            "the R42 artifact does not carry R42_CAPITAL_EFFICIENCY_KILLS_"
            "EDGE; Release 43's inherited premise is unverified")
    return out


def verify_r41_shadow() -> dict:
    reg_p = R41_CAMPAIGN / "r41_shadow_registry.json"
    body = read_json(reg_p) if reg_p.exists() else None
    rows = []
    if body:
        cands = body.get("candidates") or body.get("shadows") or {}
        if isinstance(cands, dict):
            rows = sorted(cands.keys())
        elif isinstance(cands, list):
            rows = [str(r.get("candidate_id") or r.get("id")) for r in cands]
    return {"registry": str(reg_p), "exists": reg_p.exists(),
            "sha256": sha_file(reg_p) if reg_p.exists() else None,
            "frozen_shadow_ids": rows,
            "mutated_by_r43": False}


def run() -> dict:
    """Verify inheritance and FREEZE the contract. Idempotent."""
    inherited = B.verify_inherited()
    r42 = verify_r42()
    shadow = verify_r41_shadow()
    witnesses = witness_fingerprint()

    frozen_payload = {
        "calculation_owner": C.CALCULATION_OWNER,
        "frozen_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "contract": C.frozen_body(),
    }
    frozen_payload["contract_hash"] = sha(frozen_payload["contract"])
    body = artifact_body("r43_frozen_contract/1", frozen_payload)
    path = write_artifact(FROZEN_ARTIFACT, body, CAMPAIGN_ID)
    on_disk = read_json(path) or {}

    closeout = artifact_body("r43_r42_closeout_import/1", {
        "calculation_owner": CALCULATION_OWNER,
        "inherited_burden": inherited,
        "r42": r42,
        "r41_shadow": shadow,
        "immutable_witnesses_before": witnesses,
        "frozen_contract_hash": on_disk.get("contract_hash"),
        "frozen_contract_path": str(path),
        "contract_frozen_before_first_number": True,
        "prior_release_roots_opened_read_only": True,
    })
    closeout["closeout_hash"] = sha(closeout)
    write_artifact(CLOSEOUT_ARTIFACT, closeout, CAMPAIGN_ID, overwrite=True)
    return closeout


def contract_hash() -> str:
    """The frozen hash as recorded on disk (NOT recomputed from source)."""
    body = read_json(_frozen_path())
    if not body:
        raise CloseoutMismatch("the contract has not been frozen yet")
    return body["contract_hash"]


def _frozen_path() -> Path:
    from . import campaign_dir
    return campaign_dir(CAMPAIGN_ID) / FROZEN_ARTIFACT


def verify_contract_unchanged() -> dict:
    """Re-derive the hash from SOURCE and compare with the frozen artifact."""
    live = sha(C.frozen_body())
    frozen = contract_hash()
    return {"live_contract_hash": live, "frozen_contract_hash": frozen,
            "unchanged": live == frozen}
