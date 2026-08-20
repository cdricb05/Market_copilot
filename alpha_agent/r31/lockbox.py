"""alpha_agent.r31.lockbox - the ONE Release 31 lockbox access owner.

The lockbox is the latest contiguous block of history. It exists to answer one
question honestly: does a candidate that was SELECTED on earlier evidence still
work on evidence that played no part in selecting it?

That only works if the lockbox is touched under rules that cannot be renegotiated
after the result:

* the finalist set is FROZEN and hashed BEFORE the first lockbox execution;
* at most 12 finalists in total, at most 2 from any one family, so a family
  cannot buy extra attempts by entering many near-identical variants;
* each finalist runs EXACTLY ONCE, and the access is logged with the timestamp
  and the spec hash;
* a finalist that fails may not be revised and re-submitted to the same lockbox.
  This module refuses a spec hash it has already served AND refuses a NEW spec
  hash from a family whose lockbox attempts are spent, which is the loophole a
  "small fix and retry" would otherwise take.

A campaign that could retry the lockbox would be using it as a validation set
with extra steps, and its final number would mean nothing.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import r31
from . import contract as _contract
from .registry import _write_mutable

CALCULATION_OWNER = "alpha_agent.r31.lockbox"
FINALIST_SCHEMA = "r31_lockbox_finalists/1"
ACCESS_SCHEMA = "r31_lockbox_access_log/1"
RESULT_SCHEMA = "r31_lockbox_results/1"

FINALISTS_NAME = "lockbox_finalists.json"
ACCESS_NAME = "lockbox_access_log.json"
RESULTS_NAME = "lockbox_results.json"


class LockboxViolation(RuntimeError):
    """An attempt to use the lockbox outside its frozen contract."""


def finalists_path(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / FINALISTS_NAME


def access_path(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / ACCESS_NAME


def results_path(campaign_id: str) -> Path:
    return r31.campaign_dir(campaign_id) / RESULTS_NAME


# --------------------------------------------------------------------------- #
# Freezing the finalist set
# --------------------------------------------------------------------------- #
def freeze_finalists(finalists: list, *, campaign_id: str = _contract.CAMPAIGN_ID,
                     selected_at: str, selection_basis: str) -> dict:
    """Freeze the finalist set. Refuses to widen an already-frozen set.

    ``finalists`` is a list of ``{"candidate_id", "spec_hash", "family",
    "sample", "horizon_sessions"}`` chosen on DISCOVERY and VALIDATION evidence
    only.
    """
    if len(finalists) > _contract.MAX_LOCKBOX_CANDIDATES:
        raise LockboxViolation(
            "%d finalists exceeds the frozen lockbox budget of %d"
            % (len(finalists), _contract.MAX_LOCKBOX_CANDIDATES))
    per_family: dict = {}
    for f in finalists:
        fam = str(f["family"])
        per_family[fam] = per_family.get(fam, 0) + 1
        if per_family[fam] > _contract.MAX_LOCKBOX_PER_FAMILY:
            raise LockboxViolation(
                "family %r has %d finalists, above the frozen per-family limit "
                "of %d" % (fam, per_family[fam], _contract.MAX_LOCKBOX_PER_FAMILY))
    hashes = [str(f["spec_hash"]) for f in finalists]
    if len(set(hashes)) != len(hashes):
        raise LockboxViolation("duplicate specification hash in the finalist set")

    body = {
        "contract": FINALIST_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "selected_at": str(selected_at),
        "selection_basis": str(selection_basis),
        "selection_used_lockbox": False,
        "finalists": sorted(finalists, key=lambda f: str(f["spec_hash"])),
        "count": len(finalists),
        "budget": _contract.MAX_LOCKBOX_CANDIDATES,
        "per_family": per_family,
        "per_family_budget": _contract.MAX_LOCKBOX_PER_FAMILY,
    }
    body["finalist_set_hash"] = r31.sha(body)
    body.update(r31.safety_block())

    existing = r31.read_json(finalists_path(campaign_id))
    if existing is not None and existing.get("finalist_set_hash") != body["finalist_set_hash"]:
        raise LockboxViolation(
            "the finalist set is already frozen with a different hash; a new "
            "finalist set requires a new campaign id")
    r31.write_json(finalists_path(campaign_id), body)
    return body


def load_finalists(campaign_id: str = _contract.CAMPAIGN_ID) -> Optional[dict]:
    return r31.read_json(finalists_path(campaign_id))


# --------------------------------------------------------------------------- #
# Access control
# --------------------------------------------------------------------------- #
def _access_log(campaign_id: str) -> dict:
    existing = r31.read_json(access_path(campaign_id))
    if existing:
        return existing
    return {"contract": ACCESS_SCHEMA, "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER, "accesses": [],
            "access_count": 0, "budget": _contract.MAX_LOCKBOX_CANDIDATES}


def access_count(campaign_id: str = _contract.CAMPAIGN_ID) -> int:
    return int(_access_log(campaign_id).get("access_count") or 0)


def authorise(spec_hash: str, *, campaign_id: str = _contract.CAMPAIGN_ID,
              family: str, candidate_id: str, at: str) -> dict:
    """Authorise ONE lockbox execution, or refuse with a named reason."""
    frozen = load_finalists(campaign_id)
    if frozen is None:
        raise LockboxViolation(
            "the lockbox cannot be opened before the finalist set is frozen")
    allowed = {str(f["spec_hash"]): f for f in frozen["finalists"]}
    if str(spec_hash) not in allowed:
        raise LockboxViolation(
            "candidate %s is not in the frozen finalist set; a candidate revised "
            "after a lockbox result may not be resubmitted" % candidate_id)

    log = _access_log(campaign_id)
    served = {a["spec_hash"] for a in log["accesses"]}
    if str(spec_hash) in served:
        raise LockboxViolation(
            "candidate %s has already used its single lockbox execution"
            % candidate_id)
    fam_used = sum(1 for a in log["accesses"] if a.get("family") == family)
    if fam_used >= _contract.MAX_LOCKBOX_PER_FAMILY:
        raise LockboxViolation(
            "family %r has spent its %d lockbox executions"
            % (family, _contract.MAX_LOCKBOX_PER_FAMILY))
    if int(log["access_count"]) >= _contract.MAX_LOCKBOX_CANDIDATES:
        raise LockboxViolation(
            "the lockbox budget of %d executions is spent"
            % _contract.MAX_LOCKBOX_CANDIDATES)

    log["accesses"].append({"spec_hash": str(spec_hash), "family": str(family),
                            "candidate_id": str(candidate_id), "at": str(at),
                            "sequence": int(log["access_count"]) + 1})
    log["access_count"] = int(log["access_count"]) + 1
    log.update(r31.safety_block())
    _write_mutable(access_path(campaign_id), log)
    return log["accesses"][-1]


def persist_results(results: list, *, campaign_id: str = _contract.CAMPAIGN_ID,
                    finalist_set_hash: str) -> Path:
    body = {
        "contract": RESULT_SCHEMA,
        "campaign_id": campaign_id,
        "calculation_owner": CALCULATION_OWNER,
        "finalist_set_hash": finalist_set_hash,
        "access_count": access_count(campaign_id),
        "budget": _contract.MAX_LOCKBOX_CANDIDATES,
        "results": results,
        "one_execution_per_candidate": True,
        "no_redesign_and_retry": True,
    }
    body["lockbox_results_hash"] = r31.sha(body)
    body.update(r31.safety_block())
    return r31.write_json(results_path(campaign_id), body)
