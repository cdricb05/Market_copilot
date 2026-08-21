"""alpha_agent.r33.lockbox - the ONE Release 33 lockbox access owner.

The lockbox is the latest contiguous block of history. It answers one question
honestly: does a candidate SELECTED on earlier evidence still work on evidence
that played no part in selecting it?

That only works under rules that cannot be renegotiated after the result:

* the finalist set is FROZEN and hashed BEFORE the first lockbox execution;
* at most ``MAX_LOCKBOX_FINALISTS`` in total and ``MAX_LOCKBOX_PER_FAMILY`` from
  any one family, so a family cannot buy extra attempts by entering many
  near-identical variants;
* each finalist executes EXACTLY ONCE, logged with its spec hash;
* a finalist that fails may not be revised and resubmitted. This module refuses
  a spec hash it has already served AND refuses a NEW spec hash from a family
  whose attempts are spent - which is the loophole a "small fix and retry" would
  otherwise walk through.

A campaign that could retry the lockbox would be using it as a validation set
with extra steps, and its final number would mean nothing.

This mirrors the Release-31 discipline deliberately. It is not imported from
there because that module is bound to Release 31's research root, budgets and
contract constants; what is shared is the RULE, and both are asserted by their
own release tests.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import r33
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r33.lockbox"
FINALIST_SCHEMA = "r33_lockbox_manifest/1"
MANIFEST_NAME = "lockbox_manifest.json"


class LockboxViolation(RuntimeError):
    """An attempt to use the lockbox outside its frozen contract."""


class Lockbox:
    def __init__(self, *, campaign_id: str = _contract.CAMPAIGN_ID):
        self.campaign_id = campaign_id
        self._finalists: Optional[list] = None
        self._finalist_set_hash: Optional[str] = None
        self._accesses: list = []
        self._selection_basis: Optional[str] = None

    # ------------------------------------------------------------------ #
    def freeze_finalists(self, finalists: list, *, selected_at: str,
                         selection_basis: str) -> dict:
        if self._finalists is not None:
            raise LockboxViolation(
                "the finalist set is already frozen; a new finalist set "
                "requires a new campaign id")
        if len(finalists) > _contract.MAX_LOCKBOX_FINALISTS:
            raise LockboxViolation(
                f"{len(finalists)} finalists exceeds the frozen budget of "
                f"{_contract.MAX_LOCKBOX_FINALISTS}")
        per_family: dict = {}
        for f in finalists:
            fam = str(f["family"])
            per_family[fam] = per_family.get(fam, 0) + 1
            if per_family[fam] > _contract.MAX_LOCKBOX_PER_FAMILY:
                raise LockboxViolation(
                    f"family {fam!r} has {per_family[fam]} finalists, above "
                    f"the limit of {_contract.MAX_LOCKBOX_PER_FAMILY}")
        hashes = [str(f["spec_hash"]) for f in finalists]
        if len(set(hashes)) != len(hashes):
            raise LockboxViolation("duplicate spec hash in the finalist set")

        body = {"contract": FINALIST_SCHEMA, "campaign_id": self.campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                "selected_at": str(selected_at),
                "selection_basis": str(selection_basis),
                "selection_used_lockbox": False,
                "finalists": sorted(finalists,
                                    key=lambda f: str(f["spec_hash"])),
                "count": len(finalists),
                "budget": _contract.MAX_LOCKBOX_FINALISTS,
                "per_family": per_family,
                "per_family_budget": _contract.MAX_LOCKBOX_PER_FAMILY}
        self._finalist_set_hash = r33.sha(body)
        body["finalist_set_hash"] = self._finalist_set_hash
        self._finalists = list(body["finalists"])
        self._selection_basis = str(selection_basis)
        return body

    # ------------------------------------------------------------------ #
    def authorise(self, spec_hash: str, *, family: str, candidate_id: str,
                  at: str) -> dict:
        if self._finalists is None:
            raise LockboxViolation(
                "the lockbox cannot be opened before the finalist set is frozen")
        allowed = {str(f["spec_hash"]) for f in self._finalists}
        if str(spec_hash) not in allowed:
            raise LockboxViolation(
                f"candidate {candidate_id} is not in the frozen finalist set; "
                f"a candidate revised after a lockbox result may not be "
                f"resubmitted")
        if any(a["spec_hash"] == str(spec_hash) for a in self._accesses):
            raise LockboxViolation(
                f"candidate {candidate_id} has already used its single "
                f"lockbox execution")
        fam_used = sum(1 for a in self._accesses if a["family"] == family)
        if fam_used >= _contract.MAX_LOCKBOX_PER_FAMILY:
            raise LockboxViolation(
                f"family {family!r} has spent its "
                f"{_contract.MAX_LOCKBOX_PER_FAMILY} lockbox executions")
        if len(self._accesses) >= _contract.MAX_LOCKBOX_FINALISTS:
            raise LockboxViolation("the lockbox budget is spent")
        entry = {"spec_hash": str(spec_hash), "family": str(family),
                 "candidate_id": str(candidate_id), "at": str(at),
                 "sequence": len(self._accesses) + 1}
        self._accesses.append(entry)
        return entry

    # ------------------------------------------------------------------ #
    @property
    def access_count(self) -> int:
        return len(self._accesses)

    @property
    def finalist_set_hash(self) -> Optional[str]:
        return self._finalist_set_hash

    def every_finalist_accessed_exactly_once(self) -> bool:
        if self._finalists is None:
            return False
        served = [a["spec_hash"] for a in self._accesses]
        expected = [str(f["spec_hash"]) for f in self._finalists]
        return sorted(served) == sorted(expected) and \
            len(set(served)) == len(served)

    def manifest(self, *, created_at: str, results: list) -> dict:
        payload = {
            "calculation_owner": CALCULATION_OWNER,
            "campaign_id": self.campaign_id,
            "created_at": created_at,
            "finalist_set_hash": self._finalist_set_hash,
            "selection_basis": self._selection_basis,
            "selection_used_lockbox": False,
            "finalists": self._finalists or [],
            "accesses": self._accesses,
            "access_count": self.access_count,
            "budget": _contract.MAX_LOCKBOX_FINALISTS,
            "per_family_budget": _contract.MAX_LOCKBOX_PER_FAMILY,
            "one_execution_per_candidate": True,
            "every_finalist_accessed_exactly_once":
                self.every_finalist_accessed_exactly_once(),
            "retuning_after_lockbox_allowed":
                _contract.RETUNING_AFTER_LOCKBOX_ALLOWED,
            "results": results,
        }
        body = r33.artifact_body(FINALIST_SCHEMA, payload)
        body["lockbox_manifest_hash"] = r33.sha(payload)
        return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r33.campaign_dir(campaign_id) / MANIFEST_NAME
