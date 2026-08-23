"""alpha_agent.r39.zones - evidence zones, Zone-B reuse ledger, Zone-C lockbox.

Three zones make broad machine search admissible:

* ZONE A (discovery) is searched freely; no Alpha claim can come from it.
* ZONE B (selection) ranks candidates, and every evaluation is COUNTED in a
  reuse ledger - the ledger is the denominator the search-burden correction
  eats, so heavy reuse penalises the campaign rather than flattering it.
* ZONE C (locked confirmation) opens only for a frozen finalist set under
  the Release-31 lockbox discipline: the finalist set is hashed before the
  first access, at most ``MAX_LOCKBOX_CANDIDATES`` finalists with at most
  ``MAX_LOCKBOX_PER_FAMILY`` per family (both constants IMPORTED from the
  r31 contract), one execution per spec hash, no revise-and-retry.

Zone C is labelled ``HISTORICAL_CONFIRMATION_EVIDENCE`` and never
``FRESH_UNSEEN_EVIDENCE``: releases 31-38 read this chronology at the
family level, and the label refuses to pretend otherwise. TRUE_FORWARD
evidence begins only after a frozen forward handoff.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from .. import r39
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r39.zones"
FINALIST_SCHEMA = "r39_lockbox_finalists/1"
ACCESS_SCHEMA = "r39_lockbox_access_log/1"
REUSE_SCHEMA = "r39_zone_b_reuse_ledger/1"

FINALISTS_NAME = "lockbox_finalists.json"
ACCESS_NAME = "lockbox_access_log.json"
REUSE_NAME = "zone_b_reuse_ledger.json"


class LockboxViolation(RuntimeError):
    """An attempt to use Zone C outside its frozen contract."""


def _p(campaign_id: str, name: str) -> Path:
    return r39.campaign_dir(campaign_id) / name


# --------------------------------------------------------------------------- #
# Zone-B reuse ledger
# --------------------------------------------------------------------------- #
def _load_reuse(campaign_id: str) -> dict:
    body = r39.read_json(_p(campaign_id, REUSE_NAME))
    if body:
        return body
    return {"contract": REUSE_SCHEMA, "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "evaluations": {}, "total_evaluations": 0,
            "distinct_candidates": 0}


def record_zone_b(candidate_id: str, *, stage: str,
                  campaign_id: str = C.CAMPAIGN_ID) -> None:
    led = _load_reuse(campaign_id)
    entry = led["evaluations"].setdefault(str(candidate_id),
                                          {"count": 0, "stages": []})
    entry["count"] += 1
    if stage not in entry["stages"]:
        entry["stages"].append(stage)
    led["total_evaluations"] = int(led["total_evaluations"]) + 1
    led["distinct_candidates"] = len(led["evaluations"])
    r39.write_json(_p(campaign_id, REUSE_NAME), led, immutable=False)


def reuse_summary(campaign_id: str = C.CAMPAIGN_ID) -> dict:
    led = _load_reuse(campaign_id)
    return {"total_zone_b_evaluations": int(led["total_evaluations"]),
            "distinct_candidates_on_zone_b": int(led["distinct_candidates"])}


# --------------------------------------------------------------------------- #
# Zone-C lockbox (Release-31 discipline, Release-39 store)
# --------------------------------------------------------------------------- #
def freeze_finalists(finalists: list, *, campaign_id: str = C.CAMPAIGN_ID,
                     selected_at: str, selection_basis: str) -> dict:
    if len(finalists) > C.MAX_LOCKBOX_CANDIDATES:
        raise LockboxViolation(
            "%d finalists exceeds the lockbox budget of %d"
            % (len(finalists), C.MAX_LOCKBOX_CANDIDATES))
    per_family: dict = {}
    for f in finalists:
        fam = str(f["family"])
        per_family[fam] = per_family.get(fam, 0) + 1
        if per_family[fam] > C.MAX_LOCKBOX_PER_FAMILY:
            raise LockboxViolation(
                "family %r has %d finalists, above the per-family limit %d"
                % (fam, per_family[fam], C.MAX_LOCKBOX_PER_FAMILY))
    hashes = [str(f["spec_hash"]) for f in finalists]
    if len(set(hashes)) != len(hashes):
        raise LockboxViolation("duplicate spec hash in the finalist set")
    body = {"contract": FINALIST_SCHEMA, "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER,
            "selected_at": str(selected_at),
            "selection_basis": str(selection_basis),
            "selection_used_zone_c": False,
            "finalists": sorted(finalists,
                                key=lambda f: str(f["spec_hash"])),
            "count": len(finalists),
            "budget": C.MAX_LOCKBOX_CANDIDATES,
            "per_family": per_family,
            "per_family_budget": C.MAX_LOCKBOX_PER_FAMILY,
            "zone_c_label": C.ZONE_C_EVIDENCE_LABEL}
    body["finalist_set_hash"] = r39.sha(body)
    body["safety_block"] = r39.safety_block()
    existing = r39.read_json(_p(campaign_id, FINALISTS_NAME))
    if existing is not None and \
            existing.get("finalist_set_hash") != body["finalist_set_hash"]:
        raise LockboxViolation(
            "the finalist set is already frozen with a different hash; a "
            "new set requires a new campaign id")
    r39.write_json(_p(campaign_id, FINALISTS_NAME), body)
    return body


def load_finalists(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r39.read_json(_p(campaign_id, FINALISTS_NAME))


def _access_log(campaign_id: str) -> dict:
    existing = r39.read_json(_p(campaign_id, ACCESS_NAME))
    if existing:
        return existing
    return {"contract": ACCESS_SCHEMA, "campaign_id": campaign_id,
            "calculation_owner": CALCULATION_OWNER, "accesses": [],
            "access_count": 0, "budget": C.MAX_LOCKBOX_CANDIDATES}


def access_count(campaign_id: str = C.CAMPAIGN_ID) -> int:
    return int(_access_log(campaign_id).get("access_count") or 0)


def authorise(spec_hash: str, *, campaign_id: str = C.CAMPAIGN_ID,
              family: str, candidate_id: str, at: str) -> dict:
    frozen = load_finalists(campaign_id)
    if frozen is None:
        raise LockboxViolation(
            "Zone C cannot be opened before the finalist set is frozen")
    allowed = {str(f["spec_hash"]) for f in frozen["finalists"]}
    if str(spec_hash) not in allowed:
        raise LockboxViolation(
            "candidate %s is not in the frozen finalist set; a candidate "
            "revised after a Zone-C result may not be resubmitted"
            % candidate_id)
    log = _access_log(campaign_id)
    served = {a["spec_hash"] for a in log["accesses"]}
    if str(spec_hash) in served:
        raise LockboxViolation(
            "candidate %s has already used its single Zone-C execution"
            % candidate_id)
    fam_used = sum(1 for a in log["accesses"] if a.get("family") == family)
    if fam_used >= C.MAX_LOCKBOX_PER_FAMILY:
        raise LockboxViolation(
            "family %r has spent its %d Zone-C executions"
            % (family, C.MAX_LOCKBOX_PER_FAMILY))
    if int(log["access_count"]) >= C.MAX_LOCKBOX_CANDIDATES:
        raise LockboxViolation(
            "the Zone-C budget of %d executions is spent"
            % C.MAX_LOCKBOX_CANDIDATES)
    log["accesses"].append({"spec_hash": str(spec_hash),
                            "family": str(family),
                            "candidate_id": str(candidate_id),
                            "at": str(at),
                            "sequence": int(log["access_count"]) + 1})
    log["access_count"] = int(log["access_count"]) + 1
    r39.write_json(_p(campaign_id, ACCESS_NAME), log, immutable=False)
    return log["accesses"][-1]
