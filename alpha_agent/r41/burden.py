"""alpha_agent.r41.burden - GLOBAL + FAMILY search-burden ledgers.

Blocker 4: the estate inherits 230 effective trials and must not launder
them through a new campaign id — and must also not divide one meaningless
global denominator into every unrelated scientific question. Two ledgers:

* GLOBAL: starts at 230 (verified by closeout_import); every distinct
  candidate scored on any family's ZONE_B adds one.
* FAMILY: one ledger per contract.BURDEN_FAMILIES member; deflated-Sharpe
  denominators for a family's candidates use the FAMILY count, and the
  global count is REPORTED beside it.

Every candidate registers its full lineage (contract.LINEAGE_FIELDS).
Re-scoring the SAME candidate id does not add burden (reuse is counted).
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

from . import CAMPAIGN_ID, campaign_dir, read_json, sha
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r41.burden"
LEDGER_NAME = "r41_search_burden_ledger.json"


def _path(campaign_id: str = CAMPAIGN_ID) -> Path:
    return campaign_dir(campaign_id) / LEDGER_NAME


def _load(campaign_id: str = CAMPAIGN_ID) -> dict:
    body = read_json(_path(campaign_id))
    if body:
        return body
    return {"schema": "r41_search_burden_ledger/1",
            "global_inherited": C.GLOBAL_INHERITED_EFFECTIVE_TRIALS,
            "candidates": {}, "evaluations": 0}


def _save(body: dict, campaign_id: str = CAMPAIGN_ID) -> None:
    p = _path(campaign_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(body, indent=1, sort_keys=True, default=str),
                 encoding="utf-8")


def candidate_id(spec: dict) -> str:
    return "c41_" + sha(spec)[:12]


def record_zone_b(spec: dict, *, family: str,
                  campaign_id: str = CAMPAIGN_ID) -> str:
    """Register one ZONE_B evaluation of ``spec``; returns the candidate id.
    A repeated evaluation of the same id increments its touch count only."""
    if family not in C.BURDEN_FAMILIES:
        raise ValueError("unknown burden family %r" % family)
    cid = candidate_id(spec)
    body = _load(campaign_id)
    row = body["candidates"].get(cid)
    if row is None:
        lineage = {k: spec.get(k) for k in C.LINEAGE_FIELDS}
        row = {"family": family, "lineage": lineage, "touches": 0,
               "first_seen": _dt.datetime.now(_dt.timezone.utc).isoformat()}
        body["candidates"][cid] = row
    row["touches"] += 1
    body["evaluations"] += 1
    _save(body, campaign_id)
    return cid


def summary(campaign_id: str = CAMPAIGN_ID) -> dict:
    body = _load(campaign_id)
    fam = {f: 0 for f in C.BURDEN_FAMILIES}
    for row in body["candidates"].values():
        fam[row["family"]] = fam.get(row["family"], 0) + 1
    distinct = len(body["candidates"])
    return {"global_inherited": body["global_inherited"],
            "r41_distinct_zone_b_candidates": distinct,
            "global_cumulative": body["global_inherited"] + distinct,
            "zone_b_evaluations": body["evaluations"],
            "family_counts": fam,
            "never_reset": True}


def family_count(family: str, campaign_id: str = CAMPAIGN_ID) -> int:
    return summary(campaign_id)["family_counts"].get(family, 0)
