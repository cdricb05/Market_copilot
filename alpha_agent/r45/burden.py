"""alpha_agent.r45.burden - the search burden ledger, which only ever grows.

Release 45 inherits 310 headline / 312 conservative effective trials from
Release 44 and may not reset either number. The interesting question this
release forces is what a REPLICATION costs.

The answer declared in the contract, before any result: testing ONE
predeclared mechanism in many markets is ONE confirmation programme, not one
trial per market. The rule, its direction, its entry, its hold and its cost
model were all fixed by Release 44; Release 45 chose no parameter, so it
paid for no search. It is charged 1.

The moment anything moves - a hold, a delay, an event subset, a hedge ratio,
a model family - the exemption is gone and every cell is charged, which is
exactly what :func:`charge` does for every other family.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from . import contract as C

CALCULATION_OWNER = "alpha_agent.r45.burden"
LEDGER_NAME = "r45_search_burden_ledger.json"


class BurdenLaundering(RuntimeError):
    """Raised if anything tries to lower an inherited count."""


def _path() -> Path:
    return C.ARTIFACT_DIR / LEDGER_NAME


def _blank() -> dict:
    return {
        "schema": "r45_search_burden_ledger/1",
        "campaign_id": C.CAMPAIGN_ID,
        "calculation_owner": CALCULATION_OWNER,
        "global_inherited": C.INHERITED_GLOBAL_BURDEN,
        "global_inherited_conservative":
            C.INHERITED_GLOBAL_BURDEN_CONSERVATIVE,
        "frozen_replication_is_one_trial": C.FROZEN_REPLICATION_IS_ONE_TRIAL,
        "candidates": {}, "by_family": {f: 0 for f in C.BURDEN_FAMILIES},
        "evaluations": 0,
    }


def _load() -> dict:
    p = _path()
    if not p.exists():
        return _blank()
    body = json.loads(p.read_text(encoding="utf-8"))
    if body.get("global_inherited", 0) < C.INHERITED_GLOBAL_BURDEN:
        raise BurdenLaundering(
            f"ledger claims {body.get('global_inherited')} inherited trials, "
            f"contract inherits {C.INHERITED_GLOBAL_BURDEN}")
    return body


def _save(body: dict) -> None:
    C.ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    _path().write_text(json.dumps(body, indent=2, default=str),
                       encoding="utf-8")


def candidate_id(spec: dict) -> str:
    h = hashlib.sha256(json.dumps(spec, sort_keys=True,
                                  default=str).encode("utf-8")).hexdigest()
    return f"c45_{h[:12]}"


def charge(spec: dict, *, family: str, lane: str, label: str = None) -> dict:
    """Charge one effective trial for one distinct searched cell.

    Charging is keyed on the SPEC, not on the label, so re-scoring the same
    book under a prettier name cannot inflate - or launder - the count.
    """
    if family not in C.BURDEN_FAMILIES:
        raise ValueError(f"unknown burden family {family!r}")
    body = _load()
    cid = candidate_id(spec)
    node = body["candidates"].get(cid)
    if node is None:
        body["candidates"][cid] = {
            "family": family, "lane": lane, "label": label,
            "spec": spec, "touches": 1}
        body["by_family"][family] = body["by_family"].get(family, 0) + 1
        body["evaluations"] += 1
        charged = True
    else:
        node["touches"] += 1
        charged = False
    _save(body)
    return {"candidate_id": cid, "charged_new_trial": charged,
            "family": family, "lane": lane}


def charge_frozen_replication(markets: list) -> dict:
    """The whole predeclared replication programme, for one trial."""
    return charge(
        {"programme": "FROZEN_R44_RULE_REPLICATION",
         "rule": C.FROZEN_RULE["rule"],
         "entry_delay_min": C.FROZEN_RULE["entry_delay_min"],
         "hold_min": C.FROZEN_RULE["hold_min"],
         "markets": sorted(markets),
         "parameters_chosen_by_r45": 0},
        family="FROZEN_MACRO_REPLICATION", lane="L1-L4",
        label="frozen R44 rule, unchanged, in every reachable market")


def summary() -> dict:
    body = _load()
    new = int(body.get("evaluations", 0))
    return {
        "schema": "r45_search_burden/1",
        "calculation_owner": CALCULATION_OWNER,
        "inherited_global_cumulative": C.INHERITED_GLOBAL_BURDEN,
        "inherited_global_conservative":
            C.INHERITED_GLOBAL_BURDEN_CONSERVATIVE,
        "new_r45_effective_trials": new,
        "GLOBAL_SEARCH_BURDEN": C.INHERITED_GLOBAL_BURDEN + new,
        "GLOBAL_SEARCH_BURDEN_CONSERVATIVE":
            C.INHERITED_GLOBAL_BURDEN_CONSERVATIVE + new,
        "by_family": dict(body.get("by_family", {})),
        "n_distinct_candidates": len(body.get("candidates", {})),
        "burden_may_never_be_reset": C.BURDEN_MAY_NEVER_BE_RESET,
        "note": "one predeclared mechanism tested in many markets is charged "
                "ONE trial; every cell whose parameters Release 45 chose is "
                "charged separately",
    }


def global_count() -> int:
    return summary()["GLOBAL_SEARCH_BURDEN"]
