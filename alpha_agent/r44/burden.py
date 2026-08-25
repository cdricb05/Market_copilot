"""alpha_agent.r44.burden - the never-reset GLOBAL + FAMILY search burden.

Release 43's ledger is the authority for everything spent up to the end of
Release 43 and is opened READ-ONLY: :func:`verify_inherited` re-derives
289 + 13 = 302 from the bytes on disk - which in turn re-derived 230 + 59
from R41's - and REFUSES to run if the contract's declared inheritance
disagrees. The number 302 is never typed from memory into a result.

Release 44 keeps its own ledger of its own distinct ZONE_B candidates and
reports ``global_cumulative = 302 + r44_distinct``.

One thing here is genuinely new. PORTFOLIO_SYNTHESIS is a burden family like
any other. Eight predeclared combination rules evaluated against the same
streams are eight trials, and a release that reports the best of them
without saying so has laundered seven. They are charged.
"""
from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

from . import CAMPAIGN_ID, R43_RESEARCH_ROOT, campaign_dir, read_json, sha
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r44.burden"
LEDGER_NAME = "r44_search_burden_ledger.json"
R43_LEDGER = (R43_RESEARCH_ROOT / "r43_global_alpha_offensive_v1"
              / "r43_search_burden_ledger.json")


class BurdenLaundering(RuntimeError):
    """Raised when the inherited burden cannot be verified from bytes."""


# --------------------------------------------------------------------------- #
# Inheritance, verified from the R43 artifact (never typed from memory)
# --------------------------------------------------------------------------- #
def verify_inherited() -> dict:
    if not R43_LEDGER.exists():
        raise BurdenLaundering(
            "R43 burden ledger not found at %s - the inherited burden "
            "cannot be verified and must not be assumed" % R43_LEDGER)
    body = json.loads(R43_LEDGER.read_text(encoding="utf-8"))
    pre_r43 = int(body["global_inherited"])
    r43_distinct = len(body.get("candidates") or {})
    total = pre_r43 + r43_distinct

    fam = dict(body.get("inherited_family_counts") or {})
    for row in (body.get("candidates") or {}).values():
        fam[row["family"]] = fam.get(row["family"], 0) + 1

    out = {"r43_ledger": str(R43_LEDGER),
           "r43_ledger_sha256": sha(body),
           "pre_r43_effective_trials": pre_r43,
           "r43_distinct_zone_b_candidates": r43_distinct,
           "r43_zone_b_evaluations": int(body.get("evaluations") or 0),
           "inherited_global_cumulative": total,
           "inherited_family_counts": fam,
           "contract_declares": C.GLOBAL_INHERITED_EFFECTIVE_TRIALS,
           "verified": total == C.GLOBAL_INHERITED_EFFECTIVE_TRIALS,
           "read_only": True}
    if not out["verified"]:
        raise BurdenLaundering(
            "inherited burden mismatch: R43 ledger says %d (%d + %d), the "
            "R44 contract declares %d" % (
                total, pre_r43, r43_distinct,
                C.GLOBAL_INHERITED_EFFECTIVE_TRIALS))

    declared = {k: v for k, v in C.INHERITED_FAMILY_COUNTS.items() if v}
    derived = {k: v for k, v in fam.items() if v}
    if derived != declared:
        raise BurdenLaundering(
            "inherited FAMILY counts mismatch: R43 ledger %r vs R44 contract "
            "%r" % (derived, declared))
    return out


# --------------------------------------------------------------------------- #
# The R44 ledger
# --------------------------------------------------------------------------- #
def _path(campaign_id: str = CAMPAIGN_ID) -> Path:
    return campaign_dir(campaign_id) / LEDGER_NAME


def _load(campaign_id: str = CAMPAIGN_ID) -> dict:
    body = read_json(_path(campaign_id))
    if body:
        return body
    return {"schema": "r44_search_burden_ledger/1",
            "global_inherited": C.GLOBAL_INHERITED_EFFECTIVE_TRIALS,
            "inherited_family_counts": dict(C.INHERITED_FAMILY_COUNTS),
            "candidates": {}, "evaluations": 0, "lane_counts": {}}


def _save(body: dict, campaign_id: str = CAMPAIGN_ID) -> None:
    p = _path(campaign_id)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(body, indent=1, sort_keys=True, default=str),
                 encoding="utf-8")


def candidate_id(spec: dict) -> str:
    return "c44_" + sha(spec)[:12]


def record_zone_b(spec: dict, *, family: str, lane: str,
                  campaign_id: str = CAMPAIGN_ID) -> str:
    """Register ONE ZONE_B evaluation. Enforces the lane's frozen cap."""
    if family not in C.BURDEN_FAMILIES:
        raise ValueError("unknown burden family %r" % family)
    if lane not in C.LANE_CAPS:
        raise ValueError("unknown lane %r" % lane)
    cid = candidate_id(spec)
    body = _load(campaign_id)
    row = (body["candidates"] or {}).get(cid)
    if row is None:
        cap = int(C.LANE_CAPS[lane])
        used = sum(1 for r in body["candidates"].values()
                   if r.get("lane") == lane)
        if used >= cap:
            raise ValueError(
                "lane %s has exhausted its FROZEN ZONE_B cap of %d; a lane "
                "that wants more candidates must fail instead" % (lane, cap))
        lineage = {k: spec.get(k) for k in C.LINEAGE_FIELDS}
        row = {"family": family, "lane": lane, "lineage": lineage,
               "touches": 0,
               "first_seen": _dt.datetime.now(_dt.timezone.utc).isoformat()}
        body["candidates"][cid] = row
    row["touches"] += 1
    body["evaluations"] += 1
    body["lane_counts"] = _lane_counts(body)
    _save(body, campaign_id)
    return cid


def _lane_counts(body: dict) -> dict:
    out = {}
    for row in body["candidates"].values():
        out[row.get("lane")] = out.get(row.get("lane"), 0) + 1
    return out


def summary(campaign_id: str = CAMPAIGN_ID) -> dict:
    body = _load(campaign_id)
    inherited_fam = dict(body.get("inherited_family_counts")
                         or C.INHERITED_FAMILY_COUNTS)
    r44_fam = {}
    for row in body["candidates"].values():
        r44_fam[row["family"]] = r44_fam.get(row["family"], 0) + 1
    distinct = len(body["candidates"])
    total_fam = {f: inherited_fam.get(f, 0) + r44_fam.get(f, 0)
                 for f in C.BURDEN_FAMILIES}
    return {
        "calculation_owner": CALCULATION_OWNER,
        "global_inherited": body["global_inherited"],
        "r44_distinct_zone_b_candidates": distinct,
        "global_cumulative": body["global_inherited"] + distinct,
        "r44_zone_b_evaluations": body["evaluations"],
        "r44_family_counts": r44_fam,
        "cumulative_family_counts": total_fam,
        "lane_counts": _lane_counts(body),
        "lane_caps": dict(C.LANE_CAPS),
        "total_zone_b_budget": C.TOTAL_ZONE_B_BUDGET,
        "portfolio_synthesis_is_charged": (
            C.PORTFOLIO_SYNTHESIS_IS_A_SEARCHED_FAMILY),
        "never_reset": True,
        "r43_ledger_mutated": False,
    }


def family_count(family: str, campaign_id: str = CAMPAIGN_ID) -> int:
    """CUMULATIVE family denominator (inherited + R44), for deflated Sharpe."""
    return summary(campaign_id)["cumulative_family_counts"].get(family, 0)


def global_count(campaign_id: str = CAMPAIGN_ID) -> int:
    return summary(campaign_id)["global_cumulative"]
