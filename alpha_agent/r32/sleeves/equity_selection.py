"""EQUITY_SELECTION - the CONTROL sleeve. Loaded from Release 31, never rerun.

Release 31 executed 77 candidates across 12 known-method families and 43 novel
specifications over the point-in-time S&P 500, judged them on the canonical
zero-base allocator with cash free between 0 % and 100 %, and returned
``R31_CURRENT_INFORMATION_MODEL_FRONTIER_EXHAUSTED``. Its dominant constraint was
``INFORMATION_NOT_METHOD``: 67 of 77 candidates could not be mapped into economic
return units at all, and the best lockbox result lost to the benchmark BEFORE
costs.

Re-running that search would be the single most expensive way to learn nothing.
Worse, it would be actively harmful: every additional pass over the same
information adds to the multiple-testing denominator, so the more often the
question is asked, the weaker any eventual answer becomes. The result is
therefore INHERITED, with its frozen evidence and its verdict hash, and this
sleeve carries it into the frontier as the equity-selection baseline every other
sleeve is compared against.

``funnel.assert_control_not_researched`` enforces the "never rerun" half of that
sentence, and it is a real assertion rather than a comment because the pressure
to "just try one more family" is exactly what a bounded campaign exists to
resist.
"""
from __future__ import annotations

from typing import Optional

from ... import r31
from .. import contract as _contract

SLEEVE = _contract.SLEEVE_EQUITY_SELECTION
IS_CONTROL = True
MAY_BE_RESEARCHED_IN_R32 = False

R31_CAMPAIGN_ID = _contract.R31_CAMPAIGN_ID
VERDICT_ARTIFACT = "final_verdict.json"
MULTIPLE_TESTING_ARTIFACT = "multiple_testing_results.json"
LOCKBOX_ARTIFACT = "lockbox_results.json"


def screening_specs() -> list:
    """The control is not screened. It has already been measured."""
    return []


def qualification_specs(families: list) -> list:
    return []


def artifact_path(name: str):
    return r31.campaign_dir(R31_CAMPAIGN_ID) / name


def load_inherited(*, campaign_id: str = R31_CAMPAIGN_ID) -> dict:
    """Read Release 31's frozen terminal evidence. Read-only, never rewritten."""
    root = r31.campaign_dir(campaign_id)
    verdict = r31.read_json(root / VERDICT_ARTIFACT)
    mt = r31.read_json(root / MULTIPLE_TESTING_ARTIFACT)
    if verdict is None:
        return {"sleeve": SLEEVE, "inherited": False,
                "reason": "R31_VERDICT_ARTIFACT_NOT_FOUND",
                "expected_path": str(root / VERDICT_ARTIFACT)}
    out = {
        "sleeve": SLEEVE,
        "inherited": True,
        "is_control": IS_CONTROL,
        "rerun_in_r32": False,
        "campaign_id": campaign_id,
        # The key is ``primary_verdict``; an earlier read looked for ``verdict``
        # and silently produced None, which put a null where the control's
        # terminal result belonged.
        "verdict": verdict.get("primary_verdict"),
        "secondary_verdict": verdict.get("secondary_verdict"),
        "dominant_constraint": _contract.R31_DOMINANT_CONSTRAINT,
        "verdict_hash": verdict.get("final_verdict_hash"),
        "artifact_path": str(root / VERDICT_ARTIFACT),
        "artifact_sha256": r31.sha_file(root / VERDICT_ARTIFACT),
    }
    if isinstance(mt, dict):
        out["multiple_testing_denominator"] = mt.get("denominator")
        out["fdr_survivors"] = mt.get("n_survivors")
    if not out["verdict"]:
        out["inherited"] = False
        out["reason"] = "R31_VERDICT_KEY_ABSENT"
    return out


def economics(inherited: dict) -> dict:
    """The control's standalone economics, as Release 31 measured them.

    Reported in the frontier so the equity-selection baseline is visible next to
    every new sleeve rather than being an absence.
    """
    verdict = r31.read_json(inherited.get("artifact_path") or "") or {}
    best = (verdict.get("operational_comparison")
            or verdict.get("superiority")
            or {})
    return {"source": "RELEASE_31_FROZEN_EVIDENCE",
            "rerun": False,
            "summary": best if isinstance(best, dict) else {},
            "qualified": False,
            "state": "REJECTED",
            "rejection_reason": _contract.R31_VERDICT}
