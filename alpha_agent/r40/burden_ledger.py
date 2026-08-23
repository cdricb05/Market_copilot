"""alpha_agent.r40.burden_ledger - the cumulative search burden, inherited.

The deflated-Sharpe denominator for every qualification claim in this
release family is the CUMULATIVE count of distinct candidates ever scored
on the selection zone. Release 40 starts at the R39 continuation's 194 and
never resets. The ledger itself is the R39 owner's
(:mod:`alpha_agent.r39.zones` - ``record_zone_b`` / ``reuse_summary``),
served under the R40 research root through ``r39.register_campaign_root``;
this module only INITIALISES it from the continuation ledger and reports
the three numbers the final answer must carry:

    R39_INHERITED_EFFECTIVE_TRIALS
    R40_NEW_EFFECTIVE_TRIALS
    CUMULATIVE_R39_R40_EFFECTIVE_TRIALS
"""
from __future__ import annotations

from .. import r39 as _r39
from ..r39 import zones
from . import CAMPAIGN_ID, campaign_dir
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r40.burden_ledger"
INHERITED_STAGE_MARKER = "R39_INHERITED"
LEDGER_ARTIFACT = "r40_cumulative_search_ledger.json"


def inherit(campaign_id: str = CAMPAIGN_ID,
            from_campaign_id: str = C.R39_CONTINUATION_CAMPAIGN_ID) -> dict:
    """Initialise the R40 reuse ledger FROM the R39 continuation ledger.

    Idempotent; refuses to guess if the source does not hold exactly the
    expected 194 distinct candidates (the number is verified, not assumed).
    """
    cdir = campaign_dir(campaign_id)
    cdir.mkdir(parents=True, exist_ok=True)
    path = cdir / zones.REUSE_NAME
    if not path.exists():
        src = _r39.read_json(_r39.campaign_dir(from_campaign_id)
                             / zones.REUSE_NAME)
        if not src:
            raise FileNotFoundError(
                "the R39 continuation reuse ledger is required to inherit "
                "the burden: %s" % from_campaign_id)
        distinct = int(src["distinct_candidates"])
        if distinct != C.R39_INHERITED_EFFECTIVE_TRIALS_EXPECTED:
            raise ValueError(
                "R39 continuation ledger holds %d distinct candidates; the "
                "R40 contract expected %d - refusing to guess"
                % (distinct, C.R39_INHERITED_EFFECTIVE_TRIALS_EXPECTED))
        evaluations = {}
        for cid, entry in src["evaluations"].items():
            stages = [str(s) for s in entry.get("stages", [])]
            evaluations[cid] = {"count": int(entry["count"]),
                                "stages": [INHERITED_STAGE_MARKER] + stages}
        body = {"contract": zones.REUSE_SCHEMA, "campaign_id": campaign_id,
                "calculation_owner": CALCULATION_OWNER,
                "inherited_from_campaign": from_campaign_id,
                "inherited_distinct_candidates": distinct,
                "inherited_total_evaluations": int(src["total_evaluations"]),
                "burden_never_resets": C.BURDEN_NEVER_RESETS,
                "evaluations": evaluations,
                "total_evaluations": int(src["total_evaluations"]),
                "distinct_candidates": len(evaluations)}
        _r39.write_json(path, body, immutable=False)
    return summary(campaign_id)


def record(candidate_id: str, *, stage: str,
           campaign_id: str = CAMPAIGN_ID) -> None:
    """Every R40 validation evaluation goes through the R39 owner."""
    campaign_dir(campaign_id)  # ensures the root registration
    zones.record_zone_b(candidate_id, stage=stage, campaign_id=campaign_id)


def summary(campaign_id: str = CAMPAIGN_ID) -> dict:
    campaign_dir(campaign_id)
    led = _r39.read_json(_r39.campaign_dir(campaign_id) / zones.REUSE_NAME)
    if not led:
        raise FileNotFoundError("inherit() must run first")
    inherited = int(led.get("inherited_distinct_candidates") or 0)
    distinct = int(led["distinct_candidates"])
    return {
        "R39_INHERITED_EFFECTIVE_TRIALS": inherited,
        "R40_NEW_EFFECTIVE_TRIALS": distinct - inherited,
        "CUMULATIVE_R39_R40_EFFECTIVE_TRIALS": distinct,
        "total_zone_b_evaluations": int(led["total_evaluations"]),
        "burden_never_resets": C.BURDEN_NEVER_RESETS,
        "dsr_denominator": distinct,
    }


def new_candidate_ids(campaign_id: str = CAMPAIGN_ID) -> list:
    campaign_dir(campaign_id)
    led = _r39.read_json(_r39.campaign_dir(campaign_id) / zones.REUSE_NAME)
    return sorted(cid for cid, e in (led or {}).get("evaluations", {}).items()
                  if INHERITED_STAGE_MARKER not in e.get("stages", []))


def write_artifact(campaign_id: str = CAMPAIGN_ID, **extra) -> dict:
    from . import artifact_body
    body = artifact_body("r40_cumulative_search_ledger/1", {
        "calculation_owner": CALCULATION_OWNER,
        **summary(campaign_id),
        "r40_new_candidate_ids": new_candidate_ids(campaign_id),
        "no_campaign_id_laundering": C.NO_CAMPAIGN_ID_LAUNDERING,
        "dsr_denominator_rule": "the CUMULATIVE distinct count (R39 v1 + "
                                "R39 continuation + R40) is the deflated-"
                                "Sharpe trial denominator for every "
                                "qualification claim in this release "
                                "family",
        **extra,
    })
    body["ledger_hash"] = _r39.sha(body)
    _r39.write_json(campaign_dir(campaign_id) / LEDGER_ARTIFACT, body,
                    immutable=False)
    return body
