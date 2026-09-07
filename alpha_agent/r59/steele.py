"""alpha_agent.r59.steele - the analyst-revision SAMPLE GATE, armed in advance.

Section J. A sample is expected from an analyst-revision vendor. No purchase may
occur, and no research waits on it. What R59 owes the operator is that the
moment a sample lands, the estate already knows exactly how it will be judged -
because deciding the criteria AFTER seeing the data is how a vendor evaluation
turns into a rationalisation.

So the gate is written now, with its thresholds fixed now, and it will refuse to
score a sample that does not exist. ``evaluate_sample`` requires a real file;
there is deliberately no simulated, illustrative or expected-shape result
anywhere in this module. (The name is ``evaluate_sample`` and not the bare verb
because the architecture audit reserves that exact function name for the ONE
research-state kernel, ``engine.research_agent``.)

The thresholds are not new: they are the ones the estate already paid to learn.

* Stage 13B built a PROSPECTIVE revision ledger and found PEAD on sales
  revisions at 63 sessions (t = 2.27).
* Stage 13C tested it out of sample and it DID NOT REPLICATE (t = -0.29).
* The Intrinio live trial closed DO_NOT_BUY because a survivorship-safe
  reconstruction was impossible.

The binding lesson is the third one: a revision dataset that cannot reproduce
what a subscriber would have seen on a delisted name at the time cannot be
tested honestly at any price. That is why ``inactive_delisted_support`` and
``restatement_backfill_behavior`` are HARD conditions here rather than scored
ones - no amount of coverage compensates for a survivor-only history.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from .. import r59

CALCULATION_OWNER = "alpha_agent.r59.steele"
SCHEMA = "r59_analyst_sample_gate/1"

SAMPLE_ENV = "PAPER_TRADER_R59_ANALYST_SAMPLE"

STATE_NOT_RECEIVED = "SAMPLE_NOT_RECEIVED"
STATE_UNREADABLE = "SAMPLE_UNREADABLE"
STATE_EVALUATED = "SAMPLE_EVALUATED"

# --- HARD conditions: any one failing ends the evaluation as DO_NOT_BUY ----- #
HARD_CONDITIONS = (
    ("point_in_time_guarantee",
     "the vendor states, in writing, that a record reflects what a subscriber "
     "could have seen on its as-of date"),
    ("inactive_delisted_support",
     "estimates survive for names that were later delisted or acquired"),
    ("no_restatement_backfill",
     "a later revision is a NEW record, never an overwrite of the old one"),
    ("research_use_allowed",
     "the licence permits the research this estate performs"),
)

# --- SCORED conditions: each carries a measured minimum ---------------------- #
SCORED_CONDITIONS = (
    ("history_years", 15.0, "years of revision history"),
    ("universe_size", 800, "distinct securities"),
    ("universe_coverage_ratio", 0.60,
     "fraction of the owned survivorship-safe universe matched"),
    ("identifier_match_rate", 0.90,
     "fraction of records resolvable to the estate's security identity"),
    ("revision_records_per_security_per_year", 4.0,
     "revision density - a consensus snapshot is not a revision history"),
    ("timestamp_resolution_days", 1.0,
     "as-of resolution, in days (lower is better)"),
    ("missingness_fraction", 0.10,
     "fraction of expected fields null (lower is better)"),
    ("effective_independent_cohorts", 12,
     "independent forward cohorts the sample could score"),
)

LOWER_IS_BETTER = ("timestamp_resolution_days", "missingness_fraction")

#: The exact hypotheses a passing sample would unlock, registered before the
#: sample arrives so the test set cannot grow to fit whatever the data supports.
UNLOCKED_HYPOTHESES = (
    {"id": "AR1_EPS_REVISION_BREADTH",
     "statement": "the cross-section of up-minus-down EPS revision breadth "
                  "predicts 21-session forward excess return",
     "horizon": 21},
    {"id": "AR2_REVISION_MAGNITUDE",
     "statement": "the magnitude of consensus EPS change, scaled by price, "
                  "predicts 21-session forward excess return",
     "horizon": 21},
    {"id": "AR3_SALES_REVISION_PEAD",
     "statement": "Stage 13B's sales-revision drift replicates out of sample "
                  "at 63 sessions on a survivorship-safe universe",
     "horizon": 63},
    {"id": "AR4_REVISION_ACCELERATION",
     "statement": "the change in revision breadth adds information beyond its "
                  "level",
     "horizon": 21},
    {"id": "AR5_DISPERSION_CHANGE",
     "statement": "narrowing estimate dispersion precedes positive drift",
     "horizon": 21},
    {"id": "AR6_ORTHOGONALITY",
     "statement": "any surviving revision signal is not a repackaging of the "
                  "owned price-momentum or fundamental-change families",
     "horizon": 21},
)

MIN_VIABLE_BACKTEST = {
    "partition": "discovery 2011-07-01 / validation 2018-01-01 / lockbox "
                 "2023-01-01, the SAME partition R57 and R58 used",
    "cadence_sessions": r59.CADENCE,
    "horizon_sessions": r59.HORIZON,
    "universe": "the owned Norgate S&P 500 Current & Past PIT membership, "
                "including delisted names",
    "cost_model": "12.5bp per side charged on traded notional, symmetric on "
                  "strategy and benchmark",
    "statistic": "Newey-West one-sided t on net excess, Benjamini-Hochberg "
                 "across the campaign, corrected for the estate's prior "
                 "search burden",
    "obs_floor": r59.OBS_FLOOR,
    "refusal_rule": "if the sample cannot fill the lockbox layer, the "
                    "evaluation returns INSUFFICIENT_SAMPLE and no purchase "
                    "recommendation is produced",
}


def readiness() -> dict:
    """The armed gate, independent of whether a sample exists."""
    return {
        "calculation_owner": CALCULATION_OWNER,
        "schema": SCHEMA,
        "state": STATE_NOT_RECEIVED if not sample_path() else "SAMPLE_PRESENT",
        "sample_path_env": SAMPLE_ENV,
        "sample_path": str(sample_path()) if sample_path() else None,
        "hard_conditions": [{"key": k, "requirement": d}
                            for k, d in HARD_CONDITIONS],
        "scored_conditions": [{"key": k, "minimum": v, "meaning": d}
                              for k, v, d in SCORED_CONDITIONS],
        "lower_is_better": list(LOWER_IS_BETTER),
        "unlocked_hypotheses": list(UNLOCKED_HYPOTHESES),
        "minimum_viable_backtest": dict(MIN_VIABLE_BACKTEST),
        "prior_evidence": {
            "stage_13b": "prospective revision ledger; PEAD on sales "
                         "revisions at 63 sessions, t = 2.27",
            "stage_13c": "out-of-sample replication FAILED, t = -0.29",
            "intrinio_trial": "DO_NOT_BUY; survivorship-safe reconstruction "
                              "was not possible",
        },
        "purchase_recommendation_rule":
            "BUY is possible only if every hard condition passes, every scored "
            "condition meets its minimum, AND the sample can actually score "
            "AR3 out of sample - the exact test that already failed once. Any "
            "other outcome is DO_NOT_BUY or INSUFFICIENT_SAMPLE.",
        "purchase_authority": "NONE - this module cannot purchase, subscribe, "
                              "start a trial or transmit payment details",
        "no_fabricated_results": True,
        "research_continues_without_it": True,
    }


def sample_path() -> Optional[Path]:
    import os
    p = os.environ.get(SAMPLE_ENV)
    if not p:
        return None
    path = Path(p)
    return path if path.exists() else None


def evaluate_sample(sample: Optional[dict] = None, *,
                    path: Optional[Path] = None) -> dict:
    """Score a REAL sample manifest against the armed gate.

    Refuses to produce a verdict when no sample exists. There is no default,
    illustrative or expected-shape sample in this module by design.
    """
    if sample is None:
        p = path or sample_path()
        if p is None:
            return {"calculation_owner": CALCULATION_OWNER,
                    "state": STATE_NOT_RECEIVED,
                    "verdict": None,
                    "reason": "no sample file; set %s to a manifest path once "
                              "the vendor delivers one" % SAMPLE_ENV,
                    "readiness": readiness()}
        try:
            sample = json.loads(Path(p).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            return {"calculation_owner": CALCULATION_OWNER,
                    "state": STATE_UNREADABLE, "verdict": None,
                    "reason": "sample present but unreadable: %s" % exc}

    hard = {}
    for key, _desc in HARD_CONDITIONS:
        val = sample.get(key)
        hard[key] = {"value": val, "pass": val is True}
    hard_failed = [k for k, v in hard.items() if not v["pass"]]

    scored = {}
    for key, minimum, _desc in SCORED_CONDITIONS:
        val = sample.get(key)
        if val is None:
            ok = False
        elif key in LOWER_IS_BETTER:
            ok = float(val) <= float(minimum)
        else:
            ok = float(val) >= float(minimum)
        scored[key] = {"value": val, "minimum": minimum, "pass": bool(ok)}
    scored_failed = [k for k, v in scored.items() if not v["pass"]]

    can_score_ar3 = bool(sample.get("can_score_ar3_out_of_sample"))

    if hard_failed:
        verdict = "DO_NOT_BUY"
        reason = ("hard condition(s) failed: %s - a survivor-only or "
                  "restatement-overwritten revision history cannot be tested "
                  "honestly at any price" % ", ".join(hard_failed))
    elif scored_failed:
        verdict = "INSUFFICIENT_SAMPLE"
        reason = ("scored condition(s) below minimum: %s"
                  % ", ".join(scored_failed))
    elif not can_score_ar3:
        verdict = "INSUFFICIENT_SAMPLE"
        reason = ("the sample cannot score AR3 out of sample; that is the "
                  "exact test Stage 13C already failed, so a purchase could "
                  "not be justified by this sample")
    else:
        verdict = "PURCHASE_CANDIDATE"
        reason = ("every hard and scored condition passed and the sample can "
                  "run the AR3 out-of-sample replication; the formal "
                  "ten-condition purchase gate (api.data_expansion) is the "
                  "next and only authority")

    return {
        "calculation_owner": CALCULATION_OWNER,
        "schema": SCHEMA,
        "state": STATE_EVALUATED,
        "verdict": verdict,
        "reason": reason,
        "hard_conditions": hard,
        "scored_conditions": scored,
        "can_score_ar3_out_of_sample": can_score_ar3,
        "unlocked_hypotheses": list(UNLOCKED_HYPOTHESES),
        "minimum_viable_backtest": dict(MIN_VIABLE_BACKTEST),
        "purchase_authority": "NONE - a verdict here is an input to the formal "
                              "gate, never an authorisation",
    }


def publish() -> dict:
    body = readiness()
    body["evaluation"] = evaluate_sample()
    p = r59.write_artifact("r59_analyst_sample_readiness.json", body)
    body["artifact_path"] = str(p)
    return body
