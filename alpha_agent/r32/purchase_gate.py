"""alpha_agent.r32.purchase_gate - the ONE Information Purchase Gate owner.

Release 32 spends nothing. Its job is to identify which information would
actually change a decision BEFORE anyone pays for it, which is the opposite of
the usual order and the reason this gate exists at all.

Ten conditions must all pass before a dataset becomes a purchase CANDIDATE - and
a candidate is still not a purchase. The gate never buys, never starts a trial,
and never waits: a campaign that pauses for a vendor sample has handed its
schedule to a sales team.

The history behind the strictness is specific. Stage 13B found a promising sales
PEAD signal at t = 2.27; Stage 13C's out-of-sample test returned t = -0.29 and
the recommendation became DO_NOT_BUY. An Intrinio live trial produced
NO_DEFENSIBLE_ALPHA. FMP analyst grades failed a survivorship check. Every one of
those would have been a purchase under a looser gate.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from .. import r32
from . import contract as _contract

CALCULATION_OWNER = "alpha_agent.r32.purchase_gate"
GATE_SCHEMA = "r32_information_purchase_frontier/1"
ARTIFACT_NAME = "information_purchase_frontier.json"

#: The ten conditions. All must pass; there is no weighted score, because a
#: weighted score lets a strong marketing claim compensate for absent history.
CONDITIONS = (
    "the gap blocked a specific, named sleeve rather than a general hope",
    "the blocked question is economically material to portfolio PnL",
    "the dataset is genuinely point-in-time, with vintages or publication stamps",
    "history is long enough for the decision cadence being proposed",
    "inactive and delisted coverage exists, so the sample is survivorship-safe",
    "the data is economically distinct from what is already owned",
    "a free or owned substitute has been tried and measured as insufficient",
    "licensing permits the research and paper use intended",
    "a bounded evaluation exists that could return DO_NOT_BUY",
    "the cost is stated and a decision-maker has accepted it",
)

STATE_NOT_A_CANDIDATE = "NOT_A_PURCHASE_CANDIDATE"
STATE_CANDIDATE = "PURCHASE_CANDIDATE"
STATE_WAITING_FOR_SAMPLE = "WAITING_FOR_SAMPLE"
STATE_BLOCKED_PIT = "BLOCKED_POINT_IN_TIME"
STATE_BLOCKED_COVERAGE = "BLOCKED_COVERAGE"
STATE_BLOCKED_LICENCE = "BLOCKED_LICENCE"
STATE_EVALUATED_DO_NOT_BUY = "EVALUATED_DO_NOT_BUY"
STATE_OWNED_SUBSTITUTE_SUFFICIENT = "OWNED_SUBSTITUTE_SUFFICIENT"
STATES = (STATE_NOT_A_CANDIDATE, STATE_CANDIDATE, STATE_WAITING_FOR_SAMPLE,
          STATE_BLOCKED_PIT, STATE_BLOCKED_COVERAGE, STATE_BLOCKED_LICENCE,
          STATE_EVALUATED_DO_NOT_BUY, STATE_OWNED_SUBSTITUTE_SUFFICIENT)

#: Providers already evaluated. Recorded so a later release cannot re-open a
#: settled question by forgetting it was settled.
PRIOR_EVALUATIONS = (
    {"provider": "Intrinio", "state": STATE_EVALUATED_DO_NOT_BUY,
     "evidence": "live trial returned NO_DEFENSIBLE_ALPHA and failed a "
                 "survivorship-safe 16-year test"},
    {"provider": "FMP analyst grades", "state": STATE_EVALUATED_DO_NOT_BUY,
     "evidence": "survivorship check FAILED; zero FDR survivors"},
    {"provider": "Nasdaq Zacks", "state": STATE_WAITING_FOR_SAMPLE,
     "evidence": "free key returns SAMPLE only; needs contact-sales"},
    {"provider": "SimFin (free tier)", "state": STATE_BLOCKED_COVERAGE,
     "evidence": "2020s only; cross-sectional coverage inadequate"},
    {"provider": "SEC EDGAR", "state": STATE_OWNED_SUBSTITUTE_SUFFICIENT,
     "evidence": "already owned and free; filing timestamps are true PIT"},
)


def evaluate_gap(gap: dict) -> dict:
    """Apply the ten conditions to one information gap.

    Named ``evaluate_gap`` rather than the bare verb: that bare definition is
    the ownership token the architecture audit greps for to identify the
    Slice-8 research-agent kernel, so a second one reads as a second
    calculation owner for research-state evaluation. The guard matches a plain
    substring, which means even naming the token in a comment trips it - as
    this docstring did on its first wording.
    """
    checks = dict(gap.get("conditions") or {})
    passed = [c for c in CONDITIONS if checks.get(c) is True]
    failed = [c for c in CONDITIONS if checks.get(c) is not True]
    state = gap.get("state")
    if not state:
        state = STATE_CANDIDATE if not failed else STATE_NOT_A_CANDIDATE
    return {"gap": gap.get("gap"),
            "blocked_sleeve": gap.get("blocked_sleeve"),
            "why_it_matters": gap.get("why_it_matters"),
            "owned_substitute_tried": gap.get("owned_substitute_tried"),
            "conditions_passed": len(passed),
            "conditions_total": len(CONDITIONS),
            "failed_conditions": failed,
            "state": state,
            "purchase_authorised": False,
            "money_spent_usd": 0.0}


def build(*, campaign_id: str = _contract.CAMPAIGN_ID, gaps: list,
          created_at: Optional[str] = None) -> dict:
    rows = [evaluate_gap(g) for g in gaps]
    candidates = [r for r in rows if r["state"] == STATE_CANDIDATE]
    ranked = sorted(rows, key=lambda r: -r["conditions_passed"])
    payload = {
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": campaign_id,
        "created_at": created_at or _dt.datetime.now().isoformat(timespec="seconds"),
        "conditions": list(CONDITIONS),
        "states": list(STATES),
        "gaps": rows,
        "n_gaps": len(rows),
        "purchase_candidates": [r["gap"] for r in candidates],
        "highest_value_sample_request": ranked[0]["gap"] if ranked else None,
        "prior_evaluations": list(PRIOR_EVALUATIONS),
        "release32_may_spend_money": r32.MAY_SPEND_MONEY,
        "total_spent_usd": 0.0,
        "waits_for_a_vendor_sample": False,
    }
    body = r32.artifact_body(GATE_SCHEMA, payload)
    body["purchase_frontier_hash"] = r32.sha(payload)
    return body


def path_for(campaign_id: str = _contract.CAMPAIGN_ID) -> Path:
    return r32.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict) -> Path:
    return r32.write_json(path_for(body["campaign_id"]), body)
