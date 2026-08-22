"""alpha_agent.r38.steele - the Intrinio/Steele Barcomb parallel track.

Historical point-in-time analyst revisions remain the one repeatedly named,
never-proven data family (Stage 13B t=2.27 in-sample; Stage 13C out-of-sample
t=-0.29; the Intrinio live trial NO_DEFENSIBLE_ALPHA on a survivorship-safe
16-year test; R35 measured every free tier inadmissible). The commercial
blocker is that historical PIT analyst data is far more expensive than the
futures package and its PIT semantics have never been demonstrated.

The next evidence is a FIVE-TICKER HISTORICAL SAMPLE from the vendor. This
module produces the operator-ready request. It purchases nothing, starts no
licence, and the sample it asks for is explicitly
``SCHEMA_AND_PIT_VALIDATION_ONLY`` - five tickers can validate observation
dates and identifier semantics and can never validate Alpha.

No frozen five-name sample exists in project evidence (searched: Stage 13A/B/C
artifacts, R32/R35 analyst lanes), so a deliberately informative set is
proposed here, each name chosen to stress a specific failure mode.
"""
from __future__ import annotations

import datetime as _dt
from typing import Optional

from .. import r38
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r38.steele"
SCHEMA = "r38_intrinio_steele_sample_request/1"
ARTIFACT_NAME = C.ARTIFACT_NAMES["intrinio_steele_sample_request"]
MESSAGE_NAME = "intrinio_steele_sample_request_message.md"

SAMPLE_PURPOSE = "SCHEMA_AND_PIT_VALIDATION_ONLY"
SAMPLE_IS_ALPHA_EVIDENCE = False

#: Five names, each stressing a distinct failure mode of historical analyst
#: data. Proposed (no frozen prior set exists); reasons are part of the ask.
PROPOSED_TICKERS = (
    {"ticker": "AAPL", "why": (
        "dense mega-cap coverage with a September fiscal year end: stresses "
        "as-of date density, analyst count, dispersion, and fiscal-period "
        "identity on a non-December calendar")},
    {"ticker": "MON", "why": (
        "Monsanto, acquired by Bayer and delisted June 2018: stresses "
        "inactive/delisted handling and whether pre-delisting consensus "
        "history survives in the archive")},
    {"ticker": "META", "why": (
        "ticker and name change from FB in 2022: stresses issuer identifier "
        "continuity across a symbol change")},
    {"ticker": "HTZ", "why": (
        "Hertz - Chapter 11 in 2020, OTC as HTZGQ, re-emerged 2021: "
        "stresses bankruptcy handling, coverage gaps, and identifier "
        "continuity across a re-listing")},
    {"ticker": "CALM", "why": (
        "Cal-Maine Foods - thin small-cap coverage with a May/June fiscal "
        "year end: stresses sparse-coverage months, low analyst counts, and "
        "dispersion computed on two or three estimates")},
)

#: What the sample must PROVE, field by field. Anything the vendor cannot
#: supply is an answer too - it converts an unknown into a measured gap.
REQUIRED_EVIDENCE = (
    "true historical observation/as-of dates (the date each consensus value "
    "became knowable, not the fiscal date it refers to)",
    "historical EPS consensus per fiscal period",
    "historical revenue consensus where available",
    "mean, median, high and low of the estimate distribution",
    "analyst count per observation",
    "dispersion (standard deviation) per observation",
    "revision-up and revision-down counts where available",
    "fiscal-period identity (FY/FQ label and period end date)",
    "the subsequent reported actual for each estimated period",
    "issuer identifier continuity (permanent ID / CIK / FIGI, surviving "
    "ticker and name changes)",
    "inactive/delisted handling (does history for MON and pre-emergence HTZ "
    "exist, and under which identifier)",
)

WINDOW_ASK = "monthly (or finer) observations, 2004-01 through 2024-12"


def operator_message() -> str:
    """The message the OPERATOR may send to Steele Barcomb. Claude sends
    nothing."""
    tickers = ", ".join(t["ticker"] for t in PROPOSED_TICKERS)
    lines = [
        "Subject: Historical analyst estimates - 5-ticker sample request",
        "",
        "Hi Steele,",
        "",
        "Following up on the historical analyst estimates discussion: before "
        "we can take a licence conversation further, we need to validate the "
        "historical schema and point-in-time semantics on a small concrete "
        "sample. Could you provide a historical extract for FIVE tickers:",
        "",
        "    %s" % tickers,
        "",
        "Window: %s. CSV or similar is fine." % WINDOW_ASK,
        "",
        "What the sample needs to demonstrate, per observation row:",
        "",
    ]
    lines += ["- %s" % item for item in REQUIRED_EVIDENCE]
    lines += [
        "",
        "Why these five: AAPL (dense coverage, September FYE), MON "
        "(delisted 2018 - archive completeness for inactive names), META "
        "(FB ticker/name change - identifier continuity), HTZ (bankruptcy, "
        "OTC period and re-listing), CALM (sparse small-cap coverage, "
        "unusual fiscal calendar).",
        "",
        "To be explicit about scope: this sample is for schema and "
        "point-in-time validation only. Five names cannot and will not be "
        "used as evidence of predictive value; that evaluation would need a "
        "separate, survivorship-safe historical universe if the schema "
        "checks out.",
        "",
        "Thanks,",
        "Cedric",
    ]
    return "\n".join(lines)


def build(*, campaign_id: str = C.CAMPAIGN_ID,
          created_at: Optional[str] = None) -> dict:
    created = created_at or _dt.datetime.now(_dt.timezone.utc).isoformat()
    payload = {
        "campaign_id": campaign_id,
        "created_at": created,
        "calculation_owner": CALCULATION_OWNER,
        "lane": "HISTORICAL_ANALYST_REVISIONS",
        "runs_in_parallel_with_r38": True,
        "blocks_r38": False,
        "purpose": SAMPLE_PURPOSE,
        "sample_is_alpha_evidence": SAMPLE_IS_ALPHA_EVIDENCE,
        "prior_evidence": {
            "stage_13b": "PEAD sales 63d t=2.27 (in-sample)",
            "stage_13c": "out-of-sample DID NOT REPLICATE (t=-0.29)",
            "intrinio_live_trial":
                "NO_DEFENSIBLE_ALPHA / DO_NOT_BUY on survivorship-safe 16y",
            "r35": "all six free entitlements inadmissible as history",
            "r37": "canonical Stage-A state for intrinio_analyst_estimates: "
                   "REJECT; lseg_ibes_estimates and zacks_consensus_history: "
                   "CANDIDATE",
        },
        "commercial_blocker": (
            "historical PIT analyst revisions cost substantially more than "
            "the Norgate futures package and the required historical/PIT "
            "semantics have never been demonstrated by any tested source"),
        "proposed_tickers": [dict(t) for t in PROPOSED_TICKERS],
        "frozen_prior_ticker_set_found": False,
        "required_evidence": list(REQUIRED_EVIDENCE),
        "window": WINDOW_ASK,
        "operator_actions": [
            "review and send the prepared message to Steele Barcomb",
            "do NOT purchase, trial, or accept a licence - the sample "
            "request is free and non-binding",
        ],
        "claude_sends_nothing": True,
        "purchases_made_by_this_module": 0,
        "message_markdown": operator_message(),
    }
    return r38.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / ARTIFACT_NAME


def message_path(campaign_id: str = C.CAMPAIGN_ID):
    return r38.campaign_dir(campaign_id) / MESSAGE_NAME


def freeze(body: dict) -> None:
    path = path_for(body["campaign_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    r38.write_json(path, body)
    message_path(body["campaign_id"]).write_text(
        body["message_markdown"], encoding="utf-8")


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    path = path_for(campaign_id)
    if not path.exists():
        return None
    return r38.read_json(path)
