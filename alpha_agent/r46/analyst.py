"""alpha_agent.r46.analyst - the prospective revision ledger, read and reported.

This ledger is the only analyst-revision history this estate will ever own
that cannot have been restated. It is captured forward, one snapshot at a
time, and it is worth exactly as much as its refusal to be backfilled.

Release 45 measured why that matters. It compared the estate's own captured
snapshots against the vendor's ``epsTrend7daysAgo`` backward strip - the field
that claims to tell you what the estimate WAS - and got a 53 % match rate,
with 25 differences above five cents and a worst case of 0.19 EPS on JPM.
Some of that is capture-time convention. Cent-level noise is. A 19-cent gap
is not. A vendor's backward strip is a CURRENT statement about the past, and
this project has been burned enough times to treat those as marketing.

Release 46 adds nothing to the ledger and rewrites nothing in it. It reads it,
reports how far it is from judgeable, and - the R46-specific act - declares
that when it IS judgeable, the analyst-revision challenger enters the
tournament through the same frozen-specification, forward-only door as
everything else. No backfill. No historical-vintage purchase. No head start.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, campaign_dir, sha, write_json
from . import contract as C

CALCULATION_OWNER = "alpha_agent.r46.analyst"

ARTIFACT = "R46_ANALYST_LANE.json"

MIN_REVISIONS_TO_JUDGE = 250
READ_ONLY = True
NEVER_BACKFILLED = True

#: The frozen specification an analyst-revision challenger will use the day
#: the ledger becomes judgeable. Declared NOW, before the evidence exists,
#: for exactly the reason the rest of this release exists.
PREDECLARED_CHALLENGER = {
    "challenger_id": "r46_analyst_revision_drift",
    "challenger_version": "v1",
    "family": "ANALYST_REVISION_DRIFT",
    "asset_class": "US_EQUITY",
    "prediction_type": "CROSS_SECTIONAL_LONG_SHORT",
    "statement": "names whose consensus forward EPS was revised UP between "
                 "two prospectively captured snapshots outperform names "
                 "revised DOWN over the following 21 sessions",
    "signal": "(latest captured consensus / previously captured consensus - 1), "
              "measured ONLY between two snapshots this estate captured "
              "itself, ranked cross-sectionally",
    "position": "long the top third of the revision cross-section, short the "
                "bottom third, dollar-neutral",
    "horizon_sessions": 21,
    "control": C.CONTROL_CASH,
    "cost_class": "US_EQUITY",
    "admissible_input": "PROSPECTIVELY CAPTURED SNAPSHOTS ONLY",
    "inadmissible_input": "any vendor backward strip, restated series or "
                          "as-of reconstruction, whatever it claims",
    "entry_condition": "the ledger holds at least %d observed revisions "
                       "across at least 12 distinct snapshot dates"
                       % MIN_REVISIONS_TO_JUDGE,
    "promotion_allowed": False,
}


def predeclaration_hash() -> str:
    return sha(PREDECLARED_CHALLENGER)


def _r45_state() -> dict:
    """The R45 measurement of the ledger, read from its own artifact."""
    from . import read_json
    from pathlib import Path
    p = Path(r"D:\Stock_Prediction_app_data\macro_event_alpha_r45"
             r"\r45_macro_event_alpha_v1\R45_LANE_RESULTS.json")
    body = read_json(p, default=None)
    if not isinstance(body, dict):
        return {"state": "R45_ARTIFACT_UNAVAILABLE"}
    lane = ((body.get("lanes") or {}).get("L14_ANALYST")) or {}
    return {"state": "READ", "ledger": lane.get("ledger"),
            "revision_frequency": lane.get("revision_frequency"),
            "hard_pit_floor": lane.get("hard_pit_floor"),
            "vendor_backward_strip_reconciliation":
                (lane.get("vendor_backward_strip_reconciliation") or {})}


def _live_ledger_state() -> dict:
    """Release 46.4 - the prospective snapshot ledger AS IT IS NOW, read-only.

    The Release-44 capture owner keeps writing one snapshot directory per
    capture day. Counting them here (never writing) lets the lane report how
    the sample has grown since Release 45 measured it, so an operator reads
    a live number rather than a two-releases-old one.
    """
    try:
        from ..r44 import acquisition as R44AQ
        days = R44AQ._snapshot_dates()
    except Exception as exc:                    # noqa: BLE001 - reported
        return {"state": "UNREADABLE", "error": type(exc).__name__}
    if not days:
        return {"state": "NO_SNAPSHOTS", "n_snapshot_dates": 0}
    return {"state": "READ", "n_snapshot_dates": len(days),
            "first_snapshot": str(days[0]), "last_snapshot": str(days[-1]),
            "span_days": (days[-1] - days[0]).days,
            "read_only": True, "wrote_nothing": True}


def run(campaign_id: str = CAMPAIGN_ID) -> dict:
    prior = _r45_state()
    freq = prior.get("revision_frequency") or {}
    observed = freq.get("n_observed_revisions")
    per_30d = freq.get("revisions_per_series_per_30d")
    span = freq.get("span_days")
    live = _live_ledger_state()

    still_needed = (None if observed is None
                    else max(0, MIN_REVISIONS_TO_JUDGE - int(observed)))
    months_left = None
    if observed is not None and per_30d and span:
        try:
            per_month = float(observed) / max(1.0, float(span) / 30.0)
            if per_month > 0:
                months_left = round(float(still_needed) / per_month, 1)
        except (TypeError, ValueError, ZeroDivisionError):
            months_left = None

    recon = prior.get("vendor_backward_strip_reconciliation") or {}
    body = artifact_body(
        "r46_analyst_lane/1", CALCULATION_OWNER,
        question="how far is the prospective revision ledger from judgeable, "
                 "and is its challenger frozen before it gets there?",
        read_only=READ_ONLY,
        wrote_into_the_ledger=False,
        backfilled=NEVER_BACKFILLED is False,
        never_backfilled=NEVER_BACKFILLED,
        purchased_historical_vintages=False,
        hard_pit_floor=prior.get("hard_pit_floor"),
        ledger=prior.get("ledger"),
        live_ledger=live,
        economic_tracking_rule=(
            "Release 46.4: an observed revision may be tracked economically "
            "in the research shadow system only AFTER its capture instant; "
            "no trade is ever created before the revision was known, and no "
            "historical revision is reconstructed"),
        revision_frequency=freq,
        judgeable_sample={
            "revisions_required": MIN_REVISIONS_TO_JUDGE,
            "revisions_observed": observed,
            "revisions_still_required": still_needed,
            "approx_months_remaining": months_left,
            "state": ("JUDGEABLE" if still_needed == 0
                      else "ACCRUING_ON_TIME" if still_needed is not None
                      else "UNKNOWN"),
        },
        why_this_ledger_is_worth_waiting_for=(
            "it is the only analyst-revision history this estate will own "
            "that cannot have been restated. R45 compared it against the "
            "vendor's own backward strip and matched on %s of %s comparisons, "
            "with %s differences above five cents and a worst case of %s EPS. "
            "Cent-level noise is capture-time convention. A nineteen-cent gap "
            "is a restatement, and a restated series is not point-in-time "
            "evidence however convenient it is."
            % (recon.get("n_matching"), recon.get("n_comparisons"),
               recon.get("n_differences_above_5_cents"),
               recon.get("max_abs_diff"))),
        vendor_backward_strip_reconciliation=recon,
        predeclared_challenger=dict(PREDECLARED_CHALLENGER),
        predeclaration_hash=predeclaration_hash(),
        challenger_frozen_before_the_evidence_exists=True,
        it_enters_through_the_same_door=(
            "when the ledger is judgeable this challenger is registered like "
            "any other - frozen spec, its own forward clock starting at "
            "registration, no credit for the history that qualified it"),
        money_spent_usd=0.0,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
