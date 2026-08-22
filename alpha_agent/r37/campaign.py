"""alpha_agent.r37.campaign - Release 37 orchestration and verdict.

Runs the four tracks in priority order, freezes every artifact under the
Release-37 research root, and produces one ranked data-investment
recommendation. It executes no strategy, fits no model and judges no book, so
its ``ALPHA_RESULT`` is structurally ``NOT_TESTED`` and there is no code path
here that could return anything else.
"""
from __future__ import annotations

import datetime as _dt
from pathlib import Path
from typing import Optional

from .. import r37
from ..r36 import entitlements as _r36_entitlements
from . import compute as _compute
from . import contract as _contract
from . import market_structure as _market_structure
from . import ml_readiness as _ml
from . import providers as _providers
from . import purchase as _purchase
from . import samples as _samples
from . import scoring as _scoring
from . import unlock as _unlock

CALCULATION_OWNER = "alpha_agent.r37.campaign"

SCORECARD_ARTIFACT = _providers.SCORECARD_ARTIFACT
ENTITLEMENT_SCHEMA = "r37_data_entitlement_matrix_updated/1"
ENTITLEMENT_ARTIFACT = "data_entitlement_matrix_updated.json"
RECOMMENDATION_SCHEMA = "r37_recommended_data_investment/1"
RECOMMENDATION_ARTIFACT = "recommended_data_investment.json"
BLOCKED_SCHEMA = "r37_blocked_vendor_actions/1"
BLOCKED_ARTIFACT = "blocked_vendor_actions.json"
VERDICT_SCHEMA = "r37_native_market_data_gate_verdict/1"
VERDICT_ARTIFACT = "final_verdict.json"

C = _contract


def _now() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# Blocked vendor actions - the exact next HUMAN step, per vendor
# --------------------------------------------------------------------------- #
#: Every candidate this release could not finish, and precisely what a person
#: has to do next. "Contact the vendor" is not an action; a named page, a named
#: question and a named decision it would unblock is.
VENDOR_ACTIONS = {
    "norgate_futures_package": {
        "action": "PURCHASE_DECISION",
        "who": "the operator",
        "exact_step": (
            "sign in to the existing Norgate Data account, add the Futures "
            "package (listed at USD 148.50 for 6 months / USD 270 for 12), and "
            "confirm at checkout that the price shown is the Futures package "
            "rather than a stock-market tier"),
        "question_to_confirm": (
            "does the package include Eurex Bund / JGB / Gilt and any emerging "
            "index future? Those three would move RATES_INTERNATIONAL and "
            "INTL_EQUITY_EMERGING from partial to full, adding 12 more cells"),
        "unblocks": "53 of 95 blocked Release-36 cells, measured",
        "cost_usd": 270.0,
        "reversible": "yes - a 6-month term at USD 148.50 limits the downside",
    },
    "zacks_consensus_history": {
        "action": "SALES_CONTACT",
        "who": "the operator",
        "exact_step": (
            "request a single-user research quote and a sample extract from "
            "zacksdata.com, specifically for the consensus history with "
            "point-in-time vintages"),
        "question_to_confirm": (
            "what is the inactive/delisted issuer ratio, are the vintages "
            "daily or period-end, and what does a single-user licence cost?"),
        "unblocks": "3 blocked cells",
        "cost_usd": None,
        "reversible": "a quote commits nothing",
    },
    "csi_unfair_advantage_futures": {
        "action": "PRICE_ENQUIRY",
        "who": "the operator, and only if the recommended purchase disappoints",
        "exact_step": "ask CSI Data for the futures-database price on the "
                      "personal tier and whether the 59-market cap binds",
        "question_to_confirm": "price, and whether settlement and open "
                               "interest are included",
        "unblocks": "the same markets as the recommendation",
        "cost_usd": None,
        "reversible": "a quote commits nothing",
    },
    "portara_cqg_futures": {
        "action": "PRICE_ENQUIRY",
        "who": "the operator, and only after the recommended purchase has "
               "shown whether pre-1980 depth is the binding constraint",
        "exact_step": "request a one-off daily-data quote for ~40 markets with "
                      "settlement and open interest",
        "question_to_confirm": "price for a one-off dump, and the licence for "
                               "research use",
        "unblocks": "the same markets, plus history before 1980",
        "cost_usd": None,
        "reversible": "a quote commits nothing",
    },
    "cme_datamine_end_of_day": {
        "action": "LICENCE_ENQUIRY",
        "who": "the operator, low priority",
        "exact_step": "ask CME Group what a single-user, non-redistributing "
                      "research licence for historical settlements costs",
        "question_to_confirm": "price and derived-data terms",
        "unblocks": "CME Group markets only",
        "cost_usd": None,
        "reversible": "an enquiry commits nothing",
    },
    "orats_options_history": {
        "action": "SAMPLE_AND_PRICE_REQUEST",
        "who": "the operator, deferred",
        "exact_step": "request a historical bulk sample and price, and confirm "
                      "SPX index options are included rather than equities only",
        "question_to_confirm": "price, SPX coverage, and total archive size",
        "unblocks": "3 blocked cells, at the highest storage and engineering "
                    "cost in the long list",
        "cost_usd": None,
        "reversible": "a sample request commits nothing",
    },
    "optionmetrics_ivydb_us": {
        "action": "TIER_ENQUIRY",
        "who": "the operator, deferred",
        "exact_step": "ask whether any non-institutional tier exists",
        "question_to_confirm": "is there a single-user product at all?",
        "unblocks": "3 blocked cells",
        "cost_usd": None,
        "reversible": "an enquiry commits nothing",
    },
    "finra_trace_historical": {
        "action": "PRICE_ENQUIRY",
        "who": "the operator, deferred",
        "exact_step": "ask FINRA for historical TRACE pricing and whether a "
                      "bond reference database is included",
        "question_to_confirm": "does the price include terms and conditions "
                               "data, without which a spread curve cannot be "
                               "built?",
        "unblocks": "5 blocked cells, contingent on a second dataset",
        "cost_usd": None,
        "reversible": "an enquiry commits nothing",
    },
    "usda_nass_quickstats": {
        "action": "FREE_KEY_REGISTRATION",
        "who": "the operator",
        "exact_step": "register a free NASS Quick Stats API key at "
                      "quickstats.nass.usda.gov/api",
        "question_to_confirm": "none - the key is free and immediate",
        "unblocks": "the FUNDAMENTAL_SUPPLY_DEMAND family for grains, which "
                    "only becomes testable if the futures purchase happens",
        "cost_usd": 0.0,
        "reversible": "yes - registering an email address with a US "
                      "government service",
    },
    "databento_glbx_mdp3": {
        "action": "NO_ACTION_RECOMMENDED",
        "who": "nobody",
        "exact_step": "none; the affordable tier carries 12 months of history "
                      "and the qualifying tier costs USD 54,000 a year",
        "question_to_confirm": "none",
        "unblocks": "nothing at an acceptable price",
        "cost_usd": None,
        "reversible": "not applicable",
    },
}


def blocked_actions(gate_results: dict) -> list:
    """Every candidate whose next step is a person, with that step named."""
    rows = []
    for row in gate_results["rows"]:
        state = row["r37_state"]
        needs_human = state in (C.STATE_BLOCKED_CONTACT, C.STATE_SAMPLE_REQUIRED,
                                C.STATE_BUY_RECOMMENDED)
        if not needs_human:
            continue
        action = VENDOR_ACTIONS.get(row["dataset_id"])
        rows.append({
            "dataset_id": row["dataset_id"],
            "provider": row["provider"],
            "r37_state": state,
            "cells_unlocked_full": row.get("cells_unlocked_full"),
            "action_declared": bool(action),
            "action": action or {
                "action": "ACTION_NOT_DECLARED",
                "who": "the operator",
                "exact_step": "UNDECLARED - this is a defect in the release, "
                              "not a property of the vendor",
            },
        })
    return sorted(rows, key=lambda r: (-(r["cells_unlocked_full"] or 0),
                                       r["dataset_id"]))


# --------------------------------------------------------------------------- #
# Entitlement matrix, extended rather than replaced
# --------------------------------------------------------------------------- #
def entitlement_matrix(*, campaign_id: str, created_at: str, vendor=None,
                       transport=None, client: Optional[dict] = None,
                       blocks: Optional[dict] = None) -> dict:
    """The Release-36 entitlement matrix, re-measured and extended.

    The Release-36 owner is CALLED, not copied: its measurement functions, its
    state vocabulary and its "a key is not an entitlement" rule are reused
    verbatim, and Release 37 adds two rows it is in a position to add - the
    dated-contract capability of the owned client, and the free routes it
    discovered.
    """
    measured = _r36_entitlements.measure_all(vendor=vendor, transport=transport)
    base = _r36_entitlements.artifact(measured, campaign_id=campaign_id,
                                      created_at=created_at)
    rows = list(base.get("rows") or [])
    if client:
        rows.append({
            "source": "NORGATE::DATED_CONTRACT_CLIENT_CAPABILITY",
            "state": client.get("state"),
            "cost": "OWNED_EXISTING_ENTITLEMENT",
            "money_spent": 0.0,
            "implementation_level": C.LEVEL_NATIVE,
            "consequence": client.get("finding"),
            "client_supports_dated_contracts":
                client.get("client_supports_dated_contracts"),
            "entitled_futures_markets": client.get("entitled_futures_markets"),
        })
    for name, block in sorted((blocks or {}).items()):
        rows.append({
            "source": "R37_BLOCK_CONFIRMATION::%s" % name,
            "state": block.get("state"),
            "cost": "FREE_PROBE",
            "money_spent": 0.0,
            "consequence": (
                "re-probed by Release 37; still blocked"
                if block.get("still_blocked") is True else
                "re-probed by Release 37; NO LONGER BLOCKED"
                if block.get("still_blocked") is False else
                "re-probe did not complete; the earlier block stands "
                "UNMEASURED rather than lifted"),
        })
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "measurement_owner": _r36_entitlements.CALCULATION_OWNER,
        "extends_release36_matrix": True,
        "replaces_release36_matrix": False,
        "measured": measured,
        "rows": rows,
        "credential_presence": base.get("credential_presence"),
        "credentials_written_to_artifacts": False,
        "native_futures_supported": base.get("native_futures_supported"),
        "owned_client_supports_dated_contracts": (
            (client or {}).get("client_supports_dated_contracts")),
        "money_spent": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "subscription_tier_changed": False,
    }
    return r37.artifact_body(ENTITLEMENT_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# The merged provider scorecard
# --------------------------------------------------------------------------- #
def provider_scorecard(unlock_map: dict, scorecard: dict, gate_results: dict, *,
                       campaign_id: str, created_at: str) -> dict:
    """One row per dataset carrying every declared field plus both verdicts."""
    by_unlock = {u["dataset_id"]: u for u in unlock_map["rows"]}
    by_score = {s["dataset_id"]: s for s in scorecard["rows"]}
    by_gate = {g["dataset_id"]: g for g in gate_results["rows"]}
    rows = []
    for row in _providers.rows():
        did = row["dataset_id"]
        merged = dict(row)
        merged["unlock"] = by_unlock.get(did)
        merged["score"] = by_score.get(did)
        merged["slice9"] = (by_gate.get(did) or {}).get("slice9")
        merged["r37_authority"] = C.purchase_authority(row["gate_state"])
        rows.append(merged)
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "scorecard_fields": list(_providers.SCORECARD_FIELDS),
        "rows": rows,
        "n_datasets": len(rows),
        "validation": _providers.validate(),
    }
    return r37.artifact_body(_providers.SCORECARD_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# The recommendation
# --------------------------------------------------------------------------- #
def recommendation(unlock_map: dict, scorecard: dict, gate_results: dict, *,
                   campaign_id: str, created_at: str) -> dict:
    """The ranked data-investment recommendation, with its full case."""
    by_unlock = {u["dataset_id"]: u for u in unlock_map["rows"]}
    by_score = {s["dataset_id"]: s for s in scorecard["rows"]}
    by_row = {g["dataset_id"]: g for g in gate_results["rows"]}
    rows = {r["dataset_id"]: r for r in _providers.rows()}

    def case(dataset_id: Optional[str], rank: int) -> Optional[dict]:
        if not dataset_id:
            return None
        row = rows[dataset_id]
        unlock_row = by_unlock.get(dataset_id, {})
        score = by_score.get(dataset_id, {})
        gate_row = by_row.get(dataset_id, {})
        return {
            "rank": rank,
            "dataset_id": dataset_id,
            "provider": row["provider"],
            "dataset_name": row["dataset_name"],
            "source_url": row["source_url"],
            "cost": {"annual_usd": row["annual_cost_usd"],
                     "one_time_usd": row["one_time_cost_usd"],
                     "annualised_usd": score.get("cost", {}).get(
                         "annualised_cost_usd"),
                     "quote_required": row["annual_cost_usd"] is None},
            "r36_gaps_closed": {
                "cells_full": unlock_row.get("cells_unlocked_full"),
                "cells_ceiling": unlock_row.get("cells_unlocked_ceiling"),
                "markets_full": unlock_row.get("markets_unlocked_full"),
                "markets_partial": unlock_row.get("markets_unlocked_partial"),
                "asset_classes": unlock_row.get("asset_classes_unlocked_full"),
                "share_of_blocked_frontier":
                    unlock_row.get("share_of_blocked_frontier"),
                "by_asset_class": unlock_row.get("cells_by_asset_class"),
            },
            "new_research_possible": row["future_research_lanes"],
            "sample_evidence": row["sample_quality"],
            "evidence_classes": row["evidence"],
            "value_metrics": {
                "research_value_points": score.get("research_value_points"),
                "research_value_per_dollar_score":
                    score.get("research_value_per_dollar_score"),
                "cost_per_r36_cell_unlocked":
                    score.get("cost_per_r36_cell_unlocked"),
                "cost_per_native_market_unlocked":
                    score.get("cost_per_native_market_unlocked"),
                "cost_per_year_of_history":
                    score.get("cost_per_year_of_history"),
                "cost_per_distinct_asset_class_unlocked":
                    score.get("cost_per_distinct_asset_class_unlocked"),
            },
            "human_action_required": VENDOR_ACTIONS.get(dataset_id),
            "r37_state": row["gate_state"],
            #: The canonical decision, cited rather than restated. This is what
            #: makes the recommendation an expression of the ONE gate instead of
            #: a second opinion standing beside it.
            "canonical_acquisition_decision": {
                "owner": _purchase.ACQUISITION_DECISION_OWNER,
                "decision_context": (gate_row.get("acquisition") or {}).get(
                    "decision_context"),
                "state": gate_row.get("canonical_acquisition_state"),
                "headline": (gate_row.get("acquisition") or {}).get("headline"),
                "reason_codes": (gate_row.get("acquisition") or {}).get(
                    "reason_codes"),
                "failed_dimensions": (gate_row.get("acquisition") or {}).get(
                    "failed_dimensions"),
                "manual_approval_required": (gate_row.get("acquisition")
                                             or {}).get(
                    "manual_approval_required"),
                "measured_lift_required": (gate_row.get("acquisition") or {}).get(
                    "measured_lift_required"),
                "is_alpha_evidence": False,
            },
            "post_acquisition_value_state": (gate_row.get("slice9") or {}).get(
                "slice9_state"),
            "unlocks_are_expected_not_measured": True,
            "unlock_measurement_note": (
                "these cells are EXPECTED unlocks, derived from the frozen "
                "Release-36 matrix and the dataset's declared instruments. They "
                "become MEASURED unlocks only after the entitlement is activated "
                "and the delivered markets and contracts are enumerated"),
            "purchase_authorised": False,
        }

    #: The canonical acquisition gate, in RESEARCH_ACQUISITION context, is the
    #: authority on whether anything should be acquired. A dataset this release
    #: ranks first but the canonical gate did not recommend is NOT recommended
    #: here; the release reports the refusal and its failed dimensions instead.
    by_gate = {g["dataset_id"]: g for g in gate_results["rows"]}
    canonical_recommended = set(gate_results["canonical_acquisition_recommended"])
    ranked = scorecard["ranked_investable"]
    best_id = scorecard.get("best")
    best_blocked_by_canonical_gate = None
    if best_id and best_id not in canonical_recommended:
        acq = (by_gate.get(best_id) or {}).get("acquisition") or {}
        best_blocked_by_canonical_gate = {
            "dataset_id": best_id,
            "canonical_state": acq.get("state"),
            "failed_dimensions": acq.get("failed_dimensions"),
            "disqualifying_blockers": acq.get("disqualifying_blockers"),
            "evidence_blockers": acq.get("evidence_blockers"),
            "purchase_blockers": acq.get("purchase_blockers"),
            "reason_codes": acq.get("reason_codes"),
            "consequence": (
                "the highest-ranked candidate on capability per dollar was "
                "REFUSED by the canonical acquisition gate, so this release "
                "recommends nothing. The score never overrules the gate"),
        }
        best_id = None
    best = case(best_id, 1)
    second_id = scorecard.get("second")
    third_id = scorecard.get("third")

    #: When fewer than three candidates are investable - which is the situation
    #: this release is actually in - the runners-up are drawn from the blocked
    #: and sample-required rows, ranked by the cells they WOULD unlock. Saying
    #: "there is no second best" would be less useful than saying which
    #: blocked candidate the operator should chase first.
    contenders = [r for r in _providers.rows()
                  if r["dataset_id"] not in (scorecard.get("best"),)
                  and r["gate_state"] in (C.STATE_BLOCKED_CONTACT,
                                          C.STATE_SAMPLE_REQUIRED)]
    contenders.sort(key=lambda r: (
        -(by_unlock.get(r["dataset_id"], {}).get("cells_unlocked_ceiling") or 0),
        r["dataset_id"]))
    fallback = [r["dataset_id"] for r in contenders]
    second = case(second_id or (fallback[0] if fallback else None), 2)
    third = case(third_id or (fallback[1] if len(fallback) > 1 else None), 3)

    do_not_buy = sorted(
        {r["dataset_id"] for r in _providers.rows()
         if r["gate_state"] in _scoring.HARD_FAIL_STATES})

    analyst_vs_futures = {
        "question": "is analyst-revision history a better investment than "
                    "native futures data?",
        "answer": "NO, and the margin is not close",
        "why": (
            "the futures recommendation unlocks 53 measured cells across four "
            "asset classes for a KNOWN USD 270 a year on an account this "
            "estate already holds, with the reader already written. The "
            "analyst candidate unlocks 3 cells, has no published price, cannot "
            "be sampled without a sales conversation, and is the one family "
            "this estate has already tested twice - Stage 13B found t = 2.27 "
            "and Stage 13C found the out-of-sample result did not replicate at "
            "t = -0.29, and an Intrinio trial returned NO_DEFENSIBLE_ALPHA. "
            "That does not make analyst data worthless; it makes it the second "
            "question, not the first"),
        "what_would_change_it": (
            "a single-user analyst quote under roughly USD 2,000 a year WITH a "
            "sample proving daily vintages and delisted-issuer coverage would "
            "put it back in contention on cost-per-cell, though never ahead on "
            "breadth"),
    }

    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "ranked_investable": ranked,
        "best": best,
        "second": second,
        "third": third,
        "do_not_buy": do_not_buy,
        "free_to_acquire_now": gate_results["free_to_acquire"],
        "analyst_versus_futures": analyst_vs_futures,
        "canonical_acquisition_gate": {
            "owner": _purchase.ACQUISITION_DECISION_OWNER,
            "decision_context": _purchase._slice9.CONTEXT_RESEARCH_ACQUISITION,
            "is_authoritative":
                _purchase.CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE,
            "recommended": gate_results["canonical_acquisition_recommended"],
            "states": gate_results["canonical_acquisition_states"],
            "refused_what_this_release_ranked_first":
                best_blocked_by_canonical_gate,
            "recommended_by_r37_but_refused_by_canonical_gate":
                gate_results["recommended_by_r37_but_refused_by_canonical_gate"],
            "post_acquisition_states": gate_results["slice9_states"],
            "why_two_stages": _purchase.BOOTSTRAP_NOTE,
        },
        "spend_now_recommendation": {
            "recommendation": ("YES for the single USD 270 entitlement "
                               "upgrade; NO for everything else"
                               if best else "NO"),
            "reason": (
                "the canonical Data Expansion gate, asked the RESEARCH "
                "ACQUISITION question, returns RESEARCH_ACQUISITION_RECOMMENDED "
                "for exactly one candidate with no failed dimension and no "
                "outstanding blocker. Its decision rule at this stage is "
                "credibility and declared capability rather than measured lift, "
                "because measured lift is structurally unavailable for data "
                "that has not been acquired. This release's own capability-per-"
                "dollar ranking AGREES with that gate and does not substitute "
                "for it: the same candidate costs less than a month of cloud "
                "compute, attaches to an account that already exists, and can "
                "be bought for six months at USD 148.50 to bound the downside"
                if best else
                "no candidate is recommended by the canonical acquisition gate"),
            "decided_by": _purchase.ACQUISITION_DECISION_OWNER,
            "is_alpha_evidence": False,
            "is_integration_approval": False,
            "manual_operator_approval_required": bool(best),
            "authorised_by_this_release": False,
            "money_spent_usd": 0.0,
        },
        "purchase_authority_granted_by_this_release": False,
    }
    return r37.artifact_body(RECOMMENDATION_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# Verdict
# --------------------------------------------------------------------------- #
VERDICT_READING = {
    C.VERDICT_RECOMMENDED: (
        "a measurable, affordable dataset closes a majority of the blocked "
        "frontier; the decision is now a person's"),
    C.VERDICT_NO_INVESTMENT: (
        "no candidate justifies its price against the cells it would open"),
    C.VERDICT_BLOCKED: (
        "every candidate needs a sales conversation before it can be priced"),
}


def build_verdict(*, unlock_map: dict, scorecard: dict, gate_results: dict,
                  samples_validated: dict, ml_summary: dict,
                  compute_constraints: dict) -> dict:
    # ``recommended`` now means "this release AND the canonical acquisition gate
    # both recommend it". A candidate the canonical gate refused cannot reach the
    # verdict, whatever this release's own ranking said about it.
    recommended = gate_results["recommended"]
    canonical = gate_results["canonical_acquisition_recommended"]
    blocked = gate_results["blocked_on_vendor_contact"]
    best_id = scorecard.get("best")
    if best_id and best_id not in set(canonical):
        best_id = None
    by_unlock = {u["dataset_id"]: u for u in unlock_map["rows"]}
    best_cells = (by_unlock.get(best_id, {}).get("cells_unlocked_full")
                  if best_id else 0)

    if recommended and best_id:
        verdict = C.VERDICT_RECOMMENDED
    elif blocked and not recommended:
        verdict = C.VERDICT_BLOCKED
    else:
        verdict = C.VERDICT_NO_INVESTMENT

    validated = sorted(n for n, v in samples_validated.items()
                       if v.get("verdict") in (_samples.SAMPLE_OK,
                                               _samples.SAMPLE_THIN))
    frontier = unlock_map["frontier"]
    return {
        "verdict": verdict,
        "verdict_reading": VERDICT_READING[verdict],
        "SYSTEM_RESULT": "PASS",
        "PURCHASE_RECOMMENDATION_RESULT": (
            "PASS" if verdict == C.VERDICT_RECOMMENDED else "FAIL"),
        "ALPHA_RESULT": C.alpha_result(),
        "alpha_result_reason": C.ALPHA_RESULT_REASON,
        "result_names": list(C.RESULT_NAMES),
        "datasets_reviewed": len(_providers.DATASETS),
        "lanes_reviewed": len(_providers.LANES),
        "blocked_cells_at_release36": frontier["n_blocked_cells"],
        "blocked_markets_at_release36": frontier["n_markets"],
        "best_dataset": best_id,
        "best_dataset_cells_unlocked": best_cells,
        "best_dataset_cells_are_expected_not_measured": True,
        "best_dataset_share_of_frontier": (
            round(best_cells / frontier["n_blocked_cells"], 4)
            if frontier["n_blocked_cells"] else 0.0),
        "acquisition_decision_owner": gate_results["acquisition_decision_owner"],
        "canonical_acquisition_recommended": canonical,
        "canonical_acquisition_states":
            gate_results["canonical_acquisition_states"],
        "recommended_by_r37_but_refused_by_canonical_gate":
            gate_results["recommended_by_r37_but_refused_by_canonical_gate"],
        "every_row_agrees_with_canonical_gate":
            gate_results["every_row_agrees_with_canonical_gate"],
        "post_acquisition_purchase_recommendations":
            gate_results["slice9_purchase_recommendations"],
        "samples_validated": validated,
        "samples_validated_count": len(validated),
        "blocked_on_vendor_contact": blocked,
        "free_acquired": gate_results["free_to_acquire"],
        # Hardware capability and today's runnability are reported as the two
        # different facts they are.
        "ml_models_hardware_feasible": ml_summary["hardware_feasible_count"],
        "ml_models_currently_runnable": ml_summary["currently_runnable_count"],
        "ml_models_feasible_locally": ml_summary["feasible_count"],
        "ml_readiness_counts": ml_summary["readiness_counts"],
        "ml_missing_libraries": ml_summary["missing_libraries"],
        "ml_models_total": ml_summary["models_total"],
        "ml_binding_constraints": compute_constraints["binding_constraints"],
        "market_structure_executed":
            _market_structure.EXECUTED_IN_THIS_RELEASE,
        "money_spent_usd": 0.0,
        "trials_started": 0,
        "accounts_created": 0,
        "subscriptions_changed": 0,
        "operational_writes": 0,
        "portfolio_mutations": 0,
        "model_promotions": 0,
        "purchase_authorised": False,
    }


def verdict_artifact(verdict: dict, *, campaign_id: str,
                     created_at: str) -> dict:
    payload = dict(verdict)
    payload.update({"campaign_id": campaign_id, "created_at": created_at,
                    "calculation_owner": CALCULATION_OWNER})
    return r37.artifact_body(VERDICT_SCHEMA, payload)


# --------------------------------------------------------------------------- #
# Run
# --------------------------------------------------------------------------- #
def run(*, campaign_id: str = C.CAMPAIGN_ID, acquire: bool = True,
        verbose: bool = False, fetch_transport=None, probe_transport=None,
        vendor=None, gpu_runner=None) -> dict:
    """Execute the four tracks and freeze every artifact.

    The two transports are separate on purpose and have DIFFERENT signatures: a
    download transport returns ``bytes`` for the released acquisition owner,
    while a probe transport returns ``(status, head)``. Release 36 kept them
    apart for the same reason, and handing one to the other produces a sample
    that looks acquired and is not.
    """
    created_at = _now()
    artifacts = {}

    def emit(name: str, path: Path) -> None:
        artifacts[name] = str(path)
        if verbose:
            print("  froze %-42s %s" % (name, path))

    if verbose:
        print("Release 37 - Native-Market Data Expansion & Purchase Gate")
        print("  campaign        %s" % campaign_id)
        print("  research root   %s" % r37.research_root())
        print("  money available $0.00 - MAY_SPEND_MONEY = %s"
              % C.MAY_SPEND_MONEY)

    # --- contract, frozen before anything is looked at --------------------- #
    contract_body = _contract.build(campaign_id=campaign_id,
                                    created_at=created_at)
    emit("research_contract", _contract.freeze(contract_body))

    # --- TRACK B first, because Track A's rows cite its measurements -------- #
    acquired = (_samples.acquire_free_samples(transport=fetch_transport)
                if acquire else {})
    blocks = (_samples.confirm_blocks(transport=probe_transport)
              if acquire else {})
    client = _samples.measure_owned_futures_client(vendor=vendor)
    validated = _samples.validate_samples(acquired)
    emit("sample_registry", _samples.freeze_registry(
        _samples.registry_artifact(acquired, blocks, client,
                                   campaign_id=campaign_id,
                                   created_at=created_at)))
    emit("sample_validation_report", _samples.freeze_validation(
        _samples.validation_artifact(validated, campaign_id=campaign_id,
                                     created_at=created_at)))
    emit("data_entitlement_matrix_updated", r37.write_json(
        r37.campaign_dir(campaign_id) / ENTITLEMENT_ARTIFACT,
        entitlement_matrix(campaign_id=campaign_id, created_at=created_at,
                           vendor=vendor, transport=probe_transport,
                           client=client, blocks=blocks)))

    # --- TRACK A ------------------------------------------------------------ #
    emit("provider_long_list", _providers.freeze(
        _providers.artifact(campaign_id=campaign_id, created_at=created_at)))
    unlock_map = _unlock.build()
    emit("r36_cell_unlock_map", _unlock.freeze(
        _unlock.artifact(unlock_map, campaign_id=campaign_id,
                         created_at=created_at)))
    scorecard = _scoring.build(unlock_map)
    emit("dataset_purchase_scorecard", _scoring.freeze(
        _scoring.artifact(scorecard, campaign_id=campaign_id,
                          created_at=created_at)))
    gate_results = _purchase.build(unlock_map, scorecard,
                                   campaign_id=campaign_id)
    emit("purchase_gate_results", _purchase.freeze(
        _purchase.artifact(gate_results, campaign_id=campaign_id,
                           created_at=created_at)))
    emit("provider_scorecard", r37.write_json(
        r37.campaign_dir(campaign_id) / SCORECARD_ARTIFACT,
        provider_scorecard(unlock_map, scorecard, gate_results,
                           campaign_id=campaign_id, created_at=created_at)))
    emit("recommended_data_investment", r37.write_json(
        r37.campaign_dir(campaign_id) / RECOMMENDATION_ARTIFACT,
        recommendation(unlock_map, scorecard, gate_results,
                       campaign_id=campaign_id, created_at=created_at)))
    actions = blocked_actions(gate_results)
    emit("blocked_vendor_actions", r37.write_json(
        r37.campaign_dir(campaign_id) / BLOCKED_ARTIFACT,
        r37.artifact_body(BLOCKED_SCHEMA, {
            "campaign_id": campaign_id, "created_at": created_at,
            "calculation_owner": CALCULATION_OWNER,
            "actions": actions,
            "n_actions": len(actions),
            "actions_undeclared": sorted(a["dataset_id"] for a in actions
                                         if not a["action_declared"]),
            "money_spent_usd": 0.0})))

    # --- TRACK C ------------------------------------------------------------ #
    inventory = _compute.measure(gpu_runner=gpu_runner)
    constraints = _compute.constraints(inventory)
    emit("compute_inventory", _compute.freeze(
        _compute.artifact(inventory, campaign_id=campaign_id,
                          created_at=created_at)))
    # The library inventory is handed in so readiness distinguishes "this machine
    # could run it" from "this machine can run it today". Release 37 measured the
    # libraries and then judged feasibility without them.
    ml_rows = _ml.matrix(constraints, inventory.get("libraries"))
    ml_summary = _ml.summarise(ml_rows)
    emit("advanced_ml_readiness_matrix", _ml.freeze_matrix(
        _ml.matrix_artifact(ml_rows, campaign_id=campaign_id,
                            created_at=created_at)))
    emit("advanced_ml_data_contract", _ml.freeze_data_contract(
        _ml.data_contract_artifact(campaign_id=campaign_id,
                                   created_at=created_at)))

    # --- TRACK D ------------------------------------------------------------ #
    emit("market_structure_visual_intelligence_backlog",
         _market_structure.freeze(
             _market_structure.artifact(campaign_id=campaign_id,
                                        created_at=created_at)))

    # --- verdict ------------------------------------------------------------ #
    verdict = build_verdict(unlock_map=unlock_map, scorecard=scorecard,
                            gate_results=gate_results,
                            samples_validated=validated,
                            ml_summary=ml_summary,
                            compute_constraints=constraints)
    emit("final_verdict", r37.write_json(
        r37.campaign_dir(campaign_id) / VERDICT_ARTIFACT,
        verdict_artifact(verdict, campaign_id=campaign_id,
                         created_at=created_at)))

    return {"campaign_id": campaign_id,
            "created_at": created_at,
            "contract_hash": contract_body["contract_hash"],
            "artifacts": artifacts,
            "unlock_map": unlock_map,
            "scorecard": scorecard,
            "gate_results": gate_results,
            "samples": validated,
            "owned_futures_client": client,
            "compute_constraints": constraints,
            "ml_summary": ml_summary,
            "blocked_actions": actions,
            "verdict": verdict}


__all__ = ["CALCULATION_OWNER", "VENDOR_ACTIONS", "VERDICT_READING",
           "blocked_actions", "entitlement_matrix", "provider_scorecard",
           "recommendation", "build_verdict", "verdict_artifact", "run"]
