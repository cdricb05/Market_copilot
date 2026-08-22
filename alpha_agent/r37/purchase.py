"""alpha_agent.r37.purchase - Release 37's purchase-gate COMPOSITION owner.

There is no Release-37 purchase gate, and that is the point of this module.

Two gates already exist and both are canonical:

* :mod:`paper_trader.engine.data_expansion_gate` (Slice 9), composed through
  :mod:`paper_trader.api.data_expansion`. It answers BOTH acquisition questions,
  selected by an explicit ``decision_context``: Stage A *"is this worth paying to
  LEARN?"* and Stage B *"did the measured evidence earn continued purchase?"*
* :mod:`alpha_agent.r32.purchase_gate` - the ten information conditions. It
  answers *"is this information gap worth closing at all?"*

This module assembles each candidate into both gates' input shapes, runs them,
and records what they said. It never persists into the Slice-9 evaluation store,
because a research release must not write an operational artifact; the results
land in Release 37's own immutable artifact instead.

**The tension Release 37 surfaced, and how 37.1 resolved it.** Stage B requires
measured out-of-sample lift before it will say ``PURCHASE_RECOMMENDED``. Release
37 had measured no lift for any candidate, because the lift of data you cannot
obtain cannot be measured - that is what "blocked" means. Stage B therefore
returned ``INSUFFICIENT_EVIDENCE`` for every row, correctly, and Release 37 left
its own capability judgement standing beside it. Correct, but two apparently
competing acquisition truths.

Release 37.1 does not soften Stage B and does not promote the release's own
opinion. It uses the canonical gate's **RESEARCH_ACQUISITION** context, which is
the question Release 37 was actually asking all along, and which the ONE canonical
gate now models explicitly. The Stage-A result is the AUTHORITY for "should we
acquire this?"; the Release-37 row state is a triage label that may agree with it
and may never overrule it. ``CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE`` is
True, ``SLICE9_RESULT_MAY_BE_OVERRIDDEN`` is False, and both stages are recorded
verbatim so the reader can see the same dataset answered under both questions.
"""
from __future__ import annotations

from typing import Optional

from .. import r37
from ..r32 import purchase_gate as _r32_gate
from ...api import data_expansion as _slice9
from . import contract as _contract
from . import providers as _providers

CALCULATION_OWNER = "alpha_agent.r37.purchase"
SCHEMA = "r37_purchase_gate_results/1"
ARTIFACT_NAME = "purchase_gate_results.json"

C = _contract

#: The Release-37 state is a CAPABILITY judgement. The Stage-B state is a
#: MEASURED-LIFT judgement. They are different questions and are never merged.
SLICE9_RESULT_MAY_BE_OVERRIDDEN = False
R37_STATE_IS_A_CAPABILITY_JUDGEMENT = True
R37_STATE_IS_A_MEASURED_LIFT_JUDGEMENT = False

#: Release 37.1: the ONE canonical gate, asked the acquisition question, is the
#: authority on whether a dataset should be acquired. This release's own row
#: state is a triage label. If they disagree, the canonical gate wins and the
#: disagreement is reported rather than resolved in this release's favour.
CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE = True
R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED = False
ACQUISITION_DECISION_OWNER = "engine.data_expansion_gate (RESEARCH_ACQUISITION)"

#: Why Stage B is INSUFFICIENT_EVIDENCE for every row, and why that is no longer
#: the end of the story. Stated once so no reader mistakes either half for a
#: defect in the gate, the release, or the recommendation.
BOOTSTRAP_NOTE = (
    "Stage B (POST_ACQUISITION_VALUE) requires measured out-of-sample lift. No "
    "candidate here can supply it, because every one of them is blocked - the "
    "lift of data that cannot be obtained cannot be measured. "
    "INSUFFICIENT_EVIDENCE is the correct answer to the question Stage B asks, "
    "and it is recorded verbatim rather than engineered around. Stage A "
    "(RESEARCH_ACQUISITION) asks the question this release is actually asking - "
    "is the dataset credible and valuable enough to justify paying to LEARN - "
    "and it is strict about every gate except the one that cannot exist yet."
)

#: Universe sizes handed to the Slice-9 gate, with the source of each number.
#: Declared here rather than in the long list because a universe size is an
#: input to ONE gate's breadth dimension, not a property of the dataset.
UNIVERSE = {
    # vendor content tables: ~100 futures markets across 11 exchange groups
    "norgate_futures_package": (100, 0.90),
    "databento_glbx_mdp3": (200, 0.95),         # exchange-wide CME Group
    "firstrate_data_futures": (122, 0.60),      # vendor's stated ticker count
    "csi_unfair_advantage_futures": (59, 0.55),  # personal-tier market cap
    "portara_cqg_futures": (200, 0.95),
    "cme_datamine_end_of_day": (150, 0.60),     # CME Group only, no ICE/Eurex
    "cboe_datashop_vx_futures": (1, 0.05),      # one market
    "cboe_cfe_volume_open_interest_free": (1, 0.05),
    "zacks_consensus_history": (6000, 0.85),
    "lseg_ibes_estimates": (20000, 0.95),
    "free_analyst_estimate_tiers": (5000, 0.50),
    "intrinio_analyst_estimates": (3000, 0.50),
    "orats_options_history": (4000, 0.90),
    "optionmetrics_ivydb_us": (5000, 0.95),
    "finra_trace_historical": (20000, 0.90),
    "binance_public_data_archive": (300, 0.40),
    "lbma_precious_metal_benchmarks": (3, 0.05),
    "nyfed_primary_dealer_positions": (40, 0.20),
    "norgate_owned_entitlement": (41833, 0.99),  # measured live: live+delisted
    "usda_nass_quickstats": (30, 0.30),
}

#: Minimum universe a lane's research actually needs. A futures cross-section is
#: twenty markets, not five hundred names; applying an equity floor to a futures
#: archive would fail it for a reason that has nothing to do with the data.
LANE_MIN_UNIVERSE = {
    _providers.LANE_FUTURES: (20, 0.50),
    _providers.LANE_ANALYST: (C.ANALYST_MIN_ISSUERS, 0.60),
    _providers.LANE_OPTIONS: (500, 0.60),
    _providers.LANE_CREDIT: (500, 0.60),
    _providers.LANE_CRYPTO: (10, 0.40),
    _providers.LANE_OTHER: (3, 0.05),
}

LANE_MIN_HISTORY = {
    _providers.LANE_FUTURES: C.FUTURES_MIN_HISTORY_YEARS_FLOOR,
    _providers.LANE_ANALYST: C.ANALYST_MIN_HISTORY_YEARS,
    _providers.LANE_OPTIONS: 10,
    _providers.LANE_CREDIT: 10,
    _providers.LANE_CRYPTO: 5,
    _providers.LANE_OTHER: 10,
}

_PIT_GUARANTEE = {
    "TRUE_POINT_IN_TIME": "EFFECTIVE_DATED",
    "OBSERVED_AS_PUBLISHED": "SNAPSHOT_ONLY",
    "REVISED_HISTORY": "SNAPSHOT_ONLY",
    "CURRENT_SNAPSHOT_ONLY": "CURRENT_ONLY",
    "UNKNOWN": None,
}
_SURVIVORSHIP_RISK = {
    "DISCONTINUED_RETAINED": "LOW",
    "NOT_APPLICABLE": "LOW",
    "PARTIAL_INACTIVE_COVERAGE": "HIGH",
    "SURVIVORS_ONLY": "SURVIVORSHIP_ONLY",
    "UNKNOWN": None,
}
_INACTIVE_SUPPORT = {
    "DISCONTINUED_RETAINED": True,
    "NOT_APPLICABLE": None,
    "PARTIAL_INACTIVE_COVERAGE": None,
    "SURVIVORS_ONLY": False,
    "UNKNOWN": None,
}
_RESEARCH_USE = {"RESEARCH_USE_CLEAR": True, "RESEARCH_USE_AMBIGUOUS": None,
                 "RESEARCH_USE_PROHIBITED": False, "UNKNOWN": None}
_RESTATEMENT = {"TRUE_POINT_IN_TIME": "PIT_PRESERVED",
                "OBSERVED_AS_PUBLISHED": "PIT_PRESERVED",
                "REVISED_HISTORY": "RESTATED",
                "CURRENT_SNAPSHOT_ONLY": "BACKFILLED",
                "UNKNOWN": None}


#: The owned/free substitute this estate has already built and measured. Stage A
#: requires that owned data was tried first, and this is the sentence that
#: answers it - Release 36 built every native lane owned or free data supported.
OWNED_SUBSTITUTE_TRIED = (
    "Release 36 built every native lane owned or free data supported: FX "
    "forwards across 20 currencies and five dated NYMEX energy curves, plus "
    "Release 35's six free orthogonal information families. Release 37 "
    "re-measured the owned Norgate entitlement live and confirmed it serves ONE "
    "futures market. The blocked frontier is what remains AFTER that")


def acquisition_case(row: dict, unlock_row: dict) -> dict:
    """Release 37's explanatory metrics, expressed as the canonical Stage-A input.

    This is the join between the two corrections: the release keeps computing
    cells unlocked, cost per cell and economic distinctness, and those numbers
    now FEED the canonical acquisition gate instead of supporting a parallel
    verdict. Nothing here is a judgement - every field is a declaration the gate
    then judges, and an undeclared field produces INSUFFICIENT_EVIDENCE rather
    than a pass.
    """
    cells = unlock_row.get("cells_unlocked_full")
    ceiling = unlock_row.get("cells_unlocked_ceiling")
    markets = unlock_row.get("markets_unlocked_full") or []
    classes = unlock_row.get("asset_classes_unlocked_full") or []
    distinct = str(row.get("incremental_distinctness") or "UNKNOWN")
    declared = ("HIGH" if distinct.startswith("HIGH")
                else "MEDIUM" if distinct.startswith("MEDIUM")
                else "LOW" if distinct.startswith("LOW")
                else "NONE" if distinct.startswith("NONE")
                else "UNKNOWN")
    return {
        "capability_unlocked_count": cells,
        #: A dataset that reaches a market only at a weaker implementation level
        #: is limited rather than useless, and the canonical gate distinguishes
        #: the two. Declaring both keeps a ceiling-only candidate a CANDIDATE
        #: instead of collapsing it into the same answer as one that unlocks
        #: nothing at all.
        "capability_unlocked_ceiling_count": ceiling,
        "capability_unlocked_units": "RELEASE36_BLOCKED_CELLS",
        "capability_unlocked_detail": (
            "%d of Release 36's 95 blocked cells become executable, across %s "
            "(%s). These are EXPECTED unlocks derived from the frozen "
            "Release-36 matrix and the dataset's declared instruments; they "
            "become MEASURED unlocks only after the entitlement is activated "
            "and the delivered markets are enumerated"
            % (cells or 0, ", ".join(markets) or "no market",
               ", ".join(classes) or "no asset class")
            if cells else None),
        "capability_is_expected_not_measured": True,
        "expected_incremental_distinctness": declared,
        "expected_distinctness_basis": row.get("incremental_distinctness"),
        "owned_substitute_tried": True,
        "owned_substitute_detail": OWNED_SUBSTITUTE_TRIED,
        "bounded_evaluation_declared": True,
        "bounded_evaluation_detail": (
            "Release 38 would execute the unlocked cells with the Release-36 "
            "machinery - trailing statistics, per-lane controls, cost on traded "
            "notional, a minimum detectable effect on every failure - and is "
            "able to return DO_NOT_BUY_AGAIN / NO_DEFENSIBLE_ALPHA, as five "
            "consecutive releases have"),
    }


def catalog_entry(row: dict, unlock_row: dict) -> dict:
    """Assemble one Release-37 candidate into the Slice-9 catalog shape."""
    size, coverage = UNIVERSE.get(row["dataset_id"], (None, None))
    min_size, min_cov = LANE_MIN_UNIVERSE.get(row["lane"], (None, None))
    identity = row.get("identity_class")
    owned = row["gate_state"] == C.STATE_ENTITLEMENT_OWNED
    acquired = row["gate_state"] == C.STATE_FREE_ACQUIRE_NOW
    return {
        "dataset_id": row["dataset_id"],
        "provider": row["provider"],
        "dataset_name": row["dataset_name"],
        "data_category": row["lane"].lower(),
        "feature_families": sorted({f for d in (unlock_row.get("detail") or [])
                                    for f in (d.get("families") or [])}),
        "dataset": {
            "pit_guarantee": _PIT_GUARANTEE.get(row.get("pit_class")),
            "publication_timestamps": row.get("pit_class") in (
                "TRUE_POINT_IN_TIME", "OBSERVED_AS_PUBLISHED"),
            "contains_future_leakage": False,
            "history_years": row.get("history_years"),
            "inactive_delisted_support": _INACTIVE_SUPPORT.get(
                row.get("survivorship_class")),
            "survivorship_bias_risk": _SURVIVORSHIP_RISK.get(
                row.get("survivorship_class")),
            "revision_history_support": row["lane"] == _providers.LANE_ANALYST,
            "estimates_are_current_consensus": (
                True if row.get("pit_class") == "CURRENT_SNAPSHOT_ONLY"
                and row["lane"] == _providers.LANE_ANALYST else None),
            "restatement_backfill_behavior": _RESTATEMENT.get(
                row.get("pit_class")),
            "universe_size": size,
            "universe_coverage_ratio": coverage,
            "identifier_quality": identity,
            "identifier_mapping_available": identity != "UNKNOWN",
            "update_frequency": row.get("frequency"),
            "operational_reliability": (
                "HIGH" if row.get("confidence_in_vendor_claims") == "HIGH"
                else "MEDIUM"),
            "implementation_complexity": row.get("implementation_complexity"),
            "licensing": {
                "research_use_allowed": _RESEARCH_USE.get(
                    row.get("licence_class")),
                "prohibited": row.get("licence_class") ==
                "RESEARCH_USE_PROHIBITED",
                "redistribution_allowed": False,
                "notes": row.get("licence_constraints"),
            },
            "cost": {
                "annual_usd": row.get("annual_cost_usd"),
                "one_time_usd": row.get("one_time_cost_usd"),
                "monthly_usd": row.get("monthly_cost_usd"),
                "cost_known": row.get("annual_cost_usd") is not None,
                "notes": row.get("commercial_terms"),
            },
            "existing_entitlement_state": "ENTITLED" if owned else "NONE",
            "acquisition_state": ("PURCHASED" if owned else
                                  "ACQUIRED" if acquired else "NOT_ACQUIRED"),
            "technical_integration_state": ("INTEGRATED" if owned
                                            else "NOT_INTEGRATED"),
        },
        "research_requirements": {
            "requires_point_in_time": True,
            "requires_historical_revisions":
                row["lane"] == _providers.LANE_ANALYST,
            "requires_full_universe_integrity":
                row.get("survivorship_class") != "NOT_APPLICABLE",
            "min_history_years": LANE_MIN_HISTORY.get(row["lane"]),
            "min_universe_size": min_size,
            "min_universe_coverage": min_cov,
            "min_effective_sample": 24,
            "research_intent": (
                "Unlock Release-36 blocked cells: %d fully, %d at ceiling, "
                "across %s."
                % (unlock_row.get("cells_unlocked_full") or 0,
                   unlock_row.get("cells_unlocked_ceiling") or 0,
                   ", ".join(unlock_row.get("asset_classes_unlocked_full")
                             or ["no asset class"]))),
        },
        #: Deliberately EMPTY. Release 37 measured no research lift, and
        #: supplying an optimistic placeholder is how a gate gets talked into a
        #: purchase it has no evidence for.
        "evidence": {
            "available": False,
            "study_reference": "Release 37 ran no experiment; see "
                               "docs/RELEASE37_NATIVE_MARKET_DATA_GATE.md",
            "evidence_owner": "alpha_agent.experiment_contracts",
        },
        #: The Stage-A declarations. Ignored entirely by the post-acquisition
        #: context, which is why supplying them costs Stage B nothing.
        "acquisition_case": acquisition_case(row, unlock_row),
        "existing_ownership": {
            "entitlement_state": "ENTITLED" if owned else "NONE",
            "owned_baseline": ("alpha_agent.r36 native lanes: FX forwards and "
                               "five dated NYMEX energy curves"),
        },
        "provenance": "alpha_agent.r37.providers row, evidence classes %s"
                      % ",".join(row.get("evidence") or []),
        "notes": row.get("gate_reason"),
    }


def _run_canonical_gate(row: dict, unlock_row: dict, *, context: str) -> dict:
    """Run the ONE canonical gate in one decision context; record it verbatim."""
    entry = catalog_entry(row, unlock_row)
    try:
        # The composition owner returns {"input_contract", "evaluation"}; the
        # kernel result is the second half, and reading the wrapper as though
        # it were the result is how every field silently becomes None.
        outcome = _slice9.run_evaluation(catalog_entry=entry,
                                         decision_context=context)
        result = outcome["evaluation"]
    except Exception as exc:  # noqa: BLE001 - an unrunnable gate fails visibly
        return {"dataset_id": row["dataset_id"],
                "decision_context": context,
                "state": "UNMEASURABLE",
                "error": "%s: %s" % (type(exc).__name__, str(exc)[:160]),
                "gate_owner": _slice9.COMPOSITION_OWNER}
    recommendation = result.get("recommendation") or {}
    return {
        "dataset_id": row["dataset_id"],
        "gate_owner": _slice9.COMPOSITION_OWNER,
        "kernel_owner": result.get("calculation_owner"),
        "policy_version": result.get("policy_version"),
        "decision_context": result.get("decision_context"),
        "decision_context_question": result.get("decision_context_question"),
        "measured_lift_required": result.get("measured_lift_required"),
        "state": result.get("recommendation_state"),
        "headline": recommendation.get("headline"),
        "reason_codes": recommendation.get("reason_codes"),
        "failed_dimensions": result.get("failed_dimensions"),
        "watch_dimensions": result.get("watch_dimensions"),
        "unknown_dimensions": result.get("unknown_dimensions"),
        "disqualifying_blockers": result.get("disqualifying_blockers"),
        "evidence_blockers": result.get("evidence_blockers"),
        "purchase_blockers": result.get("purchase_blockers"),
        "dimension_summary": result.get("dimension_summary"),
        "evaluation_hash": result.get("evaluation_hash"),
        "manual_approval_required":
            recommendation.get("manual_approval_required"),
        "auto_purchase_allowed": recommendation.get("auto_purchase_allowed"),
        "auto_acquisition_allowed":
            recommendation.get("auto_acquisition_allowed"),
        "is_research_acquisition_recommendation":
            recommendation.get("is_research_acquisition_recommendation"),
        "is_alpha_evidence": recommendation.get("is_alpha_evidence"),
        "persisted_to_slice9_store": False,
    }


def slice9_result(row: dict, unlock_row: dict) -> dict:
    """Stage B - the post-acquisition value question, recorded verbatim.

    Preserved unchanged, including its INSUFFICIENT_EVIDENCE answer. The release
    reports what the strict measured-lift gate said even though that is not the
    question it is asking, because deleting the inconvenient half of a pair of
    results is how a release stops being checkable.
    """
    out = _run_canonical_gate(row, unlock_row,
                              context=_slice9.CONTEXT_POST_ACQUISITION_VALUE)
    # The historical field names are kept so nothing downstream has to be edited.
    out["slice9_state"] = out.get("state")
    out["slice9_headline"] = out.get("headline")
    out["slice9_reason_codes"] = out.get("reason_codes")
    return out


def acquisition_result(row: dict, unlock_row: dict) -> dict:
    """Stage A - the canonical acquisition decision, and the authority here."""
    return _run_canonical_gate(row, unlock_row,
                               context=_slice9.CONTEXT_RESEARCH_ACQUISITION)


# --------------------------------------------------------------------------- #
# The ten-condition information gate
# --------------------------------------------------------------------------- #
def information_gap(row: dict, unlock_row: dict) -> dict:
    """One Release-37 candidate expressed as a Release-32 information gap."""
    cells = unlock_row.get("cells_unlocked_full") or 0
    markets = unlock_row.get("markets_unlocked_full") or []
    conditions = {
        "the gap blocked a specific, named sleeve rather than a general hope":
            bool(markets),
        "the blocked question is economically material to portfolio PnL":
            cells > 0,
        "the dataset is genuinely point-in-time, with vintages or publication "
        "stamps": row.get("pit_class") in ("TRUE_POINT_IN_TIME",
                                           "OBSERVED_AS_PUBLISHED"),
        "history is long enough for the decision cadence being proposed":
            float(row.get("history_years") or 0.0)
            >= float(LANE_MIN_HISTORY.get(row["lane"]) or 0.0),
        "inactive and delisted coverage exists, so the sample is "
        "survivorship-safe": row.get("survivorship_class") in (
            "DISCONTINUED_RETAINED", "NOT_APPLICABLE"),
        "the data is economically distinct from what is already owned":
            str(row.get("incremental_distinctness") or "").startswith(
                ("HIGH", "MEDIUM")),
        "a free or owned substitute has been tried and measured as "
        "insufficient": True,
        "licensing permits the research and paper use intended":
            row.get("licence_class") == "RESEARCH_USE_CLEAR",
        "a bounded evaluation exists that could return DO_NOT_BUY": True,
        "the cost is stated and a decision-maker has accepted it": False,
    }
    return {
        "gap": row["dataset_id"],
        "blocked_sleeve": ", ".join(markets) or "none",
        "why_it_matters": (
            "%d of Release 36's blocked cells become executable" % cells),
        "owned_substitute_tried": (
            "Release 36 built every native lane owned or free data supported: "
            "FX forwards across 20 currencies and five dated NYMEX energy "
            "curves. This candidate covers markets those lanes cannot reach"),
        "conditions": conditions,
    }


def information_gate(unlock_map: dict, *, campaign_id: str) -> dict:
    """Run the released ten-condition gate over every candidate.

    ``alpha_agent.r32.purchase_gate.build`` is called for its evaluation only;
    its own ``freeze`` is deliberately NOT called, so nothing is written outside
    the Release-37 research root.
    """
    by_id = {u["dataset_id"]: u for u in unlock_map["rows"]}
    gaps = [information_gap(row, by_id.get(row["dataset_id"], {}))
            for row in _providers.rows()]
    body = _r32_gate.build(campaign_id=campaign_id, gaps=gaps)
    return {"gate_owner": _r32_gate.CALCULATION_OWNER,
            "conditions": list(_r32_gate.CONDITIONS),
            "states": list(_r32_gate.STATES),
            "gaps": body["gaps"],
            "prior_evaluations": body["prior_evaluations"],
            "purchase_frontier_hash": body["purchase_frontier_hash"],
            "written_to_r32_root": False}


# --------------------------------------------------------------------------- #
# Composition
# --------------------------------------------------------------------------- #
def build(unlock_map: dict, scorecard: dict, *, campaign_id: str) -> dict:
    """Every candidate through the canonical gate in BOTH contexts, plus R37's state."""
    by_unlock = {u["dataset_id"]: u for u in unlock_map["rows"]}
    by_score = {s["dataset_id"]: s for s in scorecard["rows"]}
    acq_state = _slice9.kernel.REC_RESEARCH_ACQUISITION
    rows = []
    for row in _providers.rows():
        unlock_row = by_unlock.get(row["dataset_id"], {})
        score = by_score.get(row["dataset_id"], {})
        slice9 = slice9_result(row, unlock_row)
        acquisition = acquisition_result(row, unlock_row)
        canonical_recommends = acquisition.get("state") == acq_state
        r37_recommends = row["gate_state"] == C.STATE_BUY_RECOMMENDED
        rows.append({
            "dataset_id": row["dataset_id"],
            "provider": row["provider"],
            "lane": row["lane"],
            "r37_state": row["gate_state"],
            "r37_reason": row["gate_reason"],
            "r37_authority": C.purchase_authority(row["gate_state"]),
            "cells_unlocked_full": unlock_row.get("cells_unlocked_full"),
            "cells_unlocked_ceiling": unlock_row.get("cells_unlocked_ceiling"),
            "research_value_per_dollar_score":
                score.get("research_value_per_dollar_score"),
            "cost_per_r36_cell_unlocked": score.get("cost_per_r36_cell_unlocked"),
            "slice9": slice9,
            "acquisition": acquisition,
            "canonical_acquisition_state": acquisition.get("state"),
            "canonical_acquisition_recommends": canonical_recommends,
            #: The whole point of the correction: this release may recommend a
            #: purchase only where the canonical gate does. Disagreement is
            #: reported, not resolved in the release's favour.
            "r37_agrees_with_canonical_gate":
                r37_recommends == canonical_recommends,
            "recommendable": bool(r37_recommends and canonical_recommends),
            "evidence": row.get("evidence"),
        })
    info = information_gate(unlock_map, campaign_id=campaign_id)
    states = sorted({r["r37_state"] for r in rows})
    canonical_recommended = sorted(r["dataset_id"] for r in rows
                                   if r["canonical_acquisition_recommends"])
    recommended = sorted(r["dataset_id"] for r in rows if r["recommendable"])
    r37_only = sorted(r["dataset_id"] for r in rows
                      if r["r37_state"] == C.STATE_BUY_RECOMMENDED
                      and not r["canonical_acquisition_recommends"])
    return {
        "rows": rows,
        "information_gate": info,
        "r37_states_present": states,
        "every_candidate_terminal": all(r["r37_state"] in C.GATE_STATES
                                        for r in rows),
        "acquisition_decision_owner": ACQUISITION_DECISION_OWNER,
        "canonical_gate_is_authoritative":
            CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE,
        "canonical_acquisition_states": {
            r["dataset_id"]: r["canonical_acquisition_state"] for r in rows},
        "canonical_acquisition_recommended": canonical_recommended,
        #: Datasets this release would have recommended but the canonical gate
        #: refused. It MUST stay empty: a non-empty list is the release trying to
        #: outrank its own authority, and the campaign refuses to recommend them.
        "recommended_by_r37_but_refused_by_canonical_gate": r37_only,
        "every_row_agrees_with_canonical_gate": all(
            r["r37_agrees_with_canonical_gate"] for r in rows),
        "recommended": recommended,
        "free_to_acquire": sorted(r["dataset_id"] for r in rows
                                  if r["r37_state"] == C.STATE_FREE_ACQUIRE_NOW),
        "blocked_on_vendor_contact": sorted(
            r["dataset_id"] for r in rows
            if r["r37_state"] == C.STATE_BLOCKED_CONTACT),
        "slice9_states": {r["dataset_id"]: r["slice9"].get("slice9_state")
                          for r in rows},
        "slice9_purchase_recommendations": sorted(
            r["dataset_id"] for r in rows
            if r["slice9"].get("slice9_state") == "PURCHASE_RECOMMENDED"),
    }


def artifact(built: dict, *, campaign_id: str, created_at: str) -> dict:
    payload = {
        "campaign_id": campaign_id,
        "created_at": created_at,
        "calculation_owner": CALCULATION_OWNER,
        "gate_owners": {
            "dataset_purchase_calculation": _slice9.kernel.CALCULATION_OWNER,
            "dataset_purchase_composition": _slice9.COMPOSITION_OWNER,
            "acquisition_decision": ACQUISITION_DECISION_OWNER,
            "information_conditions": _r32_gate.CALCULATION_OWNER,
        },
        "decision_contexts": list(_slice9.DECISION_CONTEXT_VOCAB),
        "acquisition_decision_owner": ACQUISITION_DECISION_OWNER,
        "canonical_gate_is_authoritative":
            CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE,
        "r37_may_recommend_what_the_canonical_gate_refused":
            R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED,
        "canonical_acquisition_states": built["canonical_acquisition_states"],
        "canonical_acquisition_recommended":
            built["canonical_acquisition_recommended"],
        "recommended_by_r37_but_refused_by_canonical_gate":
            built["recommended_by_r37_but_refused_by_canonical_gate"],
        "every_row_agrees_with_canonical_gate":
            built["every_row_agrees_with_canonical_gate"],
        "release37_defines_no_gate": True,
        "slice9_result_may_be_overridden": SLICE9_RESULT_MAY_BE_OVERRIDDEN,
        "r37_state_is_a_capability_judgement": R37_STATE_IS_A_CAPABILITY_JUDGEMENT,
        "r37_state_is_a_measured_lift_judgement":
            R37_STATE_IS_A_MEASURED_LIFT_JUDGEMENT,
        "bootstrap_note": BOOTSTRAP_NOTE,
        "gate_states": list(C.GATE_STATES),
        "rows": built["rows"],
        "information_gate": built["information_gate"],
        "recommended": built["recommended"],
        "free_to_acquire": built["free_to_acquire"],
        "blocked_on_vendor_contact": built["blocked_on_vendor_contact"],
        "slice9_states": built["slice9_states"],
        "slice9_purchase_recommendations":
            built["slice9_purchase_recommendations"],
        "every_candidate_terminal": built["every_candidate_terminal"],
        "purchase_authority_granted_by_this_release":
            C.PURCHASE_AUTHORITY_GRANTED_BY_THIS_RELEASE,
        "money_spent_usd": 0.0,
        "persisted_to_slice9_store": False,
    }
    return r37.artifact_body(SCHEMA, payload)


def path_for(campaign_id: str = C.CAMPAIGN_ID):
    return r37.campaign_dir(campaign_id) / ARTIFACT_NAME


def freeze(body: dict):
    return r37.write_json(path_for(body["campaign_id"]), body)


def load(campaign_id: str = C.CAMPAIGN_ID) -> Optional[dict]:
    return r37.read_json(path_for(campaign_id))


__all__ = ["CALCULATION_OWNER", "SLICE9_RESULT_MAY_BE_OVERRIDDEN",
           "R37_STATE_IS_A_CAPABILITY_JUDGEMENT",
           "R37_STATE_IS_A_MEASURED_LIFT_JUDGEMENT", "BOOTSTRAP_NOTE",
           "CANONICAL_ACQUISITION_GATE_IS_AUTHORITATIVE",
           "R37_MAY_RECOMMEND_WHAT_THE_CANONICAL_GATE_REFUSED",
           "ACQUISITION_DECISION_OWNER", "OWNED_SUBSTITUTE_TRIED",
           "UNIVERSE", "LANE_MIN_UNIVERSE", "LANE_MIN_HISTORY",
           "acquisition_case", "catalog_entry", "slice9_result",
           "acquisition_result", "information_gap",
           "information_gate", "build", "artifact", "freeze", "load",
           "path_for"]
