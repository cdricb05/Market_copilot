"""alpha_agent.r46.planner - what is the next highest-value prospective experiment?

A planning owner, not a portfolio owner and not a registrar. It reads the
frozen registry, the velocity artifact and the lane artifacts, and produces a
RANKED answer to one question: where would the next unit of effort buy the
most genuinely independent forward evidence per week? It allocates no capital,
freezes no challenger and writes no registry entry - a nomination becomes a
challenger only through the existing registration contract, deliberately, in
a release or by the operator.

Ranking is mechanical and declared: a candidate scores on

* ``independence_gain`` - does it open a NEW dependence cluster or a new
  information family, or thicken an existing one? (new > thicker);
* ``clock_gain`` - projected effective observations per week it adds, from
  the same arithmetic the velocity owner uses;
* ``time_to_first_evidence`` - how many sessions until its first row can
  mature;
* ``feasibility`` - AVAILABLE_NOW beats PROSPECTIVE_ONLY beats DATA_BLOCKED,
  and a blocked candidate carries its exact blocker.

The information-set frontier (section 30) lives here too: the ranked list of
ORTHOGONAL data expansions that would matter if the current information estate
proves insufficient. Ranking it is not buying it - Release 46.3 purchases
nothing, and the sufficiency state itself is computed by the velocity owner,
never here.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, campaign_dir, read_json, write_json
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import registry as RG
from . import velocity as VL

CALCULATION_OWNER = "alpha_agent.r46.planner"

ARTIFACT = "R46_THROUGHPUT_PLAN.json"

FEAS_AVAILABLE = "AVAILABLE_NOW"
FEAS_PROSPECTIVE = "PROSPECTIVE_ONLY"
FEAS_BLOCKED = "DATA_BLOCKED"


def _lane(campaign_id: str, name: str) -> dict:
    return read_json(campaign_dir(campaign_id) / name, default=None) or {}


def _score(candidate: dict) -> float:
    feas = {FEAS_AVAILABLE: 1.0, FEAS_PROSPECTIVE: 0.5,
            FEAS_BLOCKED: 0.0}.get(candidate.get("feasibility"), 0.0)
    gain = float(candidate.get("projected_effective_per_week") or 0.0)
    fresh = 2.0 if candidate.get("opens_new_cluster") else \
        (1.0 if candidate.get("opens_new_information_family") else 0.5)
    wait = float(candidate.get("sessions_to_first_evidence") or 20)
    return round(feas * fresh * gain / max(1.0, wait / 5.0), 4)


def build(campaign_id: str = CAMPAIGN_ID, registry: dict = None,
          velocity: dict = None) -> dict:
    reg = registry if registry is not None else RG.load(campaign_id)
    vel = velocity if velocity is not None else (
        read_json(campaign_dir(campaign_id) / VL.ARTIFACT, default=None)
        or {})
    options = _lane(campaign_id, "R46_OPTIONS_LANE.json")
    analyst = _lane(campaign_id, "R46_ANALYST_LANE.json")
    intraday = _lane(campaign_id, "R46_INTRADAY_LANE.json")

    clusters = {cl.get("cluster") for cl in
                (vel.get("dependence_clusters") or ())}
    info_families = set(vel.get("information_family_diversity") or ())

    candidates = []

    # 1. The options lane: three hypotheses frozen before the confirming
    #    sessions exist; the judgeable threshold is a session count, not work.
    js = (options.get("judgeable_sample") or {})
    still = js.get("sessions_still_required")
    candidates.append({
        "candidate": "score_frozen_option_hypotheses",
        "lane": "OPTIONS_VOL",
        "feasibility": (FEAS_AVAILABLE if still == 0 else FEAS_PROSPECTIVE),
        "opens_new_cluster": True,
        "opens_new_information_family": "OPTION_SURFACE" not in info_families,
        "projected_effective_per_week": 1.0,
        "sessions_to_first_evidence": (0 if still == 0 else still),
        "detail": ("the free SPY surface holds %s of %s required sessions; "
                   "when the threshold arrives, ONLY the three pre-frozen "
                   "hypotheses may be scored - nothing may be redefined "
                   "after seeing the confirming sessions"
                   % (js.get("usable_sessions_now"),
                      js.get("sessions_required"))),
    })

    # 2. The analyst lane: the only revision history that cannot have been
    #    restated. Its challenger is already frozen; the ledger decides when.
    ajs = (analyst.get("judgeable_sample") or {})
    candidates.append({
        "candidate": "register_frozen_analyst_revision_challenger",
        "lane": "ANALYST_REVISIONS",
        "feasibility": (FEAS_AVAILABLE
                        if ajs.get("state") == "JUDGEABLE"
                        else FEAS_PROSPECTIVE),
        "opens_new_cluster": True,
        "opens_new_information_family": "ANALYST_REVISIONS"
                                        not in info_families,
        "projected_effective_per_week": 0.25,
        "sessions_to_first_evidence": 21,
        "detail": ("%s of %s prospectively captured revisions observed; "
                   "approximately %s months remain; the challenger enters "
                   "through the same frozen door as everything else, with no "
                   "credit for the history that qualified it"
                   % (ajs.get("revisions_observed"),
                      ajs.get("revisions_required"),
                      ajs.get("approx_months_remaining"))),
    })

    # 3. Positioning: CFTC commitments data is free, the estate has parsed it
    #    before, and no active cluster reads positioning today.
    candidates.append({
        "candidate": "cot_positioning_challenger",
        "lane": "POSITIONING",
        "feasibility": FEAS_PROSPECTIVE,
        "opens_new_cluster": "FUT_POSITIONING" not in clusters,
        "opens_new_information_family": "POSITIONING" not in info_families,
        "projected_effective_per_week": 0.25,
        "sessions_to_first_evidence": 5,
        "detail": "weekly CFTC commitments reports are free and previously "
                  "parsed in this estate; a positioning challenger would be "
                  "a genuinely new information family, at a weekly decision "
                  "cadence, and must be frozen before its first emission",
    })

    # 4. Credit: free FRED spread series could sign a defensive/credit cell.
    candidates.append({
        "candidate": "credit_spread_regime_challenger",
        "lane": "CREDIT_PROXY",
        "feasibility": FEAS_PROSPECTIVE,
        "opens_new_cluster": "CREDIT_REGIME" not in clusters,
        "opens_new_information_family": "CREDIT_SPREADS" not in info_families,
        "projected_effective_per_week": 0.5,
        "sessions_to_first_evidence": 5,
        "detail": "free daily high-yield spread series exist through the "
                  "owned FRED key; an economically defensible credit-regime "
                  "challenger would add an information family no active cell "
                  "reads",
    })

    # 5. Intraday: blocked is blocked, and it is listed WITH its blocker so
    #    the absence is a recorded fact rather than a forgotten idea.
    candidates.append({
        "candidate": "intraday_event_cohort",
        "lane": "INTRADAY",
        "feasibility": (FEAS_AVAILABLE
                        if intraday.get("state") == "AVAILABLE_NOW"
                        else FEAS_BLOCKED),
        "opens_new_cluster": True,
        "opens_new_information_family": True,
        "projected_effective_per_week": 5.0,
        "sessions_to_first_evidence": 1,
        "detail": intraday.get("exact_blocker")
                  or "probe the intraday lane artifact",
    })

    for c in candidates:
        c["priority_score"] = _score(c)
    candidates.sort(key=lambda c: -c["priority_score"])

    # ---- Information-set frontier (section 30) --------------------------- #
    # Ranked by the declared criteria. Listing is not buying: every row is
    # research planning, and the sufficiency state that would make this list
    # actionable is computed by the velocity owner, not here.
    frontier = [
        {"dataset": "single_name_option_surface",
         "economic_distinctness": "HIGH - dispersion and single-name skew "
                                  "have no expression in the current estate",
         "pit_integrity": "HIGH for newly captured data; vendor history "
                          "must be vintage-verified",
         "history_depth": "vendor-dependent",
         "delisted_coverage": "vendor-dependent - the known failure mode",
         "expected_effective_gain_per_week": 1.0,
         "families_unlocked": ["DISPERSION", "SINGLE_NAME_SKEW"],
         "licensing": "commercial", "cost": "recurring, not priced here"},
        {"dataset": "fx_forward_points_g10",
         "economic_distinctness": "HIGH - FX carry is measured, not proxied",
         "pit_integrity": "HIGH if captured forward",
         "history_depth": "vendor-dependent",
         "delisted_coverage": "not applicable",
         "expected_effective_gain_per_week": 0.5,
         "families_unlocked": ["FX_CARRY"],
         "licensing": "commercial or central-bank partial",
         "cost": "recurring, not priced here"},
        {"dataset": "cftc_commitments_weekly",
         "economic_distinctness": "HIGH - positioning is not price",
         "pit_integrity": "HIGH - release-stamped weekly reports",
         "history_depth": "decades, free",
         "delisted_coverage": "not applicable",
         "expected_effective_gain_per_week": 0.25,
         "families_unlocked": ["POSITIONING"],
         "licensing": "free", "cost": "zero"},
        {"dataset": "credit_spread_series_fred",
         "economic_distinctness": "MEDIUM - correlated with equity vol but "
                                  "a distinct market's price",
         "pit_integrity": "HIGH for current observations",
         "history_depth": "decades, free",
         "delisted_coverage": "not applicable",
         "expected_effective_gain_per_week": 0.5,
         "families_unlocked": ["CREDIT_REGIME"],
         "licensing": "free", "cost": "zero"},
        {"dataset": "licensed_realtime_intraday_feed",
         "economic_distinctness": "HIGH - event reaction and microstructure "
                                  "are unreachable at daily cadence",
         "pit_integrity": "HIGH by construction for a live feed",
         "history_depth": "n/a - forward capture",
         "delisted_coverage": "n/a",
         "expected_effective_gain_per_week": 5.0,
         "families_unlocked": ["EVENT_REACTION", "MICROSTRUCTURE"],
         "licensing": "commercial", "cost": "recurring, not priced here"},
    ]

    body = artifact_body(
        "r46_throughput_plan/1", CALCULATION_OWNER,
        built_at_utc=CK.iso(CK.now_utc()),
        campaign_id=campaign_id,
        question="where does the next unit of effort buy the most genuinely "
                 "independent forward evidence per week?",
        current_projected_effective_per_week=vel.get(
            "projected_effective_per_week"),
        current_bottleneck=(vel.get("current_evidence_bottleneck") or {})
        .get("binding"),
        ranked_candidates=candidates,
        top_candidate=(candidates[0] if candidates else None),
        information_set_state=vel.get("information_set_state"),
        information_set_frontier=frontier,
        frontier_is_planning_only=True,
        nominates_but_never_registers=True,
        allocates_no_capital=True,
        purchases_nothing=True,
        scoring_rule="feasibility x novelty x projected effective "
                     "observations per week, discounted by sessions until "
                     "first evidence",
        registry_hash=reg.get("registry_hash"),
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
