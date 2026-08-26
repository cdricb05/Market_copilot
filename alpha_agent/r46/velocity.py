"""alpha_agent.r46.velocity - ONE owner for evidence velocity and independence.

Release 46.3's optimisation target is EFFECTIVE INDEPENDENT TRUE_FORWARD
EVIDENCE PER UNIT OF CALENDAR TIME - not ledger rows. Rows are cheap: one
cross-sectional challenger emitting daily at a twenty-session horizon
manufactures five rows a week forever, and every one of them is mostly the
same bet. This module is the single place where the difference between "the
ledger got bigger" and "we learned something" is computed, and everything that
displays a velocity number reads it from here.

THE DEPENDENCE ACCOUNTING, DECLARED
-----------------------------------
Three layers, each conservative, each stated so it can be argued with:

1. **Within a cell** (one challenger at one horizon): the canonical per-cell
   correction stays exactly where Release 46 put it -
   :func:`alpha_agent.r46.evidence.effective_independent` divides raw matured
   rows by the horizon and caps at distinct decision dates. Overlapping
   fixed-horizon bets are not independent draws.

2. **Within a dependence cluster** (declared per challenger in the frozen
   specifications): members express the SAME economic mechanism on the same
   information - four price-state rules on one equity universe, the same VIX
   basis at two speeds. Their evidence is counted as the MAXIMUM member cell,
   i.e. perfect dependence is assumed. That deliberately over-discounts; the
   alternative quietly under-discounts, and this project has the scar tissue.

3. **Across clusters**: distinct economic mechanisms on distinct information
   are summed as independent. Same-calendar-date co-movement across clusters
   is real residual dependence; it is DISCLOSED as a stated limitation rather
   than modelled away, because inventing a cross-cluster correlation matrix
   from zero matured observations would be fiction wearing mathematics.

``DEPENDENCE_PENALTY`` is the difference between the naive sum of cell
effectives and the tournament-level number - the amount of apparent evidence
this accounting refuses to count.

Read-only over the ledgers; writes exactly one artifact into the Release-46
research root. No operational store, no order, no portfolio, no promotion.
"""
from __future__ import annotations

from . import CAMPAIGN_ID, artifact_body, campaign_dir, write_json
from . import challengers as CH
from . import clock as CK
from . import contract as C
from . import evidence as EV
from . import ledger as LG
from . import registry as RG

CALCULATION_OWNER = "alpha_agent.r46.velocity"

ARTIFACT = "R46_EVIDENCE_VELOCITY.json"

SESSIONS_PER_WEEK = 5.0

#: Planning thresholds the operator asks about, in effective observations.
EVIDENCE_TARGETS = (10, 20, 40)

#: Expected rows per eligible session for a cell, by dependence cluster.
#: 1.0 means the rule decides every session. The turn-of-month rule holds a
#: position only in its declared window (four sessions of roughly twenty-one),
#: so projecting daily emission for it would overstate its clock by 5x.
EXPECTED_EMISSION_RATE_BY_CLUSTER = {"CALENDAR_TOM": 4.0 / 21.0}

#: Sufficiency vocabulary for the information-set gate (section 30). The
#: INSUFFICIENT state must not trigger early: it requires the contract's own
#: escalation rule - enough effective evidence across enough economically
#: distinct families, with nothing surviving the forward gates.
INFO_SET_TOO_EARLY = "TOO_EARLY_TO_JUDGE"
INFO_SET_ACCRUING = "ACCRUING"
INFO_SET_INSUFFICIENT = C.INFORMATION_SET_INSUFFICIENT
INFO_SET_STATES = (INFO_SET_TOO_EARLY, INFO_SET_ACCRUING,
                   INFO_SET_INSUFFICIENT)


# --------------------------------------------------------------------------- #
def _cells(reg: dict, preds: list, outs: list) -> list:
    outs_by_cid: dict = {}
    for o in outs:
        outs_by_cid.setdefault(o.get("challenger_id"), []).append(o)
    preds_by_cid: dict = {}
    for p in preds:
        preds_by_cid.setdefault(p.get("challenger_id"), []).append(p)

    cells = []
    for c in (reg.get("challengers") or ()):
        cid = c.get("challenger_id")
        for h in (c.get("horizons") or ()):
            my_outs = outs_by_cid.get(cid, [])
            summary = EV.summarise(my_outs, h)
            emitted = sum(1 for p in preds_by_cid.get(cid, [])
                          if int(p.get("horizon") or 0) == int(h))
            cluster = CH.cluster_for(c)
            rate = (EXPECTED_EMISSION_RATE_BY_CLUSTER.get(cluster, 1.0)
                    if c.get("state") != C.DATA_BLOCKED else 0.0)
            cells.append({
                "challenger_id": cid,
                "challenger_version": c.get("challenger_version"),
                "cohort": c.get("cohort", "R46_SEED"),
                "family": c.get("family"),
                "asset_class": c.get("asset_class"),
                "information_family": (c.get("information_family")
                                       or CH.info_family_for(c)),
                "dependence_cluster": cluster,
                "horizon": int(h),
                "state": c.get("state"),
                "raw_emitted": emitted,
                "raw_matured": summary["raw_matured"],
                "effective_independent": summary["effective_independent"],
                "n_distinct_decision_dates":
                    summary["n_distinct_decision_dates"],
                "expected_rows_per_eligible_session": rate,
                "expected_effective_per_session":
                    rate / max(1, int(h)),
            })
    return cells


def _clusters(cells: list) -> list:
    grouped: dict = {}
    for cell in cells:
        grouped.setdefault(cell["dependence_cluster"], []).append(cell)
    out = []
    for name in sorted(grouped):
        members = grouped[name]
        eff = max((m["effective_independent"] for m in members), default=0)
        naive = sum(m["effective_independent"] for m in members)
        rate = max((m["expected_effective_per_session"] for m in members
                    if m["state"] != C.DATA_BLOCKED), default=0.0)
        out.append({
            "cluster": name,
            "n_cells": len(members),
            "members": sorted({m["challenger_id"] for m in members}),
            "asset_classes": sorted({m["asset_class"] for m in members}),
            "information_families": sorted({m["information_family"]
                                            for m in members}),
            "raw_matured": sum(m["raw_matured"] for m in members),
            "effective_independent": eff,
            "naive_sum_of_cell_effectives": naive,
            "within_cluster_discount": naive - eff,
            "projected_effective_per_week":
                round(SESSIONS_PER_WEEK * rate, 4),
            "accounting": "members assumed perfectly dependent; the cluster "
                          "counts its best cell once",
        })
    return out


def _weeks_to(target: int, current: int, per_week: float):
    if current >= target:
        return 0.0
    if per_week <= 0:
        return None
    return round((target - current) / per_week, 1)


def _bottleneck(cells: list, outs: list, preds: list,
                next_maturity) -> dict:
    """The single binding constraint, named, with the ranked runners-up."""
    ranked = []
    if not preds:
        ranked.append({"code": "NO_FORWARD_PREDICTIONS",
                       "detail": "nothing is on the record yet"})
    if preds and not outs:
        ranked.append({
            "code": "AWAITING_FIRST_MATURITY",
            "detail": "every emitted prediction is still inside its outcome "
                      "window; no amount of additional emission changes when "
                      "the first one matures (%s)" % (next_maturity or
                                                      "unscheduled")})
    active = [c for c in cells if c["state"] != C.DATA_BLOCKED]
    dates = {d for p in preds for d in [str(p.get("effective_as_of"))]}
    if len(dates) <= 3:
        ranked.append({
            "code": "FEW_DISTINCT_DECISION_DATES",
            "detail": "%d distinct decision date(s) on the record; effective "
                      "independence is capped by decision dates, so only the "
                      "calendar can raise this" % len(dates)})
    h20 = [c for c in active if c["horizon"] >= 20]
    if active and len(h20) / float(len(active)) > 0.5:
        ranked.append({
            "code": "LONG_HORIZON_DOMINANCE",
            "detail": "%d of %d active cells mature in 20+ sessions; their "
                      "evidence accrues at one twentieth of a row per session "
                      "per cell" % (len(h20), len(active))})
    blocked = [c for c in cells if c["state"] == C.DATA_BLOCKED]
    if blocked:
        ranked.append({
            "code": "BLOCKED_LANES",
            "detail": "%d cell(s) cannot accrue from this location"
                      % len(blocked)})
    if not ranked:
        ranked.append({"code": "NONE_IDENTIFIED",
                       "detail": "no structural constraint dominates; "
                                 "evidence accrues on the declared clocks"})
    return {"binding": ranked[0], "ranked": ranked}


# --------------------------------------------------------------------------- #
def build(campaign_id: str = CAMPAIGN_ID, registry: dict = None) -> dict:
    """Compute and persist the one authoritative velocity artifact."""
    reg = registry if registry is not None else RG.load(campaign_id)
    preds = LG.predictions(campaign_id)
    outs = LG.outcomes(campaign_id)

    cells = _cells(reg, preds, outs)
    clusters = _clusters(cells)
    active_cells = [c for c in cells if c["state"] != C.DATA_BLOCKED]

    tournament_effective = sum(cl["effective_independent"] for cl in clusters)
    naive_sum = sum(c["effective_independent"] for c in cells)
    dependence_penalty = naive_sum - tournament_effective

    emission_dates = sorted({str(p.get("effective_as_of")) for p in preds})
    matured_dates = sorted({str(o.get("effective_as_of")) for o in outs})
    scored_ids = {str(o.get("prediction_id")) for o in outs}
    pending = [p for p in preds
               if str(p.get("prediction_id")) not in scored_ids]
    next_maturity = min((str(p.get("horizon_end_expected")) for p in pending
                         if p.get("horizon_end_expected")), default=None)

    # Realised velocity: what has actually landed per elapsed week since the
    # first decision date. Zero until something matures - reported as zero,
    # never extrapolated into a claim.
    weeks_elapsed = None
    realised_eff_per_week = None
    if emission_dates:
        first = CK.parse_iso(emission_dates[0] + "T00:00:00Z")
        now = CK.now_utc()
        weeks_elapsed = max(0.2, round(
            (now - first).total_seconds() / (7 * 86400.0), 2))
        realised_eff_per_week = round(tournament_effective / weeks_elapsed, 3)

    # Projected velocity: from the declared structure of the ACTIVE field -
    # cluster by cluster, best cell per cluster, at the declared emission
    # rates. A projection, clearly labelled; the future owes it nothing.
    projected_eff_per_week = round(
        sum(cl["projected_effective_per_week"] for cl in clusters), 3)
    projected_raw_per_session = round(
        sum(c["expected_rows_per_eligible_session"] for c in active_cells), 3)

    per_prediction_breadth = [int(p.get("n_legs") or 0) for p in preds]
    breadth = (round(sum(per_prediction_breadth)
                     / float(len(per_prediction_breadth)), 1)
               if per_prediction_breadth else None)

    projections = {
        "tournament": {
            "current_effective": tournament_effective,
            "projected_effective_per_week": projected_eff_per_week,
            "weeks_to_target": {
                str(t): _weeks_to(t, tournament_effective,
                                  projected_eff_per_week)
                for t in EVIDENCE_TARGETS},
        },
        "per_cluster": [{
            "cluster": cl["cluster"],
            "current_effective": cl["effective_independent"],
            "projected_effective_per_week":
                cl["projected_effective_per_week"],
            "weeks_to_target": {
                str(t): _weeks_to(t, cl["effective_independent"],
                                  cl["projected_effective_per_week"])
                for t in EVIDENCE_TARGETS},
        } for cl in clusters],
        "uncertainty": (
            "projections assume the field keeps emitting on its declared "
            "clocks, sessions keep printing, and no lane is killed or "
            "blocked. None of that is guaranteed and none of it is evidence; "
            "matured rows are evidence."),
    }

    # Information-set sufficiency (section 30). INSUFFICIENT requires the
    # contract's escalation rule to actually bind - never declared early.
    rule = C.ESCALATION_RULE
    families_with_evidence = sorted({c["family"] for c in cells
                                     if c["effective_independent"] > 0})
    any_confirmed = any(c["state"] == C.FORWARD_CONFIRMED for c in cells)
    if (tournament_effective >= rule["min_effective_evidence_for_escalation"]
            and len(families_with_evidence)
            >= rule["min_families_for_escalation"]
            and not any_confirmed):
        info_state = INFO_SET_INSUFFICIENT
    elif outs:
        info_state = INFO_SET_ACCRUING
    else:
        info_state = INFO_SET_TOO_EARLY

    body = artifact_body(
        "r46_evidence_velocity/1", CALCULATION_OWNER,
        built_at_utc=CK.iso(CK.now_utc()),
        campaign_id=campaign_id,

        # ---- raw vs effective, always together --------------------------- #
        raw_predictions_emitted=len(preds),
        raw_predictions_pending=len(pending),
        raw_matured_rows=len(outs),
        raw_predictions_per_session=projected_raw_per_session,
        effective_independent_observations=tournament_effective,
        naive_sum_of_cell_effectives=naive_sum,
        dependence_penalty=dependence_penalty,
        raw_rows_are_not_evidence=(
            "one strategy ranking three hundred stocks on one date is one "
            "decision, not three hundred; the effective number is the one "
            "the gates read"),

        # ---- diversity ---------------------------------------------------- #
        decision_date_count=len(emission_dates),
        matured_decision_date_count=len(matured_dates),
        asset_class_diversity=sorted({c["asset_class"]
                                      for c in active_cells}),
        economic_family_diversity=sorted({c["family"]
                                          for c in active_cells}),
        information_family_diversity=sorted({c["information_family"]
                                             for c in active_cells}),
        horizon_diversity=sorted({c["horizon"] for c in active_cells}),
        cross_sectional_breadth_mean_legs=breadth,

        # ---- velocity ----------------------------------------------------- #
        weeks_elapsed_since_first_decision=weeks_elapsed,
        realised_effective_per_week=realised_eff_per_week,
        projected_effective_per_week=projected_eff_per_week,
        projected_raw_rows_per_week=round(
            projected_raw_per_session * SESSIONS_PER_WEEK, 1),
        projections=projections,

        # ---- structure ---------------------------------------------------- #
        n_cells=len(cells),
        n_active_cells=len(active_cells),
        cells=cells,
        n_dependence_clusters=len(clusters),
        dependence_clusters=clusters,
        dependence_accounting={
            "within_cell": "raw_matured / horizon, capped at distinct "
                           "decision dates (alpha_agent.r46.evidence)",
            "within_cluster": "members assumed perfectly dependent; the "
                              "cluster counts its best cell once",
            "across_clusters": "distinct mechanisms on distinct information "
                               "summed as independent",
            "disclosed_limitation": (
                "same-date co-movement across clusters is residual "
                "dependence this accounting does not model; with zero to few "
                "matured rows there is nothing defensible to estimate a "
                "cross-cluster correlation from, so the limitation is stated "
                "instead of guessed"),
        },

        # ---- the operator's questions ------------------------------------- #
        current_evidence_bottleneck=_bottleneck(cells, outs, preds,
                                               next_maturity),
        next_maturity=next_maturity,
        information_set_state=info_state,
        information_set_state_vocabulary=list(INFO_SET_STATES),
        information_set_rule=dict(rule),
        families_with_matured_evidence=families_with_evidence,

        raw_and_effective_always_travel_together=True,
        proven_alpha_is_not_a_state=True,
    )
    write_json(campaign_dir(campaign_id) / ARTIFACT, body)
    return body
