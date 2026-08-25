"""alpha_agent.r45.frontier - the ranked event-time frontier, the freeze gate
and the data purchase gate.

The freeze gate is the part worth reading. Release 45 is allowed to freeze up
to three prospective shadows and it is explicitly forbidden to freeze a
mediocre one just to have something to show. A shadow is a promise to watch
a specific rule forward forever; making that promise about a candidate the
release already knows is dead is not caution, it is noise.
"""
from __future__ import annotations

import datetime as _dt

from . import contract as C
from . import implementable as JD

CALCULATION_OWNER = "alpha_agent.r45.frontier"


# --------------------------------------------------------------------------- #
def _row(card: dict, *, rank: int, lane: str, expression: str,
         outright_or_rv: str, model: str, stress: dict = None,
         placebo: str = None, timing: str = None,
         event_family: str = "ALL_SCHEDULED_US_MACRO") -> dict:
    stress = stress or {}
    econ = JD.score(card, symbol=card.get("symbol"))
    return {
        "RANK": rank,
        "CANDIDATE_ID": f"r45_{lane}_{card.get('symbol')}".replace("=", ""),
        "INSTRUMENTS": card.get("symbol"),
        "INSTRUMENT_CLASS": card.get("instrument_class"),
        "EVENT_FAMILY": event_family,
        "ENTRY_DELAY": C.FROZEN_RULE["entry_delay_min"],
        "HOLD": C.FROZEN_RULE["hold_min"],
        "ECONOMIC_EXPRESSION": expression,
        "OUTRIGHT_OR_RV": outright_or_rv,
        "MODEL": model,
        "N_EVENTS": card.get("n_events"),
        "GROSS_BPS_PER_EVENT": card.get("gross_bps_per_event"),
        "COST_BPS_PER_EVENT": card.get("cost_bps_per_event"),
        "COST_SOURCE": card.get("cost_source"),
        "NET_BPS_PER_EVENT": card.get("net_bps_per_event"),
        "NET_T": card.get("net_t_cluster"),
        "HIT_RATE": card.get("hit_rate"),
        "CAPITAL": econ.get("committed_capital_per_leg_unit"),
        "NET_ANNUAL_ON_COMMITTED_MARGIN":
            econ.get("net_annual_excess_return_on_committed_margin"),
        "CAPACITY": econ.get("capacity", {}).get("state"),
        "MAX_EVENT_LOSS": card.get("max_event_loss_bps"),
        "YEAR_CONCENTRATION": card.get("largest_year_share_of_pnl"),
        "EVENT_CONCENTRATION": card.get("largest_event_share_of_pnl"),
        "PLACEBO_RESULT": placebo,
        "TIMING_PERTURBATION_RESULT": timing,
        "COST_X2": stress.get("survives_x2"),
        "COST_X3": stress.get("survives_x3"),
        "LATENCY_RESULT": stress.get("survives_latency_plus_1min"),
        "SEARCH_ADJUSTMENT": stress.get("search_adjustment"),
        "PIT_STATUS": "PIT_SAFE",
        "REPLICATION_STATUS": card.get("replication_state"),
        "FORWARD_READY": False,
        "QUALIFICATION_STATE": qualification_state(card, stress),
    }


def qualification_state(card: dict, stress: dict = None) -> str:
    if not card or card.get("state") != "MEASURED":
        return "DATA_INSUFFICIENT"
    n = int(card.get("n_events") or 0)
    net = float(card.get("net_bps_per_event") or 0.0)
    t = card.get("net_t_cluster")
    t = float(t) if t is not None else 0.0
    stress = stress or {}
    if n < C.MIN_EVENTS_TO_JUDGE_REPLICATION:
        return "DATA_INSUFFICIENT"
    if net <= 0:
        return "REFUTED" if t < -1.0 else "NOT_A_CANDIDATE"
    if n < C.MIN_EVENTS_TO_QUALIFY:
        return "WEAK_EVIDENCE"
    if t >= C.QUALIFICATION["net_t_cluster_ge"] and stress.get("survives_x2"):
        return "QUALIFIED_ALPHA"
    if t >= C.REPLICATION_NET_T_MIN:
        return "RESEARCH_CANDIDATE"
    return "WEAK_EVIDENCE"


def build(replication: dict, rv: dict = None, killer: dict = None,
          causal: dict = None) -> dict:
    rows, rank = [], 0
    stress = {}
    if killer:
        stress = {
            "survives_x2": killer.get("cost_stress", {}).get("survives_x2"),
            "survives_x3": killer.get("cost_stress", {}).get("survives_x3"),
            "survives_latency_plus_1min":
                killer.get("latency_stress", {}).get("survives_plus_1min"),
        }
    placebo = timing = None
    if causal:
        placebo = (causal.get("verdicts_by_zone", {}).get("BC", {})
                   .get("beats_shifted_placebo"))
        placebo = "BEATS_PLACEBO" if placebo else "DOES_NOT_BEAT_PLACEBO"
        sw = causal.get("timing_sweeps_by_zone", {}).get("BC", {})
        timing = ("PEAKS_AT_DECLARED_MINUTE"
                  if sw.get("peak_is_at_the_declared_minute")
                  else f"PEAKS_AT_{sw.get('peak_offset_min')}_MIN")

    for card in (replication or {}).get("ranked", []):
        if card.get("n_events") is None:
            continue
        rank += 1
        rows.append(_row(card, rank=rank, lane="OUTRIGHT",
                         expression="fade the print, +5m to +120m",
                         outright_or_rv="OUTRIGHT",
                         model="FROZEN_TRANSPARENT_RULE",
                         stress=stress if rank == 1 else None,
                         placebo=placebo if rank == 1 else None,
                         timing=timing if rank == 1 else None))

    for r in (rv or {}).get("ranked_by_holdout_t", []):
        hold = r.get("holdout_net_bps")
        if hold is None:
            continue
        rank += 1
        rows.append({
            "RANK": rank, "CANDIDATE_ID": f"r45_RV_{r['id']}",
            "INSTRUMENTS": f"{r['target']} vs {'+'.join(r['hedges'])}",
            "INSTRUMENT_CLASS": "RELATIVE_VALUE",
            "EVENT_FAMILY": "ALL_SCHEDULED_US_MACRO",
            "ENTRY_DELAY": C.FROZEN_RULE["entry_delay_min"],
            "HOLD": C.FROZEN_RULE["hold_min"],
            "ECONOMIC_EXPRESSION": "fade the HEDGED residual of the print",
            "OUTRIGHT_OR_RV": "RV", "MODEL": "OLS_HEDGE_FITTED_ON_ZONE_A",
            "N_EVENTS": r.get("holdout_n"),
            "GROSS_BPS_PER_EVENT": None, "COST_BPS_PER_EVENT": None,
            "NET_BPS_PER_EVENT": hold, "NET_T": r.get("holdout_net_t"),
            "HIT_RATE": r.get("holdout_hit"),
            "PIT_STATUS": "PIT_SAFE",
            "REPLICATION_STATUS": "HOLDOUT",
            "FORWARD_READY": False,
            "QUALIFICATION_STATE": "NOT_A_CANDIDATE" if hold <= 0
            else "WEAK_EVIDENCE",
        })

    # A candidate that cannot be judged cannot be the best one. Sixteen
    # events will happily print +40 bps and a flattering t; ranking that
    # above a 370-event result would be the exact mistake this release exists
    # to expose. Judgeable rows sort first, and `best` is drawn only from
    # them - the rest stay on the frontier, labelled, so nothing is hidden.
    def _judgeable(r):
        return int((r.get("N_EVENTS") or 0)
                   >= C.MIN_EVENTS_TO_JUDGE_REPLICATION)

    rows.sort(key=lambda r: (-_judgeable(r), -((r.get("NET_T") or -9))))
    for i, r in enumerate(rows, start=1):
        r["RANK"] = i
        r["JUDGEABLE"] = bool(_judgeable(r))
    judged = [r for r in rows if r["JUDGEABLE"]]
    return {"schema": "r45_research_frontier/1",
            "calculation_owner": CALCULATION_OWNER,
            "campaign_id": C.CAMPAIGN_ID,
            "n_candidates": len(rows), "rows": rows,
            "n_judgeable": len(judged),
            "min_events_to_judge": C.MIN_EVENTS_TO_JUDGE_REPLICATION,
            "best": judged[0] if judged else None,
            "best_is_drawn_only_from_judgeable_candidates": True,
            "why_no_best": None if judged else
            "no candidate reached the declared event floor",
            "n_qualified": sum(1 for r in rows
                               if r["QUALIFICATION_STATE"] == "QUALIFIED_ALPHA"),
            "n_research_candidates": sum(
                1 for r in rows
                if r["QUALIFICATION_STATE"] == "RESEARCH_CANDIDATE")}


# --------------------------------------------------------------------------- #
def freeze_gate(frontier: dict) -> dict:
    """Freeze only what earned it. Nothing else."""
    eligible = [r for r in (frontier or {}).get("rows", [])
                if r.get("QUALIFICATION_STATE") in
                ("QUALIFIED_ALPHA", "RESEARCH_CANDIDATE")]
    frozen = eligible[:C.MAX_NEW_SHADOWS]
    return {
        "schema": "r45_shadow_registry/1",
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": C.CAMPAIGN_ID,
        "freeze_requires": list(C.FREEZE_REQUIRES),
        "max_new_shadows": C.MAX_NEW_SHADOWS,
        "n_eligible": len(eligible),
        "FORWARD_SHADOWS_ADDED": len(frozen),
        "shadows": [{"candidate_id": r["CANDIDATE_ID"],
                     "instruments": r["INSTRUMENTS"],
                     "frozen_at": _dt.datetime.now(_dt.timezone.utc)
                     .isoformat(),
                     "promotion_allowed": False,
                     "research_shadow_only": True} for r in frozen],
        "prior_shadows_are_immutable": C.PRIOR_SHADOWS_ARE_IMMUTABLE,
        "never_backfill_prospective_rows": C.NEVER_BACKFILL_PROSPECTIVE_ROWS,
        "why_none": None if frozen else
        "no candidate reached RESEARCH_CANDIDATE. The contract forbids "
        "freezing a mediocre candidate to create a shadow, and a shadow is a "
        "permanent forward promise about a rule this release has already "
        "measured as dead.",
    }


# --------------------------------------------------------------------------- #
def purchase_gate(replication: dict, acquisition: dict = None) -> dict:
    """What deep native futures history would buy, and what it costs."""
    l3 = (replication or {}).get("L3_NATIVE_FUTURES", {})
    n_native = max([c.get("n_events") or 0
                    for c in l3.get("cards", {}).values()] or [0])

    candidates = [{
        "rank": 1, "rank_key": "INTRADAY_FUTURES",
        "PROVIDER": "Databento (CME MDP-3)",
        "DATASET": "GLBX.MDP3 historical intraday, 1-minute OHLCV",
        "EXACT_INSTRUMENTS": "ZT, ZF, ZN, ZB, ES, NQ, GC, 6E, 6J, CL",
        "RESOLUTION": "tick / MBO / 1-minute",
        "HISTORY": "2010 onward for most CME products",
        "PRICE": "pay-as-you-go; 1-minute OHLCV for ten outrights over "
                 "2012-2019 is a small fraction of a full subscription. "
                 "Budget $125-$500 for the first study.",
        "ACCOUNT_REQUIREMENT": "ACCOUNT_REQUIRED - unauthenticated metadata "
                               "calls return HTTP 401",
        "LICENCE_REQUIREMENT": "CME redistribution terms; research use "
                               "permitted",
        "SAMPLE_AVAILABILITY": "no sample reachable without an account",
        "SIGNUP_CREDIT": "the provider has advertised signup credits; not "
                         "verified by this release because verifying it "
                         "requires creating the account",
        "PIT_QUALITY": "STRONG - exchange feed, dated contracts",
        "CONTRACT_IDENTITY": "STRONG - dated contracts with roll state",
        "BID_ASK_AVAILABILITY": "YES at MBO/MBP tiers - which would replace "
                                "the ESTIMATED spread this release had to use",
        "EXACT_R45_EXPERIMENT_UNLOCKED":
            "the frozen +5/+120 rule on ZN/ZF/ZT/ES over 2012-2019, at the "
            "same sample size as the gold test, with an OBSERVED spread "
            "instead of an estimated one - and the curve RV expressions "
            "(RV08/RV09) which currently have 16 events each",
        "RECOMMEND": "DO_NOT_BUY_YET",
        "WHY": "Release 45 no longer has a live result pointing at it. The "
               "gold effect that justified the purchase failed on its own "
               "holdout, in listed US rates and equities over two years, and "
               "in every other market the estate owns. Buying deep native "
               "history would now be funding a fresh search, not confirming "
               "a finding - and this estate has priced 45 releases of fresh "
               "searches.",
        "WHAT_WOULD_CHANGE_IT": "a prospective native-futures capture that "
                               "accumulates events at $0 and shows a live "
                               "signal before any money is committed",
    }]
    return {
        "schema": "r45_data_frontier/1",
        "calculation_owner": CALCULATION_OWNER,
        "campaign_id": C.CAMPAIGN_ID,
        "authorized_spend_usd": C.AUTHORIZED_SPEND_USD,
        "money_spent_usd": 0.0,
        "accounts_created": 0, "licences_accepted": 0,
        "payment_details_submitted": 0,
        "native_futures_events_obtained_at_zero_cost": n_native,
        "events_needed_to_judge": C.MIN_EVENTS_TO_JUDGE_REPLICATION,
        "candidates": candidates,
        "TOP_DATA_PURCHASE_RECOMMENDATION": candidates[0]["PROVIDER"]
        + " - " + candidates[0]["DATASET"],
        "TOP_RECOMMENDATION_STATE": "DO_NOT_BUY_YET",
        "EXACT_PRICE_IF_ANY": candidates[0]["PRICE"],
        "ACCOUNT_REQUIRED": True,
        "PAYMENT_REQUIRED": True,
        "blocked_routes": (acquisition or {}).get(
            "blocked_native_routes", {}).get("rows", []),
    }
